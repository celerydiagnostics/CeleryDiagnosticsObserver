"""On-demand, bounded, read-only Celery diagnostic probes."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from threading import Event, Thread
from typing import Any, Callable

import requests

from .capabilities import active_probe_capabilities
from .config import ObserverConfig
from .policy import TelemetryPolicy
from .redis_message_probe import redis_task_message_present
from .redis_result_probe import redis_result_status
from .sanitizer import redact_url_credentials, stable_hash


logger = logging.getLogger(__name__)
TERMINAL_RESULT_STATES = {"SUCCESS", "FAILURE", "REVOKED"}


class ActiveProbeExecutor:
    def __init__(
        self,
        app: Any,
        config: ObserverConfig,
        policy: TelemetryPolicy,
        *,
        redis_client_factory: Callable[[str], Any] | None = None,
    ):
        self.app = app
        self.config = config
        self.policy = policy
        self.redis_client_factory = (
            redis_client_factory or self._default_redis_client_factory
        )
        self.capabilities = set(
            active_probe_capabilities(app, config, policy)
        )
        self.handlers: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
            "celery.broker.message_presence": self._query_broker_message,
            "celery.inspect.query_task": self._query_task,
            "celery.inspect.task_reservation": self._query_reservation,
            "celery.inspect.task_schedule": self._query_schedule,
            "celery.inspect.worker_capacity": self._query_capacity,
            "celery.inspect.worker_presence": self._query_worker_presence,
            "celery.broker.queue_consumers": self._query_queue_consumers,
            "celery.result_backend.status": self._query_result_backend,
        }

    def execute(self, request: dict[str, Any]) -> dict[str, Any]:
        started = time.monotonic()
        observed_at = datetime.now(timezone.utc)
        request_id = str(request.get("request_id") or "")
        capability = str(request.get("capability") or "")
        base = {
            "request_id": request_id,
            "observer_id": self.config.observer_id,
            "capability": capability,
            "observed_at": observed_at.isoformat(),
        }
        if not request_id or capability not in self.capabilities:
            return {
                **base,
                "complete": False,
                "error_type": "unsupported_capability",
                "latency_seconds": time.monotonic() - started,
            }
        handler = self.handlers.get(capability)
        if handler is None:
            return {
                **base,
                "complete": False,
                "error_type": "missing_handler",
                "latency_seconds": time.monotonic() - started,
            }
        try:
            outcome = handler(request)
        except Exception as error:  # noqa: BLE001
            logger.warning(
                "active diagnostic probe failed capability=%s error=%s",
                capability,
                type(error).__name__,
            )
            outcome = {
                "complete": False,
                "error_type": type(error).__name__,
            }
        return {
            **base,
            **outcome,
            "latency_seconds": time.monotonic() - started,
        }

    def _query_task(self, request: dict[str, Any]) -> dict[str, Any]:
        task_id = _required(request, "task_id")
        expected = _expected_workers(request)
        queried = self._inspector(request).query_task(task_id) or {}
        match = _find_queried_task(queried, task_id)
        replies = self._reply_labels(queried)
        if match is not None and match[1] == "active":
            return _positive(
                "active",
                "active",
                expected=expected,
                replies=replies,
                worker=match[0],
            )
        if match is not None:
            location = "scheduled" if match[1] == "ready" else "reserved"
            complete = True
        else:
            location = "not_active"
            complete = _negative_complete(expected, replies)
        return _negative(
            "absent",
            location,
            complete=complete,
            expected=expected,
            replies=replies,
        )

    def _query_reservation(self, request: dict[str, Any]) -> dict[str, Any]:
        task_id = _required(request, "task_id")
        expected = _expected_workers(request)
        queried = self._inspector(request).query_task(task_id) or {}
        match = _find_queried_task(queried, task_id)
        replies = self._reply_labels(queried)
        if match is not None and match[1] in {"reserved", "ready"}:
            return _positive(
                "reserved",
                "scheduled" if match[1] == "ready" else "reserved",
                expected=expected,
                replies=replies,
                worker=match[0],
            )
        return _negative(
            "not_reserved",
            "not_reserved",
            complete=(
                True
                if match is not None
                else _negative_complete(expected, replies)
            ),
            expected=expected,
            replies=replies,
        )

    def _query_schedule(self, request: dict[str, Any]) -> dict[str, Any]:
        target = _parse_datetime(
            request.get("scheduled_for") or request.get("eta")
        )
        if target is None:
            return {
                "complete": False,
                "error_type": "schedule_metadata_unavailable",
            }
        outcome = (
            "eligibility_future"
            if datetime.now(timezone.utc) < target
            else "eligibility_due"
        )
        return {
            "outcome_id": outcome,
            "complete": True,
            "state": outcome,
        }

    def _query_capacity(self, request: dict[str, Any]) -> dict[str, Any]:
        expected = _expected_workers(request)
        inspector = self._inspector(request, request_count=2)
        active = self._labeled_mapping(inspector.active() or {})
        stats = self._labeled_mapping(inspector.stats() or {})
        replies = set(active) & set(stats)
        scope = set(expected) if expected else replies
        if not scope or not scope.issubset(replies):
            return _negative(
                "",
                "capacity_partial",
                complete=False,
                expected=expected,
                replies=replies,
                error_type="partial_reply",
            )
        active_count = sum(_collection_count(active.get(worker)) for worker in scope)
        capacities = [_worker_concurrency(stats.get(worker)) for worker in scope]
        if any(item is None for item in capacities):
            return _negative(
                "",
                "capacity_unknown",
                complete=False,
                expected=expected,
                replies=replies,
                error_type="capacity_unknown",
            )
        total_capacity = sum(int(item) for item in capacities if item is not None)
        outcome = (
            "saturated"
            if total_capacity > 0 and active_count >= total_capacity
            else "available"
        )
        return {
            "outcome_id": outcome,
            "complete": True,
            "state": outcome,
            "worker_replies_expected": len(scope),
            "worker_replies_received": len(replies & scope),
        }

    def _query_worker_presence(self, request: dict[str, Any]) -> dict[str, Any]:
        expected = _expected_workers(request)
        if not expected:
            return {
                "complete": False,
                "error_type": "target_worker_unknown",
            }
        ping = self._inspector(request).ping() or {}
        replies = self._reply_labels(ping)
        if set(expected).issubset(replies):
            return {
                "outcome_id": "online",
                "complete": True,
                "state": "online",
                "worker_replies_expected": len(expected),
                "worker_replies_received": len(set(expected) & replies),
            }
        # A missing broadcast reply is not proof that the worker is offline.
        return {
            "complete": False,
            "error_type": "worker_presence_unconfirmed",
            "state": "no_ping_reply",
            "worker_replies_expected": len(expected),
            "worker_replies_received": len(set(expected) & replies),
        }

    def _query_queue_consumers(self, request: dict[str, Any]) -> dict[str, Any]:
        queue = _required(request, "queue")
        expected = _expected_workers(request)
        active_queues = self._inspector(request).active_queues() or {}
        consumers = {
            self._worker_label(worker)
            for worker, rows in active_queues.items()
            if queue in self._queue_names(rows)
        }
        if consumers:
            return {
                "outcome_id": "consumers_present",
                "complete": True,
                "state": "consumers_present",
                "queue": queue,
                "worker_replies_expected": len(expected),
                "worker_replies_received": len(active_queues),
            }
        replies = self._reply_labels(active_queues)
        complete = _negative_complete(expected, replies)
        return _negative(
            "no_consumers",
            "no_consumers",
            complete=complete,
            expected=expected,
            replies=replies,
            queue=queue,
        )

    def _query_result_backend(self, request: dict[str, Any]) -> dict[str, Any]:
        task_id = _required(request, "task_id")
        state = redis_result_status(self.app, task_id)
        if state is None:
            return {
                "complete": False,
                "error_type": "status_only_result_unavailable",
            }
        return {
            "outcome_id": (
                "terminal" if state in TERMINAL_RESULT_STATES else "non_terminal"
            ),
            "complete": True,
            "state": state,
        }

    def _query_broker_message(
        self,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        task_id = _required(request, "task_id")
        queue = self._configured_queue(_required(request, "queue"))
        if queue is None:
            return {
                "complete": False,
                "error_type": "queue_not_configured",
            }
        client = self.redis_client_factory(self.config.broker_url)
        try:
            present = redis_task_message_present(
                client,
                queue,
                task_id,
                max_messages=self.config.broker_message_scan_limit,
            )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        if present is None:
            return {
                "complete": False,
                "error_type": "broker_message_identity_unavailable",
                "state": "unverifiable",
                "queue": self._queue_label(queue),
            }
        outcome = "present" if present else "absent"
        return {
            "outcome_id": outcome,
            "complete": True,
            "state": outcome,
            "queue": self._queue_label(queue),
        }

    def _inspector(self, request: dict[str, Any], *, request_count: int = 1):
        timeout = _bounded_timeout(request.get("timeout_seconds"))
        timeout = max(0.2, timeout / max(1, request_count))
        expected = _expected_workers(request)
        kwargs: dict[str, Any] = {"timeout": timeout}
        if (
            expected
            and self.policy.send_worker_names
            and all(_is_routable_worker_name(worker) for worker in expected)
        ):
            kwargs["destination"] = expected
        return self.app.control.inspect(**kwargs)

    def _worker_label(self, value: Any) -> str:
        return _privacy_label(
            value,
            kind="worker",
            allowed=self.policy.send_worker_names,
            config=self.config,
        )

    def _queue_label(self, value: Any) -> str:
        return _privacy_label(
            value,
            kind="queue",
            allowed=self.policy.send_queue_names,
            config=self.config,
        )

    def _reply_labels(self, payload: Any) -> set[str]:
        if not isinstance(payload, dict):
            return set()
        return {
            label
            for worker in payload
            if (label := self._worker_label(worker))
        }

    def _labeled_mapping(self, payload: Any) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        result = {}
        for worker, value in payload.items():
            label = self._worker_label(worker)
            if label:
                result[label] = value
        return result

    def _queue_names(self, payload: Any) -> set[str]:
        return {
            label
            for queue in _queue_names(payload)
            if (label := self._queue_label(queue))
        }

    def _configured_queue(self, requested: str) -> str | None:
        for queue in self.config.queues:
            if self._queue_label(queue) == requested:
                return queue
        return None

    @staticmethod
    def _default_redis_client_factory(broker_url: str):
        import redis

        return redis.Redis.from_url(
            broker_url,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
        )


class ActiveProbeLoop:
    def __init__(
        self,
        executor: ActiveProbeExecutor,
        config: ObserverConfig,
        *,
        interval: float = 1.0,
        session: requests.Session | None = None,
    ):
        self.executor = executor
        self.config = config
        self.interval = max(0.25, float(interval))
        self.session = session or requests.Session()
        self.stop_event = Event()
        self.thread: Thread | None = None

    def start(self) -> None:
        if not self.executor.capabilities:
            return
        if self.thread is not None and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = Thread(
            target=self._run,
            name="celery-diagnostics-active-probes",
            daemon=True,
        )
        self.thread.start()

    def stop(self, *, timeout: float = 2.0) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=timeout)

    def run_once(self) -> bool:
        headers = {
            "Authorization": f"Bearer {self.config.project_key}",
            "User-Agent": "celery-diagnostics-observer",
        }
        response = self.session.get(
            self.config.probe_next_url,
            params={"observer_id": self.config.observer_id},
            headers=headers,
            timeout=5.0,
        )
        if response.status_code == 204:
            return False
        response.raise_for_status()
        request = response.json()
        result = self.executor.execute(request)
        submitted = self.session.post(
            self.config.probe_result_url,
            json=result,
            headers=headers,
            timeout=5.0,
        )
        submitted.raise_for_status()
        return True

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                worked = self.run_once()
            except Exception as error:  # noqa: BLE001
                logger.warning(
                    "active probe exchange failed url=%s error=%s",
                    redact_url_credentials(self.config.probe_next_url),
                    type(error).__name__,
                )
                worked = False
            self.stop_event.wait(0.05 if worked else self.interval)


def _required(request: dict[str, Any], field: str) -> str:
    value = str(request.get(field) or "").strip()
    if not value:
        raise ValueError(f"{field} is required")
    return value


def _expected_workers(request: dict[str, Any]) -> tuple[str, ...]:
    values = request.get("expected_workers")
    if not isinstance(values, list):
        worker = str(request.get("worker") or "").strip()
        return (worker,) if worker else ()
    return tuple(sorted({str(item).strip() for item in values if str(item).strip()}))


def _find_queried_task(
    payload: Any,
    task_id: str,
) -> tuple[str, str] | None:
    if not isinstance(payload, dict):
        return None
    for worker, rows in payload.items():
        if not isinstance(rows, dict) or task_id not in rows:
            continue
        value = rows[task_id]
        if not isinstance(value, (list, tuple)) or not value:
            continue
        state = str(value[0] or "").strip().lower()
        if state:
            return str(worker), state
    return None


def _negative_complete(
    expected: tuple[str, ...],
    replies: set[str],
) -> bool:
    return bool(expected and set(expected).issubset(replies))


def _is_routable_worker_name(value: str) -> bool:
    """Return false for privacy aliases that Celery cannot use as destinations."""
    normalized = str(value or "").strip()
    is_privacy_alias = (
        normalized.lower().startswith("worker_") and "@" not in normalized
    )
    return bool(normalized) and not is_privacy_alias


def _positive(
    outcome_id: str,
    location: str,
    *,
    expected: tuple[str, ...],
    replies: set[str],
    worker: str,
) -> dict[str, Any]:
    return {
        "outcome_id": outcome_id,
        "complete": True,
        "matched_location": location,
        "state": location,
        "worker_replies_expected": len(expected),
        "worker_replies_received": len(replies),
    }


def _negative(
    outcome_id: str,
    location: str,
    *,
    complete: bool,
    expected: tuple[str, ...],
    replies: set[str],
    error_type: str = "",
    queue: str = "",
) -> dict[str, Any]:
    payload = {
        "complete": complete,
        "matched_location": location,
        "state": location,
        "worker_replies_expected": len(expected),
        "worker_replies_received": len(set(expected) & replies),
    }
    if complete and outcome_id:
        payload["outcome_id"] = outcome_id
    else:
        payload["error_type"] = error_type or "partial_reply"
    if queue:
        payload["queue"] = queue
    return payload


def _queue_names(payload: Any) -> set[str]:
    if not isinstance(payload, list):
        return set()
    result = set()
    for item in payload:
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("queue") or "").strip()
        else:
            name = str(item or "").strip()
        if name:
            result.add(name)
    return result


def _collection_count(value: Any) -> int:
    return len(value) if isinstance(value, (list, dict)) else 0


def _worker_concurrency(stats: Any) -> int | None:
    if not isinstance(stats, dict):
        return None
    pool = stats.get("pool")
    pool = pool if isinstance(pool, dict) else {}
    value = (
        pool.get("max-concurrency")
        or pool.get("max_concurrency")
        or stats.get("concurrency")
    )
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _bounded_timeout(value: Any) -> float:
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        timeout = 2.0
    return min(max(timeout, 0.2), 10.0)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _privacy_label(
    value: Any,
    *,
    kind: str,
    allowed: bool,
    config: ObserverConfig,
) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if allowed:
        return raw[:255]
    digest = stable_hash(raw, salt=config.project_key).split(":", 1)[1][:16]
    return f"{kind}_{digest}"


__all__ = ["ActiveProbeExecutor", "ActiveProbeLoop"]
