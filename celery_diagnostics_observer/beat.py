from __future__ import annotations

import atexit
import copy
import hashlib
import json
import logging
import os
import socket
import time
import uuid
from datetime import datetime, timedelta, timezone
from threading import Event, Thread
from typing import Any

from celery.beat import PersistentScheduler
from kombu.exceptions import OperationalError as KombuOperationalError

from .config import config_from_env
from .policy import policy_from_name
from .sanitizer import sanitize_celery_event
from .transport import ObserverTransport


logger = logging.getLogger(__name__)
MAX_SCHEDULE_SNAPSHOT_ENTRIES = 200


class ObserverPersistentScheduler(PersistentScheduler):
    """Default Celery Beat scheduler with privacy-safe schedule evidence."""

    def __init__(self, *args, **kwargs):
        self._cd_config = config_from_env(
            {
                "observer_id": os.getenv("CD_BEAT_OBSERVER_ID") or f"beat@{socket.gethostname()}",
                "batch_size": 1000,
                "spool_path": os.getenv("CD_BEAT_SPOOL_PATH", ""),
            }
        )
        self._cd_policy = policy_from_name(self._cd_config.telemetry_policy)
        self._cd_enabled = bool(self._cd_config.project_key)
        self._cd_transport = ObserverTransport(self._cd_config) if self._cd_enabled else None
        self._cd_flush_loop = _TransportFlushLoop(self._cd_transport, interval=self._cd_config.flush_interval)
        self._cd_last_snapshot_at = 0.0
        self._cd_snapshot_interval = _bounded_seconds(
            os.getenv("CD_BEAT_SNAPSHOT_INTERVAL", "30"),
            default=30.0,
            minimum=5.0,
            maximum=3600.0,
        )
        super().__init__(*args, **kwargs)
        if self._cd_enabled:
            self._cd_flush_loop.start()
            atexit.register(self._cd_shutdown)
        else:
            logger.warning("Celery Diagnostics Beat adapter disabled: CD_PROJECT_KEY is not configured")

    def tick(self, *args, **kwargs):
        self._cd_emit_schedule_snapshot()
        self._cd_current_due_entry = None
        due_entry = _first_due_entry(getattr(self, "schedule", None))
        try:
            return super().tick(*args, **kwargs)
        except Exception as exc:
            # Celery resolves ``self.producer`` before calling ``apply_async``.
            # A broker connection failure at that point would otherwise bypass
            # the adapter's task-level evidence entirely.
            # Celery reserves the due entry before it resolves ``producer``.
            # Its heap drift can make our pre-tick is_due check disagree by a
            # few milliseconds, so prefer the exact entry remembered by
            # ``reserve`` when producer creation fails.
            due_entry = self._cd_current_due_entry or due_entry
            if self._cd_enabled and due_entry is not None and _is_broker_error(exc):
                prepared_entry, context = _prepare_entry(
                    due_entry,
                    beat_hostname=self._cd_config.observer_id,
                )
                self._cd_enqueue(
                    _periodic_task_payload(
                        "beat-task-due",
                        prepared_entry,
                        context=context,
                        config=self._cd_config,
                        policy=self._cd_policy,
                    )
                )
                self._cd_enqueue(
                    _periodic_task_payload(
                        "beat-publish-failed",
                        prepared_entry,
                        context=context,
                        config=self._cd_config,
                        policy=self._cd_policy,
                        exception=exc,
                    )
                )
                # Beat may terminate immediately after a broker connection
                # failure.  Do not depend on the background flush loop or an
                # atexit handler to preserve the only direct failure evidence.
                self._cd_flush_failure_evidence()
            raise

    def reserve(self, entry):
        self._cd_current_due_entry = entry
        return super().reserve(entry)

    def apply_async(self, entry, producer=None, advance=True, **kwargs):
        if not self._cd_enabled:
            return super().apply_async(entry, producer=producer, advance=advance, **kwargs)

        prepared_entry, context = _prepare_entry(entry, beat_hostname=self._cd_config.observer_id)
        due_payload = _periodic_task_payload(
            "beat-task-due",
            prepared_entry,
            context=context,
            config=self._cd_config,
            policy=self._cd_policy,
        )
        self._cd_enqueue(due_payload)
        try:
            result = super().apply_async(prepared_entry, producer=producer, advance=advance, **kwargs)
        except Exception as exc:
            failed_payload = _periodic_task_payload(
                "beat-publish-failed",
                prepared_entry,
                context=context,
                config=self._cd_config,
                policy=self._cd_policy,
                exception=exc,
            )
            self._cd_enqueue(failed_payload)
            self._cd_flush_failure_evidence()
            raise

        actual_task_id = str(getattr(result, "id", "") or "")
        expected_task_id = str(context.get("task_id") or "")
        if actual_task_id and actual_task_id != expected_task_id:
            logger.warning("Beat returned a different task id than the preallocated diagnostics id")
        return result

    def close(self):
        self._cd_shutdown()
        return super().close()

    def _cd_emit_schedule_snapshot(self, *, force: bool = False) -> None:
        if not self._cd_enabled:
            return
        now = time.monotonic()
        if not force and now - self._cd_last_snapshot_at < self._cd_snapshot_interval:
            return
        try:
            schedule = getattr(self, "schedule", None)
            entries = _schedule_entries(schedule)
            total_entry_count = _schedule_entry_count(schedule, fallback=len(entries))
            raw = {
                "type": "beat-schedule-snapshot",
                "hostname": self._cd_config.observer_id,
                "timestamp": time.time(),
                "beat_schedule_version": _stable_hash(
                    [
                        {
                            "schedule_id": item["schedule_id"],
                            "schedule_hash": item["schedule_hash"],
                            "enabled": item["enabled"],
                        }
                        for item in entries
                    ]
                ),
                "beat_schedule_entry_count": total_entry_count,
                "beat_schedule_snapshot_complete": total_entry_count <= len(entries),
                "beat_schedule_entries": entries,
            }
            payload = sanitize_celery_event(raw, config=self._cd_config, policy=self._cd_policy)
            self._cd_enqueue(payload)
            self._cd_last_snapshot_at = now
        except Exception as exc:  # noqa: BLE001 - diagnostics must not interrupt Beat.
            logger.warning(
                "Beat schedule snapshot failed error=%s detail=%s",
                type(exc).__name__,
                str(exc)[:240],
            )

    def _cd_enqueue(self, payload: dict[str, Any] | None) -> None:
        if payload is not None and self._cd_transport is not None:
            self._cd_transport.enqueue(payload)

    def _cd_flush_failure_evidence(self) -> None:
        transport = self._cd_transport
        if transport is None:
            return
        try:
            transport.flush_once(force=True)
        except Exception as exc:  # noqa: BLE001 - diagnostics must not interrupt Beat.
            logger.warning(
                "Beat failure evidence flush failed error=%s",
                type(exc).__name__,
            )

    def _cd_shutdown(self) -> None:
        loop = getattr(self, "_cd_flush_loop", None)
        if loop is not None:
            loop.stop()
        transport = getattr(self, "_cd_transport", None)
        if transport is not None:
            transport.flush_once(force=True)


class _TransportFlushLoop:
    def __init__(self, transport: ObserverTransport | None, *, interval: float):
        self.transport = transport
        self.interval = max(0.1, float(interval))
        self.stop_event = Event()
        self.thread: Thread | None = None

    def start(self) -> None:
        if self.transport is None or (self.thread is not None and self.thread.is_alive()):
            return
        self.stop_event.clear()
        self.thread = Thread(target=self._run, name="celery-diagnostics-beat-transport", daemon=True)
        self.thread.start()

    def stop(self, *, timeout: float = 2.0) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self.stop_event.wait(self.interval):
            try:
                if self.transport is not None:
                    self.transport.flush_once()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Beat evidence transport failed error=%s", type(exc).__name__)


def _prepare_entry(entry: Any, *, beat_hostname: str) -> tuple[Any, dict[str, Any]]:
    schedule_name = str(getattr(entry, "name", "") or "")
    task_name = str(getattr(entry, "task", "") or "")
    options = dict(getattr(entry, "options", {}) or {})
    task_id = str(options.get("task_id") or uuid.uuid4())
    scheduled_fire_at = _expected_fire_at(entry)
    schedule_id = _stable_hash({"name": schedule_name, "task_name": task_name})
    schedule_hash = _entry_hash(entry)
    occurrence_key = _stable_hash(
        {
            "schedule_id": schedule_id,
            "scheduled_fire_at": scheduled_fire_at.isoformat(),
            "total_run_count": _non_negative_int(getattr(entry, "total_run_count", None)) or 0,
        }
    )
    lateness_ms = max(0, int((datetime.now(timezone.utc) - scheduled_fire_at).total_seconds() * 1000))
    context = {
        "task_id": task_id,
        "periodic_schedule_name": schedule_name or task_name,
        "periodic_schedule_id": schedule_id,
        "periodic_schedule_hash": schedule_hash,
        "periodic_schedule_version": schedule_hash,
        "periodic_occurrence_key": occurrence_key,
        "periodic_scheduled_fire_at": scheduled_fire_at.isoformat(),
        "periodic_task_lateness_ms": lateness_ms,
        "beat_hostname": beat_hostname,
    }
    headers = dict(options.get("headers") or {})
    headers.update(
        {
            "celery_diagnostics_periodic": "1",
            "celery_diagnostics_schedule_name": context["periodic_schedule_name"],
            "celery_diagnostics_schedule_id": schedule_id,
            "celery_diagnostics_schedule_hash": schedule_hash,
            "celery_diagnostics_schedule_version": schedule_hash,
            "celery_diagnostics_occurrence_key": occurrence_key,
            "celery_diagnostics_scheduled_fire_at": context["periodic_scheduled_fire_at"],
            "celery_diagnostics_beat_hostname": beat_hostname,
        }
    )
    options["headers"] = headers
    options["task_id"] = task_id
    prepared = copy.copy(entry)
    prepared.options = options
    return prepared, context


def _periodic_task_payload(
    event_type: str,
    entry: Any,
    *,
    context: dict[str, Any],
    config,
    policy,
    exception: Exception | None = None,
) -> dict[str, Any] | None:
    options = dict(getattr(entry, "options", {}) or {})
    raw = {
        "type": event_type,
        "uuid": context["task_id"],
        "name": str(getattr(entry, "task", "") or ""),
        "queue": options.get("queue") or options.get("routing_key") or "",
        "routing_key": options.get("routing_key") or options.get("queue") or "",
        "exchange": options.get("exchange") or "",
        "timestamp": time.time(),
        "is_periodic_task": True,
        **context,
    }
    if exception is not None:
        raw["exc_type"] = type(exception).__name__
    return sanitize_celery_event(raw, config=config, policy=policy)


def _schedule_entries(schedule: Any) -> list[dict[str, Any]]:
    items = getattr(schedule, "items", None)
    if not callable(items):
        return []
    entries: list[dict[str, Any]] = []
    for name, entry in list(items())[:MAX_SCHEDULE_SNAPSHOT_ENTRIES]:
        task_name = str(getattr(entry, "task", "") or "")
        if not task_name:
            continue
        options = dict(getattr(entry, "options", {}) or {})
        schedule_name = str(name or getattr(entry, "name", "") or task_name)
        schedule_hash = _entry_hash(entry)
        next_due_at = _next_due_at(entry)
        last_run_at = _as_utc(getattr(entry, "last_run_at", None))
        entries.append(
            {
                "schedule_id": _stable_hash({"name": schedule_name, "task_name": task_name}),
                "schedule_name": schedule_name,
                "task_name": task_name,
                "schedule_type": _schedule_type(getattr(entry, "schedule", None)),
                "schedule_display": str(getattr(entry, "schedule", "") or "")[:255],
                "schedule_hash": schedule_hash,
                "schedule_version": schedule_hash,
                "enabled": _entry_enabled(entry),
                "queue": str(options.get("queue") or ""),
                "routing_key": str(options.get("routing_key") or ""),
                "exchange": str(options.get("exchange") or ""),
                "next_due_at": next_due_at.isoformat() if next_due_at else "",
                "last_run_at": last_run_at.isoformat() if last_run_at else "",
                "total_run_count": _non_negative_int(getattr(entry, "total_run_count", None)),
                "interval_seconds": _interval_seconds(getattr(entry, "schedule", None)),
            }
        )
    return entries


def _schedule_entry_count(schedule: Any, *, fallback: int) -> int:
    try:
        return max(0, int(len(schedule)))
    except (TypeError, ValueError, AttributeError):
        return fallback


def _first_due_entry(schedule: Any) -> Any | None:
    values = getattr(schedule, "values", None)
    if not callable(values):
        return None
    for entry in list(values())[:MAX_SCHEDULE_SNAPSHOT_ENTRIES]:
        is_due = getattr(entry, "is_due", None)
        if not callable(is_due):
            continue
        try:
            due, _next_check = is_due()
        except Exception:  # noqa: BLE001 - one invalid entry must not affect Beat.
            continue
        if due:
            return entry
    return None


def _is_broker_error(exc: Exception) -> bool:
    current: BaseException | None = exc
    visited: set[int] = set()
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        if isinstance(current, (KombuOperationalError, ConnectionError, OSError)):
            return True
        module = type(current).__module__.lower()
        name = type(current).__name__.lower()
        if name in {"connectionerror", "timeouterror"} and (
            module.startswith("redis") or module.startswith("kombu")
        ):
            return True
        current = current.__cause__ or current.__context__
    return False


def _entry_hash(entry: Any) -> str:
    options = dict(getattr(entry, "options", {}) or {})
    return _stable_hash(
        {
            "name": str(getattr(entry, "name", "") or ""),
            "task": str(getattr(entry, "task", "") or ""),
            "schedule_type": _schedule_type(getattr(entry, "schedule", None)),
            "schedule": str(getattr(entry, "schedule", "") or "")[:255],
            "options": {
                key: options.get(key)
                for key in ("queue", "routing_key", "exchange", "expires", "priority")
            },
        }
    )


def _expected_fire_at(entry: Any) -> datetime:
    last_run_at = _as_utc(getattr(entry, "last_run_at", None))
    interval_seconds = _interval_seconds(getattr(entry, "schedule", None))
    if last_run_at is not None and interval_seconds:
        return (last_run_at + timedelta(seconds=interval_seconds)).replace(microsecond=0)
    return datetime.now(timezone.utc).replace(microsecond=0)


def _next_due_at(entry: Any) -> datetime | None:
    is_due = getattr(entry, "is_due", None)
    if not callable(is_due):
        return None
    try:
        due, next_check = is_due()
        seconds = 0.0 if due else float(next_check)
    except (TypeError, ValueError, OverflowError):
        return None
    if seconds < 0 or seconds > 366 * 24 * 60 * 60:
        return None
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def _schedule_type(schedule: Any) -> str:
    name = type(schedule).__name__.lower() if schedule is not None else ""
    if "crontab" in name:
        return "crontab"
    if "solar" in name:
        return "solar"
    if "clocked" in name:
        return "clocked"
    if "schedule" in name:
        return "interval"
    return name[:64]


def _interval_seconds(schedule: Any) -> int | None:
    run_every = getattr(schedule, "run_every", None)
    if isinstance(run_every, timedelta):
        seconds = int(run_every.total_seconds())
        return seconds if seconds > 0 else None
    try:
        seconds = int(run_every)
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


def _entry_enabled(entry: Any) -> bool:
    value = getattr(entry, "enabled", None)
    model = getattr(entry, "model", None)
    if value is None and model is not None:
        value = getattr(model, "enabled", None)
    return True if value is None else bool(value)


def _as_utc(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _non_negative_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _bounded_seconds(value: Any, *, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, minimum), maximum)


__all__ = ["ObserverPersistentScheduler"]
