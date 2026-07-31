from __future__ import annotations

import hashlib
import platform
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from .config import ObserverConfig
from .identity import identity_ref, seal_identity, task_run_ref
from .policy import TelemetryPolicy


SCHEMA_VERSION = "observer.v1"

FORBIDDEN_FIELDS = {
    "args",
    "kwargs",
    "argsrepr",
    "kwargsrepr",
    "result",
    "retval",
    "traceback",
    "einfo",
    "request",
    "body",
}

TASK_EVENT_MAP = {
    "task-sent": ("task_published", "published"),
    "task-received": ("task_received", "received"),
    "task-started": ("task_started", "started"),
    "task-progress": ("task_progress", "progress"),
    "task-succeeded": ("task_succeeded", "succeeded"),
    "task-failed": ("task_failed", "failed"),
    "task-retried": ("task_retried", "retried"),
    "task-rejected": ("task_rejected", "rejected"),
    "task-revoked": ("task_revoked", "revoked"),
}

WORKER_EVENT_MAP = {
    "worker-online": ("worker_ready", "online"),
    "worker-heartbeat": ("worker_heartbeat", "online"),
    "worker-offline": ("worker_shutdown", "offline"),
}

_EXCEPTION_TYPE_RE = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_.]*)(?:\(|:|$)")
_KNOWN_INNER_EXCEPTION_RE = re.compile(
    r"(?<![A-Za-z0-9_])((?:[A-Za-z_][A-Za-z0-9_]*\.)*"
    r"(?:SoftTimeLimitExceeded|TimeLimitExceeded|WorkerLostError))(?:\(|:|$)"
)
_WRAPPER_EXCEPTION_TYPES = {"ExceptionWithTraceback"}


def sanitize_celery_event(raw_event: dict[str, Any], *, config: ObserverConfig, policy: TelemetryPolicy) -> dict[str, Any] | None:
    if not isinstance(raw_event, dict):
        return None
    event_type = str(raw_event.get("type") or raw_event.get("event_type") or "").strip()
    if event_type in TASK_EVENT_MAP:
        return _sanitize_task_event(raw_event, event_type=event_type, config=config, policy=policy)
    if event_type in WORKER_EVENT_MAP:
        return _sanitize_worker_event(raw_event, event_type=event_type, config=config, policy=policy)
    return None


def sanitize_queue_snapshot(
    *,
    queue: str,
    messages_ready_approx: int | None,
    sampled_at: datetime | str | None,
    config: ObserverConfig,
    policy: TelemetryPolicy,
    broker_reachable: bool | None = True,
    error_type: str = "",
) -> dict[str, Any]:
    observed_at = utc_now_iso()
    label, is_hashed = _label(queue, allowed=policy.send_queue_names, kind="queue", config=config)
    return {
        "schema_version": SCHEMA_VERSION,
        "observer_id": config.observer_id,
        "observer_mode": config.mode,
        "telemetry_policy": policy.name,
        "source": "redis_queue_sampler",
        "event_type": "queue_snapshot",
        "broker_type": "redis",
        "queue": label,
        "queue_is_hashed": is_hashed,
        "messages_ready_approx": _optional_non_negative_int(messages_ready_approx),
        "messages_unacknowledged": None,
        "consumers": None,
        "sampled_at": _datetime_iso(sampled_at) or observed_at,
        "observed_at": observed_at,
        "broker_reachable": broker_reachable,
        "error_type": _safe_token(error_type, 128),
        "payload_redacted": True,
    }


def sanitize_worker_snapshot(
    *,
    worker: str,
    active_queues: list[str] | None,
    active_count: int | None,
    reserved_count: int | None,
    scheduled_count: int | None,
    sampled_at: datetime | str | None,
    config: ObserverConfig,
    policy: TelemetryPolicy,
    alive: bool = True,
) -> dict[str, Any]:
    observed_at = utc_now_iso()
    worker_label, worker_hashed = _label(worker, allowed=policy.send_worker_names, kind="worker", config=config)
    queues = []
    for queue in active_queues or []:
        queue_label, _hashed = _label(queue, allowed=policy.send_queue_names, kind="queue", config=config)
        if queue_label:
            queues.append(queue_label)
    return {
        "schema_version": SCHEMA_VERSION,
        "observer_id": config.observer_id,
        "observer_mode": config.mode,
        "telemetry_policy": policy.name,
        "source": "celery_control_inspect",
        "event_type": "worker_snapshot",
        "worker": worker_label,
        "worker_is_hashed": worker_hashed,
        "alive": bool(alive),
        "active_queues": queues,
        "active_count": _optional_non_negative_int(active_count),
        "reserved_count": _optional_non_negative_int(reserved_count),
        "scheduled_count": _optional_non_negative_int(scheduled_count),
        "sampled_at": _datetime_iso(sampled_at) or observed_at,
        "observed_at": observed_at,
        "payload_redacted": True,
    }


def sanitize_observer_heartbeat(
    *,
    config: ObserverConfig,
    policy: TelemetryPolicy,
    transport: Any | None = None,
    status: str = "healthy",
    capabilities: tuple[str, ...] = (),
) -> dict[str, Any]:
    details = {
        "python_version": platform.python_version(),
        "active_probe_capabilities": ",".join(
            sorted(
                {
                    _safe_token(item, 128)
                    for item in capabilities
                    if _safe_token(item, 128)
                }
            )
        ),
    }
    if transport is not None:
        for key in [
            "failed_send_count",
            "dropped_event_count",
            "sent_event_count",
            "spooled_event_count",
            "last_error",
        ]:
            if hasattr(transport, key):
                details[key] = getattr(transport, key)
    return {
        "schema_version": SCHEMA_VERSION,
        "observer_id": config.observer_id,
        "observer_mode": config.mode,
        "telemetry_policy": policy.name,
        "source": "observer",
        "event_type": "observer_heartbeat",
        "status": _safe_status(status),
        "observed_at": utc_now_iso(),
        "details": _safe_details(details),
        "payload_redacted": True,
    }


def redact_url_credentials(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return "[invalid-url]"
    if not parsed.username and not parsed.password:
        return value
    host = parsed.hostname or ""
    if parsed.port:
        host = f"{host}:{parsed.port}"
    return urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))


def stable_hash(value: Any, *, salt: str = "") -> str:
    payload = f"{salt}:{repr(value)}" if salt else repr(value)
    return "sha256:" + hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sanitize_task_event(
    raw_event: dict[str, Any],
    *,
    event_type: str,
    config: ObserverConfig,
    policy: TelemetryPolicy,
) -> dict[str, Any] | None:
    task_id = str(raw_event.get("uuid") or raw_event.get("id") or raw_event.get("task_id") or "").strip()
    if not task_id:
        return None
    normalized_type, state = TASK_EVENT_MAP[event_type]
    observed_at = utc_now_iso()
    task_name, task_name_hashed = _label(
        raw_event.get("name") or raw_event.get("task") or "",
        allowed=policy.send_task_names,
        kind="task",
        config=config,
    )
    queue, queue_hashed = _label(
        raw_event.get("queue") or raw_event.get("routing_key") or "",
        allowed=policy.send_queue_names,
        kind="queue",
        config=config,
    )
    routing_key, routing_key_hashed = _label(
        raw_event.get("routing_key") or "",
        allowed=policy.send_routing_labels,
        kind="route",
        config=config,
    )
    if event_type == "task-sent":
        worker, worker_hashed = "", False
    else:
        worker, worker_hashed = _label(
            raw_event.get("hostname") or raw_event.get("worker") or "",
            allowed=policy.send_worker_names,
            kind="worker",
            config=config,
        )
    exception_type, exception_module = _exception_identity(raw_event, policy=policy)
    run_ref = (
        task_run_ref(task_id, identity_key=config.identity_key)
        if policy.identities_local_only
        else ""
    )
    raw_worker = "" if event_type == "task-sent" else str(
        raw_event.get("hostname") or raw_event.get("worker") or ""
    ).strip()
    identity_capsule = (
        seal_identity(
            {
                "task_id": task_id,
                "task_name": raw_event.get("name") or raw_event.get("task") or "",
                "queue": raw_event.get("queue") or raw_event.get("routing_key") or "",
                "routing_key": raw_event.get("routing_key") or "",
                "worker": raw_worker,
            },
            identity_key=config.identity_key,
        )
        if policy.identities_local_only
        else ""
    )
    expired_revoke = event_type == "task-revoked" and _truthy(raw_event.get("expired"))
    requeued_rejection = event_type == "task-rejected" and _truthy(
        raw_event.get("requeue")
    )
    delivery_info = raw_event.get("delivery_info")
    delivery_info = delivery_info if isinstance(delivery_info, dict) else {}
    delivery_redelivered = requeued_rejection or _truthy(
        raw_event.get("redelivered") or delivery_info.get("redelivered")
    )
    metadata = {
        "celery_clock": _optional_non_negative_int(raw_event.get("clock")),
        "local_received": _datetime_iso(raw_event.get("local_received")),
    }
    if expired_revoke:
        metadata["expired"] = True
    if event_type == "task-rejected":
        metadata["requeue"] = requeued_rejection
    if event_type == "task-progress":
        metadata["progress_sequence"] = _optional_non_negative_int(
            raw_event.get("progress_sequence")
        )
        metadata["progress_max_silence_seconds"] = _optional_positive_int(
            raw_event.get("progress_max_silence_seconds")
        )
    return _strip_forbidden(
        {
            "schema_version": SCHEMA_VERSION,
            "observer_id": config.observer_id,
            "observer_mode": config.mode,
            "telemetry_policy": policy.name,
            "source": "celery_event_stream",
            "event_type": event_type,
            "normalized_event_type": normalized_type,
            "task_id": run_ref or task_id,
            "run_ref": run_ref,
            "identity_capsule": identity_capsule,
            "task_name": task_name,
            "task_name_is_hashed": task_name_hashed,
            "queue": queue,
            "queue_is_hashed": queue_hashed,
            "routing_key": routing_key,
            "routing_key_is_hashed": routing_key_hashed,
            "worker": worker,
            "worker_is_hashed": worker_hashed,
            "state": "expired" if expired_revoke else state,
            "event_timestamp": _datetime_iso(raw_event.get("timestamp")) or observed_at,
            "observed_at": observed_at,
            "root_id": _task_relation_ref(raw_event.get("root_id"), config=config, policy=policy),
            "parent_id": _task_relation_ref(raw_event.get("parent_id"), config=config, policy=policy),
            "retries": _optional_non_negative_int(raw_event.get("retries")),
            "delivery_redelivered": delivery_redelivered,
            "eta": _datetime_iso(raw_event.get("eta")),
            "runtime": _optional_float(raw_event.get("runtime")),
            "exception_type": exception_type,
            "exception_module": exception_module,
            "payload_redacted": True,
            "metadata": metadata,
        }
    )


def _sanitize_worker_event(
    raw_event: dict[str, Any],
    *,
    event_type: str,
    config: ObserverConfig,
    policy: TelemetryPolicy,
) -> dict[str, Any] | None:
    worker_label, worker_hashed = _label(
        raw_event.get("hostname") or raw_event.get("worker") or "",
        allowed=policy.send_worker_names,
        kind="worker",
        config=config,
    )
    if not worker_label:
        return None
    normalized_type, state = WORKER_EVENT_MAP[event_type]
    observed_at = utc_now_iso()
    return _strip_forbidden(
        {
            "schema_version": SCHEMA_VERSION,
            "observer_id": config.observer_id,
            "observer_mode": config.mode,
            "telemetry_policy": policy.name,
            "source": "celery_event_stream",
            "event_type": event_type,
            "normalized_event_type": normalized_type,
            "worker": worker_label,
            "worker_is_hashed": worker_hashed,
            "state": state,
            "event_timestamp": _datetime_iso(raw_event.get("timestamp")) or observed_at,
            "observed_at": observed_at,
            "payload_redacted": True,
            "metadata": {
                "celery_clock": _optional_non_negative_int(raw_event.get("clock")),
                "freq": _optional_float(raw_event.get("freq")),
            },
        }
    )


def _label(value: Any, *, allowed: bool, kind: str, config: ObserverConfig) -> tuple[str, bool]:
    raw = str(value or "").strip()
    if not raw:
        return "", False
    if allowed:
        return _safe_token(raw, 255), False
    return identity_ref(kind, raw, identity_key=config.identity_key), True


def _task_relation_ref(
    value: Any,
    *,
    config: ObserverConfig,
    policy: TelemetryPolicy,
) -> str:
    raw = _safe_token(value, 255)
    if not raw or not policy.identities_local_only:
        return raw
    return task_run_ref(raw, identity_key=config.identity_key)


def _exception_identity(raw_event: dict[str, Any], *, policy: TelemetryPolicy) -> tuple[str | None, str | None]:
    if not policy.collect_exception_type:
        return None, None
    wrapper: tuple[str | None, str | None] = (None, None)
    for key in ("exception", "exc_type"):
        parsed = _parse_exception_identity(raw_event.get(key), policy=policy)
        if not parsed[0]:
            continue
        if parsed[0] in _WRAPPER_EXCEPTION_TYPES:
            wrapper = parsed
            continue
        return parsed
    inner = _known_inner_exception_identity(raw_event, policy=policy)
    if inner[0]:
        return inner
    return wrapper


def _parse_exception_identity(value: Any, *, policy: TelemetryPolicy) -> tuple[str | None, str | None]:
    raw = str(value or "").strip()
    if not raw:
        return None, None
    match = _EXCEPTION_TYPE_RE.match(raw)
    if not match:
        return None, None
    return _split_exception_identity(match.group(1), policy=policy)


def _known_inner_exception_identity(raw_event: dict[str, Any], *, policy: TelemetryPolicy) -> tuple[str | None, str | None]:
    for key in ("exception", "result", "traceback"):
        raw = str(raw_event.get(key) or "")
        if not raw:
            continue
        match = _KNOWN_INNER_EXCEPTION_RE.search(raw)
        if match:
            return _split_exception_identity(match.group(1), policy=policy)
    return None, None


def _split_exception_identity(dotted: str, *, policy: TelemetryPolicy) -> tuple[str | None, str | None]:
    if "." in dotted and policy.collect_exception_module:
        module, _, type_name = dotted.rpartition(".")
        return _safe_token(type_name, 255), _safe_token(module, 255)
    return _safe_token(dotted.rsplit(".", 1)[-1], 255), None


def _strip_forbidden(payload: dict[str, Any]) -> dict[str, Any]:
    for field in FORBIDDEN_FIELDS:
        payload.pop(field, None)
    return payload


def _datetime_iso(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = datetime.fromtimestamp(float(text), tz=timezone.utc)
            except ValueError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _optional_non_negative_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _optional_positive_int(value: Any) -> int | None:
    parsed = _optional_non_negative_int(value)
    return parsed if parsed is not None and parsed > 0 else None


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_token(value: Any, limit: int) -> str:
    text = str(value or "").replace("\x00", "").strip()
    return text[:limit]


def _safe_status(value: Any) -> str:
    status = str(value or "healthy").strip().lower()
    return status if status in {"healthy", "stale", "lost", "unavailable"} else "healthy"


def _safe_details(value: dict[str, Any]) -> dict[str, Any]:
    details = {}
    for key, item in value.items():
        if isinstance(item, bool) or item is None:
            details[key] = item
        elif isinstance(item, int):
            details[key] = max(0, item)
        else:
            details[key] = _safe_token(
                item,
                1024 if key == "active_probe_capabilities" else 128,
            )
    return details
