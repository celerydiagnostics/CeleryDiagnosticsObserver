from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

import requests
from celery import signals


SCHEMA_VERSION = 2
_ACTIVE_HANDLES: list["PublisherProbeHandle"] = []
_HANDLES_BY_APP_ID: dict[int, "PublisherProbeHandle"] = {}


@dataclass
class PublisherProbeHandle:
    enabled: bool
    project_key: str
    ingest_url: str
    service_name: str = ""
    privacy: str = "balanced"
    sent_event_count: int = 0
    dropped_event_count: int = 0
    failed_send_count: int = 0
    last_error: str = ""
    max_queue_size: int = 1000
    timeout: float = 2.0
    session: Any = None
    _events: list[dict[str, Any]] | None = None
    _before_uid: str = ""
    _after_uid: str = ""
    _app_id: int | None = None

    @property
    def pending_events(self) -> list[dict[str, Any]]:
        return list(self._events or [])

    def enqueue(self, event: dict[str, Any]) -> None:
        if not self.enabled:
            return
        if self._events is None:
            self._events = []
        if len(self._events) >= self.max_queue_size:
            self.dropped_event_count += 1
            self.last_error = "queue_full"
            return
        self._events.append(event)

    def flush_once(self) -> bool:
        if not self.enabled or not self._events:
            return False
        batch = list(self._events)
        try:
            session = self.session or requests.Session()
            self.session = session
            response = session.post(
                _batch_url(self.ingest_url),
                json={"events": batch},
                headers={
                    "Authorization": f"Bearer {self.project_key}",
                    "User-Agent": "celery-diagnostics-publisher/v1",
                },
                timeout=self.timeout,
            )
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            self.failed_send_count += 1
            self.last_error = type(exc).__name__
            return False
        self.sent_event_count += len(batch)
        self._events = []
        self.last_error = ""
        return True

    def stop(self) -> None:
        if self._before_uid:
            signals.before_task_publish.disconnect(dispatch_uid=self._before_uid)
            self._before_uid = ""
        if self._after_uid:
            signals.after_task_publish.disconnect(dispatch_uid=self._after_uid)
            self._after_uid = ""
        if self in _ACTIVE_HANDLES:
            _ACTIVE_HANDLES.remove(self)
        if self._app_id is not None and _HANDLES_BY_APP_ID.get(self._app_id) is self:
            del _HANDLES_BY_APP_ID[self._app_id]


def track_task_publishing(
    app: Any,
    *,
    project_key: str | None = None,
    ingest_url: str | None = None,
    service_name: str | None = None,
    privacy: str = "balanced",
    start_transport: bool = True,
    max_queue_size: int = 1000,
) -> PublisherProbeHandle:
    resolved_project_key = _project_key(project_key)
    app_id = id(app)
    if resolved_project_key and app_id in _HANDLES_BY_APP_ID:
        return _HANDLES_BY_APP_ID[app_id]
    handle = PublisherProbeHandle(
        enabled=bool(resolved_project_key),
        project_key=resolved_project_key,
        ingest_url=_ingest_url(ingest_url),
        service_name=str(service_name or os.getenv("CD_SERVICE_NAME") or os.getenv("CELERY_DIAGNOSTICS_SERVICE_NAME") or ""),
        privacy=_privacy(privacy),
        max_queue_size=_max_queue_size(max_queue_size),
        session=requests.Session() if start_transport else None,
        _events=[],
        _app_id=app_id,
    )
    if handle.enabled:
        _connect_publish_signals(handle)
        _ACTIVE_HANDLES.append(handle)
        _HANDLES_BY_APP_ID[app_id] = handle
    return handle


def record_publish_failed(
    *,
    task_id: str,
    task_name: str = "",
    exception: BaseException | None = None,
    queue: str = "",
    exchange: str = "",
    routing_key: str = "",
    headers: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> bool:
    handle = _active_handle()
    if handle is None:
        return False
    event = _publish_failed_event(
        task_id=task_id,
        task_name=task_name,
        exception=exception,
        queue=queue,
        exchange=exchange,
        routing_key=routing_key,
        headers=headers,
        metadata=metadata,
        handle=handle,
    )
    if event is None:
        return False
    try:
        handle.enqueue(event)
        return True
    except Exception:  # noqa: BLE001
        handle.dropped_event_count += 1
        handle.last_error = "record_publish_failed"
        return False


def _connect_publish_signals(handle: PublisherProbeHandle) -> None:
    uid_base = f"celery-diagnostics-publisher-{uuid.uuid4().hex}"
    handle._before_uid = uid_base + "-before"
    handle._after_uid = uid_base + "-after"

    def _before(sender=None, headers=None, body=None, exchange=None, routing_key=None, **_kwargs):
        del body
        event = _publish_event(
            "before_task_publish",
            sender=sender,
            headers=headers,
            exchange=exchange,
            routing_key=routing_key,
            handle=handle,
        )
        if event is not None:
            handle.enqueue(event)

    def _after(sender=None, headers=None, body=None, exchange=None, routing_key=None, **_kwargs):
        del body
        event = _publish_event(
            "after_task_publish",
            sender=sender,
            headers=headers,
            exchange=exchange,
            routing_key=routing_key,
            handle=handle,
        )
        if event is not None:
            handle.enqueue(event)

    signals.before_task_publish.connect(_before, weak=False, dispatch_uid=handle._before_uid)
    signals.after_task_publish.connect(_after, weak=False, dispatch_uid=handle._after_uid)


def _publish_event(
    event_type: str,
    *,
    sender: Any,
    headers: Any,
    exchange: Any,
    routing_key: Any,
    handle: PublisherProbeHandle,
) -> dict[str, Any] | None:
    safe_headers = headers if isinstance(headers, dict) else {}
    task_id = _safe_token(safe_headers.get("id"), 255)
    if not task_id:
        return None
    task_name = _safe_token(sender or safe_headers.get("task"), 255)
    queue = _safe_token(routing_key, 255)
    routing = _safe_token(routing_key, 255)
    exchange_value = _safe_token(exchange, 255)
    event_id = f"publisher:{event_type}:{task_id}:{uuid.uuid4().hex}"
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "dedupe_key": event_id,
        "timestamp": _utc_now(),
        "event_type": event_type,
        "source": "producer",
        "service_name": _safe_token(handle.service_name, 255),
        "privacy_mode": handle.privacy,
        "task_id": task_id,
        "task_name": _label(task_name, kind="task", handle=handle),
        "queue": _label(queue, kind="queue", handle=handle),
        "routing_key": _label(routing, kind="route", handle=handle),
        "exchange": _label(exchange_value, kind="exchange", handle=handle),
        "root_id": _safe_token(safe_headers.get("root_id"), 255),
        "parent_id": _safe_token(safe_headers.get("parent_id"), 255),
        "group_id": _safe_token(safe_headers.get("group"), 255),
        "chord_id": _safe_token(safe_headers.get("chord"), 255),
        "metadata": {
            "instrumentation": "publisher_probe",
            "publisher_probe_version": "v1",
        },
    }
    return {key: value for key, value in payload.items() if value not in ("", None)}


def _publish_failed_event(
    *,
    task_id: str,
    task_name: str,
    exception: BaseException | None,
    queue: str,
    exchange: str,
    routing_key: str,
    headers: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
    handle: PublisherProbeHandle,
) -> dict[str, Any] | None:
    safe_task_id = _safe_token(task_id, 255)
    if not safe_task_id:
        return None
    safe_headers = headers if isinstance(headers, dict) else {}
    event_id = f"publisher:publish_failed:{safe_task_id}:{uuid.uuid4().hex}"
    safe_metadata = {
        "instrumentation": "publisher_probe",
        "publisher_probe_version": "v1",
    }
    for key, value in (metadata or {}).items():
        safe_key = _safe_token(key, 64)
        if safe_key and safe_key not in {"headers", "body", "args", "kwargs", "traceback"}:
            safe_metadata[safe_key] = _safe_token(value, 255)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id,
        "dedupe_key": event_id,
        "timestamp": _utc_now(),
        "event_type": "publish_failed",
        "source": "producer",
        "service_name": _safe_token(handle.service_name, 255),
        "privacy_mode": handle.privacy,
        "task_id": safe_task_id,
        "task_name": _label(_safe_token(task_name, 255), kind="task", handle=handle),
        "queue": _label(_safe_token(queue, 255), kind="queue", handle=handle),
        "routing_key": _label(_safe_token(routing_key, 255), kind="route", handle=handle),
        "exchange": _label(_safe_token(exchange, 255), kind="exchange", handle=handle),
        "root_id": _safe_token(safe_headers.get("root_id"), 255),
        "parent_id": _safe_token(safe_headers.get("parent_id"), 255),
        "group_id": _safe_token(safe_headers.get("group"), 255),
        "chord_id": _safe_token(safe_headers.get("chord"), 255),
        "exception_type": _safe_token(type(exception).__name__ if exception is not None else "", 255),
        "exception_module": _safe_token(type(exception).__module__ if exception is not None else "", 255),
        "metadata": safe_metadata,
    }
    return {key: value for key, value in payload.items() if value not in ("", None)}


def _project_key(value: str | None) -> str:
    return str(value or os.getenv("CD_PROJECT_KEY") or os.getenv("CELERY_DIAGNOSTICS_PROJECT_KEY") or "")


def _ingest_url(value: str | None) -> str:
    return str(value or os.getenv("CD_INGEST_URL") or os.getenv("CELERY_DIAGNOSTICS_INGEST_URL") or "http://127.0.0.1:8000")


def _privacy(value: str | None) -> str:
    normalized = str(value or "balanced").strip().lower()
    return normalized if normalized in {"private", "balanced"} else "balanced"


def _active_handle() -> PublisherProbeHandle | None:
    for handle in reversed(_ACTIVE_HANDLES):
        if handle.enabled:
            return handle
    return None


def _batch_url(ingest_url: str) -> str:
    return ingest_url.rstrip("/") + "/api/events/batch/"


def _max_queue_size(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 1000
    return min(max(parsed, 1), 100_000)


def _label(value: str, *, kind: str, handle: PublisherProbeHandle) -> str:
    if not value:
        return ""
    if handle.privacy != "private":
        return value
    digest = sha256(f"{handle.project_key}:{kind}:{value}".encode("utf-8", errors="replace")).hexdigest()[:16]
    return f"{kind}_{digest}"


def _safe_token(value: Any, limit: int) -> str:
    return str(value or "").replace("\x00", "").strip()[:limit]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
