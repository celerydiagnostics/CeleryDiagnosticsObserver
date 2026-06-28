from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AppContextSnapshot:
    loaded: bool
    error: str = ""
    routing_config_known: bool = False
    route_count: int | None = None
    queue_count: int | None = None
    default_queue_configured: bool = False
    beat_schedule_known: bool = False
    beat_schedule_entry_count: int | None = None
    visibility_timeout: int | None = None
    task_track_started: bool | None = None


def inspect_celery_app(app: Any) -> AppContextSnapshot:
    conf = getattr(app, "conf", None)
    task_routes = _conf_get(conf, "task_routes")
    task_queues = _conf_get(conf, "task_queues")
    default_queue = _safe_string(_conf_get(conf, "task_default_queue"))
    beat_schedule = _conf_get(conf, "beat_schedule")
    broker_transport_options = _conf_get(conf, "broker_transport_options")

    route_count = _safe_count(task_routes)
    queue_count = _safe_count(task_queues)
    default_queue_configured = bool(default_queue)
    beat_schedule_count = _safe_count(beat_schedule) if beat_schedule is not None else None

    return AppContextSnapshot(
        loaded=True,
        routing_config_known=bool(default_queue_configured or (route_count or 0) > 0 or (queue_count or 0) > 0),
        route_count=route_count,
        queue_count=queue_count,
        default_queue_configured=default_queue_configured,
        beat_schedule_known=beat_schedule is not None,
        beat_schedule_entry_count=beat_schedule_count,
        visibility_timeout=_visibility_timeout(broker_transport_options),
        task_track_started=_optional_bool(_conf_get(conf, "task_track_started")),
    )


def failed_app_context(exc: BaseException) -> AppContextSnapshot:
    return AppContextSnapshot(loaded=False, error=type(exc).__name__)


def unloaded_app_context() -> AppContextSnapshot:
    return AppContextSnapshot(loaded=False)


def _conf_get(conf: Any, key: str) -> Any:
    if conf is None:
        return None
    try:
        return getattr(conf, key)
    except Exception:  # noqa: BLE001
        return None


def _visibility_timeout(options: Any) -> int | None:
    if not isinstance(options, Mapping):
        return None
    return _positive_int(options.get("visibility_timeout"))


def _positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _optional_bool(value: Any) -> bool | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return None


def _safe_count(value: Any) -> int | None:
    if value in (None, ""):
        return 0
    if isinstance(value, Mapping):
        return len(value)
    try:
        return len(value)
    except Exception:  # noqa: BLE001
        pass
    try:
        return 1 if bool(value) else 0
    except Exception:  # noqa: BLE001
        return None


def _safe_string(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        return str(value).strip()
    except Exception:  # noqa: BLE001
        return ""
