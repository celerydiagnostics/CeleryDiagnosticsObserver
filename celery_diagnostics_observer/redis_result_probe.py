"""Status-only Celery result checks for JSON records stored in Redis."""

from __future__ import annotations

from typing import Any


_MISSING = "__CELERY_DIAGNOSTICS_MISSING__"
_INVALID = "__CELERY_DIAGNOSTICS_INVALID__"
_STATUS_ONLY_LUA = f"""
local value = redis.call('GET', KEYS[1])
if not value then
    return '{_MISSING}'
end
local ok, decoded = pcall(cjson.decode, value)
if not ok or type(decoded) ~= 'table' then
    return '{_INVALID}'
end
local status = decoded['status']
if type(status) ~= 'string' then
    return '{_INVALID}'
end
return status
""".strip()


def supports_status_only_result_probe(app: Any) -> bool:
    """Return whether status can be obtained without fetching a result value."""

    conf = getattr(app, "conf", None)
    serializer = str(
        getattr(conf, "result_serializer", "") or ""
    ).strip().lower()
    if serializer != "json":
        return False
    backend = getattr(app, "backend", None)
    client = getattr(backend, "client", None)
    return bool(
        callable(getattr(backend, "get_key_for_task", None))
        and callable(getattr(client, "eval", None))
    )


def redis_result_status(app: Any, task_id: str) -> str | None:
    """Return only the task state; keep the result record inside Redis."""

    if not supports_status_only_result_probe(app):
        return None
    backend = app.backend
    key = backend.get_key_for_task(task_id)
    value = backend.client.eval(_STATUS_ONLY_LUA, 1, key)
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError:
            return None
    state = str(value or "").strip().upper()
    if state == _MISSING:
        return "PENDING"
    if not state or state == _INVALID:
        return None
    return state


__all__ = [
    "redis_result_status",
    "supports_status_only_result_probe",
]
