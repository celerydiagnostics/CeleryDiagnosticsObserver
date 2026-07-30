"""Bounded, read-only task identity checks for the default Kombu Redis layout."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any


KOMBU_PRIORITY_SEPARATOR = "\x06\x16"
DEFAULT_PRIORITY_STEPS = (0, 3, 6, 9)
DEFAULT_SCAN_LIMIT = 10_000


def redis_task_message_present(
    client: Any,
    queue: str,
    task_id: str,
    *,
    max_messages: int = DEFAULT_SCAN_LIMIT,
) -> bool | None:
    """Return identity presence without retaining or exposing message bodies.

    ``False`` is trustworthy only when all default priority lists were decoded
    and their lengths stayed unchanged during the bounded scan. Ambiguous,
    malformed, changing, or oversized reads return ``None``.
    """

    if not queue or not task_id:
        raise ValueError("queue and task_id are required")
    if (
        isinstance(max_messages, bool)
        or not isinstance(max_messages, int)
        or max_messages <= 0
    ):
        raise ValueError("max_messages must be a positive integer")

    keys = tuple(
        queue
        if priority == 0
        else f"{queue}{KOMBU_PRIORITY_SEPARATOR}{priority}"
        for priority in DEFAULT_PRIORITY_STEPS
    )
    depths: dict[str, int] = {}
    total = 0
    for key in keys:
        depth = client.llen(key)
        if isinstance(depth, bool) or not isinstance(depth, int) or depth < 0:
            return None
        total += depth
        if total > max_messages:
            return None
        depths[key] = depth

    for key, depth in depths.items():
        rows = client.lrange(key, 0, depth - 1) if depth else ()
        if (
            not isinstance(rows, Sequence)
            or isinstance(rows, (str, bytes, bytearray))
            or len(rows) != depth
        ):
            return None
        for row in rows:
            envelope = _envelope(row)
            if envelope is None:
                return None
            headers = envelope.get("headers")
            if not isinstance(headers, Mapping):
                return None
            candidate = headers.get("id")
            if not isinstance(candidate, str) or not candidate:
                return None
            if candidate == task_id:
                return True

    for key, depth in depths.items():
        after = client.llen(key)
        if (
            isinstance(after, bool)
            or not isinstance(after, int)
            or after != depth
        ):
            return None
    return False


def uses_default_priority_layout(app: Any) -> bool:
    conf = getattr(app, "conf", None)
    options = getattr(conf, "broker_transport_options", None)
    if not isinstance(options, Mapping):
        return True
    steps = options.get("priority_steps")
    if steps in (None, ""):
        return True
    try:
        return tuple(int(item) for item in steps) == DEFAULT_PRIORITY_STEPS
    except (TypeError, ValueError):
        return False


def _envelope(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None
    return parsed if isinstance(parsed, Mapping) else None


__all__ = [
    "DEFAULT_SCAN_LIMIT",
    "redis_task_message_present",
    "uses_default_priority_layout",
]
