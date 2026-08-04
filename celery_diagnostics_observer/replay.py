"""Replay genuine Celery event captures through the production privacy filter."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import ObserverConfig
from .policy import TelemetryPolicy
from .sanitizer import sanitize_celery_event


@dataclass(frozen=True, slots=True)
class ReplayExport:
    read: int
    before_cutoff: int
    exported: int
    unsupported: int
    shift_seconds: float


def export_event_replay(
    source: str | Path,
    destination: str | Path,
    *,
    config: ObserverConfig,
    policy: TelemetryPolicy,
    cutoff: float,
    anchor: datetime | None = None,
) -> ReplayExport:
    """Time-shift a public capture without changing its event intervals."""

    if cutoff < 0:
        raise ValueError("cutoff must be non-negative")
    anchor = anchor or datetime.now(timezone.utc)
    if anchor.tzinfo is None:
        raise ValueError("anchor must be timezone-aware")
    shift_seconds = anchor.timestamp() - cutoff
    rows = _read_jsonl(Path(source))
    selected = [
        row
        for row in rows
        if (event_time := _event_time(row)) is not None and event_time <= cutoff
    ]
    output = []
    unsupported = 0
    for row in selected:
        shifted = _shifted_event(row, shift_seconds)
        payload = sanitize_celery_event(
            shifted,
            config=config,
            policy=policy,
        )
        if payload is None:
            unsupported += 1
            continue
        payload["research_replay"] = {
            "public_cutoff": cutoff,
            "cutoff_anchor": anchor.isoformat(),
            "event_intervals_preserved": True,
        }
        observed_at = _number(shifted.get("local_received"))
        if observed_at is not None:
            payload["observed_at"] = datetime.fromtimestamp(
                observed_at,
                tz=timezone.utc,
            ).isoformat()
        output.append(payload)
    _write_jsonl(Path(destination), output)
    return ReplayExport(
        read=len(rows),
        before_cutoff=len(selected),
        exported=len(output),
        unsupported=unsupported,
        shift_seconds=shift_seconds,
    )


def _shifted_event(row: dict[str, Any], shift_seconds: float) -> dict[str, Any]:
    value = dict(row)
    for key in ("timestamp", "local_received"):
        number = _number(value.get(key))
        if number is not None:
            value[key] = number + shift_seconds
    return value


def _event_time(row: dict[str, Any]) -> float | None:
    return _number(row.get("local_received")) or _number(row.get("timestamp"))


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    output = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number} is not a JSON object")
        output.append(value)
    return output


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(
                row,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            )
            + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
