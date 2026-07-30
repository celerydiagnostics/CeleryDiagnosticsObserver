"""Optional privacy-safe progress obligations for long-running Celery tasks."""

from __future__ import annotations

from typing import Any


def report_task_progress(
    task: Any,
    *,
    sequence: int,
    max_silence_seconds: int,
) -> bool:
    """Report forward progress without sending a business progress value.

    ``sequence`` must increase whenever the task makes meaningful forward
    progress. ``max_silence_seconds`` is the task-owned monitoring obligation,
    not a diagnostic-system guess.
    """

    if (
        isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or sequence < 0
    ):
        raise ValueError("sequence must be a non-negative integer")
    if (
        isinstance(max_silence_seconds, bool)
        or not isinstance(max_silence_seconds, int)
        or max_silence_seconds <= 0
    ):
        raise ValueError("max_silence_seconds must be a positive integer")
    send_event = getattr(task, "send_event", None)
    if not callable(send_event):
        raise TypeError("task must provide Celery Task.send_event")
    return bool(
        send_event(
            "task-progress",
            progress_sequence=sequence,
            progress_max_silence_seconds=max_silence_seconds,
            payload_redacted=True,
        )
    )


__all__ = ["report_task_progress"]
