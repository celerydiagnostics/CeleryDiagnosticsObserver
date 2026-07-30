from __future__ import annotations

import pytest

from celery_diagnostics import report_task_progress


class _Task:
    def __init__(self):
        self.calls = []

    def send_event(self, event_type, **fields):
        self.calls.append((event_type, fields))
        return True


def test_progress_reports_only_sequence_and_declared_silence_obligation():
    task = _Task()

    assert report_task_progress(
        task,
        sequence=7,
        max_silence_seconds=30,
    )

    assert task.calls == [
        (
            "task-progress",
            {
                "progress_sequence": 7,
                "progress_max_silence_seconds": 30,
                "payload_redacted": True,
            },
        )
    ]


@pytest.mark.parametrize(
    ("sequence", "silence"),
    [(-1, 10), (True, 10), (0, 0), (0, True)],
)
def test_progress_rejects_invalid_obligations(sequence, silence):
    with pytest.raises(ValueError):
        report_task_progress(
            _Task(),
            sequence=sequence,
            max_silence_seconds=silence,
        )
