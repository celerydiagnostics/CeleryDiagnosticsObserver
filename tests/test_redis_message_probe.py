from __future__ import annotations

import json

from celery_diagnostics_observer.redis_message_probe import (
    redis_task_message_present,
)


class _Redis:
    def __init__(self, rows_by_key, *, mutate_after=False):
        self.rows_by_key = {
            key: list(rows) for key, rows in rows_by_key.items()
        }
        self.mutate_after = mutate_after
        self.llen_calls = {}

    def llen(self, key):
        self.llen_calls[key] = self.llen_calls.get(key, 0) + 1
        value = len(self.rows_by_key.get(key, []))
        if self.mutate_after and self.llen_calls[key] > 1:
            return value + 1
        return value

    def lrange(self, key, start, end):
        return self.rows_by_key.get(key, [])[start : end + 1]


def _message(task_id: str, *, payload: str = "private") -> bytes:
    return json.dumps(
        {
            "headers": {"id": task_id},
            "body": payload,
        }
    ).encode()


def test_identity_scan_finds_target_without_returning_payload():
    redis = _Redis({"celery": [_message("other"), _message("target")]})

    assert redis_task_message_present(redis, "celery", "target") is True


def test_identity_scan_returns_false_only_for_stable_decodable_lists():
    assert (
        redis_task_message_present(
            _Redis({"celery": [_message("other")]}),
            "celery",
            "target",
        )
        is False
    )
    assert (
        redis_task_message_present(
            _Redis({"celery": [_message("other")]}, mutate_after=True),
            "celery",
            "target",
        )
        is None
    )


def test_identity_scan_fails_closed_for_malformed_or_oversized_queues():
    assert (
        redis_task_message_present(
            _Redis({"celery": [b"not-json"]}),
            "celery",
            "target",
        )
        is None
    )
    assert (
        redis_task_message_present(
            _Redis({"celery": [_message("other"), _message("second")]}),
            "celery",
            "target",
            max_messages=1,
        )
        is None
    )
