from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from celery_diagnostics_observer.active_probes import ActiveProbeExecutor
from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.policy import policy_from_name


class _Inspect:
    def __init__(self, *, active=None, reserved=None, scheduled=None, queues=None, ping=None, stats=None):
        self._active = active or {}
        self._reserved = reserved or {}
        self._scheduled = scheduled or {}
        self._queues = queues or {}
        self._ping = ping or {}
        self._stats = stats or {}

    def active(self):
        return self._active

    def reserved(self):
        return self._reserved

    def scheduled(self):
        return self._scheduled

    def active_queues(self):
        return self._queues

    def ping(self):
        return self._ping

    def stats(self):
        return self._stats

    def query_task(self, task_id):
        workers = {
            *self._active,
            *self._reserved,
            *self._scheduled,
        }
        result = {worker: {} for worker in workers}
        for state, payload in [
            ("active", self._active),
            ("reserved", self._reserved),
            ("ready", self._scheduled),
        ]:
            for worker, rows in payload.items():
                for row in rows:
                    candidate = row.get("request", row) if isinstance(row, dict) else {}
                    if str(candidate.get("id") or candidate.get("uuid") or "") == task_id:
                        result[worker][task_id] = [state, candidate]
        return result


class _Control:
    def __init__(self, inspect):
        self.value = inspect
        self.calls = []

    def inspect(self, **kwargs):
        self.calls.append(kwargs)
        return self.value


class _ResultClient:
    def __init__(self):
        self.calls = []

    def eval(self, script, key_count, key):
        self.calls.append((script, key_count, key))
        return b"SUCCESS"


class _Backend:
    def __init__(self):
        self.client = _ResultClient()

    def get_key_for_task(self, task_id):
        return f"celery-task-meta-{task_id}".encode()


class _Conf:
    result_serializer = "json"


class _App:
    def __init__(self, inspect):
        self.control = _Control(inspect)
        self.backend = _Backend()
        self.conf = _Conf()


def _executor(
    inspect: _Inspect,
    *,
    policy_name: str = "detailed",
    redis_client_factory=None,
) -> ActiveProbeExecutor:
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("emails",),
        project_key="cf_test",
        ingest_url="http://backend",
        observer_id="observer-1",
        telemetry_policy=policy_name,
    )
    return ActiveProbeExecutor(
        _App(inspect),
        config,
        policy_from_name(policy_name),
        redis_client_factory=redis_client_factory,
    )


def _request(capability: str, **values):
    return {
        "request_id": "request-1",
        "capability": capability,
        "task_id": "task-1",
        "queue": "emails",
        "expected_workers": ["worker-a", "worker-b"],
        "timeout_seconds": 2.0,
        **values,
    }


def test_positive_task_match_is_reliable_even_when_other_workers_do_not_reply():
    executor = _executor(
        _Inspect(active={"worker-a": [{"id": "task-1"}]})
    )

    result = executor.execute(_request("celery.inspect.query_task"))

    assert result["outcome_id"] == "active"
    assert result["complete"] is True
    assert result["matched_location"] == "active"
    assert len(executor.app.control.calls) == 1


def test_protected_worker_alias_uses_broadcast_and_accepts_positive_match():
    executor = _executor(
        _Inspect(active={"celery@worker-host": [{"id": "task-1"}]})
    )

    result = executor.execute(
        {
            **_request("celery.inspect.query_task"),
            "expected_workers": ["WORKER_2c7fc0b2d11b"],
        }
    )

    assert result["outcome_id"] == "active"
    assert result["complete"] is True
    assert "destination" not in executor.app.control.calls[0]


def test_negative_task_result_requires_all_expected_worker_replies():
    executor = _executor(
        _Inspect(
            active={"worker-a": []},
            reserved={"worker-a": []},
            scheduled={"worker-a": []},
        )
    )

    result = executor.execute(_request("celery.inspect.query_task"))

    assert "outcome_id" not in result
    assert result["complete"] is False
    assert result["error_type"] == "partial_reply"


def test_non_active_query_match_is_reliable_without_three_control_round_trips():
    executor = _executor(
        _Inspect(reserved={"worker-a": [{"id": "task-1"}]})
    )

    result = executor.execute(_request("celery.inspect.query_task"))

    assert result["outcome_id"] == "absent"
    assert result["complete"] is True
    assert result["matched_location"] == "reserved"
    assert len(executor.app.control.calls) == 1


def test_no_consumers_is_reliable_only_with_complete_topology_replies():
    executor = _executor(
        _Inspect(
            queues={
                "worker-a": [{"name": "other"}],
                "worker-b": [{"name": "other"}],
            }
        )
    )

    result = executor.execute(_request("celery.broker.queue_consumers"))

    assert result["outcome_id"] == "no_consumers"
    assert result["complete"] is True
    assert result["queue"] == "emails"


def test_result_backend_returns_only_state_from_server_side_redis_filter():
    executor = _executor(_Inspect())

    result = executor.execute(_request("celery.result_backend.status"))

    assert result["outcome_id"] == "terminal"
    assert result["state"] == "SUCCESS"
    assert result["complete"] is True
    [(script, key_count, key)] = executor.app.backend.client.calls
    assert "redis.call('GET', KEYS[1])" in script
    assert key_count == 1
    assert key == b"celery-task-meta-task-1"


def test_schedule_probe_uses_only_eta_metadata():
    executor = _executor(_Inspect())
    eta = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()

    result = executor.execute(
        _request("celery.inspect.task_schedule", eta=eta)
    )

    assert result["outcome_id"] == "eligibility_future"
    assert result["complete"] is True


class _Redis:
    def __init__(self, messages):
        self.messages = list(messages)
        self.closed = False

    def llen(self, key):
        return len(self.messages) if key == "emails" else 0

    def lrange(self, key, start, end):
        return self.messages[start : end + 1] if key == "emails" else []

    def close(self):
        self.closed = True


def test_broker_probe_compares_only_protocol_identity_and_closes_client():
    client = _Redis(
        [
            json.dumps(
                {
                    "headers": {"id": "task-1"},
                    "body": "private-payload",
                }
            ).encode()
        ]
    )
    executor = _executor(
        _Inspect(),
        redis_client_factory=lambda _url: client,
    )

    result = executor.execute(
        _request("celery.broker.message_presence")
    )

    assert result["outcome_id"] == "present"
    assert result["complete"] is True
    assert "private-payload" not in json.dumps(result)
    assert client.closed is True


def test_balanced_policy_matches_hashed_worker_replies_without_leaking_name():
    executor = _executor(
        _Inspect(
            active={"worker-secret": []},
            reserved={"worker-secret": []},
            scheduled={"worker-secret": []},
        ),
        policy_name="balanced",
    )
    worker_label = executor._worker_label("worker-secret")

    result = executor.execute(
        {
            **_request("celery.inspect.query_task"),
            "expected_workers": [worker_label],
        }
    )

    assert result["outcome_id"] == "absent"
    assert result["complete"] is True
    assert "worker-secret" not in json.dumps(result)
