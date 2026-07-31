from __future__ import annotations

import json

from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.inspect_sampler import CeleryInspectSampler
from celery_diagnostics_observer.policy import policy_from_name


class FakeInspector:
    def ping(self):
        return {"celery@worker-1": {"ok": "pong"}}

    def active_queues(self):
        return {"celery@worker-1": [{"name": "default", "routing_key": "default"}]}

    def active(self):
        return {"celery@worker-1": [{"id": "task-1", "args": ["secret"], "kwargs": {"token": "secret"}}]}

    def reserved(self):
        return {"celery@worker-1": [{"id": "task-2", "argsrepr": "('secret',)"}]}

    def scheduled(self):
        return {"celery@worker-1": [{"request": {"kwargs": {"token": "secret"}}}]}


class FakeControl:
    def inspect(self, timeout):
        assert timeout == 2.0
        return FakeInspector()


class FakeApp:
    control = FakeControl()


def test_inspect_sampler_sends_only_sanitized_worker_summary():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_inspect",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )
    sampler = CeleryInspectSampler(FakeApp(), config, policy_from_name("readable"))

    snapshots = sampler.sample_once()

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot["event_type"] == "worker_snapshot"
    assert snapshot["worker"] == "celery@worker-1"
    assert snapshot["active_queues"] == ["default"]
    assert snapshot["active_count"] == 1
    assert snapshot["reserved_count"] == 1
    assert snapshot["scheduled_count"] == 1
    rendered = json.dumps(snapshot, sort_keys=True)
    assert "secret" not in rendered
    assert "args" not in rendered
    assert "kwargs" not in rendered
