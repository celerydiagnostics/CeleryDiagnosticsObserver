from __future__ import annotations

import json

from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.policy import policy_from_name
from celery_diagnostics_observer.redis_sampler import RedisQueueSampler
from celery_diagnostics_observer.sanitizer import sanitize_celery_event, sanitize_queue_snapshot


def test_private_policy_hashes_task_queue_and_worker_and_strips_forbidden_fields():
    config = ObserverConfig(
        broker_url="redis://:secret@redis:6379/0",
        queues=("private-queue",),
        project_key="cd_secret",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )
    policy = policy_from_name("private")

    payload = sanitize_celery_event(
        {
            "type": "task-failed",
            "uuid": "task-1",
            "name": "billing.tasks.charge_customer",
            "routing_key": "private-queue",
            "hostname": "celery@worker-1",
            "exception": "billing.errors.CardDeclined('card 4242')",
            "args": ["customer-secret"],
            "kwargs": {"token": "secret"},
            "argsrepr": "('customer-secret',)",
            "kwargsrepr": "{'token': 'secret'}",
            "result": {"secret": True},
        },
        config=config,
        policy=policy,
    )

    assert payload is not None
    assert payload["task_name"].startswith("task_")
    assert payload["task_name_is_hashed"] is True
    assert payload["queue"].startswith("queue_")
    assert payload["queue_is_hashed"] is True
    assert payload["worker"].startswith("worker_")
    assert payload["worker_is_hashed"] is True
    assert payload["exception_type"] == "CardDeclined"
    assert payload["exception_module"] is None
    rendered = json.dumps(payload, sort_keys=True)
    assert "customer-secret" not in rendered
    assert "token" not in rendered
    assert "4242" not in rendered


def test_balanced_policy_sends_task_and_queue_but_hashes_worker():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )
    policy = policy_from_name("balanced")

    payload = sanitize_celery_event(
        {
            "type": "task-started",
            "uuid": "task-2",
            "name": "billing.tasks.send_email",
            "routing_key": "emails",
            "hostname": "celery@email-worker",
        },
        config=config,
        policy=policy,
    )

    assert payload is not None
    assert payload["task_name"] == "billing.tasks.send_email"
    assert payload["task_name_is_hashed"] is False
    assert payload["queue"] == "emails"
    assert payload["queue_is_hashed"] is False
    assert payload["worker"].startswith("worker_")
    assert payload["worker_is_hashed"] is True


def test_task_sent_does_not_treat_producer_hostname_as_worker():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("emails",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )

    payload = sanitize_celery_event(
        {
            "type": "task-sent",
            "uuid": "task-published",
            "name": "billing.tasks.send_email",
            "routing_key": "emails",
            "hostname": "web@producer-1",
        },
        config=config,
        policy=policy_from_name("balanced"),
    )

    assert payload is not None
    assert payload["normalized_event_type"] == "task_published"
    assert payload["queue"] == "emails"
    assert payload["worker"] == ""
    assert payload["worker_is_hashed"] is False


def test_expired_revoke_preserves_expired_hint():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )

    payload = sanitize_celery_event(
        {
            "type": "task-revoked",
            "uuid": "task-expired",
            "name": "billing.tasks.expire_me",
            "routing_key": "default",
            "hostname": "celery@worker-1",
            "expired": True,
        },
        config=config,
        policy=policy_from_name("balanced"),
    )

    assert payload is not None
    assert payload["normalized_event_type"] == "task_revoked"
    assert payload["state"] == "expired"
    assert payload["metadata"]["expired"] is True


def test_queue_snapshot_represents_redis_consumers_as_unknown():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )
    payload = sanitize_queue_snapshot(
        queue="default",
        messages_ready_approx=42,
        sampled_at=None,
        config=config,
        policy=policy_from_name("balanced"),
    )

    assert payload["messages_ready_approx"] == 42
    assert payload["messages_unacknowledged"] is None
    assert payload["consumers"] is None


def test_redis_sampler_uses_llen_only():
    class FakeRedis:
        def __init__(self):
            self.calls = []

        def llen(self, queue):
            self.calls.append(("llen", queue))
            return 7

        def lrange(self, *_args):
            raise AssertionError("LRANGE must not be used")

        def lpop(self, *_args):
            raise AssertionError("LPOP must not be used")

        def close(self):
            self.calls.append(("close", None))

    fake = FakeRedis()
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default", "emails"),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )
    sampler = RedisQueueSampler(config, policy_from_name("balanced"), client_factory=lambda _url: fake)

    snapshots = sampler.sample_once()

    assert [snapshot["messages_ready_approx"] for snapshot in snapshots] == [7, 7]
    assert fake.calls == [("llen", "default"), ("llen", "emails"), ("close", None)]
