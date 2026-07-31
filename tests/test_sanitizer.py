from __future__ import annotations

import json

from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.identity import open_identity
from celery_diagnostics_observer.policy import policy_from_name
from celery_diagnostics_observer.redis_sampler import RedisQueueSampler
from celery_diagnostics_observer.sanitizer import sanitize_celery_event, sanitize_queue_snapshot


def test_local_only_policy_references_identifiers_and_strips_forbidden_fields():
    config = ObserverConfig(
        broker_url="redis://:secret@redis:6379/0",
        queues=("private-queue",),
        project_key="cd_secret",
        ingest_url="http://ingest",
        observer_id="obs-1",
        identity_key="customer-owned-identity-key",
    )
    policy = policy_from_name("local-only")

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
    assert payload["task_id"].startswith("R-")
    assert payload["run_ref"] == payload["task_id"]
    assert payload["task_name"].startswith("T-")
    assert payload["task_name_is_hashed"] is True
    assert payload["queue"].startswith("Q-")
    assert payload["queue_is_hashed"] is True
    assert payload["worker"].startswith("W-")
    assert payload["worker_is_hashed"] is True
    assert payload["exception_type"] == "CardDeclined"
    assert payload["exception_module"] is None
    assert "project_key" not in payload
    identity = open_identity(
        payload["identity_capsule"],
        identity_key=config.identity_key,
    )
    assert identity == {
        "task_id": "task-1",
        "task_name": "billing.tasks.charge_customer",
        "queue": "private-queue",
        "routing_key": "private-queue",
        "worker": "celery@worker-1",
    }
    rendered = json.dumps(payload, sort_keys=True)
    assert "customer-secret" not in rendered
    assert "token" not in rendered
    assert "4242" not in rendered


def test_readable_policy_sends_operational_identifiers():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )
    policy = policy_from_name("readable")

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
    assert payload["worker"] == "celery@email-worker"
    assert payload["worker_is_hashed"] is False
    assert payload["task_id"] == "task-2"
    assert payload["run_ref"] == ""
    assert payload["identity_capsule"] == ""
    assert "project_key" not in payload


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
        policy=policy_from_name("readable"),
    )

    assert payload is not None
    assert payload["normalized_event_type"] == "task_published"
    assert payload["queue"] == "emails"
    assert payload["worker"] == ""
    assert payload["worker_is_hashed"] is False


def test_task_failed_unwraps_time_limit_from_exception_with_traceback_result():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )

    payload = sanitize_celery_event(
        {
            "type": "task-failed",
            "uuid": "task-hard-time-limit",
            "name": "diagnostics.hard_timeout",
            "routing_key": "default",
            "hostname": "celery@worker-1",
            "exc_type": "billiard.einfo.ExceptionWithTraceback",
            "result": "billiard.exceptions.TimeLimitExceeded(1,)",
        },
        config=config,
        policy=policy_from_name("readable"),
    )

    assert payload is not None
    assert payload["exception_type"] == "TimeLimitExceeded"
    assert payload["exception_module"] == "billiard.exceptions"
    rendered = json.dumps(payload, sort_keys=True)
    assert "TimeLimitExceeded(1,)" not in rendered


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
        policy=policy_from_name("readable"),
    )

    assert payload is not None
    assert payload["normalized_event_type"] == "task_revoked"
    assert payload["state"] == "expired"
    assert payload["metadata"]["expired"] is True


def test_requeued_rejection_preserves_delivery_semantics_without_payload():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )

    payload = sanitize_celery_event(
        {
            "type": "task-rejected",
            "uuid": "task-redelivered",
            "name": "billing.tasks.charge_customer",
            "routing_key": "default",
            "hostname": "celery@worker-1",
            "requeue": True,
            "argsrepr": "('customer-secret',)",
            "kwargsrepr": "{'token': 'secret'}",
            "result": {"card": "4242"},
        },
        config=config,
        policy=policy_from_name("readable"),
    )

    assert payload is not None
    assert payload["normalized_event_type"] == "task_rejected"
    assert payload["state"] == "rejected"
    assert payload["delivery_redelivered"] is True
    assert payload["metadata"]["requeue"] is True


def test_received_event_preserves_broker_redelivery_marker():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )

    payload = sanitize_celery_event(
        {
            "type": "task-received",
            "uuid": "task-redelivered",
            "name": "billing.tasks.charge_customer",
            "hostname": "celery@worker-2",
            "delivery_info": {
                "routing_key": "default",
                "redelivered": True,
            },
            "retries": 0,
        },
        config=config,
        policy=policy_from_name("readable"),
    )

    assert payload is not None
    assert payload["event_type"] == "task-received"
    assert payload["normalized_event_type"] == "task_received"
    assert payload["delivery_redelivered"] is True
    rendered = json.dumps(payload, sort_keys=True)
    assert "customer-secret" not in rendered
    assert "token" not in rendered
    assert "4242" not in rendered


def test_public_delivery_fact_preserves_attempt_discriminators_only():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )

    payload = sanitize_celery_event(
        {
            "type": "task-delivery-fact",
            "uuid": "task-redelivered",
            "hostname": "celery@worker-2",
            "application_retry_index": 1,
            "delivery_info": {
                "routing_key": "default",
                "redelivered": True,
                "priority": 0,
            },
            "execution_pid": 12345,
            "args": ["private"],
        },
        config=config,
        policy=policy_from_name("readable"),
    )

    assert payload is not None
    assert payload["event_type"] == "task-received"
    assert payload["normalized_event_type"] == "task_received"
    assert payload["queue"] == "default"
    assert payload["retries"] == 1
    assert payload["delivery_redelivered"] is True
    rendered = json.dumps(payload, sort_keys=True)
    assert "execution_pid" not in rendered
    assert '"args"' not in rendered


def test_task_progress_keeps_only_sequence_and_declared_silence_obligation():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_balanced",
        ingest_url="http://ingest",
        observer_id="obs-1",
    )

    payload = sanitize_celery_event(
        {
            "type": "task-progress",
            "uuid": "task-progress-1",
            "hostname": "worker-private",
            "progress_sequence": 8,
            "progress_max_silence_seconds": 30,
            "progress_value": {"customer": "private"},
            "args": ["private"],
        },
        config=config,
        policy=policy_from_name("readable"),
    )

    assert payload is not None
    assert payload["normalized_event_type"] == "task_progress"
    assert payload["metadata"]["progress_sequence"] == 8
    assert payload["metadata"]["progress_max_silence_seconds"] == 30
    rendered = json.dumps(payload)
    assert "progress_value" not in rendered
    assert "customer" not in rendered
    assert '"private"' not in rendered


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
        policy=policy_from_name("readable"),
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
    sampler = RedisQueueSampler(config, policy_from_name("readable"), client_factory=lambda _url: fake)

    snapshots = sampler.sample_once()

    assert [snapshot["messages_ready_approx"] for snapshot in snapshots] == [7, 7]
    assert fake.calls == [("llen", "default"), ("llen", "emails"), ("close", None)]
