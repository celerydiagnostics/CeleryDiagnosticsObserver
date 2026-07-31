from __future__ import annotations

from celery_diagnostics_observer.capabilities import active_probe_capabilities
from celery_diagnostics_observer.policy import policy_from_name
from celery_diagnostics_observer.sanitizer import sanitize_observer_heartbeat
from celery_diagnostics_observer.config import ObserverConfig


class _Client:
    def eval(self, *_args):
        return b"PENDING"


class _Backend:
    client = _Client()

    def get_key_for_task(self, task_id):
        return task_id


class _Conf:
    result_serializer = "json"


class _App:
    backend = _Backend()
    conf = _Conf()


def test_readable_observer_advertises_only_implemented_read_only_capabilities():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("celery",),
        project_key="cd_test",
        ingest_url="http://backend",
    )
    capabilities = active_probe_capabilities(
        _App(),
        config,
        policy_from_name("readable"),
    )

    assert "celery.inspect.query_task" in capabilities
    assert "celery.inspect.worker_capacity" in capabilities
    assert "celery.broker.queue_consumers" in capabilities
    assert "celery.result_backend.status" in capabilities
    assert "celery.broker.message_presence" in capabilities
    assert "celery.worker.process_status" not in capabilities


def test_result_status_is_not_advertised_for_payload_fetching_backend():
    class App:
        backend = object()
        conf = _Conf()

    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("celery",),
        project_key="cd_test",
        ingest_url="http://backend",
    )

    capabilities = active_probe_capabilities(
        App(),
        config,
        policy_from_name("readable"),
    )

    assert "celery.result_backend.status" not in capabilities


def test_result_status_is_not_advertised_for_non_json_serializer():
    class Conf:
        result_serializer = "pickle"

    class App:
        backend = _Backend()
        conf = Conf()

    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("celery",),
        project_key="cd_test",
        ingest_url="http://backend",
    )

    capabilities = active_probe_capabilities(
        App(),
        config,
        policy_from_name("readable"),
    )

    assert "celery.result_backend.status" not in capabilities


def test_active_checks_are_independent_of_payload_detail_policy():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("celery",),
        project_key="cd_test",
        ingest_url="http://backend",
    )

    capabilities = active_probe_capabilities(
        _App(),
        config,
        policy_from_name("readable"),
    )

    assert "celery.inspect.query_task" in capabilities
    assert "celery.broker.message_presence" in capabilities


def test_active_checks_can_be_disabled_explicitly():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("celery",),
        project_key="cd_test",
        ingest_url="http://backend",
        active_probes=False,
    )

    assert (
        active_probe_capabilities(
            _App(),
            config,
            policy_from_name("readable"),
        )
        == ()
    )


def test_heartbeat_carries_a_sanitized_capability_manifest():
    payload = sanitize_observer_heartbeat(
        config=ObserverConfig(
            broker_url="redis://redis:6379/0",
            queues=("celery",),
            project_key="cd_test",
            ingest_url="http://backend",
            observer_id="observer-1",
        ),
        policy=policy_from_name("readable"),
        capabilities=("celery.inspect.query_task", "celery.result_backend.status"),
    )

    assert payload["details"]["active_probe_capabilities"] == (
        "celery.inspect.query_task,celery.result_backend.status"
    )


def test_heartbeat_does_not_truncate_the_declared_capability_manifest():
    capabilities = (
        "celery.broker.message_presence",
        "celery.broker.queue_consumers",
        "celery.inspect.query_task",
        "celery.inspect.task_reservation",
        "celery.inspect.task_schedule",
        "celery.inspect.worker_capacity",
        "celery.inspect.worker_presence",
        "celery.result_backend.status",
    )

    payload = sanitize_observer_heartbeat(
        config=ObserverConfig(
            broker_url="redis://redis:6379/0",
            queues=("celery",),
            project_key="cd_test",
            ingest_url="http://backend",
            observer_id="observer-1",
        ),
        policy=policy_from_name("readable"),
        capabilities=capabilities,
    )

    manifest = payload["details"]["active_probe_capabilities"]
    assert set(manifest.split(",")) == set(capabilities)
    assert manifest.endswith("celery.result_backend.status")
