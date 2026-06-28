from __future__ import annotations

import json

from celery import Celery
from celery import signals


def test_track_task_publishing_without_project_key_returns_disabled_handle(monkeypatch):
    from celery_diagnostics.publisher import track_task_publishing

    monkeypatch.delenv("CD_PROJECT_KEY", raising=False)
    monkeypatch.delenv("CELERY_DIAGNOSTICS_PROJECT_KEY", raising=False)
    app = Celery("publisher-test", broker="memory://")

    handle = track_task_publishing(app)

    assert handle.enabled is False
    assert handle.project_key == ""
    assert handle.sent_event_count == 0
    handle.stop()


def test_track_task_publishing_prefers_cd_project_key(monkeypatch):
    from celery_diagnostics.publisher import track_task_publishing

    monkeypatch.setenv("CD_PROJECT_KEY", "cd_primary")
    monkeypatch.setenv("CELERY_DIAGNOSTICS_PROJECT_KEY", "cd_legacy")
    app = Celery("publisher-test", broker="memory://")

    handle = track_task_publishing(app, start_transport=False)

    assert handle.enabled is True
    assert handle.project_key == "cd_primary"
    handle.stop()


def test_track_task_publishing_is_idempotent_per_app(monkeypatch):
    from celery_diagnostics.publisher import track_task_publishing

    monkeypatch.setenv("CD_PROJECT_KEY", "cd_once")
    app = Celery("publisher-test", broker="memory://")
    first = track_task_publishing(app, start_transport=False)
    second = track_task_publishing(app, start_transport=False)

    try:
        signals.after_task_publish.send(
            sender="demo.task",
            headers={"id": "task-once-1", "task": "demo.task"},
            body=None,
            exchange="celery",
            routing_key="default",
        )
    finally:
        first.stop()

    assert second is first
    assert [event["task_id"] for event in first.pending_events] == ["task-once-1"]


def test_before_task_publish_signal_queues_sanitized_publish_attempt(monkeypatch):
    from celery_diagnostics.publisher import track_task_publishing

    monkeypatch.setenv("CD_PROJECT_KEY", "cd_signal")
    app = Celery("publisher-test", broker="memory://")
    handle = track_task_publishing(app, service_name="api", start_transport=False)
    headers = {
        "id": "task-before-1",
        "task": "billing.tasks.charge",
        "root_id": "root-1",
        "parent_id": "parent-1",
        "authorization": "secret-header",
    }

    try:
        signals.before_task_publish.send(
            sender="billing.tasks.charge",
            headers=headers,
            body=(["secret-arg"], {"token": "secret-kwarg"}),
            exchange="celery",
            routing_key="billing",
        )
    finally:
        handle.stop()

    event = handle.pending_events[-1]
    assert event["schema_version"] == 2
    assert event["event_type"] == "before_task_publish"
    assert event["source"] == "producer"
    assert event["service_name"] == "api"
    assert event["task_id"] == "task-before-1"
    assert event["task_name"] == "billing.tasks.charge"
    assert event["queue"] == "billing"
    assert event["routing_key"] == "billing"
    assert event["exchange"] == "celery"
    assert event["root_id"] == "root-1"
    assert event["parent_id"] == "parent-1"
    assert event["metadata"]["instrumentation"] == "publisher_probe"
    rendered = json.dumps(event, sort_keys=True)
    assert "secret-arg" not in rendered
    assert "secret-kwarg" not in rendered
    assert "secret-header" not in rendered
    assert "headers" not in event
    assert "body" not in event


def test_after_task_publish_signal_queues_broker_accepted_event(monkeypatch):
    from celery_diagnostics.publisher import track_task_publishing

    monkeypatch.setenv("CD_PROJECT_KEY", "cd_signal")
    app = Celery("publisher-test", broker="memory://")
    handle = track_task_publishing(app, start_transport=False)

    try:
        signals.after_task_publish.send(
            sender="reports.tasks.build",
            headers={"id": "task-after-1", "task": "reports.tasks.build"},
            body=None,
            exchange="reports",
            routing_key="reports.low",
        )
    finally:
        handle.stop()

    event = handle.pending_events[-1]
    assert event["event_type"] == "after_task_publish"
    assert event["task_id"] == "task-after-1"
    assert event["task_name"] == "reports.tasks.build"
    assert event["queue"] == "reports.low"


def test_private_privacy_hashes_task_and_routing_labels(monkeypatch):
    from celery_diagnostics.publisher import track_task_publishing

    monkeypatch.setenv("CD_PROJECT_KEY", "cd_private")
    app = Celery("publisher-test", broker="memory://")
    handle = track_task_publishing(app, privacy="private", start_transport=False)

    try:
        signals.before_task_publish.send(
            sender="billing.tasks.charge",
            headers={"id": "task-private-1", "task": "billing.tasks.charge"},
            body=None,
            exchange="celery",
            routing_key="billing",
        )
    finally:
        handle.stop()

    event = handle.pending_events[-1]
    assert event["task_name"].startswith("task_")
    assert event["queue"].startswith("queue_")
    assert event["routing_key"].startswith("route_")
    assert event["exchange"].startswith("exchange_")
    rendered = json.dumps(event, sort_keys=True)
    assert "billing.tasks.charge" not in rendered
    assert "billing" not in rendered


def test_flush_once_posts_events_and_updates_counters(monkeypatch):
    from celery_diagnostics.publisher import track_task_publishing

    class Response:
        def raise_for_status(self):
            return None

    class Session:
        def __init__(self):
            self.calls = []

        def post(self, url, *, json, headers, timeout):
            self.calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
            return Response()

    monkeypatch.setenv("CD_PROJECT_KEY", "cd_transport")
    session = Session()
    app = Celery("publisher-test", broker="memory://")
    handle = track_task_publishing(app, ingest_url="https://ingest.example.test", start_transport=False)
    handle.session = session
    try:
        signals.after_task_publish.send(
            sender="reports.tasks.build",
            headers={"id": "task-flush-1", "task": "reports.tasks.build"},
            body=None,
            exchange="reports",
            routing_key="reports.low",
        )
        flushed = handle.flush_once()
    finally:
        handle.stop()

    assert flushed is True
    assert handle.sent_event_count == 1
    assert handle.pending_events == []
    assert session.calls[0]["url"] == "https://ingest.example.test/api/events/batch/"
    assert session.calls[0]["headers"]["Authorization"] == "Bearer cd_transport"
    assert session.calls[0]["json"]["events"][0]["task_id"] == "task-flush-1"


def test_flush_once_failure_is_fail_open_and_keeps_events(monkeypatch):
    from celery_diagnostics.publisher import track_task_publishing

    class Response:
        def raise_for_status(self):
            raise RuntimeError("network-secret")

    class Session:
        def post(self, *_args, **_kwargs):
            return Response()

    monkeypatch.setenv("CD_PROJECT_KEY", "cd_transport")
    app = Celery("publisher-test", broker="memory://")
    handle = track_task_publishing(app, start_transport=False)
    handle.session = Session()
    try:
        signals.after_task_publish.send(
            sender="reports.tasks.build",
            headers={"id": "task-fail-open-1", "task": "reports.tasks.build"},
            body=None,
            exchange="reports",
            routing_key="reports.low",
        )
        flushed = handle.flush_once()
    finally:
        handle.stop()

    assert flushed is False
    assert handle.failed_send_count == 1
    assert handle.last_error == "RuntimeError"
    assert len(handle.pending_events) == 1


def test_record_publish_failed_queues_sanitized_failure_event(monkeypatch):
    from celery_diagnostics.publisher import record_publish_failed, track_task_publishing

    monkeypatch.setenv("CD_PROJECT_KEY", "cd_failed")
    app = Celery("publisher-test", broker="memory://")
    handle = track_task_publishing(app, start_transport=False)

    try:
        recorded = record_publish_failed(
            task_id="task-publish-failed-1",
            task_name="billing.tasks.charge",
            exception=RuntimeError("broker password secret"),
            queue="billing",
            exchange="celery",
            routing_key="billing",
            headers={"authorization": "secret-header"},
        )
    finally:
        handle.stop()

    assert recorded is True
    event = handle.pending_events[-1]
    assert event["event_type"] == "publish_failed"
    assert event["task_id"] == "task-publish-failed-1"
    assert event["task_name"] == "billing.tasks.charge"
    assert event["exception_type"] == "RuntimeError"
    assert event["exception_module"] == "builtins"
    rendered = json.dumps(event, sort_keys=True)
    assert "broker password secret" not in rendered
    assert "secret-header" not in rendered


def test_record_publish_failed_without_active_handle_returns_false():
    from celery_diagnostics.publisher import record_publish_failed

    assert record_publish_failed(task_id="task-no-handle", task_name="demo.task") is False
