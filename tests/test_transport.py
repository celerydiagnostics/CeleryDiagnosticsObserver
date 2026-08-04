from __future__ import annotations

import requests

from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.transport import ObserverTransport


class FailingSession:
    def post(self, *_args, **_kwargs):
        raise requests.ConnectionError("offline")


class SuccessResponse:
    def raise_for_status(self):
        return None


class SuccessSession:
    def __init__(self):
        self.calls = 0

    def post(self, *_args, **_kwargs):
        self.calls += 1
        self.kwargs = _kwargs
        return SuccessResponse()


def test_transport_keeps_failed_batch_for_retry_without_spool():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_transport",
        ingest_url="https://ingest.example",
        observer_id="obs-1",
        batch_size=10,
    )
    transport = ObserverTransport(config)
    transport.session = FailingSession()
    transport.enqueue({"schema_version": "observer.v1", "event_type": "observer_heartbeat"})

    assert transport.flush_once(force=True) is True
    assert transport.failed_send_count == 1
    assert transport.dropped_event_count == 0
    assert transport.flush_once(force=True) is False

    transport._retry_state.next_retry_at = 0
    success = SuccessSession()
    transport.session = success

    assert transport.flush_once(force=True) is True
    assert success.calls == 1
    assert transport.sent_event_count == 1


def test_transport_keeps_project_key_only_in_authorization_header(tmp_path):
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_transport_secret",
        ingest_url="https://ingest.example",
        observer_id="obs-1",
        spool_path=str(tmp_path / "observer.jsonl"),
    )
    transport = ObserverTransport(config)
    transport.session = FailingSession()
    transport.enqueue(
        {
            "schema_version": "observer.v1",
            "event_type": "observer_heartbeat",
            "project_key": "cd_transport_secret",
        }
    )

    assert transport.flush_once(force=True) is True
    assert "cd_transport_secret" not in (tmp_path / "observer.jsonl").read_text()

    transport._retry_state.next_retry_at = 0
    success = SuccessSession()
    transport.session = success
    assert transport.flush_once(force=True) is True
    assert success.kwargs["headers"]["Authorization"] == "Bearer cd_transport_secret"
    assert all("project_key" not in event for event in success.kwargs["json"]["events"])


def test_transport_rejects_cleartext_remote_ingest():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_transport",
        ingest_url="http://ingest.example",
    )

    try:
        ObserverTransport(config)
    except ValueError as error:
        assert "must use HTTPS" in str(error)
    else:
        raise AssertionError("remote cleartext ingest must be rejected")


def test_spool_permissions_are_owner_only(tmp_path):
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_transport",
        ingest_url="https://ingest.example",
        spool_path=str(tmp_path / "private" / "observer.jsonl"),
    )
    transport = ObserverTransport(config)
    transport.session = FailingSession()
    transport.enqueue({"event_type": "observer_heartbeat"})
    transport.flush_once(force=True)

    assert (tmp_path / "private").stat().st_mode & 0o777 == 0o700
    assert (tmp_path / "private" / "observer.jsonl").stat().st_mode & 0o777 == 0o600
