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
        return SuccessResponse()


def test_transport_keeps_failed_batch_for_retry_without_spool():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cd_transport",
        ingest_url="http://ingest",
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
