from __future__ import annotations

import logging
import socket

from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.event_receiver import EventReceiver, connection_failure_context


class _FakeConnection:
    def __init__(self, stop_event):
        self.stop_event = stop_event

    def __enter__(self):
        return self

    def __exit__(self, exc_type, _exc, _tb):
        return False


class _FakeApp:
    def __init__(self):
        self.events = None
        self.stop_event = None
        self.connection_count = 0

    def connection(self):
        self.connection_count += 1
        return _FakeConnection(self.stop_event)


class _FlushStopsTransport:
    def __init__(self, *, stop_after=1):
        self.flush_count = 0
        self.stop_event = None
        self.stop_after = stop_after

    def enqueue(self, _payload):
        return None

    def flush_once(self):
        self.flush_count += 1
        if self.flush_count >= self.stop_after:
            self.stop_event.set()
        return False


class _IdleDrainConnection:
    def __init__(self):
        self.drain_count = 0
        self.heartbeat_check_count = 0

    def drain_events(self, *, timeout):
        self.drain_count += 1
        raise socket.timeout()

    def heartbeat_check(self):
        self.heartbeat_check_count += 1


class _ConsumerContext:
    def __init__(self, receiver):
        self.receiver = receiver

    def __enter__(self):
        self.receiver.consumer_context_entry_count += 1
        return self.receiver.consumer_connection, None, ()

    def __exit__(self, _exc_type, _exc, _tb):
        self.receiver.consumer_context_exit_count += 1
        return False


class _IdleConsumerReceiver:
    def __init__(self):
        self.capture_count = 0
        self.consumer_context_entry_count = 0
        self.consumer_context_exit_count = 0
        self.consumer_context_kwargs = []
        self.iteration_count = 0
        self.consumer_connection = _IdleDrainConnection()

    def capture(self, **_kwargs):
        self.capture_count += 1
        raise socket.timeout()

    def consumer_context(self, **kwargs):
        self.consumer_context_kwargs.append(kwargs)
        return _ConsumerContext(self)

    def on_iteration(self):
        self.iteration_count += 1


class _FakeConsumerEvents:
    def __init__(self):
        self.receiver = _IdleConsumerReceiver()

    def Receiver(self, _connection, handlers):
        self.handlers = handlers
        return self.receiver


class _FakeConsumerApp(_FakeApp):
    def __init__(self):
        super().__init__()
        self.events = _FakeConsumerEvents()


def test_connection_failure_context_redacts_broker_credentials_and_drops_cause_detail():
    config = ObserverConfig(
        broker_url="redis://:secret@redis:6379/0",
        queues=("default",),
        project_key="cf_test",
        ingest_url="http://ingest",
        observer_id="observer-1",
    )
    exc = RuntimeError("Error connecting to redis://:secret@redis:6379/0: Name or service not known")

    context = connection_failure_context(config, exc)

    assert context["broker_url"] == "redis://redis:6379/0"
    assert context["error_type"] == "RuntimeError"
    assert context["error_detail"] == ""
    assert "secret" not in context["error_detail"]


def test_event_receiver_treats_idle_drain_timeout_as_normal_poll(caplog):
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cf_test",
        ingest_url="http://ingest",
        observer_id="observer-1",
    )
    app = _FakeConsumerApp()
    transport = _FlushStopsTransport()
    receiver = EventReceiver(app, config, policy=None, transport=transport)
    app.stop_event = receiver.stop_event
    transport.stop_event = receiver.stop_event
    receiver.install_signal_handlers = lambda: None

    with caplog.at_level(logging.WARNING):
        receiver.run_forever()

    celery_receiver = app.events.receiver
    assert celery_receiver.capture_count == 0
    assert celery_receiver.consumer_context_entry_count == 1
    assert celery_receiver.consumer_context_exit_count == 1
    assert celery_receiver.consumer_context_kwargs == [{"wakeup": True}]
    assert celery_receiver.iteration_count == 1
    assert celery_receiver.consumer_connection.drain_count == 1
    assert celery_receiver.consumer_connection.heartbeat_check_count == 1
    assert transport.flush_count == 1
    assert app.connection_count == 1
    assert "event receiver connection failed" not in caplog.text


def test_event_receiver_keeps_consumer_open_across_idle_polls(caplog):
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("default",),
        project_key="cf_test",
        ingest_url="http://ingest",
        observer_id="observer-1",
    )
    app = _FakeConsumerApp()
    transport = _FlushStopsTransport(stop_after=2)
    receiver = EventReceiver(app, config, policy=None, transport=transport)
    app.stop_event = receiver.stop_event
    transport.stop_event = receiver.stop_event
    receiver.install_signal_handlers = lambda: None

    with caplog.at_level(logging.WARNING):
        receiver.run_forever()

    celery_receiver = app.events.receiver
    assert celery_receiver.capture_count == 0
    assert celery_receiver.consumer_context_entry_count == 1
    assert celery_receiver.consumer_context_exit_count == 1
    assert celery_receiver.consumer_context_kwargs == [{"wakeup": True}]
    assert celery_receiver.iteration_count == 2
    assert celery_receiver.consumer_connection.drain_count == 2
    assert celery_receiver.consumer_connection.heartbeat_check_count == 2
    assert transport.flush_count == 2
    assert app.connection_count == 1
    assert "event receiver connection failed" not in caplog.text
