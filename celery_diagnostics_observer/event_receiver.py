from __future__ import annotations

import logging
import signal
import socket
import time
from threading import Event
from typing import Any

from .config import ObserverConfig
from .policy import TelemetryPolicy
from .sanitizer import redact_url_credentials, sanitize_celery_event
from .transport import ObserverTransport


logger = logging.getLogger(__name__)


class EventReceiver:
    def __init__(self, app: Any, config: ObserverConfig, policy: TelemetryPolicy, transport: ObserverTransport):
        self.app = app
        self.config = config
        self.policy = policy
        self.transport = transport
        self.stop_event = Event()

    def install_signal_handlers(self) -> None:
        def _stop(_signum, _frame):
            self.stop_event.set()

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

    def handle_event(self, event: dict[str, Any]) -> None:
        payload = sanitize_celery_event(event, config=self.config, policy=self.policy)
        if payload is not None:
            self.transport.enqueue(payload)

    def run_forever(self) -> None:
        self.install_signal_handlers()
        backoff = 1.0
        while not self.stop_event.is_set():
            try:
                with self.app.connection() as connection:
                    receiver = self.app.events.Receiver(connection, handlers={"*": self.handle_event})
                    backoff = 1.0
                    with receiver.consumer_context(wakeup=True) as (event_connection, _channel, _consumers):
                        while not self.stop_event.is_set():
                            receiver.on_iteration()
                            try:
                                event_connection.drain_events(timeout=1.0)
                            except socket.timeout:
                                event_connection.heartbeat_check()
                            self.transport.flush_once()
            except Exception as exc:  # noqa: BLE001
                context = connection_failure_context(self.config, exc)
                logger.warning(
                    "event receiver connection failed broker=%s error=%s detail=%s retry_in=%.1fs",
                    context["broker_url"],
                    context["error_type"],
                    context["error_detail"],
                    backoff,
                )
                self.stop_event.wait(backoff)
                backoff = min(backoff * 2, 60.0)


def connection_failure_context(config: ObserverConfig, exc: Exception) -> dict[str, str]:
    safe_broker_url = redact_url_credentials(config.broker_url)
    detail = str(exc) or type(exc).__name__
    if config.broker_url:
        detail = detail.replace(config.broker_url, safe_broker_url)
    return {
        "broker_url": safe_broker_url,
        "error_type": type(exc).__name__,
        "error_detail": detail[:500],
    }


def dry_run_sample_events(config: ObserverConfig, policy: TelemetryPolicy) -> list[dict[str, Any]]:
    now = time.time()
    examples = [
        {
            "type": "task-sent",
            "uuid": "dry-run-task-id",
            "name": "example.tasks.process",
            "routing_key": config.queues[0] if config.queues else "default",
            "timestamp": now,
            "argsrepr": "(secret,)",
            "kwargsrepr": "{'token': 'secret'}",
        },
        {
            "type": "worker-heartbeat",
            "hostname": "celery@dry-run-worker",
            "timestamp": now,
        },
    ]
    return [payload for event in examples if (payload := sanitize_celery_event(event, config=config, policy=policy))]
