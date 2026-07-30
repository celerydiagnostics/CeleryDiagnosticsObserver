from __future__ import annotations

import logging
from threading import Event, Thread

from .config import ObserverConfig
from .policy import TelemetryPolicy
from .sanitizer import sanitize_observer_heartbeat


logger = logging.getLogger(__name__)


class ObserverHealthLoop:
    def __init__(
        self,
        config: ObserverConfig,
        policy: TelemetryPolicy,
        transport,
        *,
        capabilities: tuple[str, ...] = (),
        interval: float = 30.0,
    ):
        self.config = config
        self.policy = policy
        self.transport = transport
        self.capabilities = tuple(capabilities)
        self.interval = max(5.0, float(interval))
        self.stop_event = Event()
        self.thread: Thread | None = None

    def start(self) -> None:
        if self.thread is not None and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = Thread(target=self._run, name="celery-diagnostics-observer-health", daemon=True)
        self.thread.start()

    def stop(self, *, timeout: float = 2.0) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=timeout)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.transport.enqueue(
                    sanitize_observer_heartbeat(
                        config=self.config,
                        policy=self.policy,
                        transport=self.transport,
                        capabilities=self.capabilities,
                    )
                )
                self.transport.flush_once()
            except Exception as exc:  # noqa: BLE001
                logger.warning("observer health heartbeat failed error=%s", type(exc).__name__)
            self.stop_event.wait(self.interval)
