from __future__ import annotations

import logging
from datetime import datetime, timezone
from threading import Event, Thread
from typing import Any

from .config import ObserverConfig
from .policy import TelemetryPolicy
from .sanitizer import sanitize_worker_snapshot


logger = logging.getLogger(__name__)


class CeleryInspectSampler:
    def __init__(self, app: Any, config: ObserverConfig, policy: TelemetryPolicy):
        self.app = app
        self.config = config
        self.policy = policy

    def sample_once(self) -> list[dict[str, Any]]:
        if not self.policy.enable_control_inspect:
            return []
        sampled_at = datetime.now(timezone.utc)
        inspector = self.app.control.inspect(timeout=2.0)
        try:
            ping = inspector.ping() or {}
            active_queues = inspector.active_queues() or {}
            active = inspector.active() or {}
            reserved = inspector.reserved() or {}
            scheduled = inspector.scheduled() or {}
        except Exception as exc:  # noqa: BLE001
            logger.warning("celery inspect failed error=%s", type(exc).__name__)
            return []

        workers = sorted(
            {
                *[str(worker) for worker in ping.keys()],
                *[str(worker) for worker in active_queues.keys()],
                *[str(worker) for worker in active.keys()],
                *[str(worker) for worker in reserved.keys()],
                *[str(worker) for worker in scheduled.keys()],
            }
        )
        snapshots = []
        for worker in workers:
            snapshots.append(
                sanitize_worker_snapshot(
                    worker=worker,
                    active_queues=_queue_names(active_queues.get(worker)),
                    active_count=_inspect_count(active.get(worker)),
                    reserved_count=_inspect_count(reserved.get(worker)),
                    scheduled_count=_inspect_count(scheduled.get(worker)),
                    sampled_at=sampled_at,
                    config=self.config,
                    policy=self.policy,
                    alive=worker in ping,
                )
            )
        return snapshots


class CeleryInspectSamplerLoop:
    def __init__(self, sampler: CeleryInspectSampler, transport):
        self.sampler = sampler
        self.transport = transport
        self.stop_event = Event()
        self.thread: Thread | None = None

    def start(self) -> None:
        if not self.sampler.policy.enable_control_inspect:
            return
        if self.thread is not None and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = Thread(target=self._run, name="celery-diagnostics-inspect-sampler", daemon=True)
        self.thread.start()

    def stop(self, *, timeout: float = 2.0) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=timeout)

    def _run(self) -> None:
        interval = self.sampler.config.inspect_interval
        while not self.stop_event.is_set():
            try:
                for snapshot in self.sampler.sample_once():
                    self.transport.enqueue(snapshot)
                self.transport.flush_once()
            except Exception as exc:  # noqa: BLE001
                logger.warning("celery inspect sampler failed error=%s", type(exc).__name__)
            self.stop_event.wait(interval)


def _queue_names(payload: Any) -> list[str]:
    if not isinstance(payload, list):
        return []
    names = []
    for item in payload:
        if isinstance(item, dict):
            name = str(item.get("name") or item.get("queue") or "").strip()
        else:
            name = str(item or "").strip()
        if name and name not in names:
            names.append(name)
    return names[:100]


def _inspect_count(payload: Any) -> int | None:
    if payload in (None, ""):
        return None
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        return len(payload)
    return None
