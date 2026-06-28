from __future__ import annotations

import logging
from threading import Event, Thread
from datetime import datetime, timezone
from typing import Any, Callable

from .config import ObserverConfig
from .policy import TelemetryPolicy
from .sanitizer import sanitize_queue_snapshot


logger = logging.getLogger(__name__)


class RedisQueueSampler:
    def __init__(
        self,
        config: ObserverConfig,
        policy: TelemetryPolicy,
        *,
        client_factory: Callable[[str], Any] | None = None,
    ):
        self.config = config
        self.policy = policy
        self.client_factory = client_factory or self._default_client_factory

    def sample_once(self) -> list[dict[str, Any]]:
        if self.config.broker_type != "redis" or not self.policy.enable_redis_queue_snapshots:
            return []
        sampled_at = datetime.now(timezone.utc)
        try:
            client = self.client_factory(self.config.broker_url)
        except Exception as exc:  # noqa: BLE001
            return [
                sanitize_queue_snapshot(
                    queue=queue,
                    messages_ready_approx=None,
                    sampled_at=sampled_at,
                    config=self.config,
                    policy=self.policy,
                    broker_reachable=False,
                    error_type=type(exc).__name__,
                )
                for queue in self.config.queues
            ]

        snapshots: list[dict[str, Any]] = []
        try:
            for queue in self.config.queues:
                try:
                    count = client.llen(queue)
                except Exception as exc:  # noqa: BLE001
                    snapshots.append(
                        sanitize_queue_snapshot(
                            queue=queue,
                            messages_ready_approx=None,
                            sampled_at=sampled_at,
                            config=self.config,
                            policy=self.policy,
                            broker_reachable=False,
                            error_type=type(exc).__name__,
                        )
                    )
                    continue
                snapshots.append(
                    sanitize_queue_snapshot(
                        queue=queue,
                        messages_ready_approx=count,
                        sampled_at=sampled_at,
                        config=self.config,
                        policy=self.policy,
                    )
                )
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
        return snapshots

    @staticmethod
    def _default_client_factory(broker_url: str):
        import redis

        return redis.Redis.from_url(broker_url, socket_timeout=2.0, socket_connect_timeout=2.0)


class RedisSamplerLoop:
    def __init__(self, sampler: RedisQueueSampler, transport):
        self.sampler = sampler
        self.transport = transport
        self.stop_event = Event()
        self.thread: Thread | None = None

    def start(self) -> None:
        if self.thread is not None and self.thread.is_alive():
            return
        if not self.sampler.config.queues:
            return
        if self.sampler.config.broker_type != "redis":
            return
        self.stop_event.clear()
        self.thread = Thread(target=self._run, name="celery-diagnostics-redis-sampler", daemon=True)
        self.thread.start()

    def stop(self, *, timeout: float = 2.0) -> None:
        self.stop_event.set()
        if self.thread is not None and self.thread.is_alive():
            self.thread.join(timeout=timeout)

    def _run(self) -> None:
        interval = self.sampler.config.sample_interval
        while not self.stop_event.is_set():
            try:
                for snapshot in self.sampler.sample_once():
                    self.transport.enqueue(snapshot)
                self.transport.flush_once()
            except Exception as exc:  # noqa: BLE001
                logger.warning("redis sampler failed error=%s", type(exc).__name__)
            self.stop_event.wait(interval)
