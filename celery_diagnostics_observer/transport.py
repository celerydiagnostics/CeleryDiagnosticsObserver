from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import requests

from .config import ObserverConfig
from .sanitizer import redact_url_credentials
from .spool import JsonlSpool
from .version import __version__


logger = logging.getLogger(__name__)


@dataclass
class _RetryState:
    batch: list[dict[str, Any]]
    attempts: int = 0
    next_retry_at: float = 0.0


class ObserverTransport:
    def __init__(self, config: ObserverConfig):
        self.config = config
        self.buffer: deque[dict[str, Any]] = deque()
        self.spool = JsonlSpool(config.spool_path) if config.spool_path else None
        self.session = requests.Session()
        self.failed_send_count = 0
        self.dropped_event_count = 0
        self.sent_event_count = 0
        self.spooled_event_count = 0
        self.last_error = ""
        self.last_flush_at = 0.0
        self._retry_state: _RetryState | None = None

    def enqueue(self, event: dict[str, Any]) -> None:
        if not isinstance(event, dict):
            return
        event = _without_project_key(event)
        if self.config.dry_run:
            if self.config.print_sanitized_events:
                print(json.dumps(_without_project_key(event), sort_keys=True, default=str))
            return
        self.buffer.append(event)
        if len(self.buffer) >= self.config.batch_size:
            self.flush_once(force=True)

    def flush_once(self, *, force: bool = False) -> bool:
        if self.config.dry_run:
            return False
        now = time.monotonic()
        if not force and now - self.last_flush_at < self.config.flush_interval:
            return False
        if self._retry_state is not None and now < self._retry_state.next_retry_at:
            return False
        batch = self._drain_batch()
        if not batch:
            return False
        # Strip credentials from new and previously spooled events. Project
        # authentication belongs only in the HTTP header.
        batch = [_without_project_key(event) for event in batch]
        try:
            response = self.session.post(
                self.config.observer_events_url,
                json={"events": batch},
                headers={
                    "Authorization": f"Bearer {self.config.project_key}",
                    "User-Agent": f"celery-diagnostics-observer/{__version__}",
                },
                timeout=5.0,
            )
            response.raise_for_status()
            self.last_flush_at = now
            self.sent_event_count += len(batch)
            self.last_error = ""
            self._retry_state = None
            return True
        except Exception as exc:  # noqa: BLE001
            self.failed_send_count += 1
            self.last_error = type(exc).__name__
            retry_delay = self._retry_delay()
            if self.spool is not None:
                self.spool.append_many(batch)
                self.spooled_event_count += len(batch)
                self._retry_state = _RetryState(batch=[], attempts=self.failed_send_count, next_retry_at=now + retry_delay)
            else:
                self._retry_state = _RetryState(batch=batch, attempts=self.failed_send_count, next_retry_at=now + retry_delay)
            safe_url = redact_url_credentials(self.config.observer_events_url)
            logger.warning("observer batch send failed url=%s error=%s retry_in=%.1fs", safe_url, type(exc).__name__, retry_delay)
            return True

    def _drain_batch(self) -> list[dict[str, Any]]:
        if self._retry_state is not None and self._retry_state.batch:
            return self._retry_state.batch
        batch: list[dict[str, Any]] = []
        if self.spool is not None and len(batch) < self.config.batch_size:
            batch.extend(self.spool.pop_many(self.config.batch_size - len(batch)))
        while self.buffer and len(batch) < self.config.batch_size:
            batch.append(self.buffer.popleft())
        return batch

    def _retry_delay(self) -> float:
        attempts = self._retry_state.attempts + 1 if self._retry_state is not None else self.failed_send_count
        return min(60.0, 2 ** min(max(attempts, 1), 6))


def _without_project_key(event: dict[str, Any]) -> dict[str, Any]:
    safe = dict(event)
    safe.pop("project_key", None)
    return safe
