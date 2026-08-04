from __future__ import annotations

import os
import socket
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from .policy import DEFAULT_POLICY, normalize_policy_name


MODE_STANDALONE = "standalone"
MODE_PROJECT_AWARE = "project-aware"
MODE_CHOICES = {MODE_STANDALONE, MODE_PROJECT_AWARE}


@dataclass(frozen=True)
class ObserverConfig:
    broker_url: str
    queues: tuple[str, ...]
    project_key: str
    ingest_url: str
    telemetry_policy: str = DEFAULT_POLICY
    identity_key: str = ""
    mode: str = MODE_STANDALONE
    app: str = ""
    observer_id: str = ""
    sample_interval: float = 10.0
    inspect_interval: float = 30.0
    batch_size: int = 50
    flush_interval: float = 5.0
    spool_path: str = ""
    log_level: str = "INFO"
    dry_run: bool = False
    print_sanitized_events: bool = False
    active_probes: bool = True
    broker_message_scan_limit: int = 10_000

    def __post_init__(self) -> None:
        set_value = object.__setattr__
        set_value(self, "broker_url", str(self.broker_url or ""))
        set_value(self, "queues", tuple(_parse_queues(self.queues)))
        set_value(self, "project_key", str(self.project_key or ""))
        set_value(self, "ingest_url", str(self.ingest_url or "http://127.0.0.1:8000"))
        set_value(self, "telemetry_policy", normalize_policy_name(self.telemetry_policy))
        set_value(self, "identity_key", str(self.identity_key or ""))
        mode = str(self.mode or MODE_STANDALONE).strip().lower().replace("_", "-")
        set_value(self, "mode", mode if mode in MODE_CHOICES else MODE_STANDALONE)
        set_value(self, "app", str(self.app or ""))
        set_value(self, "observer_id", str(self.observer_id or _default_observer_id()))
        set_value(self, "sample_interval", _bounded_float(self.sample_interval, default=10.0, minimum=0.5, maximum=3600.0))
        set_value(self, "inspect_interval", _bounded_float(self.inspect_interval, default=30.0, minimum=1.0, maximum=3600.0))
        set_value(self, "batch_size", _bounded_int(self.batch_size, default=50, minimum=1, maximum=1000))
        set_value(self, "flush_interval", _bounded_float(self.flush_interval, default=5.0, minimum=0.1, maximum=3600.0))
        set_value(self, "spool_path", str(self.spool_path or ""))
        set_value(self, "log_level", str(self.log_level or "INFO").upper())
        set_value(self, "dry_run", bool(self.dry_run))
        set_value(self, "print_sanitized_events", bool(self.print_sanitized_events))
        set_value(self, "active_probes", bool(self.active_probes))
        set_value(
            self,
            "broker_message_scan_limit",
            _bounded_int(
                self.broker_message_scan_limit,
                default=10_000,
                minimum=1,
                maximum=100_000,
            ),
        )

    @property
    def observer_events_url(self) -> str:
        return self.ingest_url.rstrip("/") + "/api/ingest/observer/events/"

    @property
    def probe_next_url(self) -> str:
        return self.ingest_url.rstrip("/") + "/api/ingest/observer/probes/next/"

    @property
    def probe_result_url(self) -> str:
        return self.ingest_url.rstrip("/") + "/api/ingest/observer/probes/result/"

    @property
    def identity_resolve_url(self) -> str:
        return self.ingest_url.rstrip("/") + "/api/ingest/observer/identities/"

    @property
    def broker_type(self) -> str:
        return self.broker_url.split(":", 1)[0].lower() if ":" in self.broker_url else ""

    @property
    def public_observer_id(self) -> str:
        if self.telemetry_policy != "local-only":
            return self.observer_id[:255]
        from .identity import identity_ref

        return identity_ref("observer", self.observer_id, identity_key=self.identity_key)

    def validate_ingest_transport(self) -> None:
        parsed = urlsplit(self.ingest_url)
        host = (parsed.hostname or "").lower()
        if parsed.scheme == "https":
            return
        if parsed.scheme == "http" and host in {"127.0.0.1", "localhost", "::1"}:
            return
        raise ValueError("CD_INGEST_URL must use HTTPS (plain HTTP is allowed only for loopback development)")


def config_from_env(overrides: dict[str, Any] | None = None) -> ObserverConfig:
    overrides = overrides or {}
    queues = overrides.get("queues")
    if queues is None:
        queues = os.getenv("CD_QUEUES", "")
    active_probes = overrides.get("active_probes")
    if active_probes is None:
        active_probes = os.getenv("CD_ACTIVE_PROBES", "1")
    return ObserverConfig(
        broker_url=overrides.get("broker_url") or os.getenv("CELERY_BROKER_URL", ""),
        queues=_parse_queues(queues),
        project_key=os.getenv("CD_PROJECT_KEY", ""),
        ingest_url=overrides.get("ingest_url") or os.getenv("CD_INGEST_URL", "http://127.0.0.1:8000"),
        telemetry_policy=overrides.get("telemetry_policy") or os.getenv("CD_TELEMETRY_POLICY", DEFAULT_POLICY),
        identity_key=overrides.get("identity_key") or os.getenv("CD_IDENTITY_KEY", ""),
        mode=overrides.get("mode") or os.getenv("CD_OBSERVER_MODE", MODE_STANDALONE),
        app=overrides.get("app") or os.getenv("CELERY_APP", ""),
        observer_id=overrides.get("observer_id") or os.getenv("CD_OBSERVER_ID", ""),
        sample_interval=overrides.get("sample_interval") or os.getenv("CD_SAMPLE_INTERVAL", 10.0),
        inspect_interval=overrides.get("inspect_interval") or os.getenv("CD_INSPECT_INTERVAL", 30.0),
        batch_size=overrides.get("batch_size") or os.getenv("CD_BATCH_SIZE", 50),
        flush_interval=overrides.get("flush_interval") or os.getenv("CD_FLUSH_INTERVAL", 5.0),
        spool_path=overrides.get("spool_path") or os.getenv("CD_SPOOL_PATH", ""),
        log_level=overrides.get("log_level") or os.getenv("CD_LOG_LEVEL", "INFO"),
        dry_run=overrides.get("dry_run", False),
        print_sanitized_events=overrides.get("print_sanitized_events", False),
        active_probes=_boolean(active_probes, default=True),
        broker_message_scan_limit=overrides.get(
            "broker_message_scan_limit"
        )
        or os.getenv("CD_BROKER_MESSAGE_SCAN_LIMIT", 10_000),
    )


def _parse_queues(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        items = value.split(",")
    else:
        items = list(value)
    queues = []
    for item in items:
        queue = str(item or "").strip()
        if queue and queue not in queues:
            queues.append(queue)
    return queues


def _bounded_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, minimum), maximum)


def _bounded_float(value: Any, *, default: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, minimum), maximum)


def _default_observer_id() -> str:
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


def _boolean(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default
