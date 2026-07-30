"""Read-only diagnostic capabilities that this observer can actually execute."""

from __future__ import annotations

from typing import Any

from .policy import TelemetryPolicy
from .config import ObserverConfig
from .redis_message_probe import uses_default_priority_layout
from .redis_result_probe import supports_status_only_result_probe


CONTROL_INSPECT_CAPABILITIES = (
    "celery.broker.queue_consumers",
    "celery.inspect.query_task",
    "celery.inspect.task_reservation",
    "celery.inspect.task_schedule",
    "celery.inspect.worker_capacity",
    "celery.inspect.worker_presence",
)


def active_probe_capabilities(
    app: Any,
    config: ObserverConfig,
    policy: TelemetryPolicy,
) -> tuple[str, ...]:
    if not config.active_probes:
        return ()
    capabilities: list[str] = []
    capabilities.extend(CONTROL_INSPECT_CAPABILITIES)
    if supports_status_only_result_probe(app):
        capabilities.append("celery.result_backend.status")
    if (
        config.broker_type == "redis"
        and policy.enable_redis_queue_snapshots
        and config.queues
        and uses_default_priority_layout(app)
    ):
        capabilities.append("celery.broker.message_presence")
    return tuple(sorted(set(capabilities)))


__all__ = ["active_probe_capabilities"]
