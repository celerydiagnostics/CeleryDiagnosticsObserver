from __future__ import annotations

from dataclasses import dataclass
from typing import Any


POLICY_LOCAL_ONLY = "local-only"
POLICY_READABLE = "readable"
POLICY_CHOICES = {POLICY_LOCAL_ONLY, POLICY_READABLE}
DEFAULT_POLICY = POLICY_READABLE


@dataclass(frozen=True)
class TelemetryPolicy:
    """Identity visibility policy for the supported Observer data contract."""

    name: str
    send_task_names: bool
    send_queue_names: bool
    send_routing_labels: bool
    send_worker_names: bool
    collect_exception_type: bool
    collect_exception_module: bool
    enable_redis_queue_snapshots: bool
    enable_control_inspect: bool
    collect_runtime_details: bool

    @property
    def identities_local_only(self) -> bool:
        return self.name == POLICY_LOCAL_ONLY


def normalize_policy_name(value: Any) -> str:
    policy = str(value or DEFAULT_POLICY).strip().lower().replace("_", "-")
    return policy if policy in POLICY_CHOICES else DEFAULT_POLICY


def policy_from_name(value: Any) -> TelemetryPolicy:
    name = normalize_policy_name(value)
    readable = name == POLICY_READABLE
    return TelemetryPolicy(
        name=name,
        send_task_names=readable,
        send_queue_names=readable,
        send_routing_labels=readable,
        send_worker_names=readable,
        collect_exception_type=True,
        collect_exception_module=readable,
        # Evidence collection is identical in both modes. Only identity
        # visibility changes.
        enable_redis_queue_snapshots=True,
        enable_control_inspect=True,
        collect_runtime_details=True,
    )


__all__ = [
    "DEFAULT_POLICY",
    "POLICY_CHOICES",
    "POLICY_LOCAL_ONLY",
    "POLICY_READABLE",
    "TelemetryPolicy",
    "normalize_policy_name",
    "policy_from_name",
]
