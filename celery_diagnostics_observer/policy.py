from __future__ import annotations

from dataclasses import dataclass
from typing import Any


POLICY_PRIVATE = "private"
POLICY_BALANCED = "balanced"
POLICY_DETAILED = "detailed"
POLICY_CUSTOM = "custom"
POLICY_CHOICES = {POLICY_PRIVATE, POLICY_BALANCED, POLICY_DETAILED, POLICY_CUSTOM}
DEFAULT_POLICY = POLICY_BALANCED


@dataclass(frozen=True)
class TelemetryPolicy:
    name: str
    send_task_names: bool
    send_queue_names: bool
    send_routing_labels: bool
    send_worker_names: bool
    send_registered_tasks: bool
    collect_exception_type: bool
    collect_exception_module: bool
    collect_exception_summary: bool
    collect_exception_message: bool
    collect_stack_frames: bool
    collect_argument_shape: bool
    collect_result_shape: bool
    enable_redis_queue_snapshots: bool
    enable_control_inspect: bool
    collect_runtime_details: bool


def normalize_policy_name(value: Any) -> str:
    policy = str(value or DEFAULT_POLICY).strip().lower()
    return policy if policy in POLICY_CHOICES else DEFAULT_POLICY


def policy_from_name(value: Any, overrides: dict[str, Any] | None = None) -> TelemetryPolicy:
    name = normalize_policy_name(value)
    base = _policy_defaults(POLICY_BALANCED if name == POLICY_CUSTOM else name)
    if name == POLICY_CUSTOM and overrides:
        allowed_overrides = set(base)
        for key, override in overrides.items():
            if key in allowed_overrides:
                base[key] = bool(override)
    base["collect_exception_message"] = False
    base["collect_stack_frames"] = bool(base["collect_stack_frames"] and name == POLICY_DETAILED)
    return TelemetryPolicy(name=name, **base)


def _policy_defaults(name: str) -> dict[str, bool]:
    if name == POLICY_PRIVATE:
        return {
            "send_task_names": False,
            "send_queue_names": False,
            "send_routing_labels": False,
            "send_worker_names": False,
            "send_registered_tasks": False,
            "collect_exception_type": True,
            "collect_exception_module": False,
            "collect_exception_summary": False,
            "collect_exception_message": False,
            "collect_stack_frames": False,
            "collect_argument_shape": False,
            "collect_result_shape": False,
            "enable_redis_queue_snapshots": True,
            "enable_control_inspect": False,
            "collect_runtime_details": True,
        }
    if name == POLICY_DETAILED:
        return {
            "send_task_names": True,
            "send_queue_names": True,
            "send_routing_labels": True,
            "send_worker_names": True,
            "send_registered_tasks": True,
            "collect_exception_type": True,
            "collect_exception_module": True,
            "collect_exception_summary": True,
            "collect_exception_message": False,
            "collect_stack_frames": True,
            "collect_argument_shape": True,
            "collect_result_shape": True,
            "enable_redis_queue_snapshots": True,
            "enable_control_inspect": True,
            "collect_runtime_details": True,
        }
    return {
        "send_task_names": True,
        "send_queue_names": True,
        "send_routing_labels": True,
        "send_worker_names": False,
        "send_registered_tasks": False,
        "collect_exception_type": True,
        "collect_exception_module": True,
        "collect_exception_summary": True,
        "collect_exception_message": False,
        "collect_stack_frames": False,
        "collect_argument_shape": False,
        "collect_result_shape": False,
        "enable_redis_queue_snapshots": True,
        "enable_control_inspect": False,
        "collect_runtime_details": True,
    }
