from __future__ import annotations

import pytest

from celery_diagnostics_observer.identity import (
    IdentityCapsuleError,
    identity_key_id,
    identity_ref,
    open_identity,
    seal_identity,
    task_run_ref,
)


IDENTITY_KEY = "customer-owned-identity-key"


def test_identity_references_are_stable_and_project_key_independent():
    assert task_run_ref("task-1", identity_key=IDENTITY_KEY) == task_run_ref(
        "task-1",
        identity_key=IDENTITY_KEY,
    )
    assert task_run_ref("task-1", identity_key=IDENTITY_KEY).startswith("R-")
    assert identity_ref("task", "billing.tasks.charge", identity_key=IDENTITY_KEY).startswith("T-")
    assert identity_ref("queue", "payments", identity_key=IDENTITY_KEY).startswith("Q-")
    assert identity_ref("worker", "celery@worker-1", identity_key=IDENTITY_KEY).startswith("W-")
    assert task_run_ref("task-1", identity_key=IDENTITY_KEY) != task_run_ref(
        "task-1",
        identity_key="another-customer-owned-key",
    )


def test_identity_capsule_round_trip_contains_only_operational_identity():
    capsule = seal_identity(
        {
            "task_id": "task-1",
            "task_name": "billing.tasks.charge",
            "queue": "payments",
            "worker": "celery@worker-1",
            "args": ["must-not-enter-capsule"],
            "result": "must-not-enter-capsule",
        },
        identity_key=IDENTITY_KEY,
    )

    assert capsule.startswith(f"v1.{identity_key_id(IDENTITY_KEY)}.")
    assert "task-1" not in capsule
    assert "must-not-enter-capsule" not in capsule
    assert open_identity(capsule, identity_key=IDENTITY_KEY) == {
        "task_id": "task-1",
        "task_name": "billing.tasks.charge",
        "queue": "payments",
        "worker": "celery@worker-1",
    }


def test_identity_capsule_rejects_the_wrong_key():
    capsule = seal_identity({"task_id": "task-1"}, identity_key=IDENTITY_KEY)

    with pytest.raises(IdentityCapsuleError, match="does not match"):
        open_identity(capsule, identity_key="another-customer-owned-key")


def test_identity_key_has_a_minimum_length():
    with pytest.raises(IdentityCapsuleError, match="at least 16"):
        seal_identity({"task_id": "task-1"}, identity_key="short")
