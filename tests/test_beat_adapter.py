from __future__ import annotations

import json
from datetime import datetime, timezone

from celery import Celery
from celery.beat import ScheduleEntry
from celery.schedules import schedule

from celery_diagnostics_observer.beat import (
    _first_due_entry,
    _is_broker_error,
    _prepare_entry,
    _schedule_entries,
)
from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.policy import policy_from_name
from celery_diagnostics_observer.sanitizer import sanitize_celery_event


def _entry() -> ScheduleEntry:
    app = Celery("beat-adapter-test")
    return ScheduleEntry(
        name="billing-every-minute",
        task="billing.tasks.capture",
        schedule=schedule(run_every=60),
        options={"queue": "billing", "headers": {"existing": "header"}},
        last_run_at=datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc),
        total_run_count=3,
        app=app,
    )


def test_prepare_entry_adds_only_operational_periodic_headers():
    original = _entry()

    prepared, context = _prepare_entry(original, beat_hostname="beat@test-host")

    assert prepared is not original
    assert original.options == {"queue": "billing", "headers": {"existing": "header"}}
    assert prepared.options["headers"]["existing"] == "header"
    assert prepared.options["task_id"] == context["task_id"]
    assert prepared.options["headers"]["celery_diagnostics_occurrence_key"] == context["periodic_occurrence_key"]
    assert context["periodic_scheduled_fire_at"] == "2026-08-01T12:01:00+00:00"
    rendered = json.dumps(prepared.options, sort_keys=True)
    assert "args" not in rendered
    assert "kwargs" not in rendered


def test_schedule_inventory_uses_safe_configuration_fields_only():
    entry = _entry()

    entries = _schedule_entries({entry.name: entry})

    assert len(entries) == 1
    assert entries[0]["schedule_name"] == "billing-every-minute"
    assert entries[0]["task_name"] == "billing.tasks.capture"
    assert entries[0]["queue"] == "billing"
    assert entries[0]["interval_seconds"] == 60
    assert set(entries[0]) == {
        "schedule_id",
        "schedule_name",
        "task_name",
        "schedule_type",
        "schedule_display",
        "schedule_hash",
        "schedule_version",
        "enabled",
        "queue",
        "routing_key",
        "exchange",
        "next_due_at",
        "last_run_at",
        "total_run_count",
        "interval_seconds",
    }


def test_local_only_beat_events_hide_operational_names():
    config = ObserverConfig(
        broker_url="redis://redis:6379/0",
        queues=("billing",),
        project_key="cf_test",
        ingest_url="http://backend",
        observer_id="beat@test-host",
        identity_key="customer-owned-identity-key",
    )
    policy = policy_from_name("local-only")
    snapshot = sanitize_celery_event(
        {
            "type": "beat-schedule-snapshot",
            "hostname": "beat@test-host",
            "timestamp": 1_754_048_000,
            "beat_schedule_version": "sha256:version",
            "beat_schedule_entry_count": 1,
            "beat_schedule_snapshot_complete": True,
            "beat_schedule_entries": _schedule_entries({"billing-every-minute": _entry()}),
        },
        config=config,
        policy=policy,
    )

    assert snapshot is not None
    assert snapshot["event_type"] == "beat-schedule-snapshot"
    assert snapshot["normalized_event_type"] == "beat_schedule_snapshot"
    assert snapshot["beat_schedule_snapshot_complete"] is True
    assert snapshot["beat_schedule_entries"][0]["schedule_name"].startswith("T-")
    assert snapshot["beat_schedule_entries"][0]["task_name"].startswith("T-")
    assert snapshot["beat_schedule_entries"][0]["queue"].startswith("Q-")
    rendered = json.dumps(snapshot, sort_keys=True)
    assert "billing-every-minute" not in rendered
    assert "billing.tasks.capture" not in rendered


def test_due_entry_and_broker_error_helpers_cover_pre_publish_failure():
    entry = _entry()
    entry.last_run_at = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
    entry.schedule.nowfun = lambda: datetime(2026, 8, 1, 12, 2, tzinfo=timezone.utc)

    assert _first_due_entry({entry.name: entry}) is entry
    assert _is_broker_error(ConnectionRefusedError("broker unavailable")) is True
    assert _is_broker_error(ValueError("invalid schedule")) is False
