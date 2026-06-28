from __future__ import annotations

from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.app_context import AppContextSnapshot
from celery_diagnostics_observer.coverage import (
    build_coverage_report,
    render_doctor_report,
    render_startup_summary,
    render_status_report,
)
from celery_diagnostics_observer.policy import policy_from_name


def _policy():
    return policy_from_name("balanced")


def test_basic_redis_coverage_blocks_strong_topology_claims():
    config = ObserverConfig(
        broker_url="redis://localhost:6379/0",
        queues=("default",),
        project_key="cd_secret",
        ingest_url="https://ingest.example.test",
        telemetry_policy="balanced",
    )

    report = build_coverage_report(config, _policy())

    assert report.diagnostic_level == "Basic Observer"
    assert report.redis_broker_sampling.status == "available"
    assert report.worker_topology.status == "unknown"
    assert "queue backlog growth" in report.safe_claims
    assert any(item.claim == "no_worker_consuming_queue" for item in report.blocked_claims)
    assert any(item.claim == "worker_wedged_after_broker_reconnect" for item in report.blocked_claims)
    assert any(item.claim == "routing_mismatch" for item in report.blocked_claims)


def test_enhanced_observer_coverage_uses_app_context_facts():
    config = ObserverConfig(
        broker_url="redis://localhost:6379/0",
        queues=("default",),
        project_key="cd_secret",
        ingest_url="https://ingest.example.test",
        telemetry_policy="balanced",
        mode="project-aware",
        app="demo.celery:app",
    )
    app_context = AppContextSnapshot(
        loaded=True,
        routing_config_known=True,
        route_count=2,
        queue_count=1,
        default_queue_configured=True,
        beat_schedule_known=True,
        beat_schedule_entry_count=1,
        visibility_timeout=120,
        task_track_started=True,
    )

    report = build_coverage_report(config, _policy(), app_context=app_context)

    assert report.diagnostic_level == "Enhanced Observer"
    assert report.app_context.status == "loaded"
    assert report.routing_config.status == "known"
    assert "2 explicit route" in report.routing_config.detail
    assert report.visibility_timeout.status == "known"
    assert "120s" in report.visibility_timeout.detail
    assert report.task_track_started.status == "enabled"
    assert report.beat_schedule.status == "known"
    assert any(
        item.claim == "visibility_timeout_runtime_risk" and "runtime evidence" in item.reason
        for item in report.blocked_claims
    )


def test_missing_broker_keeps_redis_sampling_unavailable():
    config = ObserverConfig(
        broker_url="",
        queues=(),
        project_key="",
        ingest_url="http://127.0.0.1:8000",
        telemetry_policy="balanced",
    )

    report = build_coverage_report(config, _policy())

    assert report.redis_broker_sampling.status == "unavailable"
    assert "configure CELERY_BROKER_URL" in report.next_steps


def test_startup_summary_redacts_secrets_and_explains_blocked_claims():
    config = ObserverConfig(
        broker_url="redis://:broker-secret@localhost:6379/0",
        queues=("default",),
        project_key="cd_secret",
        ingest_url="https://user:ingest-secret@ingest.example.test/api?project_key=cd_secret",
        telemetry_policy="balanced",
        sample_interval=5,
    )
    report = build_coverage_report(config, _policy())

    rendered = render_startup_summary(config, report)

    assert "Celery Diagnostics Observer" in rendered
    assert "Diagnostic level: Basic Observer" in rendered
    assert "queue backlog growth" in rendered
    assert "no_worker_consuming_queue" in rendered
    assert "worker_received_but_not_executing" in rendered
    assert "broker-secret" not in rendered
    assert "ingest-secret" not in rendered
    assert "cd_secret" not in rendered


def test_doctor_report_does_not_require_project_key():
    config = ObserverConfig(
        broker_url="redis://localhost:6379/0",
        queues=(),
        project_key="",
        ingest_url="http://127.0.0.1:8000",
        telemetry_policy="balanced",
    )
    report = build_coverage_report(config, _policy())

    rendered = render_doctor_report(config, report)

    assert "Celery Diagnostics Doctor" in rendered
    assert "Project key configured: no" in rendered
    assert "Broker configured: yes" in rendered
    assert "Blocked claims:" in rendered


def test_status_report_is_short_subset():
    config = ObserverConfig(
        broker_url="redis://localhost:6379/0",
        queues=(),
        project_key="",
        ingest_url="http://127.0.0.1:8000",
        telemetry_policy="balanced",
    )
    report = build_coverage_report(config, _policy())

    rendered = render_status_report(config, report)

    assert "Celery Diagnostics Status" in rendered
    assert "Observer configured: yes" in rendered
    assert "Run `celery-diagnostics doctor` for detailed telemetry coverage." in rendered
    assert "Blocked claims:" not in rendered
