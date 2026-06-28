from __future__ import annotations

from celery import Celery

from celery_diagnostics_observer.app_context import inspect_celery_app


def test_inspect_celery_app_extracts_allowlisted_config_facts():
    app = Celery("enhanced-observer-test", broker="redis://localhost:6379/0")
    app.conf.update(
        broker_transport_options={"visibility_timeout": 120},
        task_track_started=True,
        task_default_queue="default",
        task_routes={"billing.tasks.charge": {"queue": "billing"}},
        beat_schedule={
            "billing-nightly": {
                "task": "billing.tasks.nightly",
                "schedule": 3600,
            }
        },
    )

    context = inspect_celery_app(app)

    assert context.loaded is True
    assert context.routing_config_known is True
    assert context.route_count == 1
    assert context.default_queue_configured is True
    assert context.visibility_timeout == 120
    assert context.task_track_started is True
    assert context.beat_schedule_known is True
    assert context.beat_schedule_entry_count == 1


def test_inspect_celery_app_keeps_missing_visibility_timeout_unknown():
    app = Celery("enhanced-observer-test", broker="redis://localhost:6379/0")
    app.conf.update(
        broker_transport_options={},
        task_track_started=False,
        beat_schedule={},
    )

    context = inspect_celery_app(app)

    assert context.loaded is True
    assert context.visibility_timeout is None
    assert context.task_track_started is False
    assert context.beat_schedule_known is True
    assert context.beat_schedule_entry_count == 0
