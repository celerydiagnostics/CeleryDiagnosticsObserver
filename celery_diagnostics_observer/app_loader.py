from __future__ import annotations

from importlib import import_module
from typing import Any

from .config import MODE_PROJECT_AWARE, ObserverConfig


def load_celery_app(config: ObserverConfig) -> Any:
    if config.mode == MODE_PROJECT_AWARE and config.app:
        return _load_project_app(config.app)
    from celery import Celery

    app = Celery("celery_diagnostics_observer", broker=config.broker_url)
    app.conf.worker_send_task_events = True
    app.conf.task_send_sent_event = True
    app.conf.task_track_started = True
    return app


def _load_project_app(import_path: str) -> Any:
    module_name, sep, attr = import_path.partition(":")
    if not sep and "." in import_path:
        module_name, _, attr = import_path.rpartition(".")
    module = import_module(module_name)
    return getattr(module, attr) if attr else module
