from __future__ import annotations

import argparse
import logging
import sys

from .app_context import AppContextSnapshot, failed_app_context, inspect_celery_app, unloaded_app_context
from .app_loader import load_celery_app
from .config import MODE_PROJECT_AWARE, MODE_STANDALONE, config_from_env
from .coverage import build_coverage_report, render_doctor_report, render_startup_summary, render_status_report
from .event_receiver import EventReceiver, dry_run_sample_events
from .health import ObserverHealthLoop
from .inspect_sampler import CeleryInspectSampler, CeleryInspectSamplerLoop
from .policy import POLICY_CHOICES, policy_from_name
from .redis_sampler import RedisQueueSampler, RedisSamplerLoop
from .sanitizer import sanitize_observer_heartbeat
from .transport import ObserverTransport


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "observe":
        return observe(args)
    if args.command == "doctor":
        return doctor(args)
    if args.command == "status":
        return status(args)
    parser.print_help()
    return 2


def observe(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    logging.basicConfig(level=getattr(logging, config.log_level, logging.INFO), format="%(levelname)s %(message)s")
    policy = policy_from_name(config.telemetry_policy)
    transport = ObserverTransport(config)
    if config.dry_run:
        for event in dry_run_sample_events(config, policy):
            transport.enqueue(event)
        transport.enqueue(sanitize_observer_heartbeat(config=config, policy=policy, transport=transport))
        for event in RedisQueueSampler(config, policy).sample_once():
            transport.enqueue(event)
        return 0
    if not config.project_key:
        print("CD_PROJECT_KEY environment variable is required", file=sys.stderr)
        return 2
    if not config.broker_url and config.mode == MODE_STANDALONE:
        print("Redis broker URL is required in standalone mode", file=sys.stderr)
        return 2
    try:
        app = load_celery_app(config)
    except Exception as exc:  # noqa: BLE001
        app_context = failed_app_context(exc)
        report = build_coverage_report(config, policy, app_context=app_context)
        print(render_startup_summary(config, report), file=sys.stderr)
        return 2
    app_context = inspect_celery_app(app) if config.mode == MODE_PROJECT_AWARE and config.app else unloaded_app_context()
    report = build_coverage_report(config, policy, app_context=app_context)
    print(render_startup_summary(config, report), file=sys.stderr)
    sampler_loop = RedisSamplerLoop(RedisQueueSampler(config, policy), transport)
    inspect_loop = CeleryInspectSamplerLoop(CeleryInspectSampler(app, config, policy), transport)
    health_loop = ObserverHealthLoop(config, policy, transport)
    health_loop.start()
    sampler_loop.start()
    inspect_loop.start()
    receiver = EventReceiver(app, config, policy, transport)
    try:
        receiver.run_forever()
    finally:
        health_loop.stop()
        inspect_loop.stop()
        sampler_loop.stop()
        transport.flush_once(force=True)
    return 0


def doctor(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    policy = policy_from_name(config.telemetry_policy)
    report = build_coverage_report(config, policy, app_context=_app_context_for_report(config))
    print(render_doctor_report(config, report))
    return 0


def status(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    policy = policy_from_name(config.telemetry_policy)
    report = build_coverage_report(config, policy, app_context=_app_context_for_report(config))
    print(render_status_report(config, report))
    return 0


def _app_context_for_report(config) -> AppContextSnapshot:
    if config.mode != MODE_PROJECT_AWARE or not config.app:
        return unloaded_app_context()
    try:
        return inspect_celery_app(load_celery_app(config))
    except Exception as exc:  # noqa: BLE001
        return failed_app_context(exc)


def _config_from_args(args: argparse.Namespace):
    return config_from_env(
        {
            "broker_url": getattr(args, "broker", None),
            "queues": getattr(args, "queues", None),
            "ingest_url": getattr(args, "ingest_url", None),
            "telemetry_policy": getattr(args, "policy", None),
            "mode": getattr(args, "mode", None),
            "app": getattr(args, "app", None),
            "observer_id": getattr(args, "observer_id", None),
            "sample_interval": getattr(args, "sample_interval", None),
            "inspect_interval": getattr(args, "inspect_interval", None),
            "batch_size": getattr(args, "batch_size", None),
            "flush_interval": getattr(args, "flush_interval", None),
            "spool_path": getattr(args, "spool_path", None),
            "log_level": getattr(args, "log_level", None),
            "dry_run": getattr(args, "dry_run", False),
            "print_sanitized_events": getattr(args, "print_sanitized_events", False),
        }
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="celery-diagnostics")
    subparsers = parser.add_subparsers(dest="command")
    observe_parser = subparsers.add_parser("observe", help="Run the standalone Celery Diagnostics observer.")
    _add_common_config_args(observe_parser)
    observe_parser.add_argument("--observer-id", default=None)
    observe_parser.add_argument("--sample-interval", default=None, type=float)
    observe_parser.add_argument("--inspect-interval", default=None, type=float)
    observe_parser.add_argument("--batch-size", default=None, type=int)
    observe_parser.add_argument("--flush-interval", default=None, type=float)
    observe_parser.add_argument("--spool-path", default=None)
    observe_parser.add_argument("--log-level", default=None)
    observe_parser.add_argument("--dry-run", action="store_true")
    observe_parser.add_argument("--print-sanitized-events", action="store_true")

    doctor_parser = subparsers.add_parser("doctor", help="Explain current telemetry coverage and diagnostic limits.")
    _add_common_config_args(doctor_parser)

    status_parser = subparsers.add_parser("status", help="Show lightweight observer integration status.")
    _add_common_config_args(status_parser)
    return parser


def _add_common_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--broker", default=None, help="Celery broker URL. Redis only for the MVP.")
    parser.add_argument("--queues", default=None, help="Comma-separated queue names to sample.")
    parser.add_argument("--ingest-url", default=None, help="Celery Diagnostics ingest base URL.")
    parser.add_argument("--policy", choices=sorted(POLICY_CHOICES), default=None)
    parser.add_argument("--mode", choices=[MODE_STANDALONE, MODE_PROJECT_AWARE], default=None)
    parser.add_argument("-A", "--app", default=None, help="Project Celery app import path for project-aware mode.")


if __name__ == "__main__":
    raise SystemExit(main())
