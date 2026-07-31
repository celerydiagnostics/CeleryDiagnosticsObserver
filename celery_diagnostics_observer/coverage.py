from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .app_context import AppContextSnapshot, unloaded_app_context
from .config import MODE_PROJECT_AWARE, ObserverConfig
from .policy import TelemetryPolicy
from .sanitizer import redact_url_credentials


SECRET_QUERY_KEYS = {
    "access_token",
    "api_key",
    "auth",
    "key",
    "password",
    "project_key",
    "secret",
    "signature",
    "token",
}


@dataclass(frozen=True)
class CoverageSource:
    label: str
    status: str
    detail: str = ""


@dataclass(frozen=True)
class BlockedClaim:
    claim: str
    reason: str


@dataclass(frozen=True)
class CoverageReport:
    redis_broker_sampling: CoverageSource
    celery_event_stream: CoverageSource
    app_context: CoverageSource
    worker_topology: CoverageSource
    routing_config: CoverageSource
    beat_schedule: CoverageSource
    visibility_timeout: CoverageSource
    task_track_started: CoverageSource
    producer_publish_tracking: CoverageSource
    diagnostic_level: str
    safe_claims: tuple[str, ...]
    blocked_claims: tuple[BlockedClaim, ...]
    next_steps: tuple[str, ...]


def build_coverage_report(
    config: ObserverConfig,
    policy: TelemetryPolicy,
    *,
    app_context: AppContextSnapshot | None = None,
    app_context_loaded: bool = False,
    app_context_error: str = "",
) -> CoverageReport:
    app_context = _normalize_app_context(
        app_context,
        app_context_loaded=app_context_loaded,
        app_context_error=app_context_error,
    )
    broker_type = config.broker_type
    redis_status = "available" if broker_type in {"redis", "rediss"} else "unavailable"
    redis_detail = (
        "Redis broker sampling can use safe queue depth checks."
        if redis_status == "available"
        else "Set CELERY_BROKER_URL to a Redis broker URL."
    )

    app_status = "loaded" if app_context.loaded else "not_loaded"
    app_detail = (
        "App context was loaded in the observer process."
        if app_context.loaded
        else "No Celery app context is loaded."
    )
    if app_context.error:
        app_status = "failed"
        app_detail = f"App context load failed: {_safe_error_label(app_context.error)}"

    worker_topology = CoverageSource(
        "Worker topology",
        "limited" if policy.enable_control_inspect else "unknown",
        (
            "Control inspect can provide active queues when reachable."
            if policy.enable_control_inspect
            else "Worker active queues are not confirmed."
        ),
    )

    safe_claims = _safe_claims(redis_status)
    next_steps = _next_steps(config, policy, app_context)
    diagnostic_level = "Enhanced Observer" if app_context.loaded else "Basic Observer"

    return CoverageReport(
        redis_broker_sampling=CoverageSource("Redis broker sampling", redis_status, redis_detail),
        celery_event_stream=CoverageSource(
            "Celery event stream",
            "unknown",
            "The receiver runs during observe; doctor does not confirm live task events yet.",
        ),
        app_context=CoverageSource("App context", app_status, app_detail),
        worker_topology=worker_topology,
        routing_config=_routing_config_source(app_context),
        beat_schedule=_beat_schedule_source(app_context),
        visibility_timeout=_visibility_timeout_source(app_context),
        task_track_started=_task_track_started_source(app_context),
        producer_publish_tracking=CoverageSource(
            "Producer publish tracking",
            "unavailable",
            "The standalone observer does not run inside producer processes.",
        ),
        diagnostic_level=diagnostic_level,
        safe_claims=safe_claims,
        blocked_claims=_blocked_claims(app_context),
        next_steps=next_steps,
    )


def render_startup_summary(config: ObserverConfig, report: CoverageReport) -> str:
    lines = [
        "Celery Diagnostics Observer",
        "",
        f"Mode: {_mode_label(config)}",
        f"Broker: {_redact_url(config.broker_url) or 'not configured'}",
        f"Ingest: {_redact_url(config.ingest_url) or 'not configured'}",
        f"Sample interval: {_format_seconds(config.sample_interval)}",
        f"Telemetry policy: {config.telemetry_policy}",
        "",
        "Telemetry sources:",
        *_source_lines(report),
        "",
        f"Diagnostic level: {report.diagnostic_level}",
        "",
        "CD can detect:",
        *_bullet_lines(report.safe_claims),
        "",
        "CD will not claim yet:",
        *_blocked_lines(report.blocked_claims),
        "",
        "Reason:",
        "Required telemetry is missing for stronger claims.",
    ]
    return "\n".join(lines)


def render_doctor_report(config: ObserverConfig, report: CoverageReport) -> str:
    lines = [
        "Celery Diagnostics Doctor",
        "",
        "Connectivity:",
        f"- Broker configured: {_yes_no(bool(config.broker_url))}",
        f"- Broker URL: {_redact_url(config.broker_url) or 'not configured'}",
        f"- Ingest configured: {_yes_no(bool(config.ingest_url))}",
        f"- Ingest URL: {_redact_url(config.ingest_url) or 'not configured'}",
        f"- Project key configured: {_yes_no(bool(config.project_key))}",
        "",
        "Telemetry sources:",
        *_source_lines(report),
        "",
        f"Diagnostic level: {report.diagnostic_level}",
        "",
        "Safe claims:",
        *_bullet_lines(report.safe_claims),
        "",
        "Blocked claims:",
        *_blocked_lines(report.blocked_claims),
        "",
        "Next steps:",
        *_bullet_lines(report.next_steps),
    ]
    return "\n".join(lines)


def render_status_report(config: ObserverConfig, report: CoverageReport) -> str:
    lines = [
        "Celery Diagnostics Status",
        "",
        f"Observer configured: {_yes_no(bool(config.broker_url or config.ingest_url))}",
        f"Broker configured: {_yes_no(bool(config.broker_url))}",
        f"Mode: {_mode_label(config)}",
        f"Telemetry policy: {config.telemetry_policy}",
        f"Diagnostic level: {report.diagnostic_level}",
        "",
        "Run `celery-diagnostics doctor` for detailed telemetry coverage.",
    ]
    return "\n".join(lines)


def _safe_claims(redis_status: str) -> tuple[str, ...]:
    claims = ["broker reachability problems"]
    if redis_status == "available":
        claims.extend(["queue backlog growth", "queue pressure symptoms"])
    return tuple(claims)


def _blocked_claims(app_context: AppContextSnapshot) -> tuple[BlockedClaim, ...]:
    routing_reason = (
        "requires worker active queue evidence"
        if app_context.loaded and app_context.routing_config_known
        else "requires routing config plus worker active queues"
    )
    beat_reason = (
        "requires beat runtime scheduling evidence"
        if app_context.loaded and app_context.beat_schedule_known
        else "requires beat schedule evidence"
    )
    visibility_reason = (
        "requires runtime evidence and ack/redelivery semantics"
        if app_context.loaded and app_context.visibility_timeout is not None
        else "requires known visibility timeout and runtime evidence"
    )
    return (
        BlockedClaim("no_worker_consuming_queue", "requires worker topology or active queue evidence"),
        BlockedClaim("worker_wedged_after_broker_reconnect", "requires broker reconnect, fresh worker liveness, queue backlog, and broker consumer evidence"),
        BlockedClaim("worker_received_but_not_executing", "requires task_track_started plus fresh worker liveness after receive/reserve"),
        BlockedClaim("routing_mismatch", routing_reason),
        BlockedClaim("beat_schedule_missed", beat_reason),
        BlockedClaim("visibility_timeout_runtime_risk", visibility_reason),
        BlockedClaim(
            "publish_failed_before_broker",
            "cannot be established from external observer evidence alone",
        ),
    )


def _next_steps(config: ObserverConfig, policy: TelemetryPolicy, app_context: AppContextSnapshot) -> tuple[str, ...]:
    steps: list[str] = []
    if not config.broker_url:
        steps.append("configure CELERY_BROKER_URL")
    if app_context.error:
        steps.append("fix the Celery app import path passed with `-A`")
    elif config.mode != MODE_PROJECT_AWARE or not config.app:
        steps.append("run with `-A myproject.celery:app` for app-aware context")
    elif not app_context.loaded:
        steps.append("make sure the Celery app can be imported by the observer process")
    if app_context.loaded and app_context.visibility_timeout is None:
        steps.append("set broker_transport_options.visibility_timeout when Redis/SQS redelivery risk matters")
    if app_context.loaded and app_context.task_track_started is not True:
        steps.append("enable task_track_started for stronger received-to-started diagnostics")
    if not policy.enable_control_inspect:
        steps.append("use a telemetry policy with control inspect when worker topology is safe to collect")
    steps.append("enable Celery task events so the observer can see lifecycle transitions")
    return tuple(steps)


def _source_lines(report: CoverageReport) -> list[str]:
    return [
        _source_line(report.redis_broker_sampling),
        _source_line(report.celery_event_stream),
        _source_line(report.app_context),
        _source_line(report.worker_topology),
        _source_line(report.routing_config),
        _source_line(report.beat_schedule),
        _source_line(report.visibility_timeout),
        _source_line(report.task_track_started),
        _source_line(report.producer_publish_tracking),
    ]


def _source_line(source: CoverageSource) -> str:
    detail = f" - {source.detail}" if source.detail else ""
    return f"- {source.label}: {source.status}{detail}"


def _normalize_app_context(
    app_context: AppContextSnapshot | None,
    *,
    app_context_loaded: bool,
    app_context_error: str,
) -> AppContextSnapshot:
    if app_context is not None:
        return app_context
    if app_context_error:
        return AppContextSnapshot(loaded=False, error=app_context_error)
    if app_context_loaded:
        return AppContextSnapshot(loaded=True)
    return unloaded_app_context()


def _routing_config_source(app_context: AppContextSnapshot) -> CoverageSource:
    if not app_context.loaded:
        return CoverageSource("Routing config", "unknown", "Routing config requires app-aware observer context.")
    if not app_context.routing_config_known:
        return CoverageSource("Routing config", "unknown", "No safe routing config facts were found on the Celery app.")
    parts = []
    if app_context.route_count is not None:
        parts.append(_count_label(app_context.route_count, "explicit route"))
    if app_context.queue_count is not None:
        parts.append(_count_label(app_context.queue_count, "configured queue"))
    if app_context.default_queue_configured:
        parts.append("default queue configured")
    return CoverageSource("Routing config", "known", "; ".join(parts) or "Routing config facts are available.")


def _beat_schedule_source(app_context: AppContextSnapshot) -> CoverageSource:
    if not app_context.loaded:
        return CoverageSource("Beat schedule", "unknown", "Beat schedule requires app-aware observer context.")
    if not app_context.beat_schedule_known:
        return CoverageSource("Beat schedule", "unknown", "No safe beat_schedule config was found on the Celery app.")
    count = app_context.beat_schedule_entry_count
    status = "empty" if count == 0 else "known"
    detail = _count_label(count or 0, "configured schedule entry")
    return CoverageSource("Beat schedule", status, detail)


def _visibility_timeout_source(app_context: AppContextSnapshot) -> CoverageSource:
    if not app_context.loaded:
        return CoverageSource("Visibility timeout", "unknown", "Visibility timeout requires app-aware observer context.")
    if app_context.visibility_timeout is None:
        return CoverageSource(
            "Visibility timeout",
            "unknown",
            "broker_transport_options.visibility_timeout is not configured or not safely readable.",
        )
    return CoverageSource("Visibility timeout", "known", f"{app_context.visibility_timeout}s")


def _task_track_started_source(app_context: AppContextSnapshot) -> CoverageSource:
    if not app_context.loaded:
        return CoverageSource("task_track_started", "unknown", "Task start tracking requires app-aware observer context.")
    if app_context.task_track_started is True:
        return CoverageSource("task_track_started", "enabled", "Received-to-started gaps can be interpreted more safely.")
    if app_context.task_track_started is False:
        return CoverageSource("task_track_started", "disabled", "Missing task-started events may be expected.")
    return CoverageSource("task_track_started", "unknown", "task_track_started is not safely readable.")


def _count_label(count: int, noun: str) -> str:
    suffix = "" if count == 1 else "s"
    return f"{count} {noun}{suffix}"


def _bullet_lines(items: tuple[str, ...]) -> list[str]:
    return [f"- {item}" for item in items] or ["- none"]


def _blocked_lines(items: tuple[BlockedClaim, ...]) -> list[str]:
    return [f"- {item.claim}: {item.reason}" for item in items] or ["- none"]


def _mode_label(config: ObserverConfig) -> str:
    return "enhanced" if config.mode == MODE_PROJECT_AWARE and config.app else "basic"


def _format_seconds(value: float) -> str:
    return f"{int(value)}s" if float(value).is_integer() else f"{value:.1f}s"


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"


def _redact_url(value: str) -> str:
    if not value:
        return ""
    redacted = redact_url_credentials(value)
    try:
        parsed = urlsplit(redacted)
    except ValueError:
        return "[invalid-url]"
    query_items = []
    for key, item_value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        safe_value = "[redacted]" if lowered in SECRET_QUERY_KEYS or lowered.endswith("_key") else item_value
        query_items.append((key, safe_value))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query_items), parsed.fragment))


def _safe_error_label(value: str) -> str:
    return value.replace("\n", " ").replace("\r", " ")[:120]
