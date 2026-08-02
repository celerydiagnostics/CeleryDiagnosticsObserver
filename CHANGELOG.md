# Changelog

All notable changes to the standalone Celery Diagnostics observer package are documented here.

## 0.2.0

- Added observer-first CLI package with `celery-diagnostics observe`.
- Added `python -m celery_diagnostics_observer` fallback entrypoint.
- Added Redis queue sampling, Celery event receiver, control inspect sampling, observer health events, HTTP transport, and sanitized local spool support.
- Added telemetry coverage commands: `celery-diagnostics doctor` and `celery-diagnostics status`.
- Added startup coverage summary for real `observe` runs.
- Added Enhanced Observer app-aware coverage through `--mode project-aware -A myproject.celery:app`.
- Removed the in-process publisher and progress helpers. The distribution now
  contains the standalone Observer and the optional Beat scheduler adapter,
  with no producer or worker SDK.
- Added an optional Celery Beat scheduler adapter for privacy-safe schedule
  inventory, due decisions, and publish-failure evidence. It does not restore
  producer or worker instrumentation.

## 0.1.0

- Initial observer package skeleton.
