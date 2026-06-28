# Celery Diagnostics Observer

Standalone observer-first integration for Celery Diagnostics.

The observer runs beside a Celery system. It connects to broker/event sources,
samples safe telemetry, and sends sanitized evidence to Celery Diagnostics. It
does not install code into customer web or worker processes.

## What This Package Is

- A CLI package that provides `celery-diagnostics`.
- The primary customer integration path for Celery Diagnostics.
- A passive observer for Redis broker queue depth, Celery task/worker events,
  optional Celery control inspect snapshots, observer health, transport retry,
  and local sanitized spool.

## What This Package Is Not

- It is not a broad SDK.
- It does not require changing task definitions.
- It does not require a custom Celery `Task` base.
- It does not collect task args, kwargs, results, raw tracebacks, frame locals,
  or task payloads by default.
- It must not claim worker topology or zero consumers from Redis queue depth
  alone.

## Install

Released package:

```bash
python -m pip install --upgrade celery-diagnostics
```

Local development from this directory:

```bash
python -m pip install --upgrade -e ".[dev]"
```

If the shell cannot find `celery-diagnostics`, activate the virtual environment
where it was installed or use:

```bash
python -m celery_diagnostics_observer --help
```

## Quick Start

```bash
CD_PROJECT_KEY=cd_xxx \
CELERY_BROKER_URL=redis://YOUR_REDIS_HOST:6379/0 \
celery-diagnostics observe \
  --queues default,emails \
  --ingest-url https://ingest.celerydiagnostics.com
```

`CD_PROJECT_KEY` is intentionally environment-only. The CLI does not support a
`--project-key` option so project keys are less likely to appear in shell
history or process listings.

## Commands

### `observe`

Run the long-lived observer process:

```bash
CD_PROJECT_KEY=cd_xxx \
CELERY_BROKER_URL=redis://localhost:6379/0 \
celery-diagnostics observe --queues default
```

`observe` prints a startup coverage summary before the runtime loops start. In
dry-run mode, stdout stays machine-readable JSON lines.

### `doctor`

Explain what Celery Diagnostics can and cannot currently know:

```bash
CELERY_BROKER_URL=redis://localhost:6379/0 \
celery-diagnostics doctor
```

`doctor` does not require `CD_PROJECT_KEY`. It reports telemetry coverage,
safe claims, blocked claims, and next steps. It is a diagnostic coverage
explanation, not a fake health check.

### `status`

Show lightweight local integration status:

```bash
celery-diagnostics status
```

Use `doctor` when you need detailed telemetry coverage.

## Configuration

| Variable | Purpose |
| --- | --- |
| `CD_PROJECT_KEY` | Project key used by `observe` when sending telemetry. Required for non-dry-run `observe`. |
| `CELERY_BROKER_URL` | Celery broker URL. Redis is the current observer target. |
| `CD_QUEUES` | Comma-separated queue names to sample when `--queues` is not provided. |
| `CD_INGEST_URL` | Celery Diagnostics ingest base URL. Defaults to `http://127.0.0.1:8000`. |
| `CD_TELEMETRY_POLICY` | Privacy policy: `private`, `balanced`, `detailed`, or `custom`. Defaults to `balanced`. |
| `CD_OBSERVER_MODE` | `standalone` or `project-aware`. Defaults to `standalone`. |
| `CELERY_APP` | Celery app import path used by project-aware mode. |
| `CD_SAMPLE_INTERVAL` | Redis queue sample interval in seconds. |
| `CD_INSPECT_INTERVAL` | Celery control inspect interval in seconds. |
| `CD_BATCH_SIZE` | HTTP transport batch size. |
| `CD_FLUSH_INTERVAL` | HTTP transport flush interval in seconds. |
| `CD_SPOOL_PATH` | Optional sanitized JSONL local spool path. |
| `CD_LOG_LEVEL` | Python logging level. |

Every CLI option other than the project key can also be passed as an argument.
For example:

```bash
celery-diagnostics observe \
  --broker redis://localhost:6379/0 \
  --queues default \
  --policy balanced
```

## Privacy Defaults

The observer sanitizes telemetry before it leaves the customer environment.

Default behavior:

- no task args;
- no task kwargs;
- no task results;
- no task payload body;
- no raw tracebacks;
- no frame locals;
- project keys are redacted in dry-run output;
- broker and ingest URL credentials are redacted in CLI reports.

Redis queue sampling uses safe queue depth checks. Redis-only evidence can show
queue pressure and backlog symptoms, but it cannot prove that no worker is
consuming a queue.

## Local Development

Install editable dependencies:

```bash
python -m pip install --upgrade -e ".[dev]"
```

Run tests:

```bash
python -m pytest tests -q
```

Run syntax checks:

```bash
python -m py_compile celery_diagnostics_observer/*.py
```

Build a wheel:

```bash
python -m build
```

## Repository Layout

```text
celery_diagnostics_observer/
  cli.py              # celery-diagnostics command implementation
  config.py           # env and CLI config normalization
  coverage.py         # doctor/status/startup coverage model
  event_receiver.py   # Celery event stream receiver
  redis_sampler.py    # Redis queue depth sampler
  inspect_sampler.py  # optional Celery control inspect sampler
  sanitizer.py        # privacy-safe event normalization
  transport.py        # HTTP transport and retry behavior
  spool.py            # sanitized local JSONL spool
tests/
  test_*.py           # observer unit tests
```

## Release Notes

The package name is `celery-diagnostics`; the import package is
`celery_diagnostics_observer`.

Before release:

```bash
python -m pytest tests -q
python -m py_compile celery_diagnostics_observer/*.py
python -m build
```

See `CHANGELOG.md` for version notes.
