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
- An executor for bounded, read-only diagnostic checks requested by Celery
  Diagnostics. Checks cover task presence, reservation, scheduling, worker
  capacity and presence, queue consumers, status-only JSON result records in
  Redis, and task-specific presence in configured Redis queues.
- An optional app-aware observer when run with `-A myproject.celery:app`; this
  loads the Celery app inside the observer process to explain routing,
  visibility timeout, `task_track_started`, and beat schedule coverage.
- A narrow optional Publisher Probe API for producer processes that need
  publish attempt, broker-accepted, and explicit publish-failure evidence.

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

Enhanced observer mode loads your Celery app in the observer process only:

```bash
CD_PROJECT_KEY=cd_xxx \
celery-diagnostics observe \
  --mode project-aware \
  -A myproject.celery:app
```

`observe` prints a startup coverage summary before the runtime loops start. In
dry-run mode, stdout stays machine-readable JSON lines.

Read-only diagnostic checks are enabled by default. Disable them explicitly
when the observer must remain passive:

```bash
celery-diagnostics observe --no-active-probes
```

For Redis message-presence checks, the observer scans only configured queues,
uses the default Kombu priority layout, applies a bounded scan limit, and reads
only the protocol-v2 task id header. A missing task is reported only when all
scanned lists were stable and decodable; partial or malformed observations
remain inconclusive.

Result-backend status checks are advertised only for a Redis backend with the
JSON result serializer. A server-side Redis script returns the `status` field
alone, so the Observer does not fetch the task's result value. Other result
backends and serializers remain unsupported rather than silently loading a
private result record through `AsyncResult.state`.

### `doctor`

Explain what Celery Diagnostics can and cannot currently know:

```bash
CELERY_BROKER_URL=redis://localhost:6379/0 \
celery-diagnostics doctor
```

For app-aware coverage:

```bash
celery-diagnostics doctor --mode project-aware -A myproject.celery:app
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

## Publisher Probe

Publisher Probe is optional producer-side instrumentation. It is useful when
you need to distinguish a producer publish failure from a task that was
accepted by the broker but never picked up by a worker.

```python
from celery_diagnostics.publisher import track_task_publishing

track_task_publishing(app)
```

Configuration uses the same project key convention as the observer:

```bash
CD_PROJECT_KEY=cd_xxx
CD_INGEST_URL=https://ingest.celerydiagnostics.com
```

For explicit publish failures in code paths where you already catch the
exception:

```python
from celery_diagnostics.publisher import record_publish_failed

try:
    task.apply_async(task_id=task_id)
except Exception as exc:
    record_publish_failed(task_id=task_id, task_name=task.name, exception=exc)
    raise
```

The probe is fail-open. Missing project key, network failure, or telemetry
errors do not change Celery publish behavior.

## Configuration

| Variable | Purpose |
| --- | --- |
| `CD_PROJECT_KEY` | Project key used by `observe` and Publisher Probe when sending telemetry. Required for non-dry-run `observe`; Publisher Probe disables itself when missing. |
| `CELERY_BROKER_URL` | Celery broker URL. Redis is the current observer target. |
| `CD_QUEUES` | Comma-separated queue names to sample when `--queues` is not provided. |
| `CD_INGEST_URL` | Celery Diagnostics ingest base URL. Defaults to `http://127.0.0.1:8000`. |
| `CD_TELEMETRY_POLICY` | Privacy policy: `private`, `balanced`, `detailed`, or `custom`. Defaults to `balanced`. |
| `CD_OBSERVER_MODE` | `standalone` or `project-aware`. Defaults to `standalone`. |
| `CELERY_APP` | Celery app import path used by project-aware mode, equivalent to `-A`. |
| `CD_SAMPLE_INTERVAL` | Redis queue sample interval in seconds. |
| `CD_INSPECT_INTERVAL` | Celery control inspect interval in seconds. |
| `CD_BATCH_SIZE` | HTTP transport batch size. |
| `CD_FLUSH_INTERVAL` | HTTP transport flush interval in seconds. |
| `CD_SPOOL_PATH` | Optional sanitized JSONL local spool path. |
| `CD_LOG_LEVEL` | Python logging level. |
| `CD_ACTIVE_PROBES` | Enable bounded read-only diagnostic checks. Defaults to `1`. |
| `CD_BROKER_MESSAGE_SCAN_LIMIT` | Maximum Redis messages inspected by one task-presence check. Defaults to `10000`, bounded to `100000`. |

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

Project-aware mode uses an allowlist of app configuration facts. It does not
dump arbitrary Celery config, task payloads, broker credentials, or exception
messages from failed app imports.

Publisher Probe sends only publish lifecycle fields: task id, task name or
hashed task label, queue/routing/exchange labels, correlation ids already in
Celery headers, and exception type/module for explicit publish failures. It
does not send task args, kwargs, message body, raw headers, exception messages,
or stack frames.

## Optional Progress Obligation

Long-running tasks can declare a privacy-safe liveness obligation without
reporting business progress:

```python
from celery_diagnostics import report_task_progress

report_task_progress(self, sequence=4, max_silence_seconds=30)
```

The helper emits only a monotonic sequence number and the declared maximum
silence interval. It does not send percentages, customer data, task arguments,
results, or arbitrary progress values. Celery Diagnostics treats silence as a
stall only after the declared interval expires and the event source is still
fresh; otherwise the execution state remains unknown.

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
python -m py_compile celery_diagnostics/*.py celery_diagnostics_observer/*.py
```

Build a wheel:

```bash
python -m build
```

## Repository Layout

```text
celery_diagnostics/
  publisher.py        # optional producer-side publish tracker
  progress.py         # optional privacy-safe progress obligation
celery_diagnostics_observer/
  active_probes.py    # bounded on-demand read-only checks
  capabilities.py     # advertised active-check capabilities
  app_context.py      # allowlisted app-aware config extraction
  cli.py              # celery-diagnostics command implementation
  config.py           # env and CLI config normalization
  coverage.py         # doctor/status/startup coverage model
  event_receiver.py   # Celery event stream receiver
  redis_sampler.py    # Redis queue depth sampler
  redis_message_probe.py # bounded task-id presence check for Redis
  inspect_sampler.py  # optional Celery control inspect sampler
  sanitizer.py        # privacy-safe event normalization
  transport.py        # HTTP transport and retry behavior
  spool.py            # sanitized local JSONL spool
tests/
  test_*.py           # observer unit tests
```

## Release Notes

The package name is `celery-diagnostics`; observer code imports as
`celery_diagnostics_observer`, and the optional publisher API imports as
`celery_diagnostics.publisher`.

Before release:

```bash
python -m pytest tests -q
python -m py_compile celery_diagnostics/*.py celery_diagnostics_observer/*.py
python -m build
```

See `CHANGELOG.md` for version notes.
