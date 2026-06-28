# Contributing

This package is the standalone observer-first integration for Celery Diagnostics.

## Local Setup

```bash
python -m pip install --upgrade -e ".[dev]"
```

## Test

```bash
python -m pytest tests -q
python -m py_compile celery_diagnostics_observer/*.py
```

## Package Check

```bash
python -m build
```

## Guardrails

- Keep `celery-diagnostics observe` as the primary integration path.
- Do not require imports in customer web or worker processes.
- Do not add `--project-key`; project keys are provided through `CD_PROJECT_KEY`.
- Do not collect task args, kwargs, results, raw tracebacks, frame locals, or broad Celery config dumps by default.
- Do not claim worker topology or zero consumers from Redis-only queue depth evidence.
