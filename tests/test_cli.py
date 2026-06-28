from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from celery_diagnostics_observer.cli import main
from celery_diagnostics_observer.config import config_from_env


def test_dry_run_reads_project_key_from_env(monkeypatch, capsys):
    monkeypatch.setenv("CD_PROJECT_KEY", "cd_secret")

    exit_code = main(
        [
            "observe",
            "--broker",
            "memory://",
            "--queues",
            "default",
            "--dry-run",
            "--print-sanitized-events",
        ]
    )

    assert exit_code == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert lines
    assert all(line["project_key"] == "[redacted]" for line in lines)
    rendered = json.dumps(lines)
    assert "secret" not in rendered


def test_project_key_cli_argument_is_not_supported():
    with pytest.raises(SystemExit) as exc:
        main(["observe", "--project-key", "cd_secret", "--dry-run"])

    assert exc.value.code == 2


def test_project_key_config_override_is_ignored(monkeypatch):
    monkeypatch.delenv("CD_PROJECT_KEY", raising=False)

    config = config_from_env({"project_key": "cd_secret"})

    assert config.project_key == ""


def test_module_entrypoint_runs_cli():
    env = {**os.environ, "CD_PROJECT_KEY": "cd_secret"}

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "celery_diagnostics_observer",
            "observe",
            "--broker",
            "memory://",
            "--queues",
            "default",
            "--dry-run",
            "--print-sanitized-events",
        ],
        check=False,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "[redacted]" in result.stdout
    assert "cd_secret" not in result.stdout


def test_help_supports_observe_doctor_and_status(capsys):
    with pytest.raises(SystemExit) as exc:
        main(["--help"])

    assert exc.value.code == 0
    output = capsys.readouterr().out
    assert "observe" in output
    assert "doctor" in output
    assert "status" in output


@pytest.mark.parametrize("argv", [["observe", "--help"], ["doctor", "--help"], ["status", "--help"]])
def test_subcommand_help(argv, capsys):
    with pytest.raises(SystemExit) as exc:
        main(argv)

    assert exc.value.code == 0
    assert "usage:" in capsys.readouterr().out


@pytest.mark.parametrize("command", ["doctor", "status"])
def test_non_observe_commands_do_not_require_project_key(monkeypatch, command, capsys):
    monkeypatch.delenv("CD_PROJECT_KEY", raising=False)
    monkeypatch.setenv("CELERY_BROKER_URL", "redis://localhost:6379/0")

    exit_code = main([command])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Celery Diagnostics" in output


def test_doctor_reports_coverage_and_redacts_secrets(monkeypatch, capsys):
    monkeypatch.setenv("CELERY_BROKER_URL", "redis://:broker-secret@localhost:6379/0")
    monkeypatch.setenv("CD_INGEST_URL", "https://user:ingest-secret@ingest.example.test/api?project_key=cd_secret")
    monkeypatch.setenv("CD_PROJECT_KEY", "cd_secret")

    exit_code = main(["doctor"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Celery Diagnostics Doctor" in output
    assert "Diagnostic level: Basic Observer" in output
    assert "no_worker_consuming_queue" in output
    assert "Producer publish tracking: unavailable" in output
    assert "broker-secret" not in output
    assert "ingest-secret" not in output
    assert "cd_secret" not in output


def test_doctor_with_app_reports_enhanced_observer_without_project_key(monkeypatch, tmp_path, capsys):
    module_path = tmp_path / "demo_celery_app.py"
    module_path.write_text(
        "\n".join(
            [
                "from celery import Celery",
                "app = Celery('demo', broker='redis://localhost:6379/0')",
                "app.conf.update(",
                "    broker_transport_options={'visibility_timeout': 120},",
                "    task_track_started=True,",
                "    task_default_queue='default',",
                "    task_routes={'billing.tasks.charge': {'queue': 'billing'}},",
                "    beat_schedule={'billing-nightly': {'task': 'billing.tasks.nightly', 'schedule': 3600}},",
                ")",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delenv("CD_PROJECT_KEY", raising=False)

    exit_code = main(
        [
            "doctor",
            "--mode",
            "project-aware",
            "-A",
            "demo_celery_app:app",
            "--broker",
            "redis://:broker-secret@localhost:6379/0",
        ]
    )

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Diagnostic level: Enhanced Observer" in output
    assert "App context: loaded" in output
    assert "Routing config: known" in output
    assert "Visibility timeout: known" in output
    assert "120s" in output
    assert "task_track_started: enabled" in output
    assert "Beat schedule: known" in output
    assert "broker-secret" not in output


def test_doctor_with_bad_app_reports_failed_context(monkeypatch, capsys):
    monkeypatch.delenv("CD_PROJECT_KEY", raising=False)

    exit_code = main(["doctor", "--mode", "project-aware", "-A", "missing_demo_app:app"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "App context: failed" in output
    assert "Diagnostic level: Basic Observer" in output


def test_doctor_with_bad_app_does_not_echo_exception_message_secrets(monkeypatch, tmp_path, capsys):
    module_path = tmp_path / "bad_celery_app.py"
    module_path.write_text("raise RuntimeError('token-secret-value')\n", encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delenv("CD_PROJECT_KEY", raising=False)

    exit_code = main(["doctor", "--mode", "project-aware", "-A", "bad_celery_app:app"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "App context: failed" in output
    assert "RuntimeError" in output
    assert "token-secret-value" not in output


def test_status_is_lightweight(monkeypatch, capsys):
    monkeypatch.setenv("CELERY_BROKER_URL", "redis://localhost:6379/0")

    exit_code = main(["status"])

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Celery Diagnostics Status" in output
    assert "Run `celery-diagnostics doctor` for detailed telemetry coverage." in output
    assert "Blocked claims:" not in output


def test_existing_observer_env_names_are_preserved(monkeypatch):
    monkeypatch.setenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("CD_OBSERVER_MODE", "project-aware")
    monkeypatch.setenv("CD_SAMPLE_INTERVAL", "7")
    monkeypatch.setenv("CD_TELEMETRY_POLICY", "private")
    monkeypatch.setenv("CELERY_APP", "demo.celery:app")

    config = config_from_env({})

    assert config.broker_url == "redis://localhost:6379/0"
    assert config.mode == "project-aware"
    assert config.sample_interval == 7
    assert config.telemetry_policy == "private"
    assert config.app == "demo.celery:app"


def test_dry_run_stdout_remains_json_lines(monkeypatch, capsys):
    monkeypatch.setenv("CD_PROJECT_KEY", "cd_secret")

    exit_code = main(
        [
            "observe",
            "--broker",
            "memory://",
            "--queues",
            "default",
            "--dry-run",
            "--print-sanitized-events",
        ]
    )

    assert exit_code == 0
    captured = capsys.readouterr()
    lines = [json.loads(line) for line in captured.out.splitlines() if line.strip()]
    assert lines
    assert all(isinstance(line, dict) for line in lines)
    assert "Celery Diagnostics Observer" not in captured.out


def test_observe_missing_project_key_does_not_print_startup_summary(monkeypatch, capsys):
    monkeypatch.delenv("CD_PROJECT_KEY", raising=False)

    exit_code = main(["observe", "--broker", "redis://localhost:6379/0"])

    assert exit_code == 2
    captured = capsys.readouterr()
    assert "CD_PROJECT_KEY environment variable is required" in captured.err
    assert "Celery Diagnostics Observer" not in captured.err
