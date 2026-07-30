from __future__ import annotations

import json
from datetime import datetime, timezone

from celery_diagnostics_observer.config import ObserverConfig
from celery_diagnostics_observer.policy import policy_from_name
from celery_diagnostics_observer.replay import export_event_replay


def test_public_capture_replay_preserves_intervals_and_privacy(tmp_path):
    source = tmp_path / "raw.jsonl"
    destination = tmp_path / "observer.jsonl"
    source.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "type": "task-sent",
                        "uuid": "task-1",
                        "name": "jobs.run",
                        "routing_key": "jobs",
                        "timestamp": 100.0,
                        "local_received": 100.2,
                        "argsrepr": "('secret',)",
                    }
                ),
                json.dumps(
                    {
                        "type": "task-started",
                        "uuid": "task-1",
                        "hostname": "worker-a",
                        "timestamp": 102.0,
                        "local_received": 102.2,
                        "kwargs": {"token": "secret"},
                    }
                ),
                json.dumps(
                    {
                        "type": "task-succeeded",
                        "uuid": "task-1",
                        "timestamp": 110.0,
                        "local_received": 110.2,
                        "result": {"secret": True},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config = ObserverConfig(
        broker_url="",
        queues=("jobs",),
        project_key="cf_replay",
        ingest_url="http://ingest",
        observer_id="research-replay",
        telemetry_policy="detailed",
    )

    result = export_event_replay(
        source,
        destination,
        config=config,
        policy=policy_from_name("detailed"),
        cutoff=105.0,
        anchor=datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc),
    )

    rows = [
        json.loads(line)
        for line in destination.read_text(encoding="utf-8").splitlines()
    ]
    assert result.read == 3
    assert result.before_cutoff == 2
    assert result.exported == 2
    assert rows[0]["research_replay"] == {
        "public_cutoff": 105.0,
        "cutoff_anchor": "2026-07-29T12:00:00+00:00",
        "event_intervals_preserved": True,
    }
    published = datetime.fromisoformat(rows[0]["event_timestamp"]).timestamp()
    started = datetime.fromisoformat(rows[1]["event_timestamp"]).timestamp()
    assert started - published == 2.0
    rendered = destination.read_text(encoding="utf-8")
    assert "secret" not in rendered
    assert "argsrepr" not in rendered
    assert "kwargs" not in rendered
