from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class JsonlSpool:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append_many(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        with self.path.open("a", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event, sort_keys=True, separators=(",", ":"), default=str))
                handle.write("\n")

    def pop_many(self, limit: int) -> list[dict[str, Any]]:
        if limit <= 0 or not self.path.exists():
            return []
        lines = self.path.read_text(encoding="utf-8").splitlines()
        selected = lines[:limit]
        remaining = lines[limit:]
        events = []
        for line in selected:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                events.append(payload)
        if remaining:
            self.path.write_text("\n".join(remaining) + "\n", encoding="utf-8")
        else:
            self.path.unlink(missing_ok=True)
        return events
