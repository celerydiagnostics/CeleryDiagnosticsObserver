from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


class JsonlSpool:
    def __init__(self, path: str):
        self.path = Path(path)
        parent_existed = self.path.parent.exists()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not parent_existed and not self.path.parent.is_symlink():
            self.path.parent.chmod(0o700)

    def append_many(self, events: list[dict[str, Any]]) -> None:
        if not events:
            return
        flags = os.O_APPEND | os.O_CREAT | os.O_WRONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(self.path, flags, 0o600)
        os.chmod(self.path, 0o600)
        with os.fdopen(descriptor, "a", encoding="utf-8") as handle:
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
            self.path.chmod(0o600)
        else:
            self.path.unlink(missing_ok=True)
        return events
