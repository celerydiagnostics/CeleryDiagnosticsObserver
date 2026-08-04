from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from typing import Any, Mapping

from cryptography.fernet import Fernet, InvalidToken


CAPSULE_VERSION = "v1"
REFERENCE_TOKEN_LENGTH = 20
RUN_REF_RE = re.compile(rf"^R-[A-Z2-7]{{{REFERENCE_TOKEN_LENGTH}}}$")
_IDENTITY_FIELDS = ("task_id", "task_name", "queue", "routing_key", "worker")
_PREFIXES = {
    "task": "T",
    "queue": "Q",
    "route": "K",
    "worker": "W",
    "run": "R",
    "observer": "O",
    "exchange": "X",
}


class IdentityCapsuleError(ValueError):
    pass


def identity_key_id(identity_key: str) -> str:
    key = _required_key(identity_key)
    return hashlib.sha256(key).hexdigest()[:12]


def identity_ref(kind: str, value: Any, *, identity_key: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    prefix = _PREFIXES.get(kind)
    if prefix is None:
        raise ValueError(f"unsupported identity kind: {kind}")
    digest = hmac.new(
        _required_key(identity_key),
        f"celery-diagnostics:{kind}:{raw}".encode("utf-8", errors="replace"),
        hashlib.sha256,
    ).digest()
    token = base64.b32encode(digest).decode("ascii").rstrip("=")[:12]
    return f"{prefix}-{token}"


def task_run_ref(task_id: Any, *, identity_key: str) -> str:
    raw = str(task_id or "").strip()
    if not raw:
        return ""
    digest = hmac.new(
        _required_key(identity_key),
        f"celery-diagnostics:run:{raw}".encode("utf-8", errors="replace"),
        hashlib.sha256,
    ).digest()
    token = base64.b32encode(digest).decode("ascii").rstrip("=")[:REFERENCE_TOKEN_LENGTH]
    return f"R-{token}"


def seal_identity(values: Mapping[str, Any], *, identity_key: str) -> str:
    payload = {
        field: str(values.get(field) or "").strip()[:255]
        for field in _IDENTITY_FIELDS
        if str(values.get(field) or "").strip()
    }
    if not payload.get("task_id"):
        raise IdentityCapsuleError("identity capsule requires task_id")
    token = _fernet(identity_key).encrypt(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return f"{CAPSULE_VERSION}.{identity_key_id(identity_key)}.{token}"


def open_identity(capsule: str, *, identity_key: str) -> dict[str, str]:
    parts = str(capsule or "").split(".", 2)
    if len(parts) != 3 or parts[0] != CAPSULE_VERSION:
        raise IdentityCapsuleError("unsupported identity capsule")
    if not hmac.compare_digest(parts[1], identity_key_id(identity_key)):
        raise IdentityCapsuleError("identity key does not match capsule")
    try:
        raw = _fernet(identity_key).decrypt(parts[2].encode("ascii"))
        decoded = json.loads(raw.decode("utf-8"))
    except (InvalidToken, UnicodeError, json.JSONDecodeError, ValueError) as error:
        raise IdentityCapsuleError("invalid identity capsule") from error
    if not isinstance(decoded, dict) or not decoded.get("task_id"):
        raise IdentityCapsuleError("identity capsule has no task_id")
    return {
        field: str(decoded.get(field) or "").strip()[:255]
        for field in _IDENTITY_FIELDS
        if str(decoded.get(field) or "").strip()
    }


def _fernet(identity_key: str) -> Fernet:
    derived = hashlib.sha256(_required_key(identity_key)).digest()
    return Fernet(base64.urlsafe_b64encode(derived))


def _required_key(identity_key: str) -> bytes:
    value = str(identity_key or "").encode("utf-8")
    if len(value) < 16:
        raise IdentityCapsuleError("identity key must contain at least 16 characters")
    return value


__all__ = [
    "IdentityCapsuleError",
    "REFERENCE_TOKEN_LENGTH",
    "RUN_REF_RE",
    "identity_key_id",
    "identity_ref",
    "open_identity",
    "seal_identity",
    "task_run_ref",
]
