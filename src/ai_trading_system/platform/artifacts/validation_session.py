from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from functools import wraps
from hashlib import sha256
from pathlib import Path
from typing import Any, ParamSpec, TypeVar

_P = ParamSpec("_P")
_R = TypeVar("_R")
_ValidationKey = tuple[str, str, str]
_VALIDATION_SESSION: ContextVar[dict[_ValidationKey, dict[str, Any]] | None] = ContextVar(
    "artifact_validation_session",
    default=None,
)


def artifact_fingerprint(root: Path) -> str:
    """Hash one artifact and every live file referenced by checksum bindings."""
    pending = (
        {path.resolve() for path in root.iterdir() if path.is_file()} if root.is_dir() else set()
    )
    observed: dict[Path, bytes | None] = {}
    while pending:
        path = min(pending, key=str)
        pending.remove(path)
        if path in observed:
            continue
        try:
            payload = path.read_bytes()
        except OSError:
            observed[path] = None
            continue
        observed[path] = payload
        if path.suffix.lower() != ".json":
            continue
        try:
            document = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        stack = [document]
        while stack:
            value = stack.pop()
            if isinstance(value, Mapping):
                bound_path = value.get("path")
                if isinstance(bound_path, str) and "sha256" in value:
                    pending.add(Path(bound_path).resolve())
                stack.extend(value.values())
            elif isinstance(value, list):
                stack.extend(value)

    digest = sha256()
    for path in sorted(observed, key=str):
        digest.update(str(path).encode("utf-8"))
        payload = observed[path]
        if payload is None:
            digest.update(b"\0MISSING\0")
        else:
            digest.update(len(payload).to_bytes(8, "big"))
            digest.update(payload)
    return digest.hexdigest()


@contextmanager
def artifact_validation_session() -> Iterator[None]:
    """Reuse PASS reports only while their complete artifact fingerprint is unchanged."""
    current = _VALIDATION_SESSION.get()
    if current is not None:
        yield
        return
    token = _VALIDATION_SESSION.set({})
    try:
        yield
    finally:
        _VALIDATION_SESSION.reset(token)


def with_artifact_validation_session(function: Callable[_P, _R]) -> Callable[_P, _R]:
    @wraps(function)
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        with artifact_validation_session():
            return function(*args, **kwargs)

    return wrapped


def cached_artifact_validation(
    *,
    validator: Callable[..., dict[str, Any]],
    validator_key: str,
    artifact_id: str,
    root: Path,
) -> dict[str, Any]:
    """Validate once per identity/path/content tuple inside an active session."""
    cache = _VALIDATION_SESSION.get()
    if cache is None:
        return validator(**{validator_key: artifact_id, "output_dir": root})

    artifact_root = root / artifact_id
    fingerprint = artifact_fingerprint(artifact_root)
    validator_name = f"{validator.__module__}.{validator.__qualname__}:{validator_key}"
    key = (validator_name, str(artifact_root.resolve()), fingerprint)
    cached = cache.get(key)
    if cached is not None:
        return deepcopy(cached)

    validation = validator(**{validator_key: artifact_id, "output_dir": root})
    if validation.get("status") == "PASS":
        cache[key] = deepcopy(validation)
    return validation
