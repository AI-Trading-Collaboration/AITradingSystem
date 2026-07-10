from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Generic, TypeVar

from pydantic import BaseModel, ValidationError

from ai_trading_system.yaml_loader import safe_load_yaml_path

ConfigModelT = TypeVar("ConfigModelT", bound=BaseModel)


class ConfigResolutionError(ValueError):
    def __init__(self, code: str, path: Path, message: str) -> None:
        self.code = code
        self.path = path
        self.message = message
        super().__init__(f"{code}: {path}: {message}")


@dataclass(frozen=True)
class ConfigRef:
    policy_id: str
    version: str
    status: str
    path: str
    sha256: str
    loaded_at: datetime

    def __post_init__(self) -> None:
        if not all(
            value.strip() for value in (self.policy_id, self.version, self.status, self.path)
        ):
            raise ValueError("ConfigRef text fields must be non-empty")
        if len(self.sha256) != 64 or any(char not in "0123456789abcdef" for char in self.sha256):
            raise ValueError("ConfigRef sha256 must be lowercase hex")
        if self.loaded_at.tzinfo is None or self.loaded_at.utcoffset() is None:
            raise ValueError("ConfigRef loaded_at must be timezone-aware")

    def to_dict(self) -> dict[str, str]:
        return {
            "policy_id": self.policy_id,
            "version": self.version,
            "status": self.status,
            "path": self.path,
            "sha256": self.sha256,
            "loaded_at": self.loaded_at.isoformat(),
        }


@dataclass(frozen=True)
class ResolvedConfig(Generic[ConfigModelT]):
    value: ConfigModelT
    reference: ConfigRef


def resolve_yaml_config(
    path: Path | str,
    model_type: type[ConfigModelT],
    *,
    policy_id: str,
    version: str | None = None,
    status: str | None = None,
    loaded_at: datetime | None = None,
) -> ResolvedConfig[ConfigModelT]:
    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigResolutionError("CONFIG_FILE_MISSING", config_path, "file does not exist")
    timestamp = loaded_at or datetime.now(tz=UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ConfigResolutionError(
            "CONFIG_LOADED_AT_TZ_REQUIRED", config_path, "loaded_at must be timezone-aware"
        )
    try:
        raw = safe_load_yaml_path(config_path)
    except OSError as exc:
        raise ConfigResolutionError("CONFIG_READ_FAILED", config_path, str(exc)) from exc
    if not isinstance(raw, Mapping):
        raise ConfigResolutionError("CONFIG_ROOT_NOT_MAPPING", config_path, type(raw).__name__)
    payload = dict(raw)
    try:
        value = model_type.model_validate(payload)
    except ValidationError as exc:
        raise ConfigResolutionError("CONFIG_SCHEMA_INVALID", config_path, str(exc)) from exc
    resolved_version = version or _first_text(
        payload.get("schema_version"),
        payload.get("policy_version"),
        payload.get("version"),
        default="legacy-unversioned",
    )
    metadata = payload.get("policy_metadata")
    metadata_status = metadata.get("status") if isinstance(metadata, Mapping) else None
    resolved_status = status or _first_text(
        payload.get("status"), metadata_status, default="active-legacy"
    )
    reference = ConfigRef(
        policy_id=policy_id,
        version=resolved_version,
        status=resolved_status,
        path=str(config_path),
        sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
        loaded_at=timestamp,
    )
    return ResolvedConfig(value=value, reference=reference)


def _first_text(*values: object, default: str) -> str:
    for value in values:
        if value not in (None, ""):
            return str(value)
    return default
