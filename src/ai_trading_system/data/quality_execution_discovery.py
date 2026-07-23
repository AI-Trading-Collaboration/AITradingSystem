from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import ClassVar

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_execution import (
    DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DataQualityExecutionContractError,
    DataQualityExecutionReceipt,
)
from ai_trading_system.data.immutable_publish import (
    DataPublicationError,
    read_contained_artifact_bytes,
    write_contained_artifact_bytes,
)
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionResult,
    DataQualityExecutionError,
    verify_daily_default_execution_profile_receipt,
)

DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID = DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
DISCOVERY_POINTER_SCHEMA_VERSION = "data_quality_execution_discovery_pointer.v1"
_DEFAULT_PROFILE_PATH_COMPONENT = "daily_default"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_RECEIPT_ID_PATTERN = re.compile(r"^dq_execution_[0-9a-f]{64}$")
_WINDOWS_DRIVE_PATTERN = re.compile(r"^[A-Za-z]:")
_POINTER_KEYS = frozenset(
    {
        "schema_version",
        "profile_id",
        "as_of",
        "published_at",
        "receipt_id",
        "receipt_path",
        "receipt_sha256",
        "receipt_size_bytes",
    }
)


@dataclass(frozen=True)
class DataQualityExecutionDiscoveryPointer:
    schema_version: ClassVar[str] = DISCOVERY_POINTER_SCHEMA_VERSION

    profile_id: str
    as_of: date
    published_at: datetime
    receipt_id: str
    receipt_path: str
    receipt_sha256: str
    receipt_size_bytes: int

    def __post_init__(self) -> None:
        if self.profile_id != DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID",
                f"unsupported discovery profile={self.profile_id!r}",
            )
        _date_value(self.as_of, "as_of")
        _utc_datetime(self.published_at, "published_at")
        if not _RECEIPT_ID_PATTERN.fullmatch(self.receipt_id):
            raise DataQualityExecutionError(
                "DQ_RECEIPT_ID_MISMATCH", f"invalid receipt_id={self.receipt_id!r}"
            )
        checked_path = _repo_relative_posix_path(self.receipt_path, "receipt_path")
        expected_path = _canonical_receipt_path(self.receipt_id)
        if checked_path != expected_path:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_ID_MISMATCH",
                f"expected receipt_path={expected_path} actual={checked_path}",
            )
        if not _SHA256_PATTERN.fullmatch(self.receipt_sha256):
            raise DataQualityExecutionError(
                "DQ_RECEIPT_ID_MISMATCH", "receipt_sha256 must be lowercase SHA-256"
            )
        if (
            not isinstance(self.receipt_size_bytes, int)
            or isinstance(self.receipt_size_bytes, bool)
            or self.receipt_size_bytes <= 0
        ):
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID", "receipt_size_bytes must be positive"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "profile_id": self.profile_id,
            "as_of": self.as_of.isoformat(),
            "published_at": self.published_at.isoformat(),
            "receipt_id": self.receipt_id,
            "receipt_path": self.receipt_path,
            "receipt_sha256": self.receipt_sha256,
            "receipt_size_bytes": self.receipt_size_bytes,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, content: bytes) -> DataQualityExecutionDiscoveryPointer:
        try:
            decoded = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID", "discovery pointer is not UTF-8"
            ) from exc
        payload = _strict_json_mapping(decoded)
        keys = set(payload)
        if keys != _POINTER_KEYS:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID",
                f"discovery keys mismatch missing={sorted(_POINTER_KEYS - keys)} "
                f"unknown={sorted(keys - _POINTER_KEYS)}",
            )
        schema_version = _text(payload.get("schema_version"), "schema_version")
        if schema_version != cls.schema_version:
            raise DataQualityExecutionError("DQ_RECEIPT_SCHEMA_UNSUPPORTED", schema_version)
        pointer = cls(
            profile_id=_text(payload.get("profile_id"), "profile_id"),
            as_of=_date_value(payload.get("as_of"), "as_of"),
            published_at=_utc_datetime(payload.get("published_at"), "published_at"),
            receipt_id=_text(payload.get("receipt_id"), "receipt_id"),
            receipt_path=_repo_relative_posix_path(payload.get("receipt_path"), "receipt_path"),
            receipt_sha256=_text(payload.get("receipt_sha256"), "receipt_sha256"),
            receipt_size_bytes=_positive_int(
                payload.get("receipt_size_bytes"), "receipt_size_bytes"
            ),
        )
        if content != pointer.canonical_bytes:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID",
                "discovery pointer bytes are not canonical",
            )
        return pointer


@dataclass(frozen=True)
class DiscoveredDataQualityExecution:
    pointer_path: Path
    pointer: DataQualityExecutionDiscoveryPointer
    receipt_path: Path
    receipt: DataQualityExecutionReceipt


def default_data_quality_execution_discovery_path(
    as_of: date,
    *,
    project_root: Path = PROJECT_ROOT,
) -> Path:
    checked_as_of = _date_value(as_of, "as_of")
    return project_root.resolve() / Path(_discovery_relative_path(checked_as_of))


def publish_default_data_quality_execution_discovery(
    result: CanonicalDataQualityExecutionResult,
    *,
    project_root: Path = PROJECT_ROOT,
) -> DiscoveredDataQualityExecution:
    if not isinstance(result, CanonicalDataQualityExecutionResult):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            "result must be CanonicalDataQualityExecutionResult",
        )
    root = project_root.resolve()
    receipt_relative, receipt_absolute = _contained_path(root, result.receipt_path)
    receipt, receipt_bytes = _load_receipt_bytes(root, receipt_relative)
    if receipt.canonical_bytes != result.receipt.canonical_bytes:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH", "result receipt differs from receipt on disk"
        )
    verify_daily_default_execution_profile_receipt(receipt, project_root=root)
    published_at = _utc_datetime(_utc_now(), "published_at")
    if published_at < receipt.ended_at:
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID",
            "published_at cannot precede receipt ended_at",
        )
    pointer = DataQualityExecutionDiscoveryPointer(
        profile_id=DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        as_of=receipt.as_of,
        published_at=published_at,
        receipt_id=receipt.receipt_id,
        receipt_path=receipt_relative,
        receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        receipt_size_bytes=len(receipt_bytes),
    )
    expected_pointer_path = default_data_quality_execution_discovery_path(
        receipt.as_of, project_root=root
    )
    pointer_relative, pointer_path = _contained_path(root, expected_pointer_path)
    if pointer_relative != _discovery_relative_path(receipt.as_of):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "discovery pointer path containment mismatch"
        )
    _write_pointer_bytes(root, pointer_relative, pointer.canonical_bytes)
    return load_default_data_quality_execution_discovery(receipt.as_of, project_root=root)


def load_default_data_quality_execution_discovery(
    as_of: date,
    *,
    project_root: Path = PROJECT_ROOT,
) -> DiscoveredDataQualityExecution:
    checked_as_of = _date_value(as_of, "as_of")
    root = project_root.resolve()
    expected_pointer_path = default_data_quality_execution_discovery_path(
        checked_as_of, project_root=root
    )
    pointer_relative, pointer_path = _contained_path(root, expected_pointer_path)
    if pointer_relative != _discovery_relative_path(checked_as_of):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "discovery pointer path containment mismatch"
        )
    pointer_bytes = _read_secure_bytes(root, pointer_relative, "DQ_RECEIPT_MISSING")
    pointer = DataQualityExecutionDiscoveryPointer.from_json_bytes(pointer_bytes)
    if pointer.profile_id != DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID:
        raise DataQualityExecutionError("DQ_RECEIPT_FIELDS_INVALID", "discovery profile mismatch")
    if pointer.as_of != checked_as_of:
        raise DataQualityExecutionError(
            "DQ_AS_OF_MISMATCH",
            f"expected={checked_as_of.isoformat()} actual={pointer.as_of.isoformat()}",
        )
    receipt_relative, receipt_absolute = _contained_path(root, root / Path(pointer.receipt_path))
    if receipt_relative != pointer.receipt_path:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH", "receipt path normalization mismatch"
        )
    receipt_bytes = _read_secure_bytes(root, receipt_relative, "DQ_RECEIPT_MISSING")
    if (
        hashlib.sha256(receipt_bytes).hexdigest() != pointer.receipt_sha256
        or len(receipt_bytes) != pointer.receipt_size_bytes
    ):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH", "discovered receipt bytes mismatch"
        )
    try:
        receipt = DataQualityExecutionReceipt.from_json_bytes(receipt_bytes)
    except DataQualityExecutionContractError as exc:
        raise DataQualityExecutionError(exc.code, exc.message) from exc
    if receipt.receipt_id != pointer.receipt_id:
        raise DataQualityExecutionError("DQ_RECEIPT_ID_MISMATCH", "pointer receipt_id mismatch")
    if receipt_relative != _canonical_receipt_path(receipt.receipt_id):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH", "discovered receipt path is not content-addressed"
        )
    if receipt.as_of != checked_as_of:
        raise DataQualityExecutionError("DQ_AS_OF_MISMATCH", "discovered receipt as_of mismatch")
    verify_daily_default_execution_profile_receipt(receipt, project_root=root)
    if pointer.published_at < receipt.ended_at:
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID",
            "pointer published_at precedes receipt ended_at",
        )
    return DiscoveredDataQualityExecution(
        pointer_path=pointer_path,
        pointer=pointer,
        receipt_path=receipt_absolute,
        receipt=receipt,
    )


def _discovery_relative_path(as_of: date) -> str:
    return (
        "outputs/data_quality/executions/discovery/"
        f"{_DEFAULT_PROFILE_PATH_COMPONENT}/{as_of.isoformat()}/current.json"
    )


def _canonical_receipt_path(receipt_id: str) -> str:
    return f"outputs/data_quality/executions/{receipt_id}/receipt.json"


def _contained_path(root: Path, value: Path) -> tuple[str, Path]:
    if value.is_symlink():
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", f"path must not be a symlink: {value}"
        )
    absolute = value.resolve(strict=False)
    try:
        relative = absolute.relative_to(root)
    except ValueError as exc:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", f"path is outside project root: {value}"
        ) from exc
    relative_text = relative.as_posix()
    _repo_relative_posix_path(relative_text, "path")
    return relative_text, absolute


def _load_receipt_bytes(
    root: Path,
    relative_path: str,
) -> tuple[DataQualityExecutionReceipt, bytes]:
    content = _read_secure_bytes(root, relative_path, "DQ_RECEIPT_MISSING")
    try:
        receipt = DataQualityExecutionReceipt.from_json_bytes(content)
    except DataQualityExecutionContractError as exc:
        raise DataQualityExecutionError(exc.code, exc.message) from exc
    if relative_path != _canonical_receipt_path(receipt.receipt_id):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH", "receipt is not at its content-addressed path"
        )
    return receipt, content


def _write_pointer_bytes(root: Path, relative_path: str, content: bytes) -> None:
    try:
        result = write_contained_artifact_bytes(
            root=root,
            relative_path=relative_path,
            content=content,
            immutable=False,
        )
    except DataPublicationError as exc:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            f"secure discovery pointer write failed: {exc}",
        ) from exc
    if result.sha256 != hashlib.sha256(content).hexdigest() or result.size_bytes != len(content):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH", "secure discovery pointer attestation mismatch"
        )


def _read_secure_bytes(root: Path, relative_path: str, missing_code: str) -> bytes:
    try:
        return read_contained_artifact_bytes(root=root, relative_path=relative_path)
    except DataPublicationError as exc:
        code = (
            missing_code
            if exc.code in {"CONTAINED_ARTIFACT_MISSING", "ARTIFACT_BOUND_DIRECTORY_FAILED"}
            else "DQ_RECEIPT_FIELDS_INVALID"
        )
        raise DataQualityExecutionError(
            code,
            f"secure contained artifact read failed: {exc}",
        ) from exc


def _strict_json_mapping(value: str) -> Mapping[str, object]:
    try:
        parsed = json.loads(
            value,
            object_pairs_hook=_strict_object_pairs,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError as exc:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "invalid discovery JSON"
        ) from exc
    if not isinstance(parsed, Mapping):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", "discovery pointer must be an object"
        )
    return parsed


def _strict_object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID", f"duplicate JSON key: {key}"
            )
        result[key] = value
    return result


def _reject_json_constant(value: str) -> None:
    raise DataQualityExecutionError(
        "DQ_RECEIPT_FIELDS_INVALID", f"non-finite JSON constant: {value}"
    )


def _text(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise DataQualityExecutionError("DQ_RECEIPT_FIELDS_INVALID", f"{field} is required")
    return value


def _date_value(value: object, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID", f"invalid {field}"
            ) from exc
    raise DataQualityExecutionError("DQ_RECEIPT_FIELDS_INVALID", f"{field} must be an ISO date")


def _utc_datetime(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID", f"invalid {field}"
            ) from exc
    else:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be an ISO datetime"
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID", f"{field} requires timezone"
        )
    if parsed.utcoffset() != timedelta(0):
        raise DataQualityExecutionError("DQ_EXECUTION_CHRONOLOGY_INVALID", f"{field} must be UTC")
    return parsed


def _positive_int(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise DataQualityExecutionError("DQ_RECEIPT_FIELDS_INVALID", f"{field} must be positive")
    return value


def _repo_relative_posix_path(value: object, field: str) -> str:
    path = _text(value, field)
    parsed = PurePosixPath(path)
    if (
        "\\" in path
        or path.startswith("/")
        or _WINDOWS_DRIVE_PATTERN.match(path)
        or any(part in {"", ".", ".."} for part in parsed.parts)
        or str(parsed) != path
    ):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            f"{field} must be a normalized repo-relative POSIX path",
        )
    return path


def _utc_now() -> datetime:
    return datetime.now(UTC)
