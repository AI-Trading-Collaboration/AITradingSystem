from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import PurePosixPath
from typing import ClassVar, Self

from ai_trading_system.contracts.data_quality import DataQualityEvidence, DataQualityEvidenceError
from ai_trading_system.contracts.status import PolicyRole

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_WINDOWS_DRIVE_PATTERN = re.compile(r"^[A-Za-z]:")
_MANIFEST_RECORD_REF_PATTERN = re.compile(r"^manifest_record_[0-9a-f]{16,64}$")
_PYTHON_ENTRYPOINT_PATTERN = re.compile(
    r"^(?P<module>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*):"
    r"(?P<callable>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)$"
)
_REPORT_STATUSES = frozenset({"PASS", "PASS_WITH_WARNINGS", "FAIL"})
_PRODUCTION_EFFECT_NONE = "none"
_VERIFIED_PREFLIGHT_SEAL = object()

# These identities govern discovery/publication behavior, not investment policy.
# Keeping them in the shared contract prevents CLI-only string drift.
DAILY_DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID = "daily_default.v1"
MANUAL_DATA_QUALITY_EXECUTION_PROFILE_ID = "manual.v1"


class DataQualityExecutionContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _text_value(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise DataQualityExecutionContractError("DQ_RECEIPT_FIELDS_INVALID", f"{field} is required")
    return value


def _require_text(value: object, field: str) -> None:
    _text_value(value, field)


def _optional_text_value(value: object, field: str) -> str | None:
    return None if value is None else _text_value(value, field)


def _repo_relative_path(value: object, field: str) -> str:
    path = _text_value(value, field)
    parsed = PurePosixPath(path)
    if (
        "\\" in path
        or path.startswith("/")
        or _WINDOWS_DRIVE_PATTERN.match(path)
        or any(part in {"", ".", ".."} for part in parsed.parts)
        or str(parsed) != path
    ):
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be a normalized repo-relative POSIX path"
        )
    return path


def _sha256(value: object, field: str) -> str:
    checked = _text_value(value, field)
    if not _SHA256_PATTERN.fullmatch(checked):
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be lowercase SHA-256"
        )
    return checked


def _date_value(value: object, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", f"{field}={value!r}"
            ) from exc
    raise DataQualityExecutionContractError(
        "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be an ISO date"
    )


def _datetime_value(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", f"{field}={value!r}"
            ) from exc
    else:
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be an ISO datetime"
        )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DataQualityExecutionContractError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID", f"{field} must include timezone"
        )
    if parsed.utcoffset() != timedelta(0):
        raise DataQualityExecutionContractError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID", f"{field} must be normalized to UTC"
        )
    return parsed


def _int_value(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be a non-negative integer"
        )
    return value


def _optional_int_value(value: object, field: str) -> int | None:
    return None if value is None else _int_value(value, field)


def _bool_value(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be boolean"
        )
    return value


def _mapping_value(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be a mapping"
        )
    return value


def _require_exact_keys(
    payload: Mapping[str, object], expected: frozenset[str], field: str
) -> None:
    keys = set(payload.keys())
    if keys != expected:
        missing = sorted(expected - keys)
        unknown = sorted(str(item) for item in keys - expected)
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID",
            f"{field} keys mismatch missing={missing} unknown={unknown}",
        )


def _list_value(value: object, field: str) -> list[object]:
    if not isinstance(value, list):
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} must be a list"
        )
    return value


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    raw = _list_value(value, field)
    return tuple(_text_value(item, field) for item in raw)


def _normalize_unique_strings(values: tuple[str, ...], field: str) -> tuple[str, ...]:
    checked = tuple(_text_value(item, field) for item in values)
    if len(set(checked)) != len(checked):
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"{field} cannot contain duplicate values"
        )
    return tuple(sorted(checked))


def _reject_json_constant(value: str) -> None:
    raise DataQualityExecutionContractError(
        "DQ_RECEIPT_FIELDS_INVALID", f"non-finite JSON constant is forbidden: {value}"
    )


def _strict_object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", f"duplicate JSON key: {key}"
            )
        result[key] = value
    return result


def _strict_json_loads(value: str) -> object:
    try:
        parsed = json.loads(
            value,
            object_pairs_hook=_strict_object_pairs,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError as exc:
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", "invalid JSON"
        ) from exc
    _validate_json_value(parsed, "invocation")
    return parsed


def _validate_json_value(value: object, field: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", f"{field} contains a non-finite number"
            )
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{field}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise DataQualityExecutionContractError(
                    "DQ_RECEIPT_FIELDS_INVALID", f"{field} contains a non-string object key"
                )
            _validate_json_value(item, f"{field}.{key}")
        return
    raise DataQualityExecutionContractError(
        "DQ_RECEIPT_FIELDS_INVALID", f"{field} contains an unsupported JSON value"
    )


def canonical_json_value(value: object) -> str:
    """Encode one invocation value without allowing representation-dependent hashes."""

    _validate_json_value(value, "invocation")
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", "invocation value must be JSON serializable"
        ) from exc


@dataclass(frozen=True)
class DataQualityDateWindow:
    start: date
    end: date

    def __post_init__(self) -> None:
        _date_value(self.start, "window.start")
        _date_value(self.end, "window.end")
        if self.start > self.end:
            raise DataQualityExecutionContractError(
                "DQ_WINDOW_INVALID", f"{self.start.isoformat()}>{self.end.isoformat()}"
            )

    def contains(self, other: DataQualityDateWindow) -> bool:
        return self.start <= other.start and other.end <= self.end

    def to_dict(self) -> dict[str, str]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityDateWindow:
        _require_exact_keys(payload, frozenset({"start", "end"}), "window")
        return cls(
            start=_date_value(payload.get("start"), "window.start"),
            end=_date_value(payload.get("end"), "window.end"),
        )


@dataclass(frozen=True)
class DataQualityPolicyBinding:
    policy_id: str
    policy_version: str
    status: str
    owner: str
    role: PolicyRole
    path: str
    sha256: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.policy_id, "policy.policy_id"),
            (self.policy_version, "policy.policy_version"),
            (self.status, "policy.status"),
            (self.owner, "policy.owner"),
        ):
            _require_text(value, field)
        if not isinstance(self.role, PolicyRole) or self.role is not PolicyRole.DATA_QUALITY:
            raise DataQualityExecutionContractError(
                "DQ_POLICY_ID_MISMATCH", f"unexpected policy role={self.role!r}"
            )
        _repo_relative_path(self.path, "policy.path")
        _sha256(self.sha256, "policy.sha256")
        if self.status != "REVIEWED":
            raise DataQualityExecutionContractError(
                "DQ_POLICY_NOT_REVIEWED", f"status={self.status}"
            )

    def to_dict(self) -> dict[str, str]:
        return {
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "status": self.status,
            "owner": self.owner,
            "role": self.role.value,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityPolicyBinding:
        _require_exact_keys(
            payload,
            frozenset({"policy_id", "policy_version", "status", "owner", "role", "path", "sha256"}),
            "policy",
        )
        role_value = _text_value(payload.get("role"), "policy.role")
        try:
            role = PolicyRole(role_value)
        except ValueError as exc:
            raise DataQualityExecutionContractError(
                "DQ_POLICY_ID_MISMATCH", f"unknown policy role={role_value}"
            ) from exc
        return cls(
            policy_id=_text_value(payload.get("policy_id"), "policy.policy_id"),
            policy_version=_text_value(payload.get("policy_version"), "policy.policy_version"),
            status=_text_value(payload.get("status"), "policy.status"),
            owner=_text_value(payload.get("owner"), "policy.owner"),
            role=role,
            path=_repo_relative_path(payload.get("path"), "policy.path"),
            sha256=_text_value(payload.get("sha256"), "policy.sha256"),
        )


@dataclass(frozen=True)
class DataQualityImplementationSourceBinding:
    path: str
    sha256: str

    def __post_init__(self) -> None:
        checked_path = _repo_relative_path(self.path, "validator.implementation_source.path")
        if not checked_path.startswith("src/ai_trading_system/") or not checked_path.endswith(
            ".py"
        ):
            raise DataQualityExecutionContractError(
                "DQ_VALIDATOR_IMPLEMENTATION_MISSING",
                "validator implementation sources must be Python modules under "
                "src/ai_trading_system",
            )
        _sha256(self.sha256, "validator.implementation_source.sha256")

    def to_dict(self) -> dict[str, str]:
        return {"path": self.path, "sha256": self.sha256}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityImplementationSourceBinding:
        _require_exact_keys(payload, frozenset({"path", "sha256"}), "implementation source")
        return cls(
            path=_repo_relative_path(payload.get("path"), "implementation_source.path"),
            sha256=_text_value(payload.get("sha256"), "implementation_source.sha256"),
        )


@dataclass(frozen=True)
class DataQualityValidatorBinding:
    validator_id: str
    validator_version: str
    entrypoint: str
    implementation_sources: tuple[DataQualityImplementationSourceBinding, ...]

    def __post_init__(self) -> None:
        for value, field in (
            (self.validator_id, "validator.validator_id"),
            (self.validator_version, "validator.validator_version"),
            (self.entrypoint, "validator.entrypoint"),
        ):
            _require_text(value, field)
        entrypoint_match = _PYTHON_ENTRYPOINT_PATTERN.fullmatch(self.entrypoint)
        if entrypoint_match is None:
            raise DataQualityExecutionContractError(
                "DQ_VALIDATOR_ENTRYPOINT_MISMATCH", self.entrypoint
            )
        if not self.implementation_sources or any(
            not isinstance(item, DataQualityImplementationSourceBinding)
            for item in self.implementation_sources
        ):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "validator requires implementation sources"
            )
        sources = tuple(sorted(self.implementation_sources, key=lambda item: item.path))
        if len({item.path for item in sources}) != len(sources):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "duplicate validator implementation source"
            )
        entrypoint_source = "src/" + entrypoint_match.group("module").replace(".", "/") + ".py"
        if entrypoint_source not in {item.path for item in sources}:
            raise DataQualityExecutionContractError(
                "DQ_VALIDATOR_IMPLEMENTATION_MISSING",
                f"entrypoint source is not bound: {entrypoint_source}",
            )
        object.__setattr__(self, "implementation_sources", sources)

    def to_dict(self) -> dict[str, object]:
        return {
            "validator_id": self.validator_id,
            "validator_version": self.validator_version,
            "entrypoint": self.entrypoint,
            "implementation_sources": [item.to_dict() for item in self.implementation_sources],
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityValidatorBinding:
        _require_exact_keys(
            payload,
            frozenset(
                {"validator_id", "validator_version", "entrypoint", "implementation_sources"}
            ),
            "validator",
        )
        return cls(
            validator_id=_text_value(payload.get("validator_id"), "validator.validator_id"),
            validator_version=_text_value(
                payload.get("validator_version"), "validator.validator_version"
            ),
            entrypoint=_text_value(payload.get("entrypoint"), "validator.entrypoint"),
            implementation_sources=tuple(
                DataQualityImplementationSourceBinding.from_dict(
                    _mapping_value(item, "implementation source")
                )
                for item in _list_value(
                    payload.get("implementation_sources"), "validator.implementation_sources"
                )
            ),
        )


@dataclass(frozen=True)
class DataQualityInvocationParameter:
    name: str
    value_json: str

    def __post_init__(self) -> None:
        _require_text(self.name, "invocation.name")
        _require_text(self.value_json, "invocation.value_json")
        parsed = _strict_json_loads(self.value_json)
        if canonical_json_value(parsed) != self.value_json:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", f"non-canonical invocation JSON for {self.name}"
            )

    @classmethod
    def from_value(cls, name: str, value: object) -> DataQualityInvocationParameter:
        return cls(name=name, value_json=canonical_json_value(value))

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "value_json": self.value_json}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityInvocationParameter:
        _require_exact_keys(payload, frozenset({"name", "value_json"}), "invocation parameter")
        return cls(
            name=_text_value(payload.get("name"), "invocation.name"),
            value_json=_text_value(payload.get("value_json"), "invocation.value_json"),
        )


@dataclass(frozen=True)
class DataQualityInputBinding:
    role: str
    path: str
    exists: bool
    schema_id: str
    source_role: str
    sha256: str | None
    size_bytes: int | None
    row_count: int | None
    manifest_path: str | None = None
    manifest_sha256: str | None = None
    matched_source_ids: tuple[str, ...] = ()
    matched_record_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, field in (
            (self.role, "input.role"),
            (self.schema_id, "input.schema_id"),
            (self.source_role, "input.source_role"),
        ):
            _require_text(value, field)
        _bool_value(self.exists, "input.exists")
        _repo_relative_path(self.path, "input.path")
        if self.exists:
            if self.sha256 is None or self.size_bytes is None or self.row_count is None:
                raise DataQualityExecutionContractError(
                    "DQ_RECEIPT_FIELDS_INVALID",
                    f"existing input {self.role} requires checksum, size and row count",
                )
            _sha256(self.sha256, f"input[{self.role}].sha256")
            _int_value(self.size_bytes, f"input[{self.role}].size_bytes")
            _int_value(self.row_count, f"input[{self.role}].row_count")
        elif self.sha256 is not None or self.size_bytes is not None or self.row_count is not None:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID",
                f"missing input {self.role} cannot carry checksum, size or row count",
            )
        if (self.manifest_path is None) != (self.manifest_sha256 is None):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID",
                f"input {self.role} manifest path/checksum must appear together",
            )
        if self.manifest_path is not None and self.manifest_sha256 is not None:
            _repo_relative_path(self.manifest_path, f"input[{self.role}].manifest_path")
            _sha256(self.manifest_sha256, f"input[{self.role}].manifest_sha256")
        object.__setattr__(
            self,
            "matched_source_ids",
            _normalize_unique_strings(self.matched_source_ids, "matched_source_ids"),
        )
        object.__setattr__(
            self,
            "matched_record_refs",
            _normalize_unique_strings(self.matched_record_refs, "matched_record_refs"),
        )
        if (self.matched_source_ids or self.matched_record_refs) and self.manifest_path is None:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID",
                f"input {self.role} source/record refs require manifest binding",
            )
        for record_ref in self.matched_record_refs:
            if not _MANIFEST_RECORD_REF_PATTERN.fullmatch(record_ref):
                raise DataQualityExecutionContractError(
                    "DQ_RECEIPT_FIELDS_INVALID",
                    f"input {self.role} has non-content-derived manifest record ref",
                )

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "path": self.path,
            "exists": self.exists,
            "schema_id": self.schema_id,
            "source_role": self.source_role,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "row_count": self.row_count,
            "manifest_path": self.manifest_path,
            "manifest_sha256": self.manifest_sha256,
            "matched_source_ids": list(self.matched_source_ids),
            "matched_record_refs": list(self.matched_record_refs),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityInputBinding:
        _require_exact_keys(
            payload,
            frozenset(
                {
                    "role",
                    "path",
                    "exists",
                    "schema_id",
                    "source_role",
                    "sha256",
                    "size_bytes",
                    "row_count",
                    "manifest_path",
                    "manifest_sha256",
                    "matched_source_ids",
                    "matched_record_refs",
                }
            ),
            "input",
        )
        return cls(
            role=_text_value(payload.get("role"), "input.role"),
            path=_repo_relative_path(payload.get("path"), "input.path"),
            exists=_bool_value(payload.get("exists"), "input.exists"),
            schema_id=_text_value(payload.get("schema_id"), "input.schema_id"),
            source_role=_text_value(payload.get("source_role"), "input.source_role"),
            sha256=_optional_text_value(payload.get("sha256"), "input.sha256"),
            size_bytes=_optional_int_value(payload.get("size_bytes"), "input.size_bytes"),
            row_count=_optional_int_value(payload.get("row_count"), "input.row_count"),
            manifest_path=(
                None
                if payload.get("manifest_path") is None
                else _repo_relative_path(payload.get("manifest_path"), "input.manifest_path")
            ),
            manifest_sha256=_optional_text_value(
                payload.get("manifest_sha256"), "input.manifest_sha256"
            ),
            matched_source_ids=_string_tuple(
                payload.get("matched_source_ids", []), "input.matched_source_ids"
            ),
            matched_record_refs=_string_tuple(
                payload.get("matched_record_refs", []), "input.matched_record_refs"
            ),
        )


@dataclass(frozen=True)
class DataQualityReportBinding:
    path: str
    sha256: str
    size_bytes: int
    status: str
    error_count: int
    warning_count: int
    info_count: int
    issue_codes: tuple[str, ...] = ()
    blocking_issue_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _repo_relative_path(self.path, "report.path")
        _sha256(self.sha256, "report.sha256")
        for value, field in (
            (self.size_bytes, "report.size_bytes"),
            (self.error_count, "report.error_count"),
            (self.warning_count, "report.warning_count"),
            (self.info_count, "report.info_count"),
        ):
            _int_value(value, field)
        if self.status not in _REPORT_STATUSES:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_STATUS_CONFLICT", f"unsupported status={self.status}"
            )
        if self.status == "PASS" and (self.error_count or self.warning_count):
            raise DataQualityExecutionContractError(
                "DQ_REPORT_STATUS_CONFLICT", "PASS cannot contain errors or warnings"
            )
        if self.status == "PASS_WITH_WARNINGS" and (self.error_count or not self.warning_count):
            raise DataQualityExecutionContractError(
                "DQ_REPORT_STATUS_CONFLICT",
                "PASS_WITH_WARNINGS requires warnings and zero errors",
            )
        if self.status == "PASS_WITH_WARNINGS" and not self.issue_codes:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_COUNT_MISMATCH", "PASS_WITH_WARNINGS requires issue codes"
            )
        if self.status == "FAIL" and not self.error_count:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_STATUS_CONFLICT", "FAIL requires at least one error"
            )
        object.__setattr__(
            self, "issue_codes", _normalize_unique_strings(self.issue_codes, "report.issue_codes")
        )
        object.__setattr__(
            self,
            "blocking_issue_codes",
            _normalize_unique_strings(self.blocking_issue_codes, "report.blocking_issue_codes"),
        )
        if not set(self.blocking_issue_codes).issubset(self.issue_codes):
            raise DataQualityExecutionContractError(
                "DQ_REPORT_COUNT_MISMATCH", "blocking issues must be present in issue codes"
            )
        if self.status == "FAIL" and not self.blocking_issue_codes:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_COUNT_MISMATCH", "FAIL requires at least one blocking issue"
            )
        if self.status != "FAIL" and self.blocking_issue_codes:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_STATUS_CONFLICT", "passing report cannot contain blocking issues"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "status": self.status,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "issue_codes": list(self.issue_codes),
            "blocking_issue_codes": list(self.blocking_issue_codes),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityReportBinding:
        _require_exact_keys(
            payload,
            frozenset(
                {
                    "path",
                    "sha256",
                    "size_bytes",
                    "status",
                    "error_count",
                    "warning_count",
                    "info_count",
                    "issue_codes",
                    "blocking_issue_codes",
                }
            ),
            "report",
        )
        return cls(
            path=_repo_relative_path(payload.get("path"), "report.path"),
            sha256=_text_value(payload.get("sha256"), "report.sha256"),
            size_bytes=_int_value(payload.get("size_bytes"), "report.size_bytes"),
            status=_text_value(payload.get("status"), "report.status"),
            error_count=_int_value(payload.get("error_count"), "report.error_count"),
            warning_count=_int_value(payload.get("warning_count"), "report.warning_count"),
            info_count=_int_value(payload.get("info_count"), "report.info_count"),
            issue_codes=_string_tuple(payload.get("issue_codes"), "report.issue_codes"),
            blocking_issue_codes=_string_tuple(
                payload.get("blocking_issue_codes"), "report.blocking_issue_codes"
            ),
        )


def _data_quality_evidence_from_payload(
    payload: Mapping[str, object],
) -> DataQualityEvidence:
    _require_exact_keys(
        payload,
        frozenset(
            {
                "evidence_id",
                "schema_version",
                "contract_id",
                "policy_id",
                "policy_version",
                "status",
                "passed",
                "checked_at",
                "as_of",
                "report_path",
                "report_sha256",
                "error_count",
                "warning_count",
                "checked_input_count",
                "blocking_issues",
            }
        ),
        "data_quality_evidence",
    )
    if _text_value(payload.get("schema_version"), "evidence.schema_version") != (
        DataQualityEvidence.schema_version
    ):
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", "unsupported embedded evidence schema"
        )
    try:
        evidence = DataQualityEvidence(
            contract_id=_text_value(payload.get("contract_id"), "evidence.contract_id"),
            policy_id=_text_value(payload.get("policy_id"), "evidence.policy_id"),
            policy_version=_text_value(payload.get("policy_version"), "evidence.policy_version"),
            status=_text_value(payload.get("status"), "evidence.status"),
            passed=_bool_value(payload.get("passed"), "evidence.passed"),
            checked_at=_datetime_value(payload.get("checked_at"), "evidence.checked_at"),
            as_of=_date_value(payload.get("as_of"), "evidence.as_of"),
            report_path=_repo_relative_path(payload.get("report_path"), "evidence.report_path"),
            report_sha256=_text_value(payload.get("report_sha256"), "evidence.report_sha256"),
            error_count=_int_value(payload.get("error_count"), "evidence.error_count"),
            warning_count=_int_value(payload.get("warning_count"), "evidence.warning_count"),
            checked_input_count=_int_value(
                payload.get("checked_input_count"), "evidence.checked_input_count"
            ),
            blocking_issues=_string_tuple(
                payload.get("blocking_issues"), "evidence.blocking_issues"
            ),
        )
    except DataQualityEvidenceError as exc:
        raise DataQualityExecutionContractError(
            "DQ_RECEIPT_FIELDS_INVALID", f"invalid embedded evidence: {exc.code}"
        ) from exc
    supplied_id = _text_value(payload.get("evidence_id"), "evidence.evidence_id")
    if supplied_id != evidence.evidence_id:
        raise DataQualityExecutionContractError(
            "DQ_EVIDENCE_ID_MISMATCH",
            f"supplied={supplied_id} actual={evidence.evidence_id}",
        )
    return evidence


@dataclass(frozen=True)
class DataQualityExecutionReceipt:
    schema_version: ClassVar[str] = "data_quality_execution_receipt.v1"

    run_id: str
    contract_id: str
    started_at: datetime
    ended_at: datetime
    checked_at: datetime
    as_of: date
    requested_window: DataQualityDateWindow
    evaluated_window: DataQualityDateWindow
    policy: DataQualityPolicyBinding
    validator: DataQualityValidatorBinding
    invocation: tuple[DataQualityInvocationParameter, ...]
    inputs: tuple[DataQualityInputBinding, ...]
    report: DataQualityReportBinding
    data_quality_evidence: DataQualityEvidence
    dq_execution_provenance_verified: bool = True
    consumer_cutover_allowed: bool = False
    production_effect: str = _PRODUCTION_EFFECT_NONE

    def __post_init__(self) -> None:
        _require_text(self.run_id, "run_id")
        _require_text(self.contract_id, "contract_id")
        _datetime_value(self.started_at, "started_at")
        _datetime_value(self.ended_at, "ended_at")
        _datetime_value(self.checked_at, "checked_at")
        _date_value(self.as_of, "as_of")
        if not isinstance(self.requested_window, DataQualityDateWindow) or not isinstance(
            self.evaluated_window, DataQualityDateWindow
        ):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "receipt windows must use typed bindings"
            )
        if not isinstance(self.policy, DataQualityPolicyBinding) or not isinstance(
            self.validator, DataQualityValidatorBinding
        ):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "receipt policy/validator must use typed bindings"
            )
        if not isinstance(self.report, DataQualityReportBinding) or not isinstance(
            self.data_quality_evidence, DataQualityEvidence
        ):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "receipt report/evidence must use typed bindings"
            )
        if not self.started_at <= self.checked_at <= self.ended_at:
            raise DataQualityExecutionContractError(
                "DQ_EXECUTION_CHRONOLOGY_INVALID",
                "expected started_at <= checked_at <= ended_at",
            )
        if self.requested_window.end > self.as_of or self.evaluated_window.end > self.as_of:
            raise DataQualityExecutionContractError(
                "DQ_WINDOW_MISMATCH", "window cannot extend beyond as_of"
            )
        if not self.requested_window.contains(self.evaluated_window):
            raise DataQualityExecutionContractError(
                "DQ_WINDOW_MISMATCH", "evaluated window must be within requested window"
            )
        if any(not isinstance(item, DataQualityInvocationParameter) for item in self.invocation):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "invalid invocation binding type"
            )
        invocation = tuple(sorted(self.invocation, key=lambda item: item.name))
        if not invocation:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "receipt requires exact invocation parameters"
            )
        if len({item.name for item in invocation}) != len(invocation):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "duplicate invocation parameter"
            )
        object.__setattr__(self, "invocation", invocation)
        if any(not isinstance(item, DataQualityInputBinding) for item in self.inputs):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "invalid input binding type"
            )
        inputs = tuple(sorted(self.inputs, key=lambda item: (item.role, item.path)))
        if not inputs:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "receipt requires at least one input binding"
            )
        if len({(item.role, item.path) for item in inputs}) != len(inputs):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "duplicate input role/path binding"
            )
        object.__setattr__(self, "inputs", inputs)
        _bool_value(self.dq_execution_provenance_verified, "dq_execution_provenance_verified")
        _bool_value(self.consumer_cutover_allowed, "consumer_cutover_allowed")
        _text_value(self.production_effect, "production_effect")
        self._validate_evidence_projection()
        if not self.dq_execution_provenance_verified:
            raise DataQualityExecutionContractError("DQ_PROVENANCE_NOT_VERIFIED", self.run_id)
        if self.consumer_cutover_allowed:
            raise DataQualityExecutionContractError(
                "DQ_CONSUMER_NOT_AUTHORIZED", "D0B1 receipt cannot authorize cutover"
            )
        if self.production_effect != _PRODUCTION_EFFECT_NONE:
            raise DataQualityExecutionContractError(
                "PRODUCTION_EFFECT_INVALID", self.production_effect
            )

    def _validate_evidence_projection(self) -> None:
        evidence = self.data_quality_evidence
        _datetime_value(evidence.checked_at, "data_quality_evidence.checked_at")
        if evidence.contract_id != self.contract_id:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_STATUS_CONFLICT", "evidence contract id differs from receipt"
            )
        if evidence.policy_id != self.policy.policy_id:
            raise DataQualityExecutionContractError("DQ_POLICY_ID_MISMATCH", evidence.policy_id)
        if evidence.policy_version != self.policy.policy_version:
            raise DataQualityExecutionContractError(
                "DQ_POLICY_VERSION_MISMATCH", evidence.policy_version
            )
        if evidence.as_of != self.as_of or evidence.checked_at != self.checked_at:
            raise DataQualityExecutionContractError(
                "DQ_AS_OF_MISMATCH", "evidence chronology differs from receipt"
            )
        if evidence.status != self.report.status:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_STATUS_CONFLICT", "evidence status differs from report"
            )
        if (
            evidence.error_count != self.report.error_count
            or evidence.warning_count != self.report.warning_count
        ):
            raise DataQualityExecutionContractError(
                "DQ_REPORT_COUNT_MISMATCH", "evidence counts differ from report"
            )
        expected_passed = self.report.status in {"PASS", "PASS_WITH_WARNINGS"}
        if evidence.passed is not expected_passed:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_STATUS_CONFLICT", "evidence passed flag differs from report"
            )
        if evidence.report_path != self.report.path or evidence.report_sha256 != self.report.sha256:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_SHA_MISMATCH", "evidence report ref differs from receipt report"
            )
        if evidence.checked_input_count != len(self.inputs):
            raise DataQualityExecutionContractError(
                "DQ_REPORT_COUNT_MISMATCH", "attempted input count differs from bound inputs"
            )
        if evidence.blocking_issues != self.report.blocking_issue_codes:
            raise DataQualityExecutionContractError(
                "DQ_REPORT_COUNT_MISMATCH", "evidence blocking issues differ from report"
            )

    @property
    def receipt_id(self) -> str:
        material = json.dumps(
            self._semantic_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return f"dq_execution_{hashlib.sha256(material).hexdigest()}"

    @property
    def canonical_bytes(self) -> bytes:
        return (
            json.dumps(
                self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False
            )
            + "\n"
        ).encode("utf-8")

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def passed(self) -> bool:
        return self.report.status in {"PASS", "PASS_WITH_WARNINGS"}

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "contract_id": self.contract_id,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "checked_at": self.checked_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "requested_window": self.requested_window.to_dict(),
            "evaluated_window": self.evaluated_window.to_dict(),
            "policy": self.policy.to_dict(),
            "validator": self.validator.to_dict(),
            "invocation": [item.to_dict() for item in self.invocation],
            "inputs": [item.to_dict() for item in self.inputs],
            "report": self.report.to_dict(),
            "data_quality_evidence": self.data_quality_evidence.to_dict(),
            "dq_execution_provenance_verified": self.dq_execution_provenance_verified,
            "consumer_cutover_allowed": self.consumer_cutover_allowed,
            "production_effect": self.production_effect,
        }

    def to_dict(self) -> dict[str, object]:
        return {"receipt_id": self.receipt_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityExecutionReceipt:
        _require_exact_keys(
            payload,
            frozenset(
                {
                    "receipt_id",
                    "schema_version",
                    "run_id",
                    "contract_id",
                    "started_at",
                    "ended_at",
                    "checked_at",
                    "as_of",
                    "requested_window",
                    "evaluated_window",
                    "policy",
                    "validator",
                    "invocation",
                    "inputs",
                    "report",
                    "data_quality_evidence",
                    "dq_execution_provenance_verified",
                    "consumer_cutover_allowed",
                    "production_effect",
                }
            ),
            "receipt",
        )
        schema_version = _text_value(payload.get("schema_version"), "schema_version")
        if schema_version != cls.schema_version:
            raise DataQualityExecutionContractError("DQ_RECEIPT_SCHEMA_UNSUPPORTED", schema_version)
        receipt = cls(
            run_id=_text_value(payload.get("run_id"), "run_id"),
            contract_id=_text_value(payload.get("contract_id"), "contract_id"),
            started_at=_datetime_value(payload.get("started_at"), "started_at"),
            ended_at=_datetime_value(payload.get("ended_at"), "ended_at"),
            checked_at=_datetime_value(payload.get("checked_at"), "checked_at"),
            as_of=_date_value(payload.get("as_of"), "as_of"),
            requested_window=DataQualityDateWindow.from_dict(
                _mapping_value(payload.get("requested_window"), "requested_window")
            ),
            evaluated_window=DataQualityDateWindow.from_dict(
                _mapping_value(payload.get("evaluated_window"), "evaluated_window")
            ),
            policy=DataQualityPolicyBinding.from_dict(
                _mapping_value(payload.get("policy"), "policy")
            ),
            validator=DataQualityValidatorBinding.from_dict(
                _mapping_value(payload.get("validator"), "validator")
            ),
            invocation=tuple(
                DataQualityInvocationParameter.from_dict(_mapping_value(item, "invocation item"))
                for item in _list_value(payload.get("invocation"), "invocation")
            ),
            inputs=tuple(
                DataQualityInputBinding.from_dict(_mapping_value(item, "input item"))
                for item in _list_value(payload.get("inputs"), "inputs")
            ),
            report=DataQualityReportBinding.from_dict(
                _mapping_value(payload.get("report"), "report")
            ),
            data_quality_evidence=_data_quality_evidence_from_payload(
                _mapping_value(payload.get("data_quality_evidence"), "data_quality_evidence")
            ),
            dq_execution_provenance_verified=_bool_value(
                payload.get("dq_execution_provenance_verified"),
                "dq_execution_provenance_verified",
            ),
            consumer_cutover_allowed=_bool_value(
                payload.get("consumer_cutover_allowed"), "consumer_cutover_allowed"
            ),
            production_effect=_text_value(payload.get("production_effect"), "production_effect"),
        )
        supplied_id = _text_value(payload.get("receipt_id"), "receipt_id")
        if supplied_id != receipt.receipt_id:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_ID_MISMATCH",
                f"supplied={supplied_id} actual={receipt.receipt_id}",
            )
        return receipt

    @classmethod
    def from_json_bytes(cls, content: bytes) -> DataQualityExecutionReceipt:
        try:
            decoded = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "receipt is not valid UTF-8 JSON"
            ) from exc
        payload = _strict_json_loads(decoded)
        receipt = cls.from_dict(_mapping_value(payload, "receipt"))
        if content != receipt.canonical_bytes:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_FIELDS_INVALID", "receipt bytes are not canonical"
            )
        return receipt


@dataclass(frozen=True, init=False)
class VerifiedDataQualityPreflight:
    schema_version: ClassVar[str] = "verified_data_quality_preflight.v1"

    receipt: DataQualityExecutionReceipt
    receipt_path: str
    receipt_sha256: str
    receipt_size_bytes: int
    verified_at: datetime

    def __init__(
        self,
        *,
        receipt: DataQualityExecutionReceipt,
        receipt_path: str,
        receipt_sha256: str,
        receipt_size_bytes: int,
        verified_at: datetime,
        _verification_seal: object,
    ) -> None:
        if _verification_seal is not _VERIFIED_PREFLIGHT_SEAL:
            raise DataQualityExecutionContractError(
                "DQ_PROVENANCE_NOT_VERIFIED", "preflight must be created by canonical verifier"
            )
        checked_path = _repo_relative_path(receipt_path, "preflight.receipt_path")
        expected_path = f"outputs/data_quality/executions/{receipt.receipt_id}/receipt.json"
        if checked_path != expected_path:
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_ID_MISMATCH",
                f"receipt path must be content-addressed: expected={expected_path}",
            )
        checked_sha = _sha256(receipt_sha256, "preflight.receipt_sha256")
        checked_size = _int_value(receipt_size_bytes, "preflight.receipt_size_bytes")
        checked_at = _datetime_value(verified_at, "preflight.verified_at")
        if checked_at < receipt.ended_at:
            raise DataQualityExecutionContractError(
                "DQ_EXECUTION_CHRONOLOGY_INVALID",
                "preflight verification cannot precede receipt completion",
            )
        if checked_sha != receipt.canonical_sha256 or checked_size != len(receipt.canonical_bytes):
            raise DataQualityExecutionContractError(
                "DQ_RECEIPT_ID_MISMATCH", "preflight receipt bytes checksum mismatch"
            )
        object.__setattr__(self, "receipt", receipt)
        object.__setattr__(self, "receipt_path", checked_path)
        object.__setattr__(self, "receipt_sha256", checked_sha)
        object.__setattr__(self, "receipt_size_bytes", checked_size)
        object.__setattr__(self, "verified_at", checked_at)

    @property
    def receipt_id(self) -> str:
        return self.receipt.receipt_id

    @property
    def status(self) -> str:
        return self.receipt.report.status

    @property
    def as_of(self) -> date:
        return self.receipt.as_of

    @property
    def data_quality_evidence(self) -> DataQualityEvidence:
        return self.receipt.data_quality_evidence

    def assert_strict_passed(self) -> Self:
        if self.status == "FAIL":
            raise DataQualityExecutionContractError("DQ_EXECUTION_FAILED", self.receipt.receipt_id)
        if self.status == "PASS_WITH_WARNINGS":
            raise DataQualityExecutionContractError(
                "DQ_WARNING_NOT_ALLOWED", self.receipt.receipt_id
            )
        return self


def _build_verified_data_quality_preflight(
    *,
    receipt: DataQualityExecutionReceipt,
    receipt_path: str,
    receipt_sha256: str,
    receipt_size_bytes: int,
    verified_at: datetime,
) -> VerifiedDataQualityPreflight:
    """Verifier-only capability factory; callers must never treat it as byte verification."""

    return VerifiedDataQualityPreflight(
        receipt=receipt,
        receipt_path=receipt_path,
        receipt_sha256=receipt_sha256,
        receipt_size_bytes=receipt_size_bytes,
        verified_at=verified_at,
        _verification_seal=_VERIFIED_PREFLIGHT_SEAL,
    )
