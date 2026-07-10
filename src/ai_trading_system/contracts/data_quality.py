from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar, Self

from ai_trading_system.contracts.research_context import DataQualityContractRef

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_PASS_STATUSES = frozenset({"PASS", "PASS_WITH_WARNINGS"})


class DataQualityEvidenceError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise DataQualityEvidenceError("REQUIRED_DQ_FIELD_EMPTY", f"{field} is required")


def _date_value(value: object, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise DataQualityEvidenceError("INVALID_DQ_DATE", f"{field}={value!r}") from exc
    raise DataQualityEvidenceError("INVALID_DQ_DATE", f"{field}={value!r}")


def _datetime_value(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise DataQualityEvidenceError("INVALID_DQ_DATETIME", f"{field}={value!r}") from exc
    else:
        raise DataQualityEvidenceError("INVALID_DQ_DATETIME", f"{field}={value!r}")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DataQualityEvidenceError("DQ_DATETIME_TZ_REQUIRED", field)
    return parsed


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise DataQualityEvidenceError("INVALID_DQ_PAYLOAD", f"{field} must be a list")
    return tuple(str(item) for item in value)


def _int_value(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise DataQualityEvidenceError("INVALID_DQ_COUNT", f"{field} must be an integer")
    return value


@dataclass(frozen=True)
class DataQualityEvidence:
    schema_version: ClassVar[str] = "data_quality_evidence.v1"

    contract_id: str
    policy_id: str
    policy_version: str
    status: str
    passed: bool
    checked_at: datetime
    as_of: date
    report_path: str | None
    report_sha256: str | None
    error_count: int = 0
    warning_count: int = 0
    checked_input_count: int = 0
    blocking_issues: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for text_value, text_field in (
            (self.contract_id, "contract_id"),
            (self.policy_id, "policy_id"),
            (self.policy_version, "policy_version"),
            (self.status, "status"),
        ):
            _require_text(text_value, text_field)
        _datetime_value(self.checked_at, "checked_at")
        if self.checked_at.date() < self.as_of:
            raise DataQualityEvidenceError(
                "DQ_CHECKED_BEFORE_AS_OF", "checked_at date must not precede as_of"
            )
        for count_value, count_field in (
            (self.error_count, "error_count"),
            (self.warning_count, "warning_count"),
            (self.checked_input_count, "checked_input_count"),
        ):
            if count_value < 0:
                raise DataQualityEvidenceError("INVALID_DQ_COUNT", f"{count_field} must be >= 0")
        if self.report_sha256 is not None and not _SHA256_PATTERN.fullmatch(self.report_sha256):
            raise DataQualityEvidenceError("INVALID_DQ_REPORT_CHECKSUM", self.contract_id)
        if (self.report_path is None) != (self.report_sha256 is None):
            raise DataQualityEvidenceError(
                "INCOMPLETE_DQ_REPORT_REF", "report path and checksum must appear together"
            )
        issues = tuple(sorted(set(self.blocking_issues)))
        object.__setattr__(self, "blocking_issues", issues)
        if self.passed:
            if self.status not in _PASS_STATUSES:
                raise DataQualityEvidenceError(
                    "DQ_PASS_STATUS_CONFLICT", f"passed=true status={self.status}"
                )
            if self.error_count or issues:
                raise DataQualityEvidenceError(
                    "DQ_PASS_HAS_BLOCKERS", "passed evidence cannot contain errors/blockers"
                )
        elif self.status in _PASS_STATUSES:
            raise DataQualityEvidenceError(
                "DQ_FAILED_STATUS_CONFLICT", f"passed=false status={self.status}"
            )

    @property
    def evidence_id(self) -> str:
        material = json.dumps(
            self._semantic_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return f"dq_evidence_{hashlib.sha256(material).hexdigest()[:20]}"

    @property
    def ready(self) -> bool:
        return self.passed and self.report_path is not None and self.report_sha256 is not None

    def assert_ready(self) -> Self:
        if not self.passed:
            raise DataQualityEvidenceError(
                "DATA_QUALITY_NOT_PASSED", ",".join(self.blocking_issues) or self.status
            )
        if self.report_path is None or self.report_sha256 is None:
            raise DataQualityEvidenceError("DATA_QUALITY_REPORT_REF_REQUIRED", self.contract_id)
        return self

    def to_contract_ref(self) -> DataQualityContractRef:
        return DataQualityContractRef(
            contract_id=self.contract_id,
            status=self.status,
            passed=self.passed,
            as_of=self.as_of,
            policy_ref_id=self.policy_id,
            report_path=self.report_path,
            report_sha256=self.report_sha256,
        )

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "contract_id": self.contract_id,
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "status": self.status,
            "passed": self.passed,
            "checked_at": self.checked_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "report_path": self.report_path,
            "report_sha256": self.report_sha256,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "checked_input_count": self.checked_input_count,
            "blocking_issues": list(self.blocking_issues),
        }

    def to_dict(self) -> dict[str, object]:
        return {"evidence_id": self.evidence_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityEvidence:
        evidence = cls(
            contract_id=str(payload.get("contract_id", "")),
            policy_id=str(payload.get("policy_id", "")),
            policy_version=str(payload.get("policy_version", "")),
            status=str(payload.get("status", "")),
            passed=payload.get("passed") is True,
            checked_at=_datetime_value(payload.get("checked_at"), "checked_at"),
            as_of=_date_value(payload.get("as_of"), "as_of"),
            report_path=(
                None if payload.get("report_path") is None else str(payload.get("report_path"))
            ),
            report_sha256=(
                None if payload.get("report_sha256") is None else str(payload.get("report_sha256"))
            ),
            error_count=_int_value(payload.get("error_count", 0), "error_count"),
            warning_count=_int_value(payload.get("warning_count", 0), "warning_count"),
            checked_input_count=_int_value(
                payload.get("checked_input_count", 0), "checked_input_count"
            ),
            blocking_issues=_string_tuple(payload.get("blocking_issues"), "blocking_issues"),
        )
        supplied_id = payload.get("evidence_id")
        if supplied_id is not None and str(supplied_id) != evidence.evidence_id:
            raise DataQualityEvidenceError(
                "DQ_EVIDENCE_ID_MISMATCH", f"supplied={supplied_id} actual={evidence.evidence_id}"
            )
        return evidence
