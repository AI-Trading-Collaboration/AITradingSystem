from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import ClassVar, Self, TypeVar

from ai_trading_system.contracts.status import (
    ContextResolutionStatus,
    EvidenceRole,
    PolicyRole,
    ResearchWindowRole,
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_REQUIRED_POLICY_ROLES = {
    PolicyRole.MARKET_REGIME,
    PolicyRole.RESEARCH_WINDOW,
    PolicyRole.DATA_QUALITY,
}
_EXPECTED_EVIDENCE_ROLE = {
    ResearchWindowRole.PRIMARY_VALIDATED: EvidenceRole.PRIMARY_DECISION_EVIDENCE,
    ResearchWindowRole.LEGACY_COMPARISON: EvidenceRole.LEGACY_COMPARISON_EVIDENCE,
    ResearchWindowRole.SENSITIVITY: EvidenceRole.SENSITIVITY_EVIDENCE_WITH_CAVEAT,
    ResearchWindowRole.PROXY_ROBUSTNESS: EvidenceRole.DIAGNOSTIC_ONLY,
    ResearchWindowRole.METADATA_ONLY: EvidenceRole.METADATA_ONLY,
}
_CAVEAT_REQUIRED_ROLES = {
    ResearchWindowRole.SENSITIVITY,
    ResearchWindowRole.PROXY_ROBUSTNESS,
    ResearchWindowRole.METADATA_ONLY,
}
_EnumT = TypeVar("_EnumT", bound=StrEnum)


class ResearchContextError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise ResearchContextError("REQUIRED_CONTEXT_FIELD_EMPTY", f"{field} is required")


def _date_value(value: object, field: str) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ResearchContextError("INVALID_CONTEXT_DATE", f"{field}={value!r}") from exc
    raise ResearchContextError("INVALID_CONTEXT_DATE", f"{field}={value!r}")


def _optional_date(value: object, field: str) -> date | None:
    return None if value is None else _date_value(value, field)


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ResearchContextError("INVALID_CONTEXT_PAYLOAD", f"{field} must be a list")
    return tuple(str(item) for item in value)


def _enum_value(enum_type: type[_EnumT], value: object, code: str) -> _EnumT:
    try:
        return enum_type(str(value))
    except ValueError as exc:
        raise ResearchContextError(code, f"unsupported value: {value!r}") from exc


def _bool_value(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ResearchContextError("INVALID_CONTEXT_BOOLEAN", f"{field} must be boolean")
    return value


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start > self.end:
            raise ResearchContextError(
                "INVALID_DATE_RANGE",
                f"start {self.start.isoformat()} is after end {self.end.isoformat()}",
            )

    def contains(self, value: date) -> bool:
        return self.start <= value <= self.end

    def contains_range(self, other: DateRange) -> bool:
        return self.start <= other.start and other.end <= self.end

    def to_dict(self) -> dict[str, str]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DateRange:
        return cls(
            start=_date_value(payload.get("start"), "range.start"),
            end=_date_value(payload.get("end"), "range.end"),
        )


@dataclass(frozen=True)
class CoverageInterval:
    source_id: str
    date_range: DateRange
    coverage_type: str = "point_in_time"
    caveats: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.source_id, "coverage.source_id")
        _require_text(self.coverage_type, "coverage.coverage_type")
        object.__setattr__(self, "caveats", tuple(sorted(set(self.caveats))))

    def to_dict(self) -> dict[str, object]:
        return {
            "source_id": self.source_id,
            "date_range": self.date_range.to_dict(),
            "coverage_type": self.coverage_type,
            "caveats": list(self.caveats),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> CoverageInterval:
        range_payload = payload.get("date_range")
        if not isinstance(range_payload, Mapping):
            raise ResearchContextError(
                "INVALID_COVERAGE_INTERVAL", "coverage.date_range must be a mapping"
            )
        return cls(
            source_id=str(payload.get("source_id", "")),
            date_range=DateRange.from_dict(range_payload),
            coverage_type=str(payload.get("coverage_type", "")),
            caveats=_string_tuple(payload.get("caveats"), "coverage.caveats"),
        )


@dataclass(frozen=True)
class EffectiveCoverage:
    intervals: tuple[CoverageInterval, ...]

    def __post_init__(self) -> None:
        normalized = tuple(sorted(self.intervals, key=lambda item: item.source_id))
        source_ids = [item.source_id for item in normalized]
        if len(source_ids) != len(set(source_ids)):
            raise ResearchContextError(
                "DUPLICATE_COVERAGE_SOURCE", "effective coverage source ids must be unique"
            )
        object.__setattr__(self, "intervals", normalized)

    def to_dict(self) -> dict[str, object]:
        return {"intervals": [item.to_dict() for item in self.intervals]}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> EffectiveCoverage:
        raw = payload.get("intervals")
        if not isinstance(raw, list):
            raise ResearchContextError(
                "INVALID_EFFECTIVE_COVERAGE", "effective_coverage.intervals must be a list"
            )
        intervals = []
        for item in raw:
            if not isinstance(item, Mapping):
                raise ResearchContextError(
                    "INVALID_EFFECTIVE_COVERAGE", "coverage interval must be a mapping"
                )
            intervals.append(CoverageInterval.from_dict(item))
        return cls(tuple(intervals))


@dataclass(frozen=True)
class PolicyRef:
    policy_id: str
    role: PolicyRole
    version: str
    status: str
    path: str
    sha256: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.policy_id, "policy_id"),
            (self.version, "policy.version"),
            (self.status, "policy.status"),
            (self.path, "policy.path"),
        ):
            _require_text(value, field)
        if not _SHA256_PATTERN.fullmatch(self.sha256):
            raise ResearchContextError(
                "INVALID_POLICY_CHECKSUM", f"{self.policy_id} sha256 must be lowercase hex"
            )

    def to_dict(self) -> dict[str, str]:
        return {
            "policy_id": self.policy_id,
            "role": self.role.value,
            "version": self.version,
            "status": self.status,
            "path": self.path,
            "sha256": self.sha256,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> PolicyRef:
        return cls(
            policy_id=str(payload.get("policy_id", "")),
            role=_enum_value(PolicyRole, payload.get("role"), "UNKNOWN_POLICY_ROLE"),
            version=str(payload.get("version", "")),
            status=str(payload.get("status", "")),
            path=str(payload.get("path", "")),
            sha256=str(payload.get("sha256", "")),
        )


@dataclass(frozen=True)
class DataQualityContractRef:
    contract_id: str
    status: str
    passed: bool
    as_of: date
    policy_ref_id: str
    report_path: str | None = None
    report_sha256: str | None = None

    def __post_init__(self) -> None:
        _require_text(self.contract_id, "data_quality.contract_id")
        _require_text(self.status, "data_quality.status")
        _require_text(self.policy_ref_id, "data_quality.policy_ref_id")
        if self.report_sha256 is not None and not _SHA256_PATTERN.fullmatch(self.report_sha256):
            raise ResearchContextError(
                "INVALID_DATA_QUALITY_CHECKSUM", "data-quality report sha256 is invalid"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_id": self.contract_id,
            "status": self.status,
            "passed": self.passed,
            "as_of": self.as_of.isoformat(),
            "policy_ref_id": self.policy_ref_id,
            "report_path": self.report_path,
            "report_sha256": self.report_sha256,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DataQualityContractRef:
        return cls(
            contract_id=str(payload.get("contract_id", "")),
            status=str(payload.get("status", "")),
            passed=_bool_value(payload.get("passed"), "data_quality.passed"),
            as_of=_date_value(payload.get("as_of"), "data_quality.as_of"),
            policy_ref_id=str(payload.get("policy_ref_id", "")),
            report_path=(
                None if payload.get("report_path") is None else str(payload.get("report_path"))
            ),
            report_sha256=(
                None if payload.get("report_sha256") is None else str(payload.get("report_sha256"))
            ),
        )


@dataclass(frozen=True)
class MarketRegimeSpec:
    regime_id: str
    anchor_date: date
    start_date: date

    def __post_init__(self) -> None:
        _require_text(self.regime_id, "market_regime_id")
        if self.anchor_date > self.start_date:
            raise ResearchContextError(
                "REGIME_ANCHOR_AFTER_START", "regime anchor must not follow regime start"
            )


@dataclass(frozen=True)
class ResearchWindowSpec:
    window_id: str
    start_date: date
    role: ResearchWindowRole
    evidence_role: EvidenceRole
    caveats: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.window_id, "research_window_id")
        expected = _EXPECTED_EVIDENCE_ROLE[self.role]
        if self.evidence_role is not expected:
            raise ResearchContextError(
                "WINDOW_EVIDENCE_ROLE_MISMATCH",
                f"{self.role.value} requires {expected.value}",
            )
        object.__setattr__(self, "caveats", tuple(sorted(set(self.caveats))))
        if self.role in _CAVEAT_REQUIRED_ROLES and not self.caveats:
            raise ResearchContextError(
                "WINDOW_CAVEAT_REQUIRED", f"{self.role.value} requires at least one caveat"
            )


@dataclass(frozen=True)
class ResearchEvaluationContext:
    schema_version: ClassVar[str] = "research_evaluation_context.v1"

    status: ContextResolutionStatus
    market_regime_id: str
    regime_anchor: date
    regime_start: date
    research_window_id: str
    research_window_start: date
    window_role: ResearchWindowRole
    evidence_role: EvidenceRole
    requested_range: DateRange
    actual_data_range: DateRange | None
    effective_feature_start: date | None
    effective_prediction_start: date | None
    actual_portfolio_start: date | None
    evaluation_range: DateRange | None
    as_of: date
    trading_calendar: str
    effective_coverage: EffectiveCoverage | None
    data_quality: DataQualityContractRef
    caveats: tuple[str, ...]
    policy_refs: tuple[PolicyRef, ...]
    blocking_issues: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, field in (
            (self.market_regime_id, "market_regime_id"),
            (self.research_window_id, "research_window_id"),
            (self.trading_calendar, "trading_calendar"),
        ):
            _require_text(value, field)
        if self.regime_anchor > self.regime_start:
            raise ResearchContextError(
                "REGIME_ANCHOR_AFTER_START", "regime anchor must not follow regime start"
            )
        expected_role = _EXPECTED_EVIDENCE_ROLE[self.window_role]
        if self.evidence_role is not expected_role:
            raise ResearchContextError(
                "WINDOW_EVIDENCE_ROLE_MISMATCH",
                f"{self.window_role.value} requires {expected_role.value}",
            )

        caveats = tuple(sorted(set(self.caveats)))
        issues = tuple(sorted(set(self.blocking_issues)))
        policies = tuple(
            sorted(self.policy_refs, key=lambda item: (item.role.value, item.policy_id))
        )
        object.__setattr__(self, "caveats", caveats)
        object.__setattr__(self, "blocking_issues", issues)
        object.__setattr__(self, "policy_refs", policies)
        if self.window_role in _CAVEAT_REQUIRED_ROLES and not caveats:
            raise ResearchContextError(
                "WINDOW_CAVEAT_REQUIRED", f"{self.window_role.value} requires a caveat"
            )

        policy_ids = [item.policy_id for item in policies]
        if len(policy_ids) != len(set(policy_ids)):
            raise ResearchContextError(
                "DUPLICATE_POLICY_REF", "policy_ref ids must be unique within a context"
            )
        policy_roles = {item.role for item in policies}
        missing_roles = sorted(role.value for role in _REQUIRED_POLICY_ROLES - policy_roles)
        if missing_roles:
            raise ResearchContextError("REQUIRED_POLICY_REF_MISSING", ",".join(missing_roles))
        data_quality_policies = {
            item.policy_id for item in policies if item.role is PolicyRole.DATA_QUALITY
        }
        if self.data_quality.policy_ref_id not in data_quality_policies:
            raise ResearchContextError(
                "DATA_QUALITY_POLICY_REF_MISMATCH", self.data_quality.policy_ref_id
            )
        if self.data_quality.as_of != self.as_of:
            raise ResearchContextError(
                "DATA_QUALITY_AS_OF_MISMATCH",
                f"data-quality={self.data_quality.as_of} context={self.as_of}",
            )

        self._validate_known_ranges()
        if self.status is ContextResolutionStatus.COMPLETE:
            self._validate_complete()
        elif not issues:
            raise ResearchContextError(
                "BLOCKED_CONTEXT_ISSUE_MISSING", "blocked context requires blocker codes"
            )

    def _validate_known_ranges(self) -> None:
        actual = self.actual_data_range
        if actual is not None:
            if not self.requested_range.contains_range(actual):
                raise ResearchContextError(
                    "ACTUAL_RANGE_OUTSIDE_REQUESTED", "actual range must be within requested range"
                )
            if actual.end > self.as_of:
                raise ResearchContextError(
                    "ACTUAL_RANGE_AFTER_AS_OF", "actual data end must not follow as_of"
                )

        effective_values = {
            "effective_feature_start": self.effective_feature_start,
            "effective_prediction_start": self.effective_prediction_start,
            "actual_portfolio_start": self.actual_portfolio_start,
        }
        for field, value in effective_values.items():
            if value is None:
                continue
            if actual is None or not actual.contains(value):
                raise ResearchContextError(
                    "EFFECTIVE_START_OUTSIDE_ACTUAL", f"{field} is outside actual data range"
                )

        if self.effective_coverage is not None:
            if actual is None:
                raise ResearchContextError(
                    "EFFECTIVE_COVERAGE_WITHOUT_ACTUAL_RANGE",
                    "effective coverage requires an actual data range",
                )
            for interval in self.effective_coverage.intervals:
                if not actual.contains_range(interval.date_range):
                    raise ResearchContextError(
                        "EFFECTIVE_COVERAGE_OUTSIDE_ACTUAL",
                        f"{interval.source_id} coverage is outside actual data range",
                    )

        if self.evaluation_range is not None:
            if actual is None or not actual.contains_range(self.evaluation_range):
                raise ResearchContextError(
                    "EVALUATION_RANGE_OUTSIDE_ACTUAL",
                    "evaluation range must be within actual data range",
                )
            starts = [value for value in effective_values.values() if value is not None]
            if starts and self.evaluation_range.start < max(starts):
                raise ResearchContextError(
                    "EVALUATION_BEFORE_EFFECTIVE_START",
                    "evaluation start precedes effective feature/prediction/portfolio start",
                )
            if self.evaluation_range.end > self.as_of:
                raise ResearchContextError(
                    "EVALUATION_AFTER_AS_OF", "evaluation end must not follow as_of"
                )

    def _validate_complete(self) -> None:
        required = {
            "actual_data_range": self.actual_data_range,
            "effective_feature_start": self.effective_feature_start,
            "effective_prediction_start": self.effective_prediction_start,
            "actual_portfolio_start": self.actual_portfolio_start,
            "evaluation_range": self.evaluation_range,
            "effective_coverage": self.effective_coverage,
        }
        missing = sorted(field for field, value in required.items() if value is None)
        if missing:
            raise ResearchContextError("COMPLETE_CONTEXT_FIELD_MISSING", ",".join(missing))
        if self.blocking_issues:
            raise ResearchContextError(
                "COMPLETE_CONTEXT_HAS_BLOCKERS", ",".join(self.blocking_issues)
            )
        if not self.data_quality.passed:
            raise ResearchContextError("DATA_QUALITY_NOT_PASSED", self.data_quality.status)
        if not self.effective_coverage or not self.effective_coverage.intervals:
            raise ResearchContextError(
                "EFFECTIVE_COVERAGE_EMPTY", "complete context requires source coverage"
            )

    @property
    def context_id(self) -> str:
        encoded = json.dumps(
            self._semantic_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return f"research_context_{hashlib.sha256(encoded).hexdigest()[:20]}"

    def assert_complete(self) -> Self:
        if self.status is not ContextResolutionStatus.COMPLETE:
            raise ResearchContextError("RESEARCH_CONTEXT_BLOCKED", ",".join(self.blocking_issues))
        return self

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status.value,
            "market_regime_id": self.market_regime_id,
            "regime_anchor": self.regime_anchor.isoformat(),
            "regime_start": self.regime_start.isoformat(),
            "research_window_id": self.research_window_id,
            "research_window_start": self.research_window_start.isoformat(),
            "window_role": self.window_role.value,
            "evidence_role": self.evidence_role.value,
            "requested_range": self.requested_range.to_dict(),
            "actual_data_range": (
                None if self.actual_data_range is None else self.actual_data_range.to_dict()
            ),
            "effective_feature_start": (
                None
                if self.effective_feature_start is None
                else self.effective_feature_start.isoformat()
            ),
            "effective_prediction_start": (
                None
                if self.effective_prediction_start is None
                else self.effective_prediction_start.isoformat()
            ),
            "actual_portfolio_start": (
                None
                if self.actual_portfolio_start is None
                else self.actual_portfolio_start.isoformat()
            ),
            "evaluation_range": (
                None if self.evaluation_range is None else self.evaluation_range.to_dict()
            ),
            "as_of": self.as_of.isoformat(),
            "trading_calendar": self.trading_calendar,
            "effective_coverage": (
                None if self.effective_coverage is None else self.effective_coverage.to_dict()
            ),
            "data_quality": self.data_quality.to_dict(),
            "caveats": list(self.caveats),
            "policy_refs": [item.to_dict() for item in self.policy_refs],
            "blocking_issues": list(self.blocking_issues),
        }

    def to_dict(self) -> dict[str, object]:
        return {"context_id": self.context_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchEvaluationContext:
        requested = payload.get("requested_range")
        if not isinstance(requested, Mapping):
            raise ResearchContextError(
                "INVALID_CONTEXT_PAYLOAD", "requested_range must be a mapping"
            )
        actual_raw = payload.get("actual_data_range")
        evaluation_raw = payload.get("evaluation_range")
        coverage_raw = payload.get("effective_coverage")
        quality_raw = payload.get("data_quality")
        policy_raw = payload.get("policy_refs")
        if not isinstance(quality_raw, Mapping) or not isinstance(policy_raw, list):
            raise ResearchContextError(
                "INVALID_CONTEXT_PAYLOAD", "data_quality/policy_refs are invalid"
            )
        policies = []
        for item in policy_raw:
            if not isinstance(item, Mapping):
                raise ResearchContextError(
                    "INVALID_CONTEXT_PAYLOAD", "policy_ref must be a mapping"
                )
            policies.append(PolicyRef.from_dict(item))
        context = cls(
            status=_enum_value(
                ContextResolutionStatus,
                payload.get("status"),
                "UNKNOWN_CONTEXT_STATUS",
            ),
            market_regime_id=str(payload.get("market_regime_id", "")),
            regime_anchor=_date_value(payload.get("regime_anchor"), "regime_anchor"),
            regime_start=_date_value(payload.get("regime_start"), "regime_start"),
            research_window_id=str(payload.get("research_window_id", "")),
            research_window_start=_date_value(
                payload.get("research_window_start"), "research_window_start"
            ),
            window_role=_enum_value(
                ResearchWindowRole,
                payload.get("window_role"),
                "UNKNOWN_RESEARCH_WINDOW_ROLE",
            ),
            evidence_role=_enum_value(
                EvidenceRole,
                payload.get("evidence_role"),
                "UNKNOWN_EVIDENCE_ROLE",
            ),
            requested_range=DateRange.from_dict(requested),
            actual_data_range=(
                DateRange.from_dict(actual_raw) if isinstance(actual_raw, Mapping) else None
            ),
            effective_feature_start=_optional_date(
                payload.get("effective_feature_start"), "effective_feature_start"
            ),
            effective_prediction_start=_optional_date(
                payload.get("effective_prediction_start"), "effective_prediction_start"
            ),
            actual_portfolio_start=_optional_date(
                payload.get("actual_portfolio_start"), "actual_portfolio_start"
            ),
            evaluation_range=(
                DateRange.from_dict(evaluation_raw) if isinstance(evaluation_raw, Mapping) else None
            ),
            as_of=_date_value(payload.get("as_of"), "as_of"),
            trading_calendar=str(payload.get("trading_calendar", "")),
            effective_coverage=(
                EffectiveCoverage.from_dict(coverage_raw)
                if isinstance(coverage_raw, Mapping)
                else None
            ),
            data_quality=DataQualityContractRef.from_dict(quality_raw),
            caveats=_string_tuple(payload.get("caveats"), "caveats"),
            policy_refs=tuple(policies),
            blocking_issues=_string_tuple(payload.get("blocking_issues"), "blocking_issues"),
        )
        supplied_id = payload.get("context_id")
        if supplied_id is not None and supplied_id != context.context_id:
            raise ResearchContextError(
                "CONTEXT_ID_MISMATCH", f"expected {context.context_id}, got {supplied_id}"
            )
        return context


def _validate_declared_specs(
    *,
    regime: MarketRegimeSpec,
    window: ResearchWindowSpec,
    declared_regime_start: date | None,
    declared_research_window_start: date | None,
) -> None:
    if declared_regime_start is not None and declared_regime_start != regime.start_date:
        raise ResearchContextError(
            "MARKET_REGIME_START_CONFLICT",
            f"{regime.regime_id} expects {regime.start_date}, got {declared_regime_start}",
        )
    if (
        declared_research_window_start is not None
        and declared_research_window_start != window.start_date
    ):
        raise ResearchContextError(
            "RESEARCH_WINDOW_START_CONFLICT",
            f"{window.window_id} expects {window.start_date}, got "
            f"{declared_research_window_start}",
        )


def resolve_complete_research_context(
    *,
    regime: MarketRegimeSpec,
    window: ResearchWindowSpec,
    requested_range: DateRange,
    actual_data_range: DateRange,
    effective_feature_start: date,
    effective_prediction_start: date,
    actual_portfolio_start: date,
    evaluation_range: DateRange,
    as_of: date,
    trading_calendar: str,
    effective_coverage: EffectiveCoverage,
    data_quality: DataQualityContractRef,
    policy_refs: tuple[PolicyRef, ...],
    caveats: tuple[str, ...] = (),
    declared_regime_start: date | None = None,
    declared_research_window_start: date | None = None,
) -> ResearchEvaluationContext:
    _validate_declared_specs(
        regime=regime,
        window=window,
        declared_regime_start=declared_regime_start,
        declared_research_window_start=declared_research_window_start,
    )
    return ResearchEvaluationContext(
        status=ContextResolutionStatus.COMPLETE,
        market_regime_id=regime.regime_id,
        regime_anchor=regime.anchor_date,
        regime_start=regime.start_date,
        research_window_id=window.window_id,
        research_window_start=window.start_date,
        window_role=window.role,
        evidence_role=window.evidence_role,
        requested_range=requested_range,
        actual_data_range=actual_data_range,
        effective_feature_start=effective_feature_start,
        effective_prediction_start=effective_prediction_start,
        actual_portfolio_start=actual_portfolio_start,
        evaluation_range=evaluation_range,
        as_of=as_of,
        trading_calendar=trading_calendar,
        effective_coverage=effective_coverage,
        data_quality=data_quality,
        caveats=(*window.caveats, *caveats),
        policy_refs=policy_refs,
    )


def resolve_blocked_research_context(
    *,
    regime: MarketRegimeSpec,
    window: ResearchWindowSpec,
    requested_range: DateRange,
    as_of: date,
    trading_calendar: str,
    data_quality: DataQualityContractRef,
    policy_refs: tuple[PolicyRef, ...],
    blocking_issues: tuple[str, ...],
    actual_data_range: DateRange | None = None,
    effective_feature_start: date | None = None,
    effective_prediction_start: date | None = None,
    actual_portfolio_start: date | None = None,
    evaluation_range: DateRange | None = None,
    effective_coverage: EffectiveCoverage | None = None,
    caveats: tuple[str, ...] = (),
    declared_regime_start: date | None = None,
    declared_research_window_start: date | None = None,
) -> ResearchEvaluationContext:
    _validate_declared_specs(
        regime=regime,
        window=window,
        declared_regime_start=declared_regime_start,
        declared_research_window_start=declared_research_window_start,
    )
    return ResearchEvaluationContext(
        status=ContextResolutionStatus.BLOCKED,
        market_regime_id=regime.regime_id,
        regime_anchor=regime.anchor_date,
        regime_start=regime.start_date,
        research_window_id=window.window_id,
        research_window_start=window.start_date,
        window_role=window.role,
        evidence_role=window.evidence_role,
        requested_range=requested_range,
        actual_data_range=actual_data_range,
        effective_feature_start=effective_feature_start,
        effective_prediction_start=effective_prediction_start,
        actual_portfolio_start=actual_portfolio_start,
        evaluation_range=evaluation_range,
        as_of=as_of,
        trading_calendar=trading_calendar,
        effective_coverage=effective_coverage,
        data_quality=data_quality,
        caveats=(*window.caveats, *caveats),
        policy_refs=policy_refs,
        blocking_issues=blocking_issues,
    )


def require_research_evaluation_context(
    payload: Mapping[str, object],
) -> ResearchEvaluationContext:
    raw_context = payload.get("research_evaluation_context")
    if not isinstance(raw_context, Mapping):
        raise ResearchContextError(
            "INVESTMENT_ARTIFACT_CONTEXT_MISSING",
            "research_evaluation_context is required",
        )
    context = ResearchEvaluationContext.from_dict(raw_context)
    flat_context_id = payload.get("research_evaluation_context_id")
    if flat_context_id != context.context_id:
        raise ResearchContextError(
            "INVESTMENT_ARTIFACT_CONTEXT_ID_MISMATCH",
            f"expected {context.context_id}, got {flat_context_id}",
        )
    return context
