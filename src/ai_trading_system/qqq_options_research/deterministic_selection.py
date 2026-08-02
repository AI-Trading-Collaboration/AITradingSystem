from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    CanonicalDecimal,
    ContractCandidateSnapshotRecord,
    DailySignalRecord,
    DQReportRecord,
    QQQOptionsSafetyBoundary,
    ReasonCount,
    SelectionDecisionRecord,
)
from ai_trading_system.qqq_options_research.qc_project_adapter import (
    QCProjectAdapterDescriptor,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH = Path(
    "config/research/qqq_options_deterministic_selection_v1.yaml"
)

_UNKNOWN = "UNKNOWN_REQUIRES_POLICY_REVIEW"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_GIT_OBJECT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_UNRESOLVED_RANK_COMPONENTS: tuple[str, ...] = ("option_sid",)
_EXPECTED_ACTIVE_RANK_COMPONENTS = frozenset(
    {
        "delta_distance",
        "dte_distance",
        "moneyness_distance",
        "negative_open_interest",
        "negative_volume",
        "option_sid",
        "relative_spread",
    }
)
_EXPECTED_DQ_CHECK_IDS = (
    "cache_identity",
    "chain_presence",
    "engine_identity",
    "evidence_identity",
    "exchange_calendar_identity",
    "fill_forward_ambiguity",
    "local_cache_dq_scope_separation",
    "open_interest_freshness",
    "order_fill_chronology",
    "prior_day_model_freshness",
    "provider_raw_checksum",
    "quote_freshness",
    "quote_integrity",
    "signal_selection_chronology",
    "symbol_mapping_identity",
)
_SELECTION_STAGE_REQUIRED_CHECK_IDS = (
    "cache_identity",
    "chain_presence",
    "engine_identity",
    "exchange_calendar_identity",
    "fill_forward_ambiguity",
    "local_cache_dq_scope_separation",
    "open_interest_freshness",
    "prior_day_model_freshness",
    "quote_freshness",
    "quote_integrity",
    "signal_selection_chronology",
    "symbol_mapping_identity",
)
_STAGE_DQ_REASON_BY_CHECK = {
    check_id: f"{check_id.upper()}_NOT_PASS" for check_id in _SELECTION_STAGE_REQUIRED_CHECK_IDS
}
_SHARED_POLICY_SHA256 = "d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349"
_DQ_PIT_POLICY_SHA256 = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
_SIGNAL_EXPORT_POLICY_SHA256 = "cf9d6ba3044bdf1d601de1ae7fe6f82fa3e26cc7811dc50160d24dfc902259e9"
_ADAPTER_POLICY_SHA256 = "b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616"
_RESERVED_SOURCE_IDS = frozenset(
    {
        "qqq.options.adapter_descriptor",
        "qqq.options.daily_signal",
        "qqq.options.dq_report",
        "qqq.options.selection_candidate_set",
        "qqq.options.selection_policy",
    }
)


class QQQOptionSelectionContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a portable identifier")
    return checked


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _git_object_sha(value: str, field: str) -> str:
    if not _GIT_OBJECT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase Git object id")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must be UTC")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _selection_safety() -> QQQOptionsSafetyBoundary:
    return QQQOptionsSafetyBoundary(
        research_only=True,
        promotion_allowed=False,
        paper_shadow_allowed=False,
        production_allowed=False,
        raw_options_data_export_allowed=False,
        strategy_execution_allowed=False,
        bounded_cloud_pilot_authorized=False,
        production_effect="none",
        broker_action="none",
    )


RankComponent = Literal[
    "delta_distance",
    "dte_distance",
    "moneyness_distance",
    "negative_open_interest",
    "negative_volume",
    "option_sid",
    "relative_spread",
]


class UnresolvedSelectionCriteria(_PolicyModel):
    mode: Literal["UNRESOLVED"]
    min_dte: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    target_dte: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_dte: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_abs_moneyness_deviation: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    min_abs_delta: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    target_abs_delta: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_abs_delta: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_quote_age_seconds: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_relative_spread: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    min_open_interest: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    min_volume: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    rank_components: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]


class ActiveSelectionCriteria(_PolicyModel):
    mode: Literal["ACTIVE"]
    min_dte: int = Field(ge=0)
    target_dte: int = Field(ge=0)
    max_dte: int = Field(ge=0)
    max_abs_moneyness_deviation: CanonicalDecimal
    min_abs_delta: CanonicalDecimal
    target_abs_delta: CanonicalDecimal
    max_abs_delta: CanonicalDecimal
    max_quote_age_seconds: int = Field(ge=0)
    max_relative_spread: CanonicalDecimal
    min_open_interest: int = Field(ge=0)
    min_volume: int = Field(ge=0)
    rank_components: tuple[RankComponent, ...]

    @model_validator(mode="after")
    def _validate_active_criteria(self) -> Self:
        if not self.min_dte <= self.target_dte <= self.max_dte:
            raise ValueError("DTE criteria must satisfy min <= target <= max")
        if self.max_abs_moneyness_deviation < 0:
            raise ValueError("moneyness deviation cannot be negative")
        if not (
            Decimal("0")
            <= self.min_abs_delta
            <= self.target_abs_delta
            <= self.max_abs_delta
            <= Decimal("1")
        ):
            raise ValueError("absolute delta criteria must be ordered within [0, 1]")
        if self.max_relative_spread < 0:
            raise ValueError("relative spread cannot be negative")
        if len(self.rank_components) != len(set(self.rank_components)):
            raise ValueError("rank components must be unique")
        if set(self.rank_components) != _EXPECTED_ACTIVE_RANK_COMPONENTS:
            raise ValueError("active rank components must be complete")
        if self.rank_components[-1] != "option_sid":
            raise ValueError("option_sid must be the final stable tie-break")
        return self


SelectionCriteria = Annotated[
    UnresolvedSelectionCriteria | ActiveSelectionCriteria,
    Field(discriminator="mode"),
]


class QQQOptionSelectionSafety(_PolicyModel):
    research_only: Literal[True]
    long_premium_only: Literal[True]
    single_leg_only: Literal[True]
    roll_allowed: Literal[False]
    multi_leg_allowed: Literal[False]
    short_option_allowed: Literal[False]
    order_generation_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionSelectionPolicy(_PolicyModel):
    schema_version: Literal["qqq_option_deterministic_selection_policy.v1"]
    policy_id: Literal["qqq_options_deterministic_selection_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED_BASELINE", "OWNER_REVIEWED_ACTIVE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    selection_authorized: bool
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    signal_export_policy_sha256: str
    adapter_policy_sha256: str
    primary_research_start: date
    approved_non_primary_authority_count: int
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    selection_stage_required_check_ids: tuple[str, ...]
    criteria: SelectionCriteria
    safety: QQQOptionSelectionSafety

    @field_validator(
        "owner",
        "owner_decision",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "shared_contract_sha256",
        "shared_policy_sha256",
        "dq_pit_policy_sha256",
        "signal_export_policy_sha256",
        "adapter_policy_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        if self.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("selection policy must bind the exact shared contract")
        if self.shared_policy_sha256 != _SHARED_POLICY_SHA256:
            raise ValueError("selection policy must bind the exact shared policy")
        if self.dq_pit_policy_sha256 != _DQ_PIT_POLICY_SHA256:
            raise ValueError("selection policy must bind the exact DQ/PIT policy")
        if self.signal_export_policy_sha256 != _SIGNAL_EXPORT_POLICY_SHA256:
            raise ValueError("selection policy must bind the exact signal policy")
        if self.adapter_policy_sha256 != _ADAPTER_POLICY_SHA256:
            raise ValueError("selection policy must bind the exact adapter policy")
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.approved_non_primary_authority_count != 0:
            raise ValueError("no non-primary window authority is approved")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker drifted")
        if self.selection_stage_required_check_ids != _SELECTION_STAGE_REQUIRED_CHECK_IDS:
            raise ValueError("selection-stage DQ check set drifted")
        is_active = isinstance(self.criteria, ActiveSelectionCriteria)
        if self.status == "OWNER_REVIEWED_ACTIVE":
            if not self.selection_authorized or not is_active:
                raise ValueError("active policy requires authorized active criteria")
        elif self.selection_authorized or is_active:
            raise ValueError("baseline policy must remain unauthorized and unresolved")
        return self


@dataclass(frozen=True)
class QQQOptionSelectionPolicyLoadResult:
    policy: QQQOptionSelectionPolicy
    policy_path: Path
    policy_sha256: str


class QQQOptionSelectionCandidateInput(_StrictModel):
    option_sid: str
    right: Literal["CALL", "PUT"]
    expiry: date
    strike_usd_per_share: CanonicalDecimal
    contract_multiplier: int = Field(gt=0)
    underlying_price_usd_per_share: CanonicalDecimal
    model_delta: CanonicalDecimal
    prior_day_model_as_of_session: date
    open_interest: int = Field(ge=0)
    open_interest_as_of_session: date
    volume: int = Field(ge=0)
    volume_as_of_session: date
    quote_bid_per_share: CanonicalDecimal
    quote_ask_per_share: CanonicalDecimal
    quote_end_utc: datetime
    source_id: str
    source_sha256: str
    dq_report_bytes: bytes
    dq_report_sha256: str
    field_export_classification: Literal["EXPORT_PROHIBITED", "QC_ONLY_NOT_EXPORTED"]

    @field_validator("option_sid")
    @classmethod
    def _validate_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        checked = _identifier(value, "source_id")
        if checked in _RESERVED_SOURCE_IDS:
            raise ValueError("candidate source_id collides with a reserved source id")
        return checked

    @field_validator("source_sha256", "dq_report_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("quote_end_utc")
    @classmethod
    def _validate_quote_time(cls, value: datetime) -> datetime:
        return _utc(value, "quote_end_utc")

    @model_validator(mode="after")
    def _validate_candidate(self) -> Self:
        if self.strike_usd_per_share <= 0 or self.underlying_price_usd_per_share <= 0:
            raise ValueError("strike and underlying price must be positive")
        if not Decimal("-1") <= self.model_delta <= Decimal("1"):
            raise ValueError("model_delta must be within [-1, 1]")
        if self.quote_bid_per_share < 0 or self.quote_ask_per_share <= 0:
            raise ValueError("quote prices are invalid")
        if self.quote_ask_per_share < self.quote_bid_per_share:
            raise ValueError("crossed quotes are invalid")
        if _content_sha256(self.dq_report_bytes) != self.dq_report_sha256:
            raise ValueError("DQ report file hash does not match bytes")
        DQReportRecord.from_json_bytes(self.dq_report_bytes)
        return self

    @property
    def dq_report(self) -> DQReportRecord:
        return DQReportRecord.from_json_bytes(self.dq_report_bytes)


class QQQOptionSelectionRequest(_StrictModel):
    adapter_descriptor: QCProjectAdapterDescriptor
    daily_signal: DailySignalRecord
    selection_session: date
    expected_prior_session: date
    selection_snapshot_utc: datetime
    decision_id: str
    created_at_utc: datetime
    producer_version: str
    lineage_id: str
    candidates: tuple[QQQOptionSelectionCandidateInput, ...]

    @field_validator("selection_snapshot_utc", "created_at_utc")
    @classmethod
    def _validate_times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator("decision_id", "producer_version", "lineage_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_request(self) -> Self:
        descriptor = self.adapter_descriptor
        signal = self.daily_signal
        QCProjectAdapterDescriptor.from_json_bytes(descriptor.canonical_bytes)
        DailySignalRecord.from_json_bytes(signal.canonical_bytes)
        for candidate in self.candidates:
            QQQOptionSelectionCandidateInput.model_validate(candidate.model_dump())
        if self.created_at_utc < self.selection_snapshot_utc:
            raise ValueError("selection record cannot be created before the snapshot")
        if self.expected_prior_session >= self.selection_session:
            raise ValueError("expected prior session must precede selection session")
        if self.selection_snapshot_utc.date() != self.selection_session:
            raise ValueError("selection snapshot UTC date must equal selection session")
        if signal.signal_as_of_utc >= self.selection_snapshot_utc:
            raise ValueError("signal must strictly precede selection")
        if signal.earliest_effective_session != self.selection_session:
            raise ValueError("signal effective session must equal selection session")
        if not (descriptor.evaluated_start <= self.selection_session <= descriptor.evaluated_end):
            raise ValueError("selection session is outside the evaluated window")
        if descriptor.run_id != signal.run_id:
            raise ValueError("adapter descriptor and daily signal run ids differ")
        if descriptor.repository_code_sha != signal.repository_code_sha:
            raise ValueError("adapter descriptor and daily signal code ids differ")
        if descriptor.run_id != signal.run_id or descriptor.daily_signal_count <= 0:
            raise ValueError("adapter descriptor signal identity is invalid")
        if descriptor.adapter_policy_sha256 != _ADAPTER_POLICY_SHA256:
            raise ValueError("adapter policy hash drifted")
        if descriptor.signal_export_policy_sha256 != _SIGNAL_EXPORT_POLICY_SHA256:
            raise ValueError("signal policy hash drifted")
        if descriptor.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("shared contract hash drifted")
        if descriptor.shared_policy_sha256 != _SHARED_POLICY_SHA256:
            raise ValueError("shared policy hash drifted")
        if descriptor.dq_pit_policy_sha256 != _DQ_PIT_POLICY_SHA256:
            raise ValueError("DQ/PIT policy hash drifted")
        if descriptor.requested_start != date(2021, 2, 22):
            raise ValueError("primary requested start must remain 2021-02-22")
        if descriptor.evaluated_start != date(2021, 2, 22):
            raise ValueError("primary evaluated start must remain 2021-02-22")
        if descriptor.option_event_dq_status != "NOT_EVALUATED" or (
            descriptor.option_event_pit_status != "NOT_EVALUATED"
        ):
            raise ValueError("adapter cannot pre-promote option-event DQ/PIT")
        if descriptor.input_admission_status != "UNKNOWN_REQUIRES_PLATFORM_EVIDENCE":
            raise ValueError("adapter input admission status drifted")
        if descriptor.cloud_run_authorized:
            raise ValueError("selection request cannot authorize a cloud run")
        if signal.policy_sha256 != _SIGNAL_EXPORT_POLICY_SHA256:
            raise ValueError("daily signal policy hash drifted")
        if signal.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("daily signal contract hash drifted")
        if signal.requested_start != descriptor.requested_start or (
            signal.requested_end != descriptor.requested_end
            or signal.evaluated_start != descriptor.evaluated_start
            or signal.evaluated_end != descriptor.evaluated_end
        ):
            raise ValueError("daily signal and adapter ranges differ")
        option_sids = tuple(item.option_sid for item in self.candidates)
        if len(option_sids) != len(set(option_sids)):
            raise ValueError("candidate option SIDs must be unique")
        if any(item.quote_end_utc > self.selection_snapshot_utc for item in self.candidates):
            raise ValueError("candidate quote cannot be in the future of selection")
        return self


class QQQOptionSelectionResult(_StrictModel):
    policy_sha256: str
    adapter_descriptor_sha256: str
    candidate_set_sha256: str
    selection_authorized: bool
    cash_preservation_required: bool
    candidate_snapshots: tuple[ContractCandidateSnapshotRecord, ...]
    decision: SelectionDecisionRecord

    @field_validator("policy_sha256", "adapter_descriptor_sha256", "candidate_set_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_result(self) -> Self:
        if self.decision.candidate_set_sha256 != self.candidate_set_sha256:
            raise ValueError("decision candidate-set hash differs from result")
        if self.cash_preservation_required != (self.decision.selected_option_sid is None):
            raise ValueError("cash preservation must match no-contract disposition")
        if not self.selection_authorized and self.decision.selected_option_sid is not None:
            raise ValueError("unauthorized selection cannot contain a selected SID")
        sids = tuple(item.option_sid for item in self.candidate_snapshots)
        if sids != tuple(sorted(sids)) or len(sids) != len(set(sids)):
            raise ValueError("candidate snapshots must be sorted and unique by SID")
        selected_sid = self.decision.selected_option_sid
        if selected_sid is not None:
            selected = [
                item for item in self.candidate_snapshots if item.option_sid == selected_sid
            ]
            if len(selected) != 1 or not selected[0].eligible:
                raise ValueError("selected SID must identify one eligible candidate")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))


def load_qqq_option_selection_policy(
    path: Path = DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionSelectionPolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionSelectionPolicy.model_validate(payload, strict=False)
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QQQOptionSelectionPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=_content_sha256(content),
    )


def _candidate_hash_payload(
    candidate: QQQOptionSelectionCandidateInput,
) -> dict[str, Any]:
    report = candidate.dq_report
    payload = candidate.model_dump(mode="json", exclude={"dq_report_bytes"})
    payload["dq_report_content_sha256"] = report.content_sha256
    return payload


def build_qqq_option_selection_candidate_set_sha256(
    request: QQQOptionSelectionRequest,
    *,
    policy_path: Path = DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> str:
    loaded = load_qqq_option_selection_policy(policy_path, project_root=project_root)
    payload = {
        "schema_version": "qqq_option_selection_candidate_set.v1",
        "adapter_descriptor_sha256": request.adapter_descriptor.canonical_sha256,
        "daily_signal_content_sha256": request.daily_signal.content_sha256,
        "policy_sha256": loaded.policy_sha256,
        "run_id": request.adapter_descriptor.run_id,
        "selection_session": request.selection_session.isoformat(),
        "expected_prior_session": request.expected_prior_session.isoformat(),
        "selection_snapshot_utc": request.selection_snapshot_utc.isoformat(),
        "candidates": [
            _candidate_hash_payload(candidate)
            for candidate in sorted(request.candidates, key=lambda item: item.option_sid)
        ],
    }
    return _content_sha256(_canonical_json_bytes(payload))


def _source_pairs(
    request: QQQOptionSelectionRequest,
    candidate: QQQOptionSelectionCandidateInput | None = None,
) -> tuple[tuple[str, str], ...]:
    pairs = [
        ("qqq.options.adapter_descriptor", request.adapter_descriptor.canonical_sha256),
        ("qqq.options.daily_signal", request.daily_signal.content_sha256),
    ]
    if candidate is not None:
        pairs.extend(
            (
                (candidate.source_id, candidate.source_sha256),
                ("qqq.options.dq_report", candidate.dq_report_sha256),
            )
        )
    ordered = tuple(sorted(pairs))
    if len({source_id for source_id, _ in ordered}) != len(ordered):
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_SOURCE_ID_COLLISION",
            "selection source ids must remain unique",
        )
    return ordered


def _validate_candidate_report(
    *,
    request: QQQOptionSelectionRequest,
    candidate: QQQOptionSelectionCandidateInput,
) -> tuple[DQReportRecord, tuple[str, ...]]:
    report = candidate.dq_report
    descriptor = request.adapter_descriptor
    if report.run_id != descriptor.run_id:
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_RUN_MISMATCH", candidate.option_sid
        )
    if report.repository_code_sha != descriptor.repository_code_sha:
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_CODE_MISMATCH", candidate.option_sid
        )
    if report.policy_id != "qqq_options_dq_pit_identity_v1" or (
        report.policy_version != "1.0.0" or report.policy_sha256 != _DQ_PIT_POLICY_SHA256
    ):
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_POLICY_MISMATCH", candidate.option_sid
        )
    if report.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_CONTRACT_MISMATCH", candidate.option_sid
        )
    if (
        report.requested_start != descriptor.requested_start
        or report.requested_end != descriptor.requested_end
        or report.evaluated_start != descriptor.evaluated_start
        or report.evaluated_end != descriptor.evaluated_end
    ):
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_RANGE_MISMATCH", candidate.option_sid
        )
    if report.storage_timezone != "UTC" or report.exchange_timezone != "America/New_York":
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_TIMEZONE_MISMATCH", candidate.option_sid
        )
    if report.scope != "qqq_options_event_dq_pit_identity":
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_SCOPE_MISMATCH", candidate.option_sid
        )
    if report.safety != _selection_safety():
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_SAFETY_MISMATCH", candidate.option_sid
        )
    if report.generated_at_utc < request.selection_snapshot_utc:
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_PRE_SELECTION_REPORT", candidate.option_sid
        )
    check_ids = tuple(check.check_id for check in report.checks)
    if check_ids != _EXPECTED_DQ_CHECK_IDS:
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_CHECK_SET_MISMATCH", candidate.option_sid
        )
    source_pairs = dict(zip(report.source_ids, report.source_checksums, strict=True))
    required_pairs = {
        "qqq.options.adapter_descriptor": descriptor.canonical_sha256,
        candidate.source_id: candidate.source_sha256,
    }
    if any(
        source_pairs.get(source_id) != checksum for source_id, checksum in required_pairs.items()
    ):
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_DQ_SOURCE_MISMATCH", candidate.option_sid
        )
    reasons: list[str] = []
    checks = {check.check_id: check for check in report.checks}
    if report.dq_status == "FAIL":
        reasons.append("DQ_REPORT_FAIL")
    if report.pit_status == "FAIL":
        reasons.append("PIT_REPORT_FAIL")
    for check_id in _SELECTION_STAGE_REQUIRED_CHECK_IDS:
        if checks[check_id].status != "PASS":
            reasons.append(_STAGE_DQ_REASON_BY_CHECK[check_id])
    return report, tuple(sorted(set(reasons)))


def _relative_spread(candidate: QQQOptionSelectionCandidateInput) -> Decimal:
    midpoint = (candidate.quote_bid_per_share + candidate.quote_ask_per_share) / 2
    return (candidate.quote_ask_per_share - candidate.quote_bid_per_share) / midpoint


def _candidate_metrics(
    *,
    request: QQQOptionSelectionRequest,
    candidate: QQQOptionSelectionCandidateInput,
) -> dict[str, Decimal | int | str]:
    dte = (candidate.expiry - request.selection_session).days
    moneyness = candidate.strike_usd_per_share / candidate.underlying_price_usd_per_share
    return {
        "dte": dte,
        "moneyness": moneyness,
        "abs_moneyness_deviation": abs(moneyness - Decimal("1")),
        "abs_delta": abs(candidate.model_delta),
        "relative_spread": _relative_spread(candidate),
        "option_sid": candidate.option_sid,
    }


def _eligibility_reasons(
    *,
    request: QQQOptionSelectionRequest,
    candidate: QQQOptionSelectionCandidateInput,
    criteria: ActiveSelectionCriteria,
    dq_reasons: tuple[str, ...],
) -> tuple[str, ...]:
    reasons = list(dq_reasons)
    expected_right = "CALL" if request.daily_signal.signal == "LONG_CALL" else "PUT"
    metrics = _candidate_metrics(request=request, candidate=candidate)
    dte = int(metrics["dte"])
    deviation = Decimal(metrics["abs_moneyness_deviation"])
    absolute_delta = Decimal(metrics["abs_delta"])
    relative_spread = Decimal(metrics["relative_spread"])
    if candidate.right != expected_right:
        reasons.append("WRONG_OPTION_RIGHT")
    if dte < criteria.min_dte:
        reasons.append("DTE_BELOW_MIN")
    if dte > criteria.max_dte:
        reasons.append("DTE_ABOVE_MAX")
    if deviation > criteria.max_abs_moneyness_deviation:
        reasons.append("MONEYNESS_OUTSIDE_POLICY")
    if absolute_delta < criteria.min_abs_delta:
        reasons.append("DELTA_BELOW_MIN")
    if absolute_delta > criteria.max_abs_delta:
        reasons.append("DELTA_ABOVE_MAX")
    if request.selection_snapshot_utc - candidate.quote_end_utc > timedelta(
        seconds=criteria.max_quote_age_seconds
    ):
        reasons.append("QUOTE_TOO_OLD")
    if relative_spread > criteria.max_relative_spread:
        reasons.append("SPREAD_TOO_WIDE")
    if candidate.open_interest < criteria.min_open_interest:
        reasons.append("OPEN_INTEREST_BELOW_MIN")
    if candidate.volume < criteria.min_volume:
        reasons.append("VOLUME_BELOW_MIN")
    if candidate.prior_day_model_as_of_session != request.expected_prior_session:
        reasons.append("MODEL_SESSION_NOT_EXACT_PRIOR")
    if candidate.open_interest_as_of_session != request.expected_prior_session:
        reasons.append("OI_SESSION_NOT_EXACT_PRIOR")
    if candidate.volume_as_of_session != request.expected_prior_session:
        reasons.append("VOLUME_SESSION_NOT_EXACT_PRIOR")
    return tuple(sorted(set(reasons)))


def _rank_key(
    *,
    request: QQQOptionSelectionRequest,
    candidate: QQQOptionSelectionCandidateInput,
    criteria: ActiveSelectionCriteria,
) -> tuple[Decimal | int | str, ...]:
    metrics = _candidate_metrics(request=request, candidate=candidate)
    values: dict[str, Decimal | int | str] = {
        "dte_distance": abs(int(metrics["dte"]) - criteria.target_dte),
        "moneyness_distance": Decimal(metrics["abs_moneyness_deviation"]),
        "delta_distance": abs(Decimal(metrics["abs_delta"]) - criteria.target_abs_delta),
        "relative_spread": Decimal(metrics["relative_spread"]),
        "negative_open_interest": -candidate.open_interest,
        "negative_volume": -candidate.volume,
        "option_sid": candidate.option_sid,
    }
    return tuple(values[component] for component in criteria.rank_components)


def _envelope_dq_status(report: DQReportRecord) -> Literal["FAIL", "NOT_EVALUATED"]:
    return "FAIL" if report.dq_status == "FAIL" else "NOT_EVALUATED"


def _envelope_pit_status(report: DQReportRecord) -> Literal["FAIL", "NOT_EVALUATED"]:
    return "FAIL" if report.pit_status == "FAIL" else "NOT_EVALUATED"


def _candidate_snapshot(
    *,
    request: QQQOptionSelectionRequest,
    candidate: QQQOptionSelectionCandidateInput,
    report: DQReportRecord,
    policy: QQQOptionSelectionPolicy,
    policy_sha256: str,
    index: int,
    eligible: bool,
    quote_is_fresh: bool,
) -> ContractCandidateSnapshotRecord:
    pairs = _source_pairs(request, candidate)
    metrics = _candidate_metrics(request=request, candidate=candidate)
    return ContractCandidateSnapshotRecord.seal(
        schema_name="contract_candidate_snapshot",
        schema_version="1.0.0",
        run_id=request.adapter_descriptor.run_id,
        record_id=f"{request.decision_id}.candidate.{index:04d}",
        created_at_utc=request.created_at_utc,
        producer_version=request.producer_version,
        repository_code_sha=request.adapter_descriptor.repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy_sha256,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=tuple(source_id for source_id, _ in pairs),
        source_checksums=tuple(checksum for _, checksum in pairs),
        requested_start=request.adapter_descriptor.requested_start,
        requested_end=request.adapter_descriptor.requested_end,
        evaluated_start=request.adapter_descriptor.evaluated_start,
        evaluated_end=request.adapter_descriptor.evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=_envelope_dq_status(report),
        pit_status=_envelope_pit_status(report),
        export_classification=candidate.field_export_classification,
        lineage_id=request.lineage_id,
        safety=_selection_safety(),
        selection_snapshot_utc=request.selection_snapshot_utc,
        option_sid=candidate.option_sid,
        right=candidate.right,
        expiry=candidate.expiry,
        strike_usd_per_share=candidate.strike_usd_per_share,
        contract_multiplier=candidate.contract_multiplier,
        dte=int(metrics["dte"]),
        moneyness=Decimal(metrics["moneyness"]),
        prior_day_model_as_of_session=candidate.prior_day_model_as_of_session,
        open_interest_as_of_session=candidate.open_interest_as_of_session,
        quote_bid_per_share=candidate.quote_bid_per_share,
        quote_ask_per_share=candidate.quote_ask_per_share,
        quote_end_utc=candidate.quote_end_utc,
        quote_validity="VALID" if quote_is_fresh else "STALE",
        eligible=eligible,
        field_export_classification=candidate.field_export_classification,
    )


def _decision(
    *,
    request: QQQOptionSelectionRequest,
    policy: QQQOptionSelectionPolicy,
    policy_sha256: str,
    candidate_set_sha256: str,
    rank_components: tuple[str, ...],
    selected_option_sid: str | None,
    no_contract_reason: str | None,
    rejected_counts: dict[str, int],
    dq_status: Literal["FAIL", "NOT_EVALUATED"],
    pit_status: Literal["FAIL", "NOT_EVALUATED"],
) -> SelectionDecisionRecord:
    pairs = tuple(
        sorted(
            (
                ("qqq.options.adapter_descriptor", request.adapter_descriptor.canonical_sha256),
                ("qqq.options.daily_signal", request.daily_signal.content_sha256),
                ("qqq.options.selection_candidate_set", candidate_set_sha256),
                ("qqq.options.selection_policy", policy_sha256),
            )
        )
    )
    return SelectionDecisionRecord.seal(
        schema_name="selection_decision",
        schema_version="1.0.0",
        run_id=request.adapter_descriptor.run_id,
        record_id=request.decision_id,
        created_at_utc=request.created_at_utc,
        producer_version=request.producer_version,
        repository_code_sha=request.adapter_descriptor.repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy_sha256,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=tuple(source_id for source_id, _ in pairs),
        source_checksums=tuple(checksum for _, checksum in pairs),
        requested_start=request.adapter_descriptor.requested_start,
        requested_end=request.adapter_descriptor.requested_end,
        evaluated_start=request.adapter_descriptor.evaluated_start,
        evaluated_end=request.adapter_descriptor.evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=dq_status,
        pit_status=pit_status,
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id=request.lineage_id,
        safety=_selection_safety(),
        decision_id=request.decision_id,
        selection_snapshot_utc=request.selection_snapshot_utc,
        selected_option_sid=selected_option_sid,
        no_contract_reason=no_contract_reason,
        candidate_set_sha256=candidate_set_sha256,
        stable_rank_components=rank_components,
        rejected_counts=tuple(
            ReasonCount(reason_code=reason, count=count)
            for reason, count in sorted(rejected_counts.items())
        ),
    )


def select_qqq_option_contract(
    request: QQQOptionSelectionRequest,
    *,
    policy_path: Path = DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionSelectionResult:
    loaded = load_qqq_option_selection_policy(policy_path, project_root=project_root)
    policy = loaded.policy
    candidate_set_sha256 = build_qqq_option_selection_candidate_set_sha256(
        request, policy_path=policy_path, project_root=project_root
    )
    if request.daily_signal.signal == "FLAT":
        decision = _decision(
            request=request,
            policy=policy,
            policy_sha256=loaded.policy_sha256,
            candidate_set_sha256=candidate_set_sha256,
            rank_components=_UNRESOLVED_RANK_COMPONENTS,
            selected_option_sid=None,
            no_contract_reason="FLAT_SIGNAL_CASH",
            rejected_counts={"FLAT_SIGNAL": len(request.candidates)},
            dq_status="NOT_EVALUATED",
            pit_status="NOT_EVALUATED",
        )
        return QQQOptionSelectionResult(
            policy_sha256=loaded.policy_sha256,
            adapter_descriptor_sha256=request.adapter_descriptor.canonical_sha256,
            candidate_set_sha256=candidate_set_sha256,
            selection_authorized=False,
            cash_preservation_required=True,
            candidate_snapshots=(),
            decision=decision,
        )
    if not policy.selection_authorized:
        decision = _decision(
            request=request,
            policy=policy,
            policy_sha256=loaded.policy_sha256,
            candidate_set_sha256=candidate_set_sha256,
            rank_components=_UNRESOLVED_RANK_COMPONENTS,
            selected_option_sid=None,
            no_contract_reason="SELECTION_POLICY_REVIEW_REQUIRED",
            rejected_counts={"SELECTION_POLICY_REVIEW_REQUIRED": len(request.candidates)},
            dq_status="NOT_EVALUATED",
            pit_status="NOT_EVALUATED",
        )
        return QQQOptionSelectionResult(
            policy_sha256=loaded.policy_sha256,
            adapter_descriptor_sha256=request.adapter_descriptor.canonical_sha256,
            candidate_set_sha256=candidate_set_sha256,
            selection_authorized=False,
            cash_preservation_required=True,
            candidate_snapshots=(),
            decision=decision,
        )
    if not isinstance(policy.criteria, ActiveSelectionCriteria):
        raise QQQOptionSelectionContractError(
            "QQQ_OPTION_SELECTION_ACTIVE_CRITERIA_MISSING",
            "authorized policy must carry active criteria",
        )
    criteria = policy.criteria
    rejected_counts: dict[str, int] = {}
    evaluations: list[
        tuple[
            QQQOptionSelectionCandidateInput,
            DQReportRecord,
            tuple[str, ...],
            tuple[Decimal | int | str, ...],
        ]
    ] = []
    snapshots: list[ContractCandidateSnapshotRecord] = []
    any_dq_fail = False
    any_pit_fail = False
    sorted_candidates = sorted(request.candidates, key=lambda item: item.option_sid)
    for index, candidate in enumerate(sorted_candidates):
        report, dq_reasons = _validate_candidate_report(request=request, candidate=candidate)
        reasons = _eligibility_reasons(
            request=request,
            candidate=candidate,
            criteria=criteria,
            dq_reasons=dq_reasons,
        )
        for reason in reasons:
            rejected_counts[reason] = rejected_counts.get(reason, 0) + 1
        eligible = not reasons
        quote_is_fresh = dict((check.check_id, check.status) for check in report.checks)[
            "quote_freshness"
        ] == "PASS" and request.selection_snapshot_utc - candidate.quote_end_utc <= timedelta(
            seconds=criteria.max_quote_age_seconds
        )
        snapshots.append(
            _candidate_snapshot(
                request=request,
                candidate=candidate,
                report=report,
                policy=policy,
                policy_sha256=loaded.policy_sha256,
                index=index,
                eligible=eligible,
                quote_is_fresh=quote_is_fresh,
            )
        )
        if eligible:
            evaluations.append(
                (
                    candidate,
                    report,
                    reasons,
                    _rank_key(request=request, candidate=candidate, criteria=criteria),
                )
            )
        any_dq_fail = any_dq_fail or report.dq_status == "FAIL"
        any_pit_fail = any_pit_fail or report.pit_status == "FAIL"
    selected_option_sid: str | None = None
    no_contract_reason: str | None = "NO_ELIGIBLE_CONTRACT_CASH"
    if evaluations:
        selected_option_sid = min(evaluations, key=lambda item: item[3])[0].option_sid
        no_contract_reason = None
    decision = _decision(
        request=request,
        policy=policy,
        policy_sha256=loaded.policy_sha256,
        candidate_set_sha256=candidate_set_sha256,
        rank_components=tuple(criteria.rank_components),
        selected_option_sid=selected_option_sid,
        no_contract_reason=no_contract_reason,
        rejected_counts=rejected_counts,
        dq_status="FAIL" if any_dq_fail else "NOT_EVALUATED",
        pit_status="FAIL" if any_pit_fail else "NOT_EVALUATED",
    )
    return QQQOptionSelectionResult(
        policy_sha256=loaded.policy_sha256,
        adapter_descriptor_sha256=request.adapter_descriptor.canonical_sha256,
        candidate_set_sha256=candidate_set_sha256,
        selection_authorized=True,
        cash_preservation_required=selected_option_sid is None,
        candidate_snapshots=tuple(snapshots),
        decision=decision,
    )


__all__ = [
    "ActiveSelectionCriteria",
    "DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH",
    "QQQOptionSelectionCandidateInput",
    "QQQOptionSelectionContractError",
    "QQQOptionSelectionPolicy",
    "QQQOptionSelectionPolicyLoadResult",
    "QQQOptionSelectionRequest",
    "QQQOptionSelectionResult",
    "QQQOptionSelectionSafety",
    "RankComponent",
    "UnresolvedSelectionCriteria",
    "build_qqq_option_selection_candidate_set_sha256",
    "load_qqq_option_selection_policy",
    "select_qqq_option_contract",
]
