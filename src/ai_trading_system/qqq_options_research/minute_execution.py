from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
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
    DQReportRecord,
    DQStatus,
    FillEventRecord,
    OrderEventRecord,
    OrderIntentRecord,
    OrderSide,
    QQQOptionsSafetyBoundary,
    SelectionDecisionRecord,
)
from ai_trading_system.qqq_options_research.deterministic_selection import (
    DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
    load_qqq_option_selection_policy,
)
from ai_trading_system.qqq_options_research.dq_pit_identity import (
    QQQOptionsDQIdentityEvaluation,
    QQQOptionsDQObservation,
    evaluate_qqq_options_dq_pit_identity,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_MINUTE_EXECUTION_POLICY_PATH = Path(
    "config/research/qqq_options_minute_execution_reality_v1.yaml"
)

_UNKNOWN = "UNKNOWN_REQUIRES_POLICY_REVIEW"
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_SHARED_POLICY_SHA256 = "d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349"
_DQ_PIT_POLICY_SHA256 = "1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358"
_ADAPTER_POLICY_SHA256 = "b9e48f0b53a6259a5bbc9594cbe1929721568d1723d498591ce14b8e3be92616"
_TRACKED_SELECTION_POLICY_SHA256 = (
    "bbb51a147e89dd279f35ed005810b7274c1ac2ff302df492c183e2f7f2abad30"
)
_SELECTION_STAGE_CHECK_IDS = (
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
_EXECUTION_STAGE_CHECK_IDS = _SELECTION_STAGE_CHECK_IDS + ("order_fill_chronology",)
_RESERVED_QUOTE_SOURCE_IDS = frozenset(
    {
        "qqq.options.adapter_descriptor",
        "qqq.options.dq_policy",
        "qqq.options.execution_dq_report",
        "qqq.options.execution_policy",
        "qqq.options.execution_quote_set",
        "qqq.options.selection_decision",
        "qqq.options.selection_dq_report",
        "qqq.options.selection_policy",
    }
)

ExecutionScenarioRole = Literal[
    "REALITY_BASELINE", "SENSITIVITY", "ISOLATION_SENSITIVITY"
]
ExecutionDisposition = Literal["TRADEABLE", "MISSING", "VENUE_REJECTED"]
ExecutionReason = Literal[
    "EXECUTION_POLICY_REVIEW_REQUIRED",
    "SELECTION_POLICY_REVIEW_REQUIRED",
    "NO_SELECTED_CONTRACT_CASH",
    "SELECTION_DQ_NOT_PASS",
    "FILLED",
    "PARTIAL_CANCELED",
    "NO_FILL_CANCELED",
    "VENUE_REJECTED",
    "EXECUTION_DQ_REJECTED",
]


class QQQOptionMinuteExecutionContractError(ValueError):
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


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must use UTC offset")
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


def _next_minute_boundary(value: datetime) -> datetime:
    normalized = _utc(value, "minute boundary input")
    return normalized.replace(second=0, microsecond=0) + timedelta(minutes=1)


def _shared_safety() -> QQQOptionsSafetyBoundary:
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


class UnresolvedExecutionCriteria(_PolicyModel):
    mode: Literal["UNRESOLVED"]
    scenario_role: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    dq_caveat: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    submission_latency_ms: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    fill_latency_ms: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_quote_age_ms: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    marketable_limit_buffer_per_share: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    slippage_per_share: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    fee_per_contract_usd: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_contracts_per_quote: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    cancel_after_ms: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    reality_baseline: Literal[False]


class ActiveExecutionCriteria(_PolicyModel):
    mode: Literal["ACTIVE"]
    scenario_role: ExecutionScenarioRole
    dq_caveat: str
    submission_latency_ms: int = Field(ge=0)
    fill_latency_ms: int = Field(gt=0)
    max_quote_age_ms: int = Field(gt=0)
    marketable_limit_buffer_per_share: CanonicalDecimal
    slippage_per_share: CanonicalDecimal
    fee_per_contract_usd: CanonicalDecimal
    max_contracts_per_quote: int = Field(gt=0)
    cancel_after_ms: int = Field(gt=0)
    reality_baseline: bool

    @field_validator("dq_caveat")
    @classmethod
    def _validate_caveat(cls, value: str) -> str:
        return _required_text(value, "dq_caveat")

    @model_validator(mode="after")
    def _validate_active(self) -> Self:
        if min(
            self.marketable_limit_buffer_per_share,
            self.slippage_per_share,
            self.fee_per_contract_usd,
        ) < 0:
            raise ValueError("execution prices, slippage and fee cannot be negative")
        if self.scenario_role == "REALITY_BASELINE" and not self.reality_baseline:
            raise ValueError("REALITY_BASELINE role must declare reality_baseline=true")
        if self.scenario_role != "REALITY_BASELINE" and self.reality_baseline:
            raise ValueError("only REALITY_BASELINE may declare reality_baseline=true")
        if self.slippage_per_share == 0 and (
            self.scenario_role != "ISOLATION_SENSITIVITY" or self.reality_baseline
        ):
            raise ValueError("zero slippage is only an isolation sensitivity")
        return self


ExecutionCriteria = Annotated[
    UnresolvedExecutionCriteria | ActiveExecutionCriteria,
    Field(discriminator="mode"),
]


class QQQOptionMinuteExecutionSafety(_PolicyModel):
    research_only: Literal[True]
    long_premium_only: Literal[True]
    minute_quotes_only: Literal[True]
    next_independent_minute_required: Literal[True]
    daily_close_fill_allowed: Literal[False]
    same_bar_fill_allowed: Literal[False]
    fill_forward_allowed: Literal[False]
    mid_or_last_price_fill_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    external_order_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionMinuteExecutionPolicy(_PolicyModel):
    schema_version: Literal["qqq_options_minute_execution_reality_policy.v1"]
    policy_id: Literal["qqq_options_minute_execution_reality_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED_BASELINE", "OWNER_REVIEWED_ACTIVE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    execution_authorized: bool
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    adapter_policy_sha256: str
    selection_policy_sha256: str
    primary_research_start: date
    approved_non_primary_authority_count: int
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    criteria: ExecutionCriteria
    safety: QQQOptionMinuteExecutionSafety

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
        "adapter_policy_sha256",
        "selection_policy_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        if self.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("execution policy must bind the exact shared contract")
        if self.shared_policy_sha256 != _SHARED_POLICY_SHA256:
            raise ValueError("execution policy must bind the exact shared policy")
        if self.dq_pit_policy_sha256 != _DQ_PIT_POLICY_SHA256:
            raise ValueError("execution policy must bind the exact DQ/PIT policy")
        if self.adapter_policy_sha256 != _ADAPTER_POLICY_SHA256:
            raise ValueError("execution policy must bind the exact adapter policy")
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.approved_non_primary_authority_count != 0:
            raise ValueError("no non-primary research-window authority is approved")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker drifted")
        active = isinstance(self.criteria, ActiveExecutionCriteria)
        if self.status == "OWNER_REVIEWED_ACTIVE":
            if not self.execution_authorized or not active:
                raise ValueError("active execution policy requires active criteria")
        elif self.execution_authorized or active:
            raise ValueError("baseline execution policy must remain unauthorized")
        if self.status == "OWNER_REVIEW_REQUIRED_BASELINE" and (
            self.selection_policy_sha256 != _TRACKED_SELECTION_POLICY_SHA256
        ):
            raise ValueError("baseline must bind the exact blocked 2485 policy")
        return self


@dataclass(frozen=True)
class QQQOptionMinuteExecutionPolicyLoadResult:
    policy: QQQOptionMinuteExecutionPolicy
    policy_path: Path
    policy_sha256: str


class QQQOptionExecutionQuoteInput(_StrictModel):
    source_id: str
    source_sha256: str
    quote_start_utc: datetime
    quote_end_utc: datetime
    resolution: Literal["MINUTE"]
    disposition: ExecutionDisposition
    bid_per_share: CanonicalDecimal | None
    ask_per_share: CanonicalDecimal | None
    available_contracts: int = Field(ge=0)
    rejection_reason_code: str | None

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        checked = _identifier(value, "source_id")
        if checked in _RESERVED_QUOTE_SOURCE_IDS:
            raise ValueError("quote source id collides with a reserved source id")
        return checked

    @field_validator("source_sha256")
    @classmethod
    def _validate_source_hash(cls, value: str) -> str:
        return _sha256(value, "source_sha256")

    @field_validator("quote_start_utc", "quote_end_utc")
    @classmethod
    def _validate_times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator("rejection_reason_code")
    @classmethod
    def _validate_reason(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "rejection_reason_code")

    @model_validator(mode="after")
    def _validate_quote(self) -> Self:
        if self.quote_end_utc - self.quote_start_utc != timedelta(minutes=1):
            raise ValueError("execution quote must be one exact minute")
        if self.disposition == "TRADEABLE":
            if self.bid_per_share is None or self.ask_per_share is None:
                raise ValueError("tradeable quote requires bid and ask")
            if self.bid_per_share < 0 or self.ask_per_share <= 0:
                raise ValueError("quote prices are invalid")
            if self.available_contracts <= 0 or self.rejection_reason_code is not None:
                raise ValueError("tradeable quote capacity/reason is invalid")
        elif self.disposition == "MISSING":
            if any(value is not None for value in (self.bid_per_share, self.ask_per_share)):
                raise ValueError("missing quote cannot carry bid or ask")
            if self.available_contracts != 0 or self.rejection_reason_code is not None:
                raise ValueError("missing quote cannot carry capacity or rejection")
        elif self.rejection_reason_code is None or self.available_contracts != 0:
            raise ValueError("venue rejection requires a reason and zero capacity")
        return self

    def identity_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class QQQOptionExecutionRequest(_StrictModel):
    selection_decision: SelectionDecisionRecord
    side: OrderSide
    contracts: int = Field(gt=0)
    contract_multiplier: int = Field(gt=0)
    reserved_cash_usd: CanonicalDecimal
    selection_quote_bid_per_share: CanonicalDecimal | None
    selection_quote_ask_per_share: CanonicalDecimal | None
    selection_quote_end_utc: datetime | None
    selection_quote_source_id: str | None
    selection_quote_source_sha256: str | None
    signal_as_of_utc: datetime
    intent_id: str
    platform_order_id: str
    intent_at_utc: datetime
    producer_version: str
    lineage_id: str
    selection_dq_report_bytes: bytes | None
    selection_dq_report_sha256: str | None
    dq_observation_template: QQQOptionsDQObservation | None
    dq_record_id_prefix: str
    dq_lineage_id: str
    quotes: tuple[QQQOptionExecutionQuoteInput, ...]

    @field_validator(
        "signal_as_of_utc", "intent_at_utc", "selection_quote_end_utc"
    )
    @classmethod
    def _validate_times(
        cls, value: datetime | None, info: ValidationInfo
    ) -> datetime | None:
        return None if value is None else _utc(value, str(info.field_name))

    @field_validator(
        "intent_id",
        "platform_order_id",
        "producer_version",
        "lineage_id",
        "dq_record_id_prefix",
        "dq_lineage_id",
    )
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("selection_quote_source_id")
    @classmethod
    def _validate_optional_source_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        checked = _identifier(value, "selection_quote_source_id")
        if checked in _RESERVED_QUOTE_SOURCE_IDS:
            raise ValueError("selection quote source id is reserved")
        return checked

    @field_validator("selection_quote_source_sha256", "selection_dq_report_sha256")
    @classmethod
    def _validate_optional_hash(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
        return None if value is None else _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_request(self) -> Self:
        SelectionDecisionRecord.from_json_bytes(self.selection_decision.canonical_bytes)
        if self.signal_as_of_utc >= self.selection_decision.selection_snapshot_utc:
            raise ValueError("signal must strictly precede selection")
        if self.intent_at_utc <= self.selection_decision.selection_snapshot_utc:
            raise ValueError("intent must strictly follow selection")
        if self.reserved_cash_usd < 0:
            raise ValueError("reserved cash cannot be negative")
        quote_identity = (
            self.selection_quote_bid_per_share,
            self.selection_quote_ask_per_share,
            self.selection_quote_end_utc,
            self.selection_quote_source_id,
            self.selection_quote_source_sha256,
        )
        if any(item is None for item in quote_identity) != all(
            item is None for item in quote_identity
        ):
            raise ValueError("selection quote identity must be complete or absent")
        if self.selection_quote_bid_per_share is not None:
            assert self.selection_quote_ask_per_share is not None
            assert self.selection_quote_end_utc is not None
            if self.selection_quote_bid_per_share < 0 or self.selection_quote_ask_per_share <= 0:
                raise ValueError("selection quote prices are invalid")
            if self.selection_quote_ask_per_share < self.selection_quote_bid_per_share:
                raise ValueError("selection quote cannot be crossed")
            if self.selection_quote_end_utc > self.selection_decision.selection_snapshot_utc:
                raise ValueError("selection quote cannot be after selection")
        report_identity = (self.selection_dq_report_bytes, self.selection_dq_report_sha256)
        if (report_identity[0] is None) != (report_identity[1] is None):
            raise ValueError("selection DQ report bytes and hash must be paired")
        if self.selection_dq_report_bytes is not None:
            assert self.selection_dq_report_sha256 is not None
            if _content_sha256(self.selection_dq_report_bytes) != self.selection_dq_report_sha256:
                raise ValueError("selection DQ report file hash differs from bytes")
            DQReportRecord.from_json_bytes(self.selection_dq_report_bytes)
        source_ids = tuple(item.source_id for item in self.quotes)
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("execution quote source ids must be unique")
        semantic_ids = tuple(
            (item.quote_start_utc, item.quote_end_utc, item.source_id) for item in self.quotes
        )
        if len(semantic_ids) != len(set(semantic_ids)):
            raise ValueError("execution quote identities must be unique")
        return self

    @property
    def selection_dq_report(self) -> DQReportRecord | None:
        if self.selection_dq_report_bytes is None:
            return None
        return DQReportRecord.from_json_bytes(self.selection_dq_report_bytes)


class QQQOptionExecutionResult(_StrictModel):
    schema_version: Literal["qqq_options_minute_execution_result.v1"]
    policy_sha256: str
    selection_policy_sha256: str
    selection_decision_sha256: str
    quote_set_sha256: str
    execution_authorized: bool
    selection_authorized: bool
    cash_preservation_required: bool
    reason_code: ExecutionReason
    execution_stage_dq_status: DQStatus
    global_dq_status: DQStatus
    global_pit_status: DQStatus
    accounting_status: Literal["NOT_EVALUATED"]
    order_intent: OrderIntentRecord | None
    order_events: tuple[OrderEventRecord, ...]
    fill_events: tuple[FillEventRecord, ...]
    execution_dq_reports: tuple[DQReportRecord, ...]
    content_sha256: str

    @field_validator(
        "policy_sha256",
        "selection_policy_sha256",
        "selection_decision_sha256",
        "quote_set_sha256",
        "content_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    def content_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _content_sha256(_canonical_json_bytes(self.content_payload()))

    @model_validator(mode="after")
    def _validate_result(self) -> Self:
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("execution result content hash does not match payload")
        if self.cash_preservation_required != (not self.fill_events):
            raise ValueError("cash preservation must match the absence of fills")
        if not self.execution_authorized or not self.selection_authorized:
            if self.order_intent is not None or self.order_events or self.fill_events:
                raise ValueError("unauthorized result cannot contain order or fill records")
        if self.order_intent is None and (self.order_events or self.fill_events):
            raise ValueError("order/fill records require a shared order intent")
        if self.order_intent is not None:
            OrderIntentRecord.from_json_bytes(self.order_intent.canonical_bytes)
        for event in self.order_events:
            OrderEventRecord.from_json_bytes(event.canonical_bytes)
        for fill in self.fill_events:
            FillEventRecord.from_json_bytes(fill.canonical_bytes)
        for report in self.execution_dq_reports:
            DQReportRecord.from_json_bytes(report.canonical_bytes)
        event_sequences = tuple(item.event_sequence for item in self.order_events)
        if event_sequences != tuple(range(len(event_sequences))):
            raise ValueError("order event sequences must be contiguous from zero")
        fill_sequences = tuple(item.fill_sequence for item in self.fill_events)
        if fill_sequences != tuple(range(1, len(fill_sequences) + 1)):
            raise ValueError("fill sequences must be contiguous from one")
        cumulative = tuple(item.filled_contracts_total for item in self.order_events)
        if any(
            after < before
            for before, after in zip(cumulative, cumulative[1:], strict=False)
        ):
            raise ValueError("cumulative fill quantity must be monotone")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        provisional = cls.model_construct(**payload, content_sha256=_UNSEALED_SHA256)
        return cls(**payload, content_sha256=provisional.compute_content_sha256())

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            result = cls.model_validate_json(content)
        except ValueError as exc:
            raise QQQOptionMinuteExecutionContractError(
                "QQQ_OPTION_EXECUTION_RESULT_INVALID", str(exc)
            ) from exc
        if content != result.canonical_bytes:
            raise QQQOptionMinuteExecutionContractError(
                "QQQ_OPTION_EXECUTION_RESULT_NOT_CANONICAL",
                "execution result bytes do not match canonical JSON encoding",
            )
        return result


def load_qqq_options_minute_execution_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_MINUTE_EXECUTION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionMinuteExecutionPolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionMinuteExecutionPolicy.model_validate(payload, strict=False)
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_MINUTE_EXECUTION_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QQQOptionMinuteExecutionPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=_content_sha256(content),
    )


def build_qqq_option_execution_quote_set_sha256(
    request: QQQOptionExecutionRequest,
) -> str:
    payload = {
        "schema_version": "qqq_option_execution_quote_set.v1",
        "selection_decision_sha256": request.selection_decision.content_sha256,
        "selection_quote": request.model_dump(
            mode="json",
            include={
                "selection_quote_bid_per_share",
                "selection_quote_ask_per_share",
                "selection_quote_end_utc",
                "selection_quote_source_id",
                "selection_quote_source_sha256",
            },
        ),
        "quotes": [
            item.identity_payload()
            for item in sorted(
                request.quotes,
                key=lambda value: (
                    value.quote_start_utc,
                    value.quote_end_utc,
                    value.source_id,
                ),
            )
        ],
    }
    return _content_sha256(_canonical_json_bytes(payload))


def _decision_source_map(decision: SelectionDecisionRecord) -> dict[str, str]:
    sources = dict(zip(decision.source_ids, decision.source_checksums, strict=True))
    required = {
        "qqq.options.adapter_descriptor",
        "qqq.options.selection_candidate_set",
        "qqq.options.selection_policy",
    }
    if not required.issubset(sources):
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_SELECTION_LINEAGE_MISSING",
            "selection decision lacks required adapter/candidate/policy lineage",
        )
    if sources["qqq.options.selection_candidate_set"] != decision.candidate_set_sha256:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_SELECTION_CANDIDATE_HASH_MISMATCH",
            "selection candidate-set source differs from decision",
        )
    return sources


def _selection_report_status(
    request: QQQOptionExecutionRequest,
    *,
    adapter_descriptor_sha256: str,
) -> tuple[DQStatus, DQReportRecord | None]:
    report = request.selection_dq_report
    if report is None:
        return "NOT_EVALUATED", None
    decision = request.selection_decision
    if report.scope != "qqq_options_event_dq_pit_identity":
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_SCOPE_MISMATCH", "selection DQ scope drifted"
        )
    if report.policy_sha256 != _DQ_PIT_POLICY_SHA256:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_POLICY_MISMATCH", "selection DQ policy drifted"
        )
    if report.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_CONTRACT_MISMATCH", "selection DQ contract drifted"
        )
    if (
        report.run_id != decision.run_id
        or report.repository_code_sha != decision.repository_code_sha
        or report.requested_start != decision.requested_start
        or report.requested_end != decision.requested_end
        or report.evaluated_start != decision.evaluated_start
        or report.evaluated_end != decision.evaluated_end
    ):
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_ENVELOPE_MISMATCH",
            "selection DQ run/code/range identity differs from decision",
        )
    if report.generated_at_utc > request.intent_at_utc:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_AS_OF_FUTURE", "selection DQ is after intent"
        )
    sources = dict(zip(report.source_ids, report.source_checksums, strict=True))
    if sources.get("qqq.options.adapter_descriptor") != adapter_descriptor_sha256:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_ADAPTER_MISMATCH",
            "selection DQ adapter identity differs from decision",
        )
    statuses = {item.check_id: item.status for item in report.checks}
    if set(_SELECTION_STAGE_CHECK_IDS) - set(statuses):
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_CHECK_SET_MISSING",
            "selection DQ report lacks required stage checks",
        )
    required_statuses = tuple(statuses[item] for item in _SELECTION_STAGE_CHECK_IDS)
    if "FAIL" in required_statuses:
        return "FAIL", report
    if "NOT_EVALUATED" in required_statuses:
        return "NOT_EVALUATED", report
    return "PASS", report


def _execution_stage_status(report: DQReportRecord) -> DQStatus:
    statuses = {item.check_id: item.status for item in report.checks}
    if set(_EXECUTION_STAGE_CHECK_IDS) - set(statuses):
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_CHECK_SET_MISSING",
            "execution DQ report lacks required stage checks",
        )
    required = tuple(statuses[item] for item in _EXECUTION_STAGE_CHECK_IDS)
    if "FAIL" in required:
        return "FAIL"
    if "NOT_EVALUATED" in required:
        return "NOT_EVALUATED"
    return "PASS"


def _record_sources(
    *,
    request: QQQOptionExecutionRequest,
    selection_policy_sha256: str,
    execution_policy_sha256: str,
    quote_set_sha256: str,
    dq_report: DQReportRecord | None,
) -> tuple[tuple[str, str], ...]:
    pairs = [
        ("qqq.options.selection_decision", request.selection_decision.content_sha256),
        ("qqq.options.selection_policy", selection_policy_sha256),
        ("qqq.options.execution_policy", execution_policy_sha256),
        ("qqq.options.execution_quote_set", quote_set_sha256),
    ]
    if request.selection_dq_report_sha256 is not None:
        pairs.append(("qqq.options.selection_dq_report", request.selection_dq_report_sha256))
    if dq_report is not None:
        pairs.append(("qqq.options.execution_dq_report", dq_report.content_sha256))
    if request.selection_quote_source_id is not None:
        assert request.selection_quote_source_sha256 is not None
        pairs.append(
            (request.selection_quote_source_id, request.selection_quote_source_sha256)
        )
    ordered = tuple(sorted(pairs))
    if len({item[0] for item in ordered}) != len(ordered):
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_SOURCE_ID_COLLISION",
            "execution record source ids must be unique",
        )
    return ordered


def _record_envelope(
    *,
    request: QQQOptionExecutionRequest,
    policy: QQQOptionMinuteExecutionPolicy,
    policy_sha256: str,
    created_at_utc: datetime,
    record_id: str,
    dq_report: DQReportRecord | None,
    sources: tuple[tuple[str, str], ...],
) -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "run_id": request.selection_decision.run_id,
        "record_id": record_id,
        "created_at_utc": created_at_utc,
        "producer_version": request.producer_version,
        "repository_code_sha": request.selection_decision.repository_code_sha,
        "policy_id": policy.policy_id,
        "policy_version": policy.policy_version,
        "policy_sha256": policy_sha256,
        "contract_schema_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
        "source_ids": tuple(item[0] for item in sources),
        "source_checksums": tuple(item[1] for item in sources),
        "requested_start": request.selection_decision.requested_start,
        "requested_end": request.selection_decision.requested_end,
        "evaluated_start": request.selection_decision.evaluated_start,
        "evaluated_end": request.selection_decision.evaluated_end,
        "storage_timezone": "UTC",
        "exchange_timezone": "America/New_York",
        "dq_status": "NOT_EVALUATED" if dq_report is None else dq_report.dq_status,
        "pit_status": "NOT_EVALUATED" if dq_report is None else dq_report.pit_status,
        "export_classification": "EXPORT_ALLOWED_DERIVED",
        "lineage_id": request.lineage_id,
        "safety": _shared_safety(),
    }


def _evaluate_dq(
    *,
    request: QQQOptionExecutionRequest,
    quote: QQQOptionExecutionQuoteInput | None,
    intent_at_utc: datetime,
    submit_at_utc: datetime,
    fill_at_utc: datetime | None,
    index: int,
    project_root: Path,
) -> QQQOptionsDQIdentityEvaluation:
    template = request.dq_observation_template
    if template is None:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_DQ_OBSERVATION_MISSING",
            "active execution requires a canonical DQ observation template",
        )
    quote_end = None if quote is None else quote.quote_end_utc
    observed_at = fill_at_utc or (
        quote_end if quote_end is not None else submit_at_utc
    )
    payload = template.model_dump(mode="python")
    payload.update(
        {
            "observed_at_utc": observed_at,
            "chain_present": True,
            "candidate_present": request.selection_decision.selected_option_sid is not None,
            "signal_as_of_utc": request.signal_as_of_utc,
            "selection_snapshot_utc": request.selection_decision.selection_snapshot_utc,
            "order_intent_utc": intent_at_utc,
            "order_submit_utc": submit_at_utc,
            "fill_quote_end_utc": quote_end if fill_at_utc is not None else None,
            "fill_utc": fill_at_utc,
        }
    )
    observation = QQQOptionsDQObservation.model_validate(payload, strict=True)
    return evaluate_qqq_options_dq_pit_identity(
        source_record=request.selection_decision,
        observation=observation,
        record_id=f"{request.dq_record_id_prefix}.{index:04d}",
        created_at_utc=observed_at,
        producer_version=request.producer_version,
        lineage_id=request.dq_lineage_id,
        project_root=project_root,
    )


def _no_order_result(
    *,
    policy_sha256: str,
    selection_policy_sha256: str,
    request: QQQOptionExecutionRequest,
    quote_set_sha256: str,
    execution_authorized: bool,
    selection_authorized: bool,
    reason_code: ExecutionReason,
    stage_status: DQStatus = "NOT_EVALUATED",
) -> QQQOptionExecutionResult:
    return QQQOptionExecutionResult.seal(
        schema_version="qqq_options_minute_execution_result.v1",
        policy_sha256=policy_sha256,
        selection_policy_sha256=selection_policy_sha256,
        selection_decision_sha256=request.selection_decision.content_sha256,
        quote_set_sha256=quote_set_sha256,
        execution_authorized=execution_authorized,
        selection_authorized=selection_authorized,
        cash_preservation_required=True,
        reason_code=reason_code,
        execution_stage_dq_status=stage_status,
        global_dq_status="NOT_EVALUATED" if stage_status != "FAIL" else "FAIL",
        global_pit_status="NOT_EVALUATED" if stage_status != "FAIL" else "FAIL",
        accounting_status="NOT_EVALUATED",
        order_intent=None,
        order_events=(),
        fill_events=(),
        execution_dq_reports=(),
    )


def simulate_qqq_option_minute_execution(
    request: QQQOptionExecutionRequest,
    *,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_MINUTE_EXECUTION_POLICY_PATH,
    selection_policy_path: Path = DEFAULT_QQQ_OPTION_SELECTION_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionExecutionResult:
    loaded = load_qqq_options_minute_execution_policy(
        policy_path, project_root=project_root
    )
    policy = loaded.policy
    selection_loaded = load_qqq_option_selection_policy(
        selection_policy_path, project_root=project_root
    )
    decision = request.selection_decision
    decision_sources = _decision_source_map(decision)
    if decision.policy_sha256 != selection_loaded.policy_sha256 or (
        decision_sources["qqq.options.selection_policy"] != selection_loaded.policy_sha256
    ):
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_SELECTION_POLICY_MISMATCH",
            "selection decision does not bind the loaded selection policy",
        )
    if policy.selection_policy_sha256 != selection_loaded.policy_sha256:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_POLICY_SELECTOR_MISMATCH",
            "execution policy does not bind the loaded selector policy",
        )
    quote_set_sha256 = build_qqq_option_execution_quote_set_sha256(request)
    if not policy.execution_authorized:
        return _no_order_result(
            policy_sha256=loaded.policy_sha256,
            selection_policy_sha256=selection_loaded.policy_sha256,
            request=request,
            quote_set_sha256=quote_set_sha256,
            execution_authorized=False,
            selection_authorized=selection_loaded.policy.selection_authorized,
            reason_code="EXECUTION_POLICY_REVIEW_REQUIRED",
        )
    if not selection_loaded.policy.selection_authorized:
        return _no_order_result(
            policy_sha256=loaded.policy_sha256,
            selection_policy_sha256=selection_loaded.policy_sha256,
            request=request,
            quote_set_sha256=quote_set_sha256,
            execution_authorized=True,
            selection_authorized=False,
            reason_code="SELECTION_POLICY_REVIEW_REQUIRED",
        )
    if decision.selected_option_sid is None:
        return _no_order_result(
            policy_sha256=loaded.policy_sha256,
            selection_policy_sha256=selection_loaded.policy_sha256,
            request=request,
            quote_set_sha256=quote_set_sha256,
            execution_authorized=True,
            selection_authorized=True,
            reason_code="NO_SELECTED_CONTRACT_CASH",
        )
    criteria = policy.criteria
    if not isinstance(criteria, ActiveExecutionCriteria):
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_ACTIVE_CRITERIA_MISSING",
            "authorized execution policy lacks active criteria",
        )
    if any(
        item is None
        for item in (
            request.selection_quote_bid_per_share,
            request.selection_quote_ask_per_share,
            request.selection_quote_end_utc,
            request.selection_quote_source_id,
            request.selection_quote_source_sha256,
        )
    ):
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_SELECTION_QUOTE_MISSING",
            "selected contract execution requires a complete selection quote",
        )
    adapter_sha256 = decision_sources["qqq.options.adapter_descriptor"]
    selection_dq_status, _ = _selection_report_status(
        request, adapter_descriptor_sha256=adapter_sha256
    )
    if selection_dq_status != "PASS":
        return _no_order_result(
            policy_sha256=loaded.policy_sha256,
            selection_policy_sha256=selection_loaded.policy_sha256,
            request=request,
            quote_set_sha256=quote_set_sha256,
            execution_authorized=True,
            selection_authorized=True,
            reason_code="SELECTION_DQ_NOT_PASS",
            stage_status=selection_dq_status,
        )
    assert request.selection_quote_bid_per_share is not None
    assert request.selection_quote_ask_per_share is not None
    reference_price = (
        request.selection_quote_ask_per_share
        if request.side == "BUY_TO_OPEN"
        else request.selection_quote_bid_per_share
    )
    limit_price = (
        reference_price + criteria.marketable_limit_buffer_per_share
        if request.side == "BUY_TO_OPEN"
        else reference_price - criteria.marketable_limit_buffer_per_share
    )
    if limit_price <= 0:
        raise QQQOptionMinuteExecutionContractError(
            "QQQ_OPTION_EXECUTION_LIMIT_NOT_POSITIVE",
            "reviewed marketable-limit policy produced a non-positive limit",
        )
    not_before = _next_minute_boundary(request.intent_at_utc)
    submit_at = not_before + timedelta(milliseconds=criteria.submission_latency_ms)
    cancel_at = submit_at + timedelta(milliseconds=criteria.cancel_after_ms)
    base_sources = _record_sources(
        request=request,
        selection_policy_sha256=selection_loaded.policy_sha256,
        execution_policy_sha256=loaded.policy_sha256,
        quote_set_sha256=quote_set_sha256,
        dq_report=None,
    )
    intent = OrderIntentRecord.seal(
        schema_name="order_intent",
        **_record_envelope(
            request=request,
            policy=policy,
            policy_sha256=loaded.policy_sha256,
            created_at_utc=request.intent_at_utc,
            record_id=request.intent_id,
            dq_report=None,
            sources=base_sources,
        ),
        intent_id=request.intent_id,
        decision_id=decision.decision_id,
        option_sid=decision.selected_option_sid,
        side=request.side,
        contracts=request.contracts,
        order_type="MARKETABLE_LIMIT",
        limit_price_per_share=limit_price,
        reserved_cash_usd=request.reserved_cash_usd,
        not_before_utc=not_before,
    )
    order_events: list[OrderEventRecord] = []
    fill_events: list[FillEventRecord] = []
    dq_reports: list[DQReportRecord] = []

    def append_order_event(
        event_type: Literal[
            "CREATED",
            "SUBMITTED",
            "UPDATED",
            "PARTIALLY_FILLED",
            "FILLED",
            "CANCELED",
            "REJECTED",
        ],
        at: datetime,
        filled_total: int,
        reason: str | None,
        report: DQReportRecord | None,
    ) -> None:
        sources = _record_sources(
            request=request,
            selection_policy_sha256=selection_loaded.policy_sha256,
            execution_policy_sha256=loaded.policy_sha256,
            quote_set_sha256=quote_set_sha256,
            dq_report=report,
        )
        sequence = len(order_events)
        order_events.append(
            OrderEventRecord.seal(
                schema_name="order_event",
                **_record_envelope(
                    request=request,
                    policy=policy,
                    policy_sha256=loaded.policy_sha256,
                    created_at_utc=at,
                    record_id=f"{request.platform_order_id}.event.{sequence:04d}",
                    dq_report=report,
                    sources=sources,
                ),
                platform_order_id=request.platform_order_id,
                event_sequence=sequence,
                event_type=event_type,
                event_at_utc=at,
                side=request.side,
                order_contracts=request.contracts,
                filled_contracts_total=filled_total,
                limit_price_per_share=limit_price,
                reason_code=reason,
            )
        )

    append_order_event("CREATED", request.intent_at_utc, 0, None, None)
    append_order_event("SUBMITTED", submit_at, 0, None, None)
    filled_total = 0
    terminal_reason: ExecutionReason | None = None
    independent_quote_start = _next_minute_boundary(submit_at)
    ordered_quotes = sorted(
        request.quotes,
        key=lambda item: (item.quote_start_utc, item.quote_end_utc, item.source_id),
    )
    for quote_index, quote in enumerate(ordered_quotes, start=1):
        if quote.quote_start_utc < independent_quote_start:
            continue
        if quote.quote_end_utc > cancel_at:
            break
        if quote.disposition == "VENUE_REJECTED":
            evaluation = _evaluate_dq(
                request=request,
                quote=quote,
                intent_at_utc=request.intent_at_utc,
                submit_at_utc=submit_at,
                fill_at_utc=None,
                index=quote_index,
                project_root=project_root,
            )
            dq_reports.append(evaluation.report)
            append_order_event(
                "REJECTED",
                quote.quote_end_utc,
                filled_total,
                quote.rejection_reason_code,
                evaluation.report,
            )
            terminal_reason = "VENUE_REJECTED"
            break
        if quote.disposition == "MISSING":
            evaluation = _evaluate_dq(
                request=request,
                quote=quote,
                intent_at_utc=request.intent_at_utc,
                submit_at_utc=submit_at,
                fill_at_utc=None,
                index=quote_index,
                project_root=project_root,
            )
            dq_reports.append(evaluation.report)
            continue
        assert quote.bid_per_share is not None
        assert quote.ask_per_share is not None
        crossed = quote.ask_per_share < quote.bid_per_share
        quote_side_price = (
            quote.ask_per_share if request.side == "BUY_TO_OPEN" else quote.bid_per_share
        )
        candidate_fill_price = (
            quote_side_price + criteria.slippage_per_share
            if request.side == "BUY_TO_OPEN"
            else quote_side_price - criteria.slippage_per_share
        )
        marketable = (
            candidate_fill_price <= limit_price
            if request.side == "BUY_TO_OPEN"
            else candidate_fill_price >= limit_price
        )
        fill_at = quote.quote_end_utc + timedelta(milliseconds=criteria.fill_latency_ms)
        stale = fill_at - quote.quote_end_utc > timedelta(
            milliseconds=criteria.max_quote_age_ms
        )
        if crossed or stale or not marketable or candidate_fill_price <= 0:
            evaluation = _evaluate_dq(
                request=request,
                quote=quote,
                intent_at_utc=request.intent_at_utc,
                submit_at_utc=submit_at,
                fill_at_utc=None,
                index=quote_index,
                project_root=project_root,
            )
            dq_reports.append(evaluation.report)
            if crossed:
                append_order_event(
                    "REJECTED",
                    quote.quote_end_utc,
                    filled_total,
                    "CROSSED_EXECUTION_QUOTE",
                    evaluation.report,
                )
                terminal_reason = "EXECUTION_DQ_REJECTED"
                break
            continue
        evaluation = _evaluate_dq(
            request=request,
            quote=quote,
            intent_at_utc=request.intent_at_utc,
            submit_at_utc=submit_at,
            fill_at_utc=fill_at,
            index=quote_index,
            project_root=project_root,
        )
        dq_reports.append(evaluation.report)
        stage_status = _execution_stage_status(evaluation.report)
        if stage_status != "PASS":
            if stage_status == "FAIL":
                append_order_event(
                    "REJECTED",
                    fill_at,
                    filled_total,
                    "EXECUTION_DQ_NOT_PASS",
                    evaluation.report,
                )
                terminal_reason = "EXECUTION_DQ_REJECTED"
                break
            continue
        fill_contracts = min(
            request.contracts - filled_total,
            quote.available_contracts,
            criteria.max_contracts_per_quote,
        )
        if fill_contracts <= 0:
            continue
        fill_sequence = len(fill_events) + 1
        fill_sources = _record_sources(
            request=request,
            selection_policy_sha256=selection_loaded.policy_sha256,
            execution_policy_sha256=loaded.policy_sha256,
            quote_set_sha256=quote_set_sha256,
            dq_report=evaluation.report,
        )
        gross = candidate_fill_price * request.contract_multiplier * fill_contracts
        fee = criteria.fee_per_contract_usd * fill_contracts
        fill_events.append(
            FillEventRecord.seal(
                schema_name="fill_event",
                **_record_envelope(
                    request=request,
                    policy=policy,
                    policy_sha256=loaded.policy_sha256,
                    created_at_utc=fill_at,
                    record_id=f"{request.platform_order_id}.fill.{fill_sequence:04d}",
                    dq_report=evaluation.report,
                    sources=fill_sources,
                ),
                platform_order_id=request.platform_order_id,
                fill_sequence=fill_sequence,
                fill_at_utc=fill_at,
                quote_end_utc=quote.quote_end_utc,
                side=request.side,
                filled_contracts=fill_contracts,
                fill_price_per_share=candidate_fill_price,
                contract_multiplier=request.contract_multiplier,
                fee_usd=fee,
                settlement_currency="USD",
                quote_side="ASK" if request.side == "BUY_TO_OPEN" else "BID",
                gross_cash_delta_usd=-gross if request.side == "BUY_TO_OPEN" else gross,
            )
        )
        filled_total += fill_contracts
        event_type: Literal["PARTIALLY_FILLED", "FILLED"] = (
            "FILLED" if filled_total == request.contracts else "PARTIALLY_FILLED"
        )
        append_order_event(event_type, fill_at, filled_total, None, evaluation.report)
        if filled_total == request.contracts:
            terminal_reason = "FILLED"
            break
    if terminal_reason is None:
        append_order_event(
            "CANCELED",
            cancel_at,
            filled_total,
            "PARTIAL_FILL_TIMEOUT" if filled_total else "NO_FILL_TIMEOUT",
            dq_reports[-1] if dq_reports else None,
        )
        terminal_reason = "PARTIAL_CANCELED" if filled_total else "NO_FILL_CANCELED"
    final_report = dq_reports[-1] if dq_reports else None
    stage_status = (
        "NOT_EVALUATED" if final_report is None else _execution_stage_status(final_report)
    )
    result = QQQOptionExecutionResult.seal(
        schema_version="qqq_options_minute_execution_result.v1",
        policy_sha256=loaded.policy_sha256,
        selection_policy_sha256=selection_loaded.policy_sha256,
        selection_decision_sha256=decision.content_sha256,
        quote_set_sha256=quote_set_sha256,
        execution_authorized=True,
        selection_authorized=True,
        cash_preservation_required=not fill_events,
        reason_code=terminal_reason,
        execution_stage_dq_status=stage_status,
        global_dq_status="NOT_EVALUATED" if final_report is None else final_report.dq_status,
        global_pit_status="NOT_EVALUATED" if final_report is None else final_report.pit_status,
        accounting_status="NOT_EVALUATED",
        order_intent=intent,
        order_events=tuple(order_events),
        fill_events=tuple(fill_events),
        execution_dq_reports=tuple(dq_reports),
    )
    return QQQOptionExecutionResult.from_json_bytes(result.canonical_bytes)


__all__ = [
    "ActiveExecutionCriteria",
    "DEFAULT_QQQ_OPTIONS_MINUTE_EXECUTION_POLICY_PATH",
    "ExecutionDisposition",
    "ExecutionReason",
    "ExecutionScenarioRole",
    "QQQOptionExecutionQuoteInput",
    "QQQOptionExecutionRequest",
    "QQQOptionExecutionResult",
    "QQQOptionMinuteExecutionContractError",
    "QQQOptionMinuteExecutionPolicy",
    "QQQOptionMinuteExecutionPolicyLoadResult",
    "QQQOptionMinuteExecutionSafety",
    "UnresolvedExecutionCriteria",
    "build_qqq_option_execution_quote_set_sha256",
    "load_qqq_options_minute_execution_policy",
    "simulate_qqq_option_minute_execution",
]
