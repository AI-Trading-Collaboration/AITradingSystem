from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, ClassVar, Literal, Self

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    ValidationInfo,
    field_validator,
    model_validator,
)

DQStatus = Literal["PASS", "FAIL", "NOT_EVALUATED"]
PITStatus = Literal["PASS", "FAIL", "NOT_EVALUATED"]
ExportClassification = Literal[
    "QC_ONLY_NOT_EXPORTED",
    "EXPORT_ALLOWED_DERIVED",
    "UNKNOWN_REQUIRES_LICENSE_REVIEW",
    "EXPORT_PROHIBITED",
]
SignalDirection = Literal["LONG_CALL", "LONG_PUT", "FLAT"]
OptionRight = Literal["CALL", "PUT"]
OrderSide = Literal["BUY_TO_OPEN", "SELL_TO_CLOSE"]
OrderLifecycleState = Literal[
    "CREATED",
    "SUBMITTED",
    "UPDATED",
    "PARTIALLY_FILLED",
    "FILLED",
    "CANCELED",
    "REJECTED",
]
PositionLifecycleState = Literal[
    "FLAT",
    "INTENT_PENDING",
    "OPEN_PARTIAL",
    "OPEN",
    "EXIT_PENDING",
    "EXIT_BLOCKED",
    "CLOSED",
    "SCOPE_VIOLATION",
    "INVALID_RUN",
]
ReconciliationStatus = Literal[
    "PASS",
    "EXPLAINED_DIFFERENCE",
    "FAIL",
    "INCOMPLETE",
]
CapabilityStatus = Literal["CONFIRMED", "UNKNOWN", "CONTRADICTED"]

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_OBJECT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_UNSEALED_SHA256 = "0" * 64


class QQQOptionsContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or not _TEXT_PATTERN.fullmatch(value):
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
        raise ValueError(f"{field} must be a lowercase Git object SHA")
    return value


def _utc_datetime(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    if value.utcoffset().total_seconds() != 0:
        raise ValueError(f"{field} must use UTC offset")
    return value.astimezone(UTC)


def _parse_decimal(value: object) -> Decimal:
    if isinstance(value, bool) or isinstance(value, (int, float)):
        raise ValueError("decimal fields require Decimal or canonical decimal string")
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, str):
        if not value or value != value.strip():
            raise ValueError("decimal strings must be normalized")
        try:
            parsed = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError("invalid decimal string") from exc
    else:
        raise ValueError("decimal fields require Decimal or canonical decimal string")
    if not parsed.is_finite():
        raise ValueError("decimal fields must be finite")
    return parsed


def _decimal_json(value: Decimal) -> str:
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


CanonicalDecimal = Annotated[
    Decimal,
    BeforeValidator(_parse_decimal),
    PlainSerializer(_decimal_json, return_type=str, when_used="json"),
]


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


class QQQOptionsSafetyBoundary(_StrictModel):
    research_only: bool
    promotion_allowed: bool
    paper_shadow_allowed: bool
    production_allowed: bool
    raw_options_data_export_allowed: bool
    strategy_execution_allowed: bool
    bounded_cloud_pilot_authorized: bool
    production_effect: str
    broker_action: str

    @field_validator("production_effect", "broker_action")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_boundary(self) -> Self:
        if not self.research_only:
            raise ValueError("QQQ options shared contract must remain research-only")
        if any(
            (
                self.promotion_allowed,
                self.paper_shadow_allowed,
                self.production_allowed,
                self.raw_options_data_export_allowed,
                self.strategy_execution_allowed,
                self.bounded_cloud_pilot_authorized,
            )
        ):
            raise ValueError("shared contract cannot authorize export, execution, or promotion")
        if self.production_effect != "none" or self.broker_action != "none":
            raise ValueError("shared contract cannot create production or broker effects")
        return self


class ReasonCount(_StrictModel):
    reason_code: str
    count: int = Field(ge=0)

    @field_validator("reason_code")
    @classmethod
    def _validate_reason(cls, value: str) -> str:
        return _identifier(value, "reason_code")


class DQCheckResult(_StrictModel):
    check_id: str
    status: DQStatus
    reason_code: str | None
    observed_at_utc: datetime

    @field_validator("check_id")
    @classmethod
    def _validate_check_id(cls, value: str) -> str:
        return _identifier(value, "check_id")

    @field_validator("reason_code")
    @classmethod
    def _validate_reason(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "reason_code")

    @field_validator("observed_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "observed_at_utc")

    @model_validator(mode="after")
    def _validate_disposition(self) -> Self:
        if self.status == "PASS" and self.reason_code is not None:
            raise ValueError("passing DQ checks cannot carry a failure reason")
        if self.status != "PASS" and self.reason_code is None:
            raise ValueError("non-passing DQ checks require a reason code")
        return self


class EvidenceArtifact(_StrictModel):
    artifact_id: str
    locator: str
    sha256: str
    byte_count: int = Field(ge=0)
    export_classification: ExportClassification

    @field_validator("artifact_id")
    @classmethod
    def _validate_artifact_id(cls, value: str) -> str:
        return _identifier(value, "artifact_id")

    @field_validator("locator")
    @classmethod
    def _validate_locator(cls, value: str) -> str:
        return _required_text(value, "locator")

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "sha256")


class QQQOptionsRecordEnvelope(_StrictModel):
    """Common immutable envelope for every QQQ options research record."""

    schema_name: str
    schema_version: Literal["1.0.0"]
    run_id: str
    record_id: str
    created_at_utc: datetime
    producer_version: str
    repository_code_sha: str
    policy_id: str
    policy_version: str
    policy_sha256: str
    contract_schema_sha256: str
    source_ids: tuple[str, ...]
    source_checksums: tuple[str, ...]
    requested_start: date
    requested_end: date
    evaluated_start: date | None
    evaluated_end: date | None
    storage_timezone: Literal["UTC"]
    exchange_timezone: Literal["America/New_York"]
    dq_status: DQStatus
    pit_status: PITStatus
    export_classification: ExportClassification
    lineage_id: str
    content_sha256: str
    safety: QQQOptionsSafetyBoundary

    _schema_names: ClassVar[frozenset[str]] = frozenset()

    @field_validator(
        "schema_name",
        "run_id",
        "record_id",
        "producer_version",
        "policy_id",
        "policy_version",
        "lineage_id",
    )
    @classmethod
    def _validate_identifiers(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_object_sha(value, "repository_code_sha")

    @field_validator("policy_sha256", "contract_schema_sha256", "content_sha256")
    @classmethod
    def _validate_sha(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("created_at_utc")
    @classmethod
    def _validate_created_at(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "created_at_utc")

    @field_validator("source_ids")
    @classmethod
    def _validate_source_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(_identifier(item, "source_ids") for item in value)
        if not normalized or normalized != tuple(sorted(normalized)):
            raise ValueError("source_ids must be non-empty and sorted")
        if len(normalized) != len(set(normalized)):
            raise ValueError("source_ids must be unique")
        return normalized

    @field_validator("source_checksums")
    @classmethod
    def _validate_source_checksums(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("source_checksums must be non-empty")
        return tuple(_sha256(item, "source_checksums") for item in value)

    @model_validator(mode="after")
    def _validate_envelope(self, info: ValidationInfo) -> Self:
        if len(self.source_ids) != len(self.source_checksums):
            raise ValueError("source ids and checksums must have equal length")
        if self.requested_start > self.requested_end:
            raise ValueError("requested range is reversed")
        if (self.evaluated_start is None) != (self.evaluated_end is None):
            raise ValueError("evaluated range must be wholly present or absent")
        if self.evaluated_start is not None and self.evaluated_end is not None:
            if not (
                self.requested_start
                <= self.evaluated_start
                <= self.evaluated_end
                <= self.requested_end
            ):
                raise ValueError("evaluated range must be contained by requested range")
        allow_unsealed = bool(
            info.context and info.context.get("qqq_options_allow_unsealed") is True
        )
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("content SHA-256 does not match record semantics")
        return self

    def semantic_payload_without_hash(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return hashlib.sha256(
            _canonical_json_bytes(self.semantic_payload_without_hash())
        ).hexdigest()

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_CONTENT_HASH_CALLER_SUPPLIED",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        provisional = cls.model_validate(
            {**payload, "content_sha256": _UNSEALED_SHA256},
            context={"qqq_options_allow_unsealed": True},
        )
        return cls.model_validate(
            {**payload, "content_sha256": provisional.compute_content_sha256()}
        )

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            record = cls.model_validate_json(content)
        except ValueError as exc:
            raise QQQOptionsContractError("QQQ_OPTIONS_RECORD_INVALID", str(exc)) from exc
        if content != record.canonical_bytes:
            raise QQQOptionsContractError(
                "QQQ_OPTIONS_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical JSON encoding",
            )
        return record


class RunManifestRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["run_manifest"]
    underlying: Literal["QQQ"]
    initial_cash_usd: CanonicalDecimal
    account_currency: Literal["USD"]
    account_type: Literal["CASH"]
    signal_resolution: Literal["DAILY"]
    execution_resolution: Literal["MINUTE"]
    signal_artifact_sha256: str
    engine_identity_status: CapabilityStatus
    engine_identity: str | None
    evidence_admission_decision: Literal[
        "CAPABILITY_CONFIRMED_FOR_BOUNDED_PILOT",
        "CAPABILITY_OR_LICENSE_BLOCKED",
    ]

    @field_validator("signal_artifact_sha256")
    @classmethod
    def _validate_signal_hash(cls, value: str) -> str:
        return _sha256(value, "signal_artifact_sha256")

    @field_validator("engine_identity")
    @classmethod
    def _validate_engine_identity(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "engine_identity")

    @model_validator(mode="after")
    def _validate_manifest(self) -> Self:
        if self.initial_cash_usd <= 0:
            raise ValueError("initial cash must be positive")
        if self.engine_identity_status == "CONFIRMED" and self.engine_identity is None:
            raise ValueError("confirmed engine status requires engine identity")
        if self.engine_identity_status != "CONFIRMED" and self.engine_identity is not None:
            raise ValueError("unconfirmed engine status cannot assert an engine identity")
        return self


class DailySignalRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["daily_signal"]
    signal_session: date
    signal_as_of_utc: datetime
    generated_at_utc: datetime
    earliest_effective_session: date
    signal: SignalDirection
    signal_source_sha256: str

    @field_validator("signal_as_of_utc", "generated_at_utc")
    @classmethod
    def _validate_times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc_datetime(value, str(info.field_name))

    @field_validator("signal_source_sha256")
    @classmethod
    def _validate_source_hash(cls, value: str) -> str:
        return _sha256(value, "signal_source_sha256")

    @model_validator(mode="after")
    def _validate_signal_timeline(self) -> Self:
        if not (self.signal_as_of_utc <= self.generated_at_utc <= self.created_at_utc):
            raise ValueError("signal chronology is invalid")
        if self.earliest_effective_session <= self.signal_session:
            raise ValueError("daily signal cannot be effective in the source session")
        return self


class ContractCandidateSnapshotRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["contract_candidate_snapshot"]
    selection_snapshot_utc: datetime
    option_sid: str
    right: OptionRight
    expiry: date
    strike_usd_per_share: CanonicalDecimal
    contract_multiplier: int = Field(gt=0)
    dte: int = Field(ge=0)
    moneyness: CanonicalDecimal
    prior_day_model_as_of_session: date
    open_interest_as_of_session: date
    quote_bid_per_share: CanonicalDecimal | None
    quote_ask_per_share: CanonicalDecimal | None
    quote_end_utc: datetime | None
    quote_validity: Literal[
        "VALID",
        "MISSING",
        "STALE",
        "ZERO_ASK",
        "CROSSED",
    ]
    eligible: bool
    field_export_classification: ExportClassification

    @field_validator("selection_snapshot_utc")
    @classmethod
    def _validate_snapshot_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "selection_snapshot_utc")

    @field_validator("quote_end_utc")
    @classmethod
    def _validate_quote_time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _utc_datetime(value, "quote_end_utc")

    @field_validator("option_sid")
    @classmethod
    def _validate_option_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @model_validator(mode="after")
    def _validate_candidate(self) -> Self:
        if self.selection_snapshot_utc > self.created_at_utc:
            raise ValueError("candidate snapshot cannot occur after record creation")
        if self.strike_usd_per_share <= 0:
            raise ValueError("strike must be positive")
        if self.prior_day_model_as_of_session >= self.selection_snapshot_utc.date():
            raise ValueError("model fields must come from a prior session")
        if self.open_interest_as_of_session >= self.selection_snapshot_utc.date():
            raise ValueError("open interest must come from a prior session")
        quotes_present = (
            self.quote_bid_per_share is not None and self.quote_ask_per_share is not None
        )
        if quotes_present != (self.quote_end_utc is not None):
            raise ValueError("quote sides and quote timestamp must be present together")
        if (self.quote_bid_per_share is None) != (self.quote_ask_per_share is None):
            raise ValueError("bid and ask must be present together")
        if quotes_present:
            assert self.quote_bid_per_share is not None
            assert self.quote_ask_per_share is not None
            assert self.quote_end_utc is not None
            if self.quote_bid_per_share < 0 or self.quote_ask_per_share <= 0:
                raise ValueError("quote prices are invalid")
            if self.quote_ask_per_share < self.quote_bid_per_share:
                raise ValueError("crossed quotes are invalid")
            if self.quote_end_utc > self.selection_snapshot_utc:
                raise ValueError("candidate cannot use a future quote")
        if self.quote_validity == "VALID" and not quotes_present:
            raise ValueError("valid quote classification requires bid, ask, and quote timestamp")
        if self.eligible and self.quote_validity != "VALID":
            raise ValueError("an eligible candidate must have a valid quote")
        return self


class SelectionDecisionRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["selection_decision"]
    decision_id: str
    selection_snapshot_utc: datetime
    selected_option_sid: str | None
    no_contract_reason: str | None
    candidate_set_sha256: str
    stable_rank_components: tuple[str, ...]
    rejected_counts: tuple[ReasonCount, ...]

    @field_validator("decision_id")
    @classmethod
    def _validate_decision_id(cls, value: str) -> str:
        return _identifier(value, "decision_id")

    @field_validator("selection_snapshot_utc")
    @classmethod
    def _validate_snapshot_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "selection_snapshot_utc")

    @field_validator("selected_option_sid")
    @classmethod
    def _validate_selected_sid(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "selected_option_sid")

    @field_validator("no_contract_reason")
    @classmethod
    def _validate_no_contract_reason(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "no_contract_reason")

    @field_validator("candidate_set_sha256")
    @classmethod
    def _validate_candidate_hash(cls, value: str) -> str:
        return _sha256(value, "candidate_set_sha256")

    @field_validator("stable_rank_components")
    @classmethod
    def _validate_rank_components(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "stable_rank_components") for item in value)
        if not checked or len(checked) != len(set(checked)):
            raise ValueError("stable rank components must be non-empty and unique")
        return checked

    @model_validator(mode="after")
    def _validate_selection(self) -> Self:
        if self.decision_id != self.record_id:
            raise ValueError("decision id must equal record id")
        if (self.selected_option_sid is None) == (self.no_contract_reason is None):
            raise ValueError(
                "selection must contain exactly one selected SID or no-contract reason"
            )
        if self.selection_snapshot_utc > self.created_at_utc:
            raise ValueError("selection cannot occur after record creation")
        reasons = tuple(item.reason_code for item in self.rejected_counts)
        if reasons != tuple(sorted(reasons)) or len(reasons) != len(set(reasons)):
            raise ValueError("rejected counts must be sorted and unique")
        return self


class OrderIntentRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["order_intent"]
    intent_id: str
    decision_id: str
    option_sid: str
    side: OrderSide
    contracts: int = Field(gt=0)
    order_type: Literal["MARKETABLE_LIMIT"]
    limit_price_per_share: CanonicalDecimal
    reserved_cash_usd: CanonicalDecimal
    not_before_utc: datetime

    @field_validator("intent_id", "decision_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("option_sid")
    @classmethod
    def _validate_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @field_validator("not_before_utc")
    @classmethod
    def _validate_not_before(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "not_before_utc")

    @model_validator(mode="after")
    def _validate_intent(self) -> Self:
        if self.intent_id != self.record_id:
            raise ValueError("intent id must equal record id")
        if self.limit_price_per_share <= 0 or self.reserved_cash_usd < 0:
            raise ValueError("order price and reserved cash are invalid")
        if self.not_before_utc < self.created_at_utc:
            raise ValueError("not-before time cannot precede intent creation")
        return self


class OrderEventRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["order_event"]
    platform_order_id: str
    event_sequence: int = Field(ge=0)
    event_type: OrderLifecycleState
    event_at_utc: datetime
    side: OrderSide
    order_contracts: int = Field(gt=0)
    filled_contracts_total: int = Field(ge=0)
    limit_price_per_share: CanonicalDecimal
    reason_code: str | None

    @field_validator("platform_order_id")
    @classmethod
    def _validate_order_id(cls, value: str) -> str:
        return _identifier(value, "platform_order_id")

    @field_validator("event_at_utc")
    @classmethod
    def _validate_event_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "event_at_utc")

    @field_validator("reason_code")
    @classmethod
    def _validate_reason(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "reason_code")

    @model_validator(mode="after")
    def _validate_order_event(self) -> Self:
        if self.event_at_utc > self.created_at_utc:
            raise ValueError("order event cannot occur after record creation")
        if self.filled_contracts_total > self.order_contracts:
            raise ValueError("filled contracts exceed order quantity")
        if self.limit_price_per_share <= 0:
            raise ValueError("order limit must be positive")
        if self.event_type in {"REJECTED", "CANCELED"} and self.reason_code is None:
            raise ValueError("terminal non-fill order events require a reason")
        return self


class FillEventRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["fill_event"]
    platform_order_id: str
    fill_sequence: int = Field(gt=0)
    fill_at_utc: datetime
    quote_end_utc: datetime
    side: OrderSide
    filled_contracts: int = Field(gt=0)
    fill_price_per_share: CanonicalDecimal
    contract_multiplier: int = Field(gt=0)
    fee_usd: CanonicalDecimal
    settlement_currency: Literal["USD"]
    quote_side: Literal["ASK", "BID"]
    gross_cash_delta_usd: CanonicalDecimal

    @field_validator("platform_order_id")
    @classmethod
    def _validate_order_id(cls, value: str) -> str:
        return _identifier(value, "platform_order_id")

    @field_validator("fill_at_utc", "quote_end_utc")
    @classmethod
    def _validate_times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc_datetime(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_fill(self) -> Self:
        if not self.quote_end_utc < self.fill_at_utc <= self.created_at_utc:
            raise ValueError("fill chronology is invalid")
        if self.fill_price_per_share <= 0 or self.fee_usd < 0:
            raise ValueError("fill price and fee are invalid")
        expected_quote_side = "ASK" if self.side == "BUY_TO_OPEN" else "BID"
        if self.quote_side != expected_quote_side:
            raise ValueError("fill quote side does not match order side")
        gross = self.fill_price_per_share * self.contract_multiplier * self.filled_contracts
        expected_delta = -gross if self.side == "BUY_TO_OPEN" else gross
        if self.gross_cash_delta_usd != expected_delta:
            raise ValueError("gross cash delta does not match price, multiplier, and quantity")
        return self


_ALLOWED_POSITION_TRANSITIONS: dict[str, frozenset[str]] = {
    "FLAT": frozenset({"INTENT_PENDING"}),
    "INTENT_PENDING": frozenset({"OPEN_PARTIAL", "OPEN", "FLAT"}),
    "OPEN_PARTIAL": frozenset({"OPEN", "EXIT_PENDING"}),
    "OPEN": frozenset({"EXIT_PENDING", "SCOPE_VIOLATION"}),
    "EXIT_PENDING": frozenset({"CLOSED", "EXIT_BLOCKED"}),
    "EXIT_BLOCKED": frozenset({"CLOSED", "INVALID_RUN"}),
    "SCOPE_VIOLATION": frozenset({"INVALID_RUN"}),
    "CLOSED": frozenset({"FLAT"}),
    "INVALID_RUN": frozenset(),
}


class PositionLifecycleEventRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["position_lifecycle_event"]
    position_id: str
    event_sequence: int = Field(ge=0)
    occurred_at_utc: datetime
    prior_state: PositionLifecycleState
    next_state: PositionLifecycleState
    quantity_delta_contracts: int
    cash_delta_usd: CanonicalDecimal
    reason_code: str

    @field_validator("position_id", "reason_code")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("occurred_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "occurred_at_utc")

    @model_validator(mode="after")
    def _validate_transition(self) -> Self:
        if self.occurred_at_utc > self.created_at_utc:
            raise ValueError("lifecycle event cannot occur after record creation")
        if self.next_state not in _ALLOWED_POSITION_TRANSITIONS[self.prior_state]:
            raise ValueError("illegal position lifecycle transition")
        return self


class PortfolioSnapshotRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["portfolio_snapshot"]
    snapshot_at_utc: datetime
    currency: Literal["USD"]
    settled_cash_usd: CanonicalDecimal
    unsettled_cash_usd: CanonicalDecimal
    reserved_cash_usd: CanonicalDecimal
    option_market_value_usd: CanonicalDecimal
    fees_paid_usd: CanonicalDecimal
    realized_pnl_usd: CanonicalDecimal
    unrealized_pnl_usd: CanonicalDecimal

    @field_validator("snapshot_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "snapshot_at_utc")

    @model_validator(mode="after")
    def _validate_snapshot(self) -> Self:
        if self.snapshot_at_utc > self.created_at_utc:
            raise ValueError("portfolio snapshot cannot occur after record creation")
        if (
            min(
                self.settled_cash_usd,
                self.unsettled_cash_usd,
                self.reserved_cash_usd,
                self.option_market_value_usd,
                self.fees_paid_usd,
            )
            < 0
        ):
            raise ValueError("cash, market value, and fees cannot be negative")
        return self


class DQReportRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["dq_report"]
    scope: str
    report_version: str
    generated_at_utc: datetime
    checks: tuple[DQCheckResult, ...]

    @field_validator("scope", "report_version")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("generated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "generated_at_utc")

    @model_validator(mode="after")
    def _validate_report(self) -> Self:
        if self.generated_at_utc > self.created_at_utc:
            raise ValueError("DQ report generation cannot occur after record creation")
        check_ids = tuple(item.check_id for item in self.checks)
        if not check_ids or check_ids != tuple(sorted(check_ids)):
            raise ValueError("DQ checks must be non-empty and sorted")
        if len(check_ids) != len(set(check_ids)):
            raise ValueError("DQ check ids must be unique")
        if any(item.observed_at_utc > self.generated_at_utc for item in self.checks):
            raise ValueError("DQ checks cannot observe future state")
        if self.dq_status == "PASS" and any(item.status != "PASS" for item in self.checks):
            raise ValueError("passing DQ report requires every check to pass")
        return self


class PlatformEvidenceManifestRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["platform_evidence_manifest"]
    bundle_id: str
    platform: Literal["QuantConnect"]
    backtest_id: str | None
    tier_status: CapabilityStatus
    engine_identity_status: CapabilityStatus
    collected_at_utc: datetime
    collected_by: str
    artifacts: tuple[EvidenceArtifact, ...]
    limitations: tuple[str, ...]
    raw_option_rows_included: bool
    account_or_broker_identifiers_included: bool

    @field_validator("bundle_id", "collected_by")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("backtest_id")
    @classmethod
    def _validate_backtest_id(cls, value: str | None) -> str | None:
        return None if value is None else _identifier(value, "backtest_id")

    @field_validator("collected_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "collected_at_utc")

    @field_validator("limitations")
    @classmethod
    def _validate_limitations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_required_text(item, "limitations") for item in value)
        if checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError("limitations must be sorted and unique")
        return checked

    @model_validator(mode="after")
    def _validate_evidence(self) -> Self:
        if self.bundle_id != self.record_id:
            raise ValueError("bundle id must equal record id")
        if self.collected_at_utc > self.created_at_utc:
            raise ValueError("evidence collection cannot occur after record creation")
        if self.raw_option_rows_included or self.account_or_broker_identifiers_included:
            raise ValueError("evidence manifest contains prohibited content")
        artifact_ids = tuple(item.artifact_id for item in self.artifacts)
        if artifact_ids != tuple(sorted(artifact_ids)) or len(artifact_ids) != len(
            set(artifact_ids)
        ):
            raise ValueError("evidence artifacts must be sorted and unique")
        return self


class ReconciliationReportRecord(QQQOptionsRecordEnvelope):
    schema_name: Literal["reconciliation_report"]
    check_id: str
    status: ReconciliationStatus
    difference_class: str
    local_value: CanonicalDecimal | None
    platform_value: CanonicalDecimal | None
    delta: CanonicalDecimal | None
    unit: str
    tolerance_policy_id: str
    tolerance_policy_version: str
    tolerance_policy_sha256: str
    explanation: str
    evaluated_at_utc: datetime

    @field_validator(
        "check_id",
        "difference_class",
        "unit",
        "tolerance_policy_id",
        "tolerance_policy_version",
    )
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("tolerance_policy_sha256")
    @classmethod
    def _validate_tolerance_hash(cls, value: str) -> str:
        return _sha256(value, "tolerance_policy_sha256")

    @field_validator("explanation")
    @classmethod
    def _validate_explanation(cls, value: str) -> str:
        return _required_text(value, "explanation")

    @field_validator("evaluated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc_datetime(value, "evaluated_at_utc")

    @model_validator(mode="after")
    def _validate_reconciliation(self) -> Self:
        if self.check_id != self.record_id:
            raise ValueError("check id must equal record id")
        if self.evaluated_at_utc > self.created_at_utc:
            raise ValueError("reconciliation cannot occur after record creation")
        present = tuple(
            value is not None for value in (self.local_value, self.platform_value, self.delta)
        )
        if len(set(present)) != 1:
            raise ValueError("local, platform, and delta values must be jointly present or absent")
        if self.local_value is not None:
            assert self.platform_value is not None
            assert self.delta is not None
            if self.delta != self.local_value - self.platform_value:
                raise ValueError("reconciliation delta does not match values")
        if self.status != "INCOMPLETE" and self.local_value is None:
            raise ValueError("complete reconciliation status requires numeric values")
        return self


QQQ_OPTIONS_RECORD_TYPES = (
    RunManifestRecord,
    DailySignalRecord,
    ContractCandidateSnapshotRecord,
    SelectionDecisionRecord,
    OrderIntentRecord,
    OrderEventRecord,
    FillEventRecord,
    PositionLifecycleEventRecord,
    PortfolioSnapshotRecord,
    DQReportRecord,
    PlatformEvidenceManifestRecord,
    ReconciliationReportRecord,
)

QQQ_OPTIONS_SCHEMA_NAMES = tuple(
    model.model_fields["schema_name"].annotation.__args__[0]  # type: ignore[union-attr]
    for model in QQQ_OPTIONS_RECORD_TYPES
)


def canonical_qqq_options_contract_schema_bytes() -> bytes:
    schema_by_name = {
        schema_name: model.model_json_schema(mode="validation")
        for model, schema_name in zip(
            QQQ_OPTIONS_RECORD_TYPES, QQQ_OPTIONS_SCHEMA_NAMES, strict=True
        )
    }
    return _canonical_json_bytes(
        {
            "schema_version": "qqq_options_shared_record_contract.v1",
            "records": schema_by_name,
        }
    )


QQQ_OPTIONS_CONTRACT_SHA256 = hashlib.sha256(
    canonical_qqq_options_contract_schema_bytes()
).hexdigest()


__all__ = [
    "CapabilityStatus",
    "CanonicalDecimal",
    "ContractCandidateSnapshotRecord",
    "DQCheckResult",
    "DQReportRecord",
    "DQStatus",
    "DailySignalRecord",
    "EvidenceArtifact",
    "ExportClassification",
    "FillEventRecord",
    "OptionRight",
    "OrderEventRecord",
    "OrderIntentRecord",
    "OrderLifecycleState",
    "OrderSide",
    "PITStatus",
    "PlatformEvidenceManifestRecord",
    "PortfolioSnapshotRecord",
    "PositionLifecycleEventRecord",
    "PositionLifecycleState",
    "QQQOptionsContractError",
    "QQQOptionsRecordEnvelope",
    "QQQOptionsSafetyBoundary",
    "QQQ_OPTIONS_CONTRACT_SHA256",
    "QQQ_OPTIONS_RECORD_TYPES",
    "QQQ_OPTIONS_SCHEMA_NAMES",
    "ReasonCount",
    "ReconciliationReportRecord",
    "ReconciliationStatus",
    "RunManifestRecord",
    "SelectionDecisionRecord",
    "SignalDirection",
    "canonical_qqq_options_contract_schema_bytes",
]
