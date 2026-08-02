from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Annotated, Any, Literal, Self
from zoneinfo import ZoneInfo

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
    DQStatus,
    FillEventRecord,
    OrderEventRecord,
    OrderIntentRecord,
    PortfolioSnapshotRecord,
    QQQOptionsSafetyBoundary,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.minute_execution import (
    QQQOptionExecutionResult,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QQQ_OPTIONS_CASH_ACCOUNTING_POLICY_PATH = Path(
    "config/research/qqq_options_cash_premium_settlement_accounting_v1.yaml"
)

_UNKNOWN = "UNKNOWN_REQUIRES_POLICY_REVIEW"
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_SHARED_POLICY_SHA256 = "d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349"
_TRACKED_EXECUTION_POLICY_SHA256 = (
    "8c8823ddcc509e7dfdb81803a6fe7099b1ff44fccefc5a607c2a9abc7875226a"
)
_EXCHANGE_TIMEZONE = ZoneInfo("America/New_York")

AccountingScenarioRole = Literal[
    "REALITY_BASELINE",
    "SENSITIVITY",
    "SYNTHETIC_TEST_ONLY",
]
AccountingReason = Literal[
    "ACCOUNTING_POLICY_REVIEW_REQUIRED",
    "EXECUTION_BLOCKED_CASH_PRESERVED",
    "ACCOUNTING_REPLAY_READY",
    "INITIAL_CASH_POLICY_MISMATCH",
    "INSUFFICIENT_SETTLED_CASH",
    "PREMIUM_BUDGET_EXCEEDED",
    "MAX_CONTRACTS_EXCEEDED",
    "RESERVATION_MISMATCH",
    "FEE_BUFFER_EXCEEDED",
    "NEGATIVE_CASH_PROHIBITED",
    "SHORT_OPTION_PROHIBITED",
    "SETTLEMENT_CALENDAR_INVALID",
    "VALUATION_QUOTE_REQUIRED",
    "EXECUTION_IDENTITY_INVALID",
]
LedgerEntryType = Literal[
    "RESERVATION_CREATED",
    "BUY_FILL_SETTLED",
    "SELL_FILL_UNSETTLED",
    "RESERVATION_RELEASED",
    "SELL_PROCEEDS_SETTLED",
]


class QQQOptionCashAccountingContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _ReplayAbort(Exception):
    def __init__(self, reason: AccountingReason, message: str) -> None:
        self.reason = reason
        self.message = message
        super().__init__(message)


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


def _worst_status(statuses: list[DQStatus]) -> DQStatus:
    if "FAIL" in statuses:
        return "FAIL"
    if "NOT_EVALUATED" in statuses:
        return "NOT_EVALUATED"
    return "PASS"


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def content_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _content_sha256(_canonical_json_bytes(self.content_payload()))

    @model_validator(mode="after")
    def _validate_seal(self) -> Self:
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("content SHA-256 does not match canonical semantics")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QQQOptionCashAccountingContractError(
                "QQQ_OPTION_ACCOUNTING_HASH_CALLER_SUPPLIED",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        provisional = cls.model_construct(**payload, content_sha256=_UNSEALED_SHA256)
        return cls(**payload, content_sha256=provisional.compute_content_sha256())

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            record = cls.model_validate_json(content)
        except ValueError as exc:
            raise QQQOptionCashAccountingContractError(
                "QQQ_OPTION_ACCOUNTING_RECORD_INVALID", str(exc)
            ) from exc
        if content != record.canonical_bytes:
            raise QQQOptionCashAccountingContractError(
                "QQQ_OPTION_ACCOUNTING_RECORD_NOT_CANONICAL",
                "record bytes do not match canonical JSON encoding",
            )
        return record


class UnresolvedCashAccountingCriteria(_PolicyModel):
    mode: Literal["UNRESOLVED"]
    scenario_role: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    dq_caveat: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    approved_initial_cash_usd: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    premium_budget_usd: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    max_contracts_per_order: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    fee_buffer_per_contract_usd: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    sell_proceeds_settlement_lag_sessions: Literal[
        "UNKNOWN_REQUIRES_POLICY_REVIEW"
    ]
    max_valuation_quote_age_ms: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    cost_basis_method: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    include_fees_in_cost_basis: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    cash_quantum_usd: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    rounding_mode: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    reality_baseline: Literal[False]


class ActiveCashAccountingCriteria(_PolicyModel):
    mode: Literal["ACTIVE"]
    scenario_role: AccountingScenarioRole
    dq_caveat: str
    approved_initial_cash_usd: CanonicalDecimal
    premium_budget_usd: CanonicalDecimal
    max_contracts_per_order: int = Field(gt=0)
    fee_buffer_per_contract_usd: CanonicalDecimal
    sell_proceeds_settlement_lag_sessions: int = Field(ge=0)
    max_valuation_quote_age_ms: int = Field(gt=0)
    cost_basis_method: Literal["FIFO"]
    include_fees_in_cost_basis: bool
    cash_quantum_usd: CanonicalDecimal
    rounding_mode: Literal["ROUND_HALF_EVEN"]
    reality_baseline: bool

    @field_validator("dq_caveat")
    @classmethod
    def _validate_caveat(cls, value: str) -> str:
        return _required_text(value, "dq_caveat")

    @model_validator(mode="after")
    def _validate_active(self) -> Self:
        if self.approved_initial_cash_usd <= 0:
            raise ValueError("approved initial cash must be positive")
        if self.premium_budget_usd <= 0:
            raise ValueError("premium budget must be positive")
        if self.premium_budget_usd > self.approved_initial_cash_usd:
            raise ValueError("premium budget cannot exceed approved initial cash")
        if self.fee_buffer_per_contract_usd < 0:
            raise ValueError("fee buffer cannot be negative")
        if self.cash_quantum_usd <= 0:
            raise ValueError("cash quantum must be positive")
        if self.scenario_role == "REALITY_BASELINE" and not self.reality_baseline:
            raise ValueError("REALITY_BASELINE must declare reality_baseline=true")
        if self.scenario_role != "REALITY_BASELINE" and self.reality_baseline:
            raise ValueError("non-baseline scenarios cannot claim a reality baseline")
        return self


CashAccountingCriteria = Annotated[
    UnresolvedCashAccountingCriteria | ActiveCashAccountingCriteria,
    Field(discriminator="mode"),
]


class QQQOptionCashAccountingSafety(_PolicyModel):
    research_only: Literal[True]
    cash_account_only: Literal[True]
    long_premium_only: Literal[True]
    margin_allowed: Literal[False]
    negative_settled_cash_allowed: Literal[False]
    short_option_allowed: Literal[False]
    qqq_share_position_allowed: Literal[False]
    bid_liquidation_value_required: Literal[True]
    daily_close_valuation_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    external_order_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QQQOptionCashAccountingPolicy(_PolicyModel):
    schema_version: Literal[
        "qqq_options_cash_premium_settlement_accounting_policy.v1"
    ]
    policy_id: Literal["qqq_options_cash_premium_settlement_accounting_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED_BASELINE", "OWNER_REVIEWED_ACTIVE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    accounting_authorized: bool
    shared_contract_sha256: str
    shared_policy_sha256: str
    execution_policy_sha256: str
    primary_research_start: date
    approved_non_primary_authority_count: int
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    criteria: CashAccountingCriteria
    safety: QQQOptionCashAccountingSafety

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
        "execution_policy_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_authority(self) -> Self:
        if self.shared_contract_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
            raise ValueError("accounting policy must bind the exact shared contract")
        if self.shared_policy_sha256 != _SHARED_POLICY_SHA256:
            raise ValueError("accounting policy must bind the exact shared policy")
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.approved_non_primary_authority_count != 0:
            raise ValueError("no non-primary research-window authority is approved")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker drifted")
        active = isinstance(self.criteria, ActiveCashAccountingCriteria)
        if self.status == "OWNER_REVIEWED_ACTIVE":
            if not self.accounting_authorized or not active:
                raise ValueError("active accounting policy requires active criteria")
        elif self.accounting_authorized or active:
            raise ValueError("baseline accounting policy must remain unauthorized")
        if self.status == "OWNER_REVIEW_REQUIRED_BASELINE" and (
            self.execution_policy_sha256 != _TRACKED_EXECUTION_POLICY_SHA256
        ):
            raise ValueError("baseline must bind the exact blocked execution policy")
        return self


@dataclass(frozen=True)
class QQQOptionCashAccountingPolicyLoadResult:
    policy: QQQOptionCashAccountingPolicy
    policy_path: Path
    policy_sha256: str


class QQQOptionIntentAccountingInput(_StrictModel):
    intent_content_sha256: str
    contract_multiplier: int = Field(gt=0)
    source_id: str
    source_sha256: str

    @field_validator("intent_content_sha256", "source_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        return _identifier(value, "source_id")


class QQQOptionValuationQuoteInput(_StrictModel):
    option_sid: str
    source_id: str
    source_sha256: str
    quote_end_utc: datetime
    resolution: Literal["MINUTE"]
    bid_per_share: CanonicalDecimal
    ask_per_share: CanonicalDecimal

    @field_validator("option_sid")
    @classmethod
    def _validate_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        return _identifier(value, "source_id")

    @field_validator("source_sha256")
    @classmethod
    def _validate_source_hash(cls, value: str) -> str:
        return _sha256(value, "source_sha256")

    @field_validator("quote_end_utc")
    @classmethod
    def _validate_quote_time(cls, value: datetime) -> datetime:
        return _utc(value, "quote_end_utc")

    @model_validator(mode="after")
    def _validate_quote(self) -> Self:
        if self.bid_per_share < 0 or self.ask_per_share <= 0:
            raise ValueError("valuation prices are invalid")
        if self.ask_per_share < self.bid_per_share:
            raise ValueError("valuation quote cannot be crossed")
        return self

    def identity_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class QQQOptionCashAccountingRequest(_StrictModel):
    run_manifest_bytes: bytes
    run_manifest_file_sha256: str
    execution_result_bytes: tuple[bytes, ...]
    execution_result_file_sha256s: tuple[str, ...]
    intent_accounting_inputs: tuple[QQQOptionIntentAccountingInput, ...]
    snapshot_at_utc: datetime
    as_of_session: date
    exchange_sessions: tuple[date, ...]
    exchange_calendar_source_id: str
    exchange_calendar_source_sha256: str
    valuation_quotes: tuple[QQQOptionValuationQuoteInput, ...]
    producer_version: str
    lineage_id: str

    @field_validator("run_manifest_file_sha256", "exchange_calendar_source_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("execution_result_file_sha256s")
    @classmethod
    def _validate_result_hashes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_sha256(item, "execution_result_file_sha256s") for item in value)

    @field_validator("snapshot_at_utc")
    @classmethod
    def _validate_snapshot_time(cls, value: datetime) -> datetime:
        return _utc(value, "snapshot_at_utc")

    @field_validator(
        "exchange_calendar_source_id",
        "producer_version",
        "lineage_id",
    )
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_request(self) -> Self:
        if _content_sha256(self.run_manifest_bytes) != self.run_manifest_file_sha256:
            raise ValueError("run manifest file hash differs from bytes")
        manifest = RunManifestRecord.from_json_bytes(self.run_manifest_bytes)
        if self.snapshot_at_utc < manifest.created_at_utc:
            raise ValueError("snapshot cannot precede the run manifest")
        if not self.execution_result_bytes:
            raise ValueError("accounting replay requires execution result artifacts")
        if len(self.execution_result_bytes) != len(self.execution_result_file_sha256s):
            raise ValueError("execution result bytes and hashes must have equal length")
        results: list[QQQOptionExecutionResult] = []
        for content, expected_hash in zip(
            self.execution_result_bytes,
            self.execution_result_file_sha256s,
            strict=True,
        ):
            if _content_sha256(content) != expected_hash:
                raise ValueError("execution result file hash differs from bytes")
            results.append(QQQOptionExecutionResult.from_json_bytes(content))
        result_content_hashes = tuple(item.content_sha256 for item in results)
        if len(result_content_hashes) != len(set(result_content_hashes)):
            raise ValueError("execution result content identities must be unique")
        if len(self.execution_result_file_sha256s) != len(
            set(self.execution_result_file_sha256s)
        ):
            raise ValueError("execution result file identities must be unique")
        if not self.exchange_sessions:
            raise ValueError("reviewed exchange sessions cannot be empty")
        if self.exchange_sessions != tuple(sorted(self.exchange_sessions)):
            raise ValueError("exchange sessions must be sorted")
        if len(self.exchange_sessions) != len(set(self.exchange_sessions)):
            raise ValueError("exchange sessions must be unique")
        if any(item.weekday() >= 5 for item in self.exchange_sessions):
            raise ValueError("reviewed exchange sessions cannot include weekends")
        if self.as_of_session not in self.exchange_sessions:
            raise ValueError("as-of session must exist in the reviewed calendar")
        if _exchange_session(self.snapshot_at_utc) != self.as_of_session:
            raise ValueError("snapshot UTC must resolve to the declared as-of session")
        intent_hashes = tuple(
            item.intent_content_sha256 for item in self.intent_accounting_inputs
        )
        if len(intent_hashes) != len(set(intent_hashes)):
            raise ValueError("intent accounting identities must be unique")
        valuation_sids = tuple(item.option_sid for item in self.valuation_quotes)
        if len(valuation_sids) != len(set(valuation_sids)):
            raise ValueError("valuation option SIDs must be unique")
        valuation_sources = tuple(item.source_id for item in self.valuation_quotes)
        if len(valuation_sources) != len(set(valuation_sources)):
            raise ValueError("valuation source ids must be unique")
        return self

    @property
    def run_manifest(self) -> RunManifestRecord:
        return RunManifestRecord.from_json_bytes(self.run_manifest_bytes)

    @property
    def execution_results(self) -> tuple[QQQOptionExecutionResult, ...]:
        return tuple(
            QQQOptionExecutionResult.from_json_bytes(content)
            for content in self.execution_result_bytes
        )


class QQQOptionCashLedgerEntry(_SealedModel):
    schema_version: Literal["qqq_option_cash_ledger_entry.v1"]
    entry_sequence: int = Field(ge=0)
    entry_id: str
    entry_type: LedgerEntryType
    effective_at_utc: datetime
    effective_session: date
    option_sid: str | None
    source_record_sha256: str
    settled_cash_delta_usd: CanonicalDecimal
    unsettled_cash_delta_usd: CanonicalDecimal
    reserved_cash_delta_usd: CanonicalDecimal
    fee_delta_usd: CanonicalDecimal
    contracts_delta: int
    settled_cash_after_usd: CanonicalDecimal
    unsettled_cash_after_usd: CanonicalDecimal
    reserved_cash_after_usd: CanonicalDecimal
    open_contracts_after: int = Field(ge=0)

    @field_validator("entry_id")
    @classmethod
    def _validate_entry_id(cls, value: str) -> str:
        return _identifier(value, "entry_id")

    @field_validator("option_sid")
    @classmethod
    def _validate_optional_sid(cls, value: str | None) -> str | None:
        return None if value is None else _required_text(value, "option_sid")

    @field_validator("source_record_sha256")
    @classmethod
    def _validate_source_hash(cls, value: str) -> str:
        return _sha256(value, "source_record_sha256")

    @field_validator("effective_at_utc")
    @classmethod
    def _validate_effective_time(cls, value: datetime) -> datetime:
        return _utc(value, "effective_at_utc")

    @model_validator(mode="after")
    def _validate_balances(self) -> Self:
        if min(
            self.settled_cash_after_usd,
            self.unsettled_cash_after_usd,
            self.reserved_cash_after_usd,
        ) < 0:
            raise ValueError("ledger balances cannot be negative")
        if self.fee_delta_usd < 0:
            raise ValueError("fee delta cannot be negative")
        return self


class QQQOptionAccountingLot(_SealedModel):
    schema_version: Literal["qqq_option_accounting_lot.v1"]
    lot_id: str
    option_sid: str
    opened_at_utc: datetime
    contract_multiplier: int = Field(gt=0)
    contracts_open: int = Field(gt=0)
    remaining_cost_basis_usd: CanonicalDecimal
    source_fill_sha256: str

    @field_validator("lot_id")
    @classmethod
    def _validate_lot_id(cls, value: str) -> str:
        return _identifier(value, "lot_id")

    @field_validator("option_sid")
    @classmethod
    def _validate_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @field_validator("opened_at_utc")
    @classmethod
    def _validate_open_time(cls, value: datetime) -> datetime:
        return _utc(value, "opened_at_utc")

    @field_validator("source_fill_sha256")
    @classmethod
    def _validate_fill_hash(cls, value: str) -> str:
        return _sha256(value, "source_fill_sha256")

    @model_validator(mode="after")
    def _validate_cost(self) -> Self:
        if self.remaining_cost_basis_usd < 0:
            raise ValueError("remaining lot cost basis cannot be negative")
        return self


class QQQOptionAccountingPosition(_SealedModel):
    schema_version: Literal["qqq_option_accounting_position.v1"]
    option_sid: str
    contract_multiplier: int = Field(gt=0)
    contracts_open: int = Field(gt=0)
    remaining_cost_basis_usd: CanonicalDecimal
    liquidation_value_usd: CanonicalDecimal
    unrealized_pnl_usd: CanonicalDecimal
    valuation_quote_sha256: str
    lots: tuple[QQQOptionAccountingLot, ...]

    @field_validator("option_sid")
    @classmethod
    def _validate_sid(cls, value: str) -> str:
        return _required_text(value, "option_sid")

    @field_validator("valuation_quote_sha256")
    @classmethod
    def _validate_quote_hash(cls, value: str) -> str:
        return _sha256(value, "valuation_quote_sha256")

    @model_validator(mode="after")
    def _validate_position(self) -> Self:
        for lot in self.lots:
            QQQOptionAccountingLot.from_json_bytes(lot.canonical_bytes)
        if not self.lots:
            raise ValueError("open position requires at least one lot")
        if any(lot.option_sid != self.option_sid for lot in self.lots):
            raise ValueError("position lots must share the option SID")
        if any(lot.contract_multiplier != self.contract_multiplier for lot in self.lots):
            raise ValueError("position lots must share the contract multiplier")
        if sum(lot.contracts_open for lot in self.lots) != self.contracts_open:
            raise ValueError("position contract total differs from lots")
        if sum(lot.remaining_cost_basis_usd for lot in self.lots) != (
            self.remaining_cost_basis_usd
        ):
            raise ValueError("position cost basis differs from lots")
        if self.liquidation_value_usd < 0:
            raise ValueError("liquidation value cannot be negative")
        if self.unrealized_pnl_usd != (
            self.liquidation_value_usd - self.remaining_cost_basis_usd
        ):
            raise ValueError("unrealized PnL does not reconcile")
        return self


class QQQOptionCashAccountingResult(_SealedModel):
    schema_version: Literal["qqq_options_cash_accounting_result.v1"]
    policy_sha256: str
    execution_policy_sha256: str
    input_sha256: str
    accounting_authorized: bool
    investment_interpretation_allowed: bool
    reality_baseline: bool
    cash_preservation_required: bool
    reason_code: AccountingReason
    ledger_entries: tuple[QQQOptionCashLedgerEntry, ...]
    positions: tuple[QQQOptionAccountingPosition, ...]
    portfolio_snapshot: PortfolioSnapshotRecord | None

    @field_validator("policy_sha256", "execution_policy_sha256", "input_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_result(self) -> Self:
        for entry in self.ledger_entries:
            QQQOptionCashLedgerEntry.from_json_bytes(entry.canonical_bytes)
        for position in self.positions:
            QQQOptionAccountingPosition.from_json_bytes(position.canonical_bytes)
        if self.portfolio_snapshot is not None:
            PortfolioSnapshotRecord.from_json_bytes(self.portfolio_snapshot.canonical_bytes)
        ready = self.reason_code == "ACCOUNTING_REPLAY_READY"
        if ready != (self.portfolio_snapshot is not None):
            raise ValueError("ready accounting result must carry the shared snapshot")
        if not ready and (self.ledger_entries or self.positions):
            raise ValueError("blocked accounting result cannot expose partial state")
        if not self.accounting_authorized and ready:
            raise ValueError("unauthorized accounting cannot produce a ready snapshot")
        if self.investment_interpretation_allowed and not (
            self.accounting_authorized and self.reality_baseline and ready
        ):
            raise ValueError("investment interpretation requires an active reality baseline")
        if self.cash_preservation_required == ready:
            raise ValueError("cash preservation must be the inverse of ready replay")
        entry_sequences = tuple(item.entry_sequence for item in self.ledger_entries)
        if entry_sequences != tuple(range(len(entry_sequences))):
            raise ValueError("ledger entry sequences must be contiguous from zero")
        position_sids = tuple(item.option_sid for item in self.positions)
        if position_sids != tuple(sorted(position_sids)):
            raise ValueError("positions must be sorted by option SID")
        if len(position_sids) != len(set(position_sids)):
            raise ValueError("position option SIDs must be unique")
        if ready:
            if not self.ledger_entries or self.portfolio_snapshot is None:
                raise ValueError("ready accounting requires a non-empty ledger and snapshot")
            prior_settled = (
                self.ledger_entries[0].settled_cash_after_usd
                - self.ledger_entries[0].settled_cash_delta_usd
            )
            prior_unsettled = (
                self.ledger_entries[0].unsettled_cash_after_usd
                - self.ledger_entries[0].unsettled_cash_delta_usd
            )
            prior_reserved = (
                self.ledger_entries[0].reserved_cash_after_usd
                - self.ledger_entries[0].reserved_cash_delta_usd
            )
            if min(prior_settled, prior_unsettled, prior_reserved) < 0:
                raise ValueError("ledger cannot imply a negative opening cash balance")
            if prior_unsettled != 0 or prior_reserved != 0:
                raise ValueError("accounting replay must begin without unsettled or reserved cash")
            prior_time: datetime | None = None
            for entry in self.ledger_entries:
                if prior_time is not None and entry.effective_at_utc < prior_time:
                    raise ValueError("ledger effective chronology must be non-decreasing")
                if entry.settled_cash_after_usd != (
                    prior_settled + entry.settled_cash_delta_usd
                ):
                    raise ValueError("ledger settled cash recurrence does not reconcile")
                if entry.unsettled_cash_after_usd != (
                    prior_unsettled + entry.unsettled_cash_delta_usd
                ):
                    raise ValueError("ledger unsettled cash recurrence does not reconcile")
                if entry.reserved_cash_after_usd != (
                    prior_reserved + entry.reserved_cash_delta_usd
                ):
                    raise ValueError("ledger reserved cash recurrence does not reconcile")
                prior_settled = entry.settled_cash_after_usd
                prior_unsettled = entry.unsettled_cash_after_usd
                prior_reserved = entry.reserved_cash_after_usd
                prior_time = entry.effective_at_utc
            snapshot = self.portfolio_snapshot
            if (
                snapshot.settled_cash_usd != prior_settled
                or snapshot.unsettled_cash_usd != prior_unsettled
                or snapshot.reserved_cash_usd != prior_reserved
            ):
                raise ValueError("shared snapshot cash differs from terminal ledger balances")
            if snapshot.policy_sha256 != self.policy_sha256:
                raise ValueError("shared snapshot policy differs from accounting result")
            if snapshot.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256:
                raise ValueError("shared snapshot contract authority drifted")
            if snapshot.option_market_value_usd != sum(
                (item.liquidation_value_usd for item in self.positions), Decimal("0")
            ):
                raise ValueError("shared snapshot market value differs from positions")
            if snapshot.unrealized_pnl_usd != sum(
                (item.unrealized_pnl_usd for item in self.positions), Decimal("0")
            ):
                raise ValueError("shared snapshot unrealized PnL differs from positions")
            if snapshot.fees_paid_usd != sum(
                (item.fee_delta_usd for item in self.ledger_entries), Decimal("0")
            ):
                raise ValueError("shared snapshot fees differ from ledger entries")
            if sum(item.contracts_open for item in self.positions) != (
                self.ledger_entries[-1].open_contracts_after
            ):
                raise ValueError("terminal ledger quantity differs from positions")
        return self


@dataclass
class _LotState:
    lot_id: str
    option_sid: str
    opened_at_utc: datetime
    contract_multiplier: int
    contracts_open: int
    remaining_cost_basis_usd: Decimal
    source_fill_sha256: str


@dataclass(frozen=True)
class _PendingSettlement:
    due_session: date
    amount_usd: Decimal
    option_sid: str
    source_fill_sha256: str


def load_qqq_options_cash_accounting_policy(
    path: Path = DEFAULT_QQQ_OPTIONS_CASH_ACCOUNTING_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionCashAccountingPolicyLoadResult:
    resolved = path if path.is_absolute() else project_root / path
    try:
        content = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QQQOptionCashAccountingPolicy.model_validate(payload, strict=False)
    except (OSError, TypeError, ValueError) as exc:
        raise QQQOptionCashAccountingContractError(
            "QQQ_OPTION_CASH_ACCOUNTING_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QQQOptionCashAccountingPolicyLoadResult(
        policy=policy,
        policy_path=resolved,
        policy_sha256=_content_sha256(content),
    )


def build_qqq_option_cash_accounting_input_sha256(
    request: QQQOptionCashAccountingRequest,
) -> str:
    payload = {
        "schema_version": "qqq_options_cash_accounting_input.v1",
        "run_manifest_file_sha256": request.run_manifest_file_sha256,
        "execution_result_file_sha256s": sorted(
            request.execution_result_file_sha256s
        ),
        "intent_accounting_inputs": [
            item.model_dump(mode="json")
            for item in sorted(
                request.intent_accounting_inputs,
                key=lambda value: value.intent_content_sha256,
            )
        ],
        "snapshot_at_utc": request.snapshot_at_utc.isoformat(),
        "as_of_session": request.as_of_session.isoformat(),
        "exchange_sessions": [item.isoformat() for item in request.exchange_sessions],
        "exchange_calendar_source_id": request.exchange_calendar_source_id,
        "exchange_calendar_source_sha256": request.exchange_calendar_source_sha256,
        "valuation_quotes": [
            item.identity_payload()
            for item in sorted(request.valuation_quotes, key=lambda value: value.option_sid)
        ],
        "producer_version": request.producer_version,
        "lineage_id": request.lineage_id,
    }
    return _content_sha256(_canonical_json_bytes(payload))


def _blocked_result(
    *,
    policy_sha256: str,
    execution_policy_sha256: str,
    input_sha256: str,
    accounting_authorized: bool,
    reason_code: AccountingReason,
    reality_baseline: bool = False,
) -> QQQOptionCashAccountingResult:
    return QQQOptionCashAccountingResult.seal(
        schema_version="qqq_options_cash_accounting_result.v1",
        policy_sha256=policy_sha256,
        execution_policy_sha256=execution_policy_sha256,
        input_sha256=input_sha256,
        accounting_authorized=accounting_authorized,
        investment_interpretation_allowed=False,
        reality_baseline=reality_baseline,
        cash_preservation_required=True,
        reason_code=reason_code,
        ledger_entries=(),
        positions=(),
        portfolio_snapshot=None,
    )


def _result_sort_key(result: QQQOptionExecutionResult) -> tuple[str, str, str]:
    if result.order_intent is None:
        return ("9999-12-31T23:59:59+00:00", "zzzz", result.content_sha256)
    return (
        result.order_intent.created_at_utc.isoformat(),
        result.order_intent.intent_id,
        result.content_sha256,
    )


def _exchange_session(value: datetime) -> date:
    return value.astimezone(_EXCHANGE_TIMEZONE).date()


def _due_session(
    sessions: tuple[date, ...],
    fill_session: date,
    lag: int,
) -> date:
    try:
        index = sessions.index(fill_session)
    except ValueError as exc:
        raise _ReplayAbort(
            "SETTLEMENT_CALENDAR_INVALID",
            "fill session is absent from the reviewed exchange calendar",
        ) from exc
    due_index = index + lag
    if due_index >= len(sessions):
        raise _ReplayAbort(
            "SETTLEMENT_CALENDAR_INVALID",
            "reviewed exchange calendar does not cover the settlement due session",
        )
    return sessions[due_index]


def _validate_execution_result(
    result: QQQOptionExecutionResult,
    *,
    manifest: RunManifestRecord,
    execution_policy_sha256: str,
) -> None:
    if result.policy_sha256 != execution_policy_sha256:
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID",
            "execution result policy differs from the accounting policy lineage",
        )
    if result.accounting_status != "NOT_EVALUATED":
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID",
            "execution result cannot pre-assert accounting status",
        )
    records: list[OrderIntentRecord | OrderEventRecord | FillEventRecord] = []
    if result.order_intent is not None:
        records.append(result.order_intent)
    records.extend(result.order_events)
    records.extend(result.fill_events)
    for record in records:
        if (
            record.run_id != manifest.run_id
            or record.repository_code_sha != manifest.repository_code_sha
            or record.contract_schema_sha256 != QQQ_OPTIONS_CONTRACT_SHA256
            or record.requested_start != manifest.requested_start
            or record.requested_end != manifest.requested_end
            or record.evaluated_start != manifest.evaluated_start
            or record.evaluated_end != manifest.evaluated_end
        ):
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID",
                "execution record run/code/contract/range differs from the manifest",
            )
        if record.policy_sha256 != result.policy_sha256:
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID",
                "execution record policy differs from the sealed result",
            )
    if result.fill_events and (
        not result.execution_authorized or not result.selection_authorized
    ):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID",
            "filled result must preserve execution and selection authorization",
        )
    if result.order_intent is None:
        if result.order_events or result.fill_events:
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID", "order/fill records require an intent"
            )
        return
    intent = result.order_intent
    if len(result.order_events) < 3:
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID",
            "canonical execution requires created, submitted, and terminal events",
        )
    if (
        result.order_events[0].event_type != "CREATED"
        or result.order_events[0].event_at_utc != intent.created_at_utc
        or result.order_events[1].event_type != "SUBMITTED"
        or result.order_events[1].event_at_utc < intent.not_before_utc
    ):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID",
            "execution created/submitted chronology differs from 2486 authority",
        )
    event_times = tuple(item.event_at_utc for item in result.order_events)
    if event_times != tuple(sorted(event_times)):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID", "order event chronology is not monotone"
        )
    fill_times = tuple(item.fill_at_utc for item in result.fill_events)
    if fill_times != tuple(sorted(fill_times)):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID", "fill chronology is not monotone"
        )
    if any(
        item.event_type in {"FILLED", "CANCELED", "REJECTED"}
        for item in result.order_events[:-1]
    ):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID", "terminal order event is not final"
        )
    if any(event.side != intent.side for event in result.order_events):
        raise _ReplayAbort("EXECUTION_IDENTITY_INVALID", "order side drifted from intent")
    if any(event.order_contracts != intent.contracts for event in result.order_events):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID", "order quantity drifted from intent"
        )
    if any(
        event.limit_price_per_share != intent.limit_price_per_share
        for event in result.order_events
    ):
        raise _ReplayAbort("EXECUTION_IDENTITY_INVALID", "order limit drifted from intent")
    platform_ids = {item.platform_order_id for item in result.order_events}
    platform_ids.update(item.platform_order_id for item in result.fill_events)
    if len(platform_ids) > 1:
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID", "platform order identity is inconsistent"
        )
    if any(fill.side != intent.side for fill in result.fill_events):
        raise _ReplayAbort("EXECUTION_IDENTITY_INVALID", "fill side drifted from intent")
    if intent.side == "BUY_TO_OPEN" and any(
        fill.fill_price_per_share > intent.limit_price_per_share
        for fill in result.fill_events
    ):
        raise _ReplayAbort("EXECUTION_IDENTITY_INVALID", "buy fill exceeds limit")
    if intent.side == "SELL_TO_CLOSE" and any(
        fill.fill_price_per_share < intent.limit_price_per_share
        for fill in result.fill_events
    ):
        raise _ReplayAbort("EXECUTION_IDENTITY_INVALID", "sell fill is below limit")
    total_filled = sum(fill.filled_contracts for fill in result.fill_events)
    if total_filled > intent.contracts:
        raise _ReplayAbort("EXECUTION_IDENTITY_INVALID", "fills exceed intent quantity")
    if result.fill_events and not result.order_events:
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID", "fill records require order lifecycle events"
        )
    if result.order_events:
        if result.order_events[-1].filled_contracts_total != total_filled:
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID",
                "terminal cumulative fill differs from fill records",
            )
        terminal = result.order_events[-1].event_type
        if terminal not in {"FILLED", "CANCELED", "REJECTED"}:
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID", "order lifecycle lacks a terminal event"
            )
        if terminal == "FILLED" and total_filled != intent.contracts:
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID", "filled order is not completely filled"
            )
        expected_reason: str
        if terminal == "FILLED":
            expected_reason = "FILLED"
        elif terminal == "CANCELED":
            expected_reason = "PARTIAL_CANCELED" if total_filled else "NO_FILL_CANCELED"
        else:
            expected_reason = result.reason_code
            if expected_reason not in {"VENUE_REJECTED", "EXECUTION_DQ_REJECTED"}:
                raise _ReplayAbort(
                    "EXECUTION_IDENTITY_INVALID",
                    "rejected lifecycle has an incompatible result reason",
                )
        if result.reason_code != expected_reason:
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID",
                "terminal lifecycle and execution result reason differ",
            )
        cumulative_fills = 0
        for fill in result.fill_events:
            cumulative_fills += fill.filled_contracts
            matching = tuple(
                event
                for event in result.order_events
                if event.event_at_utc == fill.fill_at_utc
                and event.event_type in {"PARTIALLY_FILLED", "FILLED"}
                and event.filled_contracts_total == cumulative_fills
            )
            if len(matching) != 1:
                raise _ReplayAbort(
                    "EXECUTION_IDENTITY_INVALID",
                    "each fill must match one cumulative order lifecycle event",
                )


def _validate_global_execution_identities(
    results: tuple[QQQOptionExecutionResult, ...],
) -> None:
    record_identities: set[tuple[str, str]] = set()
    record_hashes: set[str] = set()
    platform_owners: dict[str, str] = {}
    for result in results:
        intent = result.order_intent
        records: tuple[OrderIntentRecord | OrderEventRecord | FillEventRecord, ...] = (
            (() if intent is None else (intent,))
            + result.order_events
            + result.fill_events
        )
        for record in records:
            identity = (record.schema_name, record.record_id)
            if identity in record_identities or record.content_sha256 in record_hashes:
                raise _ReplayAbort(
                    "EXECUTION_IDENTITY_INVALID",
                    "execution records contain duplicate semantic or content identity",
                )
            record_identities.add(identity)
            record_hashes.add(record.content_sha256)
        if intent is None:
            continue
        platform_ids = {item.platform_order_id for item in result.order_events}
        platform_ids.update(item.platform_order_id for item in result.fill_events)
        for platform_order_id in platform_ids:
            owner = platform_owners.setdefault(platform_order_id, intent.intent_id)
            if owner != intent.intent_id:
                raise _ReplayAbort(
                    "EXECUTION_IDENTITY_INVALID",
                    "one platform order identity is bound to multiple intents",
                )


def _build_sources(
    request: QQQOptionCashAccountingRequest,
    *,
    input_sha256: str,
) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = [
        ("qqq.options.accounting.calendar", request.exchange_calendar_source_sha256),
        ("qqq.options.accounting.input", input_sha256),
        ("qqq.options.accounting.run_manifest", request.run_manifest_file_sha256),
    ]
    pairs.extend(
        (f"qqq.options.accounting.execution.{index:04d}", value)
        for index, value in enumerate(sorted(request.execution_result_file_sha256s))
    )
    pairs.extend(
        (f"qqq.options.accounting.valuation.{index:04d}", quote.source_sha256)
        for index, quote in enumerate(
            sorted(request.valuation_quotes, key=lambda value: value.option_sid)
        )
    )
    ordered = tuple(sorted(pairs))
    if len({item[0] for item in ordered}) != len(ordered):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID", "accounting source ids must be unique"
        )
    return ordered


def _replay_active(
    request: QQQOptionCashAccountingRequest,
    *,
    policy: QQQOptionCashAccountingPolicy,
    policy_sha256: str,
    input_sha256: str,
) -> QQQOptionCashAccountingResult:
    criteria = policy.criteria
    if not isinstance(criteria, ActiveCashAccountingCriteria):
        raise QQQOptionCashAccountingContractError(
            "QQQ_OPTION_CASH_ACCOUNTING_ACTIVE_CRITERIA_MISSING",
            "authorized accounting policy lacks active criteria",
        )
    manifest = request.run_manifest
    if manifest.initial_cash_usd != criteria.approved_initial_cash_usd:
        raise _ReplayAbort(
            "INITIAL_CASH_POLICY_MISMATCH",
            "run initial cash differs from the approved accounting policy",
        )
    if manifest.requested_start != date(2021, 2, 22) or (
        manifest.evaluated_start != date(2021, 2, 22)
    ):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID",
            "primary accounting replay must start on 2021-02-22",
        )
    results = tuple(sorted(request.execution_results, key=_result_sort_key))
    for result in results:
        _validate_execution_result(
            result,
            manifest=manifest,
            execution_policy_sha256=policy.execution_policy_sha256,
        )
    _validate_global_execution_identities(results)
    intents = {
        item.intent_content_sha256: item for item in request.intent_accounting_inputs
    }
    required_intent_hashes = {
        result.order_intent.content_sha256
        for result in results
        if result.order_intent is not None
    }
    if required_intent_hashes != set(intents):
        raise _ReplayAbort(
            "EXECUTION_IDENTITY_INVALID",
            "intent multiplier/source identities must exactly cover execution intents",
        )
    if not any(result.fill_events for result in results):
        for result in results:
            intent = result.order_intent
            if intent is None:
                continue
            intent_input = intents[intent.content_sha256]
            if intent.contracts > criteria.max_contracts_per_order:
                raise _ReplayAbort("MAX_CONTRACTS_EXCEEDED", "intent exceeds max contracts")
            expected = Decimal("0")
            if intent.side == "BUY_TO_OPEN":
                expected = (
                    intent.limit_price_per_share
                    * intent_input.contract_multiplier
                    * intent.contracts
                    + criteria.fee_buffer_per_contract_usd * intent.contracts
                )
            if intent.reserved_cash_usd != expected:
                raise _ReplayAbort(
                    "RESERVATION_MISMATCH", "intent reservation is not policy-exact"
                )
            if expected > criteria.premium_budget_usd:
                raise _ReplayAbort(
                    "PREMIUM_BUDGET_EXCEEDED",
                    "reservation exceeds the approved premium budget",
                )
            if expected > manifest.initial_cash_usd:
                raise _ReplayAbort(
                    "INSUFFICIENT_SETTLED_CASH", "reservation exceeds settled cash"
                )
        return _blocked_result(
            policy_sha256=policy_sha256,
            execution_policy_sha256=policy.execution_policy_sha256,
            input_sha256=input_sha256,
            accounting_authorized=True,
            reason_code="EXECUTION_BLOCKED_CASH_PRESERVED",
            reality_baseline=criteria.reality_baseline,
        )

    settled = Decimal(manifest.initial_cash_usd)
    unsettled = Decimal("0")
    reserved = Decimal("0")
    fees_paid = Decimal("0")
    realized_pnl = Decimal("0")
    cumulative_buy_spend = Decimal("0")
    lots: dict[str, list[_LotState]] = {}
    pending: list[_PendingSettlement] = []
    entries: list[QQQOptionCashLedgerEntry] = []

    def open_contracts(option_sid: str | None = None) -> int:
        if option_sid is None:
            return sum(lot.contracts_open for values in lots.values() for lot in values)
        return sum(lot.contracts_open for lot in lots.get(option_sid, []))

    def append_entry(
        *,
        entry_type: LedgerEntryType,
        effective_at_utc: datetime,
        effective_session: date,
        option_sid: str | None,
        source_record_sha256: str,
        settled_delta: Decimal = Decimal("0"),
        unsettled_delta: Decimal = Decimal("0"),
        reserved_delta: Decimal = Decimal("0"),
        fee_delta: Decimal = Decimal("0"),
        contracts_delta: int = 0,
    ) -> None:
        entries.append(
            QQQOptionCashLedgerEntry.seal(
                schema_version="qqq_option_cash_ledger_entry.v1",
                entry_sequence=len(entries),
                entry_id=f"accounting.{len(entries):04d}",
                entry_type=entry_type,
                effective_at_utc=effective_at_utc,
                effective_session=effective_session,
                option_sid=option_sid,
                source_record_sha256=source_record_sha256,
                settled_cash_delta_usd=settled_delta,
                unsettled_cash_delta_usd=unsettled_delta,
                reserved_cash_delta_usd=reserved_delta,
                fee_delta_usd=fee_delta,
                contracts_delta=contracts_delta,
                settled_cash_after_usd=settled,
                unsettled_cash_after_usd=unsettled,
                reserved_cash_after_usd=reserved,
                open_contracts_after=open_contracts(),
            )
        )

    def settle_due(session: date, at_utc: datetime) -> None:
        nonlocal settled, unsettled, pending
        due_now = sorted(
            (item for item in pending if item.due_session <= session),
            key=lambda item: (item.due_session, item.source_fill_sha256),
        )
        if not due_now:
            return
        remaining = [item for item in pending if item.due_session > session]
        for item in due_now:
            settled += item.amount_usd
            unsettled -= item.amount_usd
            if unsettled < 0:
                raise _ReplayAbort(
                    "NEGATIVE_CASH_PROHIBITED", "settlement made unsettled cash negative"
                )
            append_entry(
                entry_type="SELL_PROCEEDS_SETTLED",
                effective_at_utc=at_utc,
                effective_session=session,
                option_sid=item.option_sid,
                source_record_sha256=item.source_fill_sha256,
                settled_delta=item.amount_usd,
                unsettled_delta=-item.amount_usd,
            )
        pending = remaining

    for result in results:
        intent = result.order_intent
        if intent is None:
            continue
        identity = intents[intent.content_sha256]
        multiplier = identity.contract_multiplier
        if intent.contracts > criteria.max_contracts_per_order:
            raise _ReplayAbort("MAX_CONTRACTS_EXCEEDED", "intent exceeds max contracts")
        if any(fill.contract_multiplier != multiplier for fill in result.fill_events):
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID", "actual fill multiplier differs from intent identity"
            )
        expected_reservation = Decimal("0")
        if intent.side == "BUY_TO_OPEN":
            expected_reservation = (
                intent.limit_price_per_share * multiplier * intent.contracts
                + criteria.fee_buffer_per_contract_usd * intent.contracts
            )
        if intent.reserved_cash_usd != expected_reservation:
            raise _ReplayAbort(
                "RESERVATION_MISMATCH", "intent reservation is not policy-exact"
            )
        if intent.side == "BUY_TO_OPEN" and (
            settled - reserved < expected_reservation
        ):
            raise _ReplayAbort(
                "INSUFFICIENT_SETTLED_CASH", "available settled cash cannot fund reservation"
            )
        intent_session = _exchange_session(intent.created_at_utc)
        if (
            intent_session not in request.exchange_sessions
            or intent_session > request.as_of_session
        ):
            raise _ReplayAbort(
                "SETTLEMENT_CALENDAR_INVALID",
                "intent session is absent from calendar or after accounting as-of",
            )
        for event in result.order_events:
            event_session = _exchange_session(event.event_at_utc)
            if (
                event_session not in request.exchange_sessions
                or event_session > request.as_of_session
            ):
                raise _ReplayAbort(
                    "SETTLEMENT_CALENDAR_INVALID",
                    "order event session is absent from calendar or after accounting as-of",
                )
        if intent.side == "BUY_TO_OPEN" and (
            cumulative_buy_spend + expected_reservation
            > criteria.premium_budget_usd
        ):
            raise _ReplayAbort(
                "PREMIUM_BUDGET_EXCEEDED",
                "reservation exceeds the remaining approved premium budget",
            )
        if not result.fill_events:
            continue
        if expected_reservation:
            reserved += expected_reservation
            append_entry(
                entry_type="RESERVATION_CREATED",
                effective_at_utc=intent.created_at_utc,
                effective_session=intent_session,
                option_sid=intent.option_sid,
                source_record_sha256=intent.content_sha256,
                reserved_delta=expected_reservation,
            )
        filled_contracts = 0
        for fill in result.fill_events:
            fill_session = _exchange_session(fill.fill_at_utc)
            if (
                fill_session not in request.exchange_sessions
                or fill_session > request.as_of_session
            ):
                raise _ReplayAbort(
                    "SETTLEMENT_CALENDAR_INVALID",
                    "fill session is absent from calendar or after accounting as-of",
                )
            settle_due(fill_session, fill.fill_at_utc)
            gross = fill.fill_price_per_share * multiplier * fill.filled_contracts
            expected_gross_delta = -gross if fill.side == "BUY_TO_OPEN" else gross
            if fill.gross_cash_delta_usd != expected_gross_delta:
                raise _ReplayAbort(
                    "EXECUTION_IDENTITY_INVALID", "fill gross cash identity drifted"
                )
            fee_limit = criteria.fee_buffer_per_contract_usd * fill.filled_contracts
            if fill.fee_usd > fee_limit:
                raise _ReplayAbort(
                    "FEE_BUFFER_EXCEEDED", "actual fill fee exceeds approved fee buffer"
                )
            fees_paid += fill.fee_usd
            filled_contracts += fill.filled_contracts
            if fill.side == "BUY_TO_OPEN":
                reservation_release = (
                    intent.limit_price_per_share * multiplier * fill.filled_contracts
                    + fee_limit
                )
                if reservation_release > reserved:
                    raise _ReplayAbort(
                        "RESERVATION_MISMATCH", "fill releases more cash than reserved"
                    )
                actual_debit = gross + fill.fee_usd
                cumulative_buy_spend += actual_debit
                if cumulative_buy_spend > criteria.premium_budget_usd:
                    raise _ReplayAbort(
                        "PREMIUM_BUDGET_EXCEEDED", "buy premium and fees exceed budget"
                    )
                if actual_debit > settled:
                    raise _ReplayAbort(
                        "INSUFFICIENT_SETTLED_CASH", "buy fill exceeds settled cash"
                    )
                reserved -= reservation_release
                settled -= actual_debit
                if settled < 0 or reserved < 0:
                    raise _ReplayAbort(
                        "NEGATIVE_CASH_PROHIBITED", "buy fill produced negative cash"
                    )
                lot_cost = gross + (
                    fill.fee_usd if criteria.include_fees_in_cost_basis else Decimal("0")
                )
                lots.setdefault(intent.option_sid, []).append(
                    _LotState(
                        lot_id=f"lot.{fill.content_sha256[:24]}",
                        option_sid=intent.option_sid,
                        opened_at_utc=fill.fill_at_utc,
                        contract_multiplier=multiplier,
                        contracts_open=fill.filled_contracts,
                        remaining_cost_basis_usd=lot_cost,
                        source_fill_sha256=fill.content_sha256,
                    )
                )
                append_entry(
                    entry_type="BUY_FILL_SETTLED",
                    effective_at_utc=fill.fill_at_utc,
                    effective_session=fill_session,
                    option_sid=intent.option_sid,
                    source_record_sha256=fill.content_sha256,
                    settled_delta=-actual_debit,
                    reserved_delta=-reservation_release,
                    fee_delta=fill.fee_usd,
                    contracts_delta=fill.filled_contracts,
                )
            else:
                if intent.reserved_cash_usd != 0:
                    raise _ReplayAbort(
                        "RESERVATION_MISMATCH", "sell-to-close cannot reserve premium cash"
                    )
                if open_contracts(intent.option_sid) < fill.filled_contracts:
                    raise _ReplayAbort(
                        "SHORT_OPTION_PROHIBITED", "sell fill exceeds open long contracts"
                    )
                contracts_to_close = fill.filled_contracts
                allocated_cost = Decimal("0")
                for lot in lots.get(intent.option_sid, []):
                    if contracts_to_close == 0:
                        break
                    take = min(contracts_to_close, lot.contracts_open)
                    if take == lot.contracts_open:
                        cost = lot.remaining_cost_basis_usd
                    else:
                        cost = (
                            lot.remaining_cost_basis_usd
                            * Decimal(take)
                            / Decimal(lot.contracts_open)
                        ).quantize(
                            criteria.cash_quantum_usd,
                            rounding=ROUND_HALF_EVEN,
                        )
                    lot.contracts_open -= take
                    lot.remaining_cost_basis_usd -= cost
                    allocated_cost += cost
                    contracts_to_close -= take
                lots[intent.option_sid] = [
                    lot for lot in lots.get(intent.option_sid, []) if lot.contracts_open > 0
                ]
                net_proceeds = gross - fill.fee_usd
                if net_proceeds < 0:
                    raise _ReplayAbort(
                        "NEGATIVE_CASH_PROHIBITED", "sell fee exceeds gross proceeds"
                    )
                due = _due_session(
                    request.exchange_sessions,
                    fill_session,
                    criteria.sell_proceeds_settlement_lag_sessions,
                )
                unsettled += net_proceeds
                pending.append(
                    _PendingSettlement(
                        due_session=due,
                        amount_usd=net_proceeds,
                        option_sid=intent.option_sid,
                        source_fill_sha256=fill.content_sha256,
                    )
                )
                realized_pnl += net_proceeds - allocated_cost
                append_entry(
                    entry_type="SELL_FILL_UNSETTLED",
                    effective_at_utc=fill.fill_at_utc,
                    effective_session=fill_session,
                    option_sid=intent.option_sid,
                    source_record_sha256=fill.content_sha256,
                    unsettled_delta=net_proceeds,
                    fee_delta=fill.fee_usd,
                    contracts_delta=-fill.filled_contracts,
                )
                settle_due(fill_session, fill.fill_at_utc)
        if intent.side == "BUY_TO_OPEN":
            terminal_release = expected_reservation - (
                intent.limit_price_per_share * multiplier * filled_contracts
                + criteria.fee_buffer_per_contract_usd * filled_contracts
            )
            if terminal_release:
                if terminal_release < 0 or terminal_release > reserved:
                    raise _ReplayAbort(
                        "RESERVATION_MISMATCH", "terminal reservation release is invalid"
                    )
                reserved -= terminal_release
                terminal_at = (
                    result.order_events[-1].event_at_utc
                    if result.order_events
                    else intent.created_at_utc
                )
                append_entry(
                    entry_type="RESERVATION_RELEASED",
                    effective_at_utc=terminal_at,
                    effective_session=_exchange_session(terminal_at),
                    option_sid=intent.option_sid,
                    source_record_sha256=intent.content_sha256,
                    reserved_delta=-terminal_release,
                )

    settle_due(request.as_of_session, request.snapshot_at_utc)
    if reserved != 0:
        raise _ReplayAbort(
            "RESERVATION_MISMATCH", "terminal accounting replay retained reserved cash"
        )
    if min(settled, unsettled, reserved) < 0:
        raise _ReplayAbort("NEGATIVE_CASH_PROHIBITED", "cash balance became negative")

    valuation_map = {item.option_sid: item for item in request.valuation_quotes}
    open_sids = {
        option_sid
        for option_sid, option_lots in lots.items()
        if any(lot.contracts_open > 0 for lot in option_lots)
    }
    if set(valuation_map) != open_sids:
        raise _ReplayAbort(
            "VALUATION_QUOTE_REQUIRED",
            "valuation quote SIDs must exactly cover the open option positions",
        )
    positions: list[QQQOptionAccountingPosition] = []
    option_market_value = Decimal("0")
    unrealized_pnl = Decimal("0")
    for option_sid in sorted(lots):
        active_lots = [lot for lot in lots[option_sid] if lot.contracts_open > 0]
        if not active_lots:
            continue
        quote = valuation_map.get(option_sid)
        if quote is None:
            raise _ReplayAbort(
                "VALUATION_QUOTE_REQUIRED", "open position lacks a bid valuation quote"
            )
        if quote.quote_end_utc > request.snapshot_at_utc:
            raise _ReplayAbort(
                "VALUATION_QUOTE_REQUIRED", "valuation quote is after snapshot time"
            )
        if request.snapshot_at_utc - quote.quote_end_utc > timedelta(
            milliseconds=criteria.max_valuation_quote_age_ms
        ):
            raise _ReplayAbort(
                "VALUATION_QUOTE_REQUIRED", "valuation quote exceeds approved freshness"
            )
        multipliers = {lot.contract_multiplier for lot in active_lots}
        if len(multipliers) != 1:
            raise _ReplayAbort(
                "EXECUTION_IDENTITY_INVALID", "one SID has conflicting contract multipliers"
            )
        multiplier = next(iter(multipliers))
        contracts_open = sum(lot.contracts_open for lot in active_lots)
        remaining_cost = sum(
            (lot.remaining_cost_basis_usd for lot in active_lots),
            Decimal("0"),
        )
        liquidation_value = quote.bid_per_share * multiplier * contracts_open
        quote_sha256 = _content_sha256(
            _canonical_json_bytes(quote.identity_payload())
        )
        sealed_lots = tuple(
            QQQOptionAccountingLot.seal(
                schema_version="qqq_option_accounting_lot.v1",
                lot_id=lot.lot_id,
                option_sid=lot.option_sid,
                opened_at_utc=lot.opened_at_utc,
                contract_multiplier=lot.contract_multiplier,
                contracts_open=lot.contracts_open,
                remaining_cost_basis_usd=lot.remaining_cost_basis_usd,
                source_fill_sha256=lot.source_fill_sha256,
            )
            for lot in active_lots
        )
        position = QQQOptionAccountingPosition.seal(
            schema_version="qqq_option_accounting_position.v1",
            option_sid=option_sid,
            contract_multiplier=multiplier,
            contracts_open=contracts_open,
            remaining_cost_basis_usd=remaining_cost,
            liquidation_value_usd=liquidation_value,
            unrealized_pnl_usd=liquidation_value - remaining_cost,
            valuation_quote_sha256=quote_sha256,
            lots=sealed_lots,
        )
        positions.append(position)
        option_market_value += liquidation_value
        unrealized_pnl += position.unrealized_pnl_usd

    sources = _build_sources(request, input_sha256=input_sha256)
    statuses = [manifest.dq_status]
    pit_statuses = [manifest.pit_status]
    for result in results:
        if result.order_intent is not None:
            statuses.append(result.order_intent.dq_status)
            pit_statuses.append(result.order_intent.pit_status)
        statuses.extend(event.dq_status for event in result.order_events)
        pit_statuses.extend(event.pit_status for event in result.order_events)
        statuses.extend(fill.dq_status for fill in result.fill_events)
        pit_statuses.extend(fill.pit_status for fill in result.fill_events)
        statuses.append(result.global_dq_status)
        pit_statuses.append(result.global_pit_status)
    snapshot = PortfolioSnapshotRecord.seal(
        schema_name="portfolio_snapshot",
        schema_version="1.0.0",
        run_id=manifest.run_id,
        record_id=f"portfolio-snapshot-{input_sha256[:24]}",
        created_at_utc=request.snapshot_at_utc,
        producer_version=request.producer_version,
        repository_code_sha=manifest.repository_code_sha,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        policy_sha256=policy_sha256,
        contract_schema_sha256=QQQ_OPTIONS_CONTRACT_SHA256,
        source_ids=tuple(item[0] for item in sources),
        source_checksums=tuple(item[1] for item in sources),
        requested_start=manifest.requested_start,
        requested_end=manifest.requested_end,
        evaluated_start=manifest.evaluated_start,
        evaluated_end=manifest.evaluated_end,
        storage_timezone="UTC",
        exchange_timezone="America/New_York",
        dq_status=_worst_status(statuses),
        pit_status=_worst_status(pit_statuses),
        export_classification="EXPORT_ALLOWED_DERIVED",
        lineage_id=request.lineage_id,
        safety=_shared_safety(),
        snapshot_at_utc=request.snapshot_at_utc,
        currency="USD",
        settled_cash_usd=settled,
        unsettled_cash_usd=unsettled,
        reserved_cash_usd=reserved,
        option_market_value_usd=option_market_value,
        fees_paid_usd=fees_paid,
        realized_pnl_usd=realized_pnl,
        unrealized_pnl_usd=unrealized_pnl,
    )
    return QQQOptionCashAccountingResult.seal(
        schema_version="qqq_options_cash_accounting_result.v1",
        policy_sha256=policy_sha256,
        execution_policy_sha256=policy.execution_policy_sha256,
        input_sha256=input_sha256,
        accounting_authorized=True,
        investment_interpretation_allowed=(
            criteria.scenario_role == "REALITY_BASELINE" and criteria.reality_baseline
        ),
        reality_baseline=criteria.reality_baseline,
        cash_preservation_required=False,
        reason_code="ACCOUNTING_REPLAY_READY",
        ledger_entries=tuple(entries),
        positions=tuple(positions),
        portfolio_snapshot=snapshot,
    )


def replay_qqq_option_cash_accounting(
    request: QQQOptionCashAccountingRequest,
    *,
    policy_path: Path = DEFAULT_QQQ_OPTIONS_CASH_ACCOUNTING_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionCashAccountingResult:
    loaded = load_qqq_options_cash_accounting_policy(
        policy_path,
        project_root=project_root,
    )
    policy = loaded.policy
    input_sha256 = build_qqq_option_cash_accounting_input_sha256(request)
    if not policy.accounting_authorized:
        return _blocked_result(
            policy_sha256=loaded.policy_sha256,
            execution_policy_sha256=policy.execution_policy_sha256,
            input_sha256=input_sha256,
            accounting_authorized=False,
            reason_code="ACCOUNTING_POLICY_REVIEW_REQUIRED",
        )
    try:
        return _replay_active(
            request,
            policy=policy,
            policy_sha256=loaded.policy_sha256,
            input_sha256=input_sha256,
        )
    except _ReplayAbort as exc:
        criteria = policy.criteria
        reality_baseline = (
            criteria.reality_baseline
            if isinstance(criteria, ActiveCashAccountingCriteria)
            else False
        )
        return _blocked_result(
            policy_sha256=loaded.policy_sha256,
            execution_policy_sha256=policy.execution_policy_sha256,
            input_sha256=input_sha256,
            accounting_authorized=True,
            reason_code=exc.reason,
            reality_baseline=reality_baseline,
        )


__all__ = [
    "ActiveCashAccountingCriteria",
    "DEFAULT_QQQ_OPTIONS_CASH_ACCOUNTING_POLICY_PATH",
    "QQQOptionAccountingLot",
    "QQQOptionAccountingPosition",
    "QQQOptionCashAccountingContractError",
    "QQQOptionCashAccountingPolicy",
    "QQQOptionCashAccountingPolicyLoadResult",
    "QQQOptionCashAccountingRequest",
    "QQQOptionCashAccountingResult",
    "QQQOptionCashAccountingSafety",
    "QQQOptionCashLedgerEntry",
    "QQQOptionIntentAccountingInput",
    "QQQOptionValuationQuoteInput",
    "UnresolvedCashAccountingCriteria",
    "build_qqq_option_cash_accounting_input_sha256",
    "load_qqq_options_cash_accounting_policy",
    "replay_qqq_option_cash_accounting",
]
