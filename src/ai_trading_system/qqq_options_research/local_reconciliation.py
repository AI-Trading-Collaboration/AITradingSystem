from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Annotated, Any, Literal, Self, cast, overload

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

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_research.cash_accounting import (
    QQQOptionCashAccountingResult,
)
from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    DQReportRecord,
    ReconciliationReportRecord,
    RunManifestRecord,
)
from ai_trading_system.qqq_options_research.minute_execution import (
    QQQOptionExecutionResult,
)
from ai_trading_system.qqq_options_research.platform_evidence_bundle import (
    DEFAULT_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_BUNDLE_POLICY_PATH,
    LoadedQCPlatformEvidenceBundle,
    QCPlatformEvidenceBundleContractError,
    load_qc_qqq_options_manual_evidence_bundle,
)
from ai_trading_system.qqq_options_research.position_lifecycle import (
    QQQOptionPositionLifecycleResult,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_LOCAL_RECONCILIATION_POLICY_PATH = Path(
    "config/research/qc_qqq_options_local_ingest_reconciliation_v1.yaml"
)

_SHARED_POLICY_PATH = Path("config/research/qqq_options_shared_contract_v1.yaml")
_DQ_PIT_POLICY_PATH = Path("config/research/qqq_options_dq_pit_identity_v1.yaml")
_SIGNAL_POLICY_PATH = Path("config/research/qqq_options_signal_export_v1.yaml")
_ADAPTER_POLICY_PATH = Path("config/research/qc_qqq_options_project_adapter_contract_v1.yaml")
_SELECTION_POLICY_PATH = Path("config/research/qqq_options_deterministic_selection_v1.yaml")
_EXECUTION_POLICY_PATH = Path("config/research/qqq_options_minute_execution_reality_v1.yaml")
_ACCOUNTING_POLICY_PATH = Path(
    "config/research/qqq_options_cash_premium_settlement_accounting_v1.yaml"
)
_LIFECYCLE_POLICY_PATH = Path(
    "config/research/qqq_options_lifecycle_expiry_corporate_action_safety_v1.yaml"
)
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_NOT_GRANTED = "NOT_GRANTED_FOR_RECONCILIATION_POLICY"

QCReconciliationDifferenceClass = Literal[
    "LOGIC",
    "PLATFORM",
    "PROVIDER",
    "TIMING",
    "REALITY_MODEL",
    "LICENSE",
    "MANUAL_COLLECTION",
]
QCReconciliationDisposition = Literal[
    "ACCEPTED_EXPLAINED", "REQUIRES_FIX", "BLOCKED_EVIDENCE", "INVALID_RUN"
]
QCReconciliationOutcome = Literal[
    "LOCAL_RECONCILIATION_POLICY_BLOCKED",
    "LOCAL_RECONCILIATION_INPUT_INVALID",
    "LOCAL_RECONCILIATION_INCOMPLETE",
    "LOCAL_RECONCILIATION_REQUIRES_FIX",
    "LOCAL_RECONCILIATION_READY_FOR_OWNER_REVIEW",
]

_DIFFERENCE_CLASSES = (
    "LICENSE",
    "LOGIC",
    "MANUAL_COLLECTION",
    "PLATFORM",
    "PROVIDER",
    "REALITY_MODEL",
    "TIMING",
)
_DISPOSITIONS = (
    "ACCEPTED_EXPLAINED",
    "BLOCKED_EVIDENCE",
    "INVALID_RUN",
    "REQUIRES_FIX",
)
_EXACT_FIELDS = (
    "evaluated_range",
    "fill_identity",
    "lineage",
    "order_identity",
    "order_side",
    "order_state",
    "option_sid",
    "quantity",
    "repository_code_sha",
    "requested_range",
    "source_checksum",
    "underlying",
)
_ORDER_CANONICAL_FIELDS = (
    "event_at_utc",
    "event_sequence",
    "order_contracts",
    "order_id",
    "order_state",
    "option_sid",
    "side",
    "underlying",
    "filled_contracts_total",
    "limit_price_per_share",
)
_TRADE_CANONICAL_FIELDS = (
    "contract_multiplier",
    "fee_usd",
    "fill_at_utc",
    "fill_id",
    "fill_price_per_share",
    "filled_contracts",
    "gross_cash_delta_usd",
    "order_id",
    "option_sid",
    "side",
)


class QCLocalReconciliationContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field_name: str) -> str:
    if not value or value.strip() != value or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field_name} must be non-empty normalized text")
    return value


def _identifier(value: str, field_name: str) -> str:
    value = _required_text(value, field_name)
    if not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be a portable identifier")
    return value


def _sha256(value: str, field_name: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be lowercase SHA-256")
    return value


def _git_sha(value: str, field_name: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field_name} must be a lowercase Git object id")
    return value


def _utc(value: datetime, field_name: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _parse_decimal(value: object) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("boolean is not a decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("value must be a canonical decimal") from exc
    if not result.is_finite():
        raise ValueError("decimal must be finite")
    return result


def _decimal_json(value: Decimal) -> str:
    return format(value, "f")


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
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


def _content_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    @model_validator(mode="after")
    def _validate_seal(self, info: ValidationInfo) -> Self:
        allow_unsealed = bool(info.context and info.context.get("allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("content SHA-256 does not match canonical semantics")
        return self

    def semantic_payload_without_hash(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _content_sha256(_canonical_json_bytes(self.semantic_payload_without_hash()))

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QCLocalReconciliationContractError(
                "CALLER_SUPPLIED_CONTENT_SHA256", "content_sha256 is derived"
            )
        provisional = cls.model_validate(
            {**payload, "content_sha256": _UNSEALED_SHA256},
            context={"allow_unsealed": True},
        )
        return cls.model_validate(
            {**payload, "content_sha256": provisional.compute_content_sha256()}
        )

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return _content_sha256(self.canonical_bytes)

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            record = cls.model_validate_json(content)
        except ValueError as exc:
            raise QCLocalReconciliationContractError(
                "LOCAL_RECONCILIATION_RECORD_INVALID", str(exc)
            ) from exc
        if content != record.canonical_bytes:
            raise QCLocalReconciliationContractError(
                "LOCAL_RECONCILIATION_RECORD_NONCANONICAL",
                f"{cls.__name__} bytes are not canonical",
            )
        return record


class QCPlatformColumnMapping(_PolicyModel):
    canonical_field: str
    source_column: str

    @field_validator("canonical_field")
    @classmethod
    def _validate_canonical_field(cls, value: str) -> str:
        return _identifier(value, "canonical_field")

    @field_validator("source_column")
    @classmethod
    def _validate_source_column(cls, value: str) -> str:
        return _required_text(value, "source_column")


class UnresolvedLocalReconciliationCriteria(_PolicyModel):
    mode: Literal["UNRESOLVED"]
    ingest_profile_status: Literal["UNKNOWN_REQUIRES_PLATFORM_EVIDENCE"]
    tolerance_policy_status: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    results_projection_key: Literal["UNKNOWN_REQUIRES_PLATFORM_EVIDENCE"]
    orders_csv_column_mapping: tuple[()]
    trades_csv_column_mapping: tuple[()]
    monetary_absolute_tolerance_usd: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    metric_absolute_tolerance: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    timing_absolute_tolerance_seconds: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    decimal_rounding_policy: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]
    reviewed_authority_id: Literal["UNKNOWN_REQUIRES_POLICY_REVIEW"]


class ActiveLocalReconciliationCriteria(_PolicyModel):
    mode: Literal["ACTIVE"]
    ingest_profile_status: Literal["OWNER_REVIEWED_ACTIVE"]
    tolerance_policy_status: Literal["OWNER_REVIEWED_ACTIVE"]
    results_projection_key: str
    orders_csv_column_mapping: tuple[QCPlatformColumnMapping, ...]
    trades_csv_column_mapping: tuple[QCPlatformColumnMapping, ...]
    monetary_absolute_tolerance_usd: CanonicalDecimal
    metric_absolute_tolerance: CanonicalDecimal
    timing_absolute_tolerance_seconds: CanonicalDecimal
    decimal_rounding_policy: Literal["NO_ROUNDING_EXACT_DECIMAL"]
    reviewed_authority_id: str

    @field_validator("results_projection_key", "reviewed_authority_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_mapping(self) -> Self:
        order_fields = tuple(item.canonical_field for item in self.orders_csv_column_mapping)
        trade_fields = tuple(item.canonical_field for item in self.trades_csv_column_mapping)
        if order_fields != _ORDER_CANONICAL_FIELDS:
            raise ValueError("orders CSV canonical mapping must be complete, sorted, and exact")
        if trade_fields != _TRADE_CANONICAL_FIELDS:
            raise ValueError("trades CSV canonical mapping must be complete, sorted, and exact")
        for mappings in (self.orders_csv_column_mapping, self.trades_csv_column_mapping):
            columns = tuple(item.source_column for item in mappings)
            if len(columns) != len(set(columns)):
                raise ValueError("CSV source columns must be unique")
        tolerances = (
            self.monetary_absolute_tolerance_usd,
            self.metric_absolute_tolerance,
            self.timing_absolute_tolerance_seconds,
        )
        if any(value < 0 for value in tolerances):
            raise ValueError("reviewed tolerances cannot be negative")
        return self


class QCLocalReconciliationSafety(_PolicyModel):
    research_only: Literal[True]
    external_platform_action_allowed: Literal[False]
    cloud_run_authorized: Literal[False]
    api_allowed: Literal[False]
    cli_allowed: Literal[False]
    remote_http_allowed: Literal[False]
    object_store_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    raw_option_chain_reconstruction_allowed: Literal[False]
    raw_minute_quote_reconstruction_allowed: Literal[False]
    raw_open_interest_reconstruction_allowed: Literal[False]
    paper_shadow_allowed: Literal[False]
    production_allowed: Literal[False]
    promotion_allowed: Literal[False]
    strategy_execution_allowed: Literal[False]
    external_pass_may_override_internal_failure: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCLocalReconciliationPolicy(_PolicyModel):
    schema_version: Literal["qc_qqq_options_local_ingest_reconciliation_policy.v1"]
    policy_id: Literal["qc_qqq_options_local_ingest_reconciliation_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEW_REQUIRED_BASELINE", "OWNER_REVIEWED_ACTIVE"]
    owner: str
    owner_decision: str
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    reconciliation_authorized: bool
    owner_authorization_status: str
    shared_contract_sha256: str
    shared_policy_sha256: str
    dq_pit_policy_sha256: str
    signal_export_policy_sha256: str
    adapter_policy_sha256: str
    selection_policy_sha256: str
    execution_policy_sha256: str
    accounting_policy_sha256: str
    lifecycle_policy_sha256: str
    platform_evidence_policy_sha256: str
    primary_research_start: date
    approved_non_primary_authority_count: Literal[0]
    legacy_non_default_start: date
    legacy_non_default_start_is_default: Literal[False]
    difference_classes: tuple[QCReconciliationDifferenceClass, ...]
    dispositions: tuple[QCReconciliationDisposition, ...]
    exact_comparison_fields: tuple[str, ...]
    criteria: Annotated[
        UnresolvedLocalReconciliationCriteria | ActiveLocalReconciliationCriteria,
        Field(discriminator="mode"),
    ]
    safety: QCLocalReconciliationSafety
    decision: Literal[
        "LOCAL_QC_RECONCILIATION_V1_READY_POLICY_BLOCKED",
        "LOCAL_QC_RECONCILIATION_V1_READY",
    ]

    @field_validator(
        "owner",
        "owner_decision",
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
        "owner_authorization_status",
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
        "selection_policy_sha256",
        "execution_policy_sha256",
        "accounting_policy_sha256",
        "lifecycle_policy_sha256",
        "platform_evidence_policy_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_policy(self) -> Self:
        if self.primary_research_start != date(2021, 2, 22):
            raise ValueError("primary research start must remain 2021-02-22")
        if self.legacy_non_default_start != date(2022, 12, 1):
            raise ValueError("legacy non-default marker drifted")
        if self.difference_classes != _DIFFERENCE_CLASSES:
            raise ValueError("difference taxonomy must be complete, sorted, and exact")
        if self.dispositions != _DISPOSITIONS:
            raise ValueError("reconciliation dispositions must be complete, sorted, and exact")
        if self.exact_comparison_fields != _EXACT_FIELDS:
            raise ValueError("exact comparison inventory drifted")
        if self.status == "OWNER_REVIEW_REQUIRED_BASELINE":
            if self.reconciliation_authorized or self.owner_authorization_status != _NOT_GRANTED:
                raise ValueError("baseline cannot authorize reconciliation")
            if not isinstance(self.criteria, UnresolvedLocalReconciliationCriteria):
                raise ValueError("baseline criteria must remain unresolved")
            if self.decision != "LOCAL_QC_RECONCILIATION_V1_READY_POLICY_BLOCKED":
                raise ValueError("baseline decision must remain policy blocked")
        else:
            if not self.reconciliation_authorized:
                raise ValueError("active policy must authorize reconciliation")
            if not self.owner_authorization_status.startswith("OWNER_REVIEWED:"):
                raise ValueError("active policy requires reviewed owner authority")
            if not isinstance(self.criteria, ActiveLocalReconciliationCriteria):
                raise ValueError("active policy requires active criteria")
            if self.decision != "LOCAL_QC_RECONCILIATION_V1_READY":
                raise ValueError("active policy decision mismatch")
        return self


@dataclass(frozen=True)
class QCLocalReconciliationPolicyLoadResult:
    policy: QCLocalReconciliationPolicy
    policy_sha256: str
    policy_path: Path


class QCCanonicalArtifact(_StrictModel):
    artifact_id: str
    content: bytes
    sha256: str

    @field_validator("artifact_id")
    @classmethod
    def _validate_artifact_id(cls, value: str) -> str:
        return _identifier(value, "artifact_id")

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "sha256")

    @model_validator(mode="after")
    def _validate_content(self) -> Self:
        if not self.content or _content_sha256(self.content) != self.sha256:
            raise ValueError("artifact bytes do not match declared SHA-256")
        return self


class QCLocalReconciliationRequest(_SealedModel):
    schema_version: Literal["qc_qqq_options_local_reconciliation_request.v1"]
    request_id: str
    evaluated_at_utc: datetime
    run_manifest: QCCanonicalArtifact
    execution_results: tuple[QCCanonicalArtifact, ...]
    cash_accounting_result: QCCanonicalArtifact
    lifecycle_result: QCCanonicalArtifact

    @field_validator("request_id")
    @classmethod
    def _validate_request_id(cls, value: str) -> str:
        return _identifier(value, "request_id")

    @field_validator("evaluated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "evaluated_at_utc")

    @model_validator(mode="after")
    def _validate_artifacts(self) -> Self:
        if not self.execution_results:
            raise ValueError("at least one execution result is required")
        ids = tuple(item.artifact_id for item in self.execution_results)
        if ids != tuple(sorted(ids)) or len(ids) != len(set(ids)):
            raise ValueError("execution artifacts must be sorted and unique")
        try:
            manifest = RunManifestRecord.from_json_bytes(self.run_manifest.content)
            executions = tuple(
                QQQOptionExecutionResult.from_json_bytes(item.content)
                for item in self.execution_results
            )
            accounting = QQQOptionCashAccountingResult.from_json_bytes(
                self.cash_accounting_result.content
            )
            lifecycle = QQQOptionPositionLifecycleResult.from_json_bytes(
                self.lifecycle_result.content
            )
        except ValueError as exc:
            raise ValueError(f"canonical predecessor artifact invalid: {exc}") from exc
        if self.run_manifest.artifact_id != "run_manifest":
            raise ValueError("run manifest artifact id mismatch")
        if self.cash_accounting_result.artifact_id != "cash_accounting_result":
            raise ValueError("cash accounting artifact id mismatch")
        if self.lifecycle_result.artifact_id != "lifecycle_result":
            raise ValueError("lifecycle artifact id mismatch")
        for result in executions:
            records = (
                (() if result.order_intent is None else (result.order_intent,))
                + result.order_events
                + result.fill_events
            )
            if any(record.run_id != manifest.run_id for record in records):
                raise ValueError("execution result run id differs from manifest")
        if accounting.input_sha256 == "" or lifecycle.input_sha256 == "":
            raise ValueError("predecessor input identity missing")
        return self

    def parsed_manifest(self) -> RunManifestRecord:
        return RunManifestRecord.from_json_bytes(self.run_manifest.content)

    def parsed_executions(self) -> tuple[QQQOptionExecutionResult, ...]:
        return tuple(
            QQQOptionExecutionResult.from_json_bytes(item.content)
            for item in self.execution_results
        )

    def parsed_accounting(self) -> QQQOptionCashAccountingResult:
        return QQQOptionCashAccountingResult.from_json_bytes(self.cash_accounting_result.content)

    def parsed_lifecycle(self) -> QQQOptionPositionLifecycleResult:
        return QQQOptionPositionLifecycleResult.from_json_bytes(self.lifecycle_result.content)


class QCPlatformOrderFact(_SealedModel):
    schema_version: Literal["qc_platform_order_fact.v1"]
    order_id: str
    event_sequence: int
    option_sid: str
    underlying: Literal["QQQ"]
    side: Literal["BUY_TO_OPEN", "SELL_TO_CLOSE"]
    order_contracts: int
    filled_contracts_total: int
    order_state: Literal[
        "CREATED", "SUBMITTED", "UPDATED", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED"
    ]
    limit_price_per_share: CanonicalDecimal
    event_at_utc: datetime

    @field_validator("order_id", "option_sid")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("event_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "event_at_utc")

    @model_validator(mode="after")
    def _validate_counts(self) -> Self:
        if self.event_sequence < 0 or self.order_contracts <= 0:
            raise ValueError("order sequence/count is invalid")
        if not 0 <= self.filled_contracts_total <= self.order_contracts:
            raise ValueError("cumulative fill quantity is invalid")
        if self.limit_price_per_share <= 0:
            raise ValueError("limit price must be positive")
        return self


class QCPlatformFillFact(_SealedModel):
    schema_version: Literal["qc_platform_fill_fact.v1"]
    fill_id: str
    order_id: str
    option_sid: str
    side: Literal["BUY_TO_OPEN", "SELL_TO_CLOSE"]
    filled_contracts: int
    fill_price_per_share: CanonicalDecimal
    contract_multiplier: int
    fee_usd: CanonicalDecimal
    gross_cash_delta_usd: CanonicalDecimal
    fill_at_utc: datetime

    @field_validator("fill_id", "order_id", "option_sid")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("fill_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "fill_at_utc")

    @model_validator(mode="after")
    def _validate_numbers(self) -> Self:
        if self.filled_contracts <= 0 or self.contract_multiplier <= 0:
            raise ValueError("fill quantity/multiplier must be positive")
        if self.fill_price_per_share <= 0 or self.fee_usd < 0:
            raise ValueError("fill price/fee is invalid")
        return self


class QCPlatformCashFact(_SealedModel):
    schema_version: Literal["qc_platform_cash_fact.v1"]
    settled_cash_usd: CanonicalDecimal
    unsettled_cash_usd: CanonicalDecimal
    reserved_cash_usd: CanonicalDecimal
    option_market_value_usd: CanonicalDecimal
    fees_paid_usd: CanonicalDecimal
    realized_pnl_usd: CanonicalDecimal
    unrealized_pnl_usd: CanonicalDecimal


class QCPlatformLifecycleFact(_SealedModel):
    schema_version: Literal["qc_platform_lifecycle_fact.v1"]
    position_id: str
    option_sid: str
    terminal_state: str
    contracts_open: int
    remaining_cost_basis_usd: CanonicalDecimal

    @field_validator("position_id", "option_sid", "terminal_state")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_quantity(self) -> Self:
        if self.contracts_open < 0:
            raise ValueError("open contracts cannot be negative")
        return self


class QCPlatformMetricFact(_SealedModel):
    schema_version: Literal["qc_platform_metric_fact.v1"]
    metric_id: str
    value: CanonicalDecimal
    unit: str

    @field_validator("metric_id", "unit")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))


class QCPlatformReconciliationProjection(_SealedModel):
    schema_version: Literal["qc_qqq_options_platform_reconciliation_projection.v1"]
    run_id: str
    repository_code_sha: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    lineage_id: str
    source_ids: tuple[str, ...]
    source_checksums: tuple[str, ...]
    option_event_dq_report: DQReportRecord
    orders: tuple[QCPlatformOrderFact, ...]
    fills: tuple[QCPlatformFillFact, ...]
    cash: QCPlatformCashFact
    lifecycle: tuple[QCPlatformLifecycleFact, ...]
    metrics: tuple[QCPlatformMetricFact, ...]

    @field_validator("run_id", "lineage_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator("source_checksums")
    @classmethod
    def _validate_source_hashes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(_sha256(item, "source_checksums") for item in value)

    @model_validator(mode="after")
    def _validate_projection(self) -> Self:
        if not (
            self.requested_start <= self.evaluated_start <= self.evaluated_end <= self.requested_end
        ):
            raise ValueError("projection date ranges are inconsistent")
        if len(self.source_ids) != len(self.source_checksums):
            raise ValueError("projection source ids/checksums are not aligned")
        if self.source_ids != tuple(sorted(self.source_ids)) or len(self.source_ids) != len(
            set(self.source_ids)
        ):
            raise ValueError("projection sources must be sorted and unique")
        for values, key in (
            (self.orders, lambda item: (item.order_id, item.event_sequence)),
            (self.fills, lambda item: (item.order_id, item.fill_at_utc, item.fill_id)),
            (self.lifecycle, lambda item: (item.position_id, item.option_sid)),
            (self.metrics, lambda item: item.metric_id),
        ):
            keys = tuple(key(item) for item in values)
            if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
                raise ValueError("projection facts must be sorted and unique")
        report = self.option_event_dq_report
        if (
            report.run_id != self.run_id
            or report.repository_code_sha != self.repository_code_sha
            or report.requested_start != self.requested_start
            or report.requested_end != self.requested_end
            or report.evaluated_start != self.evaluated_start
            or report.evaluated_end != self.evaluated_end
            or report.lineage_id != self.lineage_id
        ):
            raise ValueError("option-event DQ report does not match projection identity")
        return self


class QCExactReconciliationCheck(_SealedModel):
    schema_version: Literal["qc_exact_reconciliation_check.v1"]
    check_id: str
    layer: str
    difference_class: QCReconciliationDifferenceClass
    local_value: str
    platform_value: str
    status: Literal["PASS", "FAIL"]
    local_evidence_sha256: str
    platform_evidence_sha256: str
    explanation: str

    @field_validator("check_id", "layer")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("local_evidence_sha256", "platform_evidence_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("local_value", "platform_value", "explanation")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_status(self) -> Self:
        expected = "PASS" if self.local_value == self.platform_value else "FAIL"
        if self.status != expected:
            raise ValueError("exact check status is not derived from values")
        return self


class QCReconciliationDifference(_SealedModel):
    schema_version: Literal["qc_reconciliation_difference.v1"]
    check_id: str
    layer: str
    difference_class: QCReconciliationDifferenceClass
    disposition: QCReconciliationDisposition
    owner: str
    impact: str
    explanation: str
    local_evidence_sha256: str
    platform_evidence_sha256: str

    @field_validator("check_id", "layer", "owner")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("impact", "explanation")
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("local_evidence_sha256", "platform_evidence_sha256")
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class QCLocalReconciliationResult(_SealedModel):
    schema_version: Literal["qc_qqq_options_local_reconciliation_result.v1"]
    request_id: str
    run_id: str
    evaluated_at_utc: datetime
    policy_sha256: str
    input_sha256: str
    platform_bundle_validation_sha256: str | None
    platform_projection_sha256: str | None
    requested_start: date
    requested_end: date
    evaluated_start: date | None
    evaluated_end: date | None
    local_dq_status: Literal["PASS", "FAIL", "NOT_EVALUATED"]
    local_pit_status: Literal["PASS", "FAIL", "NOT_EVALUATED"]
    option_event_dq_status: Literal["PASS", "FAIL", "NOT_EVALUATED"]
    option_event_pit_status: Literal["PASS", "FAIL", "NOT_EVALUATED"]
    external_pass_overrode_internal_failure: Literal[False]
    exact_checks: tuple[QCExactReconciliationCheck, ...]
    numeric_reports: tuple[ReconciliationReportRecord, ...]
    differences: tuple[QCReconciliationDifference, ...]
    outcome: QCReconciliationOutcome
    reason_codes: tuple[str, ...]
    investment_interpretation_allowed: Literal[False]
    range_expansion_allowed: Literal[False]
    new_order_count: Literal[0]
    new_fill_count: Literal[0]
    safety: QCLocalReconciliationSafety

    @field_validator("request_id", "run_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("evaluated_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "evaluated_at_utc")

    @field_validator("policy_sha256", "input_sha256")
    @classmethod
    def _validate_required_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("platform_bundle_validation_sha256", "platform_projection_sha256")
    @classmethod
    def _validate_optional_hashes(cls, value: str | None, info: ValidationInfo) -> str | None:
        return None if value is None else _sha256(value, str(info.field_name))

    @field_validator("reason_codes")
    @classmethod
    def _validate_reasons(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_identifier(item, "reason_codes") for item in value)
        if checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError("reason codes must be sorted and unique")
        return checked

    @model_validator(mode="after")
    def _validate_result(self) -> Self:
        for keys in (
            tuple(item.check_id for item in self.exact_checks),
            tuple(item.check_id for item in self.numeric_reports),
            tuple(item.check_id for item in self.differences),
        ):
            if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
                raise ValueError("reconciliation outputs must be sorted and unique")
        internal_statuses = (self.local_dq_status, self.local_pit_status)
        event_statuses = (self.option_event_dq_status, self.option_event_pit_status)
        if self.outcome == "LOCAL_RECONCILIATION_READY_FOR_OWNER_REVIEW":
            if any(value != "PASS" for value in internal_statuses + event_statuses):
                raise ValueError("ready outcome requires all DQ/PIT axes PASS")
            if any(item.status == "FAIL" for item in self.exact_checks):
                raise ValueError("ready outcome cannot contain failed exact checks")
            if any(item.status in {"FAIL", "INCOMPLETE"} for item in self.numeric_reports):
                raise ValueError("ready outcome cannot contain failed numeric checks")
        return self


def load_qc_qqq_options_local_reconciliation_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_LOCAL_RECONCILIATION_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCLocalReconciliationPolicyLoadResult:
    resolved = _resolve(path, project_root=project_root)
    try:
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("policy root must be a mapping")
        policy = QCLocalReconciliationPolicy.model_validate(payload)
        policy_sha256 = sha256_path(resolved)
        _validate_inherited_authority(policy, project_root=project_root)
    except QCLocalReconciliationContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCLocalReconciliationContractError(
            "LOCAL_RECONCILIATION_POLICY_INVALID", f"{resolved}: {exc}"
        ) from exc
    return QCLocalReconciliationPolicyLoadResult(policy, policy_sha256, resolved)


def build_qc_qqq_options_local_reconciliation_input_sha256(
    request: QCLocalReconciliationRequest,
    *,
    policy_sha256: str,
) -> str:
    payload = {
        "policy_sha256": _sha256(policy_sha256, "policy_sha256"),
        "request_content_sha256": request.content_sha256,
        "request_canonical_sha256": request.canonical_sha256,
        "run_manifest_sha256": request.run_manifest.sha256,
        "execution_result_sha256s": [item.sha256 for item in request.execution_results],
        "cash_accounting_result_sha256": request.cash_accounting_result.sha256,
        "lifecycle_result_sha256": request.lifecycle_result.sha256,
    }
    return _content_sha256(_canonical_json_bytes(payload))


def reconcile_qc_qqq_options_local_evidence(
    request: QCLocalReconciliationRequest,
    *,
    package_root: Path,
    capability_receipt_path: Path,
    policy_path: Path = DEFAULT_QC_QQQ_OPTIONS_LOCAL_RECONCILIATION_POLICY_PATH,
    platform_evidence_policy_path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_BUNDLE_POLICY_PATH
    ),
    capability_policy_path: Path,
    capability_evidence_path: Path,
    project_root: Path = PROJECT_ROOT,
) -> QCLocalReconciliationResult:
    loaded_policy = load_qc_qqq_options_local_reconciliation_policy(
        policy_path, project_root=project_root
    )
    policy = loaded_policy.policy
    input_sha256 = build_qc_qqq_options_local_reconciliation_input_sha256(
        request, policy_sha256=loaded_policy.policy_sha256
    )
    manifest = request.parsed_manifest()
    if not policy.reconciliation_authorized or not isinstance(
        policy.criteria, ActiveLocalReconciliationCriteria
    ):
        return _blocked_result(
            request=request,
            manifest=manifest,
            policy=policy,
            policy_sha256=loaded_policy.policy_sha256,
            input_sha256=input_sha256,
            outcome="LOCAL_RECONCILIATION_POLICY_BLOCKED",
            reason_codes=("RECONCILIATION_POLICY_REVIEW_REQUIRED",),
        )
    try:
        bundle = load_qc_qqq_options_manual_evidence_bundle(
            package_root,
            capability_receipt_path=capability_receipt_path,
            policy_path=platform_evidence_policy_path,
            capability_policy_path=capability_policy_path,
            capability_evidence_path=capability_evidence_path,
            project_root=project_root,
        )
    except QCPlatformEvidenceBundleContractError as exc:
        outcome: QCReconciliationOutcome = (
            "LOCAL_RECONCILIATION_INCOMPLETE"
            if exc.code == "MANUAL_COLLECTION_INCOMPLETE"
            else "LOCAL_RECONCILIATION_INPUT_INVALID"
        )
        return _blocked_result(
            request=request,
            manifest=manifest,
            policy=policy,
            policy_sha256=loaded_policy.policy_sha256,
            input_sha256=input_sha256,
            outcome=outcome,
            reason_codes=(exc.code,),
        )
    return _reconcile_loaded_bundle(
        request=request,
        manifest=manifest,
        policy=policy,
        policy_sha256=loaded_policy.policy_sha256,
        input_sha256=input_sha256,
        bundle=bundle,
    )


def _blocked_result(
    *,
    request: QCLocalReconciliationRequest,
    manifest: RunManifestRecord,
    policy: QCLocalReconciliationPolicy,
    policy_sha256: str,
    input_sha256: str,
    outcome: QCReconciliationOutcome,
    reason_codes: tuple[str, ...],
) -> QCLocalReconciliationResult:
    return QCLocalReconciliationResult.seal(
        schema_version="qc_qqq_options_local_reconciliation_result.v1",
        request_id=request.request_id,
        run_id=manifest.run_id,
        evaluated_at_utc=request.evaluated_at_utc,
        policy_sha256=policy_sha256,
        input_sha256=input_sha256,
        platform_bundle_validation_sha256=None,
        platform_projection_sha256=None,
        requested_start=manifest.requested_start,
        requested_end=manifest.requested_end,
        evaluated_start=manifest.evaluated_start,
        evaluated_end=manifest.evaluated_end,
        local_dq_status=manifest.dq_status,
        local_pit_status=manifest.pit_status,
        option_event_dq_status="NOT_EVALUATED",
        option_event_pit_status="NOT_EVALUATED",
        external_pass_overrode_internal_failure=False,
        exact_checks=(),
        numeric_reports=(),
        differences=(),
        outcome=outcome,
        reason_codes=tuple(sorted(reason_codes)),
        investment_interpretation_allowed=False,
        range_expansion_allowed=False,
        new_order_count=0,
        new_fill_count=0,
        safety=policy.safety,
    )


def _reconcile_loaded_bundle(
    *,
    request: QCLocalReconciliationRequest,
    manifest: RunManifestRecord,
    policy: QCLocalReconciliationPolicy,
    policy_sha256: str,
    input_sha256: str,
    bundle: LoadedQCPlatformEvidenceBundle,
) -> QCLocalReconciliationResult:
    criteria = policy.criteria
    assert isinstance(criteria, ActiveLocalReconciliationCriteria)
    projection = _load_platform_projection(bundle, criteria)
    internal_statuses = _internal_statuses(request, manifest)
    if any(status != "PASS" for status in internal_statuses):
        return QCLocalReconciliationResult.seal(
            schema_version="qc_qqq_options_local_reconciliation_result.v1",
            request_id=request.request_id,
            run_id=manifest.run_id,
            evaluated_at_utc=request.evaluated_at_utc,
            policy_sha256=policy_sha256,
            input_sha256=input_sha256,
            platform_bundle_validation_sha256=bundle.validation.content_sha256,
            platform_projection_sha256=projection.content_sha256,
            requested_start=manifest.requested_start,
            requested_end=manifest.requested_end,
            evaluated_start=manifest.evaluated_start,
            evaluated_end=manifest.evaluated_end,
            local_dq_status=_worst_status(internal_statuses[::2]),
            local_pit_status=_worst_status(internal_statuses[1::2]),
            option_event_dq_status=projection.option_event_dq_report.dq_status,
            option_event_pit_status=projection.option_event_dq_report.pit_status,
            external_pass_overrode_internal_failure=False,
            exact_checks=(),
            numeric_reports=(),
            differences=(),
            outcome="LOCAL_RECONCILIATION_INPUT_INVALID",
            reason_codes=("INTERNAL_OR_OPTION_EVENT_DQ_PIT_NOT_PASS",),
            investment_interpretation_allowed=False,
            range_expansion_allowed=False,
            new_order_count=0,
            new_fill_count=0,
            safety=policy.safety,
        )
    local = _build_local_projection(request, projection.option_event_dq_report)
    exact_checks = _exact_checks(manifest, local, projection, bundle)
    numeric_reports = _numeric_reports(
        request=request,
        manifest=manifest,
        local=local,
        platform=projection,
        policy=policy,
        policy_sha256=policy_sha256,
        criteria=criteria,
        platform_evidence_sha256=projection.content_sha256,
    )
    differences = _differences(exact_checks, numeric_reports, policy.owner)
    all_statuses = internal_statuses + (
        projection.option_event_dq_report.dq_status,
        projection.option_event_dq_report.pit_status,
    )
    reasons: set[str] = set()
    if "FAIL" in all_statuses or "NOT_EVALUATED" in all_statuses:
        outcome: QCReconciliationOutcome = "LOCAL_RECONCILIATION_INPUT_INVALID"
        reasons.add("INTERNAL_OR_OPTION_EVENT_DQ_PIT_NOT_PASS")
    elif any(item.status == "FAIL" for item in exact_checks) or any(
        item.status in {"FAIL", "INCOMPLETE"} for item in numeric_reports
    ):
        outcome = "LOCAL_RECONCILIATION_REQUIRES_FIX"
        reasons.add("RECONCILIATION_DIFFERENCE_REQUIRES_FIX")
    else:
        outcome = "LOCAL_RECONCILIATION_READY_FOR_OWNER_REVIEW"
    return QCLocalReconciliationResult.seal(
        schema_version="qc_qqq_options_local_reconciliation_result.v1",
        request_id=request.request_id,
        run_id=manifest.run_id,
        evaluated_at_utc=request.evaluated_at_utc,
        policy_sha256=policy_sha256,
        input_sha256=input_sha256,
        platform_bundle_validation_sha256=bundle.validation.content_sha256,
        platform_projection_sha256=projection.content_sha256,
        requested_start=manifest.requested_start,
        requested_end=manifest.requested_end,
        evaluated_start=manifest.evaluated_start,
        evaluated_end=manifest.evaluated_end,
        local_dq_status=_worst_status(internal_statuses[::2]),
        local_pit_status=_worst_status(internal_statuses[1::2]),
        option_event_dq_status=projection.option_event_dq_report.dq_status,
        option_event_pit_status=projection.option_event_dq_report.pit_status,
        external_pass_overrode_internal_failure=False,
        exact_checks=tuple(sorted(exact_checks, key=lambda item: item.check_id)),
        numeric_reports=tuple(sorted(numeric_reports, key=lambda item: item.check_id)),
        differences=tuple(sorted(differences, key=lambda item: item.check_id)),
        outcome=outcome,
        reason_codes=tuple(sorted(reasons)),
        investment_interpretation_allowed=False,
        range_expansion_allowed=False,
        new_order_count=0,
        new_fill_count=0,
        safety=policy.safety,
    )


def _load_platform_projection(
    bundle: LoadedQCPlatformEvidenceBundle,
    criteria: ActiveLocalReconciliationCriteria,
) -> QCPlatformReconciliationProjection:
    root = bundle.package_root
    paths = {
        "results_json": root / "artifacts/results.json",
        "orders_csv": root / "artifacts/orders.csv",
        "trades_csv": root / "artifacts/trades.csv",
    }
    contents: dict[str, bytes] = {}
    for artifact_id, path in paths.items():
        relative = path.relative_to(root).as_posix()
        content = path.read_bytes()
        if bundle.file_sha256s.get(relative) != _content_sha256(content):
            raise QCLocalReconciliationContractError(
                "LOCAL_RECONCILIATION_PLATFORM_FILE_TAMPERED", relative
            )
        contents[artifact_id] = content
    try:
        results = json.loads(contents["results_json"])
        raw_projection = results[criteria.results_projection_key]
        if not isinstance(raw_projection, dict):
            raise TypeError("platform reconciliation projection must be an object")
        projection = QCPlatformReconciliationProjection.model_validate_json(
            _canonical_json_bytes(raw_projection)
        )
    except (UnicodeDecodeError, KeyError, TypeError, ValueError) as exc:
        raise QCLocalReconciliationContractError(
            "LOCAL_RECONCILIATION_PLATFORM_PROJECTION_INVALID", str(exc)
        ) from exc
    csv_orders = _parse_csv_facts(
        contents["orders_csv"], criteria.orders_csv_column_mapping, QCPlatformOrderFact
    )
    csv_fills = _parse_csv_facts(
        contents["trades_csv"], criteria.trades_csv_column_mapping, QCPlatformFillFact
    )
    if tuple(csv_orders) != projection.orders:
        raise QCLocalReconciliationContractError(
            "LOCAL_RECONCILIATION_ORDERS_CSV_MISMATCH",
            "Orders CSV facts do not equal Results JSON projection",
        )
    if tuple(csv_fills) != projection.fills:
        raise QCLocalReconciliationContractError(
            "LOCAL_RECONCILIATION_TRADES_CSV_MISMATCH",
            "Trades CSV facts do not equal Results JSON projection",
        )
    return projection


@overload
def _parse_csv_facts(
    content: bytes,
    mappings: tuple[QCPlatformColumnMapping, ...],
    model: type[QCPlatformOrderFact],
) -> list[QCPlatformOrderFact]: ...


@overload
def _parse_csv_facts(
    content: bytes,
    mappings: tuple[QCPlatformColumnMapping, ...],
    model: type[QCPlatformFillFact],
) -> list[QCPlatformFillFact]: ...


def _parse_csv_facts(
    content: bytes,
    mappings: tuple[QCPlatformColumnMapping, ...],
    model: type[QCPlatformOrderFact] | type[QCPlatformFillFact],
) -> list[QCPlatformOrderFact] | list[QCPlatformFillFact]:
    try:
        reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
        source_columns = tuple(item.source_column for item in mappings)
        if tuple(reader.fieldnames or ()) != source_columns:
            raise ValueError("CSV header does not equal reviewed mapping")
        rows = list(reader)
    except (UnicodeDecodeError, csv.Error, ValueError) as exc:
        raise QCLocalReconciliationContractError(
            "LOCAL_RECONCILIATION_CSV_INVALID", str(exc)
        ) from exc
    facts: list[Any] = []
    for index, row in enumerate(rows):
        payload = {item.canonical_field: row[item.source_column] for item in mappings}
        payload["schema_version"] = (
            "qc_platform_order_fact.v1"
            if model is QCPlatformOrderFact
            else "qc_platform_fill_fact.v1"
        )
        for integer_field in (
            "event_sequence",
            "order_contracts",
            "filled_contracts_total",
            "filled_contracts",
            "contract_multiplier",
        ):
            if integer_field in payload:
                try:
                    payload[integer_field] = int(payload[integer_field])
                except (TypeError, ValueError) as exc:
                    raise QCLocalReconciliationContractError(
                        "LOCAL_RECONCILIATION_CSV_INVALID",
                        f"row {index} field {integer_field} is not an integer",
                    ) from exc
        for timestamp_field in ("event_at_utc", "fill_at_utc"):
            if timestamp_field in payload:
                try:
                    payload[timestamp_field] = datetime.fromisoformat(
                        str(payload[timestamp_field]).replace("Z", "+00:00")
                    )
                except ValueError as exc:
                    raise QCLocalReconciliationContractError(
                        "LOCAL_RECONCILIATION_CSV_INVALID",
                        f"row {index} field {timestamp_field} is not ISO-8601 UTC",
                    ) from exc
        try:
            facts.append(model.seal(**payload))
        except ValueError as exc:
            raise QCLocalReconciliationContractError(
                "LOCAL_RECONCILIATION_CSV_INVALID", f"row {index}: {exc}"
            ) from exc
    if model is QCPlatformOrderFact:
        order_facts = cast(list[QCPlatformOrderFact], facts)
        return sorted(order_facts, key=lambda item: (item.order_id, item.event_sequence))
    fill_facts = cast(list[QCPlatformFillFact], facts)
    return sorted(fill_facts, key=lambda item: (item.order_id, item.fill_at_utc, item.fill_id))


def _build_local_projection(
    request: QCLocalReconciliationRequest,
    option_event_dq_report: DQReportRecord,
) -> QCPlatformReconciliationProjection:
    manifest = request.parsed_manifest()
    if manifest.evaluated_start is None or manifest.evaluated_end is None:
        raise QCLocalReconciliationContractError(
            "LOCAL_RECONCILIATION_INTERNAL_RANGE_INCOMPLETE",
            "run manifest must carry the actual evaluated range",
        )
    orders: list[QCPlatformOrderFact] = []
    fills: list[QCPlatformFillFact] = []
    for result in request.parsed_executions():
        intent = result.order_intent
        if intent is None:
            if result.order_events or result.fill_events:
                raise QCLocalReconciliationContractError(
                    "LOCAL_RECONCILIATION_INTERNAL_EXECUTION_INVALID",
                    "order/fill events require an order intent",
                )
            continue
        for event in result.order_events:
            orders.append(
                QCPlatformOrderFact.seal(
                    schema_version="qc_platform_order_fact.v1",
                    order_id=event.platform_order_id,
                    event_sequence=event.event_sequence,
                    option_sid=intent.option_sid,
                    underlying="QQQ",
                    side=event.side,
                    order_contracts=event.order_contracts,
                    filled_contracts_total=event.filled_contracts_total,
                    order_state=event.event_type,
                    limit_price_per_share=event.limit_price_per_share,
                    event_at_utc=event.event_at_utc,
                )
            )
        for fill in result.fill_events:
            fills.append(
                QCPlatformFillFact.seal(
                    schema_version="qc_platform_fill_fact.v1",
                    fill_id=fill.record_id,
                    order_id=fill.platform_order_id,
                    option_sid=intent.option_sid,
                    side=fill.side,
                    filled_contracts=fill.filled_contracts,
                    fill_price_per_share=fill.fill_price_per_share,
                    contract_multiplier=fill.contract_multiplier,
                    fee_usd=fill.fee_usd,
                    gross_cash_delta_usd=fill.gross_cash_delta_usd,
                    fill_at_utc=fill.fill_at_utc,
                )
            )
    accounting = request.parsed_accounting()
    lifecycle_result = request.parsed_lifecycle()
    snapshot = accounting.portfolio_snapshot
    if snapshot is None:
        raise QCLocalReconciliationContractError(
            "LOCAL_RECONCILIATION_INTERNAL_ACCOUNTING_INCOMPLETE",
            "accounting result lacks portfolio snapshot",
        )
    cash = QCPlatformCashFact.seal(
        schema_version="qc_platform_cash_fact.v1",
        settled_cash_usd=snapshot.settled_cash_usd,
        unsettled_cash_usd=snapshot.unsettled_cash_usd,
        reserved_cash_usd=snapshot.reserved_cash_usd,
        option_market_value_usd=snapshot.option_market_value_usd,
        fees_paid_usd=snapshot.fees_paid_usd,
        realized_pnl_usd=snapshot.realized_pnl_usd,
        unrealized_pnl_usd=snapshot.unrealized_pnl_usd,
    )
    lifecycle = tuple(
        QCPlatformLifecycleFact.seal(
            schema_version="qc_platform_lifecycle_fact.v1",
            position_id=item.position_id,
            option_sid=item.option_sid,
            terminal_state=item.terminal_state,
            contracts_open=item.contracts_open,
            remaining_cost_basis_usd=item.remaining_cost_basis_usd,
        )
        for item in lifecycle_result.positions
    )
    metrics = tuple(
        QCPlatformMetricFact.seal(
            schema_version="qc_platform_metric_fact.v1",
            metric_id=metric_id,
            value=value,
            unit=unit,
        )
        for metric_id, value, unit in (
            ("fill_event_count", Decimal(len(fills)), "COUNT"),
            ("order_event_count", Decimal(len(orders)), "COUNT"),
            ("total_fees_usd", snapshot.fees_paid_usd, "USD"),
        )
    )
    return QCPlatformReconciliationProjection.seal(
        schema_version="qc_qqq_options_platform_reconciliation_projection.v1",
        run_id=manifest.run_id,
        repository_code_sha=manifest.repository_code_sha,
        requested_start=manifest.requested_start,
        requested_end=manifest.requested_end,
        evaluated_start=manifest.evaluated_start,
        evaluated_end=manifest.evaluated_end,
        lineage_id=manifest.lineage_id,
        source_ids=manifest.source_ids,
        source_checksums=manifest.source_checksums,
        option_event_dq_report=option_event_dq_report,
        orders=tuple(sorted(orders, key=lambda item: (item.order_id, item.event_sequence))),
        fills=tuple(
            sorted(fills, key=lambda item: (item.order_id, item.fill_at_utc, item.fill_id))
        ),
        cash=cash,
        lifecycle=tuple(sorted(lifecycle, key=lambda item: (item.position_id, item.option_sid))),
        metrics=tuple(sorted(metrics, key=lambda item: item.metric_id)),
    )


def _exact_checks(
    manifest: RunManifestRecord,
    local: QCPlatformReconciliationProjection,
    platform: QCPlatformReconciliationProjection,
    bundle: LoadedQCPlatformEvidenceBundle,
) -> tuple[QCExactReconciliationCheck, ...]:
    rows: list[tuple[str, str, QCReconciliationDifferenceClass, str, str, str, str]] = [
        (
            "run_id",
            "lineage",
            "LOGIC",
            local.run_id,
            platform.run_id,
            local.content_sha256,
            platform.content_sha256,
        ),
        (
            "repository_code_sha",
            "lineage",
            "LOGIC",
            local.repository_code_sha,
            platform.repository_code_sha,
            local.content_sha256,
            platform.content_sha256,
        ),
        (
            "requested_range",
            "lineage",
            "LOGIC",
            f"{local.requested_start}/{local.requested_end}",
            f"{platform.requested_start}/{platform.requested_end}",
            local.content_sha256,
            platform.content_sha256,
        ),
        (
            "evaluated_range",
            "lineage",
            "LOGIC",
            f"{local.evaluated_start}/{local.evaluated_end}",
            f"{platform.evaluated_start}/{platform.evaluated_end}",
            local.content_sha256,
            platform.content_sha256,
        ),
        (
            "lineage_id",
            "lineage",
            "LOGIC",
            local.lineage_id,
            platform.lineage_id,
            local.content_sha256,
            platform.content_sha256,
        ),
        (
            "source_checksums",
            "provider",
            "PROVIDER",
            "|".join(local.source_checksums),
            "|".join(platform.source_checksums),
            local.content_sha256,
            platform.content_sha256,
        ),
        (
            "engine_identity",
            "platform",
            "PLATFORM",
            "CONFIRMED",
            bundle.metadata.engine_identity_status,
            manifest.content_sha256,
            bundle.metadata.content_sha256,
        ),
        (
            "license",
            "license",
            "LICENSE",
            "CONFIRMED",
            bundle.metadata.license_status,
            manifest.content_sha256,
            bundle.metadata.content_sha256,
        ),
        (
            "manual_collection",
            "evidence",
            "MANUAL_COLLECTION",
            "MANUAL_COLLECTION_READY_FOR_LOCAL_RECONCILIATION",
            bundle.validation.disposition,
            manifest.content_sha256,
            bundle.validation.content_sha256,
        ),
    ]
    local_orders = {(item.order_id, item.event_sequence): item for item in local.orders}
    platform_orders = {(item.order_id, item.event_sequence): item for item in platform.orders}
    for order_key in sorted(set(local_orders) | set(platform_orders)):
        order_left = local_orders.get(order_key)
        order_right = platform_orders.get(order_key)
        prefix = f"order.{order_key[0]}.{order_key[1]}"
        if order_left is None or order_right is None:
            rows.append(
                (
                    prefix + ".identity",
                    "orders",
                    "LOGIC",
                    "PRESENT" if order_left else "MISSING",
                    "PRESENT" if order_right else "MISSING",
                    local.content_sha256,
                    platform.content_sha256,
                )
            )
            continue
        order_fields: tuple[tuple[str, QCReconciliationDifferenceClass], ...] = (
            ("option_sid", "LOGIC"),
            ("underlying", "LOGIC"),
            ("side", "LOGIC"),
            ("order_contracts", "LOGIC"),
            ("filled_contracts_total", "LOGIC"),
            ("order_state", "PLATFORM"),
        )
        for field, diff_class in order_fields:
            rows.append(
                (
                    prefix + "." + field,
                    "orders",
                    diff_class,
                    str(getattr(order_left, field)),
                    str(getattr(order_right, field)),
                    order_left.content_sha256,
                    order_right.content_sha256,
                )
            )
    local_fills = {item.fill_id: item for item in local.fills}
    platform_fills = {item.fill_id: item for item in platform.fills}
    for fill_key in sorted(set(local_fills) | set(platform_fills)):
        fill_left = local_fills.get(fill_key)
        fill_right = platform_fills.get(fill_key)
        prefix = f"fill.{fill_key}"
        if fill_left is None or fill_right is None:
            rows.append(
                (
                    prefix + ".identity",
                    "fills",
                    "LOGIC",
                    "PRESENT" if fill_left else "MISSING",
                    "PRESENT" if fill_right else "MISSING",
                    local.content_sha256,
                    platform.content_sha256,
                )
            )
            continue
        for field in ("order_id", "option_sid", "side", "filled_contracts", "contract_multiplier"):
            rows.append(
                (
                    prefix + "." + field,
                    "fills",
                    "LOGIC",
                    str(getattr(fill_left, field)),
                    str(getattr(fill_right, field)),
                    fill_left.content_sha256,
                    fill_right.content_sha256,
                )
            )
    local_lifecycle = {item.position_id: item for item in local.lifecycle}
    platform_lifecycle = {item.position_id: item for item in platform.lifecycle}
    for position_key in sorted(set(local_lifecycle) | set(platform_lifecycle)):
        lifecycle_left = local_lifecycle.get(position_key)
        lifecycle_right = platform_lifecycle.get(position_key)
        prefix = f"lifecycle.{position_key}"
        if lifecycle_left is None or lifecycle_right is None:
            rows.append(
                (
                    prefix + ".identity",
                    "lifecycle",
                    "LOGIC",
                    "PRESENT" if lifecycle_left else "MISSING",
                    "PRESENT" if lifecycle_right else "MISSING",
                    local.content_sha256,
                    platform.content_sha256,
                )
            )
            continue
        for field in ("option_sid", "terminal_state", "contracts_open"):
            rows.append(
                (
                    prefix + "." + field,
                    "lifecycle",
                    "LOGIC",
                    str(getattr(lifecycle_left, field)),
                    str(getattr(lifecycle_right, field)),
                    lifecycle_left.content_sha256,
                    lifecycle_right.content_sha256,
                )
            )
    return tuple(
        QCExactReconciliationCheck.seal(
            schema_version="qc_exact_reconciliation_check.v1",
            check_id=_portable_check_id(check_id),
            layer=layer,
            difference_class=diff_class,
            local_value=left,
            platform_value=right,
            status="PASS" if left == right else "FAIL",
            local_evidence_sha256=local_hash,
            platform_evidence_sha256=platform_hash,
            explanation=("Exact values match." if left == right else "Exact values differ."),
        )
        for check_id, layer, diff_class, left, right, local_hash, platform_hash in rows
    )


def _numeric_reports(
    *,
    request: QCLocalReconciliationRequest,
    manifest: RunManifestRecord,
    local: QCPlatformReconciliationProjection,
    platform: QCPlatformReconciliationProjection,
    policy: QCLocalReconciliationPolicy,
    policy_sha256: str,
    criteria: ActiveLocalReconciliationCriteria,
    platform_evidence_sha256: str,
) -> tuple[ReconciliationReportRecord, ...]:
    rows: list[tuple[str, QCReconciliationDifferenceClass, Decimal, Decimal, str, Decimal]] = []
    local_orders = {(item.order_id, item.event_sequence): item for item in local.orders}
    platform_orders = {(item.order_id, item.event_sequence): item for item in platform.orders}
    for order_key in sorted(set(local_orders) & set(platform_orders)):
        order_left = local_orders[order_key]
        order_right = platform_orders[order_key]
        prefix = f"order.{order_key[0]}.{order_key[1]}"
        rows.extend(
            (
                (
                    prefix + ".limit_price",
                    "REALITY_MODEL",
                    order_left.limit_price_per_share,
                    order_right.limit_price_per_share,
                    "USD",
                    criteria.monetary_absolute_tolerance_usd,
                ),
                (
                    prefix + ".event_time",
                    "TIMING",
                    Decimal(str(order_left.event_at_utc.timestamp())),
                    Decimal(str(order_right.event_at_utc.timestamp())),
                    "SECONDS",
                    criteria.timing_absolute_tolerance_seconds,
                ),
            )
        )
    local_fills = {item.fill_id: item for item in local.fills}
    platform_fills = {item.fill_id: item for item in platform.fills}
    for fill_key in sorted(set(local_fills) & set(platform_fills)):
        fill_left = local_fills[fill_key]
        fill_right = platform_fills[fill_key]
        prefix = f"fill.{fill_key}"
        rows.extend(
            (
                (
                    prefix + ".fill_price",
                    "REALITY_MODEL",
                    fill_left.fill_price_per_share,
                    fill_right.fill_price_per_share,
                    "USD",
                    criteria.monetary_absolute_tolerance_usd,
                ),
                (
                    prefix + ".fee",
                    "REALITY_MODEL",
                    fill_left.fee_usd,
                    fill_right.fee_usd,
                    "USD",
                    criteria.monetary_absolute_tolerance_usd,
                ),
                (
                    prefix + ".gross_cash_delta",
                    "REALITY_MODEL",
                    fill_left.gross_cash_delta_usd,
                    fill_right.gross_cash_delta_usd,
                    "USD",
                    criteria.monetary_absolute_tolerance_usd,
                ),
                (
                    prefix + ".fill_time",
                    "TIMING",
                    Decimal(str(fill_left.fill_at_utc.timestamp())),
                    Decimal(str(fill_right.fill_at_utc.timestamp())),
                    "SECONDS",
                    criteria.timing_absolute_tolerance_seconds,
                ),
            )
        )
    for field in (
        "settled_cash_usd",
        "unsettled_cash_usd",
        "reserved_cash_usd",
        "option_market_value_usd",
        "fees_paid_usd",
        "realized_pnl_usd",
        "unrealized_pnl_usd",
    ):
        rows.append(
            (
                f"cash.{field}",
                "REALITY_MODEL",
                getattr(local.cash, field),
                getattr(platform.cash, field),
                "USD",
                criteria.monetary_absolute_tolerance_usd,
            )
        )
    local_lifecycle = {item.position_id: item for item in local.lifecycle}
    platform_lifecycle = {item.position_id: item for item in platform.lifecycle}
    for position_key in sorted(set(local_lifecycle) & set(platform_lifecycle)):
        rows.append(
            (
                f"lifecycle.{position_key}.remaining_cost_basis",
                "REALITY_MODEL",
                local_lifecycle[position_key].remaining_cost_basis_usd,
                platform_lifecycle[position_key].remaining_cost_basis_usd,
                "USD",
                criteria.monetary_absolute_tolerance_usd,
            )
        )
    local_metrics = {item.metric_id: item for item in local.metrics}
    platform_metrics = {item.metric_id: item for item in platform.metrics}
    for metric_key in sorted(set(local_metrics) & set(platform_metrics)):
        tolerance = (
            criteria.monetary_absolute_tolerance_usd
            if local_metrics[metric_key].unit == "USD"
            else criteria.metric_absolute_tolerance
        )
        rows.append(
            (
                f"metric.{metric_key}",
                "PLATFORM",
                local_metrics[metric_key].value,
                platform_metrics[metric_key].value,
                local_metrics[metric_key].unit,
                tolerance,
            )
        )
    source_ids = tuple(sorted(("local_projection", "platform_projection")))
    source_checksums = tuple(
        local.content_sha256 if item == "local_projection" else platform_evidence_sha256
        for item in source_ids
    )
    reports: list[ReconciliationReportRecord] = []
    for raw_id, diff_class, local_value, platform_value, unit, tolerance in rows:
        delta = local_value - platform_value
        status: Literal["PASS", "EXPLAINED_DIFFERENCE", "FAIL"]
        if delta == 0:
            status = "PASS"
        elif abs(delta) <= tolerance:
            status = "EXPLAINED_DIFFERENCE"
        else:
            status = "FAIL"
        check_id = _portable_check_id(raw_id)
        reports.append(
            ReconciliationReportRecord.seal(
                schema_name="reconciliation_report",
                schema_version="1.0.0",
                run_id=manifest.run_id,
                record_id=check_id,
                created_at_utc=request.evaluated_at_utc,
                producer_version="TRADING-2490.v1",
                repository_code_sha=manifest.repository_code_sha,
                policy_id=policy.policy_id,
                policy_version=policy.policy_version,
                policy_sha256=policy_sha256,
                contract_schema_sha256=policy.shared_contract_sha256,
                source_ids=source_ids,
                source_checksums=source_checksums,
                requested_start=manifest.requested_start,
                requested_end=manifest.requested_end,
                evaluated_start=manifest.evaluated_start,
                evaluated_end=manifest.evaluated_end,
                storage_timezone="UTC",
                exchange_timezone="America/New_York",
                dq_status=manifest.dq_status,
                pit_status=manifest.pit_status,
                export_classification="EXPORT_ALLOWED_DERIVED",
                lineage_id=manifest.lineage_id,
                safety=manifest.safety,
                check_id=check_id,
                status=status,
                difference_class=diff_class,
                local_value=local_value,
                platform_value=platform_value,
                delta=delta,
                unit=unit,
                tolerance_policy_id=policy.policy_id,
                tolerance_policy_version=policy.policy_version,
                tolerance_policy_sha256=policy_sha256,
                explanation=f"Reviewed absolute tolerance={_decimal_json(tolerance)} {unit}.",
                evaluated_at_utc=request.evaluated_at_utc,
            )
        )
    return tuple(reports)


def _differences(
    exact_checks: tuple[QCExactReconciliationCheck, ...],
    numeric_reports: tuple[ReconciliationReportRecord, ...],
    owner: str,
) -> tuple[QCReconciliationDifference, ...]:
    result: list[QCReconciliationDifference] = []
    for check in exact_checks:
        if check.status == "PASS":
            continue
        result.append(
            QCReconciliationDifference.seal(
                schema_version="qc_reconciliation_difference.v1",
                check_id=check.check_id,
                layer=check.layer,
                difference_class=check.difference_class,
                disposition="REQUIRES_FIX",
                owner=owner,
                impact="Exact contract identity differs; reconciliation cannot pass.",
                explanation=check.explanation,
                local_evidence_sha256=check.local_evidence_sha256,
                platform_evidence_sha256=check.platform_evidence_sha256,
            )
        )
    for report in numeric_reports:
        if report.status == "PASS":
            continue
        disposition: QCReconciliationDisposition = (
            "ACCEPTED_EXPLAINED" if report.status == "EXPLAINED_DIFFERENCE" else "REQUIRES_FIX"
        )
        result.append(
            QCReconciliationDifference.seal(
                schema_version="qc_reconciliation_difference.v1",
                check_id=report.check_id,
                layer="numeric",
                difference_class=report.difference_class,
                disposition=disposition,
                owner=owner,
                impact=(
                    "Difference is within the reviewed tolerance."
                    if disposition == "ACCEPTED_EXPLAINED"
                    else "Difference exceeds the reviewed tolerance."
                ),
                explanation=report.explanation,
                local_evidence_sha256=report.source_checksums[
                    report.source_ids.index("local_projection")
                ],
                platform_evidence_sha256=report.source_checksums[
                    report.source_ids.index("platform_projection")
                ],
            )
        )
    return tuple(result)


def _internal_statuses(
    request: QCLocalReconciliationRequest,
    manifest: RunManifestRecord,
) -> tuple[Literal["PASS", "FAIL", "NOT_EVALUATED"], ...]:
    statuses: list[Literal["PASS", "FAIL", "NOT_EVALUATED"]] = [
        manifest.dq_status,
        manifest.pit_status,
    ]
    for result in request.parsed_executions():
        statuses.extend((result.global_dq_status, result.global_pit_status))
        statuses.append(result.execution_stage_dq_status)
        statuses.append(result.global_pit_status)
    lifecycle = request.parsed_lifecycle()
    statuses.extend((lifecycle.lifecycle_stage_dq_status, lifecycle.lifecycle_stage_pit_status))
    return tuple(statuses)


def _worst_status(
    values: tuple[Literal["PASS", "FAIL", "NOT_EVALUATED"], ...],
) -> Literal["PASS", "FAIL", "NOT_EVALUATED"]:
    if "FAIL" in values:
        return "FAIL"
    if "NOT_EVALUATED" in values:
        return "NOT_EVALUATED"
    return "PASS"


def _validate_inherited_authority(
    policy: QCLocalReconciliationPolicy, *, project_root: Path
) -> None:
    expected = {
        "shared_contract_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
        "shared_policy_sha256": sha256_path(project_root / _SHARED_POLICY_PATH),
        "dq_pit_policy_sha256": sha256_path(project_root / _DQ_PIT_POLICY_PATH),
        "signal_export_policy_sha256": sha256_path(project_root / _SIGNAL_POLICY_PATH),
        "adapter_policy_sha256": sha256_path(project_root / _ADAPTER_POLICY_PATH),
        "selection_policy_sha256": sha256_path(project_root / _SELECTION_POLICY_PATH),
        "execution_policy_sha256": sha256_path(project_root / _EXECUTION_POLICY_PATH),
        "accounting_policy_sha256": sha256_path(project_root / _ACCOUNTING_POLICY_PATH),
        "lifecycle_policy_sha256": sha256_path(project_root / _LIFECYCLE_POLICY_PATH),
        "platform_evidence_policy_sha256": sha256_path(
            project_root / DEFAULT_QC_QQQ_OPTIONS_PLATFORM_EVIDENCE_BUNDLE_POLICY_PATH
        ),
    }
    for field, actual in expected.items():
        if getattr(policy, field) != actual:
            raise QCLocalReconciliationContractError(
                "LOCAL_RECONCILIATION_INHERITED_AUTHORITY_DRIFT",
                f"{field} does not match current predecessor authority",
            )


def _portable_check_id(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.:-]+", "_", value)
    return _identifier(normalized, "check_id")


def _resolve(path: Path, *, project_root: Path) -> Path:
    return path if path.is_absolute() else project_root / path


__all__ = [
    "ActiveLocalReconciliationCriteria",
    "DEFAULT_QC_QQQ_OPTIONS_LOCAL_RECONCILIATION_POLICY_PATH",
    "QCCanonicalArtifact",
    "QCExactReconciliationCheck",
    "QCLocalReconciliationContractError",
    "QCLocalReconciliationPolicy",
    "QCLocalReconciliationPolicyLoadResult",
    "QCLocalReconciliationRequest",
    "QCLocalReconciliationResult",
    "QCLocalReconciliationSafety",
    "QCPlatformCashFact",
    "QCPlatformColumnMapping",
    "QCPlatformFillFact",
    "QCPlatformLifecycleFact",
    "QCPlatformMetricFact",
    "QCPlatformOrderFact",
    "QCPlatformReconciliationProjection",
    "QCReconciliationDifference",
    "QCReconciliationDifferenceClass",
    "QCReconciliationDisposition",
    "QCReconciliationOutcome",
    "UnresolvedLocalReconciliationCriteria",
    "build_qc_qqq_options_local_reconciliation_input_sha256",
    "load_qc_qqq_options_local_reconciliation_policy",
    "reconcile_qc_qqq_options_local_evidence",
]
