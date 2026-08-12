from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    primary_window_derived_calibration_evidence_generator as generator_v1,
)
from ai_trading_system.trading_calendar import us_equity_market_session
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EXPORT_SAFE_DERIVED_AGGREGATE_COLLECTOR_POLICY_PATH = Path(
    "config/research/qc_qqq_options_primary_window_export_safe_derived_aggregate_collector_v1.yaml"
)

_PRIMARY_RESEARCH_START = date(2021, 2, 22)
_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")
_CHART_ID = "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1"
_SUPPORTED_SLOT_IDS = (
    "SEL_DELTA_SOURCE_RANGE",
    "SEL_DTE_WINDOW",
    "SEL_MONEYNESS_RANGE",
    "SEL_OPEN_INTEREST_FLOOR",
    "SEL_RANK_PRIORITY",
    "SEL_SPREAD_LIMIT",
    "SEL_VOLUME_FLOOR",
    "EXE_MARKETABLE_LIMIT",
    "EXE_QUOTE_DISPOSITION",
)
_UNSUPPORTED_SLOT_REASONS = (
    ("ACC_CASH_RESERVATION", "ZERO_ORDER_ACCOUNTING_NOT_OBSERVED"),
    ("ACC_DQ_PIT_REPRO", "LOCAL_DQ_GATE_REQUIRED"),
    ("ACC_FEE_SCHEDULE", "ZERO_ORDER_ACCOUNTING_NOT_OBSERVED"),
    ("ACC_RESULT_INCLUSION", "LOCAL_ADMISSION_REQUIRED"),
    ("ACC_SAMPLE_COVERAGE", "LOCAL_ADMISSION_REQUIRED"),
    ("ACC_SIZING_EXPOSURE", "ZERO_ORDER_ACCOUNTING_NOT_OBSERVED"),
    ("LIFE_EXPIRY_EXIT_GUARD", "ZERO_ORDER_LIFECYCLE_NOT_OBSERVED"),
    ("LIFE_TERMINAL_VALUATION", "ZERO_ORDER_LIFECYCLE_NOT_OBSERVED"),
    ("SEL_QUOTE_FRESHNESS", "DAILY_DATA_HAS_NO_INTRADAY_QUOTE_AGE"),
)
_EXPECTED_SERIES_IDS = (
    "S01_DELTA_RANGE",
    "S02_DTE_WINDOW",
    "S03_MONEYNESS_RANGE",
    "S04_OPEN_INTEREST",
    "S05_RANK_PRIORITY",
    "S06_SPREAD_RANGE",
    "S07_VOLUME_RANGE",
    "S08_ASK_RANGE",
    "S09_QUOTE_DISPOSITION_A",
    "S10_QUOTE_DISPOSITION_B",
)
_COUNT_STATISTIC_IDS = frozenset(
    {
        "candidate_count",
        "deterministic_tie_count",
        "missing_quote_count",
        "one_sided_quote_count",
        "two_sided_quote_count",
        "open_interest_max",
        "open_interest_min_nonzero",
        "volume_max",
        "volume_min_nonzero",
        "dte_days_max",
        "dte_days_min",
    }
)
_POSITIVE_STATISTIC_IDS = frozenset(
    {
        "moneyness_ratio_max",
        "moneyness_ratio_min",
        "open_interest_max",
        "open_interest_min_nonzero",
        "volume_max",
        "volume_min_nonzero",
        "ask_price_max",
        "ask_price_min",
    }
)
_NONNEGATIVE_STATISTIC_IDS = frozenset(
    {
        "dte_days_max",
        "dte_days_min",
        "candidate_count",
        "deterministic_tie_count",
        "relative_spread_max",
        "relative_spread_min",
        "missing_quote_count",
        "one_sided_quote_count",
        "two_sided_quote_count",
    }
)

_ALLOWED_EXTERNAL_ACTIONS = (
    "QUANTCONNECT_LOGIN",
    "MODIFY_EXISTING_DEDICATED_PROJECT_ONCE",
    "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST",
    "EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION",
)
_PROHIBITED_EXTERNAL_ACTIONS = (
    "API",
    "BROKER",
    "CLI",
    "HTTP",
    "INVESTMENT_INTERPRETATION",
    "LIVE",
    "OBJECT_STORE",
    "PAPER",
    "PRODUCTION",
    "PURCHASE_OR_SUBSCRIPTION",
    "RAW_OPTIONS_DATA_DOWNLOAD",
    "RAW_OPTION_ROW_LOGGING_OR_EXPORT",
    "SECOND_CLOUD_BACKTEST",
)
_PROHIBITED_RESULT_MARKER_KEYS = frozenset(
    {
        "datasetrows",
        "logs",
        "logdata",
        "objectstore",
        "optionrows",
        "rawoptionrows",
        "rawoptionsdata",
        "rawrows",
    }
)


class QCQQQOptionsDerivedAggregateCollectorError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, use_enum_values=False)


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


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase 40-character Git SHA")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field} must use a timezone-aware UTC value")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _canonical_sha256(payload: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def _duplicate_key_rejecting_json(raw: bytes) -> object:
    def pairs_hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite JSON constant is prohibited: {value}")

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs_hook,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not UTF-8 JSON") from exc


def _require_relative_path(value: str, field: str) -> str:
    checked = _required_text(value, field)
    path = Path(checked)
    if path.is_absolute() or path.drive or ".." in path.parts or path.as_posix() != checked:
        raise ValueError(f"{field} must be a normalized project-relative path")
    return checked


def _bound_path(path: Path, *, root: Path, field: str, must_exist: bool) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the configured root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes the configured root")
    current = resolved_root
    for part in relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot use a symlink")
    if must_exist and not candidate.is_file():
        raise ValueError(f"{field} must be a regular file")
    return candidate


def _trading_sessions(start: date, end: date) -> tuple[date, ...]:
    sessions: list[date] = []
    current = start
    while current <= end:
        if us_equity_market_session(current).is_trading_day:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def _format_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _decimal(value: object, field: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise ValueError(f"{field} must be a finite decimal-compatible value")
    rendered = str(value)
    try:
        parsed = Decimal(rendered)
    except InvalidOperation as exc:
        raise ValueError(f"{field} must be finite") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field} must be finite")
    return parsed


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_sha256(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _canonical_sha256(self.semantic_payload())

    @model_validator(mode="after")
    def _validate_content_seal(self, info: ValidationInfo) -> Self:
        allow_unsealed = bool(info.context and info.context.get("collector_allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("semantic content SHA-256 mismatch")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def seal(cls, **payload: object) -> Self:
        if "content_sha256" in payload:
            raise QCQQQOptionsDerivedAggregateCollectorError(
                "COLLECTOR_PAYLOAD_MISMATCH",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        try:
            candidate = cls.model_validate(
                {**payload, "content_sha256": _UNSEALED_SHA256},
                context={"collector_allow_unsealed": True},
            )
            return cls.model_validate(
                {**payload, "content_sha256": candidate.compute_content_sha256()}
            )
        except (TypeError, ValueError) as exc:
            raise QCQQQOptionsDerivedAggregateCollectorError(
                "COLLECTOR_PAYLOAD_MISMATCH", str(exc)
            ) from exc

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            payload = _duplicate_key_rejecting_json(raw)
            if not isinstance(payload, dict):
                raise TypeError("record JSON root must be an object")
            value = cls.model_validate_json(raw)
            if raw != value.canonical_bytes:
                raise ValueError("record is not canonical JSON bytes")
            return value
        except QCQQQOptionsDerivedAggregateCollectorError:
            raise
        except (TypeError, ValueError) as exc:
            raise QCQQQOptionsDerivedAggregateCollectorError(
                "COLLECTOR_RECORD_INVALID", str(exc)
            ) from exc


class CollectorUpstreamGenerator(_PolicyModel):
    policy_path: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    implementation_path: str
    implementation_file_sha256: str

    @field_validator("policy_path", "implementation_path")
    @classmethod
    def _validate_paths(cls, value: str, info: ValidationInfo) -> str:
        return _require_relative_path(value, str(info.field_name))

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "implementation_file_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class CollectorPlatformConstraints(_PolicyModel):
    organization_tier: Literal["FREE"]
    max_custom_series: Literal[10]
    max_points_per_series: Literal[4000]
    max_primary_sessions: Literal[2000]
    log_bytes_per_backtest: Literal[10240]
    logs_as_data_carrier_allowed: Literal[False]
    object_store_allowed: Literal[False]
    result_carrier: Literal["MANUAL_DOWNLOAD_RESULTS_JSON"]
    result_carrier_includes: tuple[
        Literal["charts"], Literal["orders"], Literal["runtime_statistics"]
    ]
    result_download_requires_separate_owner_authorization: Literal[True]
    official_sources: tuple[str, ...]

    @model_validator(mode="after")
    def _validate_sources(self) -> Self:
        if self.result_carrier_includes != ("charts", "orders", "runtime_statistics"):
            raise ValueError("result carrier inventory drifted")
        if len(self.official_sources) != 4 or len(set(self.official_sources)) != 4:
            raise ValueError("official source inventory must contain exact four unique URLs")
        if any(not value.startswith("https://") for value in self.official_sources):
            raise ValueError("official sources must use HTTPS")
        return self


class CollectorStatisticMapping(_PolicyModel):
    ordinal_second: int = Field(ge=1, le=2)
    slot_id: str
    statistic_id: str
    unit_id: str

    @field_validator("slot_id", "statistic_id", "unit_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))


class CollectorSeriesMapping(_PolicyModel):
    series_id: str
    unit_id: str
    points_per_session: int = Field(ge=1, le=2)
    mappings: tuple[CollectorStatisticMapping, ...]

    @field_validator("series_id", "unit_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_mappings(self) -> Self:
        ordinals = tuple(item.ordinal_second for item in self.mappings)
        if ordinals != tuple(range(1, self.points_per_session + 1)):
            raise ValueError("series ordinal inventory must be contiguous and exact")
        if any(item.unit_id != self.unit_id for item in self.mappings):
            raise ValueError("series and statistic units differ")
        return self


class CollectorTransport(_PolicyModel):
    chart_id: Literal["TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1"]
    algorithm_time_zone: Literal["America/New_York"]
    point_local_hour: Literal[12]
    point_local_minute: Literal[0]
    decimal_places: Literal[12]
    expected_series: tuple[CollectorSeriesMapping, ...]

    @model_validator(mode="after")
    def _validate_transport(self) -> Self:
        series_ids = tuple(item.series_id for item in self.expected_series)
        if series_ids != _EXPECTED_SERIES_IDS:
            raise ValueError("transport series inventory or order drifted")
        pairs = tuple(
            (mapping.slot_id, mapping.statistic_id)
            for series in self.expected_series
            for mapping in series.mappings
        )
        if len(pairs) != 19 or len(set(pairs)) != 19:
            raise ValueError("transport must map exact 19 unique slot/statistic pairs")
        if tuple(dict.fromkeys(slot for slot, _ in pairs)) != _SUPPORTED_SLOT_IDS:
            raise ValueError("transport supported slot order drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


class CollectorUnsupportedSlot(_PolicyModel):
    slot_id: str
    reason_code: str

    @field_validator("slot_id", "reason_code")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))


class CollectorSafety(_PolicyModel):
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    external_action_authorized: Literal[False]
    investment_interpretation_allowed: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCQQQOptionsDerivedAggregateCollectorPolicy(_PolicyModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_export_safe_derived_aggregate_collector_policy.v1"
    ]
    policy_id: Literal["qc_qqq_options_primary_window_export_safe_derived_aggregate_collector_v1"]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["OWNER_REVIEW_REQUIRED_ENGINEERING_BASELINE"]
    task_id: Literal[
        "TRADING-2512_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EXPORT_SAFE_DERIVED_AGGREGATE_COLLECTOR_CONTRACT_V1"
    ]
    primary_research_start: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    provider_id: Literal["QuantConnect"]
    dataset_id: Literal["US_EQUITY_OPTIONS_DAILY_DERIVED_AGGREGATES"]
    upstream_generator: CollectorUpstreamGenerator
    platform_constraints: CollectorPlatformConstraints
    transport: CollectorTransport
    supported_slots: tuple[str, ...]
    unsupported_slots: tuple[CollectorUnsupportedSlot, ...]
    safety: CollectorSafety

    @model_validator(mode="after")
    def _validate_policy(self) -> Self:
        if self.primary_research_start != _PRIMARY_RESEARCH_START:
            raise ValueError("PRIMARY research start must remain 2021-02-22")
        if self.supported_slots != _SUPPORTED_SLOT_IDS:
            raise ValueError("supported slot inventory or order drifted")
        unsupported = tuple((item.slot_id, item.reason_code) for item in self.unsupported_slots)
        if unsupported != _UNSUPPORTED_SLOT_REASONS:
            raise ValueError("unsupported slot taxonomy drifted")
        all_slots = {slot for slot, _ in _UNSUPPORTED_SLOT_REASONS} | set(self.supported_slots)
        if len(all_slots) != 18:
            raise ValueError("supported and unsupported inventories must cover exact 18 slots")
        constraints = self.platform_constraints
        if len(self.transport.expected_series) != constraints.max_custom_series:
            raise ValueError("transport must consume exact reviewed custom-series quota")
        if constraints.max_primary_sessions * 2 != constraints.max_points_per_series:
            raise ValueError("primary session quota is not derived from the two-point series limit")
        return self

    @property
    def canonical_sha256(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


@dataclass(frozen=True)
class QCQQQOptionsDerivedAggregateCollectorPolicyLoadResult:
    policy: QCQQQOptionsDerivedAggregateCollectorPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str


class QCQQQOptionsDerivedAggregateCollectorRunScope(_SealedModel):
    schema_version: Literal["qc_qqq_options_derived_aggregate_collector_run_scope.v1"]
    run_scope_id: str
    created_at_utc: datetime
    repository_code_sha: str
    target_project_id: int = Field(ge=1)
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    session_ids: tuple[date, ...]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    raw_option_rows_allowed: Literal[False]
    log_data_carrier_allowed: Literal[False]
    object_store_allowed: Literal[False]

    @field_validator("run_scope_id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return _identifier(value, "run_scope_id")

    @field_validator("created_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @model_validator(mode="after")
    def _validate_scope(self) -> Self:
        if self.requested_start != _PRIMARY_RESEARCH_START:
            raise ValueError("PRIMARY requested_start must remain 2021-02-22")
        if self.evaluated_start != _PRIMARY_RESEARCH_START:
            raise ValueError("PRIMARY evaluated_start must remain 2021-02-22")
        if (
            self.requested_end != self.evaluated_end
            or self.requested_start != self.evaluated_start
            or self.requested_end < self.requested_start
        ):
            raise ValueError("collector requested and evaluated ranges must match exactly")
        expected = _trading_sessions(self.evaluated_start, self.evaluated_end)
        if not expected or self.session_ids != expected:
            raise ValueError("session inventory differs from reviewed XNYS calendar")
        return self


@dataclass(frozen=True)
class RenderedQCCollectorProject:
    code_bytes: bytes
    code_lf_sha256: str
    code_lf_byte_count: int


class QCQQQOptionsDerivedAggregateCollectorProposal(_SealedModel):
    schema_version: Literal["qc_qqq_options_derived_aggregate_collector_proposal.v1"]
    proposal_id: str
    issued_at_utc: datetime
    run_scope: QCQQQOptionsDerivedAggregateCollectorRunScope
    collector_policy_file_sha256: str
    collector_policy_canonical_sha256: str
    transport_map_sha256: str
    project_code_lf_sha256: str
    project_code_lf_byte_count: int = Field(ge=1)
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    decision: Literal["OWNER_AUTHORIZATION_REQUIRED"]
    allowed_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("proposal_id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        return _identifier(value, "proposal_id")

    @field_validator("issued_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "issued_at_utc")

    @field_validator(
        "collector_policy_file_sha256",
        "collector_policy_canonical_sha256",
        "transport_map_sha256",
        "project_code_lf_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_actions(self) -> Self:
        if self.allowed_actions != _ALLOWED_EXTERNAL_ACTIONS:
            raise ValueError("proposal allowed-action inventory drifted")
        if self.prohibited_actions != _PROHIBITED_EXTERNAL_ACTIONS:
            raise ValueError("proposal prohibited-action inventory drifted")
        return self


class QCQQQOptionsDerivedAggregateCollectorAuthorization(_SealedModel):
    schema_version: Literal["qc_qqq_options_derived_aggregate_collector_authorization.v1"]
    owner_decision_token: str
    authorized_at_utc: datetime
    expires_at_utc: datetime
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
    proposal_content_sha256: str
    run_scope_content_sha256: str
    repository_code_sha: str
    target_project_id: int = Field(ge=1)
    project_code_lf_sha256: str
    collector_policy_file_sha256: str
    collector_policy_canonical_sha256: str
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    allowed_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    collector_id: str
    independent_reviewer_id: str

    @field_validator("owner_decision_token", "collector_id", "independent_reviewer_id")
    @classmethod
    def _validate_texts(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("authorized_at_utc", "expires_at_utc")
    @classmethod
    def _validate_times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "proposal_content_sha256",
        "run_scope_content_sha256",
        "project_code_lf_sha256",
        "collector_policy_file_sha256",
        "collector_policy_canonical_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_authorization(self) -> Self:
        if self.expires_at_utc <= self.authorized_at_utc:
            raise ValueError("authorization expiry must follow authorization time")
        if self.allowed_actions != _ALLOWED_EXTERNAL_ACTIONS:
            raise ValueError("authorization allowed-action inventory drifted")
        if self.prohibited_actions != _PROHIBITED_EXTERNAL_ACTIONS:
            raise ValueError("authorization prohibited-action inventory drifted")
        return self


class QCQQQOptionsDerivedAggregateCollectorEvidence(_SealedModel):
    schema_version: Literal["qc_qqq_options_derived_aggregate_collector_evidence.v1"]
    evidence_id: str
    collected_at_utc: datetime
    repository_code_sha: str
    target_project_id: int = Field(ge=1)
    backtest_id: str
    proposal_content_sha256: str
    authorization_content_sha256: str
    run_scope_content_sha256: str
    collector_policy_file_sha256: str
    collector_policy_canonical_sha256: str
    transport_map_sha256: str
    reviewed_project_code_lf_sha256: str
    result_file_sha256: str
    result_payload_sha256: str
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    session_ids: tuple[date, ...]
    supported_slot_ids: tuple[str, ...]
    unsupported_slots: tuple[CollectorUnsupportedSlot, ...]
    observations: tuple[generator_v1.DerivedSlotSessionObservation, ...]
    dq_status: Literal["NOT_EVALUATED_PENDING_LOCAL_DQ_GATE"]
    decision: Literal["RESULT_PARSED_DQ_NOT_EVALUATED"]
    orders: Literal[0]
    fills: Literal[0]
    raw_option_rows_exported: Literal[False]
    log_data_carrier_used: Literal[False]
    object_store_used: Literal[False]
    external_action_performed: Literal[True]
    investment_interpretation_generated: Literal[False]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("evidence_id", "backtest_id")
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("collected_at_utc")
    @classmethod
    def _validate_time(cls, value: datetime) -> datetime:
        return _utc(value, "collected_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "proposal_content_sha256",
        "authorization_content_sha256",
        "run_scope_content_sha256",
        "collector_policy_file_sha256",
        "collector_policy_canonical_sha256",
        "transport_map_sha256",
        "reviewed_project_code_lf_sha256",
        "result_file_sha256",
        "result_payload_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_evidence(self) -> Self:
        if self.requested_start != _PRIMARY_RESEARCH_START:
            raise ValueError("evidence PRIMARY start drifted")
        if (self.requested_start, self.requested_end) != (
            self.evaluated_start,
            self.evaluated_end,
        ):
            raise ValueError("evidence requested/evaluated range mismatch")
        expected_sessions = _trading_sessions(self.evaluated_start, self.evaluated_end)
        if self.session_ids != expected_sessions:
            raise ValueError("evidence session inventory differs from XNYS calendar")
        if self.supported_slot_ids != _SUPPORTED_SLOT_IDS:
            raise ValueError("evidence supported slots drifted")
        unsupported = tuple((item.slot_id, item.reason_code) for item in self.unsupported_slots)
        if unsupported != _UNSUPPORTED_SLOT_REASONS:
            raise ValueError("evidence unsupported slots drifted")
        keys = tuple((item.slot_id, item.session_id) for item in self.observations)
        expected_keys = tuple(
            sorted(
                (slot_id, session_id)
                for slot_id in self.supported_slot_ids
                for session_id in self.session_ids
            )
        )
        if keys != expected_keys:
            raise ValueError("evidence observation inventory is incomplete or unsorted")
        return self


def load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
    path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EXPORT_SAFE_DERIVED_AGGREGATE_COLLECTOR_POLICY_PATH
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDerivedAggregateCollectorPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _bound_path(path, root=root, field="collector policy", must_exist=True)
        raw = policy_path.read_bytes()
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("collector policy root must be a mapping")
        policy = QCQQQOptionsDerivedAggregateCollectorPolicy.model_validate(payload, strict=False)
        upstream = generator_v1.load_qqq_options_derived_calibration_evidence_generator_policy(
            project_root=root
        )
        declared = policy.upstream_generator
        implementation_path = _bound_path(
            Path(declared.implementation_path),
            root=root,
            field="upstream implementation",
            must_exist=True,
        )
        if (
            declared.policy_path != upstream.policy_path.relative_to(root).as_posix()
            or declared.policy_file_sha256 != upstream.policy_file_sha256
            or declared.policy_canonical_sha256 != upstream.policy_canonical_sha256
            or hashlib.sha256(implementation_path.read_bytes()).hexdigest()
            != declared.implementation_file_sha256
        ):
            raise ValueError("2511 upstream generator authority drifted")
    except QCQQQOptionsDerivedAggregateCollectorError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDerivedAggregateCollectorError(
            "COLLECTOR_AUTHORITY_BINDING_MISMATCH", str(exc)
        ) from exc
    return QCQQQOptionsDerivedAggregateCollectorPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
    )


def build_qc_qqq_options_derived_aggregate_collector_run_scope(
    *,
    run_scope_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    target_project_id: int,
    requested_end: date,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDerivedAggregateCollectorRunScope:
    loaded = load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=project_root
    )
    sessions = _trading_sessions(_PRIMARY_RESEARCH_START, requested_end)
    if len(sessions) > loaded.policy.platform_constraints.max_primary_sessions:
        raise QCQQQOptionsDerivedAggregateCollectorError(
            "COLLECTOR_SESSION_QUOTA_EXCEEDED",
            "requested primary-window sessions exceed the reviewed Free chart transport quota",
        )
    return QCQQQOptionsDerivedAggregateCollectorRunScope.seal(
        schema_version="qc_qqq_options_derived_aggregate_collector_run_scope.v1",
        run_scope_id=run_scope_id,
        created_at_utc=created_at_utc,
        repository_code_sha=repository_code_sha,
        target_project_id=target_project_id,
        requested_start=_PRIMARY_RESEARCH_START,
        requested_end=requested_end,
        evaluated_start=_PRIMARY_RESEARCH_START,
        evaluated_end=requested_end,
        primary_research_role="PRIMARY",
        exchange_calendar="XNYS",
        session_ids=sessions,
        maximum_orders=0,
        maximum_fills=0,
        raw_option_rows_allowed=False,
        log_data_carrier_allowed=False,
        object_store_allowed=False,
    )


def _render_project_code_text(
    *,
    run_scope: QCQQQOptionsDerivedAggregateCollectorRunScope,
    loaded: QCQQQOptionsDerivedAggregateCollectorPolicyLoadResult,
) -> str:
    policy = loaded.policy
    start = run_scope.requested_start
    end = run_scope.requested_end
    sessions_literal = repr(tuple(item.isoformat() for item in run_scope.session_ids))
    identity = (
        "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1"
        f"|scope={run_scope.content_sha256}"
        f"|repository={run_scope.repository_code_sha}"
        f"|policy_file={loaded.policy_file_sha256}"
        f"|policy_canonical={loaded.policy_canonical_sha256}"
        f"|transport={policy.transport.canonical_sha256}"
    )
    template = """from AlgorithmImports import *
from datetime import datetime
import math

# TRADING-2512 export-safe DAILY derived aggregate collection only.
# No policy thresholds, orders, fills, raw-row output, logs-as-data, Object Store, or network.

SCHEMA_VERSION = "qc_qqq_options_derived_aggregate_collector_runtime.v1"
CHART_ID = "__CHART_ID__"
EXPECTED_SESSIONS = __EXPECTED_SESSIONS__
IDENTITY = "__IDENTITY__"
DECIMAL_PLACES = 12


class QQQOptionsPrimaryWindowDerivedAggregateCollector(QCAlgorithm):
    def initialize(self):
        self.set_start_date(__START_YEAR__, __START_MONTH__, __START_DAY__)
        self.set_end_date(__END_YEAR__, __END_MONTH__, __END_DAY__)
        self.set_cash(100000)
        self.set_time_zone(TimeZones.NEW_YORK)
        self._equity = self.add_equity(
            "QQQ", Resolution.DAILY, data_normalization_mode=DataNormalizationMode.RAW
        ).symbol
        option = self.add_option("QQQ", Resolution.DAILY)
        option.set_filter(lambda universe: universe.contracts(lambda symbols: symbols))
        self._option = option.symbol
        self._expected_sessions = set(EXPECTED_SESSIONS)
        self._seen_sessions = set()
        self._invalid_sessions = set()
        self._series = {}
        units = {
            "S01_DELTA_RANGE": "ratio",
            "S02_DTE_WINDOW": "days",
            "S03_MONEYNESS_RANGE": "ratio",
            "S04_OPEN_INTEREST": "contracts",
            "S05_RANK_PRIORITY": "contracts",
            "S06_SPREAD_RANGE": "ratio",
            "S07_VOLUME_RANGE": "contracts",
            "S08_ASK_RANGE": "usd",
            "S09_QUOTE_DISPOSITION_A": "quotes",
            "S10_QUOTE_DISPOSITION_B": "quotes",
        }
        chart = Chart(CHART_ID)
        for series_id, unit in units.items():
            series = Series(series_id, SeriesType.SCATTER, unit)
            chart.add_series(series)
            self._series[series_id] = series
        self.add_chart(chart)
        self.schedule.on(
            self.date_rules.every_day(self._equity),
            self.time_rules.after_market_open(self._equity, 1),
            self._collect_session,
        )

    @staticmethod
    def _finite(value):
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        return result if math.isfinite(result) else None

    @staticmethod
    def _transport(value):
        return round(float(value), DECIMAL_PLACES)

    @staticmethod
    def _attribute(item, *names):
        for name in names:
            if hasattr(item, name):
                return getattr(item, name)
        return None

    def _plot_pair(self, series_id, session, first, second=None):
        first_time = datetime(session.year, session.month, session.day, 12, 0, 1)
        self._series[series_id].add_point(first_time, self._transport(first))
        if second is not None:
            second_time = datetime(session.year, session.month, session.day, 12, 0, 2)
            self._series[series_id].add_point(second_time, self._transport(second))

    def _collect_session(self):
        session = self.time.date()
        session_id = session.isoformat()
        if session_id not in self._expected_sessions or session_id in self._seen_sessions:
            self._invalid_sessions.add(session_id)
            return
        chain = list(self.option_chain(self._option))
        if not chain:
            self._invalid_sessions.add(session_id)
            return
        missing_quotes = 0
        one_sided_quotes = 0
        two_sided_quotes = 0
        candidates = []
        for contract in chain:
            bid = self._finite(self._attribute(contract, "bid_price", "bidprice"))
            ask = self._finite(self._attribute(contract, "ask_price", "askprice"))
            bid_positive = bid is not None and bid > 0
            ask_positive = ask is not None and ask > 0
            if bid_positive and ask_positive and ask >= bid:
                two_sided_quotes += 1
            elif bid_positive or ask_positive:
                one_sided_quotes += 1
                continue
            else:
                missing_quotes += 1
                continue
            underlying = self._finite(self._attribute(contract, "underlying"))
            strike = self._finite(self._attribute(contract, "strike"))
            if strike is None and hasattr(contract, "symbol"):
                strike = self._finite(contract.symbol.id.strike_price)
            expiry = self._attribute(contract, "expiry")
            if expiry is None and hasattr(contract, "symbol"):
                expiry = contract.symbol.id.date
            delta = None
            if hasattr(contract, "greeks"):
                delta = self._finite(contract.greeks.delta)
            open_interest = self._finite(self._attribute(contract, "open_interest", "openinterest"))
            volume = self._finite(self._attribute(contract, "volume"))
            if (
                underlying is None or underlying <= 0
                or strike is None or strike <= 0
            or (not hasattr(expiry, "date") and not hasattr(expiry, "year"))
                or delta is None
                or open_interest is None or open_interest < 0
                or volume is None or volume < 0
            ):
                continue
            expiry_date = expiry.date() if hasattr(expiry, "date") else expiry
            dte = (expiry_date - session).days
            if dte < 0:
                continue
            midpoint = (bid + ask) / 2
            if midpoint <= 0:
                continue
            moneyness = strike / underlying
            relative_spread = (ask - bid) / midpoint
            if not math.isfinite(moneyness) or not math.isfinite(relative_spread):
                continue
            vector = (
                self._transport(delta),
                dte,
                self._transport(moneyness),
                self._transport(relative_spread),
                int(open_interest),
                int(volume),
                self._transport(ask),
            )
            candidates.append(vector)
        if not candidates:
            self._invalid_sessions.add(session_id)
            return
        delta_values = [item[0] for item in candidates]
        dte_values = [item[1] for item in candidates]
        moneyness_values = [item[2] for item in candidates]
        spread_values = [item[3] for item in candidates]
        oi_values = [item[4] for item in candidates]
        volume_values = [item[5] for item in candidates]
        ask_values = [item[6] for item in candidates]
        positive_oi_values = [value for value in oi_values if value > 0]
        positive_volume_values = [value for value in volume_values if value > 0]
        if not positive_oi_values or not positive_volume_values:
            self._invalid_sessions.add(session_id)
            return
        tie_count = len(candidates) - len(set(candidates))
        self._plot_pair("S01_DELTA_RANGE", session, max(delta_values), min(delta_values))
        self._plot_pair("S02_DTE_WINDOW", session, max(dte_values), min(dte_values))
        self._plot_pair(
            "S03_MONEYNESS_RANGE", session, max(moneyness_values), min(moneyness_values)
        )
        self._plot_pair(
            "S04_OPEN_INTEREST",
            session,
            max(oi_values),
            min(positive_oi_values),
        )
        self._plot_pair("S05_RANK_PRIORITY", session, len(candidates), tie_count)
        self._plot_pair("S06_SPREAD_RANGE", session, max(spread_values), min(spread_values))
        self._plot_pair(
            "S07_VOLUME_RANGE",
            session,
            max(volume_values),
            min(positive_volume_values),
        )
        self._plot_pair("S08_ASK_RANGE", session, max(ask_values), min(ask_values))
        self._plot_pair("S09_QUOTE_DISPOSITION_A", session, missing_quotes, one_sided_quotes)
        self._plot_pair("S10_QUOTE_DISPOSITION_B", session, two_sided_quotes)
        self._seen_sessions.add(session_id)

    def on_order_event(self, order_event):
        self._invalid_sessions.add("ORDER_EVENT_PROHIBITED")

    def on_end_of_algorithm(self):
        if self.portfolio.invested:
            self._invalid_sessions.add("PORTFOLIO_INVESTED_PROHIBITED")
        complete = self._seen_sessions == self._expected_sessions and not self._invalid_sessions
        status = "COMPLETE" if complete else "INVALID_INCOMPLETE"
        self.set_runtime_statistic("TRADING2512_IDENTITY", IDENTITY)
        self.set_runtime_statistic(
            "TRADING2512_TERMINAL",
            "status=" + status
            + "|observed_sessions=" + str(len(self._seen_sessions))
            + "|invalid_sessions=" + str(len(self._invalid_sessions))
            + "|orders=0|fills=0|portfolio_invested=false"
            + "|raw_rows=false|log_data=false|object_store=false",
        )
"""
    replacements = {
        "__CHART_ID__": policy.transport.chart_id,
        "__EXPECTED_SESSIONS__": sessions_literal,
        "__IDENTITY__": identity,
        "__START_YEAR__": str(start.year),
        "__START_MONTH__": str(start.month),
        "__START_DAY__": str(start.day),
        "__END_YEAR__": str(end.year),
        "__END_MONTH__": str(end.month),
        "__END_DAY__": str(end.day),
    }
    rendered = template
    for marker, value in replacements.items():
        rendered = rendered.replace(marker, value)
    if "__" in rendered:
        raise QCQQQOptionsDerivedAggregateCollectorError(
            "COLLECTOR_PROJECT_TEMPLATE_INVALID", "unresolved project-code marker"
        )
    return rendered.replace("\r\n", "\n").replace("\r", "\n")


def render_qc_qqq_options_primary_window_derived_aggregate_collector_project(
    *,
    run_scope: QCQQQOptionsDerivedAggregateCollectorRunScope,
    project_root: Path = PROJECT_ROOT,
) -> RenderedQCCollectorProject:
    loaded = load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=project_root
    )
    if len(run_scope.session_ids) > loaded.policy.platform_constraints.max_primary_sessions:
        raise QCQQQOptionsDerivedAggregateCollectorError(
            "COLLECTOR_SESSION_QUOTA_EXCEEDED", "run scope exceeds chart point quota"
        )
    text = _render_project_code_text(run_scope=run_scope, loaded=loaded)
    code_bytes = text.encode("utf-8")
    return RenderedQCCollectorProject(
        code_bytes=code_bytes,
        code_lf_sha256=hashlib.sha256(code_bytes).hexdigest(),
        code_lf_byte_count=len(code_bytes),
    )


def build_qc_qqq_options_derived_aggregate_collector_proposal(
    *,
    proposal_id: str,
    issued_at_utc: datetime,
    run_scope: QCQQQOptionsDerivedAggregateCollectorRunScope,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDerivedAggregateCollectorProposal:
    loaded = load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=project_root
    )
    rendered = render_qc_qqq_options_primary_window_derived_aggregate_collector_project(
        run_scope=run_scope, project_root=project_root
    )
    return QCQQQOptionsDerivedAggregateCollectorProposal.seal(
        schema_version="qc_qqq_options_derived_aggregate_collector_proposal.v1",
        proposal_id=proposal_id,
        issued_at_utc=issued_at_utc,
        run_scope=run_scope,
        collector_policy_file_sha256=loaded.policy_file_sha256,
        collector_policy_canonical_sha256=loaded.policy_canonical_sha256,
        transport_map_sha256=loaded.policy.transport.canonical_sha256,
        project_code_lf_sha256=rendered.code_lf_sha256,
        project_code_lf_byte_count=rendered.code_lf_byte_count,
        maximum_project_mutations=1,
        maximum_cloud_backtests=1,
        maximum_orders=0,
        maximum_fills=0,
        authorization_status="NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS",
        decision="OWNER_AUTHORIZATION_REQUIRED",
        allowed_actions=_ALLOWED_EXTERNAL_ACTIONS,
        prohibited_actions=_PROHIBITED_EXTERNAL_ACTIONS,
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )


def _mapping_payload(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field} must be an object")
    return value


def _reject_prohibited_result_markers(value: object, *, path: str = "result") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).casefold())
            if normalized in _PROHIBITED_RESULT_MARKER_KEYS:
                raise ValueError(f"prohibited result marker present: {path}.{key}")
            _reject_prohibited_result_markers(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_prohibited_result_markers(child, path=f"{path}[{index}]")


def _zero_currency(value: object, field: str) -> None:
    if not isinstance(value, (int, float, str, Decimal)) or isinstance(value, bool):
        raise ValueError(f"{field} must be a zero monetary value")
    rendered = str(value).strip().replace("$", "").replace(",", "")
    if _decimal(rendered, field) != 0:
        raise ValueError(f"{field} must be zero")


def _parse_iso_datetime_date(value: object, field: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO timestamp")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO timestamp") from exc


def _runtime_identity(
    *,
    proposal: QCQQQOptionsDerivedAggregateCollectorProposal,
) -> str:
    return (
        "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1"
        f"|scope={proposal.run_scope.content_sha256}"
        f"|repository={proposal.run_scope.repository_code_sha}"
        f"|policy_file={proposal.collector_policy_file_sha256}"
        f"|policy_canonical={proposal.collector_policy_canonical_sha256}"
        f"|transport={proposal.transport_map_sha256}"
    )


def _validate_result_envelope(
    *,
    payload: dict[str, object],
    proposal: QCQQQOptionsDerivedAggregateCollectorProposal,
    authorization: QCQQQOptionsDerivedAggregateCollectorAuthorization,
    backtest_id: str,
) -> dict[str, object]:
    scope = proposal.run_scope
    if (
        authorization.proposal_content_sha256 != proposal.content_sha256
        or authorization.run_scope_content_sha256 != scope.content_sha256
        or authorization.repository_code_sha != scope.repository_code_sha
        or authorization.target_project_id != scope.target_project_id
        or authorization.project_code_lf_sha256 != proposal.project_code_lf_sha256
        or authorization.collector_policy_file_sha256 != proposal.collector_policy_file_sha256
        or authorization.collector_policy_canonical_sha256
        != proposal.collector_policy_canonical_sha256
    ):
        raise ValueError("authorization and proposal identity mismatch")
    algorithm = _mapping_payload(payload.get("algorithmConfiguration"), "algorithmConfiguration")
    if (
        _parse_iso_datetime_date(algorithm.get("startDate"), "startDate") != scope.requested_start
        or _parse_iso_datetime_date(algorithm.get("endDate"), "endDate") != scope.requested_end
    ):
        raise ValueError("result algorithm range differs from run scope")
    state = _mapping_payload(payload.get("state"), "state")
    if state.get("Status") != "Completed" or state.get("RuntimeError") not in ("", None):
        raise ValueError("backtest did not complete without runtime error")
    if str(state.get("OrderCount")) != "0":
        raise ValueError("backtest state reports orders")
    hostname = state.get("Hostname")
    if not isinstance(hostname, str) or not hostname.endswith(backtest_id):
        raise ValueError("backtest id is not bound by result hostname")
    orders = payload.get("orders")
    if not isinstance(orders, (dict, list)) or len(orders) != 0:
        raise ValueError("result orders inventory is not empty")
    statistics = _mapping_payload(payload.get("statistics"), "statistics")
    if str(statistics.get("Total Orders")) != "0" or statistics.get("Total Fees") != "$0.00":
        raise ValueError("result statistics report orders or fees")
    runtime = _mapping_payload(payload.get("runtimeStatistics"), "runtimeStatistics")
    if runtime.get("TRADING2512_IDENTITY") != _runtime_identity(proposal=proposal):
        raise ValueError("runtime identity mismatch")
    expected_terminal = (
        "status=COMPLETE"
        f"|observed_sessions={len(scope.session_ids)}"
        "|invalid_sessions=0|orders=0|fills=0|portfolio_invested=false"
        "|raw_rows=false|log_data=false|object_store=false"
    )
    if runtime.get("TRADING2512_TERMINAL") != expected_terminal:
        raise ValueError("runtime terminal status is incomplete or unsafe")
    for field in ("Volume", "Holdings", "Unrealized"):
        _zero_currency(runtime.get(field), f"runtime {field}")
    charts = _mapping_payload(payload.get("charts"), "charts")
    chart = _mapping_payload(charts.get(_CHART_ID), "collector chart")
    if chart.get("name") != _CHART_ID:
        raise ValueError("collector chart name mismatch")
    return chart


def _series_values(
    *,
    series_payload: dict[str, object],
    mapping: CollectorSeriesMapping,
    session_ids: tuple[date, ...],
    transport: CollectorTransport,
) -> dict[tuple[date, str, str], Decimal]:
    if series_payload.get("name") != mapping.series_id:
        raise ValueError(f"series name mismatch: {mapping.series_id}")
    if series_payload.get("unit") != mapping.unit_id:
        raise ValueError(f"series unit mismatch: {mapping.series_id}")
    series_type = series_payload.get("seriesType")
    if isinstance(series_type, bool) or not isinstance(series_type, int) or series_type != 1:
        raise ValueError(f"series type must be Scatter: {mapping.series_id}")
    values = series_payload.get("values")
    if not isinstance(values, list):
        raise ValueError(f"series values must be a list: {mapping.series_id}")
    expected_count = len(session_ids) * mapping.points_per_session
    if len(values) != expected_count:
        raise ValueError(f"series point count mismatch: {mapping.series_id}")
    zone = ZoneInfo(transport.algorithm_time_zone)
    result: dict[tuple[date, str, str], Decimal] = {}
    previous_epoch: int | None = None
    expected_items = tuple(
        (session_id, statistic) for session_id in session_ids for statistic in mapping.mappings
    )
    for raw_point, (expected_session, statistic) in zip(values, expected_items, strict=True):
        if (
            not isinstance(raw_point, list)
            or len(raw_point) != 2
            or isinstance(raw_point[0], bool)
            or not isinstance(raw_point[0], int)
        ):
            raise ValueError(f"series point shape mismatch: {mapping.series_id}")
        epoch = raw_point[0]
        if previous_epoch is not None and epoch <= previous_epoch:
            raise ValueError(f"series timestamps are not strictly increasing: {mapping.series_id}")
        previous_epoch = epoch
        local = datetime.fromtimestamp(epoch, tz=UTC).astimezone(zone)
        expected_local = datetime.combine(
            expected_session,
            time(
                transport.point_local_hour,
                transport.point_local_minute,
                statistic.ordinal_second,
            ),
            tzinfo=zone,
        )
        if local != expected_local:
            raise ValueError(f"series session/ordinal timestamp mismatch: {mapping.series_id}")
        value = _decimal(raw_point[1], f"{mapping.series_id}.{statistic.statistic_id}")
        exponent = value.as_tuple().exponent
        if not isinstance(exponent, int):
            raise ValueError(f"series value must be finite: {mapping.series_id}")
        if exponent < -transport.decimal_places:
            raise ValueError(f"series value exceeds transport precision: {mapping.series_id}")
        if statistic.statistic_id in _COUNT_STATISTIC_IDS and value != value.to_integral_value():
            raise ValueError(f"count statistic must be integral: {statistic.statistic_id}")
        if statistic.statistic_id in _POSITIVE_STATISTIC_IDS and value <= 0:
            raise ValueError(f"statistic must be positive: {statistic.statistic_id}")
        if statistic.statistic_id in _NONNEGATIVE_STATISTIC_IDS and value < 0:
            raise ValueError(f"statistic must be nonnegative: {statistic.statistic_id}")
        key = (expected_session, statistic.slot_id, statistic.statistic_id)
        if key in result:
            raise ValueError("duplicate decoded statistic")
        result[key] = value
    return result


def _validate_session_statistics(
    *,
    session_id: date,
    values: dict[tuple[date, str, str], Decimal],
    transport: CollectorTransport,
) -> tuple[generator_v1.DerivedSlotSessionObservation, ...]:
    def get(slot_id: str, statistic_id: str) -> Decimal:
        try:
            return values[(session_id, slot_id, statistic_id)]
        except KeyError as exc:
            raise ValueError(f"missing decoded statistic: {slot_id}/{statistic_id}") from exc

    candidate_count = get("SEL_RANK_PRIORITY", "candidate_count")
    tie_count = get("SEL_RANK_PRIORITY", "deterministic_tie_count")
    missing = get("EXE_QUOTE_DISPOSITION", "missing_quote_count")
    one_sided = get("EXE_QUOTE_DISPOSITION", "one_sided_quote_count")
    two_sided = get("EXE_QUOTE_DISPOSITION", "two_sided_quote_count")
    quote_count = missing + one_sided + two_sided
    if candidate_count <= 0 or quote_count <= 0:
        raise ValueError("session candidate and quote populations must be positive")
    if tie_count > candidate_count or two_sided < candidate_count:
        raise ValueError("session candidate/tie/quote populations are inconsistent")
    ranges = (
        ("SEL_DELTA_SOURCE_RANGE", "delta_max", "delta_min"),
        ("SEL_DTE_WINDOW", "dte_days_max", "dte_days_min"),
        ("SEL_MONEYNESS_RANGE", "moneyness_ratio_max", "moneyness_ratio_min"),
        ("SEL_OPEN_INTEREST_FLOOR", "open_interest_max", "open_interest_min_nonzero"),
        ("SEL_SPREAD_LIMIT", "relative_spread_max", "relative_spread_min"),
        ("SEL_VOLUME_FLOOR", "volume_max", "volume_min_nonzero"),
        ("EXE_MARKETABLE_LIMIT", "ask_price_max", "ask_price_min"),
    )
    for slot_id, maximum, minimum in ranges:
        if get(slot_id, maximum) < get(slot_id, minimum):
            raise ValueError(f"session max/min envelope is inverted: {slot_id}")
    slot_to_values: dict[str, list[tuple[str, Decimal]]] = {}
    for (point_session, slot_id, statistic_id), value in values.items():
        if point_session == session_id:
            slot_to_values.setdefault(slot_id, []).append((statistic_id, value))
    observations: list[generator_v1.DerivedSlotSessionObservation] = []
    for slot_id in _SUPPORTED_SLOT_IDS:
        sample_count = int(quote_count if slot_id == "EXE_QUOTE_DISPOSITION" else candidate_count)
        statistics = tuple(
            generator_v1.DerivedSessionStatistic(
                statistic_id=statistic_id,
                value=_format_decimal(value),
                unit_id=_statistic_unit(
                    transport=transport,
                    slot_id=slot_id,
                    statistic_id=statistic_id,
                ),
                sample_count=sample_count,
                is_policy_value=False,
            )
            for statistic_id, value in sorted(slot_to_values.get(slot_id, []))
        )
        observations.append(
            generator_v1.DerivedSlotSessionObservation(
                slot_id=slot_id,
                session_id=session_id,
                statistics=statistics,
                derived_export_safe=True,
                contains_raw_option_rows=False,
            )
        )
    return tuple(observations)


def _statistic_unit(*, transport: CollectorTransport, slot_id: str, statistic_id: str) -> str:
    for series in transport.expected_series:
        for mapping in series.mappings:
            if mapping.slot_id == slot_id and mapping.statistic_id == statistic_id:
                return mapping.unit_id
    raise ValueError(f"unknown statistic unit: {slot_id}/{statistic_id}")


def build_qc_qqq_options_primary_window_derived_aggregate_collector_evidence(
    *,
    evidence_id: str,
    collected_at_utc: datetime,
    backtest_id: str,
    result_bytes: bytes,
    proposal: QCQQQOptionsDerivedAggregateCollectorProposal,
    authorization: QCQQQOptionsDerivedAggregateCollectorAuthorization,
    reviewed_target_project_id: int,
    reviewed_project_code_lf_sha256: str,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDerivedAggregateCollectorEvidence:
    loaded = load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
        project_root=project_root
    )
    try:
        collected_at = _utc(collected_at_utc, "collected_at_utc")
        if (
            collected_at < authorization.authorized_at_utc
            or collected_at > authorization.expires_at_utc
        ):
            raise ValueError("evidence collection time is outside authorization window")
        if reviewed_target_project_id != authorization.target_project_id:
            raise ValueError("reviewed target-project precheck differs from authorization")
        if reviewed_project_code_lf_sha256 != authorization.project_code_lf_sha256:
            raise ValueError("reviewed project-code precheck differs from authorization")
        if (
            proposal.collector_policy_file_sha256 != loaded.policy_file_sha256
            or proposal.collector_policy_canonical_sha256 != loaded.policy_canonical_sha256
            or proposal.transport_map_sha256 != loaded.policy.transport.canonical_sha256
        ):
            raise ValueError("proposal collector authority drifted")
        parsed = _duplicate_key_rejecting_json(result_bytes)
        _reject_prohibited_result_markers(parsed)
        payload = _mapping_payload(parsed, "result")
        chart = _validate_result_envelope(
            payload=payload,
            proposal=proposal,
            authorization=authorization,
            backtest_id=_identifier(backtest_id, "backtest_id"),
        )
        series_payload = _mapping_payload(chart.get("series"), "collector chart series")
        if set(series_payload) != set(_EXPECTED_SERIES_IDS):
            raise ValueError("collector chart series inventory drifted")
        decoded: dict[tuple[date, str, str], Decimal] = {}
        for mapping in loaded.policy.transport.expected_series:
            current = _series_values(
                series_payload=_mapping_payload(
                    series_payload.get(mapping.series_id), mapping.series_id
                ),
                mapping=mapping,
                session_ids=proposal.run_scope.session_ids,
                transport=loaded.policy.transport,
            )
            if decoded.keys() & current.keys():
                raise ValueError("decoded statistic inventory collision")
            decoded.update(current)
        observations = tuple(
            item
            for session_id in proposal.run_scope.session_ids
            for item in _validate_session_statistics(
                session_id=session_id,
                values=decoded,
                transport=loaded.policy.transport,
            )
        )
        observations = tuple(sorted(observations, key=lambda item: (item.slot_id, item.session_id)))
        unsupported = tuple(
            CollectorUnsupportedSlot(slot_id=slot_id, reason_code=reason_code)
            for slot_id, reason_code in _UNSUPPORTED_SLOT_REASONS
        )
    except QCQQQOptionsDerivedAggregateCollectorError:
        raise
    except (KeyError, OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDerivedAggregateCollectorError(
            "COLLECTOR_RESULT_ADMISSION_REJECTED", str(exc)
        ) from exc
    scope = proposal.run_scope
    return QCQQQOptionsDerivedAggregateCollectorEvidence.seal(
        schema_version="qc_qqq_options_derived_aggregate_collector_evidence.v1",
        evidence_id=evidence_id,
        collected_at_utc=collected_at,
        repository_code_sha=scope.repository_code_sha,
        target_project_id=scope.target_project_id,
        backtest_id=backtest_id,
        proposal_content_sha256=proposal.content_sha256,
        authorization_content_sha256=authorization.content_sha256,
        run_scope_content_sha256=scope.content_sha256,
        collector_policy_file_sha256=loaded.policy_file_sha256,
        collector_policy_canonical_sha256=loaded.policy_canonical_sha256,
        transport_map_sha256=loaded.policy.transport.canonical_sha256,
        reviewed_project_code_lf_sha256=reviewed_project_code_lf_sha256,
        result_file_sha256=hashlib.sha256(result_bytes).hexdigest(),
        result_payload_sha256=_canonical_sha256(payload),
        requested_start=scope.requested_start,
        requested_end=scope.requested_end,
        evaluated_start=scope.evaluated_start,
        evaluated_end=scope.evaluated_end,
        session_ids=scope.session_ids,
        supported_slot_ids=_SUPPORTED_SLOT_IDS,
        unsupported_slots=unsupported,
        observations=observations,
        dq_status="NOT_EVALUATED_PENDING_LOCAL_DQ_GATE",
        decision="RESULT_PARSED_DQ_NOT_EVALUATED",
        orders=0,
        fills=0,
        raw_option_rows_exported=False,
        log_data_carrier_used=False,
        object_store_used=False,
        external_action_performed=True,
        investment_interpretation_generated=False,
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        production_effect="none",
        broker_action="none",
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EXPORT_SAFE_DERIVED_AGGREGATE_COLLECTOR_POLICY_PATH",
    "CollectorPlatformConstraints",
    "CollectorSafety",
    "CollectorSeriesMapping",
    "CollectorStatisticMapping",
    "CollectorTransport",
    "CollectorUnsupportedSlot",
    "CollectorUpstreamGenerator",
    "QCQQQOptionsDerivedAggregateCollectorAuthorization",
    "QCQQQOptionsDerivedAggregateCollectorError",
    "QCQQQOptionsDerivedAggregateCollectorEvidence",
    "QCQQQOptionsDerivedAggregateCollectorPolicy",
    "QCQQQOptionsDerivedAggregateCollectorPolicyLoadResult",
    "QCQQQOptionsDerivedAggregateCollectorProposal",
    "QCQQQOptionsDerivedAggregateCollectorRunScope",
    "RenderedQCCollectorProject",
    "build_qc_qqq_options_derived_aggregate_collector_proposal",
    "build_qc_qqq_options_primary_window_derived_aggregate_collector_evidence",
    "build_qc_qqq_options_derived_aggregate_collector_run_scope",
    "load_qc_qqq_options_primary_window_derived_aggregate_collector_policy",
    "render_qc_qqq_options_primary_window_derived_aggregate_collector_project",
]
