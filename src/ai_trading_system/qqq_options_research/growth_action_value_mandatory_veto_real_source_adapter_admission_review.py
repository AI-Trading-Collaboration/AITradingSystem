from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_synthetic_producer_contract as synthetic,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_REAL_SOURCE_ADMISSION_REVIEW_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "real_source_adapter_admission_review_v1.yaml"
)

_SYNTHETIC_PATH = (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "synthetic_producer_contract_v1.yaml"
)
_SYNTHETIC_FILE_SHA256 = (
    "14a8995e0bcb5cdc1a5fccb67d6389c5e72fb65ce1efdb926d1f9520e1d4d314"
)
_SYNTHETIC_CANONICAL_SHA256 = (
    "c064ec2418f43184e89fdecdf1ced60c932b15e5de6b6548fa01dc6af99ac95c"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)
_CANDIDATE_SURFACE = (
    (
        "fmp_spy_qqq_eod_adjusted_v1",
        "fmp_eod_daily_prices",
        "FmpPriceProvider",
        "https://financialmodelingprep.com/stable/historical-price-eod/"
        "non-split-adjusted; https://financialmodelingprep.com/stable/"
        "historical-price-eod/dividend-adjusted",
        "PRIMARY_PRICE_CANDIDATE",
    ),
    (
        "marketstack_spy_qqq_second_source_v1",
        "marketstack_eod_daily_prices",
        "MarketstackPriceProvider",
        "https://api.marketstack.com/v2/eod",
        "SECOND_SOURCE_RECONCILIATION_ONLY",
    ),
    (
        "cboe_vix_index_daily_v1",
        "cboe_vix_daily_prices",
        "CboeVixPriceProvider",
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
        "PRIMARY_VIX_INDEX_CANDIDATE",
    ),
    (
        "fred_vixcls_diagnostic_crosscheck_v1",
        "fred_market_series",
        "FredRateProvider",
        "https://fred.stlouisfed.org/graph/fredgraph.csv",
        "DIAGNOSTIC_CROSSCHECK_ONLY",
    ),
    (
        "federal_reserve_fomc_schedule_capture_v1",
        "federal_reserve_fomc_calendar",
        "FederalReserveFomcScheduleAdapterPlanned",
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
        "OFFICIAL_EVENT_COVERAGE_CANDIDATE",
    ),
    (
        "bls_release_schedule_capture_v1",
        "bls_release_calendar",
        "BlsReleaseScheduleAdapterPlanned",
        "https://www.bls.gov/schedule/news_release/",
        "OFFICIAL_EVENT_COVERAGE_CANDIDATE",
    ),
    (
        "bea_release_schedule_capture_v1",
        "bea_release_metadata",
        "BeaReleaseScheduleAdapterPlanned",
        "https://apps.bea.gov/api/data",
        "OFFICIAL_EVENT_COVERAGE_CANDIDATE",
    ),
)
_CANDIDATE_DETAILS = {
    "fmp_spy_qqq_eod_adjusted_v1": (
        "Financial Modeling Prep",
        "PAID_VENDOR_PRIMARY_CANDIDATE",
        "EXISTING_ADAPTER_REQUIRES_PIT_RECEIPT_BINDING",
        ("SPY", "QQQ", "daily", "raw_close", "dividend_adjusted_close"),
        "America/New_York",
        False,
    ),
    "marketstack_spy_qqq_second_source_v1": (
        "Marketstack",
        "PAID_VENDOR_SECOND_SOURCE_ONLY",
        "EXISTING_ADAPTER_DIAGNOSTIC_ONLY",
        ("SPY", "QQQ", "daily", "raw_close", "adjusted_close_crosscheck"),
        "UTC",
        False,
    ),
    "cboe_vix_index_daily_v1": (
        "Cboe Global Markets",
        "OFFICIAL_PRIMARY_CANDIDATE",
        "EXISTING_ADAPTER_REQUIRES_PIT_RECEIPT_BINDING",
        ("VIX", "daily", "index_level_ohlc", "close_equals_adjusted_close"),
        "America/Chicago",
        True,
    ),
    "fred_vixcls_diagnostic_crosscheck_v1": (
        "Federal Reserve Economic Data",
        "PUBLIC_DIAGNOSTIC_SECOND_SOURCE_ONLY",
        "EXISTING_ADAPTER_DIAGNOSTIC_ONLY",
        ("VIXCLS", "daily", "diagnostic_crosscheck"),
        "America/New_York",
        True,
    ),
    "federal_reserve_fomc_schedule_capture_v1": (
        "Federal Reserve Board",
        "OFFICIAL_EVENT_CAPTURE_CANDIDATE",
        "PLANNED_CAPTURE_ADAPTER_NOT_IMPLEMENTED",
        ("FOMC_RATE_DECISION",),
        "America/New_York",
        True,
    ),
    "bls_release_schedule_capture_v1": (
        "Bureau of Labor Statistics",
        "OFFICIAL_EVENT_CAPTURE_CANDIDATE",
        "PLANNED_CAPTURE_ADAPTER_NOT_IMPLEMENTED",
        ("CPI", "NONFARM_PAYROLLS"),
        "America/New_York",
        True,
    ),
    "bea_release_schedule_capture_v1": (
        "Bureau of Economic Analysis",
        "OFFICIAL_EVENT_CAPTURE_CANDIDATE",
        "PLANNED_CAPTURE_ADAPTER_NOT_IMPLEMENTED",
        ("PCE", "GDP_ADVANCE"),
        "America/New_York",
        True,
    ),
}
_REVIEW_SURFACE = (
    (
        "broad_market_risk_off_veto",
        "qqq_options_growth_action_value_broad_market_risk_off_v1",
        "evaluate_broad_market_risk_off",
        ("fmp_spy_qqq_eod_adjusted_v1",),
        ("marketstack_spy_qqq_second_source_v1",),
    ),
    (
        "realized_volatility_veto",
        "volatility_compression_free_v1_successor_adapter",
        "evaluate_realized_volatility_veto",
        ("fmp_spy_qqq_eod_adjusted_v1", "cboe_vix_index_daily_v1"),
        (
            "marketstack_spy_qqq_second_source_v1",
            "fred_vixcls_diagnostic_crosscheck_v1",
        ),
    ),
    (
        "scheduled_event_risk_veto",
        "official_macro_release_calendar_pit_v1",
        "evaluate_scheduled_event_risk",
        (
            "federal_reserve_fomc_schedule_capture_v1",
            "bls_release_schedule_capture_v1",
            "bea_release_schedule_capture_v1",
        ),
        (),
    ),
    (
        "underlying_trend_break_veto",
        "qqq_underlying_trend_break_v1",
        "evaluate_underlying_trend_break",
        ("fmp_spy_qqq_eod_adjusted_v1",),
        ("marketstack_spy_qqq_second_source_v1",),
    ),
)
_REQUIRED_FIELDS = {
    "broad_market_risk_off_veto": (
        "provider",
        "source_id",
        "endpoint",
        "request_parameters",
        "ticker",
        "provider_symbol_alias",
        "adjustment_basis",
        "adjustment_vintage",
        "available_at",
        "downloaded_at",
        "row_count",
        "checksum",
        "schema_version",
    ),
    "realized_volatility_veto": (
        "provider",
        "source_id",
        "endpoint",
        "request_parameters",
        "ticker",
        "adjustment_basis",
        "session_timezone",
        "available_at",
        "downloaded_at",
        "row_count",
        "checksum",
        "schema_version",
    ),
    "scheduled_event_risk_veto": (
        "authority",
        "source_id",
        "endpoint",
        "request_parameters",
        "stable_event_key",
        "event_type",
        "revision_id",
        "revision_action",
        "scheduled_for",
        "source_published_at",
        "captured_at",
        "available_at",
        "coverage_through",
        "row_count",
        "checksum",
        "schema_version",
    ),
    "underlying_trend_break_veto": (
        "provider",
        "source_id",
        "endpoint",
        "request_parameters",
        "ticker",
        "provider_symbol_alias",
        "adjustment_basis",
        "adjustment_vintage",
        "available_at",
        "downloaded_at",
        "row_count",
        "checksum",
        "schema_version",
        "replay_start",
        "initial_checkpoint_sha256",
        "target_start_checkpoint_sha256",
        "state_transition_lineage_sha256",
    ),
}
_REVIEW_DETAILS = {
    "broad_market_risk_off_veto": (
        ("SPY_RAW_CLOSE", "SPY_DIVIDEND_ADJUSTED_CLOSE", "QQQ_EXCHANGE_SESSION"),
        (
            "YAHOO_PRIMARY_OR_FALLBACK",
            "MARKETSTACK_PRIMARY_OVERRIDE",
            "LOCAL_CACHE_GAP_FILL",
        ),
        "EXACT_MANIFEST_REPLAY_PASS_AND_NEW_OWNER_ADMISSION",
        (
            "FMP_ADJUSTMENT_VINTAGE_UNBOUND",
            "SPY_PIT_AVAILABILITY_UNOBSERVED",
            "EXACT_1202_INVENTORY_UNOBSERVED",
        ),
    ),
    "realized_volatility_veto": (
        (
            "QQQ_DIVIDEND_ADJUSTED_CLOSE",
            "VIX_OFFICIAL_INDEX_CLOSE",
            "QQQ_EXCHANGE_SESSION",
        ),
        (
            "FRED_VIXCLS_FILL_OR_OVERRIDE",
            "YAHOO_PRIMARY_OR_FALLBACK",
            "CROSS_DATE_VIX_FILL",
            "LOCAL_CACHE_GAP_FILL",
        ),
        "EXACT_MANIFEST_REPLAY_PASS_AND_NEW_OWNER_ADMISSION",
        (
            "QQQ_ADJUSTMENT_VINTAGE_UNBOUND",
            "CBOE_VIX_AVAILABLE_AT_UNOBSERVED",
            "VIX_QQQ_SESSION_JOIN_UNOBSERVED",
            "EXACT_1202_INVENTORY_UNOBSERVED",
        ),
    ),
    "scheduled_event_risk_veto": (
        (
            "FOMC_RATE_DECISION",
            "CPI",
            "NONFARM_PAYROLLS",
            "PCE",
            "GDP_ADVANCE",
            "QQQ_NEXT_ACTION_SESSION",
        ),
        (
            "FRED_RELEASE_CALENDAR_MODEL_READY",
            "MANUAL_CALENDAR_WITHOUT_PUBLISHED_AT",
            "MISSING_AUTHORITY_AS_CLEAR",
            "CROSS_DATE_EVENT_FILL",
        ),
        "THREE_AUTHORITY_MANIFEST_REPLAY_PASS_AND_NEW_OWNER_ADMISSION",
        (
            "THREE_CAPTURE_ADAPTERS_NOT_IMPLEMENTED",
            "OFFICIAL_PUBLISHED_AT_POLICY_UNOBSERVED",
            "THREE_AUTHORITY_COVERAGE_UNOBSERVED",
            "EXACT_1202_INVENTORY_UNOBSERVED",
        ),
    ),
    "underlying_trend_break_veto": (
        (
            "QQQ_RAW_CLOSE",
            "QQQ_DIVIDEND_ADJUSTED_CLOSE",
            "QQQ_EXCHANGE_SESSION",
            "TREND_STATE_CHECKPOINT",
        ),
        (
            "YAHOO_PRIMARY_OR_FALLBACK",
            "MARKETSTACK_PRIMARY_OVERRIDE",
            "STATE_RESET_AT_TARGET_START",
            "LOCAL_CACHE_GAP_FILL",
        ),
        "EXACT_MANIFEST_AND_CHECKPOINT_REPLAY_PASS_AND_NEW_OWNER_ADMISSION",
        (
            "QQQ_ADJUSTMENT_VINTAGE_UNBOUND",
            "PRE_TARGET_STATE_LINEAGE_UNOBSERVED",
            "TARGET_START_CHECKPOINT_UNOBSERVED",
            "EXACT_1202_INVENTORY_UNOBSERVED",
        ),
    ),
}
_WARMUP_SURFACE = (
    ("broad_market_risk_off_veto_SPY", 199, False),
    ("realized_volatility_veto_QQQ_RV20", 19, False),
    ("realized_volatility_veto_VIX_PERCENTILE252", 251, False),
    ("underlying_trend_break_veto_QQQ", 199, True),
)
_MANIFEST_GATES = (
    "EXACT_CODE_AND_POLICY_IDENTITIES",
    "EXACT_QQQ_CALENDAR_IDENTITY",
    "EXACT_PROVIDER_ENDPOINT_AND_REQUEST_PARAMS",
    "SCHEMA_AND_TICKER_IDENTITY",
    "TIMEZONE_AND_AVAILABLE_AT",
    "DUPLICATE_GAP_AND_CONFLICT_FREE",
    "CORPORATE_ACTION_ADJUSTMENT_VINTAGE",
    "WARMUP_TARGET_SEPARATION",
    "EXACT_1202_SESSION_EQUALITY",
    "SECOND_SOURCE_RECONCILIATION",
    "THREE_OFFICIAL_EVENT_COVERAGE_RECEIPTS",
    "TREND_CHECKPOINT_AND_STATE_LINEAGE",
    "ARTIFACT_CHECKSUMS_AND_ACTUAL_COUNTERS",
    "ZERO_EXECUTION_COUNTERS",
)


class MandatoryVetoRealSourceAdmissionReviewError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class MandatoryVetoRealSourcePlanningReceiptError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")


class _CanonicalModel(_StrictModel):
    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


def _bound_file(path: Path, *, root: Path, field: str) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes project root")
    current = resolved_root
    for part in relative.parts:
        current /= part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if not resolved.is_file() or candidate.is_symlink():
        raise ValueError(f"{field} must be a non-symlink regular file")
    return resolved


class SyntheticProducerBinding(_StrictModel):
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "synthetic_producer_contract_v1.yaml"
    ]
    file_sha256: str
    canonical_sha256: str
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if (self.file_sha256, self.canonical_sha256) != (
            _SYNTHETIC_FILE_SHA256,
            _SYNTHETIC_CANONICAL_SHA256,
        ):
            raise ValueError("S5 synthetic producer exact identity drifted")
        return self


class AuthorizationScope(_StrictModel):
    instruction_ref: Literal[
        "owner_instruction:TRADING-2542G:S6:2026-08-26:"
        "prepare_non_executable_real_source_review_pack_v1"
    ]
    review_contract_preparation_authorized: Literal[True]
    in_memory_planning_receipt_validation_authorized: Literal[True]
    provider_query_authorized: Literal[False]
    cache_or_real_data_read_authorized: Literal[False]
    adapter_execution_authorized: Literal[False]
    source_or_inventory_admission_authorized: Literal[False]
    manifest_replay_or_series_authorized: Literal[False]
    real_dq_or_backtest_authorized: Literal[False]
    authorization_state: Literal["STANDING_OWNER_SCOPE"]


class SourceCandidate(_StrictModel):
    candidate_id: str
    provider: str
    source_id: str
    source_class: Literal[
        "PAID_VENDOR_PRIMARY_CANDIDATE",
        "PAID_VENDOR_SECOND_SOURCE_ONLY",
        "OFFICIAL_PRIMARY_CANDIDATE",
        "PUBLIC_DIAGNOSTIC_SECOND_SOURCE_ONLY",
        "OFFICIAL_EVENT_CAPTURE_CANDIDATE",
    ]
    adapter_id: str
    implementation_state: Literal[
        "EXISTING_ADAPTER_REQUIRES_PIT_RECEIPT_BINDING",
        "EXISTING_ADAPTER_DIAGNOSTIC_ONLY",
        "PLANNED_CAPTURE_ADAPTER_NOT_IMPLEMENTED",
    ]
    endpoint: str
    request_scope: tuple[str, ...]
    timestamp_timezone: Literal["America/New_York", "America/Chicago", "UTC"]
    official_source: bool
    permitted_role: Literal[
        "PRIMARY_PRICE_CANDIDATE",
        "SECOND_SOURCE_RECONCILIATION_ONLY",
        "PRIMARY_VIX_INDEX_CANDIDATE",
        "DIAGNOSTIC_CROSSCHECK_ONLY",
        "OFFICIAL_EVENT_COVERAGE_CANDIDATE",
    ]
    live_probe_performed: Literal[False]
    admitted: Literal[False]


class ReviewRow(_StrictModel):
    veto_id: str
    producer_id: str
    callable_name: str
    primary_candidate_ids: tuple[str, ...]
    diagnostic_candidate_ids: tuple[str, ...]
    consumer_bindings: tuple[str, ...]
    required_receipt_fields: tuple[str, ...]
    forbidden_substitutes: tuple[str, ...]
    candidate_ready_for_review: Literal[True]
    recommended_current_decision: Literal["FREEZE_REVIEW_CONTRACT_ONLY"]
    future_admission_condition: Literal[
        "EXACT_MANIFEST_REPLAY_PASS_AND_NEW_OWNER_ADMISSION",
        "THREE_AUTHORITY_MANIFEST_REPLAY_PASS_AND_NEW_OWNER_ADMISSION",
        "EXACT_MANIFEST_AND_CHECKPOINT_REPLAY_PASS_AND_NEW_OWNER_ADMISSION",
    ]
    real_source_identity_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    observed_inventory_lf_sha256: None
    remaining_blockers: tuple[str, ...]

    @model_validator(mode="after")
    def validate_inventory(self) -> Self:
        if len(set(self.required_receipt_fields)) != len(self.required_receipt_fields):
            raise ValueError("duplicate required receipt field")
        if not self.forbidden_substitutes or not self.remaining_blockers:
            raise ValueError("fail-closed blockers and forbidden substitutes are required")
        return self


class WarmupRow(_StrictModel):
    consumer_id: str
    minimum_pre_target_sessions: int
    continuous_state_replay_required: bool


class ExactInventoryPlan(_StrictModel):
    calendar_authority_id: Literal["qqq_exact_1202_session_sheet_v4"]
    calendar_policy_state: Literal["POLICY_FROZEN_OBSERVED_IDENTITY_UNBOUND"]
    target_start: Literal["2021-02-22"]
    target_session_count: Literal[1202]
    target_end: None
    target_session_list_lf_sha256: None
    observed_target_session_count: None
    warmup_separate_from_target: Literal[True]
    warmup_rows: tuple[WarmupRow, WarmupRow, WarmupRow, WarmupRow]
    observed_source_snapshot_sha256: None
    observed_manifest_sha256: None

    @model_validator(mode="after")
    def validate_warmup(self) -> Self:
        observed = tuple(
            (
                row.consumer_id,
                row.minimum_pre_target_sessions,
                row.continuous_state_replay_required,
            )
            for row in self.warmup_rows
        )
        if observed != _WARMUP_SURFACE:
            raise ValueError("exact warmup and state replay surface drifted")
        return self


class ManifestReplayPlan(_StrictModel):
    executable_in_this_artifact: Literal[False]
    replay_status: Literal["NOT_RUN_NOT_AUTHORIZED"]
    required_gates: tuple[str, ...]
    observed_gate_results: tuple[()]
    technical_validation_state: Literal["NOT_RUN"]
    source_admission_on_replay_pass: Literal[False]

    @model_validator(mode="after")
    def validate_gates(self) -> Self:
        if self.required_gates != _MANIFEST_GATES:
            raise ValueError("manifest replay gate surface drifted")
        return self


class OwnerDecisionRow(_StrictModel):
    veto_id: str
    candidate_contract_decision: Literal["REVIEW_READY_NOT_FROZEN"]
    recommended_owner_action: Literal[
        "APPROVE_CONTRACT_FOR_FUTURE_MANIFEST_REPLAY_ONLY",
        "APPROVE_CONTRACT_FOR_FUTURE_ADAPTER_IMPLEMENTATION_AND_MANIFEST_REPLAY_ONLY",
        "APPROVE_CONTRACT_FOR_FUTURE_MANIFEST_AND_CHECKPOINT_REPLAY_ONLY",
    ]
    admission_decision: Literal[
        "DEFER_UNTIL_EXACT_REPLAY_PASS",
        "DEFER_UNTIL_THREE_AUTHORITY_REPLAY_PASS",
    ]


class AggregateState(_StrictModel):
    review_ready_veto_ids: tuple[str, ...]
    admitted_real_source_identities: tuple[()]
    admitted_exact_1202_session_inventories: tuple[()]
    observed_manifest_replays: tuple[()]
    terminal: Literal[
        "OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4"
    ]
    next_legal_action: Literal[
        "OWNER_REVIEW_OF_ADAPTER_AND_MANIFEST_CONTRACT_WITHOUT_SOURCE_ADMISSION"
    ]


class Safety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    filesystem_io_allowed_in_receipt_validator: Literal[False]
    network_io_allowed: Literal[False]
    provider_query_authorized: Literal[False]
    cache_read_authorized: Literal[False]
    real_data_read_authorized: Literal[False]
    adapter_execution_authorized: Literal[False]
    real_source_admission_allowed: Literal[False]
    exact_inventory_admission_allowed: Literal[False]
    manifest_replay_allowed: Literal[False]
    veto_series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    real_dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
    parameter_or_threshold_search_allowed: Literal[False]
    constant_false_fill_allowed: Literal[False]
    missing_as_clear_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class MandatoryVetoRealSourceAdmissionReview(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_real_source_adapter_admission_review.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "real_source_adapter_admission_review_v1"
    ]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal[
        "OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4"
    ]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    synthetic_producer_binding: SyntheticProducerBinding
    authorization_scope: AuthorizationScope
    source_candidates: tuple[
        SourceCandidate,
        SourceCandidate,
        SourceCandidate,
        SourceCandidate,
        SourceCandidate,
        SourceCandidate,
        SourceCandidate,
    ]
    review_rows: tuple[ReviewRow, ReviewRow, ReviewRow, ReviewRow]
    exact_inventory_plan: ExactInventoryPlan
    manifest_replay_plan: ManifestReplayPlan
    owner_decision_rows: tuple[
        OwnerDecisionRow, OwnerDecisionRow, OwnerDecisionRow, OwnerDecisionRow
    ]
    aggregate_state: AggregateState
    safety: Safety

    @model_validator(mode="after")
    def validate_surface(self) -> Self:
        candidate_surface = tuple(
            (
                row.candidate_id,
                row.source_id,
                row.adapter_id,
                row.endpoint,
                row.permitted_role,
            )
            for row in self.source_candidates
        )
        if candidate_surface != _CANDIDATE_SURFACE:
            raise ValueError("real-source candidate surface drifted")
        for source_candidate in self.source_candidates:
            observed_candidate_details = (
                source_candidate.provider,
                source_candidate.source_class,
                source_candidate.implementation_state,
                source_candidate.request_scope,
                source_candidate.timestamp_timezone,
                source_candidate.official_source,
            )
            if observed_candidate_details != _CANDIDATE_DETAILS[
                source_candidate.candidate_id
            ]:
                raise ValueError(
                    f"{source_candidate.candidate_id} source candidate details drifted"
                )
        review_surface = tuple(
            (
                row.veto_id,
                row.producer_id,
                row.callable_name,
                row.primary_candidate_ids,
                row.diagnostic_candidate_ids,
            )
            for row in self.review_rows
        )
        if review_surface != _REVIEW_SURFACE:
            raise ValueError("real-source review row surface drifted")
        candidate_ids = {row.candidate_id for row in self.source_candidates}
        for review_row in self.review_rows:
            if review_row.required_receipt_fields != _REQUIRED_FIELDS[review_row.veto_id]:
                raise ValueError(
                    f"{review_row.veto_id} required receipt fields drifted"
                )
            observed_review_details = (
                review_row.consumer_bindings,
                review_row.forbidden_substitutes,
                review_row.future_admission_condition,
                review_row.remaining_blockers,
            )
            if observed_review_details != _REVIEW_DETAILS[review_row.veto_id]:
                raise ValueError(f"{review_row.veto_id} review details drifted")
            referenced = set(
                review_row.primary_candidate_ids + review_row.diagnostic_candidate_ids
            )
            if not referenced <= candidate_ids:
                raise ValueError(
                    f"{review_row.veto_id} references unknown source candidate"
                )
        if tuple(row.veto_id for row in self.owner_decision_rows) != _VETO_IDS:
            raise ValueError("owner decision row order drifted")
        if self.aggregate_state.review_ready_veto_ids != _VETO_IDS:
            raise ValueError("review-ready inventory drifted")
        return self


@dataclass(frozen=True)
class MandatoryVetoRealSourceAdmissionReviewLoadResult:
    policy: MandatoryVetoRealSourceAdmissionReview
    path: Path
    file_sha256: str
    canonical_sha256: str
    synthetic_producer_contract: synthetic.MandatoryVetoSyntheticProducerContractLoadResult
    terminal: Literal[
        "OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4"
    ]


class PlanningVetoReceipt(_StrictModel):
    veto_id: str
    candidate_contract_complete: Literal[True]
    remaining_blockers_acknowledged: Literal[True]
    provider_query_count: Literal[0]
    cache_read_count: Literal[0]
    adapter_execution_count: Literal[0]
    real_source_identity_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    observed_inventory_lf_sha256: None
    observed_target_end: None
    observed_row_count: None


class PlanningManifestReceipt(_StrictModel):
    calendar_authority_id: Literal["qqq_exact_1202_session_sheet_v4"]
    target_start: Literal["2021-02-22"]
    expected_target_session_count: Literal[1202]
    observed_target_session_count: None
    target_end: None
    target_session_list_lf_sha256: None
    source_snapshot_sha256: None
    manifest_sha256: None
    dq_report_sha256: None
    veto_series_sha256: None
    event_authority_coverage_receipts: tuple[()]
    trend_target_start_checkpoint_sha256: None
    manifest_replay_executed: Literal[False]
    manifest_replay_status: Literal["NOT_RUN_NOT_AUTHORIZED"]


class ZeroExecutionCounters(_StrictModel):
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    paper_actions: Literal[0]
    live_actions: Literal[0]
    production_effects: Literal[0]
    broker_actions: Literal[0]


class MandatoryVetoRealSourcePlanningReceipt(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_real_source_admission_planning_receipt.v1"
    ]
    review_policy_file_sha256: str
    review_policy_canonical_sha256: str
    status: Literal["REVIEW_READY_NOT_ADMITTED"]
    veto_rows: tuple[
        PlanningVetoReceipt,
        PlanningVetoReceipt,
        PlanningVetoReceipt,
        PlanningVetoReceipt,
    ]
    manifest: PlanningManifestReceipt
    execution_counters: ZeroExecutionCounters

    @field_validator("review_policy_file_sha256", "review_policy_canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def validate_rows(self) -> Self:
        if tuple(row.veto_id for row in self.veto_rows) != _VETO_IDS:
            raise ValueError("planning receipt veto row order drifted")
        return self


def load_mandatory_veto_real_source_admission_review(
    *,
    path: Path = DEFAULT_REAL_SOURCE_ADMISSION_REVIEW_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoRealSourceAdmissionReviewLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="real_source_admission_review")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoRealSourceAdmissionReview.model_validate(payload)
        loaded_synthetic = synthetic.load_mandatory_veto_synthetic_producer_contract(
            project_root=project_root
        )
        binding = policy.synthetic_producer_binding
        if (loaded_synthetic.file_sha256, loaded_synthetic.canonical_sha256) != (
            binding.file_sha256,
            binding.canonical_sha256,
        ):
            raise ValueError("S5 synthetic producer loader identity drifted")
        bound_synthetic = _bound_file(
            Path(binding.path), root=project_root, field="synthetic_producer_binding"
        )
        if hashlib.sha256(bound_synthetic.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("S5 synthetic producer bound file SHA-256 mismatch")
        synthetic_surface = tuple(
            (row.veto_id, row.producer_id, row.callable_name)
            for row in loaded_synthetic.policy.producer_rows
        )
        review_surface = tuple(
            (row.veto_id, row.producer_id, row.callable_name) for row in policy.review_rows
        )
        if synthetic_surface != review_surface:
            raise ValueError("real-source review does not replay S5 callable surface")
    except (
        MandatoryVetoRealSourceAdmissionReviewError,
        synthetic.MandatoryVetoSyntheticProducerContractError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoRealSourceAdmissionReviewError(
            "MANDATORY_VETO_REAL_SOURCE_ADMISSION_REVIEW_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoRealSourceAdmissionReviewLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        synthetic_producer_contract=loaded_synthetic,
        terminal="OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4",
    )


def validate_mandatory_veto_real_source_planning_receipt(
    payload: Mapping[str, Any],
    *,
    review: MandatoryVetoRealSourceAdmissionReviewLoadResult,
) -> MandatoryVetoRealSourcePlanningReceipt:
    """Validate an already supplied planning receipt without filesystem or provider I/O."""

    try:
        receipt = MandatoryVetoRealSourcePlanningReceipt.model_validate(payload)
        if (
            receipt.review_policy_file_sha256,
            receipt.review_policy_canonical_sha256,
        ) != (review.file_sha256, review.canonical_sha256):
            raise ValueError("planning receipt review-policy identity mismatch")
    except MandatoryVetoRealSourcePlanningReceiptError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise MandatoryVetoRealSourcePlanningReceiptError(
            "MANDATORY_VETO_REAL_SOURCE_PLANNING_RECEIPT_REJECTED", str(exc)
        ) from exc
    return receipt


__all__ = [
    "DEFAULT_REAL_SOURCE_ADMISSION_REVIEW_PATH",
    "MandatoryVetoRealSourceAdmissionReview",
    "MandatoryVetoRealSourceAdmissionReviewError",
    "MandatoryVetoRealSourceAdmissionReviewLoadResult",
    "MandatoryVetoRealSourcePlanningReceipt",
    "MandatoryVetoRealSourcePlanningReceiptError",
    "load_mandatory_veto_real_source_admission_review",
    "validate_mandatory_veto_real_source_planning_receipt",
]
