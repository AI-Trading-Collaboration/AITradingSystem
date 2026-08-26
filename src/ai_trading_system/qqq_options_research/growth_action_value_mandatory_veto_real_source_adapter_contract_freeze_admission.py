from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_real_source_adapter_admission_review as review,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_REAL_SOURCE_ADAPTER_CONTRACT_FREEZE_ADMISSION_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "real_source_adapter_contract_freeze_admission_v1.yaml"
)

_REVIEW_PATH = (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "real_source_adapter_admission_review_v1.yaml"
)
_REVIEW_FILE_SHA256 = "d0adae89a1faf7c160cf82edc9d51ede74fa2ea279fcc2526c009752a9a5b57e"
_REVIEW_CANONICAL_SHA256 = "be705f1b46431e432169b186db6d336bb68d51cf296ca08ca5d6cca465ffc6e3"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)
_CANDIDATE_IDS = (
    "fmp_spy_qqq_eod_adjusted_v1",
    "marketstack_spy_qqq_second_source_v1",
    "cboe_vix_index_daily_v1",
    "fred_vixcls_diagnostic_crosscheck_v1",
    "federal_reserve_fomc_schedule_capture_v1",
    "bls_release_schedule_capture_v1",
    "bea_release_schedule_capture_v1",
)
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
_FREEZE_SURFACE = (
    (
        "broad_market_risk_off_veto",
        "qqq_options_growth_action_value_broad_market_risk_off_v1",
        "evaluate_broad_market_risk_off",
        ("fmp_spy_qqq_eod_adjusted_v1",),
        ("marketstack_spy_qqq_second_source_v1",),
        "APPROVE_CONTRACT_FOR_FUTURE_MANIFEST_REPLAY_ONLY",
        "DEFER_UNTIL_EXACT_REPLAY_PASS",
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
        "APPROVE_CONTRACT_FOR_FUTURE_MANIFEST_REPLAY_ONLY",
        "DEFER_UNTIL_EXACT_REPLAY_PASS",
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
        "APPROVE_CONTRACT_FOR_FUTURE_ADAPTER_IMPLEMENTATION_AND_MANIFEST_REPLAY_ONLY",
        "DEFER_UNTIL_THREE_AUTHORITY_REPLAY_PASS",
    ),
    (
        "underlying_trend_break_veto",
        "qqq_underlying_trend_break_v1",
        "evaluate_underlying_trend_break",
        ("fmp_spy_qqq_eod_adjusted_v1",),
        ("marketstack_spy_qqq_second_source_v1",),
        "APPROVE_CONTRACT_FOR_FUTURE_MANIFEST_AND_CHECKPOINT_REPLAY_ONLY",
        "DEFER_UNTIL_EXACT_REPLAY_PASS",
    ),
)


class MandatoryVetoRealSourceAdapterContractFreezeAdmissionError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
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


class OwnerDecision(_StrictModel):
    decision_ref: Literal[
        "owner_decision:TRADING-2542G:S7:2026-08-26:"
        "freeze_s6_real_source_adapter_manifest_inventory_contract_v1"
    ]
    adapter_manifest_inventory_contract_exact_freeze_granted: Literal[True]
    all_four_review_rows_frozen_together: Literal[True]
    predecessor_bytes_must_remain_immutable: Literal[True]
    separate_non_executable_adapter_implementation_followup_authorized: Literal[True]
    manifest_replay_execution_authorized: Literal[False]
    real_source_or_inventory_admission_authorized: Literal[False]
    real_data_dq_or_backtest_authorized: Literal[False]
    authorization_state: Literal["STANDING_OWNER_SCOPE"]


class ReviewAuthorityBinding(_StrictModel):
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "real_source_adapter_admission_review_v1.yaml"
    ]
    artifact_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_real_source_adapter_admission_review_v1"
    ]
    file_sha256: str
    canonical_sha256: str
    role: Literal["OWNER_EXACT_FROZEN_REAL_SOURCE_ADAPTER_MANIFEST_INVENTORY_CONTRACT"]
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
            _REVIEW_FILE_SHA256,
            _REVIEW_CANONICAL_SHA256,
        ):
            raise ValueError("S6 review exact identity drifted")
        return self


class WarmupRow(_StrictModel):
    consumer_id: str
    minimum_pre_target_sessions: int
    continuous_state_replay_required: bool


class FrozenExactInventory(_StrictModel):
    calendar_authority_id: Literal["qqq_exact_1202_session_sheet_v4"]
    target_start: Literal["2021-02-22"]
    target_session_count: Literal[1202]
    target_end: None
    target_session_list_lf_sha256: None
    observed_target_session_count: None
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
            raise ValueError("frozen exact inventory warmup surface drifted")
        return self


class FrozenContractSurface(_StrictModel):
    source_candidate_ids: tuple[str, ...]
    review_veto_ids: tuple[str, ...]
    exact_inventory: FrozenExactInventory
    manifest_replay_gates: tuple[str, ...]
    predecessor_terminal: Literal["OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4"]

    @model_validator(mode="after")
    def validate_surface(self) -> Self:
        if self.source_candidate_ids != _CANDIDATE_IDS:
            raise ValueError("frozen source candidate inventory drifted")
        if self.review_veto_ids != _VETO_IDS:
            raise ValueError("frozen review veto inventory drifted")
        if self.manifest_replay_gates != _MANIFEST_GATES:
            raise ValueError("frozen manifest replay gate surface drifted")
        return self


class FreezeRow(_StrictModel):
    veto_id: str
    producer_id: str
    callable_name: str
    primary_candidate_ids: tuple[str, ...]
    diagnostic_candidate_ids: tuple[str, ...]
    predecessor_recommended_owner_action: Literal[
        "APPROVE_CONTRACT_FOR_FUTURE_MANIFEST_REPLAY_ONLY",
        "APPROVE_CONTRACT_FOR_FUTURE_ADAPTER_IMPLEMENTATION_AND_MANIFEST_REPLAY_ONLY",
        "APPROVE_CONTRACT_FOR_FUTURE_MANIFEST_AND_CHECKPOINT_REPLAY_ONLY",
    ]
    predecessor_admission_decision: Literal[
        "DEFER_UNTIL_EXACT_REPLAY_PASS",
        "DEFER_UNTIL_THREE_AUTHORITY_REPLAY_PASS",
    ]
    owner_adapter_manifest_contract_frozen: Literal[True]
    adapter_implementation_admitted: Literal[False]
    real_source_identity_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    observed_inventory_lf_sha256: None
    observed_manifest_sha256: None
    manifest_replay_allowed: Literal[False]


class AggregateState(_StrictModel):
    owner_adapter_manifest_contract_frozen_vetoes: tuple[str, ...]
    admitted_adapter_implementations: tuple[()]
    admitted_real_source_identities: tuple[()]
    admitted_exact_1202_session_inventories: tuple[()]
    observed_manifest_replays: tuple[()]
    terminal: Literal["OWNER_ADAPTER_MANIFEST_CONTRACT_FROZEN_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4"]
    next_legal_action: Literal[
        "BEGIN_SEPARATE_NON_EXECUTABLE_ADAPTER_IMPLEMENTATION_FROM_NEW_EXACT_MAIN"
    ]
    predecessor_terminal_superseded: Literal[
        "OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4"
    ]


class FreezeSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    owner_contract_freeze_admission_only: Literal[True]
    adapter_implementation_allowed_in_this_wave: Literal[False]
    adapter_implementation_followup_requires_new_exact_base: Literal[True]
    filesystem_market_data_read_allowed: Literal[False]
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


class MandatoryVetoRealSourceAdapterContractFreezeAdmission(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_real_source_adapter_contract_freeze_admission.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "real_source_adapter_contract_freeze_admission_v1"
    ]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_ADAPTER_MANIFEST_CONTRACT_FROZEN_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4"]
    task_id: Literal["TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"]
    owner_decision: OwnerDecision
    review_authority_binding: ReviewAuthorityBinding
    frozen_contract_surface: FrozenContractSurface
    freeze_rows: tuple[FreezeRow, FreezeRow, FreezeRow, FreezeRow]
    aggregate_state: AggregateState
    safety: FreezeSafety

    @model_validator(mode="after")
    def validate_exact_surface(self) -> Self:
        observed_rows = tuple(
            (
                row.veto_id,
                row.producer_id,
                row.callable_name,
                row.primary_candidate_ids,
                row.diagnostic_candidate_ids,
                row.predecessor_recommended_owner_action,
                row.predecessor_admission_decision,
            )
            for row in self.freeze_rows
        )
        if observed_rows != _FREEZE_SURFACE:
            raise ValueError("adapter contract freeze row surface drifted")
        if self.aggregate_state.owner_adapter_manifest_contract_frozen_vetoes != _VETO_IDS:
            raise ValueError("atomic four-veto adapter contract freeze inventory drifted")
        return self


@dataclass(frozen=True)
class MandatoryVetoRealSourceAdapterContractFreezeAdmissionLoadResult:
    policy: MandatoryVetoRealSourceAdapterContractFreezeAdmission
    path: Path
    file_sha256: str
    canonical_sha256: str
    review: review.MandatoryVetoRealSourceAdmissionReviewLoadResult
    terminal: Literal["OWNER_ADAPTER_MANIFEST_CONTRACT_FROZEN_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4"]


def load_mandatory_veto_real_source_adapter_contract_freeze_admission(
    *,
    path: Path = DEFAULT_REAL_SOURCE_ADAPTER_CONTRACT_FREEZE_ADMISSION_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoRealSourceAdapterContractFreezeAdmissionLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="contract_freeze_admission")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(payload)
        loaded_review = review.load_mandatory_veto_real_source_admission_review(
            project_root=project_root
        )
        binding = policy.review_authority_binding
        if (loaded_review.file_sha256, loaded_review.canonical_sha256) != (
            binding.file_sha256,
            binding.canonical_sha256,
        ):
            raise ValueError("S6 review loader identity drifted")
        bound_review = _bound_file(
            Path(binding.path), root=project_root, field="review_authority_binding"
        )
        if hashlib.sha256(bound_review.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("S6 review bound file SHA-256 mismatch")

        observed_candidates = tuple(
            item.candidate_id for item in loaded_review.policy.source_candidates
        )
        if observed_candidates != policy.frozen_contract_surface.source_candidate_ids:
            raise ValueError("S7 source candidates do not replay S6 exactly")

        review_rows = tuple(
            (
                row.veto_id,
                row.producer_id,
                row.callable_name,
                row.primary_candidate_ids,
                row.diagnostic_candidate_ids,
            )
            for row in loaded_review.policy.review_rows
        )
        freeze_rows = tuple(
            (
                row.veto_id,
                row.producer_id,
                row.callable_name,
                row.primary_candidate_ids,
                row.diagnostic_candidate_ids,
            )
            for row in policy.freeze_rows
        )
        if review_rows != freeze_rows:
            raise ValueError("S7 freeze rows do not replay S6 review rows exactly")

        predecessor_decisions = tuple(
            (row.recommended_owner_action, row.admission_decision)
            for row in loaded_review.policy.owner_decision_rows
        )
        freeze_decisions = tuple(
            (
                row.predecessor_recommended_owner_action,
                row.predecessor_admission_decision,
            )
            for row in policy.freeze_rows
        )
        if predecessor_decisions != freeze_decisions:
            raise ValueError("S7 owner decision surface does not replay S6 exactly")

        predecessor_inventory = loaded_review.policy.exact_inventory_plan
        frozen_inventory = policy.frozen_contract_surface.exact_inventory
        predecessor_warmup = tuple(
            (
                row.consumer_id,
                row.minimum_pre_target_sessions,
                row.continuous_state_replay_required,
            )
            for row in predecessor_inventory.warmup_rows
        )
        frozen_warmup = tuple(
            (
                row.consumer_id,
                row.minimum_pre_target_sessions,
                row.continuous_state_replay_required,
            )
            for row in frozen_inventory.warmup_rows
        )
        if (
            predecessor_inventory.calendar_authority_id,
            predecessor_inventory.target_start,
            predecessor_inventory.target_session_count,
            predecessor_warmup,
        ) != (
            frozen_inventory.calendar_authority_id,
            frozen_inventory.target_start,
            frozen_inventory.target_session_count,
            frozen_warmup,
        ):
            raise ValueError("S7 exact inventory plan does not replay S6 exactly")
        if (
            loaded_review.policy.manifest_replay_plan.required_gates
            != policy.frozen_contract_surface.manifest_replay_gates
        ):
            raise ValueError("S7 manifest gates do not replay S6 exactly")
    except (
        MandatoryVetoRealSourceAdapterContractFreezeAdmissionError,
        review.MandatoryVetoRealSourceAdmissionReviewError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoRealSourceAdapterContractFreezeAdmissionError(
            "MANDATORY_VETO_REAL_SOURCE_ADAPTER_CONTRACT_FREEZE_ADMISSION_REJECTED",
            str(exc),
        ) from exc
    return MandatoryVetoRealSourceAdapterContractFreezeAdmissionLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        review=loaded_review,
        terminal=("OWNER_ADAPTER_MANIFEST_CONTRACT_FROZEN_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4"),
    )


__all__ = [
    "DEFAULT_REAL_SOURCE_ADAPTER_CONTRACT_FREEZE_ADMISSION_PATH",
    "MandatoryVetoRealSourceAdapterContractFreezeAdmission",
    "MandatoryVetoRealSourceAdapterContractFreezeAdmissionError",
    "MandatoryVetoRealSourceAdapterContractFreezeAdmissionLoadResult",
    "load_mandatory_veto_real_source_adapter_contract_freeze_admission",
]
