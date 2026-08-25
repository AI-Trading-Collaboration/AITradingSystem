from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_producer_contract_draft as producer_draft,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_OWNER_FREEZE_DECISION_PACK_DRAFT_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v1.yaml"
)

_TASK_ID = "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
_PRODUCER_DRAFT_FILE_SHA256 = (
    "8bd9799b42a0d2f547afbb5bb8708775bef0de35d504197b117ed210e49a6baa"
)
_PRODUCER_DRAFT_CANONICAL_SHA256 = (
    "a6e3ff096d5c5c6df6ec76756581bf0262be4988b696cb2cfb6457dd1b07f063"
)
_SESSION_INVENTORY_SHA256 = "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)
_ROW_SURFACE = (
    (
        "broad_market_risk_off_veto",
        "broad_market_risk_off_spy_sma200_drawdown63_v1_proposal",
        "qqq_options_growth_action_value_broad_market_risk_off_v1",
        "BROAD_MARKET_TREND_OR_DRAWDOWN_STATE",
        "OR",
        (
            "OWNER_EXACT_FREEZE_NOT_GRANTED",
            "DEDICATED_CALLABLE_PRODUCER_NOT_IMPLEMENTED",
            "EXACT_1202_SESSION_INVENTORY_NOT_OBSERVED_OR_ADMITTED",
        ),
    ),
    (
        "realized_volatility_veto",
        "realized_volatility_vix252_or_qqq_rv20_v1_proposal",
        "volatility_compression_free_v1_successor_adapter",
        "VIX_PERCENTILE_OR_QQQ_REALIZED_VOLATILITY_STATE",
        "OR",
        (
            "OWNER_EXACT_FREEZE_NOT_GRANTED",
            "SUCCESSOR_ADAPTER_IDENTITY_NOT_FROZEN",
            "VIX_AVAILABLE_AT_CONTRACT_NOT_FROZEN",
            "EXACT_1202_SESSION_INVENTORY_NOT_OBSERVED_OR_ADMITTED",
        ),
    ),
    (
        "scheduled_event_risk_veto",
        "scheduled_event_official_next_session_any_v1_proposal",
        "official_macro_release_calendar_pit_v1",
        "OFFICIAL_SCHEDULE_NEXT_SESSION_BOOLEAN_STATE",
        "ANY_EVENT",
        (
            "OWNER_EXACT_FREEZE_NOT_GRANTED",
            "PUBLISHED_AT_AND_REVISION_SCHEMA_NOT_IMPLEMENTED",
            "OFFICIAL_SOURCE_ADAPTERS_NOT_IMPLEMENTED",
            "EXACT_1202_SESSION_INVENTORY_NOT_OBSERVED_OR_ADMITTED",
        ),
    ),
    (
        "underlying_trend_break_veto",
        "qqq_sma200_drawdown63_confirmed_recovery_v1_proposal",
        "qqq_underlying_trend_break_v1",
        "QQQ_TREND_AND_DRAWDOWN_BREAK_WITH_RECOVERY_HYSTERESIS",
        "ENTER_AND_RECOVERY_HYSTERESIS",
        (
            "OWNER_EXACT_FREEZE_NOT_GRANTED",
            "DEDICATED_CALLABLE_PRODUCER_NOT_IMPLEMENTED",
            "RECOVERY_STATE_INITIALIZATION_NOT_IMPLEMENTED",
            "EXACT_1202_SESSION_INVENTORY_NOT_OBSERVED_OR_ADMITTED",
        ),
    ),
)


class MandatoryVetoOwnerFreezeDecisionPackDraftError(ValueError):
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


class ProducerDraftBinding(_StrictModel):
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_producer_contract_draft_v1.yaml"
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
            _PRODUCER_DRAFT_FILE_SHA256,
            _PRODUCER_DRAFT_CANONICAL_SHA256,
        ):
            raise ValueError("producer-draft exact identity drifted")
        return self


class OwnerScope(_StrictModel):
    instruction_ref: Literal[
        "owner_instruction:TRADING-2542G:2026-08-26:"
        "continue_prepare_exact_freeze_decision_pack_v1"
    ]
    decision_pack_drafting_authorized: Literal[True]
    recommendation_values_are_owner_frozen: Literal[False]
    producer_contract_admission_authorized: Literal[False]
    veto_series_generation_authorized: Literal[False]
    r1_manifest_generation_authorized: Literal[False]
    real_data_dq_or_backtest_authorized: Literal[False]
    authorization_state: Literal["STANDING_OWNER_SCOPE"]


class TargetInventory(_StrictModel):
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: Literal[1202]
    target_session_inventory_lf_sha256: str

    @model_validator(mode="after")
    def validate_inventory(self) -> Self:
        if not (
            self.requested_start
            == self.evaluated_start
            == date(2021, 2, 22)
            and self.requested_end
            == self.evaluated_end
            == date(2025, 12, 2)
        ):
            raise ValueError("decision-pack research scope drifted")
        if self.target_session_inventory_lf_sha256 != _SESSION_INVENTORY_SHA256:
            raise ValueError("decision-pack target inventory identity drifted")
        return self


class ReviewPolicy(_StrictModel):
    evidence_role: Literal["COMPATIBILITY_ANCHORED_OWNER_REVIEW_PROPOSAL"]
    calibration_status: Literal["UNVALIDATED_NO_REAL_DATA_OR_BACKTEST"]
    recommendation_values_may_drive_runtime: Literal[False]
    owner_must_freeze_each_veto_separately: Literal[True]
    partial_owner_freeze_may_generate_series: Literal[False]
    expiry_condition: Literal["BEFORE_ANY_SOURCE_CONTRACT_ADMISSION_OR_SERIES_GENERATION"]
    missing_terminal: Literal["INSUFFICIENT"]
    malformed_authority_terminal: Literal["INVALID"]


class ProducerDecision(_StrictModel):
    producer_id: str
    callable_state: str
    independent_input_universe: tuple[str, ...]
    forbidden_input_classes: tuple[str, ...]


class FormulaDecision(_StrictModel):
    formula_category: str
    exact_formula: str
    window_inventory: dict[str, int]
    threshold_inventory: dict[str, float]
    combination_rule: str
    entry_confirmation_sessions: int
    recovery_rule: str
    rationale: str


class TimingDecision(_StrictModel):
    decision_as_of: Literal["TARGET_QQQ_SESSION_OFFICIAL_CLOSE"]
    available_at: str
    effective_session: Literal["NEXT_VALID_QQQ_EXCHANGE_SESSION"]
    same_session_action_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]


class PITDecision(_StrictModel):
    required_source_fields: tuple[str, ...]
    published_at_required: bool
    revision_identity_required: bool
    missing_terminal: Literal["INSUFFICIENT"]
    malformed_authority_terminal: Literal["INVALID"]


class EventTaxonomy(_StrictModel):
    authority: Literal["FEDERAL_RESERVE", "BLS", "BEA"]
    event_types: tuple[str, ...]


class DecisionRow(_StrictModel):
    veto_id: str
    recommendation_id: str
    recommendation_state: Literal["RECOMMENDED_PROPOSAL_NOT_OWNER_FROZEN"]
    producer_decision: ProducerDecision
    formula_decision: FormulaDecision
    timing_decision: TimingDecision
    pit_decision: PITDecision
    admitted_event_taxonomy: tuple[EventTaxonomy, ...] = ()
    source_precedence: str | None = None
    revision_treatment: str | None = None
    provenance_refs: tuple[str, ...]
    rejected_alternatives: tuple[str, ...]
    open_evidence_blockers: tuple[str, ...]
    decision_object_complete_for_owner_review: Literal[True]
    owner_exact_freeze_granted: Literal[False]
    producer_contract_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    observed_inventory_lf_sha256: None
    series_generation_allowed: Literal[False]


class AggregateState(_StrictModel):
    decision_objects_ready_for_owner_review: tuple[str, ...]
    owner_frozen_producer_contracts: tuple[()]
    admitted_producer_contracts: tuple[()]
    unresolved_producer_contracts: tuple[str, ...]
    terminal: Literal["OWNER_EXACT_FREEZE_DECISION_REQUIRED_0_OF_4_ADMITTED"]
    next_legal_action: Literal[
        "OWNER_REVIEW_AND_EXACT_FREEZE_EACH_RECOMMENDATION_OR_RETURN_WITH_CHANGES"
    ]
    producer_draft_terminal_preserved: Literal[
        "OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"
    ]


class DecisionPackSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    decision_pack_draft_only: Literal[True]
    recommendation_values_are_runtime_policy: Literal[False]
    exact_formula_or_threshold_owner_frozen: Literal[False]
    source_contract_admission_allowed: Literal[False]
    veto_series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    cache_read_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    real_dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
    parameter_or_threshold_search_allowed: Literal[False]
    constant_false_fill_allowed: Literal[False]
    retained_series_truncation_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class MandatoryVetoOwnerFreezeDecisionPackDraft(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v1"
    ]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal["OWNER_EXACT_FREEZE_DECISION_REQUIRED_0_OF_4_ADMITTED"]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    producer_draft_binding: ProducerDraftBinding
    owner_scope: OwnerScope
    target_inventory: TargetInventory
    review_policy: ReviewPolicy
    decision_rows: tuple[DecisionRow, ...]
    aggregate_state: AggregateState
    safety: DecisionPackSafety

    @model_validator(mode="after")
    def validate_contract_surface(self) -> Self:
        observed = tuple(
            (
                row.veto_id,
                row.recommendation_id,
                row.producer_decision.producer_id,
                row.formula_decision.formula_category,
                row.formula_decision.combination_rule,
                row.open_evidence_blockers,
            )
            for row in self.decision_rows
        )
        if observed != _ROW_SURFACE:
            raise ValueError("owner-freeze decision row surface drifted")
        if self.aggregate_state.decision_objects_ready_for_owner_review != _VETO_IDS:
            raise ValueError("owner-review ready inventory drifted")
        if self.aggregate_state.unresolved_producer_contracts != _VETO_IDS:
            raise ValueError("unresolved producer inventory drifted")
        by_id = {row.veto_id: row for row in self.decision_rows}
        broad = by_id["broad_market_risk_off_veto"]
        if any(
            field.startswith("QQQ.")
            for field in broad.producer_decision.independent_input_universe
        ):
            raise ValueError("broad-market recommendation cannot read QQQ")
        trend = by_id["underlying_trend_break_veto"]
        if not all(
            field.startswith("QQQ.")
            for field in trend.producer_decision.independent_input_universe
        ):
            raise ValueError("underlying-trend recommendation must be QQQ-only")
        event = by_id["scheduled_event_risk_veto"]
        if not event.pit_decision.published_at_required:
            raise ValueError("scheduled-event recommendation requires published_at")
        if not event.pit_decision.revision_identity_required:
            raise ValueError("scheduled-event recommendation requires revision identity")
        required_event_fields = {"scheduled_for", "published_at", "revision_id"}
        if not required_event_fields.issubset(event.pit_decision.required_source_fields):
            raise ValueError("scheduled-event PIT fields are incomplete")
        if event.source_precedence != "EXACT_OFFICIAL_AUTHORITY_ONLY_NO_CROSS_PROVIDER_FILL":
            raise ValueError("scheduled-event source precedence drifted")
        for row in self.decision_rows:
            if not row.formula_decision.window_inventory:
                raise ValueError(f"{row.veto_id} window inventory is empty")
            if not row.formula_decision.threshold_inventory:
                raise ValueError(f"{row.veto_id} threshold inventory is empty")
            if "OWNER_EXACT_FREEZE_NOT_GRANTED" not in row.open_evidence_blockers:
                raise ValueError(f"{row.veto_id} dropped the owner-freeze blocker")
        return self


@dataclass(frozen=True)
class MandatoryVetoOwnerFreezeDecisionPackDraftLoadResult:
    policy: MandatoryVetoOwnerFreezeDecisionPackDraft
    path: Path
    file_sha256: str
    canonical_sha256: str
    producer_draft: producer_draft.MandatoryVetoProducerContractDraftLoadResult
    terminal: Literal["OWNER_EXACT_FREEZE_DECISION_REQUIRED_0_OF_4_ADMITTED"]


def load_mandatory_veto_owner_freeze_decision_pack_draft(
    *,
    path: Path = DEFAULT_OWNER_FREEZE_DECISION_PACK_DRAFT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoOwnerFreezeDecisionPackDraftLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="owner_freeze_decision_pack")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoOwnerFreezeDecisionPackDraft.model_validate(payload)
        bound_producer_draft = _bound_file(
            Path(policy.producer_draft_binding.path),
            root=project_root,
            field="producer_draft_binding",
        )
        if (
            hashlib.sha256(bound_producer_draft.read_bytes()).hexdigest()
            != policy.producer_draft_binding.file_sha256
        ):
            raise ValueError("producer-draft bound file SHA-256 mismatch")
        loaded_producer_draft = (
            producer_draft.load_mandatory_veto_producer_contract_draft(
                project_root=project_root
            )
        )
        if (
            loaded_producer_draft.file_sha256,
            loaded_producer_draft.canonical_sha256,
        ) != (
            policy.producer_draft_binding.file_sha256,
            policy.producer_draft_binding.canonical_sha256,
        ):
            raise ValueError("producer-draft loader identity drifted")
        for row in policy.decision_rows:
            for ref in row.provenance_refs:
                ref_path = ref.partition("#")[0]
                _bound_file(
                    Path(ref_path),
                    root=project_root,
                    field=f"{row.veto_id}.provenance_ref",
                )
    except (
        MandatoryVetoOwnerFreezeDecisionPackDraftError,
        producer_draft.MandatoryVetoProducerContractDraftError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoOwnerFreezeDecisionPackDraftError(
            "MANDATORY_VETO_OWNER_FREEZE_DECISION_PACK_DRAFT_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoOwnerFreezeDecisionPackDraftLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        producer_draft=loaded_producer_draft,
        terminal="OWNER_EXACT_FREEZE_DECISION_REQUIRED_0_OF_4_ADMITTED",
    )


__all__ = [
    "DEFAULT_OWNER_FREEZE_DECISION_PACK_DRAFT_PATH",
    "MandatoryVetoOwnerFreezeDecisionPackDraft",
    "MandatoryVetoOwnerFreezeDecisionPackDraftError",
    "MandatoryVetoOwnerFreezeDecisionPackDraftLoadResult",
    "load_mandatory_veto_owner_freeze_decision_pack_draft",
]
