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
    growth_action_value_mandatory_veto_source_contract as source_contract,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_PRODUCER_CONTRACT_DRAFT_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_producer_contract_draft_v1.yaml"
)

_TASK_ID = "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
_SOURCE_WAVE_FILE_SHA256 = "76e38c969ee0849c77ac4012b72d0e65115f0a3448ecb276c9ca8cfef5faf8b5"
_SOURCE_WAVE_CANONICAL_SHA256 = (
    "0f8204170b4c8810cf2685e63dd5035801cef79788932b63cdf5691c1ba28e26"
)
_SESSION_INVENTORY_SHA256 = "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)
_FORBIDDEN_INPUTS = (
    "selected_call_contract_identity",
    "selected_put_contract_identity",
    "selected_pair_checksum",
    "selected_call_activity",
    "selected_put_activity",
    "option_alpha_state",
    "growth_allowed",
    "growth_active",
    "growth_inactive",
    "candidate_target_weights",
    "candidate_return",
    "v4_result",
    "result_dependent_contributor_universe",
)
_REQUIRED_DECISION_DIMENSIONS = (
    "independent_input_universe",
    "structural_formula",
    "exact_formula",
    "window_inventory",
    "threshold_inventory",
    "decision_as_of",
    "available_at",
    "missing_and_malformed_terminals",
    "exact_1202_session_inventory",
)
_ROW_SURFACE = (
    (
        "broad_market_risk_off_veto",
        "BROAD_MARKET_INDEPENDENT_SOURCE",
        "qqq_options_growth_action_value_broad_market_risk_off_candidate_v1",
        "PLANNED_INDEPENDENT_PRODUCER_NOT_CALLABLE",
        "BROAD_MARKET_TREND_AND_DRAWDOWN_STATE",
        (
            "CALLABLE_INDEPENDENT_PRODUCER_MISSING",
            "PROXY_UNIVERSE_NOT_OWNER_FROZEN",
            "EXACT_FORMULA_WINDOW_AND_THRESHOLD_NOT_FROZEN",
            "TIMING_NOT_OWNER_FROZEN",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
    (
        "realized_volatility_veto",
        "REALIZED_VOLATILITY_INDEPENDENT_SOURCE",
        "volatility_compression_free_v1_candidate_adapter",
        "CALLABLE_CANDIDATE_NOT_SUCCESSOR_ADMITTED",
        "TRAILING_REALIZED_VOLATILITY_AND_VIX_PERCENTILE_STATE",
        (
            "SUCCESSOR_ADAPTER_IDENTITY_NOT_OWNER_FROZEN",
            "EXACT_FORMULA_WINDOWS_THRESHOLDS_AND_COMBINATION_NOT_FROZEN",
            "VIX_AVAILABLE_AT_CONTRACT_NOT_OWNER_FROZEN",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
    (
        "scheduled_event_risk_veto",
        "PIT_SCHEDULED_EVENT_CALENDAR_SOURCE",
        "macro_event_calendar_free_v1_pit_hardening_candidate",
        "CALLABLE_CANDIDATE_PIT_INCOMPLETE",
        "PIT_SCHEDULED_EVENT_PROXIMITY_STATE",
        (
            "PUBLISHED_AT_AND_REVISION_SCHEMA_NOT_IMPLEMENTED",
            "EVENT_AUTHORITY_SET_NOT_OWNER_FROZEN",
            "EXACT_WINDOWS_SCORE_THRESHOLD_AND_SOURCE_PRECEDENCE_NOT_FROZEN",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
    (
        "underlying_trend_break_veto",
        "QQQ_UNDERLYING_INDEPENDENT_SOURCE",
        "qqq_underlying_trend_break_candidate_v1",
        "PLANNED_DEDICATED_PRODUCER_NOT_CALLABLE",
        "QQQ_UNDERLYING_TREND_BREAK_STATE",
        (
            "CALLABLE_DEDICATED_PRODUCER_MISSING",
            "EXACT_TREND_REFERENCE_BREAK_AND_RECOVERY_FORMULA_NOT_FROZEN",
            "TIMING_NOT_OWNER_FROZEN",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
)


class MandatoryVetoProducerContractDraftError(ValueError):
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


class SourceWaveBinding(_StrictModel):
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_source_contract_wave_v1.yaml"
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
            _SOURCE_WAVE_FILE_SHA256,
            _SOURCE_WAVE_CANONICAL_SHA256,
        ):
            raise ValueError("source-wave exact identity drifted")
        return self


class OwnerScope(_StrictModel):
    instruction_ref: Literal[
        "owner_instruction:TRADING-2542G:2026-08-25:"
        "continue_non_executable_producer_contract_draft_v1"
    ]
    producer_contract_drafting_authorized: Literal[True]
    exact_producer_formula_or_threshold_freeze_granted: Literal[False]
    source_admission_authorized: Literal[False]
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
            raise ValueError("producer draft research scope drifted")
        if self.target_session_inventory_lf_sha256 != _SESSION_INVENTORY_SHA256:
            raise ValueError("producer draft target inventory identity drifted")
        return self


class InputContract(_StrictModel):
    allowed_input_fields: tuple[str, ...]
    price_adjustment_rule: str
    input_identity_state: str


class StructuralFormula(_StrictModel):
    formula_category: str
    ordered_components: tuple[str, ...]
    combination_rule: Literal["OWNER_DECISION_REQUIRED"]
    exact_formula_frozen: Literal[False]


class TimingContract(_StrictModel):
    decision_as_of: str
    available_at: str
    effective_session: Literal["NEXT_VALID_QQQ_EXCHANGE_SESSION"]
    timing_state: Literal["STRUCTURE_PROPOSAL_OWNER_FREEZE_REQUIRED"]
    same_session_action_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]


class PITContract(_StrictModel):
    required_source_fields: tuple[str, ...]
    published_at_required: bool
    event_date_can_substitute_for_published_at: Literal[False]
    revision_identity_required: bool
    missing_terminal: Literal["INSUFFICIENT"]
    malformed_authority_terminal: Literal["INVALID"]


class ThresholdDecision(_StrictModel):
    decision_id: str
    candidate_provenance: str | None
    exact_value: None
    owner_freeze_state: Literal["PENDING_OWNER_EXACT_FREEZE"]


class CandidateEvidence(_StrictModel):
    path: str
    file_sha256: str
    role: str
    admitted_as_producer_authority: Literal[False]

    @field_validator("file_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class ProducerContractRow(_StrictModel):
    veto_id: str
    source_independence_class: str
    draft_producer_id: str
    producer_state: str
    draft_status: Literal["OWNER_EXACT_FREEZE_REQUIRED"]
    input_contract: InputContract
    structural_formula: StructuralFormula
    timing_contract: TimingContract
    pit_contract: PITContract
    threshold_decisions: tuple[ThresholdDecision, ...]
    candidate_evidence: tuple[CandidateEvidence, ...]
    producer_contract_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    series_generation_allowed: Literal[False]
    blocker_codes: tuple[str, ...]


class DependencyPolicy(_StrictModel):
    forbidden_producer_inputs: tuple[str, ...]
    option_or_result_dependency_allowed: Literal[False]
    broad_market_may_read_qqq_or_growth_state: Literal[False]
    event_date_may_substitute_for_published_at: Literal[False]
    pilot_threshold_may_be_labeled_owner_frozen: Literal[False]
    shared_session_clock_and_target_inventory_allowed: Literal[True]

    @model_validator(mode="after")
    def validate_inputs(self) -> Self:
        if self.forbidden_producer_inputs != _FORBIDDEN_INPUTS:
            raise ValueError("producer draft forbidden input inventory drifted")
        return self


class AggregateState(_StrictModel):
    admitted_producer_contracts: tuple[()]
    unresolved_producer_contracts: tuple[str, ...]
    terminal: Literal["OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"]
    next_legal_action: Literal[
        "OWNER_REVIEW_EXACT_PRODUCER_FORMULA_THRESHOLD_TIMING_AND_INVENTORY"
    ]
    source_wave_terminal_preserved: Literal[
        "BLOCKED_PRE_R1_MANIFEST_INCOMPLETE_MANDATORY_SOURCE_CONTRACTS"
    ]


class DraftSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    producer_contract_draft_only: Literal[True]
    exact_formula_or_threshold_frozen: Literal[False]
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


class MandatoryVetoProducerContractDraft(_CanonicalModel):
    schema_version: Literal["growth_action_value_mandatory_veto_producer_contract_draft.v1"]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_producer_contract_draft_v1"
    ]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal["OWNER_REVIEW_REQUIRED_0_OF_4_ADMITTED"]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    source_wave_binding: SourceWaveBinding
    owner_scope: OwnerScope
    target_inventory: TargetInventory
    required_decision_dimensions: tuple[str, ...]
    producer_contracts: tuple[ProducerContractRow, ...]
    dependency_policy: DependencyPolicy
    aggregate_state: AggregateState
    safety: DraftSafety

    @model_validator(mode="after")
    def validate_contract_surface(self) -> Self:
        if self.required_decision_dimensions != _REQUIRED_DECISION_DIMENSIONS:
            raise ValueError("producer decision dimension inventory drifted")
        observed = tuple(
            (
                row.veto_id,
                row.source_independence_class,
                row.draft_producer_id,
                row.producer_state,
                row.structural_formula.formula_category,
                row.blocker_codes,
            )
            for row in self.producer_contracts
        )
        if observed != _ROW_SURFACE:
            raise ValueError("producer draft row surface drifted")
        if self.aggregate_state.unresolved_producer_contracts != _VETO_IDS:
            raise ValueError("unresolved producer inventory drifted")
        forbidden = set(self.dependency_policy.forbidden_producer_inputs)
        for row in self.producer_contracts:
            if forbidden.intersection(row.input_contract.allowed_input_fields):
                raise ValueError(f"{row.veto_id} allowed inputs include a forbidden dependency")
            if any(item.exact_value is not None for item in row.threshold_decisions):
                raise ValueError(f"{row.veto_id} draft contains an exact threshold value")
        by_id = {row.veto_id: row for row in self.producer_contracts}
        broad = by_id["broad_market_risk_off_veto"]
        if any(field.startswith("QQQ.") for field in broad.input_contract.allowed_input_fields):
            raise ValueError("broad-market producer cannot read QQQ")
        event = by_id["scheduled_event_risk_veto"]
        if not event.pit_contract.published_at_required:
            raise ValueError("scheduled-event producer requires published_at")
        if not event.pit_contract.revision_identity_required:
            raise ValueError("scheduled-event producer requires revision identity")
        required_event_fields = {"scheduled_for", "published_at", "revision_id"}
        if not required_event_fields.issubset(event.pit_contract.required_source_fields):
            raise ValueError("scheduled-event PIT source fields are incomplete")
        return self


@dataclass(frozen=True)
class MandatoryVetoProducerContractDraftLoadResult:
    policy: MandatoryVetoProducerContractDraft
    path: Path
    file_sha256: str
    canonical_sha256: str
    source_wave: source_contract.MandatoryVetoSourceContractWaveLoadResult
    terminal: Literal["OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"]


def load_mandatory_veto_producer_contract_draft(
    *,
    path: Path = DEFAULT_PRODUCER_CONTRACT_DRAFT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoProducerContractDraftLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="producer_contract_draft")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoProducerContractDraft.model_validate(payload)
        source_wave = source_contract.load_mandatory_veto_source_contract_wave(
            project_root=project_root
        )
        binding = policy.source_wave_binding
        if (source_wave.file_sha256, source_wave.canonical_sha256) != (
            binding.file_sha256,
            binding.canonical_sha256,
        ):
            raise ValueError("source-wave loader identity drifted")
        bound_source_wave = _bound_file(
            Path(binding.path), root=project_root, field="source_wave_binding"
        )
        if hashlib.sha256(bound_source_wave.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("source-wave bound file SHA-256 mismatch")
        for row in policy.producer_contracts:
            for evidence in row.candidate_evidence:
                evidence_path = _bound_file(
                    Path(evidence.path),
                    root=project_root,
                    field=f"{row.veto_id}.candidate_evidence",
                )
                if hashlib.sha256(evidence_path.read_bytes()).hexdigest() != evidence.file_sha256:
                    raise ValueError(f"{row.veto_id} candidate evidence SHA-256 mismatch")
    except (
        MandatoryVetoProducerContractDraftError,
        source_contract.MandatoryVetoSourceContractError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoProducerContractDraftError(
            "MANDATORY_VETO_PRODUCER_CONTRACT_DRAFT_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoProducerContractDraftLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        source_wave=source_wave,
        terminal="OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED",
    )


__all__ = [
    "DEFAULT_PRODUCER_CONTRACT_DRAFT_PATH",
    "MandatoryVetoProducerContractDraft",
    "MandatoryVetoProducerContractDraftError",
    "MandatoryVetoProducerContractDraftLoadResult",
    "load_mandatory_veto_producer_contract_draft",
]
