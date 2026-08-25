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
    growth_action_value_veto_option_signal_architecture as architecture_contract,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_ARCHITECTURE_FREEZE_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_veto_option_signal_architecture_freeze_v1.yaml"
)
DEFAULT_SOURCE_CONTRACT_WAVE_PATH = Path(
    "config/research/qc_qqq_options_growth_action_value_mandatory_veto_source_contract_wave_v1.yaml"
)

_TASK_ID = "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
_ARCHITECTURE_FILE_SHA256 = "9b4856614298d64b2c8b5897980735a9e2a19c46fecb6c2362cb750ae13b136d"
_ARCHITECTURE_CANONICAL_SHA256 = "88e1283b0333bafca24779c9c527d362acef40b65d4cff1a9d081ded07ac70e4"
_COMPATIBILITY_FILE_SHA256 = "c5867551aec4f152256219e4fb19b7c52ec5a6b7f8d8c316961d33a75749679d"
_COMPATIBILITY_CANONICAL_SHA256 = "067a6b23daa1bfff22a6d4f4fcb773346a7d866e21cf2adb759acde75d04f524"
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
    "growth_active",
    "growth_inactive",
    "candidate_target_weights",
    "candidate_return",
    "v4_result",
    "result_dependent_contributor_universe",
)
_FREEZE_BINDINGS = (
    (
        "config/research/"
        "qc_qqq_options_growth_action_value_veto_option_signal_architecture_v1.yaml",
        "qc_qqq_options_growth_action_value_veto_option_signal_architecture_v1",
        "1.0.0-draft.1",
        _ARCHITECTURE_FILE_SHA256,
        _ARCHITECTURE_CANONICAL_SHA256,
        "OWNER_EXACT_FROZEN_ARCHITECTURE_AUTHORITY",
    ),
    (
        "config/research/qc_qqq_options_growth_action_value_legacy_veto_compatibility_map_v1.yaml",
        "qc_qqq_options_growth_action_value_legacy_veto_compatibility_map_v1",
        "1.0.0-draft.1",
        _COMPATIBILITY_FILE_SHA256,
        _COMPATIBILITY_CANONICAL_SHA256,
        "OWNER_EXACT_FROZEN_LEGACY_COMPATIBILITY_AUTHORITY",
    ),
)
_SOURCE_ROWS = (
    (
        "broad_market_risk_off_veto",
        "risk_off_veto",
        "BROAD_MARKET_INDEPENDENT_SOURCE",
        "BLOCKED_INDEPENDENT_PRODUCER_NOT_FROZEN",
        "BROAD_MARKET_PRICE_AND_MACRO_STATE",
        "BLOCKED_INDEPENDENT_PRODUCER_FORMULA_AND_TIMING_NOT_FROZEN",
        (
            "INDEPENDENT_PRODUCER_IDENTITY_MISSING",
            "FORMULA_AND_THRESHOLD_AUTHORITY_NOT_FROZEN",
            "DECISION_AS_OF_AND_AVAILABLE_AT_NOT_FROZEN",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
            "GROWTH_ALLOWED_ALIAS_FORBIDDEN",
        ),
    ),
    (
        "realized_volatility_veto",
        "volatility_veto",
        "REALIZED_VOLATILITY_INDEPENDENT_SOURCE",
        "SOURCE_CONTRACT_READY_SERIES_NOT_GENERATED",
        "TRAILING_REALIZED_VOLATILITY_AND_COMPRESSION",
        "BLOCKED_SUCCESSOR_THRESHOLD_PRODUCER_AND_TIMING_NOT_FROZEN",
        (
            "SUCCESSOR_PRODUCER_IDENTITY_MISSING",
            "LEGACY_RUNTIME_THRESHOLDS_NOT_SEPARATE_OWNER_AUTHORITY",
            "DECISION_AS_OF_AND_AVAILABLE_AT_NOT_FROZEN",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
    (
        "scheduled_event_risk_veto",
        "event_risk_veto",
        "PIT_SCHEDULED_EVENT_CALENDAR_SOURCE",
        "BLOCKED_PIT_SOURCE_NOT_FROZEN",
        "PIT_SCHEDULED_MACRO_EVENT_WINDOW",
        "BLOCKED_PUBLISHED_AT_EVENT_SET_WINDOW_AND_THRESHOLD_NOT_FROZEN",
        (
            "PIT_PUBLISHED_AT_AUTHORITY_MISSING",
            "EVENT_SET_WINDOW_AND_THRESHOLD_NOT_FROZEN",
            "INDEPENDENT_PRODUCER_IDENTITY_MISSING",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
    (
        "underlying_trend_break_veto",
        "trend_break_veto",
        "QQQ_UNDERLYING_INDEPENDENT_SOURCE",
        "BLOCKED_ADAPTER_CONTRACT_NOT_FROZEN",
        "QQQ_UNDERLYING_PRICE_TREND_BREAK",
        "BLOCKED_CALLABLE_PRODUCER_FORMULA_AND_TIMING_NOT_FROZEN",
        (
            "CALLABLE_PRODUCER_IDENTITY_MISSING",
            "FORMULA_AND_THRESHOLD_AUTHORITY_NOT_FROZEN",
            "DECISION_AS_OF_AND_AVAILABLE_AT_NOT_FROZEN",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
)


class MandatoryVetoSourceContractError(ValueError):
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


class OwnerInstruction(_StrictModel):
    decision_id: Literal[
        "owner_decision:TRADING-2542F-2542G:2026-08-25:approve_exact_architecture_freeze_and_source_contract_followup_v1"
    ]
    architecture_and_compatibility_exact_freeze_approved: Literal[True]
    result_blind_source_contract_wave_authorized: Literal[True]
    concrete_formula_or_threshold_exact_freeze_granted: Literal[False]
    veto_series_generation_authorized: Literal[False]
    r1_manifest_generation_authorized: Literal[False]
    real_data_dq_or_backtest_authorized: Literal[False]
    authorization_state: Literal["EXACT_PREAUTHORIZED"]


class ApprovedBinding(_StrictModel):
    path: str
    artifact_id: str
    artifact_version: str
    file_sha256: str
    canonical_sha256: str
    role: str
    disposition: Literal["APPROVED_PREDECESSOR_RETAINED_IMMUTABLE"]
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class FrozenSemantics(_StrictModel):
    successor_market_veto_taxonomy: tuple[str, ...]
    historical_tqqq_role: Literal["NO_LEVERAGE_ETF_ACTION_GUARD"]
    dq_is_market_veto: Literal[False]
    option_alpha_is_market_veto: Literal[False]
    concrete_source_contract_values_selected: Literal[False]
    predecessor_bytes_may_change: Literal[False]
    successor_may_override_approved_architecture: Literal[False]

    @model_validator(mode="after")
    def validate_taxonomy(self) -> Self:
        if self.successor_market_veto_taxonomy != _VETO_IDS:
            raise ValueError("frozen successor veto taxonomy drifted")
        return self


class FreezeSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    source_contract_drafting_allowed: Literal[True]
    veto_series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    cache_read_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    real_dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
    threshold_or_formula_search_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class ArchitectureFreezeAdmission(_CanonicalModel):
    schema_version: Literal["growth_action_value_veto_option_signal_architecture_freeze.v1"]
    freeze_id: Literal[
        "qc_qqq_options_growth_action_value_veto_option_signal_architecture_freeze_v1"
    ]
    freeze_version: Literal["1.0.0"]
    status: Literal["OWNER_EXACT_FROZEN_NON_EXECUTABLE_SOURCE_CONTRACT_WAVE_ONLY"]
    task_id: Literal["TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"]
    owner_instruction: OwnerInstruction
    approved_bindings: tuple[ApprovedBinding, ...]
    frozen_semantics: FrozenSemantics
    safety: FreezeSafety

    @model_validator(mode="after")
    def validate_bindings(self) -> Self:
        observed = tuple(
            (
                row.path,
                row.artifact_id,
                row.artifact_version,
                row.file_sha256,
                row.canonical_sha256,
                row.role,
            )
            for row in self.approved_bindings
        )
        if observed != _FREEZE_BINDINGS:
            raise ValueError("owner exact-freeze binding surface drifted")
        return self


class FreezeAdmissionBinding(_StrictModel):
    path: Literal[
        "config/research/qc_qqq_options_growth_action_value_veto_option_signal_architecture_freeze_v1.yaml"
    ]
    freeze_id: Literal[
        "qc_qqq_options_growth_action_value_veto_option_signal_architecture_freeze_v1"
    ]
    freeze_version: Literal["1.0.0"]
    file_sha256: str
    canonical_sha256: str
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class TargetInventory(_StrictModel):
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: Literal[1202]
    target_session_inventory_lf_sha256: str

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        expected = (date(2021, 2, 22), date(2025, 12, 2))
        if (self.requested_start, self.requested_end) != expected:
            raise ValueError("requested source-contract window drifted")
        if (self.evaluated_start, self.evaluated_end) != expected:
            raise ValueError("evaluated source-contract window drifted")
        if self.target_session_inventory_lf_sha256 != _SESSION_INVENTORY_SHA256:
            raise ValueError("target session inventory identity drifted")
        return self


class CandidateEvidence(_StrictModel):
    path: str
    file_sha256: str
    role: str
    admitted_as_successor_source: Literal[False]

    @field_validator("file_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class ExactInventoryReadiness(_StrictModel):
    required: Literal[True]
    target_inventory_lf_sha256: str
    admitted: Literal[False]
    observed_inventory_lf_sha256: None

    @field_validator("target_inventory_lf_sha256")
    @classmethod
    def validate_target_sha(cls, value: str) -> str:
        if value != _SESSION_INVENTORY_SHA256:
            raise ValueError("source row target inventory identity drifted")
        return value


class SourceIdentityDraft(_StrictModel):
    source_contract_sha256: None
    independent_producer_identity: None
    formula_category: None
    threshold_policy_id: None
    decision_as_of: None
    available_at: None
    missing_terminal: Literal["INSUFFICIENT"]
    malformed_authority_terminal: Literal["INVALID"]
    exact_1202_session_inventory: ExactInventoryReadiness


class MandatoryVetoSourceRow(_StrictModel):
    veto_id: str
    legacy_field: str
    source_independence_class: str
    architecture_readiness_state: str
    proposed_formula_category: str
    formula_category_state: Literal["DRAFT_REQUIRES_OWNER_EXACT_FREEZE"]
    successor_admission_state: str
    blocker_codes: tuple[str, ...]
    required_identity: SourceIdentityDraft
    candidate_evidence: tuple[CandidateEvidence, ...]
    option_alpha_input_allowed: Literal[False]
    result_input_allowed: Literal[False]
    series_generation_allowed: Literal[False]


class DependencyPolicy(_StrictModel):
    forbidden_source_inputs: tuple[str, ...]
    growth_allowed_alias_allowed: Literal[False]
    selected_pair_or_activity_allowed: Literal[False]
    result_dependent_formula_or_bucket_allowed: Literal[False]
    shared_clock_and_target_inventory_allowed: Literal[True]

    @model_validator(mode="after")
    def validate_forbidden_inputs(self) -> Self:
        if self.forbidden_source_inputs != _FORBIDDEN_INPUTS:
            raise ValueError("source-contract forbidden input set drifted")
        return self


class AggregateState(_StrictModel):
    admitted_source_contracts: tuple[()]
    unresolved_source_contracts: tuple[str, ...]
    terminal: Literal["BLOCKED_PRE_R1_MANIFEST_INCOMPLETE_MANDATORY_SOURCE_CONTRACTS"]
    missing_required_source_outcome: Literal["INSUFFICIENT_EVIDENCE_TO_BUILD_R1_MANIFEST"]
    malformed_authority_outcome: Literal["PRE_RUN_AUTHORITY_INVALID"]

    @model_validator(mode="after")
    def validate_aggregate(self) -> Self:
        if self.unresolved_source_contracts != _VETO_IDS:
            raise ValueError("unresolved mandatory source set drifted")
        return self


class SourceWaveSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    concrete_formula_or_threshold_frozen: Literal[False]
    veto_series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    cache_read_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    real_dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
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


class MandatoryVetoSourceContractWave(_CanonicalModel):
    schema_version: Literal["growth_action_value_mandatory_veto_source_contract_wave.v1"]
    policy_id: Literal["qc_qqq_options_growth_action_value_mandatory_veto_source_contract_wave_v1"]
    policy_version: Literal["1.0.0-draft.1"]
    status: Literal["TYPED_BLOCKERS_PRE_R1_NON_EXECUTABLE_DRAFT"]
    task_id: Literal["TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"]
    architecture_freeze: FreezeAdmissionBinding
    target_inventory: TargetInventory
    required_source_identity_fields: tuple[str, ...]
    source_contracts: tuple[MandatoryVetoSourceRow, ...]
    dependency_policy: DependencyPolicy
    aggregate_state: AggregateState
    safety: SourceWaveSafety

    @model_validator(mode="after")
    def validate_source_surface(self) -> Self:
        required_fields = (
            "source_contract_sha256",
            "independent_producer_identity",
            "formula_category",
            "decision_as_of",
            "available_at",
            "missing_terminal",
            "exact_1202_session_inventory",
        )
        if self.required_source_identity_fields != required_fields:
            raise ValueError("required source identity field set drifted")
        observed = tuple(
            (
                row.veto_id,
                row.legacy_field,
                row.source_independence_class,
                row.architecture_readiness_state,
                row.proposed_formula_category,
                row.successor_admission_state,
                row.blocker_codes,
            )
            for row in self.source_contracts
        )
        if observed != _SOURCE_ROWS:
            raise ValueError("mandatory veto source readiness surface drifted")
        return self


@dataclass(frozen=True)
class ArchitectureFreezeAdmissionLoadResult:
    admission: ArchitectureFreezeAdmission
    path: Path
    file_sha256: str
    canonical_sha256: str
    terminal: Literal["OWNER_FROZEN_ARCHITECTURE_SOURCE_WAVE_NON_EXECUTABLE"]


@dataclass(frozen=True)
class MandatoryVetoSourceContractWaveLoadResult:
    policy: MandatoryVetoSourceContractWave
    path: Path
    file_sha256: str
    canonical_sha256: str
    architecture_freeze: ArchitectureFreezeAdmissionLoadResult
    terminal: Literal["BLOCKED_PRE_R1_MANIFEST_INCOMPLETE_MANDATORY_SOURCE_CONTRACTS"]


def load_architecture_freeze_admission(
    *,
    path: Path = DEFAULT_ARCHITECTURE_FREEZE_PATH,
    project_root: Path = PROJECT_ROOT,
) -> ArchitectureFreezeAdmissionLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="architecture_freeze")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        admission = ArchitectureFreezeAdmission.model_validate(payload)
        architecture = architecture_contract.load_veto_option_signal_architecture(
            project_root=project_root
        )
        actual_identities = (
            (architecture.file_sha256, architecture.canonical_sha256),
            (
                architecture.compatibility_map.file_sha256,
                architecture.compatibility_map.canonical_sha256,
            ),
        )
        for binding, actual in zip(admission.approved_bindings, actual_identities, strict=True):
            bound = _bound_file(Path(binding.path), root=project_root, field=binding.role)
            if hashlib.sha256(bound.read_bytes()).hexdigest() != binding.file_sha256:
                raise ValueError(f"{binding.role} file SHA-256 mismatch")
            if (binding.file_sha256, binding.canonical_sha256) != actual:
                raise ValueError(f"{binding.role} canonical identity drifted")
    except (
        architecture_contract.VetoOptionSignalArchitectureError,
        MandatoryVetoSourceContractError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoSourceContractError(
            "ARCHITECTURE_EXACT_FREEZE_ADMISSION_REJECTED", str(exc)
        ) from exc
    return ArchitectureFreezeAdmissionLoadResult(
        admission=admission,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=admission.canonical_sha256,
        terminal="OWNER_FROZEN_ARCHITECTURE_SOURCE_WAVE_NON_EXECUTABLE",
    )


def load_mandatory_veto_source_contract_wave(
    *,
    path: Path = DEFAULT_SOURCE_CONTRACT_WAVE_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoSourceContractWaveLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="source_contract_wave")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoSourceContractWave.model_validate(payload)
        freeze = load_architecture_freeze_admission(project_root=project_root)
        binding = policy.architecture_freeze
        if (freeze.file_sha256, freeze.canonical_sha256) != (
            binding.file_sha256,
            binding.canonical_sha256,
        ):
            raise ValueError("architecture freeze admission identity drifted")
        bound_freeze = _bound_file(
            Path(binding.path), root=project_root, field="architecture_freeze_binding"
        )
        if hashlib.sha256(bound_freeze.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("architecture freeze admission file SHA-256 mismatch")

        architecture = architecture_contract.load_veto_option_signal_architecture(
            project_root=project_root
        )
        architecture_rows = tuple(
            (
                row.veto_id,
                row.legacy_field,
                row.source_independence_class,
                row.source_contract_state,
            )
            for row in architecture.architecture.mandatory_market_state_vetoes
        )
        policy_rows = tuple(
            (
                row.veto_id,
                row.legacy_field,
                row.source_independence_class,
                row.architecture_readiness_state,
            )
            for row in policy.source_contracts
        )
        if policy_rows != architecture_rows:
            raise ValueError("source wave no longer refines exact-frozen architecture")

        for row in policy.source_contracts:
            for evidence in row.candidate_evidence:
                evidence_path = _bound_file(
                    Path(evidence.path),
                    root=project_root,
                    field=f"{row.veto_id}.candidate_evidence",
                )
                if hashlib.sha256(evidence_path.read_bytes()).hexdigest() != evidence.file_sha256:
                    raise ValueError(f"{row.veto_id} candidate evidence SHA-256 mismatch")
    except (
        architecture_contract.VetoOptionSignalArchitectureError,
        MandatoryVetoSourceContractError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoSourceContractError(
            "MANDATORY_VETO_SOURCE_CONTRACT_WAVE_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoSourceContractWaveLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        architecture_freeze=freeze,
        terminal="BLOCKED_PRE_R1_MANIFEST_INCOMPLETE_MANDATORY_SOURCE_CONTRACTS",
    )


__all__ = [
    "ArchitectureFreezeAdmission",
    "ArchitectureFreezeAdmissionLoadResult",
    "DEFAULT_ARCHITECTURE_FREEZE_PATH",
    "DEFAULT_SOURCE_CONTRACT_WAVE_PATH",
    "MandatoryVetoSourceContractError",
    "MandatoryVetoSourceContractWave",
    "MandatoryVetoSourceContractWaveLoadResult",
    "load_architecture_freeze_admission",
    "load_mandatory_veto_source_contract_wave",
]
