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
from ai_trading_system.qqq_options_research.growth_action_value_real_review import (
    load_growth_action_value_real_review_exact_freeze,
)
from ai_trading_system.strategy_growth_action_value_dq_pit_contract_v3 import (
    load_strategy_growth_action_value_dq_pit_contract_v3,
)
from ai_trading_system.strategy_growth_action_value_freeze_readiness_contract_v4 import (
    load_strategy_growth_action_value_freeze_readiness_contract_v4,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_ARCHITECTURE_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_veto_option_signal_architecture_v1.yaml"
)
DEFAULT_LEGACY_COMPATIBILITY_MAP_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_legacy_veto_compatibility_map_v1.yaml"
)

_TASK_ID = (
    "TRADING-2542F_GROWTH_ACTION_VALUE_VETO_OPTION_SIGNAL_ARCHITECTURE_CONTRACT_V1"
)
_SESSION_INVENTORY_SHA256 = (
    "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_LEGACY_FIELDS = (
    "risk_off_veto",
    "volatility_veto",
    "event_risk_veto",
    "trend_break_veto",
    "tqqq_veto",
)
_LAYERS = (
    "L0_AUTHORITY_AND_IDENTITY",
    "L1_DATA_QUALIFICATION",
    "L2_ACTION_UNIVERSE_CONSTRAINTS",
    "L3_ORTHOGONAL_MARKET_STATE_VETOES",
    "L4_OPTION_ALPHA_SIGNAL",
    "L5_OPTION_RISK_DIAGNOSTICS",
    "L6_NEXT_SESSION_ACTION_JOIN",
)
_TERMINALS = ("PASS", "FAIL", "INSUFFICIENT", "INVALID")
_TERMINAL_PRECEDENCE = ("INVALID", "FAIL", "INSUFFICIENT", "PASS")
_FORBIDDEN_VETO_INPUTS = (
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
_REQUIRED_SOURCE_IDENTITY = (
    "source_contract_sha256",
    "independent_producer_identity",
    "decision_as_of",
    "available_at",
    "missing_terminal",
    "exact_1202_session_inventory",
)
_AUTHORITY_PATHS = (
    "config/research/qc_qqq_options_growth_action_value_real_review_execution_v1.yaml",
    "config/research/qc_qqq_options_growth_action_value_real_review_execution_v2.yaml",
    "config/research/strategy_growth_action_value_canonical_dq_pit_contract_v3.yaml",
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v4.yaml",
    "config/research/"
    "qc_qqq_options_growth_action_value_legacy_veto_compatibility_map_v1.yaml",
)
_LEGACY_MAPPING = (
    (
        "risk_off_veto",
        "broad_market_risk_off_veto",
        "MARKET_STATE_VETO_REQUIRES_INDEPENDENT_PRODUCER",
        "BLOCKED_OWNER_EXACT_FREEZE",
    ),
    (
        "volatility_veto",
        "realized_volatility_veto",
        "MARKET_STATE_VETO_COMPATIBLE_SOURCE_ROLE",
        "SOURCE_CONTRACT_READY_SERIES_NOT_GENERATED",
    ),
    (
        "event_risk_veto",
        "scheduled_event_risk_veto",
        "SCHEDULED_EVENT_VETO_OPTION_EVENT_DIAGNOSTIC_SPLIT",
        "BLOCKED_OWNER_EXACT_FREEZE",
    ),
    (
        "trend_break_veto",
        "underlying_trend_break_veto",
        "ORTHOGONAL_QQQ_UNDERLYING_MARKET_STATE_VETO",
        "BLOCKED_OWNER_EXACT_FREEZE",
    ),
    (
        "tqqq_veto",
        "NO_LEVERAGE_ETF_ACTION_GUARD",
        "ACTION_UNIVERSE_GUARD_COMPATIBILITY_ONLY",
        "REMOVED_FROM_SUCCESSOR_MARKET_CLEAR_GATE",
    ),
)
_MARKET_VETOES = (
    (
        "broad_market_risk_off_veto",
        "risk_off_veto",
        "BROAD_MARKET_INDEPENDENT_SOURCE",
        "BLOCKED_INDEPENDENT_PRODUCER_NOT_FROZEN",
    ),
    (
        "realized_volatility_veto",
        "volatility_veto",
        "REALIZED_VOLATILITY_INDEPENDENT_SOURCE",
        "SOURCE_CONTRACT_READY_SERIES_NOT_GENERATED",
    ),
    (
        "scheduled_event_risk_veto",
        "event_risk_veto",
        "PIT_SCHEDULED_EVENT_CALENDAR_SOURCE",
        "BLOCKED_PIT_SOURCE_NOT_FROZEN",
    ),
    (
        "underlying_trend_break_veto",
        "trend_break_veto",
        "QQQ_UNDERLYING_INDEPENDENT_SOURCE",
        "BLOCKED_ADAPTER_CONTRACT_NOT_FROZEN",
    ),
)


class VetoOptionSignalArchitectureError(ValueError):
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


class ImmutableAuthorityBinding(_StrictModel):
    path: str
    file_sha256: str
    canonical_sha256: str
    role: str
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class LegacyMappingRow(_StrictModel):
    legacy_field: str
    successor_field: str
    successor_role: str
    current_state: str
    legacy_bytes_retained: Literal[True]
    direct_successor_consumption_allowed: Literal[False]


class LegacyCompatibilitySafety(_StrictModel):
    legacy_replay_allowed: Literal[True]
    legacy_field_deletion_allowed: Literal[False]
    legacy_field_in_place_rename_allowed: Literal[False]
    retained_label_truncation_allowed: Literal[False]
    missing_unknown_or_non_pit_as_false_allowed: Literal[False]
    predecessor_rule_override_allowed: Literal[False]
    external_action: Literal["none"]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class LegacyVetoCompatibilityMap(_CanonicalModel):
    schema_version: Literal["growth_action_value_legacy_veto_compatibility_map.v1"]
    map_id: Literal[
        "qc_qqq_options_growth_action_value_legacy_veto_compatibility_map_v1"
    ]
    map_version: Literal["1.0.0-draft.1"]
    status: Literal["DRAFT_FOR_OWNER_EXACT_FREEZE"]
    task_id: Literal[
        "TRADING-2542F_GROWTH_ACTION_VALUE_VETO_OPTION_SIGNAL_ARCHITECTURE_CONTRACT_V1"
    ]
    execution_v1: ImmutableAuthorityBinding
    execution_v2: ImmutableAuthorityBinding
    legacy_taxonomy: tuple[str, ...]
    mapping: tuple[LegacyMappingRow, ...]
    safety: LegacyCompatibilitySafety

    @model_validator(mode="after")
    def validate_exact_map(self) -> Self:
        if self.legacy_taxonomy != _LEGACY_FIELDS:
            raise ValueError("legacy veto taxonomy drifted")
        if (self.execution_v1.path, self.execution_v2.path) != _AUTHORITY_PATHS[:2]:
            raise ValueError("legacy execution authority path drifted")
        observed = tuple(
            (
                row.legacy_field,
                row.successor_field,
                row.successor_role,
                row.current_state,
            )
            for row in self.mapping
        )
        if observed != _LEGACY_MAPPING:
            raise ValueError("legacy compatibility mapping drifted")
        return self


class ArchitectureLayers(_StrictModel):
    ordered_layers: tuple[str, ...]
    dq_is_market_veto: Literal[False]
    action_guard_is_market_veto: Literal[False]
    option_alpha_is_market_veto: Literal[False]
    option_risk_diagnostics_mandatory: Literal[False]

    @model_validator(mode="after")
    def validate_layers(self) -> Self:
        if self.ordered_layers != _LAYERS:
            raise ValueError("architecture layer order drifted")
        return self


class ResearchScope(_StrictModel):
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: Literal[1202]
    pre_window_prior_session: date
    target_session_inventory_lf_sha256: str

    @field_validator("target_session_inventory_lf_sha256")
    @classmethod
    def validate_inventory_sha(cls, value: str) -> str:
        if value != _SESSION_INVENTORY_SHA256:
            raise ValueError("session inventory identity drifted")
        return value

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        expected = (date(2021, 2, 22), date(2025, 12, 2))
        if (self.requested_start, self.requested_end) != expected:
            raise ValueError("requested primary window drifted")
        if (self.evaluated_start, self.evaluated_end) != expected:
            raise ValueError("evaluated primary window drifted")
        if self.pre_window_prior_session != date(2021, 2, 19):
            raise ValueError("pre-window prior drifted")
        return self


class DataQualificationContract(_StrictModel):
    authority: Literal["UNCHANGED_DQ_PIT_V3"]
    terminals: tuple[str, ...]
    precedence: tuple[str, ...]
    non_pass_can_be_market_clear: Literal[False]
    missing_unknown_or_non_pit_terminal: Literal["INVALID"]
    exact_common_session_inventory_required: Literal[True]

    @model_validator(mode="after")
    def validate_terminals(self) -> Self:
        if self.terminals != _TERMINALS or self.precedence != _TERMINAL_PRECEDENCE:
            raise ValueError("DQ terminal semantics drifted")
        return self


class ActionUniverseConstraints(_StrictModel):
    allowed_assets: tuple[str, ...]
    options_position_allowed: Literal[False]
    tqqq_or_qld_allowed: Literal[False]
    borrowed_leverage_allowed: Literal[False]
    historical_tqqq_veto_successor_role: Literal["NO_LEVERAGE_ETF_ACTION_GUARD"]
    action_guard_required_before_join: Literal[True]

    @model_validator(mode="after")
    def validate_assets(self) -> Self:
        if self.allowed_assets != ("QQQ", "SGOV"):
            raise ValueError("action universe drifted")
        return self


class MarketStateVeto(_StrictModel):
    veto_id: str
    legacy_field: str
    source_independence_class: str
    source_contract_state: str
    mandatory: Literal[True]
    source_contract_sha_required: Literal[True]
    independent_producer_required: Literal[True]
    available_at_required: Literal[True]
    missing_terminal_required: Literal[True]
    exact_1202_inventory_required: Literal[True]
    option_alpha_input_allowed: Literal[False]
    series_generation_allowed_now: Literal[False]


class OptionAlphaContract(_StrictModel):
    authority: Literal["IMMUTABLE_EXECUTION_V1_V2_SELECTED_PAIR_ALPHA"]
    selected_call_put_activity_role: Literal["ALPHA_ONLY"]
    signal_effective_timing: Literal["NEXT_VALID_QQQ_SESSION"]
    mandatory_veto_input_allowed: Literal[False]
    strategy_result_input_allowed: Literal[False]


class OptionRiskDiagnostics(_StrictModel):
    role: Literal["OPTIONAL_INDEPENDENT_DIAGNOSTIC_ONLY"]
    current_capability: Literal["NOT_ADMITTED_NO_EXACT_1202_PIT_VALID_SURFACE"]
    contributor_universe: Literal["PRE_SELECTION_RESULT_BLIND_FIXED_BUCKETS"]
    selected_pair_input_allowed: Literal[False]
    adaptive_or_result_dependent_bucket_allowed: Literal[False]
    raw_option_rows_allowed: Literal[False]
    option_sid_allowed: Literal[False]
    mandatory_market_clear_input_allowed_now: Literal[False]
    requires_separate_owner_authorization: Literal[True]
    full_iv_skew_term_surface_state: Literal["DEFERRED_INDEPENDENT_DATA_LANE"]


class DependencyPolicy(_StrictModel):
    forbidden_mandatory_veto_inputs: tuple[str, ...]
    alpha_to_veto_edge_allowed: Literal[False]
    veto_to_alpha_selection_edge_allowed: Literal[False]
    result_to_source_or_bucket_edge_allowed: Literal[False]
    shared_clock_and_identity_allowed: Literal[True]

    @model_validator(mode="after")
    def validate_forbidden_inputs(self) -> Self:
        if self.forbidden_mandatory_veto_inputs != _FORBIDDEN_VETO_INPUTS:
            raise ValueError("forbidden alpha-to-veto input set drifted")
        return self


class JoinContract(_StrictModel):
    dq_required_terminal: Literal["PASS"]
    action_guard_required_terminal: Literal["PASS"]
    mandatory_market_veto_required_value: Literal[False]
    all_mandatory_market_vetoes_must_be_clear: Literal[True]
    option_alpha_consumed_only_after_preconditions: Literal[True]
    next_session_only: Literal[True]
    series_generation_allowed_now: Literal[False]
    target_weight_generation_allowed_now: Literal[False]


class ManifestStopPolicy(_StrictModel):
    required_source_identity_fields: tuple[str, ...]
    stop_before: Literal["R1_MANIFEST_GENERATION"]
    missing_required_source_outcome: Literal[
        "INSUFFICIENT_EVIDENCE_TO_BUILD_R1_MANIFEST"
    ]
    malformed_authority_outcome: Literal["PRE_RUN_AUTHORITY_INVALID"]
    constant_false_fill_allowed: Literal[False]
    retained_series_truncation_allowed: Literal[False]
    cross_date_fallback_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_required_fields(self) -> Self:
        if self.required_source_identity_fields != _REQUIRED_SOURCE_IDENTITY:
            raise ValueError("manifest stop identity surface drifted")
        return self


class ArchitectureSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    owner_exact_freeze_required_before_source_wave: Literal[True]
    veto_series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    cache_read_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    real_dq_authorized: Literal[False]
    backtest_authorized: Literal[False]
    candidate_or_threshold_search_allowed: Literal[False]
    order_generation_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class VetoOptionSignalArchitecture(_CanonicalModel):
    schema_version: Literal["growth_action_value_veto_option_signal_architecture.v1"]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_veto_option_signal_architecture_v1"
    ]
    policy_version: Literal["1.0.0-draft.1"]
    policy_family_generation: Literal["RESULT_BLIND_SUCCESSOR_V3"]
    status: Literal["DRAFT_FOR_OWNER_EXACT_FREEZE_NON_EXECUTABLE"]
    task_id: Literal[
        "TRADING-2542F_GROWTH_ACTION_VALUE_VETO_OPTION_SIGNAL_ARCHITECTURE_CONTRACT_V1"
    ]
    owner_decision_id: Literal[
        "owner_decision:TRADING-2542F:2026-08-25:authorize_serial_veto_option_architecture_contract_wave_v1"
    ]
    authority_bindings: tuple[ImmutableAuthorityBinding, ...]
    research_scope: ResearchScope
    layers: ArchitectureLayers
    data_qualification: DataQualificationContract
    action_universe_constraints: ActionUniverseConstraints
    mandatory_market_state_vetoes: tuple[MarketStateVeto, ...]
    option_alpha: OptionAlphaContract
    option_risk_diagnostics: OptionRiskDiagnostics
    dependency_policy: DependencyPolicy
    next_session_join: JoinContract
    manifest_stop_policy: ManifestStopPolicy
    safety: ArchitectureSafety

    @model_validator(mode="after")
    def validate_exact_architecture(self) -> Self:
        if tuple(row.path for row in self.authority_bindings) != _AUTHORITY_PATHS:
            raise ValueError("architecture authority path order drifted")
        observed = tuple(
            (
                row.veto_id,
                row.legacy_field,
                row.source_independence_class,
                row.source_contract_state,
            )
            for row in self.mandatory_market_state_vetoes
        )
        if observed != _MARKET_VETOES:
            raise ValueError("mandatory market-state veto set drifted")
        if any(row.legacy_field == "tqqq_veto" for row in self.mandatory_market_state_vetoes):
            raise ValueError("tqqq_veto cannot remain in successor market-state gate")
        return self


@dataclass(frozen=True)
class LegacyVetoCompatibilityMapLoadResult:
    compatibility_map: LegacyVetoCompatibilityMap
    path: Path
    file_sha256: str
    canonical_sha256: str


@dataclass(frozen=True)
class VetoOptionSignalArchitectureLoadResult:
    architecture: VetoOptionSignalArchitecture
    path: Path
    file_sha256: str
    canonical_sha256: str
    compatibility_map: LegacyVetoCompatibilityMapLoadResult
    terminal: Literal["DRAFT_READY_FOR_OWNER_EXACT_FREEZE_NO_EXECUTION_AUTHORITY"]


def load_legacy_veto_compatibility_map(
    *,
    path: Path = DEFAULT_LEGACY_COMPATIBILITY_MAP_PATH,
    project_root: Path = PROJECT_ROOT,
) -> LegacyVetoCompatibilityMapLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="legacy_compatibility_map")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        compatibility_map = LegacyVetoCompatibilityMap.model_validate(payload)
        freeze = load_growth_action_value_real_review_exact_freeze(
            project_root=project_root
        )
        for field, binding, actual_file, actual_canonical in (
            (
                "execution_v1",
                compatibility_map.execution_v1,
                freeze.approved_draft.policy_file_sha256,
                freeze.approved_draft.policy_canonical_sha256,
            ),
            (
                "execution_v2",
                compatibility_map.execution_v2,
                freeze.policy_file_sha256,
                freeze.policy_canonical_sha256,
            ),
        ):
            bound = _bound_file(Path(binding.path), root=project_root, field=field)
            if hashlib.sha256(bound.read_bytes()).hexdigest() != binding.file_sha256:
                raise ValueError(f"{field} file SHA-256 mismatch")
            if binding.file_sha256 != actual_file:
                raise ValueError(f"{field} immutable file identity drifted")
            if binding.canonical_sha256 != actual_canonical:
                raise ValueError(f"{field} immutable canonical identity drifted")
    except VetoOptionSignalArchitectureError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise VetoOptionSignalArchitectureError(
            "LEGACY_VETO_COMPATIBILITY_MAP_REJECTED", str(exc)
        ) from exc
    return LegacyVetoCompatibilityMapLoadResult(
        compatibility_map=compatibility_map,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=compatibility_map.canonical_sha256,
    )


def load_veto_option_signal_architecture(
    *,
    path: Path = DEFAULT_ARCHITECTURE_PATH,
    project_root: Path = PROJECT_ROOT,
) -> VetoOptionSignalArchitectureLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="architecture_policy")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        architecture = VetoOptionSignalArchitecture.model_validate(payload)
        compatibility = load_legacy_veto_compatibility_map(project_root=project_root)
        freeze = load_growth_action_value_real_review_exact_freeze(
            project_root=project_root
        )
        dq = load_strategy_growth_action_value_dq_pit_contract_v3(
            project_root=project_root
        )
        sheet = load_strategy_growth_action_value_freeze_readiness_contract_v4(
            project_root=project_root
        )
        expected = (
            (
                freeze.approved_draft.policy_file_sha256,
                freeze.approved_draft.policy_canonical_sha256,
            ),
            (freeze.policy_file_sha256, freeze.policy_canonical_sha256),
            (dq.contract_file_sha256, dq.contract_canonical_sha256),
            (sheet.contract_file_sha256, sheet.contract_canonical_sha256),
            (compatibility.file_sha256, compatibility.canonical_sha256),
        )
        for binding, identity in zip(architecture.authority_bindings, expected, strict=True):
            bound = _bound_file(
                Path(binding.path), root=project_root, field=binding.role
            )
            if hashlib.sha256(bound.read_bytes()).hexdigest() != binding.file_sha256:
                raise ValueError(f"{binding.role} file SHA-256 mismatch")
            if (binding.file_sha256, binding.canonical_sha256) != identity:
                raise ValueError(f"{binding.role} immutable identity drifted")
    except VetoOptionSignalArchitectureError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise VetoOptionSignalArchitectureError(
            "VETO_OPTION_SIGNAL_ARCHITECTURE_REJECTED", str(exc)
        ) from exc
    return VetoOptionSignalArchitectureLoadResult(
        architecture=architecture,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=architecture.canonical_sha256,
        compatibility_map=compatibility,
        terminal="DRAFT_READY_FOR_OWNER_EXACT_FREEZE_NO_EXECUTION_AUTHORITY",
    )


__all__ = [
    "DEFAULT_ARCHITECTURE_PATH",
    "DEFAULT_LEGACY_COMPATIBILITY_MAP_PATH",
    "LegacyVetoCompatibilityMap",
    "LegacyVetoCompatibilityMapLoadResult",
    "VetoOptionSignalArchitecture",
    "VetoOptionSignalArchitectureError",
    "VetoOptionSignalArchitectureLoadResult",
    "load_legacy_veto_compatibility_map",
    "load_veto_option_signal_architecture",
]
