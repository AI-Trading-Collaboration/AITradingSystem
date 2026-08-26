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
    growth_action_value_mandatory_veto_exact_semantics_freeze_admission as freeze,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_SYNTHETIC_PRODUCER_CONTRACT_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_synthetic_producer_contract_v1.yaml"
)

_FREEZE_PATH = (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "exact_semantics_freeze_admission_v1.yaml"
)
_FREEZE_FILE_SHA256 = "ef075527750efd24433eafbd8a2e586104562868f4ce2b666043069fe5368765"
_FREEZE_CANONICAL_SHA256 = (
    "97f3678417b5dcb0a4965a308953552d17d17e4cb947532316dceca2506df147"
)
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
        "broad_market_risk_off_spy_sma200_drawdown63_v2_exact_semantics_proposal",
        "qqq_options_growth_action_value_broad_market_risk_off_v1",
        "evaluate_broad_market_risk_off",
    ),
    (
        "realized_volatility_veto",
        "realized_volatility_vix252_or_qqq_rv20_v2_exact_semantics_proposal",
        "volatility_compression_free_v1_successor_adapter",
        "evaluate_realized_volatility_veto",
    ),
    (
        "scheduled_event_risk_veto",
        "scheduled_event_official_next_session_any_v2_exact_semantics_proposal",
        "official_macro_release_calendar_pit_v1",
        "evaluate_scheduled_event_risk",
    ),
    (
        "underlying_trend_break_veto",
        "qqq_sma200_drawdown63_confirmed_recovery_v2_exact_semantics_proposal",
        "qqq_underlying_trend_break_v1",
        "evaluate_underlying_trend_break",
    ),
)


class MandatoryVetoSyntheticProducerContractError(ValueError):
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


class FreezeAdmissionBinding(_StrictModel):
    path: Literal[
        "config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "exact_semantics_freeze_admission_v1.yaml"
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
            _FREEZE_FILE_SHA256,
            _FREEZE_CANONICAL_SHA256,
        ):
            raise ValueError("S4B freeze-admission exact identity drifted")
        return self


class AuthorizationScope(_StrictModel):
    decision_ref: Literal[
        "owner_decision:TRADING-2542G:S4B:2026-08-26:"
        "freeze_s4a_v2_exact_semantics_and_continue_non_executable_admission_v1"
    ]
    separate_synthetic_producer_followup_authorized: Literal[True]
    pure_in_memory_synthetic_conformance_only: Literal[True]
    real_source_adapter_admission_authorized: Literal[False]
    exact_inventory_or_series_authorized: Literal[False]
    provider_cache_dq_or_backtest_authorized: Literal[False]
    authorization_state: Literal["STANDING_OWNER_SCOPE"]


class ProducerRow(_StrictModel):
    veto_id: str
    recommendation_id: str
    producer_id: str
    callable_name: str
    conformance_state: Literal[
        "SYNTHETIC_CALLABLE_CONFORMANCE_IMPLEMENTED_NOT_SOURCE_ADMITTED"
    ]
    exact_semantics_owner_frozen: Literal[True]
    synthetic_callable_conformance_implemented: Literal[True]
    producer_contract_admitted: Literal[False]
    real_source_identity_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    observed_inventory_lf_sha256: None
    series_generation_allowed: Literal[False]
    remaining_blockers: tuple[str, ...]


class AggregateState(_StrictModel):
    synthetic_callable_conformance_ready: tuple[str, ...]
    admitted_producer_contracts: tuple[()]
    admitted_real_source_identities: tuple[()]
    admitted_exact_1202_session_inventories: tuple[()]
    terminal: Literal[
        "SYNTHETIC_CALLABLE_CONFORMANCE_READY_4_OF_4_SOURCE_UNADMITTED_0_OF_4"
    ]
    next_legal_action: Literal[
        "REVIEW_REAL_SOURCE_ADAPTER_CONTRACT_AND_EXACT_INVENTORY_ADMISSION_PLAN"
    ]


class Safety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    pure_in_memory_synthetic_inputs_only: Literal[True]
    filesystem_or_network_io_allowed_in_producers: Literal[False]
    real_source_contract_admission_allowed: Literal[False]
    exact_inventory_admission_allowed: Literal[False]
    veto_series_generation_allowed: Literal[False]
    r1_manifest_generation_allowed: Literal[False]
    cache_read_authorized: Literal[False]
    provider_query_authorized: Literal[False]
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


class MandatoryVetoSyntheticProducerContract(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_synthetic_producer_contract.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_synthetic_producer_contract_v1"
    ]
    policy_version: Literal["1.0.0"]
    status: Literal[
        "SYNTHETIC_CALLABLE_CONFORMANCE_READY_4_OF_4_SOURCE_UNADMITTED_0_OF_4"
    ]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    freeze_admission_binding: FreezeAdmissionBinding
    authorization_scope: AuthorizationScope
    producer_rows: tuple[ProducerRow, ProducerRow, ProducerRow, ProducerRow]
    aggregate_state: AggregateState
    safety: Safety

    @model_validator(mode="after")
    def validate_surface(self) -> Self:
        observed = tuple(
            (row.veto_id, row.recommendation_id, row.producer_id, row.callable_name)
            for row in self.producer_rows
        )
        if observed != _ROW_SURFACE:
            raise ValueError("synthetic producer row surface drifted")
        if self.aggregate_state.synthetic_callable_conformance_ready != _VETO_IDS:
            raise ValueError("synthetic conformance inventory drifted")
        return self


@dataclass(frozen=True)
class MandatoryVetoSyntheticProducerContractLoadResult:
    policy: MandatoryVetoSyntheticProducerContract
    path: Path
    file_sha256: str
    canonical_sha256: str
    freeze_admission: freeze.MandatoryVetoExactSemanticsFreezeAdmissionLoadResult
    terminal: Literal[
        "SYNTHETIC_CALLABLE_CONFORMANCE_READY_4_OF_4_SOURCE_UNADMITTED_0_OF_4"
    ]


def load_mandatory_veto_synthetic_producer_contract(
    *,
    path: Path = DEFAULT_SYNTHETIC_PRODUCER_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoSyntheticProducerContractLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="synthetic_producer_contract")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoSyntheticProducerContract.model_validate(payload)
        loaded_freeze = freeze.load_mandatory_veto_exact_semantics_freeze_admission(
            project_root=project_root
        )
        binding = policy.freeze_admission_binding
        if (loaded_freeze.file_sha256, loaded_freeze.canonical_sha256) != (
            binding.file_sha256,
            binding.canonical_sha256,
        ):
            raise ValueError("S4B freeze-admission loader identity drifted")
        bound_freeze = _bound_file(
            Path(binding.path), root=project_root, field="freeze_admission_binding"
        )
        if hashlib.sha256(bound_freeze.read_bytes()).hexdigest() != binding.file_sha256:
            raise ValueError("S4B freeze-admission bound file SHA-256 mismatch")
        freeze_surface = tuple(
            (row.veto_id, row.recommendation_id, row.producer_id)
            for row in loaded_freeze.policy.freeze_rows
        )
        producer_surface = tuple(
            (row.veto_id, row.recommendation_id, row.producer_id)
            for row in policy.producer_rows
        )
        if freeze_surface != producer_surface:
            raise ValueError("synthetic producers do not replay the frozen S4B row surface")
    except (
        MandatoryVetoSyntheticProducerContractError,
        freeze.MandatoryVetoExactSemanticsFreezeAdmissionError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoSyntheticProducerContractError(
            "MANDATORY_VETO_SYNTHETIC_PRODUCER_CONTRACT_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoSyntheticProducerContractLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        freeze_admission=loaded_freeze,
        terminal="SYNTHETIC_CALLABLE_CONFORMANCE_READY_4_OF_4_SOURCE_UNADMITTED_0_OF_4",
    )


__all__ = [
    "DEFAULT_SYNTHETIC_PRODUCER_CONTRACT_PATH",
    "MandatoryVetoSyntheticProducerContract",
    "MandatoryVetoSyntheticProducerContractError",
    "MandatoryVetoSyntheticProducerContractLoadResult",
    "load_mandatory_veto_synthetic_producer_contract",
]
