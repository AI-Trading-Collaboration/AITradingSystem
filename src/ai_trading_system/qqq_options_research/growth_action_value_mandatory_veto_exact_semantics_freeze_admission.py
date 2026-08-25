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
    growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v2 as v2_draft,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_EXACT_SEMANTICS_FREEZE_ADMISSION_PATH = Path(
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "exact_semantics_freeze_admission_v1.yaml"
)

_SEMANTICS_PATH = (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_calculation_semantics_v1.yaml"
)
_V2_PATH = (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "owner_freeze_decision_pack_draft_v2.yaml"
)
_SEMANTICS_FILE_SHA256 = "813c2eb2bb0d4b4f7673048889b66fa843b739a48405cc2e87272d925dd7b0d0"
_SEMANTICS_CANONICAL_SHA256 = (
    "824ef20a66e4eba3c2841489cae8b03ff3a6cad4f73003469c086d8e09237cf1"
)
_V2_FILE_SHA256 = "d08480c07047e636f8b4a8208ec60406acd5debdc60f30541411310e401b789f"
_V2_CANONICAL_SHA256 = (
    "99ed7dbdac82faf594633ab25be1ffb1417709030af0817fb19c4ace332dc389"
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
        (
            "CALLABLE_PRODUCER_NOT_IMPLEMENTED",
            "SYNTHETIC_CONFORMANCE_NOT_ADMITTED",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
    (
        "realized_volatility_veto",
        "realized_volatility_vix252_or_qqq_rv20_v2_exact_semantics_proposal",
        "volatility_compression_free_v1_successor_adapter",
        (
            "SUCCESSOR_ADAPTER_CONFORMANCE_NOT_ADMITTED",
            "VIX_SOURCE_TIMING_IDENTITY_NOT_ADMITTED",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
    (
        "scheduled_event_risk_veto",
        "scheduled_event_official_next_session_any_v2_exact_semantics_proposal",
        "official_macro_release_calendar_pit_v1",
        (
            "OFFICIAL_PIT_ADAPTER_NOT_IMPLEMENTED",
            "COVERAGE_RECEIPT_CONFORMANCE_NOT_ADMITTED",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
    (
        "underlying_trend_break_veto",
        "qqq_sma200_drawdown63_confirmed_recovery_v2_exact_semantics_proposal",
        "qqq_underlying_trend_break_v1",
        (
            "STATEFUL_PRODUCER_NOT_IMPLEMENTED",
            "CHECKPOINT_REPLAY_CONFORMANCE_NOT_ADMITTED",
            "EXACT_1202_INVENTORY_NOT_ADMITTED",
        ),
    ),
)


class MandatoryVetoExactSemanticsFreezeAdmissionError(ValueError):
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
        "owner_decision:TRADING-2542G:S4B:2026-08-26:"
        "freeze_s4a_v2_exact_semantics_and_continue_non_executable_admission_v1"
    ]
    calculation_semantics_exact_freeze_granted: Literal[True]
    all_four_veto_objects_frozen_together: Literal[True]
    predecessor_bytes_must_remain_immutable: Literal[True]
    separate_synthetic_producer_followup_authorized: Literal[True]
    source_contract_admission_authorized: Literal[False]
    exact_inventory_or_series_authorized: Literal[False]
    real_data_or_backtest_authorized: Literal[False]
    authorization_state: Literal["STANDING_OWNER_SCOPE"]


class AuthorityBinding(_StrictModel):
    path: str
    artifact_id: str
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


class FreezeRow(_StrictModel):
    veto_id: str
    recommendation_id: str
    producer_id: str
    exact_semantics_authority_role: Literal["OWNER_EXACT_FROZEN_IMMUTABLE_POLICY"]
    owner_exact_freeze_granted: Literal[True]
    producer_contract_admitted: Literal[False]
    producer_callable_conformance_admitted: Literal[False]
    exact_1202_session_inventory_admitted: Literal[False]
    observed_inventory_lf_sha256: None
    series_generation_allowed: Literal[False]
    remaining_blockers: tuple[str, ...]


class AggregateState(_StrictModel):
    owner_exact_frozen_vetoes: tuple[str, ...]
    admitted_producer_contracts: tuple[()]
    admitted_exact_1202_session_inventories: tuple[()]
    terminal: Literal["OWNER_EXACT_FROZEN_4_OF_4_PRODUCER_UNADMITTED_0_OF_4"]
    next_legal_action: Literal[
        "BEGIN_SEPARATE_NON_EXECUTABLE_SYNTHETIC_PRODUCER_CONTRACT_WAVE_FROM_NEW_EXACT_BASE"
    ]
    predecessor_terminal_superseded: Literal[
        "OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"
    ]


class FreezeSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    owner_exact_freeze_admission_only: Literal[True]
    producer_implementation_allowed_in_this_wave: Literal[False]
    synthetic_producer_followup_requires_new_exact_base: Literal[True]
    source_contract_admission_allowed: Literal[False]
    exact_inventory_admission_allowed: Literal[False]
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


class MandatoryVetoExactSemanticsFreezeAdmission(_CanonicalModel):
    schema_version: Literal[
        "growth_action_value_mandatory_veto_exact_semantics_freeze_admission.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_growth_action_value_mandatory_veto_"
        "exact_semantics_freeze_admission_v1"
    ]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_EXACT_FROZEN_4_OF_4_PRODUCER_UNADMITTED_0_OF_4"]
    task_id: Literal[
        "TRADING-2542G_GROWTH_ACTION_VALUE_MANDATORY_VETO_SOURCE_CONTRACT_WAVE_V1"
    ]
    owner_decision: OwnerDecision
    authority_bindings: tuple[AuthorityBinding, AuthorityBinding]
    freeze_rows: tuple[FreezeRow, FreezeRow, FreezeRow, FreezeRow]
    aggregate_state: AggregateState
    safety: FreezeSafety

    @model_validator(mode="after")
    def validate_exact_surface(self) -> Self:
        observed_bindings = tuple(
            (item.path, item.file_sha256, item.canonical_sha256, item.role)
            for item in self.authority_bindings
        )
        if observed_bindings != (
            (
                _SEMANTICS_PATH,
                _SEMANTICS_FILE_SHA256,
                _SEMANTICS_CANONICAL_SHA256,
                "OWNER_EXACT_FROZEN_SHARED_CALCULATION_TIME_STATE_AUTHORITY",
            ),
            (
                _V2_PATH,
                _V2_FILE_SHA256,
                _V2_CANONICAL_SHA256,
                "OWNER_EXACT_FROZEN_FOUR_VETO_TYPED_SEMANTICS_AUTHORITY",
            ),
        ):
            raise ValueError("freeze authority binding surface drifted")
        observed_rows = tuple(
            (row.veto_id, row.recommendation_id, row.producer_id, row.remaining_blockers)
            for row in self.freeze_rows
        )
        if observed_rows != _ROW_SURFACE:
            raise ValueError("freeze row surface drifted")
        if self.aggregate_state.owner_exact_frozen_vetoes != _VETO_IDS:
            raise ValueError("atomic four-veto freeze inventory drifted")
        return self


@dataclass(frozen=True)
class MandatoryVetoExactSemanticsFreezeAdmissionLoadResult:
    policy: MandatoryVetoExactSemanticsFreezeAdmission
    path: Path
    file_sha256: str
    canonical_sha256: str
    v2_draft: v2_draft.MandatoryVetoOwnerFreezeDecisionPackDraftV2LoadResult
    terminal: Literal["OWNER_EXACT_FROZEN_4_OF_4_PRODUCER_UNADMITTED_0_OF_4"]


def load_mandatory_veto_exact_semantics_freeze_admission(
    *,
    path: Path = DEFAULT_EXACT_SEMANTICS_FREEZE_ADMISSION_PATH,
    project_root: Path = PROJECT_ROOT,
) -> MandatoryVetoExactSemanticsFreezeAdmissionLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="freeze_admission")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = MandatoryVetoExactSemanticsFreezeAdmission.model_validate(payload)
        loaded_v2 = v2_draft.load_mandatory_veto_owner_freeze_decision_pack_draft_v2(
            project_root=project_root
        )
        semantics_binding, v2_binding = policy.authority_bindings
        if (
            loaded_v2.calculation_semantics.file_sha256,
            loaded_v2.calculation_semantics.canonical_sha256,
        ) != (semantics_binding.file_sha256, semantics_binding.canonical_sha256):
            raise ValueError("calculation-semantics loader identity drifted")
        if (loaded_v2.file_sha256, loaded_v2.canonical_sha256) != (
            v2_binding.file_sha256,
            v2_binding.canonical_sha256,
        ):
            raise ValueError("V2 decision-pack loader identity drifted")
        for binding in policy.authority_bindings:
            authority = _bound_file(
                Path(binding.path), root=project_root, field="authority_binding"
            )
            if hashlib.sha256(authority.read_bytes()).hexdigest() != binding.file_sha256:
                raise ValueError(f"authority file SHA-256 mismatch: {binding.path}")
        v2_surface = tuple(
            (row.veto_id, row.recommendation_id, row.producer_contract.producer_id)
            for row in loaded_v2.policy.decision_rows
        )
        freeze_surface = tuple(
            (row.veto_id, row.recommendation_id, row.producer_id)
            for row in policy.freeze_rows
        )
        if v2_surface != freeze_surface:
            raise ValueError("freeze rows do not exactly replay the V2 decision surface")
    except (
        MandatoryVetoExactSemanticsFreezeAdmissionError,
        v2_draft.MandatoryVetoExactSemanticsDraftError,
    ):
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise MandatoryVetoExactSemanticsFreezeAdmissionError(
            "MANDATORY_VETO_EXACT_SEMANTICS_FREEZE_ADMISSION_REJECTED", str(exc)
        ) from exc
    return MandatoryVetoExactSemanticsFreezeAdmissionLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        v2_draft=loaded_v2,
        terminal="OWNER_EXACT_FROZEN_4_OF_4_PRODUCER_UNADMITTED_0_OF_4",
    )


__all__ = [
    "DEFAULT_EXACT_SEMANTICS_FREEZE_ADMISSION_PATH",
    "MandatoryVetoExactSemanticsFreezeAdmission",
    "MandatoryVetoExactSemanticsFreezeAdmissionError",
    "MandatoryVetoExactSemanticsFreezeAdmissionLoadResult",
    "load_mandatory_veto_exact_semantics_freeze_admission",
]
