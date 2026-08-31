from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_quality import (
    frozen_signal_value_confirmation_preregistration as preregistration,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_FROZEN_SIGNAL_VALUE_CONFIRMATION_PREREGISTRATION_FREEZE_ADMISSION_PATH = Path(
    "config/research/frozen_signal_value_confirmation_preregistration_freeze_admission_v1.yaml"
)

_TASK_ID = "TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1"
_POLICY_PATH = "config/research/frozen_signal_value_confirmation_preregistration_v1.yaml"
_POLICY_FILE_SHA256 = "507ab3dd3610971c0962fa093cec0c7f09e1b816f694b7dd946c4b9703013dfa"
_POLICY_CANONICAL_SHA256 = (
    "7d12dd62127cb02676d4e18510c06fddc9e2a0afa03ec2f0e758ba6143bed88c"
)
_AUTHORITY_SET_SHA256 = (
    "45d508d563b46b0929d80687155213d265399a4f105da69f31810780a34c754f"
)
_APPROVED_INSTRUCTION = (
    "批准 `frozen_signal_value_confirmation_preregistration_v1@1.0.0-draft.1`（file SHA "
    "`507ab3dd3610971c0962fa093cec0c7f09e1b816f694b7dd946c4b9703013dfa` / canonical SHA "
    "`7d12dd62127cb02676d4e18510c06fddc9e2a0afa03ec2f0e758ba6143bed88c`）全部所列 signal "
    "identity、candidate、exposure-matched comparator、USD 100,000 common capital、adjusted-close "
    "calendar、5 bps one-way cost、primary estimand、zero thresholds、drawdown non-regression 与 "
    "reducer precedence 规则按草案精确冻结；仅限 non-executable DATA_RESEARCH；"
    "不授权市场数据读取或下载、DQ、confirmation/backtest、QuantConnect/provider/cache、"
    "paper/live/production/broker 或任何 "
    "orders/fills/positions。"
)
_FROZEN_SECTIONS = (
    "signal_identity",
    "candidate_implementation",
    "primary_comparator",
    "accounting_and_clock",
    "cost_model",
    "primary_metric",
    "falsification_guard",
    "verdict_reducer",
    "safety",
)
_TERMINAL: Literal[
    "OWNER_EXACT_PREREGISTRATION_FROZEN_NO_EMPIRICAL_RUN_AUTHORITY"
] = "OWNER_EXACT_PREREGISTRATION_FROZEN_NO_EMPIRICAL_RUN_AUTHORITY"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class FrozenSignalValuePreregistrationFreezeAdmissionError(ValueError):
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
        "owner_decision:TRADING-2550:2026-09-01:freeze_signal_value_confirmation_preregistration_v1"
    ]
    approved_instruction: str
    exact_preregistration_freeze_granted: Literal[True]
    full_listed_surface_frozen: Literal[True]
    predecessor_bytes_must_remain_immutable: Literal[True]
    approved_file_sha256: str
    approved_canonical_sha256: str
    empirical_run_authorized: Literal[False]
    authorization_state: Literal["EXACT_PREAUTHORIZED"]

    @field_validator("approved_file_sha256", "approved_canonical_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("identity must be a lowercase SHA-256")
        return value

    @model_validator(mode="after")
    def validate_instruction_and_identity(self) -> Self:
        if self.approved_instruction != _APPROVED_INSTRUCTION:
            raise ValueError("owner approved instruction drifted")
        if (self.approved_file_sha256, self.approved_canonical_sha256) != (
            _POLICY_FILE_SHA256,
            _POLICY_CANONICAL_SHA256,
        ):
            raise ValueError("owner approved identity drifted")
        return self


class AuthorityBinding(_StrictModel):
    path: str
    artifact_id: Literal["frozen_signal_value_confirmation_preregistration_v1"]
    artifact_version: Literal["1.0.0-draft.1"]
    file_sha256: str
    canonical_sha256: str
    authority_set_sha256: str
    role: Literal["OWNER_EXACT_FROZEN_SIGNAL_VALUE_PREREGISTRATION"]
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256", "authority_set_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("identity must be a lowercase SHA-256")
        return value


class FrozenSurface(_StrictModel):
    frozen_sections: tuple[str, ...]
    predecessor_policy_status_preserved: Literal["DRAFT_OWNER_REVIEW_REQUIRED"]
    predecessor_owner_review_state_preserved: Literal["OWNER_REVIEW_REQUIRED"]
    owner_exact_frozen_via_separate_admission: Literal[True]
    signal_package_id: Literal["trading_2542i_operational_forecast_real_v3"]
    signal_session_count: Literal[1202]
    return_interval_count: Literal[1201]
    candidate_implementation_id: Literal[
        "FROZEN_SIGNAL_FULLY_FUNDED_QQQ_ZERO_RETURN_CASH"
    ]
    primary_comparator_id: Literal["EXPOSURE_MATCHED_STATIC_QQQ_ZERO_RETURN_CASH"]
    initial_capital_usd: Literal[100000]
    price_field: Literal["ADJUSTED_CLOSE"]
    return_clock: Literal["EFFECTIVE_SESSION_CLOSE_TO_NEXT_XNYS_SESSION_CLOSE"]
    one_way_cost_bps: float
    primary_metric_id: Literal["NET_TOTAL_RETURN_DIFFERENCE_PERCENTAGE_POINTS"]
    return_threshold_strictly_greater_than: float
    drawdown_guard_id: Literal["MAX_DRAWDOWN_MAGNITUDE_NON_REGRESSION"]
    drawdown_threshold_less_than_or_equal_to: float
    reducer_precedence: tuple[Literal["INSUFFICIENT", "REJECT", "RETAIN"], ...]

    @model_validator(mode="after")
    def validate_exact_surface(self) -> Self:
        if self.frozen_sections != _FROZEN_SECTIONS:
            raise ValueError("frozen section inventory drifted")
        if (
            self.one_way_cost_bps,
            self.return_threshold_strictly_greater_than,
            self.drawdown_threshold_less_than_or_equal_to,
        ) != (5.0, 0.0, 0.0):
            raise ValueError("cost or zero-threshold freeze drifted")
        if self.reducer_precedence != ("INSUFFICIENT", "REJECT", "RETAIN"):
            raise ValueError("reducer precedence freeze drifted")
        return self


class EmpiricalState(_StrictModel):
    signal_value_verdict: Literal["UNRESOLVED"]
    empirical_confirmation_completed: Literal[False]
    preregistration_freeze_complete_after_admission: Literal[True]
    task_status_after_admission: Literal["BASELINE_DONE"]
    successor_task_implicitly_created: Literal[False]
    next_legal_action: Literal[
        "OWNER_SEPARATE_EXACT_BOUNDED_RUN_AUTHORIZATION_REQUIRED"
    ]


class FreezeSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    owner_exact_freeze_admission_only: Literal[True]
    predecessor_policy_mutation_allowed: Literal[False]
    outcome_access_authorized: Literal[False]
    market_data_read_authorized: Literal[False]
    market_data_download_authorized: Literal[False]
    dq_authorized: Literal[False]
    confirmation_authorized: Literal[False]
    backtest_authorized: Literal[False]
    quantconnect_authorized: Literal[False]
    provider_authorized: Literal[False]
    cache_mutation_authorized: Literal[False]
    option_data_use_authorized: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    investment_conclusion_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class FrozenSignalValuePreregistrationFreezeAdmission(_CanonicalModel):
    schema_version: Literal[
        "frozen_signal_value_confirmation_preregistration_freeze_admission.v1"
    ]
    admission_id: Literal[
        "frozen_signal_value_confirmation_preregistration_freeze_admission_v1"
    ]
    admission_version: Literal["1.0.0"]
    status: Literal["OWNER_EXACT_PREREGISTRATION_FROZEN_NO_EMPIRICAL_RUN_AUTHORITY"]
    task_id: str
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    owner_decision: OwnerDecision
    authority_binding: AuthorityBinding
    frozen_surface: FrozenSurface
    empirical_state: EmpiricalState
    safety: FreezeSafety
    terminal: Literal["OWNER_EXACT_PREREGISTRATION_FROZEN_NO_EMPIRICAL_RUN_AUTHORITY"]

    @model_validator(mode="after")
    def validate_exact_admission(self) -> Self:
        if self.task_id != _TASK_ID:
            raise ValueError("task identity drifted")
        binding = self.authority_binding
        if (
            binding.path,
            binding.file_sha256,
            binding.canonical_sha256,
            binding.authority_set_sha256,
        ) != (
            _POLICY_PATH,
            _POLICY_FILE_SHA256,
            _POLICY_CANONICAL_SHA256,
            _AUTHORITY_SET_SHA256,
        ):
            raise ValueError("approved preregistration identity drifted")
        return self


@dataclass(frozen=True)
class FrozenSignalValuePreregistrationFreezeAdmissionLoadResult:
    admission: FrozenSignalValuePreregistrationFreezeAdmission
    path: Path
    file_sha256: str
    canonical_sha256: str
    preregistration: preregistration.FrozenSignalValuePreregistrationLoadResult
    terminal: Literal["OWNER_EXACT_PREREGISTRATION_FROZEN_NO_EMPIRICAL_RUN_AUTHORITY"]


def load_frozen_signal_value_confirmation_preregistration_freeze_admission(
    *,
    path: Path = DEFAULT_FROZEN_SIGNAL_VALUE_CONFIRMATION_PREREGISTRATION_FREEZE_ADMISSION_PATH,
    project_root: Path = PROJECT_ROOT,
) -> FrozenSignalValuePreregistrationFreezeAdmissionLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="freeze_admission")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        admission = FrozenSignalValuePreregistrationFreezeAdmission.model_validate(payload)
        loaded = preregistration.load_frozen_signal_value_confirmation_preregistration(
            policy_path=Path(admission.authority_binding.path), project_root=project_root
        )
        if (
            loaded.policy_file_sha256,
            loaded.policy_canonical_sha256,
            loaded.authority_set_sha256,
        ) != (
            admission.authority_binding.file_sha256,
            admission.authority_binding.canonical_sha256,
            admission.authority_binding.authority_set_sha256,
        ):
            raise ValueError("approved preregistration loader identity drifted")
        policy = loaded.policy
        surface = admission.frozen_surface
        replay = (
            policy.policy_status,
            policy.owner_review.decision_state,
            policy.signal_identity.package_id,
            policy.signal_identity.expected_signal_sessions,
            policy.signal_identity.expected_return_intervals,
            policy.candidate_implementation.implementation_id,
            policy.primary_comparator.comparator_id,
            policy.accounting_and_clock.initial_capital_usd,
            policy.accounting_and_clock.price_field,
            policy.accounting_and_clock.return_clock,
            policy.cost_model.one_way_cost_bps,
            policy.primary_metric.metric_id,
            policy.primary_metric.retain_threshold_strictly_greater_than,
            policy.falsification_guard.guard_id,
            policy.falsification_guard.retain_threshold_less_than_or_equal_to,
            policy.verdict_reducer.precedence,
        )
        expected = (
            surface.predecessor_policy_status_preserved,
            surface.predecessor_owner_review_state_preserved,
            surface.signal_package_id,
            surface.signal_session_count,
            surface.return_interval_count,
            surface.candidate_implementation_id,
            surface.primary_comparator_id,
            surface.initial_capital_usd,
            surface.price_field,
            surface.return_clock,
            surface.one_way_cost_bps,
            surface.primary_metric_id,
            surface.return_threshold_strictly_greater_than,
            surface.drawdown_guard_id,
            surface.drawdown_threshold_less_than_or_equal_to,
            surface.reducer_precedence,
        )
        if replay != expected:
            raise ValueError("approved preregistration surface replay drifted")
        policy_file = _bound_file(
            Path(admission.authority_binding.path),
            root=project_root,
            field="authority_binding",
        )
        if hashlib.sha256(policy_file.read_bytes()).hexdigest() != _POLICY_FILE_SHA256:
            raise ValueError("approved preregistration file SHA-256 mismatch")
    except FrozenSignalValuePreregistrationFreezeAdmissionError:
        raise
    except preregistration.FrozenSignalValuePreregistrationError as exc:
        raise FrozenSignalValuePreregistrationFreezeAdmissionError(
            "FROZEN_SIGNAL_VALUE_PREREGISTRATION_FREEZE_ADMISSION_REJECTED", str(exc)
        ) from exc
    except (OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise FrozenSignalValuePreregistrationFreezeAdmissionError(
            "FROZEN_SIGNAL_VALUE_PREREGISTRATION_FREEZE_ADMISSION_REJECTED", str(exc)
        ) from exc
    return FrozenSignalValuePreregistrationFreezeAdmissionLoadResult(
        admission=admission,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=admission.canonical_sha256,
        preregistration=loaded,
        terminal=_TERMINAL,
    )


__all__ = [
    "DEFAULT_FROZEN_SIGNAL_VALUE_CONFIRMATION_PREREGISTRATION_FREEZE_ADMISSION_PATH",
    "FrozenSignalValuePreregistrationFreezeAdmission",
    "FrozenSignalValuePreregistrationFreezeAdmissionError",
    "FrozenSignalValuePreregistrationFreezeAdmissionLoadResult",
    "load_frozen_signal_value_confirmation_preregistration_freeze_admission",
]
