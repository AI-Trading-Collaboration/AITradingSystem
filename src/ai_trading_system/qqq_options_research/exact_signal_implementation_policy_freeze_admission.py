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
    exact_signal_implementation_policy_draft as draft_policy,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_EXACT_SIGNAL_IMPLEMENTATION_POLICY_FREEZE_ADMISSION_PATH = Path(
    "config/research/qc_qqq_options_exact_signal_implementation_policy_freeze_admission_v1.yaml"
)

_TASK_ID = "TRADING-2542I_QQQ_OPTIONS_EXACT_SIGNAL_AND_IMPLEMENTATION_POLICY_DRAFT_V1"
_DRAFT_PATH = "config/research/qc_qqq_options_exact_signal_implementation_policy_draft_v1.yaml"
_DRAFT_FILE_SHA256 = "22335aa324ffb13c9917b65ad57f51916831ecd95c05fe357f7faa13f74b57d0"
_DRAFT_CANONICAL_SHA256 = "45c247010f47ad3172215f90aa7c9cd40044b5332284e1789095d230075a5d83"
_TERMINAL: Literal["OWNER_EXACT_POLICY_FROZEN_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"] = (
    "OWNER_EXACT_POLICY_FROZEN_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FROZEN_SECTIONS = (
    "research_window",
    "source_candidate",
    "signal_mapping",
    "selection_proposal",
    "execution_proposal",
    "accounting_proposal",
    "lifecycle_proposal",
    "result_admission",
    "slot_proposals",
    "safety",
)
_MAPPING_ROWS = (
    ("risk_on", "LONG_CALL"),
    ("constructive", "LONG_CALL"),
    ("neutral", "FLAT"),
    ("defensive", "FLAT"),
    ("risk_off", "FLAT"),
)
_BLOCKERS = (
    "SOURCE_PRIMARY_START_NOT_COVERED",
    "EXACT_1202_SESSION_PACKAGE_MISSING",
    "CODE_CONFIG_INPUT_DQ_PIT_IDENTITY_MISSING",
    "REAL_DQ_NOT_AUTHORIZED",
)


class ExactSignalImplementationPolicyFreezeAdmissionError(ValueError):
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
        "owner_decision:TRADING-2542I:2026-08-29:"
        "freeze_exact_signal_implementation_policy_and_continue_"
        "non_executable_signal_preparation_v1"
    ]
    approved_instruction: Literal["好的，那就冻结吧，你继续推进"]
    exact_draft_freeze_granted: Literal[True]
    five_state_mapping_frozen: Literal[True]
    all_37_successor_slots_frozen: Literal[True]
    whole_draft_surface_frozen: Literal[True]
    predecessor_bytes_must_remain_immutable: Literal[True]
    signal_package_preparation_authorized: Literal[True]
    real_dq_or_backtest_authorized: Literal[False]
    authorization_state: Literal["EXACT_PREAUTHORIZED"]


class AuthorityBinding(_StrictModel):
    path: str
    artifact_id: Literal["qc_qqq_options_exact_signal_implementation_policy_draft_v1"]
    artifact_version: Literal["1.0.0-draft.1"]
    file_sha256: str
    canonical_sha256: str
    role: Literal["OWNER_EXACT_FROZEN_WHOLE_DRAFT_AUTHORITY"]
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("identity must be a lowercase SHA-256")
        return value


class SignalMappingRow(_StrictModel):
    source_state: Literal["risk_on", "constructive", "neutral", "defensive", "risk_off"]
    option_action: Literal["LONG_CALL", "FLAT"]


class FrozenSurface(_StrictModel):
    whole_draft_frozen: Literal[True]
    draft_status_preserved: Literal["OWNER_REVIEW_DRAFT_NON_EXECUTABLE"]
    frozen_sections: tuple[str, ...]
    baseline_actions: tuple[Literal["LONG_CALL", "FLAT"], ...]
    long_put_baseline_allowed: Literal[False]
    long_put_role: Literal["SEPARATE_SENSITIVITY_ONLY"]
    signal_mapping_rows: tuple[SignalMappingRow, ...]
    frozen_slot_count: int
    frozen_slot_ids: tuple[str, ...]
    all_slots_owner_frozen: Literal[True]

    @model_validator(mode="after")
    def validate_frozen_surface(self) -> Self:
        if self.frozen_sections != _FROZEN_SECTIONS:
            raise ValueError("whole-draft frozen section inventory drifted")
        if self.baseline_actions != ("LONG_CALL", "FLAT"):
            raise ValueError("baseline action set drifted")
        observed_mapping = tuple(
            (row.source_state, row.option_action) for row in self.signal_mapping_rows
        )
        if observed_mapping != _MAPPING_ROWS:
            raise ValueError("five-state frozen mapping drifted")
        if self.frozen_slot_count != 37 or len(self.frozen_slot_ids) != 37:
            raise ValueError("all 37 successor slots must be frozen exactly once")
        if len(set(self.frozen_slot_ids)) != 37:
            raise ValueError("frozen slot inventory contains duplicates")
        return self


class SignalPackageState(_StrictModel):
    producer_id: Literal["first_layer_composer_v2"]
    source_locator: Literal[
        "outputs/research_trends/models/first_layer_composer_v2_predictions.csv:trend_state"
    ]
    requested_start: date
    requested_end: date
    calendar: Literal["XNYS"]
    expected_session_count: int
    documented_source_start: date
    documented_source_end: date
    exact_1202_session_package_present: Literal[False]
    exact_1202_session_package_admitted: Literal[False]
    observed_package_sha256: None
    manifest_replay_executed: Literal[False]
    manifest_replay_status: Literal["NOT_RUN_MISSING_EXACT_SIGNAL_PACKAGE"]
    missing_rows_may_be_filled: Literal[False]
    poc_rewrap_allowed: Literal[False]
    manual_csv_allowed: Literal[False]
    cross_session_fill_allowed: Literal[False]
    next_legal_action: Literal[
        "BUILD_AND_REVIEW_FAIL_CLOSED_EXACT_SIGNAL_GENERATION_MANIFEST_AFTER_REAL_DQ_AUTHORIZATION"
    ]
    blockers: tuple[str, ...]

    @model_validator(mode="after")
    def validate_readiness_facts(self) -> Self:
        observed = (
            self.requested_start,
            self.requested_end,
            self.expected_session_count,
            self.documented_source_start,
            self.documented_source_end,
        )
        expected = (
            date(2021, 2, 22),
            date(2025, 12, 2),
            1202,
            date(2023, 2, 22),
            date(2026, 3, 27),
        )
        if observed != expected:
            raise ValueError("exact signal readiness facts drifted")
        if self.blockers != _BLOCKERS:
            raise ValueError("exact signal blocker inventory drifted")
        return self


class FreezeSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    owner_exact_freeze_admission_only: Literal[True]
    signal_package_preparation_authorized: Literal[True]
    signal_package_generation_authorized: Literal[False]
    exact_signal_package_admission_allowed: Literal[False]
    executable_policy_authorized: Literal[False]
    r1_manifest_generation_authorized: Literal[False]
    real_dq_authorized: Literal[False]
    qc_backtest_authorized: Literal[False]
    qc_project_mutation_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    raw_option_payload_download_or_export_allowed: Literal[False]
    parameter_or_threshold_search_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class ExactSignalImplementationPolicyFreezeAdmission(_CanonicalModel):
    schema_version: Literal["qqq_options_exact_signal_implementation_policy_freeze_admission.v1"]
    admission_id: Literal["qc_qqq_options_exact_signal_implementation_policy_freeze_admission_v1"]
    admission_version: Literal["1.0.0"]
    status: Literal["OWNER_EXACT_POLICY_FROZEN_SIGNAL_PACKAGE_UNADMITTED"]
    task_id: str
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    owner_decision: OwnerDecision
    authority_binding: AuthorityBinding
    frozen_surface: FrozenSurface
    signal_package_state: SignalPackageState
    safety: FreezeSafety
    terminal: Literal["OWNER_EXACT_POLICY_FROZEN_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"]

    @model_validator(mode="after")
    def validate_exact_admission(self) -> Self:
        if self.task_id != _TASK_ID:
            raise ValueError("task identity drifted")
        binding = self.authority_binding
        if (
            binding.path,
            binding.file_sha256,
            binding.canonical_sha256,
        ) != (_DRAFT_PATH, _DRAFT_FILE_SHA256, _DRAFT_CANONICAL_SHA256):
            raise ValueError("approved draft identity drifted")
        return self


@dataclass(frozen=True)
class ExactSignalImplementationPolicyFreezeAdmissionLoadResult:
    policy: ExactSignalImplementationPolicyFreezeAdmission
    path: Path
    file_sha256: str
    canonical_sha256: str
    draft: draft_policy.ExactSignalImplementationPolicyDraftLoadResult
    terminal: Literal["OWNER_EXACT_POLICY_FROZEN_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"]


def load_exact_signal_implementation_policy_freeze_admission(
    *,
    path: Path = DEFAULT_EXACT_SIGNAL_IMPLEMENTATION_POLICY_FREEZE_ADMISSION_PATH,
    project_root: Path = PROJECT_ROOT,
) -> ExactSignalImplementationPolicyFreezeAdmissionLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="freeze_admission")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        policy = ExactSignalImplementationPolicyFreezeAdmission.model_validate(payload)
        loaded_draft = draft_policy.load_exact_signal_implementation_policy_draft(
            path=Path(policy.authority_binding.path), project_root=project_root
        )
        if (loaded_draft.file_sha256, loaded_draft.canonical_sha256) != (
            policy.authority_binding.file_sha256,
            policy.authority_binding.canonical_sha256,
        ):
            raise ValueError("approved draft loader identity drifted")
        draft_slots = tuple(row.slot_id for row in loaded_draft.draft.slot_proposals)
        if policy.frozen_surface.frozen_slot_ids != draft_slots:
            raise ValueError("freeze admission does not replay all 37 draft slots")
        draft_mapping = tuple(
            (row.source_state, row.option_action) for row in loaded_draft.draft.signal_mapping.rows
        )
        frozen_mapping = tuple(
            (row.source_state, row.option_action)
            for row in policy.frozen_surface.signal_mapping_rows
        )
        if frozen_mapping != draft_mapping:
            raise ValueError("freeze admission does not replay the draft mapping")
        if any(row.owner_frozen for row in loaded_draft.draft.slot_proposals):
            raise ValueError("approved draft bytes were mutated instead of admitted separately")
        if loaded_draft.draft.safety.owner_exact_freeze is not False:
            raise ValueError("approved draft safety bytes were mutated")
        draft_file = _bound_file(
            Path(policy.authority_binding.path),
            root=project_root,
            field="authority_binding",
        )
        if hashlib.sha256(draft_file.read_bytes()).hexdigest() != (
            policy.authority_binding.file_sha256
        ):
            raise ValueError("approved draft file SHA-256 mismatch")
    except ExactSignalImplementationPolicyFreezeAdmissionError:
        raise
    except draft_policy.ExactSignalImplementationPolicyDraftError as exc:
        raise ExactSignalImplementationPolicyFreezeAdmissionError(
            "EXACT_SIGNAL_IMPLEMENTATION_POLICY_FREEZE_ADMISSION_REJECTED",
            str(exc),
        ) from exc
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise ExactSignalImplementationPolicyFreezeAdmissionError(
            "EXACT_SIGNAL_IMPLEMENTATION_POLICY_FREEZE_ADMISSION_REJECTED",
            str(exc),
        ) from exc
    return ExactSignalImplementationPolicyFreezeAdmissionLoadResult(
        policy=policy,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=policy.canonical_sha256,
        draft=loaded_draft,
        terminal=_TERMINAL,
    )


__all__ = [
    "DEFAULT_EXACT_SIGNAL_IMPLEMENTATION_POLICY_FREEZE_ADMISSION_PATH",
    "ExactSignalImplementationPolicyFreezeAdmission",
    "ExactSignalImplementationPolicyFreezeAdmissionError",
    "ExactSignalImplementationPolicyFreezeAdmissionLoadResult",
    "load_exact_signal_implementation_policy_freeze_admission",
]
