from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import paired_comparison_contract
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_PAIRED_COMPARISON_CONTRACT_FREEZE_ADMISSION_PATH = Path(
    "config/research/qc_qqq_options_paired_comparison_contract_freeze_admission_v1.yaml"
)

_TASK_ID = "TRADING-2548_QQQ_OPTIONS_PAIRED_COMPARATOR_ESTIMAND_AND_EXPORT_CONTRACT_V1"
_CONTRACT_PATH = "config/research/qc_qqq_options_paired_comparison_contract_v1.yaml"
_CONTRACT_FILE_SHA256 = "8c748634f6869eb4d4e9dfb14493acd072d146074ce7e86462eec0adae15714a"
_CONTRACT_CANONICAL_SHA256 = (
    "6f77cf17af6e435799a2e86e1fb6a81936368e053b2367efb3a8e2be13412267"
)
_APPROVED_INSTRUCTION = (
    "批准 `qc_qqq_options_paired_comparison_contract_v1@1.0.0-draft.1`（file SHA "
    "`8c748634…` / canonical SHA `6f77cf17…`）全部所列 comparator/estimand/export/"
    "calendar/falsification/safety 规则按草案精确冻结；仅限 non-executable DATA_RESEARCH；"
    "不授权 exporter、manifest、DQ、QuantConnect save/build/backtest/retry、raw option export、"
    "provider/Object Store/public share、paper/live/production/broker。"
)
_TERMINAL: Literal[
    "OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FROZEN_NO_SUCCESSOR_AUTHORITY"
] = "OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FROZEN_NO_SUCCESSOR_AUTHORITY"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_FROZEN_SECTIONS = (
    "authority_bindings",
    "frozen_inheritance",
    "existing_result",
    "primary_comparator",
    "primary_estimand",
    "secondary_view",
    "diagnostics",
    "export_safe_fields",
    "calendar_diagnostics",
    "falsification",
    "interpretation",
    "safety",
)
_NAMED_DIAGNOSTICS = ("SGOV_CARRY_COMPARATOR", "QQQ_BUY_AND_HOLD")
_CALENDAR_PARTITIONS = (
    "PRIMARY_WINDOW_CALENDAR_2021",
    "CALENDAR_2022",
    "CALENDAR_2023",
    "CALENDAR_2024",
    "PRIMARY_WINDOW_CALENDAR_2025",
)
_FALSIFICATION_AXES = (
    "FROZEN_SIGNAL_IDENTITY",
    "SESSION_COVERAGE",
    "DQ_PIT_MANIFEST",
    "FROZEN_37_SLOT_POLICY",
    "OPTION_ALPHA_ISOLATION",
    "COMPARATOR_CONTRACT",
    "CAPITAL_NORMALIZATION",
    "EVENT_ALIGNMENT",
    "ACCOUNTING",
    "RISK_FIELDS",
    "EXPORT_SAFETY",
    "PLATFORM_IDENTITY",
    "CALENDAR_SUBPERIOD_COMPLETENESS",
    "MULTIPLICITY",
    "PRIMARY_IMPLEMENTATION_ESTIMAND",
    "EXTERNAL_AUTHORIZATION",
)


class PairedComparisonContractFreezeAdmissionError(ValueError):
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
        "owner_decision:TRADING-2548:2026-08-30:freeze_paired_comparison_contract_v1"
    ]
    approved_instruction: str
    exact_contract_freeze_granted: Literal[True]
    full_contract_surface_frozen: Literal[True]
    predecessor_bytes_must_remain_immutable: Literal[True]
    approved_file_sha_prefix: Literal["8c748634"]
    approved_canonical_sha_prefix: Literal["6f77cf17"]
    successor_implementation_authorized: Literal[False]
    authorization_state: Literal["EXACT_PREAUTHORIZED"]

    @model_validator(mode="after")
    def validate_instruction(self) -> Self:
        if self.approved_instruction != _APPROVED_INSTRUCTION:
            raise ValueError("owner approved instruction drifted")
        return self


class AuthorityBinding(_StrictModel):
    path: str
    artifact_id: Literal["qc_qqq_options_paired_comparison_contract_v1"]
    artifact_version: Literal["1.0.0-draft.1"]
    file_sha256: str
    canonical_sha256: str
    role: Literal["OWNER_EXACT_FROZEN_PAIRED_COMPARISON_CONTRACT"]
    immutable: Literal[True]

    @field_validator("file_sha256", "canonical_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("identity must be a lowercase SHA-256")
        return value


class FrozenSurface(_StrictModel):
    whole_contract_frozen: Literal[True]
    contract_status_preserved: Literal["STATIC_CONTRACT_READY_OWNER_EXACT_FREEZE_REQUIRED"]
    frozen_sections: tuple[str, ...]
    primary_comparator_method: Literal["SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT"]
    primary_estimand_view: Literal["COMMON_CAPITAL_ACCOUNT_VIEW"]
    secondary_view: Literal["CAPITAL_AT_RISK_TIME_VIEW"]
    named_diagnostic_ids: tuple[str, ...]
    calendar_partition_ids: tuple[str, ...]
    falsification_axis_ids: tuple[str, ...]
    existing_result_disposition_preserved: Literal["INSUFFICIENT_PLATFORM_EVIDENCE"]
    draft_owner_exact_frozen_flag_preserved: Literal[False]
    owner_exact_frozen_via_separate_admission: Literal[True]

    @model_validator(mode="after")
    def validate_exact_surface(self) -> Self:
        if self.frozen_sections != _FROZEN_SECTIONS:
            raise ValueError("whole-contract frozen section inventory drifted")
        if self.named_diagnostic_ids != _NAMED_DIAGNOSTICS:
            raise ValueError("named diagnostic freeze inventory drifted")
        if self.calendar_partition_ids != _CALENDAR_PARTITIONS:
            raise ValueError("calendar partition freeze inventory drifted")
        if self.falsification_axis_ids != _FALSIFICATION_AXES:
            raise ValueError("falsification axis freeze inventory drifted")
        return self


class SuccessorState(_StrictModel):
    paired_comparator_outcome: Literal["INSUFFICIENT_PLATFORM_EVIDENCE"]
    empirical_comparison_completed: Literal[False]
    current_contract_task_complete_after_admission: Literal[True]
    successor_task_implicitly_created: Literal[False]
    next_legal_action: Literal[
        "OWNER_SEPARATE_SUCCESSOR_SCOPE_REQUIRED_NO_AUTOMATIC_FOLLOW_ON"
    ]


class FreezeSafety(_StrictModel):
    non_executable_data_research_only: Literal[True]
    owner_exact_freeze_admission_only: Literal[True]
    comparator_contract_mutation_allowed: Literal[False]
    qc_exporter_implementation_authorized: Literal[False]
    local_result_admission_implementation_authorized: Literal[False]
    run_manifest_generation_authorized: Literal[False]
    real_dq_authorized: Literal[False]
    quantconnect_save_authorized: Literal[False]
    quantconnect_build_authorized: Literal[False]
    quantconnect_backtest_authorized: Literal[False]
    quantconnect_retry_authorized: Literal[False]
    provider_query_or_purchase_authorized: Literal[False]
    raw_option_payload_download_or_export_allowed: Literal[False]
    object_store_write_allowed: Literal[False]
    public_share_allowed: Literal[False]
    parameter_or_threshold_search_allowed: Literal[False]
    orders_outside_qc_simulation: Literal[0]
    fills_outside_qc_simulation: Literal[0]
    positions_outside_qc_simulation: Literal[0]
    investment_conclusion_generated: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    production_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class PairedComparisonContractFreezeAdmission(_CanonicalModel):
    schema_version: Literal["qqq_options_paired_comparison_contract_freeze_admission.v1"]
    admission_id: Literal["qc_qqq_options_paired_comparison_contract_freeze_admission_v1"]
    admission_version: Literal["1.0.0"]
    status: Literal["OWNER_EXACT_PAIRED_COMPARATOR_CONTRACT_FROZEN_NO_SUCCESSOR_AUTHORITY"]
    task_id: str
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    owner_decision: OwnerDecision
    authority_binding: AuthorityBinding
    frozen_surface: FrozenSurface
    successor_state: SuccessorState
    safety: FreezeSafety
    terminal: Literal[
        "OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FROZEN_NO_SUCCESSOR_AUTHORITY"
    ]

    @model_validator(mode="after")
    def validate_exact_admission(self) -> Self:
        if self.task_id != _TASK_ID:
            raise ValueError("task identity drifted")
        binding = self.authority_binding
        if (
            binding.path,
            binding.file_sha256,
            binding.canonical_sha256,
        ) != (_CONTRACT_PATH, _CONTRACT_FILE_SHA256, _CONTRACT_CANONICAL_SHA256):
            raise ValueError("approved comparator contract identity drifted")
        return self


@dataclass(frozen=True)
class PairedComparisonContractFreezeAdmissionLoadResult:
    admission: PairedComparisonContractFreezeAdmission
    path: Path
    file_sha256: str
    canonical_sha256: str
    contract: paired_comparison_contract.PairedComparisonContractLoadResult
    terminal: Literal[
        "OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FROZEN_NO_SUCCESSOR_AUTHORITY"
    ]


def load_paired_comparison_contract_freeze_admission(
    *,
    path: Path = DEFAULT_PAIRED_COMPARISON_CONTRACT_FREEZE_ADMISSION_PATH,
    project_root: Path = PROJECT_ROOT,
) -> PairedComparisonContractFreezeAdmissionLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="freeze_admission")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        admission = PairedComparisonContractFreezeAdmission.model_validate(payload)
        loaded_contract = paired_comparison_contract.load_paired_comparison_contract(
            path=Path(admission.authority_binding.path), project_root=project_root
        )
        if (loaded_contract.file_sha256, loaded_contract.canonical_sha256) != (
            admission.authority_binding.file_sha256,
            admission.authority_binding.canonical_sha256,
        ):
            raise ValueError("approved comparator contract loader identity drifted")
        contract = loaded_contract.contract
        surface = admission.frozen_surface
        if contract.status != surface.contract_status_preserved:
            raise ValueError("approved comparator contract status was rewritten")
        if contract.safety.owner_exact_frozen is not (
            surface.draft_owner_exact_frozen_flag_preserved
        ):
            raise ValueError("approved comparator contract safety bytes were rewritten")
        if contract.primary_comparator.method != surface.primary_comparator_method:
            raise ValueError("primary comparator freeze replay drifted")
        if contract.primary_estimand.view_id != surface.primary_estimand_view:
            raise ValueError("primary estimand freeze replay drifted")
        if contract.secondary_view.view_id != surface.secondary_view:
            raise ValueError("secondary view freeze replay drifted")
        if tuple(row.diagnostic_id for row in contract.diagnostics.named) != (
            surface.named_diagnostic_ids
        ):
            raise ValueError("named diagnostic freeze replay drifted")
        if tuple(row.partition_id for row in contract.calendar_diagnostics.partitions) != (
            surface.calendar_partition_ids
        ):
            raise ValueError("calendar partition freeze replay drifted")
        if tuple(row.axis_id for row in contract.falsification.axes) != (
            surface.falsification_axis_ids
        ):
            raise ValueError("falsification axis freeze replay drifted")
        if contract.existing_result.paired_comparator_outcome != (
            surface.existing_result_disposition_preserved
        ):
            raise ValueError("existing result disposition freeze replay drifted")
        contract_file = _bound_file(
            Path(admission.authority_binding.path),
            root=project_root,
            field="authority_binding",
        )
        if hashlib.sha256(contract_file.read_bytes()).hexdigest() != (
            admission.authority_binding.file_sha256
        ):
            raise ValueError("approved comparator contract file SHA-256 mismatch")
    except PairedComparisonContractFreezeAdmissionError:
        raise
    except paired_comparison_contract.PairedComparisonContractError as exc:
        raise PairedComparisonContractFreezeAdmissionError(
            "PAIRED_COMPARISON_CONTRACT_FREEZE_ADMISSION_REJECTED", str(exc)
        ) from exc
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise PairedComparisonContractFreezeAdmissionError(
            "PAIRED_COMPARISON_CONTRACT_FREEZE_ADMISSION_REJECTED", str(exc)
        ) from exc
    return PairedComparisonContractFreezeAdmissionLoadResult(
        admission=admission,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=admission.canonical_sha256,
        contract=loaded_contract,
        terminal=_TERMINAL,
    )


__all__ = [
    "DEFAULT_PAIRED_COMPARISON_CONTRACT_FREEZE_ADMISSION_PATH",
    "PairedComparisonContractFreezeAdmission",
    "PairedComparisonContractFreezeAdmissionError",
    "PairedComparisonContractFreezeAdmissionLoadResult",
    "load_paired_comparison_contract_freeze_admission",
]
