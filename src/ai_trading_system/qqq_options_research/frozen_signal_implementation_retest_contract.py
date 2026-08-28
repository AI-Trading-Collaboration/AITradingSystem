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
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_FROZEN_SIGNAL_RETEST_CONTRACT_PATH = Path(
    "config/research/qc_qqq_options_frozen_signal_implementation_retest_contract_v1.yaml"
)

_TASK_ID = "TRADING-2542H_QQQ_OPTIONS_FROZEN_SIGNAL_IMPLEMENTATION_RETEST_CONTRACT_V1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_AUTHORITY_PATHS = (
    "config/research/qqq_options_signal_export_v1.yaml",
    "config/research/qqq_options_deterministic_selection_v1.yaml",
    "config/research/qqq_options_minute_execution_reality_v1.yaml",
    "config/research/qqq_options_cash_premium_settlement_accounting_v1.yaml",
    "config/research/qqq_options_lifecycle_expiry_corporate_action_safety_v1.yaml",
    "config/research/qqq_options_daily_primary_backtest_contract_v1.yaml",
    "config/research/qqq_options_owner_decision_manifest_v2.yaml",
    "inputs/research/qqq_options/"
    "trading_2541_exact_date_subscription_recovery_execution_v3/"
    "export_safe_terminal_evidence.json",
    "config/research/qc_qqq_options_growth_action_value_real_review_execution_v2.yaml",
    "config/research/"
    "qc_qqq_options_growth_action_value_veto_option_signal_architecture_freeze_v1.yaml",
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "historical_pit_source_candidate_evidence_review_v1.yaml",
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v4.yaml",
)
_AUTHORITY_ROLES = (
    "SIGNAL_PACKAGE_CONTRACT",
    "DETERMINISTIC_SELECTOR_MECHANICS",
    "EXECUTION_REALITY_MECHANICS",
    "CASH_ACCOUNTING_MECHANICS",
    "LIFECYCLE_MECHANICS",
    "DAILY_PRIMARY_CONTRACT",
    "OWNER_DECISION_SLOT_INVENTORY",
    "QC_CHAIN_COVERAGE_EVIDENCE",
    "LEGACY_2542E_HISTORY_ONLY",
    "LEGACY_2542F_HISTORY_ONLY",
    "OPTIONAL_2542G_OVERLAY_EVIDENCE_ONLY",
    "LEGACY_NON_OPTIONS_VALUE_SHEET_HISTORY_ONLY",
)
_LOCAL_RESPONSIBILITIES = (
    "FREEZE_EXISTING_SIGNAL_IDENTITY",
    "FREEZE_MAPPING_AND_POLICY",
    "BUILD_AND_REPLAY_RUN_MANIFEST",
    "ADMIT_EXPORT_SAFE_RESULT_EVIDENCE",
    "VALIDATE_PAIRED_COMPARATOR_IDENTITY_AND_AGGREGATES",
)
_QC_RESPONSIBILITIES = (
    "PROVIDE_QQQ_OPTION_CHAIN",
    "SELECT_CONTRACT_BY_REVIEWED_RESULT_BLIND_POLICY",
    "SIMULATE_EXECUTION_FILL_FEES_AND_SLIPPAGE",
    "PROCESS_POSITION_LIFECYCLE_AND_MARKS",
    "CALCULATE_CASH_EQUITY_AND_PNL",
)
_BASELINE_REQUIRED = (
    "FROZEN_STRATEGY_SIGNAL_IDENTITY",
    "REVIEWED_SIGNAL_TO_OPTION_ACTION_MAPPING",
    "QC_QQQ_OPTION_CHAIN_AND_FIELD_SOURCE_IDENTITY",
    "RESULT_BLIND_DETERMINISTIC_SELECTION_POLICY",
    "EXECUTION_FILL_FEE_AND_SLIPPAGE_POLICY",
    "LIFECYCLE_MARK_AND_ACCOUNTING_POLICY",
    "EXACT_RUN_DQ_PIT_AND_EXPORT_SAFE_RESULT_IDENTITY",
    "SAME_SIGNAL_UNDERLYING_AND_OPTIONIZED_COMPARATOR",
)
_OVERLAY_PROVIDERS = (
    "FMP_SPY_QQQ",
    "CBOE_VIX",
    "FED_SCHEDULE",
    "BLS_SCHEDULE",
    "BEA_SCHEDULE",
)
_TARGET_SIGNALS = ("LONG_CALL", "LONG_PUT", "FLAT")
_RESULT_FIELDS = (
    "RUN_PROJECT_BACKTEST_IDENTITY",
    "REPOSITORY_CODE_POLICY_MANIFEST_SHA",
    "LEAN_VERSION",
    "REQUESTED_AND_EVALUATED_RANGE",
    "SESSION_INVENTORY_AND_DQ_PIT",
    "SIGNAL_AND_MAPPING_IDENTITY",
    "SELECTION_AND_NO_CONTRACT_COUNTS",
    "INTENT_SUBMIT_FILL_REJECT_CANCEL_COUNTS",
    "LIFECYCLE_DISPOSITION_COUNTS",
    "FEE_SLIPPAGE_CASH_EQUITY_RETURN_DRAWDOWN",
    "TERMINAL_AND_COMPARATOR_IDENTITY",
)
_STOP_REASONS = (
    "MISSING_FROZEN_SIGNAL_IDENTITY",
    "UNREVIEWED_SIGNAL_MAPPING",
    "OPTION_ALPHA_LEAKAGE",
    "UNFROZEN_SELECTION_POLICY",
    "UNFROZEN_EXECUTION_POLICY",
    "UNFROZEN_ACCOUNTING_POLICY",
    "UNFROZEN_LIFECYCLE_POLICY",
    "FIELD_SOURCE_UNPROVEN",
    "MULTIPLIER_UNPROVEN",
    "MARK_POLICY_UNSPECIFIED",
    "RUN_IDENTITY_UNSPECIFIED",
    "MANIFEST_REPLAY_NOT_PASS",
    "INSUFFICIENT_PLATFORM_EVIDENCE",
    "IMMUTABLE_LINEAGE_MUTATION",
    "EXTERNAL_RUN_NOT_AUTHORIZED",
)
_TERMINAL_PRECEDENCE = ("INVALID", "FAIL", "INSUFFICIENT", "PASS")


class FrozenSignalImplementationRetestContractError(ValueError):
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


class ImmutableAuthorityBinding(_StrictModel):
    path: str
    file_sha256: str
    role: str
    immutable: Literal[True]

    @field_validator("file_sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("invalid lowercase SHA-256")
        return value


class ResearchScope(_StrictModel):
    ticker: Literal["QQQ"]
    calendar: Literal["XNYS"]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: Literal[1202]
    primary_role: Literal["PRIMARY"]
    legacy_2022_12_01_is_default: Literal[False]

    @model_validator(mode="after")
    def validate_primary_window(self) -> Self:
        expected = (date(2021, 2, 22), date(2025, 12, 2))
        if (self.requested_start, self.requested_end) != expected:
            raise ValueError("requested primary window drifted")
        if (self.evaluated_start, self.evaluated_end) != expected:
            raise ValueError("evaluated primary window drifted")
        return self


class QCChainCoverage(_StrictModel):
    technical_validation_state: Literal["PASS"]
    expected_session_count: Literal[1202]
    observed_session_count: Literal[1202]
    unresolved_session_count: Literal[0]
    normal_slice_session_count: Literal[1201]
    recovered_session_count: Literal[1]
    recovered_contract_count: Literal[6496]
    chain_presence_status: Literal["PASS_WITH_EXACT_DATE_PROVIDER_HISTORY_RECOVERY"]
    data_quality_status: Literal["PASS_FOR_RESEARCH_TRANSPORT_COMPLETENESS"]
    point_in_time_status: Literal["PASS_FOR_EXACT_SOURCE_AND_AVAILABILITY_DATE"]
    strategy_engine_status: Literal["NOT_IN_SCOPE_ZERO_ORDER_VALIDATION"]
    proves_strategy_return: Literal[False]


class ResponsibilitySplit(_StrictModel):
    local: tuple[str, ...]
    quantconnect: tuple[str, ...]
    local_option_repricing_allowed: Literal[False]
    quantconnect_direction_signal_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_responsibilities(self) -> Self:
        if self.local != _LOCAL_RESPONSIBILITIES:
            raise ValueError("local responsibility surface drifted")
        if self.quantconnect != _QC_RESPONSIBILITIES:
            raise ValueError("QuantConnect responsibility surface drifted")
        return self


class OptionalOverlay(_StrictModel):
    provider: str
    role: Literal["OPTIONAL_RESULT_BLIND_RISK_OVERLAY"]
    mandatory_for_baseline: Literal[False]
    direction_signal_allowed: Literal[False]
    missing_blocks_baseline: Literal[False]


class SignalMapping(_StrictModel):
    source_signal_artifact_status: Literal["MISSING_EXACT_RETAINED_PACKAGE"]
    source_signal_enum_status: Literal["UNBOUND"]
    target_option_signal_enum: tuple[str, ...]
    mapping_status: Literal["UNKNOWN_REQUIRES_OWNER_REVIEW"]
    mapping_rows: tuple[object, ...]
    defensive_or_sgov_mapping: Literal["UNKNOWN_REQUIRES_OWNER_REVIEW"]
    option_or_result_input_allowed: Literal[False]
    missing_source_signal_terminal: Literal["INVALID"]
    effective_session_rule: Literal["NEXT_VALID_XNYS_SESSION"]

    @model_validator(mode="after")
    def validate_unresolved_mapping(self) -> Self:
        if self.target_option_signal_enum != _TARGET_SIGNALS:
            raise ValueError("target option signal enum drifted")
        if self.mapping_rows:
            raise ValueError("mapping rows require a separate Owner exact-freeze successor")
        return self


class PolicyGates(_StrictModel):
    selection_status: Literal["OWNER_REVIEW_REQUIRED_BASELINE"]
    execution_status: Literal["OWNER_REVIEW_REQUIRED_BASELINE"]
    accounting_status: Literal["OWNER_REVIEW_REQUIRED_BASELINE"]
    lifecycle_status: Literal["OWNER_REVIEW_REQUIRED_BASELINE"]
    selection_frozen: Literal[False]
    execution_frozen: Literal[False]
    accounting_frozen: Literal[False]
    lifecycle_frozen: Literal[False]
    quantconnect_engine_defaults_allowed: Literal[False]


class PairedComparator(_StrictModel):
    same_frozen_signal_identity_required: Literal[True]
    required_implementations: tuple[str, ...]
    optional_overlay_is_separate_lane: Literal[True]
    result_blind_parameter_freeze_required: Literal[True]

    @model_validator(mode="after")
    def validate_implementations(self) -> Self:
        if self.required_implementations != (
            "UNDERLYING_IMPLEMENTATION",
            "OPTIONIZED_IMPLEMENTATION",
        ):
            raise ValueError("paired comparator implementation set drifted")
        return self


class ExportSafeResult(_StrictModel):
    required_fields: tuple[str, ...]
    raw_option_rows_allowed: Literal[False]
    complete_chain_export_allowed: Literal[False]
    contract_quote_history_export_allowed: Literal[False]
    local_substitute_pnl_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_required_fields(self) -> Self:
        if self.required_fields != _RESULT_FIELDS:
            raise ValueError("export-safe result field surface drifted")
        return self


class StopPolicy(_StrictModel):
    reason_codes: tuple[str, ...]
    terminal_precedence: tuple[str, ...]
    missing_unknown_or_not_evaluated_can_pass: Literal[False]
    cross_date_fallback_allowed: Literal[False]
    result_after_parameter_freeze_allowed: Literal[True]

    @model_validator(mode="after")
    def validate_stop_surface(self) -> Self:
        if self.reason_codes != _STOP_REASONS:
            raise ValueError("typed stop reason surface drifted")
        if self.terminal_precedence != _TERMINAL_PRECEDENCE:
            raise ValueError("terminal precedence drifted")
        return self


class Safety(_StrictModel):
    static_contract_authorized: Literal[True]
    signal_mapping_frozen: Literal[False]
    executable_policy_frozen: Literal[False]
    manifest_generation_authorized: Literal[False]
    manifest_replay_executed: Literal[False]
    real_dq_authorized: Literal[False]
    qc_backtest_authorized: Literal[False]
    qc_project_mutation_authorized: Literal[False]
    provider_query_authorized: Literal[False]
    raw_option_payload_download_or_export_allowed: Literal[False]
    orders: Literal[0]
    fills: Literal[0]
    positions: Literal[0]
    investment_interpretation_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class FrozenSignalImplementationRetestContract(_CanonicalModel):
    schema_version: Literal["qqq_options_frozen_signal_implementation_retest_contract.v1"]
    contract_id: Literal["qc_qqq_options_frozen_signal_implementation_retest_contract_v1"]
    contract_version: Literal["1.0.0-draft.1"]
    status: Literal["STATIC_CONTRACT_READY_OWNER_EXACT_POLICY_FREEZE_REQUIRED"]
    task_id: Literal["TRADING-2542H_QQQ_OPTIONS_FROZEN_SIGNAL_IMPLEMENTATION_RETEST_CONTRACT_V1"]
    owner_decision_id: Literal[
        "owner_decision:TRADING-2542H:2026-08-28:"
        "adopt_quantconnect_frozen_signal_implementation_retest_path_v1"
    ]
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    authority_bindings: tuple[ImmutableAuthorityBinding, ...]
    research_scope: ResearchScope
    qc_chain_coverage: QCChainCoverage
    responsibility_split: ResponsibilitySplit
    baseline_required: tuple[str, ...]
    optional_overlays: tuple[OptionalOverlay, ...]
    signal_mapping: SignalMapping
    policy_gates: PolicyGates
    paired_comparator: PairedComparator
    export_safe_result: ExportSafeResult
    stop_policy: StopPolicy
    safety: Safety
    terminal: Literal["OWNER_EXACT_POLICY_FREEZE_REQUIRED_NO_BACKTEST"]

    @model_validator(mode="after")
    def validate_exact_contract(self) -> Self:
        if self.task_id != _TASK_ID:
            raise ValueError("task identity drifted")
        if tuple(row.path for row in self.authority_bindings) != _AUTHORITY_PATHS:
            raise ValueError("authority path order drifted")
        if tuple(row.role for row in self.authority_bindings) != _AUTHORITY_ROLES:
            raise ValueError("authority role order drifted")
        if self.baseline_required != _BASELINE_REQUIRED:
            raise ValueError("baseline required surface drifted")
        if tuple(row.provider for row in self.optional_overlays) != _OVERLAY_PROVIDERS:
            raise ValueError("optional overlay provider surface drifted")
        return self


@dataclass(frozen=True)
class FrozenSignalImplementationRetestContractLoadResult:
    contract: FrozenSignalImplementationRetestContract
    path: Path
    file_sha256: str
    canonical_sha256: str
    terminal: Literal["OWNER_EXACT_POLICY_FREEZE_REQUIRED_NO_BACKTEST"]


def _read_yaml(path: Path) -> dict[str, object]:
    payload = load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a mapping")
    return payload


def _require_upstream_blockers(*, paths_by_role: dict[str, Path]) -> None:
    signal = _read_yaml(paths_by_role["SIGNAL_PACKAGE_CONTRACT"])
    if signal.get("etf_signal_mapping_status") != "UNKNOWN_REQUIRES_OWNER_REVIEW":
        raise ValueError("signal mapping predecessor status drifted")
    if signal.get("etf_signal_mapping_allowed") is not False:
        raise ValueError("signal mapping predecessor unexpectedly activated")

    for role, status_key, authorization_key in (
        ("DETERMINISTIC_SELECTOR_MECHANICS", "status", "selection_authorized"),
        ("EXECUTION_REALITY_MECHANICS", "status", "execution_authorized"),
        ("CASH_ACCOUNTING_MECHANICS", "status", "accounting_authorized"),
        ("LIFECYCLE_MECHANICS", "status", "lifecycle_authorized"),
    ):
        payload = _read_yaml(paths_by_role[role])
        if payload.get(status_key) != "OWNER_REVIEW_REQUIRED_BASELINE":
            raise ValueError(f"{role} predecessor status drifted")
        if payload.get(authorization_key) is not False:
            raise ValueError(f"{role} predecessor unexpectedly activated")

    daily = _read_yaml(paths_by_role["DAILY_PRIMARY_CONTRACT"])
    if daily.get("backtest_execution_authorized") is not False:
        raise ValueError("daily primary backtest unexpectedly activated")
    daily_safety = daily.get("safety")
    if not isinstance(daily_safety, dict) or daily_safety.get("cloud_run_authorized") is not False:
        raise ValueError("daily primary cloud boundary drifted")

    owner_slots = _read_yaml(paths_by_role["OWNER_DECISION_SLOT_INVENTORY"])
    slot_safety = owner_slots.get("safety")
    if not isinstance(slot_safety, dict):
        raise ValueError("owner slot safety missing")
    if slot_safety.get("executable_policy_authorized") is not False:
        raise ValueError("owner decision slot inventory unexpectedly executable")

    evidence = json.loads(paths_by_role["QC_CHAIN_COVERAGE_EVIDENCE"].read_text(encoding="utf-8"))
    expected = {
        "technical_validation_state": "PASS",
        "expected_session_count": 1202,
        "observed_session_count": 1202,
        "unresolved_session_count": 0,
        "normal_slice_session_count": 1201,
        "recovered_session_count": 1,
        "exact_date_contract_count": 6496,
        "chain_presence_status": "PASS_WITH_EXACT_DATE_PROVIDER_HISTORY_RECOVERY",
        "data_quality_status": "PASS_FOR_RESEARCH_TRANSPORT_COMPLETENESS",
        "point_in_time_status": "PASS_FOR_EXACT_SOURCE_AND_AVAILABILITY_DATE",
        "strategy_engine_status": "NOT_IN_SCOPE_ZERO_ORDER_VALIDATION",
        "orders": 0,
        "fills": 0,
        "portfolio_invested": False,
        "raw_option_rows_exported": False,
    }
    for key, value in expected.items():
        if evidence.get(key) != value:
            raise ValueError(f"QC chain coverage evidence drifted at {key}")


def load_frozen_signal_implementation_retest_contract(
    *,
    path: Path = DEFAULT_FROZEN_SIGNAL_RETEST_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> FrozenSignalImplementationRetestContractLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="retest_contract")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        contract = FrozenSignalImplementationRetestContract.model_validate(payload)
        paths_by_role: dict[str, Path] = {}
        for binding in contract.authority_bindings:
            bound = _bound_file(Path(binding.path), root=project_root, field=binding.role)
            observed_sha = hashlib.sha256(bound.read_bytes()).hexdigest()
            if observed_sha != binding.file_sha256:
                raise ValueError(f"{binding.role} file SHA-256 mismatch")
            paths_by_role[binding.role] = bound
        _require_upstream_blockers(paths_by_role=paths_by_role)
    except FrozenSignalImplementationRetestContractError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise FrozenSignalImplementationRetestContractError(
            "FROZEN_SIGNAL_IMPLEMENTATION_RETEST_CONTRACT_REJECTED", str(exc)
        ) from exc
    return FrozenSignalImplementationRetestContractLoadResult(
        contract=contract,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=contract.canonical_sha256,
        terminal="OWNER_EXACT_POLICY_FREEZE_REQUIRED_NO_BACKTEST",
    )


__all__ = [
    "DEFAULT_FROZEN_SIGNAL_RETEST_CONTRACT_PATH",
    "FrozenSignalImplementationRetestContract",
    "FrozenSignalImplementationRetestContractError",
    "FrozenSignalImplementationRetestContractLoadResult",
    "load_frozen_signal_implementation_retest_contract",
]
