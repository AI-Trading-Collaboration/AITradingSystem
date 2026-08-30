from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_PAIRED_COMPARISON_CONTRACT_PATH = Path(
    "config/research/qc_qqq_options_paired_comparison_contract_v1.yaml"
)

_TASK_ID = "TRADING-2548_QQQ_OPTIONS_PAIRED_COMPARATOR_ESTIMAND_AND_EXPORT_CONTRACT_V1"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_AUTHORITY_PATHS = (
    "config/research/qc_qqq_options_exact_signal_implementation_policy_draft_v1.yaml",
    "config/research/qc_qqq_options_exact_signal_implementation_policy_freeze_admission_v1.yaml",
    "config/research/qc_qqq_options_exact_signal_implementation_backtest_execution_v1.yaml",
    "registry/development_tasks/2f/"
    "2f96dc5335fe6ba122c905841f6bcc0d25c252cb25828c9c06f0b65990486c7f.yaml",
)
_AUTHORITY_ROLES = (
    "FROZEN_SIGNAL_IMPLEMENTATION_POLICY",
    "OWNER_FREEZE_ADMISSION",
    "SINGLE_RUN_EXECUTION_POLICY",
    "TERMINAL_RESULT_ADMISSION_EVENT",
)
_SOURCE_SIGNAL_STATES = ("risk_on", "constructive", "neutral", "defensive", "risk_off")
_OPTION_ACTIONS = ("LONG_CALL", "LONG_CALL", "FLAT", "FLAT", "FLAT")
_IDENTITY_FIELDS = (
    "RUN_ID",
    "PROJECT_ID",
    "BACKTEST_ID",
    "REPOSITORY_EXACT_COMMIT",
    "QC_CODE_FILE_SHA256",
    "POLICY_FILE_SHA256",
    "POLICY_CANONICAL_SHA256",
    "FREEZE_ADMISSION_FILE_SHA256",
    "COMPARATOR_CONTRACT_FILE_SHA256",
    "COMPARATOR_CONTRACT_CANONICAL_SHA256",
    "SIGNAL_PACKAGE_RECEIPT_SHA256",
    "SIGNAL_INDEX_SHA256",
    "NORMALIZED_SIGNAL_SOURCE_SHA256",
    "RUN_MANIFEST_SHA256",
    "LEAN_VERSION",
    "PLATFORM_VERSION",
    "CLOUD_BUILD_IDENTITIES",
    "REQUESTED_DATE_RANGE",
    "EVALUATED_DATE_RANGE",
    "CALENDAR_ID",
    "SESSION_COUNT",
)
_DQ_SIGNAL_FIELDS = (
    "CANONICAL_DQ_RECEIPT_IDENTITY",
    "DATA_QUALITY_STATUS",
    "POINT_IN_TIME_STATUS",
    "MANIFEST_REPLAY_STATUS",
    "EXPECTED_SIGNAL_SESSION_COUNT",
    "OBSERVED_SIGNAL_SESSION_COUNT",
    "MISSING_SIGNAL_SESSION_COUNT",
    "DUPLICATE_SIGNAL_SESSION_COUNT",
    "UNKNOWN_SIGNAL_COUNT",
    "EXPECTED_TRANSITION_COUNT",
    "OBSERVED_TRANSITION_COUNT",
    "SIGNAL_MAPPING_IDENTITY",
)
_EVENT_FIELDS = (
    "SELECTION_ATTEMPT_COUNT",
    "ELIGIBLE_SELECTION_COUNT",
    "NO_ELIGIBLE_CONTRACT_COUNT",
    "ENTRY_INTENT_COUNT",
    "ENTRY_SUBMIT_COUNT",
    "ENTRY_FILL_COUNT",
    "ENTRY_REJECT_COUNT",
    "ENTRY_TIMEOUT_COUNT",
    "ENTRY_CANCEL_COUNT",
    "EXIT_FILL_COUNT",
    "EXIT_FLAT_COUNT",
    "EXIT_PRE_EXPIRY_GUARD_COUNT",
    "EXIT_TERMINAL_COUNT",
    "FRESH_REENTRY_COUNT",
    "INVALID_LIFECYCLE_COUNT",
    "MISSING_TERMINAL_MARK_COUNT",
    "EVENT_RECONCILIATION_STATUS",
)
_ACCOUNT_FIELDS = (
    "OPTIONIZED_START_EQUITY_USD",
    "OPTIONIZED_END_EQUITY_USD",
    "OPTIONIZED_NET_PNL_USD",
    "OPTIONIZED_NET_RETURN",
    "OPTIONIZED_FEES_USD",
    "OPTIONIZED_SPREAD_SLIPPAGE_COST_USD",
    "OPTIONIZED_MIN_CASH_USD",
    "OPTIONIZED_ENDING_CASH_USD",
    "OPTIONIZED_PEAK_EQUITY_USD",
    "OPTIONIZED_MAX_DRAWDOWN",
    "OPTIONIZED_TIME_IN_MARKET",
    "UNDERLYING_START_EQUITY_USD",
    "UNDERLYING_END_EQUITY_USD",
    "UNDERLYING_NET_PNL_USD",
    "UNDERLYING_NET_RETURN",
    "UNDERLYING_FEES_USD",
    "UNDERLYING_SPREAD_SLIPPAGE_COST_USD",
    "UNDERLYING_MIN_CASH_USD",
    "UNDERLYING_ENDING_CASH_USD",
    "UNDERLYING_PEAK_EQUITY_USD",
    "UNDERLYING_MAX_DRAWDOWN",
    "UNDERLYING_TIME_IN_MARKET",
)
_RISK_FIELDS = (
    "GROSS_PREMIUM_DEBIT_USD",
    "MAX_ENTRY_PREMIUM_AT_RISK_USD",
    "AVERAGE_PREMIUM_UTILIZATION",
    "MAX_PREMIUM_UTILIZATION",
    "PREMIUM_AT_RISK_HOLDING_TIME",
    "QQQ_DEPLOYED_CAPITAL_HOLDING_TIME",
    "ENTRY_DELTA_NOTIONAL_USD",
    "AVERAGE_DELTA_NOTIONAL_USD",
    "MAX_DELTA_NOTIONAL_USD",
    "DELTA_OBSERVATION_COUNT",
    "DELTA_MISSING_COUNT",
    "TIME_IN_MARKET_SESSIONS",
    "TIME_IN_MARKET_MINUTES",
)
_COMPARATOR_FIELDS = (
    "COMPARATOR_ID",
    "COMPARATOR_VERSION",
    "COMPARATOR_CONTRACT_SHA256",
    "SIGNAL_IDENTITY_MATCH",
    "LONG_EPISODE_COUNT",
    "FLAT_EPISODE_COUNT",
    "EFFECTIVE_EVENT_ALIGNMENT_COUNT",
    "EFFECTIVE_EVENT_MISMATCH_COUNT",
    "ENTRY_QUOTE_AVAILABLE_COUNT",
    "ENTRY_QUOTE_MISSING_COUNT",
    "EXIT_QUOTE_AVAILABLE_COUNT",
    "EXIT_QUOTE_MISSING_COUNT",
    "PRIMARY_RETURN_DELTA",
    "PRIMARY_DRAWDOWN_DELTA",
    "SECONDARY_CAPITAL_AT_RISK_TIME_RESULT",
    "PREREGISTERED_NAMED_DIAGNOSTIC_RESULTS",
)
_PARTITIONS = (
    ("PRIMARY_WINDOW_CALENDAR_2021", date(2021, 2, 22), date(2021, 12, 31)),
    ("CALENDAR_2022", date(2022, 1, 1), date(2022, 12, 31)),
    ("CALENDAR_2023", date(2023, 1, 1), date(2023, 12, 31)),
    ("CALENDAR_2024", date(2024, 1, 1), date(2024, 12, 31)),
    ("PRIMARY_WINDOW_CALENDAR_2025", date(2025, 1, 1), date(2025, 12, 2)),
)
_AXIS_MATRIX = (
    (
        "FROZEN_SIGNAL_IDENTITY",
        "ALL_SIGNAL_PACKAGE_AND_SOURCE_IDENTITIES_EXACT",
        "VALIDATED_SIGNAL_SEMANTICS_DIFFER_FROM_FROZEN_MAPPING",
        "REQUIRED_SIGNAL_IDENTITY_EVIDENCE_MISSING",
        "SIGNAL_OR_MAPPING_HASH_DRIFT_OR_OPTION_ALPHA_INPUT",
    ),
    (
        "SESSION_COVERAGE",
        "EXPECTED_OBSERVED_UNIQUE_SESSIONS_EQUAL_1202",
        "VALIDATED_SESSION_GAP_DUPLICATE_OR_UNKNOWN_EXISTS",
        "SESSION_INVENTORY_NOT_EXPORTED",
        "WINDOW_OR_CALENDAR_CHANGED_AFTER_FREEZE",
    ),
    (
        "DQ_PIT_MANIFEST",
        "DQ_PIT_AND_MANIFEST_REPLAY_ALL_PASS",
        "VALIDATED_DQ_PIT_OR_REPLAY_FAILURE",
        "DQ_PIT_OR_REPLAY_EVIDENCE_MISSING",
        "DQ_RECEIPT_OR_MANIFEST_IDENTITY_DRIFT",
    ),
    (
        "FROZEN_37_SLOT_POLICY",
        "ALL_37_FROZEN_SLOTS_EXACT",
        "PLATFORM_CANNOT_IMPLEMENT_A_FROZEN_SLOT",
        "SLOT_REPLAY_EVIDENCE_MISSING",
        "ANY_SLOT_CHANGED_OR_ENGINE_DEFAULT_SUBSTITUTED",
    ),
    (
        "OPTION_ALPHA_ISOLATION",
        "OPTION_DATA_NEVER_INFLUENCES_DIRECTION_SIGNAL",
        "VALIDATED_DIRECTION_SIGNAL_DEPENDS_ON_OPTION_DATA",
        "SIGNAL_INPUT_LINEAGE_INCOMPLETE",
        "RESULT_OPTION_OR_CHAIN_DATA_LEAKS_INTO_SIGNAL_OR_POLICY",
    ),
    (
        "COMPARATOR_CONTRACT",
        "ONE_PRIMARY_COMPARATOR_EXACT_AND_SIGNAL_MATCHED",
        "PLATFORM_CANNOT_MAINTAIN_REQUIRED_LEDGER",
        "COMPARATOR_PLATFORM_EVIDENCE_MISSING",
        "COMPARATOR_CHANGED_AFTER_FREEZE_OR_SUBMITS_ORDERS",
    ),
    (
        "CAPITAL_NORMALIZATION",
        "BOTH_PRIMARY_ACCOUNTS_START_WITH_USD_100000",
        "VALIDATED_ACCOUNTING_SHOWS_NONCOMPARABLE_CAPITAL_BASE",
        "START_CAPITAL_OR_LEDGER_EVIDENCE_MISSING",
        "LEVERAGE_MARGIN_NEGATIVE_CASH_OR_POST_RESULT_NORMALIZATION",
    ),
    (
        "EVENT_ALIGNMENT",
        "ALL_EFFECTIVE_EVENTS_RECONCILE_WITH_ZERO_MISMATCH",
        "VALIDATED_NONZERO_EFFECTIVE_EVENT_MISMATCH",
        "EVENT_ALIGNMENT_COUNTS_MISSING",
        "EVENT_CLOCK_OR_QUOTE_SIDE_CHANGED_AFTER_FREEZE",
    ),
    (
        "ACCOUNTING",
        "BOTH_LEDGER_CHRONOLOGIES_AND_BALANCES_RECONCILE",
        "VALIDATED_LEDGER_RECONCILIATION_FAILURE",
        "REQUIRED_ACCOUNT_AGGREGATE_MISSING",
        "LOCAL_OPTION_REPRICING_OR_UNFROZEN_ACCOUNTING_SUBSTITUTE",
    ),
    (
        "RISK_FIELDS",
        "ALL_MANDATORY_RISK_FIELDS_FINITE_AND_RECONCILED",
        "VALIDATED_RISK_FIELD_RECONCILIATION_FAILURE",
        "MANDATORY_RISK_FIELD_OR_DELTA_SAMPLING_EVIDENCE_MISSING",
        "LOCAL_GREEK_RECONSTRUCTION_OR_NONAUTHORITATIVE_SUBSTITUTE",
    ),
    (
        "EXPORT_SAFETY",
        "ONLY_PREREGISTERED_AGGREGATE_FIELDS_EXPORTED",
        "VALIDATED_REQUIRED_EXPORT_FIELD_OMITTED",
        "EXPORT_INVENTORY_EVIDENCE_MISSING",
        "RAW_OPTION_CHAIN_CONTRACT_ROW_QUOTE_HISTORY_OR_SID_EXPORTED",
    ),
    (
        "PLATFORM_IDENTITY",
        "RUN_PROJECT_CODE_LEAN_BUILD_IDENTITIES_EXACT",
        "VALIDATED_PLATFORM_IDENTITY_UNSUPPORTED",
        "LEAN_BUILD_OR_PROJECT_IDENTITY_MISSING",
        "PROJECT_CODE_POLICY_OR_BUILD_IDENTITY_DRIFT",
    ),
    (
        "CALENDAR_SUBPERIOD_COMPLETENESS",
        "FIVE_FIXED_PARTITIONS_COVER_PRIMARY_WINDOW_EXACTLY_ONCE",
        "VALIDATED_PARTITION_GAP_OVERLAP_OR_DROPPED_ZERO_EVENT_YEAR",
        "SUBPERIOD_AGGREGATE_MISSING",
        "POST_RESULT_WINDOW_ADDED_REMOVED_OR_RELABELED",
    ),
    (
        "MULTIPLICITY",
        "ONE_PRIMARY_AND_AT_MOST_TWO_NAMED_DIAGNOSTICS_ONLY",
        "VALIDATED_PREREGISTERED_DIAGNOSTIC_CANNOT_BE_COMPUTED",
        "DIAGNOSTIC_INVENTORY_EVIDENCE_MISSING",
        "POST_RESULT_COMPARATOR_BENCHMARK_OR_SENSITIVITY_ADDED",
    ),
    (
        "PRIMARY_IMPLEMENTATION_ESTIMAND",
        "RETURN_DELTA_STRICTLY_POSITIVE_WITH_ALL_GATES_PASS",
        "RETURN_DELTA_NONPOSITIVE_WITH_VALID_COMPLETE_EVIDENCE",
        "PAIRED_RETURN_DELTA_NOT_COMPUTABLE_FROM_PLATFORM_EVIDENCE",
        "RESULT_USED_TO_SELECT_COMPARATOR_NORMALIZATION_WINDOW_OR_BASELINE",
    ),
    (
        "EXTERNAL_AUTHORIZATION",
        "EXACT_SEPARATE_RUN_AUTHORITY_AND_ALL_ACTION_MAXIMA_SATISFIED",
        "OWNER_EXPLICITLY_DENIES_OR_EXPIRES_AUTHORITY_BEFORE_DISPATCH",
        "OWNER_RUN_AUTHORITY_NOT_GRANTED",
        "UNAUTHORIZED_SAVE_BUILD_BACKTEST_RETRY_EXPORT_OR_PROVIDER_ACTION",
    ),
)


class PairedComparisonContractError(ValueError):
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


class FrozenInheritance(_StrictModel):
    direction_signal_id: Literal["first_layer_composer_v2:trend_state"]
    direction_model_change_allowed: Literal[False]
    source_signal_states: tuple[str, ...]
    option_actions: tuple[str, ...]
    mapping_frozen: Literal[True]
    long_put_in_baseline: Literal[False]
    option_policy_slot_count: Literal[37]
    option_policy_mutation_allowed: Literal[False]
    calendar: Literal["XNYS"]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: Literal[1202]
    expected_transition_count: Literal[83]

    @model_validator(mode="after")
    def validate_exact_inheritance(self) -> Self:
        if self.source_signal_states != _SOURCE_SIGNAL_STATES:
            raise ValueError("source signal state order drifted")
        if self.option_actions != _OPTION_ACTIONS:
            raise ValueError("five-state option mapping drifted")
        window = (date(2021, 2, 22), date(2025, 12, 2))
        if (self.requested_start, self.requested_end) != window:
            raise ValueError("requested primary window drifted")
        if (self.evaluated_start, self.evaluated_end) != window:
            raise ValueError("evaluated primary window drifted")
        return self


class ExistingResultDisposition(_StrictModel):
    backtest_id: Literal["f2879a3cee7ec4e0b68b4f943aafd1f8"]
    authorization_state: Literal["RETROSPECTIVELY_REVIEWED"]
    technical_validation_state: Literal["PASS_EXPORT_SAFE_AGGREGATE_ONLY"]
    evidence_role: Literal["CAPABILITY_AND_DIAGNOSTIC_EVIDENCE_ONLY"]
    baseline_role: Literal["FROZEN_BASELINE_SINGLE_RUN_AGGREGATE"]
    paired_comparator_outcome: Literal["INSUFFICIENT_PLATFORM_EVIDENCE"]
    end_equity_usd: Decimal
    net_profit_percent: Decimal
    fees_usd: Decimal
    orders: Literal[116]
    entries: Literal[58]
    exits: Literal[58]
    cancels: Literal[0]
    can_select_comparator: Literal[False]
    can_select_normalization: Literal[False]
    can_select_window: Literal[False]
    can_change_baseline: Literal[False]

    @model_validator(mode="after")
    def validate_exact_aggregate(self) -> Self:
        observed = (self.end_equity_usd, self.net_profit_percent, self.fees_usd)
        expected = (Decimal("104479.60"), Decimal("4.48"), Decimal("75.40"))
        if observed != expected:
            raise ValueError("admitted aggregate identity drifted")
        return self


class PrimaryComparator(_StrictModel):
    implementation_id: Literal["UNDERLYING_IMPLEMENTATION"]
    method: Literal["SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT"]
    version: Literal["1.0.0-draft.1"]
    initial_cash_usd: Decimal
    long_call_exposure: Literal["UNLEVERED_LONG_QQQ"]
    flat_exposure: Literal["ZERO_RETURN_CASH"]
    same_signal_package_required: Literal[True]
    same_mapping_required: Literal[True]
    same_effective_session_required: Literal[True]
    same_event_clock_required: Literal[True]
    entry_mark: Literal["CURRENT_QQQ_ASK_AT_SAME_LEGAL_EVENT"]
    exit_mark: Literal["CURRENT_QQQ_BID_AT_SAME_LEGAL_EVENT"]
    virtual_ledger_only: Literal[True]
    order_submission_allowed: Literal[False]
    no_eligible_contract_treatment: Literal["RETAIN_UNDERLYING_SIGNAL_EXPOSURE"]
    option_lifecycle_forces_underlying_roll: Literal[False]
    negative_cash_allowed: Literal[False]
    margin_allowed: Literal[False]
    leverage_allowed: Literal[False]
    short_qqq_allowed: Literal[False]
    fill_forward_allowed: Literal[False]

    @field_validator("initial_cash_usd")
    @classmethod
    def validate_initial_cash(cls, value: Decimal) -> Decimal:
        if value != Decimal("100000.00"):
            raise ValueError("primary comparator initial cash drifted")
        return value


class PrimaryEstimand(_StrictModel):
    view_id: Literal["COMMON_CAPITAL_ACCOUNT_VIEW"]
    optionized_initial_cash_usd: Decimal
    underlying_initial_cash_usd: Decimal
    headline_metric: Literal[
        "OPTIONIZED_NET_RETURN_MINUS_UNDERLYING_IMPLEMENTATION_NET_RETURN"
    ]
    pass_rule: Literal["STRICTLY_POSITIVE_RETURN_DELTA_WITH_ALL_GATES_PASS"]
    fail_rule: Literal["NONPOSITIVE_RETURN_DELTA_WITH_VALID_COMPLETE_EVIDENCE"]
    result_blind_freeze_required: Literal[True]
    negative_result_is_valid: Literal[True]
    parameter_change_or_retry_on_failure_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_common_capital(self) -> Self:
        expected = Decimal("100000.00")
        if (self.optionized_initial_cash_usd, self.underlying_initial_cash_usd) != (
            expected,
            expected,
        ):
            raise ValueError("common-capital primary view drifted")
        return self


class SecondaryView(_StrictModel):
    view_id: Literal["CAPITAL_AT_RISK_TIME_VIEW"]
    option_measure: Literal["ENTRY_PREMIUM_DEBIT_TIMES_HOLDING_TIME"]
    underlying_measure: Literal["DEPLOYED_QQQ_CAPITAL_TIMES_HOLDING_TIME"]
    role: Literal["MANDATORY_EXPLANATORY_SECONDARY"]
    may_override_primary: Literal[False]
    conflicting_direction_terminal: Literal["MIXED_IMPLEMENTATION_TRADEOFF"]


class NamedDiagnostic(_StrictModel):
    diagnostic_id: Literal["SGOV_CARRY_COMPARATOR", "QQQ_BUY_AND_HOLD"]
    role: Literal["CONTEXT_ONLY_NOT_PRIMARY_PASS_FAIL"]
    preregistered: Literal[True]


class DiagnosticPolicy(_StrictModel):
    named: tuple[NamedDiagnostic, ...]
    legacy_one_share_role: Literal["EVENT_CLOCK_AND_QUOTE_PATH_DIAGNOSTIC"]
    maximum_primary_comparators: Literal[1]
    maximum_named_diagnostics: Literal[2]
    post_result_addition_allowed: Literal[False]
    realized_delta_without_continuous_platform_evidence: Literal[
        "INSUFFICIENT_PLATFORM_EVIDENCE"
    ]
    local_delta_reconstruction_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_diagnostics(self) -> Self:
        expected = ("SGOV_CARRY_COMPARATOR", "QQQ_BUY_AND_HOLD")
        if tuple(row.diagnostic_id for row in self.named) != expected:
            raise ValueError("named diagnostic surface drifted")
        return self


class ExportSafeFields(_StrictModel):
    identity: tuple[str, ...]
    dq_signal: tuple[str, ...]
    events: tuple[str, ...]
    accounts: tuple[str, ...]
    risk: tuple[str, ...]
    comparator: tuple[str, ...]
    raw_option_rows_allowed: Literal[False]
    complete_chain_allowed: Literal[False]
    contract_identifiers_allowed: Literal[False]
    contract_quote_history_allowed: Literal[False]
    local_option_repricing_input_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_field_surface(self) -> Self:
        actual = (
            self.identity,
            self.dq_signal,
            self.events,
            self.accounts,
            self.risk,
            self.comparator,
        )
        expected = (
            _IDENTITY_FIELDS,
            _DQ_SIGNAL_FIELDS,
            _EVENT_FIELDS,
            _ACCOUNT_FIELDS,
            _RISK_FIELDS,
            _COMPARATOR_FIELDS,
        )
        if actual != expected:
            raise ValueError("export-safe field surface drifted")
        return self


class CalendarPartition(_StrictModel):
    partition_id: str
    start: date
    end: date


class CalendarDiagnostics(_StrictModel):
    partitions: tuple[CalendarPartition, ...]
    exact_once_required: Literal[True]
    independent_backtests: Literal[False]
    refit_or_policy_reselection_allowed: Literal[False]
    zero_event_year_disposition: Literal["RETAIN_ZERO_EVENT_COUNT"]
    post_result_window_addition_allowed: Literal[False]

    @model_validator(mode="after")
    def validate_partitions(self) -> Self:
        actual = tuple((row.partition_id, row.start, row.end) for row in self.partitions)
        if actual != _PARTITIONS:
            raise ValueError("calendar diagnostic partitions drifted")
        for left, right in zip(self.partitions, self.partitions[1:], strict=False):
            if left.end >= right.start:
                raise ValueError("calendar diagnostic partitions overlap")
        return self


class FalsificationAxis(_StrictModel):
    axis_id: str
    pass_when: str
    fail_when: str
    insufficient_when: str
    invalid_when: str


class FalsificationPolicy(_StrictModel):
    terminal_precedence: tuple[str, ...]
    allowed_statuses: tuple[str, ...]
    axes: tuple[FalsificationAxis, ...]
    missing_unknown_or_not_evaluated_can_pass: Literal[False]
    fail_action: Literal["STOP_AND_REPORT_NO_PARAMETER_CHANGE_NO_RETRY"]
    insufficient_action: Literal["RETAIN_GAP_NO_LOCAL_SUBSTITUTE"]
    invalid_action: Literal["QUARANTINE_NO_ADMISSION"]

    @model_validator(mode="after")
    def validate_matrix(self) -> Self:
        if self.terminal_precedence != ("INVALID", "FAIL", "INSUFFICIENT", "PASS"):
            raise ValueError("terminal precedence drifted")
        if self.allowed_statuses != ("PASS", "FAIL", "INSUFFICIENT", "INVALID"):
            raise ValueError("allowed status surface drifted")
        actual = tuple(
            (
                row.axis_id,
                row.pass_when,
                row.fail_when,
                row.insufficient_when,
                row.invalid_when,
            )
            for row in self.axes
        )
        if actual != _AXIS_MATRIX:
            raise ValueError("16-axis falsification matrix drifted")
        return self


class InterpretationBoundary(_StrictModel):
    maximum_interpretation: Literal["RESEARCH_IMPLEMENTATION_COMPARISON_ONLY"]
    signal_alpha_proven: Literal[False]
    robustness_proven: Literal[False]
    investability_proven: Literal[False]
    production_readiness_proven: Literal[False]
    broker_eligibility_proven: Literal[False]


class Safety(_StrictModel):
    static_contract_authorized: Literal[True]
    owner_exact_frozen: Literal[False]
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


class PairedComparisonContract(_CanonicalModel):
    schema_version: Literal["qqq_options_paired_comparison_contract.v1"]
    contract_id: Literal["qc_qqq_options_paired_comparison_contract_v1"]
    contract_version: Literal["1.0.0-draft.1"]
    status: Literal["STATIC_CONTRACT_READY_OWNER_EXACT_FREEZE_REQUIRED"]
    task_id: Literal[
        "TRADING-2548_QQQ_OPTIONS_PAIRED_COMPARATOR_ESTIMAND_AND_EXPORT_CONTRACT_V1"
    ]
    owner_decision_id: Literal[
        "owner_decision:TRADING-2548:2026-08-30:adopt_paired_comparator_contract_wave_v1"
    ]
    scope: Literal["NON_EXECUTABLE_DATA_RESEARCH"]
    authority_bindings: tuple[ImmutableAuthorityBinding, ...]
    frozen_inheritance: FrozenInheritance
    existing_result: ExistingResultDisposition
    primary_comparator: PrimaryComparator
    primary_estimand: PrimaryEstimand
    secondary_view: SecondaryView
    diagnostics: DiagnosticPolicy
    export_safe_fields: ExportSafeFields
    calendar_diagnostics: CalendarDiagnostics
    falsification: FalsificationPolicy
    interpretation: InterpretationBoundary
    safety: Safety
    terminal: Literal["OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FREEZE_REQUIRED_NO_BACKTEST"]

    @model_validator(mode="after")
    def validate_exact_contract(self) -> Self:
        if self.task_id != _TASK_ID:
            raise ValueError("task identity drifted")
        if tuple(row.path for row in self.authority_bindings) != _AUTHORITY_PATHS:
            raise ValueError("authority path order drifted")
        if tuple(row.role for row in self.authority_bindings) != _AUTHORITY_ROLES:
            raise ValueError("authority role order drifted")
        return self


@dataclass(frozen=True)
class PairedComparisonContractLoadResult:
    contract: PairedComparisonContract
    path: Path
    file_sha256: str
    canonical_sha256: str
    terminal: Literal["OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FREEZE_REQUIRED_NO_BACKTEST"]


def _read_mapping(path: Path) -> dict[str, object]:
    payload = load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a mapping")
    return payload


def _require_predecessor_state(*, paths_by_role: dict[str, Path]) -> None:
    draft = _read_mapping(paths_by_role["FROZEN_SIGNAL_IMPLEMENTATION_POLICY"])
    if draft.get("draft_id") != "qc_qqq_options_exact_signal_implementation_policy_draft_v1":
        raise ValueError("frozen policy predecessor identity drifted")
    if draft.get("status") != "OWNER_REVIEW_DRAFT_NON_EXECUTABLE":
        raise ValueError("frozen policy predecessor status drifted")

    freeze = _read_mapping(paths_by_role["OWNER_FREEZE_ADMISSION"])
    if freeze.get("status") != "OWNER_EXACT_POLICY_FROZEN_SIGNAL_PACKAGE_UNADMITTED":
        raise ValueError("owner freeze admission status drifted")
    frozen_surface = freeze.get("frozen_surface")
    if not isinstance(frozen_surface, dict) or frozen_surface.get("frozen_slot_count") != 37:
        raise ValueError("owner freeze admission slot count drifted")

    execution = _read_mapping(paths_by_role["SINGLE_RUN_EXECUTION_POLICY"])
    if execution.get("status") != "OWNER_AUTHORIZED_SINGLE_BOUNDED_QC_DATA_RESEARCH_EXECUTION":
        raise ValueError("single-run execution predecessor status drifted")
    comparator = execution.get("paired_comparator")
    if not isinstance(comparator, dict):
        raise ValueError("single-run predecessor comparator missing")
    expected_comparator = {
        "implementation_id": "UNDERLYING_IMPLEMENTATION",
        "method": "NORMALIZED_ONE_SHARE_QQQ_QUOTE_LEDGER",
        "sizing_or_capital_assumption": "NONE_NORMALIZED_RETURN_ONLY",
        "order_submission_allowed": False,
    }
    for key, value in expected_comparator.items():
        if comparator.get(key) != value:
            raise ValueError(f"single-run predecessor comparator drifted at {key}")

    result_task = _read_mapping(paths_by_role["TERMINAL_RESULT_ADMISSION_EVENT"])
    identity = result_task.get("stable_task_identity")
    projection = result_task.get("projection")
    events = result_task.get("events")
    if not isinstance(identity, dict) or identity.get("task_id") != (
        "TRADING-2542I_QQQ_OPTIONS_EXACT_SIGNAL_AND_IMPLEMENTATION_POLICY_DRAFT_V1"
    ):
        raise ValueError("terminal result task identity drifted")
    if not isinstance(projection, dict) or projection.get("terminal") is not True:
        raise ValueError("terminal result projection drifted")
    if not isinstance(events, list) or not events:
        raise ValueError("terminal result event history missing")
    latest = events[-1]
    if not isinstance(latest, dict) or latest.get("to_status") != "DONE":
        raise ValueError("terminal result admission event drifted")
    latest_payload = latest.get("payload")
    if not isinstance(latest_payload, dict):
        raise ValueError("terminal result admission payload missing")
    joined = json.dumps(latest_payload, ensure_ascii=False, sort_keys=True)
    for marker in (
        "f2879a3cee7ec4e0b68b4f943aafd1f8",
        "RETROSPECTIVELY_REVIEWED",
        "PASS_EXPORT_SAFE_AGGREGATE_ONLY",
    ):
        if marker not in joined:
            raise ValueError(f"terminal result admission marker missing: {marker}")


def load_paired_comparison_contract(
    *,
    path: Path = DEFAULT_PAIRED_COMPARISON_CONTRACT_PATH,
    project_root: Path = PROJECT_ROOT,
) -> PairedComparisonContractLoadResult:
    try:
        resolved = _bound_file(path, root=project_root, field="paired_comparison_contract")
        raw = resolved.read_bytes()
        payload = load_strict_yaml_text(raw.decode("utf-8"), label=str(path))
        contract = PairedComparisonContract.model_validate(payload)
        paths_by_role: dict[str, Path] = {}
        for binding in contract.authority_bindings:
            bound = _bound_file(Path(binding.path), root=project_root, field=binding.role)
            observed_sha = hashlib.sha256(bound.read_bytes()).hexdigest()
            if observed_sha != binding.file_sha256:
                raise ValueError(f"{binding.role} file SHA-256 mismatch")
            paths_by_role[binding.role] = bound
        _require_predecessor_state(paths_by_role=paths_by_role)
    except PairedComparisonContractError:
        raise
    except (KeyError, OSError, TypeError, UnicodeDecodeError, ValueError) as exc:
        raise PairedComparisonContractError(
            "PAIRED_COMPARISON_CONTRACT_REJECTED", str(exc)
        ) from exc
    return PairedComparisonContractLoadResult(
        contract=contract,
        path=resolved,
        file_sha256=hashlib.sha256(raw).hexdigest(),
        canonical_sha256=contract.canonical_sha256,
        terminal="OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FREEZE_REQUIRED_NO_BACKTEST",
    )


__all__ = [
    "DEFAULT_PAIRED_COMPARISON_CONTRACT_PATH",
    "PairedComparisonContract",
    "PairedComparisonContractError",
    "PairedComparisonContractLoadResult",
    "load_paired_comparison_contract",
]
