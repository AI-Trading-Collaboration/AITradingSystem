"""Build and replay the single authorized TRADING-2542I QuantConnect package.

This module is offline-only.  It has no browser, network, QuantConnect API, order,
or broker path.  Its sole external-action output is a content-bound ``main.py``
whose dispatch remains gated by the separately replayed execution manifest.
"""

# The embedded QuantConnect single-file program is intentionally compact to stay
# below the reviewed FREE-project 32 KiB boundary.
# ruff: noqa: E501

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Final

from ai_trading_system.platform.artifacts import (
    canonical_json_bytes,
    sha256_bytes,
    sha256_path,
    write_bytes_atomic,
    write_json_atomic,
)
from ai_trading_system.qqq_options_research.exact_signal_implementation_policy_draft import (
    load_exact_signal_implementation_policy_draft,
)
from ai_trading_system.qqq_options_research.qc_project_adapter_v2 import (
    load_qqq_options_signal_package_for_qc,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path(
    "config/research/qc_qqq_options_exact_signal_implementation_backtest_execution_v1.yaml"
)
DEFAULT_PACKAGE_ROOT = Path(
    "inputs/research/qqq_options/"
    "trading_2542i_exact_signal_implementation_backtest_execution_v1"
)
TASK_ID: Final = (
    "TRADING-2542I_QQQ_OPTIONS_EXACT_SIGNAL_AND_IMPLEMENTATION_POLICY_DRAFT_V1"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_EXPECTED_FILES = ("execution_manifest.json", "main.py", "manifest_replay_receipt.json")


class ExactSignalImplementationBacktestExecutionError(ValueError):
    """Typed fail-closed package or replay error."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class SignalTransition:
    effective_session: date
    action: str


@dataclass(frozen=True)
class ExecutionPolicy:
    path: Path
    file_sha256: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class BuiltExecutionPackage:
    package_root: Path
    main_path: Path
    manifest_path: Path
    replay_receipt_path: Path
    main_sha256: str
    manifest_sha256: str
    replay_receipt_sha256: str
    transition_count: int


def _seal(payload: Mapping[str, Any]) -> dict[str, Any]:
    body = dict(payload)
    if "content_sha256" in body:
        raise ValueError("payload already has content_sha256")
    body["content_sha256"] = sha256_bytes(canonical_json_bytes(body))
    return body


def _verify_seal(payload: Mapping[str, Any], *, code: str) -> None:
    body = dict(payload)
    observed = body.pop("content_sha256", None)
    if observed != sha256_bytes(canonical_json_bytes(body)):
        raise ExactSignalImplementationBacktestExecutionError(code, "content_sha256")


def _require_mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", f"{field} must be a mapping"
        )
    return value


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    actual = set(value)
    if actual != expected:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID",
            f"{field} key drift: missing={sorted(expected-actual)} extra={sorted(actual-expected)}",
        )


def _require_sha(value: object, field: str) -> str:
    text = str(value)
    if not _SHA256.fullmatch(text):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", f"{field} must be lowercase SHA-256"
        )
    return text


def _bound_path(root: Path, value: object, *, field: str, directory: bool = False) -> Path:
    text = str(value)
    relative = Path(text)
    if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != text:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_PATH_INVALID", field
        )
    candidate = root / relative
    current = root
    for part in relative.parts:
        current /= part
        if current.exists() and current.is_symlink():
            raise ExactSignalImplementationBacktestExecutionError(
                "QC_EXECUTION_SYMLINK_PROHIBITED", field
            )
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_PATH_INVALID", field
        ) from exc
    valid = resolved.is_dir() if directory else resolved.is_file()
    if not valid:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_PATH_INVALID", field
        )
    return resolved


def _json_mapping(path: Path, *, code: str) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExactSignalImplementationBacktestExecutionError(code, str(path)) from exc
    if not isinstance(payload, dict):
        raise ExactSignalImplementationBacktestExecutionError(code, str(path))
    return payload


def load_execution_policy(
    path: Path = DEFAULT_POLICY_PATH, *, project_root: Path = PROJECT_ROOT
) -> ExecutionPolicy:
    root = project_root.resolve()
    if path.is_absolute():
        try:
            policy_locator = path.resolve(strict=True).relative_to(root).as_posix()
        except (OSError, ValueError) as exc:
            raise ExactSignalImplementationBacktestExecutionError(
                "QC_EXECUTION_PATH_INVALID", "execution_policy"
            ) from exc
    else:
        policy_locator = path.as_posix()
    resolved = _bound_path(root, policy_locator, field="execution_policy")
    try:
        payload = safe_load_yaml_path(resolved)
    except (OSError, TypeError, ValueError) as exc:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", str(exc)
        ) from exc
    policy = _require_mapping(payload, "policy")
    _require_exact_keys(
        policy,
        {
            "schema_version", "policy_id", "policy_version", "status", "task_id",
            "owner", "owner_decision_ids", "authorization_state", "scope",
            "repository", "quantconnect_target", "authority_bindings",
            "signal_package", "research_window", "implementation",
            "paired_comparator", "action_maxima", "dispatch_gate",
            "result_boundary", "safety", "review_condition", "expiry_condition",
        },
        "policy",
    )
    expected_scalars = {
        "schema_version": "qc_qqq_options_exact_signal_implementation_backtest_execution_policy.v1",
        "policy_id": "qc_qqq_options_exact_signal_implementation_backtest_execution_v1",
        "policy_version": "1.0.0",
        "status": "OWNER_AUTHORIZED_SINGLE_BOUNDED_QC_DATA_RESEARCH_EXECUTION",
        "task_id": TASK_ID,
        "authorization_state": "STANDING_OWNER_SCOPE",
        "scope": "NON_EXECUTABLE_DATA_RESEARCH",
    }
    for key, expected_scalar in expected_scalars.items():
        if policy.get(key) != expected_scalar:
            raise ExactSignalImplementationBacktestExecutionError(
                "QC_EXECUTION_POLICY_INVALID", key
            )
    if policy.get("owner") != "project_owner" or policy.get("owner_decision_ids") != [
        "owner_decision:TRADING-2542I:2026-08-29:authorize_real_materialization_and_conditional_qc_v1",
        "owner_decision:TRADING-2542I:2026-08-30:authorize_fixed_clone_single_qc_retest_v1",
    ]:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "owner_authority"
        )

    expected_repository = {
        "remote_url": "https://github.com/JIEJOE0331/AITradingSystem.git",
        "frozen_base_commit": "a8676a9d53081d8ae5fe6baf9c1523da1df6a0ab",
        "branch": "codex/trading-2542i-qc-bounded-retest",
    }
    if policy.get("repository") != expected_repository:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "repository"
        )

    expected_authorities = [
        {
            "path": "config/research/qc_qqq_options_exact_signal_implementation_policy_draft_v1.yaml",
            "file_sha256": "22335aa324ffb13c9917b65ad57f51916831ecd95c05fe357f7faa13f74b57d0",
            "canonical_sha256": "45c247010f47ad3172215f90aa7c9cd40044b5332284e1789095d230075a5d83",
            "role": "OWNER_EXACT_FROZEN_POLICY",
        },
        {
            "path": "config/research/qc_qqq_options_exact_signal_implementation_policy_freeze_admission_v1.yaml",
            "file_sha256": "a89c3c245795bda3733b9579cbb0f78cf16b5f30ec6115217acab10b26b72d34",
            "role": "OWNER_FREEZE_ADMISSION",
        },
        {
            "path": "config/research/qc_qqq_options_exact_signal_package_admission_v1.yaml",
            "file_sha256": "097ad9409e18311326b4d875c83c2f2a7cf30508092766c27cdd7ec35986d8da",
            "role": "EXACT_SOURCE_ADMISSION_POLICY",
        },
        {
            "path": "config/research/qc_qqq_options_project_adapter_contract_v2.yaml",
            "file_sha256": "081db02fc0d3d1e9e7880a22ce562d0e74ae2693212e7afda7b7344b33347cd1",
            "role": "QC_PROJECT_ADAPTER",
        },
        {
            "path": "config/research/qqq_options_signal_export_v2.yaml",
            "file_sha256": "d6cae89234380794eae841c71a69c1cf9bde237d3a0d5f4c74081f99c3b0dac9",
            "role": "SIGNAL_PACKAGE_POLICY",
        },
        {
            "path": "outputs/research_trends/operational_forecast/trading_2542i_real_v3/real_materialization_receipt.json",
            "file_sha256": "f508581f98b1fa64763b8488568cf0631bfa260fea2b7aa55f9d7f5a0590a230",
            "role": "REAL_DQ_MATERIALIZATION_RECEIPT",
        },
        {
            "path": "outputs/research_trends/operational_forecast/trading_2542i_real_v3/manifest_replay_receipt.json",
            "file_sha256": "1106de7d6e9b63a20d9e68d7228267ea6777a84b2ea1de16215699d3fa7cd9bc",
            "role": "SIGNAL_PACKAGE_MANIFEST_REPLAY",
        },
    ]
    if policy.get("authority_bindings") != expected_authorities:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "authority_bindings"
        )

    target = _require_mapping(policy["quantconnect_target"], "quantconnect_target")
    expected_target = {
        "organization_tier": "FREE", "project_id": 35444189,
        "expected_project_name": "Clone of Sleepy Yellow-Green Shark",
        "protected_original_project_id": 34808569,
        "protected_original_project_name": "Sleepy Yellow-Green Shark",
        "algorithm_language": "Python", "expected_node": "B-MICRO",
        "expected_node_cores": 2, "expected_node_memory_gb": 8,
        "project_file": "main.py", "maximum_project_file_bytes": 32768,
    }
    if dict(target) != expected_target:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "quantconnect_target"
        )

    window = _require_mapping(policy["research_window"], "research_window")
    expected_window = {
        "calendar": "XNYS", "requested_start": "2021-02-22",
        "requested_end": "2025-12-02", "evaluated_start": "2021-02-22",
        "evaluated_end": "2025-12-02", "expected_session_count": 1202,
        "role": "PRIMARY",
    }
    if dict(window) != expected_window:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "research_window"
        )

    expected_signal_package = {
        "root": "outputs/qqq_options/signal_packages/trading_2542i_operational_forecast_real_v3",
        "run_id": "trading_2542i_operational_forecast_real_v3",
        "expected_session_count": 1202,
        "expected_transition_count": 83,
        "package_receipt_sha256": "7cb8807c5938be5453e49c392e3173aca38e10643c643c28b335914196eda494",
        "signal_index_sha256": "d2af99e55d8f9a69a14d4479eb7ba1fd2aeed323c457ae10705a140c7a62ffda",
        "run_manifest_sha256": "214192b58ef7d1965ffba2c0ab658b0cb5768f8856db78c18a1b53ea35d0238b",
        "normalized_signal_source_sha256": "4d26b56bcfc1b21764cb90373fb2da9134838e6c42709b80ba7cbbf0856703f1",
    }
    if policy.get("signal_package") != expected_signal_package:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "signal_package"
        )

    expected_implementation = {
        "underlying": "QQQ", "direction_mapping": {"LONG_CALL": "LONG_CALL", "FLAT": "FLAT"},
        "effective_session_rule": "NEXT_VALID_XNYS_SESSION", "option_right": "CALL",
        "long_premium_only": True, "single_leg_only": True,
        "min_dte_inclusive": 30, "target_dte": 35, "max_dte_inclusive": 45,
        "delta_source": "PRIOR_COMPLETED_SESSION_OPTION_UNIVERSE",
        "min_abs_delta_inclusive": 0.45, "target_abs_delta": 0.50,
        "max_abs_delta_inclusive": 0.60, "min_moneyness_inclusive": 0.90,
        "max_moneyness_inclusive": 1.10, "max_quote_age_seconds": 60,
        "max_relative_spread": 0.20, "min_prior_session_open_interest": 100,
        "rank_components": ["ABS_DELTA_DISTANCE", "ABS_DTE_DISTANCE", "RELATIVE_SPREAD", "OPEN_INTEREST_DESC", "EXPIRY", "STRIKE", "SID"],
        "selection_time_rule": "AFTER_FIRST_COMPLETE_DPLUS1_MINUTE_BAR",
        "submit_time_rule": "NEXT_INDEPENDENT_MINUTE_AFTER_SELECTION",
        "adverse_price_adjustment_per_share_usd": 0.01,
        "fee_per_contract_per_side_usd": 0.65, "cancel_after_minutes": 5,
        "same_session_retry_allowed": False,
        "contract_substitution_after_failure_allowed": False,
        "max_contracts_per_order": 1, "initial_cash_usd": 100000.0,
        "premium_budget_fraction_of_pretrade_nav": 0.02,
        "max_open_contracts": 1, "required_platform_multiplier": 100,
        "pre_expiry_guard_xnys_sessions": 7, "fill_forward_allowed": False,
        "exercise_allowed": False, "assignment_allowed": False,
        "share_delivery_allowed": False, "terminal_mark": "VALID_BID_LIQUIDATION_VALUE",
        "maximum_terminal_mark_quote_age_seconds": 60,
    }
    if policy.get("implementation") != expected_implementation:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "implementation"
        )

    expected_comparator = {
        "implementation_id": "UNDERLYING_IMPLEMENTATION",
        "method": "NORMALIZED_ONE_SHARE_QQQ_QUOTE_LEDGER",
        "signal_identity": "SAME_EFFECTIVE_LONG_CALL_FLAT_TRANSITIONS",
        "entry_mark": "CURRENT_QQQ_ASK_AT_SAME_SIGNAL_NEXT_INDEPENDENT_MINUTE_EVENT",
        "exit_mark": "CURRENT_QQQ_BID_AT_SAME_SIGNAL_NEXT_INDEPENDENT_MINUTE_EVENT",
        "sizing_or_capital_assumption": "NONE_NORMALIZED_RETURN_ONLY",
        "order_submission_allowed": False, "fee_or_slippage_assumption": "NONE",
    }
    if policy.get("paired_comparator") != expected_comparator:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "paired_comparator"
        )

    maxima = _require_mapping(policy["action_maxima"], "action_maxima")
    expected_maxima = {
        "quantconnect_project_mutations": 1, "quantconnect_project_saves": 1,
        "quantconnect_automatic_builds": 1, "quantconnect_cloud_backtests": 1,
        "quantconnect_retries": 0, "quantconnect_project_clones": 0,
        "quantconnect_project_deletions": 0, "external_provider_queries": 0,
        "raw_option_payload_downloads": 0, "raw_option_payload_exports": 0,
        "object_store_writes": 0, "public_shares": 0,
        "custom_terminal_aggregate_logs": 1, "custom_chart_series": 0,
        "maximum_runtime_seconds": 3600,
        "maximum_simulated_order_submissions": 1202,
        "maximum_simulated_fills": 1202,
        "maximum_simultaneous_positions": 1,
        "maximum_contracts_per_simulated_order": 1,
        "orders_outside_qc_simulation": 0, "fills_outside_qc_simulation": 0,
        "positions_outside_qc_simulation": 0,
    }
    if dict(maxima) != expected_maxima:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "action_maxima"
        )

    expected_dispatch = {
        "exact_manifest_replay_required": True,
        "project_identity_visible_match_required": True,
        "main_py_visible_hash_match_required": True,
        "automatic_retry_allowed": False,
        "invalidated_after_first_backtest_dispatch": True,
        "failure_disposition": "STOP_AND_REPORT_NO_RETRY",
    }
    if policy.get("dispatch_gate") != expected_dispatch:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "dispatch_gate"
        )
    expected_result = {
        "exact_session_coverage_required": True,
        "no_contract_no_fill_cancel_retained_as_cash_facts": True,
        "invalid_run_in_aggregate_allowed": False,
        "same_signal_paired_comparator_required": True,
        "maximum_interpretation": "RESEARCH_COMPARISON_ONLY",
        "raw_option_rows_allowed": False,
        "contract_identifiers_exported": False,
        "local_option_repricing_allowed": False,
    }
    if policy.get("result_boundary") != expected_result:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "result_boundary"
        )

    safety = _require_mapping(policy["safety"], "safety")
    expected_safety = {
        "non_executable_data_research_only": True, "paper_allowed": False,
        "live_allowed": False, "production_allowed": False,
        "promotion_allowed": False, "provider_purchase_allowed": False,
        "broker_action": "none", "production_effect": "none",
    }
    if dict(safety) != expected_safety:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "safety"
        )
    return ExecutionPolicy(
        path=resolved, file_sha256=sha256_path(resolved), payload=policy
    )


def _verify_authorities(policy: ExecutionPolicy, root: Path) -> dict[str, str]:
    bindings = policy.payload["authority_bindings"]
    if not isinstance(bindings, list) or len(bindings) != 7:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_POLICY_INVALID", "authority_bindings"
        )
    observed: dict[str, str] = {}
    for index, item in enumerate(bindings):
        binding = _require_mapping(item, f"authority_bindings[{index}]")
        expected_keys = {"path", "file_sha256", "role"}
        if "canonical_sha256" in binding:
            expected_keys.add("canonical_sha256")
        _require_exact_keys(binding, expected_keys, f"authority_bindings[{index}]")
        path = _bound_path(root, binding["path"], field=f"authority[{index}]")
        expected = _require_sha(binding["file_sha256"], f"authority[{index}].sha")
        actual = sha256_path(path)
        if actual != expected:
            raise ExactSignalImplementationBacktestExecutionError(
                "QC_EXECUTION_AUTHORITY_HASH_MISMATCH", str(binding["role"])
            )
        observed[str(binding["role"])] = actual

    draft = load_exact_signal_implementation_policy_draft(project_root=root)
    first = _require_mapping(bindings[0], "authority_bindings[0]")
    if (
        draft.file_sha256 != first["file_sha256"]
        or draft.canonical_sha256 != first.get("canonical_sha256")
    ):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_FROZEN_DRAFT_MISMATCH", "exact draft identity"
        )

    materialization_path = _bound_path(
        root, bindings[5]["path"], field="materialization_receipt"
    )
    materialization = _json_mapping(
        materialization_path, code="QC_EXECUTION_MATERIALIZATION_INVALID"
    )
    required_materialization = {
        "status": "PASS", "package_manifest_replay_status": "PASS",
        "source_admission_status": "PASS", "prediction_row_count": 1202,
        "unique_session_count": 1202, "evaluation_proxy_row_count": 0,
        "orders_outside_qc_simulation": 0, "fills_outside_qc_simulation": 0,
        "positions_outside_qc_simulation": 0, "production_effect": "none",
        "broker_action": "none",
    }
    for key, expected_materialization in required_materialization.items():
        if materialization.get(key) != expected_materialization:
            raise ExactSignalImplementationBacktestExecutionError(
                "QC_EXECUTION_MATERIALIZATION_INVALID", key
            )
    replay_path = _bound_path(root, bindings[6]["path"], field="signal_replay_receipt")
    replay = _json_mapping(replay_path, code="QC_EXECUTION_SIGNAL_REPLAY_INVALID")
    required_replay = {
        "status": "PASS", "daily_signal_count": 1202,
        "canonical_reconstruction_match": True,
        "quantconnect_dispatch_allowed_by_this_receipt": False,
        "requested_start": "2021-02-22", "requested_end": "2025-12-02",
        "evaluated_start": "2021-02-22", "evaluated_end": "2025-12-02",
        "production_effect": "none", "broker_action": "none",
    }
    for key, expected_replay in required_replay.items():
        if replay.get(key) != expected_replay:
            raise ExactSignalImplementationBacktestExecutionError(
                "QC_EXECUTION_SIGNAL_REPLAY_INVALID", key
            )
    return observed


def _load_signal_transitions(
    policy: ExecutionPolicy, root: Path
) -> tuple[tuple[SignalTransition, ...], Any]:
    package_policy = _require_mapping(policy.payload["signal_package"], "signal_package")
    package_root = _bound_path(root, package_policy["root"], field="signal_package", directory=True)
    loaded = load_qqq_options_signal_package_for_qc(
        package_root,
        adapter_policy_path=Path("config/research/qc_qqq_options_project_adapter_contract_v2.yaml"),
        signal_policy_path=Path("config/research/qqq_options_signal_export_v2.yaml"),
        project_root=root,
    )
    expected_hashes = {
        "package_receipt.json": package_policy["package_receipt_sha256"],
        "signal_index.json": package_policy["signal_index_sha256"],
        "run_manifest.json": package_policy["run_manifest_sha256"],
    }
    for name, expected_hash in expected_hashes.items():
        if loaded.file_sha256s.get(name) != expected_hash:
            raise ExactSignalImplementationBacktestExecutionError(
                "QC_EXECUTION_SIGNAL_PACKAGE_HASH_MISMATCH", name
            )
    records = loaded.package.daily_signals
    if len(records) != int(package_policy["expected_session_count"]):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_SIGNAL_COVERAGE_INVALID", "daily_signal_count"
        )
    if (
        records[0].signal_session != date(2021, 2, 22)
        or records[-1].signal_session != date(2025, 12, 2)
        or any(record.signal not in {"LONG_CALL", "FLAT"} for record in records)
    ):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_SIGNAL_COVERAGE_INVALID", "range_or_action"
        )
    transitions: list[SignalTransition] = []
    previous: str | None = None
    for record in records:
        if record.signal != previous:
            transitions.append(
                SignalTransition(record.earliest_effective_session, record.signal)
            )
            previous = record.signal
    if len(transitions) != int(package_policy["expected_transition_count"]):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_SIGNAL_TRANSITION_COUNT_INVALID", str(len(transitions))
        )
    if transitions[0] != SignalTransition(date(2021, 2, 23), "FLAT"):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_SIGNAL_TRANSITION_INVALID", "first"
        )
    return tuple(transitions), loaded


def _transition_bytes(transitions: Sequence[SignalTransition]) -> bytes:
    return canonical_json_bytes(
        [[item.effective_session.isoformat(), item.action] for item in transitions],
        indent=None,
    )


def _render_main(
    *, transitions: Sequence[SignalTransition], policy_sha256: str,
    package_receipt_sha256: str, transition_sha256: str
) -> bytes:
    transition_literal = repr(
        tuple((item.effective_session.isoformat(), item.action == "LONG_CALL") for item in transitions)
    )
    template = r'''from AlgorithmImports import *
from datetime import datetime, timedelta
import json, math

# TRADING-2542I: one owner-authorized QC DATA_RESEARCH backtest; no live/broker/export path.
POLICY_SHA="__POLICY_SHA__"
PACKAGE_SHA="__PACKAGE_SHA__"
TRANSITION_SHA="__TRANSITION_SHA__"
TRANSITIONS=__TRANSITIONS__
START_CASH=100000.0

class PerContractFee(FeeModel):
    def get_order_fee(self, p):
        return OrderFee(CashAmount(abs(p.order.quantity)*0.65,"USD"))

class AdverseLimitFill(ImmediateFillModel):
    def limit_fill(self, asset, order):
        event=super().limit_fill(asset,order)
        if event.status==OrderStatus.FILLED:
            if order.quantity>0: event.fill_price=min(order.limit_price,event.fill_price+0.01)
            else: event.fill_price=max(order.limit_price,event.fill_price-0.01)
        return event

class QQQOptionsExactSignalImplementationRetest(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2021,2,22); self.set_end_date(2025,12,2)
        self.set_cash(START_CASH); self.set_time_zone(TimeZones.NEW_YORK)
        self.set_brokerage_model(DefaultBrokerageModel(AccountType.CASH))
        self.universe_settings.asynchronous=False
        self._qqq=self.add_equity("QQQ",Resolution.MINUTE,fill_forward=False,
            data_normalization_mode=DataNormalizationMode.RAW).symbol
        option=self.add_option("QQQ",Resolution.MINUTE,fill_forward=False)
        option.set_filter(lambda u:u.include_weeklys().calls_only().expiration(30,45).contracts(self._universe))
        self._option=option.symbol; self._prior={}; self._ticket=None; self._pending=None
        self._open=None; self._selected_day=None; self._blocked_day=None; self._invalid=None
        self._sessions=set(); self._long_sessions=0; self._no_candidate=0; self._cancels=0
        self._submissions=0; self._fills=0; self._entries=0; self._exits=0
        self._cmp_active=False; self._cmp_factor=1.0; self._cmp_entry=None
        self._cmp_state=False; self._cmp_day=None; self._last_qqq_bid=None; self._last_qqq_bid_time=None
        self._last_open_bid=None; self._last_open_bid_time=None
        self._signal_day=None; self._signal_value=False

    def _fail(self, reason):
        if self._invalid is None: self._invalid=reason; self.quit(reason)

    @staticmethod
    def _finite(value):
        try: value=float(value)
        except (TypeError,ValueError): return None
        return value if math.isfinite(value) else None

    def _signal(self):
        day=self.time.date()
        if self._signal_day==day: return self._signal_value
        key=day.isoformat(); active=False
        for effective,value in TRANSITIONS:
            if effective>key: break
            active=value
        self._signal_day=day; self._signal_value=active
        return self._signal_value

    def _universe(self, contracts):
        selected=[]; prior={}
        for item in contracts:
            try:
                delta=abs(float(item.greeks.delta)); oi=int(item.open_interest); symbol=item.symbol
            except Exception: continue
            if math.isfinite(delta) and 0.45<=delta<=0.60 and oi>=100:
                selected.append(symbol); prior[symbol]=(delta,oi)
        self._prior=prior
        return selected

    def _quote(self,data,symbol):
        qb=data.quote_bars.get(symbol)
        if qb is None or qb.bid is None or qb.ask is None: return None
        bid=self._finite(qb.bid.close); ask=self._finite(qb.ask.close)
        if bid is None or ask is None or bid<0 or ask<=0 or ask<bid: return None
        end=getattr(qb,"end_time",self.time)
        age=(self.time-end).total_seconds()
        if age<0 or age>60: return None
        return bid,ask

    def _qqq_quote(self,data):
        quote=self._quote(data,self._qqq)
        if quote is not None: self._last_qqq_bid=quote[0]; self._last_qqq_bid_time=self.time
        return quote

    def _comparator(self,data,desired):
        if self._cmp_day==self.time.date() or self.time.hour<9 or (self.time.hour==9 and self.time.minute<32): return
        quote=self._qqq_quote(data)
        if quote is None: return
        self._cmp_day=self.time.date()
        if desired and not self._cmp_state:
            self._cmp_active=True; self._cmp_entry=quote[1]; self._cmp_state=True
        elif not desired and self._cmp_state:
            if self._cmp_entry is None or self._cmp_entry<=0: self._fail("COMPARATOR_ENTRY_INVALID"); return
            self._cmp_factor*=quote[0]/self._cmp_entry
            self._cmp_active=False; self._cmp_entry=None; self._cmp_state=False

    def _select(self,data):
        chain=data.option_chains.get(self._option)
        if chain is None: return None
        underlying=self._finite(self.securities[self._qqq].price)
        if underlying is None or underlying<=0: return None
        ranked=[]
        for contract in chain:
            symbol=contract.symbol; meta=self._prior.get(symbol)
            if meta is None: continue
            quote=self._quote(data,symbol)
            if quote is None: continue
            bid,ask=quote; mid=(bid+ask)/2.0
            if mid<=0 or (ask-bid)/mid>0.20: continue
            strike=float(symbol.id.strike_price); m=underlying/strike if strike>0 else 0
            expiry=symbol.id.date.date() if hasattr(symbol.id.date,"date") else symbol.id.date
            dte=(expiry-self.time.date()).days
            if not (30<=dte<=45 and 0.90<=m<=1.10): continue
            delta,oi=meta
            ranked.append(((abs(delta-0.50),abs(dte-35),(ask-bid)/mid,-oi,expiry,strike,str(symbol.id)),symbol,ask))
        return min(ranked,key=lambda x:x[0]) if ranked else None

    def _sessions_to_expiry(self,symbol):
        expiry=symbol.id.date.date() if hasattr(symbol.id.date,"date") else symbol.id.date
        hours=self.securities[self._qqq].exchange.hours; cursor=self.time; count=0
        for _ in range(15):
            nxt=hours.get_next_market_open(cursor,False)
            if nxt.date()>=expiry: break
            count+=1; cursor=nxt+timedelta(days=1)
        return count

    def _plan_exit(self):
        if self._pending is None and self._ticket is None and self._open is not None:
            self._pending=("EXIT",self._open,self.time); self._blocked_day=self.time.date()

    def _submit_pending(self,data):
        if self._pending is None or self.time<=self._pending[2]: return
        side,symbol,_=self._pending; quote=self._quote(data,symbol)
        if quote is None: return
        bid,ask=quote
        if side=="ENTRY":
            limit=round(ask+0.01,2); reserve=limit*100+0.65
            nav=float(self.portfolio.total_portfolio_value)
            cash=float(self.portfolio.cash_book["USD"].amount)
            if reserve>nav*0.02 or reserve>cash: self._blocked_day=self.time.date(); self._pending=None; return
            security=self.securities[symbol]
            if int(security.symbol_properties.contract_multiplier)!=100: self._fail("MULTIPLIER_INVALID"); return
            security.set_fee_model(PerContractFee()); security.set_fill_model(AdverseLimitFill())
            self._ticket=self.limit_order(symbol,1,limit,tag="T2542I_ENTRY")
        else:
            limit=max(0.01,round(bid-0.01,2)); self._ticket=self.limit_order(symbol,-1,limit,tag="T2542I_EXIT")
        self._submissions+=1; self._pending=None
        if self._submissions>1202: self._fail("ORDER_MAXIMUM_EXCEEDED")

    def _cancel_stale(self):
        if self._ticket is None: return
        order=self.transactions.get_order_by_id(self._ticket.order_id)
        if order is None: return
        if self.time-order.time>=timedelta(minutes=5):
            self._ticket.cancel("T2542I_FIVE_MINUTE_CANCEL"); self._cancels+=1
            self._blocked_day=self.time.date(); self._ticket=None

    def on_data(self,data):
        if self._invalid is not None: return
        day=self.time.date()
        if day not in self._sessions:
            self._sessions.add(day)
            if self._signal(): self._long_sessions+=1
        if float(self.portfolio.cash_book["USD"].amount)<-0.001: self._fail("NEGATIVE_CASH"); return
        if self.portfolio[self._qqq].invested: self._fail("SHARE_DELIVERY_PROHIBITED"); return
        if self._open is not None and abs(float(self.portfolio[self._open].quantity))>1: self._fail("POSITION_MAXIMUM_EXCEEDED"); return
        desired=self._signal(); self._comparator(data,desired); self._cancel_stale()
        self._qqq_quote(data)
        if self._open is not None:
            open_quote=self._quote(data,self._open)
            if open_quote is not None: self._last_open_bid=open_quote[0]; self._last_open_bid_time=self.time
        if self._open is not None and (not desired or self._sessions_to_expiry(self._open)<=7): self._plan_exit()
        self._submit_pending(data)
        if (desired and self._open is None and self._ticket is None and self._pending is None
            and self._selected_day!=day and self._blocked_day!=day
            and (self.time.hour>9 or (self.time.hour==9 and self.time.minute>=31))):
            self._selected_day=day; selected=self._select(data)
            if selected is None: self._no_candidate+=1; self._blocked_day=day
            else: self._pending=("ENTRY",selected[1],self.time)

    def on_order_event(self,event):
        if event.status in (OrderStatus.CANCELED,OrderStatus.INVALID):
            self._blocked_day=self.time.date(); self._ticket=None; return
        if event.status==OrderStatus.PARTIALLY_FILLED: self._fail("PARTIAL_FILL"); return
        if event.status!=OrderStatus.FILLED: return
        self._fills+=1
        order=self.transactions.get_order_by_id(event.order_id)
        if order is None or abs(order.quantity)!=1: self._fail("ORDER_IDENTITY_INVALID"); return
        if order.quantity>0:
            security=self.securities[order.symbol]
            if int(security.symbol_properties.contract_multiplier)!=100: self._fail("MULTIPLIER_INVALID"); return
            self._open=order.symbol; self._entries+=1
        else: self._open=None; self._exits+=1; self._blocked_day=self.time.date()
        self._ticket=None
        if self._fills>1202: self._fail("FILL_MAXIMUM_EXCEEDED")

    def on_assignment_order_event(self,event): self._fail("ASSIGNMENT_OR_EXERCISE")

    def on_end_of_algorithm(self):
        if self._ticket is not None:
            self._ticket.cancel("T2542I_TERMINAL_CANCEL"); self._cancels+=1; self._ticket=None
        valid=self._invalid is None and len(self._sessions)==1202 and self._submissions<=1202 and self._fills<=1202
        if self.portfolio[self._qqq].invested: valid=False; self._invalid=self._invalid or "SHARE_DELIVERY_PROHIBITED"
        terminal=float(self.portfolio.total_portfolio_value)
        if self._open is not None:
            fresh=(self._last_open_bid_time is not None and self._sessions and self._last_open_bid_time.date()==max(self._sessions))
            if self._last_open_bid is None or self._last_open_bid<0 or not fresh: valid=False; self._invalid=self._invalid or "TERMINAL_BID_MISSING_OR_STALE"
            else: terminal-=float(self.portfolio[self._open].holdings_value); terminal+=self._last_open_bid*100
        cmp=self._cmp_factor
        if self._cmp_active:
            fresh=(self._last_qqq_bid_time is not None and self._sessions and self._last_qqq_bid_time.date()==max(self._sessions))
            if self._cmp_entry is None or self._last_qqq_bid is None or not fresh: valid=False; self._invalid=self._invalid or "COMPARATOR_TERMINAL_BID_MISSING_OR_STALE"
            else: cmp*=self._last_qqq_bid/self._cmp_entry
        summary={"schema":"trading_2542i_qc_terminal.v1","status":"PASS" if valid else "INVALID",
          "reason":self._invalid,"requested":"2021-02-22..2025-12-02","evaluated":"2021-02-22..2025-12-02",
          "sessions":len(self._sessions),"expected_sessions":1202,"long_signal_sessions":self._long_sessions,
          "option_return":round(terminal/START_CASH-1,10),"underlying_comparator_return":round(cmp-1,10),
          "order_submissions":self._submissions,"fills":self._fills,"entries":self._entries,"exits":self._exits,
          "no_candidate_sessions":self._no_candidate,"cancels":self._cancels,
          "policy_sha":POLICY_SHA,"package_sha":PACKAGE_SHA,"transition_sha":TRANSITION_SHA,
          "raw_option_rows":False,"contract_identifiers_exported":False,"object_store":False,
          "paper":False,"live":False,"production":False,"broker":"none"}
        self.debug("TRADING2542I_TERMINAL:"+json.dumps(summary,separators=(",",":"),sort_keys=True))
'''
    rendered = (
        template.replace("__POLICY_SHA__", policy_sha256)
        .replace("__PACKAGE_SHA__", package_receipt_sha256)
        .replace("__TRANSITION_SHA__", transition_sha256)
        .replace("__TRANSITIONS__", transition_literal)
        .replace("\r\n", "\n")
    )
    return rendered.encode("utf-8")


def _build_expected(
    *, policy_path: Path = DEFAULT_POLICY_PATH, project_root: Path = PROJECT_ROOT
) -> tuple[ExecutionPolicy, tuple[SignalTransition, ...], bytes, dict[str, Any]]:
    root = project_root.resolve()
    policy = load_execution_policy(policy_path, project_root=root)
    authorities = _verify_authorities(policy, root)
    transitions, loaded = _load_signal_transitions(policy, root)
    transition_raw = _transition_bytes(transitions)
    transition_sha = sha256_bytes(transition_raw)
    package_receipt_sha = loaded.file_sha256s["package_receipt.json"]
    main = _render_main(
        transitions=transitions,
        policy_sha256=policy.file_sha256,
        package_receipt_sha256=package_receipt_sha,
        transition_sha256=transition_sha,
    )
    maximum = int(policy.payload["quantconnect_target"]["maximum_project_file_bytes"])
    if len(main) > maximum:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_MAIN_FILE_TOO_LARGE", f"{len(main)}>{maximum}"
        )
    manifest = _seal(
        {
            "schema_version": "qc_qqq_options_exact_signal_implementation_execution_manifest.v1",
            "manifest_id": "trading_2542i_exact_signal_implementation_single_qc_v1",
            "task_id": TASK_ID,
            "status": "READY_FOR_SINGLE_DISPATCH_AFTER_REPLAY_PASS",
            "authorization_state": policy.payload["authorization_state"],
            "owner_decision_ids": policy.payload["owner_decision_ids"],
            "repository": policy.payload["repository"],
            "offline_generator": {
                "path": Path(__file__).resolve().relative_to(root).as_posix(),
                "file_sha256": sha256_path(Path(__file__).resolve()),
            },
            "execution_policy_path": policy.path.relative_to(root).as_posix(),
            "execution_policy_file_sha256": policy.file_sha256,
            "authority_file_sha256s_by_role": authorities,
            "signal_package": {
                "root": policy.payload["signal_package"]["root"],
                "run_id": loaded.package.receipt.run_id,
                "daily_signal_count": len(loaded.package.daily_signals),
                "transition_count": len(transitions),
                "transition_lf_sha256": transition_sha,
                "package_receipt_sha256": package_receipt_sha,
                "signal_index_sha256": loaded.file_sha256s["signal_index.json"],
                "run_manifest_sha256": loaded.file_sha256s["run_manifest.json"],
            },
            "quantconnect_target": policy.payload["quantconnect_target"],
            "research_window": {
                key: str(value) if isinstance(value, date) else value
                for key, value in policy.payload["research_window"].items()
            },
            "main_py_lf_sha256": sha256_bytes(main),
            "main_py_lf_byte_count": len(main),
            "action_maxima": policy.payload["action_maxima"],
            "dispatch_gate": policy.payload["dispatch_gate"],
            "result_boundary": policy.payload["result_boundary"],
            "safety": policy.payload["safety"],
            "manifest_replay_status": "PENDING",
            "dispatch_count": 0,
            "orders_outside_qc_simulation": 0,
            "fills_outside_qc_simulation": 0,
            "positions_outside_qc_simulation": 0,
        }
    )
    return policy, transitions, main, manifest


def build_execution_package(
    *, output_root: Path = DEFAULT_PACKAGE_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH, project_root: Path = PROJECT_ROOT
) -> BuiltExecutionPackage:
    root = project_root.resolve()
    policy, transitions, main, manifest = _build_expected(
        policy_path=policy_path, project_root=root
    )
    target = output_root if output_root.is_absolute() else root / output_root
    if target.exists() and target.is_symlink():
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_OUTPUT_PATH_INVALID", str(output_root)
        )
    target = target.resolve()
    main_result = write_bytes_atomic(target / "main.py", main)
    manifest_result = write_json_atomic(target / "execution_manifest.json", manifest)
    replay = replay_execution_package(
        package_root=target, policy_path=policy.path, project_root=root,
        expected_manifest=manifest, validate_existing_receipt=False,
    )
    receipt_result = write_json_atomic(target / "manifest_replay_receipt.json", replay)
    return BuiltExecutionPackage(
        package_root=target,
        main_path=main_result.path,
        manifest_path=manifest_result.path,
        replay_receipt_path=receipt_result.path,
        main_sha256=main_result.sha256,
        manifest_sha256=manifest_result.sha256,
        replay_receipt_sha256=receipt_result.sha256,
        transition_count=len(transitions),
    )


def replay_execution_package(
    *, package_root: Path = DEFAULT_PACKAGE_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH, project_root: Path = PROJECT_ROOT,
    expected_manifest: Mapping[str, Any] | None = None,
    validate_existing_receipt: bool = True,
) -> dict[str, Any]:
    root = project_root.resolve()
    target = package_root if package_root.is_absolute() else root / package_root
    try:
        target = target.resolve(strict=True)
    except OSError as exc:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_PACKAGE_PATH_INVALID", str(package_root)
        ) from exc
    if target.is_symlink() or not target.is_dir():
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_PACKAGE_PATH_INVALID", str(package_root)
        )
    inventory = tuple(sorted(item.name for item in target.iterdir() if item.is_file()))
    allowed_inventory = ("execution_manifest.json", "main.py") if not (target / "manifest_replay_receipt.json").exists() else _EXPECTED_FILES
    if inventory != allowed_inventory or any(item.is_symlink() for item in target.iterdir()):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_PACKAGE_INVENTORY_INVALID", repr(inventory)
        )
    _, transitions, expected_main, rebuilt_manifest = _build_expected(
        policy_path=policy_path, project_root=root
    )
    manifest = _json_mapping(
        target / "execution_manifest.json", code="QC_EXECUTION_MANIFEST_INVALID"
    )
    _verify_seal(manifest, code="QC_EXECUTION_MANIFEST_SEAL_INVALID")
    comparison = expected_manifest if expected_manifest is not None else rebuilt_manifest
    if dict(manifest) != dict(comparison):
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_MANIFEST_REPLAY_MISMATCH", "execution_manifest.json"
        )
    actual_main = (target / "main.py").read_bytes()
    if actual_main != expected_main:
        raise ExactSignalImplementationBacktestExecutionError(
            "QC_EXECUTION_MAIN_REPLAY_MISMATCH", "main.py"
        )
    manifest_sha = sha256_path(target / "execution_manifest.json")
    receipt = _seal(
        {
            "schema_version": "qc_qqq_options_exact_signal_implementation_manifest_replay_receipt.v1",
            "task_id": TASK_ID,
            "status": "PASS",
            "technical_validation_state": "PASS",
            "quantconnect_dispatch_gate": "PASS",
            "authorization_state": "STANDING_OWNER_SCOPE",
            "project_id": 35444189,
            "protected_original_project_id": 34808569,
            "execution_manifest_file_sha256": manifest_sha,
            "execution_manifest_content_sha256": manifest["content_sha256"],
            "main_py_lf_sha256": sha256_bytes(actual_main),
            "main_py_lf_byte_count": len(actual_main),
            "daily_signal_count": 1202,
            "signal_transition_count": len(transitions),
            "requested_start": "2021-02-22",
            "requested_end": "2025-12-02",
            "evaluated_start": "2021-02-22",
            "evaluated_end": "2025-12-02",
            "maximum_cloud_backtests": 1,
            "automatic_retry_allowed": False,
            "orders_outside_qc_simulation": 0,
            "fills_outside_qc_simulation": 0,
            "positions_outside_qc_simulation": 0,
            "raw_option_payload_export_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    if validate_existing_receipt and (target / "manifest_replay_receipt.json").exists():
        existing = _json_mapping(
            target / "manifest_replay_receipt.json",
            code="QC_EXECUTION_REPLAY_RECEIPT_INVALID",
        )
        _verify_seal(existing, code="QC_EXECUTION_REPLAY_RECEIPT_SEAL_INVALID")
        if dict(existing) != receipt:
            raise ExactSignalImplementationBacktestExecutionError(
                "QC_EXECUTION_REPLAY_RECEIPT_MISMATCH", "manifest_replay_receipt.json"
            )
    return receipt


__all__ = [
    "DEFAULT_PACKAGE_ROOT", "DEFAULT_POLICY_PATH", "BuiltExecutionPackage",
    "ExactSignalImplementationBacktestExecutionError", "ExecutionPolicy",
    "SignalTransition", "build_execution_package", "load_execution_policy",
    "replay_execution_package",
]
