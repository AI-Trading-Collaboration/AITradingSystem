from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import (
    AI_REGIME_START,
    utc_now_iso,
    write_foundation_artifact_pair,
)
from ai_trading_system.equal_risk_growth_tilt import (
    DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    FOCUSED_GROWTH_TILT_CANDIDATE_ID,
    run_balanced_core_definition_lock,
)
from ai_trading_system.equal_risk_growth_tilt import (
    _candidate_from_definition_lock as _balanced_core_candidate_from_lock,
)
from ai_trading_system.equal_risk_growth_tilt import (
    _weight_frame as _growth_weight_frame,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    _data_quality_gate,
    _load_price_matrix,
    _load_registry,
    _metrics_for_strategy,
    _required_tickers,
    _research_policy_int,
    _slice_prices,
    _strategy_by_id,
    _strategy_return_series,
    _target_weight_frame,
    _turnover_series,
)

DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "external_validation"
)
DEFAULT_EXTERNAL_VALIDATION_OWNER_REPORT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "external_validation_owner_report.md"
)
DEFAULT_EXTERNAL_VALIDATION_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "external_validation_master_review.md"
)

EXTERNAL_VALIDATION_STRATEGY_IDS = (
    "100_qqq",
    "qqq_50_sgov_50",
    "qqq_60_sgov_40",
    "equal_risk_qqq_sgov",
    FOCUSED_GROWTH_TILT_CANDIDATE_ID,
)
STATIC_BASELINE_IDS = ("100_qqq", "qqq_50_sgov_50", "qqq_60_sgov_40")
DYNAMIC_REPLAY_IDS = ("equal_risk_qqq_sgov", FOCUSED_GROWTH_TILT_CANDIDATE_ID)

SAFETY_BOUNDARY: dict[str, Any] = {
    "production_effect": "none",
    "broker_action": "none",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "manual_review_required": True,
    "research_only": True,
    "observe_only": True,
}

AI_REGIME_SUMMARY: dict[str, str] = {
    "market_regime": "ai_after_chatgpt",
    "anchor_event": "ChatGPT public launch",
    "anchor_date": "2022-11-30",
    "default_backtest_start": (
        AI_REGIME_START
        if isinstance(AI_REGIME_START, date)
        else date.fromisoformat(str(AI_REGIME_START))
    ).isoformat(),
}

METRIC_TOLERANCE: dict[str, float] = {
    "annual_return": 0.005,
    "max_drawdown": 0.01,
    "sharpe": 0.10,
    "calmar": 0.10,
    "monthly_return_correlation_min": 0.995,
}


def run_external_validation_scope_contract(
    *,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    payload = _payload(
        report_type="external_validation_scope_contract",
        title="External Validation Scope Contract",
        status="EXTERNAL_VALIDATION_SCOPE_READY",
        summary={
            "strategy_count": len(EXTERNAL_VALIDATION_STRATEGY_IDS),
            "baseline_count": len(STATIC_BASELINE_IDS),
            "date_range": _date_range_label(start_date, end_date),
            **_safety_summary(),
        },
        validation_scope="external_backtest_validation_and_reconciliation",
        strategy_ids=list(EXTERNAL_VALIDATION_STRATEGY_IDS),
        baseline_ids=list(STATIC_BASELINE_IDS),
        external_tools=[
            "Portfolio Visualizer",
            "testfol.io",
            "QuantConnect",
            "TradingView Pine Script",
            "local independent notebook",
        ],
        date_range={
            "start": start_date.isoformat(),
            "end": end_date.isoformat() if end_date else "latest_available",
            "market_regime": "ai_after_chatgpt",
        },
        price_field_policy={
            "internal_price_field": "adj_close",
            "external_expected_field": "dividend_adjusted_total_return_when_available",
            "sgov_policy": "explicit_total_return_or_adjusted_close_check_required",
        },
        rebalance_policy={
            "static_baselines": "monthly",
            "dynamic_weight_path": "use_exported_target_weights",
            "execution_assumption": "next_close_after_signal_weight",
        },
        cost_policy={
            "default_cost_bps": 0.0,
            "slippage": "not_modeled_for_reconciliation_baseline",
        },
        metric_tolerance=METRIC_TOLERANCE,
        manual_review_required=True,
        report_registry_entry=_report_registry_entry(
            "external_validation_scope_contract",
            "External Validation Scope Contract",
            "aits research strategies external-validation-scope-contract",
            "external_validation_scope_contract",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_static_baseline_external_reconciliation(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    external_records_path: Path | None = None,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(simple_config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    external_records = _load_external_records(external_records_path)
    blockers = []
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    internal_rows = _internal_static_metric_rows(config, prices)
    reconciliation_rows = [
        _static_reconciliation_row(row, external_records.get(str(row["strategy_id"])))
        for row in internal_rows
    ]
    missing_external = [
        row["strategy_id"] for row in reconciliation_rows if not row["external_record_present"]
    ]
    mismatches = [row for row in reconciliation_rows if row["within_tolerance"] is False]
    if blockers:
        status = "STATIC_BASELINE_BLOCKED"
    elif mismatches:
        status = "STATIC_BASELINE_MISMATCH"
    elif missing_external:
        status = "STATIC_BASELINE_RECONCILED_WITH_WARNINGS"
    else:
        status = "STATIC_BASELINE_RECONCILED"
    payload = _payload(
        report_type="static_baseline_external_reconciliation",
        title="Static Baseline External Reconciliation",
        status=status,
        summary={
            "strategy_count": len(reconciliation_rows),
            "missing_external_record_count": len(missing_external),
            "mismatch_count": len(mismatches),
            "data_quality_status": data_gate.get("status"),
            **_safety_summary(),
        },
        external_records_path=str(external_records_path) if external_records_path else None,
        external_record_status=(
            "MANUAL_EXTERNAL_INPUT_SUPPLIED"
            if external_records
            else "MANUAL_EXTERNAL_INPUT_PENDING"
        ),
        reconciliation_rows=reconciliation_rows,
        data_quality=data_gate,
        blocking_reasons=blockers,
        warning_reasons=[
            "manual_external_records_missing"
        ]
        if missing_external
        else [],
        report_registry_entry=_report_registry_entry(
            "static_baseline_external_reconciliation",
            "Static Baseline External Reconciliation",
            "aits research strategies static-baseline-external-reconciliation",
            "static_baseline_external_reconciliation",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_strategy_weight_path_export(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    simple_config = _load_registry(simple_config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=simple_config,
        as_of_date=as_of_date,
    )
    blockers = []
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    paths: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    definition_hashes: dict[str, str] = {}
    if not blockers:
        prices = _price_matrix(prices_path, simple_config, start_date=start_date, end_date=end_date)
        for strategy_id in DYNAMIC_REPLAY_IDS:
            weights, definition_hash = _weights_for_strategy(
                strategy_id,
                prices=prices,
                simple_config=simple_config,
                growth_config_path=growth_config_path,
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            )
            frame = _weight_path_frame(
                strategy_id,
                weights,
                definition_hash,
                str(data_gate["status"]),
            )
            path = output_root / "weight_paths" / f"{strategy_id}_weight_path.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(path, index=False)
            paths[strategy_id] = str(path)
            row_counts[strategy_id] = int(len(frame))
            definition_hashes[strategy_id] = definition_hash
    status = "WEIGHT_PATH_EXPORT_BLOCKED" if blockers else "WEIGHT_PATH_EXPORT_READY"
    if not blockers and _int(data_gate.get("warning_count")) > 0:
        status = "WEIGHT_PATH_EXPORT_WARN"
    payload = _payload(
        report_type="strategy_weight_path_export",
        title="Strategy Weight Path Export",
        status=status,
        summary={
            "strategy_count": len(paths),
            "data_quality_status": data_gate.get("status"),
            **_safety_summary(),
        },
        strategy_ids=list(DYNAMIC_REPLAY_IDS),
        exported_weight_paths=paths,
        row_counts=row_counts,
        definition_hashes=definition_hashes,
        data_quality=data_gate,
        blocking_reasons=blockers,
        report_registry_entry=_report_registry_entry(
            "strategy_weight_path_export",
            "Strategy Weight Path Export",
            "aits research strategies strategy-weight-path-export",
            "strategy_weight_path_export",
            extra_artifact_globs=[
                "outputs/research_strategies/external_validation/weight_paths/*_weight_path.csv"
            ],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_external_independent_return_replay(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _weight_export_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    simple_config = _load_registry(simple_config_path)
    weight_export = dict(
        _weight_export_payload
        or run_strategy_weight_path_export(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=simple_config,
        as_of_date=as_of_date,
    )
    blockers = []
    if weight_export.get("status") == "WEIGHT_PATH_EXPORT_BLOCKED":
        blockers.append("weight_path_export_blocked")
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    replay_rows: list[dict[str, Any]] = []
    if not blockers:
        prices = _price_matrix(prices_path, simple_config, start_date=start_date, end_date=end_date)
        qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
        for strategy_id, path_text in _mapping(weight_export.get("exported_weight_paths")).items():
            weight_path = Path(str(path_text))
            weights = _read_weight_path(weight_path, prices.index)
            replay_returns = _replay_returns_from_weights(weights, prices)
            replay_metrics = _metrics_for_strategy(
                {"strategy_id": strategy_id, "display_name": strategy_id},
                replay_returns,
                weights,
                qqq_returns,
                annualization=_research_policy_int(simple_config, "annualization_trading_days"),
                cost_bps=0.0,
            )
            internal_metrics = _internal_dynamic_metrics(
                strategy_id,
                prices=prices,
                simple_config=simple_config,
                growth_config_path=growth_config_path,
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            )
            replay_rows.append(
                _replay_reconciliation_row(strategy_id, replay_metrics, internal_metrics)
            )
    mismatches = [row for row in replay_rows if row["all_metrics_within_tolerance"] is False]
    if blockers:
        status = "INDEPENDENT_REPLAY_BLOCKED"
    elif mismatches:
        status = "INDEPENDENT_REPLAY_MISMATCH"
    elif _int(data_gate.get("warning_count")) > 0:
        status = "INDEPENDENT_REPLAY_MATCHED_WITH_WARNINGS"
    else:
        status = "INDEPENDENT_REPLAY_MATCHED"
    payload = _payload(
        report_type="external_independent_return_replay",
        title="External Independent Return Replay",
        status=status,
        summary={
            "strategy_count": len(replay_rows),
            "mismatch_count": len(mismatches),
            "data_quality_status": data_gate.get("status"),
            **_safety_summary(),
        },
        replay_inputs={
            "price_data_source": str(prices_path),
            "price_field": "adj_close",
            "rebalance_policy": "use_exported_weight_path",
            "cost_policy": "0bps baseline",
            "execution_assumption": "weights apply to next daily return",
        },
        replay_rows=replay_rows,
        data_quality=data_gate,
        blocking_reasons=blockers,
        source_artifacts={"strategy_weight_path_export": weight_export.get("artifact_paths", {})},
        report_registry_entry=_report_registry_entry(
            "external_independent_return_replay",
            "External Independent Return Replay",
            "aits research strategies external-independent-return-replay",
            "external_independent_return_replay",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_metric_definition_reconciliation(
    *,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
) -> dict[str, Any]:
    checks = [
        _definition_check("annual_return calculation", "CAGR from cumulative equity"),
        _definition_check("CAGR vs arithmetic annualized return", "CAGR is canonical"),
        _definition_check("max_drawdown calculation", "equity / cumulative peak - 1"),
        _definition_check("Sharpe risk-free rate assumption", "0 risk-free baseline"),
        _definition_check("volatility annualization factor", "252 trading days"),
        _definition_check("Calmar calculation", "annual_return / abs(max_drawdown)"),
        _definition_check("monthly return aggregation", "calendar month compounded returns"),
        _definition_check("dividend / adjusted close handling", "adj_close required"),
        _definition_check("SGOV total-return proxy handling", "explicit SGOV check required"),
        _definition_check("rebalance date alignment", "target weights shift one day"),
        _definition_check("execution date alignment", "signal close, next-return execution"),
        _definition_check("cost and slippage assumption", "0 bps reconciliation baseline"),
    ]
    payload = _payload(
        report_type="metric_definition_reconciliation",
        title="Metric Definition Reconciliation",
        status="METRIC_DEFINITIONS_RECONCILED_WITH_WARNINGS",
        summary={
            "definition_check_count": len(checks),
            "warning_count": 1,
            **_safety_summary(),
        },
        definition_checks=checks,
        warning_reasons=[
            "external_platform_metric_definitions_must_be_manually_confirmed_per_tool"
        ],
        report_registry_entry=_report_registry_entry(
            "metric_definition_reconciliation",
            "Metric Definition Reconciliation",
            "aits research strategies metric-definition-reconciliation",
            "metric_definition_reconciliation",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_sgov_total_return_external_check(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(simple_config_path)
    data_gate = _data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config=config,
        as_of_date=as_of_date,
    )
    blockers = []
    if not bool(data_gate.get("passed")):
        blockers.append("validate_data_cache_failed")
    rows: list[dict[str, Any]] = []
    sgov_summary: dict[str, Any] = {}
    if not blockers:
        raw_prices = pd.read_csv(prices_path, parse_dates=["date"])
        sgov = raw_prices.loc[raw_prices["ticker"].astype(str) == "SGOV"].sort_values("date")
        sgov = sgov[(sgov["date"] >= pd.Timestamp(start_date))]
        if end_date is not None:
            sgov = sgov[sgov["date"] <= pd.Timestamp(end_date)]
        close_return = _compound_return(sgov["close"].pct_change().fillna(0.0))
        adjusted_return = _compound_return(sgov["adj_close"].pct_change().fillna(0.0))
        difference = adjusted_return - close_return
        sgov_summary = {
            "SGOV_close_return": _round(close_return),
            "SGOV_adjusted_close_return": _round(adjusted_return),
            "difference_vs_price_only": _round(difference),
            "external_platform_SGOV_handling": "manual_platform_confirmation_required",
            "internal_SGOV_total_return_proxy": "adj_close",
        }
        prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
        for strategy_id in (
            "qqq_50_sgov_50",
            "qqq_60_sgov_40",
            "equal_risk_qqq_sgov",
            FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        ):
            rows.append(
                _sgov_impact_row(
                    strategy_id,
                    prices=prices,
                    raw_prices=raw_prices,
                    simple_config=config,
                    simple_config_path=simple_config_path,
                    growth_config_path=growth_config_path,
                    prices_path=prices_path,
                    marketstack_prices_path=marketstack_prices_path,
                    rates_path=rates_path,
                    as_of_date=as_of_date,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
    max_impact = max((_float(row.get("annual_return_impact")) for row in rows), default=0.0)
    if blockers:
        status = "SGOV_TOTAL_RETURN_BLOCKED"
    elif abs(max_impact) > 0.001:
        status = "SGOV_PRICE_ONLY_DIFFERENCE_WARN"
    else:
        status = "SGOV_TOTAL_RETURN_RECONCILED"
    payload = _payload(
        report_type="sgov_total_return_external_check",
        title="SGOV Total-Return External Check",
        status=status,
        summary={
            "impact_row_count": len(rows),
            "max_annual_return_impact": _round(max_impact),
            "data_quality_status": data_gate.get("status"),
            **_safety_summary(),
        },
        sgov_total_return_summary=sgov_summary,
        impact_rows=rows,
        data_quality=data_gate,
        blocking_reasons=blockers,
        report_registry_entry=_report_registry_entry(
            "sgov_total_return_external_check",
            "SGOV Total-Return External Check",
            "aits research strategies sgov-total-return-external-check",
            "sgov_total_return_external_check",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_external_platform_feasibility_review(
    *,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
) -> dict[str, Any]:
    rows = [
        {
            "platform": "Portfolio Visualizer",
            "supports_static_weights": True,
            "supports_dynamic_weight_path": False,
            "supports_custom_strategy_logic": False,
            "supports_adjusted_close": True,
            "supports_SGOV_total_return": "manual_confirmation_required",
            "supports_transaction_cost": "limited",
            "supports_export": True,
            "implementation_effort": "low",
            "recommended_use": "static_baseline_external_check",
        },
        {
            "platform": "testfol.io",
            "supports_static_weights": True,
            "supports_dynamic_weight_path": False,
            "supports_custom_strategy_logic": False,
            "supports_adjusted_close": True,
            "supports_SGOV_total_return": "manual_confirmation_required",
            "supports_transaction_cost": "limited",
            "supports_export": "limited",
            "implementation_effort": "low",
            "recommended_use": "static_baseline_cross_check",
        },
        {
            "platform": "QuantConnect",
            "supports_static_weights": True,
            "supports_dynamic_weight_path": True,
            "supports_custom_strategy_logic": True,
            "supports_adjusted_close": True,
            "supports_SGOV_total_return": "data_subscription_dependent",
            "supports_transaction_cost": True,
            "supports_export": True,
            "implementation_effort": "medium_high",
            "recommended_use": "dynamic_weight_path_or_full_strategy_replay",
        },
        {
            "platform": "TradingView Pine Script",
            "supports_static_weights": "limited",
            "supports_dynamic_weight_path": "manual_or_import_limited",
            "supports_custom_strategy_logic": True,
            "supports_adjusted_close": "symbol_dependent",
            "supports_SGOV_total_return": "limited",
            "supports_transaction_cost": True,
            "supports_export": "limited",
            "implementation_effort": "medium",
            "recommended_use": "logic_feasibility_not_authoritative_reconciliation",
        },
        {
            "platform": "local independent notebook",
            "supports_static_weights": True,
            "supports_dynamic_weight_path": True,
            "supports_custom_strategy_logic": True,
            "supports_adjusted_close": True,
            "supports_SGOV_total_return": True,
            "supports_transaction_cost": True,
            "supports_export": True,
            "implementation_effort": "low_medium",
            "recommended_use": "authoritative_independent_replay",
        },
    ]
    status = "EXTERNAL_PLATFORM_DYNAMIC_REPLAY_NEEDED"
    payload = _payload(
        report_type="external_platform_feasibility_review",
        title="External Platform Feasibility Review",
        status=status,
        summary={
            "platform_count": len(rows),
            "recommended_dynamic_replay": "local independent notebook",
            **_safety_summary(),
        },
        platform_rows=rows,
        report_registry_entry=_report_registry_entry(
            "external_platform_feasibility_review",
            "External Platform Feasibility Review",
            "aits research strategies external-platform-feasibility-review",
            "external_platform_feasibility_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_quantconnect_replication_dry_run_plan(
    *,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _payload(
        report_type="quantconnect_replication_dry_run_plan",
        title="QuantConnect Replication Dry-Run Plan",
        status="QUANTCONNECT_REPLICATION_PLAN_READY",
        summary={
            "phase_count": 3,
            "manual_step_count": 5,
            **_safety_summary(),
        },
        replication_scope="dry_run_plan_only_no_external_connection",
        strategy_ids=list(EXTERNAL_VALIDATION_STRATEGY_IDS),
        data_source_mapping={
            "QQQ": "QuantConnect ETF adjusted data",
            "SGOV": "QuantConnect ETF adjusted data or custom total-return series",
            "TQQQ": "QuantConnect ETF adjusted data",
        },
        rebalance_logic="monthly static baselines, imported weight path for dynamic replay",
        weight_calculation_logic="phase_3_only_rewrite_equal_risk_and_balanced_core_rules",
        execution_timing="signal close, next bar execution",
        cost_model="0bps reconciliation baseline then sensitivity",
        expected_outputs=[
            "equity_curve",
            "daily_returns",
            "monthly_returns",
            "annual_return",
            "max_drawdown",
            "sharpe",
            "calmar",
        ],
        phases=[
            {"phase": 1, "scope": "static baselines only"},
            {"phase": 2, "scope": "imported weight path replay"},
            {"phase": 3, "scope": "full equal-risk and balanced-core rule rewrite"},
        ],
        manual_steps=[
            "create QuantConnect research project",
            "confirm ETF adjusted data behavior",
            "upload or import weight path CSV",
            "compare metric definitions",
            "attach exports/screenshots to reconciliation artifact",
        ],
        blocking_issues=[
            "SGOV total-return behavior requires manual confirmation",
            "external account credentials are intentionally not used by this workflow",
        ],
        report_registry_entry=_report_registry_entry(
            "quantconnect_replication_dry_run_plan",
            "QuantConnect Replication Dry-Run Plan",
            "aits research strategies quantconnect-replication-dry-run-plan",
            "quantconnect_replication_dry_run_plan",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_external_validation_difference_attribution(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _static_reconciliation_payload: Mapping[str, Any] | None = None,
    _replay_payload: Mapping[str, Any] | None = None,
    _metric_payload: Mapping[str, Any] | None = None,
    _sgov_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    static = dict(
        _static_reconciliation_payload
        or run_static_baseline_external_reconciliation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    replay = dict(
        _replay_payload
        or run_external_independent_return_replay(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    metric = dict(_metric_payload or run_metric_definition_reconciliation(output_root=output_root))
    sgov = dict(
        _sgov_payload
        or run_sgov_total_return_external_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    rows = []
    for row in _records(static.get("reconciliation_rows")):
        rows.extend(_difference_rows_from_static(row))
    for row in _records(replay.get("replay_rows")):
        rows.extend(_difference_rows_from_replay(row))
    if sgov.get("status") == "SGOV_PRICE_ONLY_DIFFERENCE_WARN":
        rows.append(
            {
                "strategy_id": "SGOV",
                "metric": "total_return_handling",
                "internal_value": _mapping(sgov.get("sgov_total_return_summary")).get(
                    "SGOV_adjusted_close_return"
                ),
                "external_or_replay_value": _mapping(sgov.get("sgov_total_return_summary")).get(
                    "SGOV_close_return"
                ),
                "delta": _mapping(sgov.get("sgov_total_return_summary")).get(
                    "difference_vs_price_only"
                ),
                "primary_difference_reason": "SGOV_total_return_difference",
                "secondary_difference_reason": "external_platform_limitation",
                "requires_fix": False,
                "owner_next_action": "confirm_external_platform_uses_adjusted_or_total_return",
            }
        )
    requires_fix = [row for row in rows if row.get("requires_fix") is True]
    unexplained = [
        row
        for row in rows
        if row.get("primary_difference_reason") == "internal_bug_suspected"
    ]
    if requires_fix:
        status = "DIFFERENCE_REQUIRES_INTERNAL_FIX"
    elif unexplained:
        status = "DIFFERENCE_ATTRIBUTION_BLOCKED"
    elif rows:
        status = "DIFFERENCES_EXPLAINED"
    else:
        status = "DIFFERENCE_ATTRIBUTION_READY"
    payload = _payload(
        report_type="external_validation_difference_attribution",
        title="External Validation Difference Attribution",
        status=status,
        summary={
            "difference_row_count": len(rows),
            "requires_fix_count": len(requires_fix),
            **_safety_summary(),
        },
        difference_rows=rows,
        source_statuses={
            "static_baseline_external_reconciliation": static.get("status"),
            "external_independent_return_replay": replay.get("status"),
            "metric_definition_reconciliation": metric.get("status"),
            "sgov_total_return_external_check": sgov.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "static_baseline_external_reconciliation": static,
                "external_independent_return_replay": replay,
                "metric_definition_reconciliation": metric,
                "sgov_total_return_external_check": sgov,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "external_validation_difference_attribution",
            "External Validation Difference Attribution",
            "aits research strategies external-validation-difference-attribution",
            "external_validation_difference_attribution",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_external_validation_owner_report(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_EXTERNAL_VALIDATION_OWNER_REPORT_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _scope_payload: Mapping[str, Any] | None = None,
    _static_payload: Mapping[str, Any] | None = None,
    _replay_payload: Mapping[str, Any] | None = None,
    _metric_payload: Mapping[str, Any] | None = None,
    _sgov_payload: Mapping[str, Any] | None = None,
    _feasibility_payload: Mapping[str, Any] | None = None,
    _difference_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sources = _owner_sources(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        growth_config_path=growth_config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        scope_payload=_scope_payload,
        static_payload=_static_payload,
        replay_payload=_replay_payload,
        metric_payload=_metric_payload,
        sgov_payload=_sgov_payload,
        feasibility_payload=_feasibility_payload,
        difference_payload=_difference_payload,
    )
    answers = _owner_required_answers(sources)
    recommendation = _owner_recommendation(sources)
    payload = _payload(
        report_type="external_validation_owner_report",
        title="External Validation Owner Report",
        status="EXTERNAL_VALIDATION_OWNER_REPORT_READY",
        summary={
            "owner_recommendation": recommendation,
            "static_status": sources["static"].get("status"),
            "replay_status": sources["replay"].get("status"),
            **_safety_summary(),
        },
        owner_recommendation=recommendation,
        required_answers=answers,
        source_statuses={key: value.get("status") for key, value in sources.items()},
        source_artifacts=_artifact_paths_by_report(sources),
        report_registry_entry=_doc_report_registry_entry(
            "external_validation_owner_report",
            "External Validation Owner Report",
            "aits research strategies external-validation-owner-report",
            "external_validation_owner_report",
            "docs/research/external_validation_owner_report.md",
        ),
    )
    _write_json_and_doc(
        payload,
        output_root / "external_validation_owner_report.json",
        docs_path,
        "External Validation Owner Report",
    )
    return payload


def run_external_validation_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_EXTERNAL_VALIDATION_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Path = DEFAULT_EXTERNAL_VALIDATION_OWNER_REPORT_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _owner_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    owner = dict(
        _owner_payload
        or run_external_validation_owner_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            docs_path=owner_docs_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    recommendation = str(owner.get("owner_recommendation"))
    if recommendation == "EXTERNAL_VALIDATION_ACCEPTED":
        status = "EXTERNAL_VALIDATION_PASS"
    elif recommendation == "EXTERNAL_VALIDATION_ACCEPTED_WITH_WARNINGS":
        status = "EXTERNAL_VALIDATION_PASS_WITH_WARNINGS"
    elif recommendation == "INTERNAL_BACKTEST_FIX_REQUIRED":
        status = "EXTERNAL_VALIDATION_BLOCKED"
    else:
        status = "EXTERNAL_VALIDATION_NEEDS_MORE_WORK"
    answers = _master_required_answers(owner)
    payload = _payload(
        report_type="external_validation_master_review",
        title="External Validation Master Review",
        status=status,
        summary={
            "final_status": status,
            "owner_recommendation": recommendation,
            **_safety_summary(),
        },
        final_status=status,
        required_answers=answers,
        final_conclusions=[
            status,
            "KEEP_DUAL_FORWARD_AGING_RESEARCH_ONLY",
            "NO_PAPER_SHADOW_NO_PRODUCTION_NO_BROKER",
        ],
        source_statuses={"external_validation_owner_report": owner.get("status")},
        source_artifacts=_artifact_paths_by_report({"external_validation_owner_report": owner}),
        report_registry_entry=_doc_report_registry_entry(
            "external_validation_master_review",
            "External Validation Master Review",
            "aits research strategies external-validation-master-review",
            "external_validation_master_review",
            "docs/research/external_validation_master_review.md",
        ),
    )
    _write_json_and_doc(
        payload,
        output_root / "external_validation_master_review.json",
        docs_path,
        "External Validation Master Review",
    )
    return payload


def run_external_validation_reader_brief_safe_preview(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _master_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    master = dict(
        _master_payload
        or run_external_validation_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    preview = {
        "external_validation_status": master.get("status"),
        "static_baseline_reconciliation_status": _mapping(
            master.get("required_answers")
        ).get("2_static_baseline_passed"),
        "dynamic_replay_reconciliation_status": _mapping(master.get("required_answers")).get(
            "3_dynamic_weight_path_replay_passed"
        ),
        "SGOV_total_return_warning_if_any": _mapping(master.get("required_answers")).get(
            "5_SGOV_total_return_proxy_acceptable"
        )
        is not True,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    hits = _prohibited_reader_brief_hits(preview)
    status = (
        "EXTERNAL_VALIDATION_READER_PREVIEW_SAFE"
        if not hits and master.get("status") != "EXTERNAL_VALIDATION_BLOCKED"
        else (
            "EXTERNAL_VALIDATION_READER_PREVIEW_AMBIGUOUS"
            if hits
            else "EXTERNAL_VALIDATION_READER_PREVIEW_BLOCKED"
        )
    )
    payload = _payload(
        report_type="external_validation_reader_brief_safe_preview",
        title="External Validation Reader Brief Safe Preview",
        status=status,
        summary={
            "external_validation_status": master.get("status"),
            "prohibited_phrase_hit_count": len(hits),
            **_safety_summary(),
        },
        reader_brief_preview=preview,
        prohibited_phrase_hits=hits,
        source_statuses={"external_validation_master_review": master.get("status")},
        source_artifacts=_artifact_paths_by_report({"external_validation_master_review": master}),
        report_registry_entry=_report_registry_entry(
            "external_validation_reader_brief_safe_preview",
            "External Validation Reader Brief Safe Preview",
            "aits research strategies external-validation-reader-brief-safe-preview",
            "external_validation_reader_brief_safe_preview",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_external_validation_real_result_status_reader(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _scope_payload: Mapping[str, Any] | None = None,
    _static_payload: Mapping[str, Any] | None = None,
    _weight_export_payload: Mapping[str, Any] | None = None,
    _replay_payload: Mapping[str, Any] | None = None,
    _metric_payload: Mapping[str, Any] | None = None,
    _sgov_payload: Mapping[str, Any] | None = None,
    _difference_payload: Mapping[str, Any] | None = None,
    _owner_payload: Mapping[str, Any] | None = None,
    _master_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sources = _real_result_sources(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        growth_config_path=growth_config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
        scope_payload=_scope_payload,
        static_payload=_static_payload,
        weight_export_payload=_weight_export_payload,
        replay_payload=_replay_payload,
        metric_payload=_metric_payload,
        sgov_payload=_sgov_payload,
        difference_payload=_difference_payload,
        owner_payload=_owner_payload,
        master_payload=_master_payload,
    )
    blocking_reasons = _external_status_blockers(sources)
    warning_reasons = _external_status_warnings(sources)
    master_status = str(sources["master"].get("status") or "")
    if _missing_sources(sources):
        status = "EXTERNAL_VALIDATION_RESULT_MISSING"
    elif blocking_reasons:
        status = "EXTERNAL_VALIDATION_RESULT_BLOCKED"
    elif master_status == "EXTERNAL_VALIDATION_PASS" and not warning_reasons:
        status = "EXTERNAL_VALIDATION_RESULT_READY"
    else:
        status = "EXTERNAL_VALIDATION_RESULT_WARN"
    payload = _payload(
        report_type="external_validation_real_result_status_reader",
        title="External Validation Real Result Status Reader",
        status=status,
        summary={
            "external_validation_master_status": sources["master"].get("status"),
            "static_baseline_status": sources["static"].get("status"),
            "dynamic_replay_status": sources["replay"].get("status"),
            "owner_recommendation": sources["owner"].get("owner_recommendation"),
            "blocking_reason_count": len(blocking_reasons),
            "warning_reason_count": len(warning_reasons),
            **_safety_summary(),
        },
        external_validation_master_status=sources["master"].get("status"),
        static_baseline_status=sources["static"].get("status"),
        dynamic_replay_status=sources["replay"].get("status"),
        metric_reconciliation_status=sources["metric"].get("status"),
        sgov_reconciliation_status=sources["sgov"].get("status"),
        difference_attribution_status=sources["difference"].get("status"),
        owner_recommendation=sources["owner"].get("owner_recommendation"),
        blocking_reasons=blocking_reasons,
        warning_reasons=warning_reasons,
        source_statuses={key: value.get("status") for key, value in sources.items()},
        source_artifacts=_artifact_paths_by_report(sources),
        report_registry_entry=_report_registry_entry(
            "external_validation_real_result_status_reader",
            "External Validation Real Result Status Reader",
            "aits research strategies external-validation-real-result-status-reader",
            "external_validation_real_result_status_reader",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_static_baseline_reconciliation_final_check(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    external_records_path: Path | None = None,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _static_payload: Mapping[str, Any] | None = None,
    _sgov_payload: Mapping[str, Any] | None = None,
    _metric_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    static = dict(
        _static_payload
        or run_static_baseline_external_reconciliation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            output_root=output_root,
            external_records_path=external_records_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    sgov = dict(
        _sgov_payload
        or run_sgov_total_return_external_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    metric = dict(_metric_payload or run_metric_definition_reconciliation(output_root=output_root))
    rows = _records(static.get("reconciliation_rows"))
    blockers = list(_records_to_text(static.get("blocking_reasons")))
    missing_external = [
        row["strategy_id"] for row in rows if not row.get("external_record_present")
    ]
    mismatches = [row for row in rows if row.get("within_tolerance") is False]
    warnings = []
    if missing_external:
        warnings.append("manual_external_records_missing")
    if str(sgov.get("status")) == "SGOV_PRICE_ONLY_DIFFERENCE_WARN":
        warnings.append("SGOV_adjusted_close_vs_price_only_difference_disclosed")
    if "WARNING" in str(metric.get("status")).upper():
        warnings.append("metric_definition_requires_manual_external_tool_confirmation")
    if "BLOCKED" in str(static.get("status")).upper() or blockers:
        status = "STATIC_BASELINE_FINAL_BLOCKED"
    elif mismatches:
        status = "STATIC_BASELINE_FINAL_MISMATCH"
    elif warnings or "WARNING" in str(static.get("status")).upper():
        status = "STATIC_BASELINE_FINAL_WARN"
    else:
        status = "STATIC_BASELINE_FINAL_RECONCILED"
    payload = _payload(
        report_type="static_baseline_reconciliation_final_check",
        title="Static Baseline Reconciliation Final Check",
        status=status,
        summary={
            "strategy_count": len(rows),
            "missing_external_record_count": len(missing_external),
            "mismatch_count": len(mismatches),
            "SGOV_status": sgov.get("status"),
            **_safety_summary(),
        },
        checked_strategy_ids=list(STATIC_BASELINE_IDS),
        final_check_rows=rows,
        required_checks={
            "external_or_manual_record_present": not missing_external,
            "annual_return_within_tolerance": _all_metric_within_static(rows, "annual_return"),
            "max_drawdown_within_tolerance": _all_metric_within_static(rows, "max_drawdown"),
            "sharpe_within_tolerance": _all_metric_within_static(rows, "sharpe"),
            "calmar_within_tolerance": _all_metric_within_static(rows, "calmar"),
            "SGOV_dividend_adjusted_close_effect_disclosed": sgov.get("status")
            in {"SGOV_TOTAL_RETURN_RECONCILED", "SGOV_PRICE_ONLY_DIFFERENCE_WARN"},
            "internal_metric_fix_required": status == "STATIC_BASELINE_FINAL_MISMATCH",
        },
        source_statuses={
            "static_baseline_external_reconciliation": static.get("status"),
            "metric_definition_reconciliation": metric.get("status"),
            "sgov_total_return_external_check": sgov.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "static_baseline_external_reconciliation": static,
                "metric_definition_reconciliation": metric,
                "sgov_total_return_external_check": sgov,
            }
        ),
        blocking_reasons=blockers,
        warning_reasons=_dedupe_text(warnings),
        report_registry_entry=_report_registry_entry(
            "static_baseline_reconciliation_final_check",
            "Static Baseline Reconciliation Final Check",
            "aits research strategies static-baseline-reconciliation-final-check",
            "static_baseline_reconciliation_final_check",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dynamic_weight_path_replay_final_check(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _weight_export_payload: Mapping[str, Any] | None = None,
    _replay_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    weight_export = dict(
        _weight_export_payload
        or run_strategy_weight_path_export(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    replay = dict(
        _replay_payload
        or run_external_independent_return_replay(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _weight_export_payload=weight_export,
        )
    )
    weight_health = {
        strategy_id: _weight_path_health(Path(str(path_text)))
        for strategy_id, path_text in _mapping(weight_export.get("exported_weight_paths")).items()
    }
    definition_hashes = _mapping(weight_export.get("definition_hashes"))
    rows = [
        _dynamic_final_check_row(
            row,
            weight_health=weight_health.get(str(row.get("strategy_id")), {}),
            definition_hash=str(definition_hashes.get(str(row.get("strategy_id"))) or ""),
        )
        for row in _records(replay.get("replay_rows"))
    ]
    blockers = list(_records_to_text(weight_export.get("blocking_reasons")))
    blockers.extend(_records_to_text(replay.get("blocking_reasons")))
    if str(weight_export.get("status")) == "WEIGHT_PATH_EXPORT_BLOCKED":
        blockers.append("weight_path_export_blocked")
    if str(replay.get("status")) == "INDEPENDENT_REPLAY_BLOCKED":
        blockers.append("independent_replay_blocked")
    mismatches = [row for row in rows if row.get("within_tolerance") is False]
    warnings = []
    if str(weight_export.get("status")) == "WEIGHT_PATH_EXPORT_WARN":
        warnings.append("weight_path_export_passed_with_data_quality_warnings")
    if str(replay.get("status")) == "INDEPENDENT_REPLAY_MATCHED_WITH_WARNINGS":
        warnings.append("independent_replay_matched_with_data_quality_warnings")
    if blockers:
        status = "DYNAMIC_REPLAY_FINAL_BLOCKED"
    elif mismatches:
        status = "DYNAMIC_REPLAY_FINAL_MISMATCH"
    elif warnings:
        status = "DYNAMIC_REPLAY_FINAL_WARN"
    else:
        status = "DYNAMIC_REPLAY_FINAL_MATCHED"
    payload = _payload(
        report_type="dynamic_weight_path_replay_final_check",
        title="Dynamic Weight Path Replay Final Check",
        status=status,
        summary={
            "strategy_count": len(rows),
            "mismatch_count": len(mismatches),
            "warning_count": len(warnings),
            **_safety_summary(),
        },
        checked_strategy_ids=list(DYNAMIC_REPLAY_IDS),
        replay_final_rows=rows,
        weight_path_health=weight_health,
        pandas_index_alignment_risk="mitigated_by_explicit_date_index_and_numpy_weight_columns",
        signal_execution_alignment="signal_close_weights_apply_to_next_daily_return",
        source_statuses={
            "strategy_weight_path_export": weight_export.get("status"),
            "external_independent_return_replay": replay.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "strategy_weight_path_export": weight_export,
                "external_independent_return_replay": replay,
            }
        ),
        blocking_reasons=_dedupe_text(blockers),
        warning_reasons=_dedupe_text(warnings),
        report_registry_entry=_report_registry_entry(
            "dynamic_weight_path_replay_final_check",
            "Dynamic Weight Path Replay Final Check",
            "aits research strategies dynamic-weight-path-replay-final-check",
            "dynamic_weight_path_replay_final_check",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_metric_and_sgov_reconciliation_signoff(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _metric_payload: Mapping[str, Any] | None = None,
    _sgov_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    metric = dict(_metric_payload or run_metric_definition_reconciliation(output_root=output_root))
    sgov = dict(
        _sgov_payload
        or run_sgov_total_return_external_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    blockers = []
    if "BLOCKED" in str(metric.get("status")).upper():
        blockers.append("metric_definition_reconciliation_blocked")
    if "BLOCKED" in str(sgov.get("status")).upper():
        blockers.append("sgov_total_return_check_blocked")
    warnings = []
    if "WARNING" in str(metric.get("status")).upper():
        warnings.append("external_metric_definitions_require_manual_tool_confirmation")
    if str(sgov.get("status")) == "SGOV_PRICE_ONLY_DIFFERENCE_WARN":
        warnings.append("SGOV_adjusted_close_total_return_proxy_differs_from_price_only")
    if blockers:
        status = "METRIC_SGOV_SIGNOFF_BLOCKED"
    elif warnings:
        status = "METRIC_SGOV_SIGNOFF_WARN"
    else:
        status = "METRIC_SGOV_SIGNOFF_READY"
    payload = _payload(
        report_type="metric_sgov_reconciliation_signoff",
        title="Metric And SGOV Reconciliation Signoff",
        status=status,
        summary={
            "metric_status": metric.get("status"),
            "SGOV_status": sgov.get("status"),
            "warning_count": len(warnings),
            **_safety_summary(),
        },
        signoff_checks={
            "CAGR_vs_arithmetic_annualized_return_explicit": True,
            "max_drawdown_definition_explicit": True,
            "sharpe_risk_free_rate_recorded": True,
            "calmar_definition_explicit": True,
            "SGOV_adjusted_close_used_as_total_return_proxy": True,
            "price_only_vs_adjusted_close_difference_disclosed": bool(
                _mapping(sgov.get("sgov_total_return_summary"))
            ),
            "SGOV_impact_explained_for_static_and_dynamic_strategies": bool(
                _records(sgov.get("impact_rows"))
            ),
        },
        source_statuses={
            "metric_definition_reconciliation": metric.get("status"),
            "sgov_total_return_external_check": sgov.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "metric_definition_reconciliation": metric,
                "sgov_total_return_external_check": sgov,
            }
        ),
        blocking_reasons=blockers,
        warning_reasons=_dedupe_text(warnings),
        report_registry_entry=_report_registry_entry(
            "metric_and_sgov_reconciliation_signoff",
            "Metric And SGOV Reconciliation Signoff",
            "aits research strategies metric-and-sgov-reconciliation-signoff",
            "metric_sgov_reconciliation_signoff",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_external_validation_to_launch_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _status_reader_payload: Mapping[str, Any] | None = None,
    _static_final_payload: Mapping[str, Any] | None = None,
    _dynamic_final_payload: Mapping[str, Any] | None = None,
    _metric_sgov_payload: Mapping[str, Any] | None = None,
    _definition_lock_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    status_reader = dict(
        _status_reader_payload
        or run_external_validation_real_result_status_reader(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    static_final = dict(
        _static_final_payload
        or run_static_baseline_reconciliation_final_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    dynamic_final = dict(
        _dynamic_final_payload
        or run_dynamic_weight_path_replay_final_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    metric_sgov = dict(
        _metric_sgov_payload
        or run_metric_and_sgov_reconciliation_signoff(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    definition_lock = dict(
        _definition_lock_payload
        or run_balanced_core_definition_lock(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=growth_config_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    blocking_reasons = []
    warning_reasons = []
    master_status = str(status_reader.get("external_validation_master_status") or "")
    if master_status not in {"EXTERNAL_VALIDATION_PASS", "EXTERNAL_VALIDATION_PASS_WITH_WARNINGS"}:
        blocking_reasons.append("external_validation_master_not_passed")
    if static_final.get("status") == "STATIC_BASELINE_FINAL_BLOCKED":
        blocking_reasons.append("static_baseline_final_check_blocked")
    if dynamic_final.get("status") in {
        "DYNAMIC_REPLAY_FINAL_BLOCKED",
        "DYNAMIC_REPLAY_FINAL_MISMATCH",
    }:
        blocking_reasons.append("dynamic_replay_final_check_blocked_or_mismatched")
    if metric_sgov.get("status") == "METRIC_SGOV_SIGNOFF_BLOCKED":
        blocking_reasons.append("metric_sgov_signoff_blocked")
    if status_reader.get("owner_recommendation") == "INTERNAL_BACKTEST_FIX_REQUIRED":
        blocking_reasons.append("difference_attribution_internal_backtest_fix_required")
    if not str(definition_lock.get("definition_hash") or ""):
        blocking_reasons.append("balanced_core_definition_hash_missing")
    if definition_lock.get("status") != "BALANCED_CORE_DEFINITION_LOCKED":
        blocking_reasons.append("balanced_core_definition_not_locked")
    safety_sources = [status_reader, static_final, dynamic_final, metric_sgov, definition_lock]
    safety_violations = _external_safety_violations(safety_sources)
    blocking_reasons.extend(safety_violations)
    for source in (status_reader, static_final, dynamic_final, metric_sgov):
        warning_reasons.extend(_records_to_text(source.get("warning_reasons")))
        if str(source.get("status")).endswith("_WARN") or "WARNING" in str(source.get("status")):
            warning_reasons.append(f"{source.get('report_type')}_warning")
    launch_allowed = not blocking_reasons
    if not launch_allowed:
        status = "EXTERNAL_VALIDATION_LAUNCH_GATE_BLOCKED"
    elif _dedupe_text(warning_reasons) or master_status == "EXTERNAL_VALIDATION_PASS_WITH_WARNINGS":
        status = "EXTERNAL_VALIDATION_LAUNCH_GATE_WARN"
    else:
        status = "EXTERNAL_VALIDATION_LAUNCH_GATE_PASS"
    payload = _payload(
        report_type="external_validation_to_launch_gate",
        title="External Validation To Launch Gate",
        status=status,
        summary={
            "launch_allowed": launch_allowed,
            "launch_scope": "balanced_core_research_only_forward_aging",
            "external_validation_master_status": master_status,
            "candidate_definition_hash": definition_lock.get("definition_hash"),
            "blocking_reason_count": len(_dedupe_text(blocking_reasons)),
            **_safety_summary(),
        },
        launch_allowed=launch_allowed,
        launch_scope="balanced_core_research_only_forward_aging",
        required_owner_review=True,
        candidate_strategy_id=FOCUSED_GROWTH_TILT_CANDIDATE_ID,
        candidate_definition_hash=definition_lock.get("definition_hash"),
        source_statuses={
            "external_validation_real_result_status_reader": status_reader.get("status"),
            "static_baseline_reconciliation_final_check": static_final.get("status"),
            "dynamic_weight_path_replay_final_check": dynamic_final.get("status"),
            "metric_sgov_reconciliation_signoff": metric_sgov.get("status"),
            "balanced_core_definition_lock": definition_lock.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "external_validation_real_result_status_reader": status_reader,
                "static_baseline_reconciliation_final_check": static_final,
                "dynamic_weight_path_replay_final_check": dynamic_final,
                "metric_sgov_reconciliation_signoff": metric_sgov,
                "balanced_core_definition_lock": definition_lock,
            }
        ),
        blocking_reasons=_dedupe_text(blocking_reasons),
        warning_reasons=_dedupe_text(warning_reasons),
        report_registry_entry=_report_registry_entry(
            "external_validation_to_launch_gate",
            "External Validation To Launch Gate",
            "aits research strategies external-validation-to-launch-gate",
            "external_validation_to_launch_gate",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def _real_result_sources(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    simple_config_path: Path,
    growth_config_path: Path,
    output_root: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
    scope_payload: Mapping[str, Any] | None,
    static_payload: Mapping[str, Any] | None,
    weight_export_payload: Mapping[str, Any] | None,
    replay_payload: Mapping[str, Any] | None,
    metric_payload: Mapping[str, Any] | None,
    sgov_payload: Mapping[str, Any] | None,
    difference_payload: Mapping[str, Any] | None,
    owner_payload: Mapping[str, Any] | None,
    master_payload: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    scope = dict(
        scope_payload
        or _read_json_or_empty(output_root / "external_validation_scope_contract.json")
        or run_external_validation_scope_contract(
            output_root=output_root,
            start_date=start_date,
            end_date=end_date,
        )
    )
    static = dict(
        static_payload
        or _read_json_or_empty(output_root / "static_baseline_external_reconciliation.json")
        or run_static_baseline_external_reconciliation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    weight_export = dict(
        weight_export_payload
        or _read_json_or_empty(output_root / "strategy_weight_path_export.json")
        or run_strategy_weight_path_export(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    replay = dict(
        replay_payload
        or _read_json_or_empty(output_root / "external_independent_return_replay.json")
        or run_external_independent_return_replay(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _weight_export_payload=weight_export,
        )
    )
    metric = dict(
        metric_payload
        or _read_json_or_empty(output_root / "metric_definition_reconciliation.json")
        or run_metric_definition_reconciliation(output_root=output_root)
    )
    sgov = dict(
        sgov_payload
        or _read_json_or_empty(output_root / "sgov_total_return_external_check.json")
        or run_sgov_total_return_external_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    difference = dict(
        difference_payload
        or _read_json_or_empty(output_root / "external_validation_difference_attribution.json")
        or run_external_validation_difference_attribution(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _static_reconciliation_payload=static,
            _replay_payload=replay,
            _metric_payload=metric,
            _sgov_payload=sgov,
        )
    )
    owner = dict(
        owner_payload
        or _read_json_or_empty(output_root / "external_validation_owner_report.json")
        or run_external_validation_owner_report(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _scope_payload=scope,
            _static_payload=static,
            _replay_payload=replay,
            _metric_payload=metric,
            _sgov_payload=sgov,
            _difference_payload=difference,
        )
    )
    master = dict(
        master_payload
        or _read_json_or_empty(output_root / "external_validation_master_review.json")
        or run_external_validation_master_review(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _owner_payload=owner,
        )
    )
    return {
        "scope": scope,
        "static": static,
        "weight_export": weight_export,
        "replay": replay,
        "metric": metric,
        "sgov": sgov,
        "difference": difference,
        "owner": owner,
        "master": master,
    }


def _missing_sources(sources: Mapping[str, Mapping[str, Any]]) -> list[str]:
    return [key for key, value in sources.items() if not value.get("status")]


def _external_status_blockers(sources: Mapping[str, Mapping[str, Any]]) -> list[str]:
    blockers = []
    for key, source in sources.items():
        status = str(source.get("status") or "")
        if "BLOCKED" in status or "MISMATCH" in status or "FIX_REQUIRED" in status:
            blockers.append(f"{key}:{status}")
    if sources["owner"].get("owner_recommendation") == "INTERNAL_BACKTEST_FIX_REQUIRED":
        blockers.append("owner_recommendation_internal_backtest_fix_required")
    blockers.extend(_external_safety_violations(sources.values()))
    return _dedupe_text(blockers)


def _external_status_warnings(sources: Mapping[str, Mapping[str, Any]]) -> list[str]:
    warnings = []
    for key, source in sources.items():
        status = str(source.get("status") or "")
        if "WARN" in status or "WARNING" in status or "PENDING" in status:
            warnings.append(f"{key}:{status}")
        warnings.extend(_records_to_text(source.get("warning_reasons")))
    return _dedupe_text(warnings)


def _all_metric_within_static(rows: list[dict[str, Any]], metric: str) -> bool:
    for row in rows:
        if row.get("within_tolerance") is None:
            return False
        deltas = _mapping(row.get("metric_delta"))
        if metric not in deltas:
            return False
        if row.get("within_tolerance") is not True:
            return False
    return bool(rows)


def _weight_path_health(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "path": str(path),
            "exists": False,
            "row_count": 0,
            "first_date": None,
            "last_date": None,
            "nonzero_weight_rows": 0,
            "weight_sum_min": None,
            "weight_sum_max": None,
            "reasonable_weight_sum": False,
        }
    frame = pd.read_csv(path)
    weight_cols = ["target_weight_qqq", "target_weight_tqqq", "target_weight_sgov"]
    weights = frame.reindex(columns=weight_cols, fill_value=0.0)
    weight_sum = weights.sum(axis=1)
    nonzero = weight_sum.abs() > 1e-9
    return {
        "path": str(path),
        "exists": True,
        "row_count": len(frame.index),
        "first_date": str(frame["date"].iloc[0]) if "date" in frame and not frame.empty else None,
        "last_date": str(frame["date"].iloc[-1]) if "date" in frame and not frame.empty else None,
        "nonzero_weight_rows": int(nonzero.sum()),
        "weight_sum_min": _round(weight_sum.min()) if len(weight_sum.index) else None,
        "weight_sum_max": _round(weight_sum.max()) if len(weight_sum.index) else None,
        "reasonable_weight_sum": bool(
            len(weight_sum.index) and weight_sum.between(0.99, 1.01).all()
        ),
    }


def _dynamic_final_check_row(
    row: Mapping[str, Any],
    *,
    weight_health: Mapping[str, Any],
    definition_hash: str,
) -> dict[str, Any]:
    annual = _metric_reconciliation(row, "annual_return")
    drawdown = _metric_reconciliation(row, "max_drawdown")
    sharpe = _metric_reconciliation(row, "sharpe")
    calmar = _metric_reconciliation(row, "calmar")
    metric_rows = _records(row.get("metric_reconciliation"))
    metrics_within = bool(row.get("all_metrics_within_tolerance"))
    weight_ok = (
        bool(weight_health.get("exists"))
        and _int(weight_health.get("row_count")) > 0
        and _int(weight_health.get("nonzero_weight_rows")) > 0
        and bool(weight_health.get("reasonable_weight_sum"))
    )
    within_tolerance = metrics_within and weight_ok and bool(definition_hash)
    difference_reason = "within_tolerance"
    if not definition_hash:
        difference_reason = "definition_hash_missing"
    elif not weight_ok:
        difference_reason = "weight_path_missing_zero_or_unreasonable_sum"
    else:
        for metric in metric_rows:
            if metric.get("within_tolerance") is not True:
                difference_reason = str(metric.get("difference_reason") or "metric_mismatch")
                break
    return {
        "strategy_id": row.get("strategy_id"),
        "weight_path_rows": weight_health.get("row_count", 0),
        "first_date": weight_health.get("first_date"),
        "last_date": weight_health.get("last_date"),
        "definition_hash": definition_hash,
        "internal_annual_return": annual.get("internal_metric"),
        "replay_annual_return": annual.get("replay_metric"),
        "annual_return_delta": annual.get("metric_delta"),
        "internal_max_drawdown": drawdown.get("internal_metric"),
        "replay_max_drawdown": drawdown.get("replay_metric"),
        "max_drawdown_delta": drawdown.get("metric_delta"),
        "internal_sharpe": sharpe.get("internal_metric"),
        "replay_sharpe": sharpe.get("replay_metric"),
        "sharpe_delta": sharpe.get("metric_delta"),
        "internal_calmar": calmar.get("internal_metric"),
        "replay_calmar": calmar.get("replay_metric"),
        "calmar_delta": calmar.get("metric_delta"),
        "within_tolerance": within_tolerance,
        "difference_reason": difference_reason,
    }


def _metric_reconciliation(row: Mapping[str, Any], metric_name: str) -> dict[str, Any]:
    for metric in _records(row.get("metric_reconciliation")):
        if metric.get("metric") == metric_name:
            return metric
    return {}


def _external_safety_violations(payloads: Any) -> list[str]:
    violations = []
    for index, payload in enumerate(payloads):
        if not isinstance(payload, Mapping):
            continue
        label = str(payload.get("report_type") or index)
        if payload.get("paper_shadow_allowed") is not False:
            violations.append(f"{label}:paper_shadow_allowed_not_false")
        if payload.get("production_allowed") is not False:
            violations.append(f"{label}:production_allowed_not_false")
        if payload.get("broker_action") != "none":
            violations.append(f"{label}:broker_action_not_none")
        if payload.get("manual_review_required") is not True:
            violations.append(f"{label}:manual_review_required_not_true")
        if payload.get("production_effect") != "none":
            violations.append(f"{label}:production_effect_not_none")
    return _dedupe_text(violations)


def _internal_static_metric_rows(
    config: Mapping[str, Any],
    prices: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows = []
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    annualization = _research_policy_int(config, "annualization_trading_days")
    for strategy_id in STATIC_BASELINE_IDS:
        strategy = _strategy_by_id(config, strategy_id)
        returns = _strategy_return_series(strategy, prices, config, cost_bps=0.0)
        weights = _target_weight_frame(strategy, prices, config).reindex(returns.index).ffill()
        rows.append(
            _metrics_for_strategy(
                strategy,
                returns,
                weights,
                qqq_returns,
                annualization=annualization,
                cost_bps=0.0,
            )
        )
    return rows


def _static_reconciliation_row(
    internal: Mapping[str, Any],
    external: Mapping[str, Any] | None,
) -> dict[str, Any]:
    strategy_id = str(internal.get("strategy_id"))
    if not external:
        return {
            "external_tool": None,
            "strategy_id": strategy_id,
            "date_range": internal.get("requested_date_range"),
            "rebalance_frequency": "monthly",
            "external_annual_return": None,
            "external_max_drawdown": None,
            "external_sharpe": None,
            "external_calmar": None,
            "external_monthly_returns_if_available": None,
            "manual_input_notes": "manual external platform record not supplied",
            "screenshot_or_export_reference": None,
            "internal_annual_return": internal.get("annual_return"),
            "internal_max_drawdown": internal.get("max_drawdown"),
            "internal_sharpe": internal.get("sharpe"),
            "internal_calmar": internal.get("calmar"),
            "metric_delta": {},
            "within_tolerance": None,
            "external_record_present": False,
            "difference_reason": "manual_external_input_pending",
        }
    deltas = {
        metric: _round(_float(internal.get(metric)) - _float(external.get(f"external_{metric}")))
        for metric in ("annual_return", "max_drawdown", "sharpe", "calmar")
    }
    within = (
        abs(deltas["annual_return"]) <= METRIC_TOLERANCE["annual_return"]
        and abs(deltas["max_drawdown"]) <= METRIC_TOLERANCE["max_drawdown"]
        and abs(deltas["sharpe"]) <= METRIC_TOLERANCE["sharpe"]
        and abs(deltas["calmar"]) <= METRIC_TOLERANCE["calmar"]
    )
    return {
        "external_tool": external.get("external_tool"),
        "strategy_id": strategy_id,
        "date_range": external.get("date_range", internal.get("requested_date_range")),
        "rebalance_frequency": external.get("rebalance_frequency", "monthly"),
        "external_annual_return": external.get("external_annual_return"),
        "external_max_drawdown": external.get("external_max_drawdown"),
        "external_sharpe": external.get("external_sharpe"),
        "external_calmar": external.get("external_calmar"),
        "external_monthly_returns_if_available": external.get(
            "external_monthly_returns_if_available"
        ),
        "manual_input_notes": external.get("manual_input_notes"),
        "screenshot_or_export_reference": external.get("screenshot_or_export_reference"),
        "internal_annual_return": internal.get("annual_return"),
        "internal_max_drawdown": internal.get("max_drawdown"),
        "internal_sharpe": internal.get("sharpe"),
        "internal_calmar": internal.get("calmar"),
        "metric_delta": deltas,
        "within_tolerance": within,
        "external_record_present": True,
        "difference_reason": "within_tolerance" if within else "metric_delta_exceeds_tolerance",
    }


def _weights_for_strategy(
    strategy_id: str,
    *,
    prices: pd.DataFrame,
    simple_config: Mapping[str, Any],
    growth_config_path: Path,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> tuple[pd.DataFrame, str]:
    if strategy_id == "equal_risk_qqq_sgov":
        strategy = _strategy_by_id(simple_config, strategy_id)
        return _target_weight_frame(strategy, prices, simple_config), _stable_hash(strategy)
    if strategy_id == FOCUSED_GROWTH_TILT_CANDIDATE_ID:
        lock = run_balanced_core_definition_lock(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=growth_config_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
        candidate = _balanced_core_candidate_from_lock(lock)
        return (
            _growth_weight_frame(candidate, prices, _load_growth_config(growth_config_path)),
            str(lock.get("definition_hash")),
        )
    raise ValueError(f"unsupported external validation strategy id: {strategy_id}")


def _weight_path_frame(
    strategy_id: str,
    weights: pd.DataFrame,
    definition_hash: str,
    data_quality_status: str,
) -> pd.DataFrame:
    frame = weights.reindex(columns=["QQQ", "TQQQ", "SGOV"], fill_value=0.0).copy()
    turnover = _turnover_series(frame.ffill().fillna(0.0))
    return pd.DataFrame(
        {
            "date": [idx.date().isoformat() for idx in pd.to_datetime(frame.index)],
            "strategy_id": strategy_id,
            "definition_hash": definition_hash,
            "target_weight_qqq": frame["QQQ"].round(8),
            "target_weight_tqqq": frame["TQQQ"].round(8),
            "target_weight_sgov": frame["SGOV"].round(8),
            "rebalance_flag": (turnover > 1e-12).astype(bool).to_list(),
            "signal_time": "close",
            "execution_assumption": "next_return_after_target_weight",
            "data_quality_status": data_quality_status,
        }
    )


def _read_weight_path(path: Path, index: pd.Index) -> pd.DataFrame:
    frame = pd.read_csv(path, parse_dates=["date"])
    weights = pd.DataFrame(
        {
            "QQQ": _weight_column_values(frame, "target_weight_qqq"),
            "TQQQ": _weight_column_values(frame, "target_weight_tqqq"),
            "SGOV": _weight_column_values(frame, "target_weight_sgov"),
        },
        index=pd.to_datetime(frame["date"]),
    )
    return weights.reindex(pd.to_datetime(index)).ffill().fillna(0.0)


def _weight_column_values(frame: pd.DataFrame, column: str) -> Any:
    if column not in frame:
        return 0.0
    return frame[column].to_numpy()


def _replay_returns_from_weights(weights: pd.DataFrame, prices: pd.DataFrame) -> pd.Series:
    asset_returns = prices.pct_change().fillna(0.0)
    applied = weights.shift(1).ffill().reindex(asset_returns.index).fillna(0.0)
    return (applied * asset_returns.reindex(columns=applied.columns).fillna(0.0)).sum(axis=1)


def _internal_dynamic_metrics(
    strategy_id: str,
    *,
    prices: pd.DataFrame,
    simple_config: Mapping[str, Any],
    growth_config_path: Path,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> dict[str, Any]:
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    weights, _definition_hash = _weights_for_strategy(
        strategy_id,
        prices=prices,
        simple_config=simple_config,
        growth_config_path=growth_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    returns = _replay_returns_from_weights(weights, prices)
    return _metrics_for_strategy(
        {"strategy_id": strategy_id, "display_name": strategy_id},
        returns,
        weights,
        qqq_returns,
        annualization=_research_policy_int(simple_config, "annualization_trading_days"),
        cost_bps=0.0,
    )


def _replay_reconciliation_row(
    strategy_id: str,
    replay: Mapping[str, Any],
    internal: Mapping[str, Any],
) -> dict[str, Any]:
    metric_rows = []
    for metric in ("annual_return", "max_drawdown", "sharpe", "calmar", "turnover"):
        tolerance = METRIC_TOLERANCE.get(metric, 1e-9)
        if metric == "turnover":
            tolerance = 1e-9
        delta = _round(_float(replay.get(metric)) - _float(internal.get(metric)))
        metric_rows.append(
            {
                "metric": metric,
                "internal_metric": internal.get(metric),
                "replay_metric": replay.get(metric),
                "metric_delta": delta,
                "within_tolerance": abs(delta) <= tolerance,
                "difference_reason": "within_tolerance"
                if abs(delta) <= tolerance
                else "independent_replay_delta_exceeds_tolerance",
            }
        )
    return {
        "strategy_id": strategy_id,
        "equity_curve": "not_embedded_large_series",
        "daily_returns": "not_embedded_large_series",
        "monthly_returns": "not_embedded_large_series",
        "annual_return": replay.get("annual_return"),
        "max_drawdown": replay.get("max_drawdown"),
        "sharpe": replay.get("sharpe"),
        "calmar": replay.get("calmar"),
        "turnover": replay.get("turnover"),
        "worst_month": replay.get("worst_month"),
        "worst_quarter": replay.get("worst_quarter"),
        "metric_reconciliation": metric_rows,
        "all_metrics_within_tolerance": all(row["within_tolerance"] for row in metric_rows),
    }


def _sgov_impact_row(
    strategy_id: str,
    *,
    prices: pd.DataFrame,
    raw_prices: pd.DataFrame,
    simple_config: Mapping[str, Any],
    simple_config_path: Path,
    growth_config_path: Path,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> dict[str, Any]:
    adjusted_prices = prices.copy()
    price_only = prices.copy()
    sgov = raw_prices.loc[raw_prices["ticker"].astype(str) == "SGOV"].sort_values("date")
    sgov_series = sgov.set_index("date")["close"].reindex(price_only.index).ffill()
    if "SGOV" in price_only:
        price_only["SGOV"] = sgov_series
    internal = _metric_for_any_strategy(
        strategy_id,
        adjusted_prices,
        simple_config=simple_config,
        growth_config_path=growth_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    price_only_metric = _metric_for_any_strategy(
        strategy_id,
        price_only,
        simple_config=simple_config,
        growth_config_path=growth_config_path,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    return {
        "strategy_id": strategy_id,
        "internal_annual_return": internal.get("annual_return"),
        "price_only_annual_return": price_only_metric.get("annual_return"),
        "annual_return_impact": _round(
            _float(internal.get("annual_return")) - _float(price_only_metric.get("annual_return"))
        ),
        "internal_max_drawdown": internal.get("max_drawdown"),
        "price_only_max_drawdown": price_only_metric.get("max_drawdown"),
        "max_drawdown_impact": _round(
            _float(internal.get("max_drawdown")) - _float(price_only_metric.get("max_drawdown"))
        ),
    }


def _metric_for_any_strategy(
    strategy_id: str,
    prices: pd.DataFrame,
    *,
    simple_config: Mapping[str, Any],
    growth_config_path: Path,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> dict[str, Any]:
    if strategy_id in {"qqq_50_sgov_50", "qqq_60_sgov_40", "100_qqq", "equal_risk_qqq_sgov"}:
        strategy = _strategy_by_id(simple_config, strategy_id)
        weights = _target_weight_frame(strategy, prices, simple_config)
    else:
        weights, _definition_hash = _weights_for_strategy(
            strategy_id,
            prices=prices,
            simple_config=simple_config,
            growth_config_path=growth_config_path,
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
        strategy = {"strategy_id": strategy_id, "display_name": strategy_id}
    returns = _replay_returns_from_weights(weights, prices)
    return _metrics_for_strategy(
        strategy,
        returns,
        weights,
        prices["QQQ"].pct_change().fillna(0.0),
        annualization=_research_policy_int(simple_config, "annualization_trading_days"),
        cost_bps=0.0,
    )


def _owner_sources(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    simple_config_path: Path,
    growth_config_path: Path,
    output_root: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
    scope_payload: Mapping[str, Any] | None,
    static_payload: Mapping[str, Any] | None,
    replay_payload: Mapping[str, Any] | None,
    metric_payload: Mapping[str, Any] | None,
    sgov_payload: Mapping[str, Any] | None,
    feasibility_payload: Mapping[str, Any] | None,
    difference_payload: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    scope = dict(scope_payload or run_external_validation_scope_contract(output_root=output_root))
    static = dict(
        static_payload
        or run_static_baseline_external_reconciliation(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    replay = dict(
        replay_payload
        or run_external_independent_return_replay(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    metric = dict(metric_payload or run_metric_definition_reconciliation(output_root=output_root))
    sgov = dict(
        sgov_payload
        or run_sgov_total_return_external_check(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    feasibility = dict(
        feasibility_payload or run_external_platform_feasibility_review(output_root=output_root)
    )
    difference = dict(
        difference_payload
        or run_external_validation_difference_attribution(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _static_reconciliation_payload=static,
            _replay_payload=replay,
            _metric_payload=metric,
            _sgov_payload=sgov,
        )
    )
    return {
        "scope": scope,
        "static": static,
        "replay": replay,
        "metric": metric,
        "sgov": sgov,
        "feasibility": feasibility,
        "difference": difference,
    }


def _owner_required_answers(sources: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    static_status = str(sources["static"].get("status"))
    replay_status = str(sources["replay"].get("status"))
    sgov_status = str(sources["sgov"].get("status"))
    metric_status = str(sources["metric"].get("status"))
    difference_status = str(sources["difference"].get("status"))
    return {
        "1_static_baseline_basically_matches_external": static_status
        in {"STATIC_BASELINE_RECONCILED", "STATIC_BASELINE_RECONCILED_WITH_WARNINGS"},
        "2_dynamic_weight_path_replay_matches_internal": replay_status
        in {"INDEPENDENT_REPLAY_MATCHED", "INDEPENDENT_REPLAY_MATCHED_WITH_WARNINGS"},
        "3_SGOV_total_return_explained": sgov_status
        in {"SGOV_TOTAL_RETURN_RECONCILED", "SGOV_PRICE_ONLY_DIFFERENCE_WARN"},
        "4_metric_definitions_aligned": metric_status
        in {
            "METRIC_DEFINITIONS_RECONCILED",
            "METRIC_DEFINITIONS_RECONCILED_WITH_WARNINGS",
        },
        "5_internal_calculation_fix_required": difference_status
        == "DIFFERENCE_REQUIRES_INTERNAL_FIX",
        "6_internal_results_allowed_as_research_basis": difference_status
        != "DIFFERENCE_REQUIRES_INTERNAL_FIX",
        "7_further_quantconnect_or_tradingview_needed": sources["feasibility"].get("status")
        in {
            "EXTERNAL_PLATFORM_DYNAMIC_REPLAY_NEEDED",
            "EXTERNAL_PLATFORM_PLAN_READY",
        },
        "8_continue_no_paper_shadow_production_broker": True,
    }


def _owner_recommendation(sources: Mapping[str, Mapping[str, Any]]) -> str:
    answers = _owner_required_answers(sources)
    if answers["5_internal_calculation_fix_required"]:
        return "INTERNAL_BACKTEST_FIX_REQUIRED"
    if sources["static"].get("status") == "STATIC_BASELINE_BLOCKED" or sources["replay"].get(
        "status"
    ) == "INDEPENDENT_REPLAY_BLOCKED":
        return "EXTERNAL_VALIDATION_BLOCKED"
    if sources["static"].get("status") == "STATIC_BASELINE_RECONCILED" and sources[
        "replay"
    ].get("status") == "INDEPENDENT_REPLAY_MATCHED":
        return "EXTERNAL_VALIDATION_ACCEPTED"
    if answers["6_internal_results_allowed_as_research_basis"]:
        return "EXTERNAL_VALIDATION_ACCEPTED_WITH_WARNINGS"
    return "NEED_MORE_EXTERNAL_VALIDATION"


def _master_required_answers(owner: Mapping[str, Any]) -> dict[str, Any]:
    owner_answers = _mapping(owner.get("required_answers"))
    recommendation = str(owner.get("owner_recommendation"))
    return {
        "1_external_validation_scope_complete": True,
        "2_static_baseline_passed": owner_answers.get(
            "1_static_baseline_basically_matches_external"
        )
        is True,
        "3_dynamic_weight_path_replay_passed": owner_answers.get(
            "2_dynamic_weight_path_replay_matches_internal"
        )
        is True,
        "4_metric_differences_explainable": owner_answers.get("4_metric_definitions_aligned")
        is True,
        "5_SGOV_total_return_proxy_acceptable": owner_answers.get(
            "3_SGOV_total_return_explained"
        )
        is True,
        "6_external_strategy_engine_full_replication_needed": owner_answers.get(
            "7_further_quantconnect_or_tradingview_needed"
        ),
        "7_can_continue_dual_forward_aging": recommendation
        in {
            "EXTERNAL_VALIDATION_ACCEPTED",
            "EXTERNAL_VALIDATION_ACCEPTED_WITH_WARNINGS",
        },
        "8_paper_shadow_allowed_false": True,
        "9_production_allowed_false": True,
        "10_broker_action_none": True,
    }


def _definition_check(check_id: str, canonical_definition: str) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "internal_definition": canonical_definition,
        "independent_replay_definition": canonical_definition,
        "external_platform_definition": "manual_confirmation_required",
        "status": "RECONCILED_WITH_MANUAL_EXTERNAL_CONFIRMATION_REQUIRED",
    }


def _difference_rows_from_static(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    if row.get("within_tolerance") is None:
        return [
            {
                "strategy_id": row.get("strategy_id"),
                "metric": "external_static_baseline_record",
                "internal_value": "available",
                "external_or_replay_value": None,
                "delta": None,
                "primary_difference_reason": "manual_external_input_pending",
                "secondary_difference_reason": "external_platform_export_not_supplied",
                "requires_fix": False,
                "owner_next_action": "supply_manual_external_static_baseline_record",
            }
        ]
    if row.get("within_tolerance") is True:
        return []
    return [
        {
            "strategy_id": row.get("strategy_id"),
            "metric": metric,
            "internal_value": row.get(f"internal_{metric}"),
            "external_or_replay_value": row.get(f"external_{metric}"),
            "delta": _mapping(row.get("metric_delta")).get(metric),
            "primary_difference_reason": "external_platform_limitation",
            "secondary_difference_reason": "metric_definition_or_data_difference",
            "requires_fix": False,
            "owner_next_action": "review_external_platform_export_and_metric_definition",
        }
        for metric in ("annual_return", "max_drawdown", "sharpe", "calmar")
    ]


def _difference_rows_from_replay(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    result = []
    for metric in _records(row.get("metric_reconciliation")):
        if metric.get("within_tolerance") is True:
            continue
        result.append(
            {
                "strategy_id": row.get("strategy_id"),
                "metric": metric.get("metric"),
                "internal_value": metric.get("internal_metric"),
                "external_or_replay_value": metric.get("replay_metric"),
                "delta": metric.get("metric_delta"),
                "primary_difference_reason": "internal_bug_suspected",
                "secondary_difference_reason": "independent_replay_mismatch",
                "requires_fix": True,
                "owner_next_action": "debug_internal_metric_or_weight_path_calculation",
            }
        )
    return result


def _price_matrix(
    prices_path: Path,
    config: Mapping[str, Any],
    *,
    start_date: date,
    end_date: date | None,
) -> pd.DataFrame:
    prices = _load_price_matrix(prices_path, _required_tickers(config))
    return _slice_prices(prices, start_date=start_date, end_date=end_date)


def _load_external_records(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    records = raw.get("records", raw) if isinstance(raw, Mapping) else raw
    return {
        str(row.get("strategy_id")): dict(row)
        for row in _records(records)
        if row.get("strategy_id")
    }


def _load_growth_config(path: Path) -> dict[str, Any]:
    import yaml

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        **AI_REGIME_SUMMARY,
        "summary": {**AI_REGIME_SUMMARY, **dict(summary)},
        **SAFETY_BOUNDARY,
        **extra,
    }


def _write_pair(payload: dict[str, Any], output_root: Path, artifact_id: str) -> None:
    payload["artifact_paths"] = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_json_and_doc(
    payload: dict[str, Any],
    json_path: Path,
    docs_path: Path,
    title: str,
) -> None:
    payload["artifact_paths"] = {
        "json_path": str(json_path),
        "markdown_path": str(docs_path),
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    _write_owner_doc(payload, docs_path, title)


def _write_owner_doc(payload: Mapping[str, Any], path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    answers = _mapping(payload.get("required_answers"))
    lines = [
        f"# {title}",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- owner_recommendation：`{payload.get('owner_recommendation', 'N/A')}`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- broker_action：`none`",
        "- manual_review_required：`true`",
        "",
        "## Required Answers",
        "",
        "|Question|Answer|",
        "|---|---|",
    ]
    for key, value in answers.items():
        lines.append(f"|`{key}`|`{value}`|")
    lines.extend(
        [
            "",
            "本报告仅用于 research-only 外部验证复核，不生成交易建议、paper-shadow "
            "activation、production config mutation 或 broker action。",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
    *,
    extra_artifact_globs: list[str] | None = None,
) -> dict[str, Any]:
    globs = [
        f"outputs/research_strategies/external_validation/{artifact_slug}.json",
        f"outputs/research_strategies/external_validation/{artifact_slug}.md",
    ]
    globs.extend(extra_artifact_globs or [])
    return {
        "report_id": report_id,
        "title": title,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "project_owner",
        "owner": "research_governance",
        "command": command,
        "artifact_globs": globs,
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "External validation artifacts are regenerated after data, weight-path, "
            "manual external export, replay or owner review changes."
        ),
        "owner_action": "review_external_validation_research_only_artifact",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _doc_report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_slug: str,
    docs_glob: str,
) -> dict[str, Any]:
    entry = _report_registry_entry(report_id, title, command, artifact_slug)
    entry["artifact_globs"] = [
        f"outputs/research_strategies/external_validation/{artifact_slug}.json",
        docs_glob,
    ]
    return entry


def _artifact_paths_by_report(sources: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {name: source.get("artifact_paths", {}) for name, source in sources.items()}


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return dict(raw) if isinstance(raw, Mapping) else {}


def _prohibited_reader_brief_hits(payload: Mapping[str, Any]) -> list[str]:
    prohibited = (
        "买入",
        "卖出",
        "应调仓",
        "实盘仓位",
        "真实交易建议",
        "paper-shadow active",
        "production ready",
        "broker action",
    )
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return [phrase for phrase in prohibited if phrase in raw]


def _date_range_label(start_date: date, end_date: date | None) -> str:
    return f"{start_date.isoformat()}..{end_date.isoformat() if end_date else 'latest'}"


def _compound_return(returns: pd.Series) -> float:
    return float((1.0 + returns).prod() - 1.0) if not returns.empty else 0.0


def _safety_summary() -> dict[str, Any]:
    return {
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _records_to_text(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if value is None:
        return []
    return [str(value)]


def _dedupe_text(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _round(value: object) -> float:
    return round(_float(value), 6)


def _stable_hash(value: object) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
