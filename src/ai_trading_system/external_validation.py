from __future__ import annotations

import csv
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
    PRIMARY_RESEARCH_START,
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
DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR = (
    PROJECT_ROOT / "inputs" / "external_validation" / "manual_external_records"
)
DEFAULT_MANUAL_EXTERNAL_RECORDS_TEMPLATE_YAML_PATH = (
    DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR / "static_baseline_external_records.template.yaml"
)
DEFAULT_MANUAL_EXTERNAL_RECORDS_TEMPLATE_CSV_PATH = (
    DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR / "static_baseline_external_records.template.csv"
)
DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH = (
    DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR / "static_baseline_external_records.yaml"
)
DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH = (
    DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR / "static_baseline_external_records.csv"
)
DEFAULT_MANUAL_EXTERNAL_RECORD_INPUT_GUIDE_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "manual_external_record_input_guide.md"
)
DEFAULT_STATIC_BASELINE_EXTERNAL_MANUAL_RUNBOOK_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "static_baseline_external_manual_runbook.md"
)
DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH = (
    DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR / "external_platform_metric_convention_signoff.yaml"
)
DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH = (
    DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR / "sgov_external_convention_signoff.yaml"
)
DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_OWNER_SIGNOFF_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "external_validation_manual_evidence_owner_signoff.md"
)
DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "external_validation_manual_evidence_master_review.md"
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
STATIC_BASELINE_INTERNAL_IDS = {
    "100_qqq": "qqq_100_static",
    "qqq_50_sgov_50": "qqq_50_sgov_50",
    "qqq_60_sgov_40": "qqq_60_sgov_40",
}
STATIC_BASELINE_EXPECTED_WEIGHTS: dict[str, dict[str, float]] = {
    "100_qqq": {"QQQ": 1.0},
    "qqq_50_sgov_50": {"QQQ": 0.5, "SGOV": 0.5},
    "qqq_60_sgov_40": {"QQQ": 0.6, "SGOV": 0.4},
}
SINGLE_ASSET_REBALANCE_EQUIVALENTS = {
    "no rebalancing",
    "no_rebalancing",
    "none",
    "not_applicable",
    "not applicable",
    "n/a",
    "na",
}
MANUAL_EXTERNAL_RECORD_FIELDS = (
    "external_tool",
    "external_tool_url_or_name",
    "strategy_id",
    "date_range_start",
    "date_range_end",
    "asset_weights",
    "rebalance_frequency",
    "dividend_reinvestment",
    "price_or_total_return_policy",
    "annual_return",
    "max_drawdown",
    "sharpe",
    "calmar",
    "turnover",
    "monthly_returns_available",
    "export_file_path",
    "screenshot_reference",
    "manual_notes",
    "owner",
    "recorded_at",
)
MANUAL_METRIC_FIELDS = ("annual_return", "max_drawdown", "sharpe", "calmar", "turnover")
RECONCILIATION_METRIC_FIELDS = ("annual_return", "max_drawdown", "sharpe", "calmar")
METRIC_UNAVAILABLE_MARKER = "metric_unavailable_on_platform"
SGOV_CONVENTIONS = {"unknown", "price_only", "adjusted", "total_return", "platform_default"}
METRIC_INTERNAL_DEFINITIONS = {
    "annual_return": "CAGR from cumulative daily equity path",
    "max_drawdown": "daily equity path drawdown from cumulative peak",
    "sharpe": "daily excess return Sharpe with zero risk-free baseline",
    "calmar": "CAGR divided by absolute max drawdown",
    "turnover": "sum of absolute target-weight changes after rebalance",
    "rebalance": "monthly static baseline rebalance unless explicitly stated",
    "dividend": "adjusted close / total-return proxy when available",
}

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
    "market_regime": "unified_primary_2021",
    "anchor_event": "validated QQQ/SGOV/TQQQ common history start",
    "anchor_date": "2021-02-22",
    "default_backtest_start": (
        PRIMARY_RESEARCH_START
        if isinstance(PRIMARY_RESEARCH_START, date)
        else date.fromisoformat(str(PRIMARY_RESEARCH_START))
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
            "market_regime": "unified_primary_2021",
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


def run_manual_external_record_template(
    *,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    template_dir: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_DIR,
    guide_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORD_INPUT_GUIDE_DOC_PATH,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    yaml_path = template_dir / DEFAULT_MANUAL_EXTERNAL_RECORDS_TEMPLATE_YAML_PATH.name
    csv_path = template_dir / DEFAULT_MANUAL_EXTERNAL_RECORDS_TEMPLATE_CSV_PATH.name
    template_records = _manual_template_records(start_date=start_date, end_date=end_date)
    _write_manual_external_template_files(template_records, yaml_path, csv_path)
    _write_manual_external_record_input_guide(
        guide_path,
        yaml_path=yaml_path,
        csv_path=csv_path,
        start_date=start_date,
        end_date=end_date,
    )
    written_paths = [yaml_path, csv_path, guide_path]
    status = (
        "MANUAL_EXTERNAL_TEMPLATE_READY"
        if all(path.exists() for path in written_paths)
        else "MANUAL_EXTERNAL_TEMPLATE_PARTIAL"
    )
    payload = _payload(
        report_type="manual_external_record_template",
        title="Manual External Record Template",
        status=status,
        summary={
            "template_field_count": len(MANUAL_EXTERNAL_RECORD_FIELDS),
            "baseline_count": len(STATIC_BASELINE_IDS),
            "date_range": _date_range_label(start_date, end_date),
            **_safety_summary(),
        },
        template_fields=list(MANUAL_EXTERNAL_RECORD_FIELDS),
        baseline_templates=template_records,
        template_paths={
            "yaml": str(yaml_path),
            "csv": str(csv_path),
            "guide": str(guide_path),
        },
        required_statuses=[
            "MANUAL_EXTERNAL_TEMPLATE_READY",
            "MANUAL_EXTERNAL_TEMPLATE_PARTIAL",
            "MANUAL_EXTERNAL_TEMPLATE_BLOCKED",
        ],
        report_registry_entry=_report_registry_entry(
            "manual_external_record_template",
            "Manual External Record Template",
            "aits research strategies manual-external-record-template",
            "manual_external_record_template",
            extra_artifact_globs=[
                "inputs/external_validation/manual_external_records/"
                "static_baseline_external_records.template.yaml",
                "inputs/external_validation/manual_external_records/"
                "static_baseline_external_records.template.csv",
                "docs/research/manual_external_record_input_guide.md",
            ],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_static_baseline_external_manual_runbook(
    *,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_STATIC_BASELINE_EXTERNAL_MANUAL_RUNBOOK_DOC_PATH,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(simple_config_path)
    baseline_rows = _manual_baseline_rows(config)
    payload = _payload(
        report_type="static_baseline_external_manual_runbook",
        title="Static Baseline External Manual Runbook",
        status="MANUAL_RUNBOOK_READY",
        summary={
            "baseline_count": len(baseline_rows),
            "date_range": _date_range_label(start_date, end_date),
            "required_metric_count": len(MANUAL_METRIC_FIELDS),
            **_safety_summary(),
        },
        baseline_rows=baseline_rows,
        date_range={
            "start": start_date.isoformat(),
            "end": end_date.isoformat() if end_date else "latest_available_internal_as_of",
        },
        required_metrics=list(MANUAL_METRIC_FIELDS),
        screenshot_and_export_policy={
            "minimum_evidence": "screenshot_reference_or_export_file_path_required",
            "forbidden": [
                "broker_account_information",
                "personal_account_screenshots",
                "internal_metrics_copied_as_external_results",
            ],
        },
        manual_records_target_paths={
            "yaml": str(DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH),
            "csv": str(DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH),
        },
        report_registry_entry=_doc_report_registry_entry(
            "static_baseline_external_manual_runbook",
            "Static Baseline External Manual Runbook",
            "aits research strategies static-baseline-external-manual-runbook",
            "static_baseline_external_manual_runbook",
            "docs/research/static_baseline_external_manual_runbook.md",
        ),
    )
    _write_runbook_artifact(
        payload,
        output_root / "static_baseline_external_manual_runbook.json",
        docs_path,
    )
    return payload


def run_static_baseline_external_manual_input_ingestion(
    *,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    input_yaml_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH,
    input_csv_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    config = _load_registry(simple_config_path)
    loaded = _load_manual_external_records(input_yaml_path, input_csv_path)
    normalized_records = [
        _normalize_manual_external_record(
            row,
            source=source,
            start_date=start_date,
            end_date=end_date,
        )
        for source, row in loaded["records"]
    ]
    expected = set(STATIC_BASELINE_IDS)
    present = {str(row.get("strategy_id")) for row in normalized_records}
    missing_strategy_ids = sorted(expected - present)
    invalid_records = [
        row for row in normalized_records if _records_to_text(row.get("validation_errors"))
    ]
    valid_records = [
        row for row in normalized_records if not _records_to_text(row.get("validation_errors"))
    ]
    for row in valid_records:
        row["baseline_definition"] = _manual_baseline_definition(
            config,
            str(row.get("strategy_id")),
        )
    if loaded["parse_errors"]:
        status = "MANUAL_EXTERNAL_INPUT_BLOCKED"
    elif not loaded["source_paths"]:
        status = "MANUAL_EXTERNAL_INPUT_MISSING"
    elif invalid_records or missing_strategy_ids:
        status = "MANUAL_EXTERNAL_INPUT_PARTIAL"
    else:
        status = "MANUAL_EXTERNAL_INPUT_RECORDED"
    payload = _payload(
        report_type="static_baseline_external_manual_input_ingestion",
        title="Static Baseline External Manual Input Ingestion",
        status=status,
        summary={
            "source_file_count": len(loaded["source_paths"]),
            "record_count": len(normalized_records),
            "valid_record_count": len(valid_records),
            "invalid_record_count": len(invalid_records),
            "missing_strategy_count": len(missing_strategy_ids),
            **_safety_summary(),
        },
        input_paths=loaded["source_paths"],
        parsed_record_count=len(normalized_records),
        records=normalized_records,
        valid_records=valid_records,
        invalid_records=invalid_records,
        missing_strategy_ids=missing_strategy_ids,
        parse_errors=loaded["parse_errors"],
        allowed_strategy_ids=list(STATIC_BASELINE_IDS),
        required_sgov_conventions=sorted(SGOV_CONVENTIONS),
        report_registry_entry=_report_registry_entry(
            "static_baseline_external_manual_input_ingestion",
            "Static Baseline External Manual Input Ingestion",
            "aits research strategies static-baseline-external-manual-input-ingestion",
            "static_baseline_external_manual_input_ingestion",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_external_platform_metric_convention_signoff(
    *,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    signoff_path: Path = DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH,
    input_yaml_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH,
    input_csv_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH,
    _manual_input_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manual_input = dict(
        _manual_input_payload
        or run_static_baseline_external_manual_input_ingestion(
            output_root=output_root,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
        )
    )
    signoff_rows, parse_errors = _load_metric_convention_signoff_rows(signoff_path)
    external_tools = _manual_external_tools(manual_input)
    rows = _metric_convention_rows(signoff_rows, external_tools)
    convention_namespace = load_external_metric_convention_namespace(signoff_path)
    unknown_rows = [
        row for row in rows if str(row["manual_confirmation_status"]).lower() == "unknown"
    ]
    limitation_rows = [
        row
        for row in rows
        if str(row["definition_match_status"]).lower()
        in {"platform_default", "partial", "limitation", "different"}
        or _bool(row.get("difference_expected"))
    ]
    if parse_errors:
        status = "METRIC_CONVENTIONS_BLOCKED"
    elif unknown_rows:
        status = "METRIC_CONVENTIONS_STILL_UNKNOWN"
    elif limitation_rows:
        status = "METRIC_CONVENTIONS_CONFIRMED_WITH_LIMITATIONS"
    else:
        status = "METRIC_CONVENTIONS_CONFIRMED"
    payload = _payload(
        report_type="external_platform_metric_convention_signoff",
        title="External Platform Metric Convention Signoff",
        status=status,
        summary={
            "metric_row_count": len(rows),
            "unknown_row_count": len(unknown_rows),
            "limitation_row_count": len(limitation_rows),
            **_safety_summary(),
        },
        signoff_input_path=str(signoff_path),
        convention_namespace=convention_namespace,
        metric_rows=rows,
        parse_errors=parse_errors,
        source_statuses={
            "static_baseline_external_manual_input_ingestion": manual_input.get("status")
        },
        source_artifacts=_artifact_paths_by_report(
            {"static_baseline_external_manual_input_ingestion": manual_input}
        ),
        report_registry_entry=_report_registry_entry(
            "external_platform_metric_convention_signoff",
            "External Platform Metric Convention Signoff",
            "aits research strategies external-platform-metric-convention-signoff",
            "external_platform_metric_convention_signoff",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def load_external_metric_convention_namespace(
    signoff_path: Path = DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH,
) -> dict[str, Any]:
    if not signoff_path.exists():
        return {
            "status": "METRIC_CONVENTION_NAMESPACE_MISSING",
            "signoff_path": str(signoff_path),
            "return_metrics": {},
            "risk_metrics": {},
        }
    try:
        import yaml

        raw = yaml.safe_load(signoff_path.read_text(encoding="utf-8")) or {}
    except (OSError, ValueError) as exc:
        return {
            "status": "METRIC_CONVENTION_NAMESPACE_BLOCKED",
            "signoff_path": str(signoff_path),
            "parse_error": str(exc),
            "return_metrics": {},
            "risk_metrics": {},
        }
    if not isinstance(raw, Mapping):
        return {
            "status": "METRIC_CONVENTION_NAMESPACE_BLOCKED",
            "signoff_path": str(signoff_path),
            "parse_error": "signoff YAML root must be a mapping",
            "return_metrics": {},
            "risk_metrics": {},
        }
    platform = _mapping(raw.get("portfolio_visualizer"))
    return {
        "status": (
            "METRIC_CONVENTION_NAMESPACE_READY"
            if platform
            else "METRIC_CONVENTION_NAMESPACE_LEGACY_RECORDS_ONLY"
        ),
        "signoff_path": str(signoff_path),
        "platform": "portfolio_visualizer",
        "platform_role": platform.get("platform_role"),
        "return_metrics": _mapping(platform.get("return_metrics")),
        "risk_metrics": _mapping(platform.get("risk_metrics")),
    }


def validate_external_metric_convention_usage(
    *,
    external_metric_id: str,
    internal_metric_id: str,
    usage_context: str = "promotion_gate",
    signoff_path: Path = DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH,
) -> dict[str, Any]:
    namespace = load_external_metric_convention_namespace(signoff_path)
    rows: list[dict[str, Any]] = []
    for metric_group in ("return_metrics", "risk_metrics"):
        for metric_name, raw in _mapping(namespace.get(metric_group)).items():
            if isinstance(raw, Mapping):
                row = dict(raw)
                row["metric_name"] = metric_name
                row["metric_group"] = metric_group
                rows.append(row)
    match = next(
        (
            row
            for row in rows
            if row.get("external_metric_id") == external_metric_id
            and row.get("internal_metric_id") == internal_metric_id
        ),
        None,
    )
    if not match:
        return {
            "status": "METRIC_CONVENTION_USAGE_UNKNOWN",
            "hard_warning": True,
            "external_metric_id": external_metric_id,
            "internal_metric_id": internal_metric_id,
            "usage_context": usage_context,
            "reason": "metric_pair_not_signed_off",
            "namespace_status": namespace.get("status"),
        }
    promotion_usage = str(match.get("promotion_usage") or "")
    if promotion_usage == "not_cross_comparable":
        return {
            "status": "METRIC_CONVENTION_USAGE_BLOCKED",
            "hard_warning": True,
            "external_metric_id": external_metric_id,
            "internal_metric_id": internal_metric_id,
            "usage_context": usage_context,
            "reason": "signed_off_as_not_cross_comparable",
            "metric_group": match.get("metric_group"),
            "promotion_usage": promotion_usage,
        }
    return {
        "status": "METRIC_CONVENTION_USAGE_ALLOWED",
        "hard_warning": False,
        "external_metric_id": external_metric_id,
        "internal_metric_id": internal_metric_id,
        "usage_context": usage_context,
        "reason": "signed_off_usage_allowed",
        "metric_group": match.get("metric_group"),
        "promotion_usage": promotion_usage,
    }


def run_sgov_external_convention_signoff(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    signoff_path: Path = DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _sgov_check_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sgov_check = dict(
        _sgov_check_payload
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
    rows, parse_errors = _load_sgov_convention_signoff_rows(signoff_path, sgov_check)
    accepted_rows = [row for row in rows if _bool(row.get("convention_accepted"))]
    unknown_rows = [
        row
        for row in rows
        if str(row.get("sgov_convention", "unknown")).lower() == "unknown"
        or not _bool(row.get("convention_accepted"))
    ]
    limitation_rows = [
        row
        for row in accepted_rows
        if str(row.get("sgov_convention", "")).lower() in {"price_only", "platform_default"}
    ]
    if parse_errors:
        status = "SGOV_CONVENTION_BLOCKED"
    elif unknown_rows:
        status = "SGOV_CONVENTION_STILL_UNKNOWN"
    elif limitation_rows:
        status = "SGOV_CONVENTION_LIMITATION_ACCEPTED"
    else:
        status = "SGOV_CONVENTION_CONFIRMED"
    payload = _payload(
        report_type="sgov_external_convention_signoff",
        title="SGOV External Convention Signoff",
        status=status,
        summary={
            "sgov_row_count": len(rows),
            "accepted_row_count": len(accepted_rows),
            "unknown_row_count": len(unknown_rows),
            **_safety_summary(),
        },
        signoff_input_path=str(signoff_path),
        sgov_rows=rows,
        parse_errors=parse_errors,
        source_statuses={"sgov_total_return_external_check": sgov_check.get("status")},
        source_artifacts=_artifact_paths_by_report(
            {"sgov_total_return_external_check": sgov_check}
        ),
        report_registry_entry=_report_registry_entry(
            "sgov_external_convention_signoff",
            "SGOV External Convention Signoff",
            "aits research strategies sgov-external-convention-signoff",
            "sgov_external_convention_signoff",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_static_baseline_final_reconciliation_after_manual_input(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    input_yaml_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH,
    input_csv_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH,
    metric_signoff_path: Path = DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH,
    sgov_signoff_path: Path = DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _manual_input_payload: Mapping[str, Any] | None = None,
    _metric_signoff_payload: Mapping[str, Any] | None = None,
    _sgov_signoff_payload: Mapping[str, Any] | None = None,
    _static_final_check_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    config = _load_registry(simple_config_path)
    manual_input = dict(
        _manual_input_payload
        or run_static_baseline_external_manual_input_ingestion(
            simple_config_path=simple_config_path,
            output_root=output_root,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            start_date=start_date,
            end_date=end_date,
        )
    )
    metric_signoff = dict(
        _metric_signoff_payload
        or run_external_platform_metric_convention_signoff(
            output_root=output_root,
            signoff_path=metric_signoff_path,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            _manual_input_payload=manual_input,
        )
    )
    sgov_signoff = dict(
        _sgov_signoff_payload
        or run_sgov_external_convention_signoff(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            signoff_path=sgov_signoff_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    static_final_check = dict(
        _static_final_check_payload
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
    if str(manual_input.get("status")) in {
        "MANUAL_EXTERNAL_INPUT_MISSING",
        "MANUAL_EXTERNAL_INPUT_BLOCKED",
    }:
        blockers.append("manual_external_input_missing_or_blocked")
    prices = _price_matrix(prices_path, config, start_date=start_date, end_date=end_date)
    internal_rows = {
        str(row.get("strategy_id")): row for row in _internal_static_metric_rows(config, prices)
    }
    manual_records = {
        str(row.get("strategy_id")): row for row in _records(manual_input.get("valid_records"))
    }
    reconciliation_rows = [
        _manual_final_reconciliation_row(
            strategy_id,
            internal_rows.get(strategy_id, {}),
            manual_records.get(strategy_id),
            metric_signoff=metric_signoff,
            sgov_signoff=sgov_signoff,
        )
        for strategy_id in STATIC_BASELINE_IDS
    ]
    hard_mismatches = [
        row
        for row in reconciliation_rows
        if row.get("difference_reason") == "metric_delta_exceeds_tolerance"
    ]
    warning_rows = [
        row
        for row in reconciliation_rows
        if row.get("difference_reason") not in {"within_tolerance", "manual_external_input_missing"}
    ]
    if blockers:
        status = "STATIC_BASELINE_MANUAL_BLOCKED"
    elif hard_mismatches:
        status = "STATIC_BASELINE_MANUAL_MISMATCH"
    elif warning_rows:
        status = "STATIC_BASELINE_MANUAL_RECONCILED_WITH_WARNINGS"
    else:
        status = "STATIC_BASELINE_MANUAL_RECONCILED"
    payload = _payload(
        report_type="static_baseline_final_reconciliation_after_manual_input",
        title="Static Baseline Final Reconciliation After Manual Input",
        status=status,
        summary={
            "row_count": len(reconciliation_rows),
            "hard_mismatch_count": len(hard_mismatches),
            "warning_row_count": len(warning_rows),
            "data_quality_status": data_gate.get("status"),
            **_safety_summary(),
        },
        reconciliation_rows=reconciliation_rows,
        data_quality=data_gate,
        blocking_reasons=blockers,
        warning_reasons=_dedupe_text(
            [str(row.get("difference_reason")) for row in warning_rows]
        ),
        source_statuses={
            "static_baseline_external_manual_input_ingestion": manual_input.get("status"),
            "external_platform_metric_convention_signoff": metric_signoff.get("status"),
            "sgov_external_convention_signoff": sgov_signoff.get("status"),
            "static_baseline_reconciliation_final_check": static_final_check.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "static_baseline_external_manual_input_ingestion": manual_input,
                "external_platform_metric_convention_signoff": metric_signoff,
                "sgov_external_convention_signoff": sgov_signoff,
                "static_baseline_reconciliation_final_check": static_final_check,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "static_baseline_final_reconciliation_after_manual_input",
            "Static Baseline Final Reconciliation After Manual Input",
            "aits research strategies static-baseline-final-reconciliation-after-manual-input",
            "static_baseline_final_reconciliation_after_manual_input",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dynamic_weight_path_external_support_check(
    *,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
) -> dict[str, Any]:
    rows = [
        {
            "platform": "Portfolio Visualizer",
            "supports_dynamic_weight_path_import": False,
            "supports_custom_rebalance_dates": "limited",
            "supports_custom_weights_by_date": False,
            "supports_cash_or_sgov_handling": "static_only_manual_confirmation_required",
            "supports_exported_weight_path_replay": False,
            "supports_custom_strategy_code": False,
            "manual_work_required": "use only for static baseline checks",
            "recommended_role": "STATIC_BASELINE_ONLY",
        },
        {
            "platform": "testfol.io",
            "supports_dynamic_weight_path_import": False,
            "supports_custom_rebalance_dates": "limited",
            "supports_custom_weights_by_date": False,
            "supports_cash_or_sgov_handling": "static_only_manual_confirmation_required",
            "supports_exported_weight_path_replay": False,
            "supports_custom_strategy_code": False,
            "manual_work_required": "use only for static baseline cross-checks",
            "recommended_role": "STATIC_BASELINE_ONLY",
        },
        {
            "platform": "QuantConnect",
            "supports_dynamic_weight_path_import": True,
            "supports_custom_rebalance_dates": True,
            "supports_custom_weights_by_date": True,
            "supports_cash_or_sgov_handling": True,
            "supports_exported_weight_path_replay": True,
            "supports_custom_strategy_code": True,
            "manual_work_required": "implement custom data import and research-only backtest",
            "recommended_role": "FULL_STRATEGY_REPLICATION_POSSIBLE",
        },
        {
            "platform": "TradingView",
            "supports_dynamic_weight_path_import": "manual_or_script_limited",
            "supports_custom_rebalance_dates": True,
            "supports_custom_weights_by_date": "limited",
            "supports_cash_or_sgov_handling": "symbol_and_script_dependent",
            "supports_exported_weight_path_replay": "limited",
            "supports_custom_strategy_code": True,
            "manual_work_required": "script recreation; not authoritative for reconciliation",
            "recommended_role": "WEIGHT_PATH_REPLAY_POSSIBLE",
        },
        {
            "platform": "local independent notebook",
            "supports_dynamic_weight_path_import": True,
            "supports_custom_rebalance_dates": True,
            "supports_custom_weights_by_date": True,
            "supports_cash_or_sgov_handling": True,
            "supports_exported_weight_path_replay": True,
            "supports_custom_strategy_code": True,
            "manual_work_required": "maintain independent replay notebook or script audit trail",
            "recommended_role": "WEIGHT_PATH_REPLAY_POSSIBLE",
        },
    ]
    status = "DYNAMIC_EXTERNAL_SUPPORT_REQUIRES_CUSTOM_ENGINE"
    payload = _payload(
        report_type="dynamic_weight_path_external_support_check",
        title="Dynamic Weight Path External Support Check",
        status=status,
        summary={
            "platform_count": len(rows),
            "custom_engine_required": True,
            "recommended_dynamic_platform": "QuantConnect or local independent notebook",
            **_safety_summary(),
        },
        platform_rows=rows,
        allowed_recommended_roles=[
            "STATIC_BASELINE_ONLY",
            "WEIGHT_PATH_REPLAY_POSSIBLE",
            "FULL_STRATEGY_REPLICATION_POSSIBLE",
            "NOT_RECOMMENDED",
        ],
        report_registry_entry=_report_registry_entry(
            "dynamic_weight_path_external_support_check",
            "Dynamic Weight Path External Support Check",
            "aits research strategies dynamic-weight-path-external-support-check",
            "dynamic_weight_path_external_support_check",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_quantconnect_weight_path_replay_preflight(
    *,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
) -> dict[str, Any]:
    payload = _payload(
        report_type="quantconnect_weight_path_replay_preflight",
        title="QuantConnect Weight Path Replay Preflight",
        status="QC_WEIGHT_PATH_PREFLIGHT_NEEDS_MANUAL_IMPLEMENTATION",
        summary={
            "schema_field_count": 10,
            "manual_step_count": 7,
            "blocker_count": 3,
            **_safety_summary(),
        },
        weight_path_csv_schema=[
            "date",
            "strategy_id",
            "definition_hash",
            "target_weight_qqq",
            "target_weight_tqqq",
            "target_weight_sgov",
            "rebalance_flag",
            "signal_time",
            "execution_assumption",
            "data_quality_status",
        ],
        custom_data_import_schema={
            "date": "QC Time field / trading date",
            "weights": "decimal target weights by ticker",
            "definition_hash": "string audit key, not trading signal",
        },
        symbol_mapping={"QQQ": "QQQ", "SGOV": "SGOV", "TQQQ": "TQQQ"},
        rebalance_date_handling="rebalance only on rows where rebalance_flag is true",
        execution_timing="apply imported target weights after signal close / next bar execution",
        cash_or_sgov_handling="SGOV treated as defensive ETF; cash remains residual only",
        transaction_cost_model="0 bps reconciliation baseline, then explicit sensitivity",
        expected_output_metrics=[
            "annual_return",
            "max_drawdown",
            "sharpe",
            "calmar",
            "turnover",
            "monthly_returns",
        ],
        manual_implementation_steps=[
            "create research-only QuantConnect project",
            "confirm ETF adjusted data and dividend reinvestment behavior",
            "import exported weight path CSV as custom data",
            "map symbols and dates to QC Securities",
            "apply target weights with no broker account credentials",
            "export equity curve and monthly returns",
            "attach exports/screenshots to manual evidence records",
        ],
        current_blockers=[
            "manual QuantConnect implementation not yet created",
            "SGOV adjusted/total-return behavior still needs owner confirmation",
            "external platform metric definitions still need owner confirmation",
        ],
        report_registry_entry=_report_registry_entry(
            "quantconnect_weight_path_replay_preflight",
            "QuantConnect Weight Path Replay Preflight",
            "aits research strategies quantconnect-weight-path-replay-preflight",
            "quantconnect_weight_path_replay_preflight",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_external_validation_manual_evidence_owner_signoff(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_OWNER_SIGNOFF_DOC_PATH,
    input_yaml_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH,
    input_csv_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH,
    metric_signoff_path: Path = DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH,
    sgov_signoff_path: Path = DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _manual_input_payload: Mapping[str, Any] | None = None,
    _metric_signoff_payload: Mapping[str, Any] | None = None,
    _sgov_signoff_payload: Mapping[str, Any] | None = None,
    _final_reconciliation_payload: Mapping[str, Any] | None = None,
    _dynamic_support_payload: Mapping[str, Any] | None = None,
    _qc_preflight_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manual_input = dict(
        _manual_input_payload
        or run_static_baseline_external_manual_input_ingestion(
            simple_config_path=simple_config_path,
            output_root=output_root,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            start_date=start_date,
            end_date=end_date,
        )
    )
    metric_signoff = dict(
        _metric_signoff_payload
        or run_external_platform_metric_convention_signoff(
            output_root=output_root,
            signoff_path=metric_signoff_path,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            _manual_input_payload=manual_input,
        )
    )
    sgov_signoff = dict(
        _sgov_signoff_payload
        or run_sgov_external_convention_signoff(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            signoff_path=sgov_signoff_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    final_reconciliation = dict(
        _final_reconciliation_payload
        or run_static_baseline_final_reconciliation_after_manual_input(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            metric_signoff_path=metric_signoff_path,
            sgov_signoff_path=sgov_signoff_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
            _manual_input_payload=manual_input,
            _metric_signoff_payload=metric_signoff,
            _sgov_signoff_payload=sgov_signoff,
        )
    )
    dynamic_support = dict(
        _dynamic_support_payload
        or run_dynamic_weight_path_external_support_check(output_root=output_root)
    )
    qc_preflight = dict(
        _qc_preflight_payload
        or run_quantconnect_weight_path_replay_preflight(output_root=output_root)
    )
    answers = _manual_owner_required_answers(
        manual_input,
        metric_signoff,
        sgov_signoff,
        final_reconciliation,
        dynamic_support,
        qc_preflight,
    )
    recommendation = _manual_owner_recommendation(
        manual_input,
        metric_signoff,
        sgov_signoff,
        final_reconciliation,
        dynamic_support,
    )
    payload = _payload(
        report_type="external_validation_manual_evidence_owner_signoff",
        title="External Validation Manual Evidence Owner Signoff",
        status="EXTERNAL_VALIDATION_MANUAL_EVIDENCE_OWNER_SIGNOFF_READY",
        summary={
            "owner_recommendation": recommendation,
            "manual_input_status": manual_input.get("status"),
            "valid_external_record_count": len(_records(manual_input.get("valid_records"))),
            "missing_strategy_ids": manual_input.get("missing_strategy_ids", []),
            "final_reconciliation_status": final_reconciliation.get("status"),
            **_safety_summary(),
        },
        owner_recommendation=recommendation,
        required_answers=answers,
        source_statuses={
            "static_baseline_external_manual_input_ingestion": manual_input.get("status"),
            "external_platform_metric_convention_signoff": metric_signoff.get("status"),
            "sgov_external_convention_signoff": sgov_signoff.get("status"),
            "static_baseline_final_reconciliation_after_manual_input": final_reconciliation.get(
                "status"
            ),
            "dynamic_weight_path_external_support_check": dynamic_support.get("status"),
            "quantconnect_weight_path_replay_preflight": qc_preflight.get("status"),
        },
        source_artifacts=_artifact_paths_by_report(
            {
                "static_baseline_external_manual_input_ingestion": manual_input,
                "external_platform_metric_convention_signoff": metric_signoff,
                "sgov_external_convention_signoff": sgov_signoff,
                "static_baseline_final_reconciliation_after_manual_input": final_reconciliation,
                "dynamic_weight_path_external_support_check": dynamic_support,
                "quantconnect_weight_path_replay_preflight": qc_preflight,
            }
        ),
        report_registry_entry=_doc_report_registry_entry(
            "external_validation_manual_evidence_owner_signoff",
            "External Validation Manual Evidence Owner Signoff",
            "aits research strategies external-validation-manual-evidence-owner-signoff",
            "external_validation_manual_evidence_owner_signoff",
            "docs/research/external_validation_manual_evidence_owner_signoff.md",
        ),
    )
    _write_json_and_doc(
        payload,
        output_root / "external_validation_manual_evidence_owner_signoff.json",
        docs_path,
        "External Validation Manual Evidence Owner Signoff",
    )
    return payload


def run_external_validation_manual_evidence_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_EQUAL_RISK_GROWTH_TILT_CONFIG_PATH,
    output_root: Path = DEFAULT_EXTERNAL_VALIDATION_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_MASTER_REVIEW_DOC_PATH,
    owner_docs_path: Path = DEFAULT_EXTERNAL_VALIDATION_MANUAL_EVIDENCE_OWNER_SIGNOFF_DOC_PATH,
    input_yaml_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_YAML_PATH,
    input_csv_path: Path = DEFAULT_MANUAL_EXTERNAL_RECORDS_CSV_PATH,
    metric_signoff_path: Path = DEFAULT_METRIC_CONVENTION_SIGNOFF_INPUT_PATH,
    sgov_signoff_path: Path = DEFAULT_SGOV_CONVENTION_SIGNOFF_INPUT_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
    _owner_signoff_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    owner = dict(
        _owner_signoff_payload
        or run_external_validation_manual_evidence_owner_signoff(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            simple_config_path=simple_config_path,
            growth_config_path=growth_config_path,
            output_root=output_root,
            docs_path=owner_docs_path,
            input_yaml_path=input_yaml_path,
            input_csv_path=input_csv_path,
            metric_signoff_path=metric_signoff_path,
            sgov_signoff_path=sgov_signoff_path,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        )
    )
    recommendation = str(owner.get("owner_recommendation"))
    if recommendation == "ACCEPT_EXTERNAL_VALIDATION":
        status = "EXTERNAL_MANUAL_EVIDENCE_ACCEPTED"
    elif recommendation == "ACCEPT_EXTERNAL_VALIDATION_WITH_WARNINGS":
        status = "EXTERNAL_MANUAL_EVIDENCE_ACCEPTED_WITH_WARNINGS"
    elif recommendation in {"NEED_MORE_MANUAL_EVIDENCE", "NEED_DYNAMIC_PLATFORM_REPLAY"}:
        status = "EXTERNAL_MANUAL_EVIDENCE_NEEDS_MORE_INPUT"
    else:
        status = "EXTERNAL_MANUAL_EVIDENCE_BLOCKED"
    answers = _manual_master_required_answers(owner)
    payload = _payload(
        report_type="external_validation_manual_evidence_master_review",
        title="External Validation Manual Evidence Master Review",
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
            "KEEP_EXTERNAL_VALIDATION_RESEARCH_ONLY",
            "NO_PAPER_SHADOW_NO_PRODUCTION_NO_BROKER",
        ],
        source_statuses={
            "external_validation_manual_evidence_owner_signoff": owner.get("status")
        },
        source_artifacts=_artifact_paths_by_report(
            {"external_validation_manual_evidence_owner_signoff": owner}
        ),
        report_registry_entry=_doc_report_registry_entry(
            "external_validation_manual_evidence_master_review",
            "External Validation Manual Evidence Master Review",
            "aits research strategies external-validation-manual-evidence-master-review",
            "external_validation_manual_evidence_master_review",
            "docs/research/external_validation_manual_evidence_master_review.md",
        ),
    )
    _write_json_and_doc(
        payload,
        output_root / "external_validation_manual_evidence_master_review.json",
        docs_path,
        "External Validation Manual Evidence Master Review",
    )
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


def _manual_template_records(
    *,
    start_date: date,
    end_date: date | None,
) -> list[dict[str, Any]]:
    end = end_date.isoformat() if end_date else "latest_available_internal_as_of"
    records = []
    for strategy_id in STATIC_BASELINE_IDS:
        records.append(
            {
                "external_tool": "TODO_Portfolio_Visualizer_or_testfol",
                "external_tool_url_or_name": "",
                "strategy_id": strategy_id,
                "date_range_start": start_date.isoformat(),
                "date_range_end": end,
                "asset_weights": STATIC_BASELINE_EXPECTED_WEIGHTS[strategy_id],
                "rebalance_frequency": "monthly",
                "dividend_reinvestment": "unknown",
                "price_or_total_return_policy": "unknown",
                "annual_return": "TODO_OR_metric_unavailable_on_platform",
                "max_drawdown": "TODO_OR_metric_unavailable_on_platform",
                "sharpe": "TODO_OR_metric_unavailable_on_platform",
                "calmar": "TODO_OR_metric_unavailable_on_platform",
                "turnover": "TODO_OR_metric_unavailable_on_platform",
                "monthly_returns_available": "unknown",
                "export_file_path": "",
                "screenshot_reference": "",
                "manual_notes": (
                    "Do not copy internal metrics into external fields. Record platform "
                    "limitations and date-range differences here."
                ),
                "owner": "",
                "recorded_at": "",
            }
        )
    return records


def _write_manual_external_template_files(
    records: list[dict[str, Any]],
    yaml_path: Path,
    csv_path: Path,
) -> None:
    import yaml

    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_payload = {
        "schema_version": 1,
        "instructions": [
            "Fill these records only with real external platform results.",
            "Use metric_unavailable_on_platform when a platform does not provide a metric.",
            "Do not paste internal system metrics as external platform evidence.",
        ],
        "records": records,
    }
    yaml_path.write_text(
        yaml.safe_dump(yaml_payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(MANUAL_EXTERNAL_RECORD_FIELDS))
        writer.writeheader()
        for row in records:
            writer.writerow(
                {
                    field: (
                        json.dumps(row[field], ensure_ascii=False, sort_keys=True)
                        if field == "asset_weights"
                        else row[field]
                    )
                    for field in MANUAL_EXTERNAL_RECORD_FIELDS
                }
            )


def _write_manual_external_record_input_guide(
    path: Path,
    *,
    yaml_path: Path,
    csv_path: Path,
    start_date: date,
    end_date: date | None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    end = end_date.isoformat() if end_date else "latest available internal date"
    lines = [
        "# Manual External Record Input Guide",
        "",
        "本指南用于记录 Portfolio Visualizer、testfol.io 或其他外部组合回测平台的"
        "静态 baseline 结果。只填写真实外部平台导出、截图或手工记录，不得把内部系统"
        "结果复制为外部证据。",
        "",
        f"- YAML template: `{yaml_path}`",
        f"- CSV template: `{csv_path}`",
        f"- Required date range: `{start_date.isoformat()}` to `{end}`",
        "- Required baselines: `100_qqq`, `qqq_50_sgov_50`, `qqq_60_sgov_40`",
        "- Required evidence: `screenshot_reference` or `export_file_path` must be filled.",
        "- SGOV convention must be one of `unknown`, `price_only`, `adjusted`, "
        "`total_return`, `platform_default`.",
        "",
        "## Metric Fields",
        "",
        "For `annual_return`, `max_drawdown`, `sharpe`, `calmar`, and `turnover`, enter"
        " the platform value when available. If the platform does not provide that metric,"
        f" enter `{METRIC_UNAVAILABLE_MARKER}` exactly.",
        "",
        "## Safety Boundary",
        "",
        "- `paper_shadow_allowed=false`",
        "- `production_allowed=false`",
        "- `broker_action=none`",
        "- `manual_review_required=true`",
        "",
        "Do not upload broker account information or personal account screenshots.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _manual_baseline_rows(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [_manual_baseline_definition(config, strategy_id) for strategy_id in STATIC_BASELINE_IDS]


def _manual_baseline_definition(config: Mapping[str, Any], strategy_id: str) -> dict[str, Any]:
    internal_id = STATIC_BASELINE_INTERNAL_IDS.get(strategy_id, strategy_id)
    strategy = _strategy_by_id(config, internal_id)
    weights = STATIC_BASELINE_EXPECTED_WEIGHTS.get(strategy_id, {})
    return {
        "strategy_id": strategy_id,
        "internal_strategy_id": internal_id,
        "display_name": strategy.get("display_name", strategy_id),
        "asset_weights": weights,
        "rebalance_frequency": strategy.get("rebalance_frequency", "monthly"),
        "dividend_reinvestment": "external_platform_must_confirm",
        "price_or_total_return_policy": "external_platform_must_confirm",
    }


def _write_runbook_artifact(payload: dict[str, Any], json_path: Path, docs_path: Path) -> None:
    payload["artifact_paths"] = {
        "json_path": str(json_path),
        "markdown_path": str(docs_path),
    }
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Static Baseline External Manual Runbook",
        "",
        f"- 状态：`{payload.get('status')}`",
        "- paper_shadow_allowed：`false`",
        "- production_allowed：`false`",
        "- broker_action：`none`",
        "- manual_review_required：`true`",
        "",
        "## Baselines",
        "",
        "|strategy_id|asset_weights|rebalance_frequency|",
        "|---|---|---|",
    ]
    for row in _records(payload.get("baseline_rows")):
        lines.append(
            f"|`{row.get('strategy_id')}`|`{json.dumps(row.get('asset_weights'), sort_keys=True)}`|"
            f"`{row.get('rebalance_frequency')}`|"
        )
    lines.extend(
        [
            "",
            "## Required External Platform Steps",
            "",
            "1. Run each baseline on Portfolio Visualizer, testfol.io, or another platform "
            "that supports ETF portfolio backtests.",
            "2. Match the requested date range, monthly rebalance, and dividend "
            "reinvestment setting where the platform allows it.",
            "3. Record annual return, max drawdown, Sharpe, Calmar, turnover, and monthly "
            f"returns availability. Use `{METRIC_UNAVAILABLE_MARKER}` when unavailable.",
            "4. Record SGOV handling as unknown, price_only, adjusted, total_return, or "
            "platform_default.",
            "5. Save a screenshot or export CSV and fill the manual records YAML/CSV.",
            "",
            "Do not include broker account data, personal account screenshots, real trading "
            "instructions, or production readiness claims.",
        ]
    )
    docs_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _load_manual_external_records(yaml_path: Path, csv_path: Path) -> dict[str, Any]:
    records: list[tuple[str, dict[str, Any]]] = []
    source_paths: list[str] = []
    parse_errors: list[str] = []
    if yaml_path.exists():
        source_paths.append(str(yaml_path))
        try:
            records.extend((str(yaml_path), row) for row in _read_yaml_record_list(yaml_path))
        except (OSError, ValueError) as exc:
            parse_errors.append(f"{yaml_path}:{exc}")
    if csv_path.exists():
        source_paths.append(str(csv_path))
        try:
            records.extend((str(csv_path), row) for row in _read_csv_record_list(csv_path))
        except (OSError, ValueError) as exc:
            parse_errors.append(f"{csv_path}:{exc}")
    return {"records": records, "source_paths": source_paths, "parse_errors": parse_errors}


def _read_yaml_record_list(path: Path) -> list[dict[str, Any]]:
    import yaml

    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        return []
    records = raw.get("records", raw) if isinstance(raw, Mapping) else raw
    if not isinstance(records, list):
        raise ValueError("manual external records YAML must contain a records list")
    return [dict(row) for row in records if isinstance(row, Mapping)]


def _read_csv_record_list(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _normalize_manual_external_record(
    row: Mapping[str, Any],
    *,
    source: str,
    start_date: date,
    end_date: date | None,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {field: row.get(field) for field in MANUAL_EXTERNAL_RECORD_FIELDS}
    normalized["source_path"] = source
    normalized["strategy_id"] = str(normalized.get("strategy_id") or "").strip()
    normalized["asset_weights"] = _parse_asset_weights(normalized.get("asset_weights"))
    normalized["unavailable_metrics"] = [
        metric for metric in MANUAL_METRIC_FIELDS if _metric_is_unavailable(normalized.get(metric))
    ]
    for metric in MANUAL_METRIC_FIELDS:
        normalized[f"external_{metric}"] = _manual_metric_value(normalized.get(metric))
    normalized["evidence_reference_present"] = bool(
        str(normalized.get("export_file_path") or "").strip()
        or str(normalized.get("screenshot_reference") or "").strip()
    )
    normalized["sgov_convention"] = str(
        normalized.get("price_or_total_return_policy") or "unknown"
    ).strip().lower()
    errors, warnings = _manual_record_validation_errors(
        normalized,
        start_date=start_date,
        end_date=end_date,
    )
    normalized["validation_errors"] = errors
    normalized["validation_warnings"] = warnings
    normalized["weight_match"] = not any(error.startswith("asset_weights") for error in errors)
    normalized["date_range_match"] = not any(error.startswith("date_range") for error in errors)
    normalized["metrics_complete_or_marked_unavailable"] = not any(
        error.startswith("metric") for error in errors
    )
    return normalized


def _manual_record_validation_errors(
    row: Mapping[str, Any],
    *,
    start_date: date,
    end_date: date | None,
) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    strategy_id = str(row.get("strategy_id") or "")
    if strategy_id not in STATIC_BASELINE_IDS:
        errors.append("strategy_id_not_allowed")
    expected_weights = STATIC_BASELINE_EXPECTED_WEIGHTS.get(strategy_id, {})
    if expected_weights and not _asset_weights_match(
        _mapping(row.get("asset_weights")),
        expected_weights,
    ):
        errors.append("asset_weights_do_not_match_baseline_definition")
    rebalance_frequency = str(row.get("rebalance_frequency") or "").strip().lower()
    if rebalance_frequency != "monthly" and _single_asset_rebalance_equivalent(
        rebalance_frequency,
        expected_weights,
    ):
        warnings.append("rebalance_frequency_no_rebalancing_equivalent_for_single_asset")
    elif rebalance_frequency != "monthly":
        errors.append("rebalance_frequency_must_be_monthly_for_static_baseline")
    expected_start = start_date.isoformat()
    expected_end = end_date.isoformat() if end_date else None
    actual_start = str(row.get("date_range_start") or "").strip()
    actual_end = str(row.get("date_range_end") or "").strip()
    notes = str(row.get("manual_notes") or "").lower()
    date_diff_documented = "date_range_difference" in notes or "date range difference" in notes
    if actual_start != expected_start:
        if date_diff_documented:
            warnings.append("date_range_start_diff_documented")
        else:
            errors.append("date_range_start_mismatch_without_manual_note")
    if expected_end and actual_end != expected_end:
        if date_diff_documented:
            warnings.append("date_range_end_diff_documented")
        else:
            errors.append("date_range_end_mismatch_without_manual_note")
    if not row.get("evidence_reference_present"):
        errors.append("screenshot_reference_or_export_file_path_required")
    if str(row.get("sgov_convention") or "unknown") not in SGOV_CONVENTIONS:
        errors.append("price_or_total_return_policy_invalid")
    for metric in MANUAL_METRIC_FIELDS:
        value = row.get(metric)
        if _metric_is_unavailable(value):
            continue
        if value is None or str(value).strip() == "":
            errors.append(f"metric_{metric}_blank_without_unavailable_marker")
        elif _manual_metric_value(value) is None:
            errors.append(f"metric_{metric}_not_numeric_or_unavailable_marker")
    return errors, warnings


def _parse_asset_weights(value: object) -> dict[str, float]:
    if isinstance(value, Mapping):
        return {str(key).upper(): _float(raw) for key, raw in value.items()}
    raw = str(value or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, Mapping):
        return {str(key).upper(): _float(raw_value) for key, raw_value in parsed.items()}
    weights: dict[str, float] = {}
    for chunk in raw.replace(",", ";").split(";"):
        if not chunk.strip() or ":" not in chunk:
            continue
        ticker, raw_weight = chunk.split(":", 1)
        weights[ticker.strip().upper()] = _float(raw_weight.strip())
    return weights


def _asset_weights_match(actual: Mapping[str, Any], expected: Mapping[str, float]) -> bool:
    tickers = set(actual) | set(expected)
    return all(
        abs(_float(actual.get(ticker)) - _float(expected.get(ticker))) <= 0.0001
        for ticker in tickers
    )


def _single_asset_rebalance_equivalent(
    rebalance_frequency: str,
    expected_weights: Mapping[str, float],
) -> bool:
    if rebalance_frequency not in SINGLE_ASSET_REBALANCE_EQUIVALENTS:
        return False
    non_zero_weights = [
        weight for weight in expected_weights.values() if abs(_float(weight)) > 0.0001
    ]
    return len(non_zero_weights) == 1 and abs(non_zero_weights[0] - 1.0) <= 0.0001


def _metric_is_unavailable(value: object) -> bool:
    return str(value or "").strip().lower() == METRIC_UNAVAILABLE_MARKER


def _manual_metric_value(value: object) -> float | None:
    if _metric_is_unavailable(value):
        return None
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("%"):
        return _float(raw[:-1]) / 100.0
    parsed = _float(raw, default=math.nan)
    return None if math.isnan(parsed) else parsed


def _load_metric_convention_signoff_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []
    try:
        records = _read_yaml_record_list(path)
    except (OSError, ValueError) as exc:
        return [], [f"{path}:{exc}"]
    return records, []


def _manual_external_tools(manual_input: Mapping[str, Any]) -> list[str]:
    tools = sorted(
        {
            str(row.get("external_tool") or row.get("external_tool_url_or_name") or "").strip()
            for row in _records(manual_input.get("valid_records"))
            if str(row.get("external_tool") or row.get("external_tool_url_or_name") or "").strip()
        }
    )
    return tools or ["unknown_external_tool_pending"]


def _metric_convention_rows(
    signoff_rows: list[dict[str, Any]],
    external_tools: list[str],
) -> list[dict[str, Any]]:
    keyed = {
        (str(row.get("external_tool") or ""), str(row.get("metric_name") or "")): row
        for row in signoff_rows
    }
    rows = []
    for tool in external_tools:
        for metric_name in METRIC_INTERNAL_DEFINITIONS:
            raw = keyed.get((tool, metric_name), {})
            rows.append(
                {
                    "external_tool": tool,
                    "metric_name": metric_name,
                    "platform_definition": raw.get("platform_definition", "unknown"),
                    "internal_definition": METRIC_INTERNAL_DEFINITIONS[metric_name],
                    "definition_match_status": raw.get("definition_match_status", "unknown"),
                    "manual_confirmation_status": raw.get(
                        "manual_confirmation_status", "unknown"
                    ),
                    "difference_expected": _bool(raw.get("difference_expected", False)),
                    "owner_notes": raw.get("owner_notes", ""),
                }
            )
    return rows


def _load_sgov_convention_signoff_rows(
    path: Path,
    sgov_check: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    if path.exists():
        try:
            records = _read_yaml_record_list(path)
        except (OSError, ValueError) as exc:
            return [], [f"{path}:{exc}"]
        return [_normalize_sgov_signoff_row(row, sgov_check) for row in records], []
    summary = _mapping(sgov_check.get("sgov_total_return_summary"))
    return [
        {
            "external_tool": "unknown_external_tool_pending",
            "sgov_convention": "unknown",
            "internal_sgov_convention": summary.get(
                "internal_SGOV_total_return_proxy",
                "adj_close",
            ),
            "sgov_annual_return_external": None,
            "sgov_annual_return_internal": summary.get("SGOV_adjusted_close_return"),
            "annual_return_delta": None,
            "impact_on_static_baselines": "unknown_until_owner_signoff",
            "impact_on_dynamic_strategies": "unknown_until_owner_signoff",
            "convention_accepted": False,
            "owner_notes": "manual SGOV external convention signoff not supplied",
        }
    ], []


def _normalize_sgov_signoff_row(
    row: Mapping[str, Any],
    sgov_check: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _mapping(sgov_check.get("sgov_total_return_summary"))
    external = _manual_metric_value(row.get("sgov_annual_return_external"))
    internal = _manual_metric_value(row.get("sgov_annual_return_internal"))
    if internal is None:
        internal = _float(summary.get("SGOV_adjusted_close_return"))
    delta = _manual_metric_value(row.get("annual_return_delta"))
    if delta is None and external is not None and internal is not None:
        delta = external - internal
    return {
        "external_tool": row.get("external_tool", "unknown_external_tool"),
        "sgov_convention": str(row.get("sgov_convention", "unknown")).lower(),
        "internal_sgov_convention": row.get(
            "internal_sgov_convention",
            summary.get("internal_SGOV_total_return_proxy", "adj_close"),
        ),
        "sgov_annual_return_external": external,
        "sgov_annual_return_internal": internal,
        "annual_return_delta": _round(delta),
        "impact_on_static_baselines": row.get("impact_on_static_baselines", "owner_noted"),
        "impact_on_dynamic_strategies": row.get("impact_on_dynamic_strategies", "owner_noted"),
        "convention_accepted": _bool(row.get("convention_accepted")),
        "owner_notes": row.get("owner_notes", ""),
    }


def _manual_final_reconciliation_row(
    strategy_id: str,
    internal: Mapping[str, Any],
    external: Mapping[str, Any] | None,
    *,
    metric_signoff: Mapping[str, Any],
    sgov_signoff: Mapping[str, Any],
) -> dict[str, Any]:
    if not external:
        return {
            "strategy_id": strategy_id,
            "internal_annual_return": internal.get("annual_return"),
            "external_annual_return": None,
            "annual_return_delta": None,
            "internal_max_drawdown": internal.get("max_drawdown"),
            "external_max_drawdown": None,
            "max_drawdown_delta": None,
            "internal_sharpe": internal.get("sharpe"),
            "external_sharpe": None,
            "sharpe_delta": None,
            "internal_calmar": internal.get("calmar"),
            "external_calmar": None,
            "calmar_delta": None,
            "within_tolerance": None,
            "difference_reason": "manual_external_input_missing",
        }
    deltas = {
        metric: (
            None
            if external.get(f"external_{metric}") is None
            else _round(_float(internal.get(metric)) - _float(external.get(f"external_{metric}")))
        )
        for metric in RECONCILIATION_METRIC_FIELDS
    }
    missing_metrics = [
        metric for metric in RECONCILIATION_METRIC_FIELDS if deltas.get(metric) is None
    ]
    within = (
        not missing_metrics
        and abs(_float(deltas["annual_return"])) <= METRIC_TOLERANCE["annual_return"]
        and abs(_float(deltas["max_drawdown"])) <= METRIC_TOLERANCE["max_drawdown"]
        and abs(_float(deltas["sharpe"])) <= METRIC_TOLERANCE["sharpe"]
        and abs(_float(deltas["calmar"])) <= METRIC_TOLERANCE["calmar"]
    )
    if within:
        reason = "within_tolerance"
    elif missing_metrics and str(metric_signoff.get("status")) in {
        "METRIC_CONVENTIONS_CONFIRMED",
        "METRIC_CONVENTIONS_CONFIRMED_WITH_LIMITATIONS",
    }:
        reason = "metric_unavailable_on_platform"
    elif str(sgov_signoff.get("status")) in {
        "SGOV_CONVENTION_CONFIRMED",
        "SGOV_CONVENTION_LIMITATION_ACCEPTED",
    } and "SGOV" in STATIC_BASELINE_EXPECTED_WEIGHTS.get(strategy_id, {}):
        reason = "explained_by_sgov_or_total_return_convention"
    elif str(metric_signoff.get("status")) == "METRIC_CONVENTIONS_CONFIRMED_WITH_LIMITATIONS":
        reason = "explained_by_metric_convention_limitation"
    else:
        reason = "metric_delta_exceeds_tolerance"
    return {
        "strategy_id": strategy_id,
        "internal_annual_return": internal.get("annual_return"),
        "external_annual_return": external.get("external_annual_return"),
        "annual_return_delta": deltas["annual_return"],
        "internal_max_drawdown": internal.get("max_drawdown"),
        "external_max_drawdown": external.get("external_max_drawdown"),
        "max_drawdown_delta": deltas["max_drawdown"],
        "internal_sharpe": internal.get("sharpe"),
        "external_sharpe": external.get("external_sharpe"),
        "sharpe_delta": deltas["sharpe"],
        "internal_calmar": internal.get("calmar"),
        "external_calmar": external.get("external_calmar"),
        "calmar_delta": deltas["calmar"],
        "within_tolerance": within,
        "difference_reason": reason,
    }


def _manual_owner_required_answers(
    manual_input: Mapping[str, Any],
    metric_signoff: Mapping[str, Any],
    sgov_signoff: Mapping[str, Any],
    final_reconciliation: Mapping[str, Any],
    dynamic_support: Mapping[str, Any],
    qc_preflight: Mapping[str, Any],
) -> dict[str, Any]:
    final_status = str(final_reconciliation.get("status"))
    metric_status = str(metric_signoff.get("status"))
    sgov_status = str(sgov_signoff.get("status"))
    valid_external_record_count = len(_records(manual_input.get("valid_records")))
    invalid_external_record_count = len(_records(manual_input.get("invalid_records")))
    missing_strategy_ids = list(manual_input.get("missing_strategy_ids") or [])
    return {
        "1_real_external_platform_records_provided": manual_input.get("status")
        == "MANUAL_EXTERNAL_INPUT_RECORDED",
        "1a_valid_external_record_count": valid_external_record_count,
        "1b_invalid_external_record_count": invalid_external_record_count,
        "1c_missing_static_baselines": ", ".join(str(item) for item in missing_strategy_ids),
        "2_static_baseline_aligned": final_status
        in {
            "STATIC_BASELINE_MANUAL_RECONCILED",
            "STATIC_BASELINE_MANUAL_RECONCILED_WITH_WARNINGS",
        },
        "3_sgov_difference_explained": sgov_status
        in {"SGOV_CONVENTION_CONFIRMED", "SGOV_CONVENTION_LIMITATION_ACCEPTED"},
        "4_metric_convention_confirmed": metric_status
        in {
            "METRIC_CONVENTIONS_CONFIRMED",
            "METRIC_CONVENTIONS_CONFIRMED_WITH_LIMITATIONS",
        },
        "5_unexplained_difference_remaining": final_status
        == "STATIC_BASELINE_MANUAL_MISMATCH",
        "6_still_need_quantconnect_or_tradingview_replay": dynamic_support.get("status")
        == "DYNAMIC_EXTERNAL_SUPPORT_REQUIRES_CUSTOM_ENGINE"
        and qc_preflight.get("status") == "QC_WEIGHT_PATH_PREFLIGHT_NEEDS_MANUAL_IMPLEMENTATION",
        "7_external_validation_can_be_pass_or_pass_with_warnings": final_status
        in {
            "STATIC_BASELINE_MANUAL_RECONCILED",
            "STATIC_BASELINE_MANUAL_RECONCILED_WITH_WARNINGS",
        }
        and metric_status
        in {
            "METRIC_CONVENTIONS_CONFIRMED",
            "METRIC_CONVENTIONS_CONFIRMED_WITH_LIMITATIONS",
        }
        and sgov_status
        in {"SGOV_CONVENTION_CONFIRMED", "SGOV_CONVENTION_LIMITATION_ACCEPTED"},
        "8_continue_no_paper_shadow_no_production_no_broker": True,
    }


def _manual_owner_recommendation(
    manual_input: Mapping[str, Any],
    metric_signoff: Mapping[str, Any],
    sgov_signoff: Mapping[str, Any],
    final_reconciliation: Mapping[str, Any],
    dynamic_support: Mapping[str, Any],
) -> str:
    final_status = str(final_reconciliation.get("status"))
    if str(manual_input.get("status")) != "MANUAL_EXTERNAL_INPUT_RECORDED":
        return "NEED_MORE_MANUAL_EVIDENCE"
    if str(metric_signoff.get("status")) == "METRIC_CONVENTIONS_STILL_UNKNOWN" or str(
        sgov_signoff.get("status")
    ) == "SGOV_CONVENTION_STILL_UNKNOWN":
        return "NEED_MORE_MANUAL_EVIDENCE"
    if final_status == "STATIC_BASELINE_MANUAL_MISMATCH":
        return "INTERNAL_FIX_REQUIRED"
    if final_status == "STATIC_BASELINE_MANUAL_BLOCKED":
        return "BLOCKED"
    if str(dynamic_support.get("status")) == "DYNAMIC_EXTERNAL_SUPPORT_REQUIRES_CUSTOM_ENGINE":
        return "ACCEPT_EXTERNAL_VALIDATION_WITH_WARNINGS"
    if final_status == "STATIC_BASELINE_MANUAL_RECONCILED":
        return "ACCEPT_EXTERNAL_VALIDATION"
    return "ACCEPT_EXTERNAL_VALIDATION_WITH_WARNINGS"


def _manual_master_required_answers(owner: Mapping[str, Any]) -> dict[str, Any]:
    answers = _mapping(owner.get("required_answers"))
    recommendation = str(owner.get("owner_recommendation"))
    return {
        "1_external_record_status_recorded": answers.get(
            "1_real_external_platform_records_provided"
        )
        is True,
        "2_metric_conventions_confirmed": answers.get("4_metric_convention_confirmed") is True,
        "3_sgov_convention_confirmed_or_limitation_accepted": answers.get(
            "3_sgov_difference_explained"
        )
        is True,
        "4_static_baseline_final_reconciled": answers.get("2_static_baseline_aligned") is True,
        "5_dynamic_weight_path_external_support_status": owner.get("source_statuses", {}).get(
            "dynamic_weight_path_external_support_check"
        )
        if isinstance(owner.get("source_statuses"), Mapping)
        else None,
        "6_quantconnect_weight_path_replay_needed": answers.get(
            "6_still_need_quantconnect_or_tradingview_replay"
        )
        is True,
        "7_dual_forward_aging_can_continue": recommendation
        in {"ACCEPT_EXTERNAL_VALIDATION", "ACCEPT_EXTERNAL_VALIDATION_WITH_WARNINGS"},
        "8_external_validation_warn_can_be_accepted_with_warnings": recommendation
        == "ACCEPT_EXTERNAL_VALIDATION_WITH_WARNINGS",
        "9_continue_no_paper_shadow_no_production_no_broker": True,
    }


def _internal_static_metric_rows(
    config: Mapping[str, Any],
    prices: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows = []
    qqq_returns = prices["QQQ"].pct_change().fillna(0.0)
    annualization = _research_policy_int(config, "annualization_trading_days")
    for strategy_id in STATIC_BASELINE_IDS:
        internal_strategy_id = STATIC_BASELINE_INTERNAL_IDS.get(strategy_id, strategy_id)
        strategy = _strategy_by_id(config, internal_strategy_id)
        returns = _strategy_return_series(strategy, prices, config, cost_bps=0.0)
        weights = _target_weight_frame(strategy, prices, config).reindex(returns.index).ffill()
        metrics = _metrics_for_strategy(
            strategy,
            returns,
            weights,
            qqq_returns,
            annualization=annualization,
            cost_bps=0.0,
        )
        metrics["strategy_id"] = strategy_id
        metrics["internal_strategy_id"] = internal_strategy_id
        rows.append(metrics)
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


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    return str(value or "").strip().lower() in {"true", "yes", "y", "1", "accepted"}


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
