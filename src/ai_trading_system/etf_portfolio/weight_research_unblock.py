from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import (
    DataQualityReport,
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REPORT_DIR,
    ETFConfigBundle,
    load_etf_config_bundle,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_RATES_CACHE_PATH = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv"
DEFAULT_SCOPE_FREEZE_PATH = PROJECT_ROOT / "docs" / "research" / "ablation_runner_scope_freeze.json"
DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH = (
    PROJECT_ROOT / "docs" / "research" / "signal_robustness_entry_contract.json"
)
DEFAULT_HOLDOUT_POLICY_PATH = (
    PROJECT_ROOT / "docs" / "research" / "untouched_holdout_final_gate_policy.json"
)
DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "weight_research_unblock.yaml"
)
DEFAULT_WEIGHT_RESEARCH_REPORT_DIR = DEFAULT_ETF_REPORT_DIR / "weight_research"
DEFAULT_RESEARCH_SOURCE_DIR = PROJECT_ROOT / "docs" / "research"
DEFAULT_HISTORICAL_B1_RESULT_PATH = (
    DEFAULT_RESEARCH_SOURCE_DIR / "b1_execution_control_result.json"
)

EXPECTED_LAYER_SEQUENCE = ("B0", "B1", "B2", "B3", "B4", "B5", "B6")
B1_FORBIDDEN_MECHANISMS = {
    "trend_signal",
    "momentum_signal",
    "relative_strength_signal",
    "risk_scaler",
    "regime_signal",
    "confidence_shrinkage",
    "mixed_dynamic_allocation_logic",
}
REQUIRED_SIGNAL_CONTRACT_FIELDS = {
    "required_inputs",
    "required_feature_columns",
    "required_signal_series",
    "coverage_threshold",
    "stale_input_behavior",
    "schema_compatibility",
    "fail_closed_behavior",
    "allowed_warnings",
    "blocking_conditions",
}
SAFETY_BOUNDARY = {
    "research_only": True,
    "manual_review_only": True,
    "paper_shadow_activation": False,
    "official_target_weights": False,
    "broker_action_allowed": False,
    "order_ticket_generated": False,
    "owner_decision_appended": False,
    "production_effect": "none",
}


@dataclass(frozen=True)
class B1ExecutionPolicy:
    policy_id: str
    deadband_abs_weight: float
    min_benefit_cost_ratio: float
    max_daily_turnover: float
    max_single_asset_adjustment: float
    allowed_mechanisms: tuple[str, ...]
    forbidden_mechanisms: tuple[str, ...]


@dataclass(frozen=True)
class ResearchDataContext:
    generated_at: datetime
    etf_config: ETFConfigBundle
    prices: pd.DataFrame
    data_quality_report: DataQualityReport
    data_quality_output_path: Path
    etf_quality_status: str
    contract_validation: dict[str, Any]


def build_contract_validation(
    *,
    scope_path: Path = DEFAULT_SCOPE_FREEZE_PATH,
    signal_contract_path: Path = DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    holdout_policy_path: Path = DEFAULT_HOLDOUT_POLICY_PATH,
    config_path: Path = DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    layer_id: str = "B1",
    run_start: date | None = None,
    run_end: date | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    scope = _read_json(scope_path)
    signal_contract = _read_json(signal_contract_path)
    holdout_policy = _read_json(holdout_policy_path)
    config = _read_yaml_mapping(config_path)
    checks: list[dict[str, Any]] = []

    _append_scope_checks(scope, checks)
    _append_signal_contract_checks(signal_contract, checks)
    _append_holdout_checks(
        holdout_policy,
        checks,
        layer_id=layer_id,
        run_start=run_start,
        run_end=run_end,
    )
    _append_b1_policy_checks(config, checks)
    _append_safety_checks(
        {
            "scope": scope,
            "signal_contract": signal_contract,
            "holdout_policy": holdout_policy,
        },
        checks,
    )

    status = "PASS" if all(check["status"] == "PASS" for check in checks) else "FAIL"
    return {
        "schema_version": 1,
        "task_id": "TRADING-511B",
        "report_type": "weight_research_unblock_contract_validation",
        "status": status,
        "generated_at": generated.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "layer_id": layer_id,
        "run_window": (
            None
            if run_start is None or run_end is None
            else {"start_date": run_start.isoformat(), "end_date": run_end.isoformat()}
        ),
        "input_artifacts": {
            "scope_freeze": str(scope_path),
            "signal_contract": str(signal_contract_path),
            "holdout_policy": str(holdout_policy_path),
            "policy_config": str(config_path),
        },
        "checks": checks,
        "blocking_checks": [
            check["check_id"] for check in checks if check["status"] != "PASS"
        ],
        "reader_brief": {
            "summary": (
                "511A-C contracts are ready for B1 only."
                if status == "PASS"
                else "511A-C contract validation failed."
            ),
            "key_result": status,
            "blocking_issues": "none" if status == "PASS" else "See blocking_checks.",
            "warnings": "B1 remains no-signal execution-control only; B2-B6 are not authorized.",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": (
                "run B1 execution-control mini-backfill"
                if status == "PASS"
                else "repair contract blockers before B1"
            ),
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def write_contract_validation(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"contract_validation_{stamp}.json"
    md_path = output_dir / f"contract_validation_{stamp}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(render_contract_validation(payload), encoding="utf-8")
    return json_path, md_path


def render_contract_validation(payload: dict[str, Any]) -> str:
    lines = [
        "# Weight Research Unblock Contract Validation",
        "",
        f"- Status：{payload['status']}",
        f"- Layer：{payload['layer_id']}",
        f"- Generated At：{payload['generated_at']}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
        "## Checks",
        "",
        "| Check | Status | Message |",
        "|---|---|---|",
    ]
    for check in payload["checks"]:
        lines.append(f"| {check['check_id']} | {check['status']} | {_cell(check['message'])} |")
    lines.extend(
        [
            "",
            "## Reader Brief",
            "",
            f"- Summary：{payload['reader_brief']['summary']}",
            f"- Key Result：{payload['reader_brief']['key_result']}",
            f"- Blocking Issues：{payload['reader_brief']['blocking_issues']}",
            f"- Warnings：{payload['reader_brief']['warnings']}",
            f"- Safety Boundary：{payload['reader_brief']['safety_boundary']}",
            f"- Next Action：{payload['reader_brief']['next_action']}",
        ]
    )
    return "\n".join(lines) + "\n"


def build_b1_metric_semantics_audit(
    *,
    historical_b1_path: Path = DEFAULT_HISTORICAL_B1_RESULT_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    historical = _read_json(historical_b1_path)
    comparison = historical.get("b1_vs_b0_comparison", {})
    b1_metrics = historical.get("b1_metrics", {})
    b0_metrics = historical.get("b0_metrics", {})
    checks = [
        _audit_check(
            "return_delta_semantics",
            "PASS",
            (
                "absolute total-return fraction difference: "
                "B1 total_return - historical B0 total_return"
            ),
            observed_value=comparison.get("return_delta"),
        ),
        _audit_check(
            "drawdown_reduction_semantics",
            "PASS",
            "abs(B0 max_drawdown) - abs(B1 max_drawdown); positive means drawdown improved",
            observed_value=comparison.get("drawdown_reduction"),
        ),
        _audit_check(
            "turnover_delta_semantics",
            "PASS",
            "absolute cumulative-turnover difference: B1 turnover - historical B0 turnover",
            observed_value=comparison.get("turnover_delta"),
        ),
        _audit_check(
            "historical_b0_turnover_zero",
            "WARN" if _safe_float(b0_metrics.get("turnover")) == 0.0 else "PASS",
            "historical B0 reports zero turnover, so it is not a valid rebalance comparator",
            observed_value=b0_metrics.get("turnover"),
        ),
        _audit_check(
            "historical_b1_positive_turnover",
            "WARN" if _safe_float(b1_metrics.get("turnover")) > 0.0 else "PASS",
            (
                "historical B1 positive turnover can come from static target "
                "rebalancing drift, not only execution control"
            ),
            observed_value=b1_metrics.get("turnover"),
        ),
        _audit_check(
            "pure_execution_attribution",
            "WARN",
            (
                "B1 - historical B0 mixes execution/no-trade controls with the "
                "introduction of static target rebalancing"
            ),
            observed_value="requires B0R comparator",
        ),
    ]
    status = "B1_ATTRIBUTION_PARTIAL"
    if any(check["status"] == "FAIL" for check in checks):
        status = "B1_ATTRIBUTION_INVALID"
    return {
        "schema_version": 1,
        "task_id": "TRADING-511E",
        "report_type": "b1_metric_semantics_and_comparator_audit",
        "status": status,
        "generated_at": generated.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "source_artifacts": {
            "historical_b1_result": str(historical_b1_path),
            "historical_b1_status": historical.get("status"),
        },
        "metric_contract": {
            "return_delta": {
                "unit": "absolute return fraction",
                "formula": "candidate.total_return - comparator.total_return",
                "positive_direction": "candidate return is higher",
                "historical_comparator": "historical B0 field in TRADING-511D artifact",
            },
            "drawdown_reduction": {
                "unit": "absolute max-drawdown fraction",
                "formula": "abs(comparator.max_drawdown) - abs(candidate.max_drawdown)",
                "positive_direction": "candidate drawdown is smaller",
                "historical_comparator": "historical B0 field in TRADING-511D artifact",
            },
            "turnover_delta": {
                "unit": "absolute cumulative turnover",
                "formula": "candidate.turnover - comparator.turnover",
                "positive_direction": (
                    "lower is better for execution control when comparator is B0R"
                ),
                "historical_comparator": "historical B0 field in TRADING-511D artifact",
            },
        },
        "checks": checks,
        "historical_b1_usage_scope": {
            "allowed": [
                "runner smoke test",
                "research-only mixed evidence",
                "input for comparator audit",
            ],
            "forbidden": [
                "pure execution-control attribution conclusion",
                "permission to skip B0R",
                "permission to continue B2-B6 without B1 attribution rerun",
            ],
        },
        "reader_brief": {
            "summary": (
                "历史 B1 指标单位和方向可以解释，但 comparator attribution 只部分有效。"
            ),
            "key_result": status,
            "blocking_issues": "B1E 必须使用 B0R 作为 primary comparator 后才能进入有效归因。",
            "warnings": "历史 B1 - B0 不应解释为纯 execution/no-trade 模块贡献。",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "运行 B0H/B0R baseline family，然后重新运行 B1E vs B0R。",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def write_b1_metric_semantics_audit(
    payload: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"b1_metric_semantics_and_comparator_audit_{stamp}.json"
    md_path = output_dir / f"b1_metric_semantics_and_comparator_audit_{stamp}.md"
    markdown = render_b1_metric_semantics_audit(payload)
    _write_json(json_path, payload)
    md_path.write_text(markdown, encoding="utf-8")
    if alias_dir is not None:
        _write_aliases(
            alias_dir=alias_dir,
            stem="b1_metric_semantics_and_comparator_audit",
            payload=payload,
            markdown=markdown,
        )
    return json_path, md_path


def render_b1_metric_semantics_audit(payload: dict[str, Any]) -> str:
    lines = [
        "# B1 Metric Semantics And Comparator Audit",
        "",
        f"- Status：{payload['status']}",
        f"- Source：{payload['source_artifacts']['historical_b1_result']}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
        "## Metric Contract",
        "",
        "| Metric | Unit | Formula | Positive Direction |",
        "|---|---|---|---|",
    ]
    for metric, contract in payload["metric_contract"].items():
        lines.append(
            "| "
            f"{metric} | "
            f"{_cell(contract['unit'])} | "
            f"{_cell(contract['formula'])} | "
            f"{_cell(contract['positive_direction'])} |"
        )
    lines.extend(
        [
            "",
            "## Checks",
            "",
            "| Check | Status | Message | Observed |",
            "|---|---|---|---|",
        ]
    )
    for check in payload["checks"]:
        lines.append(
            "| "
            f"{check['check_id']} | "
            f"{check['status']} | "
            f"{_cell(check['message'])} | "
            f"{_cell(check.get('observed_value', ''))} |"
        )
    lines.extend(
        [
            "",
            "## Reader Brief",
            "",
            f"- Summary：{payload['reader_brief']['summary']}",
            f"- Key Result：{payload['reader_brief']['key_result']}",
            f"- Blocking Issues：{payload['reader_brief']['blocking_issues']}",
            f"- Warnings：{payload['reader_brief']['warnings']}",
            f"- Safety Boundary：{payload['reader_brief']['safety_boundary']}",
            f"- Next Action：{payload['reader_brief']['next_action']}",
        ]
    )
    return "\n".join(lines) + "\n"


def run_static_baseline_family(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    scope_path: Path = DEFAULT_SCOPE_FREEZE_PATH,
    signal_contract_path: Path = DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    holdout_policy_path: Path = DEFAULT_HOLDOUT_POLICY_PATH,
    config_path: Path = DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    generated_at: datetime | None = None,
    data_quality_output_path: Path | None = None,
    alias_dir: Path | None = None,
) -> tuple[dict[str, Any], Path, Path, Path]:
    context = _prepare_research_data_context(
        prices_path=prices_path,
        rates_path=rates_path,
        start=start,
        end=end,
        scope_path=scope_path,
        signal_contract_path=signal_contract_path,
        holdout_policy_path=holdout_policy_path,
        config_path=config_path,
        generated_at=generated_at,
        data_quality_output_path=data_quality_output_path,
    )
    if context.contract_validation["status"] != "PASS":
        payload = _blocked_phase0_payload(
            task_id="TRADING-511F",
            report_type="static_baseline_family_result",
            status="STATIC_BASELINE_FAMILY_BLOCKED",
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="contract_validation_failed",
            details=context.contract_validation["blocking_checks"],
        )
        json_path, md_path, daily_path = write_static_baseline_family_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
            alias_dir=alias_dir,
        )
        return payload, json_path, md_path, daily_path
    if not context.data_quality_report.passed:
        payload = _blocked_phase0_payload(
            task_id="TRADING-511F",
            report_type="static_baseline_family_result",
            status="STATIC_BASELINE_FAMILY_BLOCKED",
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="validate_data_failed",
            details=[context.data_quality_report.status],
        )
        json_path, md_path, daily_path = write_static_baseline_family_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
            alias_dir=alias_dir,
        )
        return payload, json_path, md_path, daily_path
    if context.etf_quality_status != "PASS":
        payload = _blocked_phase0_payload(
            task_id="TRADING-511F",
            report_type="static_baseline_family_result",
            status="STATIC_BASELINE_FAMILY_BLOCKED",
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="etf_price_quality_failed",
            details=[context.etf_quality_status],
        )
        json_path, md_path, daily_path = write_static_baseline_family_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
            alias_dir=alias_dir,
        )
        return payload, json_path, md_path, daily_path

    b0h_daily = simulate_static_baseline_path(
        prices=context.prices,
        config=context.etf_config,
        start=start,
        end=end,
        variant_id="B0H",
    )
    b0r_daily = simulate_static_baseline_path(
        prices=context.prices,
        config=context.etf_config,
        start=start,
        end=end,
        variant_id="B0R",
    )
    daily = pd.concat([b0h_daily, b0r_daily], ignore_index=True)
    payload = build_static_baseline_family_payload(
        b0h_daily=b0h_daily,
        b0r_daily=b0r_daily,
        context=context,
        start=start,
        end=end,
        prices_path=prices_path,
        config_path=config_path,
    )
    json_path, md_path, daily_path = write_static_baseline_family_result(
        payload,
        daily,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    return payload, json_path, md_path, daily_path


def simulate_static_baseline_path(
    *,
    prices: pd.DataFrame,
    config: ETFConfigBundle,
    start: date,
    end: date,
    variant_id: str,
) -> pd.DataFrame:
    if variant_id not in {"B0H", "B0R"}:
        raise ValueError("variant_id must be B0H or B0R")
    close_pivot = _price_pivot(prices, config.backtest.backtest.price_field)
    trading_dates = [item.date() for item in close_pivot.index if start <= item.date() <= end]
    if len(trading_dates) < 3:
        raise ValueError("static baseline mini-backfill requires at least three trading dates")
    target_weights = _default_weights(config)
    current_weights = dict(target_weights)
    rows: list[dict[str, Any]] = []
    signal_lag_days = int(config.backtest.backtest.signal_lag_days)
    total_cost_bps = _total_cost_bps(config)
    portfolio_equity = 1.0
    for index, signal_date in enumerate(trading_dates):
        execution_index = index + signal_lag_days
        return_index = execution_index + 1
        if return_index >= len(trading_dates):
            break
        execution_date = trading_dates[execution_index]
        return_date = trading_dates[return_index]
        if variant_id == "B0R":
            executed_delta = {
                symbol: target_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0)
                for symbol in sorted(set(target_weights) | set(current_weights))
            }
            post_trade_weights = dict(target_weights)
            decision = "REBALANCE_TO_STATIC_TARGET"
            decision_reason = "naive_deterministic_daily_rebalance"
        else:
            executed_delta = {symbol: 0.0 for symbol in sorted(current_weights)}
            post_trade_weights = dict(current_weights)
            decision = "HOLD"
            decision_reason = "natural_drift_no_rebalance"
        turnover = sum(abs(value) for value in executed_delta.values())
        period_returns = _period_returns(close_pivot, execution_date, return_date)
        gross_return = sum(
            post_trade_weights.get(symbol, 0.0) * period_returns.get(symbol, 0.0)
            for symbol in target_weights
        )
        transaction_cost = turnover * total_cost_bps / 10_000.0
        strategy_return = gross_return - transaction_cost
        portfolio_equity *= 1.0 + strategy_return
        rows.append(
            {
                "signal_date": signal_date.isoformat(),
                "execution_date": execution_date.isoformat(),
                "return_date": return_date.isoformat(),
                "market_regime": config.backtest.backtest.regime,
                "layer_id": variant_id,
                "base_layer": "B0",
                "added_mechanism": (
                    "static_hold_natural_drift_reference"
                    if variant_id == "B0H"
                    else "static_target_naive_deterministic_rebalance_reference"
                ),
                "decision": decision,
                "decision_reason": decision_reason,
                "gross_return": gross_return,
                "transaction_cost": transaction_cost,
                "strategy_return": strategy_return,
                "portfolio_equity": portfolio_equity,
                "turnover": turnover,
                "pre_trade_weights_json": json.dumps(
                    current_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "target_weights_json": json.dumps(
                    target_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "post_trade_weights_json": json.dumps(
                    post_trade_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "executed_delta_json": json.dumps(
                    executed_delta,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "period_returns_json": json.dumps(
                    period_returns,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "forbidden_logic_check": "PASS_NO_SIGNAL_ALLOCATOR_REGIME_OR_CONFIDENCE_INPUTS",
                "official_target_weights": False,
                "production_effect": "none",
            }
        )
        current_weights = _drift_weights(post_trade_weights, period_returns, gross_return)
    return pd.DataFrame(rows)


def build_static_baseline_family_payload(
    *,
    b0h_daily: pd.DataFrame,
    b0r_daily: pd.DataFrame,
    context: ResearchDataContext,
    start: date,
    end: date,
    prices_path: Path,
    config_path: Path,
) -> dict[str, Any]:
    b0h_metrics = _metrics_from_daily(b0h_daily)
    b0r_metrics = _metrics_from_daily(b0r_daily)
    comparison = _comparison_payload(b0r_metrics, b0h_metrics)
    target_path_checksum = _target_path_checksum(b0r_daily)
    run_id = f"WRP1-{context.generated_at.strftime('%Y%m%dT%H%M%SZ')}-B0H-B0R"
    return {
        "schema_version": 1,
        "task_id": "TRADING-511F",
        "report_type": "static_baseline_family_result",
        "status": "STATIC_BASELINE_FAMILY_READY_RESEARCH_ONLY",
        "generated_at": context.generated_at.isoformat(),
        "market_regime": context.etf_config.backtest.backtest.regime,
        "run_id": run_id,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "first_signal_date": str(b0r_daily.iloc[0]["signal_date"]),
        "last_signal_date": str(b0r_daily.iloc[-1]["signal_date"]),
        "row_count_per_variant": {"B0H": int(len(b0h_daily)), "B0R": int(len(b0r_daily))},
        "input_artifacts": {
            "prices_path": str(prices_path),
            "data_quality_report": str(context.data_quality_output_path),
            "contract_validation_status": context.contract_validation["status"],
            "policy_config": str(config_path),
        },
        "data_quality_gate": _data_quality_payload(
            context.data_quality_report,
            context.data_quality_output_path,
        ),
        "runtime_quality": {"etf_price_quality_status": context.etf_quality_status},
        "target_path": {
            "target_path_artifact_id": f"{run_id}-static-target-path",
            "target_path_checksum": target_path_checksum,
            "source_config_path": "config/etf_portfolio/assets.yaml#assets.default_weight",
            "same_as_b1_static_target_path": True,
        },
        "b0h_metrics": _metrics_payload(b0h_metrics),
        "b0r_metrics": _metrics_payload(b0r_metrics),
        "b0r_vs_b0h_comparison": comparison,
        "b0h_contract": {
            "semantics": "static buy-and-hold / natural drift reference",
            "uses_market_signal": False,
            "uses_rebalance_rule": False,
            "expected_turnover_source": "none after initial research starting weights",
        },
        "b0r_contract": {
            "semantics": "static target + naive deterministic rebalance reference",
            "uses_market_signal": False,
            "uses_deadband": False,
            "uses_cost_threshold": False,
            "uses_turnover_optimization": False,
            "rebalance_rule": "daily deterministic rebalance to frozen static target path",
            "expected_turnover_source": "drift back to pre-frozen static target weights",
        },
        "artifact_boundary": {
            "feature_artifact_id": f"{run_id}-price-return-feature-artifact",
            "signal_artifact_id": f"{run_id}-no-signal-artifact",
            "target_path_artifact_id": f"{run_id}-static-target-path",
            "executed_weight_artifact_id": f"{run_id}-b0h-b0r-executed-weight-path",
            "evaluation_artifact_id": f"{run_id}-b0h-b0r-evaluation",
            "run_manifest_artifact_id": f"{run_id}-run-manifest",
        },
        "holdout_accessed": False,
        "forbidden_logic_check": "PASS_NO_P0_ALLOCATOR_SIGNALS_REGIME_FEATURE_STORE_OR_CONFIDENCE",
        "reader_brief": {
            "summary": "B0H/B0R 双基准已生成；B0R 可作为 B1E 的 primary comparator。",
            "key_result": "STATIC_BASELINE_FAMILY_READY_RESEARCH_ONLY",
            "blocking_issues": "B1E attribution rerun 尚未完成前不得继续 B2/B3。",
            "warnings": "B0R 的 turnover 来自预先冻结的静态目标再平衡，不代表市场信号。",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "运行 B1E vs B0R attribution gate。",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def write_static_baseline_family_result(
    payload: dict[str, Any],
    daily: pd.DataFrame,
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"static_baseline_family_result_{stamp}.json"
    md_path = output_dir / f"static_baseline_family_result_{stamp}.md"
    daily_path = output_dir / f"static_baseline_family_daily_{stamp}.csv"
    markdown = render_static_baseline_family_result(payload)
    _write_json(json_path, payload)
    md_path.write_text(markdown, encoding="utf-8")
    daily.to_csv(daily_path, index=False)
    if alias_dir is not None:
        _write_aliases(
            alias_dir=alias_dir,
            stem="static_baseline_family_result",
            payload=payload,
            markdown=markdown,
        )
    return json_path, md_path, daily_path


def render_static_baseline_family_result(payload: dict[str, Any]) -> str:
    lines = [
        "# Static Baseline Family Result",
        "",
        f"- Status：{payload['status']}",
        f"- Range：{payload.get('requested_start')} 至 {payload.get('requested_end')}",
        f"- Data Quality：{payload.get('data_quality_gate', {}).get('status')}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
    ]
    if "b0h_metrics" in payload:
        lines.extend(
            [
                "## Metrics",
                "",
                _metric_line("B0H", payload["b0h_metrics"]),
                _metric_line("B0R", payload["b0r_metrics"]),
                "",
                "## B0R vs B0H",
                "",
            ]
        )
        for key, value in payload["b0r_vs_b0h_comparison"].items():
            lines.append(f"- {key}：{float(value):.6f}")
        lines.append("")
    lines.extend(
        [
            "## Boundary",
            "",
            (
                "- Target Path Checksum："
                f"`{payload.get('target_path', {}).get('target_path_checksum')}`"
            ),
            f"- Forbidden Logic：{payload.get('forbidden_logic_check')}",
            f"- Holdout Accessed：{str(payload.get('holdout_accessed')).lower()}",
            "",
            "## Reader Brief",
            "",
            f"- Summary：{payload['reader_brief']['summary']}",
            f"- Key Result：{payload['reader_brief']['key_result']}",
            f"- Blocking Issues：{payload['reader_brief']['blocking_issues']}",
            f"- Warnings：{payload['reader_brief']['warnings']}",
            f"- Safety Boundary：{payload['reader_brief']['safety_boundary']}",
            f"- Next Action：{payload['reader_brief']['next_action']}",
        ]
    )
    return "\n".join(lines) + "\n"


def run_b1_isolated_attribution(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    scope_path: Path = DEFAULT_SCOPE_FREEZE_PATH,
    signal_contract_path: Path = DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    holdout_policy_path: Path = DEFAULT_HOLDOUT_POLICY_PATH,
    config_path: Path = DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    generated_at: datetime | None = None,
    data_quality_output_path: Path | None = None,
    alias_dir: Path | None = None,
) -> tuple[dict[str, Any], Path, Path, Path]:
    context = _prepare_research_data_context(
        prices_path=prices_path,
        rates_path=rates_path,
        start=start,
        end=end,
        scope_path=scope_path,
        signal_contract_path=signal_contract_path,
        holdout_policy_path=holdout_policy_path,
        config_path=config_path,
        generated_at=generated_at,
        data_quality_output_path=data_quality_output_path,
    )
    if context.contract_validation["status"] != "PASS":
        payload = _blocked_phase0_payload(
            task_id="TRADING-511G",
            report_type="b1_isolated_attribution_result",
            status="B1_ATTRIBUTION_INVALID",
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="contract_validation_failed",
            details=context.contract_validation["blocking_checks"],
        )
        json_path, md_path, daily_path = write_b1_isolated_attribution_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
            alias_dir=alias_dir,
        )
        return payload, json_path, md_path, daily_path
    if not context.data_quality_report.passed:
        payload = _blocked_phase0_payload(
            task_id="TRADING-511G",
            report_type="b1_isolated_attribution_result",
            status="B1_ATTRIBUTION_INVALID",
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="validate_data_failed",
            details=[context.data_quality_report.status],
        )
        json_path, md_path, daily_path = write_b1_isolated_attribution_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
            alias_dir=alias_dir,
        )
        return payload, json_path, md_path, daily_path
    if context.etf_quality_status != "PASS":
        payload = _blocked_phase0_payload(
            task_id="TRADING-511G",
            report_type="b1_isolated_attribution_result",
            status="B1_ATTRIBUTION_INVALID",
            generated_at=context.generated_at,
            start=start,
            end=end,
            reason="etf_price_quality_failed",
            details=[context.etf_quality_status],
        )
        json_path, md_path, daily_path = write_b1_isolated_attribution_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
            alias_dir=alias_dir,
        )
        return payload, json_path, md_path, daily_path

    policy = load_b1_execution_policy(config_path)
    b1_daily = simulate_b1_execution_control(
        prices=context.prices,
        config=context.etf_config,
        policy=policy,
        start=start,
        end=end,
    )
    b0h_daily = simulate_static_baseline_path(
        prices=context.prices,
        config=context.etf_config,
        start=start,
        end=end,
        variant_id="B0H",
    )
    b0r_daily = simulate_static_baseline_path(
        prices=context.prices,
        config=context.etf_config,
        start=start,
        end=end,
        variant_id="B0R",
    )
    daily = pd.concat(
        [
            b0h_daily,
            b0r_daily,
            b1_daily.assign(layer_id="B1E", base_layer="B0R"),
        ],
        ignore_index=True,
    )
    payload = build_b1_isolated_attribution_payload(
        b1_daily=b1_daily,
        b0r_daily=b0r_daily,
        b0h_daily=b0h_daily,
        context=context,
        policy=policy,
        start=start,
        end=end,
        prices_path=prices_path,
        config_path=config_path,
    )
    json_path, md_path, daily_path = write_b1_isolated_attribution_result(
        payload,
        daily,
        output_dir=output_dir,
        alias_dir=alias_dir,
    )
    return payload, json_path, md_path, daily_path


def build_b1_isolated_attribution_payload(
    *,
    b1_daily: pd.DataFrame,
    b0r_daily: pd.DataFrame,
    b0h_daily: pd.DataFrame,
    context: ResearchDataContext,
    policy: B1ExecutionPolicy,
    start: date,
    end: date,
    prices_path: Path,
    config_path: Path,
) -> dict[str, Any]:
    b1_metrics = _metrics_from_daily(b1_daily)
    b0r_metrics = _metrics_from_daily(b0r_daily)
    b0h_metrics = _metrics_from_daily(b0h_daily)
    primary = _comparison_payload(b1_metrics, b0r_metrics)
    secondary = _comparison_payload(b1_metrics, b0h_metrics)
    b1_target_checksum = _target_path_checksum(b1_daily)
    b0r_target_checksum = _target_path_checksum(b0r_daily)
    attribution_valid = (
        b1_target_checksum == b0r_target_checksum
        and len(b1_daily) == len(b0r_daily)
        and not b1_daily.empty
    )
    status = (
        _classify_b1_attribution_status(primary)
        if attribution_valid
        else "B1_ATTRIBUTION_INVALID"
    )
    cost_saved = float(b0r_daily["transaction_cost"].sum()) - float(
        b1_daily["transaction_cost"].sum()
    )
    run_id = f"WRP1-{context.generated_at.strftime('%Y%m%dT%H%M%SZ')}-B1E-B0R"
    execution_metrics = {
        "gross_target_turnover": float(b0r_daily["turnover"].sum()),
        "executed_turnover": float(b1_daily["turnover"].sum()),
        "skipped_trades": int((b1_daily["decision"] == "NO_TRADE").sum()),
        "cost_saved": cost_saved,
        "missed_benefit_proxy": max(0.0, b0r_metrics.total_return - b1_metrics.total_return),
        "average_execution_delay": "NOT_MODELED_B1_USES_SKIP_OR_TRADE_DECISIONS",
        "constraint_hit_count": int((b1_daily["decision_reason"] == "capped_adjustment").sum()),
        "urgent_risk_action_delay": "NOT_APPLICABLE_NO_RISK_SIGNAL_IN_B1",
    }
    return {
        "schema_version": 1,
        "task_id": "TRADING-511G",
        "report_type": "b1_isolated_attribution_result",
        "status": status,
        "generated_at": context.generated_at.isoformat(),
        "market_regime": context.etf_config.backtest.backtest.regime,
        "run_id": run_id,
        "layer_id": "B1E",
        "base_layer": "B0R",
        "added_mechanism": "execution_no_trade_turnover_control_only",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "first_signal_date": str(b1_daily.iloc[0]["signal_date"]),
        "last_signal_date": str(b1_daily.iloc[-1]["signal_date"]),
        "row_count": int(len(b1_daily)),
        "input_artifacts": {
            "prices_path": str(prices_path),
            "data_quality_report": str(context.data_quality_output_path),
            "contract_validation_status": context.contract_validation["status"],
            "policy_config": str(config_path),
        },
        "data_quality_gate": _data_quality_payload(
            context.data_quality_report,
            context.data_quality_output_path,
        ),
        "runtime_quality": {"etf_price_quality_status": context.etf_quality_status},
        "policy": {
            "policy_id": policy.policy_id,
            "deadband_abs_weight": policy.deadband_abs_weight,
            "min_benefit_cost_ratio": policy.min_benefit_cost_ratio,
            "max_daily_turnover": policy.max_daily_turnover,
            "max_single_asset_adjustment": policy.max_single_asset_adjustment,
            "allowed_mechanisms": list(policy.allowed_mechanisms),
            "forbidden_mechanisms": list(policy.forbidden_mechanisms),
        },
        "target_path_validation": {
            "status": "PASS" if attribution_valid else "FAIL",
            "b1e_target_path_checksum": b1_target_checksum,
            "b0r_target_path_checksum": b0r_target_checksum,
            "same_target_path": b1_target_checksum == b0r_target_checksum,
            "same_row_count": len(b1_daily) == len(b0r_daily),
        },
        "b1e_metrics": _metrics_payload(b1_metrics),
        "b0r_metrics": _metrics_payload(b0r_metrics),
        "b0h_metrics": _metrics_payload(b0h_metrics),
        "b1e_vs_b0r_comparison": primary,
        "b1e_vs_b0h_secondary_comparison": secondary,
        "execution_metrics": execution_metrics,
        "attribution_gate": {
            "b1_attribution_valid": attribution_valid,
            "b2_b3_continuation_condition": (
                "B1_ATTRIBUTION_VALID_*" if attribution_valid else "BLOCKED"
            ),
            "b2_b3_may_continue": attribution_valid,
            "execution_default_candidate_allowed": status == "B1_ATTRIBUTION_VALID_POSITIVE",
            "e0_e1_variants_required_for_r_t": status
            in {"B1_ATTRIBUTION_VALID_MIXED", "B1_ATTRIBUTION_VALID_NEGATIVE"},
        },
        "artifact_boundary": {
            "feature_artifact_id": f"{run_id}-price-return-feature-artifact",
            "signal_artifact_id": f"{run_id}-no-signal-artifact",
            "target_path_artifact_id": f"{run_id}-static-target-path",
            "executed_weight_artifact_id": f"{run_id}-b1e-executed-weight-path",
            "evaluation_artifact_id": f"{run_id}-b1e-vs-b0r-evaluation",
            "run_manifest_artifact_id": f"{run_id}-run-manifest",
        },
        "signal_robustness_status": "NOT_APPLICABLE_B1_EXECUTION_CONTROL_NO_SIGNAL_INPUT",
        "holdout_accessed": False,
        "forbidden_logic_check": "PASS_NO_P0_ALLOCATOR_SIGNALS_REGIME_FEATURE_STORE_OR_CONFIDENCE",
        "reader_brief": {
            "summary": "B1E 已使用 B0R 作为 primary comparator 形成可审计归因。",
            "key_result": status,
            "blocking_issues": (
                "none"
                if attribution_valid
                else "B0R 与 B1E target path 不一致或 row count 不一致。"
            ),
            "warnings": (
                "若结果为 mixed/negative，E 不得默认进入最终候选，后续 R/T 必须运行 E0/E1。"
            ),
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": (
                "冻结五层接口并建立 signal diagnostics framework"
                if attribution_valid
                else "修复 B0R/B1E comparator 后重跑"
            ),
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def write_b1_isolated_attribution_result(
    payload: dict[str, Any],
    daily: pd.DataFrame,
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    alias_dir: Path | None = None,
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"b1_isolated_attribution_result_{stamp}.json"
    md_path = output_dir / f"b1_isolated_attribution_result_{stamp}.md"
    daily_path = output_dir / f"b1_isolated_attribution_daily_{stamp}.csv"
    markdown = render_b1_isolated_attribution_result(payload)
    _write_json(json_path, payload)
    md_path.write_text(markdown, encoding="utf-8")
    daily.to_csv(daily_path, index=False)
    if alias_dir is not None:
        _write_aliases(
            alias_dir=alias_dir,
            stem="b1_isolated_attribution_result",
            payload=payload,
            markdown=markdown,
        )
    return json_path, md_path, daily_path


def render_b1_isolated_attribution_result(payload: dict[str, Any]) -> str:
    lines = [
        "# B1 Isolated Attribution Result",
        "",
        f"- Status：{payload['status']}",
        f"- Range：{payload.get('requested_start')} 至 {payload.get('requested_end')}",
        f"- Data Quality：{payload.get('data_quality_gate', {}).get('status')}",
        f"- Target Path Validation：{payload.get('target_path_validation', {}).get('status')}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
    ]
    if "b1e_metrics" in payload:
        lines.extend(
            [
                "## Metrics",
                "",
                _metric_line("B1E", payload["b1e_metrics"]),
                _metric_line("B0R", payload["b0r_metrics"]),
                _metric_line("B0H", payload["b0h_metrics"]),
                "",
                "## B1E vs B0R",
                "",
            ]
        )
        for key, value in payload["b1e_vs_b0r_comparison"].items():
            lines.append(f"- {key}：{float(value):.6f}")
        lines.extend(["", "## Execution Metrics", ""])
        for key, value in payload["execution_metrics"].items():
            lines.append(f"- {key}：{value}")
        lines.append("")
    lines.extend(
        [
            "## Attribution Gate",
            "",
            (
                "- B2/B3 May Continue："
                f"{payload.get('attribution_gate', {}).get('b2_b3_may_continue')}"
            ),
            (
                "- E0/E1 Variants Required："
                f"{payload.get('attribution_gate', {}).get('e0_e1_variants_required_for_r_t')}"
            ),
            "",
            "## Reader Brief",
            "",
            f"- Summary：{payload['reader_brief']['summary']}",
            f"- Key Result：{payload['reader_brief']['key_result']}",
            f"- Blocking Issues：{payload['reader_brief']['blocking_issues']}",
            f"- Warnings：{payload['reader_brief']['warnings']}",
            f"- Safety Boundary：{payload['reader_brief']['safety_boundary']}",
            f"- Next Action：{payload['reader_brief']['next_action']}",
        ]
    )
    return "\n".join(lines) + "\n"


def run_b1_execution_control(
    *,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
    scope_path: Path = DEFAULT_SCOPE_FREEZE_PATH,
    signal_contract_path: Path = DEFAULT_SIGNAL_ROBUSTNESS_CONTRACT_PATH,
    holdout_policy_path: Path = DEFAULT_HOLDOUT_POLICY_PATH,
    config_path: Path = DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
    generated_at: datetime | None = None,
    data_quality_output_path: Path | None = None,
) -> tuple[dict[str, Any], Path, Path, Path]:
    generated = generated_at or datetime.now(UTC)
    contract_validation = build_contract_validation(
        scope_path=scope_path,
        signal_contract_path=signal_contract_path,
        holdout_policy_path=holdout_policy_path,
        config_path=config_path,
        layer_id="B1",
        run_start=start,
        run_end=end,
        generated_at=generated,
    )
    if contract_validation["status"] != "PASS":
        payload = _blocked_b1_payload(
            generated_at=generated,
            start=start,
            end=end,
            reason="contract_validation_failed",
            details=contract_validation["blocking_checks"],
        )
        json_path, md_path, daily_path = write_b1_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
        )
        return payload, json_path, md_path, daily_path

    data_quality_report, quality_output = _run_validate_data_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=generated.date(),
        output_path=data_quality_output_path,
    )
    if not data_quality_report.passed:
        payload = _blocked_b1_payload(
            generated_at=generated,
            start=start,
            end=end,
            reason="validate_data_failed",
            details=[data_quality_report.status],
        )
        json_path, md_path, daily_path = write_b1_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
        )
        return payload, json_path, md_path, daily_path

    etf_config = load_etf_config_bundle()
    prices, etf_quality = load_standard_prices(prices_path, etf_config.assets, etf_config.strategy)
    if not etf_quality.passed:
        payload = _blocked_b1_payload(
            generated_at=generated,
            start=start,
            end=end,
            reason="etf_price_quality_failed",
            details=[etf_quality.status],
        )
        json_path, md_path, daily_path = write_b1_result(
            payload,
            pd.DataFrame(),
            output_dir=output_dir,
        )
        return payload, json_path, md_path, daily_path

    policy = load_b1_execution_policy(config_path)
    daily = simulate_b1_execution_control(
        prices=prices,
        config=etf_config,
        policy=policy,
        start=start,
        end=end,
    )
    payload = build_b1_result_payload(
        daily=daily,
        config=etf_config,
        policy=policy,
        generated_at=generated,
        start=start,
        end=end,
        prices_path=prices_path,
        data_quality_report=data_quality_report,
        data_quality_output_path=quality_output,
        etf_quality_status=etf_quality.status,
        contract_validation=contract_validation,
    )
    json_path, md_path, daily_path = write_b1_result(payload, daily, output_dir=output_dir)
    return payload, json_path, md_path, daily_path


def simulate_b1_execution_control(
    *,
    prices: pd.DataFrame,
    config: ETFConfigBundle,
    policy: B1ExecutionPolicy,
    start: date,
    end: date,
) -> pd.DataFrame:
    close_pivot = _price_pivot(prices, config.backtest.backtest.price_field)
    trading_dates = [item.date() for item in close_pivot.index if start <= item.date() <= end]
    if len(trading_dates) < 3:
        raise ValueError("B1 mini-backfill requires at least three trading dates")
    target_weights = _default_weights(config)
    current_weights = dict(target_weights)
    rows: list[dict[str, Any]] = []
    signal_lag_days = int(config.backtest.backtest.signal_lag_days)
    total_cost_bps = (
        float(config.risk.transaction_costs.commission_bps)
        + float(config.risk.transaction_costs.slippage_bps)
    )
    portfolio_equity = 1.0
    b0_equity = 1.0

    for index, signal_date in enumerate(trading_dates):
        execution_index = index + signal_lag_days
        return_index = execution_index + 1
        if return_index >= len(trading_dates):
            break
        execution_date = trading_dates[execution_index]
        return_date = trading_dates[return_index]
        decision = _execution_control_decision(
            current_weights=current_weights,
            target_weights=target_weights,
            policy=policy,
            total_cost_bps=total_cost_bps,
        )
        post_trade_weights = decision["post_trade_weights"]
        period_returns = _period_returns(close_pivot, execution_date, return_date)
        gross_return = sum(
            post_trade_weights.get(symbol, 0.0) * period_returns.get(symbol, 0.0)
            for symbol in target_weights
        )
        transaction_cost = float(decision["turnover"]) * total_cost_bps / 10_000.0
        strategy_return = gross_return - transaction_cost
        b0_return = sum(
            target_weights.get(symbol, 0.0) * period_returns.get(symbol, 0.0)
            for symbol in target_weights
        )
        portfolio_equity *= 1.0 + strategy_return
        b0_equity *= 1.0 + b0_return
        rows.append(
            {
                "signal_date": signal_date.isoformat(),
                "execution_date": execution_date.isoformat(),
                "return_date": return_date.isoformat(),
                "market_regime": config.backtest.backtest.regime,
                "layer_id": "B1",
                "base_layer": "B0",
                "added_mechanism": "execution_no_trade_turnover_control_only",
                "decision": decision["decision"],
                "decision_reason": decision["reason"],
                "benefit_cost_ratio": decision["benefit_cost_ratio"],
                "gross_return": gross_return,
                "transaction_cost": transaction_cost,
                "strategy_return": strategy_return,
                "b0_return": b0_return,
                "portfolio_equity": portfolio_equity,
                "b0_equity": b0_equity,
                "turnover": decision["turnover"],
                "max_abs_drift": decision["max_abs_drift"],
                "pre_trade_weights_json": json.dumps(
                    current_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "target_weights_json": json.dumps(
                    target_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "post_trade_weights_json": json.dumps(
                    post_trade_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "executed_delta_json": json.dumps(
                    decision["executed_delta"],
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "period_returns_json": json.dumps(
                    period_returns,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "forbidden_logic_check": "PASS_NO_SIGNAL_ALLOCATOR_REGIME_OR_CONFIDENCE_INPUTS",
                "official_target_weights": False,
                "production_effect": "none",
            }
        )
        current_weights = _drift_weights(post_trade_weights, period_returns, gross_return)

    return pd.DataFrame(rows)


def build_b1_result_payload(
    *,
    daily: pd.DataFrame,
    config: ETFConfigBundle,
    policy: B1ExecutionPolicy,
    generated_at: datetime,
    start: date,
    end: date,
    prices_path: Path,
    data_quality_report: DataQualityReport,
    data_quality_output_path: Path,
    etf_quality_status: str,
    contract_validation: dict[str, Any],
) -> dict[str, Any]:
    b1_returns = [float(value) for value in daily["strategy_return"]]
    b0_returns = [float(value) for value in daily["b0_return"]]
    turnovers = [float(value) for value in daily["turnover"]]
    exposures = [
        1.0 - json.loads(str(value)).get("CASH", 0.0)
        for value in daily["post_trade_weights_json"]
    ]
    b0_exposures = [1.0 - _default_weights(config).get("CASH", 0.0)] * len(b0_returns)
    b1_metrics = summarize_long_only_backtest(b1_returns, exposures, turnovers)
    b0_metrics = summarize_long_only_backtest(b0_returns, b0_exposures, [0.0] * len(b0_returns))
    comparison = _comparison_payload(b1_metrics, b0_metrics)
    status = "B1_MINI_BACKFILL_COMPLETE_RESEARCH_ONLY"
    if comparison["return_delta"] <= 0 and comparison["drawdown_reduction"] <= 0:
        status = "B1_MINI_BACKFILL_MIXED_RESEARCH_ONLY"
    return {
        "schema_version": 1,
        "task_id": "TRADING-511D",
        "report_type": "b1_execution_control_result",
        "status": status,
        "generated_at": generated_at.isoformat(),
        "market_regime": config.backtest.backtest.regime,
        "layer_id": "B1",
        "base_layer": "B0",
        "added_mechanism": "execution_no_trade_turnover_control_only",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "first_signal_date": str(daily.iloc[0]["signal_date"]),
        "last_signal_date": str(daily.iloc[-1]["signal_date"]),
        "row_count": int(len(daily)),
        "input_artifacts": {
            "prices_path": str(prices_path),
            "data_quality_report": str(data_quality_output_path),
            "contract_validation_status": contract_validation["status"],
            "policy_config": str(DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH),
        },
        "data_quality_gate": {
            "required_command": "aits validate-data",
            "status": data_quality_report.status,
            "passed": data_quality_report.passed,
            "error_count": data_quality_report.error_count,
            "warning_count": data_quality_report.warning_count,
            "info_count": data_quality_report.info_count,
            "report_path": str(data_quality_output_path),
            "price_cache_sha256": data_quality_report.price_summary.sha256,
            "price_cache_rows": data_quality_report.price_summary.rows,
        },
        "runtime_quality": {"etf_price_quality_status": etf_quality_status},
        "policy": {
            "policy_id": policy.policy_id,
            "deadband_abs_weight": policy.deadband_abs_weight,
            "min_benefit_cost_ratio": policy.min_benefit_cost_ratio,
            "max_daily_turnover": policy.max_daily_turnover,
            "max_single_asset_adjustment": policy.max_single_asset_adjustment,
            "allowed_mechanisms": list(policy.allowed_mechanisms),
            "forbidden_mechanisms": list(policy.forbidden_mechanisms),
        },
        "b1_metrics": _metrics_payload(b1_metrics),
        "b0_metrics": _metrics_payload(b0_metrics),
        "b1_vs_b0_comparison": comparison,
        "signal_robustness_status": "NOT_APPLICABLE_B1_EXECUTION_CONTROL_NO_SIGNAL_INPUT",
        "holdout_accessed": False,
        "forbidden_logic_check": "PASS_NO_P0_ALLOCATOR_SIGNALS_REGIME_FEATURE_STORE_OR_CONFIDENCE",
        "reader_brief": {
            "summary": (
                "B1 execution-control mini-backfill completed without mixed P0 dynamic logic."
            ),
            "key_result": status,
            "blocking_issues": (
                "B2-B6 remain blocked pending independent runners and signal robustness evidence."
            ),
            "warnings": (
                "B1 is research-only and does not create official target weights "
                "or paper-shadow activation."
            ),
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": (
                "review B1 vs B0 before deciding whether B2 runner scope may be implemented"
            ),
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def write_b1_result(
    payload: dict[str, Any],
    daily: pd.DataFrame,
    *,
    output_dir: Path = DEFAULT_WEIGHT_RESEARCH_REPORT_DIR,
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = _stamp_from_generated_at(str(payload["generated_at"]))
    json_path = output_dir / f"b1_execution_control_result_{stamp}.json"
    md_path = output_dir / f"b1_execution_control_result_{stamp}.md"
    daily_path = output_dir / f"b1_execution_control_daily_{stamp}.csv"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    md_path.write_text(render_b1_result(payload), encoding="utf-8")
    daily.to_csv(daily_path, index=False)
    return json_path, md_path, daily_path


def render_b1_result(payload: dict[str, Any]) -> str:
    lines = [
        "# B1 Execution Control Result",
        "",
        f"- Status：{payload['status']}",
        f"- Range：{payload.get('requested_start')} 至 {payload.get('requested_end')}",
        f"- Data Quality：{payload.get('data_quality_gate', {}).get('status')}",
        f"- Signal Robustness：{payload.get('signal_robustness_status')}",
        f"- Holdout Accessed：{str(payload.get('holdout_accessed')).lower()}",
        f"- Production Effect：{payload['safety_boundary']['production_effect']}",
        "",
    ]
    if "b1_metrics" in payload:
        lines.extend(
            [
                "## Metrics",
                "",
                _metric_line("B1", payload["b1_metrics"]),
                _metric_line("B0", payload["b0_metrics"]),
                "",
                "## B1 vs B0",
                "",
            ]
        )
        comparison = payload["b1_vs_b0_comparison"]
        for key in [
            "return_delta",
            "cagr_delta",
            "drawdown_reduction",
            "turnover_delta",
        ]:
            lines.append(f"- {key}：{comparison[key]:.6f}")
        lines.append("")
    lines.extend(
        [
            "## Reader Brief",
            "",
            f"- Summary：{payload['reader_brief']['summary']}",
            f"- Key Result：{payload['reader_brief']['key_result']}",
            f"- Blocking Issues：{payload['reader_brief']['blocking_issues']}",
            f"- Warnings：{payload['reader_brief']['warnings']}",
            f"- Safety Boundary：{payload['reader_brief']['safety_boundary']}",
            f"- Next Action：{payload['reader_brief']['next_action']}",
        ]
    )
    return "\n".join(lines) + "\n"


def load_b1_execution_policy(
    path: Path = DEFAULT_WEIGHT_RESEARCH_UNBLOCK_CONFIG_PATH,
) -> B1ExecutionPolicy:
    raw = _read_yaml_mapping(path)
    section = raw.get("b1_execution_control")
    if not isinstance(section, dict):
        raise ValueError("weight research unblock config missing b1_execution_control")
    return _b1_execution_policy_from_mapping(section)


def _b1_execution_policy_from_mapping(section: dict[str, Any]) -> B1ExecutionPolicy:
    return B1ExecutionPolicy(
        policy_id=str(section["policy_id"]),
        deadband_abs_weight=float(section["deadband_abs_weight"]),
        min_benefit_cost_ratio=float(section["min_benefit_cost_ratio"]),
        max_daily_turnover=float(section["max_daily_turnover"]),
        max_single_asset_adjustment=float(section["max_single_asset_adjustment"]),
        allowed_mechanisms=tuple(str(item) for item in section.get("allowed_mechanisms", [])),
        forbidden_mechanisms=tuple(str(item) for item in section.get("forbidden_mechanisms", [])),
    )


def _append_scope_checks(scope: dict[str, Any], checks: list[dict[str, Any]]) -> None:
    layers = scope.get("layers") if isinstance(scope.get("layers"), list) else []
    layer_ids = [str(layer.get("layer_id")) for layer in layers if isinstance(layer, dict)]
    _check(
        checks,
        "scope_layer_sequence",
        tuple(layer_ids) == EXPECTED_LAYER_SEQUENCE,
        f"expected {EXPECTED_LAYER_SEQUENCE}, got {tuple(layer_ids)}",
    )
    by_id = {str(layer.get("layer_id")): layer for layer in layers if isinstance(layer, dict)}
    b1 = by_id.get("B1", {})
    b1_forbidden = set(str(item) for item in b1.get("forbidden_mechanisms", []))
    _check(
        checks,
        "b1_forbidden_mixed_logic",
        B1_FORBIDDEN_MECHANISMS.issubset(b1_forbidden),
        "B1 must forbid trend/momentum/relative/risk/regime/confidence/mixed logic",
    )
    global_forbidden = set(str(item) for item in scope.get("global_forbidden_substitutes", []))
    _check(
        checks,
        "p0_dynamic_strategy_forbidden",
        {"P0 dynamic strategy", "allocate_portfolio mixed allocator"}.issubset(global_forbidden),
        "mixed P0 dynamic strategy and allocator must be forbidden",
    )
    for layer_id, layer in by_id.items():
        _check(
            checks,
            f"{layer_id.lower()}_declares_added_mechanism",
            bool(str(layer.get("added_mechanism", "")).strip()),
            f"{layer_id} must declare added mechanism",
        )


def _append_signal_contract_checks(contract: dict[str, Any], checks: list[dict[str, Any]]) -> None:
    missing = sorted(REQUIRED_SIGNAL_CONTRACT_FIELDS - set(contract))
    _check(checks, "signal_contract_required_fields", not missing, f"missing={missing}")
    required_series = contract.get("required_signal_series", {})
    b1_series = required_series.get("B1") if isinstance(required_series, dict) else None
    _check(checks, "b1_has_no_signal_series", b1_series == [], "B1 must have no signal series")
    layer_rules = contract.get("layer_entry_rules", {})
    b1_rule = layer_rules.get("B1", {}) if isinstance(layer_rules, dict) else {}
    _check(
        checks,
        "b1_signal_robustness_no_signal_entry",
        b1_rule.get("signal_robustness_required") is False,
        "B1 signal robustness must be not required because it is no-signal execution control",
    )
    threshold = contract.get("coverage_threshold", {})
    coverage = threshold.get("min_symbol_date_coverage") if isinstance(threshold, dict) else None
    _check(
        checks,
        "signal_coverage_threshold_configured",
        isinstance(coverage, int | float) and 0.0 < float(coverage) <= 1.0,
        "min_symbol_date_coverage must be in (0, 1]",
    )


def _append_holdout_checks(
    holdout_policy: dict[str, Any],
    checks: list[dict[str, Any]],
    *,
    layer_id: str,
    run_start: date | None,
    run_end: date | None,
) -> None:
    window_sets = holdout_policy.get("window_sets", {})
    _check(
        checks,
        "holdout_window_sets_present",
        all(
            key in window_sets
            for key in [
                "development_windows",
                "mini_backfill_windows",
                "full_backfill_windows",
                "untouched_holdout_windows",
            ]
        )
        if isinstance(window_sets, dict)
        else False,
        "development/mini/full/untouched window sets are required",
    )
    access_policy = holdout_policy.get("holdout_access_policy", {})
    _check(
        checks,
        "early_holdout_use_fails_closed",
        isinstance(access_policy, dict)
        and access_policy.get("development_can_access_holdout") is False
        and access_policy.get("mini_backfill_can_access_holdout") is False
        and access_policy.get("full_backfill_can_access_holdout") is False
        and access_policy.get("on_early_holdout_use") == "FAIL_CLOSED",
        "holdout must be blocked before final gate",
    )
    if run_start is None or run_end is None:
        return
    overlaps = _overlapping_holdout_windows(holdout_policy, run_start, run_end)
    _check(
        checks,
        "run_window_does_not_use_holdout_early",
        not overlaps,
        f"{layer_id} run window overlaps untouched holdout: {overlaps}",
    )


def _append_b1_policy_checks(config: dict[str, Any], checks: list[dict[str, Any]]) -> None:
    metadata = config.get("policy_metadata", {})
    _check(
        checks,
        "policy_metadata_present",
        all(
            str(metadata.get(key, "")).strip()
            for key in [
                "version",
                "status",
                "owner",
                "rationale",
                "validation",
                "review_condition",
            ]
        )
        if isinstance(metadata, dict)
        else False,
        "policy metadata must include governance fields",
    )
    section = config.get("b1_execution_control")
    if not isinstance(section, dict):
        _check(checks, "b1_policy_loads", False, "missing b1_execution_control")
        return
    policy = _b1_execution_policy_from_mapping(section)
    _check(
        checks,
        "b1_policy_forbids_mixed_logic",
        B1_FORBIDDEN_MECHANISMS.issubset(set(policy.forbidden_mechanisms)),
        "B1 policy must forbid mixed logic",
    )
    _check(
        checks,
        "b1_policy_bounds",
        0 < policy.deadband_abs_weight <= 1
        and policy.min_benefit_cost_ratio >= 0
        and 0 < policy.max_daily_turnover <= 1
        and 0 < policy.max_single_asset_adjustment <= 1,
        "B1 policy numeric bounds must be valid",
    )


def _append_safety_checks(
    payloads: dict[str, dict[str, Any]],
    checks: list[dict[str, Any]],
) -> None:
    for name, payload in payloads.items():
        safety = payload.get("safety_boundary", {})
        ok = all(safety.get(key) == value for key, value in SAFETY_BOUNDARY.items())
        _check(
            checks,
            f"{name}_safety_boundary",
            ok,
            f"{name} safety boundary must match research-only contract",
        )


def _execution_control_decision(
    *,
    current_weights: dict[str, float],
    target_weights: dict[str, float],
    policy: B1ExecutionPolicy,
    total_cost_bps: float,
) -> dict[str, Any]:
    drift = {
        symbol: target_weights.get(symbol, 0.0) - current_weights.get(symbol, 0.0)
        for symbol in sorted(set(target_weights) | set(current_weights))
    }
    max_abs_drift = max((abs(value) for value in drift.values()), default=0.0)
    desired_turnover = sum(abs(value) for value in drift.values())
    estimated_cost = desired_turnover * total_cost_bps / 10_000.0
    benefit_proxy = desired_turnover
    benefit_cost_ratio = float("inf") if estimated_cost == 0 else benefit_proxy / estimated_cost
    if max_abs_drift < policy.deadband_abs_weight:
        return _decision_payload(
            decision="NO_TRADE",
            reason="inside_deadband",
            current_weights=current_weights,
            executed_delta={symbol: 0.0 for symbol in drift},
            max_abs_drift=max_abs_drift,
            benefit_cost_ratio=benefit_cost_ratio,
        )
    if benefit_cost_ratio < policy.min_benefit_cost_ratio:
        return _decision_payload(
            decision="NO_TRADE",
            reason="benefit_cost_below_threshold",
            current_weights=current_weights,
            executed_delta={symbol: 0.0 for symbol in drift},
            max_abs_drift=max_abs_drift,
            benefit_cost_ratio=benefit_cost_ratio,
        )
    scale = 1.0
    if max_abs_drift > policy.max_single_asset_adjustment:
        scale = min(scale, policy.max_single_asset_adjustment / max_abs_drift)
    if desired_turnover > policy.max_daily_turnover:
        scale = min(scale, policy.max_daily_turnover / desired_turnover)
    executed_delta = {symbol: value * scale for symbol, value in drift.items()}
    reason = "full_rebalance" if scale == 1.0 else "capped_adjustment"
    return _decision_payload(
        decision="TRADE",
        reason=reason,
        current_weights=current_weights,
        executed_delta=executed_delta,
        max_abs_drift=max_abs_drift,
        benefit_cost_ratio=benefit_cost_ratio,
    )


def _decision_payload(
    *,
    decision: str,
    reason: str,
    current_weights: dict[str, float],
    executed_delta: dict[str, float],
    max_abs_drift: float,
    benefit_cost_ratio: float,
) -> dict[str, Any]:
    post_trade = {
        symbol: current_weights.get(symbol, 0.0) + executed_delta.get(symbol, 0.0)
        for symbol in sorted(set(current_weights) | set(executed_delta))
    }
    return {
        "decision": decision,
        "reason": reason,
        "post_trade_weights": _normalize_weights(post_trade),
        "executed_delta": executed_delta,
        "turnover": sum(abs(value) for value in executed_delta.values()),
        "max_abs_drift": max_abs_drift,
        "benefit_cost_ratio": None if benefit_cost_ratio == float("inf") else benefit_cost_ratio,
    }


def _prepare_research_data_context(
    *,
    prices_path: Path,
    rates_path: Path,
    start: date,
    end: date,
    scope_path: Path,
    signal_contract_path: Path,
    holdout_policy_path: Path,
    config_path: Path,
    generated_at: datetime | None,
    data_quality_output_path: Path | None,
) -> ResearchDataContext:
    generated = generated_at or datetime.now(UTC)
    contract_validation = build_contract_validation(
        scope_path=scope_path,
        signal_contract_path=signal_contract_path,
        holdout_policy_path=holdout_policy_path,
        config_path=config_path,
        layer_id="B1",
        run_start=start,
        run_end=end,
        generated_at=generated,
    )
    data_quality_report, quality_output = _run_validate_data_gate(
        prices_path=prices_path,
        rates_path=rates_path,
        as_of=generated.date(),
        output_path=data_quality_output_path,
    )
    etf_config = load_etf_config_bundle()
    prices, etf_quality = load_standard_prices(prices_path, etf_config.assets, etf_config.strategy)
    return ResearchDataContext(
        generated_at=generated,
        etf_config=etf_config,
        prices=prices,
        data_quality_report=data_quality_report,
        data_quality_output_path=quality_output,
        etf_quality_status=etf_quality.status,
        contract_validation=contract_validation,
    )


def prepare_research_data_context(
    *,
    prices_path: Path,
    rates_path: Path,
    start: date,
    end: date,
    scope_path: Path,
    signal_contract_path: Path,
    holdout_policy_path: Path,
    config_path: Path,
    generated_at: datetime | None,
    data_quality_output_path: Path | None,
) -> ResearchDataContext:
    return _prepare_research_data_context(
        prices_path=prices_path,
        rates_path=rates_path,
        start=start,
        end=end,
        scope_path=scope_path,
        signal_contract_path=signal_contract_path,
        holdout_policy_path=holdout_policy_path,
        config_path=config_path,
        generated_at=generated_at,
        data_quality_output_path=data_quality_output_path,
    )


def _blocked_phase0_payload(
    *,
    task_id: str,
    report_type: str,
    status: str,
    generated_at: datetime,
    start: date,
    end: date,
    reason: str,
    details: list[Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": task_id,
        "report_type": report_type,
        "status": status,
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "blocking_reason": reason,
        "blocking_details": details,
        "reader_brief": {
            "summary": "Weight research phase 0 artifact blocked before producing metrics.",
            "key_result": status,
            "blocking_issues": reason,
            "warnings": "No metrics should be inferred from a blocked artifact.",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "repair blocker before rerunning phase 0.",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def _metrics_from_daily(daily: pd.DataFrame) -> BacktestMetrics:
    returns = [float(value) for value in daily["strategy_return"]]
    turnovers = [float(value) for value in daily["turnover"]]
    exposures = [
        1.0 - json.loads(str(value)).get("CASH", 0.0)
        for value in daily["post_trade_weights_json"]
    ]
    return summarize_long_only_backtest(returns, exposures, turnovers)


def _data_quality_payload(report: DataQualityReport, output_path: Path) -> dict[str, Any]:
    return {
        "required_command": "aits validate-data",
        "status": report.status,
        "passed": report.passed,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "info_count": report.info_count,
        "report_path": str(output_path),
        "price_cache_sha256": report.price_summary.sha256,
        "price_cache_rows": report.price_summary.rows,
    }


def _target_path_checksum(daily: pd.DataFrame) -> str:
    records = [
        {
            "signal_date": str(row["signal_date"]),
            "target_weights_json": str(row["target_weights_json"]),
        }
        for _, row in daily.iterrows()
    ]
    return _json_checksum(records)


def _json_checksum(payload: Any) -> str:
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return sha256(normalized.encode("utf-8")).hexdigest()


def _classify_b1_attribution_status(comparison: dict[str, float]) -> str:
    return_delta = float(comparison["return_delta"])
    drawdown_reduction = float(comparison["drawdown_reduction"])
    turnover_delta = float(comparison["turnover_delta"])
    turnover_improved = turnover_delta < 0.0
    if turnover_improved and return_delta >= 0.0 and drawdown_reduction >= 0.0:
        return "B1_ATTRIBUTION_VALID_POSITIVE"
    if turnover_improved and (return_delta >= 0.0 or drawdown_reduction >= 0.0):
        return "B1_ATTRIBUTION_VALID_MIXED"
    if turnover_improved:
        return "B1_ATTRIBUTION_VALID_NEGATIVE"
    if return_delta <= 0.0 and drawdown_reduction <= 0.0:
        return "B1_ATTRIBUTION_VALID_NEGATIVE"
    return "B1_ATTRIBUTION_VALID_MIXED"


def _audit_check(
    check_id: str,
    status: str,
    message: str,
    *,
    observed_value: Any,
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": status,
        "message": message,
        "observed_value": observed_value,
    }


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_aliases(
    *,
    alias_dir: Path,
    stem: str,
    payload: dict[str, Any],
    markdown: str,
) -> None:
    alias_dir.mkdir(parents=True, exist_ok=True)
    _write_json(alias_dir / f"{stem}.json", payload)
    (alias_dir / f"{stem}.md").write_text(markdown, encoding="utf-8")


def _total_cost_bps(config: ETFConfigBundle) -> float:
    return (
        float(config.risk.transaction_costs.commission_bps)
        + float(config.risk.transaction_costs.slippage_bps)
    )


def _run_validate_data_gate(
    *,
    prices_path: Path,
    rates_path: Path,
    as_of: date,
    output_path: Path | None,
) -> tuple[DataQualityReport, Path]:
    quality_output = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        as_of,
    )
    universe = load_universe()
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=prices_path.parent / "download_manifest.csv",
        secondary_prices_path=prices_path.parent / "prices_marketstack_daily.csv",
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(report, quality_output)
    return report, quality_output


def _blocked_b1_payload(
    *,
    generated_at: datetime,
    start: date,
    end: date,
    reason: str,
    details: list[Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "task_id": "TRADING-511D",
        "report_type": "b1_execution_control_result",
        "status": "B1_BLOCKED",
        "generated_at": generated_at.isoformat(),
        "market_regime": "ai_after_chatgpt",
        "layer_id": "B1",
        "base_layer": "B0",
        "added_mechanism": "execution_no_trade_turnover_control_only",
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "blocking_reason": reason,
        "blocking_details": details,
        "reader_brief": {
            "summary": "B1 execution-control runner blocked before producing metrics.",
            "key_result": "B1_BLOCKED",
            "blocking_issues": reason,
            "warnings": "No B1 metrics should be inferred from a blocked run.",
            "safety_boundary": (
                "research_only=true; official_target_weights=false; production_effect=none"
            ),
            "next_action": "repair blocker before rerunning B1",
        },
        "safety_boundary": dict(SAFETY_BOUNDARY),
    }


def _comparison_payload(b1: BacktestMetrics, b0: BacktestMetrics) -> dict[str, float]:
    return {
        "return_delta": b1.total_return - b0.total_return,
        "cagr_delta": b1.cagr - b0.cagr,
        "drawdown_reduction": abs(b0.max_drawdown) - abs(b1.max_drawdown),
        "sharpe_delta": (b1.sharpe or 0.0) - (b0.sharpe or 0.0),
        "turnover_delta": b1.turnover - b0.turnover,
    }


def _metrics_payload(metrics: BacktestMetrics) -> dict[str, float | None]:
    return {
        "total_return": metrics.total_return,
        "cagr": metrics.cagr,
        "max_drawdown": metrics.max_drawdown,
        "sharpe": metrics.sharpe,
        "sortino": metrics.sortino,
        "calmar": metrics.calmar,
        "time_in_market": metrics.time_in_market,
        "turnover": metrics.turnover,
    }


def _price_pivot(prices: pd.DataFrame, price_field: str) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame[price_field], errors="coerce")
    pivot = frame.pivot(index="_date", columns="symbol", values="_price").sort_index()
    return pivot.dropna(how="all")


def _period_returns(close_pivot: pd.DataFrame, left: date, right: date) -> dict[str, float]:
    left_row = close_pivot.loc[pd.Timestamp(left)]
    right_row = close_pivot.loc[pd.Timestamp(right)]
    returns: dict[str, float] = {}
    for symbol in close_pivot.columns:
        left_value = left_row.get(symbol)
        right_value = right_row.get(symbol)
        if pd.isna(left_value) or pd.isna(right_value) or float(left_value) == 0:
            continue
        returns[str(symbol)] = float(right_value) / float(left_value) - 1.0
    returns["CASH"] = 0.0
    return returns


def _drift_weights(
    weights: dict[str, float],
    period_returns: dict[str, float],
    gross_return: float,
) -> dict[str, float]:
    denominator = 1.0 + gross_return
    if denominator <= 0:
        return dict(weights)
    drifted = {
        symbol: weight * (1.0 + period_returns.get(symbol, 0.0)) / denominator
        for symbol, weight in weights.items()
    }
    return _normalize_weights(drifted)


def _normalize_weights(weights: dict[str, float]) -> dict[str, float]:
    total = sum(float(value) for value in weights.values())
    if total == 0:
        return dict(weights)
    return {symbol: float(value) / total for symbol, value in weights.items()}


def _default_weights(config: ETFConfigBundle) -> dict[str, float]:
    return {symbol: float(asset.default_weight) for symbol, asset in config.assets.assets.items()}


def _overlapping_holdout_windows(
    holdout_policy: dict[str, Any],
    run_start: date,
    run_end: date,
) -> list[str]:
    window_sets = holdout_policy.get("window_sets", {})
    holdouts = (
        window_sets.get("untouched_holdout_windows", [])
        if isinstance(window_sets, dict)
        else []
    )
    overlaps: list[str] = []
    for item in holdouts:
        if not isinstance(item, dict):
            continue
        start_value = item.get("start_date")
        end_value = item.get("end_date")
        if not start_value or not end_value:
            continue
        holdout_start = date.fromisoformat(str(start_value))
        holdout_end = date.fromisoformat(str(end_value))
        if run_start <= holdout_end and holdout_start <= run_end:
            overlaps.append(str(item.get("window_id", "unknown_holdout")))
    return overlaps


def _requires_marketstack_prices(prices_path: Path) -> bool:
    try:
        return prices_path.resolve() == DEFAULT_ETF_PRICE_PATH.resolve()
    except OSError:
        return prices_path == DEFAULT_ETF_PRICE_PATH


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError(f"expected YAML mapping: {path}")
    return raw


def _check(checks: list[dict[str, Any]], check_id: str, passed: bool, message: str) -> None:
    checks.append(
        {"check_id": check_id, "status": "PASS" if passed else "FAIL", "message": message}
    )


def _stamp_from_generated_at(value: str) -> str:
    return value.replace("-", "").replace(":", "").split(".")[0].replace("+0000", "Z")


def _cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def _metric_line(label: str, metrics: dict[str, Any]) -> str:
    return (
        f"- {label} Total Return：{float(metrics['total_return']):.2%}；"
        f"CAGR：{float(metrics['cagr']):.2%}；"
        f"Max Drawdown：{float(metrics['max_drawdown']):.2%}；"
        f"Turnover：{float(metrics['turnover']):.4f}"
    )
