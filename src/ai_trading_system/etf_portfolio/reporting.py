from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.etf_portfolio.models import ETFConfigBundle, ETFQualityReport
from ai_trading_system.etf_portfolio.no_lookahead import (
    raise_for_no_lookahead_violations,
    validate_daily_brief_no_lookahead,
)

AI_REGIME_ANCHOR_DATE = date(2022, 11, 30)
AI_REGIME_START_DATE = date(2022, 12, 1)

# These bands mirror the allocation reason-code policy in allocation.py; they
# explain existing decisions and do not introduce a new allocation threshold.
DRIVER_SUPPORT_SCORE = 60.0
DRIVER_WEAK_SCORE = 45.0
MAX_DRIVER_COUNT = 3


def render_daily_brief(
    *,
    run_date: date,
    config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    signals: pd.DataFrame,
    regime: pd.Series,
    allocation: pd.DataFrame,
    previous_allocation: pd.DataFrame | None = None,
    simulation_summary: str | None = None,
) -> str:
    regime_name = str(regime["regime"])
    action = _suggested_action(regime_name, allocation)
    main_change = _main_change(allocation)
    previous = _previous_weights(previous_allocation)
    lines = [
        f"# AITradingSystem Daily Portfolio Brief - {run_date.isoformat()}",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- production_effect = none",
        "- manual_review_only = true",
        "- no broker action",
        "- 本简报只解释 ETF target weights；不会写 production weights 或触发 broker。",
        "",
        "## 1. Executive Summary",
        "",
        f"- Regime: {regime_name}",
        f"- Suggested Action: {action}",
        f"- Main Change: {main_change}",
        f"- Market Regime Window: {config.backtest.backtest.regime}",
        f"- AI Regime Anchor: ChatGPT public launch {AI_REGIME_ANCHOR_DATE.isoformat()}",
        f"- AI Regime Start: {AI_REGIME_START_DATE.isoformat()}",
        f"- Requested Date: {run_date.isoformat()}",
        f"- Data Quality: {quality_report.status}",
        f"- Model Version: {config.strategy.model.version}",
        f"- Config Hash: `{config.config_hash}`",
        "- Safety Status: observe_only=true; production_effect=none; "
        "manual_review_only=true; no broker action",
        "- Actionability: decision-support only; manual review required.",
        "",
        "## 2. Market Regime",
        "",
        f"- Current regime: {regime_name}",
        f"- Regime score: {float(regime['regime_score']):.2f}",
        f"- Requested date: {run_date.isoformat()}",
        f"- Configured market regime: {config.backtest.backtest.regime}",
        f"- Default AI-cycle conclusion window starts: {AI_REGIME_START_DATE.isoformat()}",
        f"- Reason codes: {_reason_codes(regime.get('reason_codes'))}",
        "",
        "## 3. ETF Signal Dashboard",
        "",
        "| Symbol | Trend | Momentum | Relative Strength | Risk | Composite | "
        "Direction | Reason Codes |",
        "|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for _, row in signals.sort_values("symbol").iterrows():
        lines.append(
            "| "
            f"{row['symbol']} | "
            f"{float(row['trend_score']):.1f} | "
            f"{float(row['momentum_score']):.1f} | "
            f"{float(row['relative_strength_score']):.1f} | "
            f"{float(row['risk_score']):.1f} | "
            f"{float(row['composite_score']):.1f} | "
            f"{row['direction']} |"
            f" {_reason_codes(row.get('reason_codes'))} |"
        )
    lines.extend(
        [
            "",
            "## 4. Target Weights",
            "",
            "| Symbol | Previous Target | New Target | Delta | Reason |",
            "|---|---:|---:|---:|---|",
        ]
    )
    for _, row in allocation.sort_values("symbol").iterrows():
        symbol = str(row["symbol"])
        previous_weight = _previous_weight(symbol, row, previous)
        target = _optional_float(row.get("target_weight")) or 0.0
        delta = _weight_delta(row, previous_weight)
        lines.append(
            "| "
            f"{symbol} | "
            f"{_pct(previous_weight)} | "
            f"{target:.1%} | "
            f"{_pct(delta)} | "
            f"{_reason_codes(row.get('reason_codes'))} |"
        )
    lines.extend(
        [
            "",
            "## Weight Change Explanation",
            "",
            "| Symbol | Target Weight | Weight Delta | Top Positive Drivers | "
            "Top Negative Drivers | Constraints Applied | Explanation |",
            "|---|---:|---:|---|---|---|---|",
            *_weight_change_rows(
                allocation,
                signals=signals,
                previous=previous,
                regime_name=regime_name,
                config=config,
            ),
            "",
            "## 5. Risk Constraints",
            "",
            f"- Equity cap: {config.risk.regime_constraints[regime_name].equity_cap:.0%}",
            f"- Cash minimum: {config.risk.regime_constraints[regime_name].cash_min:.0%}",
            f"- Semiconductor cap: "
            f"{config.risk.regime_constraints[regime_name].semiconductor_cap:.0%}",
            f"- Max single asset weight: "
            f"{config.risk.portfolio_constraints.max_single_asset_weight:.0%}",
            f"- Min rebalance delta: {config.strategy.model.min_rebalance_delta:.1%}",
            f"- Max rebalance trade weight: "
            f"{config.risk.portfolio_constraints.max_rebalance_trade_weight:.0%}",
            f"- Max daily turnover: {config.risk.portfolio_constraints.max_daily_turnover:.0%}",
            f"- Constraints applied: {_constraints(allocation)}",
            "",
            "### Constraint Diagnostics",
            "",
            *_constraint_diagnostics_rows(allocation),
            "",
            "## 6. Relative Strength",
            "",
            *_relative_strength_rows(signals),
            "",
            "## Benchmark Context",
            "",
            *_benchmark_context_rows(config),
            "",
            "## 7. Simulation Performance",
            "",
            f"- Simulation status: {_simulation_status(simulation_summary)}",
            simulation_summary or "模拟舱暂无足够 forward window 评估。",
            "",
            "## P2/Live Candidate-Only Note",
            "",
            *_p2_live_boundary_rows(config),
            "",
            "## Actionability Note",
            "",
            "- Actionability note: 本报告是 ETF decision-support explanation，"
            "不是交易指令。",
            "- 人工复核重点：确认 data quality、regime、drivers、constraints、"
            "benchmark context 和 simulation status 后再决定是否记录人工意见。",
            "- Broker boundary: no broker action；不得把 target weights 当成 order ticket。",
            "- Decision sections exclude delayed evaluation outcomes；Simulation Performance "
            "仅用于事后观察。",
            "",
            "## 8. Main Risks",
            "",
            *_main_risks(regime_name, allocation, quality_report),
            "",
            "## 9. Action Checklist",
            "",
            f"- Rebalance required: {_rebalance_required(allocation)}",
            f"- Trades above threshold: {_trades_above_threshold(allocation)}",
            "- Observe-only notes: 本报告为 decision-support output，不是自动实盘下单指令。",
        ]
    )
    markdown = "\n".join(lines) + "\n"
    raise_for_no_lookahead_violations(validate_daily_brief_no_lookahead(markdown))
    return markdown


def write_daily_brief(markdown: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")
    return output_path


def _suggested_action(regime: str, allocation: pd.DataFrame) -> str:
    if regime == "Risk-Off":
        return "Reduce Risk"
    if _rebalance_required(allocation) == "yes":
        return "Rebalance"
    return "Hold"


def _main_change(allocation: pd.DataFrame) -> str:
    if "trade_delta" not in allocation:
        return "No previous target"
    deltas = allocation.dropna(subset=["trade_delta"]).copy()
    if deltas.empty:
        return "No previous target"
    deltas["_abs_delta"] = deltas["trade_delta"].astype(float).abs()
    row = deltas.sort_values("_abs_delta", ascending=False).iloc[0]
    if float(row["_abs_delta"]) <= 1e-12:
        return "No target weight change"
    return f"{row['symbol']} {float(row['trade_delta']):+.1%}"


def _reason_codes(value: object) -> str:
    if _is_missing(value):
        return "n/a"
    parsed = _maybe_json(value)
    if isinstance(parsed, list):
        return ", ".join(str(item) for item in parsed)
    return str(parsed)


def _constraints(allocation: pd.DataFrame) -> str:
    values: list[str] = []
    for value in allocation.get("constraints_applied", []):
        values.extend(_json_list(value))
    return ", ".join(sorted(set(values))) if values else "none"


def _weight_change_rows(
    allocation: pd.DataFrame,
    *,
    signals: pd.DataFrame,
    previous: dict[str, float],
    regime_name: str,
    config: ETFConfigBundle,
) -> list[str]:
    rows: list[str] = []
    signals_by_symbol = _signal_lookup(signals)
    for _, row in allocation.sort_values("symbol").iterrows():
        symbol = str(row["symbol"])
        signal = signals_by_symbol.get(symbol, {})
        previous_weight = _previous_weight(symbol, row, previous)
        target = _optional_float(row.get("target_weight")) or 0.0
        delta = _weight_delta(row, previous_weight)
        positive = _top_positive_drivers(
            symbol=symbol,
            signal=signal,
            target=target,
            delta=delta,
            regime_name=regime_name,
            config=config,
        )
        negative = _top_negative_drivers(
            symbol=symbol,
            signal=signal,
            target=target,
            delta=delta,
            regime_name=regime_name,
            row=row,
            config=config,
        )
        rows.append(
            "| "
            f"{symbol} | "
            f"{target:.1%} | "
            f"{_pct(delta)} | "
            f"{_driver_cell(positive)} | "
            f"{_driver_cell(negative)} | "
            f"{_cell(_constraint_context_for_symbol(symbol, row, config))} | "
            f"{_cell(_change_explanation(delta, config.strategy.model.min_rebalance_delta))} |"
        )
    return rows


def _signal_lookup(signals: pd.DataFrame) -> dict[str, dict[str, object]]:
    return {str(row["symbol"]): dict(row) for _, row in signals.iterrows()}


def _top_positive_drivers(
    *,
    symbol: str,
    signal: Mapping[str, object],
    target: float,
    delta: float | None,
    regime_name: str,
    config: ETFConfigBundle,
) -> list[str]:
    drivers: list[str] = []
    constraints = config.risk.regime_constraints[regime_name]
    if delta is not None and delta > 0:
        drivers.append(f"Target increased {_pct(delta)}")
    if symbol == "CASH":
        if target > 0:
            drivers.append("Cash absorbs unallocated or risk-controlled weight")
        if constraints.cash_min > 0:
            drivers.append(f"{regime_name} cash minimum is {constraints.cash_min:.0%}")
        return _limit_drivers(drivers, "No positive allocation driver; cash is residual buffer")

    composite = _optional_float(signal.get("composite_score"))
    if composite is not None and composite >= DRIVER_SUPPORT_SCORE:
        drivers.append(f"CompositeScore {composite:.1f} in support band")
    for field, label in (
        ("trend_score", "TrendScore"),
        ("momentum_score", "MomentumScore"),
        ("relative_strength_score", "RelativeStrengthScore"),
        ("risk_score", "RiskScore"),
    ):
        score = _optional_float(signal.get(field))
        if score is not None and score >= DRIVER_SUPPORT_SCORE:
            drivers.append(f"{label} {score:.1f} supports allocation")
    if regime_name == "Risk-On":
        drivers.append(f"Regime Risk-On permits equity cap {constraints.equity_cap:.0%}")
    elif constraints.equity_cap >= target:
        drivers.append(f"{regime_name} still permits target within equity cap")
    return _limit_drivers(drivers, "No strong positive driver; target follows policy baseline")


def _top_negative_drivers(
    *,
    symbol: str,
    signal: Mapping[str, object],
    target: float,
    delta: float | None,
    regime_name: str,
    row: pd.Series,
    config: ETFConfigBundle,
) -> list[str]:
    drivers: list[str] = []
    constraints = config.risk.regime_constraints[regime_name]
    if delta is not None and delta < 0:
        drivers.append(f"Target reduced {_pct(delta)}")
    if symbol == "CASH":
        if target == 0:
            drivers.append("No cash target after risk allocation")
        return _limit_drivers(drivers, "No major negative driver for cash buffer")

    composite = _optional_float(signal.get("composite_score"))
    if composite is not None:
        if composite < DRIVER_WEAK_SCORE:
            drivers.append(f"CompositeScore {composite:.1f} in underweight band")
        elif composite < DRIVER_SUPPORT_SCORE:
            drivers.append(f"CompositeScore {composite:.1f} is neutral, not overweight")
    for field, label in (
        ("trend_score", "TrendScore"),
        ("momentum_score", "MomentumScore"),
        ("relative_strength_score", "RelativeStrengthScore"),
        ("risk_score", "RiskScore"),
    ):
        score = _optional_float(signal.get(field))
        if score is not None and score < DRIVER_WEAK_SCORE:
            drivers.append(f"{label} {score:.1f} weakens allocation")
    if regime_name != "Risk-On":
        drivers.append(f"{regime_name} equity cap {constraints.equity_cap:.0%} limits risk")
    constraint_context = _constraint_context_for_symbol(symbol, row, config)
    if constraint_context != "none":
        drivers.append(f"Constraint context: {constraint_context}")
    return _limit_drivers(drivers, "No major negative driver")


def _limit_drivers(drivers: list[str], empty_text: str) -> list[str]:
    if not drivers:
        return [empty_text]
    deduped: list[str] = []
    seen: set[str] = set()
    for driver in drivers:
        if driver in seen:
            continue
        seen.add(driver)
        deduped.append(driver)
    return deduped[:MAX_DRIVER_COUNT]


def _driver_cell(drivers: list[str]) -> str:
    return "<br>".join(_cell(driver) for driver in drivers)


def _change_explanation(delta: float | None, threshold: float) -> str:
    if delta is None:
        return "No previous target available; first visible decision row."
    if abs(delta) < threshold:
        return (
            f"Weight held because absolute delta {_pct(abs(delta))} is below "
            f"min_rebalance_delta {threshold:.1%}."
        )
    if delta > 0:
        return "Weight increased after score, regime, and constraints were applied."
    return "Weight decreased after score, regime, and constraints were applied."


def _constraint_context_for_symbol(
    symbol: str,
    row: pd.Series,
    config: ETFConfigBundle,
) -> str:
    relevant_scopes = {symbol, "portfolio"}
    asset = config.assets.assets.get(symbol)
    if asset is not None:
        relevant_scopes.add(asset.risk_group)
    if symbol != "CASH":
        relevant_scopes.add("equity")
    else:
        relevant_scopes.add("CASH")
    diagnostics = []
    for diagnostic in _parse_constraint_diagnostics(row.get("constraint_diagnostics")):
        if str(diagnostic.get("asset_or_sleeve")) in relevant_scopes:
            diagnostics.append(str(diagnostic.get("constraint_id")))
    if diagnostics:
        return ", ".join(sorted(set(diagnostics)))
    constraints = _json_list(row.get("constraints_applied"))
    return ", ".join(sorted(set(constraints))) if constraints else "none"


def _constraint_diagnostics_rows(allocation: pd.DataFrame) -> list[str]:
    diagnostics = _constraint_diagnostics(allocation)
    if not diagnostics:
        constraints = _constraints(allocation)
        if constraints != "none":
            return [
                "- Constraint diagnostics: unavailable in allocation artifact; "
                f"constraints_applied={constraints}. Rerun ETF allocation/daily run "
                "to refresh structured diagnostics."
            ]
        return ["- Constraint diagnostics: none"]
    rows = [
        "| Constraint | Asset/Sleeve | Before | After | Severity | Reason |",
        "|---|---|---:|---:|---|---|",
    ]
    for diagnostic in diagnostics:
        before = _optional_float(diagnostic.get("before_weight"))
        after = _optional_float(diagnostic.get("after_weight"))
        rows.append(
            "| "
            f"{_cell(diagnostic.get('constraint_id'))} | "
            f"{_cell(diagnostic.get('asset_or_sleeve'))} | "
            f"{_pct(before)} | "
            f"{_pct(after)} | "
            f"{_cell(diagnostic.get('severity', 'info'))} | "
            f"{_cell(diagnostic.get('reason'))} |"
        )
    return rows


def _constraint_diagnostics(allocation: pd.DataFrame) -> list[dict[str, object]]:
    diagnostics: list[dict[str, object]] = []
    seen: set[tuple[object, object, object, object]] = set()
    for value in allocation.get("constraint_diagnostics", []):
        for diagnostic in _parse_constraint_diagnostics(value):
            key = (
                diagnostic.get("constraint_id"),
                diagnostic.get("asset_or_sleeve"),
                diagnostic.get("before_weight"),
                diagnostic.get("after_weight"),
            )
            if key in seen:
                continue
            seen.add(key)
            diagnostics.append(diagnostic)
    return diagnostics


def _parse_constraint_diagnostics(value: object) -> list[dict[str, object]]:
    parsed = _maybe_json(value)
    if isinstance(parsed, Mapping):
        return [dict(parsed)]
    if isinstance(parsed, list):
        return [dict(item) for item in parsed if isinstance(item, Mapping)]
    return []


def _benchmark_context_rows(config: ETFConfigBundle) -> list[str]:
    settings = config.backtest.backtest
    primary_id = settings.primary_benchmark_id
    primary = settings.benchmarks.get(primary_id)
    primary_name = "UNKNOWN" if primary is None else primary.name
    primary_type = "UNKNOWN" if primary is None else primary.benchmark_type
    benchmark_set = ", ".join(
        f"{benchmark_id}={benchmark.name}"
        for benchmark_id, benchmark in sorted(settings.benchmarks.items())
    )
    return [
        f"- Primary Benchmark: {primary_id} ({primary_name})",
        f"- Primary Benchmark Type: {primary_type}",
        f"- Benchmark Set: {benchmark_set or 'none'}",
        "- Benchmark context is read-only comparison context; the daily brief does "
        "not rerun backtests or convert benchmark history into a trade instruction.",
    ]


def _simulation_status(simulation_summary: str | None) -> str:
    if not simulation_summary:
        return "INSUFFICIENT_EVALUATION_WINDOW"
    if "暂无" in simulation_summary or "未找到 ledger" in simulation_summary:
        return "LIMITED"
    if "evaluation_records=0" in simulation_summary or "20d hit rate=n/a" in simulation_summary:
        return "PENDING_EVALUATION"
    return "EVALUATION_AVAILABLE"


def _p2_live_boundary_rows(config: ETFConfigBundle) -> list[str]:
    if config.p2 is None:
        return [
            "- P2/live candidate-only note: P2 config is absent; no live interface enabled.",
            "- observe_only = true; production_effect = none; no broker action",
        ]
    p2 = config.p2
    return [
        "- P2/live candidate-only note: candidate_only = true; observe_only = true; "
        "production_effect = none",
        f"- ML ranking candidate_only: {str(p2.ml_ranking.candidate_only).lower()}",
        f"- Weight optimizer candidate_only: "
        f"{str(p2.weight_optimizer.candidate_only).lower()}",
        f"- Ensemble candidate_only: {str(p2.ensemble.candidate_only).lower()}",
        f"- Live interface enabled: {str(p2.live_interface.enabled).lower()}",
        f"- Live interface read_only: {str(p2.live_interface.read_only).lower()}",
        f"- broker_routing_allowed: "
        f"{str(p2.live_interface.broker_routing_allowed).lower()}",
        "- Boundary: manual review only; no broker action.",
    ]


def _relative_strength_rows(signals: pd.DataFrame) -> list[str]:
    rows = []
    for _, row in signals.sort_values("symbol").iterrows():
        rows.append(
            f"- {row['symbol']}: relative strength score "
            f"{float(row['relative_strength_score']):.1f}"
        )
    return rows


def _main_risks(
    regime: str,
    allocation: pd.DataFrame,
    quality_report: ETFQualityReport,
) -> list[str]:
    risks = [f"- Trend risk: current regime is {regime}."]
    cash = allocation.loc[allocation["symbol"] == "CASH", "target_weight"]
    cash_weight = float(cash.iloc[0]) if not cash.empty else 0.0
    risks.append(f"- Volatility risk: cash buffer is {cash_weight:.1%}.")
    semiconductor_weight = allocation.loc[
        allocation["symbol"].isin(["SMH", "SOXX"]),
        "target_weight",
    ].astype(float).sum()
    risks.append(f"- Concentration risk: semiconductor sleeve is {semiconductor_weight:.1%}.")
    risks.append(f"- Data risk: data quality status is {quality_report.status}.")
    risks.append("- Event risk: P0 only reports basic event-risk placeholder.")
    return risks


def _previous_weights(previous_allocation: pd.DataFrame | None) -> dict[str, float]:
    if previous_allocation is None or previous_allocation.empty:
        return {}
    return {
        str(row["symbol"]): float(row["target_weight"])
        for _, row in previous_allocation.iterrows()
        if pd.notna(row.get("target_weight"))
    }


def _previous_weight(
    symbol: str,
    row: pd.Series,
    previous: dict[str, float],
) -> float | None:
    if symbol in previous:
        return previous[symbol]
    return _optional_float(row.get("previous_weight"))


def _weight_delta(row: pd.Series, previous_weight: float | None) -> float | None:
    trade_delta = _optional_float(row.get("trade_delta"))
    if trade_delta is not None:
        return trade_delta
    target = _optional_float(row.get("target_weight"))
    if target is None or previous_weight is None:
        return None
    return target - previous_weight


def _rebalance_required(allocation: pd.DataFrame) -> str:
    if "trade_delta" not in allocation:
        return "no"
    deltas = pd.to_numeric(allocation["trade_delta"], errors="coerce").dropna().abs()
    return "yes" if not deltas.empty and (deltas >= 0.02).any() else "no"


def _trades_above_threshold(allocation: pd.DataFrame) -> str:
    if "trade_delta" not in allocation:
        return "none"
    rows = []
    for _, row in allocation.iterrows():
        try:
            delta = abs(float(row["trade_delta"]))
        except (TypeError, ValueError):
            continue
        if delta >= 0.02:
            rows.append(f"{row['symbol']} {float(row['trade_delta']):+.1%}")
    return ", ".join(rows) if rows else "none"


def _pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.1%}"


def _json_list(value: object) -> list[str]:
    parsed = _maybe_json(value)
    if _is_missing(parsed):
        return []
    if isinstance(parsed, list | tuple):
        return [str(item) for item in parsed]
    return [str(parsed)]


def _maybe_json(value: object) -> object:
    if _is_missing(value):
        return None
    if isinstance(value, Mapping | list | tuple):
        return value
    text = str(value).strip()
    if not text or text[0] not in "[{":
        return value
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return value


def _optional_float(value: object) -> float | None:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip().lower() in {"", "nan", "nat", "none", "null"}:
        return True
    if isinstance(value, Mapping | list | tuple):
        return False
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    try:
        return bool(result)
    except (TypeError, ValueError):
        return False


def _cell(value: object) -> str:
    if _is_missing(value):
        return "n/a"
    return str(value).replace("|", "/").replace("\n", " ").strip()
