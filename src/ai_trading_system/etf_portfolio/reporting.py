from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.etf_portfolio.models import ETFConfigBundle, ETFQualityReport


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
    lines = [
        f"# AITradingSystem Daily Portfolio Brief - {run_date.isoformat()}",
        "",
        "## 1. Executive Summary",
        "",
        f"- Regime: {regime_name}",
        f"- Suggested Action: {action}",
        f"- Main Change: {main_change}",
        f"- Market Regime Window: {config.backtest.backtest.regime}",
        f"- Requested Date: {run_date.isoformat()}",
        f"- Data Quality: {quality_report.status}",
        f"- Model Version: {config.strategy.model.version}",
        f"- Config Hash: `{config.config_hash}`",
        "",
        "## 2. Market Regime",
        "",
        f"- Current regime: {regime_name}",
        f"- Regime score: {float(regime['regime_score']):.2f}",
        f"- Reason codes: {_reason_codes(regime.get('reason_codes'))}",
        "",
        "## 3. ETF Signal Dashboard",
        "",
        "| Symbol | Trend | Momentum | Relative Strength | Risk | Composite | Direction |",
        "|---|---:|---:|---:|---:|---:|---|",
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
    previous = _previous_weights(previous_allocation)
    for _, row in allocation.sort_values("symbol").iterrows():
        symbol = str(row["symbol"])
        previous_weight = previous.get(symbol)
        target = float(row["target_weight"])
        delta = None if previous_weight is None else target - previous_weight
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
            "## 5. Risk Constraints",
            "",
            f"- Equity cap: {config.risk.regime_constraints[regime_name].equity_cap:.0%}",
            f"- Cash minimum: {config.risk.regime_constraints[regime_name].cash_min:.0%}",
            f"- Semiconductor cap: "
            f"{config.risk.regime_constraints[regime_name].semiconductor_cap:.0%}",
            f"- Constraints triggered: {_constraints(allocation)}",
            "",
            "## 6. Relative Strength",
            "",
            *_relative_strength_rows(signals),
            "",
            "## 7. Simulation Performance",
            "",
            simulation_summary or "模拟舱暂无足够 forward window 评估。",
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
    return "\n".join(lines) + "\n"


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
    return f"{row['symbol']} {float(row['trade_delta']):+.1%}"


def _reason_codes(value: object) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return str(value)
    if isinstance(parsed, list):
        return ", ".join(str(item) for item in parsed)
    return str(parsed)


def _constraints(allocation: pd.DataFrame) -> str:
    values: list[str] = []
    for value in allocation.get("constraints_applied", []):
        if pd.isna(value):
            continue
        try:
            parsed = json.loads(str(value))
        except json.JSONDecodeError:
            parsed = [str(value)]
        values.extend(str(item) for item in parsed)
    return ", ".join(sorted(set(values))) if values else "none"


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
