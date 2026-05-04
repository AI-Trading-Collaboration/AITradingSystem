from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd

from ai_trading_system.backtest.daily import DEFAULT_BENCHMARK_TICKERS, BacktestRegimeContext
from ai_trading_system.benchmark_policy import (
    BenchmarkPolicyReport,
    render_benchmark_policy_summary_section,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import DataQualityReport

DEFAULT_OUTCOME_HORIZONS = (1, 5, 20, 60, 120)
DEFAULT_DECISION_OUTCOMES_PATH = PROJECT_ROOT / "data" / "processed" / "decision_outcomes.csv"


@dataclass(frozen=True)
class DecisionOutcomeBuildResult:
    as_of: date
    snapshots: tuple[dict[str, Any], ...]
    outcome_rows: tuple[dict[str, Any], ...]
    available_rows: tuple[dict[str, Any], ...]
    pending_rows: tuple[dict[str, Any], ...]
    missing_rows: tuple[dict[str, Any], ...]
    horizons: tuple[int, ...]
    strategy_ticker: str
    benchmark_tickers: tuple[str, ...]
    market_regime: BacktestRegimeContext | None
    data_quality_report: DataQualityReport
    benchmark_policy_report: BenchmarkPolicyReport | None = None


def default_decision_calibration_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"decision_calibration_{as_of.isoformat()}.md"


def load_decision_snapshots(input_path: Path) -> tuple[dict[str, Any], ...]:
    paths = _snapshot_paths(input_path)
    snapshots: list[dict[str, Any]] = []
    for path in paths:
        snapshots.append(json.loads(path.read_text(encoding="utf-8")))
    return tuple(sorted(snapshots, key=lambda item: item.get("signal_date", "")))


def build_decision_outcomes(
    *,
    snapshots: tuple[dict[str, Any], ...],
    prices: pd.DataFrame,
    as_of: date,
    horizons: tuple[int, ...] = DEFAULT_OUTCOME_HORIZONS,
    strategy_ticker: str = "SMH",
    benchmark_tickers: tuple[str, ...] = DEFAULT_BENCHMARK_TICKERS,
    market_regime: BacktestRegimeContext | None = None,
    data_quality_report: DataQualityReport,
    benchmark_policy_report: BenchmarkPolicyReport | None = None,
) -> DecisionOutcomeBuildResult:
    if not horizons:
        raise ValueError("至少需要一个 outcome 观察窗口")
    if any(horizon <= 0 for horizon in horizons):
        raise ValueError("outcome 观察窗口必须为正整数交易日")
    tickers = tuple(dict.fromkeys((strategy_ticker, *benchmark_tickers)))
    close_pivot = _prepare_close_pivot(prices, tickers)

    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        signal_date = date.fromisoformat(snapshot["signal_date"])
        for horizon in horizons:
            rows.append(
                _outcome_row(
                    snapshot=snapshot,
                    signal_date=signal_date,
                    close_pivot=close_pivot,
                    as_of=as_of,
                    horizon=horizon,
                    strategy_ticker=strategy_ticker,
                    benchmark_tickers=benchmark_tickers,
                )
            )

    available = tuple(row for row in rows if row["outcome_status"] == "AVAILABLE")
    pending = tuple(row for row in rows if row["outcome_status"] == "PENDING")
    missing = tuple(row for row in rows if row["outcome_status"] == "MISSING_DATA")
    return DecisionOutcomeBuildResult(
        as_of=as_of,
        snapshots=snapshots,
        outcome_rows=tuple(rows),
        available_rows=available,
        pending_rows=pending,
        missing_rows=missing,
        horizons=horizons,
        strategy_ticker=strategy_ticker,
        benchmark_tickers=benchmark_tickers,
        market_regime=market_regime,
        data_quality_report=data_quality_report,
        benchmark_policy_report=benchmark_policy_report,
    )


def write_decision_outcomes_csv(
    result: DecisionOutcomeBuildResult,
    output_path: Path = DEFAULT_DECISION_OUTCOMES_PATH,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pd.DataFrame(result.outcome_rows)
    if output_path.exists():
        existing = pd.read_csv(output_path)
        required = {"snapshot_id", "horizon_days"}
        missing = required - set(existing.columns)
        if missing:
            raise ValueError(
                "existing decision outcome file is missing columns: "
                f"{', '.join(sorted(missing))}"
            )
        current_keys = set(zip(new_frame["snapshot_id"], new_frame["horizon_days"], strict=True))
        existing = existing.loc[
            [
                (snapshot_id, horizon) not in current_keys
                for snapshot_id, horizon in zip(
                    existing["snapshot_id"],
                    existing["horizon_days"],
                    strict=True,
                )
            ]
        ]
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    sort_columns = [
        column
        for column in ("signal_date", "snapshot_id", "horizon_days")
        if column in new_frame.columns
    ]
    if sort_columns:
        new_frame = new_frame.sort_values(sort_columns).reset_index(drop=True)
    new_frame.to_csv(output_path, index=False)
    return output_path


def render_decision_calibration_report(
    result: DecisionOutcomeBuildResult,
    outcomes_path: Path,
    data_quality_report_path: Path,
) -> str:
    available_count = len(result.available_rows)
    sample_status = "PASS" if available_count >= 30 else "PASS_WITH_LIMITATIONS"
    lines = [
        "# 决策结果校准报告",
        "",
        f"- 状态：{sample_status}",
        f"- 生成日期：{result.as_of.isoformat()}",
        f"- 市场阶段：{_market_regime_summary(result.market_regime)}",
        f"- 决策快照数：{len(result.snapshots)}",
        f"- outcome 行数：{len(result.outcome_rows)}",
        f"- 可用观察：{available_count}",
        f"- 等待窗口完成：{len(result.pending_rows)}",
        f"- 缺失价格数据：{len(result.missing_rows)}",
        f"- 观察窗口：{', '.join(f'{horizon}D' for horizon in result.horizons)}",
        f"- AI proxy：{result.strategy_ticker}",
        f"- 对比基准：{', '.join(result.benchmark_tickers)}",
        f"- 基准政策状态：{_benchmark_policy_status(result)}",
        f"- 数据质量状态：{result.data_quality_report.status}",
        f"- 数据质量报告：`{data_quality_report_path}`",
        f"- 机器可读 outcome：`{outcomes_path}`",
        "- 治理边界：本报告只能进入规则复核和人工评估，不能自动修改生产评分、"
        "`position_gate` 或仓位建议。",
        "",
        "## 样本限制",
        "",
        _sample_limitation_text(result),
        "",
        "## 全局摘要",
        "",
        _summary_table(result.available_rows, result.benchmark_tickers),
        "",
    ]
    benchmark_policy_section = render_benchmark_policy_summary_section(
        result.benchmark_policy_report
    )
    if benchmark_policy_section:
        lines.extend([benchmark_policy_section.rstrip(), ""])
    lines.extend(
        [
            "## 分桶校准",
            "",
        ]
    )
    bucket_specs = (
        ("总分分桶", "score_bucket"),
        ("置信度分桶", "confidence_level"),
        ("仓位闸门状态", "gate_state"),
        ("Thesis 状态", "thesis_status"),
        ("风险等级", "risk_level"),
        ("估值状态", "valuation_state"),
    )
    for title, column in bucket_specs:
        lines.extend([f"### {title}", "", _bucket_table(result.available_rows, column), ""])
    return "\n".join(lines).rstrip() + "\n"


def write_decision_calibration_report(
    result: DecisionOutcomeBuildResult,
    outcomes_path: Path,
    data_quality_report_path: Path,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_decision_calibration_report(
            result,
            outcomes_path=outcomes_path,
            data_quality_report_path=data_quality_report_path,
        ),
        encoding="utf-8",
    )
    return output_path


def _outcome_row(
    *,
    snapshot: dict[str, Any],
    signal_date: date,
    close_pivot: pd.DataFrame,
    as_of: date,
    horizon: int,
    strategy_ticker: str,
    benchmark_tickers: tuple[str, ...],
) -> dict[str, Any]:
    base = _snapshot_base_record(snapshot, horizon)
    if strategy_ticker not in close_pivot.columns:
        return {
            **base,
            "outcome_status": "MISSING_DATA",
            "outcome_reason": f"缺少 AI proxy 价格：{strategy_ticker}",
        }
    series = close_pivot[strategy_ticker].dropna()
    if signal_date not in set(series.index.date):
        return {
            **base,
            "outcome_status": "MISSING_DATA",
            "outcome_reason": f"signal_date 无 AI proxy 收盘价：{signal_date.isoformat()}",
        }
    start_index = _date_position(series, signal_date)
    end_index = start_index + horizon
    if end_index >= len(series):
        return {
            **base,
            "outcome_status": "PENDING",
            "outcome_reason": "价格历史尚未覆盖完整观察窗口",
            "available_through": series.index[-1].date().isoformat(),
        }
    end_date = series.index[end_index].date()
    if end_date > as_of:
        return {
            **base,
            "outcome_status": "PENDING",
            "outcome_reason": "观察窗口结束日在本次 as_of 之后",
            "available_through": as_of.isoformat(),
        }

    window = series.iloc[start_index : end_index + 1]
    strategy_return = _window_return(window)
    row = {
        **base,
        "outcome_status": "AVAILABLE",
        "outcome_reason": "",
        "outcome_start_date": signal_date.isoformat(),
        "outcome_end_date": end_date.isoformat(),
        "ai_proxy_ticker": strategy_ticker,
        "ai_proxy_return": strategy_return,
        "ai_proxy_max_drawdown": _max_drawdown(window),
        "ai_proxy_realized_volatility": _realized_volatility(window),
        "hit": strategy_return > 0,
    }
    for benchmark in benchmark_tickers:
        benchmark_return = _benchmark_window_return(
            close_pivot,
            benchmark,
            signal_date,
            end_date,
        )
        row[f"{benchmark}_return"] = benchmark_return
        row[f"excess_{benchmark}_return"] = (
            None if benchmark_return is None else strategy_return - benchmark_return
        )
        row[f"excess_{benchmark}_hit"] = (
            None if benchmark_return is None else strategy_return > benchmark_return
        )
    return row


def _snapshot_base_record(snapshot: dict[str, Any], horizon: int) -> dict[str, Any]:
    scores = snapshot.get("scores", {})
    positions = snapshot.get("positions", {})
    quality = snapshot.get("quality", {})
    risk_state = snapshot.get("risk_event_state") or {}
    valuation_state = snapshot.get("valuation_state") or {}
    triggered_gates = [
        gate
        for gate in positions.get("position_gates", [])
        if gate.get("triggered") and gate.get("gate_id") != "score_model"
    ]
    return {
        "snapshot_id": snapshot.get("snapshot_id"),
        "signal_date": snapshot.get("signal_date"),
        "horizon_days": horizon,
        "market_regime_id": (snapshot.get("market_regime") or {}).get("regime_id"),
        "overall_score": scores.get("overall_score"),
        "score_bucket": _score_bucket(scores.get("overall_score")),
        "confidence_score": scores.get("confidence_score"),
        "confidence_level": scores.get("confidence_level"),
        "final_risk_asset_ai_min": (
            positions.get("final_risk_asset_ai_band") or {}
        ).get("min_position"),
        "final_risk_asset_ai_max": (
            positions.get("final_risk_asset_ai_band") or {}
        ).get("max_position"),
        "triggered_gate_count": len(triggered_gates),
        "triggered_gate_ids": ",".join(str(gate.get("gate_id")) for gate in triggered_gates),
        "gate_state": "extra_gate_triggered" if triggered_gates else "score_model_only",
        "thesis_status": _manual_review_status(snapshot, "交易 thesis"),
        "risk_level": _snapshot_risk_level(risk_state),
        "risk_score_eligible_active_count": risk_state.get("score_eligible_active_count"),
        "valuation_state": valuation_state.get("status") or "not_connected",
        "valuation_crowded_count": _valuation_crowded_count(valuation_state),
        "market_data_status": quality.get("market_data_status"),
        "feature_status": quality.get("feature_status"),
        "belief_state_path": (
            (snapshot.get("belief_state_ref") or {}).get("path")
            if snapshot.get("belief_state_ref")
            else None
        ),
    }


def _prepare_close_pivot(prices: pd.DataFrame, tickers: tuple[str, ...]) -> pd.DataFrame:
    required = {"date", "ticker", "adj_close"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"价格数据缺少必需字段：{', '.join(sorted(missing))}")
    frame = prices.loc[prices["ticker"].isin(tickers)].copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["_date"].notna() & frame["_adj_close"].notna()].copy()
    if frame.empty:
        raise ValueError("价格数据中没有 outcome 所需 ticker")
    return (
        frame.pivot_table(
            index="_date",
            columns="ticker",
            values="_adj_close",
            aggfunc="last",
        )
        .sort_index()
        .ffill()
    )


def _benchmark_window_return(
    close_pivot: pd.DataFrame,
    ticker: str,
    start_date: date,
    end_date: date,
) -> float | None:
    if ticker not in close_pivot.columns:
        return None
    series = close_pivot[ticker].dropna()
    if start_date not in set(series.index.date) or end_date not in set(series.index.date):
        return None
    window = series.iloc[_date_position(series, start_date) : _date_position(series, end_date) + 1]
    return _window_return(window)


def _date_position(series: pd.Series, value: date) -> int:
    matches = series.index[series.index.date == value]
    if len(matches) == 0:
        raise ValueError(f"date not found in series: {value.isoformat()}")
    return int(series.index.get_loc(matches[0]))


def _window_return(window: pd.Series) -> float:
    return float(window.iloc[-1] / window.iloc[0] - 1.0)


def _max_drawdown(window: pd.Series) -> float:
    running_max = window.cummax()
    drawdowns = window / running_max - 1.0
    return float(drawdowns.min())


def _realized_volatility(window: pd.Series) -> float:
    returns = window.pct_change().dropna()
    if len(returns) < 2:
        return 0.0
    return float(returns.std(ddof=1) * (252 ** 0.5))


def _snapshot_paths(input_path: Path) -> tuple[Path, ...]:
    if input_path.is_file():
        return (input_path,)
    if input_path.is_dir():
        return tuple(sorted(input_path.glob("decision_snapshot_*.json")))
    raise FileNotFoundError(f"decision snapshot path not found: {input_path}")


def _score_bucket(score: object) -> str:
    if score is None:
        return "unknown"
    value = float(score)
    if value >= 80:
        return "80_100"
    if value >= 65:
        return "65_80"
    if value >= 50:
        return "50_65"
    if value >= 35:
        return "35_50"
    return "0_35"


def _manual_review_status(snapshot: dict[str, Any], review_name: str) -> str:
    for item in snapshot.get("manual_review", []):
        if item.get("name") == review_name:
            return str(item.get("status") or "unknown")
    return "not_connected"


def _snapshot_risk_level(risk_state: dict[str, Any]) -> str:
    order = {"L3": 3, "L2": 2, "L1": 1}
    levels = [
        str(item.get("level"))
        for item in risk_state.get("items", [])
        if item.get("status") == "active" and item.get("score_eligible")
    ]
    if not levels:
        return "none"
    return max(levels, key=lambda level: order.get(level, 0))


def _valuation_crowded_count(valuation_state: dict[str, Any]) -> int:
    return sum(
        1
        for item in valuation_state.get("items", [])
        if item.get("health") in {"EXPENSIVE_OR_CROWDED", "EXTREME_OVERHEATED"}
    )


def _sample_limitation_text(result: DecisionOutcomeBuildResult) -> str:
    limitations: list[str] = []
    if len(result.available_rows) < 30:
        limitations.append(
            "- 可用 outcome 少于 30 行，只能作为早期校准观察，不能解释为稳定统计结论。"
        )
    if result.pending_rows:
        limitations.append(
            f"- 有 {len(result.pending_rows)} 行观察窗口尚未完成，长窗口结论会滞后。"
        )
    if result.missing_rows:
        limitations.append(
            f"- 有 {len(result.missing_rows)} 行缺失价格数据，相关快照不会进入分桶统计。"
        )
    limitations.append("- 1D/5D/20D/60D/120D 窗口可能重叠，不能当作独立样本。")
    limitations.append("- 校准发现只能进入规则复核；生产规则变更必须走 `GOV-001`。")
    return "\n".join(limitations)


def _benchmark_policy_status(result: DecisionOutcomeBuildResult) -> str:
    if result.benchmark_policy_report is None:
        return "未连接"
    return result.benchmark_policy_report.status


def _summary_table(rows: tuple[dict[str, Any], ...], benchmark_tickers: tuple[str, ...]) -> str:
    if not rows:
        return "暂无可用 outcome。"
    return _group_table(rows, "horizon_days", benchmark_tickers)


def _bucket_table(rows: tuple[dict[str, Any], ...], column: str) -> str:
    if not rows:
        return "暂无可用 outcome。"
    return _group_table(rows, column, ("SPY", "QQQ", "SMH", "SOXX"))


def _group_table(
    rows: tuple[dict[str, Any], ...],
    group_column: str,
    benchmark_tickers: tuple[str, ...],
) -> str:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get(group_column, "unknown")), []).append(row)

    preferred_benchmarks = [
        ticker
        for ticker in ("SPY", "QQQ", "SMH", "SOXX")
        if ticker in benchmark_tickers
    ]
    lines = [
        (
            "| 分组 | 样本数 | 平均 AI proxy return | 中位 AI proxy return | "
            "胜率 | 平均最大回撤 | 平均实现波动 | 平均超额收益 |"
        ),
        "|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for group, group_rows in sorted(groups.items()):
        returns = _float_values(group_rows, "ai_proxy_return")
        drawdowns = _float_values(group_rows, "ai_proxy_max_drawdown")
        vols = _float_values(group_rows, "ai_proxy_realized_volatility")
        hit_rate = sum(bool(row.get("hit")) for row in group_rows) / len(group_rows)
        excess_text = "；".join(
            f"{ticker} {_format_pct(_mean(_float_values(group_rows, f'excess_{ticker}_return')))}"
            for ticker in preferred_benchmarks
            if _float_values(group_rows, f"excess_{ticker}_return")
        )
        if not excess_text:
            excess_text = "无"
        lines.append(
            "| "
            f"{group} | "
            f"{len(group_rows)} | "
            f"{_format_pct(_mean(returns))} | "
            f"{_format_pct(median(returns) if returns else None)} | "
            f"{hit_rate:.0%} | "
            f"{_format_pct(_mean(drawdowns))} | "
            f"{_format_pct(_mean(vols))} | "
            f"{excess_text} |"
        )
    return "\n".join(lines)


def _float_values(rows: list[dict[str, Any]], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(column)
        if value is None or pd.isna(value):
            continue
        values.append(float(value))
    return values


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _format_pct(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value:.2%}"


def _market_regime_summary(market_regime: BacktestRegimeContext | None) -> str:
    if market_regime is None:
        return "未指定"
    return (
        f"{market_regime.regime_id}（{market_regime.name}，"
        f"anchor={market_regime.anchor_date.isoformat()}，"
        f"start={market_regime.start_date.isoformat()}）"
    )
