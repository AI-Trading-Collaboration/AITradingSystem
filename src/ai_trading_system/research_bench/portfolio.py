"""P1 的组合尺子：同样的 QQQ 权重下，储备放资产还是放现金。

依据 owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler 和复盘线程需求文件
``knowledge/claude/research-review/p1-portfolio-ruler-requirements-2026-10-21.md``。
A 是 QQQ 权重 w、其余放储备的组合；B 是同平均敞口、其余放现金的固定敞口基准。
三样数是几何差、10% 下跌段里的垫底、再平衡收益；分解恒等式为：
几何差 = 储备自身超额（全窗口）+ 再平衡收益。
平时超额取下跌段外的储备对现金对数超额，段外交易日少于一半时退回整窗口。
每段 ΔD 是 A 减 B 的段内收益，C 去掉 (1-w) × 平时超额 × 月数/12，
r = C / |B 段内收益|；只有 B 段内收益低于 -2% 才计入汇总。
算法和 TRADING-2565-38 探针 ``research_probes.portfolio_ruler_check.measure`` 逐项相同。
这里只描述不判，规则在门的 ``portfolio_rules``。
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.research_bench import segments as seg
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_bench.settings import EpisodeSpec
from ai_trading_system.research_bench.simulate import Simulation

# owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler，探针原样的计入线和退回规则。
SMALL_LOSS = -0.02
QUIET_SHARE = 0.5
# 规则能写成 portfolio.<field> 的数。
PORTFOLIO_FIELDS: tuple[str, ...] = (
    "weight",
    "geometric_difference",
    "reserve_excess",
    "rebalance_return",
    "excess_annual",
    "quiet_excess",
    "quiet_days",
    "segment_count",
    "positive_count",
    "positive_share",
    "median_ratio",
    "min_ratio",
)


def geometric(series: pd.Series, sim: Simulation) -> float:
    held = series.reindex(sim.returns.index)
    if held.isna().any():
        when = pd.Timestamp(held.index[held.isna()][0]).date()
        raise ValueError(f"{series.name} has no return on {when}")
    years = (sim.returns.index[-1] - sim.start).days / DAYS_PER_YEAR
    return float(float(np.prod(1.0 + held.to_numpy(dtype=float))) ** (1.0 / years) - 1.0)


def quiet_dates(dates: pd.DatetimeIndex, declines: Sequence[EpisodeSpec]) -> np.ndarray[Any, Any]:
    quiet = np.ones(len(dates), dtype=bool)
    months = dates.to_period("M")
    for s in declines:
        quiet &= ~np.asarray(
            (months >= pd.Period(s.start, freq="M")) & (months <= pd.Period(s.end, freq="M"))
        )
    return quiet


def quiet_excess(
    reserve: pd.Series, cash: pd.Series, dates: pd.DatetimeIndex, days_per_year: int
) -> float:
    if dates.empty:
        raise ValueError("quiet excess needs return dates")
    logs = np.log1p(reserve.reindex(dates).to_numpy(dtype=float)) - np.log1p(
        cash.reindex(dates).to_numpy(dtype=float)
    )
    return math.expm1(float(logs.sum()) * days_per_year / len(dates))


def cushion_rows(
    strategy: Simulation,
    benchmark: Simulation,
    reserve: pd.Series,
    cash: pd.Series,
    weight: float,
    excess: float,
    declines: Sequence[EpisodeSpec],
) -> list[dict[str, Any]]:
    dates = strategy.returns.index
    held, cash = reserve.reindex(dates), cash.reindex(dates)
    rows = []
    for s in seg.listed("decline", declines):
        own = seg.segment_return(strategy.returns, s)
        reference = seg.segment_return(benchmark.returns, s)
        difference = own - reference
        cushion = difference - (1.0 - weight) * excess * s.months / 12.0
        counted = reference < SMALL_LOSS
        rows.append(
            {
                "id": s.id,
                "start": s.start,
                "end": s.end,
                "months": s.months,
                "strategy_return": own,
                "benchmark_return": reference,
                "difference": difference,
                "cushion": cushion,
                "ratio": cushion / abs(reference) if counted else None,
                "reserve_excess": (1.0 + seg.segment_return(held, s))
                / (1.0 + seg.segment_return(cash, s))
                - 1.0,
                "counted": counted,
            }
        )
    return rows


def cushion_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ratios = [r["ratio"] for r in rows if r["counted"]]
    positive = sum(r > 0 for r in ratios)
    return {
        "segment_count": len(ratios),
        "positive_count": positive,
        "positive_share": positive / len(ratios) if ratios else None,
        "median_ratio": float(np.median(ratios)) if ratios else None,
        "min_ratio": min(ratios) if ratios else None,
        "excluded": [r["id"] for r in rows if not r["counted"]],
    }


def portfolio_section(
    strategy: Simulation,
    benchmark: Simulation,
    strategy_summary: Mapping[str, Any],
    benchmark_summary: Mapping[str, Any],
    returns: pd.DataFrame,
    reserve: pd.Series,
    weight: float,
    declines: Sequence[EpisodeSpec],
    days_per_year: int,
) -> dict[str, Any]:
    if not declines:
        raise ValueError("the portfolio ruler needs the window's declines")
    cash = returns["cash"]
    legs = {
        "strategy": strategy_summary["geometric_annual_return"],
        "benchmark": benchmark_summary["geometric_annual_return"],
        "market": geometric(returns["qqq"], strategy),
        "reserve": geometric(reserve, strategy),
        "cash": geometric(cash, strategy),
    }
    dates = pd.DatetimeIndex(strategy.returns.index)
    quiet = quiet_dates(dates, declines)
    whole = (1.0 + legs["reserve"]) / (1.0 + legs["cash"]) - 1.0
    fallback = int(quiet.sum()) < QUIET_SHARE * len(dates)
    excess = whole if fallback else quiet_excess(reserve, cash, dates[quiet], days_per_year)
    w = weight
    rebalancing = (legs["strategy"] - (w * legs["market"] + (1.0 - w) * legs["reserve"])) - (
        legs["benchmark"] - (w * legs["market"] + (1.0 - w) * legs["cash"])
    )
    rows = cushion_rows(strategy, benchmark, reserve, cash, weight, excess, declines)
    return {
        "weight": weight,
        "legs": legs,
        "geometric_difference": legs["strategy"] - legs["benchmark"],
        "reserve_excess": (1.0 - w) * (legs["reserve"] - legs["cash"]),
        "rebalance_return": rebalancing,
        "excess_annual": whole,
        "quiet_excess": excess,
        "quiet_days": int(quiet.sum()),
        "quiet_fallback": fallback,
        "segments": rows,
        **cushion_summary(rows),
    }
