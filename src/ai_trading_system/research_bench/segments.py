"""慢熊段和长牛段的只读表，从不是门的规则。

划段规则从慢熊探针（TRADING-2565-34）原样搬来，依据
owner_decision:TRADING-2565:2026-10-21:slow_bear_readonly：月末累计水平跌至少 20%，
高点到谷底至少 6 个月为慢熊，否则为快跌；下跌段外超过 36 个月为长牛。
门按开发窗口列段，运行报告策略及其同平均敞口基准的段内表现；不进指纹、不进规则。

下跌段（decline）用同一算法把开段线改成 10%，不设最少月数，是 P1 组合尺子的单元。
依据 owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler，TRADING-2565-38 探针原样；
组合尺子怎么用它们见 ``research_bench.portfolio``。
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_bench.settings import EpisodeSpec, Window
from ai_trading_system.research_bench.simulate import Simulation

# Descriptive probe rules, preserved by the owner decision above; never acceptance thresholds.
DROP = 0.20
# owner_decision:TRADING-2565:2026-10-21:p1_portfolio_ruler，TRADING-2565-38 探针原样。
DECLINE_LINE = 0.10
SLOW_MONTHS = 6
LONG_BULL_MONTHS = 36


@dataclass(frozen=True)
class Segment:
    id: str
    kind: str
    start: str
    end: str
    peak: str | None
    trough: str | None
    months: int
    change: float | None


def window_returns(returns: pd.Series, window: Window) -> pd.Series:
    dates = pd.DatetimeIndex(returns.index)
    inside = returns.loc[
        (dates >= pd.Timestamp(window.start))
        & (True if window.end is None else dates <= pd.Timestamp(window.end))
    ]
    if len(inside) < 2:
        raise ValueError(f"{window.window_id}: no return dates inside window")
    # simulate's first available date is the initial holding date, not a return date.
    daily = inside.iloc[1:]
    if daily.isna().any():
        when = pd.Timestamp(daily.index[daily.isna()][0]).date()
        raise ValueError(f"missing return on {when}")
    return daily


def monthly_level(daily: pd.Series) -> pd.Series:
    return (1.0 + daily).groupby(pd.DatetimeIndex(daily.index).to_period("M")).prod().cumprod()


def _drops(level: pd.Series, line: float) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    values = level.to_numpy(dtype=float)
    begin = 0
    while begin < len(level):
        peak = begin
        crossed = False
        for pos in range(begin + 1, len(level)):
            if values[pos] >= values[peak]:
                peak = pos
            elif values[pos] <= values[peak] * (1.0 - line):
                crossed = True
                break
        if not crossed:
            break
        recovery = next(
            (i for i in range(peak + 1, len(level)) if values[i] >= values[peak]), len(level)
        )
        trough = peak + 1 + int(np.argmin(values[peak + 1 : recovery]))
        out.append((peak, trough))
        begin = trough + 1
    return out


def _segment(level: pd.Series, kind: str, peak: int, trough: int) -> Segment:
    values = level.to_numpy(dtype=float)
    months = pd.PeriodIndex(level.index, freq="M")
    return Segment(
        f"{kind}_{months[peak].strftime('%Y%m')}",
        kind,
        str(months[peak] + 1),
        str(months[trough]),
        str(months[peak]),
        str(months[trough]),
        months[trough].ordinal - months[peak].ordinal,
        float(values[trough] / values[peak] - 1.0),
    )


def drop_segments(
    level: pd.Series, line: float = DROP, slow_months: int = SLOW_MONTHS
) -> list[Segment]:
    months = pd.PeriodIndex(level.index, freq="M")
    return [
        _segment(
            level,
            "slow_bear"
            if months[trough].ordinal - months[peak].ordinal >= slow_months
            else "fast_drop",
            peak,
            trough,
        )
        for peak, trough in _drops(level, line)
    ]


def decline_segments(level: pd.Series, line: float = DECLINE_LINE) -> list[Segment]:
    return [_segment(level, "decline", peak, trough) for peak, trough in _drops(level, line)]


def declines(daily: pd.Series) -> list[Segment]:
    return decline_segments(monthly_level(daily))


def long_bull_segments(level: pd.Series, drops: Sequence[Segment]) -> list[Segment]:
    if level.empty:
        return []
    months = pd.PeriodIndex(level.index, freq="M")
    begin = months[0]
    gaps: list[tuple[pd.Period, pd.Period]] = []
    for drop in drops:
        gaps.append((begin, pd.Period(drop.peak, freq="M")))
        begin = pd.Period(drop.trough, freq="M") + 1
    gaps.append((begin, months[-1]))
    out = []
    for first, last in gaps:
        duration = last.ordinal - first.ordinal + 1
        if duration <= LONG_BULL_MONTHS:
            continue
        prior = level.loc[months < first]
        base = 1.0 if prior.empty else float(prior.iloc[-1])
        out.append(
            Segment(
                f"long_bull_{first.strftime('%Y%m')}",
                "long_bull",
                str(first),
                str(last),
                None,
                None,
                duration,
                float(level.loc[str(last)]) / base - 1.0,
            )
        )
    return out


def segments(daily: pd.Series) -> list[Segment]:
    level = monthly_level(daily)
    drops = drop_segments(level)
    return sorted([*drops, *long_bull_segments(level, drops)], key=lambda item: item.start)


def _inside(returns: pd.Series, segment: Segment) -> pd.Series:
    months = pd.DatetimeIndex(returns.index).to_period("M")
    return returns.loc[
        (months >= pd.Period(segment.start, freq="M"))
        & (months <= pd.Period(segment.end, freq="M"))
    ]


def segment_return(returns: pd.Series, segment: Segment) -> float:
    daily = _inside(returns, segment)
    if daily.empty:
        raise ValueError(f"{segment.id}: no return days")
    return float(np.prod(1.0 + daily.to_numpy(dtype=float))) - 1.0


def annual_return(returns: pd.Series, segment: Segment, start: pd.Timestamp) -> float:
    daily = _inside(returns, segment)
    total = segment_return(returns, segment)
    prior = returns.loc[returns.index < daily.index[0]]
    begin = start if prior.empty else pd.Timestamp(prior.index[-1])
    years = (pd.Timestamp(daily.index[-1]) - begin).days / DAYS_PER_YEAR
    if years <= 0:
        raise ValueError(f"{segment.id}: no positive calendar span")
    return float((1.0 + total) ** (1.0 / years) - 1.0)


def segment_rows(
    strategy: Simulation, benchmark: Simulation, items: Sequence[Segment]
) -> list[dict[str, Any]]:
    rows = []
    for item in items:
        own = segment_return(strategy.returns, item)
        reference = segment_return(benchmark.returns, item)
        row = {
            "id": item.id,
            "kind": item.kind,
            "strategy_return": own,
            "benchmark_return": reference,
            "difference": own - reference,
            "average_exposure": float(_inside(strategy.exposure, item).mean()),
        }
        if item.kind == "long_bull":
            own_annual = annual_return(strategy.returns, item, strategy.start)
            reference_annual = annual_return(benchmark.returns, item, benchmark.start)
            row.update(
                strategy_annual=own_annual,
                benchmark_annual=reference_annual,
                annual_difference=own_annual - reference_annual,
            )
        rows.append(row)
    return rows


def slow_bear_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    values = [r["difference"] for r in rows if r["kind"] == "slow_bear"]
    return {
        "count": len(values),
        "median_difference": float(np.median(values)) if values else None,
        "positive_share": sum(v > 0 for v in values) / len(values) if values else None,
        "min_difference": min(values) if values else None,
        "nonpositive": sum(v <= 0 for v in values) if values else None,
    }


def long_bull_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    values = [r["annual_difference"] for r in rows if r["kind"] == "long_bull"]
    return {
        "count": len(values),
        "mean_annual_difference": float(np.mean(values)) if values else None,
        "worst_annual_difference": min(values) if values else None,
    }


def listed(kind: str, specs: Sequence[EpisodeSpec]) -> list[Segment]:
    return [
        Segment(
            spec.id,
            kind,
            spec.start,
            spec.end,
            None,
            None,
            pd.Period(spec.end, freq="M").ordinal - pd.Period(spec.start, freq="M").ordinal + 1,
            None,
        )
        for spec in specs
    ]


def segment_section(
    strategy: Simulation,
    benchmark: Simulation,
    slow_bears: Sequence[EpisodeSpec],
    long_bulls: Sequence[EpisodeSpec],
) -> dict[str, Any] | None:
    if not slow_bears and not long_bulls:
        return None
    section: dict[str, Any] = {}
    measured: list[dict[str, Any]] = []
    for kind, specs in (("slow_bear", slow_bears), ("long_bull", long_bulls)):
        rows = []
        for item in listed(kind, specs):
            own = _inside(strategy.returns, item)
            reference = _inside(benchmark.returns, item)
            row: dict[str, Any] = {
                "id": item.id,
                "kind": item.kind,
                "start": item.start,
                "end": item.end,
                "months": item.months,
                "days": len(own),
                "strategy_return": None,
                "benchmark_return": None,
                "difference": None,
                "average_exposure": None,
            }
            if kind == "long_bull":
                row.update(strategy_annual=None, benchmark_annual=None, annual_difference=None)
            if not own.empty and not reference.empty:
                values = segment_rows(strategy, benchmark, (item,))[0]
                row.update(values)
                measured.append(values)
            rows.append(row)
        section[kind] = rows
    section["slow_bear_summary"] = slow_bear_summary(measured)
    section["long_bull_summary"] = long_bull_summary(measured)
    return section


def decline_problems(found: Sequence[Segment], specs: Sequence[EpisodeSpec]) -> list[str]:
    expected = [(s.id, s.start, s.end) for s in specs]
    recomputed = [(s.id, s.start, s.end) for s in found if s.kind == "decline"]
    return [
        f"listed {id_} {start}..{end} is not recomputed"
        for id_, start, end in expected
        if (id_, start, end) not in recomputed
    ] + [
        f"recomputed {id_} {start}..{end} is not listed"
        for id_, start, end in recomputed
        if (id_, start, end) not in expected
    ]


def list_problems(
    found: Sequence[Segment],
    slow_bears: Sequence[EpisodeSpec],
    long_bulls: Sequence[EpisodeSpec],
) -> list[str]:
    problems: list[str] = []
    for kind, specs in (("slow_bear", slow_bears), ("long_bull", long_bulls)):
        expected = [(s.id, s.start, s.end) for s in specs]
        recomputed = [(s.id, s.start, s.end) for s in found if s.kind == kind]
        problems.extend(
            f"listed {id_} {start}..{end} is not recomputed"
            for id_, start, end in expected
            if (id_, start, end) not in recomputed
        )
        problems.extend(
            f"recomputed {id_} {start}..{end} is not listed"
            for id_, start, end in recomputed
            if (id_, start, end) not in expected
        )
    return problems
