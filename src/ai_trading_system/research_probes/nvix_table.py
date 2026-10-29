"""新闻波动 NVIX 是不是只是波动水平（TRADING-2565-92）。

依据 owner_decision:TRADING-2565:2026-10-29:print_only_tables、第 6 期章程第 23 节，
四张只印的表的第四张。快照的美股大盘日频因子和 Manela 月度 NVIX 截到 2010-12。
第一道是 NVIX 和同月已实现波动的秩相关，≥ 0.8 判 only_vol。第二道是 log NVIX
对 log 已实现波动的残差；月底 M 用 M − 1 那一对的扩张分位 ≥ 0.8 记状态 H，
照第 5 期 3.2 的线读十个慢熊段和七个长牛段。另印原样 NVIX 的状态 R、按块打乱
的对照和 F0001 慢腿退出的先后。只印，不是探针、不是试验、不进账本、不模拟新规则
的仓位线，不是长牛代价的读数。check 只数月数和分布，不印秩相关和斜率，不碰段。
"""

from __future__ import annotations

import hashlib
import json
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial
from ai_trading_system.research_bench.prereg import StrategyEntry, load_prereg
from ai_trading_system.research_bench.settings import load_settings
from ai_trading_system.research_bench.sources import SourceSpec
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import runup_state_table as rs
from ai_trading_system.research_probes import state_table as st

# 需求第 3、4 节及章程第 23.4 条，跑之前写死的身份和数据规格。
RUN_ID = "TRADING-2565-92"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-29:print_only_tables"
SNAPSHOT_ID = "sig-d5cea4795c30"
FACTORS = cp.FACTORS
NVIX = "nvix_monthly"
NVIX_SPEC = ("manela", "nvix", "monthly", 0, 5)
FACTORS_SPEC = ("ken_french", "F-F_Research_Data_Factors_daily", "daily", 0, 0)
# 需求第 3 节：原序列、回归、读表和打乱各自的月度边界。
NVIX_FIRST = pd.Period("1889-07", freq="M")
RESID_FIRST = pd.Period("1926-07", freq="M")
FIRST_MONTH = pd.Period("1929-01", freq="M")
LAST_MONTH = pd.Period("2010-12", freq="M")
SHUFFLE_FIRST = pd.Period("1929-07", freq="M")
QUESTION, RULES, LEG = "QQQ-Q3", rs.RULES, "slow"
# 需求第 3.1、3.3 节：15 天、36 对、120 个值，跑之前写死。
MIN_DAYS, MIN_RESID, MIN_RAW = 15, 36, 120
# 需求第 3、4 节：主分位、敏感度和第一道线，跑之前写死。
PERCENTILE, SENSITIVE_PERCENTILES, ONLY_VOL = 0.8, (0.7, 0.9), 0.8
BEFORE_MONTHS = st.BEFORE_MONTHS
# 第 5 期 3.2 的线：循环线程定的，没有依据；这里只沿用读表，不作准入门。
LINE_SLOW, LINE_BULL = st.LINE_SLOW, st.LINE_BULL
# 需求第 4 节：按块打乱的只印对照和读法，跑之前写死。
BLOCK, DRAWS, SEED, P_LINE, NULL_NOTE = 12, 1000, 20261029, 0.05, 0.1
HALVES = (("1929–1969", 1929, 1969), ("1970–2010", 1970, 2010))
DECADES = (
    ("1929–1939", 1929, 1939),
    ("1940–1949", 1940, 1949),
    ("1950–1959", 1950, 1959),
    ("1960–1969", 1960, 1969),
    ("1970–1979", 1970, 1979),
    ("1980–1989", 1980, 1989),
    ("1990–1999", 1990, 1999),
    ("2000–2010", 2000, 2010),
)
# 需求第 3、4 节：NVIX 自身的映射分段，不是市场状态分段。
PARTS = (
    ("back_cast", NVIX_FIRST, pd.Period("1985-12", freq="M")),
    ("test", pd.Period("1986-01", freq="M"), pd.Period("1995-12", freq="M")),
    ("train", pd.Period("1996-01", freq="M"), pd.Period("2009-12", freq="M")),
    ("update", pd.Period("2010-01", freq="M"), LAST_MONTH),
)
RHO_PARTS = (("back_cast", FIRST_MONTH, PARTS[0][2]), *PARTS[1:3])
WITHOUT_LAST = pd.Period("2009-12", freq="M")
# 需求第 4 节的状态、判定及输出名，跑之前写死。
STATES = ("H", "R")
VERDICTS = ("only_vol", "holds", "not_holds")
CHECK, MONTHS, SUMMARY, REPORT = (
    "nvix_check.json",
    "nvix_months.jsonl",
    "nvix_table.json",
    "nvix_table.md",
)

Record = dict[str, Any]
Segment = tuple[str, pd.Period, pd.Period]
app = typer.Typer()


@dataclass(frozen=True)
class Inputs:
    factors: pd.DataFrame
    nvix: pd.Series
    slow: tuple[Segment, ...]
    bull: tuple[Segment, ...]
    entries: dict[str, StrategyEntry]
    rules_sha256: str
    sources: list[Record]


@dataclass(frozen=True)
class Built:
    nvix: pd.Series
    factors: pd.DataFrame
    rv: pd.Series
    exits: tuple[pd.Period, ...]
    slow: tuple[Segment, ...]
    bull: tuple[Segment, ...]
    sources: list[Record]


def nvix_months(frame: pd.DataFrame) -> pd.Series:
    months = pd.DatetimeIndex(frame.index).to_period("M")
    keep = months <= LAST_MONTH
    months = months[keep]
    if months.has_duplicates:
        raise ValueError("NVIX has a repeated month")
    values = frame.loc[keep, "value"].to_numpy(dtype=float)
    present = values[~np.isnan(values)]
    if (present <= 0).any() or not np.isfinite(present).all():
        raise ValueError("NVIX values must be positive and finite")
    return pd.Series(values, index=months, name="nvix").reindex(
        pd.period_range(NVIX_FIRST, LAST_MONTH, freq="M")
    )


def realized_vol(factors: pd.DataFrame, *, demean: bool = False, span: int = 1) -> pd.Series:
    if span not in (1, 2):
        raise ValueError("RV span must be 1 or 2 months")
    returns, _ = rs.market_series(factors)
    logs = pd.Series(np.log1p(returns.to_numpy(dtype=float)), index=returns.index)
    months = pd.DatetimeIndex(logs.index).to_period("M")
    if demean:
        logs = logs - logs.groupby(months).transform("mean")
    sums = (logs**2).groupby(months).sum()
    counts = logs.groupby(months).count()
    sums = sums.where(counts >= MIN_DAYS).reindex(
        pd.period_range(RESID_FIRST, LAST_MONTH, freq="M")
    )
    if span == 2:
        sums = sums + sums.shift(1)
    return pd.Series(
        100.0 * np.sqrt(12.0 / span * sums.to_numpy(dtype=float)), index=sums.index, name="rv"
    )


def between(values: pd.Series, first: pd.Period, last: pd.Period) -> pd.Series:
    return values.reindex(pd.period_range(first, last, freq="M"))


def _pairs(x: pd.Series, y: pd.Series, first: pd.Period, last: pd.Period) -> pd.DataFrame:
    return pd.DataFrame({"x": between(x, first, last), "y": between(y, first, last)}).dropna()


def _finite(value: Any) -> float | None:
    return float(value) if value is not None and np.isfinite(value) else None


def spearman(x: pd.Series, y: pd.Series, first: pd.Period, last: pd.Period) -> float | None:
    pairs = _pairs(x, y, first, last)
    if len(pairs) < 3:
        return None
    return _finite(pairs["x"].rank().corr(pairs["y"].rank()))


def pearson(x: pd.Series, y: pd.Series, first: pd.Period, last: pd.Period) -> float | None:
    pairs = _pairs(x, y, first, last)
    if len(pairs) < 3:
        return None
    return _finite(pairs["x"].corr(pairs["y"]))


def first_stage(nvix: pd.Series, rv: pd.Series) -> Record:
    rho = spearman(nvix, rv, FIRST_MONTH, LAST_MONTH)
    return {
        "rho": rho,
        "months": int(len(_pairs(nvix, rv, FIRST_MONTH, LAST_MONTH))),
        "only_vol": rho is not None and rho >= ONLY_VOL,
        "halves": {
            label: spearman(nvix, rv, pd.Period(f"{a}-01", "M"), pd.Period(f"{b}-12", "M"))
            for label, a, b in HALVES
        },
        "decades": {
            label: spearman(nvix, rv, pd.Period(f"{a}-01", "M"), pd.Period(f"{b}-12", "M"))
            for label, a, b in DECADES
        },
        "log_pearson": pearson(np.log(nvix), np.log(rv), FIRST_MONTH, LAST_MONTH),
        "next_month": spearman(nvix, rv.shift(-1), FIRST_MONTH, LAST_MONTH - 1),
        "parts": {label: spearman(nvix, rv, a, b) for label, a, b in RHO_PARTS},
        "without_2010": spearman(nvix, rv, FIRST_MONTH, WITHOUT_LAST),
    }


def fit(nvix: pd.Series, rv: pd.Series, last: pd.Period = LAST_MONTH) -> tuple[float, float, int]:
    pairs = _pairs(np.log(nvix), np.log(rv), RESID_FIRST, last)
    if len(pairs) < 2:
        raise ValueError("too few months for the residual regression")
    x, y = pairs["y"].to_numpy(dtype=float), pairs["x"].to_numpy(dtype=float)
    design = np.column_stack((np.ones(len(x)), x))
    a, b = np.linalg.lstsq(design, y, rcond=None)[0]
    return float(a), float(b), int(len(pairs))


def residuals(
    nvix: pd.Series, rv: pd.Series, last: pd.Period = LAST_MONTH
) -> tuple[pd.Series, float, int]:
    a, b, count = fit(nvix, rv, last)
    e = np.log(between(nvix, RESID_FIRST, LAST_MONTH)) - (
        a + b * np.log(between(rv, RESID_FIRST, LAST_MONTH))
    )
    return e.rename("e"), b, count


def expanding_residuals(nvix: pd.Series, rv: pd.Series) -> tuple[pd.Series, float | None]:
    index = pd.period_range(RESID_FIRST, LAST_MONTH, freq="M")
    e = pd.Series(np.nan, index=index, name="e")
    pairs = _pairs(np.log(nvix), np.log(rv), RESID_FIRST, LAST_MONTH)
    # 用每个前缀的同一最小二乘拟合；缺月不消耗历史月数。
    slope: float | None = None
    log_n, log_rv = pairs["x"].to_numpy(dtype=float), pairs["y"].to_numpy(dtype=float)
    for position in range(MIN_RESID - 1, len(pairs)):
        month = pd.Period(pairs.index[position], freq="M")
        a, slope, _ = fit(nvix, rv, month)
        e.iloc[month.ordinal - RESID_FIRST.ordinal] = float(log_n[position]) - (
            a + slope * float(log_rv[position])
        )
    return e, slope


def month_end_state(values: pd.Series, min_history: int, percentile: float) -> pd.Series:
    pct = st.expanding_percentile(values, min_history)
    index = pd.period_range(FIRST_MONTH, LAST_MONTH, freq="M")
    prior = pct.reindex(index - 1).to_numpy(dtype=float)
    return pd.Series(
        np.where(np.isnan(prior), np.nan, (prior >= percentile).astype(float)),
        index=index,
        name="state",
    )


def h_state(e: pd.Series, percentile: float = PERCENTILE) -> pd.Series:
    return month_end_state(e, MIN_RESID, percentile)


def r_state(nvix: pd.Series, percentile: float = PERCENTILE) -> pd.Series:
    return month_end_state(nvix, MIN_RAW, percentile)


def first_defined(state: pd.Series) -> pd.Period | None:
    defined = state.dropna()
    return pd.Period(defined.index[0], freq="M") if len(defined) else None


def nvix_part(month: pd.Period) -> str:
    for label, first, last in PARTS:
        if first <= month <= last:
            return label
    raise ValueError(f"month outside NVIX parts: {month}")


def first_on(state: pd.Series, first: pd.Period, last: pd.Period) -> pd.Period | None:
    values = between(state, first, last)
    on = values[values.eq(1.0)]
    return pd.Period(on.index[0], freq="M") if len(on) else None


def slow_rows(
    states: Mapping[str, pd.Series],
    segments: Sequence[Segment],
    exits: Sequence[pd.Period],
    first_h: pd.Period | None,
) -> list[Record]:
    rows: list[Record] = []
    for name, start, end in segments:
        before = start - (BEFORE_MONTHS - 1)
        leg = next((m for m in exits if before <= m <= end), None)
        row: Record = {
            "id": name,
            "start": str(start),
            "end": str(end),
            "part": nvix_part(start),
            "incomplete": first_h is None or before < first_h,
            "no_defined": bool(between(states["H"], before, start).notna().sum() == 0),
            "exit": str(leg) if leg is not None else None,
        }
        for key, state in states.items():
            marks_before = st.marks(state, before, start)
            inside = st.marks(state, start, end)
            on = first_on(state, before, end)
            row[key] = {
                "before": marks_before,
                "before_on": st.MARKS[1.0] in marks_before,
                "inside": inside,
                "inside_on": inside.count(st.MARKS[1.0]),
                "switch": st.nearest_switch(state, start, end),
                "first_on": str(on) if on is not None else None,
                "lead": int(leg.ordinal - on.ordinal)
                if leg is not None and on is not None
                else None,
            }
        rows.append(row)
    return rows


def bull_rows(
    states: Mapping[str, pd.Series], segments: Sequence[Segment], last: pd.Period = LAST_MONTH
) -> Record:
    rows: list[Record] = []
    for name, start, end in segments:
        end = min(end, last)
        months = int(end.ordinal - start.ordinal + 1)
        if months <= 0:
            continue
        row: Record = {"id": name, "start": str(start), "end": str(end), "months": months}
        for key, state in states.items():
            on = int(between(state, start, end).eq(1.0).sum())
            row[key] = {"on": on, "share": on / months}
        rows.append(row)
    total = sum(row["months"] for row in rows)
    pooled: Record = {"months": total}
    for key in states:
        on = sum(row[key]["on"] for row in rows)
        pooled[key] = {"on": on, "share": on / total if total else None}
    return {"rows": rows, "pooled": pooled}


def line(slow: Sequence[Record], bull: Record, key: str) -> Record:
    on = sum(bool(row[key]["before_on"]) for row in slow)
    share = bull["pooled"][key]["share"]
    slow_ok, bull_ok = on >= LINE_SLOW, share is not None and share < LINE_BULL
    return {
        "slow_on": on,
        "slow_count": len(slow),
        "bull_share": share,
        "slow_ok": slow_ok,
        "bull_ok": bull_ok,
        "passes": slow_ok and bull_ok,
    }


def read_state(
    state: pd.Series,
    slow: Sequence[Segment],
    bull: Sequence[Segment],
    last: pd.Period = LAST_MONTH,
) -> Record:
    kept = state.copy()
    kept.iloc[np.flatnonzero(kept.index > last)] = np.nan
    return line(slow_rows({"H": kept}, slow, (), None), bull_rows({"H": kept}, bull, last), "H")


def blocks() -> list[tuple[pd.Period, pd.Period]]:
    return [
        (first, min(first + BLOCK - 1, LAST_MONTH))
        for first in pd.period_range(SHUFFLE_FIRST, LAST_MONTH, freq="M")[::BLOCK]
    ]


def shuffled(state: pd.Series, order: Sequence[int]) -> pd.Series:
    chunks = blocks()
    if sorted(order) != list(range(len(chunks))):
        raise ValueError("block order must be a permutation")
    values = [between(state, first, last).to_numpy() for first, last in chunks]
    result = state.copy()
    result[pd.period_range(SHUFFLE_FIRST, LAST_MONTH, freq="M")] = np.concatenate(
        [values[i] for i in order]
    )
    return result


def null(
    state: pd.Series,
    slow: Sequence[Segment],
    bull: Sequence[Segment],
    draws: int = DRAWS,
    seed: int = SEED,
) -> Record:
    if draws <= 0:
        raise ValueError("draws must be positive")
    rng = np.random.default_rng(seed)
    actual = read_state(state, slow, bull)["slow_on"]
    # 数组版等同于每次 shuffled 后 read_state：只数 before_on 和 pooled share。
    # 拼接短尾块而不是补齐为 12 月，保证块移位后月底位置与 shuffled 一致。
    index = pd.period_range(FIRST_MONTH, LAST_MONTH, freq="M")
    original = state.reindex(index).to_numpy(dtype=float)
    chunks = [between(state, a, b).to_numpy(dtype=float) for a, b in blocks()]
    slow_positions = [
        index.get_indexer(pd.period_range(start - BEFORE_MONTHS + 1, start, freq="M"))
        for _, start, _ in slow
    ]
    bull_positions = [
        index.get_indexer(pd.period_range(start, min(end, LAST_MONTH), freq="M"))
        for _, start, end in bull
        if start <= min(end, LAST_MONTH)
    ]
    denominator = sum(len(positions) for positions in bull_positions)
    counts: list[int] = []
    passes = 0
    for _ in range(draws):
        values = original.copy()
        values[index >= SHUFFLE_FIRST] = np.concatenate(
            [chunks[int(i)] for i in rng.permutation(len(chunks))]
        )
        on = values == 1.0
        count = sum(bool(on[positions[positions >= 0]].any()) for positions in slow_positions)
        bull_on = sum(int(on[positions[positions >= 0]].sum()) for positions in bull_positions)
        counts.append(count)
        passes += int(count >= LINE_SLOW and denominator > 0 and bull_on / denominator < LINE_BULL)
    p = sum(count >= actual for count in counts) / draws
    return {
        "actual": actual,
        "draws": draws,
        "p": p,
        "holds": p <= P_LINE,
        "mid_rank": (
            sum(count < actual for count in counts) + 0.5 * sum(count == actual for count in counts)
        )
        / draws,
        "pass_share": passes / draws,
        "median": float(statistics.median(counts)),
    }


def decade_counts(state: pd.Series) -> dict[str, int]:
    return {
        label: int(between(state, pd.Period(f"{a}-01", "M"), pd.Period(f"{b}-12", "M")).eq(1).sum())
        for label, a, b in DECADES
    }


def persistence(state: pd.Series) -> Record:
    on = between(state, FIRST_MONTH, LAST_MONTH).eq(1.0).to_numpy()
    runs = int(np.sum(on & ~np.concatenate(([False], on[:-1]))))
    return {"runs": runs, "mean_months": int(on.sum()) / runs if runs else None}


def state_counts(state: pd.Series) -> Record:
    values = between(state, SHUFFLE_FIRST, LAST_MONTH)
    on = int(values.eq(1.0).sum())
    return {
        "on": on,
        "defined": int(values.count()),
        "month_ends": len(values),
        "share": on / len(values),
    }


def rv_medians(state: pd.Series, rv: pd.Series) -> Record:
    defined = state.dropna()
    index = pd.PeriodIndex(defined.index, freq="M")
    values = pd.Series(rv.reindex(index - 1).to_numpy(dtype=float), index=index)
    return {"on": _finite(values[defined.eq(1.0)].median()), "all": _finite(values.median())}


def assemble(
    nvix: pd.Series,
    factors: pd.DataFrame,
    months: pd.DataFrame,
    slow: tuple[Segment, ...],
    bull: tuple[Segment, ...],
    sources: list[Record],
) -> Built:
    exits = tuple(m for m, _ in rs.leg_exits(months, LEG))
    return Built(nvix, factors, realized_vol(factors), exits, slow, bull, sources)


def build(inputs: Inputs) -> Built:
    returns, cash = rs.market_series(inputs.factors)
    return assemble(
        inputs.nvix,
        inputs.factors,
        rs.month_targets(returns, cash, inputs.entries),
        inputs.slow,
        inputs.bull,
        inputs.sources,
    )


def check_summary(built: Built) -> Record:
    e, _, pairs = residuals(built.nvix, built.rv)
    h, r = h_state(e), r_state(built.nvix)
    values = between(built.nvix, FIRST_MONTH, LAST_MONTH)
    first = first_defined(h)
    states = {"H": h, "R": r}
    return {
        "run_id": RUN_ID,
        "snapshot_id": SNAPSHOT_ID,
        "sources": built.sources,
        "nvix_months": int(values.count()),
        "nvix_missing": int(values.isna().sum()),
        "nvix_missing_1926_1928": int(
            between(built.nvix, RESID_FIRST, FIRST_MONTH - 1).isna().sum()
        ),
        "rv_months": int(between(built.rv, FIRST_MONTH, LAST_MONTH).count()),
        "regression_months": pairs,
        "h_first": str(first) if first is not None else None,
        "states": {k: state_counts(s) for k, s in states.items()},
        "decades": {k: decade_counts(s) for k, s in states.items()},
        "persistence": {k: persistence(s) for k, s in states.items()},
    }


def judge(first: Record, found: Record) -> Record:
    verdict = "only_vol" if first["only_vol"] else "holds" if found["passes"] else "not_holds"
    return {
        "verdict": verdict,
        "rho": first["rho"],
        **{k: found[k] for k in ("slow_on", "bull_share", "slow_ok", "bull_ok")},
    }


def readings(lines: Record, nulls: Record) -> list[str]:
    result = []
    if nulls["H"]["pass_share"] > NULL_NOTE:
        result.append(f"没信息的状态也有约 {nulls['H']['pass_share']:.0%} 会过这条线")
    if lines["R"]["passes"] and not lines["H"]["passes"]:
        result.append("原样的 NVIX 亮是因为波动")
    return result


def sensitivity(built: Built, e: pd.Series, expanding: pd.Series, slope: float | None) -> Record:
    slow, bull = built.slow, built.bull
    result: Record = {
        f"h_{p}": read_state(h_state(e, p), slow, bull) for p in SENSITIVE_PERCENTILES
    }
    h = h_state(expanding)
    result["expanding"] = {
        "b_last": slope,
        "line": read_state(h, slow, bull),
        "null": null(h, slow, bull),
    }
    for key, rv in (
        ("rv_demeaned", realized_vol(built.factors, demean=True)),
        ("rv_two_months", realized_vol(built.factors, span=2)),
    ):
        residual, _, _ = residuals(built.nvix, rv)
        result[key] = {
            "rho": spearman(built.nvix, rv, FIRST_MONTH, LAST_MONTH),
            "line": read_state(h_state(residual), slow, bull),
        }
    e2, b2, _ = residuals(built.nvix, built.rv, WITHOUT_LAST)
    result["without_2010"] = {
        "rho": spearman(built.nvix, built.rv, FIRST_MONTH, WITHOUT_LAST),
        "b": b2,
        "bull_share": bull_rows({"H": h_state(e2)}, bull, WITHOUT_LAST)["pooled"]["H"]["share"],
    }
    return result


def month_rows(built: Built, e: pd.Series, h: pd.Series, r: pd.Series) -> list[Record]:
    return [
        {
            "month": str(month),
            **{
                key: _finite(values.get(month))
                for key, values in {
                    "nvix": built.nvix,
                    "rv": built.rv,
                    "e": e,
                    "H": h,
                    "R": r,
                }.items()
            },
        }
        for month in pd.period_range(RESID_FIRST, LAST_MONTH, freq="M")
    ]


def read(inputs: Inputs) -> tuple[Record, list[Record]]:
    built = build(inputs)
    first = first_stage(built.nvix, built.rv)
    e, slope, pairs = residuals(built.nvix, built.rv)
    expanding, slope_last = expanding_residuals(built.nvix, built.rv)
    h, r = h_state(e), r_state(built.nvix)
    states = {"H": h, "R": r}
    slow = slow_rows(states, built.slow, built.exits, first_defined(h))
    bull = bull_rows(states, built.bull)
    lines = {k: line(slow, bull, k) for k in STATES}
    nulls = {k: null(s, built.slow, built.bull) for k, s in states.items()}
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": SNAPSHOT_ID,
        "rules_sha256": inputs.rules_sha256,
        "check": check_summary(built),
        "first_stage": first,
        "second_stage": {
            "b": slope,
            "b_expanding_last": slope_last,
            "regression_months": pairs,
            "slow_bear": slow,
            "long_bull": bull,
            "lines": lines,
            "null": nulls,
            "rv_medians": {k: rv_medians(s, built.rv) for k, s in states.items()},
        },
        "sensitivity": sensitivity(built, e, expanding, slope_last),
        "decision": judge(first, lines["H"]),
        "readings": readings(lines, nulls),
    }, month_rows(built, e, h, r)


def _number(value: Any, fmt: str = ".3f") -> str:
    return "–" if value is None else format(value, fmt)


def _yes(value: Any) -> str:
    return "是" if value else "否"


def _markdown(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    return [
        "",
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
        *("| " + " | ".join(row) + " |" for row in rows),
        "",
    ]


def render(result: Record) -> str:
    check, first, second = result["check"], result["first_stage"], result["second_stage"]
    sensitive, decision = result["sensitivity"], result["decision"]
    found = second["lines"]["H"]
    verdicts = {
        "only_vol": "`only_vol`：NVIX 在 1929–2010 只是波动水平",
        "holds": "`holds`：NVIX 去掉波动以后的部分在慢熊开始以前亮",
        "not_holds": "`not_holds`：NVIX 去掉波动以后的部分不在慢熊开始以前亮",
    }
    halves = "、".join(f"{label} {_number(first['halves'][label])}" for label, _, _ in HALVES)
    lines = [
        "# 新闻波动 NVIX 是不是只是波动水平（只印的表）",
        "",
        f"运行 {result['run_id']}，{result['owner_decision']}，快照 {result['snapshot_id']}。"
        "只印：不进账本，不是探针、不是试验，不模拟新规则的仓位线，不是长牛代价的读数。",
        "",
        f"## 判定：{verdicts[decision['verdict']]}",
        "",
        f"- 第一道：ρ = {_number(first['rho'])}（{first['months']} 个月，"
        f"线 ≥ 0.8，按全精度判）；两半 {halves}。",
    ]
    if decision["verdict"] == "only_vol":
        lines.append("- 第二道照印，不读（读法第 1 条）。")
    control = second["null"]["H"]
    lines += [
        f"- 第二道：H 在 {found['slow_on']}/{found['slow_count']} 个慢熊段起点或之前为真"
        f"（线 ≥ 8）{_yes(found['slow_ok'])}；长牛段的月份里 H 为真的占比 "
        f"{_number(found['bull_share'], '.1%')}（线 < 50%）{_yes(found['bull_ok'])}。",
        f"- 斜率 b：整段 {_number(second['b'])}（{second['regression_months']} 个月），"
        f"扩张回归最后一个月 {_number(second['b_expanding_last'])}。",
        f"- 打乱对照（H，只印）：p = {_number(control['p'])}，"
        f"中位秩 {_number(control['mid_rank'])}，"
        f"过线的比例 {_number(control['pass_share'], '.1%')}。",
        *(f"- {note}。" for note in result["readings"]),
        "",
        "## 第一道：秩相关（只有全段的 ρ 判）",
    ]
    part_names = {"back_cast": "回推", "test": "检验", "train": "训练", "update": "更新"}
    lines += _markdown(
        ("段", "ρ"),
        [
            *[[label, _number(first["decades"][label])] for label, _, _ in DECADES],
            *[[part_names[label], _number(first["parts"][label])] for label, _, _ in RHO_PARTS],
            ["log N 对 log RV 的 Pearson", _number(first["log_pearson"])],
            ["N 对下个月 RV", _number(first["next_month"])],
            ["去掉 2010", _number(first["without_2010"])],
        ],
    )
    lines.append("## 慢熊段（H 判，R 只印）")
    slow_rows_text = []
    for row in second["slow_bear"]:
        flag = (
            "窗口里没有有定义的月底"
            if row["no_defined"]
            else "窗口不全"
            if row["incomplete"]
            else ""
        )
        slow_rows_text.append(
            [
                f"{row['start']}..{row['end']}",
                part_names[row["part"]],
                row["H"]["before"],
                row["H"]["inside"],
                row["R"]["before"],
                row["R"]["inside"],
                _number(row["H"]["switch"], "d"),
                _number(row["R"]["switch"], "d"),
                row["exit"] or "–",
                _number(row["H"]["lead"], "d"),
                _number(row["R"]["lead"], "d"),
                flag,
            ]
        )
    lines += _markdown(
        (
            "段",
            "NVIX 段",
            "H 前 6 个月",
            "H 段内",
            "R 前 6 个月",
            "R 段内",
            "H 翻的月份对起点",
            "R 翻的月份对起点",
            "慢腿退出",
            "H 比慢腿早（月）",
            "R 比慢腿早（月）",
            "标记",
        ),
        slow_rows_text,
    )
    lines += [
        "● 为真，○ 为假，– 不定（算「没有」）。",
        "",
        "慢腿退出印「–」：[段起点 − 5, 段终点] 里慢腿没有新的退出"
        "（也可能窗口以前就已经出去）；比慢腿早印「–」：慢腿没退出，或者状态在窗口里没亮过。",
        "",
        "## 长牛段",
    ]
    bull = second["long_bull"]
    lines += _markdown(
        ("段", "月数", "H 为真", "R 为真"),
        [
            [
                f"{row['start']}..{row['end']}" if "start" in row else "合计",
                str(row["months"]),
                *(f"{row[k]['on']}（{_number(row[k]['share'], '.1%')}）" for k in STATES),
            ]
            for row in [*bull["rows"], bull["pooled"]]
        ],
    )
    lines += [
        "## 打乱对照（只印，不改判定）",
        "",
        f"每块 {BLOCK} 个月，共 {len(blocks())} 块（最后一块不足 12 个月）；"
        f"numpy.random.default_rng({SEED}) 的 {DRAWS} 个排列。",
    ]
    lines += _markdown(
        ("状态", "实际段数", "p", "中位秩", "过线的比例", "打乱的中位数"),
        [
            [
                label,
                str(row["actual"]),
                _number(row["p"]),
                _number(row["mid_rank"]),
                _number(row["pass_share"], ".1%"),
                _number(row["median"], ".1f"),
            ]
            for label, row in (
                ("H", second["null"]["H"]),
                ("R（原样的 NVIX）", second["null"]["R"]),
                ("H（扩张回归的残差）", sensitive["expanding"]["null"]),
            )
        ],
    )
    lines.append("## 敏感度（只印，不判）")
    sensitive_rows = []
    for key, label in (
        ("h_0.7", "H 的分位线 0.7"),
        ("h_0.9", "H 的分位线 0.9"),
        ("expanding", "残差用扩张回归"),
        ("rv_demeaned", "RV 减去月内均值"),
        ("rv_two_months", "RV 用 M − 1 和 M 两个月"),
    ):
        variant = sensitive[key]
        reading = variant if key.startswith("h_") else variant["line"]
        sensitive_rows.append(
            [
                label,
                _number(variant.get("rho")),
                f"{reading['slow_on']}/{reading['slow_count']}",
                _number(reading["bull_share"], ".1%"),
                _yes(reading["passes"]),
            ]
        )
    without = sensitive["without_2010"]
    sensitive_rows.append(
        [
            f"去掉 2010 年（b = {_number(without['b'])}）",
            _number(without["rho"]),
            "–",
            _number(without["bull_share"], ".1%"),
            "–",
        ]
    )
    lines += _markdown(("变体", "ρ", "慢熊段", "长牛段占比", "两条都过"), sensitive_rows)
    lines += [
        "残差用扩张回归时 e 要先有 36 对、H 再要 36 个 e，H 最早 1932-06 起才有定义"
        "（早年缺月会更晚）：第一段慢熊（1929-09）的窗口里没有有定义的月底，"
        "第二段（1932-09）只有一部分，这一行的慢熊段数天生少 1–2 段。",
        "",
        "## 次数和分布",
        "",
        f"- N 有值 {check['nvix_months']} 个月，缺 {check['nvix_missing']} 个月；"
        f"1926-07..1928-12 缺 {check['nvix_missing_1926_1928']} 个月；"
        f"RV 有值 {check['rv_months']} 个月；残差回归 {check['regression_months']} 个月。",
        f"- H 第一个有定义的月底 {check['h_first'] or '–'}。",
    ]
    for key in STATES:
        counts, runs = check["states"][key], check["persistence"][key]
        medians = second["rv_medians"][key]
        lines.append(
            f"- {key}：为真 {counts['on']} 个月底，共 {counts['month_ends']} 个，"
            f"有定义 {counts['defined']} 个，占比 {_number(counts['share'], '.1%')}；"
            f"连续段 {runs['runs']} 段，平均 {_number(runs['mean_months'], '.1f')} 个月；"
            f"为真的月份里 RV 的中位数 {_number(medians['on'], '.1f')}，"
            f"全部的中位数 {_number(medians['all'], '.1f')}。"
        )
    lines += _markdown(
        ("十年", "H 为真", "R 为真"),
        [[label, *(str(check["decades"][k][label]) for k in STATES)] for label, _, _ in DECADES],
    )
    lines.append("## 数据和核对")
    lines += _markdown(
        ("来源", "首", "末", "个数"),
        [
            [s["source"], s["first"] or "–", s["last"] or "–", str(s.get("months", s.get("days")))]
            for s in check["sources"]
        ],
    )
    lines += [
        f"- 快照 {result['snapshot_id']}；QQQ-Q3 规则 sha256 {result['rules_sha256']}；"
        f"代码 {result.get('provenance', {}).get('commit') or '–'}。",
        "- production_effect=none；broker_action=none。",
        "",
        "## 局限",
        "",
        "- e 是「NVIX 比这个月 RV 多出来的那部分」，里面有 VIX 本来就有的预期和风险溢价；"
        "e 亮，也可能只是「文本预期以后波动会高」，还是波动这一族的读法。",
        "- 1986 年以前的 NVIX 是用 1996–2009 的词和 VIX 的关系回推的："
        "「当时知道」只对文本成立，映射用了以后的数据。",
        "- 起点在 1985-12 以前的慢熊段用的是回推的 NVIX；起点在训练期 1996–2009 里的段，"
        "那几年的 NVIX 基本就是拟合出来的 VIX。",
        "- 残差回归的斜率 b 用了 1926-07..2010-12 整段，只有它用到以后的数据"
        "（截距对 H 没有影响）；扩张回归那一行是对照。",
        "- 2010 年的 12 个月出自后来的更新（基本上同一个模型）；缺 WSJ 文本的月份是缺值，"
        "不定的月底一律算「没有」。",
        "- NVIX 2016 年停更：就算判 `holds` 也不进前瞻账本，不开正式题。",
        "",
    ]
    return "\n".join(lines)


def _write_json(value: Record, path: Path) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_check(summary: Record, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(summary, out_dir / CHECK)


def write_result(result: Record, months: Sequence[Record], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / MONTHS).write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
            for row in months
        ),
        encoding="utf-8",
    )
    _write_json(result, out_dir / SUMMARY)
    (out_dir / REPORT).write_text(render(result), encoding="utf-8")


def spec_problems(specs: Mapping[str, SourceSpec]) -> list[str]:
    problems = []
    for source, expected in ((NVIX, NVIX_SPEC), (FACTORS, FACTORS_SPEC)):
        spec = specs.get(source)
        if spec is None:
            problems.append(f"{source}: no spec in the snapshot")
            continue
        actual = (
            spec.provider,
            spec.dataset,
            spec.frequency,
            spec.release_lag_months,
            spec.known_lag_trading_days,
        )
        if actual != expected:
            problems.append(f"{source}: spec {actual} is not {expected}")
    return problems


def load_inputs(root: Path = PROJECT_ROOT) -> Inputs:
    loaded = load_settings()
    research = lg.research_root(loaded)
    gate, _ = trial.load_trial(research, st.PLAN_ID)
    problems = trial.gate_problems(gate, loaded)
    snapshot = signal_sources.load_snapshot(root, SNAPSHOT_ID)
    problems += spec_problems({spec.source_id: spec for spec in snapshot.specs})
    if problems:
        raise ValueError("; ".join(problems))
    nvix = nvix_months(snapshot.frames[NVIX])
    factors = rs.cut(snapshot.frames[FACTORS])
    window = pt.market_window(loaded, gate, "us_market")
    slow, bull = gate.markets["us_market"].segment_lists(window.window_id)
    slow_segments = tuple(
        (spec.id, pd.Period(spec.start, freq="M"), pd.Period(spec.end, freq="M")) for spec in slow
    )
    bull_segments = tuple(
        (spec.id, pd.Period(spec.start, freq="M"), pd.Period(spec.end, freq="M")) for spec in bull
    )
    present = nvix.dropna()
    sources = [
        {
            "source": NVIX,
            "first": str(present.index[0]) if len(present) else None,
            "last": str(present.index[-1]) if len(present) else None,
            "months": int(len(present)),
        },
        {
            "source": FACTORS,
            "first": pd.Timestamp(factors.index[0]).date().isoformat() if len(factors) else None,
            "last": pd.Timestamp(factors.index[-1]).date().isoformat() if len(factors) else None,
            "days": int(len(factors)),
        },
    ]
    rules = research / QUESTION / "prereg.yaml"
    prereg = load_prereg(rules)
    entries = {name: prereg.strategy(strategy) for name, strategy in RULES.items()}
    return Inputs(
        factors,
        nvix,
        slow_segments,
        bull_segments,
        entries,
        hashlib.sha256(rules.read_bytes()).hexdigest(),
        sources,
    )


def _load() -> Inputs:
    try:
        return load_inputs()
    except (OSError, ValueError, KeyError, trial.TrialError) as exc:
        message = str(exc).splitlines()[0] if str(exc) else ""
        typer.echo(f"nvix_table: 数据载入失败：{message}")
        raise typer.Exit(1) from exc


@app.callback()
def cli() -> None:
    """只印新闻波动 NVIX 表，不进账本、不模拟新规则。"""


@app.command("check")
def check_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        summary = check_summary(build(inputs))
    except ValueError as exc:
        typer.echo(f"nvix_table: 建数据失败：{exc}")
        raise typer.Exit(1) from exc
    write_check(summary, out)
    typer.echo(
        f"nvix_table: N 有值 {summary['nvix_months']} 个月；H 第一个有定义的月底 "
        f"{summary['h_first'] or '–'}；H 为真 {summary['states']['H']['on']} 个月底"
    )


@app.command("run")
def run_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        result, months = read(inputs)
    except ValueError as exc:
        typer.echo(f"nvix_table: 建数据失败：{exc}")
        raise typer.Exit(1) from exc
    result["provenance"] = git_provenance(PROJECT_ROOT)
    write_check(result["check"], out)
    write_result(result, months, out)
    typer.echo(f"nvix_table: 判定 {result['decision']['verdict']}")


if __name__ == "__main__":
    app()
