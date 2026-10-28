"""行业暴涨以后的跌破可不可信（TRADING-2565-84）。

依据 owner_decision:TRADING-2565:2026-10-29:print_only_tables、第 6 期章程第 23 节，
四张只印的表的第一张。信号快照的 49 个行业市值加权日频，照 GSY 不用「其他」，
加大盘因子，截到 2010-12-31；1929-01 以前只用于预热和回看。
F0001 和两条腿照 QQQ-Q3 预注册，用评估台的 signal_exposure；快腿退出按退出
月末 GSY（Greenwood、Shleifer、You 2019）的暴涨状态分组。先核 GSY 的崩盘比例，
再按跑前写死的读法判定。只印，不是探针、不是试验、不进账本、不模拟新规则，
不是长牛代价的读数。check 只数次数和退出前的波动，不碰退出以后的收益。
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import segments
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.prereg import StrategyEntry, load_prereg
from ai_trading_system.research_bench.settings import load_settings
from ai_trading_system.research_bench.signal_strategy import signal_exposure
from ai_trading_system.research_bench.sources import DEFAULT_MAX_STALE_DAYS
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import longbull_tech_read as lt

RUN_ID = "TRADING-2565-84"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-29:print_only_tables"
SNAPSHOT_ID = cp.SNAPSHOT_ID
INDUSTRIES, FACTORS = cp.INDUSTRIES, cp.FACTORS
MARKET, CASH = "Mkt-RF", "RF"
LAST_DAY = date(2010, 12, 31)
# 退出从 1929-01 的月末数到 2010-12；GSY 的段起点到 2008-12，之后满 24 个月。
FIRST_MONTH = pd.Period("1929-01", freq="M")
LAST_MONTH = pd.Period("2010-12", freq="M")
EPISODE_LAST = pd.Period("2008-12", freq="M")
# F0001 和两条腿：QQQ-Q3 预注册里的策略 id。
QUESTION = "QQQ-Q3"
RULES = {"f0001": "fast_slow", "fast": "fast_only", "slow": "slow_only"}
LEGS = ("fast", "slow")
# 缺数据的规矩，不是研究阈值。
MAX_MISSING_DAYS = DEFAULT_MAX_STALE_DAYS
MAX_EXCLUDED = 10
# GSY 的暴涨（需求第 3.2 节，跑之前写死）。
RUNUP_MONTHS, RUNUP_RAW, RUNUP_NET = 24, 1.0, 1.0
LONG_MONTHS, LONG_RAW, STATE_MONTHS = 60, 0.5, 24
# 退出以后跟着真跌的线、GSY 的崩。
FOLLOW_DAYS, FOLLOW_DROP = 252, -0.20
CRASH_MONTHS, CRASH_DROP, LEG_SHARE = 24, -0.40, 0.5
# GSY（NBER w23191）印的：美国行业 100% 门槛 40 段、21 段崩，无条件 14%。
# 1970 年以后无条件 11%。
GSY = {"episodes": 40, "crashes": 21, "share": 0.53, "unconditional": 0.14, "after_1970": 0.11}
# 判定（需求第 4 节，跑之前写死）。
FIDELITY_MIN, FIDELITY_LOW, FIDELITY_HIGH = 15, 0.30, 0.76
MIN_R_EXITS, GAP_LINE = 60, 0.15
HALVES = (("1929–1969", 1929, 1969), ("1970–2010", 1970, 2010))
HALF_MIN_R, DROP_YEAR_LINE = 20, 0.10
# 比较的浮点容差，不是研究阈值。
EPSILON = 1e-9
# GSY 用 49 个行业的前 48 个，去掉残差行业「其他」。
LEFT_OUT = ("Other",)
# 只印的对照：退出前的年化波动（交易日数、年化天数）；survey 的回价交易日口径。
VOL_DAYS, ANNUAL, BACK_DAYS = 63, 252, 63
# 读法的两句（需求第 4 节，跑之前写死，不改判定）。
MOSTLY_VOL_SHARE, CONCENTRATION_LINE = 0.5, 0.10
TECH = ("Hardw", "Chips", "Softw")
STATES = ("R", "N", "undetermined")
VERDICTS = ("incomplete", "fidelity_miss", "too_few", "holds", "not_holds")
CHECK, EXITS, SUMMARY, REPORT = (
    "runup_check.json",
    "runup_exits.jsonl",
    "runup_table.json",
    "runup_table.md",
)

Record = dict[str, Any]
Progress = Callable[[str], None]
app = typer.Typer()


@dataclass(frozen=True)
class Inputs:
    industries: pd.DataFrame
    factors: pd.DataFrame
    entries: dict[str, StrategyEntry]
    rules_sha256: str


@dataclass(frozen=True)
class Industry:
    name: str
    returns: pd.Series
    cash: pd.Series
    months: pd.DataFrame
    flags: pd.DataFrame
    starts: list[pd.Period]


@dataclass(frozen=True)
class Built:
    industries: dict[str, Industry]
    rows: list[Record]
    market: pd.Series
    market_flags: pd.DataFrame
    left_out: tuple[str, ...] = ()


def cut(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.loc[frame.index <= pd.Timestamp(LAST_DAY)]


def clean(daily: pd.Series) -> tuple[pd.Series, list[str]]:
    first = daily.first_valid_index()
    if first is None:
        raise ValueError("no returns")
    daily = daily.loc[first:].astype(float)
    missing = [pd.Timestamp(day).date().isoformat() for day in daily.index[daily.isna()]]
    result = daily.dropna()
    if not np.isfinite(result.to_numpy(dtype=float)).all():
        raise ValueError("non-finite return")
    return result, missing


def market_series(factors: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    factors = cut(factors)
    for column in (MARKET, CASH):
        if column not in factors:
            raise ValueError(f"missing {column} column")
    if not np.isfinite(factors[[MARKET, CASH]].to_numpy(dtype=float)).all():
        raise ValueError("market factors have missing or non-finite values")
    return factors[MARKET] + factors[CASH], factors[CASH]


def month_targets(
    returns: pd.Series, cash: pd.Series, entries: Mapping[str, StrategyEntry]
) -> pd.DataFrame:
    data = lt.bench_data(returns, cash)
    days = pd.DatetimeIndex(returns.index)
    targets = pd.DataFrame({"date": days, "pos": np.arange(len(days))}, index=days)
    for name in RULES:
        targets[name] = strategy_targets(
            signal_exposure, data.inputs, entries[name].params
        ).reindex(days)
    ends = targets.groupby(days.to_period("M")).tail(1).copy()
    ends.index = pd.DatetimeIndex(ends.index).to_period("M")
    return ends


def runup_flags(level: pd.Series, market: pd.Series) -> pd.DataFrame:
    index = pd.PeriodIndex(level.index, freq="M")
    own = level.to_numpy(dtype=float)
    raw = own / level.reindex(index - RUNUP_MONTHS).to_numpy(dtype=float) - 1.0
    market_raw = (
        market.reindex(index).to_numpy(dtype=float)
        / market.reindex(index - RUNUP_MONTHS).to_numpy(dtype=float)
        - 1.0
    )
    net = raw - market_raw
    long_raw = own / level.reindex(index - LONG_MONTHS).to_numpy(dtype=float) - 1.0
    defined = np.isfinite(raw) & np.isfinite(net)
    waived = defined & ~np.isfinite(long_raw)
    runup = defined & (raw >= RUNUP_RAW) & (net >= RUNUP_NET) & (waived | (long_raw >= LONG_RAW))
    return pd.DataFrame(
        {
            "raw": raw,
            "net": net,
            "long_raw": long_raw,
            "defined": defined,
            "waived": waived,
            "runup": runup,
        },
        index=level.index,
    )


def states(flags: pd.DataFrame) -> pd.Series:
    if flags.empty:
        return pd.Series(index=flags.index, dtype=object)
    index = pd.period_range(flags.index.min() - STATE_MONTHS + 1, flags.index.max(), freq="M")
    full = flags.reindex(index)
    defined = full["defined"].eq(True).rolling(STATE_MONTHS).sum().eq(STATE_MONTHS)
    hits = full["runup"].eq(True).rolling(STATE_MONTHS).sum().gt(0)
    result = pd.Series(np.where(defined, np.where(hits, "R", "N"), "undetermined"), index=index)
    return result.reindex(flags.index).astype(object)


def episode_starts(flags: pd.DataFrame) -> list[pd.Period]:
    hits = sorted(pd.Period(p, freq="M") for p in flags.index[flags["runup"].to_numpy(dtype=bool)])
    return [
        p for i, p in enumerate(hits) if i == 0 or p.ordinal - hits[i - 1].ordinal > STATE_MONTHS
    ]


def build_industry(
    name: str,
    daily: pd.Series,
    cash: pd.Series,
    market_level: pd.Series,
    entries: Mapping[str, StrategyEntry],
) -> Industry:
    returns, _ = clean(daily)
    rf = cash.reindex(returns.index)
    months = month_targets(returns, rf, entries)
    level = segments.monthly_level(returns)
    flags = runup_flags(level, market_level)
    months = months.join(flags[["runup"]], how="left")
    months["state"] = states(flags).reindex(months.index)
    return Industry(name, returns, rf, months, flags, episode_starts(flags))


def build(inputs: Inputs, progress: Progress | None = None) -> Built:
    market, cash = market_series(inputs.factors)
    market_level = segments.monthly_level(market)
    daily = cut(inputs.industries)
    left_out = tuple(str(name) for name in daily if str(name) in LEFT_OUT)
    industries: dict[str, Industry] = {}
    rows: list[Record] = []
    for column in daily:
        name = str(column)
        if name in LEFT_OUT:
            continue
        row: Record = {"name": name, "first": None, "last": None, "missing": 0}
        try:
            returns, missing = clean(daily[column])
            row.update(
                first=pd.Timestamp(returns.index[0]).date().isoformat(),
                last=pd.Timestamp(returns.index[-1]).date().isoformat(),
                missing=len(missing),
                missing_days=missing[:5],
            )
            if len(missing) > MAX_MISSING_DAYS:
                row.update(used=False, reason=f"缺 {len(missing)} 天")
            else:
                industries[name] = build_industry(name, returns, cash, market_level, inputs.entries)
                row.update(used=True, reason=None)
        except (ValueError, KeyError) as exc:
            row.update(used=False, reason=f"出错：{exc}")
        rows.append(row)
        if progress is not None:
            progress(name)
    return Built(industries, rows, market, runup_flags(market_level, market_level), left_out)


def leg_exits(months: pd.DataFrame, leg: str) -> list[tuple[pd.Period, pd.Period | None]]:
    usable = months.dropna(subset=list(RULES))
    periods = list(pd.PeriodIndex(usable.index, freq="M"))
    targets = usable[leg].to_numpy(dtype=float)
    result: list[tuple[pd.Period, pd.Period | None]] = []
    for i in range(1, len(periods)):
        month = periods[i]
        if FIRST_MONTH <= month <= LAST_MONTH and targets[i - 1] == 1.0 and targets[i] == 0.0:
            back = next((periods[j] for j in range(i + 1, len(periods)) if targets[j] == 1.0), None)
            result.append((month, back))
    return result


def identity_mismatches(months: pd.DataFrame) -> list[str]:
    usable = months.dropna(subset=list(RULES))
    usable = usable.loc[(usable.index >= FIRST_MONTH) & (usable.index <= LAST_MONTH)]
    mismatch = (usable["f0001"] - (usable["fast"] + usable["slow"]) / 2).abs() > EPSILON
    return [str(p) for p in usable.index[mismatch]]


def positions(months: pd.DataFrame) -> dict[pd.Period, int]:
    return {pd.Period(str(p), freq="M"): int(pos) for p, pos in months["pos"].items()}


def vol_before(returns: np.ndarray[Any, Any], at: int) -> float | None:
    if at + 1 < VOL_DAYS:
        return None
    return float(np.std(returns[at + 1 - VOL_DAYS : at + 1], ddof=1)) * math.sqrt(ANNUAL)


def exit_rows(industry: Industry, leg: str) -> list[Record]:
    returns = industry.returns.to_numpy(dtype=float)
    cash = industry.cash.to_numpy(dtype=float)
    excess = np.concatenate(([0.0], np.cumsum(np.log1p(returns) - np.log1p(cash))))
    level = np.concatenate(([1.0], np.cumprod(1.0 + returns)))
    where = positions(industry.months)
    rows: list[Record] = []
    for month, back in leg_exits(industry.months, leg):
        at = where[month] + 1
        until = where[back] + 1 if back is not None else len(returns)
        out = float(excess[until] - excess[at])
        follow = (
            float(np.min(level[at + 1 : at + FOLLOW_DAYS + 1]) / level[at] - 1.0)
            if at + FOLLOW_DAYS <= len(returns)
            else None
        )
        back63 = (
            bool(np.max(level[at + 1 : at + BACK_DAYS + 1]) >= level[at])
            if at + BACK_DAYS <= len(returns)
            else None
        )
        state = str(industry.months.at[month, "state"])
        start = next((s for s in reversed(industry.starts) if s <= month), None)
        rows.append(
            {
                "industry": industry.name,
                "leg": leg,
                "month": str(month),
                "date": pd.Timestamp(str(industry.months.at[month, "date"])).date().isoformat(),
                "year": int(month.year),
                "state": state,
                "episode": str(start) if state == "R" and start is not None else None,
                "complete": back is not None,
                "reentry": str(back) if back is not None else None,
                "months_out": int(back.ordinal - month.ordinal) if back is not None else None,
                "out_excess": out,
                "whipsaw": out > 0 if back is not None else None,
                "leg_pnl": -LEG_SHARE * out,
                "follow_drop": follow,
                "real_drop": follow <= FOLLOW_DROP if follow is not None else None,
                "vol63": vol_before(returns, at - 1),
                "back63": back63,
            }
        )
    return rows


def all_exits(built: Built) -> list[Record]:
    return [
        row
        for leg in LEGS
        for industry in built.industries.values()
        for row in exit_rows(industry, leg)
    ]


def _counts(values: Sequence[str]) -> dict[str, int]:
    counts = Counter(values)
    return {state: counts[state] for state in STATES}


def quartiles(values: Sequence[float]) -> Record:
    if not values:
        return {"n": 0, "q25": None, "median": None, "q75": None}
    q25, median, q75 = np.quantile(values, [0.25, 0.5, 0.75])
    return {"n": len(values), "q25": float(q25), "median": float(median), "q75": float(q75)}


def check_summary(built: Built) -> Record:
    rows = [dict(row) for row in built.rows]
    by_name = {row["name"]: row for row in rows}
    exits: dict[str, list[tuple[str, pd.Period, str]]] = {leg: [] for leg in LEGS}
    mismatches: list[str] = []
    runups = waived = episodes = in_window = 0
    vols: dict[str, list[float]] = {"R": [], "N": []}
    for name, industry in built.industries.items():
        mismatches.extend(f"{name} {p}" for p in identity_mismatches(industry.months))
        flags = industry.flags
        window = flags.loc[(flags.index >= FIRST_MONTH) & (flags.index <= LAST_MONTH)]
        runups += int(window["runup"].sum())
        waived += int((window["runup"] & window["waived"]).sum())
        episodes += len(industry.starts)
        in_window += sum(FIRST_MONTH <= s <= EPISODE_LAST for s in industry.starts)
        where = positions(industry.months)
        daily = industry.returns.to_numpy(dtype=float)
        for leg in LEGS:
            own = [
                (name, month, str(industry.months.at[month, "state"]))
                for month, _ in leg_exits(industry.months, leg)
            ]
            exits[leg].extend(own)
            by_name[name][leg] = _counts([s for _, _, s in own])
            if leg == "fast":
                for _, month, state in own:
                    if state in vols:
                        vol = vol_before(daily, where[month])
                        if vol is not None:
                            vols[state].append(vol)
    fast = exits["fast"]
    market = built.market_flags
    market = market.loc[(market.index >= FIRST_MONTH) & (market.index <= LAST_MONTH)]
    return {
        "industries": rows,
        "used": len(built.industries),
        "excluded": [row["name"] for row in rows if not row["used"]],
        "identity_mismatches": len(mismatches),
        "identity_first": sorted(mismatches)[:5],
        "runup_months": runups,
        "waived_months": waived,
        "episodes": episodes,
        "episodes_in_window": in_window,
        "exits": {leg: _counts([s for _, _, s in exits[leg]]) for leg in LEGS},
        "halves": {
            label: _counts([s for _, p, s in fast if first <= p.year <= last])
            for label, first, last in HALVES
        },
        "decades": {
            f"{decade}s": _counts([s for _, p, s in fast if decade <= p.year < decade + 10])
            for decade in range(1920, 2011, 10)
        },
        "tech": {
            name: _counts([s for n, _, s in fast if n == name])
            for name in TECH
            if name in built.industries
        },
        "market_runup_months": int((market["raw"] >= RUNUP_RAW).sum()),
        "vol63": {state: quartiles(values) for state, values in vols.items()},
        "left_out": list(built.left_out),
    }


def crashes(industry: Industry, starts: Sequence[pd.Period]) -> list[bool | None]:
    level = np.cumprod(1.0 + industry.returns.to_numpy(dtype=float))
    where = positions(industry.months)
    day_months = [p.ordinal for p in pd.DatetimeIndex(industry.returns.index).to_period("M")]
    last = pd.Period(pd.Timestamp(industry.returns.index[-1]), freq="M")
    result: list[bool | None] = []
    for start in starts:
        end = start + CRASH_MONTHS
        if last < end or start not in where:
            result.append(None)
            continue
        stop = int(np.searchsorted(day_months, end.ordinal, side="right"))
        path = level[where[start] : stop]
        result.append(bool(np.min(path / np.maximum.accumulate(path) - 1.0) <= CRASH_DROP))
    return result


def fidelity(built: Built) -> Record:
    rows: list[Record] = []
    all_months: list[bool] = []
    later: list[bool] = []
    for name, industry in built.industries.items():
        starts = [s for s in industry.starts if FIRST_MONTH <= s <= EPISODE_LAST]
        rows.extend(
            {"industry": name, "start": str(s), "crash": hit}
            for s, hit in zip(starts, crashes(industry, starts), strict=True)
        )
        defined = [
            pd.Period(p, freq="M")
            for p in industry.flags.index[industry.flags["defined"].to_numpy(dtype=bool)]
            if FIRST_MONTH <= p <= EPISODE_LAST
        ]
        for month, hit in zip(defined, crashes(industry, defined), strict=True):
            if hit is not None:
                all_months.append(hit)
                if month.year >= 1970:
                    later.append(hit)
    counted = [row["crash"] for row in rows if row["crash"] is not None]
    share = statistics.fmean(counted) if counted else None
    return {
        "list": rows,
        "episodes": len(counted),
        "crashes": sum(counted),
        "share": share,
        "unconditional": statistics.fmean(all_months) if all_months else None,
        "unconditional_months": len(all_months),
        "after_1970": statistics.fmean(later) if later else None,
        "gsy": dict(GSY),
        "passed": len(counted) >= FIDELITY_MIN
        and share is not None
        and FIDELITY_LOW - EPSILON <= share <= FIDELITY_HIGH + EPSILON,
    }


def group_stats(rows: Sequence[Record]) -> Record:
    complete = [r for r in rows if r["complete"]]
    pnl = [r["leg_pnl"] for r in complete]
    follow = [r for r in complete if r["follow_drop"] is not None]
    back = [r["back63"] for r in rows if r["back63"] is not None]
    return {
        "exits": len(rows),
        "complete": len(complete),
        "open": len(rows) - len(complete),
        "whipsaw": statistics.fmean(r["whipsaw"] for r in complete) if complete else None,
        "pnl_median": statistics.median(pnl) if pnl else None,
        "pnl_mean": statistics.fmean(pnl) if pnl else None,
        "pnl_sum": math.fsum(pnl),
        "follow": len(follow),
        "no_follow": len(complete) - len(follow),
        "real_drop": statistics.fmean(r["real_drop"] for r in follow) if follow else None,
        "months_median": statistics.median(r["months_out"] for r in complete) if complete else None,
        "back63": statistics.fmean(back) if back else None,
    }


def _gap(left: float | None, right: float | None) -> float | None:
    return left - right if left is not None and right is not None else None


def table(rows: Sequence[Record]) -> Record:
    r = group_stats([row for row in rows if row["state"] == "R"])
    n = group_stats([row for row in rows if row["state"] == "N"])
    return {
        "R": r,
        "N": n,
        "undetermined": sum(row["state"] == "undetermined" for row in rows),
        "gap": _gap(n["whipsaw"], r["whipsaw"]),
    }


def drop_year(rows: Sequence[Record]) -> Record:
    counts = Counter(r["year"] for r in rows if r["state"] == "R" and r["complete"])
    year = min(counts, key=lambda y: (-counts[y], y)) if counts else None
    return {
        "year": year,
        "removed": counts[year] if year is not None else 0,
        "table": table([r for r in rows if r["year"] != year]),
    }


def episode_weighted(rows: Sequence[Record]) -> Record:
    groups: dict[tuple[str, str], list[bool]] = {}
    for row in rows:
        if row["state"] == "R" and row["complete"] and row["episode"] is not None:
            groups.setdefault((row["industry"], row["episode"]), []).append(row["whipsaw"])
    return {
        "episodes": len(groups),
        "whipsaw": statistics.fmean(statistics.fmean(g) for g in groups.values())
        if groups
        else None,
    }


def protection(rows: Sequence[Record]) -> Record:
    result: Record = {}
    for state in ("R", "N"):
        for drop, suffix in ((True, "drop"), (False, "no_drop")):
            pnl = [
                r["leg_pnl"]
                for r in rows
                if r["complete"] and r["state"] == state and r["real_drop"] is drop
            ]
            result[f"{state}_{suffix}"] = {
                "exits": len(pnl),
                "pnl_sum": math.fsum(pnl),
                "pnl_mean": statistics.fmean(pnl) if pnl else None,
            }
    return result


def _vol_cell(rows: Sequence[Record]) -> Record:
    follow = [r["real_drop"] for r in rows if r["real_drop"] is not None]
    return {
        "complete": len(rows),
        "whipsaw": statistics.fmean(r["whipsaw"] for r in rows) if rows else None,
        "real_drop": statistics.fmean(follow) if follow else None,
        "follow": len(follow),
    }


def _weighted(
    tiers: Sequence[Record], metric: str, weight: str
) -> tuple[float | None, float | None]:
    used = [tier for tier in tiers if tier["R"][weight] > 0]
    total = sum(tier["R"][weight] for tier in used)
    if total == 0 or any(tier["N"][metric] is None for tier in used):
        return None, None
    return (
        math.fsum(tier["R"][metric] * tier["R"][weight] for tier in used) / total,
        math.fsum(tier["N"][metric] * tier["R"][weight] for tier in used) / total,
    )


def vol_tiers(rows: Sequence[Record]) -> Record:
    usable = [r for r in rows if r["complete"] and r["vol63"] is not None]
    result: Record = {
        "cuts": None,
        "tiers": [],
        "R_whipsaw": None,
        "N_weighted_whipsaw": None,
        "weighted_gap": None,
        "R_real_drop": None,
        "N_weighted_real_drop": None,
        "weighted_drop_gap": None,
    }
    if len(usable) < 3:
        return result
    q1, q2 = (float(v) for v in np.quantile([r["vol63"] for r in usable], [1 / 3, 2 / 3]))
    groups: list[dict[str, list[Record]]] = [{"R": [], "N": []} for _ in range(3)]
    for row in usable:
        if row["state"] in ("R", "N"):
            tier = 0 if row["vol63"] <= q1 else 1 if row["vol63"] <= q2 else 2
            groups[tier][row["state"]].append(row)
    tiers = [{state: _vol_cell(group[state]) for state in ("R", "N")} for group in groups]
    r_whip, n_whip = _weighted(tiers, "whipsaw", "complete")
    r_drop, n_drop = _weighted(tiers, "real_drop", "follow")
    result.update(
        cuts=[q1, q2],
        tiers=tiers,
        R_whipsaw=r_whip,
        N_weighted_whipsaw=n_whip,
        weighted_gap=_gap(n_whip, r_whip),
        R_real_drop=r_drop,
        N_weighted_real_drop=n_drop,
        weighted_drop_gap=_gap(r_drop, n_drop),
    )
    return result


def concentration(main: Record, weighted: Record) -> Record:
    pooled, by_episode = main["R"]["whipsaw"], weighted["whipsaw"]
    return {
        "pooled": pooled,
        "by_episode": by_episode,
        "flag": abs(by_episode - pooled) > CONCENTRATION_LINE + EPSILON
        if pooled is not None and by_episode is not None
        else None,
    }


def vol_reading(decision: Record, tables: Record) -> bool | None:
    weighted, gap = tables["vol"]["weighted_gap"], tables["main"]["gap"]
    if decision["verdict"] != "holds" or weighted is None or gap is None:
        return None
    return bool(weighted < MOSTLY_VOL_SHARE * gap)


def judge(check: Record, fid: Record, tables: Record) -> Record:
    reasons = []
    if len(check["excluded"]) > MAX_EXCLUDED:
        reasons.append(f"不用的行业 {len(check['excluded'])} 个，超过 10 个")
    main = tables["main"]
    if main["R"]["exits"] + main["N"]["exits"] == 0:
        reasons.append("没有快腿退出")
    if reasons:
        return {"verdict": "incomplete", "reasons": reasons}
    if not fid["passed"]:
        return {"verdict": "fidelity_miss", "reasons": []}
    if main["R"]["complete"] < MIN_R_EXITS:
        return {"verdict": "too_few", "reasons": []}
    gap_ok = main["gap"] is not None and main["gap"] >= GAP_LINE - EPSILON
    halves_ok = all(half["ok"] is not False for half in tables["halves"].values())
    drop_gap = tables["drop_year"]["table"]["gap"]
    drop_ok = drop_gap is not None and drop_gap >= DROP_YEAR_LINE - EPSILON
    return {
        "verdict": "holds" if gap_ok and halves_ok and drop_ok else "not_holds",
        "reasons": [],
        "gap_ok": gap_ok,
        "halves_ok": halves_ok,
        "drop_ok": drop_ok,
    }


def tables_of(exits: Sequence[Record], built: Built) -> Record:
    fast = [r for r in exits if r["leg"] == "fast"]
    slow = [r for r in exits if r["leg"] == "slow"]
    main, weighted = table(fast), episode_weighted(fast)
    halves = {}
    for label, first, last in HALVES:
        half = table([r for r in fast if first <= r["year"] <= last])
        judged = half["R"]["complete"] >= HALF_MIN_R
        half.update(
            judged=judged, ok=(half["gap"] is not None and half["gap"] > 0) if judged else None
        )
        halves[label] = half
    return {
        "main": main,
        "halves": halves,
        "drop_year": drop_year(fast),
        "episode_weighted": weighted,
        "concentration": concentration(main, weighted),
        "vol": vol_tiers(fast),
        "protection": protection(fast),
        "tech": {
            name: table([r for r in fast if r["industry"] == name])
            for name in TECH
            if name in built.industries
        },
        "slow": table(slow),
    }


def read(inputs: Inputs, progress: Progress | None = None) -> tuple[Record, list[Record]]:
    built = build(inputs, progress)
    check = check_summary(built)
    exits = all_exits(built)
    fid = fidelity(built)
    tables = tables_of(exits, built)
    decision = judge(check, fid, tables)
    result = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": SNAPSHOT_ID,
        "rules_sha256": inputs.rules_sha256,
        "check": check,
        "fidelity": fid,
        "tables": tables,
        **decision,
        "mostly_vol": vol_reading(decision, tables),
    }
    return result, exits


READING = {
    "incomplete": "没读：数据或核对出了问题，表照印，不判；修什么先回循环线程。",
    "fidelity_miss": "保真没过：我们的暴涨定义抓的不是 GSY 那种状态，表只印不读；"
    "`research/families.md` 新开「价格路径」一行，记「定义对不上 GSY」，停。",
    "too_few": "暴涨状态下的快腿退出太少，读不出东西；"
    "`research/families.md` 新开「价格路径」一行，记一句，停。",
    "holds": "暴涨以后的跌破更可信：11-20 续期卡问要不要在前瞻账本加一行"
    "「F0001 加暴涨状态」，连同保护表一起看；不开正式题。",
    "not_holds": "暴涨状态不让快腿的跌破更可信：`research/families.md` 新开"
    "「价格路径」一行，记一句，停。",
}
VOL_READING = {
    True: "按波动加权以后，N − R 不到合起来的一半：多半是高波动的状态，不是暴涨本身；"
    "11-20 续期卡问的是这个。",
    False: "按波动加权以后，N − R 还有合起来的一半以上：不只是高波动。",
}
GROUP_HEADERS = (
    "组",
    "完整退出",
    "被来回打",
    "快腿得失中位（点）",
    "平均（点）",
    "合计（点）",
    "跟着真跌",
    "出去月数中位",
    "未完",
    "不满 252 天",
    "63 天内回到退出价",
)


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def _points(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:+.2f}"


def _difference(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:+.1f} 个百分点"


def _number(value: int | float | None) -> str:
    return "—" if value is None else f"{value:g}"


def _markdown(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    return [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
        *("| " + " | ".join(row) + " |" for row in rows),
        "",
    ]


def _group(part: Record) -> list[str]:
    rows = []
    for state, label in (("R", "暴涨（R）"), ("N", "没暴涨（N）")):
        cell = part[state]
        rows.append(
            [
                label,
                str(cell["complete"]),
                _pct(cell["whipsaw"]),
                _points(cell["pnl_median"]),
                _points(cell["pnl_mean"]),
                _points(cell["pnl_sum"]),
                _pct(cell["real_drop"]),
                _number(cell["months_median"]),
                str(cell["open"]),
                str(cell["no_follow"]),
                _pct(cell["back63"]),
            ]
        )
    return _markdown(GROUP_HEADERS, rows)


def render(result: Record) -> str:
    check, fid, tables = result["check"], result["fidelity"], result["tables"]
    main, vol, dropped = tables["main"], tables["vol"], tables["drop_year"]
    lines = [
        f"# 行业暴涨以后的跌破可不可信（{result['run_id']}）",
        "",
        f"- 依据：`{result['owner_decision']}`，第 6 期章程第 23 节；"
        "只印，不是探针、不是试验、不进账本、不模拟新规则，不是长牛代价的读数。",
        f"- 数据：快照 `{result['snapshot_id']}`，49 个行业市值加权日频和大盘因子，"
        f"截止 {LAST_DAY.isoformat()}；规则文件 `research/{QUESTION}/prereg.yaml`，"
        f"sha256 `{result['rules_sha256']}`；策略 `fast_slow`、`fast_only`、`slow_only`。",
        f"- 代码：{result.get('provenance', {}).get('commit') or '—'}",
        "",
        f"## 判定：`{result['verdict']}`",
        "",
        READING[result["verdict"]],
        "",
    ]
    if result["reasons"]:
        lines.extend(["原因：" + "；".join(result["reasons"]) + "。", ""])
    if result["mostly_vol"] is not None:
        lines.extend([VOL_READING[result["mostly_vol"]], ""])
    lines.extend(
        [
            "## GSY 保真核对",
            "",
            f"起点在 1929-01..2008-12 的暴涨段 {fid['episodes']} 段，"
            f"24 个月内崩（回撤到 −40%）{fid['crashes']} 段，{_pct(fid['share'])}；"
            "GSY 印的 40 段、21 段、53.0%。线：至少 15 段，比例在 30.0%–76.0%。",
            "",
            f"只印：所有有定义的月末之后 24 个月崩的比例 {_pct(fid['unconditional'])}"
            f"（{fid['unconditional_months']} 个月末；GSY 无条件 14.0%）；"
            f"1970 年以后 {_pct(fid['after_1970'])}（GSY 11.0%）。",
            "",
            "## 主表：快腿的退出，所有行业合起来",
            "",
            *_group(main),
            f"N − R（被来回打的比例）：{_difference(main['gap'])}；线 ≥ 15 个百分点。"
            f"状态不定的退出 {main['undetermined']} 次，不进比较。",
            "",
            "## 两半",
            "",
        ]
    )
    for label, _, _ in HALVES:
        half = tables["halves"][label]
        note = "判" if half["judged"] else "R 组不到 20 次，只印"
        lines.extend(
            [
                f"### {label}（{note}）",
                "",
                *_group(half),
                f"N − R：{_difference(half['gap'])}。",
                "",
            ]
        )
    lines.extend(
        [
            "## 去掉一年",
            "",
            f"R 组完整退出最多的一年：{_number(dropped['year'])}（{dropped['removed']} 次）。"
            f"去掉这一年以后 N − R：{_difference(dropped['table']['gap'])}；线 ≥ 10 个百分点。",
            "",
            *_group(dropped["table"]),
            "## 波动对照（只印，不进判定）",
            "",
        ]
    )
    if vol["cuts"] is None:
        lines.extend(["完整的快腿退出不到 3 次，不分档。", ""])
    else:
        lines.extend(
            [
                "完整的快腿退出按退出前 63 个交易日的年化波动三等分，"
                f"切点 {_pct(vol['cuts'][0])}、{_pct(vol['cuts'][1])}。",
                "",
            ]
        )
        vol_rows = []
        for label, tier in zip(("低", "中", "高"), vol["tiers"], strict=True):
            r, n = tier["R"], tier["N"]
            vol_rows.append(
                [
                    label,
                    str(r["complete"]),
                    _pct(r["whipsaw"]),
                    _pct(r["real_drop"]),
                    str(n["complete"]),
                    _pct(n["whipsaw"]),
                    _pct(n["real_drop"]),
                ]
            )
        lines.extend(
            _markdown(
                (
                    "波动档",
                    "R 完整退出",
                    "R 被来回打",
                    "R 跟着真跌",
                    "N 完整退出",
                    "N 被来回打",
                    "N 跟着真跌",
                ),
                vol_rows,
            )
        )
    lines.extend(
        [
            f"N 组按 R 组的波动分布加权：被来回打 {_pct(vol['N_weighted_whipsaw'])}"
            f"（R {_pct(vol['R_whipsaw'])}），N − R {_difference(vol['weighted_gap'])}，"
            f"合起来是 {_difference(main['gap'])}；"
            f"跟着真跌 R − N {_difference(vol['weighted_drop_gap'])}。",
            "",
            "## 只印",
            "",
        ]
    )
    weighted, conc = tables["episode_weighted"], tables["concentration"]
    lines.extend(
        [
            f"按段平均：{weighted['episodes']} 段，被来回打 {_pct(weighted['whipsaw'])}"
            f"（合起来 {_pct(conc['pooled'])}）。"
            + ("差超过 10 个百分点：几段定的。" if conc["flag"] else ""),
            "",
            "保护表（快腿只在暴涨时开，慢熊里丢多少的那一头；不判）：",
            "",
        ]
    )
    protection_rows = []
    for state in ("R", "N"):
        for key, label in (("drop", "是"), ("no_drop", "否")):
            cell = tables["protection"][f"{state}_{key}"]
            protection_rows.append(
                [
                    state,
                    label,
                    str(cell["exits"]),
                    _points(cell["pnl_sum"]),
                    _points(cell["pnl_mean"]),
                ]
            )
    lines.extend(
        _markdown(("组", "跟着真跌", "次数", "快腿得失合计（点）", "平均（点）"), protection_rows)
    )
    lines.extend(["按十年的快腿退出次数（全部，含未完）：", ""])
    lines.extend(
        _markdown(
            ("十年", "R", "N", "不定"),
            [
                [label, *(str(counts[s]) for s in STATES)]
                for label, counts in check["decades"].items()
            ],
        )
    )
    for name, part in tables["tech"].items():
        lines.extend([f"科技行业 {name}（次数少，只印）：", "", *_group(part)])
    lines.extend(
        [
            f"美股大盘两年原始收益 ≥ 100% 的月末数：{check['market_runup_months']}。",
            "",
            "慢腿的退出（只印）：",
            "",
            *_group(tables["slow"]),
            "## 数据和核对",
            "",
        ]
    )
    first = "（" + "、".join(check["identity_first"]) + "）" if check["identity_first"] else ""
    lines.extend(
        [
            f"用了 {check['used']} 个行业；不用的：{'、'.join(check['excluded']) or '无'}。"
            f"F0001 核对不等 {check['identity_mismatches']} 处{first}；"
            f"暴涨条件成立的月末 {check['runup_months']} 个，其中五年那条不用的 "
            f"{check['waived_months']} 个；暴涨段 {check['episodes']} 段"
            f"（窗口里 {check['episodes_in_window']} 段）。"
            f"照 GSY 不用的：{'、'.join(check['left_out']) or '无'}。",
            "",
        ]
    )
    vol_text = []
    for state in ("R", "N"):
        stats = check["vol63"][state]
        vol_text.append(
            f"{state} {stats['n']} 次，中位 {_pct(stats['median'])}"
            f"（四分位 {_pct(stats['q25'])}–{_pct(stats['q75'])}）"
        )
    lines.extend(
        ["快腿退出前 63 个交易日的年化波动（全部退出，含未完）：" + "；".join(vol_text) + "。", ""]
    )
    data_rows = []
    for row in check["industries"]:
        counts = "/".join(str(row["fast"][s]) for s in STATES) if row["used"] else "—"
        data_rows.append(
            [
                row["name"],
                row["first"] or "—",
                row["last"] or "—",
                str(row["missing"]),
                "用" if row["used"] else f"不用（{row['reason']}）",
                counts,
            ]
        )
    lines.extend(
        _markdown(("行业", "起", "止", "去掉的日子", "用没用", "快腿退出 R/N/不定"), data_rows)
    )
    lines.extend(
        [
            "## 局限",
            "",
            "- 和 GSY 的差别：日频算月末水平对 CRSP 月度，没有「至少 10 家公司」那条；"
            "扣大盘是两段累计收益直接相减，和 GSY 一样不用「其他」。",
            "- 快腿得失是一条腿一趟的近似账，不是新规则的净值，不含成本或执行差异。",
            "- 行业之间常常同时暴涨，退出不独立；GSY 按日历年聚类，"
            "这里只做两半和去掉一年，不当独立样本推断。",
            "",
            "production_effect=none；broker_action=none。",
            "",
        ]
    )
    return "\n".join(lines)


def _write_json(value: Record, path: Path) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=1, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_check(check: Record, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(check, out_dir / CHECK)


def write_result(result: Record, exits: Sequence[Record], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / EXITS).write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
            for row in exits
        ),
        encoding="utf-8",
    )
    _write_json(result, out_dir / SUMMARY)
    (out_dir / REPORT).write_text(render(result), encoding="utf-8")


def load_inputs(root: Path = PROJECT_ROOT) -> Inputs:
    snapshot = signal_sources.load_snapshot(root, SNAPSHOT_ID)
    loaded = load_settings()
    rules = lg.research_root(loaded) / QUESTION / "prereg.yaml"
    prereg = load_prereg(rules)
    entries = {name: prereg.strategy(rule) for name, rule in RULES.items()}
    return Inputs(
        cut(snapshot.frames[INDUSTRIES]),
        cut(snapshot.frames[FACTORS]),
        entries,
        hashlib.sha256(rules.read_bytes()).hexdigest(),
    )


def _load() -> Inputs:
    try:
        return load_inputs()
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"runup_state_table: 数据载入失败：{exc}")
        raise typer.Exit(1) from exc


def _progress(name: str) -> None:
    typer.echo(f"runup_state_table: 建好 {name}")


@app.callback()
def cli() -> None:
    """只印行业暴涨以后的快腿退出表，不进账本、不模拟新规则。"""


@app.command("check")
def check_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        built = build(inputs, _progress)
    except ValueError as exc:
        typer.echo(f"runup_state_table: 建数据失败：{exc}")
        raise typer.Exit(1) from exc
    check = check_summary(built)
    write_check(check, out)
    fast, vol = check["exits"]["fast"], check["vol63"]
    typer.echo(
        f"runup_state_table: 用了 {check['used']} 个行业，不用 {len(check['excluded'])} 个；"
        f"快腿退出 R {fast['R']}、N {fast['N']}、不定 {fast['undetermined']}；"
        f"暴涨段 {check['episodes_in_window']} 段（1929-01..2008-12）；"
        f"退出前 63 天波动中位 R {_pct(vol['R']['median'])}、N {_pct(vol['N']['median'])}"
    )


@app.command("run")
def run_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        result, exits = read(inputs, _progress)
    except ValueError as exc:
        typer.echo(f"runup_state_table: 建数据失败：{exc}")
        raise typer.Exit(1) from exc
    result["provenance"] = git_provenance(PROJECT_ROOT)
    write_check(result["check"], out)
    write_result(result, exits, out)
    typer.echo(f"runup_state_table: 判定 {result['verdict']}")


if __name__ == "__main__":
    app()
