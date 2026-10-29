"""失业率趋势能不能挑掉 F0001 的假退出（TRADING-2565-89）。

依据 owner_decision:TRADING-2565:2026-10-29:print_only_tables、第 6 期章程第 23 节，
四张只印的表的第三张。快照的美股大盘日频因子和三条 FRED 月度失业率截到
2010-12-31；月底 M 只用 M − 1 那个月的值，不顺延。F0001 和两条腿照 QQQ-Q3
预注册，用评估台的 signal_exposure。逐次问两条腿的退出：要是等失业率趋势向上
（失业率高于自己 12 个月均值）才出，这条腿这一趟多得多少；扣掉多在场月数按
全窗口月均超额该得的那份，再和不看失业率、只晚同样月数出去的对照比。
只量博客规则晚出去的一半；只印，不是探针、不是试验、不进账本、不模拟新规则的
仓位线，不是长牛代价的读数。check 只数次数和分布，不印逐趟的日期，不碰退出以后的收益。
"""

from __future__ import annotations

import hashlib
import json
import math
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
from ai_trading_system.research_probes import macro_reentry_table as mra
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import runup_state_table as rs
from ai_trading_system.research_probes import state_table as st

RUN_ID = "TRADING-2565-89"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-29:print_only_tables"
SNAPSHOT_ID = "sig-c68ae8decccd"
FACTORS = cp.FACTORS
# 两条腿的退出从 1929-01 的月末数到 2010-12；
# 全窗口的月均超额和标准差也按这 984 个月算。
FIRST_MONTH = pd.Period("1929-01", freq="M")
LAST_MONTH = pd.Period("2010-12", freq="M")
# F0001 和两条腿：QQQ-Q3 预注册里的策略 id；两条腿合起来判一次。
QUESTION, RULES, LEGS, LEG_SHARE, POOLED = "QQQ-Q3", rs.RULES, ("fast", "slow"), 0.5, "both"
# 三条失业率（需求第 2.1 节）：按这个次序选序列；范围是 FRED 页面上的，载入时核对。
SERIES = ("unrate", "unrate_nber_b", "unrate_nber_a")
DATASETS = {
    "unrate": "UNRATE",
    "unrate_nber_b": "M0892BUSM156SNBR",
    "unrate_nber_a": "M0892AUSM156SNBR",
}
RANGES = {
    "unrate": (pd.Period("1948-01", freq="M"), LAST_MONTH),
    "unrate_nber_b": (pd.Period("1940-01", freq="M"), pd.Period("1946-12", freq="M")),
    "unrate_nber_a": (pd.Period("1929-04", freq="M"), pd.Period("1942-06", freq="M")),
}
# 状态（需求第 3.2 节，跑之前写死）：（均值的月数，U 用 ≥，不定也拦）。
MAIN = (12, False, False)
# 只印的敏感度，不判。
SENSITIVITY = {
    "mean_6": (6, False, False),
    "mean_18": (18, False, False),
    "ge": (12, True, False),
    "undefined_blocks": (12, False, True),
}
# 只印的数据核对：两条 NBER 序列都满 12 个月的 M*。
AGREE = (pd.Period("1940-12", freq="M"), pd.Period("1942-06", freq="M"))
# 判定（需求第 4 节，跑之前写死；0.25 是循环线程定的，没有依据）。
MIN_CHANGED, HALF_MIN, PROTECT_PART = 20, 5, 0.25
HALVES = (("1929–1969", 1929, 1969), ("1970–2010", 1970, 2010))
# 读法：没信息的条件判 holds 的概率（复盘线程模拟，推断）。
NULL_HOLDS = 0.17
# 比较的浮点容差，不是研究阈值。
EPSILON = 1e-9
VERDICTS = ("too_few", "holds", "not_holds")
STATES = ("U", "N", "undefined")
CHECK, TRIPS, SUMMARY, REPORT = (
    "unemp_check.json",
    "unemp_trips.jsonl",
    "unemp_table.json",
    "unemp_table.md",
)
LEG_NAMES = {"fast": "快腿", "slow": "慢腿"}

Record = dict[str, Any]
Segment = tuple[str, pd.Period, pd.Period]
Variant = tuple[int, bool, bool]
app = typer.Typer()


@dataclass(frozen=True)
class Inputs:
    factors: pd.DataFrame
    rates: pd.DataFrame
    sources: list[Record]
    segments: tuple[Segment, ...]
    entries: dict[str, StrategyEntry]
    rules_sha256: str


@dataclass(frozen=True)
class Built:
    months: pd.DataFrame
    rates: pd.DataFrame
    excess: np.ndarray[Any, Any]
    where: dict[pd.Period, int]
    exits: dict[str, list[tuple[pd.Period, pd.Period | None]]]
    mu: float
    sigma: float
    segments: tuple[Segment, ...]
    sources: list[Record]


def variants() -> dict[str, Variant]:
    return {"main": MAIN, **SENSITIVITY}


def unemployment_frame(frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    index = pd.period_range(FIRST_MONTH - 12, LAST_MONTH, freq="M")
    result = pd.DataFrame(index=index)
    for source in SERIES:
        if source not in frames or "value" not in frames[source]:
            raise ValueError(f"snapshot has no {source} values")
        frame = frames[source]
        months = pd.DatetimeIndex(frame.index).to_period("M")
        keep = months <= LAST_MONTH
        months = months[keep]
        if months.has_duplicates:
            raise ValueError(f"{source} repeats a month")
        values = pd.to_numeric(frame.loc[keep, "value"], errors="coerce")
        result[source] = pd.Series(values.to_numpy(dtype=float), index=months).reindex(index)
    return result


def source_problems(rates: pd.DataFrame) -> list[str]:
    problems = []
    for source in SERIES:
        positions = np.flatnonzero(rates[source].notna().to_numpy())
        if not len(positions):
            problems.append(f"{source}: no values")
            continue
        first, last = rates.index[positions[0]], rates.index[positions[-1]]
        start, end = RANGES[source]
        if first != start:
            problems.append(f"{source}: starts {first}, not {start}")
        if last != end:
            problems.append(f"{source}: ends {last}, not {end}")
    return problems


def missing_months(rates: pd.DataFrame, source: str) -> list[str]:
    positions = np.flatnonzero(rates[source].notna().to_numpy())
    if not len(positions):
        return []
    first, last = rates.index[positions[0]], rates.index[positions[-1]]
    span = rates[source].reindex(pd.period_range(first, last, freq="M"))
    return [str(p) for p in span.index[span.isna()]]


def spec_problems(specs: Mapping[str, SourceSpec]) -> list[str]:
    problems = []
    for source in SERIES:
        spec = specs.get(source)
        if spec is None or (
            spec.provider,
            spec.dataset,
            spec.frequency,
            spec.release_lag_months,
            spec.known_lag_trading_days,
        ) != ("fred", DATASETS[source], "monthly", 0, 5):
            problems.append(f"{source}: spec must be monthly FRED {DATASETS[source]}, lags 0 and 5")
    return problems


def monthly_excess(excess: np.ndarray[Any, Any], where: Mapping[pd.Period, int]) -> list[float]:
    return [
        float(excess[where[t] + 1] - excess[where[t - 1] + 1])
        for t in pd.period_range(FIRST_MONTH, LAST_MONTH, freq="M")
    ]


def assemble(
    months: pd.DataFrame,
    returns: pd.Series,
    cash: pd.Series,
    rates: pd.DataFrame,
    segments: tuple[Segment, ...],
    sources: list[Record],
) -> Built:
    daily, rf = returns.to_numpy(dtype=float), cash.to_numpy(dtype=float)
    excess = np.concatenate(([0.0], np.cumsum(np.log1p(daily) - np.log1p(rf))))
    where = rs.positions(months)
    if FIRST_MONTH - 1 not in where or LAST_MONTH not in where:
        raise ValueError("market days do not cover 1928-12..2010-12")
    return Built(
        months,
        rates,
        excess,
        where,
        {leg: rs.leg_exits(months, leg) for leg in LEGS},
        mra.window_mean(excess, where),
        float(np.std(monthly_excess(excess, where), ddof=1)),
        segments,
        sources,
    )


def build(inputs: Inputs) -> Built:
    returns, cash = rs.market_series(inputs.factors)
    return assemble(
        rs.month_targets(returns, cash, inputs.entries),
        returns,
        cash,
        inputs.rates,
        inputs.segments,
        inputs.sources,
    )


def state_at(
    rates: pd.DataFrame, month: pd.Period, window: int = 12, at_or_above: bool = False
) -> tuple[str, str | None, int]:
    known = month - 1
    values = rates.reindex(pd.period_range(known - window + 1, known, freq="M"))
    for minimum in (window, window // 2):
        for source in SERIES:
            series = values[source]
            now = series.get(known)
            count = int(series.count())
            if pd.notna(now) and count >= minimum:
                mean = float(series.mean())
                up = now >= mean - EPSILON if at_or_above else now > mean + EPSILON
                return ("U" if up else "N", source, count)
    return "undefined", None, 0


def state_table(rates: pd.DataFrame, window: int = 12, at_or_above: bool = False) -> pd.DataFrame:
    index = pd.period_range(FIRST_MONTH, LAST_MONTH, freq="M")
    rows = [state_at(rates, month, window, at_or_above) for month in index]
    table = pd.DataFrame(rows, index=index, columns=["state", "source", "values"])
    table["source"] = pd.Series([row[1] for row in rows], index=index, dtype=object)
    return table


def late_exit(
    states: pd.Series, month: pd.Period, back: pd.Period, undefined_blocks: bool = False
) -> pd.Period:
    for t in pd.period_range(month, back - 1, freq="M"):
        state = states.get(t)
        if state == "U" or (state == "undefined" and not undefined_blocks):
            return t
    return back


def timing_rows(built: Built) -> list[Record]:
    """只读两条腿的目标和失业率，不碰收益。"""

    designs = variants()
    tables = {
        (window, ge): state_table(built.rates, window, ge)
        for window, ge in dict.fromkeys((w, ge) for w, ge, _ in designs.values())
    }
    main = tables[MAIN[:2]]
    rows: list[Record] = []
    for leg in LEGS:
        for month, back in built.exits[leg]:
            late = (
                {
                    name: late_exit(tables[(window, ge)]["state"], month, back, blocks)
                    for name, (window, ge, blocks) in designs.items()
                }
                if back is not None
                else {}
            )
            rows.append(
                {
                    "leg": leg,
                    "month": str(month),
                    "year": int(month.year),
                    "complete": back is not None,
                    "reentry": str(back) if back is not None else None,
                    "months_out": back.ordinal - month.ordinal if back is not None else None,
                    "state": main.at[month, "state"],
                    "source": main.at[month, "source"],
                    "slow_bears": mra.overlaps(built.segments, month, back),
                    "late": {name: str(t) for name, t in late.items()},
                    "late_months": {name: t.ordinal - month.ordinal for name, t in late.items()},
                }
            )
    return rows


def control_ks(rows: Sequence[Record]) -> dict[str, int | None]:
    return {
        leg: mra.control_k(
            [
                row["late_months"]["main"]
                for row in rows
                if row["leg"] == leg and row["complete"] and row["late_months"]["main"] > 0
            ]
        )
        for leg in LEGS
    }


def agreement(rates: pd.DataFrame) -> dict[str, int]:
    months, agree = 0, 0
    for known in pd.period_range(*AGREE, freq="M"):
        span = rates.reindex(pd.period_range(known - 11, known, freq="M"))
        a, b = span["unrate_nber_a"], span["unrate_nber_b"]
        a_now, b_now = a.get(known), b.get(known)
        if a_now is not None and b_now is not None and a.notna().all() and b.notna().all():
            months += 1
            a_up = float(a_now) > a.mean() + EPSILON
            b_up = float(b_now) > b.mean() + EPSILON
            agree += int(a_up == b_up)
    return {"months": months, "agree": agree}


def in_half(rows: Sequence[Record], first: int, last: int) -> list[Record]:
    return [row for row in rows if first <= row["year"] <= last]


def _timing_counts(rows: Sequence[Record], name: str = "main") -> dict[str, int]:
    return {
        "changed": sum(int(row["late_months"][name] > 0) for row in rows),
        "removed": sum(int(row["late"][name] == row["reentry"]) for row in rows),
    }


def check_summary(built: Built, rows: Sequence[Record], ks: Mapping[str, int | None]) -> Record:
    table = state_table(built.rates)
    complete = [row for row in rows if row["complete"]]
    legs = {}
    for leg in LEGS:
        part = [row for row in complete if row["leg"] == leg]
        changed = [row for row in part if row["late_months"]["main"] > 0]
        legs[leg] = {
            "complete": len(part),
            "incomplete": sum(int(row["leg"] == leg and not row["complete"]) for row in rows),
            "halves": {label: len(in_half(part, a, b)) for label, a, b in HALVES},
            "decades": {
                f"{decade}s": sum(int(decade <= row["year"] < decade + 10) for row in part)
                for decade in range(1920, 2011, 10)
            },
            "months_out": rs.quartiles([row["months_out"] for row in part]),
            "by_state": {s: sum(int(row["state"] == s) for row in part) for s in STATES},
            **_timing_counts(part),
            "late_months": rs.quartiles([row["late_months"]["main"] for row in changed]),
            "k": ks[leg],
        }
    u_share: dict[str, float | None] = {}
    years = pd.PeriodIndex(table.index).year
    for label, first, last in (("all", FIRST_MONTH.year, LAST_MONTH.year), *HALVES):
        states = table.loc[(years >= first) & (years <= last), "state"]
        defined = int(states.ne("undefined").sum())
        u_share[label] = int(states.eq("U").sum()) / defined if defined else None
    return {
        "run_id": RUN_ID,
        "snapshot_id": SNAPSHOT_ID,
        "sources": built.sources,
        "identity_mismatches": len(rs.identity_mismatches(built.months)),
        "agreement": agreement(built.rates),
        "states": {s: int(table["state"].eq(s).sum()) for s in STATES},
        "u_share": u_share,
        "series_months": {s: int(table["source"].eq(s).sum()) for s in SERIES},
        "legs": legs,
        "both": {
            "complete": len(complete),
            **_timing_counts(complete),
            "halves_changed": {
                label: _timing_counts(in_half(complete, a, b))["changed"] for label, a, b in HALVES
            },
        },
        "sensitivity": {name: _timing_counts(complete, name) for name in SENSITIVITY},
        "slow_bears": len(built.segments),
    }


def timing_check(built: Built) -> Record:
    rows = timing_rows(built)
    return check_summary(built, rows, control_ks(rows))


def out_excess(built: Built, start: pd.Period, end: pd.Period) -> float:
    return float(built.excess[built.where[end] + 1] - built.excess[built.where[start] + 1])


def trip(built: Built, month: pd.Period, late: pd.Period, back: pd.Period) -> Record:
    months = late.ordinal - month.ordinal
    gain = LEG_SHARE * out_excess(built, month, late) if months else 0.0
    return {
        "late": str(late),
        "late_months": months,
        "removed": late == back,
        "gain": gain,
        "net": gain - LEG_SHARE * months * built.mu,
    }


def priced_rows(built: Built, rows: Sequence[Record], ks: Mapping[str, int | None]) -> list[Record]:
    priced: list[Record] = []
    for original in rows:
        row = {**original, "trips": {}, "control": None}
        if row["complete"]:
            month, back = pd.Period(row["month"], freq="M"), pd.Period(row["reentry"], freq="M")
            out = out_excess(built, month, back)
            pnl = -LEG_SHARE * out
            row.update(out_excess=out, leg_pnl=pnl, false_exit=pnl < 0)
            row["trips"] = {
                name: trip(built, month, pd.Period(late, freq="M"), back)
                for name, late in row["late"].items()
            }
            k = ks[row["leg"]]
            row["control"] = (
                trip(built, month, min(month + k, back), back) if k is not None else None
            )
        priced.append(row)
    return priced


def stats(
    rows: Sequence[Record], variant: str = "main", part: str = "trips", leg: str | None = None
) -> Record:
    complete = [row for row in rows if row["complete"] and (leg is None or row["leg"] == leg)]
    taken = [row["trips"][variant] if part == "trips" else row["control"] for row in complete]
    trips = [t for t in taken if t is not None]
    changed = [t for t in trips if t["late_months"] > 0]
    return {
        "trips": len(complete),
        "changed": len(changed),
        "removed": sum(int(t["removed"]) for t in changed),
        "gain": math.fsum(t["gain"] for t in trips),
        "net": math.fsum(t["net"] for t in trips),
        "net_positive": sum(int(t["net"] > 0) for t in changed) / len(changed) if changed else None,
        "worst": min(t["gain"] for t in changed) if changed else None,
        "late_months_sum": sum(t["late_months"] for t in trips),
    }


def main_table(rows: Sequence[Record], variant: str = "main") -> Record:
    return {
        **{leg: stats(rows, variant, leg=leg) for leg in LEGS},
        POOLED: stats(rows, variant),
        "control": {
            **{leg: stats(rows, variant, "control", leg) for leg in LEGS},
            POOLED: stats(rows, variant, "control"),
        },
    }


def strength(table: Record, sigma: float) -> float | None:
    both = table[POOLED]
    if both["late_months_sum"] <= 0 or sigma <= EPSILON:
        return None
    return float(both["net"] / (LEG_SHARE * sigma * math.sqrt(both["late_months_sum"])))


def shares(table: Record) -> dict[str, float | None]:
    net = table[POOLED]["net"]
    return {leg: float(table[leg]["net"] / net) if net > EPSILON else None for leg in LEGS}


def groups(rows: Sequence[Record], leg: str) -> Record:
    result = {}
    for state in STATES:
        part = [r for r in rows if r["complete"] and r["leg"] == leg and r["state"] == state]
        result[state] = {
            "trips": len(part),
            "false_exits": sum(int(r["false_exit"]) for r in part) / len(part) if part else None,
            "leg_pnl": math.fsum(r["leg_pnl"] for r in part),
            "months_out": rs.quartiles([r["months_out"] for r in part])["median"],
        }
    return result


def bears(rows: Sequence[Record], segments: Sequence[Segment]) -> Record:
    complete = [r for r in rows if r["complete"]]

    def cell(part: Sequence[Record]) -> Record:
        saved = math.fsum(max(r["leg_pnl"], 0.0) for r in part)
        return {
            "trips": len(part),
            "saved": saved,
            "gain": math.fsum(r["trips"]["main"]["gain"] for r in part),
            "late_months": sum(r["trips"]["main"]["late_months"] for r in part),
            "line": -PROTECT_PART * saved,
        }

    return {
        "segments": {
            name: cell([r for r in complete if name in r["slow_bears"]]) for name, _, _ in segments
        },
        "protection": cell([r for r in complete if r["slow_bears"]]),
    }


def judge(tables: Record) -> Record:
    main = tables["main"]
    reasons = []
    if main[POOLED]["changed"] < MIN_CHANGED:
        reasons.append(f"改了的趟 {main[POOLED]['changed']} 趟，少于 20 趟")
    for label, _, _ in HALVES:
        count = tables["halves"][label][POOLED]["changed"]
        if count < HALF_MIN:
            reasons.append(f"{label} 改了的趟 {count} 趟，少于 5 趟")
    if reasons:
        return {"verdict": "too_few", "reasons": reasons}
    save_ok = main[POOLED]["net"] > EPSILON
    control_ok = main[POOLED]["net"] > main["control"][POOLED]["net"] + EPSILON
    halves_judged = {
        label: tables["halves"][label][POOLED]["net"] > EPSILON for label, _, _ in HALVES
    }
    halves_ok = all(halves_judged.values())
    protection = tables["bears"]["protection"]
    protect_ok = protection["gain"] >= protection["line"] - EPSILON
    return {
        "verdict": "holds" if save_ok and control_ok and halves_ok and protect_ok else "not_holds",
        "reasons": [],
        "save_ok": save_ok,
        "control_ok": control_ok,
        "halves_judged": halves_judged,
        "halves_ok": halves_ok,
        "protect_ok": protect_ok,
    }


def readings(decision: Record, table: Record, z: float | None) -> list[str]:
    if decision["verdict"] != "holds":
        return []
    z_text = "—" if z is None else f"{z:.2f}"
    lines = [f"没信息的条件约六分之一也会判 holds（复盘线程模拟，推断）；强度 z = {z_text}。"]
    negative = next((leg for leg in LEGS if table[leg]["net"] < -EPSILON), None)
    if negative is not None:
        other = next(leg for leg in LEGS if leg != negative)
        lines.append(
            f"主要省在{LEG_NAMES[other]}，{LEG_NAMES[negative]}是负的"
            f"（{table[negative]['net'] * 100:+.2f} 点）。"
        )
    else:
        for leg, share in shares(table).items():
            if share is not None and share > 0.5 + EPSILON:
                lines.append(f"失业率过滤主要省在{LEG_NAMES[leg]}。")
    lines.append("不因为一条腿的数好就只过滤那一条腿：那是看了结果的选择，要另开预注册。")
    return lines


def trip_record(row: Record) -> Record:
    return {key: row[key] for key in sorted(row)}


def read(inputs: Inputs) -> tuple[Record, list[Record]]:
    built = build(inputs)
    rows = timing_rows(built)
    ks = control_ks(rows)
    check = check_summary(built, rows, ks)
    priced = priced_rows(built, rows, ks)
    main = main_table(priced)
    tables = {
        "main": main,
        "halves": {label: main_table(in_half(priced, a, b)) for label, a, b in HALVES},
        "groups": {leg: groups(priced, leg) for leg in LEGS},
        "bears": bears(priced, built.segments),
        "sensitivity": {name: main_table(priced, name)[POOLED] for name in SENSITIVITY},
    }
    decision = judge(tables)
    z = strength(main, built.sigma)
    result = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": SNAPSHOT_ID,
        "rules_sha256": inputs.rules_sha256,
        "check": check,
        "mu": built.mu,
        "sigma": built.sigma,
        "tables": tables,
        "decision": decision,
        "strength": z,
        "shares": shares(main),
        "readings": readings(decision, main, z),
    }
    return result, [trip_record(row) for row in priced]


VERDICT_TEXT = {
    "holds": "成立：失业率趋势挑掉的多半是假退出，晚出去省下的比丢的多",
    "not_holds": "不成立",
    "too_few": "改了的趟太少，不判",
}
MAIN_HEADERS = (
    "行",
    "完整的趟",
    "改了的趟",
    "整趟没有",
    "Σ g（点）",
    "Σ g′（点）",
    "g′ > 0",
    "最差的 g（点）",
    "多在场的月数",
)


def _number(value: int | float | None) -> str:
    return "—" if value is None else f"{value:g}"


def _points(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:+.2f}"


def _pct(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def _yes(value: bool | None) -> str:
    return "—" if value is None else "是" if value else "否"


def _markdown(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    return [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
        *("| " + " | ".join(row) + " |" for row in rows),
        "",
    ]


def _main_row(label: str, part: Record) -> list[str]:
    return [
        label,
        _number(part["trips"]),
        _number(part["changed"]),
        _number(part["removed"]),
        _points(part["gain"]),
        _points(part["net"]),
        _pct(part["net_positive"]),
        _points(part["worst"]),
        _number(part["late_months_sum"]),
    ]


def _main_rows(table: Record) -> list[list[str]]:
    rows = []
    for key, label in (*LEG_NAMES.items(), (POOLED, "两条腿合起来")):
        rows.append(_main_row(label, table[key]))
        rows.append(_main_row(f"对照：晚 k 个月（{label}）", table["control"][key]))
    return rows


def _slow_row(row: Record) -> list[str]:
    part = row["trips"].get("main")
    return [
        row["month"],
        row["reentry"] or "未完",
        _number(row["months_out"]),
        _points(row.get("leg_pnl")),
        row["state"],
        part["late"] if part is not None else "—",
        _points(part["gain"]) if part is not None else "—",
        _points(part["net"]) if part is not None else "—",
        "、".join(row["slow_bears"]) or "—",
    ]


def render(result: Record) -> str:
    check, tables, decision = result["check"], result["tables"], result["decision"]
    verdict = decision["verdict"]
    lines = [
        f"# 失业率趋势能不能挑掉 F0001 的假退出（{result['run_id']}）",
        "",
        f"- 依据：`{result['owner_decision']}`，第 6 期章程第 23 节第 23.3 条；"
        "只印，不是探针、不是试验、不进账本、不模拟新规则的仓位线，不是长牛代价的读数。",
        f"- 数据：信号快照 `{result['snapshot_id']}`，截止 2010-12-31；"
        f"规则文件 `research/{QUESTION}/prereg.yaml`，sha256 `{result['rules_sha256']}`；"
        "两条腿 `fast_only`、`slow_only`。",
        f"- 代码：{result.get('provenance', {}).get('commit') or '—'}",
        "",
        "## 判定",
        "",
        "两条腿合起来判一次；每条腿单独的数、对照和敏感度不判。",
        "",
        f"- `{verdict}`，{VERDICT_TEXT[verdict]}。",
    ]
    if verdict == "too_few":
        lines.append("  - " + "；".join(decision["reasons"]) + "。")
    else:
        protection = tables["bears"]["protection"]
        lines.append(
            f"  - 省下：Σ g′ > 0 {_yes(decision['save_ok'])}；"
            f"大于对照 {_yes(decision['control_ok'])}。两半 {_yes(decision['halves_ok'])}。"
            f"不伤慢熊：Σ g {_points(protection['gain'])} ≥ {_points(protection['line'])} "
            f"{_yes(decision['protect_ok'])}。"
        )
    lines.extend(f"- {text}" for text in result["readings"])
    share = result["shares"]
    fast_share = _number(round(share["fast"], 3) if share["fast"] is not None else None)
    slow_share = _number(round(share["slow"], 3) if share["slow"] is not None else None)
    z = result["strength"]
    z_text = "—" if z is None else f"{z:.2f}"
    lines.extend(
        [
            "",
            "## 主表：美股大盘 1929–2010，F0001 两条腿的退出",
            "",
            f"全窗口月均超额 μ：{_points(result['mu'])} 点；"
            f"月标准差 σ：{_points(result['sigma'])} 点。",
            "g 是这条腿晚出去、从原退出月末到晚出去月末的那一半超额对数收益。",
            "g′ 是 g 扣掉多在场月数按全窗口月均超额该得的那份。",
            f"强度 z（只印）{z_text}；份额（只印）快腿 {fast_share}、慢腿 {slow_share}。",
            "",
            *_markdown(MAIN_HEADERS, _main_rows(tables["main"])),
            "## 两半",
            "",
        ]
    )
    for label, _, _ in HALVES:
        lines.extend(
            [f"### {label}", "", *_markdown(MAIN_HEADERS, _main_rows(tables["halves"][label]))]
        )
    lines.extend(["## 慢熊表", ""])
    bear_rows = []
    for name, part in (
        *tables["bears"]["segments"].items(),
        ("合计（每趟只算一次）", tables["bears"]["protection"]),
    ):
        bear_rows.append(
            [
                name,
                _number(part["trips"]),
                _points(part["saved"]),
                _points(part["gain"]),
                _number(part["late_months"]),
            ]
        )
    lines.extend(
        _markdown(
            ("段", "趟数", "F0001 省下的 Σ 得失⁺（点）", "过滤的 Σ g（点）", "多在场的月数"),
            bear_rows,
        )
    )
    lines.extend(["## 按出去那个月末的状态分组（只印）", ""])
    for leg in LEGS:
        group_rows = []
        for state in STATES:
            part = tables["groups"][leg][state]
            group_rows.append(
                [
                    state,
                    _number(part["trips"]),
                    _pct(part["false_exits"]),
                    _points(part["leg_pnl"]),
                    _number(part["months_out"]),
                ]
            )
        lines.extend(
            [
                f"### {LEG_NAMES[leg]}",
                "",
                *_markdown(
                    ("状态", "趟数", "假退出的比例", "Σ 得失（点）", "出去的月数中位"), group_rows
                ),
            ]
        )
    lines.extend(
        [
            "## 敏感度（只印，不判）",
            "",
            *_markdown(
                MAIN_HEADERS, [_main_row(name, tables["sensitivity"][name]) for name in SENSITIVITY]
            ),
            "## 慢腿每趟",
            "",
            *_markdown(
                (
                    "退出",
                    "F0001 回来",
                    "出去的月数",
                    "得失（点）",
                    "m 的状态",
                    "晚出去",
                    "g（点）",
                    "g′（点）",
                    "慢熊段",
                ),
                [_slow_row(row) for row in result.get("trips", []) if row["leg"] == "slow"],
            ),
            "## 数据和核对",
            "",
            f"F0001 核对不等 {check['identity_mismatches']} 处；"
            f"状态 U {check['states']['U']}、N {check['states']['N']}、"
            f"不定 {check['states']['undefined']} 个月；"
            f"两条 NBER 序列都满 12 个月的 {check['agreement']['months']} 个月里"
            f"状态一致 {check['agreement']['agree']} 个；"
            f"未完的趟 快腿 {check['legs']['fast']['incomplete']}、"
            f"慢腿 {check['legs']['slow']['incomplete']} 趟。",
            "",
            *_markdown(
                ("序列", "起", "止", "有值的月数", "中间缺的月", "用作状态的月数"),
                [
                    [
                        s["source"],
                        s["first"] or "—",
                        s["last"] or "—",
                        _number(s["months"]),
                        "、".join(s["missing"]) or "—",
                        _number(check["series_months"][s["source"]]),
                    ]
                    for s in check["sources"]
                ],
            ),
            "## 局限",
            "",
            "- 一条腿一趟的近似账，不是新规则的净值，不含成本；扣基准用的是全窗口月均。",
            "- 只量博客规则晚出去的一半，早回来那一半没测。",
            f"- 没信息时约 {NULL_HOLDS * 100:.0f}% 也会判 holds（复盘线程模拟，推断）。",
            "- 约 30 次慢腿退出挤在几次大熊里，不当独立样本推断。",
            "- 1929–1946 的失业率是后来回推的，「当时知道」是虚构的；"
            "UNRATE 用现在的版本；1947 年到 1948 年中不定。",
            "- 不换算成每年多少点，不和博客的数并排比；g 是 0.5 × 超额，即一条腿的那一半。",
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


def write_check(summary: Record, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(summary, out_dir / CHECK)


def write_result(result: Record, trips: Sequence[Record], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / TRIPS).write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True, allow_nan=False) + "\n"
            for row in trips
        ),
        encoding="utf-8",
    )
    _write_json(result, out_dir / SUMMARY)
    (out_dir / REPORT).write_text(render({**result, "trips": list(trips)}), encoding="utf-8")


def load_inputs(root: Path = PROJECT_ROOT) -> Inputs:
    loaded = load_settings()
    research = lg.research_root(loaded)
    gate, _ = trial.load_trial(research, st.PLAN_ID)
    problems = trial.gate_problems(gate, loaded)
    snapshot = signal_sources.load_snapshot(root, SNAPSHOT_ID)
    rates = unemployment_frame(snapshot.frames)
    problems += spec_problems({spec.source_id: spec for spec in snapshot.specs})
    problems += source_problems(rates)
    if problems:
        raise ValueError("; ".join(problems))
    window = pt.market_window(loaded, gate, "us_market")
    slow, _ = gate.markets["us_market"].segment_lists(window.window_id)
    segments = tuple(
        (spec.id, pd.Period(spec.start, freq="M"), pd.Period(spec.end, freq="M")) for spec in slow
    )
    sources = []
    for source in SERIES:
        present = np.flatnonzero(rates[source].notna().to_numpy())
        sources.append(
            {
                "source": source,
                "first": str(rates.index[present[0]]),
                "last": str(rates.index[present[-1]]),
                "months": int(len(present)),
                "missing": missing_months(rates, source),
            }
        )
    rules = research / QUESTION / "prereg.yaml"
    prereg = load_prereg(rules)
    entries = {name: prereg.strategy(rule) for name, rule in RULES.items()}
    return Inputs(
        rs.cut(snapshot.frames[FACTORS]),
        rates,
        sources,
        segments,
        entries,
        hashlib.sha256(rules.read_bytes()).hexdigest(),
    )


def _load() -> Inputs:
    try:
        return load_inputs()
    except (OSError, ValueError, KeyError, trial.TrialError) as exc:
        first_line = str(exc).splitlines()[0] if str(exc) else ""
        typer.echo(f"unemployment_filter_table: 数据载入失败：{first_line}")
        raise typer.Exit(1) from exc


@app.callback()
def cli() -> None:
    """只印失业率趋势的退出过滤表，不进账本、不模拟新规则。"""


@app.command("check")
def check_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        summary = timing_check(build(inputs))
    except ValueError as exc:
        typer.echo(f"unemployment_filter_table: 建数据失败：{exc}")
        raise typer.Exit(1) from exc
    write_check(summary, out)
    fast, slow = summary["legs"]["fast"], summary["legs"]["slow"]
    typer.echo(
        f"unemployment_filter_table: 完整的趟 快腿 {fast['complete']}、慢腿 {slow['complete']}；"
        f"改了的 快腿 {fast['changed']}、慢腿 {slow['changed']}；"
        f"对照 k 快腿 {_number(fast['k'])}、慢腿 {_number(slow['k'])}"
    )


@app.command("run")
def run_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        result, trips = read(inputs)
    except ValueError as exc:
        typer.echo(f"unemployment_filter_table: 建数据失败：{exc}")
        raise typer.Exit(1) from exc
    result["provenance"] = git_provenance(PROJECT_ROOT)
    write_check(result["check"], out)
    write_result(result, trips, out)
    typer.echo(f"unemployment_filter_table: 判定 {result['decision']['verdict']}")


if __name__ == "__main__":
    app()
