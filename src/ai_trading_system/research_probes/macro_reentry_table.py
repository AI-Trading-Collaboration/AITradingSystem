"""宏观条件能不能让 F0001 的慢腿早回来（TRADING-2565-86）。

依据 owner_decision:TRADING-2565:2026-10-29:print_only_tables、第 6 期章程第 23 节，
四张只印的表的第二张。快照 sig-ed9e988ffe7c 的美股大盘日频因子和 Shiller、
Baa、Aaa 月度序列截到 2010-12-31；宏观序列照 state_table 取月底已知的值。
F0001 和两条腿照 QQQ-Q3 预注册，用评估台的 signal_exposure。
逐次问慢腿退出后，甲（利差从退出以来的高点回落两成）、乙（CAPE 在 1871 年
以来的中位以下）要是先到，在那个月末回来，这条腿这一趟多得多少；扣掉多在场
月数按全窗口月均超额该得的那份，再和不看宏观、只等同样月数的对照比。
只印，不是探针、不是试验、不进账本、不模拟新规则的仓位线，不是长牛代价的读数。
check 只数次数和分布，不印逐次的日期，不碰退出以后的收益。
"""

from __future__ import annotations

import hashlib
import json
import math
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
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import runup_state_table as rs
from ai_trading_system.research_probes import state_table as st

RUN_ID = "TRADING-2565-86"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-29:print_only_tables"
SNAPSHOT_ID = st.SNAPSHOT_ID
FACTORS = cp.FACTORS
# 慢腿的退出从 1929-01 的月末数到 2010-12；全窗口月均超额也按这 984 个月算。
FIRST_MONTH = pd.Period("1929-01", freq="M")
LAST_MONTH = pd.Period("2010-12", freq="M")
# F0001 和两条腿：QQQ-Q3 预注册里的策略 id；这张表只看慢腿。
QUESTION, RULES, LEG, LEG_SHARE = "QQQ-Q3", rs.RULES, "slow", 0.5
# 两个回来条件（需求第 3.2 节，跑之前写死）：甲 利差回落 20%，乙 CAPE 分位 ≤ 0.5。
# 甲的高点从这次退出起算；乙是 1871 年以来的扩张分位。
CONDITIONS = {"spread": ("spread", 0.20), "cape": ("cape", 0.5)}
# 只印的敏感度，不判。
SENSITIVITY = {
    "spread_10": ("spread", 0.10),
    "spread_30": ("spread", 0.30),
    "cape_30": ("cape", 0.3),
    "cape_70": ("cape", 0.7),
}
EITHER = "either"
# 二次下跌：早回来以后到 F0001 自己回来之间，大盘比回来那天再跌 20%。
DIP = -0.20
# 判定（需求第 4 节，跑之前写死；0.25 和 ÷ 3 是循环线程定的，没有依据）。
MIN_EXITS, MIN_EARLY, HALF_MIN = 20, 6, 3
SHARE_LINE, DIP_PART = 0.25, 3
HALVES = (("1929–1969", 1929, 1969), ("1970–2010", 1970, 2010))
# 读法：乙判 holds 时，Σ g′ 一半以上来自「m 已成立」组就读成「便宜时慢腿不出」。
FILTER_SHARE = 0.5
# 比较的浮点容差，不是研究阈值。
EPSILON = 1e-9
VERDICTS = ("too_few", "holds", "not_holds")
CHECK, EXITS, SUMMARY, REPORT = (
    "macro_check.json",
    "macro_exits.jsonl",
    "macro_table.json",
    "macro_table.md",
)

Record = dict[str, Any]
Segment = tuple[str, pd.Period, pd.Period]
app = typer.Typer()


@dataclass(frozen=True)
class Inputs:
    factors: pd.DataFrame
    macro: pd.DataFrame
    sources: list[Record]
    segments: tuple[Segment, ...]
    entries: dict[str, StrategyEntry]
    rules_sha256: str


@dataclass(frozen=True)
class Built:
    months: pd.DataFrame
    states: pd.DataFrame
    excess: np.ndarray[Any, Any]
    level: np.ndarray[Any, Any]
    where: dict[pd.Period, int]
    exits: list[tuple[pd.Period, pd.Period | None]]
    mu: float
    segments: tuple[Segment, ...]
    sources: list[Record]


def macro_states(macro: pd.DataFrame) -> pd.DataFrame:
    return st.state_frame(macro)[["spread_value", "cape_pct"]]


def window_mean(excess: np.ndarray[Any, Any], where: Mapping[pd.Period, int]) -> float:
    total = excess[where[LAST_MONTH] + 1] - excess[where[FIRST_MONTH - 1] + 1]
    return float(total) / (LAST_MONTH.ordinal - FIRST_MONTH.ordinal + 1)


def assemble(
    months: pd.DataFrame,
    returns: pd.Series,
    cash: pd.Series,
    states: pd.DataFrame,
    segments: tuple[Segment, ...],
    sources: list[Record],
) -> Built:
    daily, rf = returns.to_numpy(dtype=float), cash.to_numpy(dtype=float)
    excess = np.concatenate(([0.0], np.cumsum(np.log1p(daily) - np.log1p(rf))))
    level = np.concatenate(([1.0], np.cumprod(1.0 + daily)))
    where = rs.positions(months)
    if FIRST_MONTH - 1 not in where or LAST_MONTH not in where:
        raise ValueError("market days do not cover 1928-12..2010-12")
    return Built(
        months,
        states,
        excess,
        level,
        where,
        rs.leg_exits(months, LEG),
        window_mean(excess, where),
        segments,
        sources,
    )


def build(inputs: Inputs) -> Built:
    returns, cash = rs.market_series(inputs.factors)
    return assemble(
        rs.month_targets(returns, cash, inputs.entries),
        returns,
        cash,
        macro_states(inputs.macro),
        inputs.segments,
        inputs.sources,
    )


def holds_at(
    states: pd.DataFrame, kind: str, line: float, exit_month: pd.Period, t: pd.Period
) -> bool:
    if kind == "spread":
        values = states.loc[exit_month:t, "spread_value"].dropna().to_numpy(dtype=float)
        now = float(
            states["spread_value"].reindex(pd.PeriodIndex([t], freq="M")).to_numpy(dtype=float)[0]
        )
        if not len(values) or pd.isna(now):
            return False
        top = float(np.max(values))
        return now <= (1.0 - line) * top + EPSILON
    if kind == "cape":
        now = float(
            states["cape_pct"].reindex(pd.PeriodIndex([t], freq="M")).to_numpy(dtype=float)[0]
        )
        return bool(pd.notna(now) and now <= line + EPSILON)
    raise ValueError(f"unknown condition {kind}")


def first_fire(
    states: pd.DataFrame,
    kind: str,
    line: float,
    exit_month: pd.Period,
    back: pd.Period,
) -> pd.Period | None:
    return next(
        (
            t
            for t in pd.period_range(exit_month + 1, back - 1, freq="M")
            if holds_at(states, kind, line, exit_month, t)
        ),
        None,
    )


def all_conditions() -> dict[str, tuple[str, float]]:
    return {**CONDITIONS, **SENSITIVITY}


def overlaps(
    segments: Sequence[Segment], exit_month: pd.Period, back: pd.Period | None
) -> list[str]:
    last = back if back is not None else LAST_MONTH
    return [name for name, start, end in segments if start <= last and exit_month < end]


def timing_rows(built: Built) -> list[Record]:
    """只读慢腿目标和宏观状态；不为退出以后的收益定价。"""

    rows: list[Record] = []
    for month, back in built.exits:
        at_exit = {
            name: holds_at(built.states, kind, line, month, month)
            for name, (kind, line) in all_conditions().items()
        }
        at_exit[EITHER] = at_exit["spread"] or at_exit["cape"]
        fires: dict[str, str | None] = {}
        if back is not None:
            for name, (kind, line) in all_conditions().items():
                fire = first_fire(built.states, kind, line, month, back)
                fires[name] = str(fire) if fire is not None else None
            either = [text for name in CONDITIONS if (text := fires[name]) is not None]
            fires[EITHER] = min(either) if either else None
        rows.append(
            {
                "month": str(month),
                "year": month.year,
                "complete": back is not None,
                "reentry": str(back) if back is not None else None,
                "months_out": back.ordinal - month.ordinal if back is not None else None,
                "slow_bears": overlaps(built.segments, month, back),
                "at_exit": at_exit,
                "fires": fires,
            }
        )
    return rows


def control_k(offsets: Sequence[int]) -> int | None:
    return max(1, math.floor(statistics.median(offsets) + 0.5)) if offsets else None


def control_ks(rows: Sequence[Record]) -> dict[str, int | None]:
    return {
        name: control_k(
            [
                pd.Period(row["fires"][name], freq="M").ordinal
                - pd.Period(row["month"], freq="M").ordinal
                for row in rows
                if row["complete"] and row["fires"][name] is not None
            ]
        )
        for name in CONDITIONS
    }


def out_excess(built: Built, start: pd.Period, end: pd.Period) -> float:
    return float(built.excess[built.where[end] + 1] - built.excess[built.where[start] + 1])


def trip(built: Built, exit_month: pd.Period, fire: pd.Period, back: pd.Period) -> Record:
    gain = LEG_SHARE * out_excess(built, fire, back)
    early = back.ordinal - fire.ordinal
    at, until = built.where[fire] + 1, built.where[back] + 1
    low = float(np.min(built.level[at + 1 : until + 1]) / built.level[at] - 1.0)
    return {
        "fire": str(fire),
        "after": fire.ordinal - exit_month.ordinal,
        "early_months": early,
        "gain": gain,
        "net": gain - LEG_SHARE * early * built.mu,
        "low": low,
        "dip": low <= DIP + EPSILON,
    }


def best_return(built: Built, exit_month: pd.Period, back: pd.Period, first: int) -> float:
    values = [
        out_excess(built, exit_month + offset, back)
        for offset in range(first, back.ordinal - exit_month.ordinal)
    ]
    return LEG_SHARE * max([0.0, *values])


def priced_rows(built: Built, rows: Sequence[Record], ks: Mapping[str, int | None]) -> list[Record]:
    priced: list[Record] = []
    for original in rows:
        row = {**original, "trips": {}, "control": {}}
        if row["complete"]:
            month, back = pd.Period(row["month"], freq="M"), pd.Period(row["reentry"], freq="M")
            out = out_excess(built, month, back)
            row.update(
                out_excess=out,
                leg_pnl=-LEG_SHARE * out,
                best=best_return(built, month, back, 0),
                best_reach=best_return(built, month, back, 1),
            )
            row["trips"] = {
                name: trip(built, month, pd.Period(fire, freq="M"), back)
                if fire is not None
                else None
                for name, fire in row["fires"].items()
            }
            row["control"] = {
                name: trip(built, month, month + k, back)
                if k is not None and back.ordinal - month.ordinal > k
                else None
                for name, k in ks.items()
            }
        priced.append(row)
    return priced


def stats(rows: Sequence[Record], name: str, part: str = "trips") -> Record:
    complete = [row for row in rows if row["complete"]]
    trips = [row[part][name] for row in complete if row[part][name] is not None]
    best = math.fsum(row["best"] for row in complete)
    reach = math.fsum(row["best_reach"] for row in complete)
    gain = math.fsum(t["gain"] for t in trips)
    return {
        "exits": len(complete),
        "early": len(trips),
        "early_months": rs.quartiles([t["early_months"] for t in trips]),
        "early_months_sum": sum(t["early_months"] for t in trips),
        "gain": gain,
        "net": math.fsum(t["net"] for t in trips),
        "net_positive": sum(t["net"] > 0 for t in trips) / len(trips) if trips else None,
        "share": gain / best if best > EPSILON else None,
        "share_reach": gain / reach if reach > EPSILON else None,
        "dips": sum(t["dip"] for t in trips),
        "worst": min(t["gain"] for t in trips) if trips else None,
        "best": best,
        "best_reach": reach,
    }


def in_half(rows: Sequence[Record], first: int, last: int) -> list[Record]:
    return [row for row in rows if first <= row["year"] <= last]


def groups(rows: Sequence[Record], name: str) -> Record:
    result: Record = {}
    for label, at_exit in (("at_exit", True), ("later", False)):
        trips = [
            row["trips"][name]
            for row in rows
            if row["complete"]
            and row["trips"][name] is not None
            and row["at_exit"][name] == at_exit
        ]
        result[label] = {
            "early": len(trips),
            "gain": math.fsum(t["gain"] for t in trips),
            "net": math.fsum(t["net"] for t in trips),
            "dips": sum(t["dip"] for t in trips),
        }
    return result


def condition_tables(rows: Sequence[Record], name: str) -> Record:
    table = {
        "main": stats(rows, name),
        "halves": {label: stats(in_half(rows, first, last), name) for label, first, last in HALVES},
        "groups": groups(rows, name),
    }
    if name in CONDITIONS:
        table["control"] = stats(rows, name, "control")
        table["control_halves"] = {
            label: stats(in_half(rows, first, last), name, "control")
            for label, first, last in HALVES
        }
    return table


def judge(table: Record) -> Record:
    main = table["main"]
    reasons = []
    if main["exits"] < MIN_EXITS:
        reasons.append(f"完整退出 {main['exits']} 次，少于 20 次")
    if main["early"] < MIN_EARLY:
        reasons.append(f"早到 {main['early']} 次，少于 6 次")
    if reasons:
        return {"verdict": "too_few", "reasons": reasons}
    net_ok = main["net"] > EPSILON
    control_ok = main["net"] > table["control"]["net"] + EPSILON
    share_ok = main["share"] is not None and main["share"] >= SHARE_LINE - EPSILON
    halves_judged = {
        label: half["net"] > EPSILON
        for label, half in table["halves"].items()
        if half["early"] >= HALF_MIN
    }
    halves_ok = all(halves_judged.values())
    dips_ok = main["dips"] * DIP_PART <= main["early"]
    recover_ok = net_ok and control_ok and share_ok
    return {
        "verdict": "holds" if recover_ok and halves_ok and dips_ok else "not_holds",
        "reasons": [],
        "net_ok": net_ok,
        "control_ok": control_ok,
        "share_ok": share_ok,
        "halves_judged": halves_judged,
        "halves_ok": halves_ok,
        "dips_ok": dips_ok,
        "recover_ok": recover_ok,
    }


def cheap_filter(decision: Record, table: Record) -> bool | None:
    if decision["verdict"] != "holds":
        return None
    return bool(
        table["main"]["net"] > EPSILON
        and table["groups"]["at_exit"]["net"] > FILTER_SHARE * table["main"]["net"] + EPSILON
    )


def check_summary(built: Built, rows: Sequence[Record], ks: Mapping[str, int | None]) -> Record:
    complete = [row for row in rows if row["complete"]]
    conditions = {}
    for name in [*all_conditions(), EITHER]:
        early = [row for row in complete if row["fires"][name] is not None]
        months = [
            pd.Period(row["reentry"], freq="M").ordinal
            - pd.Period(row["fires"][name], freq="M").ordinal
            for row in early
        ]
        conditions[name] = {
            "early": len(early),
            "halves": {label: len(in_half(early, first, last)) for label, first, last in HALVES},
            "early_months": rs.quartiles(months),
            "early_months_sum": sum(months),
            "at_exit": sum(row["at_exit"][name] for row in early),
            "next_month": sum(
                pd.Period(row["fires"][name], freq="M") == pd.Period(row["month"], freq="M") + 1
                for row in early
            ),
        }
    return {
        "run_id": RUN_ID,
        "snapshot_id": SNAPSHOT_ID,
        "sources": built.sources,
        "identity_mismatches": len(rs.identity_mismatches(built.months)),
        "exits": {
            "complete": len(complete),
            "incomplete": len(rows) - len(complete),
            "halves": {label: len(in_half(complete, first, last)) for label, first, last in HALVES},
            "decades": {
                f"{decade}s": sum(decade <= row["year"] < decade + 10 for row in complete)
                for decade in range(1920, 2011, 10)
            },
        },
        "months_out": rs.quartiles([row["months_out"] for row in complete]),
        "conditions": conditions,
        "control": {
            name: {
                "k": k,
                "early": sum(row["months_out"] > k for row in complete) if k is not None else 0,
            }
            for name, k in ks.items()
        },
        "slow_bears": len(built.segments),
    }


def timing_check(built: Built) -> Record:
    rows = timing_rows(built)
    return check_summary(built, rows, control_ks(rows))


def exit_record(row: Record) -> Record:
    return {key: row[key] for key in sorted(row)}


def read(inputs: Inputs) -> tuple[Record, list[Record]]:
    built = build(inputs)
    rows = timing_rows(built)
    ks = control_ks(rows)
    check = check_summary(built, rows, ks)
    priced = priced_rows(built, rows, ks)
    tables = {name: condition_tables(priced, name) for name in [*all_conditions(), EITHER]}
    decisions = {name: judge(tables[name]) for name in CONDITIONS}
    complete = [row for row in priced if row["complete"]]
    result = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "snapshot_id": SNAPSHOT_ID,
        "rules_sha256": inputs.rules_sha256,
        "check": check,
        "mu": built.mu,
        "leg_pnl": math.fsum(row["leg_pnl"] for row in complete),
        "best": math.fsum(row["best"] for row in complete),
        "best_reach": math.fsum(row["best_reach"] for row in complete),
        "tables": tables,
        "decisions": decisions,
        "cheap_filter": cheap_filter(decisions["cape"], tables["cape"]),
        "single_holds": sum(d["verdict"] == "holds" for d in decisions.values()) == 1,
    }
    return result, [exit_record(row) for row in priced]


NAMES = {
    "spread": "甲 利差回落（比这次退出以来的最高点低 20%）",
    "cape": "乙 估值便宜（CAPE 分位 ≤ 0.5）",
    "spread_10": "甲 回落 10%（只印）",
    "spread_30": "甲 回落 30%（只印）",
    "cape_30": "乙 分位 ≤ 0.3（只印）",
    "cape_70": "乙 分位 ≤ 0.7（只印）",
    "either": "甲或乙先到（只印）",
}
VERDICT_TEXT = {
    "holds": "成立：这个条件让慢腿早回来、收回得值",
    "not_holds": "不成立",
    "too_few": "次数太少，不判",
}
MAIN_HEADERS = (
    "行",
    "完整退出",
    "早到",
    "早的月数中位",
    "Σ g（点）",
    "Σ g′（点）",
    "g′ > 0",
    "收回的比例",
    "够得着的比例",
    "二次下跌",
    "最差的 g（点）",
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
        _number(part["exits"]),
        _number(part["early"]),
        _number(part["early_months"]["median"]),
        _points(part["gain"]),
        _points(part["net"]),
        _pct(part["net_positive"]),
        _pct(part["share"]),
        _pct(part["share_reach"]),
        _number(part["dips"]),
        _points(part["worst"]),
    ]


def _exit_row(row: Record) -> list[str]:
    cells = [
        row["month"],
        row["reentry"] or "未完",
        _number(row["months_out"]),
        _points(row.get("leg_pnl")),
        _points(row.get("best")),
        "、".join(row["slow_bears"]) or "—",
    ]
    for name in CONDITIONS:
        part = row["trips"].get(name)
        cells.extend(
            [
                part["fire"],
                _number(part["early_months"]),
                _points(part["gain"]),
                _points(part["net"]),
                _yes(part["dip"]),
            ]
            if part is not None
            else ["—"] * 5
        )
    return cells


def render(result: Record) -> str:
    check, tables = result["check"], result["tables"]
    lines = [
        f"# 宏观条件能不能让 F0001 的慢腿早回来（{result['run_id']}）",
        "",
        f"- 依据：`{result['owner_decision']}`，第 6 期章程第 23 节第 23.2 条；"
        "只印，不是探针、不是试验、不进账本、不模拟新规则的仓位线，不是长牛代价的读数。",
        f"- 数据：快照 `{result['snapshot_id']}`，截止 2010-12-31；"
        f"规则文件 `research/{QUESTION}/prereg.yaml`，sha256 `{result['rules_sha256']}`；"
        "只看慢腿 `slow_only`。",
        f"- 代码：{result.get('provenance', {}).get('commit') or '—'}",
        "",
        "## 判定",
        "",
        "甲、乙各判一次；「甲或乙」、对照和敏感度不判。两个条件算两次看。",
        "",
    ]
    for name in CONDITIONS:
        decision = result["decisions"][name]
        verdict = decision["verdict"]
        lines.append(f"- {NAMES[name]}：`{verdict}`，{VERDICT_TEXT[verdict]}。")
        if verdict == "too_few":
            lines.append("  - " + "；".join(decision["reasons"]) + "。")
        else:
            lines.append(
                f"  - 收回：Σ g′ > 0 {_yes(decision['net_ok'])}；"
                f"大于对照 {_yes(decision['control_ok'])}；"
                f"收回的比例 ≥ 25% {_yes(decision['share_ok'])}。"
                f"两半 {_yes(decision['halves_ok'])}；"
                f"二次下跌 ≤ 早到 ÷ 3 {_yes(decision['dips_ok'])}。"
            )
    if result["cheap_filter"]:
        lines.append(
            "- 乙判 holds，Σ g′ 一半以上来自「m 已成立」那组："
            "续期卡读成「便宜时慢腿不出」，是退出的过滤，不是早回来。"
        )
    if result["single_holds"]:
        lines.append(
            "- 只有一个条件判 holds：续期卡写「两个判的条件里的一个」；"
            "两个都没信息时，至少一个判 holds 的机会约 15%（复盘线程模拟，推断）。"
        )
    lines.extend(
        [
            "",
            "## 主表：美股大盘 1929–2010，F0001 慢腿的退出",
            "",
            f"全窗口月均超额 μ：{_points(result['mu'])} 点；"
            f"慢腿得失合计：{_points(result['leg_pnl'])} 点；"
            f"最多能收回的合计：{_points(result['best'])} 点；"
            f"其中条件够得着的：{_points(result['best_reach'])} 点。",
            "g 是慢腿在条件先到的月末回来、到 F0001 自己回来之间的超额对数收益。",
            "g′ 是 g 扣掉多在场月数按全窗口月均超额该得的那份。",
            "收回的比例的分母是所有完整退出各自最多能收回的合计，含「根本不出」。",
            "够得着的比例的分母也含所有完整退出，但最早只能在退出的下个月末回来。",
            "",
        ]
    )
    main_rows = []
    for name in CONDITIONS:
        main_rows.append(_main_row(NAMES[name], tables[name]["main"]))
        k = _number(check["control"][name]["k"])
        main_rows.append(_main_row(f"对照：等 {k} 个月", tables[name]["control"]))
    main_rows.append(_main_row(NAMES[EITHER], tables[EITHER]["main"]))
    lines.extend(_markdown(MAIN_HEADERS, main_rows))
    lines.extend(["## 两半", ""])
    for label, _, _ in HALVES:
        half_rows = []
        for name in CONDITIONS:
            half_rows.append(_main_row(NAMES[name], tables[name]["halves"][label]))
            half_rows.append(_main_row("对照", tables[name]["control_halves"][label]))
        lines.extend([f"### {label}", "", *_markdown(MAIN_HEADERS, half_rows)])
    lines.extend(["## 乙按退出那个月末分两组（只印）", ""])
    group_rows = []
    for key, label in (("at_exit", "m 已成立"), ("later", "m 以后才成立")):
        part = tables["cape"]["groups"][key]
        group_rows.append(
            [
                label,
                _number(part["early"]),
                _points(part["gain"]),
                _points(part["net"]),
                _number(part["dips"]),
            ]
        )
    lines.extend(_markdown(("组", "早到", "Σ g（点）", "Σ g′（点）", "二次下跌"), group_rows))
    spread, cape = check["conditions"]["spread"], check["conditions"]["cape"]
    # 这里数所有退出的 m 状态；check 的 at_exit 只数早到的那一组。
    at_exit_count = sum(row["at_exit"]["spread"] for row in result.get("exits", []))
    lines.extend(
        [
            f"甲在退出那个月末已经成立 {at_exit_count} 次"
            "（锚点从退出那个月起，利差为正时不会成立）；"
            f"下一个月末就到的甲 {spread['next_month']} 次、乙 {cape['next_month']} 次。",
            "",
            "## 敏感度（只印，不判）",
            "",
            *_markdown(
                MAIN_HEADERS, [_main_row(NAMES[name], tables[name]["main"]) for name in SENSITIVITY]
            ),
            "## 每次退出",
            "",
        ]
    )
    exit_headers = [
        "退出",
        "F0001 回来",
        "出去月数",
        "慢腿得失（点）",
        "最多能收回（点）",
        "慢熊段",
    ]
    for label in ("甲", "乙"):
        exit_headers.extend(
            f"{label} {suffix}"
            for suffix in ("回来月", "早的月数", "g（点）", "g′（点）", "二次下跌")
        )
    lines.extend(_markdown(exit_headers, [_exit_row(row) for row in result.get("exits", [])]))
    lines.extend(
        [
            "## 数据和核对",
            "",
            f"F0001 核对不等 {check['identity_mismatches']} 处；"
            f"完整退出 {check['exits']['complete']} 次，未完 {check['exits']['incomplete']} 次；"
            f"门里的美股大盘慢熊段 {check['slow_bears']} 段。",
            "",
            *_markdown(
                ("序列", "起", "止", "月底用的是"),
                [
                    [s["source"], s["first"] or "—", s["last"] or "—", s["offsets"]]
                    for s in check["sources"]
                ],
            ),
            "## 局限",
            "",
            "- 一条腿一趟的近似账，不是新规则的净值，不含成本；"
            "扣基准用的是全窗口月均，不是同平均敞口的固定线。",
            "- 约 30 次退出挤在几次大熊市里，不当独立样本推断；"
            "两个条件和四个敏感度是一起看的，「甲或乙」不判。",
            "- 利差低的年份（1950–60 年代）跌两成只是零点一个百分点的波动，"
            "甲在那里的早到多半是噪声。",
            "- 不换算成每年多少点，不和 QQQ 2002–07 的数并排比。",
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
    (out_dir / REPORT).write_text(render({**result, "exits": list(exits)}), encoding="utf-8")


def load_inputs(root: Path = PROJECT_ROOT) -> Inputs:
    loaded = load_settings()
    research = lg.research_root(loaded)
    gate, plan = trial.load_trial(research, st.PLAN_ID)
    problems = trial.gate_problems(gate, loaded)
    if plan.data.signal_snapshot_id != SNAPSHOT_ID:
        problems.append(f"signal snapshot must be {SNAPSHOT_ID}")
    snapshot = signal_sources.load_snapshot(root, SNAPSHOT_ID)
    macro = st.monthly_frame(snapshot.frames)
    problems += st.check_problems(
        macro, {spec.source_id: spec for spec in snapshot.specs}, gate, loaded
    )
    if problems:
        raise ValueError("; ".join(problems))
    window = pt.market_window(loaded, gate, "us_market")
    slow, _ = gate.markets["us_market"].segment_lists(window.window_id)
    segments = tuple(
        (spec.id, pd.Period(spec.start, freq="M"), pd.Period(spec.end, freq="M")) for spec in slow
    )
    sources = [
        {
            "source": source,
            "first": str(first)
            if (first := macro[source].first_valid_index()) is not None
            else None,
            "last": str(last) if (last := macro[source].last_valid_index()) is not None else None,
            "offsets": st._offsets(st.known_offsets(*lags)),
        }
        for source, lags in st.LAGS.items()
    ]
    rules = research / QUESTION / "prereg.yaml"
    prereg = load_prereg(rules)
    entries = {name: prereg.strategy(rule) for name, rule in RULES.items()}
    return Inputs(
        rs.cut(snapshot.frames[FACTORS]),
        macro,
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
        typer.echo(f"macro_reentry_table: 数据载入失败：{first_line}")
        raise typer.Exit(1) from exc


@app.callback()
def cli() -> None:
    """只印宏观条件下的慢腿回补表，不进账本、不模拟新规则。"""


@app.command("check")
def check_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        summary = timing_check(build(inputs))
    except ValueError as exc:
        typer.echo(f"macro_reentry_table: 建数据失败：{exc}")
        raise typer.Exit(1) from exc
    write_check(summary, out)
    spread, cape = summary["conditions"]["spread"], summary["conditions"]["cape"]
    typer.echo(
        f"macro_reentry_table: 慢腿完整退出 {summary['exits']['complete']} 次，"
        f"未完 {summary['exits']['incomplete']} 次；"
        f"甲早到 {spread['early']} 次，乙早到 {cape['early']} 次；"
        f"早的月数中位 甲 {_number(spread['early_months']['median'])}、"
        f"乙 {_number(cape['early_months']['median'])}；"
        f"对照 k 甲 {_number(summary['control']['spread']['k'])}、"
        f"乙 {_number(summary['control']['cape']['k'])}"
    )


@app.command("run")
def run_command(out: Annotated[Path, typer.Option("--out")]) -> None:
    inputs = _load()
    try:
        result, exits = read(inputs)
    except ValueError as exc:
        typer.echo(f"macro_reentry_table: 建数据失败：{exc}")
        raise typer.Exit(1) from exc
    result["provenance"] = git_provenance(PROJECT_ROOT)
    write_check(result["check"], out)
    write_result(result, exits, out)
    verdicts = "、".join(f"{name} {result['decisions'][name]['verdict']}" for name in CONDITIONS)
    typer.echo(f"macro_reentry_table: 判定 {verdicts}")


if __name__ == "__main__":
    app()
