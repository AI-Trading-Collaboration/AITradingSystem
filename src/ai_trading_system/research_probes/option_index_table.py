"""Nasdaq-100 option indices at total-return drawdown (TRADING-2567-41).

Owner decision: owner_decision:TRADING-2567:2026-11-22:option_index_first_look.
Charge option legs on roll-day net value before leverage_table.simulate_levered.
A holds verdict needs the post-2010 period and both halves to gain one point in
main costs and not lose in robust costs. Check computes only the 2009-2010 development
part and prints no post-2010 values. Run records one post-2010 read, self-checks there
before tables and refuses a second read. Print only: no ledger, gate, ruler, daily
report or broker changes.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.paths import join_root
from ai_trading_system.research_bench import segments as sg
from ai_trading_system.research_bench.settings import load_settings
from ai_trading_system.research_data import option_indices as oi
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import leverage_table as lt
from ai_trading_system.yaml_loader import safe_load_yaml_path

RUN_ID = "TRADING-2567-41"
OWNER_DECISION = "owner_decision:TRADING-2567:2026-11-22:option_index_first_look"
INDICES = oi.INDEX_IDS
LEGS = {"BXN": 1, "NQTRI": 1, "NQCLRI": 2}
ROLL_MONTHS = {"BXN": tuple(range(1, 13)), "NQTRI": (3, 6, 9, 12), "NQCLRI": (3, 6, 9, 12)}
QLH_SNAPSHOT_ID = "qlh-20261013-0b0df53cb1a2"
QLH_LAST_CLOSE = date(2026, 10, 12)
DEV_WINDOW = "qqq_long_history_development"
READ_WINDOW = "qqq_after_2010_owner_print"
OPTION_ASSETS = tuple(f"NDX_OPTION_INDEX_{i}" for i in INDICES)
DEV_ASSETS = ("QQQ_PROXY_NDX_TR", "CASH_DTB3", "SYNTHETIC_2X", "SYNTHETIC_3X", *OPTION_ASSETS)
READ_ASSETS = ("QQQ", "CASH_DTB3", "NDX_RETURN", "NDX_TR_RETURN", *OPTION_ASSETS)
READ_POLICY = "once_per_owner_decision"
READ_CAVEAT = "ndx_option_indices_read_as_qqq_own_options_per_owner_decision"
SPANS = ("P", "Q", "Q1", "Q2", "LIVE")
FULL_SPANS = SPANS[:4]
JUDGED = ("Q", "Q1", "Q2")
SPAN_START = {
    "P": date(2009, 6, 22),
    "Q": date(2011, 1, 3),
    "Q1": date(2011, 1, 3),
    "Q2": date(2019, 1, 2),
    "LIVE": date(2021, 8, 23),
}
SPAN_END = {
    "P": date(2010, 12, 31),
    "Q": QLH_LAST_CLOSE,
    "Q1": date(2018, 12, 31),
    "Q2": QLH_LAST_CLOSE,
    "LIVE": QLH_LAST_CLOSE,
}
P_START_BXN = date(2009, 9, 21)
SEAM_DAY = date(2021, 8, 23)
# Temporary print-only baseline fixed by the named owner decision; no tuning after the read.
L_GRID = tuple(round(i / 100, 2) for i in range(1, 501))
LEG_BPS_MAIN = 2.0
LEG_BPS_ROBUST = 10.0
LEG_BPS_SENSITIVITY = (0.0, 2.0, 5.0, 10.0, 20.0)
GROSS = lt.Costs(0.0, 0.0)
HOLDS_MAIN = 0.01
HOLDS_ROBUST = 0.0
# Numerical reproduction tolerance, not an investment acceptance band.
SELF_CHECK_TOLERANCE = 1e-9
DAYS_PER_YEAR = 365.25
TRADING_DAYS = 252
TAIL_DAYS = 20
TAIL_SHARE = 0.05
CHECK_JSON = "option_index_table_check.json"
RESULT_JSON = "option_index_table.json"
RESULT_MD = "option_index_table.md"
TOP_TEXT = "到顶，这一段不判"
DELEVER_TEXT = "降杠杆后才与 XNDX 同回撤"
STOP_CHECK = "输入日期、完整性或 P 段自检未过，停下不读"
STOP_SELF_CHECK = "Q 段自检未过，停下不印表"
STOP_BOUND = "L* 超过回撤上界，停下检查实现"
READINGS = {
    "one_holds": "至少一条指数同回撤后三段主算都多 1 点、稳健都不输；值得继续核对，不是生产批准。",
    "not_judged": "没有指数确认站住，且有指数到顶或到底不能判；先收口，不据此追加调参或重读。",
    "none_holds": "三条指数都没有稳稳超过 XNDX；这次指数初看不支持继续这条路。",
}


class Aligned(NamedTuple):
    index_id: str
    dates: pd.DatetimeIndex
    index_return: pd.Series
    hold_return: pd.Series
    cash_return: pd.Series
    rolls: pd.DatetimeIndex
    qlh_only: int
    index_only: int


class Inputs(NamedTuple):
    levels: dict[str, pd.Series]
    daily: pd.DataFrame
    windows: dict[str, Any]
    sources: dict[str, Any]


class Layout(NamedTuple):
    anchor: pd.Timestamp
    first_return: pd.Timestamp
    last: pd.Timestamp
    dates: pd.DatetimeIndex


def third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(4 - first.weekday()) % 7 + 14)


def roll_dates(dates: pd.DatetimeIndex, months: Sequence[int]) -> pd.DatetimeIndex:
    found = []
    for year, month in sorted({(d.year, d.month) for d in dates if d.month in months}):
        friday = pd.Timestamp(third_friday(year, month))
        if friday > dates[-1]:
            continue
        eligible = dates[(dates.year == year) & (dates.month == month) & (dates <= friday)]
        if len(eligible):
            found.append(eligible[-1])
    return pd.DatetimeIndex(found)


def _calendar(levels: pd.Series, daily: pd.DataFrame) -> tuple[pd.Series, pd.DatetimeIndex]:
    cut = levels.loc[levels.index <= pd.Timestamp(QLH_LAST_CLOSE)]
    if cut.empty or cut.index.has_duplicates or not cut.index.is_monotonic_increasing:
        raise ValueError("指数日历为空、重复或乱序")
    dates = pd.DatetimeIndex(cut.index.intersection(daily.index))
    if len(dates) < 2:
        raise ValueError("共同日历不足")
    return cut, dates


def align(index_id: str, levels: pd.Series, daily: pd.DataFrame) -> Aligned:
    cut, dates = _calendar(levels, daily)

    def compounded(column: str) -> pd.Series:
        out = [float("nan")]
        calendar = pd.DatetimeIndex(daily.index)
        for left, right in zip(dates[:-1], dates[1:], strict=True):
            part = daily[column].loc[(calendar > left) & (calendar <= right)]
            if part.empty or not np.isfinite(part.to_numpy(dtype=float)).all():
                out.append(float("nan"))
            elif len(part) == 1:
                out.append(float(part.iloc[0]))
            else:
                out.append(float(np.prod(1.0 + part.to_numpy(dtype=float))) - 1.0)
        return pd.Series(out, index=dates)

    between = daily.index[(daily.index >= cut.index[0]) & (daily.index <= cut.index[-1])]
    return Aligned(
        index_id,
        dates,
        cut.loc[dates].pct_change(fill_method=None),
        compounded("ndx_tr_return"),
        compounded("cash_return"),
        roll_dates(dates, ROLL_MONTHS[index_id]),
        len(between.difference(cut.index)),
        len(cut.index.difference(daily.index)),
    )


def net_returns(aligned: Aligned, bps: float) -> pd.Series:
    result = aligned.index_return.copy()
    roll = aligned.dates.isin(aligned.rolls)
    k = LEGS[aligned.index_id] * bps / 10_000
    result.loc[roll] = (1.0 + result.loc[roll]) * (1.0 - k) - 1.0
    return result


def sim_frame(qqq: pd.Series, cash: pd.Series) -> pd.DataFrame:
    if qqq.empty or not qqq.index.equals(cash.index):
        raise ValueError("模拟输入日历不一致或为空")
    prior = pd.Timestamp(qqq.index[0]).replace(day=1) - pd.Timedelta(days=1)
    frame = pd.DataFrame({"qqq": qqq, "cash": cash})
    return pd.concat([pd.DataFrame({"qqq": [np.nan], "cash": [np.nan]}, index=[prior]), frame])


def _start(index_id: str, span: str) -> date:
    return P_START_BXN if index_id == "BXN" and span == "P" else SPAN_START[span]


def _layout(dates: pd.DatetimeIndex, index_id: str, span: str) -> Layout:
    first = int(dates.searchsorted(pd.Timestamp(_start(index_id, span))))
    last = int(dates.searchsorted(pd.Timestamp(SPAN_END[span]), side="right")) - 1
    if first == 0 or first >= len(dates) or last < first:
        raise ValueError(f"{index_id} {span} 没有完整的收益段")
    return Layout(dates[first - 1], dates[first], dates[last], dates[first : last + 1])


def layout(aligned: Aligned, span: str) -> Layout:
    return _layout(aligned.dates, aligned.index_id, span)


def max_drawdown(returns: Any) -> float:
    equity = np.concatenate(([1.0], np.cumprod(1.0 + np.asarray(returns, dtype=float))))
    return float(np.min(equity / np.maximum.accumulate(equity) - 1.0))


def slope(line: Any, hold: Any) -> float:
    y, x = np.asarray(line, dtype=float), np.asarray(hold, dtype=float)
    centered = x - x.mean()
    denominator = float(np.dot(centered, centered))
    if denominator == 0:
        raise ValueError("基准收益没有波动，不能计算 beta")
    return float(np.dot(centered, y - y.mean()) / denominator)


def _cagr(returns: pd.Series, anchor: pd.Timestamp) -> float:
    years = (pd.Timestamp(returns.index[-1]) - anchor).days / DAYS_PER_YEAR
    if returns.empty or years <= 0:
        raise ValueError("收益段没有正的日历年数")
    return float(float(np.prod(1.0 + returns.to_numpy(dtype=float))) ** (1.0 / years) - 1.0)


def _volatility(returns: pd.Series) -> float:
    return float(returns.std(ddof=1) * np.sqrt(TRADING_DAYS))


def _stats(returns: pd.Series, hold: pd.Series, place: Layout) -> dict[str, Any]:
    equity = pd.Series(
        np.concatenate(([1.0], np.cumprod(1.0 + returns.to_numpy(dtype=float)))),
        index=pd.DatetimeIndex([place.anchor]).append(place.dates),
    )
    drawdown = equity / equity.cummax() - 1.0
    trough = pd.Timestamp(drawdown.idxmin())
    peak = pd.Timestamp(equity.loc[:trough].idxmax())
    rolling = (1.0 + returns).rolling(TAIL_DAYS).apply(np.prod, raw=True).dropna() - 1.0
    tail = rolling.loc[rolling <= rolling.quantile(TAIL_SHARE)]
    return {
        "total": float(equity.iloc[-1] - 1.0),
        "cagr": _cagr(returns, place.anchor),
        "max_drawdown": max_drawdown(returns),
        "drawdown_start": _day(peak),
        "drawdown_end": _day(trough),
        "volatility": _volatility(returns),
        "worst20": None if tail.empty else float(tail.mean()),
        "beta": slope(returns, hold),
    }


def _simulate(
    qqq: pd.Series, cash: pd.Series, place: Layout, leverage: float, costs: lt.Costs
) -> pd.Series:
    frame = sim_frame(qqq, cash)
    return lt.simulate_levered(
        frame,
        pd.Series(1.0, index=frame.index),
        start=place.anchor.date(),
        end=place.last.date(),
        leverage=leverage,
        costs=costs,
    ).simulation.daily["return"]


def self_check(aligned: Aligned, daily: pd.DataFrame, span: str) -> dict[str, Any]:
    place = layout(aligned, span)
    # Trim before simulation and at its explicit end; development never computes later values.
    limited = aligned._replace(
        dates=aligned.dates[aligned.dates <= place.last],
        index_return=aligned.index_return.loc[: place.last],
        hold_return=aligned.hold_return.loc[: place.last],
        cash_return=aligned.cash_return.loc[: place.last],
    )
    raw = limited.index_return.loc[place.dates]
    reproduced = _simulate(net_returns(limited, 0.0), limited.cash_return, place, 1.0, lt.MAIN)
    reproduce_ok = bool(
        np.isfinite(reproduced.to_numpy()).all()
        and np.max(np.abs(reproduced.to_numpy() - raw.to_numpy())) <= SELF_CHECK_TOLERANCE
    )
    charged = _simulate(
        net_returns(limited, LEG_BPS_MAIN), limited.cash_return, place, 1.0, lt.MAIN
    )
    rolls = int(place.dates.isin(aligned.rolls).sum())
    years = (place.last - place.anchor).days / DAYS_PER_YEAR
    k = LEGS[aligned.index_id] * LEG_BPS_MAIN / 10_000
    expected = (1.0 + _cagr(raw, place.anchor)) * (1.0 - k) ** (rolls / years)
    cost_ok = bool(
        rolls > 0 and abs(1.0 + _cagr(charged, place.anchor) - expected) <= SELF_CHECK_TOLERANCE
    )
    direct = daily["ndx_tr_return"].loc[(daily.index > place.anchor) & (daily.index <= place.last)]
    hold_ok = bool(
        np.isfinite(direct.to_numpy()).all()
        and abs(
            _cagr(aligned.hold_return.loc[place.dates], place.anchor) - _cagr(direct, place.anchor)
        )
        <= SELF_CHECK_TOLERANCE
    )
    return {
        "span": span,
        "reproduce": reproduce_ok,
        "cost": cost_ok,
        "hold": hold_ok,
        "ok": reproduce_ok and cost_ok and hold_ok,
    }


def registered_windows(registry: dict[str, Any]) -> dict[str, Any]:
    found = {}
    try:
        for name, start, end, role, assets in (
            (DEV_WINDOW, "1999-03-10", "2010-12-31", "development", DEV_ASSETS),
            (READ_WINDOW, "2011-01-03", "latest", "owner_print_only", READ_ASSETS),
        ):
            entry = registry["windows"][name]
            if (str(entry["start"]), str(entry["end"]), entry["role"]) != (
                start,
                end,
                role,
            ) or tuple(entry["assets"]) != assets:
                raise ValueError
            if name == READ_WINDOW and (
                entry["read_policy"] != READ_POLICY or READ_CAVEAT not in entry["caveats"]
            ):
                raise ValueError
            found[name] = _clean({**entry, "start": start, "end": end, "assets": list(assets)})
    except (KeyError, TypeError, ValueError):
        raise ValueError("期权指数表的窗口登记不匹配") from None
    return found


def _day(value: Any) -> str:
    return pd.Timestamp(value).date().isoformat()


def _rule() -> dict[str, Any]:
    return {
        "owner_decision": OWNER_DECISION,
        "grid": list(L_GRID),
        "judged": list(JUDGED),
        "holds_main": HOLDS_MAIN,
        "holds_robust": HOLDS_ROBUST,
        "leg_bps_main": LEG_BPS_MAIN,
        "leg_bps_robust": LEG_BPS_ROBUST,
        "main": dict(lt.MAIN._asdict()),
        "robust": dict(lt.ROBUST._asdict()),
        "cagr_days_per_year": DAYS_PER_YEAR,
        "volatility_days": TRADING_DAYS,
        "tail_days": TAIL_DAYS,
        "tail_share": TAIL_SHARE,
    }


def check_table(inputs: Inputs) -> dict[str, Any]:
    indices: dict[str, Any] = {}
    ok = bool(len(inputs.daily) and _day(inputs.daily.index[-1]) == QLH_LAST_CLOSE.isoformat())
    dev_end = pd.Timestamp(SPAN_END["P"])
    for index_id in INDICES:
        levels = inputs.levels[index_id]
        cut, dates = _calendar(levels, inputs.daily)
        # Full-period metadata use dates and missing masks only, never post-development returns.
        aligned = align(index_id, levels.loc[:dev_end], inputs.daily.loc[:dev_end])
        checked = self_check(aligned, inputs.daily.loc[:dev_end], "P")
        spans: dict[str, Any] = {}
        rolls = roll_dates(dates, ROLL_MONTHS[index_id])
        for span in SPANS:
            place = _layout(dates, index_id, span)
            calendar = inputs.daily.index
            inside = (calendar > place.anchor) & (calendar <= place.last)
            hold_missing = int((~np.isfinite(inputs.daily.loc[inside, "ndx_tr_return"])).sum())
            cash_missing = int((~np.isfinite(inputs.daily.loc[inside, "cash_return"])).sum())
            row = {
                "anchor": _day(place.anchor),
                "first_return": _day(place.first_return),
                "last": _day(place.last),
                "return_days": len(place.dates),
                "rolls": int(place.dates.isin(rolls).sum()),
                "start_ok": place.first_return.date() == _start(index_id, span),
                "end_ok": place.last.date() == SPAN_END[span],
                "hold_missing": hold_missing,
                "cash_missing": cash_missing,
            }
            spans[span] = row
            ok = bool(
                ok and row["start_ok"] and row["end_ok"] and not hold_missing and not cash_missing
            )
        between = inputs.daily.index[
            (inputs.daily.index >= cut.index[0]) & (inputs.daily.index <= cut.index[-1])
        ]
        indices[index_id] = {
            "first_date": _day(dates[0]),
            "last_date": _day(dates[-1]),
            "dates": len(dates),
            "qlh_only_days": len(between.difference(cut.index)),
            "index_only_days": len(cut.index.difference(inputs.daily.index)),
            "spans": spans,
            "self_check_p": checked,
        }
        ok = ok and checked["ok"]
    return {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "windows": inputs.windows,
        "sources": inputs.sources,
        "rule": _rule(),
        "indices": indices,
        "ok": bool(ok),
    }


def _bear(hold: pd.Series, raw: pd.Series, levered: pd.Series) -> dict[str, Any]:
    segments = [s for s in sg.segments(hold) if s.kind != "long_bull"]
    months = pd.DatetimeIndex(hold.index).to_period("M")
    inside = np.zeros(len(hold), dtype=bool)
    for segment in segments:
        inside |= (months >= pd.Period(segment.start, freq="M")) & (
            months <= pd.Period(segment.end, freq="M")
        )

    def annual(series: pd.Series, mask: Any) -> float | None:
        values = series.to_numpy(dtype=float)[mask]
        return (
            None
            if not len(values)
            else float(np.prod(1.0 + values)) ** (TRADING_DAYS / len(values)) - 1.0
        )

    lines: dict[str, Any] = {}
    for name, returns in (("unlevered", raw), ("levered", levered)):
        row: dict[str, Any] = {}
        for label, mask in (("inside", inside), ("outside", ~inside)):
            own, benchmark = annual(returns, mask), annual(hold, mask)
            row[label] = None if own is None or benchmark is None else own - benchmark
        lines[name] = row
    return {
        "segments": [asdict(s) for s in segments],
        "inside_days": int(inside.sum()),
        "lines": lines,
    }


def span_table(aligned: Aligned, span: str, *, grid: Sequence[float] = L_GRID) -> dict[str, Any]:
    place = layout(aligned, span)
    raw = aligned.index_return.loc[place.dates]
    hold = aligned.hold_return.loc[place.dates]
    raw_stats, hold_stats = _stats(raw, hold, place), _stats(hold, hold, place)
    raw_stats["rolls"] = int(place.dates.isin(aligned.rolls).sum())
    hold_stats["rolls"] = 0
    result: dict[str, Any] = {
        "anchor": _day(place.anchor),
        "first_return": _day(place.first_return),
        "last": _day(place.last),
        "return_days": len(place.dates),
        "raw": raw_stats,
        "hold": hold_stats,
    }
    cache: dict[tuple[float, float, lt.Costs], pd.Series] = {}

    def evaluate(bps: float, leverage: float, costs: lt.Costs) -> pd.Series:
        key = (bps, leverage, costs)
        if key not in cache:
            cache[key] = _simulate(
                net_returns(aligned, bps), aligned.cash_return, place, leverage, costs
            )
        return cache[key]

    def matched(bps: float) -> tuple[lt.Match, list[pd.Series]]:
        curve = [evaluate(bps, leverage, lt.MAIN) for leverage in grid]
        match = lt.largest_allowed(
            grid, [max_drawdown(line) >= hold_stats["max_drawdown"] for line in curve]
        )
        return match, curve

    match, curve = matched(LEG_BPS_MAIN)
    chosen = evaluate(LEG_BPS_MAIN, match.leverage, lt.MAIN)
    robust = evaluate(LEG_BPS_ROBUST, match.leverage, lt.ROBUST)
    vol = lt.largest_allowed(
        grid, [_volatility(line) <= hold_stats["volatility"] for line in curve]
    )
    one_dd = max_drawdown(evaluate(LEG_BPS_MAIN, 1.0, lt.MAIN))
    bound = math.inf if one_dd == 0 else lt.drawdown_bound(hold_stats["max_drawdown"], one_dd)
    r2 = {
        "leverage": match.leverage,
        "flag": match.flag,
        "monotone": match.monotone,
        "switches": list(match.switches),
        "delever": match.leverage < 1.0,
        "cagr": _cagr(chosen, place.anchor),
        "max_drawdown": max_drawdown(chosen),
        "d2_main": _cagr(chosen, place.anchor) - hold_stats["cagr"],
        "d2_robust": _cagr(robust, place.anchor) - hold_stats["cagr"],
        "robust_drawdown": max_drawdown(robust),
        "robust_deeper_by": max_drawdown(robust) - hold_stats["max_drawdown"],
        "bound": None if math.isinf(bound) else bound,
        "bound_problem": lt.bound_problem(match.leverage, bound),
        "vol_leverage": vol.leverage,
        "vol_flag": vol.flag,
        "vol_d2_main": _cagr(evaluate(LEG_BPS_MAIN, vol.leverage, lt.MAIN), place.anchor)
        - hold_stats["cagr"],
    }
    result["r2"] = r2
    if r2["bound_problem"]:
        return result
    if span in FULL_SPANS:
        sensitivity = []
        for bps in LEG_BPS_SENSITIVITY:
            found, _ = matched(bps)
            sensitivity.append(
                {
                    "bps": bps,
                    "leverage": found.leverage,
                    "flag": found.flag,
                    "d2_main": _cagr(evaluate(bps, found.leverage, lt.MAIN), place.anchor)
                    - hold_stats["cagr"],
                }
            )
        result["sensitivity"] = sensitivity
        beta = slope(raw, hold)
        if beta < 0:
            raise ValueError("beta 为负，拒绝固定 beta 对照")
        fixed = _simulate(aligned.hold_return, aligned.cash_return, place, beta, GROSS)
        result["beta"] = {
            "beta": beta,
            "cagr": _cagr(fixed, place.anchor),
            "max_drawdown": max_drawdown(fixed),
            "index_minus_fixed": raw_stats["cagr"] - _cagr(fixed, place.anchor),
        }
        result["bear"] = _bear(hold, raw, chosen)
        years = []
        for year in sorted(set(place.dates.year)):
            mask = place.dates.year == year
            positions = np.flatnonzero(mask)
            anchor = place.anchor if positions[0] == 0 else place.dates[positions[0] - 1]
            h = _cagr(hold.loc[mask], anchor)
            years.append(
                {"year": int(year), "hold": h, "difference": _cagr(raw.loc[mask], anchor) - h}
            )
        result["years"] = years
    elif aligned.index_id != "BXN":
        result["seam_residual"] = float(
            aligned.index_return.loc[pd.Timestamp(SEAM_DAY)]
            - aligned.hold_return.loc[pd.Timestamp(SEAM_DAY)]
        )
    return result


def verdict(rows: Mapping[str, Any]) -> str:
    if any(
        rows[s]["flag"] == "ok"
        and not (rows[s]["d2_main"] >= HOLDS_MAIN and rows[s]["d2_robust"] >= HOLDS_ROBUST)
        for s in JUDGED
    ):
        return "not_holds"
    return "not_judged" if any(rows[s]["flag"] != "ok" for s in JUDGED) else "holds"


def reading(verdicts: Mapping[str, str]) -> dict[str, Any]:
    case = (
        "one_holds"
        if "holds" in verdicts.values()
        else ("not_judged" if "not_judged" in verdicts.values() else "none_holds")
    )
    return {
        "case": case,
        "text": READINGS[case],
        "not_judged": [i for i in INDICES if verdicts[i] == "not_judged"],
    }


def run_table(
    inputs: Inputs,
    *,
    grid: Sequence[float] = L_GRID,
    on_read: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    if (
        not grid
        or list(grid) != sorted(set(grid))
        or any(not math.isfinite(v) or v <= 0 for v in grid)
    ):
        raise ValueError("杠杆网格必须正、有限、递增且唯一")
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "rule": _rule(),
        "check": check_table(inputs),
        "complete": False,
        "stop": None,
    }
    if not result["check"]["ok"]:
        result["stop"] = STOP_CHECK
        return result
    record = {
        "window_id": READ_WINDOW,
        "reads": 1,
        "owner_decision": OWNER_DECISION,
        "run_id": RUN_ID,
        "spans": list(SPANS),
        "assets": list(READ_ASSETS),
        "option_snapshot": inputs.sources["option_snapshot"],
        "qlh_snapshot": QLH_SNAPSHOT_ID,
    }
    if on_read is not None:
        on_read(record)
    result["window_read"] = record
    aligned = {i: align(i, inputs.levels[i], inputs.daily) for i in INDICES}
    checks = {i: self_check(aligned[i], inputs.daily, "Q") for i in INDICES}
    result["self_check"] = checks
    if not all(item["ok"] for item in checks.values()):
        result["stop"] = STOP_SELF_CHECK
        return result
    tables: dict[str, Any] = {}
    for index_id in INDICES:
        tables[index_id] = {}
        for span in SPANS:
            table = span_table(aligned[index_id], span, grid=grid)
            if table["r2"]["bound_problem"]:
                result.update(
                    stop=STOP_BOUND,
                    bound_problem={
                        "index": index_id,
                        "span": span,
                        "leverage": table["r2"]["leverage"],
                        "bound": table["r2"]["bound"],
                    },
                )
                return result
            tables[index_id][span] = table
    verdicts = {i: verdict({s: tables[i][s]["r2"] for s in JUDGED}) for i in INDICES}
    summary = [
        {
            "index": i,
            "spans": {
                s: {k: tables[i][s]["r2"][k] for k in ("leverage", "flag", "d2_main", "d2_robust")}
                for s in JUDGED
            },
            "verdict": verdicts[i],
        }
        for i in INDICES
    ]
    result.update(
        tables=tables, verdicts=verdicts, summary=summary, reading=reading(verdicts), complete=True
    )
    return result


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> list[str]:
    return [
        "| " + " | ".join(map(str, row)) + " |" for row in [headers, ["---"] * len(headers), *rows]
    ] + [""]


def _pct(value: Any) -> str:
    return "—" if value is None else f"{value * 100:+.2f}%"


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = [f"# 纳指 100 期权指数同回撤表 · {RUN_ID}", "", "## 总表", ""]
    if not result["complete"]:
        return "\n".join([*lines, str(result["stop"]), ""])
    rows = []
    for row in result["summary"]:
        cells = [row["index"]]
        for span in JUDGED:
            r = row["spans"][span]
            cells.extend(
                [f"{r['leverage']:.2f} ({r['flag']})", _pct(r["d2_main"]), _pct(r["d2_robust"])]
            )
        cells.append(row["verdict"])
        rows.append(cells)
    lines += _table(
        ["指数", *[f"{s} {k}" for s in JUDGED for k in ("L*", "主算 D2", "稳健 D2")], "判定"],
        rows,
    )
    lines += [
        result["reading"]["text"],
        "",
        OWNER_DECISION,
        "数据质量及来源：" + json.dumps(result["check"]["sources"], ensure_ascii=False),
        "主算：每腿 2 bps，利差 1%、换手 4 bps；稳健：每腿 10 bps，利差 2%、换手 25 bps。",
        "",
    ]
    for index_id in INDICES:
        for span in SPANS:
            t = result["tables"][index_id][span]
            lines += [
                f"## {index_id} · {span}",
                "",
                f"请求 {_start(index_id, span)}..{SPAN_END[span]}；"
                f"实际 {t['first_return']}..{t['last']}，"
                f"锚点 {t['anchor']}，{t['return_days']} 天。",
            ]
            lines += _table(
                [
                    "线",
                    "总收益",
                    "年化",
                    "最大回撤",
                    "回撤起",
                    "回撤止",
                    "波动",
                    "最差 5% 的 20 日",
                    "beta",
                    "换仓",
                ],
                [
                    [
                        name,
                        *[_pct(t[name][k]) for k in ("total", "cagr", "max_drawdown")],
                        t[name]["drawdown_start"],
                        t[name]["drawdown_end"],
                        _pct(t[name]["volatility"]),
                        _pct(t[name]["worst20"]),
                        f"{t[name]['beta']:.4f}",
                        t[name]["rolls"],
                    ]
                    for name in ("raw", "hold")
                ],
            )
            r2 = t["r2"]
            lines += ["### R2", "", json.dumps(r2, ensure_ascii=False, allow_nan=False), ""]
            if r2["flag"] == "top":
                lines.append(TOP_TEXT)
            if r2["delever"]:
                lines.append(DELEVER_TEXT)
            for key, title in (
                ("sensitivity", "成本敏感性"),
                ("beta", "beta 对照"),
                ("bear", "熊段"),
                ("years", "逐年"),
                ("seam_residual", "接缝残差"),
            ):
                if key in t:
                    lines += [
                        f"### {title}",
                        "",
                        json.dumps(t[key], ensure_ascii=False, allow_nan=False),
                        "",
                    ]
    lines += [
        "## 局限",
        "",
        "L* 与 L_v 在样本内重定；指数含回溯历史，不是可交易产品；"
        "2009–2010 只印不判，2011 年以后及 LIVE 不是新的样本外证据；"
        "期权腿成本为固定假设，未覆盖实际产品、税费和尾部成交风险。"
        "不改研究门、尺子或生产资格；读取后不调参重跑。",
        "",
        "## 读取记录",
        "",
        json.dumps(result["window_read"], ensure_ascii=False),
        "",
        "production_effect=none；broker_action=none。",
        "",
    ]
    return "\n".join(lines)


def _clean(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(k): _clean(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_clean(v) for v in value]
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_clean(value), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_check(check: Mapping[str, Any], out_dir: Path) -> None:
    _write_json(check, out_dir / CHECK_JSON)


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    _write_json(result, out_dir / RESULT_JSON)
    if result["complete"]:
        (out_dir / RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


def read_record_path(root: Path) -> Path:
    return Path(oi.snapshot_root(root)) / "reads" / f"{RUN_ID}.json"


def load_inputs(root: Path, qlh_root: Path, snapshot: str | None) -> Inputs:
    settings = load_settings()
    windows = registered_windows(
        safe_load_yaml_path(join_root(root, settings.settings.window_registry))
    )
    option = oi.load_snapshot(root, snapshot or oi.latest_snapshot_id(root))
    if option.spec.owner_decision != OWNER_DECISION or option.spec.qlh_snapshot != QLH_SNAPSHOT_ID:
        raise ValueError("期权快照的决定或 QQQ 快照不匹配")
    daily, report = qlh.load_daily(qlh.resolve_snapshot_dir(qlh_root, QLH_SNAPSHOT_ID))
    if report["snapshot_id"] != QLH_SNAPSHOT_ID:
        raise ValueError("QQQ 快照标识不匹配")
    sources = {
        "option_snapshot": option.snapshot_id,
        "option_status": option.check["status"],
        "option_warned": option.check["warned"],
        "qlh_snapshot": report["snapshot_id"],
        "qlh_status": report["status"],
        "series_csv_sha256": report["series_csv_sha256"],
    }
    return Inputs(option.levels, daily[["ndx_tr_return", "cash_return"]], windows, sources)


app = typer.Typer(help="期权指数同回撤表：只印、只读一次，不进账本。")


@app.callback()
def main() -> None:
    """只印三条期权指数的同回撤表。"""


def _refuse_existing(out: Path) -> None:
    if any((out / name).exists() for name in (CHECK_JSON, RESULT_JSON, RESULT_MD)):
        typer.echo("option_index_table: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)


def _load(root: Path, qlh_root: Path, snapshot: str | None) -> Inputs:
    try:
        return load_inputs(root, qlh_root, snapshot)
    except (OSError, ValueError, KeyError) as exc:
        typer.echo(f"option_index_table: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option()],
    root: Annotated[Path, typer.Option()] = PROJECT_ROOT,
    qlh_root: Annotated[Path, typer.Option()] = qlh.DEFAULT_SNAPSHOT_ROOT,
    snapshot: Annotated[str | None, typer.Option()] = None,
) -> None:
    _refuse_existing(out)
    inputs = _load(root, qlh_root, snapshot)
    check = check_table(inputs)
    write_check(check, out)
    text = "；".join(
        f"{i} {r['first_date']}..{r['last_date']}，{r['dates']} 天，"
        f"P 段自检 {'PASS' if r['self_check_p']['ok'] else 'FAIL'}"
        for i, r in check["indices"].items()
    )
    typer.echo(f"option_index_table: {text}")
    if not check["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option()],
    root: Annotated[Path, typer.Option()] = PROJECT_ROOT,
    qlh_root: Annotated[Path, typer.Option()] = qlh.DEFAULT_SNAPSHOT_ROOT,
    snapshot: Annotated[str | None, typer.Option()] = None,
) -> None:
    _refuse_existing(out)
    record_path = read_record_path(root)
    if record_path.exists():
        typer.echo("option_index_table: 已有读取记录，拒绝再读。")
        raise typer.Exit(code=1)
    inputs = _load(root, qlh_root, snapshot)

    def record_read(record: dict[str, Any]) -> None:
        record_path.parent.mkdir(parents=True, exist_ok=True)
        body = {**record, "read_at": datetime.now(UTC).isoformat()}
        # Exclusive creation also refuses concurrent attempts; a stopped read remains consumed.
        with record_path.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(body, indent=2, ensure_ascii=False, allow_nan=False) + "\n")

    result = run_table(inputs, on_read=record_read)
    write_check(result["check"], out)
    write_result(result, out)
    if not result["complete"]:
        typer.echo(result["stop"])
        raise typer.Exit(code=1)
    for row in result["summary"]:
        cells = "；".join(
            f"{s} L*={r['leverage']:.2f}，主算 D2={_pct(r['d2_main'])}，"
            f"稳健 D2={_pct(r['d2_robust'])}"
            for s, r in row["spans"].items()
        )
        typer.echo(f"{row['index']}：{row['verdict']}；{cells}")


if __name__ == "__main__":
    app()
