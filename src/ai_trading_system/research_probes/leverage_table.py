"""F0001 levered to buy-and-hold's drawdown or volatility (TRADING-2565-101).

Owner decision owner_decision:TRADING-2565:2026-10-30:leverage_table and period-6
charter section 25 specify two rulers (QQQ and US market), a 1.00..3.00 grid in
0.01 steps, L* matching drawdown and L_v matching volatility, and two reading
lines: all main differences >= one point, all robust differences >= zero.
Print only: not a probe or experiment, no accounting entries, no research-goal
change, and no daily-report or broker action. The check only counts inputs;
it does not inspect investment returns.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Annotated, Any, NamedTuple

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import BenchData
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR, summarize
from ai_trading_system.research_bench.prereg import StrategyEntry
from ai_trading_system.research_bench.settings import LoadedSettings
from ai_trading_system.research_bench.simulate import Simulation, schedule_mask
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_probes import backfill_check as bc
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import policy_table as pt

RUN_ID = "TRADING-2565-101"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-30:leverage_table"
DATA_TRIAL = pt.DATA_TRIAL
MARKETS: tuple[tr.Market, ...] = ("qqq", "us_market")
MARKET_TEXT = pt.MARKET_TEXT
# All reading parameters below are the temporary, print-only baseline in charter §25
# and the named owner decision; exit at the 11-20 renewal review, not promotion.
GRID = tuple(round(1.0 + i / 100, 2) for i in range(201))
TABLE_LEVERAGE = (1.0, 1.25, 1.5, 2.0)
DAILY_RESET_LEVERAGE = 2.0
CAP_SENSITIVITY = 2.0
LINE_1 = 0.010
LINE_2 = 0.0
NEAR_HIGH = 0.05
ROLLING_YEARS = (5, 10)
BOUND_TOLERANCE = 0.10
ERAS = (
    ("1929_1961", date(1929, 1, 1), date(1961, 12, 31)),
    ("1962_1989", date(1962, 1, 1), date(1989, 12, 31)),
    ("1990_2010", date(1990, 1, 1), date(2010, 12, 31)),
)


class Costs(NamedTuple):
    spread: float
    bps: float


MAIN = Costs(spread=0.01, bps=4.0)
ROBUST = Costs(spread=0.02, bps=25.0)
SENSITIVITIES = {
    "spread_only": Costs(0.02, 4.0),
    "cost_only": Costs(0.01, 25.0),
    "no_spread": Costs(0.0, 4.0),
}


class Match(NamedTuple):
    leverage: float
    flag: str
    monotone: bool
    switches: tuple[float, ...]


class Levered(NamedTuple):
    simulation: Simulation
    bust: pd.Timestamp | None


HOLDS_TEXT = (
    "F0001 加杠杆到买入持有的回撤或波动，全周期两把尺子都多挣 1 点以上，"
    "多付 1 点利差、成本 25 bps 也不输"
)
NOT_HOLDS_TEXT = "同回撤（同波动）下加杠杆的 F0001 没有稳稳超过买入持有"
STOP_REPRODUCE = "L = 1 复现不了印过的数，不判"
STOP_BOUND = "L* 超过上界，多半是实现的错，不判"
STOP_CHECK = "开跑前输入检查没过，不判"
ERA_NOTE = "L* 是 1929–2010 全周期定的，1929–32 在里面，后两段用的是前一段定的杠杆。"
OLD_ERA_TEXT = "全周期的多挣主要来自 1929–1961"
LIMITATIONS = (
    "样本里定的 L* 和 L_v",
    "两把尺子的窗口都含趋势最占便宜的长熊（1929–32、2000–02、2008）",
    "2011–2021 长牛没读",
    "没有趋势信息时也判 `holds` 约 5%–7%（不加 L_v 约 10%–15%；合成模拟，推断）",
)
# Printed historical self-checks, not new outcome reads; QQQ full precision ±1e-6.
REPRODUCE = {
    "qqq": {
        "f0001": {
            metric: (value - 1e-6, value + 1e-6)
            for metric, value in (
                ("geometric_annual_return", 0.042236272246680695),
                ("annual_volatility", 0.18631682590323612),
                ("max_drawdown", -0.5020165649999986),
                ("average_exposure", 0.5941523375781023),
                ("annual_turnover", 3.5222403413498977),
            )
        },
        "buy_and_hold": {
            metric: (value - 1e-6, value + 1e-6)
            for metric, value in (
                ("geometric_annual_return", 0.008568284208556598),
                ("annual_volatility", 0.33310343608383863),
                ("max_drawdown", -0.8298086606243703),
            )
        },
    },
    "us_market": {
        "f0001": {
            "geometric_annual_return": (0.0875, 0.0885),
            "max_drawdown": (-0.4835, -0.4825),
            "average_exposure": (0.655, 0.665),
            "annual_turnover": (3.115, 3.125),
        },
        "buy_and_hold": {
            "geometric_annual_return": (0.0885, 0.0895),
            "max_drawdown": (-0.8415, -0.8405),
        },
    },
}
CHECK_JSON = "leverage_check.json"
RESULT_JSON = "leverage_table.json"
RESULT_MD = "leverage_table.md"
app = typer.Typer(help="杠杆账表：只印开发窗口，不改研究目标，不进账本。")


def f0001_entry(root_dir: Path) -> StrategyEntry:
    return bc.source_entry(root_dir, bc.CONFIGS[0])


def f0001_targets(data: BenchData, root_dir: Path) -> pd.Series:
    entry = f0001_entry(root_dir)
    return strategy_targets(tr.STRATEGY_FUNCTIONS[entry.function], data.inputs, entry.params)


def _layout(
    index: pd.DatetimeIndex, start: date, end: date | None
) -> tuple[int, int, list[bool], int | None]:
    first = int(index.searchsorted(pd.Timestamp(start)))
    last = (
        len(index) - 1
        if end is None
        else int(index.searchsorted(pd.Timestamp(end), side="right")) - 1
    )
    if first >= len(index) or last <= first:
        raise ValueError(f"no return dates between {start} and {end}")
    scheduled = schedule_mask(index, "month_end")
    opening = next((p for p in range(first - 1, -1, -1) if scheduled[p]), None)
    return first, last, scheduled, opening


def _target(value: float) -> float:
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"target must be finite and nonnegative, got {value}")
    return value


def simulate_levered(
    returns: pd.DataFrame,
    targets: pd.Series,
    *,
    start: date,
    end: date | None,
    leverage: float,
    costs: Costs,
    daily_reset: bool = False,
) -> Levered:
    if not targets.index.equals(returns.index):
        raise ValueError("targets must be indexed by the same dates as the returns")
    if not math.isfinite(leverage) or leverage < 0:
        raise ValueError("leverage must be finite and nonnegative")
    if not all(math.isfinite(c) and c >= 0 for c in costs):
        raise ValueError("costs must be finite and nonnegative")
    index = pd.DatetimeIndex(returns.index)
    first, last, scheduled, opening = _layout(index, start, end)
    matrix = returns[["qqq", "cash"]].to_numpy(dtype=float)
    if not np.isfinite(matrix[first + 1 : last + 1]).all():
        raise ValueError("market or cash has missing or nonfinite returns inside the window")
    values = targets.to_numpy(dtype=float)
    if opening is None:
        raise ValueError("no target is executable at start; extend the lookback")
    effective = _target(float(values[opening]))
    weight = leverage * effective
    rows: list[tuple[float, float, float, float, float, float]] = []
    bust = None
    for q in range(first + 1, last + 1):
        if bust is not None:
            rows.append((0.0, 0.0, 0.0, 0.0, 0.0, 0.0))
            continue
        market, cash = matrix[q]
        held = weight
        # The requirement fixes a 252-day financing convention, unlike calendar-year CAGR.
        spread = max(weight - 1.0, 0.0) * costs.spread / 252
        gross = weight * market + (1.0 - weight) * cash - spread
        if 1.0 + gross <= 0.0:
            bust = index[q]
            rows.append((-1.0, gross, held, 0.0, 0.0, spread))
            continue
        weight = weight * (1.0 + market) / (1.0 + gross)
        decided = q - 1
        execute = scheduled[decided]
        if execute:
            effective = _target(float(values[decided]))
        turnover = 0.0
        if execute or daily_reset:
            target = leverage * effective
            turnover = abs(target - weight)
            weight = target
        cost = turnover * costs.bps / 10_000.0
        net = (1.0 + gross) * (1.0 - cost) - 1.0
        rows.append((net, gross, held, turnover, cost, spread))
    daily = pd.DataFrame(
        rows,
        index=index[first + 1 : last + 1],
        columns=["return", "gross_return", "exposure", "turnover", "cost", "spread"],
    )
    return Levered(Simulation(daily=daily, start=index[first]), bust)


def window_stats(returns: pd.Series, anchor: pd.Timestamp) -> dict[str, Any]:
    if returns.empty:
        raise ValueError("window has no return dates")
    equity = pd.concat([pd.Series([1.0], index=[anchor]), (1.0 + returns).cumprod()])
    years = (returns.index[-1] - anchor).days / DAYS_PER_YEAR
    return {
        "first_return_date": returns.index[0].date().isoformat(),
        "last_return_date": returns.index[-1].date().isoformat(),
        "return_days": len(returns),
        "years": years,
        "geometric_annual_return": float(equity.iloc[-1]) ** (1.0 / years) - 1.0,
        "max_drawdown": float((equity / equity.cummax() - 1.0).min()),
    }


def largest_allowed(grid: Sequence[float], allowed: Sequence[bool]) -> Match:
    if not grid or len(grid) != len(allowed):
        raise ValueError("grid and allowed must be nonempty and aligned")
    valid = [value for value, ok in zip(grid, allowed, strict=True) if ok]
    return Match(
        max(valid) if valid else grid[0],
        "bottom" if not valid else "top" if allowed[-1] else "ok",
        not any(not a and b for a, b in zip(allowed[:-1], allowed[1:], strict=True)),
        tuple(grid[i] for i in range(1, len(grid)) if allowed[i] != allowed[i - 1]),
    )


def range_where(grid: Sequence[float], ok: Sequence[bool]) -> dict[str, Any] | None:
    if len(grid) != len(ok):
        raise ValueError("grid and ok must be aligned")
    positions = [i for i, value in enumerate(ok) if value]
    if not positions:
        return None
    first, last = positions[0], positions[-1]
    return {"low": grid[first], "high": grid[last], "contiguous": all(ok[first : last + 1])}


def drawdown_bound(buy_hold: float, f0001: float) -> float:
    return math.log1p(buy_hold) / math.log1p(f0001)


def bound_problem(leverage: float, bound: float) -> bool:
    return leverage > bound + BOUND_TOLERANCE


def reproduce_rows(
    market: str,
    f0001: Mapping[str, Any],
    buy_hold: Mapping[str, Any],
    reproduce: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    expected = REPRODUCE if reproduce is None else reproduce
    summaries = {"f0001": f0001, "buy_and_hold": buy_hold}
    return [
        {
            "market": market,
            "line": line,
            "metric": metric,
            "value": summaries[line][metric],
            "low": low,
            "high": high,
            "ok": low <= summaries[line][metric] <= high,
        }
        for line, metrics in expected[market].items()
        for metric, (low, high) in metrics.items()
    ]


def failure_text(item: Mapping[str, Any]) -> str:
    pairing = "回撤" if item["pairing"] == "drawdown" else "波动"
    line = "线 1（主算 ≥ +1 点）" if item["line"] == "line_1" else "线 2（稳健 ≥ 0）"
    return f"{MARKET_TEXT[item['market']]} 按{pairing}配平，{line}{item['value'] * 100:+.2f} 点"


def verdict(
    main: Mapping[str, Mapping[str, float]], robust: Mapping[str, Mapping[str, float]]
) -> dict[str, Any]:
    failed = [
        {"line": line, "market": market, "pairing": pairing, "value": values[market][pairing]}
        for line, values, threshold in (("line_1", main, LINE_1), ("line_2", robust, LINE_2))
        for market in MARKETS
        for pairing in ("drawdown", "volatility")
        if values[market][pairing] < threshold
    ]
    return {
        "verdict": "not_holds" if failed else "holds",
        "line_1": not any(item["line"] == "line_1" for item in failed),
        "line_2": not any(item["line"] == "line_2" for item in failed),
        "failed": failed,
        "text": NOT_HOLDS_TEXT + "：" + "；".join(map(failure_text, failed))
        if failed
        else HOLDS_TEXT,
    }


def daily_reset_text(difference: float) -> str | None:
    if abs(difference) <= 0.01:
        return None
    return f"用按日重置的产品会{'多' if difference > 0 else '少'} {abs(difference) * 100:.2f} 点/年"


def old_era_text(difference: float, eras: Sequence[Mapping[str, Any]]) -> str | None:
    if (
        difference >= LINE_1
        and eras
        and all(row["difference"]["drawdown"] < LINE_1 for row in eras[1:])
    ):
        return OLD_ERA_TEXT
    return None


def _rolling_starts(
    index: pd.DatetimeIndex, anchor: pd.Timestamp, years: int
) -> list[pd.Timestamp]:
    calendar = pd.DatetimeIndex(pd.DatetimeIndex([anchor]).append(index))
    return [
        d
        for d, scheduled in zip(calendar, schedule_mask(calendar, "month_end"), strict=True)
        if scheduled and d + pd.DateOffset(years=years) <= index[-1]
    ]


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    return {
        "count": len(values),
        "worst": min(values) if values else None,
        "p10": float(np.percentile(values, 10)) if values else None,
        "median": float(np.median(values)) if values else None,
        "beat_share": float(np.mean(np.asarray(values) > 0)) if values else None,
    }


def rolling_windows(
    a: pd.Series,
    b: pd.Series,
    anchor: pd.Timestamp,
    years: int,
    near_high: float | None = None,
) -> dict[str, Any]:
    if not a.index.equals(b.index):
        raise ValueError("rolling lines must share an index")
    near_high = NEAR_HIGH if near_high is None else near_high
    index = pd.DatetimeIndex(a.index)
    equity = pd.concat([pd.Series([1.0], index=[anchor]), (1.0 + b).cumprod()])
    relative = equity / equity.cummax()
    differences, near = [], []
    for start in _rolling_starts(index, anchor, years):
        inside = (index > start) & (index <= start + pd.DateOffset(years=years))
        diff = (
            window_stats(a[inside], start)["geometric_annual_return"]
            - window_stats(b[inside], start)["geometric_annual_return"]
        )
        differences.append(diff)
        if relative.loc[start] > 1.0 - near_high:
            near.append(diff)
    return {"years": years, "all": _distribution(differences), "near_high": _distribution(near)}


def era_rows(
    lines: Mapping[str, pd.Series],
    anchor: pd.Timestamp,
    eras: Sequence[tuple[str, date, date]] | None = None,
) -> list[dict[str, Any]]:
    index = pd.DatetimeIndex(lines["buy_and_hold"].index)
    calendar = pd.DatetimeIndex([anchor]).append(index)
    rows = []
    for name, start, end in ERAS if eras is None else eras:
        inside = (index >= pd.Timestamp(start)) & (index <= pd.Timestamp(end))
        dates = index[inside]
        if not len(dates):
            continue
        era_anchor = calendar[int(calendar.searchsorted(dates[0])) - 1]
        stats = {key: window_stats(series[inside], era_anchor) for key, series in lines.items()}
        rows.append(
            {
                "era": name,
                "start": dates[0].date().isoformat(),
                "end": dates[-1].date().isoformat(),
                "anchor": era_anchor.date().isoformat(),
                "return_days": len(dates),
                **stats,
                "difference": {
                    pairing: stats[pairing]["geometric_annual_return"]
                    - stats["buy_and_hold"]["geometric_annual_return"]
                    for pairing in ("drawdown", "volatility")
                },
            }
        )
    return rows


def check_inputs(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
) -> dict[str, Any]:
    markets: dict[str, Any] = {}
    for market in MARKETS:
        data = datasets[market]
        window = pt.market_window(loaded, gate, market)
        index = pd.DatetimeIndex(data.returns.index)
        first, last, scheduled, opening = _layout(index, window.start, window.end)
        dates = index[first + 1 : last + 1]
        targets = f0001_targets(data, root_dir)
        values = targets.to_numpy(dtype=float)
        finite = np.flatnonzero(np.isfinite(values))
        counts = dict.fromkeys(("0", "0.5", "1", "other"), 0)
        for p in range(first, last + 1):
            if scheduled[p]:
                value = values[p]
                key = (
                    "0" if value == 0 else "0.5" if value == 0.5 else "1" if value == 1 else "other"
                )
                counts[key] += 1
        missing = ~np.isfinite(data.returns.loc[dates, ["qqq", "cash"]].to_numpy(dtype=float))
        markets[market] = {
            "window_id": window.window_id,
            "start": window.start.isoformat(),
            "end": window.end.isoformat() if window.end is not None else None,
            "return_days": len(dates),
            "first_return_date": dates[0].date().isoformat(),
            "last_return_date": dates[-1].date().isoformat(),
            "missing_days": int(missing.any(axis=1).sum()),
            "first_target_date": index[int(finite[0])].date().isoformat() if len(finite) else None,
            "target_defined_at_start": bool(opening is not None and np.isfinite(values[opening])),
            "months_by_exposure": counts,
            "rebalances": sum(scheduled[p - 1] for p in range(first + 1, last + 1)),
            "eras": [
                {
                    "era": name,
                    "return_days": int(
                        ((dates >= pd.Timestamp(a)) & (dates <= pd.Timestamp(b))).sum()
                    ),
                }
                for name, a, b in ERAS
            ]
            if market == "us_market"
            else [],
            "rolling": {
                str(y): len(_rolling_starts(dates, index[first], y)) for y in ROLLING_YEARS
            },
        }
    return {
        "run_id": RUN_ID,
        "markets": markets,
        "ok": all(
            m["missing_days"] == 0 and m["target_defined_at_start"] for m in markets.values()
        ),
    }


def _summary(levered: Levered, data: BenchData, loaded: LoadedSettings) -> dict[str, Any]:
    sim = levered.simulation
    summary = summarize(sim, data.returns["cash"], loaded)
    summary["above_one_share"] = float((sim.exposure > 1.0).mean())
    summary["annual_spread"] = float(sim.daily["spread"].sum()) / summary["years"]
    return summary


def _evaluate_market(
    market: tr.Market,
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    targets: pd.Series,
    grid: Sequence[float],
    hold: Levered,
    hold_summary: dict[str, Any],
    one_summary: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    window = pt.market_window(loaded, gate, market)
    cache: dict[tuple[Costs, float, bool], tuple[Levered, dict[str, Any]]] = {}

    def evaluate(
        leverage: float, costs: Costs, daily: bool = False
    ) -> tuple[Levered, dict[str, Any]]:
        key = (costs, leverage, daily)
        if key not in cache:
            sim = simulate_levered(
                data.returns,
                targets,
                start=window.start,
                end=window.end,
                leverage=leverage,
                costs=costs,
                daily_reset=daily,
            )
            cache[key] = sim, _summary(sim, data, loaded)
        return cache[key]

    baseline = hold_summary["geometric_annual_return"]
    curve = []
    for leverage in grid:
        point: dict[str, Any] = {"leverage": leverage}
        for setting, costs in (("main", MAIN), ("robust", ROBUST)):
            _, summary = evaluate(leverage, costs)
            point[setting] = {
                key: summary[key]
                for key in ("geometric_annual_return", "annual_volatility", "max_drawdown")
            }
            point[setting]["difference"] = summary["geometric_annual_return"] - baseline
        curve.append(point)
    dd = largest_allowed(
        grid, [p["main"]["max_drawdown"] >= hold_summary["max_drawdown"] for p in curve]
    )
    vol = largest_allowed(
        grid, [p["main"]["annual_volatility"] <= hold_summary["annual_volatility"] for p in curve]
    )
    bound = drawdown_bound(hold_summary["max_drawdown"], one_summary["max_drawdown"])
    bound_check = {
        "leverage": dd.leverage,
        "bound": bound,
        "problem": bound_problem(dd.leverage, bound),
    }
    # Do not construct descriptive tables until the upper-bound self-check passes globally.
    return {
        "window": {
            "window_id": window.window_id,
            "start": window.start.isoformat(),
            "end": window.end.isoformat() if window.end else None,
        },
        "buy_and_hold": hold_summary,
        "one": one_summary,
        "bound": bound,
        "drawdown_match": dict(dd._asdict()),
        "volatility_match": dict(vol._asdict()),
        "curve": curve,
        "ranges": {
            "main": range_where(grid, [p["main"]["difference"] >= LINE_1 for p in curve]),
            "robust": range_where(grid, [p["robust"]["difference"] >= LINE_2 for p in curve]),
        },
    }, {"evaluate": evaluate, "hold": hold, "bound_check": bound_check}


def _complete_market(
    market: str,
    found: dict[str, Any],
    context: Mapping[str, Any],
    data: BenchData,
    loaded: LoadedSettings,
    gate: tr.TrialGate,
) -> None:
    evaluate = context["evaluate"]
    hold = context["hold"]
    baseline = found["buy_and_hold"]["geometric_annual_return"]
    dd = found["drawdown_match"]["leverage"]
    vol = found["volatility_match"]["leverage"]
    leverages = [(str(value), value) for value in TABLE_LEVERAGE] + [
        ("drawdown", dd),
        ("volatility", vol),
    ]
    table: dict[str, list[dict[str, Any]]] = {}
    busts = []

    def row(
        name: str, leverage: float | None, sim: Levered, summary: dict[str, Any], setting: str
    ) -> dict[str, Any]:
        if sim.bust is not None:
            busts.append({"line": name, "setting": setting, "date": sim.bust.date().isoformat()})
        return {
            "id": name,
            "leverage": leverage,
            "summary": summary,
            "difference": summary["geometric_annual_return"] - baseline,
            "matched_difference": None,
        }

    for setting, costs in (("main", MAIN), ("robust", ROBUST)):
        rows = [row("buy_and_hold", 1.0, hold, found["buy_and_hold"], setting)]
        matched_rows = []
        for suffix, leverage in leverages:
            sim, summary = evaluate(leverage, costs)
            own = row(f"f0001_{suffix}", leverage, sim, summary, setting)
            constant = summary["average_exposure"]
            # These windows are the same two development rulers used by the main curve.
            w = found["window"]
            matched = simulate_levered(
                data.returns,
                pd.Series(constant, index=data.returns.index),
                start=date.fromisoformat(w["start"]),
                end=date.fromisoformat(w["end"]) if w["end"] else None,
                leverage=1.0,
                costs=costs,
            )
            match_summary = _summary(matched, data, loaded)
            own["matched_difference"] = (
                summary["geometric_annual_return"] - match_summary["geometric_annual_return"]
            )
            rows.append(own)
            matched_rows.append(
                {
                    **row(f"matched_{suffix}", None, matched, match_summary, setting),
                    "constant": constant,
                }
            )
        rows.extend(matched_rows)
        for suffix, leverage in (
            (str(DAILY_RESET_LEVERAGE), DAILY_RESET_LEVERAGE),
            ("drawdown", dd),
        ):
            sim, summary = evaluate(leverage, costs, True)
            rows.append(row(f"daily_{suffix}", leverage, sim, summary, setting))
        table[setting] = rows
    points = {p["leverage"]: p for p in found["curve"]}
    sensitivities = {
        name: {
            pairing: evaluate(leverage, costs)[1]["geometric_annual_return"] - baseline
            for pairing, leverage in (("drawdown", dd), ("volatility", vol))
        }
        for name, costs in SENSITIVITIES.items()
    }
    sensitivities["cap_2"] = {
        pairing: points[min(leverage, CAP_SENSITIVITY)]["main"]["difference"]
        for pairing, leverage in (("drawdown", dd), ("volatility", vol))
    }
    resets = []
    for leverage in (DAILY_RESET_LEVERAGE, dd):
        month = points[leverage]["main"]["geometric_annual_return"]
        daily = evaluate(leverage, MAIN, True)[1]["geometric_annual_return"]
        resets.append(
            {
                "leverage": leverage,
                "month_end": month,
                "daily": daily,
                "difference": daily - month,
                "text": daily_reset_text(daily - month),
            }
        )
    sim_dd = evaluate(dd, MAIN)[0].simulation
    sim_vol = evaluate(vol, MAIN)[0].simulation
    eras = (
        era_rows(
            {
                "buy_and_hold": hold.simulation.returns,
                "drawdown": sim_dd.returns,
                "volatility": sim_vol.returns,
            },
            sim_dd.start,
        )
        if market == "us_market"
        else []
    )
    found.update(
        table=table,
        sensitivities=sensitivities,
        daily_reset=resets,
        rolling=[
            rolling_windows(sim_dd.returns, hold.simulation.returns, sim_dd.start, y)
            for y in ROLLING_YEARS
        ],
        eras=eras,
        old_era_text=old_era_text(points[dd]["main"]["difference"], eras)
        if market == "us_market"
        else None,
        busts=busts,
    )


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: tr.TrialGate,
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    grid: Sequence[float] | None = None,
    reproduce: Mapping[str, Any] | None = None,
    progress: Callable[[str, str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    grid = GRID if grid is None else grid
    reproduce = REPRODUCE if reproduce is None else reproduce
    required = {1.0, *TABLE_LEVERAGE, DAILY_RESET_LEVERAGE, CAP_SENSITIVITY}
    if (
        not required.issubset(grid)
        or any(not math.isfinite(v) for v in grid)
        or list(grid) != sorted(set(grid))
    ):
        raise ValueError("grid must be ordered, unique, finite and include every table leverage")
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "owner_decision": OWNER_DECISION,
        "data_trial": DATA_TRIAL,
        "provenance": dict(provenance),
        "grid": list(grid),
        "costs": {
            name: dict(costs._asdict())
            for name, costs in (("main", MAIN), ("robust", ROBUST), *SENSITIVITIES.items())
        },
        "check": check_inputs(datasets, loaded, gate, root_dir),
        "reproduce": [],
        "stopped": None,
        "markets": {},
        "differences": None,
        "verdict": None,
        "complete": False,
    }
    if not result["check"]["ok"]:
        result["stopped"] = {"reason": "check", "text": STOP_CHECK}
        return dict(gp._clean(result))
    originals = {}
    for market in MARKETS:
        data = datasets[market]
        window = pt.market_window(loaded, gate, market)
        targets = f0001_targets(data, root_dir)
        hold = simulate_levered(
            data.returns,
            pd.Series(1.0, index=data.returns.index),
            start=window.start,
            end=window.end,
            leverage=1.0,
            costs=MAIN,
        )
        one = simulate_levered(
            data.returns,
            targets,
            start=window.start,
            end=window.end,
            leverage=1.0,
            costs=MAIN,
        )
        hold_summary, one_summary = _summary(hold, data, loaded), _summary(one, data, loaded)
        result["reproduce"].extend(reproduce_rows(market, one_summary, hold_summary, reproduce))
        originals[market] = (targets, hold, hold_summary, one_summary)
    if not all(r["ok"] for r in result["reproduce"]):
        result["stopped"] = {"reason": "reproduce", "text": STOP_REPRODUCE}
        return dict(gp._clean(result))
    markets, contexts = {}, {}
    for market in MARKETS:
        targets, hold, hold_summary, one_summary = originals[market]
        markets[market], contexts[market] = _evaluate_market(
            market, datasets[market], loaded, gate, targets, grid, hold, hold_summary, one_summary
        )
        if progress is not None:
            progress(market, "curve", {"text": "杠杆曲线已算完"})
    bounds = {market: contexts[market]["bound_check"] for market in MARKETS}
    if any(b["problem"] for b in bounds.values()):
        result["stopped"] = {"reason": "bound", "text": STOP_BOUND, "bound": bounds}
        return dict(gp._clean(result))
    for market in MARKETS:
        _complete_market(market, markets[market], contexts[market], datasets[market], loaded, gate)
        if progress is not None:
            progress(market, "table", {"text": "只印表已算完"})
    differences: dict[str, dict[str, dict[str, float]]] = {
        setting: {
            market: {
                pairing: next(
                    p[setting]["difference"]
                    for p in markets[market]["curve"]
                    if p["leverage"] == markets[market][f"{pairing}_match"]["leverage"]
                )
                for pairing in ("drawdown", "volatility")
            }
            for market in MARKETS
        }
        for setting in ("main", "robust")
    }
    result.update(
        markets=markets,
        differences=differences,
        verdict=verdict(differences["main"], differences["robust"]),
        complete=True,
    )
    return dict(gp._clean(result))


def _pct(value: Any) -> str:
    return "—" if value is None else f"{value * 100:.2f}%"


def _points(value: Any) -> str:
    return "—" if value is None else f"{value * 100:+.2f} 点"


def _table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> list[str]:
    def line(cells: Sequence[Any]) -> str:
        return "| " + " | ".join(str(c) for c in cells) + " |"

    return [line(headers), line(["---"] * len(headers)), *[line(r) for r in rows], ""]


def render_markdown(result: Mapping[str, Any]) -> str:
    lines = [f"# F0001 杠杆账表 · {RUN_ID}", "", OWNER_DECISION, "", "## 判定", ""]
    stopped = result["stopped"]
    if stopped is not None:
        lines.append(stopped["text"])
        if stopped["reason"] == "reproduce":
            lines += _table(
                ["尺子", "线", "指标", "值", "下界", "上界", "ok"],
                [
                    [
                        MARKET_TEXT[r["market"]],
                        r["line"],
                        r["metric"],
                        r["value"],
                        r["low"],
                        r["high"],
                        r["ok"],
                    ]
                    for r in result["reproduce"]
                ],
            )
        if stopped["reason"] == "bound":
            for market, b in stopped["bound"].items():
                lines.append(
                    f"{MARKET_TEXT[market]}：L* {b['leverage']:.2f}，上界 {b['bound']:.2f}"
                )
        if stopped["reason"] == "check":
            for market, check in result["check"]["markets"].items():
                lines.append(
                    f"{MARKET_TEXT[market]}：缺 {check['missing_days']} 天，"
                    f"起点目标可用 {check['target_defined_at_start']}"
                )
        return "\n".join([*lines, ""])
    lines += [result["verdict"]["text"], ""]
    for market in MARKETS:
        f = result["markets"][market]
        w = f["window"]
        hold = f["buy_and_hold"]
        lines.append(
            f"{MARKET_TEXT[market]}：开发窗口 {w['window_id']}，请求 {w['start']}..{w['end']}；"
            f"实际收益日 {hold['first_return_date']}..{hold['last_return_date']}；"
            f"数据质量检查 {'PASS' if result['check']['ok'] else 'FAIL'}；"
            f"L* {f['drawdown_match']['leverage']:.2f}，"
            f"L_v {f['volatility_match']['leverage']:.2f}，上界 {f['bound']:.2f}。"
        )
        for setting, label in (("main", "主算"), ("robust", "稳健")):
            for pairing, text in (("drawdown", "回撤"), ("volatility", "波动")):
                difference = result["differences"][setting][market][pairing]
                lines.append(f"- {label}按{text}配平：{_points(difference)}/年")
    lines += [
        "",
        "## 全周期表",
        "",
        "主算：利差 1%、4 bps；稳健：利差 2%、25 bps。读法：第 6 期章程第 25 节。",
        "",
    ]
    for market in MARKETS:
        for setting, label in (("main", "主算"), ("robust", "稳健")):
            lines += [f"### {MARKET_TEXT[market]} · {label}", ""]
            rows = []
            for r in result["markets"][market]["table"][setting]:
                s = r["summary"]
                rows.append(
                    [
                        r["id"],
                        "—" if r["leverage"] is None else f"{r['leverage']:.2f}",
                        _pct(s["geometric_annual_return"]),
                        _pct(s["max_drawdown"]),
                        _pct(s["annual_volatility"]),
                        f"{s['average_exposure']:.2f}",
                        _pct(s["above_one_share"]),
                        f"{s['annual_turnover']:.2f}",
                        _pct(s["annual_spread"]),
                        _points(r["difference"]),
                        _points(r["matched_difference"]),
                    ]
                )
            lines += _table(
                [
                    "线",
                    "L",
                    "几何年化",
                    "最大回撤",
                    "年化波动",
                    "平均敞口",
                    "敞口 > 1 占比",
                    "年换手",
                    "每年利差",
                    "差",
                    "对同平均敞口的差",
                ],
                rows,
            )
    lines += ["## D(L) 曲线", ""]
    for market in MARKETS:
        f = result["markets"][market]
        lines += [f"### {MARKET_TEXT[market]}", ""]
        printed = {round(1.0 + i / 4, 2) for i in range(9)}
        lines += _table(
            ["L", "主算 D(L)", "稳健 D(L)"],
            [
                [
                    f"{p['leverage']:.2f}",
                    _points(p["main"]["difference"]),
                    _points(p["robust"]["difference"]),
                ]
                for p in f["curve"]
                if p["leverage"] in printed
            ],
        )
        for setting in ("main", "robust"):
            r = f["ranges"][setting]
            text = "无" if r is None else f"{r['low']:.2f}..{r['high']:.2f}，连续 {r['contiguous']}"
            lines.append(f"- {setting} 达线范围：{text}")
        for pairing in ("drawdown", "volatility"):
            m = f[f"{pairing}_match"]
            lines.append(f"- {pairing}：{m['flag']}，单调 {m['monotone']}，切换点 {m['switches']}")
    lines += ["", "## 滚动窗口", ""]
    rows = []
    for market in MARKETS:
        for r in result["markets"][market]["rolling"]:
            for key, label in (("all", "全部"), ("near_high", "近高点")):
                s = r[key]
                rows.append(
                    [
                        MARKET_TEXT[market],
                        r["years"],
                        label,
                        s["count"],
                        _points(s["worst"]),
                        _points(s["p10"]),
                        _points(s["median"]),
                        _pct(s["beat_share"]),
                    ]
                )
    lines += _table(
        ["尺子", "年数", "起点", "个数", "最差", "第 10 百分位", "中位", "跑赢占比"], rows
    )
    lines += ["## 年代段（美股大盘）", "", ERA_NOTE, ""]
    us = result["markets"]["us_market"]
    rows = []
    for era in us["eras"]:
        rows.append(
            [
                era["era"],
                *[
                    _pct(era[line][metric])
                    for line in ("buy_and_hold", "drawdown", "volatility")
                    for metric in ("geometric_annual_return", "max_drawdown")
                ],
                _points(era["difference"]["drawdown"]),
                _points(era["difference"]["volatility"]),
            ]
        )
    lines += _table(
        [
            "年代",
            "买入持有年化",
            "买入持有回撤",
            "L* 年化",
            "L* 回撤",
            "L_v 年化",
            "L_v 回撤",
            "D*",
            "D_v",
        ],
        rows,
    )
    if us["old_era_text"]:
        lines += [us["old_era_text"], ""]
    lines += ["## 敏感度", ""]
    for market in MARKETS:
        f = result["markets"][market]
        lines += [f"### {MARKET_TEXT[market]}", ""]
        lines += _table(
            ["设置", "回撤配平差", "波动配平差"],
            [
                [name, _points(s["drawdown"]), _points(s["volatility"])]
                for name, s in f["sensitivities"].items()
            ],
        )
        lines += _table(
            ["按日重置 L", "月底几何年化", "按日几何年化", "差"],
            [
                [
                    f"{r['leverage']:.2f}",
                    _pct(r["month_end"]),
                    _pct(r["daily"]),
                    _points(r["difference"]),
                ]
                for r in f["daily_reset"]
            ],
        )
        lines += [r["text"] for r in f["daily_reset"] if r["text"] is not None]
    lines += ["", "## 局限", "", *[f"- {text}" for text in LIMITATIONS]]
    for market in MARKETS:
        for b in result["markets"][market]["busts"]:
            lines.append(f"- 爆仓：{MARKET_TEXT[market]} {b['setting']} {b['line']} {b['date']}")
    return "\n".join([*lines, "", "production_effect=none；broker_action=none。", ""])


def write_check(check: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(CHECK_JSON).write_text(
        json.dumps(gp._clean(dict(check)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def write_result(result: Mapping[str, Any], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir.joinpath(RESULT_JSON).write_text(
        json.dumps(gp._clean(dict(result)), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    out_dir.joinpath(RESULT_MD).write_text(render_markdown(result), encoding="utf-8")


def load_data(
    snapshot_root: Path,
) -> tuple[dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    return pt.load_data(snapshot_root)


def _load(root: Path) -> tuple[dict[str, BenchData], tr.TrialGate, LoadedSettings, Path]:
    try:
        return load_data(root)
    except (OSError, ValueError, KeyError, tr.TrialError) as exc:
        typer.echo(f"leverage_table: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


@app.callback()
def main() -> None:
    """只印杠杆账，不改研究目标。"""


@app.command("check")
def check_command(
    out: Annotated[Path, typer.Option(help="输入次数的保存目录。")],
    root: Annotated[
        Path, typer.Option(help="长历史快照根目录。待发布后研究会话使用。")
    ] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    datasets, gate, loaded, root_dir = _load(root)
    check = check_inputs(datasets, loaded, gate, root_dir)
    write_check(check, out)
    text = "；".join(
        f"{MARKET_TEXT[m]} {c['return_days']} 天，缺 {c['missing_days']} 天"
        for m, c in check["markets"].items()
    )
    typer.echo(f"leverage_table: {text}；输入检查 {'PASS' if check['ok'] else 'FAIL'}")
    if not check["ok"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，已有结果拒绝覆盖。")],
    root: Annotated[Path, typer.Option(help="长历史快照根目录。")] = qlh.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any(out.joinpath(name).exists() for name in (RESULT_JSON, RESULT_MD)):
        typer.echo("leverage_table: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    datasets, gate, loaded, root_dir = _load(root)
    result = run_probe(
        datasets,
        loaded,
        gate,
        root_dir,
        provenance=git_provenance(PROJECT_ROOT),
        grid=GRID,
        reproduce=REPRODUCE,
    )
    write_check(result["check"], out)
    write_result(result, out)
    if result["stopped"] is not None:
        typer.echo(result["stopped"]["text"])
        raise typer.Exit(code=1)
    typer.echo(f"leverage_table: 判定 {result['verdict']['verdict']}；结果目录 {out}")


if __name__ == "__main__":
    app()
