"""章程第 3.1 节 H-A 的只读探针：美股 1929–2010、QQQ 1999–2010 开发窗口。

腿是评估台自己的 sign 分数；simulate_on 照抄评估台，只改成交日掩码。
同一次运行的 fast_slow 必须和决定表相同才读 H-A；reference 先于 run，
拆分先供复盘线程重写概率。只输出描述表和图，不读验证和一致性窗口，
不写研究账本、记录或试跑，不开题、不改门和规则，不授权生产或券商动作。
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal

import numpy as np
import pandas as pd
import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core.provenance import git_provenance
from ai_trading_system.research_bench import ledger, trial
from ai_trading_system.research_bench.checks import (
    check_no_lookahead,
    check_parameters_consumed,
    cut_dates,
    strategy_targets,
)
from ai_trading_system.research_bench.data import ASSETS, BenchData
from ai_trading_system.research_bench.events import derisk_events
from ai_trading_system.research_bench.falsify import bootstrap_interval
from ai_trading_system.research_bench.metrics import compare, summarize
from ai_trading_system.research_bench.prereg import StrategyEntry, evaluate_rules
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.segments import segment_section, segments, window_returns
from ai_trading_system.research_bench.settings import EpisodeSpec, LoadedSettings, Window
from ai_trading_system.research_bench.signal_strategy import signal_exposure
from ai_trading_system.research_bench.simulate import (
    _FACTORS,
    Rebalance,
    Simulation,
    _checked_weights,
    _needs_trade,
    constant_targets,
    schedule_mask,
    simulate,
)
from ai_trading_system.research_data import qqq_long_history
from ai_trading_system.research_probes import backfill_check, gate_power, plot_run, policy_table

RUN_ID = "TRADING-2565-48"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-22:research_loop_p4"
DIRECTION = "owner_decision:TRADING-2565:2026-10-22:period4_two_questions"
DATA_TRIAL = backfill_check.DATA_TRIAL
MARKETS: tuple[trial.Market, ...] = ("us_market", "qqq")
MARKET_TEXT = policy_table.MARKET_TEXT
FAST_SIGNAL = "tsmom_21"
SLOW_SIGNAL = "tsmom_252"
REENTRY = ("weekly_fast", "none")
Mode = Literal["monthly", "weekly", "h_a"]
RULES = {
    "fast_slow": "月频 fast_slow",
    "fast_slow_weekly": "周频 fast_slow",
    "h_a": "H-A 不对称再入",
}
COST_BPS = policy_table.COST_BPS
QQQ_BULL = "long_bull_200210"
REFERENCE_JSON = "reference.json"
REFERENCE_MD = "reference.md"
RUN_JSON = "h_a.json"
RUN_MD = "h_a.md"
FIGURES = "figures"
# 章程第 5.4 节已登记的描述性概率阶梯，不是投资或生产接受门槛。
ROW3_STEPS = ((0.5, 0.6), (0.3, 0.45))
ROW3_FLOOR = 0.3
ROW3_KEEP = 0.5
ROW3_NO_SHARE = "reference 分母不为正"
# 章程第 3.1 节机械判法；本探针只印，不据此晋级。
SLOW_MEDIAN = 0.08
SLOW_POSITIVE = 8
BULL_RETAIN = 0.5
EXTRA_EXIT_SHARE = 0.5
# 决定表按一位小数的点打印；0.05 点舍入半格，加数值浮点余量。
DECISION_TOLERANCE = 0.0005 + 1e-12
DECISION_TABLE: dict[str, Any] = {
    "us_market": {
        "slow_bear": {
            "slow_bear_192908": 0.278,
            "slow_bear_193208": 0.186,
            "slow_bear_193702": 0.097,
            "slow_bear_193909": 0.045,
            "slow_bear_194605": 0.026,
            "slow_bear_196112": 0.065,
            "slow_bear_196811": 0.118,
            "slow_bear_197212": 0.287,
            "slow_bear_200003": 0.122,
            "slow_bear_200710": 0.153,
        },
        "summary": {
            "count": 10,
            "median_difference": 0.120,
            "positive": 10,
            "min_difference": 0.026,
        },
        "long_bull": {},
    },
    "qqq": {
        "slow_bear": {"slow_bear_200003": 0.119, "slow_bear_200710": 0.066},
        "summary": {"count": 2, "median_difference": 0.092, "positive": 2, "min_difference": 0.066},
        "long_bull": {QQQ_BULL: -0.033},
    },
}
Progress = Callable[[str, str, dict[str, Any]], None]
app = typer.Typer(help="H-A 只读开发窗口探针：先 reference 后 run，不判、不写研究账本。")


def leg_params(signal: Mapping[str, Any]) -> dict[str, Any]:
    return {"signals": [dict(signal)], "mapping": "linear", "low": 0, "high": 1}


def leg_scores(inputs: pd.DataFrame, signals: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    if [s.get("id") for s in signals] != [FAST_SIGNAL, SLOW_SIGNAL]:
        raise ValueError("signals must be tsmom_21 then tsmom_252")
    return pd.DataFrame(
        {
            "fast": signal_exposure(inputs, leg_params(signals[0])),
            "slow": signal_exposure(inputs, leg_params(signals[1])),
        },
        index=inputs.index,
    )


def decide(scores: pd.DataFrame, mode: Mode) -> pd.DataFrame:
    if mode not in ("monthly", "weekly", "h_a"):
        raise ValueError(f"unknown mode {mode}")
    index = pd.DatetimeIndex(scores.index)
    month_end = schedule_mask(index, "month_end")
    week_end = schedule_mask(index, "week_end")
    full = week_end if mode == "weekly" else month_end
    f, s = math.nan, math.nan
    rows = []
    for p, (fast, slow) in enumerate(scores[["fast", "slow"]].itertuples(index=False, name=None)):
        reentry = False
        if full[p]:
            if math.isfinite(fast) and math.isfinite(slow):
                f, s = float(fast), float(slow)
        elif mode == "h_a" and week_end[p] and math.isfinite(f) and f < 1 and fast == 1:
            f, reentry = 1.0, True
        rows.append((fast, slow, f, s, 0.5 * f + 0.5 * s, full[p] or reentry, reentry))
    return pd.DataFrame(
        rows,
        index=scores.index,
        columns=["fast_score", "slow_score", "fast", "slow", "target", "trade", "reentry"],
    )


def asymmetric_targets(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    if set(params) != {"signals", "reentry"} or params["reentry"] not in REENTRY:
        raise ValueError("params need signals and reentry (weekly_fast or none)")
    mode: Mode = "h_a" if params["reentry"] == "weekly_fast" else "monthly"
    return pd.Series(
        decide(leg_scores(inputs, params["signals"]), mode)["target"].to_numpy(), index=inputs.index
    )


def h_a_params(entry: StrategyEntry, reentry: str = "weekly_fast") -> dict[str, Any]:
    return {"signals": [dict(s) for s in entry.params["signals"]], "reentry": reentry}


def simulate_on(
    returns: pd.DataFrame,
    targets: pd.Series,
    trade_days: Sequence[bool] | pd.Series | np.ndarray[Any, Any],
    *,
    start: date,
    end: date | None,
    cost_bps: float,
    band: float = 0.0,
    delay_days: int = 0,
) -> Simulation:
    """Scalar bench simulate, with only its schedule supplied by the caller."""
    if list(returns.columns) != list(ASSETS):
        raise ValueError(f"returns columns must be {list(ASSETS)}")
    if not targets.index.equals(returns.index):
        raise ValueError("targets must be indexed by the same dates as the returns")
    if delay_days < 0:
        raise ValueError("delay_days must not be negative")
    if cost_bps < 0:
        raise ValueError("cost_bps must not be negative")
    if len(trade_days) != len(returns):
        raise ValueError("trade_days must have one entry per date")
    Rebalance("daily", band)  # The bench's band check, without a schedule override.
    index = pd.DatetimeIndex(returns.index)
    first = int(index.searchsorted(pd.Timestamp(start)))
    last = (
        len(index) - 1
        if end is None
        else int(index.searchsorted(pd.Timestamp(end), side="right")) - 1
    )
    if first >= len(index) or last <= first:
        raise ValueError(f"no return dates between {start} and {end}")
    window = returns.iloc[first + 1 : last + 1]
    if window.isna().to_numpy().any():
        bad = window[window.isna().any(axis=1)]
        column = bad.columns[bad.isna().iloc[0].to_numpy()][0]
        raise ValueError(f"{column} has no return on {bad.index[0].date()} inside the window")
    lag = 1 + delay_days
    scheduled = np.asarray(trade_days, dtype=bool)
    values = targets.to_numpy(dtype=float)
    opening = next((p for p in range(first - lag, -1, -1) if scheduled[p]), None)
    if opening is None or not math.isfinite(values[opening]):
        raise ValueError(f"no target is executable by {index[first].date()}; extend the lookback")
    weights = _checked_weights(values[opening], index[opening])
    rate = cost_bps / 10_000.0
    matrix = returns.to_numpy(dtype=float)
    rows: list[tuple[float, float, float, float, float]] = []
    for q in range(first + 1, last + 1):
        day = matrix[q]
        held = sum(w * f for w, f in zip(weights, _FACTORS, strict=True))
        gross = sum(w * r for w, r in zip(weights, day, strict=True))
        if 1.0 + gross <= 0.0:
            raise ValueError(f"portfolio lost everything on {index[q].date()}")
        drifted = [w * (1.0 + r) / (1.0 + gross) for w, r in zip(weights, day, strict=True)]
        turnover = 0.0
        weights = drifted
        decided = q - lag
        if decided >= 0 and scheduled[decided]:
            target = _checked_weights(values[decided], index[decided])
            target_exposure = sum(w * f for w, f in zip(target, _FACTORS, strict=True))
            current = sum(w * f for w, f in zip(drifted, _FACTORS, strict=True))
            if _needs_trade(target_exposure, current, band):
                turnover = sum(abs(t - d) for t, d in zip(target, drifted, strict=True)) / 2.0
                weights = target
        cost = turnover * rate
        net = (1.0 + gross) * (1.0 - cost) - 1.0
        rows.append((net, gross, held, turnover, cost))
    daily = pd.DataFrame(
        rows,
        index=index[first + 1 : last + 1],
        columns=["return", "gross_return", "exposure", "turnover", "cost"],
    )
    return Simulation(daily=daily, start=index[first])


def _held_positions(decided: pd.DataFrame, delay_days: int) -> np.ndarray[Any, Any]:
    trades = np.where(decided["trade"].to_numpy(dtype=bool), np.arange(len(decided)), -1)
    latest = np.maximum.accumulate(trades)
    result = np.full(len(decided), -1, dtype=int)
    lag = 2 + delay_days
    if lag < len(decided):
        result[lag:] = latest[:-lag]
    return result


def held_legs(decided: pd.DataFrame, sim: Simulation, delay_days: int = 0) -> pd.DataFrame:
    positions = decided.index.get_indexer(sim.daily.index)
    if (positions < 0).any():
        raise ValueError("a return date holds no decided legs")
    held = _held_positions(decided, delay_days)[positions]
    if (held < 0).any():
        raise ValueError("a return date holds no decided legs")
    values = decided[["fast", "slow"]].iloc[held].to_numpy(dtype=float)
    if not np.isfinite(values).all():
        raise ValueError("a return date holds no decided legs")
    return pd.DataFrame(values, index=sim.daily.index, columns=["fast", "slow"])


def exit_counts(
    moves: pd.DataFrame, start: str | None = None, end: str | None = None
) -> dict[str, int]:
    counts = plot_run.count_moves(moves, start, end)
    fast, slow = counts["fast"], counts["slow"]
    return {
        "fast_exits": fast["exit"],
        "slow_exits": slow["exit"],
        "exits": fast["exit"] + slow["exit"],
        "entries": fast["entry"] + slow["entry"],
    }


def missed_gains(
    decided: pd.DataFrame, asset: pd.Series, dates: pd.Index, delay_days: int = 0
) -> pd.DataFrame:
    index = pd.DatetimeIndex(decided.index)
    positions = _held_positions(decided, delay_days)
    values = np.full((len(index), 2), np.nan)
    valid = positions >= 0
    values[valid] = decided[["fast", "slow"]].to_numpy(dtype=float)[positions[valid]]
    outside = np.isfinite(values[:, 0]) & (values[:, 0] < 1)
    checks = np.asarray(schedule_mask(index, "month_end")) | np.asarray(
        schedule_mask(index, "week_end")
    )
    positive = np.flatnonzero(checks & (decided["fast_score"].to_numpy() == 1))
    stint = np.full(len(index), -1, dtype=int)
    late = np.zeros(len(index), dtype=bool)
    number = -1
    tau: int | None = None
    for q in range(len(index)):
        if not outside[q]:
            continue
        if q == 0 or not outside[q - 1]:
            number += 1
            later = positive[positive > positions[q]]
            tau = int(later[0]) if len(later) else None
        stint[q] = number
        late[q] = tau is not None and q - (1 + delay_days) - 1 >= tau
    logs = np.log1p(asset.reindex(index).to_numpy(dtype=float))
    fast_part = np.where(np.isfinite(values[:, 0]), 0.5 * (1 - values[:, 0]) * logs, 0.0)
    slow_part = np.where(np.isfinite(values[:, 1]), 0.5 * (1 - values[:, 1]) * logs, 0.0)
    return pd.DataFrame(
        {
            "false_exit": np.where(late, 0.0, fast_part),
            "reentry_lag": np.where(late, fast_part, 0.0),
            "slow": slow_part,
            "stint": stint,
        },
        index=index,
    ).loc[dates]


def lag_share(numerator: float, denominator: float) -> float | None:
    return None if denominator <= 0 else min(1.0, max(0.0, numerator / denominator))


def _shares(row: dict[str, Any]) -> dict[str, Any]:
    fast = row["false_exit"] + row["reentry_lag"]
    return {
        **row,
        "share": lag_share(row["reentry_lag"], fast + row["slow"]),
        "share_fast_only": lag_share(row["reentry_lag"], fast),
    }


def split_rows(gains: pd.DataFrame, bulls: Sequence[EpisodeSpec]) -> list[dict[str, Any]]:
    months = pd.DatetimeIndex(gains.index).to_period("M")
    rows = []
    for bull in bulls:
        part = gains.loc[
            (months >= pd.Period(bull.start, "M")) & (months <= pd.Period(bull.end, "M"))
        ]
        rows.append(
            _shares(
                {
                    "id": bull.id,
                    "start": bull.start,
                    "end": bull.end,
                    "stints": int(part.loc[part["stint"] >= 0, "stint"].nunique()),
                    **{k: float(part[k].sum()) for k in ("false_exit", "reentry_lag", "slow")},
                }
            )
        )
    return rows


def pooled(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _shares(
        {
            "id": "合计",
            **{k: sum(r[k] for r in rows) for k in ("stints", "false_exit", "reentry_lag", "slow")},
        }
    )


def row3_probability(share: float | None) -> float:
    if share is None:
        return ROW3_KEEP
    return next((probability for floor, probability in ROW3_STEPS if share >= floor), ROW3_FLOOR)


@dataclass(frozen=True)
class RulePath:
    rule: str
    decided: pd.DataFrame
    sim: Simulation
    legs: pd.DataFrame
    moves: pd.DataFrame


def market_window(loaded: LoadedSettings, gate: trial.TrialGate, market: trial.Market) -> Window:
    return policy_table.market_window(loaded, gate, market)


def probe_path(
    rule: str, data: BenchData, scores: pd.DataFrame, window: Window, loaded: LoadedSettings
) -> RulePath:
    modes: dict[str, Mode] = {"fast_slow": "monthly", "fast_slow_weekly": "weekly", "h_a": "h_a"}
    decided = decide(scores, modes[rule])
    sim = simulate_on(
        data.returns,
        decided["target"],
        decided["trade"],
        start=window.start,
        end=window.end,
        cost_bps=loaded.cost_bps,
    )
    legs = held_legs(decided, sim)
    return RulePath(rule, decided, sim, legs, plot_run.leg_moves(legs))


def bench_simulation(
    data: BenchData, entry: StrategyEntry, window: Window, loaded: LoadedSettings
) -> Simulation:
    targets = strategy_targets(signal_exposure, data.inputs, entry.params)
    return simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=entry.rebalance.to_rebalance(),
        cost_bps=loaded.cost_bps,
    )


def same_days(left: Simulation, right: Simulation) -> bool:
    return left.start == right.start and left.daily.equals(right.daily)


def matched_simulation(
    data: BenchData, sim: Simulation, window: Window, cost_bps: float
) -> Simulation:
    return simulate(
        data.returns,
        constant_targets(data.returns.index, float(sim.exposure.mean())),
        start=window.start,
        end=window.end,
        rebalance=Rebalance("month_end", 0.0),
        cost_bps=cost_bps,
    )


def event_table(
    sim: Simulation, data: BenchData, loaded: LoadedSettings, episodes: Sequence[EpisodeSpec]
) -> dict[str, Any]:
    return derisk_events(
        sim.exposure,
        data.returns["qqq"].reindex(sim.exposure.index),
        reference_exposure=float(sim.exposure.mean()),
        settings=load_screen_settings().settings.derisk_events,
        episodes=tuple(episodes),
        trading_days_per_year=loaded.settings.trading_days_per_year,
    )


def source_entries(root_dir: Path) -> dict[str, StrategyEntry]:
    return {
        c.id: backfill_check.source_entry(root_dir, c)
        for c in backfill_check.CONFIGS
        if c.id in ("fast_slow", "fast_slow_weekly")
    }


def bench_row(name: str, root_dir: Path) -> policy_table.Row:
    return next(row for row in policy_table.table_rows(root_dir) if row.id == name)


def scores_of(path: RulePath) -> pd.DataFrame:
    return path.decided[["fast_score", "slow_score"]].rename(
        columns={"fast_score": "fast", "slow_score": "slow"}
    )


def equivalence(
    data: BenchData,
    entries: Mapping[str, StrategyEntry],
    paths: Mapping[str, RulePath],
    window: Window,
    loaded: LoadedSettings,
) -> dict[str, bool]:
    found = {
        name: same_days(path.sim, bench_simulation(data, entries[name], window, loaded))
        for name, path in paths.items()
        if name != "h_a"
    }
    if "h_a" in paths:
        decided = decide(scores_of(paths["h_a"]), "monthly")
        off = simulate_on(
            data.returns,
            decided["target"],
            decided["trade"],
            start=window.start,
            end=window.end,
            cost_bps=loaded.cost_bps,
        )
        found["h_a_without_reentry"] = same_days(
            off, bench_simulation(data, entries["fast_slow"], window, loaded)
        )
    return found


def evaluate_h_a(
    path: RulePath,
    data: BenchData,
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    market: trial.Market,
    window: Window,
) -> dict[str, Any]:
    matched = matched_simulation(data, path.sim, window, loaded.cost_bps)
    cash = data.returns["cash"]
    own, base = summarize(path.sim, cash, loaded), summarize(matched, cash, loaded)
    comparison = compare(own, base, loaded)
    expensive = simulate_on(
        data.returns,
        path.decided["target"],
        path.decided["trade"],
        start=window.start,
        end=window.end,
        cost_bps=COST_BPS,
    )
    cost_own = summarize(expensive, cash, loaded)
    cost_base = summarize(matched_simulation(data, expensive, window, COST_BPS), cash, loaded)
    cost_difference = compare(cost_own, cost_base, loaded)["geometric_annual_return"]
    low, high = bootstrap_interval(
        path.sim.returns.to_numpy(dtype=float),
        matched.returns.to_numpy(dtype=float),
        cash.reindex(path.sim.daily.index).to_numpy(dtype=float),
        loaded,
        gate.confidence,
    )
    episodes = gate.markets[market].event_episodes
    events = event_table(
        path.sim, data, loaded, loaded.settings.episodes if episodes is None else episodes
    )
    slow, bull = gate.markets[market].segment_lists(window.window_id)
    rules = evaluate_rules(
        gate.markets[market].rules,
        {
            "comparison": comparison,
            "ci_low": low,
            "ci_high": high,
            "cost_25": {"geometric_annual_return": cost_difference},
        },
    )
    return dict(
        gate_power._clean(
            {
                "row": "h_a",
                "market": market,
                "status": "ok",
                "strategy": {k: own[k] for k in policy_table.METRICS},
                "benchmark": {k: base[k] for k in policy_table.METRICS},
                "comparison": {k: comparison[k] for k in policy_table.COMPARED},
                "cost": {
                    "bps": COST_BPS,
                    "strategy": cost_own["geometric_annual_return"],
                    "benchmark": cost_base["geometric_annual_return"],
                    "difference": cost_difference,
                },
                "events": {k: events[k] for k in ("count", "per_year", "short_count")}
                | {k: v["count"] for k, v in events["outcomes"].items()},
                "segments": policy_table.segment_tables(
                    segment_section(path.sim, matched, slow, bull)
                ),
                "gate": rules,
                "ci": {
                    "low": low["cvar_5pct_relative_improvement"],
                    "high": high["cvar_5pct_relative_improvement"],
                },
            }
        )
    )


def decision_differences(
    row: Mapping[str, Any], market: str, decision_table: Mapping[str, Any] | None = None
) -> list[str]:
    if row.get("status") != "ok":
        return ["没有算出"]
    expected = (DECISION_TABLE if decision_table is None else decision_table)[market]
    section = row.get("segments") or {}
    differences = []

    def different(actual: Any, value: float) -> bool:
        return (
            actual is None or not math.isfinite(actual) or abs(actual - value) > DECISION_TOLERANCE
        )

    for kind, key, suffix in (
        ("slow_bear", "difference", "D"),
        ("long_bull", "annual_difference", "年化差"),
    ):
        if kind == "long_bull":
            summary = section.get("slow_bear_summary") or {}
            for metric, value in expected["summary"].items():
                actual = summary.get(metric)
                if actual != value if metric in ("count", "positive") else different(actual, value):
                    differences.append(
                        {"count": "慢熊段数", "positive": "D > 0 段数"}.get(
                            metric, f"慢熊 {metric}"
                        )
                    )
        actuals = {s["id"]: s.get(key) for s in section.get(kind, [])}
        for sid, value in expected[kind].items():
            if different(actuals.get(sid), value):
                differences.append(f"{sid} {suffix}")
    return differences


def lag_table(
    path: RulePath, data: BenchData, loaded: LoadedSettings, slow: Sequence[EpisodeSpec]
) -> dict[str, Any]:
    episodes = event_table(path.sim, data, loaded, slow)["episodes"]
    found = {}
    for segment in slow:
        counts = plot_run.count_moves(path.moves, segment.start, segment.end)
        reading = episodes.get(segment.id, {})
        found[segment.id] = {
            "exit_lag_days": reading.get("exit_lag_days"),
            "reentry_lag_days": reading.get("reentry_lag_days"),
            **{
                f"{leg}_{kind}": counts[leg][kind]
                for leg in plot_run.LEGS
                for kind in plot_run.KINDS
            },
        }
    return found


def in_window(index: pd.Index, window: Window) -> np.ndarray[Any, Any]:
    dates = pd.DatetimeIndex(index)
    return np.asarray(
        (dates > pd.Timestamp(window.start))
        & (True if window.end is None else dates <= pd.Timestamp(window.end))
    )


def reentry_count(
    decided: pd.DataFrame, window: Window, start: str | None = None, end: str | None = None
) -> int:
    inside = in_window(decided.index, window)
    months = pd.DatetimeIndex(decided.index).to_period("M")
    if start is not None:
        inside &= months >= pd.Period(start, "M")
    if end is not None:
        inside &= months <= pd.Period(end, "M")
    return int(decided.loc[inside, "reentry"].sum())


def h_a_checks(data: BenchData, entry: StrategyEntry, window: Window) -> list[str]:
    index = pd.DatetimeIndex(data.inputs.index)
    start = pd.Timestamp(window.start)
    end = index[-1] if window.end is None else pd.Timestamp(window.end)
    params = h_a_params(entry)
    problems = check_no_lookahead(
        asymmetric_targets, data.inputs, params, cut_dates(index, start, end, 8)
    )
    return (
        problems + check_parameters_consumed(asymmetric_targets, data.inputs, params, start, end)[0]
    )


def draw(
    step: str,
    market: trial.Market,
    paths: Mapping[str, RulePath],
    results: Mapping[str, Any],
    data: BenchData,
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    window: Window,
    provenance: Mapping[str, Any],
    out_dir: Path,
) -> list[dict[str, Any]]:
    slow, bull = gate.markets[market].segment_lists(window.window_id)
    bands = [
        plot_run.SegmentBand(s.id, kind, s.start, s.end)
        for kind, specs in (("slow_bear", slow), ("long_bull", bull))
        for s in specs
    ]
    bands += [
        plot_run.SegmentBand(s.id, s.kind, s.start, s.end)
        for s in segments(window_returns(data.returns["qqq"], window))
        if s.kind == "fast_drop"
    ]
    rules = []
    readings: dict[str, dict[str, plot_run.SegmentReading]] = {}
    footer = [
        f"{RUN_ID} {step}；{MARKET_TEXT[market]}；窗口 {window.window_id} "
        f"{window.start.isoformat()}..{window.end.isoformat() if window.end else ''}；"
        f"提交 {provenance['commit']}；快照 "
        + "、".join(
            str(data.source[k]) for k in ("snapshot_id", "signal_snapshot_id") if data.source.get(k)
        ),
        "竖线：每条腿每次出入一根，落在生效日（t 收盘定、t+1 收盘成交、t+2 持有）的指数上；"
        "出场朝上 ▼，入场朝下 ▲；慢腿长、快腿短；灰 = 月频 fast_slow，蓝 = 对比规则。",
    ]
    roles: tuple[plot_run.Role, ...] = ("base", "compared")
    for role, (name, path) in zip(roles, paths.items(), strict=True):
        rules.append(
            plot_run.RuleSeries(
                RULES[name],
                role,
                path.sim.exposure,
                path.sim.returns,
                path.legs,
                matched_simulation(data, path.sim, window, loaded.cost_bps).returns,
            )
        )
        lags = lag_table(path, data, loaded, slow)
        for kind, key in (("slow_bear", "difference"), ("long_bull", "annual_difference")):
            for segment in results[name]["segments"][kind]:
                lag = lags.get(segment["id"], {})
                readings.setdefault(segment["id"], {})[role] = plot_run.SegmentReading(
                    segment.get(key), lag.get("exit_lag_days"), lag.get("reentry_lag_days")
                )
        episodes = gate.markets[market].event_episodes
        events = event_table(
            path.sim, data, loaded, loaded.settings.episodes if episodes is None else episodes
        )
        footer.append(
            f"{RULES[name]}：减仓事件 {events['count']}，每年 {events['per_year']:.2f}，"
            f"短事件 {events['short_count']}（持有敞口 ≤ 平均敞口 × 0.75，评估台事件表口径）"
        )
    title = f"{MARKET_TEXT[market]}，" + " 对 ".join(RULES[name] for name in paths)
    asset = data.returns["qqq"].reindex(next(iter(paths.values())).sim.daily.index)
    spec = plot_run.FigureSpec(
        step, market, title, window.window_id, asset, bands, rules, readings, footer
    )
    return [
        {
            "name": r.name,
            "sha256": r.sha256,
            "bytes": r.bytes,
            "counts": gate_power._clean(r.counts),
        }
        for r in plot_run.figures(spec, out_dir / FIGURES)
    ]


def _begin(
    step: str,
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    root_dir: Path,
    provenance: Mapping[str, Any],
    progress: Progress | None,
    decision_table: Mapping[str, Any] | None,
) -> dict[str, Any]:
    rules: dict[str, Any] = {}
    for market in MARKETS:
        rules[market] = {}
        names = ("fast_slow", "fast_slow_weekly") if step == "reference" else ("fast_slow",)
        for name in names:
            row = policy_table.evaluate(
                bench_row(name, root_dir), market, datasets[market], loaded, gate
            )
            rules[market][name] = row
            if progress is not None:
                progress(market, name, row)
    windows = {m: market_window(loaded, gate, m) for m in MARKETS}
    evaluated: dict[str, dict[str, str | None]] = {}
    for market, window in windows.items():
        dates = pd.DatetimeIndex(datasets[market].returns.index)
        days = dates[
            (dates >= pd.Timestamp(window.start))
            & (True if window.end is None else dates <= pd.Timestamp(window.end))
        ][1:]
        evaluated[market] = {
            "evaluated_start": days[0].date().isoformat() if len(days) else None,
            "evaluated_end": days[-1].date().isoformat() if len(days) else None,
        }
    stop = {m: decision_differences(rules[m]["fast_slow"], m, decision_table) for m in MARKETS}
    backfill = next(
        r
        for r in policy_table.backfill_check([rules["us_market"]["fast_slow"]])
        if r["row"] == "fast_slow"
    )
    return {
        "run_id": RUN_ID,
        "step": step,
        "owner_decision": OWNER_DECISION,
        "direction": DIRECTION,
        "data_trial": DATA_TRIAL,
        "gate_sha256": ledger.chain_digest(
            (root_dir / trial.TRIALS_DIR / "prereg.yaml").read_bytes()
        ),
        "sources": {m: dict(datasets[m].source) for m in MARKETS},
        "settings_sha256": loaded.sha256,
        "windows": {
            m: {
                "window_id": w.window_id,
                "start": w.start.isoformat(),
                "end": None if w.end is None else w.end.isoformat(),
                **evaluated[m],
            }
            for m, w in windows.items()
        },
        "provenance": dict(provenance),
        "rules": rules,
        "stop": stop,
        "stopped": any(stop.values()),
        "backfill": [backfill],
        "complete": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def run_reference(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    out_dir: Path,
    progress: Progress | None = None,
    decision_table: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    result = _begin(
        "reference", datasets, loaded, gate, root_dir, provenance, progress, decision_table
    )
    if result["stopped"]:
        return dict(gate_power._clean(result))
    entries = source_entries(root_dir)
    for key in ("equivalence", "moves", "split", "lags", "figures"):
        result[key] = {}
    for market in MARKETS:
        data = datasets[market]
        window = market_window(loaded, gate, market)
        scores = leg_scores(data.inputs, entries["fast_slow"].params["signals"])
        paths = {name: probe_path(name, data, scores, window, loaded) for name in entries}
        eq = equivalence(data, entries, paths, window, loaded)
        result["equivalence"][market] = eq
        if not all(eq.values()):
            continue
        slow, bull = gate.markets[market].segment_lists(window.window_id)
        result["moves"][market] = {}
        for name, path in paths.items():
            counts = exit_counts(path.moves)
            years = len(path.sim.daily) / loaded.settings.trading_days_per_year
            result["moves"][market][name] = {**counts, "per_year": counts["exits"] / years}
        monthly = paths["fast_slow"]
        rows = split_rows(
            missed_gains(monthly.decided, data.returns["qqq"], monthly.sim.daily.index), bull
        )
        result["split"][market] = {"rows": rows, "pooled": pooled(rows)}
        result["lags"][market] = {
            name: lag_table(path, data, loaded, slow) for name, path in paths.items()
        }
        result["figures"][market] = draw(
            "reference",
            market,
            paths,
            result["rules"][market],
            data,
            loaded,
            gate,
            window,
            provenance,
            out_dir,
        )
    cell = next(
        (r for r in result["split"].get("qqq", {}).get("rows", []) if r["id"] == QQQ_BULL), None
    )
    share = None if cell is None else cell["share"]
    result["row3"] = {
        "share": share,
        "share_fast_only": None if cell is None else cell["share_fast_only"],
        "probability": row3_probability(share),
        "kept": share is None,
        "reason": "QQQ 2002–07 没有算出"
        if cell is None
        else ROW3_NO_SHARE
        if share is None
        else None,
        "us_pooled_share": result["split"].get("us_market", {}).get("pooled", {}).get("share"),
    }
    result["complete"] = all(all(result["equivalence"].get(m, {}).values()) for m in MARKETS)
    return dict(gate_power._clean(result))


def reference_problems(reference: Mapping[str, Any]) -> None:
    if reference.get("run_id") != RUN_ID or reference.get("step") != "reference":
        raise ValueError(f"not a {RUN_ID} reference result")
    if reference.get("stopped") or reference.get("complete") is not True:
        raise ValueError("the reference stopped or is incomplete")


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    root_dir: Path,
    *,
    reference: Mapping[str, Any],
    provenance: Mapping[str, Any],
    out_dir: Path,
    progress: Progress | None = None,
    decision_table: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    reference_problems(reference)
    result = _begin("run", datasets, loaded, gate, root_dir, provenance, progress, decision_table)
    result["reference_row3"] = dict(reference["row3"])
    if result["stopped"]:
        return dict(gate_power._clean(result))
    entries = source_entries(root_dir)
    for key in ("checks", "equivalence", "h_a", "exits", "lags", "figures"):
        result[key] = {}
    for market in MARKETS:
        data = datasets[market]
        window = market_window(loaded, gate, market)
        result["checks"][market] = h_a_checks(data, entries["fast_slow"], window)
        scores = leg_scores(data.inputs, entries["fast_slow"].params["signals"])
        paths = {
            name: probe_path(name, data, scores, window, loaded) for name in ("fast_slow", "h_a")
        }
        eq = equivalence(data, entries, paths, window, loaded)
        result["equivalence"][market] = eq
        if not all(eq.values()) or result["checks"][market]:
            continue
        found = evaluate_h_a(paths["h_a"], data, loaded, gate, market, window)
        result["h_a"][market] = found
        if progress is not None:
            progress(market, "h_a", found)
        slow, bull = gate.markets[market].segment_lists(window.window_id)
        span = next((b for b in bull if b.id == QQQ_BULL), None) if market == "qqq" else None
        if market == "qqq" and span is None:
            raise ValueError(f"missing {QQQ_BULL} in the gate")
        start, end = (None, None) if span is None else (span.start, span.end)
        counts = {name: exit_counts(path.moves, start, end) for name, path in paths.items()}
        extra = counts["h_a"]["exits"] - counts["fast_slow"]["exits"]
        if extra < 0:
            raise AssertionError("H-A cannot remove monthly exits")
        result["exits"][market] = {
            "span": "window" if span is None else QQQ_BULL,
            **counts,
            "extra": extra,
            "reentries": reentry_count(paths["h_a"].decided, window, start, end),
        }
        result["lags"][market] = {
            name: lag_table(path, data, loaded, slow) for name, path in paths.items()
        }
        results = {"fast_slow": result["rules"][market]["fast_slow"], "h_a": found}
        result["figures"][market] = draw(
            "run", market, paths, results, data, loaded, gate, window, provenance, out_dir
        )
    result["complete"] = all(m in result["h_a"] for m in MARKETS)
    result["readings"] = (
        row_readings(result["h_a"], result["rules"], result["exits"]) if result["complete"] else []
    )
    result["first_four"] = first_four(result["readings"])
    return dict(gate_power._clean(result))


def _bull_cell(row: Mapping[str, Any]) -> Any:
    return next(s["annual_difference"] for s in row["segments"]["long_bull"] if s["id"] == QQQ_BULL)


def row_readings(
    h_a: Mapping[str, Any], bench: Mapping[str, Any], exits: Mapping[str, Any]
) -> list[dict[str, Any]]:
    summary = h_a["us_market"]["segments"]["slow_bear_summary"]
    us = {k: summary[k] for k in ("median_difference", "positive", "count")}
    differences = [s["difference"] for s in h_a["qqq"]["segments"]["slow_bear"]]
    cell, base = _bull_cell(h_a["qqq"]), _bull_cell(bench["qqq"]["fast_slow"])
    threshold = base * BULL_RETAIN
    geometric = (
        h_a["us_market"]["strategy"]["geometric_annual_return"]
        - bench["us_market"]["fast_slow"]["strategy"]["geometric_annual_return"]
    )
    exit_reading = {
        m: {"extra": exits[m]["extra"], "fast_slow_exits": exits[m]["fast_slow"]["exits"]}
        for m in MARKETS
    }
    return [
        {
            "row": 1,
            "reading": us,
            "threshold": "D 中位 ≥ +8.0 且 D > 0 ≥ 8 段",
            "holds": us["median_difference"] is not None
            and us["median_difference"] >= SLOW_MEDIAN
            and us["positive"] >= SLOW_POSITIVE,
        },
        {
            "row": 2,
            "reading": {"differences": differences},
            "threshold": "两段 D 都 > 0",
            "holds": len(differences) == 2 and all(d is not None and d > 0 for d in differences),
        },
        {
            "row": 3,
            "reading": {"h_a": cell, "fast_slow": base},
            "threshold": threshold,
            "holds": cell is not None and cell >= threshold,
        },
        {
            "row": 4,
            "reading": {"geometric_difference": geometric},
            "threshold": 0.0,
            "holds": geometric >= 0,
        },
        {"row": 5, "reading": None, "threshold": "不读（章程第 8 节）", "holds": None},
        {
            "row": 6,
            "reading": exit_reading,
            "threshold": "两个窗口：多出的退出 ≤ fast_slow 退出 / 2",
            "holds": all(
                r["extra"] <= r["fast_slow_exits"] * EXTRA_EXIT_SHARE for r in exit_reading.values()
            ),
        },
    ]


def first_four(readings: Sequence[Mapping[str, Any]]) -> bool | None:
    if not readings:
        return None
    by_row = {r["row"]: r["holds"] for r in readings}
    return all(by_row.get(n) is True for n in (1, 2, 3, 4))


def _share(value: Any) -> str:
    return "没有代价，份额不定义" if value is None else f"{value:.2f}"


def _probability(row: Mapping[str, Any], reason: bool = False) -> str:
    if row["kept"]:
        return f"保持 {row['probability']}" + (f"（{row['reason']}）" if reason else "")
    return str(row["probability"]) + (
        "（≥ 0.5 写 0.6，0.3–0.5 写 0.45，< 0.3 写 0.3）" if reason else ""
    )


def _header(result: Mapping[str, Any], title: str) -> list[str]:
    sources = result["sources"]
    lines = [
        title,
        "",
        f"- 运行：{result['run_id']} {result['step']}；依据 {result['owner_decision']}"
        f"（方向 {result['direction']}）；B 区探针，不判、不计数、不改门、"
        "不写研究账本，不读验证和一致性窗口。",
        f"- 提交 {result['provenance']['commit']}；数据：计划 {result['data_trial']} 的快照"
        f"（美股大盘 {sources['us_market'].get('signal_snapshot_id')}，"
        f"QQQ {sources['qqq'].get('snapshot_id')}），"
        f"载入后截到 2010-12-31；段清单取自门文件（sha256 {result['gate_sha256']}）。",
    ]
    for market, window in result["windows"].items():
        lines.append(
            f"- 窗口（{MARKET_TEXT[market]}）：{window['window_id']}，"
            f"请求 {window['start']}..{window['end'] or ''}；"
            f"实际评估 {window['evaluated_start']}..{window['evaluated_end']}；"
            "数据质量：载入入口校验，来源见 JSON sources。"
        )
    if result["stopped"]:
        for market, differences in result["stop"].items():
            if differences:
                lines.append(
                    f"- 同一次运行的 fast_slow 和决定表不同（{MARKET_TEXT[market]}）："
                    + "、".join(differences)
                )
        lines.append("- 按章程第 3.1 节停下：不读 H-A，不出图。")
    else:
        lines.append(
            "- 同一次运行的 fast_slow 和决定表（TRADING-2565-42）相同（差都不超过 0.05 点）。"
        )
    for item in result["backfill"]:
        text = "一致" if item["consistent"] else "不一致：" + "、".join(item["differs"])
        lines.append(f"- 和回填检验（TRADING-2567-33）：{text}")
    for market, eq in result.get("equivalence", {}).items():
        text = (
            "探针的腿和成交日逐日复现评估台"
            if all(eq.values())
            else "不等：" + "、".join(k for k, v in eq.items() if not v)
        )
        lines.append(f"- 等同核对（{MARKET_TEXT[market]}）：{text}")
    return lines


def _table(
    lines: list[str], title: str, headers: Sequence[str], rows: Sequence[Sequence[Any]]
) -> None:
    lines.extend(["", title, "", policy_table._table_row(headers), "|" + "---|" * len(headers)])
    lines.extend(policy_table._table_row(row) for row in rows)


def _details(lines: list[str], result: Mapping[str, Any]) -> None:
    for market in MARKETS:
        rules = dict(result["rules"][market])
        if market in result.get("h_a", {}):
            rules["h_a"] = result["h_a"][market]
        names = list(rules)
        for kind, key, title in (
            ("slow_bear", "difference", "慢熊段 D（点）"),
            ("long_bull", "annual_difference", "长牛段年化差（点/年）"),
        ):
            good = {
                name: {s["id"]: s for s in row.get("segments", {}).get(kind, [])}
                for name, row in rules.items()
            }
            ids = list(dict.fromkeys(sid for segments_ in good.values() for sid in segments_))
            _table(
                lines,
                f"## {MARKET_TEXT[market]}：{title}",
                ["段", *[RULES[n] for n in names]],
                [
                    [sid, *[policy_table._points(good[n].get(sid, {}).get(key)) for n in names]]
                    for sid in ids
                ],
            )
    rows = []
    for market, rules in result.get("lags", {}).items():
        for name, segments_ in rules.items():
            for sid, item in segments_.items():
                rows.append(
                    [
                        MARKET_TEXT[market],
                        RULES[name],
                        sid,
                        *[
                            item[k]
                            for k in (
                                "fast_exit",
                                "fast_entry",
                                "slow_exit",
                                "slow_entry",
                                "exit_lag_days",
                                "reentry_lag_days",
                            )
                        ],
                    ]
                )
    _table(
        lines,
        "## 慢熊段的腿出入和滞后（交易日）",
        ["市场", "规则", "段", "快腿出", "快腿入", "慢腿出", "慢腿入", "出场滞后", "回仓滞后"],
        rows,
    )
    _table(
        lines,
        "## 图",
        ["文件", "sha256", "字节"],
        [
            [r["name"], r["sha256"], r["bytes"]]
            for records in result.get("figures", {}).values()
            for r in records
        ],
    )


def render_reference(result: Mapping[str, Any]) -> str:
    lines = _header(result, "# H-A 的 reference：月频和周频 fast_slow，长牛代价拆分")
    if not result["stopped"]:
        row3 = result["row3"]
        lines.extend(
            [
                "",
                "## 第 3 行的份额（章程第 5.4 节）",
                "",
                f"- QQQ 2002–07：再入迟占代价的份额 {_share(row3['share'])}"
                f"（只算快腿 {_share(row3['share_fast_only'])}）。"
                f"美股七段合计份额 {_share(row3['us_pooled_share'])}，只作对照。"
                "份额截在 0 到 1：再入迟 ≤ 0 记 0，大于分母记 1。",
                f"- 第 3 行按份额应写 {_probability(row3, True)}；"
                "复盘线程照抄进记分表以后才 `run`。",
            ]
        )
        rows = []
        for market, split in result["split"].items():
            for r in [*split["rows"], *([split["pooled"]] if market == "us_market" else [])]:
                rows.append(
                    [
                        MARKET_TEXT[market],
                        r["id"],
                        r["stints"],
                        *[
                            policy_table._points(r[k])
                            for k in ("false_exit", "reentry_lag", "slow")
                        ],
                        _share(r["share"]),
                        _share(r["share_fast_only"]),
                    ]
                )
        _table(
            lines,
            "## 长牛代价拆分（带符号的 log 收益，点）",
            ["市场", "段", "快腿在外段数", "假退出", "再入迟", "慢腿", "份额", "只算快腿的份额"],
            rows,
        )
        _table(
            lines,
            "## 腿的出入次数",
            ["市场", "规则", "快腿出", "慢腿出", "退出", "入场", "退出/年"],
            [
                [
                    MARKET_TEXT[m],
                    RULES[n],
                    *[c[k] for k in ("fast_exits", "slow_exits", "exits", "entries")],
                    f"{c['per_year']:.2f}",
                ]
                for m, rules in result["moves"].items()
                for n, c in rules.items()
            ],
        )
        _details(lines, result)
    return "\n".join([*lines, "", "production_effect=none；broker_action=none。", ""])


def _reading_text(row: Mapping[str, Any]) -> str:
    r, number = row["reading"], row["row"]
    points = policy_table._points
    if number == 1:
        return f"D 中位 {points(r['median_difference'])}，D > 0 {r['positive']}/{r['count']}"
    if number == 2:
        return "、".join(points(d) for d in r["differences"])
    if number == 3:
        return f"H-A {points(r['h_a'])}，fast_slow {points(r['fast_slow'])}"
    if number == 4:
        return f"H-A − fast_slow {points(r['geometric_difference'])} 点/年"
    if number == 5:
        return "不读"
    return "；".join(
        f"{MARKET_TEXT[m]}多出 {r[m]['extra']}，fast_slow 退出 {r[m]['fast_slow_exits']}"
        for m in MARKETS
    )


def _holds(value: Any) -> str:
    return "—" if value is None else "成立" if value else "不成立"


def render_run(result: Mapping[str, Any]) -> str:
    lines = _header(result, "# H-A 不对称再入：读数")
    if not result["stopped"]:
        row3 = result["reference_row3"]
        lines.append(
            f"- reference 的第 3 行份额 {_share(row3['share'])}"
            f"（按份额应写 {_probability(row3)}）。"
        )
        for market, problems in result["checks"].items():
            if problems:
                lines.append(f"- H-A 核对（{MARKET_TEXT[market]}）没过：" + "、".join(problems))
        if result["complete"]:
            _table(
                lines,
                "## 第 1 到 6 行（章程第 3.1 节判法，机械比较；对错由循环线程填进记分表）",
                ["行", "读数", "门槛", "按判法"],
                [
                    [
                        r["row"],
                        _reading_text(r),
                        f"≥ {policy_table._points(r['threshold'])}"
                        if isinstance(r["threshold"], (int, float))
                        else r["threshold"],
                        _holds(r["holds"]),
                    ]
                    for r in result["readings"]
                ],
            )
            lines.append(f"- 第 1 到 4 行都成立：{'是' if result['first_four'] else '否'}")
        rows = []
        for market in MARKETS:
            rules = {**result["rules"][market]}
            if market in result["h_a"]:
                rules["h_a"] = result["h_a"][market]
            for name, row in rules.items():
                if row["status"] != "ok":
                    rows.append([MARKET_TEXT[market], RULES[name], "算不出"])
                    continue
                own, comp, cost = row["strategy"], row["comparison"], row["cost"]
                rows.append(
                    [
                        MARKET_TEXT[market],
                        RULES[name],
                        policy_table._with_difference(
                            own["geometric_annual_return"], comp["geometric_annual_return"]
                        ),
                        policy_table._with_difference(cost["strategy"], cost["difference"]),
                        policy_table._with_difference(own["max_drawdown"], comp["max_drawdown"]),
                        f"{own['cvar_5pct'] * 100:.2f}%"
                        f"（{policy_table._relative(comp['cvar_5pct_relative_improvement'])}）"
                        if own["cvar_5pct"] is not None
                        else "—",
                        f"{own['average_exposure']:.2f}",
                        f"{own['annual_turnover']:.2f}",
                    ]
                )
        _table(
            lines,
            "## 主表（括号内对同平均敞口固定敞口）",
            [
                "市场",
                "规则",
                "几何年化",
                "25 bps 下几何年化",
                "最大回撤",
                "最差 5% 日均",
                "平均敞口",
                "年换手",
            ],
            rows,
        )
        _table(
            lines,
            "## 退出次数",
            ["市场", "范围", "月频退出", "H-A 退出", "多出", "周末再入"],
            [
                [
                    MARKET_TEXT[m],
                    r["span"],
                    r["fast_slow"]["exits"],
                    r["h_a"]["exits"],
                    r["extra"],
                    r["reentries"],
                ]
                for m, r in result["exits"].items()
            ],
        )
        _table(
            lines,
            "## 门的条件（只印，不判，H-A 不走试跑门，章程第 3.1 节）",
            ["市场", "条件", "读数", "门槛", "印出"],
            [
                [
                    MARKET_TEXT[m],
                    r.get("metric"),
                    r.get("observed"),
                    f"{r['op']} {r['value']}",
                    _holds(r.get("holds")),
                ]
                for m, row in result["h_a"].items()
                for r in row["gate"]
            ],
        )
        _details(lines, result)
    return "\n".join([*lines, "", "production_effect=none；broker_action=none。", ""])


def write_json(result: Mapping[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(gate_power._clean(result), indent=2, ensure_ascii=False, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def load_inputs(root: Path) -> tuple[dict[str, BenchData], trial.TrialGate, LoadedSettings, Path]:
    return policy_table.load_data(root)


def _load(root: Path) -> tuple[dict[str, BenchData], trial.TrialGate, LoadedSettings, Path]:
    try:
        return load_inputs(root)
    except (OSError, ValueError, KeyError, trial.TrialError) as exc:
        typer.echo(f"asymmetric_reentry: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


def check_rows(
    datasets: Mapping[str, BenchData], loaded: LoadedSettings, gate: trial.TrialGate, root_dir: Path
) -> list[dict[str, Any]]:
    found = []
    for market in MARKETS:
        row: dict[str, Any] = {"market": market}
        try:
            window = market_window(loaded, gate, market)
            entries = source_entries(root_dir)
            data = datasets[market]
            scores = leg_scores(data.inputs, entries["fast_slow"].params["signals"])
            dates = pd.DatetimeIndex(scores.index)
            mask = (dates >= pd.Timestamp(window.start)) & (
                True if window.end is None else dates <= pd.Timestamp(window.end)
            )
            missing = int(scores.loc[mask].isna().any(axis=1).sum())
            paths = {
                name: probe_path(name, data, scores, window, loaded) for name in (*entries, "h_a")
            }
            eq = equivalence(data, entries, paths, window, loaded)
            problems = h_a_checks(data, entries["fast_slow"], window)
            row.update(
                window_id=window.window_id,
                missing_days=missing,
                equivalence=eq,
                problems=problems,
                ok=bool(mask.any()) and missing == 0 and all(eq.values()) and not problems,
            )
        except Exception as exc:
            row.update(ok=False, error=policy_table._error(exc))
        found.append(row)
    return found


def _print_checks(rows: Sequence[Mapping[str, Any]]) -> bool:
    for row in rows:
        prefix = f"asymmetric_reentry: {row['market']}"
        if "error" in row:
            typer.echo(f"{prefix} 算不出：{row['error']}")
        else:
            eq = "、".join(f"{k} {'同' if v else '不同'}" for k, v in row["equivalence"].items())
            check = "过" if not row["problems"] else "没过：" + "、".join(row["problems"])
            typer.echo(
                f"{prefix} 窗口 {row['window_id']} 缺 {row['missing_days']} 天；等同核对 {eq}；"
                f"H-A 前视和参数核对 {check}；{'可以' if row['ok'] else '不行'}"
            )
    return all(row["ok"] for row in rows)


def _preflight(root: Path) -> tuple[dict[str, BenchData], trial.TrialGate, LoadedSettings, Path]:
    datasets, gate, loaded, root_dir = _load(root)
    if not _print_checks(check_rows(datasets, loaded, gate, root_dir)):
        typer.echo("asymmetric_reentry: 开跑前检查没过，停下。")
        raise typer.Exit(code=1)
    return datasets, gate, loaded, root_dir


def _no_overwrite(out: Path, names: Sequence[str]) -> None:
    if any((out / name).exists() for name in (*names, FIGURES)):
        typer.echo("asymmetric_reentry: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)


def _progress(market: str, name: str, row: dict[str, Any]) -> None:
    typer.echo(f"asymmetric_reentry: {market} {name} {row['status']}")


@app.callback()
def main() -> None:
    """只量开发窗口，reference 先于 run。"""


@app.command("check")
def check_command(
    root: Annotated[Path, typer.Option()] = qqq_long_history.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    datasets, gate, loaded, root_dir = _load(root)
    if not _print_checks(check_rows(datasets, loaded, gate, root_dir)):
        raise typer.Exit(code=1)


@app.command("reference")
def reference_command(
    out: Annotated[Path, typer.Option()],
    root: Annotated[Path, typer.Option()] = qqq_long_history.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    _no_overwrite(out, (REFERENCE_JSON, REFERENCE_MD))
    datasets, gate, loaded, root_dir = _preflight(root)
    out.mkdir(parents=True, exist_ok=True)
    result = run_reference(
        datasets,
        loaded,
        gate,
        root_dir,
        provenance=git_provenance(PROJECT_ROOT),
        out_dir=out,
        progress=_progress,
    )
    write_json(result, out / REFERENCE_JSON)
    (out / REFERENCE_MD).write_text(render_reference(result), encoding="utf-8")
    if result["stopped"]:
        typer.echo("asymmetric_reentry: 同一次运行的 fast_slow 和决定表不同，停下；见报告头部。")
        raise typer.Exit(code=1)
    row3 = result["row3"]
    typer.echo(
        f"asymmetric_reentry: QQQ 2002–07 再入迟份额 {_share(row3['share'])}，"
        f"第 3 行按份额应写 {_probability(row3)}；结果目录 {out}"
    )


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option()],
    reference: Annotated[Path, typer.Option()],
    root: Annotated[Path, typer.Option()] = qqq_long_history.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    _no_overwrite(out, (RUN_JSON, RUN_MD))
    try:
        previous = json.loads(reference.read_text(encoding="utf-8"))
        if not isinstance(previous, dict):
            raise ValueError(f"not a {RUN_ID} reference result")
        reference_problems(previous)
        previous["row3"]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        typer.echo(f"asymmetric_reentry: reference 不能用：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None
    datasets, gate, loaded, root_dir = _preflight(root)
    out.mkdir(parents=True, exist_ok=True)
    result = run_probe(
        datasets,
        loaded,
        gate,
        root_dir,
        reference=previous,
        provenance=git_provenance(PROJECT_ROOT),
        out_dir=out,
        progress=_progress,
    )
    write_json(result, out / RUN_JSON)
    (out / RUN_MD).write_text(render_run(result), encoding="utf-8")
    if result["stopped"] or not result["complete"]:
        typer.echo("asymmetric_reentry: 没有读完（停下核对或等同核对没过）；见报告头部。")
        raise typer.Exit(code=1)
    for row in result["readings"]:
        typer.echo(f"asymmetric_reentry: 第 {row['row']} 行 {_holds(row['holds'])}")
    typer.echo(f"asymmetric_reentry: 完成；结果目录 {out}")


if __name__ == "__main__":
    app()
