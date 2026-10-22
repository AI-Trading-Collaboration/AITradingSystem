"""章程第 3.2 节 H-B 的只读探针：美股 1929–2010、QQQ 1999–2010 两个开发窗口。

状态只看月底已知的 Aaa 收益率和六个月底前之差，t 收盘决定、t+1 收盘成交、t+2 持有。
simulate_reserve 照抄评估台，只多一格国债；同一次运行的 T0008 必须和它的记录相同才读 H-B。
第 1 行按 Baker 选的「按实际持有扣」读 r′，尺子本身不动。
只输出描述表和图，不读验证和一致性窗口，不写研究账本、记录或试跑，
不开题、不改门和规则，不授权生产或券商动作。
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from numbers import Integral, Real
from pathlib import Path
from typing import Annotated, Any

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
)
from ai_trading_system.research_bench.data import ASSETS, BenchData, strategy_returns
from ai_trading_system.research_bench.fixed_strategy import fixed_exposure
from ai_trading_system.research_bench.metrics import compare, summarize
from ai_trading_system.research_bench.portfolio import portfolio_section, quiet_dates, quiet_excess
from ai_trading_system.research_bench.prereg import Rule, evaluate_rules
from ai_trading_system.research_bench.run import (
    PRIMARY_BENCHMARK,
    RunSpec,
    benchmark_simulations,
    run_strategy,
)
from ai_trading_system.research_bench.settings import (
    EpisodeSpec,
    LoadedSettings,
    Window,
    load_settings,
    resolve_window,
)
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
from ai_trading_system.research_probes import gate_power, plot_run, policy_table

RUN_ID = "TRADING-2565-51"
OWNER_DECISION = "owner_decision:TRADING-2565:2026-10-22:research_loop_p4"
DIRECTION = "owner_decision:TRADING-2565:2026-10-22:period4_two_questions"
# Baker 2026-10-22 选「按实际持有扣」，第 1 行读 r′，尺子、门和第 2 到 4 行照旧读 r。
HELD_DEDUCTION = "owner_decision:TRADING-2565:2026-10-22:hb_row1_held_deduction"
# Baker 选「记 0，不数」：段内持国债 0 月的段，第 1 行的 r′ 记 0，不算正也不算负。
CASH_ZERO = "owner_decision:TRADING-2565:2026-10-22:hb_row1_cash_zero"
PLAN_ID = "T0008"
PRIMARY = "qqq60_t10s40"
RESERVE = "treasury_10y_shiller"
# 章程第 3.2 节已登记的 H-B 配置，不是生产配置或接受门槛。
WEIGHT = 0.6
MONTHS = 6
STATE_COLUMN = "source:aaa_monthly:value"
RF_COLUMN = "cash_rate_known"
MARKETS: tuple[trial.Market, ...] = ("us_market", "qqq")
MARKET_TEXT = policy_table.MARKET_TEXT
COST_BPS = policy_table.COST_BPS
REBALANCE = Rebalance(schedule="month_end", band=0.0)
RULES = {"t0008": "T0008 主配置（一直持国债）", "h_b": "H-B 条件化储备"}
RUN_JSON = "h_b.json"
RUN_MD = "h_b.md"
FIGURES = "figures"
# 章程第 3.2 节判法表的范围和机械判法；本探针只印，不据此晋级。
ROW1_SPAN = ("1966-01", "1990-12")
ROW1_POSITIVE = (3, 5)
ROW1_FLOOR = -0.05
ROW2_SPANS = (("1929-01", "1962-12"), ("1998-01", "2010-12"))
ROW2_TOLERANCE = 0.05
ROW3_METRICS = ("portfolio.segment_count", "portfolio.median_ratio", "portfolio.positive_share")
ROW4_FLOOR = -0.010
# 已发布报告 trial-r0055、r0056 的打印精度：半个末位加浮点余量。
FOUR_PLACES = 0.00005 + 1e-12
THREE_PLACES = 0.0005 + 1e-12
SAME_RUN = 1e-12  # 同一次运行内两条计算路径的浮点误差容限，不是研究接受门槛。
T0008_RECORD: dict[str, Any] = {
    "us_market": {
        "comparison": 0.0075,
        "cost_25": 0.0075,
        "median_ratio": 0.0017,
        "positive_share": 0.5,
        "rebalance_return": 0.0002,
        "segment_count": 24,
        "excluded": ["decline_198011"],
        "ratios": {
            "decline_192908": -0.029,
            "decline_193208": 0.036,
            "decline_193308": 0.015,
            "decline_193401": 0.057,
            "decline_193702": 0.007,
            "decline_193812": 0.022,
            "decline_193909": -0.054,
            "decline_194605": -0.026,
            "decline_194805": 0.043,
            "decline_195707": 0.236,
            "decline_196112": 0.018,
            "decline_196601": -0.284,
            "decline_196811": -0.353,
            "decline_197212": -0.188,
            "decline_197506": -0.222,
            "decline_197808": -0.103,
            "decline_198001": -0.672,
            "decline_198011": None,
            "decline_198306": -1.720,
            "decline_198708": -0.004,
            "decline_199005": -0.015,
            "decline_199806": 0.035,
            "decline_200003": 0.188,
            "decline_200710": 0.100,
            "decline_201004": 0.283,
        },
    },
    "qqq": {
        "comparison": 0.0134,
        "median_ratio": 0.0546,
        "rebalance_return": 0.0012,
        "segment_count": 6,
        "excluded": [],
        "ratios": {
            "decline_200003": 0.084,
            "decline_200211": 0.025,
            "decline_200412": -0.005,
            "decline_200601": -0.295,
            "decline_200710": 0.157,
            "decline_201004": 0.303,
        },
    },
}
Progress = Callable[[str, str, Mapping[str, Any]], None]
app = typer.Typer(help="H-B 条件化储备：只核对和印开发窗口读数，不判、不写研究账本。")


def state_params(column: str = STATE_COLUMN, months: int = MONTHS) -> dict[str, Any]:
    return {"column": column, "months": months}


def reserve_state(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    if set(params) != {"column", "months"}:
        raise ValueError("params need column and months")
    months = params["months"]
    if isinstance(months, bool) or not isinstance(months, Integral) or months < 1:
        raise ValueError("months must be a positive integer")
    column = params["column"]
    if column not in inputs:
        raise ValueError(f"inputs have no column {column}")
    index = pd.DatetimeIndex(inputs.index)
    ends = np.flatnonzero(schedule_mask(index, "month_end"))
    values = inputs[column].iloc[ends].to_numpy(dtype=float)
    states = np.full(len(ends), np.nan)
    for k in range(int(months), len(ends)):
        change = values[k] - values[k - int(months)]
        if math.isfinite(change):
            states[k] = float(change < 0)
    # Index positions propagate even a missing month-end state; ffill would conceal that gap.
    latest = np.searchsorted(ends, np.arange(len(index)), side="right") - 1
    result = np.full(len(index), np.nan)
    valid = latest >= 0
    result[valid] = states[latest[valid]]
    return pd.Series(result, index=inputs.index, name="reserve_state")


def simulate_reserve(
    returns: pd.DataFrame,
    treasury: pd.Series,
    targets: pd.Series,
    state: pd.Series,
    *,
    start: date,
    end: date | None,
    rebalance: Rebalance,
    cost_bps: float,
    delay_days: int = 0,
) -> Simulation:
    """Scalar bench simulate, with only a separate Treasury slot and reserve-state trades added."""
    if list(returns.columns) != list(ASSETS):
        raise ValueError(f"returns columns must be {list(ASSETS)}")
    for name, series in (("targets", targets), ("state", state), ("treasury", treasury)):
        if not series.index.equals(returns.index):
            raise ValueError(f"{name} must be indexed by the same dates as the returns")
    if delay_days < 0:
        raise ValueError("delay_days must not be negative")
    if cost_bps < 0:
        raise ValueError("cost_bps must not be negative")
    # 国债紧跟现金：一种状态下两格必有一格为 0，加总次序和评估台一样，
    # 储备不变时结果逐位相同（包括杠杆、漂移、换手和成本）。
    frame = returns.copy()
    frame.insert(2, "treasury", treasury.to_numpy(dtype=float))
    factors = [*_FACTORS[:2], 0.0, *_FACTORS[2:]]
    index = pd.DatetimeIndex(returns.index)
    first = int(index.searchsorted(pd.Timestamp(start)))
    last = (
        len(index) - 1
        if end is None
        else int(index.searchsorted(pd.Timestamp(end), side="right")) - 1
    )
    if first >= len(index) or last <= first:
        raise ValueError(f"no return dates between {start} and {end}")
    window = frame.iloc[first + 1 : last + 1]
    if window.isna().to_numpy().any():
        bad = window[window.isna().any(axis=1)]
        column = bad.columns[bad.isna().iloc[0].to_numpy()][0]
        raise ValueError(f"{column} has no return on {bad.index[0].date()} inside the window")

    lag = 1 + delay_days
    scheduled = schedule_mask(index, rebalance.schedule)
    values = targets.to_numpy(dtype=float)
    states = state.to_numpy(dtype=float)

    def target_at(p: int) -> list[float]:
        if states[p] not in (0.0, 1.0):
            raise ValueError(f"no reserve state on {index[p].date()}")
        qqq, cash, lev2, lev3 = _checked_weights(values[p], index[p])
        return [qqq, 0.0, cash, lev2, lev3] if states[p] == 1 else [qqq, cash, 0.0, lev2, lev3]

    opening = next((p for p in range(first - lag, -1, -1) if scheduled[p]), None)
    if opening is None or not math.isfinite(values[opening]):
        raise ValueError(f"no target is executable by {index[first].date()}; extend the lookback")
    weights = target_at(opening)
    held_state = float(states[opening])
    rate = cost_bps / 10_000.0
    matrix = frame.to_numpy(dtype=float)

    rows: list[tuple[float, float, float, float, float, float]] = []
    for q in range(first + 1, last + 1):
        day = matrix[q]
        held = sum(w * f for w, f in zip(weights, factors, strict=True))
        gross = sum(w * r for w, r in zip(weights, day, strict=True))
        if 1.0 + gross <= 0.0:
            raise ValueError(f"portfolio lost everything on {index[q].date()}")
        drifted = [w * (1.0 + r) / (1.0 + gross) for w, r in zip(weights, day, strict=True)]
        turnover = 0.0
        weights = drifted
        day_state = held_state
        decided = q - lag
        if decided >= 0 and scheduled[decided]:
            target = target_at(decided)
            target_exposure = sum(w * f for w, f in zip(target, factors, strict=True))
            current = sum(w * f for w, f in zip(drifted, factors, strict=True))
            if (
                _needs_trade(target_exposure, current, rebalance.band)
                or states[decided] != held_state
            ):
                turnover = sum(abs(t - d) for t, d in zip(target, drifted, strict=True)) / 2.0
                weights = target
                held_state = float(states[decided])
        cost = turnover * rate
        net = (1.0 + gross) * (1.0 - cost) - 1.0
        rows.append((net, gross, held, turnover, cost, day_state))

    daily = pd.DataFrame(
        rows,
        index=index[first + 1 : last + 1],
        columns=["return", "gross_return", "exposure", "turnover", "cost", "treasury"],
    )
    return Simulation(daily=daily, start=index[first])


def reserve_leg(sim: Simulation, treasury: pd.Series, cash: pd.Series) -> pd.Series:
    dates = sim.daily.index
    return pd.Series(
        np.where(sim.daily["treasury"] == 1, treasury.reindex(dates), cash.reindex(dates)),
        index=dates,
        name="reserve",
    )


def held_deduction(
    sim: Simulation,
    reserve: pd.Series,
    cash: pd.Series,
    rows: Sequence[Mapping[str, Any]],
    segments: Sequence[EpisodeSpec],
    fallback: float,
    days_per_year: int,
) -> dict[str, Any]:
    dates = pd.DatetimeIndex(sim.daily.index)
    held = (sim.daily["treasury"] == 1).to_numpy()
    quiet = dates[quiet_dates(dates, segments) & held]
    excess = quiet_excess(reserve, cash, quiet, days_per_year) if len(quiet) else fallback
    months = dates.to_period("M")
    found = []
    for row in rows:
        inside = (months >= pd.Period(row["start"], "M")) & (months <= pd.Period(row["end"], "M"))
        held_days = int((inside & held).sum())
        deduction = (1 - WEIGHT) * excess * held_days / days_per_year
        cushion = row["difference"] - deduction
        found.append(
            {
                "id": row["id"],
                "held_days": held_days,
                "days": int(inside.sum()),
                "deduction": deduction,
                "cushion": cushion,
                "ratio": cushion / abs(row["benchmark_return"]) if row["counted"] else None,
            }
        )
    return {"excess": excess, "days": len(quiet), "fallback": len(quiet) == 0, "segments": found}


def same_days(left: Simulation, right: Simulation) -> bool:
    columns = ["return", "gross_return", "exposure", "turnover", "cost"]
    return bool(left.daily[columns].equals(right.daily[columns]))


def same_numbers(left: Any, right: Any, tolerance: float = SAME_RUN) -> bool:
    if isinstance(left, Mapping) or isinstance(right, Mapping):
        return (
            isinstance(left, Mapping)
            and isinstance(right, Mapping)
            and left.keys() == right.keys()
            and all(same_numbers(left[k], right[k], tolerance) for k in left)
        )
    if isinstance(left, list | tuple) or isinstance(right, list | tuple):
        return (
            isinstance(left, list | tuple)
            and isinstance(right, list | tuple)
            and len(left) == len(right)
            and all(same_numbers(a, b, tolerance) for a, b in zip(left, right, strict=True))
        )
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left == right
    if isinstance(left, Real) and isinstance(right, Real):
        a, b = float(left), float(right)
        return (math.isnan(a) and math.isnan(b)) or abs(a - b) <= tolerance
    return bool(left == right)


@dataclass(frozen=True)
class ReservePath:
    state: pd.Series
    sim: Simulation
    benchmark: Simulation
    reserve: pd.Series
    values: dict[str, Any]


def market_window(loaded: LoadedSettings, gate: trial.TrialGate, market: trial.Market) -> Window:
    return resolve_window(loaded, gate.markets[market].question_type_for(RESERVE), "development")


def declines(
    gate: trial.TrialGate, market: trial.Market, window: Window
) -> tuple[EpisodeSpec, ...]:
    found = gate.markets[market].decline_list(window.window_id)
    if not found:
        raise ValueError(f"{window.window_id}: the gate lists no declines")
    return found


def _rules(gate: trial.TrialGate, market: trial.Market, window: Window) -> tuple[Rule, ...]:
    found = gate.markets[market].portfolio_rules_for(window.window_id)
    if found is None:
        raise ValueError(f"{window.window_id}: the gate names no portfolio rules")
    return found


def reserve_path(
    data: BenchData,
    state: pd.Series,
    window: Window,
    loaded: LoadedSettings,
    segments: Sequence[EpisodeSpec],
    *,
    fallback: float,
) -> ReservePath:
    targets = fixed_exposure(data.inputs, {"exposure": WEIGHT})
    treasury = data.reserves[RESERVE].reindex(data.returns.index)
    cash = data.returns["cash"]

    def simulate_pair(cost: float) -> tuple[Simulation, Simulation]:
        own = simulate_reserve(
            data.returns,
            treasury,
            targets,
            state,
            start=window.start,
            end=window.end,
            rebalance=REBALANCE,
            cost_bps=cost,
        )
        matched = simulate(
            data.returns,
            constant_targets(data.returns.index, float(own.exposure.mean())),
            start=window.start,
            end=window.end,
            rebalance=REBALANCE,
            cost_bps=cost,
        )
        return own, matched

    sim, _ = simulate_pair(loaded.cost_bps)
    expensive, expensive_base = simulate_pair(COST_BPS)
    benchmark = benchmark_simulations(
        data,
        window,
        average_exposure=float(sim.exposure.mean()),
        rebalance=REBALANCE,
        cost_bps=loaded.cost_bps,
        leveraged=False,
    )[PRIMARY_BENCHMARK]
    own, base = summarize(sim, cash, loaded), summarize(benchmark, cash, loaded)
    reserve = reserve_leg(sim, treasury, cash)
    values = {
        "strategy": own,
        "benchmark": base,
        "comparison": compare(own, base, loaded),
        "cost_25": compare(
            summarize(expensive, cash, loaded), summarize(expensive_base, cash, loaded), loaded
        ),
        "portfolio": portfolio_section(
            sim,
            benchmark,
            own,
            base,
            data.returns,
            reserve,
            WEIGHT,
            segments,
            loaded.settings.trading_days_per_year,
        ),
    }
    values["held"] = held_deduction(
        sim,
        reserve,
        cash,
        values["portfolio"]["segments"],
        segments,
        fallback,
        loaded.settings.trading_days_per_year,
    )
    for held, row in zip(values["held"]["segments"], values["portfolio"]["segments"], strict=True):
        held["held_months"] = months_held(state, row["start"], row["end"])[0]
    return ReservePath(state, sim, benchmark, reserve, values)


def t0008_spec(gate: trial.TrialGate, plan: trial.TrialPlan, market: trial.Market) -> RunSpec:
    config = plan.config(PRIMARY)
    return RunSpec(
        question_type=gate.markets[market].question_type_for(config.reserve),
        strategy_id=config.id,
        params=config.params,
        window_role="development",
        rebalance=config.rebalance.to_rebalance(),
        delay_days=config.delay_days,
        leveraged_benchmarks=config.leveraged_benchmarks,
        reserve=config.reserve,
    )


def t0008_run(
    data: BenchData,
    gate: trial.TrialGate,
    plan: trial.TrialPlan,
    market: trial.Market,
    loaded: LoadedSettings,
    window: Window,
) -> dict[str, Any]:
    entry = gate.markets[market]
    slow, bull = entry.segment_lists(window.window_id)
    result = run_strategy(
        data,
        fixed_exposure,
        t0008_spec(gate, plan, market),
        loaded,
        falsification_confidence=gate.confidence,
        episodes=entry.event_episodes,
        slow_bears=slow,
        long_bulls=bull,
        declines=declines(gate, market, window),
        portfolio_weight=float(plan.config(PRIMARY).params["exposure"]),
    )
    if result.falsification is None or result.portfolio is None:
        raise ValueError("T0008 has no falsification or portfolio section")
    return {
        "comparison": result.comparison,
        "cost_25": result.falsification["cost_25"],
        "portfolio": result.portfolio,
    }


def record_differences(
    found: Mapping[str, Any], market: str, records: Mapping[str, Any] | None = None
) -> list[str]:
    record = (T0008_RECORD if records is None else records)[market]
    portfolio = found["portfolio"]
    differences = []
    for key in ("comparison", "cost_25", "median_ratio", "positive_share", "rebalance_return"):
        if key not in record:
            continue
        value = (
            found[key]["geometric_annual_return"]
            if key in ("comparison", "cost_25")
            else portfolio[key]
        )
        if not same_numbers(value, record[key], FOUR_PLACES):
            differences.append(f"{key} {value:.4f}（记录 {record[key]:.4f}）")
    if portfolio["segment_count"] != record["segment_count"]:
        differences.append(
            f"计入段数 {portfolio['segment_count']}（记录 {record['segment_count']}）"
        )
    if list(portfolio["excluded"]) != record["excluded"]:
        differences.append(f"不计入的段 {list(portfolio['excluded'])}（记录 {record['excluded']}）")
    ratios = {row["id"]: row["ratio"] for row in portfolio["segments"]}
    if ratios.keys() != record["ratios"].keys():
        return [*differences, "下跌段和记录不同"]
    for sid, expected in record["ratios"].items():
        value = ratios[sid]
        if not same_numbers(value, expected, THREE_PLACES):
            text = "—" if value is None else f"{value:+.3f}"
            differences.append(f"{sid} r {text}（记录 {expected}）")
    return differences


def state_checks(
    data: BenchData, window: Window, loaded: LoadedSettings, column: str = STATE_COLUMN
) -> list[str]:
    inputs = data.inputs
    index = pd.DatetimeIndex(inputs.index)
    start = pd.Timestamp(window.start)
    end = index[-1] if window.end is None else pd.Timestamp(window.end)
    params = state_params(column)
    problems = check_no_lookahead(
        reserve_state,
        inputs,
        params,
        cut_dates(index, start, end, loaded.settings.lookahead_cut_dates),
    )
    problems.extend(check_parameters_consumed(reserve_state, inputs, params, start, end)[0])
    state = reserve_state(inputs, params)
    ends = index[np.asarray(schedule_mask(index, "month_end"), dtype=bool)]
    missing = int(state.reindex(ends[(ends >= start) & (ends <= end)]).isna().sum())
    if missing:
        problems.append(f"{missing} 个月底没有状态")
    before = ends[ends < start]
    if not len(before) or pd.isna(state.loc[before[-1]]):
        problems.append("窗口开头没有可用的状态")
    return problems


def equivalence(
    data: BenchData,
    window: Window,
    loaded: LoadedSettings,
    segments: Sequence[EpisodeSpec],
    t0008: Mapping[str, Any],
) -> dict[str, bool]:
    index = data.returns.index
    treasury = data.reserves[RESERVE].reindex(index)
    targets = fixed_exposure(data.inputs, {"exposure": WEIGHT})
    result = {}
    for name, value, returns in (
        ("treasury", 1.0, strategy_returns(data, RESERVE)),
        ("cash", 0.0, data.returns),
    ):
        for cost in (loaded.cost_bps, COST_BPS):
            ours = simulate_reserve(
                data.returns,
                treasury,
                targets,
                pd.Series(value, index=index),
                start=window.start,
                end=window.end,
                rebalance=REBALANCE,
                cost_bps=cost,
            )
            bench = simulate(
                returns,
                targets,
                start=window.start,
                end=window.end,
                rebalance=REBALANCE,
                cost_bps=cost,
            )
            result[f"{name}_{cost:g}bps"] = same_days(ours, bench)
    path = reserve_path(
        data,
        pd.Series(1.0, index=index),
        window,
        loaded,
        segments,
        fallback=t0008["portfolio"]["quiet_excess"],
    )
    result["t0008_path"] = same_numbers(
        {k: path.values[k] for k in ("comparison", "cost_25", "portfolio")}, t0008
    )
    return result


def _in_span(month: str, span: tuple[str, str]) -> bool:
    return span[0] <= month <= span[1]


def months_held(state: pd.Series, start: str, end: str) -> tuple[int, int]:
    index = pd.DatetimeIndex(state.index)
    ends = np.asarray(schedule_mask(index, "month_end"), dtype=bool)
    monthly = pd.Series(
        state.iloc[np.flatnonzero(ends)].to_numpy(), index=index[ends].to_period("M").shift(1)
    )
    months = pd.period_range(start, end, freq="M")
    return int((monthly.reindex(months) == 1).sum()), len(months)


def switches(sim: Simulation) -> int:
    return int((sim.daily["treasury"].diff().dropna() != 0).sum())


def segment_table(path: ReservePath, base: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    base_rows = {} if base is None else {r["id"]: r for r in base["portfolio"]["segments"]}
    base_held = {} if base is None else {r["id"]: r for r in base["held"]["segments"]}
    held = {r["id"]: r for r in path.values["held"]["segments"]}
    rows = []
    for row in path.values["portfolio"]["segments"]:
        sid = row["id"]
        months, total = months_held(path.state, row["start"], row["end"])
        h = held[sid]
        rows.append(
            {
                **row,
                "deduction": row["difference"] - row["cushion"],
                "held_months": months,
                "months_counted": total,
                "base_ratio": base_rows.get(sid, {}).get("ratio"),
                "held_days": h["held_days"],
                "days": h["days"],
                "held_deduction": h["deduction"],
                "held_cushion": h["cushion"],
                "held_ratio": h["ratio"],
                "base_held_ratio": base_held.get(sid, {}).get("ratio"),
            }
        )
    return rows


def row_readings(
    us: Mapping[str, Any], base: Mapping[str, Any], rules: tuple[Rule, ...]
) -> list[dict[str, Any]]:
    """段内持国债 0 月的段 r′ 记 0（CASH_ZERO）。"""
    rows = us["portfolio"]["segments"]
    base_rows = {r["id"]: r for r in base["portfolio"]["segments"]}
    held = {r["id"]: r for r in us["held"]["segments"]}
    base_held = {r["id"]: r for r in base["held"]["segments"]}
    selected = [r for r in rows if r["counted"] and _in_span(r["start"], ROW1_SPAN)]
    base_selected = [
        r for r in base_rows.values() if r["counted"] and _in_span(r["start"], ROW1_SPAN)
    ]
    cash_zero = [r["id"] for r in selected if held[r["id"]]["held_months"] == 0]
    ratios = {
        r["id"]: 0.0 if held[r["id"]]["held_months"] == 0 else held[r["id"]]["ratio"]
        for r in selected
    }
    positive = sum(r > 0 for r in ratios.values())
    below = [sid for sid, r in ratios.items() if r < ROW1_FLOOR]
    pairs, skipped = [], []
    for row in rows:
        if not any(_in_span(row["start"], span) for span in ROW2_SPANS):
            continue
        other = base_rows.get(row["id"])
        if not row["counted"] or other is None or not other["counted"]:
            skipped.append(row["id"])
        else:
            pairs.append(
                {
                    "id": row["id"],
                    "ratio": row["ratio"],
                    "base_ratio": other["ratio"],
                    "change": row["ratio"] - other["ratio"],
                }
            )
    outside = [p["id"] for p in pairs if abs(p["change"]) > ROW2_TOLERANCE]
    gate = evaluate_rules(rules, {k: us[k] for k in ("comparison", "cost_25", "portfolio")})
    p2 = [r for r in gate if r["metric"] in ROW3_METRICS]
    geometric = us["comparison"]["geometric_annual_return"]
    return [
        {
            "row": 1,
            "reads": "r′",
            "decision": HELD_DEDUCTION,
            "excess": us["held"]["excess"],
            "excess_days": us["held"]["days"],
            "fallback": us["held"]["fallback"],
            "segments": list(ratios),
            "ratios": ratios,
            "cash_zero": cash_zero,
            "count": len(selected),
            "base_count": len(base_selected),
            "base_positive": sum(
                (0.0 if base_held[r["id"]]["held_months"] == 0 else base_held[r["id"]]["ratio"]) > 0
                for r in base_selected
            ),
            "positive": positive,
            "min_ratio": min(ratios.values()) if ratios else None,
            "below_floor": below,
            "holds": bool(selected)
            and ROW1_POSITIVE[0] <= positive <= ROW1_POSITIVE[1]
            and not below,
            "closes_p1": positive < ROW1_POSITIVE[0] or bool(below),
            "six_or_more": positive > ROW1_POSITIVE[1],
        },
        {
            "row": 2,
            "pairs": pairs,
            "skipped": skipped,
            "outside": outside,
            "holds": bool(pairs) and not outside,
        },
        {
            "row": 3,
            "gate": gate,
            "rules": p2,
            "holds": {r["metric"] for r in p2} == set(ROW3_METRICS) and all(r["holds"] for r in p2),
            "gate_holds": all(r["holds"] for r in gate),
        },
        {
            "row": 4,
            "geometric_difference": geometric,
            "cost_25": us["cost_25"]["geometric_annual_return"],
            "holds": geometric >= ROW4_FLOOR,
        },
    ]


def first_four(readings: Sequence[Mapping[str, Any]]) -> bool | None:
    return all(r["holds"] for r in readings) if len(readings) == 4 else None


def draw(
    market: trial.Market,
    paths: Mapping[str, ReservePath],
    tables: Mapping[str, Sequence[Mapping[str, Any]]],
    closeups: Sequence[str],
    data: BenchData,
    window: Window,
    provenance: Mapping[str, Any],
    out_dir: Path,
) -> list[dict[str, Any]]:
    bands = [
        plot_run.SegmentBand(r["id"], "decline", r["start"], r["end"])
        for r in next(iter(tables.values()))
    ]
    rules = []
    readings: dict[str, dict[str, plot_run.SegmentReading]] = {}
    roles: tuple[plot_run.Role, ...] = ("base", "compared")
    for role, (name, path) in zip(roles, paths.items(), strict=True):
        rules.append(
            plot_run.RuleSeries(
                RULES[name],
                role,
                path.sim.exposure,
                path.sim.returns,
                pd.DataFrame({"reserve": path.sim.daily["treasury"]}),
                path.benchmark.returns,
            )
        )
        for row in tables[name]:
            readings.setdefault(row["id"], {})[role] = plot_run.SegmentReading(
                row["difference"],
                ratio=row["held_ratio"],
                held_months=row["held_months"],
                months=row["months_counted"],
                ratio_name="r′",
            )
    footer = [
        f"{RUN_ID} run；窗口 {window.window_id} {window.start}..{window.end or ''}；"
        f"提交 {provenance['commit']}；快照 "
        + "、".join(
            str(data.source[k]) for k in ("snapshot_id", "signal_snapshot_id") if data.source.get(k)
        ),
        "竖线：H-B 每次换储备一根，落在 t+2 生效日，朝下转国债、朝上转现金，灰 T0008、蓝 H-B。",
        f"r′ 按实际持有扣（{HELD_DEDUCTION}）：平时超额只按段内持国债的交易日扣；"
        "尺子原样的 r 在报告的段表里。",
    ]
    spec = plot_run.FigureSpec(
        "run",
        market,
        f"{MARKET_TEXT[market]}，{RULES['t0008']} 对 {RULES['h_b']}",
        window.window_id,
        data.returns["qqq"].reindex(next(iter(paths.values())).sim.daily.index),
        bands,
        rules,
        readings,
        footer,
        closeups=closeups,
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


def _summary(path: ReservePath, loaded: LoadedSettings) -> dict[str, Any]:
    dates = pd.DatetimeIndex(path.sim.daily.index)
    held, months = months_held(
        path.state, str(dates[0].to_period("M")), str(dates[-1].to_period("M"))
    )
    count = switches(path.sim)
    return {
        **{k: path.values[k] for k in ("strategy", "comparison", "cost_25", "portfolio")},
        "held": {k: path.values["held"][k] for k in ("excess", "days", "fallback")},
        "held_months": held,
        "months": months,
        "switches": count,
        "switches_per_year": count / (len(dates) / loaded.settings.trading_days_per_year),
    }


def run_probe(
    datasets: Mapping[str, BenchData],
    loaded: LoadedSettings,
    gate: trial.TrialGate,
    plan: trial.TrialPlan,
    root_dir: Path,
    *,
    provenance: Mapping[str, Any],
    out_dir: Path | None,
    progress: Progress | None = None,
    records: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    windows = {m: market_window(loaded, gate, m) for m in MARKETS}
    window_rows = {}
    for market, window in windows.items():
        index = pd.DatetimeIndex(datasets[market].returns.index)
        first = int(index.searchsorted(pd.Timestamp(window.start)))
        last = (
            len(index)
            if window.end is None
            else int(index.searchsorted(pd.Timestamp(window.end), side="right"))
        )
        dates = index[first + 1 : last]
        window_rows[market] = {
            "window_id": window.window_id,
            "start": window.start.isoformat(),
            "end": None if window.end is None else window.end.isoformat(),
            "evaluated_start": dates[0].date().isoformat() if len(dates) else None,
            "evaluated_end": dates[-1].date().isoformat() if len(dates) else None,
        }
    result: dict[str, Any] = {
        "run_id": RUN_ID,
        "step": "check" if out_dir is None else "run",
        "owner_decision": OWNER_DECISION,
        "direction": DIRECTION,
        "plan": PLAN_ID,
        "gate_sha256": ledger.chain_digest(
            (root_dir / trial.TRIALS_DIR / "prereg.yaml").read_bytes()
        ),
        "sources": {m: dict(datasets[m].source) for m in MARKETS},
        "settings_sha256": loaded.sha256,
        "windows": window_rows,
        "provenance": dict(provenance),
        "production_effect": "none",
        "broker_action": "none",
        "complete": False,
        "t0008": {},
        "stop": {},
        "checks": {},
        "equivalence": {},
    }
    for market in MARKETS:
        data, window = datasets[market], windows[market]
        found = t0008_run(data, gate, plan, market, loaded, window)
        result["t0008"][market] = found
        result["stop"][market] = record_differences(found, market, records)
        result["checks"][market] = state_checks(data, window, loaded)
        result["equivalence"][market] = equivalence(
            data, window, loaded, declines(gate, market, window), found
        )
        if progress is not None:
            progress(market, "t0008", found)
    result["stopped"] = any(result["stop"].values())
    ready = (
        not result["stopped"]
        and not any(result["checks"].values())
        and all(all(eq.values()) for eq in result["equivalence"].values())
    )
    if not ready or out_dir is None:
        result["complete"] = ready
        return dict(gate_power._clean(result))
    for key in ("h_b", "segments", "figures"):
        result[key] = {}
    us_values: dict[str, dict[str, Any]] = {}
    for market in MARKETS:
        data, window = datasets[market], windows[market]
        segments = declines(gate, market, window)
        fallback = result["t0008"][market]["portfolio"]["quiet_excess"]
        states = {
            "t0008": pd.Series(1.0, index=data.inputs.index),
            "h_b": reserve_state(data.inputs, state_params()),
        }
        paths = {
            name: reserve_path(data, state, window, loaded, segments, fallback=fallback)
            for name, state in states.items()
        }
        base = paths["t0008"].values
        tables = {name: segment_table(path, base) for name, path in paths.items()}
        result["h_b"][market] = {name: _summary(path, loaded) for name, path in paths.items()}
        result["segments"][market] = tables["h_b"]
        if progress is not None:
            progress(market, "h_b", result["h_b"][market]["h_b"])
        if market == "us_market":
            us_values = {name: path.values for name, path in paths.items()}
            rf = reserve_path(
                data,
                reserve_state(data.inputs, state_params(RF_COLUMN)),
                window,
                loaded,
                segments,
                fallback=fallback,
            )
            result["rf"] = {
                "summary": _summary(rf, loaded),
                "segments": segment_table(rf, base),
                "readings": row_readings(rf.values, base, _rules(gate, market, window)),
            }
            closeups = [
                r["id"] for r in tables["h_b"] if r["counted"] and _in_span(r["start"], ROW1_SPAN)
            ]
        else:
            result["direction_only"] = {
                name: evaluate_rules(_rules(gate, market, window), path.values)
                for name, path in paths.items()
            }
            closeups = [s.id for s in segments]
        result["figures"][market] = draw(
            market, paths, tables, closeups, data, window, provenance, out_dir
        )
    result["readings"] = row_readings(
        us_values["h_b"], us_values["t0008"], _rules(gate, "us_market", windows["us_market"])
    )
    result["first_four"] = first_four(result["readings"])
    result["complete"] = True
    return dict(gate_power._clean(result))


def _number(value: Any) -> str:
    return "—" if value is None else f"{value:+.3f}"


def _points(value: Any) -> str:
    return "—" if value is None else f"{value * 100:+.2f}"


def _holds(value: Any) -> str:
    return "成立" if value else "不成立"


def _excess(value: float, days: int, fallback: bool) -> str:
    return f"{value * 100:+.2f} 点/年" + (
        "（段外没有持国债的日子，退回同一次运行 T0008 主配置的平时超额）"
        if fallback
        else f"，{days} 个交易日"
    )


def _table(lines: list[str], headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> None:
    lines.extend(["", policy_table._table_row(headers), "|" + "---|" * len(headers)])
    lines.extend(policy_table._table_row(row) for row in rows)


def _segment_report(lines: list[str], rows: Sequence[Mapping[str, Any]]) -> None:
    lines.append("\n下跌段（每段 ΔD、尺子扣的和按持有扣的平时超额、C、r、r′ 并排；单位：点）")
    _table(
        lines,
        [
            "段",
            "月份",
            "月数",
            "持国债（月）",
            "持国债（交易日）",
            "B 段内",
            "A 段内",
            "ΔD",
            "尺子扣的",
            "按持有扣的",
            "C",
            "r",
            "r′",
            "T0008 r",
            "T0008 r′",
        ],
        [
            [
                r["id"],
                f"{r['start']}..{r['end']}",
                r["months_counted"],
                r["held_months"],
                f"{r['held_days']}/{r['days']}",
                *[
                    _points(r[k])
                    for k in (
                        "benchmark_return",
                        "strategy_return",
                        "difference",
                        "deduction",
                        "held_deduction",
                        "cushion",
                    )
                ],
                *[
                    "不计入" if r[k] is None else _number(r[k])
                    for k in ("ratio", "held_ratio", "base_ratio", "base_held_ratio")
                ],
            ]
            for r in rows
        ],
    )


def _rule_text(rule: Mapping[str, Any], *, threshold: bool = False) -> str:
    metric = rule["metric"]
    value = rule["value"] if threshold else rule["observed"]
    names = {
        "comparison.geometric_annual_return": "几何差",
        "cost_25.geometric_annual_return": "25 bps 下几何差",
        "portfolio.rebalance_return": "再平衡项",
        "portfolio.segment_count": "计入段数",
        "portfolio.median_ratio": "r 中位",
        "portfolio.positive_share": "r > 0 占比",
    }
    text = (
        "—"
        if value is None
        else str(int(value))
        if metric == "portfolio.segment_count"
        else _number(value)
        if metric in ROW3_METRICS
        else f"{_points(value)} 点"
    )
    return (
        f"{names.get(metric, metric)} "
        + (f"{rule['op']} " if threshold else "")
        + text
        + ("" if threshold else f" {_holds(rule['holds'])}")
    )


def render(result: Mapping[str, Any]) -> str:
    check = result["step"] == "check"
    lines = [
        f"# {RUN_ID}：H-B 条件化储备（{'只核对' if check else '读数'}）",
        "",
        f"- 依据：{result['owner_decision']}；方向：{result['direction']}。",
        f"- 计划和数据：{result['plan']}；门 sha256 {result['gate_sha256']}；"
        f"设置 sha256 {result['settings_sha256']}。",
        f"- 提交：{result['provenance']['commit']}。",
        f"- 第 1 行读 r′，按实际持有扣（{HELD_DEDUCTION}，Baker 选「按实际持有扣」）；"
        "尺子、门和第 2 到 4 行照旧读 r。",
        f"- 第 1 行里段内持国债 0 月的段 r′ 记 0，不算正也不算负（{CASH_ZERO}，"
        "Baker 选「记 0，不数」）；段表照旧印算出来的 r′。",
        "- 只读开发窗口；不判、不写研究账本、不开题、不改门；"
        "production_effect=none；broker_action=none。",
    ]
    for market in MARKETS:
        w, source = result["windows"][market], result["sources"][market]
        lines.append(
            f"- 窗口（{MARKET_TEXT[market]}）：{w['window_id']}；"
            f"请求 {w['start']}..{w['end'] or ''}；"
            f"实际评估 {w['evaluated_start']}..{w['evaluated_end']}；"
            f"快照 {source.get('snapshot_id')}，信号快照 {source.get('signal_snapshot_id')}；"
            "数据质量：载入入口校验，来源见 JSON sources。"
        )
        for key, title in (
            ("stop", "同一次运行的 T0008 和它的记录不同"),
            ("checks", "开跑前检查没过"),
        ):
            if result[key][market]:
                lines.append(
                    f"**{title}（{MARKET_TEXT[market]}）：**" + "；".join(result[key][market])
                )
        failed = [k for k, v in result["equivalence"][market].items() if not v]
        if failed:
            lines.append(f"**等同核对没过（{MARKET_TEXT[market]}）：**" + "；".join(failed))
    lines.extend(["", "## 核对", ""])
    for market in MARKETS:
        eq = "、".join(
            f"{k} {'是' if v else '否'}" for k, v in result["equivalence"][market].items()
        )
        lines.append(
            f"- {MARKET_TEXT[market]}：T0008 对记录 "
            f"{'不同' if result['stop'][market] else '相同'}；等同核对 {eq}；"
            f"状态检查 {'有问题' if result['checks'][market] else '没有问题'}"
        )
    if check or not result["complete"]:
        lines.extend(
            [
                "",
                "开跑前核对都过了。"
                if check and result["complete"]
                else "没有读完：停下核对或等同核对没过。",
                "",
            ]
        )
        return "\n".join(lines)
    one, two, three, four = result["readings"]
    p2 = {r["metric"]: r["observed"] for r in three["rules"]}
    lines.append("\n## 第 1 到 4 行读数（章程第 3.2 节判法表，机械比较）")
    row1 = (
        f"1966–1990 计入 {one['count']} 段，r′ > 0 的 {one['positive']} 段"
        f"（T0008 同样扣：计入 {one['base_count']} 段、r′ > 0 的 "
        f"{one['base_positive']} 段），最小 r′ {_number(one['min_ratio'])}；r′ < −0.05 的段："
        + ("、".join(one["below_floor"]) or "无")
        + "；持国债 0 月、记 0 的段："
        + ("、".join(one["cash_zero"]) or "无")
    )
    _table(
        lines,
        ["行", "读数", "门槛", "按判法"],
        [
            [
                1,
                row1,
                "r′ > 0 的段 3 到 5，且没有一段 r′ < −0.05（持国债 0 月的段记 0）",
                _holds(one["holds"]),
            ],
            [
                2,
                f"判 {len(two['pairs'])} 段；超出的段：{'、'.join(two['outside']) or '无'}；"
                f"不计入不判的段：{'、'.join(two['skipped']) or '无'}",
                "1929–1962、1998–2010 成对 r 的差绝对值 ≤ 0.05",
                _holds(two["holds"]),
            ],
            [
                3,
                f"计入段数 {int(p2['portfolio.segment_count'])}；"
                f"r 中位 {_number(p2['portfolio.median_ratio'])}；"
                f"r > 0 占比 {_number(p2['portfolio.positive_share'])}",
                "；".join(_rule_text(r, threshold=True) for r in three["rules"]),
                _holds(three["holds"]),
            ],
            [
                4,
                f"几何差 {_points(four['geometric_difference'])} 点；"
                f"25 bps 下 {_points(four['cost_25'])} 点",
                "几何差 ≥ −1.00 点",
                _holds(four["holds"]),
            ],
        ],
    )
    lines.extend(
        [
            "",
            "- 关 P1 的条件：≤ 2 段 r′ 为正，或有 r′ < −0.05 的段；"
            f"本次{'触及' if one['closes_p1'] else '未触及'}。"
            f"六段或更多为正：{'是' if one['six_or_more'] else '否'}；只印，不判。",
            "- 第 1 行的 e_T（国债对现金的平时超额，只用段外且 H-B 持国债的交易日）："
            + _excess(one["excess"], one["excess_days"], one["fallback"]),
            "- 门的第一段六条："
            + "；".join(_rule_text(r) for r in three["gate"])
            + f"；全部成立：{'是' if three['gate_holds'] else '否'}。",
            f"- 第 1 到 4 行都成立：{'是' if result['first_four'] else '否'}。",
        ]
    )
    _table(
        lines,
        ["段", "H-B r", "T0008 r", "差"],
        [
            [r["id"], _number(r["ratio"]), _number(r["base_ratio"]), _number(r["change"])]
            for r in two["pairs"]
        ],
    )
    for market in MARKETS:
        lines.append(f"\n## {MARKET_TEXT[market]}")
        summaries = result["h_b"][market]
        summary_rows = []
        for name, s in summaries.items():
            own, p = s["strategy"], s["portfolio"]
            summary_rows.append(
                [
                    RULES[name],
                    f"{_points(own['geometric_annual_return'])}%",
                    _points(s["comparison"]["geometric_annual_return"]),
                    _points(s["cost_25"]["geometric_annual_return"]),
                    f"{_points(own['max_drawdown'])}%",
                    f"{own['average_exposure']:.3f}",
                    f"{own['annual_turnover']:.2f}",
                    f"{s['held_months']}/{s['months']}",
                    f"{s['switches']}（每年 {s['switches_per_year']:.2f}）",
                    f"{_points(p['geometric_difference'])} = {_points(p['reserve_excess'])}"
                    f" + {_points(p['rebalance_return'])}",
                    f"{p['positive_count']}/{p['segment_count']}",
                ]
            )
        _table(
            lines,
            [
                "规则",
                "几何年化",
                "对同平均敞口（点）",
                "25 bps 下（点）",
                "最大回撤",
                "平均敞口",
                "年换手",
                "持国债的月",
                "换储备次数",
                "几何差 = 储备超额 + 再平衡",
                "r > 0 / 计入",
            ],
            summary_rows,
        )
        lines.append(
            "\n按实际持有扣的 e_T（段外且持国债的交易日）："
            + "；".join(
                f"{RULES[name]} "
                f"{_excess(s['held']['excess'], s['held']['days'], s['held']['fallback'])}"
                f"（尺子的平时超额 {_points(s['portfolio']['quiet_excess'])} 点/年，"
                f"{s['portfolio']['quiet_days']} 天）"
                for name, s in summaries.items()
            )
        )
        _segment_report(lines, result["segments"][market])
        if market == "qqq":
            lines.append("\nQQQ 第二段（只查方向，只印）")
            for name, rules in result["direction_only"].items():
                lines.append(f"- {RULES[name]}：" + "；".join(_rule_text(r) for r in rules))
    rf = result["rf"]
    rf1 = rf["readings"][0]
    lines.extend(
        [
            "\n## 只印：同一规则改用 Ken French RF 的 6 个月变化当状态（美股大盘，不判）",
            f"- 持国债 {rf['summary']['held_months']}/{rf['summary']['months']} 月，"
            f"换储备 {rf['summary']['switches']} 次；四行："
            + "、".join(_holds(r["holds"]) for r in rf["readings"]),
            f"- 第 1 行 r′：计入 {rf1['count']} 段，r′ > 0 的 {rf1['positive']} 段"
            f"（持国债 0 月、记 0 的 {len(rf1['cash_zero'])} 段），"
            f"最小 {_number(rf1['min_ratio'])}；"
            f"e_T {_excess(rf1['excess'], rf1['excess_days'], rf1['fallback'])}。",
        ]
    )
    _segment_report(lines, rf["segments"])
    lines.append("\n## 图\n")
    for records in result["figures"].values():
        lines.extend(f"- {r['name']}：sha256 {r['sha256']}，{r['bytes']} 字节" for r in records)
    return "\n".join([*lines, ""])


def write_json(result: Mapping[str, Any], path: Path) -> None:
    with path.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")


def cut(data: BenchData) -> BenchData:
    kept = gate_power.cut(data)
    last = kept.returns.index[-1]
    return BenchData(
        kept.returns,
        kept.inputs,
        kept.source,
        {name: s.loc[s.index <= last] for name, s in data.reserves.items()},
    )


def load_inputs(
    snapshot_root: Path,
) -> tuple[dict[str, BenchData], trial.TrialGate, trial.TrialPlan, LoadedSettings, Path]:
    loaded = load_settings()
    root_dir = ledger.research_root(loaded)
    gate, plan = trial.load_trial(root_dir, PLAN_ID)
    problems = trial.gate_problems(gate, loaded)
    if problems:
        raise ValueError("; ".join(problems))
    datasets: dict[str, BenchData] = {}
    for market in MARKETS:
        data = trial.market_data(
            market, gate, plan, snapshot_root=snapshot_root, project_root=PROJECT_ROOT
        )
        problems = trial._data_problems(market, gate, plan, data)
        if RESERVE not in data.reserves:
            problems.append(f"no {RESERVE} reserve")
        if STATE_COLUMN not in data.inputs:
            problems.append(f"no {STATE_COLUMN} input column")
        if problems:
            raise ValueError(f"{market}: " + "; ".join(problems))
        datasets[market] = cut(data)
    return datasets, gate, plan, loaded, root_dir


def _load(
    root: Path,
) -> tuple[dict[str, BenchData], trial.TrialGate, trial.TrialPlan, LoadedSettings, Path]:
    try:
        return load_inputs(root)
    except (OSError, ValueError, KeyError, trial.TrialError) as exc:
        typer.echo(f"conditional_reserve: 数据载入失败：{str(exc).splitlines()[0]}")
        raise typer.Exit(code=1) from None


def _progress(market: str, name: str, found: Mapping[str, Any]) -> None:
    typer.echo(f"conditional_reserve: {market} {name} 算完")


@app.callback()
def main() -> None:
    """只读开发窗口，核对后印 H-B 读数。"""


@app.command("check")
def check_command(
    root: Annotated[
        Path, typer.Option(help="长历史快照根目录。")
    ] = qqq_long_history.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    datasets, gate, plan, loaded, root_dir = _load(root)
    result = run_probe(
        datasets,
        loaded,
        gate,
        plan,
        root_dir,
        provenance=git_provenance(PROJECT_ROOT),
        out_dir=None,
    )
    typer.echo(render(result))
    if not result["complete"]:
        raise typer.Exit(code=1)


@app.command("run")
def run_command(
    out: Annotated[Path, typer.Option(help="结果目录，拒绝覆盖。")],
    root: Annotated[
        Path, typer.Option(help="长历史快照根目录。")
    ] = qqq_long_history.DEFAULT_SNAPSHOT_ROOT,
) -> None:
    if any((out / name).exists() for name in (RUN_JSON, RUN_MD, FIGURES)):
        typer.echo("conditional_reserve: 已有结果，拒绝覆盖。")
        raise typer.Exit(code=1)
    datasets, gate, plan, loaded, root_dir = _load(root)
    result = run_probe(
        datasets,
        loaded,
        gate,
        plan,
        root_dir,
        provenance=git_provenance(PROJECT_ROOT),
        out_dir=out,
        progress=_progress,
    )
    out.mkdir(parents=True, exist_ok=True)
    write_json(result, out / RUN_JSON)
    with (out / RUN_MD).open("x", encoding="utf-8") as handle:
        handle.write(render(result))
    if not result["complete"]:
        typer.echo("conditional_reserve: 没有读完（停下核对或等同核对没过）；见报告头部。")
        raise typer.Exit(code=1)
    for row in result["readings"]:
        typer.echo(f"conditional_reserve: 第 {row['row']} 行 {_holds(row['holds'])}")
    typer.echo(f"conditional_reserve: 完成；结果目录 {out}")


if __name__ == "__main__":
    app()
