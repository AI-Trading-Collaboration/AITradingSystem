"""Invariants of the standard falsification (TRADING-2565 S3c).

A result counts only if it survives these checks, so each check must be able to fail. Each guard
has a negative control: a deliberately fragile or impossible strategy that the check must expose.
Draw counts are cut down for speed; the code paths are the ones validation runs use.

18. The placebo is a same-exposure random timing: every placebo path is a reordering of the targets
    the strategy acted on, and the strategy's own targets elsewhere. A strategy that knows the next
    return ranks above every placebo; a constant exposure ranks exactly in the middle.
19. The bootstrap interval is honest: zero for a strategy equal to its benchmark, above zero for a
    fixed daily edge, around zero for an independent strategy with no edge, wider with more noise,
    wider than a day-by-day bootstrap when the edge comes in long runs, and the same for the same
    seed.
20. Leave-one-out exposes a result carried by one period: an edge earned in a single year, or a
    single fixed episode, vanishes in the worst leave-one-out value; an edge spread over every year
    does not.
21. Reruns use the bench: at the unified cost the rerun is the main comparison; more cost gives a
    lower relative return; an edge that needs next-day execution is gone one day later.
(22, validation and consistency runs are always falsified: ``test_research_ledger_invariants.py``.)
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ai_trading_system.research_bench import (
    BenchData,
    Rebalance,
    RunSpec,
    from_daily_frame,
    load_settings,
    run_strategy,
    simulate,
)
from ai_trading_system.research_bench.falsify import (
    FalsificationInputs,
    bootstrap_interval,
    cost_section,
    delay_section,
    falsify,
    leave_one_out,
    paired_comparison,
    paired_fields,
    placebo_targets,
)
from ai_trading_system.research_bench.prereg import Rule, evaluate_rules
from ai_trading_system.research_bench.settings import resolve_window
from ai_trading_system.research_bench.simulate import constant_targets, schedule_mask

FULL = load_settings()
LOADED = replace(
    FULL,
    settings=FULL.settings.model_copy(
        update={
            "falsification": FULL.settings.falsification.model_copy(
                update={"placebo_draws": 40, "bootstrap_draws": 1000}
            )
        }
    ),
)
DEV = resolve_window(LOADED, "qqq_direction", "development")


def _data(seed: int = 11) -> BenchData:
    index = pd.bdate_range("1997-01-02", "2011-06-30", name="date")
    rng = np.random.default_rng(seed)
    regime = np.where((index.year >= 2000) & (index.year <= 2002), 0.03, 0.015)
    r = rng.normal(0.0004, 1.0, len(index)) * regime
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "cash_return": np.full(len(index), 0.0001),
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    daily.iloc[0, :4] = np.nan
    return from_daily_frame(daily, qqq_series="proxy", source={"snapshot_id": f"synthetic-{seed}"})


DATA = _data()


def _inputs(
    targets: pd.Series, rebalance: Rebalance | None = None, cost: float = 4.0
) -> FalsificationInputs:
    rebalance = rebalance or Rebalance()
    sim = simulate(
        DATA.returns, targets, start=DEV.start, end=DEV.end, rebalance=rebalance, cost_bps=cost
    )
    matched = simulate(
        DATA.returns,
        constant_targets(DATA.returns.index, float(sim.exposure.mean())),
        start=DEV.start,
        end=DEV.end,
        rebalance=rebalance,
        cost_bps=cost,
    )
    return FalsificationInputs(
        DATA.returns, targets, DEV.start, DEV.end, rebalance, 0, cost, sim, matched
    )


def _oracle() -> pd.Series:
    """Knows the return its trade earns (decided at t, earns t+1 to t+2): impossible on purpose."""

    return (DATA.inputs["qqq_return"].shift(-2) > 0).astype(float)


def vol_target(inputs: pd.DataFrame, params: Any) -> pd.Series:
    vol = inputs["qqq_return"].rolling(int(params["window"])).std() * np.sqrt(252)
    return (float(params["target"]) / vol).clip(upper=float(params["cap"]))


# 18 ----------------------------------------------------------------------------------------------


def test_placebos_reorder_the_acted_targets_and_only_the_timing() -> None:
    month_end = Rebalance(schedule="month_end")
    targets = vol_target(DATA.inputs, {"window": 21, "target": 0.2, "cap": 1.5})
    inputs = _inputs(targets, month_end)
    index = pd.DatetimeIndex(targets.index)
    scheduled = np.asarray(schedule_mask(index, "month_end"))
    first = int(index.searchsorted(inputs.strategy.start))
    last = int(index.searchsorted(inputs.strategy.returns.index[-1]))
    lo, hi = first - 1, last
    acted = np.maximum.accumulate(np.where(scheduled, np.arange(len(index)), -1))
    path = targets.to_numpy()[acted[lo:hi]]
    paths = list(placebo_targets(inputs, LOADED))
    for placebo in paths:
        values = placebo.to_numpy()
        assert np.array_equal(np.sort(values[lo:hi]), np.sort(path))
        outside = np.r_[0:lo, hi : len(values)]
        assert np.array_equal(values[outside], targets.to_numpy()[outside], equal_nan=True)
    assert len({placebo.to_numpy()[lo:hi].tobytes() for placebo in paths}) == len(paths)


def test_a_strategy_that_knows_the_next_return_beats_every_placebo() -> None:
    oracle = falsify(_inputs(_oracle()), LOADED, 0.95)
    assert oracle["placebo_rank"]["geometric_annual_return"] == 1.0
    assert oracle["placebo_rank"]["sharpe_vs_cash"] == 1.0
    assert oracle["placebo_rank"]["max_drawdown"] == 1.0
    constant = falsify(_inputs(constant_targets(DATA.returns.index, 0.6)), LOADED, 0.95)
    ranks = {value for value in constant["placebo_rank"].values() if value is not None}
    assert ranks == {0.5}


# 19 ----------------------------------------------------------------------------------------------


def _window_arrays(
    seed: int,
) -> tuple[pd.DatetimeIndex, np.ndarray[Any, Any], np.ndarray[Any, Any]]:
    dates = pd.bdate_range("1999-03-11", "2010-12-31")
    benchmark = np.random.default_rng(seed).normal(0.0003, 0.012, len(dates))
    return dates, benchmark, np.full(len(dates), 0.0001)


def test_the_bootstrap_interval_is_zero_positive_or_around_zero_as_it_should_be() -> None:
    dates, benchmark, cash = _window_arrays(1)
    fields = paired_fields(LOADED.settings)

    low, high = bootstrap_interval(benchmark, benchmark, cash, LOADED, 0.95)
    assert all(low[field] == 0.0 and high[field] == 0.0 for field in fields)

    edge = benchmark + 0.0004
    low, high = bootstrap_interval(edge, benchmark, cash, LOADED, 0.95)
    assert low["geometric_annual_return"] > 0.05 and low["sharpe_vs_cash"] > 0.0
    observed = paired_comparison(edge, benchmark, cash, LOADED)
    assert low["geometric_annual_return"] < observed["geometric_annual_return"]
    assert observed["geometric_annual_return"] < high["geometric_annual_return"]

    _, other, _ = _window_arrays(2)
    low, high = bootstrap_interval(other, benchmark, cash, LOADED, 0.95)
    assert low["geometric_annual_return"] < 0.0 < high["geometric_annual_return"]
    assert low["sharpe_vs_cash"] < 0.0 < high["sharpe_vs_cash"]
    assert (low, high) == bootstrap_interval(other, benchmark, cash, LOADED, 0.95)

    noise = np.random.default_rng(3).normal(0.0, 0.004, len(dates))
    widths = []
    for scale in (1.0, 3.0):
        low, high = bootstrap_interval(benchmark + scale * noise, benchmark, cash, LOADED, 0.95)
        widths.append(high["geometric_annual_return"] - low["geometric_annual_return"])
    assert widths[1] > 2.0 * widths[0]
    narrow, wide = (bootstrap_interval(other, benchmark, cash, LOADED, c) for c in (0.5, 0.99))
    assert wide[0]["geometric_annual_return"] < narrow[0]["geometric_annual_return"]

    # Half-year runs of gains and losses: resampling single days would hide how lumpy they are.
    lumpy = benchmark + np.where((np.arange(len(dates)) // 126) % 2 == 0, 0.001, -0.0008)
    daily = replace(
        LOADED,
        settings=LOADED.settings.model_copy(
            update={
                "falsification": LOADED.settings.falsification.model_copy(
                    update={"bootstrap_mean_block_days": 1.0}
                )
            }
        ),
    )
    widths = []
    for loaded in (LOADED, daily):
        low, high = bootstrap_interval(lumpy, benchmark, cash, loaded, 0.95)
        widths.append(high["geometric_annual_return"] - low["geometric_annual_return"])
    assert widths[0] > 1.5 * widths[1]


# 20 ----------------------------------------------------------------------------------------------


def test_leave_one_out_exposes_an_edge_from_one_year_or_one_episode() -> None:
    dates, benchmark, cash = _window_arrays(4)
    rule = (Rule(metric="leave_year_min.geometric_annual_return", op=">", value=0.0),)

    one_year = benchmark + np.where(dates.year == 2005, 0.002, 0.0)
    result = leave_one_out(dates, one_year, benchmark, cash, LOADED)
    full = paired_comparison(one_year, benchmark, cash, LOADED)
    assert full["geometric_annual_return"] > 0.0
    assert result["leave_year"]["2005"]["geometric_annual_return"] == 0.0
    assert result["leave_year_min"]["geometric_annual_return"] == 0.0
    assert result["leave_year_max"]["geometric_annual_return"] > 0.0
    assert evaluate_rules(rule, result)[0]["holds"] is False

    months = dates.to_period("M")
    crisis = (months >= pd.Period("2007-10", "M")) & (months <= pd.Period("2009-03", "M"))
    one_episode = benchmark + np.where(crisis, 0.002, 0.0)
    result = leave_one_out(dates, one_episode, benchmark, cash, LOADED)
    assert set(result["leave_episode"]) == {"dotcom_bear", "global_financial_crisis"}
    assert result["leave_episode_min"]["geometric_annual_return"] == 0.0
    assert result["leave_episode_max"]["geometric_annual_return"] > 0.0

    spread = benchmark + 0.0002
    result = leave_one_out(dates, spread, benchmark, cash, LOADED)
    assert result["leave_year_min"]["geometric_annual_return"] > 0.03
    assert evaluate_rules(rule, result)[0]["holds"] is True


# 21 ----------------------------------------------------------------------------------------------


def test_reruns_use_the_bench_cost_and_delay() -> None:
    spec = RunSpec(
        "qqq_direction",
        "vol",
        {"window": 21, "target": 0.3, "cap": 1.0},
        "development",
    )
    result = run_strategy(DATA, vol_target, spec, LOADED, falsification_confidence=0.95)
    assert result.falsification is not None
    base = result.falsification[cost_section(LOADED.cost_bps)]
    assert base == pytest.approx(result.comparison, nan_ok=True)
    costs = [result.falsification[cost_section(bps)] for bps in (4, 15, 25)]
    returns = [item["geometric_annual_return"] for item in costs]
    assert returns[0] > returns[1] > returns[2]

    oracle = falsify(_inputs(_oracle()), LOADED, 0.95)
    edge = oracle["paired"]["geometric_annual_return"]
    late = oracle[delay_section(1)]["geometric_annual_return"]
    assert edge > 0.5 and late < 0.05
