"""Invariants of the research bench (TRADING-2565, TRADING-2567 S3a).

Every research result from now on is produced by this bench, so a silent defect here would make all
of them wrong at once. Each guard has a negative control: a deliberately defective strategy,
metric or timing that the test must catch.

1. Benchmarks are what they claim: buy-and-hold is the QQQ series, fixed 3x is the 3x series, and a
   constant-exposure strategy equals its matched benchmark.
2. No look-ahead: a target never moves when only later data moves.
3. Every declared parameter is read.
4. No metric is a constant: every field moves when the returns move.
5. Cost: no turnover, no cost; more bps, less wealth.
6. Timing: a target decided at close t first earns the return from t+1 to t+2; a delay of k moves
   that by exactly k dates.
9. Reproducible: the same inputs give the same fingerprint, different inputs a different one.
10. The old simulator agrees: on the same prices the 200-day all-in/all-out rule earns the same
    daily returns as ``simple_baseline_portfolio_control``'s ``qqq_200dma_risk_off``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np
import pandas as pd
import pytest

from ai_trading_system import simple_baseline_portfolio_control as old
from ai_trading_system.research_bench import (
    BenchCheckError,
    BenchData,
    Rebalance,
    RunSpec,
    from_daily_frame,
    load_settings,
    run_strategy,
    simulate,
)
from ai_trading_system.research_bench.checks import (
    check_no_lookahead,
    check_parameters_consumed,
    cut_dates,
)
from ai_trading_system.research_bench.run import benchmark_simulations, benchmark_summaries
from ai_trading_system.research_bench.settings import resolve_window

LOADED = load_settings()
DEV = resolve_window(LOADED, "qqq_direction", "development")
STRUCTURAL_FIELDS = {"first_return_date", "last_return_date", "return_days", "years", "episodes"}


def _data(seed: int = 7, scale: float = 1.0) -> BenchData:
    index = pd.bdate_range("1997-01-02", "2011-06-30", name="date")
    rng = np.random.default_rng(seed)
    regime = np.where((index.year >= 2000) & (index.year <= 2002), 0.03, 0.015)
    r = rng.normal(0.0004, 1.0, len(index)) * regime * scale
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r - 0.002 / 252,
            "etf_reference_return": r,
            "cash_return": np.full(len(index), 0.03 / 360 * 1.4),
            "lev2_return": 2 * r - 0.04 / 252,
            "lev3_return": 3 * r - 0.07 / 252,
            "ndx_price": level * 0.98,
            "ndx_tr": level,
            "vix": 20.0 + 100 * np.abs(r),
            "vxn": 25.0 + 100 * np.abs(r),
            "vxo": 21.0,
            "cash_rate_annual": 0.03,
        },
        index=index,
    )
    daily.iloc[0, :5] = np.nan
    return from_daily_frame(daily, qqq_series="proxy", source={"snapshot_id": f"synthetic-{seed}"})


def vol_target(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    vol = inputs["qqq_return"].rolling(int(params["window"])).std() * np.sqrt(252)
    return (float(params["target"]) / vol).clip(upper=float(params["cap"]))


def leaky(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    """Negative control: tomorrow's return decides today's target."""

    tomorrow = inputs["qqq_return"].shift(-1)
    return (tomorrow > 0).astype(float) * float(params["cap"])


def ignores_window(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    """Negative control: declares a window it never reads."""

    return pd.Series(float(params["cap"]) * 0.5, index=inputs.index)


def _spec(**changes: Any) -> RunSpec:
    base: dict[str, Any] = {
        "question_type": "qqq_direction",
        "strategy_id": "vol_target",
        "params": {"window": 21, "target": 0.2, "cap": 1.0},
        "window_role": "development",
        "rebalance": Rebalance("month_end"),
    }
    base.update(changes)
    return RunSpec(**base)


# 1 -----------------------------------------------------------------------------------------------


def test_fixed_benchmarks_are_the_series_they_name() -> None:
    data = _data()
    sims = benchmark_simulations(
        data, DEV, average_exposure=1.0, rebalance=Rebalance(), cost_bps=4.0, leveraged=True
    )
    window = sims["buy_and_hold_qqq"].returns.index
    for name, column in (("buy_and_hold_qqq", "qqq"), ("fixed_3x", "lev3"), ("cash", "cash")):
        np.testing.assert_allclose(
            sims[name].returns.to_numpy(), data.returns.loc[window, column].to_numpy(), atol=1e-15
        )
        assert sims[name].daily["turnover"].sum() == 0.0
    # Negative control: the 2x benchmark is not the 3x series.
    assert not np.allclose(sims["fixed_2x"].returns, data.returns.loc[window, "lev3"])


def test_constant_strategy_equals_its_matched_benchmark() -> None:
    data = _data()

    def constant(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
        return pd.Series(float(params["level"]), index=inputs.index)

    result = run_strategy(
        data,
        constant,
        _spec(strategy_id="constant", params={"level": 0.6}, rebalance=Rebalance("daily")),
        LOADED,
    )
    assert result.benchmarks["matched_constant"]["average_exposure"] == pytest.approx(0.6)
    for field, difference in result.comparison.items():
        assert difference == pytest.approx(0.0, abs=1e-12), field
    # Negative control: buy-and-hold is not the matched benchmark of a 0.6 strategy.
    assert result.benchmarks["buy_and_hold_qqq"]["max_drawdown"] != pytest.approx(
        result.strategy["max_drawdown"]
    )


# 2 -----------------------------------------------------------------------------------------------


def test_lookahead_check_passes_an_honest_strategy_and_catches_a_leak() -> None:
    data = _data()
    dates = cut_dates(
        pd.DatetimeIndex(data.inputs.index), pd.Timestamp(DEV.start), pd.Timestamp(DEV.end), 20
    )
    assert len(dates) == 20
    params = {"window": 21, "target": 0.2, "cap": 1.0}
    assert check_no_lookahead(vol_target, data.inputs, params, dates) == []
    problems = check_no_lookahead(leaky, data.inputs, params, dates)
    assert len(problems) == 20
    with pytest.raises(BenchCheckError, match="changed when data after"):
        run_strategy(data, leaky, _spec(strategy_id="leaky"), LOADED)


# 3 -----------------------------------------------------------------------------------------------


def test_parameter_check_catches_a_declared_parameter_that_is_never_read() -> None:
    data = _data()
    start, end = pd.Timestamp(DEV.start), pd.Timestamp(DEV.end)
    params = {"window": 21, "target": 0.2, "cap": 1.0, "label": "q1a"}
    problems, unchecked = check_parameters_consumed(vol_target, data.inputs, params, start, end)
    assert problems == []
    assert unchecked == ["label"]
    problems, _ = check_parameters_consumed(ignores_window, data.inputs, params, start, end)
    assert problems == [
        "parameter window does not change any target in the window",
        "parameter target does not change any target in the window",
    ]
    with pytest.raises(BenchCheckError, match="parameter window"):
        run_strategy(data, ignores_window, _spec(strategy_id="ignores_window"), LOADED)


# 4 -----------------------------------------------------------------------------------------------


def _unmoved(first: Mapping[str, Any], second: Mapping[str, Any]) -> list[str]:
    return [
        field
        for field in first
        if field not in STRUCTURAL_FIELDS and first[field] == pytest.approx(second[field], abs=0)
    ]


def test_every_metric_moves_when_the_returns_move() -> None:
    first = run_strategy(_data(seed=7), vol_target, _spec(), LOADED).strategy
    second = run_strategy(_data(seed=7, scale=1.4), vol_target, _spec(), LOADED).strategy
    assert _unmoved(first, second) == []
    assert first["episodes"] != second["episodes"]
    # Negative control: a field written as a constant is found.
    assert _unmoved({**first, "verdict": 1.0}, {**second, "verdict": 1.0}) == ["verdict"]


# 5 -----------------------------------------------------------------------------------------------


def test_cost_is_zero_without_turnover_and_grows_with_bps() -> None:
    data = _data()
    _, summaries = benchmark_summaries(data, LOADED)
    assert summaries["buy_and_hold_qqq"]["annual_cost"] == 0.0
    targets = vol_target(data.inputs, {"window": 21, "target": 0.2, "cap": 1.0})
    ends = []
    for bps in (0.0, 4.0, 15.0, 25.0):
        sim = simulate(
            data.returns,
            targets,
            start=DEV.start,
            end=DEV.end,
            rebalance=Rebalance("month_end"),
            cost_bps=bps,
        )
        ends.append(float((1.0 + sim.returns).prod()))
        assert sim.daily["cost"].sum() == pytest.approx(sim.daily["turnover"].sum() * bps / 1e4)
    assert ends == sorted(ends, reverse=True)
    assert len(set(ends)) == 4


# 6 -----------------------------------------------------------------------------------------------


@pytest.mark.parametrize("delay", [0, 1, 2])
def test_a_target_first_earns_the_return_two_dates_after_its_decision(delay: int) -> None:
    data = _data()
    index = data.returns.index
    decided = int(index.searchsorted(pd.Timestamp("2005-06-15")))
    targets = pd.Series(0.0, index=index)
    targets.iloc[decided:] = 1.0
    sim = simulate(
        data.returns,
        targets,
        start=DEV.start,
        end=DEV.end,
        rebalance=Rebalance(),
        cost_bps=0.0,
        delay_days=delay,
    )
    first_long = index[decided + 2 + delay]
    assert sim.exposure.loc[: index[decided + 1 + delay]].max() == 0.0
    assert sim.exposure.loc[first_long:].min() == 1.0
    assert sim.returns.loc[first_long] == pytest.approx(data.returns.loc[first_long, "qqq"])
    # Negative control: the date before earns cash, not QQQ.
    before = index[decided + 1 + delay]
    assert sim.returns.loc[before] == pytest.approx(data.returns.loc[before, "cash"])


# 9 -----------------------------------------------------------------------------------------------


def test_same_inputs_give_the_same_fingerprint() -> None:
    first = run_strategy(_data(), vol_target, _spec(), LOADED)
    again = run_strategy(_data(), vol_target, _spec(), LOADED)
    assert first.fingerprint == again.fingerprint
    other_param = run_strategy(
        _data(), vol_target, _spec(params={"window": 22, "target": 0.2, "cap": 1.0}), LOADED
    )
    other_data = run_strategy(_data(seed=8), vol_target, _spec(), LOADED)
    assert len({first.fingerprint, other_param.fingerprint, other_data.fingerprint}) == 3


# 10 ----------------------------------------------------------------------------------------------


def test_the_200_day_rule_matches_the_old_simulator() -> None:
    index = pd.bdate_range("2019-01-02", periods=700, name="date")
    rng = np.random.default_rng(3)
    swing = np.sin(np.arange(len(index)) / 60.0) * 0.004
    qqq = 100.0 * np.cumprod(1.0 + swing + rng.normal(0.0003, 0.012, len(index)))
    sgov = 100.0 * np.cumprod(np.full(len(index), 1.0 + 0.0001))
    prices = pd.DataFrame({"QQQ": qqq, "SGOV": sgov}, index=index)
    config = old._load_registry(old.DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH)
    old_returns = old._strategy_return_series(
        {"strategy_id": "qqq_200dma_risk_off"}, prices, config, cost_bps=0.0
    )

    asset = prices.pct_change()
    returns = pd.DataFrame(
        {
            "qqq": asset["QQQ"],
            "cash": asset["SGOV"],
            "lev2": 2 * asset["QQQ"],
            "lev3": 3 * asset["QQQ"],
        }
    )
    moving_average = prices["QQQ"].rolling(200, min_periods=200).mean()
    targets = (prices["QQQ"] >= moving_average).astype(float).where(moving_average.notna())
    assert targets.loc["2019-12-01":].nunique() == 2  # the rule really switches
    start = index[260].date()
    sim = simulate(returns, targets, start=start, end=None, rebalance=Rebalance(), cost_bps=0.0)
    np.testing.assert_allclose(
        sim.returns.to_numpy(), old_returns.loc[sim.returns.index].to_numpy(), atol=1e-12
    )
    # Negative control: one extra day of delay no longer matches.
    late = simulate(
        returns, targets, start=start, end=None, rebalance=Rebalance(), cost_bps=0.0, delay_days=1
    )
    assert not np.allclose(late.returns, old_returns.loc[late.returns.index], atol=1e-12)
