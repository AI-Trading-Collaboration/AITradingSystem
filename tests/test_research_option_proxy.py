from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.research_bench.data import BenchData, from_daily_frame
from ai_trading_system.research_bench.settings import load_settings
from ai_trading_system.research_probes import option_proxy as op

LOADED = load_settings()
ROOT = Path(__file__).resolve().parents[1]


# Independent Black-Scholes, written out here so that expected values do not come from the module.
def _cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _d1(spot: float, rate: float, sigma: float, years: float) -> float:
    return (math.log(spot) + (rate + 0.5 * sigma**2) * years) / (sigma * math.sqrt(years))


def _put(spot: float, rate: float, sigma: float, years: float) -> float:
    d1 = _d1(spot, rate, sigma, years)
    d2 = d1 - sigma * math.sqrt(years)
    return math.exp(-rate * years) * _cdf(-d2) - spot * _cdf(-d1)


def _atm(sigma: float, days: int) -> float:
    """At-the-money put or call with spot = strike = 1 and no interest."""

    return math.erf(sigma * math.sqrt(days / 252.0) / (2.0 * math.sqrt(2.0)))


def _index() -> pd.DatetimeIndex:
    # Month ends: 2003-01-31, 2003-02-28, 2003-03-31, 2003-04-30. Cycles of 20, 21 and 22 days.
    return pd.bdate_range("2003-01-01", "2003-04-30", name="date")


def _series(index: pd.DatetimeIndex, value: Any) -> pd.Series:
    if np.isscalar(value):
        return pd.Series(float(value), index=index)
    return pd.Series(np.asarray(value, dtype=float), index=index)


def _inputs(
    index: pd.DatetimeIndex | None = None,
    ndx: Any = 0.0,
    cash: Any = 0.0,
    vxn: Any = 20.0,
    rate: Any = 0.0,
) -> op.ProxyInputs:
    index = _index() if index is None else index
    return op.ProxyInputs(
        ndx_return=_series(index, ndx),
        cash_return=_series(index, cash),
        vxn=_series(index, vxn),
        rate=_series(index, rate),
        filled_days={"vxn": 0, "rate": 0},
    )


def _moves(moves: dict[str, float], index: pd.DatetimeIndex | None = None) -> pd.Series:
    index = _index() if index is None else index
    out = pd.Series(0.0, index=index)
    for day, value in moves.items():
        out.loc[pd.Timestamp(day)] = value
    return out


def _random_inputs(seed: int = 7) -> op.ProxyInputs:
    index = _index()
    rng = np.random.default_rng(seed)
    return _inputs(
        index,
        ndx=rng.normal(0.0004, 0.012, len(index)),
        cash=np.full(len(index), 0.00008),
        vxn=rng.uniform(15.0, 40.0, len(index)),
        rate=rng.uniform(0.01, 0.05, len(index)),
    )


def _wide(seed: int = 3, ndx: Any = None, vxn: Any = None) -> dict[str, pd.Series]:
    """Frames that run past both ends of the development window."""

    index = pd.bdate_range("2000-06-01", "2011-03-31", name="date")
    rng = np.random.default_rng(seed)
    values = rng.normal(0.0004, 0.014, len(index)) if ndx is None else ndx
    implied = rng.uniform(18.0, 35.0, len(index)) if vxn is None else vxn
    return {
        "ndx_return": _series(index, values),
        "cash_return": _series(index, 0.00005),
        "vxn": _series(index, implied),
        "rate": _series(index, 0.02),
    }


def _walk_lists(value: Any) -> list[int]:
    if isinstance(value, dict):
        return [n for item in value.values() for n in _walk_lists(item)]
    if isinstance(value, list):
        return [len(value), *[n for item in value for n in _walk_lists(item)]]
    return []


# --- Black-Scholes ---------------------------------------------------------------------------


def test_prices_match_known_values() -> None:
    assert op.bs_price("call", 1.0, 1.0, 0.0, 0.2, 1.0) == pytest.approx(0.0796556745, abs=1e-9)
    assert op.bs_price("put", 1.0, 1.0, 0.0, 0.2, 1.0) == pytest.approx(0.0796556745, abs=1e-9)
    assert op.bs_price("call", 100.0, 100.0, 0.05, 0.2, 1.0) == pytest.approx(10.4506, abs=1e-4)
    assert op.bs_price("put", 100.0, 100.0, 0.05, 0.2, 1.0) == pytest.approx(5.5735, abs=1e-4)
    assert op.normal_cdf(0.0) == 0.5
    assert op.normal_cdf(1.0) == pytest.approx(0.8413447461, abs=1e-9)


@pytest.mark.parametrize(
    ("spot", "strike", "rate", "sigma", "years"),
    [(1.0, 1.0, 0.03, 0.25, 0.08), (1.1, 1.0, 0.0, 0.4, 0.25), (0.8, 1.0, 0.05, 0.15, 1.0)],
)
def test_put_call_parity_and_delta(
    spot: float, strike: float, rate: float, sigma: float, years: float
) -> None:
    call = op.bs_price("call", spot, strike, rate, sigma, years)
    put = op.bs_price("put", spot, strike, rate, sigma, years)
    assert call - put == pytest.approx(spot - strike * math.exp(-rate * years), abs=1e-12)
    step = 1e-6
    for kind in ("call", "put"):
        up = op.bs_price(kind, spot + step, strike, rate, sigma, years)  # type: ignore[arg-type]
        down = op.bs_price(kind, spot - step, strike, rate, sigma, years)  # type: ignore[arg-type]
        delta = op.bs_delta(kind, spot, strike, rate, sigma, years)  # type: ignore[arg-type]
        assert delta == pytest.approx((up - down) / (2 * step), abs=1e-6)
    assert op.bs_delta("call", spot, strike, rate, sigma, years) - op.bs_delta(
        "put", spot, strike, rate, sigma, years
    ) == pytest.approx(1.0, abs=1e-12)


def test_delta_value_and_limits() -> None:
    assert op.bs_delta("call", 1.0, 1.0, 0.0, 0.2, 1.0) == pytest.approx(0.5398278373, abs=1e-9)
    assert op.bs_delta("put", 1.0, 1.0, 0.0, 0.2, 1.0) == pytest.approx(-0.4601721627, abs=1e-9)
    assert op.bs_price("call", 1.2, 1.0, 0.03, 0.2, 0.0) == pytest.approx(0.2)
    assert op.bs_price("put", 0.9, 1.0, 0.03, 0.2, 0.0) == pytest.approx(0.1)
    assert op.bs_price("put", 1.2, 1.0, 0.03, 0.2, 0.0) == 0.0
    assert op.bs_price("call", 1.0, 1.0, 0.0, 0.0, 0.5) == 0.0
    assert op.bs_price("put", 0.9, 1.0, 0.0, 0.0, 0.5) == pytest.approx(0.1)
    assert op.bs_price("call", 1.0, 1.0, 0.02, 0.0, 1.0) == pytest.approx(1.0 - math.exp(-0.02))
    assert op.bs_delta("call", 1.2, 1.0, 0.0, 0.2, 0.0) == 1.0
    assert op.bs_delta("call", 0.9, 1.0, 0.0, 0.2, 0.0) == 0.0
    assert op.bs_delta("put", 0.9, 1.0, 0.0, 0.2, 0.0) == -1.0
    assert op.bs_delta("put", 1.2, 1.0, 0.0, 0.2, 0.0) == 0.0


def test_an_option_exactly_at_the_money_at_expiry_is_worth_nothing_and_has_no_delta() -> None:
    for kind in ("call", "put"):
        assert op.bs_price(kind, 1.0, 1.0, 0.03, 0.2, 0.0) == 0.0  # type: ignore[arg-type]
        assert op.bs_delta(kind, 1.0, 1.0, 0.03, 0.2, 0.0) == 0.0  # type: ignore[arg-type]
    forward = math.exp(-0.03 * 0.5)
    assert op.bs_delta("call", forward, 1.0, 0.03, 0.0, 0.5) == 0.0
    assert op.bs_delta("put", forward, 1.0, 0.03, 0.0, 0.5) == 0.0
    assert op.bs_delta("call", forward + 1e-9, 1.0, 0.03, 0.0, 0.5) == 1.0
    assert op.bs_delta("put", forward - 1e-9, 1.0, 0.03, 0.0, 0.5) == -1.0


def test_bad_option_inputs_are_refused() -> None:
    with pytest.raises(ValueError, match="kind"):
        op.bs_price("straddle", 1.0, 1.0, 0.0, 0.2, 1.0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="kind"):
        op.bs_delta("straddle", 1.0, 1.0, 0.0, 0.2, 1.0)  # type: ignore[arg-type]
    for args in [(0.0, 1.0, 0.0, 0.2, 1.0), (1.0, -1.0, 0.0, 0.2, 1.0)]:
        with pytest.raises(ValueError):
            op.bs_price("put", *args)
    with pytest.raises(ValueError):
        op.bs_price("put", 1.0, 1.0, 0.0, -0.2, 1.0)
    with pytest.raises(ValueError):
        op.bs_price("put", 1.0, 1.0, 0.0, 0.2, -1.0)
    with pytest.raises(ValueError):
        op.bs_delta("call", 1.0, 1.0, 0.0, float("nan"), 1.0)


# --- cycles ----------------------------------------------------------------------------------


def test_cycles_run_from_month_end_to_month_end() -> None:
    index = _index()
    bounds = op.cycle_bounds(index)
    assert [(index[a].date().isoformat(), index[b].date().isoformat()) for a, b in bounds] == [
        ("2003-01-31", "2003-02-28"),
        ("2003-02-28", "2003-03-31"),
        ("2003-03-31", "2003-04-30"),
    ]
    assert [b - a for a, b in bounds] == [20, 21, 22]


def test_a_month_that_is_not_finished_is_not_a_cycle() -> None:
    index = pd.bdate_range("2003-01-01", "2003-04-15", name="date")
    bounds = op.cycle_bounds(index)
    assert [index[b].date().isoformat() for _, b in bounds] == ["2003-02-28", "2003-03-31"]
    with pytest.raises(ValueError, match="cycle"):
        op.structure_path(
            _inputs(pd.bdate_range("2003-02-03", "2003-02-20", name="date")), "put_write", 1.0
        )


# --- put-write: hand-computed paths ----------------------------------------------------------


def test_flat_market_put_write_earns_the_premium() -> None:
    path = op.structure_path(_inputs(), "put_write", 1.0)
    assert [cycle.days for cycle in path.cycles] == [20, 21, 22]
    for cycle in path.cycles:
        assert cycle.premium == pytest.approx(_atm(0.2, cycle.days), abs=1e-12)
        assert cycle.cycle_return == pytest.approx(_atm(0.2, cycle.days), abs=1e-12)
        assert cycle.underlying_return == 0.0
    assert len(path.returns) == 20 + 21 + 22
    assert path.returns.index[0] == pd.Timestamp("2003-02-03")
    assert path.returns.index[-1] == pd.Timestamp("2003-04-30")
    first = path.returns.iloc[:20]
    assert float(np.prod(1.0 + first.to_numpy())) - 1.0 == pytest.approx(path.cycles[0].premium)
    # The first day: the put is one day older, so its liability has shrunk by the time decay.
    assert path.returns.iloc[0] == pytest.approx(_atm(0.2, 20) - _atm(0.2, 19), abs=1e-12)


def test_sigma_scale_scales_the_volatility_used() -> None:
    path = op.structure_path(_inputs(), "put_write", 0.9)
    assert path.cycles[0].premium == pytest.approx(_atm(0.18, 20), abs=1e-12)
    assert path.cycles[0].premium < op.structure_path(_inputs(), "put_write", 1.0).cycles[0].premium


def test_put_write_loses_what_the_market_loses_below_the_strike() -> None:
    path = op.structure_path(_inputs(ndx=_moves({"2003-02-28": -0.10})), "put_write", 1.0)
    assert path.cycles[0].cycle_return == pytest.approx(_atm(0.2, 20) - 0.10, abs=1e-12)
    assert path.cycles[0].underlying_return == pytest.approx(-0.10)
    assert path.cycles[1].cycle_return == pytest.approx(_atm(0.2, 21), abs=1e-12)


def test_put_write_gains_nothing_from_a_rally() -> None:
    path = op.structure_path(_inputs(ndx=_moves({"2003-02-28": 0.10})), "put_write", 1.0)
    assert path.cycles[0].cycle_return == pytest.approx(_atm(0.2, 20), abs=1e-12)


def test_cash_and_premium_earn_the_cash_return() -> None:
    path = op.structure_path(_inputs(cash=0.0001), "put_write", 1.0)
    expected = (1.0 + _atm(0.2, 20)) * 1.0001**20 - 1.0
    assert path.cycles[0].cycle_return == pytest.approx(expected, abs=1e-12)


def test_cash_return_compounds_over_the_days_of_each_cycle() -> None:
    index = _index()
    cash = pd.Series(np.arange(len(index)) * 2e-6, index=index)
    puts = op.structure_path(_inputs(cash=cash), "put_write", 1.0)
    calls = op.structure_path(_inputs(cash=cash), "covered_call", 1.0)
    spans = [("2003-02-03", "2003-02-28", 20), ("2003-03-03", "2003-03-31", 21)]
    for position, (first, last, days) in enumerate(spans):
        grown = float(np.prod(1.0 + cash.loc[first:last].to_numpy()))
        premium = _atm(0.2, days)
        assert puts.cycles[position].cycle_return == pytest.approx(
            (1.0 + premium) * grown - 1.0, abs=1e-12
        )
        assert calls.cycles[position].cycle_return == pytest.approx(premium * grown, abs=1e-12)


def test_the_daily_marking_uses_the_day_s_volatility_and_rate() -> None:
    rate = 0.04
    ndx = _moves({"2003-02-10": 0.03})
    base = _inputs(ndx=ndx, rate=rate)
    day = pd.Timestamp("2003-02-10")
    spot = 1.03
    years = 14 / 252.0  # 2003-02-10 is the 6th trading day of the 20-day cycle
    liability = _put(spot, rate, 0.2, years)
    entry = _put(1.0, rate, 0.2, 20 / 252.0)
    wealth = 1.0 + entry - liability
    previous = 1.0 + entry - _put(1.0, rate, 0.2, 15 / 252.0)
    path = op.structure_path(base, "put_write", 1.0)
    assert path.returns.loc[day] == pytest.approx(wealth / previous - 1.0, abs=1e-12)
    changed = op.structure_path(
        _inputs(ndx=ndx, rate=rate, vxn=_moves({"2003-02-10": 0.0}) + 20.0), "put_write", 1.0
    )
    assert changed.returns.loc[day] == path.returns.loc[day]
    higher = _moves({"2003-02-10": 10.0}) + 20.0
    moved = op.structure_path(_inputs(ndx=ndx, rate=rate, vxn=higher), "put_write", 1.0)
    assert moved.returns.loc[day] < path.returns.loc[day]


def test_cycle_return_is_the_compounded_daily_returns() -> None:
    inputs = _random_inputs()
    for structure in op.STRUCTURES:
        path = op.structure_path(inputs, structure, 0.9)
        position = 0
        for cycle in path.cycles:
            part = path.returns.iloc[position : position + cycle.days]
            assert float(np.prod(1.0 + part.to_numpy())) - 1.0 == pytest.approx(
                cycle.cycle_return, abs=1e-12
            )
            position += cycle.days
        assert position == len(path.returns)


# --- covered call ----------------------------------------------------------------------------


def test_covered_call_hand_computed_cycles() -> None:
    flat = op.structure_path(_inputs(), "covered_call", 1.0)
    assert flat.cycles[0].cycle_return == pytest.approx(_atm(0.2, 20), abs=1e-12)
    up = op.structure_path(_inputs(ndx=_moves({"2003-02-28": 0.10})), "covered_call", 1.0)
    assert up.cycles[0].cycle_return == pytest.approx(_atm(0.2, 20), abs=1e-12)
    assert up.cycles[0].underlying_return == pytest.approx(0.10)
    down = op.structure_path(_inputs(ndx=_moves({"2003-02-28": -0.10})), "covered_call", 1.0)
    assert down.cycles[0].cycle_return == pytest.approx(_atm(0.2, 20) - 0.10, abs=1e-12)


def test_without_interest_the_two_structures_are_the_same_bet() -> None:
    index = _index()
    rng = np.random.default_rng(11)
    inputs = _inputs(
        index,
        ndx=rng.normal(0.0, 0.015, len(index)),
        vxn=rng.uniform(15.0, 40.0, len(index)),
    )
    put = op.structure_path(inputs, "put_write", 1.0)
    call = op.structure_path(inputs, "covered_call", 1.0)
    np.testing.assert_allclose(put.returns.to_numpy(), call.returns.to_numpy(), atol=1e-12)
    np.testing.assert_allclose(put.exposure.to_numpy(), call.exposure.to_numpy(), atol=1e-12)


# --- exposure and control ---------------------------------------------------------------------


def test_exposure_is_the_delta_at_the_previous_close_per_unit_of_wealth() -> None:
    rate, sigma = 0.05, 0.2
    ndx = _moves({"2003-02-03": 0.02})
    path = op.structure_path(_inputs(ndx=ndx, rate=rate), "put_write", 1.0)
    r = rate
    first = _cdf(-_d1(1.0, r, sigma, 20 / 252.0))
    assert path.exposure.iloc[0] == pytest.approx(first, abs=1e-12)
    spot = 1.02
    entry = _put(1.0, r, sigma, 20 / 252.0)
    wealth = 1.0 + entry - _put(spot, r, sigma, 19 / 252.0)
    second = _cdf(-_d1(spot, r, sigma, 19 / 252.0)) * spot / wealth
    assert path.exposure.iloc[1] == pytest.approx(second, abs=1e-12)
    cover = op.structure_path(_inputs(ndx=ndx, rate=rate), "covered_call", 1.0)
    assert cover.exposure.iloc[0] == pytest.approx(first, abs=1e-12)


def test_nothing_after_a_close_reaches_back_into_the_position_held_over_that_day() -> None:
    base = op.structure_path(_inputs(), "put_write", 1.0)
    day = pd.Timestamp("2003-02-12")
    bigger = _moves({"2003-02-12": 40.0}) + 20.0
    changed = op.structure_path(_inputs(vxn=bigger), "put_write", 1.0)
    before = base.returns.index < day
    pd.testing.assert_series_equal(base.returns[before], changed.returns[before])
    pd.testing.assert_series_equal(
        base.exposure[base.exposure.index <= day], changed.exposure[changed.exposure.index <= day]
    )
    following = base.exposure.index[base.exposure.index > day][0]
    assert changed.exposure.loc[following] != base.exposure.loc[following]
    assert changed.cycles[0].premium == base.cycles[0].premium


def test_the_premium_is_set_by_the_volatility_on_the_opening_day_only() -> None:
    base = op.structure_path(_inputs(), "put_write", 1.0)
    on_open = op.structure_path(_inputs(vxn=_moves({"2003-01-31": 10.0}) + 20.0), "put_write", 1.0)
    after_open = op.structure_path(
        _inputs(vxn=_moves({"2003-02-03": 10.0}) + 20.0), "put_write", 1.0
    )
    assert on_open.cycles[0].premium == pytest.approx(_atm(0.3, 20), abs=1e-12)
    assert on_open.cycles[0].vxn_start == 30.0
    assert after_open.cycles[0].premium == base.cycles[0].premium


def test_the_last_day_of_a_cycle_does_not_change_what_was_held_before_it() -> None:
    inputs = _inputs(ndx=_random_inputs().ndx_return)
    last = pd.Timestamp("2003-02-28")
    shocked = _inputs(ndx=inputs.ndx_return.where(inputs.ndx_return.index != last, -0.15))
    base = op.structure_path(inputs, "put_write", 1.0)
    other = op.structure_path(shocked, "put_write", 1.0)
    first_cycle = base.returns.index <= last
    pd.testing.assert_series_equal(base.exposure[first_cycle], other.exposure[first_cycle])
    pd.testing.assert_series_equal(
        base.returns[base.returns.index < last], other.returns[other.returns.index < last]
    )
    assert other.returns.loc[last] < base.returns.loc[last]


def test_control_is_a_constant_mix_at_the_mean_exposure() -> None:
    inputs = _random_inputs()
    path = op.structure_path(inputs, "put_write", 0.9)
    control, mean = op.control_returns(inputs, path)
    assert mean == pytest.approx(float(path.exposure.mean()))
    expected = (
        mean * inputs.ndx_return.loc[path.returns.index]
        + (1.0 - mean) * inputs.cash_return.loc[path.returns.index]
    )
    pd.testing.assert_series_equal(control, expected, check_names=False)
    assert 0.0 < mean < 1.0


# --- the development window and the inputs --------------------------------------------------


def test_the_window_constants_are_the_registered_development_window() -> None:
    registry = yaml.safe_load(
        (ROOT / "config/research/research_window_registry.yaml").read_text(encoding="utf-8")
    )
    window = registry["windows"]["qqq_long_history_development"]
    assert window["end"] == op.DEV_END == date(2010, 12, 31)
    assert op.DEV_START == date(2001, 2, 2)
    assert window["start"] <= op.DEV_START
    assert (op.SIGMA_SCALES, op.CENTRAL_SIGMA_SCALE, op.TRADING_DAYS) == ((1.0, 0.9, 0.8), 0.9, 252)


def test_inputs_beyond_the_development_window_are_refused() -> None:
    late = pd.bdate_range("2010-11-01", "2011-01-31", name="date")
    with pytest.raises(ValueError, match="within"):
        _inputs(late)
    early = pd.bdate_range("2001-01-15", "2001-03-30", name="date")
    with pytest.raises(ValueError, match="within"):
        _inputs(early)


def test_frames_are_cut_to_the_development_window_and_later_data_never_matters() -> None:
    frames = _wide()
    cut = op.ProxyInputs.from_frames(**frames)
    assert cut.vxn.index[0] == pd.Timestamp("2001-02-02")
    assert cut.vxn.index[-1] == pd.Timestamp("2010-12-31")
    wild = _wide()
    after = wild["vxn"].index > pd.Timestamp("2010-12-31")
    for name in ("ndx_return", "vxn"):
        wild[name] = wild[name].mask(after, 500.0)
    other = op.ProxyInputs.from_frames(**wild)
    a, b = op.structure_path(cut, "put_write", 1.0), op.structure_path(other, "put_write", 1.0)
    pd.testing.assert_series_equal(a.returns, b.returns)
    pd.testing.assert_series_equal(a.exposure, b.exposure)
    assert op.run_probe(cut, LOADED, draws=10) == op.run_probe(other, LOADED, draws=10)


def test_short_gaps_in_volatility_and_rate_are_filled_and_counted() -> None:
    frames = _wide()
    frames["vxn"] = frames["vxn"].copy()
    frames["vxn"].loc[pd.Timestamp("2005-06-07")] = np.nan
    frames["rate"] = frames["rate"].copy()
    frames["rate"].loc[pd.Timestamp("2005-06-07") : pd.Timestamp("2005-06-09")] = np.nan
    inputs = op.ProxyInputs.from_frames(**frames)
    assert inputs.filled_days == {"vxn": 1, "rate": 3}
    assert inputs.vxn.loc["2005-06-07"] == frames["vxn"].loc["2005-06-06"]
    assert inputs.rate.loc["2005-06-09"] == frames["rate"].loc["2005-06-06"]
    frames["vxn"].loc[pd.Timestamp("2005-06-07") : pd.Timestamp("2005-06-10")] = np.nan
    with pytest.raises(ValueError, match="gaps longer"):
        op.ProxyInputs.from_frames(**frames)


def test_missing_returns_and_bad_inputs_are_refused() -> None:
    frames = _wide()
    frames["ndx_return"] = frames["ndx_return"].copy()
    frames["ndx_return"].loc[pd.Timestamp("2005-06-07")] = np.nan
    with pytest.raises(ValueError, match="ndx_return"):
        op.ProxyInputs.from_frames(**frames)
    index = _index()
    with pytest.raises(ValueError, match="vxn must be positive"):
        _inputs(index, vxn=0.0)
    with pytest.raises(ValueError, match="same dates"):
        op.ProxyInputs(
            ndx_return=_series(index[:-1], 0.0),
            cash_return=_series(index, 0.0),
            vxn=_series(index, 20.0),
            rate=_series(index, 0.0),
            filled_days={"vxn": 0, "rate": 0},
        )
    with pytest.raises(ValueError, match="structure"):
        op.structure_path(_inputs(), "straddle", 1.0)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="sigma_scale"):
        op.structure_path(_inputs(), "put_write", 0.0)


# --- the probe -------------------------------------------------------------------------------


def _alternating(index: pd.DatetimeIndex) -> np.ndarray[Any, Any]:
    return np.where(np.arange(len(index)) % 2 == 0, 0.01, -0.01)


def test_realized_volatility_is_the_root_mean_square_annualized_in_percent() -> None:
    frames = _wide()
    index = frames["vxn"].index
    frames["ndx_return"] = _series(index, _alternating(index))
    frames["vxn"] = _series(index, 25.0)
    inputs = op.ProxyInputs.from_frames(**frames)
    premium = op.premium_summary(op.structure_path(inputs, "put_write", 1.0).cycles)
    realized = 100.0 * math.sqrt(252.0 * 0.0001)
    assert premium["mean_vol_points"] == pytest.approx(25.0 - realized, abs=1e-9)
    assert premium["median_vol_points"] == pytest.approx(25.0 - realized, abs=1e-9)
    assert premium["share_positive"] == 1.0
    assert premium["cycles"] == 118
    assert list(premium["by_year"]) == [str(year) for year in range(2001, 2011)]
    assert all(
        value == pytest.approx(25.0 - realized, abs=1e-9) for value in premium["by_year"].values()
    )
    assert premium["worst_cycle"]["vol_points"] == pytest.approx(25.0 - realized, abs=1e-9)


def test_realized_volatility_uses_the_days_inside_each_cycle() -> None:
    ndx = _series(_index(), 0.01) - 0.0
    ndx.loc[pd.Timestamp("2003-02-28")] = 0.03
    ndx.loc[pd.Timestamp("2003-03-31")] = -0.02
    cycles = op.structure_path(_inputs(ndx=ndx, vxn=25.0), "put_write", 1.0).cycles
    squares = (20 * 0.0001 + 0.0004) / 21.0
    assert cycles[0].realized_vol == pytest.approx(
        100.0 * math.sqrt(252.0 * (19 * 0.0001 + 0.0009) / 20.0), abs=1e-9
    )
    assert cycles[1].realized_vol == pytest.approx(100.0 * math.sqrt(252.0 * squares), abs=1e-9)
    assert cycles[2].realized_vol == pytest.approx(100.0 * math.sqrt(252.0 * 0.0001), abs=1e-9)
    assert [cycle.premium_vol for cycle in cycles] == pytest.approx(
        [25.0 - cycle.realized_vol for cycle in cycles]
    )


def test_premium_by_year_groups_cycles_by_the_year_they_end() -> None:
    cycles = op.structure_path(op.ProxyInputs.from_frames(**_wide()), "put_write", 1.0).cycles
    expected: dict[str, list[float]] = {}
    for cycle in cycles:
        expected.setdefault(str(cycle.end.year), []).append(cycle.premium_vol)
    premium = op.premium_summary(cycles)
    assert premium["by_year"] == {
        year: pytest.approx(sum(values) / len(values)) for year, values in expected.items()
    }
    assert [cycle.start.year for cycle in cycles] != [cycle.end.year for cycle in cycles]


def test_premium_summary_picks_the_worst_cycle_and_the_years() -> None:
    inputs = _random_inputs()
    cycles = op.structure_path(inputs, "put_write", 1.0).cycles
    premium = op.premium_summary(cycles)
    values = [cycle.premium_vol for cycle in cycles]
    assert premium["mean_vol_points"] == pytest.approx(sum(values) / len(values))
    assert premium["median_vol_points"] == pytest.approx(sorted(values)[1])
    assert premium["worst_cycle"]["vol_points"] == pytest.approx(min(values))
    assert premium["worst_cycle"]["end"] == cycles[values.index(min(values))].end.strftime("%Y-%m")
    assert premium["by_year"] == {"2003": pytest.approx(sum(values) / len(values))}


@pytest.fixture(scope="module")
def result() -> dict[str, Any]:
    return op.run_probe(op.ProxyInputs.from_frames(**_wide()), LOADED, draws=40)


def test_the_result_reports_put_write_at_every_scale(result: dict[str, Any]) -> None:
    assert result["probe"] == "option_proxy" and result["task"] == "TRADING-2567-20"
    assert list(result["structures"]) == ["put_write"]
    assert list(result["structures"]["put_write"]) == ["1", "0.9", "0.8"]
    assert list(result["parity"]) == ["1", "0.9", "0.8"]
    assert result["window"] == {
        "start": "2001-02-28",
        "end": "2010-12-31",
        "cycles": 118,
        "return_days": result["window"]["return_days"],
    }
    assert result["filled_days"] == {"vxn": 0, "rate": 0}
    assumptions = result["assumptions"]
    assert assumptions["sigma_scales"] == [1.0, 0.9, 0.8] and assumptions["costs"] == "none"
    assert assumptions["central_sigma_scale"] == 0.9


def test_each_variant_is_consistent_with_the_bench_metrics(result: dict[str, Any]) -> None:
    inputs = op.ProxyInputs.from_frames(**_wide())
    for structure in ("put_write",):
        for scale in op.SIGMA_SCALES:
            row = result["structures"][structure][f"{scale:g}"]
            path = op.structure_path(inputs, structure, scale)
            daily = path.returns.to_numpy()
            equity = np.concatenate([[1.0], np.cumprod(1.0 + daily)])
            drawdown = float((equity / np.maximum.accumulate(equity) - 1.0).min())
            tail = float(np.sort(daily)[: math.ceil(0.05 * len(daily))].mean())
            assert row["strategy"]["max_drawdown"] == pytest.approx(drawdown, abs=1e-12)
            assert row["strategy"]["cvar_5pct"] == pytest.approx(tail, abs=1e-12)
            assert row["mean_exposure"] == pytest.approx(float(path.exposure.mean()))
            assert row["strategy"]["average_exposure"] == pytest.approx(float(path.exposure.mean()))
            assert row["control"]["average_exposure"] == pytest.approx(row["mean_exposure"])
            paired = row["paired"]
            assert paired["max_drawdown"] == pytest.approx(
                row["strategy"]["max_drawdown"] - row["control"]["max_drawdown"], abs=1e-12
            )
            assert paired["cvar_5pct_relative_improvement"] == pytest.approx(
                (row["strategy"]["cvar_5pct"] - row["control"]["cvar_5pct"])
                / abs(row["control"]["cvar_5pct"]),
                abs=1e-12,
            )
            for field in ("max_drawdown", "cvar_5pct_relative_improvement"):
                assert row["ci_low"][field] <= row["ci_high"][field]
            assert row["confidence"] == LOADED.settings.falsification.confidence
            assert row["cycles"] == 118
            assert row["sigma_scale"] == scale


def test_worst_cycles_are_the_five_lowest_returns(result: dict[str, Any]) -> None:
    inputs = op.ProxyInputs.from_frames(**_wide())
    cycles = op.structure_path(inputs, "put_write", 0.9).cycles
    row = result["structures"]["put_write"]["0.9"]
    lowest = sorted(cycles, key=lambda cycle: cycle.cycle_return)[:5]
    assert [item["end"] for item in row["worst_cycles"]] == [
        c.end.strftime("%Y-%m") for c in lowest
    ]
    returns = [item["cycle_return"] for item in row["worst_cycles"]]
    assert returns == sorted(returns)
    assert row["mean_premium"] == pytest.approx(sum(c.premium for c in cycles) / len(cycles))
    wins = sum(1 for c in cycles if c.cycle_return > 0.0) / len(cycles)
    assert row["share_cycles_positive"] == pytest.approx(wins)


def test_a_lower_scale_prices_a_smaller_premium_and_does_worse_in_a_calm_market(
    result: dict[str, Any],
) -> None:
    rows = result["structures"]["put_write"]
    assert rows["1"]["mean_premium"] > rows["0.9"]["mean_premium"] > rows["0.8"]["mean_premium"]
    geometric = [
        rows[scale]["strategy"]["geometric_annual_return"] for scale in ("1", "0.9", "0.8")
    ]
    assert geometric[0] > geometric[1] > geometric[2]


def test_the_bootstrap_gets_the_requested_draws_and_the_configured_confidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[int, float]] = []

    def fake(
        strategy: Any, benchmark: Any, cash: Any, loaded: Any, confidence: float
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        seen.append((loaded.settings.falsification.bootstrap_draws, confidence))
        paired = op.paired_comparison(strategy, benchmark, cash, loaded)
        return paired, paired

    monkeypatch.setattr(op, "bootstrap_interval", fake)
    inputs = op.ProxyInputs.from_frames(**_wide())
    op.run_probe(inputs, LOADED, scales=(1.0,), draws=7)
    assert seen == [(7, LOADED.settings.falsification.confidence)]
    seen.clear()
    op.run_probe(inputs, LOADED, scales=(0.9,))
    assert seen == [
        (LOADED.settings.falsification.bootstrap_draws, LOADED.settings.falsification.confidence)
    ]


def test_years_are_counted_from_the_first_cycle_opening(result: dict[str, Any]) -> None:
    inputs = op.ProxyInputs.from_frames(**_wide())
    path = op.structure_path(inputs, "put_write", 1.0)
    equity = float(np.prod(1.0 + path.returns.to_numpy()))
    years = (path.returns.index[-1] - path.cycles[0].start).days / 365.25
    row = result["structures"]["put_write"]["1"]
    assert row["strategy"]["geometric_annual_return"] == pytest.approx(
        equity ** (1.0 / years) - 1.0, abs=1e-12
    )


def test_the_result_holds_only_aggregates(result: dict[str, Any]) -> None:
    text = json.dumps(result, allow_nan=False)
    assert max(_walk_lists(result)) <= 130
    assert len(text) < 200_000
    assert set(result["structures"]["put_write"]["1"]) == {
        "sigma_scale",
        "mean_exposure",
        "strategy",
        "control",
        "geometric_annual_difference",
        "replica",
        "strategy_vs_replica",
        "paired",
        "ci_low",
        "ci_high",
        "confidence",
        "cycles",
        "mean_premium",
        "premium_vol",
        "share_cycles_positive",
        "worst_cycles",
    }
    assert set(result["structures"]["put_write"]["1"]["strategy"]) == set(op.SUMMARY_KEYS)


def test_the_result_does_not_depend_on_anything_but_its_inputs(result: dict[str, Any]) -> None:
    again = op.run_probe(op.ProxyInputs.from_frames(**_wide()), LOADED, draws=40)
    assert again == result
    other = op.run_probe(op.ProxyInputs.from_frames(**_wide(seed=4)), LOADED, draws=40)
    assert other != result
    few = op.run_probe(op.ProxyInputs.from_frames(**_wide()), LOADED, scales=(1.0,), draws=10)
    assert list(few["structures"]["put_write"]) == ["1"] and list(few["parity"]) == ["1"]
    assert few["assumptions"]["sigma_scales"] == [1.0]


def test_clean_makes_plain_json_values() -> None:
    cleaned = op.clean(
        {"a": np.float64(1.5), "b": [np.int64(2), float("nan")], "c": (float("inf"),)}
    )
    assert cleaned == {"a": 1.5, "b": [2, None], "c": [None]}
    assert type(cleaned["a"]) is float and type(cleaned["b"][0]) is int


def test_files_are_written_and_read_back(tmp_path: Path, result: dict[str, Any]) -> None:
    out = tmp_path / "nested" / "probe"
    op.write_result(result, out)
    assert sorted(item.name for item in out.iterdir()) == ["probe_result.json", "probe_result.md"]
    assert json.loads((out / "probe_result.json").read_text(encoding="utf-8")) == result
    text = (out / "probe_result.md").read_text(encoding="utf-8")
    for needle in ("put_write", "covered call", "TRADING-2567-20", "2001-02-28", "2010-12-31"):
        assert needle in text
    for scale in ("1", "0.9", "0.8"):
        assert f"| {scale} |" in text
    assert text.endswith("\n")


def qlh_default_root() -> Path:
    return op.qlh.DEFAULT_SNAPSHOT_ROOT


def _bench_data() -> BenchData:
    index = pd.bdate_range("2000-06-01", "2011-03-31", name="date")
    rng = np.random.default_rng(5)
    r = rng.normal(0.0004, 0.013, len(index))
    level = 100.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "etf_reference_return": r,
            "cash_return": np.full(len(index), 0.00005),
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": rng.uniform(18.0, 35.0, len(index)),
            "vxo": np.nan,
            "cash_rate_annual": 0.02,
        },
        index=index,
    )
    return from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "synthetic"})


def test_inputs_from_the_bench_use_the_series_the_bench_calls_ndx_cash_and_rate() -> None:
    data = _bench_data()
    inputs = op.ProxyInputs.from_bench(data)
    window = (data.inputs.index >= pd.Timestamp("2001-02-02")) & (
        data.inputs.index <= pd.Timestamp("2010-12-31")
    )
    pd.testing.assert_series_equal(inputs.ndx_return, data.inputs["ndx_return"][window])
    pd.testing.assert_series_equal(
        inputs.cash_return, data.returns["cash"][window], check_names=False
    )
    pd.testing.assert_series_equal(inputs.vxn, data.inputs["vxn"][window])
    pd.testing.assert_series_equal(
        inputs.rate, data.inputs["cash_rate_known"][window], check_names=False
    )
    assert inputs.vxn.index[0] == pd.Timestamp("2001-02-02")


def test_the_command_writes_the_result_and_refuses_a_bad_series(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    seen: dict[str, Any] = {}

    def fake_from_snapshot(directory: Path, *, qqq_series: str) -> BenchData:
        seen["directory"], seen["series"] = directory, qqq_series
        return _bench_data()

    monkeypatch.setattr(op, "from_snapshot", fake_from_snapshot)
    monkeypatch.setattr(
        op.qlh, "resolve_snapshot_dir", lambda root, snapshot: root / (snapshot or "LATEST")
    )
    runner = CliRunner()
    out = tmp_path / "out"
    done = runner.invoke(
        op.app,
        [
            "run",
            "--out",
            str(out),
            "--snapshot",
            "qlh-test",
            "--root",
            str(tmp_path),
            "--draws",
            "20",
        ],
    )
    assert done.exit_code == 0, done.output
    assert seen == {"directory": tmp_path / "qlh-test", "series": "actual"}
    written = json.loads((out / "probe_result.json").read_text(encoding="utf-8"))
    assert written["source"] == {"snapshot_id": "synthetic", "qqq_series": "actual"}
    assert written["window"]["cycles"] == 118
    assert "118 cycles" in done.output
    latest = runner.invoke(op.app, ["run", "--out", str(out), "--draws", "5"])
    assert latest.exit_code == 0, latest.output
    assert seen["directory"] == qlh_default_root() / "LATEST"
    bad = runner.invoke(op.app, ["run", "--out", str(out), "--qqq-series", "other"])
    assert bad.exit_code == 1 and "qqq-series" in bad.output


# --- units, calendar, premium volatility, parity and the same-exposure replica ----------------


def test_rates_are_decimal_per_year_and_a_percent_by_mistake_is_refused() -> None:
    for rate in (0.04, 0.5, -0.05):
        assert float(_inputs(rate=rate).rate.iloc[0]) == rate
    for rate in (4.0, 0.51, -0.0501):
        with pytest.raises(ValueError, match="rate"):
            _inputs(rate=rate)


def test_a_rate_from_the_bench_is_priced_as_it_is() -> None:
    inputs = op.ProxyInputs.from_bench(_bench_data())
    first = op.structure_path(inputs, "put_write", 1.0).cycles[0]
    assert float(inputs.rate.iloc[0]) == 0.02
    expected = _put(1.0, 0.02, first.vxn_start / 100.0, first.days / 252.0)
    assert first.premium == pytest.approx(expected, abs=1e-12)
    assert abs(expected - _put(1.0, 0.0002, first.vxn_start / 100.0, first.days / 252.0)) > 1e-4


def test_the_result_reports_the_mean_volatility_and_rate_it_used(result: dict[str, Any]) -> None:
    frames = _wide()
    inputs = op.ProxyInputs.from_frames(**frames)
    assert result["inputs"]["mean_rate"] == pytest.approx(0.02)
    assert result["inputs"]["mean_vxn"] == pytest.approx(float(inputs.vxn.mean()))


def test_the_premium_volatility_is_the_priced_volatility_minus_realized() -> None:
    inputs = _inputs(ndx=_alternating(_index()), vxn=25.0)
    realized = 100.0 * math.sqrt(252.0 * 0.0001)
    for scale in (1.0, 0.9, 0.8):
        cycles = op.structure_path(inputs, "put_write", scale).cycles
        assert [cycle.premium_vol for cycle in cycles] == pytest.approx(
            [25.0 * scale - realized] * 3
        )


def test_each_scale_reports_its_own_premium_volatility(result: dict[str, Any]) -> None:
    inputs = op.ProxyInputs.from_frames(**_wide())
    rows = result["structures"]["put_write"]
    means = {}
    for scale in op.SIGMA_SCALES:
        cycles = op.structure_path(inputs, "put_write", scale).cycles
        assert rows[f"{scale:g}"]["premium_vol"] == op.clean(op.premium_summary(cycles))
        means[scale] = rows[f"{scale:g}"]["premium_vol"]["mean_vol_points"]
    opening = float(
        np.mean([c.vxn_start for c in op.structure_path(inputs, "put_write", 1.0).cycles])
    )
    assert means[1.0] - means[0.9] == pytest.approx(0.1 * opening)
    assert means[0.9] - means[0.8] == pytest.approx(0.1 * opening)
    assert result["premium"] == rows["1"]["premium_vol"]


def test_a_month_end_with_a_holiday_still_counts_but_a_truncated_month_does_not() -> None:
    # Memorial Day 2010 is on May 31: the last trading date is May 28, three days before.
    holiday = pd.bdate_range("2010-03-01", "2010-05-28")
    assert op.cycle_bounds(holiday)[-1][1] == len(holiday) - 1
    near = pd.bdate_range("2010-03-01", "2010-05-27")
    assert op.cycle_bounds(near)[-1][1] == len(near) - 1
    short = pd.bdate_range("2010-03-01", "2010-05-26")
    assert [short[b].date().isoformat() for _, b in op.cycle_bounds(short)] == ["2010-04-30"]


def test_a_cycle_that_does_not_end_near_a_month_end_is_refused() -> None:
    four_days = _index().drop(pd.bdate_range("2003-03-28", "2003-03-31"))
    path = op.structure_path(_inputs(four_days), "put_write", 1.0)
    assert path.cycles[1].end == pd.Timestamp("2003-03-27")
    five_days = _index().drop(pd.bdate_range("2003-03-27", "2003-03-31"))
    with pytest.raises(ValueError, match="month end"):
        op.structure_path(_inputs(five_days), "put_write", 1.0)


def _sparse(interior: int) -> pd.DatetimeIndex:
    dates = [pd.Timestamp("2003-01-31"), *pd.bdate_range("2003-02-10", periods=interior)]
    return pd.DatetimeIndex([*dates, pd.Timestamp("2003-02-28")], name="date")


def test_a_cycle_needs_a_minimum_number_of_trading_days() -> None:
    enough = op.structure_path(_inputs(_sparse(11)), "put_write", 1.0)
    assert [cycle.days for cycle in enough.cycles] == [op.MIN_CYCLE_DAYS] == [12]
    with pytest.raises(ValueError, match="only 11 trading days"):
        op.structure_path(_inputs(_sparse(10)), "put_write", 1.0)
    thin = _index().drop(pd.bdate_range("2003-02-03", "2003-02-27"))
    with pytest.raises(ValueError, match="gap"):
        _inputs(thin)


def test_a_gap_in_the_dates_longer_than_ten_days_is_refused() -> None:
    ten = _index().drop(pd.bdate_range("2003-02-10", "2003-02-14"))  # Feb 7 to Feb 17
    assert float(np.diff(ten.to_numpy()).max() / np.timedelta64(1, "D")) == 10.0
    _inputs(ten)
    eleven = _index().drop(
        [pd.Timestamp("2003-02-07"), *pd.bdate_range("2003-02-10", "2003-02-14")]
    )
    with pytest.raises(ValueError, match="gap longer than 10 days"):
        _inputs(eleven)
    september = _index().drop(pd.bdate_range("2003-02-11", "2003-02-14"))  # a 7-day closure
    op.structure_path(_inputs(september), "put_write", 1.0)


def test_parity_is_exact_without_interest_and_reports_the_model_gap_with_it() -> None:
    index = _index()
    rng = np.random.default_rng(11)
    plain = _inputs(
        index, ndx=rng.normal(0.0, 0.015, len(index)), vxn=rng.uniform(15, 40, len(index))
    )
    quiet = op.run_probe(plain, LOADED, scales=(0.9,), draws=5)["parity"]["0.9"]
    assert quiet["max_abs_daily_difference"] < 1e-12
    assert abs(quiet["wealth_ratio_minus_one"]) < 1e-12
    inputs = _random_inputs()
    gap = op.run_probe(inputs, LOADED, scales=(0.9,), draws=5)["parity"]["0.9"]
    put = op.structure_path(inputs, "put_write", 0.9).returns.to_numpy()
    call = op.structure_path(inputs, "covered_call", 0.9).returns.to_numpy()
    assert gap["max_abs_daily_difference"] == pytest.approx(float(np.abs(put - call).max()))
    assert gap["wealth_ratio_minus_one"] == pytest.approx(
        float(np.prod(1.0 + put) / np.prod(1.0 + call) - 1.0)
    )
    assert gap["max_abs_daily_difference"] > 1e-6


def test_the_replica_holds_the_strategy_s_exposure_in_the_index_and_cash() -> None:
    ndx = _moves({"2003-02-03": 0.02})
    inputs = _inputs(ndx=ndx, cash=0.0001)
    path = op.structure_path(inputs, "put_write", 1.0)
    replica = op.replica_returns(inputs, path)
    first = _cdf(-_d1(1.0, 0.0, 0.2, 20 / 252.0))
    assert replica.iloc[0] == pytest.approx(first * 0.02 + (1.0 - first) * 0.0001, abs=1e-12)
    assert replica.index.equals(path.returns.index)
    second = float(path.exposure.iloc[1])
    assert replica.iloc[1] == pytest.approx((1.0 - second) * 0.0001, abs=1e-12)


def test_each_variant_reports_the_replica_and_the_differences(result: dict[str, Any]) -> None:
    inputs = op.ProxyInputs.from_frames(**_wide())
    for scale in op.SIGMA_SCALES:
        row = result["structures"]["put_write"][f"{scale:g}"]
        path = op.structure_path(inputs, "put_write", scale)
        daily = op.replica_returns(inputs, path).to_numpy()
        equity = np.concatenate([[1.0], np.cumprod(1.0 + daily)])
        drawdown = float((equity / np.maximum.accumulate(equity) - 1.0).min())
        tail = float(np.sort(daily)[: math.ceil(0.05 * len(daily))].mean())
        assert row["replica"]["max_drawdown"] == pytest.approx(drawdown, abs=1e-12)
        assert row["replica"]["cvar_5pct"] == pytest.approx(tail, abs=1e-12)
        assert row["replica"]["average_exposure"] == pytest.approx(float(path.exposure.mean()))
        assert set(row["strategy_vs_replica"]) == {
            "geometric_annual_return",
            "max_drawdown",
            "cvar_5pct",
        }
        for key in op.REPLICA_COMPARED:
            assert row["strategy_vs_replica"][key] == pytest.approx(
                row["strategy"][key] - row["replica"][key], abs=1e-12
            )
        assert row["geometric_annual_difference"] == pytest.approx(
            row["strategy"]["geometric_annual_return"] - row["control"]["geometric_annual_return"],
            abs=1e-12,
        )
    assert result["assumptions"]["paired_annualisation"].endswith("252_day_log_mean")


def test_the_markdown_shows_the_replica_the_parity_check_and_the_inputs(
    result: dict[str, Any],
) -> None:
    text = op.render_markdown(result)
    for needle in ("同日敞口对照", "核对：covered call", "平均利率 2.00%", "几何年化差"):
        assert needle in text
    assert "sigma_scale 0.9 同日敞口对照" in text


def test_the_first_and_the_last_cycle_dates_are_checked_too() -> None:
    first = _index().drop(pd.bdate_range("2003-01-27", "2003-01-31"))  # January ends on the 24th
    with pytest.raises(ValueError, match="2003-01-24 is not near a month end"):
        op.structure_path(_inputs(first), "put_write", 1.0)
    index = pd.bdate_range("2003-01-01", "2003-04-15", name="date")
    last = index.drop(pd.bdate_range("2003-03-26", "2003-03-31"))  # March ends on the 25th
    with pytest.raises(ValueError, match="2003-03-25 is not near a month end"):
        op.structure_path(_inputs(last), "put_write", 1.0)


def test_the_mean_rate_and_the_bootstrap_settings_are_reported() -> None:
    inputs = _random_inputs()
    assert float(inputs.rate.mean()) != pytest.approx(float(inputs.rate.median()), abs=1e-9)
    result = op.run_probe(inputs, LOADED, scales=(0.9,), draws=5)
    assert result["inputs"]["mean_rate"] == pytest.approx(float(inputs.rate.mean()))
    assumptions = result["assumptions"]
    assert assumptions["bootstrap_draws"] == 5
    assert assumptions["bootstrap_mean_block_days"] == (
        LOADED.settings.falsification.bootstrap_mean_block_days
    )
