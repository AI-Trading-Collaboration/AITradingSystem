"""Tests for the Treasury stand-in check before T0008 (TRADING-2565-40).

Written before the code (owner decision ``owner_decision:TRADING-2565:2026-10-21:treasury_1929``).
The check puts the DGS10 Treasury and the Shiller one side by side on us_market_1962_2010 and
applies the use rule fixed before any run. Every test uses synthetic data; nothing reads a
snapshot.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import segments as seg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    SHILLER_TREASURY_RESERVE,
    TREASURY_RESERVE,
    BenchData,
    from_daily_frame,
    monthly_treasury_returns,
    par_bond_price,
    treasury_returns,
)
from ai_trading_system.research_bench.fixed_strategy import fixed_exposure
from ai_trading_system.research_bench.run import RunSpec, run_strategy
from ai_trading_system.research_bench.settings import EpisodeSpec, load_settings, resolve_window
from ai_trading_system.research_probes import treasury_stand_in as ts

LOADED = load_settings()
WINDOW = resolve_window(LOADED, "us_market_bond_trial", "development")


def _market() -> BenchData:
    index = pd.bdate_range("1961-01-02", "2011-06-30", name="date")
    rng = np.random.default_rng(5)
    months = index.to_period("M")
    drift = np.where((months.year % 7 == 3) & (months.month > 4), -0.002, 4e-4)
    r = drift + rng.normal(0.0, 0.009, len(index))
    level = 100.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "etf_reference_return": r,
            "cash_return": 0.0002,
            "lev2_return": 2 * r,
            "lev3_return": 3 * r,
            "ndx_price": level,
            "ndx_tr": level,
            "vix": 20.0,
            "vxn": 25.0,
            "vxo": 21.0,
            "cash_rate_annual": 0.05,
        },
        index=index,
    )
    return from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "synthetic"})


MARKET = _market()
CALENDAR = pd.DatetimeIndex(MARKET.returns.index)
DECLINES = tuple(
    EpisodeSpec(id=s.id, start=s.start, end=s.end)
    for s in seg.declines(seg.window_returns(MARKET.returns["qqq"], WINDOW))
)


def _daily_yields(seed: int) -> pd.Series:
    steps = np.random.default_rng(seed).normal(0.0, 0.05, len(CALENDAR))
    return pd.Series(np.clip(6.0 + np.cumsum(steps), 1.0, 15.0), index=CALENDAR)


def _monthly(daily: pd.Series) -> pd.Series:
    """Monthly averages dated at month start, as Shiller's GS10 column."""

    means = daily.groupby(pd.DatetimeIndex(daily.index).to_period("M")).mean()
    return pd.Series(means.to_numpy(), index=means.index.to_timestamp(how="start"))


def _uncut(shiller_seed: int | None) -> tuple[BenchData, pd.Series]:
    daily = _daily_yields(21)
    monthly = _monthly(daily if shiller_seed is None else _daily_yields(shiller_seed))
    data = BenchData(
        returns=MARKET.returns,
        inputs=MARKET.inputs,
        source=MARKET.source,
        reserves={
            TREASURY_RESERVE: treasury_returns(daily, CALENDAR),
            SHILLER_TREASURY_RESERVE: monthly_treasury_returns(monthly, CALENDAR),
        },
    )
    # Interpolated from yearly values that go up and down by 0.4: every second difference inside
    # a year is zero, every first difference is 0.4 / 12.
    yearly = 3.0 + 0.2 * np.where(np.arange(25) % 2 == 0, 1.0, -1.0)
    early = pd.Series(
        np.interp(np.arange(24 * 12), np.arange(25) * 12, yearly),
        index=pd.date_range("1929-01-01", periods=24 * 12, freq="MS"),
    )
    return data, pd.concat([early, monthly]).sort_index()


def _inputs(shiller_seed: int | None) -> tuple[BenchData, Any, Any, Any, pd.Series, pd.Series]:
    data, yields = _uncut(shiller_seed)
    return ts.cut(data), LOADED, WINDOW, DECLINES, yields, _daily_yields(21)


@pytest.fixture(scope="module")
def close() -> dict[str, Any]:
    return ts.stand_in(*_inputs(None))


def test_the_check_is_fixed_before_the_run() -> None:
    assert (ts.RUN_ID, ts.OWNER_DECISION, ts.PLAN_ID) == (
        "TRADING-2565-40",
        "owner_decision:TRADING-2565:2026-10-21:treasury_1929",
        "T0008",
    )
    assert (ts.MARKET, ts.QUESTION_TYPE, ts.WEIGHT) == ("us_market", "us_market_bond_trial", 0.6)
    assert ts.RESERVES == ("treasury_10y", "treasury_10y_shiller")
    assert (ts.MIN_CORRELATION, ts.MIN_AGREE, ts.FLAT_STEP) == (0.90, 12, 0.011)
    assert ts.PERIODS == {"1929_1952": ("1929-01", "1952-12"), "1962_2010": ("1962-01", "2010-12")}
    assert WINDOW.window_id == "us_market_1962_2010"


def test_each_reserve_gets_the_ruler_of_a_60_40_mix(close: dict[str, Any]) -> None:
    assert close["window"] == {
        "window_id": "us_market_1962_2010",
        "start": "1962-01-02",
        "end": "2010-12-31",
    }
    assert close["verdict"] == ts.use_rule(close["annual"]["correlation"], close["agree"])
    for reserve in ts.RESERVES:
        spec = RunSpec(
            question_type="us_market_bond_trial",
            strategy_id=f"mix60_{reserve}",
            params={"exposure": 0.6},
            window_role="development",
            rebalance=ts.REBALANCE,
            reserve=reserve,
        )
        direct = run_strategy(
            ts.cut(_inputs(None)[0]),
            fixed_exposure,
            spec,
            LOADED,
            declines=DECLINES,
            portfolio_weight=0.6,
        )
        assert close["portfolio"][reserve] == direct.portfolio
    assert [row["id"] for row in close["declines"]] == [d.id for d in DECLINES]


def test_close_reserves_agree_and_are_usable(close: dict[str, Any]) -> None:
    rows = close["declines"]
    for row in rows:
        excess = row["reserve_excess"]
        assert row["agree"] == (
            np.sign(excess[TREASURY_RESERVE]) == np.sign(excess[SHILLER_TREASURY_RESERVE])
        )
        assert set(row["ratio"]) == set(ts.RESERVES)
    assert close["agree"] == sum(row["agree"] for row in rows)
    monthly, annual = close["monthly"], close["annual"]
    assert monthly["months"] == 588  # 1962-01..2010-12
    # Monthly averages smear a month's move over two: the monthly correlation of a random-walk
    # rate is low and decides nothing; calendar years are close.
    assert 0.5 < monthly["correlation"] < 0.8
    assert set(monthly["volatility"]) == set(ts.RESERVES)
    assert annual["years"] == 49
    assert annual["correlation"] > 0.95
    assert close["verdict"] == ("usable" if close["agree"] >= 12 else "diverges")
    # Same basis: the synthetic Shiller rate is DGS10's monthly average, so priced by the same
    # monthly formula the two are one series (printed only).
    assert close["same_basis"] == {
        "months": 588,
        "correlation": pytest.approx(1.0, abs=1e-12),
        "yield_gap_mean": 0.0,
        "yield_gap_max": 0.0,
    }


def test_an_unrelated_rate_diverges(capsys: pytest.CaptureFixture[str]) -> None:
    result = ts.stand_in(*_inputs(99))
    assert result["annual"]["correlation"] < 0.5
    assert result["verdict"] == "diverges"
    basis = result["same_basis"]
    assert basis["months"] == 588 and basis["correlation"] < 0.5
    gaps = (_monthly(_daily_yields(99)) - _monthly(_daily_yields(21))).loc[
        "1962-01-01":"2010-12-01"
    ]
    assert basis["yield_gap_mean"] == pytest.approx(float(gaps.abs().mean()), abs=1e-12)
    assert basis["yield_gap_max"] == pytest.approx(float(gaps.abs().max()), abs=1e-12)
    rates = pd.DataFrame({"s": _monthly(_daily_yields(99)), "d": _monthly(_daily_yields(21))}) / 100
    previous = rates.shift(1)
    priced = (par_bond_price(previous, rates) - 1 + previous / 12).loc["1962-01-01":"2010-12-01"]
    assert basis["correlation"] == pytest.approx(float(priced["s"].corr(priced["d"])), abs=1e-12)
    ts.print_summary(result)
    assert (
        f"同口径（DGS10 月均按同一公式）588 个月，月收益相关 {basis['correlation']:.3f}；"
        f"利率差平均 {basis['yield_gap_mean']:.3f}、最大 {basis['yield_gap_max']:.3f} 个百分点"
        "（只印不判）"
    ) in capsys.readouterr().out


def test_the_same_basis_prices_each_month_from_the_month_before() -> None:
    rates = pd.Series(
        [4.0, 4.5, np.nan, 4.2], index=pd.period_range("1970-01", periods=4, freq="M")
    )
    out = ts.monthly_bond_returns(rates)
    assert np.isnan(out.iloc[0]) and np.isnan(out.iloc[3])
    assert out.iloc[1] == pytest.approx(0.04 / 12 + par_bond_price(0.04, 0.045) - 1, abs=1e-15)
    daily = pd.Series(
        [4.0, 4.2, np.nan, 5.0],
        index=pd.to_datetime(["1970-01-02", "1970-01-30", "1970-02-02", "1970-02-27"]),
    )
    assert ts.month_means(daily).to_dict() == {
        pd.Period("1970-01", freq="M"): pytest.approx(4.1),
        pd.Period("1970-02", freq="M"): 5.0,
    }


@pytest.mark.parametrize(
    ("annual", "agree", "verdict"),
    [(0.89, 15, "diverges"), (0.95, 11, "diverges"), (0.90, 12, "usable"), (0.99, 15, "usable")],
)
def test_the_use_rule_needs_both_lines(annual: float, agree: int, verdict: str) -> None:
    assert ts.use_rule(annual, agree) == verdict


def test_the_rate_shape_names_an_interpolated_period(close: dict[str, Any]) -> None:
    shape = close["rate_shape"]
    assert set(shape) == {"1929_1952", "1962_2010"}
    assert shape["1929_1952"]["months"] == 288
    # Interpolated within each year: only the 23 Januaries inside the period bend.
    assert shape["1929_1952"]["flat_share"] == pytest.approx(263 / 286, abs=1e-12)
    assert shape["1929_1952"]["change_std"] == pytest.approx(0.4 / 12, abs=1e-3)
    assert shape["1962_2010"]["months"] == 588
    assert shape["1962_2010"]["flat_share"] < 0.3
    monthly = _monthly(_daily_yields(21))
    changes = monthly.loc["1962-01-01":"2010-12-01"].diff()
    assert shape["1962_2010"]["change_std"] == pytest.approx(float(changes.std()), abs=1e-12)


def test_the_period_returns_compound_the_window_s_days() -> None:
    daily = pd.Series(0.001, index=CALENDAR)
    assert list(ts.period_returns(daily, WINDOW, "Y").index.astype(str))[::48] == ["1962", "2010"]
    monthly = ts.period_returns(daily, WINDOW, "M")
    assert str(monthly.index[0]) == "1962-01" and str(monthly.index[-1]) == "2010-12"
    january = CALENDAR[
        (CALENDAR >= pd.Timestamp(WINDOW.start)) & (CALENDAR.to_period("M") == "1962-01")
    ]
    assert monthly.iloc[0] == pytest.approx(1.001 ** len(january) - 1.0, abs=1e-14)


def test_the_cut_keeps_the_reserves_and_nothing_after_2010() -> None:
    data = _inputs(None)[0]
    assert pd.DatetimeIndex(data.returns.index).max() <= pd.Timestamp("2010-12-31")
    for series in data.reserves.values():
        assert series.index.equals(data.returns.index)


def test_the_cli_writes_once_and_prints_the_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ts, "load_inputs", lambda root: _inputs(None))
    out = tmp_path.joinpath("check")
    first = CliRunner().invoke(ts.app, ["check", "--out", str(out)])
    assert first.exit_code == 0, first.output
    written = json.loads(out.joinpath(ts.RESULT_JSON).read_text(encoding="utf-8"))
    assert written["verdict"] in ("usable", "diverges")
    assert (written["production_effect"], written["broker_action"]) == ("none", "none")
    assert "treasury_stand_in: 方向一致" in first.output
    assert "同口径（DGS10 月均按同一公式）588 个月，月收益相关 1.000；" in first.output
    assert f"判定 {written['verdict']}" in first.output
    again = CliRunner().invoke(ts.app, ["check", "--out", str(out)])
    assert again.exit_code == 1
    assert "已有结果，拒绝覆盖" in again.output


def test_the_cli_stops_on_a_load_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(root: Path) -> Any:
        raise ValueError("data have no treasury_10y_shiller")

    monkeypatch.setattr(ts, "load_inputs", fail)
    result = CliRunner().invoke(ts.app, ["check", "--out", str(tmp_path.joinpath("x"))])
    assert result.exit_code == 1
    assert "数据载入失败：data have no treasury_10y_shiller" in result.output
    assert not tmp_path.joinpath("x").exists()


def test_the_inputs_are_the_plan_s_data_up_to_2010(monkeypatch: pytest.MonkeyPatch) -> None:
    data, yields = _uncut(None)
    dgs10 = _daily_yields(21)
    seen: dict[str, Any] = {}

    def market_data(market: str, gate: Any, plan: Any, **kwargs: Any) -> BenchData:
        seen.update(market=market, plan=plan.trial_id, root=kwargs["snapshot_root"])
        return held

    def load_snapshot(root: Path, snapshot_id: str) -> Any:
        seen["signal_snapshot"] = snapshot_id
        frames = {"shiller_gs10": pd.DataFrame({"value": yields}), "dgs10": dgs10.to_frame("value")}
        return SimpleNamespace(frames=frames)

    monkeypatch.setattr(ts.tr, "market_data", market_data)
    monkeypatch.setattr(ts.signal_sources, "load_snapshot", load_snapshot)
    held = data
    cut, loaded, window, declines, rate, daily = ts.load_inputs(Path("snapshots"))
    assert seen == {
        "market": "us_market",
        "plan": "T0008",
        "root": Path("snapshots"),
        "signal_snapshot": "sig-ed9e988ffe7c",
    }
    gate = tr.load_gate(PROJECT_ROOT / "research/_trials/prereg.yaml").markets["us_market"]
    assert window.window_id == "us_market_1962_2010"
    assert declines == gate.decline_list("us_market_1962_2010") and len(declines) == 15
    assert pd.DatetimeIndex(cut.returns.index).max() == pd.Timestamp("2010-12-31")
    for series in cut.reserves.values():
        assert series.index.equals(cut.returns.index)
    assert rate.index.min() == yields.index.min()
    assert rate.index.max() == pd.Timestamp("2010-12-01")
    assert daily.index.min() == dgs10.index.min()
    assert daily.index.max() == pd.Timestamp("2010-12-31")

    held = replace(data, reserves={TREASURY_RESERVE: data.reserves[TREASURY_RESERVE]})
    with pytest.raises(ValueError, match="data have no treasury_10y_shiller"):
        ts.load_inputs(Path("snapshots"))
