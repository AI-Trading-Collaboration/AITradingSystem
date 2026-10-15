"""The US market on the bench, its trial window and the event-table episodes (TRADING-2565 trial
mode, S5a).

Written before the code; the hand-computed values come from the requirement, not from the code.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import RunSpec, load_settings, resolve_window, run_strategy
from ai_trading_system.research_bench.data import (
    ASSETS,
    INPUT_COLUMNS,
    BenchData,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.metrics import DAYS_PER_YEAR
from ai_trading_system.research_bench.settings import EpisodeSpec, Window, WindowRole
from ai_trading_system.research_data.signal_sources import parse_ken_french_csv

LOADED = load_settings()
SNAPSHOT = "sig-0123456789ab"
DECISION = "owner_decision:TRADING-2565:2026-10-15:trial_mode"
REGISTRY = PROJECT_ROOT / "config/research/research_window_registry.yaml"
POLICY = PROJECT_ROOT / "config/research/primary_research_window_policy.yaml"


def _factors(dates: list[str], excess: list[float], rf: list[float]) -> pd.DataFrame:
    index = pd.DatetimeIndex(pd.to_datetime(dates))
    count = len(index)
    return pd.DataFrame(
        {"Mkt-RF": excess, "SMB": [0.001] * count, "HML": [-0.002] * count, "RF": rf},
        index=index,
    )


SMALL = _factors(
    ["2001-01-02", "2001-01-03", "2001-01-04", "2001-01-05"],
    [0.01, -0.02, 0.005, 0.0],
    [0.0001, 0.0002, 0.0001, 0.00015],
)


# from_us_market ----------------------------------------------------------------------------------


def test_returns_are_the_market_and_the_risk_free_rate() -> None:
    data = from_us_market({"ff_factors_daily": SMALL}, snapshot_id=SNAPSHOT)

    assert isinstance(data, BenchData)
    assert list(data.returns.columns) == list(ASSETS)
    assert data.returns.index.equals(SMALL.index) and data.returns.index.name == "date"
    expected = pd.DataFrame(
        {
            "qqq": [0.0101, -0.0198, 0.0051, 0.00015],
            "cash": [0.0001, 0.0002, 0.0001, 0.00015],
            # Two and three times the market, the extra exposure financed at RF, no fees.
            "lev2": [0.0201, -0.0398, 0.0101, 0.00015],
            "lev3": [0.0301, -0.0598, 0.0151, 0.00015],
        },
        index=SMALL.index.rename("date"),
    )
    pd.testing.assert_frame_equal(data.returns, expected, check_exact=False, atol=1e-15)


def test_inputs_carry_the_market_its_level_and_the_known_rate() -> None:
    data = from_us_market({"ff_factors_daily": SMALL}, snapshot_id=SNAPSHOT)
    inputs = data.inputs

    assert list(inputs.columns) == list(INPUT_COLUMNS)
    assert inputs.index.equals(SMALL.index) and inputs.index.name == "date"
    assert all(inputs[column].dtype == np.float64 for column in inputs.columns)
    market = [0.0101, -0.0198, 0.0051, 0.00015]
    np.testing.assert_allclose(inputs["qqq_return"], market, rtol=0, atol=1e-15)
    np.testing.assert_allclose(inputs["ndx_return"], market, rtol=0, atol=1e-15)
    # The level is 1 on the first date and then compounds the market's returns.
    level = [1.0, 0.9802, 0.9802 * 1.0051, 0.9802 * 1.0051 * 1.00015]
    np.testing.assert_allclose(inputs["ndx_price"], level, rtol=1e-13)
    np.testing.assert_allclose(inputs["ndx_tr"], level, rtol=1e-13)
    for column in ("vix", "vxn", "vxo"):
        assert inputs[column].isna().all()
    # A rate dated t is known the next trading day; RF is a daily decimal, the input an annual one.
    known = inputs["cash_rate_known"].to_numpy()
    assert np.isnan(known[0])
    np.testing.assert_allclose(known[1:], [0.0252, 0.0504, 0.0252], rtol=1e-12)


def test_source_names_the_market_and_the_snapshot() -> None:
    data = from_us_market({"ff_factors_daily": SMALL}, snapshot_id=SNAPSHOT)

    assert data.source == {
        "market": "us_market",
        "snapshot_id": SNAPSHOT,
        "returns_source": "ff_factors_daily",
    }


def test_the_ken_french_percent_file_arrives_as_decimals() -> None:
    # The units come from the loader: the Ken French file is in percent, the parser divides by 100.
    text = "\n".join(
        [
            "This file was created by CMPT_ME_BEME_RETS_DAILY using the 202608 CRSP database.",
            "",
            ",Mkt-RF,SMB,HML,RF",
            "19261230,   1.20,   0.10,  -0.20,   0.010",
            "19261231,  -0.80,   0.05,   0.10,   0.010",
            "19270103,  -1.50,  -0.10,   0.30,   0.011",
            "",
            "Copyright 2026 Eugene F. Fama and Kenneth R. French",
        ]
    )
    factors = parse_ken_french_csv(text)
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id=SNAPSHOT)

    np.testing.assert_allclose(data.returns["qqq"], [0.0121, -0.0079, -0.01489], rtol=1e-12)
    np.testing.assert_allclose(data.returns["cash"], [0.0001, 0.0001, 0.00011], rtol=1e-12)


def test_other_sources_in_the_snapshot_are_not_read() -> None:
    other = pd.DataFrame({"value": [1.0]}, index=pd.DatetimeIndex(["1990-01-02"], name="date"))
    alone = from_us_market({"ff_factors_daily": SMALL}, snapshot_id=SNAPSHOT)
    together = from_us_market({"baa10y": other, "ff_factors_daily": SMALL}, snapshot_id=SNAPSHOT)

    pd.testing.assert_frame_equal(alone.returns, together.returns)
    pd.testing.assert_frame_equal(alone.inputs, together.inputs)


def _with(column: str, position: int, value: float) -> pd.DataFrame:
    frame = SMALL.copy()
    frame.loc[frame.index[position], column] = value
    return frame


@pytest.mark.parametrize(
    ("frames", "match"),
    [
        ({}, "ff_factors_daily"),
        ({"ff_factors_daily": SMALL.drop(columns="RF")}, "RF"),
        ({"ff_factors_daily": SMALL.drop(columns="Mkt-RF")}, "Mkt-RF"),
        ({"ff_factors_daily": _with("Mkt-RF", 2, np.nan)}, "missing"),
        ({"ff_factors_daily": _with("RF", 0, np.nan)}, "missing"),
        ({"ff_factors_daily": SMALL.iloc[[0, 2, 1, 3]]}, "increasing"),
        ({"ff_factors_daily": SMALL.iloc[[0, 1, 1, 3]]}, "unique"),
        ({"ff_factors_daily": _with("Mkt-RF", 1, 1.2)}, "percent"),
        ({"ff_factors_daily": _with("Mkt-RF", 1, -0.6)}, "percent"),
        ({"ff_factors_daily": _with("RF", 3, 0.02)}, "percent"),
        ({"ff_factors_daily": _with("RF", 3, 0.006)}, "percent"),
    ],
)
def test_bad_factor_frames_are_refused(frames: dict[str, pd.DataFrame], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        from_us_market(frames, snapshot_id=SNAPSHOT)


def test_extreme_but_real_days_are_accepted() -> None:
    # 1987-10-19: the market fell about 17%; T-bill rates near 16% a year are about 0.06% a day.
    frame = _with("Mkt-RF", 1, -0.45)
    frame.loc[frame.index[2], "RF"] = 0.0045
    data = from_us_market({"ff_factors_daily": frame}, snapshot_id=SNAPSHOT)

    assert data.returns["qqq"].iloc[1] == pytest.approx(-0.4498)


def test_signal_sources_join_the_market_calendar() -> None:
    data = from_us_market({"ff_factors_daily": SMALL}, snapshot_id=SNAPSHOT)
    joined = with_signal_sources(
        data, {"ff_factors_daily": SMALL}, {"ff_factors_daily": 0}, snapshot_id=SNAPSHOT
    )

    assert joined.source == {**data.source, "signal_snapshot_id": SNAPSHOT}
    np.testing.assert_allclose(
        joined.inputs["source:ff_factors_daily:Mkt-RF"], SMALL["Mkt-RF"], rtol=0
    )
    pd.testing.assert_frame_equal(joined.returns, data.returns)


# The trial window --------------------------------------------------------------------------------


def _yaml(path: Any) -> dict[str, Any]:
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def test_the_trial_type_has_a_development_window_only() -> None:
    assert resolve_window(LOADED, "us_market_trial", "development") == Window(
        window_id="us_market_1929_2010",
        role="development",
        start=date(1929, 1, 2),
        end=date(2010, 12, 31),
    )
    closed: tuple[WindowRole, ...] = ("validation", "consistency_check")
    for role in closed:
        with pytest.raises(KeyError, match="us_market_trial has no"):
            resolve_window(LOADED, "us_market_trial", role)

    roles = _yaml(POLICY)["question_windows"]["us_market_trial"]
    assert roles["owner_decision"] == DECISION
    assert roles["development"] == "us_market_1929_2010"
    assert not {"validation", "consistency_check", "final_confirmation"} & set(roles)


def test_the_trial_window_ends_with_the_qqq_development_window() -> None:
    windows = _yaml(REGISTRY)["windows"]
    entry = windows["us_market_1929_2010"]
    qqq = windows[_yaml(POLICY)["question_windows"]["qqq_direction"]["development"]]

    assert entry["role"] == "development"
    assert entry["question_types"] == ["us_market_trial"]
    assert entry["start"] == date(1929, 1, 2)
    assert entry["end"] == qqq["end"] == date(2010, 12, 31)
    assert "validation_read" in entry["blocked_usage"]
    users = [
        name
        for name, item in windows.items()
        if "us_market_trial" in item.get("question_types", [])
    ]
    assert users == ["us_market_1929_2010"]


# The bench on the US market ----------------------------------------------------------------------


def _long_market() -> BenchData:
    index = pd.bdate_range("1928-01-02", "2011-03-31", name="date")
    rng = np.random.default_rng(7)
    excess = rng.normal(0.0003, 0.011, len(index))
    rf = np.full(len(index), 0.0001)
    factors = pd.DataFrame({"Mkt-RF": excess, "SMB": 0.0, "HML": 0.0, "RF": rf}, index=index)
    return from_us_market({"ff_factors_daily": factors}, snapshot_id=SNAPSHOT)


MARKET = _long_market()
SPEC = RunSpec(
    question_type="us_market_trial",
    strategy_id="half",
    params={"level": 0.5},
    window_role="development",
)
CRASH = (EpisodeSpec(id="crash_1929", start="1929-10", end="1929-11"),)


def _half(inputs: pd.DataFrame, params: Mapping[str, Any]) -> pd.Series:
    return pd.Series(float(params["level"]), index=inputs.index)


@pytest.fixture(scope="module")
def runs() -> dict[str, Any]:
    return {
        "default": run_strategy(MARKET, _half, SPEC, LOADED),
        "crash": run_strategy(MARKET, _half, SPEC, LOADED, episodes=CRASH),
        "none": run_strategy(MARKET, _half, SPEC, LOADED, episodes=()),
    }


def test_a_run_on_the_us_market_reads_its_window_by_hand(runs: dict[str, Any]) -> None:
    result = runs["default"]
    market = MARKET.returns["qqq"]
    inside = market.loc["1929-01-03":"2010-12-31"]
    years = (pd.Timestamp("2010-12-31") - pd.Timestamp("1929-01-02")).days / DAYS_PER_YEAR
    hold = result.benchmarks["buy_and_hold_qqq"]
    cash = result.benchmarks["cash"]

    assert result.window == {
        "window_id": "us_market_1929_2010",
        "role": "development",
        "start": "1929-01-02",
        "end": "2010-12-31",
    }
    assert result.source == MARKET.source
    assert (hold["first_return_date"], hold["last_return_date"]) == ("1929-01-03", "2010-12-31")
    assert hold["return_days"] == len(inside)
    expected = float(np.prod(1.0 + inside.to_numpy())) ** (1.0 / years) - 1.0
    assert hold["geometric_annual_return"] == pytest.approx(expected, rel=1e-9)
    assert cash["geometric_annual_return"] == pytest.approx(
        1.0001 ** (len(inside) / years) - 1.0, rel=1e-9
    )
    assert result.strategy["average_exposure"] == pytest.approx(0.5, abs=1e-12)


def test_episodes_choose_the_event_table_rows(runs: dict[str, Any]) -> None:
    default, crash, none = runs["default"], runs["crash"], runs["none"]
    market = MARKET.returns["qqq"]
    days = market.loc["1929-10-01":"1929-11-30"]

    # Without episodes the bench settings' episodes are used: two of them lie inside 1929-2010.
    assert set(default.events["episodes"]) == {"dotcom_bear", "global_financial_crisis"}
    assert set(crash.events["episodes"]) == {"crash_1929"}
    assert none.events["episodes"] == {}
    row = crash.events["episodes"]["crash_1929"]
    assert row["days"] == len(days)
    # A constant half exposure is never de-risked, so it misses the whole episode's move.
    assert row["derisked_share"] == 0.0
    assert row["missed_loss"] == pytest.approx(float(np.prod(1.0 + days.to_numpy()) - 1.0))
    assert row["exit_lag_days"] is None
    # Everything else in the event table, and the result itself, do not depend on the episodes.
    for result in (crash, none):
        assert {k: v for k, v in result.events.items() if k != "episodes"} == {
            k: v for k, v in default.events.items() if k != "episodes"
        }
        assert result.fingerprint == default.fingerprint
        assert result.strategy == default.strategy
