from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import load_settings
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.metrics import _episodes
from ai_trading_system.research_bench.prereg import Rule, evaluate_rules
from ai_trading_system.research_bench.run import RunSpec, run_strategy
from ai_trading_system.research_bench.settings import EpisodeSpec, Window
from ai_trading_system.research_bench.simulate import Rebalance, constant_targets, simulate
from ai_trading_system.research_probes import backfill_check as bc
from ai_trading_system.research_probes import gate_false_pass as fp
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import slow_bear_check as sb

pytestmark = pytest.mark.filterwarnings("ignore:invalid value encountered in log1p")

FULL = load_settings()
# Fewer placebo and bootstrap draws than the bench uses: the same code paths, faster.
LOADED = replace(
    FULL,
    settings=FULL.settings.model_copy(
        update={
            "falsification": FULL.settings.falsification.model_copy(
                update={"placebo_draws": 10, "bootstrap_draws": 200}
            )
        }
    ),
)
ROOT_DIR = lg.research_root(LOADED)
PREREG = PROJECT_ROOT / "research" / "_trials" / "prereg.yaml"
GATE = tr.load_gate(PREREG)
# Short windows keep the whole-run tests fast; the real ones are probe_windows'.
WINDOWS = {
    "us_1929": Window("us_test_1929", "development", date(1929, 1, 2), date(1936, 12, 31)),
    "us_1963": Window("us_test_1963", "development", date(1931, 1, 2), date(1936, 12, 31)),
    "us_1962": Window("us_test_1962", "development", date(1930, 1, 2), date(1936, 12, 31)),
    "qqq": Window("qqq_test", "development", date(1999, 3, 10), date(2004, 12, 31)),
}
SEEDS, CONTROL_SEEDS = 2, 1


def _drift(index: pd.DatetimeIndex, bears: list[tuple[str, str, float]], up: float) -> np.ndarray:
    months = index.to_period("M")
    drift = np.full(len(index), up)
    for start, end, value in bears:
        inside = (months >= pd.Period(start, freq="M")) & (months <= pd.Period(end, freq="M"))
        drift[np.asarray(inside)] = value
    return drift


def _us_data(start: str = "1926-07-01", end: str = "1936-12-31", seed: int = 5) -> BenchData:
    """Ken French-like daily market returns with a slow bear (1929-10..1932-06), a fast recovery
    and a fast drop (1935-03..1935-04)."""

    index = pd.bdate_range(start, end, name="date")
    rng = np.random.default_rng(seed)
    moves = [
        ("1929-10", "1932-06", -0.0015),
        ("1932-07", "1934-12", 0.003),
        ("1935-03", "1935-04", -0.008),
        ("1935-05", "1935-12", 0.003),
    ]
    drift = _drift(index, moves, 6e-4)
    factors = pd.DataFrame(
        {
            "Mkt-RF": drift + rng.normal(0.0, 0.005, len(index)) - 0.0001,
            "SMB": 0.0,
            "HML": 0.0,
            "RF": 0.0001,
        },
        index=index,
    )
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id="sig-test")
    return with_signal_sources(data, {}, {}, snapshot_id="sig-test")


def _qqq_data(seed: int = 3) -> BenchData:
    index = pd.bdate_range("1994-01-03", "2010-12-31", name="date")
    rng = np.random.default_rng(seed)
    moves = [
        ("2000-04", "2002-09", -0.0015),
        ("2002-10", "2004-12", 0.003),
        ("2007-11", "2008-11", -0.002),
    ]
    r = _drift(index, moves, 6e-4) + rng.normal(0.0, 0.008, len(index))
    level = 1000.0 * np.cumprod(1.0 + r)
    daily = pd.DataFrame(
        {
            "qqq_proxy_return": r,
            "etf_reference_return": r,
            "cash_return": 0.0001,
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
    data = from_daily_frame(daily, qqq_series="actual", source={"snapshot_id": "qlh-test"})
    return with_signal_sources(data, {}, {}, snapshot_id="sig-test")


US = _us_data()
QQQ = _qqq_data()
DATA = {"us_market": US, "qqq": QQQ}
NULL_DATA = {market: gp.cut(data) for market, data in DATA.items()}
FIXED = sb.Item(
    id="T0002",
    source="trial",
    label="T0002 fixed",
    function="fixed_exposure",
    params={"exposure": 0.6},
    rebalance=Rebalance(schedule="month_end"),
    reserve="cash",
    primary="fixed",
)


ITEMS = [next(item for item in sb.probe_items(ROOT_DIR) if item.id == "fast_slow"), FIXED]


def _items() -> list[sb.Item]:
    return list(ITEMS)


@pytest.fixture(scope="module")
def probe_run(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, Any], Path]:
    journal = tmp_path_factory.mktemp("slow_bear") / sb.JOURNAL_FILE
    result = sb.run_probe(
        DATA,
        NULL_DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        items=_items(),
        sources={},
        seeds=SEEDS,
        control_seeds=CONTROL_SEEDS,
        windows=WINDOWS,
        journal=journal,
    )
    return result, journal


@pytest.fixture(scope="module")
def probe(probe_run: tuple[dict[str, Any], Path]) -> dict[str, Any]:
    return probe_run[0]


def _level(values: list[float], start: str = "2000-01") -> pd.Series:
    return pd.Series(values, index=pd.period_range(start, periods=len(values), freq="M"))


def _drops(values: list[float], start: str = "2000-01") -> list[tuple[str, str, str, int]]:
    return [
        (item.kind, str(item.peak), str(item.trough), item.months)
        for item in sb.drop_segments(_level(values, start))
    ]


# --- the fixed design ----------------------------------------------------------------------------


def test_the_constants_are_the_requirements() -> None:
    assert sb.RUN_ID == "TRADING-2565-34"
    assert sb.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-21:research_loop_p2"
    assert (sb.DROP, sb.SLOW_MONTHS, sb.LONG_BULL_MONTHS, sb.MIN_MEDIAN_SEGMENTS) == (
        0.20,
        6,
        36,
        4,
    )
    assert (sb.SB1, sb.SB2, sb.TIGHT_SB1, sb.TIGHT_NONPOSITIVE) == (0.10, 0.75, 0.15, 1)
    assert (sb.FIDELITY_TOLERANCE, sb.COST_BPS, sb.USABLE_LINE) == (1e-9, 25.0, 0.025)
    assert sb.TRIALS == tuple(f"T000{n}" for n in range(1, 8))
    assert sb.POSITIVE_CONTROLS == ("fast_slow", "trend4", "slow_only")
    assert sb.WINDOW_KEYS == ("us_1929", "us_1963", "us_1962", "qqq")
    assert sb.FALSE_PASS_WINDOWS == ("us_1929", "us_1963", "qqq")
    assert sb.BOOTSTRAP_WINDOWS == ("us_1963", "qqq")
    assert (sb.JOURNAL_FILE, sb.RESULT_JSON, sb.RESULT_MD) == (
        "slow_bear_rows.jsonl",
        "slow_bear_result.json",
        "slow_bear_result.md",
    )


def test_the_inferred_slow_bears_are_the_requirements_table() -> None:
    assert sb.INFERRED["us_1929"] == (
        ("1929-09", "1932-06"),
        ("1937-03", "1938-03"),
        ("1938-11", "1942-04"),
        ("1946-05", "1949-06"),
        ("1961-12", "1962-06"),
        ("1968-11", "1970-06"),
        ("1973-01", "1974-09"),
        ("1980-11", "1982-07"),
        ("2000-08", "2002-09"),
        ("2007-10", "2009-02"),
    )
    assert sb.INFERRED["us_1963"] == sb.INFERRED["us_1929"][5:]
    assert sb.INFERRED["qqq"] == (("2000-03", "2002-09"), ("2007-10", "2008-11"))
    assert "us_1962" not in sb.INFERRED


def test_the_expectations_are_the_ones_written_before_the_run() -> None:
    expected = {
        "fast_slow": (True, 0.7, True, 0.8, True, 0.56),
        "trend4": (True, 0.7, True, 0.8, True, 0.56),
        "slow_only": (True, 0.6, True, 0.85, True, 0.5),
        "rv21": (True, 0.9, True, 0.85, True, 0.77),
        "fast_slow_weekly": (True, 0.75, True, 0.8, True, 0.6),
        "rv21_weekly": (True, 0.9, True, 0.85, True, 0.77),
        "T0001": (False, 0.8, False, 0.7, False, None),
        "T0002": (False, 0.9, False, 0.9, False, None),
        "T0003": (False, 0.8, True, 0.6, False, None),
        "T0004": (True, 0.9, True, 0.9, True, 0.81),
        "T0005": (True, 0.65, True, 0.75, True, 0.5),
        "T0006": (False, 0.85, None, 0.5, False, None),
        "T0007": (False, 0.6, None, 0.5, False, None),
    }
    assert {
        key: (
            e.stage1,
            e.stage1_probability,
            e.stage2,
            e.stage2_probability,
            e.overall,
            e.overall_probability,
        )
        for key, e in sb.EXPECTATIONS.items()
    } == expected


def test_the_windows_are_the_trials_and_the_1963_start() -> None:
    windows = sb.probe_windows(LOADED, GATE)
    assert list(windows) == list(sb.WINDOW_KEYS)
    assert {key: (w.window_id, w.start, w.end) for key, w in windows.items()} == {
        "us_1929": ("us_market_1929_2010", date(1929, 1, 2), date(2010, 12, 31)),
        "us_1963": ("us_market_1963_2010", date(1963, 1, 2), date(2010, 12, 31)),
        "us_1962": ("us_market_1962_2010", date(1962, 1, 2), date(2010, 12, 31)),
        "qqq": ("qqq_long_history_development", date(1999, 3, 10), date(2010, 12, 31)),
    }
    assert all(w.role == "development" for w in windows.values())
    assert sb.WINDOW_MARKETS == {
        "us_1929": "us_market",
        "us_1963": "us_market",
        "us_1962": "us_market",
        "qqq": "qqq",
    }


def test_the_items_are_the_backfill_configs_and_the_trial_primaries() -> None:
    items = sb.probe_items(ROOT_DIR)
    assert [item.id for item in items] == [c.id for c in bc.CONFIGS] + list(sb.TRIALS)
    for config, item in zip(bc.CONFIGS, items, strict=False):
        entry = bc.source_entry(ROOT_DIR, config)
        assert (item.source, item.label, item.function) == (
            "backfill",
            f"{config.question} {config.strategy}",
            "signal_exposure",
        )
        assert item.params == entry.params and item.rebalance == entry.rebalance.to_rebalance()
        assert item.reserve == "cash" and item.primary is None
    for item in items[len(bc.CONFIGS) :]:
        _, plan = tr.load_trial(ROOT_DIR, item.id)
        primary = plan.config(plan.primary)
        assert (item.source, item.primary, item.label) == (
            "trial",
            plan.primary,
            f"{item.id} {plan.primary}",
        )
        assert (item.function, item.params, item.reserve) == (
            primary.function,
            primary.params,
            primary.reserve,
        )
        assert item.rebalance == primary.rebalance.to_rebalance()
    t0002 = items[len(bc.CONFIGS) + 1]
    assert (t0002.function, t0002.reserve) == ("fixed_exposure", "treasury_10y")


def test_each_item_uses_its_trials_windows() -> None:
    for item in sb.probe_items(ROOT_DIR):
        first = "us_1962" if item.id == "T0002" else "us_1929"
        assert sb.stage_windows(item, GATE) == {"stage1": first, "stage2": "qqq"}


def test_the_common_rules_are_the_gates_rules_without_the_left_tail() -> None:
    assert [r.metric for r in sb.common_rules(GATE, "us_market")] == [
        "comparison.max_drawdown",
        "comparison.geometric_annual_return",
        "cost_25.geometric_annual_return",
    ]
    assert [r.metric for r in sb.common_rules(GATE, "qqq")] == [
        "comparison.geometric_annual_return"
    ]
    assert all(r in GATE.markets["us_market"].rules for r in sb.common_rules(GATE, "us_market"))


# --- segments ------------------------------------------------------------------------------------


def test_six_months_from_peak_to_trough_is_a_slow_bear_five_a_fast_drop() -> None:
    six = [1.0, 1.1, 1.0, 0.9, 0.85, 0.8, 0.75, 0.7, 0.8, 0.9, 1.0, 1.2]
    assert _drops(six) == [("slow_bear", "2000-02", "2000-08", 6)]
    five = [1.0, 1.1, 1.0, 0.95, 0.9, 0.85, 0.7, 0.9, 1.2]
    assert _drops(five) == [("fast_drop", "2000-02", "2000-07", 5)]
    item = sb.drop_segments(_level(six))[0]
    assert item.id == "slow_bear_200002"
    assert (item.start, item.end) == ("2000-03", "2000-08")
    assert item.change == pytest.approx(0.7 / 1.1 - 1.0)


def test_a_drop_of_exactly_twenty_percent_opens_a_segment() -> None:
    assert _drops([1.0, 0.8, 1.0]) == [("fast_drop", "2000-01", "2000-02", 1)]
    assert _drops([1.25, 1.0, 1.3]) == [("fast_drop", "2000-01", "2000-02", 1)]
    assert _drops([1.0, 0.8000001, 1.0]) == []


def test_equal_highs_take_the_latest_and_equal_lows_the_earliest() -> None:
    assert _drops([1.0, 1.1, 1.05, 1.1, 0.8, 1.2]) == [("fast_drop", "2000-04", "2000-05", 1)]
    assert _drops([1.0, 0.7, 0.75, 0.7, 1.1]) == [("fast_drop", "2000-01", "2000-02", 1)]


def test_without_recovery_the_trough_is_the_lowest_month_to_the_window_end() -> None:
    values = [1.0, 0.95, 0.9, 0.8, 0.75, 0.7, 0.65, 0.6, 0.62, 0.61]
    assert _drops(values) == [("slow_bear", "2000-01", "2000-08", 7)]


def test_a_new_bear_after_the_trough_is_its_own_segment() -> None:
    # The running high restarts after each trough, so a bear that starts before the level
    # regains the first peak is not swallowed by it (requirement section 2, rule 2).
    values = [1.0, 0.9, 0.8, 0.7, 0.6, 0.55, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]
    values += [0.7, 0.6, 0.65, 0.7, 0.8, 0.9, 1.05]
    assert _drops(values) == [
        ("slow_bear", "2000-01", "2000-07", 6),
        ("fast_drop", "2000-12", "2001-02", 2),
    ]


def test_long_bulls_are_gaps_longer_than_36_months() -> None:
    def rising(months: int) -> list[float]:
        return [1.0 + 0.01 * n for n in range(months)]

    for months, bulls in ((36, 0), (37, 1)):
        values = rising(months)
        peak = values[-1]
        values += [peak * 0.9, peak * 0.75, peak * 1.1]
        level = _level(values)
        found = sb.long_bull_segments(level, sb.drop_segments(level))
        assert len(found) == bulls
    item = found[0]
    assert (item.id, item.kind, item.start, item.end, item.months) == (
        "long_bull_200001",
        "long_bull",
        "2000-01",
        "2003-01",
        37,
    )
    assert item.peak is None and item.trough is None
    assert item.change == pytest.approx(values[36] - 1.0)


def test_a_long_bull_after_a_trough_is_measured_from_the_trough() -> None:
    values = [1.0, 0.7] + [0.7 * 1.01 ** (n + 1) for n in range(40)]
    level = _level(values)
    drops = sb.drop_segments(level)
    assert [(d.kind, d.trough) for d in drops] == [("fast_drop", "2000-02")]
    bulls = sb.long_bull_segments(level, drops)
    assert [(b.start, b.end, b.months) for b in bulls] == [("2000-03", "2003-06", 40)]
    assert bulls[0].change == pytest.approx(values[-1] / 0.7 - 1.0)


def test_segments_come_sorted_by_start() -> None:
    daily = sb.window_returns(US.returns["qqq"], WINDOWS["us_1929"])
    found = sb.segments(daily)
    assert [item.start for item in found] == sorted(item.start for item in found)
    assert {item.kind for item in found} <= {"slow_bear", "fast_drop", "long_bull"}
    assert any(item.kind == "slow_bear" for item in found)


def test_the_window_returns_are_the_simulations_days() -> None:
    window = WINDOWS["us_1929"]
    held = simulate(
        US.returns,
        constant_targets(US.returns.index, 1.0),
        start=window.start,
        end=window.end,
        rebalance=Rebalance(),
        cost_bps=0.0,
    )
    daily = sb.window_returns(US.returns["qqq"], window)
    pd.testing.assert_index_equal(daily.index, held.returns.index)
    np.testing.assert_allclose(daily.to_numpy(), held.returns.to_numpy(), rtol=0, atol=1e-15)


def test_a_missing_return_in_the_window_is_refused() -> None:
    returns = US.returns["qqq"].copy()
    returns.loc["1930-06-02"] = np.nan
    with pytest.raises(ValueError, match="1930-06-02"):
        sb.window_returns(returns, WINDOWS["us_1929"])


def test_the_monthly_level_compounds_the_daily_returns() -> None:
    daily = pd.Series(
        [0.1, -0.1, 0.2, 0.05],
        index=pd.DatetimeIndex(["2000-01-30", "2000-01-31", "2000-02-01", "2000-04-03"]),
    )
    level = sb.monthly_level(daily)
    assert [str(p) for p in level.index] == ["2000-01", "2000-02", "2000-04"]
    assert level.to_numpy() == pytest.approx([1.1 * 0.9, 1.1 * 0.9 * 1.2, 1.1 * 0.9 * 1.2 * 1.05])


@pytest.mark.parametrize(("data", "key"), [(US, "us_1929"), (QQQ, "qqq")], ids=["us", "qqq"])
def test_the_market_held_at_exposure_one_returns_each_segments_depth(
    data: BenchData, key: str
) -> None:
    # Catches a segment window shifted by a month: the peak month's own return is not inside.
    window = WINDOWS[key]
    held = simulate(
        data.returns,
        constant_targets(data.returns.index, 1.0),
        start=window.start,
        end=window.end,
        rebalance=Rebalance(),
        cost_bps=0.0,
    )
    items = sb.segments(sb.window_returns(data.returns["qqq"], window))
    assert any(item.kind == "slow_bear" for item in items)
    for item in items:
        assert sb.segment_return(held.returns, item) == pytest.approx(item.change, rel=1e-12)


def test_the_segment_return_is_what_metrics_episodes_computes() -> None:
    daily = sb.window_returns(US.returns["qqq"], WINDOWS["us_1929"])
    items = sb.segments(daily)
    loaded = replace(
        LOADED,
        settings=LOADED.settings.model_copy(
            update={
                "episodes": tuple(
                    EpisodeSpec(id=item.id, start=item.start, end=item.end) for item in items
                )
            }
        ),
    )
    episodes = _episodes(daily, pd.Timestamp(WINDOWS["us_1929"].start), loaded)
    for item in items:
        assert sb.segment_return(daily, item) == episodes[item.id]["return"]


def test_the_inferred_list_is_set_beside_the_found_one() -> None:
    def slow(peak: str, trough: str) -> sb.Segment:
        return sb.Segment(
            f"slow_bear_{peak.replace('-', '')}", "slow_bear", peak, trough, peak, trough, 6, -0.3
        )

    found = [
        slow("1929-09", "1932-06"),
        slow("1937-02", "1938-03"),
        slow("1950-01", "1950-09"),
        slow("1966-06", "1967-03"),
        sb.Segment(
            "fast_drop_196201", "fast_drop", "1962-02", "1962-06", "1962-01", "1962-06", 5, -0.25
        ),
    ]
    rows = sb.compare_inferred(
        found,
        (
            ("1929-09", "1932-06"),
            ("1937-03", "1938-03"),
            ("1961-12", "1962-06"),
            ("1965-01", "1966-06"),
        ),
    )
    assert rows == [
        {"inferred": "1929-09..1932-06", "found": "slow_bear_192909", "match": "same"},
        {"inferred": "1937-03..1938-03", "found": "slow_bear_193702", "match": "shifted"},
        {"inferred": "1961-12..1962-06", "found": None, "match": "missing"},
        {"inferred": "1965-01..1966-06", "found": "slow_bear_196606", "match": "shifted"},
        {"inferred": None, "found": "slow_bear_195001", "match": "extra"},
    ]


# --- statistics and rules ------------------------------------------------------------------------


def _pair(window: Window = WINDOWS["us_1929"], exposure: float = 0.6) -> sb.Pair:
    targets = constant_targets(US.returns.index, exposure)
    return sb.simulate_pair(US, targets, window, Rebalance(schedule="month_end"), LOADED)


def test_segment_rows_hold_both_returns_their_difference_and_the_exposure() -> None:
    pair = _pair()
    items = sb.segments(sb.window_returns(US.returns["qqq"], WINDOWS["us_1929"]))
    rows = sb.segment_rows(pair.strategy, pair.benchmark, items)
    assert [row["id"] for row in rows] == [item.id for item in items]
    for item, row in zip(items, rows, strict=True):
        assert row["strategy_return"] == sb.segment_return(pair.strategy.returns, item)
        assert row["benchmark_return"] == sb.segment_return(pair.benchmark.returns, item)
        assert row["difference"] == row["strategy_return"] - row["benchmark_return"]
        assert row["average_exposure"] == pytest.approx(0.6, abs=0.05)
        if item.kind == "long_bull":
            assert row["annual_difference"] == row["strategy_annual"] - row["benchmark_annual"]
        else:
            assert "annual_difference" not in row


def test_the_segment_exposure_is_the_strategys_own() -> None:
    index = pd.DatetimeIndex(US.returns.index)
    targets = pd.Series(np.where(index.year < 1931, 1.0, 0.2), index=index)
    window = WINDOWS["us_1929"]
    pair = sb.simulate_pair(US, targets, window, Rebalance(schedule="month_end"), LOADED)
    items = sb.segments(sb.window_returns(US.returns["qqq"], window))
    for item, row in zip(items, sb.segment_rows(pair.strategy, pair.benchmark, items), strict=True):
        months = pd.DatetimeIndex(pair.strategy.exposure.index).to_period("M")
        inside = (months >= pd.Period(item.start, freq="M")) & (
            months <= pd.Period(item.end, freq="M")
        )
        assert row["average_exposure"] == pytest.approx(
            float(pair.strategy.exposure[np.asarray(inside)].mean())
        )


def test_the_annual_return_counts_years_from_the_date_before_the_segment() -> None:
    index = pd.DatetimeIndex(["2000-01-31", "2000-02-29", "2000-03-31", "2001-02-28"])
    returns = pd.Series([0.5, 0.1, 0.1, 0.1], index=index)
    item = sb.Segment("long_bull_200002", "long_bull", "2000-02", "2001-02", None, None, 13, 0.0)
    years = (pd.Timestamp("2001-02-28") - pd.Timestamp("2000-01-31")).days / 365.25
    assert sb.annual_return(returns, item, pd.Timestamp("1999-12-31")) == pytest.approx(
        (1.1**3) ** (1 / years) - 1
    )
    first = sb.Segment("long_bull_200001", "long_bull", "2000-01", "2000-03", None, None, 3, 0.0)
    years = (pd.Timestamp("2000-03-31") - pd.Timestamp("1999-12-31")).days / 365.25
    assert sb.annual_return(returns, first, pd.Timestamp("1999-12-31")) == pytest.approx(
        (1.5 * 1.1 * 1.1) ** (1 / years) - 1
    )


def test_the_slow_bear_summary() -> None:
    rows = [{"kind": "slow_bear", "difference": value} for value in (0.2, 0.1, -0.05, 0.0, 0.3)] + [
        {"kind": "fast_drop", "difference": -1.0}
    ]
    assert sb.slow_bear_summary(rows) == {
        "count": 5,
        "median_difference": 0.1,
        "positive_share": 0.6,
        "min_difference": -0.05,
        "nonpositive": 2,
    }
    assert sb.slow_bear_summary(rows[5:]) == {
        "count": 0,
        "median_difference": None,
        "positive_share": None,
        "min_difference": None,
        "nonpositive": None,
    }
    assert sb.long_bull_summary(rows) == {
        "count": 0,
        "mean_annual_difference": None,
        "worst_annual_difference": None,
    }
    bulls = [{"kind": "long_bull", "annual_difference": v} for v in (0.01, -0.03)]
    assert sb.long_bull_summary(bulls) == {
        "count": 2,
        "mean_annual_difference": pytest.approx(-0.01),
        "worst_annual_difference": -0.03,
    }


@pytest.mark.parametrize("count", range(1, 13))
def test_a_three_quarter_share_is_three_quarters_rounded_up(count: int) -> None:
    for positive in range(count + 1):
        share = positive / count
        assert (share >= sb.SB2) == (positive >= math.ceil(0.75 * count))


def test_the_difference_moves_with_the_strategys_returns() -> None:
    pair = _pair()
    items = sb.segments(sb.window_returns(US.returns["qqq"], WINDOWS["us_1929"]))
    slow = next(item for item in items if item.kind == "slow_bear")
    before = sb.slow_bear_summary(sb.segment_rows(pair.strategy, pair.benchmark, items))
    daily = pair.strategy.daily.copy()
    inside = pd.DatetimeIndex(daily.index).to_period("M") == pd.Period(slow.end, freq="M")
    daily.loc[inside, "return"] += 0.01
    moved = replace(pair.strategy, daily=daily)
    after = sb.slow_bear_summary(sb.segment_rows(moved, pair.benchmark, items))
    assert after["median_difference"] > before["median_difference"]
    assert after["min_difference"] != before["min_difference"]


def test_the_slow_bear_checks() -> None:
    def shown(checks: tuple[sb.Check, ...]) -> list[tuple[str, str, float]]:
        return [(c.metric, c.op, c.value) for c in checks]

    for count in (sb.MIN_MEDIAN_SEGMENTS, 7):
        assert shown(sb.slow_bear_checks("stage1", count)) == [
            ("slow_bear.median_difference", ">=", 0.10),
            ("slow_bear.positive_share", ">=", 0.75),
        ]
    assert shown(sb.slow_bear_checks("stage1", 7, tight=True)) == [
        ("slow_bear.median_difference", ">=", 0.15),
        ("slow_bear.nonpositive", "<=", 1.0),
    ]
    assert shown(sb.slow_bear_checks("stage1", sb.MIN_MEDIAN_SEGMENTS - 1)) == [
        ("slow_bear.positive_share", ">=", 1.0)
    ]
    for tight in (False, True):
        for count in (2, 7):
            assert shown(sb.slow_bear_checks("stage2", count, tight=tight)) == [
                ("slow_bear.positive_share", ">=", 1.0)
            ]


def test_check_rows_have_the_shape_of_evaluate_rules() -> None:
    sections = {"comparison": {"max_drawdown": 0.06, "geometric_annual_return": None}}
    for op, value in ((">=", 0.05), ("<", 0.05), (">", 0.06), ("<=", 0.06)):
        rule = Rule(metric="comparison.max_drawdown", op=op, value=value)  # type: ignore[arg-type]
        check = sb.Check("comparison.max_drawdown", op, value)
        assert sb.check_rows((check,), sections) == evaluate_rules((rule,), sections)
    missing = sb.check_rows((sb.Check("comparison.geometric_annual_return", ">=", -1.0),), sections)
    assert missing[0]["holds"] is False and missing[0]["observed"] is None
    nan = sb.check_rows(
        (sb.Check("slow_bear.count", ">=", 0.0),), {"slow_bear": {"count": math.nan}}
    )
    assert nan[0]["holds"] is False


def test_the_slow_bear_track_puts_its_checks_before_the_common_rules() -> None:
    sections: dict[str, Any] = {
        "comparison": {"max_drawdown": 0.06, "geometric_annual_return": -0.005},
        "cost_25": {"geometric_annual_return": -0.02},
        "slow_bear": {
            "count": 5,
            "median_difference": 0.12,
            "positive_share": 0.8,
            "min_difference": -0.01,
            "nonpositive": 1,
        },
    }
    rows = sb.slow_bear_track("stage1", sections, GATE)
    assert [(r["metric"], r["holds"]) for r in rows] == [
        ("slow_bear.median_difference", True),
        ("slow_bear.positive_share", True),
        ("comparison.max_drawdown", True),
        ("comparison.geometric_annual_return", True),
        ("cost_25.geometric_annual_return", False),
    ]
    tight = sb.slow_bear_track("stage1", sections, GATE, tight=True)
    assert [(r["metric"], r["holds"]) for r in tight[:2]] == [
        ("slow_bear.median_difference", False),
        ("slow_bear.nonpositive", True),
    ]
    second = sb.slow_bear_track(
        "stage2", {**sections, "slow_bear": {"count": 2, "positive_share": 1.0}}, GATE
    )
    assert [(r["metric"], r["holds"]) for r in second] == [
        ("slow_bear.positive_share", True),
        ("comparison.geometric_annual_return", True),
    ]
    crash = sb.crash_track("stage2", sections, GATE)
    assert [r["metric"] for r in crash] == [r.metric for r in GATE.markets["qqq"].rules]


# --- one simulation pair -------------------------------------------------------------------------


def _treasury(data: BenchData) -> BenchData:
    reserve = pd.Series(0.0002, index=data.returns.index)
    return replace(data, reserves={"treasury_10y": reserve})


@pytest.mark.parametrize("reserve", ["cash", "treasury_10y"])
def test_the_pair_is_what_a_trial_run_reads(reserve: str) -> None:
    data = _treasury(QQQ)
    spec = RunSpec(
        question_type="qqq_direction",
        strategy_id="fixed",
        params={"exposure": 0.6},
        window_role="development",
        rebalance=Rebalance(schedule="month_end"),
        reserve=reserve,
    )
    result = run_strategy(
        data, tr.STRATEGY_FUNCTIONS["fixed_exposure"], spec, LOADED, falsification_confidence=0.95
    ).to_dict()
    window = sb.probe_windows(LOADED, GATE)["qqq"]
    targets = constant_targets(data.inputs.index, 0.6)
    pair = sb.simulate_pair(data, targets, window, spec.rebalance, LOADED, reserve)
    assert pair.comparison == result["comparison"]
    assert pair.cost == result["falsification"]["cost_25"]


def test_the_pair_of_a_signal_config_is_what_a_trial_run_reads() -> None:
    item = _items()[0]
    spec = RunSpec(
        question_type="qqq_direction",
        strategy_id=item.id,
        params=item.params,
        window_role="development",
        rebalance=item.rebalance,
    )
    result = run_strategy(
        QQQ, tr.STRATEGY_FUNCTIONS[item.function], spec, LOADED, falsification_confidence=0.95
    ).to_dict()
    targets = tr.STRATEGY_FUNCTIONS[item.function](QQQ.inputs, dict(item.params))
    pair = sb.simulate_pair(
        QQQ, targets, sb.probe_windows(LOADED, GATE)["qqq"], item.rebalance, LOADED
    )
    assert pair.comparison == result["comparison"]
    assert pair.cost == result["falsification"]["cost_25"]


# --- fidelity ------------------------------------------------------------------------------------


METRICS = ["comparison.max_drawdown", "comparison.geometric_annual_return"]


def _gate_rows(drawdown: float, geometric: float) -> list[dict[str, Any]]:
    return [
        {"metric": "comparison.cvar_5pct_relative_improvement", "observed": 0.3, "holds": True},
        {"metric": METRICS[0], "observed": drawdown, "holds": True},
        {"metric": METRICS[1], "observed": geometric, "holds": True},
    ]


def test_fidelity_allows_1e9_and_catches_1e8() -> None:
    source = {"run_id": "trial-r0001", "values": {METRICS[0]: 0.2, METRICS[1]: -0.01}}
    same = sb.fidelity({METRICS[0]: 0.2 + 5e-10, METRICS[1]: -0.01}, source)
    assert same["status"] == "match" and same["run_id"] == "trial-r0001"
    assert same["differences"][METRICS[0]] == pytest.approx(5e-10)
    off = sb.fidelity({METRICS[0]: 0.2 + 1e-8, METRICS[1]: -0.01}, source)
    assert off["status"] == "mismatch"
    missing = sb.fidelity({METRICS[0]: None, METRICS[1]: -0.01}, source)
    assert missing["status"] == "mismatch" and missing["differences"][METRICS[0]] is None
    assert sb.fidelity({METRICS[0]: 0.2}, None) == {
        "status": "no_record",
        "run_id": None,
        "differences": {},
    }


def test_the_trial_source_is_the_first_ok_record_of_the_primary_on_the_market() -> None:
    def record(run_id: str, **changes: Any) -> dict[str, Any]:
        base = {
            "run_id": run_id,
            "trial_id": "T0004",
            "config": "primary",
            "market": "us_market",
            "status": "ok",
            "gate": _gate_rows(0.2, 0.005),
        }
        return {**base, **changes}

    records = [
        record("trial-r0001", trial_id="T0003"),
        record("trial-r0002", market="qqq"),
        record("trial-r0003", config="other"),
        record("trial-r0004", status="error"),
        record("trial-r0005"),
        record("trial-r0006", gate=_gate_rows(0.1, 0.0)),
    ]
    source = sb.trial_source(records, "T0004", "primary", "us_market", METRICS)
    assert source == {"run_id": "trial-r0005", "values": {METRICS[0]: 0.2, METRICS[1]: 0.005}}
    assert sb.trial_source(records[:4], "T0004", "primary", "us_market", METRICS) is None


def test_the_backfill_source_and_file(tmp_path: Path) -> None:
    result = {
        "run_id": "TRADING-2567-33",
        "complete": True,
        "rows": [
            {"config": "rv21", "status": "error", "gate": []},
            {"config": "fast_slow", "status": "ok", "gate": _gate_rows(0.192, 0.013)},
        ],
    }
    assert sb.backfill_source(result, "fast_slow", METRICS) == {
        "run_id": "TRADING-2567-33 fast_slow",
        "values": {METRICS[0]: 0.192, METRICS[1]: 0.013},
    }
    assert sb.backfill_source(result, "rv21", METRICS) is None
    path = tmp_path / "backfill_result.json"
    path.write_text(json.dumps(result), encoding="utf-8")
    assert sb.load_backfill(path) == result
    broken: Any
    for broken in ({**result, "run_id": "TRADING-2565-22"}, {**result, "complete": False}, []):
        path.write_text(json.dumps(broken), encoding="utf-8")
        with pytest.raises(ValueError, match="not a complete TRADING-2567-33 result"):
            sb.load_backfill(path)


# --- the configurations --------------------------------------------------------------------------


def test_each_configuration_reads_both_stages(probe: dict[str, Any]) -> None:
    assert [row["config"] for row in probe["configs"]] == ["fast_slow", "T0002"]
    for row in probe["configs"]:
        assert set(row["stages"]) == {"stage1", "stage2"}
        first, second = row["stages"]["stage1"], row["stages"]["stage2"]
        assert (first["window_key"], second["window_key"]) == ("us_1929", "qqq")
        assert (first["market"], second["market"]) == ("us_market", "qqq")
        assert first["window"] == sb.window_record(WINDOWS["us_1929"])
        for stage in (first, second):
            assert stage["status"] == "ok" and stage["read"] is True
            assert stage["fidelity"]["status"] == "no_record"
            assert stage["standard_holds"] == all(r["holds"] for r in stage["standard"])
            assert stage["tight_holds"] == all(r["holds"] for r in stage["tight"])
        assert [r["metric"] for r in first["standard"]][:2] == [
            "slow_bear.median_difference",
            "slow_bear.positive_share",
        ] or first["slow_bear"]["count"] < sb.MIN_MEDIAN_SEGMENTS
        assert list(first["values"]) == [r.metric for r in sb.common_rules(GATE, "us_market")]
        assert list(second["values"]) == [r.metric for r in sb.common_rules(GATE, "qqq")]


def test_a_stage_is_the_pair_on_the_windows_segments(probe: dict[str, Any]) -> None:
    row = probe["configs"][1]
    first = row["stages"]["stage1"]
    pair = sb.simulate_pair(
        US,
        constant_targets(US.inputs.index, 0.6),
        WINDOWS["us_1929"],
        Rebalance(schedule="month_end"),
        LOADED,
    )
    items = sb.segments(sb.window_returns(US.returns["qqq"], WINDOWS["us_1929"]))
    rows = gp._clean(sb.segment_rows(pair.strategy, pair.benchmark, items))
    assert first["segments"] == rows
    assert first["slow_bear"] == gp._clean(sb.slow_bear_summary(rows))
    assert first["long_bull"] == gp._clean(sb.long_bull_summary(rows))
    assert first["values"]["comparison.max_drawdown"] == pair.comparison["max_drawdown"]
    assert first["average_exposure"] == pytest.approx(float(pair.strategy.exposure.mean()))


def test_the_verdict_needs_both_stages_and_is_set_beside_the_expectation(
    probe: dict[str, Any],
) -> None:
    for row in probe["configs"]:
        first, second = row["stages"]["stage1"], row["stages"]["stage2"]
        standard = "pass" if first["standard_holds"] and second["standard_holds"] else "fail"
        tight = "pass" if first["tight_holds"] and second["tight_holds"] else "fail"
        assert row["verdict"] == {"standard": standard, "tight": tight}
        expected = sb.EXPECTATIONS[row["config"]]
        assert row["expected"]["stage1"] == expected.stage1
        assert row["judged"]["stage1"] == (first["standard_holds"] == expected.stage1)
        assert row["judged"]["overall"] == ((standard == "pass") == expected.overall)
    assert probe["configs"][1]["judged"]["stage2"] == (
        probe["configs"][1]["stages"]["stage2"]["standard_holds"] is False
    )


def test_the_judgement_follows_the_standard_set(monkeypatch: pytest.MonkeyPatch) -> None:
    def track(
        stage: str, sections: Any, gate: tr.TrialGate, *, tight: bool = False
    ) -> list[dict[str, Any]]:
        rule = {"metric": "slow_bear.positive_share", "op": ">=", "value": 1.0}
        return [{**rule, "observed": 1.0, "holds": not tight}]

    monkeypatch.setattr(sb, "slow_bear_track", track)
    lists = sb.segment_lists(DATA, WINDOWS)
    row = sb.evaluate_item(replace(FIXED, id="T0004"), DATA, WINDOWS, lists, GATE, LOADED, {})
    assert row["verdict"] == {"standard": "pass", "tight": "fail"}
    assert row["judged"] == {"stage1": True, "stage2": True, "overall": True}


def test_a_mismatched_stage_is_not_read() -> None:
    windows = sb.probe_windows(LOADED, GATE)
    lists = sb.segment_lists(DATA, {**windows, **WINDOWS})
    wrong = {
        "run_id": "trial-r0009",
        "values": {r.metric: 9.0 for r in sb.common_rules(GATE, "us_market")},
    }
    row = sb.evaluate_item(
        FIXED, DATA, WINDOWS, lists, GATE, LOADED, {"stage1": wrong, "stage2": None}
    )
    first = row["stages"]["stage1"]
    assert first["fidelity"]["status"] == "mismatch" and first["read"] is False
    assert row["verdict"] == {"standard": "unread", "tight": "unread"}
    assert row["judged"] == {"stage1": None, "stage2": row["judged"]["stage2"], "overall": None}
    assert row["judged"]["stage2"] is not None


def test_an_expectation_that_is_not_set_is_not_judged() -> None:
    lists = sb.segment_lists(DATA, WINDOWS)
    row = sb.evaluate_item(replace(FIXED, id="T0006"), DATA, WINDOWS, lists, GATE, LOADED, {})
    assert row["expected"]["stage2"] is None and row["judged"]["stage2"] is None


def test_a_failing_configuration_is_an_error_row() -> None:
    lists = sb.segment_lists(DATA, WINDOWS)
    broken = replace(FIXED, params={"exposure": 7.0})
    row = sb.evaluate_item(broken, DATA, WINDOWS, lists, GATE, LOADED, {})
    stage = row["stages"]["stage1"]
    assert stage["status"] == "error" and stage["read"] is False and "exposure" in stage["error"]
    assert row["verdict"]["standard"] == "unread"


def test_positive_controls_all_failing_is_flagged() -> None:
    def row(config: str, read: bool, holds: bool) -> dict[str, Any]:
        return {"config": config, "stages": {"stage1": {"read": read, "standard_holds": holds}}}

    failing = [row(c, True, False) for c in sb.POSITIVE_CONTROLS]
    assert sb.positive_controls(failing) == {
        "configs": list(sb.POSITIVE_CONTROLS),
        "all_read": True,
        "all_failed": True,
    }
    assert sb.positive_controls([*failing[:2], row("slow_only", True, True)])["all_failed"] is False
    unread = sb.positive_controls([*failing[:2], row("slow_only", False, False)])
    assert unread["all_read"] is False and unread["all_failed"] is False
    assert sb.positive_controls(failing[:2])["all_read"] is False


# --- false passes --------------------------------------------------------------------------------


def test_the_tasks_cover_every_family_seed_and_window() -> None:
    tasks = sb.false_pass_tasks(2, 1)
    assert tasks[:3] == [
        ("null", fp.SEED_START, "us_1929"),
        ("null", fp.SEED_START, "us_1963"),
        ("null", fp.SEED_START, "qqq"),
    ]
    assert len(tasks) == (3 * 2 + 2 * 1) * 3
    assert {family for family, _, _ in tasks} == set(fp.FAMILIES)


def test_a_null_evaluation_keeps_what_the_readings_need() -> None:
    lists = sb.segment_lists(NULL_DATA, WINDOWS)
    targets = fp.family_targets(fp.FAMILIES["null"], NULL_DATA["us_market"].inputs, fp.SEED_START)
    plain = sb.evaluate_null(
        NULL_DATA["us_market"],
        targets,
        WINDOWS["us_1929"],
        lists["us_1929"],
        LOADED,
        GATE,
        bootstrap=False,
    )
    assert set(plain["sections"]) == {"comparison", "cost_25", "slow_bear"}
    assert set(plain["sections"]["comparison"]) == {
        "cvar_5pct_relative_improvement",
        "max_drawdown",
        "geometric_annual_return",
    }
    assert set(plain["effect"]) == set(fp.EFFECT_FIELDS) and plain["error"] is None
    full = sb.evaluate_null(
        NULL_DATA["us_market"],
        targets,
        WINDOWS["us_1963"],
        lists["us_1963"],
        LOADED,
        GATE,
        bootstrap=True,
    )
    assert set(full["sections"]) == {"comparison", "cost_25", "ci_low", "ci_high", "slow_bear"}
    assert set(full["sections"]["ci_low"]) == {"cvar_5pct_relative_improvement"}


def test_the_crash_reading_is_the_false_pass_probes_evaluation() -> None:
    lists = sb.segment_lists(NULL_DATA, WINDOWS)
    data = NULL_DATA["us_market"]
    targets = fp.family_targets(fp.FAMILIES["vol_noise_1"], data.inputs, fp.SEED_START)
    mine = sb.evaluate_null(
        data, targets, WINDOWS["us_1963"], lists["us_1963"], LOADED, GATE, bootstrap=True
    )
    rules = GATE.markets["us_market"].rules
    theirs = fp.evaluate(data, targets, WINDOWS["us_1963"], LOADED, rules, GATE.confidence)
    assert sb.crash_track("stage1", mine["sections"], GATE) == theirs["rules"]
    assert mine["effect"] == theirs["effect"]


def _evaluation(
    family: str,
    seed: int,
    key: str,
    *,
    stage1: bool = False,
    stage2: bool = True,
    sb1: bool | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    holds = stage1 if key != "qqq" else stage2
    median = 0.12 if (sb1 if sb1 is not None else holds) else 0.01
    sections = {
        "comparison": {
            "max_drawdown": 0.1,
            "geometric_annual_return": 0.0,
            "cvar_5pct_relative_improvement": 0.2,
        },
        "cost_25": {"geometric_annual_return": 0.0},
        "ci_low": {"cvar_5pct_relative_improvement": 0.01 if holds else -0.01},
        "ci_high": {"cvar_5pct_relative_improvement": 0.3},
        "slow_bear": {
            "count": 5 if key != "qqq" else 2,
            "median_difference": median,
            "positive_share": 1.0 if holds else 0.4,
            "min_difference": 0.0,
            "nonpositive": 0 if holds else 3,
        },
    }
    return {
        "family": family,
        "role": fp.FAMILIES[family].role,
        "seed": seed,
        "window_key": key,
        "average_exposure": 0.5,
        "return_days": 100,
        "effect": {field: 0.0 for field in fp.EFFECT_FIELDS},
        "sections": {} if error else sections,
        "error": error,
    }


def _evaluations(passing: dict[str, int], seeds: int = 50) -> list[dict[str, Any]]:
    """Zero families with ``passing[key]`` seeds holding on stage-1 window ``key``."""

    rows = []
    for family in fp.ZERO_FAMILIES:
        for n in range(seeds):
            for key in sb.FALSE_PASS_WINDOWS:
                rows.append(
                    _evaluation(
                        family.family_id,
                        fp.SEED_START + n,
                        key,
                        stage1=n < passing.get(key, 0),
                    )
                )
    return rows


def test_the_reading_rows_have_the_false_pass_probes_shape() -> None:
    rows = sb.reading_rows(_evaluations({"us_1929": 1}, seeds=2), sb.READINGS[0], GATE)
    assert len(rows) == 3 * 2 * 2
    assert {row["stage"] for row in rows} == {"stage1", "stage2"}
    first = rows[0]
    assert set(first) == {"family", "role", "seed", "stage", "holds", "error", "rules", "effect"}
    assert first["holds"] is True and [r["metric"] for r in first["rules"]][:2] == [
        "slow_bear.median_difference",
        "slow_bear.positive_share",
    ]
    crash = sb.reading_rows(_evaluations({}, seeds=1), sb.READINGS[2], GATE)
    assert [r["metric"] for r in crash[0]["rules"]] == [
        r.metric for r in GATE.markets["us_market"].rules
    ]
    summary = fp.family_summary(rows)
    assert summary["both"]["passes"] == 3 and summary["both"]["total"] == 6


def test_an_evaluation_error_fails_its_unit_and_makes_the_reading_incomplete() -> None:
    rows = _evaluations({}, seeds=50)
    rows[0] = _evaluation("null", fp.SEED_START, "us_1929", error="ValueError: boom")
    readings = sb.false_pass_readings(rows, GATE)
    assert readings["slow_1929"]["standard"]["verdict"] == "incomplete"
    assert readings["slow_1963"]["standard"]["verdict"] == "usable"
    assert readings["applies"] == "incomplete"


def test_the_readings_and_which_set_applies() -> None:
    clean = sb.false_pass_readings(_evaluations({}), GATE)
    for reading in ("slow_1929", "slow_1963"):
        entry = clean[reading]
        assert entry["standard"]["passes"] == 0 and entry["standard"]["total"] == 150
        assert entry["standard"]["upper"] == pytest.approx(fp.wilson(0, 150)[1])
        assert entry["standard"]["verdict"] == "usable" and entry["tight"]["verdict"] == "usable"
    assert clean["crash_1963"]["gate"]["verdict"] == "usable"
    assert clean["applies"] == "standard"
    # Seeds passing on stage 1 also hold the tighter set here (median 0.12 < 0.15 does not).
    over = sb.false_pass_readings(_evaluations({"us_1963": 5}), GATE)
    assert over["slow_1963"]["standard"]["verdict"] == "over"
    assert over["slow_1963"]["tight"]["passes"] == 0
    assert over["slow_1929"]["standard"]["verdict"] == "usable"
    assert over["crash_1963"]["gate"]["passes"] == 15
    assert over["applies"] == "tight"


def test_an_upper_bound_on_the_line_is_usable(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = sb.reading_rows(_evaluations({"us_1929": 2}), sb.READINGS[0], GATE)
    upper = sb.read_rows(rows)["upper"]
    monkeypatch.setattr(sb, "USABLE_LINE", upper)
    assert sb.read_rows(rows)["verdict"] == "usable"
    monkeypatch.setattr(sb, "USABLE_LINE", upper - 1e-9)
    assert sb.read_rows(rows)["verdict"] == "over"


def test_the_tight_set_failing_too_means_neither() -> None:
    rows = _evaluations({"us_1929": 5})
    for row in rows:
        if row["window_key"] == "us_1929" and row["sections"]["slow_bear"]["positive_share"] == 1.0:
            row["sections"]["slow_bear"]["median_difference"] = 0.2
    readings = sb.false_pass_readings(rows, GATE)
    assert readings["slow_1929"]["tight"]["verdict"] == "over"
    assert readings["applies"] == "neither"


def test_the_crash_reading_never_triggers_the_tight_set() -> None:
    rows = _evaluations({})
    for row in rows:
        if row["window_key"] == "us_1963" and row["seed"] < fp.SEED_START + 5:
            row["sections"]["ci_low"]["cvar_5pct_relative_improvement"] = 0.05
    readings = sb.false_pass_readings(rows, GATE)
    assert readings["crash_1963"]["gate"]["verdict"] == "over"
    assert readings["applies"] == "standard"


def test_sb_counts_split_the_zero_families_stage_one() -> None:
    rows = []
    for n, (sb1, holds) in enumerate(((True, True), (True, False), (False, True), (False, False))):
        rows.append(_evaluation("null", fp.SEED_START + n, "us_1929", stage1=holds, sb1=sb1))
    rows.append(_evaluation("vol_noise_1", fp.SEED_START, "us_1929", stage1=True))
    rows.append(_evaluation("null", fp.SEED_START + 9, "us_1929", error="ValueError: boom"))
    counts = sb.sb_counts(sb.reading_rows(rows, sb.READINGS[0], GATE))
    assert counts == {"sb1_only": 1, "sb2_only": 1, "both": 1, "neither": 1}


def test_the_false_pass_part_of_the_run(probe: dict[str, Any]) -> None:
    evaluations = probe["false_pass"]["evaluations"]
    assert len(evaluations) == len(sb.false_pass_tasks(SEEDS, CONTROL_SEEDS))
    assert all(row["error"] is None for row in evaluations)
    for row in evaluations:
        bootstrapped = "ci_low" in row["sections"]
        assert bootstrapped == (row["window_key"] in sb.BOOTSTRAP_WINDOWS)
    assert [(r["family"], r["seed"], r["window_key"]) for r in evaluations] == [
        (f, s, k) for f, s, k in sb.false_pass_tasks(SEEDS, CONTROL_SEEDS)
    ]
    readings = probe["false_pass"]["readings"]
    assert set(readings) == {"slow_1929", "slow_1963", "crash_1963", "applies"}
    assert readings["slow_1929"]["standard"]["total"] == 3 * SEEDS
    assert readings == gp._clean(sb.false_pass_readings(evaluations, GATE))


def test_a_failed_false_pass_evaluation_leaves_the_run_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken(*args: Any, **kwargs: Any) -> pd.Series:
        raise ValueError("boom")

    monkeypatch.setattr(fp, "family_targets", broken)
    result = sb.run_probe(
        DATA,
        NULL_DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        items=[],
        sources={},
        seeds=1,
        control_seeds=0,
        windows=WINDOWS,
    )
    evaluations = result["false_pass"]["evaluations"]
    assert len(evaluations) == 3 * 3
    assert all(row["error"] == "ValueError: boom" for row in evaluations)
    assert result["false_pass"]["readings"]["applies"] == "incomplete"
    assert result["complete"] is False


def test_parallel_workers_give_the_same_evaluations(probe: dict[str, Any]) -> None:
    result = sb.run_probe(
        DATA,
        NULL_DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        items=[],
        sources={},
        seeds=SEEDS,
        control_seeds=CONTROL_SEEDS,
        windows=WINDOWS,
        workers=2,
    )
    assert result["false_pass"] == probe["false_pass"]


def test_the_null_data_must_be_cut() -> None:
    with pytest.raises(ValueError, match="cut them first"):
        sb.run_probe(
            DATA,
            {"us_market": _us_data(start="2009-01-01", end="2011-03-31"), "qqq": QQQ},
            LOADED,
            GATE,
            ROOT_DIR,
            items=[],
            sources={},
            windows=WINDOWS,
        )


# --- the run -------------------------------------------------------------------------------------


def test_the_head_names_the_gate_the_thresholds_and_the_windows(probe: dict[str, Any]) -> None:
    assert probe["run_id"] == sb.RUN_ID and probe["owner_decision"] == sb.OWNER_DECISION
    assert probe["gate_sha256"] == lg.chain_digest(PREREG.read_bytes())
    assert probe["settings_sha256"] == LOADED.sha256 and probe["confidence"] == 0.95
    assert probe["thresholds"] == {
        "drop": 0.2,
        "slow_months": 6,
        "long_bull_months": 36,
        "min_median_segments": 4,
        "sb1": 0.1,
        "sb2": 0.75,
        "tight_sb1": 0.15,
        "tight_nonpositive": 1,
        "fidelity_tolerance": 1e-9,
        "usable_line": 0.025,
    }
    assert probe["windows"] == {key: sb.window_record(WINDOWS[key]) for key in sb.WINDOW_KEYS}
    assert probe["source"] == {market: DATA[market].source for market in ("qqq", "us_market")}
    assert (probe["seeds"], probe["control_seeds"]) == (SEEDS, CONTROL_SEEDS)
    assert probe["complete"] is True


def test_the_segment_lists_are_in_the_result(probe: dict[str, Any]) -> None:
    found = probe["segments"]
    assert list(found) == list(sb.WINDOW_KEYS)
    lists = sb.segment_lists(DATA, WINDOWS)
    for key in sb.WINDOW_KEYS:
        assert found[key]["window"] == sb.window_record(WINDOWS[key])
        assert found[key]["segments"] == gp._clean([s.__dict__ for s in lists[key]])
    assert found["us_1962"]["inferred"] is None
    assert found["us_1929"]["inferred"] == sb.compare_inferred(
        lists["us_1929"], sb.INFERRED["us_1929"]
    )
    kinds = [
        (s["kind"], s["peak"][:4], s["trough"][:4])
        for s in found["us_1929"]["segments"]
        if s["kind"] != "long_bull"
    ]
    assert kinds == [("slow_bear", "1929", "1932"), ("fast_drop", "1935", "1935")]


def test_the_journal_holds_the_head_segments_configs_and_evaluations(
    probe_run: tuple[dict[str, Any], Path],
) -> None:
    result, journal = probe_run
    lines = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]
    assert lines[0]["run_id"] == sb.RUN_ID and "configs" not in lines[0]
    assert lines[1] == {"segments": result["segments"]}
    assert lines[2:4] == result["configs"]
    assert lines[4:] == result["false_pass"]["evaluations"]


def test_the_probe_writes_nothing_under_the_research_root(tmp_path: Path) -> None:
    before = sorted(str(path) for path in ROOT_DIR.rglob("*") if "__pycache__" not in path.parts)
    sb.run_probe(
        DATA,
        NULL_DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        items=[FIXED],
        sources={},
        seeds=1,
        control_seeds=0,
        windows=WINDOWS,
        journal=tmp_path / sb.JOURNAL_FILE,
    )
    after = sorted(str(path) for path in ROOT_DIR.rglob("*") if "__pycache__" not in path.parts)
    assert after == before


def test_the_journal_is_never_overwritten(tmp_path: Path) -> None:
    journal = tmp_path / sb.JOURNAL_FILE
    journal.write_text("", encoding="utf-8")
    with pytest.raises(FileExistsError):
        sb.run_probe(
            DATA,
            NULL_DATA,
            LOADED,
            GATE,
            ROOT_DIR,
            items=[],
            sources={},
            seeds=1,
            control_seeds=0,
            windows=WINDOWS,
            journal=journal,
        )


# --- the report and the commands -----------------------------------------------------------------


def test_the_result_is_written_as_json_and_markdown(probe: dict[str, Any], tmp_path: Path) -> None:
    sb.write_result(probe, tmp_path)
    assert json.loads(tmp_path.joinpath(sb.RESULT_JSON).read_text(encoding="utf-8")) == probe
    text = tmp_path.joinpath(sb.RESULT_MD).read_text(encoding="utf-8")
    assert text.startswith("# 慢熊判据探针（TRADING-2565-34）")
    for key in sb.WINDOW_KEYS:
        assert f"### {key}：" in text
    assert "| 推断的慢熊段 | 算出的段 | 对照 |" in text
    assert "| fast_slow | QQQ-Q3 fast_slow | 无原记录 / 无原记录 | us_1929 |" in text
    assert "## 误放率" in text and "| slow_1929 | 收紧 |" in text
    assert "| crash_1963 | 现在的门 |" in text and "适用的一套" in text
    assert "阳性对照全没过" not in text


def test_the_report_flags_failed_positive_controls(probe: dict[str, Any]) -> None:
    flagged = {**probe, "positive_controls": {"configs": [], "all_read": True, "all_failed": True}}
    assert "阳性对照全没过：判据或划段要回炉（需求第 6 节）。" in sb.render_markdown(flagged)


def _backfill(tmp_path: Path) -> Path:
    path = tmp_path / "backfill_result.json"
    path.write_text(json.dumps({"run_id": bc.RUN_ID, "complete": True, "rows": []}), "utf-8")
    return path


def _loaded_data(_: Path) -> Any:
    return DATA, NULL_DATA, GATE, LOADED, ROOT_DIR


def test_the_command_refuses_to_overwrite(tmp_path: Path) -> None:
    tmp_path.joinpath(sb.RESULT_JSON).write_text("{}", encoding="utf-8")
    outcome = CliRunner().invoke(
        sb.app, ["run", "--out", str(tmp_path), "--backfill-result", str(_backfill(tmp_path))]
    )
    assert outcome.exit_code == 1 and "拒绝覆盖" in outcome.output


def test_the_command_refuses_a_wrong_backfill_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    called: list[Path] = []
    monkeypatch.setattr(sb, "load_data", lambda root: called.append(root))
    path = tmp_path / "other.json"
    path.write_text(json.dumps({"run_id": "TRADING-2565-22", "complete": True}), "utf-8")
    out = tmp_path / "out"
    outcome = CliRunner().invoke(sb.app, ["run", "--out", str(out), "--backfill-result", str(path)])
    assert outcome.exit_code == 1 and "回填结果不能用" in outcome.output
    assert called == [] and not out.exists()


def test_the_commands_stop_when_the_data_do_not_load(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail(_: Path) -> Any:
        raise ValueError("no snapshot")

    monkeypatch.setattr(sb, "load_data", fail)
    out = tmp_path / "out"
    for args in (
        ["check"],
        ["run", "--out", str(out), "--backfill-result", str(_backfill(tmp_path))],
    ):
        outcome = CliRunner().invoke(sb.app, args)
        assert outcome.exit_code == 1 and "数据载入失败：no snapshot" in outcome.output
    assert not out.exists()


def test_the_check_prints_the_segments_and_the_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sb, "load_data", _loaded_data)
    monkeypatch.setattr(sb, "probe_windows", lambda loaded, gate: WINDOWS)
    monkeypatch.setattr(sb, "probe_items", lambda root_dir: _items())
    outcome = CliRunner().invoke(sb.app, ["check"])
    assert outcome.exit_code == 0, outcome.output
    assert "slow_bear_check: us_1929 slow_bear_1929" in outcome.output
    assert "slow_bear_check: us_1929 推断 1929-09..1932-06" in outcome.output
    assert "slow_bear_check: fast_slow stage1 us_1929 窗口内" in outcome.output
    assert "缺 0 天，可以" in outcome.output


def test_the_run_stops_when_the_check_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    late = {**DATA, "us_market": _us_data(start="1928-10-01")}
    monkeypatch.setattr(sb, "load_data", lambda _: (late, NULL_DATA, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(sb, "probe_windows", lambda loaded, gate: WINDOWS)
    monkeypatch.setattr(sb, "probe_items", lambda root_dir: _items())
    out = tmp_path / "out"
    outcome = CliRunner().invoke(
        sb.app, ["run", "--out", str(out), "--backfill-result", str(_backfill(tmp_path))]
    )
    assert outcome.exit_code == 1 and "开跑前检查没过" in outcome.output
    assert "缺" in outcome.output and "不行" in outcome.output
    assert not out.joinpath(sb.JOURNAL_FILE).exists()


def test_the_command_runs_the_probe_and_writes_the_result(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(sb, "load_data", _loaded_data)
    monkeypatch.setattr(sb, "probe_windows", lambda loaded, gate: WINDOWS)
    monkeypatch.setattr(sb, "probe_items", lambda root_dir: _items())
    monkeypatch.setattr(tr, "trial_records", lambda root_dir: [])
    seen: dict[str, Any] = {}
    real = sb.run_probe

    def small(*args: Any, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return real(*args, **{**kwargs, "seeds": 1, "control_seeds": 0})

    monkeypatch.setattr(sb, "run_probe", small)
    out = tmp_path / "out"
    outcome = CliRunner().invoke(
        sb.app,
        ["run", "--out", str(out), "--backfill-result", str(_backfill(tmp_path)), "--workers", "1"],
    )
    assert outcome.exit_code == 0, outcome.output
    assert (seen["seeds"], seen["control_seeds"], seen["workers"]) == (150, 20, 1)
    assert seen["sources"] == {
        "fast_slow": {"stage1": None, "stage2": None},
        "T0002": {"stage1": None, "stage2": None},
    }
    assert "slow_bear_check: fast_slow 两段" in outcome.output
    assert "slow_bear_check: 误放率 slow_1929" in outcome.output
    assert "slow_bear_check: 误放率 crash_1963" in outcome.output and "完成" in outcome.output
    result = json.loads(out.joinpath(sb.RESULT_JSON).read_text(encoding="utf-8"))
    assert [row["config"] for row in result["configs"]] == ["fast_slow", "T0002"]
    assert out.joinpath(sb.JOURNAL_FILE).exists() and out.joinpath(sb.RESULT_MD).exists()


def test_the_sources_come_from_the_backfill_result_and_the_trial_records() -> None:
    items = sb.probe_items(ROOT_DIR)
    backfill = {
        "run_id": bc.RUN_ID,
        "rows": [{"config": "fast_slow", "status": "ok", "gate": _gate_rows(0.19, 0.013)}],
    }
    metrics = [r.metric for r in sb.common_rules(GATE, "us_market")]
    record = {
        "run_id": "trial-r0023",
        "trial_id": "T0004",
        "config": "primary",
        "market": "us_market",
        "status": "ok",
        "gate": [{"metric": m, "observed": 0.1} for m in metrics],
    }
    sources = sb.collect_sources(items, GATE, [record], backfill)
    assert list(sources) == [item.id for item in items]
    assert sources["fast_slow"]["stage1"] == {
        "run_id": "TRADING-2567-33 fast_slow",
        "values": {metrics[0]: 0.19, metrics[1]: 0.013, metrics[2]: None},
    }
    assert sources["fast_slow"]["stage2"] is None and sources["trend4"]["stage1"] is None
    assert sources["T0004"]["stage1"] == {
        "run_id": "trial-r0023",
        "values": dict.fromkeys(metrics, 0.1),
    }
    assert sources["T0004"]["stage2"] is None


def test_the_loaded_data_are_t0001s_markets_and_the_null_data_are_cut(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[str, str]] = []

    def market_data(market: str, gate: tr.TrialGate, plan: tr.TrialPlan, **_: Any) -> BenchData:
        seen.append((market, plan.trial_id))
        base = _qqq_data() if market == "qqq" else _us_data(end="2012-12-31")
        return replace(base, source={**base.source, "signal_snapshot_id": "sig-7ad8aa00b736"})

    monkeypatch.setattr(tr, "market_data", market_data)
    datasets, null, gate, loaded, root_dir = sb.load_data(Path("unused"))
    assert seen == [("qqq", "T0001"), ("us_market", "T0001")]
    assert gate == GATE and root_dir == ROOT_DIR
    assert datasets["us_market"].returns.index.max() > pd.Timestamp("2011-01-01")
    assert null["us_market"].returns.index.max() <= pd.Timestamp("2010-12-31")
    assert null["qqq"].returns.index.max() <= pd.Timestamp("2010-12-31")


def test_the_loader_refuses_other_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tr, "market_data", lambda *_, **__: US)
    with pytest.raises(ValueError, match="are not of sig-7ad8aa00b736"):
        sb.load_data(Path("unused"))
