"""TRADING-2565-56: the slow leg's return condition (period 5, 3.1) on synthetic data."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
from test_research_policy_table import DATA, GATE, LOADED, PROVENANCE, ROOT_DIR
from typer.testing import CliRunner

from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_bench.simulate import Rebalance, simulate
from ai_trading_system.research_probes import asymmetric_reentry as ar
from ai_trading_system.research_probes import plot_run
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import slow_leg_return as sl

pytestmark = pytest.mark.filterwarnings("ignore:invalid value encountered in log1p")

DAYS = pd.bdate_range("2001-01-01", "2001-04-30")


def _when(text: str) -> pd.Timestamp:
    return pd.Timestamp(text)


def _scores(fast: list[tuple[str, float]], slow: list[tuple[str, float]]) -> pd.DataFrame:
    def build(changes: list[tuple[str, float]]) -> pd.Series:
        series = pd.Series(np.nan, index=DAYS)
        for start, value in changes:
            series.loc[_when(start) :] = value
        return series

    return pd.DataFrame({"fast": build(fast), "slow": build(slow)}, index=DAYS)


# Month ends 01-31, 02-28, 03-30, 04-30. Fast is 1, 0 at the February month end, 1 again from
# 03-15; slow is 1 until 01-15, then 0, and 0.5 (a zero return) from 04-20.
SCORES = _scores(
    [("2001-01-01", 1.0), ("2001-02-15", 0.0), ("2001-03-15", 1.0)],
    [("2001-01-01", 1.0), ("2001-01-15", 0.0), ("2001-04-20", 0.5)],
)


def _row(decided: pd.DataFrame, day: str) -> tuple[float, float, float, bool]:
    row = decided.loc[_when(day)]
    return float(row["fast"]), float(row["slow"]), float(row["target"]), bool(row["r"])


# ---------------------------------------------------------------- the rule


def test_the_rule_decides_at_month_ends_and_takes_the_larger_score_for_the_slow_leg() -> None:
    decided = sl.decide(SCORES)
    assert list(decided.columns) == [
        "fast_score",
        "slow_score",
        "fast",
        "slow",
        "target",
        "trade",
        "r",
    ]
    assert decided.loc[: _when("2001-01-30"), ["fast", "slow", "target"]].isna().all().all()
    assert not decided.loc[: _when("2001-01-30"), "r"].any()
    # 01-31: fast 1, slow score 0 -> slow leg 1, exposure 1, an R month end.
    assert _row(decided, "2001-01-31") == (1.0, 1.0, 1.0, True)
    assert _row(decided, "2001-02-27") == (1.0, 1.0, 1.0, True)
    # 02-28: both 0 -> out; not R. The fast score's return on 03-15 waits for the month end.
    assert _row(decided, "2001-02-28") == (0.0, 0.0, 0.0, False)
    assert _row(decided, "2001-03-29") == (0.0, 0.0, 0.0, False)
    assert _row(decided, "2001-03-30") == (1.0, 1.0, 1.0, True)
    # 04-30: fast 1, slow 0.5 -> slow leg 1; 1 > 0.5 is still an R month end.
    assert _row(decided, "2001-04-30") == (1.0, 1.0, 1.0, True)
    trades = list(decided.index[decided["trade"]])
    assert trades == [_when(d) for d in ("2001-01-31", "2001-02-28", "2001-03-30", "2001-04-30")]
    assert decided["fast_score"].equals(SCORES["fast"])
    assert decided["slow_score"].equals(SCORES["slow"])


def test_the_rule_differs_from_monthly_fast_slow_only_in_the_r_cell() -> None:
    grid = [0.0, 0.5, 1.0]
    for fast in grid:
        for slow in grid:
            scores = _scores([("2001-01-01", fast)], [("2001-01-01", slow)])
            mine = sl.decide(scores).loc[_when("2001-01-31")]
            theirs = ar.decide(scores, "monthly").loc[_when("2001-01-31")]
            assert mine["fast"] == theirs["fast"] == fast
            assert mine["slow"] == max(fast, slow)
            assert mine["target"] - theirs["target"] == pytest.approx(0.5 * max(0.0, fast - slow))
            assert bool(mine["r"]) == (fast > slow)


def test_a_month_end_without_both_scores_keeps_the_previous_state() -> None:
    scores = SCORES.copy()
    scores.loc[_when("2001-02-28"), "slow"] = np.nan
    decided = sl.decide(scores)
    assert _row(decided, "2001-02-28") == (1.0, 1.0, 1.0, True)
    assert decided.loc[_when("2001-02-28"), "trade"]
    late = SCORES.copy()
    late.loc[: _when("2001-02-27"), "slow"] = np.nan
    decided = sl.decide(late)
    assert decided.loc[: _when("2001-02-27"), "target"].isna().all()
    assert _row(decided, "2001-02-28") == (0.0, 0.0, 0.0, False)


def test_the_targets_function_is_the_rule_on_the_bench_legs() -> None:
    entry = ar.source_entries(ROOT_DIR)["fast_slow"]
    params = sl.slow_return_params(entry)
    assert params == {"signals": [dict(s) for s in entry.params["signals"]]}
    data = DATA["qqq"]
    targets = sl.slow_return_targets(data.inputs, params)
    expected = sl.decide(ar.leg_scores(data.inputs, entry.params["signals"]))["target"]
    assert targets.index.equals(data.inputs.index)
    assert np.array_equal(targets.to_numpy(), expected.to_numpy(), equal_nan=True)
    with pytest.raises(ValueError, match="signals only"):
        sl.slow_return_targets(data.inputs, {**params, "reentry": "none"})


def test_zero_returns_are_counted_where_the_bench_and_the_charter_differ() -> None:
    # A zero return scores 0.5 on the bench; the charter reads ">= 0" as 1.
    # 01-31: fast 0.5, slow 0 -> bench 0.5, charter 1. 02-28: fast 1, slow 0.5 -> both 1.
    # 03-30: fast 0.5, slow 0.5 -> bench 0.5, charter 1. 04-30: fast 0, slow 0.5 -> bench 0.25,
    # charter 0.5.
    scores = _scores(
        [("2001-01-01", 0.5), ("2001-02-15", 1.0), ("2001-03-15", 0.5), ("2001-04-15", 0.0)],
        [("2001-01-01", 0.0), ("2001-02-15", 0.5)],
    )
    decided = sl.decide(scores)
    assert _row(decided, "2001-01-31") == (0.5, 0.5, 0.5, True)
    assert _row(decided, "2001-04-30") == (0.0, 0.5, 0.25, False)
    window = Window("w", "development", _when("2001-01-01").date(), None)
    assert sl.zero_scores(decided, window) == {"month_ends": 4, "fast": 2, "slow": 3, "differs": 3}
    # The window starts on a month end and takes it.
    late = Window("w", "development", _when("2001-02-28").date(), _when("2001-03-31").date())
    assert sl.zero_scores(decided, late) == {"month_ends": 2, "fast": 1, "slow": 2, "differs": 1}
    assert sl.zero_scores(sl.decide(SCORES), window) == {
        "month_ends": 4,
        "fast": 0,
        "slow": 1,
        "differs": 0,
    }


def test_an_unknown_rule_is_refused() -> None:
    data = DATA["qqq"]
    window = ar.market_window(LOADED, GATE, "qqq")
    scores = ar.leg_scores(data.inputs, ar.source_entries(ROOT_DIR)["fast_slow"].params["signals"])
    with pytest.raises(ValueError, match="unknown rule"):
        sl.probe_path("h_a", data, scores, window, LOADED)


# ---------------------------------------------------------------- the bench and the checks


def _paths(market: str) -> tuple[Any, Window, dict[str, ar.RulePath]]:
    entry = ar.source_entries(ROOT_DIR)["fast_slow"]
    data = DATA[market]
    window = ar.market_window(LOADED, GATE, market)
    scores = ar.leg_scores(data.inputs, entry.params["signals"])
    return entry, window, {n: sl.probe_path(n, data, scores, window, LOADED) for n in sl.RULES}


@pytest.mark.parametrize("market", ar.MARKETS)
def test_both_rules_reproduce_the_bench_day_by_day(market: str) -> None:
    entry, window, paths = _paths(market)
    data = DATA[market]
    assert sl.equivalence(data, entry, paths, window, LOADED) == {
        "fast_slow": True,
        "slow_return": True,
    }
    assert paths["fast_slow"].decided.equals(ar.decide(ar.scores_of(paths["fast_slow"]), "monthly"))
    bench = sl.bench_slow_return(data, ar.scores_of(paths["slow_return"]), window, LOADED)
    assert bench.daily.equals(paths["slow_return"].sim.daily)
    # The rule trades only at month ends: one day after each month end, nowhere else.
    turnover = paths["slow_return"].sim.daily["turnover"]
    traded = pd.DatetimeIndex(turnover.index[turnover > 0])
    decided = paths["slow_return"].decided
    trade_days = set(decided.index[decided["trade"]])
    positions = decided.index.get_indexer(traded)
    assert all(decided.index[p - 1] in trade_days for p in positions)


def test_the_equivalence_notices_a_different_simulation() -> None:
    entry, window, paths = _paths("qqq")
    data = DATA["qqq"]

    def spoiled(name: str) -> dict[str, ar.RulePath]:
        path = paths[name]
        changed = path.decided.copy()
        month_end = changed.index[changed["trade"]][-3]
        changed.loc[month_end, "target"] = 0.25
        sim = ar.simulate_on(
            data.returns,
            changed["target"],
            changed["trade"],
            start=window.start,
            end=window.end,
            cost_bps=LOADED.cost_bps,
        )
        return {**paths, name: ar.RulePath(name, changed, sim, path.legs, path.moves)}

    assert sl.equivalence(data, entry, spoiled("slow_return"), window, LOADED) == {
        "fast_slow": True,
        "slow_return": False,
    }
    assert sl.equivalence(data, entry, spoiled("fast_slow"), window, LOADED) == {
        "fast_slow": False,
        "slow_return": True,
    }


def test_the_bench_path_takes_the_larger_score_every_day() -> None:
    data = DATA["us_market"]
    window = ar.market_window(LOADED, GATE, "us_market")
    scores = ar.leg_scores(data.inputs, ar.source_entries(ROOT_DIR)["fast_slow"].params["signals"])
    targets = 0.5 * scores["fast"] + 0.5 * np.maximum(scores["fast"], scores["slow"])
    expected = simulate(
        data.returns,
        targets,
        start=window.start,
        end=window.end,
        rebalance=Rebalance("month_end", 0.0),
        cost_bps=LOADED.cost_bps,
    )
    assert sl.bench_slow_return(data, scores, window, LOADED).daily.equals(expected.daily)


def test_the_checks_catch_a_lookahead(monkeypatch: pytest.MonkeyPatch) -> None:
    entry = ar.source_entries(ROOT_DIR)["fast_slow"]
    data = DATA["qqq"]
    window = ar.market_window(LOADED, GATE, "qqq")
    assert sl.checks(data, entry, window) == []
    honest = sl.decide

    def peeking(scores: pd.DataFrame) -> pd.DataFrame:
        return honest(scores.shift(-5))

    monkeypatch.setattr(sl, "decide", peeking)
    assert sl.checks(data, entry, window)


# ---------------------------------------------------------------- R months


def _tiny_path() -> ar.RulePath:
    """SCORES on a market that gains 1% every day; holds from 02-02 to 04-30."""

    returns = pd.DataFrame(
        {"qqq": 0.01, "cash": 0.0, "lev2": 0.02, "lev3": 0.03}, index=DAYS, dtype=float
    )
    decided = sl.decide(SCORES)
    sim = ar.simulate_on(
        returns,
        decided["target"],
        decided["trade"],
        start=_when("2001-02-01").date(),
        end=None,
        cost_bps=0.0,
    )
    legs = ar.held_legs(decided, sim)
    return ar.RulePath("slow_return", decided, sim, legs, plot_run.leg_moves(legs))


def _days(start: str, end: str) -> int:
    return len(pd.bdate_range(start, end))


def test_r_months_count_decisions_by_their_held_days() -> None:
    path = _tiny_path()
    asset = pd.Series(0.01, index=DAYS)
    assert path.sim.daily.index[0] == _when("2001-02-02")
    # Held R decisions: 01-31 (02-02..03-01) and 03-30 (04-03..04-30); 02-28 holds 03-02..04-02.
    first, third = _days("2001-02-02", "2001-03-01"), _days("2001-04-03", "2001-04-30")
    found = sl.r_cell(path, asset)
    assert found["count"] == 2
    assert found["return"] == pytest.approx(1.01 ** (first + third) - 1, rel=1e-12)
    february = sl.r_cell(path, asset, "2001-02", "2001-02")
    assert february == {
        "count": 1,
        "return": pytest.approx(1.01 ** _days("2001-02-02", "2001-02-28") - 1),
    }
    # March holds the 01-31 decision on 03-01 only: one R decision, one day.
    assert sl.r_cell(path, asset, "2001-03", "2001-03") == {
        "count": 1,
        "return": pytest.approx(0.01),
    }
    assert sl.r_cell(path, asset, "2001-01", "2001-01") == {"count": 0, "return": None}


def test_the_window_line_compounds_each_held_month() -> None:
    path = _tiny_path()
    asset = pd.Series(0.01, index=DAYS)
    lengths = [
        _days("2001-02-02", "2001-03-01"),
        _days("2001-03-02", "2001-04-02"),
        _days("2001-04-03", "2001-04-30"),
    ]
    monthly = [1.01**n - 1 for n in lengths]
    found = sl.r_window(path, asset)
    assert found["months"] == 3 and found["r_months"] == 2
    assert found["share"] == pytest.approx(2 / 3)
    assert found["r_mean"] == pytest.approx((monthly[0] + monthly[2]) / 2, rel=1e-12)
    assert found["all_mean"] == pytest.approx(sum(monthly) / 3, rel=1e-12)


# ---------------------------------------------------------------- the long-bull split


def test_the_split_puts_the_band_exit_ceiling_on_the_fast_slow_side_only() -> None:
    _, window, paths = _paths("us_market")
    _, bull = GATE.markets["us_market"].segment_lists(window.window_id)
    asset = DATA["us_market"].returns["qqq"]
    found = sl.split_table(paths, asset, bull)
    for name, path in paths.items():
        expected = ar.split_rows(ar.missed_gains(path.decided, asset, path.sim.daily.index), bull)
        assert [
            {k: v for k, v in r.items() if k != "band_ceiling"} for r in found[name]["rows"]
        ] == expected
    for row in [*found["fast_slow"]["rows"], found["fast_slow"]["pooled"]]:
        total = row["false_exit"] + row["reentry_lag"] + row["slow"]
        assert row["band_ceiling"] == ar.lag_share(row["false_exit"], total)
    assert all("band_ceiling" not in r for r in found["slow_return"]["rows"])
    # The fast leg is the same in both rules, so its two parts are too.
    for mine, theirs in zip(found["slow_return"]["rows"], found["fast_slow"]["rows"], strict=True):
        assert mine["false_exit"] == theirs["false_exit"]
        assert mine["reentry_lag"] == theirs["reentry_lag"]


# ---------------------------------------------------------------- the rows


def _segments(slow: list[float | None], bull: float | None) -> dict[str, Any]:
    measured = [d for d in slow if d is not None]
    return {
        "slow_bear": [{"id": f"s{i}", "difference": d} for i, d in enumerate(slow)],
        "long_bull": [{"id": ar.QQQ_BULL, "annual_difference": bull}],
        "slow_bear_summary": {
            "count": len(measured),
            "median_difference": float(np.median(measured)) if measured else None,
            "positive": sum(d > 0 for d in measured),
        },
    }


def _rules(
    *,
    us: list[float | None],
    qqq: list[float | None],
    bull: float | None,
    geometric: float | None,
) -> dict[str, Any]:
    return {
        "us_market": {
            "segments": _segments(us, None),
            "comparison": {"geometric_annual_return": geometric},
            "cost": {"difference": -0.002},
        },
        "qqq": {
            "segments": _segments(qqq, bull),
            "comparison": {"geometric_annual_return": 0.0},
            "cost": {"difference": -0.004},
        },
    }


def _exits(us: tuple[int, int], qqq: tuple[int, int]) -> dict[str, Any]:
    return {
        m: {"fast_slow": {"exits": base}, "extra": extra}
        for m, (base, extra) in (("us_market", us), ("qqq", qqq))
    }


BASE = _rules(us=[0.1] * 10, qqq=[0.1, 0.1], bull=-0.033, geometric=0.004)


def _holds(new: dict[str, Any], exits: dict[str, Any] | None = None) -> dict[int, bool]:
    rows = sl.row_readings(new, BASE, exits or _exits((261, 130), (42, 21)))
    assert [r["row"] for r in rows] == [1, 2, 3, 4, 5]
    return {r["row"]: r["holds"] for r in rows}


def test_the_rows_hold_on_their_boundaries() -> None:
    edge = _rules(
        us=[0.08] * 5 + [0.09] * 3 + [-0.01, -0.02],
        qqq=[0.001, 0.0005],
        bull=-0.0165,
        geometric=0.004,
    )
    assert _holds(edge) == {1: True, 2: True, 3: True, 4: True, 5: True}
    assert ar.first_four(sl.row_readings(edge, BASE, _exits((261, 130), (42, 21)))) is True


def test_the_rows_fail_just_past_their_boundaries() -> None:
    low = _rules(us=[0.0799] * 10, qqq=[0.1, 0.0], bull=-0.0166, geometric=0.0039)
    assert _holds(low, _exits((261, 131), (42, 21))) == {
        1: False,
        2: False,
        3: False,
        4: False,
        5: False,
    }
    seven = _rules(us=[0.2] * 7 + [-0.01] * 3, qqq=[0.1, 0.1, 0.1], bull=None, geometric=None)
    found = _holds(seven, _exits((261, -12), (42, 22)))
    assert found == {1: False, 2: False, 3: False, 4: False, 5: False}


def test_rows_3_and_4_print_two_decimals() -> None:
    # -1.66 against -1.65 both print -1.7 with one decimal; the row fails and must look it.
    new = _rules(us=[0.1] * 10, qqq=[0.1, 0.1], bull=-0.0166, geometric=0.006)
    rows = {r["row"]: r for r in sl.row_readings(new, BASE, _exits((261, 50), (42, 7)))}
    assert rows[3]["holds"] is False
    assert sl._reading_text(rows[3]) == "慢腿快回 -1.66，fast_slow -3.30"
    assert sl._threshold(rows[3]) == "≥ -1.65"
    assert sl._reading_text(rows[4]) == (
        "慢腿快回 +0.60，fast_slow +0.40 点/年（25 bps 下 -0.20、-0.20，只印）"
    )
    assert sl._threshold(rows[4]) == "≥ +0.40"
    assert sl._reading_text(rows[1]) == "D 中位 +10.0，D > 0 10/10"


def test_more_exits_can_be_fewer() -> None:
    assert _holds(BASE, _exits((261, -5), (42, -1)))[5] is True


def test_the_readings_carry_both_sides_and_the_cost_column() -> None:
    new = _rules(us=[0.1] * 10, qqq=[0.1, 0.1], bull=-0.02, geometric=0.006)
    rows = {r["row"]: r for r in sl.row_readings(new, BASE, _exits((261, 50), (42, 7)))}
    assert rows[1]["reading"] == {"median_difference": 0.1, "positive": 10, "count": 10}
    assert rows[2]["reading"] == {"differences": [0.1, 0.1]}
    assert rows[3]["reading"] == {"slow_return": -0.02, "fast_slow": -0.033}
    assert rows[3]["threshold"] == pytest.approx(-0.0165)
    assert rows[4]["reading"] == {
        "slow_return": 0.006,
        "fast_slow": 0.004,
        "cost_25": {"slow_return": -0.002, "fast_slow": -0.002},
    }
    assert rows[4]["threshold"] == 0.004
    assert rows[5]["reading"] == {
        "us_market": {"extra": 50, "fast_slow_exits": 261},
        "qqq": {"extra": 7, "fast_slow_exits": 42},
    }


# ---------------------------------------------------------------- whole runs on synthetic data


@pytest.fixture(scope="module")
def evaluated() -> Iterator[dict[tuple[str, str], dict[str, Any]]]:
    cache: dict[tuple[str, str], dict[str, Any]] = {}
    original = pt.evaluate

    def remembered(row: pt.Row, market: str, *args: Any, **kwargs: Any) -> dict[str, Any]:
        key = (row.id, market)
        if key not in cache:
            cache[key] = original(row, market, *args, **kwargs)
        return cache[key]

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(pt, "evaluate", remembered)
        yield cache


@pytest.fixture(scope="module")
def same_run(evaluated: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    """A decision table printed from this run's fast_slow, as the policy table printed its own."""

    table: dict[str, Any] = {}
    for market in ar.MARKETS:
        row = pt.evaluate(ar.bench_row("fast_slow", ROOT_DIR), market, DATA[market], LOADED, GATE)
        segs = row["segments"]
        summary = segs["slow_bear_summary"]
        table[market] = {
            "slow_bear": {s["id"]: round(s["difference"], 3) for s in segs["slow_bear"]},
            "summary": {
                "count": summary["count"],
                "median_difference": round(summary["median_difference"], 3),
                "positive": summary["positive"],
                "min_difference": round(summary["min_difference"], 3),
            },
            "long_bull": {
                s["id"]: round(s["annual_difference"], 3)
                for s in segs["long_bull"]
                if market == "qqq"
            },
        }
    return table


@pytest.fixture(scope="module")
def probe(same_run: dict[str, Any], tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    out = tmp_path_factory.mktemp("run")
    result = sl.run_probe(
        DATA, LOADED, GATE, ROOT_DIR, provenance=PROVENANCE, out_dir=out, decision_table=same_run
    )
    return {"result": result, "out": out}


def test_the_real_decision_table_stops_the_run_on_synthetic_data(
    evaluated: dict[tuple[str, str], dict[str, Any]], tmp_path: Path
) -> None:
    result = sl.run_probe(DATA, LOADED, GATE, ROOT_DIR, provenance=PROVENANCE, out_dir=tmp_path)
    assert result["stopped"] and not result["complete"]
    assert result["stop"]["us_market"] and result["stop"]["qqq"]
    assert result["run_id"] == sl.RUN_ID and result["owner_decision"] == sl.OWNER_DECISION
    assert "direction" not in result
    assert "slow_return" not in result and "figures" not in result
    assert not (tmp_path / sl.FIGURES).exists()
    text = sl.render(result)
    assert "同一次运行的 fast_slow 和决定表不同（美股大盘 1929–2010）" in text
    assert "第 1 到 4 行都成立：没有读完" in text
    assert "不读新规则，不出图" in text
    assert "| 行 | 读数 | 门槛 | 按判法 |" not in text


def test_the_run_reads_five_rows_on_its_own_matched_benchmark(probe: dict[str, Any]) -> None:
    result = probe["result"]
    assert not result["stopped"] and result["complete"]
    assert result["run_id"] == sl.RUN_ID and result["step"] == "run"
    assert result["owner_decision"] == sl.OWNER_DECISION and "direction" not in result
    assert result["checks"] == {"us_market": [], "qqq": []}
    assert result["same_run"] == {"us_market": [], "qqq": []}
    assert result["equivalence"] == {
        m: {"fast_slow": True, "slow_return": True} for m in ar.MARKETS
    }
    assert set(result["rules"]["qqq"]) == {"fast_slow"}
    rows = {r["row"]: r for r in result["readings"]}
    assert sorted(rows) == [1, 2, 3, 4, 5]
    new = result["slow_return"]
    base = {m: result["rules"][m]["fast_slow"] for m in ar.MARKETS}
    assert result["readings"] == sl.row_readings(new, base, result["exits"])
    assert (
        rows[4]["reading"]["slow_return"]
        == new["us_market"]["comparison"]["geometric_annual_return"]
    )
    assert (
        rows[4]["reading"]["fast_slow"]
        == base["us_market"]["comparison"]["geometric_annual_return"]
    )
    assert result["first_four"] == all(rows[n]["holds"] for n in (1, 2, 3, 4))
    assert [len(new[m]["gate"]) for m in ar.MARKETS] == [5, 2]
    assert result["production_effect"] == "none" and result["broker_action"] == "none"


def test_the_new_rule_is_read_like_h_a(probe: dict[str, Any]) -> None:
    result = probe["result"]
    for market in ar.MARKETS:
        data = DATA[market]
        window = ar.market_window(LOADED, GATE, market)
        _, _, paths = _paths(market)
        expected = ar.evaluate_h_a(paths["slow_return"], data, LOADED, GATE, market, window)
        # Its events draw the line at the new rule's own mean exposure; they are left out.
        del expected["events"]
        assert result["slow_return"][market] == {**expected, "row": "slow_return"}


def _line_words(line: dict[str, Any]) -> str:
    reference, threshold = line["reference_exposure"], line["threshold"]
    return (
        f"减仓线：fast_slow 平均敞口 {reference:.3f}；减仓 ≤ 0.75 × {reference:.3f} = "
        f"{threshold:.3f}，回来 ≥ {reference:.3f}；两条规则共用（第 5 期章程第 6 节）"
    )


def test_both_rules_share_one_derisk_line(probe: dict[str, Any]) -> None:
    result = probe["result"]
    for market in ar.MARKETS:
        data = DATA[market]
        window = ar.market_window(LOADED, GATE, market)
        slow, _ = GATE.markets[market].segment_lists(window.window_id)
        _, _, paths = _paths(market)
        reference = float(paths["fast_slow"].sim.exposure.mean())
        own = float(paths["slow_return"].sim.exposure.mean())
        assert own > reference  # the new rule holds more, so its own line would sit higher
        line = result["derisk_line"][market]
        assert line["ratio"] == 0.75 and line["reference_exposure"] == reference
        assert line["threshold"] == pytest.approx(0.75 * reference, rel=1e-12)
        # fast_slow's line is its own mean: its lags are H-A's and the decision table's.
        assert result["lags"][market]["fast_slow"] == ar.lag_table(
            paths["fast_slow"], data, LOADED, slow
        )
        assert result["lags"][market]["slow_return"] == sl.lag_table(
            paths["slow_return"], data, LOADED, slow, reference
        )
        # H-A's table is this one on the rule's own mean exposure.
        assert ar.lag_table(paths["slow_return"], data, LOADED, slow) == sl.lag_table(
            paths["slow_return"], data, LOADED, slow, own
        )
        episodes = GATE.markets[market].event_episodes
        episodes = LOADED.settings.episodes if episodes is None else episodes
        for name, path in paths.items():
            table = sl.event_table(path.sim, DATA[market], LOADED, episodes, reference)
            assert table["threshold"] == line["threshold"]
            assert result["events"][market][name] == sl.event_summary(table)
        assert sl.event_table(paths["slow_return"].sim, data, LOADED, episodes, own)[
            "threshold"
        ] == pytest.approx(0.75 * own)
        assert result["zero_scores"][market] == sl.zero_scores(paths["slow_return"].decided, window)


def test_the_lag_table_draws_the_line_it_is_given() -> None:
    _, window, paths = _paths("us_market")
    data = DATA["us_market"]
    slow, _ = GATE.markets["us_market"].segment_lists(window.window_id)
    path = paths["slow_return"]
    high = sl.lag_table(path, data, LOADED, slow, 10.0)
    low = sl.lag_table(path, data, LOADED, slow, 1e-9)
    assert high != low
    episodes = sl.event_table(path.sim, data, LOADED, slow, 10.0)["episodes"]
    assert [high[s.id]["exit_lag_days"] for s in slow] == [
        episodes.get(s.id, {}).get("exit_lag_days") for s in slow
    ]


def test_the_two_fast_slow_paths_print_the_same_numbers(probe: dict[str, Any]) -> None:
    result = probe["result"]
    _, window, paths = _paths("qqq")
    row = result["rules"]["qqq"]["fast_slow"]
    assert sl.same_run(paths["fast_slow"], DATA["qqq"], LOADED, GATE, "qqq", window, row) == []
    shifted = {
        **row,
        "comparison": {
            **row["comparison"],
            "geometric_annual_return": row["comparison"]["geometric_annual_return"] + 1e-9,
        },
    }
    assert sl.same_run(paths["fast_slow"], DATA["qqq"], LOADED, GATE, "qqq", window, shifted) == [
        "同平均敞口几何差"
    ]
    bear = row["segments"]["slow_bear"]
    moved = {
        **row,
        "segments": {
            **row["segments"],
            "slow_bear": [{**bear[0], "difference": bear[0]["difference"] + 1e-9}, *bear[1:]],
        },
    }
    assert sl.same_run(paths["fast_slow"], DATA["qqq"], LOADED, GATE, "qqq", window, moved) == [
        bear[0]["id"]
    ]
    dropped = {**row, "segments": {**row["segments"], "slow_bear": bear[1:]}}
    assert sl.same_run(paths["fast_slow"], DATA["qqq"], LOADED, GATE, "qqq", window, dropped) == [
        "slow_bear 段清单"
    ]
    assert sl.same_run(
        paths["fast_slow"], DATA["qqq"], LOADED, GATE, "qqq", window, {"status": "error"}
    ) == ["评估台的 fast_slow 没有算出"]


def test_a_different_fast_slow_path_stops_before_the_new_rule(
    same_run: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(sl, "same_run", lambda *args: ["同平均敞口几何差"])
    result = sl.run_probe(
        DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        provenance=PROVENANCE,
        out_dir=tmp_path,
        decision_table=same_run,
    )
    assert not result["complete"] and result["readings"] == [] and result["first_four"] is None
    assert result["slow_return"] == {} and result["figures"] == {}
    text = sl.render(result)
    assert "fast_slow 两条算法（QQQ 1999–2010）：不同：同平均敞口几何差" in text
    assert "第 1 到 4 行都成立：没有读完" in text


def test_exits_are_counted_by_leg_over_the_whole_windows(probe: dict[str, Any]) -> None:
    result = probe["result"]
    for market in ar.MARKETS:
        _, _, paths = _paths(market)
        found = result["exits"][market]
        assert found["fast_slow"] == ar.exit_counts(paths["fast_slow"].moves)
        assert found["slow_return"] == ar.exit_counts(paths["slow_return"].moves)
        assert found["extra"] == found["slow_return"]["exits"] - found["fast_slow"]["exits"]
        # The fast leg is the same rule in both.
        assert found["slow_return"]["fast_exits"] == found["fast_slow"]["fast_exits"]


def test_the_r_lines_cover_every_segment_and_the_window(probe: dict[str, Any]) -> None:
    result = probe["result"]
    for market in ar.MARKETS:
        window = ar.market_window(LOADED, GATE, market)
        slow, bull = GATE.markets[market].segment_lists(window.window_id)
        _, _, paths = _paths(market)
        asset = DATA[market].returns["qqq"]
        assert result["r_months"][market] == [
            {"id": s.id, "kind": kind, **sl.r_cell(paths["slow_return"], asset, s.start, s.end)}
            for kind, specs in (("slow_bear", slow), ("long_bull", bull))
            for s in specs
        ]
        assert result["r_window"][market] == sl.r_window(paths["slow_return"], asset)
        decided = paths["slow_return"].decided
        month_ends = decided.loc[decided["trade"]]
        r = month_ends["fast_score"] > month_ends["slow_score"]
        assert result["r_window"][market]["r_months"] <= int(r.sum())


def test_the_run_draws_fast_slow_against_the_new_rule(probe: dict[str, Any]) -> None:
    result, out = probe["result"], probe["out"]
    for market in ar.MARKETS:
        records = result["figures"][market]
        assert [r["name"] for r in records] == [f"run-{market}-{p}.svg" for p in "ABC"]
        exits = result["exits"][market]
        window = records[0]["counts"]["window"]
        for role, name in (("base", "fast_slow"), ("compared", "slow_return")):
            assert window[role]["fast"]["exit"] == exits[name]["fast_exits"]
            assert window[role]["slow"]["exit"] == exits[name]["slow_exits"]
        for record in records:
            assert (out / sl.FIGURES / record["name"]).stat().st_size == record["bytes"]
    figure = (out / sl.FIGURES / "run-qqq-A.svg").read_text(encoding="utf-8")
    assert "月频 fast_slow" in figure and "慢腿快回" in figure and sl.RUN_ID in figure
    assert _line_words(result["derisk_line"]["qqq"]) in figure


def test_the_report_carries_the_key_lines_and_no_data_rows(probe: dict[str, Any]) -> None:
    result = probe["result"]
    text = sl.render(result)
    four = "是" if result["first_four"] else "否"
    for line in (
        "# 慢腿的回来条件：读数",
        "- 判法：第 5 期章程第 3.1 节判法表第 1 到 5 行，机械比较；对错由循环线程填进记分表。"
        f"第 1 到 4 行都成立：{four}",
        f"- 规则：{sl.RULE_TEXT}。",
        f"- 美股大盘 1929–2010：{_line_words(result['derisk_line']['us_market'])}；"
        "两条规则的减仓事件和滞后都用这条线。",
        "收益恰为 0 的月底（QQQ 1999–2010，窗口里",
        "## 减仓事件（两条规则共用减仓线，评估台事件表口径，全窗口）",
        "同一次运行的 fast_slow 和决定表（TRADING-2565-42）相同",
        "等同核对（美股大盘 1929–2010）：两条规则的探针路径都逐日复现评估台",
        "fast_slow 两条算法（QQQ 1999–2010）：同数",
        "| 行 | 读数 | 门槛 | 按判法 |",
        "## R 月（快腿分数 > 慢腿分数的月底之后那个持有月，只印）",
        "带宽退出的上限（假退出份额）",
        "只印，不判，这题不走试跑门",
        "production_effect=none；broker_action=none。",
    ):
        assert line in text
    tables = [line for line in text.splitlines() if line.startswith("| ")]
    assert len(tables) < 150
    assert not any(line.startswith(("| 19", "| 20")) for line in tables)


def test_the_results_are_plain_json(probe: dict[str, Any], tmp_path: Path) -> None:
    sl.write_json(probe["result"], tmp_path / sl.RUN_JSON)
    written = json.loads((tmp_path / sl.RUN_JSON).read_text(encoding="utf-8"))
    assert written["run_id"] == sl.RUN_ID and written["provenance"] == PROVENANCE


# ---------------------------------------------------------------- commands


def test_check_rows_pass_on_synthetic_data() -> None:
    rows = sl.check_rows(DATA, LOADED, GATE, ROOT_DIR)
    assert [r["market"] for r in rows] == list(ar.MARKETS)
    for row in rows:
        assert row["ok"], row
        assert row["missing_days"] == 0
        assert row["equivalence"] == {"fast_slow": True, "slow_return": True}


def test_the_module_reads_no_research_record_or_validation_window() -> None:
    source = Path(sl.__file__).read_text(encoding="utf-8")
    for forbidden in ("window_reads", "runs/", "_screens", "data/raw", "validation_"):
        assert forbidden not in source


def test_the_module_draws_no_line_of_its_own_per_rule() -> None:
    # H-A's event and lag tables put each rule on its own mean exposure (two lines).
    source = Path(sl.__file__).read_text(encoding="utf-8")
    for forbidden in ("ar.event_table", "ar.lag_table"):
        assert forbidden not in source


def _synthetic_load(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sl, "load_inputs", lambda root: (DATA, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(sl, "git_provenance", lambda root: PROVENANCE)


def test_the_check_command_prints_one_line_per_market(monkeypatch: pytest.MonkeyPatch) -> None:
    _synthetic_load(monkeypatch)
    found = CliRunner().invoke(sl.app, ["check"])
    assert found.exit_code == 0, found.output
    lines = [line for line in found.output.splitlines() if line.startswith("slow_leg_return:")]
    assert len(lines) == 2
    assert all(line.endswith("可以") for line in lines)
    assert "fast_slow 同、slow_return 同" in lines[0]


def test_the_run_command_refuses_existing_results(tmp_path: Path) -> None:
    runner = CliRunner()
    for number, name in enumerate((sl.RUN_JSON, sl.RUN_MD, sl.FIGURES)):
        out = tmp_path / f"out{number}"
        out.mkdir()
        if name == sl.FIGURES:
            (out / name).mkdir()
        else:
            (out / name).write_text("{}", encoding="utf-8")
        found = runner.invoke(sl.app, ["run", "--out", str(out)])
        assert found.exit_code == 1 and "拒绝覆盖" in found.output


def test_the_run_command_writes_its_files_and_stops_on_the_real_table(
    evaluated: dict[tuple[str, str], dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _synthetic_load(monkeypatch)
    found = CliRunner().invoke(sl.app, ["run", "--out", str(tmp_path / "run")])
    assert found.exit_code == 1
    assert "没有读完" in found.output
    written = json.loads((tmp_path / "run" / sl.RUN_JSON).read_text(encoding="utf-8"))
    assert written["stopped"] is True and written["provenance"] == PROVENANCE
    report = (tmp_path / "run" / sl.RUN_MD).read_text(encoding="utf-8")
    assert report.startswith("# 慢腿的回来条件：读数")


def test_the_run_command_prints_the_rows_when_it_reads(
    same_run: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _synthetic_load(monkeypatch)
    original = sl.run_probe

    def with_table(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return original(*args, **kwargs, decision_table=same_run)

    monkeypatch.setattr(sl, "run_probe", with_table)
    found = CliRunner().invoke(sl.app, ["run", "--out", str(tmp_path / "run")])
    assert found.exit_code == 0, found.output
    lines = [line for line in found.output.splitlines() if line.startswith("slow_leg_return: 第 ")]
    assert [line.split("第 ")[1].split(" 行")[0] for line in lines] == ["1", "2", "3", "4", "5"]
    assert found.output.rstrip().endswith(f"完成；结果目录 {tmp_path / 'run'}")
    assert len(list((tmp_path / "run" / sl.FIGURES).iterdir())) == 6
