from __future__ import annotations

import json
import math
from collections.abc import Callable
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
from ai_trading_system.research_bench.data import BenchData, from_us_market, with_signal_sources
from ai_trading_system.research_bench.falsify import bootstrap_interval
from ai_trading_system.research_bench.settings import Window
from ai_trading_system.research_probes import gate_power as gp
from ai_trading_system.research_probes import ruler_power as rp

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
GATE = tr.load_gate(PROJECT_ROOT / "research/_trials/prereg.yaml")
ROOT_DIR = lg.research_root(LOADED)
US_END = "1945-12-31"
# Small windows keep the whole-run tests fast: three-year checks, four-year long windows, starts
# every fourth year (checks and long windows 1930, 1934, 1938, 1942; pairs 1930, 1934, 1938).
RUN: dict[str, Any] = {
    "first_year": 1930,
    "step": 4,
    "check_years": 3,
    "spans": (4,),
    "pair_spans": (4,),
    "noise_levels": (0.0, 3.0),
}
RULE_IDS = ["null", "noise_0", "noise_3", "q1a", "trend4", "ma10"]
RULE_IDS += ["dd_null", "dd_noise_0", "dd_noise_3"]


def _us_data(end: str = US_END, seed: int = 5) -> BenchData:
    """Daily market returns whose volatility changes month by month (as in the gate_power
    tests)."""

    index = pd.bdate_range("1926-07-01", end, name="date")
    rng = np.random.default_rng(seed)
    codes, uniques = pd.factorize(index.to_period("M"))
    monthly_vol = np.exp(rng.normal(math.log(0.01), 0.6, len(uniques)))
    factors = pd.DataFrame(
        {
            "Mkt-RF": rng.normal(0.0003, 1.0, len(index)) * monthly_vol[codes],
            "SMB": 0.0,
            "HML": 0.0,
            "RF": 0.0001,
        },
        index=index,
    )
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id="sig-test")
    return with_signal_sources(data, {}, {}, snapshot_id="sig-test")


US = _us_data()


@pytest.fixture(scope="module")
def ruler_run(tmp_path_factory: pytest.TempPathFactory) -> tuple[dict[str, Any], Path]:
    journal = tmp_path_factory.mktemp("ruler") / rp.JOURNAL_FILE
    return rp.run_ruler(US, LOADED, GATE, ROOT_DIR, journal=journal, **RUN), journal


@pytest.fixture(scope="module")
def ruler(ruler_run: tuple[dict[str, Any], Path]) -> dict[str, Any]:
    return ruler_run[0]


def _rule(rule_id: str, data: BenchData = US) -> gp.PowerRule:
    rules = rp.power_rules(data, ROOT_DIR, (0.0, 3.0))
    return next(rule for rule in rules if rule.rule_id == rule_id)


# --- planted drawdown rules ---------------------------------------------------------------------


def test_next_month_drawdown_is_the_deepest_fall_from_a_running_peak() -> None:
    index = pd.DatetimeIndex(
        ["2000-01-31", "2000-02-01", "2000-02-02", "2000-02-03", "2000-02-29"]
        + ["2000-03-01", "2000-03-02", "2000-03-31"]
    )
    returns = pd.Series([0.0, 0.10, -0.20, 0.05, 0.0, -0.10, 0.05, 0.01], index=index)

    out = rp.next_month_drawdown(returns)

    assert list(out.index) == [pd.Timestamp(d) for d in ("2000-01-31", "2000-02-29", "2000-03-31")]
    # February: up to 1.1, down to 0.88: a fall of a fifth from the peak.
    assert out.iloc[0] == pytest.approx(0.2)
    # March: from the month end's level 1.0 down to 0.9, then up to 0.945 and 0.95445.
    assert out.iloc[1] == pytest.approx(0.1)
    assert math.isnan(out.iloc[2])  # the last month end has no next month


def test_a_month_that_only_rises_has_no_drawdown_and_a_short_month_has_none() -> None:
    index = pd.DatetimeIndex(["2000-01-31", "2000-02-01", "2000-02-29", "2000-03-31"])
    returns = pd.Series([0.0, 0.01, 0.02, 0.03], index=index)

    out = rp.next_month_drawdown(returns)

    assert out.iloc[0] == 0.0  # February rises on both days
    assert math.isnan(out.iloc[1])  # March has one return


def test_drawdown_targets_are_one_minus_the_percentile_of_the_noisy_forecast() -> None:
    inputs = US.inputs
    signal = rp.next_month_drawdown(inputs["ndx_return"])
    draws = np.random.default_rng(rp.HOLDOUT_SEED).standard_normal(len(signal))
    score = gp.expanding_percentile(gp.planted_forecast(signal, 1.0, draws), gp.WARMUP_MONTHS)
    expected = (1.0 - score).reindex(pd.DatetimeIndex(inputs.index)).ffill()

    targets = rp.drawdown_targets(inputs, 1.0)

    pd.testing.assert_series_equal(targets, expected.rename("target"))
    assert targets.dropna().between(0.0, 1.0).all()
    # Decided at month ends and held until the next one.
    ends = gp.month_ends(pd.DatetimeIndex(inputs.index))
    changes = targets.dropna().ne(targets.dropna().shift()).iloc[1:]
    assert set(changes[changes].index) <= set(ends)


def test_the_drawdown_rule_without_noise_knows_next_months_fall() -> None:
    targets = rp.drawdown_targets(US.inputs, 0.0)
    signal = rp.next_month_drawdown(US.inputs["ndx_return"])
    paired = pd.DataFrame({"exposure": targets.reindex(signal.index), "fall": signal}).dropna()

    ranks = paired.rank()  # a rank correlation without scipy
    assert ranks["exposure"].corr(ranks["fall"]) < -0.9


def test_the_rules_are_gate_powers_and_the_held_out_drawdown_family() -> None:
    rules = rp.power_rules(US, ROOT_DIR, (0.0, 3.0))

    assert [rule.rule_id for rule in rules] == RULE_IDS
    assert [rp.family(rule.rule_id, rule.kind) for rule in rules] == (
        ["volatility"] * 3 + ["real"] * 3 + ["drawdown"] * 3
    )
    planted = {rule.rule_id: rule for rule in rules}
    pd.testing.assert_series_equal(planted["noise_3"].targets, gp.planted_targets(US.inputs, 3.0))
    pd.testing.assert_series_equal(
        planted["dd_noise_3"].targets, rp.drawdown_targets(US.inputs, 3.0)
    )
    assert planted["dd_null"].noise is None and planted["dd_noise_3"].noise == 3.0
    # Its own draws: the held-out null is not the volatility null.
    assert not planted["dd_null"].targets.equals(planted["null"].targets)


# --- windows ------------------------------------------------------------------------------------


def test_the_check_windows_are_gate_powers_windows() -> None:
    index = pd.DatetimeIndex(US.returns.index)

    ours = rp.check_windows(index)
    theirs = gp.us_windows(index)

    assert [(w.window_id, w.start, w.end) for w in ours] == [
        (w.window_id, w.start, w.end) for w in theirs
    ]
    assert [w.window_id for w in ours] == ["us_1930", "us_1932", "us_1934"]


def test_long_windows_end_on_the_last_day_of_their_last_year() -> None:
    index = pd.DatetimeIndex(US.returns.index)

    windows = rp.span_windows(index, 4, first_year=1930, step=4)

    assert [(w.window_id, w.start, w.end) for w in windows] == [
        ("us4_1930", date(1930, 1, 1), date(1933, 12, 31)),
        ("us4_1934", date(1934, 1, 1), date(1937, 12, 31)),
        ("us4_1938", date(1938, 1, 3), date(1941, 12, 31)),
        ("us4_1942", date(1942, 1, 1), date(1945, 12, 31)),
    ]
    assert rp.span_windows(index, 17, first_year=1930, step=4) == []  # 1930-1946 runs past 1945


def test_the_cutoff_ends_every_window() -> None:
    index = pd.bdate_range("2000-01-03", "2011-06-30")

    assert rp.span_windows(index, 12, first_year=2000, step=1) == []  # to 2011-12-31
    ends = [(w.window_id, w.end) for w in rp.span_windows(index, 11, first_year=2000, step=1)]
    assert ends == [("us11_2000", date(2010, 12, 31))]  # the data run on, the window stops
    assert [w.window_id for w in rp.span_windows(index, 10, first_year=2000)] == ["us10_2000"]
    # A check window may end on the last date of the data, not after it.
    short = pd.bdate_range("1926-07-01", "1945-02-19")
    checks = rp.check_windows(short, first_year=1930, step=4, years=3)
    assert [w.window_id for w in checks][-1] == "us_1942" and checks[-1].end == date(1945, 2, 19)
    assert rp.check_windows(short[:-1], first_year=1930, step=4, years=3)[-1].window_id == "us_1938"


def test_a_start_year_before_the_data_gives_no_window() -> None:
    index = pd.DatetimeIndex(US.returns.index)  # from 1926-07-01

    long = rp.span_windows(index, 4, first_year=1924, step=2)
    checks = rp.check_windows(index, first_year=1924, step=2, years=3)

    assert [w.window_id for w in long[:2]] == ["us4_1926", "us4_1928"]
    assert long[0].start == date(1926, 7, 1) and checks[0].window_id == "us_1926"


def test_a_check_window_starting_inside_its_long_window_is_no_pair() -> None:
    long = {
        4: [
            Window(
                window_id="us4_1930",
                role="development",
                start=date(1930, 1, 2),
                end=date(1934, 6, 29),
            )
        ]
    }
    check = Window(
        window_id="us_1934", role="development", start=date(1934, 1, 2), end=date(1937, 2, 19)
    )

    assert rp.pair_list(long, [check], (4,)) == {4: []}


def test_pairs_join_a_long_window_to_the_check_window_starting_the_next_year() -> None:
    index = pd.DatetimeIndex(US.returns.index)
    checks = rp.check_windows(index, first_year=1930, step=4, years=3)
    long = {4: rp.span_windows(index, 4, first_year=1930, step=4)}

    pairs = rp.pair_list(long, checks, (4,))

    assert pairs == {4: [("us4_1930", "us_1934"), ("us4_1934", "us_1938"), ("us4_1938", "us_1942")]}
    assert rp.pair_list({}, checks, (4,)) == {4: []}


def test_the_longest_windows_come_first() -> None:
    def window(window_id: str) -> Window:
        return Window(window_id=window_id, role="development", start=date(1930, 1, 2), end=None)

    full, checks = window(rp.FULL_ID), [window("us_1930"), window("us_1932")]
    long = {4: [window("us4_1930")], 6: [window("us6_1930"), window("us6_1932")]}

    ordered = rp.ordered_windows(full, long, checks, 3)

    assert [(w.window_id, span) for w, span in ordered] == [
        (rp.FULL_ID, None),
        ("us6_1930", 6),
        ("us6_1932", 6),
        ("us4_1930", 4),
        ("us_1930", 3),
        ("us_1932", 3),
    ]


# --- one evaluation -----------------------------------------------------------------------------


def test_the_bounds_at_each_confidence_are_the_bootstrap_intervals() -> None:
    rng = np.random.default_rng(11)
    strategy, benchmark = rng.normal(0.0003, 0.01, (2, 800))
    cash = np.full(800, 0.0001)
    # More draws than one chunk, so the chunks must match as well.
    loaded = replace(
        FULL,
        settings=FULL.settings.model_copy(
            update={
                "falsification": FULL.settings.falsification.model_copy(
                    update={"bootstrap_draws": 600}
                )
            }
        ),
    )

    bounds = rp.bootstrap_bounds(strategy, benchmark, cash, loaded, (0.95, 0.99, 0.997))

    for confidence in (0.95, 0.99, 0.997):
        low, high = bootstrap_interval(strategy, benchmark, cash, loaded, confidence)
        assert bounds[rp.confidence_key(confidence)] == [
            pytest.approx(low[rp.TAIL_FIELD], abs=0, rel=0),
            pytest.approx(high[rp.TAIL_FIELD], abs=0, rel=0),
        ]
    with pytest.raises(ValueError, match="confidence"):
        rp.bootstrap_bounds(strategy, benchmark, cash, loaded, (1.0,))
    # A benchmark without a left tail gives no finite improvement: no bounds, as in the interval.
    flat = np.zeros(800)
    assert rp.bootstrap_bounds(strategy, flat, cash, loaded, (0.95, 0.997)) == {
        "0.95": [None, None],
        "0.997": [None, None],
    }
    assert bootstrap_interval(strategy, flat, cash, loaded, 0.95)[0][rp.TAIL_FIELD] is None


def test_an_evaluation_is_gate_powers_with_the_holds_at_each_confidence() -> None:
    rule = _rule("noise_0")
    loaded = gp.with_episodes(LOADED, gp.US_EPISODES)
    window = rp.check_windows(pd.DatetimeIndex(US.returns.index))[0]

    theirs = gp.evaluate(US, rule.targets, window, loaded, GATE, rule.rebalance)
    ours = rp.evaluate(
        US, rule.targets, window, loaded, GATE, rule.rebalance, (0.95, gp.STRICT_CONFIDENCE, 0.997)
    )

    assert ours["gate"] == theirs["gate"]
    for key in ("window_id", "start", "end", "return_days", "saturdays", "episodes", "comparison"):
        assert ours[key] == theirs[key], key
    assert ours["holds_at"]["0.95"] == {row["metric"]: row["holds"] for row in theirs["gate"]}
    assert ours["holds_at"]["0.9875"] == {
        row["metric"]: row["holds"] for row in theirs["gate_strict"]
    }
    ci = next(row for row in ours["gate"] if row["metric"] == rp.CI_METRIC)
    assert ci["observed"] == ours["tail_bounds"]["0.95"][0]
    strict = next(row for row in theirs["gate_strict"] if row["metric"] == rp.CI_METRIC)
    assert strict["observed"] == ours["tail_bounds"]["0.9875"][0]
    low, high = ours["tail_bounds"]["0.997"]
    assert low <= ours["tail_bounds"]["0.95"][0] <= high


def test_the_holds_at_each_confidence_read_that_levels_lower_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bounds = {"0.95": [0.01, 0.3], "0.99": [-0.01, 0.4], "0.997": [-0.02, 0.5]}
    monkeypatch.setattr(rp, "bootstrap_bounds", lambda *args, **kwargs: bounds)
    rule = _rule("noise_0")
    loaded = gp.with_episodes(LOADED, gp.US_EPISODES)
    window = rp.check_windows(pd.DatetimeIndex(US.returns.index))[0]

    row = rp.evaluate(US, rule.targets, window, loaded, GATE, rule.rebalance)

    assert row["tail_bounds"] == bounds
    assert [row["holds_at"][key][rp.CI_METRIC] for key in bounds] == [True, False, False]
    others = {r["metric"]: r["holds"] for r in row["gate"] if r["metric"] != rp.CI_METRIC}
    for holds in row["holds_at"].values():
        assert {metric: hold for metric, hold in holds.items() if metric != rp.CI_METRIC} == others


def test_without_episodes_the_leave_episode_rules_hold_and_are_flagged() -> None:
    result: dict[str, Any] = {"leave_episode_min": {}, "comparison": {}}
    rules = GATE.markets["qqq"].rules

    rows = rp.gate_rows(rules, result, no_episode=True)

    flagged = [row for row in rows if row["metric"].startswith(gp.LEAVE_EPISODE)]
    assert len(flagged) == 2
    assert all(row["holds"] and row["no_episode"] for row in flagged)


# --- the two stages -----------------------------------------------------------------------------

METRICS = [rule.metric for rule in GATE.markets["qqq"].rules]


def _row(failing: tuple[str, ...] = (), **extra: Any) -> dict[str, Any]:
    holds = {metric: metric not in failing for metric in METRICS}
    row = {
        "error": None,
        "holds_at": {"0.95": dict(holds), "0.99": dict(holds), "0.997": dict(holds)},
        "comparison": {rp.TAIL_FIELD: 0.1, "max_drawdown": 0.1, "geometric_annual_return": 0.0},
        "tail_bounds": {"0.95": [-0.1, 0.2]},
    }
    return {**row, **extra}


def test_stage_one_reads_the_core_four_or_all_eleven_at_one_confidence() -> None:
    assert rp.stage1(_row(), "core4", "0.99") and rp.stage1(_row(), "all11", "0.99")
    placebo = "placebo_rank.cvar_5pct_relative_improvement"
    assert rp.stage1(_row((placebo,)), "core4", "0.95")
    assert not rp.stage1(_row((placebo,)), "all11", "0.95")
    for metric in rp.CORE_METRICS:
        assert not rp.stage1(_row((metric,)), "core4", "0.95"), metric
    row = _row()
    row["holds_at"]["0.997"][rp.CI_METRIC] = False
    assert rp.stage1(row, "core4", "0.99") and not rp.stage1(row, "core4", "0.997")
    assert not rp.stage1({**_row(), "error": "x"}, "core4", "0.95")
    assert not rp.stage1(_row(), "core4", "0.9")  # a level never computed passes nothing


def test_stage_two_checks_the_direction_only() -> None:
    assert rp.stage2(_row(), "S2") and rp.stage2(_row(), "S3")
    worse_tail = _row(comparison={rp.TAIL_FIELD: -0.01, "max_drawdown": 0.1})
    assert not rp.stage2(worse_tail, "S2") and rp.stage2(worse_tail, "S3")
    worse_drawdown = _row(comparison={rp.TAIL_FIELD: 0.1, "max_drawdown": 0.0})
    assert not rp.stage2(worse_drawdown, "S2")
    surely_worse = _row(tail_bounds={"0.95": [-0.3, -0.01]})
    assert rp.stage2(surely_worse, "S2") and not rp.stage2(surely_worse, "S3")
    # S3 reads the 95% interval whatever other levels the row has.
    assert rp.stage2(_row(tail_bounds={"0.95": [-0.3, 0.1], "0.997": [-0.5, -0.01]}), "S3")
    assert not rp.stage2(_row(tail_bounds={"0.95": [-0.3, -0.01], "0.997": [-0.5, 0.2]}), "S3")
    low_return = _row((rp.GEO_METRIC,))
    assert not rp.stage2(low_return, "S2") and not rp.stage2(low_return, "S3")
    assert not rp.stage2(_row(comparison={rp.TAIL_FIELD: None, "max_drawdown": 0.1}), "S2")
    assert not rp.stage2({**_row(), "error": "x"}, "S3")
    with pytest.raises(ValueError, match="unknown check"):
        rp.stage2(_row(), "S9")


def test_the_candidates_come_in_the_tie_order() -> None:
    names = [item["candidate"] for item in rp.candidates()]

    assert names[:4] == ["core4@0.997+S2", "core4@0.997+S3", "core4@0.99+S2", "core4@0.99+S3"]
    assert names[-1] == "all11@0.95+S3" and len(names) == 12


# --- the reading --------------------------------------------------------------------------------

GAINS = {"null": -0.2, "noise_0": 0.4, "noise_1": 0.22, "noise_2": 0.08, "noise_3": 0.01}
GAINS |= {f"dd_{key}": value for key, value in GAINS.items()}
LEVELS = (0.0, 1.0, 2.0, 3.0)


def _reading_result(errors: tuple[tuple[str, str], ...] = ()) -> dict[str, Any]:
    pairs = {"30": [["us30_1930", "us_1960"]], "40": [["us40_1930", "us_1970"]]}
    rows = [
        {"rule_id": rule_id, "kind": "planted", "window_id": window_id, "error": None}
        for rule_id in GAINS
        for window_id in ("us30_1930", "us_1960", "us40_1930", "us_1970")
    ]
    for rule_id, window_id in errors:
        next(r for r in rows if (r["rule_id"], r["window_id"]) == (rule_id, window_id))["error"] = (
            "x"
        )
    return {
        "effects": {rule_id: {rp.TAIL_FIELD: gain} for rule_id, gain in GAINS.items()},
        "noise_levels": list(LEVELS),
        "confidences": [0.95, 0.99, 0.997],
        "windows": {"pairs": pairs},
        "evaluations": rows,
    }


def _shares(
    by_candidate: dict[str, dict[str, tuple[float, float]]], default: dict[str, Any]
) -> Callable[[Any, dict[str, Any]], dict[str, Any]]:
    """A fake ``pass_shares``: pair shares (30, 40) per rule, per candidate or by default."""

    def fake(result: Any, candidate: dict[str, Any]) -> dict[str, Any]:
        table = {**default, **by_candidate.get(candidate["candidate"], {})}
        return {
            rule_id: {"pairs": {"30": a, "40": b}, "stage1": {}, "full": False, "stage2": None}
            for rule_id, (a, b) in table.items()
        }

    return fake


GOOD = {rule_id: (0.0, 0.0) for rule_id in GAINS} | {
    "noise_1": (0.9, 0.85),
    "dd_noise_1": (0.95, 0.9),
    "noise_0": (1.0, 1.0),
    "dd_noise_0": (1.0, 1.0),
}


def test_the_levels_and_the_zero_effect_rules_follow_the_written_lines() -> None:
    effects = {rule_id: {rp.TAIL_FIELD: gain} for rule_id, gain in GAINS.items()}

    assert rp.level(effects, "", LEVELS) == "noise_1"
    assert rp.level(effects, "dd_", LEVELS) == "dd_noise_1"
    assert rp.zero_rules(effects, "", LEVELS) == ["null", "noise_3"]
    tied = effects | {"noise_2": {rp.TAIL_FIELD: 0.22}}
    assert rp.level(tied, "", LEVELS) == "noise_2"  # a tie goes to the noisier level
    assert rp.level({}, "", LEVELS) is None


def test_the_highest_scoring_eligible_candidate_is_adopted_when_the_holdout_holds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    better = {"noise_1": (0.95, 0.9)}
    monkeypatch.setattr(rp, "pass_shares", _shares({"all11@0.95+S3": better}, GOOD))

    read = rp.choose(_reading_result())

    assert (read["chosen"], read["score"], read["verdict"]) == ("all11@0.95+S3", 0.9, "adopt")
    assert read["holdout"] == {"score": 0.9, "false_pass": 0.0, "holds": True}
    assert (read["level"], read["zero_rules"]) == ("noise_1", ["null", "noise_3"])


def test_a_tie_goes_to_the_first_candidate_in_the_tie_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rp, "pass_shares", _shares({}, GOOD))

    read = rp.choose(_reading_result())

    assert read["chosen"] == "core4@0.997+S2" and read["score"] == 0.85
    assert all(item["eligible"] for item in read["candidates"])


def test_a_candidate_letting_a_zero_effect_rule_pass_is_not_eligible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loose = {"noise_1": (1.0, 1.0), "noise_3": (0.0, 0.0625)}
    monkeypatch.setattr(rp, "pass_shares", _shares({"core4@0.997+S2": loose}, GOOD))

    read = rp.choose(_reading_result())

    first = read["candidates"][0]
    assert first["candidate"] == "core4@0.997+S2" and not first["eligible"]
    assert first["false_pass"] == 0.0625
    assert read["chosen"] == "core4@0.997+S3"


def test_the_holdout_family_can_fail_the_chosen_ruler(monkeypatch: pytest.MonkeyPatch) -> None:
    for change in ({"dd_noise_1": (0.7, 0.9)}, {"dd_null": (0.1, 0.0)}):
        monkeypatch.setattr(rp, "pass_shares", _shares({}, GOOD | change))

        read = rp.choose(_reading_result())

        assert read["verdict"] == "holdout_failed" and read["holdout"]["holds"] is False


def test_the_other_verdicts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rp, "pass_shares", _shares({}, GOOD | {"noise_1": (0.7, 0.9)}))
    assert rp.choose(_reading_result())["verdict"] == "below_80"

    monkeypatch.setattr(rp, "pass_shares", _shares({}, GOOD | {"null": (0.1, 0.0)}))
    assert rp.choose(_reading_result())["verdict"] == "none_eligible"

    monkeypatch.setattr(rp, "pass_shares", _shares({}, GOOD))
    weak = _reading_result()
    for key in ("dd_noise_0", "dd_noise_1"):
        weak["effects"][key] = {rp.TAIL_FIELD: 0.1}
    assert rp.choose(weak)["verdict"] == "holdout_no_level"

    flat = _reading_result()
    for key in ("noise_0", "noise_1"):
        flat["effects"][key] = {rp.TAIL_FIELD: 0.1}
    assert rp.choose(flat)["verdict"] == "no_level"

    broken = rp.choose(_reading_result((("dd_noise_3", "us_1970"),)))
    assert (broken["verdict"], broken["errors"]) == ("incomplete", ["dd_noise_3"])

    blank = _reading_result()
    blank["effects"]["noise_2"] = {rp.TAIL_FIELD: None}
    read = rp.choose(blank)
    assert (read["verdict"], read["errors"], read["missing_effects"]) == (
        "incomplete",
        [],
        ["noise_2"],
    )


def test_errors_outside_the_planted_pair_windows_leave_the_reading_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(rp, "pass_shares", _shares({}, GOOD))
    result = _reading_result()
    result["evaluations"] += [
        {"rule_id": "noise_3", "kind": "planted", "window_id": rp.FULL_ID, "error": "x"},
        {"rule_id": "noise_3", "kind": "planted", "window_id": "us_1942", "error": "x"},
        {"rule_id": "q1a", "kind": "real", "window_id": "us30_1930", "error": "x"},
    ]

    assert rp.choose(result)["verdict"] == "adopt"


def test_the_lines_hold_at_their_exact_values(monkeypatch: pytest.MonkeyPatch) -> None:
    at_lines = {
        "noise_1": (0.8, 0.9),
        "noise_3": (0.05, 0.0),
        "dd_noise_1": (0.8, 0.8),
        "dd_null": (0.0, 0.05),
    }
    monkeypatch.setattr(rp, "pass_shares", _shares({}, GOOD | at_lines))

    read = rp.choose(_reading_result())

    assert all(item["eligible"] for item in read["candidates"])
    assert (read["chosen"], read["score"], read["verdict"]) == ("core4@0.997+S2", 0.8, "adopt")
    assert read["holdout"] == {"score": 0.8, "false_pass": 0.05, "holds": True}
    effects = {rule_id: {rp.TAIL_FIELD: gain} for rule_id, gain in GAINS.items()}
    effects |= {"noise_1": {rp.TAIL_FIELD: 0.2}, "noise_3": {rp.TAIL_FIELD: 0.05}}
    assert rp.level(effects, "", LEVELS) == "noise_1"
    assert rp.zero_rules(effects, "", LEVELS) == ["null"]


def test_the_lines_in_pairs() -> None:
    assert [rp.allowed_counts(n) for n in (16, 21, 20, 40, 3)] == [
        (0, 13),
        (1, 17),
        (1, 16),
        (2, 32),
        (0, 3),
    ]


# A hand-built run: two 30+10 pairs and one 40+10 pair, read with the real pass shares.
BUILT_IDS = [rp.FULL_ID, "us30_1930", "us30_1932", "us40_1930", "us_1960", "us_1962", "us_1970"]
BUILT_WINDOWS = {
    "check": [{"window_id": w} for w in ("us_1960", "us_1962", "us_1970")],
    "long": {
        "30": [{"window_id": "us30_1930"}, {"window_id": "us30_1932"}],
        "40": [{"window_id": "us40_1930"}],
    },
    "pairs": {
        "30": [["us30_1930", "us_1960"], ["us30_1932", "us_1962"]],
        "40": [["us40_1930", "us_1970"]],
    },
}
WORSE_TAIL = {rp.TAIL_FIELD: -0.01, "max_drawdown": 0.1}
FAILS = _row(
    (rp.CI_METRIC,),
    comparison={rp.TAIL_FIELD: -0.1, "max_drawdown": -0.1},
    tail_bounds={"0.95": [-0.3, -0.1]},
)


def _built(rows: dict[str, dict[str, dict[str, Any]]], default: dict[str, Any]) -> dict[str, Any]:
    evaluations = [
        {
            **rows.get(rule_id, {}).get(window_id, default),
            "rule_id": rule_id,
            "kind": "planted",
            "window_id": window_id,
        }
        for rule_id in GAINS
        for window_id in BUILT_IDS
    ]
    return {
        "effects": {rule_id: {rp.TAIL_FIELD: gain} for rule_id, gain in GAINS.items()},
        "noise_levels": list(LEVELS),
        "confidences": [0.95, 0.99, 0.997],
        "windows": BUILT_WINDOWS,
        "evaluations": evaluations,
    }


def _candidate(name: str) -> dict[str, Any]:
    return next(item for item in rp.candidates() if item["candidate"] == name)


def test_the_pass_shares_join_stage_one_on_the_long_window_to_stage_two_on_the_check() -> None:
    no_ci_997 = _row()
    no_ci_997["holds_at"]["0.997"][rp.CI_METRIC] = False
    rows = {
        "noise_1": {
            "us30_1932": no_ci_997,
            "us40_1930": _row(("placebo_rank.cvar_5pct_relative_improvement",)),
            "us_1962": _row(comparison=WORSE_TAIL),
            "us_1970": _row(comparison={rp.TAIL_FIELD: 0.1, "max_drawdown": 0.0}),
        }
    }
    result = _built(rows, _row())

    strict = rp.pass_shares(result, _candidate("core4@0.997+S2"))["noise_1"]
    loose = rp.pass_shares(result, _candidate("core4@0.99+S3"))["noise_1"]
    every = rp.pass_shares(result, _candidate("all11@0.95+S3"))["noise_1"]

    assert strict == {
        "pairs": {"30": 0.5, "40": 0.0},
        "stage1": {"30": 0.5, "40": 1.0},
        "full": True,
        "stage2": pytest.approx(1 / 3),
    }
    assert loose == {
        "pairs": {"30": 1.0, "40": 1.0},
        "stage1": {"30": 1.0, "40": 1.0},
        "full": True,
        "stage2": 1.0,
    }
    assert every["pairs"] == {"30": 1.0, "40": 0.0} and every["stage1"]["40"] == 0.0
    assert rp.pass_shares(result, _candidate("core4@0.997+S2"))["null"]["pairs"] == {
        "30": 1.0,
        "40": 1.0,
    }


def test_the_reading_with_the_real_pass_shares_takes_the_holdout_from_the_chosen_ruler() -> None:
    # Both Ls improve the left tail over their long windows but not its point estimate in the
    # check windows: only S3 rulers pass them, and the zero-effect rules pass nothing.
    late = {
        window_id: _row(comparison=WORSE_TAIL) for window_id in ("us_1960", "us_1962", "us_1970")
    }
    zero = dict.fromkeys(BUILT_IDS, FAILS)
    rows = {"noise_1": late, "dd_noise_1": late} | {
        rule_id: zero for rule_id in ("null", "noise_3", "dd_null", "dd_noise_3")
    }
    result = _built(rows, _row())

    read = rp.choose(result)

    scores = {item["candidate"]: item["score"] for item in read["candidates"]}
    assert {name for name, score in scores.items() if score == 1.0} == {
        name for name in scores if name.endswith("+S3")
    }
    assert {score for name, score in scores.items() if name.endswith("+S2")} == {0.0}
    assert all(item["eligible"] and item["false_pass"] == 0.0 for item in read["candidates"])
    assert (read["chosen"], read["verdict"]) == ("core4@0.997+S3", "adopt")
    assert read["holdout"] == {"score": 1.0, "false_pass": 0.0, "holds": True}

    result["reading"] = read
    detail = rp.pair_detail(result, _candidate("core4@0.997+S3"))
    assert [(item["span"], item["long"], item["check"]) for item in detail] == [
        ("30", "us30_1930", "us_1960"),
        ("30", "us30_1932", "us_1962"),
        ("40", "us40_1930", "us_1970"),
    ]
    assert detail[0]["volatility"] == {
        "stage1": True,
        "stage2": True,
        "zero_both": 0,
        "zero_count": 2,
    }
    assert detail[0]["drawdown"]["stage2"] is True
    s2 = rp.pair_detail(result, _candidate("core4@0.997+S2"))
    assert [item["volatility"]["stage2"] for item in s2] == [False, False, False]
    passing = _built({}, _row()) | {"reading": read}
    detail = rp.pair_detail(passing, _candidate("core4@0.997+S2"))
    assert [item["volatility"]["zero_both"] for item in detail] == [2, 2, 2]
    # A zero-effect rule passing stage 1 only is not counted.
    long_only = dict.fromkeys(BUILT_IDS, FAILS) | dict.fromkeys(BUILT_IDS[:4], _row())
    half = _built(rows | {"null": long_only}, _row()) | {"reading": read}
    detail = rp.pair_detail(half, _candidate("core4@0.997+S3"))
    assert [item["volatility"]["zero_both"] for item in detail] == [0, 0, 0]
    assert rp.pass_shares(half, _candidate("core4@0.997+S3"))["null"]["stage1"] == {
        "30": 1.0,
        "40": 1.0,
    }


# --- the whole run ------------------------------------------------------------------------------


def test_every_rule_is_evaluated_in_every_window(ruler: dict[str, Any]) -> None:
    windows = ruler["windows"]
    ids = [rp.FULL_ID] + [w["window_id"] for w in windows["long"]["4"]]
    ids += [w["window_id"] for w in windows["check"]]

    seen = [(row["rule_id"], row["window_id"]) for row in ruler["evaluations"]]

    assert seen == [(rule_id, window_id) for window_id in ids for rule_id in RULE_IDS]
    assert ids == [rp.FULL_ID, "us4_1930", "us4_1934", "us4_1938", "us4_1942"] + [
        "us_1930",
        "us_1934",
        "us_1938",
        "us_1942",
    ]
    assert all(row["error"] is None for row in ruler["evaluations"])
    spans = {row["window_id"]: row["span"] for row in ruler["evaluations"]}
    assert (spans[rp.FULL_ID], spans["us4_1930"], spans["us_1930"]) == (None, 4, 3)
    assert windows["pairs"] == {
        "4": [["us4_1930", "us_1934"], ["us4_1934", "us_1938"]] + [["us4_1938", "us_1942"]]
    }
    assert windows["full"] == {"window_id": rp.FULL_ID, "start": "1930-01-01", "end": US_END}


def test_the_holds_at_each_level_change_only_the_interval_rule(ruler: dict[str, Any]) -> None:
    rows = [row for row in ruler["evaluations"] if row["error"] is None]

    assert any(row["no_episode"] for row in rows)
    for row in rows:
        gate = {item["metric"]: item["holds"] for item in row["gate"]}
        for key, holds in row["holds_at"].items():
            low = row["tail_bounds"][key][0]
            assert holds[rp.CI_METRIC] == (low is not None and low > 0)
            assert {m: h for m, h in holds.items() if m != rp.CI_METRIC} == {
                m: h for m, h in gate.items() if m != rp.CI_METRIC
            }
        assert row["holds_at"]["0.95"] == gate


def test_the_journal_keeps_the_record_and_each_evaluation_as_it_arrives(
    ruler_run: tuple[dict[str, Any], Path],
) -> None:
    result, journal = ruler_run

    lines = [json.loads(line) for line in journal.read_text(encoding="utf-8").splitlines()]

    head = lines[0]
    assert head["tasks"] == len(result["evaluations"]) == len(lines) - 1
    assert head["provenance"] == result["provenance"] and head["run"] == rp.RUN_ID
    assert "evaluations" not in head and "reading" not in head
    assert lines[1:] == [gp._clean(row) for row in result["evaluations"]]


def test_the_result_records_what_produced_it(ruler: dict[str, Any]) -> None:
    trials = ROOT_DIR.joinpath(tr.TRIALS_DIR)

    assert ruler["run"] == "TRADING-2565-18" and ruler["cutoff"] == "2010-12-31"
    assert ruler["provenance"]["commit"]
    assert ruler["gate_sha256"] == lg.chain_digest(trials.joinpath(tr.GATE_FILE).read_bytes())
    assert ruler["plan_sha256"] == lg.chain_digest(trials.joinpath("T0001.yaml").read_bytes())
    assert ruler["settings_sha256"] == LOADED.sha256
    assert ruler["confidences"] == [0.95, 0.99, 0.997] and ruler["check_years"] == 3
    assert ruler["seeds"] == {"volatility": gp.PLANTED_SEED, "drawdown": rp.HOLDOUT_SEED}
    assert ruler["reading"]["verdict"] in rp.VERDICT_TEXT
    full = Window(
        window_id=rp.FULL_ID, role="development", start=date(1930, 1, 1), end=date(1945, 12, 31)
    )
    rule = _rule("dd_noise_0")
    assert ruler["effects"]["dd_noise_0"] == gp.effect(
        US, rule.targets, full, LOADED, rule.rebalance
    )


def test_the_planted_effects_shrink_with_noise(ruler: dict[str, Any]) -> None:
    effects = ruler["effects"]

    for prefix in ("", "dd_"):
        assert effects[f"{prefix}noise_0"][rp.TAIL_FIELD] > 0.2
        assert (
            effects[f"{prefix}noise_0"][rp.TAIL_FIELD] > effects[f"{prefix}noise_3"][rp.TAIL_FIELD]
        )


def test_spawned_workers_give_the_same_rows(ruler: dict[str, Any]) -> None:
    spawned = rp.run_ruler(US, LOADED, GATE, ROOT_DIR, workers=2, **{**RUN, "noise_levels": ()})

    ours = {(row["rule_id"], row["window_id"]): row for row in ruler["evaluations"]}
    assert len(spawned["evaluations"]) == 5 * 9
    for row in spawned["evaluations"]:
        assert gp._clean(row) == gp._clean(ours[(row["rule_id"], row["window_id"])])


def test_a_window_that_raises_is_kept_as_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    provenance = rp.git_provenance

    def record(root: Path) -> Any:
        calls.append("provenance")
        return provenance(root)

    def broken(data: Any, targets: Any, window: Window, *args: Any) -> dict[str, Any]:
        calls.append("evaluate")
        raise RuntimeError("boom\nsecond line")

    monkeypatch.setattr(rp, "git_provenance", record)
    monkeypatch.setattr(rp, "evaluate", broken)

    result = rp.run_ruler(US, LOADED, GATE, ROOT_DIR, **{**RUN, "noise_levels": (0.0,)})

    rows = result["evaluations"]
    assert rows and all(row["error"] == "RuntimeError: boom" for row in rows)
    assert not any(rp.stage1(row, "core4", "0.95") or rp.stage2(row, "S3") for row in rows)
    assert result["reading"]["verdict"] == "incomplete"
    assert calls[:2] == ["provenance", "evaluate"] and calls.count("provenance") == 1
    text = rp.render_markdown(result)
    assert "| 出错 |" in text and "L 两段都过最高的候选（没有选中）" in text


def test_the_probe_refuses_late_data_and_another_gate() -> None:
    late = _us_data(end="2011-01-31")
    with pytest.raises(ValueError, match="past 2010-12-31"):
        rp.run_ruler(late, LOADED, GATE, ROOT_DIR, **RUN)
    other = GATE.model_copy(update={"confidence": 0.9})
    with pytest.raises(ValueError, match="confidence 0.9 is not 0.95"):
        rp.run_ruler(US, LOADED, other, ROOT_DIR, **RUN)
    with pytest.raises(ValueError, match="must include the gate's 0.95"):
        rp.run_ruler(US, LOADED, GATE, ROOT_DIR, **{**RUN, "confidences": (0.99,)})
    with pytest.raises(ValueError, match="pair length"):
        rp.run_ruler(US, LOADED, GATE, ROOT_DIR, **{**RUN, "pair_spans": (5,)})
    qqq = GATE.markets["qqq"]
    rules = [rule for rule in qqq.rules if rule.metric != rp.COST_METRIC]
    markets = {**GATE.markets, "qqq": qqq.model_copy(update={"rules": rules})}
    with pytest.raises(ValueError, match="no rule on cost_25"):
        rp.run_ruler(US, LOADED, GATE.model_copy(update={"markets": markets}), ROOT_DIR, **RUN)
    twice = [*qqq.rules, qqq.rules[0]]
    markets = {**GATE.markets, "qqq": qqq.model_copy(update={"rules": twice})}
    with pytest.raises(ValueError, match="two rules on one metric"):
        rp.run_ruler(US, LOADED, GATE.model_copy(update={"markets": markets}), ROOT_DIR, **RUN)


def test_a_pair_length_without_pairs_is_refused() -> None:
    with pytest.raises(ValueError, match="no pair"):
        rp.run_ruler(US, LOADED, GATE, ROOT_DIR, **{**RUN, "first_year": 1942})


# --- output and command -------------------------------------------------------------------------


def test_the_result_is_written_as_strict_json_and_a_chinese_summary(
    ruler: dict[str, Any], tmp_path: Path
) -> None:
    rp.write_result(ruler, tmp_path)

    data = json.loads(tmp_path.joinpath("ruler_result.json").read_text(encoding="utf-8"))
    text = tmp_path.joinpath("ruler_result.md").read_text(encoding="utf-8")
    assert data["run"] == "TRADING-2565-18" and data["reading"] == gp._clean(ruler["reading"])
    assert text.startswith("# 两段尺子结果（TRADING-2565-18）\n")
    assert rp.VERDICT_TEXT[ruler["reading"]["verdict"]] in text
    assert "3 年查方向窗口 4 个；长窗口 4 年 4 个" in text and "配对 4+3 年 3 对" in text
    for item in rp.candidates():
        assert f"| {item['candidate']} |" in text
    for rule_id in RULE_IDS:
        assert f"| {rule_id} |" in text
    assert "| 规则 | 整段左尾改善 | 4+3 两段都过 | 4 年第一段 | 整段第一段 | 3 年查方向 |" in text
    assert "线换成对数：4+3 年 3 对里几乎没效果的最多过 0 对、L 至少过 3 对。" in text
    assert "每对（按第一段起点）" in text
    read = ruler["reading"]
    assert ("（没有采用）" in text) == (read["chosen"] is not None and read["verdict"] != "adopt")
    for long_id, check_id in ruler["windows"]["pairs"]["4"]:
        assert f"| 4+3 | {long_id} | {check_id} | " in text


def test_the_command_writes_the_result(
    ruler: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []
    out = tmp_path / "ruler"

    def fake_run(*args: Any, workers: int, journal: Path, **kwargs: Any) -> dict[str, Any]:
        calls.append(workers)
        assert journal == out / rp.JOURNAL_FILE and out.is_dir()
        return dict(ruler)

    monkeypatch.setattr(rp, "load_us_market", lambda root: (US, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(rp, "run_ruler", fake_run)
    result = CliRunner().invoke(rp.app, ["run", "--out", str(out), "--workers", "3"])

    assert result.exit_code == 0, result.output
    assert calls == [3] and "判读" in result.output
    data = json.loads(out.joinpath("ruler_result.json").read_text(encoding="utf-8"))
    assert data["source"] == US.source


def test_the_command_stops_when_the_data_do_not_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(root: Path) -> Any:
        raise ValueError("snapshot not checked")

    monkeypatch.setattr(rp, "load_us_market", refuse)

    result = CliRunner().invoke(rp.app, ["run", "--out", str(tmp_path / "x")])

    assert result.exit_code == 1
    assert "数据载入失败：snapshot not checked" in result.output
    assert not tmp_path.joinpath("x").exists()


def test_the_loaded_us_market_is_cut_at_2010(monkeypatch: pytest.MonkeyPatch) -> None:
    late = _us_data(end="2011-06-30")
    monkeypatch.setattr(tr, "market_data", lambda market, *args, **kwargs: late)

    data, gate, loaded, root_dir = rp.load_us_market(Path("unused"))

    assert root_dir == ROOT_DIR and gate.confidence == 0.95
    assert pd.Timestamp(data.returns.index.max()) == pd.Timestamp("2010-12-31")
    assert pd.Timestamp(data.inputs.index.max()) == pd.Timestamp("2010-12-31")
