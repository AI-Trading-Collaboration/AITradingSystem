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
import yaml
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import RunSpec, load_settings, run_strategy
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.checks import strategy_targets
from ai_trading_system.research_bench.data import (
    BenchData,
    from_daily_frame,
    from_us_market,
    with_signal_sources,
)
from ai_trading_system.research_bench.prereg import evaluate_rules
from ai_trading_system.research_bench.settings import EpisodeSpec, Window, resolve_window
from ai_trading_system.research_bench.simulate import Rebalance
from ai_trading_system.research_probes import gate_power as gp

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
GATE = gp.load_measured_gate()
ROOT_DIR = lg.research_root(LOADED)
US_END = "1943-06-30"  # two US windows: 1930 (to 1940-02-19) and 1932 (to 1942-02-19)
LEVELS = (0.0, 3.0)  # two noise levels keep the whole-probe tests fast


def _qqq_data(seed: int = 3) -> BenchData:
    index = pd.bdate_range("1994-01-03", "2010-12-31", name="date")
    r = np.random.default_rng(seed).normal(0.0004, 0.015, len(index))
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


def _us_data(end: str = US_END, seed: int = 5) -> BenchData:
    """Daily market returns whose volatility changes month by month, so that knowing next month's
    volatility is worth something."""

    index = pd.bdate_range("1926-07-01", end, name="date")
    rng = np.random.default_rng(seed)
    months = index.to_period("M")
    codes, uniques = pd.factorize(months)
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


QQQ = _qqq_data()
US = _us_data()


@pytest.fixture(scope="module")
def power() -> dict[str, Any]:
    return gp.run_power({"us_market": US, "qqq": QQQ}, LOADED, GATE, ROOT_DIR, noise_levels=LEVELS)


# --- data cut ---------------------------------------------------------------------------------


def test_cut_drops_every_date_after_the_cutoff() -> None:
    data = _us_data(end="2011-03-31")
    cut = gp.cut(data)

    assert pd.Timestamp(cut.returns.index.max()) == pd.Timestamp("2010-12-31")
    assert pd.Timestamp(cut.inputs.index.max()) == pd.Timestamp("2010-12-31")
    assert cut.source == data.source
    pd.testing.assert_frame_equal(cut.returns, data.returns.loc[:"2010-12-31"])
    with pytest.raises(ValueError, match="no data on or before"):
        gp.cut(data, date(1900, 1, 1))


def test_the_probe_refuses_data_past_the_cutoff_and_missing_markets() -> None:
    late = _us_data(end="2011-01-03")
    with pytest.raises(ValueError, match="run past 2010-12-31"):
        gp.run_power({"us_market": late, "qqq": QQQ}, LOADED, GATE, ROOT_DIR)
    with pytest.raises(ValueError, match="missing qqq"):
        gp.run_power({"us_market": US}, LOADED, GATE, ROOT_DIR)


# --- the planted rules ------------------------------------------------------------------------


def test_next_month_log_vol_reads_the_returns_after_each_month_end() -> None:
    # Month ends 2003-01-31, 2003-02-28 and the last date 2003-03-14.
    index = pd.bdate_range("2003-01-01", "2003-03-14", name="date")
    returns = pd.Series(np.arange(len(index), dtype=float) / 1000.0, index=index)
    log_vol = gp.next_month_log_vol(returns)

    ends = [pd.Timestamp("2003-01-31"), pd.Timestamp("2003-02-28"), pd.Timestamp("2003-03-14")]
    assert list(log_vol.index) == ends
    february = returns.loc["2003-02-03":"2003-02-28"].to_numpy()
    march = returns.loc["2003-03-03":"2003-03-14"].to_numpy()
    assert log_vol.iloc[0] == pytest.approx(math.log(np.std(february, ddof=1) * math.sqrt(252)))
    assert log_vol.iloc[1] == pytest.approx(math.log(np.std(march, ddof=1) * math.sqrt(252)))
    assert math.isnan(log_vol.iloc[2])


def test_next_month_log_vol_is_nan_for_flat_or_short_months() -> None:
    index = pd.bdate_range("2003-01-01", "2003-03-31", name="date")
    returns = pd.Series(0.0, index=index)
    returns.loc["2003-03-03":"2003-03-30"] = np.nan

    log_vol = gp.next_month_log_vol(returns)

    assert math.isnan(log_vol.loc["2003-01-31"])  # February is flat: no volatility
    assert math.isnan(log_vol.loc["2003-02-28"])  # March has one finite return


def test_planted_forecast_adds_scaled_noise_and_the_null_is_noise_alone() -> None:
    index = pd.date_range("2000-01-31", periods=4, freq="ME")
    log_vol = pd.Series([1.0, 2.0, np.nan, 4.0], index=index)
    draws = np.array([0.5, -1.0, 2.0, 0.0])
    spread = float(np.std([1.0, 2.0, 4.0], ddof=1))

    exact = gp.planted_forecast(log_vol, 0.0, draws)
    noisy = gp.planted_forecast(log_vol, 2.0, draws)
    null = gp.planted_forecast(log_vol, None, draws)

    np.testing.assert_allclose(exact.to_numpy(), [1.0, 2.0, np.nan, 4.0])
    np.testing.assert_allclose(noisy.to_numpy(), [1.0 + spread, 2.0 - 2.0 * spread, np.nan, 4.0])
    np.testing.assert_allclose(null.to_numpy(), [0.5, -1.0, np.nan, 0.0])
    with pytest.raises(ValueError, match="one draw per month end"):
        gp.planted_forecast(log_vol, 1.0, draws[:3])
    with pytest.raises(ValueError, match="must not be negative"):
        gp.planted_forecast(log_vol, -0.5, draws)


def test_expanding_percentile_counts_earlier_values_ties_half_after_the_warmup() -> None:
    values = pd.Series([1.0, 2.0, 2.0, np.nan, 3.0, 0.0])

    out = gp.expanding_percentile(values, warmup=2).to_numpy()

    assert np.isnan(out[:2]).all()
    assert out[2] == pytest.approx(0.75)  # earlier 1, 2: one below, one tied
    assert np.isnan(out[3])
    assert out[4] == pytest.approx(1.0)  # earlier 1, 2, 2: all below; the NaN is skipped
    assert out[5] == pytest.approx(0.0)


def test_planted_targets_decide_at_month_ends_and_hold_until_the_next() -> None:
    inputs = US.inputs
    targets = gp.planted_targets(inputs, 0.5, warmup=3)
    ends = gp.month_ends(pd.DatetimeIndex(inputs.index))

    assert targets.index.equals(inputs.index)
    finite = targets.dropna()
    assert finite.between(0.0, 1.0).all()
    assert finite.index[0] == ends[3]  # the fourth month end has three earlier forecasts
    changes = finite.index[finite.diff().fillna(0.0) != 0.0]
    assert set(changes) <= set(ends)


def test_without_noise_the_target_is_one_minus_the_percentile_of_next_months_volatility() -> None:
    inputs = US.inputs
    log_vol = gp.next_month_log_vol(inputs["ndx_return"])
    expected = (1.0 - gp.expanding_percentile(log_vol, gp.WARMUP_MONTHS)).reindex(inputs.index)

    exact = gp.planted_targets(inputs, 0.0)
    other_seed = gp.planted_targets(inputs, 0.0, seed=1)

    pd.testing.assert_series_equal(exact, expected.ffill().rename("target"))
    pd.testing.assert_series_equal(exact, other_seed)
    assert not gp.planted_targets(inputs, 1.0).equals(gp.planted_targets(inputs, 1.0, seed=1))


def test_the_planted_rule_knows_next_months_volatility(power: dict[str, Any]) -> None:
    effects = power["effects"]["us_market"]

    assert effects["noise_0"][gp.TAIL_FIELD] > gp.EFFECT_LINE
    assert effects["noise_0"][gp.TAIL_FIELD] > effects["noise_3"][gp.TAIL_FIELD]
    assert effects["noise_0"][gp.TAIL_FIELD] > effects["null"][gp.TAIL_FIELD]


# --- windows and episodes ---------------------------------------------------------------------


def test_us_windows_span_the_validation_windows_calendar_from_each_even_year() -> None:
    index = pd.bdate_range("1926-07-01", "2010-12-31")
    windows = gp.us_windows(index)

    assert len(windows) == 36
    assert (windows[0].window_id, windows[0].start, windows[0].end) == (
        "us_1930",
        date(1930, 1, 1),
        date(1940, 2, 19),
    )
    assert (windows[-1].window_id, windows[-1].end) == ("us_2000", date(2010, 2, 19))
    assert {window.role for window in windows} == {"development"}
    short = gp.us_windows(pd.bdate_range("1926-07-01", "2005-06-30"))
    assert short[-1].window_id == "us_1994"


def test_non_overlapping_takes_windows_greedily() -> None:
    windows = gp.us_windows(pd.bdate_range("1926-07-01", "2010-12-31"))

    assert gp.non_overlapping(windows) == [
        "us_1930",
        "us_1942",
        "us_1954",
        "us_1966",
        "us_1978",
        "us_1990",
    ]


def test_us_episodes_are_ordered_inside_1929_2010_and_cover_the_gates_crashes() -> None:
    ids = [episode.id for episode in gp.US_EPISODES]
    gate_ids = {episode.id for episode in GATE.markets["us_market"].event_episodes or ()}

    assert len(ids) == len(set(ids)) == 17
    assert gate_ids <= set(ids)
    for episode in gp.US_EPISODES:
        assert "1929-01" <= episode.start <= episode.end <= "2010-12"
    assert [episode.start for episode in gp.US_EPISODES] == sorted(
        episode.start for episode in gp.US_EPISODES
    )


def test_with_episodes_changes_only_the_episodes() -> None:
    episodes = (EpisodeSpec(id="one", start="1930-01", end="1930-06"),)
    changed = gp.with_episodes(LOADED, episodes)

    assert changed.settings.episodes == episodes
    assert LOADED.settings.episodes != episodes
    assert changed.settings.model_copy(update={"episodes": LOADED.settings.episodes}) == (
        LOADED.settings
    )
    assert (changed.root, changed.sha256) == (LOADED.root, LOADED.sha256)


# --- one evaluation ---------------------------------------------------------------------------


def _q1a() -> tuple[Any, dict[str, Any], Rebalance]:
    question = lg.load_question(ROOT_DIR.joinpath("QQQ-Q1"))
    entry = question.prereg.strategy("q1a")
    return lg.load_strategy(question, "q1a"), dict(entry.params), entry.rebalance.to_rebalance()


def test_an_evaluation_is_the_bench_run_of_the_development_window_with_the_gate() -> None:
    function, params, rebalance = _q1a()
    window = resolve_window(LOADED, "qqq_direction", "development")
    targets = strategy_targets(function, QQQ.inputs, params)
    spec = RunSpec(
        question_type="qqq_direction",
        strategy_id="q1a",
        params=params,
        window_role="development",
        rebalance=rebalance,
    )
    direct = run_strategy(QQQ, function, spec, LOADED, falsification_confidence=0.95)
    outcome = direct.to_dict()
    result = {**(outcome.get("falsification") or {}), **outcome}  # as trial.py merges them

    row = gp.evaluate(QQQ, targets, window, LOADED, GATE, rebalance)

    assert (row["window_id"], row["start"], row["end"]) == (
        "qqq_long_history_development",
        "1999-03-10",
        "2010-12-31",
    )
    assert row["no_episode"] is False and row["error"] is None
    assert row["comparison"][gp.TAIL_FIELD] == pytest.approx(direct.comparison[gp.TAIL_FIELD])
    assert row["gate"] == evaluate_rules(GATE.markets["qqq"].rules, result)
    assert row["us_gate"] == evaluate_rules(GATE.markets["us_market"].rules, result)
    assert row["holds"] is all(item["holds"] for item in row["gate"])
    assert row["us_holds"] is all(item["holds"] for item in row["us_gate"])
    assert row["return_days"] == len(QQQ.returns.loc["1999-03-10":"2010-12-31"]) - 1
    assert row["saturdays"] == 0
    assert row["episodes"] == list(outcome["falsification"]["leave_episode"])


def test_a_us_evaluation_is_the_bench_run_with_the_us_episodes() -> None:
    # trend4, not q1a: q1a's target volatility is the median before 1999, which on the US market
    # lies after these windows (the bench's look-ahead check refuses it; the probe only shows q1a).
    question = lg.load_question(ROOT_DIR.joinpath("QQQ-Q2"))
    entry = question.prereg.strategy("trend4")
    function, params = lg.load_strategy(question, "trend4"), dict(entry.params)
    rebalance = entry.rebalance.to_rebalance()
    loaded = gp.with_episodes(LOADED, gp.US_EPISODES)
    window = resolve_window(loaded, "us_market_trial", "development")
    spec = RunSpec(
        question_type="us_market_trial",
        strategy_id="trend4",
        params=params,
        window_role="development",
        rebalance=rebalance,
    )
    direct = run_strategy(US, function, spec, loaded, falsification_confidence=0.95)
    outcome = direct.to_dict()
    result = {**(outcome.get("falsification") or {}), **outcome}

    row = gp.evaluate(
        US, strategy_targets(function, US.inputs, params), window, loaded, GATE, rebalance
    )

    assert row["gate"] == evaluate_rules(GATE.markets["qqq"].rules, result)
    assert row["us_gate"] == evaluate_rules(GATE.markets["us_market"].rules, result)
    assert row["episodes"] == ["crash_1929", "bear_1937", "war_1939"]


def test_the_strict_rows_differ_only_in_the_interval_bound() -> None:
    function, params, rebalance = _q1a()
    window = resolve_window(LOADED, "qqq_direction", "development")
    targets = strategy_targets(function, QQQ.inputs, params)

    row = gp.evaluate(QQQ, targets, window, LOADED, GATE, rebalance)

    pairs = list(zip(row["gate"], row["gate_strict"], strict=True))
    assert [a["metric"] for a, _ in pairs] == [b["metric"] for _, b in pairs]
    for normal, strict in pairs:
        if normal["metric"].startswith("ci_low."):
            assert strict["observed"] < normal["observed"]
        else:
            assert strict == normal
    assert row["holds_strict"] is all(item["holds"] for item in row["gate_strict"])


def test_a_window_without_episodes_counts_its_episode_rules_as_holding_and_says_so() -> None:
    targets = gp.planted_targets(US.inputs, 0.0)
    window = Window(window_id="us_1930", role="development", start=date(1930, 1, 2), end=None)
    plain = gp.evaluate(US, targets, window, gp.with_episodes(LOADED, ()), GATE, gp.MONTH_END)
    episodes = gp.evaluate(
        US, targets, window, gp.with_episodes(LOADED, gp.US_EPISODES), GATE, gp.MONTH_END
    )

    assert plain["no_episode"] is True and episodes["no_episode"] is False
    assert (plain["episodes"], episodes["episodes"]) == (
        [],
        ["crash_1929", "bear_1937", "war_1939"],
    )
    for empty, full in zip(plain["gate"], episodes["gate"], strict=True):
        if empty["metric"].startswith(gp.LEAVE_EPISODE):
            assert (empty["holds"], empty["no_episode"]) == (True, True)
        else:
            assert empty == full
    assert all("no_episode" not in item for item in episodes["gate"])


def test_a_refused_window_is_an_error_that_passes_nothing() -> None:
    gp._init(
        {
            "data": {"us_market": US},
            "rules": {
                "us_market": {
                    "late": gp.PowerRule(
                        rule_id="late",
                        kind="planted",
                        noise=0.0,
                        targets=pd.Series(np.nan, index=US.inputs.index),
                        rebalance=gp.MONTH_END,
                    )
                }
            },
            "loaded": {"us_market": LOADED},
            "gate": GATE,
        }
    )
    window = Window(window_id="us_1930", role="development", start=date(1930, 1, 2), end=None)

    row = gp._task(("us_market", "late", window))

    assert row["error"].startswith("ValueError: ") and row["gate"] == []
    assert (row["holds"], row["holds_strict"], row["us_holds"]) == (False, False, False)


def test_any_failure_in_a_window_is_kept_as_its_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken(*args: Any) -> dict[str, Any]:
        raise RuntimeError("boom\nsecond line")

    rule = gp.PowerRule(
        rule_id="x",
        kind="planted",
        noise=0.0,
        targets=gp.planted_targets(US.inputs, 0.0),
        rebalance=gp.MONTH_END,
    )
    gp._init(
        {
            "data": {"us_market": US},
            "rules": {"us_market": {"x": rule}},
            "loaded": {"us_market": LOADED},
            "gate": GATE,
        }
    )
    monkeypatch.setattr(gp, "evaluate", broken)
    window = Window(window_id="us_1930", role="development", start=date(1930, 1, 2), end=None)

    row = gp._task(("us_market", "x", window))

    assert row["error"] == "RuntimeError: boom"
    assert (row["holds"], row["return_days"], row["episodes"]) == (False, None, [])


# --- the whole probe --------------------------------------------------------------------------


def test_the_probe_evaluates_every_rule_in_every_window(power: dict[str, Any]) -> None:
    rule_ids = ["null", "noise_0", "noise_3"]
    rule_ids += ["q1a", "trend4", "ma10"]
    evaluations = power["evaluations"]

    assert [w["window_id"] for w in power["windows"]["us_market"]] == ["us_1930", "us_1932"]
    assert power["windows"]["us_non_overlapping"] == ["us_1930"]
    assert power["windows"]["qqq"] == {
        "window_id": "qqq_long_history_development",
        "start": "1999-03-10",
        "end": "2010-12-31",
    }
    us = [(row["window_id"], row["rule_id"]) for row in evaluations if row["market"] == "us_market"]
    assert us == [(window, rule) for window in ("us_1930", "us_1932") for rule in rule_ids]
    qqq = [row["rule_id"] for row in evaluations if row["market"] == "qqq"]
    assert qqq == rule_ids
    assert list(power["effects"]["us_market"]) == list(power["effects"]["qqq"]) == rule_ids
    assert not [row for row in evaluations if row["error"]]
    assert not [row for row in evaluations if row["no_episode"]]  # the US rows use US_EPISODES
    assert power["episodes"]["us_market"] == [e.model_dump() for e in gp.US_EPISODES]
    assert power["summary"]["us_market"]["noise_0"]["windows"] == 2
    assert power["reading"]["verdict"] in gp.VERDICT_TEXT


def test_the_result_records_what_produced_it(power: dict[str, Any]) -> None:
    trials = ROOT_DIR.joinpath("_trials")

    assert set(power["provenance"]) >= {"commit", "branch", "code_modified"}
    assert power["settings_sha256"] == LOADED.sha256
    assert power["gate_sha256"] == lg.chain_digest(gp.MEASURED_GATE.read_bytes())
    assert power["plan_sha256"] == lg.chain_digest(trials.joinpath("T0001.yaml").read_bytes())
    assert power["falsification_settings"]["placebo_draws"] == 10
    assert power["falsification_settings"]["bootstrap_draws"] == 200
    for row in power["evaluations"]:
        assert row["return_days"] > 2000 and row["saturdays"] == 0


def test_a_gate_at_another_confidence_is_refused() -> None:
    other = GATE.model_copy(update={"confidence": 0.9})
    with pytest.raises(ValueError, match="confidence 0.9 is not 0.95"):
        gp.run_power({"us_market": US, "qqq": QQQ}, LOADED, other, ROOT_DIR)


def test_saturday_sessions_are_counted() -> None:
    index = pd.bdate_range(
        "1926-07-01", US_END, freq="C", weekmask="Mon Tue Wed Thu Fri Sat", name="date"
    )
    factors = pd.DataFrame(
        {
            "Mkt-RF": np.random.default_rng(7).normal(0.0003, 0.01, len(index)),
            "SMB": 0.0,
            "HML": 0.0,
            "RF": 0.0001,
        },
        index=index,
    )
    data = from_us_market({"ff_factors_daily": factors}, snapshot_id="sig-test")
    window = Window(
        window_id="us_1930", role="development", start=date(1930, 1, 2), end=date(1940, 2, 19)
    )
    targets = gp.planted_targets(data.inputs, 0.0)

    row = gp.evaluate(
        data, targets, window, gp.with_episodes(LOADED, gp.US_EPISODES), GATE, gp.MONTH_END
    )

    inside = index[(index > pd.Timestamp("1930-01-02")) & (index <= pd.Timestamp("1940-02-19"))]
    assert row["saturdays"] == int((inside.weekday == 5).sum()) > 500


def test_the_us_effects_cover_the_whole_us_period(power: dict[str, Any]) -> None:
    effect = power["effects"]["us_market"]["noise_0"]

    assert (effect["window_id"], effect["start"], effect["end"]) == (
        "us_full",
        "1930-01-01",
        US_END,
    )


def test_the_real_rules_use_their_preregistered_parameters() -> None:
    rules = {rule.rule_id: rule for rule in gp.rules_for(US, ROOT_DIR)}
    function, params, rebalance = _q1a()

    assert [rule.kind for rule in rules.values()].count("real") == 3
    assert rules["q1a"].rebalance == rebalance
    pd.testing.assert_series_equal(
        rules["q1a"].targets, strategy_targets(function, US.inputs, params)
    )
    for rule_id in ("trend4", "ma10"):
        question = lg.load_question(ROOT_DIR.joinpath("QQQ-Q2"))
        entry = question.prereg.strategy(rule_id)
        expected = strategy_targets(
            lg.load_strategy(question, rule_id), US.inputs, dict(entry.params)
        )
        pd.testing.assert_series_equal(rules[rule_id].targets, expected)
        assert rules[rule_id].rebalance == entry.rebalance.to_rebalance()


def test_parallel_workers_give_the_same_result(power: dict[str, Any]) -> None:
    parallel = gp.run_power(
        {"us_market": US, "qqq": QQQ}, LOADED, GATE, ROOT_DIR, workers=2, noise_levels=LEVELS
    )

    assert json.dumps(gp._clean(parallel)) == json.dumps(gp._clean(power))


# --- the fixed reading ------------------------------------------------------------------------


def _result(
    gains: dict[str, float | None], passes: dict[str, int], windows: int = 100
) -> dict[str, Any]:
    rows = []
    for rule_id, count in passes.items():
        for i in range(windows):
            rows.append(
                {
                    "market": "us_market",
                    "rule_id": rule_id,
                    "holds": i < count,
                    "holds_strict": i < count // 2,
                    "error": None,
                }
            )
    effects = {rule_id: {gp.TAIL_FIELD: gain} for rule_id, gain in gains.items()}
    return {
        "noise_levels": list(gp.NOISE_LEVELS),
        "effects": {"us_market": effects},
        "evaluations": rows,
    }


GAINS = {
    "null": -0.3,
    "noise_0": 0.5,
    "noise_0.25": 0.45,
    "noise_0.5": 0.35,
    "noise_1": 0.20,
    "noise_1.5": 0.19,
    "noise_2": None,
    "noise_3": 0.05,
}


@pytest.mark.parametrize(
    ("passes", "verdict"),
    [(49, "below_50"), (50, "50_to_80"), (79, "50_to_80"), (80, "at_least_80")],
)
def test_power_is_read_on_the_weakest_level_at_the_effect_line(passes: int, verdict: str) -> None:
    read = gp.reading(_result(GAINS, {"null": 3, "noise_1": passes, "noise_0": 100}))

    assert read["level"] == 1.0 and read["level_effect"] == 0.20
    assert read["power"] == pytest.approx(passes / 100)
    assert read["power_strict"] == pytest.approx((passes // 2) / 100)
    assert read["verdict"] == verdict
    assert (read["null_rate"], read["null_loose"]) == (0.03, False)


@pytest.mark.parametrize(("passes", "loose"), [(10, False), (11, True)])
def test_a_null_passing_in_more_than_a_tenth_of_windows_is_a_loose_gate(
    passes: int, loose: bool
) -> None:
    read = gp.reading(_result(GAINS, {"null": passes, "noise_1": 90}))

    assert read["null_loose"] is loose


def test_l_is_the_smallest_improvement_at_the_line_even_if_the_curve_bends() -> None:
    gains = {**GAINS, "noise_1": 0.22, "noise_1.5": 0.24, "noise_2": 0.15}

    read = gp.reading(_result(gains, {"null": 0, "noise_1": 28, "noise_1.5": 83}))

    assert (read["level"], read["level_effect"], read["monotone"]) == (1.0, 0.22, False)
    assert read["verdict"] == "below_50"
    assert gp.reading(_result(GAINS, {"null": 0, "noise_1": 90}))["monotone"] is True


def test_on_a_tie_l_is_the_noisier_level() -> None:
    gains = {**GAINS, "noise_0.5": 0.20}

    assert gp.reading(_result(gains, {"null": 0, "noise_1": 90}))["level"] == 1.0


@pytest.mark.parametrize(("passes", "above"), [(5, False), (6, True)])
def test_a_null_above_five_percent_is_flagged(passes: int, above: bool) -> None:
    read = gp.reading(_result(GAINS, {"null": passes, "noise_1": 90}))

    assert (read["null_above_design"], read["null_loose"]) == (above, False)


def test_power_without_saturdays_reads_only_the_windows_without_them() -> None:
    result = _result(GAINS, {"null": 0, "noise_1": 50})
    for row in result["evaluations"]:
        row["saturdays"] = 30 if row["holds"] else 0

    read = gp.reading(result)

    assert read["power"] == 0.5 and read["power_without_saturdays"] == 0.0


def test_no_level_at_the_effect_line_leaves_the_reading_open() -> None:
    gains = {key: (None if value is None else min(value, 0.19)) for key, value in GAINS.items()}

    read = gp.reading(_result(gains, {"null": 0, "noise_0": 100}))

    assert (read["level"], read["power"], read["verdict"]) == (None, None, "no_level")


def test_an_error_in_the_level_or_the_null_makes_the_reading_incomplete() -> None:
    result = _result(GAINS, {"null": 0, "noise_1": 90})
    next(row for row in result["evaluations"] if row["rule_id"] == "noise_1")["error"] = "x"

    assert gp.reading(result)["verdict"] == "incomplete"


# --- output and command -----------------------------------------------------------------------


def test_the_result_is_written_as_strict_json_and_a_chinese_summary(
    power: dict[str, Any], tmp_path: Path
) -> None:
    gp.write_result(power, tmp_path)

    data = json.loads(tmp_path.joinpath("power_result.json").read_text(encoding="utf-8"))
    text = tmp_path.joinpath("power_result.md").read_text(encoding="utf-8")
    assert data["run"] == "TRADING-2565-17" and data["cutoff"] == "2010-12-31"
    assert data["reading"]["verdict"] == power["reading"]["verdict"]
    assert text.startswith("# 查尺子结果（TRADING-2565-17）\n")
    assert gp.VERDICT_TEXT[power["reading"]["verdict"]] in text
    for rule_id in power["summary"]["us_market"]:
        assert f"| {rule_id} |" in text
    assert "| 规则 | 左尾改善 | 回撤改善 | 11 条成立 | 全过 |" in text
    assert "含周六交易的窗口 0 个" in text and "不重叠的只有 1 个" in text
    assert data["provenance"] == power["provenance"]


def test_the_command_writes_the_result(
    power: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[int] = []

    def fake_run(*args: Any, workers: int, **kwargs: Any) -> dict[str, Any]:
        calls.append(workers)
        return dict(power)

    monkeypatch.setattr(
        gp, "load_datasets", lambda root: ({"us_market": US, "qqq": QQQ}, GATE, LOADED, ROOT_DIR)
    )
    monkeypatch.setattr(gp, "run_power", fake_run)

    out = tmp_path / "power"
    result = CliRunner().invoke(gp.app, ["run", "--out", str(out), "--workers", "3"])

    assert result.exit_code == 0, result.output
    assert calls == [3]
    assert "判读" in result.output
    data = json.loads(out.joinpath("power_result.json").read_text(encoding="utf-8"))
    assert data["source"] == {"us_market": US.source, "qqq": QQQ.source}


def test_the_loaded_data_are_cut_at_2010(monkeypatch: pytest.MonkeyPatch) -> None:
    late = {"us_market": _us_data(end="2011-06-30"), "qqq": _qqq_data()}
    late["qqq"] = BenchData(
        returns=late["qqq"].returns.reindex(pd.bdate_range("1994-01-03", "2011-06-30")),
        inputs=late["qqq"].inputs.reindex(pd.bdate_range("1994-01-03", "2011-06-30")),
        source=late["qqq"].source,
    )
    given: list[tr.TrialGate] = []

    def fake_data(market: str, gate: tr.TrialGate, *args: Any, **kwargs: Any) -> BenchData:
        given.append(gate)
        return late[market]

    monkeypatch.setattr(tr, "market_data", fake_data)

    datasets, gate, loaded, root_dir = gp.load_datasets(Path("unused"))

    # The data come with the measured gate, not the one trials use now.
    assert root_dir == ROOT_DIR and gate.confidence == 0.95
    assert gate == gp.load_measured_gate()
    assert given == [gate, gate]
    for market, data in datasets.items():
        assert pd.Timestamp(data.returns.index.max()) == pd.Timestamp("2010-12-31"), market
        assert pd.Timestamp(data.inputs.index.max()) == pd.Timestamp("2010-12-31"), market


def test_the_command_stops_when_the_data_do_not_load(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def refuse(root: Path) -> Any:
        raise ValueError("snapshot not checked")

    monkeypatch.setattr(gp, "load_datasets", refuse)

    result = CliRunner().invoke(gp.app, ["run", "--out", str(tmp_path / "x")])

    assert result.exit_code == 1
    assert "数据载入失败：snapshot not checked" in result.output
    assert not tmp_path.joinpath("x").exists()


def test_the_measured_gate_is_the_one_both_probe_runs_recorded() -> None:
    # Both real runs (gate_power on main 7f665986, ruler_power on main e019b10a) recorded this
    # digest; the trial gate moved to the two-stage ruler after them.
    gate = gp.load_measured_gate()
    q3 = yaml.safe_load((PROJECT_ROOT / "research/QQQ-Q3/prereg.yaml").read_text(encoding="utf-8"))[
        "validation"
    ]["rules"]["fast_slow"]

    assert lg.chain_digest(gp.MEASURED_GATE.read_bytes()) == (
        "b17d97a9f9358f34c404cf84b28e031c64c2f87296b080bb7cd0c53e94fa3b0d"
    )
    assert gate.owner_decision == "owner_decision:TRADING-2565:2026-10-15:trial_mode"
    assert [(r.metric, r.op, r.value) for r in gate.markets["qqq"].rules] == [
        (r["metric"], r["op"], r["value"]) for r in q3
    ]
    assert [(r.metric, r.op, r.value) for r in gate.markets["us_market"].rules] == [
        ("comparison.max_drawdown", ">=", 0.05),
        ("comparison.cvar_5pct_relative_improvement", ">=", 0.10),
        ("cost_25.geometric_annual_return", ">=", -0.010),
        ("delay_2.cvar_5pct_relative_improvement", ">=", 0.10),
    ]
