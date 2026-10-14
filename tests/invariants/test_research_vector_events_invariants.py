"""Invariants of the vectorized placebo and the de-risk event table (TRADING-2565 S4c).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-13:signal_screen_layer``). Each guard has a hand-checked
case.

23. ``simulate_many`` is ``simulate`` many times: every row equals ``simulate`` on that row's
    targets (drift, band, schedule, delay, cost), to 1e-12.
24. A path that loses everything stops the batch and names the path and the date.
25. The placebo is unchanged: ``placebo_comparisons`` equals the per-path reference built from
    ``placebo_targets`` and ``simulate``.
26. De-risk events follow their definitions on a hand-built path: which days count, merging,
    outcome, short events, and per fixed episode the de-risked share, missed loss, exit and
    re-entry lag.
27. Every event setting is read, the defaults are the ones the owner approved, and the event table
    never touches the fingerprint or ``config/research/research_bench.yaml``.

The edge cases at the end (unusable targets, chunking, threshold edges, settings files, the run's
own table, the ledger and the report) were added after mutation checks on a throwaway reference.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import (
    BenchData,
    Rebalance,
    RunSpec,
    from_daily_frame,
    load_settings,
    run_and_record,
    run_strategy,
    simulate,
)
from ai_trading_system.research_bench import falsify as falsify_module
from ai_trading_system.research_bench import ledger as lg
from ai_trading_system.research_bench.events import EventSettings, derisk_events
from ai_trading_system.research_bench.falsify import (
    FalsificationInputs,
    paired_comparison,
    placebo_comparisons,
    placebo_targets,
)
from ai_trading_system.research_bench.report import question_report
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.settings import EpisodeSpec, resolve_window
from ai_trading_system.research_bench.simulate import constant_targets, simulate_many

FULL = load_settings()
LOADED = replace(
    FULL,
    settings=FULL.settings.model_copy(
        update={
            "falsification": FULL.settings.falsification.model_copy(
                update={"placebo_draws": 30, "bootstrap_draws": 500}
            )
        }
    ),
)
DEV = resolve_window(LOADED, "qqq_direction", "development")
# research_bench.yaml as of QQQ-Q1's opening (TRADING-2567-09); S4 must not change it.
BENCH_SETTINGS_SHA256 = "69351836d2e2bacb8b2888fe783a0949f485af9bf3e0294ffdb78ba50d0c46c2"
TOL = 1e-12


def _data(seed: int = 7) -> BenchData:
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


def vol_target(inputs: pd.DataFrame, params: Any) -> pd.Series:
    vol = inputs["qqq_return"].rolling(int(params["window"])).std() * np.sqrt(252)
    return (float(params["target"]) / vol).clip(upper=float(params["cap"]))


def _target_rows(seed: int) -> np.ndarray[Any, Any]:
    """Seven target paths over the whole calendar: daily noise, piecewise-constant switches over
    the full 0..3 range, the two extremes, and a volatility target."""

    n = len(DATA.returns.index)
    rng = np.random.default_rng(seed)
    noise = rng.uniform(0.0, 3.0, n)
    switches = np.repeat(rng.uniform(0.0, 3.0, n // 15 + 1), 15)[:n]
    low = rng.uniform(0.0, 1.0, n)
    vol = vol_target(DATA.inputs, {"window": 21, "target": 0.25, "cap": 2.5}).to_numpy()
    vol = np.where(np.isfinite(vol), vol, 1.0)
    return np.vstack([noise, switches, low, np.zeros(n), np.full(n, 3.0), vol, np.full(n, 1.0)])


CONFIGS = [
    (Rebalance(), 0, 4.0),
    (Rebalance(schedule="month_end", band=0.1), 1, 25.0),
    (Rebalance(schedule="week_end", band=0.05), 2, 0.0),
    (Rebalance(schedule="daily", band=0.2), 0, 15.0),
]


# 23 ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("rebalance", "delay", "cost"), CONFIGS)
def test_simulate_many_equals_simulate_row_by_row(
    rebalance: Rebalance, delay: int, cost: float
) -> None:
    rows = _target_rows(seed=delay * 100 + int(cost))
    batch = simulate_many(
        DATA.returns,
        rows,
        start=DEV.start,
        end=DEV.end,
        rebalance=rebalance,
        cost_bps=cost,
        delay_days=delay,
    )
    assert batch.returns.shape[0] == rows.shape[0]
    for d, row in enumerate(rows):
        single = simulate(
            DATA.returns,
            pd.Series(row, index=DATA.returns.index),
            start=DEV.start,
            end=DEV.end,
            rebalance=rebalance,
            cost_bps=cost,
            delay_days=delay,
        )
        assert batch.index.equals(single.daily.index)
        assert batch.start == single.start
        for column, values in (
            ("return", batch.returns),
            ("gross_return", batch.gross_returns),
            ("exposure", batch.exposure),
            ("turnover", batch.turnover),
            ("cost", batch.cost),
        ):
            expected = single.daily[column].to_numpy(dtype=float)
            assert np.max(np.abs(values[d] - expected)) <= TOL, (d, column)


def test_simulate_many_refuses_targets_of_the_wrong_width() -> None:
    rows = _target_rows(seed=1)[:, :-1]
    with pytest.raises(ValueError):
        simulate_many(
            DATA.returns, rows, start=DEV.start, end=DEV.end, rebalance=Rebalance(), cost_bps=4.0
        )


# 24 ----------------------------------------------------------------------------------------------


def test_a_path_that_loses_everything_names_the_path_and_the_date() -> None:
    returns = DATA.returns.copy()
    crash = pd.Timestamp("2005-06-15")
    returns.loc[crash, "lev3"] = -1.0
    n = len(returns.index)
    rows = np.vstack([np.full(n, 0.5), np.full(n, 3.0), np.full(n, 1.0)])
    with pytest.raises(ValueError, match=r"path 1\b.*lost everything on 2005-06-15"):
        simulate_many(
            returns, rows, start=DEV.start, end=DEV.end, rebalance=Rebalance(), cost_bps=4.0
        )


# 25 ----------------------------------------------------------------------------------------------


def _inputs(
    targets: pd.Series, rebalance: Rebalance, delay: int, cost: float
) -> FalsificationInputs:
    sim = simulate(
        DATA.returns,
        targets,
        start=DEV.start,
        end=DEV.end,
        rebalance=rebalance,
        cost_bps=cost,
        delay_days=delay,
    )
    matched = simulate(
        DATA.returns,
        constant_targets(DATA.returns.index, float(sim.exposure.mean())),
        start=DEV.start,
        end=DEV.end,
        rebalance=rebalance,
        cost_bps=cost,
        delay_days=delay,
    )
    return FalsificationInputs(
        DATA.returns, targets, DEV.start, DEV.end, rebalance, delay, cost, sim, matched
    )


@pytest.mark.parametrize(("rebalance", "delay", "cost"), CONFIGS)
def test_the_placebo_equals_the_per_path_reference(
    rebalance: Rebalance, delay: int, cost: float
) -> None:
    targets = vol_target(DATA.inputs, {"window": 21, "target": 0.25, "cap": 2.5})
    inputs = _inputs(targets, rebalance, delay, cost)
    rows = [
        simulate(
            inputs.returns,
            placebo,
            start=inputs.start,
            end=inputs.end,
            rebalance=inputs.rebalance,
            cost_bps=inputs.cost_bps,
            delay_days=inputs.delay_days,
        ).returns.to_numpy(dtype=float)
        for placebo in placebo_targets(inputs, LOADED)
    ]
    benchmark = inputs.benchmark.returns.to_numpy(dtype=float)
    cash = inputs.returns["cash"].reindex(inputs.benchmark.returns.index).to_numpy(dtype=float)
    expected = paired_comparison(np.vstack(rows), benchmark, cash, LOADED)
    actual = placebo_comparisons(inputs, LOADED)
    assert set(actual) == set(expected)
    for field, values in expected.items():
        reference = np.asarray(values, dtype=float)
        got = np.asarray(actual[field], dtype=float)
        assert got.shape == reference.shape
        both_nan = np.isnan(got) & np.isnan(reference)
        assert np.all(both_nan | (np.abs(got - reference) <= TOL)), field


# 26 ----------------------------------------------------------------------------------------------

INDEX = pd.bdate_range("2005-01-03", periods=60)  # January 0..20, February 21..40, March 41..59
SETTINGS = EventSettings(
    derisk_ratio=0.75,
    merge_gap_days=5,
    protective_loss=-0.05,
    false_alarm_gain=0.05,
    short_event_days=21,
)
EPISODES = (
    EpisodeSpec(id="jan", start="2005-01", end="2005-01"),
    EpisodeSpec(id="feb", start="2005-02", end="2005-02"),
    EpisodeSpec(id="apr", start="2005-04", end="2005-04"),
)


def _path() -> tuple[pd.Series, pd.Series]:
    exposure = np.ones(60)
    exposure[5:8] = 0.5  # run 5..7
    exposure[10:12] = 0.4  # run 10..11, two days after the first: merged into one event 5..11
    exposure[23:47] = 0.2  # event 23..46, 24 days
    exposure[52:54] = 0.75  # event 52..53: exactly the threshold counts; five days after 46
    exposure[54:] = 1.2
    qqq = np.zeros(60)
    qqq[2] = 0.02  # January, not de-risked
    qqq[6] = -0.03  # inside the first event
    qqq[9] = -0.03  # a gap day inside the merged first event, not itself de-risked
    qqq[22] = -0.02  # February, the day before the second event
    qqq[30] = 0.06  # inside the second event
    qqq[52] = 0.01  # inside the third event
    qqq[55] = -0.04  # after every event
    return pd.Series(exposure, index=INDEX), pd.Series(qqq, index=INDEX)


def test_events_follow_their_definitions() -> None:
    exposure, qqq = _path()
    table = derisk_events(
        exposure, qqq, reference_exposure=1.0, settings=SETTINGS, episodes=EPISODES
    )
    assert table["reference_exposure"] == 1.0
    assert table["threshold"] == pytest.approx(0.75)
    events = table["events"]
    assert [(e["start"], e["end"], e["days"]) for e in events] == [
        (INDEX[5].date().isoformat(), INDEX[11].date().isoformat(), 7),
        (INDEX[23].date().isoformat(), INDEX[46].date().isoformat(), 24),
        (INDEX[52].date().isoformat(), INDEX[53].date().isoformat(), 2),
    ]
    assert [e["min_exposure"] for e in events] == pytest.approx([0.4, 0.2, 0.75])
    first_loss = 0.97 * 0.97 - 1.0
    assert [e["qqq_return"] for e in events] == pytest.approx([first_loss, 0.06, 0.01])
    assert [e["outcome"] for e in events] == ["protective", "false_alarm", "neutral"]

    assert table["count"] == 3
    assert table["short_count"] == 2
    assert table["per_year"] == pytest.approx(3 / (60 / 252))
    assert table["outcomes"]["protective"] == pytest.approx({"count": 1, "qqq_return": first_loss})
    assert table["outcomes"]["false_alarm"] == pytest.approx({"count": 1, "qqq_return": 0.06})
    assert table["outcomes"]["neutral"] == pytest.approx({"count": 1, "qqq_return": 0.01})

    episodes = table["episodes"]
    assert set(episodes) == {"jan", "feb"}  # April is outside the data
    assert episodes["jan"]["days"] == 21
    assert episodes["jan"]["derisked_share"] == pytest.approx(5 / 21)
    assert episodes["jan"]["missed_loss"] == pytest.approx(1.02 * 0.97 - 1.0)
    assert episodes["jan"]["exit_lag_days"] == 5
    assert episodes["jan"]["reentry_lag_days"] == 0
    assert episodes["feb"]["days"] == 20
    assert episodes["feb"]["derisked_share"] == pytest.approx(18 / 20)
    assert episodes["feb"]["missed_loss"] == pytest.approx(-0.02)
    assert episodes["feb"]["exit_lag_days"] == 2
    assert episodes["feb"]["reentry_lag_days"] == 7


def test_an_episode_never_de_risked_or_never_re_entered_reports_none() -> None:
    exposure = pd.Series(np.ones(60), index=INDEX)
    exposure.iloc[45:] = 0.1  # de-risked from March 7 to the end of the data
    qqq = pd.Series(np.zeros(60), index=INDEX)
    episodes = (
        EpisodeSpec(id="feb", start="2005-02", end="2005-02"),
        EpisodeSpec(id="mar", start="2005-03", end="2005-03"),
    )
    table = derisk_events(
        exposure, qqq, reference_exposure=1.0, settings=SETTINGS, episodes=episodes
    )
    assert table["episodes"]["feb"]["exit_lag_days"] is None
    assert table["episodes"]["feb"]["derisked_share"] == 0.0
    assert table["episodes"]["mar"]["exit_lag_days"] == 4
    assert table["episodes"]["mar"]["reentry_lag_days"] is None
    assert table["count"] == 1


# 27 ----------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "change",
    [
        {"derisk_ratio": 0.7},
        {"merge_gap_days": 2},
        {"merge_gap_days": 6},
        {"protective_loss": -0.06},
        {"false_alarm_gain": 0.07},
        {"short_event_days": 25},
    ],
)
def test_every_event_setting_is_read(change: dict[str, Any]) -> None:
    exposure, qqq = _path()
    base = derisk_events(
        exposure, qqq, reference_exposure=1.0, settings=SETTINGS, episodes=EPISODES
    )
    moved = derisk_events(
        exposure,
        qqq,
        reference_exposure=1.0,
        settings=SETTINGS.model_copy(update=change),
        episodes=EPISODES,
    )
    assert moved != base


@pytest.mark.parametrize(
    "bad",
    [
        {"derisk_ratio": 0.0},
        {"derisk_ratio": 1.0},
        {"merge_gap_days": 0},
        {"protective_loss": 0.01},
        {"false_alarm_gain": -0.01},
        {"short_event_days": 0},
    ],
)
def test_event_settings_out_of_range_are_refused(bad: dict[str, Any]) -> None:
    values = SETTINGS.model_dump() | bad
    with pytest.raises(ValueError):
        EventSettings(**values)


def test_the_event_defaults_are_the_approved_ones() -> None:
    loaded = load_screen_settings()
    assert loaded.settings.derisk_events == SETTINGS
    assert loaded.settings.production_effect == "none"
    assert loaded.settings.broker_action == "none"


def test_the_bench_settings_file_is_unchanged() -> None:
    path = PROJECT_ROOT / "config" / "research" / "research_bench.yaml"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == BENCH_SETTINGS_SHA256


def test_runs_carry_the_event_table_outside_the_fingerprint() -> None:
    spec = RunSpec(
        "qqq_direction", "vol", {"window": 21, "target": 0.25, "cap": 1.0}, "development"
    )
    loaded_screen = load_screen_settings()
    stricter = replace(
        loaded_screen,
        settings=loaded_screen.settings.model_copy(
            update={
                "derisk_events": loaded_screen.settings.derisk_events.model_copy(
                    update={"derisk_ratio": 0.5}
                )
            }
        ),
    )
    first = run_strategy(DATA, vol_target, spec, LOADED, screen=loaded_screen)
    second = run_strategy(DATA, vol_target, spec, LOADED, screen=stricter)
    assert first.events is not None and second.events is not None
    assert first.events != second.events
    assert first.fingerprint == second.fingerprint
    assert "events" in first.to_dict()
    seen = first.events["episodes"]
    assert set(seen) <= {episode.id for episode in LOADED.settings.episodes}


# Edge cases (added after mutation checks) --------------------------------------------------------


def test_simulate_many_names_the_path_of_an_unusable_target() -> None:
    """23/24: the range check of ``_checked_weights`` and the opening target, with ``path d``
    at the start of the message."""

    n = len(DATA.returns.index)
    first = int(DATA.returns.index.searchsorted(pd.Timestamp(DEV.start)))
    rows = np.vstack([np.full(n, 1.0), np.full(n, 0.5), np.full(n, 2.0)])
    rows[2, first + 40] = 3.5  # decided inside the window, above the 3x cap
    with pytest.raises(ValueError, match=r"^path 2\b"):
        simulate_many(
            DATA.returns, rows, start=DEV.start, end=DEV.end, rebalance=Rebalance(), cost_bps=4.0
        )
    rows = np.vstack([np.full(n, 1.0), np.full(n, 0.5), np.full(n, 2.0)])
    rows[1, first + 40] = -0.1
    with pytest.raises(ValueError, match=r"^path 1\b"):
        simulate_many(
            DATA.returns, rows, start=DEV.start, end=DEV.end, rebalance=Rebalance(), cost_bps=4.0
        )
    rows = np.vstack([np.full(n, 1.0), np.full(n, 0.5), np.full(n, 2.0)])
    rows[1, :first] = np.nan  # nothing to open with
    with pytest.raises(ValueError, match=r"^path 1\b"):
        simulate_many(
            DATA.returns, rows, start=DEV.start, end=DEV.end, rebalance=Rebalance(), cost_bps=4.0
        )


def test_the_placebo_is_unchanged_across_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    """25 with chunks of 7 paths: 30 draws are four full chunks and a partial one, so a chunk that
    is kept twice or dropped shows (with the real settings, 1,000 draws pass 250 per chunk)."""

    monkeypatch.setattr(falsify_module, "_CHUNK", 7)
    rebalance, delay, cost = CONFIGS[1]
    targets = vol_target(DATA.inputs, {"window": 21, "target": 0.25, "cap": 2.5})
    inputs = _inputs(targets, rebalance, delay, cost)
    rows = [
        simulate(
            inputs.returns,
            placebo,
            start=inputs.start,
            end=inputs.end,
            rebalance=inputs.rebalance,
            cost_bps=inputs.cost_bps,
            delay_days=inputs.delay_days,
        ).returns.to_numpy(dtype=float)
        for placebo in placebo_targets(inputs, LOADED)
    ]
    benchmark = inputs.benchmark.returns.to_numpy(dtype=float)
    cash = inputs.returns["cash"].reindex(inputs.benchmark.returns.index).to_numpy(dtype=float)
    expected = paired_comparison(np.vstack(rows), benchmark, cash, LOADED)
    actual = placebo_comparisons(inputs, LOADED)
    for field, values in expected.items():
        reference = np.asarray(values, dtype=float)
        got = np.asarray(actual[field], dtype=float)
        assert got.shape == reference.shape
        both_nan = np.isnan(got) & np.isnan(reference)
        assert np.all(both_nan | (np.abs(got - reference) <= TOL)), field


EDGE_SETTINGS = EventSettings(
    derisk_ratio=0.75,
    merge_gap_days=5,
    protective_loss=-0.25,  # exact in binary, so the boundary is hit exactly
    false_alarm_gain=0.25,
    short_event_days=4,
)


def _edge_path() -> tuple[pd.Series, pd.Series]:
    exposure = np.ones(60)
    exposure[1:5] = 0.5  # event 1..4, 4 days: exactly short_event_days, not short
    exposure[10:13] = 0.5  # event 10..12, 3 days, short; five days after the first
    exposure[18:21] = 0.5  # event 18..20, 3 days, short; ends on January's last day
    exposure[21:25] = 0.9  # above the threshold, below the reference: not yet re-entered
    qqq = np.zeros(60)
    qqq[2] = -0.25  # first event: exactly protective_loss
    qqq[11] = 0.25  # second event: exactly false_alarm_gain
    qqq[19] = -0.5  # third event: protective
    return pd.Series(exposure, index=INDEX), pd.Series(qqq, index=INDEX)


def test_event_edges_follow_their_definitions() -> None:
    """26 at the edges: thresholds count with their equal sign, ``days == short_event_days`` is not
    short, a class compounds over all its events, an empty class is 0.0, ``per_year`` uses the
    given year length, re-entry needs the reference exposure, and every value is plain JSON."""

    exposure, qqq = _edge_path()
    episodes = (EpisodeSpec(id="jan", start="2005-01", end="2005-01"),)
    table = derisk_events(
        exposure,
        qqq,
        reference_exposure=1.0,
        settings=EDGE_SETTINGS,
        episodes=episodes,
        trading_days_per_year=120,
    )
    events = table["events"]
    assert [(e["days"], e["outcome"]) for e in events] == [
        (4, "protective"),
        (3, "false_alarm"),
        (3, "protective"),
    ]
    assert [e["qqq_return"] for e in events] == pytest.approx([-0.25, 0.25, -0.5])
    assert table["count"] == 3
    assert table["short_count"] == 2
    assert table["per_year"] == pytest.approx(3 / (60 / 120))
    protective = {"count": 2, "qqq_return": 0.75 * 0.5 - 1.0}  # compounded, not -0.75
    assert table["outcomes"]["protective"] == pytest.approx(protective)
    assert table["outcomes"]["false_alarm"] == pytest.approx({"count": 1, "qqq_return": 0.25})
    assert table["outcomes"]["neutral"] == pytest.approx({"count": 0, "qqq_return": 0.0})
    jan = table["episodes"]["jan"]
    assert jan["derisked_share"] == pytest.approx(10 / 21)
    assert jan["exit_lag_days"] == 1
    assert jan["reentry_lag_days"] == 5  # day 25, not day 21 where the cut ends
    assert json.loads(json.dumps(table)) == table
    assert all(type(e["days"]) is int for e in table["events"])
    assert type(table["count"]) is int and type(table["short_count"]) is int
    assert type(jan["exit_lag_days"]) is int and type(jan["reentry_lag_days"]) is int
    assert type(jan["days"]) is int


def test_events_refuse_returns_on_other_dates() -> None:
    exposure, qqq = _path()
    shifted = pd.Series(qqq.to_numpy(), index=pd.bdate_range("2005-01-04", periods=60))
    with pytest.raises(ValueError):
        derisk_events(
            exposure, shifted, reference_exposure=1.0, settings=SETTINGS, episodes=EPISODES
        )
    with pytest.raises(ValueError):
        derisk_events(
            exposure, qqq.iloc[:-1], reference_exposure=1.0, settings=SETTINGS, episodes=EPISODES
        )


@pytest.mark.parametrize("bad", [{"protective_loss": 0.0}, {"false_alarm_gain": 0.0}])
def test_event_settings_on_the_boundary_are_refused(bad: dict[str, Any]) -> None:
    values = SETTINGS.model_dump() | bad
    with pytest.raises(ValueError):
        EventSettings(**values)


def test_screen_settings_are_read_from_the_given_file(tmp_path: Path) -> None:
    raw = yaml.safe_load(
        (PROJECT_ROOT / "config" / "research" / "research_screen.yaml").read_text("utf-8")
    )
    raw["derisk_events"]["derisk_ratio"] = 0.6
    raw["derisk_events"]["short_event_days"] = 10
    path = tmp_path / "screen.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    loaded = load_screen_settings(path)
    assert loaded.settings.derisk_events == SETTINGS.model_copy(
        update={"derisk_ratio": 0.6, "short_event_days": 10}
    )
    assert loaded.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    for extra in ({"derisk_events": {**raw["derisk_events"], "merge_gap": 3}}, {"spare": 1}):
        path.write_text(yaml.safe_dump(raw | extra), encoding="utf-8")
        with pytest.raises(ValueError):
            load_screen_settings(path)


def _close(got: Any, expected: Any) -> bool:
    if isinstance(expected, dict):
        return (
            isinstance(got, dict)
            and set(got) == set(expected)
            and all(_close(got[key], value) for key, value in expected.items())
        )
    if isinstance(expected, list):
        return (
            isinstance(got, list)
            and len(got) == len(expected)
            and all(_close(a, b) for a, b in zip(got, expected, strict=True))
        )
    if isinstance(expected, float) and isinstance(got, float):
        return math.isclose(got, expected, rel_tol=1e-12, abs_tol=1e-12)
    return bool(got == expected)


def test_runs_build_the_event_table_from_their_own_simulation() -> None:
    """27: the run's table is ``derisk_events`` on the run's exposure, QQQ's returns on the same
    dates, the average exposure as reference and the bench's episodes."""

    params = {"window": 21, "target": 0.25, "cap": 1.0}
    spec = RunSpec("qqq_direction", "vol", params, "development")
    loaded_screen = load_screen_settings()
    result = run_strategy(DATA, vol_target, spec, LOADED, screen=loaded_screen)
    sim = simulate(
        DATA.returns,
        vol_target(DATA.inputs, params).astype(float),
        start=DEV.start,
        end=DEV.end,
        rebalance=Rebalance(),
        cost_bps=LOADED.cost_bps,
    )
    expected = derisk_events(
        sim.exposure,
        DATA.returns["qqq"].loc[sim.exposure.index],
        reference_exposure=float(sim.exposure.mean()),
        settings=loaded_screen.settings.derisk_events,
        episodes=LOADED.settings.episodes,
    )
    assert expected["count"] > 0 and expected["episodes"]
    assert _close(result.events, expected)


STRATEGY_MODULE = """
def level(inputs, params):
    return inputs["qqq_return"] * 0.0 + float(params["level"])


def vol(inputs, params):
    vol = inputs["qqq_return"].rolling(int(params["window"])).std() * 252 ** 0.5
    return (float(params["target"]) / vol).clip(upper=float(params["cap"]))
"""


def _prereg() -> dict[str, Any]:
    return {
        "schema_version": "research_prereg.v1",
        "question_id": "QQQ-E1",
        "question": "Does a volatility target cut exposure ahead of losses?",
        "question_type": "qqq_direction",
        "data": {"snapshot_id": "synthetic-7", "qqq_series": "proxy"},
        "strategies": [
            {"id": "vol", "function": "vol", "params": {"window": 21, "target": 0.25, "cap": 1.0}},
            {"id": "flat", "function": "level", "params": {"level": 0.5}},
        ],
        "validation": {
            "strategies": ["vol"],
            "rules": {"vol": [{"metric": "ci_low.max_drawdown", "op": ">", "value": 0.0}]},
        },
        "prior_expectation": {"effect": "e", "fails_when": "f", "sources": ["s"]},
        "sample_floor": {"min_return_days": 100},
        "trial_cap": 4,
        "stop_conditions": ["cap"],
        "production_effect": "none",
        "broker_action": "none",
    }


def test_the_ledger_records_the_table_and_the_report_shows_it(tmp_path: Path) -> None:
    """D: the run record carries ``events``; ``report`` adds the 减仓事件 section from the record
    and says 无（S4c 之前的运行） for a record without it."""

    directory = tmp_path / "QQQ-E1"
    directory.mkdir()
    (directory / "prereg.yaml").write_text(yaml.safe_dump(_prereg()), encoding="utf-8")
    (directory / "strategy.py").write_text(STRATEGY_MODULE, encoding="utf-8")
    question = lg.load_question(directory)
    vol = run_and_record(question, DATA, "vol", loaded=LOADED)
    run_and_record(question, DATA, "flat", loaded=LOADED)
    assert vol.result is not None and vol.result.events is not None
    records = lg.run_records(directory)
    assert records[0]["result"]["events"] == json.loads(json.dumps(vol.result.events))

    # The last record has no successor, so taking ``events`` out of it keeps the chain whole:
    # it reads like a record written before S4c.
    last = directory / lg.RUNS_DIR / "0002.json"
    old = json.loads(last.read_bytes())
    del old["result"]["events"]
    last.write_text(json.dumps(old, ensure_ascii=False, indent=1, sort_keys=True) + "\n", "utf-8")

    lines = question_report(tmp_path, "QQQ-E1", LOADED.settings)
    heads = [i for i, line in enumerate(lines) if line.startswith("QQQ-E1-r000")]
    assert len(heads) == 2
    vol_section = "\n".join(lines[heads[0] : heads[1]])
    flat_section = "\n".join(lines[heads[1] :])
    assert "减仓事件" in vol_section
    assert "无（S4c 之前的运行）" not in vol_section
    for episode in vol.result.events["episodes"]:
        assert episode in vol_section
    assert "无（S4c 之前的运行）" in flat_section
