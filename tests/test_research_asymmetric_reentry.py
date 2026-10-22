"""TRADING-2565-48: H-A asymmetric re-entry probe on synthetic data shaped like the long history."""

from __future__ import annotations

import json
import math
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
from test_research_policy_table import DATA, GATE, LOADED, PROVENANCE, ROOT_DIR
from typer.testing import CliRunner

from ai_trading_system.research_bench.metrics import summarize
from ai_trading_system.research_bench.settings import EpisodeSpec
from ai_trading_system.research_bench.simulate import Rebalance, schedule_mask, simulate
from ai_trading_system.research_probes import asymmetric_reentry as ar
from ai_trading_system.research_probes import plot_run as pr
from ai_trading_system.research_probes import policy_table as pt

pytestmark = pytest.mark.filterwarnings("ignore:invalid value encountered in log1p")

DAYS = pd.bdate_range("2001-01-01", "2001-03-30")


def _when(text: str) -> pd.Timestamp:
    return pd.Timestamp(text)


def _scores(fast: list[tuple[str, float]], slow: list[tuple[str, float]]) -> pd.DataFrame:
    """Scores that take each value from its date on (the first value from the first day)."""

    def build(changes: list[tuple[str, float]]) -> pd.Series:
        series = pd.Series(np.nan, index=DAYS)
        for start, value in changes:
            series.loc[_when(start) :] = value
        return series

    return pd.DataFrame({"fast": build(fast), "slow": build(slow)}, index=DAYS)


# Fast out on the January month end, back to 1 on Friday 02-09, out again 02-16 (a week end,
# which never exits), out at the February month end, exactly zero (0.5) on Friday 03-02, back
# on Friday 03-09; slow turns 0 on Friday 03-09 and leaves only at the March month end.
SCORES = _scores(
    [
        ("2001-01-01", 1.0),
        ("2001-01-31", 0.0),
        ("2001-02-09", 1.0),
        ("2001-02-16", 0.0),
        ("2001-03-02", 0.5),
        ("2001-03-09", 1.0),
    ],
    [("2001-01-01", 1.0), ("2001-03-09", 0.0)],
)


def _legs_between(decided: pd.DataFrame, start: str, end: str) -> set[tuple[float, float]]:
    part = decided.loc[_when(start) : _when(end), ["fast", "slow"]]
    return {(float(f), float(s)) for f, s in part.itertuples(index=False)}


def test_monthly_decides_both_legs_at_month_ends_only() -> None:
    decided = ar.decide(SCORES, "monthly")
    assert decided.loc[: _when("2001-01-30"), ["fast", "slow", "target"]].isna().all().all()
    assert _legs_between(decided, "2001-01-31", "2001-02-27") == {(0.0, 1.0)}
    assert _legs_between(decided, "2001-02-28", "2001-03-29") == {(0.0, 1.0)}
    assert _legs_between(decided, "2001-03-30", "2001-03-30") == {(1.0, 0.0)}
    trades = list(decided.index[decided["trade"]])
    assert trades == [_when("2001-01-31"), _when("2001-02-28"), _when("2001-03-30")]
    assert not decided["reentry"].any()
    assert decided["target"].equals(0.5 * decided["fast"] + 0.5 * decided["slow"])


def test_weekly_decides_both_legs_at_week_ends() -> None:
    decided = ar.decide(SCORES, "weekly")
    assert decided.loc[: _when("2001-01-04"), "target"].isna().all()
    assert _legs_between(decided, "2001-01-05", "2001-02-01") == {(1.0, 1.0)}
    assert _legs_between(decided, "2001-02-02", "2001-02-08") == {(0.0, 1.0)}
    assert _legs_between(decided, "2001-02-09", "2001-02-15") == {(1.0, 1.0)}
    assert _legs_between(decided, "2001-03-02", "2001-03-08") == {(0.5, 1.0)}
    assert _legs_between(decided, "2001-03-09", "2001-03-30") == {(1.0, 0.0)}
    assert list(decided.index[decided["trade"]]) == list(
        DAYS[np.asarray(schedule_mask(DAYS, "week_end"), dtype=bool)]
    )


def test_h_a_brings_the_fast_leg_back_on_a_week_end_and_never_exits_there() -> None:
    decided = ar.decide(SCORES, "h_a")
    assert decided.loc[: _when("2001-01-30"), "target"].isna().all()
    assert _legs_between(decided, "2001-01-31", "2001-02-08") == {(0.0, 1.0)}
    # Back on Friday 02-09; the zero score on Friday 02-16 does not take it out.
    assert _legs_between(decided, "2001-02-09", "2001-02-27") == {(1.0, 1.0)}
    assert _legs_between(decided, "2001-02-28", "2001-03-08") == {(0.0, 1.0)}
    # A 0.5 score is not a re-entry; the slow leg's zero on Friday 03-09 waits for the month end.
    assert _legs_between(decided, "2001-03-09", "2001-03-29") == {(1.0, 1.0)}
    assert _legs_between(decided, "2001-03-30", "2001-03-30") == {(1.0, 0.0)}
    assert list(decided.index[decided["reentry"]]) == [_when("2001-02-09"), _when("2001-03-09")]
    assert list(decided.index[decided["trade"]]) == [
        _when("2001-01-31"),
        _when("2001-02-09"),
        _when("2001-02-28"),
        _when("2001-03-09"),
        _when("2001-03-30"),
    ]


def test_h_a_waits_for_a_month_end_with_both_scores() -> None:
    scores = SCORES.copy()
    scores.loc[: _when("2001-02-27"), "slow"] = np.nan
    decided = ar.decide(scores, "h_a")
    # No state before the first month end with both scores, so the 02-09 week end does nothing;
    # the January month end stays a trade day, as in the bench's month-end schedule.
    assert decided.loc[: _when("2001-02-27"), "target"].isna().all()
    assert not decided.loc[: _when("2001-02-27"), "reentry"].any()
    assert list(decided.index[decided["trade"]])[:2] == [_when("2001-01-31"), _when("2001-02-28")]
    assert _legs_between(decided, "2001-02-28", "2001-03-08") == {(0.0, 1.0)}
    assert decided.loc[_when("2001-03-09"), "reentry"]


def test_unknown_mode_is_refused() -> None:
    with pytest.raises(ValueError, match="unknown mode"):
        ar.decide(SCORES, "daily")  # type: ignore[arg-type]


def _fast_slow_signals() -> list[dict[str, Any]]:
    return ar.source_entries(ROOT_DIR)["fast_slow"].params["signals"]


def test_the_legs_are_the_fast_slow_signals_one_at_a_time() -> None:
    signals = _fast_slow_signals()
    assert [s["id"] for s in signals] == [ar.FAST_SIGNAL, ar.SLOW_SIGNAL]
    assert ar.leg_params(signals[0]) == {
        "signals": [signals[0]],
        "mapping": "linear",
        "low": 0,
        "high": 1,
    }
    inputs = DATA["qqq"].inputs
    scores = ar.leg_scores(inputs, signals)
    finite = scores.dropna()
    assert set(np.unique(finite.to_numpy())) <= {0.0, 0.5, 1.0}
    with pytest.raises(ValueError, match="tsmom_21 then tsmom_252"):
        ar.leg_scores(inputs, list(reversed(signals)))


def test_reentry_none_targets_are_monthly_fast_slow() -> None:
    entry = ar.source_entries(ROOT_DIR)["fast_slow"]
    inputs = DATA["qqq"].inputs
    scores = ar.leg_scores(inputs, entry.params["signals"])
    off = ar.asymmetric_targets(inputs, ar.h_a_params(entry, "none"))
    assert off.equals(ar.decide(scores, "monthly")["target"])
    on = ar.asymmetric_targets(inputs, ar.h_a_params(entry))
    assert on.equals(ar.decide(scores, "h_a")["target"])
    assert not on.equals(off)
    with pytest.raises(ValueError, match="reentry"):
        ar.asymmetric_targets(inputs, {"signals": entry.params["signals"], "reentry": "daily"})
    with pytest.raises(ValueError, match="reentry"):
        ar.asymmetric_targets(inputs, {"signals": entry.params["signals"]})


@pytest.mark.parametrize("schedule", ["daily", "week_end", "month_end"])
@pytest.mark.parametrize("band", [0.0, 0.2])
@pytest.mark.parametrize("delay", [0, 1])
def test_simulate_on_is_the_bench_simulate_day_by_day(
    schedule: str, band: float, delay: int
) -> None:
    returns = DATA["qqq"].returns
    rng = np.random.default_rng(7)
    steps = rng.choice([0.0, 0.3, 0.5, 0.8, 1.0, 1.6, 2.4], size=len(returns) // 15 + 1)
    targets = pd.Series(np.repeat(steps, 15)[: len(returns)], index=returns.index)
    rebalance = Rebalance(schedule=schedule, band=band)  # type: ignore[arg-type]
    kwargs: dict[str, Any] = {"start": date(2000, 1, 3), "end": date(2004, 12, 31), "cost_bps": 5.0}
    expected = simulate(returns, targets, rebalance=rebalance, delay_days=delay, **kwargs)
    found = ar.simulate_on(
        returns,
        targets,
        schedule_mask(pd.DatetimeIndex(returns.index), schedule),
        band=band,
        delay_days=delay,
        **kwargs,
    )
    assert found.start == expected.start
    assert found.daily.equals(expected.daily)


def test_simulate_on_refuses_what_simulate_refuses() -> None:
    returns = DATA["qqq"].returns
    targets = pd.Series(0.5, index=returns.index)
    days = [True] * len(returns)
    kwargs: dict[str, Any] = {"start": date(2000, 1, 3), "end": date(2001, 12, 31), "cost_bps": 5.0}
    with pytest.raises(ValueError, match="one entry per date"):
        ar.simulate_on(returns, targets, days[1:], **kwargs)
    with pytest.raises(ValueError, match="same dates"):
        ar.simulate_on(returns, targets.iloc[1:], days, **kwargs)
    with pytest.raises(ValueError, match="delay_days"):
        ar.simulate_on(returns, targets, days, delay_days=-1, **kwargs)
    with pytest.raises(ValueError, match="band"):
        ar.simulate_on(returns, targets, days, band=1.0, **kwargs)
    with pytest.raises(ValueError, match="no target is executable"):
        ar.simulate_on(returns, pd.Series(np.nan, index=returns.index), days, **kwargs)
    late = [False] * len(returns)
    with pytest.raises(ValueError, match="no target is executable"):
        ar.simulate_on(returns, targets, late, **kwargs)


def test_held_legs_are_the_last_trade_days_decision_two_days_on() -> None:
    decided = ar.decide(SCORES, "h_a")
    sim = SimpleNamespace(daily=pd.DataFrame(index=DAYS[DAYS >= _when("2001-02-02")]))
    legs = ar.held_legs(decided, sim)  # type: ignore[arg-type]
    assert list(legs.columns) == ["fast", "slow"]
    # 01-31 decides, 02-01 trades, 02-02 holds; the 02-09 re-entry is held from 02-13.
    assert tuple(legs.loc[_when("2001-02-02")]) == (0.0, 1.0)
    assert tuple(legs.loc[_when("2001-02-12")]) == (0.0, 1.0)
    assert tuple(legs.loc[_when("2001-02-13")]) == (1.0, 1.0)
    assert tuple(legs.loc[_when("2001-03-01")]) == (1.0, 1.0)
    assert tuple(legs.loc[_when("2001-03-02")]) == (0.0, 1.0)
    moves = pr.leg_moves(legs)
    assert [(m.date, m.leg, m.kind) for m in moves.itertuples()] == [
        (_when("2001-02-13"), "fast", "entry"),
        (_when("2001-03-02"), "fast", "exit"),
        (_when("2001-03-13"), "fast", "entry"),
    ]
    held_too_early = SimpleNamespace(daily=pd.DataFrame(index=DAYS[DAYS >= _when("2001-02-01")]))
    with pytest.raises(ValueError, match="holds no decided legs"):
        ar.held_legs(decided, held_too_early)  # type: ignore[arg-type]


def test_exit_counts_add_the_legs() -> None:
    legs = pd.DataFrame(
        {"fast": [1.0, 0.0, 1.0, 0.0, 0.0], "slow": [1.0, 1.0, 1.0, 0.0, 1.0]},
        index=pd.bdate_range("2001-01-29", periods=5),
    )
    moves = pr.leg_moves(legs)
    assert ar.exit_counts(moves) == {"fast_exits": 2, "slow_exits": 1, "exits": 3, "entries": 2}
    assert ar.exit_counts(moves, "2001-02", "2001-02") == {
        "fast_exits": 1,
        "slow_exits": 1,
        "exits": 2,
        "entries": 1,
    }


def _gains(scores: pd.DataFrame, asset: float = 0.01) -> pd.DataFrame:
    decided = ar.decide(scores, "monthly")
    returns = pd.Series(asset, index=DAYS)
    return ar.missed_gains(decided, returns, DAYS[DAYS >= _when("2001-02-01")])


def test_missed_gains_split_a_fast_stint_at_the_first_positive_check() -> None:
    gains = _gains(SCORES)
    half = 0.5 * math.log1p(0.01)
    # Out from 02-02 (the 01-31 decision); the fast score is 1 at the 02-09 check, so days held
    # before 02-13 are false exit and the rest of the stint is re-entry lag.
    false_days = gains.index[
        (gains.index >= _when("2001-02-02")) & (gains.index < _when("2001-02-13"))
    ]
    lag_days = gains.index[gains.index >= _when("2001-02-13")]
    assert gains.loc[false_days, "false_exit"].tolist() == pytest.approx([half] * len(false_days))
    assert (gains.loc[false_days, "reentry_lag"] == 0.0).all()
    assert gains.loc[lag_days, "reentry_lag"].tolist() == pytest.approx([half] * len(lag_days))
    assert (gains.loc[lag_days, "false_exit"] == 0.0).all()
    assert gains.loc[_when("2001-02-01"), ["false_exit", "reentry_lag", "slow"]].tolist() == [
        0.0,
        0.0,
        0.0,
    ]
    assert (gains["slow"] == 0.0).all()
    assert set(gains.loc[gains["stint"] >= 0, "stint"]) == {0}
    assert gains.loc[_when("2001-02-01"), "stint"] == -1


def test_a_comeback_at_the_month_end_itself_has_no_lag() -> None:
    scores = _scores(
        [("2001-01-01", 1.0), ("2001-01-31", 0.0), ("2001-02-28", 1.0)], [("2001-01-01", 1.0)]
    )
    gains = _gains(scores)
    assert (gains["reentry_lag"] == 0.0).all()
    out = gains.loc[_when("2001-02-02") : _when("2001-03-01")]
    assert (out["false_exit"] > 0).all()
    assert (gains.loc[_when("2001-03-02") :, "false_exit"] == 0.0).all()


def test_the_slow_leg_is_kept_apart_and_parts_are_signed() -> None:
    scores = _scores([("2001-01-01", 1.0)], [("2001-01-01", 1.0), ("2001-01-31", 0.0)])
    gains = _gains(scores, asset=-0.02)
    out = gains.loc[_when("2001-02-02") :]
    assert out["slow"].tolist() == pytest.approx([0.5 * math.log1p(-0.02)] * len(out))
    assert (gains[["false_exit", "reentry_lag"]] == 0.0).all().all()
    assert (gains["stint"] == -1).all()


def _bull(sid: str, start: str, end: str) -> EpisodeSpec:
    return EpisodeSpec(id=sid, start=start, end=end)


def test_split_rows_sum_by_month_and_share_is_held_to_zero_one() -> None:
    index = pd.bdate_range("2001-01-01", "2001-06-29")
    months = index.month
    gains = pd.DataFrame(
        {
            "false_exit": np.where(months <= 2, 0.01, 0.0),
            "reentry_lag": np.where(months == 3, 0.02, np.where(months == 5, -0.01, 0.0)),
            "slow": np.where(months == 4, 0.005, 0.0),
            "stint": np.where(months <= 3, 0, np.where(months == 5, 1, -1)),
        },
        index=index,
    )
    bulls = [_bull("long_bull_a", "2001-01", "2001-04"), _bull("long_bull_b", "2001-05", "2001-06")]
    rows = ar.split_rows(gains, bulls)
    first, second = rows
    n = {m: int((months == m).sum()) for m in range(1, 7)}
    assert first["false_exit"] == pytest.approx(0.01 * (n[1] + n[2]))
    assert first["reentry_lag"] == pytest.approx(0.02 * n[3])
    assert first["slow"] == pytest.approx(0.005 * n[4])
    assert first["stints"] == 1
    total = first["false_exit"] + first["reentry_lag"] + first["slow"]
    assert first["share"] == pytest.approx(first["reentry_lag"] / total)
    assert first["share_fast_only"] == pytest.approx(
        first["reentry_lag"] / (first["false_exit"] + first["reentry_lag"])
    )
    # A negative lag with nothing else: no cost, the share is undefined.
    assert (
        second["reentry_lag"] < 0 and second["share"] is None and second["share_fast_only"] is None
    )
    both = ar.pooled(rows)
    assert both["id"] == "合计"
    assert both["stints"] == 2
    assert both["reentry_lag"] == pytest.approx(first["reentry_lag"] + second["reentry_lag"])
    assert set(first) >= {"id", "start", "end"}


@pytest.mark.parametrize(
    ("numerator", "denominator", "share"),
    [
        (0.5, 1.0, 0.5),
        (-0.1, 1.0, 0.0),
        (0.0, 1.0, 0.0),
        (1.5, 1.0, 1.0),
        (0.3, 0.0, None),
        (0.3, -0.2, None),
    ],
)
def test_the_share_bounds(numerator: float, denominator: float, share: float | None) -> None:
    assert ar.lag_share(numerator, denominator) == share


@pytest.mark.parametrize(
    ("share", "probability"),
    [(None, 0.5), (1.0, 0.6), (0.5, 0.6), (0.4999, 0.45), (0.3, 0.45), (0.2999, 0.3), (0.0, 0.3)],
)
def test_row_3_follows_the_charter_steps(share: float | None, probability: float) -> None:
    assert ar.row3_probability(share) == probability


def test_the_decision_table_is_the_policy_table_as_printed() -> None:
    us = ar.DECISION_TABLE["us_market"]
    assert {k: round(v * 100, 1) for k, v in us["slow_bear"].items()} == {
        "slow_bear_192908": 27.8,
        "slow_bear_193208": 18.6,
        "slow_bear_193702": 9.7,
        "slow_bear_193909": 4.5,
        "slow_bear_194605": 2.6,
        "slow_bear_196112": 6.5,
        "slow_bear_196811": 11.8,
        "slow_bear_197212": 28.7,
        "slow_bear_200003": 12.2,
        "slow_bear_200710": 15.3,
    }
    assert us["summary"] == {
        "count": 10,
        "median_difference": 0.120,
        "positive": 10,
        "min_difference": 0.026,
    }
    assert us["long_bull"] == {}
    qqq = ar.DECISION_TABLE["qqq"]
    assert qqq["slow_bear"] == {"slow_bear_200003": 0.119, "slow_bear_200710": 0.066}
    assert qqq["summary"] == {
        "count": 2,
        "median_difference": 0.092,
        "positive": 2,
        "min_difference": 0.066,
    }
    assert qqq["long_bull"] == {"long_bull_200210": -0.033}
    assert ar.DECISION_TOLERANCE == pytest.approx(0.0005, abs=1e-11)


def _table_row(market: str, shift: float = 0.0) -> dict[str, Any]:
    table = ar.DECISION_TABLE[market]
    return {
        "status": "ok",
        "segments": {
            "slow_bear": [
                {"id": k, "difference": v + shift} for k, v in table["slow_bear"].items()
            ],
            "slow_bear_summary": {
                "count": table["summary"]["count"],
                "positive": table["summary"]["positive"],
                "median_difference": table["summary"]["median_difference"] + shift,
                "min_difference": table["summary"]["min_difference"] + shift,
            },
            "long_bull": [
                {"id": k, "annual_difference": v + shift} for k, v in table["long_bull"].items()
            ],
        },
    }


@pytest.mark.parametrize("market", ar.MARKETS)
def test_decision_differences_pass_within_half_a_printed_digit(market: str) -> None:
    assert ar.decision_differences(_table_row(market), market) == []
    assert ar.decision_differences(_table_row(market, 0.0005), market) == []
    assert ar.decision_differences(_table_row(market, -0.0005), market) == []
    off = ar.decision_differences(_table_row(market, 0.000501), market)
    assert f"{next(iter(ar.DECISION_TABLE[market]['slow_bear']))} D" in off
    assert "慢熊 median_difference" in off and "慢熊 min_difference" in off
    if market == "qqq":
        assert "long_bull_200210 年化差" in off


def test_decision_differences_name_counts_gaps_and_failures() -> None:
    row = _table_row("qqq")
    row["segments"]["slow_bear"] = row["segments"]["slow_bear"][:1]
    row["segments"]["slow_bear_summary"]["count"] = 1
    row["segments"]["slow_bear_summary"]["positive"] = 1
    row["segments"]["long_bull"] = [{"id": "long_bull_200210", "annual_difference": None}]
    assert ar.decision_differences(row, "qqq") == [
        "slow_bear_200710 D",
        "慢熊段数",
        "D > 0 段数",
        "long_bull_200210 年化差",
    ]
    assert ar.decision_differences({"status": "error"}, "qqq") == ["没有算出"]
    custom = {"qqq": {**ar.DECISION_TABLE["qqq"], "long_bull": {"long_bull_200210": 0.5}}}
    assert ar.decision_differences(_table_row("qqq"), "qqq", custom) == ["long_bull_200210 年化差"]


def _rows_input(
    median: float, positive: int, qqq_d: list[float], cell: float, geometric: float, extra: int
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    def strategy(value: float) -> dict[str, Any]:
        return {"geometric_annual_return": value}

    h_a = {
        "us_market": {
            "segments": {
                "slow_bear_summary": {
                    "median_difference": median,
                    "positive": positive,
                    "count": 10,
                }
            },
            "strategy": strategy(0.05 + geometric),
        },
        "qqq": {
            "segments": {
                "slow_bear": [{"id": f"s{k}", "difference": d} for k, d in enumerate(qqq_d)],
                "long_bull": [{"id": ar.QQQ_BULL, "annual_difference": cell}],
            }
        },
    }
    bench = {
        "us_market": {"fast_slow": {"strategy": strategy(0.05)}},
        "qqq": {
            "fast_slow": {
                "segments": {"long_bull": [{"id": ar.QQQ_BULL, "annual_difference": -0.04}]}
            }
        },
    }
    exits = {
        "us_market": {"extra": extra, "fast_slow": {"exits": 10}},
        "qqq": {"extra": 1, "fast_slow": {"exits": 4}},
    }
    return h_a, bench, exits


def test_the_six_rows_at_their_boundaries() -> None:
    rows = ar.row_readings(*_rows_input(0.08, 8, [0.01, 0.02], -0.02, 0.0, 5))
    assert [r["row"] for r in rows] == [1, 2, 3, 4, 5, 6]
    assert [r["holds"] for r in rows] == [True, True, True, True, None, True]
    assert rows[2]["threshold"] == pytest.approx(-0.02)
    assert rows[0]["reading"] == {"median_difference": 0.08, "positive": 8, "count": 10}
    assert rows[5]["reading"]["us_market"] == {"extra": 5, "fast_slow_exits": 10}
    assert ar.first_four(rows) is True
    rows = ar.row_readings(*_rows_input(0.0799, 8, [0.01, 0.0], -0.0201, -0.0001, 6))
    assert [r["holds"] for r in rows] == [False, False, False, False, None, False]
    assert ar.first_four(rows) is False
    rows = ar.row_readings(*_rows_input(0.09, 7, [0.01, 0.02], -0.01, 0.01, 0))
    assert rows[0]["holds"] is False
    assert (
        ar.first_four(
            [{**r, "holds": True} if r["row"] != 4 else {**r, "holds": False} for r in rows]
        )
        is False
    )
    assert ar.first_four([{**r, "holds": r["row"] in (1, 2, 3, 4)} for r in rows]) is True
    assert ar.first_four([]) is None


# ---------------------------------------------------------------- whole runs on synthetic data


@pytest.fixture(scope="module")
def evaluated() -> Iterator[dict[tuple[str, str], dict[str, Any]]]:
    """``policy_table.evaluate`` once per market and row; the probe calls it again and again."""

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
def reference(same_run: dict[str, Any], tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    out = tmp_path_factory.mktemp("reference")
    result = ar.run_reference(
        DATA, LOADED, GATE, ROOT_DIR, provenance=PROVENANCE, out_dir=out, decision_table=same_run
    )
    return {"result": result, "out": out}


@pytest.fixture(scope="module")
def probe(
    same_run: dict[str, Any], reference: dict[str, Any], tmp_path_factory: pytest.TempPathFactory
) -> dict[str, Any]:
    out = tmp_path_factory.mktemp("run")
    result = ar.run_probe(
        DATA,
        LOADED,
        GATE,
        ROOT_DIR,
        reference=reference["result"],
        provenance=PROVENANCE,
        out_dir=out,
        decision_table=same_run,
    )
    return {"result": result, "out": out}


def test_the_real_decision_table_stops_both_steps_on_synthetic_data(
    evaluated: dict[tuple[str, str], dict[str, Any]], tmp_path: Path
) -> None:
    result = ar.run_reference(DATA, LOADED, GATE, ROOT_DIR, provenance=PROVENANCE, out_dir=tmp_path)
    assert result["stopped"] and not result["complete"]
    assert result["stop"]["us_market"] and result["stop"]["qqq"]
    assert "split" not in result and "figures" not in result
    assert not (tmp_path / ar.FIGURES).exists()
    text = ar.render_reference(result)
    assert "同一次运行的 fast_slow 和决定表不同（美股大盘 1929–2010）" in text
    assert "按章程第 3.1 节停下" in text


def test_the_reference_reproduces_the_bench_and_splits_the_long_bulls(
    reference: dict[str, Any],
) -> None:
    result = reference["result"]
    assert not result["stopped"] and result["complete"]
    assert result["stop"] == {"us_market": [], "qqq": []}
    assert result["equivalence"] == {
        market: {"fast_slow": True, "fast_slow_weekly": True} for market in ar.MARKETS
    }
    assert set(result["rules"]["qqq"]) == {"fast_slow", "fast_slow_weekly"}
    assert [r["id"] for r in result["split"]["qqq"]["rows"]] == [ar.QQQ_BULL]
    assert len(result["split"]["us_market"]["rows"]) == 7
    for split in result["split"].values():
        for row in [*split["rows"], split["pooled"]]:
            assert row["share"] is None or 0.0 <= row["share"] <= 1.0
    cell = result["split"]["qqq"]["rows"][0]
    row3 = result["row3"]
    assert row3["share"] == cell["share"]
    assert row3["probability"] == ar.row3_probability(cell["share"])
    assert row3["kept"] == (cell["share"] is None)
    assert row3["reason"] == (ar.ROW3_NO_SHARE if cell["share"] is None else None)
    assert result["production_effect"] == "none" and result["broker_action"] == "none"
    assert result["run_id"] == ar.RUN_ID and result["step"] == "reference"
    assert result["owner_decision"] == ar.OWNER_DECISION and result["direction"] == ar.DIRECTION


def test_the_reference_draws_monthly_against_weekly(reference: dict[str, Any]) -> None:
    result, out = reference["result"], reference["out"]
    names = [f["name"] for market in ar.MARKETS for f in result["figures"][market]]
    assert names == [f"reference-{m}-{p}.svg" for m in ar.MARKETS for p in "ABC"]
    for market in ar.MARKETS:
        first = result["figures"][market][0]
        moves = result["moves"][market]
        for role, name in (("base", "fast_slow"), ("compared", "fast_slow_weekly")):
            window = first["counts"]["window"][role]
            assert window["fast"]["exit"] == moves[name]["fast_exits"]
            assert window["slow"]["exit"] == moves[name]["slow_exits"]
            assert window["fast"]["entry"] + window["slow"]["entry"] == moves[name]["entries"]
    figure = (out / ar.FIGURES / "reference-qqq-A.svg").read_text(encoding="utf-8")
    assert "月频 fast_slow" in figure and "周频 fast_slow" in figure


def test_the_run_reads_six_rows_and_prints_the_gate(
    reference: dict[str, Any], probe: dict[str, Any]
) -> None:
    result = probe["result"]
    assert not result["stopped"] and result["complete"]
    assert result["checks"] == {"us_market": [], "qqq": []}
    assert result["equivalence"] == {
        market: {"fast_slow": True, "h_a_without_reentry": True} for market in ar.MARKETS
    }
    rows = {r["row"]: r for r in result["readings"]}
    assert sorted(rows) == [1, 2, 3, 4, 5, 6]
    us = result["h_a"]["us_market"]["segments"]["slow_bear_summary"]
    assert rows[1]["holds"] == (us["median_difference"] >= 0.08 and us["positive"] >= 8)
    base = next(
        s["annual_difference"]
        for s in result["rules"]["qqq"]["fast_slow"]["segments"]["long_bull"]
        if s["id"] == ar.QQQ_BULL
    )
    assert rows[3]["threshold"] == pytest.approx(base * 0.5)
    geometric = (
        result["h_a"]["us_market"]["strategy"]["geometric_annual_return"]
        - result["rules"]["us_market"]["fast_slow"]["strategy"]["geometric_annual_return"]
    )
    assert rows[4]["reading"]["geometric_difference"] == pytest.approx(geometric)
    assert rows[5]["holds"] is None
    assert rows[6]["holds"] == all(
        result["exits"][m]["extra"] <= result["exits"][m]["fast_slow"]["exits"] / 2
        for m in ar.MARKETS
    )
    assert result["first_four"] == all(rows[n]["holds"] for n in (1, 2, 3, 4))
    assert [len(result["h_a"][m]["gate"]) for m in ar.MARKETS] == [5, 2]
    assert result["reference_row3"] == reference["result"]["row3"]


def test_h_a_only_adds_exits_and_counts_its_reentries(probe: dict[str, Any]) -> None:
    exits = probe["result"]["exits"]
    assert exits["us_market"]["span"] == "window" and exits["qqq"]["span"] == ar.QQQ_BULL
    for market in ar.MARKETS:
        found = exits[market]
        assert found["extra"] == found["h_a"]["exits"] - found["fast_slow"]["exits"] >= 0
        assert found["h_a"]["slow_exits"] == found["fast_slow"]["slow_exits"]
        assert found["reentries"] >= found["extra"]


def test_h_a_matches_the_bench_strategy_and_trades_one_day_before_each_leg_move(
    reference: dict[str, Any],
) -> None:
    entries = ar.source_entries(ROOT_DIR)
    for market in ar.MARKETS:
        data = DATA[market]
        window = ar.market_window(LOADED, GATE, market)
        scores = ar.leg_scores(data.inputs, entries["fast_slow"].params["signals"])
        path = ar.probe_path("h_a", data, scores, window, LOADED)
        targets = ar.asymmetric_targets(data.inputs, ar.h_a_params(entries["fast_slow"]))
        assert path.decided["target"].equals(targets)
        turnover = path.sim.daily["turnover"]
        positions = turnover.index.get_indexer(pd.DatetimeIndex(path.moves["date"]))
        assert (positions >= 1).all()
        assert (turnover.iloc[positions - 1].to_numpy() > 0).all()
        assert ar.reentry_count(path.decided, window) == int(
            path.decided["reentry"].to_numpy()[ar.in_window(path.decided.index, window)].sum()
        )


def test_the_run_draws_monthly_against_h_a_with_matched_ratios(probe: dict[str, Any]) -> None:
    result, out = probe["result"], probe["out"]
    for market in ar.MARKETS:
        records = result["figures"][market]
        assert [r["name"] for r in records] == [f"run-{market}-{p}.svg" for p in "ABC"]
        for record in records:
            path = out / ar.FIGURES / record["name"]
            assert path.stat().st_size == record["bytes"]
        panels = records[1]["counts"]["panels"]
        assert len(panels) == len(result["lags"][market]["h_a"])
        for sid, panel in panels.items():
            lag = result["lags"][market]["h_a"][sid]
            assert panel["in_segment"]["compared"]["fast"] == {
                "exit": lag["fast_exit"],
                "entry": lag["fast_entry"],
            }
    text = (out / ar.FIGURES / "run-qqq-C.svg").read_text(encoding="utf-8")
    assert "同平均敞口固定敞口的累计" in text
    bear = result["rules"]["qqq"]["fast_slow"]["segments"]["slow_bear"][0]
    assert f"{bear['difference'] * 100:+.1f}" in text


def test_the_cost_line_and_the_figure_ratios_use_the_matched_fixed_exposure(
    probe: dict[str, Any],
) -> None:
    result = probe["result"]
    entries = ar.source_entries(ROOT_DIR)
    for market in ar.MARKETS:
        data = DATA[market]
        window = ar.market_window(LOADED, GATE, market)
        scores = ar.leg_scores(data.inputs, entries["fast_slow"].params["signals"])
        ratios = result["figures"][market][2]["counts"]["final_ratio"]
        for role, name in (("base", "fast_slow"), ("compared", "h_a")):
            path = ar.probe_path(name, data, scores, window, LOADED)
            matched = ar.matched_simulation(data, path.sim, window, LOADED.cost_bps)
            expected = (1 + path.sim.returns).prod() / (1 + matched.returns).prod()
            assert ratios[role] == pytest.approx(expected, rel=1e-12)
        path = ar.probe_path("h_a", data, scores, window, LOADED)
        own = ar.simulate_on(
            data.returns,
            path.decided["target"],
            path.decided["trade"].to_numpy(dtype=bool),
            start=window.start,
            end=window.end,
            cost_bps=25.0,
        )
        cost = result["h_a"][market]["cost"]
        assert cost["bps"] == 25.0
        assert cost["strategy"] == pytest.approx(
            summarize(own, data.returns["cash"], LOADED)["geometric_annual_return"], rel=1e-12
        )


def test_the_run_refuses_a_stopped_or_foreign_reference(reference: dict[str, Any]) -> None:
    good = reference["result"]
    for bad, message in (
        ({**good, "run_id": "TRADING-2565-42"}, "not a"),
        ({**good, "step": "run"}, "not a"),
        ({**good, "stopped": True}, "stopped or is incomplete"),
        ({**good, "complete": False}, "stopped or is incomplete"),
    ):
        with pytest.raises(ValueError, match=message):
            ar.run_probe(
                DATA,
                LOADED,
                GATE,
                ROOT_DIR,
                reference=bad,
                provenance=PROVENANCE,
                out_dir=Path("unused"),
            )


def test_the_h_a_checks_catch_a_lookahead(monkeypatch: pytest.MonkeyPatch) -> None:
    entry = ar.source_entries(ROOT_DIR)["fast_slow"]
    data = DATA["qqq"]
    window = ar.market_window(LOADED, GATE, "qqq")
    assert ar.h_a_checks(data, entry, window) == []
    honest = ar.decide

    def peeking(scores: pd.DataFrame, mode: ar.Mode) -> pd.DataFrame:
        return honest(scores.shift(-5), mode)

    monkeypatch.setattr(ar, "decide", peeking)
    assert ar.h_a_checks(data, entry, window)


def test_the_results_are_plain_json(
    reference: dict[str, Any], probe: dict[str, Any], tmp_path: Path
) -> None:
    for name, result in (("reference.json", reference["result"]), ("h_a.json", probe["result"])):
        ar.write_json(result, tmp_path / name)
        assert json.loads((tmp_path / name).read_text(encoding="utf-8"))["run_id"] == ar.RUN_ID


def test_the_reports_carry_the_key_lines_and_no_data_rows(
    reference: dict[str, Any], probe: dict[str, Any]
) -> None:
    ref_text = ar.render_reference(reference["result"])
    assert "同一次运行的 fast_slow 和决定表（TRADING-2565-42）相同" in ref_text
    assert "等同核对（美股大盘 1929–2010）：探针的腿和成交日逐日复现评估台" in ref_text
    assert "- 第 3 行按份额应写 " in ref_text
    assert "份额截在 0 到 1" in ref_text
    run_text = ar.render_run(probe["result"])
    assert "| 行 | 读数 | 门槛 | 按判法 |" in run_text
    assert "第 1 到 4 行都成立：" in run_text
    assert "只印，不判，H-A 不走试跑门" in run_text
    assert "| 5 | 不读 | 不读（章程第 8 节） | — |" in run_text
    for text in (ref_text, run_text):
        assert "production_effect=none；broker_action=none。" in text
        tables = [line for line in text.splitlines() if line.startswith("| ")]
        assert len(tables) < 120
        assert not any(line.startswith(("| 19", "| 20")) for line in tables)


def test_the_row_3_line_says_keep_without_a_share(reference: dict[str, Any]) -> None:
    result = reference["result"]
    split = result["split"]
    cell = {**split["qqq"]["rows"][0], "share": None, "share_fast_only": None}
    kept = {
        **result,
        "split": {**split, "qqq": {"rows": [cell], "pooled": split["qqq"]["pooled"]}},
        "row3": {
            **result["row3"],
            "share": None,
            "share_fast_only": None,
            "probability": 0.5,
            "kept": True,
            "reason": ar.ROW3_NO_SHARE,
        },
    }
    lines = ar.render_reference(kept).splitlines()
    assert (
        "- 第 3 行按份额应写 保持 0.5（reference 分母不为正）；复盘线程照抄进记分表以后才 `run`。"
        in lines
    )
    assert any(
        line.startswith("| QQQ 1999–2010 | long_bull_200210 |") and "没有代价，份额不定义" in line
        for line in lines
    )
    given = {
        **result,
        "row3": {
            **result["row3"],
            "share": 0.42,
            "probability": 0.45,
            "kept": False,
            "reason": None,
        },
    }
    assert (
        "- 第 3 行按份额应写 0.45（≥ 0.5 写 0.6，0.3–0.5 写 0.45，< 0.3 写 0.3）；"
        "复盘线程照抄进记分表以后才 `run`。" in (ar.render_reference(given).splitlines())
    )


def test_the_commands_refuse_existing_results_and_a_bad_reference(tmp_path: Path) -> None:
    runner = CliRunner()
    (tmp_path / "old").mkdir()
    (tmp_path / "old" / ar.REFERENCE_JSON).write_text("{}", encoding="utf-8")
    found = runner.invoke(ar.app, ["reference", "--out", str(tmp_path / "old")])
    assert found.exit_code == 1 and "拒绝覆盖" in found.output
    (tmp_path / "figs" / ar.FIGURES).mkdir(parents=True)
    found = runner.invoke(
        ar.app, ["run", "--out", str(tmp_path / "figs"), "--reference", str(tmp_path / "x.json")]
    )
    assert found.exit_code == 1 and "拒绝覆盖" in found.output
    found = runner.invoke(ar.app, ["run", "--out", str(tmp_path / "new")])
    assert found.exit_code != 0
    (tmp_path / "stopped.json").write_text(
        json.dumps({"run_id": ar.RUN_ID, "step": "reference", "stopped": True}), encoding="utf-8"
    )
    found = runner.invoke(
        ar.app,
        ["run", "--out", str(tmp_path / "new"), "--reference", str(tmp_path / "stopped.json")],
    )
    assert found.exit_code == 1 and "reference 不能用" in found.output
    (tmp_path / "list.json").write_text("[]", encoding="utf-8")
    found = runner.invoke(
        ar.app, ["run", "--out", str(tmp_path / "new"), "--reference", str(tmp_path / "list.json")]
    )
    assert found.exit_code == 1 and "reference 不能用" in found.output
    assert not (tmp_path / "new").exists()


def test_check_rows_pass_on_synthetic_data() -> None:
    rows = ar.check_rows(DATA, LOADED, GATE, ROOT_DIR)
    assert [r["market"] for r in rows] == list(ar.MARKETS)
    for row in rows:
        assert row["ok"], row
        assert row["missing_days"] == 0
        assert row["equivalence"] == {
            "fast_slow": True,
            "fast_slow_weekly": True,
            "h_a_without_reentry": True,
        }


def test_the_module_reads_no_research_record_or_validation_window() -> None:
    source = Path(ar.__file__).read_text(encoding="utf-8")
    for forbidden in ("window_reads", "runs/", "_screens", "data/raw", "validation_"):
        assert forbidden not in source


def _synthetic_load(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ar, "load_inputs", lambda root: (DATA, GATE, LOADED, ROOT_DIR))
    monkeypatch.setattr(ar, "git_provenance", lambda root: PROVENANCE)


def test_the_check_command_prints_one_line_per_market(monkeypatch: pytest.MonkeyPatch) -> None:
    _synthetic_load(monkeypatch)
    found = CliRunner().invoke(ar.app, ["check"])
    assert found.exit_code == 0, found.output
    lines = [line for line in found.output.splitlines() if line.startswith("asymmetric_reentry:")]
    assert len(lines) == 2
    assert all(line.endswith("可以") for line in lines)
    assert "fast_slow 同、fast_slow_weekly 同、h_a_without_reentry 同" in lines[0]


def test_the_reference_command_writes_its_files_and_stops_on_the_real_table(
    evaluated: dict[tuple[str, str], dict[str, Any]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _synthetic_load(monkeypatch)
    found = CliRunner().invoke(ar.app, ["reference", "--out", str(tmp_path / "ref")])
    assert found.exit_code == 1
    assert "同一次运行的 fast_slow 和决定表不同，停下" in found.output
    written = json.loads((tmp_path / "ref" / ar.REFERENCE_JSON).read_text(encoding="utf-8"))
    assert written["stopped"] is True and written["provenance"] == PROVENANCE
    assert (
        (tmp_path / "ref" / ar.REFERENCE_MD)
        .read_text(encoding="utf-8")
        .startswith("# H-A 的 reference")
    )
    found = CliRunner().invoke(
        ar.app,
        [
            "run",
            "--out",
            str(tmp_path / "run"),
            "--reference",
            str(tmp_path / "ref" / ar.REFERENCE_JSON),
        ],
    )
    assert found.exit_code == 1 and "reference 不能用" in found.output
