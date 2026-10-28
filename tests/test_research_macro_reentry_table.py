"""Acceptance tests for the macro re-entry table (TRADING-2565-86).

owner_decision:TRADING-2565:2026-10-29:print_only_tables, charter p6 sec. 23, the second of four
print-only tables.
F0001's slow-leg exits on the US market, 1929-2010, are listed one by one; for each, the table asks
what the leg would have made had it come back at the first month end on which the Baa - Aaa spread
had fallen 20% from its high since the exit (A) or the CAPE percentile since 1871 was at most 0.5
(B), before F0001 came back by itself, net of the window's mean monthly excess for the extra months,
and against waiting the same number of months without looking at either. Not a probe, not a trial,
no ledger, no new rule's position line, not a long-bull cost reading.

Synthetic data only: no snapshot is read. The fake signal snapshot stands in for sig-ed9e988ffe7c;
the QQQ-Q3 preregistration, the trial gate and the bench's signals are the repository's own files.
runup_state_table, state_table, policy_table and cycle_power are imported, not changed.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench.sources import SourceSpec
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_data.signal_sources import SignalSnapshot
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import macro_reentry_table as mt
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import runup_state_table as rs
from ai_trading_system.research_probes import state_table as st

SOURCE = Path(mt.__file__).read_text(encoding="utf-8")
PREREG = Path(__file__).resolve().parents[1] / "research" / "QQQ-Q3" / "prereg.yaml"
UNCHANGED = {
    rs: "610c76cc5ae34a92b4f1aa3f04614dc4321c8394be3567613941421b219e3c97",
    st: "0a39bc86e58495a3af598c7f73abc0da6c988015d26be649115f56126eca8a22",
    pt: "2a8b550855ebc43b0ccaf7c81f49efa46071b7a518027455a568813744b5a6ca",
    cp: "53e5286fec515c99dcb264f4ce8959afb4c4c54d970ab24b2df5d4c3843efafc",
}
FIRST_HALF, SECOND_HALF = "1929–1969", "1970–2010"


def P(text: str) -> Any:
    """A month, typed loosely so it can index the frames below."""

    return pd.Period(text, freq="M")


# ----- hand-built months: the slow leg, the two macro series and the market are set per test
DAYS = pd.bdate_range("1928-11-01", "2010-12-31")
MONTH_OF = DAYS.to_period("M")


def _last_day(month: str) -> pd.Timestamp:
    return pd.Timestamp(DAYS[MONTH_OF == P(month)].max())


def _span(first: str, last: str) -> list[str]:
    return [str(p) for p in pd.period_range(first, last, freq="M")]


def _targets(out: Iterable[str]) -> pd.DataFrame:
    """Month-end rows like month_targets: the slow leg is 0 in the listed months, 1 elsewhere."""

    outs = set(out)
    last = pd.Series(np.arange(len(DAYS)), index=DAYS).groupby(MONTH_OF).tail(1)
    index = pd.DatetimeIndex(last.index).to_period("M")
    slow = np.array([0.0 if str(p) in outs else 1.0 for p in index])
    return pd.DataFrame(
        {
            "date": pd.DatetimeIndex(last.index),
            "pos": last.to_numpy(),
            "f0001": (1.0 + slow) / 2.0,
            "fast": 1.0,
            "slow": slow,
        },
        index=index,
    )


def _states(
    spread: Mapping[str, float] | None = None, cape: Mapping[str, float] | None = None
) -> pd.DataFrame:
    """Month-end macro states like macro_states: spread 1.0 and CAPE percentile 0.9 by default."""

    frame = pd.DataFrame({"spread_value": 1.0, "cape_pct": 0.9}, index=st.months())
    for month, value in (spread or {}).items():
        frame.loc[P(month), "spread_value"] = value
    for month, value in (cape or {}).items():
        frame.loc[P(month), "cape_pct"] = value
    return frame


def _returns(
    months: Mapping[str, float] | None = None,
    days: Mapping[str, float] | None = None,
    base: float = 0.0,
) -> pd.Series:
    """Daily market returns: base, every day of a listed month, or a listed day."""

    values = pd.Series(base, index=DAYS, dtype=float)
    for month, value in (months or {}).items():
        values[MONTH_OF == P(month)] = value
    for day, value in (days or {}).items():
        values[DAYS == pd.Timestamp(day)] = value
    return values


def _built(
    out: Iterable[str],
    *,
    spread: Mapping[str, float] | None = None,
    cape: Mapping[str, float] | None = None,
    returns: pd.Series | None = None,
    cash: float = 0.0,
    segments: Sequence[tuple[str, str, str]] = (),
) -> mt.Built:
    return mt.assemble(
        _targets(out),
        returns if returns is not None else _returns(),
        pd.Series(cash, index=DAYS),
        _states(spread, cape),
        tuple((name, P(start), P(end)) for name, start, end in segments),
        [],
    )


def _rows(built: mt.Built) -> list[dict[str, Any]]:
    timing = mt.timing_rows(built)
    return mt.priced_rows(built, timing, mt.control_ks(timing))


def _row(rows: Sequence[dict[str, Any]], month: str) -> dict[str, Any]:
    return next(row for row in rows if row["month"] == month)


def _excess(returns: pd.Series, cash: float, start: str, end: str) -> float:
    """Sum of daily log excess returns after start's month end, up to end's month end."""

    mask = (DAYS > _last_day(start)) & (DAYS <= _last_day(end))
    return float((np.log1p(returns[mask]) - math.log1p(cash)).sum())


# ----- a whole snapshot: daily market factors with planted bears, five monthly macro series
def _frames(seed: int = 3) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    days = pd.bdate_range("1926-07-01", "2012-06-29")
    months = days.to_period("M")
    drift = np.full(len(days), 0.0004)
    for first, length in (
        ("1929-09", 33),
        ("1937-03", 13),
        ("1973-01", 21),
        ("2000-09", 30),
        ("2007-10", 17),
    ):
        start = P(first)
        drift[(months >= start) & (months < start + length)] = -0.0025
    market = drift + rng.normal(0.0, 0.01, len(days))
    factors = pd.DataFrame({"Mkt-RF": market - 0.0001, "SMB": 0.0, "RF": 0.0001}, index=days)

    def monthly(start: str, base: float, step: float, vol: float) -> pd.DataFrame:
        dates = pd.date_range(start, "2012-06-01", freq="MS")
        values = base * np.exp(np.cumsum(rng.normal(step, vol, len(dates))))
        return pd.DataFrame({"value": values}, index=dates)

    baa = monthly("1919-01-01", 6.0, 0.0, 0.02)
    aaa = baa.copy()
    aaa["value"] = baa["value"] - np.abs(rng.normal(1.0, 0.5, len(baa)))
    return {
        cp.FACTORS: factors,
        st.PRICE: monthly("1871-01-01", 5.0, 0.003, 0.04),
        st.EARNINGS: monthly("1871-01-01", 0.4, 0.002, 0.02),
        st.CPI: monthly("1871-01-01", 12.0, 0.002, 0.004),
        st.BAA: baa,
        st.AAA: aaa,
    }


def _spec(
    source: str,
    provider: Literal["fred", "ken_french", "shiller"],
    dataset: str,
    lag: int = 0,
    release: int = 0,
) -> SourceSpec:
    return SourceSpec(
        source_id=source,
        provider=provider,
        dataset=dataset,
        frequency="daily" if provider == "ken_french" else "monthly",
        known_lag_trading_days=lag,
        release_lag_months=release,
        description=source,
    )


SPECS = (
    _spec(cp.FACTORS, "ken_french", "F-F_Research_Data_Factors_daily"),
    _spec(st.PRICE, "shiller", "P"),
    _spec(st.EARNINGS, "shiller", "E", release=15),
    _spec(st.CPI, "shiller", "CPI", release=15),
    _spec(st.BAA, "fred", "BAA", lag=5),
    _spec(st.AAA, "fred", "AAA", lag=5),
)


def _snapshot(
    frames: Mapping[str, pd.DataFrame], specs: Sequence[SourceSpec] = SPECS
) -> SignalSnapshot:
    return SignalSnapshot(mt.SNAPSHOT_ID, dict(frames), tuple(specs))


def _inputs(frames: Mapping[str, pd.DataFrame]) -> mt.Inputs:
    snapshot = _snapshot(frames)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
        return mt.load_inputs()


FRAMES = _frames()
BASE = _inputs(FRAMES)
BUILT = mt.build(BASE)
RESULT, EXITS = mt.read(BASE)


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [text for item in value.values() for text in _strings(item)]
    if isinstance(value, list):
        return [text for item in value for text in _strings(item)]
    return []


# ----- constants: charter p6 sec. 23.2 and the requirement's sections 3 and 4, fixed before the run
def test_the_design_is_the_one_fixed_before_the_run() -> None:
    assert mt.RUN_ID == "TRADING-2565-86"
    assert mt.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-29:print_only_tables"
    assert mt.SNAPSHOT_ID == st.SNAPSHOT_ID == "sig-ed9e988ffe7c"
    assert st.PLAN_ID == "T0008"
    assert mt.FACTORS == "ff_factors_daily"
    assert (mt.FIRST_MONTH, mt.LAST_MONTH) == (P("1929-01"), P("2010-12"))
    assert mt.QUESTION == "QQQ-Q3"
    assert mt.RULES == {"f0001": "fast_slow", "fast": "fast_only", "slow": "slow_only"}
    assert (mt.LEG, mt.LEG_SHARE) == ("slow", 0.5)
    assert mt.CONDITIONS == {"spread": ("spread", 0.20), "cape": ("cape", 0.5)}
    assert mt.SENSITIVITY == {
        "spread_10": ("spread", 0.10),
        "spread_30": ("spread", 0.30),
        "cape_30": ("cape", 0.3),
        "cape_70": ("cape", 0.7),
    }
    assert mt.EITHER == "either"
    assert mt.DIP == -0.20
    assert (mt.MIN_EXITS, mt.MIN_EARLY, mt.HALF_MIN) == (20, 6, 3)
    assert (mt.SHARE_LINE, mt.DIP_PART, mt.FILTER_SHARE) == (0.25, 3, 0.5)
    assert mt.HALVES == ((FIRST_HALF, 1929, 1969), (SECOND_HALF, 1970, 2010))
    assert mt.EPSILON == 1e-9
    assert mt.VERDICTS == ("too_few", "holds", "not_holds")
    assert (mt.CHECK, mt.EXITS, mt.SUMMARY, mt.REPORT) == (
        "macro_check.json",
        "macro_exits.jsonl",
        "macro_table.json",
        "macro_table.md",
    )


def test_imported_modules_are_unchanged() -> None:
    for module, digest in UNCHANGED.items():
        assert hashlib.sha256(Path(str(module.__file__)).read_bytes()).hexdigest() == digest


def test_source_reuses_the_state_table_and_the_run_up_table() -> None:
    for line in (
        "from ai_trading_system.research_probes import runup_state_table as rs",
        "from ai_trading_system.research_probes import state_table as st",
        "from ai_trading_system.research_probes import policy_table as pt",
    ):
        assert line in SOURCE
    for call in (
        "st.state_frame(",
        "st.monthly_frame(",
        "st.check_problems(",
        "rs.month_targets(",
        "rs.market_series(",
        "rs.leg_exits(",
        "rs.identity_mismatches(",
        "pt.market_window(",
    ):
        assert call in SOURCE, call
    for name in (
        "def state_frame",
        "def expanding_percentile",
        "def known_month_end",
        "def month_targets",
        "def leg_exits",
        "def signal_exposure",
        "def simulate",
    ):
        assert f"{name}(" not in SOURCE
    for text in ("2011-", "qqq_long_history", "simulate(", "forward_ledger", "long_bull"):
        assert text not in SOURCE
    assert SOURCE.count("load_snapshot") == 1
    imports = [line for line in SOURCE.splitlines() if line.startswith(("import ", "from "))]
    for banned in ("strategies", "screen", "forward", "qqq"):
        assert not any(banned in line for line in imports), banned


# ----- loading ------------------------------------------------------------------------------------
def test_load_inputs_reads_the_snapshot_the_gate_and_the_qqq_q3_rules(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _snapshot(FRAMES)
    asked: list[str] = []

    def load(root: Path, snapshot_id: str) -> SignalSnapshot:
        asked.append(snapshot_id)
        return snapshot

    monkeypatch.setattr(signal_sources, "load_snapshot", load)
    inputs = mt.load_inputs()
    assert asked == [mt.SNAPSHOT_ID]
    assert inputs.factors.index.max() <= pd.Timestamp("2010-12-31")
    assert inputs.factors.index.min() == pd.Timestamp("1926-07-01")
    assert list(inputs.macro.columns) == list(st.LAGS)
    assert inputs.macro.index[-1] == P("2010-12")
    assert {name: entry.id for name, entry in inputs.entries.items()} == mt.RULES
    assert inputs.rules_sha256 == hashlib.sha256(PREREG.read_bytes()).hexdigest()
    # The US market's slow-bear segments from the gate's development window, as state_table uses.
    assert len(inputs.segments) == 10
    assert all(start <= end for _, start, end in inputs.segments)
    assert [s["source"] for s in inputs.sources] == list(st.LAGS)
    assert {s["source"]: s["offsets"] for s in inputs.sources} == {
        source: st._offsets(st.known_offsets(*lags)) for source, lags in st.LAGS.items()
    }
    assert inputs.sources[3] == {
        "source": st.BAA,
        "first": "1919-01",
        "last": "2010-12",
        "offsets": "M−1",
    }


def test_load_inputs_stops_on_a_wrong_lag(monkeypatch: pytest.MonkeyPatch) -> None:
    specs = tuple(
        _spec(st.BAA, "fred", "BAA", lag=1) if spec.source_id == st.BAA else spec for spec in SPECS
    )
    snapshot = _snapshot(FRAMES, specs)
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    with pytest.raises(ValueError, match="baa_monthly"):
        mt.load_inputs()


def test_build_takes_the_slow_leg_from_the_bench_targets_and_the_states_from_state_table() -> None:
    returns, cash = rs.market_series(BASE.factors)
    months = rs.month_targets(returns, cash, BASE.entries)
    pd.testing.assert_frame_equal(BUILT.months, months)
    assert BUILT.exits == rs.leg_exits(months, "slow") != rs.leg_exits(months, "fast")
    pd.testing.assert_frame_equal(
        BUILT.states, st.state_frame(BASE.macro)[["spread_value", "cape_pct"]]
    )
    assert BUILT.segments == BASE.segments and BUILT.sources == BASE.sources


def test_window_mean_is_the_1929_to_2010_monthly_mean_excess() -> None:
    returns = _returns(months={"1928-12": 0.05, "1950-06": 0.002}, base=0.0003)
    built = _built([], returns=returns, cash=0.0001)
    mask = (DAYS > _last_day("1928-12")) & (DAYS <= pd.Timestamp("2010-12-31"))
    expected = float((np.log1p(returns[mask]) - math.log1p(0.0001)).sum()) / 984
    assert built.mu == pytest.approx(expected, abs=1e-12)


def test_assemble_needs_the_month_before_1929() -> None:
    late = pd.bdate_range("1929-01-01", "2010-12-31")
    months = _targets([]).loc[P("1929-01") :]
    with pytest.raises(ValueError, match="1928-12"):
        mt.assemble(
            months,
            pd.Series(0.0, index=late),
            pd.Series(0.0, index=late),
            _states(),
            (),
            [],
        )


# ----- the two conditions -------------------------------------------------------------------------
def test_a_one_month_exit_never_comes_back_early_but_stays_in_the_denominator() -> None:
    # Out at the 1931-05 month end, back at 1931-06: no month end lies strictly between.
    returns = _returns(months={"1931-06": 0.001})
    built = _built(["1931-05"], cape={"1931-05": 0.1, "1931-06": 0.1}, returns=returns)
    assert built.exits == [(P("1931-05"), P("1931-06"))]
    (row,) = _rows(built)
    assert row["months_out"] == 1
    assert row["at_exit"]["cape"] is True
    assert all(fire is None for fire in row["fires"].values())
    assert all(trip is None for trip in row["trips"].values())
    assert row["best"] == pytest.approx(0.5 * _excess(returns, 0.0, "1931-05", "1931-06"))
    assert row["best"] > 0
    assert row["best_reach"] == 0.0


def test_a_condition_true_at_the_exit_comes_back_the_next_month() -> None:
    built = _built(
        [*_span("1932-01", "1932-06"), "1936-05", *_span("1937-01", "1937-05")],
        cape={
            "1932-01": 0.4,
            "1932-02": 0.4,  # true at m and m + 1: back at m + 1
            "1936-05": 0.4,  # true at m of a one-month exit: never early
            "1937-01": 0.4,  # true at m, not at m + 1, again at m + 2
            "1937-03": 0.4,
        },
    )
    first, short, gap = _rows(built)
    assert (first["month"], first["reentry"]) == ("1932-01", "1932-07")
    assert first["at_exit"]["cape"] is True and first["at_exit"]["either"] is True
    assert first["fires"]["cape"] == "1932-02"
    assert first["trips"]["cape"]["after"] == 1
    assert first["trips"]["cape"]["early_months"] == 5
    assert short["at_exit"]["cape"] is True and short["fires"]["cape"] is None
    assert gap["at_exit"]["cape"] is True and gap["fires"]["cape"] == "1937-03"
    summary = mt.timing_check(built)
    assert summary["conditions"]["cape"]["early"] == 2
    assert summary["conditions"]["cape"]["at_exit"] == 2
    assert summary["conditions"]["cape"]["next_month"] == 1
    table = mt.condition_tables(_rows(built), "cape")
    assert table["groups"]["at_exit"]["early"] == 2
    assert table["groups"]["later"]["early"] == 0


def test_cape_is_cheap_at_or_below_the_line_and_undefined_is_not_cheap() -> None:
    built = _built(
        _span("1934-01", "1934-08"),
        cape={"1934-02": float("nan"), "1934-03": float("nan"), "1934-04": 0.5},
    )
    (row,) = _rows(built)
    assert row["fires"]["cape"] == "1934-04"
    assert row["fires"]["cape_70"] == "1934-04"
    assert row["fires"]["cape_30"] is None
    assert row["at_exit"]["cape"] is False


def test_the_spread_line_is_from_its_high_since_the_exit() -> None:
    spread = {
        "1932-12": 9.0,  # before the exit: not part of the high
        "1933-01": 2.0,
        "1933-02": 2.5,
        "1933-03": 2.1,  # 0.84 of the high: only the 10% line
        "1933-04": float("nan"),  # missing: never fires, and leaves the high alone
        "1933-05": 2.0,  # exactly 0.8 of the high
        **dict.fromkeys(_span("1933-06", "1933-08"), 2.0),
    }
    built = _built(_span("1933-01", "1933-08"), spread=spread)
    (row,) = _rows(built)
    assert row["fires"]["spread"] == "1933-05"
    assert row["fires"]["spread_10"] == "1933-03"
    assert row["fires"]["spread_30"] is None
    assert row["at_exit"]["spread"] is False
    assert mt.timing_check(built)["conditions"]["spread"]["next_month"] == 0


def test_the_spread_high_includes_the_exit_month() -> None:
    built = _built(_span("1935-01", "1935-05"), spread={"1935-01": 3.0, "1935-02": 2.4})
    (row,) = _rows(built)
    assert row["fires"]["spread"] == "1935-02"
    assert row["trips"]["spread"]["after"] == 1


def test_a_missing_spread_at_the_exit_month_takes_the_high_from_later_months() -> None:
    built = _built(
        _span("1936-01", "1936-06"),
        spread={"1936-01": float("nan"), "1936-02": 2.0, "1936-03": 1.7, "1936-04": 1.6},
    )
    (row,) = _rows(built)
    assert row["fires"]["spread"] == "1936-04"


def test_holds_at_refuses_an_unknown_condition() -> None:
    with pytest.raises(ValueError, match="unknown condition"):
        mt.holds_at(_states(), "rates", 0.5, P("1950-01"), P("1950-02"))


def test_either_is_the_earlier_of_the_two() -> None:
    built = _built(
        _span("1938-01", "1938-09"),
        spread={**dict.fromkeys(_span("1938-01", "1938-03"), 2.0), "1938-04": 1.5},
        cape={"1938-02": 0.6, "1938-03": 0.2},  # 0.6: only the 0.7 line, a month earlier
    )
    (row,) = _rows(built)
    assert (row["fires"]["spread"], row["fires"]["cape"]) == ("1938-04", "1938-03")
    assert row["fires"]["cape_70"] == "1938-02"
    assert row["fires"]["either"] == "1938-03"
    assert row["trips"]["either"]["fire"] == "1938-03"
    assert row["at_exit"]["either"] is False
    built = _built(_span("1938-01", "1938-09"))
    (row,) = _rows(built)
    assert row["fires"]["either"] is None and row["trips"]["either"] is None


def test_an_exit_still_out_at_2010_is_only_counted() -> None:
    built = _built(_span("2010-06", "2010-12"), cape={"2010-08": 0.1})
    (row,) = _rows(built)
    assert row["complete"] is False
    assert row["reentry"] is None and row["months_out"] is None
    assert row["fires"] == {} and row["trips"] == {} and row["control"] == {}
    assert "best" not in row and "leg_pnl" not in row
    summary = mt.timing_check(built)
    assert summary["exits"]["complete"] == 0 and summary["exits"]["incomplete"] == 1
    assert mt.stats(_rows(built), "cape")["exits"] == 0


def test_slow_bears_are_the_segments_the_months_out_overlap() -> None:
    segments = [("ends_at_exit", "1930-01", "1931-05"), ("inside", "1931-06", "1931-06")]
    built = _built(["1931-05"], segments=segments)
    (row,) = _rows(built)
    assert row["slow_bears"] == ["inside"]
    segments = [("after_2010", "2010-10", "2011-06"), ("before", "2009-01", "2010-05")]
    built = _built(_span("2010-06", "2010-12"), segments=segments)
    assert mt.timing_rows(built)[0]["slow_bears"] == ["after_2010"]


# ----- the trip: g, g', the dip and the two denominators ------------------------------------------
def test_g_and_g_prime_are_half_the_excess_from_the_fire_to_f0001s_return() -> None:
    returns = _returns(
        months={"1950-02": -0.002, "1950-03": 0.001, "1950-04": 0.003, "1950-05": -0.001},
        base=0.0002,
    )
    built = _built(_span("1950-01", "1950-06"), cape={"1950-03": 0.2}, returns=returns, cash=0.0001)
    (row,) = _rows(built)
    assert (row["fires"]["cape"], row["reentry"]) == ("1950-03", "1950-07")
    trip = row["trips"]["cape"]
    gain = 0.5 * _excess(returns, 0.0001, "1950-03", "1950-07")
    assert trip["gain"] == pytest.approx(gain, abs=1e-12)
    assert trip["early_months"] == 4
    assert trip["net"] == pytest.approx(gain - 0.5 * 4 * built.mu, abs=1e-12)
    out = _excess(returns, 0.0001, "1950-01", "1950-07")
    assert row["out_excess"] == pytest.approx(out, abs=1e-12)
    assert row["leg_pnl"] == pytest.approx(-0.5 * out, abs=1e-12)
    mask = (DAYS > _last_day("1950-03")) & (DAYS <= _last_day("1950-07"))
    low = float(np.cumprod(1.0 + returns[mask].to_numpy()).min()) - 1.0
    assert low > 0
    assert trip["low"] == pytest.approx(low, abs=1e-12)
    assert trip["dip"] is False


def test_the_best_return_picks_the_best_month_end_with_and_without_the_exit_month() -> None:
    returns = _returns(months={"1951-02": 0.008, "1951-03": -0.006, "1951-04": 0.002})
    built = _built(_span("1951-01", "1951-03"), returns=returns)
    (row,) = _rows(built)
    assert row["reentry"] == "1951-04"
    starts = _span("1951-01", "1951-03")
    best = max(0.0, *(_excess(returns, 0.0, b, "1951-04") for b in starts))
    reach = max(0.0, *(_excess(returns, 0.0, b, "1951-04") for b in starts[1:]))
    assert row["best"] == pytest.approx(0.5 * best, abs=1e-12)
    assert row["best_reach"] == pytest.approx(0.5 * reach, abs=1e-12)
    assert row["best"] > row["best_reach"] > 0
    falling = _built(_span("1951-01", "1951-03"), returns=_returns(base=-0.001))
    (row,) = _rows(falling)
    assert row["best"] == 0.0 and row["best_reach"] == 0.0


@pytest.mark.parametrize(("drop", "dip"), [(-0.20, True), (-0.1999, False)])
def test_a_dip_is_a_fall_of_20_percent_after_the_fire_up_to_f0001s_return(
    drop: float, dip: bool
) -> None:
    returns = _returns(days={"1940-06-12": drop})
    built = _built(_span("1940-01", "1940-11"), cape={"1940-03": 0.2}, returns=returns)
    (row,) = _rows(built)
    trip = row["trips"]["cape"]
    assert trip["low"] == pytest.approx(drop, abs=1e-12)
    assert trip["dip"] is dip


def test_the_dip_counts_the_path_not_one_day() -> None:
    returns = _returns(days={"1940-06-12": -0.11, "1940-06-13": -0.11})
    built = _built(_span("1940-01", "1940-11"), cape={"1940-03": 0.2}, returns=returns)
    assert _rows(built)[0]["trips"]["cape"]["dip"] is True


def test_the_dip_starts_after_the_fire_close_and_ends_at_the_return_close() -> None:
    returns = _returns(
        days={
            str(_last_day("1940-03").date()): -0.5,  # the fire day itself
            str((_last_day("1940-12") + pd.offsets.BDay(1)).date()): -0.5,  # after r
            str(_last_day("1940-12").date()): -0.15,  # r's own close counts
        }
    )
    built = _built(_span("1940-01", "1940-11"), cape={"1940-03": 0.2}, returns=returns)
    trip = _rows(built)[0]["trips"]["cape"]
    assert trip["low"] == pytest.approx(-0.15, abs=1e-12)
    assert trip["dip"] is False


# ----- the wait-k-months control ------------------------------------------------------------------
@pytest.mark.parametrize(
    ("offsets", "k"),
    [
        ([2, 3], 3),
        ([1, 2], 2),
        ([2, 2, 3, 3], 3),
        ([1], 1),
        ([5, 1, 1], 1),
        ([4, 7, 9], 7),
        ([], None),
    ],
)
def test_control_k_is_the_median_rounded_half_up(offsets: list[int], k: int | None) -> None:
    assert mt.control_k(offsets) == k


def test_the_control_waits_k_months_on_every_complete_exit_out_longer_than_k() -> None:
    built = _built(
        [
            *_span("1960-01", "1960-03"),  # A: out 3 months, CAPE fires after 2
            *_span("1962-01", "1962-06"),  # B: out 6 months, CAPE fires after 3
            *_span("1964-01", "1964-05"),  # C: out 5 months, CAPE never fires
        ],
        cape={"1960-03": 0.2, "1962-04": 0.2},
    )
    rows = _rows(built)
    assert [row["months_out"] for row in rows] == [3, 6, 5]
    assert mt.control_ks(mt.timing_rows(built)) == {"spread": None, "cape": 3}
    a, b, c = rows
    assert [a["trips"]["cape"]["after"], b["trips"]["cape"]["after"]] == [2, 3]
    assert c["trips"]["cape"] is None
    assert a["control"]["cape"] is None
    assert b["control"]["cape"]["fire"] == "1962-04" and b["control"]["cape"]["early_months"] == 3
    assert c["control"]["cape"]["fire"] == "1964-04" and c["control"]["cape"]["early_months"] == 2
    assert all(row["control"]["spread"] is None for row in rows)
    summary = mt.timing_check(built)
    assert summary["control"] == {"spread": {"k": None, "early": 0}, "cape": {"k": 3, "early": 2}}
    table = mt.condition_tables(rows, "cape")
    assert table["control"]["early"] == 2
    assert table["control"]["gain"] == pytest.approx(
        b["control"]["cape"]["gain"] + c["control"]["cape"]["gain"]
    )


# ----- the tables ---------------------------------------------------------------------------------
def _trip(gain: float, net: float, early: int = 2, dip: bool = False) -> dict[str, Any]:
    return {"gain": gain, "net": net, "early_months": early, "dip": dip, "after": 1}


def _record(
    year: int,
    best: float,
    reach: float,
    trip: dict[str, Any] | None,
    *,
    at_exit: bool = False,
    complete: bool = True,
) -> dict[str, Any]:
    return {
        "year": year,
        "complete": complete,
        "best": best,
        "best_reach": reach,
        "at_exit": {"cape": at_exit},
        "trips": {"cape": trip},
        "control": {"cape": None},
    }


def test_stats_divide_by_every_complete_exit() -> None:
    rows = [
        _record(1932, 0.30, 0.20, _trip(0.10, 0.08, early=4, dip=True), at_exit=True),
        _record(1975, 0.20, 0.20, _trip(-0.04, -0.05, early=2)),
        _record(1982, 0.10, 0.00, None),
        _record(2010, 0.50, 0.50, None, complete=False),
    ]
    part = mt.stats(rows, "cape")
    assert part["exits"] == 3 and part["early"] == 2
    assert part["gain"] == pytest.approx(0.06) and part["net"] == pytest.approx(0.03)
    assert part["share"] == pytest.approx(0.06 / 0.60)
    assert part["share_reach"] == pytest.approx(0.06 / 0.40)
    assert part["net_positive"] == 0.5
    assert (part["dips"], part["worst"], part["early_months_sum"]) == (1, -0.04, 6)
    assert part["early_months"]["median"] == 3.0
    groups = mt.groups(rows, "cape")
    assert groups["at_exit"] == {"early": 1, "gain": 0.10, "net": 0.08, "dips": 1}
    assert groups["later"]["early"] == 1 and groups["later"]["net"] == pytest.approx(-0.05)
    halves = mt.condition_tables(rows, "cape")["halves"]
    assert halves[FIRST_HALF]["exits"] == 1 and halves[SECOND_HALF]["exits"] == 2
    empty = mt.stats([_record(1982, 0.0, 0.0, None)], "cape")
    assert empty["share"] is None and empty["net_positive"] is None and empty["worst"] is None


def test_halves_split_at_1970() -> None:
    built = _built([*_span("1969-12", "1970-01"), *_span("1970-06", "1970-07")])
    summary = mt.timing_check(built)
    assert summary["exits"]["halves"] == {FIRST_HALF: 1, SECOND_HALF: 1}
    assert summary["exits"]["decades"]["1960s"] == 1 and summary["exits"]["decades"]["1970s"] == 1


# ----- the verdict --------------------------------------------------------------------------------
def _table(
    *,
    exits: int = 30,
    early: int = 9,
    net: float = 0.30,
    share: float | None = 0.30,
    dips: int = 2,
    control: float = 0.10,
    first: tuple[int, float] = (5, 0.20),
    second: tuple[int, float] = (4, 0.10),
    at_exit: float = 0.10,
) -> dict[str, Any]:
    return {
        "main": {"exits": exits, "early": early, "net": net, "share": share, "dips": dips},
        "control": {"net": control},
        "halves": {
            FIRST_HALF: {"early": first[0], "net": first[1]},
            SECOND_HALF: {"early": second[0], "net": second[1]},
        },
        "groups": {"at_exit": {"net": at_exit}},
    }


@pytest.mark.parametrize(
    ("changes", "verdict"),
    [
        ({}, "holds"),
        ({"exits": 19}, "too_few"),
        ({"exits": 20}, "holds"),
        ({"early": 5, "dips": 1}, "too_few"),
        ({"early": 6, "dips": 2}, "holds"),
        ({"net": 0.0, "control": -0.10}, "not_holds"),
        ({"control": 0.30}, "not_holds"),
        ({"control": 0.2999}, "holds"),
        ({"share": 0.2499}, "not_holds"),
        ({"share": 0.25}, "holds"),
        ({"share": None}, "not_holds"),
        ({"first": (5, -0.01)}, "not_holds"),
        ({"second": (4, 0.0)}, "not_holds"),
        ({"second": (2, -0.5)}, "holds"),
        ({"second": (3, -0.01)}, "not_holds"),
        ({"dips": 3}, "holds"),
        ({"dips": 4}, "not_holds"),
    ],
)
def test_verdict(changes: dict[str, Any], verdict: str) -> None:
    assert mt.judge(_table(**changes))["verdict"] == verdict


def test_verdict_parts_and_reasons() -> None:
    decision = mt.judge(_table(exits=12, early=3))
    assert decision == {
        "verdict": "too_few",
        "reasons": ["完整退出 12 次，少于 20 次", "早到 3 次，少于 6 次"],
    }
    decision = mt.judge(_table(second=(2, 0.10), dips=4))
    assert decision["halves_judged"] == {FIRST_HALF: True}
    assert decision["recover_ok"] is True and decision["halves_ok"] is True
    assert decision["dips_ok"] is False and decision["verdict"] == "not_holds"


def test_cheap_filter_reads_only_a_holding_b() -> None:
    holds = {"verdict": "holds"}
    assert mt.cheap_filter(holds, _table(at_exit=0.16)) is True
    assert mt.cheap_filter(holds, _table(at_exit=0.15)) is False
    assert mt.cheap_filter(holds, _table(net=-0.1, at_exit=0.10)) is False
    assert mt.cheap_filter({"verdict": "not_holds"}, _table(at_exit=0.30)) is None


def test_read_judges_a_and_b_only(monkeypatch: pytest.MonkeyPatch) -> None:
    assert set(RESULT["decisions"]) == {"spread", "cape"}
    assert set(RESULT["tables"]) == {*mt.CONDITIONS, *mt.SENSITIVITY, mt.EITHER}
    assert "control" in RESULT["tables"]["cape"] and "control" not in RESULT["tables"]["either"]
    seen: list[dict[str, Any]] = []

    def judge(table: dict[str, Any]) -> dict[str, Any]:
        seen.append(table)
        return {"verdict": "holds" if len(seen) == 1 else "not_holds", "reasons": []}

    monkeypatch.setattr(mt, "judge", judge)
    result, _ = mt.read(BASE)
    assert len(seen) == 2
    assert [table["main"] for table in seen] == [
        RESULT["tables"]["spread"]["main"],
        RESULT["tables"]["cape"]["main"],
    ]
    assert result["decisions"]["spread"]["verdict"] == "holds"
    assert result["single_holds"] is True and result["cheap_filter"] is None
    monkeypatch.setattr(mt, "judge", lambda table: {"verdict": "holds", "reasons": []})
    result, _ = mt.read(BASE)
    assert result["single_holds"] is False
    assert result["cheap_filter"] is mt.cheap_filter({"verdict": "holds"}, RESULT["tables"]["cape"])


# ----- check: counts and distributions only -------------------------------------------------------
def test_check_never_prices_a_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("check must not look at returns after the exit")

    for name in ("out_excess", "trip", "best_return", "priced_rows"):
        monkeypatch.setattr(mt, name, forbidden)
    assert mt.timing_check(BUILT) == RESULT["check"]


def test_check_prints_no_exit_dates() -> None:
    summary = RESULT["check"]
    assert set(summary) == {
        "run_id",
        "snapshot_id",
        "sources",
        "identity_mismatches",
        "exits",
        "months_out",
        "conditions",
        "control",
        "slow_bears",
    }
    rest = {key: value for key, value in summary.items() if key != "sources"}
    assert sorted(set(_strings(rest))) == sorted({mt.RUN_ID, mt.SNAPSHOT_ID})
    text = json.dumps(summary, ensure_ascii=False)
    for row in EXITS:
        for value in (row["month"], row["reentry"], *row["fires"].values()):
            if value is not None:
                assert f'"{value}"' not in text
    for key in ("gain", "net", "best", "best_reach", "leg_pnl", "out_excess", "dip", "low"):
        assert f'"{key}"' not in text


def test_check_counts_match_the_exits() -> None:
    summary = RESULT["check"]
    complete = [row for row in EXITS if row["complete"]]
    assert summary["exits"]["complete"] == len(complete) > 20
    assert summary["exits"]["incomplete"] == len(EXITS) - len(complete)
    assert sum(summary["exits"]["decades"].values()) == len(complete)
    assert summary["months_out"] == rs.quartiles([row["months_out"] for row in complete])
    for name in [*mt.CONDITIONS, *mt.SENSITIVITY, mt.EITHER]:
        trips = [row["trips"][name] for row in complete if row["trips"][name]]
        part = summary["conditions"][name]
        assert part["early"] == len(trips) == RESULT["tables"][name]["main"]["early"]
        assert part["early_months"] == rs.quartiles([t["early_months"] for t in trips])
        assert part["early_months_sum"] == sum(t["early_months"] for t in trips)
        assert part["next_month"] == sum(t["after"] == 1 for t in trips)
        assert part["at_exit"] == sum(
            1 for row in complete if row["trips"][name] and row["at_exit"][name]
        )
        assert sum(part["halves"].values()) == part["early"]
    for name in mt.CONDITIONS:
        assert summary["control"][name]["early"] == sum(
            1 for row in complete if row["control"][name]
        )
    assert summary["identity_mismatches"] == 0
    assert summary["slow_bears"] == len(BASE.segments)
    assert summary["conditions"]["spread"]["at_exit"] == 0


# ----- the whole read -----------------------------------------------------------------------------
def test_the_read_has_every_part_and_dumps_as_json() -> None:
    assert RESULT["run_id"] == mt.RUN_ID and RESULT["owner_decision"] == mt.OWNER_DECISION
    assert RESULT["snapshot_id"] == mt.SNAPSHOT_ID
    assert RESULT["mu"] == BUILT.mu
    complete = [row for row in EXITS if row["complete"]]
    assert RESULT["leg_pnl"] == pytest.approx(sum(row["leg_pnl"] for row in complete))
    assert RESULT["best"] == pytest.approx(sum(row["best"] for row in complete))
    assert RESULT["best_reach"] == pytest.approx(sum(row["best_reach"] for row in complete))
    assert len(EXITS) == len(BUILT.exits)
    assert [list(row) for row in EXITS] == [sorted(row) for row in EXITS]
    json.dumps(RESULT, allow_nan=False)
    json.dumps(EXITS, allow_nan=False)
    holds = [name for name in mt.CONDITIONS if RESULT["decisions"][name]["verdict"] == "holds"]
    assert RESULT["single_holds"] is (len(holds) == 1)
    for name in mt.CONDITIONS:
        assert RESULT["decisions"][name]["verdict"] in mt.VERDICTS


def test_data_after_2010_never_matter() -> None:
    later = {}
    for source, frame in FRAMES.items():
        frame = frame.copy()
        after = frame.index > pd.Timestamp("2010-12-31")
        frame.loc[after] = frame.loc[after] * 3.0 - 0.01
        later[source] = frame
    assert mt.read(_inputs(later)) == (RESULT, EXITS)


def test_report_sentences() -> None:
    text = mt.render({**RESULT, "exits": EXITS, "provenance": {"commit": "abc123"}})
    first_screen = text.split("## 主表")[0]
    assert "甲、乙各判一次；「甲或乙」、对照和敏感度不判。" in first_screen
    for name in mt.CONDITIONS:
        assert f"`{RESULT['decisions'][name]['verdict']}`" in first_screen
    assert mt.SNAPSHOT_ID in text and RESULT["rules_sha256"] in text and "abc123" in text
    for heading in (
        "## 主表",
        "## 两半",
        "## 乙按退出那个月末分两组（只印）",
        "## 敏感度（只印，不判）",
        "## 每次退出",
        "## 数据和核对",
        "## 局限",
    ):
        assert heading in text
    assert text.index("## 两半") < text.index("## 敏感度") < text.index("## 每次退出")
    exit_table = text.split("## 每次退出")[1].split("## 数据和核对")[0]
    assert sum(1 for line in exit_table.splitlines() if line.startswith("| ")) == len(EXITS) + 1
    assert "不换算成每年多少点，不和 QQQ 2002–07 的数并排比。" in text
    assert "不是长牛代价的读数" in text
    assert "production_effect=none；broker_action=none。" in text
    assert "便宜时慢腿不出" not in text
    assert RESULT["single_holds"] is False and "至少一个判 holds" not in text


def test_report_readings_appear_only_when_they_apply() -> None:
    result = {**RESULT, "exits": EXITS, "cheap_filter": True, "single_holds": True}
    text = mt.render(result)
    assert "续期卡读成「便宜时慢腿不出」" in text
    assert "至少一个判 holds 的机会约 15%（复盘线程模拟，推断）" in text
    too_few = {"verdict": "too_few", "reasons": ["早到 3 次，少于 6 次"]}
    text = mt.render({**result, "decisions": {**RESULT["decisions"], "cape": too_few}})
    assert "`too_few`，次数太少，不判。" in text and "早到 3 次，少于 6 次" in text


# ----- files and commands -------------------------------------------------------------------------
def test_files(tmp_path: Path) -> None:
    mt.write_check(RESULT["check"], tmp_path / "c")
    assert json.loads((tmp_path / "c" / mt.CHECK).read_text(encoding="utf-8")) == RESULT["check"]
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [mt.CHECK]
    mt.write_result(RESULT, EXITS, tmp_path / "r")
    lines = (tmp_path / "r" / mt.EXITS).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == EXITS
    assert json.loads((tmp_path / "r" / mt.SUMMARY).read_text(encoding="utf-8")) == RESULT
    assert (tmp_path / "r" / mt.REPORT).read_text(encoding="utf-8") == mt.render(
        {**RESULT, "exits": EXITS}
    )


def test_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(mt, "load_inputs", lambda: BASE)
    monkeypatch.setattr(mt, "git_provenance", lambda root: {"commit": "abc123"})
    runner = CliRunner()

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("check must not look at returns after the exit")

    with pytest.MonkeyPatch.context() as patch:
        for name in ("out_excess", "trip", "best_return", "priced_rows"):
            patch.setattr(mt, name, forbidden)
        out = runner.invoke(mt.app, ["check", "--out", str(tmp_path / "c")])
    assert out.exit_code == 0, out.output
    summary = RESULT["check"]
    spread, cape = summary["conditions"]["spread"], summary["conditions"]["cape"]
    assert (
        f"macro_reentry_table: 慢腿完整退出 {summary['exits']['complete']} 次，"
        f"未完 {summary['exits']['incomplete']} 次；"
        f"甲早到 {spread['early']} 次，乙早到 {cape['early']} 次；"
        f"早的月数中位 甲 {spread['early_months']['median']:g}、"
        f"乙 {cape['early_months']['median']:g}；"
        f"对照 k 甲 {summary['control']['spread']['k']:g}、"
        f"乙 {summary['control']['cape']['k']:g}"
    ) in out.output
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [mt.CHECK]
    out = runner.invoke(mt.app, ["run", "--out", str(tmp_path / "r")])
    assert out.exit_code == 0, out.output
    verdicts = "、".join(f"{name} {RESULT['decisions'][name]['verdict']}" for name in mt.CONDITIONS)
    assert f"macro_reentry_table: 判定 {verdicts}" in out.output
    assert sorted(p.name for p in (tmp_path / "r").iterdir()) == sorted(
        [mt.CHECK, mt.EXITS, mt.SUMMARY, mt.REPORT]
    )
    written = json.loads((tmp_path / "r" / mt.SUMMARY).read_text(encoding="utf-8"))
    assert written == {**RESULT, "provenance": {"commit": "abc123"}}
    assert (tmp_path / "r" / mt.CHECK).read_bytes() == (tmp_path / "c" / mt.CHECK).read_bytes()
    report = (tmp_path / "r" / mt.REPORT).read_text(encoding="utf-8")
    assert "abc123" in report


def test_commands_stop_when_loading_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def broken() -> mt.Inputs:
        raise ValueError("no snapshot\nsecond line")

    monkeypatch.setattr(mt, "load_inputs", broken)
    for command in ("check", "run"):
        out = CliRunner().invoke(mt.app, [command, "--out", str(tmp_path / command)])
        assert out.exit_code == 1
        assert "macro_reentry_table: 数据载入失败：no snapshot" in out.output
        assert "second line" not in out.output
        assert not (tmp_path / command).exists()
    monkeypatch.setattr(mt, "load_inputs", lambda: BASE)
    monkeypatch.setattr(
        rs, "market_series", lambda factors: (_ for _ in ()).throw(ValueError("bad market"))
    )
    for command in ("check", "run"):
        out = CliRunner().invoke(mt.app, [command, "--out", str(tmp_path / f"m{command}")])
        assert out.exit_code == 1 and "macro_reentry_table: 建数据失败：bad market" in out.output
        assert not (tmp_path / f"m{command}").exists()
