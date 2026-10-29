"""Acceptance tests for the unemployment filter table (TRADING-2565-89).

owner_decision:TRADING-2565:2026-10-29:print_only_tables, charter p6 sec. 23, the third of four
print-only tables.
Each exit of F0001's two legs on the US market, 1929-2010, is asked once: had the leg waited to
go out until the unemployment rate was above its own 12-month mean (M* = M - 1, no carry-forward),
what would this leg have made on this trip, net of the window's mean monthly excess for the extra
months, and against going out the same number of months late without looking at unemployment.
Only the late-exit half of the blog rule. Not a probe, not a trial, no ledger, no new rule's
position line, not a long-bull cost reading.

Synthetic data only: no snapshot is read. The fake signal snapshot stands in for S3, the snapshot
with the three FRED unemployment rates; the QQQ-Q3 preregistration, the trial gate and the
bench's signals are the repository's own files. runup_state_table, state_table, policy_table,
cycle_power and macro_reentry_table are imported, not changed.
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
from ai_trading_system.research_probes import macro_reentry_table as mra
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import runup_state_table as rs
from ai_trading_system.research_probes import state_table as st
from ai_trading_system.research_probes import unemployment_filter_table as uf

# S3: the snapshot the research session fetched after TRADING-2565-88.
S3 = "sig-c68ae8decccd"
SOURCE = Path(uf.__file__).read_text(encoding="utf-8")
PREREG = Path(__file__).resolve().parents[1] / "research" / "QQQ-Q3" / "prereg.yaml"
UNCHANGED = {
    rs: "610c76cc5ae34a92b4f1aa3f04614dc4321c8394be3567613941421b219e3c97",
    st: "0a39bc86e58495a3af598c7f73abc0da6c988015d26be649115f56126eca8a22",
    pt: "2a8b550855ebc43b0ccaf7c81f49efa46071b7a518027455a568813744b5a6ca",
    cp: "53e5286fec515c99dcb264f4ce8959afb4c4c54d970ab24b2df5d4c3843efafc",
    mra: "7827dd60da475665b360b5b0bafe37f5dd4525c0de1d64e45473478650fcbf2b",
}
FIRST_HALF, SECOND_HALF = "1929–1969", "1970–2010"
A, B, U = "unrate_nber_a", "unrate_nber_b", "unrate"
# The three series' own ranges on FRED (requirement sec. 2.1).
RANGES = {U: ("1948-01", "2010-12"), B: ("1940-01", "1946-12"), A: ("1929-04", "1942-06")}


def P(text: str) -> Any:
    """A month, typed loosely so it can index the frames below."""

    return pd.Period(text, freq="M")


# ----- hand-built months: the two legs, the three rates and the market are set per test
DAYS = pd.bdate_range("1928-11-01", "2010-12-31")
MONTH_OF = DAYS.to_period("M")


def _last_day(month: str) -> pd.Timestamp:
    return pd.Timestamp(DAYS[MONTH_OF == P(month)].max())


def _span(first: str, last: str) -> list[str]:
    return [str(p) for p in pd.period_range(first, last, freq="M")]


def _targets(fast: Iterable[str] = (), slow: Iterable[str] = ()) -> pd.DataFrame:
    """Month-end rows like month_targets: a leg is 0 in its listed months, 1 elsewhere."""

    outs = {"fast": set(fast), "slow": set(slow)}
    last = pd.Series(np.arange(len(DAYS)), index=DAYS).groupby(MONTH_OF).tail(1)
    index = pd.DatetimeIndex(last.index).to_period("M")
    legs = {
        leg: np.array([0.0 if str(p) in out else 1.0 for p in index]) for leg, out in outs.items()
    }
    return pd.DataFrame(
        {
            "date": pd.DatetimeIndex(last.index),
            "pos": last.to_numpy(),
            "f0001": (legs["fast"] + legs["slow"]) / 2.0,
            "fast": legs["fast"],
            "slow": legs["slow"],
        },
        index=index,
    )


def _unemployment(
    values: Mapping[str, Mapping[str, float]] | None = None,
    *,
    base: float = 5.0,
    last: str = "2010-12",
) -> dict[str, pd.DataFrame]:
    """FRED-style frames (month-start dates) over each series' range: base, or a listed value."""

    frames = {}
    for source, (first, end) in RANGES.items():
        dates = pd.date_range(f"{first}-01", f"{last if source == U else end}-01", freq="MS")
        frame = pd.DataFrame({"value": base}, index=dates)
        for month, value in (values or {}).get(source, {}).items():
            frame.loc[pd.Timestamp(f"{month}-01"), "value"] = value
        frames[source] = frame
    return frames


def _rates(**values: Mapping[str, float]) -> pd.DataFrame:
    """The rates frame the module builds; keyword names are the series ids."""

    return uf.unemployment_frame(_unemployment(values))


RATES = _rates()


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
    fast: Iterable[str] = (),
    slow: Iterable[str] = (),
    *,
    rates: pd.DataFrame | None = None,
    returns: pd.Series | None = None,
    cash: float = 0.0,
    segments: Sequence[tuple[str, str, str]] = (),
) -> uf.Built:
    return uf.assemble(
        _targets(fast, slow),
        returns if returns is not None else _returns(),
        pd.Series(cash, index=DAYS),
        rates if rates is not None else RATES,
        tuple((name, P(start), P(end)) for name, start, end in segments),
        [],
    )


def _rows(built: uf.Built) -> list[dict[str, Any]]:
    timing = uf.timing_rows(built)
    return uf.priced_rows(built, timing, uf.control_ks(timing))


def _row(rows: Sequence[dict[str, Any]], leg: str, month: str) -> dict[str, Any]:
    return next(row for row in rows if row["leg"] == leg and row["month"] == month)


def _excess(returns: pd.Series, cash: float, start: str, end: str) -> float:
    """Sum of daily log excess returns after start's month end, up to end's month end."""

    mask = (DAYS > _last_day(start)) & (DAYS <= _last_day(end))
    return float((np.log1p(returns[mask]) - math.log1p(cash)).sum())


# ----- a whole snapshot: daily market factors with planted bears, three monthly unemployment rates
def _frames(seed: int = 5) -> dict[str, pd.DataFrame]:
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
    frames = {cp.FACTORS: factors}
    for source, (first, end) in RANGES.items():
        dates = pd.date_range(f"{first}-01", f"{'2012-06' if source == U else end}-01", freq="MS")
        values = np.clip(5.0 + np.cumsum(rng.normal(0.0, 0.25, len(dates))), 1.0, 25.0)
        frames[source] = pd.DataFrame({"value": np.round(values, 1)}, index=dates)
    return frames


def _spec(
    source: str,
    provider: Literal["fred", "ken_french"],
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
    _spec(A, "fred", "M0892AUSM156SNBR", lag=5),
    _spec(B, "fred", "M0892BUSM156SNBR", lag=5),
    _spec(U, "fred", "UNRATE", lag=5),
)


def _snapshot(
    frames: Mapping[str, pd.DataFrame], specs: Sequence[SourceSpec] = SPECS
) -> SignalSnapshot:
    return SignalSnapshot(uf.SNAPSHOT_ID, dict(frames), tuple(specs))


def _inputs(frames: Mapping[str, pd.DataFrame]) -> uf.Inputs:
    snapshot = _snapshot(frames)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
        return uf.load_inputs()


FRAMES = _frames()
BASE = _inputs(FRAMES)
BUILT = uf.build(BASE)
RESULT, TRIPS = uf.read(BASE)


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [text for item in value.values() for text in _strings(item)]
    if isinstance(value, list):
        return [text for item in value for text in _strings(item)]
    return []


# ----- constants: charter p6 sec. 23.3 and the requirement's sections 3 and 4, fixed before the run
def test_the_design_is_the_one_fixed_before_the_run() -> None:
    assert uf.RUN_ID == "TRADING-2565-89"
    assert uf.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-29:print_only_tables"
    assert uf.SNAPSHOT_ID == S3 != st.SNAPSHOT_ID
    assert st.PLAN_ID == "T0008"
    assert uf.FACTORS == "ff_factors_daily"
    assert (uf.FIRST_MONTH, uf.LAST_MONTH) == (P("1929-01"), P("2010-12"))
    assert uf.QUESTION == "QQQ-Q3"
    assert uf.RULES == {"f0001": "fast_slow", "fast": "fast_only", "slow": "slow_only"}
    assert (uf.LEGS, uf.LEG_SHARE, uf.POOLED) == (("fast", "slow"), 0.5, "both")
    assert uf.SERIES == (U, B, A)
    assert uf.DATASETS == {U: "UNRATE", B: "M0892BUSM156SNBR", A: "M0892AUSM156SNBR"}
    assert uf.RANGES == {source: (P(first), P(last)) for source, (first, last) in RANGES.items()}
    assert uf.MAIN == (12, False, False)
    assert uf.SENSITIVITY == {
        "mean_6": (6, False, False),
        "mean_18": (18, False, False),
        "ge": (12, True, False),
        "undefined_blocks": (12, False, True),
    }
    assert uf.AGREE == (P("1940-12"), P("1942-06"))
    assert (uf.MIN_CHANGED, uf.HALF_MIN, uf.PROTECT_PART) == (20, 5, 0.25)
    assert uf.HALVES == ((FIRST_HALF, 1929, 1969), (SECOND_HALF, 1970, 2010))
    assert uf.NULL_HOLDS == 0.17
    assert uf.EPSILON == 1e-9
    assert uf.VERDICTS == ("too_few", "holds", "not_holds")
    assert uf.STATES == ("U", "N", "undefined")
    assert (uf.CHECK, uf.TRIPS, uf.SUMMARY, uf.REPORT) == (
        "unemp_check.json",
        "unemp_trips.jsonl",
        "unemp_table.json",
        "unemp_table.md",
    )


def test_imported_modules_are_unchanged() -> None:
    for module, digest in UNCHANGED.items():
        assert hashlib.sha256(Path(str(module.__file__)).read_bytes()).hexdigest() == digest


def test_source_reuses_the_bench_targets_and_the_second_table() -> None:
    for line in (
        "from ai_trading_system.research_probes import runup_state_table as rs",
        "from ai_trading_system.research_probes import state_table as st",
        "from ai_trading_system.research_probes import policy_table as pt",
        "from ai_trading_system.research_probes import macro_reentry_table as mra",
    ):
        assert line in SOURCE
    for call in (
        "rs.month_targets(",
        "rs.market_series(",
        "rs.leg_exits(",
        "rs.identity_mismatches(",
        "rs.quartiles(",
        "pt.market_window(",
        "mra.window_mean(",
        "mra.overlaps(",
        "mra.control_k(",
    ):
        assert call in SOURCE, call
    for name in (
        "def month_targets",
        "def leg_exits",
        "def signal_exposure",
        "def simulate",
        "def window_mean",
        "def overlaps",
        "def control_k",
    ):
        assert f"{name}(" not in SOURCE
    # Requirement sec. 7: no carry-forward, no state_table source check, no T0008 snapshot compare.
    for text in (
        "known_month_end",
        "monthly_known_at",
        "check_problems",
        "signal_snapshot_id",
        "2011-",
        "qqq_long_history",
        "simulate(",
        "forward_ledger",
        "long_bull",
    ):
        assert text not in SOURCE, text
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
    inputs = uf.load_inputs()
    assert asked == [uf.SNAPSHOT_ID]
    assert inputs.factors.index.max() <= pd.Timestamp("2010-12-31")
    assert inputs.factors.index.min() == pd.Timestamp("1926-07-01")
    assert list(inputs.rates.columns) == [U, B, A]
    assert (inputs.rates.index[0], inputs.rates.index[-1]) == (P("1928-01"), P("2010-12"))
    assert inputs.rates.loc[P("2010-12"), U] == FRAMES[U].loc[pd.Timestamp("2010-12-01"), "value"]
    assert {name: entry.id for name, entry in inputs.entries.items()} == uf.RULES
    assert inputs.rules_sha256 == hashlib.sha256(PREREG.read_bytes()).hexdigest()
    # The US market's slow-bear segments from the gate's development window, as the second table.
    assert inputs.segments == BASE.segments
    assert len(inputs.segments) == 10
    assert all(start <= end for _, start, end in inputs.segments)
    assert inputs.sources == [
        {"source": U, "first": "1948-01", "last": "2010-12", "months": 756, "missing": []},
        {"source": B, "first": "1940-01", "last": "1946-12", "months": 84, "missing": []},
        {"source": A, "first": "1929-04", "last": "1942-06", "months": 159, "missing": []},
    ]


@pytest.mark.parametrize(
    ("source", "spec"),
    [
        (A, _spec(A, "fred", "M0892AUSM156SNBR", lag=1)),
        (B, _spec(B, "fred", "M0892AUSM156SNBR", lag=5)),
        (U, _spec(U, "fred", "UNRATE", lag=5, release=1)),
    ],
)
def test_load_inputs_stops_on_a_wrong_spec(
    monkeypatch: pytest.MonkeyPatch, source: str, spec: SourceSpec
) -> None:
    specs = tuple(spec if s.source_id == source else s for s in SPECS)
    snapshot = _snapshot(FRAMES, specs)
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    with pytest.raises(ValueError, match=f"^{source}: spec"):
        uf.load_inputs()


def test_load_inputs_stops_on_a_missing_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshot = _snapshot(FRAMES, [s for s in SPECS if s.source_id != B])
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    with pytest.raises(ValueError, match=B):
        uf.load_inputs()


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda f: f.drop(index=f.index[:3]), "unrate_nber_a: starts 1929-07, not 1929-04"),
        (lambda f: f.drop(index=f.index[-1]), "unrate_nber_a: ends 1942-05, not 1942-06"),
        (lambda f: f.assign(value=np.nan), "unrate_nber_a: no values"),
    ],
)
def test_load_inputs_checks_each_series_range_itself(
    monkeypatch: pytest.MonkeyPatch, change: Any, message: str
) -> None:
    snapshot = _snapshot({**FRAMES, A: change(FRAMES[A])})
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    with pytest.raises(ValueError, match=message):
        uf.load_inputs()


def test_a_month_missing_inside_a_series_is_listed_not_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # S3: unrate_nber_b has 83 rows over 84 months, unrate 944 over 945 (to 2026-09).
    frames = {
        **FRAMES,
        B: FRAMES[B].drop(index=pd.Timestamp("1943-05-01")),
        U: FRAMES[U]
        .drop(index=pd.Timestamp("2011-10-01"))
        .assign(value=lambda f: f["value"].mask(f.index == pd.Timestamp("1960-05-01"))),
    }
    snapshot = _snapshot(frames)
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    inputs = uf.load_inputs()
    assert inputs.sources == [
        {"source": U, "first": "1948-01", "last": "2010-12", "months": 755, "missing": ["1960-05"]},
        {"source": B, "first": "1940-01", "last": "1946-12", "months": 83, "missing": ["1943-05"]},
        {"source": A, "first": "1929-04", "last": "1942-06", "months": 159, "missing": []},
    ]
    assert uf.source_problems(inputs.rates) == []
    # Not filled and not carried: M* 1943-05 has no series, then b has 11 values (step 2).
    assert uf.state_at(inputs.rates, P("1943-06")) == ("undefined", None, 0)
    assert uf.state_at(inputs.rates, P("1943-07"))[1:] == (B, 11)
    assert uf.state_at(inputs.rates, P("1944-05"))[1:] == (B, 11)
    assert uf.state_at(inputs.rates, P("1944-06"))[1:] == (B, 12)
    text = uf.render(uf.read(inputs)[0] | {"trips": [], "provenance": {"commit": "abc123"}})
    data = text.split("## 数据和核对")[1].split("## 局限")[0]
    assert "| 中间缺的月 |" in data
    assert "| 1960-05 |" in data and "| 1943-05 |" in data
    assert data.count("| — |") >= 1


def test_missing_months_lists_only_the_gaps_between_the_first_and_last_value() -> None:
    rates = _rates(unrate_nber_a={"1929-04": math.nan, "1930-02": math.nan, "1930-03": math.nan})
    assert uf.missing_months(rates, A) == ["1930-02", "1930-03"]
    assert uf.missing_months(rates, B) == []
    empty = RATES.assign(**{A: math.nan})
    assert uf.missing_months(empty, A) == []


def test_source_problems_and_spec_problems_are_empty_for_the_real_shapes() -> None:
    assert uf.source_problems(RATES) == []
    assert uf.source_problems(BASE.rates) == []
    assert uf.spec_problems({spec.source_id: spec for spec in SPECS}) == []
    late = _unemployment()
    late[U] = late[U].iloc[:-1]
    assert uf.source_problems(uf.unemployment_frame(late)) == ["unrate: ends 2010-11, not 2010-12"]


def test_unemployment_frame_drops_later_values_and_refuses_a_repeated_month() -> None:
    frames = _unemployment(last="2012-06")
    rates = uf.unemployment_frame(frames)
    assert rates.index[-1] == P("2010-12")
    pd.testing.assert_frame_equal(rates, RATES)
    repeated = {**frames, B: pd.concat([frames[B], frames[B].iloc[[5]]])}
    with pytest.raises(ValueError, match="unrate_nber_b repeats a month"):
        uf.unemployment_frame(repeated)
    with pytest.raises(ValueError, match="unrate_nber_a"):
        uf.unemployment_frame({U: frames[U], B: frames[B]})


def test_build_takes_both_legs_from_the_bench_targets() -> None:
    returns, cash = rs.market_series(BASE.factors)
    months = rs.month_targets(returns, cash, BASE.entries)
    pd.testing.assert_frame_equal(BUILT.months, months)
    assert BUILT.exits == {leg: rs.leg_exits(months, leg) for leg in ("fast", "slow")}
    assert len(BUILT.exits["fast"]) > len(BUILT.exits["slow"]) > 10
    pd.testing.assert_frame_equal(BUILT.rates, BASE.rates)
    assert BUILT.segments == BASE.segments and BUILT.sources == BASE.sources


def test_mu_and_sigma_are_over_the_984_months_of_1929_to_2010() -> None:
    returns = _returns(months={"1928-12": 0.05, "1950-06": 0.002, "1990-03": -0.004}, base=0.0003)
    built = _built(returns=returns, cash=0.0001)
    mask = (DAYS > _last_day("1928-12")) & (DAYS <= pd.Timestamp("2010-12-31"))
    expected = float((np.log1p(returns[mask]) - math.log1p(0.0001)).sum()) / 984
    assert built.mu == pytest.approx(expected, abs=1e-12)
    monthly = [
        _excess(returns, 0.0001, str(p - 1), str(p))
        for p in pd.period_range("1929-01", "2010-12", freq="M")
    ]
    assert len(monthly) == 984
    assert built.sigma == pytest.approx(float(np.std(monthly, ddof=1)), abs=1e-12)


def test_assemble_needs_the_month_before_1929() -> None:
    late = pd.bdate_range("1929-01-01", "2010-12-31")
    months = _targets().loc[P("1929-01") :]
    with pytest.raises(ValueError, match="1928-12"):
        uf.assemble(months, pd.Series(0.0, index=late), pd.Series(0.0, index=late), RATES, (), [])


# ----- the state: M* = M - 1, which series, U strictly above the mean --------------------------
SPANS = (
    ("1929-01", "1929-09", None),
    ("1929-10", "1930-03", A),
    ("1930-04", "1940-12", A),
    ("1941-01", "1947-01", B),
    ("1947-02", "1948-06", None),
    ("1948-07", "1948-12", U),
    ("1949-01", "2010-12", U),
)


def test_each_month_end_uses_the_series_of_the_requirement_table() -> None:
    table = uf.state_table(RATES)
    assert list(table.columns) == ["state", "source", "values"]
    assert (table.index[0], table.index[-1], len(table)) == (P("1929-01"), P("2010-12"), 984)
    for first, last, source in SPANS:
        part = table.loc[P(first) : P(last)]
        assert part["source"].tolist() == [source] * len(part), (first, last)
        if source is None:
            assert set(part["state"]) == {"undefined"} and set(part["values"]) == {0}
        else:
            assert set(part["state"]) == {"N"}  # a flat rate is never above its mean
    # The two short windows start at 6 values; the long ones always have 12.
    assert table.loc[P("1929-10") : P("1930-03"), "values"].tolist() == [6, 7, 8, 9, 10, 11]
    assert table.loc[P("1948-07") : P("1948-12"), "values"].tolist() == [6, 7, 8, 9, 10, 11]
    assert set(table.loc[P("1930-04") : P("1940-12"), "values"]) == {12}
    assert set(table.loc[P("1941-01") : P("1947-01"), "values"]) == {12}
    assert set(table.loc[P("1949-01") :, "values"]) == {12}


def test_b_with_fewer_than_12_values_leaves_a_in_place() -> None:
    # M* 1940-11: b has 11 values (1940-01..11), a has 12, so step 1 takes a.
    assert uf.state_at(RATES, P("1940-12"))[1:] == (A, 12)
    assert uf.state_at(RATES, P("1941-01"))[1:] == (B, 12)


def test_the_first_month_after_a_series_ends_is_undefined_not_carried() -> None:
    assert uf.state_at(RATES, P("1947-01")) == ("N", B, 12)  # M* 1946-12, b's last value
    assert uf.state_at(RATES, P("1947-02")) == ("undefined", None, 0)  # M* 1947-01
    assert uf.state_at(RATES, P("1948-06")) == ("undefined", None, 0)  # M* 1948-05: 5 values
    assert uf.state_at(RATES, P("1948-07")) == ("N", U, 6)


def test_a_missing_value_at_m_star_moves_to_the_next_series_or_is_undefined() -> None:
    rates = _rates(unrate_nber_b={"1941-03": math.nan}, unrate={"1960-05": math.nan})
    assert uf.state_at(rates, P("1941-04")) == ("N", A, 12)
    assert uf.state_at(rates, P("1941-05")) == ("N", A, 12)  # b has 11 values: step 1 takes a
    assert uf.state_at(rates, P("1960-06")) == ("undefined", None, 0)
    assert uf.state_at(rates, P("1960-07")) == ("N", U, 11)


def test_u_is_strictly_above_the_mean_of_the_chosen_series() -> None:
    rates = _rates(
        unrate_nber_a={"1929-09": 5.1},  # 6 values: above their mean
        unrate_nber_b={"1941-06": 6.0},  # b is chosen; a stays flat
        unrate={"1960-12": 5.0 + 1e-12, "1975-03": 4.9},
    )
    assert uf.state_at(rates, P("1929-10")) == ("U", A, 6)
    assert uf.state_at(rates, P("1929-11")) == ("N", A, 7)  # 5.0 < the mean with 5.1 in it
    assert uf.state_at(rates, P("1941-07")) == ("U", B, 12)
    assert uf.state_at(rates, P("1961-01")) == ("N", U, 12)  # equal within 1e-9: N
    assert uf.state_at(rates, P("1961-01"), at_or_above=True) == ("U", U, 12)
    assert uf.state_at(rates, P("1975-04"), at_or_above=True) == ("N", U, 12)
    # The mean uses only the chosen series' values in the window.
    rates = _rates(unrate_nber_a={"1929-04": 3.0, "1929-05": 9.0, "1929-09": 6.1})
    values = [3.0, 9.0, 5.0, 5.0, 5.0, 6.1]
    assert 6.1 > sum(values) / 6
    assert uf.state_at(rates, P("1929-10")) == ("U", A, 6)


def test_the_window_length_sets_both_steps() -> None:
    assert uf.state_at(RATES, P("1929-07"), window=6) == ("N", A, 3)  # step 2: 3 of 6
    assert uf.state_at(RATES, P("1929-06"), window=6) == ("undefined", None, 0)
    assert uf.state_at(RATES, P("1929-10"), window=6) == ("N", A, 6)
    assert uf.state_at(RATES, P("1930-04"), window=18) == ("N", A, 12)  # step 2: 12 of 18
    assert uf.state_at(RATES, P("1930-01"), window=18) == ("N", A, 9)
    assert uf.state_at(RATES, P("1929-12"), window=18) == ("undefined", None, 0)
    assert uf.state_at(RATES, P("1941-01"), window=18) == ("N", A, 18)  # b has 12 of 18
    assert uf.state_at(RATES, P("1941-07"), window=18) == ("N", B, 18)


def test_the_mean_uses_only_the_chosen_series_where_a_and_b_overlap() -> None:
    # M* 1941-12: b is chosen (12 values); a also has 12 values in the window, far higher.
    rates = _rates(
        unrate_nber_a=dict.fromkeys(_span("1941-01", "1941-12"), 10.0),
        unrate_nber_b={"1941-12": 5.5},
    )
    assert uf.state_at(rates, P("1942-01")) == ("U", B, 12)  # 5.5 > b's mean, not a and b's
    assert uf.state_table(rates).at[P("1942-01"), "state"] == "U"


def test_the_sensitivity_windows_are_their_own() -> None:
    rates = _rates(
        unrate={
            # M* 1964-12: above the 6-month mean, below the 12-month one.
            **dict.fromkeys(_span("1964-01", "1964-06"), 7.0),
            "1964-12": 5.5,
            # M* 1974-12: above the 18-month mean, below the 12-month one.
            **dict.fromkeys(_span("1973-07", "1973-12"), 3.0),
            **dict.fromkeys(_span("1974-01", "1974-11"), 5.6),
            "1974-12": 5.5,
        }
    )
    built = _built(slow=[*_span("1965-01", "1965-06"), *_span("1975-01", "1975-03")], rates=rates)
    six, eighteen = uf.timing_rows(built)
    assert six["state"] == "N"
    assert (six["late"]["main"], six["late"]["mean_6"], six["late"]["mean_18"]) == (
        "1965-07",
        "1965-01",
        "1965-07",
    )
    assert eighteen["state"] == "N"
    assert (eighteen["late"]["main"], eighteen["late"]["mean_6"], eighteen["late"]["mean_18"]) == (
        "1975-04",
        "1975-04",
        "1975-01",
    )
    summary = uf.timing_check(built)
    assert summary["both"]["changed"] == 2
    assert summary["sensitivity"]["mean_6"] == {"changed": 1, "removed": 1}
    assert summary["sensitivity"]["mean_18"] == {"changed": 1, "removed": 1}


def test_agreement_counts_the_19_months_both_nber_series_are_full() -> None:
    assert uf.agreement(RATES) == {"months": 19, "agree": 19}
    rates = _rates(unrate_nber_b={"1941-06": 6.0}, unrate_nber_a={"1942-06": 6.0})
    assert uf.agreement(rates) == {"months": 19, "agree": 17}
    rates = _rates(unrate_nber_a={"1941-02": math.nan})
    assert uf.agreement(rates)["months"] == 7  # a misses 1941-02: M* 1941-02..1942-01 drop out


# ----- the late exit c ----------------------------------------------------------------------------
def _states(*states: str, first: str = "1950-01") -> pd.Series:
    return pd.Series(list(states), index=pd.period_range(first, periods=len(states), freq="M"))


@pytest.mark.parametrize(
    ("states", "blocks", "late"),
    [
        (("U", "N", "N", "N"), False, "1950-01"),  # m is U: unchanged
        (("N", "N", "U", "N"), False, "1950-03"),
        (("N", "N", "N", "U"), False, "1950-04"),  # U at r itself does not count
        (("N", "undefined", "U", "N"), False, "1950-02"),  # undefined does not block
        (("N", "undefined", "U", "N"), True, "1950-03"),
        (("N", "undefined", "N", "N"), True, "1950-04"),
        (("undefined", "N", "N", "N"), False, "1950-01"),
    ],
)
def test_late_exit_is_the_first_u_or_undefined_month_end_before_r(
    states: tuple[str, ...], blocks: bool, late: str
) -> None:
    series = _states(*states, "U")
    assert uf.late_exit(series, P("1950-01"), P("1950-04"), blocks) == P(late)


def test_timing_rows_for_both_legs() -> None:
    rates = _rates(unrate={"1952-03": 6.0, "1956-04": 6.0})
    built = _built(
        fast=["1956-05", *_span("1958-01", "1958-02")],
        slow=[*_span("1952-01", "1952-06"), *_span("1954-01", "1954-03")],
        rates=rates,
        segments=[("bear", "1952-03", "1953-01")],
    )
    rows = uf.timing_rows(built)
    assert [(row["leg"], row["month"]) for row in rows] == [
        ("fast", "1956-05"),
        ("fast", "1958-01"),
        ("slow", "1952-01"),
        ("slow", "1954-01"),
    ]
    one, two = rows[0], rows[1]
    assert one["state"] == "U" and one["source"] == U  # M* 1956-04 is above the mean
    assert one["late"]["main"] == "1956-05" and one["late_months"]["main"] == 0
    assert one["months_out"] == 1 and one["reentry"] == "1956-06"
    assert two["state"] == "N" and two["late"]["main"] == "1958-03" == two["reentry"]
    assert two["late_months"]["main"] == 2
    turned, never = rows[2], rows[3]
    assert turned == {
        "leg": "slow",
        "month": "1952-01",
        "year": 1952,
        "complete": True,
        "reentry": "1952-07",
        "months_out": 6,
        "state": "N",
        "source": U,
        "slow_bears": ["bear"],
        "late": {
            "main": "1952-04",
            "mean_6": "1952-04",
            "mean_18": "1952-04",
            "ge": "1952-01",  # a flat rate equals its mean
            "undefined_blocks": "1952-04",
        },
        "late_months": {"main": 3, "mean_6": 3, "mean_18": 3, "ge": 0, "undefined_blocks": 3},
    }
    assert never["late"]["main"] == never["reentry"] == "1954-04"
    assert never["late_months"]["main"] == 3 and never["slow_bears"] == []


def test_undefined_month_ends_do_not_block_the_exit_in_the_main_variant() -> None:
    built = _built(slow=_span("1946-11", "1947-05"))
    (row,) = uf.timing_rows(built)
    assert (row["state"], row["source"], row["reentry"]) == ("N", B, "1947-06")
    assert row["late"]["main"] == "1947-02"  # the first undefined month end
    assert row["late"]["undefined_blocks"] == "1947-06"
    assert row["late_months"] == {
        "main": 3,
        "mean_6": 3,
        "mean_18": 3,
        "ge": 0,
        "undefined_blocks": 7,
    }


def test_an_exit_at_an_undefined_month_end_is_unchanged() -> None:
    built = _built(fast=["1929-03"], slow=_span("1947-08", "1947-09"))
    rows = uf.timing_rows(built)
    assert [row["state"] for row in rows] == ["undefined", "undefined"]
    assert [row["source"] for row in rows] == [None, None]
    assert [row["late_months"]["main"] for row in rows] == [0, 0]
    assert [row["late_months"]["undefined_blocks"] for row in rows] == [1, 2]


def test_an_exit_still_out_at_2010_is_only_counted() -> None:
    rates = _rates(unrate={"2010-08": 9.0})
    built = _built(
        slow=_span("2010-06", "2010-12"), rates=rates, segments=[("late", "2010-10", "2011-06")]
    )
    (row,) = _rows(built)
    assert row["complete"] is False
    assert row["reentry"] is None and row["months_out"] is None
    assert row["late"] == {} and row["late_months"] == {}
    assert row["trips"] == {} and row["control"] is None
    assert row["slow_bears"] == ["late"]
    assert "leg_pnl" not in row and "out_excess" not in row and "false_exit" not in row
    summary = uf.timing_check(built)
    assert summary["legs"]["slow"]["complete"] == 0 and summary["legs"]["slow"]["incomplete"] == 1
    assert uf.stats(_rows(built), leg="slow")["trips"] == 0


def test_slow_bears_are_the_segments_the_months_out_overlap() -> None:
    segments = [("ends_at_exit", "1930-01", "1931-05"), ("inside", "1931-06", "1931-06")]
    built = _built(slow=["1931-05"], segments=segments)
    (row,) = uf.timing_rows(built)
    assert row["slow_bears"] == mra.overlaps(built.segments, P("1931-05"), P("1931-06"))
    assert row["slow_bears"] == ["inside"]


# ----- the wait-k-months control ------------------------------------------------------------------
def test_control_k_is_per_leg_over_changed_trips_and_rounds_half_up() -> None:
    rates = _rates(unrate={"1962-02": 6.0, "1964-02": 6.0, "1966-01": 6.0})
    built = _built(
        fast=["1966-02", *_span("1968-01", "1968-02")],  # unchanged, then removed after 2
        slow=[
            *_span("1962-01", "1962-05"),  # the 1962-03 month end is U (M* 1962-02): c after 2
            *_span("1964-01", "1964-03"),  # c after 2, one month before r
            *_span("1966-06", "1966-09"),  # never U: removed after 4
        ],
        rates=rates,
    )
    rows = uf.timing_rows(built)
    assert [row["late_months"]["main"] for row in rows] == [0, 2, 2, 2, 4]
    # Slow: median of 2, 2, 4 is 2. Fast: only the changed trip counts.
    assert uf.control_ks(rows) == {"fast": 2, "slow": 2}
    assert mra.control_k([2, 3]) == 3  # 2.5 rounds up, as the requirement says
    rows = uf.timing_rows(_built(slow=_span("1970-01", "1970-02")))
    assert uf.control_ks(rows) == {"fast": None, "slow": 2}


def test_the_control_goes_out_at_m_plus_k_but_never_after_r() -> None:
    rates = _rates(unrate={"1972-03": 6.0})
    returns = _returns(months={"1972-02": 0.002, "1972-03": -0.001, "1975-02": 0.003})
    built = _built(
        slow=[
            *_span("1972-01", "1972-06"),  # c after 3 (the 1972-04 month end is U)
            *_span("1975-01", "1975-02"),  # never U, out 2 months: c = r after 2
            *_span("1977-01", "1977-05"),  # never U, out 5 months: removed after 5
        ],
        rates=rates,
        returns=returns,
    )
    rows = _rows(built)
    assert [row["trips"]["main"]["late_months"] for row in rows] == [3, 2, 5]
    k = 3
    assert uf.control_ks(uf.timing_rows(built)) == {"fast": None, "slow": k}
    first, short, long = rows
    assert first["control"]["late"] == "1972-04" and first["control"]["late_months"] == 3
    assert first["control"]["gain"] == pytest.approx(
        0.5 * _excess(returns, 0.0, "1972-01", "1972-04"), abs=1e-12
    )
    assert short["control"] == {**short["trips"]["main"]}
    assert short["control"]["removed"] is True and short["control"]["late"] == "1975-03"
    assert long["control"]["late"] == "1977-04" and long["control"]["removed"] is False
    assert all(
        row["control"] is None
        for row in _rows(_built(slow=["1972-01"], rates=_rates(unrate={"1971-12": 6.0})))
    )


# ----- the trip: g and g' ------------------------------------------------------------------------
def test_g_and_g_prime_are_half_the_excess_from_m_to_c() -> None:
    rates = _rates(unrate={"1980-04": 7.0})
    returns = _returns(
        months={"1980-02": -0.002, "1980-03": 0.001, "1980-04": 0.003, "1980-06": -0.001},
        base=0.0002,
    )
    built = _built(slow=_span("1980-01", "1980-06"), rates=rates, returns=returns, cash=0.0001)
    (row,) = _rows(built)
    assert (row["late"]["main"], row["reentry"]) == ("1980-05", "1980-07")
    trip = row["trips"]["main"]
    gain = 0.5 * _excess(returns, 0.0001, "1980-01", "1980-05")
    assert trip == {
        "late": "1980-05",
        "late_months": 4,
        "removed": False,
        "gain": pytest.approx(gain, abs=1e-12),
        "net": pytest.approx(gain - 0.5 * 4 * built.mu, abs=1e-12),
    }
    out = _excess(returns, 0.0001, "1980-01", "1980-07")
    assert row["out_excess"] == pytest.approx(out, abs=1e-12)
    assert row["leg_pnl"] == pytest.approx(-0.5 * out, abs=1e-12)
    assert row["false_exit"] is (out > 0)
    assert set(row["trips"]) == {"main", *uf.SENSITIVITY}


def test_an_unchanged_trip_counts_zero_and_a_removed_trip_counts_up_to_r() -> None:
    rates = _rates(unrate={"1985-12": 7.0})
    returns = _returns(months={"1986-01": 0.004, "1986-02": 0.001, "1988-02": -0.003}, base=0.0001)
    built = _built(fast=["1986-01", *_span("1988-01", "1988-02")], rates=rates, returns=returns)
    unchanged, removed = _rows(built)
    assert unchanged["trips"]["main"] == {
        "late": "1986-01",
        "late_months": 0,
        "removed": False,
        "gain": 0.0,
        "net": 0.0,
    }
    assert removed["trips"]["main"]["removed"] is True
    assert removed["trips"]["main"]["gain"] == pytest.approx(0.5 * removed["out_excess"], abs=1e-12)
    assert removed["trips"]["main"]["gain"] == pytest.approx(-removed["leg_pnl"], abs=1e-12)


# ----- the tables ---------------------------------------------------------------------------------
def _trip(gain: float, net: float, late_months: int = 2, removed: bool = False) -> dict[str, Any]:
    return {"late": "x", "late_months": late_months, "removed": removed, "gain": gain, "net": net}


def _record(
    leg: str,
    year: int,
    trip: dict[str, Any],
    *,
    state: str = "N",
    leg_pnl: float = -0.01,
    months_out: int = 2,
    bears: Sequence[str] = (),
    control: dict[str, Any] | None = None,
    complete: bool = True,
) -> dict[str, Any]:
    return {
        "leg": leg,
        "year": year,
        "complete": complete,
        "state": state,
        "months_out": months_out,
        "leg_pnl": leg_pnl,
        "false_exit": leg_pnl < 0,
        "slow_bears": list(bears),
        "trips": {"main": trip},
        "control": control,
    }


RECORDS = [
    _record("slow", 1932, _trip(0.10, 0.08, 4, True), leg_pnl=-0.06, months_out=4, bears=["b1"]),
    _record("slow", 1975, _trip(-0.04, -0.05, 2), state="N", leg_pnl=0.12, bears=["b1", "b2"]),
    _record("slow", 1990, _trip(0.0, 0.0, 0), state="U", leg_pnl=0.05, months_out=7),
    _record("fast", 1950, _trip(0.02, 0.015, 1, True), leg_pnl=-0.02, months_out=1),
    _record("fast", 1980, _trip(0.0, 0.0, 0), state="undefined", leg_pnl=-0.03, months_out=1),
    _record("fast", 2010, _trip(9.0, 9.0, 3), complete=False),
]


def test_stats_count_every_complete_trip_and_judge_only_changed_ones() -> None:
    part = uf.stats(RECORDS, leg="slow")
    assert (part["trips"], part["changed"], part["removed"]) == (3, 2, 1)
    assert part["gain"] == pytest.approx(0.06) and part["net"] == pytest.approx(0.03)
    assert part["net_positive"] == 0.5 and part["worst"] == -0.04
    assert part["late_months_sum"] == 6
    both = uf.stats(RECORDS)
    assert (both["trips"], both["changed"], both["removed"]) == (5, 3, 2)
    assert both["net"] == pytest.approx(0.045) and both["late_months_sum"] == 7
    empty = uf.stats([RECORDS[2]], leg="slow")
    assert empty["changed"] == 0 and empty["net_positive"] is None and empty["worst"] is None


def test_main_table_has_both_legs_the_pool_and_a_control_under_each() -> None:
    control = _trip(0.01, 0.005, 1)
    rows = [{**row, "control": control if row["complete"] else None} for row in RECORDS]
    table = uf.main_table(rows)
    assert set(table) == {"fast", "slow", "both", "control"}
    assert set(table["control"]) == {"fast", "slow", "both"}
    assert table["control"]["both"]["changed"] == 5
    assert table["control"]["both"]["net"] == pytest.approx(0.025)
    assert table["both"] == uf.stats(rows)


def test_strength_and_shares() -> None:
    table = {
        "fast": {"net": 0.10},
        "slow": {"net": 0.30},
        "both": {"net": 0.40, "late_months_sum": 16},
    }
    assert uf.strength(table, 0.05) == pytest.approx(0.40 / (0.5 * 0.05 * 4))
    assert uf.shares(table) == {"fast": pytest.approx(0.25), "slow": pytest.approx(0.75)}
    assert uf.strength({**table, "both": {"net": 0.4, "late_months_sum": 0}}, 0.05) is None
    assert uf.shares({**table, "both": {"net": 0.0, "late_months_sum": 16}}) == {
        "fast": None,
        "slow": None,
    }
    assert uf.shares({**table, "both": {"net": -0.1, "late_months_sum": 16}})["slow"] is None


def test_groups_by_the_state_at_the_exit() -> None:
    groups = uf.groups(RECORDS, "slow")
    assert set(groups) == {"U", "N", "undefined"}
    assert groups["N"] == {
        "trips": 2,
        "false_exits": 0.5,
        "leg_pnl": pytest.approx(0.06),
        "months_out": 3.0,
    }
    assert groups["U"]["trips"] == 1 and groups["U"]["false_exits"] == 0.0
    assert groups["undefined"] == {
        "trips": 0,
        "false_exits": None,
        "leg_pnl": 0.0,
        "months_out": None,
    }
    assert uf.groups(RECORDS, "fast")["undefined"]["trips"] == 1


def test_groups_print_the_median_months_out() -> None:
    rows = [_record("fast", 1950, _trip(0.0, 0.0, 0), months_out=m) for m in (1, 2, 9)]
    assert uf.groups(rows, "fast")["N"]["months_out"] == 2.0  # the median, not the mean 4


def test_the_slow_bear_table_counts_each_trip_once_in_the_total() -> None:
    bears = uf.bears(
        RECORDS, [("b1", P("1930-01"), P("1932-06")), ("b2", P("1975-01"), P("1975-06"))]
    )
    assert bears["segments"]["b1"]["trips"] == 2
    assert bears["segments"]["b1"]["saved"] == pytest.approx(0.12)  # only positive leg_pnl
    assert bears["segments"]["b2"]["trips"] == 1
    protection = bears["protection"]
    assert protection["trips"] == 2
    assert protection["saved"] == pytest.approx(0.12)
    assert protection["gain"] == pytest.approx(0.06)  # Σ g, not net of the mean
    assert protection["late_months"] == 6
    assert protection["line"] == pytest.approx(-0.25 * 0.12)


def test_halves_split_at_1970_by_the_exit_year() -> None:
    built = _built(slow=[*_span("1969-12", "1970-01"), *_span("1970-06", "1970-07")])
    summary = uf.timing_check(built)
    assert summary["legs"]["slow"]["halves"] == {FIRST_HALF: 1, SECOND_HALF: 1}
    assert summary["legs"]["slow"]["decades"]["1960s"] == 1
    assert summary["legs"]["slow"]["decades"]["1970s"] == 1
    assert [len(uf.in_half(RECORDS, a, b)) for _, a, b in uf.HALVES] == [2, 4]


# ----- the verdict --------------------------------------------------------------------------------
def _tables(
    *,
    changed: int = 30,
    net: float = 0.30,
    control: float = 0.10,
    first: tuple[int, float] = (15, 0.20),
    second: tuple[int, float] = (15, 0.10),
    gain: float = -0.10,
    line: float = -0.15,
) -> dict[str, Any]:
    return {
        "main": {"both": {"changed": changed, "net": net}, "control": {"both": {"net": control}}},
        "halves": {
            FIRST_HALF: {"both": {"changed": first[0], "net": first[1]}},
            SECOND_HALF: {"both": {"changed": second[0], "net": second[1]}},
        },
        "bears": {"protection": {"gain": gain, "line": line}},
    }


@pytest.mark.parametrize(
    ("changes", "verdict"),
    [
        ({}, "holds"),
        ({"changed": 19}, "too_few"),
        ({"changed": 20}, "holds"),
        ({"first": (4, 0.20)}, "too_few"),
        ({"first": (5, 0.20)}, "holds"),
        ({"second": (4, 0.10)}, "too_few"),
        ({"net": 0.0, "control": -0.10}, "not_holds"),
        ({"net": -0.01, "control": -0.10}, "not_holds"),
        ({"control": 0.30}, "not_holds"),
        ({"control": 0.2999}, "holds"),
        ({"first": (15, 0.0)}, "not_holds"),
        ({"second": (15, -0.01)}, "not_holds"),
        ({"gain": -0.1501}, "not_holds"),
        ({"gain": -0.15}, "holds"),
    ],
)
def test_verdict(changes: dict[str, Any], verdict: str) -> None:
    assert uf.judge(_tables(**changes))["verdict"] == verdict


def test_verdict_parts_and_reasons() -> None:
    assert uf.judge(_tables(changed=12, first=(3, 0.2))) == {
        "verdict": "too_few",
        "reasons": ["改了的趟 12 趟，少于 20 趟", "1929–1969 改了的趟 3 趟，少于 5 趟"],
    }
    assert uf.judge(_tables(gain=-0.2)) == {
        "verdict": "not_holds",
        "reasons": [],
        "save_ok": True,
        "control_ok": True,
        "halves_judged": {FIRST_HALF: True, SECOND_HALF: True},
        "halves_ok": True,
        "protect_ok": False,
    }


def test_readings_only_when_the_pool_holds() -> None:
    holds = {"verdict": "holds"}
    table = {"fast": {"net": 0.30}, "slow": {"net": 0.10}, "both": {"net": 0.40}}
    assert uf.readings(holds, table, 1.234) == [
        "没信息的条件约六分之一也会判 holds（复盘线程模拟，推断）；强度 z = 1.23。",
        "失业率过滤主要省在快腿。",
        "不因为一条腿的数好就只过滤那一条腿：那是看了结果的选择，要另开预注册。",
    ]
    table = {"fast": {"net": 0.50}, "slow": {"net": -0.10}, "both": {"net": 0.40}}
    assert uf.readings(holds, table, None)[:2] == [
        "没信息的条件约六分之一也会判 holds（复盘线程模拟，推断）；强度 z = —。",
        "主要省在快腿，慢腿是负的（-10.00 点）。",
    ]
    table = {"fast": {"net": 0.20}, "slow": {"net": 0.20}, "both": {"net": 0.40}}
    assert len(uf.readings(holds, table, 1.0)) == 2
    assert uf.readings({"verdict": "not_holds"}, table, 1.0) == []
    assert uf.readings({"verdict": "too_few"}, table, 1.0) == []


def test_read_judges_the_pool_once() -> None:
    assert RESULT["decision"] == uf.judge(RESULT["tables"])
    assert set(RESULT["tables"]) == {"main", "halves", "groups", "bears", "sensitivity"}
    assert set(RESULT["tables"]["halves"]) == {FIRST_HALF, SECOND_HALF}
    assert set(RESULT["tables"]["sensitivity"]) == set(uf.SENSITIVITY)
    main = RESULT["tables"]["main"]
    assert RESULT["strength"] == uf.strength(main, BUILT.sigma)
    assert RESULT["shares"] == uf.shares(main)
    assert RESULT["readings"] == uf.readings(RESULT["decision"], main, RESULT["strength"])
    assert RESULT["decision"]["verdict"] in uf.VERDICTS


# ----- check: counts and distributions only -------------------------------------------------------
def test_check_never_prices_a_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("check must not look at returns after the exit")

    for name in ("out_excess", "trip", "priced_rows"):
        monkeypatch.setattr(uf, name, forbidden)
    assert uf.timing_check(BUILT) == RESULT["check"]


def test_check_prints_no_trip_dates() -> None:
    summary = RESULT["check"]
    assert set(summary) == {
        "run_id",
        "snapshot_id",
        "sources",
        "identity_mismatches",
        "agreement",
        "states",
        "u_share",
        "series_months",
        "legs",
        "both",
        "sensitivity",
        "slow_bears",
    }
    rest = {key: value for key, value in summary.items() if key != "sources"}
    assert sorted(set(_strings(rest))) == sorted({uf.RUN_ID, uf.SNAPSHOT_ID})
    text = json.dumps(rest, ensure_ascii=False)
    for row in TRIPS:
        for value in (row["month"], row["reentry"], *row["late"].values()):
            if value is not None:
                assert f'"{value}"' not in text
    for key in ("gain", "net", "leg_pnl", "out_excess", "false_exit", "late", "month", "reentry"):
        assert f'"{key}"' not in json.dumps(summary, ensure_ascii=False)


def test_check_counts_match_the_trips() -> None:
    summary = RESULT["check"]
    assert summary["identity_mismatches"] == 0
    assert summary["slow_bears"] == len(BASE.segments)
    assert summary["sources"] == BASE.sources
    table = uf.state_table(BASE.rates)
    assert summary["states"] == {s: int((table["state"] == s).sum()) for s in uf.STATES}
    assert sum(summary["states"].values()) == 984
    assert summary["series_months"] == {s: int((table["source"] == s).sum()) for s in (U, B, A)}
    assert summary["agreement"] == uf.agreement(BASE.rates)
    assert set(summary["u_share"]) == {"all", FIRST_HALF, SECOND_HALF}
    for leg in ("fast", "slow"):
        legs = summary["legs"][leg]
        complete = [row for row in TRIPS if row["leg"] == leg and row["complete"]]
        changed = [row for row in complete if row["late_months"]["main"] > 0]
        assert legs["complete"] == len(complete) > 10
        assert legs["incomplete"] == sum(row["leg"] == leg for row in TRIPS) - len(complete)
        assert sum(legs["halves"].values()) == sum(legs["decades"].values()) == len(complete)
        assert legs["months_out"] == rs.quartiles([row["months_out"] for row in complete])
        assert sum(legs["by_state"].values()) == len(complete)
        assert legs["changed"] == len(changed) == RESULT["tables"]["main"][leg]["changed"]
        assert legs["removed"] == sum(row["trips"]["main"]["removed"] for row in changed)
        assert legs["late_months"] == rs.quartiles([row["late_months"]["main"] for row in changed])
        assert legs["k"] == mra.control_k([row["late_months"]["main"] for row in changed])
    both = summary["both"]
    assert both["changed"] == RESULT["tables"]["main"]["both"]["changed"]
    assert both["complete"] == RESULT["tables"]["main"]["both"]["trips"]
    assert both["halves_changed"] == {
        label: RESULT["tables"]["halves"][label]["both"]["changed"]
        for label in both["halves_changed"]
    }
    for name in uf.SENSITIVITY:
        assert (
            summary["sensitivity"][name]["changed"]
            == RESULT["tables"]["sensitivity"][name]["changed"]
        )


def test_check_on_flat_rates() -> None:
    built = _built(fast=["1950-03"], slow=_span("1960-01", "1960-02"))
    summary = uf.timing_check(built)
    assert summary["states"] == {"U": 0, "N": 958, "undefined": 26}
    assert summary["series_months"] == {U: 750, B: 73, A: 135}
    assert summary["u_share"] == {"all": 0.0, FIRST_HALF: 0.0, SECOND_HALF: 0.0}
    assert summary["agreement"] == {"months": 19, "agree": 19}
    assert summary["both"] == {
        "complete": 2,
        "changed": 2,
        "removed": 2,
        "halves_changed": {FIRST_HALF: 2, SECOND_HALF: 0},
    }
    assert summary["legs"]["fast"]["by_state"] == {"U": 0, "N": 1, "undefined": 0}
    assert (summary["legs"]["fast"]["k"], summary["legs"]["slow"]["k"]) == (1, 2)
    assert summary["sensitivity"]["ge"] == {"changed": 0, "removed": 0}


def test_u_share_is_over_defined_month_ends_by_year() -> None:
    summary = uf.timing_check(_built(rates=_rates(unrate={"1980-06": 6.0, "2010-05": 6.0})))
    assert summary["states"] == {"U": 2, "N": 956, "undefined": 26}
    assert summary["u_share"] == {
        "all": pytest.approx(2 / 958),
        FIRST_HALF: 0.0,
        SECOND_HALF: pytest.approx(2 / 492),
    }


# ----- the whole read -----------------------------------------------------------------------------
def test_the_read_has_every_part_and_dumps_as_json() -> None:
    assert RESULT["run_id"] == uf.RUN_ID and RESULT["owner_decision"] == uf.OWNER_DECISION
    assert RESULT["snapshot_id"] == uf.SNAPSHOT_ID
    assert (RESULT["mu"], RESULT["sigma"]) == (BUILT.mu, BUILT.sigma)
    assert RESULT["check"] == uf.timing_check(BUILT)
    assert len(TRIPS) == sum(len(exits) for exits in BUILT.exits.values())
    assert [list(row) for row in TRIPS] == [sorted(row) for row in TRIPS]
    json.dumps(RESULT, allow_nan=False)
    json.dumps(TRIPS, allow_nan=False)


def test_data_after_2010_never_matter() -> None:
    later = {}
    for source, frame in FRAMES.items():
        frame = frame.copy()
        after = frame.index > pd.Timestamp("2010-12-31")
        frame.loc[after] = frame.loc[after] * 3.0 - 0.01
        later[source] = frame
    assert uf.read(_inputs(later)) == (RESULT, TRIPS)


def test_report_sentences() -> None:
    text = uf.render({**RESULT, "trips": TRIPS, "provenance": {"commit": "abc123"}})
    first_screen = text.split("## 按出去那个月末的状态分组")[0]
    assert "两条腿合起来判一次；每条腿单独的数、对照和敏感度不判。" in first_screen
    assert f"`{RESULT['decision']['verdict']}`" in first_screen
    for heading in ("## 判定", "## 主表", "## 两半", "## 慢熊表"):
        assert heading in first_screen
    assert "强度 z（只印）" in first_screen and "份额（只印）" in first_screen
    assert uf.SNAPSHOT_ID in text and RESULT["rules_sha256"] in text and "abc123" in text
    order = [
        "## 主表",
        "## 两半",
        "## 慢熊表",
        "## 按出去那个月末的状态分组（只印）",
        "## 敏感度（只印，不判）",
        "## 慢腿每趟",
        "## 数据和核对",
        "## 局限",
    ]
    assert [text.index(heading) for heading in order] == sorted(text.index(h) for h in order)
    slow_table = text.split("## 慢腿每趟")[1].split("## 数据和核对")[0]
    slow = [row for row in TRIPS if row["leg"] == "slow"]
    assert sum(1 for line in slow_table.splitlines() if line.startswith("| ")) == len(slow) + 1
    unfinished = [row for row in slow if not row["complete"]]
    assert unfinished
    for row in unfinished:
        assert f"| {row['month']} | 未完 | — | — | {row['state']} | — | — | — |" in slow_table
    decision, protection = RESULT["decision"], RESULT["tables"]["bears"]["protection"]
    yes = {True: "是", False: "否"}
    assert (
        f"  - 省下：Σ g′ > 0 {yes[decision['save_ok']]}；大于对照 {yes[decision['control_ok']]}。"
        f"两半 {yes[decision['halves_ok']]}。不伤慢熊：Σ g {protection['gain'] * 100:+.2f} ≥ "
        f"{protection['line'] * 100:+.2f} {yes[decision['protect_ok']]}。"
    ) in first_screen
    for sentence in (
        "只量博客规则晚出去的一半，早回来那一半没测。",
        "没信息时约 17% 也会判 holds（复盘线程模拟，推断）。",
        "1947 年到 1948 年中不定。",
        "不换算成每年多少点，不和博客的数并排比；g 是 0.5 × 超额，即一条腿的那一半。",
        "不是长牛代价的读数",
        "production_effect=none；broker_action=none。",
    ):
        assert sentence in text, sentence


def test_report_verdict_lines() -> None:
    too_few = {"verdict": "too_few", "reasons": ["改了的趟 12 趟，少于 20 趟"]}
    text = uf.render({**RESULT, "trips": TRIPS, "decision": too_few, "readings": []})
    assert "`too_few`，改了的趟太少，不判。" in text and "改了的趟 12 趟，少于 20 趟" in text
    holds = {**uf.judge(_tables()), "verdict": "holds"}
    readings = ["没信息的条件约六分之一也会判 holds（复盘线程模拟，推断）；强度 z = 1.50。"]
    text = uf.render({**RESULT, "trips": TRIPS, "decision": holds, "readings": readings})
    assert "`holds`，成立：失业率趋势挑掉的多半是假退出，晚出去省下的比丢的多。" in text
    assert readings[0] in text
    assert "不伤慢熊" in text


# ----- files and commands -------------------------------------------------------------------------
def test_files(tmp_path: Path) -> None:
    uf.write_check(RESULT["check"], tmp_path / "c")
    assert json.loads((tmp_path / "c" / uf.CHECK).read_text(encoding="utf-8")) == RESULT["check"]
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [uf.CHECK]
    uf.write_result(RESULT, TRIPS, tmp_path / "r")
    lines = (tmp_path / "r" / uf.TRIPS).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == TRIPS
    assert json.loads((tmp_path / "r" / uf.SUMMARY).read_text(encoding="utf-8")) == RESULT
    assert (tmp_path / "r" / uf.REPORT).read_text(encoding="utf-8") == uf.render(
        {**RESULT, "trips": TRIPS}
    )


def test_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(uf, "load_inputs", lambda: BASE)
    monkeypatch.setattr(uf, "git_provenance", lambda root: {"commit": "abc123"})
    runner = CliRunner()

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("check must not look at returns after the exit")

    with pytest.MonkeyPatch.context() as patch:
        for name in ("out_excess", "trip", "priced_rows"):
            patch.setattr(uf, name, forbidden)
        out = runner.invoke(uf.app, ["check", "--out", str(tmp_path / "c")])
    assert out.exit_code == 0, out.output
    legs = RESULT["check"]["legs"]
    assert (
        f"unemployment_filter_table: 完整的趟 快腿 {legs['fast']['complete']}、"
        f"慢腿 {legs['slow']['complete']}；改了的 快腿 {legs['fast']['changed']}、"
        f"慢腿 {legs['slow']['changed']}；对照 k 快腿 {legs['fast']['k']:g}、"
        f"慢腿 {legs['slow']['k']:g}"
    ) in out.output
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [uf.CHECK]
    out = runner.invoke(uf.app, ["run", "--out", str(tmp_path / "r")])
    assert out.exit_code == 0, out.output
    assert f"unemployment_filter_table: 判定 {RESULT['decision']['verdict']}" in out.output
    assert sorted(p.name for p in (tmp_path / "r").iterdir()) == sorted(
        [uf.CHECK, uf.TRIPS, uf.SUMMARY, uf.REPORT]
    )
    written = json.loads((tmp_path / "r" / uf.SUMMARY).read_text(encoding="utf-8"))
    assert written == {**RESULT, "provenance": {"commit": "abc123"}}
    assert (tmp_path / "r" / uf.CHECK).read_bytes() == (tmp_path / "c" / uf.CHECK).read_bytes()
    assert "abc123" in (tmp_path / "r" / uf.REPORT).read_text(encoding="utf-8")


def test_commands_stop_when_loading_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def broken() -> uf.Inputs:
        raise ValueError("no snapshot\nsecond line")

    monkeypatch.setattr(uf, "load_inputs", broken)
    for command in ("check", "run"):
        out = CliRunner().invoke(uf.app, [command, "--out", str(tmp_path / command)])
        assert out.exit_code == 1
        assert "unemployment_filter_table: 数据载入失败：no snapshot" in out.output
        assert "second line" not in out.output
        assert not (tmp_path / command).exists()
    monkeypatch.setattr(uf, "load_inputs", lambda: BASE)
    monkeypatch.setattr(
        rs, "market_series", lambda factors: (_ for _ in ()).throw(ValueError("bad market"))
    )
    for command in ("check", "run"):
        out = CliRunner().invoke(uf.app, [command, "--out", str(tmp_path / f"m{command}")])
        assert out.exit_code == 1
        assert "unemployment_filter_table: 建数据失败：bad market" in out.output
        assert not (tmp_path / f"m{command}").exists()
