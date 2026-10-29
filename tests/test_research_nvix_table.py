"""Acceptance tests for the NVIX print-only table (TRADING-2565-92).

owner_decision:TRADING-2565:2026-10-29:print_only_tables, charter p6 sec. 23, the fourth of four
print-only tables.
First pass: the rank correlation of Manela's news implied volatility (NVIX) with the same month's
realized volatility of the US market, 1929-2010; 0.8 or more is only_vol. Second pass: the residual
of log NVIX on log realized volatility, its expanding percentile at M - 1 as state H, read on the
ten slow bears and seven long bulls as the fifth period's 3.2 state table did; the raw NVIX state
R, a block-shuffled control and the lead over F0001's slow leg are printed only. Not a probe, not a
trial, no ledger, no new rule's position line, not a long-bull cost reading.

Synthetic data only: no snapshot is read. The fake signal snapshot stands in for S4, the snapshot
with NVIX; the QQQ-Q3 preregistration, the trial gate, the research screen's sources and the
bench's signals are the repository's own files. runup_state_table, state_table, policy_table and
cycle_power are imported, not changed.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.sources import SourceSpec
from ai_trading_system.research_data import signal_sources
from ai_trading_system.research_data.signal_sources import SignalSnapshot
from ai_trading_system.research_probes import cycle_power as cp
from ai_trading_system.research_probes import nvix_table as nv
from ai_trading_system.research_probes import policy_table as pt
from ai_trading_system.research_probes import runup_state_table as rs
from ai_trading_system.research_probes import state_table as st

# S4: the snapshot the research session fetched after TRADING-2565-91.
S4 = "sig-d5cea4795c30"
SOURCE = Path(nv.__file__).read_text(encoding="utf-8")
PREREG = Path(__file__).resolve().parents[1] / "research" / "QQQ-Q3" / "prereg.yaml"
UNCHANGED = {
    rs: "610c76cc5ae34a92b4f1aa3f04614dc4321c8394be3567613941421b219e3c97",
    st: "0a39bc86e58495a3af598c7f73abc0da6c988015d26be649115f56126eca8a22",
    pt: "2a8b550855ebc43b0ccaf7c81f49efa46071b7a518027455a568813744b5a6ca",
    cp: "53e5286fec515c99dcb264f4ce8959afb4c4c54d970ab24b2df5d4c3843efafc",
}
FIRST_HALF, SECOND_HALF = "1929–1969", "1970–2010"
DECADES = (
    ("1929–1939", 1929, 1939),
    ("1940–1949", 1940, 1949),
    ("1950–1959", 1950, 1959),
    ("1960–1969", 1960, 1969),
    ("1970–1979", 1970, 1979),
    ("1980–1989", 1980, 1989),
    ("1990–1999", 1990, 1999),
    ("2000–2010", 2000, 2010),
)
SLOW_IDS = [
    "slow_bear_192908",
    "slow_bear_193208",
    "slow_bear_193702",
    "slow_bear_193909",
    "slow_bear_194605",
    "slow_bear_196112",
    "slow_bear_196811",
    "slow_bear_197212",
    "slow_bear_200003",
    "slow_bear_200710",
]
BULL_IDS = [
    "long_bull_193303",
    "long_bull_194205",
    "long_bull_194706",
    "long_bull_196207",
    "long_bull_197410",
    "long_bull_198712",
    "long_bull_200210",
]


def P(text: str) -> Any:
    """A month, typed loosely so it can index the series below."""

    return pd.Period(text, freq="M")


def _span(first: str, last: str) -> list[str]:
    return [str(p) for p in pd.period_range(first, last, freq="M")]


# ----- hand-built months --------------------------------------------------------------------------
ALL = pd.period_range("1889-07", "2010-12", freq="M")
RESID = pd.period_range("1926-07", "2010-12", freq="M")
ENDS = pd.period_range("1929-01", "2010-12", freq="M")
DAYS = pd.bdate_range("1926-07-01", "2010-12-31")
MONTH_OF = DAYS.to_period("M")


def _state(
    on: Iterable[str] = (), undefined: Iterable[str] = (), first: str = "1929-01"
) -> pd.Series:
    """A month-end state over 1929-01..2010-12: 1.0 when listed, NaN before first or listed."""

    state = pd.Series(0.0, index=ENDS)
    state[ENDS < P(first)] = np.nan
    for month in on:
        state[P(month)] = 1.0
    for month in undefined:
        state[P(month)] = np.nan
    return state


def _pair(seed: int = 3) -> tuple[pd.Series, pd.Series]:
    """NVIX over 1889-07..2010-12 and RV over 1926-07..2010-12, related but far from equal."""

    rng = np.random.default_rng(seed)
    rv = pd.Series(np.exp(rng.normal(2.6, 0.4, len(RESID))), index=RESID)
    nvix = pd.Series(np.exp(rng.normal(3.0, 0.2, len(ALL))), index=ALL)
    nvix[RESID] = nvix[RESID] * rv**0.3
    return nvix, rv


NVIX, RV = _pair()


Values = Sequence[float] | pd.Series | np.ndarray[Any, Any]


def _rank_corr(x: Values, y: Values) -> float:
    frame = pd.DataFrame({"x": np.asarray(x, dtype=float), "y": np.asarray(y, dtype=float)})
    frame = frame.dropna()
    return float(np.corrcoef(frame["x"].rank(), frame["y"].rank())[0, 1])


def _months_rho(first: str, last: str, x: pd.Series = NVIX, y: pd.Series = RV) -> float:
    months = pd.period_range(first, last, freq="M")
    return _rank_corr(x.reindex(months), y.reindex(months))


def _factors(
    months: Mapping[str, float] | None = None,
    drop: Iterable[str] = (),
    last: str = "2010-12-31",
) -> pd.DataFrame:
    """Daily factors in the snapshot's decimals: Mkt-RF + RF is ± a month's size on alternate days,
    or exactly the listed size on every day of a listed month; listed days are dropped."""

    days = pd.bdate_range("1926-07-01", last)
    month_of = days.to_period("M")
    ordinal = (month_of.year * 12 + month_of.month).to_numpy()
    size = 0.004 + 0.012 * ((ordinal * 7919) % 97) / 97.0
    sign = np.where(np.arange(len(days)) % 2 == 0, 1.0, -1.0)
    total = size * sign
    for month, value in (months or {}).items():
        total[month_of == P(month)] = value
    frame = pd.DataFrame(
        {"Mkt-RF": total - 0.0001, "SMB": 0.0, "HML": 0.0, "RF": 0.0001}, index=days
    )
    return frame.drop(index=[pd.Timestamp(day) for day in drop])


def _squares(frame: pd.DataFrame, month: str, *, demean: bool = False) -> float:
    total = (frame["Mkt-RF"] + frame["RF"])[
        pd.DatetimeIndex(frame.index).to_period("M") == P(month)
    ]
    logs = np.log1p(total.to_numpy(dtype=float))
    if demean:
        logs = logs - logs.mean()
    return float(np.sum(logs**2))


def _nvix_frame(
    values: Mapping[str, float] | None = None, series: pd.Series = NVIX, last: str = "2016-03"
) -> pd.DataFrame:
    """A Manela-style frame (month-start dates, column value) from 1889-07 to last."""

    dates = pd.date_range("1889-07-01", f"{last}-01", freq="MS")
    months = dates.to_period("M")
    base = series.reindex(months).to_numpy(dtype=float, copy=True)
    base[np.isnan(base)] = 25.0
    frame = pd.DataFrame({"value": base}, index=dates)
    for month, value in (values or {}).items():
        frame.loc[pd.Timestamp(f"{month}-01"), "value"] = value
    return frame


def _targets(slow_out: Iterable[str] = ()) -> pd.DataFrame:
    """Month-end rows like month_targets: the slow leg is 0 in its listed months, 1 elsewhere."""

    out = set(slow_out)
    last = pd.Series(np.arange(len(DAYS)), index=DAYS).groupby(MONTH_OF).tail(1)
    index = pd.DatetimeIndex(last.index).to_period("M")
    slow = np.array([0.0 if str(p) in out else 1.0 for p in index])
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


def _built(
    nvix: pd.Series | None = None,
    factors: pd.DataFrame | None = None,
    slow_out: Iterable[str] = (),
) -> nv.Built:
    return nv.assemble(
        nvix if nvix is not None else nv.nvix_months(_nvix_frame()),
        factors if factors is not None else _factors(),
        _targets(slow_out),
        BASE.slow,
        BASE.bull,
        [],
    )


# ----- a whole snapshot: daily market factors with volatility regimes, NVIX partly following them
def _frames(seed: int = 7) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    days = pd.bdate_range("1926-07-01", "2012-06-29")
    months = pd.period_range("1926-07", "2012-06", freq="M")
    vol = pd.Series(np.exp(rng.normal(0.0, 0.3, len(months))), index=months)
    market = rng.normal(0.0003, 0.01, len(days)) * vol.reindex(days.to_period("M")).to_numpy()
    factors = pd.DataFrame(
        {"Mkt-RF": market - 0.0001, "SMB": 0.0, "HML": 0.0, "RF": 0.0001}, index=days
    )
    dates = pd.date_range("1889-07-01", "2016-03-01", freq="MS")
    nvix = pd.Series(20.0 * np.exp(rng.normal(0.0, 0.2, len(dates))), index=dates.to_period("M"))
    shared = vol.index.intersection(nvix.index)
    nvix[shared] = nvix[shared] * vol[shared] ** 0.5
    frame = pd.DataFrame({"value": nvix.to_numpy()}, index=dates)
    frame.loc[pd.Timestamp("1992-01-01"), "value"] = np.nan
    return {cp.FACTORS: factors, nv.NVIX: frame}


def _spec(
    source: str,
    provider: Literal["manela", "ken_french"],
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
    _spec(nv.NVIX, "manela", "nvix", lag=5),
)


def _snapshot(
    frames: Mapping[str, pd.DataFrame], specs: Sequence[SourceSpec] = SPECS
) -> SignalSnapshot:
    return SignalSnapshot(nv.SNAPSHOT_ID, dict(frames), tuple(specs))


def _inputs(frames: Mapping[str, pd.DataFrame]) -> nv.Inputs:
    snapshot = _snapshot(frames)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
        return nv.load_inputs()


FRAMES = _frames()
BASE = _inputs(FRAMES)
BUILT = nv.build(BASE)
RESULT, MONTH_ROWS = nv.read(BASE)
E_BASE = nv.residuals(BUILT.nvix, BUILT.rv)[0]
H_BASE, R_BASE = nv.h_state(E_BASE), nv.r_state(BUILT.nvix)


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [text for item in value.values() for text in _strings(item)]
    if isinstance(value, list):
        return [text for item in value for text in _strings(item)]
    return []


def _forbidden(*args: Any, **kwargs: Any) -> Any:
    raise AssertionError("check must not read a rank correlation, a slope or a segment")


CHECK_FORBIDDEN = (
    "spearman",
    "pearson",
    "first_stage",
    "expanding_residuals",
    "slow_rows",
    "bull_rows",
    "line",
    "read_state",
    "shuffled",
    "null",
    "sensitivity",
)


# ----- constants: charter p6 sec. 23.4 and the requirement's sections 3 and 4, fixed before the run
def test_the_design_is_the_one_fixed_before_the_run() -> None:
    assert nv.RUN_ID == "TRADING-2565-92"
    assert nv.OWNER_DECISION == "owner_decision:TRADING-2565:2026-10-29:print_only_tables"
    assert nv.SNAPSHOT_ID == S4 != st.SNAPSHOT_ID
    assert st.PLAN_ID == "T0008"
    assert (nv.FACTORS, nv.NVIX) == ("ff_factors_daily", "nvix_monthly")
    assert nv.NVIX_SPEC == ("manela", "nvix", "monthly", 0, 5)
    assert nv.FACTORS_SPEC == ("ken_french", "F-F_Research_Data_Factors_daily", "daily", 0, 0)
    assert (nv.NVIX_FIRST, nv.RESID_FIRST, nv.FIRST_MONTH, nv.LAST_MONTH) == (
        P("1889-07"),
        P("1926-07"),
        P("1929-01"),
        P("2010-12"),
    )
    assert nv.SHUFFLE_FIRST == P("1929-07")
    assert nv.QUESTION == "QQQ-Q3"
    assert nv.RULES == {"f0001": "fast_slow", "fast": "fast_only", "slow": "slow_only"}
    assert nv.LEG == "slow"
    assert (nv.MIN_DAYS, nv.MIN_RESID, nv.MIN_RAW) == (15, 36, 120)
    assert (nv.PERCENTILE, nv.SENSITIVE_PERCENTILES, nv.ONLY_VOL) == (0.8, (0.7, 0.9), 0.8)
    assert (nv.BEFORE_MONTHS, nv.LINE_SLOW, nv.LINE_BULL) == (6, 8, 0.5)
    assert (nv.BLOCK, nv.DRAWS, nv.SEED, nv.P_LINE, nv.NULL_NOTE) == (12, 1000, 20261029, 0.05, 0.1)
    assert nv.HALVES == ((FIRST_HALF, 1929, 1969), (SECOND_HALF, 1970, 2010))
    assert nv.DECADES == DECADES
    assert nv.PARTS == (
        ("back_cast", P("1889-07"), P("1985-12")),
        ("test", P("1986-01"), P("1995-12")),
        ("train", P("1996-01"), P("2009-12")),
        ("update", P("2010-01"), P("2010-12")),
    )
    assert nv.RHO_PARTS == (
        ("back_cast", P("1929-01"), P("1985-12")),
        ("test", P("1986-01"), P("1995-12")),
        ("train", P("1996-01"), P("2009-12")),
    )
    assert nv.WITHOUT_LAST == P("2009-12")
    assert nv.STATES == ("H", "R")
    assert nv.VERDICTS == ("only_vol", "holds", "not_holds")
    assert (nv.CHECK, nv.MONTHS, nv.SUMMARY, nv.REPORT) == (
        "nvix_check.json",
        "nvix_months.jsonl",
        "nvix_table.json",
        "nvix_table.md",
    )


def test_imported_modules_are_unchanged() -> None:
    for module, digest in UNCHANGED.items():
        assert hashlib.sha256(Path(str(module.__file__)).read_bytes()).hexdigest() == digest


def test_source_reuses_the_bench_and_the_state_table() -> None:
    for line in (
        "from ai_trading_system.research_probes import runup_state_table as rs",
        "from ai_trading_system.research_probes import state_table as st",
        "from ai_trading_system.research_probes import policy_table as pt",
        "from ai_trading_system.research_probes import cycle_power as cp",
    ):
        assert line in SOURCE
    for call in (
        "rs.cut(",
        "rs.market_series(",
        "rs.month_targets(",
        "rs.leg_exits(",
        "st.expanding_percentile(",
        "st.marks(",
        "st.nearest_switch(",
        "pt.market_window(",
        "trial.gate_problems(",
    ):
        assert call in SOURCE, call
    for name in (
        "def month_targets",
        "def leg_exits",
        "def market_series",
        "def expanding_percentile",
        "def marks",
        "def nearest_switch",
        "def signal_exposure",
        "def simulate",
    ):
        assert f"{name}(" not in SOURCE
    # Requirement sec. 1: no carry-forward, no categories, nothing after 2010, no position line.
    for text in (
        "known_month_end",
        "monthly_known_at",
        "check_problems",
        "2011-",
        "qqq_long_history",
        "simulate(",
        "forward_ledger",
        "Government",
        "openpyxl",
        "requests",
    ):
        assert text not in SOURCE, text
    assert SOURCE.count("load_snapshot") == 1
    imports = [line for line in SOURCE.splitlines() if line.startswith(("import ", "from "))]
    for banned in ("strategies", "screen", "forward", "qqq", "scipy", "statsmodels"):
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
    inputs = nv.load_inputs()
    assert asked == [nv.SNAPSHOT_ID]
    assert inputs.factors.index.min() == pd.Timestamp("1926-07-01")
    assert inputs.factors.index.max() == pd.Timestamp("2010-12-31")
    assert (inputs.nvix.index[0], inputs.nvix.index[-1]) == (P("1889-07"), P("2010-12"))
    frame = FRAMES[nv.NVIX]
    assert inputs.nvix[P("2010-12")] == frame.loc[pd.Timestamp("2010-12-01"), "value"]
    assert inputs.nvix[P("1889-07")] == frame.loc[pd.Timestamp("1889-07-01"), "value"]
    assert math.isnan(inputs.nvix[P("1992-01")])
    assert {name: entry.id for name, entry in inputs.entries.items()} == nv.RULES
    assert inputs.rules_sha256 == hashlib.sha256(PREREG.read_bytes()).hexdigest()
    # The US market's slow bears and long bulls of the gate's development window (p5 3.2).
    assert [name for name, _, _ in inputs.slow] == SLOW_IDS
    assert [name for name, _, _ in inputs.bull] == BULL_IDS
    assert inputs.slow[0][1:] == (P("1929-09"), P("1932-06"))
    assert inputs.slow[-1][1:] == (P("2007-11"), P("2009-02"))
    assert inputs.bull[0][1:] == (P("1933-03"), P("1937-02"))
    assert all(start <= end for _, start, end in (*inputs.slow, *inputs.bull))
    assert inputs.sources == [
        {"source": nv.NVIX, "first": "1889-07", "last": "2010-12", "months": 1457},
        {
            "source": nv.FACTORS,
            "first": "1926-07-01",
            "last": "2010-12-31",
            "days": len(pd.bdate_range("1926-07-01", "2010-12-31")),
        },
    ]


@pytest.mark.parametrize(
    ("source", "spec"),
    [
        (nv.NVIX, _spec(nv.NVIX, "manela", "nvix", lag=4)),
        (nv.NVIX, _spec(nv.NVIX, "manela", "nvix", lag=5, release=1)),
        (cp.FACTORS, _spec(cp.FACTORS, "ken_french", "F-F_Research_Data_Factors_daily", lag=1)),
        (cp.FACTORS, _spec(cp.FACTORS, "ken_french", "49_Industry_Portfolios_daily")),
    ],
)
def test_load_inputs_stops_on_a_wrong_spec(
    monkeypatch: pytest.MonkeyPatch, source: str, spec: SourceSpec
) -> None:
    specs = tuple(spec if s.source_id == source else s for s in SPECS)
    snapshot = _snapshot(FRAMES, specs)
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    with pytest.raises(ValueError, match=f"^{source}: spec"):
        nv.load_inputs()


@pytest.mark.parametrize("source", [nv.NVIX, cp.FACTORS])
def test_load_inputs_stops_on_a_missing_spec(monkeypatch: pytest.MonkeyPatch, source: str) -> None:
    snapshot = _snapshot(FRAMES, [s for s in SPECS if s.source_id != source])
    monkeypatch.setattr(signal_sources, "load_snapshot", lambda root, snapshot_id: snapshot)
    with pytest.raises(ValueError, match=f"^{source}: no spec in the snapshot"):
        nv.load_inputs()


def test_the_configured_sources_are_the_ones_the_module_expects() -> None:
    configured = {spec.source_id: spec for spec in load_screen_settings().settings.sources}
    assert nv.spec_problems(configured) == []
    assert nv.spec_problems({spec.source_id: spec for spec in SPECS}) == []


def test_nvix_months_cut_2010_before_the_checks_and_refuse_bad_values() -> None:
    frame = _nvix_frame({"1992-01": math.nan, "2012-05": -1.0, "2016-03": math.inf})
    later = pd.concat([frame, frame.iloc[-1:]])
    months = nv.nvix_months(later)
    assert (months.index[0], months.index[-1], len(months)) == (P("1889-07"), P("2010-12"), 1458)
    assert math.isnan(months[P("1992-01")])
    assert months[P("1992-02")] == frame.loc[pd.Timestamp("1992-02-01"), "value"]
    short = nv.nvix_months(frame.iloc[24:])
    assert (short.index[0], short.isna().sum()) == (P("1889-07"), 25)
    for bad in (0.0, -0.5, math.inf, -math.inf):
        with pytest.raises(ValueError, match="^NVIX values must be positive and finite$"):
            nv.nvix_months(_nvix_frame({"1950-06": bad}))
    with pytest.raises(ValueError, match="^NVIX has a repeated month$"):
        nv.nvix_months(pd.concat([frame.iloc[:5], frame.iloc[4:5]]))


# ----- realized volatility: 100 x sqrt(12 x sum of squared daily log returns), 15 days or more ----
def test_rv_is_100_root_12_sum_of_squared_daily_log_returns() -> None:
    factors = _factors({"1950-03": 0.01})
    rv = nv.realized_vol(factors)
    days = int((MONTH_OF == P("1950-03")).sum())
    assert rv[P("1950-03")] == pytest.approx(100.0 * math.log1p(0.01) * math.sqrt(12.0 * days))
    for month in ("1926-07", "1929-01", "1987-10", "2010-12"):
        expected = 100.0 * math.sqrt(12.0 * _squares(factors, month))
        assert rv[P(month)] == pytest.approx(expected, rel=1e-12)
    assert rv.name == "rv"
    assert list(rv.index) == list(RESID)
    assert list(nv.realized_vol(_factors(last="2012-06-29")).index) == list(RESID)


def test_rv_demeaned_and_over_two_months() -> None:
    factors = _factors({"1950-03": 0.01})
    demeaned = nv.realized_vol(factors, demean=True)
    assert demeaned[P("1950-03")] == pytest.approx(0.0, abs=1e-9)
    expected = 100.0 * math.sqrt(12.0 * _squares(factors, "1960-05", demean=True))
    assert demeaned[P("1960-05")] == pytest.approx(expected, rel=1e-12)
    two = nv.realized_vol(factors, span=2)
    both = _squares(factors, "1950-02") + _squares(factors, "1950-03")
    assert two[P("1950-03")] == pytest.approx(100.0 * math.sqrt(6.0 * both), rel=1e-12)
    assert math.isnan(two[P("1926-07")])
    assert list(two.index) == list(RESID)


def test_a_month_with_15_days_counts_and_one_with_14_is_missing() -> None:
    march = [str(day.date()) for day in DAYS[MONTH_OF == P("1950-03")]]
    april = [str(day.date()) for day in DAYS[MONTH_OF == P("1950-04")]]
    factors = _factors(drop=[*march[15:], *april[14:]])
    rv = nv.realized_vol(factors)
    assert rv[P("1950-03")] == pytest.approx(100.0 * math.sqrt(12.0 * _squares(factors, "1950-03")))
    assert math.isnan(rv[P("1950-04")])
    assert int(rv.isna().sum()) == 1
    assert math.isnan(nv.realized_vol(factors, demean=True)[P("1950-04")])
    two = nv.realized_vol(factors, span=2)
    assert math.isnan(two[P("1950-04")]) and math.isnan(two[P("1950-05")])
    both = _squares(factors, "1950-02") + _squares(factors, "1950-03")
    assert two[P("1950-03")] == pytest.approx(100.0 * math.sqrt(6.0 * both))


# ----- first pass ---------------------------------------------------------------------------------
def test_rho_is_the_rank_correlation_of_the_1929_to_2010_pairs() -> None:
    nvix, rv = NVIX.copy(), RV.copy()
    nvix[P("1950-01")] = np.nan
    rv[P("1960-01")] = np.nan
    first = nv.first_stage(nvix, rv)
    assert first["rho"] == pytest.approx(_months_rho("1929-01", "2010-12", nvix, rv), abs=1e-12)
    assert first["months"] == 982
    assert first["only_vol"] is False
    moved_nvix, moved_rv = nvix.copy(), rv.copy()
    moved_nvix[P("1928-12")] = 1e6
    moved_nvix[P("1900-01")] = 1e-6
    moved_rv[P("1927-01")] = 1e6
    assert nv.first_stage(moved_nvix, moved_rv) == first


def test_the_first_pass_prints_halves_decades_parts_and_the_next_month() -> None:
    first = nv.first_stage(NVIX, RV)
    assert set(first) == {
        "rho",
        "months",
        "only_vol",
        "halves",
        "decades",
        "log_pearson",
        "next_month",
        "parts",
        "without_2010",
    }
    assert first["halves"] == pytest.approx(
        {
            FIRST_HALF: _months_rho("1929-01", "1969-12"),
            SECOND_HALF: _months_rho("1970-01", "2010-12"),
        }
    )
    assert first["decades"] == pytest.approx(
        {label: _months_rho(f"{a}-01", f"{b}-12") for label, a, b in DECADES}
    )
    assert first["parts"] == pytest.approx(
        {
            "back_cast": _months_rho("1929-01", "1985-12"),
            "test": _months_rho("1986-01", "1995-12"),
            "train": _months_rho("1996-01", "2009-12"),
        }
    )
    assert first["without_2010"] == pytest.approx(_months_rho("1929-01", "2009-12"))
    logs = np.corrcoef(np.log(NVIX[ENDS]), np.log(RV[ENDS]))[0, 1]
    assert first["log_pearson"] == pytest.approx(float(logs))
    now = NVIX[pd.period_range("1929-01", "2010-11", freq="M")].to_numpy()
    after = RV[pd.period_range("1929-02", "2010-12", freq="M")].to_numpy()
    assert first["next_month"] == pytest.approx(_rank_corr(now, after))


def test_only_vol_is_a_rho_of_0_8_or_more_at_full_precision(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    same = NVIX.copy()
    same[RESID] = RV * 1.5
    first = nv.first_stage(same, RV)
    assert first["rho"] == pytest.approx(1.0) and first["only_vol"] is True
    monkeypatch.setattr(nv, "spearman", lambda *args: 0.7996)
    assert nv.first_stage(NVIX, RV)["only_vol"] is False
    monkeypatch.setattr(nv, "spearman", lambda *args: 0.8)
    assert nv.first_stage(NVIX, RV)["only_vol"] is True


def test_a_rank_correlation_needs_three_pairs() -> None:
    few = pd.Series(np.nan, index=ALL)
    few[P("1950-01")], few[P("1950-02")] = 1.0, 2.0
    assert nv.spearman(few, RV, P("1929-01"), P("2010-12")) is None
    assert nv.pearson(few, RV, P("1929-01"), P("2010-12")) is None
    few[P("1950-03")] = 3.0
    assert nv.spearman(few, RV, P("1929-01"), P("2010-12")) == pytest.approx(
        _rank_corr([1.0, 2.0, 3.0], RV[[P("1950-01"), P("1950-02"), P("1950-03")]])
    )


# ----- second pass: the residual and the states ---------------------------------------------------
def test_the_residual_is_least_squares_of_log_n_on_log_rv_from_1926_07() -> None:
    a, b, n = nv.fit(NVIX, RV)
    want_b, want_a = np.polyfit(np.log(RV[RESID]), np.log(NVIX[RESID]), 1)
    assert a == pytest.approx(want_a) and b == pytest.approx(want_b) and n == 1014
    e, slope, pairs = nv.residuals(NVIX, RV)
    assert (slope, pairs) == (b, n)
    assert list(e.index) == list(RESID)
    expected = np.log(NVIX[RESID]) - (a + b * np.log(RV[RESID]))
    assert np.allclose(e.to_numpy(), expected.to_numpy(), rtol=0, atol=1e-12)
    earlier = NVIX.copy()
    earlier[P("1926-06")] = 1e6
    assert nv.fit(earlier, RV) == (a, b, n)
    gap = NVIX.copy()
    gap[P("1927-03")] = np.nan
    assert nv.fit(gap, RV)[2] == 1013
    assert math.isnan(nv.residuals(gap, RV)[0][P("1927-03")])
    moved = NVIX.copy()
    moved[P("1926-08")] = 1e3
    assert nv.fit(moved, RV)[1] != pytest.approx(b)


def test_residuals_can_fit_up_to_an_earlier_month() -> None:
    a, b, n = nv.fit(NVIX, RV, P("2009-12"))
    assert n == 1002
    e, slope, pairs = nv.residuals(NVIX, RV, P("2009-12"))
    assert (slope, pairs) == (b, n) and list(e.index) == list(RESID)
    month = P("2010-06")
    assert e[month] == pytest.approx(math.log(NVIX[month]) - (a + b * math.log(RV[month])))


def test_the_expanding_regression_refits_up_to_each_month_from_the_36th_pair() -> None:
    gap = NVIX.copy()
    gap[P("1927-03")] = np.nan
    e, last = nv.expanding_residuals(gap, RV)
    assert list(e.index) == list(RESID)
    assert e.dropna().index[0] == P("1929-07")
    assert math.isnan(e[P("1927-03")])
    for month in (P("1929-07"), P("1945-05"), P("1987-10"), P("2010-12")):
        a, b, _ = nv.fit(gap, RV, month)
        expected = math.log(gap[month]) - (a + b * math.log(RV[month]))
        assert e[month] == pytest.approx(expected, abs=1e-9)
    assert last == pytest.approx(nv.fit(gap, RV)[1], abs=1e-9)


def test_h_at_month_end_m_is_the_expanding_percentile_of_e_at_m_minus_1() -> None:
    e, _, _ = nv.residuals(NVIX, RV)
    h = nv.h_state(e)
    percentile = st.expanding_percentile(e, 36)
    assert list(h.index) == list(ENDS)
    before = percentile.reindex(ENDS - 1).to_numpy()
    expected = np.where(np.isnan(before), np.nan, (before >= 0.8).astype(float))
    assert np.array_equal(h.to_numpy(), expected, equal_nan=True)
    assert nv.first_defined(h) == P("1929-07")
    assert nv.first_defined(pd.Series(np.nan, index=ENDS)) is None


def test_h_needs_36_values_and_counts_ties() -> None:
    e = pd.Series(np.nan, index=RESID)
    e[pd.period_range("1926-07", "1929-05", freq="M")] = 0.0
    e[P("1929-06")] = -1.0
    e[P("1929-07")] = 0.0
    e[P("1929-09")] = -0.5
    h = nv.h_state(e)
    assert h[ENDS < P("1929-07")].isna().all()
    assert (h[P("1929-07")], h[P("1929-08")], h[P("1929-10")]) == (0.0, 1.0, 0.0)
    assert math.isnan(h[P("1929-09")])
    rising = pd.Series(np.nan, index=RESID)
    rising[pd.period_range("1926-07", "1929-09", freq="M")] = np.arange(1.0, 40.0)
    for value, expected in ((31.5, 1.0), (30.5, 0.0)):
        rising[P("1929-10")] = value
        assert nv.h_state(rising)[P("1929-11")] == expected
        assert nv.h_state(rising, 0.7)[P("1929-11")] == 1.0
    assert nv.h_state(rising, 0.9)[P("1929-11")] == 0.0


def test_r_is_the_raw_nvix_from_1889_with_120_values() -> None:
    r = nv.r_state(NVIX)
    percentile = st.expanding_percentile(NVIX, 120)
    expected = (percentile.reindex(ENDS - 1).to_numpy() >= 0.8).astype(float)
    assert np.array_equal(r.to_numpy(), expected)
    late = NVIX.copy()
    late[late.index < P("1920-01")] = np.nan
    assert nv.first_defined(nv.r_state(late)) == P("1930-01")


def test_a_missing_n_or_rv_at_m_star_leaves_the_month_end_undefined() -> None:
    nvix, rv = NVIX.copy(), RV.copy()
    nvix[P("1950-05")] = np.nan
    rv[P("1970-08")] = np.nan
    h = nv.h_state(nv.residuals(nvix, rv)[0])
    assert math.isnan(h[P("1950-06")]) and math.isnan(h[P("1970-09")])
    assert not math.isnan(h[P("1950-07")]) and not math.isnan(h[P("1950-05")])
    r = nv.r_state(nvix)
    assert math.isnan(r[P("1950-06")]) and not math.isnan(r[P("1970-09")])


def test_the_nvix_part_of_a_start() -> None:
    months = ("1929-09", "1985-12", "1986-01", "1995-12", "1996-01", "2009-12", "2010-01")
    assert [nv.nvix_part(P(m)) for m in months] == [
        "back_cast",
        "back_cast",
        "test",
        "test",
        "train",
        "train",
        "update",
    ]


# ----- segments (the fifth period's 3.2 reading) --------------------------------------------------
SEGMENT = ("slow_x", P("1950-07"), P("1951-03"))


def test_slow_rows_read_the_six_month_ends_up_to_the_start_and_the_inside() -> None:
    h = _state(on=["1950-02", "1950-09"])
    r = _state(on=["1950-01"], undefined=["1950-04"])
    exits = [P("1949-12"), P("1950-05"), P("1950-11")]
    rows = nv.slow_rows({"H": h, "R": r}, [SEGMENT], exits, P("1929-07"))
    assert rows == [
        {
            "id": "slow_x",
            "start": "1950-07",
            "end": "1951-03",
            "part": "back_cast",
            "incomplete": False,
            "no_defined": False,
            "exit": "1950-05",
            "H": {
                "before": "●○○○○○",
                "before_on": True,
                "inside": "○○●○○○○○○",
                "inside_on": 1,
                "switch": 2,
                "first_on": "1950-02",
                "lead": 3,
            },
            "R": {
                "before": "○○–○○○",
                "before_on": False,
                "inside": "○○○○○○○○○",
                "inside_on": 0,
                "switch": -6,
                "first_on": None,
                "lead": None,
            },
        }
    ]
    assert rows[0]["H"]["switch"] == st.nearest_switch(h, P("1950-07"), P("1951-03"))
    late = nv.slow_rows({"H": _state(on=["1950-09"]), "R": r}, [SEGMENT], exits, P("1929-07"))
    assert (late[0]["H"]["first_on"], late[0]["H"]["lead"]) == ("1950-09", -4)
    none = nv.slow_rows({"H": h, "R": r}, [SEGMENT], [P("1951-04")], P("1929-07"))
    assert (none[0]["exit"], none[0]["H"]["lead"]) == (None, None)


@pytest.mark.parametrize(
    ("month", "on"), [("1950-01", False), ("1950-02", True), ("1950-07", True), ("1950-08", False)]
)
def test_the_before_window_is_the_start_and_the_five_month_ends_before_it(
    month: str, on: bool
) -> None:
    rows = nv.slow_rows({"H": _state(on=[month]), "R": _state()}, [SEGMENT], [], P("1929-07"))
    assert rows[0]["H"]["before_on"] is on


def test_the_first_segment_is_incomplete_and_an_empty_window_is_not_on() -> None:
    first = ("slow_bear_192908", P("1929-09"), P("1932-06"))
    h = _state(first="1929-07", on=["1929-08"])
    row = nv.slow_rows({"H": h, "R": h}, [first], [], P("1929-07"))[0]
    assert (row["incomplete"], row["no_defined"]) == (True, False)
    assert (row["H"]["before"], row["H"]["before_on"]) == ("–––○●○", True)
    late = _state(first="1931-01", on=["1931-02"])
    row = nv.slow_rows({"H": late, "R": late}, [first], [], P("1931-01"))[0]
    assert (row["incomplete"], row["no_defined"]) == (True, True)
    assert (row["H"]["before"], row["H"]["before_on"]) == ("––––––", False)
    row = nv.slow_rows({"H": h, "R": h}, [SEGMENT], [], None)[0]
    assert (row["incomplete"], row["no_defined"]) == (True, False)
    row = nv.slow_rows({"H": h, "R": h}, [SEGMENT], [], P("1950-02"))[0]
    assert row["incomplete"] is False
    assert nv.slow_rows({"H": h, "R": h}, [SEGMENT], [], P("1950-03"))[0]["incomplete"] is True


def test_long_bull_shares_keep_undefined_month_ends_in_the_denominator() -> None:
    h = _state(on=_span("1950-01", "1950-03"), undefined=_span("1950-04", "1950-06"))
    r = _state(on=["2010-08"])
    segments = [("bull_a", P("1950-01"), P("1950-12")), ("bull_b", P("2010-07"), P("2011-06"))]
    bull = nv.bull_rows({"H": h, "R": r}, segments)
    assert bull == {
        "rows": [
            {
                "id": "bull_a",
                "start": "1950-01",
                "end": "1950-12",
                "months": 12,
                "H": {"on": 3, "share": 0.25},
                "R": {"on": 0, "share": 0.0},
            },
            {
                "id": "bull_b",
                "start": "2010-07",
                "end": "2010-12",
                "months": 6,
                "H": {"on": 0, "share": 0.0},
                "R": {"on": 1, "share": 1 / 6},
            },
        ],
        "pooled": {"months": 18, "H": {"on": 3, "share": 3 / 18}, "R": {"on": 1, "share": 1 / 18}},
    }
    earlier = nv.bull_rows({"H": h}, segments, P("2009-12"))
    assert [row["id"] for row in earlier["rows"]] == ["bull_a"]
    assert earlier["pooled"] == {"months": 12, "H": {"on": 3, "share": 0.25}}


def test_the_line_is_8_slow_bears_and_a_long_bull_share_under_a_half() -> None:
    def slow(on: int) -> list[dict[str, Any]]:
        return [{"H": {"before_on": i < on}} for i in range(10)]

    def bull(share: float | None) -> dict[str, Any]:
        return {"pooled": {"H": {"share": share}}}

    assert nv.line(slow(8), bull(0.4999), "H") == {
        "slow_on": 8,
        "slow_count": 10,
        "bull_share": 0.4999,
        "slow_ok": True,
        "bull_ok": True,
        "passes": True,
    }
    assert nv.line(slow(7), bull(0.1), "H")["passes"] is False
    at_half = nv.line(slow(10), bull(0.5), "H")
    assert (at_half["bull_ok"], at_half["passes"]) == (False, False)
    assert nv.line(slow(10), bull(None), "H")["passes"] is False


def test_read_state_is_the_line_of_one_state_on_its_own() -> None:
    found = nv.read_state(H_BASE, BUILT.slow, BUILT.bull)
    rows = nv.slow_rows({"H": H_BASE}, BUILT.slow, (), None)
    assert found == nv.line(rows, nv.bull_rows({"H": H_BASE}, BUILT.bull), "H")
    assert found["slow_on"] == RESULT["second_stage"]["lines"]["H"]["slow_on"]


# ----- the block-shuffled control ----------------------------------------------------------------
def test_the_blocks_are_82_from_1929_07_and_the_last_has_six_months() -> None:
    blocks = nv.blocks()
    assert len(blocks) == 82
    assert blocks[0] == (P("1929-07"), P("1930-06"))
    assert blocks[80] == (P("2009-07"), P("2010-06"))
    assert blocks[-1] == (P("2010-07"), P("2010-12"))
    assert all(last.ordinal - first.ordinal == 11 for first, last in blocks[:81])
    assert all(blocks[i + 1][0] == blocks[i][1] + 1 for i in range(81))


def test_shuffled_moves_whole_blocks_and_keeps_1929_before_july() -> None:
    state = pd.Series(np.arange(984, dtype=float), index=ENDS)
    kept = state.copy()
    out = nv.shuffled(state, list(range(81, -1, -1)))
    pd.testing.assert_series_equal(state, kept)
    assert list(out[pd.period_range("1929-01", "1929-06", freq="M")]) == list(range(6))
    tail = state[pd.period_range("2010-07", "2010-12", freq="M")].to_numpy()
    assert list(out[pd.period_range("1929-07", "1929-12", freq="M")]) == list(tail)
    assert out[P("1930-01")] == state[P("2009-07")]
    assert out[P("2010-12")] == state[P("1930-06")]
    assert sorted(out.to_numpy()) == sorted(state.to_numpy())
    pd.testing.assert_series_equal(nv.shuffled(state, list(range(82))), state)
    for bad in ([0] * 82, list(range(81)), list(range(1, 83))):
        with pytest.raises(ValueError):
            nv.shuffled(state, bad)


@pytest.mark.parametrize("key", ["H", "R"])
def test_the_null_is_each_draw_shuffled_and_read_by_the_same_line(key: str) -> None:
    state = {"H": H_BASE, "R": R_BASE}[key]
    got = nv.null(state, BUILT.slow, BUILT.bull, draws=40)
    rng = np.random.default_rng(20261029)
    reads = [
        nv.read_state(nv.shuffled(state, rng.permutation(82).tolist()), BUILT.slow, BUILT.bull)
        for _ in range(40)
    ]
    actual = nv.read_state(state, BUILT.slow, BUILT.bull)["slow_on"]
    found = [read["slow_on"] for read in reads]
    assert got == {
        "actual": actual,
        "draws": 40,
        "p": pytest.approx(sum(f >= actual for f in found) / 40),
        "holds": sum(f >= actual for f in found) / 40 <= 0.05,
        "mid_rank": pytest.approx(
            (sum(f < actual for f in found) + 0.5 * sum(f == actual for f in found)) / 40
        ),
        "pass_share": pytest.approx(sum(read["passes"] for read in reads) / 40),
        "median": float(statistics.median(found)),
    }
    assert RESULT["second_stage"]["null"][key] == nv.null(state, BUILT.slow, BUILT.bull)
    assert RESULT["second_stage"]["null"][key]["draws"] == 1000


def test_the_p_value_counts_ties_and_the_mid_rank_halves_them() -> None:
    off = nv.null(_state(), BUILT.slow, BUILT.bull, draws=20)
    assert off == {
        "actual": 0,
        "draws": 20,
        "p": 1.0,
        "holds": False,
        "mid_rank": 0.5,
        "pass_share": 0.0,
        "median": 0.0,
    }
    on = nv.null(_state(on=_span("1929-01", "2010-12")), BUILT.slow, BUILT.bull, draws=20)
    assert (on["actual"], on["p"], on["pass_share"], on["median"]) == (10, 1.0, 0.0, 10.0)


def test_the_null_holds_when_few_draws_reach_the_actual_count() -> None:
    starts = [start for _, start, _ in BUILT.slow]
    exact = _state(first="1929-07", on=[str(start) for start in starts])
    found = nv.null(exact, BUILT.slow, BUILT.bull, draws=200)
    assert found["actual"] == 10
    assert found["p"] <= 0.05 and found["holds"] is True
    assert found["mid_rank"] > 0.9


# ----- counts -----------------------------------------------------------------------------------
def test_state_counts_are_over_the_978_month_ends_from_1929_07() -> None:
    h = _state(first="1929-08", on=["1929-08", "1950-01", "1950-02"], undefined=["1960-01"])
    assert nv.state_counts(h) == {"on": 3, "defined": 976, "month_ends": 978, "share": 3 / 978}
    early = _state(on=["1929-03"])
    assert nv.state_counts(early)["on"] == 0


def test_decade_counts_and_persistence() -> None:
    h = _state(
        on=["1929-05", "1939-12", "1940-01", "1940-02", "1950-01", "1950-03", "2010-12"],
        undefined=["1950-02"],
    )
    assert nv.decade_counts(h) == {
        "1929–1939": 2,
        "1940–1949": 2,
        "1950–1959": 2,
        "1960–1969": 0,
        "1970–1979": 0,
        "1980–1989": 0,
        "1990–1999": 0,
        "2000–2010": 1,
    }
    assert nv.persistence(h) == {"runs": 5, "mean_months": 7 / 5}
    assert nv.persistence(_state()) == {"runs": 0, "mean_months": None}


def test_rv_medians_use_rv_at_m_minus_1_over_defined_month_ends() -> None:
    rv = pd.Series(np.arange(1014, dtype=float) + 1.0, index=RESID)
    h = _state(first="1929-07", on=["1950-01", "1950-02", "1950-03"])
    medians = nv.rv_medians(h, rv)
    assert medians["on"] == rv[P("1950-01")]
    assert medians["all"] == float(rv[pd.period_range("1929-06", "2010-11", freq="M")].median())
    assert nv.rv_medians(_state(first="1929-07"), rv)["on"] is None


# ----- build and check --------------------------------------------------------------------------
def test_build_takes_the_slow_leg_exits_from_the_bench_targets() -> None:
    returns, cash = rs.market_series(BASE.factors)
    months = rs.month_targets(returns, cash, BASE.entries)
    assert BUILT.exits == tuple(month for month, _ in rs.leg_exits(months, "slow"))
    assert len(BUILT.exits) > 10
    pd.testing.assert_series_equal(BUILT.rv, nv.realized_vol(BASE.factors))
    pd.testing.assert_series_equal(BUILT.nvix, BASE.nvix)
    assert (BUILT.slow, BUILT.bull, BUILT.sources) == (BASE.slow, BASE.bull, BASE.sources)
    built = _built(slow_out=[*_span("1950-03", "1950-08"), "1960-01"])
    assert built.exits == (P("1950-03"), P("1960-01"))


def test_check_counts_only_and_never_reads_a_rank_a_slope_or_a_segment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in CHECK_FORBIDDEN:
        monkeypatch.setattr(nv, name, _forbidden)
    summary = nv.check_summary(BUILT)
    assert summary == RESULT["check"]
    assert set(summary) == {
        "run_id",
        "snapshot_id",
        "sources",
        "nvix_months",
        "nvix_missing",
        "nvix_missing_1926_1928",
        "rv_months",
        "regression_months",
        "h_first",
        "states",
        "decades",
        "persistence",
    }
    rest = {key: value for key, value in summary.items() if key != "sources"}
    assert sorted(set(_strings(rest))) == sorted({nv.RUN_ID, nv.SNAPSHOT_ID, summary["h_first"]})
    text = json.dumps(summary, ensure_ascii=False)
    for key in ("rho", "b", "slow_bear", "long_bull", "lead", "null", "e"):
        assert f'"{key}"' not in text
    for value in (RESULT["first_stage"]["rho"], RESULT["second_stage"]["b"]):
        assert repr(value) not in text
    for row in RESULT["second_stage"]["slow_bear"]:
        assert row["start"] not in text


def test_check_counts_on_hand_built_months() -> None:
    nvix = nv.nvix_months(_nvix_frame({"1927-02": np.nan, "1950-03": np.nan, "1992-01": np.nan}))
    april = [str(day.date()) for day in DAYS[MONTH_OF == P("1960-05")]]
    summary = nv.check_summary(_built(nvix=nvix, factors=_factors(drop=april[14:])))
    assert (summary["nvix_months"], summary["nvix_missing"]) == (982, 2)
    assert summary["nvix_missing_1926_1928"] == 1
    assert (summary["rv_months"], summary["regression_months"]) == (983, 1010)
    assert summary["h_first"] == "1929-08"
    for key in ("H", "R"):
        assert summary["states"][key]["month_ends"] == 978
    # H: 1929-07 has 35 values at M*; 1950-04, 1960-06 and 1992-02 have no pair at M*.
    assert summary["states"]["H"]["defined"] == 974
    assert summary["states"]["R"]["defined"] == 976
    h = nv.h_state(nv.residuals(nvix, nv.realized_vol(_factors(drop=april[14:])))[0])
    assert summary["states"]["H"] == nv.state_counts(h)
    assert summary["decades"]["H"] == nv.decade_counts(h)
    assert summary["persistence"]["H"] == nv.persistence(h)


# ----- verdict and readings -----------------------------------------------------------------------
LINE = {"slow_on": 8, "bull_share": 0.3, "slow_ok": True, "bull_ok": True, "passes": True}


@pytest.mark.parametrize(
    ("only_vol", "changes", "verdict"),
    [
        (True, {}, "only_vol"),
        (True, {"passes": False, "slow_ok": False}, "only_vol"),
        (False, {}, "holds"),
        (False, {"slow_on": 7, "slow_ok": False, "passes": False}, "not_holds"),
        (False, {"bull_share": 0.5, "bull_ok": False, "passes": False}, "not_holds"),
    ],
)
def test_verdict(only_vol: bool, changes: dict[str, Any], verdict: str) -> None:
    found = {**LINE, **changes}
    decision = nv.judge({"rho": 0.79, "only_vol": only_vol}, found)
    assert decision == {
        "verdict": verdict,
        "rho": 0.79,
        "slow_on": found["slow_on"],
        "bull_share": found["bull_share"],
        "slow_ok": found["slow_ok"],
        "bull_ok": found["bull_ok"],
    }


def test_readings_are_printed_only() -> None:
    lines = {"H": {"passes": False}, "R": {"passes": True}}
    assert nv.readings(lines, {"H": {"pass_share": 0.1}}) == ["原样的 NVIX 亮是因为波动"]
    assert nv.readings(lines, {"H": {"pass_share": 0.104}}) == [
        "没信息的状态也有约 10% 会过这条线",
        "原样的 NVIX 亮是因为波动",
    ]
    both = {"H": {"passes": True}, "R": {"passes": True}}
    assert nv.readings(both, {"H": {"pass_share": 0.0}}) == []


# ----- the whole read -----------------------------------------------------------------------------
def test_the_read_has_every_part_and_dumps_as_json() -> None:
    assert set(RESULT) == {
        "run_id",
        "owner_decision",
        "snapshot_id",
        "rules_sha256",
        "check",
        "first_stage",
        "second_stage",
        "sensitivity",
        "decision",
        "readings",
    }
    assert (RESULT["run_id"], RESULT["owner_decision"]) == (nv.RUN_ID, nv.OWNER_DECISION)
    assert (RESULT["snapshot_id"], RESULT["rules_sha256"]) == (nv.SNAPSHOT_ID, BASE.rules_sha256)
    assert RESULT["check"] == nv.check_summary(BUILT)
    first = nv.first_stage(BUILT.nvix, BUILT.rv)
    assert RESULT["first_stage"] == first
    second = RESULT["second_stage"]
    e, b, pairs = nv.residuals(BUILT.nvix, BUILT.rv)
    assert (second["b"], second["regression_months"]) == (b, pairs)
    assert second["b_expanding_last"] == nv.expanding_residuals(BUILT.nvix, BUILT.rv)[1]
    states = {"H": H_BASE, "R": R_BASE}
    slow = nv.slow_rows(states, BUILT.slow, BUILT.exits, nv.first_defined(H_BASE))
    bull = nv.bull_rows(states, BUILT.bull)
    assert (second["slow_bear"], second["long_bull"]) == (slow, bull)
    lines = {key: nv.line(slow, bull, key) for key in ("H", "R")}
    assert second["lines"] == lines
    assert second["rv_medians"] == {key: nv.rv_medians(s, BUILT.rv) for key, s in states.items()}
    assert RESULT["decision"] == nv.judge(first, lines["H"])
    assert RESULT["readings"] == nv.readings(lines, second["null"])
    assert len(slow) == 10 and len(bull["rows"]) == 7
    json.dumps(RESULT, allow_nan=False)
    json.dumps(MONTH_ROWS, allow_nan=False)


def test_the_sensitivity_rows() -> None:
    sensitive = RESULT["sensitivity"]
    assert set(sensitive) == {
        "h_0.7",
        "h_0.9",
        "expanding",
        "rv_demeaned",
        "rv_two_months",
        "without_2010",
    }
    for p in (0.7, 0.9):
        assert sensitive[f"h_{p}"] == nv.read_state(nv.h_state(E_BASE, p), BUILT.slow, BUILT.bull)
    expanding, last = nv.expanding_residuals(BUILT.nvix, BUILT.rv)
    h_expanding = nv.h_state(expanding)
    assert sensitive["expanding"] == {
        "b_last": last,
        "line": nv.read_state(h_expanding, BUILT.slow, BUILT.bull),
        "null": nv.null(h_expanding, BUILT.slow, BUILT.bull),
    }
    for key, rv in (
        ("rv_demeaned", nv.realized_vol(BUILT.factors, demean=True)),
        ("rv_two_months", nv.realized_vol(BUILT.factors, span=2)),
    ):
        h = nv.h_state(nv.residuals(BUILT.nvix, rv)[0])
        assert sensitive[key] == {
            "rho": nv.spearman(BUILT.nvix, rv, P("1929-01"), P("2010-12")),
            "line": nv.read_state(h, BUILT.slow, BUILT.bull),
        }
    e_without, b_without, _ = nv.residuals(BUILT.nvix, BUILT.rv, P("2009-12"))
    share = nv.bull_rows({"H": nv.h_state(e_without)}, BUILT.bull, P("2009-12"))["pooled"]["H"]
    assert sensitive["without_2010"] == {
        "rho": RESULT["first_stage"]["without_2010"],
        "b": b_without,
        "bull_share": share["share"],
    }


def test_month_rows_cover_1926_07_to_2010_12() -> None:
    assert len(MONTH_ROWS) == 1014
    assert [row["month"] for row in MONTH_ROWS] == [str(p) for p in RESID]
    assert all(sorted(row) == ["H", "R", "e", "month", "nvix", "rv"] for row in MONTH_ROWS)
    rows = {row["month"]: row for row in MONTH_ROWS}
    assert rows["1992-01"]["nvix"] is None and rows["1992-01"]["e"] is None
    assert rows["1928-12"]["H"] is None and rows["1928-12"]["R"] is None
    assert rows["1950-01"] == {
        "month": "1950-01",
        "nvix": BUILT.nvix[P("1950-01")],
        "rv": BUILT.rv[P("1950-01")],
        "e": E_BASE[P("1950-01")],
        "H": H_BASE[P("1950-01")],
        "R": R_BASE[P("1950-01")],
    }


def test_data_after_2010_never_matter() -> None:
    later = {}
    for source, frame in FRAMES.items():
        frame = frame.copy()
        after = frame.index > pd.Timestamp("2010-12-31")
        frame.loc[after] = frame.loc[after] * 3.0 - 0.01
        later[source] = frame
    nvix = later[nv.NVIX]
    nvix.loc[pd.Timestamp("2014-01-01"), "value"] = -1.0
    later[nv.NVIX] = pd.concat([nvix, nvix.iloc[-1:]])
    assert nv.read(_inputs(later)) == (RESULT, MONTH_ROWS)


def test_an_only_vol_read_still_prints_the_second_pass() -> None:
    rv = nv.realized_vol(FRAMES[cp.FACTORS])
    frame = FRAMES[nv.NVIX].copy()
    tilt = 1.0 + 1e-3 * (RESID.month.to_numpy() % 3)
    frame.loc[RESID.to_timestamp(), "value"] = 1.2 * rv.to_numpy() * tilt
    result, _ = nv.read(_inputs({**FRAMES, nv.NVIX: frame}))
    assert result["first_stage"]["only_vol"] is True
    assert result["decision"]["verdict"] == "only_vol"
    assert len(result["second_stage"]["slow_bear"]) == 10
    assert "第二道照印，不读（读法第 1 条）。" in nv.render(result)


# ----- report -----------------------------------------------------------------------------------
def test_report_sentences() -> None:
    text = nv.render({**RESULT, "provenance": {"commit": "abc123"}})
    order = [
        "## 判定",
        "## 第一道：秩相关",
        "## 慢熊段",
        "## 长牛段",
        "## 打乱对照",
        "## 敏感度",
        "## 次数和分布",
        "## 数据和核对",
        "## 局限",
    ]
    assert [text.index(heading) for heading in order] == sorted(text.index(h) for h in order)
    first_screen = text.split("## 第一道：秩相关")[0]
    assert f"`{RESULT['decision']['verdict']}`" in first_screen
    assert f"ρ = {RESULT['first_stage']['rho']:.3f}" in first_screen
    assert f"{RESULT['second_stage']['b']:.3f}" in first_screen
    assert "第二道照印，不读" not in text
    for sentence in (
        "只印：不进账本，不是探针、不是试验，不模拟新规则的仓位线，不是长牛代价的读数。",
        "production_effect=none；broker_action=none。",
        "e 是「NVIX 比这个月 RV 多出来的那部分」",
        "NVIX 2016 年停更：就算判 `holds` 也不进前瞻账本，不开正式题。",
        "H 最早 1932-06 起才有定义",
        "这一行的慢熊段数天生少 1–2 段。",
        "慢腿退出印「–」：[段起点 − 5, 段终点] 里慢腿没有新的退出（也可能窗口以前就已经出去）",
    ):
        assert sentence in text, sentence
    for note in RESULT["readings"]:
        assert note in first_screen
    assert nv.SNAPSHOT_ID in text and RESULT["rules_sha256"] in text and "abc123" in text
    slow_table = text.split("## 慢熊段")[1].split("## 长牛段")[0]
    for row in RESULT["second_stage"]["slow_bear"]:
        assert f"| {row['start']}..{row['end']} |" in slow_table
        assert row["H"]["before"] in slow_table and row["R"]["before"] in slow_table
    assert "窗口不全" in slow_table
    bull_table = text.split("## 长牛段")[1].split("## 打乱对照")[0]
    assert sum(1 for line in bull_table.splitlines() if line.startswith("| ")) == 7 + 2


# ----- files and commands -------------------------------------------------------------------------
def test_files(tmp_path: Path) -> None:
    nv.write_check(RESULT["check"], tmp_path / "c")
    assert json.loads((tmp_path / "c" / nv.CHECK).read_text(encoding="utf-8")) == RESULT["check"]
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [nv.CHECK]
    nv.write_result(RESULT, MONTH_ROWS, tmp_path / "r")
    lines = (tmp_path / "r" / nv.MONTHS).read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == MONTH_ROWS
    assert json.loads((tmp_path / "r" / nv.SUMMARY).read_text(encoding="utf-8")) == RESULT
    assert (tmp_path / "r" / nv.REPORT).read_text(encoding="utf-8") == nv.render(RESULT)


def test_commands(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(nv, "load_inputs", lambda: BASE)
    monkeypatch.setattr(nv, "git_provenance", lambda root: {"commit": "abc123"})
    runner = CliRunner()
    with pytest.MonkeyPatch.context() as patch:
        for name in CHECK_FORBIDDEN:
            patch.setattr(nv, name, _forbidden)
        out = runner.invoke(nv.app, ["check", "--out", str(tmp_path / "c")])
    assert out.exit_code == 0, out.output
    check = RESULT["check"]
    assert (
        f"nvix_table: N 有值 {check['nvix_months']} 个月；H 第一个有定义的月底 "
        f"{check['h_first']}；H 为真 {check['states']['H']['on']} 个月底"
    ) in out.output
    assert sorted(p.name for p in (tmp_path / "c").iterdir()) == [nv.CHECK]
    out = runner.invoke(nv.app, ["run", "--out", str(tmp_path / "r")])
    assert out.exit_code == 0, out.output
    assert f"nvix_table: 判定 {RESULT['decision']['verdict']}" in out.output
    assert sorted(p.name for p in (tmp_path / "r").iterdir()) == sorted(
        [nv.CHECK, nv.MONTHS, nv.SUMMARY, nv.REPORT]
    )
    written = json.loads((tmp_path / "r" / nv.SUMMARY).read_text(encoding="utf-8"))
    assert written == {**RESULT, "provenance": {"commit": "abc123"}}
    assert (tmp_path / "r" / nv.CHECK).read_bytes() == (tmp_path / "c" / nv.CHECK).read_bytes()
    assert "abc123" in (tmp_path / "r" / nv.REPORT).read_text(encoding="utf-8")


def test_commands_stop_when_loading_fails(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def broken() -> nv.Inputs:
        raise ValueError("no snapshot\nsecond line")

    monkeypatch.setattr(nv, "load_inputs", broken)
    for command in ("check", "run"):
        out = CliRunner().invoke(nv.app, [command, "--out", str(tmp_path / command)])
        assert out.exit_code == 1
        assert "nvix_table: 数据载入失败：no snapshot" in out.output
        assert "second line" not in out.output
        assert not (tmp_path / command).exists()
    monkeypatch.setattr(nv, "load_inputs", lambda: BASE)
    monkeypatch.setattr(
        rs, "market_series", lambda factors: (_ for _ in ()).throw(ValueError("bad market"))
    )
    for command in ("check", "run"):
        out = CliRunner().invoke(nv.app, [command, "--out", str(tmp_path / f"m{command}")])
        assert out.exit_code == 1
        assert "nvix_table: 建数据失败：bad market" in out.output
        assert not (tmp_path / f"m{command}").exists()


def test_tied_values_take_average_ranks() -> None:
    # Spearman ranks ties by their average rank (pandas rank()), not in order (argsort ranks).
    few = pd.Series(np.nan, index=ALL)
    for month, value in (("1950-01", 1.0), ("1950-02", 1.0), ("1950-03", 2.0), ("1950-04", 3.0)):
        few[P(month)] = value
    months = [P("1950-01"), P("1950-02"), P("1950-03"), P("1950-04")]
    rv = RV.copy()
    rv[months] = [4.0, 3.0, 2.0, 1.0]
    expected = _rank_corr([1.0, 1.0, 2.0, 3.0], [4.0, 3.0, 2.0, 1.0])
    assert expected == pytest.approx(-0.9486832980505138)
    assert nv.spearman(few, rv, P("1929-01"), P("2010-12")) == pytest.approx(expected, abs=1e-12)
    tied = NVIX.copy()
    tied[pd.period_range("1950-01", "1950-12", freq="M")] = 20.0
    want = _months_rho("1929-01", "2010-12", tied, RV)
    assert nv.first_stage(tied, RV)["rho"] == pytest.approx(want, abs=1e-12)


def test_the_early_missing_count_is_1926_07_to_1928_12() -> None:
    nvix = nv.nvix_months(_nvix_frame({"1928-12": np.nan, "1929-01": np.nan, "1929-06": np.nan}))
    summary = nv.check_summary(_built(nvix=nvix))
    assert (summary["nvix_missing_1926_1928"], summary["nvix_missing"]) == (1, 2)


@pytest.mark.parametrize(
    ("verdict", "heading"),
    [
        ("only_vol", "`only_vol`：NVIX 在 1929–2010 只是波动水平"),
        ("holds", "`holds`：NVIX 去掉波动以后的部分在慢熊开始以前亮"),
        ("not_holds", "`not_holds`：NVIX 去掉波动以后的部分不在慢熊开始以前亮"),
    ],
)
def test_the_verdict_headings(verdict: str, heading: str) -> None:
    text = nv.render({**RESULT, "decision": {**RESULT["decision"], "verdict": verdict}})
    assert f"\n## 判定：{heading}\n" in text
