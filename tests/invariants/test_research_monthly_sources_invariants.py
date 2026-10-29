"""Invariants of the monthly long-history sources (TRADING-2565-37, -88 and -91).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-21:monthly_long_history``; the unemployment rates of
TRADING-2565-88 under ``owner_decision:TRADING-2565:2026-10-29:print_only_tables``; Manela's NVIX
of TRADING-2565-91 under ``owner_decision:TRADING-2565:2026-10-29:print_only_tables_nvix``). Every
value below is synthetic; no test reads a real snapshot or downloads anything.

53. A monthly observation is used only once its month is known. Month M is published on the first
    trading day on or after the first day of month M + 1 + release_lag_months and used
    known_lag_trading_days trading days later. It stays until the next month is used, over at most
    one missing month. Changing anything from month M on leaves every value before M's use
    unchanged.
54. Source specs stay strict: Shiller sources are the monthly columns P, D, E, CPI and GS10, Ken
    French stays daily, only a monthly source has a release lag, and FRED keeps at least one
    trading day of lag. A spec written before this change still reads as before.
55. Every path that aligns sources follows the monthly rule: ``align_source``,
    ``with_signal_sources``, the trial's market data and the screen.
56. Shiller's sheet is read by its header cells. The month y.1 is October, and the rows end at
    the first row without a month. A missing header, no rows and a repeated month are refused.
57. Snapshots carry monthly sources. Shiller's workbook is fetched once per snapshot, and
    ``fetch.json`` names its address. The check fails a monthly date that is not the first of a
    month and warns on a missing month, not on missing weekdays. A snapshot written before this
    change still loads under its id.
58. The approved monthly sources are the eleven in the settings file, with their lags.
68. A monthly source may cover only some of the years: a FRED series that ends early or starts
    late checks without a gap and loads as fetched. Nothing requires a monthly source to reach a
    given year (TRADING-2565-88: the unemployment rates end in 1942 and 1946 and start in 1948).
69. Manela's NVIX workbook is read with the standard library: the sheet news_implied_volatility
    found by name through a relative or absolute path, its Date and NVIX headers on row 1 in any
    case, shared or inline strings, yyyymmdd month ends turned into month starts, and the
    missing-value spellings, empty text and an empty value as NaN, keeping the row; rows without
    a date or NVIX cell are skipped, and other columns are not read. A missing sheet or header,
    headers below row 1, a sheet path outside xl/, a date that is not a yyyymmdd month end, a
    skipped, repeated or earlier month, other text, a value or shared-string index that is not a
    number, a shared string that is not there, a member over 20 MB, a document type, broken XML
    and bytes that are not a zip are refused, and no refusal names a cell's value.
70. Snapshots carry NVIX: the workbook is fetched once with requests and its default headers,
    ``fetch.json`` names its address, missing months stay missing and the check passes; Manela
    sources are the monthly nvix column with at least one trading day of lag, and the fetch
    command takes --manela-url.
"""

from __future__ import annotations

import io
import json
import zipfile
from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import requests
import yaml
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench import trial as tr
from ai_trading_system.research_bench.data import BenchData, from_us_market, with_signal_sources
from ai_trading_system.research_bench.screen import ScreenMarket, load_signal, screen_signal
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.sources import (
    SourceSpec,
    align_source,
    known_at,
    monthly_known_at,
)
from ai_trading_system.research_data import qqq_long_history as qlh
from ai_trading_system.research_data import signal_sources as ss

MONTHLY: dict[str, Any] = {
    "source_id": "baa_monthly",
    "provider": "fred",
    "dataset": "BAA",
    "frequency": "monthly",
    "release_lag_months": 0,
    "known_lag_trading_days": 5,
    "description": "Baa yield, monthly",
}
SHILLER: dict[str, Any] = {
    "source_id": "shiller_earnings",
    "provider": "shiller",
    "dataset": "E",
    "frequency": "monthly",
    "release_lag_months": 15,
    "known_lag_trading_days": 0,
    "description": "earnings",
}
MANELA: dict[str, Any] = {
    "source_id": "nvix_monthly",
    "provider": "manela",
    "dataset": "nvix",
    "frequency": "monthly",
    "release_lag_months": 0,
    "known_lag_trading_days": 5,
    "description": "news implied volatility",
}
DAILY: dict[str, Any] = {
    "source_id": "baa10y",
    "provider": "fred",
    "dataset": "BAA10Y",
    "frequency": "daily",
    "known_lag_trading_days": 1,
    "description": "credit spread",
}
# 1 January and 1 June 2020 are holidays here; 1 February and 1 March 2020 are weekend days.
CAL = pd.bdate_range("2019-12-02", "2020-06-30").drop(pd.to_datetime(["2020-01-01", "2020-06-01"]))
MONTHS = pd.Series(
    [1.0, 2.0, np.nan, 4.0, 5.0, 6.0],
    index=pd.to_datetime(
        ["2019-12-01", "2020-01-01", "2020-02-01", "2020-03-15", "2020-04-01", "2020-06-01"]
    ),
)


def _expected(spans: list[tuple[str, str, float]]) -> np.ndarray:
    out = pd.Series(np.nan, index=CAL)
    for first, last, value in spans:
        out.loc[first:last] = value
    return out.to_numpy()


# 53 ----------------------------------------------------------------------------------------------


def test_a_month_is_used_from_the_day_it_is_known() -> None:
    # December is known on Thursday 2 January (1 January is a holiday). February is missing, so
    # January is carried over it. March is dated mid-month and still counts as March. June would be
    # published on 1 July, after the calendar ends, so it is never used.
    plain = monthly_known_at(MONTHS, CAL, 0, 0)
    expected = _expected(
        [
            ("2020-01-02", "2020-01-31", 1.0),
            ("2020-02-03", "2020-03-31", 2.0),
            ("2020-04-01", "2020-04-30", 4.0),
            ("2020-05-01", "2020-06-30", 5.0),
        ]
    )
    np.testing.assert_array_equal(plain.to_numpy(), expected)
    assert plain.index.equals(CAL)

    # One release month and two trading days: December is published on Monday 3 February (the
    # first trading day on or after 1 February) and used on Wednesday 5 February. June 1 is a
    # holiday, so April is published on 2 June and used on 4 June.
    lagged = monthly_known_at(MONTHS, CAL, 1, 2)
    expected = _expected(
        [
            ("2020-02-05", "2020-03-03", 1.0),
            ("2020-03-04", "2020-05-04", 2.0),
            ("2020-05-05", "2020-06-03", 4.0),
            ("2020-06-04", "2020-06-30", 5.0),
        ]
    )
    np.testing.assert_array_equal(lagged.to_numpy(), expected)


def test_a_second_missing_month_empties_the_source() -> None:
    values = pd.Series([1.0, 4.0], index=pd.to_datetime(["2019-12-01", "2020-03-01"]))
    out = monthly_known_at(values, CAL, 0, 0)
    # December is used from 2 January. January is missing and December is carried; February is
    # missing too, so December ends on the day February would be used (2 March, as 1 March is a
    # Sunday). March is used from 1 April; April and May are missing, so March ends on the day May
    # would be used (2 June, as 1 June is a holiday).
    expected = _expected([("2020-01-02", "2020-02-28", 1.0), ("2020-04-01", "2020-05-29", 4.0)])
    np.testing.assert_array_equal(out.to_numpy(), expected)
    # A month published long before the calendar starts is not carried into it.
    old = pd.Series([7.0], index=pd.to_datetime(["2019-06-01"]))
    assert monthly_known_at(old, CAL, 0, 0).isna().all()


def _by_hand(values: pd.Series, calendar: pd.DatetimeIndex, release: int, lag: int) -> np.ndarray:
    """Day by day: the newest month already used, unless two later months are already due."""

    observations = values.dropna()
    months = [(d.year, d.month) for d in observations.index]

    def due(year: int, month: int, offset: int) -> int:
        total = year * 12 + (month - 1) + offset + release
        first = pd.Timestamp(total // 12, total % 12 + 1, 1)
        return int(np.searchsorted(calendar.to_numpy(), first.to_datetime64(), side="left")) + lag

    out = np.full(len(calendar), np.nan)
    for i in range(len(calendar)):
        best = None
        for k, (year, month) in enumerate(months):
            if due(year, month, 1) <= i:
                best = k
        if best is None:
            continue
        year, month = months[best]
        if i < due(year, month, 3):
            out[i] = observations.iloc[best]
    return out


@pytest.mark.parametrize("seed", range(6))
def test_the_monthly_rule_matches_a_day_by_day_reading_and_never_looks_ahead(seed: int) -> None:
    rng = np.random.default_rng(seed)
    calendar = pd.bdate_range("1995-01-02", "1997-12-31")
    calendar = calendar.delete(np.sort(rng.choice(len(calendar), 40, replace=False)))
    months = pd.date_range("1994-01-01", "1997-12-01", freq="MS")
    days = rng.integers(0, 28, len(months))
    values = pd.Series(rng.normal(size=len(months)), index=months + pd.to_timedelta(days, unit="D"))
    values[rng.random(len(months)) < 0.15] = np.nan
    release = int(rng.choice([0, 1, 15]))
    lag = int(rng.choice([0, 1, 5]))
    out = monthly_known_at(values, calendar, release, lag)
    np.testing.assert_array_equal(out.to_numpy(), _by_hand(values, calendar, release, lag))

    cut = int(rng.integers(12, len(months) - 1))
    changed = values.copy()
    changed.iloc[cut:] = rng.normal(size=len(months) - cut) + 100.0
    moved = monthly_known_at(changed, calendar, release, lag)
    first = months[cut].to_period("M") + 1 + release
    used = int(calendar.searchsorted(first.to_timestamp(how="start"), side="left")) + lag
    np.testing.assert_array_equal(moved.to_numpy()[:used], out.to_numpy()[:used])


def test_the_monthly_rule_refuses_bad_inputs() -> None:
    with pytest.raises(ValueError):
        monthly_known_at(MONTHS, CAL, -1, 0)
    with pytest.raises(ValueError):
        monthly_known_at(MONTHS, CAL, 0, -1)
    with pytest.raises(ValueError):
        monthly_known_at(MONTHS, CAL[::-1], 0, 0)
    twice = pd.Series([1.0, 2.0], index=pd.to_datetime(["2020-01-01", "2020-01-20"]))
    with pytest.raises(ValueError):
        monthly_known_at(twice, CAL, 0, 0)
    backwards = pd.Series([1.0, 2.0], index=pd.to_datetime(["2020-02-01", "2020-01-01"]))
    with pytest.raises(ValueError):
        monthly_known_at(backwards, CAL, 0, 0)


# 54 ----------------------------------------------------------------------------------------------


def test_good_monthly_specs_are_accepted() -> None:
    assert SourceSpec(**MONTHLY).release_lag_months == 0
    assert SourceSpec(**SHILLER).release_lag_months == 15
    for column in ("P", "D", "E", "CPI", "GS10"):
        assert SourceSpec(**(SHILLER | {"dataset": column})).dataset == column


@pytest.mark.parametrize(
    ("base", "bad"),
    [
        (SHILLER, {"frequency": "daily", "release_lag_months": 0}),
        (SHILLER, {"dataset": "CAPE"}),
        (SHILLER, {"dataset": "p"}),
        (MONTHLY, {"known_lag_trading_days": 0}),
        (MONTHLY, {"release_lag_months": -1}),
        (MONTHLY, {"release_lag_months": "1"}),
        (MONTHLY, {"release_lag_months": 1.0}),
        (MONTHLY, {"frequency": "weekly"}),
        (DAILY, {"release_lag_months": 1}),
        (
            DAILY,
            {
                "provider": "ken_french",
                "dataset": "F-F_Research_Data_Factors_daily",
                "frequency": "monthly",
                "known_lag_trading_days": 0,
            },
        ),
    ],
)
def test_monthly_specs_are_strict(base: dict[str, Any], bad: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        SourceSpec(**(base | bad))


def test_a_spec_written_before_monthly_sources_reads_as_before() -> None:
    spec = SourceSpec.model_validate(DAILY)
    assert (spec.frequency, spec.release_lag_months, spec.known_lag_trading_days) == ("daily", 0, 1)


# 55 ----------------------------------------------------------------------------------------------


def _daily_values() -> pd.Series:
    return pd.Series(np.arange(len(CAL), dtype=float), index=CAL)


def test_align_source_chooses_the_rule_by_frequency() -> None:
    daily = _daily_values()
    np.testing.assert_array_equal(
        align_source(daily, CAL, 2).to_numpy(), known_at(daily, CAL, 2).to_numpy()
    )
    np.testing.assert_array_equal(
        align_source(daily, CAL, SourceSpec(**DAILY)).to_numpy(),
        known_at(daily, CAL, 1).to_numpy(),
    )
    lagged = SourceSpec(**(MONTHLY | {"release_lag_months": 1, "known_lag_trading_days": 2}))
    np.testing.assert_array_equal(
        align_source(MONTHS, CAL, lagged).to_numpy(),
        monthly_known_at(MONTHS, CAL, 1, 2).to_numpy(),
    )


def test_bench_inputs_take_monthly_sources_by_their_rule() -> None:
    returns = pd.DataFrame({"qqq": 0.001, "cash": 0.0}, index=CAL)
    base = BenchData(returns=returns, inputs=returns.copy(), source={"market": "test"})
    frames = {
        "baa_monthly": MONTHS.to_frame("value"),
        "baa10y": _daily_values().to_frame("value"),
    }
    out = with_signal_sources(
        base, frames, {"baa_monthly": SourceSpec(**MONTHLY), "baa10y": 1}, snapshot_id="sig-x"
    )
    np.testing.assert_array_equal(
        out.inputs["source:baa_monthly:value"].to_numpy(),
        monthly_known_at(MONTHS, CAL, 0, 5).to_numpy(),
    )
    np.testing.assert_array_equal(
        out.inputs["source:baa10y:value"].to_numpy(),
        known_at(_daily_values(), CAL, 1).to_numpy(),
    )
    pd.testing.assert_frame_equal(out.returns, returns)


def _monthly_frame(start: str, end: str, seed: int) -> pd.DataFrame:
    months = pd.date_range(start, end, freq="MS", name="date")
    rng = np.random.default_rng(seed)
    return pd.DataFrame({"value": rng.normal(8.0, 1.0, len(months))}, index=months)


def test_the_trial_reads_monthly_sources_by_their_rule(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = tr.load_gate(PROJECT_ROOT / "research/_trials/prereg.yaml")
    tsmom = load_signal("tsmom_21")
    config = {
        "function": "signal_exposure",
        "params": {
            "signals": [
                {
                    "id": "tsmom_21",
                    "module_sha256": tsmom.module_sha256,
                    "spec_sha256": tsmom.spec_sha256,
                    "score": "sign",
                }
            ],
            "mapping": "threshold",
            "low": 0.0,
            "high": 1.0,
        },
        "rebalance": {"schedule": "month_end", "band": 0.0},
    }
    plan = tr.TrialPlan.model_validate(
        {
            "schema_version": "research_trial_plan.v1",
            "trial_id": "T0001",
            "candidate": {"signals": ["tsmom_21"], "screens": ["S0001"]},
            "hypothesis_card": "test card",
            "from_validation_window": False,
            "data": {"snapshot_id": "qlh-test-snapshot", "signal_snapshot_id": "sig-0123456789ab"},
            "primary": "primary",
            "configs": [config | {"id": "primary"}, config | {"id": "half"}],
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    index = pd.bdate_range("1990-01-01", "1990-12-31", name="date")
    factors = pd.DataFrame({"Mkt-RF": 0.0003, "SMB": 0.0, "HML": 0.0, "RF": 0.0001}, index=index)
    baa = _monthly_frame("1989-01-01", "1990-12-01", 1)
    specs = (
        SourceSpec(
            source_id="ff_factors_daily",
            provider="ken_french",
            dataset="F-F_Research_Data_Factors_daily",
            frequency="daily",
            known_lag_trading_days=0,
            description="factors",
        ),
        SourceSpec(**MONTHLY),
    )
    snapshot = ss.SignalSnapshot(
        "sig-0123456789ab", {"ff_factors_daily": factors, "baa_monthly": baa}, specs
    )
    monkeypatch.setattr(ss, "load_snapshot", lambda root, snapshot_id: snapshot)
    monkeypatch.setattr(qlh, "resolve_snapshot_dir", lambda root, snapshot_id: root / snapshot_id)
    us = tr.market_data("us_market", gate, plan, snapshot_root=Path("."), project_root=PROJECT_ROOT)
    calendar = pd.DatetimeIndex(us.inputs.index)
    np.testing.assert_array_equal(
        us.inputs["source:baa_monthly:value"].to_numpy(),
        monthly_known_at(baa["value"], calendar, 0, 5).to_numpy(),
    )
    assert us.inputs["source:baa_monthly:value"].notna().mean() > 0.95


READER = """
def signal(returns, sources, params):
    return sources["baa_monthly"]["value"]
"""


def test_the_screen_reads_monthly_sources_by_their_rule(tmp_path: Path) -> None:
    spec = {
        "signal_id": "monthly_reader",
        "mechanism": "test signal",
        "expected_sign": 1,
        "target": "return",
        "params": {},
        "sources": ["baa_monthly"],
        "markets": "all",
        "literature": "none",
    }
    (tmp_path / "monthly_reader.py").write_text(READER, encoding="utf-8")
    (tmp_path / "monthly_reader.yaml").write_text(json.dumps(spec), encoding="utf-8")
    loaded = load_signal("monthly_reader", directory=tmp_path)
    rng = np.random.default_rng(4)
    us_index = pd.bdate_range("1960-01-01", "2012-12-31")
    market = pd.Series(rng.normal(0.0003, 0.01, len(us_index)), index=us_index)
    qqq_index = pd.bdate_range("1986-01-02", "2012-12-31")
    qqq = 1.3 * market.reindex(qqq_index) + pd.Series(
        rng.normal(0, 0.008, len(qqq_index)), index=qqq_index
    )
    markets = {
        "qqq": ScreenMarket("qqq", qqq, date(1999, 3, 10)),
        "us_market": ScreenMarket("us_market", market, date(1961, 7, 3)),
    }
    settings = load_screen_settings().settings.screen.model_copy(
        update={"bootstrap_draws": 50, "long_eval_start": date(1961, 7, 3)}
    )
    # The months start after both markets' calendars do, so each calendar sees the same months.
    baa = _monthly_frame("1987-01-01", "2012-12-01", 2)
    monthly = screen_signal(
        loaded, markets, {"baa_monthly": baa}, {"baa_monthly": SourceSpec(**MONTHLY)}, settings
    )
    end = pd.Timestamp(settings.screen_end)
    cut = baa.loc[baa.index <= end]
    by_hand = monthly_known_at(cut["value"], us_index[us_index <= end], 0, 5).to_frame("value")
    daily = screen_signal(loaded, markets, {"baa_monthly": by_hand}, {"baa_monthly": 0}, settings)
    assert json.dumps(monthly["markets"], sort_keys=True) == json.dumps(
        daily["markets"], sort_keys=True
    )


# 56 ----------------------------------------------------------------------------------------------


def _sheet(rows: list[list[Any]]) -> pd.DataFrame:
    title = [["U.S. Stock Markets 1871-Present", None, None, None, None, None, None, None]]
    upper = [[None, "S&P", None, None, "Consumer", None, "Long", None]]
    header = [["Date", "P", "D", "E", "CPI", "Date Fraction", "Rate GS10", "CAPE"]]
    notes = [[None] * 8, ["Note: data are monthly", None, None, None, None, None, None, None]]
    return pd.DataFrame(title + upper + header + rows + notes)


def test_the_shiller_sheet_is_read_by_its_headers() -> None:
    rows = [
        [1871.01, 4.44, 0.26, 0.4, 12.46, 1871.04, 5.32, None],
        [1871.02, 4.5, 0.26, 0.4, 12.84, 1871.13, 5.32, None],
        [1871.1, 4.6, "", 0.41, 12.9, 1871.79, 5.3, None],
        ["1871.11", 4.7, 0.27, 0.41, 12.8, 1871.88, 5.29, 10.5],
    ]
    table = ss.parse_shiller_table(_sheet(rows))
    assert list(table.index) == [
        pd.Timestamp("1871-01-01"),
        pd.Timestamp("1871-02-01"),
        pd.Timestamp("1871-10-01"),
        pd.Timestamp("1871-11-01"),
    ]
    assert set(table.columns) == {"P", "D", "E", "CPI", "GS10"}
    assert table.loc["1871-10-01", "P"] == 4.6
    assert np.isnan(table.loc["1871-10-01", "D"])
    assert table.loc["1871-11-01", "GS10"] == 5.29
    assert table.loc["1871-02-01", "CPI"] == 12.84
    assert table.dtypes.map(lambda dtype: dtype == np.float64).all()


def test_the_shiller_rows_end_at_the_first_row_without_a_month() -> None:
    rows = [
        [1871.01, 4.44, 0.26, 0.4, 12.46, 1871.04, 5.32, None],
        [None] * 8,
        [1871.02, 9.99, 9.99, 9.99, 99.9, 1871.13, 9.99, None],
    ]
    table = ss.parse_shiller_table(_sheet(rows))
    assert list(table.index) == [pd.Timestamp("1871-01-01")]
    assert table.loc["1871-01-01", "P"] == 4.44
    # A month written as text with one digit after the point is October, like the number y.1.
    text = ss.parse_shiller_table(_sheet([["1872.1", 5.0, 0.3, 0.5, 12.0, 1872.79, 5.2, None]]))
    assert list(text.index) == [pd.Timestamp("1872-10-01")]


def test_a_shiller_sheet_without_its_headers_or_rows_is_refused() -> None:
    rows = [[1871.01, 4.44, 0.26, 0.4, 12.46, 1871.04, 5.32, None]]
    missing = _sheet(rows)
    missing.iat[2, 6] = "Long Rate"
    with pytest.raises(ValueError):
        ss.parse_shiller_table(missing)
    doubled = _sheet(rows)
    doubled.iat[2, 7] = "P"
    with pytest.raises(ValueError):
        ss.parse_shiller_table(doubled)
    with pytest.raises(ValueError):
        ss.parse_shiller_table(_sheet([]))
    repeated = _sheet(rows + [[1871.01, 4.5, 0.26, 0.4, 12.5, 1871.1, 5.3, None]])
    with pytest.raises(ValueError):
        ss.parse_shiller_table(repeated)
    with pytest.raises(ValueError):
        ss.parse_shiller_table(pd.DataFrame([["no header here", 1.0]]))


# 57 ----------------------------------------------------------------------------------------------

MARK = 4321.8765  # a value that must never appear in printed output or check.md
COLUMNS = ("CPI", "D", "E", "GS10", "P")


def _shiller_table() -> pd.DataFrame:
    months = pd.date_range("1985-10-01", "1986-03-01", freq="MS", name="date")
    rng = np.random.default_rng(7)
    table = pd.DataFrame(
        {name: np.round(rng.normal(10.0, 1.0, len(months)), 2) for name in COLUMNS},
        index=months,
    )
    table.iloc[2, 0] = MARK
    return table


def _fred(seed: int = 0) -> dict[str, pd.Series]:
    dates = pd.bdate_range("1986-01-02", "1986-03-31")
    months = pd.date_range("1985-10-01", "1986-03-01", freq="MS")
    rng = np.random.default_rng(seed)
    daily = {
        name: pd.Series(np.round(rng.normal(5.0, 0.2, len(dates)), 2), index=dates)
        for name in ("BAA10Y", "T10Y3M", "VIXCLS", "VXNCLS", "DGS10", "DTB3")
    }
    monthly = {
        name: pd.Series(np.round(rng.normal(9.0, 0.3, len(months)), 2), index=months)
        for name in ("BAA", "AAA")
    }
    # The unemployment rates (TRADING-2565-88): drawn last, so nothing above changes.
    unemployment = {
        name: pd.Series(np.round(rng.normal(6.0, 0.5, len(months)), 1), index=months)
        for name in ("M0892AUSM156SNBR", "M0892BUSM156SNBR", "UNRATE")
    }
    return daily | monthly | unemployment


def _ken_french(dataset: str) -> bytes:
    if dataset == "F-F_Research_Data_Factors_daily":
        text = (
            ",Mkt-RF,SMB,HML,RF\n"
            "19860102, 0.10, 0.01, 0.02, 0.03\n"
            "19860103, 0.20, 0.01, 0.02, 0.03\n"
        )
    else:
        text = ",Agric,Food\n19860102, 0.10, 0.20\n19860103, 0.30, 0.40\n"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("data.CSV", text)
    return buffer.getvalue()


def _nvix_table() -> pd.DataFrame:
    """Manela's table as parsed (TRADING-2565-91): one missing month, no value used above."""

    months = pd.date_range("1985-10-01", "1986-03-01", freq="MS", name="date")
    values = np.round(np.random.default_rng(13).normal(20.0, 3.0, len(months)), 2)
    values[1] = np.nan
    values[3] = MARK
    return pd.DataFrame({"nvix": values}, index=months)


def _fetch(
    root: Path,
    calls: list[int] | None = None,
    table: pd.DataFrame | None = None,
    series: dict[str, pd.Series] | None = None,
    nvix: pd.DataFrame | None = None,
    nvix_calls: list[int] | None = None,
) -> str:
    fred = _fred() if series is None else series

    def shiller() -> pd.DataFrame:
        if calls is not None:
            calls.append(1)
        return (table if table is not None else _shiller_table()).copy()

    def manela() -> pd.DataFrame:
        if nvix_calls is not None:
            nvix_calls.append(1)
        return (nvix if nvix is not None else _nvix_table()).copy()

    return ss.fetch_snapshot(
        load_screen_settings(),
        root,
        fred_fetch=lambda dataset: fred[dataset].copy(),
        ken_french_fetch=_ken_french,
        shiller_fetch=shiller,
        shiller_url="https://example.invalid/ie_data.xls",
        manela_fetch=manela,
        manela_url="https://example.invalid/nvix.xlsx",
    )


def _directory(root: Path, snapshot: str) -> Path:
    return root / "data" / "raw" / "research" / "signal_sources" / snapshot


def test_a_snapshot_carries_the_monthly_sources(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    calls: list[int] = []
    snapshot = _fetch(tmp_path, calls)
    assert calls == [1]
    report = ss.check_snapshot(tmp_path, snapshot)
    assert report["status"] in {"PASS", "WARN"}
    for source_id in ("baa_monthly", "aaa_monthly", "shiller_price", "shiller_gs10"):
        assert report["sources"][source_id]["gap_count"] == 0
        assert report["sources"][source_id]["off_month_start_count"] == 0
    loaded = ss.load_snapshot(tmp_path, snapshot)
    table = _shiller_table()
    for source_id, column in (
        ("shiller_price", "P"),
        ("shiller_dividend", "D"),
        ("shiller_earnings", "E"),
        ("shiller_cpi", "CPI"),
        ("shiller_gs10", "GS10"),
    ):
        frame = loaded.frames[source_id]
        assert list(frame.columns) == ["value"]
        np.testing.assert_array_equal(frame["value"].to_numpy(), table[column].to_numpy())
        assert pd.DatetimeIndex(frame.index).equals(pd.DatetimeIndex(table.index))
    specs = {spec.source_id: spec for spec in loaded.specs}
    assert specs["shiller_earnings"].release_lag_months == 15
    assert specs["baa_monthly"].frequency == "monthly"
    fetched = json.loads((_directory(tmp_path, snapshot) / "fetch.json").read_text("utf-8"))
    endpoints = {item["source_id"]: item["endpoint"] for item in fetched["sources"]}
    assert endpoints["shiller_price"] == "https://example.invalid/ie_data.xls"
    assert endpoints["baa_monthly"] == ss.FredRateProvider.base_url
    assert _fetch(tmp_path / "again") == snapshot
    changed = _shiller_table()
    changed.iloc[4, 1] += 0.01
    assert _fetch(tmp_path / "changed", table=changed) != snapshot
    printed = capsys.readouterr()
    check_md = (_directory(tmp_path, snapshot) / "check.md").read_text(encoding="utf-8")
    for text in (printed.out, printed.err, check_md):
        assert "4321.8765" not in text and "4321.88" not in text


def test_shiller_sources_need_a_shiller_fetch(tmp_path: Path) -> None:
    fred = _fred()
    with pytest.raises(ValueError):
        ss.fetch_snapshot(
            load_screen_settings(),
            tmp_path,
            fred_fetch=lambda dataset: fred[dataset].copy(),
            ken_french_fetch=_ken_french,
        )
    with pytest.raises(ValueError):
        _fetch(tmp_path / "short", table=_shiller_table().drop(columns=["GS10"]))


def test_the_check_reads_monthly_dates_by_month(tmp_path: Path) -> None:
    gapped = _shiller_table().drop(index=pd.Timestamp("1985-12-01"))
    snapshot = _fetch(tmp_path / "gap", table=gapped)
    report = ss.check_snapshot(tmp_path / "gap", snapshot)
    assert report["status"] == "WARN"
    assert report["sources"]["shiller_price"]["gap_count"] == 1
    assert report["sources"]["baa_monthly"]["gap_count"] == 0  # weekdays do not count

    shifted = _shiller_table()
    shifted.index = shifted.index + pd.Timedelta(days=14)
    snapshot = _fetch(tmp_path / "mid", table=shifted)
    report = ss.check_snapshot(tmp_path / "mid", snapshot)
    assert report["status"] == "FAIL"
    assert report["sources"]["shiller_price"]["off_month_start_count"] == len(shifted)
    with pytest.raises(ValueError):
        ss.load_snapshot(tmp_path / "mid", snapshot)


def test_a_snapshot_written_before_monthly_sources_still_loads(tmp_path: Path) -> None:
    payload = b"date,value\n1986-01-02,5.1\n1986-01-03,5.2\n"
    spec = dict(DAILY)
    manifest = {
        "schema_version": "research_signal_snapshot.v1",
        "sources": [
            {
                "spec": spec,
                "filename": "baa10y.csv",
                "sha256": ss._digest(payload),
                "row_count": 2,
                "first_date": "1986-01-02",
                "last_date": "1986-01-03",
            }
        ],
    }
    snapshot = "sig-" + ss._digest(ss._canonical_json(manifest))[:12]
    directory = _directory(tmp_path, snapshot)
    directory.mkdir(parents=True)
    (directory / "baa10y.csv").write_bytes(payload)
    (directory / "manifest.json").write_bytes(ss._canonical_json(manifest) + b"\n")
    assert ss.check_snapshot(tmp_path, snapshot)["status"] == "PASS"
    loaded = ss.load_snapshot(tmp_path, snapshot)
    assert loaded.snapshot_id == snapshot
    assert loaded.specs[0].release_lag_months == 0
    assert loaded.frames["baa10y"]["value"].tolist() == [5.1, 5.2]


# 58 ----------------------------------------------------------------------------------------------


def test_the_approved_monthly_sources_are_in_the_settings_file() -> None:
    sources = {s.source_id: s for s in load_screen_settings().settings.sources}
    monthly = {key: spec for key, spec in sources.items() if spec.frequency == "monthly"}
    assert {
        key: (s.provider, s.dataset, s.release_lag_months, s.known_lag_trading_days)
        for key, s in monthly.items()
    } == {
        "baa_monthly": ("fred", "BAA", 0, 5),
        "aaa_monthly": ("fred", "AAA", 0, 5),
        "shiller_price": ("shiller", "P", 0, 0),
        "shiller_dividend": ("shiller", "D", 15, 0),
        "shiller_earnings": ("shiller", "E", 15, 0),
        "shiller_cpi": ("shiller", "CPI", 15, 0),
        "shiller_gs10": ("shiller", "GS10", 15, 0),
        "unrate_nber_a": ("fred", "M0892AUSM156SNBR", 0, 5),
        "unrate_nber_b": ("fred", "M0892BUSM156SNBR", 0, 5),
        "unrate": ("fred", "UNRATE", 0, 5),
        "nvix_monthly": ("manela", "nvix", 0, 5),
    }
    raw = yaml.safe_load((PROJECT_ROOT / "config/research/research_screen.yaml").read_text("utf-8"))
    written = [item for item in raw["sources"] if item["frequency"] == "monthly"]
    assert all("release_lag_months" in item for item in written)
    # The workbook's address on shillerdata.com, Shiller's data site, as listed on 2026-10-21; the
    # fetch command takes --shiller-url when the site lists a newer one.
    assert ss.SHILLER_URL == (
        "https://img1.wsimg.com/blobby/go/e5e77e0b-59d1-44d9-ab25-4763ac982e53/downloads/"
        "1449043e-a1ed-4e2e-8e9c-7b9a2a064493/ie_data.xls?ver=1791297298731"
    )
    # Manela's NVIX workbook as listed on his data page on 2026-10-29 (TRADING-2565-91).
    assert ss.MANELA_URL == (
        "https://asaf.manela.org/papers/mm/nvix/nvix_and_categories_timeseries_mar2016.xlsx"
    )


def test_the_us_market_base_is_unchanged_by_monthly_sources() -> None:
    index = pd.bdate_range("1990-01-01", "1990-03-30", name="date")
    factors = pd.DataFrame({"Mkt-RF": 0.001, "SMB": 0.0, "HML": 0.0, "RF": 0.0001}, index=index)
    alone = from_us_market({"ff_factors_daily": factors}, snapshot_id="sig-0123456789ab")
    both = from_us_market(
        {"ff_factors_daily": factors, "baa_monthly": _monthly_frame("1989-01-01", "1990-03-01", 3)},
        snapshot_id="sig-0123456789ab",
    )
    pd.testing.assert_frame_equal(alone.returns, both.returns)
    pd.testing.assert_frame_equal(alone.inputs, both.inputs)


# 68 ----------------------------------------------------------------------------------------------


def test_a_monthly_source_may_end_early_or_start_late(tmp_path: Path) -> None:
    fred = _fred()
    early = fred["M0892AUSM156SNBR"].iloc[:3]  # 1985-10..1985-12, nothing after
    late = fred["UNRATE"].iloc[4:]  # nothing before 1986-02
    middle = fred["M0892BUSM156SNBR"]
    snapshot = _fetch(tmp_path, series=fred | {"M0892AUSM156SNBR": early, "UNRATE": late})
    report = ss.check_snapshot(tmp_path, snapshot)
    assert report["status"] == "PASS"
    expected = {
        "unrate_nber_a": (early, "1985-10-01", "1985-12-01"),
        "unrate_nber_b": (middle, "1985-10-01", "1986-03-01"),
        "unrate": (late, "1986-02-01", "1986-03-01"),
    }
    loaded = ss.load_snapshot(tmp_path, snapshot)
    for source_id, (series, first, last) in expected.items():
        checked = report["sources"][source_id]
        assert checked["gap_count"] == 0
        assert (checked["first_date"], checked["last_date"]) == (first, last)
        assert checked["row_count"] == len(series)
        frame = loaded.frames[source_id]
        assert pd.DatetimeIndex(frame.index).equals(pd.DatetimeIndex(series.index))
        np.testing.assert_array_equal(frame["value"].to_numpy(), series.to_numpy())


# 69 ----------------------------------------------------------------------------------------------

MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
RELATIONS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE = "http://schemas.openxmlformats.org/package/2006/relationships"
PROLOG = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
COLUMN_LETTERS = "ABCDEFGH"


def _cell(ref: str, value: Any, strings: list[str], inline: bool) -> str:
    """A number, a text (shared or inline), or (type, inner XML) for any other kind of cell."""

    if value is None:
        return ""
    if isinstance(value, tuple):
        kind, inner = value
        return f'<c r="{ref}" t="{kind}">{inner}</c>' if kind else f'<c r="{ref}">{inner}</c>'
    if isinstance(value, str):
        if inline:
            return f'<c r="{ref}" t="inlineStr"><is><t>{value}</t></is></c>'
        strings.append(value)
        return f'<c r="{ref}" t="s"><v>{len(strings) - 1}</v></c>'
    return f'<c r="{ref}"><v>{value!r}</v></c>'


def _workbook(
    rows: list[list[Any]],
    *,
    sheet: str = "news_implied_volatility",
    inline: bool = False,
    absolute: bool = False,
    target: str | None = None,
    pad: int = 0,
    doctype: bool = False,
    broken: bool = False,
    first: int = 1,
) -> bytes:
    """A workbook laid out like Manela's: a readme sheet first, the data sheet second."""

    strings: list[str] = ["readme text"]
    body = "".join(
        f'<row r="{r}">'
        + "".join(
            _cell(f"{COLUMN_LETTERS[c]}{r}", value, strings, inline) for c, value in enumerate(row)
        )
        + "</row>"
        for r, row in enumerate(rows, start=first)
    )
    head = PROLOG + ('<!DOCTYPE worksheet [<!ENTITY note "x">]>\n' if doctype else "")
    data = f'{head}<worksheet xmlns="{MAIN}"><sheetData>{body}</sheetData>{" " * pad}</worksheet>'
    if broken:
        data = data[: len(data) // 2]
    readme = (
        f'{PROLOG}<worksheet xmlns="{MAIN}"><sheetData><row r="1">'
        '<c r="A1" t="s"><v>0</v></c></row></sheetData></worksheet>'
    )
    workbook = (
        f'{PROLOG}<workbook xmlns="{MAIN}" xmlns:r="{RELATIONS}"><workbookPr date1904="false"/>'
        f'<sheets><sheet name="readme" sheetId="1" r:id="rId1"/>'
        f'<sheet name="{sheet}" sheetId="2" r:id="rId2"/></sheets></workbook>'
    )
    folder = "/xl/" if absolute else ""
    paths = {1: f"{folder}worksheets/sheet1.xml", 2: target or f"{folder}worksheets/sheet2.xml"}
    rels = (
        f'{PROLOG}<Relationships xmlns="{PACKAGE}">'
        + "".join(
            f'<Relationship Id="rId{n}" Type="{RELATIONS}/worksheet" Target="{paths[n]}"/>'
            for n in (1, 2)
        )
        + f'<Relationship Id="rId3" Type="{RELATIONS}/sharedStrings" Target="sharedStrings.xml"/>'
        + "</Relationships>"
    )
    shared = "".join(f"<si><t>{text}</t></si>" for text in strings)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", rels)
        archive.writestr("xl/sharedStrings.xml", f'{PROLOG}<sst xmlns="{MAIN}">{shared}</sst>')
        archive.writestr("xl/worksheets/sheet1.xml", readme)
        archive.writestr("xl/worksheets/sheet2.xml", data)
        if target:  # the same sheet at the odd path too, so only the path rule can refuse it
            archive.writestr(target.lstrip("/") if target.startswith("/") else f"xl/{target}", data)
    return buffer.getvalue()


HEADER = ("Date", "NVIX", "Government", "War")


def _rows() -> list[list[Any]]:
    return [
        list(HEADER),
        [18890731, 20.5, 1.25, 0.5],
        [18890831, ("n", "<v>21.75</v>"), 1.0, "text in another column is not read"],
        [18890930, None, 1.0, 0.5],  # no NVIX cell
        [18891031, ("", ""), 1.0, 0.5],  # a cell without a value
        [18891130, "NaN", 1.0, 0.5],
        [18891231, " na ", 1.0, 0.5],
        [18900131, "#N/A", 1.0, 0.5],
        [18900228, ("e", "<v>#N/A</v>"), 1.0, 0.5],  # an error cell
        [18900331, 19.0, 1.0, 0.5],
        [18900430, MARK, None, None],
    ]


def test_the_nvix_workbook_is_read_by_its_sheet_and_headers() -> None:
    table = ss.parse_manela_workbook(_workbook(_rows()))
    assert list(table.columns) == ["nvix"]
    assert table.index.name == "date"
    assert pd.DatetimeIndex(table.index).equals(
        pd.date_range("1889-07-01", "1890-04-01", freq="MS")
    )
    assert table.dtypes.map(lambda dtype: dtype == np.float64).all()
    expected = [20.5, 21.75, *[np.nan] * 6, 19.0, MARK]
    np.testing.assert_array_equal(table["nvix"].to_numpy(), np.array(expected))


def test_the_nvix_headers_may_be_inline_text_in_any_case_and_order() -> None:
    rows = [[" nvix ", "War", "DATE"], [20.5, 0.5, 20101231], [21.5, 0.5, 20110131]]
    for workbook in (_workbook(rows, inline=True), _workbook(rows, absolute=True)):
        table = ss.parse_manela_workbook(workbook)
        assert list(table.index) == [pd.Timestamp("2010-12-01"), pd.Timestamp("2011-01-01")]
        assert table["nvix"].tolist() == [20.5, 21.5]
    blank = [
        list(HEADER),
        [18890731, 20.5, 1.0, 0.5],
        [None, None, 1.0, 0.5],
        [18890831, 21.5, 1.0],
    ]
    assert ss.parse_manela_workbook(_workbook(blank))["nvix"].tolist() == [20.5, 21.5]
    empty = [list(HEADER), [18890731, ""], [18890831, ("", "<v></v>")], [18890930, 1.5]]
    assert ss.parse_manela_workbook(_workbook(empty))["nvix"].isna().tolist() == [
        True,
        True,
        False,
    ]


def _bad(change: Callable[[list[list[Any]]], None]) -> list[list[Any]]:
    rows = _rows()
    change(rows)
    return rows


def _setter(row: int, column: int, value: Any) -> Callable[[list[list[Any]]], None]:
    def change(rows: list[list[Any]]) -> None:
        rows[row][column] = value

    return change


BAD_WORKBOOKS: dict[str, Callable[[], bytes]] = {
    "the sheet is missing": lambda: _workbook(_rows(), sheet="nvix"),
    "a sheet path that leaves xl/": lambda: _workbook(
        _rows(), target="../xl/worksheets/sheet2.xml"
    ),
    "a sheet path outside xl/": lambda: _workbook(_rows(), target="/docs/sheet2.xml"),
    "no NVIX header": lambda: _workbook(_bad(_setter(0, 1, "NVIX2"))),
    "two NVIX headers": lambda: _workbook(_bad(_setter(0, 3, "nvix"))),
    "no Date header": lambda: _workbook(_bad(_setter(0, 0, "Month"))),
    "the header is not on the first row": lambda: _workbook([["NVIX data"], *_rows()]),
    "a date that is not a whole number": lambda: _workbook(_bad(_setter(2, 0, 18890831.5))),
    "a date that is not a day": lambda: _workbook(_bad(_setter(2, 0, 43218765))),
    "a date written as text": lambda: _workbook(_bad(_setter(2, 0, "1889-08"))),
    "a row without a date": lambda: _workbook(_bad(_setter(2, 0, None))),
    "a skipped month": lambda: _workbook(_bad(lambda rows: rows.pop(3))),
    "a date that is not the last day of its month": lambda: _workbook(
        _bad(_setter(2, 0, 18890815))
    ),
    "a repeated month": lambda: _workbook(_bad(_setter(2, 0, 18890731))),
    "an earlier month": lambda: _workbook(_bad(_setter(10, 0, 18890430))),
    "other text in the NVIX column": lambda: _workbook(_bad(_setter(4, 1, "4321.8765x"))),
    "no data rows": lambda: _workbook([list(HEADER)]),
    "a value that is not a number": lambda: _workbook(
        _bad(_setter(4, 1, ("", "<v>4321.8765x</v>")))
    ),
    "a date that is not a number": lambda: _workbook(_bad(_setter(2, 0, ("n", "<v>4321x</v>")))),
    "a shared string that is not there": lambda: _workbook(
        _bad(_setter(4, 1, ("s", "<v>4321</v>")))
    ),
    "a shared string index that is not a number": lambda: _workbook(
        _bad(_setter(4, 1, ("s", "<v>4321x</v>")))
    ),
    "the headers are not on row 1": lambda: _workbook(_rows(), first=2),
    "a member over 20 MB": lambda: _workbook(_rows(), pad=21 * 1024 * 1024),
    "a document type": lambda: _workbook(_rows(), doctype=True),
    "broken XML": lambda: _workbook(_rows(), broken=True),
    "not a zip": lambda: b"<html>4321.8765</html>",
}


@pytest.mark.parametrize("case", sorted(BAD_WORKBOOKS))
def test_a_bad_nvix_workbook_is_refused_without_naming_a_value(case: str) -> None:
    with pytest.raises(ValueError) as refused:
        ss.parse_manela_workbook(BAD_WORKBOOKS[case]())
    for text in ("4321", "43218765", "18890815", "18890831.5", "1889-08"):
        assert text not in str(refused.value)


# 70 ----------------------------------------------------------------------------------------------


def test_a_snapshot_carries_nvix(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    calls: list[int] = []
    snapshot = _fetch(tmp_path, nvix_calls=calls)
    assert calls == [1]
    report = ss.check_snapshot(tmp_path, snapshot)
    assert report["status"] in {"PASS", "WARN"}
    checked = report["sources"]["nvix_monthly"]
    assert (checked["gap_count"], checked["off_month_start_count"]) == (0, 0)
    assert checked["nonempty_ratio"] == {"value": 5 / 6}
    loaded = ss.load_snapshot(tmp_path, snapshot)
    frame = loaded.frames["nvix_monthly"]
    table = _nvix_table()
    assert pd.DatetimeIndex(frame.index).equals(pd.DatetimeIndex(table.index))
    np.testing.assert_array_equal(frame["value"].to_numpy(), table["nvix"].to_numpy())
    spec = {s.source_id: s for s in loaded.specs}["nvix_monthly"]
    assert (spec.provider, spec.dataset, spec.frequency) == ("manela", "nvix", "monthly")
    fetched = json.loads((_directory(tmp_path, snapshot) / "fetch.json").read_text("utf-8"))
    endpoints = {item["source_id"]: item["endpoint"] for item in fetched["sources"]}
    assert endpoints["nvix_monthly"] == "https://example.invalid/nvix.xlsx"
    assert endpoints["shiller_price"] == "https://example.invalid/ie_data.xls"
    changed = _nvix_table()
    changed.iloc[4, 0] += 0.01
    assert _fetch(tmp_path / "changed", nvix=changed) != snapshot
    printed = capsys.readouterr()
    check_md = (_directory(tmp_path, snapshot) / "check.md").read_text(encoding="utf-8")
    for text in (printed.out, printed.err, check_md):
        assert "4321.8765" not in text and "4321.88" not in text


def test_manela_sources_need_a_manela_fetch(tmp_path: Path) -> None:
    fred = _fred()
    with pytest.raises(ValueError):
        ss.fetch_snapshot(
            load_screen_settings(),
            tmp_path,
            fred_fetch=lambda dataset: fred[dataset].copy(),
            ken_french_fetch=_ken_french,
            shiller_fetch=_shiller_table,
        )
    with pytest.raises(ValueError):
        _fetch(tmp_path / "short", nvix=_nvix_table().rename(columns={"nvix": "war"}))


def test_manela_specs_are_the_monthly_nvix_column_with_a_lag() -> None:
    assert SourceSpec(**MANELA).provider == "manela"
    for bad in (
        {"dataset": "NVIX"},
        {"dataset": "war"},
        {"frequency": "daily", "release_lag_months": 0},
        {"known_lag_trading_days": 0},
    ):
        with pytest.raises(ValueError):
            SourceSpec(**(MANELA | bad))


class _Response:
    def __init__(self, content: bytes, status: int = 200) -> None:
        self.content = content
        self.status = status

    def raise_for_status(self) -> None:
        if self.status != 200:
            raise requests.HTTPError(f"{self.status} Forbidden")


def test_the_manela_workbook_is_downloaded_with_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[tuple[str, dict[str, Any]]] = []

    def get(url: str, **options: Any) -> _Response:
        asked.append((url, options))
        return _Response(_workbook(_rows()))

    monkeypatch.setattr(ss.requests, "get", get)
    table = ss._manela_fetch("https://example.invalid/nvix.xlsx")
    pd.testing.assert_frame_equal(table, ss.parse_manela_workbook(_workbook(_rows())))
    assert asked == [("https://example.invalid/nvix.xlsx", {"timeout": 60})]
    ss._manela_fetch()
    assert asked[-1] == (ss.MANELA_URL, {"timeout": 60})
    monkeypatch.setattr(ss.requests, "get", lambda url, **options: _Response(b"", 403))
    with pytest.raises(requests.HTTPError):
        ss._manela_fetch()


def test_the_fetch_command_takes_a_manela_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, Any] = {}
    urls: list[str] = []
    monkeypatch.setattr(
        ss, "fetch_snapshot", lambda settings, root, **options: seen.update(options)
    )
    monkeypatch.setattr(ss, "_manela_fetch", lambda url=ss.MANELA_URL: urls.append(url))
    address = "https://example.invalid/new_nvix.xlsx"
    outcome = CliRunner().invoke(
        ss.app, ["fetch", "--root", str(tmp_path), "--manela-url", address]
    )
    assert outcome.exit_code == 0, outcome.output
    assert seen["manela_url"] == address
    seen["manela_fetch"]()
    assert urls == [address]
    CliRunner().invoke(ss.app, ["fetch", "--root", str(tmp_path)])
    assert seen["manela_url"] == ss.MANELA_URL
