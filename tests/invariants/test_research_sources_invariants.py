"""Invariants of the research data intake for signal screening (TRADING-2565 S4a).

Acceptance tests written before the code (owner decision
``owner_decision:TRADING-2565:2026-10-13:signal_screen_layer``). Every value below is synthetic;
no test reads a real snapshot.

28. ``known_at`` uses only what was published: a value dated d is known ``lag`` trading days
    later, missing values are not carried, and a value older than ``max_stale_days`` is dropped.
    Changing anything dated after d leaves every value known by then unchanged.
29. The Ken French parser reads the first block only, turns percent into decimals and the
    missing-value codes into NaN.
30. Snapshots are content addressed: the same bytes give the same id in any directory, and one
    changed value gives a different id.
31. ``load_snapshot`` refuses an unchecked snapshot, a failed check, and a file changed after the
    check.
32. Nothing printed and nothing in ``check.md`` carries a data value.
33. Source specs are strict, the approved sources are the ones in the settings file, and
    ``config/research/research_bench.yaml`` is unchanged.

The edge cases at the end (stale limit, off-calendar dates, bad downloads, the id, every check
status, value leaks, loaded frames, strict specs, the gap length) were added after mutation checks
on a throwaway reference.
"""

from __future__ import annotations

import hashlib
import io
import re
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_bench.screen_settings import load_screen_settings
from ai_trading_system.research_bench.sources import SourceSpec, known_at
from ai_trading_system.research_data import signal_sources as ss

BENCH_SETTINGS_SHA256 = "69351836d2e2bacb8b2888fe783a0949f485af9bf3e0294ffdb78ba50d0c46c2"
CAL = pd.bdate_range("2020-01-06", periods=10)  # Mon 6 .. Fri 17 January 2020


# 28 ----------------------------------------------------------------------------------------------


def _source() -> pd.Series:
    dates = pd.to_datetime(
        ["2020-01-06", "2020-01-07", "2020-01-08", "2020-01-09", "2020-01-13", "2020-01-14"]
        + ["2020-01-15", "2020-01-16", "2020-01-17"]
    )
    return pd.Series([1.0, 2.0, np.nan, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0], index=dates)


def test_known_at_follows_the_publication_lag() -> None:
    one_day = known_at(_source(), CAL, lag=1)
    assert one_day.index.equals(CAL)
    expected = [np.nan, 1.0, 2.0, 2.0, 4.0, 4.0, 5.0, 6.0, 7.0, 8.0]
    np.testing.assert_array_equal(one_day.to_numpy(), np.array(expected))
    same_day = known_at(_source(), CAL, lag=0)
    expected_same = [1.0, 2.0, 2.0, 4.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
    np.testing.assert_array_equal(same_day.to_numpy(), np.array(expected_same))


def test_known_at_drops_stale_values() -> None:
    short = _source().loc[:"2020-01-09"]
    values = known_at(short, CAL, lag=1, max_stale_days=2)
    # On Jan 13 (lag 1: as of Jan 10) the last value is Jan 9's, one day old; on Jan 14 (as of
    # Jan 13) two days old; on Jan 15 (as of Jan 14) three days old, too stale.
    assert values.loc["2020-01-13"] == 4.0
    assert values.loc["2020-01-14"] == 4.0
    assert np.isnan(values.loc["2020-01-15"])
    assert np.isnan(values.loc["2020-01-17"])


@pytest.mark.parametrize("lag", [0, 1, 3])
def test_known_at_never_uses_a_later_value(lag: int) -> None:
    calendar = pd.bdate_range("2001-01-01", "2003-12-31")
    rng = np.random.default_rng(lag)
    source = pd.Series(rng.normal(size=len(calendar)), index=calendar)
    source = source.drop(source.sample(frac=0.05, random_state=lag).index)
    base = known_at(source, calendar, lag=lag)
    for cut in (100, 400, 700):
        changed = source.copy()
        changed[changed.index > calendar[cut]] = 1e9
        moved = known_at(changed, calendar, lag=lag)
        known_by = cut + lag
        np.testing.assert_array_equal(
            moved.to_numpy()[: known_by + 1], base.to_numpy()[: known_by + 1]
        )
        assert not np.array_equal(moved.to_numpy(), base.to_numpy(), equal_nan=True)


# 29 ----------------------------------------------------------------------------------------------

FACTORS_CSV = """This file was created by CMPT_ME_BEME_RETS_DAILY using the 202407 CRSP database.
The Tbill return is the simple daily rate that, over the number of trading days
in the month, compounds to 1-month TBill rate from Ibbotson and Associates Inc.

,Mkt-RF,SMB,HML,RF
19260701,    0.10,   -0.25,   -0.27,    0.01
19260702,    0.45,   -0.33,   -0.06,    0.01
19260706,  -999,     0.12,    0.20,    0.01

Copyright 2024 Kenneth R. French
"""

INDUSTRIES_CSV = """This file was created by CMPT_IND_RETS_DAILY using the 202407 CRSP database.
It contains value- and equal-weighted returns for 49 industry portfolios.

  Average Value Weighted Returns -- Daily
         ,Agric ,Food  ,Soda
19260701,   0.56,  -0.12, -99.99
19260702,  -1.20,   0.30,   0.25

  Average Equal Weighted Returns -- Daily
         ,Agric ,Food  ,Soda
19260701,   9.99,   9.99,   9.99
19260702,   9.99,   9.99,   9.99
"""


def _zip(name: str, text: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text)
    return buffer.getvalue()


def test_the_ken_french_parser_reads_the_first_block_in_decimals() -> None:
    factors = ss.parse_ken_french_csv(FACTORS_CSV)
    assert list(factors.columns) == ["Mkt-RF", "SMB", "HML", "RF"]
    assert list(factors.index) == list(pd.to_datetime(["1926-07-01", "1926-07-02", "1926-07-06"]))
    assert factors.loc["1926-07-01", "Mkt-RF"] == pytest.approx(0.001)
    assert factors.loc["1926-07-02", "SMB"] == pytest.approx(-0.0033)
    assert np.isnan(factors.loc["1926-07-06", "Mkt-RF"])

    industries = ss.parse_ken_french_csv(INDUSTRIES_CSV)
    assert list(industries.columns) == ["Agric", "Food", "Soda"]
    assert len(industries) == 2
    assert industries.loc["1926-07-01", "Agric"] == pytest.approx(0.0056)
    assert industries.loc["1926-07-02", "Agric"] == pytest.approx(-0.012)
    assert np.isnan(industries.loc["1926-07-01", "Soda"])
    assert np.nanmax(industries.to_numpy()) < 0.05  # the equal-weighted block (9.99) is not read


def test_the_csv_comes_out_of_the_zip() -> None:
    data = _zip("F-F_Research_Data_Factors_daily.CSV", FACTORS_CSV)
    assert ss.extract_csv(data) == FACTORS_CSV
    two = io.BytesIO()
    with zipfile.ZipFile(two, "w") as archive:
        archive.writestr("a.CSV", FACTORS_CSV)
        archive.writestr("b.CSV", FACTORS_CSV)
    with pytest.raises(ValueError):
        ss.extract_csv(two.getvalue())


# 30 ----------------------------------------------------------------------------------------------

MARK = 1234.5678  # a value that must never appear in printed output or check.md


def _fred(values: dict[str, pd.Series]) -> Callable[[str], pd.Series]:
    def fetch(series_id: str) -> pd.Series:
        return values[series_id].copy()

    return fetch


def _fred_series(seed: int = 0) -> dict[str, pd.Series]:
    dates = pd.bdate_range("1986-01-02", "1986-03-31")
    rng = np.random.default_rng(seed)
    baa = pd.Series(np.round(rng.normal(2.0, 0.1, len(dates)), 2), index=dates)
    baa.iloc[10] = MARK
    t10y3m = pd.Series(np.round(rng.normal(1.0, 0.2, len(dates)), 2), index=dates)
    return {"BAA10Y": baa, "T10Y3M": t10y3m}


def _ken_french() -> Callable[[str], bytes]:
    files = {
        "F-F_Research_Data_Factors_daily": _zip("factors.CSV", FACTORS_CSV),
        "49_Industry_Portfolios_daily": _zip("industries.CSV", INDUSTRIES_CSV),
    }
    return lambda dataset: files[dataset]


def _fetch(root: Path, fred: dict[str, pd.Series] | None = None) -> str:
    return ss.fetch_snapshot(
        load_screen_settings(),
        root,
        fred_fetch=_fred(fred or _fred_series()),
        ken_french_fetch=_ken_french(),
    )


def test_snapshots_are_content_addressed(tmp_path: Path) -> None:
    first = _fetch(tmp_path / "a")
    second = _fetch(tmp_path / "b")
    assert first == second
    assert first.startswith("sig-")
    changed = _fred_series()
    changed["T10Y3M"].iloc[5] += 0.01
    third = _fetch(tmp_path / "c", changed)
    assert third != first
    directory = tmp_path / "a" / "data" / "raw" / "research" / "signal_sources" / first
    assert (directory / "manifest.json").is_file()


# 31 ----------------------------------------------------------------------------------------------


def test_load_refuses_unchecked_failed_and_altered_snapshots(tmp_path: Path) -> None:
    snapshot = _fetch(tmp_path)
    with pytest.raises(ValueError):
        ss.load_snapshot(tmp_path, snapshot)
    result = ss.check_snapshot(tmp_path, snapshot)
    assert result["status"] in {"PASS", "WARN"}
    loaded = ss.load_snapshot(tmp_path, snapshot)
    assert set(loaded.frames) == {s.source_id for s in load_screen_settings().settings.sources}
    assert loaded.snapshot_id == snapshot

    directory = tmp_path / "data" / "raw" / "research" / "signal_sources" / snapshot
    csv = next(directory.glob("baa10y*.csv"))
    csv.write_bytes(csv.read_bytes().replace(b"1986", b"1987", 1))
    with pytest.raises(ValueError):
        ss.load_snapshot(tmp_path, snapshot)

    duplicated = _fred_series()
    baa = duplicated["BAA10Y"]
    duplicated["BAA10Y"] = pd.concat([baa, baa.iloc[[3]]]).sort_index()
    bad = _fetch(tmp_path / "dup", duplicated)
    failed = ss.check_snapshot(tmp_path / "dup", bad)
    assert failed["status"] == "FAIL"
    with pytest.raises(ValueError):
        ss.load_snapshot(tmp_path / "dup", bad)


# 32 ----------------------------------------------------------------------------------------------


def test_no_data_value_is_printed_or_written_to_check_md(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    snapshot = _fetch(tmp_path)
    ss.check_snapshot(tmp_path, snapshot)
    printed = capsys.readouterr()
    directory = tmp_path / "data" / "raw" / "research" / "signal_sources" / snapshot
    check_md = (directory / "check.md").read_text(encoding="utf-8")
    for text in (printed.out, printed.err, check_md):
        for token in ("1234.5678", "1234.57", "0.0056", "-0.0033", "0.56", "-1.20"):
            assert token not in text


# 33 ----------------------------------------------------------------------------------------------

GOOD: dict[str, Any] = {
    "source_id": "baa10y",
    "provider": "fred",
    "dataset": "BAA10Y",
    "frequency": "daily",
    "known_lag_trading_days": 1,
    "description": "credit spread",
}


@pytest.mark.parametrize(
    "bad",
    [
        {"description": None},
        {"extra": 1},
        {"known_lag_trading_days": 0},
        {"source_id": "BAA 10Y"},
        {"provider": "yahoo"},
        {"provider": "ken_french", "dataset": "Some_Other_Data_daily", "known_lag_trading_days": 0},
    ],
)
def test_source_specs_are_strict(bad: dict[str, Any]) -> None:
    values = {key: value for key, value in (GOOD | bad).items() if value is not None}
    with pytest.raises(ValueError):
        SourceSpec(**values)


def test_the_approved_sources_are_in_the_settings_file() -> None:
    sources = {s.source_id: s for s in load_screen_settings().settings.sources}
    assert set(sources) == {"baa10y", "t10y3m", "ff_factors_daily", "ff_49_industries_daily"}
    assert (sources["baa10y"].provider, sources["baa10y"].dataset) == ("fred", "BAA10Y")
    assert (sources["t10y3m"].provider, sources["t10y3m"].dataset) == ("fred", "T10Y3M")
    assert sources["ff_factors_daily"].dataset == "F-F_Research_Data_Factors_daily"
    assert sources["ff_49_industries_daily"].dataset == "49_Industry_Portfolios_daily"
    assert sources["baa10y"].known_lag_trading_days == 1
    assert sources["t10y3m"].known_lag_trading_days == 1
    assert sources["ff_factors_daily"].known_lag_trading_days == 0
    assert sources["ff_49_industries_daily"].known_lag_trading_days == 0


def test_the_bench_settings_file_is_unchanged() -> None:
    path = PROJECT_ROOT / "config" / "research" / "research_bench.yaml"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == BENCH_SETTINGS_SHA256


# Edge cases (added after mutation checks) --------------------------------------------------------


def test_known_at_drops_values_older_than_five_trading_days_by_default() -> None:
    source = pd.Series([1.0], index=pd.to_datetime(["2020-01-06"]))
    values = known_at(source, CAL, lag=0)
    assert values.loc["2020-01-13"] == 1.0  # position 5: five days old, kept
    assert np.isnan(values.loc["2020-01-14"])  # position 6: six days old


def test_known_at_does_not_carry_missing_values_past_the_stale_limit() -> None:
    source = pd.Series(
        [1.0, np.nan, np.nan, np.nan, np.nan, 6.0], index=pd.bdate_range("2020-01-06", periods=6)
    )
    values = known_at(source, CAL, lag=0, max_stale_days=2)
    expected = [1.0, 1.0, 1.0, np.nan, np.nan, 6.0, 6.0, 6.0, np.nan, np.nan]
    np.testing.assert_array_equal(values.to_numpy(), np.array(expected))


def test_known_at_places_an_off_calendar_date_on_the_calendar_day_before() -> None:
    # Saturday 11 January sits at Friday 10 January's position (searchsorted right minus one).
    source = pd.Series([1.0, 2.0], index=pd.to_datetime(["2020-01-06", "2020-01-11"]))
    values = known_at(source, CAL, lag=0, max_stale_days=1)
    expected = [1.0, 1.0, np.nan, np.nan, np.nan, 2.0, np.nan, np.nan, np.nan, np.nan]
    np.testing.assert_array_equal(values.to_numpy(), np.array(expected))


def test_a_zip_without_exactly_one_csv_is_refused() -> None:
    with pytest.raises(ValueError):
        ss.extract_csv(_zip("readme.txt", "no table here"))


def test_bytes_that_are_not_a_zip_are_refused() -> None:
    with pytest.raises(ValueError):
        ss.extract_csv(b"<html>404 Not Found</html>")


def _today() -> set[str]:
    now, local = datetime.now(tz=UTC), datetime.now()
    return {day.strftime(fmt) for day in (now, local) for fmt in ("%Y-%m-%d", "%Y%m%d")}


def test_the_id_is_twelve_hex_digits_and_the_manifest_has_no_fetch_time(tmp_path: Path) -> None:
    before = _today()
    snapshot = _fetch(tmp_path)
    after = _today()
    assert re.fullmatch(r"sig-[0-9a-f]{12}", snapshot)
    directory = tmp_path / "data" / "raw" / "research" / "signal_sources" / snapshot
    manifest = (directory / "manifest.json").read_text(encoding="utf-8")
    for day in before | after:
        assert day not in manifest
    assert (directory / "fetch.json").is_file()


def _fetch_checked(root: Path, fred: dict[str, pd.Series]) -> tuple[str, dict[str, Any]]:
    snapshot = _fetch(root, fred)
    return snapshot, ss.check_snapshot(root, snapshot)


def test_check_fails_unordered_empty_and_altered_sources_and_warns_on_gaps(
    tmp_path: Path,
) -> None:
    _, clean = _fetch_checked(tmp_path / "clean", _fred_series())
    assert clean["status"] == "PASS"

    unordered = _fred_series()
    baa = unordered["BAA10Y"]
    unordered["BAA10Y"] = baa.iloc[[1, 0, *range(2, len(baa))]]
    snapshot, result = _fetch_checked(tmp_path / "unordered", unordered)
    assert result["status"] == "FAIL"
    with pytest.raises(ValueError):
        ss.load_snapshot(tmp_path / "unordered", snapshot)

    empty = _fred_series()
    empty["T10Y3M"] = empty["T10Y3M"].iloc[:0]
    snapshot, result = _fetch_checked(tmp_path / "empty", empty)
    assert result["status"] == "FAIL"

    gap = _fred_series()
    gap["T10Y3M"] = gap["T10Y3M"].drop(gap["T10Y3M"].index[20:30])  # ten business days missing
    snapshot, result = _fetch_checked(tmp_path / "gap", gap)
    assert result["status"] == "WARN"
    assert set(ss.load_snapshot(tmp_path / "gap", snapshot).frames) == {
        s.source_id for s in load_screen_settings().settings.sources
    }

    snapshot = _fetch(tmp_path / "altered")
    directory = tmp_path / "altered" / "data" / "raw" / "research" / "signal_sources" / snapshot
    csv = next(directory.glob("t10y3m*.csv"))
    rows = csv.read_bytes().split(b"\n")
    rows[5] = rows[5].split(b",")[0] + b",9.75"  # one value changed; dates still in order
    csv.write_bytes(b"\n".join(rows))
    assert ss.check_snapshot(tmp_path / "altered", snapshot)["status"] == "FAIL"


MARKS = {"1234.5678": 1234.5678, "4321.8765": 4321.8765, "5678.1234": 5678.1234}


def test_no_value_leaks_from_first_last_or_duplicated_rows(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """32 with marks on the first and last rows and a duplicated marked row: neither the screen,
    check.md nor check.json shows them (check.json is under the same rule as check.md)."""

    marked = _fred_series()
    marked["BAA10Y"].iloc[0] = MARKS["4321.8765"]
    marked["BAA10Y"].iloc[-1] = MARKS["5678.1234"]
    marked["T10Y3M"].iloc[-1] = MARKS["5678.1234"]
    duplicated = {key: value.copy() for key, value in marked.items()}
    baa = duplicated["BAA10Y"]
    duplicated["BAA10Y"] = pd.concat([baa, baa.iloc[[10]]]).sort_index()  # the 1234.5678 row
    texts: list[str] = []
    for name, series in (("marked", marked), ("duplicated", duplicated)):
        snapshot, _ = _fetch_checked(tmp_path / name, series)
        directory = tmp_path / name / "data" / "raw" / "research" / "signal_sources" / snapshot
        texts.append((directory / "check.md").read_text(encoding="utf-8"))
        texts.append((directory / "check.json").read_text(encoding="utf-8"))
    printed = capsys.readouterr()
    texts.extend([printed.out, printed.err])
    for text in texts:
        for token, value in MARKS.items():
            assert token not in text
            assert f"{value:.2f}" not in text


def test_loaded_frames_are_the_fetched_values_on_their_dates(tmp_path: Path) -> None:
    """Frames are indexed by date, as ``parse_ken_french_csv`` returns them."""

    series = _fred_series()
    snapshot, _ = _fetch_checked(tmp_path, series)
    loaded = ss.load_snapshot(tmp_path, snapshot)
    baa = loaded.frames["baa10y"]
    assert pd.DatetimeIndex(baa.index).equals(pd.DatetimeIndex(series["BAA10Y"].index))
    np.testing.assert_allclose(baa.iloc[:, 0].to_numpy(dtype=float), series["BAA10Y"].to_numpy())
    factors = loaded.frames["ff_factors_daily"]
    expected = ss.parse_ken_french_csv(FACTORS_CSV)
    assert list(factors.columns) == list(expected.columns)
    assert pd.DatetimeIndex(factors.index).equals(pd.DatetimeIndex(expected.index))
    np.testing.assert_allclose(
        factors.to_numpy(dtype=float), expected.to_numpy(dtype=float), rtol=1e-9, equal_nan=True
    )


@pytest.mark.parametrize(
    "bad",
    [
        {"description": ""},
        {"dataset": ""},
        {
            "provider": "ken_french",
            "dataset": "F-F_Research_Data_Factors_daily",
            "known_lag_trading_days": -1,
        },
    ],
)
def test_source_specs_refuse_empty_text_and_negative_lags(bad: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        SourceSpec(**(GOOD | bad))


def test_duplicate_source_ids_are_refused(tmp_path: Path) -> None:
    raw = yaml.safe_load(
        (PROJECT_ROOT / "config" / "research" / "research_screen.yaml").read_text("utf-8")
    )
    raw["sources"].append(dict(raw["sources"][0]))
    path = tmp_path / "screen.yaml"
    path.write_text(yaml.safe_dump(raw), encoding="utf-8")
    with pytest.raises(ValueError):
        load_screen_settings(path)


def test_a_gap_is_more_than_five_missing_business_days(tmp_path: Path) -> None:
    five = _fred_series()
    five["T10Y3M"] = five["T10Y3M"].drop(five["T10Y3M"].index[20:25])
    assert _fetch_checked(tmp_path / "five", five)[1]["status"] == "PASS"
    six = _fred_series()
    six["T10Y3M"] = six["T10Y3M"].drop(six["T10Y3M"].index[20:26])
    assert _fetch_checked(tmp_path / "six", six)[1]["status"] == "WARN"
