"""DATA-034-01: a split the price cache missed is refetched, not mixed into the history.

The 2026-11-10 12:00 JST daily (as_of 2026-11-09) stopped at validate_data: SOXX split
three-for-one with split-adjusted trading from 2026-11-05, the daily price refresh only fetched
the tail, so the cached SOXX rows up to 2026-11-03 kept the pre-split adjusted close while the
rows from 2026-11-04 came on the new basis (adj_close -67.7% on 2026-11-04).

The primary price download now re-reads the last 20 cached rows of every ticker; when a re-read
adjusted close differs from the cache by more than 10 %, the whole cached history of that ticker
is fetched again in the same run (in pieces of at most 1200 calendar days) and replaces the
cached rows. Registering the split itself is DATA-034-02 (tests/test_data034_soxx_known_split.py).
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_universe
from ai_trading_system.data.download import (
    PRICE_HISTORY_REBASE_ADJ_CLOSE_REL_TOLERANCE,
    PRICE_HISTORY_REBASE_CHUNK_CALENDAR_DAYS,
    PRICE_HISTORY_REBASE_COVERAGE_SHORTFALL,
    PRICE_HISTORY_REBASE_OVERLAP_ROWS,
    DataDownloadSummary,
    _price_fetch_windows,
    download_daily_data,
)
from ai_trading_system.data.download_publication import resolve_download_publication
from ai_trading_system.data.market_data import PriceRequest, RateRequest
from ai_trading_system.trading_calendar import is_us_equity_trading_day

REPO_ROOT = Path(__file__).resolve().parents[1]
SPLIT_DATE = date(2026, 11, 5)
HISTORY_START = date(2026, 9, 1)
PRE_SPLIT_CLOSE = 300.0
POST_SPLIT_CLOSE = 100.0
PRICE_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]


def _trading_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    current = start
    while current <= end:
        if is_us_equity_trading_day(current):
            days.append(current)
        current += timedelta(days=1)
    return days


def _row(value_date: date, ticker: str, close: float, adj_close: float) -> dict[str, object]:
    return {
        "date": value_date.isoformat(),
        "ticker": ticker,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "adj_close": adj_close,
        "volume": 1000,
    }


class SplitHistoryProvider:
    """FMP-like rows: raw close (non-split-adjusted) and adjusted close for SOXX.

    mode "pre": before the split, adj_close equals the raw close.
    mode "partial": the provider adjusted only rows from 2026-11-04 on (what the
    2026-11-10 capture saw); older rows still look like the pre-split capture.
    mode "post": the whole SOXX history is on the split-adjusted basis.
    Every other ticker is flat at 1.0. ``drop_dates`` are left out of SOXX-only requests.
    """

    def __init__(self) -> None:
        self.mode = "pre"
        self.drop_dates: set[date] = set()
        self.calls: list[PriceRequest] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.calls.append(request)
        rows: list[dict[str, object]] = []
        for current in _trading_days(request.start, request.end):
            for ticker in request.tickers:
                if ticker != "SOXX":
                    rows.append(_row(current, ticker, 1.0, 1.0))
                    continue
                if request.tickers == ["SOXX"] and current in self.drop_dates:
                    continue
                close = PRE_SPLIT_CLOSE if current < SPLIT_DATE else POST_SPLIT_CLOSE
                adjusted = current < SPLIT_DATE and (
                    self.mode == "post" or (self.mode == "partial" and current >= date(2026, 11, 4))
                )
                rows.append(_row(current, ticker, close, close / 3.0 if adjusted else close))
        return pd.DataFrame(rows, columns=PRICE_COLUMNS)


class ScaledHistoryProvider:
    """SOXX at 300; rows on or before ``scaled_through`` get adj_close = 300 * ``factor``.

    ``row_factors`` overrides the factor on single dates.
    """

    def __init__(self) -> None:
        self.factor = 1.0
        self.scaled_through = date(1900, 1, 1)
        self.row_factors: dict[date, float] = {}
        self.calls: list[PriceRequest] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.calls.append(request)
        rows: list[dict[str, object]] = []
        for current in _trading_days(request.start, request.end):
            for ticker in request.tickers:
                if ticker != "SOXX":
                    rows.append(_row(current, ticker, 1.0, 1.0))
                    continue
                factor = self.row_factors.get(
                    current, self.factor if current <= self.scaled_through else 1.0
                )
                rows.append(_row(current, ticker, 300.0, 300.0 * factor))
        return pd.DataFrame(rows, columns=PRICE_COLUMNS)


class FlatRateProvider:
    def download_rates(self, request: RateRequest) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"date": request.end.isoformat(), "series": series, "value": 4.5}
                for series in request.series_ids
            ]
        )


def _download(
    tmp_path: Path,
    provider: SplitHistoryProvider | ScaledHistoryProvider,
    end: date,
    start: date = HISTORY_START,
) -> DataDownloadSummary:
    return download_daily_data(
        load_universe(),
        start=start,
        end=end,
        output_dir=tmp_path,
        price_provider=provider,
        rate_provider=FlatRateProvider(),
    )


def _prices(summary: DataDownloadSummary) -> pd.DataFrame:
    return pd.read_csv(summary.prices_path, dtype={"date": str, "ticker": str})


def _soxx(summary: DataDownloadSummary) -> pd.DataFrame:
    frame = _prices(summary)
    return frame.loc[frame["ticker"] == "SOXX"].sort_values("date").reset_index(drop=True)


def _incremental_refresh(summary: DataDownloadSummary) -> dict[str, object]:
    assert summary.publication_manifest_path is not None
    transaction = json.loads(summary.publication_manifest_path.read_text(encoding="utf-8"))
    live = [
        source
        for source in transaction["source_event_records"]
        if source.get("artifact_role") == "prices" and source.get("source_kind") == "LIVE_PROVIDER"
    ]
    assert len(live) == 1
    incremental = live[0]["request_parameters"]["incremental_refresh"]
    assert isinstance(incremental, dict)
    return incremental


def _source_winning_rows(summary: DataDownloadSummary, source_kind: str) -> int:
    assert summary.publication_manifest_path is not None
    transaction = json.loads(summary.publication_manifest_path.read_text(encoding="utf-8"))
    matches = [
        source
        for source in transaction["source_event_records"]
        if source.get("artifact_role") == "prices" and source.get("source_kind") == source_kind
    ]
    assert len(matches) == 1
    return int(matches[0]["winning_row_count"])


def test_overlap_constants() -> None:
    assert PRICE_HISTORY_REBASE_OVERLAP_ROWS == 20
    assert PRICE_HISTORY_REBASE_ADJ_CLOSE_REL_TOLERANCE == 0.10
    assert PRICE_HISTORY_REBASE_CHUNK_CALENDAR_DAYS == 1200
    assert PRICE_HISTORY_REBASE_COVERAGE_SHORTFALL == "PRICE_HISTORY_REBASE_COVERAGE_SHORTFALL"


def test_fetch_windows_start_at_the_overlap_row_only_when_asked() -> None:
    long_days = _trading_days(date(2026, 9, 1), date(2026, 11, 9))
    short_days = _trading_days(date(2026, 11, 2), date(2026, 11, 6))
    rows = [_row(day, "AAA", 1.0, 1.0) for day in long_days]
    rows += [_row(day, "BBB", 1.0, 1.0) for day in short_days]
    rows += [_row(day, "CCC", 1.0, 1.0) for day in _trading_days(date(2026, 11, 2), SPLIT_DATE)]
    rows += [_row(date(2026, 11, 10), "CCC", 1.0, 1.0)]
    existing = pd.DataFrame(rows, columns=PRICE_COLUMNS)
    tickers = ("AAA", "BBB", "CCC", "DDD")
    end = date(2026, 11, 10)

    overlap = _price_fetch_windows(
        existing,
        tickers=tickers,
        start=date(2026, 9, 1),
        end=end,
        overlap_rows=20,
    )
    assert [(window.tickers, window.start, window.end) for window in overlap] == [
        (("DDD",), date(2026, 9, 1), end),
        (("AAA",), long_days[-20], end),
        (("BBB",), date(2026, 11, 2), end),
    ]

    tail_only = _price_fetch_windows(
        existing,
        tickers=tickers,
        start=date(2026, 9, 1),
        end=end,
    )
    assert [(window.tickers, window.start, window.end) for window in tail_only] == [
        (("DDD",), date(2026, 9, 1), end),
        (("BBB",), date(2026, 11, 9), end),
        (("AAA",), end, end),
    ]
    with pytest.raises(ValueError):
        _price_fetch_windows(
            existing,
            tickers=tickers,
            start=date(2026, 9, 1),
            end=end,
            overlap_rows=-1,
        )


def test_split_after_the_cached_rows_refetches_the_whole_ticker(tmp_path: Path) -> None:
    provider = SplitHistoryProvider()
    first = _download(tmp_path, provider, date(2026, 11, 3))
    cached_days = _trading_days(HISTORY_START, date(2026, 11, 3))
    all_days = _trading_days(HISTORY_START, date(2026, 11, 10))
    first_prices = _prices(first)

    provider.mode = "post"
    second = _download(tmp_path, provider, date(2026, 11, 10))

    # One overlap window for every ticker, then one SOXX-only refetch from its first cached day.
    assert [(call.start, call.end) for call in provider.calls[1:]] == [
        (cached_days[-20], date(2026, 11, 10)),
        (HISTORY_START, date(2026, 11, 10)),
    ]
    assert provider.calls[2].tickers == ["SOXX"]

    expected_rebase = {
        "ticker": "SOXX",
        "check_date": cached_days[-20].isoformat(),
        "cached_adj_close": 300.0,
        "provider_adj_close": 100.0,
        "refetch_start": HISTORY_START.isoformat(),
        "refetch_end": "2026-11-10",
        "replaced_row_count": len(cached_days),
        "refetched_row_count": len(all_days),
    }
    assert second.price_history_rebases == (expected_rebase,)
    incremental = _incremental_refresh(second)
    assert incremental["history_rebase_check"] == {
        "overlap_rows": 20,
        "adj_close_rel_tolerance": 0.10,
    }
    assert incremental["history_rebases"] == [expected_rebase]
    fetch_windows = incremental["fetch_windows"]
    assert isinstance(fetch_windows, list)
    assert [window["start"] for window in fetch_windows] == [
        cached_days[-20].isoformat(),
        HISTORY_START.isoformat(),
    ]

    soxx = _soxx(second)
    assert list(soxx["date"]) == [day.isoformat() for day in all_days]
    assert soxx["adj_close"].pct_change().abs().max() == pytest.approx(0.0)
    assert set(soxx.loc[soxx["date"] < "2026-11-05", "close"]) == {PRE_SPLIT_CLOSE}
    assert set(soxx.loc[soxx["date"] >= "2026-11-05", "close"]) == {POST_SPLIT_CLOSE}

    # Other tickers keep their cached rows; SOXX rows all come from the live provider.
    prices = _prices(second)
    others_before = first_prices.loc[first_prices["ticker"] != "SOXX"].reset_index(drop=True)
    others_after = prices.loc[
        (prices["ticker"] != "SOXX") & (prices["date"] <= "2026-11-03")
    ].reset_index(drop=True)
    pd.testing.assert_frame_equal(others_after, others_before)
    reused = _source_winning_rows(second, "CANONICAL_PREDECESSOR_REUSE")
    assert reused == len(first_prices) - len(cached_days)
    assert _source_winning_rows(second, "LIVE_PROVIDER") == len(prices) - reused
    assert incremental["reused_row_count"] == reused


def test_the_2026_11_10_cache_shape_heals_on_the_next_run(tmp_path: Path) -> None:
    provider = SplitHistoryProvider()
    _download(tmp_path, provider, date(2026, 11, 3))

    # The provider re-adjusted only the new rows: the overlap rows still match the cache, so
    # nothing is refetched and the cache ends up as production did on 2026-11-10.
    provider.mode = "partial"
    incident = _download(tmp_path, provider, date(2026, 11, 9))
    assert incident.price_history_rebases == ()
    soxx = _soxx(incident).set_index("date")
    assert soxx.loc["2026-11-03", "adj_close"] == pytest.approx(PRE_SPLIT_CLOSE)
    assert soxx.loc["2026-11-04", "close"] == pytest.approx(PRE_SPLIT_CLOSE)
    assert soxx.loc["2026-11-04", "adj_close"] == pytest.approx(POST_SPLIT_CLOSE)
    assert _soxx(incident)["adj_close"].pct_change().min() < -0.5

    provider.mode = "post"
    healed = _download(tmp_path, provider, date(2026, 11, 10))

    cached_days = _trading_days(HISTORY_START, date(2026, 11, 9))
    assert [item["ticker"] for item in healed.price_history_rebases] == ["SOXX"]
    rebase = healed.price_history_rebases[0]
    assert rebase["check_date"] == cached_days[-20].isoformat()
    assert rebase["cached_adj_close"] == pytest.approx(PRE_SPLIT_CLOSE)
    assert rebase["provider_adj_close"] == pytest.approx(POST_SPLIT_CLOSE)
    assert rebase["replaced_row_count"] == len(cached_days)
    healed_soxx = _soxx(healed)
    assert healed_soxx["adj_close"].pct_change().abs().max() == pytest.approx(0.0)
    assert healed_soxx["date"].iloc[-1] == "2026-11-10"


@pytest.mark.parametrize(
    ("factor", "refetched"),
    [
        (0.95, False),  # dividend-sized re-adjustment: keep the cache
        (1.05, False),
        (0.85, True),
        (10.0, True),  # reverse split
    ],
)
def test_only_split_sized_differences_refetch(
    tmp_path: Path,
    factor: float,
    refetched: bool,
) -> None:
    provider = ScaledHistoryProvider()
    first = _download(tmp_path, provider, date(2026, 11, 3))
    first_soxx = _soxx(first)

    provider.factor = factor
    provider.scaled_through = date(2026, 11, 3)
    second = _download(tmp_path, provider, date(2026, 11, 10))
    soxx = _soxx(second)
    old = soxx.loc[soxx["date"] <= "2026-11-03"].reset_index(drop=True)
    new = soxx.loc[soxx["date"] > "2026-11-03"]

    assert set(new["adj_close"]) == {300.0}
    assert len(new) == len(_trading_days(date(2026, 11, 4), date(2026, 11, 10)))
    if refetched:
        assert [item["ticker"] for item in second.price_history_rebases] == ["SOXX"]
        assert len(provider.calls) == 3
        assert list(old["adj_close"]) == pytest.approx([300.0 * factor] * len(old))
    else:
        assert second.price_history_rebases == ()
        assert _incremental_refresh(second)["history_rebases"] == []
        assert len(provider.calls) == 2
        # The cached rows win over the re-read rows when the basis did not move.
        pd.testing.assert_frame_equal(old, first_soxx)


def test_rebase_record_names_the_row_that_differs_most(tmp_path: Path) -> None:
    provider = ScaledHistoryProvider()
    _download(tmp_path, provider, date(2026, 11, 3))

    # Three re-read rows differ by more than 10%; two tie for the largest difference.
    provider.row_factors = {
        date(2026, 10, 28): 0.85,
        date(2026, 10, 30): 0.5,
        date(2026, 11, 2): 0.5,
    }
    second = _download(tmp_path, provider, date(2026, 11, 10))

    assert len(second.price_history_rebases) == 1
    rebase = second.price_history_rebases[0]
    assert rebase["ticker"] == "SOXX"
    assert rebase["check_date"] == "2026-10-30"  # the earliest of the two largest differences
    assert rebase["cached_adj_close"] == pytest.approx(300.0)
    assert rebase["provider_adj_close"] == pytest.approx(150.0)


def test_long_history_is_refetched_in_chunks_of_1200_days(tmp_path: Path) -> None:
    provider = SplitHistoryProvider()
    start = date(2022, 1, 3)
    _download(tmp_path, provider, date(2026, 11, 3), start=start)

    provider.mode = "post"
    # The run's own start is earlier than the cached history: the refetch still starts at the
    # ticker's first cached day (the daily refresh never fills head gaps).
    second = _download(tmp_path, provider, date(2026, 11, 10), start=date(2021, 6, 1))

    refetch_calls = provider.calls[2:]
    assert [(call.start, call.end) for call in refetch_calls] == [
        (date(2022, 1, 3), date(2025, 4, 16)),
        (date(2025, 4, 17), date(2026, 11, 10)),
    ]
    assert all(call.tickers == ["SOXX"] for call in refetch_calls)
    rebase = second.price_history_rebases[0]
    assert rebase["refetch_start"] == "2022-01-03"
    assert rebase["refetch_end"] == "2026-11-10"
    assert rebase["refetched_row_count"] == len(_trading_days(start, date(2026, 11, 10)))
    soxx = _soxx(second)
    assert list(soxx["date"]) == [
        day.isoformat() for day in _trading_days(start, date(2026, 11, 10))
    ]
    assert soxx["adj_close"].pct_change().abs().max() == pytest.approx(0.0)


def test_refetch_that_misses_a_cached_day_fails_closed(tmp_path: Path) -> None:
    provider = SplitHistoryProvider()
    first = _download(tmp_path, provider, date(2026, 11, 3))
    before = resolve_download_publication(output_dir=tmp_path).transaction_id
    assert before == first.publication_transaction_id

    provider.mode = "post"
    provider.drop_dates = {date(2026, 9, 15)}
    with pytest.raises(ValueError) as raised:
        _download(tmp_path, provider, date(2026, 11, 10))

    message = str(raised.value)
    assert message.startswith(PRICE_HISTORY_REBASE_COVERAGE_SHORTFALL)
    assert "SOXX" in message
    assert "2026-09-15" in message
    assert resolve_download_publication(output_dir=tmp_path).transaction_id == before


def test_download_cli_prints_each_history_refetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ai_trading_system.cli_commands import data_cache as data_cache_cli

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    rebase: dict[str, object] = {
        "ticker": "SOXX",
        "check_date": "2026-10-12",
        "cached_adj_close": 465.0,
        "provider_adj_close": 155.0,
        "refetch_start": "2018-01-02",
        "refetch_end": "2026-11-10",
        "replaced_row_count": 2224,
        "refetched_row_count": 2229,
    }

    def fake_download_daily_data(*_args: object, **_kwargs: object) -> DataDownloadSummary:
        return DataDownloadSummary(
            prices_path=tmp_path / "prices_daily.csv",
            rates_path=tmp_path / "rates_daily.csv",
            manifest_path=tmp_path / "download_manifest.csv",
            price_rows=1,
            rate_rows=1,
            price_tickers=("SOXX",),
            rate_series=("DGS2",),
            price_history_rebases=(rebase,),
        )

    monkeypatch.setattr(data_cache_cli, "download_daily_data", fake_download_daily_data)
    result = CliRunner().invoke(
        app,
        ["download-data", "--output-dir", str(tmp_path), "--end", "2026-11-10"],
    )

    assert result.exit_code == 0, result.output
    assert (
        "价格历史整段重取（DATA-034）：SOXX check_date=2026-10-12 cached_adj_close=465.0 "
        "provider_adj_close=155.0 refetch=2018-01-02..2026-11-10 replaced_rows=2224 "
        "refetched_rows=2229"
    ) in result.output.replace("\n", "")
