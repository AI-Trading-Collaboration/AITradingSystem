from __future__ import annotations

import json
import math
import re
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT, load_data_sources
from ai_trading_system.data.market_data import FmpPriceProvider, PriceRequest, RateRequest
from ai_trading_system.research_data import qqq_long_history as qlh

REQUEST_END = date(2020, 6, 30)


class FakeProvider:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.requests: list[RateRequest] = []

    def download_rates(self, request: RateRequest) -> pd.DataFrame:
        self.requests.append(request)
        return self.frame[self.frame["series"].isin(request.series_ids)].copy()


class FakePriceProvider:
    def __init__(self, prices: pd.DataFrame) -> None:
        self.prices = prices
        self.requests: list[PriceRequest] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        self.requests.append(request)
        dates = pd.to_datetime(self.prices["date"]).dt.date
        in_window = (dates >= request.start) & (dates <= request.end)
        return self.prices[self.prices["ticker"].isin(request.tickers) & in_window].copy()


def _write_spec(tmp_path: Path) -> Path:
    raw = yaml.safe_load(qlh.DEFAULT_SPEC_PATH.read_text(encoding="utf-8"))
    raw["request_start"] = "2019-12-01"
    first_dates = {
        "NASDAQXNDX": "2020-01-02",
        "NASDAQ100": "2019-12-02",
        "DTB3": "2019-12-02",
        "VIXCLS": "2019-12-02",
        "VXNCLS": "2020-02-03",
        "VXOCLS": "2019-12-02",
    }
    for item in raw["series"]:
        item["expected_first_date"] = first_dates[item["series_id"]]
    raw["etf_reference"]["expected_first_date"] = "2020-01-02"
    path = tmp_path / "qqq_long_history.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return path


def _series_frame(drop_dates: tuple[str, ...] = ()) -> pd.DataFrame:
    dates = pd.bdate_range("2019-12-02", "2020-06-30")
    dates = dates[~dates.isin(pd.to_datetime(list(drop_dates)))]
    steps = pd.Series(range(len(dates)), index=dates, dtype=float)
    price = 8000 * (1 + 0.004 * steps.map(math.sin)).cumprod()
    total_return = price * (1 + 0.00004) ** steps * 1.25
    rows: list[pd.DataFrame] = []

    def add(series_id: str, values: pd.Series) -> None:
        rows.append(
            pd.DataFrame(
                {
                    "date": values.index.strftime("%Y-%m-%d"),
                    "series": series_id,
                    "value": values.to_numpy(),
                }
            )
        )

    add("NASDAQ100", price)
    add("NASDAQXNDX", total_return[total_return.index >= "2020-01-02"])
    add("DTB3", pd.Series(1.5, index=dates))
    add("VIXCLS", 15 + steps % 7)
    add("VXNCLS", (18 + steps % 5)[steps.index >= "2020-02-03"])
    add("VXOCLS", (16 + steps % 3)[steps.index <= "2020-05-01"])
    return pd.concat(rows, ignore_index=True)


def _write_prices(
    tmp_path: Path, spec_path: Path, frame: pd.DataFrame, drift: dict[str, float]
) -> Path:
    daily = qlh.derive_daily(frame, qlh.load_spec(spec_path).spec)
    rows = []
    for ticker, column in (("QQQ", "qqq_proxy"), ("QLD", "lev2"), ("TQQQ", "lev3")):
        returns = daily[f"{column}_return"].dropna() + drift.get(ticker, 0.0)
        rows.append(_price_rows(ticker, 100 * (1 + returns).cumprod()))
    path = tmp_path / "prices_daily.csv"
    pd.concat(rows, ignore_index=True).to_csv(path, index=False)
    return path


def _price_rows(ticker: str, levels: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": levels.index.strftime("%Y-%m-%d"),
            "ticker": ticker,
            "open": levels.to_numpy(),
            "high": levels.to_numpy(),
            "low": levels.to_numpy(),
            "close": levels.to_numpy(),
            "adj_close": levels.to_numpy(),
            "volume": 1000,
        }
    )


def _reference_prices(spec_path: Path, frame: pd.DataFrame, drift: float = 0.0) -> pd.DataFrame:
    """Actual QQQ that tracks the proxy exactly, plus ``drift`` per day."""

    daily = qlh.derive_daily(frame, qlh.load_spec(spec_path).spec)
    returns = daily["qqq_proxy_return"].dropna() + drift
    first = returns.index[0] - pd.offsets.BDay(1)
    levels = pd.concat([pd.Series([100.0], index=[first]), 100 * (1 + returns).cumprod()])
    return _price_rows("QQQ", levels)


def _fetch(
    tmp_path: Path,
    spec_path: Path,
    frame: pd.DataFrame,
    prices: pd.DataFrame | None = None,
) -> Path:
    if prices is None:
        prices = _reference_prices(spec_path, frame)
    return qlh.fetch_snapshot(
        qlh.load_spec(spec_path),
        end=REQUEST_END,
        provider=FakeProvider(frame),
        price_provider=FakePriceProvider(prices),
        root=tmp_path / "snapshots",
    )


def test_real_spec_names_the_reviewed_series_and_funds() -> None:
    loaded = qlh.load_spec()
    spec = loaded.spec

    assert {item.series_id: item.expected_first_date for item in spec.series} == {
        "NASDAQXNDX": date(1999, 3, 4),
        "NASDAQ100": date(1986, 1, 2),
        "DTB3": date(1986, 1, 2),
        "VIXCLS": date(1990, 1, 2),
        "VXNCLS": date(2001, 2, 2),
        "VXOCLS": date(1986, 1, 2),
    }
    assert spec.owner_decision == "owner_decision:TRADING-2567:2026-10-12:qqq_direction_1999_window"
    assert {fund.fund_id: fund.calibration_ticker for fund in spec.derivation.synthetic_funds} == {
        "lev2": "QLD",
        "lev3": "TQQQ",
    }
    assert spec.source.redistribution_allowed is False
    assert spec.source.external_llm_allowed is False
    assert spec.etf_reference.ticker == "QQQ"
    assert spec.etf_reference.expected_first_date == date(1999, 3, 10)
    assert spec.etf_reference.source.redistribution_allowed is False
    assert spec.etf_reference.source.external_llm_allowed is False
    # Calibrated on TQQQ after the first snapshot (TRADING-2567 progress, 2026-10-13).
    assert spec.derivation.financing_spread_annual == 0.0125


def test_data_source_entries_match_the_spec_and_stay_out_of_reconciliation() -> None:
    sources = {source.source_id: source for source in load_data_sources().sources}
    spec = qlh.load_spec().spec

    for source_spec in (spec.source, spec.etf_reference.source):
        source = sources[source_spec.source_id]
        assert source.provider == source_spec.provider
        assert source.endpoint == source_spec.endpoint
        assert source.status == "active"
        assert source.domains == ["research_history"]
        assert source.cadence == "manual"
        assert source.llm_permission.external_llm_allowed is False
        assert source.llm_permission.redistribution_allowed is False
        assert source.cache_paths == [
            qlh.DEFAULT_SNAPSHOT_ROOT.relative_to(PROJECT_ROOT).as_posix()
        ]


def test_window_registry_and_policy_split_the_qqq_question_windows() -> None:
    registry = yaml.safe_load(
        (PROJECT_ROOT / "config/research/research_window_registry.yaml").read_text(encoding="utf-8")
    )["windows"]
    policy = yaml.safe_load(
        (PROJECT_ROOT / "config/research/primary_research_window_policy.yaml").read_text(
            encoding="utf-8"
        )
    )
    roles = policy["question_windows"]["qqq_direction"]
    development = registry[roles["development"]]
    validation = registry[roles["validation"]]
    consistency = registry[roles["consistency_check"]]

    assert development["start"] == date(1999, 3, 10)
    assert development["end"] < validation["start"]
    assert validation["end"] < consistency["start"] == date(2021, 2, 22)
    assert len(pd.bdate_range(development["end"], validation["start"])) <= 2
    assert len(pd.bdate_range(validation["end"], consistency["start"])) == 2
    assert validation["read_policy"] == "once_per_preregistered_question"
    assert "parameter_selection" in validation["blocked_usage"]
    assert "qqq_question_consistency_check" in consistency["allowed_usage"]
    spec_id = qlh.load_spec().spec.source.source_id
    assert development["data_source"] == validation["data_source"] == spec_id
    assert policy["primary_research_window_policy"]["default_start"] == date(2021, 2, 22)


def test_fetch_writes_a_content_addressed_snapshot_once(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()

    first = _fetch(tmp_path, spec_path, frame)
    manifest = json.loads((first / qlh.MANIFEST_FILE).read_text(encoding="utf-8"))
    again = _fetch(tmp_path, spec_path, frame.sample(frac=1, random_state=1))

    assert again == first
    assert json.loads((again / qlh.MANIFEST_FILE).read_text(encoding="utf-8")) == manifest
    assert first.name.startswith("qlh-20200630-")
    assert manifest["series_csv_sha256"].startswith(first.name.rsplit("-", 1)[1])
    assert manifest["series"]["NASDAQXNDX"]["first_date"] == "2020-01-02"
    assert manifest["request_parameters"]["start"] == "2019-12-01"
    assert manifest["data_policy"]["redistribution_allowed"] is False
    assert manifest["series"]["QQQ"]["first_date"] == "2020-01-02"
    assert manifest["etf_reference"]["source_id"] == "fmp_qqq_long_history_research"
    assert manifest["request_parameters"]["etf_tickers"] == ["QQQ"]
    assert manifest["request_parameters"]["etf_requests"] == [
        {"start": "2020-01-02", "end": "2020-06-30"}
    ]
    assert (tmp_path / "snapshots" / qlh.LATEST_POINTER).read_text().strip() == first.name
    assert b"\r\n" not in (first / qlh.SERIES_FILE).read_bytes()


def test_fetch_refuses_when_a_series_is_missing(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()

    with pytest.raises(ValueError, match="VXNCLS"):
        _fetch(tmp_path, spec_path, frame[frame["series"] != "VXNCLS"])
    prices = _reference_prices(spec_path, frame)
    with pytest.raises(ValueError, match="QQQ"):
        _fetch(tmp_path, spec_path, frame, prices[prices["ticker"] != "QQQ"])
    assert not (tmp_path / "snapshots").exists()


def test_etf_reference_requests_cover_the_range_in_overlapping_windows() -> None:
    requests = qlh.etf_reference_requests("QQQ", date(1999, 3, 10), date(2026, 10, 13))

    assert [(item.start, item.end) for item in requests] == [
        (date(1999, 3, 10), date(2005, 1, 8)),
        (date(2005, 1, 1), date(2011, 1, 8)),
        (date(2011, 1, 1), date(2017, 1, 8)),
        (date(2017, 1, 1), date(2023, 1, 8)),
        (date(2023, 1, 1), date(2026, 10, 13)),
    ]
    assert all(item.tickers == ["QQQ"] for item in requests)
    # Every window holds fewer trading days than the 1,715 one FMP request returned in full.
    assert max(len(pd.bdate_range(item.start, item.end)) for item in requests) < 1715
    assert qlh.etf_reference_requests("QQQ", date(2020, 1, 2), date(2020, 6, 30))[0].end == date(
        2020, 6, 30
    )


def test_etf_reference_download_joins_windows_and_stops_on_disagreement() -> None:
    levels = pd.Series(1.0, index=pd.bdate_range("1999-03-10", "2012-12-31")).cumsum()
    prices = _price_rows("QQQ", levels)
    requests = qlh.etf_reference_requests("QQQ", date(1999, 3, 10), date(2012, 12, 31))

    joined = qlh._download_reference(FakePriceProvider(prices), requests)

    assert len(requests) == 3
    assert joined["date"].tolist() == prices["date"].tolist()
    assert joined["adj_close"].tolist() == prices["adj_close"].tolist()

    class ShiftedProvider(FakePriceProvider):
        def download_prices(self, request: PriceRequest) -> pd.DataFrame:
            rows = super().download_prices(request)
            if len(self.requests) == 2:
                rows["adj_close"] = rows["adj_close"] * 1.01
            return rows

    with pytest.raises(ValueError, match=r"disagree on 10 overlapping days \(first 2005-01-03\)"):
        qlh._download_reference(ShiftedProvider(prices), requests)
    gap = prices[~prices["date"].between("2005-01-01", "2011-12-31")]
    with pytest.raises(ValueError, match="no QQQ rows for 2005-01-01..2011-01-08"):
        qlh._download_reference(FakePriceProvider(gap), requests)


def test_fmp_price_provider_reads_the_key_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(qlh.FMP_API_KEY_ENV, raising=False)
    with pytest.raises(RuntimeError, match="FMP_API_KEY is not set"):
        qlh.fmp_price_provider()

    monkeypatch.setenv(qlh.FMP_API_KEY_ENV, "test-key")
    provider = qlh.fmp_price_provider()

    assert isinstance(provider, FmpPriceProvider)
    assert provider.api_key == "test-key"
    assert qlh.price_provider_factory is qlh.fmp_price_provider


def test_read_snapshot_detects_edited_data(tmp_path: Path) -> None:
    snapshot = _fetch(tmp_path, _write_spec(tmp_path), _series_frame())
    series_path = snapshot / qlh.SERIES_FILE
    series_path.write_bytes(series_path.read_bytes().replace(b"1.5", b"1.6", 1))

    with pytest.raises(qlh.SnapshotIntegrityError):
        qlh.read_snapshot(snapshot)


def test_derive_daily_arithmetic_on_fixed_numbers(tmp_path: Path) -> None:
    spec = qlh.load_spec(_write_spec(tmp_path)).spec
    frame = pd.DataFrame(
        [
            ("2020-01-02", "NASDAQ100", 100.0),
            ("2020-01-03", "NASDAQ100", 101.0),
            ("2020-01-06", "NASDAQ100", 102.01),
            ("2020-01-02", "NASDAQXNDX", 200.0),
            ("2020-01-03", "NASDAQXNDX", 202.2),
            ("2020-01-06", "NASDAQXNDX", 204.6),
            ("2020-01-02", "DTB3", 1.8),
            ("2020-01-06", "DTB3", 2.0),
            ("2020-01-06", "VIXCLS", 14.0),
        ],
        columns=["date", "series", "value"],
    )

    daily = qlh.derive_daily(frame, spec)
    monday = daily.loc["2020-01-06"]
    tr_return = 204.6 / 202.2 - 1
    spread = spec.derivation.financing_spread_annual

    assert monday["calendar_days"] == 3
    assert monday["ndx_tr_return"] == pytest.approx(tr_return)
    # Friday has no T-bill print; Monday accrues Friday's carried-forward 1.8%, not Monday's 2.0%.
    assert monday["cash_return"] == pytest.approx(0.018 * 3 / 360)
    assert monday["qqq_proxy_return"] == pytest.approx(tr_return - 0.002 * 3 / 365)
    assert monday["lev3_return"] == pytest.approx(
        3 * tr_return - 2 * (0.018 + spread) * 3 / 360 - 0.0095 * 3 / 365
    )
    assert monday["lev2_return"] == pytest.approx(
        2 * tr_return - 1 * (0.018 + spread) * 3 / 360 - 0.0095 * 3 / 365
    )
    assert monday["vix"] == 14.0
    assert math.isnan(daily.loc["2020-01-03", "vix"])
    assert math.isnan(daily.loc["2020-01-02", "lev3_return"])


def test_derive_daily_drops_price_only_dates_after_total_return_starts(tmp_path: Path) -> None:
    spec = qlh.load_spec(_write_spec(tmp_path)).spec
    frame = pd.DataFrame(
        [
            ("2019-12-31", "NASDAQ100", 99.0),
            ("2020-01-01", "NASDAQ100", 99.0),
            ("2020-01-02", "NASDAQ100", 100.0),
            ("2020-01-03", "NASDAQ100", 100.0),
            ("2020-01-06", "NASDAQ100", 102.0),
            ("2020-01-02", "NASDAQXNDX", 200.0),
            ("2020-01-06", "NASDAQXNDX", 204.6),
            ("2019-12-31", "DTB3", 1.8),
        ],
        columns=["date", "series", "value"],
    )

    daily = qlh.derive_daily(frame, spec)

    # Before the first total-return date the price index alone sets the calendar.
    assert list(daily.index.strftime("%Y-%m-%d")) == [
        "2019-12-31",
        "2020-01-01",
        "2020-01-02",
        "2020-01-06",
    ]
    monday = daily.loc["2020-01-06"]
    assert monday["calendar_days"] == 4
    assert monday["ndx_price_return"] == pytest.approx(0.02)
    assert monday["ndx_tr_return"] == pytest.approx(204.6 / 200 - 1)
    assert monday["cash_return"] == pytest.approx(0.018 * 4 / 360)


def test_repair_stale_levels_rebuilds_carried_over_levels_from_the_other_index() -> None:
    repaired = qlh.repair_stale_levels(
        [100.0, 101.0, 102.01, 103.0, 103.0, 103.004, 104.0],
        [200.0, 202.0, 202.0, 206.06, 206.5, 206.5, 208.0],
        0.0001,
    )

    # 2: total return carried over while the price index rose 1% -> rebuilt from that 1%.
    assert repaired.total_return[2] == pytest.approx(202.0 * 102.01 / 101.0)
    # 4: price carried over while total return moved -> rebuilt from the total-return move.
    assert repaired.price[4] == pytest.approx(103.0 * 206.5 / 206.06)
    # 5: total return repeated, but the price move (0.004%) is within the rounding allowance.
    assert repaired.total_return_repaired == [False, False, True, False, False, False, False]
    assert repaired.price_repaired == [False, False, False, False, True, False, False]
    # Real levels are untouched, so the next date carries the rest of the move.
    assert repaired.total_return[3] == 206.06
    assert repaired.price[6] == 104.0

    both = qlh.repair_stale_levels([100.0, 100.0], [200.0, 200.0], 0.0001)
    assert both.price_repaired == both.total_return_repaired == [False, False]


def test_check_reports_repaired_stale_levels(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()
    stale = ("2020-03-03", "2020-03-04", "2020-05-12")
    total_return = frame["series"] == "NASDAQXNDX"
    for day in stale:
        previous = frame.loc[total_return & (frame["date"] < day), "value"].iloc[-1]
        frame.loc[total_return & (frame["date"] == day), "value"] = previous
    snapshot = _fetch(tmp_path, spec_path, frame)

    report = qlh.check_snapshot(
        snapshot, qlh.load_spec(spec_path), prices_path=tmp_path / "missing.csv"
    )
    codes = {issue["code"]: issue for issue in report["issues"]}
    calendar = report["calendar"]

    assert codes["stale_levels_repaired"]["dates"] == list(stale)
    assert calendar["total_return_repaired_days"] == 3
    assert calendar["dividend_by_year"][0]["total_return_repaired_days"] == 3
    # Rebuilt from the price move, the carried-over days no longer show total return trailing.
    assert calendar["negative_component_days"] == 0
    daily, _ = qlh.load_daily(snapshot, qlh.load_spec(spec_path))
    assert daily.loc[list(stale), "tr_repaired"].all()
    assert daily["tr_repaired"].sum() == 3


def test_check_passes_clean_data_and_measures_tracking(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()
    prices = _write_prices(tmp_path, spec_path, frame, {"TQQQ": -0.0001})
    snapshot = _fetch(tmp_path, spec_path, frame)

    report = qlh.check_snapshot(snapshot, qlh.load_spec(spec_path), prices_path=prices)

    assert report["status"] == "PASS", report["issues"]
    assert report["overlap_tracking"]["QQQ"]["annual_tracking_difference"] == pytest.approx(
        0, abs=1e-9
    )
    assert report["overlap_tracking"]["TQQQ"]["annual_tracking_difference"] == pytest.approx(
        -0.0001 * qlh.TRADING_DAYS_PER_YEAR, rel=1e-3
    )
    calendar = report["calendar"]
    assert calendar["total_return_first_date"] == "2020-01-02"
    assert calendar["implied_dividend_yield_annual"] > 0
    assert calendar["price_only_dates_dropped"] == 0
    assert calendar["total_return_only_dates_ignored"] == 0
    assert calendar["total_return_repaired_days"] == calendar["price_repaired_days"] == 0
    assert calendar["negative_component_days"] == 0
    assert calendar["dividend_windows"]["windows_below_limit"] == 0
    assert calendar["dividend_windows"]["min_change"] > 0
    assert calendar["noise_attribution"] == {}
    assert [row["year"] for row in calendar["dividend_by_year"]] == [2020]
    reference = report["etf_reference"]
    assert reference["first_date"] == "2020-01-03"
    assert reference["annual_tracking_difference"] == pytest.approx(0, abs=1e-9)
    assert reference["by_year"][0]["year"] == 2020
    assert report["series"]["QQQ"]["role"] == "etf_reference"
    assert (snapshot / qlh.CHECK_JSON_FILE).exists()
    markdown = (snapshot / qlh.CHECK_MD_FILE).read_text(encoding="utf-8")
    assert "状态：**PASS**" in markdown
    assert "TQQQ" in markdown
    assert "| 2020 |" in markdown
    assert "与真实 QQQ 的对照" in markdown


def test_check_fails_when_coverage_starts_late(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    raw = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    for item in raw["series"]:
        if item["series_id"] == "VXNCLS":
            item["expected_first_date"] = "2020-01-15"
    spec_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    snapshot = _fetch(tmp_path, spec_path, _series_frame())

    report = qlh.check_snapshot(
        snapshot, qlh.load_spec(spec_path), prices_path=tmp_path / "missing.csv"
    )

    assert report["status"] == "FAIL"
    assert [issue["code"] for issue in report["issues"]] == ["first_date_late"]


def test_check_warns_on_unexplained_gaps_and_tracking_outside_tolerance(tmp_path: Path) -> None:
    gap = ("2020-03-09", "2020-03-10", "2020-03-11", "2020-03-12", "2020-03-13")
    frame = _series_frame(drop_dates=gap)
    spec_path = _write_spec(tmp_path)
    prices = _write_prices(tmp_path, spec_path, frame, {"QLD": -0.0002})
    snapshot = _fetch(tmp_path, spec_path, frame)

    report = qlh.check_snapshot(snapshot, qlh.load_spec(spec_path), prices_path=prices)
    codes = {issue["code"]: issue for issue in report["issues"]}

    assert report["status"] == "WARN"
    assert codes["long_calendar_gap"]["dates"] == ["2020-03-06..2020-03-16"]
    assert codes["tracking_outside_tolerance"]["subject"] == "QLD"


def test_known_long_gap_is_not_reported(tmp_path: Path) -> None:
    gap = ("2020-03-09", "2020-03-10", "2020-03-11", "2020-03-12", "2020-03-13")
    frame = _series_frame(drop_dates=gap)
    spec_path = _write_spec(tmp_path)
    raw = yaml.safe_load(spec_path.read_text(encoding="utf-8"))
    raw["checks"]["known_long_gaps"] = ["2020-03-06..2020-03-16"]
    spec_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    prices = _write_prices(tmp_path, spec_path, frame, {})
    snapshot = _fetch(tmp_path, spec_path, frame)

    report = qlh.check_snapshot(snapshot, qlh.load_spec(spec_path), prices_path=prices)

    assert report["status"] == "PASS", report["issues"]
    assert report["calendar"]["long_gaps"] == ["2020-03-06..2020-03-16"]


def test_check_flags_suspicious_moves_and_bad_levels(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()
    jump = (frame["series"] == "NASDAQ100") & (frame["date"] >= "2020-04-01")
    frame.loc[jump, "value"] = frame.loc[jump, "value"] * 1.4
    snapshot = _fetch(tmp_path, spec_path, frame)

    report = qlh.check_snapshot(
        snapshot, qlh.load_spec(spec_path), prices_path=tmp_path / "missing.csv"
    )
    codes = {issue["code"]: issue for issue in report["issues"]}
    dates = list(pd.bdate_range("2020-01-02", "2020-06-30").strftime("%Y-%m-%d"))
    jump_at = dates.index("2020-04-01")
    window = qlh.load_spec(spec_path).spec.checks.dividend_window_trading_days

    assert codes["suspicious_index_return"]["dates"] == ["2020-04-01"]
    # Only the price index jumped, so every window holding that day has total return trailing.
    assert codes["dividend_window_negative"]["dates"] == [
        f"{dates[jump_at - window]}..{dates[jump_at + window - 1]}"
    ]
    assert report["calendar"]["dividend_by_year"][0]["negative_component_days"] == 1
    assert codes["overlap_unavailable"]["severity"] == "WARN"

    zero = frame.copy()
    zero.loc[(zero["series"] == "NASDAQXNDX") & (zero["date"] == "2020-02-03"), "value"] = 0.0
    bad = _fetch(tmp_path / "bad", spec_path, zero, _reference_prices(spec_path, frame))
    report = qlh.check_snapshot(bad, qlh.load_spec(spec_path), prices_path=tmp_path / "x.csv")
    assert report["status"] == "FAIL"
    assert "nonpositive_index_level" in {issue["code"] for issue in report["issues"]}


def _scale(frame: pd.DataFrame, series_id: str, factors: dict[str, float]) -> pd.DataFrame:
    scaled = frame.copy()
    for day, factor in factors.items():
        rows = (scaled["series"] == series_id) & (scaled["date"] == day)
        scaled.loc[rows, "value"] = scaled.loc[rows, "value"] * factor
    return scaled


def test_check_reports_price_only_dates_and_warns_only_when_the_level_moved(
    tmp_path: Path,
) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()
    # 2020-02-17 is a holiday with the price index carried over; 2020-03-02 is a trading day
    # whose total-return level is missing.
    frame = frame[
        ~((frame["series"] == "NASDAQXNDX") & frame["date"].isin(["2020-02-17", "2020-03-02"]))
    ]
    carried = frame.loc[(frame["series"] == "NASDAQ100") & (frame["date"] == "2020-02-14"), "value"]
    frame.loc[(frame["series"] == "NASDAQ100") & (frame["date"] == "2020-02-17"), "value"] = float(
        carried.iloc[0]
    )
    snapshot = _fetch(tmp_path, spec_path, frame)

    report = qlh.check_snapshot(
        snapshot, qlh.load_spec(spec_path), prices_path=tmp_path / "missing.csv"
    )
    codes = {issue["code"]: issue for issue in report["issues"]}
    calendar = report["calendar"]

    assert calendar["price_only_dates"] == ["2020-02-17", "2020-03-02"]
    assert calendar["price_only_dates_dropped"] == 2
    assert calendar["price_only_dates_unchanged"] == 1
    assert codes["total_return_missing_on_trading_dates"]["dates"] == ["2020-03-02"]
    daily, _ = qlh.load_daily(snapshot, qlh.load_spec(spec_path))
    assert daily.loc["2020-01-03":, "qqq_proxy_return"].notna().all()


def test_dividend_windows_tolerate_single_day_noise_but_warn_on_drift(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    loaded = qlh.load_spec(spec_path)
    noisy = _scale(_series_frame(), "NASDAQXNDX", {"2020-03-03": 0.997, "2020-04-14": 1.003})
    noisy_snapshot = _fetch(tmp_path / "noisy", spec_path, noisy)

    report = qlh.check_snapshot(noisy_snapshot, loaded, prices_path=tmp_path / "missing.csv")
    codes = {issue["code"] for issue in report["issues"]}

    assert "dividend_window_negative" not in codes
    assert report["calendar"]["negative_component_days"] == 2
    assert report["calendar"]["dividend_component_lag1_autocorrelation"] < 0

    dates = pd.bdate_range("2020-03-02", "2020-04-30").strftime("%Y-%m-%d")
    drift = _scale(
        _series_frame(), "NASDAQXNDX", {day: 1 - 0.001 * step for step, day in enumerate(dates)}
    )
    drift_snapshot = _fetch(tmp_path / "drift", spec_path, drift)

    report = qlh.check_snapshot(drift_snapshot, loaded, prices_path=tmp_path / "missing.csv")
    issue = next(item for item in report["issues"] if item["code"] == "dividend_window_negative")

    assert report["status"] == "WARN"
    assert report["calendar"]["dividend_windows"]["min_change"] < -0.015
    assert issue["dates"][0].startswith("2020-")


def test_noise_share_tells_which_index_carries_level_noise() -> None:
    rng = np.random.default_rng(2567)
    dates = pd.bdate_range("2003-01-02", periods=500)
    common = pd.Series(rng.normal(0, 0.015, len(dates)), index=dates)
    level = (1 + common).cumprod()

    def returns(noise_std: float) -> pd.Series:
        noisy = level * (1 + pd.Series(rng.normal(0, noise_std, len(dates)), index=dates))
        return noisy / noisy.shift(1) - 1

    clean = level / level.shift(1) - 1
    cases = {
        "total return noisy": (returns(0.004), clean, 1.0),
        "price noisy": (clean, returns(0.004), 0.0),
        "both noisy": (returns(0.004), returns(0.004), 0.5),
    }
    for name, (total_return, price, expected) in cases.items():
        result = qlh.total_return_noise_share(total_return, (total_return - price).dropna())
        assert result is not None, name
        share, standard_error = result
        assert standard_error < 0.15, name
        assert abs(share - expected) < 4 * standard_error, (name, share, standard_error)

    assert qlh.total_return_noise_share(clean, pd.Series(0.0001, index=dates)) is None
    assert qlh.total_return_noise_share(clean.head(2), clean.head(2)) is None


def test_previous_day_coefficient_detects_an_index_that_lags_the_other() -> None:
    rng = np.random.default_rng(2568)
    dates = pd.bdate_range("2003-01-02", periods=500)
    common = pd.Series(rng.normal(0, 0.015, len(dates)), index=dates)
    noise = pd.Series(rng.normal(0, 0.004, len(dates)), index=dates)
    lagging = 0.7 * common + 0.3 * common.shift(1)
    noisy = common + noise - noise.shift(1)
    cases = {
        "total return lags": (lagging, common),
        "total return level noise": (noisy, common),
        "price level noise": (common, noisy),
        "price lags": (common, lagging),
    }
    coefficients = {}
    for name, (total_return, price) in cases.items():
        result = qlh.previous_day_coefficient(total_return.dropna(), price)
        assert result is not None, name
        coefficients[name], standard_error = result
        assert standard_error < 0.05, name

    assert coefficients["total return lags"] == pytest.approx(0.3, abs=0.05)
    assert coefficients["total return level noise"] == pytest.approx(0, abs=0.05)
    # Noise in the regressor biases the coefficient up a little, well short of a real lag.
    assert 0 <= coefficients["price level noise"] < 0.15
    assert coefficients["price lags"] < -0.2
    assert qlh.previous_day_coefficient(common.head(3), common) is None


@pytest.mark.parametrize(
    ("series_id", "low", "high"), [("NASDAQ100", -0.2, 0.2), ("NASDAQXNDX", 0.8, 1.2)]
)
def test_check_attributes_window_noise_to_the_noisy_index(
    tmp_path: Path, series_id: str, low: float, high: float
) -> None:
    spec_path = _write_spec(tmp_path)
    dates = pd.bdate_range("2020-03-02", "2020-04-30").strftime("%Y-%m-%d")
    # Every third day one index is 0.8% off and back the next day: total return trails price
    # return by more than the window limit in every window that ends on such a day.
    sign = 1.0 if series_id == "NASDAQ100" else -1.0
    noisy = _scale(_series_frame(), series_id, {day: 1 + sign * 0.008 for day in dates[::3]})
    snapshot = _fetch(tmp_path, spec_path, noisy)

    report = qlh.check_snapshot(
        snapshot, qlh.load_spec(spec_path), prices_path=tmp_path / "missing.csv"
    )
    codes = {issue["code"] for issue in report["issues"]}
    attribution = report["calendar"]["noise_attribution"]
    year = report["calendar"]["dividend_by_year"][0]

    assert "dividend_window_negative" in codes
    assert attribution["trading_days"] > 0
    assert low < attribution["total_return_noise_share"] < high
    assert low < year["total_return_noise_share"] < high
    assert year["dividend_component_daily_std"] > 0.002
    assert attribution["total_return_lag_coefficient_se"] is not None
    markdown = (snapshot / qlh.CHECK_MD_FILE).read_text(encoding="utf-8")
    assert "噪声在总收益指数的比例" in markdown
    assert "噪声在总收益的比例" in markdown
    assert "总收益滞后系数" in markdown


@pytest.mark.parametrize(
    ("series_id", "noisier"), [("NASDAQ100", "price"), ("NASDAQXNDX", "total_return")]
)
def test_check_shows_which_index_the_actual_etf_disagrees_with(
    tmp_path: Path, series_id: str, noisier: str
) -> None:
    spec_path = _write_spec(tmp_path)
    clean = _series_frame()
    prices = _reference_prices(spec_path, clean)
    dates = pd.bdate_range("2020-03-02", "2020-04-30").strftime("%Y-%m-%d")
    noisy = _scale(clean, series_id, {day: 1.008 for day in dates[::3]})
    snapshot = _fetch(tmp_path, spec_path, noisy, prices)

    report = qlh.check_snapshot(
        snapshot, qlh.load_spec(spec_path), prices_path=tmp_path / "missing.csv"
    )
    reference = report["etf_reference"]
    gaps = {
        "price": reference["price_gap_daily_std"],
        "total_return": reference["total_return_gap_daily_std"],
    }
    quieter = "total_return" if noisier == "price" else "price"

    assert gaps[noisier] > 0.003
    assert gaps[quieter] < 0.001
    assert reference["by_year"][0][f"{noisier}_gap_daily_std"] == gaps[noisier]


def test_check_warns_when_the_proxy_drifts_from_the_actual_etf(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()
    snapshot = _fetch(tmp_path, spec_path, frame, _reference_prices(spec_path, frame, -0.0001))

    report = qlh.check_snapshot(
        snapshot, qlh.load_spec(spec_path), prices_path=tmp_path / "missing.csv"
    )
    codes = {issue["code"]: issue for issue in report["issues"]}

    assert report["etf_reference"]["annual_tracking_difference"] == pytest.approx(
        -0.0001 * qlh.TRADING_DAYS_PER_YEAR, rel=1e-3
    )
    assert codes["etf_reference_tracking_outside_tolerance"]["dates"] == ["2020"]
    daily, _ = qlh.load_daily(snapshot, qlh.load_spec(spec_path))
    assert daily["etf_reference_return"].notna().sum() == len(daily.loc["2020-01-03":])


def test_load_daily_refuses_unchecked_failed_or_stale_checks(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()
    prices = _write_prices(tmp_path, spec_path, frame, {})
    snapshot = _fetch(tmp_path, spec_path, frame)
    loaded = qlh.load_spec(spec_path)

    with pytest.raises(qlh.SnapshotNotCheckedError, match="no check report"):
        qlh.load_daily(snapshot, loaded)

    qlh.check_snapshot(snapshot, loaded, prices_path=prices)
    daily, report = qlh.load_daily(snapshot, loaded)
    assert report["status"] == "PASS"
    assert {"qqq_proxy_return", "lev2_return", "lev3_return", "cash_return"} <= set(daily.columns)

    spec_path.write_text(spec_path.read_text(encoding="utf-8") + "# edited\n", encoding="utf-8")
    with pytest.raises(qlh.SnapshotNotCheckedError, match="spec changed"):
        qlh.load_daily(snapshot, qlh.load_spec(spec_path))

    check_path = snapshot / qlh.CHECK_JSON_FILE
    failed = json.loads(check_path.read_text(encoding="utf-8")) | {"status": "FAIL"}
    check_path.write_text(json.dumps(failed), encoding="utf-8")
    with pytest.raises(qlh.SnapshotNotCheckedError, match="FAIL"):
        qlh.load_daily(snapshot, loaded)


def test_cli_fetch_and_check_print_summaries_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()
    prices = _write_prices(tmp_path, spec_path, frame, {})
    monkeypatch.setattr(qlh, "provider_factory", lambda: FakeProvider(frame))
    reference = _reference_prices(spec_path, frame)
    monkeypatch.setattr(qlh, "price_provider_factory", lambda: FakePriceProvider(reference))
    root = tmp_path / "snapshots"
    common = ["--root", str(root), "--spec-path", str(spec_path), "--prices-path", str(prices)]

    fetched = CliRunner().invoke(qlh.app, ["fetch", "--end", "2020-06-30", *common])
    checked = CliRunner().invoke(qlh.app, ["check", *common])

    assert fetched.exit_code == 0, fetched.output
    assert checked.exit_code == 0, checked.output
    assert "check: PASS" in checked.output
    for output in (fetched.output, checked.output):
        assert not re.search(r"^\d{4}-\d{2}-\d{2},", output, flags=re.MULTILINE)
        assert "8000" not in output


def test_cli_fetch_stops_before_downloading_without_the_fmp_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec_path = _write_spec(tmp_path)
    fred = FakeProvider(_series_frame())
    monkeypatch.setattr(qlh, "provider_factory", lambda: fred)
    monkeypatch.delenv(qlh.FMP_API_KEY_ENV, raising=False)
    root = tmp_path / "snapshots"
    arguments = ["fetch", "--end", "2020-06-30", "--root", str(root), "--spec-path", str(spec_path)]

    fetched = CliRunner().invoke(qlh.app, arguments)

    assert fetched.exit_code == 1
    assert "FMP_API_KEY is not set" in fetched.output
    assert fred.requests == []
    assert not root.exists()


def test_cli_fetch_prints_a_failed_download_without_the_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec_path = _write_spec(tmp_path)
    monkeypatch.setattr(qlh, "provider_factory", lambda: FakeProvider(_series_frame()))
    monkeypatch.setenv(qlh.FMP_API_KEY_ENV, "secret-key-123")

    class FailingProvider:
        def download_prices(self, request: PriceRequest) -> pd.DataFrame:
            raise ConnectionError(
                "Max retries exceeded with url: /stable/x?symbol=QQQ&apikey=secret-key-123"
            )

    monkeypatch.setattr(qlh, "price_provider_factory", FailingProvider)
    root = tmp_path / "snapshots"
    arguments = ["fetch", "--end", "2020-06-30", "--root", str(root), "--spec-path", str(spec_path)]

    fetched = CliRunner().invoke(qlh.app, arguments)

    assert fetched.exit_code == 1
    assert "fetch: ConnectionError: Max retries exceeded" in fetched.output
    assert "secret-key-123" not in fetched.output
    assert "Traceback" not in fetched.output
    assert not root.exists()
