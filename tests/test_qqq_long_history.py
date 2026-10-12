from __future__ import annotations

import json
import math
import re
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.config import PROJECT_ROOT, load_data_sources
from ai_trading_system.data.market_data import RateRequest
from ai_trading_system.research_data import qqq_long_history as qlh

REQUEST_END = date(2020, 6, 30)


class FakeProvider:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.requests: list[RateRequest] = []

    def download_rates(self, request: RateRequest) -> pd.DataFrame:
        self.requests.append(request)
        return self.frame[self.frame["series"].isin(request.series_ids)].copy()


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
        levels = 100 * (1 + returns).cumprod()
        rows.append(
            pd.DataFrame(
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
        )
    path = tmp_path / "prices_daily.csv"
    pd.concat(rows, ignore_index=True).to_csv(path, index=False)
    return path


def _fetch(tmp_path: Path, spec_path: Path, frame: pd.DataFrame) -> Path:
    return qlh.fetch_snapshot(
        qlh.load_spec(spec_path),
        end=REQUEST_END,
        provider=FakeProvider(frame),
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


def test_data_source_entry_matches_the_spec_and_stays_out_of_reconciliation() -> None:
    sources = {source.source_id: source for source in load_data_sources().sources}
    source = sources[qlh.load_spec().spec.source.source_id]

    assert source.domains == ["research_history"]
    assert source.cadence == "manual"
    assert source.llm_permission.external_llm_allowed is False
    assert source.llm_permission.redistribution_allowed is False
    assert source.cache_paths == [qlh.DEFAULT_SNAPSHOT_ROOT.relative_to(PROJECT_ROOT).as_posix()]


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
    assert (tmp_path / "snapshots" / qlh.LATEST_POINTER).read_text().strip() == first.name
    assert b"\r\n" not in (first / qlh.SERIES_FILE).read_bytes()


def test_fetch_refuses_when_a_series_is_missing(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path)
    frame = _series_frame()

    with pytest.raises(ValueError, match="VXNCLS"):
        _fetch(tmp_path, spec_path, frame[frame["series"] != "VXNCLS"])


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

    assert monday["calendar_days"] == 3
    assert monday["ndx_tr_return"] == pytest.approx(tr_return)
    # Friday has no T-bill print; Monday accrues Friday's carried-forward 1.8%, not Monday's 2.0%.
    assert monday["cash_return"] == pytest.approx(0.018 * 3 / 360)
    assert monday["qqq_proxy_return"] == pytest.approx(tr_return - 0.002 * 3 / 365)
    assert monday["lev3_return"] == pytest.approx(
        3 * tr_return - 2 * (0.018 + 0.005) * 3 / 360 - 0.0095 * 3 / 365
    )
    assert monday["lev2_return"] == pytest.approx(
        2 * tr_return - 1 * (0.018 + 0.005) * 3 / 360 - 0.0095 * 3 / 365
    )
    assert monday["vix"] == 14.0
    assert math.isnan(daily.loc["2020-01-03", "vix"])
    assert math.isnan(daily.loc["2020-01-02", "lev3_return"])


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
    assert report["calendar"]["total_return_first_date"] == "2020-01-02"
    assert report["calendar"]["implied_dividend_yield_annual"] > 0
    assert (snapshot / qlh.CHECK_JSON_FILE).exists()
    markdown = (snapshot / qlh.CHECK_MD_FILE).read_text(encoding="utf-8")
    assert "状态：**PASS**" in markdown
    assert "TQQQ" in markdown


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

    assert codes["suspicious_index_return"]["dates"] == ["2020-04-01"]
    # Only the price index jumped, so total return trails price return on that day.
    assert codes["negative_dividend_component"]["dates"] == ["2020-04-01"]
    assert codes["overlap_unavailable"]["severity"] == "WARN"

    zero = frame.copy()
    zero.loc[(zero["series"] == "NASDAQXNDX") & (zero["date"] == "2020-02-03"), "value"] = 0.0
    bad = _fetch(tmp_path / "bad", spec_path, zero)
    report = qlh.check_snapshot(bad, qlh.load_spec(spec_path), prices_path=tmp_path / "x.csv")
    assert report["status"] == "FAIL"
    assert "nonpositive_index_level" in {issue["code"] for issue in report["issues"]}


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
