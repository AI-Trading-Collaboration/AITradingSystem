from __future__ import annotations

import dataclasses
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import ai_trading_system.cli_commands.pit_snapshots as pit_snapshots_cli
from ai_trading_system.cli import app
from ai_trading_system.fmp_forward_pit import (
    FmpForwardPitFetchReport,
    FmpForwardPitIssueSeverity,
    attach_fmp_forward_pit_raw_paths,
    attach_fmp_forward_pit_report_artifacts,
    fetch_fmp_forward_pit_snapshots,
    project_fmp_forward_pit_capture,
    render_fmp_forward_pit_fetch_report,
    write_fmp_forward_pit_normalized_csv_from_payloads,
    write_fmp_forward_pit_raw_payloads,
)

# DATA-031: the earnings calendar is fetched in one-week segments so that FMP's 4000-row cap
# cannot silently cut the near dates (the late-October 2026 peak was lost that way). A failed
# or capped segment is a WARNING so the rest of the day's FMP PIT is still written; only a
# calendar with no segment at all is an ERROR, as a failed whole-window request is today.
AS_OF = date(2026, 11, 2)
WINDOW_FROM = date(2026, 10, 26)
WINDOW_TO = date(2027, 1, 31)
CAP = 4000
MAX_REQUESTS = 60
DOWNLOADED_AT = datetime(2026, 11, 2, 3, 0, tzinfo=UTC)
FAILED = "fmp_forward_pit_earnings_calendar_failed"
TRUNCATED = "fmp_forward_pit_earnings_calendar_truncated"
REQUEST_CAP = "fmp_forward_pit_earnings_calendar_request_cap"
UNAVAILABLE = "fmp_forward_pit_earnings_calendar_unavailable"


class _CalendarProvider:
    """Serves a synthetic calendar by date and cuts it like FMP: past the cap only the
    latest-dated rows come back."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        fail_on: date | None = None,
        fail_all: bool = False,
        spill_days: int = 0,
        dense_per_day: int = 0,
    ) -> None:
        self.rows = rows
        self.fail_on = fail_on
        self.fail_all = fail_all
        self.spill_days = spill_days
        self.dense_per_day = dense_per_day
        self.calls: list[tuple[date, date]] = []

    def fetch_analyst_estimates(
        self, ticker: str, *, period: str, limit: int
    ) -> list[dict[str, Any]]:
        return [{"symbol": ticker, "date": "2027-01-31", "epsAvg": 1.0}]

    def fetch_price_target_summary(self, ticker: str) -> list[dict[str, Any]]:
        return []

    def fetch_price_target_consensus(self, ticker: str) -> list[dict[str, Any]]:
        return []

    def fetch_grades(self, ticker: str) -> list[dict[str, Any]]:
        return []

    def fetch_grades_consensus(self, ticker: str) -> list[dict[str, Any]]:
        return []

    def fetch_ratings_snapshot(self, ticker: str) -> list[dict[str, Any]]:
        return []

    def fetch_earnings_calendar(self, *, from_date: date, to_date: date) -> list[dict[str, Any]]:
        self.calls.append((from_date, to_date))
        if self.fail_all or (self.fail_on is not None and from_date <= self.fail_on <= to_date):
            raise RuntimeError(
                "500 Server Error for url: https://financialmodelingprep.com/stable/"
                "earnings-calendar?from=x&to=y&apikey=secret-key"
            )
        last = to_date + timedelta(days=self.spill_days)
        selected = [
            row for row in self.rows if from_date <= date.fromisoformat(row["date"]) <= last
        ]
        day = last
        dense: list[dict[str, Any]] = []
        while self.dense_per_day and day >= from_date and len(dense) < CAP:
            dense.extend(
                {"symbol": f"Y{day:%Y%m%d}{index:05d}", "date": day.isoformat()}
                for index in range(self.dense_per_day)
            )
            day -= timedelta(days=1)
        selected += dense
        if len(selected) > CAP:
            selected = sorted(selected, key=lambda row: row["date"], reverse=True)[:CAP]
        return [dict(row) for row in selected]


def _filler(start: date, days: int, per_day: int) -> list[dict[str, Any]]:
    rows = []
    for offset in range(days):
        day = (start + timedelta(days=offset)).isoformat()
        for index in range(per_day):
            rows.append({"symbol": f"Z{offset:03d}{index:04d}", "date": day, "epsEstimated": 0.1})
    return rows


def _tracked() -> list[dict[str, Any]]:
    return [
        {"symbol": "MSFT", "date": "2026-10-28", "epsEstimated": 3.6, "time": "amc"},
        {"symbol": "MSFT", "date": "2027-01-27", "epsEstimated": 3.9, "time": "amc"},
        {"symbol": "META", "date": "2026-10-29", "epsEstimated": 6.7, "time": "amc"},
        {"symbol": "META", "date": "2027-01-28", "epsEstimated": 7.1, "time": "amc"},
    ]


def _peak_week() -> list[dict[str, Any]]:
    # the 14th segment runs 2027-01-25..2027-01-31; 7 x 640 rows put it past the cap
    return _filler(date(2027, 1, 25), 7, 640)


def _fetch(
    provider: _CalendarProvider, tickers: tuple[str, ...] = ("MSFT", "META")
) -> FmpForwardPitFetchReport:
    return fetch_fmp_forward_pit_snapshots(
        list(tickers),
        "test-key",
        AS_OF,
        provider=provider,
        captured_at=AS_OF,
        downloaded_at=DOWNLOADED_AT,
        analyst_estimate_limit=2,
    )


def _calendar(report: FmpForwardPitFetchReport, ticker: str) -> list[dict[str, Any]]:
    payload = next(payload for payload in report.raw_payloads if payload.ticker == ticker)
    return list(payload.endpoint_records["earnings-calendar"])


def _parameters(report: FmpForwardPitFetchReport, ticker: str = "MSFT") -> dict[str, Any]:
    payload = next(payload for payload in report.raw_payloads if payload.ticker == ticker)
    return dict(payload.request_parameters_by_endpoint["earnings-calendar"])


def _span(segment: dict[str, Any]) -> tuple[date, date]:
    return date.fromisoformat(segment["from"]), date.fromisoformat(segment["to"])


def _issues(report: FmpForwardPitFetchReport, code: str) -> list[Any]:
    return [issue for issue in report.issues if issue.code == code]


def _calendar_issue_keys(report: FmpForwardPitFetchReport) -> list[tuple[str, str]]:
    return [
        ("错误" if issue.severity == FmpForwardPitIssueSeverity.ERROR else "警告", issue.code)
        for issue in report.issues
        if issue.endpoint == "earnings-calendar"
    ]


def _assert_tiles_window(segments: list[dict[str, Any]]) -> None:
    assert _span(segments[0])[0] == WINDOW_FROM
    assert _span(segments[-1])[1] == WINDOW_TO
    for before, after in zip(segments, segments[1:], strict=False):
        assert _span(after)[0] == _span(before)[1] + timedelta(days=1)
    for segment in segments:
        assert _span(segment)[0] <= _span(segment)[1]


def test_the_default_window_is_fetched_in_fourteen_contiguous_week_segments() -> None:
    provider = _CalendarProvider(_tracked())

    report = _fetch(provider)

    assert report.earnings_calendar_from == WINDOW_FROM
    assert report.earnings_calendar_to == WINDOW_TO
    assert len(provider.calls) == 14
    assert all((to - start).days == 6 for start, to in provider.calls)
    segments = _parameters(report)["segments"]
    assert [_span(segment) for segment in segments] == provider.calls
    _assert_tiles_window(segments)
    assert report.status == "PASS"


def test_a_window_past_the_cap_comes_back_whole_when_no_week_reaches_it() -> None:
    rows = _filler(WINDOW_FROM, 98, 100) + _tracked()
    whole = [row for row in rows if WINDOW_FROM <= date.fromisoformat(row["date"]) <= WINDOW_TO]
    assert len(whole) > 2 * CAP  # one request for the whole window would have been cut
    provider = _CalendarProvider(rows)

    report = _fetch(provider)

    assert report.status == "PASS"
    assert _calendar(report, "MSFT") == [row for row in _tracked() if row["symbol"] == "MSFT"]
    assert _calendar(report, "META") == [row for row in _tracked() if row["symbol"] == "META"]
    segments = _parameters(report)["segments"]
    assert sum(segment["rows"] for segment in segments) == len(whole)
    assert max(segment["rows"] for segment in segments) < CAP
    assert report.earnings_calendar_segment_count == 14
    assert report.earnings_calendar_resplit_count == 0
    assert report.earnings_calendar_max_segment_rows == max(segment["rows"] for segment in segments)


def test_a_week_at_the_cap_is_split_until_every_kept_segment_is_under_it() -> None:
    peak_start = date(2027, 1, 25)
    rows = _filler(WINDOW_FROM, 98, 10) + _peak_week() + _tracked()
    provider = _CalendarProvider(rows)

    report = _fetch(provider)

    assert report.status == "PASS"
    assert {row["date"] for row in _calendar(report, "MSFT")} == {"2026-10-28", "2027-01-27"}
    assert {row["date"] for row in _calendar(report, "META")} == {"2026-10-29", "2027-01-28"}
    segments = _parameters(report)["segments"]
    _assert_tiles_window(segments)
    assert all(segment["rows"] < CAP for segment in segments)
    assert sum(segment["rows"] for segment in segments) == len(rows)
    assert report.earnings_calendar_resplit_count == len(segments) - 14 >= 1
    assert (peak_start, WINDOW_TO) in provider.calls
    assert (peak_start, WINDOW_TO) not in [_span(segment) for segment in segments]


def test_a_single_day_at_the_cap_is_a_warning_and_its_rows_are_kept() -> None:
    peak_day = date(2027, 1, 26)
    peak_rows = [
        {"symbol": "AMD", "date": peak_day.isoformat(), "epsEstimated": 1.2, "time": "amc"}
    ] + _filler(peak_day, 1, CAP + 100)
    rows = _filler(WINDOW_FROM, 98, 10) + peak_rows + _tracked()
    provider = _CalendarProvider(rows)

    report = _fetch(provider, ("MSFT", "META", "AMD"))

    assert report.status == "PASS_WITH_WARNINGS"
    # exactly this one issue: a fake provider that fails its own assertion would add another
    assert [issue.code for issue in report.issues] == [TRUNCATED]
    truncated = _issues(report, TRUNCATED)
    assert truncated[0].severity == FmpForwardPitIssueSeverity.WARNING
    assert peak_day.isoformat() in truncated[0].message
    assert {row["date"] for row in _calendar(report, "MSFT")} == {"2026-10-28", "2027-01-27"}
    assert {row["date"] for row in _calendar(report, "META")} == {"2026-10-29", "2027-01-28"}
    segments = _parameters(report)["segments"]
    _assert_tiles_window(segments)
    day_segments = [s for s in segments if s["from"] == s["to"] == peak_day.isoformat()]
    assert len(day_segments) == 1 and day_segments[0]["rows"] == CAP
    assert day_segments[0].get("truncated") is True
    others = [s for s in segments if s is not day_segments[0]]
    assert all(s["rows"] < CAP and "truncated" not in s for s in others)
    # the capped day's rows are kept, not thrown away
    assert _calendar(report, "AMD") == [peak_rows[0]]


def test_a_failed_segment_is_a_warning_and_the_other_segments_are_kept() -> None:
    fail_on = date(2026, 12, 1)
    provider = _CalendarProvider(_filler(WINDOW_FROM, 98, 5) + _tracked(), fail_on=fail_on)

    report = _fetch(provider)

    assert report.status == "PASS_WITH_WARNINGS"
    assert [issue.code for issue in report.issues] == [FAILED]
    failed = _issues(report, FAILED)
    assert failed[0].severity == FmpForwardPitIssueSeverity.WARNING
    start, end = next((s, e) for s, e in provider.calls if s <= fail_on <= e)
    assert start.isoformat() in failed[0].message and end.isoformat() in failed[0].message
    assert "secret-key" not in failed[0].message
    assert {row["date"] for row in _calendar(report, "MSFT")} == {"2026-10-28", "2027-01-27"}
    segments = _parameters(report)["segments"]
    _assert_tiles_window(segments)
    errors = [s for s in segments if s.get("error") is True]
    assert len(errors) == 1
    assert errors[0]["rows"] is None
    assert _span(errors[0]) == (start, end)
    assert len(segments) == 14
    # the failed segment still counts as a segment in the report
    assert report.earnings_calendar_segment_count == 14
    assert report.earnings_calendar_resplit_count == 0


def test_a_calendar_with_no_segment_at_all_is_an_error() -> None:
    provider = _CalendarProvider(_tracked(), fail_all=True)

    report = _fetch(provider)

    assert report.status == "FAIL"
    unavailable = _issues(report, UNAVAILABLE)
    assert len(unavailable) == 1
    assert unavailable[0].severity == FmpForwardPitIssueSeverity.ERROR
    assert len(_issues(report, FAILED)) == 14
    assert all("secret-key" not in issue.message for issue in report.issues)


def test_capped_segments_stop_splitting_at_the_request_budget_and_keep_their_rows() -> None:
    provider = _CalendarProvider(_tracked(), dense_per_day=CAP + 1)

    report = _fetch(provider)

    # each split costs one request and leaves one more segment queued, so the budget allows
    # exactly (60 - 14) // 2 = 23 splits: 60 requests, 37 kept segments
    assert len(provider.calls) == MAX_REQUESTS
    segments = _parameters(report)["segments"]
    assert len(segments) == 37
    assert report.earnings_calendar_segment_count == 37
    assert report.earnings_calendar_resplit_count == 23
    _assert_tiles_window(segments)
    # every segment was requested at least once: nothing is skipped
    assert all(s.get("skipped") is not True and s["rows"] == CAP for s in segments)
    assert all(s.get("truncated") is True for s in segments)
    # a capped segment that the budget cannot split any more is kept with its rows; every
    # request is a kept leaf or a parent whose two halves were both requested
    kept_spans = {_span(s) for s in segments}
    calls = set(provider.calls)
    for start, end in provider.calls:
        if (start, end) in kept_spans:
            continue
        middle = start + timedelta(days=(end - start).days // 2)
        assert (start, middle) in calls and (middle + timedelta(days=1), end) in calls
    assert any(_span(s)[0] < _span(s)[1] for s in segments)
    assert sorted(issue.code for issue in report.issues) == sorted(
        [TRUNCATED] * len(segments) + [REQUEST_CAP]
    )
    assert _issues(report, REQUEST_CAP)[0].severity == FmpForwardPitIssueSeverity.WARNING
    assert report.status == "PASS_WITH_WARNINGS"


def test_a_window_longer_than_the_budget_skips_the_last_segments() -> None:
    provider = _CalendarProvider(_tracked())

    report = fetch_fmp_forward_pit_snapshots(
        ["MSFT"],
        "test-key",
        AS_OF,
        provider=provider,
        captured_at=AS_OF,
        downloaded_at=DOWNLOADED_AT,
        analyst_estimate_limit=2,
        earnings_calendar_lookback_days=0,
        earnings_calendar_forward_days=440,  # 441 days, 63 week segments
    )

    assert len(provider.calls) == MAX_REQUESTS
    segments = _parameters(report)["segments"]
    assert len(segments) == 63
    assert [s.get("skipped") is True for s in segments] == [False] * 60 + [True] * 3
    assert all(s["rows"] is None for s in segments[60:])
    assert [issue.code for issue in report.issues] == [REQUEST_CAP]
    assert segments[60]["from"] in report.issues[0].message
    assert report.status == "PASS_WITH_WARNINGS"


def test_a_code_error_while_merging_is_not_turned_into_a_warning() -> None:
    bad_day = date(2026, 12, 2)
    # a key JSON cannot encode: only the merge, not the provider call, can fail on it
    bad_row: dict[Any, Any] = {("not", "a", "string"): 1, "date": bad_day.isoformat()}
    provider = _CalendarProvider([*_tracked(), bad_row])

    try:
        report = _fetch(provider)
    except Exception:
        return
    assert report.status == "FAIL"


def test_rows_are_deduplicated_across_segments_only() -> None:
    # the second segment starts on this day; the spill makes the first segment return it too
    boundary = date(2026, 11, 2)
    near = [
        {"symbol": "MSFT", "date": boundary.isoformat(), "epsEstimated": 3.6},
        {"symbol": "MSFT", "date": boundary.isoformat(), "epsEstimated": 3.7},
    ]
    same_segment_twice = [
        {"symbol": "MSFT", "date": "2026-12-15", "epsEstimated": 3.8},
        {"symbol": "MSFT", "date": "2026-12-15", "epsEstimated": 3.8},
    ]
    provider = _CalendarProvider(near + same_segment_twice, spill_days=1)

    report = _fetch(provider, ("MSFT",))

    assert _calendar(report, "MSFT") == near + same_segment_twice
    assert report.status == "PASS"


def test_the_raw_payload_keeps_the_whole_window_and_records_the_segments(tmp_path: Path) -> None:
    provider = _CalendarProvider(_filler(WINDOW_FROM, 98, 10) + _peak_week() + _tracked())
    report = _fetch(provider)

    paths = write_fmp_forward_pit_raw_payloads(report.raw_payloads, tmp_path / "raw")

    raws = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    parameters = [raw["request_parameters_by_endpoint"]["earnings-calendar"] for raw in raws]
    for item in parameters:
        assert item["from"] == WINDOW_FROM.isoformat()
        assert item["to"] == WINDOW_TO.isoformat()
        assert item["row_cap"] == CAP
        assert {"from", "to", "filtered_symbol", "segments", "row_cap"} == set(item)
    assert parameters[0]["segments"] == parameters[1]["segments"]
    assert {item["filtered_symbol"] for item in parameters} == {"MSFT", "META"}
    for segment in parameters[0]["segments"]:
        assert _span(segment) in provider.calls


def _write_capture(tmp_path: Path, report: FmpForwardPitFetchReport) -> tuple[Path, Path]:
    raw_dir = tmp_path / "data" / "raw" / "capture" / "fmp_forward_pit"
    raw_paths = write_fmp_forward_pit_raw_payloads(report.raw_payloads, raw_dir)
    attached = attach_fmp_forward_pit_raw_paths(
        report.raw_payloads, raw_paths, project_root=tmp_path
    )
    capture_normalized = tmp_path / "capture" / "fmp_forward_pit_2026-11-02.csv"
    write_fmp_forward_pit_normalized_csv_from_payloads(attached, capture_normalized)
    return raw_dir, capture_normalized


def _project(tmp_path: Path, raw_dir: Path, capture_normalized: Path) -> str:
    canonical_normalized = (
        tmp_path / "data" / "processed" / "pit_snapshots" / "fmp_forward_pit_2026-11-02.csv"
    )
    canonical_report = tmp_path / "outputs" / "reports" / "fmp_forward_pit_fetch_2026-11-02.md"
    projection = project_fmp_forward_pit_capture(
        raw_input_dir=raw_dir,
        capture_normalized_input_path=capture_normalized,
        as_of=AS_OF,
        normalized_output_path=canonical_normalized,
        report_output_path=canonical_report,
        project_root=tmp_path,
    )
    assert projection.provider_request_performed is False
    assert canonical_normalized.read_bytes() == capture_normalized.read_bytes()
    return canonical_report.read_text(encoding="utf-8")


def _replay(tmp_path: Path, report: FmpForwardPitFetchReport) -> str:
    return _project(tmp_path, *_write_capture(tmp_path, report))


SEGMENT_LINE = re.compile(
    r"- earnings-calendar 分段：(\d+) 段，单段最多 (\d+) 行（上限 4000），再切 (\d+) 次"
)
ISSUE_ROW = re.compile(r"^\| (错误|警告) \| (fmp_forward_pit_earnings_calendar_\w+) \|", re.M)


def test_a_new_capture_replays_with_the_same_segment_summary(tmp_path: Path) -> None:
    report = _fetch(_CalendarProvider(_filler(WINDOW_FROM, 98, 10) + _peak_week() + _tracked()))

    text = _replay(tmp_path, report)

    match = SEGMENT_LINE.search(text)
    assert match is not None
    assert tuple(int(value) for value in match.groups()) == (
        report.earnings_calendar_segment_count,
        report.earnings_calendar_max_segment_rows,
        report.earnings_calendar_resplit_count,
    )
    assert report.earnings_calendar_resplit_count >= 1
    assert "- 状态：PASS" in text


def test_the_replayed_report_carries_the_calendar_warnings(tmp_path: Path) -> None:
    fail_on = date(2026, 12, 1)
    peak_day = date(2027, 1, 26)
    rows = _filler(WINDOW_FROM, 98, 5) + _filler(peak_day, 1, CAP + 100) + _tracked()
    provider = _CalendarProvider(rows, fail_on=fail_on)
    report = _fetch(provider)
    assert report.status == "PASS_WITH_WARNINGS"
    expected = _calendar_issue_keys(report)
    assert sorted(code for _, code in expected) == [FAILED, TRUNCATED]

    text = _replay(tmp_path, report)

    assert "- 状态：PASS_WITH_WARNINGS" in text
    assert ISSUE_ROW.findall(text) == expected
    start, end = next((s, e) for s, e in provider.calls if s <= fail_on <= e)
    failed_row = next(line for line in text.splitlines() if FAILED in line)
    assert start.isoformat() in failed_row and end.isoformat() in failed_row
    truncated_row = next(line for line in text.splitlines() if TRUNCATED in line)
    assert peak_day.isoformat() in truncated_row


def _without_segment_keys(report: FmpForwardPitFetchReport, tickers: set[str]) -> Any:
    payloads = []
    for payload in report.raw_payloads:
        parameters = {
            endpoint: dict(values)
            for endpoint, values in payload.request_parameters_by_endpoint.items()
        }
        if payload.ticker in tickers:
            parameters["earnings-calendar"].pop("segments")
            parameters["earnings-calendar"].pop("row_cap")
        payloads.append(dataclasses.replace(payload, request_parameters_by_endpoint=parameters))
    return dataclasses.replace(report, raw_payloads=tuple(payloads))


def test_an_archive_without_segments_still_replays(tmp_path: Path) -> None:
    report = _fetch(_CalendarProvider(_tracked()))
    old_report = _without_segment_keys(report, {"MSFT", "META"})

    text = _replay(tmp_path, old_report)

    assert SEGMENT_LINE.search(text) is None
    assert "- earnings-calendar 分段：无记录" in text
    assert "- 状态：PASS" in text


@pytest.mark.parametrize("change", ["drop_one", "differ"])
def test_a_batch_with_mixed_segment_records_is_refused(tmp_path: Path, change: str) -> None:
    report = _fetch(_CalendarProvider(_tracked()))
    if change == "drop_one":
        mixed = _without_segment_keys(report, {"META"})
    else:
        payloads = []
        for payload in report.raw_payloads:
            parameters: dict[str, dict[str, Any]] = {
                endpoint: dict(values)
                for endpoint, values in payload.request_parameters_by_endpoint.items()
            }
            if payload.ticker == "META":
                segments = [
                    dict(segment) for segment in parameters["earnings-calendar"]["segments"]
                ]
                segments[0]["rows"] = 999
                parameters["earnings-calendar"]["segments"] = segments
            payloads.append(dataclasses.replace(payload, request_parameters_by_endpoint=parameters))
        mixed = dataclasses.replace(report, raw_payloads=tuple(payloads))
    raw_dir, capture_normalized = _write_capture(tmp_path, mixed)

    with pytest.raises(ValueError, match="batch metadata mismatch"):
        _project(tmp_path, raw_dir, capture_normalized)


def test_the_fetch_report_states_the_segments_and_attaching_artifacts_keeps_them() -> None:
    report = _fetch(_CalendarProvider(_filler(WINDOW_FROM, 98, 10) + _peak_week() + _tracked()))
    expected = (
        report.earnings_calendar_segment_count,
        report.earnings_calendar_max_segment_rows,
        report.earnings_calendar_resplit_count,
    )
    segments = _parameters(report)["segments"]
    assert expected == (
        len(segments),
        max(segment["rows"] for segment in segments),
        len(segments) - 14,
    )

    attached = attach_fmp_forward_pit_report_artifacts(
        report,
        raw_payloads=report.raw_payloads,
        normalized_rows=report.normalized_rows,
    )

    for item in (report, attached):
        match = SEGMENT_LINE.search(render_fmp_forward_pit_fetch_report(item))
        assert match is not None
        assert tuple(int(value) for value in match.groups()) == expected


@pytest.mark.parametrize("lookback, forward", [(0, 0), (0, 6), (3, 10)])
def test_short_windows_are_tiled_without_gaps(lookback: int, forward: int) -> None:
    provider = _CalendarProvider(_tracked())

    fetch_fmp_forward_pit_snapshots(
        ["MSFT"],
        "test-key",
        AS_OF,
        provider=provider,
        captured_at=AS_OF,
        downloaded_at=DOWNLOADED_AT,
        analyst_estimate_limit=2,
        earnings_calendar_lookback_days=lookback,
        earnings_calendar_forward_days=forward,
    )

    days = lookback + forward + 1
    assert len(provider.calls) == -(-days // 7)
    assert provider.calls[0][0] == AS_OF - timedelta(days=lookback)
    assert provider.calls[-1][1] == AS_OF + timedelta(days=forward)
    for (_, before_end), (after_start, _) in zip(provider.calls, provider.calls[1:], strict=False):
        assert after_start == before_end + timedelta(days=1)


def test_the_cli_writes_the_capture_when_one_segment_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_dir = tmp_path / "raw" / "fmp_forward_pit"
    fetch_report_path = tmp_path / "reports" / "fmp_forward_pit.md"
    fail_on = date(2026, 12, 1)

    def fake_fetch(
        tickers: list[str] | tuple[str, ...],
        api_key: str,
        as_of: date,
        **kwargs: Any,
    ) -> FmpForwardPitFetchReport:
        return fetch_fmp_forward_pit_snapshots(
            tickers,
            api_key,
            as_of,
            provider=_CalendarProvider(_tracked(), fail_on=fail_on),
            downloaded_at=DOWNLOADED_AT,
            **kwargs,
        )

    monkeypatch.setenv("FMP_API_KEY", "test-key")
    monkeypatch.setattr(pit_snapshots_cli, "fetch_fmp_forward_pit_snapshots", fake_fetch)

    result = CliRunner().invoke(
        app,
        [
            "pit-snapshots",
            "fetch-fmp-forward",
            "--tickers",
            "MSFT",
            "--raw-output-dir",
            str(raw_dir),
            "--normalized-output-path",
            str(tmp_path / "processed" / "fmp_forward_pit.csv"),
            "--manifest-path",
            str(tmp_path / "pit_snapshots" / "manifest.csv"),
            "--as-of",
            AS_OF.isoformat(),
            "--output-path",
            str(fetch_report_path),
            "--pit-validation-report-path",
            str(tmp_path / "reports" / "pit_validation.md"),
            "--analyst-estimate-limit",
            "2",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "FMP PIT 抓取状态：PASS_WITH_WARNINGS" in result.output
    raw_files = sorted(raw_dir.rglob("*.json"))
    assert len(raw_files) == 1
    raw = json.loads(raw_files[0].read_text(encoding="utf-8"))
    segments = raw["request_parameters_by_endpoint"]["earnings-calendar"]["segments"]
    assert [s for s in segments if s.get("error") is True]
    assert FAILED in fetch_report_path.read_text(encoding="utf-8")
