from __future__ import annotations

import gzip
import json
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import ai_trading_system.cli_commands.pit_snapshots as pit_snapshots_cli
import ai_trading_system.fmp_forward_pit as fmp_forward_pit
from ai_trading_system.cli import app
from ai_trading_system.fmp_forward_pit import (
    FmpForwardPitFetchReport,
    FmpForwardPitIssueSeverity,
    attach_fmp_earnings_calendar_full,
    attach_fmp_forward_pit_report_artifacts,
    fetch_fmp_forward_pit_snapshots,
    load_fmp_earnings_calendar_full,
    load_fmp_forward_pit_raw_payloads,
    project_fmp_forward_pit_capture,
    render_fmp_forward_pit_fetch_report,
    write_fmp_earnings_calendar_full,
)
from ai_trading_system.pit_snapshots import discover_existing_pit_raw_snapshots

# DATA-032: the merged full-window earnings calendar is kept next to the per-ticker payloads,
# so names outside the 17-name watchlist (AAPL) have dates for research. The file does not end
# in .json, so the payload loader and the PIT manifest never see it; the capture's source-owned
# raw directory commits to it like any other file.
AS_OF = date(2026, 11, 2)
WINDOW_FROM = date(2026, 10, 26)
WINDOW_TO = date(2027, 1, 31)
DOWNLOADED_AT = datetime(2026, 11, 2, 3, 0, tzinfo=UTC)
API_KEY = "secret-key-032"
WRITE_FAILED = "fmp_forward_pit_earnings_calendar_full_write_failed"
FULL_GLOB = "fmp_earnings_calendar_full_*.jsonl.gz"
FULL_LINE = "- earnings-calendar 全窗口："
HEADER_KEYS = {
    "schema",
    "as_of",
    "captured_at",
    "downloaded_at",
    "provider",
    "endpoint",
    "from",
    "to",
    "segments",
    "row_cap",
    "row_count",
    "rows_sha256",
}
ROWS = [
    {"symbol": "AAPL", "date": "2026-10-29", "epsEstimated": 1.6, "time": "amc"},
    {"symbol": "MSFT", "date": "2026-10-28", "epsEstimated": 3.6, "time": "amc"},
    {"symbol": "GOOGL", "date": "2026-10-28", "epsEstimated": 2.3, "time": "amc"},
    {"symbol": "AAPL", "date": "2027-01-28", "epsEstimated": 2.4, "time": "amc"},
    {"symbol": "MSFT", "date": "2027-01-27", "epsEstimated": 3.9, "time": "amc"},
]


class _Provider:
    def __init__(
        self, rows: list[dict[str, Any]], *, fail_on: date | None = None, fail_all: bool = False
    ) -> None:
        self.rows = rows
        self.fail_on = fail_on
        self.fail_all = fail_all

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
        if self.fail_all or (self.fail_on is not None and from_date <= self.fail_on <= to_date):
            raise RuntimeError(
                "500 Server Error for url: https://financialmodelingprep.com/stable/"
                f"earnings-calendar?from=x&to=y&apikey={API_KEY}"
            )
        return [
            dict(row)
            for row in self.rows
            if from_date <= date.fromisoformat(row["date"]) <= to_date
        ]


def _fetch(provider: _Provider, tickers: tuple[str, ...] = ("MSFT",)) -> FmpForwardPitFetchReport:
    return fetch_fmp_forward_pit_snapshots(
        list(tickers),
        API_KEY,
        AS_OF,
        provider=provider,
        captured_at=AS_OF,
        downloaded_at=DOWNLOADED_AT,
        analyst_estimate_limit=2,
    )


def _calendar_parameters(report: FmpForwardPitFetchReport) -> dict[str, Any]:
    return dict(report.raw_payloads[0].request_parameters_by_endpoint["earnings-calendar"])


def _rewrite(path: Path, transform: Any) -> None:
    lines = gzip.decompress(path.read_bytes()).split(b"\n")
    path.write_bytes(gzip.compress(transform(lines), mtime=0))


def test_the_full_file_keeps_names_outside_the_watchlist(tmp_path: Path) -> None:
    report = _fetch(_Provider(ROWS))

    path = write_fmp_earnings_calendar_full(report, tmp_path)
    header, rows = load_fmp_earnings_calendar_full(path)

    assert path.parent == tmp_path
    assert path.name == "fmp_earnings_calendar_full_2026-11-02T03-00-00Z.jsonl.gz"
    assert {row["symbol"] for row in rows} == {"AAPL", "MSFT", "GOOGL"}
    assert len(rows) == len(ROWS) == header["row_count"]
    assert list(rows) == list(report.earnings_calendar_records)
    msft = report.raw_payloads[0]
    assert {row["symbol"] for row in msft.endpoint_records["earnings-calendar"]} == {"MSFT"}


def test_the_header_records_the_window_and_the_segments(tmp_path: Path) -> None:
    report = _fetch(_Provider(ROWS))

    path = write_fmp_earnings_calendar_full(report, tmp_path)
    header, rows = load_fmp_earnings_calendar_full(path)

    assert set(header) == HEADER_KEYS
    assert header["schema"] == "fmp_earnings_calendar_full.v1"
    assert header["endpoint"] == "earnings-calendar"
    assert header["as_of"] == AS_OF.isoformat()
    assert header["downloaded_at"] == DOWNLOADED_AT.isoformat()
    assert (header["from"], header["to"]) == (WINDOW_FROM.isoformat(), WINDOW_TO.isoformat())
    parameters = _calendar_parameters(report)
    assert header["segments"] == parameters["segments"]
    assert header["row_cap"] == parameters["row_cap"] == 4000
    data = gzip.decompress(path.read_bytes())
    head, _, body = data.partition(b"\n")
    assert json.loads(head) == header
    assert header["rows_sha256"] == sha256(body).hexdigest()
    # one compact sorted-key JSON object per line, every line ends with a newline
    expected = b"".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        for row in rows
    )
    assert body == expected


def test_the_same_report_writes_the_same_bytes(tmp_path: Path) -> None:
    report = _fetch(_Provider(ROWS))

    first = write_fmp_earnings_calendar_full(report, tmp_path / "a")
    second = write_fmp_earnings_calendar_full(report, tmp_path / "b")

    assert first.read_bytes() == second.read_bytes()


@pytest.mark.parametrize(
    "transform",
    [
        pytest.param(
            lambda lines: b"\n".join([lines[0], lines[1].replace(b"1.6", b"1.7"), *lines[2:]]),
            id="changed-row",
        ),
        pytest.param(lambda lines: b"\n".join([lines[0], *lines[2:]]), id="dropped-row"),
        pytest.param(
            lambda lines: b"\n".join(
                [lines[0].replace(b'"as_of":"2026-11-02"', b'"as_of":"soon"'), *lines[1:]]
            ),
            id="as-of-not-a-date",
        ),
    ],
)
def test_a_damaged_file_is_refused(tmp_path: Path, transform: Any) -> None:
    path = write_fmp_earnings_calendar_full(_fetch(_Provider(ROWS)), tmp_path)
    _rewrite(path, transform)

    with pytest.raises(ValueError):
        load_fmp_earnings_calendar_full(path)


def test_a_row_that_is_not_an_object_is_refused(tmp_path: Path) -> None:
    path = write_fmp_earnings_calendar_full(_fetch(_Provider(ROWS)), tmp_path)
    data = gzip.decompress(path.read_bytes())
    head, _, _ = data.partition(b"\n")
    body = b"[1, 2]\n"
    header = json.loads(head)
    header.update({"row_count": 1, "rows_sha256": sha256(body).hexdigest()})
    line = json.dumps(header, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(gzip.compress(line + body, mtime=0))

    with pytest.raises(ValueError):
        load_fmp_earnings_calendar_full(path)


def test_a_header_row_count_that_does_not_match_is_refused(tmp_path: Path) -> None:
    path = write_fmp_earnings_calendar_full(_fetch(_Provider(ROWS)), tmp_path)
    data = gzip.decompress(path.read_bytes())
    head, _, body = data.partition(b"\n")
    header = json.loads(head)
    header["row_count"] = len(ROWS) + 1
    line = json.dumps(header, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(gzip.compress(line + body, mtime=0))

    with pytest.raises(ValueError):
        load_fmp_earnings_calendar_full(path)


def test_attaching_artifacts_keeps_the_full_calendar(tmp_path: Path) -> None:
    report = attach_fmp_earnings_calendar_full(_fetch(_Provider(ROWS)), tmp_path)

    attached = attach_fmp_forward_pit_report_artifacts(
        report, raw_payloads=report.raw_payloads, normalized_rows=report.normalized_rows
    )

    assert attached.earnings_calendar_records == report.earnings_calendar_records
    line = next(
        item
        for item in render_fmp_forward_pit_fetch_report(attached).splitlines()
        if item.startswith(FULL_LINE)
    )
    assert f"{len(ROWS)} 行" in line
    assert "fmp_earnings_calendar_full_2026-11-02T03-00-00Z.jsonl.gz" in line


def test_an_empty_calendar_is_written_with_zero_rows(tmp_path: Path) -> None:
    report = _fetch(_Provider([]))

    header, rows = load_fmp_earnings_calendar_full(
        write_fmp_earnings_calendar_full(report, tmp_path)
    )

    assert report.status == "PASS"
    assert header["row_count"] == 0
    assert header["rows_sha256"] == sha256(b"").hexdigest()
    assert rows == ()


def _invoke_cli(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    provider: _Provider,
) -> tuple[Any, Path, Path, Path]:
    raw_dir = tmp_path / "data" / "raw" / "capture" / "fmp_forward_pit"
    normalized = tmp_path / "capture" / "fmp_forward_pit_2026-11-02.csv"
    fetch_report_path = tmp_path / "reports" / "fmp_forward_pit_fetch.md"

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
            provider=provider,
            downloaded_at=DOWNLOADED_AT,
            **kwargs,
        )

    monkeypatch.setenv("FMP_API_KEY", API_KEY)
    monkeypatch.setattr(pit_snapshots_cli, "fetch_fmp_forward_pit_snapshots", fake_fetch)
    result = CliRunner().invoke(
        app,
        [
            "pit-snapshots",
            "fetch-fmp-forward",
            "--tickers",
            "MSFT,NVDA",
            "--raw-output-dir",
            str(raw_dir),
            "--normalized-output-path",
            str(normalized),
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
    return result, raw_dir, normalized, fetch_report_path


def _project(tmp_path: Path, raw_dir: Path, normalized: Path) -> str:
    # default project root, as the CLI used when it wrote the capture
    report_path = tmp_path / "outputs" / "reports" / "fmp_forward_pit_fetch_2026-11-02.md"
    canonical = tmp_path / "data" / "processed" / "pit_snapshots" / "fmp_forward_pit.csv"
    project_fmp_forward_pit_capture(
        raw_input_dir=raw_dir,
        capture_normalized_input_path=normalized,
        as_of=AS_OF,
        normalized_output_path=canonical,
        report_output_path=report_path,
    )
    assert canonical.read_bytes() == normalized.read_bytes()
    return report_path.read_text(encoding="utf-8")


def test_the_cli_writes_one_full_file_that_the_payload_readers_do_not_see(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, raw_dir, normalized, fetch_report_path = _invoke_cli(
        tmp_path, monkeypatch, _Provider(ROWS)
    )

    assert result.exit_code == 0, result.output
    full_files = sorted(raw_dir.glob(FULL_GLOB))
    assert len(full_files) == 1
    assert sorted(raw_dir.rglob(FULL_GLOB)) == full_files
    header, rows = load_fmp_earnings_calendar_full(full_files[0])
    assert header["row_count"] == len(ROWS)
    text = fetch_report_path.read_text(encoding="utf-8")
    line = next(item for item in text.splitlines() if item.startswith(FULL_LINE))
    assert f"{len(ROWS)} 行" in line and full_files[0].name in line
    payloads = load_fmp_forward_pit_raw_payloads(raw_dir, expected_as_of=AS_OF)
    assert sorted(payload.ticker for payload in payloads) == ["MSFT", "NVDA"]
    records = discover_existing_pit_raw_snapshots(
        fmp_analyst_history_dir=tmp_path / "none" / "a",
        fmp_historical_valuation_dir=tmp_path / "none" / "b",
        eodhd_earnings_trends_dir=tmp_path / "none" / "c",
        fmp_forward_pit_dir=raw_dir,
        project_root=tmp_path,
    )
    assert len(records) == 2
    # the canonical projection still replays the payloads byte for byte
    _project(tmp_path, raw_dir, normalized)


def test_the_cli_writes_a_zero_row_file_for_an_empty_calendar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, raw_dir, _, _ = _invoke_cli(tmp_path, monkeypatch, _Provider([]))

    assert result.exit_code == 0, result.output
    header, rows = load_fmp_earnings_calendar_full(next(raw_dir.glob(FULL_GLOB)))
    assert header["row_count"] == 0
    assert rows == ()


def test_the_cli_writes_no_full_file_when_the_calendar_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, raw_dir, _, _ = _invoke_cli(tmp_path, monkeypatch, _Provider(ROWS, fail_all=True))

    assert "FAIL" in result.output
    assert not raw_dir.exists() or not list(raw_dir.rglob("*"))


def test_a_failed_segment_is_marked_in_the_full_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fail_on = date(2026, 12, 1)
    result, raw_dir, _, _ = _invoke_cli(tmp_path, monkeypatch, _Provider(ROWS, fail_on=fail_on))

    assert result.exit_code == 0, result.output
    assert "PASS_WITH_WARNINGS" in result.output
    header, rows = load_fmp_earnings_calendar_full(next(raw_dir.glob(FULL_GLOB)))
    failed = [segment for segment in header["segments"] if segment.get("error") is True]
    assert len(failed) == 1
    assert failed[0]["from"] <= fail_on.isoformat() <= failed[0]["to"]
    assert len(rows) == len(ROWS)


def test_a_failed_write_is_a_warning_and_the_payloads_are_still_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_write(report: FmpForwardPitFetchReport, output_dir: Path) -> Path:
        raise OSError("disk full")

    monkeypatch.setattr(fmp_forward_pit, "write_fmp_earnings_calendar_full", broken_write)

    result, raw_dir, normalized, fetch_report_path = _invoke_cli(
        tmp_path, monkeypatch, _Provider(ROWS)
    )

    assert result.exit_code == 0, result.output
    assert "PASS_WITH_WARNINGS" in result.output
    assert len(list(raw_dir.rglob("*.json"))) == 2
    assert normalized.exists()
    assert list(raw_dir.glob(FULL_GLOB)) == []
    assert [path for path in raw_dir.rglob("*") if path.is_file() and path.suffix != ".json"] == []
    text = fetch_report_path.read_text(encoding="utf-8")
    assert f"| 警告 | {WRITE_FAILED} |" in text
    assert "- earnings-calendar 全窗口：写失败，当天算缺" in text


def test_the_full_file_holds_no_api_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result, raw_dir, _, _ = _invoke_cli(
        tmp_path, monkeypatch, _Provider(ROWS, fail_on=date(2026, 12, 1))
    )

    assert result.exit_code == 0, result.output
    data = gzip.decompress(next(raw_dir.glob(FULL_GLOB)).read_bytes())
    assert API_KEY.encode() not in data


def test_the_projection_report_counts_the_full_files_without_reading_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    result, raw_dir, normalized, _ = _invoke_cli(tmp_path, monkeypatch, _Provider(ROWS))
    assert result.exit_code == 0, result.output
    full = next(raw_dir.glob(FULL_GLOB))

    def line(text: str) -> str:
        return next(item for item in text.splitlines() if item.startswith(FULL_LINE))

    def others(text: str) -> list[str]:
        return [item for item in text.splitlines() if not item.startswith(FULL_LINE)]

    one = _project(tmp_path, raw_dir, normalized)
    assert line(one) == f"{FULL_LINE}投影不读，当天 capture 的 raw 目录里有 1 份"
    assert "- 状态：PASS" in one

    # a second file (never allowed in production) and an unreadable one are only counted
    (raw_dir / "fmp_earnings_calendar_full_2026-11-02T04-00-00Z.jsonl.gz").write_bytes(b"x")
    two = _project(tmp_path, raw_dir, normalized)
    assert line(two) == f"{FULL_LINE}投影不读，当天 capture 的 raw 目录里有 2 份，算缺"

    for path in raw_dir.glob(FULL_GLOB):
        path.unlink()
    none = _project(tmp_path, raw_dir, normalized)
    assert line(none) == f"{FULL_LINE}投影不读，当天 capture 的 raw 目录里没有，算缺"
    assert full.name not in none
    # the line changes nothing else in the report
    assert others(one) == others(none)


def test_a_fetch_report_before_any_write_says_so() -> None:
    report = _fetch(_Provider(ROWS))

    text = render_fmp_forward_pit_fetch_report(report)

    assert "- earnings-calendar 全窗口：" in text
    assert "投影不读" not in text
    assert all(issue.severity != FmpForwardPitIssueSeverity.ERROR for issue in report.issues)
    assert report.earnings_calendar_records
