"""DATA-033: one unavailable official source becomes a same-day gap, not a lost daily."""

from __future__ import annotations

import csv
import json
import ssl
import urllib.error
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.official_policy_sources import (
    OFFICIAL_POLICY_MAX_GAP_SOURCES,
    OFFICIAL_POLICY_SOURCE_GAP_CODE,
    OfficialPolicyHttpResponse,
    OfficialPolicyIssueSeverity,
    OfficialPolicySourceFetchReport,
    OfficialPolicyTransportError,
    fetch_official_policy_sources,
    render_official_policy_fetch_report,
)

AS_OF = date(2026, 11, 2)
SINCE = date(2026, 10, 30)
KEYS = {"CONGRESS_API_KEY": "dummy-congress", "GOVINFO_API_KEY": "dummy-govinfo"}
ALL_SOURCE_IDS = (
    "official_federal_register_policy_documents",
    "official_bis_federal_register_notices",
    "official_ofac_sdn_xml",
    "official_ofac_consolidated_xml",
    "official_ustr_press_releases",
    "official_trade_csl_json",
    "official_congress_bills",
    "official_govinfo_federal_register",
)
SDN = "SDN.XML"
CONSOLIDATED = "CONSOLIDATED.XML"
USTR = "ustr.gov"


def _body_for(url: str) -> bytes:
    if "federalregister.gov" in url:
        return json.dumps(
            {
                "results": [
                    {
                        "document_number": "2026-00001",
                        "title": "Entity List rules for advanced computing semiconductors",
                        "publication_date": "2026-11-02",
                        "html_url": "https://www.federalregister.gov/d/2026-00001",
                    }
                ]
            }
        ).encode("utf-8")
    if SDN in url or CONSOLIDATED in url:
        return (
            b"<sdnList><sdnEntry><uid>1</uid><lastName>PRC Semiconductor Entity</lastName>"
            b"<programList><program>CMIC sanctions</program></programList></sdnEntry></sdnList>"
        )
    if USTR in url:
        return (
            b'<html><a href="/press/section-301-semiconductor">'
            b"Section 301 investigation into semiconductor supply chains</a></html>"
        )
    if "data.trade.gov" in url:
        return json.dumps(
            {"results": [{"id": "csl-1", "name": "China Advanced Semiconductor Entity"}]}
        ).encode("utf-8")
    if "api.congress.gov" in url:
        return json.dumps(
            {
                "bills": [
                    {
                        "title": "A bill on export controls for advanced AI chips to China",
                        "url": "https://api.congress.gov/v3/bill/119/hr/1",
                        "updateDate": "2026-11-02",
                    }
                ]
            }
        ).encode("utf-8")
    if "api.govinfo.gov" in url:
        return json.dumps(
            {
                "packages": [
                    {
                        "packageId": "FR-2026-11-02",
                        "title": "Federal Register issue with export control notice",
                        "dateIssued": "2026-11-02",
                    }
                ]
            }
        ).encode("utf-8")
    return b"{}"


class GapClient:
    def __init__(
        self,
        *,
        down: Sequence[str] = (),
        status: Mapping[str, int] | None = None,
        raise_direct: Mapping[str, BaseException] | None = None,
    ) -> None:
        self.down = tuple(down)
        self.status = dict(status or {})
        self.raise_direct = dict(raise_direct or {})
        self.calls: list[str] = []

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: int = 30,
    ) -> OfficialPolicyHttpResponse:
        _ = headers
        self.calls.append(url)
        for marker, exc in self.raise_direct.items():
            if marker in url:
                raise exc
        if any(marker in url for marker in self.down):
            raise OfficialPolicyTransportError(
                attempt_count=3,
                max_attempts=3,
                timeout_seconds=timeout,
                exception=urllib.error.URLError(
                    TimeoutError("_ssl.c:1063: The handshake operation timed out")
                ),
            )
        for marker, code in self.status.items():
            if marker in url:
                return OfficialPolicyHttpResponse(
                    status_code=code, headers={}, body=b"<html>Service Unavailable</html>"
                )
        return OfficialPolicyHttpResponse(status_code=200, headers={}, body=_body_for(url))


def _fetch(
    tmp_path: Path,
    client: GapClient,
    *,
    api_keys: Mapping[str, str] = KEYS,
    selected_source_ids: Sequence[str] | None = None,
) -> OfficialPolicySourceFetchReport:
    return fetch_official_policy_sources(
        as_of=AS_OF,
        since=SINCE,
        raw_dir=tmp_path / "raw" / "official_policy_sources",
        processed_dir=tmp_path / "processed",
        api_keys=api_keys,
        selected_source_ids=selected_source_ids,
        http_client=client,
        download_manifest_path=tmp_path / "raw" / "official_policy_download_manifest.csv",
    )


def _errors(report: OfficialPolicySourceFetchReport) -> list[str]:
    return [
        issue.code for issue in report.issues if issue.severity == OfficialPolicyIssueSeverity.ERROR
    ]


def _manifest_rows(tmp_path: Path) -> list[dict[str, str]]:
    path = tmp_path / "raw" / "official_policy_download_manifest.csv"
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_constants() -> None:
    assert OFFICIAL_POLICY_MAX_GAP_SOURCES == 2
    assert OFFICIAL_POLICY_SOURCE_GAP_CODE == "official_policy_source_unavailable_gap"


def test_one_unavailable_source_becomes_a_gap(tmp_path: Path) -> None:
    client = GapClient(down=[CONSOLIDATED])
    report = _fetch(tmp_path, client)

    assert len(client.calls) == 8
    assert report.status == "PASS_WITH_WARNINGS"
    assert report.passed
    assert report.error_count == 0
    assert report.blocker_codes == ()
    assert report.gap_sources == ("official_ofac_consolidated_xml",)
    assert report.payload_count == 7
    assert "official_ofac_consolidated_xml" not in {p.source_id for p in report.payloads}
    assert len(report.issues) == 1
    issue = report.issues[0]
    assert issue.severity == OfficialPolicyIssueSeverity.WARNING
    assert issue.code == "official_policy_source_unavailable_gap"
    assert issue.source_id == "official_ofac_consolidated_xml"
    assert issue.blocker_code == "PROVIDER_UNAVAILABLE"
    assert "attempt_count=3" in issue.message
    assert "DATA-033" in issue.message
    assert report.candidate_count > 0


def test_two_unavailable_sources_are_both_gaps_in_request_order(tmp_path: Path) -> None:
    report = _fetch(tmp_path, GapClient(down=[CONSOLIDATED, SDN]))

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.gap_sources == ("official_ofac_sdn_xml", "official_ofac_consolidated_xml")
    assert [issue.code for issue in report.issues] == [OFFICIAL_POLICY_SOURCE_GAP_CODE] * 2
    assert report.payload_count == 6


def test_three_unavailable_sources_still_fail_unchanged(tmp_path: Path) -> None:
    report = _fetch(tmp_path, GapClient(down=[SDN, CONSOLIDATED, USTR]))

    assert report.status == "FAIL"
    assert not report.passed
    assert report.gap_sources == ()
    assert _errors(report) == ["official_policy_source_transport_exhausted"] * 3
    assert report.blocker_codes == ("PROVIDER_UNAVAILABLE",)
    assert OFFICIAL_POLICY_SOURCE_GAP_CODE not in {issue.code for issue in report.issues}


def test_http_5xx_source_is_a_gap_and_keeps_its_payload(tmp_path: Path) -> None:
    report = _fetch(tmp_path, GapClient(status={USTR: 503}))

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.gap_sources == ("official_ustr_press_releases",)
    assert report.payload_count == 8
    ustr = [p for p in report.payloads if p.source_id == "official_ustr_press_releases"]
    assert len(ustr) == 1
    assert ustr[0].status_code == 503
    assert ustr[0].candidate_count == 0
    assert len(report.issues) == 1
    assert report.issues[0].code == OFFICIAL_POLICY_SOURCE_GAP_CODE
    assert report.issues[0].blocker_code == "PROVIDER_UNAVAILABLE"
    assert "status=503" in report.issues[0].message


@pytest.mark.parametrize(
    ("status_code", "blocker_code"),
    [
        (429, "PROVIDER_QUOTA_EXHAUSTED"),
        (404, "REQUEST_FAILED"),
        (403, "PROVIDER_PERMISSION_DENIED"),
    ],
)
def test_other_http_errors_still_fail(
    tmp_path: Path,
    status_code: int,
    blocker_code: str,
) -> None:
    report = _fetch(tmp_path, GapClient(status={USTR: status_code}))

    assert report.status == "FAIL"
    assert report.gap_sources == ()
    assert _errors(report) == ["official_policy_source_http_error"]
    assert report.blocker_codes == (blocker_code,)


def test_unavailable_source_next_to_another_error_keeps_both_errors(tmp_path: Path) -> None:
    report = _fetch(tmp_path, GapClient(down=[CONSOLIDATED], status={USTR: 404}))

    assert report.status == "FAIL"
    assert report.gap_sources == ()
    assert _errors(report) == [
        "official_policy_source_transport_exhausted",
        "official_policy_source_http_error",
    ]
    assert report.blocker_codes == ("PROVIDER_UNAVAILABLE", "REQUEST_FAILED")


def test_unavailable_source_next_to_a_parse_failure_keeps_both_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_strip_namespace(tag: object) -> str:
        _ = tag
        raise TypeError("parser state corrupted")

    monkeypatch.setattr(
        "ai_trading_system.official_policy_sources._strip_namespace",
        broken_strip_namespace,
    )
    report = _fetch(tmp_path, GapClient(down=[CONSOLIDATED]))

    assert report.status == "FAIL"
    assert report.gap_sources == ()
    assert _errors(report) == [
        "official_policy_source_parse_failed",
        "official_policy_source_transport_exhausted",
    ]


def test_no_successful_source_still_fails(tmp_path: Path) -> None:
    report = _fetch(
        tmp_path,
        GapClient(down=[CONSOLIDATED], status={SDN: 503}),
        selected_source_ids=["official_ofac_sdn_xml", "official_ofac_consolidated_xml"],
    )

    assert report.status == "FAIL"
    assert report.gap_sources == ()
    assert sorted(_errors(report)) == [
        "official_policy_source_http_error",
        "official_policy_source_transport_exhausted",
    ]


def test_single_selected_unavailable_source_still_fails(tmp_path: Path) -> None:
    report = _fetch(
        tmp_path,
        GapClient(down=[CONSOLIDATED]),
        selected_source_ids=["official_ofac_consolidated_xml"],
    )

    assert report.status == "FAIL"
    assert report.gap_sources == ()
    assert report.payload_count == 0
    assert not (tmp_path / "raw" / "official_policy_download_manifest.csv").exists()


def test_direct_transport_error_from_client_counts_as_unavailable(tmp_path: Path) -> None:
    report = _fetch(
        tmp_path,
        GapClient(
            raise_direct={CONSOLIDATED: urllib.error.URLError(ssl.SSLEOFError("EOF in violation"))}
        ),
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.gap_sources == ("official_ofac_consolidated_xml",)
    assert report.issues[0].code == OFFICIAL_POLICY_SOURCE_GAP_CODE


def test_non_transport_download_error_still_fails(tmp_path: Path) -> None:
    report = _fetch(tmp_path, GapClient(raise_direct={CONSOLIDATED: OSError("disk gone")}))

    assert report.status == "FAIL"
    assert report.gap_sources == ()
    assert _errors(report) == ["official_policy_source_download_failed"]
    assert report.blocker_codes == ("REQUEST_FAILED",)


def test_missing_key_warning_does_not_count_toward_the_gap_limit(tmp_path: Path) -> None:
    report = _fetch(
        tmp_path,
        GapClient(down=[SDN, CONSOLIDATED]),
        api_keys={"GOVINFO_API_KEY": "dummy-govinfo"},
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.skipped_sources == ("official_congress_bills",)
    assert report.gap_sources == ("official_ofac_sdn_xml", "official_ofac_consolidated_xml")
    assert [issue.code for issue in report.issues] == [
        OFFICIAL_POLICY_SOURCE_GAP_CODE,
        OFFICIAL_POLICY_SOURCE_GAP_CODE,
        "official_policy_source_missing_api_key",
    ]


def test_raw_files_match_manifest_rows_with_a_gap(tmp_path: Path) -> None:
    _fetch(tmp_path, GapClient(down=[CONSOLIDATED], status={USTR: 503}))

    rows = _manifest_rows(tmp_path)
    raw_files = {
        path.resolve()
        for path in (tmp_path / "raw" / "official_policy_sources" / "2026-11-02").iterdir()
    }
    assert {Path(row["output_path"]).resolve() for row in rows} == raw_files
    assert {row["source_id"] for row in rows} == set(ALL_SOURCE_IDS) - {
        "official_ofac_consolidated_xml"
    }


def test_report_lists_gap_sources(tmp_path: Path) -> None:
    gap = render_official_policy_fetch_report(_fetch(tmp_path / "a", GapClient(down=[SDN])))
    clean = render_official_policy_fetch_report(_fetch(tmp_path / "b", GapClient()))

    assert "- 状态：PASS_WITH_WARNINGS" in gap
    assert "- 当天缺口来源：official_ofac_sdn_xml\n" in gap
    assert "DATA-033" in gap
    assert "- 状态：PASS" in clean
    assert "- 当天缺口来源：无\n" in clean


def test_cli_exits_zero_and_prints_gap_sources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _fetch(tmp_path, GapClient(down=[CONSOLIDATED]))
    monkeypatch.setattr(
        "ai_trading_system.cli_commands.risk_events.fetch_official_policy_sources",
        lambda **_kwargs: report,
    )

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "fetch-official-sources",
            "--as-of",
            "2026-11-02",
            "--output-path",
            str(tmp_path / "official_policy_sources_2026-11-02.md"),
        ],
    )

    assert result.exit_code == 0
    assert "PASS_WITH_WARNINGS" in result.stdout
    assert "gap_sources=official_ofac_consolidated_xml" in result.stdout
    assert "blocker_code=" not in result.stdout


def test_cli_prints_no_gap_line_when_the_run_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _fetch(tmp_path, GapClient(down=[SDN, CONSOLIDATED, USTR]))
    monkeypatch.setattr(
        "ai_trading_system.cli_commands.risk_events.fetch_official_policy_sources",
        lambda **_kwargs: report,
    )

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "fetch-official-sources",
            "--as-of",
            "2026-11-02",
            "--output-path",
            str(tmp_path / "official_policy_sources_2026-11-02.md"),
        ],
    )

    assert result.exit_code == 1
    assert "blocker_code=PROVIDER_UNAVAILABLE" in result.stdout
    assert "gap_sources=" not in result.stdout
