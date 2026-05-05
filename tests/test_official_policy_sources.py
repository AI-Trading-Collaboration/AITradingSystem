from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.official_policy_sources import (
    OfficialPolicyHttpResponse,
    build_official_policy_source_requests,
    fetch_official_policy_sources,
    render_official_policy_fetch_report,
)


class FakeOfficialPolicyHttpClient:
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: int = 30,
    ) -> OfficialPolicyHttpResponse:
        _ = (headers, timeout)
        body: bytes
        if "federalregister.gov" in url:
            body = json.dumps(
                {
                    "results": [
                        {
                            "document_number": "2026-00001",
                            "title": (
                                "Bureau of Industry and Security updates Entity List "
                                "rules for advanced computing semiconductors"
                            ),
                            "publication_date": "2026-05-02",
                            "html_url": "https://www.federalregister.gov/d/2026-00001",
                        }
                    ]
                },
                ensure_ascii=False,
            ).encode("utf-8")
        elif "SDN.XML" in url:
            body = b"""
            <sdnList>
              <sdnEntry>
                <uid>1</uid>
                <lastName>PRC Semiconductor Entity</lastName>
                <programList><program>CMIC sanctions</program></programList>
              </sdnEntry>
            </sdnList>
            """
        elif "CONSOLIDATED.XML" in url:
            body = b"""
            <sdnList>
              <sdnEntry>
                <uid>2</uid>
                <lastName>Russia Advanced Computing Entity</lastName>
                <programList><program>sectoral sanctions</program></programList>
              </sdnEntry>
            </sdnList>
            """
        elif "ustr.gov" in url:
            body = (
                b'<html><a href="/press/section-301-semiconductor">'
                b"Section 301 investigation into semiconductor supply chains"
                b"</a></html>"
            )
        elif "data.trade.gov" in url:
            body = json.dumps(
                {
                    "results": [
                        {
                            "id": "csl-1",
                            "name": "China Advanced Semiconductor Entity",
                            "source": "Entity List",
                            "source_information_url": "https://www.bis.gov/entity-list",
                        }
                    ]
                }
            ).encode("utf-8")
        elif "api.congress.gov" in url:
            body = json.dumps(
                {
                    "bills": [
                        {
                            "title": "A bill on export controls for advanced AI chips to China",
                            "url": "https://api.congress.gov/v3/bill/119/hr/1",
                            "updateDate": "2026-05-02",
                            "latestAction": {
                                "actionDate": "2026-05-02",
                                "text": "Introduced in House.",
                            },
                        }
                    ]
                }
            ).encode("utf-8")
        elif "api.govinfo.gov" in url:
            body = json.dumps(
                {
                    "packages": [
                        {
                            "packageId": "FR-2026-05-02",
                            "title": "Federal Register issue with export control notice",
                            "dateIssued": "2026-05-02",
                            "packageLink": "https://api.govinfo.gov/packages/FR-2026-05-02/summary",
                        }
                    ]
                }
            ).encode("utf-8")
        else:
            body = b"{}"
        return OfficialPolicyHttpResponse(status_code=200, headers={}, body=body)


class FailingOfficialPolicyHttpClient:
    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: int = 30,
    ) -> OfficialPolicyHttpResponse:
        _ = (url, headers, timeout)
        raise OSError("network unavailable")


def test_fetch_official_policy_sources_writes_raw_candidates_and_manifest(
    tmp_path: Path,
) -> None:
    raw_dir = tmp_path / "raw" / "official_policy_sources"
    processed_dir = tmp_path / "processed"
    manifest_path = tmp_path / "raw" / "download_manifest.csv"

    report = fetch_official_policy_sources(
        as_of=date(2026, 5, 5),
        since=date(2026, 5, 1),
        raw_dir=raw_dir,
        processed_dir=processed_dir,
        api_keys={
            "CONGRESS_API_KEY": "dummy-congress-value",
            "GOVINFO_API_KEY": "dummy-govinfo-value",
        },
        http_client=FakeOfficialPolicyHttpClient(),
        download_manifest_path=manifest_path,
    )
    markdown = render_official_policy_fetch_report(report)
    candidates_path = processed_dir / "official_policy_source_candidates_2026-05-05.csv"
    candidates = pd.read_csv(candidates_path)
    manifest = pd.read_csv(manifest_path)

    assert report.status == "PASS"
    assert report.payload_count == 8
    assert report.candidate_count >= 8
    assert candidates_path.exists()
    assert "ai_chip_export_control_upgrade" in set(
        ";".join(candidates["matched_risk_ids"].fillna("")).split(";")
    )
    assert set(candidates["review_status"]) == {"pending_review"}
    assert set(candidates["production_effect"]) == {"none"}
    assert "官方政策/地缘来源抓取报告" in markdown
    assert "不会写入 `risk_event_occurrence`" in markdown
    assert len(list((raw_dir / "2026-05-05").glob("*"))) == 8
    assert set(manifest["source_id"]).issuperset(
        {
            "official_federal_register_policy_documents",
            "official_bis_federal_register_notices",
            "official_ofac_sdn_xml",
            "official_congress_bills",
            "official_govinfo_federal_register",
        }
    )
    assert "dummy-congress-value" not in "\n".join(manifest["endpoint"].astype(str))
    assert "dummy-govinfo-value" not in "\n".join(manifest["endpoint"].astype(str))


def test_federal_register_request_uses_supported_fields_query() -> None:
    requests = build_official_policy_source_requests(
        as_of=date(2026, 5, 5),
        since=date(2026, 5, 1),
        api_keys={},
    )
    federal_request = next(
        request
        for request in requests
        if request.source_id == "official_federal_register_policy_documents"
    )

    assert "fields%5B%5D=document_number" in federal_request.endpoint
    assert "fields%5B0%5D" not in federal_request.endpoint


def test_fetch_official_policy_sources_skips_missing_api_key(tmp_path: Path) -> None:
    report = fetch_official_policy_sources(
        as_of=date(2026, 5, 5),
        since=date(2026, 5, 1),
        raw_dir=tmp_path / "raw",
        processed_dir=tmp_path / "processed",
        selected_source_ids=["official_congress_bills"],
        http_client=FakeOfficialPolicyHttpClient(),
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.payload_count == 0
    assert report.skipped_sources == ("official_congress_bills",)
    assert "official_policy_source_missing_api_key" in {issue.code for issue in report.issues}


def test_fetch_official_policy_sources_fails_unknown_source_id(tmp_path: Path) -> None:
    report = fetch_official_policy_sources(
        as_of=date(2026, 5, 5),
        since=date(2026, 5, 1),
        raw_dir=tmp_path / "raw",
        processed_dir=tmp_path / "processed",
        selected_source_ids=["missing_source"],
        http_client=FakeOfficialPolicyHttpClient(),
    )

    assert report.status == "FAIL"
    assert report.payload_count == 0
    assert "official_policy_source_unknown_source_id" in {
        issue.code for issue in report.issues
    }


def test_fetch_official_policy_sources_records_download_failure(tmp_path: Path) -> None:
    manifest_path = tmp_path / "raw" / "download_manifest.csv"
    report = fetch_official_policy_sources(
        as_of=date(2026, 5, 5),
        since=date(2026, 5, 1),
        raw_dir=tmp_path / "raw",
        processed_dir=tmp_path / "processed",
        selected_source_ids=["official_federal_register_policy_documents"],
        http_client=FailingOfficialPolicyHttpClient(),
        download_manifest_path=manifest_path,
    )

    assert report.status == "FAIL"
    assert report.payload_count == 0
    assert not manifest_path.exists()
    assert "official_policy_source_download_failed" in {issue.code for issue in report.issues}


def test_risk_events_fetch_official_sources_cli_help() -> None:
    result = CliRunner().invoke(app, ["risk-events", "fetch-official-sources", "--help"])

    assert result.exit_code == 0
    assert "抓取低成本官方政策/地缘来源" in result.output
