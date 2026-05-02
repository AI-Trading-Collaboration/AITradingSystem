from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig, load_sec_companies
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecCompanyFactsRequest,
    download_sec_companyfacts,
)


@dataclass(frozen=True)
class FakeSecCompanyFactsProvider:
    def download_companyfacts(self, request: SecCompanyFactsRequest) -> dict[str, Any]:
        return {
            "cik": int(request.cik),
            "entityName": f"{request.ticker} Test Entity",
            "facts": {
                "us-gaap": {
                    "Revenues": {
                        "units": {
                            "USD": [
                                {
                                    "fy": 2025,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "val": 1000,
                                    "filed": "2026-01-31",
                                }
                            ]
                        }
                    }
                },
                "dei": {},
            },
        }

    def endpoint_for(self, cik: str) -> str:
        return f"https://example.test/companyfacts/CIK{cik}.json"


def test_download_sec_companyfacts_writes_json_and_manifest(tmp_path: Path) -> None:
    config = SecCompaniesConfig(
        companies=[
            SecCompanyConfig(
                ticker="NVDA",
                cik="0001045810",
                company_name="NVIDIA Corporation",
                expected_taxonomies=["us-gaap", "dei"],
            )
        ]
    )

    summary = download_sec_companyfacts(
        config=config,
        output_dir=tmp_path,
        provider=FakeSecCompanyFactsProvider(),
    )

    assert summary.company_count == 1
    assert summary.total_fact_count == 1
    assert summary.files[0].output_path == tmp_path / "nvda_companyfacts.json"
    assert summary.files[0].checksum_sha256
    assert summary.manifest_path == tmp_path / "sec_companyfacts_manifest.csv"

    manifest = pd.read_csv(summary.manifest_path)
    assert manifest.loc[0, "source_id"] == "sec_company_facts"
    assert manifest.loc[0, "ticker"] == "NVDA"
    assert manifest.loc[0, "fact_count"] == 1


def test_download_sec_companyfacts_rejects_unknown_ticker(tmp_path: Path) -> None:
    config = load_sec_companies()

    try:
        download_sec_companyfacts(
            config=config,
            output_dir=tmp_path,
            provider=FakeSecCompanyFactsProvider(),
            tickers=["UNKNOWN"],
        )
    except ValueError as exc:
        assert "UNKNOWN" in str(exc)
    else:
        raise AssertionError("expected unknown ticker to fail")


def test_fundamentals_cli_lists_sec_companies() -> None:
    result = CliRunner().invoke(app, ["fundamentals", "list-sec-companies"])

    assert result.exit_code == 0
    assert "SEC Company Facts" in result.output
    assert "NVDA" in result.output


def test_fundamentals_cli_requires_sec_user_agent(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "download-sec-companyfacts",
            "--tickers",
            "NVDA",
            "--output-dir",
            str(tmp_path),
        ],
        env={"SEC_USER_AGENT": ""},
    )

    assert result.exit_code != 0
    assert "SEC companyfacts 下载必须提供" in result.output
