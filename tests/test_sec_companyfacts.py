from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig, load_sec_companies
from ai_trading_system.fundamentals.sec_companyfacts import (
    SecCompanyFactsRequest,
    _write_json,
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


@dataclass(frozen=True)
class FakeRawSecCompanyFactsProvider(FakeSecCompanyFactsProvider):
    def download_companyfacts_raw(self, request: SecCompanyFactsRequest) -> bytes:
        return (
            b'{"cik":1045810,"entityName":"Raw Entity","facts":{"us-gaap":'
            b'{"Revenues":{"units":{"USD":[{"fy":2025,"fp":"FY","form":"10-K",'
            b'"val":1000,"filed":"2026-01-31"}]}}},"dei":{}}}'
        )


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


def test_download_sec_companyfacts_preserves_raw_json_bytes(tmp_path: Path) -> None:
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
        provider=FakeRawSecCompanyFactsProvider(),
    )

    raw_text = summary.files[0].output_path.read_text(encoding="utf-8")
    assert raw_text.startswith('{"cik":1045810')
    assert raw_text.endswith("\n")
    assert summary.total_fact_count == 1


def test_write_json_streams_compact_valid_json_without_sorting(tmp_path: Path) -> None:
    output_path = tmp_path / "companyfacts.json"
    payload = {
        "z_key": "kept_first",
        "a_key": "kept_second",
        "facts": {"us-gaap": {"Revenues": {"units": {"USD": []}}}},
    }

    _write_json(payload, output_path)

    text = output_path.read_text(encoding="utf-8")
    assert text == (
        '{"z_key":"kept_first","a_key":"kept_second",'
        '"facts":{"us-gaap":{"Revenues":{"units":{"USD":[]}}}}}\n'
    )
    assert json.loads(text) == payload


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
