from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import SecCompaniesConfig, SecCompanyConfig
from ai_trading_system.fundamentals.sec_validation import (
    render_sec_companyfacts_validation_report,
    validate_sec_companyfacts_cache,
)


def test_validate_sec_companyfacts_cache_passes_valid_file(tmp_path: Path) -> None:
    config = _sec_config()
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)

    report = validate_sec_companyfacts_cache(config, input_dir=tmp_path, as_of=_date())
    markdown = render_sec_companyfacts_validation_report(report)

    assert report.status == "PASS"
    assert report.available_count == 1
    assert report.files[0].facts_count == 1
    assert "SEC Company Facts 缓存校验报告" in markdown


def test_validate_sec_companyfacts_cache_rejects_cik_mismatch(tmp_path: Path) -> None:
    config = _sec_config()
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0000000001")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)

    report = validate_sec_companyfacts_cache(config, input_dir=tmp_path, as_of=_date())

    assert not report.passed
    assert "sec_companyfacts_cik_mismatch" in {issue.code for issue in report.issues}


def test_validate_sec_companyfacts_cache_rejects_missing_taxonomy(tmp_path: Path) -> None:
    config = _sec_config(expected_taxonomies=["ifrs-full"])
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)

    report = validate_sec_companyfacts_cache(config, input_dir=tmp_path, as_of=_date())

    assert not report.passed
    assert "sec_companyfacts_missing_expected_taxonomy" in {
        issue.code for issue in report.issues
    }


def test_fundamentals_cli_validate_sec_companyfacts(tmp_path: Path) -> None:
    config_path = tmp_path / "sec_companies.yaml"
    config_path.write_text(
        """
companies:
  - ticker: NVDA
    cik: "0001045810"
    company_name: NVIDIA Corporation
    expected_taxonomies:
      - us-gaap
      - dei
""",
        encoding="utf-8",
    )
    json_path = _write_companyfacts(tmp_path, ticker="NVDA", cik="0001045810")
    _write_manifest(tmp_path, ticker="NVDA", cik="0001045810", json_path=json_path)
    report_path = tmp_path / "sec_validation.md"

    result = CliRunner().invoke(
        app,
        [
            "fundamentals",
            "validate-sec-companyfacts",
            "--config-path",
            str(config_path),
            "--input-dir",
            str(tmp_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "SEC companyfacts 校验状态：PASS" in result.output
    assert report_path.exists()


def _sec_config(expected_taxonomies: list[str] | None = None) -> SecCompaniesConfig:
    return SecCompaniesConfig(
        companies=[
            SecCompanyConfig(
                ticker="NVDA",
                cik="0001045810",
                company_name="NVIDIA Corporation",
                expected_taxonomies=expected_taxonomies or ["us-gaap", "dei"],
            )
        ]
    )


def _write_companyfacts(tmp_path: Path, ticker: str, cik: str) -> Path:
    json_path = tmp_path / f"{ticker.lower()}_companyfacts.json"
    json_path.write_text(
        json.dumps(
            {
                "cik": int(cik),
                "entityName": f"{ticker} Test Entity",
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
                                    }
                                ]
                            }
                        }
                    },
                    "dei": {},
                },
            },
        ),
        encoding="utf-8",
    )
    return json_path


def _write_manifest(tmp_path: Path, ticker: str, cik: str, json_path: Path) -> None:
    pd.DataFrame(
        [
            {
                "ticker": ticker,
                "cik": cik,
                "checksum_sha256": _sha256(json_path),
            }
        ]
    ).to_csv(tmp_path / "sec_companyfacts_manifest.csv", index=False)


def _sha256(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _date() -> date:
    return date(2026, 5, 2)
