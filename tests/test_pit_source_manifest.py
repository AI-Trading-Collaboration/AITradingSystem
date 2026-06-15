from __future__ import annotations

import json
from datetime import date
from hashlib import sha256
from pathlib import Path

import pandas as pd
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import DataSourceConfig, DataSourcesConfig
from ai_trading_system.pit_source_manifest import (
    build_and_write_pit_source_manifest,
    validate_pit_source_manifest_payload,
)
from ai_trading_system.reports import reader_brief


def test_pit_source_manifest_builds_grades_and_required_fields(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    payload, paths = build_and_write_pit_source_manifest(
        config=fixture["config"],
        as_of=date(2026, 6, 16),
        download_manifest_path=fixture["download_manifest"],
        output_dir=tmp_path / "reports",
        project_root=tmp_path,
    )
    records = {record["source_id"]: record for record in payload["records"]}
    validation = validate_pit_source_manifest_payload(
        payload,
        manifest_path=paths["manifest_json"],
    )

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert validation.status == "PASS_WITH_WARNINGS"
    assert records["sec_pit_filings"]["pit_quality_grade"] == "STRONG_PIT"
    assert records["fmp_price_vendor"]["pit_quality_grade"] == "APPROX_PIT"
    assert records["yahoo_convenience_prices"]["pit_quality_grade"] == "NON_PIT"
    assert records["news_vendor_tbd"]["pit_quality_grade"] == "UNKNOWN"
    assert records["sec_pit_filings"]["retrieval_time"] == "2026-06-16T10:00:00Z"
    assert records["sec_pit_filings"]["effective_date"] == "2026-06-15"
    assert records["sec_pit_filings"]["checksum"] != "UNKNOWN"
    assert paths["manifest_json"].exists()
    assert paths["manifest_markdown"].exists()
    assert paths["validation_json"].exists()
    assert paths["reader_brief_section"].exists()

    for record in payload["records"]:
        for field_name in (
            "source_id",
            "source_name",
            "retrieval_time",
            "effective_date",
            "revision_risk",
            "pit_quality_grade",
            "cache_path",
            "checksum",
            "refresh_policy",
            "validation_policy",
        ):
            assert field_name in record


def test_pit_source_manifest_validation_fails_invalid_grade(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    payload, paths = build_and_write_pit_source_manifest(
        config=fixture["config"],
        as_of=date(2026, 6, 16),
        download_manifest_path=fixture["download_manifest"],
        output_dir=tmp_path / "reports",
        project_root=tmp_path,
    )
    payload["records"][0]["pit_quality_grade"] = "BAD_GRADE"

    validation = validate_pit_source_manifest_payload(
        payload,
        manifest_path=paths["manifest_json"],
    )

    assert validation.status == "FAIL"
    assert any(
        issue.code == "source_record_invalid_pit_quality_grade"
        for issue in validation.issues
    )


def test_pit_source_manifest_cli_report_validate_and_reader_brief(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    config_path = tmp_path / "data_sources.yaml"
    config_path.write_text(
        yaml.safe_dump(fixture["config"].model_dump(), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    output_dir = tmp_path / "pit_manifest"

    report = CliRunner().invoke(
        app,
        [
            "data-sources",
            "pit-manifest",
            "report",
            "--config-path",
            str(config_path),
            "--download-manifest-path",
            str(fixture["download_manifest"]),
            "--as-of",
            "2026-06-16",
            "--output-dir",
            str(output_dir),
        ],
    )
    validation = CliRunner().invoke(
        app,
        [
            "data-sources",
            "pit-manifest",
            "validate",
            "--latest",
            "--output-dir",
            str(output_dir),
        ],
    )

    assert report.exit_code == 0, report.output
    assert validation.exit_code == 0, validation.output
    assert "PIT source manifest status=PASS_WITH_WARNINGS" in report.output
    assert "PIT source manifest validation status=PASS_WITH_WARNINGS" in validation.output

    latest_pointer = json.loads(
        (output_dir / "latest_pit_source_manifest.json").read_text(encoding="utf-8")
    )
    manifest_path = Path(latest_pointer["manifest_path"])
    summary = reader_brief._pit_source_manifest_summary(
        {
            "reports": [
                {
                    "report_id": "pit_source_manifest",
                    "latest_artifact_path": str(manifest_path),
                }
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["status"] == "PASS_WITH_WARNINGS"
    assert summary["strong_pit_count"] == 1
    assert "news_vendor_tbd" in summary["non_strong_source_ids"]
    assert summary["production_effect"] == "none"


def _fixture(tmp_path: Path) -> dict[str, object]:
    strong_cache = _write_cache(tmp_path / "sec_pit_filings.csv")
    approx_cache = _write_cache(tmp_path / "fmp_prices.csv")
    non_pit_cache = _write_cache(tmp_path / "yahoo_prices.csv")
    download_manifest = tmp_path / "download_manifest.csv"
    pd.DataFrame(
        [
            _manifest_row(
                source_id="sec_pit_filings",
                provider="SEC EDGAR",
                endpoint="https://data.sec.gov/submissions/CIK##########.json",
                output_path=strong_cache,
                checksum=_sha256(strong_cache),
            ),
            _manifest_row(
                source_id="fmp_price_vendor",
                provider="Financial Modeling Prep",
                endpoint="https://financialmodelingprep.com/stable/historical-price-eod",
                output_path=approx_cache,
                checksum=_sha256(approx_cache),
            ),
            _manifest_row(
                source_id="yahoo_convenience_prices",
                provider="Yahoo Finance",
                endpoint="yfinance.download",
                output_path=non_pit_cache,
                checksum=_sha256(non_pit_cache),
            ),
        ]
    ).to_csv(download_manifest, index=False)
    return {
        "config": DataSourcesConfig(
            sources=[
                _source(
                    source_id="sec_pit_filings",
                    provider="SEC EDGAR",
                    source_type="primary_source",
                    domains=["fundamentals"],
                    cache_paths=[str(strong_cache)],
                    audit_fields=[
                        "provider",
                        "endpoint",
                        "request_parameters",
                        "downloaded_at",
                        "accepted_datetime",
                        "available_time",
                        "checksum",
                    ],
                    validation_checks=[
                        "schema",
                        "future_available_time_rejection",
                        "raw_manifest_checksum",
                    ],
                ),
                _source(
                    source_id="fmp_price_vendor",
                    provider="Financial Modeling Prep",
                    source_type="paid_vendor",
                    domains=["market_prices"],
                    cache_paths=[str(approx_cache)],
                ),
                _source(
                    source_id="yahoo_convenience_prices",
                    provider="Yahoo Finance",
                    source_type="public_convenience",
                    domains=["market_prices"],
                    cache_paths=[str(non_pit_cache)],
                    limitations=["公开便利源，不是 strict point-in-time source。"],
                ),
                _source(
                    source_id="news_vendor_tbd",
                    provider="To be selected",
                    source_type="paid_vendor",
                    status="planned",
                    domains=["news_events"],
                    cache_paths=[],
                    validation_checks=["source_attribution"],
                ),
            ]
        ),
        "download_manifest": download_manifest,
    }


def _source(
    *,
    source_id: str,
    provider: str,
    source_type: str,
    domains: list[str],
    cache_paths: list[str],
    status: str = "active",
    audit_fields: list[str] | None = None,
    validation_checks: list[str] | None = None,
    limitations: list[str] | None = None,
) -> DataSourceConfig:
    return DataSourceConfig(
        source_id=source_id,
        provider=provider,
        source_type=source_type,
        status=status,
        domains=domains,
        endpoint="test.endpoint",
        adapter="TestAdapter",
        cadence="daily" if status != "planned" else "event_driven",
        requires_credentials=source_type == "paid_vendor",
        cache_paths=cache_paths,
        primary_for=["test_input"],
        audit_fields=audit_fields
        or [
            "provider",
            "endpoint",
            "request_parameters",
            "downloaded_at",
            "row_count",
            "checksum",
        ],
        validation_checks=validation_checks or ["schema", "checksum"],
        limitations=limitations
        or (["测试公开便利源限制。"] if source_type == "public_convenience" else []),
        owner_notes="test",
    )


def _manifest_row(
    *,
    source_id: str,
    provider: str,
    endpoint: str,
    output_path: Path,
    checksum: str,
) -> dict[str, object]:
    return {
        "downloaded_at": "2026-06-16T10:00:00Z",
        "source_id": source_id,
        "provider": provider,
        "endpoint": endpoint,
        "request_parameters": "{}",
        "output_path": str(output_path),
        "row_count": 1,
        "checksum_sha256": checksum,
    }


def _write_cache(path: Path) -> Path:
    path.write_text("date,value\n2026-06-15,1\n", encoding="utf-8")
    return path


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()
