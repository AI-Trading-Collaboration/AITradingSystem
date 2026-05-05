from __future__ import annotations

from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import DataSourceConfig, DataSourcesConfig, load_data_sources
from ai_trading_system.data_sources import (
    DataSourceIssueSeverity,
    build_data_source_health_report,
    render_data_source_health_report,
    render_data_sources_validation_report,
    validate_data_sources_config,
)


def test_validate_data_sources_config_passes_default_catalog() -> None:
    report = validate_data_sources_config(load_data_sources(), as_of=date(2026, 5, 2))

    assert report.status == "PASS"
    assert report.active_count >= 5
    assert report.planned_count >= 1


def test_validate_data_sources_config_rejects_duplicate_source_id() -> None:
    config = load_data_sources()
    duplicate = config.sources[0].model_copy()
    report = validate_data_sources_config(
        DataSourcesConfig(sources=[config.sources[0], duplicate]),
        as_of=date(2026, 5, 2),
    )

    assert not report.passed
    assert any(
        issue.severity == DataSourceIssueSeverity.ERROR
        and issue.code == "duplicate_source_id"
        for issue in report.issues
    )


def test_validate_data_sources_config_requires_active_audit_fields() -> None:
    config = load_data_sources()
    broken = config.sources[0].model_copy(update={"audit_fields": []})
    report = validate_data_sources_config(
        DataSourcesConfig(sources=[broken]),
        as_of=date(2026, 5, 2),
    )

    assert not report.passed
    assert "active_source_missing_audit_fields" in {issue.code for issue in report.issues}


def test_render_data_sources_validation_report_is_chinese() -> None:
    report = validate_data_sources_config(load_data_sources(), as_of=date(2026, 5, 2))
    markdown = render_data_sources_validation_report(report)

    assert "# 数据源目录校验报告" in markdown
    assert "公开便利源" in markdown
    assert "审计要求" in markdown


def test_build_data_source_health_report_flags_reconciliation_gap(
    tmp_path: Path,
) -> None:
    prices_path = _write_price_cache(tmp_path / "prices_daily.csv")
    manifest_path = _write_manifest(
        tmp_path / "download_manifest.csv",
        source_id="test_prices",
        provider="Test Prices",
        endpoint="test.download",
        output_path=prices_path,
        row_count=1,
        checksum=_sha256_file(prices_path),
    )
    config = DataSourcesConfig(
        sources=[
            _source(
                source_id="test_prices",
                provider="Test Prices",
                source_type="public_convenience",
                domains=["market_prices"],
                cache_paths=[str(prices_path)],
            )
        ]
    )

    report = build_data_source_health_report(
        config=config,
        as_of=date(2026, 5, 2),
        manifest_path=manifest_path,
        project_root=tmp_path,
    )
    markdown = render_data_source_health_report(report)

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.records[0].latest_manifest is not None
    assert report.records[0].latest_manifest.row_count == 1
    assert "NOT_COVERED" in markdown
    assert "跨供应商 reconciliation 未覆盖" in markdown
    assert _sha256_file(prices_path)[:12] in markdown


def test_build_data_source_health_report_marks_market_prices_baseline_covered(
    tmp_path: Path,
) -> None:
    prices_path = _write_price_cache(tmp_path / "prices_daily.csv")
    secondary_path = _write_price_cache(tmp_path / "prices_marketstack_daily.csv")
    manifest_path = _write_manifest(
        tmp_path / "download_manifest.csv",
        source_id="marketstack_prices",
        provider="Marketstack",
        endpoint="https://api.marketstack.com/v2/eod",
        output_path=secondary_path,
        row_count=1,
        checksum=_sha256_file(secondary_path),
    )
    config = DataSourcesConfig(
        sources=[
            _source(
                source_id="yahoo_prices",
                provider="Yahoo Finance",
                source_type="public_convenience",
                domains=["market_prices"],
                cache_paths=[str(prices_path)],
            ),
            _source(
                source_id="marketstack_prices",
                provider="Marketstack",
                source_type="paid_vendor",
                domains=["market_prices"],
                cache_paths=[str(secondary_path)],
            ),
        ]
    )

    report = build_data_source_health_report(
        config=config,
        as_of=date(2026, 5, 2),
        manifest_path=manifest_path,
        project_root=tmp_path,
    )
    market_prices = next(
        item for item in report.domain_reconciliation if item.domain == "market_prices"
    )

    assert market_prices.status == "COVERED_BASELINE"
    assert market_prices.qualified_source_ids == ("marketstack_prices",)


def test_build_data_source_health_report_fails_on_checksum_mismatch(
    tmp_path: Path,
) -> None:
    prices_path = _write_price_cache(tmp_path / "prices_daily.csv")
    manifest_path = _write_manifest(
        tmp_path / "download_manifest.csv",
        source_id="test_prices",
        provider="Test Prices",
        endpoint="test.download",
        output_path=prices_path,
        row_count=1,
        checksum="bad-checksum",
    )
    config = DataSourcesConfig(
        sources=[
            _source(
                source_id="test_prices",
                provider="Test Prices",
                source_type="primary_source",
                domains=["market_prices"],
                cache_paths=[str(prices_path)],
            )
        ]
    )

    report = build_data_source_health_report(
        config=config,
        as_of=date(2026, 5, 2),
        manifest_path=manifest_path,
        project_root=tmp_path,
    )

    assert report.status == "FAIL"
    assert any(issue.code == "manifest_checksum_mismatch" for issue in report.issues)


def test_build_data_source_health_report_warns_on_inactive_checksum_mismatch(
    tmp_path: Path,
) -> None:
    prices_path = _write_price_cache(tmp_path / "prices_daily.csv")
    manifest_path = _write_manifest(
        tmp_path / "download_manifest.csv",
        source_id="inactive_prices",
        provider="Inactive Prices",
        endpoint="test.download",
        output_path=prices_path,
        row_count=1,
        checksum="bad-checksum",
    )
    config = DataSourcesConfig(
        sources=[
            _source(
                source_id="inactive_prices",
                provider="Inactive Prices",
                source_type="public_convenience",
                status="inactive",
                domains=["market_prices"],
                cache_paths=[str(prices_path)],
            )
        ]
    )

    report = build_data_source_health_report(
        config=config,
        as_of=date(2026, 5, 2),
        manifest_path=manifest_path,
        project_root=tmp_path,
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert not any(
        issue.code == "manifest_checksum_mismatch" for issue in report.issues
    )
    inactive_issue = next(
        issue
        for issue in report.issues
        if issue.code == "inactive_manifest_checksum_mismatch"
    )
    assert inactive_issue.severity == DataSourceIssueSeverity.WARNING


def test_data_sources_cli_validate_and_list(tmp_path: Path) -> None:
    report_path = tmp_path / "data_sources.md"

    validate_result = CliRunner().invoke(
        app,
        [
            "data-sources",
            "validate",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(report_path),
        ],
    )
    list_result = CliRunner().invoke(app, ["data-sources", "list", "--active-only"])

    assert validate_result.exit_code == 0
    assert list_result.exit_code == 0
    assert "数据源目录校验状态：PASS" in validate_result.output
    assert "数据源目录" in list_result.output
    assert report_path.exists()
    assert "yahoo_finance_daily_prices" in report_path.read_text(encoding="utf-8")


def test_data_sources_cli_health_writes_report(tmp_path: Path) -> None:
    prices_path = _write_price_cache(tmp_path / "prices_daily.csv")
    manifest_path = _write_manifest(
        tmp_path / "download_manifest.csv",
        source_id="test_prices",
        provider="Test Prices",
        endpoint="test.download",
        output_path=prices_path,
        row_count=1,
        checksum=_sha256_file(prices_path),
    )
    config = DataSourcesConfig(
        sources=[
            _source(
                source_id="test_prices",
                provider="Test Prices",
                source_type="public_convenience",
                domains=["market_prices"],
                cache_paths=[str(prices_path)],
            )
        ]
    )
    config_path = tmp_path / "data_sources.yaml"
    config_path.write_text(
        yaml.safe_dump(config.model_dump(), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
    report_path = tmp_path / "data_sources_health.md"

    result = CliRunner().invoke(
        app,
        [
            "data-sources",
            "health",
            "--config-path",
            str(config_path),
            "--manifest-path",
            str(manifest_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "数据源健康状态：PASS_WITH_WARNINGS" in result.output
    assert report_path.exists()
    assert "数据源健康与 reconciliation 报告" in report_path.read_text(
        encoding="utf-8"
    )


def _source(
    *,
    source_id: str,
    provider: str,
    source_type: str,
    domains: list[str],
    cache_paths: list[str],
    status: str = "active",
) -> DataSourceConfig:
    return DataSourceConfig(
        source_id=source_id,
        provider=provider,
        source_type=source_type,
        status=status,
        domains=domains,
        endpoint="test.download",
        adapter="TestProvider",
        cadence="daily",
        requires_credentials=False,
        cache_paths=cache_paths,
        primary_for=["daily_ohlcv"],
        audit_fields=[
            "provider",
            "endpoint",
            "request_parameters",
            "downloaded_at",
            "row_count",
            "checksum",
        ],
        validation_checks=["schema", "checksum"],
        limitations=["测试来源。"] if source_type == "public_convenience" else [],
        owner_notes="test",
    )


def _write_price_cache(path: Path) -> Path:
    pd.DataFrame(
        [
            {
                "date": "2026-05-01",
                "ticker": "NVDA",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "adj_close": 100.5,
                "volume": 1000,
            }
        ]
    ).to_csv(path, index=False)
    return path


def _write_manifest(
    path: Path,
    *,
    source_id: str,
    provider: str,
    endpoint: str,
    output_path: Path,
    row_count: int,
    checksum: str,
) -> Path:
    pd.DataFrame(
        [
            {
                "downloaded_at": datetime(2026, 5, 2, tzinfo=UTC).isoformat(),
                "source_id": source_id,
                "provider": provider,
                "endpoint": endpoint,
                "request_parameters": '{"tickers":["NVDA"]}',
                "output_path": str(output_path),
                "row_count": row_count,
                "checksum_sha256": checksum,
            }
        ]
    ).to_csv(path, index=False)
    return path


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
