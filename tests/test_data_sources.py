from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import DataSourcesConfig, load_data_sources
from ai_trading_system.data_sources import (
    DataSourceIssueSeverity,
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
