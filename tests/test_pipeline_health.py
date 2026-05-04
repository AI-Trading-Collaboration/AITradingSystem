from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.pipeline_health import (
    PipelineArtifactSpec,
    build_pipeline_health_report,
    render_pipeline_health_report,
)


def test_pipeline_health_report_flags_missing_required_artifact(tmp_path: Path) -> None:
    existing = tmp_path / "prices.csv"
    existing.write_text("date,ticker,adj_close\n", encoding="utf-8")

    report = build_pipeline_health_report(
        as_of=date(2026, 5, 4),
        artifacts=(
            PipelineArtifactSpec(
                "prices_daily",
                "价格缓存",
                existing,
                True,
                "run download-data",
            ),
            PipelineArtifactSpec(
                "daily_score_report",
                "每日评分报告",
                tmp_path / "missing.md",
                True,
                "run score-daily",
            ),
        ),
    )
    markdown = render_pipeline_health_report(report)

    assert report.status == "FAIL"
    assert report.error_count == 1
    assert "运行健康不等于投资结论有效" in markdown
    assert "daily_score_report" in markdown


def test_pipeline_health_cli_writes_report(tmp_path: Path) -> None:
    prices_path = _write_artifact(tmp_path / "prices_daily.csv")
    rates_path = _write_artifact(tmp_path / "rates_daily.csv")
    features_path = _write_artifact(tmp_path / "features_daily.csv")
    scores_path = _write_artifact(tmp_path / "scores_daily.csv")
    quality_path = _write_artifact(tmp_path / "data_quality.md")
    daily_report_path = _write_artifact(tmp_path / "daily_score.md")
    output_path = tmp_path / "pipeline_health.md"

    result = CliRunner().invoke(
        app,
        [
            "ops",
            "health",
            "--as-of",
            "2026-05-04",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--features-path",
            str(features_path),
            "--scores-path",
            str(scores_path),
            "--data-quality-report-path",
            str(quality_path),
            "--daily-report-path",
            str(daily_report_path),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "Pipeline health：PASS" in result.output
    assert output_path.exists()
    assert "价格缓存" in output_path.read_text(encoding="utf-8")


def _write_artifact(path: Path) -> Path:
    path.write_text("ok\n", encoding="utf-8")
    return path
