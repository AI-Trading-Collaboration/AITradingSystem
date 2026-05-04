from __future__ import annotations

from datetime import date
from hashlib import sha256
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.pipeline_health import (
    PipelineArtifactSpec,
    build_pipeline_health_report,
    build_pit_snapshot_health_checks,
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


def test_pit_snapshot_health_checks_flag_stale_and_checksum_mismatch(
    tmp_path: Path,
) -> None:
    payload_path = tmp_path / "raw" / "payload.json"
    payload_path.parent.mkdir(parents=True)
    payload_path.write_text('{"records":[1]}\n', encoding="utf-8")
    manifest_path = tmp_path / "manifest.csv"
    manifest_path.write_text(
        "\n".join(
            [
                "snapshot_id,raw_payload_path,raw_payload_sha256,available_time",
                (
                    "snapshot-1,"
                    f"{payload_path},"
                    "0000000000000000000000000000000000000000000000000000000000000000,"
                    "2026-05-01T00:00:00+00:00"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    normalized_path = tmp_path / "fmp_forward_pit_2026-05-04.csv"
    normalized_path.write_text(
        "normalized_id,available_time\n"
        "row-1,2026-05-01T00:00:00+00:00\n",
        encoding="utf-8",
    )
    validation_report_path = _write_artifact(tmp_path / "pit_validation.md")

    report = build_pipeline_health_report(
        as_of=date(2026, 5, 4),
        artifacts=(),
        extra_checks=build_pit_snapshot_health_checks(
            as_of=date(2026, 5, 4),
            manifest_path=manifest_path,
            normalized_path=normalized_path,
            validation_report_path=validation_report_path,
            project_root=tmp_path,
            max_snapshot_age_days=1,
        ),
    )
    messages_by_id = {
        check.spec.artifact_id: check.message
        for check in report.checks
        if check.severity is not None
    }

    assert report.status == "FAIL"
    assert "PIT raw payload checksum 异常" in messages_by_id["pit_manifest_checksum"]
    assert "距 as_of 已 3 天" in messages_by_id["pit_manifest_freshness"]
    assert "距 as_of 已 3 天" in messages_by_id[
        "fmp_forward_pit_normalized_freshness"
    ]


def test_pipeline_health_cli_writes_report(tmp_path: Path) -> None:
    prices_path = _write_artifact(tmp_path / "prices_daily.csv")
    rates_path = _write_artifact(tmp_path / "rates_daily.csv")
    features_path = _write_artifact(tmp_path / "features_daily.csv")
    scores_path = _write_artifact(tmp_path / "scores_daily.csv")
    quality_path = _write_artifact(tmp_path / "data_quality.md")
    daily_report_path = _write_artifact(tmp_path / "daily_score.md")
    pit_paths = _write_pit_health_inputs(tmp_path, date(2026, 5, 4))
    output_path = tmp_path / "pipeline_health.md"
    alert_output_path = tmp_path / "pipeline_health_alerts.md"

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
            "--pit-manifest-path",
            str(pit_paths["manifest"]),
            "--pit-normalized-path",
            str(pit_paths["normalized"]),
            "--pit-validation-report-path",
            str(pit_paths["validation_report"]),
            "--output-path",
            str(output_path),
            "--alert-output-path",
            str(alert_output_path),
        ],
    )

    assert result.exit_code == 0
    assert "Pipeline health：PASS" in result.output
    assert "活跃告警数：0" in result.output
    assert output_path.exists()
    assert alert_output_path.exists()
    markdown = output_path.read_text(encoding="utf-8")
    assert "价格缓存" in markdown
    assert "PIT manifest row count" in markdown
    assert "PIT manifest raw payload checksum" in markdown
    assert "未触发告警" in alert_output_path.read_text(encoding="utf-8")


def _write_artifact(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("ok\n", encoding="utf-8")
    return path


def _write_pit_health_inputs(tmp_path: Path, as_of: date) -> dict[str, Path]:
    payload_path = tmp_path / "raw" / "fmp_forward_pit" / "nvda" / "payload.json"
    payload_path.parent.mkdir(parents=True)
    payload_path.write_text('{"records":[{"symbol":"NVDA"}]}\n', encoding="utf-8")
    checksum = sha256(payload_path.read_bytes()).hexdigest()
    available_time = f"{as_of.isoformat()}T01:00:00+00:00"
    manifest_path = tmp_path / "pit_snapshots" / "manifest.csv"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        "\n".join(
            [
                "snapshot_id,raw_payload_path,raw_payload_sha256,available_time",
                f"snapshot-1,{payload_path},{checksum},{available_time}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    normalized_path = tmp_path / "processed" / f"fmp_forward_pit_{as_of.isoformat()}.csv"
    normalized_path.parent.mkdir(parents=True)
    normalized_path.write_text(
        "normalized_id,available_time\n"
        f"row-1,{available_time}\n",
        encoding="utf-8",
    )
    validation_report_path = _write_artifact(tmp_path / "reports" / "pit_validation.md")
    return {
        "manifest": manifest_path,
        "normalized": normalized_path,
        "validation_report": validation_report_path,
    }
