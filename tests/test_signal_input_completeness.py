from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from dynamic_v3_filtered_candidate_readiness_helpers import (
    assert_research_safe,
    run_signal_input_completeness_fixture,
)
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_input_completeness as signal_inputs,
)


def test_signal_input_completeness_builds_and_validates(tmp_path: Path) -> None:
    result = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    report = result["signal_input_completeness_report"]
    validation = result["signal_input_completeness_validation"]
    payload = signal_inputs.signal_input_completeness_report_payload(
        monitor_id=result["monitor_id"],
        output_dir=tmp_path / "signal_input_completeness",
    )
    summary = signal_inputs.latest_signal_input_completeness_summary(
        monitor_id=result["monitor_id"],
        output_dir=tmp_path / "signal_input_completeness",
    )

    assert validation["status"] == "PASS"
    assert payload["signal_input_completeness_report"]["monitor_id"] == result["monitor_id"]
    assert report["signal_input_status"] == "OK"
    assert report["blocking_count"] == 0
    assert report["warning_count"] == 0
    assert summary["signal_input_status"] == "OK"
    assert "signal_input_status" in result["reader_brief_section"]
    assert_research_safe(result["manifest"])
    assert result["input_snapshot"]["schema_version"] == (
        "signal_input_completeness_input_snapshot.v2"
    )
    frozen = next(
        row for row in result["input_snapshot"]["raw_sources"] if row["exists"] is True
    )
    Path(frozen["frozen_path"]).write_text("tampered\n", encoding="utf-8")
    assert signal_inputs.validate_signal_input_completeness_artifact(
        monitor_id=result["monitor_id"],
        output_dir=tmp_path / "signal_input_completeness",
        write_output=False,
    )["status"] == "FAIL"


def test_signal_input_completeness_blocks_schema_and_coverage_gaps(tmp_path: Path) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    signals = tmp_path / "signal_inputs" / "signals.csv"
    signals.write_text(
        "\n".join(
            [
                "date,symbol,trend_score,momentum_score,relative_strength_score,risk_score,composite_score,direction,confidence,reason_codes,model_version,feature_version,created_at",
                "2024-04-22,QQQ,1,1,1,1,1,bullish,high,[],9.9.9,etf_features_v0_1,2024-04-22T00:00:00+00:00",
                "",
            ]
        ),
        encoding="utf-8",
    )

    result = signal_inputs.run_signal_input_completeness_monitor(
        as_of=date(2024, 4, 22),
        policy_path=fixture["policy_path"],
        output_dir=tmp_path / "signal_input_completeness_blocked",
        generated_at=datetime(2024, 4, 22, 1, tzinfo=UTC),
    )
    report = result["signal_input_completeness_report"]
    finding = {
        row["input_id"]: row for row in result["signal_input_completeness_findings"]
    }["etf_signal_series"]

    assert report["signal_input_status"] == "BLOCKING"
    assert "etf_signal_series" in report["incompatible_schema_inputs"]
    assert "etf_signal_series" in report["partial_market_coverage_inputs"]
    assert finding["incompatible_schema_versions"] == ["9.9.9"]
    assert finding["missing_coverage_values"] == ["SMH", "SOXX", "SPY"]
    assert result["signal_input_completeness_validation"]["status"] == "PASS"


def test_signal_input_completeness_cli_run_report_and_validate(tmp_path: Path) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    output_dir = tmp_path / "signal_input_completeness_cli"
    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "signal-input-completeness",
            "run",
            "--as-of",
            "2024-04-22",
            "--policy-path",
            str(fixture["policy_path"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "signal_input_status=OK" in run.output
    assert "validation_status=PASS" in run.output
    monitor_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("monitor_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "signal-input-completeness",
            "report",
            "--monitor-id",
            monitor_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "signal_input_status=OK" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-signal-input-completeness",
            "--monitor-id",
            monitor_id,
            "--policy-path",
            str(fixture["policy_path"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_signal_input_completeness_excludes_future_rows_from_as_of(tmp_path: Path) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    signals = tmp_path / "signal_inputs" / "signals.csv"
    with signals.open("a", encoding="utf-8") as handle:
        handle.write(
            "2024-04-23,QQQ,1,1,1,1,1,bullish,high,[],9.9.9,broken_future,"
            "2024-04-23T00:00:00+00:00\n"
        )
    result = signal_inputs.run_signal_input_completeness_monitor(
        as_of=date(2024, 4, 22),
        policy_path=fixture["policy_path"],
        output_dir=tmp_path / "signal_input_completeness_future",
        generated_at=datetime(2024, 4, 24, tzinfo=UTC),
    )
    finding = {
        row["input_id"]: row for row in result["signal_input_completeness_findings"]
    }["etf_signal_series"]

    assert finding["future_row_count"] == 1
    assert finding["eligible_row_count"] == 4
    assert finding["incompatible_schema_versions"] == []
    assert finding["observed_feature_versions"] == ["etf_features_v0_1"]
