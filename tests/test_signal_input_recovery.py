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
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_input_recovery as signal_recovery,
)


def _blocking_prior_and_restored(
    tmp_path: Path, fixture: dict[str, object], *, keep_blocked: bool = False
) -> tuple[dict[str, object], dict[str, object]]:
    paths = [
        tmp_path / "signal_inputs" / "features.csv",
        tmp_path / "signal_inputs" / "signals.csv",
    ]
    saved = {path: path.read_text(encoding="utf-8") for path in paths}
    for path in paths:
        path.unlink()
    previous = signal_inputs.run_signal_input_completeness_monitor(
        as_of=date(2024, 4, 22),
        policy_path=fixture["policy_path"],
        output_dir=fixture["output_dir"],
        generated_at=datetime(2024, 4, 23, tzinfo=UTC),
    )
    if not keep_blocked:
        for path, content in saved.items():
            path.write_text(content, encoding="utf-8")
    restored = signal_inputs.run_signal_input_completeness_monitor(
        as_of=date(2024, 4, 22),
        policy_path=fixture["policy_path"],
        output_dir=fixture["output_dir"],
        generated_at=datetime(2024, 4, 24, tzinfo=UTC),
    )
    return previous, restored


def test_signal_input_recovery_records_stale_root_cause_and_restored_inputs(
    tmp_path: Path,
) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    previous, restored = _blocking_prior_and_restored(tmp_path, fixture)

    result = signal_recovery.run_signal_input_root_cause_recovery(
        as_of=date(2024, 4, 22),
        restored_monitor_id=restored["monitor_id"],
        previous_monitor_id=previous["monitor_id"],
        signal_input_dir=fixture["output_dir"],
        policy_path=fixture["policy_path"],
        output_dir=tmp_path / "signal_input_recovery",
        generated_at=datetime(2024, 4, 25, tzinfo=UTC),
    )
    report = result["signal_input_recovery_report"]
    root_rows = {row["input_id"]: row for row in report["root_cause_rows"]}

    assert report["restoration_status"] == "SIGNAL_INPUTS_RESTORED"
    assert report["signal_input_status"] == "OK"
    assert report["restored_etf_feature_matrix_artifact_id"] is None
    assert report["restored_etf_signal_series_artifact_id"] is None
    assert root_rows["etf_feature_matrix"]["root_cause"] == "upstream_artifact_missing"
    assert root_rows["etf_signal_series"]["root_cause"] == "upstream_artifact_missing"
    assert all(row["canonical_artifact_id"] is None for row in report["restored_source_bindings"])
    assert result["signal_input_recovery_validation"]["status"] == "PASS"
    assert "signal_input_recovery_status" in result["reader_brief_section"]
    assert_research_safe(result["manifest"])


def test_signal_input_recovery_fail_closes_missing_feature_matrix_and_signal_series(
    tmp_path: Path,
) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    previous, current = _blocking_prior_and_restored(tmp_path, fixture, keep_blocked=True)

    result = signal_recovery.run_signal_input_root_cause_recovery(
        as_of=date(2024, 4, 22),
        restored_monitor_id=current["monitor_id"],
        previous_monitor_id=previous["monitor_id"],
        signal_input_dir=fixture["output_dir"],
        policy_path=fixture["policy_path"],
        output_dir=tmp_path / "signal_input_recovery",
        generated_at=datetime(2024, 4, 25, 1, tzinfo=UTC),
    )
    report = result["signal_input_recovery_report"]
    root_rows = {row["input_id"]: row for row in report["root_cause_rows"]}

    assert report["restoration_status"] == "SIGNAL_INPUTS_STILL_BLOCKED"
    assert report["hard_stop_triggered"] is True
    assert "etf_feature_matrix:blocking" in report["blocking_reasons"]
    assert "etf_signal_series:blocking" in report["blocking_reasons"]
    assert root_rows["etf_feature_matrix"]["root_cause"] == "upstream_artifact_missing"
    assert root_rows["etf_signal_series"]["root_cause"] == "upstream_artifact_missing"
    assert report["next_required_action"] == "stop_and_restore_signal_inputs"
    assert result["signal_input_recovery_validation"]["status"] == "PASS"
    assert report["manual_signal_artifact_fabrication"] is False


def test_signal_input_recovery_cli_run_report_and_validate(tmp_path: Path) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    previous, restored = _blocking_prior_and_restored(tmp_path, fixture)
    output_dir = tmp_path / "signal_input_recovery_cli"
    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "signal-input-recovery",
            "run",
            "--as-of",
            "2024-04-22",
            "--restored-monitor-id",
            restored["monitor_id"],
            "--previous-monitor-id",
            previous["monitor_id"],
            "--signal-input-dir",
            str(fixture["output_dir"]),
            "--policy-path",
            str(fixture["policy_path"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "restoration_status=SIGNAL_INPUTS_RESTORED" in run.output
    assert "validation_status=PASS" in run.output
    recovery_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("recovery_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "signal-input-recovery",
            "report",
            "--recovery-id",
            recovery_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "restoration_status=SIGNAL_INPUTS_RESTORED" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-signal-input-recovery",
            "--recovery-id",
            recovery_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_signal_input_recovery_without_prior_blocker_is_not_restored(tmp_path: Path) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    result = signal_recovery.run_signal_input_root_cause_recovery(
        restored_monitor_id=fixture["monitor_id"],
        signal_input_dir=fixture["output_dir"],
        output_dir=tmp_path / "signal_input_recovery",
        generated_at=datetime(2024, 4, 23, tzinfo=UTC),
    )
    report = result["signal_input_recovery_report"]

    assert report["restoration_status"] == "NO_RECOVERY_EVIDENCE"
    assert report["root_cause_rows"] == []
    assert report["hard_stop_triggered"] is True
    assert result["signal_input_recovery_validation"]["status"] == "PASS"
