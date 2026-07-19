from __future__ import annotations

import json
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
    dynamic_v3_signal_input_completeness_recovery as recovery,
)


def _blocking_prior_and_restore(tmp_path: Path, fixture: dict[str, object]) -> dict[str, object]:
    paths = [
        tmp_path / "signal_inputs" / "features.csv",
        tmp_path / "signal_inputs" / "signals.csv",
    ]
    saved = {path: path.read_text(encoding="utf-8") for path in paths}
    for path in paths:
        path.unlink()
    prior = signal_inputs.run_signal_input_completeness_monitor(
        as_of=date(2024, 4, 22),
        policy_path=fixture["policy_path"],
        output_dir=fixture["output_dir"],
        generated_at=datetime(2024, 4, 23, tzinfo=UTC),
    )
    for path, content in saved.items():
        path.write_text(content, encoding="utf-8")
    return prior


def test_signal_input_completeness_recovery_reruns_restored_monitor(
    tmp_path: Path,
) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    prior = _blocking_prior_and_restore(tmp_path, fixture)

    result = recovery.run_signal_input_completeness_recovery(
        as_of=date(2024, 4, 22),
        monitor_id=prior["monitor_id"],
        policy_path=fixture["policy_path"],
        signal_input_dir=fixture["output_dir"],
        output_dir=tmp_path / "signal_input_completeness_recovery",
        generated_at=datetime(2024, 4, 24, 2, tzinfo=UTC),
    )
    report = result["signal_input_completeness_recovery_report"]

    assert report["recovery_status"] == "SIGNAL_INPUTS_RESTORED"
    assert report["signal_input_status"] == "OK"
    assert report["blocker_list"] == []
    assert report["transition_evidence"]["prior_blocking"] is True
    assert report["transition_evidence"]["chronology_valid"] is True
    assert report["prior_monitor_id"] == prior["monitor_id"]
    assert result["signal_input_completeness_recovery_validation"]["status"] == "PASS"
    assert "signal_input_completeness_recovery_status" in result["reader_brief_section"]
    assert_research_safe(result["manifest"])


def test_signal_input_completeness_recovery_exposes_warning_state(
    tmp_path: Path,
) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    prior = _blocking_prior_and_restore(tmp_path, fixture)
    snapshot = tmp_path / "signal_inputs" / "signal_snapshot_2024-04-22.json"
    payload = json.loads(snapshot.read_text(encoding="utf-8"))
    payload["metadata"]["status"] = "LIMITED"
    snapshot.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    result = recovery.run_signal_input_completeness_recovery(
        as_of=date(2024, 4, 22),
        monitor_id=prior["monitor_id"],
        policy_path=fixture["policy_path"],
        signal_input_dir=fixture["output_dir"],
        output_dir=tmp_path / "signal_input_completeness_recovery",
        generated_at=datetime(2024, 4, 24, 3, tzinfo=UTC),
    )
    report = result["signal_input_completeness_recovery_report"]

    assert report["recovery_status"] == "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS"
    assert report["signal_input_status"] == "WARNING"
    assert "latest_signal_snapshot:warning" in report["warning_list"]
    assert "latest_signal_snapshot:warning_signal_snapshot_status" in report["warning_list"]
    assert report["hard_stop_triggered"] is False
    assert result["signal_input_completeness_recovery_validation"]["status"] == "PASS"


def test_signal_input_completeness_recovery_fail_closes_blockers(
    tmp_path: Path,
) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    prior = _blocking_prior_and_restore(tmp_path, fixture)
    (tmp_path / "signal_inputs" / "features.csv").unlink()
    (tmp_path / "signal_inputs" / "signals.csv").unlink()

    result = recovery.run_signal_input_completeness_recovery(
        as_of=date(2024, 4, 22),
        monitor_id=prior["monitor_id"],
        policy_path=fixture["policy_path"],
        signal_input_dir=fixture["output_dir"],
        output_dir=tmp_path / "signal_input_completeness_recovery",
        generated_at=datetime(2024, 4, 24, 4, tzinfo=UTC),
    )
    report = result["signal_input_completeness_recovery_report"]
    assert report["recovery_status"] == "SIGNAL_INPUTS_STILL_BLOCKED"
    assert report["hard_stop_triggered"] is True
    assert "etf_feature_matrix:blocking" in report["blocker_list"]
    assert "etf_signal_series:blocking" in report["blocker_list"]
    assert report["transition_evidence"]["prior_blocking"] is True
    assert result["signal_input_completeness_recovery_validation"]["status"] == "PASS"


def test_signal_input_completeness_recovery_cli_run_report_and_validate(
    tmp_path: Path,
) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    prior = _blocking_prior_and_restore(tmp_path, fixture)
    output_dir = tmp_path / "signal_input_completeness_recovery_cli"
    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "signal-input-completeness-recovery",
            "run",
            "--as-of",
            "2024-04-22",
            "--monitor-id",
            prior["monitor_id"],
            "--policy-path",
            str(fixture["policy_path"]),
            "--signal-input-dir",
            str(fixture["output_dir"]),
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0
    assert "recovery_status=SIGNAL_INPUTS_RESTORED" in run.output
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
            "signal-input-completeness-recovery",
            "report",
            "--recovery-id",
            recovery_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0
    assert "recovery_status=SIGNAL_INPUTS_RESTORED" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-signal-input-completeness-recovery",
            "--recovery-id",
            recovery_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


def test_signal_input_completeness_recovery_without_prior_blocker_is_not_restored(
    tmp_path: Path,
) -> None:
    fixture = run_signal_input_completeness_fixture(tmp_path, as_of="2024-04-22")
    result = recovery.run_signal_input_completeness_recovery(
        as_of=date(2024, 4, 22),
        policy_path=fixture["policy_path"],
        signal_input_dir=fixture["output_dir"],
        output_dir=tmp_path / "signal_input_completeness_recovery",
        generated_at=datetime(2024, 4, 24, tzinfo=UTC),
    )
    report = result["signal_input_completeness_recovery_report"]

    assert report["recovery_status"] == "NO_RECOVERY_EVIDENCE"
    assert report["hard_stop_triggered"] is True
    assert report["transition_evidence"]["prior_blocking"] is False
    assert result["signal_input_completeness_recovery_validation"]["status"] == "PASS"
