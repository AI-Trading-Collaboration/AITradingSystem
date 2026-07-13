from __future__ import annotations

from pathlib import Path

from dynamic_v3_defensive_evidence_helpers import (
    run_pressure_capture_force_fixture,
    run_pressure_capture_skip_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_forward_pressure import (
    validate_pressure_capture_artifact,
)


def test_pressure_capture_skips_without_trigger_and_runs_when_forced(tmp_path):
    skipped = run_pressure_capture_skip_fixture(tmp_path / "skipped")
    forced = run_pressure_capture_force_fixture(tmp_path / "forced")

    skipped_capture = skipped["pressure_capture"]
    forced_capture = forced["pressure_capture"]

    assert skipped_capture["manifest"]["status"] == "SKIPPED"
    assert {step["status"] for step in skipped_capture["pressure_capture_steps"]["steps"]} == {
        "SKIPPED"
    }
    assert forced_capture["manifest"]["status"] == "PASS"
    assert forced_capture["pressure_capture_steps"]["manual_force"] is True
    forced_steps = {
        step["step"]: step["status"] for step in forced_capture["pressure_capture_steps"]["steps"]
    }
    assert forced_steps == {
        "pressure-regime-tag": "PASS",
        "pressure-outcome-backfill": "PASS_WITH_WARNINGS",
        "defensive-pressure-compare": "PASS",
    }

    for fixture, capture in ((skipped, skipped_capture), (forced, forced_capture)):
        validation = validate_pressure_capture_artifact(
            capture_id=capture["capture_id"],
            output_dir=fixture["pressure_capture_dir"],
        )
        assert validation["status"] == "PASS"
        assert validation["failed_check_count"] == 0


def test_pressure_capture_validation_rejects_recursive_source_and_output_drift(tmp_path):
    fixture = run_pressure_capture_skip_fixture(tmp_path)
    capture = fixture["pressure_capture"]
    trigger = fixture["pressure_trigger"]
    with Path(trigger["trigger_dir"], "pressure_trigger_report.md").open(
        "a", encoding="utf-8"
    ) as handle:
        handle.write("\nunauthorized drift\n")
    Path(capture["capture_dir"], "pressure_capture_steps.json").write_text("{}\n", encoding="utf-8")

    validation = validate_pressure_capture_artifact(
        capture_id=capture["capture_id"],
        output_dir=fixture["pressure_capture_dir"],
    )

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1
