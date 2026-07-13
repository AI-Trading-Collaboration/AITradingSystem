from __future__ import annotations

import json
from pathlib import Path

from dynamic_v3_defensive_evidence_helpers import run_pressure_trigger_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_forward_pressure import (
    validate_pressure_trigger_artifact,
)


def test_pressure_trigger_scan_distinguishes_quiet_and_pressure_windows(tmp_path):
    quiet = run_pressure_trigger_fixture(tmp_path / "quiet", triggered=False)
    stressed = run_pressure_trigger_fixture(tmp_path / "stressed", triggered=True)

    quiet_trigger = quiet["pressure_trigger"]
    stressed_trigger = stressed["pressure_trigger"]

    assert quiet_trigger["trigger_metrics"]["trigger_status"] == "NO_TRIGGER"
    assert quiet_trigger["triggered_actions"]["event_driven_capture_required"] is False
    assert stressed_trigger["trigger_metrics"]["trigger_status"] == "PRESSURE_TRIGGERED"
    assert stressed_trigger["triggered_actions"]["event_driven_capture_required"] is True
    assert stressed_trigger["manifest"]["broker_action_allowed"] is False

    for fixture, trigger in ((quiet, quiet_trigger), (stressed, stressed_trigger)):
        validation = validate_pressure_trigger_artifact(
            trigger_id=trigger["trigger_id"],
            output_dir=fixture["pressure_trigger_dir"],
        )
        assert validation["status"] == "PASS"
        assert validation["failed_check_count"] == 0


def test_pressure_trigger_validation_rejects_cache_and_derived_view_drift(tmp_path):
    fixture = run_pressure_trigger_fixture(tmp_path, triggered=True)
    trigger = fixture["pressure_trigger"]
    artifact_dir = Path(trigger["trigger_dir"])
    snapshot = json.loads(
        (artifact_dir / "pressure_trigger_input_snapshot.json").read_text(encoding="utf-8")
    )
    prices = next(row for row in snapshot["cache_bindings"] if row["source_kind"] == "etf_prices")
    with Path(prices["path"]).open("a", encoding="utf-8") as handle:
        handle.write("2026-06-22,QQQ,1\n")
    (artifact_dir / "trigger_metrics.json").write_text("{}\n", encoding="utf-8")

    validation = validate_pressure_trigger_artifact(
        trigger_id=trigger["trigger_id"],
        output_dir=fixture["pressure_trigger_dir"],
    )

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1
