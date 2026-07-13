from __future__ import annotations

import json

from dynamic_v3_defensive_evidence_helpers import run_defensive_deep_dive_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_research import (
    validate_defensive_hypothesis_deep_dive_artifact,
)


def test_defensive_hypothesis_deep_dive_splits_supporting_and_contradicting_cases(
    tmp_path,
):
    fixture = run_defensive_deep_dive_fixture(tmp_path)
    deep_dive = fixture["defensive_hypothesis_deep_dive"]
    manifest = deep_dive["manifest"]

    assert manifest["supporting_case_count"] > 0
    assert manifest["contradicting_case_count"] > 0
    assert manifest["source_mode_counts"]["BACKTEST_SIMULATION"] > 0
    assert manifest["can_support_rule_approval"] is False
    assert manifest["production_effect"] == "none"
    assert all(row["can_support_rule_approval"] is False for row in deep_dive["supporting_cases"])
    assert all(
        row["can_support_rule_approval"] is False for row in deep_dive["contradicting_cases"]
    )

    validation = validate_defensive_hypothesis_deep_dive_artifact(
        deep_dive_id=deep_dive["deep_dive_id"],
        output_dir=fixture["defensive_hypothesis_deep_dive_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0


def test_defensive_hypothesis_deep_dive_preserves_missing_exposure_as_null(tmp_path):
    fixture = run_defensive_deep_dive_fixture(tmp_path)
    attribution = fixture["defensive_hypothesis_deep_dive"]["exposure_change_attribution"]

    assert attribution["avg_risk_asset_exposure_delta"] is None
    assert attribution["exposure_available_count"] == 0
    assert attribution["exposure_missing_count"] > 0
    assert attribution["attribution_status"] == "INSUFFICIENT_SOURCE_EXPOSURE_FIELDS"


def test_defensive_hypothesis_deep_dive_validator_rejects_output_and_source_drift(tmp_path):
    fixture = run_defensive_deep_dive_fixture(tmp_path)
    deep = fixture["defensive_hypothesis_deep_dive"]
    deep_root = fixture["defensive_hypothesis_deep_dive_dir"]
    report = deep["deep_dive_dir"] / "defensive_hypothesis_deep_dive_report.md"
    original_report = report.read_text(encoding="utf-8")
    report.write_text(original_report + "tamper\n", encoding="utf-8")

    validation = validate_defensive_hypothesis_deep_dive_artifact(
        deep_dive_id=deep["deep_dive_id"], output_dir=deep_root
    )
    assert validation["status"] == "FAIL"

    report.write_text(original_report, encoding="utf-8")
    inventory = (
        fixture["pressure_backfill_dir"]
        / fixture["pressure_backfill"]["pressure_backfill_id"]
        / "pressure_outcome_inventory.jsonl"
    )
    rows = [json.loads(line) for line in inventory.read_text(encoding="utf-8").splitlines()]
    rows[0]["source_event_id"] = "tampered-event"
    inventory.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8"
    )
    validation = validate_defensive_hypothesis_deep_dive_artifact(
        deep_dive_id=deep["deep_dive_id"], output_dir=deep_root
    )
    assert validation["status"] == "FAIL"
