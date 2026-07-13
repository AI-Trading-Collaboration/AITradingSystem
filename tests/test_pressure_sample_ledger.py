from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_pressure_sample_ledger_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_forward_pressure import (
    validate_pressure_sample_ledger_artifact,
)


def test_pressure_sample_ledger_tracks_simulation_without_rule_approval(tmp_path):
    fixture = run_pressure_sample_ledger_fixture(tmp_path)
    ledger = fixture["pressure_sample_ledger"]
    summary = ledger["pressure_sample_summary"]
    simulation_samples = [
        row for row in ledger["pressure_samples"] if row["source_mode"] == "BACKTEST_SIMULATION"
    ]

    assert summary["simulation_samples"] > 0
    assert summary["forward_samples"] == 0
    assert summary["progress_to_requirement"] == 0.0
    assert simulation_samples
    assert all(row["can_support_rule_approval"] is False for row in simulation_samples)

    validation = validate_pressure_sample_ledger_artifact(
        ledger_id=ledger["ledger_id"],
        output_dir=fixture["pressure_sample_ledger_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0

    inventory_path = (
        fixture["pressure_backfill_dir"]
        / fixture["pressure_backfill"]["pressure_backfill_id"]
        / "pressure_outcome_inventory.jsonl"
    )
    inventory_count = sum(
        1 for line in inventory_path.read_text(encoding="utf-8").splitlines() if line
    )
    distinct_keys = {
        (row["source_mode"], row["source_event_id"])
        for row in ledger["pressure_samples"]
    }
    assert summary["total_samples"] == len(distinct_keys)
    assert sum(row["window_count"] for row in ledger["pressure_samples"]) == inventory_count
    assert summary["sample_unit"] == "distinct_source_mode_plus_source_event_id"


def test_pressure_sample_ledger_validation_rejects_backfill_drift(tmp_path):
    fixture = run_pressure_sample_ledger_fixture(tmp_path)
    ledger = fixture["pressure_sample_ledger"]
    backfill_dir = (
        fixture["pressure_backfill_dir"] / fixture["pressure_backfill"]["pressure_backfill_id"]
    )
    with (backfill_dir / "pressure_backfill_report.md").open("a", encoding="utf-8") as handle:
        handle.write("\nunauthorized drift\n")

    validation = validate_pressure_sample_ledger_artifact(
        ledger_id=ledger["ledger_id"],
        output_dir=fixture["pressure_sample_ledger_dir"],
    )

    assert validation["status"] == "FAIL"
    assert validation["failed_check_count"] >= 1
