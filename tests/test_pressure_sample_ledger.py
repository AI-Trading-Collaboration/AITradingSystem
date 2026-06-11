from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_pressure_sample_ledger_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
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
