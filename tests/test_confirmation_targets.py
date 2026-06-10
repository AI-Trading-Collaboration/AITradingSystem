from __future__ import annotations

from pathlib import Path

from dynamic_v3_confirmation_cycle_helpers import register_targets_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    list_confirmation_targets,
    validate_confirmation_targets_artifact,
)


def test_confirmation_targets_registers_plan_targets_with_safety(tmp_path: Path) -> None:
    fixture = register_targets_fixture(tmp_path)
    registry = fixture["registry"]
    targets = registry["targets"]

    assert registry["manifest"]["targets_total"] == 3
    assert registry["manifest"]["active_target_count"] == 2
    assert registry["manifest"]["watch_only_target_count"] == 1
    assert {row["target_id"] for row in targets} == {
        "limited_adjustment_vs_no_trade",
        "defensive_limited_adjustment_drawdown",
        "consensus_target_risk",
    }
    assert all(row["auto_apply"] is False for row in targets)
    assert all(row["owner_approval_required"] is True for row in targets)
    assert fixture["registry_yaml_path"].exists()

    listing = list_confirmation_targets(
        registry_id=registry["registry_id"],
        output_dir=fixture["registry_dir"],
    )
    assert listing["active_target_count"] == 2
    assert listing["watch_only_target_count"] == 1

    validation = validate_confirmation_targets_artifact(
        registry_id=registry["registry_id"],
        output_dir=fixture["registry_dir"],
    )
    assert validation["status"] == "PASS"
