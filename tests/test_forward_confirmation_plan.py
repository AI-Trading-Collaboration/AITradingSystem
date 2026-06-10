from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_forward_confirmation_plan_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    validate_forward_confirmation_plan_artifact,
)


def test_forward_confirmation_plan_defines_targets_and_failure_conditions(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_forward_confirmation_plan_fixture(tmp_path, monkeypatch)
    plan = fixture["confirmation_plan"]
    targets = plan["confirmation_targets"]["targets"]
    failures = plan["failure_conditions"]["failure_conditions"]
    triggers = plan["trigger_conditions"]

    assert {row["target_id"] for row in targets} == {
        "limited_adjustment_vs_no_trade",
        "defensive_limited_adjustment_drawdown",
        "consensus_target_risk",
    }
    assert triggers["calibration_ready_conditions"]
    assert triggers["calibration_not_ready_conditions"]
    assert {row["target"] for row in failures} >= {
        "limited_adjustment_vs_no_trade",
        "defensive_limited_adjustment_drawdown",
        "consensus_target_risk",
    }
    assert plan["manifest"]["auto_policy_apply"] is False
    assert "Dynamic Rescue Forward Confirmation Plan" in plan["reader_brief_section"]

    validation = validate_forward_confirmation_plan_artifact(
        confirmation_plan_id=plan["confirmation_plan_id"],
        output_dir=fixture["confirmation_plan_dir"],
    )
    assert validation["status"] == "PASS"
