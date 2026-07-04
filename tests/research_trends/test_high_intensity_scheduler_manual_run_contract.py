from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    build_high_intensity_scheduler_manual_run_contract,
)


def test_scheduler_manual_run_contract_only_allows_dry_run_modes() -> None:
    contract = build_high_intensity_scheduler_manual_run_contract()

    assert contract["allowed_modes"] == ["dry_run", "validate_only"]
    assert "live" in contract["blocked_modes"]
    assert "paper_shadow" in contract["blocked_modes"]
    assert "production" in contract["blocked_modes"]
    assert "fail_closed_safety_gate" in contract["required_safety_checks"]
    assert "no_target_weight" in contract["required_safety_checks"]
    assert "no_rebalance_instruction" in contract["required_safety_checks"]
    assert "no_broker_action" in contract["required_safety_checks"]
