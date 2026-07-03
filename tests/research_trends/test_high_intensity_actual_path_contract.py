from __future__ import annotations

from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    build_high_intensity_actual_path_outcome_contract,
    build_high_intensity_forward_observe_evidence_contract,
)


def test_high_intensity_actual_path_contract_blocks_future_outcome_leakage() -> None:
    evidence = build_high_intensity_forward_observe_evidence_contract()
    outcome = build_high_intensity_actual_path_outcome_contract()

    assert evidence["horizon_1d_required"] is True
    assert evidence["horizon_5d_required"] is True
    assert evidence["horizon_10d_required"] is True
    assert evidence["horizon_20d_required"] is True
    assert outcome["allowed_horizons"] == ["1d", "5d", "10d", "20d"]
    template = outcome["outcome_record_template"]
    assert "forward_return" in template
    assert "forward_max_drawdown" in template
    assert "false_warning_candidate" in template
    assert "missed_stress_candidate" in template
    assert "missed_upside_candidate" in template
    assert "future outcomes must not modify trigger" in outcome["future_outcome_use_policy"]
    assert outcome["promotion_allowed"] is False
    assert outcome["paper_shadow_allowed"] is False
    assert outcome["production_allowed"] is False
    assert outcome["broker_action"] == "none"
