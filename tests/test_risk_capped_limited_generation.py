from __future__ import annotations

from dynamic_v3_system_target_helpers import build_model_target_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_risk_capped_generation_preserves_weight_sum_and_safety(tmp_path) -> None:
    fixture = build_model_target_fixture(tmp_path)

    result = system_target.generate_risk_capped_limited_target(
        target_id=fixture["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "risk_capped_limited",
        regime_context="sideways_choppy",
    )

    row = result["risk_capped_target_weights"][0]
    weights = row["capped_weights"]
    assert abs(sum(weights.values()) - 1.0) <= 0.000001
    assert row["research_target_only"] is True
    assert row["not_official_target_weights"] is True
    assert row["broker_action_allowed"] is False
    assert result["cap_events"]
    assert any(event["cap_type"] == "max_semiconductor_weight" for event in result["cap_events"])
    assert any(event["destination"] == "CASH" for event in result["reallocation_events"])

    validation = system_target.validate_risk_capped_limited_artifact(
        risk_capped_id=result["risk_capped_id"],
        output_dir=tmp_path / "risk_capped_limited",
    )
    assert validation["status"] == "PASS"


def test_risk_capped_generation_applies_tech_drawdown_delta_cap(tmp_path) -> None:
    fixture = build_model_target_fixture(tmp_path)

    result = system_target.generate_risk_capped_limited_target(
        target_id=fixture["target_id"],
        model_target_dir=tmp_path / "model_target",
        output_dir=tmp_path / "risk_capped_limited",
        regime_context="tech_drawdown",
    )

    cap_types = {event["cap_type"] for event in result["cap_events"]}
    assert "max_total_risk_asset_increase_per_rebalance" in cap_types
    assert result["cap_reason_summary"]["cap_status"] == "PASS"
