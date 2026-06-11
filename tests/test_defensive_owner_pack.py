from __future__ import annotations

from dynamic_v3_defensive_evidence_helpers import run_owner_pack_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_evidence import (
    validate_defensive_owner_pack_artifact,
)


def test_defensive_owner_pack_exposes_only_manual_research_decisions(tmp_path):
    fixture = run_owner_pack_fixture(tmp_path)
    pack = fixture["defensive_owner_pack"]
    options = pack["owner_decision_options"]
    decisions = {row["decision"] for row in options["options"] if row["recommended"] is True}

    assert options["auto_apply"] is False
    assert options["policy_change_allowed"] is False
    assert options["broker_action_allowed"] is False
    assert options["production_effect"] == "none"
    assert "continue_tracking" in decisions
    assert "request_more_forward_pressure_samples" in decisions

    validation = validate_defensive_owner_pack_artifact(
        pack_id=pack["pack_id"],
        output_dir=fixture["defensive_owner_pack_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0
