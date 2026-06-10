from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from dynamic_v3_confirmation_cycle_helpers import cycle_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    create_rule_owner_decision,
    list_rule_owner_decisions,
    record_rule_owner_decision,
    rule_owner_decision_report_payload,
    validate_rule_owner_decision_artifact,
)


def test_rule_owner_decision_records_manual_choice_without_config_mutation(
    tmp_path: Path,
) -> None:
    fixture = cycle_fixture(tmp_path)
    before_config = fixture["position_config_path"].read_text(encoding="utf-8")
    created = create_rule_owner_decision(
        cycle_id=fixture["cycle"]["cycle_id"],
        cycle_dir=fixture["cycle_dir"],
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 6, 10, 4, tzinfo=UTC),
    )
    decision_id = created["decision_id"]
    assert created["record"]["owner_decision"] == "pending"
    assert created["record"]["auto_apply"] is False
    assert created["record"]["broker_action_allowed"] is False
    assert created["record"]["production_effect"] == "none"

    recorded = record_rule_owner_decision(
        decision_id=decision_id,
        decision="continue_tracking",
        notes="Continue forward evidence accumulation.",
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 6, 10, 5, tzinfo=UTC),
    )
    assert recorded["record"]["owner_decision"] == "continue_tracking"
    assert recorded["record"]["auto_apply"] is False
    assert recorded["record"]["policy_change_allowed"] is False
    assert fixture["position_config_path"].read_text(encoding="utf-8") == before_config

    listing = list_rule_owner_decisions(journal_path=fixture["journal_path"])
    assert listing["decision_count"] == 1
    assert listing["pending_count"] == 0

    report = rule_owner_decision_report_payload(
        decision_id=decision_id,
        journal_path=fixture["journal_path"],
    )
    assert report["owner_decision"] == "continue_tracking"

    validation = validate_rule_owner_decision_artifact(
        decision_id=decision_id,
        journal_path=fixture["journal_path"],
    )
    assert validation["status"] == "PASS"
