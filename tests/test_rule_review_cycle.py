from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from dynamic_v3_confirmation_cycle_helpers import cycle_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    create_rule_owner_decision,
    record_rule_owner_decision,
    rule_review_cycle_report_payload,
    validate_rule_review_cycle_artifact,
)
from ai_trading_system.reports import reader_brief


def test_rule_review_cycle_defaults_to_continue_tracking_without_policy_change(
    tmp_path: Path,
) -> None:
    fixture = cycle_fixture(tmp_path)
    review_cycle = fixture["cycle"]
    matrix = review_cycle["rule_review_decision_matrix"]
    decisions = {row["target_id"]: row for row in matrix["targets"]}

    assert matrix["cycle_recommendation"] == "continue_tracking"
    assert matrix["policy_change_allowed"] is False
    assert decisions["limited_adjustment_vs_no_trade"]["rule_review_decision"] == (
        "CONTINUE_TRACKING"
    )
    assert list(decisions) == ["limited_adjustment_vs_no_trade"]
    assert all(row["policy_change_allowed"] is False for row in matrix["targets"])
    assert "Dynamic Rescue Rule Review Cycle" in review_cycle["reader_brief_section"]

    payload = rule_review_cycle_report_payload(
        cycle_id=review_cycle["cycle_id"],
        output_dir=fixture["cycle_dir"],
    )
    assert payload["cycle_id"] == review_cycle["cycle_id"]

    validation = validate_rule_review_cycle_artifact(
        cycle_id=review_cycle["cycle_id"],
        output_dir=fixture["cycle_dir"],
    )
    assert validation["status"] == "PASS"


def test_reader_brief_surfaces_confirmation_cycle_fields(tmp_path: Path) -> None:
    fixture = cycle_fixture(tmp_path)
    created = create_rule_owner_decision(
        cycle_id=fixture["cycle"]["cycle_id"],
        cycle_dir=fixture["cycle_dir"],
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 6, 10, 4, tzinfo=UTC),
    )
    record_rule_owner_decision(
        decision_id=created["decision_id"],
        decision="continue_tracking",
        notes="Continue forward evidence accumulation.",
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 6, 10, 5, tzinfo=UTC),
    )

    report_index = {
        "reports": [
            _report(
                "etf_dynamic_v3_forward_confirmation_plan",
                fixture["confirmation_plan_root"]
                / fixture["confirmation_plan_id"]
                / "confirmation_plan_manifest.json",
            ),
            _report(
                "etf_dynamic_v3_confirmation_registry",
                fixture["registry_yaml_path"],
            ),
            _report(
                "etf_dynamic_v3_confirmation_progress",
                fixture["progress"]["progress_dir"] / "confirmation_progress_manifest.json",
            ),
            _report(
                "etf_dynamic_v3_confirmation_evaluation",
                fixture["evaluation"]["evaluation_dir"] / "confirmation_evaluation_manifest.json",
            ),
            _report(
                "etf_dynamic_v3_rule_review_cycle",
                fixture["cycle"]["cycle_dir"] / "rule_review_cycle_manifest.json",
            ),
            _report(
                "etf_dynamic_v3_rule_owner_decision",
                fixture["journal_path"].parent / "rule_owner_decision_report.md",
            ),
        ]
    }

    summary = reader_brief._etf_dynamic_v3_sim_review_summary(report_index)

    assert summary["availability"] == "PARTIAL"
    assert summary["status"] == "AVAILABLE"
    assert summary["confirmation_registry_id"] == fixture["registry"]["registry_id"]
    assert summary["confirmation_progress_id"] == fixture["progress"]["progress_id"]
    assert summary["confirmation_evaluation_id"] == fixture["evaluation"]["evaluation_id"]
    assert summary["rule_review_cycle_id"] == fixture["cycle"]["cycle_id"]
    assert summary["rule_review_cycle_recommendation"] == "continue_tracking"
    assert summary["confirmation_ready_for_evaluation_count"] == 0
    assert summary["confirmation_insufficient_events_count"] == 1
    assert summary["confirmation_success_count"] == 0
    assert summary["confirmation_not_ready_count"] == 1
    assert summary["rule_review_policy_change_allowed"] is False
    assert summary["rule_owner_decision_id"] == created["decision_id"]
    assert summary["rule_owner_decision"] == "continue_tracking"


def _report(report_id: str, path: Path) -> dict[str, str]:
    return {"report_id": report_id, "latest_artifact_path": str(path)}
