from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from dynamic_v3_defensive_evidence_helpers import (
    run_label_review_fixture,
    run_research_note_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_research import (
    DynamicV3DefensiveResearchError,
    run_defensive_failure_study,
    run_defensive_hypothesis_deep_dive,
    run_defensive_research_note,
    validate_defensive_research_note_artifact,
)


def test_defensive_research_note_keeps_hypothesis_research_only(tmp_path):
    fixture = run_research_note_fixture(tmp_path)
    note = fixture["defensive_research_note"]
    summary = note["defensive_hypothesis_summary"]

    assert summary["current_status"] == "RESEARCH_ONLY"
    assert summary["simulation_support"] == "PARTIAL"
    assert summary["forward_support"] == "NONE"
    assert summary["can_support_rule_approval"] is False
    assert summary["policy_change_allowed"] is False
    assert "Dynamic Rescue Defensive Hypothesis Review" in note["reader_brief_section"]

    validation = validate_defensive_research_note_artifact(
        note_id=note["note_id"],
        output_dir=fixture["defensive_research_note_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0


def test_defensive_research_note_rejects_cross_deep_dive_lineage(tmp_path):
    label_fixture = run_label_review_fixture(tmp_path / "label")
    source_time = datetime.fromisoformat(
        label_fixture["defensive_hypothesis_deep_dive"]["input_snapshot"]["generated_at"]
    )
    later = source_time + timedelta(seconds=1)
    second_deep_root = tmp_path / "second-deep"
    second_deep = run_defensive_hypothesis_deep_dive(
        pressure_backfill_id=label_fixture["pressure_backfill"]["pressure_backfill_id"],
        comparison_id=label_fixture["defensive_pressure_compare"]["comparison_id"],
        backfill_dir=label_fixture["pressure_backfill_dir"],
        comparison_dir=label_fixture["defensive_pressure_compare_dir"],
        output_dir=second_deep_root,
        generated_at=later,
    )
    second_failure_root = tmp_path / "second-failure"
    second_failure = run_defensive_failure_study(
        deep_dive_id=second_deep["deep_dive_id"],
        deep_dive_dir=second_deep_root,
        output_dir=second_failure_root,
        generated_at=later,
    )

    with pytest.raises(DynamicV3DefensiveResearchError, match="cross-lineage"):
        run_defensive_research_note(
            deep_dive_id=label_fixture["defensive_hypothesis_deep_dive"]["deep_dive_id"],
            label_review_id=label_fixture["defensive_label_review"]["label_review_id"],
            failure_study_id=second_failure["failure_study_id"],
            deep_dive_dir=label_fixture["defensive_hypothesis_deep_dive_dir"],
            label_review_dir=label_fixture["defensive_label_review_dir"],
            failure_study_dir=second_failure_root,
            output_dir=tmp_path / "cross-lineage-note",
            generated_at=later,
        )
