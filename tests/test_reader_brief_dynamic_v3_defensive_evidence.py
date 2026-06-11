from __future__ import annotations

from pathlib import Path

from dynamic_v3_defensive_evidence_helpers import (
    run_capture_plan_fixture,
    run_owner_pack_fixture,
    run_pressure_capture_skip_fixture,
    run_weekly_defensive_evidence_fixture,
)

from ai_trading_system.reports import reader_brief


def test_reader_brief_summarizes_defensive_hypothesis_and_forward_evidence(tmp_path):
    owner = run_owner_pack_fixture(tmp_path / "owner")
    plan = run_capture_plan_fixture(tmp_path / "plan")
    capture = run_pressure_capture_skip_fixture(tmp_path / "capture")
    weekly = run_weekly_defensive_evidence_fixture(tmp_path / "weekly")

    deep_dive = owner["defensive_hypothesis_deep_dive"]
    label = owner["defensive_label_review"]
    failure = owner["defensive_failure_study"]
    note = owner["defensive_research_note"]
    pack = owner["defensive_owner_pack"]
    trigger = capture["pressure_trigger"]
    skipped_capture = capture["pressure_capture"]
    ledger = weekly["pressure_sample_ledger"]
    weekly_defensive = weekly["weekly_defensive"]
    report_index = {
        "reports": [
            _report(
                "etf_dynamic_v3_defensive_hypothesis_deep_dive",
                deep_dive["manifest"]["deep_dive_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_defensive_label_review",
                label["manifest"]["label_review_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_defensive_failure_study",
                failure["manifest"]["failure_study_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_defensive_research_note",
                note["manifest"]["defensive_research_note_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_defensive_owner_pack",
                pack["manifest"]["defensive_owner_pack_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_forward_pressure_capture",
                plan["capture_plan"]["manifest"]["capture_plan_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_pressure_trigger",
                trigger["manifest"]["pressure_trigger_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_pressure_capture",
                skipped_capture["manifest"]["pressure_capture_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_pressure_sample_ledger",
                ledger["manifest"]["pressure_sample_ledger_manifest_path"],
            ),
            _report(
                "etf_dynamic_v3_weekly_defensive_evidence",
                weekly_defensive["manifest"]["weekly_defensive_manifest_path"],
            ),
        ]
    }

    summary = reader_brief._etf_dynamic_v3_sim_review_summary(report_index)

    assert summary["availability"] == "PARTIAL"
    assert summary["status"] == "PASS"
    assert summary["defensive_hypothesis_deep_dive_id"] == deep_dive["deep_dive_id"]
    assert summary["defensive_hypothesis_supporting_count"] > 0
    assert summary["defensive_hypothesis_contradicting_count"] > 0
    assert summary["defensive_hypothesis_can_support_rule_approval"] is False
    assert summary["defensive_label_status"] == "POTENTIALLY_MISLEADING"
    assert summary["defensive_recommended_label"] == "risk_aware_limited_adjustment"
    assert summary["defensive_failure_top_pattern"] == (
        "increased_risk_exposure_during_drawdown"
    )
    assert summary["defensive_hypothesis_current_status"] == "RESEARCH_ONLY"
    assert summary["defensive_hypothesis_forward_support"] == "NONE"
    assert summary["defensive_owner_continue_tracking_recommended"] is True
    assert summary["forward_pressure_event_trigger_count"] == 4
    assert summary["pressure_trigger_status"] == "NO_TRIGGER"
    assert summary["pressure_capture_required"] is False
    assert summary["pressure_capture_status"] == "SKIPPED"
    assert summary["pressure_forward_samples"] == 0
    assert summary["pressure_simulation_samples"] > 0
    assert summary["weekly_defensive_rule_status"] == "RESEARCH_ONLY"
    assert summary["weekly_defensive_recommendation"] == "continue_tracking"
    assert summary["auto_apply"] is False
    assert summary["production_effect"] == "none"
    assert summary["broker_action"] == "none"


def _report(report_id: str, path: str | Path) -> dict[str, str]:
    return {"report_id": report_id, "latest_artifact_path": str(path)}
