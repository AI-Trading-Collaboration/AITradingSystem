from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import (
    report_index_for_review_fixture,
    run_review_fixture,
    run_selection_review_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_refined_method_proposal_preserves_research_only_boundary(tmp_path) -> None:
    artifacts = _run_refined_fixture(tmp_path)
    proposal = artifacts["proposal"]
    decision = proposal["refined_method_decision"]
    next_methods = proposal["proposed_next_methods"]["methods"]

    assert decision["current_method"] == "limited_adjustment"
    assert decision["current_hardening_status"] == "REVIEW_REQUIRED"
    assert decision["recommended_next_step"] in {
        "IMPLEMENT_RISK_CAPPED_RESEARCH_METHOD",
        "IMPLEMENT_REGIME_GATED_RESEARCH_METHOD",
        "CONTINUE_OBSERVATION",
        "REPAIR_DATA_WARNINGS_FIRST",
        "DEFER",
    }
    assert {row["method"] for row in next_methods} >= {
        "risk_capped_limited_adjustment",
        "regime_gated_limited_adjustment",
    }
    assert decision["auto_apply"] is False
    assert decision["research_target_only"] is True
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"

    validation = system_target.validate_refined_method_proposal_artifact(
        proposal_id=proposal["proposal_id"],
        output_dir=tmp_path / "refined_method_proposal",
    )
    assert validation["status"] == "PASS"


def test_reader_brief_displays_refined_method_proposal(tmp_path) -> None:
    review_fixture = run_review_fixture(tmp_path / "review")
    artifacts = _run_refined_fixture(tmp_path / "selection")
    proposal = artifacts["proposal"]

    report_index = report_index_for_review_fixture(review_fixture)
    report_index["reports"].append(
        {
            "report_id": "etf_dynamic_v3_system_target_selection_review",
            "latest_artifact_path": str(
                artifacts["selection"]["selection_review_dir"]
                / "system_target_selection_manifest.json"
            ),
        }
    )
    report_index["reports"].append(
        {
            "report_id": "etf_dynamic_v3_research_method_hardening",
            "latest_artifact_path": str(
                artifacts["hardening"]["hardening_dir"]
                / "research_method_hardening_manifest.json"
            ),
        }
    )
    report_index["reports"].append(
        {
            "report_id": "etf_dynamic_v3_refined_method_proposal",
            "latest_artifact_path": str(
                proposal["proposal_dir"] / "refined_method_proposal_manifest.json"
            ),
        }
    )

    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["refined_proposal_id"] == proposal["proposal_id"]
    assert (
        summary["refined_recommended_next_step"]
        == proposal["refined_method_decision"]["recommended_next_step"]
    )
    assert "risk_capped_limited_adjustment" in summary["refined_proposed_next_methods"]
    assert summary["broker_action_allowed"] is False


def _run_refined_fixture(tmp_path):
    fixture = run_selection_review_fixture(tmp_path)
    selection = fixture["selection"]
    backfill_id = fixture["backfill"]["backfill_id"]
    attribution = system_target.run_selection_attribution(
        selection_review_id=selection["selection_review_id"],
        selection_review_dir=tmp_path / "system_target_selection_review",
        output_dir=tmp_path / "selection_attribution",
        generated_at=datetime(2026, 1, 7, 5, tzinfo=UTC),
    )
    long_risk = system_target.run_limited_long_risk_review(
        backfill_id=backfill_id,
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "limited_long_risk",
        generated_at=datetime(2026, 1, 7, 6, tzinfo=UTC),
    )
    consistency = system_target.run_limited_consistency_check(
        backfill_id=backfill_id,
        backfill_dir=tmp_path / "paper_shadow_backfill",
        rolling_eval_dir=tmp_path / "paper_shadow_rolling_eval",
        regime_review_dir=tmp_path / "paper_shadow_regime_review",
        stability_dir=tmp_path / "paper_shadow_stability",
        output_dir=tmp_path / "limited_consistency",
        generated_at=datetime(2026, 1, 7, 7, tzinfo=UTC),
    )
    impact = system_target.run_data_warning_impact_review(
        backfill_id=backfill_id,
        selection_review_id=selection["selection_review_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        selection_review_dir=tmp_path / "system_target_selection_review",
        output_dir=tmp_path / "data_warning_impact",
        generated_at=datetime(2026, 1, 7, 8, tzinfo=UTC),
    )
    hardening = system_target.run_research_method_hardening_pack(
        selection_attribution_id=attribution["attribution_id"],
        risk_review_id=long_risk["risk_review_id"],
        consistency_id=consistency["consistency_id"],
        data_warning_impact_id=impact["impact_id"],
        selection_attribution_dir=tmp_path / "selection_attribution",
        risk_review_dir=tmp_path / "limited_long_risk",
        consistency_dir=tmp_path / "limited_consistency",
        data_warning_impact_dir=tmp_path / "data_warning_impact",
        output_dir=tmp_path / "research_method_hardening",
        generated_at=datetime(2026, 1, 7, 9, tzinfo=UTC),
    )
    instability = system_target.run_limited_instability_diagnosis(
        backfill_id=backfill_id,
        consistency_id=consistency["consistency_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        consistency_dir=tmp_path / "limited_consistency",
        rolling_eval_dir=tmp_path / "paper_shadow_rolling_eval",
        output_dir=tmp_path / "limited_instability",
        generated_at=datetime(2026, 1, 7, 10, tzinfo=UTC),
    )
    risk = system_target.run_limited_risk_attribution(
        backfill_id=backfill_id,
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "limited_risk_attribution",
        generated_at=datetime(2026, 1, 7, 11, tzinfo=UTC),
    )
    repair = system_target.run_data_warning_repair_plan(
        impact_id=impact["impact_id"],
        data_warning_impact_dir=tmp_path / "data_warning_impact",
        output_dir=tmp_path / "data_warning_repair_plan",
        generated_at=datetime(2026, 1, 7, 12, tzinfo=UTC),
    )
    alt_review = system_target.run_alternative_method_review(
        backfill_id=backfill_id,
        risk_attribution_id=risk["risk_attribution_id"],
        instability_id=instability["instability_id"],
        backfill_dir=tmp_path / "paper_shadow_backfill",
        risk_attribution_dir=tmp_path / "limited_risk_attribution",
        instability_dir=tmp_path / "limited_instability",
        output_dir=tmp_path / "alternative_method_review",
        generated_at=datetime(2026, 1, 7, 13, tzinfo=UTC),
    )
    proposal = system_target.run_refined_method_proposal(
        instability_id=instability["instability_id"],
        risk_attribution_id=risk["risk_attribution_id"],
        repair_plan_id=repair["repair_plan_id"],
        alt_review_id=alt_review["alt_review_id"],
        instability_dir=tmp_path / "limited_instability",
        risk_attribution_dir=tmp_path / "limited_risk_attribution",
        repair_plan_dir=tmp_path / "data_warning_repair_plan",
        alt_review_dir=tmp_path / "alternative_method_review",
        output_dir=tmp_path / "refined_method_proposal",
        generated_at=datetime(2026, 1, 7, 14, tzinfo=UTC),
    )
    return {
        **fixture,
        "selection": selection,
        "hardening": hardening,
        "instability": instability,
        "risk": risk,
        "repair": repair,
        "alt_review": alt_review,
        "proposal": proposal,
    }
