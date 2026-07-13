from __future__ import annotations

from datetime import UTC, datetime

from dynamic_v3_system_target_helpers import (
    report_index_for_review_fixture,
    run_review_fixture,
    run_selection_review_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.reports import reader_brief


def test_research_method_hardening_pack_preserves_research_only_boundary(tmp_path) -> None:
    fixture = run_selection_review_fixture(tmp_path)
    selection = fixture["selection"]
    backfill_id = fixture["backfill"]["backfill_id"]

    attribution = system_target.run_selection_attribution(
        selection_review_id=selection["selection_review_id"],
        selection_review_dir=tmp_path / "system_target_selection_review",
        output_dir=tmp_path / "selection_attribution",
        generated_at=datetime(2026, 1, 7, 5, tzinfo=UTC),
    )
    risk = system_target.run_limited_long_risk_review(
        backfill_id=backfill_id,
        backfill_dir=tmp_path / "paper_shadow_backfill",
        output_dir=tmp_path / "limited_long_risk",
        generated_at=datetime(2026, 1, 7, 6, tzinfo=UTC),
    )
    consistency = system_target.run_limited_consistency_check(
        backfill_id=backfill_id,
        rolling_eval_id=fixture["rolling"]["rolling_eval_id"],
        regime_review_id=fixture["regime"]["regime_review_id"],
        stability_id=fixture["stability"]["stability_id"],
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
        risk_review_id=risk["risk_review_id"],
        consistency_id=consistency["consistency_id"],
        data_warning_impact_id=impact["impact_id"],
        selection_attribution_dir=tmp_path / "selection_attribution",
        risk_review_dir=tmp_path / "limited_long_risk",
        consistency_dir=tmp_path / "limited_consistency",
        data_warning_impact_dir=tmp_path / "data_warning_impact",
        output_dir=tmp_path / "research_method_hardening",
        generated_at=datetime(2026, 1, 7, 9, tzinfo=UTC),
    )

    decision = hardening["hardening_decision"]

    assert decision["candidate_method"] == "limited_adjustment"
    assert decision["hardening_decision"] in {
        "HARDEN_AS_PRIMARY_RESEARCH",
        "CONTINUE_OBSERVATION",
        "REVIEW_REQUIRED",
        "REJECT",
    }
    assert decision["research_target_only"] is True
    assert decision["not_official_target_weights"] is True
    assert decision["broker_action_allowed"] is False
    assert decision["production_effect"] == "none"
    assert decision["requires_forward_confirmation"] is True
    assert decision["workflow_pass_is_not_investment_conclusion"] is True
    assert hardening["manifest"]["input_snapshot_schema"] == (
        "research_method_hardening_input_snapshot.v2"
    )

    validation = system_target.validate_research_method_hardening_artifact(
        hardening_id=hardening["hardening_id"],
        output_dir=tmp_path / "research_method_hardening",
    )
    assert validation["status"] == "PASS"

    attribution_report = attribution["attribution_dir"] / "selection_attribution_report.md"
    attribution_report.write_text("tampered\n", encoding="utf-8")
    assert system_target.validate_research_method_hardening_artifact(
        hardening_id=hardening["hardening_id"],
        output_dir=tmp_path / "research_method_hardening",
    )["status"] == "FAIL"


def test_reader_brief_displays_research_method_hardening(tmp_path) -> None:
    review_fixture = run_review_fixture(tmp_path / "review")
    selection_fixture = run_selection_review_fixture(tmp_path / "selection")
    selection = selection_fixture["selection"]
    backfill_id = selection_fixture["backfill"]["backfill_id"]

    attribution = system_target.run_selection_attribution(
        selection_review_id=selection["selection_review_id"],
        selection_review_dir=tmp_path / "selection" / "system_target_selection_review",
        output_dir=tmp_path / "selection" / "selection_attribution",
        generated_at=datetime(2026, 1, 7, 5, tzinfo=UTC),
    )
    risk = system_target.run_limited_long_risk_review(
        backfill_id=backfill_id,
        backfill_dir=tmp_path / "selection" / "paper_shadow_backfill",
        output_dir=tmp_path / "selection" / "limited_long_risk",
        generated_at=datetime(2026, 1, 7, 6, tzinfo=UTC),
    )
    consistency = system_target.run_limited_consistency_check(
        backfill_id=backfill_id,
        rolling_eval_id=selection_fixture["rolling"]["rolling_eval_id"],
        regime_review_id=selection_fixture["regime"]["regime_review_id"],
        stability_id=selection_fixture["stability"]["stability_id"],
        backfill_dir=tmp_path / "selection" / "paper_shadow_backfill",
        rolling_eval_dir=tmp_path / "selection" / "paper_shadow_rolling_eval",
        regime_review_dir=tmp_path / "selection" / "paper_shadow_regime_review",
        stability_dir=tmp_path / "selection" / "paper_shadow_stability",
        output_dir=tmp_path / "selection" / "limited_consistency",
        generated_at=datetime(2026, 1, 7, 7, tzinfo=UTC),
    )
    impact = system_target.run_data_warning_impact_review(
        backfill_id=backfill_id,
        selection_review_id=selection["selection_review_id"],
        backfill_dir=tmp_path / "selection" / "paper_shadow_backfill",
        selection_review_dir=tmp_path / "selection" / "system_target_selection_review",
        output_dir=tmp_path / "selection" / "data_warning_impact",
        generated_at=datetime(2026, 1, 7, 8, tzinfo=UTC),
    )
    hardening = system_target.run_research_method_hardening_pack(
        selection_attribution_id=attribution["attribution_id"],
        risk_review_id=risk["risk_review_id"],
        consistency_id=consistency["consistency_id"],
        data_warning_impact_id=impact["impact_id"],
        selection_attribution_dir=tmp_path / "selection" / "selection_attribution",
        risk_review_dir=tmp_path / "selection" / "limited_long_risk",
        consistency_dir=tmp_path / "selection" / "limited_consistency",
        data_warning_impact_dir=tmp_path / "selection" / "data_warning_impact",
        output_dir=tmp_path / "selection" / "research_method_hardening",
        generated_at=datetime(2026, 1, 7, 9, tzinfo=UTC),
    )

    report_index = report_index_for_review_fixture(review_fixture)
    report_index["reports"].append(
        {
            "report_id": "etf_dynamic_v3_system_target_selection_review",
            "latest_artifact_path": str(
                selection["selection_review_dir"] / "system_target_selection_manifest.json"
            ),
        }
    )
    report_index["reports"].append(
        {
            "report_id": "etf_dynamic_v3_research_method_hardening",
            "latest_artifact_path": str(
                hardening["hardening_dir"] / "research_method_hardening_manifest.json"
            ),
        }
    )

    summary = reader_brief._etf_dynamic_v3_system_target_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["hardening_id"] == hardening["hardening_id"]
    assert summary["hardening_decision"] == hardening["hardening_decision"]["hardening_decision"]
    assert summary["hardening_decision_confidence"] == hardening["hardening_decision"][
        "decision_confidence"
    ]
    assert summary["broker_action_allowed"] is False
