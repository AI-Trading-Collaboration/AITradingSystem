from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_weight_batch_search_helpers import run_owner_signal_roadmap_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search
from ai_trading_system.platform.artifacts.validation_session import (
    with_artifact_validation_session,
)


def _assert_research_safe(payload: dict[str, Any]) -> None:
    assert payload["broker_action_allowed"] is False
    assert payload["not_official_target_weights"] is True
    assert payload["production_effect"] == "none"


@with_artifact_validation_session
def test_signal_feature_quality_filter_pipeline_builds_and_validates(tmp_path: Path) -> None:
    fixture = run_owner_signal_roadmap_fixture(tmp_path)

    taxonomy = fixture["signal_failure_taxonomy"]
    taxonomy_validation = weight_search.validate_signal_failure_taxonomy_artifact(
        taxonomy_id=taxonomy["taxonomy_id"],
        output_dir=tmp_path / "signal_failure_taxonomy",
    )
    assert taxonomy_validation["status"] == "PASS"
    assert taxonomy["manifest"]["failure_mode_count"] >= 10
    modes = {row["mode"] for row in taxonomy["signal_failure_mode_catalog"]["failure_modes"]}
    assert {"signal_churn", "regime_mismatch", "candidate_disagreement_high"} <= modes
    _assert_research_safe(taxonomy["manifest"])

    ledger = fixture["candidate_signal_ledger"]
    ledger_validation = weight_search.validate_candidate_signal_ledger_artifact(
        ledger_id=ledger["ledger_id"],
        output_dir=tmp_path / "candidate_signal_ledger",
    )
    assert ledger_validation["status"] == "PASS"
    ledger_summary = ledger["candidate_signal_summary"]
    assert ledger_summary["event_count"] == 0
    assert ledger_summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert ledger_summary["dominant_failure_mode"] is None
    assert ledger_summary["unstable_method_count"] is None
    assert all(
        row["event_count"] is None and row["signal_quality_status"] == "INSUFFICIENT_DATA"
        for row in ledger_summary["methods"]
    )
    assert ledger["manifest"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    _assert_research_safe(ledger["manifest"])
    assert "Candidate Signal Ledger" in ledger["reader_brief_section"]

    root_cause = fixture["signal_churn_root_cause"]
    root_validation = weight_search.validate_signal_churn_root_cause_artifact(
        root_cause_id=root_cause["root_cause_id"],
        output_dir=tmp_path / "signal_churn_root_cause",
    )
    assert root_validation["status"] == "PASS"
    assert root_cause["churn_root_cause_summary"]["dominant_root_cause"] is None
    assert root_cause["churn_root_cause_summary"]["evidence_status"] == "INSUFFICIENT_DATA"
    _assert_research_safe(root_cause["manifest"])

    mismatch = fixture["regime_mismatch_attribution"]
    mismatch_validation = weight_search.validate_regime_mismatch_attribution_artifact(
        mismatch_id=mismatch["mismatch_id"],
        output_dir=tmp_path / "regime_mismatch_attribution",
    )
    assert mismatch_validation["status"] == "PASS"
    assert mismatch["regime_mismatch_summary"]["mismatch_count"] == 0
    assert mismatch["regime_mismatch_summary"]["dominant_mismatch_type"] is None
    assert mismatch["regime_mismatch_summary"]["evidence_status"] == "INSUFFICIENT_DATA"
    _assert_research_safe(mismatch["manifest"])

    filter_design = fixture["candidate_quality_filter_design"]
    filter_validation = weight_search.validate_candidate_quality_filter_design_artifact(
        filter_design_id=filter_design["filter_design_id"],
        output_dir=tmp_path / "candidate_quality_filter_design",
    )
    assert filter_validation["status"] == "PASS"
    assert filter_design["proposed_quality_filters"]["filters"] == []
    assert filter_design["proposed_quality_filters"]["evidence_status"] == "INSUFFICIENT_DATA"
    _assert_research_safe(filter_design["manifest"])
    assert "Candidate Quality Filter Design" in filter_design["reader_brief_section"]

    filtered_backfill = fixture["filtered_candidate_backfill"]
    backfill_validation = weight_search.validate_filtered_candidate_backfill_artifact(
        filtered_backfill_id=filtered_backfill["filtered_backfill_id"],
        output_dir=tmp_path / "filtered_candidate_backfill",
    )
    assert backfill_validation["status"] == "PASS"
    assert filtered_backfill["filtered_variant_specs"] == []
    assert filtered_backfill["filtered_variant_performance"] == []
    assert filtered_backfill["filtered_variant_signal_metrics"] == []
    assert filtered_backfill["manifest"]["evidence_status"] == "INSUFFICIENT_DATA"
    assert filtered_backfill["manifest"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    _assert_research_safe(filtered_backfill["manifest"])

    comparison = fixture["filtered_vs_original_comparison"]
    comparison_validation = weight_search.validate_filtered_vs_original_comparison_artifact(
        comparison_id=comparison["comparison_id"],
        output_dir=tmp_path / "filtered_vs_original_comparison",
    )
    assert comparison_validation["status"] == "PASS"
    assert comparison["filtered_comparison_matrix"] == []
    assert comparison["filtered_improvement_summary"]["best_filtered_variant"] is None
    assert comparison["filtered_improvement_summary"]["recommendation"] == "INSUFFICIENT_DATA"
    assert comparison["filtered_improvement_summary"]["evidence_status"] == "INSUFFICIENT_DATA"
    _assert_research_safe(comparison["manifest"])

    gate_experiment = fixture["signal_gate_experiment"]
    gate_validation = weight_search.validate_signal_gate_experiment_artifact(
        signal_gate_experiment_id=gate_experiment["signal_gate_experiment_id"],
        output_dir=tmp_path / "signal_gate_experiment",
    )
    assert gate_validation["status"] == "PASS"
    assert gate_experiment["signal_gate_experiment_results"] == []
    gate_summary = gate_experiment["signal_gate_experiment_summary"]
    assert gate_summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert gate_summary["tested_gate_count"] == 0
    assert gate_summary["formalization_ready"] is False
    _assert_research_safe(gate_experiment["manifest"])
    assert "Signal Gate Experiment" in gate_experiment["reader_brief_section"]

    review = fixture["filtered_candidate_promotion_review"]
    review_validation = weight_search.validate_filtered_candidate_promotion_review_artifact(
        filtered_review_id=review["filtered_review_id"],
        output_dir=tmp_path / "filtered_candidate_promotion_review",
    )
    assert review_validation["status"] == "PASS"
    assert review["filtered_promotion_decision"]["decision"] == "INSUFFICIENT_DATA"
    assert review["filtered_promotion_decision"]["confidence"] is None
    assert review["filtered_candidate_specs"]["candidate_variant"] is None
    _assert_research_safe(review["manifest"])

    roadmap = fixture["owner_signal_roadmap"]
    roadmap_validation = weight_search.validate_owner_signal_roadmap_artifact(
        owner_signal_roadmap_id=roadmap["owner_signal_roadmap_id"],
        output_dir=tmp_path / "owner_signal_roadmap",
    )
    assert roadmap_validation["status"] == "PASS"
    roadmap_summary = roadmap["owner_signal_roadmap_summary"]
    assert roadmap_summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert roadmap_summary["candidate_available"] is False
    assert (
        roadmap_summary["recommended_owner_action"]
        == "BUILD_VALIDATED_DATED_SIGNAL_AND_FILTERED_OUTCOME_EVIDENCE"
    )
    _assert_research_safe(roadmap["manifest"])
    assert "Owner Signal Roadmap" in roadmap["reader_brief_section"]
