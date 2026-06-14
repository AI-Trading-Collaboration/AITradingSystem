from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st


def run_filtered_candidate_evidence_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = _source_filtered_candidate_fixture(tmp_path)
    evidence = readiness.run_filtered_candidate_evidence(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        filtered_comparison_id=fixture["filtered_vs_original_comparison"]["comparison_id"],
        promotion_review_id=fixture["filtered_candidate_promotion_review"]["filtered_review_id"],
        comparison_dir=tmp_path / "filtered_vs_original_comparison",
        promotion_review_dir=tmp_path / "filtered_candidate_promotion_review",
        output_dir=tmp_path / "filtered_candidate_evidence",
        generated_at=datetime(2024, 4, 9, tzinfo=UTC),
    )
    return {**fixture, "filtered_candidate_evidence": evidence}


def run_median_regime_filter_spec_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_filtered_candidate_evidence_fixture(tmp_path)
    spec = readiness.review_median_regime_filter_spec(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        output_dir=tmp_path / "median_regime_filter_spec",
        generated_at=datetime(2024, 4, 10, tzinfo=UTC),
    )
    return {**fixture, "median_regime_filter_spec": spec}


def run_filtered_candidate_stress_backfill_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_median_regime_filter_spec_fixture(tmp_path)
    stress = readiness.run_filtered_candidate_stress_backfill(
        candidate=readiness.TOP_FILTERED_CANDIDATE,
        spec_id=fixture["median_regime_filter_spec"]["spec_id"],
        spec_dir=tmp_path / "median_regime_filter_spec",
        output_dir=tmp_path / "filtered_candidate_stress_backfill",
        generated_at=datetime(2024, 4, 11, tzinfo=UTC),
    )
    return {**fixture, "filtered_candidate_stress_backfill": stress}


def run_drawdown_mismatch_reduction_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_filtered_candidate_stress_backfill_fixture(tmp_path)
    reduction = readiness.run_drawdown_mismatch_reduction(
        stress_backfill_id=fixture["filtered_candidate_stress_backfill"]["stress_backfill_id"],
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        output_dir=tmp_path / "drawdown_mismatch_reduction",
        generated_at=datetime(2024, 4, 12, tzinfo=UTC),
    )
    return {**fixture, "drawdown_mismatch_reduction": reduction}


def run_flip_rotation_reduction_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_drawdown_mismatch_reduction_fixture(tmp_path)
    flip = readiness.run_flip_rotation_reduction(
        stress_backfill_id=fixture["filtered_candidate_stress_backfill"]["stress_backfill_id"],
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        output_dir=tmp_path / "flip_rotation_reduction",
        generated_at=datetime(2024, 4, 13, tzinfo=UTC),
    )
    return {**fixture, "flip_rotation_reduction": flip}


def run_filtered_candidate_ab_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_flip_rotation_reduction_fixture(tmp_path)
    ab = readiness.run_filtered_candidate_ab_review(
        stress_backfill_id=fixture["filtered_candidate_stress_backfill"]["stress_backfill_id"],
        mismatch_reduction_id=fixture["drawdown_mismatch_reduction"]["reduction_id"],
        flip_reduction_id=fixture["flip_rotation_reduction"]["flip_reduction_id"],
        stress_backfill_dir=tmp_path / "filtered_candidate_stress_backfill",
        mismatch_reduction_dir=tmp_path / "drawdown_mismatch_reduction",
        flip_reduction_dir=tmp_path / "flip_rotation_reduction",
        output_dir=tmp_path / "filtered_candidate_ab_review",
        generated_at=datetime(2024, 4, 14, tzinfo=UTC),
    )
    return {**fixture, "filtered_candidate_ab_review": ab}


def run_signal_gate_confirmation_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_filtered_candidate_ab_review_fixture(tmp_path)
    confirmation = readiness.register_signal_gate_confirmation(
        ab_review_id=fixture["filtered_candidate_ab_review"]["ab_review_id"],
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        output_dir=tmp_path / "signal_gate_confirmation",
        generated_at=datetime(2024, 4, 15, tzinfo=UTC),
    )
    return {**fixture, "signal_gate_confirmation": confirmation}


def run_filtered_formalization_readiness_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_signal_gate_confirmation_fixture(tmp_path)
    formalization = readiness.run_filtered_formalization_readiness(
        ab_review_id=fixture["filtered_candidate_ab_review"]["ab_review_id"],
        confirmation_id=fixture["signal_gate_confirmation"]["confirmation_id"],
        ab_review_dir=tmp_path / "filtered_candidate_ab_review",
        confirmation_dir=tmp_path / "signal_gate_confirmation",
        output_dir=tmp_path / "filtered_formalization_readiness",
        generated_at=datetime(2024, 4, 16, tzinfo=UTC),
    )
    return {**fixture, "filtered_formalization_readiness": formalization}


def run_owner_filtered_candidate_review_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_filtered_formalization_readiness_fixture(tmp_path)
    owner_review = readiness.build_owner_filtered_candidate_review(
        readiness_id=fixture["filtered_formalization_readiness"]["readiness_id"],
        readiness_dir=tmp_path / "filtered_formalization_readiness",
        output_dir=tmp_path / "owner_filtered_candidate_review",
        generated_at=datetime(2024, 4, 17, tzinfo=UTC),
    )
    return {**fixture, "owner_filtered_candidate_review": owner_review}


def run_filtered_next_decision_fixture(tmp_path: Path) -> dict[str, Any]:
    fixture = run_owner_filtered_candidate_review_fixture(tmp_path)
    decision = readiness.run_filtered_next_decision(
        owner_review_id=fixture["owner_filtered_candidate_review"]["owner_review_id"],
        owner_review_dir=tmp_path / "owner_filtered_candidate_review",
        output_dir=tmp_path / "filtered_next_decision",
        generated_at=datetime(2024, 4, 18, tzinfo=UTC),
    )
    return {**fixture, "filtered_next_decision": decision}


def assert_research_safe(payload: dict[str, Any]) -> None:
    assert payload["broker_action_allowed"] is False
    assert payload["not_official_target_weights"] is True
    assert payload["production_effect"] == "none"


def _source_filtered_candidate_fixture(tmp_path: Path) -> dict[str, Any]:
    comparison_id = "filtered-vs-original-comparison_test"
    comparison_dir = tmp_path / "filtered_vs_original_comparison" / comparison_id
    comparison_dir.mkdir(parents=True)
    matrix = [
        {
            "schema_version": st.SCHEMA_VERSION,
            "variant_id": readiness.TOP_FILTERED_CANDIDATE,
            "base_method": "median_target_weights",
            "return_delta_vs_base": 0.0008,
            "drawdown_delta_vs_base": 0.0035,
            "regime_score_delta_vs_base": 0.05,
            "signal_churn_delta_vs_base": -2,
            "harmful_event_delta_vs_base": -1,
            "regime_mismatch_delta_vs_base": -1,
            "comparison_score": 0.31,
            "comparison_status": "FILTERED_WINS",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        {
            "schema_version": st.SCHEMA_VERSION,
            "variant_id": "smooth_3d_plus_high_dispersion_hold",
            "base_method": "smooth_weights_3d_limited_adjustment",
            "return_delta_vs_base": 0.0001,
            "drawdown_delta_vs_base": 0.0012,
            "regime_score_delta_vs_base": 0.03,
            "signal_churn_delta_vs_base": -1,
            "harmful_event_delta_vs_base": -1,
            "regime_mismatch_delta_vs_base": 0,
            "comparison_score": 0.18,
            "comparison_status": "FILTERED_WINS",
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
    ]
    comparison_summary = {
        "schema_version": st.SCHEMA_VERSION,
        "best_filtered_variant": readiness.TOP_FILTERED_CANDIDATE,
        "best_base_method": "median_target_weights",
        "filtered_win_count": 2,
        "tested_variant_count": 2,
        "recommendation": "PROMOTE_FOR_REVIEW",
        "confidence": "MEDIUM",
        "requires_forward_confirmation": True,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    comparison_manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_vs_original_comparison_manifest",
        "comparison_id": comparison_id,
        "filtered_backfill_id": "filtered-candidate-backfill_test",
        "filter_design_id": "candidate-quality-filter-design_test",
        "source_ledger_id": "candidate-signal-ledger_test",
        "source_backfill_id": "micro-search-v4-backfill_test",
        "generated_at": "2024-04-05T00:00:00+00:00",
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "date_start": "2022-12-01",
        "date_end": "2026-06-10",
        "data_quality_status": "PASS_WITH_WARNINGS",
        "filtered_vs_original_manifest_path": str(
            comparison_dir / "filtered_vs_original_manifest.json"
        ),
        "filtered_comparison_matrix_path": str(comparison_dir / "filtered_comparison_matrix.jsonl"),
        "filtered_improvement_summary_path": str(
            comparison_dir / "filtered_improvement_summary.json"
        ),
        "filtered_vs_original_comparison_report_path": str(
            comparison_dir / "filtered_vs_original_comparison_report.md"
        ),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    st._write_json(comparison_dir / "filtered_vs_original_manifest.json", comparison_manifest)
    st._write_jsonl(comparison_dir / "filtered_comparison_matrix.jsonl", matrix)
    st._write_json(comparison_dir / "filtered_improvement_summary.json", comparison_summary)
    st._write_text(comparison_dir / "filtered_vs_original_comparison_report.md", "# comparison\n")

    review_id = "filtered-candidate-promotion-review_test"
    review_dir = tmp_path / "filtered_candidate_promotion_review" / review_id
    review_dir.mkdir(parents=True)
    decision = {
        "schema_version": st.SCHEMA_VERSION,
        "filtered_review_id": review_id,
        "decision": "CONTINUE_TESTING",
        "best_filtered_variant": readiness.TOP_FILTERED_CANDIDATE,
        "comparison_recommendation": "PROMOTE_FOR_REVIEW",
        "gate_recommendation": "continue_forward_confirmation",
        "confidence": "MEDIUM",
        "requires_forward_confirmation": True,
        "recommended_next_action": "owner_review_and_forward_confirmation",
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": st.PRODUCTION_EFFECT,
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    specs = {
        "schema_version": st.SCHEMA_VERSION,
        "candidate_variant": {
            "variant_id": readiness.TOP_FILTERED_CANDIDATE,
            "base_method": "median_target_weights",
            "implementation_scope": "research_only",
            "candidate_status": "CONTINUE_TESTING",
            "requires_owner_approval": True,
            **st.EXPERIMENT_FACTORY_SAFETY,
        },
        "keep_testing_plan": ["continue forward confirmation"],
        "formal_implementation_plan": [],
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    review_manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_filtered_candidate_promotion_review_manifest",
        "filtered_review_id": review_id,
        "comparison_id": comparison_id,
        "signal_gate_experiment_id": "signal-gate-experiment_test",
        "filtered_backfill_id": "filtered-candidate-backfill_test",
        "filter_design_id": "candidate-quality-filter-design_test",
        "generated_at": "2024-04-07T00:00:00+00:00",
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "date_start": "2022-12-01",
        "date_end": "2026-06-10",
        "data_quality_status": "PASS_WITH_WARNINGS",
        "filtered_promotion_manifest_path": str(review_dir / "filtered_promotion_manifest.json"),
        "filtered_promotion_decision_path": str(review_dir / "filtered_promotion_decision.json"),
        "filtered_candidate_specs_path": str(review_dir / "filtered_candidate_specs.json"),
        "filtered_candidate_promotion_review_report_path": str(
            review_dir / "filtered_candidate_promotion_review_report.md"
        ),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        **st.EXPERIMENT_FACTORY_SAFETY,
    }
    st._write_json(review_dir / "filtered_promotion_manifest.json", review_manifest)
    st._write_json(review_dir / "filtered_promotion_decision.json", decision)
    st._write_json(review_dir / "filtered_candidate_specs.json", specs)
    st._write_text(review_dir / "filtered_candidate_promotion_review_report.md", "# review\n")
    st._write_text(review_dir / "reader_brief_section.md", "## review\n")
    return {
        "filtered_vs_original_comparison": {
            "comparison_id": comparison_id,
            "manifest": comparison_manifest,
            "filtered_comparison_matrix": matrix,
            "filtered_improvement_summary": comparison_summary,
        },
        "filtered_candidate_promotion_review": {
            "filtered_review_id": review_id,
            "manifest": review_manifest,
            "filtered_promotion_decision": decision,
            "filtered_candidate_specs": specs,
        },
    }
