from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"


REQUIRED_JSON_ARTIFACTS = (
    "weight_research_retrospective.json",
    "weight_control_architecture_rfc.json",
    "ablation_protocol.json",
    "statistical_validation_policy.json",
    "next_research_program_roadmap.json",
    "research_program_control_plane.json",
    "research_window_catalog.json",
    "portfolio_utility_scorecard_contract.json",
    "ablation_runner_contract.json",
    "research_result_comparison_harness.json",
    "b0_static_strategic_baseline_result.json",
    "ablation_runner_scope_freeze.json",
    "signal_robustness_entry_contract.json",
    "untouched_holdout_final_gate_policy.json",
    "b1_execution_control_result.json",
    "b1_metric_semantics_and_comparator_audit.json",
    "static_baseline_family_result.json",
    "b1_isolated_attribution_result.json",
    "research_layer_interface_contract.json",
    "dependency_boundary_validation.json",
    "signal_diagnostics_framework_contract.json",
    "b2_risk_scaler_research_result.json",
    "b3_relative_tilt_research_result.json",
    "b4_risk_tilt_interaction_result.json",
    "confidence_shrinkage_contract.json",
    "confidence_interaction_review.json",
    "regime_information_contract.json",
    "regime_incremental_evaluation.json",
    "main_interaction_effect_synthesis.json",
    "candidate_v3_spec_from_proven_effects.json",
    "candidate_v3_mini_gate_result.json",
    "weight_research_program_v1_snapshot.json",
)


def test_weight_research_program_contracts_have_required_safety_boundary() -> None:
    for file_name in REQUIRED_JSON_ARTIFACTS:
        payload = _read_json(file_name)

        assert payload["schema_version"] == 1
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["status"]
        safety = payload["safety_boundary"]
        assert safety["research_only"] is True
        assert safety["manual_review_only"] is True
        assert safety["official_target_weights"] is False
        assert safety["production_effect"] == "none"


def test_retrospective_failure_taxonomy_matches_program_contract() -> None:
    payload = _read_json("weight_research_retrospective.json")

    assert payload["failure_taxonomy"] == [
        "DATA_COVERAGE_FAILURE",
        "BINDING_FAILURE",
        "SIGNAL_ROBUSTNESS_FAILURE",
        "ALLOCATOR_MAPPING_FAILURE",
        "RISK_CONTROL_FAILURE",
        "EXECUTION_COST_FAILURE",
        "BENCHMARK_FAILURE",
        "WINDOW_FRAGILITY_FAILURE",
        "OVERFIT_RISK",
        "GOVERNANCE_HOLD",
    ]
    assert {row["category"] for row in payload["failure_attribution"]} == set(
        payload["failure_taxonomy"]
    )


def test_ablation_protocol_freezes_b0_to_b6_sequence() -> None:
    payload = _read_json("ablation_protocol.json")

    assert [row["layer_id"] for row in payload["layers"]] == [
        "B0",
        "B1",
        "B2",
        "B3",
        "B4",
        "B5",
        "B6",
    ]
    assert "signal_robustness_status" in payload["required_outputs_per_layer"]
    assert "window catalog is not frozen" in payload["hard_stops"]


def test_scorecard_contract_records_governed_pilot_lambdas() -> None:
    payload = _read_json("portfolio_utility_scorecard_contract.json")

    metadata = payload["policy_metadata"]
    assert metadata["status"] == "pilot_baseline_pre_experiment"
    assert metadata["rationale"]
    assert metadata["intended_effect"]
    assert metadata["validation_evidence"]
    assert metadata["review_condition"]
    assert {row["parameter"] for row in payload["lambda_parameters"]} >= {
        "net_return_weight",
        "drawdown_penalty_weight",
        "cost_drag_weight",
        "signal_robustness_penalty_weight",
    }
    assert payload["statuses"] == [
        "UTILITY_IMPROVED",
        "UTILITY_MIXED",
        "UTILITY_WEAK",
        "UTILITY_INVALID",
    ]


def test_window_catalog_blocks_reused_diagnostics_as_final_holdout() -> None:
    payload = _read_json("research_window_catalog.json")
    windows = {row["window_id"]: row for row in payload["windows"]}

    assert payload["status"] == "WINDOW_CATALOG_READY_WITH_HOLDOUT_BLOCKER"
    assert windows["slow_drawdown"]["purpose"] == "stress diagnostic"
    assert "final untouched holdout" in windows["slow_drawdown"]["forbidden_stage"]
    assert windows["untouched_temporal_holdout"]["data_quality_status"] == "BLOCKED_NOT_FROZEN"
    assert windows["untouched_temporal_holdout"]["start_date"] is None
    assert payload["holdout_blocker"]


def test_b0_static_baseline_result_discloses_data_quality_and_control_semantics() -> None:
    payload = _read_json("b0_static_strategic_baseline_result.json")

    assert payload["status"] == "B0_MINI_BACKFILL_COMPLETE_CONTROL_ONLY"
    assert payload["data_quality_gate"]["required_command"] == "aits validate-data"
    assert payload["data_quality_gate"]["passed"] is True
    assert payload["data_quality_gate"]["run_status"] == "PASS_WITH_WARNINGS"
    assert payload["runtime_quality"]["data_quality_status_in_summary"] == "PASS"
    assert payload["baseline_source"]["benchmark_id"] == "B000"
    assert payload["baseline_source"]["semantics"].startswith("research-only static control")
    assert payload["required_outputs"]["return_proxy"] is not None
    assert payload["required_outputs"]["drawdown_proxy"] is not None
    assert payload["required_outputs"]["signal_robustness_status"] == (
        "NOT_APPLICABLE_STATIC_CONTROL"
    )
    assert payload["hard_stop_review"]["touched_final_holdout"] is False
    assert payload["hard_stop_review"]["v3_candidate_allowed"] is False


def test_unblock_contracts_freeze_scope_signal_and_holdout_rules() -> None:
    scope = _read_json("ablation_runner_scope_freeze.json")
    signal = _read_json("signal_robustness_entry_contract.json")
    holdout = _read_json("untouched_holdout_final_gate_policy.json")

    assert scope["status"] == "ABLATION_RUNNER_SCOPE_FROZEN"
    b1 = {row["layer_id"]: row for row in scope["layers"]}["B1"]
    assert b1["added_mechanism"] == "execution_no_trade_turnover_control_only"
    assert "mixed_dynamic_allocation_logic" in b1["forbidden_mechanisms"]
    assert "P0 dynamic strategy" in scope["global_forbidden_substitutes"]
    assert signal["required_signal_series"]["B1"] == []
    assert signal["layer_entry_rules"]["B1"]["signal_robustness_required"] is False
    assert holdout["holdout_access_policy"]["on_early_holdout_use"] == "FAIL_CLOSED"
    assert holdout["window_sets"]["untouched_holdout_windows"][0]["start_date"] == "2026-07-01"


def test_b1_execution_control_result_is_research_only_and_mixed() -> None:
    payload = _read_json("b1_execution_control_result.json")

    assert payload["status"] == "B1_MINI_BACKFILL_COMPLETE_RESEARCH_ONLY"
    assert payload["added_mechanism"] == "execution_no_trade_turnover_control_only"
    assert payload["data_quality_gate"]["status"] == "PASS_WITH_WARNINGS"
    assert payload["signal_robustness_status"] == (
        "NOT_APPLICABLE_B1_EXECUTION_CONTROL_NO_SIGNAL_INPUT"
    )
    assert payload["forbidden_logic_check"] == (
        "PASS_NO_P0_ALLOCATOR_SIGNALS_REGIME_FEATURE_STORE_OR_CONFIDENCE"
    )
    assert payload["window"]["holdout_accessed"] is False
    assert payload["interpretation"]["result_class"] == "MIXED_RESEARCH_ONLY"
    assert payload["b1_vs_b0_comparison"]["return_delta"] > 0
    assert payload["b1_vs_b0_comparison"]["drawdown_reduction"] < 0


def test_phase0_b1_attribution_repair_outputs_valid_mixed_gate() -> None:
    audit = _read_json("b1_metric_semantics_and_comparator_audit.json")
    baselines = _read_json("static_baseline_family_result.json")
    attribution = _read_json("b1_isolated_attribution_result.json")

    assert audit["status"] == "B1_ATTRIBUTION_PARTIAL"
    assert baselines["status"] == "STATIC_BASELINE_FAMILY_READY_RESEARCH_ONLY"
    assert baselines["b0h_metrics"]["turnover"] == 0.0
    assert baselines["b0r_metrics"]["turnover"] > 0.0
    assert attribution["status"] == "B1_ATTRIBUTION_VALID_MIXED"
    assert attribution["target_path_validation"]["status"] == "PASS"
    assert attribution["attribution_gate"]["b2_b3_may_continue"] is True
    assert attribution["attribution_gate"]["execution_default_candidate_allowed"] is False


def test_phase1_interface_and_signal_diagnostics_contracts_are_ready() -> None:
    interface = _read_json("research_layer_interface_contract.json")
    dependency = _read_json("dependency_boundary_validation.json")
    diagnostics = _read_json("signal_diagnostics_framework_contract.json")

    assert interface["status"] == "RESEARCH_LAYER_INTERFACE_CONTRACT_READY"
    assert [layer["layer_id"] for layer in interface["layers"]] == [
        "feature",
        "signal",
        "target",
        "execution",
        "evaluation",
    ]
    assert dependency["status"] == "PASS"
    assert diagnostics["status"] == "SIGNAL_DIAGNOSTICS_FRAMEWORK_READY"
    assert diagnostics["runner_contract"]["evaluates_portfolio_return"] is False


def test_phase2_to_phase4_b2_b3_b4_research_results_are_auditable() -> None:
    b2 = _read_json("b2_risk_scaler_research_result.json")
    b3 = _read_json("b3_relative_tilt_research_result.json")
    b4 = _read_json("b4_risk_tilt_interaction_result.json")

    assert b2["status"] == "B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
    assert b2["signal_artifact"]["signal_gate_status"] == "B2_SIGNAL_READY"
    assert b2["signal_diagnostics"]["status"] == "SIGNAL_DIAGNOSTICS_PASS"
    assert b2["holdout_accessed"] is False
    assert b2["forbidden_outputs_absent"] is True

    assert b3["status"] == "B3_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
    assert b3["signal_artifact"]["signal_gate_status"] == "B3_SIGNAL_READY"
    assert b3["signal_diagnostics"]["status"] == "SIGNAL_DIAGNOSTICS_PASS"
    assert b3["holdout_accessed"] is False
    assert b3["forbidden_outputs_absent"] is True

    assert b4["status"] == "B4_INTERACTION_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
    assert b4["component_signal_gate"] == "B4_COMPONENT_SIGNALS_READY"
    assert b4["interaction_effects"]["classification"] == "INCONCLUSIVE"
    assert b4["same_window_controls"]["control_window"] == {
        "start": "2024-07-10",
        "end": "2024-08-09",
    }
    assert b4["holdout_accessed"] is False
    assert b4["forbidden_outputs_absent"] is True


def test_phase5_to_phase7_checkpoint_blocks_unproven_v3_candidate() -> None:
    confidence = _read_json("confidence_interaction_review.json")
    regime = _read_json("regime_incremental_evaluation.json")
    synthesis = _read_json("main_interaction_effect_synthesis.json")
    candidate = _read_json("candidate_v3_spec_from_proven_effects.json")
    gate = _read_json("candidate_v3_mini_gate_result.json")

    assert confidence["status"] == "CONFIDENCE_INTERACTION_BLOCKED_CORE_COMBO_INCONCLUSIVE"
    assert regime["status"] == "REGIME_INCREMENTAL_EVALUATION_BLOCKED_NO_PRE_REGIME_COMBO"
    assert synthesis["status"] == "INCONCLUSIVE"
    assert synthesis["selected_modules"] == []
    assert candidate["status"] == "V3_SPEC_BLOCKED_NO_PROVEN_EFFECTS"
    assert gate["status"] == "V3_BLOCKED"
    assert gate["untouched_holdout_accessed"] is False


def test_program_snapshot_does_not_claim_missing_later_ablation_results() -> None:
    payload = _read_json("weight_research_program_v1_snapshot.json")

    assert payload["status"] == "WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE"
    results = {row["layer_id"]: row for row in payload["b0_to_b6_results"]}
    assert results["B0"]["status"] == "B0_MINI_BACKFILL_COMPLETE_CONTROL_ONLY"
    assert results["B0"]["result_artifact"] == (
        "docs/research/b0_static_strategic_baseline_result.json"
    )
    assert results["B1"]["status"] == "B1_ATTRIBUTION_VALID_MIXED"
    assert results["B1"]["result_artifact"] == "docs/research/b1_isolated_attribution_result.json"
    assert results["B2"]["status"] == "B2_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
    assert results["B3"]["status"] == "B3_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
    assert results["B4"]["status"] == (
        "B4_INTERACTION_MINI_BACKFILL_WITH_E0_E1_COMPLETE_RESEARCH_ONLY"
    )
    assert results["B5"]["status"] == "CONFIDENCE_INTERACTION_BLOCKED_CORE_COMBO_INCONCLUSIVE"
    assert results["B6"]["status"] == "REGIME_INCREMENTAL_EVALUATION_BLOCKED_NO_PRE_REGIME_COMBO"
    assert payload["selected_modules"] == []
    assert payload["v3_candidate_status"] == "V3_SPEC_BLOCKED_NO_PROVEN_EFFECTS"
    assert payload["v3_mini_gate_status"] == "V3_BLOCKED"


def _read_json(file_name: str) -> dict[str, object]:
    path = RESEARCH_DIR / file_name
    return json.loads(path.read_text(encoding="utf-8"))
