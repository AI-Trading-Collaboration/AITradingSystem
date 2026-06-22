from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from controlled_strategy_batch_helpers import (
    TEST_AS_OF,
    CliRunner,
    _assert_safety,
    _run_tail_risk_falsification_inputs,
    app,
)

from ai_trading_system.controlled_strategy_batch import (
    run_tail_risk_artifact_determinism_check,
    run_tail_risk_baseline_dominance_gate,
    run_tail_risk_counterfactual_baseline_result_review,
    run_tail_risk_daily_reading_safety_summary,
    run_tail_risk_decision_time_boundary_audit,
    run_tail_risk_evidence_maturity_gate,
    run_tail_risk_fallback_counterfactual_validation,
    run_tail_risk_fallback_error_cost_ledger,
    run_tail_risk_forward_aging_tracker,
    run_tail_risk_forward_outcome_contract_audit,
    run_tail_risk_governance_artifact_snapshot,
    run_tail_risk_hard_block_mutation_tests,
    run_tail_risk_independent_forward_outcome_result_review,
    run_tail_risk_independent_forward_outcome_validation,
    run_tail_risk_independent_trigger_v2_builder,
    run_tail_risk_independent_trigger_v2_input_quality_review,
    run_tail_risk_leakage_stress_suite,
    run_tail_risk_next_decision_document,
    run_tail_risk_post_merge_evidence_review,
    run_tail_risk_promotion_readiness_gate,
    run_tail_risk_real_data_validation_audit,
    run_tail_risk_regime_stratified_forward_outcome_review,
    run_tail_risk_report_registry_integrity_review,
    run_tail_risk_research_master_review,
    run_tail_risk_research_readiness_score,
    run_tail_risk_status_matrix,
    run_tail_risk_tainted_metric_quarantine,
    run_tail_risk_task_coverage_map,
    run_tail_risk_threshold_sensitivity_review,
    run_tail_risk_trigger_feature_availability_catalog,
    run_tail_risk_trigger_label_independence_audit,
)
from ai_trading_system.reports import reader_brief


@pytest.fixture(scope="module")
def tail_risk_governance(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    tmp_path = tmp_path_factory.mktemp("tail_risk_independent_governance")
    paths = _run_tail_risk_falsification_inputs(tmp_path)
    output_root = tmp_path / "governance"
    payloads: dict[str, dict[str, Any]] = {}

    def record(task_id: str, payload: dict[str, Any]) -> Path:
        payloads[task_id] = payload
        return Path(payload["artifact_paths"]["json_path"])

    paths["trigger_label"] = record(
        "TRADING-827",
        run_tail_risk_trigger_label_independence_audit(
            classifier_path=paths["classifier"],
            robustness_path=paths["robustness"],
            precision_recall_path=paths["precision"],
            anti_leakage_path=paths["anti_leakage"],
            forward_integration_path=paths["forward"],
            output_root=output_root,
        ),
    )
    paths["independent_forward"] = record(
        "TRADING-828",
        run_tail_risk_independent_forward_outcome_validation(
            value_surface_expansion_path=paths["value_expansion"],
            classifier_path=paths["classifier"],
            robustness_path=paths["robustness"],
            trigger_label_audit_path=paths["trigger_label"],
            output_root=output_root,
        ),
    )
    paths["contract"] = record(
        "TRADING-829",
        run_tail_risk_forward_outcome_contract_audit(
            trigger_label_audit_path=paths["trigger_label"],
            independent_forward_path=paths["independent_forward"],
            output_root=output_root,
        ),
    )
    paths["boundary"] = record(
        "TRADING-830",
        run_tail_risk_decision_time_boundary_audit(
            trigger_label_audit_path=paths["trigger_label"],
            contract_audit_path=paths["contract"],
            output_root=output_root,
        ),
    )
    paths["quarantine"] = record(
        "TRADING-831",
        run_tail_risk_tainted_metric_quarantine(
            trigger_label_audit_path=paths["trigger_label"],
            precision_recall_path=paths["precision"],
            robustness_path=paths["robustness"],
            opportunity_cost_path=paths["opportunity"],
            output_root=output_root,
        ),
    )
    paths["counterfactual"] = record(
        "TRADING-832",
        run_tail_risk_fallback_counterfactual_validation(
            independent_forward_path=paths["independent_forward"],
            output_root=output_root,
        ),
    )
    paths["regime_review"] = record(
        "TRADING-833",
        run_tail_risk_regime_stratified_forward_outcome_review(
            independent_forward_path=paths["independent_forward"],
            counterfactual_path=paths["counterfactual"],
            output_root=output_root,
        ),
    )
    paths["sensitivity_review"] = record(
        "TRADING-834",
        run_tail_risk_threshold_sensitivity_review(
            independent_forward_path=paths["independent_forward"],
            counterfactual_path=paths["counterfactual"],
            output_root=output_root,
        ),
    )
    paths["error_cost"] = record(
        "TRADING-835",
        run_tail_risk_fallback_error_cost_ledger(
            independent_forward_path=paths["independent_forward"],
            output_root=output_root,
        ),
    )
    paths["evidence_gate"] = record(
        "TRADING-836",
        run_tail_risk_evidence_maturity_gate(
            independent_forward_path=paths["independent_forward"],
            regime_review_path=paths["regime_review"],
            output_root=output_root,
        ),
    )
    paths["aging_tracker"] = record(
        "TRADING-837",
        run_tail_risk_forward_aging_tracker(
            forward_integration_path=paths["forward"],
            independent_forward_path=paths["independent_forward"],
            output_root=output_root,
            as_of_date=TEST_AS_OF,
        ),
    )
    paths["leakage_stress"] = record(
        "TRADING-838",
        run_tail_risk_leakage_stress_suite(
            trigger_label_audit_path=paths["trigger_label"],
            independent_forward_path=paths["independent_forward"],
            contract_audit_path=paths["contract"],
            boundary_audit_path=paths["boundary"],
            output_root=output_root,
        ),
    )
    paths["promotion_gate"] = record(
        "TRADING-839",
        run_tail_risk_promotion_readiness_gate(
            trigger_label_audit_path=paths["trigger_label"],
            independent_forward_path=paths["independent_forward"],
            contract_audit_path=paths["contract"],
            boundary_audit_path=paths["boundary"],
            leakage_stress_path=paths["leakage_stress"],
            output_root=output_root,
        ),
    )
    paths["trigger_v2"] = record(
        "TRADING-840",
        run_tail_risk_independent_trigger_v2_builder(
            value_surface_expansion_path=paths["value_expansion"],
            boundary_audit_path=paths["boundary"],
            output_root=output_root,
        ),
    )
    paths["feature_catalog"] = record(
        "TRADING-841",
        run_tail_risk_trigger_feature_availability_catalog(
            trigger_v2_path=paths["trigger_v2"],
            output_root=output_root,
        ),
    )
    paths["master"] = record(
        "TRADING-842",
        run_tail_risk_research_master_review(
            trigger_label_audit_path=paths["trigger_label"],
            independent_forward_path=paths["independent_forward"],
            contract_audit_path=paths["contract"],
            boundary_audit_path=paths["boundary"],
            quarantine_path=paths["quarantine"],
            counterfactual_path=paths["counterfactual"],
            regime_review_path=paths["regime_review"],
            sensitivity_review_path=paths["sensitivity_review"],
            error_cost_path=paths["error_cost"],
            evidence_gate_path=paths["evidence_gate"],
            aging_tracker_path=paths["aging_tracker"],
            leakage_stress_path=paths["leakage_stress"],
            promotion_gate_path=paths["promotion_gate"],
            trigger_v2_path=paths["trigger_v2"],
            feature_catalog_path=paths["feature_catalog"],
            output_root=output_root,
        ),
    )
    paths["post_merge"] = record(
        "POST_MERGE",
        run_tail_risk_post_merge_evidence_review(
            trigger_label_audit_path=paths["trigger_label"],
            independent_forward_path=paths["independent_forward"],
            contract_audit_path=paths["contract"],
            boundary_audit_path=paths["boundary"],
            quarantine_path=paths["quarantine"],
            counterfactual_path=paths["counterfactual"],
            regime_review_path=paths["regime_review"],
            sensitivity_review_path=paths["sensitivity_review"],
            error_cost_path=paths["error_cost"],
            evidence_gate_path=paths["evidence_gate"],
            aging_tracker_path=paths["aging_tracker"],
            leakage_stress_path=paths["leakage_stress"],
            promotion_gate_path=paths["promotion_gate"],
            trigger_v2_path=paths["trigger_v2"],
            feature_catalog_path=paths["feature_catalog"],
            master_review_path=paths["master"],
            output_root=output_root,
        ),
    )
    governance_task_paths = {
        "TRADING-827": paths["trigger_label"],
        "TRADING-828": paths["independent_forward"],
        "TRADING-829": paths["contract"],
        "TRADING-830": paths["boundary"],
        "TRADING-831": paths["quarantine"],
        "TRADING-832": paths["counterfactual"],
        "TRADING-833": paths["regime_review"],
        "TRADING-834": paths["sensitivity_review"],
        "TRADING-835": paths["error_cost"],
        "TRADING-836": paths["evidence_gate"],
        "TRADING-837": paths["aging_tracker"],
        "TRADING-838": paths["leakage_stress"],
        "TRADING-839": paths["promotion_gate"],
        "TRADING-840": paths["trigger_v2"],
        "TRADING-841": paths["feature_catalog"],
        "TRADING-842": paths["master"],
    }
    paths["snapshot"] = record(
        "TRADING-843",
        run_tail_risk_governance_artifact_snapshot(
            artifact_paths=governance_task_paths,
            output_root=output_root,
        ),
    )
    paths["status_matrix"] = record(
        "TRADING-844",
        run_tail_risk_status_matrix(
            snapshot_path=paths["snapshot"],
            output_root=output_root,
        ),
    )
    paths["real_data_audit"] = record(
        "TRADING-845",
        run_tail_risk_real_data_validation_audit(
            snapshot_path=paths["snapshot"],
            output_root=output_root,
        ),
    )
    paths["forward_result_review"] = record(
        "TRADING-846",
        run_tail_risk_independent_forward_outcome_result_review(
            independent_forward_path=paths["independent_forward"],
            contract_audit_path=paths["contract"],
            boundary_audit_path=paths["boundary"],
            counterfactual_path=paths["counterfactual"],
            output_root=output_root,
        ),
    )
    paths["baseline_result_review"] = record(
        "TRADING-847",
        run_tail_risk_counterfactual_baseline_result_review(
            counterfactual_path=paths["counterfactual"],
            independent_forward_path=paths["independent_forward"],
            output_root=output_root,
        ),
    )
    paths["determinism"] = record(
        "TRADING-848",
        run_tail_risk_artifact_determinism_check(
            snapshot_path=paths["snapshot"],
            output_root=output_root,
        ),
    )
    paths["coverage_map"] = record(
        "TRADING-850",
        run_tail_risk_task_coverage_map(
            output_root=output_root,
            docs_path=tmp_path / "docs" / "tail_risk_coverage_map.md",
        ),
    )
    paths["mutation_tests"] = record(
        "TRADING-851",
        run_tail_risk_hard_block_mutation_tests(output_root=output_root),
    )
    paths["registry_integrity"] = record(
        "TRADING-852",
        run_tail_risk_report_registry_integrity_review(output_root=output_root),
    )
    paths["daily_safety"] = record(
        "TRADING-853",
        run_tail_risk_daily_reading_safety_summary(
            status_matrix_path=paths["status_matrix"],
            master_review_path=paths["master"],
            output_root=output_root,
        ),
    )
    paths["trigger_v2_input_quality"] = record(
        "TRADING-854",
        run_tail_risk_independent_trigger_v2_input_quality_review(
            feature_catalog_path=paths["feature_catalog"],
            output_root=output_root,
        ),
    )
    paths["baseline_dominance"] = record(
        "TRADING-856",
        run_tail_risk_baseline_dominance_gate(
            baseline_review_path=paths["baseline_result_review"],
            output_root=output_root,
        ),
    )
    paths["readiness"] = record(
        "TRADING-857",
        run_tail_risk_research_readiness_score(
            status_matrix_path=paths["status_matrix"],
            forward_review_path=paths["forward_result_review"],
            baseline_gate_path=paths["baseline_dominance"],
            evidence_gate_path=paths["evidence_gate"],
            regime_review_path=paths["regime_review"],
            sensitivity_review_path=paths["sensitivity_review"],
            output_root=output_root,
        ),
    )
    paths["next_decision"] = record(
        "TRADING-858",
        run_tail_risk_next_decision_document(
            status_matrix_path=paths["status_matrix"],
            forward_review_path=paths["forward_result_review"],
            baseline_review_path=paths["baseline_result_review"],
            baseline_gate_path=paths["baseline_dominance"],
            readiness_path=paths["readiness"],
            master_review_path=paths["master"],
            quarantine_path=paths["quarantine"],
            output_root=output_root,
            docs_path=tmp_path / "docs" / "tail_risk_next_decision.md",
        ),
    )
    return {"paths": paths, "payloads": payloads}


def test_tail_risk_independent_forward_outcome_contract(
    tail_risk_governance: dict[str, Any],
) -> None:
    payload = tail_risk_governance["payloads"]["TRADING-828"]
    _assert_safety(payload)
    assert payload["task_id"] == "TRADING-828"
    assert payload["status"] == "INDEPENDENT_FORWARD_VALIDATED"
    assert Path(payload["artifact_paths"]["json_path"]).exists()
    assert Path(payload["artifact_paths"]["markdown_path"]).exists()
    assert payload["outcome_source_contract"]["strictly_after_decision_time"] is True
    assert payload["outcome_source_contract"]["forbidden_label_or_case_fields_used"] == []
    assert payload["summary"]["outcome_forbidden_dependency_count"] == 0

    expected_outcomes = {
        "future_5d_return",
        "future_10d_return",
        "future_20d_return",
        "future_5d_max_drawdown",
        "future_10d_max_drawdown",
        "future_20d_max_drawdown",
        "future_20d_realized_vol",
        "future_20d_underperform_vs_static",
        "future_20d_recovery_failure",
        "future_gap_down_event",
    }
    assert expected_outcomes <= set(payload["independent_outcome_fields"])
    forbidden = set(payload["forbidden_outcome_fields"])
    assert expected_outcomes.isdisjoint(forbidden)
    assert all(
        row["forbidden_label_or_case_fields_used_in_outcome"] == []
        for row in payload["decision_outcomes"]
    )


def test_tail_risk_contract_boundary_quarantine_and_counterfactual(
    tail_risk_governance: dict[str, Any],
) -> None:
    contract = tail_risk_governance["payloads"]["TRADING-829"]
    boundary = tail_risk_governance["payloads"]["TRADING-830"]
    quarantine = tail_risk_governance["payloads"]["TRADING-831"]
    counterfactual = tail_risk_governance["payloads"]["TRADING-832"]

    for payload in [contract, boundary, quarantine, counterfactual]:
        _assert_safety(payload)

    assert contract["status"] == "WARN"
    assert contract["summary"]["direct_overlap_count"] == 0
    assert contract["summary"]["forbidden_dependency_count"] == 0
    assert contract["forbidden_dependency_matrix"]
    assert boundary["status"] == "TIME_BOUNDARY_BLOCKED"
    assert boundary["summary"]["blocked_feature_count"] > 0
    assert any(
        row["check_id"] == "forward_return_read" and row["issue_count"] > 0
        for row in boundary["forward_read_matrix"]
    )

    assert quarantine["metric_status"] == "TAINTED_BY_TRIGGER_LABEL_COUPLING"
    assert quarantine["usable_for_promotion"] is False
    assert quarantine["usable_for_paper_shadow"] is False
    assert quarantine["usable_for_production"] is False
    assert all(
        row["requires_independent_forward_validation"] for row in quarantine["quarantined_metrics"]
    )

    assert counterfactual["status"] in {
        "COUNTERFACTUAL_BETTER",
        "COUNTERFACTUAL_MIXED",
        "COUNTERFACTUAL_WORSE",
    }
    assert {
        "fallback_policy",
        "no_fallback_baseline",
        "static_allocation_baseline",
        "existing_best_baseline_if_available",
        "simple_trend_baseline",
        "qqq_100_baseline",
        "qqq_60_sgov_40_baseline",
        "qqq_70_sgov_30_baseline",
        "tqqq_50_sgov_50_baseline",
        "tqqq_25_sgov_75_baseline",
        "simple_200dma_risk_off_baseline",
        "simple_volatility_target_baseline",
        "equal_risk_qqq_sgov_baseline",
    } <= {row["policy_id"] for row in counterfactual["baseline_comparison"]}


def test_tail_risk_regime_sensitivity_error_cost_maturity_and_aging(
    tail_risk_governance: dict[str, Any],
) -> None:
    regime = tail_risk_governance["payloads"]["TRADING-833"]
    sensitivity = tail_risk_governance["payloads"]["TRADING-834"]
    error_cost = tail_risk_governance["payloads"]["TRADING-835"]
    evidence = tail_risk_governance["payloads"]["TRADING-836"]
    aging = tail_risk_governance["payloads"]["TRADING-837"]

    for payload in [regime, sensitivity, error_cost, evidence, aging]:
        _assert_safety(payload)

    assert regime["status"] in {
        "REGIME_ROBUST",
        "REGIME_MIXED",
        "REGIME_CONCENTRATED",
        "REGIME_INSUFFICIENT",
    }
    assert {"market_trend", "volatility", "drawdown_state"} <= {
        row["dimension"] for row in regime["regime_rows"]
    }
    assert regime["unavailable_regimes"]

    assert sensitivity["status"] in {
        "SENSITIVITY_STABLE",
        "SENSITIVITY_MIXED",
        "SENSITIVITY_FRAGILE",
    }
    assert {"trigger_threshold", "risk_score_cutoff", "lookback_window"} <= {
        row["parameter"] for row in sensitivity["sensitivity_surface"]
    }
    assert "stability_score" in sensitivity["metrics"]

    assert error_cost["status"] in {
        "ERROR_COST_ACCEPTABLE",
        "ERROR_COST_MIXED",
        "ERROR_COST_UNACCEPTABLE",
        "INSUFFICIENT_SAMPLE",
    }
    assert "false_positive_count" in error_cost["metrics"]
    assert "false_negative_count" in error_cost["metrics"]
    assert error_cost["worst_5_cases"]

    assert evidence["status"] in {
        "EVIDENCE_MATURE",
        "EVIDENCE_WEAK",
        "EVIDENCE_INSUFFICIENT",
        "EVIDENCE_BLOCKED",
    }
    assert {"triggered_count", "regime_min_sample_count"} <= {
        row["check_id"] for row in evidence["maturity_checks"]
    }

    assert aging["status"] in {
        "FORWARD_AGING_HEALTHY",
        "FORWARD_AGING_PENDING",
        "FORWARD_AGING_DETERIORATING",
        "INSUFFICIENT_NEW_SAMPLE",
    }
    assert aging["aging_bucket_summary"]["as_of"] == TEST_AS_OF.isoformat()
    assert aging["metrics"]["pending_outcomes"] >= 0


def test_tail_risk_leakage_promotion_trigger_v2_catalog_and_master_review(
    tail_risk_governance: dict[str, Any],
) -> None:
    leakage = tail_risk_governance["payloads"]["TRADING-838"]
    promotion = tail_risk_governance["payloads"]["TRADING-839"]
    trigger_v2 = tail_risk_governance["payloads"]["TRADING-840"]
    catalog = tail_risk_governance["payloads"]["TRADING-841"]
    master = tail_risk_governance["payloads"]["TRADING-842"]

    for payload in [leakage, promotion, trigger_v2, catalog, master]:
        _assert_safety(payload)

    assert leakage["status"] == "LEAKAGE_STRESS_BLOCKED"
    assert {
        "signal_lag_test",
        "label_permutation_test",
        "timestamp_boundary_test",
        "feature_availability_test",
        "forward_window_overlap_test",
        "trigger_outcome_overlap_test",
        "randomized_decision_time_test",
        "shuffled_outcome_sanity_test",
    } == {row["test_id"] for row in leakage["stress_tests"]}

    assert promotion["status"] == "PROMOTION_READINESS_BLOCKED"
    assert promotion["promotion_allowed"] is False
    assert promotion["paper_shadow_allowed"] is False
    assert promotion["production_allowed"] is False
    assert promotion["broker_action"] == "none"
    assert {"TRADING-827", "TRADING-830", "TRADING-838"} <= {
        row["task_id"] for row in promotion["blockers"]
    }

    assert trigger_v2["status"] == "CANDIDATE_BUILT"
    assert all(
        not row["uses_old_tail_risk_label"] and not row["uses_future_outcome"]
        for row in trigger_v2["candidate_trigger_v2_list"]
    )
    assert all(
        not row["depends_on_forbidden_label"] and not row["depends_on_future_outcome"]
        for row in trigger_v2["feature_dependency_list"]
    )

    assert catalog["status"] in {
        "FEATURE_CATALOG_READY",
        "FEATURE_CATALOG_PARTIAL",
        "FEATURE_CATALOG_BLOCKED",
    }
    assert all(row["usage_allowed_for_trigger"] for row in catalog["feature_catalog"])
    assert all(not row["usage_allowed_for_outcome"] for row in catalog["feature_catalog"])

    assert master["status"] == "TAIL_RISK_RESEARCH_MASTER_REVIEW_COMPLETE"
    assert master["final_recommendation"] in {
        "CONTINUE_RESEARCH",
        "REBUILD_TRIGGER",
        "REBUILD_OUTCOME",
        "KEEP_QUARANTINED",
        "PAUSE_RESEARCH",
        "DEPRECATE_CANDIDATE",
    }
    assert {"precision", "recall", "f1"} <= set(master["invalidated_metrics"])
    assert master["whether_shadow_possible_later"] is False
    assert master["whether_production_possible_later"] is False


def test_tail_risk_post_merge_evidence_review(
    tail_risk_governance: dict[str, Any],
    tmp_path: Path,
) -> None:
    post_merge = tail_risk_governance["payloads"]["POST_MERGE"]
    _assert_safety(post_merge)
    assert post_merge["status"] == "POST_MERGE_EVIDENCE_REVIEW_BLOCKED"
    assert post_merge["final_status"] == "POST_MERGE_EVIDENCE_REVIEW_BLOCKED"
    assert post_merge["promotion_allowed"] is False
    assert post_merge["paper_shadow_allowed"] is False
    assert post_merge["production_allowed"] is False
    assert post_merge["broker_action"] == "none"
    assert len(post_merge["artifact_summaries"]) == 15
    assert post_merge["dependency_artifact_summaries"]["TRADING-827"]["final_status"] == "BLOCKED"
    by_task = {row["task_id"]: row for row in post_merge["artifact_summaries"]}
    assert by_task["TRADING-828"]["final_status"] == "INDEPENDENT_FORWARD_VALIDATED"
    assert by_task["TRADING-828"]["sample_count"] > 0
    assert by_task["TRADING-831"]["final_status"] == "TAINTED_METRIC_QUARANTINED"
    assert by_task["TRADING-839"]["promotion_allowed"] is False
    assert by_task["TRADING-839"]["paper_shadow_allowed"] is False
    assert by_task["TRADING-839"]["production_allowed"] is False
    assert by_task["TRADING-842"]["sample_count"] == 15
    assert post_merge["template_only_artifacts"] == []
    assert post_merge["zero_sample_positive_artifacts"] == []
    assert post_merge["safety_violations"] == []
    checks = {row["check_id"]: row["passed"] for row in post_merge["special_checks"]}
    assert checks == {
        "trading_827_blocked_status_inherited": True,
        "trading_831_old_metrics_quarantined": True,
        "trading_839_hard_blocks_promotion": True,
        "trading_842_aggregates_827_through_841": True,
        "no_template_only_artifacts": True,
        "no_zero_sample_positive_conclusions": True,
        "independent_forward_validation_has_evidence_maturity": True,
    }
    assert {"TRADING-827", "TRADING-830", "TRADING-838", "TRADING-839"} == {
        row["task_id"] for row in post_merge["current_hard_blockers"]
    }

    paths = tail_risk_governance["paths"]
    output_root = tmp_path / "post_merge_cli"
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "tail-risk-post-merge-evidence-review",
            "--trigger-label-audit",
            str(paths["trigger_label"]),
            "--independent-forward",
            str(paths["independent_forward"]),
            "--contract-audit",
            str(paths["contract"]),
            "--boundary-audit",
            str(paths["boundary"]),
            "--quarantine",
            str(paths["quarantine"]),
            "--counterfactual",
            str(paths["counterfactual"]),
            "--regime-review",
            str(paths["regime_review"]),
            "--sensitivity-review",
            str(paths["sensitivity_review"]),
            "--error-cost",
            str(paths["error_cost"]),
            "--evidence-gate",
            str(paths["evidence_gate"]),
            "--aging-tracker",
            str(paths["aging_tracker"]),
            "--leakage-stress",
            str(paths["leakage_stress"]),
            "--promotion-gate",
            str(paths["promotion_gate"]),
            "--trigger-v2",
            str(paths["trigger_v2"]),
            "--feature-catalog",
            str(paths["feature_catalog"]),
            "--master-review",
            str(paths["master"]),
            "--output-root",
            str(output_root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (output_root / "tail_risk_post_merge_evidence_review.json").exists()
    assert (output_root / "tail_risk_post_merge_evidence_review.md").exists()


def test_tail_risk_followup_governance_artifacts(
    tail_risk_governance: dict[str, Any],
    tmp_path: Path,
) -> None:
    payloads = tail_risk_governance["payloads"]
    paths = tail_risk_governance["paths"]
    followup_ids = {
        "TRADING-843",
        "TRADING-844",
        "TRADING-845",
        "TRADING-846",
        "TRADING-847",
        "TRADING-848",
        "TRADING-850",
        "TRADING-851",
        "TRADING-852",
        "TRADING-853",
        "TRADING-854",
        "TRADING-856",
        "TRADING-857",
        "TRADING-858",
    }
    for task_id in followup_ids:
        payload = payloads[task_id]
        _assert_safety(payload)
        assert payload["task_id"] == task_id
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()

    snapshot = payloads["TRADING-843"]
    assert snapshot["status"] == "TAIL_RISK_GOVERNANCE_ARTIFACT_SNAPSHOT_BLOCKED"
    assert snapshot["metrics"]["artifact_count"] == 16
    assert snapshot["metrics"]["hard_blocker_count"] == 4
    assert snapshot["metrics"]["missing_artifact_count"] == 0

    status_matrix = payloads["TRADING-844"]
    assert status_matrix["status"] == "TAIL_RISK_RESEARCH_BLOCKED"
    assert status_matrix["overall_status"] == "TAIL_RISK_RESEARCH_BLOCKED"
    assert status_matrix["metrics"]["matrix_row_count"] == 16
    assert {row["task_id"] for row in status_matrix["current_hard_blockers"]} == {
        "TRADING-827",
        "TRADING-830",
        "TRADING-838",
        "TRADING-839",
    }

    real_data = payloads["TRADING-845"]
    assert real_data["status"] == "REAL_DATA_READY"
    assert real_data["metrics"]["fixture_fallback_count"] == 0
    assert real_data["metrics"]["input_missing_count"] == 0
    assert real_data["metrics"]["suspicious_result_count"] == 0

    forward_review = payloads["TRADING-846"]
    assert forward_review["status"] == "FORWARD_OUTCOME_USABLE_FOR_RESEARCH"
    assert forward_review["metrics"]["sample_count"] > 0
    assert forward_review["forbidden_dependency_check"]["status"] == "PASS"
    assert forward_review["baseline_comparison_available"] is True
    assert forward_review["metrics"]["promotion_allowed"] is False

    baseline_review = payloads["TRADING-847"]
    assert baseline_review["status"] == "COUNTERFACTUAL_BASELINE_DOMINATED"
    assert baseline_review["baseline_dominance_flag"] is True
    assert {
        "simple_trend_baseline",
        "equal_risk_qqq_sgov_baseline",
        "tqqq_50_sgov_50_baseline",
        "simple_volatility_target_baseline",
    } <= {row["policy_id"] for row in baseline_review["baseline_reviews"]}

    determinism = payloads["TRADING-848"]
    assert determinism["status"] == "DETERMINISTIC_PASS"
    assert determinism["metrics"]["stable_hash_match"] is True
    assert determinism["metrics"]["stable_sort_order"] is True
    assert determinism["first_stable_hash"] == determinism["second_stable_hash"]

    coverage_map = payloads["TRADING-850"]
    assert coverage_map["status"] == "TAIL_RISK_TASK_COVERAGE_MAP_COMPLETE"
    assert coverage_map["metrics"]["covered_task_count"] == 30
    assert Path(coverage_map["docs_path"]).exists()

    mutation = payloads["TRADING-851"]
    assert mutation["status"] == "HARD_BLOCK_MUTATION_PASS"
    assert mutation["metrics"]["failed_case_count"] == 0
    assert mutation["metrics"]["mutation_case_count"] == 7
    assert all(
        case["blocked"] and case["actual_status"] == case["expected_status"]
        for case in mutation["mutation_cases"]
    )

    registry = payloads["TRADING-852"]
    assert registry["status"] == "REPORT_REGISTRY_INTEGRITY_PASS"
    assert registry["metrics"]["failed_entry_count"] == 0
    assert registry["metrics"]["checked_entry_count"] >= 30

    daily = payloads["TRADING-853"]
    assert daily["status"] == "TAIL_RISK_DAILY_READING_SUMMARY_BLOCKED"
    assert daily["tail_risk_fallback_status"]["research_status"] == "TAIL_RISK_RESEARCH_BLOCKED"
    assert daily["tail_risk_fallback_status"]["promotion_allowed"] is False
    assert daily["broker_action"] == "none"

    input_quality = payloads["TRADING-854"]
    assert input_quality["status"] == "TRIGGER_V2_INPUT_QUALITY_PARTIAL"
    assert input_quality["metrics"]["partial_feature_count"] == 3
    assert {
        "market_breadth_proxy",
        "credit_liquidity_proxy",
        "vix_vxn_level_proxy",
    } <= {row["feature_name"] for row in input_quality["warnings"]}

    baseline_gate = payloads["TRADING-856"]
    assert baseline_gate["status"] == "BASELINE_DOMINATED_BLOCKED"
    assert baseline_gate["metrics"]["dominant_baseline_count"] > 0
    assert {row["policy_id"] for row in baseline_gate["blockers"]} >= {
        "simple_trend_baseline",
        "tqqq_50_sgov_50_baseline",
    }

    readiness = payloads["TRADING-857"]
    assert readiness["status"] == "TAIL_RISK_READINESS_RESEARCH_ONLY"
    assert readiness["readiness_score"] == 58
    assert readiness["readiness_band"]["label"] == "research-only"

    next_decision = payloads["TRADING-858"]
    assert next_decision["status"] == "TAIL_RISK_NEXT_DECISION_BLOCKED"
    assert next_decision["decision_answers"]["owner_next_action"] == "pause"
    assert next_decision["decision_answers"]["worth_building_trigger_v2"] is False
    assert Path(next_decision["docs_path"]).exists()

    output_root = tmp_path / "status_matrix_cli"
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "tail-risk-status-matrix",
            "--snapshot",
            str(paths["snapshot"]),
            "--output-root",
            str(output_root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (output_root / "tail_risk_status_matrix.json").exists()
    assert (output_root / "tail_risk_status_matrix.md").exists()


def test_reader_brief_tail_risk_safety_summary_renders_broker_action(
    tail_risk_governance: dict[str, Any],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    daily = tail_risk_governance["payloads"]["TRADING-853"]
    daily_path = (
        tmp_path
        / "outputs"
        / "research_strategies"
        / "value_surface_review"
        / "tail_risk_daily_reading_safety_summary.json"
    )
    daily_path.parent.mkdir(parents=True)
    daily_path.write_text(json.dumps(daily), encoding="utf-8")

    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    status = reader_brief._tail_risk_daily_reading_safety_summary()
    assert status["broker_action"] == "none"

    html = reader_brief.render_reader_brief_html({"tail_risk_fallback_status": status})
    assert "Tail-Risk Fallback Status" in html
    assert "broker_action" in html
    assert "none" in html
