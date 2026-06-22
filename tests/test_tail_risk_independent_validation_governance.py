from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from controlled_strategy_batch_helpers import (
    TEST_AS_OF,
    _assert_safety,
    _run_tail_risk_falsification_inputs,
)

from ai_trading_system.controlled_strategy_batch import (
    run_tail_risk_decision_time_boundary_audit,
    run_tail_risk_evidence_maturity_gate,
    run_tail_risk_fallback_counterfactual_validation,
    run_tail_risk_fallback_error_cost_ledger,
    run_tail_risk_forward_aging_tracker,
    run_tail_risk_forward_outcome_contract_audit,
    run_tail_risk_independent_forward_outcome_validation,
    run_tail_risk_independent_trigger_v2_builder,
    run_tail_risk_leakage_stress_suite,
    run_tail_risk_promotion_readiness_gate,
    run_tail_risk_regime_stratified_forward_outcome_review,
    run_tail_risk_research_master_review,
    run_tail_risk_tainted_metric_quarantine,
    run_tail_risk_threshold_sensitivity_review,
    run_tail_risk_trigger_feature_availability_catalog,
    run_tail_risk_trigger_label_independence_audit,
)


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
