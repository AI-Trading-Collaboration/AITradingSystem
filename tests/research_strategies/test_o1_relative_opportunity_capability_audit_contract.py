from __future__ import annotations

import hashlib
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROPOSAL_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "o1_relative_opportunity_capability_audit_v1_proposal.yaml"
)
ACTIVE_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "o1_relative_opportunity_capability_audit_v1.yaml"
)
S4_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2463_S4_O1_Relative_Opportunity_Spread_Preregistration_Freeze.md"
)
HISTORICAL_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "decision_target_capability_audit_model_ladder_v1.yaml"
)
DECISION_PACK_PATH = (
    PROJECT_ROOT
    / "docs"
    / "requirements"
    / "TRADING-2464_O1_Model_Feature_Family_Owner_Decision_Pack.md"
)
TASK_REGISTER_PATH = PROJECT_ROOT / "docs" / "task_register.md"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_yaml(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_proposal_is_inactive_and_binds_exact_authority() -> None:
    proposal = _load_yaml(PROPOSAL_PATH)
    target = proposal["target_authority"]
    dq = proposal["dq_authority"]
    execution = proposal["execution_commitment"]
    assert isinstance(target, dict)
    assert isinstance(dq, dict)
    assert isinstance(execution, dict)

    assert proposal["status"] == "OWNER_REVIEW_REQUIRED_NOT_ACTIVE"
    assert proposal["activation_allowed"] is False
    assert proposal["required_owner_decision"] == (
        "owner_decision:TRADING-2464:2026-07-30:"
        "approve_o1_m1_ridge_cross_asset_state_single_family_v1"
    )
    assert target["policy_id"] == "TRADING_2463_O1_S4_PILOT_V1"
    assert target["sha256"] == _sha256(S4_PATH)
    assert target["primary_horizon_common_sessions"] == 5
    assert target["sensitivity_horizons"] == []
    assert execution == {
        "exact_base_sha": "c6a88ecb337d2cd5ea231bd3c56f2f2bb8269d53",
        "historical_seen_only": True,
        "new_o1_result_read": False,
        "prospective_accessed": False,
        "model_training_executed": False,
        "coverage_audit_executed": False,
    }

    receipt_path = PROJECT_ROOT / str(dq["required_receipt_path"])
    assert dq["local_receipt_bytes_present_at_proposal_time"] is receipt_path.exists()
    assert receipt_path.exists() is False
    assert str(dq["execution_gate"]).startswith("BLOCK until")


def test_recovery_audit_binds_exact_chain_without_materializing() -> None:
    proposal = _load_yaml(PROPOSAL_PATH)
    recovery = proposal["dq_recovery_audit"]
    assert isinstance(recovery, dict)
    assert recovery["status"] == "PASS_EXACT_CHAIN_RECOVERABLE_NOT_MATERIALIZED"
    assert recovery["source_workspace_kind"] == "PERMANENT_RUNTIME_CLONE"
    assert recovery["source_portability"] == (
        "LOCAL_EVIDENCE_SOURCE_NOT_REPOSITORY_AUTHORITY"
    )
    assert recovery["receipt_sha256_verified"] is True
    assert recovery["authorization_sha256_verified"] is True
    assert recovery["publication_pointer_sha256_verified"] is True
    assert recovery["publication_transaction_sha256_verified"] is True
    inputs = recovery["immutable_inputs"]
    assert isinstance(inputs, dict)
    assert set(inputs) == {"prices", "rates", "secondary_prices"}
    assert all(
        isinstance(item, dict) and item["verified"] is True
        for item in inputs.values()
    )
    assert recovery["historical_acceptance_contract_validation"] == "PASS"
    assert recovery["current_runtime_live_projection_matches_receipt_inputs"] is False
    assert recovery["current_main_live_projection_matches_receipt_inputs"] is False
    assert recovery["direct_copy_over_live_data_raw_allowed"] is False
    assert recovery["materialization_executed"] is False
    assert recovery["dq_rerun_executed"] is False
    assert recovery["production_effect"] == "none"
    assert recovery["broker_action"] == "none"


def test_recommended_family_exactly_reuses_reviewed_m1_prefix() -> None:
    proposal = _load_yaml(PROPOSAL_PATH)
    historical = _load_yaml(HISTORICAL_POLICY_PATH)
    family = proposal["recommended_single_family"]
    feature_policy = historical["feature_policy"]
    model_policy = historical["model_policy"]
    assert isinstance(family, dict)
    assert isinstance(feature_policy, dict)
    assert isinstance(model_policy, dict)

    assert family["historical_policy_sha256"] == _sha256(HISTORICAL_POLICY_PATH)
    assert family["model_id"] == model_policy["primary_classification_model"]
    assert family["family_prefix"] == model_policy["primary_classification_feature_prefix"]
    assert family["standardization_zero_scale_epsilon"] == model_policy[
        "standardization_zero_scale_epsilon"
    ]

    models = model_policy["models"]
    assert isinstance(models, list)
    m1 = next(
        model
        for model in models
        if isinstance(model, dict) and model["model_id"] == "M1_RIDGE_LINEAR"
    )
    assert family["ridge_penalty"] == m1["ridge_penalty"]

    order = feature_policy["family_order"]
    features = feature_policy["features"]
    assert isinstance(order, list)
    assert isinstance(features, list)
    prefix_end = order.index("CROSS_ASSET_STATE")
    allowed_families = set(order[: prefix_end + 1])
    expected_feature_ids = [
        feature["feature_id"]
        for feature in features
        if isinstance(feature, dict) and feature["family"] in allowed_families
    ]
    assert family["feature_id_count"] == 28
    assert family["feature_ids"] == expected_feature_ids
    assert family["interaction_terms_allowed"] is False
    assert family["automatic_hyperparameter_search_allowed"] is False


def test_owner_a_decision_and_mechanical_closeout_remain_registered() -> None:
    decision_pack = DECISION_PACK_PATH.read_text(encoding="utf-8")
    task_register = TASK_REGISTER_PATH.read_text(encoding="utf-8")
    assert "OWNER_APPROVED_A_SERIAL_CONTRACT_FROZEN" in decision_pack
    assert "approve_o1_m1_ridge_cross_asset_state_single_family_v1" in decision_pack
    assert "require_new_o1_model_feature_family_pack_v1" in decision_pack
    assert "hold_o1_capability_audit_v1" in decision_pack
    assert "`receipt.json` 的实际 bytes" in decision_pack
    assert "PASS_EXACT_CHAIN_RECOVERABLE_NOT_MATERIALIZED" in decision_pack
    assert "`materialize_isolated_candidate`" in decision_pack
    assert (
        "|TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT|"
        in task_register
    )
    assert "|P0|BASELINE_DONE|" in next(
        line
        for line in task_register.splitlines()
        if line.startswith(
            "|TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT|"
        )
    )


def test_active_policy_binds_owner_exact_base_s4_and_dq_transaction() -> None:
    policy = _load_yaml(ACTIVE_POLICY_PATH)
    authority = policy["authority"]
    binding = policy["execution_binding"]
    data = policy["data_contract"]
    assert isinstance(authority, dict)
    assert isinstance(binding, dict)
    assert isinstance(data, dict)

    assert policy["status"] == (
        "CLOSED_INSUFFICIENT_COVERAGE_OR_DQ"
    )
    assert policy["owner_decision"] == (
        "owner_decision:TRADING-2464:2026-07-30:"
        "approve_o1_m1_ridge_cross_asset_state_single_family_v1"
    )
    assert authority["contract_freeze_source_base_sha"] == (
        "428cfa78149a7f037e8cfdeee8d2646833f413a5"
    )
    proposal = authority["proposal_predecessor"]
    target = authority["target_policy"]
    historical = authority["historical_model_policy"]
    assert isinstance(proposal, dict)
    assert isinstance(target, dict)
    assert isinstance(historical, dict)
    assert proposal["sha256"] == _sha256(PROPOSAL_PATH)
    assert target["sha256"] == _sha256(S4_PATH)
    assert historical["sha256"] == _sha256(HISTORICAL_POLICY_PATH)
    assert binding["contract_integration_commit_required_before_real_data_access"] is True
    assert binding["real_coverage_read_allowed_now"] is False
    assert binding["coverage_attempt_consumed"] is True
    assert binding["canonical_run_allowed_now"] is False
    assert binding["model_training_allowed_now"] is False
    assert binding["maximum_canonical_runs"] == 1

    coverage = policy["coverage_evidence"]
    assert isinstance(coverage, dict)
    assert coverage["status"] == "BLOCKED_INSUFFICIENT_COVERAGE_OR_DQ"
    assert coverage["source_commit_sha"] == (
        "1bf9fb13245064ec2a505ea864e2e127ad445d41"
    )
    assert coverage["report"] == {
        "report_id": "o1_coverage_report_9b5708c6c36ac69cc7355fee8567a953",
        "path": (
            "outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/"
            "o1_coverage_only_v1/coverage_report.json"
        ),
        "sha256": (
            "bbed79b499b57274dd49bede0c37219894233964732fcde5656626933781ada7"
        ),
        "byte_size": 29645,
    }
    assert coverage["gate"] == {
        "gate_id": "o1_coverage_gate_b240158b3b7d3211ad51852217aa6d93",
        "path": (
            "outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/"
            "o1_coverage_only_v1/coverage_gate.json"
        ),
        "sha256": (
            "a97ee44832a41aeb90a6f9a18b0358eb81cefec4d491438deb6fd27b624f31b8"
        ),
        "byte_size": 1983,
    }
    assert {
        row["check_id"] for row in coverage["failed_mandatory_checks"]
    } == {
        "F01_TRAIN_EFFECTIVE_SAMPLE",
        "F02_TEST_EFFECTIVE_SAMPLE",
        "REGIME_VOLATILITY_HIGH_FOLD_COUNT",
        "REGIME_CURRENT_DRAWDOWN_LOW_EFFECTIVE_SAMPLE",
    }
    assert coverage["mechanical_classification"] == "INSUFFICIENT_COVERAGE_OR_DQ"
    assert all(value is False for value in coverage["next_authorization"].values())
    assert coverage["single_run_consumed"] is True
    assert coverage["result_driven_retry_allowed"] is False

    receipt = data["historical_receipt"]
    publication = data["publication"]
    recovery = data["recovery"]
    assert isinstance(receipt, dict)
    assert isinstance(publication, dict)
    assert isinstance(recovery, dict)
    assert receipt["receipt_sha256"] == (
        "6a4319f15f65a06345f08965c04cada01083d00a478e06febfdfd21f5ef56a58"
    )
    assert publication["transaction_id"] == (
        "download_txn_80b403268d6023acaf33b0608630b908"
    )
    assert publication["transaction_sha256"] == (
        "9ed6e7ec705633bec21e032a25f48ca93fd7ef0ead899bbe857b0f30591d7778"
    )
    assert set(publication["immutable_members"]) == {
        "prices",
        "rates",
        "secondary_prices",
    }
    assert recovery["source_workspace_mutation_allowed"] is False
    assert recovery["isolated_candidate_required"] is True
    assert recovery["overwrite_live_data_raw_allowed"] is False


def test_active_policy_exactly_freezes_s4_split_coverage_and_primary_gate() -> None:
    policy = _load_yaml(ACTIVE_POLICY_PATH)
    target = policy["target_contract"]
    split = policy["split_contract"]
    coverage = policy["coverage_contract"]
    metric = policy["metric_contract"]
    assert isinstance(target, dict)
    assert isinstance(split, dict)
    assert isinstance(coverage, dict)
    assert isinstance(metric, dict)

    assert target == {
        "target_id": "RELATIVE_OPPORTUNITY_SPREAD",
        "form": "CONTINUOUS",
        "label": "QQQ_FORWARD_TOTAL_RETURN - SGOV_FORWARD_TOTAL_RETURN",
        "unit": "decimal_total_return_spread",
        "primary_horizon_common_sessions": 5,
        "sensitivity_horizons": [],
        "decision_cutoff": "common_session_close_publication_complete",
        "label_interval_start": "next_common_session",
        "label_interval_end": "fifth_common_session",
        "label_available_on": "fifth_common_session",
        "source_assets": ["QQQ", "SGOV"],
        "reference_only_assets": ["SPY"],
        "qld_allowed": False,
        "tqqq_allowed": False,
    }
    assert split["initial_train_raw_rows"] == 504
    assert split["outer_test_raw_rows"] == 126
    assert split["final_partial_raw_row_floor"] == 63
    assert split["embargo_common_sessions"] == 5
    assert coverage["minimum_completed_outer_folds"] == 5
    assert coverage["minimum_train_effective_sample_per_fold"] == 100
    assert coverage["minimum_test_effective_sample_per_full_fold"] == 24
    assert coverage["minimum_test_effective_sample_final_partial_fold"] == 12
    assert coverage["minimum_total_oof_effective_sample"] == 120
    assert coverage["mandatory_regime_cell_effective_sample"] == 15
    assert coverage["mandatory_regime_cell_fold_count"] == 3
    assert coverage["mandatory_event_family_episode_count"] == 3
    assert coverage["mandatory_event_family_fold_count"] == 2
    assert metric["primary_metric"] == "OOF_MSE_SKILL"
    assert metric["point_estimate_floor"] == 0.02
    assert metric["moving_block_bootstrap_sessions"] == 5
    assert metric["one_sided_confidence_level"] == 0.95
    assert metric["minimum_positive_completed_folds"] == 4
    assert metric["worst_completed_fold_skill_floor"] == -0.10


def test_active_policy_reuses_exact_model_feature_prefix_and_forbids_search() -> None:
    active = _load_yaml(ACTIVE_POLICY_PATH)
    historical = _load_yaml(HISTORICAL_POLICY_PATH)
    contract = active["model_feature_contract"]
    feature_policy = historical["feature_policy"]
    model_policy = historical["model_policy"]
    assert isinstance(contract, dict)
    assert isinstance(feature_policy, dict)
    assert isinstance(model_policy, dict)

    order = feature_policy["family_order"]
    features = feature_policy["features"]
    assert isinstance(order, list)
    assert isinstance(features, list)
    prefix_end = order.index("CROSS_ASSET_STATE")
    expected_families = set(order[: prefix_end + 1])
    expected_ids = [
        feature["feature_id"]
        for feature in features
        if isinstance(feature, dict) and feature["family"] in expected_families
    ]
    assert contract["model_id"] == "M1_RIDGE_LINEAR"
    assert contract["ridge_penalty"] == 1.0
    assert contract["family_prefix"] == "CROSS_ASSET_STATE"
    assert contract["feature_ids"] == expected_ids
    assert contract["interaction_terms_allowed"] is False
    assert contract["automatic_hyperparameter_search_allowed"] is False
    assert contract["feature_subset_selection_allowed"] is False


def test_active_policy_freezes_ledgers_falsification_and_mechanical_classes() -> None:
    policy = _load_yaml(ACTIVE_POLICY_PATH)
    event = policy["event_contract"]
    attempt = policy["attempt_ledger_contract"]
    falsification = policy["falsification_contract"]
    classification = policy["classification_contract"]
    assert isinstance(event, dict)
    assert isinstance(attempt, dict)
    assert isinstance(falsification, dict)
    assert isinstance(classification, dict)

    assert event["mandatory_event_families"] == ["FOMC", "CPI", "NFP"]
    assert event["exact_event_ledger_required_before_real_coverage_read"] is True
    assert event["current_view_or_reconstructed_unknown_known_at_allowed"] is False
    assert attempt["append_only"] is True
    assert attempt["current_attempt_family_id"] == (
        "O1_M1_RIDGE_CROSS_ASSET_STATE_V1"
    )
    contamination = attempt["historical_contamination"]
    assert isinstance(contamination, dict)
    assert contamination["family_known"] is True
    assert contamination["o1_results_known"] is True
    assert contamination["independent_novel_family_claim_allowed"] is False
    axes = falsification["axes"]
    assert isinstance(axes, dict)
    assert set(axes) == {
        "exact_reconstruction",
        "feature_timing_lag",
        "purge_embargo_stress",
        "fold_jackknife_influence",
        "regime_concentration",
        "event_concentration",
        "autocorrelation_preserving_placebo",
        "target_boundary_perturbation",
        "simple_baseline_increment",
        "multiple_testing",
        "dq_lineage_closure",
    }
    assert classification["allowed_classes"] == [
        "MEASURABLE_RELATIVE_OPPORTUNITY_SKILL",
        "NO_MEASURABLE_SKILL",
        "INSUFFICIENT_COVERAGE_OR_DQ",
        "INSUFFICIENT_ROBUSTNESS_EVIDENCE",
    ]
    assert classification["downstream_authorization_from_positive_class"] is False


def test_proposal_keeps_every_downstream_action_disabled() -> None:
    proposal = _load_yaml(PROPOSAL_PATH)
    safety = proposal["safety"]
    contamination = proposal["multiple_testing_and_contamination"]
    assert isinstance(safety, dict)
    assert isinstance(contamination, dict)
    assert safety == {
        "decision_value_audit_started": False,
        "risk_overlay_created": False,
        "candidate_family_created": False,
        "strategy_backtest_executed": False,
        "target_weights_generated": False,
        "qld_automatic_selection_enabled": False,
        "paper_shadow_changed": False,
        "promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    assert contamination["prior_family_known"] is True
    assert contamination["prior_o1_results_known"] is True
    assert contamination["treat_as_independent_novel_family"] is False
    assert contamination["new_family_after_result_read_allowed"] is False


def test_active_contract_records_coverage_but_keeps_downstream_actions_disabled() -> None:
    policy = _load_yaml(ACTIVE_POLICY_PATH)
    safety = policy["safety"]
    assert isinstance(safety, dict)
    assert safety == {
        "new_o1_result_read": True,
        "coverage_audit_executed": True,
        "model_training_executed": False,
        "prospective_accessed": False,
        "decision_value_audit_started": False,
        "risk_overlay_created": False,
        "candidate_family_created": False,
        "strategy_backtest_executed": False,
        "target_weights_generated": False,
        "qld_automatic_selection_enabled": False,
        "paper_shadow_changed": False,
        "promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
