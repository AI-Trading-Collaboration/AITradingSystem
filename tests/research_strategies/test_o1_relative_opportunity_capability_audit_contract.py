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


def test_decision_pack_and_task_register_preserve_owner_gate() -> None:
    decision_pack = DECISION_PACK_PATH.read_text(encoding="utf-8")
    task_register = TASK_REGISTER_PATH.read_text(encoding="utf-8")
    assert "OWNER_REVIEW_REQUIRED_NOT_ACTIVE" in decision_pack
    assert "approve_o1_m1_ridge_cross_asset_state_single_family_v1" in decision_pack
    assert "require_new_o1_model_feature_family_pack_v1" in decision_pack
    assert "hold_o1_capability_audit_v1" in decision_pack
    assert "`receipt.json` 的实际 bytes" in decision_pack
    assert (
        "|TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT|"
        in task_register
    )
    assert "|P0|BLOCKED_OWNER_INPUT|" in next(
        line
        for line in task_register.splitlines()
        if line.startswith(
            "|TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT|"
        )
    )


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
