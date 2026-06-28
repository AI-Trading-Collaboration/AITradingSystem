from __future__ import annotations

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_reopen_gate import evaluate_reopen_gate
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = PROJECT_ROOT / "config" / "research" / "first_layer_reopen_gate_policy.yaml"


def test_reopen_denied_when_only_target_path_improves() -> None:
    result = evaluate_reopen_gate(
        evidence={**_base_evidence(), "only_target_path_improves": True},
        policy=_policy(),
        owner_approval=True,
    )

    assert result["decision_status"] == "FIRST_LAYER_REOPEN_DENIED"
    assert "target_path_only_improvement" in result["blockers"]


def test_reopen_denied_when_2023_plus_only() -> None:
    result = evaluate_reopen_gate(
        evidence={**_base_evidence(), "depends_on_2023_plus": True},
        policy=_policy(),
        owner_approval=True,
    )

    assert result["decision_status"] == "FIRST_LAYER_REOPEN_DENIED"
    assert "2023_plus_only" in result["blockers"]


def test_reopen_denied_when_beta_dependency() -> None:
    result = evaluate_reopen_gate(
        evidence={**_base_evidence(), "beta_dependency": True},
        policy=_policy(),
        owner_approval=True,
    )

    assert result["decision_status"] == "FIRST_LAYER_REOPEN_DENIED"
    assert "beta_dependency" in result["blockers"]


def test_reopen_denied_when_pit_warning_model_ready() -> None:
    result = evaluate_reopen_gate(
        evidence={**_base_evidence(), "pit_warning_as_model_ready": True},
        policy=_policy(),
        owner_approval=True,
    )

    assert result["decision_status"] == "FIRST_LAYER_REOPEN_DENIED"
    assert "pit_warning_used_as_model_ready" in result["blockers"]


def test_reopen_requires_owner_approval() -> None:
    result = evaluate_reopen_gate(
        evidence=_base_evidence(),
        policy=_policy(),
        owner_approval=False,
    )

    assert result["decision_status"] == "FIRST_LAYER_REOPEN_DENIED"
    assert "owner_approval_missing" in result["blockers"]


def test_reopen_gate_cannot_enable_promotion() -> None:
    result = evaluate_reopen_gate(
        evidence=_base_evidence(),
        policy=_policy(),
        owner_approval=True,
    )

    assert result["reopen_allowed"] is True
    assert result["promotion_allowed"] is False
    assert result["paper_shadow_allowed"] is False
    assert result["production_allowed"] is False
    assert result["broker_action"] == "none"


def _base_evidence() -> dict[str, object]:
    return {
        "selection_rule_preregistered": True,
        "primary_window_coverage": True,
        "stress_2022_slice_not_worse": True,
        "depends_on_2023_plus": False,
        "beta_dependency": False,
        "tqqq_dependency": False,
        "actual_path_evidence_available": True,
        "only_target_path_improves": False,
        "net_of_cost_negative": False,
        "pit_warning_as_model_ready": False,
        "free_feature_final_status": "REOPEN_GATE_REVIEW_RECOMMENDED",
        "participation_final_status": "PARTICIPATION_PROXY_SUPPORTS_REOPEN_GATE",
    }


def _policy() -> dict[str, object]:
    raw = safe_load_yaml_path(POLICY_PATH)
    assert isinstance(raw, dict)
    return raw
