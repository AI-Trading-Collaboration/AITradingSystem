from __future__ import annotations

from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path

DO_NOT_DERISK_RULE_PATH = Path("config/research/do_not_de_risk_selection_rule.yaml")
RISK_ON_VETO_POLICY_PATH = Path("config/research/risk_on_veto_policy.yaml")
SIGNAL_USAGE_PATH = Path("inputs/research_reviews/first_layer_signal_usage_matrix_v2.yaml")
FINAL_MATRIX_PATH = Path(
    "inputs/research_reviews/boundary_aware_two_layer_optimization_framework_final_matrix.yaml"
)


def test_every_optimization_track_has_pre_registered_selection_rule() -> None:
    do_not_derisk = _load_yaml(DO_NOT_DERISK_RULE_PATH)
    risk_on_veto = _load_yaml(RISK_ON_VETO_POLICY_PATH)

    for policy in (do_not_derisk, risk_on_veto):
        assert policy["selection_rule"]["pre_registered"] is True
        assert policy["selection_rule"]["result_can_be_candidate_without_rule"] is False
        assert policy["safety_boundary"]["promotion_allowed"] is False
        assert policy["safety_boundary"]["broker_action"] == "none"


def test_result_without_selection_rule_cannot_be_candidate() -> None:
    final = _load_yaml(FINAL_MATRIX_PATH)

    assert final["summary"]["allocation_candidate_count"] == 0
    assert final["promotion_allowed"] is False
    assert final["research_audit_metadata"]["candidate_count"] == 0
    assert (
        final["research_audit_metadata"]["pre_registered_selection_rule"]
        == "boundary_aware_two_layer_framework_no_candidate_selection"
    )


def test_diagnostic_only_signal_cannot_be_allocation_candidate() -> None:
    matrix = _load_yaml(SIGNAL_USAGE_PATH)
    signals = {
        str(row["signal_name"]): row
        for row in matrix["signals"]
        if isinstance(row, dict)
    }

    for name in ("stay_constructive", "add_risk", "risk_on_diagnostic"):
        assert signals[name]["diagnostic_only"] is True
        assert "allocation" in signals[name]["blocked_usage"]
        assert "promotion" in signals[name]["blocked_usage"]
        assert signals[name]["can_emit_weights"] is False


def test_closeout_records_all_allowed_final_status_flags() -> None:
    final = _load_yaml(FINAL_MATRIX_PATH)

    assert "BOUNDARY_CONTRACT_READY" in final["status_flags"]
    assert "CHANNEL_POLICY_READY" in final["status_flags"]
    assert "BASE_OVERLAY_VETO_FRAMEWORK_READY" in final["status_flags"]
    assert "INDICATOR_FAMILY_ABLATION_READY" in final["status_flags"]
    assert final["dynamic_promotion_status"] == "BLOCKED"
    assert final["broker_action"] == "none"


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
