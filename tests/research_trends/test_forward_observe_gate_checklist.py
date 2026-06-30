from __future__ import annotations

from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    BLOCKED,
    NOT_READY,
    READY_RECOMMENDED,
    READY_WITH_WARNINGS,
    build_forward_observe_gate_checklist,
)


def _base_context(**overrides):
    context = {
        "source_state_from_2292": "SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE",
        "data_quality_status": "PASS",
        "sample_sufficiency_status": "SAMPLE_SUFFICIENT",
        "active_vs_inactive_comparison_label": "ACTIVE_SCOPE_OUTPERFORMS_REFERENCE",
        "false_risk_cap_cost_per_record": 0.01,
        "missed_upside_cost_per_record": 0.01,
        "risk_cap_capture_rate": 0.2,
        "scope_lineage_status": "PASS",
        "artifact_validation_status": "PASS",
        "asset_horizon_sparse_bucket_count": 0,
        "direction_min_sample": 20,
    }
    context.update(overrides)
    return context


def test_gate_checklist_ready_recommended() -> None:
    checklist = build_forward_observe_gate_checklist(_base_context())

    assert checklist["readiness_gate_status"] == READY_RECOMMENDED
    assert checklist["forward_observe_readiness_recommendation"] is True


def test_gate_checklist_ready_with_data_quality_warnings() -> None:
    checklist = build_forward_observe_gate_checklist(
        _base_context(data_quality_status="PASS_WITH_WARNINGS")
    )

    assert checklist["readiness_gate_status"] == READY_WITH_WARNINGS
    assert "DATA_QUALITY_PASS_WITH_WARNINGS" in checklist["readiness_warnings"]


def test_gate_checklist_not_ready_when_scope_is_not_better() -> None:
    checklist = build_forward_observe_gate_checklist(
        _base_context(active_vs_inactive_comparison_label="ACTIVE_SCOPE_WORSE")
    )

    assert checklist["readiness_gate_status"] == NOT_READY
    assert checklist["forward_observe_readiness_recommendation"] is False


def test_gate_checklist_blocks_when_2292_state_not_forward_observe() -> None:
    checklist = build_forward_observe_gate_checklist(
        _base_context(source_state_from_2292="SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED")
    )

    assert checklist["readiness_gate_status"] == BLOCKED
    assert checklist["readiness_review_status"] == "BLOCKED_BY_2292_STATE"


def test_gate_checklist_blocks_data_quality_fail() -> None:
    checklist = build_forward_observe_gate_checklist(_base_context(data_quality_status="FAIL"))

    assert checklist["readiness_gate_status"] == BLOCKED


def test_gate_checklist_sample_blocked_is_not_ready() -> None:
    checklist = build_forward_observe_gate_checklist(
        _base_context(sample_sufficiency_status="SAMPLE_INSUFFICIENT_FOR_SUBGROUPS")
    )

    assert checklist["readiness_gate_status"] == NOT_READY
    assert "SAMPLE_NOT_READY" in checklist["readiness_blockers"]


def test_gate_checklist_high_false_cost_is_not_ready() -> None:
    checklist = build_forward_observe_gate_checklist(
        _base_context(false_risk_cap_cost_per_record=0.2)
    )

    assert checklist["readiness_gate_status"] == NOT_READY
    assert "FALSE_RISK_CAP_COST_TOO_HIGH" in checklist["readiness_blockers"]
