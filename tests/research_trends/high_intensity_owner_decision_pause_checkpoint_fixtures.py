from __future__ import annotations

from pathlib import Path

from high_intensity_promotion_blocker_matrix_fixtures import (
    build_high_intensity_promotion_blocker_matrix_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    run_high_intensity_risk_cap_observe_only_promotion_blocker_matrix,
)

__all__ = [
    "build_high_intensity_owner_decision_pause_checkpoint_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_owner_decision_pause_checkpoint_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_promotion_blocker_matrix_fixture(tmp_path)
    promotion_blocker_dir = tmp_path / "promotion_blocker_matrix"
    promotion_blocker_docs_root = tmp_path / "promotion_blocker_docs"
    run_high_intensity_risk_cap_observe_only_promotion_blocker_matrix(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
        owner_decision_dir=fixture["owner_decision_dir"],
        gap_closure_dir=fixture["gap_closure_dir"],
        hardening_backlog_dir=fixture["hardening_backlog_dir"],
        kill_switch_dir=fixture["kill_switch_dir"],
        idempotency_replay_dir=fixture["idempotency_replay_dir"],
        event_append_dir=fixture["event_append_dir"],
        outcome_binding_dir=fixture["outcome_binding_dir"],
        paper_shadow_scope_dir=fixture["paper_shadow_scope_dir"],
        production_broker_dir=fixture["production_broker_dir"],
        output_dir=promotion_blocker_dir,
        docs_root=promotion_blocker_docs_root,
    )
    return {
        **fixture,
        "promotion_blocker_dir": promotion_blocker_dir,
        "promotion_blocker_docs_root": promotion_blocker_docs_root,
    }
