from __future__ import annotations

from pathlib import Path

from high_intensity_event_append_contract_plan_fixtures import (
    build_high_intensity_event_append_contract_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_event_append_contract_plan import (
    run_high_intensity_risk_cap_observe_only_event_append_contract_plan,
)

__all__ = [
    "build_high_intensity_outcome_binding_contract_plan_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_outcome_binding_contract_plan_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_event_append_contract_plan_fixture(tmp_path)
    event_append_dir = tmp_path / "event_append_plan"
    event_append_docs_root = tmp_path / "event_append_docs"
    run_high_intensity_risk_cap_observe_only_event_append_contract_plan(
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
        output_dir=event_append_dir,
        docs_root=event_append_docs_root,
    )
    return {
        **fixture,
        "event_append_dir": event_append_dir,
        "event_append_docs_root": event_append_docs_root,
    }
