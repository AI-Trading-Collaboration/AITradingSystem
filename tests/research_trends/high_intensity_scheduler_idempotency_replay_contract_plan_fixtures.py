from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_kill_switch_plan_fixtures import (
    build_high_intensity_scheduler_kill_switch_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_kill_switch_plan import (
    run_high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan,
)

__all__ = [
    "build_high_intensity_scheduler_idempotency_replay_contract_plan_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_idempotency_replay_contract_plan_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_kill_switch_plan_fixture(tmp_path)
    kill_switch_dir = tmp_path / "kill_switch_plan"
    kill_switch_docs_root = tmp_path / "kill_switch_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_kill_switch_plan(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
        owner_decision_dir=fixture["owner_decision_dir"],
        gap_closure_dir=fixture["gap_closure_dir"],
        hardening_backlog_dir=fixture["hardening_backlog_dir"],
        output_dir=kill_switch_dir,
        docs_root=kill_switch_docs_root,
    )
    return {
        **fixture,
        "kill_switch_dir": kill_switch_dir,
        "kill_switch_docs_root": kill_switch_docs_root,
    }
