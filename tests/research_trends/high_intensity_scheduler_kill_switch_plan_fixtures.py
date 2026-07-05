from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_hardening_backlog_fixtures import (
    build_high_intensity_scheduler_hardening_backlog_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_hardening_backlog import (
    run_high_intensity_risk_cap_observe_only_scheduler_hardening_backlog,
)

__all__ = [
    "build_high_intensity_scheduler_kill_switch_plan_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_kill_switch_plan_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_hardening_backlog_fixture(tmp_path)
    hardening_backlog_dir = tmp_path / "hardening_backlog"
    hardening_docs_root = tmp_path / "hardening_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_hardening_backlog(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
        owner_decision_dir=fixture["owner_decision_dir"],
        gap_closure_dir=fixture["gap_closure_dir"],
        output_dir=hardening_backlog_dir,
        docs_root=hardening_docs_root,
    )
    return {
        **fixture,
        "hardening_backlog_dir": hardening_backlog_dir,
        "hardening_docs_root": hardening_docs_root,
    }
