from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_owner_review_decision_fixtures import (
    build_high_intensity_scheduler_owner_review_decision_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_owner_review_decision import (
    run_high_intensity_risk_cap_observe_only_scheduler_owner_review_decision,
)

__all__ = [
    "build_high_intensity_scheduler_gap_closure_plan_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_gap_closure_plan_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_owner_review_decision_fixture(tmp_path)
    owner_decision_dir = tmp_path / "owner_decision"
    owner_docs_root = tmp_path / "owner_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_owner_review_decision(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        audit_package_dir=fixture["audit_package_dir"],
        output_dir=owner_decision_dir,
        docs_root=owner_docs_root,
    )
    return {
        **fixture,
        "owner_decision_dir": owner_decision_dir,
        "owner_docs_root": owner_docs_root,
    }
