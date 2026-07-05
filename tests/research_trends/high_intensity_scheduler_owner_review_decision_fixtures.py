from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_audit_package_fixtures import (
    build_high_intensity_scheduler_audit_package_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_audit_package import (
    run_high_intensity_risk_cap_observe_only_scheduler_audit_package,
)

__all__ = [
    "build_high_intensity_scheduler_owner_review_decision_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_owner_review_decision_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_audit_package_fixture(tmp_path)
    audit_package_dir = tmp_path / "audit_package"
    audit_docs_root = tmp_path / "audit_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_audit_package(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        replay_validation_dir=fixture["replay_validation_dir"],
        output_dir=audit_package_dir,
        docs_root=audit_docs_root,
    )
    return {
        **fixture,
        "audit_package_dir": audit_package_dir,
        "audit_docs_root": audit_docs_root,
    }
