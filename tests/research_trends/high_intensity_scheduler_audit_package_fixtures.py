from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_manual_run_replay_validation_fixtures import (
    build_high_intensity_scheduler_manual_run_replay_validation_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    run_high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation,
)

__all__ = [
    "build_high_intensity_scheduler_audit_package_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_audit_package_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_manual_run_replay_validation_fixture(
        tmp_path
    )
    replay_validation_dir = tmp_path / "replay_validation"
    replay_docs_root = tmp_path / "replay_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        manual_run_dry_run_dir=fixture["manual_run_dry_run_dir"],
        output_dir=replay_validation_dir,
        docs_root=replay_docs_root,
    )
    return {
        **fixture,
        "replay_validation_dir": replay_validation_dir,
        "replay_docs_root": replay_docs_root,
    }
