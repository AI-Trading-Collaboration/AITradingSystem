from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_manual_run_dry_run_fixtures import (
    build_high_intensity_scheduler_manual_run_dry_run_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_dry_run import (
    run_high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run,
)

__all__ = [
    "build_high_intensity_scheduler_manual_run_replay_validation_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_manual_run_replay_validation_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_manual_run_dry_run_fixture(tmp_path)
    manual_run_dry_run_dir = tmp_path / "manual_run_dry_run"
    manual_run_docs_root = tmp_path / "manual_run_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        manual_review_gate_dir=fixture["manual_review_gate_dir"],
        output_dir=manual_run_dry_run_dir,
        docs_root=manual_run_docs_root,
    )
    return {
        **fixture,
        "manual_run_dry_run_dir": manual_run_dry_run_dir,
        "manual_run_docs_root": manual_run_docs_root,
    }
