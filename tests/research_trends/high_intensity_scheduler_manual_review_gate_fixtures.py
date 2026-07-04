from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_smoke_dry_run_fixtures import (
    build_high_intensity_scheduler_smoke_dry_run_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    run_high_intensity_risk_cap_observe_only_scheduler_smoke_dry_run,
)

__all__ = [
    "build_high_intensity_scheduler_manual_review_gate_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_manual_review_gate_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_smoke_dry_run_fixture(tmp_path)
    smoke_dry_run_dir = tmp_path / "smoke_dry_run"
    smoke_docs_root = tmp_path / "smoke_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_smoke_dry_run(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        output_dir=smoke_dry_run_dir,
        docs_root=smoke_docs_root,
    )
    return {
        **fixture,
        "smoke_dry_run_dir": smoke_dry_run_dir,
        "smoke_docs_root": smoke_docs_root,
    }
