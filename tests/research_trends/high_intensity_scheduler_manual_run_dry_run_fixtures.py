from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_manual_review_gate_fixtures import (
    build_high_intensity_scheduler_manual_review_gate_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_manual_review_gate import (
    run_high_intensity_risk_cap_observe_only_scheduler_manual_review_gate,
)

__all__ = [
    "build_high_intensity_scheduler_manual_run_dry_run_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_manual_run_dry_run_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_manual_review_gate_fixture(tmp_path)
    manual_review_gate_dir = tmp_path / "manual_review_gate"
    manual_review_docs_root = tmp_path / "manual_review_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_manual_review_gate(
        disabled_wiring_dir=fixture["disabled_wiring_dir"],
        smoke_dry_run_dir=fixture["smoke_dry_run_dir"],
        output_dir=manual_review_gate_dir,
        docs_root=manual_review_docs_root,
    )
    return {
        **fixture,
        "manual_review_gate_dir": manual_review_gate_dir,
        "manual_review_docs_root": manual_review_docs_root,
    }
