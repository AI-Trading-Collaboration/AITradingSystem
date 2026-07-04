from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_disabled_wiring_fixtures import (
    build_high_intensity_scheduler_disabled_wiring_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_disabled_wiring import (
    run_high_intensity_risk_cap_observe_only_scheduler_disabled_wiring,
)

__all__ = [
    "build_high_intensity_scheduler_smoke_dry_run_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_smoke_dry_run_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_disabled_wiring_fixture(tmp_path)
    disabled_wiring_dir = tmp_path / "disabled_wiring"
    disabled_wiring_docs_root = tmp_path / "disabled_wiring_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_disabled_wiring(
        wiring_plan_dir=fixture["wiring_plan_dir"],
        scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
        output_dir=disabled_wiring_dir,
        docs_root=disabled_wiring_docs_root,
    )
    return {
        **fixture,
        "disabled_wiring_dir": disabled_wiring_dir,
        "disabled_wiring_docs_root": disabled_wiring_docs_root,
    }
