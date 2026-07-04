from __future__ import annotations

from pathlib import Path

from high_intensity_scheduler_wiring_plan_fixtures import (
    build_high_intensity_scheduler_wiring_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    run_high_intensity_risk_cap_observe_only_scheduler_wiring_plan,
)

__all__ = [
    "build_high_intensity_scheduler_disabled_wiring_fixture",
    "read_json",
    "write_json",
]


def build_high_intensity_scheduler_disabled_wiring_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_wiring_plan_fixture(tmp_path)
    wiring_plan_dir = tmp_path / "wiring_plan"
    wiring_docs_root = tmp_path / "wiring_docs"
    run_high_intensity_risk_cap_observe_only_scheduler_wiring_plan(
        scheduler_dry_run_dir=fixture["scheduler_dry_run_dir"],
        scheduler_integration_plan_dir=fixture["scheduler_integration_plan_dir"],
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        output_dir=wiring_plan_dir,
        docs_root=wiring_docs_root,
    )
    return {
        **fixture,
        "wiring_plan_dir": wiring_plan_dir,
        "wiring_docs_root": wiring_docs_root,
    }
