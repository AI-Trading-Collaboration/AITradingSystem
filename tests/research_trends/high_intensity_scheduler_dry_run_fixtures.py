from __future__ import annotations

import csv
from pathlib import Path

from high_intensity_runtime_dry_run_fixtures import (
    sample_detection_rows,
    sample_selected_rule,
    sample_trigger_source_rows,
)
from high_intensity_scheduler_plan_fixtures import (
    build_high_intensity_scheduler_plan_fixture,
    read_json,
    write_json,
)

from ai_trading_system.high_intensity_risk_cap_scheduler_integration_plan import (
    run_high_intensity_risk_cap_observe_only_runtime_scheduler_integration_plan,
)

__all__ = [
    "build_high_intensity_scheduler_dry_run_fixture",
    "read_json",
    "sample_detection_rows",
    "sample_selected_rule",
    "sample_trigger_source_rows",
    "write_json",
]


def build_high_intensity_scheduler_dry_run_fixture(tmp_path: Path) -> dict[str, Path]:
    fixture = build_high_intensity_scheduler_plan_fixture(tmp_path)
    _write_dynamic_csv_sidecars(fixture["dynamic_dry_run_dir"])
    scheduler_plan_dir = tmp_path / "scheduler_plan"
    scheduler_plan_docs_root = tmp_path / "scheduler_plan_docs"
    run_high_intensity_risk_cap_observe_only_runtime_scheduler_integration_plan(
        runtime_dry_run_dir=fixture["runtime_dry_run_dir"],
        runtime_integration_plan_dir=fixture["runtime_integration_plan_dir"],
        continue_decision_dir=fixture["continue_decision_dir"],
        event_logger_dir=fixture["event_logger_dir"],
        threshold_selection_dir=fixture["threshold_selection_dir"],
        forward_observe_plan_dir=fixture["forward_observe_plan_dir"],
        output_dir=scheduler_plan_dir,
        docs_root=scheduler_plan_docs_root,
    )
    return {
        **fixture,
        "scheduler_integration_plan_dir": scheduler_plan_dir,
        "scheduler_plan_docs_root": scheduler_plan_docs_root,
    }


def _write_dynamic_csv_sidecars(root: Path) -> None:
    trigger_rows = sample_trigger_source_rows()
    trigger_csv = root / "dynamic_target_risk_cap_trigger_alignment_matrix.csv"
    trigger_csv.parent.mkdir(parents=True, exist_ok=True)
    with trigger_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(trigger_rows[0]))
        writer.writeheader()
        writer.writerows(trigger_rows)
    (root / "dynamic_target_exposure_cap_dry_run_result.csv").write_text(
        "date,target_asset\n",
        encoding="utf-8",
    )
