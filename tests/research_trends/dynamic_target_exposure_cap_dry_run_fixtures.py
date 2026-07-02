from __future__ import annotations

from pathlib import Path

from dynamic_dry_run_readiness_fixtures import (
    build_dynamic_dry_run_readiness_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    run_dynamic_target_baseline_dry_run_readiness_with_pit_caveat,
)

__all__ = [
    "build_dynamic_target_exposure_cap_dry_run_fixture",
    "read_json",
    "write_json",
]


def build_dynamic_target_exposure_cap_dry_run_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    upstream = build_dynamic_dry_run_readiness_fixture(tmp_path)
    readiness_dir = tmp_path / "dry_run_readiness"
    run_dynamic_target_baseline_dry_run_readiness_with_pit_caveat(
        timestamp_remediation_dir=upstream["timestamp_remediation_dir"],
        source_remediation_dir=upstream["source_remediation_dir"],
        dynamic_preparation_dir=upstream["dynamic_preparation_dir"],
        source_binding_dir=upstream["source_binding_dir"],
        simulation_policy_dir=upstream["simulation_policy_dir"],
        static_dry_run_dir=upstream["static_dry_run_dir"],
        output_dir=readiness_dir,
        docs_root=tmp_path / "dry_run_readiness_docs",
    )
    return {
        **upstream,
        "dry_run_readiness_dir": readiness_dir,
    }
