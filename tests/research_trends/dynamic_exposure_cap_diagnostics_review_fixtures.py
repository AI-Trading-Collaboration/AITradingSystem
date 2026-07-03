from __future__ import annotations

from pathlib import Path

from dynamic_target_exposure_cap_dry_run_fixtures import (
    build_dynamic_target_exposure_cap_dry_run_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    run_dynamic_target_exposure_cap_dry_run,
)
from ai_trading_system.exposure_cap_diagnostics_review import (
    run_exposure_cap_vs_no_cap_diagnostics_review,
)

__all__ = [
    "build_dynamic_exposure_cap_diagnostics_review_fixture",
    "read_json",
    "write_json",
]


def build_dynamic_exposure_cap_diagnostics_review_fixture(
    tmp_path: Path,
) -> dict[str, Path]:
    upstream = build_dynamic_target_exposure_cap_dry_run_fixture(tmp_path)
    dynamic_dry_run_dir = tmp_path / "dynamic_dry_run"
    run_dynamic_target_exposure_cap_dry_run(
        dry_run_readiness_dir=upstream["dry_run_readiness_dir"],
        timestamp_remediation_dir=upstream["timestamp_remediation_dir"],
        source_remediation_dir=upstream["source_remediation_dir"],
        source_binding_dir=upstream["source_binding_dir"],
        simulation_policy_dir=upstream["simulation_policy_dir"],
        static_dry_run_dir=upstream["static_dry_run_dir"],
        market_data_source=upstream["prices_path"],
        rates_source=upstream["rates_path"],
        policy_path=upstream["policy_path"],
        quality_as_of="2023-01-10",
        output_dir=dynamic_dry_run_dir,
        docs_root=tmp_path / "dynamic_dry_run_docs",
    )
    static_diagnostics_dir = tmp_path / "static_diagnostics"
    run_exposure_cap_vs_no_cap_diagnostics_review(
        dry_run_dir=upstream["static_dry_run_dir"],
        source_binding_dir=upstream["source_binding_dir"],
        baseline_decision_dir=upstream["baseline_decision_dir"],
        simulation_policy_dir=upstream["simulation_policy_dir"],
        output_dir=static_diagnostics_dir,
        docs_root=tmp_path / "static_diagnostics_docs",
    )
    return {
        **upstream,
        "dynamic_dry_run_dir": dynamic_dry_run_dir,
        "static_diagnostics_dir": static_diagnostics_dir,
    }
