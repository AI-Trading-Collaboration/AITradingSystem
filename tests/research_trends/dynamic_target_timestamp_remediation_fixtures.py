from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_target_source_remediation_fixtures import (
    build_dynamic_target_source_remediation_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_baseline_source_remediation import (
    run_dynamic_target_baseline_source_remediation,
)

__all__ = [
    "build_dynamic_target_timestamp_remediation_fixture",
    "read_json",
    "write_json",
    "wrapper_row",
]


def build_dynamic_target_timestamp_remediation_fixture(tmp_path: Path) -> dict[str, Path]:
    upstream = build_dynamic_target_source_remediation_fixture(tmp_path)
    source_remediation_dir = tmp_path / "source_remediation"
    run_dynamic_target_baseline_source_remediation(
        dynamic_preparation_dir=upstream["dynamic_preparation_dir"],
        diagnostics_dir=upstream["diagnostics_dir"],
        static_dry_run_dir=upstream["static_dry_run_dir"],
        source_binding_dir=upstream["source_binding_dir"],
        simulation_policy_dir=upstream["simulation_policy_dir"],
        candidate_artifact_roots=str(upstream["candidate_root"]),
        output_dir=source_remediation_dir,
        docs_root=tmp_path / "source_remediation_docs",
    )
    return {
        **upstream,
        "source_remediation_dir": source_remediation_dir,
    }


def wrapper_row(
    *,
    baseline_id: str = "baseline",
    source_id: str = "source",
    target_asset: str = "QQQ",
    row_date: str = "2023-01-06",
    strict_native: bool = False,
    missing_decision: bool = False,
    missing_validity: bool = False,
) -> dict[str, Any]:
    return {
        "baseline_id": baseline_id,
        "source_id": source_id,
        "source_family": "dynamic_strategy_target_exposure",
        "source_type": "dynamic_strategy_target_exposure",
        "date": row_date,
        "target_asset": target_asset,
        "target_exposure": 0.7,
        "risk_asset_exposure": 0.7,
        "asset_weight": 0.7,
        "cash_weight": 0.3,
        "as_of_timestamp": f"{row_date}T00:00:00Z",
        "decision_timestamp": "" if missing_decision else f"{row_date}T00:00:00Z",
        "valid_from": "" if missing_validity else row_date,
        "valid_until": "" if missing_validity else row_date,
        "rebalance_flag": False,
        "rebalance_timestamp": f"{row_date}T00:00:00Z",
        "source_artifact_hash": "hash",
        "source_hash": "hash",
        "source_path": "outputs/research_strategies/source.csv",
        "baseline_schema_version": "dynamic_target_baseline.v1",
        "adapter_id": "adapter",
        "generated_at": "2026-07-03T00:00:00+00:00",
        "pit_policy": "STRICT_PIT_READY" if strict_native else "PIT_APPROXIMATION_READY",
        "known_at_semantics": "native known-at timestamp"
        if strict_native
        else "PIT approximation: date-level known-at inferred",
        "replayability_status": "REPLAYABLE",
        "simulation_ready_candidate": True,
        "target_exposure_role": "research_baseline_field_only_not_trading_instruction",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
