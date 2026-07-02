from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_target_timestamp_remediation_fixtures import (
    build_dynamic_target_timestamp_remediation_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    run_dynamic_target_baseline_timestamp_remediation,
)

__all__ = [
    "build_dynamic_dry_run_readiness_fixture",
    "dry_run_wrapper_row",
    "read_json",
    "write_json",
]


def build_dynamic_dry_run_readiness_fixture(tmp_path: Path) -> dict[str, Path]:
    upstream = build_dynamic_target_timestamp_remediation_fixture(tmp_path)
    timestamp_remediation_dir = tmp_path / "timestamp_remediation"
    run_dynamic_target_baseline_timestamp_remediation(
        source_remediation_dir=upstream["source_remediation_dir"],
        dynamic_preparation_dir=upstream["dynamic_preparation_dir"],
        diagnostics_dir=upstream["diagnostics_dir"],
        source_binding_dir=upstream["source_binding_dir"],
        simulation_policy_dir=upstream["simulation_policy_dir"],
        output_dir=timestamp_remediation_dir,
        docs_root=tmp_path / "timestamp_remediation_docs",
    )
    return {
        **upstream,
        "timestamp_remediation_dir": timestamp_remediation_dir,
    }


def dry_run_wrapper_row(
    *,
    row_date: str = "2023-01-06",
    target_asset: str = "QQQ",
    missing_target_exposure: bool = False,
) -> dict[str, Any]:
    target_exposure: float | str = "" if missing_target_exposure else 0.7
    return {
        "baseline_id": "baseline",
        "source_id": "source",
        "source_family": "dynamic_strategy_target_exposure",
        "source_type": "dynamic_strategy_target_exposure",
        "date": row_date,
        "target_asset": target_asset,
        "target_exposure": target_exposure,
        "risk_asset_exposure": 0.7,
        "asset_weight": 0.7,
        "cash_weight": 0.3,
        "as_of_timestamp": f"{row_date}T00:00:00Z",
        "decision_timestamp": "2023-01-09T00:00:00Z",
        "valid_from": "2023-01-09T00:00:00Z",
        "valid_until": "2023-01-10T00:00:00Z",
        "rebalance_flag": False,
        "rebalance_timestamp": "2023-01-09T00:00:00Z",
        "source_artifact_hash": "hash",
        "source_hash": "hash",
        "source_path": "outputs/research_strategies/source.csv",
        "baseline_schema_version": "dynamic_target_baseline.v1",
        "adapter_id": "adapter",
        "generated_at": "2026-07-03T00:00:00+00:00",
        "timestamp_remediation_policy_id": "dynamic_target_timestamp_remediation_policy_v1",
        "timestamp_derivation_mode": "deterministic_latency_policy_with_validity_window_policy",
        "pit_policy": "PIT_APPROXIMATION_READY",
        "known_at_policy": "NEXT_SESSION_DECISION_POLICY",
        "latency_policy": "NEXT_TRADING_DAY_DECISION",
        "rebalance_policy": "DAILY_DECISION_REBALANCE",
        "known_at_semantics": "PIT approximation: date-level known-at inferred",
        "replayability_status": "REPLAYABLE",
        "simulation_ready_candidate": True,
        "target_exposure_role": "research_baseline_field_only_not_trading_instruction",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
