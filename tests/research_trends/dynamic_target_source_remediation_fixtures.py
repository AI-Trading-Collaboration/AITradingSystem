from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_target_baseline_preparation_fixtures import (
    build_dynamic_target_baseline_preparation_fixture,
    read_json,
    write_json,
)

from ai_trading_system.dynamic_target_baseline_preparation import (
    run_dynamic_target_baseline_preparation,
)


def build_dynamic_target_source_remediation_fixture(
    tmp_path: Path,
    *,
    source_kind: str = "schema_adapter",
) -> dict[str, Path]:
    upstream = build_dynamic_target_baseline_preparation_fixture(
        tmp_path,
        source_kind=source_kind,
    )
    dynamic_preparation_dir = tmp_path / "dynamic_preparation"
    run_dynamic_target_baseline_preparation(
        diagnostics_dir=upstream["diagnostics_dir"],
        static_dry_run_dir=upstream["static_dry_run_dir"],
        baseline_decision_dir=upstream["baseline_decision_dir"],
        source_binding_dir=upstream["source_binding_dir"],
        simulation_policy_dir=upstream["simulation_policy_dir"],
        candidate_artifact_roots=str(upstream["candidate_root"]),
        output_dir=dynamic_preparation_dir,
        docs_root=tmp_path / "dynamic_preparation_docs",
    )
    force_2329_remediation_route(dynamic_preparation_dir)
    return {
        **upstream,
        "dynamic_preparation_dir": dynamic_preparation_dir,
    }


def force_2329_remediation_route(dynamic_preparation_dir: Path) -> None:
    summary_path = dynamic_preparation_dir / "dynamic_target_baseline_preparation_summary.json"
    summary = read_json(summary_path)
    summary["readiness_status"] = "DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED"
    summary["next_task"] = "TRADING-2329_Dynamic_Target_Baseline_Source_Remediation"
    summary["pit_ready_source_count"] = 0
    summary["recommended_candidate_count"] = 0
    write_json(summary_path, summary)

    readiness_path = dynamic_preparation_dir / "dynamic_target_baseline_2329_readiness_matrix.json"
    readiness = read_json(readiness_path)
    readiness["readiness_status"] = "DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED"
    readiness["2329_allowed"] = False
    write_json(readiness_path, readiness)

    route_path = dynamic_preparation_dir / "dynamic_target_baseline_2329_task_route.json"
    route = read_json(route_path)
    route["readiness_status"] = "DYNAMIC_BASELINE_SOURCE_REMEDIATION_REQUIRED"
    route["next_task"] = "TRADING-2329_Dynamic_Target_Baseline_Source_Remediation"
    route["2329_allowed"] = False
    write_json(route_path, route)


def source_row(
    *,
    source_id: str = "fixture_dynamic_source",
    source_type: str = "dynamic_strategy_target_exposure",
    source_available: bool = True,
    target_exposure: bool = True,
    timestamps: bool = False,
    validity: bool = False,
    source_hash: str = "fixture-hash",
    registry: bool = True,
    replayable: bool = True,
) -> dict[str, Any]:
    coverage = {
        "date": True,
        "target_asset": True,
        "target_exposure": target_exposure,
        "risk_asset_exposure": target_exposure,
        "asset_weight": target_exposure,
        "cash_weight": False,
        "as_of_timestamp": timestamps,
        "decision_timestamp": timestamps,
        "valid_from": validity,
        "valid_until": validity,
        "rebalance_flag": False,
        "source_artifact_hash": bool(source_hash),
        "signal_source_id": True,
        "advisory_id": False,
    }
    return {
        "source_id": source_id,
        "source_type": source_type,
        "source_path": "",
        "source_available": source_available,
        "source_hash": source_hash,
        "history_start": "2023-01-06" if replayable else "",
        "history_end": "2023-01-10" if replayable else "",
        "record_count": 3 if replayable else 0,
        "target_assets_supported": ["QQQ", "SPY", "SMH"] if target_exposure else [],
        "horizons_supported": ["10d"],
        "field_coverage": coverage,
        "registry_reference_available": registry,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def pit_row(
    source_id: str = "fixture_dynamic_source",
    *,
    strict: bool = False,
    replayable: bool = True,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "pit_status": "STRICT_PIT_READY" if strict else "REPLAYABLE_BUT_NOT_STRICT_PIT",
        "as_of_timestamp_available": strict,
        "decision_timestamp_available": strict,
        "valid_from_available": strict,
        "valid_until_available": strict,
        "source_artifact_hash_available": True,
        "registry_reference_available": True,
        "replayable": replayable,
        "known_at_semantics": "strict_known_at" if strict else "date_level_pit_caveat",
        "revision_risk": "LOW",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def alignment_row(source_id: str = "fixture_dynamic_source") -> dict[str, Any]:
    return {
        "source_id": source_id,
        "alignment_readiness_status": "ALIGNMENT_READY",
        "risk_cap_trigger_series_available": True,
        "market_data_available": True,
        "overlap_start": "2023-01-06",
        "overlap_end": "2023-01-10",
        "overlap_record_count": 3,
        "asset_overlap": ["QQQ", "SPY", "SMH"],
        "horizon_overlap": ["10d"],
        "calendar_alignment_status": "CALENDAR_ALIGNED",
        "timestamp_alignment_status": "TIMESTAMP_ALIGNED",
        "alignment_blockers": [],
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
