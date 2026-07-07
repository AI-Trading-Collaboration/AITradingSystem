from __future__ import annotations

from collections.abc import Mapping
from typing import Any

MATRIX_SCHEMA_VERSION = "dynamic_strategy_pit_coverage_matrix.v1"
REMEDIATION_SCHEMA_VERSION = "dynamic_strategy_pit_remediation_matrix.v1"


def build_pit_coverage_matrix(
    registry: Mapping[str, Any],
    *,
    scope: str = "dynamic_strategy",
) -> dict[str, Any]:
    rows = [_matrix_row_from_entry(entry) for entry in _as_list(registry.get("entries"))]
    return {
        "schema_version": MATRIX_SCHEMA_VERSION,
        "scope": scope,
        "registry_path": registry.get("path"),
        "registry_schema_version": registry.get("schema_version"),
        "registry_validation_status": registry.get("validation_status"),
        "row_count": len(rows),
        "pit_coverage_matrix": rows,
        "production_effect": "none",
        "broker_action": "none",
    }


def build_pit_remediation_matrix(
    matrix_rows: list[Mapping[str, Any]],
    *,
    scope: str = "dynamic_strategy",
) -> dict[str, Any]:
    rows = [
        _remediation_row(row)
        for row in matrix_rows
        if row.get("severity") in {"BLOCKING", "MATERIAL"}
    ]
    return {
        "schema_version": REMEDIATION_SCHEMA_VERSION,
        "scope": scope,
        "row_count": len(rows),
        "pit_remediation_matrix": rows,
        "production_effect": "none",
        "broker_action": "none",
    }


def blocking_gap_ids(matrix_rows: list[Mapping[str, Any]]) -> list[str]:
    return [
        str(row.get("input_id"))
        for row in matrix_rows
        if row.get("severity") == "BLOCKING"
    ]


def _matrix_row_from_entry(value: Any) -> dict[str, Any]:
    entry = _as_mapping(value)
    return {
        "input_id": entry.get("input_id"),
        "input_type": entry.get("input_type"),
        "owner_module": entry.get("owner_module"),
        "source_artifact_or_config": entry.get("source_artifact_or_config"),
        "used_by": list(_as_list(entry.get("used_by"))),
        "as_of_field": entry.get("as_of_field"),
        "generated_at_field": entry.get("generated_at_field"),
        "valid_from_field": entry.get("valid_from_field"),
        "valid_until_field": entry.get("valid_until_field"),
        "point_in_time_status": entry.get("pit_status"),
        "pit_confidence": entry.get("pit_confidence"),
        "risk_flags": list(_as_list(entry.get("risk_flags"))),
        "severity": entry.get("severity"),
        "candidate_search_blocker": entry.get("candidate_search_blocker") is True,
        "observation_blocker": entry.get("observation_blocker") is True,
        "paper_shadow_blocker": entry.get("paper_shadow_blocker") is True,
        "production_blocker": entry.get("production_blocker") is True,
        "remediation_owner": entry.get("remediation_owner"),
        "recommended_action": entry.get("recommended_action"),
    }


def _remediation_row(row: Mapping[str, Any]) -> dict[str, Any]:
    input_id = str(row.get("input_id"))
    severity = str(row.get("severity"))
    return {
        "input_id": input_id,
        "severity": severity,
        "pit_status": row.get("point_in_time_status"),
        "candidate_search_blocker": row.get("candidate_search_blocker") is True,
        "observation_blocker": row.get("observation_blocker") is True,
        "paper_shadow_blocker": row.get("paper_shadow_blocker") is True,
        "production_blocker": row.get("production_blocker") is True,
        "remediation_owner": row.get("remediation_owner"),
        "recommended_action": row.get("recommended_action"),
        "next_task": _next_task_for(input_id, row.get("remediation_owner")),
    }


def _next_task_for(input_id: str, remediation_owner: Any) -> str:
    if str(remediation_owner).startswith("TRADING-"):
        mapping = {
            "TRADING-2406": (
                "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_"
                "Remediation_Plan"
            ),
            "TRADING-2407": (
                "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_"
                "Remediation_Plan"
            ),
            "TRADING-2408": "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan",
            "TRADING-2409": "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan",
        }
        owner = str(remediation_owner)
        return mapping.get(owner, owner)
    if input_id == "growth_tilt_engine":
        return "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan"
    if input_id == "valid_until_window":
        return "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan"
    return "research_governance_review"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
