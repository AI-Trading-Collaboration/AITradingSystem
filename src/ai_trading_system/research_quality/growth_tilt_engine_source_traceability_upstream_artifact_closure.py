from __future__ import annotations

from collections.abc import Mapping
from typing import Any

CLOSURE_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_upstream_artifact_closure.v1"
)
SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_SCHEMA_VERSION = (
    "growth_tilt_engine_source_traceability_closure_evidence.v1"
)
UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_SCHEMA_VERSION = (
    "growth_tilt_engine_upstream_artifact_closure_evidence.v1"
)
UPDATED_SOURCE_FEATURE_MAPPING_SCHEMA_VERSION = (
    "growth_tilt_engine_updated_source_feature_mapping_after_traceability_closure.v1"
)
REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_remaining_blocker_summary_after_traceability_closure.v1"
)

READY_STATUS = (
    "GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE_READY"
)
NEXT_ROUTE = "TRADING-2418_Valid_Until_Window_Dependency_Evidence_Closure"
PIT_RECHECK_ROUTE = "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck"

SOURCE_TRACEABILITY_BLOCKER_FEATURES: tuple[str, ...] = (
    "volatility_inputs",
    "trend_features",
    "drawdown_features",
    "target_vol_policy",
    "growth_tilt_engine_signal_artifact",
)
PRE_RECHECK_EVIDENCE_FEATURES: tuple[str, ...] = (
    "volatility_inputs",
    "trend_features",
    "drawdown_features",
    "target_vol_policy",
)

_UPSTREAM_REPORT_BY_FEATURE: dict[str, str] = {
    "volatility_inputs": "growth_tilt_engine_as_of_semantics_remediation",
    "trend_features": "growth_tilt_engine_source_feature_contract_mapping",
    "drawdown_features": "growth_tilt_engine_as_of_semantics_remediation",
    "target_vol_policy": "growth_tilt_engine_source_feature_contract_mapping",
    "growth_tilt_engine_signal_artifact": "missing_standalone_growth_tilt_engine_signal_artifact",
}
_UPSTREAM_MODULE_BY_FEATURE: dict[str, str] = {
    "volatility_inputs": (
        "ai_trading_system.dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation"
    ),
    "trend_features": (
        "ai_trading_system.dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping"
    ),
    "drawdown_features": (
        "ai_trading_system.dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation"
    ),
    "target_vol_policy": (
        "ai_trading_system.dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping"
    ),
    "growth_tilt_engine_signal_artifact": None,
}
_UPSTREAM_PATH_BY_FEATURE: dict[str, str | None] = {
    "volatility_inputs": (
        "outputs/research_strategies/"
        "growth_tilt_engine_as_of_semantics_remediation/"
        "updated_source_feature_mapping.json"
    ),
    "trend_features": (
        "outputs/research_strategies/"
        "growth_tilt_engine_source_feature_contract_mapping/"
        "source_feature_contract_mapping.json"
    ),
    "drawdown_features": (
        "outputs/research_strategies/"
        "growth_tilt_engine_as_of_semantics_remediation/"
        "updated_source_feature_mapping.json"
    ),
    "target_vol_policy": "config/research/equal_risk_growth_tilt_candidate_registry.yaml",
    "growth_tilt_engine_signal_artifact": None,
}


def build_growth_tilt_source_traceability_upstream_artifact_closure(
    closure_result_2416: Mapping[str, Any],
    remaining_blocker_matrix_2416: Mapping[str, Any],
    source_traceability_closure_plan_2416: Mapping[str, Any],
    pit_gate_readiness_snapshot_2415: Mapping[str, Any],
    pit_gate_readiness_matrix_2415: Mapping[str, Any],
    source_traceability_remediation_result_2413: Mapping[str, Any],
    updated_source_feature_mapping_2413: Mapping[str, Any],
    as_of_updated_source_feature_mapping_2412: Mapping[str, Any],
    source_feature_contract_mapping_2410: Mapping[str, Any],
    *,
    pit_input_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    blocker_matrix = _section(remaining_blocker_matrix_2416, "remaining_blocker_matrix")
    blocker_rows = [
        _as_mapping(row) for row in _as_list(blocker_matrix.get("matrix_rows"))
    ]
    blocker_rows_by_feature = {
        _feature_id(row): row for row in blocker_rows if _feature_id(row)
    }
    source_closure_plan = _section(
        source_traceability_closure_plan_2416,
        "source_traceability_closure_plan",
    )
    closure_plan_rows = {
        _feature_id(row): _as_mapping(row)
        for row in _as_list(source_closure_plan.get("closure_rows"))
        if _feature_id(row)
    }
    mapping_2413_rows = _rows_by_feature(
        updated_source_feature_mapping_2413,
        "updated_source_feature_mapping",
    )
    mapping_2412_rows = _rows_by_feature(
        as_of_updated_source_feature_mapping_2412,
        "updated_source_feature_mapping",
    )
    mapping_2410_rows = _rows_by_feature(
        source_feature_contract_mapping_2410,
        "source_feature_contract_mapping",
    )
    evidence_rows = [
        _source_traceability_evidence_row(
            feature_id,
            blocker_row=blocker_rows_by_feature.get(feature_id, {}),
            closure_plan_row=closure_plan_rows.get(feature_id, {}),
            mapping_2413_row=mapping_2413_rows.get(feature_id, {}),
            mapping_2412_row=mapping_2412_rows.get(feature_id, {}),
            mapping_2410_row=mapping_2410_rows.get(feature_id, {}),
            closure_result_2416=closure_result_2416,
            source_traceability_remediation_result_2413=(
                source_traceability_remediation_result_2413
            ),
        )
        for feature_id in SOURCE_TRACEABILITY_BLOCKER_FEATURES
    ]
    upstream_rows = [_upstream_artifact_evidence_row(row) for row in evidence_rows]
    updated_mapping = _updated_source_feature_mapping(
        blocker_rows,
        evidence_rows=evidence_rows,
        mapping_2413_rows=mapping_2413_rows,
        mapping_2412_rows=mapping_2412_rows,
        mapping_2410_rows=mapping_2410_rows,
    )
    remaining_summary = _remaining_blocker_summary(
        blocker_rows,
        evidence_rows=evidence_rows,
        pit_input_registry=pit_input_registry or {},
    )
    counts = _counts(
        closure_result_2416,
        pit_gate_readiness_snapshot_2415,
        blocker_rows,
        evidence_rows,
    )
    validation = _validation(
        blocker_rows,
        source_closure_plan,
        pit_gate_readiness_matrix_2415,
        evidence_rows,
        counts=counts,
    )
    return {
        "schema_version": CLOSURE_SCHEMA_VERSION,
        "task_id": "TRADING-2417",
        "status": READY_STATUS,
        "engine_id": "growth_tilt_engine",
        "source_tasks": [
            "TRADING-2410",
            "TRADING-2412",
            "TRADING-2413",
            "TRADING-2415",
            "TRADING-2416",
        ],
        "source_feature_count": counts["source_feature_count"],
        "pit_gate_ready_count": counts["pit_gate_ready_count"],
        "contract_ready_count": counts["contract_ready_count"],
        "pit_gate_blocked_count": counts["pit_gate_blocked_count"],
        "blocked_by_source_traceability_count": counts[
            "blocked_by_source_traceability_count"
        ],
        "blocked_by_valid_until_window_count": counts[
            "blocked_by_valid_until_window_count"
        ],
        "source_traceability_evidence_row_count": len(evidence_rows),
        "source_traceability_pre_recheck_evidence_ready_count": counts[
            "source_traceability_pre_recheck_evidence_ready_count"
        ],
        "source_traceability_still_blocked_count": counts[
            "source_traceability_still_blocked_count"
        ],
        "upstream_artifact_closure_evidence_row_count": len(upstream_rows),
        "upstream_artifact_pre_recheck_evidence_ready_count": counts[
            "upstream_artifact_pre_recheck_evidence_ready_count"
        ],
        "upstream_artifact_still_blocked_count": counts[
            "upstream_artifact_still_blocked_count"
        ],
        "source_traceability_closure_evidence_ready": validation["valid"],
        "upstream_artifact_closure_evidence_ready": validation["valid"],
        "updated_source_feature_mapping_ready": validation["valid"],
        "remaining_blocker_summary_ready": validation["valid"],
        "source_traceability_closure_evidence": {
            "schema_version": SOURCE_TRACEABILITY_CLOSURE_EVIDENCE_SCHEMA_VERSION,
            "engine_id": "growth_tilt_engine",
            "evidence_scope": "pre_recheck_source_traceability_evidence",
            "source_traceability_blocker_count_from_2416": counts[
                "blocked_by_source_traceability_count"
            ],
            "pre_recheck_evidence_ready_count": counts[
                "source_traceability_pre_recheck_evidence_ready_count"
            ],
            "still_blocked_count": counts["source_traceability_still_blocked_count"],
            "evidence_rows": evidence_rows,
            "pit_gate_recheck_required": True,
            "auto_mark_pit_gate_ready": False,
            "auto_mark_contract_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "upstream_artifact_closure_evidence": {
            "schema_version": UPSTREAM_ARTIFACT_CLOSURE_EVIDENCE_SCHEMA_VERSION,
            "engine_id": "growth_tilt_engine",
            "evidence_scope": "pre_recheck_upstream_artifact_evidence",
            "upstream_artifact_gap_count_from_2416": counts[
                "upstream_artifact_gap_count"
            ],
            "pre_recheck_evidence_ready_count": counts[
                "upstream_artifact_pre_recheck_evidence_ready_count"
            ],
            "still_blocked_count": counts["upstream_artifact_still_blocked_count"],
            "evidence_rows": upstream_rows,
            "pit_gate_recheck_required": True,
            "auto_mark_pit_gate_ready": False,
            "auto_mark_contract_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "updated_source_feature_mapping": updated_mapping,
        "remaining_blocker_summary": remaining_summary,
        "closure_validation": validation,
        "pit_gate_recheck_required": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
        "recommended_next_research_task": NEXT_ROUTE,
        "pit_gate_recheck_route": PIT_RECHECK_ROUTE,
    }


def _source_traceability_evidence_row(
    feature_id: str,
    *,
    blocker_row: Mapping[str, Any],
    closure_plan_row: Mapping[str, Any],
    mapping_2413_row: Mapping[str, Any],
    mapping_2412_row: Mapping[str, Any],
    mapping_2410_row: Mapping[str, Any],
    closure_result_2416: Mapping[str, Any],
    source_traceability_remediation_result_2413: Mapping[str, Any],
) -> dict[str, Any]:
    mapping_row = _first_mapping(mapping_2413_row, mapping_2412_row, mapping_2410_row)
    metadata = _as_mapping(mapping_row.get("source_traceability_contract_metadata"))
    as_of_metadata = _as_mapping(mapping_row.get("as_of_contract_metadata"))
    contract_payload = _as_mapping(mapping_row.get("contract_payload"))
    evidence_ready = feature_id in PRE_RECHECK_EVIDENCE_FEATURES
    generated_at = source_traceability_remediation_result_2413.get(
        "generated_at",
        closure_result_2416.get("generated_at"),
    )
    source_config = _source_config(feature_id, metadata, contract_payload, blocker_row)
    source_data_cutoff = _source_data_cutoff(
        feature_id,
        as_of_metadata,
        closure_result_2416,
    )
    upstream_path = _UPSTREAM_PATH_BY_FEATURE.get(feature_id)
    evidence_limitations = _evidence_limitations(
        feature_id,
        metadata=metadata,
        as_of_metadata=as_of_metadata,
        source_data_cutoff=source_data_cutoff,
    )
    still_blocked_reason = None
    if not evidence_ready:
        still_blocked_reason = (
            "missing standalone growth_tilt_engine signal artifact with source "
            "config, owner module, generated_at, source_data_cutoff, feature "
            "version and source snapshot evidence"
        )
    return {
        "feature_id": feature_id,
        "source_system_before_2417": blocker_row.get("source_system"),
        "source_traceability_status_before_2417": blocker_row.get(
            "source_traceability_status"
        )
        or mapping_row.get("source_traceability_status"),
        "source_traceability_blocker_preserved_until_pit_recheck": True,
        "traceability_closure_status": (
            "PRE_RECHECK_EVIDENCE_AVAILABLE"
            if evidence_ready
            else "STILL_BLOCKED_MISSING_UPSTREAM_SIGNAL_ARTIFACT"
        ),
        "source_traceability_evidence_ready": evidence_ready,
        "upstream_artifact_evidence_ready": evidence_ready,
        "source_config_after_2417": source_config if evidence_ready else None,
        "source_artifact_after_2417": (
            _artifact_reference(feature_id, blocker_row, metadata, upstream_path)
            if evidence_ready
            else None
        ),
        "owner_module_after_2417": _UPSTREAM_MODULE_BY_FEATURE.get(feature_id),
        "upstream_report_registry_id_after_2417": _UPSTREAM_REPORT_BY_FEATURE.get(
            feature_id
        ),
        "upstream_artifact_path_after_2417": upstream_path,
        "upstream_artifact_key_after_2417": _upstream_key(feature_id, metadata),
        "generated_at_after_2417": generated_at if evidence_ready else None,
        "source_data_cutoff_after_2417": source_data_cutoff if evidence_ready else None,
        "feature_version_after_2417": (
            _feature_version(feature_id, metadata) if evidence_ready else None
        ),
        "source_snapshot_reference_after_2417": (
            metadata.get("source_snapshot_reference")
            or _derived_snapshot_reference(feature_id, upstream_path)
            if evidence_ready
            else None
        ),
        "source_snapshot_hash_after_2417": (
            metadata.get("source_snapshot_hash") if evidence_ready else None
        ),
        "artifact_row_count_available": False,
        "artifact_checksum_available": bool(metadata.get("source_snapshot_hash")),
        "artifact_row_count_and_checksum_rationale": (
            "prior artifacts expose config/path lineage; row-count/checksum is only "
            "available when the prior source_traceability metadata recorded a "
            "source_snapshot_hash"
        ),
        "missing_fields_before_2417": _missing_fields(closure_plan_row),
        "evidence_limitations": evidence_limitations,
        "still_blocked_reason": still_blocked_reason,
        "required_next_action": (
            "create_or_register_upstream_signal_artifact_metadata"
            if not evidence_ready
            else "run_TRADING_2418_then_TRADING_2419_PIT_gate_recheck"
        ),
        "pit_gate_ready_after_2417": False,
        "contract_ready_after_2417": False,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _upstream_artifact_evidence_row(
    evidence_row: Mapping[str, Any],
) -> dict[str, Any]:
    feature_id = _text(evidence_row.get("feature_id"))
    ready = evidence_row.get("upstream_artifact_evidence_ready") is True
    return {
        "feature_id": feature_id,
        "upstream_artifact_closure_status": (
            "PRE_RECHECK_UPSTREAM_EVIDENCE_AVAILABLE"
            if ready
            else "STILL_BLOCKED_MISSING_UPSTREAM_SIGNAL_ARTIFACT"
        ),
        "upstream_artifact_available_after_2417": ready,
        "upstream_report_registry_id_after_2417": evidence_row.get(
            "upstream_report_registry_id_after_2417"
        ),
        "upstream_artifact_path_after_2417": evidence_row.get(
            "upstream_artifact_path_after_2417"
        ),
        "upstream_artifact_key_after_2417": evidence_row.get(
            "upstream_artifact_key_after_2417"
        ),
        "source_artifact_after_2417": evidence_row.get("source_artifact_after_2417"),
        "source_snapshot_reference_after_2417": evidence_row.get(
            "source_snapshot_reference_after_2417"
        ),
        "source_snapshot_hash_after_2417": evidence_row.get(
            "source_snapshot_hash_after_2417"
        ),
        "artifact_row_count_available": evidence_row.get(
            "artifact_row_count_available"
        ),
        "artifact_checksum_available": evidence_row.get(
            "artifact_checksum_available"
        ),
        "evidence_limitations": list(
            _as_list(evidence_row.get("evidence_limitations"))
        ),
        "pit_gate_ready_after_2417": False,
        "contract_ready_after_2417": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _updated_source_feature_mapping(
    blocker_rows: list[Mapping[str, Any]],
    *,
    evidence_rows: list[Mapping[str, Any]],
    mapping_2413_rows: Mapping[str, Mapping[str, Any]],
    mapping_2412_rows: Mapping[str, Mapping[str, Any]],
    mapping_2410_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    evidence_by_feature = {_text(row.get("feature_id")): row for row in evidence_rows}
    mapping_rows: list[dict[str, Any]] = []
    for blocker_row in blocker_rows:
        feature_id = _feature_id(blocker_row)
        source_row = _first_mapping(
            mapping_2413_rows.get(feature_id, {}),
            mapping_2412_rows.get(feature_id, {}),
            mapping_2410_rows.get(feature_id, {}),
        )
        evidence = evidence_by_feature.get(feature_id)
        mapping_rows.append(
            {
                **dict(source_row),
                "feature_id": feature_id,
                "source_system": blocker_row.get(
                    "source_system",
                    source_row.get("source_system"),
                ),
                "upstream_artifact_or_registry_reference": blocker_row.get(
                    "upstream_artifact_or_registry_reference",
                    source_row.get("upstream_artifact_or_registry_reference"),
                ),
                "source_traceability_closure_2417": (
                    _mapping_closure_metadata(evidence)
                    if evidence is not None
                    else _unchanged_mapping_metadata(blocker_row)
                ),
                "source_traceability_status_after_2417": (
                    "evidence_available_pending_pit_recheck"
                    if evidence is not None
                    and evidence.get("source_traceability_evidence_ready") is True
                    else (
                        "still_blocked_missing_upstream_signal_artifact"
                        if evidence is not None
                        else source_row.get("source_traceability_status")
                    )
                ),
                "pit_gate_status_after_2417": "blocked_pending_pit_recheck",
                "contract_ready_after_2417": False,
                "pit_gate_ready_after_2417": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    ready_count = sum(
        1
        for row in evidence_rows
        if row.get("source_traceability_evidence_ready") is True
    )
    return {
        "schema_version": UPDATED_SOURCE_FEATURE_MAPPING_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "known_source_feature_count": len(mapping_rows),
        "source_traceability_blocker_count_from_2416": len(evidence_rows),
        "source_traceability_pre_recheck_evidence_ready_count": ready_count,
        "source_traceability_still_blocked_count": len(evidence_rows) - ready_count,
        "contract_ready_count": 0,
        "pit_gate_ready_count": 0,
        "pit_gate_recheck_required": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "mapping_rows": mapping_rows,
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_blocker_summary(
    blocker_rows: list[Mapping[str, Any]],
    *,
    evidence_rows: list[Mapping[str, Any]],
    pit_input_registry: Mapping[str, Any],
) -> dict[str, Any]:
    evidence_ready = [
        _text(row.get("feature_id"))
        for row in evidence_rows
        if row.get("source_traceability_evidence_ready") is True
    ]
    still_blocked = [
        _text(row.get("feature_id"))
        for row in evidence_rows
        if row.get("source_traceability_evidence_ready") is not True
    ]
    valid_until_blocked = [
        _feature_id(row)
        for row in blocker_rows
        if row.get("blocked_by_valid_until_window") is True
    ]
    return {
        "schema_version": REMAINING_BLOCKER_SUMMARY_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "source_feature_count": len(blocker_rows),
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": len(blocker_rows),
        "blocked_by_source_traceability_count": len(evidence_rows),
        "blocked_by_valid_until_window_count": len(valid_until_blocked),
        "source_traceability_pre_recheck_evidence_ready_count": len(evidence_ready),
        "source_traceability_still_blocked_count": len(still_blocked),
        "source_traceability_pre_recheck_evidence_feature_ids": evidence_ready,
        "source_traceability_still_blocked_feature_ids": still_blocked,
        "valid_until_window_blocked_feature_ids": valid_until_blocked,
        "growth_tilt_engine_pit_input_severity": _pit_registry_severity(
            pit_input_registry,
            "growth_tilt_engine",
        ),
        "valid_until_window_pit_input_severity": _pit_registry_severity(
            pit_input_registry,
            "valid_until_window",
        ),
        "growth_tilt_engine_blocking_gap_resolved": False,
        "growth_tilt_engine_severity_downgraded": False,
        "valid_until_window_blocking_gap_resolved": False,
        "valid_until_window_severity_downgraded": False,
        "pit_gate_recheck_required": True,
        "recommended_next_task": NEXT_ROUTE,
        "pit_gate_recheck_route": PIT_RECHECK_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _counts(
    closure_result_2416: Mapping[str, Any],
    pit_gate_readiness_snapshot_2415: Mapping[str, Any],
    blocker_rows: list[Mapping[str, Any]],
    evidence_rows: list[Mapping[str, Any]],
) -> dict[str, int]:
    source_feature_count = _int(
        closure_result_2416.get(
            "source_feature_count",
            pit_gate_readiness_snapshot_2415.get("source_feature_count", len(blocker_rows)),
        )
    )
    pit_gate_ready_count = _int(closure_result_2416.get("pit_gate_ready_count", 0))
    contract_ready_count = _int(closure_result_2416.get("contract_ready_count", 0))
    pit_gate_blocked_count = _int(
        closure_result_2416.get("pit_gate_blocked_count", len(blocker_rows))
    )
    blocked_by_source_traceability_count = _int(
        closure_result_2416.get(
            "blocked_by_source_traceability_count",
            sum(
                1
                for row in blocker_rows
                if row.get("blocked_by_source_traceability") is True
            ),
        )
    )
    blocked_by_valid_until_window_count = _int(
        closure_result_2416.get(
            "blocked_by_valid_until_window_count",
            sum(
                1
                for row in blocker_rows
                if row.get("blocked_by_valid_until_window") is True
            ),
        )
    )
    source_ready = sum(
        1
        for row in evidence_rows
        if row.get("source_traceability_evidence_ready") is True
    )
    upstream_ready = sum(
        1 for row in evidence_rows if row.get("upstream_artifact_evidence_ready") is True
    )
    return {
        "source_feature_count": source_feature_count,
        "pit_gate_ready_count": pit_gate_ready_count,
        "contract_ready_count": contract_ready_count,
        "pit_gate_blocked_count": pit_gate_blocked_count,
        "blocked_by_source_traceability_count": blocked_by_source_traceability_count,
        "blocked_by_valid_until_window_count": blocked_by_valid_until_window_count,
        "source_traceability_pre_recheck_evidence_ready_count": source_ready,
        "source_traceability_still_blocked_count": len(evidence_rows) - source_ready,
        "upstream_artifact_pre_recheck_evidence_ready_count": upstream_ready,
        "upstream_artifact_still_blocked_count": len(evidence_rows) - upstream_ready,
        "upstream_artifact_gap_count": sum(
            1
            for row in blocker_rows
            if row.get("blocked_by_upstream_artifact") is True
        ),
    }


def _validation(
    blocker_rows: list[Mapping[str, Any]],
    source_closure_plan: Mapping[str, Any],
    pit_gate_readiness_matrix_2415: Mapping[str, Any],
    evidence_rows: list[Mapping[str, Any]],
    *,
    counts: Mapping[str, int],
) -> dict[str, Any]:
    errors: list[str] = []
    if len(blocker_rows) != 10:
        errors.append("2416 remaining blocker matrix must contain 10 rows")
    readiness_matrix = _section(
        pit_gate_readiness_matrix_2415,
        "pit_gate_readiness_matrix",
    )
    if len(_as_list(readiness_matrix.get("matrix_rows"))) != 10:
        errors.append("2415 PIT gate readiness matrix must contain 10 rows")
    if source_closure_plan.get("source_traceability_gap_count") != 5:
        errors.append("2416 source traceability closure plan must keep five gaps")
    if len(evidence_rows) != 5:
        errors.append("2417 must produce five source traceability evidence rows")
    if counts.get("source_feature_count") != 10:
        errors.append("source_feature_count must remain 10")
    if counts.get("pit_gate_ready_count") != 0:
        errors.append("pit_gate_ready_count must remain 0")
    if counts.get("contract_ready_count") != 0:
        errors.append("contract_ready_count must remain 0")
    if counts.get("pit_gate_blocked_count") != 10:
        errors.append("pit_gate_blocked_count must remain 10")
    if counts.get("blocked_by_source_traceability_count") != 5:
        errors.append("blocked_by_source_traceability_count must remain 5")
    if counts.get("blocked_by_valid_until_window_count") != 1:
        errors.append("blocked_by_valid_until_window_count must remain 1")
    if counts.get("source_traceability_pre_recheck_evidence_ready_count") != 4:
        errors.append("source traceability pre-recheck evidence ready count must be 4")
    if counts.get("source_traceability_still_blocked_count") != 1:
        errors.append("source traceability still blocked count must be 1")
    return {
        "schema_version": (
            "growth_tilt_engine_source_traceability_upstream_artifact_closure_"
            "validation.v1"
        ),
        "valid": not errors,
        "error_count": len(errors),
        "errors": errors,
        "production_effect": "none",
        "broker_action": "none",
    }


def _mapping_closure_metadata(evidence: Mapping[str, Any] | None) -> dict[str, Any]:
    if evidence is None:
        return {}
    return {
        "task_id": "TRADING-2417",
        "traceability_closure_status": evidence.get("traceability_closure_status"),
        "source_traceability_evidence_ready": evidence.get(
            "source_traceability_evidence_ready"
        ),
        "upstream_artifact_evidence_ready": evidence.get(
            "upstream_artifact_evidence_ready"
        ),
        "source_config_after_2417": evidence.get("source_config_after_2417"),
        "source_artifact_after_2417": evidence.get("source_artifact_after_2417"),
        "owner_module_after_2417": evidence.get("owner_module_after_2417"),
        "generated_at_after_2417": evidence.get("generated_at_after_2417"),
        "source_data_cutoff_after_2417": evidence.get(
            "source_data_cutoff_after_2417"
        ),
        "feature_version_after_2417": evidence.get("feature_version_after_2417"),
        "source_snapshot_reference_after_2417": evidence.get(
            "source_snapshot_reference_after_2417"
        ),
        "source_snapshot_hash_after_2417": evidence.get(
            "source_snapshot_hash_after_2417"
        ),
        "evidence_limitations": list(_as_list(evidence.get("evidence_limitations"))),
        "pit_gate_recheck_required": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _unchanged_mapping_metadata(blocker_row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "task_id": "TRADING-2417",
        "traceability_closure_status": "UNCHANGED_NOT_TARGETED_BY_2417",
        "source_traceability_evidence_ready": False,
        "upstream_artifact_evidence_ready": False,
        "unchanged_reason": "feature was not one of the five 2416 source traceability blockers",
        "blocked_by_source_traceability": blocker_row.get("blocked_by_source_traceability")
        is True,
        "pit_gate_recheck_required": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    mapping = _as_mapping(document)
    nested = mapping.get(key)
    return _as_mapping(nested) if isinstance(nested, Mapping) else mapping


def _rows_by_feature(
    document: Mapping[str, Any],
    section_key: str,
) -> dict[str, Mapping[str, Any]]:
    section = _section(document, section_key)
    return {
        _feature_id(row): _as_mapping(row)
        for row in _as_list(section.get("mapping_rows"))
        if _feature_id(row)
    }


def _first_mapping(*rows: Mapping[str, Any]) -> Mapping[str, Any]:
    for row in rows:
        if row:
            return row
    return {}


def _source_config(
    feature_id: str,
    metadata: Mapping[str, Any],
    contract_payload: Mapping[str, Any],
    blocker_row: Mapping[str, Any],
) -> str | None:
    if feature_id == "growth_tilt_engine_signal_artifact":
        return None
    return _first_text(
        metadata.get("upstream_config_path")
        and (
            f"{metadata.get('upstream_config_path')}:"
            f"{metadata.get('upstream_config_key')}"
        ),
        contract_payload.get("source_config"),
        blocker_row.get("upstream_artifact_or_registry_reference"),
    )


def _source_data_cutoff(
    feature_id: str,
    as_of_metadata: Mapping[str, Any],
    closure_result_2416: Mapping[str, Any],
) -> str | None:
    if feature_id == "growth_tilt_engine_signal_artifact":
        return None
    as_of_date = _text(as_of_metadata.get("as_of_date") or closure_result_2416.get("as_of"))
    observed_at = _text(as_of_metadata.get("source_observed_at"))
    if observed_at and as_of_date:
        return f"{observed_at}; as_of_date={as_of_date}"
    if feature_id == "target_vol_policy" and as_of_date:
        return f"governed_config_snapshot_as_of={as_of_date}"
    if as_of_date:
        return f"source_rows_required_with_source_date_lte_as_of_date={as_of_date}"
    return "source_rows_required_with_source_date_lte_as_of_date"


def _artifact_reference(
    feature_id: str,
    blocker_row: Mapping[str, Any],
    metadata: Mapping[str, Any],
    upstream_path: str | None,
) -> str | None:
    if metadata.get("upstream_artifact_id"):
        return _text(metadata.get("upstream_artifact_id"))
    if upstream_path:
        return f"{upstream_path}#{feature_id}"
    return _text(blocker_row.get("upstream_artifact_or_registry_reference")) or None


def _feature_version(feature_id: str, metadata: Mapping[str, Any]) -> str:
    return _text(
        metadata.get("source_traceability_contract_id"),
        f"growth_tilt_engine:{feature_id}:source_traceability:v1",
    )


def _upstream_key(feature_id: str, metadata: Mapping[str, Any]) -> str | None:
    key = metadata.get("upstream_config_key")
    if key:
        return _text(key)
    if feature_id == "growth_tilt_engine_signal_artifact":
        return None
    return f"mapping_rows[feature_id={feature_id}]"


def _derived_snapshot_reference(feature_id: str, upstream_path: str | None) -> str | None:
    if upstream_path is None:
        return None
    return f"{upstream_path}#mapping_rows[feature_id={feature_id}]"


def _evidence_limitations(
    feature_id: str,
    *,
    metadata: Mapping[str, Any],
    as_of_metadata: Mapping[str, Any],
    source_data_cutoff: str | None,
) -> list[str]:
    if feature_id == "growth_tilt_engine_signal_artifact":
        return [
            "standalone_signal_artifact_metadata_missing",
            "source_config_missing",
            "source_snapshot_reference_missing",
        ]
    limitations: list[str] = []
    if not metadata.get("source_snapshot_hash"):
        limitations.append("source_snapshot_hash_not_available_in_prior_artifacts")
    if not source_data_cutoff:
        limitations.append("source_data_cutoff_not_available_in_prior_artifacts")
    if feature_id == "trend_features" and not as_of_metadata:
        limitations.append("as_of_contract_metadata_still_requires_PIT_recheck")
    if limitations:
        limitations.append("does_not_mark_contract_or_PIT_gate_ready")
    return limitations


def _missing_fields(closure_plan_row: Mapping[str, Any]) -> list[str]:
    fields = (
        "missing_source_config",
        "missing_source_artifact",
        "missing_owner_module",
        "missing_generated_at",
        "missing_source_data_cutoff",
        "missing_feature_version",
    )
    return [field.removeprefix("missing_") for field in fields if closure_plan_row.get(field)]


def _pit_registry_severity(registry: Mapping[str, Any], input_id: str) -> str | None:
    for entry in _as_list(registry.get("entries")):
        mapping = _as_mapping(entry)
        if mapping.get("input_id") == input_id:
            return _text(mapping.get("severity")) or None
    return None


def _feature_id(row: Any) -> str:
    mapping = _as_mapping(row)
    return _text(mapping.get("feature_id") or mapping.get("source_feature_id"))


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any, default: str = "") -> str:
    return value if isinstance(value, str) and value else default


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = _text(value)
        if text:
            return text
    return None


def _int(value: Any) -> int:
    return value if isinstance(value, int) else 0
