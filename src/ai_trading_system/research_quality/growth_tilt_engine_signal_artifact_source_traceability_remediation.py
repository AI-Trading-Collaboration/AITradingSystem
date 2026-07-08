from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = (
    "growth_tilt_engine_signal_artifact_source_traceability_remediation.v1"
)
SOURCE_TRACEABILITY_MANIFEST_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_artifact_source_traceability_manifest.v1"
)
SOURCE_LINEAGE_MAP_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_artifact_source_lineage_map.v1"
)
MISSING_SOURCE_EVIDENCE_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_artifact_missing_source_evidence_summary.v1"
)

READY_STATUS = (
    "GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_READY"
)
BLOCKED_MISSING_EVIDENCE_STATUS = (
    "GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_"
    "BLOCKED_BY_MISSING_EVIDENCE"
)
NEXT_ROUTE_READY = (
    "TRADING-2421_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck_After_"
    "Source_Traceability_Remediation"
)
NEXT_ROUTE_BLOCKED = (
    "TRADING-2421_Growth_Tilt_Engine_Source_Traceability_Missing_Evidence_Closure"
)
ARTIFACT_ID = "growth_tilt_engine_signal_artifact"
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"

SOURCE_ARTIFACT_SPECS: tuple[dict[str, str], ...] = (
    {
        "artifact_id": "growth_tilt_engine_pit_gate_readiness_recheck_result",
        "path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_pit_gate_readiness_recheck/"
            "readiness_recheck_result.json"
        ),
        "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
        "source_task": "TRADING-2419",
        "evidence_type": "pit_gate_readiness_recheck_blocker_state",
    },
    {
        "artifact_id": "growth_tilt_engine_pit_gate_recheck_blocker_classification",
        "path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_pit_gate_readiness_recheck/"
            "blocker_classification.json"
        ),
        "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
        "source_task": "TRADING-2419",
        "evidence_type": "blocker_classification",
    },
    {
        "artifact_id": "growth_tilt_engine_valid_until_dependency_evidence",
        "path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_valid_until_dependency_evidence_closure/"
            "valid_until_dependency_evidence.json"
        ),
        "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
        "source_task": "TRADING-2418",
        "evidence_type": "valid_until_dependency_closure",
    },
    {
        "artifact_id": "growth_tilt_engine_signal_validity_contract_evidence",
        "path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_valid_until_dependency_evidence_closure/"
            "signal_validity_contract_evidence.json"
        ),
        "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
        "source_task": "TRADING-2418",
        "evidence_type": "signal_validity_contract",
    },
    {
        "artifact_id": "growth_tilt_engine_stale_signal_policy_evidence",
        "path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_valid_until_dependency_evidence_closure/"
            "stale_signal_policy_evidence.json"
        ),
        "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
        "source_task": "TRADING-2418",
        "evidence_type": "stale_signal_policy",
    },
    {
        "artifact_id": "growth_tilt_engine_valid_until_alignment_evidence",
        "path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_valid_until_dependency_evidence_closure/"
            "growth_tilt_valid_until_alignment_evidence.json"
        ),
        "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
        "source_task": "TRADING-2418",
        "evidence_type": "growth_tilt_valid_until_alignment",
    },
    {
        "artifact_id": "growth_tilt_engine_source_traceability_closure_evidence",
        "path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_source_traceability_upstream_artifact_closure/"
            "source_traceability_closure_evidence.json"
        ),
        "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
        "source_task": "TRADING-2417",
        "evidence_type": "source_traceability_pre_recheck_closure",
    },
    {
        "artifact_id": "growth_tilt_engine_upstream_artifact_closure_evidence",
        "path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_source_traceability_upstream_artifact_closure/"
            "upstream_artifact_closure_evidence.json"
        ),
        "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
        "source_task": "TRADING-2417",
        "evidence_type": "upstream_artifact_pre_recheck_closure",
    },
)

SOURCE_DOCUMENT_SPECS: tuple[dict[str, str], ...] = (
    {
        "document_id": "growth_tilt_engine_pit_gate_readiness_recheck_doc",
        "path": "docs/research/growth_tilt_engine_pit_gate_readiness_recheck.md",
        "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
        "source_task": "TRADING-2419",
    },
    {
        "document_id": "growth_tilt_engine_signal_artifact_blocker_doc",
        "path": (
            "docs/research/"
            "growth_tilt_engine_signal_artifact_source_traceability_blocker.md"
        ),
        "report_id": "growth_tilt_engine_pit_gate_readiness_recheck",
        "source_task": "TRADING-2419",
    },
    {
        "document_id": "growth_tilt_engine_valid_until_dependency_evidence_doc",
        "path": (
            "docs/research/"
            "growth_tilt_engine_valid_until_dependency_evidence_closure.md"
        ),
        "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
        "source_task": "TRADING-2418",
    },
    {
        "document_id": "growth_tilt_engine_source_traceability_closure_evidence_doc",
        "path": (
            "docs/research/"
            "growth_tilt_engine_source_traceability_closure_evidence.md"
        ),
        "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
        "source_task": "TRADING-2417",
    },
    {
        "document_id": "growth_tilt_engine_upstream_artifact_closure_evidence_doc",
        "path": (
            "docs/research/growth_tilt_engine_upstream_artifact_closure_evidence.md"
        ),
        "report_id": "growth_tilt_engine_source_traceability_upstream_artifact_closure",
        "source_task": "TRADING-2417",
    },
)

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    "growth_tilt_engine_signal_artifact_source_traceability_remediation",
    "growth_tilt_engine_pit_gate_readiness_recheck",
    "growth_tilt_engine_valid_until_dependency_evidence_closure",
    "growth_tilt_engine_source_traceability_upstream_artifact_closure",
)


def build_growth_tilt_signal_artifact_source_traceability_remediation(
    readiness_recheck_2419: Mapping[str, Any],
    valid_until_dependency_evidence_2418: Mapping[str, Any],
    signal_validity_contract_evidence_2418: Mapping[str, Any],
    stale_signal_policy_evidence_2418: Mapping[str, Any],
    growth_tilt_valid_until_alignment_evidence_2418: Mapping[str, Any],
    source_traceability_closure_evidence_2417: Mapping[str, Any],
    upstream_artifact_closure_evidence_2417: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    source_file_manifest: Mapping[str, Mapping[str, Any]] | None = None,
    source_document_manifest: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    registry_entries = _source_registry_entries(report_registry, artifact_catalog_text)
    source_artifacts = _source_artifacts(
        report_registry,
        artifact_catalog_text,
        source_file_manifest or {},
    )
    source_documents = _source_documents(
        report_registry,
        artifact_catalog_text,
        source_document_manifest or {},
    )
    source_generation_commands = _source_generation_commands(
        registry_entries,
        artifact_catalog_text,
    )
    valid_until_boundary = _valid_until_boundary(
        valid_until_dependency_evidence_2418,
        signal_validity_contract_evidence_2418,
        stale_signal_policy_evidence_2418,
        growth_tilt_valid_until_alignment_evidence_2418,
    )
    dependency_closure_reference = _dependency_closure_reference(
        valid_until_dependency_evidence_2418,
    )
    prior_blocker = _prior_signal_artifact_blocker(readiness_recheck_2419)
    prior_missing_evidence = _prior_missing_signal_artifact_evidence(
        source_traceability_closure_evidence_2417,
        upstream_artifact_closure_evidence_2417,
    )
    source_lineage_map = _source_lineage_map(
        source_artifacts,
        source_documents,
        valid_until_boundary,
        dependency_closure_reference,
    )
    missing_summary = _missing_source_evidence_summary(
        readiness_recheck_2419,
        source_artifacts,
        source_documents,
        registry_entries,
        source_generation_commands,
        valid_until_boundary,
        dependency_closure_reference,
        prior_blocker=prior_blocker,
        prior_missing_evidence=prior_missing_evidence,
    )
    ready = (
        missing_summary["missing_field_count"] == 0
        and missing_summary["incomplete_field_count"] == 0
        and missing_summary["unresolved_blocker_count"] == 0
    )
    status = READY_STATUS if ready else BLOCKED_MISSING_EVIDENCE_STATUS
    manifest = {
        "schema_version": SOURCE_TRACEABILITY_MANIFEST_SCHEMA_VERSION,
        "artifact_id": ARTIFACT_ID,
        "traceability_status": "READY" if ready else "BLOCKED",
        "source_evidence_type": "standalone_signal_artifact_traceability_manifest",
        "source_artifacts": source_artifacts,
        "source_documents": source_documents,
        "source_registry_entries": registry_entries,
        "source_generation_commands": source_generation_commands,
        "as_of": readiness_recheck_2419.get("as_of"),
        "source_timestamp_boundary": {
            "as_of": readiness_recheck_2419.get("as_of"),
            "generated_at": readiness_recheck_2419.get("generated_at"),
            "source_data_cutoff": readiness_recheck_2419.get("as_of"),
            "fresh_market_data_read": False,
        },
        "valid_until_boundary": valid_until_boundary,
        "dependency_closure_reference": dependency_closure_reference,
        "prior_blocker_classification": prior_blocker,
        "prior_missing_evidence_reference": prior_missing_evidence,
        "pit_gate_ready_after_2420": False,
        "contract_ready_after_2420": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2420",
        "status": status,
        "remediation_status": "READY" if ready else "BLOCKED",
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "artifact_id": ARTIFACT_ID,
        "source_tasks": ["TRADING-2417", "TRADING-2418", "TRADING-2419"],
        "source_traceability_manifest": manifest,
        "source_lineage_map": source_lineage_map,
        "missing_source_evidence_summary": missing_summary,
        "source_traceability_evidence_complete": ready,
        "source_traceability_blocker_resolved": ready,
        "blocker_resolved": ready,
        "blocker_downgraded": False,
        "pit_gate_ready": False,
        "pit_gate_ready_count": 0,
        "contract_ready": False,
        "contract_ready_count": 0,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "daily_report_generated": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "new_signal_generated": False,
        "new_feature_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "recommended_next_research_task": (
            NEXT_ROUTE_READY if ready else NEXT_ROUTE_BLOCKED
        ),
        "recommended_next_research_task_reason": (
            "Signal artifact source traceability evidence chain is complete; "
            "TRADING-2421 must independently recheck PIT gate readiness."
            if ready
            else "Signal artifact source traceability evidence remains incomplete; "
            "TRADING-2421 must close missing evidence before PIT gate recheck."
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_artifacts(
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    source_file_manifest: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            **spec,
            "report_registry_present": _registry_has_report(
                report_registry,
                spec["report_id"],
            ),
            "catalog_reference_present": spec["path"] in artifact_catalog_text,
            "source_file_present": _source_file_record(
                source_file_manifest,
                spec["path"],
            ).get("exists"),
            "source_file_checksum": _source_file_record(
                source_file_manifest,
                spec["path"],
            ).get("sha256"),
        }
        for spec in SOURCE_ARTIFACT_SPECS
    ]


def _source_documents(
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    source_document_manifest: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            **spec,
            "report_registry_present": _registry_has_report(
                report_registry,
                spec["report_id"],
            ),
            "catalog_reference_present": spec["path"] in artifact_catalog_text,
            "document_present": _source_file_record(
                source_document_manifest,
                spec["path"],
            ).get("exists"),
        }
        for spec in SOURCE_DOCUMENT_SPECS
    ]


def _source_registry_entries(
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
) -> list[dict[str, Any]]:
    entries_by_id = _report_entries_by_id(report_registry)
    rows: list[dict[str, Any]] = []
    for report_id in REQUIRED_REPORT_IDS:
        entry = entries_by_id.get(report_id, {})
        command = _text(entry.get("command"))
        rows.append(
            {
                "report_id": report_id,
                "registry_present": bool(entry),
                "command": command or None,
                "command_catalog_reference_present": (
                    bool(command) and command in artifact_catalog_text
                ),
                "artifact_globs": list(_as_list(entry.get("artifact_globs"))),
                "production_effect": entry.get("production_effect"),
                "broker_action": entry.get("broker_action"),
            }
        )
    return rows


def _source_generation_commands(
    registry_entries: list[Mapping[str, Any]],
    artifact_catalog_text: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entry in registry_entries:
        command = _text(entry.get("command"))
        if not command:
            continue
        rows.append(
            {
                "report_id": entry.get("report_id"),
                "command": command,
                "catalog_reference_present": command in artifact_catalog_text,
                "production_effect": entry.get("production_effect"),
                "broker_action": entry.get("broker_action"),
            }
        )
    return rows


def _valid_until_boundary(
    valid_until_dependency_evidence: Mapping[str, Any],
    signal_validity_contract_evidence: Mapping[str, Any],
    stale_signal_policy_evidence: Mapping[str, Any],
    growth_tilt_valid_until_alignment_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    dependency_rows = _evidence_rows(
        valid_until_dependency_evidence,
        "valid_until_dependency_evidence",
    )
    dependency_row = next(
        (
            row
            for row in dependency_rows
            if row.get("dependent_feature_or_signal") == "execution_signal_validity_policy"
        ),
        {},
    )
    contract = _section(
        signal_validity_contract_evidence,
        "signal_validity_contract_evidence",
    )
    stale = _section(stale_signal_policy_evidence, "stale_signal_policy_evidence")
    alignment = _section(
        growth_tilt_valid_until_alignment_evidence,
        "growth_tilt_valid_until_alignment_evidence",
    )
    return {
        "boundary_explicit": bool(
            dependency_row.get("ready_for_pit_gate_recheck") is True
            and dependency_row.get("valid_until_source")
            and contract.get("ready_for_recheck") is True
        ),
        "dependency_id": dependency_row.get("dependency_id"),
        "valid_from_source": dependency_row.get("valid_from_source"),
        "valid_until_source": dependency_row.get("valid_until_source"),
        "stale_after_source": dependency_row.get("stale_after_source"),
        "policy_window_bdays": dependency_row.get("policy_window_bdays"),
        "execution_lag_bdays": dependency_row.get("execution_lag_bdays"),
        "required_fields": list(_as_list(contract.get("required_fields"))),
        "missing_field_count": contract.get("missing_field_count"),
        "stale_policy_ready": stale.get("ready_for_recheck"),
        "growth_tilt_alignment_ready": alignment.get("ready_for_recheck"),
        "remaining_gap": (
            _as_mapping(_first(_as_list(alignment.get("alignment_rows")))).get(
                "remaining_gap"
            )
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _dependency_closure_reference(
    valid_until_dependency_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    section = _section(
        valid_until_dependency_evidence,
        "valid_until_dependency_evidence",
    )
    rows = _evidence_rows(valid_until_dependency_evidence, "valid_until_dependency_evidence")
    row = _as_mapping(_first(rows))
    return {
        "source_task": "TRADING-2418",
        "dependency_feature_id": section.get("dependency_feature_id"),
        "dependency_id": row.get("dependency_id"),
        "evidence_status": row.get("evidence_status"),
        "ready_for_pit_gate_recheck": row.get("ready_for_pit_gate_recheck"),
        "source_reference": row.get("source_reference"),
        "artifact_path": (
            "outputs/research_strategies/"
            "growth_tilt_engine_valid_until_dependency_evidence_closure/"
            "valid_until_dependency_evidence.json"
        ),
        "report_id": "growth_tilt_engine_valid_until_dependency_evidence_closure",
    }


def _prior_signal_artifact_blocker(
    readiness_recheck_2419: Mapping[str, Any],
) -> dict[str, Any]:
    classification = _section(readiness_recheck_2419, "blocker_classification")
    rows = [
        _as_mapping(row)
        for row in _as_list(classification.get("rows"))
        if _as_mapping(row).get("blocker_id") == ARTIFACT_ID
    ]
    row = _as_mapping(_first(rows))
    return {
        "source_task": "TRADING-2419",
        "status": readiness_recheck_2419.get("status"),
        "remaining_blocker": ARTIFACT_ID in set(
            _text(item) for item in _as_list(readiness_recheck_2419.get("remaining_blockers"))
        ),
        "blocker_classification": row.get("blocker_classification"),
        "still_blocked_after_recheck": row.get("still_blocked_after_recheck"),
        "recommended_next_task": row.get("recommended_next_task"),
    }


def _prior_missing_signal_artifact_evidence(
    source_traceability_closure_evidence_2417: Mapping[str, Any],
    upstream_artifact_closure_evidence_2417: Mapping[str, Any],
) -> dict[str, Any]:
    source_rows = _evidence_rows(
        source_traceability_closure_evidence_2417,
        "source_traceability_closure_evidence",
    )
    upstream_rows = _evidence_rows(
        upstream_artifact_closure_evidence_2417,
        "upstream_artifact_closure_evidence",
    )
    source_row = next((row for row in source_rows if row.get("feature_id") == ARTIFACT_ID), {})
    upstream_row = next(
        (row for row in upstream_rows if row.get("feature_id") == ARTIFACT_ID),
        {},
    )
    return {
        "source_task": "TRADING-2417",
        "source_traceability_evidence_ready_before_2420": source_row.get(
            "source_traceability_evidence_ready"
        ),
        "upstream_artifact_evidence_ready_before_2420": upstream_row.get(
            "upstream_artifact_available_after_2417"
        ),
        "prior_still_blocked_reason": source_row.get("still_blocked_reason"),
        "prior_traceability_closure_status": source_row.get(
            "traceability_closure_status"
        ),
        "prior_upstream_artifact_closure_status": upstream_row.get(
            "upstream_artifact_closure_status"
        ),
        "source_artifact_after_2417": source_row.get("source_artifact_after_2417"),
    }


def _source_lineage_map(
    source_artifacts: list[Mapping[str, Any]],
    source_documents: list[Mapping[str, Any]],
    valid_until_boundary: Mapping[str, Any],
    dependency_closure_reference: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SOURCE_LINEAGE_MAP_SCHEMA_VERSION,
        "artifact_id": ARTIFACT_ID,
        "upstream_dependencies": [
            {
                "artifact_id": row.get("artifact_id"),
                "path": row.get("path"),
                "report_id": row.get("report_id"),
                "evidence_type": row.get("evidence_type"),
                "source_task": row.get("source_task"),
            }
            for row in source_artifacts
        ],
        "source_documents": [
            {
                "document_id": row.get("document_id"),
                "path": row.get("path"),
                "source_task": row.get("source_task"),
            }
            for row in source_documents
        ],
        "valid_until_boundary": dict(valid_until_boundary),
        "dependency_closure_reference": dict(dependency_closure_reference),
        "downstream_consumers": [
            "growth_tilt_engine_pit_gate_readiness",
            NEXT_ROUTE_READY,
        ],
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _missing_source_evidence_summary(
    readiness_recheck_2419: Mapping[str, Any],
    source_artifacts: list[Mapping[str, Any]],
    source_documents: list[Mapping[str, Any]],
    registry_entries: list[Mapping[str, Any]],
    source_generation_commands: list[Mapping[str, Any]],
    valid_until_boundary: Mapping[str, Any],
    dependency_closure_reference: Mapping[str, Any],
    *,
    prior_blocker: Mapping[str, Any],
    prior_missing_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    missing_fields: list[str] = []
    incomplete_fields: list[str] = []
    unresolved_blockers: list[str] = []
    if readiness_recheck_2419.get("status") != (
        "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_"
        "BLOCKED_BY_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY"
    ):
        missing_fields.append("TRADING-2419 expected blocker status")
    if prior_blocker.get("blocker_classification") != "source_traceability":
        missing_fields.append("TRADING-2419 signal artifact source_traceability blocker")
    if prior_blocker.get("remaining_blocker") is not True:
        missing_fields.append("TRADING-2419 remaining blocker growth_tilt_engine_signal_artifact")
    if prior_missing_evidence.get("source_traceability_evidence_ready_before_2420") is not False:
        incomplete_fields.append("TRADING-2417 prior missing signal artifact evidence marker")
    for row in source_artifacts:
        if row.get("report_registry_present") is not True:
            missing_fields.append(f"report registry entry for {row.get('report_id')}")
        if row.get("catalog_reference_present") is not True:
            missing_fields.append(f"artifact catalog reference for {row.get('path')}")
        if row.get("source_file_present") is not True:
            missing_fields.append(f"source artifact file {row.get('path')}")
        if not row.get("source_file_checksum"):
            incomplete_fields.append(f"source artifact checksum for {row.get('path')}")
    for row in source_documents:
        if row.get("report_registry_present") is not True:
            missing_fields.append(f"document report registry entry for {row.get('report_id')}")
        if row.get("catalog_reference_present") is not True:
            missing_fields.append(f"artifact catalog document reference for {row.get('path')}")
        if row.get("document_present") is not True:
            missing_fields.append(f"source document {row.get('path')}")
    for row in registry_entries:
        if row.get("registry_present") is not True:
            missing_fields.append(f"report registry id {row.get('report_id')}")
        if row.get("command_catalog_reference_present") is not True:
            missing_fields.append(f"catalog command reference for {row.get('report_id')}")
        if row.get("production_effect") != "none":
            incomplete_fields.append(f"production_effect=none for {row.get('report_id')}")
        if row.get("broker_action") != "none":
            incomplete_fields.append(f"broker_action=none for {row.get('report_id')}")
    if not source_generation_commands:
        missing_fields.append("source generation commands")
    if valid_until_boundary.get("boundary_explicit") is not True:
        missing_fields.append("valid-until boundary")
    if dependency_closure_reference.get("ready_for_pit_gate_recheck") is not True:
        missing_fields.append("TRADING-2418 dependency closure reference")
    if missing_fields or incomplete_fields:
        unresolved_blockers.append(ARTIFACT_ID)
    return {
        "schema_version": MISSING_SOURCE_EVIDENCE_SUMMARY_SCHEMA_VERSION,
        "artifact_id": ARTIFACT_ID,
        "missing_fields": _dedupe(missing_fields),
        "missing_field_count": len(_dedupe(missing_fields)),
        "incomplete_fields": _dedupe(incomplete_fields),
        "incomplete_field_count": len(_dedupe(incomplete_fields)),
        "unresolved_blockers": _dedupe(unresolved_blockers),
        "unresolved_blocker_count": len(_dedupe(unresolved_blockers)),
        "prior_missing_evidence_closed_by_2420": not (missing_fields or incomplete_fields),
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_file_record(
    source_manifest: Mapping[str, Mapping[str, Any]],
    canonical_path: str,
) -> Mapping[str, Any]:
    if canonical_path in source_manifest:
        return source_manifest[canonical_path]
    normalized = canonical_path.replace("\\", "/")
    for path, record in source_manifest.items():
        if path.replace("\\", "/").endswith(normalized):
            return record
    return {}


def _report_entries_by_id(report_registry: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    return {
        _text(row.get("report_id")): row
        for row in _as_list(report_registry.get("reports"))
        if isinstance(row, Mapping)
    }


def _registry_has_report(report_registry: Mapping[str, Any], report_id: str) -> bool:
    return report_id in _report_entries_by_id(report_registry)


def _evidence_rows(document: Mapping[str, Any], section_key: str) -> list[Mapping[str, Any]]:
    section = _section(document, section_key)
    return [_as_mapping(row) for row in _as_list(section.get("evidence_rows"))]


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if isinstance(value, Mapping):
        return value
    return document


def _first(values: list[Any]) -> Any:
    return values[0] if values else {}


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result
