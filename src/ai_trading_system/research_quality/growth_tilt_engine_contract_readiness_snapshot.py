from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_contract_readiness_snapshot.v1"
CONTRACT_EVIDENCE_MAP_SCHEMA_VERSION = (
    "growth_tilt_engine_contract_evidence_map.v1"
)
CONTRACT_GAP_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_contract_gap_summary.v1"
)
CONTRACT_REQUIREMENTS_SCHEMA_VERSION = (
    "growth_tilt_engine_contract_requirements.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_BLOCKED_BY_CONTRACT_GAPS"
)
NEXT_ROUTE_READY = "TRADING-2423_Growth_Tilt_Engine_Paper_Shadow_Preflight"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2423_Growth_Tilt_Engine_Contract_Gap_Remediation"
)
PRIOR_ROUTE = (
    "TRADING-2421_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck_After_Source_"
    "Traceability_Remediation"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
ARTIFACT_ID = "growth_tilt_engine_signal_artifact"
REPORT_TYPE = "growth_tilt_engine_contract_readiness_snapshot"

PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_AFTER_SOURCE_TRACEABILITY_"
    "REMEDIATION_READY"
)
SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS = (
    "GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_READY"
)

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
    "remediation",
    "growth_tilt_engine_signal_artifact_source_traceability_remediation",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-contract-readiness-snapshot",
    "outputs/research_strategies/growth_tilt_engine_contract_readiness_snapshot/"
    "contract_readiness_snapshot_result.json",
    "outputs/research_strategies/growth_tilt_engine_contract_readiness_snapshot/"
    "contract_evidence_map.json",
    "outputs/research_strategies/growth_tilt_engine_contract_readiness_snapshot/"
    "contract_gap_summary.json",
    "outputs/research_strategies/growth_tilt_engine_contract_readiness_snapshot/"
    "contract_requirements.json",
    "docs/research/growth_tilt_engine_contract_readiness_snapshot.md",
    "docs/research/growth_tilt_engine_contract_evidence_map.md",
    "docs/research/growth_tilt_engine_contract_gap_summary.md",
    "docs/research/dynamic_strategy_2423_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-contract-readiness-snapshot",
    NEXT_ROUTE_READY,
    "GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_READY",
)


def build_growth_tilt_engine_contract_readiness_snapshot(
    readiness_recheck_after_remediation_2421: Mapping[str, Any],
    remediation_result_2420: Mapping[str, Any],
    source_traceability_manifest_2420: Mapping[str, Any],
    source_lineage_map_2420: Mapping[str, Any],
    missing_source_evidence_summary_2420: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    manifest = _section(
        source_traceability_manifest_2420,
        "source_traceability_manifest",
    )
    lineage_map = _section(source_lineage_map_2420, "source_lineage_map")
    missing_summary = _section(
        missing_source_evidence_summary_2420,
        "missing_source_evidence_summary",
    )
    research_docs = research_doc_texts or {}
    requirements = _contract_requirements(
        readiness_recheck_after_remediation_2421,
        remediation_result_2420,
        manifest,
        lineage_map,
        missing_summary,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        system_flow_text=system_flow_text,
        research_doc_texts=research_docs,
    )
    gaps = [
        _gap_from_requirement(requirement)
        for requirement in requirements
        if requirement["status"] != "PASS"
    ]
    contract_ready = not gaps
    status = READY_STATUS if contract_ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if contract_ready else NEXT_ROUTE_BLOCKED
    missing_contract_evidence_count = sum(
        1 for gap in gaps if gap["classification"] == "missing_contract_evidence"
    )
    incomplete_contract_field_count = sum(
        1 for gap in gaps if gap["classification"] == "incomplete_contract_field"
    )
    contract_requirements = _contract_requirements_artifact(requirements)
    contract_gap_summary = _contract_gap_summary(
        status=status,
        gaps=gaps,
        missing_contract_evidence_count=missing_contract_evidence_count,
        incomplete_contract_field_count=incomplete_contract_field_count,
    )
    contract_evidence_map = _contract_evidence_map(
        status=status,
        requirements=requirements,
    )
    remaining_blockers = _as_list(
        readiness_recheck_after_remediation_2421.get("remaining_blockers")
    )
    pit_gate_ready = readiness_recheck_after_remediation_2421.get("pit_gate_ready") is True
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2422",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": ["TRADING-2420", "TRADING-2421"],
        "artifact_id": ARTIFACT_ID,
        "source_traceability_remediation_status": remediation_result_2420.get(
            "remediation_status"
        )
        or readiness_recheck_after_remediation_2421.get(
            "source_traceability_remediation_status"
        ),
        "source_traceability_recheck_status": (
            readiness_recheck_after_remediation_2421.get(
                "source_traceability_recheck_status"
            )
        ),
        "source_traceability_evidence_complete_after_2420": (
            readiness_recheck_after_remediation_2421.get(
                "source_traceability_evidence_complete_after_2420"
            )
            is True
            and remediation_result_2420.get("source_traceability_evidence_complete")
            is True
        ),
        "source_traceability_blocker_resolved_before_2422": (
            readiness_recheck_after_remediation_2421.get(
                "source_traceability_blocker_resolved"
            )
            is True
        ),
        "blockers_resolved_by_snapshot": False,
        "blockers_downgraded_by_snapshot": False,
        "resolved_blockers": _as_list(
            readiness_recheck_after_remediation_2421.get("resolved_blockers")
        ),
        "remaining_blockers": remaining_blockers,
        "remaining_blocker_count": len(remaining_blockers),
        "blocker_classification": _as_mapping(
            readiness_recheck_after_remediation_2421.get("blocker_classification")
        ),
        "pit_gate_ready": pit_gate_ready,
        "pit_gate_ready_count": _int_value(
            readiness_recheck_after_remediation_2421.get("pit_gate_ready_count")
        ),
        "pit_gate_blocked_count": _int_value(
            readiness_recheck_after_remediation_2421.get("pit_gate_blocked_count")
        ),
        "contract_ready": contract_ready,
        "contract_ready_count": 1 if contract_ready else 0,
        "contract_gap_count": len(gaps),
        "contract_gap_ids": [gap["requirement_id"] for gap in gaps],
        "missing_contract_evidence_count": missing_contract_evidence_count,
        "incomplete_contract_field_count": incomplete_contract_field_count,
        "contract_requirement_count": len(requirements),
        "contract_requirement_pass_count": len(requirements) - len(gaps),
        "contract_requirement_fail_count": len(gaps),
        "contract_readiness_snapshot_completed": True,
        "contract_readiness_snapshot_ready": contract_ready,
        "contract_evidence_map": contract_evidence_map,
        "contract_gap_summary": contract_gap_summary,
        "contract_requirements": contract_requirements,
        "paper_shadow_preflight_required": contract_ready,
        "paper_shadow_preflight_started": False,
        "paper_shadow_preflight_candidate_ready": contract_ready,
        "paper_shadow_blocked": True,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
        "auto_mark_pit_gate_ready": False,
        "auto_mark_contract_ready": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "new_feature_generated": False,
        "new_signal_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "PIT gate is ready and contract evidence is complete; TRADING-2423 "
            "must run paper-shadow preflight before any paper-shadow path starts."
            if contract_ready
            else "Contract readiness snapshot found gaps; TRADING-2423 must close "
            "contract evidence gaps before paper-shadow preflight."
        ),
    }


def _contract_requirements(
    readiness_recheck_after_remediation_2421: Mapping[str, Any],
    remediation_result_2420: Mapping[str, Any],
    manifest: Mapping[str, Any],
    lineage_map: Mapping[str, Any],
    missing_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str],
) -> list[dict[str, Any]]:
    report_ids = {
        item.get("report_id")
        for item in _as_list(report_registry.get("reports"))
        if isinstance(item, Mapping)
    }
    missing_reports = [
        report_id for report_id in REQUIRED_REPORT_IDS if report_id not in report_ids
    ]
    missing_catalog_refs = [
        reference
        for reference in REQUIRED_CATALOG_REFERENCES
        if reference not in artifact_catalog_text
    ]
    missing_flow_refs = [
        reference
        for reference in REQUIRED_SYSTEM_FLOW_REFERENCES
        if reference not in system_flow_text
    ]
    missing_docs = [doc_id for doc_id, text in research_doc_texts.items() if not text]
    return [
        _requirement(
            requirement_id="pit_gate_ready_after_2421",
            description="2421 after-remediation recheck must mark PIT gate ready.",
            passed=(
                readiness_recheck_after_remediation_2421.get("status")
                == PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS
                and readiness_recheck_after_remediation_2421.get("pit_gate_ready")
                is True
                and _int_value(
                    readiness_recheck_after_remediation_2421.get(
                        "pit_gate_ready_count"
                    )
                )
                == 1
            ),
            evidence={
                "status": readiness_recheck_after_remediation_2421.get("status"),
                "pit_gate_ready": readiness_recheck_after_remediation_2421.get(
                    "pit_gate_ready"
                ),
                "pit_gate_ready_count": readiness_recheck_after_remediation_2421.get(
                    "pit_gate_ready_count"
                ),
            },
            failure_message="TRADING-2421 PIT gate readiness evidence is not ready.",
            classification="incomplete_contract_field",
        ),
        _requirement(
            requirement_id="remaining_pit_blockers_closed",
            description="2421 remaining PIT blockers must be empty.",
            passed=(
                _as_list(
                    readiness_recheck_after_remediation_2421.get(
                        "remaining_blockers"
                    )
                )
                == []
                and _int_value(
                    readiness_recheck_after_remediation_2421.get(
                        "remaining_blocker_count"
                    )
                )
                == 0
            ),
            evidence={
                "remaining_blockers": readiness_recheck_after_remediation_2421.get(
                    "remaining_blockers"
                ),
                "remaining_blocker_count": readiness_recheck_after_remediation_2421.get(
                    "remaining_blocker_count"
                ),
            },
            failure_message="TRADING-2421 still has remaining PIT blockers.",
            classification="incomplete_contract_field",
        ),
        _requirement(
            requirement_id="source_traceability_remediation_ready",
            description="2420 source traceability remediation must remain READY.",
            passed=_source_traceability_ready(
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                manifest,
                lineage_map,
                missing_summary,
            ),
            evidence={
                "2420_status": remediation_result_2420.get("status"),
                "2420_remediation_status": remediation_result_2420.get(
                    "remediation_status"
                ),
                "2421_recheck_status": readiness_recheck_after_remediation_2421.get(
                    "source_traceability_recheck_status"
                ),
                "manifest_traceability_status": manifest.get("traceability_status"),
                "missing_field_count": missing_summary.get("missing_field_count"),
                "incomplete_field_count": missing_summary.get(
                    "incomplete_field_count"
                ),
                "unresolved_blocker_count": missing_summary.get(
                    "unresolved_blocker_count"
                ),
                "lineage_dependency_count": len(
                    _as_list(lineage_map.get("upstream_dependencies"))
                ),
            },
            failure_message=(
                "2420 source traceability remediation is not fully traceable."
            ),
            classification="incomplete_contract_field",
        ),
        _requirement(
            requirement_id="report_registry_registered",
            description="Report registry must include 2420, 2421, and 2422 reports.",
            passed=not missing_reports,
            evidence={"missing_report_ids": missing_reports},
            failure_message="Report registry is missing required report ids.",
            classification="missing_contract_evidence",
        ),
        _requirement(
            requirement_id="artifact_catalog_registered",
            description="Artifact catalog must include 2422 command and artifacts.",
            passed=not missing_catalog_refs,
            evidence={"missing_catalog_references": missing_catalog_refs},
            failure_message="Artifact catalog is missing required 2422 references.",
            classification="missing_contract_evidence",
        ),
        _requirement(
            requirement_id="system_flow_registered",
            description="System flow must include 2422 route and READY status.",
            passed=not missing_flow_refs,
            evidence={"missing_system_flow_references": missing_flow_refs},
            failure_message="System flow is missing required 2422 references.",
            classification="missing_contract_evidence",
        ),
        _requirement(
            requirement_id="research_docs_registered",
            description="Required 2420, 2421, and 2422 research docs must be readable.",
            passed=not missing_docs,
            evidence={"missing_research_docs": missing_docs},
            failure_message="One or more required research docs are missing.",
            classification="missing_contract_evidence",
        ),
        _requirement(
            requirement_id="paper_shadow_boundary_disabled",
            description="Paper-shadow path must remain disabled in 2422.",
            passed=_fields_false(
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=("paper_shadow_enabled", "paper_trade_created"),
            ),
            evidence={
                "2421_paper_shadow_enabled": (
                    readiness_recheck_after_remediation_2421.get(
                        "paper_shadow_enabled"
                    )
                ),
                "2420_paper_shadow_enabled": remediation_result_2420.get(
                    "paper_shadow_enabled"
                ),
            },
            failure_message="Paper-shadow boundary is not disabled.",
            classification="incomplete_contract_field",
        ),
        _requirement(
            requirement_id="production_boundary_disabled",
            description="Production path must remain disabled in 2422.",
            passed=_fields_false(
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=("production_enabled", "production_allowed"),
            ),
            evidence={
                "2421_production_enabled": readiness_recheck_after_remediation_2421.get(
                    "production_enabled"
                ),
                "2420_production_enabled": remediation_result_2420.get(
                    "production_enabled"
                ),
            },
            failure_message="Production boundary is not disabled.",
            classification="incomplete_contract_field",
        ),
        _requirement(
            requirement_id="broker_boundary_disabled",
            description="Broker and order paths must remain disabled in 2422.",
            passed=(
                _fields_false(
                    readiness_recheck_after_remediation_2421,
                    remediation_result_2420,
                    fields=("broker_enabled", "broker_action_enabled", "order_generated"),
                )
                and readiness_recheck_after_remediation_2421.get("broker_action")
                == "none"
                and remediation_result_2420.get("broker_action") == "none"
            ),
            evidence={
                "2421_broker_enabled": readiness_recheck_after_remediation_2421.get(
                    "broker_enabled"
                ),
                "2420_broker_enabled": remediation_result_2420.get("broker_enabled"),
                "2421_broker_action": readiness_recheck_after_remediation_2421.get(
                    "broker_action"
                ),
                "2420_broker_action": remediation_result_2420.get("broker_action"),
            },
            failure_message="Broker boundary is not disabled.",
            classification="incomplete_contract_field",
        ),
        _requirement(
            requirement_id="manual_review_boundary_required",
            description="Manual review boundary must remain required.",
            passed=(
                readiness_recheck_after_remediation_2421.get(
                    "manual_review_required"
                )
                is True
                and remediation_result_2420.get("manual_review_required") is True
            ),
            evidence={
                "2421_manual_review_required": (
                    readiness_recheck_after_remediation_2421.get(
                        "manual_review_required"
                    )
                ),
                "2420_manual_review_required": remediation_result_2420.get(
                    "manual_review_required"
                ),
            },
            failure_message="Manual review boundary is not explicitly required.",
            classification="incomplete_contract_field",
        ),
    ]


def _source_traceability_ready(
    readiness_recheck_after_remediation_2421: Mapping[str, Any],
    remediation_result_2420: Mapping[str, Any],
    manifest: Mapping[str, Any],
    lineage_map: Mapping[str, Any],
    missing_summary: Mapping[str, Any],
) -> bool:
    return (
        remediation_result_2420.get("status")
        == SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS
        and remediation_result_2420.get("remediation_status") == "READY"
        and remediation_result_2420.get("artifact_id") == ARTIFACT_ID
        and remediation_result_2420.get("source_traceability_evidence_complete")
        is True
        and readiness_recheck_after_remediation_2421.get(
            "source_traceability_recheck_status"
        )
        == "ACCEPTED"
        and readiness_recheck_after_remediation_2421.get(
            "source_traceability_blocker_resolved"
        )
        is True
        and manifest.get("artifact_id") == ARTIFACT_ID
        and manifest.get("traceability_status") == "READY"
        and _as_list(manifest.get("source_artifacts")) != []
        and _as_list(manifest.get("source_documents")) != []
        and lineage_map.get("artifact_id") == ARTIFACT_ID
        and _as_list(lineage_map.get("upstream_dependencies")) != []
        and _as_list(lineage_map.get("source_documents")) != []
        and _int_value(missing_summary.get("missing_field_count")) == 0
        and _int_value(missing_summary.get("incomplete_field_count")) == 0
        and _int_value(missing_summary.get("unresolved_blocker_count")) == 0
        and missing_summary.get("prior_missing_evidence_closed_by_2420") is True
    )


def _fields_false(
    *documents: Mapping[str, Any],
    fields: tuple[str, ...],
) -> bool:
    for document in documents:
        for field in fields:
            if field in document and document.get(field) is not False:
                return False
    return True


def _requirement(
    *,
    requirement_id: str,
    description: str,
    passed: bool,
    evidence: Mapping[str, Any],
    failure_message: str,
    classification: str,
) -> dict[str, Any]:
    return {
        "requirement_id": requirement_id,
        "description": description,
        "status": "PASS" if passed else "FAIL",
        "classification": classification,
        "evidence": dict(evidence),
        "failure_message": "" if passed else failure_message,
        "production_effect": "none",
        "broker_action": "none",
    }


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requirement_id": requirement.get("requirement_id"),
        "classification": requirement.get("classification"),
        "gap": requirement.get("failure_message"),
        "evidence": requirement.get("evidence", {}),
        "production_effect": "none",
        "broker_action": "none",
    }


def _contract_requirements_artifact(
    requirements: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": CONTRACT_REQUIREMENTS_SCHEMA_VERSION,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "requirement_count": len(requirements),
        "passed_requirement_count": sum(
            1 for requirement in requirements if requirement["status"] == "PASS"
        ),
        "failed_requirement_count": sum(
            1 for requirement in requirements if requirement["status"] == "FAIL"
        ),
        "requirements": requirements,
        "production_effect": "none",
        "broker_action": "none",
    }


def _contract_evidence_map(
    *,
    status: str,
    requirements: list[dict[str, Any]],
) -> dict[str, Any]:
    rows = [
        {
            "requirement_id": requirement["requirement_id"],
            "evidence_status": requirement["status"],
            "evidence": requirement["evidence"],
            "classification": requirement["classification"],
            "source_tasks": ["TRADING-2420", "TRADING-2421"],
            "production_effect": "none",
            "broker_action": "none",
        }
        for requirement in requirements
    ]
    return {
        "schema_version": CONTRACT_EVIDENCE_MAP_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "evidence_row_count": len(rows),
        "evidence_rows": rows,
        "production_effect": "none",
        "broker_action": "none",
    }


def _contract_gap_summary(
    *,
    status: str,
    gaps: list[dict[str, Any]],
    missing_contract_evidence_count: int,
    incomplete_contract_field_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": CONTRACT_GAP_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "contract_gap_count": len(gaps),
        "missing_contract_evidence_count": missing_contract_evidence_count,
        "incomplete_contract_field_count": incomplete_contract_field_count,
        "gaps": gaps,
        "next_route": NEXT_ROUTE_READY if not gaps else NEXT_ROUTE_BLOCKED,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if isinstance(value, Mapping):
        return value
    return document


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int_value(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0
