from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_paper_shadow_preflight.v1"
PREFLIGHT_CHECKLIST_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_preflight_checklist.v1"
)
PREFLIGHT_GAP_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_preflight_gap_summary.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT_BLOCKED_BY_PREFLIGHT_GAPS"
)
NEXT_ROUTE_READY = "TRADING-2424_Growth_Tilt_Engine_Paper_Shadow_Enablement_Plan"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2424_Growth_Tilt_Engine_Paper_Shadow_Preflight_Gap_Remediation"
)
PRIOR_ROUTE = "TRADING-2422_Growth_Tilt_Engine_Contract_Readiness_Snapshot"
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
ARTIFACT_ID = "growth_tilt_engine_signal_artifact"
REPORT_TYPE = "growth_tilt_engine_paper_shadow_preflight"

CONTRACT_READINESS_SNAPSHOT_READY_STATUS = (
    "GROWTH_TILT_ENGINE_CONTRACT_READINESS_SNAPSHOT_READY"
)
PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS = (
    "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_AFTER_SOURCE_TRACEABILITY_"
    "REMEDIATION_READY"
)
SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS = (
    "GROWTH_TILT_ENGINE_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY_REMEDIATION_READY"
)

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_contract_readiness_snapshot",
    "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
    "remediation",
    "growth_tilt_engine_signal_artifact_source_traceability_remediation",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-paper-shadow-preflight",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_preflight/"
    "paper_shadow_preflight_result.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_preflight/"
    "preflight_checklist.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_preflight/"
    "preflight_gap_summary.json",
    "docs/research/growth_tilt_engine_paper_shadow_preflight.md",
    "docs/research/growth_tilt_engine_paper_shadow_preflight_checklist.md",
    "docs/research/growth_tilt_engine_paper_shadow_preflight_gap_summary.md",
    "docs/research/dynamic_strategy_2424_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-paper-shadow-preflight",
    READY_STATUS,
    NEXT_ROUTE_READY,
)


def build_growth_tilt_engine_paper_shadow_preflight(
    contract_readiness_snapshot_2422: Mapping[str, Any],
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
    requirements = _preflight_requirements(
        contract_readiness_snapshot_2422,
        readiness_recheck_after_remediation_2421,
        remediation_result_2420,
        manifest,
        lineage_map,
        missing_summary,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        system_flow_text=system_flow_text,
        research_doc_texts=research_doc_texts or {},
    )
    gaps = [
        _gap_from_requirement(requirement)
        for requirement in requirements
        if requirement["status"] != "PASS"
    ]
    preflight_ready = not gaps
    status = READY_STATUS if preflight_ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if preflight_ready else NEXT_ROUTE_BLOCKED
    missing_preflight_evidence_count = sum(
        1 for gap in gaps if gap["classification"] == "missing_preflight_evidence"
    )
    safety_boundary_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "preflight_safety_boundary"
    )
    preflight_gap_summary = _preflight_gap_summary(
        status=status,
        gaps=gaps,
        missing_preflight_evidence_count=missing_preflight_evidence_count,
        safety_boundary_gap_count=safety_boundary_gap_count,
    )
    preflight_checklist = _preflight_checklist(
        status=status,
        requirements=requirements,
    )
    remaining_pit_blockers = _as_list(
        contract_readiness_snapshot_2422.get("remaining_blockers")
    )
    contract_gap_count = _int_value(
        contract_readiness_snapshot_2422.get("contract_gap_count")
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2423",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": ["TRADING-2420", "TRADING-2421", "TRADING-2422"],
        "artifact_id": ARTIFACT_ID,
        "pit_gate_ready": contract_readiness_snapshot_2422.get("pit_gate_ready")
        is True,
        "pit_gate_ready_count": _int_value(
            contract_readiness_snapshot_2422.get("pit_gate_ready_count")
        ),
        "pit_gate_blocked_count": _int_value(
            contract_readiness_snapshot_2422.get("pit_gate_blocked_count")
        ),
        "contract_readiness_status": contract_readiness_snapshot_2422.get("status"),
        "contract_ready": contract_readiness_snapshot_2422.get("contract_ready")
        is True,
        "contract_ready_count": _int_value(
            contract_readiness_snapshot_2422.get("contract_ready_count")
        ),
        "contract_gap_count": contract_gap_count,
        "missing_contract_evidence_count": _int_value(
            contract_readiness_snapshot_2422.get("missing_contract_evidence_count")
        ),
        "incomplete_contract_field_count": _int_value(
            contract_readiness_snapshot_2422.get("incomplete_contract_field_count")
        ),
        "remaining_pit_blockers": remaining_pit_blockers,
        "remaining_pit_blocker_count": len(remaining_pit_blockers),
        "source_traceability_remediation_status": remediation_result_2420.get(
            "remediation_status"
        ),
        "source_traceability_recheck_status": (
            contract_readiness_snapshot_2422.get("source_traceability_recheck_status")
            or readiness_recheck_after_remediation_2421.get(
                "source_traceability_recheck_status"
            )
        ),
        "source_traceability_accepted": (
            (
                contract_readiness_snapshot_2422.get(
                    "source_traceability_recheck_status"
                )
                or readiness_recheck_after_remediation_2421.get(
                    "source_traceability_recheck_status"
                )
            )
            == "ACCEPTED"
        ),
        "paper_shadow_preflight_started": True,
        "paper_shadow_preflight_completed": True,
        "paper_shadow_preflight_ready": preflight_ready,
        "paper_shadow_enablement_plan_required": preflight_ready,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "generated_signal": False,
        "generated_trading_advice": False,
        "new_signal_generated": False,
        "daily_report_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "preflight_requirement_count": len(requirements),
        "preflight_requirement_pass_count": len(requirements) - len(gaps),
        "preflight_requirement_fail_count": len(gaps),
        "preflight_gap_count": len(gaps),
        "preflight_gap_ids": [gap["requirement_id"] for gap in gaps],
        "missing_preflight_evidence_count": missing_preflight_evidence_count,
        "safety_boundary_gap_count": safety_boundary_gap_count,
        "preflight_checklist": preflight_checklist,
        "preflight_gap_summary": preflight_gap_summary,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "broker_action_enabled": False,
        "new_feature_generated": False,
        "production_effect": "none",
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "Paper-shadow preflight passed; TRADING-2424 must plan enablement "
            "before any paper-shadow runtime starts."
            if preflight_ready
            else "Paper-shadow preflight found gaps; TRADING-2424 must close "
            "preflight gaps before enablement planning."
        ),
    }


def _preflight_requirements(
    contract_readiness_snapshot_2422: Mapping[str, Any],
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
            requirement_id="contract_readiness_snapshot_ready",
            description="2422 contract readiness snapshot must be READY.",
            passed=(
                contract_readiness_snapshot_2422.get("status")
                == CONTRACT_READINESS_SNAPSHOT_READY_STATUS
                and contract_readiness_snapshot_2422.get("contract_ready") is True
                and _int_value(
                    contract_readiness_snapshot_2422.get("contract_ready_count")
                )
                == 1
            ),
            evidence={
                "status": contract_readiness_snapshot_2422.get("status"),
                "contract_ready": contract_readiness_snapshot_2422.get(
                    "contract_ready"
                ),
                "contract_ready_count": contract_readiness_snapshot_2422.get(
                    "contract_ready_count"
                ),
            },
            failure_message="TRADING-2422 contract readiness snapshot is not READY.",
            classification="preflight_gap",
        ),
        _requirement(
            requirement_id="pit_gate_ready",
            description="PIT gate must be ready before paper-shadow preflight passes.",
            passed=(
                contract_readiness_snapshot_2422.get("pit_gate_ready") is True
                and _int_value(
                    contract_readiness_snapshot_2422.get("pit_gate_ready_count")
                )
                == 1
                and readiness_recheck_after_remediation_2421.get("status")
                == PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS
            ),
            evidence={
                "2422_pit_gate_ready": contract_readiness_snapshot_2422.get(
                    "pit_gate_ready"
                ),
                "2422_pit_gate_ready_count": contract_readiness_snapshot_2422.get(
                    "pit_gate_ready_count"
                ),
                "2421_status": readiness_recheck_after_remediation_2421.get("status"),
            },
            failure_message="PIT gate is not ready.",
            classification="preflight_gap",
        ),
        _requirement(
            requirement_id="remaining_pit_blockers_empty",
            description="Remaining PIT blockers must be empty.",
            passed=(
                _as_list(contract_readiness_snapshot_2422.get("remaining_blockers"))
                == []
                and _as_list(
                    readiness_recheck_after_remediation_2421.get(
                        "remaining_blockers"
                    )
                )
                == []
            ),
            evidence={
                "2422_remaining_blockers": contract_readiness_snapshot_2422.get(
                    "remaining_blockers"
                ),
                "2421_remaining_blockers": (
                    readiness_recheck_after_remediation_2421.get(
                        "remaining_blockers"
                    )
                ),
            },
            failure_message="Remaining PIT blockers are not empty.",
            classification="preflight_gap",
        ),
        _requirement(
            requirement_id="contract_gap_count_zero",
            description="Contract gap count must be zero.",
            passed=(
                _int_value(contract_readiness_snapshot_2422.get("contract_gap_count"))
                == 0
                and _int_value(
                    contract_readiness_snapshot_2422.get(
                        "missing_contract_evidence_count"
                    )
                )
                == 0
                and _int_value(
                    contract_readiness_snapshot_2422.get(
                        "incomplete_contract_field_count"
                    )
                )
                == 0
            ),
            evidence={
                "contract_gap_count": contract_readiness_snapshot_2422.get(
                    "contract_gap_count"
                ),
                "missing_contract_evidence_count": (
                    contract_readiness_snapshot_2422.get(
                        "missing_contract_evidence_count"
                    )
                ),
                "incomplete_contract_field_count": (
                    contract_readiness_snapshot_2422.get(
                        "incomplete_contract_field_count"
                    )
                ),
            },
            failure_message="Contract gaps remain before paper-shadow preflight.",
            classification="preflight_gap",
        ),
        _requirement(
            requirement_id="source_traceability_accepted",
            description="Source traceability must remain accepted.",
            passed=_source_traceability_accepted(
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                manifest,
                lineage_map,
                missing_summary,
            ),
            evidence={
                "2422_source_traceability_recheck_status": (
                    contract_readiness_snapshot_2422.get(
                        "source_traceability_recheck_status"
                    )
                ),
                "2421_source_traceability_recheck_status": (
                    readiness_recheck_after_remediation_2421.get(
                        "source_traceability_recheck_status"
                    )
                ),
                "2420_status": remediation_result_2420.get("status"),
                "2420_remediation_status": remediation_result_2420.get(
                    "remediation_status"
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
            failure_message="Source traceability is not accepted.",
            classification="preflight_gap",
        ),
        _requirement(
            requirement_id="paper_shadow_not_enabled",
            description="Paper-shadow must not already be enabled.",
            passed=_fields_false(
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=(
                    "paper_shadow_enabled",
                    "paper_shadow_allowed",
                    "paper_shadow_schedule_enabled",
                    "paper_trade_created",
                    "shadow_position_created",
                ),
            ),
            evidence={
                "2422_paper_shadow_enabled": contract_readiness_snapshot_2422.get(
                    "paper_shadow_enabled"
                ),
                "2421_paper_shadow_enabled": readiness_recheck_after_remediation_2421.get(
                    "paper_shadow_enabled"
                ),
                "2420_paper_shadow_enabled": remediation_result_2420.get(
                    "paper_shadow_enabled"
                ),
            },
            failure_message="Paper-shadow is already enabled or mutated.",
            classification="preflight_safety_boundary",
        ),
        _requirement(
            requirement_id="production_disabled",
            description="Production path must remain disabled.",
            passed=_fields_false(
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=("production_enabled", "production_allowed"),
            ),
            evidence={
                "2422_production_enabled": contract_readiness_snapshot_2422.get(
                    "production_enabled"
                ),
                "2421_production_enabled": readiness_recheck_after_remediation_2421.get(
                    "production_enabled"
                ),
                "2420_production_enabled": remediation_result_2420.get(
                    "production_enabled"
                ),
            },
            failure_message="Production path is enabled.",
            classification="preflight_safety_boundary",
        ),
        _requirement(
            requirement_id="broker_disabled",
            description="Broker and order paths must remain disabled.",
            passed=(
                _fields_false(
                    contract_readiness_snapshot_2422,
                    readiness_recheck_after_remediation_2421,
                    remediation_result_2420,
                    fields=(
                        "broker_enabled",
                        "broker_action_enabled",
                        "order_generated",
                    ),
                )
                and contract_readiness_snapshot_2422.get("broker_action") == "none"
                and readiness_recheck_after_remediation_2421.get("broker_action")
                == "none"
                and remediation_result_2420.get("broker_action") == "none"
            ),
            evidence={
                "2422_broker_enabled": contract_readiness_snapshot_2422.get(
                    "broker_enabled"
                ),
                "2421_broker_enabled": readiness_recheck_after_remediation_2421.get(
                    "broker_enabled"
                ),
                "2420_broker_enabled": remediation_result_2420.get("broker_enabled"),
                "2422_broker_action": contract_readiness_snapshot_2422.get(
                    "broker_action"
                ),
                "2421_broker_action": readiness_recheck_after_remediation_2421.get(
                    "broker_action"
                ),
                "2420_broker_action": remediation_result_2420.get("broker_action"),
            },
            failure_message="Broker or order path is enabled.",
            classification="preflight_safety_boundary",
        ),
        _requirement(
            requirement_id="manual_review_only",
            description="Manual review boundary must remain present.",
            passed=(
                contract_readiness_snapshot_2422.get("manual_review_required")
                is True
                and readiness_recheck_after_remediation_2421.get(
                    "manual_review_required"
                )
                is True
                and remediation_result_2420.get("manual_review_required") is True
            ),
            evidence={
                "2422_manual_review_required": contract_readiness_snapshot_2422.get(
                    "manual_review_required"
                ),
                "2421_manual_review_required": (
                    readiness_recheck_after_remediation_2421.get(
                        "manual_review_required"
                    )
                ),
                "2420_manual_review_required": remediation_result_2420.get(
                    "manual_review_required"
                ),
            },
            failure_message="Manual review boundary is missing.",
            classification="preflight_safety_boundary",
        ),
        _requirement(
            requirement_id="preflight_started",
            description="This task records that paper-shadow preflight ran.",
            passed=True,
            evidence={"paper_shadow_preflight_started": True},
            failure_message="Paper-shadow preflight did not start.",
            classification="preflight_gap",
        ),
        _requirement(
            requirement_id="generated_signal_false",
            description="Preflight must not generate a new signal.",
            passed=_fields_false(
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=(
                    "generated_signal",
                    "new_signal_generated",
                    "new_feature_generated",
                ),
            ),
            evidence={
                "2422_new_signal_generated": contract_readiness_snapshot_2422.get(
                    "new_signal_generated"
                ),
                "2421_new_signal_generated": readiness_recheck_after_remediation_2421.get(
                    "new_signal_generated"
                ),
                "2420_new_signal_generated": remediation_result_2420.get(
                    "new_signal_generated"
                ),
            },
            failure_message="A signal or feature was generated before preflight.",
            classification="preflight_safety_boundary",
        ),
        _requirement(
            requirement_id="generated_trading_advice_false",
            description="Preflight must not generate trading advice.",
            passed=_fields_false(
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=(
                    "generated_trading_advice",
                    "trading_advice_generated",
                    "daily_report_generated",
                    "backtest_run",
                    "scoring_run",
                ),
            ),
            evidence={
                "2422_daily_report_generated": contract_readiness_snapshot_2422.get(
                    "daily_report_generated"
                ),
                "2422_backtest_run": contract_readiness_snapshot_2422.get(
                    "backtest_run"
                ),
                "2422_scoring_run": contract_readiness_snapshot_2422.get(
                    "scoring_run"
                ),
            },
            failure_message="Trading advice or derived scoring/report was generated.",
            classification="preflight_safety_boundary",
        ),
        _requirement(
            requirement_id="report_registry_registered",
            description="Report registry must include 2420, 2421, 2422, and 2423.",
            passed=not missing_reports,
            evidence={"missing_report_ids": missing_reports},
            failure_message="Report registry is missing required report ids.",
            classification="missing_preflight_evidence",
        ),
        _requirement(
            requirement_id="artifact_catalog_registered",
            description="Artifact catalog must include 2423 command and artifacts.",
            passed=not missing_catalog_refs,
            evidence={"missing_catalog_references": missing_catalog_refs},
            failure_message="Artifact catalog is missing required 2423 references.",
            classification="missing_preflight_evidence",
        ),
        _requirement(
            requirement_id="system_flow_registered",
            description="System flow must include 2423 route and READY status.",
            passed=not missing_flow_refs,
            evidence={"missing_system_flow_references": missing_flow_refs},
            failure_message="System flow is missing required 2423 references.",
            classification="missing_preflight_evidence",
        ),
        _requirement(
            requirement_id="research_docs_registered",
            description="Required 2420-2423 research docs must be readable.",
            passed=not missing_docs,
            evidence={"missing_research_docs": missing_docs},
            failure_message="One or more required research docs are missing.",
            classification="missing_preflight_evidence",
        ),
    ]


def _source_traceability_accepted(
    contract_readiness_snapshot_2422: Mapping[str, Any],
    readiness_recheck_after_remediation_2421: Mapping[str, Any],
    remediation_result_2420: Mapping[str, Any],
    manifest: Mapping[str, Any],
    lineage_map: Mapping[str, Any],
    missing_summary: Mapping[str, Any],
) -> bool:
    return (
        (
            contract_readiness_snapshot_2422.get("source_traceability_recheck_status")
            or readiness_recheck_after_remediation_2421.get(
                "source_traceability_recheck_status"
            )
        )
        == "ACCEPTED"
        and remediation_result_2420.get("status")
        == SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS
        and remediation_result_2420.get("remediation_status") == "READY"
        and remediation_result_2420.get("artifact_id") == ARTIFACT_ID
        and remediation_result_2420.get("source_traceability_evidence_complete")
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


def _preflight_checklist(
    *,
    status: str,
    requirements: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": PREFLIGHT_CHECKLIST_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "check_count": len(requirements),
        "passed_check_count": sum(
            1 for requirement in requirements if requirement["status"] == "PASS"
        ),
        "failed_check_count": sum(
            1 for requirement in requirements if requirement["status"] == "FAIL"
        ),
        "checks": requirements,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _preflight_gap_summary(
    *,
    status: str,
    gaps: list[dict[str, Any]],
    missing_preflight_evidence_count: int,
    safety_boundary_gap_count: int,
) -> dict[str, Any]:
    return {
        "schema_version": PREFLIGHT_GAP_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "preflight_gap_count": len(gaps),
        "missing_preflight_evidence_count": missing_preflight_evidence_count,
        "safety_boundary_gap_count": safety_boundary_gap_count,
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
