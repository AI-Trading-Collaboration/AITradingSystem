from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_paper_shadow_enablement_plan.v1"
ENABLEMENT_PLAN_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_enablement_plan_section.v1"
)
RUNTIME_BOUNDARY_CHECKLIST_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_runtime_boundary_checklist.v1"
)
SCHEDULE_BOUNDARY_PLAN_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_schedule_boundary_plan.v1"
)
MANUAL_REVIEW_CHECKLIST_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_manual_review_checklist.v1"
)
ROLLBACK_STOP_CONDITION_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_rollback_stop_condition_summary.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_BLOCKED_BY_PREFLIGHT_OR_"
    "CONTRACT_GAPS"
)
NEXT_ROUTE_READY = "TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Enablement_Gap_Remediation"
)
PRIOR_ROUTE = "TRADING-2423_Growth_Tilt_Engine_Paper_Shadow_Preflight"
EXPECTED_PRIOR_NEXT_ROUTE = (
    "TRADING-2424_Growth_Tilt_Engine_Paper_Shadow_Enablement_Plan"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
ARTIFACT_ID = "growth_tilt_engine_signal_artifact"
REPORT_TYPE = "growth_tilt_engine_paper_shadow_enablement_plan"

PREFLIGHT_READY_STATUS = "GROWTH_TILT_ENGINE_PAPER_SHADOW_PREFLIGHT_READY"
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
    "growth_tilt_engine_paper_shadow_preflight",
    "growth_tilt_engine_contract_readiness_snapshot",
    "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
    "remediation",
    "growth_tilt_engine_signal_artifact_source_traceability_remediation",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-paper-shadow-enablement-plan",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_enablement_plan/"
    "enablement_plan_result.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_enablement_plan/"
    "paper_shadow_enablement_plan.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_enablement_plan/"
    "runtime_boundary_checklist.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_enablement_plan/"
    "schedule_boundary_plan.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_enablement_plan/"
    "manual_review_checklist.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_enablement_plan/"
    "rollback_stop_condition_summary.json",
    "docs/research/growth_tilt_engine_paper_shadow_enablement_plan.md",
    "docs/research/growth_tilt_engine_paper_shadow_runtime_boundary_checklist.md",
    "docs/research/growth_tilt_engine_paper_shadow_schedule_boundary_plan.md",
    "docs/research/growth_tilt_engine_paper_shadow_manual_review_checklist.md",
    "docs/research/growth_tilt_engine_paper_shadow_rollback_stop_condition_summary.md",
    "docs/research/dynamic_strategy_2425_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-paper-shadow-enablement-plan",
    READY_STATUS,
    NEXT_ROUTE_READY,
)

ALLOWED_FUTURE_ACTIONS: tuple[str, ...] = (
    "paper_shadow_dry_run_wiring",
    "paper_shadow_schedule_dry_run",
    "paper_shadow_artifact_contract_validation",
    "paper_shadow_manual_review_packet_generation",
)
FORBIDDEN_ACTIONS: tuple[str, ...] = (
    "production_signal_generation",
    "trading_advice_generation",
    "broker_order_generation",
    "portfolio_weight_mutation_without_manual_review",
    "automatic_execution",
)


def build_growth_tilt_engine_paper_shadow_enablement_plan(
    paper_shadow_preflight_2423: Mapping[str, Any],
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
    requirements = _enablement_requirements(
        paper_shadow_preflight_2423,
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
    enablement_ready = not gaps
    status = READY_STATUS if enablement_ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if enablement_ready else NEXT_ROUTE_BLOCKED
    missing_evidence_count = sum(
        1 for gap in gaps if gap["classification"] == "missing_enablement_evidence"
    )
    safety_boundary_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "enablement_safety_boundary"
    )
    preflight_or_contract_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "preflight_or_contract_gap"
    )
    pit_blockers = (
        _as_list(paper_shadow_preflight_2423.get("remaining_pit_blockers"))
        or _as_list(contract_readiness_snapshot_2422.get("remaining_blockers"))
        or _as_list(readiness_recheck_after_remediation_2421.get("remaining_blockers"))
    )
    enablement_plan = _paper_shadow_enablement_plan(
        status=status,
        enablement_ready=enablement_ready,
        next_route=next_route,
        paper_shadow_preflight_2423=paper_shadow_preflight_2423,
        contract_readiness_snapshot_2422=contract_readiness_snapshot_2422,
        readiness_recheck_after_remediation_2421=(
            readiness_recheck_after_remediation_2421
        ),
        gaps=gaps,
    )
    runtime_boundary_checklist = _runtime_boundary_checklist(
        status=status,
        requirements=requirements,
    )
    schedule_boundary_plan = _schedule_boundary_plan(
        status=status,
        enablement_ready=enablement_ready,
        next_route=next_route,
    )
    manual_review_checklist = _manual_review_checklist(
        status=status,
        enablement_ready=enablement_ready,
        requirements=requirements,
    )
    rollback_stop_condition_summary = _rollback_stop_condition_summary(
        status=status,
        enablement_ready=enablement_ready,
        gaps=gaps,
        next_route=next_route,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2424",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": ["TRADING-2420", "TRADING-2421", "TRADING-2422", "TRADING-2423"],
        "artifact_id": ARTIFACT_ID,
        "pit_gate_ready": (
            paper_shadow_preflight_2423.get("pit_gate_ready") is True
            and contract_readiness_snapshot_2422.get("pit_gate_ready") is True
            and readiness_recheck_after_remediation_2421.get("pit_gate_ready") is True
        ),
        "pit_gate_ready_count": _int_value(
            paper_shadow_preflight_2423.get("pit_gate_ready_count")
            if paper_shadow_preflight_2423.get("pit_gate_ready_count") is not None
            else contract_readiness_snapshot_2422.get("pit_gate_ready_count")
        ),
        "contract_readiness_status": contract_readiness_snapshot_2422.get("status"),
        "contract_ready": (
            paper_shadow_preflight_2423.get("contract_ready") is True
            and contract_readiness_snapshot_2422.get("contract_ready") is True
        ),
        "contract_ready_count": _int_value(
            paper_shadow_preflight_2423.get("contract_ready_count")
            if paper_shadow_preflight_2423.get("contract_ready_count") is not None
            else contract_readiness_snapshot_2422.get("contract_ready_count")
        ),
        "contract_gap_count": _int_value(
            contract_readiness_snapshot_2422.get("contract_gap_count")
        ),
        "remaining_pit_blockers": pit_blockers,
        "remaining_pit_blocker_count": len(pit_blockers),
        "source_traceability_remediation_status": remediation_result_2420.get(
            "remediation_status"
        ),
        "source_traceability_recheck_status": (
            paper_shadow_preflight_2423.get("source_traceability_recheck_status")
            or contract_readiness_snapshot_2422.get(
                "source_traceability_recheck_status"
            )
            or readiness_recheck_after_remediation_2421.get(
                "source_traceability_recheck_status"
            )
        ),
        "source_traceability_accepted": _source_traceability_accepted(
            paper_shadow_preflight_2423,
            contract_readiness_snapshot_2422,
            readiness_recheck_after_remediation_2421,
            remediation_result_2420,
            manifest,
            lineage_map,
            missing_summary,
        ),
        "paper_shadow_preflight_ready": (
            paper_shadow_preflight_2423.get("status") == PREFLIGHT_READY_STATUS
            and paper_shadow_preflight_2423.get("paper_shadow_preflight_ready") is True
        ),
        "paper_shadow_enablement_plan_started": True,
        "paper_shadow_enablement_plan_completed": True,
        "enablement_plan_ready": enablement_ready,
        "enablement_gap_count": len(gaps),
        "enablement_gap_ids": [gap["requirement_id"] for gap in gaps],
        "missing_enablement_evidence_count": missing_evidence_count,
        "safety_boundary_gap_count": safety_boundary_gap_count,
        "preflight_or_contract_gap_count": preflight_or_contract_gap_count,
        "paper_shadow_enablement_plan": enablement_plan,
        "runtime_boundary_checklist": runtime_boundary_checklist,
        "schedule_boundary_plan": schedule_boundary_plan,
        "manual_review_checklist": manual_review_checklist,
        "rollback_stop_condition_summary": rollback_stop_condition_summary,
        "enablement_requirement_count": len(requirements),
        "enablement_requirement_pass_count": len(requirements) - len(gaps),
        "enablement_requirement_fail_count": len(gaps),
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "generated_signal": False,
        "generated_trading_advice": False,
        "new_signal_generated": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "automatic_execution_allowed": False,
        "dry_run_wiring_allowed": enablement_ready,
        "paper_shadow_schedule_dry_run_allowed": enablement_ready,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "broker_action_enabled": False,
        "new_feature_generated": False,
        "actual_portfolio_weights_modified": False,
        "production_effect": "none",
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "Enablement plan is ready; TRADING-2425 may design paper-shadow "
            "dry-run wiring while runtime, schedule, production, and broker "
            "paths remain disabled."
            if enablement_ready
            else "Enablement preconditions or safety boundaries failed; "
            "TRADING-2425 must remediate enablement gaps before dry-run wiring."
        ),
    }


def _enablement_requirements(
    paper_shadow_preflight_2423: Mapping[str, Any],
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
            requirement_id="paper_shadow_preflight_ready",
            description="TRADING-2423 paper-shadow preflight must be READY.",
            passed=(
                paper_shadow_preflight_2423.get("status") == PREFLIGHT_READY_STATUS
                and paper_shadow_preflight_2423.get("paper_shadow_preflight_ready")
                is True
                and _int_value(paper_shadow_preflight_2423.get("preflight_gap_count"))
                == 0
                and paper_shadow_preflight_2423.get(
                    "recommended_next_research_task"
                )
                == EXPECTED_PRIOR_NEXT_ROUTE
            ),
            evidence={
                "status": paper_shadow_preflight_2423.get("status"),
                "paper_shadow_preflight_ready": paper_shadow_preflight_2423.get(
                    "paper_shadow_preflight_ready"
                ),
                "preflight_gap_count": paper_shadow_preflight_2423.get(
                    "preflight_gap_count"
                ),
                "next_route": paper_shadow_preflight_2423.get(
                    "recommended_next_research_task"
                ),
            },
            failure_message="TRADING-2423 paper-shadow preflight is not READY.",
            classification="preflight_or_contract_gap",
        ),
        _requirement(
            requirement_id="contract_ready",
            description="TRADING-2422 contract readiness snapshot must be READY.",
            passed=(
                contract_readiness_snapshot_2422.get("status")
                == CONTRACT_READINESS_SNAPSHOT_READY_STATUS
                and contract_readiness_snapshot_2422.get("contract_ready") is True
                and _int_value(
                    contract_readiness_snapshot_2422.get("contract_ready_count")
                )
                == 1
                and _int_value(
                    contract_readiness_snapshot_2422.get("contract_gap_count")
                )
                == 0
            ),
            evidence={
                "status": contract_readiness_snapshot_2422.get("status"),
                "contract_ready": contract_readiness_snapshot_2422.get(
                    "contract_ready"
                ),
                "contract_ready_count": contract_readiness_snapshot_2422.get(
                    "contract_ready_count"
                ),
                "contract_gap_count": contract_readiness_snapshot_2422.get(
                    "contract_gap_count"
                ),
            },
            failure_message="Contract readiness is not complete for enablement plan.",
            classification="preflight_or_contract_gap",
        ),
        _requirement(
            requirement_id="pit_gate_ready",
            description="PIT gate must remain ready.",
            passed=(
                paper_shadow_preflight_2423.get("pit_gate_ready") is True
                and contract_readiness_snapshot_2422.get("pit_gate_ready") is True
                and readiness_recheck_after_remediation_2421.get("pit_gate_ready")
                is True
                and readiness_recheck_after_remediation_2421.get("status")
                == PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS
            ),
            evidence={
                "2423_pit_gate_ready": paper_shadow_preflight_2423.get(
                    "pit_gate_ready"
                ),
                "2422_pit_gate_ready": contract_readiness_snapshot_2422.get(
                    "pit_gate_ready"
                ),
                "2421_pit_gate_ready": readiness_recheck_after_remediation_2421.get(
                    "pit_gate_ready"
                ),
                "2421_status": readiness_recheck_after_remediation_2421.get("status"),
            },
            failure_message="PIT gate is not ready for enablement planning.",
            classification="preflight_or_contract_gap",
        ),
        _requirement(
            requirement_id="remaining_pit_blockers_empty",
            description="Remaining PIT blockers must be empty.",
            passed=(
                _as_list(paper_shadow_preflight_2423.get("remaining_pit_blockers"))
                == []
                and _as_list(contract_readiness_snapshot_2422.get("remaining_blockers"))
                == []
                and _as_list(
                    readiness_recheck_after_remediation_2421.get(
                        "remaining_blockers"
                    )
                )
                == []
            ),
            evidence={
                "2423_remaining_pit_blockers": paper_shadow_preflight_2423.get(
                    "remaining_pit_blockers"
                ),
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
            classification="preflight_or_contract_gap",
        ),
        _requirement(
            requirement_id="source_traceability_accepted",
            description="Source traceability must remain accepted.",
            passed=_source_traceability_accepted(
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                manifest,
                lineage_map,
                missing_summary,
            ),
            evidence={
                "2423_source_traceability_accepted": paper_shadow_preflight_2423.get(
                    "source_traceability_accepted"
                ),
                "2423_source_traceability_recheck_status": (
                    paper_shadow_preflight_2423.get(
                        "source_traceability_recheck_status"
                    )
                ),
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
                "lineage_dependency_count": len(
                    _as_list(lineage_map.get("upstream_dependencies"))
                ),
                "missing_field_count": missing_summary.get("missing_field_count"),
                "incomplete_field_count": missing_summary.get(
                    "incomplete_field_count"
                ),
                "unresolved_blocker_count": missing_summary.get(
                    "unresolved_blocker_count"
                ),
            },
            failure_message="Source traceability is not accepted.",
            classification="preflight_or_contract_gap",
        ),
        _requirement(
            requirement_id="paper_shadow_runtime_disabled",
            description="Paper-shadow runtime must remain disabled.",
            passed=_fields_false(
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=(
                    "paper_shadow_enabled",
                    "paper_shadow_allowed",
                    "paper_trade_created",
                    "shadow_position_created",
                ),
            ),
            evidence={
                "2423_paper_shadow_enabled": paper_shadow_preflight_2423.get(
                    "paper_shadow_enabled"
                ),
                "2422_paper_shadow_enabled": contract_readiness_snapshot_2422.get(
                    "paper_shadow_enabled"
                ),
            },
            failure_message="Paper-shadow runtime is already enabled or mutated.",
            classification="enablement_safety_boundary",
        ),
        _requirement(
            requirement_id="paper_shadow_schedule_disabled",
            description="Paper-shadow schedule must remain disabled.",
            passed=_fields_false(
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=(
                    "paper_shadow_schedule_enabled",
                    "paper_shadow_daily_job_enabled",
                    "paper_shadow_daily_job_run",
                    "scheduler_enabled",
                    "scheduled_task_created",
                ),
            ),
            evidence={
                "2423_paper_shadow_schedule_enabled": paper_shadow_preflight_2423.get(
                    "paper_shadow_schedule_enabled"
                ),
                "2423_scheduler_enabled": paper_shadow_preflight_2423.get(
                    "scheduler_enabled"
                ),
            },
            failure_message="Paper-shadow schedule is already enabled or executed.",
            classification="enablement_safety_boundary",
        ),
        _requirement(
            requirement_id="production_disabled",
            description="Production path must remain disabled.",
            passed=_fields_false(
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=("production_enabled", "production_allowed"),
            ),
            evidence={
                "2423_production_enabled": paper_shadow_preflight_2423.get(
                    "production_enabled"
                ),
                "2422_production_enabled": contract_readiness_snapshot_2422.get(
                    "production_enabled"
                ),
            },
            failure_message="Production path is enabled.",
            classification="enablement_safety_boundary",
        ),
        _requirement(
            requirement_id="broker_disabled",
            description="Broker and order paths must remain disabled.",
            passed=_broker_disabled(
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
            ),
            evidence={
                "2423_broker_enabled": paper_shadow_preflight_2423.get(
                    "broker_enabled"
                ),
                "2423_broker_action": paper_shadow_preflight_2423.get(
                    "broker_action"
                ),
                "2422_broker_enabled": contract_readiness_snapshot_2422.get(
                    "broker_enabled"
                ),
                "2422_broker_action": contract_readiness_snapshot_2422.get(
                    "broker_action"
                ),
            },
            failure_message="Broker or order path is enabled.",
            classification="enablement_safety_boundary",
        ),
        _requirement(
            requirement_id="manual_review_only",
            description="Manual review boundary must remain present.",
            passed=(
                paper_shadow_preflight_2423.get("manual_review_required") is True
                and paper_shadow_preflight_2423.get("manual_review_only") is True
                and contract_readiness_snapshot_2422.get("manual_review_required")
                is True
                and readiness_recheck_after_remediation_2421.get(
                    "manual_review_required"
                )
                is True
                and remediation_result_2420.get("manual_review_required") is True
            ),
            evidence={
                "2423_manual_review_required": paper_shadow_preflight_2423.get(
                    "manual_review_required"
                ),
                "2423_manual_review_only": paper_shadow_preflight_2423.get(
                    "manual_review_only"
                ),
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
            classification="enablement_safety_boundary",
        ),
        _requirement(
            requirement_id="no_signal_generation",
            description="Enablement plan must not generate a new signal.",
            passed=_fields_false(
                paper_shadow_preflight_2423,
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
                "2423_generated_signal": paper_shadow_preflight_2423.get(
                    "generated_signal"
                ),
                "2423_new_signal_generated": paper_shadow_preflight_2423.get(
                    "new_signal_generated"
                ),
            },
            failure_message="A new signal or feature was generated.",
            classification="enablement_safety_boundary",
        ),
        _requirement(
            requirement_id="no_trading_advice_or_runtime_job",
            description="Enablement plan must not generate advice or runtime jobs.",
            passed=_fields_false(
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=(
                    "generated_trading_advice",
                    "trading_advice_generated",
                    "daily_report_generated",
                    "daily_report_run",
                    "backtest_run",
                    "scoring_run",
                    "fresh_market_data_read",
                ),
            ),
            evidence={
                "2423_generated_trading_advice": paper_shadow_preflight_2423.get(
                    "generated_trading_advice"
                ),
                "2423_backtest_run": paper_shadow_preflight_2423.get("backtest_run"),
                "2423_scoring_run": paper_shadow_preflight_2423.get("scoring_run"),
            },
            failure_message="Trading advice, backtest, scoring, or runtime job ran.",
            classification="enablement_safety_boundary",
        ),
        _requirement(
            requirement_id="report_registry_registered",
            description="Report registry must include 2420 through 2424 reports.",
            passed=not missing_reports,
            evidence={"missing_report_ids": missing_reports},
            failure_message="Report registry is missing required report ids.",
            classification="missing_enablement_evidence",
        ),
        _requirement(
            requirement_id="artifact_catalog_registered",
            description="Artifact catalog must include 2424 command and artifacts.",
            passed=not missing_catalog_refs,
            evidence={"missing_catalog_references": missing_catalog_refs},
            failure_message="Artifact catalog is missing required 2424 references.",
            classification="missing_enablement_evidence",
        ),
        _requirement(
            requirement_id="system_flow_registered",
            description="System flow must include 2424 route and READY status.",
            passed=not missing_flow_refs,
            evidence={"missing_system_flow_references": missing_flow_refs},
            failure_message="System flow is missing required 2424 references.",
            classification="missing_enablement_evidence",
        ),
        _requirement(
            requirement_id="research_docs_registered",
            description="Required 2420-2424 research docs must be readable.",
            passed=not missing_docs,
            evidence={"missing_research_docs": missing_docs},
            failure_message="One or more required research docs are missing.",
            classification="missing_enablement_evidence",
        ),
    ]


def _source_traceability_accepted(
    paper_shadow_preflight_2423: Mapping[str, Any],
    contract_readiness_snapshot_2422: Mapping[str, Any],
    readiness_recheck_after_remediation_2421: Mapping[str, Any],
    remediation_result_2420: Mapping[str, Any],
    manifest: Mapping[str, Any],
    lineage_map: Mapping[str, Any],
    missing_summary: Mapping[str, Any],
) -> bool:
    return (
        paper_shadow_preflight_2423.get("source_traceability_accepted") is True
        and (
            paper_shadow_preflight_2423.get("source_traceability_recheck_status")
            or contract_readiness_snapshot_2422.get(
                "source_traceability_recheck_status"
            )
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
        and _int_value(missing_summary.get("missing_field_count")) == 0
        and _int_value(missing_summary.get("incomplete_field_count")) == 0
        and _int_value(missing_summary.get("unresolved_blocker_count")) == 0
        and missing_summary.get("prior_missing_evidence_closed_by_2420") is True
    )


def _paper_shadow_enablement_plan(
    *,
    status: str,
    enablement_ready: bool,
    next_route: str,
    paper_shadow_preflight_2423: Mapping[str, Any],
    contract_readiness_snapshot_2422: Mapping[str, Any],
    readiness_recheck_after_remediation_2421: Mapping[str, Any],
    gaps: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": ENABLEMENT_PLAN_SCHEMA_VERSION,
        "status": status,
        "enablement_plan_ready": enablement_ready,
        "enablement_gap_count": len(gaps),
        "preconditions": {
            "pit_gate_ready": (
                paper_shadow_preflight_2423.get("pit_gate_ready") is True
                and contract_readiness_snapshot_2422.get("pit_gate_ready") is True
                and readiness_recheck_after_remediation_2421.get("pit_gate_ready")
                is True
            ),
            "contract_ready": (
                paper_shadow_preflight_2423.get("contract_ready") is True
                and contract_readiness_snapshot_2422.get("contract_ready") is True
            ),
            "paper_shadow_preflight_ready": (
                paper_shadow_preflight_2423.get("paper_shadow_preflight_ready")
                is True
            ),
            "contract_gap_count": _int_value(
                contract_readiness_snapshot_2422.get("contract_gap_count")
            ),
            "remaining_pit_blockers": (
                _as_list(paper_shadow_preflight_2423.get("remaining_pit_blockers"))
                or _as_list(contract_readiness_snapshot_2422.get("remaining_blockers"))
                or _as_list(
                    readiness_recheck_after_remediation_2421.get(
                        "remaining_blockers"
                    )
                )
            ),
        },
        "runtime_boundaries": {
            "paper_shadow_enabled": False,
            "paper_shadow_schedule_enabled": False,
            "production_enabled": False,
            "broker_enabled": False,
            "manual_review_only": True,
            "automatic_execution_allowed": False,
        },
        "allowed_future_actions": list(ALLOWED_FUTURE_ACTIONS),
        "forbidden_actions": list(FORBIDDEN_ACTIONS),
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _runtime_boundary_checklist(
    *,
    status: str,
    requirements: list[dict[str, Any]],
) -> dict[str, Any]:
    boundary_ids = {
        "paper_shadow_runtime_disabled",
        "paper_shadow_schedule_disabled",
        "production_disabled",
        "broker_disabled",
        "manual_review_only",
        "no_signal_generation",
        "no_trading_advice_or_runtime_job",
    }
    checks = [
        requirement
        for requirement in requirements
        if requirement["requirement_id"] in boundary_ids
    ]
    return {
        "schema_version": RUNTIME_BOUNDARY_CHECKLIST_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "check_count": len(checks),
        "passed_check_count": sum(
            1 for requirement in checks if requirement["status"] == "PASS"
        ),
        "failed_check_count": sum(
            1 for requirement in checks if requirement["status"] == "FAIL"
        ),
        "checks": checks,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _schedule_boundary_plan(
    *,
    status: str,
    enablement_ready: bool,
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEDULE_BOUNDARY_PLAN_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "paper_shadow_schedule_enabled": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "schedule_creation_allowed": False,
        "dry_run_schedule_planning_allowed": enablement_ready,
        "allowed_future_schedule_actions": (
            ["paper_shadow_schedule_dry_run", "schedule_artifact_contract_review"]
            if enablement_ready
            else []
        ),
        "blocked_runtime_actions": [
            "create_cron_or_windows_task",
            "append_historical_event_log",
            "run_paper_shadow_daily_job",
            "bind_outcome_store",
        ],
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _manual_review_checklist(
    *,
    status: str,
    enablement_ready: bool,
    requirements: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": MANUAL_REVIEW_CHECKLIST_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "manual_review_required": True,
        "manual_review_only": True,
        "automatic_execution_allowed": False,
        "owner_approval_required_before_runtime": True,
        "ready_for_manual_review_packet": enablement_ready,
        "required_review_items": [
            "paper_shadow_enablement_plan",
            "runtime_boundary_checklist",
            "schedule_boundary_plan",
            "rollback_stop_condition_summary",
            "source_artifact_chain_2420_to_2423",
        ],
        "failed_requirement_ids": [
            requirement["requirement_id"]
            for requirement in requirements
            if requirement["status"] == "FAIL"
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _rollback_stop_condition_summary(
    *,
    status: str,
    enablement_ready: bool,
    gaps: list[dict[str, Any]],
    next_route: str,
) -> dict[str, Any]:
    return {
        "schema_version": ROLLBACK_STOP_CONDITION_SUMMARY_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "enablement_plan_ready": enablement_ready,
        "enablement_gap_count": len(gaps),
        "gaps": gaps,
        "rollback_required_now": False,
        "stop_conditions": [
            "any paper_shadow_enabled flag becomes true before owner approval",
            "paper_shadow_schedule_enabled becomes true before dry-run wiring",
            "production_enabled or broker_enabled becomes true",
            "generated_signal or generated_trading_advice becomes true",
            "source artifact chain no longer matches TRADING-2420 through TRADING-2423",
        ],
        "rollback_actions": [
            "keep paper-shadow runtime disabled",
            "keep scheduler disabled",
            "keep production and broker paths disabled",
            "route to enablement gap remediation when gaps exist",
        ],
        "next_route": next_route,
        "production_effect": "none",
        "broker_action": "none",
    }


def _fields_false(
    *documents: Mapping[str, Any],
    fields: tuple[str, ...],
) -> bool:
    for document in documents:
        for field in fields:
            if field in document and document.get(field) is not False:
                return False
    return True


def _broker_disabled(*documents: Mapping[str, Any]) -> bool:
    return _fields_false(
        *documents,
        fields=("broker_enabled", "broker_action_enabled", "order_generated"),
    ) and all(document.get("broker_action", "none") == "none" for document in documents)


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
