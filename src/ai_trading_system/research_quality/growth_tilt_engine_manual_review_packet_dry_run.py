from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_manual_review_packet_dry_run.v1"
MANUAL_REVIEW_PACKET_SCHEMA_VERSION = "growth_tilt_engine_manual_review_packet.v1"
MANUAL_REVIEW_CHECKLIST_SCHEMA_VERSION = (
    "growth_tilt_engine_manual_review_packet_checklist.v1"
)
NO_ADVICE_BOUNDARY_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_manual_review_packet_no_advice_boundary_summary.v1"
)
REVIEWER_HANDOFF_MANIFEST_SCHEMA_VERSION = (
    "growth_tilt_engine_manual_review_packet_reviewer_handoff_manifest.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_MANUAL_REVIEW_PACKET_DRY_RUN_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_MANUAL_REVIEW_PACKET_DRY_RUN_BLOCKED_BY_PACKET_GAPS"
)
NEXT_ROUTE_READY = (
    "TRADING-2428_Growth_Tilt_Engine_Observe_Only_Signal_Artifact_Boundary"
)
NEXT_ROUTE_BLOCKED = (
    "TRADING-2428_Growth_Tilt_Engine_Manual_Review_Packet_Dry_Run_Gap_Remediation"
)
PRIOR_ROUTE = "TRADING-2426_Growth_Tilt_Engine_Paper_Shadow_Schedule_Dry_Run"
EXPECTED_PRIOR_NEXT_ROUTE = (
    "TRADING-2427_Growth_Tilt_Engine_Manual_Review_Packet_Dry_Run"
)
PAPER_SHADOW_SCHEDULE_DRY_RUN_READY_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_SCHEDULE_DRY_RUN_READY"
)
PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING_READY"
)
PAPER_SHADOW_ENABLEMENT_PLAN_READY_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_READY"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
REPORT_TYPE = "growth_tilt_engine_manual_review_packet_dry_run"

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_paper_shadow_schedule_dry_run",
    "growth_tilt_engine_paper_shadow_dry_run_wiring",
    "growth_tilt_engine_paper_shadow_enablement_plan",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-manual-review-packet-dry-run",
    "outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/"
    "manual_review_packet_dry_run_result.json",
    "outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/"
    "manual_review_packet.json",
    "outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/"
    "manual_review_checklist.json",
    "outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/"
    "no_advice_boundary_summary.json",
    "outputs/research_strategies/growth_tilt_engine_manual_review_packet_dry_run/"
    "reviewer_handoff_manifest.json",
    "docs/research/growth_tilt_engine_manual_review_packet_dry_run.md",
    "docs/research/growth_tilt_engine_manual_review_packet.md",
    "docs/research/growth_tilt_engine_manual_review_packet_checklist.md",
    "docs/research/growth_tilt_engine_manual_review_packet_no_advice_boundary_summary.md",
    "docs/research/growth_tilt_engine_manual_review_packet_reviewer_handoff_manifest.md",
    "docs/research/dynamic_strategy_2428_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-manual-review-packet-dry-run",
    READY_STATUS,
    NEXT_ROUTE_READY,
)


def build_growth_tilt_engine_manual_review_packet_dry_run(
    schedule_dry_run_result_2426: Mapping[str, Any],
    schedule_boundary_checklist_2426: Mapping[str, Any],
    schedule_no_effect_audit_summary_2426: Mapping[str, Any],
    dry_run_wiring_result_2425: Mapping[str, Any],
    manual_review_handoff_wiring_plan_2425: Mapping[str, Any],
    paper_shadow_enablement_plan_2424: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    manual_review_packet: Mapping[str, Any] | None = None,
    manual_review_checklist: Sequence[Mapping[str, Any]] | None = None,
    no_advice_boundary_summary: Mapping[str, Any] | None = None,
    reviewer_handoff_manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    schedule_checklist = _section(
        schedule_boundary_checklist_2426,
        "schedule_boundary_checklist",
    )
    schedule_no_effect = _section(
        schedule_no_effect_audit_summary_2426,
        "schedule_no_effect_audit_summary",
    )
    manual_handoff_2425 = _section(
        manual_review_handoff_wiring_plan_2425,
        "manual_review_handoff_wiring_plan",
    )
    resolved_packet = (
        _default_manual_review_packet()
        if manual_review_packet is None
        else dict(manual_review_packet)
    )
    resolved_checklist = (
        _default_manual_review_checklist()
        if manual_review_checklist is None
        else [dict(item) for item in manual_review_checklist]
    )
    resolved_no_advice = (
        _default_no_advice_boundary_summary()
        if no_advice_boundary_summary is None
        else dict(no_advice_boundary_summary)
    )
    resolved_handoff = (
        _default_reviewer_handoff_manifest()
        if reviewer_handoff_manifest is None
        else dict(reviewer_handoff_manifest)
    )

    requirements = _manual_review_packet_requirements(
        schedule_dry_run_result_2426,
        schedule_checklist,
        schedule_no_effect,
        dry_run_wiring_result_2425,
        manual_handoff_2425,
        paper_shadow_enablement_plan_2424,
        manual_review_packet=resolved_packet,
        manual_review_checklist=resolved_checklist,
        no_advice_boundary_summary=resolved_no_advice,
        reviewer_handoff_manifest=resolved_handoff,
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
    ready = not gaps
    status = READY_STATUS if ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if ready else NEXT_ROUTE_BLOCKED
    missing_evidence_count = sum(
        1 for gap in gaps if gap["classification"] == "missing_manual_review_evidence"
    )
    safety_boundary_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "manual_review_safety_boundary"
    )
    packet_contract_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "manual_review_contract_gap"
    )
    precondition_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "manual_review_precondition_gap"
    )

    packet_section = _manual_review_packet_section(
        status=status,
        packet=resolved_packet,
    )
    checklist_section = _manual_review_checklist_section(
        status=status,
        checklist=resolved_checklist,
    )
    no_advice_section = _no_advice_boundary_summary_section(
        status=status,
        summary=resolved_no_advice,
        gaps=gaps,
    )
    handoff_section = _reviewer_handoff_manifest_section(
        status=status,
        manifest=resolved_handoff,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2427",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": ["TRADING-2424", "TRADING-2425", "TRADING-2426"],
        "pit_gate_ready": schedule_dry_run_result_2426.get("pit_gate_ready") is True,
        "pit_gate_ready_count": _int_value(
            schedule_dry_run_result_2426.get("pit_gate_ready_count")
        ),
        "contract_ready": schedule_dry_run_result_2426.get("contract_ready") is True,
        "contract_ready_count": _int_value(
            schedule_dry_run_result_2426.get("contract_ready_count")
        ),
        "contract_gap_count": _int_value(
            schedule_dry_run_result_2426.get("contract_gap_count")
        ),
        "paper_shadow_schedule_dry_run_status": schedule_dry_run_result_2426.get(
            "status"
        ),
        "paper_shadow_schedule_dry_run_ready": (
            schedule_dry_run_result_2426.get("paper_shadow_schedule_dry_run_ready")
            is True
        ),
        "schedule_dry_run_gap_count": _int_value(
            schedule_dry_run_result_2426.get("schedule_dry_run_gap_count")
        ),
        "paper_shadow_dry_run_wiring_status": dry_run_wiring_result_2425.get(
            "status"
        ),
        "paper_shadow_dry_run_wiring_ready": (
            dry_run_wiring_result_2425.get("dry_run_wiring_ready") is True
        ),
        "enablement_plan_status": paper_shadow_enablement_plan_2424.get("status"),
        "enablement_plan_ready": (
            paper_shadow_enablement_plan_2424.get("enablement_plan_ready") is True
        ),
        "manual_review_packet_dry_run_started": True,
        "manual_review_packet_dry_run_completed": True,
        "manual_review_packet_dry_run_ready": ready,
        "manual_review_packet_ready": _manual_review_packet_ready(resolved_packet),
        "manual_review_checklist_ready": _manual_review_checklist_ready(
            resolved_checklist
        ),
        "no_advice_boundary_ready": _no_advice_boundary_ready(resolved_no_advice),
        "reviewer_handoff_manifest_ready": _reviewer_handoff_ready(
            resolved_handoff
        ),
        "manual_review_packet_gap_count": len(gaps),
        "manual_review_packet_gap_ids": [gap["requirement_id"] for gap in gaps],
        "missing_manual_review_evidence_count": missing_evidence_count,
        "safety_boundary_gap_count": safety_boundary_gap_count,
        "packet_contract_gap_count": packet_contract_gap_count,
        "precondition_gap_count": precondition_gap_count,
        "manual_review_packet_requirement_count": len(requirements),
        "manual_review_packet_requirement_pass_count": len(requirements) - len(gaps),
        "manual_review_packet_requirement_fail_count": len(gaps),
        "manual_review_packet": packet_section,
        "manual_review_checklist": checklist_section,
        "no_advice_boundary_summary": no_advice_section,
        "reviewer_handoff_manifest": handoff_section,
        "manual_review_required": True,
        "manual_review_only": True,
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "schedule_hook_invoked": False,
        "schedule_state_mutated": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "broker_action": "none",
        "broker_order_generated": False,
        "order_generated": False,
        "portfolio_weight_mutated": False,
        "actual_portfolio_weights_modified": False,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "automatic_execution_allowed": False,
        "candidate_search_allowed": False,
        "candidate_search_resumed": False,
        "research_only_observation_allowed": False,
        "research_only_observation_approved": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "new_feature_generated": False,
        "production_effect": "none",
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "Manual review packet dry-run evidence is complete; TRADING-2428 "
            "may define observe-only signal artifact boundaries while advice, "
            "allocation, production, and broker paths remain disabled."
            if ready
            else "Manual review packet dry-run found gaps; remediate packet "
            "or no-advice evidence before observe-only signal boundary work."
        ),
    }


def _manual_review_packet_requirements(
    schedule_dry_run_result_2426: Mapping[str, Any],
    schedule_checklist: Mapping[str, Any],
    schedule_no_effect: Mapping[str, Any],
    dry_run_wiring_result_2425: Mapping[str, Any],
    manual_handoff_2425: Mapping[str, Any],
    paper_shadow_enablement_plan_2424: Mapping[str, Any],
    *,
    manual_review_packet: Mapping[str, Any],
    manual_review_checklist: Sequence[Mapping[str, Any]],
    no_advice_boundary_summary: Mapping[str, Any],
    reviewer_handoff_manifest: Mapping[str, Any],
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
            requirement_id="prior_schedule_dry_run_ready",
            description="TRADING-2426 schedule dry-run must be READY.",
            passed=(
                schedule_dry_run_result_2426.get("status")
                == PAPER_SHADOW_SCHEDULE_DRY_RUN_READY_STATUS
                and schedule_dry_run_result_2426.get(
                    "paper_shadow_schedule_dry_run_ready"
                )
                is True
                and _int_value(
                    schedule_dry_run_result_2426.get("schedule_dry_run_gap_count")
                )
                == 0
                and schedule_dry_run_result_2426.get("recommended_next_research_task")
                == EXPECTED_PRIOR_NEXT_ROUTE
            ),
            evidence={
                "status": schedule_dry_run_result_2426.get("status"),
                "paper_shadow_schedule_dry_run_ready": (
                    schedule_dry_run_result_2426.get(
                        "paper_shadow_schedule_dry_run_ready"
                    )
                ),
                "schedule_dry_run_gap_count": schedule_dry_run_result_2426.get(
                    "schedule_dry_run_gap_count"
                ),
                "next_route": schedule_dry_run_result_2426.get(
                    "recommended_next_research_task"
                ),
            },
            failure_message="TRADING-2426 schedule dry-run is not READY.",
            classification="manual_review_precondition_gap",
        ),
        _requirement(
            requirement_id="schedule_boundary_checklist_ready",
            description="TRADING-2426 schedule boundary checklist must pass.",
            passed=(
                schedule_checklist.get("schedule_boundary_checklist_ready") is True
                and _int_value(schedule_checklist.get("failed_check_count")) == 0
            ),
            evidence={
                "schedule_boundary_checklist_ready": schedule_checklist.get(
                    "schedule_boundary_checklist_ready"
                ),
                "failed_check_count": schedule_checklist.get("failed_check_count"),
            },
            failure_message="Schedule boundary checklist is not ready.",
            classification="manual_review_precondition_gap",
        ),
        _requirement(
            requirement_id="schedule_no_effect_audit_ready",
            description="TRADING-2426 schedule no-effect audit must be ready.",
            passed=_schedule_no_effect_ready(schedule_no_effect),
            evidence=dict(schedule_no_effect),
            failure_message="Schedule no-effect audit is missing or shows effects.",
            classification="manual_review_precondition_gap",
        ),
        _requirement(
            requirement_id="prior_dry_run_wiring_ready",
            description="TRADING-2425 dry-run wiring must remain READY.",
            passed=(
                dry_run_wiring_result_2425.get("status")
                == PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS
                and dry_run_wiring_result_2425.get("dry_run_wiring_ready") is True
                and _int_value(
                    dry_run_wiring_result_2425.get("dry_run_wiring_gap_count")
                )
                == 0
            ),
            evidence={
                "status": dry_run_wiring_result_2425.get("status"),
                "dry_run_wiring_ready": dry_run_wiring_result_2425.get(
                    "dry_run_wiring_ready"
                ),
                "dry_run_wiring_gap_count": dry_run_wiring_result_2425.get(
                    "dry_run_wiring_gap_count"
                ),
            },
            failure_message="TRADING-2425 dry-run wiring is not READY.",
            classification="manual_review_precondition_gap",
        ),
        _requirement(
            requirement_id="enablement_plan_ready",
            description="TRADING-2424 enablement plan must remain READY.",
            passed=(
                paper_shadow_enablement_plan_2424.get("status")
                == PAPER_SHADOW_ENABLEMENT_PLAN_READY_STATUS
                and paper_shadow_enablement_plan_2424.get("enablement_plan_ready")
                is True
                and _int_value(
                    paper_shadow_enablement_plan_2424.get("enablement_gap_count")
                )
                == 0
            ),
            evidence={
                "status": paper_shadow_enablement_plan_2424.get("status"),
                "enablement_plan_ready": paper_shadow_enablement_plan_2424.get(
                    "enablement_plan_ready"
                ),
                "enablement_gap_count": paper_shadow_enablement_plan_2424.get(
                    "enablement_gap_count"
                ),
            },
            failure_message="TRADING-2424 enablement plan is not READY.",
            classification="manual_review_precondition_gap",
        ),
        _requirement(
            requirement_id="manual_review_handoff_wired",
            description="TRADING-2425 manual review handoff must remain wired.",
            passed=_manual_handoff_wired(manual_handoff_2425),
            evidence=dict(manual_handoff_2425),
            failure_message="Manual review handoff is missing or incomplete.",
            classification="manual_review_precondition_gap",
        ),
        _requirement(
            requirement_id="manual_review_packet_resolves",
            description="Manual review packet must resolve as dry-run no-advice packet.",
            passed=_manual_review_packet_ready(manual_review_packet),
            evidence=dict(manual_review_packet),
            failure_message="Manual review packet is incomplete or advice-bearing.",
            classification="manual_review_contract_gap",
        ),
        _requirement(
            requirement_id="manual_review_checklist_passes",
            description="Manual review checklist must pass.",
            passed=_manual_review_checklist_ready(manual_review_checklist),
            evidence={
                "check_count": len(manual_review_checklist),
                "failed_check_ids": [
                    item.get("check_id")
                    for item in manual_review_checklist
                    if item.get("passed") is not True
                ],
            },
            failure_message="Manual review checklist has failing checks.",
            classification="manual_review_contract_gap",
        ),
        _requirement(
            requirement_id="no_advice_boundary_ready",
            description="No-advice boundary summary must be ready.",
            passed=_no_advice_boundary_ready(no_advice_boundary_summary),
            evidence=dict(no_advice_boundary_summary),
            failure_message="No-advice boundary is missing or shows advice/allocation.",
            classification="manual_review_contract_gap",
        ),
        _requirement(
            requirement_id="reviewer_handoff_manifest_ready",
            description="Reviewer handoff manifest must be ready.",
            passed=_reviewer_handoff_ready(reviewer_handoff_manifest),
            evidence=dict(reviewer_handoff_manifest),
            failure_message="Reviewer handoff manifest is incomplete.",
            classification="manual_review_contract_gap",
        ),
        _requirement(
            requirement_id="no_trading_advice_or_allocation_generation",
            description="Manual review packet dry-run must not generate advice/allocation.",
            passed=_fields_false(
                schedule_dry_run_result_2426,
                dry_run_wiring_result_2425,
                no_advice_boundary_summary,
                manual_review_packet,
                fields=(
                    "contains_trading_advice",
                    "contains_actionable_allocation",
                    "generated_trading_advice",
                    "trading_advice_generated",
                    "actionable_allocation_generated",
                    "allocation_change_generated",
                    "recommendation_generated",
                ),
            ),
            evidence={
                "generated_trading_advice": schedule_dry_run_result_2426.get(
                    "generated_trading_advice"
                ),
                "trading_advice_generated": no_advice_boundary_summary.get(
                    "trading_advice_generated"
                ),
                "actionable_allocation_generated": no_advice_boundary_summary.get(
                    "actionable_allocation_generated"
                ),
                "contains_trading_advice": manual_review_packet.get(
                    "contains_trading_advice"
                ),
                "contains_actionable_allocation": manual_review_packet.get(
                    "contains_actionable_allocation"
                ),
            },
            failure_message="Advice or actionable allocation was generated.",
            classification="manual_review_safety_boundary",
        ),
        _requirement(
            requirement_id="paper_shadow_runtime_disabled",
            description="Paper-shadow runtime must remain disabled.",
            passed=_fields_false(
                schedule_dry_run_result_2426,
                dry_run_wiring_result_2425,
                paper_shadow_enablement_plan_2424,
                fields=(
                    "paper_shadow_enabled",
                    "paper_shadow_allowed",
                    "paper_trade_created",
                    "shadow_position_created",
                ),
            ),
            evidence={
                "2426_paper_shadow_enabled": schedule_dry_run_result_2426.get(
                    "paper_shadow_enabled"
                ),
                "2425_paper_shadow_enabled": dry_run_wiring_result_2425.get(
                    "paper_shadow_enabled"
                ),
            },
            failure_message="Paper-shadow runtime is enabled.",
            classification="manual_review_safety_boundary",
        ),
        _requirement(
            requirement_id="paper_shadow_schedule_disabled",
            description="Paper-shadow schedule and scheduler must remain disabled.",
            passed=_fields_false(
                schedule_dry_run_result_2426,
                dry_run_wiring_result_2425,
                schedule_no_effect,
                fields=(
                    "paper_shadow_schedule_enabled",
                    "paper_shadow_daily_job_enabled",
                    "paper_shadow_daily_job_run",
                    "scheduler_enabled",
                    "scheduled_task_created",
                    "schedule_hook_invoked",
                    "schedule_state_mutated",
                ),
            ),
            evidence={
                "2426_paper_shadow_schedule_enabled": (
                    schedule_dry_run_result_2426.get("paper_shadow_schedule_enabled")
                ),
                "2426_scheduler_enabled": schedule_dry_run_result_2426.get(
                    "scheduler_enabled"
                ),
                "2426_no_effect_paper_shadow_daily_job_run": (
                    schedule_no_effect.get("paper_shadow_daily_job_run")
                ),
            },
            failure_message="Paper-shadow schedule or scheduler is enabled.",
            classification="manual_review_safety_boundary",
        ),
        _requirement(
            requirement_id="production_disabled",
            description="Production path must remain disabled.",
            passed=_fields_false(
                schedule_dry_run_result_2426,
                dry_run_wiring_result_2425,
                paper_shadow_enablement_plan_2424,
                fields=("production_enabled", "production_allowed"),
            ),
            evidence={
                "2426_production_enabled": schedule_dry_run_result_2426.get(
                    "production_enabled"
                ),
                "2425_production_enabled": dry_run_wiring_result_2425.get(
                    "production_enabled"
                ),
            },
            failure_message="Production path is enabled.",
            classification="manual_review_safety_boundary",
        ),
        _requirement(
            requirement_id="broker_disabled",
            description="Broker and order paths must remain disabled.",
            passed=_broker_disabled(
                schedule_dry_run_result_2426,
                dry_run_wiring_result_2425,
                paper_shadow_enablement_plan_2424,
                no_advice_boundary_summary,
            ),
            evidence={
                "2426_broker_enabled": schedule_dry_run_result_2426.get(
                    "broker_enabled"
                ),
                "2426_broker_action": schedule_dry_run_result_2426.get(
                    "broker_action"
                ),
                "2426_broker_order_generated": schedule_dry_run_result_2426.get(
                    "broker_order_generated"
                ),
            },
            failure_message="Broker or order path is enabled.",
            classification="manual_review_safety_boundary",
        ),
        _requirement(
            requirement_id="automatic_execution_disabled",
            description="Automatic execution must remain disabled.",
            passed=_fields_false(
                schedule_dry_run_result_2426,
                dry_run_wiring_result_2425,
                manual_handoff_2425,
                reviewer_handoff_manifest,
                fields=("automatic_execution_allowed",),
            ),
            evidence={
                "2426_automatic_execution_allowed": schedule_dry_run_result_2426.get(
                    "automatic_execution_allowed"
                ),
                "handoff_automatic_execution_allowed": reviewer_handoff_manifest.get(
                    "automatic_execution_allowed"
                ),
            },
            failure_message="Automatic execution is allowed.",
            classification="manual_review_safety_boundary",
        ),
        _requirement(
            requirement_id="no_signal_backtest_scoring_daily_report_or_fresh_data",
            description=(
                "Manual review packet dry-run must not generate signal, run "
                "backtest/scoring/daily report, or read fresh data."
            ),
            passed=_fields_false(
                schedule_dry_run_result_2426,
                dry_run_wiring_result_2425,
                no_advice_boundary_summary,
                fields=(
                    "generated_signal",
                    "new_signal_generated",
                    "new_feature_generated",
                    "backtest_run",
                    "new_strategy_backtest_run",
                    "scoring_run",
                    "daily_report_generated",
                    "daily_report_run",
                    "fresh_market_data_read",
                ),
            ),
            evidence={
                "generated_signal": schedule_dry_run_result_2426.get(
                    "generated_signal"
                ),
                "backtest_run": schedule_dry_run_result_2426.get("backtest_run"),
                "scoring_run": schedule_dry_run_result_2426.get("scoring_run"),
                "daily_report_run": schedule_dry_run_result_2426.get(
                    "daily_report_run"
                ),
                "fresh_market_data_read": schedule_dry_run_result_2426.get(
                    "fresh_market_data_read"
                ),
            },
            failure_message="Signal/backtest/scoring/daily report/fresh data path ran.",
            classification="manual_review_safety_boundary",
        ),
        _requirement(
            requirement_id="portfolio_weights_unchanged",
            description="Actual portfolio weights must not be mutated.",
            passed=_fields_false(
                schedule_dry_run_result_2426,
                dry_run_wiring_result_2425,
                no_advice_boundary_summary,
                fields=(
                    "portfolio_weight_mutated",
                    "actual_portfolio_weights_modified",
                ),
            ),
            evidence={
                "portfolio_weight_mutated": schedule_dry_run_result_2426.get(
                    "portfolio_weight_mutated"
                ),
                "actual_portfolio_weights_modified": (
                    schedule_dry_run_result_2426.get(
                        "actual_portfolio_weights_modified"
                    )
                ),
            },
            failure_message="Portfolio weights were mutated.",
            classification="manual_review_safety_boundary",
        ),
        _requirement(
            requirement_id="report_registry_registered",
            description="Report registry must include 2427 and required prior report ids.",
            passed=not missing_reports,
            evidence={"missing_report_ids": missing_reports},
            failure_message="Report registry missing required report ids.",
            classification="missing_manual_review_evidence",
        ),
        _requirement(
            requirement_id="artifact_catalog_registered",
            description="Artifact catalog must include manual review packet outputs.",
            passed=not missing_catalog_refs,
            evidence={"missing_catalog_references": missing_catalog_refs},
            failure_message="Artifact catalog missing manual review packet references.",
            classification="missing_manual_review_evidence",
        ),
        _requirement(
            requirement_id="system_flow_registered",
            description="System flow must include manual review packet command and route.",
            passed=not missing_flow_refs,
            evidence={"missing_system_flow_references": missing_flow_refs},
            failure_message="System flow missing manual review packet references.",
            classification="missing_manual_review_evidence",
        ),
        _requirement(
            requirement_id="research_docs_registered",
            description="Required prior research docs must be readable.",
            passed=not missing_docs,
            evidence={"missing_research_doc_ids": missing_docs},
            failure_message="Required research docs are missing.",
            classification="missing_manual_review_evidence",
        ),
    ]


def _default_manual_review_packet() -> dict[str, Any]:
    return {
        "manual_review_packet_ready": True,
        "packet_mode": "dry_run_no_advice",
        "manual_review_required": True,
        "manual_review_only": True,
        "contains_trading_advice": False,
        "contains_actionable_allocation": False,
        "contains_broker_order": False,
        "contains_portfolio_mutation": False,
        "review_sections": [
            "readiness_inheritance_summary",
            "safety_boundary_summary",
            "no_advice_boundary",
            "reviewer_handoff_manifest",
            "next_route_summary",
        ],
        "next_route": NEXT_ROUTE_READY,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_manual_review_checklist() -> list[dict[str, Any]]:
    checks = (
        "prior_2426_schedule_dry_run_ready",
        "prior_2425_dry_run_wiring_ready",
        "prior_2424_enablement_plan_ready",
        "manual_review_handoff_wired",
        "packet_is_dry_run_no_advice",
        "no_actionable_allocation_generated",
        "no_broker_order_generated",
        "portfolio_weights_unchanged",
        "paper_shadow_runtime_disabled",
        "schedule_disabled",
        "production_disabled",
        "broker_disabled",
        "next_route_resolved",
    )
    return [
        {
            "check_id": check_id,
            "passed": True,
            "production_effect": "none",
            "broker_action": "none",
        }
        for check_id in checks
    ]


def _default_no_advice_boundary_summary() -> dict[str, Any]:
    return {
        "no_advice_boundary_ready": True,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "allocation_change_generated": False,
        "recommendation_generated": False,
        "broker_order_generated": False,
        "order_generated": False,
        "portfolio_weight_mutated": False,
        "actual_portfolio_weights_modified": False,
        "generated_signal": False,
        "new_signal_generated": False,
        "new_feature_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "fresh_market_data_read": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_reviewer_handoff_manifest() -> dict[str, Any]:
    return {
        "reviewer_handoff_manifest_ready": True,
        "manual_review_required": True,
        "manual_review_only": True,
        "reviewer_action": "review_observe_only_boundary_readiness",
        "handoff_mode": "dry_run_no_advice",
        "automatic_execution_allowed": False,
        "next_route": NEXT_ROUTE_READY,
        "production_effect": "none",
        "broker_action": "none",
    }


def _manual_review_packet_section(
    *,
    status: str,
    packet: Mapping[str, Any],
) -> dict[str, Any]:
    section = dict(packet)
    section.update(
        {
            "schema_version": MANUAL_REVIEW_PACKET_SCHEMA_VERSION,
            "status": status,
            "manual_review_packet_ready": _manual_review_packet_ready(packet),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _manual_review_checklist_section(
    *,
    status: str,
    checklist: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": MANUAL_REVIEW_CHECKLIST_SCHEMA_VERSION,
        "status": status,
        "manual_review_checklist_ready": _manual_review_checklist_ready(checklist),
        "check_count": len(checklist),
        "failed_check_count": sum(1 for item in checklist if item.get("passed") is not True),
        "checks": [dict(item) for item in checklist],
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_advice_boundary_summary_section(
    *,
    status: str,
    summary: Mapping[str, Any],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    section = dict(summary)
    section.update(
        {
            "schema_version": NO_ADVICE_BOUNDARY_SUMMARY_SCHEMA_VERSION,
            "status": status,
            "manual_review_packet_gap_count": len(gaps),
            "gaps": [dict(gap) for gap in gaps],
            "no_advice_boundary_ready": _no_advice_boundary_ready(summary),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _reviewer_handoff_manifest_section(
    *,
    status: str,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    section = dict(manifest)
    section.update(
        {
            "schema_version": REVIEWER_HANDOFF_MANIFEST_SCHEMA_VERSION,
            "status": status,
            "reviewer_handoff_manifest_ready": _reviewer_handoff_ready(manifest),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _schedule_no_effect_ready(schedule_no_effect: Mapping[str, Any]) -> bool:
    return (
        schedule_no_effect.get("schedule_no_effect_audit_ready") is True
        and _int_value(schedule_no_effect.get("schedule_dry_run_gap_count")) == 0
        and _fields_false(
            schedule_no_effect,
            fields=(
                "paper_shadow_enabled",
                "paper_shadow_schedule_enabled",
                "paper_shadow_daily_job_run",
                "scheduler_enabled",
                "scheduled_task_created",
                "schedule_hook_invoked",
                "schedule_state_mutated",
                "generated_signal",
                "generated_trading_advice",
                "backtest_run",
                "scoring_run",
                "daily_report_run",
                "fresh_market_data_read",
                "portfolio_weight_mutated",
                "production_enabled",
                "broker_enabled",
                "broker_order_generated",
                "automatic_execution_allowed",
            ),
        )
    )


def _manual_handoff_wired(manual_handoff: Mapping[str, Any]) -> bool:
    return (
        manual_handoff.get("manual_review_required") is True
        and manual_handoff.get("manual_review_handoff_wired") is True
        and manual_handoff.get("automatic_execution_allowed") is False
    )


def _manual_review_packet_ready(packet: Mapping[str, Any]) -> bool:
    return (
        packet.get("manual_review_packet_ready") is True
        and packet.get("packet_mode") == "dry_run_no_advice"
        and packet.get("manual_review_required") is True
        and packet.get("contains_trading_advice") is False
        and packet.get("contains_actionable_allocation") is False
        and packet.get("contains_broker_order") is False
        and packet.get("contains_portfolio_mutation") is False
        and packet.get("next_route") == NEXT_ROUTE_READY
    )


def _manual_review_checklist_ready(
    checklist: Sequence[Mapping[str, Any]],
) -> bool:
    return bool(checklist) and all(item.get("passed") is True for item in checklist)


def _no_advice_boundary_ready(summary: Mapping[str, Any]) -> bool:
    return (
        summary.get("no_advice_boundary_ready") is True
        and _fields_false(
            summary,
            fields=(
                "generated_trading_advice",
                "trading_advice_generated",
                "actionable_allocation_generated",
                "allocation_change_generated",
                "recommendation_generated",
                "broker_order_generated",
                "order_generated",
                "portfolio_weight_mutated",
                "actual_portfolio_weights_modified",
                "generated_signal",
                "new_signal_generated",
                "new_feature_generated",
                "backtest_run",
                "scoring_run",
                "daily_report_generated",
                "daily_report_run",
                "fresh_market_data_read",
                "paper_shadow_enabled",
                "paper_shadow_schedule_enabled",
                "production_enabled",
                "broker_enabled",
                "automatic_execution_allowed",
            ),
        )
    )


def _reviewer_handoff_ready(manifest: Mapping[str, Any]) -> bool:
    return (
        manifest.get("reviewer_handoff_manifest_ready") is True
        and manifest.get("manual_review_required") is True
        and manifest.get("manual_review_only") is True
        and manifest.get("handoff_mode") == "dry_run_no_advice"
        and manifest.get("automatic_execution_allowed") is False
        and manifest.get("next_route") == NEXT_ROUTE_READY
    )


def _broker_disabled(*documents: Mapping[str, Any]) -> bool:
    for document in documents:
        if document.get("broker_action") not in (None, "none"):
            return False
        if not _fields_false(
            document,
            fields=(
                "broker_enabled",
                "broker_action_enabled",
                "order_generated",
                "broker_order_generated",
            ),
        ):
            return False
    return True


def _fields_false(*documents: Mapping[str, Any], fields: Sequence[str]) -> bool:
    return all(document.get(field) in (False, None) for document in documents for field in fields)


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    section = document.get(key)
    if isinstance(section, Mapping):
        return section
    return document


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
        "evidence": dict(evidence),
        "failure_message": None if passed else failure_message,
        "classification": classification,
        "production_effect": "none",
        "broker_action": "none",
    }


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requirement_id": requirement["requirement_id"],
        "classification": requirement["classification"],
        "gap": requirement["failure_message"],
        "evidence": requirement["evidence"],
        "production_effect": "none",
        "broker_action": "none",
    }


def _as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _int_value(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0
