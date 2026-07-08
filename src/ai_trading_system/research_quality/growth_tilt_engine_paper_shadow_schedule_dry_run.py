from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_paper_shadow_schedule_dry_run.v1"
SCHEDULE_BOUNDARY_CHECKLIST_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_schedule_boundary_checklist.v1"
)
SCHEDULE_NO_EFFECT_AUDIT_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_schedule_no_effect_audit_summary.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_PAPER_SHADOW_SCHEDULE_DRY_RUN_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_SCHEDULE_DRY_RUN_BLOCKED_BY_SCHEDULE_GAPS"
)
NEXT_ROUTE_READY = "TRADING-2427_Growth_Tilt_Engine_Manual_Review_Packet_Dry_Run"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2427_Growth_Tilt_Engine_Paper_Shadow_Schedule_Dry_Run_Gap_Remediation"
)
PRIOR_ROUTE = "TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring"
EXPECTED_PRIOR_NEXT_ROUTE = (
    "TRADING-2426_Growth_Tilt_Engine_Paper_Shadow_Schedule_Dry_Run"
)
PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING_READY"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
REPORT_TYPE = "growth_tilt_engine_paper_shadow_schedule_dry_run"

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_paper_shadow_dry_run_wiring",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-paper-shadow-schedule-dry-run",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_schedule_dry_run/"
    "schedule_dry_run_result.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_schedule_dry_run/"
    "schedule_boundary_checklist.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_schedule_dry_run/"
    "schedule_no_effect_audit_summary.json",
    "docs/research/growth_tilt_engine_paper_shadow_schedule_dry_run.md",
    "docs/research/growth_tilt_engine_paper_shadow_schedule_boundary_checklist.md",
    "docs/research/growth_tilt_engine_paper_shadow_schedule_no_effect_audit_summary.md",
    "docs/research/dynamic_strategy_2427_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-paper-shadow-schedule-dry-run",
    READY_STATUS,
    NEXT_ROUTE_READY,
)


def build_growth_tilt_engine_paper_shadow_schedule_dry_run(
    dry_run_wiring_result_2425: Mapping[str, Any],
    schedule_hook_disabled_verification_2425: Mapping[str, Any],
    runtime_boundary_manifest_2425: Mapping[str, Any],
    manual_review_handoff_wiring_plan_2425: Mapping[str, Any],
    dry_run_no_effect_audit_summary_2425: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    schedule_dry_run_plan: Mapping[str, Any] | None = None,
    schedule_boundary_checklist: Sequence[Mapping[str, Any]] | None = None,
    schedule_no_effect_audit_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    schedule_hook = _section(
        schedule_hook_disabled_verification_2425,
        "schedule_hook_disabled_verification",
    )
    runtime_boundary = _section(
        runtime_boundary_manifest_2425,
        "runtime_boundary_manifest",
    )
    manual_handoff = _section(
        manual_review_handoff_wiring_plan_2425,
        "manual_review_handoff_wiring_plan",
    )
    prior_no_effect = _section(
        dry_run_no_effect_audit_summary_2425,
        "dry_run_no_effect_audit_summary",
    )
    resolved_plan = (
        _default_schedule_dry_run_plan()
        if schedule_dry_run_plan is None
        else dict(schedule_dry_run_plan)
    )
    resolved_checklist = (
        _default_schedule_boundary_checklist()
        if schedule_boundary_checklist is None
        else [dict(item) for item in schedule_boundary_checklist]
    )
    resolved_audit = (
        _default_schedule_no_effect_audit_summary()
        if schedule_no_effect_audit_summary is None
        else dict(schedule_no_effect_audit_summary)
    )

    requirements = _schedule_dry_run_requirements(
        dry_run_wiring_result_2425,
        schedule_hook,
        runtime_boundary,
        manual_handoff,
        prior_no_effect,
        schedule_dry_run_plan=resolved_plan,
        schedule_boundary_checklist=resolved_checklist,
        schedule_no_effect_audit_summary=resolved_audit,
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
        1 for gap in gaps if gap["classification"] == "missing_schedule_evidence"
    )
    safety_boundary_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "schedule_safety_boundary"
    )
    schedule_contract_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "schedule_contract_gap"
    )
    precondition_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "schedule_precondition_gap"
    )

    boundary_checklist = _schedule_boundary_checklist_section(
        status=status,
        checklist=resolved_checklist,
    )
    no_effect_audit = _schedule_no_effect_audit_summary_section(
        status=status,
        audit=resolved_audit,
        gaps=gaps,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2426",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": ["TRADING-2425"],
        "pit_gate_ready": dry_run_wiring_result_2425.get("pit_gate_ready") is True,
        "pit_gate_ready_count": _int_value(
            dry_run_wiring_result_2425.get("pit_gate_ready_count")
        ),
        "contract_ready": dry_run_wiring_result_2425.get("contract_ready") is True,
        "contract_ready_count": _int_value(
            dry_run_wiring_result_2425.get("contract_ready_count")
        ),
        "contract_gap_count": _int_value(
            dry_run_wiring_result_2425.get("contract_gap_count")
        ),
        "paper_shadow_dry_run_wiring_status": dry_run_wiring_result_2425.get(
            "status"
        ),
        "paper_shadow_dry_run_wiring_ready": (
            dry_run_wiring_result_2425.get("dry_run_wiring_ready") is True
        ),
        "dry_run_wiring_gap_count": _int_value(
            dry_run_wiring_result_2425.get("dry_run_wiring_gap_count")
        ),
        "schedule_hook_verified_disabled": _schedule_hook_disabled(schedule_hook),
        "runtime_boundary_verified": _runtime_boundary_disabled(runtime_boundary),
        "manual_review_handoff_wired": _manual_review_handoff_wired(manual_handoff),
        "prior_no_effect_audit_ready": _prior_no_effect_ready(prior_no_effect),
        "paper_shadow_schedule_dry_run_started": True,
        "paper_shadow_schedule_dry_run_completed": True,
        "paper_shadow_schedule_dry_run_ready": ready,
        "schedule_dry_run_plan_ready": _schedule_dry_run_plan_ready(resolved_plan),
        "schedule_boundary_checklist_ready": _schedule_boundary_checklist_ready(
            resolved_checklist
        ),
        "schedule_no_effect_audit_ready": _schedule_no_effect_audit_ready(
            resolved_audit
        ),
        "schedule_dry_run_gap_count": len(gaps),
        "schedule_dry_run_gap_ids": [gap["requirement_id"] for gap in gaps],
        "missing_schedule_evidence_count": missing_evidence_count,
        "safety_boundary_gap_count": safety_boundary_gap_count,
        "schedule_contract_gap_count": schedule_contract_gap_count,
        "precondition_gap_count": precondition_gap_count,
        "schedule_dry_run_requirement_count": len(requirements),
        "schedule_dry_run_requirement_pass_count": len(requirements) - len(gaps),
        "schedule_dry_run_requirement_fail_count": len(gaps),
        "schedule_dry_run_plan": resolved_plan,
        "schedule_boundary_checklist": boundary_checklist,
        "schedule_no_effect_audit_summary": no_effect_audit,
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "cron_or_windows_task_created": False,
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
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "new_signal_generated": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
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
            "Schedule dry-run evidence is complete; TRADING-2427 may generate a "
            "manual review packet dry-run while runtime schedule, production, and "
            "broker paths remain disabled."
            if ready
            else "Schedule dry-run found gaps; remediate schedule wiring evidence "
            "before manual review packet dry-run."
        ),
    }


def _schedule_dry_run_requirements(
    dry_run_wiring_result_2425: Mapping[str, Any],
    schedule_hook: Mapping[str, Any],
    runtime_boundary: Mapping[str, Any],
    manual_handoff: Mapping[str, Any],
    prior_no_effect: Mapping[str, Any],
    *,
    schedule_dry_run_plan: Mapping[str, Any],
    schedule_boundary_checklist: Sequence[Mapping[str, Any]],
    schedule_no_effect_audit_summary: Mapping[str, Any],
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
            requirement_id="prior_dry_run_wiring_ready",
            description="TRADING-2425 paper-shadow dry-run wiring must be READY.",
            passed=(
                dry_run_wiring_result_2425.get("status")
                == PAPER_SHADOW_DRY_RUN_WIRING_READY_STATUS
                and dry_run_wiring_result_2425.get("dry_run_wiring_ready") is True
                and _int_value(
                    dry_run_wiring_result_2425.get("dry_run_wiring_gap_count")
                )
                == 0
                and dry_run_wiring_result_2425.get("recommended_next_research_task")
                == EXPECTED_PRIOR_NEXT_ROUTE
            ),
            evidence={
                "status": dry_run_wiring_result_2425.get("status"),
                "dry_run_wiring_ready": dry_run_wiring_result_2425.get(
                    "dry_run_wiring_ready"
                ),
                "dry_run_wiring_gap_count": dry_run_wiring_result_2425.get(
                    "dry_run_wiring_gap_count"
                ),
                "next_route": dry_run_wiring_result_2425.get(
                    "recommended_next_research_task"
                ),
            },
            failure_message="TRADING-2425 dry-run wiring is not READY.",
            classification="schedule_precondition_gap",
        ),
        _requirement(
            requirement_id="schedule_hook_remains_disabled",
            description="TRADING-2425 schedule hook verification must remain disabled.",
            passed=_schedule_hook_disabled(schedule_hook),
            evidence=dict(schedule_hook),
            failure_message="Schedule hook is not explicitly disabled.",
            classification="schedule_precondition_gap",
        ),
        _requirement(
            requirement_id="runtime_boundary_remains_disabled",
            description="Runtime boundary must remain disabled.",
            passed=_runtime_boundary_disabled(runtime_boundary),
            evidence=dict(runtime_boundary),
            failure_message="Runtime boundary is enabled or incomplete.",
            classification="schedule_precondition_gap",
        ),
        _requirement(
            requirement_id="manual_review_handoff_resolves",
            description="Manual review handoff route must remain wired.",
            passed=_manual_review_handoff_wired(manual_handoff),
            evidence=dict(manual_handoff),
            failure_message="Manual review handoff is missing or incomplete.",
            classification="schedule_precondition_gap",
        ),
        _requirement(
            requirement_id="prior_no_effect_audit_ready",
            description="TRADING-2425 no-effect audit must remain ready.",
            passed=_prior_no_effect_ready(prior_no_effect),
            evidence=dict(prior_no_effect),
            failure_message="Prior no-effect audit is missing or shows effects.",
            classification="schedule_precondition_gap",
        ),
        _requirement(
            requirement_id="schedule_dry_run_plan_resolves",
            description="Schedule dry-run plan must resolve without side effects.",
            passed=_schedule_dry_run_plan_ready(schedule_dry_run_plan),
            evidence=dict(schedule_dry_run_plan),
            failure_message="Schedule dry-run plan is incomplete or has effects.",
            classification="schedule_contract_gap",
        ),
        _requirement(
            requirement_id="schedule_boundary_checklist_passes",
            description="Schedule boundary checklist must pass.",
            passed=_schedule_boundary_checklist_ready(schedule_boundary_checklist),
            evidence={
                "check_count": len(schedule_boundary_checklist),
                "failed_check_ids": [
                    item.get("check_id")
                    for item in schedule_boundary_checklist
                    if item.get("passed") is not True
                ],
            },
            failure_message="Schedule boundary checklist has failing checks.",
            classification="schedule_contract_gap",
        ),
        _requirement(
            requirement_id="schedule_no_effect_audit_ready",
            description="Schedule dry-run no-effect audit must be ready.",
            passed=_schedule_no_effect_audit_ready(schedule_no_effect_audit_summary),
            evidence=dict(schedule_no_effect_audit_summary),
            failure_message="Schedule no-effect audit is missing or shows effects.",
            classification="schedule_contract_gap",
        ),
        _requirement(
            requirement_id="paper_shadow_runtime_disabled",
            description="Paper-shadow runtime must remain disabled.",
            passed=_fields_false(
                dry_run_wiring_result_2425,
                runtime_boundary,
                fields=(
                    "paper_shadow_enabled",
                    "paper_shadow_allowed",
                    "paper_trade_created",
                    "shadow_position_created",
                ),
            ),
            evidence={
                "paper_shadow_enabled": dry_run_wiring_result_2425.get(
                    "paper_shadow_enabled"
                ),
                "runtime_paper_shadow_enabled": runtime_boundary.get(
                    "paper_shadow_enabled"
                ),
            },
            failure_message="Paper-shadow runtime is enabled.",
            classification="schedule_safety_boundary",
        ),
        _requirement(
            requirement_id="paper_shadow_schedule_disabled",
            description="Paper-shadow schedule and scheduler must remain disabled.",
            passed=_fields_false(
                dry_run_wiring_result_2425,
                schedule_hook,
                runtime_boundary,
                fields=(
                    "paper_shadow_schedule_enabled",
                    "paper_shadow_daily_job_enabled",
                    "paper_shadow_daily_job_run",
                    "scheduler_enabled",
                    "scheduled_task_created",
                    "cron_or_windows_task_created",
                    "schedule_hook_invoked",
                    "schedule_state_mutated",
                ),
            ),
            evidence={
                "paper_shadow_schedule_enabled": dry_run_wiring_result_2425.get(
                    "paper_shadow_schedule_enabled"
                ),
                "scheduler_enabled": schedule_hook.get("scheduler_enabled"),
                "scheduled_task_created": schedule_hook.get("scheduled_task_created"),
            },
            failure_message="Paper-shadow schedule or scheduler is enabled.",
            classification="schedule_safety_boundary",
        ),
        _requirement(
            requirement_id="production_disabled",
            description="Production path must remain disabled.",
            passed=_fields_false(
                dry_run_wiring_result_2425,
                runtime_boundary,
                fields=("production_enabled", "production_allowed"),
            ),
            evidence={
                "production_enabled": dry_run_wiring_result_2425.get(
                    "production_enabled"
                ),
                "runtime_production_enabled": runtime_boundary.get(
                    "production_enabled"
                ),
            },
            failure_message="Production path is enabled.",
            classification="schedule_safety_boundary",
        ),
        _requirement(
            requirement_id="broker_disabled",
            description="Broker and order paths must remain disabled.",
            passed=_broker_disabled(dry_run_wiring_result_2425, runtime_boundary),
            evidence={
                "broker_enabled": dry_run_wiring_result_2425.get("broker_enabled"),
                "broker_action": dry_run_wiring_result_2425.get("broker_action"),
                "broker_order_generated": dry_run_wiring_result_2425.get(
                    "broker_order_generated"
                ),
            },
            failure_message="Broker or order path is enabled.",
            classification="schedule_safety_boundary",
        ),
        _requirement(
            requirement_id="automatic_execution_disabled",
            description="Automatic execution must remain disabled.",
            passed=_fields_false(
                dry_run_wiring_result_2425,
                manual_handoff,
                runtime_boundary,
                fields=("automatic_execution_allowed",),
            ),
            evidence={
                "automatic_execution_allowed": dry_run_wiring_result_2425.get(
                    "automatic_execution_allowed"
                ),
                "manual_handoff_automatic_execution_allowed": manual_handoff.get(
                    "automatic_execution_allowed"
                ),
            },
            failure_message="Automatic execution is allowed.",
            classification="schedule_safety_boundary",
        ),
        _requirement(
            requirement_id="no_signal_or_advice_generation",
            description="Schedule dry-run must not generate signals or advice.",
            passed=_fields_false(
                dry_run_wiring_result_2425,
                fields=(
                    "generated_signal",
                    "new_signal_generated",
                    "generated_trading_advice",
                    "trading_advice_generated",
                    "actionable_allocation_generated",
                    "new_feature_generated",
                ),
            ),
            evidence={
                "generated_signal": dry_run_wiring_result_2425.get(
                    "generated_signal"
                ),
                "generated_trading_advice": dry_run_wiring_result_2425.get(
                    "generated_trading_advice"
                ),
            },
            failure_message="Signal, feature, advice, or allocation output exists.",
            classification="schedule_safety_boundary",
        ),
        _requirement(
            requirement_id="no_backtest_scoring_daily_report_or_fresh_data",
            description=(
                "Schedule dry-run must not run backtest, scoring, daily report, "
                "or fresh cached market data reads."
            ),
            passed=_fields_false(
                dry_run_wiring_result_2425,
                fields=(
                    "backtest_run",
                    "new_strategy_backtest_run",
                    "scoring_run",
                    "daily_report_generated",
                    "daily_report_run",
                    "fresh_market_data_read",
                ),
            ),
            evidence={
                "backtest_run": dry_run_wiring_result_2425.get("backtest_run"),
                "scoring_run": dry_run_wiring_result_2425.get("scoring_run"),
                "daily_report_run": dry_run_wiring_result_2425.get(
                    "daily_report_run"
                ),
                "fresh_market_data_read": dry_run_wiring_result_2425.get(
                    "fresh_market_data_read"
                ),
            },
            failure_message="Backtest/scoring/daily report/fresh data path ran.",
            classification="schedule_safety_boundary",
        ),
        _requirement(
            requirement_id="portfolio_weights_unchanged",
            description="Actual portfolio weights must not be mutated.",
            passed=_fields_false(
                dry_run_wiring_result_2425,
                fields=(
                    "portfolio_weight_mutated",
                    "actual_portfolio_weights_modified",
                ),
            ),
            evidence={
                "portfolio_weight_mutated": dry_run_wiring_result_2425.get(
                    "portfolio_weight_mutated"
                ),
                "actual_portfolio_weights_modified": dry_run_wiring_result_2425.get(
                    "actual_portfolio_weights_modified"
                ),
            },
            failure_message="Portfolio weights were mutated.",
            classification="schedule_safety_boundary",
        ),
        _requirement(
            requirement_id="report_registry_registered",
            description="Report registry must include 2426 and required prior report ids.",
            passed=not missing_reports,
            evidence={"missing_report_ids": missing_reports},
            failure_message="Report registry missing required report ids.",
            classification="missing_schedule_evidence",
        ),
        _requirement(
            requirement_id="artifact_catalog_registered",
            description="Artifact catalog must include schedule dry-run outputs.",
            passed=not missing_catalog_refs,
            evidence={"missing_catalog_references": missing_catalog_refs},
            failure_message="Artifact catalog missing schedule dry-run references.",
            classification="missing_schedule_evidence",
        ),
        _requirement(
            requirement_id="system_flow_registered",
            description="System flow must include schedule dry-run command and route.",
            passed=not missing_flow_refs,
            evidence={"missing_system_flow_references": missing_flow_refs},
            failure_message="System flow missing schedule dry-run references.",
            classification="missing_schedule_evidence",
        ),
        _requirement(
            requirement_id="research_docs_registered",
            description="Required prior research docs must be readable.",
            passed=not missing_docs,
            evidence={"missing_research_doc_ids": missing_docs},
            failure_message="Required research docs are missing.",
            classification="missing_schedule_evidence",
        ),
    ]


def _default_schedule_dry_run_plan() -> dict[str, Any]:
    return {
        "schedule_dry_run_plan_ready": True,
        "dry_run_mode": "contract_only_no_effect",
        "schedule_hook_id": "growth_tilt_engine_paper_shadow_schedule",
        "schedule_hook_invoked": False,
        "schedule_state_mutated": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "cron_or_windows_task_created": False,
        "paper_shadow_daily_job_run": False,
        "artifact_route_resolved": True,
        "manual_review_required": True,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_schedule_boundary_checklist() -> list[dict[str, Any]]:
    checks = (
        "prior_2425_wiring_ready",
        "schedule_hook_disabled",
        "scheduler_not_mutated",
        "scheduled_task_not_created",
        "daily_job_not_run",
        "paper_shadow_runtime_disabled",
        "production_disabled",
        "broker_disabled",
        "no_signal_or_advice_generated",
        "no_backtest_scoring_daily_report_or_fresh_data",
        "manual_review_required",
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


def _default_schedule_no_effect_audit_summary() -> dict[str, Any]:
    return {
        "schedule_no_effect_audit_ready": True,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_run": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "schedule_hook_invoked": False,
        "schedule_state_mutated": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "fresh_market_data_read": False,
        "portfolio_weight_mutated": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_order_generated": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _schedule_boundary_checklist_section(
    *,
    status: str,
    checklist: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEDULE_BOUNDARY_CHECKLIST_SCHEMA_VERSION,
        "status": status,
        "schedule_boundary_checklist_ready": _schedule_boundary_checklist_ready(
            checklist
        ),
        "check_count": len(checklist),
        "failed_check_count": sum(1 for item in checklist if item.get("passed") is not True),
        "checks": [dict(item) for item in checklist],
        "production_effect": "none",
        "broker_action": "none",
    }


def _schedule_no_effect_audit_summary_section(
    *,
    status: str,
    audit: Mapping[str, Any],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    section = dict(audit)
    section.update(
        {
            "schema_version": SCHEDULE_NO_EFFECT_AUDIT_SUMMARY_SCHEMA_VERSION,
            "status": status,
            "schedule_dry_run_gap_count": len(gaps),
            "gaps": [dict(gap) for gap in gaps],
            "schedule_no_effect_audit_ready": _schedule_no_effect_audit_ready(audit),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _schedule_hook_disabled(schedule_hook: Mapping[str, Any]) -> bool:
    return (
        schedule_hook.get("schedule_hook_verified_disabled") is True
        and _fields_false(
            schedule_hook,
            fields=(
                "paper_shadow_schedule_enabled",
                "scheduler_enabled",
                "scheduled_task_created",
                "paper_shadow_daily_job_run",
                "cron_or_windows_task_created",
                "schedule_hook_invoked",
                "schedule_state_mutated",
            ),
        )
    )


def _runtime_boundary_disabled(runtime_boundary: Mapping[str, Any]) -> bool:
    return _fields_false(
        runtime_boundary,
        fields=(
            "paper_shadow_enabled",
            "paper_shadow_schedule_enabled",
            "production_enabled",
            "broker_enabled",
            "automatic_execution_allowed",
        ),
    )


def _manual_review_handoff_wired(manual_handoff: Mapping[str, Any]) -> bool:
    return (
        manual_handoff.get("manual_review_required") is True
        and manual_handoff.get("manual_review_handoff_wired") is True
        and manual_handoff.get("automatic_execution_allowed") is False
    )


def _prior_no_effect_ready(prior_no_effect: Mapping[str, Any]) -> bool:
    return (
        prior_no_effect.get("no_effect_audit_ready") is True
        and _int_value(prior_no_effect.get("dry_run_wiring_gap_count")) == 0
        and _fields_false(
            prior_no_effect,
            fields=(
                "paper_shadow_enabled",
                "paper_shadow_schedule_enabled",
                "production_enabled",
                "broker_enabled",
                "broker_order_generated",
                "portfolio_weight_mutated",
                "generated_signal",
                "generated_trading_advice",
                "backtest_run",
                "scoring_run",
                "daily_report_run",
                "fresh_market_data_read",
                "automatic_execution_allowed",
            ),
        )
    )


def _schedule_dry_run_plan_ready(plan: Mapping[str, Any]) -> bool:
    return (
        plan.get("schedule_dry_run_plan_ready") is True
        and plan.get("artifact_route_resolved") is True
        and plan.get("manual_review_required") is True
        and _fields_false(
            plan,
            fields=(
                "schedule_hook_invoked",
                "schedule_state_mutated",
                "scheduler_enabled",
                "scheduled_task_created",
                "cron_or_windows_task_created",
                "paper_shadow_daily_job_run",
                "automatic_execution_allowed",
            ),
        )
    )


def _schedule_boundary_checklist_ready(
    checklist: Sequence[Mapping[str, Any]],
) -> bool:
    return bool(checklist) and all(item.get("passed") is True for item in checklist)


def _schedule_no_effect_audit_ready(audit: Mapping[str, Any]) -> bool:
    return (
        audit.get("schedule_no_effect_audit_ready") is True
        and _fields_false(
            audit,
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
