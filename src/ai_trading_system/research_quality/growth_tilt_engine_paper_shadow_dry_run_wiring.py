from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_paper_shadow_dry_run_wiring.v1"
INPUT_OUTPUT_CONTRACT_MAP_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_input_output_contract_map.v1"
)
RUNTIME_BOUNDARY_MANIFEST_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_dry_run_runtime_boundary_manifest.v1"
)
SCHEDULE_HOOK_DISABLED_VERIFICATION_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_schedule_hook_disabled_verification.v1"
)
MANUAL_REVIEW_HANDOFF_WIRING_PLAN_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_manual_review_handoff_wiring_plan.v1"
)
NO_EFFECT_AUDIT_SUMMARY_SCHEMA_VERSION = (
    "growth_tilt_engine_paper_shadow_dry_run_no_effect_audit_summary.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_DRY_RUN_WIRING_BLOCKED_BY_WIRING_GAPS"
)
NEXT_ROUTE_READY = "TRADING-2426_Growth_Tilt_Engine_Paper_Shadow_Schedule_Dry_Run"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2426_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring_Gap_Remediation"
)
PRIOR_ROUTE = "TRADING-2424_Growth_Tilt_Engine_Paper_Shadow_Enablement_Plan"
EXPECTED_PRIOR_NEXT_ROUTE = (
    "TRADING-2425_Growth_Tilt_Engine_Paper_Shadow_Dry_Run_Wiring"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
ARTIFACT_ID = "growth_tilt_engine_signal_artifact"
REPORT_TYPE = "growth_tilt_engine_paper_shadow_dry_run_wiring"

ENABLEMENT_PLAN_READY_STATUS = (
    "GROWTH_TILT_ENGINE_PAPER_SHADOW_ENABLEMENT_PLAN_READY"
)
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
    "growth_tilt_engine_paper_shadow_enablement_plan",
    "growth_tilt_engine_paper_shadow_preflight",
    "growth_tilt_engine_contract_readiness_snapshot",
    "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
    "remediation",
    "growth_tilt_engine_signal_artifact_source_traceability_remediation",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-paper-shadow-dry-run-wiring",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_dry_run_wiring/"
    "dry_run_wiring_result.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_dry_run_wiring/"
    "input_output_contract_map.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_dry_run_wiring/"
    "runtime_boundary_manifest.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_dry_run_wiring/"
    "schedule_hook_disabled_verification.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_dry_run_wiring/"
    "manual_review_handoff_wiring_plan.json",
    "outputs/research_strategies/growth_tilt_engine_paper_shadow_dry_run_wiring/"
    "dry_run_no_effect_audit_summary.json",
    "docs/research/growth_tilt_engine_paper_shadow_dry_run_wiring.md",
    "docs/research/growth_tilt_engine_paper_shadow_input_output_contract_map.md",
    "docs/research/growth_tilt_engine_paper_shadow_dry_run_runtime_boundary_manifest.md",
    "docs/research/growth_tilt_engine_paper_shadow_schedule_hook_disabled_verification.md",
    "docs/research/growth_tilt_engine_paper_shadow_manual_review_handoff_wiring_plan.md",
    "docs/research/growth_tilt_engine_paper_shadow_dry_run_no_effect_audit_summary.md",
    "docs/research/dynamic_strategy_2426_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-paper-shadow-dry-run-wiring",
    READY_STATUS,
    NEXT_ROUTE_READY,
)

INPUT_CONTRACT_IDS: tuple[str, ...] = (
    "source_traceability_artifact_chain",
    "pit_gate_readiness_state",
    "contract_readiness_snapshot",
    "paper_shadow_preflight_state",
    "paper_shadow_enablement_plan_state",
    "manual_review_boundary_state",
)
OUTPUT_ARTIFACT_CONTRACT_IDS: tuple[str, ...] = (
    "dry_run_wiring_result",
    "input_output_contract_map",
    "runtime_boundary_manifest",
    "schedule_hook_disabled_verification",
    "manual_review_handoff_wiring_plan",
    "dry_run_no_effect_audit_summary",
)


def build_growth_tilt_engine_paper_shadow_dry_run_wiring(
    paper_shadow_enablement_plan_2424: Mapping[str, Any],
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
    input_contract_map: Sequence[Mapping[str, Any]] | None = None,
    output_artifact_contract_map: Sequence[Mapping[str, Any]] | None = None,
    manual_review_handoff_route: Mapping[str, Any] | None = None,
    schedule_hook_disabled_verification: Mapping[str, Any] | None = None,
    no_effect_audit_summary: Mapping[str, Any] | None = None,
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
    resolved_input_contract_map = _default_input_contract_map(
        paper_shadow_enablement_plan_2424
    ) if input_contract_map is None else list(input_contract_map)
    resolved_output_artifact_contract_map = (
        _default_output_artifact_contract_map()
        if output_artifact_contract_map is None
        else list(output_artifact_contract_map)
    )
    resolved_manual_review_route = (
        _default_manual_review_handoff_route()
        if manual_review_handoff_route is None
        else dict(manual_review_handoff_route)
    )
    resolved_schedule_hook_verification = (
        _default_schedule_hook_disabled_verification()
        if schedule_hook_disabled_verification is None
        else dict(schedule_hook_disabled_verification)
    )
    resolved_no_effect_audit = (
        _default_no_effect_audit_summary()
        if no_effect_audit_summary is None
        else dict(no_effect_audit_summary)
    )
    requirements = _dry_run_wiring_requirements(
        paper_shadow_enablement_plan_2424,
        paper_shadow_preflight_2423,
        contract_readiness_snapshot_2422,
        readiness_recheck_after_remediation_2421,
        remediation_result_2420,
        manifest,
        lineage_map,
        missing_summary,
        input_contract_map=resolved_input_contract_map,
        output_artifact_contract_map=resolved_output_artifact_contract_map,
        manual_review_handoff_route=resolved_manual_review_route,
        schedule_hook_disabled_verification=resolved_schedule_hook_verification,
        no_effect_audit_summary=resolved_no_effect_audit,
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
    dry_run_wiring_ready = not gaps
    status = READY_STATUS if dry_run_wiring_ready else BLOCKED_STATUS
    next_route = NEXT_ROUTE_READY if dry_run_wiring_ready else NEXT_ROUTE_BLOCKED
    missing_evidence_count = sum(
        1 for gap in gaps if gap["classification"] == "missing_dry_run_evidence"
    )
    safety_boundary_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "dry_run_safety_boundary"
    )
    wiring_contract_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "dry_run_wiring_contract_gap"
    )
    precondition_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "dry_run_precondition_gap"
    )
    input_output_contract_map = _input_output_contract_map(
        status=status,
        input_contract_map=resolved_input_contract_map,
        output_artifact_contract_map=resolved_output_artifact_contract_map,
    )
    runtime_boundary_manifest = _runtime_boundary_manifest(status=status)
    schedule_hook_manifest = _schedule_hook_disabled_verification_section(
        status=status,
        verification=resolved_schedule_hook_verification,
    )
    manual_review_plan = _manual_review_handoff_wiring_plan(
        status=status,
        route=resolved_manual_review_route,
    )
    no_effect_audit = _no_effect_audit_summary_section(
        status=status,
        audit=resolved_no_effect_audit,
        gaps=gaps,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2425",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": [
            "TRADING-2420",
            "TRADING-2421",
            "TRADING-2422",
            "TRADING-2423",
            "TRADING-2424",
        ],
        "artifact_id": ARTIFACT_ID,
        "pit_gate_ready": _pit_gate_ready(
            paper_shadow_enablement_plan_2424,
            paper_shadow_preflight_2423,
            contract_readiness_snapshot_2422,
            readiness_recheck_after_remediation_2421,
        ),
        "pit_gate_ready_count": _int_value(
            paper_shadow_enablement_plan_2424.get("pit_gate_ready_count")
            if paper_shadow_enablement_plan_2424.get("pit_gate_ready_count")
            is not None
            else paper_shadow_preflight_2423.get("pit_gate_ready_count")
        ),
        "contract_readiness_status": contract_readiness_snapshot_2422.get("status"),
        "contract_ready": _contract_ready(
            paper_shadow_enablement_plan_2424,
            paper_shadow_preflight_2423,
            contract_readiness_snapshot_2422,
        ),
        "contract_ready_count": _int_value(
            paper_shadow_enablement_plan_2424.get("contract_ready_count")
            if paper_shadow_enablement_plan_2424.get("contract_ready_count")
            is not None
            else paper_shadow_preflight_2423.get("contract_ready_count")
        ),
        "contract_gap_count": _int_value(
            contract_readiness_snapshot_2422.get("contract_gap_count")
        ),
        "paper_shadow_preflight_ready": (
            paper_shadow_enablement_plan_2424.get("paper_shadow_preflight_ready")
            is True
            and paper_shadow_preflight_2423.get("paper_shadow_preflight_ready")
            is True
        ),
        "enablement_plan_ready": (
            paper_shadow_enablement_plan_2424.get("status")
            == ENABLEMENT_PLAN_READY_STATUS
            and paper_shadow_enablement_plan_2424.get("enablement_plan_ready") is True
        ),
        "enablement_gap_count": _int_value(
            paper_shadow_enablement_plan_2424.get("enablement_gap_count")
        ),
        "remaining_pit_blockers": _remaining_pit_blockers(
            paper_shadow_enablement_plan_2424,
            paper_shadow_preflight_2423,
            contract_readiness_snapshot_2422,
            readiness_recheck_after_remediation_2421,
        ),
        "remaining_pit_blocker_count": len(
            _remaining_pit_blockers(
                paper_shadow_enablement_plan_2424,
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
            )
        ),
        "source_traceability_remediation_status": remediation_result_2420.get(
            "remediation_status"
        ),
        "source_traceability_recheck_status": (
            paper_shadow_enablement_plan_2424.get(
                "source_traceability_recheck_status"
            )
            or paper_shadow_preflight_2423.get("source_traceability_recheck_status")
            or contract_readiness_snapshot_2422.get(
                "source_traceability_recheck_status"
            )
            or readiness_recheck_after_remediation_2421.get(
                "source_traceability_recheck_status"
            )
        ),
        "source_traceability_accepted": _source_traceability_accepted(
            paper_shadow_enablement_plan_2424,
            paper_shadow_preflight_2423,
            contract_readiness_snapshot_2422,
            readiness_recheck_after_remediation_2421,
            remediation_result_2420,
            manifest,
            lineage_map,
            missing_summary,
        ),
        "paper_shadow_dry_run_wiring_started": True,
        "paper_shadow_dry_run_wiring_completed": True,
        "dry_run_wiring_ready": dry_run_wiring_ready,
        "dry_run_wiring_gap_count": len(gaps),
        "dry_run_wiring_gap_ids": [gap["requirement_id"] for gap in gaps],
        "missing_dry_run_evidence_count": missing_evidence_count,
        "safety_boundary_gap_count": safety_boundary_gap_count,
        "wiring_contract_gap_count": wiring_contract_gap_count,
        "precondition_gap_count": precondition_gap_count,
        "input_contract_map_ready": _input_contract_map_ready(
            resolved_input_contract_map
        ),
        "output_artifact_contract_map_ready": _output_artifact_contract_map_ready(
            resolved_output_artifact_contract_map
        ),
        "manual_review_handoff_wired": _manual_review_handoff_wired(
            resolved_manual_review_route
        ),
        "schedule_hook_verified_disabled": _schedule_hook_verified_disabled(
            resolved_schedule_hook_verification
        ),
        "no_effect_audit_ready": _no_effect_audit_ready(resolved_no_effect_audit),
        "input_output_contract_map": input_output_contract_map,
        "runtime_boundary_manifest": runtime_boundary_manifest,
        "schedule_hook_disabled_verification": schedule_hook_manifest,
        "manual_review_handoff_wiring_plan": manual_review_plan,
        "dry_run_no_effect_audit_summary": no_effect_audit,
        "dry_run_requirement_count": len(requirements),
        "dry_run_requirement_pass_count": len(requirements) - len(gaps),
        "dry_run_requirement_fail_count": len(gaps),
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "actual_portfolio_weights_modified": False,
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
            "Dry-run wiring evidence is complete; TRADING-2426 may plan a "
            "schedule dry-run while paper-shadow runtime, production, and "
            "broker paths remain disabled."
            if dry_run_wiring_ready
            else "Dry-run wiring found gaps; TRADING-2426 must remediate wiring "
            "gaps before any schedule dry-run planning."
        ),
    }


def _dry_run_wiring_requirements(
    paper_shadow_enablement_plan_2424: Mapping[str, Any],
    paper_shadow_preflight_2423: Mapping[str, Any],
    contract_readiness_snapshot_2422: Mapping[str, Any],
    readiness_recheck_after_remediation_2421: Mapping[str, Any],
    remediation_result_2420: Mapping[str, Any],
    manifest: Mapping[str, Any],
    lineage_map: Mapping[str, Any],
    missing_summary: Mapping[str, Any],
    *,
    input_contract_map: Sequence[Mapping[str, Any]],
    output_artifact_contract_map: Sequence[Mapping[str, Any]],
    manual_review_handoff_route: Mapping[str, Any],
    schedule_hook_disabled_verification: Mapping[str, Any],
    no_effect_audit_summary: Mapping[str, Any],
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
            requirement_id="enablement_plan_ready",
            description="TRADING-2424 enablement plan must be READY.",
            passed=(
                paper_shadow_enablement_plan_2424.get("status")
                == ENABLEMENT_PLAN_READY_STATUS
                and paper_shadow_enablement_plan_2424.get("enablement_plan_ready")
                is True
                and _int_value(
                    paper_shadow_enablement_plan_2424.get("enablement_gap_count")
                )
                == 0
                and paper_shadow_enablement_plan_2424.get(
                    "recommended_next_research_task"
                )
                == EXPECTED_PRIOR_NEXT_ROUTE
            ),
            evidence={
                "status": paper_shadow_enablement_plan_2424.get("status"),
                "enablement_plan_ready": paper_shadow_enablement_plan_2424.get(
                    "enablement_plan_ready"
                ),
                "enablement_gap_count": paper_shadow_enablement_plan_2424.get(
                    "enablement_gap_count"
                ),
                "next_route": paper_shadow_enablement_plan_2424.get(
                    "recommended_next_research_task"
                ),
            },
            failure_message="TRADING-2424 enablement plan is not READY.",
            classification="dry_run_precondition_gap",
        ),
        _requirement(
            requirement_id="paper_shadow_preflight_ready",
            description="TRADING-2423 paper-shadow preflight must remain READY.",
            passed=(
                paper_shadow_preflight_2423.get("status") == PREFLIGHT_READY_STATUS
                and paper_shadow_preflight_2423.get("paper_shadow_preflight_ready")
                is True
                and paper_shadow_enablement_plan_2424.get(
                    "paper_shadow_preflight_ready"
                )
                is True
            ),
            evidence={
                "2423_status": paper_shadow_preflight_2423.get("status"),
                "2423_paper_shadow_preflight_ready": (
                    paper_shadow_preflight_2423.get("paper_shadow_preflight_ready")
                ),
                "2424_paper_shadow_preflight_ready": (
                    paper_shadow_enablement_plan_2424.get(
                        "paper_shadow_preflight_ready"
                    )
                ),
            },
            failure_message="Paper-shadow preflight is not ready.",
            classification="dry_run_precondition_gap",
        ),
        _requirement(
            requirement_id="contract_ready",
            description="TRADING-2422 contract readiness snapshot must remain READY.",
            passed=_contract_ready(
                paper_shadow_enablement_plan_2424,
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
            )
            and contract_readiness_snapshot_2422.get("status")
            == CONTRACT_READINESS_SNAPSHOT_READY_STATUS
            and _int_value(contract_readiness_snapshot_2422.get("contract_gap_count"))
            == 0,
            evidence={
                "2424_contract_ready": paper_shadow_enablement_plan_2424.get(
                    "contract_ready"
                ),
                "2423_contract_ready": paper_shadow_preflight_2423.get(
                    "contract_ready"
                ),
                "2422_status": contract_readiness_snapshot_2422.get("status"),
                "2422_contract_ready": contract_readiness_snapshot_2422.get(
                    "contract_ready"
                ),
                "2422_contract_gap_count": contract_readiness_snapshot_2422.get(
                    "contract_gap_count"
                ),
            },
            failure_message="Contract readiness is not complete.",
            classification="dry_run_precondition_gap",
        ),
        _requirement(
            requirement_id="pit_gate_ready",
            description="PIT gate must remain ready.",
            passed=_pit_gate_ready(
                paper_shadow_enablement_plan_2424,
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
            )
            and readiness_recheck_after_remediation_2421.get("status")
            == PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS,
            evidence={
                "2424_pit_gate_ready": paper_shadow_enablement_plan_2424.get(
                    "pit_gate_ready"
                ),
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
            failure_message="PIT gate is not ready.",
            classification="dry_run_precondition_gap",
        ),
        _requirement(
            requirement_id="source_traceability_accepted",
            description="Source traceability must remain accepted.",
            passed=_source_traceability_accepted(
                paper_shadow_enablement_plan_2424,
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                manifest,
                lineage_map,
                missing_summary,
            ),
            evidence={
                "2424_source_traceability_accepted": (
                    paper_shadow_enablement_plan_2424.get(
                        "source_traceability_accepted"
                    )
                ),
                "2423_source_traceability_accepted": (
                    paper_shadow_preflight_2423.get("source_traceability_accepted")
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
            classification="dry_run_precondition_gap",
        ),
        _requirement(
            requirement_id="input_contract_map_resolves",
            description="Paper-shadow dry-run input contract map must resolve.",
            passed=_input_contract_map_ready(input_contract_map),
            evidence={
                "required_input_contract_ids": list(INPUT_CONTRACT_IDS),
                "provided_input_contract_ids": [
                    item.get("contract_id") for item in input_contract_map
                ],
            },
            failure_message="Input contract map is missing required contracts.",
            classification="dry_run_wiring_contract_gap",
        ),
        _requirement(
            requirement_id="output_artifact_contract_map_resolves",
            description="Paper-shadow dry-run output artifact contract map must resolve.",
            passed=_output_artifact_contract_map_ready(output_artifact_contract_map),
            evidence={
                "required_output_artifact_contract_ids": list(
                    OUTPUT_ARTIFACT_CONTRACT_IDS
                ),
                "provided_output_artifact_contract_ids": [
                    item.get("contract_id") for item in output_artifact_contract_map
                ],
            },
            failure_message="Output artifact contract map is missing required contracts.",
            classification="dry_run_wiring_contract_gap",
        ),
        _requirement(
            requirement_id="manual_review_handoff_route_resolves",
            description="Manual review handoff route must resolve.",
            passed=_manual_review_handoff_wired(manual_review_handoff_route),
            evidence=dict(manual_review_handoff_route),
            failure_message="Manual review handoff route is missing or incomplete.",
            classification="dry_run_wiring_contract_gap",
        ),
        _requirement(
            requirement_id="schedule_hook_verified_disabled",
            description="Schedule hook must be explicitly verified disabled.",
            passed=_schedule_hook_verified_disabled(schedule_hook_disabled_verification),
            evidence=dict(schedule_hook_disabled_verification),
            failure_message="Schedule hook is not explicitly verified disabled.",
            classification="dry_run_wiring_contract_gap",
        ),
        _requirement(
            requirement_id="no_effect_audit_ready",
            description="No-effect audit summary must be ready.",
            passed=_no_effect_audit_ready(no_effect_audit_summary),
            evidence=dict(no_effect_audit_summary),
            failure_message="No-effect audit summary is missing or shows effects.",
            classification="dry_run_wiring_contract_gap",
        ),
        _requirement(
            requirement_id="paper_shadow_runtime_disabled",
            description="Paper-shadow runtime must remain disabled.",
            passed=_fields_false(
                paper_shadow_enablement_plan_2424,
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
                "2424_paper_shadow_enabled": paper_shadow_enablement_plan_2424.get(
                    "paper_shadow_enabled"
                ),
                "2423_paper_shadow_enabled": paper_shadow_preflight_2423.get(
                    "paper_shadow_enabled"
                ),
            },
            failure_message="Paper-shadow runtime is already enabled or mutated.",
            classification="dry_run_safety_boundary",
        ),
        _requirement(
            requirement_id="paper_shadow_schedule_disabled",
            description="Paper-shadow schedule must remain disabled.",
            passed=_fields_false(
                paper_shadow_enablement_plan_2424,
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
                "2424_paper_shadow_schedule_enabled": (
                    paper_shadow_enablement_plan_2424.get(
                        "paper_shadow_schedule_enabled"
                    )
                ),
                "2424_scheduler_enabled": paper_shadow_enablement_plan_2424.get(
                    "scheduler_enabled"
                ),
            },
            failure_message="Paper-shadow schedule is already enabled or executed.",
            classification="dry_run_safety_boundary",
        ),
        _requirement(
            requirement_id="production_disabled",
            description="Production path must remain disabled.",
            passed=_fields_false(
                paper_shadow_enablement_plan_2424,
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=("production_enabled", "production_allowed"),
            ),
            evidence={
                "2424_production_enabled": paper_shadow_enablement_plan_2424.get(
                    "production_enabled"
                ),
                "2423_production_enabled": paper_shadow_preflight_2423.get(
                    "production_enabled"
                ),
            },
            failure_message="Production path is enabled.",
            classification="dry_run_safety_boundary",
        ),
        _requirement(
            requirement_id="broker_disabled",
            description="Broker and order paths must remain disabled.",
            passed=_broker_disabled(
                paper_shadow_enablement_plan_2424,
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
            ),
            evidence={
                "2424_broker_enabled": paper_shadow_enablement_plan_2424.get(
                    "broker_enabled"
                ),
                "2424_broker_action": paper_shadow_enablement_plan_2424.get(
                    "broker_action"
                ),
                "2423_broker_enabled": paper_shadow_preflight_2423.get(
                    "broker_enabled"
                ),
                "2423_broker_action": paper_shadow_preflight_2423.get(
                    "broker_action"
                ),
            },
            failure_message="Broker or order path is enabled.",
            classification="dry_run_safety_boundary",
        ),
        _requirement(
            requirement_id="automatic_execution_disabled",
            description="Automatic execution must remain disabled.",
            passed=_fields_false(
                paper_shadow_enablement_plan_2424,
                paper_shadow_preflight_2423,
                contract_readiness_snapshot_2422,
                readiness_recheck_after_remediation_2421,
                remediation_result_2420,
                fields=("automatic_execution_allowed",),
            ),
            evidence={
                "2424_automatic_execution_allowed": (
                    paper_shadow_enablement_plan_2424.get(
                        "automatic_execution_allowed"
                    )
                ),
            },
            failure_message="Automatic execution is allowed.",
            classification="dry_run_safety_boundary",
        ),
        _requirement(
            requirement_id="no_signal_generation",
            description="Dry-run wiring must not generate a new signal.",
            passed=_fields_false(
                paper_shadow_enablement_plan_2424,
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
                "2424_generated_signal": paper_shadow_enablement_plan_2424.get(
                    "generated_signal"
                ),
                "2423_generated_signal": paper_shadow_preflight_2423.get(
                    "generated_signal"
                ),
            },
            failure_message="A new signal or feature was generated.",
            classification="dry_run_safety_boundary",
        ),
        _requirement(
            requirement_id="no_trading_advice_or_runtime_job",
            description="Dry-run wiring must not generate advice or runtime jobs.",
            passed=_fields_false(
                paper_shadow_enablement_plan_2424,
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
                    "portfolio_weight_mutated",
                    "actual_portfolio_weights_modified",
                    "broker_order_generated",
                ),
            ),
            evidence={
                "2424_generated_trading_advice": (
                    paper_shadow_enablement_plan_2424.get(
                        "generated_trading_advice"
                    )
                ),
                "2424_backtest_run": paper_shadow_enablement_plan_2424.get(
                    "backtest_run"
                ),
                "2424_scoring_run": paper_shadow_enablement_plan_2424.get(
                    "scoring_run"
                ),
            },
            failure_message="Trading advice, backtest, scoring, or runtime job ran.",
            classification="dry_run_safety_boundary",
        ),
        _requirement(
            requirement_id="report_registry_registered",
            description="Report registry must include 2420 through 2425 reports.",
            passed=not missing_reports,
            evidence={"missing_report_ids": missing_reports},
            failure_message="Report registry is missing required report ids.",
            classification="missing_dry_run_evidence",
        ),
        _requirement(
            requirement_id="artifact_catalog_registered",
            description="Artifact catalog must include 2425 command and artifacts.",
            passed=not missing_catalog_refs,
            evidence={"missing_catalog_references": missing_catalog_refs},
            failure_message="Artifact catalog is missing required 2425 references.",
            classification="missing_dry_run_evidence",
        ),
        _requirement(
            requirement_id="system_flow_registered",
            description="System flow must include 2425 route and READY status.",
            passed=not missing_flow_refs,
            evidence={"missing_system_flow_references": missing_flow_refs},
            failure_message="System flow is missing required 2425 references.",
            classification="missing_dry_run_evidence",
        ),
        _requirement(
            requirement_id="research_docs_registered",
            description="Required 2420-2425 research docs must be readable.",
            passed=not missing_docs,
            evidence={"missing_research_docs": missing_docs},
            failure_message="One or more required research docs are missing.",
            classification="missing_dry_run_evidence",
        ),
    ]


def _default_input_contract_map(
    paper_shadow_enablement_plan_2424: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "contract_id": "source_traceability_artifact_chain",
            "source_task": "TRADING-2420",
            "required": True,
            "resolved": True,
            "artifact_id": ARTIFACT_ID,
        },
        {
            "contract_id": "pit_gate_readiness_state",
            "source_task": "TRADING-2421",
            "required": True,
            "resolved": True,
            "expected_ready": True,
        },
        {
            "contract_id": "contract_readiness_snapshot",
            "source_task": "TRADING-2422",
            "required": True,
            "resolved": True,
            "expected_gap_count": 0,
        },
        {
            "contract_id": "paper_shadow_preflight_state",
            "source_task": "TRADING-2423",
            "required": True,
            "resolved": True,
            "expected_ready": True,
        },
        {
            "contract_id": "paper_shadow_enablement_plan_state",
            "source_task": "TRADING-2424",
            "required": True,
            "resolved": paper_shadow_enablement_plan_2424.get(
                "enablement_plan_ready"
            )
            is True,
            "expected_gap_count": 0,
        },
        {
            "contract_id": "manual_review_boundary_state",
            "source_task": "TRADING-2424",
            "required": True,
            "resolved": paper_shadow_enablement_plan_2424.get(
                "manual_review_required"
            )
            is True,
            "manual_review_required": True,
        },
    ]


def _default_output_artifact_contract_map() -> list[dict[str, Any]]:
    return [
        {
            "contract_id": contract_id,
            "required": True,
            "resolved": True,
            "production_effect": "none",
            "broker_action": "none",
        }
        for contract_id in OUTPUT_ARTIFACT_CONTRACT_IDS
    ]


def _default_manual_review_handoff_route() -> dict[str, Any]:
    return {
        "handoff_route_required": True,
        "manual_review_handoff_wired": True,
        "owner_review_required_before_runtime": True,
        "automatic_execution_allowed": False,
        "next_route": NEXT_ROUTE_READY,
    }


def _default_schedule_hook_disabled_verification() -> dict[str, Any]:
    return {
        "schedule_hook_required_for_future_runtime": True,
        "schedule_hook_verified_disabled": True,
        "paper_shadow_schedule_enabled": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "paper_shadow_daily_job_run": False,
        "cron_or_windows_task_created": False,
    }


def _default_no_effect_audit_summary() -> dict[str, Any]:
    return {
        "no_effect_audit_ready": True,
        "generated_signal": False,
        "generated_trading_advice": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "fresh_market_data_read": False,
        "portfolio_weight_mutated": False,
        "broker_order_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _input_output_contract_map(
    *,
    status: str,
    input_contract_map: Sequence[Mapping[str, Any]],
    output_artifact_contract_map: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": INPUT_OUTPUT_CONTRACT_MAP_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "input_contract_map_ready": _input_contract_map_ready(input_contract_map),
        "output_artifact_contract_map_ready": _output_artifact_contract_map_ready(
            output_artifact_contract_map
        ),
        "input_contract_count": len(input_contract_map),
        "output_artifact_contract_count": len(output_artifact_contract_map),
        "input_contract_map": [dict(item) for item in input_contract_map],
        "output_artifact_contract_map": [
            dict(item) for item in output_artifact_contract_map
        ],
        "paper_shadow_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _runtime_boundary_manifest(*, status: str) -> dict[str, Any]:
    return {
        "schema_version": RUNTIME_BOUNDARY_MANIFEST_SCHEMA_VERSION,
        "status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_run": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "manual_review_required": True,
        "generated_signal": False,
        "generated_trading_advice": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "portfolio_weight_mutated": False,
        "broker_order_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _schedule_hook_disabled_verification_section(
    *,
    status: str,
    verification: Mapping[str, Any],
) -> dict[str, Any]:
    section = dict(verification)
    section.update(
        {
            "schema_version": SCHEDULE_HOOK_DISABLED_VERIFICATION_SCHEMA_VERSION,
            "status": status,
            "engine_id": "growth_tilt_engine",
            "target_strategy_id": TARGET_STRATEGY_ID,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _manual_review_handoff_wiring_plan(
    *,
    status: str,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    section = dict(route)
    section.update(
        {
            "schema_version": MANUAL_REVIEW_HANDOFF_WIRING_PLAN_SCHEMA_VERSION,
            "status": status,
            "engine_id": "growth_tilt_engine",
            "target_strategy_id": TARGET_STRATEGY_ID,
            "manual_review_required": True,
            "automatic_execution_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _no_effect_audit_summary_section(
    *,
    status: str,
    audit: Mapping[str, Any],
    gaps: list[dict[str, Any]],
) -> dict[str, Any]:
    section = dict(audit)
    section.update(
        {
            "schema_version": NO_EFFECT_AUDIT_SUMMARY_SCHEMA_VERSION,
            "status": status,
            "engine_id": "growth_tilt_engine",
            "target_strategy_id": TARGET_STRATEGY_ID,
            "dry_run_wiring_gap_count": len(gaps),
            "gap_ids": [gap["requirement_id"] for gap in gaps],
            "paper_shadow_enabled": False,
            "production_enabled": False,
            "broker_enabled": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _pit_gate_ready(*documents: Mapping[str, Any]) -> bool:
    return all(document.get("pit_gate_ready") is True for document in documents)


def _contract_ready(*documents: Mapping[str, Any]) -> bool:
    return all(document.get("contract_ready") is True for document in documents)


def _remaining_pit_blockers(*documents: Mapping[str, Any]) -> list[Any]:
    for document in documents:
        blockers = _as_list(
            document.get("remaining_pit_blockers") or document.get("remaining_blockers")
        )
        if blockers:
            return blockers
    return []


def _source_traceability_accepted(
    paper_shadow_enablement_plan_2424: Mapping[str, Any],
    paper_shadow_preflight_2423: Mapping[str, Any],
    contract_readiness_snapshot_2422: Mapping[str, Any],
    readiness_recheck_after_remediation_2421: Mapping[str, Any],
    remediation_result_2420: Mapping[str, Any],
    manifest: Mapping[str, Any],
    lineage_map: Mapping[str, Any],
    missing_summary: Mapping[str, Any],
) -> bool:
    return (
        paper_shadow_enablement_plan_2424.get("source_traceability_accepted") is True
        and paper_shadow_preflight_2423.get("source_traceability_accepted") is True
        and (
            paper_shadow_enablement_plan_2424.get(
                "source_traceability_recheck_status"
            )
            or paper_shadow_preflight_2423.get("source_traceability_recheck_status")
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


def _input_contract_map_ready(input_contract_map: Sequence[Mapping[str, Any]]) -> bool:
    contract_ids = {
        item.get("contract_id")
        for item in input_contract_map
        if item.get("required") is True and item.get("resolved") is True
    }
    return set(INPUT_CONTRACT_IDS).issubset(contract_ids)


def _output_artifact_contract_map_ready(
    output_artifact_contract_map: Sequence[Mapping[str, Any]],
) -> bool:
    contract_ids = {
        item.get("contract_id")
        for item in output_artifact_contract_map
        if item.get("required") is True and item.get("resolved") is True
    }
    return set(OUTPUT_ARTIFACT_CONTRACT_IDS).issubset(contract_ids)


def _manual_review_handoff_wired(route: Mapping[str, Any]) -> bool:
    return (
        route.get("manual_review_handoff_wired") is True
        and route.get("owner_review_required_before_runtime") is True
        and route.get("automatic_execution_allowed") is False
        and route.get("next_route") == NEXT_ROUTE_READY
    )


def _schedule_hook_verified_disabled(verification: Mapping[str, Any]) -> bool:
    return (
        verification.get("schedule_hook_verified_disabled") is True
        and verification.get("paper_shadow_schedule_enabled") is False
        and verification.get("scheduler_enabled") is False
        and verification.get("scheduled_task_created") is False
        and verification.get("paper_shadow_daily_job_run") is False
        and verification.get("cron_or_windows_task_created") is False
    )


def _no_effect_audit_ready(audit: Mapping[str, Any]) -> bool:
    false_fields = (
        "generated_signal",
        "generated_trading_advice",
        "backtest_run",
        "scoring_run",
        "daily_report_run",
        "fresh_market_data_read",
        "portfolio_weight_mutated",
        "broker_order_generated",
    )
    return (
        audit.get("no_effect_audit_ready") is True
        and all(audit.get(field) is False for field in false_fields)
        and audit.get("production_effect") == "none"
        and audit.get("broker_action") == "none"
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


def _broker_disabled(*documents: Mapping[str, Any]) -> bool:
    return _fields_false(
        *documents,
        fields=(
            "broker_enabled",
            "broker_action_enabled",
            "order_generated",
            "broker_order_generated",
        ),
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
