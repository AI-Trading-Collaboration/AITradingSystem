from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_observe_only_signal_artifact_boundary.v1"
SIGNAL_ARTIFACT_SCHEMA_VERSION = (
    "growth_tilt_engine_observe_only_signal_artifact_schema.v1"
)
VALID_UNTIL_REQUIREMENTS_SCHEMA_VERSION = (
    "growth_tilt_engine_observe_only_signal_valid_until_requirements.v1"
)
SOURCE_TRACEABILITY_REQUIREMENTS_SCHEMA_VERSION = (
    "growth_tilt_engine_observe_only_signal_source_traceability_requirements.v1"
)
PIT_CONTRACT_MANUAL_REVIEW_REQUIREMENTS_SCHEMA_VERSION = (
    "growth_tilt_engine_observe_only_signal_pit_contract_manual_review_requirements.v1"
)
NO_TRADING_ADVICE_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_engine_observe_only_signal_no_trading_advice_boundary.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY_BLOCKED_BY_"
    "BOUNDARY_GAPS"
)
NEXT_ROUTE_READY = "TRADING-2429_Growth_Tilt_Engine_Forward_Outcome_Binding_Boundary"
NEXT_ROUTE_BLOCKED = (
    "TRADING-2429_Growth_Tilt_Engine_Observe_Only_Signal_Artifact_Boundary_"
    "Gap_Remediation"
)
PRIOR_ROUTE = "TRADING-2427_Growth_Tilt_Engine_Manual_Review_Packet_Dry_Run"
EXPECTED_PRIOR_NEXT_ROUTE = (
    "TRADING-2428_Growth_Tilt_Engine_Observe_Only_Signal_Artifact_Boundary"
)
MANUAL_REVIEW_PACKET_DRY_RUN_READY_STATUS = (
    "GROWTH_TILT_ENGINE_MANUAL_REVIEW_PACKET_DRY_RUN_READY"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
REPORT_TYPE = "growth_tilt_engine_observe_only_signal_artifact_boundary"

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_manual_review_packet_dry_run",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-observe-only-signal-artifact-boundary",
    "outputs/research_strategies/"
    "growth_tilt_engine_observe_only_signal_artifact_boundary/"
    "observe_only_signal_artifact_boundary_result.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_observe_only_signal_artifact_boundary/"
    "signal_artifact_schema.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_observe_only_signal_artifact_boundary/"
    "valid_until_requirements.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_observe_only_signal_artifact_boundary/"
    "source_traceability_requirements.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_observe_only_signal_artifact_boundary/"
    "pit_contract_manual_review_requirements.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_observe_only_signal_artifact_boundary/"
    "no_trading_advice_boundary.json",
    "docs/research/growth_tilt_engine_observe_only_signal_artifact_boundary.md",
    "docs/research/growth_tilt_engine_observe_only_signal_artifact_schema.md",
    "docs/research/"
    "growth_tilt_engine_observe_only_signal_valid_until_requirements.md",
    "docs/research/"
    "growth_tilt_engine_observe_only_signal_source_traceability_requirements.md",
    "docs/research/"
    "growth_tilt_engine_observe_only_signal_pit_contract_manual_review_requirements.md",
    "docs/research/"
    "growth_tilt_engine_observe_only_signal_no_trading_advice_boundary.md",
    "docs/research/dynamic_strategy_2429_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-observe-only-signal-artifact-boundary",
    READY_STATUS,
    NEXT_ROUTE_READY,
)
REQUIRED_SIGNAL_SCHEMA_FIELDS: tuple[str, ...] = (
    "signal_artifact_id",
    "schema_version",
    "engine_id",
    "strategy_id",
    "as_of",
    "known_at",
    "decision_at",
    "valid_until",
    "valid_until_reason",
    "source_traceability",
    "pit_gate_ready",
    "contract_ready",
    "manual_review_required",
    "observe_only",
    "generated_signal",
    "generated_trading_advice",
    "production_effect",
    "broker_action",
)


def build_growth_tilt_engine_observe_only_signal_artifact_boundary(
    manual_review_packet_dry_run_result_2427: Mapping[str, Any],
    manual_review_packet_2427: Mapping[str, Any],
    manual_review_checklist_2427: Mapping[str, Any],
    no_advice_boundary_summary_2427: Mapping[str, Any],
    reviewer_handoff_manifest_2427: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    signal_artifact_schema: Mapping[str, Any] | None = None,
    valid_until_requirements: Mapping[str, Any] | None = None,
    source_traceability_requirements: Mapping[str, Any] | None = None,
    pit_contract_manual_review_requirements: Mapping[str, Any] | None = None,
    no_trading_advice_boundary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manual_packet = _section(manual_review_packet_2427, "manual_review_packet")
    manual_checklist = _section(
        manual_review_checklist_2427,
        "manual_review_checklist",
    )
    no_advice_2427 = _section(
        no_advice_boundary_summary_2427,
        "no_advice_boundary_summary",
    )
    reviewer_handoff = _section(
        reviewer_handoff_manifest_2427,
        "reviewer_handoff_manifest",
    )
    resolved_schema = (
        _default_signal_artifact_schema()
        if signal_artifact_schema is None
        else dict(signal_artifact_schema)
    )
    resolved_valid_until = (
        _default_valid_until_requirements()
        if valid_until_requirements is None
        else dict(valid_until_requirements)
    )
    resolved_source_traceability = (
        _default_source_traceability_requirements()
        if source_traceability_requirements is None
        else dict(source_traceability_requirements)
    )
    resolved_pit_contract_manual_review = (
        _default_pit_contract_manual_review_requirements()
        if pit_contract_manual_review_requirements is None
        else dict(pit_contract_manual_review_requirements)
    )
    resolved_no_advice_boundary = (
        _default_no_trading_advice_boundary()
        if no_trading_advice_boundary is None
        else dict(no_trading_advice_boundary)
    )

    requirements = _observe_only_boundary_requirements(
        manual_review_packet_dry_run_result_2427,
        manual_packet,
        manual_checklist,
        no_advice_2427,
        reviewer_handoff,
        signal_artifact_schema=resolved_schema,
        valid_until_requirements=resolved_valid_until,
        source_traceability_requirements=resolved_source_traceability,
        pit_contract_manual_review_requirements=resolved_pit_contract_manual_review,
        no_trading_advice_boundary=resolved_no_advice_boundary,
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
        1
        for gap in gaps
        if gap["classification"] == "missing_observe_only_boundary_evidence"
    )
    safety_boundary_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "observe_only_safety_boundary"
    )
    signal_contract_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "signal_artifact_contract_gap"
    )
    precondition_gap_count = sum(
        1 for gap in gaps if gap["classification"] == "observe_only_precondition_gap"
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2428",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": ["TRADING-2427"],
        "pit_gate_ready": manual_review_packet_dry_run_result_2427.get(
            "pit_gate_ready"
        )
        is True,
        "pit_gate_ready_count": _int_value(
            manual_review_packet_dry_run_result_2427.get("pit_gate_ready_count")
        ),
        "contract_ready": manual_review_packet_dry_run_result_2427.get(
            "contract_ready"
        )
        is True,
        "contract_ready_count": _int_value(
            manual_review_packet_dry_run_result_2427.get("contract_ready_count")
        ),
        "contract_gap_count": _int_value(
            manual_review_packet_dry_run_result_2427.get("contract_gap_count")
        ),
        "manual_review_packet_dry_run_status": (
            manual_review_packet_dry_run_result_2427.get("status")
        ),
        "manual_review_packet_dry_run_ready": (
            manual_review_packet_dry_run_result_2427.get(
                "manual_review_packet_dry_run_ready"
            )
            is True
        ),
        "manual_review_packet_gap_count": _int_value(
            manual_review_packet_dry_run_result_2427.get(
                "manual_review_packet_gap_count"
            )
        ),
        "manual_review_packet_ready": _manual_review_packet_ready(manual_packet),
        "manual_review_checklist_ready": _manual_review_checklist_ready(
            manual_checklist
        ),
        "prior_no_advice_boundary_ready": _prior_no_advice_boundary_ready(
            no_advice_2427
        ),
        "reviewer_handoff_manifest_ready": _reviewer_handoff_ready(
            reviewer_handoff
        ),
        "observe_only_signal_artifact_boundary_started": True,
        "observe_only_signal_artifact_boundary_completed": True,
        "observe_only_signal_artifact_boundary_ready": ready,
        "signal_artifact_schema_ready": _signal_artifact_schema_ready(
            resolved_schema
        ),
        "valid_until_required": _valid_until_requirements_ready(
            resolved_valid_until
        ),
        "valid_until_requirements_ready": _valid_until_requirements_ready(
            resolved_valid_until
        ),
        "source_traceability_required": _source_traceability_requirements_ready(
            resolved_source_traceability
        ),
        "source_traceability_requirements_ready": (
            _source_traceability_requirements_ready(resolved_source_traceability)
        ),
        "pit_contract_manual_review_requirements_ready": (
            _pit_contract_manual_review_requirements_ready(
                resolved_pit_contract_manual_review
            )
        ),
        "no_trading_advice_boundary_ready": _no_trading_advice_boundary_ready(
            resolved_no_advice_boundary
        ),
        "observe_only_signal_artifact_boundary_gap_count": len(gaps),
        "observe_only_signal_artifact_boundary_gap_ids": [
            gap["requirement_id"] for gap in gaps
        ],
        "missing_observe_only_boundary_evidence_count": missing_evidence_count,
        "safety_boundary_gap_count": safety_boundary_gap_count,
        "signal_artifact_contract_gap_count": signal_contract_gap_count,
        "precondition_gap_count": precondition_gap_count,
        "observe_only_boundary_requirement_count": len(requirements),
        "observe_only_boundary_requirement_pass_count": len(requirements) - len(gaps),
        "observe_only_boundary_requirement_fail_count": len(gaps),
        "requirements": requirements,
        "gaps": gaps,
        "signal_artifact_schema": _signal_artifact_schema_section(
            status=status,
            schema=resolved_schema,
        ),
        "valid_until_requirements": _valid_until_requirements_section(
            status=status,
            requirements=resolved_valid_until,
        ),
        "source_traceability_requirements": (
            _source_traceability_requirements_section(
                status=status,
                requirements=resolved_source_traceability,
            )
        ),
        "pit_contract_manual_review_requirements": (
            _pit_contract_manual_review_requirements_section(
                status=status,
                requirements=resolved_pit_contract_manual_review,
            )
        ),
        "no_trading_advice_boundary": _no_trading_advice_boundary_section(
            status=status,
            boundary=resolved_no_advice_boundary,
            gaps=gaps,
        ),
        "observe_only": True,
        "manual_review_required": True,
        "manual_review_only": True,
        "signal_artifact_instance_generated": False,
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
            "Observe-only signal artifact boundary is complete; TRADING-2429 "
            "may define forward outcome binding without generating a live signal."
            if ready
            else "Observe-only signal artifact boundary has gaps; remediate "
            "schema, valid-until, traceability, or no-advice evidence first."
        ),
    }


def _observe_only_boundary_requirements(
    manual_review_packet_dry_run_result_2427: Mapping[str, Any],
    manual_packet: Mapping[str, Any],
    manual_checklist: Mapping[str, Any],
    no_advice_2427: Mapping[str, Any],
    reviewer_handoff: Mapping[str, Any],
    *,
    signal_artifact_schema: Mapping[str, Any],
    valid_until_requirements: Mapping[str, Any],
    source_traceability_requirements: Mapping[str, Any],
    pit_contract_manual_review_requirements: Mapping[str, Any],
    no_trading_advice_boundary: Mapping[str, Any],
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
            requirement_id="prior_manual_review_packet_dry_run_ready",
            description="TRADING-2427 manual review packet dry-run must be READY.",
            passed=(
                manual_review_packet_dry_run_result_2427.get("status")
                == MANUAL_REVIEW_PACKET_DRY_RUN_READY_STATUS
                and manual_review_packet_dry_run_result_2427.get(
                    "manual_review_packet_dry_run_ready"
                )
                is True
                and _int_value(
                    manual_review_packet_dry_run_result_2427.get(
                        "manual_review_packet_gap_count"
                    )
                )
                == 0
                and manual_review_packet_dry_run_result_2427.get(
                    "recommended_next_research_task"
                )
                == EXPECTED_PRIOR_NEXT_ROUTE
            ),
            evidence={
                "status": manual_review_packet_dry_run_result_2427.get("status"),
                "manual_review_packet_dry_run_ready": (
                    manual_review_packet_dry_run_result_2427.get(
                        "manual_review_packet_dry_run_ready"
                    )
                ),
                "manual_review_packet_gap_count": (
                    manual_review_packet_dry_run_result_2427.get(
                        "manual_review_packet_gap_count"
                    )
                ),
                "next_route": manual_review_packet_dry_run_result_2427.get(
                    "recommended_next_research_task"
                ),
            },
            failure_message="TRADING-2427 manual review packet dry-run is not READY.",
            classification="observe_only_precondition_gap",
        ),
        _requirement(
            requirement_id="prior_manual_review_packet_ready",
            description="TRADING-2427 manual review packet must be ready.",
            passed=_manual_review_packet_ready(manual_packet),
            evidence=dict(manual_packet),
            failure_message="Manual review packet is missing or advice-bearing.",
            classification="observe_only_precondition_gap",
        ),
        _requirement(
            requirement_id="prior_manual_review_checklist_ready",
            description="TRADING-2427 manual review checklist must be ready.",
            passed=_manual_review_checklist_ready(manual_checklist),
            evidence=dict(manual_checklist),
            failure_message="Manual review checklist is incomplete.",
            classification="observe_only_precondition_gap",
        ),
        _requirement(
            requirement_id="prior_no_advice_boundary_ready",
            description="TRADING-2427 no-advice boundary must be ready.",
            passed=_prior_no_advice_boundary_ready(no_advice_2427),
            evidence=dict(no_advice_2427),
            failure_message="Prior no-advice boundary is missing or shows effects.",
            classification="observe_only_precondition_gap",
        ),
        _requirement(
            requirement_id="prior_reviewer_handoff_ready",
            description="TRADING-2427 reviewer handoff manifest must be ready.",
            passed=_reviewer_handoff_ready(reviewer_handoff),
            evidence=dict(reviewer_handoff),
            failure_message="Reviewer handoff manifest is incomplete.",
            classification="observe_only_precondition_gap",
        ),
        _requirement(
            requirement_id="signal_artifact_schema_ready",
            description="Observe-only signal artifact schema must be complete.",
            passed=_signal_artifact_schema_ready(signal_artifact_schema),
            evidence={
                "missing_required_fields": _missing_required_schema_fields(
                    signal_artifact_schema
                ),
                "signal_artifact_schema_ready": signal_artifact_schema.get(
                    "signal_artifact_schema_ready"
                ),
                "artifact_mode": signal_artifact_schema.get("artifact_mode"),
            },
            failure_message="Observe-only signal artifact schema is incomplete.",
            classification="signal_artifact_contract_gap",
        ),
        _requirement(
            requirement_id="valid_until_requirements_ready",
            description="Signal artifact must require valid-until semantics.",
            passed=_valid_until_requirements_ready(valid_until_requirements),
            evidence=dict(valid_until_requirements),
            failure_message="Valid-until requirements are incomplete.",
            classification="signal_artifact_contract_gap",
        ),
        _requirement(
            requirement_id="source_traceability_requirements_ready",
            description="Signal artifact must require source traceability.",
            passed=_source_traceability_requirements_ready(
                source_traceability_requirements
            ),
            evidence=dict(source_traceability_requirements),
            failure_message="Source traceability requirements are incomplete.",
            classification="signal_artifact_contract_gap",
        ),
        _requirement(
            requirement_id="pit_contract_manual_review_requirements_ready",
            description="Signal artifact must carry PIT, contract, and review fields.",
            passed=_pit_contract_manual_review_requirements_ready(
                pit_contract_manual_review_requirements
            ),
            evidence=dict(pit_contract_manual_review_requirements),
            failure_message="PIT/contract/manual-review requirements are incomplete.",
            classification="signal_artifact_contract_gap",
        ),
        _requirement(
            requirement_id="no_trading_advice_boundary_ready",
            description="No-trading-advice boundary must be ready.",
            passed=_no_trading_advice_boundary_ready(no_trading_advice_boundary),
            evidence=dict(no_trading_advice_boundary),
            failure_message="No-trading-advice boundary is incomplete.",
            classification="signal_artifact_contract_gap",
        ),
        _requirement(
            requirement_id="no_signal_or_advice_generated",
            description="Boundary definition must not generate signal or advice.",
            passed=_fields_false(
                manual_review_packet_dry_run_result_2427,
                no_advice_2427,
                signal_artifact_schema,
                no_trading_advice_boundary,
                fields=(
                    "generated_signal",
                    "new_signal_generated",
                    "signal_artifact_instance_generated",
                    "generated_trading_advice",
                    "trading_advice_generated",
                    "actionable_allocation_generated",
                    "broker_order_generated",
                    "order_generated",
                    "portfolio_weight_mutated",
                    "actual_portfolio_weights_modified",
                ),
            ),
            evidence={
                "generated_signal": manual_review_packet_dry_run_result_2427.get(
                    "generated_signal"
                ),
                "generated_trading_advice": (
                    manual_review_packet_dry_run_result_2427.get(
                        "generated_trading_advice"
                    )
                ),
                "boundary_generated_signal": signal_artifact_schema.get(
                    "generated_signal"
                ),
            },
            failure_message="Signal, advice, order, or portfolio mutation was generated.",
            classification="observe_only_safety_boundary",
        ),
        _requirement(
            requirement_id="paper_shadow_disabled",
            description="Paper-shadow runtime and schedule must remain disabled.",
            passed=_fields_false(
                manual_review_packet_dry_run_result_2427,
                no_trading_advice_boundary,
                fields=(
                    "paper_shadow_enabled",
                    "paper_shadow_allowed",
                    "paper_shadow_schedule_enabled",
                    "paper_shadow_daily_job_enabled",
                    "paper_shadow_daily_job_run",
                    "paper_trade_created",
                    "shadow_position_created",
                    "scheduler_enabled",
                    "scheduled_task_created",
                    "schedule_hook_invoked",
                    "schedule_state_mutated",
                ),
            ),
            evidence={
                "paper_shadow_enabled": manual_review_packet_dry_run_result_2427.get(
                    "paper_shadow_enabled"
                ),
                "paper_shadow_schedule_enabled": (
                    manual_review_packet_dry_run_result_2427.get(
                        "paper_shadow_schedule_enabled"
                    )
                ),
            },
            failure_message="Paper-shadow runtime or schedule is enabled.",
            classification="observe_only_safety_boundary",
        ),
        _requirement(
            requirement_id="production_and_broker_disabled",
            description="Production, broker, and automatic execution must stay disabled.",
            passed=_broker_disabled(
                manual_review_packet_dry_run_result_2427,
                no_trading_advice_boundary,
            )
            and _fields_false(
                manual_review_packet_dry_run_result_2427,
                no_trading_advice_boundary,
                fields=(
                    "production_enabled",
                    "production_allowed",
                    "broker_enabled",
                    "broker_action_enabled",
                    "automatic_execution_allowed",
                ),
            ),
            evidence={
                "production_enabled": manual_review_packet_dry_run_result_2427.get(
                    "production_enabled"
                ),
                "broker_enabled": manual_review_packet_dry_run_result_2427.get(
                    "broker_enabled"
                ),
                "broker_action": manual_review_packet_dry_run_result_2427.get(
                    "broker_action"
                ),
            },
            failure_message="Production, broker, or automatic execution is enabled.",
            classification="observe_only_safety_boundary",
        ),
        _requirement(
            requirement_id="no_backtest_scoring_daily_report_or_fresh_data",
            description=(
                "Boundary definition must not run backtest/scoring/daily report "
                "or read fresh data."
            ),
            passed=_fields_false(
                manual_review_packet_dry_run_result_2427,
                no_advice_2427,
                no_trading_advice_boundary,
                fields=(
                    "backtest_run",
                    "new_strategy_backtest_run",
                    "scoring_run",
                    "daily_report_generated",
                    "daily_report_run",
                    "fresh_market_data_read",
                    "new_feature_generated",
                ),
            ),
            evidence={
                "backtest_run": manual_review_packet_dry_run_result_2427.get(
                    "backtest_run"
                ),
                "scoring_run": manual_review_packet_dry_run_result_2427.get(
                    "scoring_run"
                ),
                "daily_report_run": manual_review_packet_dry_run_result_2427.get(
                    "daily_report_run"
                ),
                "fresh_market_data_read": (
                    manual_review_packet_dry_run_result_2427.get(
                        "fresh_market_data_read"
                    )
                ),
            },
            failure_message="Backtest/scoring/daily report/fresh data path ran.",
            classification="observe_only_safety_boundary",
        ),
        _requirement(
            requirement_id="report_registry_registered",
            description="Report registry must include 2428 and required prior report ids.",
            passed=not missing_reports,
            evidence={"missing_report_ids": missing_reports},
            failure_message="Report registry missing required report ids.",
            classification="missing_observe_only_boundary_evidence",
        ),
        _requirement(
            requirement_id="artifact_catalog_registered",
            description="Artifact catalog must include observe-only boundary outputs.",
            passed=not missing_catalog_refs,
            evidence={"missing_catalog_references": missing_catalog_refs},
            failure_message="Artifact catalog missing observe-only boundary references.",
            classification="missing_observe_only_boundary_evidence",
        ),
        _requirement(
            requirement_id="system_flow_registered",
            description="System flow must include observe-only boundary command and route.",
            passed=not missing_flow_refs,
            evidence={"missing_system_flow_references": missing_flow_refs},
            failure_message="System flow missing observe-only boundary references.",
            classification="missing_observe_only_boundary_evidence",
        ),
        _requirement(
            requirement_id="research_docs_registered",
            description="Required prior research docs must be readable.",
            passed=not missing_docs,
            evidence={"missing_research_doc_ids": missing_docs},
            failure_message="Required research docs are missing.",
            classification="missing_observe_only_boundary_evidence",
        ),
    ]


def _default_signal_artifact_schema() -> dict[str, Any]:
    return {
        "signal_artifact_schema_ready": True,
        "artifact_mode": "observe_only_boundary",
        "observe_only": True,
        "generated_signal": False,
        "signal_artifact_instance_generated": False,
        "required_fields": list(REQUIRED_SIGNAL_SCHEMA_FIELDS),
        "field_contract": {
            "as_of": "decision date for a future observe-only artifact",
            "known_at": "timestamp at which every input is known",
            "decision_at": "timestamp at which observe-only state is evaluated",
            "valid_until": "exclusive signal validity boundary",
            "source_traceability": "source artifact lineage and checksum evidence",
            "manual_review_required": "true for every observe-only signal artifact",
        },
        "forbidden_fields": (
            "target_weight",
            "rebalance_instruction",
            "order_instruction",
            "broker_order",
            "production_action",
        ),
        "next_route": NEXT_ROUTE_READY,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_valid_until_requirements() -> dict[str, Any]:
    return {
        "valid_until_requirements_ready": True,
        "valid_until_required": True,
        "valid_until_timezone": "UTC",
        "known_at_required": True,
        "decision_at_required": True,
        "stale_signal_policy_required": True,
        "valid_until_must_be_after_as_of": True,
        "valid_until_must_not_be_inferred_from_future_outcome": True,
        "expired_signal_action": "observe_only_expired_no_trade",
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_source_traceability_requirements() -> dict[str, Any]:
    return {
        "source_traceability_requirements_ready": True,
        "source_traceability_required": True,
        "required_source_fields": [
            "source_feature_id",
            "source_report_id",
            "source_artifact_path",
            "source_artifact_checksum",
            "source_as_of",
            "source_known_at",
            "source_valid_until",
        ],
        "missing_source_evidence_policy": "fail_closed",
        "upstream_artifact_closure_required": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_pit_contract_manual_review_requirements() -> dict[str, Any]:
    return {
        "pit_contract_manual_review_requirements_ready": True,
        "pit_fields_required": True,
        "contract_fields_required": True,
        "manual_review_required": True,
        "manual_review_only": True,
        "required_fields": [
            "pit_gate_ready",
            "pit_gate_ready_count",
            "contract_ready",
            "contract_ready_count",
            "contract_gap_count",
            "manual_review_required",
            "reviewer_handoff_manifest",
            "no_trading_advice_boundary",
        ],
        "manual_review_failure_policy": "fail_closed",
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_no_trading_advice_boundary() -> dict[str, Any]:
    return {
        "no_trading_advice_boundary_ready": True,
        "observe_only": True,
        "generated_signal": False,
        "new_signal_generated": False,
        "signal_artifact_instance_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "allocation_change_generated": False,
        "recommendation_generated": False,
        "broker_order_generated": False,
        "order_generated": False,
        "portfolio_weight_mutated": False,
        "actual_portfolio_weights_modified": False,
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


def _signal_artifact_schema_section(
    *,
    status: str,
    schema: Mapping[str, Any],
) -> dict[str, Any]:
    section = dict(schema)
    section.update(
        {
            "schema_version": SIGNAL_ARTIFACT_SCHEMA_VERSION,
            "status": status,
            "signal_artifact_schema_ready": _signal_artifact_schema_ready(schema),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _valid_until_requirements_section(
    *,
    status: str,
    requirements: Mapping[str, Any],
) -> dict[str, Any]:
    section = dict(requirements)
    section.update(
        {
            "schema_version": VALID_UNTIL_REQUIREMENTS_SCHEMA_VERSION,
            "status": status,
            "valid_until_requirements_ready": _valid_until_requirements_ready(
                requirements
            ),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _source_traceability_requirements_section(
    *,
    status: str,
    requirements: Mapping[str, Any],
) -> dict[str, Any]:
    section = dict(requirements)
    section.update(
        {
            "schema_version": SOURCE_TRACEABILITY_REQUIREMENTS_SCHEMA_VERSION,
            "status": status,
            "source_traceability_requirements_ready": (
                _source_traceability_requirements_ready(requirements)
            ),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _pit_contract_manual_review_requirements_section(
    *,
    status: str,
    requirements: Mapping[str, Any],
) -> dict[str, Any]:
    section = dict(requirements)
    section.update(
        {
            "schema_version": PIT_CONTRACT_MANUAL_REVIEW_REQUIREMENTS_SCHEMA_VERSION,
            "status": status,
            "pit_contract_manual_review_requirements_ready": (
                _pit_contract_manual_review_requirements_ready(requirements)
            ),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _no_trading_advice_boundary_section(
    *,
    status: str,
    boundary: Mapping[str, Any],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    section = dict(boundary)
    section.update(
        {
            "schema_version": NO_TRADING_ADVICE_BOUNDARY_SCHEMA_VERSION,
            "status": status,
            "observe_only_signal_artifact_boundary_gap_count": len(gaps),
            "gaps": [dict(gap) for gap in gaps],
            "no_trading_advice_boundary_ready": (
                _no_trading_advice_boundary_ready(boundary)
            ),
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return section


def _manual_review_packet_ready(packet: Mapping[str, Any]) -> bool:
    return (
        packet.get("manual_review_packet_ready") is True
        and packet.get("packet_mode") == "dry_run_no_advice"
        and packet.get("manual_review_required") is True
        and packet.get("contains_trading_advice") is False
        and packet.get("contains_actionable_allocation") is False
        and packet.get("contains_broker_order") is False
        and packet.get("contains_portfolio_mutation") is False
        and packet.get("next_route") == EXPECTED_PRIOR_NEXT_ROUTE
    )


def _manual_review_checklist_ready(checklist: Mapping[str, Any]) -> bool:
    if checklist.get("manual_review_checklist_ready") is False:
        return False
    if _int_value(checklist.get("failed_check_count")) > 0:
        return False
    checks = _as_list(checklist.get("checks"))
    if not checks:
        return checklist.get("manual_review_checklist_ready") is True
    return all(isinstance(item, Mapping) and item.get("passed") is True for item in checks)


def _prior_no_advice_boundary_ready(summary: Mapping[str, Any]) -> bool:
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
        and manifest.get("next_route") == EXPECTED_PRIOR_NEXT_ROUTE
    )


def _signal_artifact_schema_ready(schema: Mapping[str, Any]) -> bool:
    return (
        schema.get("signal_artifact_schema_ready") is True
        and schema.get("artifact_mode") == "observe_only_boundary"
        and schema.get("observe_only") is True
        and not _missing_required_schema_fields(schema)
        and _fields_false(
            schema,
            fields=("generated_signal", "signal_artifact_instance_generated"),
        )
    )


def _missing_required_schema_fields(schema: Mapping[str, Any]) -> list[str]:
    fields = {
        field for field in _as_list(schema.get("required_fields")) if isinstance(field, str)
    }
    return [field for field in REQUIRED_SIGNAL_SCHEMA_FIELDS if field not in fields]


def _valid_until_requirements_ready(requirements: Mapping[str, Any]) -> bool:
    return (
        requirements.get("valid_until_requirements_ready") is True
        and requirements.get("valid_until_required") is True
        and requirements.get("known_at_required") is True
        and requirements.get("decision_at_required") is True
        and requirements.get("stale_signal_policy_required") is True
        and requirements.get("valid_until_must_be_after_as_of") is True
        and requirements.get("valid_until_must_not_be_inferred_from_future_outcome")
        is True
    )


def _source_traceability_requirements_ready(requirements: Mapping[str, Any]) -> bool:
    required = {
        field
        for field in _as_list(requirements.get("required_source_fields"))
        if isinstance(field, str)
    }
    return (
        requirements.get("source_traceability_requirements_ready") is True
        and requirements.get("source_traceability_required") is True
        and requirements.get("missing_source_evidence_policy") == "fail_closed"
        and requirements.get("upstream_artifact_closure_required") is True
        and {
            "source_feature_id",
            "source_report_id",
            "source_artifact_path",
            "source_artifact_checksum",
            "source_as_of",
            "source_known_at",
            "source_valid_until",
        }.issubset(required)
    )


def _pit_contract_manual_review_requirements_ready(
    requirements: Mapping[str, Any],
) -> bool:
    required = {
        field for field in _as_list(requirements.get("required_fields")) if isinstance(field, str)
    }
    return (
        requirements.get("pit_contract_manual_review_requirements_ready") is True
        and requirements.get("pit_fields_required") is True
        and requirements.get("contract_fields_required") is True
        and requirements.get("manual_review_required") is True
        and requirements.get("manual_review_only") is True
        and requirements.get("manual_review_failure_policy") == "fail_closed"
        and {
            "pit_gate_ready",
            "pit_gate_ready_count",
            "contract_ready",
            "contract_ready_count",
            "contract_gap_count",
            "manual_review_required",
            "reviewer_handoff_manifest",
            "no_trading_advice_boundary",
        }.issubset(required)
    )


def _no_trading_advice_boundary_ready(boundary: Mapping[str, Any]) -> bool:
    return (
        boundary.get("no_trading_advice_boundary_ready") is True
        and boundary.get("observe_only") is True
        and _fields_false(
            boundary,
            fields=(
                "generated_signal",
                "new_signal_generated",
                "signal_artifact_instance_generated",
                "generated_trading_advice",
                "trading_advice_generated",
                "actionable_allocation_generated",
                "allocation_change_generated",
                "recommendation_generated",
                "broker_order_generated",
                "order_generated",
                "portfolio_weight_mutated",
                "actual_portfolio_weights_modified",
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
    return all(
        document.get(field) in (False, None)
        for document in documents
        for field in fields
    )


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
