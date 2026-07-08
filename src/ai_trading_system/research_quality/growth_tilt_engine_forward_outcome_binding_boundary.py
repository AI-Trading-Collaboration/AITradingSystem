from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_engine_forward_outcome_binding_boundary.v1"
OUTCOME_HORIZON_RULES_SCHEMA_VERSION = (
    "growth_tilt_engine_forward_outcome_horizon_rules.v1"
)
VALID_UNTIL_BINDING_RULES_SCHEMA_VERSION = (
    "growth_tilt_engine_forward_outcome_valid_until_binding_rules.v1"
)
OUTCOME_DECISION_RULES_SCHEMA_VERSION = (
    "growth_tilt_engine_forward_outcome_decision_rules.v1"
)
BASELINE_COMPARISON_RULES_SCHEMA_VERSION = (
    "growth_tilt_engine_forward_outcome_baseline_comparison_rules.v1"
)
OUTCOME_ARTIFACT_SCHEMA_VERSION = (
    "growth_tilt_engine_forward_outcome_artifact_schema.v1"
)
SIGNAL_TO_OUTCOME_LINKAGE_SCHEMA_VERSION = (
    "growth_tilt_engine_signal_to_outcome_linkage.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = (
    "growth_tilt_engine_forward_outcome_no_effect_boundary.v1"
)

READY_STATUS = "GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY_READY"
BLOCKED_STATUS = (
    "GROWTH_TILT_ENGINE_FORWARD_OUTCOME_BINDING_BOUNDARY_BLOCKED_BY_BOUNDARY_GAPS"
)
NEXT_ROUTE_READY = (
    "TRADING-2430_Growth_Tilt_Engine_Candidate_Promotion_Evidence_Review"
)
NEXT_ROUTE_BLOCKED = (
    "TRADING-2430_Growth_Tilt_Engine_Forward_Outcome_Binding_Boundary_"
    "Gap_Remediation"
)
PRIOR_ROUTE = "TRADING-2428_Growth_Tilt_Engine_Observe_Only_Signal_Artifact_Boundary"
EXPECTED_PRIOR_NEXT_ROUTE = (
    "TRADING-2429_Growth_Tilt_Engine_Forward_Outcome_Binding_Boundary"
)
OBSERVE_ONLY_BOUNDARY_READY_STATUS = (
    "GROWTH_TILT_ENGINE_OBSERVE_ONLY_SIGNAL_ARTIFACT_BOUNDARY_READY"
)
TARGET_STRATEGY_ID = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
REPORT_TYPE = "growth_tilt_engine_forward_outcome_binding_boundary"

OUTCOME_HORIZONS: tuple[str, ...] = ("1d", "5d", "10d", "20d")
OUTCOME_CLASSIFICATIONS: tuple[str, ...] = ("pass", "fail", "inconclusive")

REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_engine_observe_only_signal_artifact_boundary",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-engine-forward-outcome-binding-boundary",
    "outputs/research_strategies/"
    "growth_tilt_engine_forward_outcome_binding_boundary/"
    "forward_outcome_binding_boundary_result.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_forward_outcome_binding_boundary/"
    "outcome_horizon_rules.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_forward_outcome_binding_boundary/"
    "valid_until_binding_rules.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_forward_outcome_binding_boundary/"
    "outcome_decision_rules.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_forward_outcome_binding_boundary/"
    "baseline_comparison_rules.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_forward_outcome_binding_boundary/"
    "outcome_artifact_schema.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_forward_outcome_binding_boundary/"
    "signal_to_outcome_linkage.json",
    "outputs/research_strategies/"
    "growth_tilt_engine_forward_outcome_binding_boundary/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_engine_forward_outcome_binding_boundary.md",
    "docs/research/growth_tilt_engine_forward_outcome_horizon_rules.md",
    "docs/research/growth_tilt_engine_forward_outcome_valid_until_binding_rules.md",
    "docs/research/growth_tilt_engine_forward_outcome_decision_rules.md",
    "docs/research/growth_tilt_engine_forward_outcome_baseline_comparison_rules.md",
    "docs/research/growth_tilt_engine_forward_outcome_artifact_schema.md",
    "docs/research/growth_tilt_engine_signal_to_outcome_linkage.md",
    "docs/research/growth_tilt_engine_forward_outcome_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2430_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-engine-forward-outcome-binding-boundary",
    READY_STATUS,
    NEXT_ROUTE_READY,
)
REQUIRED_PRIOR_SIGNAL_FIELDS: tuple[str, ...] = (
    "signal_artifact_id",
    "schema_version",
    "engine_id",
    "strategy_id",
    "as_of",
    "known_at",
    "decision_at",
    "valid_until",
    "source_traceability",
    "pit_gate_ready",
    "contract_ready",
    "manual_review_required",
    "observe_only",
    "generated_signal",
    "generated_trading_advice",
)
REQUIRED_OUTCOME_SCHEMA_FIELDS: tuple[str, ...] = (
    "outcome_artifact_id",
    "schema_version",
    "source_signal_artifact_id",
    "source_signal_artifact_checksum",
    "engine_id",
    "strategy_id",
    "signal_as_of",
    "signal_known_at",
    "signal_decision_at",
    "signal_valid_until",
    "outcome_horizon",
    "outcome_window_start",
    "outcome_window_end",
    "outcome_bound_at",
    "outcome_status",
    "pass_fail_inconclusive",
    "baseline_id",
    "baseline_return",
    "signal_return",
    "excess_return_vs_baseline",
    "source_traceability",
    "data_quality_gate_status",
    "market_data_as_of",
    "generated_signal",
    "generated_trading_advice",
    "production_effect",
    "broker_action",
)
REQUIRED_LINKAGE_FIELDS: tuple[str, ...] = (
    "source_signal_artifact_id",
    "source_signal_artifact_checksum",
    "signal_as_of",
    "signal_known_at",
    "signal_decision_at",
    "signal_valid_until",
    "source_traceability",
)
REQUIRED_BASELINE_FIELDS: tuple[str, ...] = (
    "baseline_id",
    "baseline_return",
    "signal_return",
    "excess_return_vs_baseline",
    "comparison_policy_manifest",
)


def build_growth_tilt_engine_forward_outcome_binding_boundary(
    observe_only_boundary_result_2428: Mapping[str, Any],
    signal_artifact_schema_2428: Mapping[str, Any],
    valid_until_requirements_2428: Mapping[str, Any],
    source_traceability_requirements_2428: Mapping[str, Any],
    pit_contract_manual_review_requirements_2428: Mapping[str, Any],
    no_trading_advice_boundary_2428: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
    outcome_horizon_rules: Mapping[str, Any] | None = None,
    valid_until_binding_rules: Mapping[str, Any] | None = None,
    outcome_decision_rules: Mapping[str, Any] | None = None,
    baseline_comparison_rules: Mapping[str, Any] | None = None,
    outcome_artifact_schema: Mapping[str, Any] | None = None,
    signal_to_outcome_linkage: Mapping[str, Any] | None = None,
    no_effect_boundary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    prior_signal_schema = _section(signal_artifact_schema_2428, "signal_artifact_schema")
    prior_valid_until = _section(
        valid_until_requirements_2428,
        "valid_until_requirements",
    )
    prior_traceability = _section(
        source_traceability_requirements_2428,
        "source_traceability_requirements",
    )
    prior_pit_contract = _section(
        pit_contract_manual_review_requirements_2428,
        "pit_contract_manual_review_requirements",
    )
    prior_no_advice = _section(
        no_trading_advice_boundary_2428,
        "no_trading_advice_boundary",
    )
    resolved_horizon_rules = (
        _default_outcome_horizon_rules()
        if outcome_horizon_rules is None
        else dict(outcome_horizon_rules)
    )
    resolved_valid_until_binding = (
        _default_valid_until_binding_rules()
        if valid_until_binding_rules is None
        else dict(valid_until_binding_rules)
    )
    resolved_decision_rules = (
        _default_outcome_decision_rules()
        if outcome_decision_rules is None
        else dict(outcome_decision_rules)
    )
    resolved_baseline_rules = (
        _default_baseline_comparison_rules()
        if baseline_comparison_rules is None
        else dict(baseline_comparison_rules)
    )
    resolved_outcome_schema = (
        _default_outcome_artifact_schema()
        if outcome_artifact_schema is None
        else dict(outcome_artifact_schema)
    )
    resolved_linkage = (
        _default_signal_to_outcome_linkage()
        if signal_to_outcome_linkage is None
        else dict(signal_to_outcome_linkage)
    )
    resolved_no_effect = (
        _default_no_effect_boundary()
        if no_effect_boundary is None
        else dict(no_effect_boundary)
    )

    requirements = _binding_boundary_requirements(
        observe_only_boundary_result_2428,
        prior_signal_schema,
        prior_valid_until,
        prior_traceability,
        prior_pit_contract,
        prior_no_advice,
        outcome_horizon_rules=resolved_horizon_rules,
        valid_until_binding_rules=resolved_valid_until_binding,
        outcome_decision_rules=resolved_decision_rules,
        baseline_comparison_rules=resolved_baseline_rules,
        outcome_artifact_schema=resolved_outcome_schema,
        signal_to_outcome_linkage=resolved_linkage,
        no_effect_boundary=resolved_no_effect,
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

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2429",
        "status": status,
        "readiness_status": status,
        "engine_id": "growth_tilt_engine",
        "target_strategy_id": TARGET_STRATEGY_ID,
        "prior_route": PRIOR_ROUTE,
        "source_tasks": ["TRADING-2428"],
        "pit_gate_ready": observe_only_boundary_result_2428.get("pit_gate_ready")
        is True,
        "pit_gate_ready_count": _int_value(
            observe_only_boundary_result_2428.get("pit_gate_ready_count")
        ),
        "contract_ready": observe_only_boundary_result_2428.get("contract_ready")
        is True,
        "contract_ready_count": _int_value(
            observe_only_boundary_result_2428.get("contract_ready_count")
        ),
        "observe_only_signal_artifact_boundary_status": (
            observe_only_boundary_result_2428.get("status")
        ),
        "observe_only_signal_artifact_boundary_ready": (
            observe_only_boundary_result_2428.get(
                "observe_only_signal_artifact_boundary_ready"
            )
            is True
        ),
        "prior_signal_artifact_schema_ready": _prior_signal_schema_ready(
            prior_signal_schema
        ),
        "prior_valid_until_requirements_ready": _prior_valid_until_ready(
            prior_valid_until
        ),
        "prior_source_traceability_requirements_ready": _prior_traceability_ready(
            prior_traceability
        ),
        "prior_pit_contract_manual_review_requirements_ready": (
            _prior_pit_contract_ready(prior_pit_contract)
        ),
        "prior_no_trading_advice_boundary_ready": _prior_no_advice_ready(
            prior_no_advice
        ),
        "forward_outcome_binding_boundary_started": True,
        "forward_outcome_binding_boundary_completed": True,
        "forward_outcome_binding_boundary_ready": ready,
        "outcome_horizons": list(_horizons(resolved_horizon_rules)),
        "outcome_horizon_rules_ready": _outcome_horizon_rules_ready(
            resolved_horizon_rules
        ),
        "outcome_schema_ready": _outcome_artifact_schema_ready(
            resolved_outcome_schema
        ),
        "valid_until_binding_ready": _valid_until_binding_rules_ready(
            resolved_valid_until_binding
        ),
        "outcome_decision_rules_ready": _outcome_decision_rules_ready(
            resolved_decision_rules
        ),
        "baseline_comparison_ready": _baseline_comparison_rules_ready(
            resolved_baseline_rules
        ),
        "signal_to_outcome_linkage_ready": _signal_to_outcome_linkage_ready(
            resolved_linkage
        ),
        "no_effect_boundary_ready": _no_effect_boundary_ready(resolved_no_effect),
        "forward_outcome_binding_boundary_gap_count": len(gaps),
        "forward_outcome_binding_boundary_gap_ids": [
            gap["requirement_id"] for gap in gaps
        ],
        "missing_binding_boundary_evidence_count": _gap_count(
            gaps,
            "missing_forward_outcome_boundary_evidence",
        ),
        "safety_boundary_gap_count": _gap_count(gaps, "outcome_safety_boundary"),
        "outcome_contract_gap_count": _gap_count(gaps, "outcome_contract_gap"),
        "precondition_gap_count": _gap_count(gaps, "outcome_precondition_gap"),
        "requirements": requirements,
        "gaps": gaps,
        "outcome_horizon_rules": _status_section(resolved_horizon_rules, status),
        "valid_until_binding_rules": _status_section(
            resolved_valid_until_binding,
            status,
        ),
        "outcome_decision_rules": _status_section(resolved_decision_rules, status),
        "baseline_comparison_rules": _status_section(resolved_baseline_rules, status),
        "outcome_artifact_schema": _status_section(resolved_outcome_schema, status),
        "signal_to_outcome_linkage": _status_section(resolved_linkage, status),
        "no_effect_boundary": _status_section(
            {
                **resolved_no_effect,
                "gaps": gaps,
                "forward_outcome_binding_boundary_gap_count": len(gaps),
                "no_effect_boundary_ready": _no_effect_boundary_ready(
                    resolved_no_effect
                ),
            },
            status,
        ),
        "observe_only": True,
        "manual_review_required": True,
        "manual_review_only": True,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "recommended_next_research_task": next_route,
        "recommended_next_research_task_reason": (
            "Forward outcome binding boundary is contract-ready for future "
            "observe-only signal artifacts."
            if ready
            else "Forward outcome binding boundary has unresolved contract gaps."
        ),
    }


def _binding_boundary_requirements(
    observe_only_result: Mapping[str, Any],
    prior_signal_schema: Mapping[str, Any],
    prior_valid_until: Mapping[str, Any],
    prior_traceability: Mapping[str, Any],
    prior_pit_contract: Mapping[str, Any],
    prior_no_advice: Mapping[str, Any],
    *,
    outcome_horizon_rules: Mapping[str, Any],
    valid_until_binding_rules: Mapping[str, Any],
    outcome_decision_rules: Mapping[str, Any],
    baseline_comparison_rules: Mapping[str, Any],
    outcome_artifact_schema: Mapping[str, Any],
    signal_to_outcome_linkage: Mapping[str, Any],
    no_effect_boundary: Mapping[str, Any],
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str],
) -> list[dict[str, Any]]:
    requirements: list[dict[str, Any]] = []
    requirements.append(
        _requirement(
            "prior_observe_only_boundary_ready",
            observe_only_result.get("status") == OBSERVE_ONLY_BOUNDARY_READY_STATUS
            and observe_only_result.get("observe_only_signal_artifact_boundary_ready")
            is True
            and observe_only_result.get("recommended_next_research_task")
            == EXPECTED_PRIOR_NEXT_ROUTE,
            "outcome_precondition_gap",
            {
                "status": observe_only_result.get("status"),
                "ready": observe_only_result.get(
                    "observe_only_signal_artifact_boundary_ready"
                ),
                "next_route": observe_only_result.get(
                    "recommended_next_research_task"
                ),
            },
        )
    )
    requirements.append(
        _requirement(
            "prior_signal_artifact_schema_ready",
            _prior_signal_schema_ready(prior_signal_schema),
            "outcome_precondition_gap",
            {"required_fields": prior_signal_schema.get("required_fields")},
        )
    )
    requirements.append(
        _requirement(
            "prior_valid_until_requirements_ready",
            _prior_valid_until_ready(prior_valid_until),
            "outcome_precondition_gap",
            {
                "valid_until_required": prior_valid_until.get(
                    "valid_until_required"
                ),
                "ready": prior_valid_until.get("valid_until_requirements_ready"),
            },
        )
    )
    requirements.append(
        _requirement(
            "prior_source_traceability_requirements_ready",
            _prior_traceability_ready(prior_traceability),
            "outcome_precondition_gap",
            {
                "source_traceability_required": prior_traceability.get(
                    "source_traceability_required"
                ),
                "ready": prior_traceability.get(
                    "source_traceability_requirements_ready"
                ),
            },
        )
    )
    requirements.append(
        _requirement(
            "prior_pit_contract_manual_review_requirements_ready",
            _prior_pit_contract_ready(prior_pit_contract),
            "outcome_precondition_gap",
            {
                "ready": prior_pit_contract.get(
                    "pit_contract_manual_review_requirements_ready"
                ),
                "manual_review_required": prior_pit_contract.get(
                    "manual_review_required"
                ),
            },
        )
    )
    requirements.append(
        _requirement(
            "prior_no_trading_advice_boundary_ready",
            _prior_no_advice_ready(prior_no_advice),
            "outcome_safety_boundary",
            {
                "ready": prior_no_advice.get("no_trading_advice_boundary_ready"),
                "generated_signal": prior_no_advice.get("generated_signal"),
                "generated_trading_advice": prior_no_advice.get(
                    "generated_trading_advice"
                ),
            },
        )
    )
    requirements.append(
        _requirement(
            "outcome_horizon_rules_ready",
            _outcome_horizon_rules_ready(outcome_horizon_rules),
            "outcome_contract_gap",
            {"outcome_horizons": list(_horizons(outcome_horizon_rules))},
        )
    )
    requirements.append(
        _requirement(
            "valid_until_binding_rules_ready",
            _valid_until_binding_rules_ready(valid_until_binding_rules),
            "outcome_contract_gap",
            valid_until_binding_rules,
        )
    )
    requirements.append(
        _requirement(
            "outcome_decision_rules_ready",
            _outcome_decision_rules_ready(outcome_decision_rules),
            "outcome_contract_gap",
            outcome_decision_rules,
        )
    )
    requirements.append(
        _requirement(
            "baseline_comparison_rules_ready",
            _baseline_comparison_rules_ready(baseline_comparison_rules),
            "outcome_contract_gap",
            baseline_comparison_rules,
        )
    )
    requirements.append(
        _requirement(
            "outcome_artifact_schema_ready",
            _outcome_artifact_schema_ready(outcome_artifact_schema),
            "outcome_contract_gap",
            {"required_fields": outcome_artifact_schema.get("required_fields")},
        )
    )
    requirements.append(
        _requirement(
            "signal_to_outcome_linkage_ready",
            _signal_to_outcome_linkage_ready(signal_to_outcome_linkage),
            "outcome_contract_gap",
            {"linkage_fields": signal_to_outcome_linkage.get("linkage_fields")},
        )
    )
    requirements.append(
        _requirement(
            "no_effect_boundary_ready",
            _no_effect_boundary_ready(no_effect_boundary),
            "outcome_safety_boundary",
            no_effect_boundary,
        )
    )
    requirements.append(
        _requirement(
            "report_registry_coverage",
            _report_registry_has(report_registry, REQUIRED_REPORT_IDS),
            "missing_forward_outcome_boundary_evidence",
            {"required_report_ids": list(REQUIRED_REPORT_IDS)},
        )
    )
    requirements.append(
        _requirement(
            "artifact_catalog_coverage",
            _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES),
            "missing_forward_outcome_boundary_evidence",
            {"required_references": list(REQUIRED_CATALOG_REFERENCES)},
        )
    )
    requirements.append(
        _requirement(
            "system_flow_coverage",
            _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES),
            "missing_forward_outcome_boundary_evidence",
            {"required_references": list(REQUIRED_SYSTEM_FLOW_REFERENCES)},
        )
    )
    requirements.append(
        _requirement(
            "research_doc_coverage",
            _research_docs_cover_route(research_doc_texts),
            "missing_forward_outcome_boundary_evidence",
            {"required_route": EXPECTED_PRIOR_NEXT_ROUTE},
        )
    )
    return requirements


def _default_outcome_horizon_rules() -> dict[str, Any]:
    return {
        "schema_version": OUTCOME_HORIZON_RULES_SCHEMA_VERSION,
        "outcome_horizon_rules_ready": True,
        "outcome_horizons": list(OUTCOME_HORIZONS),
        "horizon_unit": "us_trading_day",
        "horizon_source": "TRADING-2429 owner roadmap",
        "calendar_rule": (
            "Future binding tasks must resolve horizons using a reviewed "
            "U.S. trading calendar and the cached-data quality gate."
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_valid_until_binding_rules() -> dict[str, Any]:
    return {
        "schema_version": VALID_UNTIL_BINDING_RULES_SCHEMA_VERSION,
        "valid_until_binding_ready": True,
        "bind_only_after_valid_until": True,
        "require_outcome_window_closed": True,
        "require_no_future_data_at_decision_time": True,
        "require_source_traceability_preserved": True,
        "require_data_quality_gate_for_future_binding": True,
        "backfill_real_outcome_now": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_outcome_decision_rules() -> dict[str, Any]:
    return {
        "schema_version": OUTCOME_DECISION_RULES_SCHEMA_VERSION,
        "outcome_decision_rules_ready": True,
        "allowed_classifications": list(OUTCOME_CLASSIFICATIONS),
        "pass_rule": (
            "Future bound outcome may be marked pass only when required "
            "outcome fields are complete and the reviewed comparison policy "
            "classifies the result as supporting the signal."
        ),
        "fail_rule": (
            "Future bound outcome may be marked fail only when required "
            "outcome fields are complete and the reviewed comparison policy "
            "classifies the result as not supporting the signal."
        ),
        "inconclusive_rule": (
            "Use inconclusive for missing data, not-due horizons, invalid "
            "lineage, policy absence, or mixed evidence."
        ),
        "automatic_promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_baseline_comparison_rules() -> dict[str, Any]:
    return {
        "schema_version": BASELINE_COMPARISON_RULES_SCHEMA_VERSION,
        "baseline_comparison_ready": True,
        "required_comparison_fields": list(REQUIRED_BASELINE_FIELDS),
        "comparison_policy_manifest_required": True,
        "baseline_must_be_known_at_signal_time": True,
        "no_hardcoded_return_thresholds_in_boundary": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_outcome_artifact_schema() -> dict[str, Any]:
    return {
        "schema_version": OUTCOME_ARTIFACT_SCHEMA_VERSION,
        "outcome_schema_ready": True,
        "artifact_mode": "future_observe_only_outcome_binding_boundary",
        "required_fields": list(REQUIRED_OUTCOME_SCHEMA_FIELDS),
        "allowed_outcome_horizons": list(OUTCOME_HORIZONS),
        "allowed_classifications": list(OUTCOME_CLASSIFICATIONS),
        "forbidden_fields": [
            "target_weight",
            "rebalance_instruction",
            "order_instruction",
            "broker_order",
            "production_action",
        ],
        "generated_signal": False,
        "generated_trading_advice": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_signal_to_outcome_linkage() -> dict[str, Any]:
    return {
        "schema_version": SIGNAL_TO_OUTCOME_LINKAGE_SCHEMA_VERSION,
        "signal_to_outcome_linkage_ready": True,
        "linkage_fields": list(REQUIRED_LINKAGE_FIELDS),
        "checksum_required": True,
        "source_traceability_required": True,
        "one_signal_to_many_horizons_allowed": True,
        "many_signals_to_one_outcome_forbidden": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _default_no_effect_boundary() -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "no_effect_boundary_ready": True,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _prior_signal_schema_ready(schema: Mapping[str, Any]) -> bool:
    fields = schema.get("required_fields")
    return (
        schema.get("signal_artifact_schema_ready") is True
        and _contains_all_values(fields, REQUIRED_PRIOR_SIGNAL_FIELDS)
        and schema.get("generated_signal") is False
        and schema.get("signal_artifact_instance_generated") is False
    )


def _prior_valid_until_ready(requirements: Mapping[str, Any]) -> bool:
    return (
        requirements.get("valid_until_required") is True
        and requirements.get("valid_until_requirements_ready") is True
    )


def _prior_traceability_ready(requirements: Mapping[str, Any]) -> bool:
    return (
        requirements.get("source_traceability_required") is True
        and requirements.get("source_traceability_requirements_ready") is True
    )


def _prior_pit_contract_ready(requirements: Mapping[str, Any]) -> bool:
    return (
        requirements.get("pit_contract_manual_review_requirements_ready") is True
        and requirements.get("manual_review_required") is True
        and requirements.get("pit_fields_required") is True
        and requirements.get("contract_fields_required") is True
    )


def _prior_no_advice_ready(boundary: Mapping[str, Any]) -> bool:
    return (
        boundary.get("no_trading_advice_boundary_ready") is True
        and boundary.get("generated_signal") is False
        and boundary.get("generated_trading_advice") is False
        and boundary.get("paper_shadow_enabled") is False
        and boundary.get("production_enabled") is False
        and boundary.get("broker_enabled") is False
    )


def _outcome_horizon_rules_ready(rules: Mapping[str, Any]) -> bool:
    return (
        rules.get("outcome_horizon_rules_ready") is True
        and _horizons(rules) == OUTCOME_HORIZONS
        and rules.get("horizon_unit") == "us_trading_day"
    )


def _valid_until_binding_rules_ready(rules: Mapping[str, Any]) -> bool:
    return (
        rules.get("valid_until_binding_ready") is True
        and rules.get("bind_only_after_valid_until") is True
        and rules.get("require_outcome_window_closed") is True
        and rules.get("require_no_future_data_at_decision_time") is True
        and rules.get("require_source_traceability_preserved") is True
        and rules.get("require_data_quality_gate_for_future_binding") is True
        and rules.get("backfill_real_outcome_now") is False
    )


def _outcome_decision_rules_ready(rules: Mapping[str, Any]) -> bool:
    return (
        rules.get("outcome_decision_rules_ready") is True
        and _contains_all_values(
            rules.get("allowed_classifications"),
            OUTCOME_CLASSIFICATIONS,
        )
        and bool(rules.get("pass_rule"))
        and bool(rules.get("fail_rule"))
        and bool(rules.get("inconclusive_rule"))
        and rules.get("automatic_promotion_allowed") is False
    )


def _baseline_comparison_rules_ready(rules: Mapping[str, Any]) -> bool:
    return (
        rules.get("baseline_comparison_ready") is True
        and _contains_all_values(
            rules.get("required_comparison_fields"),
            REQUIRED_BASELINE_FIELDS,
        )
        and rules.get("comparison_policy_manifest_required") is True
        and rules.get("baseline_must_be_known_at_signal_time") is True
        and rules.get("no_hardcoded_return_thresholds_in_boundary") is True
    )


def _outcome_artifact_schema_ready(schema: Mapping[str, Any]) -> bool:
    return (
        schema.get("outcome_schema_ready") is True
        and _contains_all_values(
            schema.get("required_fields"),
            REQUIRED_OUTCOME_SCHEMA_FIELDS,
        )
        and _horizons(schema, key="allowed_outcome_horizons") == OUTCOME_HORIZONS
        and _contains_all_values(
            schema.get("allowed_classifications"),
            OUTCOME_CLASSIFICATIONS,
        )
        and schema.get("generated_signal") is False
        and schema.get("generated_trading_advice") is False
    )


def _signal_to_outcome_linkage_ready(linkage: Mapping[str, Any]) -> bool:
    return (
        linkage.get("signal_to_outcome_linkage_ready") is True
        and _contains_all_values(linkage.get("linkage_fields"), REQUIRED_LINKAGE_FIELDS)
        and linkage.get("checksum_required") is True
        and linkage.get("source_traceability_required") is True
        and linkage.get("many_signals_to_one_outcome_forbidden") is True
    )


def _no_effect_boundary_ready(boundary: Mapping[str, Any]) -> bool:
    false_fields = (
        "generated_signal",
        "new_signal_generated",
        "generated_trading_advice",
        "outcome_backfilled",
        "outcome_binding_executed",
        "outcome_store_mutated",
        "fresh_market_data_read",
        "backtest_run",
        "scoring_run",
        "daily_report_run",
        "paper_shadow_enabled",
        "paper_shadow_schedule_enabled",
        "production_enabled",
        "broker_enabled",
        "broker_order_generated",
        "portfolio_weight_mutated",
        "automatic_execution_allowed",
    )
    return (
        boundary.get("no_effect_boundary_ready") is True
        and all(boundary.get(field) is False for field in false_fields)
        and boundary.get("production_effect") == "none"
        and boundary.get("broker_action") == "none"
    )


def _section(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if isinstance(value, Mapping):
        return value
    return document


def _status_section(section: Mapping[str, Any], status: str) -> dict[str, Any]:
    return {**dict(section), "status": status}


def _requirement(
    requirement_id: str,
    passed: bool,
    classification: str,
    evidence: Any,
) -> dict[str, Any]:
    return {
        "requirement_id": requirement_id,
        "status": "PASS" if passed else "FAIL",
        "classification": classification,
        "evidence": evidence,
        "production_effect": "none",
        "broker_action": "none",
    }


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requirement_id": requirement["requirement_id"],
        "classification": requirement["classification"],
        "gap": f"{requirement['requirement_id']} did not pass.",
        "evidence": requirement.get("evidence"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _gap_count(gaps: Sequence[Mapping[str, Any]], classification: str) -> int:
    return sum(1 for gap in gaps if gap.get("classification") == classification)


def _int_value(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    return 0


def _horizons(
    rules: Mapping[str, Any],
    *,
    key: str = "outcome_horizons",
) -> tuple[str, ...]:
    value = rules.get(key)
    if not isinstance(value, Sequence) or isinstance(value, str):
        return ()
    return tuple(str(item) for item in value)


def _contains_all(text: str, references: Sequence[str]) -> bool:
    return all(reference in text for reference in references)


def _contains_all_values(value: object, required: Sequence[str]) -> bool:
    if not isinstance(value, Sequence) or isinstance(value, str):
        return False
    present = {str(item) for item in value}
    return all(item in present for item in required)


def _report_registry_has(
    report_registry: Mapping[str, Any],
    report_ids: Sequence[str],
) -> bool:
    reports = report_registry.get("reports")
    if not isinstance(reports, Sequence):
        return False
    present = {
        str(report.get("report_id"))
        for report in reports
        if isinstance(report, Mapping)
    }
    return all(report_id in present for report_id in report_ids)


def _research_docs_cover_route(research_doc_texts: Mapping[str, str]) -> bool:
    joined = "\n".join(str(text) for text in research_doc_texts.values())
    return (
        EXPECTED_PRIOR_NEXT_ROUTE in joined
        and OBSERVE_ONLY_BOUNDARY_READY_STATUS in joined
    )
