from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_guardrail_closure_common import (
    MUST_NOT_ACTIONS,
    HighIntensityGuardrailClosureError,
    load_required_payloads,
    markdown_table_from_mapping,
    require_equal,
    require_false,
    require_true,
    source_evidence_rows_with_previous,
    string_paths,
    validate_generated_payloads,
    validate_safety_payload,
    validate_source_data_quality,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    DEFAULT_AUDIT_PACKAGE_ROOT,
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_DOCS_ROOT,
    DEFAULT_EVENT_APPEND_ROOT,
    DEFAULT_GAP_CLOSURE_ROOT,
    DEFAULT_HARDENING_BACKLOG_ROOT,
    DEFAULT_IDEMPOTENCY_REPLAY_ROOT,
    DEFAULT_KILL_SWITCH_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_OUTCOME_BINDING_ROOT,
    DEFAULT_OWNER_DECISION_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    OWNER_DECISION,
    load_high_intensity_paper_shadow_scope_plan_inputs,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PAPER_SHADOW_SCOPE_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    GUARDRAIL_SUMMARY as SOURCE_2360_GUARDRAIL_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    NEXT_2361_ROUTE as EXPECTED_2360_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    READINESS_STATUS as EXPECTED_2360_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    SAFETY_FIELDS as SOURCE_SAFETY_FIELDS,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    SIDE_EFFECT_SUMMARY as SOURCE_2360_SIDE_EFFECT_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    SOURCE_TASKS as SOURCE_2360_TASKS,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    STATUS as EXPECTED_2360_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    TASK_REGISTER_ID as SOURCE_2360_TASK_REGISTER_ID,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    mapping,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2361"
TASK_REGISTER_ID = (
    "TRADING-2361_OBSERVE_ONLY_PRODUCTION_AND_BROKER_HARD_BLOCKER_PLAN"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_production_broker_hard_blocker_plan"

STATUS = (
    "OBSERVE_ONLY_PRODUCTION_AND_BROKER_HARD_BLOCKER_PLAN_READY_WITH_CAVEATS_"
    "PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2362_WITH_CAVEATS"
NEXT_2362_ROUTE = (
    "TRADING-2362_Observe_Only_Consolidated_Promotion_Blocker_And_Safety_"
    "Evidence_Matrix"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
SOURCE_TASKS = [*SOURCE_2360_TASKS, "TRADING-2360"]

GUARDRAIL_SUMMARY: dict[str, Any] = {
    **SOURCE_2360_GUARDRAIL_SUMMARY,
    "production_attempted": False,
    "production_allowed": False,
    "broker_action_allowed": False,
    "capital_at_risk_allowed": False,
}

SIDE_EFFECT_SUMMARY: dict[str, bool] = {
    **SOURCE_2360_SIDE_EFFECT_SUMMARY,
    "production_attempted": False,
    "broker_action_attempted": False,
    "broker_api_called": False,
    "execution_account_queried": False,
    "order_created": False,
    "order_sent": False,
    "capital_at_risk": False,
}

SAFETY_FIELDS: dict[str, Any] = {
    **SOURCE_SAFETY_FIELDS,
    "production_broker_hard_blocker_plan_only": True,
    "paper_shadow_scope_plan_only": False,
    "production_allowed": False,
    "broker_action_allowed": False,
    "capital_at_risk_allowed": False,
    "human_confirmation_required": True,
}

PRODUCTION_HARD_BLOCKER_PLAN: dict[str, Any] = {
    "production_hard_blocker_plan_ready": True,
    "production_enabled": False,
    "production_attempted": False,
    "production_allowed": False,
    "production_currently_in_scope": False,
    "future_owner_approval_required": True,
    "future_risk_review_required": True,
    "future_rollback_plan_required": True,
    "future_capital_at_risk_limit_required": True,
    "future_operator_protocol_required": True,
}

BROKER_HARD_BLOCKER_PLAN: dict[str, Any] = {
    "broker_hard_blocker_plan_ready": True,
    "broker_action_enabled": False,
    "broker_action_attempted": False,
    "broker_action_allowed": False,
    "broker_api_safety_contract_required": True,
    "order_dry_run_validation_required": True,
    "capital_limit_required": True,
    "human_confirmation_protocol_required": True,
    "broker_api_import_allowed": False,
    "account_query_for_execution_allowed": False,
    "order_creation_allowed": False,
    "order_preview_to_broker_allowed": False,
    "order_send_allowed": False,
}

CAPITAL_AT_RISK_BLOCKER: dict[str, Any] = {
    "capital_at_risk_blocker_ready": True,
    "capital_at_risk_allowed": False,
    "capital_at_risk": False,
    "capital_limit_required_before_future_consideration": True,
    "explicit_owner_approval_required": True,
    "production_risk_review_required": True,
}

HUMAN_CONFIRMATION_REQUIREMENT: dict[str, Any] = {
    "human_confirmation_requirement_ready": True,
    "human_confirmation_required_before_broker_action": True,
    "owner_review_required": True,
    "manual_review_required": True,
    "automated_confirmation_allowed": False,
    "broker_action_without_human_confirmation_allowed": False,
}

REQUIRED_FUTURE_EVIDENCE = [
    "future_independent_owner_approval_for_production",
    "future_production_risk_review",
    "future_production_rollback_plan",
    "future_capital_at_risk_limit",
    "future_operator_protocol",
    "future_broker_api_safety_contract",
    "future_order_dry_run_validation",
    "future_human_confirmation_protocol",
]

BLOCKED_PROMOTION_REASONS = [
    "OWNER_DECISION_KEEP_DISABLED_AND_PROMOTION_BLOCKED",
    "PRODUCTION_NOT_IN_SCOPE",
    "BROKER_ACTION_NOT_IN_SCOPE",
    "CAPITAL_AT_RISK_FORBIDDEN",
    "NO_FUTURE_OWNER_APPROVAL_FOR_PRODUCTION",
    "NO_BROKER_API_SAFETY_CONTRACT",
    "NO_HUMAN_CONFIRMATION_PROTOCOL",
    "CONSOLIDATED_PROMOTION_BLOCKER_MATRIX_REQUIRED_NEXT",
]

EXPLICIT_NON_GOALS = [
    "This task does not enable production.",
    "This task does not import or call broker APIs.",
    "This task does not query an account for execution.",
    "This task does not create, preview, or send orders.",
    "This task does not permit capital at risk.",
    "This task does not enable paper-shadow.",
    "This task does not append events or bind outcomes.",
    "This task only records hard blockers for future owner review.",
]


class HighIntensityProductionBrokerHardBlockerPlanError(
    HighIntensityGuardrailClosureError
):
    pass


def run_high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    manual_review_gate_dir: Path = DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    manual_run_dry_run_dir: Path = DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    replay_validation_dir: Path = DEFAULT_REPLAY_VALIDATION_ROOT,
    audit_package_dir: Path = DEFAULT_AUDIT_PACKAGE_ROOT,
    owner_decision_dir: Path = DEFAULT_OWNER_DECISION_ROOT,
    gap_closure_dir: Path = DEFAULT_GAP_CLOSURE_ROOT,
    hardening_backlog_dir: Path = DEFAULT_HARDENING_BACKLOG_ROOT,
    kill_switch_dir: Path = DEFAULT_KILL_SWITCH_ROOT,
    idempotency_replay_dir: Path = DEFAULT_IDEMPOTENCY_REPLAY_ROOT,
    event_append_dir: Path = DEFAULT_EVENT_APPEND_ROOT,
    outcome_binding_dir: Path = DEFAULT_OUTCOME_BINDING_ROOT,
    paper_shadow_scope_dir: Path = DEFAULT_PAPER_SHADOW_SCOPE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityProductionBrokerHardBlockerPlanError(
            f"production/broker hard-blocker plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_production_broker_hard_blocker_plan_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        audit_package_dir=audit_package_dir,
        owner_decision_dir=owner_decision_dir,
        gap_closure_dir=gap_closure_dir,
        hardening_backlog_dir=hardening_backlog_dir,
        kill_switch_dir=kill_switch_dir,
        idempotency_replay_dir=idempotency_replay_dir,
        event_append_dir=event_append_dir,
        outcome_binding_dir=outcome_binding_dir,
        paper_shadow_scope_dir=paper_shadow_scope_dir,
    )
    source_review = build_production_broker_source_artifact_review(inputs=inputs)
    production_blocker = build_contract_payload(
        generated_at=generated_at,
        source_review=source_review,
        schema_name="production_hard_blocker_plan",
        ready_key="production_hard_blocker_plan_ready",
        payload_key="production_hard_blocker_plan",
        payload=PRODUCTION_HARD_BLOCKER_PLAN,
    )
    broker_blocker = build_contract_payload(
        generated_at=generated_at,
        source_review=source_review,
        schema_name="broker_hard_blocker_plan",
        ready_key="broker_hard_blocker_plan_ready",
        payload_key="broker_hard_blocker_plan",
        payload=BROKER_HARD_BLOCKER_PLAN,
    )
    capital_blocker = build_contract_payload(
        generated_at=generated_at,
        source_review=source_review,
        schema_name="capital_at_risk_blocker",
        ready_key="capital_at_risk_blocker_ready",
        payload_key="capital_at_risk_blocker",
        payload=CAPITAL_AT_RISK_BLOCKER,
    )
    confirmation = build_contract_payload(
        generated_at=generated_at,
        source_review=source_review,
        schema_name="human_confirmation_requirement",
        ready_key="human_confirmation_requirement_ready",
        payload_key="human_confirmation_requirement",
        payload=HUMAN_CONFIRMATION_REQUIREMENT,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_production_broker_hard_blocker_package(
        generated_at=generated_at,
        source_review=source_review,
        production_blocker=production_blocker,
        broker_blocker=broker_blocker,
        capital_blocker=capital_blocker,
        confirmation=confirmation,
        rationale=rationale,
    )
    route = build_2362_promotion_blocker_matrix_route(package=package)
    interpretation_boundary = build_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_safety_boundary(generated_at=generated_at, route=route)
    summary = build_summary(
        generated_at=generated_at,
        paper_shadow_scope_dir=paper_shadow_scope_dir,
        source_review=source_review,
        package=package,
        route=route,
    )
    payloads = {
        "summary": summary,
        "package": package,
        "source_review": source_review,
        "production_blocker": production_blocker,
        "broker_blocker": broker_blocker,
        "capital_blocker": capital_blocker,
        "confirmation": confirmation,
        "rationale": rationale,
        "route": route,
        "interpretation_boundary": interpretation_boundary,
        "safety_boundary": safety_boundary,
    }
    validate_generated_payloads(payloads, TASK_ID)
    validate_production_broker_contracts(package)
    paths = build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        production_blocker=production_blocker,
        broker_blocker=broker_blocker,
        capital_blocker=capital_blocker,
        confirmation=confirmation,
        rationale=rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_production_broker_hard_blocker_plan_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    audit_package_dir: Path,
    owner_decision_dir: Path,
    gap_closure_dir: Path,
    hardening_backlog_dir: Path,
    kill_switch_dir: Path,
    idempotency_replay_dir: Path,
    event_append_dir: Path,
    outcome_binding_dir: Path,
    paper_shadow_scope_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_paper_shadow_scope_plan_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
            manual_run_dry_run_dir=manual_run_dry_run_dir,
            replay_validation_dir=replay_validation_dir,
            audit_package_dir=audit_package_dir,
            owner_decision_dir=owner_decision_dir,
            gap_closure_dir=gap_closure_dir,
            hardening_backlog_dir=hardening_backlog_dir,
            kill_switch_dir=kill_switch_dir,
            idempotency_replay_dir=idempotency_replay_dir,
            event_append_dir=event_append_dir,
            outcome_binding_dir=outcome_binding_dir,
        )
    except Exception as exc:  # noqa: BLE001
        raise HighIntensityProductionBrokerHardBlockerPlanError(
            f"TRADING-2361 source chain invalid before 2360: {exc}"
        ) from exc

    paths = {
        "summary": paper_shadow_scope_dir
        / "high_intensity_paper_shadow_scope_plan_summary.json",
        "package": paper_shadow_scope_dir
        / "high_intensity_risk_cap_observe_only_paper_shadow_scope_plan.json",
        "source_review": paper_shadow_scope_dir
        / "high_intensity_paper_shadow_scope_plan_source_artifact_review.json",
        "scope_definition": paper_shadow_scope_dir
        / "high_intensity_paper_shadow_scope_definition.json",
        "no_broker_guardrail": paper_shadow_scope_dir
        / "high_intensity_no_broker_guardrail_plan.json",
        "daily_review_plan": paper_shadow_scope_dir
        / "high_intensity_paper_shadow_daily_review_plan.json",
        "owner_approval_requirement": paper_shadow_scope_dir
        / "high_intensity_paper_shadow_owner_approval_requirement.json",
        "blocked_promotion_rationale": paper_shadow_scope_dir
        / "high_intensity_paper_shadow_blocked_promotion_rationale.json",
        "route": paper_shadow_scope_dir
        / "high_intensity_2361_production_broker_hard_blocker_route.json",
        "interpretation_boundary": paper_shadow_scope_dir
        / "high_intensity_paper_shadow_scope_interpretation_boundary.json",
        "safety_boundary": paper_shadow_scope_dir
        / "high_intensity_paper_shadow_scope_safety_boundary.json",
    }
    paper_shadow_payloads = load_required_payloads(paths, "TRADING-2361")
    try:
        validate_2360_source_contracts(paper_shadow_payloads)
    except HighIntensityGuardrailClosureError as exc:
        raise HighIntensityProductionBrokerHardBlockerPlanError(str(exc)) from exc
    return {
        **source_inputs,
        "paper_shadow_scope_plan": paper_shadow_payloads,
        "paper_shadow_scope_plan_paths": string_paths(paths),
    }


def validate_2360_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    route = mapping(payloads["route"])
    source_review = mapping(payloads["source_review"])
    scope = mapping(payloads["scope_definition"])
    no_broker = mapping(payloads["no_broker_guardrail"])

    for key, payload in payloads.items():
        validate_safety_payload(f"TRADING-2361 source 2360 {key}", mapping(payload))
    require_equal(summary, "status", EXPECTED_2360_STATUS, "TRADING-2361")
    require_equal(package, "status", EXPECTED_2360_STATUS, "TRADING-2361")
    require_equal(summary, "readiness", EXPECTED_2360_READINESS, "TRADING-2361")
    require_equal(summary, "next_route", EXPECTED_2360_NEXT_ROUTE, "TRADING-2361")
    require_equal(route, "next_route", EXPECTED_2360_NEXT_ROUTE, "TRADING-2361")
    require_equal(summary, "source_tasks", SOURCE_2360_TASKS, "TRADING-2361")
    require_equal(package, "source_tasks", SOURCE_2360_TASKS, "TRADING-2361")
    require_true(summary, "evidence_chain_complete", "TRADING-2361")
    require_equal(summary, "owner_decision", OWNER_DECISION, "TRADING-2361")
    for field in (
        "paper_shadow_scope_plan_ready",
        "no_broker_guardrail_plan_ready",
        "paper_shadow_daily_review_plan_ready",
        "paper_shadow_owner_approval_requirement_ready",
    ):
        require_true(summary, field, "TRADING-2361")
    for field in (
        "promotion_allowed",
        "paper_shadow_enabled",
        "paper_shadow_attempted",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
    ):
        require_false(summary, field, "TRADING-2361")
    require_equal(
        source_review,
        "source_contract_status",
        "PASS",
        "TRADING-2361",
    )
    require_true(scope, "paper_shadow_scope_plan_ready", "TRADING-2361")
    require_true(no_broker, "no_broker_guardrail_plan_ready", "TRADING-2361")
    no_broker_payload = mapping(no_broker.get("no_broker_guardrail"))
    for field in (
        "must_block_broker_api_import",
        "must_block_order_creation",
        "must_block_order_preview_to_broker",
        "must_block_account_query_for_execution",
        "must_block_position_sync_for_execution",
        "must_block_any_capital_at_risk",
    ):
        require_true(no_broker_payload, field, "TRADING-2361")
    validate_source_data_quality(summary, "TRADING-2361 2360 summary")


def build_production_broker_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    paper_shadow = mapping(inputs["paper_shadow_scope_plan"])
    summary = mapping(paper_shadow["summary"])
    source_review = mapping(paper_shadow["source_review"])
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_production_broker_"
                "hard_blocker_plan.source_artifact_review.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": [
                *source_review.get("source_task_ids", []),
                SOURCE_2360_TASK_REGISTER_ID,
            ],
            "source_task_evidence": source_evidence_rows_with_previous(
                source_review,
                task="TRADING-2360",
                status=summary.get("status"),
                evidence="paper-shadow scope and no-broker guardrail plan present",
            ),
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_contract_status": "PASS",
            "paper_shadow_scope_status": summary.get("status"),
            "paper_shadow_scope_readiness": summary.get("readiness"),
            "paper_shadow_scope_next_route": summary.get("next_route"),
            "paper_shadow_scope_plan_ready": summary.get(
                "paper_shadow_scope_plan_ready"
            ),
            "no_broker_guardrail_plan_ready": summary.get(
                "no_broker_guardrail_plan_ready"
            ),
            "owner_decision": OWNER_DECISION,
            "evidence_chain_complete": True,
            "source_validate_data_executed": summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": summary.get("source_validate_data_as_of"),
            "source_validate_data_status": summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": summary.get(
                "source_validate_data_error_count"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_contract_payload(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    schema_name: str,
    ready_key: str,
    payload_key: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_production_broker_"
                f"hard_blocker_plan.{schema_name}.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            ready_key: True,
            payload_key: payload,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_blocked_promotion_rationale(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_production_broker_"
                "hard_blocker_plan.blocked_promotion_rationale.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "blocked_promotion_reasons": BLOCKED_PROMOTION_REASONS,
            "required_future_evidence": REQUIRED_FUTURE_EVIDENCE,
            **SAFETY_FIELDS,
        }
    )


def build_production_broker_hard_blocker_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    production_blocker: Mapping[str, Any],
    broker_blocker: Mapping[str, Any],
    capital_blocker: Mapping[str, Any],
    confirmation: Mapping[str, Any],
    rationale: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_production_broker_"
                "hard_blocker_plan.package.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Production And Broker "
                "Hard-Blocker Plan"
            ),
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_evidence": source_review.get("source_task_evidence"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "production_hard_blocker_plan_ready": production_blocker.get(
                "production_hard_blocker_plan_ready"
            ),
            "broker_hard_blocker_plan_ready": broker_blocker.get(
                "broker_hard_blocker_plan_ready"
            ),
            "capital_at_risk_blocker_ready": capital_blocker.get(
                "capital_at_risk_blocker_ready"
            ),
            "human_confirmation_requirement_ready": confirmation.get(
                "human_confirmation_requirement_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "production_enabled": False,
            "production_attempted": False,
            "broker_action_enabled": False,
            "broker_action_attempted": False,
            "capital_at_risk_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "production_hard_blocker_plan": production_blocker.get(
                "production_hard_blocker_plan"
            ),
            "broker_hard_blocker_plan": broker_blocker.get(
                "broker_hard_blocker_plan"
            ),
            "capital_at_risk_blocker": capital_blocker.get(
                "capital_at_risk_blocker"
            ),
            "human_confirmation_requirement": confirmation.get(
                "human_confirmation_requirement"
            ),
            "required_future_evidence": REQUIRED_FUTURE_EVIDENCE,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": rationale.get("blocked_promotion_reasons"),
            "explicit_non_goals": EXPLICIT_NON_GOALS,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2362_ROUTE,
            "next_task": NEXT_2362_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_2362_promotion_blocker_matrix_route(
    *,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_production_broker_"
                "hard_blocker_plan.2362_route.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": package.get("readiness"),
            "next_route": NEXT_2362_ROUTE,
            "next_task": NEXT_2362_ROUTE,
            "route_caveats": [
                "OWNER_DECISION_KEEP_DISABLED",
                "PROMOTION_BLOCKED",
                "OBSERVE_ONLY",
                "PRODUCTION_DISABLED",
                "BROKER_ACTION_DISABLED",
                "CAPITAL_AT_RISK_FORBIDDEN",
                "CONSOLIDATED_PROMOTION_BLOCKER_MATRIX_REQUIRED_NEXT",
            ],
            "route_blockers": [],
            **SAFETY_FIELDS,
        }
    )


def build_interpretation_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_production_broker_"
                "hard_blocker_plan.interpretation_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "interpretation": (
                "TRADING-2361 records production and broker hard blockers only."
            ),
            "not_production_enablement": True,
            "not_broker_approval": True,
            "not_account_access": True,
            "not_order_creation": True,
            "not_capital_at_risk": True,
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_safety_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_production_broker_"
                "hard_blocker_plan.safety_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "must_not": MUST_NOT_ACTIONS,
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_summary(
    *,
    generated_at: datetime,
    paper_shadow_scope_dir: Path,
    source_review: Mapping[str, Any],
    package: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_production_broker_"
                "hard_blocker_plan.summary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": package.get("title"),
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "paper_shadow_scope_dir": str(paper_shadow_scope_dir),
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "production_hard_blocker_plan_ready": package.get(
                "production_hard_blocker_plan_ready"
            ),
            "broker_hard_blocker_plan_ready": package.get(
                "broker_hard_blocker_plan_ready"
            ),
            "capital_at_risk_blocker_ready": package.get(
                "capital_at_risk_blocker_ready"
            ),
            "human_confirmation_requirement_ready": package.get(
                "human_confirmation_requirement_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "production_enabled": False,
            "production_attempted": False,
            "broker_action_enabled": False,
            "broker_action_attempted": False,
            "capital_at_risk_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": package.get("blocked_promotion_reasons"),
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            "source_validate_data_executed": source_review.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": source_review.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": source_review.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": source_review.get(
                "source_validate_data_error_count"
            ),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2361 only reads "
                "prior validated TRADING-2347 through TRADING-2360 research "
                "artifacts; it does not consume fresh market data, append events, "
                "bind outcomes, mutate outcome store, enable paper-shadow, enter "
                "production, call broker APIs, create orders, put capital at risk, "
                "produce technical features, score, backtest, or generate daily "
                "reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def validate_production_broker_contracts(package: Mapping[str, Any]) -> None:
    for field in (
        "production_hard_blocker_plan_ready",
        "broker_hard_blocker_plan_ready",
        "capital_at_risk_blocker_ready",
        "human_confirmation_requirement_ready",
    ):
        require_true(package, field, TASK_ID)
    for field in (
        "promotion_allowed",
        "production_enabled",
        "production_attempted",
        "broker_action_enabled",
        "broker_action_attempted",
        "capital_at_risk_allowed",
    ):
        require_false(package, field, TASK_ID)
    require_equal(package, "next_route", NEXT_2362_ROUTE, TASK_ID)


def build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_production_broker_hard_blocker_plan_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan.json",
        "source_review": output_dir
        / "high_intensity_production_broker_hard_blocker_source_artifact_review.json",
        "production_blocker": output_dir
        / "high_intensity_production_hard_blocker_plan.json",
        "broker_blocker": output_dir / "high_intensity_broker_hard_blocker_plan.json",
        "capital_blocker": output_dir / "high_intensity_capital_at_risk_blocker.json",
        "confirmation": output_dir
        / "high_intensity_human_confirmation_requirement.json",
        "rationale": output_dir
        / "high_intensity_production_broker_blocked_promotion_rationale.json",
        "route": output_dir / "high_intensity_2362_promotion_blocker_matrix_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_production_broker_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_production_broker_safety_boundary.json",
        "plan_doc": docs_root
        / "high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan.md",
        "route_doc": docs_root / "high_intensity_2362_promotion_blocker_matrix_route.md",
    }


def write_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    production_blocker: Mapping[str, Any],
    broker_blocker: Mapping[str, Any],
    capital_blocker: Mapping[str, Any],
    confirmation: Mapping[str, Any],
    rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["production_blocker"], production_blocker)
    write_json(paths["broker_blocker"], broker_blocker)
    write_json(paths["capital_blocker"], capital_blocker)
    write_json(paths["confirmation"], confirmation)
    write_json(paths["rationale"], rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["plan_doc"], render_plan_doc(package))
    write_markdown(paths["route_doc"], render_2362_route_doc(route))
    return string_paths(paths)


def render_plan_doc(package: Mapping[str, Any]) -> str:
    source_rows = [
        (
            f"- `{row.get('task')}`: status=`{row.get('status')}`, "
            f"evidence=`{row.get('evidence')}`"
        )
        for row in package.get("source_task_evidence", [])
    ]
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Production Broker Hard-Blocker Plan",
            "",
            "## Executive Summary",
            "",
            f"- status: `{package.get('status')}`",
            f"- evidence_chain_complete: `{package.get('evidence_chain_complete')}`",
            f"- owner_decision: `{package.get('owner_decision')}`",
            (
                "- production_hard_blocker_plan_ready: "
                f"`{package.get('production_hard_blocker_plan_ready')}`"
            ),
            (
                "- broker_hard_blocker_plan_ready: "
                f"`{package.get('broker_hard_blocker_plan_ready')}`"
            ),
            (
                "- capital_at_risk_blocker_ready: "
                f"`{package.get('capital_at_risk_blocker_ready')}`"
            ),
            (
                "- human_confirmation_requirement_ready: "
                f"`{package.get('human_confirmation_requirement_ready')}`"
            ),
            f"- promotion_allowed: `{package.get('promotion_allowed')}`",
            f"- readiness: `{package.get('readiness')}`",
            f"- next_route: `{package.get('next_route')}`",
            "",
            "## Source Evidence Chain",
            "",
            *source_rows,
            "",
            "## Production Hard-Blocker Plan",
            "",
            *markdown_table_from_mapping(package.get("production_hard_blocker_plan")),
            "",
            "## Broker Hard-Blocker Plan",
            "",
            *markdown_table_from_mapping(package.get("broker_hard_blocker_plan")),
            "",
            "## Capital-At-Risk Blocker",
            "",
            *markdown_table_from_mapping(package.get("capital_at_risk_blocker")),
            "",
            "## Human Confirmation Requirement",
            "",
            *markdown_table_from_mapping(
                package.get("human_confirmation_requirement")
            ),
            "",
            "## Side-Effect Summary",
            "",
            *markdown_table_from_mapping(package.get("side_effect_summary")),
            "",
            "## Explicit Non-Goals",
            "",
            *[f"- {item}" for item in package.get("explicit_non_goals", [])],
            "",
            "## Next Route",
            "",
            f"`{package.get('next_route')}`",
        ]
    )


def render_2362_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2362 Promotion Blocker Matrix Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2362 route 只能进入 consolidated promotion blocker / safety evidence matrix。",
            "它不是 production approval、不是 broker approval，也不是 capital-at-risk approval。",
        ]
    )
