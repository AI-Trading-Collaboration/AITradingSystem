from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
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
    DEFAULT_OWNER_DECISION_ROOT,
    DEFAULT_REPLAY_VALIDATION_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    OWNER_DECISION,
    load_high_intensity_outcome_binding_contract_plan_inputs,
)
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_OUTCOME_BINDING_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    GUARDRAIL_SUMMARY as EXPECTED_2359_GUARDRAIL_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    NEXT_2360_ROUTE as EXPECTED_2359_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    READINESS_STATUS as EXPECTED_2359_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    SIDE_EFFECT_SUMMARY as EXPECTED_2359_SIDE_EFFECT_SUMMARY,
)
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    SOURCE_TASKS as SOURCE_2359_TASKS,
)
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    STATUS as EXPECTED_2359_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_outcome_binding_contract_plan import (
    TASK_REGISTER_ID as SOURCE_2359_TASK_REGISTER_ID,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_common import (
    collect_real_scheduler_creation_fields,
    collect_unsafe_fields,
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

TASK_ID = "TRADING-2360"
TASK_REGISTER_ID = (
    "TRADING-2360_OBSERVE_ONLY_PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_PLAN"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_paper_shadow_scope_plan"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_paper_shadow_scope_plan"

STATUS = (
    "OBSERVE_ONLY_PAPER_SHADOW_SCOPE_AND_NO_BROKER_GUARDRAIL_PLAN_READY_WITH_"
    "CAVEATS_PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2361_WITH_CAVEATS"
NEXT_2361_ROUTE = (
    "TRADING-2361_Observe_Only_Production_And_Broker_Hard_Blocker_Plan"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE

SOURCE_TASKS = [
    *SOURCE_2359_TASKS,
    "TRADING-2359",
]

GUARDRAIL_SUMMARY: dict[str, Any] = {
    "scheduler_enabled": False,
    "manual_run_only": True,
    "dry_run_only": True,
    "manual_run_executed": False,
    "event_append_enabled": False,
    "event_append_attempted": False,
    "historical_event_log_mutated": False,
    "outcome_binding_enabled": False,
    "outcome_binding_attempted": False,
    "outcome_store_mutated": False,
    "paper_shadow_enabled": False,
    "paper_shadow_attempted": False,
    "production_enabled": False,
    "broker_action_enabled": False,
    "broker_action_attempted": False,
}

SIDE_EFFECT_SUMMARY: dict[str, bool] = {
    "real_scheduler_created": False,
    "cron_created": False,
    "windows_task_created": False,
    "github_actions_schedule_created": False,
    "manual_run_executed": False,
    "event_append_attempted": False,
    "historical_event_log_mutated": False,
    "outcome_binding_attempted": False,
    "outcome_store_mutated": False,
    "paper_shadow_attempted": False,
    "paper_trade_created": False,
    "paper_order_created": False,
    "shadow_position_created": False,
    "production_attempted": False,
    "broker_action_attempted": False,
    "broker_api_called": False,
    "order_sent": False,
    "capital_at_risk": False,
}

PAPER_SHADOW_SCOPE_DEFINITION: dict[str, Any] = {
    "scope_mode": "DISABLED_SCOPE_PLAN_ONLY",
    "paper_shadow_enabled": False,
    "allowed_future_observations_if_approved": [
        "advisory_preview",
        "dry_run_weight_delta",
        "dry_run_risk_cap_state",
        "dry_run_constraint_state",
        "dry_run_no_trade_reason",
        "dry_run_review_summary",
    ],
    "forbidden_without_owner_approval": [
        "paper_position_creation",
        "paper_trade_creation",
        "paper_order_creation",
        "paper_pnl_tracking_as_live_decision",
        "production_signal_export",
        "broker_api_call",
    ],
    "must_remain_disconnected_from_broker": True,
}

NO_BROKER_GUARDRAIL: dict[str, Any] = {
    "required": True,
    "broker_action_enabled": False,
    "broker_action_attempted": False,
    "must_block_broker_api_import": True,
    "must_block_order_creation": True,
    "must_block_order_preview_to_broker": True,
    "must_block_account_query_for_execution": True,
    "must_block_position_sync_for_execution": True,
    "must_block_any_capital_at_risk": True,
    "owner_approval_required_to_change": True,
}

PAPER_SHADOW_DAILY_REVIEW_PLAN: dict[str, Any] = {
    "required_before_any_future_paper_shadow": True,
    "review_frequency": "DAILY_IF_FUTURE_APPROVED",
    "required_review_fields": [
        "as_of",
        "source_task",
        "source_artifact",
        "dry_run_status",
        "risk_cap_state",
        "constraint_state",
        "advisory_preview",
        "no_trade_reason",
        "paper_shadow_enabled",
        "broker_action_enabled",
        "promotion_decision",
        "owner_review_required",
    ],
    "side_effect_allowed": False,
}

PAPER_SHADOW_OWNER_APPROVAL_REQUIREMENT: dict[str, Any] = {
    "owner_approval_required_before_paper_shadow": True,
    "owner_approval_required_before_any_broker_path": True,
    "manual_review_required": True,
    "dry_run_evidence_required": True,
    "no_broker_guardrail_evidence_required": True,
    "production_remains_blocked": True,
}

REQUIRED_FUTURE_EVIDENCE = [
    "paper_shadow_scope_owner_approval_record",
    "no_broker_guardrail_static_scan",
    "paper_shadow_daily_review_template",
    "paper_shadow_no_trade_dry_run_evidence",
    "production_and_broker_hard_blocker_plan",
]

EXPLICIT_NON_GOALS = [
    "This task does not enable paper-shadow.",
    "This task does not create paper trades.",
    "This task does not create shadow positions.",
    "This task does not enable production.",
    "This task does not call broker APIs.",
    "This task does not create orders.",
    "This task does not append events.",
    "This task does not bind outcomes.",
    "This task does not enable scheduler.",
    "This task does not create automated cadence.",
    (
        "This task only defines paper-shadow scope and no-broker guardrail "
        "requirements."
    ),
]

BLOCKED_PROMOTION_REASONS = [
    "OWNER_DECISION_KEEP_DISABLED_AND_PROMOTION_BLOCKED",
    "PAPER_SHADOW_SCOPE_PLAN_IS_REQUIREMENT_ONLY",
    "PAPER_SHADOW_ENABLEMENT_NOT_ALLOWED",
    "NO_BROKER_GUARDRAIL_IS_PLAN_ONLY",
    "PRODUCTION_AND_BROKER_HARD_BLOCKER_PLAN_REQUIRED_NEXT",
    "OWNER_APPROVAL_NOT_GRANTED",
]

ROUTE_CAVEATS = [
    "OWNER_DECISION_KEEP_DISABLED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "MANUAL_REVIEW_REQUIRED",
    "SCHEDULER_DISABLED",
    "EVENT_APPEND_DISABLED",
    "OUTCOME_BINDING_DISABLED",
    "PAPER_SHADOW_DISABLED",
    "PAPER_SHADOW_SCOPE_PLAN_ONLY",
    "NO_PAPER_TRADE_CREATION",
    "NO_SHADOW_POSITION_CREATION",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "NO_CAPITAL_AT_RISK",
    "PRODUCTION_AND_BROKER_HARD_BLOCKER_PLAN_REQUIRED_NEXT",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "paper_shadow_scope_plan_only": True,
    "owner_decision": OWNER_DECISION,
    "prior_validated_artifacts_only": True,
    "source_artifacts_only": True,
    "scheduler_enabled": False,
    "scheduler_default_enabled": False,
    "automated_cadence_enabled": False,
    "automated_cadence_created": False,
    "manual_run_only": True,
    "dry_run_only": True,
    "manual_run_executed": False,
    "real_replay_validation_executed": False,
    "event_append_enabled": False,
    "event_append_attempted": False,
    "event_append_executed": False,
    "historical_event_log_mutated": False,
    "event_log_mutated": False,
    "event_row_written": False,
    "event_row_mutated": False,
    "outcome_binding_enabled": False,
    "outcome_binding_attempted": False,
    "outcome_binding_executed": False,
    "outcome_store_mutated": False,
    "outcome_store_write": False,
    "outcome_row_written": False,
    "outcome_row_mutated": False,
    "outcome_reference_updated": False,
    "binding_reference_written": False,
    "event_outcome_link_mutated": False,
    "paper_shadow_enabled": False,
    "paper_shadow_attempted": False,
    "paper_shadow_allowed": False,
    "paper_trade_created": False,
    "paper_order_created": False,
    "paper_order_preview_created": False,
    "shadow_position_created": False,
    "paper_position_created": False,
    "paper_pnl_tracking_enabled": False,
    "production_enabled": False,
    "production_attempted": False,
    "production_allowed": False,
    "production_signal_exported": False,
    "broker_action_enabled": False,
    "broker_action_attempted": False,
    "broker_action_allowed": False,
    "broker_api_called": False,
    "broker_api_imported": False,
    "broker_order_generated": False,
    "broker_order_previewed": False,
    "execution_account_queried": False,
    "execution_position_synced": False,
    "capital_at_risk": False,
    "order_created": False,
    "order_sent": False,
    "send_order": False,
    "broker_action": "none",
    "promotion_allowed": False,
    "promotion_decision": "BLOCKED",
    "manual_review_required": True,
    "owner_review_required": True,
    "real_scheduler_created": False,
    "cron_created": False,
    "cron_entry_created": False,
    "windows_task_created": False,
    "github_actions_schedule_created": False,
    "github_action_schedule_created": False,
    "fresh_market_data_read": False,
    "new_signal_generated": False,
    "backtest_run": False,
    "daily_report_generated": False,
    "portfolio_effect": "none",
    "production_effect": "none",
    "manual_review_only": True,
}

FALSE_SAFETY_FIELDS = {
    "scheduler_enabled",
    "scheduler_default_enabled",
    "automated_cadence_enabled",
    "automated_cadence_created",
    "manual_run_executed",
    "real_replay_validation_executed",
    "event_append_enabled",
    "event_append_attempted",
    "event_append_executed",
    "historical_event_log_mutated",
    "event_log_mutated",
    "event_row_written",
    "event_row_mutated",
    "outcome_binding_enabled",
    "outcome_binding_attempted",
    "outcome_binding_executed",
    "outcome_store_mutated",
    "outcome_store_write",
    "outcome_row_written",
    "outcome_row_mutated",
    "outcome_reference_updated",
    "binding_reference_written",
    "event_outcome_link_mutated",
    "paper_shadow_enabled",
    "paper_shadow_attempted",
    "paper_shadow_allowed",
    "paper_trade_created",
    "paper_order_created",
    "paper_order_preview_created",
    "shadow_position_created",
    "paper_position_created",
    "paper_pnl_tracking_enabled",
    "production_enabled",
    "production_attempted",
    "production_allowed",
    "production_signal_exported",
    "broker_action_enabled",
    "broker_action_attempted",
    "broker_action_allowed",
    "broker_api_called",
    "broker_api_imported",
    "broker_order_generated",
    "broker_order_previewed",
    "execution_account_queried",
    "execution_position_synced",
    "capital_at_risk",
    "order_created",
    "order_sent",
    "send_order",
    "promotion_allowed",
    "real_scheduler_created",
    "cron_created",
    "cron_entry_created",
    "windows_task_created",
    "github_actions_schedule_created",
    "github_action_schedule_created",
    "fresh_market_data_read",
    "new_signal_generated",
    "backtest_run",
    "daily_report_generated",
    "target_weight_action_allowed",
    "target_weight_generated",
    "target_weight_action_generated",
    "rebalance_instruction_allowed",
    "rebalance_instruction_generated",
    "broker_action_requested",
    "paper_shadow_order_generated",
    "production_decision_generated",
    "paper_shadow_ready",
    "production_ready",
    "broker_ready",
    "side_effect_allowed",
    "promotion_allowed_after_task",
}

FORBIDDEN_EMIT_FIELDS = {
    "target_weight",
    "target_weight_action",
    "rebalance_instruction",
    "reduce_position_instruction",
    "increase_cash_instruction",
    "buy_signal",
    "sell_signal",
    "automatic_exposure_cap",
    "paper_trade",
    "paper_order",
    "shadow_position",
    "broker_order",
    "broker_order_preview",
}

REAL_SCHEDULER_FIELDS = {
    "external_scheduler_entry_created",
    "real_scheduler_created",
    "cron_created",
    "cron_entry_created",
    "windows_task_created",
    "github_actions_schedule_created",
    "github_action_schedule_created",
    "daily_scheduler_entry_created",
}

FORBIDDEN_TRUE_FIELDS = {
    "event_log_mutated",
    "historical_event_log_mutated",
    "event_row_written",
    "event_row_mutated",
    "outcome_binding_enabled",
    "outcome_binding_attempted",
    "outcome_binding_executed",
    "outcome_store_mutated",
    "outcome_store_write",
    "outcome_row_written",
    "outcome_row_mutated",
    "outcome_reference_updated",
    "binding_reference_written",
    "event_outcome_link_mutated",
    "paper_shadow_enabled",
    "paper_shadow_attempted",
    "paper_shadow_allowed",
    "paper_trade_created",
    "paper_order_created",
    "paper_order_preview_created",
    "shadow_position_created",
    "paper_position_created",
    "production_enabled",
    "production_attempted",
    "production_signal_exported",
    "broker_action_enabled",
    "broker_action_attempted",
    "broker_api_called",
    "broker_api_imported",
    "broker_order_generated",
    "broker_order_previewed",
    "execution_account_queried",
    "execution_position_synced",
    "capital_at_risk",
    "order_created",
    "order_sent",
    "send_order",
    "fresh_market_data_read",
    "scheduled_tasks_config_modified",
    "broker_action_taken",
    "manual_run_executed",
    "real_replay_validation_executed",
    "target_weight_generated",
    "rebalance_instruction_generated",
    "paper_shadow_order_generated",
    "production_decision_generated",
    "new_signal_generated",
    "backtest_run",
    "daily_report_generated",
}


class HighIntensityPaperShadowScopePlanError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_paper_shadow_scope_plan(
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
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityPaperShadowScopePlanError(
            f"high-intensity paper-shadow scope plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_paper_shadow_scope_plan_inputs(
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
    source_review = build_paper_shadow_scope_source_artifact_review(inputs=inputs)
    scope_definition = build_paper_shadow_scope_definition(
        generated_at=generated_at,
        source_review=source_review,
    )
    no_broker_guardrail = build_no_broker_guardrail_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    daily_review_plan = build_paper_shadow_daily_review_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    owner_approval_requirement = build_paper_shadow_owner_approval_requirement(
        generated_at=generated_at,
        source_review=source_review,
    )
    blocked_promotion_rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_paper_shadow_scope_plan_package(
        generated_at=generated_at,
        source_review=source_review,
        scope_definition=scope_definition,
        no_broker_guardrail=no_broker_guardrail,
        daily_review_plan=daily_review_plan,
        owner_approval_requirement=owner_approval_requirement,
        blocked_promotion_rationale=blocked_promotion_rationale,
    )
    route = build_high_intensity_2361_production_broker_hard_blocker_route(
        package=package
    )
    interpretation_boundary = build_paper_shadow_scope_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_paper_shadow_scope_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_paper_shadow_scope_plan_summary(
        generated_at=generated_at,
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
        source_review=source_review,
        scope_definition=scope_definition,
        no_broker_guardrail=no_broker_guardrail,
        daily_review_plan=daily_review_plan,
        owner_approval_requirement=owner_approval_requirement,
        blocked_promotion_rationale=blocked_promotion_rationale,
        package=package,
        route=route,
    )
    payloads = {
        "summary": summary,
        "package": package,
        "source_review": source_review,
        "scope_definition": scope_definition,
        "no_broker_guardrail": no_broker_guardrail,
        "daily_review_plan": daily_review_plan,
        "owner_approval_requirement": owner_approval_requirement,
        "blocked_promotion_rationale": blocked_promotion_rationale,
        "route": route,
        "interpretation_boundary": interpretation_boundary,
        "safety_boundary": safety_boundary,
    }
    _validate_generated_payloads(payloads)
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_paper_shadow_scope_plan_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        scope_definition=scope_definition,
        no_broker_guardrail=no_broker_guardrail,
        daily_review_plan=daily_review_plan,
        owner_approval_requirement=owner_approval_requirement,
        blocked_promotion_rationale=blocked_promotion_rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_paper_shadow_scope_plan_inputs(
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
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_outcome_binding_contract_plan_inputs(
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
        )
    except Exception as exc:  # noqa: BLE001
        raise HighIntensityPaperShadowScopePlanError(
            f"TRADING-2360 source chain invalid before 2359: {exc}"
        ) from exc

    paths = {
        "summary": outcome_binding_dir
        / "high_intensity_outcome_binding_contract_plan_summary.json",
        "package": outcome_binding_dir
        / "high_intensity_risk_cap_observe_only_outcome_binding_contract_plan.json",
        "source_review": outcome_binding_dir
        / "high_intensity_outcome_binding_contract_plan_source_artifact_review.json",
        "outcome_binding_contract": outcome_binding_dir
        / "high_intensity_outcome_binding_contract.json",
        "schema_contract": outcome_binding_dir
        / "high_intensity_outcome_binding_schema_contract.json",
        "idempotency_contract": outcome_binding_dir
        / "high_intensity_binding_idempotency_contract.json",
        "replay_contract": outcome_binding_dir
        / "high_intensity_binding_replay_contract.json",
        "mutation_guardrail": outcome_binding_dir
        / "high_intensity_outcome_store_mutation_guardrail.json",
        "rollback_plan": outcome_binding_dir / "high_intensity_binding_rollback_plan.json",
        "blocked_promotion_rationale": outcome_binding_dir
        / "high_intensity_outcome_binding_blocked_promotion_rationale.json",
        "route": outcome_binding_dir / "high_intensity_2360_paper_shadow_scope_route.json",
        "interpretation_boundary": outcome_binding_dir
        / "high_intensity_outcome_binding_interpretation_boundary.json",
        "safety_boundary": outcome_binding_dir
        / "high_intensity_outcome_binding_safety_boundary.json",
    }
    outcome_binding_payloads = _load_required_payloads(paths, "TRADING-2360")
    _validate_2359_source_contracts(outcome_binding_payloads)
    _validate_cross_source_contracts(source_inputs, outcome_binding_payloads)
    return {
        **source_inputs,
        "outcome_binding_plan": outcome_binding_payloads,
        "outcome_binding_plan_paths": _string_paths(paths),
    }


def build_paper_shadow_scope_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    outcome_binding = mapping(inputs["outcome_binding_plan"])
    summary = mapping(outcome_binding["summary"])
    source_review = mapping(outcome_binding["source_review"])
    inherited_rows = list(source_review.get("source_task_evidence", []))
    source_task_evidence = [
        *inherited_rows,
        {
            "task": "TRADING-2359",
            "status": summary.get("status"),
            "evidence": "outcome binding contract plan present",
            "evidence_present": True,
            "promotion_result": "blocked",
        },
    ]
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_paper_shadow_scope_"
                "plan.source_artifact_review.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": [
                *summary.get("source_task_ids", []),
                SOURCE_2359_TASK_REGISTER_ID,
            ],
            "source_task_evidence": source_task_evidence,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_contract_status": "PASS",
            "outcome_binding_artifacts_read": True,
            "outcome_binding_artifact_count": len(mapping(outcome_binding)),
            "outcome_binding_status": summary.get("status"),
            "outcome_binding_readiness": summary.get("readiness"),
            "outcome_binding_next_route": summary.get("next_route"),
            "outcome_binding_contract_ready": summary.get(
                "outcome_binding_contract_ready"
            ),
            "outcome_binding_schema_contract_ready": summary.get(
                "outcome_binding_schema_contract_ready"
            ),
            "binding_idempotency_contract_ready": summary.get(
                "binding_idempotency_contract_ready"
            ),
            "binding_replay_contract_ready": summary.get(
                "binding_replay_contract_ready"
            ),
            "outcome_store_mutation_guardrail_ready": summary.get(
                "outcome_store_mutation_guardrail_ready"
            ),
            "binding_rollback_plan_ready": summary.get(
                "binding_rollback_plan_ready"
            ),
            "event_append_status": source_review.get("event_append_status"),
            "event_append_readiness": source_review.get("event_append_readiness"),
            "idempotency_replay_status": source_review.get(
                "idempotency_replay_status"
            ),
            "idempotency_replay_readiness": source_review.get(
                "idempotency_replay_readiness"
            ),
            "kill_switch_status": source_review.get("kill_switch_status"),
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


def build_paper_shadow_scope_definition(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _plan_payload(
        schema_name="paper_shadow_scope_definition",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="paper_shadow_scope_plan_ready",
        payload_key="paper_shadow_scope_definition",
        payload=PAPER_SHADOW_SCOPE_DEFINITION,
    )


def build_no_broker_guardrail_plan(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _plan_payload(
        schema_name="no_broker_guardrail_plan",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="no_broker_guardrail_plan_ready",
        payload_key="no_broker_guardrail",
        payload=NO_BROKER_GUARDRAIL,
    )


def build_paper_shadow_daily_review_plan(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _plan_payload(
        schema_name="paper_shadow_daily_review_plan",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="paper_shadow_daily_review_plan_ready",
        payload_key="paper_shadow_daily_review_plan",
        payload=PAPER_SHADOW_DAILY_REVIEW_PLAN,
    )


def build_paper_shadow_owner_approval_requirement(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return _plan_payload(
        schema_name="paper_shadow_owner_approval_requirement",
        generated_at=generated_at,
        source_review=source_review,
        ready_key="paper_shadow_owner_approval_requirement_ready",
        payload_key="paper_shadow_owner_approval_requirement",
        payload=PAPER_SHADOW_OWNER_APPROVAL_REQUIREMENT,
    )


def _plan_payload(
    *,
    schema_name: str,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    ready_key: str,
    payload_key: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_paper_shadow_scope_"
                f"plan.{schema_name}.v1"
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
                "high_intensity_risk_cap_observe_only_paper_shadow_scope_"
                "plan.blocked_promotion_rationale.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "blocked_promotion_reasons": BLOCKED_PROMOTION_REASONS,
            "required_before_any_paper_shadow": REQUIRED_FUTURE_EVIDENCE,
            **SAFETY_FIELDS,
        }
    )


def build_paper_shadow_scope_plan_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    scope_definition: Mapping[str, Any],
    no_broker_guardrail: Mapping[str, Any],
    daily_review_plan: Mapping[str, Any],
    owner_approval_requirement: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_paper_shadow_scope_"
                "plan.package.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Paper-Shadow Scope "
                "And No-Broker Guardrail Plan"
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
            "paper_shadow_scope_plan_ready": scope_definition.get(
                "paper_shadow_scope_plan_ready"
            ),
            "no_broker_guardrail_plan_ready": no_broker_guardrail.get(
                "no_broker_guardrail_plan_ready"
            ),
            "paper_shadow_daily_review_plan_ready": daily_review_plan.get(
                "paper_shadow_daily_review_plan_ready"
            ),
            "paper_shadow_owner_approval_requirement_ready": (
                owner_approval_requirement.get(
                    "paper_shadow_owner_approval_requirement_ready"
                )
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "paper_shadow_scope_definition": scope_definition.get(
                "paper_shadow_scope_definition"
            ),
            "no_broker_guardrail": no_broker_guardrail.get("no_broker_guardrail"),
            "paper_shadow_daily_review_plan": daily_review_plan.get(
                "paper_shadow_daily_review_plan"
            ),
            "paper_shadow_owner_approval_requirement": (
                owner_approval_requirement.get(
                    "paper_shadow_owner_approval_requirement"
                )
            ),
            "required_future_evidence": REQUIRED_FUTURE_EVIDENCE,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
            "explicit_non_goals": EXPLICIT_NON_GOALS,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2361_ROUTE,
            "next_task": NEXT_2361_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2361_production_broker_hard_blocker_route(
    *,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_paper_shadow_scope_"
                "plan.2361_route.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": package.get("readiness"),
            "next_route": NEXT_2361_ROUTE,
            "next_task": NEXT_2361_ROUTE,
            "route_caveats": ROUTE_CAVEATS,
            "route_blockers": [],
            "route_rationale": (
                "paper-shadow scope and no-broker guardrail plan can enter "
                "production and broker hard-blocker planning while paper-shadow, "
                "production, and broker paths remain disabled"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_paper_shadow_scope_interpretation_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_paper_shadow_scope_"
                "plan.interpretation_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "interpretation": (
                "TRADING-2360 defines paper-shadow scope and no-broker guardrail "
                "requirements only."
            ),
            "not_paper_shadow_enablement": True,
            "not_paper_trade_creation": True,
            "not_shadow_position_creation": True,
            "not_production_readiness": True,
            "not_broker_readiness": True,
            "not_broker_api_call": True,
            "not_event_append_implementation": True,
            "not_outcome_binding_implementation": True,
            "not_scheduler_enablement": True,
            "not_manual_run_execution": True,
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_paper_shadow_scope_safety_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_paper_shadow_scope_"
                "plan.safety_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "must_not": [
                "enable_scheduler",
                "create_cron_job",
                "create_windows_task",
                "create_github_actions_schedule",
                "execute_real_manual_run",
                "append_historical_event_log",
                "mutate_event_log",
                "bind_outcome",
                "mutate_outcome_store",
                "enable_paper_shadow",
                "create_paper_trade",
                "create_shadow_position",
                "enable_production",
                "call_broker_api",
                "send_order",
                "read_fresh_market_data",
                "generate_new_signal",
                "run_backtest",
                "generate_daily_report",
            ],
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_paper_shadow_scope_plan_summary(
    *,
    generated_at: datetime,
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
    source_review: Mapping[str, Any],
    scope_definition: Mapping[str, Any],
    no_broker_guardrail: Mapping[str, Any],
    daily_review_plan: Mapping[str, Any],
    owner_approval_requirement: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    package: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_paper_shadow_scope_"
                "plan.summary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": package.get("title"),
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "disabled_wiring_dir": str(disabled_wiring_dir),
            "smoke_dry_run_dir": str(smoke_dry_run_dir),
            "manual_review_gate_dir": str(manual_review_gate_dir),
            "manual_run_dry_run_dir": str(manual_run_dry_run_dir),
            "replay_validation_dir": str(replay_validation_dir),
            "audit_package_dir": str(audit_package_dir),
            "owner_decision_dir": str(owner_decision_dir),
            "gap_closure_dir": str(gap_closure_dir),
            "hardening_backlog_dir": str(hardening_backlog_dir),
            "kill_switch_dir": str(kill_switch_dir),
            "idempotency_replay_dir": str(idempotency_replay_dir),
            "event_append_dir": str(event_append_dir),
            "outcome_binding_dir": str(outcome_binding_dir),
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "paper_shadow_scope_plan_ready": scope_definition.get(
                "paper_shadow_scope_plan_ready"
            ),
            "no_broker_guardrail_plan_ready": no_broker_guardrail.get(
                "no_broker_guardrail_plan_ready"
            ),
            "paper_shadow_daily_review_plan_ready": daily_review_plan.get(
                "paper_shadow_daily_review_plan_ready"
            ),
            "paper_shadow_owner_approval_requirement_ready": (
                owner_approval_requirement.get(
                    "paper_shadow_owner_approval_requirement_ready"
                )
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": blocked_promotion_rationale.get(
                "blocked_promotion_reasons"
            ),
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
                "aits validate-data not rerun because TRADING-2360 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, TRADING-2351 replay validation, "
                "TRADING-2352 audit package, TRADING-2353 owner decision, "
                "TRADING-2354 gap closure, TRADING-2355 hardening backlog, "
                "TRADING-2356 kill-switch plan, TRADING-2357 idempotency "
                "replay contract, TRADING-2358 event append contract, and "
                "TRADING-2359 outcome binding contract artifacts; it does not "
                "consume fresh market data, append events, bind outcomes, mutate "
                "outcome store, enable paper-shadow, call broker APIs, produce "
                "technical features, score, backtest, or generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_paper_shadow_scope_plan_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    scope_definition: Mapping[str, Any],
    no_broker_guardrail: Mapping[str, Any],
    daily_review_plan: Mapping[str, Any],
    owner_approval_requirement: Mapping[str, Any],
    blocked_promotion_rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["scope_definition"], scope_definition)
    write_json(paths["no_broker_guardrail"], no_broker_guardrail)
    write_json(paths["daily_review_plan"], daily_review_plan)
    write_json(paths["owner_approval_requirement"], owner_approval_requirement)
    write_json(paths["blocked_promotion_rationale"], blocked_promotion_rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["plan_doc"], render_paper_shadow_scope_plan_doc(package))
    write_markdown(paths["route_doc"], render_2361_route_doc(route))
    return _string_paths(paths)


def render_paper_shadow_scope_plan_doc(package: Mapping[str, Any]) -> str:
    source_rows = [
        (
            f"- `{row.get('task')}`: status=`{row.get('status')}`, "
            f"evidence=`{row.get('evidence')}`"
        )
        for row in package.get("source_task_evidence", [])
    ]
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Paper-Shadow Scope Plan",
            "",
            "## Executive Summary",
            "",
            f"- status: `{package.get('status')}`",
            f"- evidence_chain_complete: `{package.get('evidence_chain_complete')}`",
            f"- owner_decision: `{package.get('owner_decision')}`",
            (
                "- paper_shadow_scope_plan_ready: "
                f"`{package.get('paper_shadow_scope_plan_ready')}`"
            ),
            (
                "- no_broker_guardrail_plan_ready: "
                f"`{package.get('no_broker_guardrail_plan_ready')}`"
            ),
            (
                "- paper_shadow_daily_review_plan_ready: "
                f"`{package.get('paper_shadow_daily_review_plan_ready')}`"
            ),
            (
                "- paper_shadow_owner_approval_requirement_ready: "
                f"`{package.get('paper_shadow_owner_approval_requirement_ready')}`"
            ),
            f"- promotion_decision: `{package.get('promotion_decision')}`",
            f"- promotion_allowed: `{package.get('promotion_allowed')}`",
            f"- readiness: `{package.get('readiness')}`",
            f"- next_route: `{package.get('next_route')}`",
            "",
            "## Source Evidence Chain",
            "",
            *source_rows,
            "",
            "## Owner Decision Recap",
            "",
            f"`{package.get('owner_decision')}`",
            "",
            "## Current Blocked Promotion Status",
            "",
            *[f"- `{reason}`" for reason in package.get("blocked_promotion_reasons", [])],
            "",
            "## Paper-Shadow Scope Definition",
            "",
            *_markdown_table_from_mapping(
                package.get("paper_shadow_scope_definition", {})
            ),
            "",
            "## No-Broker Guardrail",
            "",
            *_markdown_table_from_mapping(package.get("no_broker_guardrail", {})),
            "",
            "## Paper-Shadow Daily Review Plan",
            "",
            *_markdown_table_from_mapping(
                package.get("paper_shadow_daily_review_plan", {})
            ),
            "",
            "## Owner Approval Requirement",
            "",
            *_markdown_table_from_mapping(
                package.get("paper_shadow_owner_approval_requirement", {})
            ),
            "",
            "## Side-Effect Summary",
            "",
            *_markdown_table_from_mapping(package.get("side_effect_summary", {})),
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


def render_2361_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2361 Production Broker Hard-Blocker Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2361 route 只能进入 production and broker hard-blocker plan。",
            "它不是 paper-shadow enablement、不是 broker safety approval、",
            "不是 production readiness，也不是 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_paper_shadow_scope_plan_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_paper_shadow_scope_plan.json",
        "source_review": output_dir
        / "high_intensity_paper_shadow_scope_plan_source_artifact_review.json",
        "scope_definition": output_dir
        / "high_intensity_paper_shadow_scope_definition.json",
        "no_broker_guardrail": output_dir
        / "high_intensity_no_broker_guardrail_plan.json",
        "daily_review_plan": output_dir
        / "high_intensity_paper_shadow_daily_review_plan.json",
        "owner_approval_requirement": output_dir
        / "high_intensity_paper_shadow_owner_approval_requirement.json",
        "blocked_promotion_rationale": output_dir
        / "high_intensity_paper_shadow_blocked_promotion_rationale.json",
        "route": output_dir
        / "high_intensity_2361_production_broker_hard_blocker_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_paper_shadow_scope_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_paper_shadow_scope_safety_boundary.json",
        "plan_doc": docs_root
        / "high_intensity_risk_cap_observe_only_paper_shadow_scope_plan.md",
        "route_doc": docs_root
        / "high_intensity_2361_production_broker_hard_blocker_route.md",
    }


def _validate_2359_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    source_review = mapping(payloads["source_review"])
    outcome_contract = mapping(payloads["outcome_binding_contract"])
    schema_contract = mapping(payloads["schema_contract"])
    idempotency_contract = mapping(payloads["idempotency_contract"])
    replay_contract = mapping(payloads["replay_contract"])
    mutation_guardrail = mapping(payloads["mutation_guardrail"])
    rollback_plan = mapping(payloads["rollback_plan"])
    rationale = mapping(payloads["blocked_promotion_rationale"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2359_STATUS:
        raise HighIntensityPaperShadowScopePlanError(
            f"TRADING-2360 requires 2359 status {EXPECTED_2359_STATUS}"
        )
    if package.get("status") != EXPECTED_2359_STATUS:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 package status"
        )
    if summary.get("readiness") != EXPECTED_2359_READINESS:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 readiness READY_FOR_2360_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2359_NEXT_ROUTE:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 summary route to paper-shadow scope plan"
        )
    if route.get("next_route") != EXPECTED_2359_NEXT_ROUTE:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 route to paper-shadow scope plan"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 source contract status PASS"
        )
    if summary.get("source_tasks") != SOURCE_2359_TASKS:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 source tasks to be 2347..2358"
        )
    if package.get("source_tasks") != SOURCE_2359_TASKS:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 package source tasks to be 2347..2358"
        )
    if summary.get("evidence_chain_complete") is not True:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 evidence_chain_complete=true"
        )
    if summary.get("owner_decision") != OWNER_DECISION:
        raise HighIntensityPaperShadowScopePlanError(
            f"TRADING-2360 requires owner_decision={OWNER_DECISION}"
        )
    ready_fields = {
        "outcome_binding_contract_ready": summary.get(
            "outcome_binding_contract_ready"
        ),
        "outcome_binding_schema_contract_ready": summary.get(
            "outcome_binding_schema_contract_ready"
        ),
        "binding_idempotency_contract_ready": summary.get(
            "binding_idempotency_contract_ready"
        ),
        "binding_replay_contract_ready": summary.get(
            "binding_replay_contract_ready"
        ),
        "outcome_store_mutation_guardrail_ready": summary.get(
            "outcome_store_mutation_guardrail_ready"
        ),
        "binding_rollback_plan_ready": summary.get("binding_rollback_plan_ready"),
        "outcome_contract_artifact_ready": outcome_contract.get(
            "outcome_binding_contract_ready"
        ),
        "schema_artifact_ready": schema_contract.get(
            "outcome_binding_schema_contract_ready"
        ),
        "idempotency_artifact_ready": idempotency_contract.get(
            "binding_idempotency_contract_ready"
        ),
        "replay_artifact_ready": replay_contract.get(
            "binding_replay_contract_ready"
        ),
        "mutation_artifact_ready": mutation_guardrail.get(
            "outcome_store_mutation_guardrail_ready"
        ),
        "rollback_artifact_ready": rollback_plan.get("binding_rollback_plan_ready"),
    }
    for field, value in ready_fields.items():
        if value is not True:
            raise HighIntensityPaperShadowScopePlanError(
                f"TRADING-2360 requires 2359 {field}=true"
            )
    if summary.get("promotion_decision") != "BLOCKED":
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 promotion_decision=BLOCKED"
        )
    if rationale.get("promotion_allowed") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 blocked rationale promotion_allowed=false"
        )
    if summary.get("promotion_allowed") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 promotion_allowed=false"
        )
    outcome_payload = mapping(package.get("outcome_binding_contract"))
    if outcome_payload.get("outcome_binding_allowed") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 outcome_binding_allowed=false"
        )
    if outcome_payload.get("paper_shadow_allowed") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 paper_shadow_allowed=false"
        )
    if outcome_payload.get("side_effect_allowed") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 side_effect_allowed=false"
        )
    idempotency = mapping(package.get("binding_idempotency_contract"))
    if "stable_semantic_hash" not in idempotency.get("idempotency_key_fields", []):
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 binding idempotency stable_semantic_hash"
        )
    replay = mapping(package.get("binding_replay_contract"))
    if replay.get("side_effect_allowed") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 binding replay side_effect_allowed=false"
        )
    if replay.get("must_not_bind_outcome") is not True:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 replay to block outcome binding"
        )
    if replay.get("must_not_enter_paper_shadow") is not True:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 replay to block paper-shadow"
        )
    if mapping(package.get("outcome_store_mutation_guardrail")).get(
        "outcome_binding_enabled"
    ) is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 outcome_binding_enabled=false"
        )
    if mapping(package.get("binding_rollback_plan")).get("rollback_strategy") != (
        "PLAN_ONLY_NO_MUTATION"
    ):
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 rollback plan-only/no-mutation"
        )
    _validate_source_data_quality(summary, "TRADING-2359 summary")
    _validate_expected_summary(
        mapping(summary.get("guardrail_summary")),
        EXPECTED_2359_GUARDRAIL_SUMMARY,
        "TRADING-2359 guardrail",
    )
    _validate_expected_summary(
        mapping(summary.get("side_effect_summary")),
        EXPECTED_2359_SIDE_EFFECT_SUMMARY,
        "TRADING-2359 side effect",
    )


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    outcome_binding_payloads: Mapping[str, Any],
) -> None:
    event_append_summary = mapping(mapping(source_inputs["event_append_plan"])["summary"])
    outcome_binding_source_review = mapping(outcome_binding_payloads["source_review"])
    if outcome_binding_source_review.get("event_append_status") != (
        event_append_summary.get("status")
    ):
        raise HighIntensityPaperShadowScopePlanError(
            "TRADING-2360 requires 2359 source review event append status to match 2358"
        )


def _validate_expected_summary(
    payload: Mapping[str, Any],
    expected: Mapping[str, Any],
    label: str,
) -> None:
    for field, expected_value in expected.items():
        if payload.get(field) is not expected_value:
            raise HighIntensityPaperShadowScopePlanError(
                f"TRADING-2360 requires {label} {field}={expected_value}"
            )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2360 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    _validate_paper_shadow_contracts(mapping(payloads["package"]))


def _validate_paper_shadow_contracts(package: Mapping[str, Any]) -> None:
    scope = mapping(package.get("paper_shadow_scope_definition"))
    no_broker = mapping(package.get("no_broker_guardrail"))
    daily_review = mapping(package.get("paper_shadow_daily_review_plan"))
    owner_approval = mapping(package.get("paper_shadow_owner_approval_requirement"))
    if scope.get("scope_mode") != "DISABLED_SCOPE_PLAN_ONLY":
        raise HighIntensityPaperShadowScopePlanError(
            "paper-shadow scope must be disabled scope plan only"
        )
    if scope.get("paper_shadow_enabled") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "paper-shadow scope must keep paper_shadow_enabled=false"
        )
    if scope.get("must_remain_disconnected_from_broker") is not True:
        raise HighIntensityPaperShadowScopePlanError(
            "paper-shadow scope must remain disconnected from broker"
        )
    for field in (
        "must_block_broker_api_import",
        "must_block_order_creation",
        "must_block_order_preview_to_broker",
        "must_block_account_query_for_execution",
        "must_block_position_sync_for_execution",
        "must_block_any_capital_at_risk",
        "owner_approval_required_to_change",
    ):
        if no_broker.get(field) is not True:
            raise HighIntensityPaperShadowScopePlanError(
                f"no-broker guardrail requires {field}=true"
            )
    if no_broker.get("broker_action_enabled") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "no-broker guardrail requires broker_action_enabled=false"
        )
    if no_broker.get("broker_action_attempted") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "no-broker guardrail requires broker_action_attempted=false"
        )
    if daily_review.get("side_effect_allowed") is not False:
        raise HighIntensityPaperShadowScopePlanError(
            "paper-shadow daily review plan must not allow side effects"
        )
    required_review_fields = daily_review.get("required_review_fields", [])
    for field in ("paper_shadow_enabled", "broker_action_enabled"):
        if field not in required_review_fields:
            raise HighIntensityPaperShadowScopePlanError(
                f"daily review plan missing {field}"
            )
    for field in (
        "owner_approval_required_before_paper_shadow",
        "owner_approval_required_before_any_broker_path",
        "manual_review_required",
        "dry_run_evidence_required",
        "no_broker_guardrail_evidence_required",
        "production_remains_blocked",
    ):
        if owner_approval.get(field) is not True:
            raise HighIntensityPaperShadowScopePlanError(
                f"owner approval requirement needs {field}=true"
            )
    if package.get("next_route") != NEXT_2361_ROUTE:
        raise HighIntensityPaperShadowScopePlanError(
            "paper-shadow scope package must route to TRADING-2361"
        )


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensityPaperShadowScopePlanError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensityPaperShadowScopePlanError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityPaperShadowScopePlanError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensityPaperShadowScopePlanError(
            f"{path}: expected JSON object"
        )
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = collect_unsafe_fields(
        payload,
        false_fields=FALSE_SAFETY_FIELDS,
        forbidden_emit_fields=FORBIDDEN_EMIT_FIELDS,
    )
    if violations:
        raise HighIntensityPaperShadowScopePlanError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _validate_no_real_scheduler_creation(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = collect_real_scheduler_creation_fields(
        payload,
        real_scheduler_fields=REAL_SCHEDULER_FIELDS,
    )
    if violations:
        raise HighIntensityPaperShadowScopePlanError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensityPaperShadowScopePlanError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensityPaperShadowScopePlanError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensityPaperShadowScopePlanError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensityPaperShadowScopePlanError(
            f"{label} requires broker_action=none"
        )


def _collect_forbidden_true_fields(value: object, prefix: str = "") -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in FORBIDDEN_TRUE_FIELDS and item is True:
                violations.append(path)
            violations.extend(_collect_forbidden_true_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(_collect_forbidden_true_fields(item, f"{prefix}[{index}]"))
    return violations


def _markdown_table_from_mapping(payload: object) -> list[str]:
    values = mapping(payload)
    lines = ["|Field|Value|", "|---|---|"]
    for key, value in values.items():
        lines.append(f"|`{key}`|`{value}`|")
    return lines


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}
