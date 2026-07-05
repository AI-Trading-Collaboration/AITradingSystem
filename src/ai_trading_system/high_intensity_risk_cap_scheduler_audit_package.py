from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_scheduler_common import (
    collect_real_scheduler_creation_fields,
    collect_unsafe_fields,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    load_high_intensity_scheduler_manual_run_replay_validation_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_REPLAY_VALIDATION_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    NEXT_2352_ROUTE as EXPECTED_2351_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    READINESS_STATUS as EXPECTED_2351_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    REPLAY_COUNT as EXPECTED_2351_REPLAY_COUNT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    STATUS as EXPECTED_2351_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_replay_validation import (
    TASK_REGISTER_ID as SOURCE_2351_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2352"
TASK_REGISTER_ID = (
    "TRADING-2352_OBSERVE_ONLY_SCHEDULER_AUDIT_PACKAGE_AND_OWNER_REVIEW_CHECKLIST"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_audit_package"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_audit_package"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_AUDIT_PACKAGE_READY_FOR_OWNER_REVIEW_WITH_CAVEATS_"
    "PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2353_WITH_CAVEATS"
NEXT_2353_ROUTE = "TRADING-2353_Observe_Only_Scheduler_Owner_Review_Decision_Record"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SOURCE_TASKS = [
    "TRADING-2347",
    "TRADING-2348",
    "TRADING-2349",
    "TRADING-2350",
    "TRADING-2351",
]

PROMOTION_BLOCKED_REASONS = [
    "OWNER_REVIEW_REQUIRED",
    "OBSERVE_ONLY_MODE",
    "MANUAL_REVIEW_REQUIRED",
    "DRY_RUN_ONLY",
    "SCHEDULER_DISABLED",
    "EVENT_APPEND_DISABLED",
    "OUTCOME_BINDING_DISABLED",
    "PAPER_SHADOW_DISABLED",
    "PRODUCTION_DISABLED",
    "BROKER_ACTION_DISABLED",
]

ROUTE_CAVEATS = [
    "OWNER_REVIEW_REQUIRED",
    "MANUAL_REVIEW_REQUIRED",
    "PROMOTION_BLOCKED",
    "OBSERVE_ONLY",
    "SCHEDULER_DISABLED",
    "MANUAL_RUN_ONLY",
    "DRY_RUN_ONLY",
    "NO_REAL_SCHEDULER",
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "AUDIT_PACKAGE_ONLY",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "audit_package_only": True,
    "owner_review_package_only": True,
    "prior_validated_artifacts_only": True,
    "source_artifacts_only": True,
    "scheduler_enabled": False,
    "scheduler_default_enabled": False,
    "manual_run_only": True,
    "dry_run_only": True,
    "manual_run_executed": False,
    "event_append_enabled": False,
    "event_append_attempted": False,
    "event_append_executed": False,
    "outcome_binding_enabled": False,
    "outcome_binding_attempted": False,
    "outcome_binding_executed": False,
    "paper_shadow_enabled": False,
    "paper_shadow_attempted": False,
    "paper_shadow_allowed": False,
    "production_enabled": False,
    "production_attempted": False,
    "production_allowed": False,
    "broker_action_enabled": False,
    "broker_action_attempted": False,
    "broker_action_allowed": False,
    "broker_action": "none",
    "promotion_allowed": False,
    "promotion_decision": "BLOCKED",
    "manual_review_required": True,
    "owner_review_required": True,
    "manual_review_completed": False,
    "owner_review_completed": False,
    "real_scheduler_created": False,
    "cron_created": False,
    "cron_entry_created": False,
    "windows_task_created": False,
    "github_actions_schedule_created": False,
    "github_action_schedule_created": False,
    "event_log_mutated": False,
    "outcome_store_mutated": False,
    "fresh_market_data_read": False,
    "new_signal_generated": False,
    "backtest_run": False,
    "daily_report_generated": False,
    "portfolio_effect": "none",
    "production_effect": "none",
    "manual_review_only": True,
}

GUARDRAIL_SUMMARY: dict[str, Any] = {
    "scheduler_enabled": False,
    "manual_run_only": True,
    "dry_run_only": True,
    "manual_run_executed": False,
    "event_append_enabled": False,
    "outcome_binding_enabled": False,
    "paper_shadow_enabled": False,
    "production_enabled": False,
    "broker_action_enabled": False,
    "promotion_allowed": False,
    "manual_review_required": True,
    "owner_review_required": True,
}

SIDE_EFFECT_SUMMARY: dict[str, bool] = {
    "real_scheduler_created": False,
    "cron_created": False,
    "windows_task_created": False,
    "github_actions_schedule_created": False,
    "event_append_attempted": False,
    "outcome_binding_attempted": False,
    "paper_shadow_attempted": False,
    "production_attempted": False,
    "broker_action_attempted": False,
}

OWNER_REVIEW_CHECKLIST: dict[str, str] = {
    "review_2347_disabled_wiring": "REQUIRED",
    "review_2348_smoke_evidence": "REQUIRED",
    "review_2349_manual_gate": "REQUIRED",
    "review_2350_manual_run_dry_run": "REQUIRED",
    "review_2351_replay_validation": "REQUIRED",
    "confirm_no_scheduler_enablement": "REQUIRED",
    "confirm_no_event_outcome_mutation": "REQUIRED",
    "confirm_no_paper_shadow_or_production_path": "REQUIRED",
    "confirm_no_broker_action": "REQUIRED",
    "confirm_next_step_scope": "REQUIRED",
}

FALSE_SAFETY_FIELDS = {
    "scheduler_enabled",
    "scheduler_default_enabled",
    "manual_run_executed",
    "event_append_enabled",
    "event_append_attempted",
    "event_append_executed",
    "outcome_binding_enabled",
    "outcome_binding_attempted",
    "outcome_binding_executed",
    "paper_shadow_enabled",
    "paper_shadow_attempted",
    "paper_shadow_allowed",
    "production_enabled",
    "production_attempted",
    "production_allowed",
    "broker_action_enabled",
    "broker_action_attempted",
    "broker_action_allowed",
    "promotion_allowed",
    "real_scheduler_created",
    "cron_created",
    "cron_entry_created",
    "windows_task_created",
    "github_actions_schedule_created",
    "github_action_schedule_created",
    "event_log_mutated",
    "historical_event_log_mutated",
    "outcome_store_mutated",
    "fresh_market_data_read",
    "new_signal_generated",
    "backtest_run",
    "daily_report_generated",
    "target_weight_action_allowed",
    "target_weight_generated",
    "target_weight_action_generated",
    "rebalance_instruction_allowed",
    "rebalance_instruction_generated",
    "broker_order_generated",
    "broker_action_requested",
    "paper_shadow_order_generated",
    "production_decision_generated",
    "paper_shadow_ready",
    "production_ready",
    "broker_ready",
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
    "outcome_store_mutated",
    "fresh_market_data_read",
    "scheduled_tasks_config_modified",
    "broker_action_taken",
    "order_sent",
    "send_order",
    "manual_run_executed",
    "new_signal_generated",
    "backtest_run",
    "daily_report_generated",
}


class HighIntensitySchedulerAuditPackageError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_audit_package(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    manual_review_gate_dir: Path = DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    manual_run_dry_run_dir: Path = DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    replay_validation_dir: Path = DEFAULT_REPLAY_VALIDATION_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerAuditPackageError(
            f"high-intensity scheduler audit package only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_audit_package_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
    )
    source_review = build_audit_source_artifact_review(inputs=inputs)
    evidence_chain = build_audit_evidence_chain(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
    )
    guardrail_summary = build_audit_guardrail_summary(
        generated_at=generated_at,
        evidence_chain=evidence_chain,
    )
    side_effect_summary = build_audit_side_effect_summary(
        generated_at=generated_at,
        inputs=inputs,
        evidence_chain=evidence_chain,
    )
    owner_review_checklist = build_owner_review_checklist(
        generated_at=generated_at,
        evidence_chain=evidence_chain,
        guardrail_summary=guardrail_summary,
        side_effect_summary=side_effect_summary,
    )
    promotion_decision = build_audit_promotion_decision(
        generated_at=generated_at,
        evidence_chain=evidence_chain,
        owner_review_checklist=owner_review_checklist,
    )
    package = build_audit_package(
        generated_at=generated_at,
        source_review=source_review,
        evidence_chain=evidence_chain,
        guardrail_summary=guardrail_summary,
        side_effect_summary=side_effect_summary,
        owner_review_checklist=owner_review_checklist,
        promotion_decision=promotion_decision,
    )
    route = build_high_intensity_2353_owner_review_decision_route(
        package=package,
        evidence_chain=evidence_chain,
        promotion_decision=promotion_decision,
    )
    interpretation_boundary = build_audit_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_audit_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_audit_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        replay_validation_dir=replay_validation_dir,
        source_review=source_review,
        evidence_chain=evidence_chain,
        guardrail_summary=guardrail_summary,
        side_effect_summary=side_effect_summary,
        owner_review_checklist=owner_review_checklist,
        promotion_decision=promotion_decision,
        route=route,
    )
    _validate_generated_payloads(
        {
            "summary": summary,
            "package": package,
            "source_review": source_review,
            "evidence_chain": evidence_chain,
            "guardrail_summary": guardrail_summary,
            "side_effect_summary": side_effect_summary,
            "owner_review_checklist": owner_review_checklist,
            "promotion_decision": promotion_decision,
            "route": route,
            "interpretation_boundary": interpretation_boundary,
            "safety_boundary": safety_boundary,
        }
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_audit_package_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        evidence_chain=evidence_chain,
        guardrail_summary=guardrail_summary,
        side_effect_summary=side_effect_summary,
        owner_review_checklist=owner_review_checklist,
        promotion_decision=promotion_decision,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_audit_package_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_scheduler_manual_run_replay_validation_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
            manual_run_dry_run_dir=manual_run_dry_run_dir,
        )
    except Exception as exc:
        raise HighIntensitySchedulerAuditPackageError(
            f"TRADING-2352 upstream 2347/2348/2349/2350 validation failed: {exc}"
        ) from exc

    replay_paths = {
        "summary": replay_validation_dir
        / "high_intensity_scheduler_manual_run_replay_validation_summary.json",
        "package": replay_validation_dir
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation.json",
        "source_review": replay_validation_dir
        / "high_intensity_scheduler_manual_run_replay_source_artifact_review.json",
        "semantic_checks": replay_validation_dir
        / "high_intensity_scheduler_manual_run_replay_semantic_checks.json",
        "side_effect_assertions": replay_validation_dir
        / "high_intensity_scheduler_manual_run_replay_side_effect_assertions.json",
        "evidence": replay_validation_dir
        / "high_intensity_scheduler_manual_run_replay_evidence.json",
        "route": replay_validation_dir / "high_intensity_2352_scheduler_audit_package_route.json",
        "interpretation_boundary": replay_validation_dir
        / "high_intensity_scheduler_manual_run_replay_interpretation_boundary.json",
        "safety_boundary": replay_validation_dir
        / "high_intensity_scheduler_manual_run_replay_safety_boundary.json",
    }
    replay_payloads = _load_required_payloads(
        replay_paths,
        "TRADING-2351 manual-run replay validation",
    )
    for key, payload in replay_payloads.items():
        label = f"TRADING-2351 replay validation {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    _validate_2351_source_contracts(replay_payloads)
    _validate_cross_source_contracts(source_inputs, replay_payloads)
    return {
        **source_inputs,
        "replay_validation_dir": str(replay_validation_dir),
        "replay_validation_paths": _string_paths(replay_paths),
        "replay_validation": replay_payloads,
    }


def build_audit_source_artifact_review(*, inputs: Mapping[str, Any]) -> dict[str, Any]:
    replay = mapping(inputs["replay_validation"])
    replay_summary = mapping(replay["summary"])
    replay_package = mapping(replay["package"])
    replay_source_review = mapping(replay["source_review"])
    source_task_ids = list(replay_source_review.get("source_task_ids", []))
    source_task_ids.append(SOURCE_2351_TASK_REGISTER_ID)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.source_artifact_review.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_task_ids,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "disabled_wiring_artifacts_read": True,
            "smoke_dry_run_artifacts_read": True,
            "manual_review_gate_artifacts_read": True,
            "manual_run_dry_run_artifacts_read": True,
            "manual_run_replay_validation_artifacts_read": True,
            "disabled_wiring_artifact_count": len(mapping(inputs["disabled_paths"])),
            "smoke_dry_run_artifact_count": len(mapping(inputs["smoke_paths"])),
            "manual_review_gate_artifact_count": len(
                mapping(inputs["manual_review_gate_paths"])
            ),
            "manual_run_dry_run_artifact_count": len(
                mapping(inputs["manual_run_dry_run_paths"])
            ),
            "manual_run_replay_validation_artifact_count": len(
                mapping(inputs["replay_validation_paths"])
            ),
            "manual_run_replay_validation_status": replay_summary.get("status"),
            "manual_run_replay_validation_package_status": replay_package.get(
                "status"
            ),
            "manual_run_replay_validation_readiness": replay_summary.get("readiness"),
            "manual_run_replay_validation_next_route": replay_summary.get(
                "next_route"
            ),
            "source_validate_data_executed": replay_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": replay_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": replay_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": replay_summary.get(
                "source_validate_data_error_count"
            ),
            "source_contract_status": "PASS",
            **SAFETY_FIELDS,
        }
    )


def build_audit_evidence_chain(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    replay = mapping(inputs["replay_validation"])
    rows = [
        _evidence_row(
            task="TRADING-2347",
            status=mapping(mapping(inputs["disabled_wiring"])["summary"]).get(
                "status"
            ),
            evidence="disabled wiring artifact present",
            promotion_result="blocked",
            complete=True,
        ),
        _evidence_row(
            task="TRADING-2348",
            status=mapping(mapping(inputs["smoke_dry_run"])["summary"]).get("status"),
            evidence="smoke dry-run evidence present",
            promotion_result="blocked",
            complete=True,
        ),
        _evidence_row(
            task="TRADING-2349",
            status=mapping(mapping(inputs["manual_review_gate"])["summary"]).get(
                "status"
            ),
            evidence="manual review gate present",
            promotion_result="blocked",
            complete=True,
        ),
        _evidence_row(
            task="TRADING-2350",
            status=mapping(mapping(inputs["manual_run_dry_run"])["summary"]).get(
                "status"
            ),
            evidence="manual-run dry-run preview present",
            promotion_result="blocked",
            complete=True,
        ),
        _evidence_row(
            task="TRADING-2351",
            status=mapping(replay["summary"]).get("status"),
            evidence="manual-run replay no-side-effect evidence present",
            promotion_result="blocked",
            complete=True,
        ),
    ]
    complete = all(row["evidence_present"] for row in rows)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.evidence_chain.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS if complete else "AUDIT_EVIDENCE_CHAIN_INCOMPLETE",
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain": {
                "disabled_wiring_implemented": rows[0]["evidence_present"],
                "smoke_dry_run_passed": rows[1]["evidence_present"],
                "manual_review_gate_ready": rows[2]["evidence_present"],
                "manual_run_dry_run_ready": rows[3]["evidence_present"],
                "manual_run_replay_no_side_effect_passed": rows[4][
                    "evidence_present"
                ],
                "evidence_chain_complete": complete,
            },
            "source_task_evidence": rows,
            "evidence_chain_complete": complete,
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2353_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_audit_guardrail_summary(
    *,
    generated_at: datetime,
    evidence_chain: Mapping[str, Any],
) -> dict[str, Any]:
    complete = evidence_chain.get("evidence_chain_complete") is True
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.guardrail_summary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS if complete else "AUDIT_GUARDRAIL_SUMMARY_BLOCKED",
            "guardrail_summary": dict(GUARDRAIL_SUMMARY),
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2353_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_audit_side_effect_summary(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    evidence_chain: Mapping[str, Any],
) -> dict[str, Any]:
    replay_assertions = mapping(
        mapping(mapping(inputs["replay_validation"])["side_effect_assertions"]).get(
            "side_effect_assertions"
        )
    )
    failed = [
        field
        for field, expected in SIDE_EFFECT_SUMMARY.items()
        if replay_assertions.get(field, expected) is not expected
    ]
    if evidence_chain.get("evidence_chain_complete") is not True:
        failed.append("evidence_chain_incomplete")
    failed = sorted(set(failed))
    passed = not failed
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.side_effect_summary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS if passed else "AUDIT_SIDE_EFFECT_SUMMARY_BLOCKED",
            "side_effect_summary": dict(SIDE_EFFECT_SUMMARY),
            "source_side_effect_assertions": {
                field: replay_assertions.get(field) for field in SIDE_EFFECT_SUMMARY
            },
            "side_effect_assertions_passed": passed,
            "side_effect_violation_count": len(failed),
            "side_effect_violations": failed,
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2353_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_owner_review_checklist(
    *,
    generated_at: datetime,
    evidence_chain: Mapping[str, Any],
    guardrail_summary: Mapping[str, Any],
    side_effect_summary: Mapping[str, Any],
) -> dict[str, Any]:
    complete = (
        evidence_chain.get("evidence_chain_complete") is True
        and side_effect_summary.get("side_effect_assertions_passed") is True
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.owner_review_checklist.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS if complete else "OWNER_REVIEW_CHECKLIST_BLOCKED",
            "owner_review_checklist": dict(OWNER_REVIEW_CHECKLIST),
            "owner_review_required": True,
            "manual_review_required": True,
            "checklist_complete": complete,
            "evidence_chain_complete": evidence_chain.get("evidence_chain_complete"),
            "guardrail_summary": guardrail_summary.get("guardrail_summary"),
            "side_effect_summary": side_effect_summary.get("side_effect_summary"),
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2353_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_audit_promotion_decision(
    *,
    generated_at: datetime,
    evidence_chain: Mapping[str, Any],
    owner_review_checklist: Mapping[str, Any],
) -> dict[str, Any]:
    ready_for_review = (
        evidence_chain.get("evidence_chain_complete") is True
        and owner_review_checklist.get("owner_review_required") is True
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.promotion_decision.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS if ready_for_review else "AUDIT_PROMOTION_DECISION_BLOCKED",
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "owner_review_required": True,
            "manual_review_required": True,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2353_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_audit_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    evidence_chain: Mapping[str, Any],
    guardrail_summary: Mapping[str, Any],
    side_effect_summary: Mapping[str, Any],
    owner_review_checklist: Mapping[str, Any],
    promotion_decision: Mapping[str, Any],
) -> dict[str, Any]:
    ready = (
        evidence_chain.get("evidence_chain_complete") is True
        and side_effect_summary.get("side_effect_assertions_passed") is True
        and owner_review_checklist.get("owner_review_required") is True
        and promotion_decision.get("promotion_decision") == "BLOCKED"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.package.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "status": STATUS if ready else "AUDIT_PACKAGE_BLOCKED",
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain": evidence_chain.get("evidence_chain"),
            "evidence_chain_complete": evidence_chain.get("evidence_chain_complete"),
            "source_task_evidence": evidence_chain.get("source_task_evidence"),
            "guardrail_summary": guardrail_summary.get("guardrail_summary"),
            "side_effect_summary": side_effect_summary.get("side_effect_summary"),
            "side_effect_assertions_passed": side_effect_summary.get(
                "side_effect_assertions_passed"
            ),
            "owner_review_checklist": owner_review_checklist.get(
                "owner_review_checklist"
            ),
            "owner_review_required": True,
            "manual_review_required": True,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2353_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2353_owner_review_decision_route(
    *,
    package: Mapping[str, Any],
    evidence_chain: Mapping[str, Any],
    promotion_decision: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if package.get("status") != STATUS:
        blockers.append("AUDIT_PACKAGE_STATUS_NOT_READY")
    if evidence_chain.get("evidence_chain_complete") is not True:
        blockers.append("EVIDENCE_CHAIN_INCOMPLETE")
    if promotion_decision.get("promotion_decision") != "BLOCKED":
        blockers.append("PROMOTION_DECISION_NOT_BLOCKED")
    readiness = "BLOCKED" if blockers else READINESS_STATUS
    next_route = (
        "TRADING-2352_Observe_Only_Scheduler_Audit_Package_Remediation"
        if blockers
        else NEXT_2353_ROUTE
    )
    rationale = (
        "audit package can enter owner review decision record"
        if not blockers
        else "audit package failed; remediation required"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2353_route.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": readiness,
            "next_task": next_route,
            "next_route": next_route,
            "route_blockers": blockers,
            "route_caveats": list(ROUTE_CAVEATS) if not blockers else [],
            "route_rationale": rationale,
            "owner_review_required": True,
            "promotion_decision": "BLOCKED",
            **SAFETY_FIELDS,
        }
    )


def build_audit_interpretation_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "next_route": route.get("next_route"),
            "interpretation": (
                "scheduler audit package and owner review checklist only; not "
                "scheduler activation, manual-run execution, owner approval, "
                "event append, outcome binding, paper-shadow approval, production "
                "readiness, or broker execution"
            ),
            "not_scheduler_enablement": True,
            "not_manual_run_execution": True,
            "not_owner_decision_record": True,
            "not_event_append": True,
            "not_outcome_binding": True,
            "not_paper_shadow_promotion": True,
            "not_production_wiring": True,
            "not_broker_execution": True,
            **SAFETY_FIELDS,
        }
    )


def build_audit_safety_boundary(
    *,
    generated_at: datetime,
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "next_route": route.get("next_route"),
            "forbidden_actions": [
                "enable_scheduler",
                "create_cron_job",
                "create_windows_task",
                "create_github_actions_schedule",
                "execute_manual_run",
                "append_historical_event_log",
                "mutate_event_log",
                "bind_outcome",
                "mutate_outcome_store",
                "enable_paper_shadow",
                "enable_production",
                "call_broker_api",
                "send_order",
                "read_fresh_market_data",
                "generate_new_signal",
                "run_backtest",
                "generate_daily_report",
                "emit_target_weight",
                "emit_rebalance_instruction",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_audit_summary(
    *,
    generated_at: datetime,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    replay_validation_dir: Path,
    source_review: Mapping[str, Any],
    evidence_chain: Mapping[str, Any],
    guardrail_summary: Mapping[str, Any],
    side_effect_summary: Mapping[str, Any],
    owner_review_checklist: Mapping[str, Any],
    promotion_decision: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Scheduler Audit Package "
                "And Owner Review Checklist"
            ),
            "status": STATUS,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "disabled_wiring_dir": str(disabled_wiring_dir),
            "smoke_dry_run_dir": str(smoke_dry_run_dir),
            "manual_review_gate_dir": str(manual_review_gate_dir),
            "manual_run_dry_run_dir": str(manual_run_dry_run_dir),
            "replay_validation_dir": str(replay_validation_dir),
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": evidence_chain.get("evidence_chain_complete"),
            "guardrail_summary": guardrail_summary.get("guardrail_summary"),
            "side_effect_summary": side_effect_summary.get("side_effect_summary"),
            "side_effect_assertions_passed": side_effect_summary.get(
                "side_effect_assertions_passed"
            ),
            "owner_review_checklist": owner_review_checklist.get(
                "owner_review_checklist"
            ),
            "owner_review_required": True,
            "manual_review_required": True,
            "promotion_decision": promotion_decision.get("promotion_decision"),
            "promotion_allowed": False,
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            "next_task": route.get("next_task"),
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
                "aits validate-data not rerun because TRADING-2352 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, TRADING-2350 "
                "manual-run dry-run, and TRADING-2351 replay validation artifacts; "
                "it does not consume fresh market data, append events, bind "
                "outcomes, produce technical features, score, backtest, or "
                "generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_audit_package_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    evidence_chain: Mapping[str, Any],
    guardrail_summary: Mapping[str, Any],
    side_effect_summary: Mapping[str, Any],
    owner_review_checklist: Mapping[str, Any],
    promotion_decision: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["evidence_chain"], evidence_chain)
    write_json(paths["guardrail_summary"], guardrail_summary)
    write_json(paths["side_effect_summary"], side_effect_summary)
    write_json(paths["owner_review_checklist"], owner_review_checklist)
    write_json(paths["promotion_decision"], promotion_decision)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["audit_doc"], render_audit_package_doc(package))
    write_markdown(paths["route_doc"], render_2353_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_audit_package_doc(package: Mapping[str, Any]) -> str:
    source_rows = [
        "|Task|Status|Evidence|Promotion Result|",
        "|---|---|---|---|",
    ]
    for row in package.get("source_task_evidence", []):
        item = mapping(row)
        source_rows.append(
            "|"
            f"{item.get('task')}|"
            f"{item.get('status')}|"
            f"{item.get('evidence')}|"
            f"{item.get('promotion_result')}|"
        )
    guardrails = _markdown_table_from_mapping(package.get("guardrail_summary", {}))
    side_effects = _markdown_table_from_mapping(package.get("side_effect_summary", {}))
    checklist = _markdown_table_from_mapping(package.get("owner_review_checklist", {}))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Audit Package",
            "",
            "## Executive Summary",
            "",
            f"- task_id: `{package.get('task_id')}`",
            f"- task_register_id: `{package.get('task_register_id')}`",
            f"- status: `{package.get('status')}`",
            f"- source_tasks: `{package.get('source_tasks')}`",
            f"- evidence_chain_complete: `{package.get('evidence_chain_complete')}`",
            f"- owner_review_required: `{package.get('owner_review_required')}`",
            f"- manual_review_required: `{package.get('manual_review_required')}`",
            f"- promotion_decision: `{package.get('promotion_decision')}`",
            f"- promotion_allowed: `{package.get('promotion_allowed')}`",
            f"- readiness: `{package.get('readiness')}`",
            f"- next_route: `{package.get('next_route')}`",
            "",
            "## Source Task Evidence Table",
            "",
            *source_rows,
            "",
            "## Guardrail Status Table",
            "",
            *guardrails,
            "",
            "## Side-Effect Assertion Table",
            "",
            *side_effects,
            "",
            "## Promotion Blocked Reasons",
            "",
            *[f"- `{reason}`" for reason in package.get("promotion_blocked_reasons", [])],
            "",
            "## Owner Review Checklist",
            "",
            *checklist,
            "",
            "## Known Caveats",
            "",
            "- No real scheduler is enabled.",
            "- No automated cadence has been created.",
            "- No event append is allowed.",
            "- No outcome binding is allowed.",
            "- No paper-shadow mode is allowed.",
            "- No production path is allowed.",
            "- No broker action is allowed.",
            "- This package is not approval to enable scheduler.",
            "",
            "## Explicit Non-Goals",
            "",
            "- Not scheduler enablement.",
            "- Not manual run execution.",
            "- Not owner decision completion.",
            "- Not event append or outcome binding.",
            "- Not paper-shadow, production, or broker readiness.",
            "",
            "## Next Route",
            "",
            f"`{package.get('next_route')}`",
        ]
    )


def render_2353_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2353 Owner Review Decision Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2353 route 只能进入 owner review decision record。",
            "它不是 scheduler enablement、不是 daily scheduler entry、不是 event append、",
            "不是 outcome binding，也不是 paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_scheduler_audit_package_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_audit_package.json",
        "source_review": output_dir
        / "high_intensity_scheduler_audit_package_source_artifact_review.json",
        "evidence_chain": output_dir
        / "high_intensity_scheduler_audit_package_evidence_chain.json",
        "guardrail_summary": output_dir
        / "high_intensity_scheduler_audit_package_guardrail_summary.json",
        "side_effect_summary": output_dir
        / "high_intensity_scheduler_audit_package_side_effect_summary.json",
        "owner_review_checklist": output_dir
        / "high_intensity_scheduler_owner_review_checklist.json",
        "promotion_decision": output_dir
        / "high_intensity_scheduler_audit_package_promotion_decision.json",
        "route": output_dir / "high_intensity_2353_owner_review_decision_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_audit_package_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_audit_package_safety_boundary.json",
        "audit_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_audit_package.md",
        "route_doc": docs_root / "high_intensity_2353_owner_review_decision_route.md",
    }


def _validate_2351_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    source_review = mapping(payloads["source_review"])
    semantic_checks = mapping(payloads["semantic_checks"])
    side_effects = mapping(payloads["side_effect_assertions"])
    evidence = mapping(payloads["evidence"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2351_STATUS:
        raise HighIntensitySchedulerAuditPackageError(
            f"TRADING-2352 requires 2351 status {EXPECTED_2351_STATUS}"
        )
    if package.get("status") != EXPECTED_2351_STATUS:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 package status"
        )
    if evidence.get("status") != EXPECTED_2351_STATUS:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 evidence status"
        )
    if summary.get("readiness") != EXPECTED_2351_READINESS:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 readiness READY_FOR_2352_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2351_NEXT_ROUTE:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 summary route to audit package"
        )
    if route.get("next_route") != EXPECTED_2351_NEXT_ROUTE:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 route to audit package"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 source contract status PASS"
        )
    if semantic_checks.get("replay_count") != EXPECTED_2351_REPLAY_COUNT:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 replay_count=3"
        )
    if semantic_checks.get("stable_semantic_replay_passed") is not True:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 stable semantic replay pass"
        )
    if side_effects.get("side_effect_assertions_passed") is not True:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 side-effect assertions passed"
        )
    if package.get("source_tasks") != SOURCE_TASKS[:-1]:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 source tasks to be 2347/2348/2349/2350"
        )
    if package.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 promotion_allowed=false"
        )
    _validate_source_data_quality(summary, "TRADING-2351 summary")

    source_side_effects = mapping(side_effects.get("side_effect_assertions"))
    for field, expected in SIDE_EFFECT_SUMMARY.items():
        if source_side_effects.get(field) is not expected:
            raise HighIntensitySchedulerAuditPackageError(
                f"TRADING-2352 requires 2351 {field}=false"
            )


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    replay_payloads: Mapping[str, Any],
) -> None:
    manual_run_summary = mapping(mapping(source_inputs["manual_run_dry_run"])["summary"])
    replay_source_review = mapping(replay_payloads["source_review"])
    if replay_source_review.get("manual_run_dry_run_status") != manual_run_summary.get(
        "status"
    ):
        raise HighIntensitySchedulerAuditPackageError(
            "TRADING-2352 requires 2351 source review manual-run status to match 2350"
        )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2352 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerAuditPackageError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerAuditPackageError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerAuditPackageError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerAuditPackageError(
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
        raise HighIntensitySchedulerAuditPackageError(
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
        raise HighIntensitySchedulerAuditPackageError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerAuditPackageError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensitySchedulerAuditPackageError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerAuditPackageError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerAuditPackageError(
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


def _evidence_row(
    *,
    task: str,
    status: Any,
    evidence: str,
    promotion_result: str,
    complete: bool,
) -> dict[str, Any]:
    return {
        "task": task,
        "status": status,
        "evidence": evidence,
        "promotion_result": promotion_result,
        "evidence_present": complete,
    }


def _markdown_table_from_mapping(payload: object) -> list[str]:
    values = mapping(payload)
    lines = ["|Field|Value|", "|---|---|"]
    for key, value in values.items():
        lines.append(f"|`{key}`|`{value}`|")
    return lines


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}
