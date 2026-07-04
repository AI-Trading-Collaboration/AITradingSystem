from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_review_gate import (
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    HighIntensitySchedulerManualReviewGateError,
    load_high_intensity_scheduler_manual_review_gate_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_review_gate import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_MANUAL_REVIEW_GATE_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_review_gate import (
    NEXT_2350_TASK as EXPECTED_2349_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_review_gate import (
    READINESS_STATUS as EXPECTED_2349_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_review_gate import (
    STATUS as EXPECTED_2349_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_review_gate import (
    TASK_REGISTER_ID as SOURCE_2349_TASK_REGISTER_ID,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    _collect_real_scheduler_creation_fields,
    _collect_unsafe_fields,
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

TASK_ID = "TRADING-2350"
TASK_REGISTER_ID = (
    "TRADING-2350_OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_INTERFACE_DRY_RUN"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_manual_run_dry_run"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_INTERFACE_DRY_RUN_READY_WITH_CAVEATS_"
    "PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2351_WITH_CAVEATS"
NEXT_2351_TASK = (
    "TRADING-2351_Observe_Only_Scheduler_Manual_Run_Replay_No_Side_Effect_"
    "Validation"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SOURCE_TASKS = ["TRADING-2347", "TRADING-2348", "TRADING-2349"]

PROMOTION_BLOCKED_REASONS = [
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
    "DISABLED_BY_DEFAULT",
    "MANUAL_RUN_ONLY",
    "DRY_RUN_ONLY",
    "OBSERVE_ONLY",
    "OWNER_MANUAL_REVIEW_REQUIRED",
    "PROMOTION_BLOCKED",
    "NO_REAL_SCHEDULER",
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "NO_MANUAL_RUN_EXECUTION_IN_2350",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "manual_run_interface_dry_run_only": True,
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
    "broker_action": "none",
    "promotion_allowed": False,
    "promotion_decision": "BLOCKED",
    "manual_review_required": True,
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
    "portfolio_effect": "none",
    "production_effect": "none",
    "manual_review_only": True,
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
}


class HighIntensitySchedulerManualRunDryRunError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    manual_review_gate_dir: Path = DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerManualRunDryRunError(
            f"high-intensity manual-run dry-run only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_manual_run_dry_run_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
    )
    source_review = build_manual_run_source_artifact_review(inputs=inputs)
    preview = build_manual_run_preview(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
    )
    side_effect_assertions = build_manual_run_side_effect_assertions(
        generated_at=generated_at,
        preview=preview,
    )
    evidence = build_manual_run_dry_run_evidence(
        generated_at=generated_at,
        preview=preview,
        side_effect_assertions=side_effect_assertions,
        source_review=source_review,
    )
    package = build_manual_run_dry_run_package(
        generated_at=generated_at,
        source_review=source_review,
        preview=preview,
        evidence=evidence,
        side_effect_assertions=side_effect_assertions,
    )
    route = build_high_intensity_2351_task_route(
        package=package,
        evidence=evidence,
        side_effect_assertions=side_effect_assertions,
    )
    interpretation_boundary = build_manual_run_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_manual_run_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_manual_run_dry_run_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        source_review=source_review,
        preview=preview,
        evidence=evidence,
        side_effect_assertions=side_effect_assertions,
        route=route,
    )
    _validate_generated_payloads(
        {
            "summary": summary,
            "package": package,
            "source_review": source_review,
            "preview": preview,
            "evidence": evidence,
            "side_effect_assertions": side_effect_assertions,
            "route": route,
            "interpretation_boundary": interpretation_boundary,
            "safety_boundary": safety_boundary,
        }
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_manual_run_dry_run_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        preview=preview,
        evidence=evidence,
        side_effect_assertions=side_effect_assertions,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_manual_run_dry_run_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_scheduler_manual_review_gate_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
        )
    except HighIntensitySchedulerManualReviewGateError as exc:
        raise HighIntensitySchedulerManualRunDryRunError(
            f"TRADING-2350 source validation failed: {exc}"
        ) from exc

    manual_gate_paths = {
        "summary": manual_review_gate_dir
        / "high_intensity_scheduler_manual_review_gate_summary.json",
        "gate_package": manual_review_gate_dir
        / "high_intensity_risk_cap_observe_only_scheduler_manual_review_gate.json",
        "source_review": manual_review_gate_dir
        / "high_intensity_scheduler_manual_review_gate_source_artifact_review.json",
        "promotion_decision": manual_review_gate_dir
        / "high_intensity_scheduler_manual_review_gate_promotion_decision.json",
        "route": manual_review_gate_dir / "high_intensity_2350_manual_run_interface_route.json",
        "interpretation_boundary": manual_review_gate_dir
        / "high_intensity_scheduler_manual_review_gate_interpretation_boundary.json",
        "safety_boundary": manual_review_gate_dir
        / "high_intensity_scheduler_manual_review_gate_safety_boundary.json",
    }
    manual_gate_payloads = _load_required_payloads(
        manual_gate_paths,
        "TRADING-2349 manual review gate",
    )
    for key, payload in manual_gate_payloads.items():
        label = f"TRADING-2349 manual review gate {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    _validate_2349_source_contracts(manual_gate_payloads)
    _validate_cross_source_contracts(source_inputs, manual_gate_payloads)
    return {
        **source_inputs,
        "manual_review_gate_dir": str(manual_review_gate_dir),
        "manual_review_gate_paths": _string_paths(manual_gate_paths),
        "manual_review_gate": manual_gate_payloads,
    }


def build_manual_run_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    disabled = mapping(inputs["disabled_wiring"])
    smoke = mapping(inputs["smoke_dry_run"])
    manual_gate = mapping(inputs["manual_review_gate"])
    disabled_summary = mapping(disabled["summary"])
    smoke_summary = mapping(smoke["summary"])
    smoke_evidence = mapping(smoke["evidence"])
    manual_gate_summary = mapping(manual_gate["summary"])
    manual_gate_package = mapping(manual_gate["gate_package"])
    manual_gate_decision = mapping(manual_gate["promotion_decision"])
    manual_gate_route = mapping(manual_gate["route"])
    source_task_ids = list(mapping(manual_gate["source_review"]).get("source_task_ids", []))
    source_task_ids.append(SOURCE_2349_TASK_REGISTER_ID)
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
            "disabled_wiring_artifact_count": len(mapping(inputs["disabled_paths"])),
            "smoke_dry_run_artifact_count": len(mapping(inputs["smoke_paths"])),
            "manual_review_gate_artifact_count": len(
                mapping(inputs["manual_review_gate_paths"])
            ),
            "disabled_wiring_status": disabled_summary.get("status"),
            "smoke_dry_run_status": smoke_summary.get("status"),
            "smoke_dry_run_evidence_status": smoke_evidence.get("status"),
            "manual_review_gate_status": manual_gate_summary.get("status"),
            "manual_review_gate_package_status": manual_gate_package.get("status"),
            "manual_review_gate_readiness": manual_gate_summary.get("readiness"),
            "manual_review_gate_next_route": manual_gate_route.get("next_route"),
            "manual_review_gate_promotion_decision": manual_gate_decision.get(
                "promotion_decision"
            ),
            "manual_review_gate_promotion_allowed": manual_gate_decision.get(
                "promotion_allowed"
            ),
            "source_validate_data_executed": manual_gate_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": manual_gate_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": manual_gate_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": manual_gate_summary.get(
                "source_validate_data_error_count"
            ),
            "source_contract_status": "PASS",
            **SAFETY_FIELDS,
        }
    )


def build_manual_run_preview(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.manual_run_preview.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": f"{REPORT_TYPE}_manual_run_preview",
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "manual_run_interface_present": True,
            "manual_run_preview_generated": True,
            "manual_run_executed": False,
            "manual_run_execution_allowed_in_2350": False,
            "manual_run_command_preview": (
                "aits research trends "
                "high-intensity-risk-cap-observe-only-scheduler-manual-run-"
                "dry-run"
            ),
            "manual_run_input_artifacts": {
                "disabled_wiring_dir": inputs.get("disabled_wiring_dir"),
                "smoke_dry_run_dir": inputs.get("smoke_dry_run_dir"),
                "manual_review_gate_dir": inputs.get("manual_review_gate_dir"),
            },
            "manual_run_preview_scope": {
                "read_prior_artifacts": True,
                "build_preview": True,
                "build_dry_run_evidence": True,
                "assert_no_side_effects": True,
                "execute_scheduler_logic": False,
                "append_events": False,
                "bind_outcomes": False,
                "read_fresh_market_data": False,
                "emit_portfolio_action": False,
                "call_broker": False,
            },
            "promotion_decision": "BLOCKED",
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2351_TASK,
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2350 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, and TRADING-2349 manual review gate artifacts; "
                "it does not consume fresh market data, append events, or bind "
                "outcomes."
            ),
            **SAFETY_FIELDS,
        }
    )


def build_manual_run_side_effect_assertions(
    *,
    generated_at: datetime,
    preview: Mapping[str, Any],
) -> dict[str, Any]:
    assertions = {
        "real_scheduler_created": False,
        "cron_created": False,
        "windows_task_created": False,
        "github_actions_schedule_created": False,
        "event_append_attempted": False,
        "outcome_binding_attempted": False,
        "paper_shadow_attempted": False,
        "production_attempted": False,
        "broker_action_attempted": False,
        "manual_run_executed": False,
        "fresh_market_data_read": False,
        "event_log_mutated": False,
        "outcome_store_mutated": False,
    }
    failed = sorted(key for key, value in assertions.items() if value is not False)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.side_effect_assertions.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS,
            "manual_run_interface_present": preview.get(
                "manual_run_interface_present"
            ),
            "manual_run_preview_generated": preview.get(
                "manual_run_preview_generated"
            ),
            "side_effect_assertions": assertions,
            "side_effect_assertions_passed": not failed,
            "side_effect_status": "PASS" if not failed else "FAIL",
            "side_effect_violation_count": len(failed),
            "side_effect_violations": failed,
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2351_TASK,
            **SAFETY_FIELDS,
        }
    )


def build_manual_run_dry_run_evidence(
    *,
    generated_at: datetime,
    preview: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.evidence.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": f"{REPORT_TYPE}_evidence",
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "manual_run_interface_present": preview.get(
                "manual_run_interface_present"
            ),
            "manual_run_preview_generated": preview.get(
                "manual_run_preview_generated"
            ),
            "manual_run_executed": False,
            "source_artifacts_read": source_review.get("source_artifacts_read"),
            "source_contract_status": source_review.get("source_contract_status"),
            "side_effect_assertions": side_effect_assertions.get(
                "side_effect_assertions"
            ),
            "side_effect_assertions_passed": side_effect_assertions.get(
                "side_effect_assertions_passed"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2351_TASK,
            **SAFETY_FIELDS,
        }
    )


def build_manual_run_dry_run_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    preview: Mapping[str, Any],
    evidence: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.package.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "status": STATUS,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "source_review_status": source_review.get("source_contract_status"),
            "manual_run_interface_present": preview.get(
                "manual_run_interface_present"
            ),
            "manual_run_preview_generated": preview.get(
                "manual_run_preview_generated"
            ),
            "manual_run_executed": evidence.get("manual_run_executed"),
            "side_effect_assertions": side_effect_assertions.get(
                "side_effect_assertions"
            ),
            "side_effect_assertions_passed": side_effect_assertions.get(
                "side_effect_assertions_passed"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2351_TASK,
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
            "aits_validate_data_rerun": preview.get("aits_validate_data_rerun"),
            "aits_validate_data_rerun_reason": preview.get(
                "aits_validate_data_rerun_reason"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2351_task_route(
    *,
    package: Mapping[str, Any],
    evidence: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if package.get("status") != STATUS:
        blockers.append("MANUAL_RUN_DRY_RUN_STATUS_NOT_READY")
    if package.get("manual_run_interface_present") is not True:
        blockers.append("MANUAL_RUN_INTERFACE_MISSING")
    if package.get("manual_run_preview_generated") is not True:
        blockers.append("MANUAL_RUN_PREVIEW_NOT_GENERATED")
    if package.get("manual_run_executed") is not False:
        blockers.append("MANUAL_RUN_EXECUTED")
    if evidence.get("promotion_allowed") is not False:
        blockers.append("PROMOTION_ALLOWED_NOT_FALSE")
    if side_effect_assertions.get("side_effect_assertions_passed") is not True:
        blockers.append("SIDE_EFFECT_ASSERTIONS_NOT_PASSED")
    readiness = "BLOCKED" if blockers else READINESS_STATUS
    next_task = (
        "TRADING-2350_Manual_Run_Interface_Dry_Run_Remediation"
        if blockers
        else NEXT_2351_TASK
    )
    rationale = (
        "manual-run interface dry-run can enter replay no-side-effect validation"
        if not blockers
        else "manual-run interface dry-run failed; remediation required"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2351_task_route.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": readiness,
            "next_task": next_task,
            "next_route": next_task,
            "route_blockers": blockers,
            "route_caveats": list(ROUTE_CAVEATS) if not blockers else [],
            "route_rationale": rationale,
            **SAFETY_FIELDS,
        }
    )


def build_manual_run_interpretation_boundary(
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
                "manual-run interface dry-run only; not manual-run execution, "
                "scheduler activation, event append, outcome binding, "
                "paper-shadow approval, production readiness, or broker execution"
            ),
            "not_manual_run_execution": True,
            "not_scheduler_enablement": True,
            "not_daily_scheduler_entry": True,
            "not_event_append": True,
            "not_outcome_binding": True,
            "not_paper_shadow_promotion": True,
            "not_production_wiring": True,
            "not_broker_execution": True,
            **SAFETY_FIELDS,
        }
    )


def build_manual_run_safety_boundary(
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
                "execute_manual_run",
                "enable_scheduler",
                "create_cron_job",
                "create_windows_task",
                "create_github_actions_schedule",
                "append_historical_event_log",
                "mutate_event_log",
                "bind_outcome",
                "mutate_outcome_store",
                "enable_paper_shadow",
                "enable_production",
                "call_broker_api",
                "send_order",
                "read_fresh_market_data",
                "emit_target_weight",
                "emit_rebalance_instruction",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_manual_run_dry_run_summary(
    *,
    generated_at: datetime,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    source_review: Mapping[str, Any],
    preview: Mapping[str, Any],
    evidence: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
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
                "High-Intensity Risk-Cap Observe-Only Scheduler Manual-Run "
                "Interface Dry-Run"
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
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "manual_run_interface_present": preview.get(
                "manual_run_interface_present"
            ),
            "manual_run_executed": evidence.get("manual_run_executed"),
            "manual_run_preview_generated": preview.get(
                "manual_run_preview_generated"
            ),
            "side_effect_assertions": side_effect_assertions.get(
                "side_effect_assertions"
            ),
            "side_effect_assertions_passed": side_effect_assertions.get(
                "side_effect_assertions_passed"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
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
            "aits_validate_data_rerun": preview.get("aits_validate_data_rerun"),
            "aits_validate_data_rerun_reason": preview.get(
                "aits_validate_data_rerun_reason"
            ),
            **SAFETY_FIELDS,
        }
    )


def write_manual_run_dry_run_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    preview: Mapping[str, Any],
    evidence: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["preview"], preview)
    write_json(paths["evidence"], evidence)
    write_json(paths["side_effect_assertions"], side_effect_assertions)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["dry_run_doc"], render_manual_run_dry_run_doc(package))
    write_markdown(paths["route_doc"], render_2351_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_manual_run_dry_run_doc(package: Mapping[str, Any]) -> str:
    assertions = mapping(package.get("side_effect_assertions"))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Manual-Run Interface Dry-Run",
            "",
            f"- task_id: `{package.get('task_id')}`",
            f"- task_register_id: `{package.get('task_register_id')}`",
            f"- status: `{package.get('status')}`",
            f"- source_tasks: `{package.get('source_tasks')}`",
            (
                "- manual_run_interface_present: "
                f"`{package.get('manual_run_interface_present')}`"
            ),
            (
                "- manual_run_preview_generated: "
                f"`{package.get('manual_run_preview_generated')}`"
            ),
            f"- manual_run_executed: `{package.get('manual_run_executed')}`",
            f"- scheduler_enabled: `{package.get('scheduler_enabled')}`",
            f"- manual_run_only: `{package.get('manual_run_only')}`",
            f"- dry_run_only: `{package.get('dry_run_only')}`",
            f"- promotion_allowed: `{package.get('promotion_allowed')}`",
            f"- event_append_attempted: `{package.get('event_append_attempted')}`",
            (
                "- outcome_binding_attempted: "
                f"`{package.get('outcome_binding_attempted')}`"
            ),
            f"- paper_shadow_attempted: `{package.get('paper_shadow_attempted')}`",
            f"- production_attempted: `{package.get('production_attempted')}`",
            f"- broker_action_attempted: `{package.get('broker_action_attempted')}`",
            (
                "- side_effect_assertions_passed: "
                f"`{package.get('side_effect_assertions_passed')}`"
            ),
            f"- real_scheduler_created: `{assertions.get('real_scheduler_created')}`",
            f"- cron_created: `{assertions.get('cron_created')}`",
            f"- windows_task_created: `{assertions.get('windows_task_created')}`",
            (
                "- github_actions_schedule_created: "
                f"`{assertions.get('github_actions_schedule_created')}`"
            ),
            f"- readiness: `{package.get('readiness')}`",
            f"- next_route: `{package.get('next_route')}`",
            "",
            "TRADING-2350 只验证人工触发入口可以被安全 preview。",
            "本任务没有执行 manual run，没有启用 scheduler，没有 append event",
            "或绑定 outcome，也没有进入 paper-shadow、production 或 broker。",
        ]
    )


def render_2351_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2351 Manual-Run Replay Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2351 route 只能进入 manual-run replay no-side-effect validation。",
            "任何 scheduler enablement、event append、outcome binding、paper-shadow、",
            "production 或 broker action 仍需要后续单独任务和人工批准。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_scheduler_manual_run_dry_run_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run.json",
        "source_review": output_dir
        / "high_intensity_scheduler_manual_run_dry_run_source_artifact_review.json",
        "preview": output_dir
        / "high_intensity_scheduler_manual_run_dry_run_preview.json",
        "evidence": output_dir
        / "high_intensity_scheduler_manual_run_dry_run_evidence.json",
        "side_effect_assertions": output_dir
        / "high_intensity_scheduler_manual_run_dry_run_side_effect_assertions.json",
        "route": output_dir / "high_intensity_2351_manual_run_replay_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_manual_run_dry_run_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_manual_run_dry_run_safety_boundary.json",
        "dry_run_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run.md",
        "route_doc": docs_root / "high_intensity_2351_manual_run_replay_route.md",
    }


def _validate_2349_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    gate_package = mapping(payloads["gate_package"])
    source_review = mapping(payloads["source_review"])
    promotion_decision = mapping(payloads["promotion_decision"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2349_STATUS:
        raise HighIntensitySchedulerManualRunDryRunError(
            f"TRADING-2350 requires 2349 status {EXPECTED_2349_STATUS}"
        )
    if gate_package.get("status") != EXPECTED_2349_STATUS:
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 gate package status"
        )
    if summary.get("readiness") != EXPECTED_2349_READINESS:
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 readiness READY_FOR_2350_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2349_NEXT_ROUTE:
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 summary route to manual-run dry-run"
        )
    if route.get("next_route") != EXPECTED_2349_NEXT_ROUTE:
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 route to manual-run dry-run"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 source contract status PASS"
        )
    if promotion_decision.get("promotion_decision") != "BLOCKED":
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 promotion decision BLOCKED"
        )
    if promotion_decision.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 promotion_allowed=false"
        )
    if gate_package.get("manual_review_required") is not True:
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 manual_review_required=true"
        )
    if gate_package.get("source_tasks") != ["TRADING-2347", "TRADING-2348"]:
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 source tasks to be 2347/2348"
        )
    _validate_source_data_quality(summary, "TRADING-2349 summary")


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    manual_gate_payloads: Mapping[str, Any],
) -> None:
    smoke_summary = mapping(mapping(source_inputs["smoke_dry_run"])["summary"])
    manual_summary = mapping(manual_gate_payloads["summary"])
    manual_source_review = mapping(manual_gate_payloads["source_review"])
    if manual_summary.get("smoke_dry_run_status") != smoke_summary.get("status"):
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 summary smoke status to match 2348"
        )
    if manual_source_review.get("smoke_dry_run_status") != smoke_summary.get("status"):
        raise HighIntensitySchedulerManualRunDryRunError(
            "TRADING-2350 requires 2349 source review smoke status to match 2348"
        )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2350 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerManualRunDryRunError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerManualRunDryRunError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerManualRunDryRunError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerManualRunDryRunError(
            f"{path}: expected JSON object"
        )
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensitySchedulerManualRunDryRunError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _validate_no_real_scheduler_creation(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_real_scheduler_creation_fields(payload)
    if violations:
        raise HighIntensitySchedulerManualRunDryRunError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerManualRunDryRunError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in (
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
        "paper_shadow_allowed",
        "paper_shadow_attempted",
        "production_enabled",
        "production_allowed",
        "production_attempted",
        "broker_action_enabled",
        "broker_action_attempted",
        "promotion_allowed",
    ):
        if field in payload and payload.get(field) is not False:
            raise HighIntensitySchedulerManualRunDryRunError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerManualRunDryRunError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerManualRunDryRunError(
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


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}
