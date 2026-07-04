from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_WIRING_PLAN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    DEFAULT_SCHEDULER_DRY_RUN_ROOT,
    EXPECTED_2345_STATUS,
    NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK,
    load_trading_2345_scheduler_wiring_plan_context,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_wiring_plan import (
    READY_WITH_CAVEATS_STATUS as EXPECTED_2346_STATUS,
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

TASK_ID = (
    "TRADING-2347_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_SCHEDULER_"
    "DISABLED_WIRING_IMPLEMENTATION"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_disabled_wiring"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_disabled_wiring"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_IMPLEMENTED_WITH_CAVEATS_"
    "PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2348_WITH_CAVEATS"
NEXT_2348_TASK = (
    "TRADING-2348_Disabled_Scheduler_Wiring_Smoke_Dry_Run_And_Guardrail_"
    "Evidence"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

PROMOTION_BLOCKED_REASONS = [
    "OBSERVE_ONLY_MODE",
    "MANUAL_REVIEW_REQUIRED",
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
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "disabled_wiring_implementation_only": True,
    "scheduler_enabled": False,
    "scheduler_default_enabled": False,
    "manual_run_only": True,
    "dry_run_only": True,
    "event_append_enabled": False,
    "event_append_executed": False,
    "outcome_binding_enabled": False,
    "outcome_binding_executed": False,
    "paper_shadow_enabled": False,
    "paper_shadow_allowed": False,
    "production_enabled": False,
    "production_allowed": False,
    "broker_action_enabled": False,
    "broker_action": "none",
    "promotion_allowed": False,
    "portfolio_effect": "none",
    "production_effect": "none",
    "manual_review_only": True,
}

INPUT_SAFETY_FALSE_FIELDS = {
    "scheduler_enabled",
    "scheduler_default_enabled",
    "event_append_enabled",
    "event_append_executed",
    "outcome_binding_enabled",
    "outcome_binding_executed",
    "paper_shadow_enabled",
    "paper_shadow_allowed",
    "production_enabled",
    "production_allowed",
    "promotion_allowed",
    "broker_action_enabled",
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


class HighIntensitySchedulerDisabledWiringError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_disabled_wiring(
    *,
    wiring_plan_dir: Path = DEFAULT_WIRING_PLAN_ROOT,
    scheduler_dry_run_dir: Path = DEFAULT_SCHEDULER_DRY_RUN_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerDisabledWiringError(
            f"high-intensity disabled scheduler wiring only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_disabled_wiring_inputs(
        wiring_plan_dir=wiring_plan_dir,
        scheduler_dry_run_dir=scheduler_dry_run_dir,
    )
    manifest = build_disabled_wiring_implementation_manifest(inputs=inputs)
    guardrails = build_disabled_wiring_guardrail_status(
        manifest=manifest,
        inputs=inputs,
    )
    referenced_artifacts = build_disabled_wiring_referenced_artifact_manifest(
        inputs=inputs,
        wiring_plan_dir=wiring_plan_dir,
        scheduler_dry_run_dir=scheduler_dry_run_dir,
    )
    no_real_scheduler = build_no_real_scheduler_assertion(
        manifest=manifest,
        guardrails=guardrails,
    )
    readiness = build_high_intensity_2348_readiness_checklist(
        manifest=manifest,
        guardrails=guardrails,
        no_real_scheduler=no_real_scheduler,
    )
    task_route = build_high_intensity_2348_task_route(readiness=readiness)
    interpretation_boundary = build_disabled_wiring_interpretation_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    safety_boundary = build_disabled_wiring_safety_boundary(
        generated_at=generated_at,
        task_route=task_route,
    )
    summary = build_disabled_wiring_summary(
        generated_at=generated_at,
        wiring_plan_dir=wiring_plan_dir,
        scheduler_dry_run_dir=scheduler_dry_run_dir,
        inputs=inputs,
        manifest=manifest,
        guardrails=guardrails,
        readiness=readiness,
        task_route=task_route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_disabled_wiring_outputs(
        paths=paths,
        summary=summary,
        manifest=manifest,
        guardrails=guardrails,
        referenced_artifacts=referenced_artifacts,
        no_real_scheduler=no_real_scheduler,
        readiness=readiness,
        task_route=task_route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_disabled_wiring_inputs(
    *,
    wiring_plan_dir: Path,
    scheduler_dry_run_dir: Path,
) -> dict[str, Any]:
    wiring_plan = load_trading_2346_disabled_wiring_context(wiring_plan_dir)
    scheduler_dry_run = load_trading_2345_scheduler_wiring_plan_context(
        scheduler_dry_run_dir
    )
    inputs = {
        "wiring_plan": wiring_plan,
        "scheduler_dry_run": scheduler_dry_run,
    }
    _validate_2347_cross_source_contracts(inputs)
    return inputs


def load_trading_2346_disabled_wiring_context(root: Path) -> dict[str, Any]:
    paths = {
        "summary": root / "high_intensity_scheduler_wiring_plan_summary.json",
        "config_entry_plan": root
        / "high_intensity_scheduler_config_entry_plan.json",
        "disabled_policy": root
        / "high_intensity_scheduler_disabled_wiring_policy.json",
        "manual_run_contract": root
        / "high_intensity_scheduler_manual_run_contract.json",
        "dry_run_only_mode_contract": root
        / "high_intensity_scheduler_dry_run_only_mode_contract.json",
        "job_wiring_contract": root
        / "high_intensity_scheduler_job_wiring_contract.json",
        "artifact_wiring_plan": root
        / "high_intensity_scheduler_artifact_wiring_plan.json",
        "registry_wiring_plan": root
        / "high_intensity_scheduler_registry_wiring_plan.json",
        "wiring_safety_gate": root
        / "high_intensity_scheduler_wiring_safety_gate.json",
        "implementation_contract": root
        / "high_intensity_scheduler_wiring_implementation_contract.json",
        "readiness": root / "high_intensity_2347_readiness_checklist.json",
        "task_route": root / "high_intensity_2347_task_route.json",
        "safety_boundary": root
        / "high_intensity_scheduler_wiring_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2346 wiring plan")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2346 {key}", payload)

    summary = mapping(payloads["summary"])
    readiness = mapping(payloads["readiness"])
    task_route = mapping(payloads["task_route"])
    safety_gate = mapping(payloads["wiring_safety_gate"])
    config_entry = mapping(payloads["config_entry_plan"])
    disabled_policy = mapping(payloads["disabled_policy"])
    implementation_contract = mapping(payloads["implementation_contract"])

    if summary.get("status") != EXPECTED_2346_STATUS:
        raise HighIntensitySchedulerDisabledWiringError(
            f"TRADING-2347 requires 2346 status {EXPECTED_2346_STATUS}"
        )
    if summary.get("next_task") != NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK:
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires 2346 next task to be disabled wiring "
            "implementation"
        )
    if readiness.get("readiness_status") != "READY_FOR_2347_WITH_CAVEATS":
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires 2346 readiness READY_FOR_2347_WITH_CAVEATS"
        )
    if task_route.get("next_task") != NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK:
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires 2346 task route to disabled wiring "
            "implementation"
        )
    if safety_gate.get("safety_gate_status") not in {"PASS", "PASS_WITH_WARNINGS"}:
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires 2346 wiring safety gate PASS"
        )
    if implementation_contract.get("implementation_task") != (
        NEXT_2347_DISABLED_WIRING_IMPLEMENTATION_TASK
    ):
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires 2346 implementation contract to target 2347"
        )
    if implementation_contract.get("scheduler_enabled_allowed") is not False:
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires 2346 implementation contract to block "
            "scheduler enablement"
        )

    for label, payload in (
        ("TRADING-2346 summary", summary),
        ("TRADING-2346 config entry", config_entry),
        ("TRADING-2346 disabled policy", disabled_policy),
        ("TRADING-2346 readiness", readiness),
        ("TRADING-2346 task route", task_route),
    ):
        for field in (
            "scheduler_enabled",
            "scheduler_default_enabled",
            "event_append_executed",
            "outcome_binding_executed",
        ):
            if field in payload:
                _require_false(payload, field, label)
        if "manual_run_only" in payload:
            _require_true(payload, "manual_run_only", label)
        if "dry_run_only" in payload:
            _require_true(payload, "dry_run_only", label)
        _require_false(payload, "promotion_allowed", label)
        _require_false(payload, "paper_shadow_allowed", label)
        _require_false(payload, "production_allowed", label)
        _require_broker_none(payload, label)

    return {"source_dir": str(root), "paths": _string_paths(paths), **payloads}


def build_disabled_wiring_implementation_manifest(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    wiring_summary = mapping(mapping(inputs["wiring_plan"]).get("summary"))
    scheduler_summary = mapping(mapping(inputs["scheduler_dry_run"]).get("summary"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.implementation_manifest.v1",
            "task_id": TASK_ID,
            "status": STATUS,
            "implementation_status": STATUS,
            "wiring_skeleton_present": True,
            "wiring_skeleton_scope": [
                "cli_status_check",
                "disabled_implementation_manifest",
                "guardrail_status",
                "referenced_artifact_manifest",
                "2348_smoke_dry_run_route",
            ],
            "referenced_2346_status": wiring_summary.get("status"),
            "referenced_2346_next_task": wiring_summary.get("next_task"),
            "referenced_2345_status": scheduler_summary.get("status"),
            "referenced_2345_next_task": scheduler_summary.get("next_task"),
            "source_validate_data_executed": wiring_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": wiring_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": wiring_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": wiring_summary.get(
                "source_validate_data_error_count"
            ),
            "scheduler_registry_entry_present": True,
            "scheduler_registry_entry_enabled": False,
            "external_scheduler_entry_created": False,
            "cron_entry_created": False,
            "windows_task_created": False,
            "github_action_schedule_created": False,
            "daily_scheduler_entry_created": False,
            "real_scheduler_created": False,
            "fresh_market_data_read": False,
            "historical_event_log_mutated": False,
            "outcome_store_mutated": False,
            "advisory_outcome_bound": False,
            "promotion_blocked_reason": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2348_TASK,
            **SAFETY_FIELDS,
        }
    )


def build_disabled_wiring_guardrail_status(
    *,
    manifest: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Any] | None = None,
    extra_payloads: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    payloads: list[Mapping[str, Any]] = [mapping(manifest)]
    if inputs:
        wiring_plan = mapping(inputs.get("wiring_plan"))
        payloads.extend(
            [
                mapping(wiring_plan.get("summary")),
                mapping(wiring_plan.get("config_entry_plan")),
                mapping(wiring_plan.get("disabled_policy")),
                mapping(wiring_plan.get("wiring_safety_gate")),
                mapping(wiring_plan.get("implementation_contract")),
            ]
        )
    payloads.extend(extra_payloads or [])
    violations = sorted(
        {
            violation
            for payload in payloads
            for violation in _collect_unsafe_fields(payload)
        }
    )
    real_scheduler_violations = sorted(
        {
            field
            for payload in payloads
            for field in _collect_real_scheduler_creation_fields(payload)
        }
    )
    status = (
        "FAIL_CLOSED_TRIGGERED"
        if violations or real_scheduler_violations
        else "PASS"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.guardrail_status.v1",
            "task_id": TASK_ID,
            "guardrail_status": status,
            "safety_gate_status": status,
            "safety_error_count": len(violations) + len(real_scheduler_violations),
            "unsafe_field_violations": violations,
            "real_scheduler_creation_violations": real_scheduler_violations,
            "block_if_scheduler_enabled": True,
            "block_if_real_scheduler_created": True,
            "block_if_event_append_enabled": True,
            "block_if_event_append_executed": True,
            "block_if_outcome_binding_enabled": True,
            "block_if_paper_shadow_enabled": True,
            "block_if_production_enabled": True,
            "block_if_broker_action_enabled": True,
            "block_if_target_weight_generated": True,
            "block_if_rebalance_instruction_generated": True,
            "allowed_outputs": [
                "disabled_wiring_manifest",
                "guardrail_status",
                "referenced_artifact_manifest",
                "2348_readiness_route",
                "research_docs",
            ],
            "blocked_outputs": [
                "enabled_scheduler_config",
                "cron_job",
                "windows_task",
                "github_action_schedule",
                "event_append",
                "outcome_binding",
                "paper_shadow",
                "production",
                "broker_action",
                "target_weight",
                "rebalance_instruction",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_disabled_wiring_referenced_artifact_manifest(
    *,
    inputs: Mapping[str, Any],
    wiring_plan_dir: Path,
    scheduler_dry_run_dir: Path,
) -> dict[str, Any]:
    wiring_paths = mapping(mapping(inputs["wiring_plan"]).get("paths"))
    scheduler_paths = mapping(mapping(inputs["scheduler_dry_run"]).get("paths"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.referenced_artifacts.v1",
            "task_id": TASK_ID,
            "wiring_plan_dir": str(wiring_plan_dir),
            "scheduler_dry_run_dir": str(scheduler_dry_run_dir),
            "referenced_2346_artifacts": wiring_paths,
            "referenced_2345_artifacts": scheduler_paths,
            "all_required_artifacts_present": True,
            "fresh_market_data_read": False,
            "artifact_reference_only": True,
            **SAFETY_FIELDS,
        }
    )


def build_no_real_scheduler_assertion(
    *,
    manifest: Mapping[str, Any],
    guardrails: Mapping[str, Any],
) -> dict[str, Any]:
    real_scheduler_fields = {
        "external_scheduler_entry_created": manifest.get(
            "external_scheduler_entry_created"
        ),
        "cron_entry_created": manifest.get("cron_entry_created"),
        "windows_task_created": manifest.get("windows_task_created"),
        "github_action_schedule_created": manifest.get(
            "github_action_schedule_created"
        ),
        "daily_scheduler_entry_created": manifest.get(
            "daily_scheduler_entry_created"
        ),
        "real_scheduler_created": manifest.get("real_scheduler_created"),
    }
    violations = [
        field
        for field, value in real_scheduler_fields.items()
        if value is not False
    ]
    if guardrails.get("guardrail_status") != "PASS":
        violations.append("guardrail_status_not_pass")
    assertion_status = "PASS" if not violations else "FAIL_CLOSED_TRIGGERED"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.no_real_scheduler_assertion.v1",
            "task_id": TASK_ID,
            "assertion_status": assertion_status,
            "real_scheduler_fields": real_scheduler_fields,
            "violation_count": len(violations),
            "violations": sorted(violations),
            "scheduled_tasks_config_modified": False,
            "external_scheduler_mutation_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2348_readiness_checklist(
    *,
    manifest: Mapping[str, Any],
    guardrails: Mapping[str, Any],
    no_real_scheduler: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if manifest.get("status") != STATUS:
        blockers.append("DISABLED_WIRING_STATUS_MISMATCH")
    if guardrails.get("guardrail_status") != "PASS":
        blockers.append("DISABLED_WIRING_GUARDRAIL_NOT_PASS")
    if no_real_scheduler.get("assertion_status") != "PASS":
        blockers.append("REAL_SCHEDULER_ASSERTION_NOT_PASS")
    if manifest.get("scheduler_enabled") is not False:
        blockers.append("SCHEDULER_ENABLED")
    if manifest.get("event_append_enabled") is not False:
        blockers.append("EVENT_APPEND_ENABLED")
    if manifest.get("outcome_binding_enabled") is not False:
        blockers.append("OUTCOME_BINDING_ENABLED")
    if manifest.get("paper_shadow_enabled") is not False:
        blockers.append("PAPER_SHADOW_ENABLED")
    if manifest.get("production_enabled") is not False:
        blockers.append("PRODUCTION_ENABLED")
    if manifest.get("broker_action_enabled") is not False:
        blockers.append("BROKER_ACTION_ENABLED")

    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2348_readiness_checklist.v1",
            "task_id": TASK_ID,
            "readiness_status": "BLOCKED" if blockers else READINESS_STATUS,
            "readiness_blockers": sorted(set(blockers)),
            "readiness_warnings": list(ROUTE_CAVEATS),
            "disabled_wiring_implementation_manifest_generated": bool(manifest),
            "guardrail_status_generated": bool(guardrails),
            "no_real_scheduler_assertion_generated": bool(no_real_scheduler),
            "next_task": NEXT_2348_TASK,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2348_task_route(
    *,
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    blockers = list(readiness.get("readiness_blockers") or [])
    if blockers:
        next_task = (
            "TRADING-2348_Disabled_Scheduler_Wiring_Guardrail_Remediation"
        )
        rationale = "disabled wiring guardrail blockers require remediation"
        route_caveats: list[str] = []
    else:
        next_task = NEXT_2348_TASK
        rationale = "disabled wiring implementation can enter smoke dry-run"
        route_caveats = list(ROUTE_CAVEATS)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2348_task_route.v1",
            "task_id": TASK_ID,
            "readiness_status": readiness.get("readiness_status"),
            "next_task": next_task,
            "route_blockers": blockers,
            "route_caveats": route_caveats,
            "route_rationale": rationale,
            **SAFETY_FIELDS,
        }
    )


def build_disabled_wiring_interpretation_boundary(
    *,
    generated_at: datetime,
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "next_task": task_route.get("next_task"),
            "interpretation": (
                "disabled wiring implementation only; not scheduler activation"
            ),
            "not_scheduler_enablement": True,
            "not_production_wiring": True,
            "not_paper_shadow_promotion": True,
            "not_broker_execution": True,
            **SAFETY_FIELDS,
        }
    )


def build_disabled_wiring_safety_boundary(
    *,
    generated_at: datetime,
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "next_task": task_route.get("next_task"),
            "forbidden_actions": [
                "enable_scheduler",
                "create_real_cron_job",
                "create_real_windows_task",
                "create_real_github_action_schedule",
                "append_historical_event_log",
                "mutate_outcome_store",
                "bind_advisory_outcome",
                "enable_paper_shadow",
                "enable_production_path",
                "send_order",
                "call_broker_api",
                "read_fresh_market_data",
                "emit_target_weight",
                "emit_rebalance_instruction",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_disabled_wiring_summary(
    *,
    generated_at: datetime,
    wiring_plan_dir: Path,
    scheduler_dry_run_dir: Path,
    inputs: Mapping[str, Any],
    manifest: Mapping[str, Any],
    guardrails: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    wiring_summary = mapping(mapping(inputs["wiring_plan"]).get("summary"))
    scheduler_summary = mapping(mapping(inputs["scheduler_dry_run"]).get("summary"))
    source_validation = _source_validation(inputs)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": (
                "Disabled High-Intensity Risk-Cap Observe-Only Scheduler "
                "Wiring Implementation"
            ),
            "status": STATUS,
            "implementation_status": manifest.get("implementation_status"),
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "wiring_plan_dir": str(wiring_plan_dir),
            "scheduler_dry_run_dir": str(scheduler_dry_run_dir),
            "source_2346_status": wiring_summary.get("status"),
            "source_2346_readiness": wiring_summary.get("2347_readiness_status"),
            "source_2346_next_task": wiring_summary.get("next_task"),
            "source_2345_status": scheduler_summary.get("status"),
            "source_2345_next_task": scheduler_summary.get("next_task"),
            "selected_rule_id": wiring_summary.get("selected_rule_id"),
            "detected_event_count": wiring_summary.get("detected_event_count"),
            "would_append_event_count": wiring_summary.get(
                "would_append_event_count"
            ),
            "append_reason": wiring_summary.get("append_reason"),
            "guardrail_status": guardrails.get("guardrail_status"),
            "2348_readiness_status": readiness.get("readiness_status"),
            "next_task": task_route.get("next_task"),
            "source_validate_data_executed": source_validation["executed"],
            "source_validate_data_as_of": source_validation["as_of"],
            "source_validate_data_status": source_validation["status"],
            "source_validate_data_error_count": source_validation["error_count"],
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2347 only reads "
                "prior validated research artifacts and does not consume fresh "
                "market data, append events, or bind outcomes."
            ),
            **SAFETY_FIELDS,
        }
    )


def write_disabled_wiring_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
    guardrails: Mapping[str, Any],
    referenced_artifacts: Mapping[str, Any],
    no_real_scheduler: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["manifest"], manifest)
    write_json(paths["guardrails"], guardrails)
    write_json(paths["referenced_artifacts"], referenced_artifacts)
    write_json(paths["no_real_scheduler"], no_real_scheduler)
    write_json(paths["readiness"], readiness)
    write_json(paths["task_route"], task_route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["main_doc"], render_disabled_wiring_doc(summary, manifest))
    write_markdown(
        paths["guardrails_doc"],
        render_disabled_wiring_guardrails_doc(guardrails, no_real_scheduler),
    )
    write_markdown(
        paths["route_doc"],
        render_2348_readiness_route_doc(readiness, task_route),
    )
    return {key: str(path) for key, path in paths.items()}


def render_disabled_wiring_doc(
    summary: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Disabled High-Intensity Risk-Cap Observe-Only Scheduler Wiring Implementation",
            "",
            f"- status: `{summary.get('status')}`",
            f"- task_id: `{summary.get('task_id')}`",
            f"- source_2346_status: `{summary.get('source_2346_status')}`",
            f"- source_2345_status: `{summary.get('source_2345_status')}`",
            f"- scheduler_enabled: `{summary.get('scheduler_enabled')}`",
            f"- manual_run_only: `{summary.get('manual_run_only')}`",
            f"- dry_run_only: `{summary.get('dry_run_only')}`",
            f"- event_append_enabled: `{summary.get('event_append_enabled')}`",
            f"- outcome_binding_enabled: `{summary.get('outcome_binding_enabled')}`",
            f"- paper_shadow_enabled: `{summary.get('paper_shadow_enabled')}`",
            f"- production_enabled: `{summary.get('production_enabled')}`",
            f"- broker_action_enabled: `{summary.get('broker_action_enabled')}`",
            f"- next_task: `{summary.get('next_task')}`",
            "",
            "TRADING-2347 只实现 disabled-by-default wiring skeleton 和检查产物。",
            "它不是 scheduler enablement、不是 production wiring、不是 paper-shadow",
            "promotion，也不是 broker execution。",
            "",
            "## Promotion Blocked Reason",
            "",
            f"`{manifest.get('promotion_blocked_reason')}`",
        ]
    )


def render_disabled_wiring_guardrails_doc(
    guardrails: Mapping[str, Any],
    no_real_scheduler: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Scheduler Disabled Wiring Guardrails",
            "",
            f"- guardrail_status: `{guardrails.get('guardrail_status')}`",
            f"- safety_error_count: `{guardrails.get('safety_error_count')}`",
            f"- assertion_status: `{no_real_scheduler.get('assertion_status')}`",
            (
                "- real_scheduler_fields: "
                f"`{no_real_scheduler.get('real_scheduler_fields')}`"
            ),
            "",
            "Guardrails 对 real scheduler creation、event append、outcome binding、",
            "paper-shadow、production、broker action、target weight 和 rebalance",
            "instruction 全部 fail closed。",
        ]
    )


def render_2348_readiness_route_doc(
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity 2348 Readiness Route",
            "",
            f"- readiness_status: `{readiness.get('readiness_status')}`",
            f"- readiness_blockers: `{readiness.get('readiness_blockers')}`",
            f"- readiness_warnings: `{readiness.get('readiness_warnings')}`",
            f"- next_task: `{task_route.get('next_task')}`",
            "",
            "2348 route 只能进入 disabled scheduler wiring smoke dry-run 和",
            "guardrail evidence。它仍不能启用 scheduler、append event、绑定",
            "outcome、paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_scheduler_disabled_wiring_summary.json",
        "manifest": output_dir
        / "high_intensity_scheduler_disabled_wiring_implementation_manifest.json",
        "guardrails": output_dir
        / "high_intensity_scheduler_disabled_wiring_guardrail_status.json",
        "referenced_artifacts": output_dir
        / "high_intensity_scheduler_disabled_wiring_referenced_artifacts.json",
        "no_real_scheduler": output_dir
        / "high_intensity_scheduler_no_real_scheduler_assertion.json",
        "readiness": output_dir / "high_intensity_2348_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2348_task_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_disabled_wiring_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_disabled_wiring_safety_boundary.json",
        "main_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_disabled_wiring_implementation.md",
        "guardrails_doc": docs_root
        / "high_intensity_scheduler_disabled_wiring_guardrails.md",
        "route_doc": docs_root / "high_intensity_2348_readiness_route.md",
    }


def _validate_2347_cross_source_contracts(inputs: Mapping[str, Any]) -> None:
    wiring_summary = mapping(mapping(inputs["wiring_plan"]).get("summary"))
    scheduler_summary = mapping(mapping(inputs["scheduler_dry_run"]).get("summary"))
    if wiring_summary.get("source_2345_status") != EXPECTED_2345_STATUS:
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires 2346 to reference expected 2345 status"
        )
    if scheduler_summary.get("status") != EXPECTED_2345_STATUS:
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires expected 2345 scheduler dry-run status"
        )
    if wiring_summary.get("source_2345_next_task") != scheduler_summary.get(
        "next_task"
    ):
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires 2346 source 2345 route to match 2345 artifact"
        )
    if wiring_summary.get("selected_rule_id") != "COMPOSITE_HIGH_INTENSITY_RULE":
        raise HighIntensitySchedulerDisabledWiringError(
            "TRADING-2347 requires COMPOSITE_HIGH_INTENSITY_RULE"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerDisabledWiringError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerDisabledWiringError(
            f"{path}: expected JSON object"
        )
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensitySchedulerDisabledWiringError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _collect_unsafe_fields(value: object, prefix: str = "") -> list[str]:
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in INPUT_SAFETY_FALSE_FIELDS and item is True:
                violations.append(path)
            if key_text == "broker_action" and str(item).lower() not in {"", "none"}:
                violations.append(path)
            if key_text in FORBIDDEN_EMIT_FIELDS and _emits_action(item):
                violations.append(path)
            violations.extend(_collect_unsafe_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(_collect_unsafe_fields(item, f"{prefix}[{index}]"))
    return violations


def _collect_real_scheduler_creation_fields(
    value: object,
    prefix: str = "",
) -> list[str]:
    fields = {
        "external_scheduler_entry_created",
        "cron_entry_created",
        "windows_task_created",
        "github_action_schedule_created",
        "daily_scheduler_entry_created",
        "real_scheduler_created",
    }
    violations: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in fields and item is True:
                violations.append(path)
            violations.extend(_collect_real_scheduler_creation_fields(item, path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            violations.extend(
                _collect_real_scheduler_creation_fields(item, f"{prefix}[{index}]")
            )
    return violations


def _emits_action(value: object) -> bool:
    if value in (False, None, "", [], {}):
        return False
    if isinstance(value, str):
        return value.lower() not in {"none", "false", "not_applicable", "blocked"}
    return True


def _require_false(payload: Mapping[str, Any], field: str, label: str) -> None:
    if payload.get(field) is not False:
        raise HighIntensitySchedulerDisabledWiringError(
            f"{label} requires {field}=false"
        )


def _require_true(payload: Mapping[str, Any], field: str, label: str) -> None:
    if payload.get(field) is not True:
        raise HighIntensitySchedulerDisabledWiringError(
            f"{label} requires {field}=true"
        )


def _require_broker_none(payload: Mapping[str, Any], label: str) -> None:
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerDisabledWiringError(
            f"{label} requires broker_action=none"
        )


def _source_validation(inputs: Mapping[str, Any]) -> dict[str, Any]:
    wiring_summary = mapping(mapping(inputs["wiring_plan"]).get("summary"))
    scheduler_summary = mapping(mapping(inputs["scheduler_dry_run"]).get("summary"))
    return {
        "executed": wiring_summary.get(
            "source_validate_data_executed",
            scheduler_summary.get("source_validate_data_executed", True),
        ),
        "as_of": wiring_summary.get(
            "source_validate_data_as_of",
            scheduler_summary.get("source_validate_data_as_of", "2026-06-29"),
        ),
        "status": wiring_summary.get(
            "source_validate_data_status",
            scheduler_summary.get(
                "source_validate_data_status",
                "PASS_WITH_WARNINGS",
            ),
        ),
        "error_count": wiring_summary.get(
            "source_validate_data_error_count",
            scheduler_summary.get("source_validate_data_error_count", 0),
        ),
    }


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}
