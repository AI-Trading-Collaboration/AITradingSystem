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
    emits_action,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_disabled_wiring import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_DISABLED_WIRING_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_disabled_wiring import (
    NEXT_2348_TASK as EXPECTED_2347_NEXT_TASK,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_disabled_wiring import (
    READINESS_STATUS as EXPECTED_2347_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_disabled_wiring import (
    STATUS as EXPECTED_2347_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_disabled_wiring import (
    TASK_ID as SOURCE_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2348"
TASK_REGISTER_ID = (
    "TRADING-2348_DISABLED_SCHEDULER_WIRING_SMOKE_DRY_RUN_AND_GUARDRAIL_"
    "EVIDENCE"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_smoke_dry_run"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_smoke_dry_run"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_SMOKE_DRY_RUN_PASSED_WITH_CAVEATS_"
    "PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2349_WITH_CAVEATS"
NEXT_2349_TASK = (
    "TRADING-2349_Manual_Review_Promotion_Gate_For_Observe_Only_Scheduler"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

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
    "NO_REAL_SCHEDULER",
    "NO_EVENT_APPEND",
    "NO_OUTCOME_BINDING",
    "NO_PAPER_SHADOW",
    "NO_PRODUCTION",
    "NO_BROKER_ACTION",
    "MANUAL_REVIEW_REQUIRED_BEFORE_PROMOTION_GATE",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "smoke_dry_run_only": True,
    "disabled_wiring_source_only": True,
    "scheduler_enabled": False,
    "scheduler_default_enabled": False,
    "manual_run_only": True,
    "dry_run_only": True,
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
    "real_scheduler_created": False,
    "cron_created": False,
    "cron_entry_created": False,
    "windows_task_created": False,
    "github_actions_schedule_created": False,
    "github_action_schedule_created": False,
    "portfolio_effect": "none",
    "production_effect": "none",
    "manual_review_only": True,
}

FALSE_SAFETY_FIELDS = {
    "scheduler_enabled",
    "scheduler_default_enabled",
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
    "promotion_allowed",
    "broker_action_enabled",
    "broker_action_attempted",
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


class HighIntensitySchedulerSmokeDryRunError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_smoke_dry_run(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerSmokeDryRunError(
            f"high-intensity scheduler smoke dry-run only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_smoke_dry_run_inputs(
        disabled_wiring_dir=disabled_wiring_dir
    )
    source_assertion = build_source_artifact_assertion(inputs=inputs)
    guardrails = build_smoke_dry_run_guardrail_assertions(
        inputs=inputs,
        source_assertion=source_assertion,
    )
    side_effects = build_smoke_dry_run_side_effect_assertions(
        inputs=inputs,
        guardrails=guardrails,
    )
    evidence = build_smoke_dry_run_evidence(
        generated_at=generated_at,
        inputs=inputs,
        source_assertion=source_assertion,
        guardrails=guardrails,
        side_effects=side_effects,
    )
    route = build_high_intensity_2349_task_route(evidence=evidence)
    interpretation_boundary = build_smoke_dry_run_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_smoke_dry_run_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_smoke_dry_run_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        inputs=inputs,
        evidence=evidence,
        guardrails=guardrails,
        side_effects=side_effects,
        route=route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_smoke_dry_run_outputs(
        paths=paths,
        summary=summary,
        evidence=evidence,
        source_assertion=source_assertion,
        guardrails=guardrails,
        side_effects=side_effects,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_smoke_dry_run_inputs(
    *,
    disabled_wiring_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_summary.json",
        "manifest": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_implementation_manifest.json",
        "guardrails": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_guardrail_status.json",
        "referenced_artifacts": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_referenced_artifacts.json",
        "no_real_scheduler": disabled_wiring_dir
        / "high_intensity_scheduler_no_real_scheduler_assertion.json",
        "readiness": disabled_wiring_dir / "high_intensity_2348_readiness_checklist.json",
        "task_route": disabled_wiring_dir / "high_intensity_2348_task_route.json",
        "interpretation_boundary": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_interpretation_boundary.json",
        "safety_boundary": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2347 disabled wiring")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2347 {key}", payload)
        _validate_no_real_scheduler_creation(f"TRADING-2347 {key}", payload)
    _validate_2347_source_contracts(payloads)
    return {
        "disabled_wiring_dir": str(disabled_wiring_dir),
        "paths": _string_paths(paths),
        **payloads,
    }


def build_source_artifact_assertion(*, inputs: Mapping[str, Any]) -> dict[str, Any]:
    summary = mapping(inputs["summary"])
    guardrails = mapping(inputs["guardrails"])
    no_real_scheduler = mapping(inputs["no_real_scheduler"])
    readiness = mapping(inputs["readiness"])
    task_route = mapping(inputs["task_route"])
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.source_artifact_assertion.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_task_id": SOURCE_TASK_REGISTER_ID,
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_artifact_count": len(mapping(inputs.get("paths"))),
            "source_status": summary.get("status"),
            "source_next_task": task_route.get("next_task"),
            "source_readiness_status": readiness.get("readiness_status"),
            "source_guardrail_status": guardrails.get("guardrail_status"),
            "source_no_real_scheduler_assertion_status": no_real_scheduler.get(
                "assertion_status"
            ),
            "source_contract_status": "PASS",
            "source_validate_data_executed": summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": summary.get("source_validate_data_as_of"),
            "source_validate_data_status": summary.get("source_validate_data_status"),
            "source_validate_data_error_count": summary.get(
                "source_validate_data_error_count"
            ),
            **SAFETY_FIELDS,
        }
    )


def build_smoke_dry_run_guardrail_assertions(
    *,
    inputs: Mapping[str, Any],
    source_assertion: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = mapping(inputs["manifest"])
    payloads = [mapping(value) for value in inputs.values() if isinstance(value, Mapping)]
    unsafe_violations = sorted(
        {
            violation
            for payload in payloads
            for violation in _collect_unsafe_fields(payload)
        }
    )
    scheduler_violations = sorted(
        {
            violation
            for payload in payloads
            for violation in _collect_real_scheduler_creation_fields(payload)
        }
    )
    guardrail_assertions = {
        "scheduler_enabled": False,
        "manual_run_only": True,
        "dry_run_only": True,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "promotion_allowed": False,
    }
    expected_violations = _expected_guardrail_violations(manifest, guardrail_assertions)
    violations = sorted(set(unsafe_violations + scheduler_violations + expected_violations))
    status = "PASS" if not violations else "FAIL_CLOSED_TRIGGERED"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.guardrail_assertions.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_task_id": source_assertion.get("source_task_id"),
            "guardrail_status": status,
            "guardrail_assertions_passed": status == "PASS",
            "guardrail_assertions": guardrail_assertions,
            "unsafe_field_violations": unsafe_violations,
            "real_scheduler_creation_violations": scheduler_violations,
            "expected_guardrail_violations": expected_violations,
            "safety_error_count": len(violations),
            "block_if_scheduler_enabled": True,
            "block_if_event_append_enabled": True,
            "block_if_outcome_binding_enabled": True,
            "block_if_paper_shadow_enabled": True,
            "block_if_production_enabled": True,
            "block_if_broker_action_enabled": True,
            "block_if_promotion_allowed": True,
            **SAFETY_FIELDS,
        }
    )


def build_smoke_dry_run_side_effect_assertions(
    *,
    inputs: Mapping[str, Any],
    guardrails: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = mapping(inputs["manifest"])
    no_real_scheduler = mapping(inputs["no_real_scheduler"])
    side_effect_assertions = {
        "real_scheduler_created": False,
        "cron_created": False,
        "windows_task_created": False,
        "github_actions_schedule_created": False,
        "event_append_attempted": False,
        "outcome_binding_attempted": False,
        "paper_shadow_attempted": False,
        "production_attempted": False,
        "broker_action_attempted": False,
        "event_log_mutated": False,
        "outcome_store_mutated": False,
        "fresh_market_data_read": False,
    }
    source_fields = {
        "real_scheduler_created": manifest.get("real_scheduler_created"),
        "cron_created": manifest.get(
            "cron_created", manifest.get("cron_entry_created")
        ),
        "windows_task_created": manifest.get("windows_task_created"),
        "github_actions_schedule_created": manifest.get(
            "github_actions_schedule_created",
            manifest.get("github_action_schedule_created"),
        ),
        "event_log_mutated": manifest.get("historical_event_log_mutated"),
        "outcome_store_mutated": manifest.get("outcome_store_mutated"),
        "fresh_market_data_read": manifest.get("fresh_market_data_read"),
    }
    violations = [
        field
        for field, expected in side_effect_assertions.items()
        if source_fields.get(field, False) is not expected
    ]
    if no_real_scheduler.get("assertion_status") != "PASS":
        violations.append("source_no_real_scheduler_assertion_not_pass")
    if guardrails.get("guardrail_status") != "PASS":
        violations.append("guardrail_status_not_pass")
    status = "PASS" if not violations else "FAIL_CLOSED_TRIGGERED"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.side_effect_assertions.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "side_effect_status": status,
            "side_effect_assertions_passed": status == "PASS",
            "side_effect_assertions": side_effect_assertions,
            "source_side_effect_fields": source_fields,
            "side_effect_violation_count": len(violations),
            "side_effect_violations": sorted(violations),
            "scheduled_tasks_config_modified": False,
            "external_scheduler_mutation_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_smoke_dry_run_evidence(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    source_assertion: Mapping[str, Any],
    guardrails: Mapping[str, Any],
    side_effects: Mapping[str, Any],
) -> dict[str, Any]:
    summary = mapping(inputs["summary"])
    evidence_status = (
        STATUS
        if guardrails.get("guardrail_assertions_passed")
        and side_effects.get("side_effect_assertions_passed")
        else "OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_SMOKE_DRY_RUN_BLOCKED"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.evidence.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "status": evidence_status,
            "source_task": "TRADING-2347",
            "source_task_id": SOURCE_TASK_REGISTER_ID,
            "source_status": summary.get("status"),
            "source_artifacts_read": source_assertion.get("source_artifacts_read"),
            "source_artifacts_parsed": source_assertion.get("source_artifacts_parsed"),
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "guardrail_assertions": guardrails.get("guardrail_assertions"),
            "guardrail_assertions_passed": guardrails.get(
                "guardrail_assertions_passed"
            ),
            "side_effect_assertions": side_effects.get("side_effect_assertions"),
            "side_effect_assertions_passed": side_effects.get(
                "side_effect_assertions_passed"
            ),
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2349_TASK,
            "source_validate_data_executed": summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": summary.get("source_validate_data_as_of"),
            "source_validate_data_status": summary.get("source_validate_data_status"),
            "source_validate_data_error_count": summary.get(
                "source_validate_data_error_count"
            ),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2348 only reads "
                "prior validated TRADING-2347 disabled wiring artifacts and does "
                "not consume fresh market data, append events, or bind outcomes."
            ),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2349_task_route(
    *,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if evidence.get("status") != STATUS:
        blockers.append("SMOKE_DRY_RUN_EVIDENCE_NOT_PASS")
    if evidence.get("guardrail_assertions_passed") is not True:
        blockers.append("GUARDRAIL_ASSERTIONS_NOT_PASS")
    if evidence.get("side_effect_assertions_passed") is not True:
        blockers.append("SIDE_EFFECT_ASSERTIONS_NOT_PASS")
    if blockers:
        next_task = "TRADING-2349_Disabled_Scheduler_Wiring_Smoke_Dry_Run_Remediation"
        readiness = "BLOCKED"
        route_caveats: list[str] = []
        rationale = "smoke dry-run evidence failed; remediation required"
    else:
        next_task = NEXT_2349_TASK
        readiness = READINESS_STATUS
        route_caveats = list(ROUTE_CAVEATS)
        rationale = "disabled scheduler smoke dry-run can enter manual review gate"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2349_task_route.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": readiness,
            "next_task": next_task,
            "next_route": next_task,
            "route_blockers": blockers,
            "route_caveats": route_caveats,
            "route_rationale": rationale,
            **SAFETY_FIELDS,
        }
    )


def build_smoke_dry_run_interpretation_boundary(
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
                "disabled scheduler wiring smoke dry-run evidence only; not "
                "scheduler activation or promotion approval"
            ),
            "not_scheduler_enablement": True,
            "not_daily_scheduler_entry": True,
            "not_paper_shadow_promotion": True,
            "not_production_wiring": True,
            "not_broker_execution": True,
            **SAFETY_FIELDS,
        }
    )


def build_smoke_dry_run_safety_boundary(
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


def build_smoke_dry_run_summary(
    *,
    generated_at: datetime,
    disabled_wiring_dir: Path,
    inputs: Mapping[str, Any],
    evidence: Mapping[str, Any],
    guardrails: Mapping[str, Any],
    side_effects: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    summary = mapping(inputs["summary"])
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Scheduler Smoke Dry-Run "
                "Guardrail Evidence"
            ),
            "status": evidence.get("status"),
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "disabled_wiring_dir": str(disabled_wiring_dir),
            "source_task": evidence.get("source_task"),
            "source_task_id": evidence.get("source_task_id"),
            "source_status": summary.get("status"),
            "source_next_task": summary.get("next_task"),
            "source_guardrail_status": summary.get("guardrail_status"),
            "source_2348_readiness_status": summary.get("2348_readiness_status"),
            "selected_rule_id": summary.get("selected_rule_id"),
            "detected_event_count": summary.get("detected_event_count"),
            "would_append_event_count": summary.get("would_append_event_count"),
            "guardrail_assertions_passed": guardrails.get(
                "guardrail_assertions_passed"
            ),
            "side_effect_assertions_passed": side_effects.get(
                "side_effect_assertions_passed"
            ),
            "readiness": evidence.get("readiness"),
            "next_route": route.get("next_route"),
            "next_task": route.get("next_task"),
            "source_validate_data_executed": evidence.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": evidence.get("source_validate_data_as_of"),
            "source_validate_data_status": evidence.get("source_validate_data_status"),
            "source_validate_data_error_count": evidence.get(
                "source_validate_data_error_count"
            ),
            "aits_validate_data_rerun": evidence.get("aits_validate_data_rerun"),
            "aits_validate_data_rerun_reason": evidence.get(
                "aits_validate_data_rerun_reason"
            ),
            **SAFETY_FIELDS,
        }
    )


def write_smoke_dry_run_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    evidence: Mapping[str, Any],
    source_assertion: Mapping[str, Any],
    guardrails: Mapping[str, Any],
    side_effects: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["evidence"], evidence)
    write_json(paths["source_assertion"], source_assertion)
    write_json(paths["guardrails"], guardrails)
    write_json(paths["side_effects"], side_effects)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["evidence_doc"], render_smoke_dry_run_evidence_doc(evidence))
    write_markdown(
        paths["guardrails_doc"],
        render_smoke_dry_run_guardrails_doc(guardrails, side_effects),
    )
    write_markdown(paths["route_doc"], render_2349_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_smoke_dry_run_evidence_doc(evidence: Mapping[str, Any]) -> str:
    side_effects = mapping(evidence.get("side_effect_assertions"))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Smoke Dry-Run Evidence",
            "",
            f"- task_id: `{evidence.get('task_id')}`",
            f"- task_register_id: `{evidence.get('task_register_id')}`",
            f"- status: `{evidence.get('status')}`",
            f"- source_task: `{evidence.get('source_task')}`",
            (
                "- guardrail_assertions_passed: "
                f"`{evidence.get('guardrail_assertions_passed')}`"
            ),
            (
                "- side_effect_assertions_passed: "
                f"`{evidence.get('side_effect_assertions_passed')}`"
            ),
            f"- scheduler_enabled: `{evidence.get('scheduler_enabled')}`",
            f"- manual_run_only: `{evidence.get('manual_run_only')}`",
            f"- dry_run_only: `{evidence.get('dry_run_only')}`",
            f"- promotion_allowed: `{evidence.get('promotion_allowed')}`",
            f"- real_scheduler_created: `{side_effects.get('real_scheduler_created')}`",
            f"- event_append_attempted: `{side_effects.get('event_append_attempted')}`",
            (
                "- outcome_binding_attempted: "
                f"`{side_effects.get('outcome_binding_attempted')}`"
            ),
            f"- paper_shadow_attempted: `{side_effects.get('paper_shadow_attempted')}`",
            f"- production_attempted: `{side_effects.get('production_attempted')}`",
            f"- broker_action_attempted: `{side_effects.get('broker_action_attempted')}`",
            f"- readiness: `{evidence.get('readiness')}`",
            f"- next_route: `{evidence.get('next_route')}`",
            "",
            "TRADING-2348 只证明 2347 disabled wiring 在真实 CLI smoke dry-run",
            "下仍无调度和交易副作用。它不是 scheduler enablement、不是",
            "paper-shadow promotion、不是 production 或 broker execution。",
        ]
    )


def render_smoke_dry_run_guardrails_doc(
    guardrails: Mapping[str, Any],
    side_effects: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Scheduler Smoke Dry-Run Guardrails",
            "",
            f"- guardrail_status: `{guardrails.get('guardrail_status')}`",
            (
                "- guardrail_assertions_passed: "
                f"`{guardrails.get('guardrail_assertions_passed')}`"
            ),
            (
                "- side_effect_status: "
                f"`{side_effects.get('side_effect_status')}`"
            ),
            (
                "- side_effect_assertions_passed: "
                f"`{side_effects.get('side_effect_assertions_passed')}`"
            ),
            f"- safety_error_count: `{guardrails.get('safety_error_count')}`",
            (
                "- side_effect_violation_count: "
                f"`{side_effects.get('side_effect_violation_count')}`"
            ),
            "",
            "Guardrails 对 real scheduler creation、cron、Windows Task、GitHub",
            "Actions schedule、event append、outcome binding、paper-shadow、",
            "production、broker action、target weight 和 rebalance instruction",
            "全部 fail closed。",
        ]
    )


def render_2349_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2349 Manual Review Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2349 route 只能进入更严格的 manual review / dry-run promotion gate。",
            "它仍不能启用 scheduler、append event、绑定 outcome、paper-shadow、",
            "production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_scheduler_smoke_dry_run_summary.json",
        "evidence": output_dir
        / "high_intensity_scheduler_smoke_dry_run_evidence.json",
        "source_assertion": output_dir
        / "high_intensity_scheduler_smoke_dry_run_source_artifact_assertion.json",
        "guardrails": output_dir
        / "high_intensity_scheduler_smoke_dry_run_guardrail_assertions.json",
        "side_effects": output_dir
        / "high_intensity_scheduler_smoke_dry_run_side_effect_assertions.json",
        "route": output_dir / "high_intensity_2349_manual_review_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_smoke_dry_run_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_smoke_dry_run_safety_boundary.json",
        "evidence_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_smoke_dry_run_evidence.md",
        "guardrails_doc": docs_root
        / "high_intensity_scheduler_smoke_dry_run_guardrails.md",
        "route_doc": docs_root / "high_intensity_2349_manual_review_route.md",
    }


def _validate_2347_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    manifest = mapping(payloads["manifest"])
    guardrails = mapping(payloads["guardrails"])
    no_real_scheduler = mapping(payloads["no_real_scheduler"])
    readiness = mapping(payloads["readiness"])
    task_route = mapping(payloads["task_route"])

    if summary.get("status") != EXPECTED_2347_STATUS:
        raise HighIntensitySchedulerSmokeDryRunError(
            f"TRADING-2348 requires 2347 status {EXPECTED_2347_STATUS}"
        )
    if manifest.get("status") != EXPECTED_2347_STATUS:
        raise HighIntensitySchedulerSmokeDryRunError(
            "TRADING-2348 requires 2347 implementation manifest status"
        )
    if summary.get("next_task") != EXPECTED_2347_NEXT_TASK:
        raise HighIntensitySchedulerSmokeDryRunError(
            "TRADING-2348 requires 2347 summary to route to smoke dry-run"
        )
    if task_route.get("next_task") != EXPECTED_2347_NEXT_TASK:
        raise HighIntensitySchedulerSmokeDryRunError(
            "TRADING-2348 requires 2347 task route to smoke dry-run"
        )
    if readiness.get("readiness_status") != EXPECTED_2347_READINESS:
        raise HighIntensitySchedulerSmokeDryRunError(
            "TRADING-2348 requires 2347 readiness READY_FOR_2348_WITH_CAVEATS"
        )
    if guardrails.get("guardrail_status") != "PASS":
        raise HighIntensitySchedulerSmokeDryRunError(
            "TRADING-2348 requires 2347 guardrail status PASS"
        )
    if no_real_scheduler.get("assertion_status") != "PASS":
        raise HighIntensitySchedulerSmokeDryRunError(
            "TRADING-2348 requires 2347 no-real-scheduler assertion PASS"
        )
    if summary.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerSmokeDryRunError(
            "TRADING-2348 requires inherited source validate-data execution"
        )
    if summary.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerSmokeDryRunError(
            "TRADING-2348 requires inherited source validate-data error_count=0"
        )

    for label, payload in (
        ("TRADING-2347 summary", summary),
        ("TRADING-2347 manifest", manifest),
        ("TRADING-2347 guardrails", guardrails),
        ("TRADING-2347 readiness", readiness),
        ("TRADING-2347 task route", task_route),
    ):
        _require_false(payload, "scheduler_enabled", label)
        if "scheduler_default_enabled" in payload:
            _require_false(payload, "scheduler_default_enabled", label)
        _require_true(payload, "manual_run_only", label)
        _require_true(payload, "dry_run_only", label)
        for field in (
            "event_append_enabled",
            "event_append_executed",
            "outcome_binding_enabled",
            "outcome_binding_executed",
            "paper_shadow_enabled",
            "paper_shadow_allowed",
            "production_enabled",
            "production_allowed",
            "broker_action_enabled",
            "promotion_allowed",
        ):
            if field in payload:
                _require_false(payload, field, label)
        _require_broker_none(payload, label)


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerSmokeDryRunError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerSmokeDryRunError(
            f"{path}: expected JSON object"
        )
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensitySchedulerSmokeDryRunError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _validate_no_real_scheduler_creation(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_real_scheduler_creation_fields(payload)
    if violations:
        raise HighIntensitySchedulerSmokeDryRunError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _collect_unsafe_fields(value: object, prefix: str = "") -> list[str]:
    return collect_unsafe_fields(
        value,
        false_fields=FALSE_SAFETY_FIELDS,
        forbidden_emit_fields=FORBIDDEN_EMIT_FIELDS,
        prefix=prefix,
    )


def _collect_real_scheduler_creation_fields(
    value: object,
    prefix: str = "",
) -> list[str]:
    return collect_real_scheduler_creation_fields(
        value,
        prefix=prefix,
        real_scheduler_fields=REAL_SCHEDULER_FIELDS,
    )


def _expected_guardrail_violations(
    payload: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> list[str]:
    violations: list[str] = []
    for field, expected_value in expected.items():
        if payload.get(field) is not expected_value:
            violations.append(field)
    return violations


def _emits_action(value: object) -> bool:
    return emits_action(value)


def _require_false(payload: Mapping[str, Any], field: str, label: str) -> None:
    if payload.get(field) is not False:
        raise HighIntensitySchedulerSmokeDryRunError(
            f"{label} requires {field}=false"
        )


def _require_true(payload: Mapping[str, Any], field: str, label: str) -> None:
    if payload.get(field) is not True:
        raise HighIntensitySchedulerSmokeDryRunError(
            f"{label} requires {field}=true"
        )


def _require_broker_none(payload: Mapping[str, Any], label: str) -> None:
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerSmokeDryRunError(
            f"{label} requires broker_action=none"
        )


def _string_paths(paths: Mapping[str, Path]) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}
