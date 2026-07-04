from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
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
    TASK_ID as SOURCE_2347_TASK_REGISTER_ID,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    DEFAULT_DISABLED_WIRING_ROOT,
    _collect_real_scheduler_creation_fields,
    _collect_unsafe_fields,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SMOKE_DRY_RUN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    NEXT_2349_TASK as EXPECTED_2348_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    READINESS_STATUS as EXPECTED_2348_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    STATUS as EXPECTED_2348_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_smoke_dry_run import (
    TASK_REGISTER_ID as SOURCE_2348_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2349"
TASK_REGISTER_ID = (
    "TRADING-2349_MANUAL_REVIEW_PROMOTION_GATE_FOR_OBSERVE_ONLY_SCHEDULER"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_scheduler_manual_review_gate"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_manual_review_gate"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_MANUAL_REVIEW_GATE_READY_WITH_CAVEATS_"
    "PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2350_WITH_CAVEATS"
NEXT_2350_TASK = "TRADING-2350_Observe_Only_Scheduler_Manual_Run_Interface_Dry_Run"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SOURCE_TASKS = ["TRADING-2347", "TRADING-2348"]

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
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "manual_review_gate_only": True,
    "prior_validated_artifacts_only": True,
    "source_artifacts_only": True,
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
}


class HighIntensitySchedulerManualReviewGateError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_manual_review_gate(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerManualReviewGateError(
            f"high-intensity manual review gate only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_manual_review_gate_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
    )
    source_review = build_manual_review_source_artifact_review(inputs=inputs)
    gate_package = build_manual_review_gate_package(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
    )
    promotion_decision = build_manual_review_promotion_decision(
        generated_at=generated_at,
        gate_package=gate_package,
    )
    route = build_high_intensity_2350_task_route(
        gate_package=gate_package,
        promotion_decision=promotion_decision,
    )
    interpretation_boundary = build_manual_review_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_manual_review_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_manual_review_gate_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        gate_package=gate_package,
        source_review=source_review,
        promotion_decision=promotion_decision,
        route=route,
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_manual_review_gate_outputs(
        paths=paths,
        summary=summary,
        gate_package=gate_package,
        source_review=source_review,
        promotion_decision=promotion_decision,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_manual_review_gate_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
) -> dict[str, Any]:
    disabled_paths = {
        "summary": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_summary.json",
        "manifest": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_implementation_manifest.json",
        "guardrails": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_guardrail_status.json",
        "no_real_scheduler": disabled_wiring_dir
        / "high_intensity_scheduler_no_real_scheduler_assertion.json",
        "readiness": disabled_wiring_dir / "high_intensity_2348_readiness_checklist.json",
        "task_route": disabled_wiring_dir / "high_intensity_2348_task_route.json",
        "safety_boundary": disabled_wiring_dir
        / "high_intensity_scheduler_disabled_wiring_safety_boundary.json",
    }
    smoke_paths = {
        "summary": smoke_dry_run_dir
        / "high_intensity_scheduler_smoke_dry_run_summary.json",
        "evidence": smoke_dry_run_dir
        / "high_intensity_scheduler_smoke_dry_run_evidence.json",
        "source_assertion": smoke_dry_run_dir
        / "high_intensity_scheduler_smoke_dry_run_source_artifact_assertion.json",
        "guardrails": smoke_dry_run_dir
        / "high_intensity_scheduler_smoke_dry_run_guardrail_assertions.json",
        "side_effects": smoke_dry_run_dir
        / "high_intensity_scheduler_smoke_dry_run_side_effect_assertions.json",
        "route": smoke_dry_run_dir / "high_intensity_2349_manual_review_route.json",
        "safety_boundary": smoke_dry_run_dir
        / "high_intensity_scheduler_smoke_dry_run_safety_boundary.json",
    }
    disabled_payloads = _load_required_payloads(
        disabled_paths,
        "TRADING-2347 disabled wiring",
    )
    smoke_payloads = _load_required_payloads(
        smoke_paths,
        "TRADING-2348 smoke dry-run",
    )
    for label, payloads in (
        ("TRADING-2347 disabled wiring", disabled_payloads),
        ("TRADING-2348 smoke dry-run", smoke_payloads),
    ):
        for key, payload in payloads.items():
            _validate_no_unsafe_fields(f"{label} {key}", payload)
            _validate_no_real_scheduler_creation(f"{label} {key}", payload)
            _validate_no_forbidden_true_fields(f"{label} {key}", payload)
            _validate_safety_payload(f"{label} {key}", payload)
    _validate_2347_source_contracts(disabled_payloads)
    _validate_2348_source_contracts(smoke_payloads)
    _validate_cross_source_contracts(disabled_payloads, smoke_payloads)
    return {
        "disabled_wiring_dir": str(disabled_wiring_dir),
        "smoke_dry_run_dir": str(smoke_dry_run_dir),
        "disabled_paths": _string_paths(disabled_paths),
        "smoke_paths": _string_paths(smoke_paths),
        "disabled_wiring": disabled_payloads,
        "smoke_dry_run": smoke_payloads,
    }


def build_manual_review_source_artifact_review(
    *,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    disabled = mapping(inputs["disabled_wiring"])
    smoke = mapping(inputs["smoke_dry_run"])
    disabled_summary = mapping(disabled["summary"])
    smoke_summary = mapping(smoke["summary"])
    smoke_evidence = mapping(smoke["evidence"])
    smoke_guardrails = mapping(smoke["guardrails"])
    smoke_side_effects = mapping(smoke["side_effects"])
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.source_artifact_review.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": [
                SOURCE_2347_TASK_REGISTER_ID,
                SOURCE_2348_TASK_REGISTER_ID,
            ],
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "disabled_wiring_artifacts_read": True,
            "smoke_dry_run_artifacts_read": True,
            "disabled_wiring_artifact_count": len(mapping(inputs["disabled_paths"])),
            "smoke_dry_run_artifact_count": len(mapping(inputs["smoke_paths"])),
            "disabled_wiring_status": disabled_summary.get("status"),
            "disabled_wiring_readiness": mapping(disabled["readiness"]).get(
                "readiness_status"
            ),
            "disabled_wiring_next_task": mapping(disabled["task_route"]).get(
                "next_task"
            ),
            "smoke_dry_run_status": smoke_summary.get("status"),
            "smoke_dry_run_evidence_status": smoke_evidence.get("status"),
            "smoke_dry_run_readiness": smoke_evidence.get("readiness"),
            "smoke_dry_run_next_route": smoke_evidence.get("next_route"),
            "guardrail_evidence_status": smoke_guardrails.get("guardrail_status"),
            "guardrail_assertions_passed": smoke_guardrails.get(
                "guardrail_assertions_passed"
            ),
            "side_effect_status": smoke_side_effects.get("side_effect_status"),
            "side_effect_assertions_passed": smoke_side_effects.get(
                "side_effect_assertions_passed"
            ),
            "source_validate_data_executed": smoke_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": smoke_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": smoke_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": smoke_summary.get(
                "source_validate_data_error_count"
            ),
            "source_contract_status": "PASS",
            **SAFETY_FIELDS,
        }
    )


def build_manual_review_gate_package(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    smoke = mapping(inputs["smoke_dry_run"])
    smoke_summary = mapping(smoke["summary"])
    review_findings = {
        "disabled_wiring_present": True,
        "smoke_dry_run_passed": True,
        "guardrail_evidence_present": True,
        "side_effect_assertions_present": True,
        "promotion_evidence_sufficient_for_enablement": False,
    }
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.gate_package.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "status": STATUS,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "manual_review_required": True,
            "review_findings": review_findings,
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2350_TASK,
            "source_review_status": source_review.get("source_contract_status"),
            "source_validate_data_executed": smoke_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": smoke_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": smoke_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": smoke_summary.get(
                "source_validate_data_error_count"
            ),
            "aits_validate_data_rerun": False,
            "aits_validate_data_rerun_reason": (
                "aits validate-data not rerun because TRADING-2349 only reads "
                "prior validated TRADING-2347 disabled wiring and TRADING-2348 "
                "smoke dry-run artifacts; it does not consume fresh market "
                "data, append events, or bind outcomes."
            ),
            **SAFETY_FIELDS,
        }
    )


def build_manual_review_promotion_decision(
    *,
    generated_at: datetime,
    gate_package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.promotion_decision.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "manual_review_required": True,
            "owner_review_required": True,
            "owner_review_completed": False,
            "manual_review_completed": False,
            "enablement_allowed_after_2349": False,
            "paper_shadow_allowed_after_2349": False,
            "production_allowed_after_2349": False,
            "broker_action_allowed_after_2349": False,
            "only_next_stage_allowed": READINESS_STATUS,
            "next_route": gate_package.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2350_task_route(
    *,
    gate_package: Mapping[str, Any],
    promotion_decision: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if gate_package.get("status") != STATUS:
        blockers.append("MANUAL_REVIEW_GATE_STATUS_NOT_READY")
    if promotion_decision.get("promotion_decision") != "BLOCKED":
        blockers.append("PROMOTION_DECISION_NOT_BLOCKED")
    if promotion_decision.get("promotion_allowed") is not False:
        blockers.append("PROMOTION_ALLOWED_NOT_FALSE")
    readiness = "BLOCKED" if blockers else READINESS_STATUS
    next_task = "TRADING-2349_Manual_Review_Gate_Remediation" if blockers else NEXT_2350_TASK
    rationale = (
        "manual review gate can enter manual-run interface dry-run review only"
        if not blockers
        else "manual review gate failed; remediation required"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2350_task_route.v1",
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


def build_manual_review_interpretation_boundary(
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
                "manual review promotion gate package only; not scheduler "
                "activation, paper-shadow approval, production readiness, or "
                "broker execution"
            ),
            "not_scheduler_enablement": True,
            "not_daily_scheduler_entry": True,
            "not_paper_shadow_promotion": True,
            "not_production_wiring": True,
            "not_broker_execution": True,
            **SAFETY_FIELDS,
        }
    )


def build_manual_review_safety_boundary(
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


def build_manual_review_gate_summary(
    *,
    generated_at: datetime,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    gate_package: Mapping[str, Any],
    source_review: Mapping[str, Any],
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
                "High-Intensity Risk-Cap Observe-Only Scheduler Manual Review "
                "Promotion Gate"
            ),
            "status": gate_package.get("status"),
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "disabled_wiring_dir": str(disabled_wiring_dir),
            "smoke_dry_run_dir": str(smoke_dry_run_dir),
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "disabled_wiring_status": source_review.get("disabled_wiring_status"),
            "smoke_dry_run_status": source_review.get("smoke_dry_run_status"),
            "guardrail_assertions_passed": source_review.get(
                "guardrail_assertions_passed"
            ),
            "side_effect_assertions_passed": source_review.get(
                "side_effect_assertions_passed"
            ),
            "promotion_decision": promotion_decision.get("promotion_decision"),
            "promotion_blocked_reasons": promotion_decision.get(
                "promotion_blocked_reasons"
            ),
            "review_findings": gate_package.get("review_findings"),
            "readiness": gate_package.get("readiness"),
            "next_route": route.get("next_route"),
            "next_task": route.get("next_task"),
            "source_validate_data_executed": gate_package.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": gate_package.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": gate_package.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": gate_package.get(
                "source_validate_data_error_count"
            ),
            "aits_validate_data_rerun": gate_package.get("aits_validate_data_rerun"),
            "aits_validate_data_rerun_reason": gate_package.get(
                "aits_validate_data_rerun_reason"
            ),
            **SAFETY_FIELDS,
        }
    )


def write_manual_review_gate_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    gate_package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    promotion_decision: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["gate_package"], gate_package)
    write_json(paths["source_review"], source_review)
    write_json(paths["promotion_decision"], promotion_decision)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["gate_doc"], render_manual_review_gate_doc(gate_package))
    write_markdown(paths["route_doc"], render_2350_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_manual_review_gate_doc(gate_package: Mapping[str, Any]) -> str:
    findings = mapping(gate_package.get("review_findings"))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Manual Review Gate",
            "",
            f"- task_id: `{gate_package.get('task_id')}`",
            f"- task_register_id: `{gate_package.get('task_register_id')}`",
            f"- status: `{gate_package.get('status')}`",
            f"- source_tasks: `{gate_package.get('source_tasks')}`",
            f"- promotion_decision: `{gate_package.get('promotion_decision')}`",
            f"- promotion_allowed: `{gate_package.get('promotion_allowed')}`",
            f"- manual_review_required: `{gate_package.get('manual_review_required')}`",
            f"- scheduler_enabled: `{gate_package.get('scheduler_enabled')}`",
            f"- manual_run_only: `{gate_package.get('manual_run_only')}`",
            f"- dry_run_only: `{gate_package.get('dry_run_only')}`",
            f"- paper_shadow_enabled: `{gate_package.get('paper_shadow_enabled')}`",
            f"- production_enabled: `{gate_package.get('production_enabled')}`",
            f"- broker_action_enabled: `{gate_package.get('broker_action_enabled')}`",
            (
                "- disabled_wiring_present: "
                f"`{findings.get('disabled_wiring_present')}`"
            ),
            f"- smoke_dry_run_passed: `{findings.get('smoke_dry_run_passed')}`",
            (
                "- guardrail_evidence_present: "
                f"`{findings.get('guardrail_evidence_present')}`"
            ),
            (
                "- side_effect_assertions_present: "
                f"`{findings.get('side_effect_assertions_present')}`"
            ),
            (
                "- promotion_evidence_sufficient_for_enablement: "
                f"`{findings.get('promotion_evidence_sufficient_for_enablement')}`"
            ),
            f"- readiness: `{gate_package.get('readiness')}`",
            f"- next_route: `{gate_package.get('next_route')}`",
            "",
            "TRADING-2349 只把 2347 disabled wiring 和 2348 smoke dry-run",
            "evidence 汇总成 owner 人工评审 gate。当前结论仍为 promotion",
            "blocked；它不是 scheduler enablement、不是 paper-shadow、不是",
            "production，也不是 broker execution。",
        ]
    )


def render_2350_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2350 Manual-Run Interface Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2350 route 只能进入 observe-only scheduler manual-run interface",
            "dry-run review。任何 scheduler enablement、event append、outcome",
            "binding、paper-shadow、production 或 broker action 仍需要后续",
            "人工批准和单独任务。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_scheduler_manual_review_gate_summary.json",
        "gate_package": output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_manual_review_gate.json",
        "source_review": output_dir
        / "high_intensity_scheduler_manual_review_gate_source_artifact_review.json",
        "promotion_decision": output_dir
        / "high_intensity_scheduler_manual_review_gate_promotion_decision.json",
        "route": output_dir / "high_intensity_2350_manual_run_interface_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_manual_review_gate_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_manual_review_gate_safety_boundary.json",
        "gate_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_manual_review_gate.md",
        "route_doc": docs_root / "high_intensity_2350_manual_run_interface_route.md",
    }


def _validate_2347_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    manifest = mapping(payloads["manifest"])
    guardrails = mapping(payloads["guardrails"])
    no_real_scheduler = mapping(payloads["no_real_scheduler"])
    readiness = mapping(payloads["readiness"])
    task_route = mapping(payloads["task_route"])
    if summary.get("status") != EXPECTED_2347_STATUS:
        raise HighIntensitySchedulerManualReviewGateError(
            f"TRADING-2349 requires 2347 status {EXPECTED_2347_STATUS}"
        )
    if manifest.get("status") != EXPECTED_2347_STATUS:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2347 implementation manifest status"
        )
    if guardrails.get("guardrail_status") != "PASS":
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2347 guardrail status PASS"
        )
    if no_real_scheduler.get("assertion_status") != "PASS":
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2347 no-real-scheduler assertion PASS"
        )
    if readiness.get("readiness_status") != EXPECTED_2347_READINESS:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2347 readiness READY_FOR_2348_WITH_CAVEATS"
        )
    if task_route.get("next_task") != EXPECTED_2347_NEXT_TASK:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2347 route to TRADING-2348"
        )
    _validate_source_data_quality(summary, "TRADING-2347 summary")


def _validate_2348_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    evidence = mapping(payloads["evidence"])
    source_assertion = mapping(payloads["source_assertion"])
    guardrails = mapping(payloads["guardrails"])
    side_effects = mapping(payloads["side_effects"])
    route = mapping(payloads["route"])
    if summary.get("status") != EXPECTED_2348_STATUS:
        raise HighIntensitySchedulerManualReviewGateError(
            f"TRADING-2349 requires 2348 status {EXPECTED_2348_STATUS}"
        )
    if evidence.get("status") != EXPECTED_2348_STATUS:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 evidence status"
        )
    if evidence.get("readiness") != EXPECTED_2348_READINESS:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 readiness READY_FOR_2349_WITH_CAVEATS"
        )
    if evidence.get("next_route") != EXPECTED_2348_NEXT_ROUTE:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 evidence next route to manual review"
        )
    if route.get("next_route") != EXPECTED_2348_NEXT_ROUTE:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 route to manual review"
        )
    if source_assertion.get("source_artifacts_read") is not True:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 source artifacts read"
        )
    if guardrails.get("guardrail_assertions_passed") is not True:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 guardrail assertions passed"
        )
    if side_effects.get("side_effect_assertions_passed") is not True:
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 side-effect assertions passed"
        )
    _validate_source_data_quality(summary, "TRADING-2348 summary")


def _validate_cross_source_contracts(
    disabled_payloads: Mapping[str, Any],
    smoke_payloads: Mapping[str, Any],
) -> None:
    disabled_summary = mapping(disabled_payloads["summary"])
    smoke_summary = mapping(smoke_payloads["summary"])
    smoke_evidence = mapping(smoke_payloads["evidence"])
    if smoke_summary.get("source_status") != disabled_summary.get("status"):
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 summary source_status to match 2347"
        )
    if smoke_evidence.get("source_status") != disabled_summary.get("status"):
        raise HighIntensitySchedulerManualReviewGateError(
            "TRADING-2349 requires 2348 evidence source_status to match 2347"
        )


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerManualReviewGateError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerManualReviewGateError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerManualReviewGateError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerManualReviewGateError(
            f"{path}: expected JSON object"
        )
    return payload


def _validate_no_unsafe_fields(label: str, payload: Mapping[str, Any]) -> None:
    violations = _collect_unsafe_fields(payload)
    if violations:
        raise HighIntensitySchedulerManualReviewGateError(
            f"{label} has unsafe fields: {sorted(set(violations))}"
        )


def _validate_no_real_scheduler_creation(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_real_scheduler_creation_fields(payload)
    if violations:
        raise HighIntensitySchedulerManualReviewGateError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerManualReviewGateError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in (
        "scheduler_enabled",
        "scheduler_default_enabled",
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
            raise HighIntensitySchedulerManualReviewGateError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerManualReviewGateError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerManualReviewGateError(
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
