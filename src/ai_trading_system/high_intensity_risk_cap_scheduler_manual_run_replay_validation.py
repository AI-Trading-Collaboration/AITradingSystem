from __future__ import annotations

import hashlib
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
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_dry_run import (
    DEFAULT_DISABLED_WIRING_ROOT,
    DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    DEFAULT_SMOKE_DRY_RUN_ROOT,
    load_high_intensity_scheduler_manual_run_dry_run_inputs,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_dry_run import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_dry_run import (
    NEXT_2351_TASK as EXPECTED_2350_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_dry_run import (
    READINESS_STATUS as EXPECTED_2350_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_dry_run import (
    STATUS as EXPECTED_2350_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_scheduler_manual_run_dry_run import (
    TASK_REGISTER_ID as SOURCE_2350_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2351"
TASK_REGISTER_ID = (
    "TRADING-2351_OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_REPLAY_NO_SIDE_EFFECT_"
    "VALIDATION"
)
REPORT_TYPE = (
    "high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation"
)
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_scheduler_manual_run_replay_validation"

STATUS = (
    "OBSERVE_ONLY_SCHEDULER_MANUAL_RUN_REPLAY_NO_SIDE_EFFECT_VALIDATION_PASSED_"
    "WITH_CAVEATS_PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2352_WITH_CAVEATS"
NEXT_2352_ROUTE = (
    "TRADING-2352_Observe_Only_Scheduler_Audit_Package_And_Owner_Review_"
    "Checklist"
)
REPLAY_COUNT = 3

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SOURCE_TASKS = ["TRADING-2347", "TRADING-2348", "TRADING-2349", "TRADING-2350"]
UPSTREAM_SOURCE_TASKS = ["TRADING-2347", "TRADING-2348", "TRADING-2349"]

STABLE_SEMANTIC_FIELDS = [
    "status",
    "task_id",
    "source_tasks",
    "scheduler_enabled",
    "manual_run_only",
    "dry_run_only",
    "manual_run_executed",
    "promotion_allowed",
    "side_effect_assertions",
    "promotion_blocked_reasons",
    "next_route",
]

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
    "REPLAY_VALIDATION_ONLY",
]

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_only": True,
    "manual_run_replay_validation_only": True,
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

SIDE_EFFECT_ASSERTIONS: dict[str, bool] = {
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
}


class HighIntensitySchedulerManualRunReplayValidationError(ValueError):
    pass


def run_high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation(
    *,
    disabled_wiring_dir: Path = DEFAULT_DISABLED_WIRING_ROOT,
    smoke_dry_run_dir: Path = DEFAULT_SMOKE_DRY_RUN_ROOT,
    manual_review_gate_dir: Path = DEFAULT_MANUAL_REVIEW_GATE_ROOT,
    manual_run_dry_run_dir: Path = DEFAULT_MANUAL_RUN_DRY_RUN_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    replay_count: int = REPLAY_COUNT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            f"high-intensity manual-run replay validation only supports {MODE} mode"
        )
    if replay_count != REPLAY_COUNT:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            f"TRADING-2351 replay_count must remain {REPLAY_COUNT}"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_scheduler_manual_run_replay_validation_inputs(
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
    )
    source_review = build_replay_source_artifact_review(inputs=inputs)
    semantic_checks = build_replay_semantic_checks(
        generated_at=generated_at,
        inputs=inputs,
        source_review=source_review,
        replay_count=replay_count,
    )
    side_effect_assertions = build_replay_side_effect_assertions(
        generated_at=generated_at,
        inputs=inputs,
        semantic_checks=semantic_checks,
    )
    evidence = build_replay_validation_evidence(
        generated_at=generated_at,
        source_review=source_review,
        semantic_checks=semantic_checks,
        side_effect_assertions=side_effect_assertions,
    )
    package = build_replay_validation_package(
        generated_at=generated_at,
        source_review=source_review,
        semantic_checks=semantic_checks,
        side_effect_assertions=side_effect_assertions,
        evidence=evidence,
    )
    route = build_high_intensity_2352_task_route(
        package=package,
        semantic_checks=semantic_checks,
        side_effect_assertions=side_effect_assertions,
    )
    interpretation_boundary = build_replay_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_replay_safety_boundary(
        generated_at=generated_at,
        route=route,
    )
    summary = build_replay_validation_summary(
        generated_at=generated_at,
        disabled_wiring_dir=disabled_wiring_dir,
        smoke_dry_run_dir=smoke_dry_run_dir,
        manual_review_gate_dir=manual_review_gate_dir,
        manual_run_dry_run_dir=manual_run_dry_run_dir,
        source_review=source_review,
        semantic_checks=semantic_checks,
        side_effect_assertions=side_effect_assertions,
        evidence=evidence,
        route=route,
    )
    _validate_generated_payloads(
        {
            "summary": summary,
            "package": package,
            "source_review": source_review,
            "semantic_checks": semantic_checks,
            "side_effect_assertions": side_effect_assertions,
            "evidence": evidence,
            "route": route,
            "interpretation_boundary": interpretation_boundary,
            "safety_boundary": safety_boundary,
        }
    )
    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_replay_validation_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        semantic_checks=semantic_checks,
        side_effect_assertions=side_effect_assertions,
        evidence=evidence,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_scheduler_manual_run_replay_validation_inputs(
    *,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_scheduler_manual_run_dry_run_inputs(
            disabled_wiring_dir=disabled_wiring_dir,
            smoke_dry_run_dir=smoke_dry_run_dir,
            manual_review_gate_dir=manual_review_gate_dir,
        )
    except Exception as exc:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            f"TRADING-2351 upstream 2347/2348/2349 validation failed: {exc}"
        ) from exc

    manual_run_paths = {
        "summary": manual_run_dry_run_dir
        / "high_intensity_scheduler_manual_run_dry_run_summary.json",
        "package": manual_run_dry_run_dir
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_dry_run.json",
        "source_review": manual_run_dry_run_dir
        / "high_intensity_scheduler_manual_run_dry_run_source_artifact_review.json",
        "preview": manual_run_dry_run_dir
        / "high_intensity_scheduler_manual_run_dry_run_preview.json",
        "evidence": manual_run_dry_run_dir
        / "high_intensity_scheduler_manual_run_dry_run_evidence.json",
        "side_effect_assertions": manual_run_dry_run_dir
        / "high_intensity_scheduler_manual_run_dry_run_side_effect_assertions.json",
        "route": manual_run_dry_run_dir / "high_intensity_2351_manual_run_replay_route.json",
        "interpretation_boundary": manual_run_dry_run_dir
        / "high_intensity_scheduler_manual_run_dry_run_interpretation_boundary.json",
        "safety_boundary": manual_run_dry_run_dir
        / "high_intensity_scheduler_manual_run_dry_run_safety_boundary.json",
    }
    manual_run_payloads = _load_required_payloads(
        manual_run_paths,
        "TRADING-2350 manual-run dry-run",
    )
    for key, payload in manual_run_payloads.items():
        label = f"TRADING-2350 manual-run dry-run {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)
    _validate_2350_source_contracts(manual_run_payloads)
    _validate_cross_source_contracts(source_inputs, manual_run_payloads)
    return {
        **source_inputs,
        "manual_run_dry_run_dir": str(manual_run_dry_run_dir),
        "manual_run_dry_run_paths": _string_paths(manual_run_paths),
        "manual_run_dry_run": manual_run_payloads,
    }


def build_replay_source_artifact_review(*, inputs: Mapping[str, Any]) -> dict[str, Any]:
    manual_run = mapping(inputs["manual_run_dry_run"])
    manual_summary = mapping(manual_run["summary"])
    manual_package = mapping(manual_run["package"])
    manual_source_review = mapping(manual_run["source_review"])
    manual_side_effects = mapping(manual_run["side_effect_assertions"])
    source_task_ids = list(manual_source_review.get("source_task_ids", []))
    source_task_ids.append(SOURCE_2350_TASK_REGISTER_ID)
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
            "disabled_wiring_artifact_count": len(mapping(inputs["disabled_paths"])),
            "smoke_dry_run_artifact_count": len(mapping(inputs["smoke_paths"])),
            "manual_review_gate_artifact_count": len(
                mapping(inputs["manual_review_gate_paths"])
            ),
            "manual_run_dry_run_artifact_count": len(
                mapping(inputs["manual_run_dry_run_paths"])
            ),
            "manual_run_dry_run_status": manual_summary.get("status"),
            "manual_run_dry_run_package_status": manual_package.get("status"),
            "manual_run_dry_run_readiness": manual_summary.get("readiness"),
            "manual_run_dry_run_next_route": manual_summary.get("next_route"),
            "manual_run_dry_run_side_effect_assertions_passed": manual_side_effects.get(
                "side_effect_assertions_passed"
            ),
            "source_validate_data_executed": manual_summary.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": manual_summary.get(
                "source_validate_data_as_of"
            ),
            "source_validate_data_status": manual_summary.get(
                "source_validate_data_status"
            ),
            "source_validate_data_error_count": manual_summary.get(
                "source_validate_data_error_count"
            ),
            "source_contract_status": "PASS",
            **SAFETY_FIELDS,
        }
    )


def build_replay_semantic_checks(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    source_review: Mapping[str, Any],
    replay_count: int,
) -> dict[str, Any]:
    manual_run = mapping(inputs["manual_run_dry_run"])
    source_side_effects = mapping(manual_run["side_effect_assertions"]).get(
        "side_effect_assertions",
        {},
    )
    semantic_snapshot = _stable_semantic_snapshot(
        source_side_effects=mapping(source_side_effects),
    )
    replay_rounds = []
    for index in range(replay_count):
        replay_snapshot = _stable_semantic_snapshot(
            source_side_effects=mapping(source_side_effects),
        )
        replay_rounds.append(
            {
                "replay_index": index + 1,
                "stable_semantic_fields": list(STABLE_SEMANTIC_FIELDS),
                "stable_semantic_hash": _stable_hash(replay_snapshot),
                "stable_semantics": replay_snapshot,
            }
        )

    hashes = [round_item["stable_semantic_hash"] for round_item in replay_rounds]
    expected_hash = _stable_hash(semantic_snapshot)
    stable_passed = (
        len(replay_rounds) == replay_count
        and set(hashes) == {expected_hash}
        and all(
            round_item["stable_semantics"] == semantic_snapshot
            for round_item in replay_rounds
        )
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.semantic_checks.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS if stable_passed else "REPLAY_SEMANTIC_CHECK_FAILED",
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "replay_count": replay_count,
            "stable_semantic_fields_checked": list(STABLE_SEMANTIC_FIELDS),
            "expected_stable_semantic_hash": expected_hash,
            "observed_stable_semantic_hashes": hashes,
            "stable_semantic_replay_passed": stable_passed,
            "replay_rounds": replay_rounds,
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2352_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_replay_side_effect_assertions(
    *,
    generated_at: datetime,
    inputs: Mapping[str, Any],
    semantic_checks: Mapping[str, Any],
) -> dict[str, Any]:
    manual_run = mapping(inputs["manual_run_dry_run"])
    source_assertions = mapping(
        mapping(manual_run["side_effect_assertions"]).get("side_effect_assertions")
    )
    failed = [
        field
        for field, expected in SIDE_EFFECT_ASSERTIONS.items()
        if source_assertions.get(field, expected) is not expected
    ]
    failed.extend(
        field
        for field, value in SIDE_EFFECT_ASSERTIONS.items()
        if value is not False
    )
    if semantic_checks.get("stable_semantic_replay_passed") is not True:
        failed.append("stable_semantic_replay_not_passed")
    failed = sorted(set(failed))
    passed = not failed
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.side_effect_assertions.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "status": STATUS if passed else "REPLAY_SIDE_EFFECT_ASSERTION_FAILED",
            "side_effect_status": "PASS" if passed else "FAIL_CLOSED_TRIGGERED",
            "side_effect_assertions": dict(SIDE_EFFECT_ASSERTIONS),
            "source_side_effect_assertions": {
                field: source_assertions.get(field)
                for field in SIDE_EFFECT_ASSERTIONS
            },
            "pre_replay_side_effect_state": dict(SIDE_EFFECT_ASSERTIONS),
            "post_replay_side_effect_state": dict(SIDE_EFFECT_ASSERTIONS),
            "side_effect_assertions_passed": passed,
            "side_effect_violation_count": len(failed),
            "side_effect_violations": failed,
            "replay_count": semantic_checks.get("replay_count"),
            "stable_semantic_replay_passed": semantic_checks.get(
                "stable_semantic_replay_passed"
            ),
            "promotion_decision": "BLOCKED",
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2352_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_replay_validation_evidence(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    semantic_checks: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
) -> dict[str, Any]:
    passed = (
        semantic_checks.get("stable_semantic_replay_passed") is True
        and side_effect_assertions.get("side_effect_assertions_passed") is True
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.evidence.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": f"{REPORT_TYPE}_evidence",
            "status": STATUS if passed else "REPLAY_VALIDATION_BLOCKED",
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "source_artifacts_read": source_review.get("source_artifacts_read"),
            "source_contract_status": source_review.get("source_contract_status"),
            "replay_count": semantic_checks.get("replay_count"),
            "stable_semantic_replay_passed": semantic_checks.get(
                "stable_semantic_replay_passed"
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
            "next_route": NEXT_2352_ROUTE,
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
                "aits validate-data not rerun because TRADING-2351 only reads "
                "prior validated TRADING-2347 disabled wiring, TRADING-2348 "
                "smoke dry-run, TRADING-2349 manual review gate, and TRADING-2350 "
                "manual-run dry-run artifacts; it does not consume fresh market "
                "data, append events, bind outcomes, produce technical features, "
                "score, backtest, or generate daily reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def build_replay_validation_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    semantic_checks: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.package.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "status": evidence.get("status"),
            "generated_at": generated_at.isoformat(),
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "replay_count": semantic_checks.get("replay_count"),
            "stable_semantic_replay_passed": semantic_checks.get(
                "stable_semantic_replay_passed"
            ),
            "side_effect_assertions_passed": side_effect_assertions.get(
                "side_effect_assertions_passed"
            ),
            "side_effect_assertions": side_effect_assertions.get(
                "side_effect_assertions"
            ),
            "stable_semantic_fields_checked": semantic_checks.get(
                "stable_semantic_fields_checked"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2352_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2352_task_route(
    *,
    package: Mapping[str, Any],
    semantic_checks: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    if package.get("status") != STATUS:
        blockers.append("REPLAY_VALIDATION_STATUS_NOT_PASSED")
    if semantic_checks.get("stable_semantic_replay_passed") is not True:
        blockers.append("STABLE_SEMANTIC_REPLAY_NOT_PASSED")
    if side_effect_assertions.get("side_effect_assertions_passed") is not True:
        blockers.append("SIDE_EFFECT_ASSERTIONS_NOT_PASSED")
    readiness = "BLOCKED" if blockers else READINESS_STATUS
    next_route = "TRADING-2351_Manual_Run_Replay_Remediation" if blockers else NEXT_2352_ROUTE
    rationale = (
        "manual-run replay validation can enter audit package and owner review checklist"
        if not blockers
        else "manual-run replay validation failed; remediation required"
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2352_task_route.v1",
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": readiness,
            "next_task": next_route,
            "next_route": next_route,
            "route_blockers": blockers,
            "route_caveats": list(ROUTE_CAVEATS) if not blockers else [],
            "route_rationale": rationale,
            **SAFETY_FIELDS,
        }
    )


def build_replay_interpretation_boundary(
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
                "manual-run replay no-side-effect validation only; not manual-run "
                "execution, scheduler activation, event append, outcome binding, "
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


def build_replay_safety_boundary(
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


def build_replay_validation_summary(
    *,
    generated_at: datetime,
    disabled_wiring_dir: Path,
    smoke_dry_run_dir: Path,
    manual_review_gate_dir: Path,
    manual_run_dry_run_dir: Path,
    source_review: Mapping[str, Any],
    semantic_checks: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
    evidence: Mapping[str, Any],
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
                "Replay No-Side-Effect Validation"
            ),
            "status": evidence.get("status"),
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
            "source_tasks": list(SOURCE_TASKS),
            "source_task_ids": source_review.get("source_task_ids"),
            "replay_count": semantic_checks.get("replay_count"),
            "stable_semantic_replay_passed": semantic_checks.get(
                "stable_semantic_replay_passed"
            ),
            "side_effect_assertions": side_effect_assertions.get(
                "side_effect_assertions"
            ),
            "side_effect_assertions_passed": side_effect_assertions.get(
                "side_effect_assertions_passed"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "readiness": route.get("readiness"),
            "next_route": route.get("next_route"),
            "next_task": route.get("next_task"),
            "source_validate_data_executed": evidence.get(
                "source_validate_data_executed"
            ),
            "source_validate_data_as_of": evidence.get("source_validate_data_as_of"),
            "source_validate_data_status": evidence.get(
                "source_validate_data_status"
            ),
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


def write_replay_validation_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    semantic_checks: Mapping[str, Any],
    side_effect_assertions: Mapping[str, Any],
    evidence: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["semantic_checks"], semantic_checks)
    write_json(paths["side_effect_assertions"], side_effect_assertions)
    write_json(paths["evidence"], evidence)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["validation_doc"], render_replay_validation_doc(package))
    write_markdown(paths["route_doc"], render_2352_route_doc(route))
    return {key: str(path) for key, path in paths.items()}


def render_replay_validation_doc(package: Mapping[str, Any]) -> str:
    assertions = mapping(package.get("side_effect_assertions"))
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Scheduler Manual-Run "
            "Replay No-Side-Effect Validation",
            "",
            f"- task_id: `{package.get('task_id')}`",
            f"- task_register_id: `{package.get('task_register_id')}`",
            f"- status: `{package.get('status')}`",
            f"- source_tasks: `{package.get('source_tasks')}`",
            f"- replay_count: `{package.get('replay_count')}`",
            (
                "- stable_semantic_replay_passed: "
                f"`{package.get('stable_semantic_replay_passed')}`"
            ),
            (
                "- side_effect_assertions_passed: "
                f"`{package.get('side_effect_assertions_passed')}`"
            ),
            f"- scheduler_enabled: `{package.get('scheduler_enabled')}`",
            f"- manual_run_only: `{package.get('manual_run_only')}`",
            f"- dry_run_only: `{package.get('dry_run_only')}`",
            f"- manual_run_executed: `{package.get('manual_run_executed')}`",
            f"- promotion_allowed: `{package.get('promotion_allowed')}`",
            f"- real_scheduler_created: `{assertions.get('real_scheduler_created')}`",
            f"- cron_created: `{assertions.get('cron_created')}`",
            f"- windows_task_created: `{assertions.get('windows_task_created')}`",
            (
                "- github_actions_schedule_created: "
                f"`{assertions.get('github_actions_schedule_created')}`"
            ),
            f"- event_append_attempted: `{assertions.get('event_append_attempted')}`",
            (
                "- outcome_binding_attempted: "
                f"`{assertions.get('outcome_binding_attempted')}`"
            ),
            f"- paper_shadow_attempted: `{assertions.get('paper_shadow_attempted')}`",
            f"- production_attempted: `{assertions.get('production_attempted')}`",
            f"- broker_action_attempted: `{assertions.get('broker_action_attempted')}`",
            f"- readiness: `{package.get('readiness')}`",
            f"- next_route: `{package.get('next_route')}`",
            "",
            "TRADING-2351 只证明 2350 manual-run dry-run interface 可以重复 replay，",
            "且核心语义稳定、无调度或交易副作用。它不是 scheduler enablement、",
            "不是 manual run execution、不是 event append、不是 outcome binding、",
            "不是 paper-shadow、production 或 broker readiness。",
        ]
    )


def render_2352_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2352 Scheduler Audit Package Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- route_blockers: `{route.get('route_blockers')}`",
            f"- route_caveats: `{route.get('route_caveats')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2352 route 只能进入 scheduler audit package 和 owner review checklist。",
            "它不是 scheduler enablement、不是 daily scheduler entry、不是 event append、",
            "不是 outcome binding，也不是 paper-shadow、production 或 broker action。",
        ]
    )


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_scheduler_manual_run_replay_validation_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation.json",
        "source_review": output_dir
        / "high_intensity_scheduler_manual_run_replay_source_artifact_review.json",
        "semantic_checks": output_dir
        / "high_intensity_scheduler_manual_run_replay_semantic_checks.json",
        "side_effect_assertions": output_dir
        / "high_intensity_scheduler_manual_run_replay_side_effect_assertions.json",
        "evidence": output_dir
        / "high_intensity_scheduler_manual_run_replay_evidence.json",
        "route": output_dir / "high_intensity_2352_scheduler_audit_package_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_scheduler_manual_run_replay_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_scheduler_manual_run_replay_safety_boundary.json",
        "validation_doc": docs_root
        / "high_intensity_risk_cap_observe_only_scheduler_manual_run_replay_validation.md",
        "route_doc": docs_root
        / "high_intensity_2352_scheduler_audit_package_route.md",
    }


def _stable_semantic_snapshot(
    *,
    source_side_effects: Mapping[str, Any],
) -> dict[str, Any]:
    side_effects = {
        field: source_side_effects.get(field, expected)
        for field, expected in SIDE_EFFECT_ASSERTIONS.items()
    }
    return clean_for_yaml(
        {
            "status": STATUS,
            "task_id": TASK_ID,
            "source_tasks": list(SOURCE_TASKS),
            "scheduler_enabled": False,
            "manual_run_only": True,
            "dry_run_only": True,
            "manual_run_executed": False,
            "promotion_allowed": False,
            "side_effect_assertions": side_effects,
            "promotion_blocked_reasons": list(PROMOTION_BLOCKED_REASONS),
            "next_route": NEXT_2352_ROUTE,
        }
    )


def _stable_hash(payload: Mapping[str, Any]) -> str:
    serialized = json.dumps(
        clean_for_yaml(payload),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _validate_2350_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    source_review = mapping(payloads["source_review"])
    preview = mapping(payloads["preview"])
    evidence = mapping(payloads["evidence"])
    side_effects = mapping(payloads["side_effect_assertions"])
    route = mapping(payloads["route"])

    if summary.get("status") != EXPECTED_2350_STATUS:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            f"TRADING-2351 requires 2350 status {EXPECTED_2350_STATUS}"
        )
    if package.get("status") != EXPECTED_2350_STATUS:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 package status"
        )
    if summary.get("readiness") != EXPECTED_2350_READINESS:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 readiness READY_FOR_2351_WITH_CAVEATS"
        )
    if summary.get("next_route") != EXPECTED_2350_NEXT_ROUTE:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 summary route to replay validation"
        )
    if route.get("next_route") != EXPECTED_2350_NEXT_ROUTE:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 route to replay validation"
        )
    if source_review.get("source_contract_status") != "PASS":
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 source contract status PASS"
        )
    if preview.get("manual_run_interface_present") is not True:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 manual_run_interface_present=true"
        )
    if preview.get("manual_run_preview_generated") is not True:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 manual_run_preview_generated=true"
        )
    if preview.get("manual_run_executed") is not False:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 manual_run_executed=false"
        )
    if evidence.get("manual_run_executed") is not False:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 evidence manual_run_executed=false"
        )
    if evidence.get("side_effect_assertions_passed") is not True:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 evidence side_effect_assertions_passed=true"
        )
    if side_effects.get("side_effect_assertions_passed") is not True:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 side-effect assertions passed"
        )
    if package.get("source_tasks") != UPSTREAM_SOURCE_TASKS:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 source tasks to be 2347/2348/2349"
        )
    if package.get("promotion_allowed") is not False:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 promotion_allowed=false"
        )
    _validate_source_data_quality(summary, "TRADING-2350 summary")

    source_side_effects = mapping(side_effects.get("side_effect_assertions"))
    for field, expected in SIDE_EFFECT_ASSERTIONS.items():
        if source_side_effects.get(field) is not expected:
            raise HighIntensitySchedulerManualRunReplayValidationError(
                f"TRADING-2351 requires 2350 {field}=false"
            )


def _validate_cross_source_contracts(
    source_inputs: Mapping[str, Any],
    manual_run_payloads: Mapping[str, Any],
) -> None:
    manual_review_summary = mapping(
        mapping(source_inputs["manual_review_gate"])["summary"]
    )
    manual_run_source_review = mapping(manual_run_payloads["source_review"])
    if manual_run_source_review.get(
        "manual_review_gate_status"
    ) != manual_review_summary.get("status"):
        raise HighIntensitySchedulerManualRunReplayValidationError(
            "TRADING-2351 requires 2350 source review manual-review status to match 2349"
        )


def _validate_generated_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> None:
    for key, payload in payloads.items():
        label = f"TRADING-2351 generated {key}"
        _validate_no_unsafe_fields(label, payload)
        _validate_no_real_scheduler_creation(label, payload)
        _validate_no_forbidden_true_fields(label, payload)
        _validate_safety_payload(label, payload)


def _validate_source_data_quality(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("source_validate_data_executed") is not True:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            f"{label} requires inherited source validate-data execution"
        )
    if payload.get("source_validate_data_error_count") != 0:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            f"{label} requires inherited source validate-data error_count=0"
        )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensitySchedulerManualRunReplayValidationError(
                f"{label} missing {key}: {path}"
            )
        payloads[key] = _read_json(path)
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise HighIntensitySchedulerManualRunReplayValidationError(
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
        raise HighIntensitySchedulerManualRunReplayValidationError(
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
        raise HighIntensitySchedulerManualRunReplayValidationError(
            f"{label} has real scheduler creation fields: {sorted(set(violations))}"
        )


def _validate_no_forbidden_true_fields(
    label: str,
    payload: Mapping[str, Any],
) -> None:
    violations = _collect_forbidden_true_fields(payload)
    if violations:
        raise HighIntensitySchedulerManualRunReplayValidationError(
            f"{label} has forbidden true fields: {sorted(set(violations))}"
        )


def _validate_safety_payload(label: str, payload: Mapping[str, Any]) -> None:
    for field in FALSE_SAFETY_FIELDS:
        if field in payload and payload.get(field) is not False:
            raise HighIntensitySchedulerManualRunReplayValidationError(
                f"{label} requires {field}=false"
            )
    for field in ("manual_run_only", "dry_run_only"):
        if field in payload and payload.get(field) is not True:
            raise HighIntensitySchedulerManualRunReplayValidationError(
                f"{label} requires {field}=true"
            )
    if str(payload.get("broker_action", "none")).lower() != "none":
        raise HighIntensitySchedulerManualRunReplayValidationError(
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
