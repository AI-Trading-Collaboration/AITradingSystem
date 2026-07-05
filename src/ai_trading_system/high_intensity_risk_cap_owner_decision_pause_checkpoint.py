from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PROMOTION_BLOCKER_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    GUARDRAIL_SUMMARY,
    SIDE_EFFECT_SUMMARY,
    load_high_intensity_promotion_blocker_matrix_inputs,
)
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    NEXT_2363_ROUTE as EXPECTED_2362_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    READINESS_STATUS as EXPECTED_2362_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    SAFETY_FIELDS as SOURCE_SAFETY_FIELDS,
)
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    SOURCE_TASKS as SOURCE_2362_TASKS,
)
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    STATUS as EXPECTED_2362_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_consolidated_promotion_blocker_matrix import (
    TASK_REGISTER_ID as SOURCE_2362_TASK_REGISTER_ID,
)
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
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PAPER_SHADOW_SCOPE_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PRODUCTION_BROKER_ROOT,
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

TASK_ID = "TRADING-2363"
TASK_REGISTER_ID = "TRADING-2363_OBSERVE_ONLY_OWNER_DECISION_PAUSE_CHECKPOINT"
REPORT_TYPE = "high_intensity_risk_cap_observe_only_owner_decision_pause_checkpoint"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_owner_decision_pause_checkpoint"

STATUS = (
    "OBSERVE_ONLY_OWNER_DECISION_PAUSE_CHECKPOINT_RECORDED_WITH_CAVEATS_"
    "PROMOTION_BLOCKED"
)
READINESS_STATUS = "PAUSE_FOR_OWNER_REASSESSMENT_WITH_CAVEATS"
OWNER_DECISION = "KEEP_DISABLED_AND_PAUSE_FOR_REASSESSMENT"
NEXT_OWNER_REASSESSMENT_ROUTE = (
    "OWNER_REASSESSMENT_REQUIRED_BEFORE_ADDITIONAL_SCHEDULER_GUARDRAIL_TASKS"
)

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
SOURCE_TASKS = [*SOURCE_2362_TASKS, "TRADING-2362"]

SAFETY_FIELDS: dict[str, Any] = {
    **SOURCE_SAFETY_FIELDS,
    "owner_decision": OWNER_DECISION,
    "promotion_blocker_matrix_only": False,
    "owner_decision_pause_checkpoint_only": True,
    "continue_linear_guardrail_tasks": False,
}

RECOMMENDED_OWNER_REASSESSMENT_TOPICS = [
    "whether_to_start_hardening_implementation",
    "whether_to_return_to_strategy_research",
    "whether_scheduler_enablement_is_still_a_priority",
    "whether_paper_shadow_is_needed_before_more_guardrail_work",
    "whether_current_engineering_depth_is_becoming_overbuilt",
]

REASSESSMENT_OPTIONS = [
    {
        "option": "A",
        "label": "pause scheduler line and return to strategy research",
        "continues_linear_guardrail_tasks": False,
        "broker_or_production_allowed": False,
    },
    {
        "option": "B",
        "label": "start hardening implementation for scheduler kill-switch / disabled enforcement",
        "continues_linear_guardrail_tasks": False,
        "broker_or_production_allowed": False,
    },
    {
        "option": "C",
        "label": "define limited paper-shadow proposal, still no broker",
        "continues_linear_guardrail_tasks": False,
        "broker_or_production_allowed": False,
    },
    {
        "option": "D",
        "label": "stop scheduler work entirely until strategy signal quality improves",
        "continues_linear_guardrail_tasks": False,
        "broker_or_production_allowed": False,
    },
]

EXPLICIT_NON_GOALS = [
    "This task does not create TRADING-2364.",
    "This task does not start scheduler hardening implementation.",
    "This task does not enable scheduler, event append, outcome binding, or paper-shadow.",
    "This task does not enable production or broker action.",
    "This task only records the pause checkpoint and owner reassessment route.",
]


class HighIntensityOwnerDecisionPauseCheckpointError(
    HighIntensityGuardrailClosureError
):
    pass


def run_high_intensity_risk_cap_observe_only_owner_decision_pause_checkpoint(
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
    production_broker_dir: Path = DEFAULT_PRODUCTION_BROKER_ROOT,
    promotion_blocker_dir: Path = DEFAULT_PROMOTION_BLOCKER_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityOwnerDecisionPauseCheckpointError(
            f"owner decision pause checkpoint only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_owner_decision_pause_checkpoint_inputs(
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
        production_broker_dir=production_broker_dir,
        promotion_blocker_dir=promotion_blocker_dir,
    )
    source_review = build_source_artifact_review(inputs=inputs)
    decision_record = build_owner_decision_record(
        generated_at=generated_at,
        source_review=source_review,
    )
    reassessment_plan = build_owner_reassessment_plan(
        generated_at=generated_at,
        source_review=source_review,
    )
    rationale = build_blocked_promotion_rationale(
        generated_at=generated_at,
        source_review=source_review,
    )
    package = build_package(
        generated_at=generated_at,
        source_review=source_review,
        decision_record=decision_record,
        reassessment_plan=reassessment_plan,
        rationale=rationale,
    )
    interpretation_boundary = build_interpretation_boundary(
        generated_at=generated_at,
        package=package,
    )
    safety_boundary = build_safety_boundary(
        generated_at=generated_at,
        package=package,
    )
    summary = build_summary(
        generated_at=generated_at,
        promotion_blocker_dir=promotion_blocker_dir,
        source_review=source_review,
        package=package,
    )
    payloads = {
        "summary": summary,
        "package": package,
        "source_review": source_review,
        "decision_record": decision_record,
        "reassessment_plan": reassessment_plan,
        "rationale": rationale,
        "interpretation_boundary": interpretation_boundary,
        "safety_boundary": safety_boundary,
    }
    validate_generated_payloads(payloads, TASK_ID)
    validate_pause_contracts(package)
    paths = build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        decision_record=decision_record,
        reassessment_plan=reassessment_plan,
        rationale=rationale,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_owner_decision_pause_checkpoint_inputs(
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
    production_broker_dir: Path,
    promotion_blocker_dir: Path,
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_promotion_blocker_matrix_inputs(
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
            production_broker_dir=production_broker_dir,
        )
    except Exception as exc:  # noqa: BLE001
        raise HighIntensityOwnerDecisionPauseCheckpointError(
            f"TRADING-2363 source chain invalid before 2362: {exc}"
        ) from exc

    paths = {
        "summary": promotion_blocker_dir
        / "high_intensity_promotion_blocker_matrix_summary.json",
        "package": promotion_blocker_dir
        / "high_intensity_risk_cap_observe_only_promotion_blocker_matrix.json",
        "source_review": promotion_blocker_dir
        / "high_intensity_promotion_blocker_matrix_source_artifact_review.json",
        "blocker_matrix": promotion_blocker_dir
        / "high_intensity_consolidated_blocker_matrix.json",
        "safety_matrix": promotion_blocker_dir
        / "high_intensity_safety_evidence_matrix.json",
        "future_gap": promotion_blocker_dir
        / "high_intensity_promotion_blocker_future_evidence_gap.json",
        "rationale": promotion_blocker_dir
        / "high_intensity_promotion_blocker_blocked_promotion_rationale.json",
        "route": promotion_blocker_dir
        / "high_intensity_2363_owner_decision_pause_route.json",
        "interpretation_boundary": promotion_blocker_dir
        / "high_intensity_promotion_blocker_interpretation_boundary.json",
        "safety_boundary": promotion_blocker_dir
        / "high_intensity_promotion_blocker_safety_boundary.json",
    }
    promotion_blocker_payloads = load_required_payloads(paths, "TRADING-2363")
    try:
        validate_2362_source_contracts(promotion_blocker_payloads)
    except HighIntensityGuardrailClosureError as exc:
        raise HighIntensityOwnerDecisionPauseCheckpointError(str(exc)) from exc
    return {
        **source_inputs,
        "promotion_blocker_matrix": promotion_blocker_payloads,
        "promotion_blocker_matrix_paths": string_paths(paths),
    }


def validate_2362_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    route = mapping(payloads["route"])
    source_review = mapping(payloads["source_review"])
    for key, payload in payloads.items():
        validate_safety_payload(f"TRADING-2363 source 2362 {key}", mapping(payload))
    require_equal(summary, "status", EXPECTED_2362_STATUS, "TRADING-2363")
    require_equal(package, "status", EXPECTED_2362_STATUS, "TRADING-2363")
    require_equal(summary, "readiness", EXPECTED_2362_READINESS, "TRADING-2363")
    require_equal(summary, "next_route", EXPECTED_2362_NEXT_ROUTE, "TRADING-2363")
    require_equal(route, "next_route", EXPECTED_2362_NEXT_ROUTE, "TRADING-2363")
    require_equal(summary, "source_tasks", SOURCE_2362_TASKS, "TRADING-2363")
    require_true(summary, "evidence_chain_complete", "TRADING-2363")
    require_true(summary, "consolidated_blocker_matrix_ready", "TRADING-2363")
    require_true(summary, "safety_evidence_matrix_ready", "TRADING-2363")
    require_false(summary, "promotion_allowed", "TRADING-2363")
    require_equal(source_review, "source_contract_status", "PASS", "TRADING-2363")
    validate_source_data_quality(summary, "TRADING-2363 2362 summary")


def build_source_artifact_review(*, inputs: Mapping[str, Any]) -> dict[str, Any]:
    source = mapping(inputs["promotion_blocker_matrix"])
    summary = mapping(source["summary"])
    source_review = mapping(source["source_review"])
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_owner_decision_pause_"
                "checkpoint.source_artifact_review.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": [
                *source_review.get("source_task_ids", []),
                SOURCE_2362_TASK_REGISTER_ID,
            ],
            "source_task_evidence": source_evidence_rows_with_previous(
                source_review,
                task="TRADING-2362",
                status=summary.get("status"),
                evidence="consolidated promotion blocker matrix present",
            ),
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_contract_status": "PASS",
            "promotion_blocker_status": summary.get("status"),
            "promotion_blocker_readiness": summary.get("readiness"),
            "promotion_blocker_next_route": summary.get("next_route"),
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


def build_owner_decision_record(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_owner_decision_pause_"
                "checkpoint.owner_decision_record.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "owner_decision_recorded": True,
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "pause_checkpoint_recorded": True,
            "continue_linear_guardrail_tasks": False,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_OWNER_REASSESSMENT_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_owner_reassessment_plan(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_owner_decision_pause_"
                "checkpoint.owner_reassessment_plan.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "pause_checkpoint_recorded": True,
            "continue_linear_guardrail_tasks": False,
            "recommended_owner_reassessment_topics": (
                RECOMMENDED_OWNER_REASSESSMENT_TOPICS
            ),
            "reassessment_options": REASSESSMENT_OPTIONS,
            "next_route": NEXT_OWNER_REASSESSMENT_ROUTE,
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
                "high_intensity_risk_cap_observe_only_owner_decision_pause_"
                "checkpoint.blocked_promotion_rationale.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "pause_checkpoint_recorded": True,
            "continue_linear_guardrail_tasks": False,
            "blocked_promotion_reasons": [
                "OWNER_DECISION_PAUSE_FOR_REASSESSMENT",
                "PROMOTION_REMAINS_BLOCKED",
                "ALL_RUNTIME_PATHS_DISABLED",
                "NO_OWNER_REASSESSMENT_YET",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    decision_record: Mapping[str, Any],
    reassessment_plan: Mapping[str, Any],
    rationale: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_owner_decision_pause_"
                "checkpoint.package.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Owner Decision Pause "
                "Checkpoint"
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
            "owner_decision_recorded": decision_record.get(
                "owner_decision_recorded"
            ),
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "pause_checkpoint_recorded": decision_record.get(
                "pause_checkpoint_recorded"
            ),
            "continue_linear_guardrail_tasks": False,
            "recommended_owner_reassessment_topics": reassessment_plan.get(
                "recommended_owner_reassessment_topics"
            ),
            "reassessment_options": reassessment_plan.get("reassessment_options"),
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "blocked_promotion_reasons": rationale.get("blocked_promotion_reasons"),
            "explicit_non_goals": EXPLICIT_NON_GOALS,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_OWNER_REASSESSMENT_ROUTE,
            "next_task": NEXT_OWNER_REASSESSMENT_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_interpretation_boundary(
    *,
    generated_at: datetime,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_owner_decision_pause_"
                "checkpoint.interpretation_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "interpretation": (
                "TRADING-2363 records a pause checkpoint and owner reassessment route."
            ),
            "not_2364_route": True,
            "not_hardening_implementation": True,
            "not_scheduler_enablement": True,
            "not_promotion_clearance": True,
            "readiness": package.get("readiness"),
            "next_route": package.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_safety_boundary(
    *,
    generated_at: datetime,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_owner_decision_pause_"
                "checkpoint.safety_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "must_not": MUST_NOT_ACTIONS,
            "continue_linear_guardrail_tasks": False,
            "readiness": package.get("readiness"),
            "next_route": package.get("next_route"),
            **SAFETY_FIELDS,
        }
    )


def build_summary(
    *,
    generated_at: datetime,
    promotion_blocker_dir: Path,
    source_review: Mapping[str, Any],
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_owner_decision_pause_"
                "checkpoint.summary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": package.get("title"),
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "promotion_blocker_dir": str(promotion_blocker_dir),
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": True,
            "owner_decision_recorded": True,
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "pause_checkpoint_recorded": True,
            "continue_linear_guardrail_tasks": False,
            "recommended_owner_reassessment_topics": (
                RECOMMENDED_OWNER_REASSESSMENT_TOPICS
            ),
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "readiness": package.get("readiness"),
            "next_route": package.get("next_route"),
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
                "aits validate-data not rerun because TRADING-2363 only reads "
                "prior validated TRADING-2347 through TRADING-2362 research "
                "artifacts; it does not consume fresh market data, append events, "
                "bind outcomes, mutate outcome store, enable paper-shadow, enter "
                "production, call broker APIs, create orders, put capital at risk, "
                "produce technical features, score, backtest, or generate daily "
                "reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def validate_pause_contracts(package: Mapping[str, Any]) -> None:
    require_true(package, "evidence_chain_complete", TASK_ID)
    require_true(package, "owner_decision_recorded", TASK_ID)
    require_equal(package, "owner_decision", OWNER_DECISION, TASK_ID)
    require_false(package, "promotion_allowed", TASK_ID)
    require_true(package, "pause_checkpoint_recorded", TASK_ID)
    require_false(package, "continue_linear_guardrail_tasks", TASK_ID)
    require_equal(package, "next_route", NEXT_OWNER_REASSESSMENT_ROUTE, TASK_ID)


def build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir
        / "high_intensity_owner_decision_pause_checkpoint_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_owner_decision_pause_checkpoint.json",
        "source_review": output_dir
        / "high_intensity_owner_decision_pause_source_artifact_review.json",
        "decision_record": output_dir / "high_intensity_owner_decision_pause_record.json",
        "reassessment_plan": output_dir
        / "high_intensity_post_2363_owner_reassessment_plan.json",
        "rationale": output_dir
        / "high_intensity_owner_decision_pause_blocked_promotion_rationale.json",
        "interpretation_boundary": output_dir
        / "high_intensity_owner_decision_pause_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_owner_decision_pause_safety_boundary.json",
        "plan_doc": docs_root
        / "high_intensity_risk_cap_observe_only_owner_decision_pause_checkpoint.md",
        "reassessment_doc": docs_root / "high_intensity_post_2363_owner_reassessment.md",
    }


def write_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    decision_record: Mapping[str, Any],
    reassessment_plan: Mapping[str, Any],
    rationale: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["decision_record"], decision_record)
    write_json(paths["reassessment_plan"], reassessment_plan)
    write_json(paths["rationale"], rationale)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["plan_doc"], render_plan_doc(package))
    write_markdown(paths["reassessment_doc"], render_reassessment_doc(package))
    return string_paths(paths)


def render_plan_doc(package: Mapping[str, Any]) -> str:
    source_rows = [
        (
            f"- `{row.get('task')}`: status=`{row.get('status')}`, "
            f"evidence=`{row.get('evidence')}`"
        )
        for row in package.get("source_task_evidence", [])
    ]
    option_rows = [
        (
            f"- Option {row.get('option')}: {row.get('label')} "
            f"(continues_linear_guardrail_tasks=`{row.get('continues_linear_guardrail_tasks')}`)"
        )
        for row in package.get("reassessment_options", [])
    ]
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Owner Decision Pause Checkpoint",
            "",
            "## Executive Summary",
            "",
            f"- status: `{package.get('status')}`",
            f"- evidence_chain_complete: `{package.get('evidence_chain_complete')}`",
            f"- owner_decision_recorded: `{package.get('owner_decision_recorded')}`",
            f"- owner_decision: `{package.get('owner_decision')}`",
            f"- promotion_allowed: `{package.get('promotion_allowed')}`",
            (
                "- pause_checkpoint_recorded: "
                f"`{package.get('pause_checkpoint_recorded')}`"
            ),
            (
                "- continue_linear_guardrail_tasks: "
                f"`{package.get('continue_linear_guardrail_tasks')}`"
            ),
            f"- readiness: `{package.get('readiness')}`",
            f"- next_route: `{package.get('next_route')}`",
            "",
            "## Full Evidence Chain From 2347 To 2362",
            "",
            *source_rows,
            "",
            "## Owner Decision",
            "",
            f"`{package.get('owner_decision')}`",
            "",
            "## Promotion Remains Blocked",
            "",
            *[f"- `{reason}`" for reason in package.get("blocked_promotion_reasons", [])],
            "",
            "## Current No-Side-Effect Summary",
            "",
            *markdown_table_from_mapping(package.get("side_effect_summary")),
            "",
            "## Why Pause Now",
            "",
            "2347-2362 evidence chain is complete for observe-only guardrail planning. "
            "Additional linear scheduler tasks now require owner reassessment.",
            "",
            "## Reassessment Options",
            "",
            *option_rows,
            "",
            "## Explicit Non-Goals",
            "",
            *[f"- {item}" for item in package.get("explicit_non_goals", [])],
            "",
            "## Recommended Next Owner Questions",
            "",
            *[
                f"- `{item}`"
                for item in package.get("recommended_owner_reassessment_topics", [])
            ],
            "",
            "## Final Checkpoint Status",
            "",
            f"`{package.get('readiness')}`",
        ]
    )


def render_reassessment_doc(package: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Post-2363 Owner Reassessment",
            "",
            f"- owner_decision: `{package.get('owner_decision')}`",
            (
                "- continue_linear_guardrail_tasks: "
                f"`{package.get('continue_linear_guardrail_tasks')}`"
            ),
            f"- next_route: `{package.get('next_route')}`",
            "",
            (
                "后续不得自动继续 2364。Owner 需要重新选择：回到策略研究主线、"
                "启动有限 hardening implementation、定义 limited paper-shadow proposal，"
                "或暂停 scheduler 工作。"
            ),
        ]
    )
