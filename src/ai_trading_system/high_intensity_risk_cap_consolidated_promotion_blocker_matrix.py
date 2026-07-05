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
)
from ai_trading_system.high_intensity_risk_cap_paper_shadow_scope_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PAPER_SHADOW_SCOPE_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PRODUCTION_BROKER_ROOT,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    GUARDRAIL_SUMMARY,
    SIDE_EFFECT_SUMMARY,
    load_high_intensity_production_broker_hard_blocker_plan_inputs,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    NEXT_2362_ROUTE as EXPECTED_2361_NEXT_ROUTE,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    READINESS_STATUS as EXPECTED_2361_READINESS,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    SAFETY_FIELDS as SOURCE_SAFETY_FIELDS,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    SOURCE_TASKS as SOURCE_2361_TASKS,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    STATUS as EXPECTED_2361_STATUS,
)
from ai_trading_system.high_intensity_risk_cap_production_broker_hard_blocker_plan import (
    TASK_REGISTER_ID as SOURCE_2361_TASK_REGISTER_ID,
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

TASK_ID = "TRADING-2362"
TASK_REGISTER_ID = (
    "TRADING-2362_OBSERVE_ONLY_CONSOLIDATED_PROMOTION_BLOCKER_AND_SAFETY_"
    "EVIDENCE_MATRIX"
)
REPORT_TYPE = "high_intensity_risk_cap_observe_only_promotion_blocker_matrix"
ARTIFACT_ROLE = REPORT_TYPE
MODE = "observe_only_promotion_blocker_matrix"

STATUS = (
    "OBSERVE_ONLY_CONSOLIDATED_PROMOTION_BLOCKER_AND_SAFETY_EVIDENCE_MATRIX_"
    "READY_WITH_CAVEATS_PROMOTION_BLOCKED"
)
READINESS_STATUS = "READY_FOR_2363_WITH_CAVEATS"
NEXT_2363_ROUTE = "TRADING-2363_Observe_Only_Owner_Decision_And_Pause_Checkpoint"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
SOURCE_TASKS = [*SOURCE_2361_TASKS, "TRADING-2361"]

SAFETY_FIELDS: dict[str, Any] = {
    **SOURCE_SAFETY_FIELDS,
    "production_broker_hard_blocker_plan_only": False,
    "promotion_blocker_matrix_only": True,
}

BLOCKER_MATRIX: dict[str, Any] = {
    "scheduler_enablement": {
        "status": "BLOCKED",
        "priority": "P0",
        "reason": "NO_EXPLICIT_OWNER_APPROVAL_AND_DISABLED_BY_DEFAULT_REQUIRED",
    },
    "event_append": {
        "status": "BLOCKED",
        "priority": "P0",
        "reason": "EVENT_APPEND_DISABLED_AND_NO_MUTATION_APPROVAL",
    },
    "outcome_binding": {
        "status": "BLOCKED",
        "priority": "P0",
        "reason": "OUTCOME_BINDING_DISABLED_AND_NO_OUTCOME_STORE_MUTATION_APPROVAL",
    },
    "paper_shadow": {
        "status": "BLOCKED",
        "priority": "P1",
        "reason": "PAPER_SHADOW_DISABLED_AND_NO_OWNER_APPROVAL",
    },
    "production": {
        "status": "BLOCKED",
        "priority": "PERMANENT_UNTIL_OWNER_APPROVAL",
        "reason": "PRODUCTION_NOT_IN_SCOPE",
    },
    "broker_action_blocker": {
        "area": "broker_action",
        "status": "BLOCKED",
        "priority": "PERMANENT_UNTIL_OWNER_APPROVAL",
        "reason": "BROKER_ACTION_NOT_IN_SCOPE_AND_CAPITAL_AT_RISK_FORBIDDEN",
    },
}

SAFETY_EVIDENCE_MATRIX = [
    {
        "area": "scheduler_enablement",
        "current_allowed": False,
        "source_task": "TRADING-2347",
        "blocking_evidence": "disabled wiring and owner-disabled decision chain",
        "future_evidence_required": "explicit owner scheduler approval",
    },
    {
        "area": "event_append",
        "current_allowed": False,
        "source_task": "TRADING-2358",
        "blocking_evidence": "event append contract remains plan-only",
        "future_evidence_required": "append mutation approval and replay evidence",
    },
    {
        "area": "outcome_binding",
        "current_allowed": False,
        "source_task": "TRADING-2359",
        "blocking_evidence": "outcome binding contract remains plan-only",
        "future_evidence_required": "outcome store mutation approval",
    },
    {
        "area": "paper_shadow",
        "current_allowed": False,
        "source_task": "TRADING-2360",
        "blocking_evidence": "paper-shadow scope is disabled and no-broker only",
        "future_evidence_required": "owner approval and daily review protocol",
    },
    {
        "area": "production",
        "current_allowed": False,
        "source_task": "TRADING-2361",
        "blocking_evidence": "production hard-blocker plan",
        "future_evidence_required": "independent owner production approval",
    },
    {
        "area": "broker_action",
        "current_allowed": False,
        "source_task": "TRADING-2361",
        "blocking_evidence": "broker action and capital-at-risk hard blockers",
        "future_evidence_required": "broker API safety contract and human confirmation",
    },
]

FUTURE_EVIDENCE_STILL_MISSING = [
    "explicit_owner_scheduler_enablement_approval",
    "event_append_mutation_approval",
    "outcome_store_mutation_approval",
    "paper_shadow_owner_approval_and_daily_review_protocol",
    "production_risk_review_and_rollback_plan",
    "broker_api_safety_contract_and_order_dry_run_validation",
    "human_confirmation_protocol",
    "capital_at_risk_limit",
]

EXPLICIT_NON_GOALS = [
    "This task does not clear any blocker.",
    "This task does not enable scheduler, event append, outcome binding, or paper-shadow.",
    "This task does not enable production or broker action.",
    "This task does not create orders or put capital at risk.",
    "This task only consolidates blocker evidence for owner review.",
]


class HighIntensityPromotionBlockerMatrixError(HighIntensityGuardrailClosureError):
    pass


def run_high_intensity_risk_cap_observe_only_promotion_blocker_matrix(
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
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityPromotionBlockerMatrixError(
            f"promotion blocker matrix only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_promotion_blocker_matrix_inputs(
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
    source_review = build_source_artifact_review(inputs=inputs)
    blocker_matrix = build_blocker_matrix(
        generated_at=generated_at,
        source_review=source_review,
    )
    safety_matrix = build_safety_evidence_matrix(
        generated_at=generated_at,
        source_review=source_review,
    )
    future_gap = build_future_evidence_gap(
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
        blocker_matrix=blocker_matrix,
        safety_matrix=safety_matrix,
        future_gap=future_gap,
        rationale=rationale,
    )
    route = build_2363_owner_decision_pause_route(package=package)
    interpretation_boundary = build_interpretation_boundary(
        generated_at=generated_at,
        route=route,
    )
    safety_boundary = build_safety_boundary(generated_at=generated_at, route=route)
    summary = build_summary(
        generated_at=generated_at,
        production_broker_dir=production_broker_dir,
        source_review=source_review,
        package=package,
        route=route,
    )
    payloads = {
        "summary": summary,
        "package": package,
        "source_review": source_review,
        "blocker_matrix": blocker_matrix,
        "safety_matrix": safety_matrix,
        "future_gap": future_gap,
        "rationale": rationale,
        "route": route,
        "interpretation_boundary": interpretation_boundary,
        "safety_boundary": safety_boundary,
    }
    validate_generated_payloads(payloads, TASK_ID)
    validate_matrix_contracts(package)
    paths = build_output_paths(output_dir=output_dir, docs_root=docs_root)
    artifact_paths = write_outputs(
        paths=paths,
        summary=summary,
        package=package,
        source_review=source_review,
        blocker_matrix=blocker_matrix,
        safety_matrix=safety_matrix,
        future_gap=future_gap,
        rationale=rationale,
        route=route,
        interpretation_boundary=interpretation_boundary,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_promotion_blocker_matrix_inputs(
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
) -> dict[str, Any]:
    try:
        source_inputs = load_high_intensity_production_broker_hard_blocker_plan_inputs(
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
    except Exception as exc:  # noqa: BLE001
        raise HighIntensityPromotionBlockerMatrixError(
            f"TRADING-2362 source chain invalid before 2361: {exc}"
        ) from exc

    paths = {
        "summary": production_broker_dir
        / "high_intensity_production_broker_hard_blocker_plan_summary.json",
        "package": production_broker_dir
        / "high_intensity_risk_cap_observe_only_production_broker_hard_blocker_plan.json",
        "source_review": production_broker_dir
        / "high_intensity_production_broker_hard_blocker_source_artifact_review.json",
        "production_blocker": production_broker_dir
        / "high_intensity_production_hard_blocker_plan.json",
        "broker_blocker": production_broker_dir
        / "high_intensity_broker_hard_blocker_plan.json",
        "capital_blocker": production_broker_dir
        / "high_intensity_capital_at_risk_blocker.json",
        "confirmation": production_broker_dir
        / "high_intensity_human_confirmation_requirement.json",
        "rationale": production_broker_dir
        / "high_intensity_production_broker_blocked_promotion_rationale.json",
        "route": production_broker_dir
        / "high_intensity_2362_promotion_blocker_matrix_route.json",
        "interpretation_boundary": production_broker_dir
        / "high_intensity_production_broker_interpretation_boundary.json",
        "safety_boundary": production_broker_dir
        / "high_intensity_production_broker_safety_boundary.json",
    }
    production_broker_payloads = load_required_payloads(paths, "TRADING-2362")
    try:
        validate_2361_source_contracts(production_broker_payloads)
    except HighIntensityGuardrailClosureError as exc:
        raise HighIntensityPromotionBlockerMatrixError(str(exc)) from exc
    return {
        **source_inputs,
        "production_broker_hard_blocker_plan": production_broker_payloads,
        "production_broker_hard_blocker_plan_paths": string_paths(paths),
    }


def validate_2361_source_contracts(payloads: Mapping[str, Any]) -> None:
    summary = mapping(payloads["summary"])
    package = mapping(payloads["package"])
    route = mapping(payloads["route"])
    source_review = mapping(payloads["source_review"])
    for key, payload in payloads.items():
        validate_safety_payload(f"TRADING-2362 source 2361 {key}", mapping(payload))
    require_equal(summary, "status", EXPECTED_2361_STATUS, "TRADING-2362")
    require_equal(package, "status", EXPECTED_2361_STATUS, "TRADING-2362")
    require_equal(summary, "readiness", EXPECTED_2361_READINESS, "TRADING-2362")
    require_equal(summary, "next_route", EXPECTED_2361_NEXT_ROUTE, "TRADING-2362")
    require_equal(route, "next_route", EXPECTED_2361_NEXT_ROUTE, "TRADING-2362")
    require_equal(summary, "source_tasks", SOURCE_2361_TASKS, "TRADING-2362")
    require_true(summary, "evidence_chain_complete", "TRADING-2362")
    require_equal(summary, "owner_decision", OWNER_DECISION, "TRADING-2362")
    for field in (
        "production_hard_blocker_plan_ready",
        "broker_hard_blocker_plan_ready",
        "capital_at_risk_blocker_ready",
        "human_confirmation_requirement_ready",
    ):
        require_true(summary, field, "TRADING-2362")
    for field in (
        "promotion_allowed",
        "production_enabled",
        "production_attempted",
        "broker_action_enabled",
        "broker_action_attempted",
        "capital_at_risk_allowed",
    ):
        require_false(summary, field, "TRADING-2362")
    require_equal(source_review, "source_contract_status", "PASS", "TRADING-2362")
    validate_source_data_quality(summary, "TRADING-2362 2361 summary")


def build_source_artifact_review(*, inputs: Mapping[str, Any]) -> dict[str, Any]:
    source = mapping(inputs["production_broker_hard_blocker_plan"])
    summary = mapping(source["summary"])
    source_review = mapping(source["source_review"])
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.source_artifact_review.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": [
                *source_review.get("source_task_ids", []),
                SOURCE_2361_TASK_REGISTER_ID,
            ],
            "source_task_evidence": source_evidence_rows_with_previous(
                source_review,
                task="TRADING-2361",
                status=summary.get("status"),
                evidence="production and broker hard-blocker plan present",
            ),
            "source_artifacts_read": True,
            "source_artifacts_parsed": True,
            "source_contract_status": "PASS",
            "production_broker_status": summary.get("status"),
            "production_broker_readiness": summary.get("readiness"),
            "production_broker_next_route": summary.get("next_route"),
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


def build_blocker_matrix(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.blocker_matrix.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "consolidated_blocker_matrix_ready": True,
            "blocker_matrix": BLOCKER_MATRIX,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_safety_evidence_matrix(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.safety_evidence_matrix.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "safety_evidence_matrix_ready": True,
            "safety_evidence_matrix": SAFETY_EVIDENCE_MATRIX,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            **SAFETY_FIELDS,
        }
    )


def build_future_evidence_gap(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.future_evidence_gap.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "future_evidence_still_missing": FUTURE_EVIDENCE_STILL_MISSING,
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
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.blocked_promotion_rationale.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "source_tasks": source_review.get("source_tasks"),
            "owner_decision": OWNER_DECISION,
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "blocked_promotion_reasons": list(BLOCKER_MATRIX),
            "future_evidence_still_missing": FUTURE_EVIDENCE_STILL_MISSING,
            **SAFETY_FIELDS,
        }
    )


def build_package(
    *,
    generated_at: datetime,
    source_review: Mapping[str, Any],
    blocker_matrix: Mapping[str, Any],
    safety_matrix: Mapping[str, Any],
    future_gap: Mapping[str, Any],
    rationale: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.package.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": (
                "High-Intensity Risk-Cap Observe-Only Consolidated Promotion "
                "Blocker And Safety Evidence Matrix"
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
            "consolidated_blocker_matrix_ready": blocker_matrix.get(
                "consolidated_blocker_matrix_ready"
            ),
            "safety_evidence_matrix_ready": safety_matrix.get(
                "safety_evidence_matrix_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "blocker_matrix": blocker_matrix.get("blocker_matrix"),
            "safety_evidence_matrix": safety_matrix.get("safety_evidence_matrix"),
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
            "future_evidence_still_missing": future_gap.get(
                "future_evidence_still_missing"
            ),
            "blocked_promotion_reasons": rationale.get("blocked_promotion_reasons"),
            "explicit_non_goals": EXPLICIT_NON_GOALS,
            "readiness": READINESS_STATUS,
            "next_route": NEXT_2363_ROUTE,
            "next_task": NEXT_2363_ROUTE,
            **SAFETY_FIELDS,
        }
    )


def build_2363_owner_decision_pause_route(
    *,
    package: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.2363_route.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "readiness": package.get("readiness"),
            "next_route": NEXT_2363_ROUTE,
            "next_task": NEXT_2363_ROUTE,
            "route_caveats": [
                "OWNER_DECISION_KEEP_DISABLED",
                "PROMOTION_BLOCKED",
                "OBSERVE_ONLY",
                "ALL_RUNTIME_PATHS_DISABLED",
                "OWNER_DECISION_PAUSE_CHECKPOINT_REQUIRED_NEXT",
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
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.interpretation_boundary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "generated_at": generated_at.isoformat(),
            "interpretation": (
                "TRADING-2362 consolidates blocker evidence but clears none."
            ),
            "not_promotion_clearance": True,
            "not_runtime_enablement": True,
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
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.safety_boundary.v1"
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
    production_broker_dir: Path,
    source_review: Mapping[str, Any],
    package: Mapping[str, Any],
    route: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": (
                "high_intensity_risk_cap_observe_only_promotion_blocker_"
                "matrix.summary.v1"
            ),
            "task_id": TASK_ID,
            "task_register_id": TASK_REGISTER_ID,
            "title": package.get("title"),
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "production_broker_dir": str(production_broker_dir),
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "status": STATUS,
            "source_tasks": SOURCE_TASKS,
            "source_task_ids": source_review.get("source_task_ids"),
            "evidence_chain_complete": True,
            "owner_decision": OWNER_DECISION,
            "consolidated_blocker_matrix_ready": package.get(
                "consolidated_blocker_matrix_ready"
            ),
            "safety_evidence_matrix_ready": package.get(
                "safety_evidence_matrix_ready"
            ),
            "promotion_decision": "BLOCKED",
            "promotion_allowed": False,
            "guardrail_summary": GUARDRAIL_SUMMARY,
            "side_effect_summary": SIDE_EFFECT_SUMMARY,
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
                "aits validate-data not rerun because TRADING-2362 only reads "
                "prior validated TRADING-2347 through TRADING-2361 research "
                "artifacts; it does not consume fresh market data, append events, "
                "bind outcomes, mutate outcome store, enable paper-shadow, enter "
                "production, call broker APIs, create orders, put capital at risk, "
                "produce technical features, score, backtest, or generate daily "
                "reports."
            ),
            **SAFETY_FIELDS,
        }
    )


def validate_matrix_contracts(package: Mapping[str, Any]) -> None:
    require_true(package, "consolidated_blocker_matrix_ready", TASK_ID)
    require_true(package, "safety_evidence_matrix_ready", TASK_ID)
    require_false(package, "promotion_allowed", TASK_ID)
    matrix = mapping(package.get("blocker_matrix"))
    for key in (
        "scheduler_enablement",
        "event_append",
        "outcome_binding",
        "paper_shadow",
        "production",
        "broker_action_blocker",
    ):
        if key not in matrix:
            raise HighIntensityPromotionBlockerMatrixError(
                f"TRADING-2362 blocker matrix missing {key}"
            )
    require_equal(package, "next_route", NEXT_2363_ROUTE, TASK_ID)


def build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_promotion_blocker_matrix_summary.json",
        "package": output_dir
        / "high_intensity_risk_cap_observe_only_promotion_blocker_matrix.json",
        "source_review": output_dir
        / "high_intensity_promotion_blocker_matrix_source_artifact_review.json",
        "blocker_matrix": output_dir
        / "high_intensity_consolidated_blocker_matrix.json",
        "safety_matrix": output_dir / "high_intensity_safety_evidence_matrix.json",
        "future_gap": output_dir
        / "high_intensity_promotion_blocker_future_evidence_gap.json",
        "rationale": output_dir
        / "high_intensity_promotion_blocker_blocked_promotion_rationale.json",
        "route": output_dir / "high_intensity_2363_owner_decision_pause_route.json",
        "interpretation_boundary": output_dir
        / "high_intensity_promotion_blocker_interpretation_boundary.json",
        "safety_boundary": output_dir
        / "high_intensity_promotion_blocker_safety_boundary.json",
        "plan_doc": docs_root
        / "high_intensity_risk_cap_observe_only_promotion_blocker_matrix.md",
        "route_doc": docs_root / "high_intensity_2363_owner_decision_pause_route.md",
    }


def write_outputs(
    *,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    package: Mapping[str, Any],
    source_review: Mapping[str, Any],
    blocker_matrix: Mapping[str, Any],
    safety_matrix: Mapping[str, Any],
    future_gap: Mapping[str, Any],
    rationale: Mapping[str, Any],
    route: Mapping[str, Any],
    interpretation_boundary: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    write_json(paths["summary"], summary)
    write_json(paths["package"], package)
    write_json(paths["source_review"], source_review)
    write_json(paths["blocker_matrix"], blocker_matrix)
    write_json(paths["safety_matrix"], safety_matrix)
    write_json(paths["future_gap"], future_gap)
    write_json(paths["rationale"], rationale)
    write_json(paths["route"], route)
    write_json(paths["interpretation_boundary"], interpretation_boundary)
    write_json(paths["safety_boundary"], safety_boundary)
    write_markdown(paths["plan_doc"], render_plan_doc(package))
    write_markdown(paths["route_doc"], render_2363_route_doc(route))
    return string_paths(paths)


def render_plan_doc(package: Mapping[str, Any]) -> str:
    source_rows = [
        (
            f"- `{row.get('task')}`: status=`{row.get('status')}`, "
            f"evidence=`{row.get('evidence')}`"
        )
        for row in package.get("source_task_evidence", [])
    ]
    blocker_rows = [
        (
            f"|`{key}`|`{value.get('status')}`|`{value.get('priority')}`|"
            f"`{value.get('reason')}`|"
        )
        for key, value in mapping(package.get("blocker_matrix")).items()
    ]
    safety_rows = [
        (
            f"- `{row.get('area')}`: allowed=`{row.get('current_allowed')}`, "
            f"source=`{row.get('source_task')}`, evidence=`{row.get('blocking_evidence')}`"
        )
        for row in package.get("safety_evidence_matrix", [])
    ]
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Observe-Only Promotion Blocker Matrix",
            "",
            "## Executive Summary",
            "",
            f"- status: `{package.get('status')}`",
            f"- evidence_chain_complete: `{package.get('evidence_chain_complete')}`",
            f"- owner_decision: `{package.get('owner_decision')}`",
            (
                "- consolidated_blocker_matrix_ready: "
                f"`{package.get('consolidated_blocker_matrix_ready')}`"
            ),
            (
                "- safety_evidence_matrix_ready: "
                f"`{package.get('safety_evidence_matrix_ready')}`"
            ),
            f"- promotion_allowed: `{package.get('promotion_allowed')}`",
            f"- readiness: `{package.get('readiness')}`",
            f"- next_route: `{package.get('next_route')}`",
            "",
            "## Full Source Evidence Chain",
            "",
            *source_rows,
            "",
            "## Consolidated Blocker Matrix",
            "",
            "|Area|Status|Priority|Reason|",
            "|---|---|---|---|",
            *blocker_rows,
            "",
            "## Safety Evidence Matrix",
            "",
            *safety_rows,
            "",
            "## Guardrail Summary",
            "",
            *markdown_table_from_mapping(package.get("guardrail_summary")),
            "",
            "## Side-Effect Summary",
            "",
            *markdown_table_from_mapping(package.get("side_effect_summary")),
            "",
            "## Future Evidence Still Missing",
            "",
            *[
                f"- `{item}`"
                for item in package.get("future_evidence_still_missing", [])
            ],
            "",
            "## Explicit Non-Goals",
            "",
            *[f"- {item}" for item in package.get("explicit_non_goals", [])],
            "",
            "## Recommended Pause Checkpoint",
            "",
            "进入 2363 owner decision / pause checkpoint，不能直接继续 2364。",
            "",
            "## Next Route",
            "",
            f"`{package.get('next_route')}`",
        ]
    )


def render_2363_route_doc(route: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity 2363 Owner Decision Pause Route",
            "",
            f"- readiness: `{route.get('readiness')}`",
            f"- next_route: `{route.get('next_route')}`",
            "",
            "2363 route 只能记录 owner decision 和 pause checkpoint。",
            "它不是新的 scheduler hardening implementation task，也不是 2364 自动入口。",
        ]
    )
