from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DECISION_CONTINUE_OPTIMIZATION,
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    READY_STATUS as SOURCE_2376_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    BASE_CANDIDATE_ID,
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT,
    RANKING_TOP_REFERENCE,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    NEXT_ROUTE as SOURCE_2379_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_slice_robustness_optimized_variant_retest import (
    READY_STATUS as SOURCE_2379_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_slice_robustness_return_gap_optimization_plan import (
    READY_STATUS as SOURCE_2378_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2380"
TASK_REGISTER_ID = (
    "TRADING-2380_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_"
    "OBSERVATION_DECISION"
)
REPORT_TYPE = "dynamic_strategy_optimized_variant_owner_review_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_optimized_variant_owner_review_and_observation_decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_"
    "BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = "DO_NOT_APPROVE_OBSERVATION_CONTINUE_OPTIMIZATION_REVIEW_REQUIRED"
NEXT_ROUTE = (
    "TRADING-2381_Dynamic_Strategy_Optimization_Plateau_And_Next_Candidate_Decision"
)
BEST_VARIANT_EXPECTED = "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
PRIMARY_EXECUTION_CADENCE = "valid_until_window"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REVIEW_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2376", "TRADING-2378", "TRADING-2379")
OBSERVATION_REJECTION_REASONS: tuple[str, ...] = (
    "BEST_VARIANT_DECISION_REMAINS_CONTINUE_OPTIMIZATION",
    "RESEARCH_ONLY_OBSERVATION_ACCEPTANCE_CRITERIA_NOT_MET",
    "TIME_OR_REGIME_SLICE_ROBUSTNESS_REQUIRES_MORE_EVIDENCE",
    "RETURN_GAP_REPAIR_NOT_SUFFICIENT_FOR_OBSERVATION_APPROVAL",
)
CONTINUE_OPTIMIZATION_RATIONALE: tuple[str, ...] = (
    "BEST_VARIANT_REDUCES_RETURN_GAP_VS_BASE",
    "BEST_VARIANT_REMAINS_RESEARCH_RELEVANT_AFTER_COST_STRESS",
    "SLICE_ROBUSTNESS_GAPS_ARE_ACTIONABLE_BUT_NOT_OBSERVATION_READY",
    "OWNER_REVIEW_REQUIRED_BEFORE_ANY_OBSERVATION_LINE_RESTART",
)
NON_APPROVED_PATHS: tuple[str, ...] = (
    "research_only_observation",
    "paper_shadow",
    "paper_trade",
    "shadow_position",
    "event_append",
    "outcome_binding",
    "scheduler",
    "scheduled_task",
    "daily_report",
    "production",
    "broker",
    "order",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_store_mutated",
    "paper_shadow_enabled",
    "paper_trade_created",
    "shadow_position_created",
    "production_enabled",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
)

DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "variant_retest_result.json"
)
DEFAULT_SOURCE_2379_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "optimized_variant_ranking.json"
)
DEFAULT_SOURCE_2378_OPTIMIZATION_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT
    / "optimization_plan_result.json"
)
DEFAULT_SOURCE_2378_VARIANT_EVALUATION_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT
    / "variant_evaluation_plan.json"
)
DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "targeted_retest_result.json"
)
DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)


def run_dynamic_strategy_optimized_variant_owner_review_decision(
    *,
    source_variant_retest_path: Path = DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_variant_decision_update_path: Path = (
        DEFAULT_SOURCE_2379_DECISION_UPDATE_PATH
    ),
    source_optimized_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH
    ),
    source_optimization_plan_path: Path = DEFAULT_SOURCE_2378_OPTIMIZATION_PLAN_PATH,
    source_variant_evaluation_plan_path: Path = (
        DEFAULT_SOURCE_2378_VARIANT_EVALUATION_PLAN_PATH
    ),
    source_targeted_retest_path: Path = DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH,
    source_targeted_decision_update_path: Path = (
        DEFAULT_SOURCE_2376_DECISION_UPDATE_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_variant_retest_path=source_variant_retest_path,
        source_variant_decision_update_path=source_variant_decision_update_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_optimization_plan_path=source_optimization_plan_path,
        source_variant_evaluation_plan_path=source_variant_evaluation_plan_path,
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
    )
    ready = not sources["source_validation_errors"]
    resolved_as_of = _resolve_as_of(as_of_date, sources["variant_retest"])
    source_findings = _source_findings(sources)
    observation_rejection = _observation_rejection_rationale(
        sources=sources,
        source_findings=source_findings,
        ready=ready,
    )
    owner_record = _owner_review_decision_record(
        sources=sources,
        source_findings=source_findings,
        observation_rejection=observation_rejection,
        as_of_date=resolved_as_of,
        ready=ready,
    )
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        source_findings=source_findings,
        observation_rejection=observation_rejection,
        owner_record=owner_record,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_variant_retest_path: Path,
    source_variant_decision_update_path: Path,
    source_optimized_variant_ranking_path: Path,
    source_optimization_plan_path: Path,
    source_variant_evaluation_plan_path: Path,
    source_targeted_retest_path: Path,
    source_targeted_decision_update_path: Path,
) -> dict[str, Any]:
    variant_retest = _load_json_document(source_variant_retest_path)
    variant_decision_update = _load_json_document(source_variant_decision_update_path)
    optimized_variant_ranking = _load_json_document(
        source_optimized_variant_ranking_path
    )
    optimization_plan = _load_json_document(source_optimization_plan_path)
    variant_evaluation_plan = _load_json_document(source_variant_evaluation_plan_path)
    targeted_retest = _load_json_document(source_targeted_retest_path)
    targeted_decision_update = _load_json_document(source_targeted_decision_update_path)
    source_status = {
        "variant_retest": _as_mapping(variant_retest).get("status"),
        "variant_decision_update": _as_mapping(variant_decision_update).get("status"),
        "optimized_variant_ranking": _as_mapping(optimized_variant_ranking).get(
            "status"
        ),
        "optimization_plan": _as_mapping(optimization_plan).get("status"),
        "variant_evaluation_plan": _as_mapping(variant_evaluation_plan).get("status"),
        "targeted_retest": _as_mapping(targeted_retest).get("status"),
        "targeted_decision_update": _as_mapping(targeted_decision_update).get(
            "status"
        ),
    }
    best_variant = _best_variant(variant_retest, optimized_variant_ranking)
    best_decision = _best_variant_decision(variant_retest, optimized_variant_ranking)
    primary_cadence = _coalesce_string(
        _as_mapping(variant_retest).get("primary_execution_cadence"),
        _as_mapping(targeted_retest).get("primary_execution_cadence"),
    )
    source_files = {
        "variant_retest": str(source_variant_retest_path),
        "variant_decision_update": str(source_variant_decision_update_path),
        "optimized_variant_ranking": str(source_optimized_variant_ranking_path),
        "optimization_plan": str(source_optimization_plan_path),
        "variant_evaluation_plan": str(source_variant_evaluation_plan_path),
        "targeted_retest": str(source_targeted_retest_path),
        "targeted_decision_update": str(source_targeted_decision_update_path),
    }
    sources: dict[str, Any] = {
        "variant_retest": variant_retest,
        "variant_decision_update": variant_decision_update,
        "optimized_variant_ranking": optimized_variant_ranking,
        "optimization_plan": optimization_plan,
        "variant_evaluation_plan": variant_evaluation_plan,
        "targeted_retest": targeted_retest,
        "targeted_decision_update": targeted_decision_update,
        "source_status": source_status,
        "source_files": source_files,
        "source_hashes": {
            key: _file_sha256(Path(path)) for key, path in source_files.items()
        },
        "base_candidate": _coalesce_string(
            _as_mapping(variant_retest).get("base_candidate"),
            _as_mapping(optimization_plan).get("primary_candidate"),
            _as_mapping(targeted_retest).get("primary_candidate"),
        ),
        "ranking_top_reference": _coalesce_string(
            _as_mapping(variant_retest).get("ranking_top_reference"),
            _as_mapping(optimization_plan).get("ranking_top_reference"),
            _as_mapping(targeted_retest).get("ranking_top_from_2365"),
        ),
        "best_variant_from_2379": best_variant,
        "best_variant_decision_from_2379": best_decision,
        "primary_execution_cadence": primary_cadence,
        "source_validation_errors": [],
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    status_expectations = {
        "variant_retest": SOURCE_2379_READY_STATUS,
        "variant_decision_update": SOURCE_2379_READY_STATUS,
        "optimized_variant_ranking": SOURCE_2379_READY_STATUS,
        "optimization_plan": SOURCE_2378_READY_STATUS,
        "variant_evaluation_plan": SOURCE_2378_READY_STATUS,
        "targeted_retest": SOURCE_2376_READY_STATUS,
        "targeted_decision_update": SOURCE_2376_READY_STATUS,
    }
    source_status = _as_mapping(sources.get("source_status"))
    for key, expected in status_expectations.items():
        actual = source_status.get(key)
        if actual != expected:
            errors.append(f"{key}.status expected {expected}, got {actual!r}")

    if sources.get("base_candidate") != BASE_CANDIDATE_ID:
        errors.append(
            f"base_candidate expected {BASE_CANDIDATE_ID}, "
            f"got {sources.get('base_candidate')!r}"
        )
    if sources.get("ranking_top_reference") != RANKING_TOP_REFERENCE:
        errors.append(
            f"ranking_top_reference expected {RANKING_TOP_REFERENCE}, "
            f"got {sources.get('ranking_top_reference')!r}"
        )
    if sources.get("best_variant_from_2379") != BEST_VARIANT_EXPECTED:
        errors.append(
            f"best_variant_from_2379 expected {BEST_VARIANT_EXPECTED}, "
            f"got {sources.get('best_variant_from_2379')!r}"
        )
    if sources.get("best_variant_decision_from_2379") != DECISION_CONTINUE_OPTIMIZATION:
        errors.append(
            "best_variant_decision_from_2379 expected "
            f"{DECISION_CONTINUE_OPTIMIZATION}, "
            f"got {sources.get('best_variant_decision_from_2379')!r}"
        )
    if sources.get("primary_execution_cadence") != PRIMARY_EXECUTION_CADENCE:
        errors.append(
            f"primary_execution_cadence expected {PRIMARY_EXECUTION_CADENCE}, "
            f"got {sources.get('primary_execution_cadence')!r}"
        )

    variant_retest = _as_mapping(sources.get("variant_retest"))
    if variant_retest.get("recommended_next_research_task") != SOURCE_2379_EXPECTED_ROUTE:
        errors.append(
            "variant_retest.recommended_next_research_task expected "
            f"{SOURCE_2379_EXPECTED_ROUTE}, "
            f"got {variant_retest.get('recommended_next_research_task')!r}"
        )
    if variant_retest.get("candidate_ready_for_research_only_observation") is True:
        errors.append("2379 candidate_ready_for_research_only_observation is true")

    for source_name in (
        "variant_retest",
        "optimization_plan",
        "targeted_retest",
    ):
        document = _as_mapping(sources.get(source_name))
        for field in SAFETY_FALSE_FIELDS:
            if document.get(field) is True:
                errors.append(f"{source_name}.{field} must remain false")
        if document.get("broker_action") not in {None, "none"}:
            errors.append(f"{source_name}.broker_action must remain none")
        if document.get("production_effect") not in {None, "none"}:
            errors.append(f"{source_name}.production_effect must remain none")
    return errors


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    variant_retest = _as_mapping(sources.get("variant_retest"))
    decision_update = _as_mapping(variant_retest.get("decision_update"))
    best_metrics = _as_mapping(decision_update.get("best_variant_metrics"))
    summary_findings = variant_retest.get("summary_findings")
    return {
        "base_candidate": sources.get("base_candidate"),
        "ranking_top_reference": sources.get("ranking_top_reference"),
        "best_variant_from_2379": sources.get("best_variant_from_2379"),
        "best_variant_decision_from_2379": sources.get(
            "best_variant_decision_from_2379"
        ),
        "candidate_ready_for_research_only_observation": bool(
            variant_retest.get("candidate_ready_for_research_only_observation")
        ),
        "primary_execution_cadence": sources.get("primary_execution_cadence"),
        "return_gap_reduction_vs_base": best_metrics.get(
            "return_gap_reduction_vs_base"
        ),
        "time_slice_pass_rate": best_metrics.get("time_slice_pass_rate"),
        "regime_slice_pass_rate": best_metrics.get("regime_slice_pass_rate"),
        "survives_realistic_cost": best_metrics.get("survives_realistic_cost"),
        "survives_conservative_cost": best_metrics.get("survives_conservative_cost"),
        "survives_harsh_cost": best_metrics.get("survives_harsh_cost"),
        "summary_findings": summary_findings if isinstance(summary_findings, list) else [],
    }


def _observation_rejection_rationale(
    *,
    sources: Mapping[str, Any],
    source_findings: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_observation_rejection_rationale.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "source_ready_for_owner_review_decision": ready,
        "best_variant_from_2379": sources.get("best_variant_from_2379"),
        "best_variant_decision_from_2379": sources.get(
            "best_variant_decision_from_2379"
        ),
        "research_only_observation_approved": False,
        "paper_shadow_approved": False,
        "continue_optimization_allowed": True,
        "optimization_plateau_review_required": True,
        "observation_rejection_reasons": list(OBSERVATION_REJECTION_REASONS),
        "continue_optimization_rationale": list(CONTINUE_OPTIMIZATION_RATIONALE),
        "source_findings": dict(source_findings),
        "plateau_review_requirement": {
            "required": True,
            "reason": (
                "2379 improved the best variant but still did not meet observation "
                "acceptance criteria; the next review must decide whether further "
                "variant search is still productive or has reached a plateau."
            ),
            "recommended_next_research_task": NEXT_ROUTE,
            "must_answer": [
                "whether cooldown_balanced_v1 has enough remaining optimization runway",
                "whether to narrow or stop the current variant line",
                "whether a different candidate family should be reviewed next",
            ],
        },
    }


def _owner_review_decision_record(
    *,
    sources: Mapping[str, Any],
    source_findings: Mapping[str, Any],
    observation_rejection: Mapping[str, Any],
    as_of_date: date,
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "as_of": as_of_date.isoformat(),
        "owner_review_decision_recorded": ready,
        "owner_decision": OWNER_DECISION if ready else "BLOCKED_SOURCE_ARTIFACT",
        "best_variant_from_2379": sources.get("best_variant_from_2379"),
        "best_variant_decision_from_2379": sources.get(
            "best_variant_decision_from_2379"
        ),
        "source_findings": dict(source_findings),
        "research_only_observation_approved": False,
        "continue_optimization_allowed": True,
        "optimization_plateau_review_required": True,
        "observation_rejection_reasons": list(
            observation_rejection.get("observation_rejection_reasons", [])
        ),
        "non_approved_paths": list(NON_APPROVED_PATHS),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    source_findings: Mapping[str, Any],
    observation_rejection: Mapping[str, Any],
    owner_record: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "market_regime": AI_REGIME_SUMMARY.get("market_regime", "ai_after_chatgpt"),
        "market_regime_summary": AI_REGIME_SUMMARY,
        "source_tasks": list(SOURCE_TASKS),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_owner_review_decision": ready,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "research_only": True,
        "observe_only": True,
        "base_candidate": sources.get("base_candidate"),
        "ranking_top_reference": sources.get("ranking_top_reference"),
        "best_variant_from_2379": sources.get("best_variant_from_2379"),
        "best_variant_decision_from_2379": sources.get(
            "best_variant_decision_from_2379"
        ),
        "primary_execution_cadence": sources.get("primary_execution_cadence"),
        "source_findings_from_2379": dict(source_findings),
        "owner_review_decision_recorded": bool(
            owner_record.get("owner_review_decision_recorded")
        ),
        "owner_decision": owner_record.get("owner_decision"),
        "owner_review_decision": dict(owner_record),
        "observation_rejection_rationale_ready": ready,
        "observation_rejection_rationale": dict(observation_rejection),
        "observation_rejection_reasons": list(OBSERVATION_REJECTION_REASONS),
        "continue_optimization_rationale": list(CONTINUE_OPTIMIZATION_RATIONALE),
        "research_only_observation_approved": False,
        "continue_optimization_allowed": True,
        "optimization_plateau_review_required": True,
        "recommended_next_research_task": NEXT_ROUTE,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "explicit_non_approval_list": list(NON_APPROVED_PATHS),
        "summary_findings": [
            (
                "2379 best variant remains "
                f"{DECISION_CONTINUE_OPTIMIZATION}; observation is not approved."
            ),
            (
                f"{BEST_VARIANT_EXPECTED} remains research-relevant but requires "
                "plateau review before more variant search."
            ),
            (
                "Paper-shadow, event append, outcome binding, scheduler, daily "
                "report, production and broker paths remain disabled."
            ),
        ],
        "artifact_paths": {},
    }


def _write_outputs(*, payload: dict[str, Any], output_root: Path, docs_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    owner_json = output_root / "owner_review_decision.json"
    rejection_json = output_root / "observation_rejection_rationale.json"
    markdown_path = docs_root / "dynamic_strategy_optimized_variant_owner_review_decision.md"
    rejection_markdown = docs_root / "dynamic_strategy_observation_rejection_rationale.md"
    route_markdown = docs_root / "dynamic_strategy_2381_route.md"
    payload["artifact_paths"] = {
        "json_path": str(owner_json),
        "owner_review_decision_json": str(owner_json),
        "observation_rejection_rationale_json": str(rejection_json),
        "markdown_path": str(markdown_path),
        "observation_rejection_rationale_markdown": str(rejection_markdown),
        "next_route_markdown": str(route_markdown),
    }
    owner_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    rejection_json.write_text(
        json.dumps(
            {
                "generated_at": payload["generated_at"],
                "report_type": REPORT_TYPE,
                "schema_version": (
                    "dynamic_strategy_observation_rejection_rationale.v1"
                ),
                "status": payload["status"],
                "production_effect": "none",
                "broker_action": "none",
                "observation_rejection_rationale": payload[
                    "observation_rejection_rationale"
                ],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text(_main_markdown(payload), encoding="utf-8")
    rejection_markdown.write_text(_rejection_markdown(payload), encoding="utf-8")
    route_markdown.write_text(_route_markdown(payload), encoding="utf-8")


def _main_markdown(payload: Mapping[str, Any]) -> str:
    rejection_reasons = _markdown_bullets(
        payload.get("observation_rejection_reasons", [])
    )
    non_approval = _markdown_bullets(payload.get("explicit_non_approval_list", []))
    source_findings = _as_mapping(payload.get("source_findings_from_2379"))
    return "\n".join(
        [
            "# Dynamic strategy optimized variant owner review decision",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload['status']}`",
            f"- owner decision：`{payload['owner_decision']}`",
            f"- best variant from 2379：`{payload['best_variant_from_2379']}`",
            f"- best variant decision from 2379：`{payload['best_variant_decision_from_2379']}`",
            "- research-only observation approved：`false`",
            "- paper-shadow approved：`false`",
            f"- next route：`{payload['recommended_next_research_task']}`",
            "",
            "## Source findings from TRADING-2379",
            "",
            f"- base candidate：`{payload['base_candidate']}`",
            f"- ranking top reference：`{payload['ranking_top_reference']}`",
            f"- primary execution cadence：`{payload['primary_execution_cadence']}`",
            "- return gap reduction vs base："
            f"`{source_findings.get('return_gap_reduction_vs_base')}`",
            f"- time slice pass rate：`{source_findings.get('time_slice_pass_rate')}`",
            f"- regime slice pass rate：`{source_findings.get('regime_slice_pass_rate')}`",
            "",
            "## Best variant review",
            "",
            "`dynamic_regime_overlay_v0_4_cooldown_balanced_v1` 是 2379 当前最优变体，"
            "但其 2379 decision 仍为 `CONTINUE_OPTIMIZATION`。该结果说明候选仍有研究价值，"
            "但不足以启动 research-only observation。",
            "",
            "## Why observation is not approved",
            "",
            rejection_reasons,
            "",
            "## Owner review decision",
            "",
            "Owner decision 固定为 "
            "`DO_NOT_APPROVE_OBSERVATION_CONTINUE_OPTIMIZATION_REVIEW_REQUIRED`。"
            "这不是 observation approval，也不是 paper-shadow approval。",
            "",
            "## Continue optimization rationale",
            "",
            _markdown_bullets(payload.get("continue_optimization_rationale", [])),
            "",
            "## Optimization plateau review requirement",
            "",
            "下一步必须先判断当前 variant search 是否已经进入 plateau / diminishing return。"
            "在完成该复盘前，不得继续把优化结果解释为 observation readiness。",
            "",
            "## Explicit non-approval list",
            "",
            non_approval,
            "",
            "## Guardrail summary",
            "",
            "- scheduler_enabled：`false`",
            "- event_append_enabled：`false`",
            "- outcome_binding_enabled：`false`",
            "- paper_shadow_enabled：`false`",
            "- paper_trade_created：`false`",
            "- shadow_position_created：`false`",
            "- production_enabled：`false`",
            "- broker_action_enabled：`false`",
            "- daily_report_generated：`false`",
            "",
            "## Recommended next route",
            "",
            f"`{payload['recommended_next_research_task']}`",
            "",
        ]
    )


def _rejection_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy observation rejection rationale",
            "",
            f"- status：`{payload['status']}`",
            f"- best variant：`{payload['best_variant_from_2379']}`",
            f"- 2379 decision：`{payload['best_variant_decision_from_2379']}`",
            "- research-only observation approved：`false`",
            "- paper-shadow approved：`false`",
            "",
            "## Rejection reasons",
            "",
            _markdown_bullets(payload.get("observation_rejection_reasons", [])),
            "",
            "## Plateau requirement",
            "",
            "继续优化前必须进入 TRADING-2381，判断当前候选线是否还值得继续搜索，"
            "或是否应收敛并转向下一候选决策。",
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2381 route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- status：`{payload['status']}`",
            f"- owner decision：`{payload['owner_decision']}`",
            f"- next route：`{payload['recommended_next_research_task']}`",
            "- route reason：2379 best variant remains `CONTINUE_OPTIMIZATION`; "
            "2380 不批准 observation，并要求先做 optimization plateau / next candidate decision。",
            "",
        ]
    )


def _load_json_document(path: Path) -> dict[str, Any]:
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Required source artifact is missing: {path}") from exc
    if not isinstance(loaded, dict):
        raise TypeError(f"Expected JSON object at {path}")
    return loaded


def _resolve_as_of(as_of_date: date | None, variant_retest: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    value = variant_retest.get("as_of")
    if isinstance(value, str):
        return date.fromisoformat(value)
    return date.today()


def _best_variant(
    variant_retest: Mapping[str, Any],
    optimized_variant_ranking: Mapping[str, Any],
) -> str | None:
    return _coalesce_string(
        variant_retest.get("best_variant_after_retest"),
        _as_mapping(variant_retest.get("decision_update")).get(
            "best_variant_after_retest"
        ),
        optimized_variant_ranking.get("best_variant_after_retest"),
    )


def _best_variant_decision(
    variant_retest: Mapping[str, Any],
    optimized_variant_ranking: Mapping[str, Any],
) -> str | None:
    return _coalesce_string(
        variant_retest.get("best_variant_decision"),
        _as_mapping(variant_retest.get("decision_update")).get("best_variant_decision"),
        optimized_variant_ranking.get("best_variant_decision"),
    )


def _coalesce_string(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _as_mapping(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _markdown_bullets(values: object) -> str:
    if not isinstance(values, list | tuple):
        return "- 无"
    return "\n".join(f"- `{item}`" for item in values)
