from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    NEXT_DIRECTION_DECISION as SOURCE_2381_NEXT_DIRECTION,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    NEXT_ROUTE as SOURCE_2381_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_optimization_plateau_next_candidate_decision import (
    READY_STATUS as SOURCE_2381_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DECISION_CONTINUE_OPTIMIZATION,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    NEXT_ROUTE as SOURCE_2380_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    OWNER_DECISION as SOURCE_2380_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_optimized_variant_owner_review_decision import (
    READY_STATUS as SOURCE_2380_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    BEST_LOWER_TURNOVER_VARIANT,
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT,
    RANKING_TOP_CANDIDATE,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    NEXT_ROUTE as SOURCE_2382_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_turnover_retest_plan import (
    READY_STATUS as SOURCE_2382_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    NEXT_ROUTE as SOURCE_2383_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_ranking_top_guarded_variant_retest import (
    READY_STATUS as SOURCE_2383_READY_STATUS,
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
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2384"
TASK_REGISTER_ID = (
    "TRADING-2384_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_"
    "OBSERVATION_DECISION"
)
REPORT_TYPE = "dynamic_strategy_guarded_variant_owner_review_decision"
SCHEMA_VERSION = (
    "dynamic_strategy_guarded_variant_owner_review_and_observation_decision.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_"
    "BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = (
    "DO_NOT_APPROVE_OBSERVATION_EXPAND_CANDIDATE_POOL_REVIEW_REQUIRED"
)
NEXT_DIRECTION = "OPTION_C_EXPAND_CANDIDATE_POOL_AND_SIGNAL_FAMILIES"
NEXT_ROUTE = (
    "TRADING-2385_Dynamic_Strategy_Candidate_Pool_Expansion_And_Signal_Family_"
    "Diversification_Plan"
)
PRIMARY_EXECUTION_CADENCE = "valid_until_window"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_REVIEW_ONLY_NO_FRESH_MARKET_DATA"
)
BEST_GUARDED_VARIANT = "equal_risk_growth_tilt_guarded_turnover_v1"
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2379",
    "TRADING-2380",
    "TRADING-2381",
    "TRADING-2382",
    "TRADING-2383",
)
DECISION_OPTIONS: tuple[str, ...] = (
    "OPTION_A_CONTINUE_LOWER_TURNOVER_LOCAL_OPTIMIZATION",
    "OPTION_B_CONTINUE_RANKING_TOP_GUARDED_LOCAL_OPTIMIZATION",
    "OPTION_C_EXPAND_CANDIDATE_POOL_AND_SIGNAL_FAMILIES",
    "OPTION_D_PAUSE_DYNAMIC_STRATEGY_AND_COLLECT_FORWARD_EVIDENCE",
    "OPTION_E_STOP_DYNAMIC_STRATEGY_LINE_FOR_NOW",
)
OBSERVATION_REJECTION_REASONS: tuple[str, ...] = (
    "LOWER_TURNOVER_LINE_REMAINS_CONTINUE_OPTIMIZATION",
    "RANKING_TOP_GUARDED_LINE_REMAINS_CONTINUE_OPTIMIZATION",
    "NO_CANDIDATE_LINE_MEETS_RESEARCH_ONLY_OBSERVATION_CRITERIA",
    "LOCAL_OPTIMIZATION_PATHS_SHOW_DIMINISHING_RETURNS",
    "OWNER_REVIEW_REQUIRED_BEFORE_ANY_OBSERVATION_LINE_RESTART",
)
EXPANSION_RATIONALE: tuple[str, ...] = (
    "LOWER_TURNOVER_LINE_ALREADY_REJECTED_FOR_OBSERVATION_BY_2380",
    "RANKING_TOP_GUARDED_RETEST_DID_NOT_REACH_OBSERVATION_READINESS",
    "CONTINUING_LOCAL_TWEAKS_RISKS_OVERFITTING_WITHOUT_NEW_SIGNAL_DIVERSITY",
    "NEXT_STAGE_SHOULD_TEST_BROADER_CANDIDATE_POOL_AND_SIGNAL_FAMILIES",
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
    "local_candidate_micro_optimization",
    "new_backtest",
    "new_signal",
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

DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT
    / "guarded_variant_retest_result.json"
)
DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_OUTPUT_ROOT
    / "guarded_variant_ranking.json"
)
DEFAULT_SOURCE_2382_RETEST_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT
    / "retest_plan_result.json"
)
DEFAULT_SOURCE_2382_GUARDED_VARIANT_PLAN_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_OUTPUT_ROOT
    / "guarded_variant_plan.json"
)
DEFAULT_SOURCE_2381_PLATEAU_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT
    / "optimization_plateau_decision.json"
)
DEFAULT_SOURCE_2381_NEXT_DIRECTION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_NEXT_CANDIDATE_DECISION_OUTPUT_ROOT
    / "next_candidate_direction.json"
)
DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "variant_retest_result.json"
)
DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_OUTPUT_ROOT
    / "optimized_variant_ranking.json"
)
DEFAULT_SOURCE_2380_OWNER_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)
DEFAULT_SOURCE_2380_OBSERVATION_REJECTION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "observation_rejection_rationale.json"
)


def run_dynamic_strategy_guarded_variant_owner_review_decision(
    *,
    source_guarded_variant_retest_path: Path = (
        DEFAULT_SOURCE_2383_GUARDED_VARIANT_RETEST_PATH
    ),
    source_guarded_decision_update_path: Path = (
        DEFAULT_SOURCE_2383_DECISION_UPDATE_PATH
    ),
    source_guarded_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2383_GUARDED_VARIANT_RANKING_PATH
    ),
    source_retest_plan_path: Path = DEFAULT_SOURCE_2382_RETEST_PLAN_PATH,
    source_guarded_variant_plan_path: Path = (
        DEFAULT_SOURCE_2382_GUARDED_VARIANT_PLAN_PATH
    ),
    source_plateau_decision_path: Path = DEFAULT_SOURCE_2381_PLATEAU_DECISION_PATH,
    source_next_direction_path: Path = DEFAULT_SOURCE_2381_NEXT_DIRECTION_PATH,
    source_variant_retest_path: Path = DEFAULT_SOURCE_2379_VARIANT_RETEST_PATH,
    source_optimized_variant_ranking_path: Path = (
        DEFAULT_SOURCE_2379_OPTIMIZED_VARIANT_RANKING_PATH
    ),
    source_owner_review_path: Path = DEFAULT_SOURCE_2380_OWNER_REVIEW_PATH,
    source_observation_rejection_path: Path = (
        DEFAULT_SOURCE_2380_OBSERVATION_REJECTION_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_guarded_variant_retest_path=source_guarded_variant_retest_path,
        source_guarded_decision_update_path=source_guarded_decision_update_path,
        source_guarded_variant_ranking_path=source_guarded_variant_ranking_path,
        source_retest_plan_path=source_retest_plan_path,
        source_guarded_variant_plan_path=source_guarded_variant_plan_path,
        source_plateau_decision_path=source_plateau_decision_path,
        source_next_direction_path=source_next_direction_path,
        source_variant_retest_path=source_variant_retest_path,
        source_optimized_variant_ranking_path=source_optimized_variant_ranking_path,
        source_owner_review_path=source_owner_review_path,
        source_observation_rejection_path=source_observation_rejection_path,
    )
    ready = not sources["source_validation_errors"]
    resolved_as_of = _resolve_as_of(as_of_date, sources["guarded_variant_retest"])
    two_line_review = _two_line_candidate_review(sources=sources, ready=ready)
    rejection = _observation_rejection_after_guarded_retest(
        two_line_review=two_line_review,
        ready=ready,
    )
    next_direction = _next_research_direction_decision(
        two_line_review=two_line_review,
        ready=ready,
    )
    owner_record = _owner_review_decision_record(
        sources=sources,
        two_line_review=two_line_review,
        rejection=rejection,
        next_direction=next_direction,
        as_of_date=resolved_as_of,
        ready=ready,
    )
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        two_line_review=two_line_review,
        rejection=rejection,
        next_direction=next_direction,
        owner_record=owner_record,
        ready=ready,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_guarded_variant_retest_path: Path,
    source_guarded_decision_update_path: Path,
    source_guarded_variant_ranking_path: Path,
    source_retest_plan_path: Path,
    source_guarded_variant_plan_path: Path,
    source_plateau_decision_path: Path,
    source_next_direction_path: Path,
    source_variant_retest_path: Path,
    source_optimized_variant_ranking_path: Path,
    source_owner_review_path: Path,
    source_observation_rejection_path: Path,
) -> dict[str, Any]:
    guarded_variant_retest = _load_json_document(source_guarded_variant_retest_path)
    guarded_decision_update = _load_json_document(source_guarded_decision_update_path)
    guarded_variant_ranking = _load_json_document(source_guarded_variant_ranking_path)
    retest_plan = _load_json_document(source_retest_plan_path)
    guarded_variant_plan = _load_json_document(source_guarded_variant_plan_path)
    plateau_decision = _load_json_document(source_plateau_decision_path)
    next_direction = _load_json_document(source_next_direction_path)
    variant_retest = _load_json_document(source_variant_retest_path)
    optimized_variant_ranking = _load_json_document(
        source_optimized_variant_ranking_path
    )
    owner_review = _load_json_document(source_owner_review_path)
    observation_rejection = _load_json_document(source_observation_rejection_path)

    source_status = {
        "guarded_variant_retest": guarded_variant_retest.get("status"),
        "guarded_decision_update": guarded_decision_update.get("status"),
        "guarded_variant_ranking": guarded_variant_ranking.get("status"),
        "retest_plan": retest_plan.get("status"),
        "guarded_variant_plan": guarded_variant_plan.get("status"),
        "plateau_decision": plateau_decision.get("status"),
        "next_direction": next_direction.get("status"),
        "variant_retest": variant_retest.get("status"),
        "optimized_variant_ranking": optimized_variant_ranking.get("status"),
        "owner_review": owner_review.get("status"),
        "observation_rejection": observation_rejection.get("status"),
    }
    source_files = {
        "guarded_variant_retest": str(source_guarded_variant_retest_path),
        "guarded_decision_update": str(source_guarded_decision_update_path),
        "guarded_variant_ranking": str(source_guarded_variant_ranking_path),
        "retest_plan": str(source_retest_plan_path),
        "guarded_variant_plan": str(source_guarded_variant_plan_path),
        "plateau_decision": str(source_plateau_decision_path),
        "next_direction": str(source_next_direction_path),
        "variant_retest": str(source_variant_retest_path),
        "optimized_variant_ranking": str(source_optimized_variant_ranking_path),
        "owner_review": str(source_owner_review_path),
        "observation_rejection": str(source_observation_rejection_path),
    }
    sources: dict[str, Any] = {
        "guarded_variant_retest": guarded_variant_retest,
        "guarded_decision_update": guarded_decision_update,
        "guarded_variant_ranking": guarded_variant_ranking,
        "retest_plan": retest_plan,
        "guarded_variant_plan": guarded_variant_plan,
        "plateau_decision": plateau_decision,
        "next_direction": next_direction,
        "variant_retest": variant_retest,
        "optimized_variant_ranking": optimized_variant_ranking,
        "owner_review": owner_review,
        "observation_rejection": observation_rejection,
        "source_status": source_status,
        "source_files": source_files,
        "source_hashes": {
            key: _file_sha256(Path(path)) for key, path in source_files.items()
        },
        "lower_turnover_base_candidate": _coalesce_string(
            variant_retest.get("base_candidate"),
            owner_review.get("base_candidate"),
        ),
        "lower_turnover_best_variant": _best_lower_turnover_variant(
            variant_retest,
            optimized_variant_ranking,
            owner_review,
        ),
        "lower_turnover_decision": _best_lower_turnover_decision(
            variant_retest,
            optimized_variant_ranking,
            owner_review,
        ),
        "lower_turnover_observation_approved": _find_nested_bool(
            owner_review,
            "research_only_observation_approved",
        ),
        "ranking_top_candidate": _coalesce_string(
            guarded_variant_retest.get("ranking_top_candidate"),
            retest_plan.get("ranking_top_candidate"),
            guarded_variant_plan.get("ranking_top_candidate"),
        ),
        "best_guarded_variant": _best_guarded_variant(
            guarded_variant_retest,
            guarded_decision_update,
            guarded_variant_ranking,
        ),
        "best_guarded_variant_decision": _best_guarded_variant_decision(
            guarded_variant_retest,
            guarded_decision_update,
            guarded_variant_ranking,
        ),
        "guarded_candidate_ready_for_observation": _find_nested_bool(
            guarded_variant_retest,
            "candidate_ready_for_research_only_observation",
        ),
        "primary_execution_cadence": _coalesce_string(
            guarded_variant_retest.get("primary_execution_cadence"),
            retest_plan.get("primary_execution_cadence"),
            variant_retest.get("primary_execution_cadence"),
            owner_review.get("primary_execution_cadence"),
        ),
        "next_direction_from_2381": _extract_next_direction(
            plateau_decision,
            next_direction,
        ),
        "source_validation_errors": [],
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    status_expectations = {
        "guarded_variant_retest": SOURCE_2383_READY_STATUS,
        "guarded_decision_update": SOURCE_2383_READY_STATUS,
        "guarded_variant_ranking": SOURCE_2383_READY_STATUS,
        "retest_plan": SOURCE_2382_READY_STATUS,
        "guarded_variant_plan": SOURCE_2382_READY_STATUS,
        "plateau_decision": SOURCE_2381_READY_STATUS,
        "next_direction": SOURCE_2381_READY_STATUS,
        "variant_retest": SOURCE_2379_READY_STATUS,
        "optimized_variant_ranking": SOURCE_2379_READY_STATUS,
        "owner_review": SOURCE_2380_READY_STATUS,
        "observation_rejection": SOURCE_2380_READY_STATUS,
    }
    source_status = _as_mapping(sources.get("source_status"))
    for key, expected in status_expectations.items():
        actual = source_status.get(key)
        if actual != expected:
            errors.append(f"{key}.status expected {expected}, got {actual!r}")

    _expect_equal(
        errors,
        "lower_turnover_base_candidate",
        sources.get("lower_turnover_base_candidate"),
        BASE_CANDIDATE_ID,
    )
    _expect_equal(
        errors,
        "lower_turnover_best_variant",
        sources.get("lower_turnover_best_variant"),
        BEST_LOWER_TURNOVER_VARIANT,
    )
    _expect_equal(
        errors,
        "lower_turnover_decision",
        sources.get("lower_turnover_decision"),
        DECISION_CONTINUE_OPTIMIZATION,
    )
    _expect_equal(
        errors,
        "ranking_top_candidate",
        sources.get("ranking_top_candidate"),
        RANKING_TOP_CANDIDATE,
    )
    _expect_equal(
        errors,
        "ranking_top_reference",
        RANKING_TOP_REFERENCE,
        RANKING_TOP_CANDIDATE,
    )
    _expect_equal(
        errors,
        "best_guarded_variant",
        sources.get("best_guarded_variant"),
        BEST_GUARDED_VARIANT,
    )
    _expect_equal(
        errors,
        "best_guarded_variant_decision",
        sources.get("best_guarded_variant_decision"),
        DECISION_CONTINUE_OPTIMIZATION,
    )
    _expect_equal(
        errors,
        "primary_execution_cadence",
        sources.get("primary_execution_cadence"),
        PRIMARY_EXECUTION_CADENCE,
    )
    _expect_equal(
        errors,
        "next_direction_from_2381",
        sources.get("next_direction_from_2381"),
        SOURCE_2381_NEXT_DIRECTION,
    )

    guarded_variant_retest = _as_mapping(sources.get("guarded_variant_retest"))
    if guarded_variant_retest.get("recommended_next_research_task") != (
        SOURCE_2383_EXPECTED_ROUTE
    ):
        errors.append(
            "guarded_variant_retest.recommended_next_research_task expected "
            f"{SOURCE_2383_EXPECTED_ROUTE}, got "
            f"{guarded_variant_retest.get('recommended_next_research_task')!r}"
        )
    if guarded_variant_retest.get("data_quality_gate_executed") is not True:
        errors.append("2383 data_quality_gate_executed must be true")
    if guarded_variant_retest.get("data_quality_passed") is not True:
        errors.append("2383 data_quality_passed must be true")
    if _find_nested_bool(
        guarded_variant_retest,
        "candidate_ready_for_research_only_observation",
    ) is not False:
        errors.append("2383 candidate_ready_for_research_only_observation is not false")

    retest_plan = _as_mapping(sources.get("retest_plan"))
    if retest_plan.get("recommended_next_research_task") != SOURCE_2382_EXPECTED_ROUTE:
        errors.append(
            "retest_plan.recommended_next_research_task expected "
            f"{SOURCE_2382_EXPECTED_ROUTE}, "
            f"got {retest_plan.get('recommended_next_research_task')!r}"
        )

    plateau_decision = _as_mapping(sources.get("plateau_decision"))
    if plateau_decision.get("recommended_next_research_task") != (
        SOURCE_2381_EXPECTED_ROUTE
    ):
        errors.append(
            "plateau_decision.recommended_next_research_task expected "
            f"{SOURCE_2381_EXPECTED_ROUTE}, "
            f"got {plateau_decision.get('recommended_next_research_task')!r}"
        )

    variant_retest = _as_mapping(sources.get("variant_retest"))
    if variant_retest.get("recommended_next_research_task") != (
        SOURCE_2379_EXPECTED_ROUTE
    ):
        errors.append(
            "variant_retest.recommended_next_research_task expected "
            f"{SOURCE_2379_EXPECTED_ROUTE}, "
            f"got {variant_retest.get('recommended_next_research_task')!r}"
        )

    owner_review = _as_mapping(sources.get("owner_review"))
    if owner_review.get("recommended_next_research_task") != SOURCE_2380_EXPECTED_ROUTE:
        errors.append(
            "owner_review.recommended_next_research_task expected "
            f"{SOURCE_2380_EXPECTED_ROUTE}, "
            f"got {owner_review.get('recommended_next_research_task')!r}"
        )
    if owner_review.get("owner_decision") != SOURCE_2380_OWNER_DECISION:
        errors.append(
            f"owner_review.owner_decision expected {SOURCE_2380_OWNER_DECISION}, "
            f"got {owner_review.get('owner_decision')!r}"
        )
    if _find_nested_bool(owner_review, "research_only_observation_approved") is not False:
        errors.append("2380 research_only_observation_approved must be false")

    observation_rejection = _as_mapping(sources.get("observation_rejection"))
    if (
        _find_nested_bool(
            observation_rejection,
            "research_only_observation_approved",
        )
        is not False
    ):
        errors.append("2380 observation rejection must keep observation false")

    for source_name in (
        "guarded_variant_retest",
        "guarded_decision_update",
        "guarded_variant_ranking",
        "retest_plan",
        "guarded_variant_plan",
        "plateau_decision",
        "next_direction",
        "variant_retest",
        "optimized_variant_ranking",
        "owner_review",
        "observation_rejection",
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


def _two_line_candidate_review(
    *,
    sources: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    lower_line = {
        "line_id": "lower_turnover_line",
        "source_tasks": ["TRADING-2379", "TRADING-2380"],
        "base_candidate": sources.get("lower_turnover_base_candidate"),
        "best_variant": sources.get("lower_turnover_best_variant"),
        "decision": sources.get("lower_turnover_decision"),
        "owner_review_decision_from_2380": SOURCE_2380_OWNER_DECISION,
        "observation_approved": False,
        "research_only_observation_approved": False,
        "line_status": "NOT_OBSERVATION_READY",
        "next_local_action_allowed": False,
    }
    ranking_line = {
        "line_id": "ranking_top_guarded_line",
        "source_tasks": ["TRADING-2381", "TRADING-2382", "TRADING-2383"],
        "base_candidate": sources.get("ranking_top_candidate"),
        "best_variant": sources.get("best_guarded_variant"),
        "decision": sources.get("best_guarded_variant_decision"),
        "candidate_ready_for_research_only_observation": False,
        "observation_approved": False,
        "research_only_observation_approved": False,
        "line_status": "NOT_OBSERVATION_READY",
        "next_local_action_allowed": False,
    }
    return {
        "schema_version": "dynamic_strategy_two_line_candidate_review.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "two_line_candidate_review_ready": ready,
        "candidate_line_count": 2,
        "primary_execution_cadence": sources.get("primary_execution_cadence"),
        "lower_turnover_line": lower_line,
        "ranking_top_guarded_line": ranking_line,
        "both_lines_observation_approved": False,
        "no_line_ready_for_research_only_observation": True,
        "continue_local_optimization_allowed": False,
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
        "review_conclusion": (
            "NO_DYNAMIC_STRATEGY_LINE_READY_FOR_RESEARCH_ONLY_OBSERVATION"
        ),
        "evidence_summary": list(EXPANSION_RATIONALE),
    }


def _observation_rejection_after_guarded_retest(
    *,
    two_line_review: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": (
            "dynamic_strategy_observation_rejection_after_guarded_retest.v1"
        ),
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "observation_rejection_ready": ready,
        "research_only_observation_approved": False,
        "paper_shadow_approved": False,
        "owner_review_required_before_observation_restart": True,
        "observation_rejection_reasons": list(OBSERVATION_REJECTION_REASONS),
        "two_line_candidate_review": dict(two_line_review),
        "non_approved_paths": list(NON_APPROVED_PATHS),
    }


def _next_research_direction_decision(
    *,
    two_line_review: Mapping[str, Any],
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_next_research_direction_decision.v1",
        "status": READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        "next_research_direction_decision_ready": ready,
        "decision_options": [
            {
                "option": option,
                "decision": "SELECT" if option == NEXT_DIRECTION else "NOT_SELECTED",
            }
            for option in DECISION_OPTIONS
        ],
        "next_direction": NEXT_DIRECTION,
        "recommended_default_direction": NEXT_DIRECTION,
        "recommended_next_research_task": NEXT_ROUTE,
        "continue_local_optimization_allowed": False,
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
        "local_optimization_disallowed_reasons": list(EXPANSION_RATIONALE),
        "required_plan_scope_for_2385": {
            "candidate_pool_expansion": True,
            "signal_family_diversification": True,
            "reuse_existing_valid_until_window": True,
            "no_observation_restart_without_owner_review": True,
        },
        "two_line_candidate_review_summary": {
            "lower_turnover_line": _as_mapping(
                two_line_review.get("lower_turnover_line")
            ),
            "ranking_top_guarded_line": _as_mapping(
                two_line_review.get("ranking_top_guarded_line")
            ),
        },
    }


def _owner_review_decision_record(
    *,
    sources: Mapping[str, Any],
    two_line_review: Mapping[str, Any],
    rejection: Mapping[str, Any],
    next_direction: Mapping[str, Any],
    as_of_date: date,
    ready: bool,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "as_of": as_of_date.isoformat(),
        "owner_review_decision_recorded": ready,
        "owner_decision": OWNER_DECISION if ready else "BLOCKED_SOURCE_ARTIFACT",
        "lower_turnover_line": _as_mapping(
            two_line_review.get("lower_turnover_line")
        ),
        "ranking_top_guarded_line": _as_mapping(
            two_line_review.get("ranking_top_guarded_line")
        ),
        "best_variant_from_2379": sources.get("lower_turnover_best_variant"),
        "best_guarded_variant_from_2383": sources.get("best_guarded_variant"),
        "research_only_observation_approved": False,
        "continue_local_optimization_allowed": False,
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
        "observation_rejection_reasons": list(
            rejection.get("observation_rejection_reasons", [])
        ),
        "next_research_direction_decision": dict(next_direction),
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    two_line_review: Mapping[str, Any],
    rejection: Mapping[str, Any],
    next_direction: Mapping[str, Any],
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
        "new_signal_generated": False,
        "scoring_run": False,
        "research_only": True,
        "observe_only": True,
        "primary_execution_cadence": sources.get("primary_execution_cadence"),
        "lower_turnover_line": _as_mapping(
            two_line_review.get("lower_turnover_line")
        ),
        "ranking_top_guarded_line": _as_mapping(
            two_line_review.get("ranking_top_guarded_line")
        ),
        "two_line_candidate_review_ready": ready,
        "two_line_candidate_review": dict(two_line_review),
        "observation_rejection_after_guarded_retest_ready": ready,
        "observation_rejection_after_guarded_retest": dict(rejection),
        "next_research_direction_decision_ready": ready,
        "next_research_direction_decision": dict(next_direction),
        "owner_review_decision_recorded": bool(
            owner_record.get("owner_review_decision_recorded")
        ),
        "owner_decision": owner_record.get("owner_decision"),
        "owner_review_decision": dict(owner_record),
        "research_only_observation_approved": False,
        "continue_local_optimization_allowed": False,
        "candidate_pool_expansion_recommended": True,
        "signal_family_diversification_recommended": True,
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
                f"Lower-turnover line best variant is {BEST_LOWER_TURNOVER_VARIANT} "
                f"with decision={DECISION_CONTINUE_OPTIMIZATION}."
            ),
            (
                f"Ranking-top guarded line best variant is {BEST_GUARDED_VARIANT} "
                f"with decision={DECISION_CONTINUE_OPTIMIZATION}."
            ),
            (
                "No candidate line is approved for research-only observation; "
                "local optimization is closed pending candidate-pool expansion."
            ),
        ],
        "artifact_paths": {},
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    owner_json = output_root / "owner_review_decision.json"
    two_line_json = output_root / "two_line_candidate_review.json"
    next_direction_json = output_root / "next_research_direction_decision.json"
    markdown_path = docs_root / "dynamic_strategy_guarded_variant_owner_review_decision.md"
    two_line_markdown = docs_root / "dynamic_strategy_two_line_candidate_review.md"
    rejection_markdown = (
        docs_root / "dynamic_strategy_observation_rejection_after_guarded_retest.md"
    )
    route_markdown = docs_root / "dynamic_strategy_2385_route.md"
    payload["artifact_paths"] = {
        "json_path": str(owner_json),
        "owner_review_decision_json": str(owner_json),
        "two_line_candidate_review_json": str(two_line_json),
        "next_research_direction_decision_json": str(next_direction_json),
        "markdown_path": str(markdown_path),
        "two_line_candidate_review_markdown": str(two_line_markdown),
        "observation_rejection_after_guarded_retest_markdown": str(
            rejection_markdown
        ),
        "next_route_markdown": str(route_markdown),
    }
    owner_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    two_line_json.write_text(
        json.dumps(
            {
                "generated_at": payload["generated_at"],
                "report_type": REPORT_TYPE,
                "schema_version": "dynamic_strategy_two_line_candidate_review.v1",
                "status": payload["status"],
                "production_effect": "none",
                "broker_action": "none",
                "two_line_candidate_review": payload["two_line_candidate_review"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    next_direction_json.write_text(
        json.dumps(
            {
                "generated_at": payload["generated_at"],
                "report_type": REPORT_TYPE,
                "schema_version": (
                    "dynamic_strategy_next_research_direction_decision.v1"
                ),
                "status": payload["status"],
                "production_effect": "none",
                "broker_action": "none",
                "next_research_direction_decision": payload[
                    "next_research_direction_decision"
                ],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    markdown_path.write_text(_main_markdown(payload), encoding="utf-8")
    two_line_markdown.write_text(_two_line_markdown(payload), encoding="utf-8")
    rejection_markdown.write_text(_rejection_markdown(payload), encoding="utf-8")
    route_markdown.write_text(_route_markdown(payload), encoding="utf-8")


def _main_markdown(payload: Mapping[str, Any]) -> str:
    lower_line = _as_mapping(payload.get("lower_turnover_line"))
    ranking_line = _as_mapping(payload.get("ranking_top_guarded_line"))
    return "\n".join(
        [
            "# Dynamic strategy guarded variant owner review decision",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload['status']}`",
            f"- owner decision：`{payload['owner_decision']}`",
            "- research-only observation approved：`false`",
            "- continue local optimization allowed：`false`",
            "- candidate pool expansion recommended：`true`",
            "- signal family diversification recommended：`true`",
            f"- next direction：`{NEXT_DIRECTION}`",
            f"- next route：`{payload['recommended_next_research_task']}`",
            "",
            "## Two-line review",
            "",
            f"- lower-turnover best variant：`{lower_line.get('best_variant')}`",
            f"- lower-turnover decision：`{lower_line.get('decision')}`",
            "- lower-turnover observation approved：`false`",
            f"- ranking-top guarded best variant：`{ranking_line.get('best_variant')}`",
            f"- ranking-top guarded decision：`{ranking_line.get('decision')}`",
            "- ranking-top guarded observation approved：`false`",
            "",
            "## Decision rationale",
            "",
            _markdown_bullets(EXPANSION_RATIONALE),
            "",
            "## Data quality gate boundary",
            "",
            "- data_quality_gate_executed：`false`",
            f"- data_quality_gate_reason：`{DATA_QUALITY_GATE_REASON}`",
            "- reason：本任务只读取 prior artifacts，不读取 fresh cached market data，"
            "不重新 backtest，不生成新 signal / scoring / daily report。",
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
        ]
    )


def _two_line_markdown(payload: Mapping[str, Any]) -> str:
    review = _as_mapping(payload.get("two_line_candidate_review"))
    lower_line = _as_mapping(review.get("lower_turnover_line"))
    ranking_line = _as_mapping(review.get("ranking_top_guarded_line"))
    return "\n".join(
        [
            "# Dynamic strategy two-line candidate review",
            "",
            f"- status：`{payload['status']}`",
            f"- primary execution cadence：`{payload['primary_execution_cadence']}`",
            "- no line ready for research-only observation：`true`",
            "",
            "## Lower-turnover line",
            "",
            f"- best variant：`{lower_line.get('best_variant')}`",
            f"- decision：`{lower_line.get('decision')}`",
            "- observation approved：`false`",
            "",
            "## Ranking-top guarded line",
            "",
            f"- best variant：`{ranking_line.get('best_variant')}`",
            f"- decision：`{ranking_line.get('decision')}`",
            "- candidate ready for research-only observation：`false`",
            "- observation approved：`false`",
            "",
        ]
    )


def _rejection_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy observation rejection after guarded retest",
            "",
            f"- status：`{payload['status']}`",
            "- research-only observation approved：`false`",
            "- paper-shadow approved：`false`",
            "- continue local optimization allowed：`false`",
            "",
            "## Rejection reasons",
            "",
            _markdown_bullets(OBSERVATION_REJECTION_REASONS),
            "",
            "## Non-approved paths",
            "",
            _markdown_bullets(NON_APPROVED_PATHS),
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2385 route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- status：`{payload['status']}`",
            f"- owner decision：`{payload['owner_decision']}`",
            f"- next direction：`{NEXT_DIRECTION}`",
            f"- next route：`{payload['recommended_next_research_task']}`",
            "- route reason：TRADING-2379 lower-turnover line 与 TRADING-2383 "
            "ranking-top guarded line 均未达到 observation readiness；下一步只允许"
            "候选池扩展与 signal family diversification plan。",
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


def _resolve_as_of(
    as_of_date: date | None,
    guarded_variant_retest: Mapping[str, Any],
) -> date:
    if as_of_date is not None:
        return as_of_date
    value = guarded_variant_retest.get("as_of")
    if isinstance(value, str):
        return date.fromisoformat(value)
    return date.today()


def _best_lower_turnover_variant(
    variant_retest: Mapping[str, Any],
    optimized_variant_ranking: Mapping[str, Any],
    owner_review: Mapping[str, Any],
) -> str | None:
    return _coalesce_string(
        variant_retest.get("best_variant_after_retest"),
        _as_mapping(variant_retest.get("decision_update")).get(
            "best_variant_after_retest"
        ),
        optimized_variant_ranking.get("best_variant_after_retest"),
        owner_review.get("best_variant_from_2379"),
    )


def _best_lower_turnover_decision(
    variant_retest: Mapping[str, Any],
    optimized_variant_ranking: Mapping[str, Any],
    owner_review: Mapping[str, Any],
) -> str | None:
    return _coalesce_string(
        variant_retest.get("best_variant_decision"),
        _as_mapping(variant_retest.get("decision_update")).get("best_variant_decision"),
        optimized_variant_ranking.get("best_variant_decision"),
        owner_review.get("best_variant_decision_from_2379"),
    )


def _best_guarded_variant(
    guarded_variant_retest: Mapping[str, Any],
    guarded_decision_update: Mapping[str, Any],
    guarded_variant_ranking: Mapping[str, Any],
) -> str | None:
    ranking = guarded_variant_ranking.get("guarded_variant_ranking")
    ranking_best = None
    if isinstance(ranking, list) and ranking and isinstance(ranking[0], Mapping):
        ranking_best = ranking[0].get("variant_id")
    return _coalesce_string(
        guarded_variant_retest.get("best_guarded_variant"),
        _as_mapping(guarded_variant_retest.get("decision_update")).get(
            "best_guarded_variant"
        ),
        _as_mapping(guarded_decision_update.get("decision_update")).get(
            "best_guarded_variant"
        ),
        ranking_best,
    )


def _best_guarded_variant_decision(
    guarded_variant_retest: Mapping[str, Any],
    guarded_decision_update: Mapping[str, Any],
    guarded_variant_ranking: Mapping[str, Any],
) -> str | None:
    ranking = guarded_variant_ranking.get("guarded_variant_ranking")
    ranking_decision = None
    if isinstance(ranking, list) and ranking and isinstance(ranking[0], Mapping):
        ranking_decision = ranking[0].get("decision")
    return _coalesce_string(
        guarded_variant_retest.get("best_guarded_variant_decision"),
        _as_mapping(guarded_variant_retest.get("decision_update")).get(
            "best_guarded_variant_decision"
        ),
        _as_mapping(guarded_decision_update.get("decision_update")).get(
            "best_guarded_variant_decision"
        ),
        ranking_decision,
    )


def _extract_next_direction(
    plateau_decision: Mapping[str, Any],
    next_direction: Mapping[str, Any],
) -> str | None:
    return _coalesce_string(
        plateau_decision.get("next_direction_decision"),
        _as_mapping(plateau_decision.get("next_candidate_direction")).get(
            "next_direction_decision"
        ),
        next_direction.get("next_direction_decision"),
        _as_mapping(next_direction.get("next_candidate_direction")).get(
            "next_direction_decision"
        ),
    )


def _find_nested_bool(document: Mapping[str, Any], field: str) -> bool | None:
    value = document.get(field)
    if isinstance(value, bool):
        return value
    for nested in document.values():
        if isinstance(nested, Mapping):
            found = _find_nested_bool(nested, field)
            if isinstance(found, bool):
                return found
    return None


def _expect_equal(
    errors: list[str],
    field: str,
    actual: object,
    expected: object,
) -> None:
    if actual != expected:
        errors.append(f"{field} expected {expected!r}, got {actual!r}")


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
