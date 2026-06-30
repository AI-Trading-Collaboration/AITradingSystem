from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.refined_candidate_local_edge_scope_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_REVIEW_ROOT,
)
from ai_trading_system.regenerated_candidate_generator_common import parse_csv_list
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_CAP_CANDIDATE_ID,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_VALIDATION_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_SCOPE_GENERATOR_ROOT,
)
from ai_trading_system.scope_narrowed_candidate_generators_regenerate import (
    RISK_APPETITE_ARCHIVE_CANDIDATE,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "scope_narrowed_forward_observe_readiness_review"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2293_SCOPE_NARROWED_FORWARD_OBSERVE_READINESS_REVIEW"
STATUS = "FORWARD_OBSERVE_READINESS_REVIEW_READY_PROMOTION_BLOCKED"
MODE = "forward_observe_readiness_review"
ARTIFACT_ROLE = "scope_narrowed_forward_observe_readiness_review"
REPORT_TYPE = "scope_narrowed_forward_observe_readiness_review"

EXPECTED_2292_STATE = "SCOPE_NARROWED_VALIDATED_FORWARD_OBSERVE_CANDIDATE"
REJECTED_2292_STATE = "SCOPE_NARROWED_VALIDATED_REJECT_RECOMMENDED"
USAGE_ROLE = "risk_cap_only"
OBSERVE_MODE = "forward_observe_only"
ALLOWED_ACTION = "observe_only"

READY_RECOMMENDED = "FORWARD_OBSERVE_READY_RECOMMENDED"
READY_WITH_WARNINGS = "FORWARD_OBSERVE_READY_WITH_WARNINGS"
NOT_READY = "FORWARD_OBSERVE_NOT_READY"
BLOCKED = "FORWARD_OBSERVE_BLOCKED"
BLOCKED_BY_2292_STATE = "BLOCKED_BY_2292_STATE"

NEXT_TASK_RUNTIME = "TRADING-2294_Forward_Observe_Runtime_Evidence_Collection_Plan"
NEXT_TASK_EXTENSION = "TRADING-2294_Evidence_Accumulation_Extension_Plan"
NEXT_TASK_ARCHIVE = "TRADING-2294_Archive_Scope_Narrowed_Risk_Cap_Candidate"

BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}
BANNED_ALLOWED_ACTIONS = {
    "reduce_position",
    "sell",
    "buy",
    "rebalance",
    "broker_action",
}

REQUIRED_SCOPE_VALIDATION_FILES = {
    "summary": "scope_narrowed_actual_path_validation_summary.json",
    "risk_cap_scorecard": "risk_cap_only_validation_scorecard.json",
    "comparison": "scope_narrowed_active_vs_inactive_comparison.json",
    "sample_sufficiency": "scope_narrowed_sample_sufficiency_report.json",
    "false_signal_cost": "scope_narrowed_false_signal_cost_matrix.json",
    "state_recommendation": "scope_narrowed_state_recommendation_matrix.json",
    "data_quality": "scope_narrowed_data_quality_report.json",
    "risk_appetite_archive": "risk_appetite_archive_carry_forward.json",
}
REQUIRED_SCOPE_GENERATOR_TOP_LEVEL_FILES = {
    "run_summary": "scope_narrowed_regeneration_run_summary.json",
    "validation_summary": "scope_narrowed_regeneration_validation_summary.json",
    "registry": "scope_narrowed_candidate_registry.json",
    "delta_summary": "scope_narrowed_original_vs_refined_vs_scope_delta_summary.json",
}
REQUIRED_SCOPE_GENERATOR_CANDIDATE_FILES = {
    "signal_spec": "scope_narrowed_candidate_signal_spec.json",
    "signal_series": "scope_narrowed_candidate_signal_series.csv",
    "prediction_artifact": "scope_narrowed_candidate_prediction_artifact.json",
    "generation_summary": "scope_narrowed_generation_summary.json",
    "validation_summary": "scope_narrowed_validation_summary.json",
    "scope_filter_report": "scope_filter_report.json",
    "lineage_report": "scope_narrowed_lineage_report.json",
    "delta": "refined_vs_scope_narrowed_delta.json",
}
REQUIRED_SCOPE_REVIEW_FILES = {
    "summary": "local_edge_scope_review_summary.json",
    "scope_recommendation": "candidate_scope_narrowing_recommendation_matrix.json",
    "direction_scope": "candidate_direction_scope_matrix.json",
    "high_conviction_scope": "candidate_high_conviction_scope_matrix.json",
    "false_cost_scope": "candidate_false_cost_scope_matrix.json",
    "next_task": "candidate_next_task_recommendation_matrix.json",
    "decision_summary": "candidate_scope_review_decision_summary.json",
}

# Research-only readiness defaults from the TRADING-2293 owner brief. They define
# observe-only evidence collection maturity, not promotion, paper-shadow,
# production, broker, or investment interpretation gates.
MINIMUM_OBSERVE_DAYS = 60
MINIMUM_ACTIVE_TRIGGER_COUNT = 10
MINIMUM_REVIEW_WINDOWS = 4
FALSE_RISK_CAP_COST_PER_RECORD_REVIEW_LIMIT = 0.08
MISSED_UPSIDE_COST_PER_RECORD_REVIEW_LIMIT = 0.08

SAFETY_FIELDS: dict[str, Any] = {
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "owner_review_required": False,
    "paper_shadow_recommendation_allowed": False,
    "production_recommendation_allowed": False,
    "broker_action_recommendation_allowed": False,
    "forward_observe_started": False,
}


class ScopeNarrowedForwardObserveReadinessReviewError(ValueError):
    pass


@dataclass(frozen=True)
class ForwardObserveReadinessInputs:
    scope_validation_dir: Path
    scope_generator_dir: Path
    scope_review_dir: Path
    candidate: str
    rejected_candidates: tuple[str, ...]
    archived_candidates: tuple[str, ...]
    target_assets: tuple[str, ...]
    horizons: tuple[str, ...]
    validation_payloads: dict[str, dict[str, Any]]
    generator_top_level_payloads: dict[str, dict[str, Any]]
    generator_candidate_payloads: dict[str, dict[str, Any]]
    scope_review_payloads: dict[str, dict[str, Any]]
    artifact_paths: dict[str, str]


def run_scope_narrowed_forward_observe_readiness_review(
    *,
    scope_validation_dir: Path = DEFAULT_SCOPE_VALIDATION_ROOT,
    scope_generator_dir: Path = DEFAULT_SCOPE_GENERATOR_ROOT,
    scope_review_dir: Path = DEFAULT_SCOPE_REVIEW_ROOT,
    candidate: str = RISK_CAP_CANDIDATE_ID,
    rejected_candidates: Sequence[str] | str = (CONFIRMATION_CANDIDATE_ID,),
    archived_candidates: Sequence[str] | str = (RISK_APPETITE_ARCHIVE_CANDIDATE,),
    target_assets: Sequence[str] | str = ("QQQ", "SPY", "SMH"),
    horizons: Sequence[str] | str = ("5d", "10d", "20d"),
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = MODE,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    if mode != MODE:
        raise ScopeNarrowedForwardObserveReadinessReviewError(
            f"scope-narrowed forward observe readiness review only supports {MODE} mode"
        )
    inputs = load_scope_narrowed_forward_observe_readiness_inputs(
        scope_validation_dir=scope_validation_dir,
        scope_generator_dir=scope_generator_dir,
        scope_review_dir=scope_review_dir,
        candidate=candidate,
        rejected_candidates=rejected_candidates,
        archived_candidates=archived_candidates,
        target_assets=target_assets,
        horizons=horizons,
    )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    artifacts = build_forward_observe_readiness_artifacts(inputs, generated_at=generated_at)
    write_forward_observe_readiness_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    return artifacts["summary"]


def load_scope_narrowed_forward_observe_readiness_inputs(
    *,
    scope_validation_dir: Path,
    scope_generator_dir: Path,
    scope_review_dir: Path,
    candidate: str,
    rejected_candidates: Sequence[str] | str,
    archived_candidates: Sequence[str] | str,
    target_assets: Sequence[str] | str,
    horizons: Sequence[str] | str,
) -> ForwardObserveReadinessInputs:
    rejected_ids = _normalize_list(rejected_candidates)
    archived_ids = _normalize_list(archived_candidates)
    asset_ids = _normalize_list(target_assets)
    horizon_ids = _normalize_list(horizons)
    if candidate != RISK_CAP_CANDIDATE_ID:
        raise ScopeNarrowedForwardObserveReadinessReviewError(
            f"readiness review candidate must be {RISK_CAP_CANDIDATE_ID}: {candidate}"
        )
    if candidate in rejected_ids:
        raise ScopeNarrowedForwardObserveReadinessReviewError(
            "rejected candidate cannot be used as readiness candidate"
        )
    if candidate in archived_ids:
        raise ScopeNarrowedForwardObserveReadinessReviewError(
            "archived candidate cannot be used as readiness candidate"
        )
    if CONFIRMATION_CANDIDATE_ID not in rejected_ids:
        raise ScopeNarrowedForwardObserveReadinessReviewError(
            f"rejected carry-forward must include {CONFIRMATION_CANDIDATE_ID}"
        )
    if RISK_APPETITE_ARCHIVE_CANDIDATE not in archived_ids:
        raise ScopeNarrowedForwardObserveReadinessReviewError(
            f"archived carry-forward must include {RISK_APPETITE_ARCHIVE_CANDIDATE}"
        )

    validation_payloads = _load_required_payloads(
        scope_validation_dir,
        REQUIRED_SCOPE_VALIDATION_FILES,
        required_context="TRADING-2292 scope validation",
    )
    generator_top_level_payloads = _load_required_payloads(
        scope_generator_dir,
        REQUIRED_SCOPE_GENERATOR_TOP_LEVEL_FILES,
        required_context="TRADING-2291 scope generator",
    )
    candidate_dir = scope_generator_dir / candidate
    generator_candidate_payloads = _load_required_payloads(
        candidate_dir,
        REQUIRED_SCOPE_GENERATOR_CANDIDATE_FILES,
        required_context="TRADING-2291 risk-cap candidate generator",
        json_only_keys={
            "signal_spec",
            "prediction_artifact",
            "generation_summary",
            "validation_summary",
            "scope_filter_report",
            "lineage_report",
            "delta",
        },
    )
    scope_review_payloads = _load_required_payloads(
        scope_review_dir,
        REQUIRED_SCOPE_REVIEW_FILES,
        required_context="TRADING-2290 scope review",
    )
    artifact_paths = {
        **{
            f"scope_validation.{key}": str(scope_validation_dir / filename)
            for key, filename in REQUIRED_SCOPE_VALIDATION_FILES.items()
        },
        **{
            f"scope_generator.{key}": str(scope_generator_dir / filename)
            for key, filename in REQUIRED_SCOPE_GENERATOR_TOP_LEVEL_FILES.items()
        },
        **{
            f"candidate.{key}": str(candidate_dir / filename)
            for key, filename in REQUIRED_SCOPE_GENERATOR_CANDIDATE_FILES.items()
        },
        **{
            f"scope_review.{key}": str(scope_review_dir / filename)
            for key, filename in REQUIRED_SCOPE_REVIEW_FILES.items()
        },
    }

    for group, payloads in (
        ("scope_validation", validation_payloads),
        ("generator_top_level", generator_top_level_payloads),
        ("generator_candidate", generator_candidate_payloads),
        ("scope_review", scope_review_payloads),
    ):
        for key, payload in payloads.items():
            if isinstance(payload, Mapping):
                _validate_input_safety(f"{group}.{key}", payload)

    return ForwardObserveReadinessInputs(
        scope_validation_dir=scope_validation_dir,
        scope_generator_dir=scope_generator_dir,
        scope_review_dir=scope_review_dir,
        candidate=candidate,
        rejected_candidates=rejected_ids,
        archived_candidates=archived_ids,
        target_assets=asset_ids,
        horizons=horizon_ids,
        validation_payloads=validation_payloads,
        generator_top_level_payloads=generator_top_level_payloads,
        generator_candidate_payloads=generator_candidate_payloads,
        scope_review_payloads=scope_review_payloads,
        artifact_paths=artifact_paths,
    )


def build_forward_observe_readiness_artifacts(
    inputs: ForwardObserveReadinessInputs,
    *,
    generated_at: datetime,
) -> dict[str, dict[str, Any]]:
    context = _candidate_context(inputs)
    checklist = build_forward_observe_gate_checklist(context)
    readiness_matrix = build_forward_observe_candidate_readiness_matrix(context, checklist)
    evidence_spec = build_forward_observe_evidence_collection_spec(inputs)
    daily_contract = build_forward_observe_daily_report_contract(inputs)
    weekly_contract = build_forward_observe_weekly_review_contract(inputs)
    stop_continue_rules = build_forward_observe_stop_continue_rules()
    operational_boundary = build_forward_observe_operational_boundary(inputs)
    metric_spec = build_risk_cap_forward_observe_metric_spec()
    trigger_spec = build_risk_cap_trigger_interpretation_spec()
    rejected = build_rejected_candidate_carry_forward_matrix(inputs, context)
    archived = build_archived_candidate_carry_forward_matrix(inputs)
    next_task = build_forward_observe_next_task_recommendation(checklist)
    summary = build_forward_observe_readiness_summary(
        inputs=inputs,
        checklist=checklist,
        next_task=next_task,
        generated_at=generated_at,
    )
    common = _common_payload(generated_at)
    return {
        "summary": summary,
        "readiness_matrix": {
            **common,
            "rows": readiness_matrix,
            "readiness_gate_status": checklist["readiness_gate_status"],
        },
        "gate_checklist": {**common, **checklist},
        "evidence_collection_spec": {**common, **evidence_spec},
        "daily_report_contract": {**common, **daily_contract},
        "weekly_review_contract": {**common, **weekly_contract},
        "stop_continue_rules": {**common, **stop_continue_rules},
        "operational_boundary": {**common, **operational_boundary},
        "metric_spec": {**common, **metric_spec},
        "trigger_interpretation_spec": {**common, **trigger_spec},
        "rejected_carry_forward": {**common, "rows": rejected},
        "archived_carry_forward": {**common, "rows": archived},
        "next_task_recommendation": {**common, **next_task},
        "docs": build_forward_observe_readiness_docs(
            summary=summary,
            checklist=checklist,
            evidence_spec=evidence_spec,
            stop_continue_rules=stop_continue_rules,
            rejected=rejected,
            archived=archived,
        ),
    }


def build_forward_observe_gate_checklist(context: Mapping[str, Any]) -> dict[str, Any]:
    source_state = str(context.get("source_state_from_2292", ""))
    data_quality_status = str(context.get("data_quality_status", "UNKNOWN"))
    sample_status = str(context.get("sample_sufficiency_status", "UNKNOWN"))
    comparison_label = str(context.get("active_vs_inactive_comparison_label", "UNKNOWN"))
    false_cost_status = _false_signal_cost_status(context)
    risk_cap_capture_status = _risk_cap_capture_status(context)
    artifact_validation_status = str(context.get("artifact_validation_status", "UNKNOWN"))
    operational_boundary_status = "PASS"
    blockers: list[str] = []
    warning_reasons: list[str] = []

    if source_state != EXPECTED_2292_STATE:
        blockers.append(BLOCKED_BY_2292_STATE)
    if data_quality_status not in {"PASS", "PASS_WITH_WARNINGS"}:
        blockers.append("DATA_QUALITY_NOT_PASSING")
    if artifact_validation_status != "PASS":
        blockers.append("ARTIFACT_VALIDATION_NOT_PASSING")
    if operational_boundary_status != "PASS":
        blockers.append("OPERATIONAL_BOUNDARY_NOT_PASSING")
    if false_cost_status in {"BOTH_FALSE_COSTS_TOO_HIGH", "FALSE_RISK_CAP_COST_TOO_HIGH"}:
        blockers.append(false_cost_status)

    if sample_status not in {"SAMPLE_SUFFICIENT", "SAMPLE_THIN_BUT_USABLE"}:
        blockers.append("SAMPLE_NOT_READY")
    if comparison_label not in {
        "ACTIVE_SCOPE_OUTPERFORMS_REFERENCE",
        "ACTIVE_SCOPE_WEAKLY_BETTER",
    }:
        blockers.append("ACTIVE_SCOPE_NOT_BETTER_THAN_REFERENCE")

    if data_quality_status == "PASS_WITH_WARNINGS":
        warning_reasons.append("DATA_QUALITY_PASS_WITH_WARNINGS")
    if sample_status == "SAMPLE_THIN_BUT_USABLE":
        warning_reasons.append("SAMPLE_THIN_BUT_USABLE")
    if int(context.get("asset_horizon_sparse_bucket_count", 0) or 0) > 0:
        warning_reasons.append("ASSET_HORIZON_BUCKETS_SPARSE")
    if int(context.get("direction_min_sample", 0) or 0) < MINIMUM_ACTIVE_TRIGGER_COUNT:
        warning_reasons.append("TRIGGER_DIRECTION_SAMPLE_SPARSE")

    if source_state != EXPECTED_2292_STATE or data_quality_status == "FAIL":
        readiness_status = BLOCKED
        readiness_review_status = (
            BLOCKED_BY_2292_STATE if source_state != EXPECTED_2292_STATE else BLOCKED
        )
    elif blockers:
        readiness_status = NOT_READY
        readiness_review_status = "FORWARD_OBSERVE_READINESS_NOT_READY"
    elif warning_reasons:
        readiness_status = READY_WITH_WARNINGS
        readiness_review_status = "FORWARD_OBSERVE_READINESS_READY_WITH_WARNINGS"
    else:
        readiness_status = READY_RECOMMENDED
        readiness_review_status = "FORWARD_OBSERVE_READINESS_READY_RECOMMENDED"

    return {
        "candidate_id": RISK_CAP_CANDIDATE_ID,
        "usage_role": USAGE_ROLE,
        "source_state_from_2292": source_state,
        "data_quality_status": data_quality_status,
        "sample_sufficiency_status": sample_status,
        "active_vs_inactive_comparison_label": comparison_label,
        "false_signal_cost_status": false_cost_status,
        "risk_cap_capture_status": risk_cap_capture_status,
        "scope_lineage_status": str(context.get("scope_lineage_status", "UNKNOWN")),
        "artifact_validation_status": artifact_validation_status,
        "operational_boundary_status": operational_boundary_status,
        "readiness_gate_status": readiness_status,
        "readiness_review_status": readiness_review_status,
        "readiness_blockers": blockers,
        "readiness_warnings": warning_reasons,
        "forward_observe_readiness_recommendation": readiness_status
        in {READY_RECOMMENDED, READY_WITH_WARNINGS},
        "forward_observe_started": False,
        **SAFETY_FIELDS,
    }


def build_forward_observe_candidate_readiness_matrix(
    context: Mapping[str, Any],
    checklist: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": RISK_CAP_CANDIDATE_ID,
            "usage_role": USAGE_ROLE,
            "source_state_from_2292": checklist["source_state_from_2292"],
            "readiness_gate_status": checklist["readiness_gate_status"],
            "readiness_review_status": checklist["readiness_review_status"],
            "forward_observe_readiness_recommendation": checklist[
                "forward_observe_readiness_recommendation"
            ],
            "active_record_count": context.get("active_record_count", 0),
            "active_eligible_count": context.get("active_eligible_count", 0),
            "comparison_label": checklist["active_vs_inactive_comparison_label"],
            "active_vs_inactive_score_delta": context.get("active_vs_inactive_score_delta", 0.0),
            "risk_cap_capture_rate": context.get("risk_cap_capture_rate", 0.0),
            "stress_event_capture_rate": context.get("stress_event_capture_rate", 0.0),
            "false_risk_cap_count": context.get("false_risk_cap_count", 0),
            "false_risk_cap_cost_per_record": context.get("false_risk_cap_cost_per_record", 0.0),
            "missed_upside_cost_per_record": context.get("missed_upside_cost_per_record", 0.0),
            "target_assets": list(context.get("target_assets", [])),
            "horizons": list(context.get("horizons", [])),
            "forward_observe_started": False,
            **SAFETY_FIELDS,
        }
    ]


def build_forward_observe_evidence_collection_spec(
    inputs: ForwardObserveReadinessInputs,
) -> dict[str, Any]:
    return {
        "candidate_id": inputs.candidate,
        "usage_role": USAGE_ROLE,
        "observe_mode": OBSERVE_MODE,
        "observe_start_policy": "start_only_after_trading_2294_observe_runtime_plan_is_approved",
        "observe_end_policy": "end_after_minimum_windows_or_extend_if_trigger_count_sparse",
        "minimum_observe_days": MINIMUM_OBSERVE_DAYS,
        "minimum_active_trigger_count": MINIMUM_ACTIVE_TRIGGER_COUNT,
        "minimum_review_windows": MINIMUM_REVIEW_WINDOWS,
        "target_assets": list(inputs.target_assets),
        "horizons": list(inputs.horizons),
        "daily_evidence_fields": [
            "report_date",
            "candidate_id",
            "usage_role",
            "target_assets",
            "risk_cap_triggered",
            "triggered_assets",
            "triggered_horizons",
            "risk_cap_score",
            "risk_cap_intensity",
            "risk_cap_reason",
            "source_signal_records",
            "market_context_snapshot",
            "data_quality_status",
            "trigger_interpretation",
            "allowed_action",
        ],
        "weekly_review_fields": [
            "week_start",
            "week_end",
            "active_trigger_count",
            "post_trigger_forward_path_status",
            "observed_max_drawdown_after_trigger",
            "observed_realized_volatility_after_trigger",
            "observed_false_risk_cap_cases",
            "observed_missed_stress_cases",
            "evidence_accumulation_status",
            "continue_observe_recommendation",
        ],
        "trigger_event_fields": [
            "risk_cap_triggered",
            "triggered_assets",
            "triggered_horizons",
            "signal_direction",
            "scope_active",
            "risk_cap_intensity",
            "trigger_severity",
        ],
        "outcome_followup_fields": [
            "forward_return_5d",
            "forward_return_10d",
            "forward_return_20d",
            "post_trigger_max_drawdown",
            "post_trigger_realized_volatility",
            "stress_event_observed",
            "false_risk_cap_case",
            "missed_stress_case",
        ],
        "manual_review_fields": [
            "manual_review_required",
            "manual_review_notes",
            "owner_precheck_candidate",
            "evidence_limitation",
        ],
        "allowed_action": ALLOWED_ACTION,
        "forbidden_actions": sorted(BANNED_ALLOWED_ACTIONS),
        **SAFETY_FIELDS,
    }


def build_forward_observe_daily_report_contract(
    inputs: ForwardObserveReadinessInputs,
) -> dict[str, Any]:
    return {
        "candidate_id": inputs.candidate,
        "usage_role": USAGE_ROLE,
        "allowed_action": ALLOWED_ACTION,
        "fields": [
            "report_date",
            "candidate_id",
            "usage_role",
            "generated_at",
            "target_assets",
            "risk_cap_triggered",
            "triggered_assets",
            "triggered_horizons",
            "risk_cap_score",
            "risk_cap_intensity",
            "risk_cap_reason",
            "source_signal_records",
            "market_context_snapshot",
            "data_quality_status",
            "trigger_interpretation",
            "manual_review_required",
            "allowed_action",
            "promotion_allowed",
            "paper_shadow_allowed",
            "production_allowed",
            "broker_action",
        ],
        "field_defaults": {
            "risk_cap_triggered": False,
            "manual_review_required": False,
            "allowed_action": ALLOWED_ACTION,
            **SAFETY_FIELDS,
        },
        "forbidden_fields": [
            "target_weight",
            "rebalance_instruction",
            "buy_signal",
            "sell_signal",
            "broker_order",
        ],
        "forbidden_allowed_actions": sorted(BANNED_ALLOWED_ACTIONS),
        **SAFETY_FIELDS,
    }


def build_forward_observe_weekly_review_contract(
    inputs: ForwardObserveReadinessInputs,
) -> dict[str, Any]:
    return {
        "candidate_id": inputs.candidate,
        "usage_role": USAGE_ROLE,
        "fields": [
            "week_start",
            "week_end",
            "candidate_id",
            "active_trigger_count",
            "triggered_asset_count",
            "triggered_horizon_count",
            "post_trigger_forward_path_status",
            "observed_max_drawdown_after_trigger",
            "observed_realized_volatility_after_trigger",
            "observed_false_risk_cap_cases",
            "observed_missed_stress_cases",
            "evidence_accumulation_status",
            "continue_observe_recommendation",
            "manual_review_notes",
            "promotion_allowed",
            "paper_shadow_allowed",
            "production_allowed",
            "broker_action",
        ],
        "field_defaults": {
            "continue_observe_recommendation": "observe_only_continue_or_extend",
            **SAFETY_FIELDS,
        },
        **SAFETY_FIELDS,
    }


def build_risk_cap_forward_observe_metric_spec() -> dict[str, Any]:
    return {
        "candidate_id": RISK_CAP_CANDIDATE_ID,
        "usage_role": USAGE_ROLE,
        "metrics": [
            "trigger_count",
            "active_trigger_rate",
            "asset_trigger_distribution",
            "horizon_trigger_distribution",
            "risk_cap_capture_rate",
            "stress_event_capture_rate",
            "downside_tail_capture_rate",
            "post_trigger_max_drawdown",
            "post_trigger_realized_volatility",
            "false_risk_cap_count",
            "false_risk_cap_cost",
            "missed_stress_count",
            "missed_downside_tail_count",
            "missed_upside_cost",
            "trigger_clustering",
            "trigger_staleness",
            "data_quality_warning_count",
        ],
        "metric_interpretation": {
            "risk_cap_capture_rate": "trigger_followed_by_drawdown_stress_or_volatility_expansion",
            "false_risk_cap_cost": "trigger_followed_by_material_upside_without_risk_event",
            "missed_stress_count": "stress_event_without_prior_risk_cap_trigger",
            "trigger_staleness": "signal_remains_active_without_discriminating_risk",
        },
        **SAFETY_FIELDS,
    }


def build_risk_cap_trigger_interpretation_spec() -> dict[str, Any]:
    return {
        "candidate_id": RISK_CAP_CANDIDATE_ID,
        "usage_role": USAGE_ROLE,
        "risk_cap_triggered": {
            "true_when": [
                "scope_active == true",
                "usage_role == risk_cap_only",
                "risk_cap_intensity above observe_threshold",
                "signal_direction in [risk_off, trend_weakening, volatility_expansion]",
            ],
            "false_when_signal_direction_in": ["risk_on", "neutral", "trend_confirming"],
        },
        "trigger_severity": {
            "low": "risk_cap_intensity in lower observe band",
            "medium": "risk_cap_intensity in middle observe band",
            "high": "risk_cap_intensity in upper observe band",
        },
        "trigger_interpretation": {
            "risk_cap_low": "observe_only_low_risk_cap_trigger",
            "risk_cap_medium": "observe_only_medium_risk_cap_trigger",
            "risk_cap_high": "observe_only_high_risk_cap_trigger",
        },
        "interpretation_boundary": [
            "risk_cap_trigger_is_not_sell_signal",
            "risk_cap_trigger_is_not_buy_signal",
            "risk_cap_trigger_is_not_target_weight",
            "risk_cap_trigger_is_not_broker_action",
            "risk_cap_trigger_is_observe_only_evidence",
        ],
        "allowed_action": ALLOWED_ACTION,
        **SAFETY_FIELDS,
    }


def build_forward_observe_stop_continue_rules() -> dict[str, Any]:
    return {
        "continue_observe_if": [
            "sufficient_triggers_accumulating",
            "false_risk_cap_cost_controlled",
            "data_quality_pass_or_pass_with_warnings",
            "operational_report_generation_stable",
        ],
        "extend_observe_if": [
            "trigger_count_below_minimum",
            "sample_thin_but_no_evidence_of_harm",
            "sparse_asset_horizon_distribution",
        ],
        "stop_observe_if": [
            "repeated_false_risk_cap_triggers",
            "false_risk_cap_cost_materially_high",
            "data_quality_fails_repeatedly",
            "trigger_staleness_high",
            "signal_no_longer_discriminates_risk",
        ],
        "escalate_to_owner_precheck_if": [
            "sufficient_triggers",
            "risk_cap_capture_evidence_positive",
            "false_risk_cap_cost_controlled",
            "evidence_stable_across_at_least_two_review_windows",
        ],
        "forbidden_rules": [
            "auto_promotion",
            "auto_paper_shadow",
            "auto_production",
            "auto_broker_action",
        ],
        **SAFETY_FIELDS,
    }


def build_forward_observe_operational_boundary(
    inputs: ForwardObserveReadinessInputs,
) -> dict[str, Any]:
    return {
        "candidate_id": inputs.candidate,
        "observe_mode": "observe_only",
        "portfolio_effect": "none",
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_only": True,
        "allowed_outputs": [
            "daily_observe_record",
            "weekly_observe_summary",
            "trigger_interpretation",
            "evidence_accumulation_status",
        ],
        "forbidden_outputs": [
            "target_weight",
            "rebalance_instruction",
            "buy_signal",
            "sell_signal",
            "broker_action",
            "production_decision",
        ],
        **SAFETY_FIELDS,
    }


def build_rejected_candidate_carry_forward_matrix(
    inputs: ForwardObserveReadinessInputs,
    context: Mapping[str, Any],
) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": candidate_id,
            "source_state_from_2292": _state_for_candidate(inputs, candidate_id)
            or REJECTED_2292_STATE,
            "carry_forward_status": "rejected_current_form",
            "included_in_forward_observe_readiness": False,
            "future_reopen_policy": "revisit_only_with_new_candidate_family",
            "readiness_candidate": candidate_id == context.get("candidate_id"),
            **SAFETY_FIELDS,
        }
        for candidate_id in inputs.rejected_candidates
    ]


def build_archived_candidate_carry_forward_matrix(
    inputs: ForwardObserveReadinessInputs,
) -> list[dict[str, Any]]:
    archive_payload = inputs.validation_payloads["risk_appetite_archive"]
    return [
        {
            "candidate_id": candidate_id,
            "source_state_from_2291": _archive_source_state(archive_payload, candidate_id),
            "carry_forward_status": "archived_current_form",
            "included_in_forward_observe_readiness": False,
            "future_reopen_policy": "revisit_only_with_new_inputs_or_candidate_family",
            **SAFETY_FIELDS,
        }
        for candidate_id in inputs.archived_candidates
    ]


def build_forward_observe_next_task_recommendation(
    checklist: Mapping[str, Any],
) -> dict[str, Any]:
    readiness_status = str(checklist.get("readiness_gate_status", ""))
    if readiness_status == READY_RECOMMENDED:
        next_task = NEXT_TASK_RUNTIME
    elif readiness_status == READY_WITH_WARNINGS:
        next_task = NEXT_TASK_EXTENSION
    else:
        next_task = NEXT_TASK_ARCHIVE
    return {
        "next_task_recommendation": next_task,
        "readiness_gate_status": readiness_status,
        "forward_observe_readiness_recommendation": bool(
            checklist.get("forward_observe_readiness_recommendation", False)
        ),
        "forward_observe_started": False,
        **SAFETY_FIELDS,
    }


def build_forward_observe_readiness_summary(
    *,
    inputs: ForwardObserveReadinessInputs,
    checklist: Mapping[str, Any],
    next_task: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": "scope_narrowed_forward_observe_readiness_review.v1",
        "report_type": REPORT_TYPE,
        "title": "Scope-Narrowed Forward Observe Readiness Review",
        "task_id": TASK_ID,
        "status": STATUS,
        "summary_status": STATUS,
        "artifact_role": ARTIFACT_ROLE,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "source_task": "TRADING-2292",
        "candidate_reviewed": inputs.candidate,
        "rejected_candidates": list(inputs.rejected_candidates),
        "archived_candidates": list(inputs.archived_candidates),
        "target_assets": list(inputs.target_assets),
        "horizons": list(inputs.horizons),
        "readiness_gate_status": checklist["readiness_gate_status"],
        "readiness_review_status": checklist["readiness_review_status"],
        "readiness_blockers": list(checklist.get("readiness_blockers", [])),
        "readiness_warnings": list(checklist.get("readiness_warnings", [])),
        "forward_observe_readiness_recommendation": bool(
            checklist.get("forward_observe_readiness_recommendation", False)
        ),
        "forward_observe_started": False,
        "next_task_recommendation": next_task["next_task_recommendation"],
        "owner_precheck_note_generated": True,
        "owner_review_package_generated": False,
        "paper_shadow_ready": False,
        "production_ready": False,
        "broker_ready": False,
        "trading_2292_state_recommendation_changed": False,
        "trading_2291_artifacts_changed": False,
        **SAFETY_FIELDS,
    }


def write_forward_observe_readiness_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Mapping[str, Any]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json(output_dir / "forward_observe_readiness_review_summary.json", artifacts["summary"])
    write_json(
        output_dir / "forward_observe_candidate_readiness_matrix.json",
        artifacts["readiness_matrix"],
    )
    write_csv_rows(
        output_dir / "forward_observe_candidate_readiness_matrix.csv",
        artifacts["readiness_matrix"]["rows"],
    )
    write_json(output_dir / "forward_observe_gate_checklist.json", artifacts["gate_checklist"])
    write_json(
        output_dir / "forward_observe_evidence_collection_spec.json",
        artifacts["evidence_collection_spec"],
    )
    write_json(
        output_dir / "forward_observe_daily_report_contract.json",
        artifacts["daily_report_contract"],
    )
    write_json(
        output_dir / "forward_observe_weekly_review_contract.json",
        artifacts["weekly_review_contract"],
    )
    write_json(
        output_dir / "forward_observe_stop_continue_rules.json",
        artifacts["stop_continue_rules"],
    )
    write_json(
        output_dir / "forward_observe_operational_boundary.json",
        artifacts["operational_boundary"],
    )
    write_json(
        output_dir / "risk_cap_forward_observe_metric_spec.json",
        artifacts["metric_spec"],
    )
    write_json(
        output_dir / "risk_cap_trigger_interpretation_spec.json",
        artifacts["trigger_interpretation_spec"],
    )
    write_json(
        output_dir / "rejected_candidate_carry_forward_matrix.json",
        artifacts["rejected_carry_forward"],
    )
    write_csv_rows(
        output_dir / "rejected_candidate_carry_forward_matrix.csv",
        artifacts["rejected_carry_forward"]["rows"],
    )
    write_json(
        output_dir / "archived_candidate_carry_forward_matrix.json",
        artifacts["archived_carry_forward"],
    )
    write_csv_rows(
        output_dir / "archived_candidate_carry_forward_matrix.csv",
        artifacts["archived_carry_forward"]["rows"],
    )
    write_json(
        output_dir / "forward_observe_next_task_recommendation.json",
        artifacts["next_task_recommendation"],
    )

    docs = artifacts["docs"]
    write_markdown(
        docs_root / "scope_narrowed_forward_observe_readiness_review.md",
        str(docs["readiness_review"]),
    )
    write_markdown(
        docs_root / "risk_cap_forward_observe_evidence_collection_spec.md",
        str(docs["evidence_collection"]),
    )
    write_markdown(
        docs_root / "forward_observe_stop_continue_rules.md",
        str(docs["stop_continue_rules"]),
    )
    write_markdown(
        docs_root / "rejected_and_archived_candidate_carry_forward.md",
        str(docs["carry_forward"]),
    )


def build_forward_observe_readiness_docs(
    *,
    summary: Mapping[str, Any],
    checklist: Mapping[str, Any],
    evidence_spec: Mapping[str, Any],
    stop_continue_rules: Mapping[str, Any],
    rejected: Sequence[Mapping[str, Any]],
    archived: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    readiness_review = "\n".join(
        [
            "# Scope-Narrowed Forward Observe Readiness Review",
            "",
            (
                "TRADING-2293 读取 TRADING-2292 scope-narrowed actual-path "
                "validation outputs，只对 "
                "`volatility_regime_scope_narrowed_risk_cap_v1` 做 forward observe "
                "readiness review。"
            ),
            "",
            f"- readiness_gate_status: `{summary['readiness_gate_status']}`",
            (
                "- forward_observe_readiness_recommendation: "
                f"`{summary['forward_observe_readiness_recommendation']}`"
            ),
            f"- next_task_recommendation: `{summary['next_task_recommendation']}`",
            f"- forward_observe_started: `{summary['forward_observe_started']}`",
            "- baseline confirmation carry-forward: rejected current form",
            "- risk appetite carry-forward: archived current form",
            "",
            (
                "Readiness review 不等于启动 forward observe；forward observe 不等于 "
                "paper-shadow。risk-cap trigger 不是 buy/sell/rebalance/broker "
                "signal，只能作为 observe-only evidence collection 输入。"
            ),
            "",
            "## Gate Checklist",
            "",
            f"- source_state_from_2292: `{checklist['source_state_from_2292']}`",
            f"- data_quality_status: `{checklist['data_quality_status']}`",
            f"- sample_sufficiency_status: `{checklist['sample_sufficiency_status']}`",
            (
                "- active_vs_inactive_comparison_label: "
                f"`{checklist['active_vs_inactive_comparison_label']}`"
            ),
            f"- false_signal_cost_status: `{checklist['false_signal_cost_status']}`",
            (
                "- readiness_warnings: "
                f"`{', '.join(checklist.get('readiness_warnings', [])) or 'none'}`"
            ),
            "",
            "Promotion、paper-shadow、production、broker action 全部继续阻断。",
            "",
        ]
    )
    evidence_collection = "\n".join(
        [
            "# Risk-Cap Forward Observe Evidence Collection Spec",
            "",
            f"- observe_mode: `{evidence_spec['observe_mode']}`",
            f"- minimum_observe_days: `{evidence_spec['minimum_observe_days']}`",
            f"- minimum_active_trigger_count: `{evidence_spec['minimum_active_trigger_count']}`",
            f"- minimum_review_windows: `{evidence_spec['minimum_review_windows']}`",
            "",
            (
                "Daily evidence 记录 risk-cap trigger、triggered assets/horizons、"
                "risk-cap intensity、source signal records、market context、data "
                "quality 和 trigger interpretation。Allowed action 固定为 "
                "`observe_only`。"
            ),
            "",
            (
                "Weekly review 汇总 trigger count、post-trigger forward path、drawdown、"
                "realized volatility、false risk-cap cases、missed stress cases 和 "
                "evidence accumulation status。"
            ),
            "",
            (
                "若 60 天内 active trigger 过少，不直接判定无效，应进入 "
                "evidence accumulation extension。"
            ),
            "",
        ]
    )
    stop_continue = "\n".join(
        [
            "# Forward Observe Stop / Continue Rules",
            "",
            "Continue observe if:",
            *[f"- `{item}`" for item in stop_continue_rules["continue_observe_if"]],
            "",
            "Extend observe if:",
            *[f"- `{item}`" for item in stop_continue_rules["extend_observe_if"]],
            "",
            "Stop observe if:",
            *[f"- `{item}`" for item in stop_continue_rules["stop_observe_if"]],
            "",
            "Escalate to owner precheck if:",
            *[f"- `{item}`" for item in stop_continue_rules["escalate_to_owner_precheck_if"]],
            "",
            "禁止 auto_promotion、auto_paper_shadow、auto_production 和 auto_broker_action。",
            "",
        ]
    )
    carry_forward = "\n".join(
        [
            "# Rejected And Archived Candidate Carry Forward",
            "",
            "TRADING-2293 不重新打开 baseline confirmation 或 risk appetite。",
            "",
            "## Rejected",
            "",
            *[
                (
                    f"- `{row['candidate_id']}`: `{row['carry_forward_status']}`, "
                    "included_in_forward_observe_readiness="
                    f"`{row['included_in_forward_observe_readiness']}`"
                )
                for row in rejected
            ],
            "",
            "## Archived",
            "",
            *[
                (
                    f"- `{row['candidate_id']}`: `{row['carry_forward_status']}`, "
                    "included_in_forward_observe_readiness="
                    f"`{row['included_in_forward_observe_readiness']}`"
                )
                for row in archived
            ],
            "",
            "所有 carry-forward rows 固定 promotion/paper-shadow/production/broker false/none。",
            "",
        ]
    )
    return {
        "readiness_review": readiness_review,
        "evidence_collection": evidence_collection,
        "stop_continue_rules": stop_continue,
        "carry_forward": carry_forward,
    }


def _candidate_context(inputs: ForwardObserveReadinessInputs) -> dict[str, Any]:
    state_row = _row_for_candidate(
        inputs.validation_payloads["state_recommendation"],
        inputs.candidate,
        row_keys=("candidate_rows", "rows"),
    )
    scorecard = _row_for_candidate(
        inputs.validation_payloads["risk_cap_scorecard"],
        inputs.candidate,
        row_keys=("candidate_scorecards", "rows"),
    )
    comparison = _row_for_candidate(
        inputs.validation_payloads["comparison"],
        inputs.candidate,
        row_keys=("rows",),
    )
    sample = _row_for_candidate(
        inputs.validation_payloads["sample_sufficiency"],
        inputs.candidate,
        row_keys=("rows",),
    )
    data_quality = _row_for_candidate(
        inputs.validation_payloads["data_quality"],
        inputs.candidate,
        row_keys=("candidate_rows", "rows"),
    )
    false_cost_rows = _rows_for_candidate(
        inputs.validation_payloads["false_signal_cost"],
        inputs.candidate,
        row_keys=("rows",),
    )
    scope_lineage = inputs.generator_candidate_payloads["lineage_report"]
    scope_review_row = _scope_review_row(inputs)
    active_eligible = int(scorecard.get("active_eligible_count") or 0)
    false_risk_cap_cost = to_float(scorecard.get("false_risk_cap_cost"))
    missed_upside_cost = to_float(scorecard.get("missed_upside_cost"))
    return {
        "candidate_id": inputs.candidate,
        "usage_role": USAGE_ROLE,
        "source_state_from_2292": state_row.get("recommended_research_status", ""),
        "data_quality_status": data_quality.get(
            "data_quality_status",
            inputs.validation_payloads["summary"]
            .get("summary", {})
            .get("source_data_quality_status"),
        ),
        "sample_sufficiency_status": sample.get("sample_sufficiency_status", ""),
        "active_vs_inactive_comparison_label": comparison.get("comparison_label", ""),
        "active_vs_inactive_score_delta": comparison.get("active_vs_inactive_score_delta", 0.0),
        "active_record_count": scorecard.get(
            "active_record_count",
            sample.get("active_record_count", 0),
        ),
        "active_eligible_count": active_eligible,
        "risk_cap_capture_rate": scorecard.get("risk_cap_capture_rate", 0.0),
        "stress_event_capture_rate": scorecard.get("stress_event_capture_rate", 0.0),
        "downside_tail_capture_rate": scorecard.get("downside_tail_capture_rate", 0.0),
        "false_risk_cap_count": scorecard.get("false_risk_cap_count", 0),
        "false_risk_cap_cost": false_risk_cap_cost,
        "false_risk_cap_cost_per_record": round_float(false_risk_cap_cost / active_eligible)
        if active_eligible
        else 0.0,
        "missed_upside_cost": missed_upside_cost,
        "missed_upside_cost_per_record": round_float(missed_upside_cost / active_eligible)
        if active_eligible
        else 0.0,
        "asset_horizon_sparse_bucket_count": sample.get("asset_horizon_sparse_bucket_count", 0),
        "direction_min_sample": sample.get("direction_min_sample", 0),
        "scope_lineage_status": "PASS"
        if scope_lineage.get("source_hashes") and scope_lineage.get("source_paths")
        else "MISSING_LINEAGE",
        "artifact_validation_status": data_quality.get(
            "input_artifact_validation_status",
            "UNKNOWN",
        ),
        "scope_review_rationale": {
            "scope_review_status": scope_review_row.get("scope_review_status"),
            "usage_recommendation": scope_review_row.get("usage_recommendation"),
            "kept_assets": scope_review_row.get("kept_assets", []),
            "kept_horizons": scope_review_row.get("kept_horizons", []),
            "kept_directions": scope_review_row.get("kept_directions", []),
        },
        "false_cost_rows": false_cost_rows,
        "target_assets": inputs.target_assets,
        "horizons": inputs.horizons,
    }


def _false_signal_cost_status(context: Mapping[str, Any]) -> str:
    false_cost_per_record = to_float(context.get("false_risk_cap_cost_per_record"))
    missed_upside_per_record = to_float(context.get("missed_upside_cost_per_record"))
    false_high = false_cost_per_record > FALSE_RISK_CAP_COST_PER_RECORD_REVIEW_LIMIT
    missed_high = missed_upside_per_record > MISSED_UPSIDE_COST_PER_RECORD_REVIEW_LIMIT
    if false_high and missed_high:
        return "BOTH_FALSE_COSTS_TOO_HIGH"
    if false_high:
        return "FALSE_RISK_CAP_COST_TOO_HIGH"
    if missed_high:
        return "MISSED_UPSIDE_COST_REVIEW_REQUIRED"
    return "FALSE_SIGNAL_COST_CONTROLLED"


def _risk_cap_capture_status(context: Mapping[str, Any]) -> str:
    if to_float(context.get("risk_cap_capture_rate")) > 0.0:
        return "RISK_CAP_CAPTURE_PRESENT"
    if to_float(context.get("stress_event_capture_rate")) > 0.0:
        return "STRESS_EVENT_CAPTURE_PRESENT"
    return "RISK_CAP_CAPTURE_NOT_MEASURABLE"


def _load_required_payloads(
    root: Path,
    required_files: Mapping[str, str],
    *,
    required_context: str,
    json_only_keys: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for key, filename in required_files.items():
        path = root / filename
        if not path.exists():
            raise ScopeNarrowedForwardObserveReadinessReviewError(
                f"missing required {required_context} artifact: {path}"
            )
        if path.suffix.lower() == ".json" or (json_only_keys and key in json_only_keys):
            payloads[key] = _read_json(path)
        else:
            payloads[key] = {"artifact_path": str(path), "status": "PRESENT"}
    return payloads


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ScopeNarrowedForwardObserveReadinessReviewError(f"JSON must be an object: {path}")
    return dict(payload)


def _validate_input_safety(name: str, payload: Mapping[str, Any]) -> None:
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise ScopeNarrowedForwardObserveReadinessReviewError(
                f"{name} opens promotion_allowed"
            )
        if item.get("paper_shadow_allowed") is True:
            raise ScopeNarrowedForwardObserveReadinessReviewError(
                f"{name} opens paper_shadow_allowed"
            )
        if item.get("production_allowed") is True:
            raise ScopeNarrowedForwardObserveReadinessReviewError(
                f"{name} opens production_allowed"
            )
        if str(item.get("broker_action", "none")).lower() != "none":
            raise ScopeNarrowedForwardObserveReadinessReviewError(
                f"{name} opens broker_action"
            )
        if item.get("owner_review_required") is True:
            raise ScopeNarrowedForwardObserveReadinessReviewError(
                f"{name} opens owner_review_required"
            )
        if item.get("actual_path_validation_ready") is True:
            raise ScopeNarrowedForwardObserveReadinessReviewError(
                f"{name} opens actual_path_validation_ready"
            )
        for value in item.values():
            if isinstance(value, str) and value in BANNED_RECOMMENDATIONS:
                raise ScopeNarrowedForwardObserveReadinessReviewError(
                    f"{name} emits banned recommendation {value}"
                )


def _walk_mappings(value: Any) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    if isinstance(value, Mapping):
        found.append(value)
        for child in value.values():
            found.extend(_walk_mappings(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_walk_mappings(child))
    return found


def _row_for_candidate(
    payload: Mapping[str, Any],
    candidate_id: str,
    *,
    row_keys: Sequence[str],
) -> dict[str, Any]:
    rows = _rows_for_candidate(payload, candidate_id, row_keys=row_keys)
    if not rows:
        raise ScopeNarrowedForwardObserveReadinessReviewError(
            f"missing candidate row for {candidate_id}"
        )
    return rows[0]


def _rows_for_candidate(
    payload: Mapping[str, Any],
    candidate_id: str,
    *,
    row_keys: Sequence[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key in row_keys:
        raw_rows = payload.get(key)
        if isinstance(raw_rows, list):
            for row in raw_rows:
                if not isinstance(row, Mapping):
                    continue
                row_candidate = row.get("scope_narrowed_candidate_id") or row.get("candidate_id")
                if row_candidate == candidate_id:
                    rows.append(dict(row))
    return rows


def _scope_review_row(inputs: ForwardObserveReadinessInputs) -> dict[str, Any]:
    payload = inputs.scope_review_payloads["scope_recommendation"]
    rows = payload.get("rows", [])
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, Mapping):
            continue
        if row.get("refined_candidate_id") == "volatility_regime_refined_confidence_v1":
            return dict(row)
        if row.get("original_candidate_id") == "volatility_regime":
            return dict(row)
    return {}


def _state_for_candidate(inputs: ForwardObserveReadinessInputs, candidate_id: str) -> str:
    try:
        row = _row_for_candidate(
            inputs.validation_payloads["state_recommendation"],
            candidate_id,
            row_keys=("candidate_rows", "rows"),
        )
    except ScopeNarrowedForwardObserveReadinessReviewError:
        return ""
    return str(row.get("recommended_research_status", ""))


def _archive_source_state(payload: Mapping[str, Any], candidate_id: str) -> str:
    for item in _walk_mappings(payload):
        if (
            item.get("candidate_id") == candidate_id
            or item.get("archived_candidate_id") == candidate_id
        ):
            return str(
                item.get("carry_forward_status")
                or item.get("archive_status")
                or item.get("source_state_from_2291")
                or "current_form_archived"
            )
    return "current_form_archived"


def _common_payload(generated_at: datetime) -> dict[str, Any]:
    return {
        "schema_version": "scope_narrowed_forward_observe_readiness_review.v1",
        "report_type": REPORT_TYPE,
        "title": "Scope-Narrowed Forward Observe Readiness Review",
        "task_id": TASK_ID,
        "status": STATUS,
        "summary_status": STATUS,
        "artifact_role": ARTIFACT_ROLE,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "research_only": True,
        **SAFETY_FIELDS,
    }


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return parse_csv_list(value)
    return tuple(str(item).strip() for item in value if str(item).strip())


__all__ = [
    "BLOCKED",
    "CONFIRMATION_CANDIDATE_ID",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_SCOPE_GENERATOR_ROOT",
    "DEFAULT_SCOPE_REVIEW_ROOT",
    "DEFAULT_SCOPE_VALIDATION_ROOT",
    "MODE",
    "NOT_READY",
    "READY_RECOMMENDED",
    "READY_WITH_WARNINGS",
    "RISK_CAP_CANDIDATE_ID",
    "RISK_APPETITE_ARCHIVE_CANDIDATE",
    "ScopeNarrowedForwardObserveReadinessReviewError",
    "build_forward_observe_evidence_collection_spec",
    "build_forward_observe_gate_checklist",
    "build_forward_observe_stop_continue_rules",
    "build_risk_cap_trigger_interpretation_spec",
    "load_scope_narrowed_forward_observe_readiness_inputs",
    "run_scope_narrowed_forward_observe_readiness_review",
]
