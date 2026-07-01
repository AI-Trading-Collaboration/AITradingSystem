from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    write_csv_rows,
    write_json,
    write_markdown,
)
from ai_trading_system.regenerated_candidate_generator_common import parse_csv_list
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    RISK_CAP_CANDIDATE_ID,
)
from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_READINESS_ROOT,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "forward_observe_evidence_accumulation_plan"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

TASK_ID = "TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN"
REPORT_TYPE = "forward_observe_evidence_accumulation_plan"
MODE = "evidence_accumulation_extension_plan"
STATUS = "FORWARD_OBSERVE_EVIDENCE_ACCUMULATION_PLAN_READY_PROMOTION_BLOCKED"
ARTIFACT_ROLE = "forward_observe_evidence_accumulation_plan"

READY_GATES = {"FORWARD_OBSERVE_READY_RECOMMENDED", "FORWARD_OBSERVE_READY_WITH_WARNINGS"}
NEXT_TASKS = {
    "TRADING-2294_Forward_Observe_Runtime_Evidence_Collection_Plan",
    "TRADING-2294_Evidence_Accumulation_Extension_Plan",
}
FOLLOWUP_HORIZONS = ("5d", "10d", "20d")
OBSERVE_MODE = "observe_only"
ALLOWED_ACTION = "observe_only"

# Pilot baseline inherited from TRADING-2293. These are evidence maturity review
# gates, not trading thresholds, and must be recalibrated or owner-reviewed before
# any promotion, paper-shadow, production, or broker use is discussed.
MINIMUM_OBSERVE_DAYS = 60
MINIMUM_ACTIVE_TRIGGER_COUNT = 10
MINIMUM_REVIEW_WINDOWS = 4

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "observe_mode": OBSERVE_MODE,
    "forward_observe_started": False,
    "runtime_started": False,
    "runtime_start_allowed": False,
    "daily_report_integration": "design_only",
    "weekly_report_integration": "design_only",
    "portfolio_effect": "none",
    "production_effect": "none",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "manual_review_only": True,
    "owner_review_required": False,
    "candidate_generation_allowed": False,
    "actual_path_validation_executed": False,
    "paper_shadow_recommendation_allowed": False,
    "production_recommendation_allowed": False,
    "broker_action_recommendation_allowed": False,
}

REQUIRED_READINESS_FILES = {
    "summary": "forward_observe_readiness_review_summary.json",
    "candidate_readiness_matrix": "forward_observe_candidate_readiness_matrix.json",
    "gate_checklist": "forward_observe_gate_checklist.json",
    "evidence_collection_spec": "forward_observe_evidence_collection_spec.json",
    "daily_report_contract": "forward_observe_daily_report_contract.json",
    "weekly_review_contract": "forward_observe_weekly_review_contract.json",
    "stop_continue_rules": "forward_observe_stop_continue_rules.json",
    "operational_boundary": "forward_observe_operational_boundary.json",
    "metric_spec": "risk_cap_forward_observe_metric_spec.json",
    "trigger_interpretation_spec": "risk_cap_trigger_interpretation_spec.json",
    "next_task": "forward_observe_next_task_recommendation.json",
}

BANNED_RECOMMENDATIONS = {
    "PROMOTION_READY",
    "PAPER_SHADOW_READY",
    "PRODUCTION_READY",
    "BROKER_READY",
}
BANNED_ALLOWED_ACTIONS = {
    "buy",
    "sell",
    "rebalance",
    "reduce_position",
    "broker_action",
}


class ForwardObserveEvidenceAccumulationPlanError(ValueError):
    pass


@dataclass(frozen=True)
class ForwardObserveEvidenceAccumulationInputs:
    readiness_dir: Path
    candidate: str
    target_assets: tuple[str, ...]
    horizons: tuple[str, ...]
    readiness_payloads: dict[str, dict[str, Any]]
    artifact_paths: dict[str, str]


def run_forward_observe_evidence_accumulation_plan(
    *,
    readiness_dir: Path = DEFAULT_READINESS_ROOT,
    candidate: str = RISK_CAP_CANDIDATE_ID,
    target_assets: Sequence[str] | str = ("QQQ", "SPY", "SMH"),
    horizons: Sequence[str] | str = FOLLOWUP_HORIZONS,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    mode: str = MODE,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    if mode != MODE:
        raise ForwardObserveEvidenceAccumulationPlanError(
            f"forward observe evidence accumulation plan only supports {MODE}"
        )
    inputs = load_forward_observe_evidence_accumulation_inputs(
        readiness_dir=readiness_dir,
        candidate=candidate,
        target_assets=target_assets,
        horizons=horizons,
    )
    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    artifacts = build_forward_observe_evidence_accumulation_artifacts(
        inputs,
        generated_at=generated_at,
    )
    artifact_paths = write_forward_observe_evidence_accumulation_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        artifacts=artifacts,
    )
    summary = dict(artifacts["summary"])
    summary["artifact_paths"] = artifact_paths
    return summary


def load_forward_observe_evidence_accumulation_inputs(
    *,
    readiness_dir: Path,
    candidate: str,
    target_assets: Sequence[str] | str,
    horizons: Sequence[str] | str,
) -> ForwardObserveEvidenceAccumulationInputs:
    if candidate != RISK_CAP_CANDIDATE_ID:
        raise ForwardObserveEvidenceAccumulationPlanError(
            f"evidence accumulation candidate must be {RISK_CAP_CANDIDATE_ID}: {candidate}"
        )
    payloads = _load_required_payloads(readiness_dir)
    for key, payload in payloads.items():
        _validate_input_safety(f"readiness.{key}", payload)
    _validate_readiness_gate(payloads, candidate)

    summary = payloads["summary"]
    asset_ids = _normalize_or_source(target_assets, summary.get("target_assets", []))
    horizon_ids = _normalize_or_source(horizons, summary.get("horizons", []))
    artifact_paths = {
        f"readiness.{key}": str(readiness_dir / filename)
        for key, filename in REQUIRED_READINESS_FILES.items()
    }
    return ForwardObserveEvidenceAccumulationInputs(
        readiness_dir=readiness_dir,
        candidate=candidate,
        target_assets=asset_ids,
        horizons=horizon_ids,
        readiness_payloads=payloads,
        artifact_paths=artifact_paths,
    )


def build_forward_observe_evidence_accumulation_artifacts(
    inputs: ForwardObserveEvidenceAccumulationInputs,
    *,
    generated_at: datetime,
) -> dict[str, Any]:
    common = _common_payload(inputs=inputs, generated_at=generated_at)
    runtime_contract = build_forward_observe_runtime_contract(inputs)
    daily_schema = build_risk_cap_daily_observe_record_schema(inputs)
    followup_schema = build_risk_cap_trigger_followup_schema(inputs)
    storage_layout = build_forward_observe_storage_layout(inputs)
    safety_boundary = build_forward_observe_runtime_safety_boundary(inputs)
    observation_policy = build_minimum_observation_window_policy(inputs)
    weekly_cadence = build_weekly_review_cadence(inputs)
    decision_rows = build_evidence_accumulation_decision_matrix(inputs)
    plan = build_evidence_accumulation_plan(
        inputs=inputs,
        observation_policy=observation_policy,
        decision_rows=decision_rows,
    )
    summary = build_evidence_accumulation_summary(
        inputs=inputs,
        generated_at=generated_at,
    )
    docs = build_forward_observe_runtime_docs(
        summary=summary,
        runtime_contract=runtime_contract,
        daily_schema=daily_schema,
        followup_schema=followup_schema,
        safety_boundary=safety_boundary,
        decision_rows=decision_rows,
    )
    return {
        "summary": summary,
        "plan": {**common, **plan},
        "runtime_contract": {**common, **runtime_contract},
        "daily_observe_record_schema": {**common, **daily_schema},
        "trigger_followup_schema": {**common, **followup_schema},
        "storage_layout": {**common, **storage_layout},
        "runtime_safety_boundary": {**common, **safety_boundary},
        "minimum_observation_window_policy": {**common, **observation_policy},
        "weekly_review_cadence": {**common, **weekly_cadence},
        "decision_matrix": {**common, "rows": decision_rows},
        "docs": docs,
    }


def build_forward_observe_runtime_contract(
    inputs: ForwardObserveEvidenceAccumulationInputs,
) -> dict[str, Any]:
    readiness = inputs.readiness_payloads["summary"]
    evidence_spec = inputs.readiness_payloads["evidence_collection_spec"]
    return {
        "contract_version": "risk_cap_forward_observe_runtime_contract.v1",
        "candidate_id": inputs.candidate,
        "source_readiness_gate_status": readiness["readiness_gate_status"],
        "source_readiness_warnings": list(readiness.get("readiness_warnings", [])),
        "runtime_implementation_status": "design_contract_only",
        "runtime_start_requires": [
            "separate_implementation_task",
            "cached_data_quality_gate_pass_or_pass_with_warnings",
            "append_only_storage_created_with_manifest",
            "manual_review_route_confirmed",
        ],
        "runtime_start_forbidden_by_this_artifact": True,
        "daily_pipeline_contract": [
            "run_aits_validate_data_or_same_code_path_before_record_generation",
            "load_latest_scope_narrowed_risk_cap_candidate_signal",
            "derive_trigger_state_without_portfolio_action",
            "write_daily_observe_record_with_source_hashes",
            "schedule_followup_records_for_5d_10d_20d",
        ],
        "weekly_pipeline_contract": [
            "read_append_only_daily_records",
            "mature_due_followup_windows",
            "summarize_false_risk_cap_and_missed_stress_cases",
            "emit_manual_review_only_weekly_summary",
        ],
        "allowed_outputs": [
            "daily_observe_record",
            "trigger_followup_record",
            "weekly_observe_summary",
            "evidence_accumulation_status",
        ],
        "forbidden_outputs": [
            "target_weight",
            "rebalance_instruction",
            "buy_signal",
            "sell_signal",
            "broker_order",
            "paper_shadow_order",
            "production_decision",
        ],
        "source_daily_evidence_fields": list(evidence_spec.get("daily_evidence_fields", [])),
        "source_followup_fields": list(evidence_spec.get("outcome_followup_fields", [])),
        "stop_on_data_quality_failure": True,
        **SAFETY_FIELDS,
    }


def build_risk_cap_daily_observe_record_schema(
    inputs: ForwardObserveEvidenceAccumulationInputs,
) -> dict[str, Any]:
    daily_contract = inputs.readiness_payloads["daily_report_contract"]
    fields = [
        _field("record_id", "string", "stable append-only record key"),
        _field("report_date", "date", "U.S. market date for the observe record"),
        _field("generated_at", "datetime", "record generation timestamp"),
        _field("candidate_id", "string", "risk-cap candidate id"),
        _field("market_regime", "string", "selected market regime"),
        _field("data_quality_status", "string", "visible cached-data quality status"),
        _field("risk_cap_triggered", "boolean", "whether the risk-cap condition triggered"),
        _field("triggered_assets", "array[string]", "assets with an active trigger"),
        _field("triggered_horizons", "array[string]", "trigger horizons"),
        _field("risk_cap_score", "number", "normalized risk-cap score"),
        _field("risk_cap_intensity", "number", "observe-only trigger intensity"),
        _field("risk_cap_reason", "string", "human-readable risk-cap reason"),
        _field("trigger_interpretation", "string", "risk-cap-only interpretation"),
        _field("source_signal_records", "array[object]", "source candidate records"),
        _field("source_artifact_paths", "array[string]", "input artifact references"),
        _field("source_artifact_checksums", "object", "input artifact checksums"),
        _field("allowed_action", "string", "must equal observe_only"),
        _field("manual_review_notes", "string", "optional manual review note", False),
        _field("row_checksum", "string", "checksum of normalized row payload"),
    ]
    return {
        "schema_name": "risk_cap_daily_observe_record_schema",
        "schema_version": "risk_cap_daily_observe_record_schema.v1",
        "candidate_id": inputs.candidate,
        "primary_key": ["record_id"],
        "unique_key": ["report_date", "candidate_id"],
        "required_fields": [field["name"] for field in fields if field["required"]],
        "field_definitions": fields,
        "field_defaults": {
            "candidate_id": inputs.candidate,
            "market_regime": MARKET_REGIME,
            "risk_cap_triggered": False,
            "triggered_assets": [],
            "triggered_horizons": [],
            "allowed_action": ALLOWED_ACTION,
            **SAFETY_FIELDS,
        },
        "allowed_action_values": [ALLOWED_ACTION],
        "data_quality_status_values": ["PASS", "PASS_WITH_WARNINGS"],
        "forbidden_fields": list(daily_contract.get("forbidden_fields", [])),
        "source_contract_fields": list(daily_contract.get("fields", [])),
        "validation_rules": [
            "data_quality_status must be PASS or PASS_WITH_WARNINGS",
            "allowed_action must equal observe_only",
            "triggered_horizons must be subset of 5d,10d,20d",
            "source artifact paths and checksums are required for auditability",
            "no target_weight, rebalance_instruction, buy_signal, sell_signal, or broker_order",
        ],
        **SAFETY_FIELDS,
    }


def build_risk_cap_trigger_followup_schema(
    inputs: ForwardObserveEvidenceAccumulationInputs,
) -> dict[str, Any]:
    fields = [
        _field("followup_record_id", "string", "stable follow-up record key"),
        _field("trigger_record_id", "string", "source daily observe record key"),
        _field("candidate_id", "string", "risk-cap candidate id"),
        _field("target_asset", "string", "triggered asset"),
        _field("followup_horizon", "string", "one of 5d, 10d, 20d"),
        _field("trigger_date", "date", "source trigger date"),
        _field("followup_due_date", "date", "date when the horizon matures"),
        _field("followup_status", "string", "pending, mature, blocked, or incomplete"),
        _field("actual_forward_return", "number", "realized forward return", False),
        _field("post_trigger_max_drawdown", "number", "worst path drawdown", False),
        _field("post_trigger_realized_volatility", "number", "realized path volatility", False),
        _field("stress_event_observed", "boolean", "whether stress materialized"),
        _field("false_risk_cap_case", "boolean", "trigger without observed stress"),
        _field("missed_stress_case", "boolean", "stress without prior trigger"),
        _field("data_quality_status", "string", "visible data quality status"),
        _field("source_price_artifact_path", "string", "validated price artifact"),
        _field("row_checksum", "string", "checksum of normalized row payload"),
    ]
    return {
        "schema_name": "risk_cap_trigger_followup_schema",
        "schema_version": "risk_cap_trigger_followup_schema.v1",
        "candidate_id": inputs.candidate,
        "primary_key": ["followup_record_id"],
        "unique_key": ["trigger_record_id", "target_asset", "followup_horizon"],
        "followup_horizons": list(FOLLOWUP_HORIZONS),
        "status_values": [
            "pending",
            "mature",
            "blocked_data_quality",
            "incomplete_market_data",
        ],
        "required_fields": [field["name"] for field in fields if field["required"]],
        "field_definitions": fields,
        "field_defaults": {
            "candidate_id": inputs.candidate,
            "false_risk_cap_case": False,
            "missed_stress_case": False,
            **SAFETY_FIELDS,
        },
        "validation_rules": [
            "each triggered asset and horizon creates one follow-up record",
            "follow-up remains pending until the horizon matures",
            "data-quality failure blocks maturation instead of imputing outcomes",
            "follow-up classification is evidence only and cannot emit trades",
        ],
        **SAFETY_FIELDS,
    }


def build_forward_observe_storage_layout(
    inputs: ForwardObserveEvidenceAccumulationInputs,
) -> dict[str, Any]:
    storage_root = (
        "outputs/forward_observe/risk_cap/"
        "volatility_regime_scope_narrowed_risk_cap_v1/"
    )
    return {
        "storage_layout_version": "risk_cap_forward_observe_storage_layout.v1",
        "storage_root_design": storage_root,
        "storage_created_by_this_task": False,
        "append_only": True,
        "tables": [
            _storage_table("daily_observe_records", "daily_observe_records.csv"),
            _storage_table("trigger_followups", "trigger_followups.csv"),
            _storage_table("weekly_observe_reviews", "weekly_observe_reviews.jsonl"),
            _storage_table("data_quality_links", "data_quality_links.jsonl"),
            _storage_table("source_manifest", "source_manifest.json"),
        ],
        "partitioning": ["candidate_id", "calendar_year"],
        "audit_requirements": [
            "provider name, endpoint, request parameters, row count, and checksum where practical",
            "source artifact path and checksum for every observe and follow-up record",
            "data-quality status visible in every downstream summary",
            "append-only correction records instead of in-place silent mutation",
        ],
        "retention_policy": "retain_all_observe_records_until_owner_review_closes_task",
        **SAFETY_FIELDS,
    }


def build_forward_observe_runtime_safety_boundary(
    inputs: ForwardObserveEvidenceAccumulationInputs,
) -> dict[str, Any]:
    operational = inputs.readiness_payloads["operational_boundary"]
    return {
        "candidate_id": inputs.candidate,
        "boundary_version": "risk_cap_forward_observe_runtime_safety_boundary.v1",
        "allowed_actions": [ALLOWED_ACTION],
        "forbidden_actions": sorted(BANNED_ALLOWED_ACTIONS),
        "allowed_outputs": list(operational.get("allowed_outputs", [])),
        "forbidden_outputs": list(operational.get("forbidden_outputs", [])),
        "reporting_boundary": {
            "daily_report_integration": "design_only",
            "weekly_report_integration": "design_only",
            "production_daily_report_integration_allowed_now": False,
            "production_weekly_report_integration_allowed_now": False,
        },
        "data_quality_boundary": {
            "required_gate": "aits validate-data or same validation code path",
            "stop_on_failure": True,
            "pass_with_warnings_allowed_only_if_visible": True,
        },
        "interpretation_boundary": [
            "risk_cap_trigger_is_not_buy_sell_or_rebalance_signal",
            "evidence_accumulation_status_is_not_promotion_readiness",
            "owner_precheck_candidate_is_not_owner_approval",
        ],
        **SAFETY_FIELDS,
    }


def build_minimum_observation_window_policy(
    inputs: ForwardObserveEvidenceAccumulationInputs,
) -> dict[str, Any]:
    evidence = inputs.readiness_payloads["evidence_collection_spec"]
    return {
        "policy_version": "risk_cap_forward_observe_observation_window_policy.v1",
        "minimum_observe_days": int(
            evidence.get("minimum_observe_days") or MINIMUM_OBSERVE_DAYS
        ),
        "minimum_active_trigger_count": int(
            evidence.get("minimum_active_trigger_count") or MINIMUM_ACTIVE_TRIGGER_COUNT
        ),
        "minimum_review_windows": int(
            evidence.get("minimum_review_windows") or MINIMUM_REVIEW_WINDOWS
        ),
        "followup_horizons": list(FOLLOWUP_HORIZONS),
        "sparse_sample_handling": "extend_observe_without_promotion_or_paper_shadow",
        "staleness_warning_policy": "warn_if_trigger_active_without_new_evidence",
        "heuristic_governance": {
            "policy_status": "temporary_pilot_baseline",
            "source_task": "TRADING-2293",
            "rationale": "minimum evidence maturity before owner precheck discussion",
            "not_investment_threshold": True,
            "exit_condition": (
                "replace or recalibrate after forward observe evidence is reviewed"
            ),
        },
        **SAFETY_FIELDS,
    }


def build_weekly_review_cadence(
    inputs: ForwardObserveEvidenceAccumulationInputs,
) -> dict[str, Any]:
    weekly_contract = inputs.readiness_payloads["weekly_review_contract"]
    return {
        "cadence_version": "risk_cap_forward_observe_weekly_review_cadence.v1",
        "review_frequency": "weekly",
        "review_trigger": "after_weekly_market_close_or_manual_research_run",
        "required_inputs": [
            "daily_observe_records",
            "matured_trigger_followups",
            "latest_data_quality_report",
            "source_artifact_manifest",
        ],
        "review_fields": list(weekly_contract.get("fields", [])),
        "weekly_status_values": [
            "continue_observe",
            "extend_observe_sparse_sample",
            "manual_review_required",
            "stop_observe_recommended",
        ],
        "manual_review_route": "research_owner_review_only",
        **SAFETY_FIELDS,
    }


def build_evidence_accumulation_decision_matrix(
    inputs: ForwardObserveEvidenceAccumulationInputs,
) -> list[dict[str, Any]]:
    warnings = set(inputs.readiness_payloads["summary"].get("readiness_warnings", []))
    return [
        _decision_row(
            case_id="sufficient_stable_triggers",
            condition="trigger_count_and_review_windows_meet_minimums",
            decision="continue_observe",
            rationale="evidence is accumulating but remains observe-only",
        ),
        _decision_row(
            case_id="sparse_trigger_sample",
            condition="trigger_count_below_minimum_or_direction_bucket_sparse",
            decision="extend_observe",
            rationale="sparse sample cannot support owner precheck",
            source_warning_active="TRIGGER_DIRECTION_SAMPLE_SPARSE" in warnings,
        ),
        _decision_row(
            case_id="data_quality_failure",
            condition="data_quality_status_fail_or_missing",
            decision="stop_record_generation_until_quality_passes",
            rationale="cached-data gate is mandatory for observe records",
        ),
        _decision_row(
            case_id="false_risk_cap_cost_high",
            condition="false_risk_cap_cases_or_cost_materially_high",
            decision="stop_observe_or_redesign",
            rationale="risk-cap veto cost may dominate stress capture",
        ),
        _decision_row(
            case_id="stress_capture_stable",
            condition="post_trigger_stress_capture_stable_across_review_windows",
            decision="owner_precheck_candidate_only",
            rationale="owner precheck can be prepared but no promotion is allowed",
        ),
        _decision_row(
            case_id="long_active_stale",
            condition="risk_cap_active_without_new_followup_evidence",
            decision="staleness_warning_and_manual_review",
            rationale="active trigger aging must be visible before interpretation",
        ),
    ]


def build_evidence_accumulation_plan(
    *,
    inputs: ForwardObserveEvidenceAccumulationInputs,
    observation_policy: Mapping[str, Any],
    decision_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "plan_version": "risk_cap_forward_observe_evidence_accumulation_plan.v1",
        "candidate_id": inputs.candidate,
        "source_readiness_gate_status": _source_readiness_gate(inputs),
        "source_readiness_warnings": list(
            inputs.readiness_payloads["summary"].get("readiness_warnings", [])
        ),
        "required_evidence_artifacts": [
            "risk_cap_daily_observe_record_schema",
            "risk_cap_trigger_followup_schema",
            "forward_observe_weekly_review_cadence",
            "forward_observe_evidence_accumulation_decision_matrix",
        ],
        "minimum_observation_policy": dict(observation_policy),
        "decision_case_ids": [str(row["case_id"]) for row in decision_rows],
        "data_quality_status": _source_data_quality_status(inputs),
        "data_quality_contract": (
            "TRADING-2294 is design-only and inherits TRADING-2293 source quality. "
            "Any future record generation must run aits validate-data first."
        ),
        "evidence_accumulation_ready": True,
        "runtime_started": False,
        **SAFETY_FIELDS,
    }


def build_evidence_accumulation_summary(
    *,
    inputs: ForwardObserveEvidenceAccumulationInputs,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        **_common_payload(inputs=inputs, generated_at=generated_at),
        "candidate_reviewed": inputs.candidate,
        "source_task": "TRADING-2293_SCOPE_NARROWED_FORWARD_OBSERVE_READINESS_REVIEW",
        "source_readiness_gate_status": _source_readiness_gate(inputs),
        "source_readiness_review_status": inputs.readiness_payloads["summary"].get(
            "readiness_review_status"
        ),
        "source_readiness_warnings": list(
            inputs.readiness_payloads["summary"].get("readiness_warnings", [])
        ),
        "source_data_quality_status": _source_data_quality_status(inputs),
        "next_runtime_state": "design_ready_runtime_not_started",
        "runtime_contract_generated": True,
        "daily_observe_record_schema_generated": True,
        "trigger_followup_schema_generated": True,
        "storage_layout_generated": True,
        "runtime_safety_boundary_generated": True,
        "forward_observe_started": False,
        "runtime_started": False,
        "owner_review_package_generated": False,
        "paper_shadow_ready": False,
        "production_ready": False,
        "broker_ready": False,
    }


def write_forward_observe_evidence_accumulation_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    artifacts: Mapping[str, Any],
) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary": output_dir / "forward_observe_evidence_accumulation_plan_summary.json",
        "plan": output_dir / "forward_observe_evidence_accumulation_plan.json",
        "runtime_contract": output_dir / "forward_observe_runtime_contract.json",
        "daily_schema": output_dir / "risk_cap_daily_observe_record_schema.json",
        "followup_schema": output_dir / "risk_cap_trigger_followup_schema.json",
        "storage_layout": output_dir / "forward_observe_storage_layout.json",
        "safety_boundary": output_dir / "forward_observe_runtime_safety_boundary.json",
        "observation_policy": output_dir / "forward_observe_minimum_observation_policy.json",
        "weekly_cadence": output_dir / "forward_observe_weekly_review_cadence.json",
        "decision_matrix_json": output_dir / "forward_observe_evidence_decision_matrix.json",
        "decision_matrix_csv": output_dir / "forward_observe_evidence_decision_matrix.csv",
        "runtime_design_markdown": output_dir / "forward_observe_runtime_design.md",
        "runtime_design_doc": docs_root / "forward_observe_runtime_design.md",
        "daily_schema_doc": docs_root / "risk_cap_daily_observe_record_schema.md",
        "followup_schema_doc": docs_root / "risk_cap_trigger_followup_schema.md",
        "safety_boundary_doc": docs_root / "forward_observe_runtime_safety_boundary.md",
    }
    write_json(paths["summary"], artifacts["summary"])
    write_json(paths["plan"], artifacts["plan"])
    write_json(paths["runtime_contract"], artifacts["runtime_contract"])
    write_json(paths["daily_schema"], artifacts["daily_observe_record_schema"])
    write_json(paths["followup_schema"], artifacts["trigger_followup_schema"])
    write_json(paths["storage_layout"], artifacts["storage_layout"])
    write_json(paths["safety_boundary"], artifacts["runtime_safety_boundary"])
    write_json(paths["observation_policy"], artifacts["minimum_observation_window_policy"])
    write_json(paths["weekly_cadence"], artifacts["weekly_review_cadence"])
    write_json(paths["decision_matrix_json"], artifacts["decision_matrix"])
    write_csv_rows(paths["decision_matrix_csv"], artifacts["decision_matrix"]["rows"])

    docs = artifacts["docs"]
    write_markdown(paths["runtime_design_markdown"], docs["runtime_design"])
    write_markdown(paths["runtime_design_doc"], docs["runtime_design"])
    write_markdown(paths["daily_schema_doc"], docs["daily_schema"])
    write_markdown(paths["followup_schema_doc"], docs["followup_schema"])
    write_markdown(paths["safety_boundary_doc"], docs["safety_boundary"])
    return {key: str(path) for key, path in paths.items()}


def build_forward_observe_runtime_docs(
    *,
    summary: Mapping[str, Any],
    runtime_contract: Mapping[str, Any],
    daily_schema: Mapping[str, Any],
    followup_schema: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
    decision_rows: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    runtime_design = "\n".join(
        [
            "# Forward Observe Runtime Design",
            "",
            "TRADING-2294 只定义 risk-cap observe-only evidence accumulation contract。",
            "",
            f"- candidate_id: `{summary['candidate_reviewed']}`",
            f"- source_readiness_gate_status: `{summary['source_readiness_gate_status']}`",
            f"- source_data_quality_status: `{summary['source_data_quality_status']}`",
            f"- runtime_started: `{summary['runtime_started']}`",
            f"- daily_report_integration: `{summary['daily_report_integration']}`",
            f"- weekly_report_integration: `{summary['weekly_report_integration']}`",
            "",
            "## Pipeline Contract",
            "",
            *[f"- `{item}`" for item in runtime_contract["daily_pipeline_contract"]],
            "",
            "## Decision Matrix",
            "",
            "|case_id|decision|source_warning_active|",
            "|---|---|---:|",
            *[
                (
                    f"|`{row['case_id']}`|`{row['decision']}`|"
                    f"{row['source_warning_active']}|"
                )
                for row in decision_rows
            ],
            "",
            (
                "本设计不启动 runtime，不接生产日报，不产生 paper-shadow、"
                "production 或 broker action。"
            ),
            "",
        ]
    )
    daily_schema_doc = "\n".join(
        [
            "# Risk-Cap Daily Observe Record Schema",
            "",
            "每日 observe record 是 append-only research evidence，不是交易指令。",
            "",
            f"- schema_version: `{daily_schema['schema_version']}`",
            f"- allowed_action_values: `{', '.join(daily_schema['allowed_action_values'])}`",
            (
                "- data_quality_status_values: "
                f"`{', '.join(daily_schema['data_quality_status_values'])}`"
            ),
            "",
            "|field|required|type|",
            "|---|---:|---|",
            *[
                f"|`{field['name']}`|{field['required']}|`{field['type']}`|"
                for field in daily_schema["field_definitions"]
            ],
            "",
        ]
    )
    followup_schema_doc = "\n".join(
        [
            "# Risk-Cap Trigger Follow-Up Schema",
            "",
            "Trigger follow-up 只记录 5d / 10d / 20d 后验路径用于复盘。",
            "",
            f"- schema_version: `{followup_schema['schema_version']}`",
            f"- followup_horizons: `{', '.join(followup_schema['followup_horizons'])}`",
            "",
            "|field|required|type|",
            "|---|---:|---|",
            *[
                f"|`{field['name']}`|{field['required']}|`{field['type']}`|"
                for field in followup_schema["field_definitions"]
            ],
            "",
        ]
    )
    safety_boundary_doc = "\n".join(
        [
            "# Forward Observe Runtime Safety Boundary",
            "",
            f"- observe_mode: `{safety_boundary['observe_mode']}`",
            f"- portfolio_effect: `{safety_boundary['portfolio_effect']}`",
            f"- production_effect: `{safety_boundary['production_effect']}`",
            f"- broker_action: `{safety_boundary['broker_action']}`",
            "",
            "## Interpretation Boundary",
            "",
            *[f"- `{item}`" for item in safety_boundary["interpretation_boundary"]],
            "",
            "Promotion、paper-shadow、production、broker action 全部保持 false / none。",
            "",
        ]
    )
    return {
        "runtime_design": runtime_design,
        "daily_schema": daily_schema_doc,
        "followup_schema": followup_schema_doc,
        "safety_boundary": safety_boundary_doc,
    }


def _load_required_payloads(root: Path) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for key, filename in REQUIRED_READINESS_FILES.items():
        path = root / filename
        if not path.exists():
            raise ForwardObserveEvidenceAccumulationPlanError(
                f"missing required TRADING-2293 readiness artifact: {path}"
            )
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ForwardObserveEvidenceAccumulationPlanError(
                f"readiness artifact must be a JSON object: {path}"
            )
        payloads[key] = dict(payload)
    return payloads


def _validate_readiness_gate(payloads: Mapping[str, Mapping[str, Any]], candidate: str) -> None:
    summary = payloads["summary"]
    checklist = payloads["gate_checklist"]
    if summary.get("candidate_reviewed") != candidate:
        raise ForwardObserveEvidenceAccumulationPlanError(
            f"readiness summary candidate mismatch: {summary.get('candidate_reviewed')}"
        )
    readiness_gate = str(
        summary.get("readiness_gate_status") or checklist.get("readiness_gate_status")
    )
    if readiness_gate not in READY_GATES:
        raise ForwardObserveEvidenceAccumulationPlanError(
            f"readiness gate must be ready before TRADING-2294: {readiness_gate}"
        )
    if summary.get("forward_observe_readiness_recommendation") is not True:
        raise ForwardObserveEvidenceAccumulationPlanError(
            "TRADING-2293 did not recommend forward observe readiness"
        )
    next_task = str(summary.get("next_task_recommendation") or "")
    if next_task not in NEXT_TASKS:
        raise ForwardObserveEvidenceAccumulationPlanError(
            f"TRADING-2293 next task is not a 2294 evidence plan route: {next_task}"
        )
    if summary.get("forward_observe_started") is not False:
        raise ForwardObserveEvidenceAccumulationPlanError(
            "TRADING-2294 cannot consume readiness output that already started observe"
        )


def _validate_input_safety(name: str, payload: Mapping[str, Any]) -> None:
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise ForwardObserveEvidenceAccumulationPlanError(f"{name} opens promotion_allowed")
        if item.get("paper_shadow_allowed") is True:
            raise ForwardObserveEvidenceAccumulationPlanError(
                f"{name} opens paper_shadow_allowed"
            )
        if item.get("production_allowed") is True:
            raise ForwardObserveEvidenceAccumulationPlanError(f"{name} opens production_allowed")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise ForwardObserveEvidenceAccumulationPlanError(f"{name} opens broker_action")
        if item.get("owner_review_required") is True:
            raise ForwardObserveEvidenceAccumulationPlanError(
                f"{name} opens owner_review_required"
            )
        if item.get("forward_observe_started") is True:
            raise ForwardObserveEvidenceAccumulationPlanError(
                f"{name} opens forward_observe_started"
            )
        if item.get("runtime_started") is True:
            raise ForwardObserveEvidenceAccumulationPlanError(f"{name} opens runtime_started")
        if str(item.get("allowed_action", ALLOWED_ACTION)) in BANNED_ALLOWED_ACTIONS:
            raise ForwardObserveEvidenceAccumulationPlanError(
                f"{name} emits banned allowed_action"
            )
        for value in item.values():
            if isinstance(value, str) and value in BANNED_RECOMMENDATIONS:
                raise ForwardObserveEvidenceAccumulationPlanError(
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


def _common_payload(
    *,
    inputs: ForwardObserveEvidenceAccumulationInputs,
    generated_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "title": "Forward Observe Evidence Accumulation Plan",
        "task_id": TASK_ID,
        "status": STATUS,
        "summary_status": STATUS,
        "artifact_role": ARTIFACT_ROLE,
        "mode": MODE,
        "generated_at": generated_at.isoformat(),
        "candidate_id": inputs.candidate,
        "target_assets": list(inputs.target_assets),
        "horizons": list(inputs.horizons),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "selected_market_regime": MARKET_REGIME,
        "actual_requested_date_range": "observe_only_design_contract",
        "source_readiness_dir": str(inputs.readiness_dir),
        "source_artifact_paths": dict(inputs.artifact_paths),
        "source_readiness_gate_status": _source_readiness_gate(inputs),
        "source_data_quality_status": _source_data_quality_status(inputs),
        **SAFETY_FIELDS,
    }


def _source_readiness_gate(inputs: ForwardObserveEvidenceAccumulationInputs) -> str:
    summary = inputs.readiness_payloads["summary"]
    checklist = inputs.readiness_payloads["gate_checklist"]
    return str(summary.get("readiness_gate_status") or checklist.get("readiness_gate_status"))


def _source_data_quality_status(inputs: ForwardObserveEvidenceAccumulationInputs) -> str:
    checklist = inputs.readiness_payloads["gate_checklist"]
    evidence = inputs.readiness_payloads["evidence_collection_spec"]
    return str(
        checklist.get("data_quality_status")
        or evidence.get("data_quality_status")
        or "UNKNOWN"
    )


def _field(
    name: str,
    field_type: str,
    description: str,
    required: bool = True,
) -> dict[str, Any]:
    return {
        "name": name,
        "type": field_type,
        "required": required,
        "description": description,
    }


def _storage_table(name: str, filename: str) -> dict[str, Any]:
    return {
        "name": name,
        "filename": filename,
        "append_only": True,
        "created_by_this_task": False,
        "checksum_required": True,
    }


def _decision_row(
    *,
    case_id: str,
    condition: str,
    decision: str,
    rationale: str,
    source_warning_active: bool = False,
) -> dict[str, Any]:
    return {
        "case_id": case_id,
        "condition": condition,
        "decision": decision,
        "rationale": rationale,
        "source_warning_active": source_warning_active,
        **SAFETY_FIELDS,
    }


def _normalize_or_source(value: Sequence[str] | str, source: object) -> tuple[str, ...]:
    parsed = _normalize_list(value)
    if parsed:
        return parsed
    if isinstance(source, list | tuple):
        return tuple(str(item).strip() for item in source if str(item).strip())
    return ()


def _normalize_list(value: Sequence[str] | str) -> tuple[str, ...]:
    if isinstance(value, str):
        return parse_csv_list(value)
    return tuple(str(item).strip() for item in value if str(item).strip())


__all__ = [
    "DEFAULT_DOCS_ROOT",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_READINESS_ROOT",
    "MODE",
    "STATUS",
    "ForwardObserveEvidenceAccumulationPlanError",
    "build_evidence_accumulation_decision_matrix",
    "build_forward_observe_runtime_contract",
    "build_forward_observe_runtime_safety_boundary",
    "build_risk_cap_daily_observe_record_schema",
    "build_risk_cap_trigger_followup_schema",
    "load_forward_observe_evidence_accumulation_inputs",
    "run_forward_observe_evidence_accumulation_plan",
]
