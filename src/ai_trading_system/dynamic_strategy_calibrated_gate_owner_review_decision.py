from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT,
    RANKING_TOP_CANDIDATE,
)
from ai_trading_system.dynamic_strategy_expanded_candidate_pool_retest import (
    READY_STATUS as SOURCE_2386_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT,
    REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
)
from ai_trading_system.dynamic_strategy_observation_gate_threshold_calibration_review import (
    READY_STATUS as SOURCE_2387_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_report_common import (
    load_json_mapping as _load_json_document,
)
from ai_trading_system.dynamic_strategy_report_common import (
    write_json_artifact,
    write_markdown_artifact,
)
from ai_trading_system.dynamic_strategy_research_filter_threshold_methodology_review import (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_research_filter_threshold_methodology_review import (
    NEXT_ROUTE as SOURCE_2388_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_research_filter_threshold_methodology_review import (
    READY_STATUS as SOURCE_2388_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2389"
TASK_REGISTER_ID = (
    "TRADING-2389_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION"
)
REPORT_TYPE = "dynamic_strategy_calibrated_gate_owner_review_decision"
SCHEMA_VERSION = "dynamic_strategy_calibrated_gate_owner_review_and_next_decision.v1"
READY_STATUS = (
    "DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_AND_NEXT_DECISION_"
    "BLOCKED_SOURCE_ARTIFACT"
)
OWNER_DECISION = (
    "ADOPT_CALIBRATED_RESEARCH_ONLY_GATE_METHODOLOGY_WITH_NO_OBSERVATION_APPROVAL"
)
NEXT_ROUTE = (
    "TRADING-2390_Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_"
    "And_Component_Attribution"
)
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OWNER_DECISION_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = ("TRADING-2386", "TRADING-2387", "TRADING-2388")
CANDIDATE_RECLASSIFICATION_TARGETS: tuple[str, ...] = (
    RANKING_TOP_CANDIDATE,
    "dynamic_turnover_budgeted_growth_tilt_v1",
    "dynamic_valid_until_expiry_strict_v1",
    "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
    "equal_risk_growth_tilt_guarded_turnover_v1",
)
SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "scheduler_enabled",
    "scheduled_task_created",
    "event_append_enabled",
    "historical_event_log_mutated",
    "outcome_binding_enabled",
    "outcome_store_mutated",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_trade_created",
    "shadow_position_created",
    "production_enabled",
    "broker_action_enabled",
    "order_generated",
    "daily_report_generated",
    "observation_approved",
    "paper_shadow_allowed",
    "production_allowed",
    "policy_update_applied",
    "rules_mutated",
)

DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_retest_result.json"
)
DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "expanded_candidate_ranking.json"
)
DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_EXPANDED_CANDIDATE_POOL_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2387_GATE_CALIBRATION_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    / "gate_calibration_review_result.json"
)
DEFAULT_SOURCE_2387_GATE_POLICY_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    / "gate_policy_review.json"
)
DEFAULT_SOURCE_2387_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    / "candidate_reclassification_preview.json"
)
DEFAULT_SOURCE_2387_RECOMMENDED_POLICY_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OBSERVATION_GATE_THRESHOLD_CALIBRATION_REVIEW_OUTPUT_ROOT
    / "recommended_gate_policy_update.json"
)
DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "threshold_methodology_review_result.json"
)
DEFAULT_SOURCE_2388_THRESHOLD_INVENTORY_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "threshold_inventory.json"
)
DEFAULT_SOURCE_2388_GATE_TAXONOMY_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "gate_taxonomy.json"
)
DEFAULT_SOURCE_2388_CANDIDATE_THRESHOLD_OUTCOME_MATRIX_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "candidate_threshold_outcome_matrix.json"
)
DEFAULT_SOURCE_2388_RECOMMENDED_GATE_POLICY_PROPOSAL_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_RESEARCH_FILTER_THRESHOLD_METHODOLOGY_REVIEW_OUTPUT_ROOT
    / "recommended_gate_policy_proposal.json"
)


def run_dynamic_strategy_calibrated_gate_owner_review_decision(
    *,
    source_expanded_candidate_retest_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RETEST_PATH
    ),
    source_expanded_candidate_ranking_path: Path = (
        DEFAULT_SOURCE_2386_EXPANDED_CANDIDATE_RANKING_PATH
    ),
    source_expanded_decision_update_path: Path = (
        DEFAULT_SOURCE_2386_DECISION_UPDATE_PATH
    ),
    source_gate_calibration_review_path: Path = (
        DEFAULT_SOURCE_2387_GATE_CALIBRATION_REVIEW_PATH
    ),
    source_gate_policy_review_path: Path = DEFAULT_SOURCE_2387_GATE_POLICY_REVIEW_PATH,
    source_candidate_reclassification_preview_path: Path = (
        DEFAULT_SOURCE_2387_CANDIDATE_RECLASSIFICATION_PREVIEW_PATH
    ),
    source_recommended_policy_update_path: Path = (
        DEFAULT_SOURCE_2387_RECOMMENDED_POLICY_UPDATE_PATH
    ),
    source_threshold_methodology_review_path: Path = (
        DEFAULT_SOURCE_2388_THRESHOLD_METHODOLOGY_REVIEW_PATH
    ),
    source_threshold_inventory_path: Path = DEFAULT_SOURCE_2388_THRESHOLD_INVENTORY_PATH,
    source_gate_taxonomy_path: Path = DEFAULT_SOURCE_2388_GATE_TAXONOMY_PATH,
    source_candidate_threshold_outcome_matrix_path: Path = (
        DEFAULT_SOURCE_2388_CANDIDATE_THRESHOLD_OUTCOME_MATRIX_PATH
    ),
    source_recommended_gate_policy_proposal_path: Path = (
        DEFAULT_SOURCE_2388_RECOMMENDED_GATE_POLICY_PROPOSAL_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_CALIBRATED_GATE_OWNER_REVIEW_DECISION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_expanded_candidate_retest_path=source_expanded_candidate_retest_path,
        source_expanded_candidate_ranking_path=source_expanded_candidate_ranking_path,
        source_expanded_decision_update_path=source_expanded_decision_update_path,
        source_gate_calibration_review_path=source_gate_calibration_review_path,
        source_gate_policy_review_path=source_gate_policy_review_path,
        source_candidate_reclassification_preview_path=(
            source_candidate_reclassification_preview_path
        ),
        source_recommended_policy_update_path=source_recommended_policy_update_path,
        source_threshold_methodology_review_path=(
            source_threshold_methodology_review_path
        ),
        source_threshold_inventory_path=source_threshold_inventory_path,
        source_gate_taxonomy_path=source_gate_taxonomy_path,
        source_candidate_threshold_outcome_matrix_path=(
            source_candidate_threshold_outcome_matrix_path
        ),
        source_recommended_gate_policy_proposal_path=(
            source_recommended_gate_policy_proposal_path
        ),
    )
    ready = not sources["source_validation_errors"]
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=as_of_date,
        sources=sources,
    )
    payload.update(_ready_sections(sources) if ready else _blocked_sections(sources))
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(**paths: Path) -> dict[str, Any]:
    source_files = {
        key.removeprefix("source_").removesuffix("_path"): path
        for key, path in paths.items()
    }
    documents = {key: _load_json_document(path) for key, path in source_files.items()}
    sources: dict[str, Any] = {
        **documents,
        "source_files": {key: str(path) for key, path in source_files.items()},
        "source_hashes": {
            key: _file_sha256(path) if path.exists() else None
            for key, path in source_files.items()
        },
        "source_status": {
            key: _as_mapping(document).get("status")
            for key, document in documents.items()
        },
    }
    sources["source_validation_errors"] = _source_validation_errors(sources)
    sources["source_ready_for_calibrated_gate_owner_review"] = not sources[
        "source_validation_errors"
    ]
    return sources


def _source_validation_errors(sources: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_status = {
        "expanded_candidate_retest": SOURCE_2386_READY_STATUS,
        "expanded_candidate_ranking": SOURCE_2386_READY_STATUS,
        "expanded_decision_update": SOURCE_2386_READY_STATUS,
        "gate_calibration_review": SOURCE_2387_READY_STATUS,
        "gate_policy_review": SOURCE_2387_READY_STATUS,
        "candidate_reclassification_preview": SOURCE_2387_READY_STATUS,
        "recommended_policy_update": SOURCE_2387_READY_STATUS,
        "threshold_methodology_review": SOURCE_2388_READY_STATUS,
        "threshold_inventory": SOURCE_2388_READY_STATUS,
        "gate_taxonomy": SOURCE_2388_READY_STATUS,
        "candidate_threshold_outcome_matrix": SOURCE_2388_READY_STATUS,
        "recommended_gate_policy_proposal": SOURCE_2388_READY_STATUS,
    }
    source_status = _as_mapping(sources.get("source_status"))
    for key, expected in expected_status.items():
        actual = source_status.get(key)
        if actual != expected:
            errors.append(f"{key}.status expected {expected}, got {actual!r}")

    expanded = _as_mapping(sources.get("expanded_candidate_retest"))
    if expanded.get("best_candidate_after_expanded_screening") != RANKING_TOP_CANDIDATE:
        errors.append("2386 current best candidate must remain ranking-top reference")
    if expanded.get("best_candidate_decision") != "CONTINUE_OPTIMIZATION":
        errors.append("2386 current best decision must remain CONTINUE_OPTIMIZATION")
    if expanded.get("candidate_ready_for_research_only_observation") is True:
        errors.append("2386 must not already approve research-only observation")

    gate_review = _as_mapping(sources.get("gate_calibration_review"))
    if (
        gate_review.get("reference_candidate_policy_recommendation")
        != REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    ):
        errors.append("2387 reference candidate policy recommendation mismatch")
    if gate_review.get("observation_approved") is True:
        errors.append("2387 must not approve observation")
    if gate_review.get("policy_update_applied") is True:
        errors.append("2387 must not mutate policy")

    methodology = _as_mapping(sources.get("threshold_methodology_review"))
    if methodology.get("recommended_next_research_task") != SOURCE_2388_EXPECTED_ROUTE:
        errors.append("2388 route must point to TRADING-2389")
    if methodology.get("threshold_methodology_review_ready") is not True:
        errors.append("2388 threshold methodology review must be ready")
    if methodology.get("research_only_vs_paper_shadow_gate_separated") is not True:
        errors.append("2388 gate taxonomy must separate research-only and paper-shadow")
    if (
        methodology.get("reference_candidate_policy_recommendation")
        != REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
    ):
        errors.append("2388 reference candidate policy recommendation mismatch")

    proposal = _recommended_policy_proposal(sources)
    reference_policy = _as_mapping(proposal.get("reference_candidate_policy"))
    if reference_policy.get("recommended") != REFERENCE_CANDIDATE_POLICY_RECOMMENDATION:
        errors.append("2388 recommended gate policy proposal mismatch")

    matrix_ids = {
        str(row.get("candidate_id"))
        for row in _candidate_threshold_rows(sources)
        if row.get("candidate_id")
    }
    missing_targets = set(CANDIDATE_RECLASSIFICATION_TARGETS).difference(matrix_ids)
    if missing_targets:
        errors.append(f"2388 candidate matrix missing targets: {sorted(missing_targets)}")

    for source_name in expected_status:
        errors.extend(
            _side_effect_validation_errors(
                source_name,
                _as_mapping(sources.get(source_name)),
            )
        )
    return errors


def _base_payload(
    *,
    status: str,
    as_of_date: date | None,
    sources: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat() if as_of_date else None,
        "market_regime": AI_REGIME_SUMMARY.get("market_regime", "ai_after_chatgpt"),
        "market_regime_summary": AI_REGIME_SUMMARY,
        "source_tasks": list(SOURCE_TASKS),
        "source_files": dict(_as_mapping(sources.get("source_files"))),
        "source_hashes": dict(_as_mapping(sources.get("source_hashes"))),
        "source_status": dict(_as_mapping(sources.get("source_status"))),
        "source_validation_errors": list(sources.get("source_validation_errors", [])),
        "source_ready_for_calibrated_gate_owner_review": bool(
            sources.get("source_ready_for_calibrated_gate_owner_review")
        ),
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "data_quality_status": DATA_QUALITY_GATE_REASON,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "new_signal_generated": False,
        "scoring_run": False,
        "owner_decision_recorded": False,
        "owner_decision": None,
        "threshold_methodology_adopted": False,
        "research_only_vs_paper_shadow_gate_separated": False,
        "reference_candidate_policy_adopted": None,
        "candidate_auto_accept_approved": False,
        "current_best_candidate_observation_approved": False,
        "calibrated_reclassification_preview_approved": False,
        "component_attribution_review_required": False,
        "future_statistical_threshold_calibration_required": False,
        "observation_approved": False,
        "paper_shadow_allowed": False,
        "paper_shadow_approved": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "artifact_paths": {},
    }


def _ready_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    source_findings = _source_findings(sources)
    adoption = _calibrated_gate_adoption_record(sources)
    non_approval = _non_approval_record(sources)
    route = _next_reclassification_route(sources)
    return {
        "owner_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "threshold_methodology_adopted": True,
        "research_only_vs_paper_shadow_gate_separated": True,
        "reference_candidate_policy_adopted": (
            REFERENCE_CANDIDATE_POLICY_RECOMMENDATION
        ),
        "candidate_auto_accept_approved": False,
        "current_best_candidate_observation_approved": False,
        "calibrated_reclassification_preview_approved": True,
        "component_attribution_review_required": True,
        "future_statistical_threshold_calibration_required": True,
        "source_findings_from_2387_2388": source_findings,
        "owner_decision_items": _owner_decision_items(),
        "calibrated_gate_adoption_record": adoption,
        "non_approval_record": non_approval,
        "next_reclassification_route": route,
        "guardrail_summary": _guardrail_summary(),
        "explicit_non_goals": _explicit_non_goals(),
    }


def _blocked_sections(sources: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "blocked_reason": "source_artifact_validation_failed",
        "owner_decision_items": {},
        "calibrated_gate_adoption_record": {},
        "non_approval_record": {},
        "next_reclassification_route": {
            "recommended_next_research_task": NEXT_ROUTE,
            "blocked_until": list(sources.get("source_validation_errors", [])),
        },
        "guardrail_summary": _guardrail_summary(),
        "explicit_non_goals": _explicit_non_goals(),
    }


def _source_findings(sources: Mapping[str, Any]) -> dict[str, Any]:
    expanded = _as_mapping(sources.get("expanded_candidate_retest"))
    gate_review = _as_mapping(sources.get("gate_calibration_review"))
    methodology = _as_mapping(sources.get("threshold_methodology_review"))
    return {
        "trading_2386": {
            "status": expanded.get("status"),
            "current_best_candidate": expanded.get(
                "best_candidate_after_expanded_screening"
            ),
            "current_best_decision": expanded.get("best_candidate_decision"),
            "candidate_ready_for_research_only_observation": expanded.get(
                "candidate_ready_for_research_only_observation"
            ),
        },
        "trading_2387": {
            "status": gate_review.get("status"),
            "research_only_gate_may_be_too_strict": gate_review.get(
                "research_only_gate_may_be_too_strict"
            ),
            "reference_candidate_policy_recommendation": gate_review.get(
                "reference_candidate_policy_recommendation"
            ),
            "observation_approved": gate_review.get("observation_approved"),
            "policy_update_applied": gate_review.get("policy_update_applied"),
        },
        "trading_2388": {
            "status": methodology.get("status"),
            "threshold_methodology_review_ready": methodology.get(
                "threshold_methodology_review_ready"
            ),
            "current_gate_may_be_too_strict_for_research_only_observation": (
                methodology.get(
                    "current_gate_may_be_too_strict_for_research_only_observation"
                )
            ),
            "reference_candidate_policy_recommendation": methodology.get(
                "reference_candidate_policy_recommendation"
            ),
            "thresholds_requiring_statistical_calibration": methodology.get(
                "thresholds_requiring_statistical_calibration"
            ),
        },
    }


def _owner_decision_items() -> dict[str, Any]:
    return {
        "adopt_threshold_methodology_review": {
            "decision": "APPROVE",
            "source": "TRADING-2388",
        },
        "separate_research_only_and_paper_shadow_gates": {
            "decision": "APPROVE",
            "source": "TRADING-2388",
        },
        "update_reference_candidate_policy": {
            "decision": "APPROVE_FOR_RESEARCH_ONLY_GATE",
            "policy": REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
        },
        "allow_candidate_auto_accept": {
            "decision": "REJECT",
            "reason": "calibrated policy still requires owner review",
        },
        "approve_current_best_candidate_for_observation": {
            "decision": "REJECT_FOR_NOW",
            "reason": "2389 is policy decision, not candidate approval",
        },
        "allow_calibrated_reclassification_preview": {
            "decision": "APPROVE",
            "next_task": NEXT_ROUTE,
        },
        "require_component_attribution_review": {
            "decision": "APPROVE",
            "reason": "failed candidates may contain useful components",
        },
        "require_statistical_threshold_calibration": {
            "decision": "APPROVE_AS_FUTURE_RESEARCH",
            "reason": "current thresholds are not fully historical-calibrated",
        },
    }


def _calibrated_gate_adoption_record(sources: Mapping[str, Any]) -> dict[str, Any]:
    methodology = _as_mapping(sources.get("threshold_methodology_review"))
    proposal = _recommended_policy_proposal(sources)
    gate_taxonomy = _as_mapping(sources.get("gate_taxonomy")).get("gate_taxonomy")
    return {
        "owner_decision": OWNER_DECISION,
        "threshold_methodology_adopted": True,
        "policy_update_applied": False,
        "rules_mutated": False,
        "calibrated_research_only_gate_policy": {
            "research_only_observation": {
                "side_effect": "none",
                "threshold_level": "moderate",
                "owner_review_allowed": True,
                "auto_accept_allowed": "limited",
            },
            "paper_shadow": {
                "side_effect": "paper_trade_or_shadow_position",
                "threshold_level": "high",
                "explicit_owner_approval_required": True,
                "currently_disabled": True,
            },
            "production_broker": {
                "side_effect": "capital_risk",
                "threshold_level": "highest",
                "currently_out_of_scope": True,
            },
        },
        "reference_candidate_policy": {
            "old_policy": "HARD_BLOCK_ACCEPTANCE",
            "adopted_policy": REFERENCE_CANDIDATE_POLICY_RECOMMENDATION,
            "scope": "RESEARCH_ONLY_OBSERVATION_GATE_ONLY",
            "does_not_apply_to": ["paper_shadow", "production", "broker"],
        },
        "decision_mapping": _decision_mapping(),
        "source_thresholds_requiring_statistical_calibration": methodology.get(
            "thresholds_requiring_statistical_calibration",
            [],
        ),
        "source_gate_taxonomy": gate_taxonomy if isinstance(gate_taxonomy, Mapping) else {},
        "source_recommended_gate_policy_proposal": proposal,
    }


def _decision_mapping() -> dict[str, Any]:
    return {
        "ACCEPT_FOR_RESEARCH_ONLY_OBSERVATION": {
            "allowed": True,
            "requires": [
                "calibrated_gate_passed",
                "owner_review_recorded",
                "no_side_effect_flags_confirmed",
            ],
        },
        "OWNER_REVIEW_REQUIRED": {
            "allowed": True,
            "meaning": "candidate has enough evidence for human review, not automatic approval",
        },
        "CONTINUE_OPTIMIZATION": {
            "allowed": True,
            "meaning": "evidence exists but still insufficient for owner review or observation",
        },
        "REJECT_FOR_NOW": {
            "allowed": True,
            "meaning": "cost / drawdown / execution / evidence failure",
        },
        "DEPRECATED": {
            "allowed": True,
            "meaning": "candidate or result should no longer guide ranking",
        },
    }


def _non_approval_record(sources: Mapping[str, Any]) -> dict[str, Any]:
    expanded = _as_mapping(sources.get("expanded_candidate_retest"))
    return {
        "owner_decision": OWNER_DECISION,
        "current_best_candidate": expanded.get("best_candidate_after_expanded_screening"),
        "current_best_candidate_decision": expanded.get("best_candidate_decision"),
        "candidate_auto_accept_approved": False,
        "current_best_candidate_observation_approved": False,
        "research_only_observation_approved": False,
        "paper_shadow_approved": False,
        "event_append_approved": False,
        "outcome_binding_approved": False,
        "scheduler_approved": False,
        "production_approved": False,
        "broker_action_approved": False,
        "daily_report_approved": False,
        "reason": [
            "2389 adopts methodology and records owner decision only",
            "candidate approval requires a separate calibrated reclassification step",
            "paper-shadow / event / outcome / scheduler paths remain out of scope",
        ],
        "explicit_non_approval_list": [
            "research_only_observation_for_candidate",
            "candidate_auto_accept",
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
        ],
    }


def _next_reclassification_route(sources: Mapping[str, Any]) -> dict[str, Any]:
    matrix_by_id = {
        str(row.get("candidate_id")): row
        for row in _candidate_threshold_rows(sources)
        if row.get("candidate_id")
    }
    return {
        "recommended_next_research_task": NEXT_ROUTE,
        "task_name": (
            "Dynamic_Strategy_Calibrated_Gate_Candidate_Reclassification_And_"
            "Component_Attribution"
        ),
        "purpose": [
            "apply calibrated gate preview to 2386 candidates",
            "distinguish candidate failure from component value",
            "identify candidates eligible for OWNER_REVIEW_REQUIRED",
            "recommend whether current best candidate should enter owner-review-only decision",
        ],
        "candidate_reclassification_targets": [
            {
                "candidate_id": candidate_id,
                "latest_decision": _as_mapping(matrix_by_id.get(candidate_id)).get(
                    "latest_decision"
                ),
                "likely_reclassification_under_calibrated_gate": _as_mapping(
                    matrix_by_id.get(candidate_id)
                ).get("likely_reclassification_under_calibrated_gate"),
                "component_value_type": _as_mapping(matrix_by_id.get(candidate_id)).get(
                    "candidate_value_type"
                ),
            }
            for candidate_id in CANDIDATE_RECLASSIFICATION_TARGETS
        ],
        "allowed_actions": [
            "calibrated_reclassification_preview",
            "component_level_attribution",
            "owner_review_candidate_identification",
        ],
        "forbidden_actions": [
            "observation_approval",
            "paper_shadow_enablement",
            "event_append",
            "outcome_binding",
            "scheduler_enablement",
            "production_or_broker_action",
        ],
    }


def _guardrail_summary() -> dict[str, Any]:
    return {
        "observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _explicit_non_goals() -> dict[str, bool]:
    return {
        "enable_scheduler": False,
        "append_historical_event_log": False,
        "bind_outcome": False,
        "mutate_outcome_store": False,
        "enable_paper_shadow": False,
        "create_paper_trade": False,
        "create_shadow_position": False,
        "enable_production": False,
        "call_broker_api": False,
        "send_order": False,
        "create_scheduled_task": False,
        "generate_daily_report": False,
        "approve_research_only_observation_for_candidate": False,
        "run_new_backtest": False,
        "generate_new_signal": False,
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    paths = {
        "json_path": str(output_root / "owner_review_decision.json"),
        "calibrated_gate_adoption_record_json": str(
            output_root / "calibrated_gate_adoption_record.json"
        ),
        "non_approval_record_json": str(output_root / "non_approval_record.json"),
        "next_reclassification_route_json": str(
            output_root / "next_reclassification_route.json"
        ),
        "markdown_path": str(
            docs_root / "dynamic_strategy_calibrated_gate_owner_review_decision.md"
        ),
        "calibrated_gate_adoption_record_markdown": str(
            docs_root / "dynamic_strategy_calibrated_gate_adoption_record.md"
        ),
        "non_approval_record_markdown": str(
            docs_root / "dynamic_strategy_calibrated_gate_non_approval_record.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2390_route.md"),
    }
    payload["artifact_paths"] = paths
    write_json_artifact(Path(paths["json_path"]), payload)
    write_json_artifact(
        Path(paths["calibrated_gate_adoption_record_json"]),
        {
            "report_type": "dynamic_strategy_calibrated_gate_adoption_record",
            "schema_version": "dynamic_strategy_calibrated_gate_adoption_record.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "calibrated_gate_adoption_record": payload.get(
                "calibrated_gate_adoption_record",
                {},
            ),
            "policy_update_applied": False,
            "rules_mutated": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["non_approval_record_json"]),
        {
            "report_type": "dynamic_strategy_calibrated_gate_non_approval_record",
            "schema_version": "dynamic_strategy_calibrated_gate_non_approval_record.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "non_approval_record": payload.get("non_approval_record", {}),
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_json_artifact(
        Path(paths["next_reclassification_route_json"]),
        {
            "report_type": "dynamic_strategy_2390_reclassification_route",
            "schema_version": "dynamic_strategy_2390_reclassification_route.v1",
            "task_id": TASK_ID,
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "next_reclassification_route": payload.get(
                "next_reclassification_route",
                {},
            ),
            "recommended_next_research_task": NEXT_ROUTE,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    write_markdown_artifact(Path(paths["markdown_path"]), _main_markdown(payload))
    write_markdown_artifact(
        Path(paths["calibrated_gate_adoption_record_markdown"]),
        _adoption_markdown(payload),
    )
    write_markdown_artifact(
        Path(paths["non_approval_record_markdown"]),
        _non_approval_markdown(payload),
    )
    write_markdown_artifact(Path(paths["next_route_markdown"]), _route_markdown(payload))


def _main_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy calibrated gate owner review decision",
            "",
            "## 1. Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- next task：`{payload.get('recommended_next_research_task')}`",
            "",
            "## 2. Source findings from TRADING-2387 / 2388",
            "",
            "```json",
            json.dumps(
                payload.get("source_findings_from_2387_2388", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 3. Owner decision",
            "",
            "```json",
            json.dumps(
                payload.get("owner_decision_items", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 4. Calibrated gate adoption record",
            "",
            "```json",
            json.dumps(
                payload.get("calibrated_gate_adoption_record", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 5. Reference candidate policy decision",
            "",
            f"- adopted policy：`{payload.get('reference_candidate_policy_adopted')}`",
            "- scope：`RESEARCH_ONLY_OBSERVATION_GATE_ONLY`",
            "- paper-shadow / production / broker：not covered by this adoption.",
            "",
            "## 6. Research-only vs paper-shadow separation",
            "",
            f"- separated：`{payload.get('research_only_vs_paper_shadow_gate_separated')}`",
            "- research-only observation remains no-side-effect and owner-review gated.",
            "- paper-shadow remains disabled and requires separate explicit owner approval.",
            "",
            "## 7. Explicit non-approval list",
            "",
            "```json",
            json.dumps(
                payload.get("non_approval_record", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 8. Next reclassification route",
            "",
            "```json",
            json.dumps(
                payload.get("next_reclassification_route", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 9. Guardrail summary",
            "",
            "```json",
            json.dumps(
                payload.get("guardrail_summary", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "## 10. Recommended next task",
            "",
            f"- `{payload.get('recommended_next_research_task')}`",
            "",
        ]
    )


def _adoption_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy calibrated gate adoption record",
            "",
            f"- status：`{payload.get('status')}`",
            f"- owner decision：`{payload.get('owner_decision')}`",
            f"- threshold methodology adopted：`{payload.get('threshold_methodology_adopted')}`",
            (
                "- reference candidate policy adopted："
                f"`{payload.get('reference_candidate_policy_adopted')}`"
            ),
            "- policy update applied：`false`",
            "- rules mutated：`false`",
            "",
            "```json",
            json.dumps(
                payload.get("calibrated_gate_adoption_record", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ]
    )


def _non_approval_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy calibrated gate non-approval record",
            "",
            f"- status：`{payload.get('status')}`",
            "- candidate auto-accept approved：`false`",
            "- current best candidate observation approved：`false`",
            "- paper-shadow approved：`false`",
            "- event append / outcome binding approved：`false`",
            "- scheduler / production / broker approved：`false`",
            "",
            "```json",
            json.dumps(
                payload.get("non_approval_record", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic strategy TRADING-2390 route",
            "",
            f"- source task：`{TASK_ID}`",
            f"- status：`{payload.get('status')}`",
            f"- recommended next route：`{payload.get('recommended_next_research_task')}`",
            "- route type：`calibrated_reclassification_and_component_attribution`",
            "- observation approved：`false`",
            "- paper-shadow enabled：`false`",
            "- production enabled：`false`",
            "- broker action：`none`",
            "",
            "```json",
            json.dumps(
                payload.get("next_reclassification_route", {}),
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ]
    )


def _recommended_policy_proposal(sources: Mapping[str, Any]) -> dict[str, Any]:
    proposal_doc = _as_mapping(sources.get("recommended_gate_policy_proposal"))
    proposal = proposal_doc.get("recommended_gate_policy_proposal")
    if isinstance(proposal, Mapping):
        return dict(proposal)
    methodology = _as_mapping(sources.get("threshold_methodology_review"))
    proposal = methodology.get("recommended_gate_policy_proposal")
    return dict(proposal) if isinstance(proposal, Mapping) else {}


def _candidate_threshold_rows(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    matrix_doc = _as_mapping(sources.get("candidate_threshold_outcome_matrix"))
    rows = _as_list(matrix_doc.get("candidate_threshold_outcome_matrix"))
    if not rows:
        methodology = _as_mapping(sources.get("threshold_methodology_review"))
        rows = _as_list(methodology.get("candidate_threshold_outcome_matrix"))
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _side_effect_validation_errors(label: str, document: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for field in SAFETY_FALSE_FIELDS:
        if document.get(field) is True:
            errors.append(f"{label}.{field} must remain false")
    if document.get("broker_action") not in {None, "none"}:
        errors.append(f"{label}.broker_action must remain none")
    if document.get("production_effect") not in {None, "none"}:
        errors.append(f"{label}.production_effect must remain none")
    return errors


def _as_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
