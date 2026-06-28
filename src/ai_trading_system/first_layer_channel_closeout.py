from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

MARKET_REGIME = "ai_after_chatgpt"
ANCHOR_EVENT = "ChatGPT public launch"
ANCHOR_DATE = "2022-11-30"
PRIMARY_WINDOW_ID = "exact_three_asset_validated"
PRIMARY_WINDOW_ALIAS = "EXACT_THREE_ASSET_VALIDATED_WINDOW"
REQUESTED_START = "2021-02-22"

DEFAULT_ARCHIVE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_channel_archive_policy.yaml"
)
DEFAULT_FORWARD_MINIMAL_PLAN_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "first_layer_forward_diagnostic_minimal_plan.yaml"
)
DEFAULT_DO_NOT_ARCHIVE_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "do_not_de_risk_v3_archive.yaml"
)
DEFAULT_RISK_VETO_DIAGNOSTIC_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "risk_on_veto_observe_only_diagnostic.yaml"
)
DEFAULT_RISK_VETO_TRADEOFF_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "risk_on_veto_tradeoff_matrix.yaml"
)
DEFAULT_RISK_VETO_COMPATIBILITY_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "risk_on_veto_return_seeking_diagnostic_compatibility.yaml"
)
DEFAULT_INDICATOR_SELECTION_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "indicator_family_selection_matrix.yaml"
)
DEFAULT_RETURN_SEEKING_FINAL_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "return_seeking_diagnostic_lane_final_matrix.yaml"
)
DEFAULT_DEFENSIVE_FINAL_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "defensive_preservation_lane_final_matrix.yaml"
)
DEFAULT_CHANNEL_V3_FINAL_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "channel_specific_first_layer_v3_final_matrix.yaml"
)

DEFAULT_MASTER_CLOSEOUT_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_channel_master_closeout.yaml"
)
DEFAULT_MASTER_CLOSEOUT_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_channel_master_closeout.md"
)
DEFAULT_STATUS_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_channel_status_matrix.yaml"
)
DEFAULT_EVIDENCE_LABELING_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "first_layer_diagnostic_evidence_labeling.yaml"
)
DEFAULT_EVIDENCE_LABELING_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_diagnostic_evidence_labeling_review.md"
)
DEFAULT_REOPEN_CRITERIA_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_channel_reopen_criteria.yaml"
)
DEFAULT_REOPEN_CRITERIA_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_channel_reopen_criteria.md"
)
DEFAULT_PIT_GAP_ROADMAP_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_pit_data_gap_roadmap.yaml"
)
DEFAULT_PIT_GAP_ROADMAP_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_pit_data_gap_roadmap.md"
)
DEFAULT_FORWARD_MINIMAL_PLAN_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_forward_diagnostic_minimal_plan.md"
)
DEFAULT_OWNER_BRIEF_PATH = (
    PROJECT_ROOT / "docs" / "research" / "first_layer_channel_closeout_owner_brief.md"
)

SAFETY_BOUNDARY = {
    "research_only": True,
    "actual_path_required": True,
    "target_path_metrics_role": "diagnostic_only",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "manual_review_required": True,
    "dynamic_promotion_status": "BLOCKED",
}


def run_first_layer_channel_closeout_pack(
    *,
    archive_policy_path: Path = DEFAULT_ARCHIVE_POLICY_PATH,
    forward_minimal_plan_path: Path = DEFAULT_FORWARD_MINIMAL_PLAN_PATH,
    do_not_archive_path: Path = DEFAULT_DO_NOT_ARCHIVE_PATH,
    risk_veto_diagnostic_path: Path = DEFAULT_RISK_VETO_DIAGNOSTIC_PATH,
    risk_veto_tradeoff_path: Path = DEFAULT_RISK_VETO_TRADEOFF_PATH,
    risk_veto_compatibility_path: Path = DEFAULT_RISK_VETO_COMPATIBILITY_PATH,
    indicator_selection_path: Path = DEFAULT_INDICATOR_SELECTION_PATH,
    return_seeking_final_path: Path = DEFAULT_RETURN_SEEKING_FINAL_PATH,
    defensive_final_path: Path = DEFAULT_DEFENSIVE_FINAL_PATH,
    channel_v3_final_path: Path = DEFAULT_CHANNEL_V3_FINAL_PATH,
) -> dict[str, Any]:
    archive_policy = _load_mapping(archive_policy_path)
    forward_plan = _load_mapping(forward_minimal_plan_path)
    do_not = _load_mapping(do_not_archive_path)
    risk_veto = _load_mapping(risk_veto_diagnostic_path)
    tradeoff = _load_mapping(risk_veto_tradeoff_path)
    compatibility = _load_mapping(risk_veto_compatibility_path)
    indicator = _load_mapping(indicator_selection_path)
    return_seeking = _load_mapping(return_seeking_final_path)
    defensive = _load_mapping(defensive_final_path)
    channel_v3 = _load_mapping(channel_v3_final_path)

    _validate_archive_policy(archive_policy, forward_plan)
    status_rows = _status_rows(
        archive_policy=archive_policy,
        do_not=do_not,
        risk_veto=risk_veto,
        tradeoff=tradeoff,
        compatibility=compatibility,
        indicator=indicator,
        return_seeking=return_seeking,
        defensive=defensive,
    )
    status_matrix = _payload(
        report_type="first_layer_channel_status_matrix",
        title="First-Layer Channel Status Matrix",
        status="FIRST_LAYER_CHANNEL_STATUS_MATRIX_READY_ARCHIVED",
        summary={
            "channel_row_count": len(status_rows),
            "candidate_count": 0,
            "owner_review_allowed": False,
            "forward_watch_allowed": False,
            "promotion_allowed": False,
        },
        rows=status_rows,
    )
    _write_yaml(DEFAULT_STATUS_MATRIX_PATH, status_matrix)

    master = _master_closeout_payload(
        status_rows=status_rows,
        do_not=do_not,
        risk_veto=risk_veto,
        tradeoff=tradeoff,
        compatibility=compatibility,
        indicator=indicator,
        return_seeking=return_seeking,
        channel_v3=channel_v3,
    )
    _write_yaml(DEFAULT_MASTER_CLOSEOUT_PATH, master)
    _write_markdown(DEFAULT_MASTER_CLOSEOUT_DOC_PATH, _render_master_closeout(master))

    evidence = _evidence_labeling_payload()
    _write_yaml(DEFAULT_EVIDENCE_LABELING_PATH, evidence)
    _write_markdown(DEFAULT_EVIDENCE_LABELING_DOC_PATH, _render_evidence_labeling(evidence))

    reopen = _reopen_criteria_payload(archive_policy)
    _write_yaml(DEFAULT_REOPEN_CRITERIA_PATH, reopen)
    _write_markdown(DEFAULT_REOPEN_CRITERIA_DOC_PATH, _render_reopen_criteria(reopen))

    pit_gap = _pit_gap_roadmap_payload()
    _write_yaml(DEFAULT_PIT_GAP_ROADMAP_PATH, pit_gap)
    _write_markdown(DEFAULT_PIT_GAP_ROADMAP_DOC_PATH, _render_pit_gap_roadmap(pit_gap))

    _write_markdown(
        DEFAULT_FORWARD_MINIMAL_PLAN_DOC_PATH,
        _render_forward_minimal_plan(forward_plan),
    )
    _write_markdown(
        DEFAULT_OWNER_BRIEF_PATH,
        _render_owner_brief(master=master, reopen=reopen, pit_gap=pit_gap),
    )

    master["artifact_paths"] = {
        "master_closeout_yaml": str(DEFAULT_MASTER_CLOSEOUT_PATH),
        "master_closeout_doc": str(DEFAULT_MASTER_CLOSEOUT_DOC_PATH),
        "status_matrix_yaml": str(DEFAULT_STATUS_MATRIX_PATH),
        "evidence_labeling_yaml": str(DEFAULT_EVIDENCE_LABELING_PATH),
        "evidence_labeling_doc": str(DEFAULT_EVIDENCE_LABELING_DOC_PATH),
        "reopen_criteria_yaml": str(DEFAULT_REOPEN_CRITERIA_PATH),
        "reopen_criteria_doc": str(DEFAULT_REOPEN_CRITERIA_DOC_PATH),
        "pit_gap_roadmap_yaml": str(DEFAULT_PIT_GAP_ROADMAP_PATH),
        "pit_gap_roadmap_doc": str(DEFAULT_PIT_GAP_ROADMAP_DOC_PATH),
        "forward_minimal_plan_config": str(forward_minimal_plan_path),
        "forward_minimal_plan_doc": str(DEFAULT_FORWARD_MINIMAL_PLAN_DOC_PATH),
        "owner_brief": str(DEFAULT_OWNER_BRIEF_PATH),
    }
    _write_yaml(DEFAULT_MASTER_CLOSEOUT_PATH, master)
    return master


def _validate_archive_policy(
    archive_policy: Mapping[str, Any],
    forward_plan: Mapping[str, Any],
) -> None:
    policy = _mapping(archive_policy.get("first_layer_channel_archive_policy"))
    reopen = _mapping(policy.get("reopen"))
    if not reopen.get("owner_approval_required"):
        raise ValueError("first-layer channel reopen must require owner approval")
    if not reopen.get("new_pit_feature_or_forward_evidence_required"):
        raise ValueError("reopen must require new PIT feature or forward evidence")
    plan = _mapping(forward_plan.get("first_layer_forward_diagnostic_minimal_plan"))
    if bool(plan.get("enabled")):
        raise ValueError("minimal forward diagnostic plan must remain disabled")
    blocked = set(_records(plan.get("blocked_outputs")))
    required_blocked = {"weights", "trade_advice", "target_allocation", "broker_action"}
    if not required_blocked <= blocked:
        raise ValueError("minimal forward diagnostic plan missing blocked outputs")


def _status_rows(
    *,
    archive_policy: Mapping[str, Any],
    do_not: Mapping[str, Any],
    risk_veto: Mapping[str, Any],
    tradeoff: Mapping[str, Any],
    compatibility: Mapping[str, Any],
    indicator: Mapping[str, Any],
    return_seeking: Mapping[str, Any],
    defensive: Mapping[str, Any],
) -> list[dict[str, Any]]:
    archived = _mapping(
        _mapping(archive_policy.get("first_layer_channel_archive_policy")).get(
            "archived_channels"
        )
    )
    do_not_summary = _mapping(do_not.get("summary"))
    trade_summary = _mapping(tradeoff.get("summary"))
    compatibility_summary = _mapping(compatibility.get("summary"))
    indicator_summary = _mapping(indicator.get("summary"))
    return_summary = _mapping(return_seeking.get("summary"))
    defensive_summary = _mapping(defensive.get("summary"))
    return [
        _channel_row(
            "do_not_de_risk_v3",
            archived,
            blockers=[
                "false_risk_off_reduction_failed",
                "missed_upside_reduction_failed",
                "defensive_probe_regression",
                "2022_slice_not_worse_failed",
            ],
            reopen_conditions=["new_defensive_feature_family", "forward_evidence"],
            evidence_artifacts=[str(DEFAULT_DO_NOT_ARCHIVE_PATH)],
            extras={
                "false_risk_off_reduction": do_not_summary.get("false_risk_off_reduction"),
                "defensive_probe_regression_count": do_not_summary.get(
                    "defensive_probe_regression_count"
                ),
                "candidate_allowed": False,
            },
        ),
        _channel_row(
            "risk_on_veto_v3",
            archived,
            blockers=[
                "net_veto_benefit_total_negative",
                "veto_too_strict_for_return_seeking_diagnostic",
                "no_allocation_candidate",
            ],
            reopen_conditions=["new_veto_family", "positive_forward_net_veto_benefit"],
            evidence_artifacts=[
                str(DEFAULT_RISK_VETO_DIAGNOSTIC_PATH),
                str(DEFAULT_RISK_VETO_TRADEOFF_PATH),
                str(DEFAULT_RISK_VETO_COMPATIBILITY_PATH),
            ],
            extras={
                "net_veto_benefit_total": trade_summary.get("net_veto_benefit_total"),
                "compatibility_status": compatibility_summary.get("compatibility_status"),
                "forward_watch_candidate": False,
                "can_emit_weights": False,
            },
        ),
        _channel_row(
            "add_risk",
            archived,
            blockers=[
                "add_risk_selected_family_none",
                "false_add_risk_risk",
                "defensive_regression_risk",
                "tqqq_beta_dependency_risk",
            ],
            reopen_conditions=["new_add_risk_family", "same_risk_actual_path_pass"],
            evidence_artifacts=[str(DEFAULT_INDICATOR_SELECTION_PATH)],
            extras={
                "selected_families": indicator_summary.get("add_risk_selected_families", []),
                "selected_family_count": len(
                    _records(indicator_summary.get("add_risk_selected_families"))
                ),
                "candidate_allowed": False,
            },
        ),
        _channel_row(
            "return_seeking_diagnostic",
            archived,
            blockers=[
                "drawdown_regression",
                "beta_dependency",
                "tqqq_dependency",
                "2023_plus_dependency",
            ],
            reopen_conditions=["owner_approved_forward_log", "no_beta_only_forward_evidence"],
            evidence_artifacts=[str(DEFAULT_RETURN_SEEKING_FINAL_PATH)],
            extras={
                "diagnostic_value_probe_count": return_summary.get(
                    "diagnostic_value_probe_count"
                ),
                "drawdown_regression_probe_count": return_summary.get(
                    "drawdown_regression_probe_count"
                ),
                "depends_on_2023_plus": return_summary.get("depends_on_2023_plus"),
            },
        ),
        {
            "channel_name": "defensive_channel",
            "status": "CLOSED_NO_MATERIAL_IMPROVEMENT",
            "verdict": "DEFENSIVE_USAGE_BLOCKED",
            "allowed_usage": ["historical_research_evidence"],
            "blocked_usage": [
                "defensive_overlay_input",
                "owner_review",
                "paper_shadow",
                "promotion",
                "production",
                "broker",
            ],
            "blockers": [
                "defensive_lane_no_material_improvement",
                "do_not_de_risk_v3_failed",
            ],
            "reopen_conditions": ["new_defensive_pit_feature", "owner_reopen_approval"],
            "evidence_artifacts": [
                str(DEFAULT_DEFENSIVE_FINAL_PATH),
                str(DEFAULT_DO_NOT_ARCHIVE_PATH),
            ],
            "defensive_lane_status": defensive.get("status"),
            "false_risk_off_cost_declined": defensive_summary.get(
                "false_risk_off_cost_declined"
            ),
            "candidate_allowed": False,
        },
        {
            "channel_name": "risk_veto_channel",
            "status": "HISTORICAL_DIAGNOSTIC_ARCHIVE",
            "verdict": "RISK_VETO_TOO_STRICT_CURRENT_EVIDENCE",
            "allowed_usage": ["historical_diagnostic_evidence", "veto_metric_reference"],
            "blocked_usage": [
                "growth_overlay_gate",
                "forward_watch_candidate",
                "owner_review",
                "paper_shadow",
                "promotion",
                "production",
                "broker",
            ],
            "blockers": [
                "risk_on_veto_v3_net_benefit_negative",
                "return_seeking_compatibility_failed",
            ],
            "reopen_conditions": ["new_veto_family", "positive_forward_veto_evidence"],
            "evidence_artifacts": [str(DEFAULT_RISK_VETO_TRADEOFF_PATH)],
            "net_veto_benefit_total": trade_summary.get("net_veto_benefit_total"),
            "candidate_allowed": False,
        },
        _pit_blocked_row("breadth_participation"),
        _pit_blocked_row("event_risk"),
    ]


def _channel_row(
    name: str,
    archived: Mapping[str, Any],
    *,
    blockers: Sequence[str],
    reopen_conditions: Sequence[str],
    evidence_artifacts: Sequence[str],
    extras: Mapping[str, Any],
) -> dict[str, Any]:
    policy = _mapping(archived.get(name))
    row = {
        "channel_name": name,
        "status": str(policy.get("status")),
        "verdict": str(policy.get("verdict")),
        "allowed_usage": _records(policy.get("allowed_usage")),
        "blocked_usage": _records(policy.get("blocked_usage")),
        "blockers": list(blockers),
        "reopen_conditions": list(reopen_conditions),
        "evidence_artifacts": list(evidence_artifacts),
    }
    row.update(dict(extras))
    return row


def _pit_blocked_row(family_name: str) -> dict[str, Any]:
    return {
        "channel_name": family_name,
        "status": "PIT_BLOCKED",
        "verdict": "CANNOT_ENTER_MODEL_WITHOUT_PIT_APPROVED_SOURCE",
        "allowed_usage": ["data_gap_roadmap"],
        "blocked_usage": [
            "first_layer_model",
            "channel_feature_set",
            "allocation",
            "owner_review",
            "paper_shadow",
            "promotion",
            "production",
            "broker",
        ],
        "blockers": ["missing_pit_approved_source", "coverage_audit_not_passed"],
        "reopen_conditions": ["pit_approved_source", "primary_window_coverage"],
        "evidence_artifacts": [str(DEFAULT_INDICATOR_SELECTION_PATH)],
        "candidate_allowed": False,
        "model_entry_allowed": False,
    }


def _master_closeout_payload(
    *,
    status_rows: Sequence[Mapping[str, Any]],
    do_not: Mapping[str, Any],
    risk_veto: Mapping[str, Any],
    tradeoff: Mapping[str, Any],
    compatibility: Mapping[str, Any],
    indicator: Mapping[str, Any],
    return_seeking: Mapping[str, Any],
    channel_v3: Mapping[str, Any],
) -> dict[str, Any]:
    risk_summary = _mapping(risk_veto.get("summary"))
    trade_summary = _mapping(tradeoff.get("summary"))
    indicator_summary = _mapping(indicator.get("summary"))
    return_summary = _mapping(return_seeking.get("summary"))
    channel_summary = _mapping(channel_v3.get("summary"))
    summary = {
        "closeout_status": "FIRST_LAYER_CHANNEL_RESEARCH_CLOSED_NO_CANDIDATE",
        "channel_v3_final_status": channel_summary.get("final_status"),
        "do_not_de_risk_archive_status": do_not.get("status"),
        "risk_on_veto_diagnostic_status": risk_veto.get("status"),
        "risk_on_veto_net_veto_benefit_total": trade_summary.get("net_veto_benefit_total"),
        "risk_on_veto_compatibility": _mapping(compatibility.get("summary")).get(
            "compatibility_status"
        ),
        "add_risk_selected_families": indicator_summary.get("add_risk_selected_families", []),
        "return_seeking_diagnostic_value_probe_count": return_summary.get(
            "diagnostic_value_probe_count"
        ),
        "return_seeking_depends_on_2023_plus": return_summary.get("depends_on_2023_plus"),
        "pit_blocked_families": ["breadth_participation", "event_risk"],
        "data_quality_status": risk_summary.get("data_quality_status"),
        "channel_row_count": len(status_rows),
        "candidate_count": 0,
        "owner_review_allowed": False,
        "forward_watch_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "next_action": "WAIT_FOR_NEW_PIT_DATA_OR_FORWARD_EVIDENCE_AND_OWNER_REOPEN",
    }
    return _payload(
        report_type="first_layer_channel_master_closeout",
        title="First-Layer Channel Master Closeout",
        status="FIRST_LAYER_CHANNEL_RESEARCH_CLOSED_NO_CANDIDATE",
        summary=summary,
        rows=status_rows,
    )


def _evidence_labeling_payload() -> dict[str, Any]:
    labels = [
        "ARCHIVED_RESEARCH_EVIDENCE",
        "HISTORICAL_DIAGNOSTIC_ONLY",
        "NOT_OWNER_REVIEWABLE",
        "NOT_PROMOTION_EVIDENCE",
        "NO_ALLOCATION_OUTPUT",
    ]
    rows = [
        {
            "artifact": "inputs/research_reviews/do_not_de_risk_v3_archive.yaml",
            "labels": labels,
            "interpretation": "do_not_de_risk v3 failure evidence only",
        },
        {
            "artifact": "inputs/research_reviews/risk_on_veto_observe_only_diagnostic.yaml",
            "labels": labels,
            "interpretation": "risk_on_veto v3 historical diagnostic only",
        },
        {
            "artifact": "inputs/research_reviews/indicator_family_selection_matrix.yaml",
            "labels": labels,
            "interpretation": "family eligibility/rejection evidence only",
        },
        {
            "artifact": "inputs/research_reviews/return_seeking_diagnostic_lane_final_matrix.yaml",
            "labels": labels,
            "interpretation": "return-seeking historical diagnostic with blockers",
        },
    ]
    return _payload(
        report_type="first_layer_diagnostic_evidence_labeling",
        title="First-Layer Diagnostic Evidence Labeling",
        status="FIRST_LAYER_DIAGNOSTIC_EVIDENCE_LABELED_ARCHIVED",
        summary={
            "labeled_artifact_count": len(rows),
            "labels": labels,
            "owner_reviewable": False,
            "promotion_evidence": False,
            "allocation_output": False,
        },
        rows=rows,
    )


def _reopen_criteria_payload(archive_policy: Mapping[str, Any]) -> dict[str, Any]:
    policy = _mapping(archive_policy.get("first_layer_channel_archive_policy"))
    reopen = _mapping(policy.get("reopen"))
    rows = [
        {
            "criteria_group": "data_or_feature",
            "allowed_reopen_trigger": [
                "PIT-approved breadth / participation data",
                "PIT-approved event risk / macro shock data",
                "robust relative strength / sector participation data",
                "forward diagnostic evidence proving future-sample signal usefulness",
                "new feature family passes primary window and 2022 slice",
            ],
        },
        {
            "criteria_group": "model_or_strategy",
            "required_conditions": [
                "selection rule pre-registered",
                "primary window used; 2022-12 legacy cannot be primary",
                "same-risk static frontier comparison passed",
                "not TQQQ / beta-only",
                "not 2023+ only",
                "defensive_probe_regression_count=0 or owner-accepted caveat",
                "net-of-cost still effective",
                "owner manual reopen approval",
            ],
        },
        {
            "criteria_group": "prohibited_reopen_evidence",
            "blocked_conditions": [
                "single historical rerun improvement",
                "2023+ only improvement",
                "higher TQQQ or QQQ-equivalent exposure explains result",
                "target-path improves but actual-path does not",
                "missing research_audit_metadata",
                "PIT coverage audit not passed",
                "diagnostic-only signal emits weights",
            ],
        },
    ]
    summary = {
        "reopen_status": "NOT_ALLOWED_CURRENT_EVIDENCE",
        "owner_approval_required": reopen.get("owner_approval_required"),
        "historical_rerun_alone_insufficient": reopen.get(
            "historical_rerun_alone_insufficient"
        ),
        "new_pit_feature_or_forward_evidence_required": reopen.get(
            "new_pit_feature_or_forward_evidence_required"
        ),
        "2023_plus_only_result_can_reopen": False,
        "target_path_only_result_can_reopen": False,
        "diagnostic_only_signal_can_emit_weights": False,
    }
    return _payload(
        report_type="first_layer_channel_reopen_criteria",
        title="First-Layer Channel Reopen Criteria",
        status="FIRST_LAYER_CHANNEL_REOPEN_CRITERIA_READY_OWNER_REQUIRED",
        summary=summary,
        rows=rows,
    )


def _pit_gap_roadmap_payload() -> dict[str, Any]:
    rows = [
        {
            "family_name": "breadth_participation",
            "why_it_matters": "distinguish broad trend from mega-cap or beta-only move",
            "current_status": "PIT_BLOCKED",
            "needed_source": "point-in-time constituent breadth / participation history",
            "minimum_required_fields": [
                "date",
                "universe_id",
                "advance_decline_or_participation_measure",
                "known_at",
                "source_timestamp",
            ],
            "expected_usage": ["risk_on_veto", "stay_constructive", "add_risk_diagnostic"],
            "blocked_until": "PIT-approved local source and coverage audit pass",
        },
        {
            "family_name": "event_risk",
            "why_it_matters": "filter false add-risk and risk-off veto around known shocks",
            "current_status": "PIT_BLOCKED",
            "needed_source": "timestamped event risk / macro shock feed",
            "minimum_required_fields": [
                "event_id",
                "event_type",
                "known_at",
                "affected_assets",
                "severity",
            ],
            "expected_usage": ["risk_on_veto", "risk_off"],
            "blocked_until": "event timestamp availability and PIT audit pass",
        },
        {
            "family_name": "sector_participation",
            "why_it_matters": "separate AI/tech breadth from narrow QQQ concentration",
            "current_status": "PIT_BLOCKED",
            "needed_source": "point-in-time sector participation and relative breadth source",
            "minimum_required_fields": ["date", "sector_id", "participation_score", "known_at"],
            "expected_usage": ["stay_constructive", "add_risk_diagnostic"],
            "blocked_until": "PIT source qualification and primary-window coverage pass",
        },
        {
            "family_name": "macro_shock",
            "why_it_matters": "improve risk-off veto and false add-risk filtering",
            "current_status": "PIT_BLOCKED",
            "needed_source": "timestamped macro shock calendar / surprise series",
            "minimum_required_fields": [
                "date",
                "release_time",
                "series_id",
                "surprise",
                "known_at",
            ],
            "expected_usage": ["risk_on_veto", "risk_off"],
            "blocked_until": "PIT release timestamp and schema validation pass",
        },
        {
            "family_name": "earnings_revision",
            "why_it_matters": "future available revisions may help separate durable AI trend",
            "current_status": "PIT_BLOCKED",
            "needed_source": "point-in-time analyst revision or earnings estimate history",
            "minimum_required_fields": ["date", "ticker", "revision_metric", "known_at"],
            "expected_usage": ["stay_constructive", "add_risk_diagnostic"],
            "blocked_until": "PIT history and issuer coverage audit pass",
        },
    ]
    return _payload(
        report_type="first_layer_pit_data_gap_roadmap",
        title="First-Layer PIT Data Gap Roadmap",
        status="FIRST_LAYER_PIT_DATA_GAP_ROADMAP_READY",
        summary={
            "gap_count": len(rows),
            "high_priority_families": ["breadth_participation", "event_risk"],
            "all_current_status": "PIT_BLOCKED",
            "model_entry_allowed_before_pit_approval": False,
        },
        rows=rows,
    )


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "research_window_id": PRIMARY_WINDOW_ID,
        "research_window_alias": PRIMARY_WINDOW_ALIAS,
        "requested_start": REQUESTED_START,
        "actual_start": REQUESTED_START,
        "actual_portfolio_start": REQUESTED_START,
        "end": "latest",
        "window_role": "primary_validated",
        "data_quality_contract": "inherited_from_tracked_research_artifacts",
        "exact_or_proxy": "exact",
        "summary": _clean_for_yaml(dict(summary)),
        "research_audit_metadata": _audit_metadata(),
        **SAFETY_BOUNDARY,
    }
    if rows is not None:
        payload["rows"] = _clean_for_yaml(list(rows))
    return payload


def _audit_metadata() -> dict[str, Any]:
    return {
        "modified_layer": "validation_only",
        "modified_channel": "first_layer_channel_closeout",
        "frozen_channels": ["defensive", "return_seeking_diagnostic", "risk_veto"],
        "frozen_first_layer_version": "first_layer_channel_archive_policy_v1",
        "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
        "research_window_id": PRIMARY_WINDOW_ID,
        "label_version": "channel_specific_labels_v3",
        "feature_set_version": "channel_specific_feature_set_v1_locked",
        "model_version": "first_layer_channel_closeout_reopen_criteria_v1",
        "threshold_policy": "first_layer_channel_archive_policy_v1",
        "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
        "signal_usage_matrix_version": "first_layer_signal_usage_matrix_v2",
        "boundary_contract_version": "two_layer_strategy_boundary_contract_v1",
        "selection_rule_version": "first_layer_channel_reopen_criteria_v1",
        "candidate_count": 0,
        "pre_registered_selection_rule": "first_layer_channel_reopen_criteria_v1",
    }


def _render_master_closeout(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "# First-Layer Channel Master Closeout",
            "",
            f"状态：`{payload.get('status')}`",
            "",
            "当前 first-layer channel 研究正式收口：没有可进入策略候选、owner review、"
            "forward watch、paper-shadow、promotion、production 或 broker 的 channel。",
            "",
            "## 关键结论",
            "",
            f"- do_not_de_risk archive: `{summary.get('do_not_de_risk_archive_status')}`",
            "- risk_on_veto net_veto_benefit_total: "
            f"`{summary.get('risk_on_veto_net_veto_benefit_total')}`",
            f"- risk_on_veto compatibility: `{summary.get('risk_on_veto_compatibility')}`",
            f"- add_risk_selected_families: `{summary.get('add_risk_selected_families')}`",
            "- return_seeking_diagnostic_value_probe_count: "
            f"`{summary.get('return_seeking_diagnostic_value_probe_count')}`",
            f"- pit_blocked_families: `{summary.get('pit_blocked_families')}`",
            "",
            "后续只能在获得新 PIT 数据、新 feature family 或 forward diagnostic evidence，"
            "并经 owner 手动批准后重开。",
            "",
        ]
    )


def _render_evidence_labeling(payload: Mapping[str, Any]) -> str:
    lines = [
        "# First-Layer Diagnostic Evidence Labeling Review",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        "相关 artifact 统一标记为 historical / archived diagnostic evidence，"
        "不是 owner-review 或 promotion evidence。",
        "",
    ]
    for row in _records(payload.get("rows")):
        lines.append(f"- `{row.get('artifact')}`: `{', '.join(row.get('labels', []))}`")
    lines.append("")
    return "\n".join(lines)


def _render_reopen_criteria(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# First-Layer Channel Reopen Criteria",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        f"- owner_approval_required: `{summary.get('owner_approval_required')}`",
        "- historical_rerun_alone_insufficient: "
        f"`{summary.get('historical_rerun_alone_insufficient')}`",
        "- new_pit_feature_or_forward_evidence_required: "
        f"`{summary.get('new_pit_feature_or_forward_evidence_required')}`",
        "",
        "## 禁止重开",
        "",
        "单次历史 rerun 变好、2023+ only、TQQQ/beta-only、target-path only、"
        "缺 research_audit_metadata、未过 PIT audit 或 diagnostic-only signal 输出 weights，"
        "均不能触发 reopen。",
        "",
    ]
    return "\n".join(lines)


def _render_pit_gap_roadmap(payload: Mapping[str, Any]) -> str:
    lines = [
        "# First-Layer PIT Data Gap Roadmap",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        "高优先级 gap 是 `breadth_participation` 与 `event_risk`。这些 family 不能在 "
        "PIT-approved source 和 coverage audit 通过前进入模型。",
        "",
    ]
    for row in _records(payload.get("rows")):
        lines.append(
            f"- `{row.get('family_name')}`: `{row.get('current_status')}`，"
            f"blocked_until=`{row.get('blocked_until')}`"
        )
    lines.append("")
    return "\n".join(lines)


def _render_forward_minimal_plan(plan: Mapping[str, Any]) -> str:
    body = _mapping(plan.get("first_layer_forward_diagnostic_minimal_plan"))
    return "\n".join(
        [
            "# First-Layer Forward Diagnostic Minimal Plan",
            "",
            f"状态：`{plan.get('status')}`",
            "",
            f"- enabled: `{body.get('enabled')}`",
            f"- requires_owner_approval: `{body.get('requires_owner_approval')}`",
            f"- allowed_outputs: `{body.get('allowed_outputs')}`",
            f"- blocked_outputs: `{body.get('blocked_outputs')}`",
            "",
            "当前不启动正式 forward watch；该文件只是 future-ready disabled plan。",
            "",
        ]
    )


def _render_owner_brief(
    *,
    master: Mapping[str, Any],
    reopen: Mapping[str, Any],
    pit_gap: Mapping[str, Any],
) -> str:
    summary = _mapping(master.get("summary"))
    reopen_summary = _mapping(reopen.get("summary"))
    pit_summary = _mapping(pit_gap.get("summary"))
    risk_net_benefit = summary.get("risk_on_veto_net_veto_benefit_total")
    risk_compatibility = summary.get("risk_on_veto_compatibility")
    owner_approval_required = reopen_summary.get("owner_approval_required")
    return "\n".join(
        [
            "# First-Layer Channel Closeout Owner Brief",
            "",
            "## 1. 当前 first-layer channel 研究为什么收口？",
            "",
            "当前没有任何 channel 形成可用策略候选。`do_not_de_risk v3` 失败，"
            "`risk_on_veto v3` net benefit 为负，add-risk 没有 selected family，"
            "return-seeking diagnostic 仍受 drawdown/beta/TQQQ/2023+ blocker 限制。",
            "",
            "## 2. do_not_de_risk 为什么归档？",
            "",
            "`do_not_de_risk v3` 未通过 false risk-off、missed upside、defensive regression "
            "和 2022 slice not worse gate。",
            "",
            "## 3. risk_on_veto 为什么不进入 forward diagnostic？",
            "",
            f"`risk_on_veto` net_veto_benefit_total=`{risk_net_benefit}`，"
            f"compatibility=`{risk_compatibility}`，因此本轮不进入正式 forward watch。",
            "",
            "## 4. add-risk 为什么不支持？",
            "",
            f"add-risk selected families 为 `{summary.get('add_risk_selected_families')}`，"
            "没有 family 通过 allocation / growth overlay gate。",
            "",
            "## 5. 哪些信号仍保留为历史诊断？",
            "",
            "`risk_on_veto`、return-seeking diagnostic、indicator family selection 和 "
            "`do_not_de_risk` failure attribution 均只保留为 historical diagnostic evidence。",
            "",
            "## 6. 后续要补哪些数据才能重开？",
            "",
            f"高优先级 PIT data gaps：`{pit_summary.get('high_priority_families')}`。"
            "还可以由新 feature family 或可信 forward diagnostic evidence 触发 owner 复核。",
            "",
            "## 7. 为什么 promotion / paper-shadow / broker 仍 blocked？",
            "",
            f"reopen_status=`{reopen_summary.get('reopen_status')}`，"
            f"owner_approval_required=`{owner_approval_required}`。"
            "当前没有候选、没有 owner approval、没有通过 same-risk / PIT / "
            "actual-path gate 的新证据。",
            "",
        ]
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_clean_for_yaml(dict(payload)), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _clean_for_yaml(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _clean_for_yaml(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clean_for_yaml(item) for item in value]
    if isinstance(value, tuple):
        return [_clean_for_yaml(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        return _clean_for_yaml(value.item())
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return 0.0
    return value
