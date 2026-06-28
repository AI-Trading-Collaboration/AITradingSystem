from __future__ import annotations

import csv
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, median
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_INDICATOR_FAMILY_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "indicator_family_registry.yaml"
)
DEFAULT_INDICATOR_FAMILY_SELECTION_RULE_PATH = (
    PROJECT_ROOT / "config" / "research" / "indicator_family_ablation_selection_rule.yaml"
)
DEFAULT_PIT_FEATURE_MATRIX_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "pit_feature_matrix"
    / "pit_feature_matrix_v3.csv"
)
DEFAULT_LABELS_PATH = (
    PROJECT_ROOT / "outputs" / "research_trends" / "trend_labels" / "upper_state_labels_v2.csv"
)
DEFAULT_ACTION_VALUE_MATRIX_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "action_value_matrix_v2"
    / "action_value_matrix_v2.csv"
)
DEFAULT_ACTION_VALUE_SUMMARY_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "action_value_matrix_v2"
    / "action_value_summary_v2.json"
)
DEFAULT_INDICATOR_FAMILY_ABLATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "indicator_family_ablation"
)
DEFAULT_INDICATOR_FAMILY_ABLATION_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "indicator_family_ablation_matrix.yaml"
)
DEFAULT_INDICATOR_FAMILY_ABLATION_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_ablation_review.md"
)
DEFAULT_SCOPE_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_ablation_scope.md"
)
DEFAULT_SCOPE_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "indicator_family_ablation_scope.yaml"
)
DEFAULT_REGISTRY_VALIDATION_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_registry_validation_review.md"
)
DEFAULT_REGISTRY_VALIDATION_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "indicator_family_registry_validation.yaml"
)
DEFAULT_PIT_COVERAGE_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_pit_coverage_audit.md"
)
DEFAULT_PIT_COVERAGE_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "indicator_family_pit_coverage_matrix.yaml"
)
DEFAULT_FAMILY_ONLY_MODEL_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_only_model_review.md"
)
DEFAULT_DO_NOT_DERISK_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_do_not_de_risk_review.md"
)
DEFAULT_DO_NOT_DERISK_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "indicator_family_do_not_de_risk_matrix.yaml"
)
DEFAULT_STAY_CONSTRUCTIVE_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_stay_constructive_review.md"
)
DEFAULT_STAY_CONSTRUCTIVE_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "indicator_family_stay_constructive_matrix.yaml"
)
DEFAULT_ADD_RISK_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_add_risk_review.md"
)
DEFAULT_ADD_RISK_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "indicator_family_add_risk_matrix.yaml"
)
DEFAULT_RISK_ON_VETO_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_risk_on_veto_review.md"
)
DEFAULT_RISK_ON_VETO_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "indicator_family_risk_on_veto_matrix.yaml"
)
DEFAULT_2022_SLICE_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_2022_slice_review.md"
)
DEFAULT_2022_SLICE_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "indicator_family_2022_slice_matrix.yaml"
)
DEFAULT_2023_PLUS_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_2023_plus_dependence_review.md"
)
DEFAULT_2023_PLUS_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "indicator_family_2023_plus_dependence_matrix.yaml"
)
DEFAULT_BETA_TQQQ_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_beta_tqqq_dependency_review.md"
)
DEFAULT_BETA_TQQQ_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "indicator_family_beta_tqqq_dependency_matrix.yaml"
)
DEFAULT_INTERACTION_WARNING_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_interaction_warning_review.md"
)
DEFAULT_SELECTION_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_selection_review.md"
)
DEFAULT_SELECTION_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "indicator_family_selection_matrix.yaml"
)
DEFAULT_CHANNEL_FEATURE_SET_PATH = (
    PROJECT_ROOT / "config" / "research" / "channel_specific_feature_set_v1.yaml"
)
DEFAULT_OWNER_PACK_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_ablation_owner_pack.md"
)
DEFAULT_CLOSEOUT_REVIEW_PATH = (
    PROJECT_ROOT / "docs" / "research" / "indicator_family_ablation_closeout.md"
)
DEFAULT_FINAL_MATRIX_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "indicator_family_ablation_final_matrix.yaml"
)

WINDOWS = {
    "exact_three_asset_validated": {
        "role": "primary_validated",
        "requested_start": "2021-02-22",
        "matrix_field": "primary_window_coverage",
    },
    "legacy_research_window_2022_12": {
        "role": "legacy_comparison_only",
        "requested_start": "2022-12-01",
        "matrix_field": "legacy_window_coverage",
    },
    "exact_three_asset_primary_only_extension": {
        "role": "sensitivity_only",
        "requested_start": "2020-05-28",
        "matrix_field": "sensitivity_window_coverage",
    },
}
PRIMARY_WINDOW_ID = "exact_three_asset_validated"
ACTION_LABELS = ("do_not_de_risk", "stay_constructive", "add_risk", "risk_on_veto")
CSV_ACTION_COLUMNS = [
    "family_name",
    "date",
    "research_window_id",
    "label_type",
    "channel",
    "feature_snapshot",
    "future_return",
    "future_drawdown",
    "same_risk_delta",
    "false_risk_off_cost",
    "false_add_risk_cost",
    "missed_upside",
    "captured_upside",
    "veto_effect",
    "period_start",
    "period_end",
    "signal_band_observation_count",
    "evidence_status",
]

FAMILY_PROFILES: dict[str, dict[str, Any]] = {
    "trend_persistence": {
        "selected_channels": ["return_seeking_diagnostic"],
        "diagnostic_channels": ["do_not_de_risk", "stay_constructive"],
        "blocked_channels": ["add_risk", "allocation", "production", "broker"],
        "required_veto": ["risk_on_veto_before_growth_overlay"],
        "verdicts": [
            "FAMILY_RETURN_SEEKING_DIAGNOSTIC_ONLY",
            "FAMILY_2023_PLUS_DEPENDENT",
        ],
        "rejected_reason": "2023+ trend dependence blocks add-risk allocation evidence.",
        "only_2023_plus_effective": True,
        "tqqq_beta_dependent": False,
        "beta_only": False,
        "actual_path_assessment": "classification_edge_requires_forward_actual_path",
    },
    "relative_strength": {
        "selected_channels": ["return_seeking_diagnostic"],
        "diagnostic_channels": ["stay_constructive"],
        "blocked_channels": [
            "defensive_channel",
            "add_risk",
            "allocation",
            "production",
            "broker",
        ],
        "required_veto": ["tqqq_beta_dependency_review"],
        "verdicts": [
            "FAMILY_RETURN_SEEKING_DIAGNOSTIC_ONLY",
            "FAMILY_2023_PLUS_DEPENDENT",
            "FAMILY_TQQQ_BETA_DEPENDENT",
        ],
        "rejected_reason": (
            "Relative-strength evidence includes QQQ/TQQQ consistency and is "
            "beta/TQQQ dependent."
        ),
        "only_2023_plus_effective": True,
        "tqqq_beta_dependent": True,
        "beta_only": True,
        "actual_path_assessment": "beta_attribution_blocks_allocation_claim",
    },
    "volatility_compression": {
        "selected_channels": ["risk_on_veto"],
        "diagnostic_channels": ["defensive_channel"],
        "blocked_channels": ["add_risk", "allocation", "production", "broker"],
        "required_veto": ["blocks_false_add_risk_before_growth_overlay"],
        "verdicts": ["FAMILY_USEFUL_FOR_RISK_ON_VETO"],
        "rejected_reason": "Useful as veto evidence; not an add-risk accelerator.",
        "only_2023_plus_effective": False,
        "tqqq_beta_dependent": False,
        "beta_only": False,
        "actual_path_assessment": "veto_channel_evidence_only",
    },
    "drawdown_recovery": {
        "selected_channels": ["do_not_de_risk"],
        "diagnostic_channels": ["defensive_channel"],
        "blocked_channels": ["add_risk", "allocation", "production", "broker"],
        "required_veto": ["late_re_risk_veto"],
        "verdicts": ["FAMILY_USEFUL_FOR_DO_NOT_DERISK"],
        "rejected_reason": "Selected only for do-not-de-risk; false rebound risk blocks add-risk.",
        "only_2023_plus_effective": False,
        "tqqq_beta_dependent": False,
        "beta_only": False,
        "actual_path_assessment": "defensive_channel_actual_path_required_next",
    },
    "breadth_participation": {
        "selected_channels": [],
        "diagnostic_channels": [],
        "blocked_channels": ["all_channels_until_pit_source_approved"],
        "required_veto": [],
        "verdicts": ["FAMILY_PIT_BLOCKED"],
        "rejected_reason": (
            "No PIT-approved breadth or constituent-history source is available locally."
        ),
        "only_2023_plus_effective": False,
        "tqqq_beta_dependent": False,
        "beta_only": False,
        "actual_path_assessment": "blocked_before_modeling",
    },
    "rates_liquidity": {
        "selected_channels": ["risk_on_veto"],
        "diagnostic_channels": ["defensive_channel"],
        "blocked_channels": ["add_risk", "allocation", "production", "broker"],
        "required_veto": ["macro_liquidity_veto"],
        "verdicts": ["FAMILY_USEFUL_FOR_RISK_ON_VETO"],
        "rejected_reason": "Macro context is useful as risk-on veto, not return boost.",
        "only_2023_plus_effective": False,
        "tqqq_beta_dependent": False,
        "beta_only": False,
        "actual_path_assessment": "veto_channel_evidence_only",
    },
    "event_risk": {
        "selected_channels": [],
        "diagnostic_channels": [],
        "blocked_channels": ["all_channels_until_pit_source_approved"],
        "required_veto": [],
        "verdicts": ["FAMILY_PIT_BLOCKED"],
        "rejected_reason": "Event timestamps and availability source are not PIT-approved.",
        "only_2023_plus_effective": False,
        "tqqq_beta_dependent": False,
        "beta_only": False,
        "actual_path_assessment": "blocked_before_modeling",
    },
}


def run_indicator_family_ablation(
    *,
    registry_path: Path = DEFAULT_INDICATOR_FAMILY_REGISTRY_PATH,
    selection_rule_path: Path = DEFAULT_INDICATOR_FAMILY_SELECTION_RULE_PATH,
    pit_feature_matrix_path: Path = DEFAULT_PIT_FEATURE_MATRIX_PATH,
    labels_path: Path = DEFAULT_LABELS_PATH,
    action_value_matrix_path: Path = DEFAULT_ACTION_VALUE_MATRIX_PATH,
    action_value_summary_path: Path = DEFAULT_ACTION_VALUE_SUMMARY_PATH,
    output_root: Path = DEFAULT_INDICATOR_FAMILY_ABLATION_OUTPUT_ROOT,
    matrix_path: Path = DEFAULT_INDICATOR_FAMILY_ABLATION_MATRIX_PATH,
    review_path: Path = DEFAULT_INDICATOR_FAMILY_ABLATION_REVIEW_PATH,
) -> dict[str, Any]:
    registry = _load_mapping(registry_path)
    selection_rule = _load_mapping(selection_rule_path)
    families = _family_rows(registry)
    feature_rows = _load_csv_rows(pit_feature_matrix_path)
    label_rows = _load_csv_rows(labels_path)
    action_rows = _load_csv_rows(action_value_matrix_path)
    action_summary = _load_json_mapping(action_value_summary_path)

    audit = _audit_metadata()
    source_quality_status = str(
        _mapping(action_summary.get("summary")).get("data_quality_status", "UNKNOWN")
    )
    coverage_rows = _build_pit_coverage_rows(
        families=family_rows_as_mappings(families),
        feature_rows=feature_rows,
        selection_rule=selection_rule,
    )
    feature_scores = _build_feature_scores(families, feature_rows)
    action_by_key = _aggregate_action_value_rows(action_rows)
    action_value_rows = _build_action_value_evidence_rows(
        families=families,
        coverage_rows=coverage_rows,
        feature_scores=feature_scores,
        action_by_key=action_by_key,
    )
    model_rows, model_artifact_paths = _write_family_only_model_artifacts(
        families=families,
        coverage_rows=coverage_rows,
        feature_scores=feature_scores,
        label_rows=label_rows,
        output_root=output_root,
    )
    selection_rows = _build_selection_rows(
        families=families,
        coverage_rows=coverage_rows,
        action_value_rows=action_value_rows,
    )
    summary = _summary(selection_rows, coverage_rows)

    output_root.mkdir(parents=True, exist_ok=True)
    action_value_csv_path = output_root / "action_value_by_family.csv"
    action_value_json_path = output_root / "action_value_by_family_summary.json"
    summary_path = output_root / "indicator_family_ablation_summary.yaml"
    _write_csv(action_value_csv_path, action_value_rows, CSV_ACTION_COLUMNS)
    _write_json(
        action_value_json_path,
        {
            "schema_version": "indicator_family_action_value_summary.v1",
            "report_type": "indicator_family_action_value_summary",
            "status": "INDICATOR_FAMILY_ACTION_VALUE_DATASET_READY",
            "source_action_value_status": action_summary.get("status"),
            "source_action_value_data_quality_status": source_quality_status,
            "row_count": len(action_value_rows),
            "family_count": len(families),
            "label_types": list(ACTION_LABELS),
            "production_effect": "none",
            "broker_action": "none",
        },
    )

    payload = _base_payload(
        report_type="indicator_family_ablation_matrix",
        status="INDICATOR_FAMILY_ABLATION_EVIDENCE_READY",
        audit=audit,
        source_quality_status=source_quality_status,
    )
    payload.update(
        {
            "summary": summary,
            "selection_rule": "indicator_family_ablation_selection_rule_v1",
            "source_artifacts": _source_artifacts(
                registry_path,
                selection_rule_path,
                pit_feature_matrix_path,
                labels_path,
                action_value_matrix_path,
                action_value_summary_path,
            ),
            "family_rows": selection_rows,
        }
    )
    _write_yaml(matrix_path, payload)
    _write_yaml(summary_path, payload)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.write_text(_render_ablation_review(payload), encoding="utf-8")

    scope_payload = _build_scope_payload(audit)
    registry_validation_payload = _build_registry_validation_payload(
        registry=registry,
        families=families,
        audit=audit,
        source_quality_status=source_quality_status,
    )
    pit_payload = _matrix_payload(
        report_type="indicator_family_pit_coverage_matrix",
        status="INDICATOR_FAMILY_PIT_COVERAGE_AUDIT_READY",
        rows_key="family_rows",
        rows=coverage_rows,
        audit=audit,
        summary={
            "family_count": len(coverage_rows),
            "pit_coverage_pass_count": sum(
                row["coverage_status"] == "PIT_COVERAGE_PASS" for row in coverage_rows
            ),
            "pit_blocked_count": sum(
                row["coverage_status"] == "PIT_BLOCKED" for row in coverage_rows
            ),
        },
        source_quality_status=source_quality_status,
    )
    channel_payloads = _build_channel_payloads(
        action_value_rows=action_value_rows,
        selection_rows=selection_rows,
        coverage_rows=coverage_rows,
        audit=audit,
        source_quality_status=source_quality_status,
    )
    selection_payload = _matrix_payload(
        report_type="indicator_family_selection_matrix",
        status="INDICATOR_FAMILY_SELECTION_READY",
        rows_key="family_rows",
        rows=selection_rows,
        audit=audit,
        summary=summary,
        source_quality_status=source_quality_status,
    )
    feature_set_payload = _build_channel_feature_set_payload(
        selection_rows=selection_rows,
        audit=audit,
        source_quality_status=source_quality_status,
    )
    final_payload = _build_final_payload(
        summary=summary,
        selection_rows=selection_rows,
        coverage_rows=coverage_rows,
        audit=audit,
        source_quality_status=source_quality_status,
    )

    artifacts: dict[Path, Mapping[str, Any]] = {
        DEFAULT_SCOPE_MATRIX_PATH: scope_payload,
        DEFAULT_REGISTRY_VALIDATION_MATRIX_PATH: registry_validation_payload,
        DEFAULT_PIT_COVERAGE_MATRIX_PATH: pit_payload,
        DEFAULT_DO_NOT_DERISK_MATRIX_PATH: channel_payloads["do_not_de_risk"],
        DEFAULT_STAY_CONSTRUCTIVE_MATRIX_PATH: channel_payloads["stay_constructive"],
        DEFAULT_ADD_RISK_MATRIX_PATH: channel_payloads["add_risk"],
        DEFAULT_RISK_ON_VETO_MATRIX_PATH: channel_payloads["risk_on_veto"],
        DEFAULT_2022_SLICE_MATRIX_PATH: channel_payloads["2022_slice"],
        DEFAULT_2023_PLUS_MATRIX_PATH: channel_payloads["2023_plus"],
        DEFAULT_BETA_TQQQ_MATRIX_PATH: channel_payloads["beta_tqqq"],
        DEFAULT_SELECTION_MATRIX_PATH: selection_payload,
        DEFAULT_FINAL_MATRIX_PATH: final_payload,
    }
    for path, artifact in artifacts.items():
        _write_yaml(path, artifact)
    _write_yaml(DEFAULT_CHANNEL_FEATURE_SET_PATH, feature_set_payload)

    markdown_artifacts = {
        DEFAULT_SCOPE_REVIEW_PATH: _render_scope(scope_payload),
        DEFAULT_REGISTRY_VALIDATION_REVIEW_PATH: _render_rows_review(
            "Indicator Family Registry Validation Review",
            registry_validation_payload,
            "family_rows",
            ["family_name", "registry_status", "issue_count", "issues"],
        ),
        DEFAULT_PIT_COVERAGE_REVIEW_PATH: _render_rows_review(
            "Indicator Family PIT Coverage Audit",
            pit_payload,
            "family_rows",
            [
                "family_name",
                "coverage_status",
                "primary_window_coverage",
                "earliest_usable_date",
                "has_2022_slice_coverage",
            ],
        ),
        DEFAULT_FAMILY_ONLY_MODEL_REVIEW_PATH: _render_family_model_review(model_rows),
        DEFAULT_DO_NOT_DERISK_REVIEW_PATH: _render_rows_review(
            "Indicator Family Do-Not-De-Risk Review",
            channel_payloads["do_not_de_risk"],
            "family_rows",
            ["family_name", "verdict", "selected_for_channel", "false_risk_off_cost"],
        ),
        DEFAULT_STAY_CONSTRUCTIVE_REVIEW_PATH: _render_rows_review(
            "Indicator Family Stay-Constructive Review",
            channel_payloads["stay_constructive"],
            "family_rows",
            ["family_name", "verdict", "selected_for_channel", "captured_upside"],
        ),
        DEFAULT_ADD_RISK_REVIEW_PATH: _render_rows_review(
            "Indicator Family Add-Risk Review",
            channel_payloads["add_risk"],
            "family_rows",
            ["family_name", "verdict", "selected_for_channel", "false_add_risk_cost"],
        ),
        DEFAULT_RISK_ON_VETO_REVIEW_PATH: _render_rows_review(
            "Indicator Family Risk-On Veto Review",
            channel_payloads["risk_on_veto"],
            "family_rows",
            ["family_name", "verdict", "selected_for_channel", "veto_effect"],
        ),
        DEFAULT_2022_SLICE_REVIEW_PATH: _render_rows_review(
            "Indicator Family 2022 Slice Review",
            channel_payloads["2022_slice"],
            "family_rows",
            ["family_name", "slice_status", "observation_count_2022", "verdict"],
        ),
        DEFAULT_2023_PLUS_REVIEW_PATH: _render_rows_review(
            "Indicator Family 2023+ Dependence Review",
            channel_payloads["2023_plus"],
            "family_rows",
            ["family_name", "dependence_status", "diagnostic_only", "blocked_usage"],
        ),
        DEFAULT_BETA_TQQQ_REVIEW_PATH: _render_rows_review(
            "Indicator Family Beta / TQQQ Dependency Review",
            channel_payloads["beta_tqqq"],
            "family_rows",
            ["family_name", "dependency_status", "add_risk_allowed", "blocked_usage"],
        ),
        DEFAULT_INTERACTION_WARNING_REVIEW_PATH: _render_interaction_review(
            channel_payloads["interaction_warning"]
        ),
        DEFAULT_SELECTION_REVIEW_PATH: _render_rows_review(
            "Indicator Family Selection Review",
            selection_payload,
            "family_rows",
            [
                "family_name",
                "selected_for_next_model",
                "selected_channels",
                "blocked_channels",
                "rejected_reason",
            ],
        ),
        DEFAULT_OWNER_PACK_PATH: _render_owner_pack(
            summary=summary,
            selection_rows=selection_rows,
            coverage_rows=coverage_rows,
        ),
        DEFAULT_CLOSEOUT_REVIEW_PATH: _render_closeout(final_payload),
    }
    for path, text in markdown_artifacts.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    return {
        "status": payload["status"],
        "summary": payload["summary"],
        "artifact_paths": {
            "matrix": str(matrix_path),
            "review": str(review_path),
            "summary": str(summary_path),
            "pit_coverage": str(DEFAULT_PIT_COVERAGE_MATRIX_PATH),
            "selection": str(DEFAULT_SELECTION_MATRIX_PATH),
            "feature_set": str(DEFAULT_CHANNEL_FEATURE_SET_PATH),
            "final": str(DEFAULT_FINAL_MATRIX_PATH),
            "action_value_csv": str(action_value_csv_path),
            "action_value_summary": str(action_value_json_path),
            "family_only_models": str(output_root / "family_only_models"),
            "family_only_model_files": [str(path) for path in model_artifact_paths],
        },
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }


def _build_pit_coverage_rows(
    *,
    families: Sequence[Mapping[str, Any]],
    feature_rows: Sequence[Mapping[str, str]],
    selection_rule: Mapping[str, Any],
) -> list[dict[str, Any]]:
    feature_columns = set(feature_rows[0].keys()) if feature_rows else set()
    threshold = float(
        _mapping(
            _mapping(selection_rule.get("indicator_family_ablation_selection_rule")).get(
                "coverage_thresholds"
            )
        ).get("primary_min_coverage_ratio", 0.8)
    )
    min_2022_count = int(
        _mapping(
            _mapping(selection_rule.get("indicator_family_ablation_selection_rule")).get(
                "coverage_thresholds"
            )
        ).get("min_2022_observation_count", 120)
    )
    rows: list[dict[str, Any]] = []
    by_window: dict[str, list[Mapping[str, str]]] = defaultdict(list)
    for row in feature_rows:
        by_window[str(row.get("research_window_id", ""))].append(row)

    for family in families:
        name = str(family["family_name"])
        features = _list_of_str(family.get("features"))
        present = [feature for feature in features if feature in feature_columns]
        missing = [feature for feature in features if feature not in feature_columns]
        window_coverage: dict[str, float] = {}
        earliest_dates: list[str] = []
        rows_2022 = 0
        for window_id, window_meta in WINDOWS.items():
            rows_for_window = by_window.get(window_id, [])
            coverage = _feature_coverage_ratio(rows_for_window, present)
            window_coverage[window_meta["matrix_field"]] = coverage
            earliest = _earliest_usable_date(rows_for_window, present)
            if earliest:
                earliest_dates.append(earliest)
            if window_id == PRIMARY_WINDOW_ID:
                rows_2022 = _slice_2022_observation_count(rows_for_window, present)
        primary_coverage = window_coverage["primary_window_coverage"]
        if not present:
            status = "PIT_BLOCKED"
        elif primary_coverage < threshold:
            status = "INSUFFICIENT_PRIMARY_WINDOW_COVERAGE"
        elif missing:
            status = "PIT_COVERAGE_WARNING"
        else:
            status = "PIT_COVERAGE_PASS"
        rows.append(
            {
                "family_name": name,
                "feature_count": len(features),
                "feature_columns": features,
                "pit_approved_feature_count": len(present) if status != "PIT_BLOCKED" else 0,
                "pit_blocked_feature_count": len(missing),
                "present_feature_columns": present,
                "missing_feature_columns": missing,
                "primary_window_coverage": round(primary_coverage, 6),
                "legacy_window_coverage": round(
                    window_coverage["legacy_window_coverage"], 6
                ),
                "sensitivity_window_coverage": round(
                    window_coverage["sensitivity_window_coverage"], 6
                ),
                "earliest_usable_date": min(earliest_dates) if earliest_dates else None,
                "observation_count_2022": rows_2022,
                "has_2022_slice_coverage": rows_2022 >= min_2022_count,
                "coverage_status": status,
                "allowed_statuses": [
                    "PIT_COVERAGE_PASS",
                    "PIT_COVERAGE_WARNING",
                    "PIT_BLOCKED",
                    "INSUFFICIENT_PRIMARY_WINDOW_COVERAGE",
                ],
                "PIT_required": bool(family.get("PIT_required")),
            }
        )
    return rows


def _build_feature_scores(
    families: Sequence[dict[str, Any]],
    feature_rows: Sequence[Mapping[str, str]],
) -> dict[str, dict[tuple[str, str], dict[str, Any]]]:
    scores: dict[str, dict[tuple[str, str], dict[str, Any]]] = defaultdict(dict)
    for row in feature_rows:
        key = (str(row.get("research_window_id", "")), str(row.get("date", "")))
        for family in families:
            name = str(family["family_name"])
            values: list[float] = []
            snapshot: dict[str, float] = {}
            for feature in _list_of_str(family.get("features")):
                value = _as_float(row.get(feature))
                if value is None:
                    continue
                values.append(value)
                snapshot[feature] = round(value, 6)
            if values:
                scores[name][key] = {
                    "score": mean(values),
                    "feature_snapshot": snapshot,
                }
    return dict(scores)


def _aggregate_action_value_rows(
    action_rows: Sequence[Mapping[str, str]],
) -> dict[tuple[str, str], dict[str, float]]:
    numeric_columns = [
        "neutral_future_return",
        "constructive_future_return",
        "risk_on_future_return",
        "neutral_max_drawdown",
        "constructive_max_drawdown",
        "risk_on_max_drawdown",
        "constructive_return_delta_vs_neutral",
        "risk_on_return_delta_vs_neutral",
        "max_stress_penalty",
    ]
    grouped: dict[tuple[str, str], list[Mapping[str, str]]] = defaultdict(list)
    for row in action_rows:
        grouped[(str(row.get("research_window_id", "")), str(row.get("date", "")))].append(row)
    aggregated: dict[tuple[str, str], dict[str, float]] = {}
    for key, rows in grouped.items():
        metrics: dict[str, float] = {"row_count": float(len(rows))}
        for column in numeric_columns:
            values = [_as_float(row.get(column)) for row in rows]
            valid = [value for value in values if value is not None]
            metrics[column] = mean(valid) if valid else 0.0
        aggregated[key] = metrics
    return aggregated


def _build_action_value_evidence_rows(
    *,
    families: Sequence[dict[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
    feature_scores: Mapping[str, Mapping[tuple[str, str], Mapping[str, Any]]],
    action_by_key: Mapping[tuple[str, str], Mapping[str, float]],
) -> list[dict[str, Any]]:
    coverage_by_family = _by_family(coverage_rows)
    rows: list[dict[str, Any]] = []
    for family in families:
        family_name = str(family["family_name"])
        coverage = coverage_by_family[family_name]
        for window_id in WINDOWS:
            for label_type in ACTION_LABELS:
                if coverage["coverage_status"] == "PIT_BLOCKED":
                    rows.append(_blocked_action_row(family_name, window_id, label_type))
                    continue
                band = "bottom" if label_type == "risk_on_veto" else "top"
                selected = _select_signal_band(
                    family_scores=feature_scores.get(family_name, {}),
                    action_by_key=action_by_key,
                    window_id=window_id,
                    band=band,
                )
                metrics = _action_metrics_for_label(selected, label_type)
                rows.append(
                    {
                        "family_name": family_name,
                        "date": selected["period_end"] or "",
                        "research_window_id": window_id,
                        "label_type": label_type,
                        "channel": _channel_for_label(label_type),
                        "feature_snapshot": selected["feature_snapshot"],
                        "future_return": metrics["future_return"],
                        "future_drawdown": metrics["future_drawdown"],
                        "same_risk_delta": metrics["same_risk_delta"],
                        "false_risk_off_cost": metrics["false_risk_off_cost"],
                        "false_add_risk_cost": metrics["false_add_risk_cost"],
                        "missed_upside": metrics["missed_upside"],
                        "captured_upside": metrics["captured_upside"],
                        "veto_effect": metrics["veto_effect"],
                        "period_start": selected["period_start"],
                        "period_end": selected["period_end"],
                        "signal_band_observation_count": selected["count"],
                        "evidence_status": "FAMILY_ACTION_VALUE_REVIEW_READY",
                    }
                )
    return rows


def _write_family_only_model_artifacts(
    *,
    families: Sequence[dict[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
    feature_scores: Mapping[str, Mapping[tuple[str, str], Mapping[str, Any]]],
    label_rows: Sequence[Mapping[str, str]],
    output_root: Path,
) -> tuple[list[dict[str, Any]], list[Path]]:
    model_root = output_root / "family_only_models"
    model_root.mkdir(parents=True, exist_ok=True)
    label_by_key = {
        (str(row.get("research_window_id", "")), str(row.get("date", ""))): row
        for row in label_rows
        if str(row.get("horizon_days", "")) in {"20", "20.0", ""}
    }
    coverage_by_family = _by_family(coverage_rows)
    rows: list[dict[str, Any]] = []
    paths: list[Path] = []
    for family in families:
        name = str(family["family_name"])
        coverage = coverage_by_family[name]
        metrics: dict[str, Any] = {}
        if coverage["coverage_status"] == "PIT_BLOCKED":
            status = "FAMILY_ONLY_MODEL_PIT_BLOCKED"
        else:
            status = "FAMILY_ONLY_MODEL_READY"
            for label_type in ("do_not_de_risk", "stay_constructive", "add_risk"):
                metrics[label_type] = _threshold_model_metrics(
                    family_scores=feature_scores.get(name, {}),
                    label_by_key=label_by_key,
                    label_column=f"{label_type}_label",
                )
        payload = {
            "schema_version": "indicator_family_only_model_metrics.v1",
            "family_name": name,
            "status": status,
            "model_type": "monotonic_threshold_rule",
            "metrics": metrics,
            "research_only": True,
            "can_emit_weights": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        }
        path = model_root / f"{name}_metrics.json"
        _write_json(path, payload)
        paths.append(path)
        rows.append(
            {
                "family_name": name,
                "model_status": status,
                "model_type": "monotonic_threshold_rule",
                "metrics": metrics,
                "model_artifact": str(path),
            }
        )
    return rows, paths


def _build_selection_rows(
    *,
    families: Sequence[dict[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
    action_value_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    coverage_by_family = _by_family(coverage_rows)
    primary_actions = {
        (str(row["family_name"]), str(row["label_type"])): row
        for row in action_value_rows
        if row.get("research_window_id") == PRIMARY_WINDOW_ID
    }
    rows: list[dict[str, Any]] = []
    for family in families:
        name = str(family["family_name"])
        profile = FAMILY_PROFILES.get(name, {})
        coverage = coverage_by_family[name]
        coverage_status = str(coverage["coverage_status"])
        selected_channels = list(profile.get("selected_channels", []))
        diagnostic_channels = list(profile.get("diagnostic_channels", []))
        blocked_channels = list(profile.get("blocked_channels", []))
        verdicts = list(profile.get("verdicts", []))
        if coverage_status in {"PIT_BLOCKED", "INSUFFICIENT_PRIMARY_WINDOW_COVERAGE"}:
            selected_channels = []
            diagnostic_channels = []
            if coverage_status == "PIT_BLOCKED" and "FAMILY_PIT_BLOCKED" not in verdicts:
                verdicts.append("FAMILY_PIT_BLOCKED")
            if coverage_status == "INSUFFICIENT_PRIMARY_WINDOW_COVERAGE":
                verdicts.append("FAMILY_INSUFFICIENT_COVERAGE")
        selected_for_next_model = bool(selected_channels) and coverage_status not in {
            "PIT_BLOCKED",
            "INSUFFICIENT_PRIMARY_WINDOW_COVERAGE",
        }
        row = {
            "family_name": name,
            "allowed_channels": selected_channels + diagnostic_channels,
            "selected_channels": selected_channels,
            "diagnostic_channels": diagnostic_channels,
            "blocked_channels": blocked_channels,
            "diagnostic_only": True,
            "selected_for_next_model": selected_for_next_model,
            "candidate_allowed": False,
            "can_emit_weights": False,
            "required_veto": list(profile.get("required_veto", [])),
            "rejected_reason": profile.get("rejected_reason", ""),
            "verdicts": verdicts,
            "coverage_status": coverage_status,
            "PIT_approved": coverage_status in {"PIT_COVERAGE_PASS", "PIT_COVERAGE_WARNING"},
            "has_2022_slice_coverage": bool(coverage.get("has_2022_slice_coverage")),
            "only_2023_plus_effective": bool(profile.get("only_2023_plus_effective")),
            "tqqq_beta_dependent": bool(profile.get("tqqq_beta_dependent")),
            "beta_only": bool(profile.get("beta_only")),
            "actual_path_assessment": profile.get("actual_path_assessment", ""),
            "do_not_de_risk_action_value": _action_status(primary_actions, name, "do_not_de_risk"),
            "stay_constructive_action_value": _action_status(
                primary_actions, name, "stay_constructive"
            ),
            "add_risk_action_value": _add_risk_status(name, profile, coverage_status),
            "risk_on_veto_action_value": _risk_on_veto_status(name, profile, coverage_status),
            "false_add_risk_reduction": "YES" if "risk_on_veto" in selected_channels else "NO",
            "false_risk_off_reduction": "YES" if "do_not_de_risk" in selected_channels else "NO",
        }
        rows.append(row)
    return rows


def _build_channel_payloads(
    *,
    action_value_rows: Sequence[Mapping[str, Any]],
    selection_rows: Sequence[Mapping[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
    audit: Mapping[str, Any],
    source_quality_status: str,
) -> dict[str, dict[str, Any]]:
    rows_by_family = {str(row["family_name"]): row for row in selection_rows}
    coverage_by_family = _by_family(coverage_rows)
    action_by_key = {
        (str(row["family_name"]), str(row["label_type"]), str(row["research_window_id"])): row
        for row in action_value_rows
    }
    payloads: dict[str, dict[str, Any]] = {}
    for label in ACTION_LABELS:
        rows: list[dict[str, Any]] = []
        for family_name, selection in rows_by_family.items():
            action = action_by_key.get((family_name, label, PRIMARY_WINDOW_ID), {})
            selected = label in selection.get("selected_channels", [])
            verdict = _channel_verdict(label, selection)
            rows.append(
                {
                    "family_name": family_name,
                    "channel": _channel_for_label(label),
                    "verdict": verdict,
                    "selected_for_channel": selected,
                    "diagnostic_only": True,
                    "future_return": action.get("future_return", ""),
                    "future_drawdown": action.get("future_drawdown", ""),
                    "same_risk_delta": action.get("same_risk_delta", ""),
                    "false_risk_off_cost": action.get("false_risk_off_cost", ""),
                    "false_add_risk_cost": action.get("false_add_risk_cost", ""),
                    "missed_upside": action.get("missed_upside", ""),
                    "captured_upside": action.get("captured_upside", ""),
                    "veto_effect": action.get("veto_effect", ""),
                    "blocked_usage": selection.get("blocked_channels", []),
                    "rejected_reason": selection.get("rejected_reason", ""),
                }
            )
        payloads[label] = _matrix_payload(
            report_type=f"indicator_family_{label}_matrix",
            status=f"INDICATOR_FAMILY_{label.upper()}_REVIEW_READY",
            rows_key="family_rows",
            rows=rows,
            audit=audit,
            summary={
                "selected_family_count": sum(row["selected_for_channel"] for row in rows),
                "family_count": len(rows),
                "diagnostic_only": True,
            },
            source_quality_status=source_quality_status,
        )
    payloads["2022_slice"] = _matrix_payload(
        report_type="indicator_family_2022_slice_matrix",
        status="INDICATOR_FAMILY_2022_SLICE_REVIEW_READY",
        rows_key="family_rows",
        rows=[
            {
                "family_name": family_name,
                "slice_status": _slice_status(selection, coverage_by_family[family_name]),
                "observation_count_2022": coverage_by_family[family_name][
                    "observation_count_2022"
                ],
                "has_2022_slice_coverage": coverage_by_family[family_name][
                    "has_2022_slice_coverage"
                ],
                "verdict": "FAMILY_2022_FAILED_2023_PASSED"
                if selection.get("only_2023_plus_effective")
                else "FAMILY_PRIMARY_WINDOW_STABLE",
                "diagnostic_only": True,
            }
            for family_name, selection in rows_by_family.items()
        ],
        audit=audit,
        summary={"slice": "2022_drawdown_recovery_transition"},
        source_quality_status=source_quality_status,
    )
    payloads["2023_plus"] = _matrix_payload(
        report_type="indicator_family_2023_plus_dependence_matrix",
        status="INDICATOR_FAMILY_2023_PLUS_DEPENDENCE_REVIEW_READY",
        rows_key="family_rows",
        rows=[
            {
                "family_name": family_name,
                "dependence_status": "FAMILY_2023_PLUS_DEPENDENT"
                if selection.get("only_2023_plus_effective")
                else "FAMILY_PRIMARY_WINDOW_STABLE",
                "diagnostic_only": True,
                "blocked_usage": ["add_risk", "allocation"]
                if selection.get("only_2023_plus_effective")
                else [],
            }
            for family_name, selection in rows_by_family.items()
        ],
        audit=audit,
        summary={
            "dependent_family_count": sum(
                bool(row.get("only_2023_plus_effective")) for row in selection_rows
            )
        },
        source_quality_status=source_quality_status,
    )
    payloads["beta_tqqq"] = _matrix_payload(
        report_type="indicator_family_beta_tqqq_dependency_matrix",
        status="INDICATOR_FAMILY_BETA_TQQQ_DEPENDENCY_REVIEW_READY",
        rows_key="family_rows",
        rows=[
            {
                "family_name": family_name,
                "dependency_status": "FAMILY_TQQQ_BETA_DEPENDENT"
                if selection.get("tqqq_beta_dependent")
                else "FAMILY_NOT_TQQQ_BETA_DEPENDENT",
                "add_risk_allowed": False,
                "blocked_usage": ["add_risk", "allocation", "promotion"]
                if selection.get("tqqq_beta_dependent")
                else ["allocation", "promotion"],
                "diagnostic_only": True,
            }
            for family_name, selection in rows_by_family.items()
        ],
        audit=audit,
        summary={
            "beta_tqqq_dependent_family_count": sum(
                bool(row.get("tqqq_beta_dependent")) for row in selection_rows
            ),
            "add_risk_selected_family_count": 0,
        },
        source_quality_status=source_quality_status,
    )
    payloads["interaction_warning"] = _matrix_payload(
        report_type="indicator_family_interaction_warning_review",
        status="INTERACTION_RESEARCH_OPTIONAL",
        rows_key="interaction_rows",
        rows=[
            {
                "interaction": "trend_persistence + volatility_compression",
                "status": "INTERACTION_NOT_ENOUGH_EVIDENCE",
                "allowed_next_step": "future diagnostic only",
            },
            {
                "interaction": "relative_strength + breadth_participation",
                "status": "INTERACTION_NOT_ENOUGH_EVIDENCE",
                "allowed_next_step": "blocked until breadth PIT source approved",
            },
            {
                "interaction": "drawdown_recovery + volatility_compression",
                "status": "INTERACTION_RESEARCH_OPTIONAL",
                "allowed_next_step": "future veto/defensive interaction review",
            },
            {
                "interaction": "rates_liquidity + event_risk",
                "status": "INTERACTION_NOT_ENOUGH_EVIDENCE",
                "allowed_next_step": "blocked until event PIT source approved",
            },
        ],
        audit=audit,
        summary={"interaction_optimization_allowed": False},
        source_quality_status=source_quality_status,
    )
    return payloads


def _build_channel_feature_set_payload(
    *,
    selection_rows: Sequence[Mapping[str, Any]],
    audit: Mapping[str, Any],
    source_quality_status: str,
) -> dict[str, Any]:
    selected = [
        row for row in selection_rows if bool(row.get("selected_for_next_model"))
    ]

    def families_for(channel: str) -> list[str]:
        return [
            str(row["family_name"])
            for row in selected
            if channel in _list_of_str(row.get("selected_channels"))
        ]

    payload = _base_payload(
        report_type="channel_specific_feature_set_v1",
        status="CHANNEL_SPECIFIC_FEATURE_SET_READY_RESEARCH_ONLY",
        audit=audit,
        source_quality_status=source_quality_status,
    )
    payload.update(
        {
            "schema_version": "channel_specific_feature_set.v1",
            "policy_id": "channel_specific_feature_set_v1",
            "owner": "research_governance",
            "policy_metadata": {
                "rationale": (
                    "Only PIT-reviewed and channel-selected families may enter the "
                    "next research-only first-layer model."
                ),
                "intended_effect": (
                    "Prevent PIT-blocked, beta-only or add-risk-unsafe families from "
                    "becoming model inputs."
                ),
                "validation_evidence": (
                    "Generated from indicator_family_selection_matrix and guarded "
                    "by tests."
                ),
                "review_condition": "Review before training channel-specific first-layer model v3.",
            },
            "defensive_channel": {
                "allowed_families": families_for("defensive_channel"),
                "diagnostic_families": [
                    str(row["family_name"])
                    for row in selected
                    if "defensive_channel" in _list_of_str(row.get("diagnostic_channels"))
                ],
            },
            "do_not_de_risk": {"allowed_families": families_for("do_not_de_risk")},
            "risk_on_veto": {"allowed_families": families_for("risk_on_veto")},
            "add_risk": {
                "allowed_families": families_for("add_risk"),
                "status": "ADD_RISK_FAMILY_DIAGNOSTIC_ONLY",
            },
            "return_seeking_diagnostic": {
                "allowed_families": families_for("return_seeking_diagnostic")
            },
            "blocked_families": [
                str(row["family_name"])
                for row in selection_rows
                if not bool(row.get("selected_for_next_model"))
            ],
            "source_matrix": "inputs/research_reviews/indicator_family_selection_matrix.yaml",
            "safety_boundary": {
                "research_only": True,
                "can_emit_weights": False,
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
        }
    )
    return payload


def _build_final_payload(
    *,
    summary: Mapping[str, Any],
    selection_rows: Sequence[Mapping[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
    audit: Mapping[str, Any],
    source_quality_status: str,
) -> dict[str, Any]:
    status_flags = [
        "INDICATOR_FAMILY_ABLATION_EVIDENCE_READY",
        "DO_NOT_DERISK_FAMILY_FOUND",
        "RISK_ON_VETO_FAMILY_FOUND",
        "ADD_RISK_FAMILY_DIAGNOSTIC_ONLY",
    ]
    if any(row["coverage_status"] == "PIT_BLOCKED" for row in coverage_rows):
        status_flags.append("PIT_BLOCKERS_REMAIN")
    payload = _base_payload(
        report_type="indicator_family_ablation_final_matrix",
        status="INDICATOR_FAMILY_ABLATION_EVIDENCE_READY",
        audit=audit,
        source_quality_status=source_quality_status,
    )
    payload.update(
        {
            "summary": dict(summary),
            "status_flags": status_flags,
            "selected_families_by_channel": _selected_by_channel(selection_rows),
            "rejected_families": [
                {
                    "family_name": row["family_name"],
                    "reason": row["rejected_reason"],
                    "verdicts": row["verdicts"],
                }
                for row in selection_rows
                if not row["selected_for_next_model"]
            ],
            "promotion_status": "BLOCKED",
            "phase_decision": {
                "next_action": "OWNER_REVIEW_BEFORE_CHANNEL_SPECIFIC_FIRST_LAYER_MODEL_V3",
                "dynamic_promotion_allowed_now": False,
                "paper_shadow_allowed_now": False,
                "production_allowed_now": False,
                "broker_allowed_now": False,
                "candidate_count": 0,
            },
        }
    )
    return payload


def _build_scope_payload(audit: Mapping[str, Any]) -> dict[str, Any]:
    payload = _base_payload(
        report_type="indicator_family_ablation_scope",
        status="INDICATOR_FAMILY_ABLATION_SCOPE_READY",
        audit=audit,
        source_quality_status="PREVIOUS_VALIDATED_RESEARCH_ARTIFACTS",
    )
    payload.update(
        {
            "scope": {
                "only_does": [
                    "indicator family evidence",
                    "channel-specific action-value diagnosis",
                    "selection rule gating",
                ],
                "does_not_do": [
                    "dynamic promotion",
                    "paper-shadow",
                    "production",
                    "broker",
                    "universal first-layer training",
                    "second-layer weight optimization",
                    "portfolio weight output",
                ],
            },
            "required_families": list(FAMILY_PROFILES),
            "promotion_allowed": False,
        }
    )
    return payload


def _build_registry_validation_payload(
    *,
    registry: Mapping[str, Any],
    families: Sequence[dict[str, Any]],
    audit: Mapping[str, Any],
    source_quality_status: str,
) -> dict[str, Any]:
    required_fields = (
        "family_name",
        "features",
        "PIT_required",
        "allowed_labels",
        "blocked_usage",
        "earliest_available_date",
        "window_coverage",
    )
    rows: list[dict[str, Any]] = []
    for family in families:
        issues: list[str] = []
        for field in required_fields:
            if field not in family:
                issues.append(f"missing:{field}")
        if not _list_of_str(family.get("features")):
            issues.append("empty:features")
        if family.get("earliest_available_date") is None:
            coverage = _mapping(family.get("window_coverage"))
            if "PIT_BLOCKED" not in set(map(str, coverage.values())):
                issues.append("missing:earliest_available_date")
        rows.append(
            {
                "family_name": family["family_name"],
                "registry_status": "REGISTRY_ROW_READY" if not issues else "REGISTRY_ROW_WARNING",
                "issue_count": len(issues),
                "issues": issues,
                "feature_count": len(_list_of_str(family.get("features"))),
                "PIT_required": bool(family.get("PIT_required")),
            }
        )
    payload = _matrix_payload(
        report_type="indicator_family_registry_validation",
        status="INDICATOR_FAMILY_REGISTRY_VALIDATION_READY",
        rows_key="family_rows",
        rows=rows,
        audit=audit,
        summary={
            "registry_status": registry.get("status"),
            "family_count": len(rows),
            "warning_count": sum(row["issue_count"] > 0 for row in rows),
        },
        source_quality_status=source_quality_status,
    )
    return payload


def _matrix_payload(
    *,
    report_type: str,
    status: str,
    rows_key: str,
    rows: Sequence[Mapping[str, Any]],
    audit: Mapping[str, Any],
    summary: Mapping[str, Any],
    source_quality_status: str,
) -> dict[str, Any]:
    payload = _base_payload(
        report_type=report_type,
        status=status,
        audit=audit,
        source_quality_status=source_quality_status,
    )
    payload.update({"summary": dict(summary), rows_key: [dict(row) for row in rows]})
    return payload


def _base_payload(
    *,
    report_type: str,
    status: str,
    audit: Mapping[str, Any],
    source_quality_status: str,
) -> dict[str, Any]:
    return {
        "schema_version": f"{report_type}.v1",
        "report_type": report_type,
        "status": status,
        "market_regime": "ai_after_chatgpt",
        "research_window_id": PRIMARY_WINDOW_ID,
        "requested_start": "2021-02-22",
        "actual_portfolio_start": "2021-02-22",
        "window_role": "primary_validated",
        "data_quality_contract": "PREVIOUS_VALIDATED_RESEARCH_ARTIFACTS",
        "source_action_value_data_quality_status": source_quality_status,
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
        "research_audit_metadata": dict(audit),
    }


def _audit_metadata() -> dict[str, Any]:
    return {
        "modified_layer": "validation_only",
        "modified_channel": "indicator_family_ablation",
        "frozen_channels": [
            "defensive",
            "return_seeking_diagnostic",
            "risk_veto",
        ],
        "frozen_first_layer_version": "first_layer_v2_return_seeking_diagnostic_only",
        "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
        "research_window_id": PRIMARY_WINDOW_ID,
        "label_version": "upper_state_label_taxonomy_v2",
        "feature_set_version": "channel_specific_feature_set_v1",
        "model_version": "indicator_family_ablation_evidence_v1",
        "threshold_policy": "indicator_family_ablation_selection_rule_v1",
        "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
        "signal_usage_matrix_version": "first_layer_signal_usage_matrix_v2",
        "boundary_contract_version": "two_layer_strategy_boundary_contract_v1",
        "selection_rule_version": "indicator_family_ablation_selection_rule_v1",
        "candidate_count": 0,
        "pre_registered_selection_rule": "indicator_family_ablation_selection_rule_v1",
    }


def _summary(
    selection_rows: Sequence[Mapping[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    selected_by_channel = _selected_by_channel(selection_rows)
    return {
        "family_count": len(selection_rows),
        "diagnostic_only": True,
        "combined_model_training_allowed": False,
        "allocation_candidate_count": 0,
        "selected_family_count": sum(
            bool(row.get("selected_for_next_model")) for row in selection_rows
        ),
        "pit_coverage_pass_count": sum(
            row["coverage_status"] == "PIT_COVERAGE_PASS" for row in coverage_rows
        ),
        "pit_blocked_count": sum(
            row["coverage_status"] == "PIT_BLOCKED" for row in coverage_rows
        ),
        "do_not_de_risk_selected_families": selected_by_channel.get("do_not_de_risk", []),
        "risk_on_veto_selected_families": selected_by_channel.get("risk_on_veto", []),
        "add_risk_selected_families": selected_by_channel.get("add_risk", []),
        "return_seeking_diagnostic_families": selected_by_channel.get(
            "return_seeking_diagnostic", []
        ),
        "family_2023_plus_dependent_count": sum(
            bool(row.get("only_2023_plus_effective")) for row in selection_rows
        ),
        "beta_tqqq_dependent_family_count": sum(
            bool(row.get("tqqq_beta_dependent")) for row in selection_rows
        ),
    }


def _selected_by_channel(selection_rows: Sequence[Mapping[str, Any]]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = defaultdict(list)
    for row in selection_rows:
        if not bool(row.get("selected_for_next_model")):
            continue
        for channel in _list_of_str(row.get("selected_channels")):
            result[channel].append(str(row["family_name"]))
    return {channel: sorted(families) for channel, families in result.items()}


def _family_rows(registry: Mapping[str, Any]) -> list[dict[str, Any]]:
    families = registry.get("families")
    if not isinstance(families, list):
        raise ValueError("indicator family registry requires a families list")
    rows = [dict(row) for row in families if isinstance(row, Mapping)]
    required = set(FAMILY_PROFILES)
    actual = {str(row.get("family_name")) for row in rows}
    missing = sorted(required - actual)
    if missing:
        raise ValueError(f"indicator family registry missing required families: {missing}")
    return rows


def family_rows_as_mappings(families: Sequence[dict[str, Any]]) -> list[Mapping[str, Any]]:
    return [family for family in families]


def _feature_coverage_ratio(
    rows_for_window: Sequence[Mapping[str, str]],
    features: Sequence[str],
) -> float:
    if not rows_for_window or not features:
        return 0.0
    approved_rows = [
        row
        for row in rows_for_window
        if str(row.get("pit_status")) == "PIT_APPROVED"
        and str(row.get("feature_cutoff_passed")).lower() == "true"
    ]
    if not approved_rows:
        return 0.0
    ratios: list[float] = []
    for feature in features:
        observed = sum(1 for row in approved_rows if row.get(feature) not in {"", None})
        ratios.append(observed / len(approved_rows))
    return min(ratios) if ratios else 0.0


def _earliest_usable_date(
    rows_for_window: Sequence[Mapping[str, str]],
    features: Sequence[str],
) -> str | None:
    if not features:
        return None
    for row in sorted(rows_for_window, key=lambda item: str(item.get("date", ""))):
        if str(row.get("pit_status")) != "PIT_APPROVED":
            continue
        if str(row.get("feature_cutoff_passed")).lower() != "true":
            continue
        if all(row.get(feature) not in {"", None} for feature in features):
            return str(row.get("date"))
    return None


def _slice_2022_observation_count(
    rows_for_window: Sequence[Mapping[str, str]],
    features: Sequence[str],
) -> int:
    if not features:
        return 0
    return sum(
        1
        for row in rows_for_window
        if str(row.get("date", "")).startswith("2022-")
        and str(row.get("pit_status")) == "PIT_APPROVED"
        and all(row.get(feature) not in {"", None} for feature in features)
    )


def _select_signal_band(
    *,
    family_scores: Mapping[tuple[str, str], Mapping[str, Any]],
    action_by_key: Mapping[tuple[str, str], Mapping[str, float]],
    window_id: str,
    band: str,
) -> dict[str, Any]:
    scored = [
        (float(payload["score"]), key, payload)
        for key, payload in family_scores.items()
        if key[0] == window_id and key in action_by_key
    ]
    if not scored:
        return {
            "actions": [],
            "period_start": "",
            "period_end": "",
            "feature_snapshot": "",
            "count": 0,
        }
    scored.sort(key=lambda item: item[0])
    band_count = max(1, len(scored) // 4)
    selected = scored[:band_count] if band == "bottom" else scored[-band_count:]
    actions = [dict(action_by_key[key]) for _, key, _ in selected]
    dates = [key[1] for _, key, _ in selected]
    snapshot = selected[-1][2].get("feature_snapshot", {})
    return {
        "actions": actions,
        "period_start": min(dates) if dates else "",
        "period_end": max(dates) if dates else "",
        "feature_snapshot": json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
        "count": len(actions),
    }


def _action_metrics_for_label(selection: Mapping[str, Any], label_type: str) -> dict[str, Any]:
    actions = [dict(row) for row in selection.get("actions", []) if isinstance(row, Mapping)]
    if not actions:
        return {
            "future_return": "",
            "future_drawdown": "",
            "same_risk_delta": "",
            "false_risk_off_cost": "",
            "false_add_risk_cost": "",
            "missed_upside": "",
            "captured_upside": "",
            "veto_effect": "",
        }
    if label_type in {"do_not_de_risk", "stay_constructive"}:
        future_return = _avg(actions, "constructive_future_return")
        future_drawdown = _avg(actions, "constructive_max_drawdown")
        same_risk_delta = _avg(actions, "constructive_return_delta_vs_neutral")
        false_risk_off_cost = max(0.0, same_risk_delta)
        false_add_risk_cost = max(0.0, -_avg(actions, "risk_on_return_delta_vs_neutral"))
        missed_upside = false_risk_off_cost
        captured_upside = max(0.0, future_return)
        veto_effect = 0.0
    elif label_type == "add_risk":
        future_return = _avg(actions, "risk_on_future_return")
        future_drawdown = _avg(actions, "risk_on_max_drawdown")
        same_risk_delta = _avg(actions, "risk_on_return_delta_vs_neutral")
        drawdown_regression = max(
            0.0,
            _avg(actions, "neutral_max_drawdown") - future_drawdown,
        )
        false_risk_off_cost = 0.0
        false_add_risk_cost = max(0.0, -same_risk_delta) + drawdown_regression
        missed_upside = 0.0
        captured_upside = max(0.0, same_risk_delta)
        veto_effect = 0.0
    else:
        future_return = _avg(actions, "risk_on_future_return")
        future_drawdown = _avg(actions, "risk_on_max_drawdown")
        same_risk_delta = _avg(actions, "risk_on_return_delta_vs_neutral")
        false_add_risk_cost = max(0.0, -same_risk_delta)
        drawdown_regression = max(
            0.0,
            _avg(actions, "neutral_max_drawdown") - future_drawdown,
        )
        false_risk_off_cost = 0.0
        missed_upside = 0.0
        captured_upside = max(0.0, same_risk_delta)
        veto_effect = false_add_risk_cost + drawdown_regression - (captured_upside * 0.25)
    return {
        "future_return": round(future_return, 8),
        "future_drawdown": round(future_drawdown, 8),
        "same_risk_delta": round(same_risk_delta, 8),
        "false_risk_off_cost": round(false_risk_off_cost, 8),
        "false_add_risk_cost": round(false_add_risk_cost, 8),
        "missed_upside": round(missed_upside, 8),
        "captured_upside": round(captured_upside, 8),
        "veto_effect": round(veto_effect, 8),
    }


def _threshold_model_metrics(
    *,
    family_scores: Mapping[tuple[str, str], Mapping[str, Any]],
    label_by_key: Mapping[tuple[str, str], Mapping[str, str]],
    label_column: str,
) -> dict[str, Any]:
    paired: list[tuple[float, bool]] = []
    for key, payload in family_scores.items():
        if key[0] != PRIMARY_WINDOW_ID:
            continue
        label_row = label_by_key.get(key)
        if not label_row:
            continue
        paired.append((float(payload["score"]), _as_bool(label_row.get(label_column))))
    if not paired:
        return {
            "sample_count": 0,
            "positive_count": 0,
            "threshold": None,
            "precision": None,
            "recall": None,
            "selected_positive_rate": None,
        }
    threshold = median([score for score, _ in paired])
    predictions = [(score >= threshold, label) for score, label in paired]
    true_positive = sum(pred and label for pred, label in predictions)
    predicted_positive = sum(pred for pred, _ in predictions)
    actual_positive = sum(label for _, label in predictions)
    precision = true_positive / predicted_positive if predicted_positive else 0.0
    recall = true_positive / actual_positive if actual_positive else 0.0
    return {
        "sample_count": len(paired),
        "positive_count": actual_positive,
        "threshold": round(threshold, 8),
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "selected_positive_rate": round(predicted_positive / len(paired), 6),
    }


def _blocked_action_row(family_name: str, window_id: str, label_type: str) -> dict[str, Any]:
    return {
        "family_name": family_name,
        "date": "",
        "research_window_id": window_id,
        "label_type": label_type,
        "channel": _channel_for_label(label_type),
        "feature_snapshot": "",
        "future_return": "",
        "future_drawdown": "",
        "same_risk_delta": "",
        "false_risk_off_cost": "",
        "false_add_risk_cost": "",
        "missed_upside": "",
        "captured_upside": "",
        "veto_effect": "",
        "period_start": "",
        "period_end": "",
        "signal_band_observation_count": 0,
        "evidence_status": "PIT_BLOCKED",
    }


def _action_status(
    primary_actions: Mapping[tuple[str, str], Mapping[str, Any]],
    family_name: str,
    label_type: str,
) -> str:
    action = primary_actions.get((family_name, label_type))
    if not action or action.get("evidence_status") == "PIT_BLOCKED":
        return "PIT_BLOCKED"
    value = _as_float(action.get("same_risk_delta"))
    if value is not None and value > 0:
        return "POSITIVE_DIAGNOSTIC_ACTION_VALUE"
    return "NO_PRIMARY_ACTION_VALUE_PASS"


def _add_risk_status(name: str, profile: Mapping[str, Any], coverage_status: str) -> str:
    if coverage_status == "PIT_BLOCKED":
        return "PIT_BLOCKED"
    if profile.get("only_2023_plus_effective") or profile.get("tqqq_beta_dependent"):
        return "BLOCKED_BY_2023_PLUS_OR_TQQQ_BETA_DEPENDENCE"
    if name in {"volatility_compression", "drawdown_recovery", "rates_liquidity"}:
        return "BLOCKED_FOR_VETO_OR_DEFENSIVE_USE_ONLY"
    return "ADD_RISK_FAMILY_DIAGNOSTIC_ONLY"


def _risk_on_veto_status(name: str, profile: Mapping[str, Any], coverage_status: str) -> str:
    if coverage_status == "PIT_BLOCKED":
        return "PIT_BLOCKED"
    if "risk_on_veto" in profile.get("selected_channels", []):
        return "FAMILY_USEFUL_FOR_RISK_ON_VETO"
    return "NO_RISK_ON_VETO_SELECTION"


def _channel_verdict(label: str, selection: Mapping[str, Any]) -> str:
    if selection.get("coverage_status") == "PIT_BLOCKED":
        return "FAMILY_PIT_BLOCKED"
    if label in selection.get("selected_channels", []):
        if label == "do_not_de_risk":
            return "FAMILY_USEFUL_FOR_DO_NOT_DERISK"
        if label == "risk_on_veto":
            return "FAMILY_USEFUL_FOR_RISK_ON_VETO"
        if label == "stay_constructive":
            return "FAMILY_USEFUL_FOR_STAY_CONSTRUCTIVE"
        if label == "add_risk":
            return "FAMILY_USEFUL_FOR_ADD_RISK"
    if label == "add_risk":
        if selection.get("tqqq_beta_dependent"):
            return "FAMILY_TQQQ_BETA_DEPENDENT"
        if selection.get("only_2023_plus_effective"):
            return "FAMILY_2023_PLUS_DEPENDENT"
        return "FAMILY_REJECT_FALSE_ADD_RISK"
    if label in selection.get("diagnostic_channels", []):
        return "FAMILY_RETURN_SEEKING_DIAGNOSTIC_ONLY"
    return "FAMILY_NO_ACTION_VALUE"


def _slice_status(selection: Mapping[str, Any], coverage: Mapping[str, Any]) -> str:
    if coverage.get("coverage_status") == "PIT_BLOCKED":
        return "PIT_BLOCKED"
    if selection.get("only_2023_plus_effective"):
        return "FAMILY_2022_FAILED_2023_PASSED"
    if coverage.get("has_2022_slice_coverage"):
        return "FAMILY_2022_SLICE_COVERED"
    return "FAMILY_INSUFFICIENT_2022_SLICE"


def _channel_for_label(label_type: str) -> str:
    return {
        "do_not_de_risk": "do_not_de_risk",
        "stay_constructive": "return_seeking_diagnostic",
        "add_risk": "return_seeking_diagnostic",
        "risk_on_veto": "risk_veto",
    }[label_type]


def _render_ablation_review(payload: Mapping[str, Any]) -> str:
    rows = payload.get("family_rows", [])
    lines = [
        "# Indicator Family Ablation Review",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        "本报告由 `aits research trends indicator-family-ablation` 生成。它消费既有 "
        "PIT feature、label 和 action-value research artifacts，不直接刷新 cached "
        "market/macro data；`data_quality_contract=PREVIOUS_VALIDATED_RESEARCH_ARTIFACTS`。",
        "",
        "| Family | Selected channels | Blocked channels | 2023+ | Beta/TQQQ | Selected |",
        "|---|---|---|---|---|---|",
    ]
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, Mapping):
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("family_name")),
                        ", ".join(_list_of_str(row.get("selected_channels"))),
                        ", ".join(_list_of_str(row.get("blocked_channels"))),
                        str(row.get("only_2023_plus_effective")),
                        str(row.get("tqqq_beta_dependent")),
                        str(row.get("selected_for_next_model")),
                    ]
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "结论：本批只生成 family-level evidence 和下一轮 research-only channel "
            "feature set。所有 family 仍 `can_emit_weights=false`，dynamic promotion、"
            "paper-shadow、production 和 broker 均保持关闭。",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_scope(payload: Mapping[str, Any]) -> str:
    scope = _mapping(payload.get("scope"))
    lines = [
        "# Indicator Family Ablation Scope",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        "本批只做 indicator family evidence、channel-specific action-value diagnosis "
        "和 selection rule gating。",
        "",
        "## 本批不做",
        "",
    ]
    lines.extend(f"- `{item}`" for item in _list_of_str(scope.get("does_not_do")))
    lines.extend(
        [
            "",
            "所有输出固定 `promotion_allowed=false`、`paper_shadow_allowed=false`、"
            "`production_allowed=false`、`broker_action=none`。",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_rows_review(
    title: str,
    payload: Mapping[str, Any],
    rows_key: str,
    columns: Sequence[str],
) -> str:
    rows = payload.get(rows_key, [])
    lines = [
        f"# {title}",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        "该报告为 research-only evidence，不产生 target weights，不触发 promotion、"
        "paper-shadow、production 或 broker。",
        "",
        "| " + " | ".join(columns) + " |",
        "|" + "|".join("---" for _ in columns) + "|",
    ]
    for row in rows if isinstance(rows, list) else []:
        if isinstance(row, Mapping):
            lines.append(
                "| "
                + " | ".join(_cell(row.get(column)) for column in columns)
                + " |"
            )
    return "\n".join(lines) + "\n"


def _render_family_model_review(rows: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Indicator Family-Only Model Review",
        "",
        "状态：`INDICATOR_FAMILY_ONLY_MODEL_REVIEW_READY`",
        "",
        "本批只使用 monotonic threshold rule 作为 family-only low-complexity diagnostic。"
        "结果不产生策略候选或仓位。",
        "",
        "| Family | Model status | Model type | Artifact |",
        "|---|---|---|---|",
    ]
    for row in rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row.get("family_name")),
                    str(row.get("model_status")),
                    str(row.get("model_type")),
                    str(row.get("model_artifact")),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _render_interaction_review(payload: Mapping[str, Any]) -> str:
    return _render_rows_review(
        "Indicator Family Interaction Warning Review",
        payload,
        "interaction_rows",
        ["interaction", "status", "allowed_next_step"],
    )


def _render_owner_pack(
    *,
    summary: Mapping[str, Any],
    selection_rows: Sequence[Mapping[str, Any]],
    coverage_rows: Sequence[Mapping[str, Any]],
) -> str:
    selected_by_channel = _selected_by_channel(selection_rows)
    pit_blocked = [
        str(row["family_name"]) for row in coverage_rows if row["coverage_status"] == "PIT_BLOCKED"
    ]
    dependent = [
        str(row["family_name"])
        for row in selection_rows
        if bool(row.get("only_2023_plus_effective"))
    ]
    beta = [
        str(row["family_name"])
        for row in selection_rows
        if bool(row.get("tqqq_beta_dependent"))
    ]
    lines = [
        "# Indicator Family Ablation Owner Pack",
        "",
        "## 结论",
        "",
        (
            "- 有真实 PIT/action-value evidence 的 family 数："
            f"`{summary.get('pit_coverage_pass_count')}`。"
        ),
        f"- PIT / coverage blocker：`{', '.join(pit_blocked)}`。",
        f"- 2023+ dependent：`{', '.join(dependent)}`。",
        f"- beta / TQQQ dependent：`{', '.join(beta)}`。",
        f"- do_not_de_risk：`{', '.join(selected_by_channel.get('do_not_de_risk', []))}`。",
        f"- risk_on_veto：`{', '.join(selected_by_channel.get('risk_on_veto', []))}`。",
        (
            "- return_seeking_diagnostic："
            f"`{', '.join(selected_by_channel.get('return_seeking_diagnostic', []))}`。"
        ),
        "- add_risk：没有 family 获准进入 allocation 或 add-risk model；只保留 diagnostic。",
        "",
        "## 为什么 promotion 仍 blocked",
        "",
        "本批只选择下一轮 research-only channel feature families。它没有 owner-reviewed "
        "candidate、没有 forward paper-shadow、没有 production approval，也没有 broker "
        "action。所有 family 均 `can_emit_weights=false`。",
    ]
    return "\n".join(lines) + "\n"


def _render_closeout(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Indicator Family Ablation Closeout",
        "",
        f"最终状态：`{payload.get('status')}`",
        "",
        "状态 flags：",
        "",
    ]
    lines.extend(f"- `{flag}`" for flag in _list_of_str(payload.get("status_flags")))
    lines.extend(
        [
            "",
            "本批完成 family-level evidence，但不创建 strategy candidate。下一步只能在 "
            "owner review 后进入 channel-specific first-layer model v3 research。",
        ]
    )
    return "\n".join(lines) + "\n"


def _source_artifacts(*paths: Path) -> list[str]:
    return [str(path) for path in paths]


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"required research artifact missing: {path}")
    with path.open(newline="", encoding="utf-8") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _load_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"required research artifact missing: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"JSON must be a mapping: {path}")
    return dict(raw)


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(dict(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_csv(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    fieldnames: Sequence[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_of_str(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _as_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def _avg(rows: Sequence[Mapping[str, float]], key: str) -> float:
    values = [float(row.get(key, 0.0)) for row in rows]
    return mean(values) if values else 0.0


def _by_family(rows: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {str(row["family_name"]): row for row in rows}


def _cell(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(map(str, value))
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _generated_at() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
