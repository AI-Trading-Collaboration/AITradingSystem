from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_INDICATOR_FAMILY_REGISTRY_PATH = (
    PROJECT_ROOT / "config" / "research" / "indicator_family_registry.yaml"
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


def run_indicator_family_ablation(
    *,
    registry_path: Path = DEFAULT_INDICATOR_FAMILY_REGISTRY_PATH,
    output_root: Path = DEFAULT_INDICATOR_FAMILY_ABLATION_OUTPUT_ROOT,
    matrix_path: Path = DEFAULT_INDICATOR_FAMILY_ABLATION_MATRIX_PATH,
    review_path: Path = DEFAULT_INDICATOR_FAMILY_ABLATION_REVIEW_PATH,
) -> dict[str, Any]:
    registry = _load_mapping(registry_path)
    families = registry.get("families")
    if not isinstance(families, list):
        raise ValueError("indicator family registry requires a families list")
    rows = [_family_row(row) for row in families if isinstance(row, Mapping)]
    payload = {
        "schema_version": "indicator_family_ablation_matrix.v1",
        "report_type": "indicator_family_ablation_matrix",
        "status": "INDICATOR_FAMILY_ABLATION_READY",
        "market_regime": "ai_after_chatgpt",
        "research_window_id": "exact_three_asset_validated",
        "requested_start": "2021-02-22",
        "actual_portfolio_start": "2021-02-22",
        "window_role": "primary_validated",
        "data_quality_contract": "NOT_REQUIRED_REGISTRY_ONLY",
        "summary": {
            "family_count": len(rows),
            "diagnostic_only": True,
            "combined_model_training_allowed": False,
            "allocation_candidate_count": 0,
        },
        "family_rows": rows,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "production_effect": "none",
        "dynamic_promotion_status": "BLOCKED",
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "modified_channel": "indicator_family_ablation",
            "frozen_channels": [
                "defensive",
                "return_seeking_diagnostic",
                "risk_veto",
            ],
            "frozen_first_layer_version": "first_layer_v2_return_seeking_diagnostic_only",
            "frozen_second_layer_version": "dynamic_second_layer_probe_registry_v2",
            "research_window_id": "exact_three_asset_validated",
            "label_version": "upper_state_label_taxonomy_v2",
            "feature_set_version": "indicator_family_registry_v1",
            "model_version": "indicator_family_ablation_registry_only_v1",
            "threshold_policy": "do_not_de_risk_selection_rule_v1+risk_on_veto_policy_v1",
            "probe_registry_version": "dynamic_second_layer_probe_registry_v2",
            "signal_usage_matrix_version": "first_layer_signal_usage_matrix_v2",
            "boundary_contract_version": "two_layer_strategy_boundary_contract_v1",
            "selection_rule_version": "family_ablation_diagnostic_only_no_candidate_selection",
            "candidate_count": 0,
            "pre_registered_selection_rule": (
                "indicator_family_ablation_diagnostic_only_no_candidate_selection"
            ),
        },
    }
    output_root.mkdir(parents=True, exist_ok=True)
    matrix_path.parent.mkdir(parents=True, exist_ok=True)
    review_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "indicator_family_ablation_summary.yaml"
    _write_yaml(matrix_path, payload)
    _write_yaml(summary_path, payload)
    review_path.write_text(_render_review(payload), encoding="utf-8")
    return {
        "status": payload["status"],
        "summary": payload["summary"],
        "artifact_paths": {
            "matrix": str(matrix_path),
            "review": str(review_path),
            "summary": str(summary_path),
        },
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }


def _family_row(family: Mapping[str, Any]) -> dict[str, Any]:
    name = str(family.get("family_name", ""))
    feature_count = (
        len(family.get("features", [])) if isinstance(family.get("features"), list) else 0
    )
    return {
        "family_name": name,
        "feature_count": feature_count,
        "PIT_required": bool(family.get("PIT_required")),
        "allowed_labels": list(family.get("allowed_labels", []))
        if isinstance(family.get("allowed_labels"), list)
        else [],
        "blocked_usage": list(family.get("blocked_usage", []))
        if isinstance(family.get("blocked_usage"), list)
        else [],
        "do_not_de_risk_help": "PENDING_REAL_ABLATION",
        "stay_constructive_help": "PENDING_REAL_ABLATION",
        "add_risk_help": "PENDING_REAL_ABLATION",
        "false_add_risk_reduction": "PENDING_REAL_ABLATION",
        "only_2023_plus_effective": "PENDING_REAL_ABLATION",
        "primary_window_and_2022_slice_pass": "PENDING_REAL_ABLATION",
        "actual_path_improved": "PENDING_REAL_ABLATION",
        "diagnostic_only": True,
        "candidate_allowed": False,
    }


def _render_review(payload: Mapping[str, Any]) -> str:
    rows = payload.get("family_rows", [])
    lines = [
        "# Indicator Family Ablation Review",
        "",
        f"状态：`{payload.get('status')}`",
        "",
        "本报告由 `aits research trends indicator-family-ablation` 生成。当前为 "
        "registry-only diagnostic matrix，不消费 cached market data，因此 "
        "`data_quality_contract=NOT_REQUIRED_REGISTRY_ONLY`。",
        "",
        "| Family | PIT | Diagnostic only | Candidate |",
        "|---|---|---|---|",
    ]
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, Mapping):
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("family_name")),
                            str(row.get("PIT_required")),
                            str(row.get("diagnostic_only")),
                            str(row.get("candidate_allowed")),
                        ]
                    )
                    + " |"
                )
    lines.extend(
        [
            "",
            "所有 family 输出均为 diagnostic-only。没有真实 ablation evidence 和预注册 "
            "selection rule 前，不允许进入 combined model candidate、promotion、"
            "paper-shadow、production 或 broker。",
        ]
    )
    return "\n".join(lines) + "\n"


def _load_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        yaml.safe_dump(dict(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )
