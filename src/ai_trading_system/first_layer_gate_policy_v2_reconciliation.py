from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_walk_forward_coverage import (
    DEFAULT_COVERAGE_SELECTION_RULE_PATH,
)
from ai_trading_system.post_2085_research_common import (
    clean_for_yaml,
    load_mapping,
    mapping,
    records,
    strings,
    write_markdown,
    write_yaml,
)

DEFAULT_AUDIT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_performance_gate_audit"
)
DEFAULT_RECOMMENDED_GATE_POLICY_PATH = DEFAULT_AUDIT_ROOT / "recommended_gate_policy.yaml"
DEFAULT_GATE_ABLATION_MATRIX_PATH = DEFAULT_AUDIT_ROOT / "gate_ablation_matrix.json"
DEFAULT_THRESHOLD_SENSITIVITY_PATH = DEFAULT_AUDIT_ROOT / "threshold_sensitivity_report.json"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_gate_policy_v2"
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

STATUS = "FIRST_LAYER_GATE_POLICY_V2_RECONCILIATION_READY_PROMOTION_BLOCKED"
TASK_ID = "TRADING-2275_FIRST_LAYER_GATE_POLICY_V2_RECONCILIATION"


def run_first_layer_gate_policy_v2_reconciliation_pack(
    *,
    recommended_gate_policy_path: Path = DEFAULT_RECOMMENDED_GATE_POLICY_PATH,
    gate_ablation_matrix_path: Path = DEFAULT_GATE_ABLATION_MATRIX_PATH,
    threshold_sensitivity_path: Path = DEFAULT_THRESHOLD_SENSITIVITY_PATH,
    active_selection_rule_path: Path = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    recommended_policy = load_mapping(recommended_gate_policy_path)
    gate_ablation = load_mapping(gate_ablation_matrix_path)
    threshold_sensitivity = load_mapping(threshold_sensitivity_path)
    active_selection_rule = load_mapping(active_selection_rule_path)

    source_summary = mapping(recommended_policy.get("summary"))
    hard_research_gates = _hard_research_gates()
    audited_gate_rows = [
        _reconcile_gate(row) for row in records(recommended_policy.get("recommended_gate_actions"))
    ]
    active_selection_plan = _active_selection_plan(
        source_summary=source_summary,
        active_selection_rule=active_selection_rule,
    )
    summary = _summary(
        source_summary=source_summary,
        audited_gate_rows=audited_gate_rows,
        active_selection_plan=active_selection_plan,
    )
    common = {
        "schema_version": "first_layer_gate_policy_v2_reconciliation.v1",
        "report_type": "first_layer_gate_policy_v2_reconciliation",
        "title": "First-Layer Gate Policy v2 Reconciliation",
        "status": STATUS,
        "task_id": TASK_ID,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": recommended_policy.get("market_regime"),
        "anchor_event": recommended_policy.get("anchor_event"),
        "anchor_date": recommended_policy.get("anchor_date"),
        "requested_start": recommended_policy.get("requested_start"),
        "actual_signal_start": recommended_policy.get("actual_signal_start"),
        "data_quality_status": recommended_policy.get("data_quality_status"),
        "source_artifacts": {
            "recommended_gate_policy": str(recommended_gate_policy_path),
            "gate_ablation_matrix": str(gate_ablation_matrix_path),
            "threshold_sensitivity_report": str(threshold_sensitivity_path),
            "active_selection_rule": str(active_selection_rule_path),
        },
        "source_audit_summary": clean_for_yaml(dict(source_summary)),
        "summary": summary,
        "research_only": True,
        "active_policy_change_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }
    payload = {
        **common,
        "hard_research_gates": hard_research_gates,
        "gate_policy_v2_rows": audited_gate_rows,
        "gate_policy_layers": _gate_policy_layers(
            hard_research_gates=hard_research_gates,
            audited_gate_rows=audited_gate_rows,
        ),
        "threshold_sensitivity_artifact": _threshold_sensitivity_artifact(
            threshold_sensitivity_path=threshold_sensitivity_path,
            threshold_sensitivity=threshold_sensitivity,
        ),
        "slice_tradeoff_summary": _slice_tradeoff_summary(audited_gate_rows),
        "active_selection_rule_audit_plan": active_selection_plan,
        "hard_boundary_gates_excluded_from_performance_waiver": [
            row["gate_id"] for row in hard_research_gates
        ],
        "safety_boundary": _safety_boundary(),
    }

    paths = {
        "recommended_gate_policy_v2": output_root / "recommended_gate_policy_v2.yaml",
        "gate_policy_v2_reconciliation_report": docs_root
        / "gate_policy_v2_reconciliation_report.md",
        "owner_review_gate_semantics": docs_root / "owner_review_gate_semantics.md",
        "active_selection_rule_audit_plan": docs_root / "active_selection_rule_audit_plan.md",
    }
    write_yaml(paths["recommended_gate_policy_v2"], payload)
    write_markdown(
        paths["gate_policy_v2_reconciliation_report"],
        _render_reconciliation_report(payload, paths),
    )
    write_markdown(
        paths["owner_review_gate_semantics"],
        _render_owner_review_semantics(payload),
    )
    write_markdown(
        paths["active_selection_rule_audit_plan"],
        _render_active_selection_plan(payload),
    )
    return clean_for_yaml(
        {
            **payload,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
            "source_gate_ablation_status": gate_ablation.get("status"),
        }
    )


def _hard_research_gates() -> list[dict[str, Any]]:
    return [
        _hard_gate("pit_no_lookahead", "PIT / no-lookahead 校验"),
        _hard_gate("data_quality", "缓存 market / macro data quality gate"),
        _hard_gate("actual_path_only", "必须使用完整 actual-path evidence"),
        _hard_gate("no_broker_action", "broker action 必须保持 none"),
        _hard_gate("owner_approval", "任何 policy activation 前必须 owner approval"),
        _hard_gate("production_boundary", "production boundary 继续关闭"),
    ]


def _hard_gate(gate_id: str, description: str) -> dict[str, Any]:
    return {
        "gate_id": gate_id,
        "description": description,
        "gate_layer": "hard_research_gate",
        "failure_action": "BLOCKED",
        "automatic_waiver_allowed": False,
        "return_improvement_waiver_allowed": False,
        "owner_review_can_override_candidate_failure": False,
    }


def _reconcile_gate(row: Mapping[str, Any]) -> dict[str, Any]:
    gate_id = str(row.get("gate_id"))
    base = {
        "gate_id": gate_id,
        "source_gate_marginal_utility": row.get("gate_marginal_utility"),
        "source_opportunity_cost": row.get("opportunity_cost"),
        "source_threshold_stability": row.get("threshold_stability"),
        "source_recommended_action": row.get("recommended_action"),
        "source_evidence_status": row.get("evidence_status"),
        "automatic_promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }
    if gate_id == "no_major_regression_in_defensive_probe":
        return {
            **base,
            "gate_layer": "strong_performance_gate",
            "v2_recommended_action": "keep_as_strong_performance_gate",
            "failure_action": "BLOCKED",
            "binary_block_allowed": True,
            "return_improvement_waiver_allowed": False,
            "offline_review_allowed_after_failure": False,
            "tradeoff_explanation": (
                "Defensive-probe regression 会直接削弱 downside-control 质量，"
                "因此收益改善不能自动豁免该 gate。"
            ),
        }
    if gate_id == "not_2023_plus_only":
        return {
            **base,
            "gate_layer": "owner_review_risk_flag",
            "v2_recommended_action": "move_to_owner_review_required",
            "failure_action": "OWNER_REVIEW_REQUIRED",
            "binary_block_allowed": False,
            "risk_flag_retained": True,
            "offline_review_allowed_after_failure": True,
            "owner_review_required": True,
            "tradeoff_explanation": (
                "TRADING-2274 显示该 gate 阻断了 highest utility actual-path "
                "candidate；它保留为 2023+ dependence risk note，但不再是自动拒绝规则。"
            ),
        }
    if gate_id in {"not_beta_dependency", "not_tqqq_dependency"}:
        return {
            **base,
            "gate_layer": "inconclusive_diagnostic_gate",
            "v2_recommended_action": "retain_as_diagnostic_attribution",
            "failure_action": "DIAGNOSTIC_ONLY",
            "binary_block_allowed": False,
            "candidate_level_evidence_required": True,
            "exposure_attribution_required": True,
            "exposure_attribution_fields": [
                "avg_qqq_equivalent_exposure",
                "avg_tqqq_weight",
                "max_tqqq_weight",
                "tqqq_beta_share",
                "qqq_beta_dependency_suspected",
                "tqqq_beta_dependency_suspected",
            ],
            "tradeoff_explanation": (
                "当前 dependency evidence 仍是 lane-level diagnostic evidence，"
                "不是 candidate-level 一票否决 proof。"
            ),
        }
    if gate_id in {"probability_threshold_0_55", "probability_threshold_0_60"}:
        return {
            **base,
            "gate_layer": "threshold_sensitivity_gate",
            "v2_recommended_action": "move_to_threshold_sensitivity",
            "failure_action": "THRESHOLD_SENSITIVITY_ONLY",
            "binary_block_allowed": False,
            "calibration_required": True,
            "hard_threshold_allowed": False,
            "tradeoff_explanation": (
                "0.55 / 0.60 probability thresholds 缺少 candidate-level probability "
                "distribution evidence，应进入 calibration 和 threshold sensitivity reporting。"
            ),
        }
    if gate_id in {"all_slices_not_worse", "no_slice_regression"}:
        return _slice_review_gate(base, gate_id)
    if gate_id == "2022_slice_not_worse_than_flat_reference":
        row_out = _slice_review_gate(base, gate_id)
        row_out["v2_recommended_action"] = "retain_as_slice_review_gate"
        return row_out
    if gate_id in {"actual_path_improved_probe_count_min", "net_of_cost_not_worse"}:
        return {
            **base,
            "gate_layer": "score_penalty_gate",
            "v2_recommended_action": "convert_to_score_penalty",
            "failure_action": "SCORE_PENALTY",
            "binary_block_allowed": False,
            "tradeoff_explanation": (
                "TRADING-2274 未证明该 gate 有硬阻断边际收益；保留为 ranking pressure，"
                "不作为自动拒绝。"
            ),
        }
    return {
        **base,
        "gate_layer": "review_gate",
        "v2_recommended_action": "owner_review_required",
        "failure_action": "OWNER_REVIEW_REQUIRED",
        "binary_block_allowed": False,
    }


def _slice_review_gate(base: Mapping[str, Any], gate_id: str) -> dict[str, Any]:
    return {
        **dict(base),
        "gate_layer": "slice_review_gate",
        "v2_recommended_action": "convert_to_slice_review_gate",
        "failure_action": "OWNER_REVIEW_REQUIRED_OR_BLOCKED_BY_SEVERITY",
        "binary_block_allowed": False,
        "severe_slice_regression_action": "BLOCKED",
        "minor_slice_regression_action": "OWNER_REVIEW_REQUIRED",
        "slice_tradeoff_summary_required": True,
        "tradeoff_explanation": (
            f"{gate_id} 需要先区分 severe slice regression 与轻微 return / "
            "drawdown tradeoff，再决定是否拒绝候选。"
        ),
    }


def _active_selection_plan(
    *,
    source_summary: Mapping[str, Any],
    active_selection_rule: Mapping[str, Any],
) -> dict[str, Any]:
    current_accept_count = int(
        source_summary.get("active_selection_rule_current_accept_count") or 0
    )
    return {
        "task_id": "TRADING-2276_FIRST_LAYER_ACTIVE_SELECTION_RULE_AUDIT",
        "current_active_selection_accept_count": current_accept_count,
        "gate_policy_v2_auto_promotion_allowed": False,
        "active_selection_policy_change_allowed_in_2275": False,
        "next_required_action": "run_active_selection_rule_ablation",
        "ablation_modes": [
            "no_active_selection",
            "relaxed_active_selection",
            "current_active_selection",
            "strict_active_selection",
        ],
        "comparison_metrics": [
            "accepted_candidate_count",
            "owner_review_required_count",
            "rejected_candidate_counterfactual_utility",
            "best_rejected_candidate_utility",
            "false_risk_on_delta",
            "false_risk_off_delta",
            "drawdown_delta",
            "turnover_delta",
            "benchmark_consistency_delta",
        ],
        "current_selection_conditions": mapping(active_selection_rule.get("selection_conditions")),
        "required_outputs": [
            "active_selection_rule_audit_report.md",
            "active_selection_ablation_matrix.json",
            "active_selection_recommended_policy.yaml",
        ],
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _summary(
    *,
    source_summary: Mapping[str, Any],
    audited_gate_rows: Sequence[Mapping[str, Any]],
    active_selection_plan: Mapping[str, Any],
) -> dict[str, Any]:
    rows_by_layer: dict[str, int] = {}
    for row in audited_gate_rows:
        layer = str(row.get("gate_layer"))
        rows_by_layer[layer] = rows_by_layer.get(layer, 0) + 1
    return {
        "task_id": TASK_ID,
        "source_task_id": source_summary.get("task_id"),
        "candidate_count_before_gate": source_summary.get("candidate_count_before_gate"),
        "candidate_count_after_current_performance_gates": source_summary.get(
            "candidate_count_after_current_performance_gates"
        ),
        "active_selection_rule_current_accept_count": active_selection_plan.get(
            "current_active_selection_accept_count"
        ),
        "gate_policy_v2_auto_promotion_allowed": False,
        "layer_counts": rows_by_layer,
        "next_required_tasks": [
            "TRADING-2276_FIRST_LAYER_ACTIVE_SELECTION_RULE_AUDIT",
            "TRADING-2277_GATE_POLICY_V2_CHALLENGER_RERUN",
        ],
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _gate_policy_layers(
    *,
    hard_research_gates: Sequence[Mapping[str, Any]],
    audited_gate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[str]]:
    layers: dict[str, list[str]] = {
        "hard_research_gate": [str(row["gate_id"]) for row in hard_research_gates]
    }
    for row in audited_gate_rows:
        layer = str(row.get("gate_layer"))
        layers.setdefault(layer, []).append(str(row.get("gate_id")))
    return layers


def _threshold_sensitivity_artifact(
    *,
    threshold_sensitivity_path: Path,
    threshold_sensitivity: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_path": str(threshold_sensitivity_path),
        "role": "calibration_and_threshold_sensitivity_not_hard_gate",
        "probability_thresholds": ["0.55", "0.60"],
        "source_status": threshold_sensitivity.get("status"),
        "candidate_probability_distribution_available": False,
        "next_required_action": "extend_candidate_level_probability_calibration",
    }


def _slice_tradeoff_summary(
    audited_gate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    slice_rows = [
        row for row in audited_gate_rows if str(row.get("gate_layer")) == "slice_review_gate"
    ]
    return {
        "slice_gate_ids": [str(row.get("gate_id")) for row in slice_rows],
        "severe_slice_regression_action": "BLOCKED",
        "minor_slice_regression_action": "OWNER_REVIEW_REQUIRED",
        "required_summary_fields": [
            "slice_id",
            "return_delta",
            "drawdown_delta",
            "sharpe_or_calmar_delta",
            "turnover_delta",
            "candidate_level_actual_path_status",
            "owner_tradeoff_note",
        ],
        "promotion_allowed": False,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "research_only": True,
        "active_policy_change_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }


def _render_reconciliation_report(
    payload: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        "# First-Layer Gate Policy v2 Reconciliation",
        "",
        "## 摘要",
        "",
        f"- task_id: `{payload.get('task_id')}`; status: `{payload.get('status')}`",
        (
            "- current active selection accept count: "
            f"`{summary.get('active_selection_rule_current_accept_count')}`; "
            "gate policy v2 不允许自动 promotion。"
        ),
        (
            "- promotion_allowed=`false`, paper_shadow_allowed=`false`, "
            "production_allowed=`false`, broker_action=`none`."
        ),
        "",
        "## Gate Policy v2 Rows",
        "",
        "| gate | layer | failure_action | binary_block | v2_action |",
        "|---|---|---|---|---|",
    ]
    for row in records(payload.get("gate_policy_v2_rows")):
        lines.append(
            f"|`{row['gate_id']}`|`{row['gate_layer']}`|"
            f"`{row['failure_action']}`|`{row.get('binary_block_allowed')}`|"
            f"`{row['v2_recommended_action']}`|"
        )
    lines.extend(["", "## Hard Research Gates", ""])
    for row in records(payload.get("hard_research_gates")):
        lines.append(f"- `{row['gate_id']}`: {row['description']}")
    lines.extend(["", "## 产物", ""])
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)


def _render_owner_review_semantics(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Owner Review Gate 语义",
        "",
        "| gate | layer | owner review meaning | tradeoff explanation |",
        "|---|---|---|---|",
    ]
    for row in records(payload.get("gate_policy_v2_rows")):
        if row.get("failure_action") not in {
            "OWNER_REVIEW_REQUIRED",
            "OWNER_REVIEW_REQUIRED_OR_BLOCKED_BY_SEVERITY",
            "DIAGNOSTIC_ONLY",
            "THRESHOLD_SENSITIVITY_ONLY",
        }:
            continue
        lines.append(
            f"|`{row['gate_id']}`|`{row['gate_layer']}`|"
            f"`{row['failure_action']}`|{row.get('tradeoff_explanation', '')}|"
        )
    lines.extend(
        [
            "",
            "Owner review 只允许候选进入 offline review / owner review 状态；不允许自动 "
            "promotion、paper-shadow、production 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_active_selection_plan(payload: Mapping[str, Any]) -> str:
    plan = mapping(payload.get("active_selection_rule_audit_plan"))
    lines = [
        "# Active Selection Rule 审计计划",
        "",
        f"- task_id: `{plan.get('task_id')}`",
        (
            "- current_active_selection_accept_count: "
            f"`{plan.get('current_active_selection_accept_count')}`"
        ),
        "- gate policy v2 放行或 owner-review state 不等于 promotion allowed。",
        "",
        "## Ablation Modes",
        "",
    ]
    for mode in strings(plan.get("ablation_modes")):
        lines.append(f"- `{mode}`")
    lines.extend(["", "## Metrics", ""])
    for metric in strings(plan.get("comparison_metrics")):
        lines.append(f"- `{metric}`")
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`.",
            "",
        ]
    )
    return "\n".join(lines)
