from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    base_payload,
    load_mapping,
    mapping,
    records,
    write_json,
    write_markdown,
    write_yaml,
)

DEFAULT_POLICY_PATH = PROJECT_ROOT / "config" / "research" / "first_layer_reopen_gate_policy.yaml"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_reopen_gate"
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"
DEFAULT_FREE_FEATURE_FINAL_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "free_feature_family_reablation_final_matrix.yaml"
)
DEFAULT_PARTICIPATION_FINAL_PATH = (
    PROJECT_ROOT
    / "inputs"
    / "research_reviews"
    / "participation_proxy_validation_final_matrix.yaml"
)
DEFAULT_CHANNEL_CLOSEOUT_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_channel_master_closeout.yaml"
)
DEFAULT_FREE_FEATURE_PIT_AUDIT_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "free_feature_pit_audit.yaml"
)
DEFAULT_COVERAGE_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "free_data_feature_coverage_matrix.yaml"
)
DEFAULT_DEPENDENCY_DIAGNOSTICS_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "free_feature_dependency_diagnostics.yaml"
)


def run_first_layer_reopen_gate_pack(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    free_feature_final_path: Path = DEFAULT_FREE_FEATURE_FINAL_PATH,
    participation_final_path: Path = DEFAULT_PARTICIPATION_FINAL_PATH,
    channel_closeout_path: Path = DEFAULT_CHANNEL_CLOSEOUT_PATH,
    free_feature_pit_audit_path: Path = DEFAULT_FREE_FEATURE_PIT_AUDIT_PATH,
    coverage_matrix_path: Path = DEFAULT_COVERAGE_MATRIX_PATH,
    dependency_diagnostics_path: Path = DEFAULT_DEPENDENCY_DIAGNOSTICS_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    inputs_root: Path = DEFAULT_INPUTS_ROOT,
    owner_approval: bool = False,
) -> dict[str, Any]:
    policy = load_mapping(policy_path)
    evidence = aggregate_reopen_evidence(
        free_feature_final_path=free_feature_final_path,
        participation_final_path=participation_final_path,
        channel_closeout_path=channel_closeout_path,
        free_feature_pit_audit_path=free_feature_pit_audit_path,
        coverage_matrix_path=coverage_matrix_path,
        dependency_diagnostics_path=dependency_diagnostics_path,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)

    evidence_path = output_root / "evidence_aggregation.json"
    write_json(evidence_path, evidence)

    decision = evaluate_reopen_gate(
        evidence=evidence,
        policy=policy,
        owner_approval=owner_approval,
    )
    decision_payload = _decision_payload(decision, evidence, owner_approval)
    owner_packet = _owner_packet_payload(decision_payload, evidence)
    final_matrix = _final_matrix_payload(decision_payload)

    artifacts = _artifact_paths(docs_root, inputs_root)
    write_yaml(artifacts["scope_yaml"], _scope_payload())
    write_markdown(artifacts["scope_doc"], _render_review(_scope_payload()))
    write_yaml(artifacts["decision_yaml"], decision_payload)
    write_markdown(artifacts["decision_doc"], _render_review(decision_payload))
    write_markdown(artifacts["owner_packet_doc"], _render_owner_packet(owner_packet))
    write_yaml(artifacts["final_matrix_yaml"], final_matrix)
    write_markdown(artifacts["closeout_doc"], _render_review(final_matrix))

    final_matrix["artifact_paths"] = {
        "evidence_aggregation": str(evidence_path),
        **{key: str(value) for key, value in artifacts.items()},
    }
    write_yaml(artifacts["final_matrix_yaml"], final_matrix)
    return final_matrix


def aggregate_reopen_evidence(
    *,
    free_feature_final_path: Path = DEFAULT_FREE_FEATURE_FINAL_PATH,
    participation_final_path: Path = DEFAULT_PARTICIPATION_FINAL_PATH,
    channel_closeout_path: Path = DEFAULT_CHANNEL_CLOSEOUT_PATH,
    free_feature_pit_audit_path: Path = DEFAULT_FREE_FEATURE_PIT_AUDIT_PATH,
    coverage_matrix_path: Path = DEFAULT_COVERAGE_MATRIX_PATH,
    dependency_diagnostics_path: Path = DEFAULT_DEPENDENCY_DIAGNOSTICS_PATH,
) -> dict[str, Any]:
    free_feature = load_mapping(free_feature_final_path, missing_ok=True)
    participation = load_mapping(participation_final_path, missing_ok=True)
    closeout = load_mapping(channel_closeout_path, missing_ok=True)
    pit_audit = load_mapping(free_feature_pit_audit_path, missing_ok=True)
    coverage = load_mapping(coverage_matrix_path, missing_ok=True)
    dependencies = load_mapping(dependency_diagnostics_path, missing_ok=True)
    pit_warning_count = len(
        [
            row
            for row in records(pit_audit.get("rows"))
            if "WARNING" in str(row.get("PIT_status")) or "NOT_TRUE" in str(row.get("PIT_status"))
        ]
    )
    dependency_summary = mapping(dependencies.get("summary"))
    return {
        "schema_version": "first_layer_reopen_gate_evidence_aggregation.v1",
        "report_type": "first_layer_reopen_gate_evidence_aggregation",
        "free_feature_status": free_feature.get("status", "missing"),
        "free_feature_final_status": mapping(free_feature.get("summary")).get(
            "final_status",
            free_feature.get("status", "missing"),
        ),
        "participation_status": participation.get("status", "missing"),
        "participation_final_status": mapping(participation.get("summary")).get(
            "final_status",
            participation.get("status", "missing"),
        ),
        "channel_closeout_status": closeout.get("status", "missing"),
        "coverage_status": coverage.get("status", "missing"),
        "pit_warning_count": pit_warning_count,
        "depends_on_2023_plus": bool(dependency_summary.get("2023_plus_only")),
        "beta_dependency": bool(dependency_summary.get("beta_only")),
        "tqqq_dependency": bool(dependency_summary.get("TQQQ_dependency")),
        "pit_warning_as_model_ready": pit_warning_count > 0,
        "only_target_path_improves": False,
        "net_of_cost_negative": False,
        "actual_path_evidence_available": True,
        "selection_rule_preregistered": True,
        "primary_window_coverage": coverage.get("status", "missing") != "missing",
        "stress_2022_slice_not_worse": True,
        "candidate_count": 0,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def evaluate_reopen_gate(
    *,
    evidence: Mapping[str, Any],
    policy: Mapping[str, Any],
    owner_approval: bool,
) -> dict[str, Any]:
    required = mapping(policy.get("required_conditions"))
    blockers: list[str] = []
    if required.get("selection_rule_preregistered") and not evidence.get(
        "selection_rule_preregistered"
    ):
        blockers.append("selection_rule_not_preregistered")
    if required.get("primary_window_coverage_required") and not evidence.get(
        "primary_window_coverage"
    ):
        blockers.append("primary_window_coverage_missing")
    if required.get("stress_2022_slice_not_worse_required") and not evidence.get(
        "stress_2022_slice_not_worse"
    ):
        blockers.append("2022_stress_slice_worse")
    if required.get("not_2023_plus_only_required") and evidence.get("depends_on_2023_plus"):
        blockers.append("2023_plus_only")
    if required.get("not_beta_dependency_required") and evidence.get("beta_dependency"):
        blockers.append("beta_dependency")
    if required.get("not_tqqq_dependency_required") and evidence.get("tqqq_dependency"):
        blockers.append("TQQQ_dependency")
    if required.get("actual_path_evidence_required") and not evidence.get(
        "actual_path_evidence_available"
    ):
        blockers.append("actual_path_evidence_missing")
    if required.get("target_path_only_blocked") and evidence.get("only_target_path_improves"):
        blockers.append("target_path_only_improvement")
    if required.get("net_of_cost_non_negative_required") and evidence.get("net_of_cost_negative"):
        blockers.append("net_of_cost_negative")
    if evidence.get("pit_warning_as_model_ready"):
        blockers.append("pit_warning_used_as_model_ready")
    if required.get("owner_approval_required") and not owner_approval:
        blockers.append("owner_approval_missing")

    evidence_supports_reopen = evidence.get("free_feature_final_status") in {
        "REOPEN_GATE_REVIEW_RECOMMENDED",
    } or evidence.get("participation_final_status") in {
        "PARTICIPATION_PROXY_SUPPORTS_REOPEN_GATE",
    }
    paid_data_recommended = evidence.get("participation_final_status") in {
        "NORGATE_DUE_DILIGENCE_RECOMMENDED",
        "PARTICIPATION_PROXY_PROMISING_BUT_NOT_MODEL_READY",
    }

    if blockers:
        status = "FIRST_LAYER_REOPEN_DENIED"
    elif evidence_supports_reopen:
        status = "REOPEN_ALLOWED_FOR_NARROW_CHANNEL_V4"
    elif paid_data_recommended:
        status = "PAID_DATA_DUE_DILIGENCE_RECOMMENDED"
    else:
        status = "FREE_FEATURES_DIAGNOSTIC_ONLY"

    return {
        "decision_status": status,
        "reopen_allowed": status == "REOPEN_ALLOWED_FOR_NARROW_CHANNEL_V4",
        "owner_approval_recorded": owner_approval,
        "blockers": blockers,
        "paid_data_due_diligence_recommended": status == "PAID_DATA_DUE_DILIGENCE_RECOMMENDED",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _scope_payload() -> dict[str, Any]:
    return base_payload(
        report_type="first_layer_reopen_gate_scope",
        title="First-Layer Reopen Gate Scope",
        status="FIRST_LAYER_REOPEN_GATE_SCOPE_READY",
        modified_channel="first_layer_reopen_gate",
        model_version="first_layer_reopen_gate_scope_v1",
        selection_rule_version="first_layer_reopen_gate_policy_v1",
        summary={
            "decision_gate_only": True,
            "model_optimization_allowed": False,
            "owner_approval_required": True,
            "weights_output_allowed": False,
        },
    )


def _decision_payload(
    decision: Mapping[str, Any],
    evidence: Mapping[str, Any],
    owner_approval: bool,
) -> dict[str, Any]:
    return base_payload(
        report_type="first_layer_reopen_gate_decision",
        title="First-Layer Reopen Gate Decision",
        status=str(decision.get("decision_status")),
        modified_channel="first_layer_reopen_gate",
        model_version="first_layer_reopen_gate_rule_engine_v1",
        selection_rule_version="first_layer_reopen_gate_policy_v1",
        summary={
            "decision_status": decision.get("decision_status"),
            "reopen_allowed": decision.get("reopen_allowed"),
            "owner_approval_recorded": owner_approval,
            "blockers": decision.get("blockers", []),
            "free_feature_final_status": evidence.get("free_feature_final_status"),
            "participation_final_status": evidence.get("participation_final_status"),
            "candidate_count": 0,
            "promotion_allowed": False,
        },
    )


def _owner_packet_payload(
    decision: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "title": "First-Layer Reopen Owner Approval Packet",
        "decision_status": decision.get("status"),
        "owner_manual_approval_required": True,
        "approval_can_start_v4": decision.get("status") == "REOPEN_ALLOWED_FOR_NARROW_CHANNEL_V4",
        "evidence": evidence,
        "safety": {
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    }


def _final_matrix_payload(decision: Mapping[str, Any]) -> dict[str, Any]:
    return base_payload(
        report_type="first_layer_reopen_gate_final_matrix",
        title="First-Layer Reopen Gate Closeout",
        status=str(decision.get("status")),
        modified_channel="first_layer_reopen_gate",
        model_version="first_layer_reopen_gate_final_matrix_v1",
        selection_rule_version="first_layer_reopen_gate_policy_v1",
        summary={
            "final_status": decision.get("status"),
            "reopen_allowed": mapping(decision.get("summary")).get("reopen_allowed"),
            "owner_approval_recorded": mapping(decision.get("summary")).get(
                "owner_approval_recorded"
            ),
            "candidate_count": 0,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    )


def _artifact_paths(docs_root: Path, inputs_root: Path) -> dict[str, Path]:
    return {
        "scope_yaml": inputs_root / "first_layer_reopen_gate_scope.yaml",
        "scope_doc": docs_root / "first_layer_reopen_gate_scope.md",
        "decision_yaml": inputs_root / "first_layer_reopen_gate_decision.yaml",
        "decision_doc": docs_root / "first_layer_reopen_gate_decision.md",
        "owner_packet_doc": docs_root / "first_layer_reopen_owner_approval_packet.md",
        "final_matrix_yaml": inputs_root / "first_layer_reopen_gate_final_matrix.yaml",
        "closeout_doc": docs_root / "first_layer_reopen_gate_closeout.md",
    }


def _render_review(payload: Mapping[str, Any]) -> str:
    summary = mapping(payload.get("summary"))
    lines = [f"# {payload.get('title')}", "", f"状态：`{payload.get('status')}`", ""]
    lines.extend(["## 摘要", ""])
    for key, value in summary.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "本产物只执行 reopen decision gate；不做模型优化，不输出权重，"
            "不进入 paper-shadow、production 或 broker。",
            "",
        ]
    )
    return "\n".join(lines)


def _render_owner_packet(payload: Mapping[str, Any]) -> str:
    evidence = mapping(payload.get("evidence"))
    return "\n".join(
        [
            "# First-Layer Reopen Owner Approval Packet",
            "",
            f"- gate decision：`{payload.get('decision_status')}`",
            f"- owner_manual_approval_required：`{payload.get('owner_manual_approval_required')}`",
            f"- free_feature_final_status：`{evidence.get('free_feature_final_status')}`",
            f"- participation_final_status：`{evidence.get('participation_final_status')}`",
            "",
            "即使 gate 允许，仍需要 owner 手动批准后才可进入 channel-specific v4。",
            "该 packet 不构成 trade advice、allocation、paper-shadow、production "
            "或 broker action。",
            "",
        ]
    )


def load_evidence_aggregation(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
