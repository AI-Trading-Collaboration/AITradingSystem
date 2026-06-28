from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    base_payload,
    load_mapping,
    mapping,
    write_markdown,
    write_yaml,
)

DEFAULT_FREEZE_CONTRACT_PATH = (
    PROJECT_ROOT / "config" / "research" / "channel_specific_first_layer_v4_freeze_contract.yaml"
)
DEFAULT_GATE_DECISION_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_reopen_gate_decision.yaml"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"
DEFAULT_FEATURE_MATRIX_PATH = (
    PROJECT_ROOT / "data" / "features" / "channel_specific_first_layer_v4_matrix.parquet"
)


def run_channel_specific_first_layer_v4_pack(
    *,
    freeze_contract_path: Path = DEFAULT_FREEZE_CONTRACT_PATH,
    gate_decision_path: Path = DEFAULT_GATE_DECISION_PATH,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    inputs_root: Path = DEFAULT_INPUTS_ROOT,
    owner_approval: bool = False,
) -> dict[str, Any]:
    contract = load_mapping(freeze_contract_path)
    gate = load_mapping(gate_decision_path, missing_ok=True)
    validation = validate_channel_specific_v4_start(
        gate_decision=gate,
        freeze_contract=contract,
        owner_approval=owner_approval,
    )
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(docs_root, inputs_root)

    scope = _scope_payload(validation)
    final = _final_matrix_payload(validation)
    write_markdown(artifacts["scope_doc"], _render_review(scope))
    write_yaml(artifacts["final_matrix_yaml"], final)
    write_markdown(artifacts["closeout_doc"], _render_review(final))
    final["artifact_paths"] = {key: str(value) for key, value in artifacts.items()}
    final["artifact_paths"]["freeze_contract"] = str(freeze_contract_path)
    write_yaml(artifacts["final_matrix_yaml"], final)
    return final


def validate_channel_specific_v4_start(
    *,
    gate_decision: Mapping[str, Any],
    freeze_contract: Mapping[str, Any],
    owner_approval: bool,
) -> dict[str, Any]:
    gate_summary = mapping(gate_decision.get("summary"))
    gate_status = str(gate_decision.get("status", "FIRST_LAYER_REOPEN_DENIED"))
    allowed = gate_status == "REOPEN_ALLOWED_FOR_NARROW_CHANNEL_V4" and owner_approval
    blockers: list[str] = []
    if gate_status != "REOPEN_ALLOWED_FOR_NARROW_CHANNEL_V4":
        blockers.append("reopen_gate_not_allowed")
    if not owner_approval:
        blockers.append("owner_approval_missing")
    if bool(gate_summary.get("promotion_allowed")):
        blockers.append("gate_attempted_to_enable_promotion")
    blocked_outputs = set(str(item) for item in freeze_contract.get("blocked_outputs", []))
    required_blocked = {
        "target_weights",
        "production_weights",
        "recommended_allocation",
        "trade_action",
        "broker_action",
    }
    if not required_blocked <= blocked_outputs:
        blockers.append("freeze_contract_missing_blocked_outputs")
    return {
        "start_allowed": allowed and not blockers,
        "gate_status": gate_status,
        "owner_approval_recorded": owner_approval,
        "blockers": blockers,
        "allowed_models_if_unlocked": freeze_contract.get("allowed_models_if_unlocked", []),
        "feature_matrix_path": str(DEFAULT_FEATURE_MATRIX_PATH),
        "candidate_count": 0,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _scope_payload(validation: Mapping[str, Any]) -> dict[str, Any]:
    return base_payload(
        report_type="channel_specific_first_layer_v4_scope",
        title="Channel-Specific First-Layer v4 Scope",
        status="CHANNEL_SPECIFIC_FIRST_LAYER_V4_SCOPE_FAIL_CLOSED"
        if not validation.get("start_allowed")
        else "CHANNEL_SPECIFIC_FIRST_LAYER_V4_SCOPE_READY",
        modified_channel="channel_specific_first_layer_v4",
        model_version="channel_specific_first_layer_v4_scope_v1",
        selection_rule_version="channel_specific_first_layer_v4_freeze_contract_v1",
        summary={
            "start_allowed": validation.get("start_allowed"),
            "gate_status": validation.get("gate_status"),
            "owner_approval_recorded": validation.get("owner_approval_recorded"),
            "blockers": validation.get("blockers", []),
            "universal_first_layer_allowed": False,
            "TQQQ_allocation_allowed": False,
        },
    )


def _final_matrix_payload(validation: Mapping[str, Any]) -> dict[str, Any]:
    if validation.get("start_allowed"):
        final_status = "CHANNEL_V4_NARROW_SIGNAL_OBSERVE_ONLY"
    else:
        final_status = "CHANNEL_V4_REOPEN_EVIDENCE_INSUFFICIENT"
    return base_payload(
        report_type="channel_specific_first_layer_v4_final_matrix",
        title="Channel-Specific First-Layer v4 Closeout",
        status=final_status,
        modified_channel="channel_specific_first_layer_v4",
        model_version="channel_specific_first_layer_v4_final_matrix_v1",
        selection_rule_version="channel_specific_first_layer_v4_freeze_contract_v1",
        summary={
            "final_status": final_status,
            "start_allowed": validation.get("start_allowed"),
            "blockers": validation.get("blockers", []),
            "candidate_count": 0,
            "promotion": "blocked",
            "paper_shadow": False,
            "production": False,
            "broker": "none",
        },
    )


def _artifact_paths(docs_root: Path, inputs_root: Path) -> dict[str, Path]:
    return {
        "scope_doc": docs_root / "channel_specific_first_layer_v4_scope.md",
        "final_matrix_yaml": inputs_root / "channel_specific_first_layer_v4_final_matrix.yaml",
        "closeout_doc": docs_root / "channel_specific_first_layer_v4_closeout.md",
    }


def _render_review(payload: Mapping[str, Any]) -> str:
    summary = mapping(payload.get("summary"))
    lines = [f"# {payload.get('title')}", "", f"状态：`{payload.get('status')}`", ""]
    lines.append("## 摘要")
    lines.append("")
    for key, value in summary.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "v4 只有在 reopen gate 和 owner approval 同时通过后才允许继续；"
            "当前产物不训练模型、不输出 allocation、不进入 paper-shadow/production/broker。",
            "",
        ]
    )
    return "\n".join(lines)
