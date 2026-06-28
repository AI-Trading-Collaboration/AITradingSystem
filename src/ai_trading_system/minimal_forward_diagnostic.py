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

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "minimal_forward_diagnostic_policy.yaml"
)
DEFAULT_SCHEMA_PATH = (
    PROJECT_ROOT / "config" / "research" / "minimal_forward_diagnostic_log_schema.yaml"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_INPUTS_ROOT = PROJECT_ROOT / "inputs" / "research_reviews"

FORBIDDEN_FIELDS = {
    "target_weights",
    "trade_action",
    "recommended_allocation",
    "broker_action",
    "paper_shadow_position",
}


def run_minimal_forward_diagnostic_pack(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    schema_path: Path = DEFAULT_SCHEMA_PATH,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    inputs_root: Path = DEFAULT_INPUTS_ROOT,
) -> dict[str, Any]:
    policy = load_mapping(policy_path)
    schema = load_mapping(schema_path)
    validation = validate_minimal_forward_diagnostic_policy(policy, schema)
    docs_root.mkdir(parents=True, exist_ok=True)
    inputs_root.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(docs_root, inputs_root)

    scope = _scope_payload(validation)
    final = _final_matrix_payload(validation)
    write_markdown(artifacts["scope_doc"], _render_review(scope))
    write_yaml(artifacts["final_matrix_yaml"], final)
    write_markdown(artifacts["closeout_doc"], _render_review(final))
    final["artifact_paths"] = {
        "policy": str(policy_path),
        "schema": str(schema_path),
        **{key: str(value) for key, value in artifacts.items()},
    }
    write_yaml(artifacts["final_matrix_yaml"], final)
    return final


def run_minimal_forward_diagnostic_outcome_backfill(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    schema_path: Path = DEFAULT_SCHEMA_PATH,
    inputs_root: Path = DEFAULT_INPUTS_ROOT,
) -> dict[str, Any]:
    policy = load_mapping(policy_path)
    schema = load_mapping(schema_path)
    validation = validate_minimal_forward_diagnostic_policy(policy, schema)
    enabled = bool(policy.get("enabled"))
    status = (
        "MINIMAL_FORWARD_DIAGNOSTIC_OUTCOME_BACKFILL_READY"
        if enabled and validation["status"] == "PASS"
        else "MINIMAL_FORWARD_DIAGNOSTIC_REQUIRES_OWNER_APPROVAL"
    )
    payload = base_payload(
        report_type="minimal_forward_diagnostic_outcome_backfill",
        title="Minimal Forward Diagnostic Outcome Backfill",
        status=status,
        modified_channel="minimal_forward_diagnostic",
        model_version="minimal_forward_diagnostic_outcome_backfill_v1",
        selection_rule_version="minimal_forward_diagnostic_policy_v1",
        summary={
            "enabled": enabled,
            "policy_status": validation["status"],
            "outcome_backfill_performed": False,
            "reason": "policy disabled or owner approval missing" if not enabled else "ready",
        },
    )
    path = inputs_root / "minimal_forward_diagnostic_outcome_backfill.yaml"
    write_yaml(path, payload)
    payload["artifact_paths"] = {"outcome_backfill_yaml": str(path)}
    write_yaml(path, payload)
    return payload


def validate_minimal_forward_diagnostic_policy(
    policy: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> dict[str, Any]:
    issues: list[str] = []
    if bool(policy.get("enabled")):
        issues.append("policy_enabled_without_owner_activation")
    if not bool(policy.get("requires_owner_approval")):
        issues.append("owner_approval_not_required")
    blocked = set(str(item) for item in policy.get("blocked_fields", []))
    forbidden = set(str(item) for item in schema.get("forbidden_fields", []))
    allowed = set(str(item) for item in policy.get("allowed_fields", []))
    if not FORBIDDEN_FIELDS <= blocked:
        issues.append("policy_missing_blocked_fields")
    if not FORBIDDEN_FIELDS <= forbidden:
        issues.append("schema_missing_forbidden_fields")
    if FORBIDDEN_FIELDS & allowed:
        issues.append("allowed_fields_include_forbidden_fields")
    for flag in (
        "emit_weights",
        "emit_trade_advice",
        "paper_shadow_enabled",
        "promotion_enabled",
        "production_enabled",
        "broker_enabled",
    ):
        if bool(policy.get(flag)):
            issues.append(f"{flag}_must_be_false")
    return {
        "status": "PASS" if not issues else "FAIL",
        "issues": issues,
        "enabled": bool(policy.get("enabled")),
        "requires_owner_approval": bool(policy.get("requires_owner_approval")),
    }


def _scope_payload(validation: Mapping[str, Any]) -> dict[str, Any]:
    return base_payload(
        report_type="minimal_forward_diagnostic_scope",
        title="Minimal Forward Diagnostic Scope",
        status="MINIMAL_FORWARD_DIAGNOSTIC_DISABLED_READY"
        if validation.get("status") == "PASS"
        else "MINIMAL_FORWARD_DIAGNOSTIC_POLICY_INVALID",
        modified_channel="minimal_forward_diagnostic",
        model_version="minimal_forward_diagnostic_scope_v1",
        selection_rule_version="minimal_forward_diagnostic_policy_v1",
        summary={
            "enabled": validation.get("enabled"),
            "requires_owner_approval": validation.get("requires_owner_approval"),
            "emit_weights": False,
            "emit_trade_advice": False,
            "paper_shadow_enabled": False,
            "promotion_enabled": False,
            "production_enabled": False,
            "broker_enabled": False,
            "policy_issues": validation.get("issues", []),
        },
    )


def _final_matrix_payload(validation: Mapping[str, Any]) -> dict[str, Any]:
    final_status = (
        "MINIMAL_FORWARD_DIAGNOSTIC_DISABLED_READY"
        if validation.get("status") == "PASS"
        else "MINIMAL_FORWARD_DIAGNOSTIC_REQUIRES_OWNER_APPROVAL"
    )
    return base_payload(
        report_type="minimal_forward_diagnostic_final_matrix",
        title="Minimal Forward Diagnostic Closeout",
        status=final_status,
        modified_channel="minimal_forward_diagnostic",
        model_version="minimal_forward_diagnostic_final_matrix_v1",
        selection_rule_version="minimal_forward_diagnostic_policy_v1",
        summary={
            "final_status": final_status,
            "enabled": validation.get("enabled"),
            "requires_owner_approval": validation.get("requires_owner_approval"),
            "candidate_count": 0,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    )


def _artifact_paths(docs_root: Path, inputs_root: Path) -> dict[str, Path]:
    return {
        "scope_doc": docs_root / "minimal_forward_diagnostic_scope.md",
        "final_matrix_yaml": inputs_root / "minimal_forward_diagnostic_final_matrix.yaml",
        "closeout_doc": docs_root / "minimal_forward_diagnostic_closeout.md",
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
            "Minimal forward diagnostic 默认 disabled；禁止 target weights、trade action、"
            "recommended allocation、paper-shadow position、production 或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)
