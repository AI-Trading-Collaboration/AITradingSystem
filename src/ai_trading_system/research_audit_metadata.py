from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_PRIMARY_RESEARCH_WINDOW_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "primary_research_window_policy.yaml"
)
DEFAULT_RESEARCH_AUDIT_METADATA_SCHEMA_PATH = (
    PROJECT_ROOT / "config" / "research" / "research_audit_metadata_schema.yaml"
)
DEFAULT_WINDOW_AWARE_SELECTION_RULE_TEMPLATES_PATH = (
    PROJECT_ROOT / "config" / "research" / "window_aware_selection_rule_templates.yaml"
)

SAFETY_BOUNDARY: dict[str, Any] = {
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


def load_primary_research_window_policy(
    path: Path = DEFAULT_PRIMARY_RESEARCH_WINDOW_POLICY_PATH,
) -> dict[str, Any]:
    return _load_yaml_mapping(path)


def load_research_audit_metadata_schema(
    path: Path = DEFAULT_RESEARCH_AUDIT_METADATA_SCHEMA_PATH,
) -> dict[str, Any]:
    return _load_yaml_mapping(path)


def load_window_aware_selection_rule_templates(
    path: Path = DEFAULT_WINDOW_AWARE_SELECTION_RULE_TEMPLATES_PATH,
) -> dict[str, Any]:
    return _load_yaml_mapping(path)


def validate_primary_research_window_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    cfg = _mapping(policy.get("primary_research_window_policy"))
    rules = _mapping(cfg.get("rules"))
    windows = _mapping(policy.get("windows"))
    issues: list[dict[str, str]] = []
    if cfg.get("default_primary_window") != "exact_three_asset_validated":
        issues.append(_issue("default_primary_window_not_2021_primary"))
    if str(cfg.get("default_start")) != "2021-02-22":
        issues.append(_issue("default_start_not_2021_02_22"))
    if cfg.get("legacy_window") != "legacy_research_window_2022_12":
        issues.append(_issue("legacy_window_not_registered"))
    if cfg.get("sensitivity_window") != "exact_three_asset_primary_only_extension":
        issues.append(_issue("sensitivity_window_not_registered"))
    if not bool(rules.get("primary_leaderboard_must_use_primary_window")):
        issues.append(_issue("primary_leaderboard_rule_disabled"))
    if not bool(rules.get("legacy_results_comparison_only")):
        issues.append(_issue("legacy_comparison_only_rule_disabled"))
    if not bool(rules.get("sensitivity_results_must_carry_caveat")):
        issues.append(_issue("sensitivity_caveat_rule_disabled"))
    if not bool(rules.get("requested_inception_dates_metadata_only_if_not_common_tradable")):
        issues.append(_issue("requested_inception_metadata_only_rule_disabled"))
    legacy = _mapping(windows.get("legacy_research_window_2022_12"))
    if "promotion_evidence" not in _string_list(legacy.get("blocked_usage")):
        issues.append(_issue("legacy_window_can_unlock_promotion"))
    safety = _mapping(policy.get("safety_boundary"))
    if safety.get("dynamic_promotion_status") != "BLOCKED":
        issues.append(_issue("dynamic_promotion_not_blocked"))
    return {"status": "PASS" if not issues else "FAIL", "issues": issues, **SAFETY_BOUNDARY}


def primary_window_required_for_primary_leaderboard(
    artifact: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> bool:
    cfg = _mapping(policy.get("primary_research_window_policy"))
    role = str(artifact.get("leaderboard_role", artifact.get("evidence_role", "")))
    if role not in {"primary_leaderboard", "PRIMARY_DECISION_EVIDENCE"}:
        return True
    return str(artifact.get("research_window_id")) == str(cfg.get("default_primary_window"))


def legacy_window_results_are_comparison_only(
    artifact: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> bool:
    cfg = _mapping(policy.get("primary_research_window_policy"))
    if str(artifact.get("research_window_id")) != str(cfg.get("legacy_window")):
        return True
    evidence_role = str(artifact.get("evidence_role", ""))
    blocked = bool(artifact.get("promotion_allowed")) is False and bool(
        artifact.get("production_allowed")
    ) is False
    return evidence_role in {
        "LEGACY_COMPARISON_EVIDENCE",
        "LEGACY_WINDOW_ONLY_EVIDENCE",
        "NOT_PRIMARY_DECISION_EVIDENCE",
        "WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT",
    } and blocked


def sensitivity_window_requires_caveat(
    artifact: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> bool:
    cfg = _mapping(policy.get("primary_research_window_policy"))
    if str(artifact.get("research_window_id")) != str(cfg.get("sensitivity_window")):
        return True
    caveats = set(_string_list(artifact.get("caveats")))
    window = _mapping(_mapping(policy.get("windows")).get(str(cfg.get("sensitivity_window"))))
    required = set(_string_list(window.get("required_caveats")))
    return required <= caveats and str(artifact.get("window_role")) == "sensitivity"


def requested_inception_date_not_used_before_common_tradable_date(
    artifact: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> bool:
    cfg = _mapping(policy.get("primary_research_window_policy"))
    if str(artifact.get("research_window_id")) != str(
        cfg.get("requested_inception_metadata_window")
    ):
        return True
    return (
        str(artifact.get("requested_start")) == str(cfg.get("requested_inception_date"))
        and str(artifact.get("actual_portfolio_start"))
        == str(cfg.get("requested_inception_actual_portfolio_start"))
        and str(artifact.get("window_role")) == "metadata_only"
    )


def validate_research_audit_metadata(
    artifact: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> dict[str, Any]:
    cfg = _mapping(schema.get("research_audit_metadata"))
    metadata = _mapping(artifact.get("research_audit_metadata"))
    issues: list[dict[str, str]] = []
    if not metadata:
        issues.append(_issue("research_audit_metadata_missing"))
        return {"status": "FAIL", "issues": issues, **SAFETY_BOUNDARY}
    missing = sorted(set(_string_list(cfg.get("required_fields"))) - set(metadata))
    if missing:
        issues.append(_issue(f"research_audit_metadata_missing_fields:{','.join(missing)}"))
    allowed_layers = set(_string_list(cfg.get("modified_layer_allowed_values")))
    if str(metadata.get("modified_layer")) not in allowed_layers:
        issues.append(_issue("modified_layer_invalid"))
    if _int(metadata.get("candidate_count"), default=-1) < _int(
        _mapping(cfg.get("candidate_count")).get("minimum"), default=0
    ):
        issues.append(_issue("candidate_count_invalid"))
    if not str(metadata.get("pre_registered_selection_rule", "")).strip():
        issues.append(_issue("pre_registered_selection_rule_missing"))
    if metadata.get("research_window_id") != artifact.get("research_window_id"):
        issues.append(_issue("research_window_id_metadata_mismatch"))
    return {"status": "PASS" if not issues else "FAIL", "issues": issues, **SAFETY_BOUNDARY}


def post_1665_artifact_requires_research_audit_metadata(
    artifact: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> bool:
    return validate_research_audit_metadata(artifact, schema)["status"] == "PASS"


def validate_window_aware_selection_rule_templates(
    templates: Mapping[str, Any],
) -> dict[str, Any]:
    issues: list[dict[str, str]] = []
    template_rows = _mapping(templates.get("templates"))
    required_fields = {
        "primary_pass_conditions",
        "legacy_comparison_usage",
        "sensitivity_block_conditions",
        "failure_attribution_labels",
        "forbidden_posthoc_metrics",
    }
    for template_id, template in template_rows.items():
        row = _mapping(template)
        missing = sorted(required_fields - set(row))
        if missing:
            issues.append(_issue(f"{template_id}_missing:{','.join(missing)}"))
        if str(row.get("primary_window_id")) != "exact_three_asset_validated":
            issues.append(_issue(f"{template_id}_primary_window_not_required"))
        forbidden = set(_string_list(row.get("forbidden_posthoc_metrics")))
        if not any("target_path" in item or "target_path_metric" == item for item in forbidden):
            issues.append(_issue(f"{template_id}_target_path_not_forbidden"))
        if "comparison_only" not in _string_list(row.get("legacy_comparison_usage")):
            issues.append(_issue(f"{template_id}_legacy_not_comparison_only"))
    safety = _mapping(templates.get("safety_boundary"))
    if safety.get("dynamic_promotion_status") != "BLOCKED":
        issues.append(_issue("dynamic_promotion_not_blocked"))
    return {"status": "PASS" if not issues else "FAIL", "issues": issues, **SAFETY_BOUNDARY}


def window_extension_reveals_legacy_overfit_blocks_promotion(
    artifact: Mapping[str, Any],
) -> bool:
    status = str(artifact.get("status", ""))
    summary = _mapping(artifact.get("summary"))
    final_status = str(summary.get("final_status", summary.get("base_status", "")))
    return (
        "WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT" in status
        or "WINDOW_EXTENSION_REVEALS_LEGACY_OVERFIT" in final_status
    ) and bool(artifact.get("promotion_allowed")) is False


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"YAML must be a mapping: {path}")
    return dict(raw)


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _string_list(value: object) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _issue(code: str) -> dict[str, str]:
    return {"code": code, "severity": "BLOCKER"}
