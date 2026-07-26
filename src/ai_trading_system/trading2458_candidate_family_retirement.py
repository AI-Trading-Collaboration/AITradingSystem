from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts.writer import canonical_json_bytes
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "trading2458_candidate_family_retirement.v1"
VALIDATION_SCHEMA_VERSION = "trading2458_candidate_family_retirement_validation.v1"
POLICY_SCHEMA_VERSION = "trading2458_candidate_family_retirement_policy.v1"
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "trading2458_candidate_family_retirement_v1.yaml"
)
FROZEN_POLICY_SHA256 = "f60874d43fc97979eea8c2b923fb8627011bd72f55112516bfa556c1a97e1d75"

EXPECTED_OWNER_DECISION = (
    "owner_decision:TRADING-2458:2026-07-25:retire_current_saturated_candidate_family"
)
EXPECTED_PACKAGE_ID = "dynamic-v3-clean-trading2452_11991ac7965cfcd7aa18"
EXPECTED_CANDIDATE_UNIVERSE_ID = "dynamic-v3-clean-universe-trading2452_aa64f915302704bb9224"
EXPECTED_PREREGISTRATION_ID = "research_preregistration_afeba0639611c7f3816f"
EXPECTED_CAMPAIGN_ID = "dynamic-v3-clean-selection-trading2452"
EXPECTED_SELECTION_POLICY_ID = "dynamic_v3_clean_selection_trading2452_fold_local_v2"
EXPECTED_CANDIDATE_DEFINITION_VERSION = (
    "dynamic_v3_clean_selection_trading2452_candidate_definition_v2"
)
EXPECTED_CANDIDATE_COUNT = 300
EXPECTED_TEMPLATE_IDS = (
    "dynamic_regime_overlay_v0_3a_constraint_smooth",
    "dynamic_regime_overlay_v0_3b_drawdown_guarded",
    "dynamic_regime_overlay_v0_3c_constraint_smooth_guarded",
    "dynamic_regime_overlay_v0_3d_emergency_only_guarded",
)
EXPECTED_CANDIDATE_AXES = (
    "rescue_intensity",
    "smooth_window_days",
    "constraint_buffer_bps",
    "turnover_penalty",
    "risk_off_confirmation_days",
    "rebalance_cooldown_days",
    "drawdown_guard",
)
ALLOWED_ACTIONS = (
    "historical_evidence_read",
    "historical_evidence_identity_validation",
    "historical_diagnostic_content_validation",
)
PROHIBITED_ACTIONS = (
    "package_write",
    "package_rebuild_to_disk",
    "historical_evaluator_rerun",
    "candidate_expansion",
    "parameter_search",
    "candidate_selection",
    "watchlist_enrollment",
    "paper_shadow_enrollment",
    "promotion",
    "production_reuse",
    "broker_execution",
)
NEW_FAMILY_REQUIREMENTS = (
    "new_family_id",
    "new_package_id",
    "independent_preregistration",
    "uncontaminated_selection_protocol",
    "new_owner_decision",
)
EXPECTED_ARTIFACT_FILENAMES = (
    "campaign.json",
    "candidate_universe.json",
    "eligibility.json",
    "package_manifest.json",
    "preregistration.json",
    "research_context.json",
    "selection_rule.yaml",
    "source_contract.json",
    "window_catalog.yaml",
)
SAFETY: dict[str, Any] = {
    "research_only": True,
    "manual_review_required": True,
    "current_package_reopened": False,
    "candidate_search_executed": False,
    "prospective_accessed": False,
    "paper_shadow_changed": False,
    "production_effect": "none",
    "broker_action": "none",
}


class Trading2458CandidateFamilyRetirementError(ValueError):
    """Raised when the retired-family boundary cannot be proven or must block."""


def load_candidate_family_retirement_policy(
    path: Path = DEFAULT_POLICY_PATH,
) -> dict[str, Any]:
    if _file_sha256(path) != FROZEN_POLICY_SHA256:
        raise Trading2458CandidateFamilyRetirementError(
            "TRADING-2458 retirement policy fingerprint mismatch"
        )
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping):
        raise Trading2458CandidateFamilyRetirementError("mapping retirement policy required")
    policy = dict(payload)
    _require_exact_keys(
        policy,
        {
            "schema_version",
            "policy_id",
            "version",
            "status",
            "owner",
            "effective_date",
            "owner_decision",
            "rationale",
            "intended_effect",
            "validation_evidence",
            "review_condition",
            "expiry_condition",
            "retired_scope",
            "immutable_package",
            "allowed_actions",
            "prohibited_actions",
            "new_family_requirements",
            "safety",
        },
        "policy",
    )
    expected_identity = {
        "schema_version": POLICY_SCHEMA_VERSION,
        "policy_id": "trading2458_candidate_family_retirement",
        "version": "1.0.0",
        "status": "OWNER_APPROVED_RETIRED",
        "owner": "strategy_research_owner",
        "effective_date": "2026-07-25",
        "owner_decision": EXPECTED_OWNER_DECISION,
    }
    for field, expected in expected_identity.items():
        if policy.get(field) != expected:
            raise Trading2458CandidateFamilyRetirementError(
                f"unexpected retirement policy {field}: {policy.get(field)!r}"
            )
    for field in (
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
        "expiry_condition",
    ):
        if not isinstance(policy[field], str) or not policy[field].strip():
            raise Trading2458CandidateFamilyRetirementError(f"non-empty {field} required")
    if tuple(_strings(policy["allowed_actions"], "allowed_actions")) != ALLOWED_ACTIONS:
        raise Trading2458CandidateFamilyRetirementError("allowed action boundary mismatch")
    if tuple(_strings(policy["prohibited_actions"], "prohibited_actions")) != PROHIBITED_ACTIONS:
        raise Trading2458CandidateFamilyRetirementError("prohibited action boundary mismatch")
    if (
        tuple(_strings(policy["new_family_requirements"], "new_family_requirements"))
        != NEW_FAMILY_REQUIREMENTS
    ):
        raise Trading2458CandidateFamilyRetirementError("new-family requirements mismatch")
    if _mapping(policy["safety"], "safety") != SAFETY:
        raise Trading2458CandidateFamilyRetirementError("retirement safety mismatch")
    _validate_retired_scope(_mapping(policy["retired_scope"], "retired_scope"))
    _validate_immutable_package_policy(_mapping(policy["immutable_package"], "immutable_package"))
    return policy


def build_candidate_family_retirement_record(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    policy = load_candidate_family_retirement_policy(policy_path)
    scope = _mapping(policy["retired_scope"], "retired_scope")
    immutable_package = _mapping(policy["immutable_package"], "immutable_package")
    package_root = _project_path(project_root, immutable_package["root"])
    artifact_hashes = _mapping(immutable_package["artifacts"], "immutable_package.artifacts")
    actual_hashes = {
        filename: _file_sha256(package_root / filename) for filename in EXPECTED_ARTIFACT_FILENAMES
    }
    if actual_hashes != artifact_hashes:
        raise Trading2458CandidateFamilyRetirementError(
            "immutable TRADING-2452 package artifact fingerprint mismatch"
        )
    _validate_package_identity(package_root=package_root, scope=scope)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "trading2458_candidate_family_retirement",
        "status": "PASS",
        "lifecycle_state": "RETIRED",
        "active_consumption_status": "BLOCKED_RETIRED_CANDIDATE_FAMILY",
        "policy": {
            "policy_id": policy["policy_id"],
            "version": policy["version"],
            "sha256": _file_sha256(policy_path),
            "owner": policy["owner"],
            "owner_decision": policy["owner_decision"],
            "effective_date": policy["effective_date"],
        },
        "retired_scope": scope,
        "immutable_package": {
            "root": immutable_package["root"],
            "artifact_count": len(actual_hashes),
            "artifacts": actual_hashes,
            "historical_manifest_eligibility_superseded": True,
            "historical_bytes_modified": False,
        },
        "allowed_actions": list(ALLOWED_ACTIONS),
        "prohibited_actions": list(PROHIBITED_ACTIONS),
        "new_family_requirements": list(NEW_FAMILY_REQUIREMENTS),
        "interpretation": {
            "generic_research_framework_retired": False,
            "equal_risk_qqq_sgov_forward_aging_retired": False,
            "qld_role_limited_instrument_retired": False,
            "new_hypothesis_or_generator_authorized": False,
            "constraint_gate_change_authorized": False,
        },
        "next_responsible_party": (
            "strategy_research_owner_preregister_new_family_or_continue_decision_target_audit"
        ),
        "safety": dict(SAFETY),
        **SAFETY,
    }


def validate_candidate_family_retirement_record(
    record: Mapping[str, Any],
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    try:
        expected = build_candidate_family_retirement_record(
            policy_path=policy_path,
            project_root=project_root,
        )
        checks.append(
            _check(
                "content_rebuilt",
                canonical_json_bytes(dict(record)) == canonical_json_bytes(expected),
            )
        )
        checks.extend(
            [
                _check("retired_state", record.get("lifecycle_state") == "RETIRED"),
                _check(
                    "active_consumption_blocked",
                    record.get("active_consumption_status") == "BLOCKED_RETIRED_CANDIDATE_FAMILY",
                ),
                _check("safety", _mapping(record.get("safety"), "record.safety") == SAFETY),
            ]
        )
    except (Trading2458CandidateFamilyRetirementError, OSError, ValueError) as exc:
        checks.append(_check("content_rebuilt", False, str(exc)))
    passed = bool(checks) and all(item["passed"] for item in checks)
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "report_type": "trading2458_candidate_family_retirement_validation",
        "status": "PASS" if passed else "FAIL",
        "failed_check_count": sum(not item["passed"] for item in checks),
        "checks": checks,
        "production_effect": "none",
        "broker_action": "none",
    }


def candidate_family_action_decision(
    action: str,
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    record = build_candidate_family_retirement_record(
        policy_path=policy_path,
        project_root=project_root,
    )
    if action in ALLOWED_ACTIONS:
        status = "ALLOWED_HISTORICAL_EVIDENCE_ONLY"
        allowed = True
        reason = "action is limited to immutable historical evidence"
    elif action in PROHIBITED_ACTIONS:
        status = "BLOCKED_RETIRED_CANDIDATE_FAMILY"
        allowed = False
        reason = "owner-approved retirement prohibits active consumption"
    else:
        status = "BLOCKED_UNKNOWN_CANDIDATE_FAMILY_ACTION"
        allowed = False
        reason = "unreviewed action is fail-closed"
    return {
        "schema_version": "trading2458_candidate_family_action_decision.v1",
        "status": status,
        "allowed": allowed,
        "action": action,
        "reason": reason,
        "package_id": EXPECTED_PACKAGE_ID,
        "candidate_universe_id": EXPECTED_CANDIDATE_UNIVERSE_ID,
        "policy_id": record["policy"]["policy_id"],
        "policy_version": record["policy"]["version"],
        "owner_decision": EXPECTED_OWNER_DECISION,
        "production_effect": "none",
        "broker_action": "none",
    }


def require_candidate_family_action_allowed(
    action: str,
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    decision = candidate_family_action_decision(
        action,
        policy_path=policy_path,
        project_root=project_root,
    )
    if not decision["allowed"]:
        raise Trading2458CandidateFamilyRetirementError(
            f"{decision['status']}: {action}: {decision['reason']}"
        )
    return decision


def render_candidate_family_retirement_markdown(record: Mapping[str, Any]) -> str:
    validation = validate_candidate_family_retirement_record(record)
    if validation["status"] != "PASS":
        raise Trading2458CandidateFamilyRetirementError(
            "validated retirement record required before rendering"
        )
    scope = _mapping(record["retired_scope"], "retired_scope")
    window = _mapping(scope["research_window"], "retired_scope.research_window")
    lines = [
        "# TRADING-2458 Candidate Family 正式退役记录",
        "",
        f"- 状态：`{record['lifecycle_state']}`",
        f"- 主动消费：`{record['active_consumption_status']}`",
        f"- Owner 决策：`{_mapping(record['policy'], 'policy')['owner_decision']}`",
        f"- Package：`{scope['package_id']}`",
        (
            f"- Candidate universe：`{scope['candidate_universe_id']}`"
            f"（{scope['candidate_count']} 个）"
        ),
        (
            "- 研究窗口：requested/evaluated start "
            f"`{window['requested_start']}` / `{window['evaluated_start']}`，"
            f"evaluated end `{window['evaluated_end']}`"
        ),
        "",
        "## 结论",
        "",
        "当前四个 template、七个 candidate axis 和 300-candidate universe 已正式退役。",
        "旧 manifest 中的历史 eligibility 仅作为 immutable evidence 保留，不再赋予任何主动资格。",
        "",
        "允许的动作仅限：",
        "",
        *[f"- `{action}`" for action in record["allowed_actions"]],
        "",
        "以下动作均 fail closed：",
        "",
        *[f"- `{action}`" for action in record["prohibited_actions"]],
        "",
        "## 边界",
        "",
        "- 不退役通用 research framework 或 `equal_risk_qqq_sgov` forward aging。",
        "- 不改变 QLD 的 role-limited implementation instrument 定位。",
        "- 不授权新 hypothesis/generator、constraint gate、prospective、paper-shadow 或生产行为。",
        "- 新 family 必须使用新 identity、独立预注册、无污染 selection protocol 和新 Owner 决策。",
        "",
        "全过程：`production_effect=none`、`broker_action=none`。",
        "",
    ]
    return "\n".join(lines)


def _validate_retired_scope(scope: Mapping[str, Any]) -> None:
    _require_exact_keys(
        scope,
        {
            "task_id",
            "strategy_family_label",
            "campaign_id",
            "package_id",
            "candidate_universe_id",
            "preregistration_id",
            "selection_policy_id",
            "candidate_definition_version",
            "candidate_count",
            "template_ids",
            "candidate_axes",
            "research_window",
        },
        "retired_scope",
    )
    expected = {
        "task_id": "TRADING-2458",
        "strategy_family_label": "dynamic_v3_rescue",
        "campaign_id": EXPECTED_CAMPAIGN_ID,
        "package_id": EXPECTED_PACKAGE_ID,
        "candidate_universe_id": EXPECTED_CANDIDATE_UNIVERSE_ID,
        "preregistration_id": EXPECTED_PREREGISTRATION_ID,
        "selection_policy_id": EXPECTED_SELECTION_POLICY_ID,
        "candidate_definition_version": EXPECTED_CANDIDATE_DEFINITION_VERSION,
        "candidate_count": EXPECTED_CANDIDATE_COUNT,
    }
    for field, expected_value in expected.items():
        if scope.get(field) != expected_value:
            raise Trading2458CandidateFamilyRetirementError(f"retired scope mismatch: {field}")
    if tuple(_strings(scope["template_ids"], "template_ids")) != EXPECTED_TEMPLATE_IDS:
        raise Trading2458CandidateFamilyRetirementError("retired template scope mismatch")
    if tuple(_strings(scope["candidate_axes"], "candidate_axes")) != EXPECTED_CANDIDATE_AXES:
        raise Trading2458CandidateFamilyRetirementError("retired axis scope mismatch")
    research_window = _mapping(scope["research_window"], "research_window")
    if research_window != {
        "requested_start": "2021-02-22",
        "evaluated_start": "2021-02-22",
        "evaluated_end": "2025-12-31",
        "prospective_untouched_start": "2026-07-22",
    }:
        raise Trading2458CandidateFamilyRetirementError("research window mismatch")


def _validate_immutable_package_policy(package: Mapping[str, Any]) -> None:
    _require_exact_keys(package, {"root", "artifacts"}, "immutable_package")
    if package.get("root") != "inputs/research/trading2452_dynamic_v3_clean_selection":
        raise Trading2458CandidateFamilyRetirementError("immutable package root mismatch")
    artifacts = _mapping(package["artifacts"], "immutable_package.artifacts")
    if tuple(artifacts) != EXPECTED_ARTIFACT_FILENAMES:
        raise Trading2458CandidateFamilyRetirementError("immutable artifact inventory mismatch")
    if any(not _is_sha256(value) for value in artifacts.values()):
        raise Trading2458CandidateFamilyRetirementError("invalid immutable artifact fingerprint")


def _validate_package_identity(*, package_root: Path, scope: Mapping[str, Any]) -> None:
    manifest = _load_json(package_root / "package_manifest.json")
    universe = _load_json(package_root / "candidate_universe.json")
    preregistration = _load_json(package_root / "preregistration.json")
    campaign = _load_json(package_root / "campaign.json")
    source_contract = _load_json(package_root / "source_contract.json")
    eligibility = _load_json(package_root / "eligibility.json")
    selection = safe_load_yaml_path(package_root / "selection_rule.yaml")
    if not isinstance(selection, Mapping):
        raise Trading2458CandidateFamilyRetirementError("mapping selection rule required")
    identity_checks = {
        "package_id": manifest.get("package_id") == scope["package_id"],
        "candidate_universe_id": (
            universe.get("candidate_universe_id") == scope["candidate_universe_id"]
        ),
        "preregistration_id": (
            preregistration.get("preregistration_id") == scope["preregistration_id"]
        ),
        "campaign_id": campaign.get("campaign_id") == scope["campaign_id"],
        "source_contract_package": (
            source_contract.get("candidate_universe_id") == scope["candidate_universe_id"]
            and source_contract.get("preregistration_id") == scope["preregistration_id"]
            and source_contract.get("campaign_id") == scope["campaign_id"]
        ),
        "eligibility_identity": (
            eligibility.get("package_id") == scope["package_id"]
            and eligibility.get("candidate_universe_id") == scope["candidate_universe_id"]
            and eligibility.get("preregistration_id") == scope["preregistration_id"]
            and eligibility.get("campaign_id") == scope["campaign_id"]
        ),
        "selection_policy_id": selection.get("policy_id") == scope["selection_policy_id"],
    }
    failed = [name for name, passed in identity_checks.items() if not passed]
    if failed:
        raise Trading2458CandidateFamilyRetirementError(
            "immutable package identity mismatch: " + ", ".join(failed)
        )
    candidates = _records(universe.get("candidates"), "candidate_universe.candidates")
    if (
        universe.get("candidate_count") != EXPECTED_CANDIDATE_COUNT
        or len(candidates) != EXPECTED_CANDIDATE_COUNT
    ):
        raise Trading2458CandidateFamilyRetirementError("candidate count mismatch")
    if tuple(_strings(universe.get("axis_order"), "candidate_universe.axis_order")) != (
        EXPECTED_CANDIDATE_AXES
    ):
        raise Trading2458CandidateFamilyRetirementError("candidate axis order mismatch")
    candidate_ids = [str(candidate.get("candidate_id", "")) for candidate in candidates]
    if len(set(candidate_ids)) != EXPECTED_CANDIDATE_COUNT or not all(candidate_ids):
        raise Trading2458CandidateFamilyRetirementError("candidate identities are not unique")
    expected_axis_set = set(EXPECTED_CANDIDATE_AXES)
    for candidate in candidates:
        parameters = _mapping(candidate.get("parameters"), "candidate.parameters")
        if (
            candidate.get("strategy_family") != scope["strategy_family_label"]
            or candidate.get("candidate_definition_version")
            != scope["candidate_definition_version"]
            or set(parameters) != expected_axis_set
        ):
            raise Trading2458CandidateFamilyRetirementError("candidate family definition mismatch")


def _project_path(project_root: Path, value: object) -> Path:
    relative = Path(str(value))
    if relative.is_absolute() or ".." in relative.parts:
        raise Trading2458CandidateFamilyRetirementError("project-relative package root required")
    root = project_root.resolve(strict=False)
    resolved = (root / relative).resolve(strict=False)
    if root not in (resolved, *resolved.parents):
        raise Trading2458CandidateFamilyRetirementError("package root escapes project")
    return resolved


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise Trading2458CandidateFamilyRetirementError(f"mapping JSON required: {path}")
    return dict(payload)


def _mapping(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise Trading2458CandidateFamilyRetirementError(f"mapping required: {field}")
    return dict(value)


def _strings(value: object, field: str) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise Trading2458CandidateFamilyRetirementError(f"sequence required: {field}")
    rows = [str(item) for item in value]
    if any(not item for item in rows):
        raise Trading2458CandidateFamilyRetirementError(f"non-empty values required: {field}")
    return rows


def _records(value: object, field: str) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise Trading2458CandidateFamilyRetirementError(f"sequence required: {field}")
    rows = []
    for item in value:
        if not isinstance(item, Mapping):
            raise Trading2458CandidateFamilyRetirementError(f"mapping rows required: {field}")
        rows.append(dict(item))
    return rows


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    actual = set(value)
    if actual != expected:
        raise Trading2458CandidateFamilyRetirementError(
            f"{field} keys mismatch: missing={sorted(expected - actual)}, "
            f"extra={sorted(actual - expected)}"
        )


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _is_sha256(value: object) -> bool:
    text = str(value or "")
    return len(text) == 64 and all(character in "0123456789abcdef" for character in text)


def _check(check_id: str, passed: bool, details: str | None = None) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "passed": bool(passed),
        "details": [] if details is None else [details],
    }


__all__ = [
    "ALLOWED_ACTIONS",
    "DEFAULT_POLICY_PATH",
    "EXPECTED_PACKAGE_ID",
    "PROHIBITED_ACTIONS",
    "Trading2458CandidateFamilyRetirementError",
    "build_candidate_family_retirement_record",
    "candidate_family_action_decision",
    "load_candidate_family_retirement_policy",
    "render_candidate_family_retirement_markdown",
    "require_candidate_family_action_allowed",
    "validate_candidate_family_retirement_record",
]
