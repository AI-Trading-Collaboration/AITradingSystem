from __future__ import annotations

import copy
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "shadow_promotion_apply_result"
RUN_REPORT_TYPE = "shadow_promotion_apply_run"
TASK_ID = "TRADING-018E2"
SOURCE_PREFLIGHT_TASK_ID = "TRADING-018E1"
SOURCE_PROPOSAL_TASK_ID = "TRADING-018D"
MODE = "explicit_approved_apply"
APPROVAL_TYPE = "shadow_promotion_apply"
PROPOSAL_DECISION_REQUIRED = "PROPOSE_FOR_MANUAL_REVIEW"
PRODUCTION_EFFECT_NONE = "none"
PRODUCTION_EFFECT_APPLIED = "profile_updated_only_if_apply_executed"
DEFAULT_TARGET_PROFILE_NAME = "production"
DANGER_FLAG = "--i-understand-this-writes-production"

DECISION_APPLIED = "APPLIED"
DECISION_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
DECISION_APPROVAL_INVALID = "APPROVAL_INVALID"
DECISION_PREFLIGHT_INVALID = "PREFLIGHT_INVALID"
DECISION_DANGER_FLAG_MISSING = "DANGER_FLAG_MISSING"
DECISION_TARGET_PROFILE_CHANGED = "TARGET_PROFILE_CHANGED"
DECISION_TARGET_PROFILE_MISMATCH = "TARGET_PROFILE_MISMATCH"
DECISION_ROLLBACK_SNAPSHOT_FAILED = "ROLLBACK_SNAPSHOT_FAILED"
DECISION_WRITE_FAILED = "WRITE_FAILED"
DECISION_POST_APPLY_VALIDATION_FAILED = "POST_APPLY_VALIDATION_FAILED"
DECISION_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DECISION_ERROR = "ERROR"

APPLY_DECISIONS = {
    DECISION_APPLIED,
    DECISION_INSUFFICIENT_DATA,
    DECISION_APPROVAL_INVALID,
    DECISION_PREFLIGHT_INVALID,
    DECISION_DANGER_FLAG_MISSING,
    DECISION_TARGET_PROFILE_CHANGED,
    DECISION_TARGET_PROFILE_MISMATCH,
    DECISION_ROLLBACK_SNAPSHOT_FAILED,
    DECISION_WRITE_FAILED,
    DECISION_POST_APPLY_VALIDATION_FAILED,
    DECISION_SAFETY_BLOCKED,
    DECISION_ERROR,
}

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"

# These candidates represent project-supported profile weight containers. They are
# schema paths, not tunable investment policy.
WEIGHT_FIELD_CANDIDATES = ("weights", "base_weights", "production_weights", "target_weights")
FORBIDDEN_APPROVAL_FIELDS = {
    "broker",
    "execution",
    "replay",
    "scheduler",
    "risk_limits",
    "api_keys",
    "account",
    "credentials",
}

# Float tolerance is only for JSON/YAML serialization noise; it must not normalize
# subjective allocation differences or silently alias weight keys.
WEIGHT_SUM_TOLERANCE = 0.000001
WEIGHT_MATCH_TOLERANCE = 0.000001


def default_preflight_json_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "preflight"
        / f"shadow_promotion_apply_preflight_{as_of.isoformat()}.json"
    )


def default_apply_approval_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root / "manual_approvals" / f"shadow_promotion_apply_approval_{as_of.isoformat()}.json"
    )


def default_proposal_json_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "proposals"
        / f"shadow_promotion_proposal_{as_of.isoformat()}.json"
    )


def default_apply_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "promotion" / "apply"


def default_apply_json_path(data_root: Path, as_of: date) -> Path:
    return default_apply_root(data_root) / f"shadow_promotion_apply_result_{as_of.isoformat()}.json"


def default_apply_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_apply_root(data_root)
        / "logs"
        / f"shadow_promotion_apply_run_{as_of.isoformat()}.json"
    )


def default_rollback_snapshot_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback"
        / f"production_profile_before_shadow_promotion_{as_of.isoformat()}.json"
    )


def write_shadow_promotion_apply_report(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    preflight_file: Path | None = None,
    apply_approval_file: Path | None = None,
    target_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    proposal_file: Path | None = None,
    expected_target_profile_sha256: str | None = None,
    danger_flag_provided: bool = False,
    write_mode: str = "atomic",
    fail_on_warning: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_apply_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = default_apply_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    preflight_path = preflight_file or default_preflight_json_path(data_root, as_of)
    approval_path = apply_approval_file or default_apply_approval_path(data_root, as_of)
    preflight = _read_json_object(preflight_path)
    approval = _read_json_object(approval_path)
    proposal_path = _resolve_proposal_path(
        data_root=data_root,
        as_of=as_of,
        proposal_file=proposal_file,
        preflight=preflight,
        approval=approval,
    )
    proposal = _read_json_object(proposal_path)
    target_profile = _read_structured_object(target_profile_path)
    target_sha_before = _sha256(target_profile_path) if target_profile_path.is_file() else ""

    input_artifacts = {
        "preflight": _artifact_record(preflight_path),
        "apply_approval": _artifact_record(approval_path),
        "proposal": _artifact_record(proposal_path),
        "target_profile_before": _artifact_record(target_profile_path),
    }
    preflight_validation = _validate_preflight(
        preflight=preflight,
        preflight_path=preflight_path,
        target_profile_path=target_profile_path,
    )
    expected_weights = _expected_weights_from_preflight(preflight)
    weight_field, production_weights_before = get_profile_weights(target_profile)
    weight_validation = _validate_expected_weights(
        expected_weights=expected_weights,
        production_weights_before=production_weights_before,
    )
    approval_validation = _validate_approval(
        approval=approval,
        preflight=preflight,
        proposal=proposal,
        preflight_path=preflight_path,
        proposal_path=proposal_path,
        target_profile_path=target_profile_path,
        preflight_sha256=_string_value(input_artifacts["preflight"].get("sha256")),
        proposal_sha256=_string_value(input_artifacts["proposal"].get("sha256")),
        expected_target_profile_sha256=expected_target_profile_sha256,
    )
    target_profile_validation = _validate_target_profile(
        approval=approval,
        preflight=preflight,
        target_profile=target_profile,
        target_profile_path=target_profile_path,
        current_sha256=target_sha_before,
        expected_target_profile_sha256=expected_target_profile_sha256,
        weight_field=weight_field,
        production_weights_before=production_weights_before,
        expected_weights=expected_weights,
    )
    danger_flag_validation = {
        "status": "PASS" if danger_flag_provided else "FAIL",
        "required_flag": DANGER_FLAG,
        "provided": danger_flag_provided,
    }
    warnings = _warning_reasons(approval=approval, write_mode=write_mode)
    safety_validation = _validate_safety(write_mode=write_mode, warnings=warnings)

    diff_applied = _build_diff_applied(
        production_weights_before=production_weights_before,
        production_weights_after=expected_weights,
    )
    rollback = _empty_rollback(data_root=data_root, as_of=as_of, target_sha256=target_sha_before)
    post_apply_validation = _empty_post_apply_validation()

    apply_decision = _pre_write_decision(
        input_artifacts=input_artifacts,
        danger_flag_validation=danger_flag_validation,
        preflight_validation=preflight_validation,
        approval_validation=approval_validation,
        target_profile_validation=target_profile_validation,
        weight_validation=weight_validation,
        safety_validation=safety_validation,
        fail_on_warning=fail_on_warning,
        warnings=warnings,
    )
    apply_executed = False
    atomic_write_used = False
    profile_sha_after = ""

    if apply_decision == "READY_TO_APPLY":
        try:
            rollback = _create_rollback_snapshot(
                data_root=data_root,
                as_of=as_of,
                target_profile=target_profile,
                target_profile_sha256=target_sha_before,
            )
        except OSError as exc:
            apply_decision = DECISION_ROLLBACK_SNAPSHOT_FAILED
            rollback = _rollback_failed(data_root=data_root, as_of=as_of, reason=str(exc))

    if apply_decision == "READY_TO_APPLY":
        try:
            updated_profile = set_profile_weights(
                profile=target_profile,
                weight_field=weight_field,
                weights=expected_weights,
            )
            _atomic_write_profile(target_profile_path, updated_profile, write_mode=write_mode)
            apply_executed = True
            atomic_write_used = write_mode == "atomic"
        except OSError as exc:
            apply_decision = DECISION_WRITE_FAILED
            post_apply_validation = _post_apply_failed(f"write_failed:{exc}")

    if apply_decision == "READY_TO_APPLY" and apply_executed:
        profile_after = _read_structured_object(target_profile_path)
        profile_sha_after = _sha256(target_profile_path) if target_profile_path.is_file() else ""
        post_apply_validation = _validate_post_apply(
            profile_before=target_profile,
            profile_after=profile_after,
            weight_field=weight_field,
            expected_weights=expected_weights,
            sha256_before=target_sha_before,
            sha256_after=profile_sha_after,
        )
        target_profile_validation["sha256_after"] = profile_sha_after
        target_profile_validation["hash_changed_after_apply"] = (
            bool(profile_sha_after) and profile_sha_after != target_sha_before
        )
        apply_decision = (
            DECISION_APPLIED
            if post_apply_validation.get("status") == "PASS"
            else DECISION_POST_APPLY_VALIDATION_FAILED
        )

    payload = _build_result_payload(
        as_of=as_of,
        generated_at=generated,
        apply_decision=apply_decision,
        input_artifacts=input_artifacts,
        approval_validation=approval_validation,
        preflight_validation=preflight_validation,
        danger_flag_validation=danger_flag_validation,
        target_profile_validation=target_profile_validation,
        weight_validation=weight_validation,
        diff_applied=diff_applied,
        rollback=rollback,
        post_apply_validation=post_apply_validation,
        warnings=warnings,
        apply_executed=apply_executed,
        atomic_write_used=atomic_write_used,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        run_log_json_path=run_log_json_path,
        run_log_md_path=run_log_md_path,
        target_profile_path=target_profile_path,
        profile_sha_after=profile_sha_after,
    )
    _assert_apply_safety_invariants(payload)
    _write_json(output_json_path, payload)
    _write_text(output_md_path, render_shadow_promotion_apply_report(payload))

    run_log = _run_log_payload(payload=payload, generated_at=generated)
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_shadow_promotion_apply_run_log(run_log))
    return payload


def get_profile_weights(profile: dict[str, Any]) -> tuple[str, dict[str, float]]:
    for field in WEIGHT_FIELD_CANDIDATES:
        weights = _weights_from_mapping(_mapping(profile.get(field)))
        if weights:
            return field, _rounded_weights(weights)
    return "", {}


def set_profile_weights(
    *,
    profile: dict[str, Any],
    weight_field: str,
    weights: dict[str, float],
) -> dict[str, Any]:
    updated = copy.deepcopy(profile)
    before_mapping = _mapping(updated.get(weight_field))
    ordered: dict[str, float] = {}
    for key in before_mapping:
        if str(key) in weights:
            ordered[str(key)] = weights[str(key)]
    for key in sorted(set(weights) - set(ordered)):
        ordered[key] = weights[key]
    updated[weight_field] = ordered
    return updated


def render_shadow_promotion_apply_report(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    approval_validation = _mapping(payload.get("approval_validation"))
    preflight_validation = _mapping(payload.get("preflight_validation"))
    danger = _mapping(payload.get("danger_flag_validation"))
    diff = _mapping(payload.get("diff_applied"))
    rollback = _mapping(payload.get("rollback"))
    post = _mapping(payload.get("post_apply_validation"))
    before = _mapping(diff.get("production_weights_before"))
    after = _mapping(diff.get("production_weights_after"))
    delta = _mapping(diff.get("delta"))

    lines = [
        f"# Shadow Promotion Apply Result - {payload.get('date')}",
        "",
        "## 1. Run Summary",
        "",
        f"- Apply Decision: `{payload.get('apply_decision')}`",
        f"- Apply Executed: `{str(payload.get('apply_executed')).lower()}`",
        f"- Promotion Executed: `{str(payload.get('promotion_executed')).lower()}`",
        f"- Production Effect: `{payload.get('production_effect')}`",
        f"- Manual Review Only: `{str(payload.get('manual_review_only')).lower()}`",
        f"- Safe For Scheduler: `{str(payload.get('safe_for_scheduler')).lower()}`",
        f"- Broker Execution: `{str(payload.get('broker_execution')).lower()}`",
        f"- Replay Execution: `{str(payload.get('replay_execution')).lower()}`",
        f"- Trading Execution: `{str(payload.get('trading_execution')).lower()}`",
        "",
    ]
    if payload.get("apply_executed") is not True:
        lines.extend(
            [
                "Apply was not executed.",
                "",
                "Production profile was not modified.",
                "",
            ],
        )

    lines.extend(
        [
            "## 2. Input Artifacts",
            "",
            "| Artifact | Status | Path | SHA256 |",
            "|---|---:|---|---|",
        ],
    )
    for key in ("preflight", "apply_approval", "proposal", "target_profile_before"):
        artifact = _mapping(artifacts.get(key))
        lines.append(
            f"| {key} | `{artifact.get('status', 'MISSING')}` | "
            f"`{artifact.get('path', '')}` | `{artifact.get('sha256', '')}` |"
        )

    lines.extend(
        [
            "",
            "## 3. Approval Validation",
            "",
            "| Check | Status | Reason |",
            "|---|---:|---|",
        ],
    )
    for check in _records(approval_validation.get("checks")):
        lines.append(_validation_row(check))

    lines.extend(
        [
            "",
            "## 4. Preflight Validation",
            "",
            "| Check | Status | Reason |",
            "|---|---:|---|",
        ],
    )
    for check in _records(preflight_validation.get("checks")):
        lines.append(_validation_row(check))

    lines.extend(
        [
            "",
            "## 5. Danger Flag Validation",
            "",
            "| Required Flag | Provided |",
            "|---|---:|",
            f"| `{danger.get('required_flag', DANGER_FLAG)}` | "
            f"`{str(danger.get('provided') is True).lower()}` |",
            "",
            "## 6. Production Diff Applied",
            "",
            "| Weight Key | Before | After | Delta |",
            "|---|---:|---:|---:|",
        ],
    )
    for key in sorted(set(before) | set(after)):
        lines.append(
            f"| {key} | {_format_float(before.get(key))} | "
            f"{_format_float(after.get(key))} | {_format_signed_float(delta.get(key))} |"
        )

    lines.extend(
        [
            "",
            "## 7. Rollback Snapshot",
            "",
            f"- Snapshot created: `{str(rollback.get('snapshot_created') is True).lower()}`",
            f"- Snapshot path: `{rollback.get('snapshot_path', '')}`",
            f"- Snapshot SHA256: `{rollback.get('snapshot_sha256', '')}`",
            "- Rollback command:",
            "  - Not implemented in TRADING-018E2.",
            "  - Future task: TRADING-018E3.",
            "",
            "## 8. Post-Apply Validation",
            "",
            "| Check | Status | Reason |",
            "|---|---:|---|",
        ],
    )
    for check in _records(post.get("checks")):
        lines.append(_validation_row(check))

    lines.extend(
        [
            "",
            "## 9. Safety Statement",
            "",
            (
                "This task only updates the target production profile weights after explicit "
                "manual approval."
            ),
            "",
            "It does not:",
            "- run broker execution",
            "- run replay execution",
            "- run trading execution",
            "- schedule automatic apply",
            "- perform rollback",
            "",
            "## 10. Next Step",
            "",
            "Run downstream validation manually before relying on the updated production profile.",
            "",
            "Rollback support will be handled by TRADING-018E3.",
            "",
        ],
    )
    return "\n".join(lines)


def render_shadow_promotion_apply_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Shadow Promotion Apply Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- apply_decision: `{payload.get('apply_decision')}`",
            f"- production_effect: `{payload.get('production_effect')}`",
            f"- manual_review_only: `{str(payload.get('manual_review_only')).lower()}`",
            f"- promotion_executed: `{str(payload.get('promotion_executed')).lower()}`",
            f"- apply_executed: `{str(payload.get('apply_executed')).lower()}`",
            f"- safe_for_scheduler: `{str(payload.get('safe_for_scheduler')).lower()}`",
            f"- broker_execution: `{str(payload.get('broker_execution')).lower()}`",
            f"- replay_execution: `{str(payload.get('replay_execution')).lower()}`",
            f"- trading_execution: `{str(payload.get('trading_execution')).lower()}`",
            f"- apply_result_json: `{payload.get('apply_result_json')}`",
            f"- apply_result_markdown: `{payload.get('apply_result_markdown')}`",
            "",
        ],
    )


def _validate_preflight(
    *,
    preflight: dict[str, Any],
    preflight_path: Path,
    target_profile_path: Path,
) -> dict[str, Any]:
    diff = _mapping(preflight.get("diff_preview"))
    rollback = _mapping(preflight.get("rollback_plan"))
    expected_weights = _expected_weights_from_preflight(preflight)
    target_path = _string_value(diff.get("target_profile_path")) or _string_value(
        rollback.get("target_profile_path")
    )
    pipeline_contract = _mapping(preflight.get("pipeline_contract"))
    unsafe_contract_fields = [
        field
        for field in (
            "runs_shadow_iteration_pipeline",
            "runs_comparison_pipeline",
            "runs_multi_day_review_pipeline",
            "runs_promotion_proposal_pipeline",
            "runs_promotion_apply",
            "runs_scoring_pipeline",
            "runs_broker_runner",
            "runs_paper_runner",
            "runs_replay_runner",
            "writes_production_profile",
            "writes_production_weights",
            "writes_approved_profile",
            "promotes_shadow_to_production",
            "triggers_trade",
        )
        if field in pipeline_contract and pipeline_contract.get(field) is not False
    ]
    checks = [
        _check("preflight_file_exists", bool(preflight), str(preflight_path)),
        _check(
            "task_id",
            preflight.get("task_id") == SOURCE_PREFLIGHT_TASK_ID,
            SOURCE_PREFLIGHT_TASK_ID,
        ),
        _check(
            "preflight_decision",
            preflight.get("preflight_decision") == "PASS",
            _string_value(preflight.get("preflight_decision")),
        ),
        _check(
            "preflight_only",
            preflight.get("preflight_only") is True,
            str(preflight.get("preflight_only")),
        ),
        _check(
            "apply_executed",
            preflight.get("apply_executed") is False,
            str(preflight.get("apply_executed")),
        ),
        _check(
            "promotion_executed",
            preflight.get("promotion_executed") is False,
            str(preflight.get("promotion_executed")),
        ),
        _check(
            "production_effect",
            preflight.get("production_effect") == PRODUCTION_EFFECT_NONE,
            _string_value(preflight.get("production_effect")),
        ),
        _check("diff_preview", bool(diff), f"key_count={len(diff)}"),
        _check(
            "production_weights_after_preview",
            bool(expected_weights),
            f"key_count={len(expected_weights)}",
        ),
        _check(
            "target_profile_path",
            _path_matches(target_path, target_profile_path),
            f"preflight={target_path}, target={target_profile_path}",
        ),
        _check(
            "target_profile_sha256_before", bool(_preflight_target_sha256(preflight)), "required"
        ),
        _check("rollback_plan", bool(rollback), f"key_count={len(rollback)}"),
        _check(
            "pipeline_contract_safe", not unsafe_contract_fields, ",".join(unsafe_contract_fields)
        ),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "preflight_decision": _string_value(preflight.get("preflight_decision")),
        "preflight_only": preflight.get("preflight_only") is True,
        "apply_executed": preflight.get("apply_executed") is True,
        "promotion_executed": preflight.get("promotion_executed") is True,
        "production_effect": _string_value(preflight.get("production_effect")),
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _validate_approval(
    *,
    approval: dict[str, Any],
    preflight: dict[str, Any],
    proposal: dict[str, Any],
    preflight_path: Path,
    proposal_path: Path,
    target_profile_path: Path,
    preflight_sha256: str,
    proposal_sha256: str,
    expected_target_profile_sha256: str | None,
) -> dict[str, Any]:
    preflight_section = _mapping(approval.get("preflight"))
    proposal_section = _mapping(approval.get("proposal"))
    target = _mapping(approval.get("target"))
    apply_scope = _mapping(approval.get("apply_scope"))
    safety = _mapping(approval.get("safety_acknowledgement"))
    forbidden_fields = set(_strings(apply_scope.get("forbidden_fields")))
    allowed_fields = set(_strings(apply_scope.get("allowed_fields")))
    preflight_target_sha = _preflight_target_sha256(preflight)
    approval_expected_target_sha = _string_value(target.get("expected_target_profile_sha256"))
    expected_cli_hash_matches = (
        True
        if not expected_target_profile_sha256
        else approval_expected_target_sha == expected_target_profile_sha256
    )
    checks = [
        _check("approval_exists", bool(approval), "apply approval JSON loaded"),
        _check("approval_type", approval.get("approval_type") == APPROVAL_TYPE, APPROVAL_TYPE),
        _check("approved", approval.get("approved") is True, str(approval.get("approved"))),
        _check(
            "preflight_file",
            _path_matches(_string_value(preflight_section.get("preflight_file")), preflight_path),
            _string_value(preflight_section.get("preflight_file")),
        ),
        _check(
            "preflight_hash_match",
            bool(preflight_sha256)
            and _string_value(preflight_section.get("preflight_sha256")) == preflight_sha256,
            _string_value(preflight_section.get("preflight_sha256")),
        ),
        _check(
            "preflight_decision",
            _string_value(preflight_section.get("preflight_decision")) == "PASS",
            _string_value(preflight_section.get("preflight_decision")),
        ),
        _check(
            "proposal_file",
            _path_matches(_string_value(proposal_section.get("proposal_file")), proposal_path),
            _string_value(proposal_section.get("proposal_file")),
        ),
        _check(
            "proposal_hash_match",
            bool(proposal_sha256)
            and _string_value(proposal_section.get("proposal_sha256")) == proposal_sha256,
            _string_value(proposal_section.get("proposal_sha256")),
        ),
        _check(
            "proposal_task_id",
            proposal.get("task_id") == SOURCE_PROPOSAL_TASK_ID,
            _string_value(proposal.get("task_id")),
        ),
        _check(
            "proposal_decision",
            _string_value(proposal_section.get("proposal_decision")) == PROPOSAL_DECISION_REQUIRED
            and proposal.get("proposal_decision") == PROPOSAL_DECISION_REQUIRED,
            _string_value(proposal_section.get("proposal_decision")),
        ),
        _check(
            "promotion_proposed",
            proposal_section.get("promotion_proposed") is True
            and proposal.get("promotion_proposed") is True,
            str(proposal_section.get("promotion_proposed")),
        ),
        _check(
            "target_profile_path",
            _path_matches(_string_value(target.get("target_profile_path")), target_profile_path),
            _string_value(target.get("target_profile_path")),
        ),
        _check(
            "expected_target_profile_sha256",
            bool(approval_expected_target_sha)
            and approval_expected_target_sha == preflight_target_sha
            and expected_cli_hash_matches,
            (
                f"approval={approval_expected_target_sha}, "
                f"preflight={preflight_target_sha}, cli={expected_target_profile_sha256 or ''}"
            ),
        ),
        _check(
            "allowed_fields_weights_only",
            "weights" in allowed_fields or "base_weights" in allowed_fields,
            ",".join(sorted(allowed_fields)),
        ),
        _check(
            "forbidden_fields_declared",
            FORBIDDEN_APPROVAL_FIELDS.issubset(forbidden_fields),
            ",".join(sorted(FORBIDDEN_APPROVAL_FIELDS - forbidden_fields)),
        ),
        _check(
            "apply_authorized",
            safety.get("apply_authorized") is True,
            str(safety.get("apply_authorized")),
        ),
        _check(
            "production_modification_authorized",
            safety.get("production_modification_authorized") is True,
            str(safety.get("production_modification_authorized")),
        ),
        _check(
            "weights_only_update",
            safety.get("weights_only_update") is True,
            str(safety.get("weights_only_update")),
        ),
        _check(
            "rollback_required",
            safety.get("rollback_required") is True,
            str(safety.get("rollback_required")),
        ),
        _check(
            "manual_command_required",
            safety.get("manual_command_required") is True,
            str(safety.get("manual_command_required")),
        ),
        _check(
            "scheduler_execution_forbidden",
            safety.get("scheduler_execution_forbidden") is True,
            str(safety.get("scheduler_execution_forbidden")),
        ),
        _check(
            "broker_execution_forbidden",
            safety.get("broker_execution_forbidden") is True,
            str(safety.get("broker_execution_forbidden")),
        ),
        _check(
            "replay_execution_forbidden",
            safety.get("replay_execution_forbidden") is True,
            str(safety.get("replay_execution_forbidden")),
        ),
        _check(
            "trading_execution_forbidden",
            safety.get("trading_execution_forbidden") is True,
            str(safety.get("trading_execution_forbidden")),
        ),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "approved": approval.get("approved") is True,
        "preflight_hash_match": _check_status(checks, "preflight_hash_match"),
        "proposal_hash_match": _check_status(checks, "proposal_hash_match"),
        "target_profile_hash_match": _check_status(checks, "expected_target_profile_sha256"),
        "apply_authorized": safety.get("apply_authorized") is True,
        "production_modification_authorized": (
            safety.get("production_modification_authorized") is True
        ),
        "weights_only_update": safety.get("weights_only_update") is True,
        "rollback_required": safety.get("rollback_required") is True,
        "manual_command_required": safety.get("manual_command_required") is True,
        "scheduler_execution_forbidden": safety.get("scheduler_execution_forbidden") is True,
        "broker_execution_forbidden": safety.get("broker_execution_forbidden") is True,
        "replay_execution_forbidden": safety.get("replay_execution_forbidden") is True,
        "trading_execution_forbidden": safety.get("trading_execution_forbidden") is True,
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _validate_target_profile(
    *,
    approval: dict[str, Any],
    preflight: dict[str, Any],
    target_profile: dict[str, Any],
    target_profile_path: Path,
    current_sha256: str,
    expected_target_profile_sha256: str | None,
    weight_field: str,
    production_weights_before: dict[str, float],
    expected_weights: dict[str, float],
) -> dict[str, Any]:
    target = _mapping(approval.get("target"))
    target_profile_name = (
        _string_value(target.get("target_profile_name")) or DEFAULT_TARGET_PROFILE_NAME
    )
    preflight_sha = _preflight_target_sha256(preflight)
    approval_sha = _string_value(target.get("expected_target_profile_sha256"))
    cli_expected_ok = (
        True
        if not expected_target_profile_sha256
        else current_sha256 == expected_target_profile_sha256
    )
    metadata_checks = _target_metadata_checks(
        target_profile=target_profile,
        target_profile_name=target_profile_name,
    )
    checks = [
        _check("target_profile_exists", bool(target_profile), str(target_profile_path)),
        _check(
            "target_profile_path",
            _path_matches(_string_value(target.get("target_profile_path")), target_profile_path),
            _string_value(target.get("target_profile_path")),
        ),
        _check("weight_field_supported", bool(weight_field), ",".join(WEIGHT_FIELD_CANDIDATES)),
        _check(
            "weight_keys_match",
            bool(production_weights_before)
            and set(production_weights_before) == set(expected_weights),
            _key_reason(set(production_weights_before), set(expected_weights)),
        ),
        _check(
            "current_matches_preflight_sha256",
            bool(current_sha256) and current_sha256 == preflight_sha,
            f"current={current_sha256}, preflight={preflight_sha}",
        ),
        _check(
            "current_matches_approval_sha256",
            bool(current_sha256) and current_sha256 == approval_sha,
            f"current={current_sha256}, approval={approval_sha}",
        ),
        _check(
            "current_matches_cli_expected_sha256",
            cli_expected_ok,
            f"current={current_sha256}, cli={expected_target_profile_sha256 or ''}",
        ),
    ]
    checks.extend(metadata_checks)
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    hash_blocking = [
        reason
        for reason in blocking
        if reason
        in {
            "current_matches_preflight_sha256",
            "current_matches_approval_sha256",
            "current_matches_cli_expected_sha256",
        }
    ]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "path": str(target_profile_path),
        "weight_field": weight_field,
        "sha256_before": current_sha256,
        "sha256_expected_from_preflight": preflight_sha,
        "sha256_expected_from_approval": approval_sha,
        "sha256_after": "",
        "hash_changed_after_apply": False,
        "blocking_reasons": blocking,
        "hash_blocking_reasons": hash_blocking,
        "checks": checks,
    }


def _validate_expected_weights(
    *,
    expected_weights: dict[str, float],
    production_weights_before: dict[str, float],
) -> dict[str, Any]:
    expected_sum = sum(expected_weights.values())
    checks = [
        _check(
            "expected_weights_present", bool(expected_weights), f"key_count={len(expected_weights)}"
        ),
        _check(
            "weights_in_range",
            bool(expected_weights)
            and all(0.0 <= value <= 1.0 for value in expected_weights.values()),
            "[0, 1]",
        ),
        _check(
            "weights_sum_valid",
            bool(expected_weights) and abs(expected_sum - 1.0) <= WEIGHT_SUM_TOLERANCE,
            f"sum={expected_sum:.10f}, tolerance={WEIGHT_SUM_TOLERANCE:.10f}",
        ),
        _check(
            "current_weight_keys_match_expected",
            bool(production_weights_before)
            and set(production_weights_before) == set(expected_weights),
            _key_reason(set(production_weights_before), set(expected_weights)),
        ),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "weights_sum_valid": _check_status(checks, "weights_sum_valid"),
        "weight_keys_match": _check_status(checks, "current_weight_keys_match_expected"),
        "blocking_reasons": blocking,
        "expected_weight_sum": round(expected_sum, 10),
        "checks": checks,
    }


def _validate_safety(*, write_mode: str, warnings: list[str]) -> dict[str, Any]:
    checks = [
        _check("write_mode_atomic", write_mode == "atomic", write_mode),
        _check("safe_for_scheduler_false", True, "018E2 is manual only"),
        _check("broker_execution_false", True, "no broker code path"),
        _check("replay_execution_false", True, "no replay code path"),
        _check("trading_execution_false", True, "no trading execution path"),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "blocking_reasons": blocking,
        "warnings": warnings,
        "checks": checks,
    }


def _pre_write_decision(
    *,
    input_artifacts: dict[str, Any],
    danger_flag_validation: dict[str, Any],
    preflight_validation: dict[str, Any],
    approval_validation: dict[str, Any],
    target_profile_validation: dict[str, Any],
    weight_validation: dict[str, Any],
    safety_validation: dict[str, Any],
    fail_on_warning: bool,
    warnings: list[str],
) -> str:
    missing = [
        name
        for name, artifact in input_artifacts.items()
        if _mapping(artifact).get("status") != "FOUND"
    ]
    if missing:
        return DECISION_INSUFFICIENT_DATA
    if danger_flag_validation.get("provided") is not True:
        return DECISION_DANGER_FLAG_MISSING
    if safety_validation.get("status") != "PASS":
        return DECISION_SAFETY_BLOCKED
    if fail_on_warning and warnings:
        return DECISION_SAFETY_BLOCKED
    if preflight_validation.get("status") != "PASS":
        return DECISION_PREFLIGHT_INVALID
    target_blocking = set(_strings(target_profile_validation.get("blocking_reasons")))
    hash_blocking = set(_strings(target_profile_validation.get("hash_blocking_reasons")))
    if target_blocking - hash_blocking:
        return DECISION_TARGET_PROFILE_MISMATCH
    if approval_validation.get("status") != "PASS":
        return DECISION_APPROVAL_INVALID
    if hash_blocking:
        return DECISION_TARGET_PROFILE_CHANGED
    if weight_validation.get("status") != "PASS":
        if "current_weight_keys_match_expected" in _strings(
            weight_validation.get("blocking_reasons")
        ):
            return DECISION_TARGET_PROFILE_MISMATCH
        return DECISION_PREFLIGHT_INVALID
    return "READY_TO_APPLY"


def _build_result_payload(
    *,
    as_of: date,
    generated_at: datetime,
    apply_decision: str,
    input_artifacts: dict[str, Any],
    approval_validation: dict[str, Any],
    preflight_validation: dict[str, Any],
    danger_flag_validation: dict[str, Any],
    target_profile_validation: dict[str, Any],
    weight_validation: dict[str, Any],
    diff_applied: dict[str, Any],
    rollback: dict[str, Any],
    post_apply_validation: dict[str, Any],
    warnings: list[str],
    apply_executed: bool,
    atomic_write_used: bool,
    output_json_path: Path,
    output_md_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    target_profile_path: Path,
    profile_sha_after: str,
) -> dict[str, Any]:
    decision = apply_decision if apply_decision in APPLY_DECISIONS else DECISION_ERROR
    applied = decision == DECISION_APPLIED
    production_effect = PRODUCTION_EFFECT_APPLIED if apply_executed else PRODUCTION_EFFECT_NONE
    if applied:
        production_effect = PRODUCTION_EFFECT_APPLIED
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "production_effect": production_effect,
        "manual_review_only": True,
        "promotion_executed": applied,
        "apply_executed": apply_executed,
        "safe_for_scheduler": False,
        "safe_for_production": applied,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "apply_decision": decision,
        "apply_reason": _apply_reason(decision),
        "input_artifacts": input_artifacts,
        "approval_validation": approval_validation,
        "preflight_validation": preflight_validation,
        "danger_flag_validation": danger_flag_validation,
        "target_profile_validation": target_profile_validation,
        "weight_validation": weight_validation,
        "diff_applied": diff_applied,
        "rollback": rollback,
        "post_apply_validation": post_apply_validation,
        "warnings": warnings,
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "run_log_json": str(run_log_json_path),
            "run_log_markdown": str(run_log_md_path),
        },
        "pipeline_contract": {
            "runs_shadow_iteration_pipeline": False,
            "runs_comparison_pipeline": False,
            "runs_multi_day_review_pipeline": False,
            "runs_promotion_proposal_pipeline": False,
            "runs_apply_preflight_pipeline": False,
            "runs_promotion_apply": apply_executed,
            "runs_scoring_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": apply_executed,
            "writes_production_weights": apply_executed,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": applied,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "production_effect": production_effect,
            "manual_review_only": True,
        },
        "audit": {
            "created_by": "scripts/run_shadow_promotion_apply.py",
            "created_at": generated_at.isoformat(),
            "atomic_write_used": atomic_write_used,
            "no_broker_execution": True,
            "no_replay_execution": True,
            "no_trading_execution": True,
            "scheduler_safe": False,
            "target_profile_path": str(target_profile_path),
            "target_profile_sha256_after": profile_sha_after,
        },
    }
    return payload


def _apply_reason(decision: str) -> str:
    if decision == DECISION_APPLIED:
        return (
            "Preflight, apply approval, danger flag, target profile hash, rollback snapshot, "
            "and post-apply validation all passed."
        )
    if decision == DECISION_INSUFFICIENT_DATA:
        return (
            "Required preflight, apply approval, proposal, or target profile artifact is missing."
        )
    if decision == DECISION_APPROVAL_INVALID:
        return "Apply approval artifact is missing required authorization or does not match inputs."
    if decision == DECISION_PREFLIGHT_INVALID:
        return "Preflight artifact is not a PASS TRADING-018E1 preflight with required diff data."
    if decision == DECISION_DANGER_FLAG_MISSING:
        return "Required explicit danger flag was not provided."
    if decision == DECISION_TARGET_PROFILE_CHANGED:
        return "Current target profile sha256 differs from the preflight or approval expected hash."
    if decision == DECISION_TARGET_PROFILE_MISMATCH:
        return (
            "Target profile path, metadata, weight field, or weight keys do not match the approval."
        )
    if decision == DECISION_ROLLBACK_SNAPSHOT_FAILED:
        return "Rollback snapshot could not be created before production profile write."
    if decision == DECISION_WRITE_FAILED:
        return "Atomic production profile write failed."
    if decision == DECISION_POST_APPLY_VALIDATION_FAILED:
        return "Production profile write completed but post-apply validation failed."
    if decision == DECISION_SAFETY_BLOCKED:
        return "One or more apply safety boundaries failed."
    return "Unexpected apply error."


def _create_rollback_snapshot(
    *,
    data_root: Path,
    as_of: date,
    target_profile: dict[str, Any],
    target_profile_sha256: str,
) -> dict[str, Any]:
    snapshot_path = default_rollback_snapshot_path(data_root, as_of)
    sha_path = snapshot_path.with_suffix(".sha256")
    _write_json(snapshot_path, target_profile)
    _write_text(sha_path, target_profile_sha256 + "\n")
    return {
        "snapshot_created": True,
        "snapshot_path": str(snapshot_path),
        "snapshot_sha256": target_profile_sha256,
        "snapshot_file_sha256": _sha256(snapshot_path),
        "snapshot_sha256_path": str(sha_path),
        "rollback_supported": True,
        "rollback_command_future_task": "TRADING-018E3",
        "blocking_reasons": [],
    }


def _empty_rollback(*, data_root: Path, as_of: date, target_sha256: str) -> dict[str, Any]:
    snapshot_path = default_rollback_snapshot_path(data_root, as_of)
    return {
        "snapshot_created": False,
        "snapshot_path": str(snapshot_path),
        "snapshot_sha256": target_sha256,
        "snapshot_file_sha256": "",
        "snapshot_sha256_path": str(snapshot_path.with_suffix(".sha256")),
        "rollback_supported": False,
        "rollback_command_future_task": "TRADING-018E3",
        "blocking_reasons": [],
    }


def _rollback_failed(*, data_root: Path, as_of: date, reason: str) -> dict[str, Any]:
    rollback = _empty_rollback(data_root=data_root, as_of=as_of, target_sha256="")
    rollback["blocking_reasons"] = [reason]
    return rollback


def _validate_post_apply(
    *,
    profile_before: dict[str, Any],
    profile_after: dict[str, Any],
    weight_field: str,
    expected_weights: dict[str, float],
    sha256_before: str,
    sha256_after: str,
) -> dict[str, Any]:
    after_field, weights_after = get_profile_weights(profile_after)
    weights_match_expected = _weights_match(weights_after, expected_weights)
    weight_keys_match = set(weights_after) == set(expected_weights) and bool(weights_after)
    weights_sum = sum(weights_after.values())
    weights_sum_valid = bool(weights_after) and abs(weights_sum - 1.0) <= WEIGHT_SUM_TOLERANCE
    only_allowed_fields_changed = _only_weight_field_changed(
        before=profile_before,
        after=profile_after,
        weight_field=weight_field,
    )
    hash_changed = bool(sha256_after) and sha256_after != sha256_before
    checks = [
        _check(
            "weights_match_expected", weights_match_expected, "post-apply weights equal expected"
        ),
        _check(
            "weight_keys_match",
            weight_keys_match,
            _key_reason(set(weights_after), set(expected_weights)),
        ),
        _check(
            "weights_sum_valid",
            weights_sum_valid,
            f"sum={weights_sum:.10f}, tolerance={WEIGHT_SUM_TOLERANCE:.10f}",
        ),
        _check("only_allowed_fields_changed", only_allowed_fields_changed, weight_field),
        _check(
            "weight_field_unchanged",
            after_field == weight_field,
            f"before={weight_field}, after={after_field}",
        ),
        _check(
            "target_profile_hash_changed",
            hash_changed,
            f"before={sha256_before}, after={sha256_after}",
        ),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "weights_match_expected": weights_match_expected,
        "weight_keys_match": weight_keys_match,
        "weights_sum_valid": weights_sum_valid,
        "only_allowed_fields_changed": only_allowed_fields_changed,
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _empty_post_apply_validation() -> dict[str, Any]:
    checks = [
        _check("weights_match_expected", False, "apply not executed"),
        _check("weight_keys_match", False, "apply not executed"),
        _check("weights_sum_valid", False, "apply not executed"),
        _check("only_allowed_fields_changed", False, "apply not executed"),
    ]
    return {
        "status": "NOT_RUN",
        "weights_match_expected": False,
        "weight_keys_match": False,
        "weights_sum_valid": False,
        "only_allowed_fields_changed": False,
        "blocking_reasons": [],
        "checks": checks,
    }


def _post_apply_failed(reason: str) -> dict[str, Any]:
    payload = _empty_post_apply_validation()
    payload["status"] = "FAIL"
    payload["blocking_reasons"] = [reason]
    return payload


def _build_diff_applied(
    *,
    production_weights_before: dict[str, float],
    production_weights_after: dict[str, float],
) -> dict[str, Any]:
    delta: dict[str, float] = {}
    for key in sorted(set(production_weights_before) | set(production_weights_after)):
        before = production_weights_before.get(key)
        after = production_weights_after.get(key)
        if before is None or after is None:
            continue
        delta[key] = round(after - before, 10)
    changed = [key for key, value in delta.items() if abs(value) > WEIGHT_MATCH_TOLERANCE]
    return {
        "changed_weight_keys": changed,
        "production_weights_before": production_weights_before,
        "production_weights_after": production_weights_after,
        "delta": delta,
    }


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("outputs"))
    decision = _string_value(payload.get("apply_decision"))
    run_log = {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "generated_at": generated_at.isoformat(),
        "run_status": "APPLIED" if decision == DECISION_APPLIED else "BLOCKED",
        "apply_decision": decision,
        "production_effect": payload.get("production_effect"),
        "manual_review_only": True,
        "promotion_executed": payload.get("promotion_executed") is True,
        "apply_executed": payload.get("apply_executed") is True,
        "safe_for_scheduler": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "apply_result_json": outputs.get("json", ""),
        "apply_result_markdown": outputs.get("markdown", ""),
        "run_log_json": outputs.get("run_log_json", ""),
        "run_log_markdown": outputs.get("run_log_markdown", ""),
        "rollback": payload.get("rollback", {}),
        "audit": payload.get("audit", {}),
    }
    _assert_apply_safety_invariants(run_log)
    return run_log


def _resolve_proposal_path(
    *,
    data_root: Path,
    as_of: date,
    proposal_file: Path | None,
    preflight: dict[str, Any],
    approval: dict[str, Any],
) -> Path:
    if proposal_file is not None:
        return proposal_file
    approval_proposal = _mapping(approval.get("proposal"))
    declared = _string_value(approval_proposal.get("proposal_file"))
    if not declared:
        preflight_artifacts = _mapping(preflight.get("input_artifacts"))
        proposal_artifact = _mapping(preflight_artifacts.get("promotion_proposal"))
        declared = _string_value(proposal_artifact.get("path"))
    if declared:
        return _declared_path(declared)
    return default_proposal_json_path(data_root, as_of)


def _expected_weights_from_preflight(preflight: dict[str, Any]) -> dict[str, float]:
    diff = _mapping(preflight.get("diff_preview"))
    return _rounded_weights(
        _weights_from_mapping(_mapping(diff.get("production_weights_after_preview")))
    )


def _preflight_target_sha256(preflight: dict[str, Any]) -> str:
    diff = _mapping(preflight.get("diff_preview"))
    rollback = _mapping(preflight.get("rollback_plan"))
    artifacts = _mapping(preflight.get("input_artifacts"))
    production = _mapping(artifacts.get("production_profile"))
    return (
        _string_value(diff.get("target_profile_sha256_before"))
        or _string_value(rollback.get("target_profile_sha256_before"))
        or _string_value(production.get("sha256"))
    )


def _target_metadata_checks(
    *,
    target_profile: dict[str, Any],
    target_profile_name: str,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for key in ("profile_name", "target_profile_name", "name"):
        value = _string_value(target_profile.get(key))
        if value:
            checks.append(
                _check(
                    key,
                    value == target_profile_name,
                    f"value={value}, target={target_profile_name}",
                )
            )
    status = _string_value(target_profile.get("status"))
    if status and target_profile_name == "production":
        checks.append(_check("status", status == "production", f"value={status}"))
    environment = _string_value(target_profile.get("environment"))
    if environment:
        checks.append(
            _check(
                "environment",
                environment in {target_profile_name, "production"},
                f"value={environment}, target={target_profile_name}",
            )
        )
    return checks


def _warning_reasons(*, approval: dict[str, Any], write_mode: str) -> list[str]:
    warnings: list[str] = []
    if approval and not _string_value(approval.get("approval_statement")):
        warnings.append("approval_statement_missing")
    if approval and not _string_value(approval.get("approved_by")):
        warnings.append("approved_by_missing")
    if approval and not _string_value(approval.get("approved_at")):
        warnings.append("approved_at_missing")
    if write_mode != "atomic":
        warnings.append("write_mode_not_atomic")
    return warnings


def _only_weight_field_changed(
    *,
    before: dict[str, Any],
    after: dict[str, Any],
    weight_field: str,
) -> bool:
    before_copy = copy.deepcopy(before)
    after_copy = copy.deepcopy(after)
    if weight_field not in before_copy or weight_field not in after_copy:
        return False
    before_copy[weight_field] = "__WEIGHTS_ALLOWED_TO_CHANGE__"
    after_copy[weight_field] = "__WEIGHTS_ALLOWED_TO_CHANGE__"
    return before_copy == after_copy


def _weights_match(left: dict[str, float], right: dict[str, float]) -> bool:
    return set(left) == set(right) and all(
        abs(left[key] - right[key]) <= WEIGHT_MATCH_TOLERANCE for key in right
    )


def _atomic_write_profile(path: Path, payload: dict[str, Any], *, write_mode: str) -> None:
    if write_mode != "atomic":
        raise OSError("only atomic write mode is supported")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(_serialize_structured_payload(path, payload), encoding="utf-8")
    tmp_path.replace(path)


def _serialize_structured_payload(path: Path, payload: dict[str, Any]) -> str:
    if path.suffix.lower() in {".yaml", ".yml"}:
        return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    return json.dumps(payload, ensure_ascii=False, indent=2) + "\n"


def _artifact_record(path: Path) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "status": "FOUND" if exists else "MISSING",
        "path": str(path),
        "exists": exists,
        "sha256": _sha256(path) if exists else "",
        "size_bytes": path.stat().st_size if exists else 0,
    }


def _check(name: str, passed: bool, reason: str) -> dict[str, Any]:
    return {"check": name, "status": "PASS" if passed else "FAIL", "reason": reason}


def _check_status(checks: list[dict[str, Any]], name: str) -> bool:
    return any(check.get("check") == name and check.get("status") == "PASS" for check in checks)


def _validation_row(check: dict[str, Any]) -> str:
    return (
        f"| {_escape_table(check.get('check', ''))} | `{check.get('status', 'FAIL')}` | "
        f"{_escape_table(check.get('reason', ''))} |"
    )


def _key_reason(left: set[str], right: set[str]) -> str:
    return (
        f"missing={sorted(left - right)}, extra={sorted(right - left)}, "
        f"left_count={len(left)}, right_count={len(right)}"
    )


def _path_matches(declared: str, actual_path: Path) -> bool:
    if not declared:
        return False
    declared_path = Path(declared)
    candidates = [declared_path]
    if not declared_path.is_absolute():
        candidates.extend([REPO_ROOT / declared_path, Path.cwd() / declared_path])
    actual_resolved = actual_path.resolve(strict=False)
    actual_text = _normalized_path_text(actual_path)
    declared_text = _normalized_path_text(declared_path)
    if declared_text == actual_text:
        return True
    return any(candidate.resolve(strict=False) == actual_resolved for candidate in candidates)


def _declared_path(value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    cwd_path = Path.cwd() / path
    repo_path = REPO_ROOT / path
    if cwd_path.exists():
        return cwd_path
    return repo_path


def _normalized_path_text(path: Path) -> str:
    return str(path).replace("\\", "/").rstrip("/")


def _assert_apply_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("manual_review_only") is not True:
        raise ValueError("shadow promotion apply must remain manual_review_only")
    if payload.get("safe_for_scheduler") is not False:
        raise ValueError("shadow promotion apply must not be scheduler-safe")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"shadow promotion apply must keep {field}=false")
    apply_executed = payload.get("apply_executed") is True
    if not apply_executed:
        if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
            raise ValueError("non-executed apply must have production_effect=none")
        if payload.get("promotion_executed") is not False:
            raise ValueError("non-executed apply must not mark promotion_executed=true")
    if payload.get("apply_decision") == DECISION_APPLIED:
        if payload.get("apply_executed") is not True:
            raise ValueError("APPLIED result must have apply_executed=true")
        if payload.get("promotion_executed") is not True:
            raise ValueError("APPLIED result must have promotion_executed=true")
        if payload.get("production_effect") != PRODUCTION_EFFECT_APPLIED:
            raise ValueError("APPLIED result must record production profile update effect")


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_structured_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            payload = json.loads(text)
        else:
            payload = yaml.safe_load(text) or {}
    except (OSError, json.JSONDecodeError, yaml.YAMLError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _weights_from_mapping(payload: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in payload.items():
        parsed = _optional_float(value)
        if parsed is not None:
            weights[str(key)] = parsed
    return weights


def _rounded_weights(weights: dict[str, float]) -> dict[str, float]:
    return {key: round(value, 10) for key, value in sorted(weights.items())}


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    if isinstance(value, set):
        return [str(item) for item in value if str(item)]
    return []


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _optional_float(value: Any) -> float | None:
    try:
        if isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _format_float(value: Any) -> str:
    parsed = _optional_float(value)
    return "NA" if parsed is None else f"{parsed:.4f}"


def _format_signed_float(value: Any) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "NA"
    sign = "+" if parsed >= 0 else ""
    return f"{sign}{parsed:.4f}"


def _escape_table(value: Any) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
