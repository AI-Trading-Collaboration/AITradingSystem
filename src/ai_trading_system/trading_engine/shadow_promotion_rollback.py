from __future__ import annotations

import copy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.shadow_promotion_apply import (
    DEFAULT_DATA_ROOT,
    DEFAULT_PRODUCTION_PROFILE_PATH,
    FORBIDDEN_APPROVAL_FIELDS,
    WEIGHT_MATCH_TOLERANCE,
    WEIGHT_SUM_TOLERANCE,
    _artifact_record,
    _atomic_write_profile,
    _check,
    _check_status,
    _declared_path,
    _format_float,
    _format_signed_float,
    _key_reason,
    _mapping,
    _only_weight_field_changed,
    _path_matches,
    _read_json_object,
    _read_structured_object,
    _records,
    _sha256,
    _string_value,
    _strings,
    _target_metadata_checks,
    _validation_row,
    _weights_match,
    _write_json,
    _write_text,
    get_profile_weights,
    set_profile_weights,
)

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "shadow_promotion_rollback_result"
RUN_REPORT_TYPE = "shadow_promotion_rollback_run"
TASK_ID = "TRADING-018E3"
SOURCE_APPLY_TASK_ID = "TRADING-018E2"
MODE = "explicit_approved_rollback"
APPROVAL_TYPE = "shadow_promotion_rollback"
DEFAULT_TARGET_PROFILE_NAME = "production"
DANGER_FLAG = "--i-understand-this-rolls-back-production"
PRODUCTION_EFFECT_NONE = "none"
PRODUCTION_EFFECT_ROLLED_BACK = "profile_rolled_back_only_if_rollback_executed"

DECISION_ROLLED_BACK = "ROLLED_BACK"
DECISION_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
DECISION_APPROVAL_INVALID = "APPROVAL_INVALID"
DECISION_APPLY_RESULT_INVALID = "APPLY_RESULT_INVALID"
DECISION_DANGER_FLAG_MISSING = "DANGER_FLAG_MISSING"
DECISION_ROLLBACK_SNAPSHOT_INVALID = "ROLLBACK_SNAPSHOT_INVALID"
DECISION_TARGET_PROFILE_CHANGED = "TARGET_PROFILE_CHANGED"
DECISION_TARGET_PROFILE_MISMATCH = "TARGET_PROFILE_MISMATCH"
DECISION_CURRENT_SNAPSHOT_FAILED = "CURRENT_SNAPSHOT_FAILED"
DECISION_WRITE_FAILED = "WRITE_FAILED"
DECISION_POST_ROLLBACK_VALIDATION_FAILED = "POST_ROLLBACK_VALIDATION_FAILED"
DECISION_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DECISION_ERROR = "ERROR"

ROLLBACK_DECISIONS = {
    DECISION_ROLLED_BACK,
    DECISION_INSUFFICIENT_DATA,
    DECISION_APPROVAL_INVALID,
    DECISION_APPLY_RESULT_INVALID,
    DECISION_DANGER_FLAG_MISSING,
    DECISION_ROLLBACK_SNAPSHOT_INVALID,
    DECISION_TARGET_PROFILE_CHANGED,
    DECISION_TARGET_PROFILE_MISMATCH,
    DECISION_CURRENT_SNAPSHOT_FAILED,
    DECISION_WRITE_FAILED,
    DECISION_POST_ROLLBACK_VALIDATION_FAILED,
    DECISION_SAFETY_BLOCKED,
    DECISION_ERROR,
}

REPO_ROOT = Path(__file__).resolve().parents[3]


def default_apply_result_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "apply"
        / f"shadow_promotion_apply_result_{as_of.isoformat()}.json"
    )


def default_rollback_approval_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "manual_approvals"
        / f"shadow_promotion_rollback_approval_{as_of.isoformat()}.json"
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


def default_rollback_results_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "promotion" / "rollback_results"


def default_rollback_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_rollback_results_root(data_root)
        / f"shadow_promotion_rollback_result_{as_of.isoformat()}.json"
    )


def default_rollback_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_rollback_results_root(data_root)
        / "logs"
        / f"shadow_promotion_rollback_run_{as_of.isoformat()}.json"
    )


def default_current_snapshot_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback_current_snapshots"
        / f"production_profile_before_rollback_{as_of.isoformat()}.json"
    )


def write_shadow_promotion_rollback_report(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    apply_result_file: Path | None = None,
    rollback_approval_file: Path | None = None,
    target_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    expected_current_profile_sha256: str | None = None,
    danger_flag_provided: bool = False,
    write_mode: str = "atomic",
    fail_on_warning: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_rollback_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = default_rollback_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    apply_result_path = apply_result_file or default_apply_result_path(data_root, as_of)
    approval_path = rollback_approval_file or default_rollback_approval_path(data_root, as_of)
    apply_result = _read_json_object(apply_result_path)
    approval = _read_json_object(approval_path)
    rollback_snapshot_path = _resolve_rollback_snapshot_path(
        data_root=data_root,
        as_of=as_of,
        apply_result=apply_result,
        approval=approval,
    )
    rollback_snapshot = _read_structured_object(rollback_snapshot_path)
    target_profile = _read_structured_object(target_profile_path)
    target_sha_before = _sha256(target_profile_path) if target_profile_path.is_file() else ""

    input_artifacts = {
        "apply_result": _artifact_record(apply_result_path),
        "rollback_approval": _artifact_record(approval_path),
        "rollback_snapshot": _artifact_record(rollback_snapshot_path),
        "target_profile_before_rollback": _artifact_record(target_profile_path),
    }
    apply_sha256 = _string_value(input_artifacts["apply_result"].get("sha256"))
    approval_sha256 = _string_value(input_artifacts["rollback_approval"].get("sha256"))
    snapshot_sha256 = _string_value(input_artifacts["rollback_snapshot"].get("sha256"))

    current_weight_field, current_weights = get_profile_weights(target_profile)
    snapshot_weight_field, rollback_weights = get_profile_weights(rollback_snapshot)
    apply_result_validation = _validate_apply_result(
        apply_result=apply_result,
        rollback_snapshot_path=rollback_snapshot_path,
        rollback_snapshot_sha256=snapshot_sha256,
    )
    rollback_snapshot_validation = _validate_rollback_snapshot(
        rollback_snapshot=rollback_snapshot,
        rollback_snapshot_path=rollback_snapshot_path,
        rollback_snapshot_sha256=snapshot_sha256,
        apply_result=apply_result,
        current_weights=current_weights,
        snapshot_weight_field=snapshot_weight_field,
        rollback_weights=rollback_weights,
    )
    approval_validation = _validate_approval(
        approval=approval,
        apply_result=apply_result,
        apply_result_path=apply_result_path,
        rollback_snapshot_path=rollback_snapshot_path,
        target_profile_path=target_profile_path,
        apply_result_sha256=apply_sha256,
        rollback_snapshot_sha256=snapshot_sha256,
        current_target_sha256=target_sha_before,
    )
    target_profile_validation = _validate_target_profile(
        approval=approval,
        apply_result=apply_result,
        target_profile=target_profile,
        target_profile_path=target_profile_path,
        current_sha256=target_sha_before,
        expected_current_profile_sha256=expected_current_profile_sha256,
        current_weight_field=current_weight_field,
        current_weights=current_weights,
        rollback_weights=rollback_weights,
        snapshot_sha256=snapshot_sha256,
    )
    danger_flag_validation = {
        "status": "PASS" if danger_flag_provided else "FAIL",
        "required_flag": DANGER_FLAG,
        "provided": danger_flag_provided,
    }
    warnings = _warning_reasons(approval=approval, write_mode=write_mode)
    safety_validation = _validate_safety(write_mode=write_mode, warnings=warnings)
    rollback_applied = _build_rollback_applied(
        production_weights_before_rollback=current_weights,
        production_weights_after_rollback=rollback_weights,
    )
    current_snapshot = _empty_current_snapshot(data_root=data_root, as_of=as_of)
    post_rollback_validation = _empty_post_rollback_validation()

    rollback_decision = _pre_write_decision(
        input_artifacts=input_artifacts,
        danger_flag_validation=danger_flag_validation,
        apply_result_validation=apply_result_validation,
        rollback_snapshot_validation=rollback_snapshot_validation,
        approval_validation=approval_validation,
        target_profile_validation=target_profile_validation,
        safety_validation=safety_validation,
        fail_on_warning=fail_on_warning,
        warnings=warnings,
    )
    rollback_executed = False
    atomic_write_used = False
    profile_sha_after = ""

    if rollback_decision == "READY_TO_ROLLBACK":
        try:
            current_snapshot = _create_current_snapshot(
                data_root=data_root,
                as_of=as_of,
                target_profile=target_profile,
                target_profile_sha256=target_sha_before,
            )
        except OSError as exc:
            rollback_decision = DECISION_CURRENT_SNAPSHOT_FAILED
            current_snapshot = _current_snapshot_failed(
                data_root=data_root,
                as_of=as_of,
                reason=str(exc),
            )

    if rollback_decision == "READY_TO_ROLLBACK":
        try:
            restored_profile = set_profile_weights(
                profile=target_profile,
                weight_field=current_weight_field,
                weights=rollback_weights,
            )
            _atomic_write_profile(target_profile_path, restored_profile, write_mode=write_mode)
            rollback_executed = True
            atomic_write_used = write_mode == "atomic"
        except OSError as exc:
            rollback_decision = DECISION_WRITE_FAILED
            post_rollback_validation = _post_rollback_failed(f"write_failed:{exc}")

    if rollback_decision == "READY_TO_ROLLBACK" and rollback_executed:
        profile_after = _read_structured_object(target_profile_path)
        profile_sha_after = _sha256(target_profile_path) if target_profile_path.is_file() else ""
        post_rollback_validation = _validate_post_rollback(
            profile_before=target_profile,
            profile_after=profile_after,
            rollback_snapshot=rollback_snapshot,
            weight_field=current_weight_field,
            rollback_weights=rollback_weights,
            snapshot_sha256=snapshot_sha256,
            sha256_before=target_sha_before,
            sha256_after=profile_sha_after,
        )
        target_profile_validation["sha256_after_rollback"] = profile_sha_after
        target_profile_validation["hash_changed_after_rollback"] = (
            bool(profile_sha_after) and profile_sha_after != target_sha_before
        )
        rollback_decision = (
            DECISION_ROLLED_BACK
            if post_rollback_validation.get("status") == "PASS"
            else DECISION_POST_ROLLBACK_VALIDATION_FAILED
        )

    payload = _build_result_payload(
        as_of=as_of,
        generated_at=generated,
        rollback_decision=rollback_decision,
        input_artifacts=input_artifacts,
        approval_validation=approval_validation,
        apply_result_validation=apply_result_validation,
        danger_flag_validation=danger_flag_validation,
        target_profile_validation=target_profile_validation,
        rollback_snapshot_validation=rollback_snapshot_validation,
        safety_validation=safety_validation,
        rollback_applied=rollback_applied,
        current_snapshot=current_snapshot,
        post_rollback_validation=post_rollback_validation,
        warnings=warnings,
        rollback_executed=rollback_executed,
        atomic_write_used=atomic_write_used,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        run_log_json_path=run_log_json_path,
        run_log_md_path=run_log_md_path,
        target_profile_path=target_profile_path,
        profile_sha_after=profile_sha_after,
        approval_sha256=approval_sha256,
    )
    _assert_rollback_safety_invariants(payload)
    _write_json(output_json_path, payload)
    _write_text(output_md_path, render_shadow_promotion_rollback_report(payload))

    run_log = _run_log_payload(payload=payload, generated_at=generated)
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_shadow_promotion_rollback_run_log(run_log))
    return payload


def render_shadow_promotion_rollback_report(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    approval_validation = _mapping(payload.get("approval_validation"))
    apply_validation = _mapping(payload.get("apply_result_validation"))
    danger = _mapping(payload.get("danger_flag_validation"))
    rollback = _mapping(payload.get("rollback_applied"))
    current_snapshot = _mapping(payload.get("current_snapshot"))
    post = _mapping(payload.get("post_rollback_validation"))
    before = _mapping(rollback.get("production_weights_before_rollback"))
    after = _mapping(rollback.get("production_weights_after_rollback"))
    delta = _mapping(rollback.get("delta"))

    lines = [
        f"# Shadow Promotion Rollback Result - {payload.get('date')}",
        "",
        "## 1. Run Summary",
        "",
        f"- Rollback Decision: `{payload.get('rollback_decision')}`",
        f"- Rollback Executed: `{str(payload.get('rollback_executed')).lower()}`",
        f"- Production Effect: `{payload.get('production_effect')}`",
        f"- Manual Review Only: `{str(payload.get('manual_review_only')).lower()}`",
        f"- Safe For Scheduler: `{str(payload.get('safe_for_scheduler')).lower()}`",
        f"- Broker Execution: `{str(payload.get('broker_execution')).lower()}`",
        f"- Replay Execution: `{str(payload.get('replay_execution')).lower()}`",
        f"- Trading Execution: `{str(payload.get('trading_execution')).lower()}`",
        "",
    ]
    if payload.get("rollback_executed") is not True:
        lines.extend(
            [
                "Rollback was not executed.",
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
    for key in (
        "apply_result",
        "rollback_approval",
        "rollback_snapshot",
        "target_profile_before_rollback",
    ):
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
            "## 4. Apply Result Validation",
            "",
            "| Check | Status | Reason |",
            "|---|---:|---|",
        ],
    )
    for check in _records(apply_validation.get("checks")):
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
            "## 6. Rollback Applied",
            "",
            "| Weight Key | Before Rollback | After Rollback | Delta |",
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
            "## 7. Current Snapshot",
            "",
            (
                "- Snapshot created: "
                f"`{str(current_snapshot.get('snapshot_created') is True).lower()}`"
            ),
            f"- Snapshot path: `{current_snapshot.get('snapshot_path', '')}`",
            f"- Snapshot SHA256: `{current_snapshot.get('snapshot_sha256', '')}`",
            "",
            "## 8. Post-Rollback Validation",
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
                "This task only restores the target production profile weights after explicit "
                "manual rollback approval."
            ),
            "",
            "It does not:",
            "- run broker execution",
            "- run replay execution",
            "- run trading execution",
            "- schedule automatic rollback",
            "",
            "## 10. Next Step",
            "",
            "Run downstream validation manually before relying on the restored production profile.",
            "",
        ],
    )
    return "\n".join(lines)


def render_shadow_promotion_rollback_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Shadow Promotion Rollback Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- rollback_decision: `{payload.get('rollback_decision')}`",
            f"- production_effect: `{payload.get('production_effect')}`",
            f"- manual_review_only: `{str(payload.get('manual_review_only')).lower()}`",
            f"- rollback_executed: `{str(payload.get('rollback_executed')).lower()}`",
            f"- safe_for_scheduler: `{str(payload.get('safe_for_scheduler')).lower()}`",
            f"- broker_execution: `{str(payload.get('broker_execution')).lower()}`",
            f"- replay_execution: `{str(payload.get('replay_execution')).lower()}`",
            f"- trading_execution: `{str(payload.get('trading_execution')).lower()}`",
            f"- rollback_result_json: `{payload.get('rollback_result_json')}`",
            f"- rollback_result_markdown: `{payload.get('rollback_result_markdown')}`",
            "",
        ],
    )


def _validate_apply_result(
    *,
    apply_result: dict[str, Any],
    rollback_snapshot_path: Path,
    rollback_snapshot_sha256: str,
) -> dict[str, Any]:
    rollback = _mapping(apply_result.get("rollback"))
    post_apply = _mapping(apply_result.get("post_apply_validation"))
    apply_snapshot_sha256 = _apply_result_snapshot_sha256(apply_result)
    target_sha_after = _apply_result_target_sha256_after(apply_result)
    checks = [
        _check("apply_result_exists", bool(apply_result), "apply result JSON loaded"),
        _check(
            "task_id",
            apply_result.get("task_id") == SOURCE_APPLY_TASK_ID,
            SOURCE_APPLY_TASK_ID,
        ),
        _check(
            "apply_decision",
            _string_value(apply_result.get("apply_decision")) == "APPLIED",
            _string_value(apply_result.get("apply_decision")),
        ),
        _check("apply_executed", apply_result.get("apply_executed") is True, "true"),
        _check("promotion_executed", apply_result.get("promotion_executed") is True, "true"),
        _check(
            "rollback_snapshot_created",
            rollback.get("snapshot_created") is True,
            str(rollback.get("snapshot_created")),
        ),
        _check(
            "rollback_supported",
            rollback.get("rollback_supported") is True,
            str(rollback.get("rollback_supported")),
        ),
        _check(
            "rollback_snapshot_path",
            _path_matches(_string_value(rollback.get("snapshot_path")), rollback_snapshot_path),
            _string_value(rollback.get("snapshot_path")),
        ),
        _check(
            "rollback_snapshot_sha256",
            bool(apply_snapshot_sha256),
            f"apply={apply_snapshot_sha256}, actual={rollback_snapshot_sha256}",
        ),
        _check(
            "target_profile_sha256_after",
            bool(target_sha_after),
            "required apply-after target profile sha256",
        ),
        _check(
            "post_apply_validation_status",
            post_apply.get("status") == "PASS",
            _string_value(post_apply.get("status")),
        ),
        _check("broker_execution", apply_result.get("broker_execution") is False, "false"),
        _check("replay_execution", apply_result.get("replay_execution") is False, "false"),
        _check("trading_execution", apply_result.get("trading_execution") is False, "false"),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "apply_decision": _string_value(apply_result.get("apply_decision")),
        "apply_executed": apply_result.get("apply_executed") is True,
        "promotion_executed": apply_result.get("promotion_executed") is True,
        "rollback_snapshot_created": rollback.get("snapshot_created") is True,
        "rollback_supported": rollback.get("rollback_supported") is True,
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _validate_approval(
    *,
    approval: dict[str, Any],
    apply_result: dict[str, Any],
    apply_result_path: Path,
    rollback_snapshot_path: Path,
    target_profile_path: Path,
    apply_result_sha256: str,
    rollback_snapshot_sha256: str,
    current_target_sha256: str,
) -> dict[str, Any]:
    apply_section = _mapping(approval.get("apply_result"))
    snapshot_section = _mapping(approval.get("rollback_snapshot"))
    target = _mapping(approval.get("target"))
    rollback_scope = _mapping(approval.get("rollback_scope"))
    safety = _mapping(approval.get("safety_acknowledgement"))
    forbidden_fields = set(_strings(rollback_scope.get("forbidden_fields")))
    allowed_fields = set(_strings(rollback_scope.get("allowed_fields")))
    expected_current_sha = _string_value(target.get("expected_current_profile_sha256"))
    expected_rollback_sha = _string_value(target.get("expected_rollback_profile_sha256"))
    checks = [
        _check("approval_exists", bool(approval), "rollback approval JSON loaded"),
        _check("approval_type", approval.get("approval_type") == APPROVAL_TYPE, APPROVAL_TYPE),
        _check("approved", approval.get("approved") is True, str(approval.get("approved"))),
        _check(
            "apply_result_file",
            _path_matches(_string_value(apply_section.get("apply_result_file")), apply_result_path),
            _string_value(apply_section.get("apply_result_file")),
        ),
        _check(
            "apply_result_hash_match",
            bool(apply_result_sha256)
            and _string_value(apply_section.get("apply_result_sha256")) == apply_result_sha256,
            _string_value(apply_section.get("apply_result_sha256")),
        ),
        _check(
            "apply_decision",
            _string_value(apply_section.get("apply_decision")) == "APPLIED"
            and _string_value(apply_result.get("apply_decision")) == "APPLIED",
            _string_value(apply_section.get("apply_decision")),
        ),
        _check(
            "apply_executed",
            apply_section.get("apply_executed") is True
            and apply_result.get("apply_executed") is True,
            str(apply_section.get("apply_executed")),
        ),
        _check(
            "rollback_snapshot_file",
            _path_matches(
                _string_value(snapshot_section.get("snapshot_file")),
                rollback_snapshot_path,
            ),
            _string_value(snapshot_section.get("snapshot_file")),
        ),
        _check(
            "rollback_snapshot_hash_match",
            bool(rollback_snapshot_sha256)
            and _string_value(snapshot_section.get("snapshot_sha256")) == rollback_snapshot_sha256,
            _string_value(snapshot_section.get("snapshot_sha256")),
        ),
        _check(
            "target_profile_path",
            _path_matches(_string_value(target.get("target_profile_path")), target_profile_path),
            _string_value(target.get("target_profile_path")),
        ),
        _check(
            "target_profile_hash_match",
            bool(expected_current_sha) and expected_current_sha == current_target_sha256,
            f"approval={expected_current_sha}, current={current_target_sha256}",
        ),
        _check(
            "expected_rollback_profile_sha256",
            bool(expected_rollback_sha) and expected_rollback_sha == rollback_snapshot_sha256,
            f"approval={expected_rollback_sha}, snapshot={rollback_snapshot_sha256}",
        ),
        _check(
            "allowed_fields_weights_only",
            "weights" in allowed_fields,
            ",".join(sorted(allowed_fields)),
        ),
        _check(
            "forbidden_fields_declared",
            FORBIDDEN_APPROVAL_FIELDS.issubset(forbidden_fields),
            ",".join(sorted(FORBIDDEN_APPROVAL_FIELDS - forbidden_fields)),
        ),
        _check(
            "rollback_authorized",
            safety.get("rollback_authorized") is True,
            str(safety.get("rollback_authorized")),
        ),
        _check(
            "production_modification_authorized",
            safety.get("production_modification_authorized") is True,
            str(safety.get("production_modification_authorized")),
        ),
        _check(
            "weights_only_restore",
            safety.get("weights_only_restore") is True,
            str(safety.get("weights_only_restore")),
        ),
        _check(
            "current_snapshot_required",
            safety.get("current_snapshot_required") is True,
            str(safety.get("current_snapshot_required")),
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
        "apply_result_hash_match": _check_status(checks, "apply_result_hash_match"),
        "rollback_snapshot_hash_match": _check_status(checks, "rollback_snapshot_hash_match"),
        "target_profile_hash_match": _check_status(checks, "target_profile_hash_match"),
        "rollback_authorized": safety.get("rollback_authorized") is True,
        "production_modification_authorized": (
            safety.get("production_modification_authorized") is True
        ),
        "weights_only_restore": safety.get("weights_only_restore") is True,
        "current_snapshot_required": safety.get("current_snapshot_required") is True,
        "manual_command_required": safety.get("manual_command_required") is True,
        "scheduler_execution_forbidden": safety.get("scheduler_execution_forbidden") is True,
        "broker_execution_forbidden": safety.get("broker_execution_forbidden") is True,
        "replay_execution_forbidden": safety.get("replay_execution_forbidden") is True,
        "trading_execution_forbidden": safety.get("trading_execution_forbidden") is True,
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _validate_rollback_snapshot(
    *,
    rollback_snapshot: dict[str, Any],
    rollback_snapshot_path: Path,
    rollback_snapshot_sha256: str,
    apply_result: dict[str, Any],
    current_weights: dict[str, float],
    snapshot_weight_field: str,
    rollback_weights: dict[str, float],
) -> dict[str, Any]:
    apply_snapshot_sha = _apply_result_snapshot_sha256(apply_result)
    weights_sum = sum(rollback_weights.values())
    key_check_required = bool(current_weights)
    checks = [
        _check(
            "rollback_snapshot_exists",
            bool(rollback_snapshot),
            str(rollback_snapshot_path),
        ),
        _check(
            "rollback_snapshot_hash_matches_apply_result",
            bool(rollback_snapshot_sha256)
            and bool(apply_snapshot_sha)
            and rollback_snapshot_sha256 == apply_snapshot_sha,
            f"snapshot={rollback_snapshot_sha256}, apply={apply_snapshot_sha}",
        ),
        _check("rollback_snapshot_parseable", bool(rollback_snapshot), "structured object"),
        _check("weight_field_supported", bool(snapshot_weight_field), snapshot_weight_field),
        _check(
            "weights_present",
            bool(rollback_weights),
            f"key_count={len(rollback_weights)}",
        ),
        _check(
            "weights_in_range",
            bool(rollback_weights)
            and all(0.0 <= value <= 1.0 for value in rollback_weights.values()),
            "[0, 1]",
        ),
        _check(
            "weights_sum_valid",
            bool(rollback_weights) and abs(weights_sum - 1.0) <= WEIGHT_SUM_TOLERANCE,
            f"sum={weights_sum:.10f}, tolerance={WEIGHT_SUM_TOLERANCE:.10f}",
        ),
        _check(
            "weight_keys_match_current",
            not key_check_required or set(rollback_weights) == set(current_weights),
            _key_reason(set(rollback_weights), set(current_weights)),
        ),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "path": str(rollback_snapshot_path),
        "sha256": rollback_snapshot_sha256,
        "weight_field": snapshot_weight_field,
        "weight_keys_match": _check_status(checks, "weight_keys_match_current"),
        "weights_sum_valid": _check_status(checks, "weights_sum_valid"),
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _validate_target_profile(
    *,
    approval: dict[str, Any],
    apply_result: dict[str, Any],
    target_profile: dict[str, Any],
    target_profile_path: Path,
    current_sha256: str,
    expected_current_profile_sha256: str | None,
    current_weight_field: str,
    current_weights: dict[str, float],
    rollback_weights: dict[str, float],
    snapshot_sha256: str,
) -> dict[str, Any]:
    target = _mapping(approval.get("target"))
    target_profile_name = (
        _string_value(target.get("target_profile_name")) or DEFAULT_TARGET_PROFILE_NAME
    )
    apply_after_sha = _apply_result_target_sha256_after(apply_result)
    approval_expected_current_sha = _string_value(target.get("expected_current_profile_sha256"))
    approval_expected_rollback_sha = _string_value(target.get("expected_rollback_profile_sha256"))
    cli_expected_ok = (
        True
        if not expected_current_profile_sha256
        else current_sha256 == expected_current_profile_sha256
    )
    metadata_checks = _target_metadata_checks(
        target_profile=target_profile,
        target_profile_name=target_profile_name,
    )
    apply_target_path = _apply_result_target_profile_path(apply_result)
    checks = [
        _check("target_profile_exists", bool(target_profile), str(target_profile_path)),
        _check(
            "target_profile_path",
            _path_matches(_string_value(target.get("target_profile_path")), target_profile_path),
            _string_value(target.get("target_profile_path")),
        ),
        _check(
            "target_profile_path_matches_apply_result",
            _path_matches(apply_target_path, target_profile_path),
            apply_target_path,
        ),
        _check("weight_field_supported", bool(current_weight_field), current_weight_field),
        _check(
            "weight_keys_match_rollback_snapshot",
            bool(current_weights) and set(current_weights) == set(rollback_weights),
            _key_reason(set(current_weights), set(rollback_weights)),
        ),
        _check(
            "current_matches_apply_after_sha256",
            bool(current_sha256) and current_sha256 == apply_after_sha,
            f"current={current_sha256}, apply_after={apply_after_sha}",
        ),
        _check(
            "current_matches_approval_expected_sha256",
            bool(current_sha256) and current_sha256 == approval_expected_current_sha,
            f"current={current_sha256}, approval={approval_expected_current_sha}",
        ),
        _check(
            "current_matches_cli_expected_sha256",
            cli_expected_ok,
            f"current={current_sha256}, cli={expected_current_profile_sha256 or ''}",
        ),
        _check(
            "expected_rollback_profile_sha256",
            bool(snapshot_sha256) and approval_expected_rollback_sha == snapshot_sha256,
            f"approval={approval_expected_rollback_sha}, snapshot={snapshot_sha256}",
        ),
    ]
    checks.extend(metadata_checks)
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    hash_blocking = [
        reason
        for reason in blocking
        if reason
        in {
            "current_matches_apply_after_sha256",
            "current_matches_approval_expected_sha256",
            "current_matches_cli_expected_sha256",
        }
    ]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "path": str(target_profile_path),
        "weight_field": current_weight_field,
        "sha256_before_rollback": current_sha256,
        "sha256_expected_current": approval_expected_current_sha or apply_after_sha,
        "sha256_expected_rollback": snapshot_sha256,
        "sha256_after_rollback": "",
        "hash_changed_after_rollback": False,
        "blocking_reasons": blocking,
        "hash_blocking_reasons": hash_blocking,
        "checks": checks,
    }


def _validate_safety(*, write_mode: str, warnings: list[str]) -> dict[str, Any]:
    checks = [
        _check("write_mode_atomic", write_mode == "atomic", write_mode),
        _check("safe_for_scheduler_false", True, "018E3 is manual only"),
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
    apply_result_validation: dict[str, Any],
    rollback_snapshot_validation: dict[str, Any],
    approval_validation: dict[str, Any],
    target_profile_validation: dict[str, Any],
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
    if apply_result_validation.get("status") != "PASS":
        return DECISION_APPLY_RESULT_INVALID
    if rollback_snapshot_validation.get("status") != "PASS":
        return DECISION_ROLLBACK_SNAPSHOT_INVALID

    target_blocking = set(_strings(target_profile_validation.get("blocking_reasons")))
    hash_blocking = set(_strings(target_profile_validation.get("hash_blocking_reasons")))
    target_non_hash_blocking = target_blocking - hash_blocking
    if target_non_hash_blocking:
        return DECISION_TARGET_PROFILE_MISMATCH

    approval_blocking = set(_strings(approval_validation.get("blocking_reasons")))
    approval_non_current_hash_blocking = approval_blocking - {"target_profile_hash_match"}
    if approval_non_current_hash_blocking:
        return DECISION_APPROVAL_INVALID
    if hash_blocking or "target_profile_hash_match" in approval_blocking:
        return DECISION_TARGET_PROFILE_CHANGED
    return "READY_TO_ROLLBACK"


def _build_result_payload(
    *,
    as_of: date,
    generated_at: datetime,
    rollback_decision: str,
    input_artifacts: dict[str, Any],
    approval_validation: dict[str, Any],
    apply_result_validation: dict[str, Any],
    danger_flag_validation: dict[str, Any],
    target_profile_validation: dict[str, Any],
    rollback_snapshot_validation: dict[str, Any],
    safety_validation: dict[str, Any],
    rollback_applied: dict[str, Any],
    current_snapshot: dict[str, Any],
    post_rollback_validation: dict[str, Any],
    warnings: list[str],
    rollback_executed: bool,
    atomic_write_used: bool,
    output_json_path: Path,
    output_md_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    target_profile_path: Path,
    profile_sha_after: str,
    approval_sha256: str,
) -> dict[str, Any]:
    decision = rollback_decision if rollback_decision in ROLLBACK_DECISIONS else DECISION_ERROR
    production_effect = (
        PRODUCTION_EFFECT_ROLLED_BACK if rollback_executed else PRODUCTION_EFFECT_NONE
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "production_effect": production_effect,
        "manual_review_only": True,
        "rollback_executed": rollback_executed,
        "safe_for_scheduler": False,
        "safe_for_production": decision == DECISION_ROLLED_BACK,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "rollback_decision": decision,
        "rollback_reason": _rollback_reason(decision),
        "input_artifacts": input_artifacts,
        "approval_validation": approval_validation,
        "apply_result_validation": apply_result_validation,
        "danger_flag_validation": danger_flag_validation,
        "target_profile_validation": target_profile_validation,
        "rollback_snapshot_validation": rollback_snapshot_validation,
        "safety_validation": safety_validation,
        "rollback_applied": rollback_applied,
        "current_snapshot": current_snapshot,
        "post_rollback_validation": post_rollback_validation,
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
            "runs_promotion_apply": False,
            "runs_promotion_rollback": rollback_executed,
            "runs_scoring_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": rollback_executed,
            "writes_production_weights": rollback_executed,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "production_effect": production_effect,
            "manual_review_only": True,
        },
        "audit": {
            "created_by": "scripts/run_shadow_promotion_rollback.py",
            "created_at": generated_at.isoformat(),
            "atomic_write_used": atomic_write_used,
            "approval_sha256": approval_sha256,
            "no_broker_execution": True,
            "no_replay_execution": True,
            "no_trading_execution": True,
            "scheduler_safe": False,
            "target_profile_path": str(target_profile_path),
            "target_profile_sha256_after": profile_sha_after,
        },
    }
    return payload


def _rollback_reason(decision: str) -> str:
    if decision == DECISION_ROLLED_BACK:
        return (
            "Apply result, rollback approval, rollback snapshot, current profile hash, "
            "danger flag, current snapshot, and post-rollback validation all passed."
        )
    if decision == DECISION_INSUFFICIENT_DATA:
        return (
            "Required apply result, rollback approval, rollback snapshot, or target profile "
            "is missing."
        )
    if decision == DECISION_APPROVAL_INVALID:
        return (
            "Rollback approval artifact is missing required authorization or does not match "
            "inputs."
        )
    if decision == DECISION_APPLY_RESULT_INVALID:
        return (
            "Apply result is not a successful TRADING-018E2 APPLIED result with rollback "
            "support."
        )
    if decision == DECISION_DANGER_FLAG_MISSING:
        return "Required explicit rollback danger flag was not provided."
    if decision == DECISION_ROLLBACK_SNAPSHOT_INVALID:
        return "Rollback snapshot hash, schema, keys, or weights failed validation."
    if decision == DECISION_TARGET_PROFILE_CHANGED:
        return (
            "Current target profile sha256 differs from the apply-after or approval expected "
            "hash."
        )
    if decision == DECISION_TARGET_PROFILE_MISMATCH:
        return (
            "Target profile path, metadata, weight field, or weight keys do not match rollback "
            "inputs."
        )
    if decision == DECISION_CURRENT_SNAPSHOT_FAILED:
        return "Current production snapshot could not be saved before rollback."
    if decision == DECISION_WRITE_FAILED:
        return "Atomic production profile rollback write failed."
    if decision == DECISION_POST_ROLLBACK_VALIDATION_FAILED:
        return "Production profile write completed but post-rollback validation failed."
    if decision == DECISION_SAFETY_BLOCKED:
        return "One or more rollback safety boundaries failed."
    return "Unexpected rollback error."


def _create_current_snapshot(
    *,
    data_root: Path,
    as_of: date,
    target_profile: dict[str, Any],
    target_profile_sha256: str,
) -> dict[str, Any]:
    snapshot_path = default_current_snapshot_path(data_root, as_of)
    sha_path = snapshot_path.with_suffix(".sha256")
    _write_json(snapshot_path, target_profile)
    _write_text(sha_path, target_profile_sha256 + "\n")
    return {
        "snapshot_created": True,
        "snapshot_path": str(snapshot_path),
        "snapshot_sha256": target_profile_sha256,
        "snapshot_file_sha256": _sha256(snapshot_path),
        "snapshot_sha256_path": str(sha_path),
        "blocking_reasons": [],
    }


def _empty_current_snapshot(*, data_root: Path, as_of: date) -> dict[str, Any]:
    snapshot_path = default_current_snapshot_path(data_root, as_of)
    return {
        "snapshot_created": False,
        "snapshot_path": str(snapshot_path),
        "snapshot_sha256": "",
        "snapshot_file_sha256": "",
        "snapshot_sha256_path": str(snapshot_path.with_suffix(".sha256")),
        "blocking_reasons": [],
    }


def _current_snapshot_failed(*, data_root: Path, as_of: date, reason: str) -> dict[str, Any]:
    snapshot = _empty_current_snapshot(data_root=data_root, as_of=as_of)
    snapshot["blocking_reasons"] = [reason]
    return snapshot


def _validate_post_rollback(
    *,
    profile_before: dict[str, Any],
    profile_after: dict[str, Any],
    rollback_snapshot: dict[str, Any],
    weight_field: str,
    rollback_weights: dict[str, float],
    snapshot_sha256: str,
    sha256_before: str,
    sha256_after: str,
) -> dict[str, Any]:
    after_field, weights_after = get_profile_weights(profile_after)
    _, snapshot_weights = get_profile_weights(rollback_snapshot)
    weights_match_rollback_snapshot = _weights_match(weights_after, rollback_weights)
    weight_keys_match = set(weights_after) == set(rollback_weights) and bool(weights_after)
    weights_sum = sum(weights_after.values())
    weights_sum_valid = bool(weights_after) and abs(weights_sum - 1.0) <= WEIGHT_SUM_TOLERANCE
    only_allowed_fields_changed = _only_weight_field_changed(
        before=profile_before,
        after=profile_after,
        weight_field=weight_field,
    )
    profile_matches_snapshot = bool(sha256_after) and sha256_after == snapshot_sha256
    hash_changed = bool(sha256_after) and sha256_after != sha256_before
    forbidden_fields_unchanged = _forbidden_fields_unchanged(
        before=profile_before,
        after=profile_after,
    )
    checks = [
        _check(
            "profile_matches_rollback_snapshot",
            profile_matches_snapshot or weights_match_rollback_snapshot,
            f"profile={sha256_after}, snapshot={snapshot_sha256}, mode=weights_only",
        ),
        _check(
            "weights_match_rollback_snapshot",
            weights_match_rollback_snapshot and _weights_match(weights_after, snapshot_weights),
            "post-rollback weights equal rollback snapshot weights",
        ),
        _check(
            "weight_keys_match",
            weight_keys_match,
            _key_reason(set(weights_after), set(rollback_weights)),
        ),
        _check(
            "weights_sum_valid",
            weights_sum_valid,
            f"sum={weights_sum:.10f}, tolerance={WEIGHT_SUM_TOLERANCE:.10f}",
        ),
        _check("only_allowed_fields_changed", only_allowed_fields_changed, weight_field),
        _check("forbidden_fields_unchanged", forbidden_fields_unchanged, "forbidden fields"),
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
        "profile_matches_rollback_snapshot": profile_matches_snapshot,
        "weights_match_rollback_snapshot": weights_match_rollback_snapshot,
        "weight_keys_match": weight_keys_match,
        "weights_sum_valid": weights_sum_valid,
        "only_allowed_fields_changed": only_allowed_fields_changed,
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _empty_post_rollback_validation() -> dict[str, Any]:
    checks = [
        _check("profile_matches_rollback_snapshot", False, "rollback not executed"),
        _check("weights_match_rollback_snapshot", False, "rollback not executed"),
        _check("weight_keys_match", False, "rollback not executed"),
        _check("weights_sum_valid", False, "rollback not executed"),
        _check("only_allowed_fields_changed", False, "rollback not executed"),
    ]
    return {
        "status": "NOT_RUN",
        "profile_matches_rollback_snapshot": False,
        "weights_match_rollback_snapshot": False,
        "weight_keys_match": False,
        "weights_sum_valid": False,
        "only_allowed_fields_changed": False,
        "blocking_reasons": [],
        "checks": checks,
    }


def _post_rollback_failed(reason: str) -> dict[str, Any]:
    payload = _empty_post_rollback_validation()
    payload["status"] = "FAIL"
    payload["blocking_reasons"] = [reason]
    return payload


def _build_rollback_applied(
    *,
    production_weights_before_rollback: dict[str, float],
    production_weights_after_rollback: dict[str, float],
) -> dict[str, Any]:
    delta: dict[str, float] = {}
    all_keys = set(production_weights_before_rollback) | set(production_weights_after_rollback)
    for key in sorted(all_keys):
        before = production_weights_before_rollback.get(key)
        after = production_weights_after_rollback.get(key)
        if before is None or after is None:
            continue
        delta[key] = round(after - before, 10)
    changed = [key for key, value in delta.items() if abs(value) > WEIGHT_MATCH_TOLERANCE]
    return {
        "changed_weight_keys": changed,
        "production_weights_before_rollback": production_weights_before_rollback,
        "production_weights_after_rollback": production_weights_after_rollback,
        "delta": delta,
    }


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("outputs"))
    decision = _string_value(payload.get("rollback_decision"))
    run_log = {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "generated_at": generated_at.isoformat(),
        "run_status": "ROLLED_BACK" if decision == DECISION_ROLLED_BACK else "BLOCKED",
        "rollback_decision": decision,
        "production_effect": payload.get("production_effect"),
        "manual_review_only": True,
        "rollback_executed": payload.get("rollback_executed") is True,
        "safe_for_scheduler": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "rollback_result_json": outputs.get("json", ""),
        "rollback_result_markdown": outputs.get("markdown", ""),
        "run_log_json": outputs.get("run_log_json", ""),
        "run_log_markdown": outputs.get("run_log_markdown", ""),
        "current_snapshot": payload.get("current_snapshot", {}),
        "audit": payload.get("audit", {}),
    }
    _assert_rollback_safety_invariants(run_log)
    return run_log


def _resolve_rollback_snapshot_path(
    *,
    data_root: Path,
    as_of: date,
    apply_result: dict[str, Any],
    approval: dict[str, Any],
) -> Path:
    rollback = _mapping(apply_result.get("rollback"))
    declared = _string_value(rollback.get("snapshot_path"))
    if not declared:
        snapshot_section = _mapping(approval.get("rollback_snapshot"))
        declared = _string_value(snapshot_section.get("snapshot_file"))
    if declared:
        return _declared_path(declared)
    return default_rollback_snapshot_path(data_root, as_of)


def _apply_result_snapshot_sha256(apply_result: dict[str, Any]) -> str:
    rollback = _mapping(apply_result.get("rollback"))
    return _string_value(rollback.get("snapshot_file_sha256")) or _string_value(
        rollback.get("snapshot_sha256")
    )


def _apply_result_target_sha256_after(apply_result: dict[str, Any]) -> str:
    target = _mapping(apply_result.get("target_profile_validation"))
    audit = _mapping(apply_result.get("audit"))
    return _string_value(target.get("sha256_after")) or _string_value(
        audit.get("target_profile_sha256_after")
    )


def _apply_result_target_profile_path(apply_result: dict[str, Any]) -> str:
    target = _mapping(apply_result.get("target_profile_validation"))
    audit = _mapping(apply_result.get("audit"))
    return _string_value(target.get("path")) or _string_value(audit.get("target_profile_path"))


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


def _forbidden_fields_unchanged(*, before: dict[str, Any], after: dict[str, Any]) -> bool:
    before_copy = copy.deepcopy(before)
    after_copy = copy.deepcopy(after)
    for field in FORBIDDEN_APPROVAL_FIELDS:
        if before_copy.get(field) != after_copy.get(field):
            return False
    return True


def _assert_rollback_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("manual_review_only") is not True:
        raise ValueError("shadow promotion rollback must remain manual_review_only")
    if payload.get("safe_for_scheduler") is not False:
        raise ValueError("shadow promotion rollback must not be scheduler-safe")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"shadow promotion rollback must keep {field}=false")
    rollback_executed = payload.get("rollback_executed") is True
    if not rollback_executed and payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("non-executed rollback must have production_effect=none")
    if payload.get("rollback_decision") == DECISION_ROLLED_BACK:
        if payload.get("rollback_executed") is not True:
            raise ValueError("ROLLED_BACK result must have rollback_executed=true")
        if payload.get("production_effect") != PRODUCTION_EFFECT_ROLLED_BACK:
            raise ValueError("ROLLED_BACK result must record production profile rollback effect")
