from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "shadow_promotion_apply_preflight"
RUN_REPORT_TYPE = "shadow_promotion_apply_preflight_run"
TASK_ID = "TRADING-018E1"
SOURCE_PROPOSAL_TASK_ID = "TRADING-018D"
MODE = "approved_apply_preflight_only"
PRODUCTION_EFFECT_NONE = "none"
DEFAULT_TARGET_PROFILE_NAME = "production"

DECISION_PASS = "PASS"
DECISION_WARNING = "WARNING"
DECISION_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
DECISION_APPROVAL_INVALID = "APPROVAL_INVALID"
DECISION_PROPOSAL_INVALID = "PROPOSAL_INVALID"
DECISION_WEIGHT_MISMATCH = "WEIGHT_MISMATCH"
DECISION_TARGET_PROFILE_MISMATCH = "TARGET_PROFILE_MISMATCH"
DECISION_SAFETY_BLOCKED = "SAFETY_BLOCKED"
DECISION_ERROR = "ERROR"

PROPOSAL_DECISION_REQUIRED = "PROPOSE_FOR_MANUAL_REVIEW"
APPROVAL_TYPE = "shadow_promotion_apply_preflight"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"
DEFAULT_PRODUCTION_PROFILE_PATH = REPO_ROOT / "config" / "weights" / "weight_profile_current.yaml"

# Float tolerance is an invariant for serialization/rounding noise only; it must not
# mask a subjective weight policy difference or normalize weights silently.
WEIGHT_SUM_TOLERANCE = 0.000001
WEIGHT_MATCH_TOLERANCE = 0.000001


def default_promotion_proposal_json_path(data_root: Path, as_of: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "proposals"
        / f"shadow_promotion_proposal_{as_of.isoformat()}.json"
    )


def default_manual_approval_path(data_root: Path, as_of: date) -> Path:
    return data_root / "manual_approvals" / f"shadow_promotion_approval_{as_of.isoformat()}.json"


def default_current_shadow_weights_path(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "shadow" / "current_shadow_weights.json"


def default_preflight_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "promotion" / "preflight"


def default_preflight_json_path(data_root: Path, as_of: date) -> Path:
    return default_preflight_root(data_root) / (
        f"shadow_promotion_apply_preflight_{as_of.isoformat()}.json"
    )


def default_preflight_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_preflight_root(data_root)
        / "logs"
        / (f"shadow_promotion_apply_preflight_run_{as_of.isoformat()}.json")
    )


def build_shadow_promotion_apply_preflight_payload(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    proposal_file: Path | None = None,
    approval_file: Path | None = None,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    current_shadow_weights_path: Path | None = None,
    target_profile_name: str = DEFAULT_TARGET_PROFILE_NAME,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    proposal_path = proposal_file or default_promotion_proposal_json_path(data_root, as_of)
    approval_path = approval_file or default_manual_approval_path(data_root, as_of)
    shadow_path = current_shadow_weights_path or default_current_shadow_weights_path(data_root)
    output_json_path = output_json_path or default_preflight_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")

    proposal = _read_json_object(proposal_path)
    approval = _read_json_object(approval_path)
    production_profile = _read_structured_object(production_profile_path)
    current_shadow = _read_json_object(shadow_path)

    production_weights = _rounded_weights(_extract_weights(production_profile))
    shadow_weights = _rounded_weights(_extract_weights(current_shadow))
    proposed_weights = _rounded_weights(_extract_proposed_weights(proposal))

    input_artifacts = {
        "promotion_proposal": _artifact_record(proposal_path),
        "approval_artifact": _artifact_record(approval_path),
        "production_profile": _artifact_record(production_profile_path),
        "current_shadow_weights": _artifact_record(shadow_path),
    }
    target_validation = _validate_target_profile(
        approval=approval,
        production_profile=production_profile,
        production_profile_path=production_profile_path,
        target_profile_name=target_profile_name,
    )
    approval_validation = _validate_approval(
        approval=approval,
        proposal=proposal,
        proposal_path=proposal_path,
        approval_path=approval_path,
        proposal_sha256=_string_value(input_artifacts["promotion_proposal"].get("sha256")),
        as_of=as_of,
        target_validation=target_validation,
    )
    proposal_validation = _validate_proposal(proposal=proposal, as_of=as_of)
    weight_validation = _validate_weights(
        production_weights=production_weights,
        shadow_weights=shadow_weights,
        proposed_weights=proposed_weights,
    )
    safety_checklist = _build_safety_checklist(proposal)
    diff_preview = _build_diff_preview(
        production_profile_path=production_profile_path,
        production_profile_sha256=_string_value(
            input_artifacts["production_profile"].get("sha256")
        ),
        production_weights=production_weights,
        proposed_weights=proposed_weights,
    )
    rollback_plan = _build_rollback_plan(
        data_root=data_root,
        as_of=as_of,
        production_profile_path=production_profile_path,
        production_profile_sha256=_string_value(
            input_artifacts["production_profile"].get("sha256")
        ),
    )
    warnings = _warning_reasons(approval=approval, proposal=proposal)
    preflight_decision = _preflight_decision(
        input_artifacts=input_artifacts,
        approval_validation=approval_validation,
        proposal_validation=proposal_validation,
        weight_validation=weight_validation,
        target_validation=target_validation,
        safety_checklist=safety_checklist,
        warnings=warnings,
    )

    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "promotion_executed": False,
        "apply_executed": False,
        "preflight_only": True,
        "safe_for_production": False,
        "preflight_decision": preflight_decision,
        "preflight_reason": _preflight_reason(
            decision=preflight_decision,
            approval_validation=approval_validation,
            proposal_validation=proposal_validation,
            weight_validation=weight_validation,
            target_validation=target_validation,
            safety_checklist=safety_checklist,
            warnings=warnings,
        ),
        "input_artifacts": input_artifacts,
        "approval_validation": approval_validation,
        "proposal_validation": proposal_validation,
        "weight_validation": weight_validation,
        "target_profile_validation": target_validation,
        "diff_preview": diff_preview,
        "rollback_plan": rollback_plan,
        "safety_checklist": safety_checklist,
        "warnings": warnings,
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "run_log_json": str(default_preflight_run_log_json_path(data_root, as_of)),
            "run_log_markdown": str(
                default_preflight_run_log_json_path(data_root, as_of).with_suffix(".md")
            ),
        },
        "pipeline_contract": {
            "reads_existing_artifacts_only": True,
            "runs_shadow_iteration_pipeline": False,
            "runs_comparison_pipeline": False,
            "runs_multi_day_review_pipeline": False,
            "runs_promotion_proposal_pipeline": False,
            "runs_promotion_apply": False,
            "runs_scoring_pipeline": False,
            "runs_broker_runner": False,
            "runs_paper_runner": False,
            "runs_replay_runner": False,
            "writes_production_profile": False,
            "writes_production_weights": False,
            "writes_approved_profile": False,
            "promotes_shadow_to_production": False,
            "changes_daily_dashboard_main_conclusion": False,
            "triggers_trade": False,
            "production_effect": PRODUCTION_EFFECT_NONE,
            "manual_review_only": True,
            "preflight_only": True,
        },
        "audit": {
            "created_by": "scripts/run_shadow_promotion_apply_preflight.py",
            "created_at": generated.isoformat(),
            "safe_for_scheduler": True,
            "safe_for_production": False,
            "no_files_modified_except_preflight_artifacts": True,
            "production_profile_sha256_before": input_artifacts["production_profile"].get(
                "sha256",
                "",
            ),
        },
    }
    _assert_safety_invariants(payload)
    return payload


def write_shadow_promotion_apply_preflight_report(
    *,
    as_of: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    proposal_file: Path | None = None,
    approval_file: Path | None = None,
    production_profile_path: Path = DEFAULT_PRODUCTION_PROFILE_PATH,
    current_shadow_weights_path: Path | None = None,
    target_profile_name: str = DEFAULT_TARGET_PROFILE_NAME,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    payload = build_shadow_promotion_apply_preflight_payload(
        as_of=as_of,
        data_root=data_root,
        proposal_file=proposal_file,
        approval_file=approval_file,
        production_profile_path=production_profile_path,
        current_shadow_weights_path=current_shadow_weights_path,
        target_profile_name=target_profile_name,
        output_json_path=output_json_path,
        output_md_path=output_md_path,
        generated_at=generated,
    )
    outputs = _mapping(payload.get("outputs"))
    json_path = Path(str(outputs["json"]))
    md_path = Path(str(outputs["markdown"]))
    _write_json(json_path, payload)
    _write_text(md_path, render_shadow_promotion_apply_preflight_report(payload))

    run_log = _run_log_payload(payload=payload, generated_at=generated)
    run_log_json_path = Path(str(outputs["run_log_json"]))
    run_log_md_path = Path(str(outputs["run_log_markdown"]))
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_shadow_promotion_apply_preflight_run_log(run_log))
    return payload


def render_shadow_promotion_apply_preflight_report(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    approval_validation = _mapping(payload.get("approval_validation"))
    proposal_validation = _mapping(payload.get("proposal_validation"))
    weight_validation = _mapping(payload.get("weight_validation"))
    diff_preview = _mapping(payload.get("diff_preview"))
    rollback = _mapping(payload.get("rollback_plan"))
    before = _mapping(diff_preview.get("production_weights_before"))
    after = _mapping(diff_preview.get("production_weights_after_preview"))
    delta = _mapping(diff_preview.get("delta"))

    lines = [
        f"# Shadow Promotion Apply Preflight - {payload.get('date')}",
        "",
        "## 1. Run Summary",
        "",
        f"- Preflight Decision: `{payload.get('preflight_decision')}`",
        "- Production Effect: `none`",
        "- Manual Review Only: `true`",
        "- Promotion Executed: `false`",
        "- Apply Executed: `false`",
        "- Preflight Only: `true`",
        "",
        "## 2. Input Artifacts",
        "",
        "| Artifact | Status | Path |",
        "|---|---:|---|",
    ]
    for key in (
        "promotion_proposal",
        "approval_artifact",
        "production_profile",
        "current_shadow_weights",
    ):
        artifact = _mapping(artifacts.get(key))
        lines.append(
            f"| {key} | `{artifact.get('status', 'MISSING')}` | " f"`{artifact.get('path', '')}` |"
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
            "## 4. Proposal Validation",
            "",
            "| Check | Status | Reason |",
            "|---|---:|---|",
        ],
    )
    for check in _records(proposal_validation.get("checks")):
        lines.append(_validation_row(check))

    lines.extend(
        [
            "",
            "## 5. Weight Validation",
            "",
            "| Check | Status | Reason |",
            "|---|---:|---|",
        ],
    )
    for check in _records(weight_validation.get("checks")):
        lines.append(_validation_row(check))

    lines.extend(
        [
            "",
            "## 6. Production Diff Preview",
            "",
            "| Weight Key | Current Production | Proposed Production | Delta |",
            "|---|---:|---:|---:|",
        ],
    )
    for key in sorted(set(before) | set(after)):
        lines.append(
            "| "
            f"{key} | {_format_float(before.get(key))} | {_format_float(after.get(key))} | "
            f"{_format_signed_float(delta.get(key))} |"
        )

    instructions = _strings(rollback.get("rollback_instructions"))
    lines.extend(
        [
            "",
            "## 7. Rollback Plan Preview",
            "",
            (
                "- Rollback snapshot would be created at: "
                f"`{rollback.get('rollback_snapshot_path_preview', '')}`"
            ),
            f"- Restore instruction: {instructions[0] if instructions else 'missing'}",
            (
                "- Validation instruction: "
                f"{instructions[1] if len(instructions) > 1 else 'missing'}"
            ),
            "",
            "## 8. Safety Statement",
            "",
            "This task does not modify production.",
            "",
            "- production_effect = none",
            "- manual_review_only = true",
            "- promotion_executed = false",
            "- apply_executed = false",
            "- preflight_only = true",
            "- safe_for_production = false",
            "",
            "## 9. Next Step",
            "",
            (
                "A future TRADING-018E2 explicit apply command may use this preflight report, "
                "but only with a separate apply approval artifact and an explicit manual command."
            ),
            "",
        ],
    )
    return "\n".join(lines)


def render_shadow_promotion_apply_preflight_run_log(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Shadow Promotion Apply Preflight Run - {payload.get('date')}",
            "",
            f"- run_status: `{payload.get('run_status')}`",
            f"- preflight_decision: `{payload.get('preflight_decision')}`",
            "- production_effect: `none`",
            "- manual_review_only: `true`",
            "- promotion_executed: `false`",
            "- apply_executed: `false`",
            "- preflight_only: `true`",
            "- safe_for_production: `false`",
            f"- preflight_json: `{payload.get('preflight_json')}`",
            f"- preflight_markdown: `{payload.get('preflight_markdown')}`",
            "",
        ],
    )


def _validate_approval(
    *,
    approval: dict[str, Any],
    proposal: dict[str, Any],
    proposal_path: Path,
    approval_path: Path,
    proposal_sha256: str,
    as_of: date,
    target_validation: dict[str, Any],
) -> dict[str, Any]:
    proposal_section = _mapping(approval.get("proposal"))
    safety = _mapping(approval.get("safety_acknowledgement"))
    checks = [
        _check("approval_file_exists", approval_path.exists(), str(approval_path)),
        _check("approval_type", approval.get("approval_type") == APPROVAL_TYPE, APPROVAL_TYPE),
        _check("approved", approval.get("approved") is True, str(approval.get("approved"))),
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
            "proposal_date_match",
            _string_value(proposal_section.get("proposal_date")) == as_of.isoformat()
            and _string_value(proposal.get("date")) == as_of.isoformat(),
            (
                f"approval={proposal_section.get('proposal_date')}, "
                f"proposal={proposal.get('date')}, expected={as_of.isoformat()}"
            ),
        ),
        _check(
            "proposal_decision",
            _string_value(proposal_section.get("proposal_decision")) == PROPOSAL_DECISION_REQUIRED
            and _string_value(proposal.get("proposal_decision")) == PROPOSAL_DECISION_REQUIRED,
            _string_value(proposal_section.get("proposal_decision")),
        ),
        _check(
            "promotion_proposed",
            proposal_section.get("promotion_proposed") is True
            and proposal.get("promotion_proposed") is True,
            str(proposal_section.get("promotion_proposed")),
        ),
        _check(
            "target_profile_match",
            target_validation.get("status") == "PASS",
            "; ".join(_strings(target_validation.get("blocking_reasons"))) or "matched",
        ),
        _check(
            "preflight_only_acknowledged",
            safety.get("preflight_only") is True,
            str(safety.get("preflight_only")),
        ),
        _check(
            "apply_not_authorized",
            safety.get("apply_not_authorized") is True,
            str(safety.get("apply_not_authorized")),
        ),
        _check(
            "production_modification_not_authorized",
            safety.get("production_modification_not_authorized") is True,
            str(safety.get("production_modification_not_authorized")),
        ),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "approved": approval.get("approved") is True,
        "proposal_hash_match": _check_status(checks, "proposal_hash_match"),
        "proposal_date_match": _check_status(checks, "proposal_date_match"),
        "target_profile_match": _check_status(checks, "target_profile_match"),
        "preflight_only_acknowledged": _check_status(checks, "preflight_only_acknowledged"),
        "apply_not_authorized": _check_status(checks, "apply_not_authorized"),
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _validate_proposal(*, proposal: dict[str, Any], as_of: date) -> dict[str, Any]:
    proposed = _mapping(proposal.get("proposed_production_weights"))
    checks = [
        _check("proposal_exists", bool(proposal), "proposal JSON loaded"),
        _check(
            "task_id", proposal.get("task_id") == SOURCE_PROPOSAL_TASK_ID, SOURCE_PROPOSAL_TASK_ID
        ),
        _check("date", _string_value(proposal.get("date")) == as_of.isoformat(), as_of.isoformat()),
        _check(
            "proposal_decision",
            proposal.get("proposal_decision") == PROPOSAL_DECISION_REQUIRED,
            _string_value(proposal.get("proposal_decision")),
        ),
        _check(
            "promotion_proposed",
            proposal.get("promotion_proposed") is True,
            str(proposal.get("promotion_proposed")),
        ),
        _check(
            "promotion_executed",
            proposal.get("promotion_executed") is False,
            str(proposal.get("promotion_executed")),
        ),
        _check(
            "production_effect",
            proposal.get("production_effect") == PRODUCTION_EFFECT_NONE,
            _string_value(proposal.get("production_effect")),
        ),
        _check(
            "manual_review_only",
            proposal.get("manual_review_only") is True,
            str(proposal.get("manual_review_only")),
        ),
        _check(
            "safe_for_production",
            proposal.get("safe_for_production") is False,
            str(proposal.get("safe_for_production")),
        ),
        _check("proposed_production_weights", bool(proposed), f"key_count={len(proposed)}"),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "proposal_decision": _string_value(proposal.get("proposal_decision")),
        "promotion_proposed": proposal.get("promotion_proposed") is True,
        "promotion_executed": proposal.get("promotion_executed") is True,
        "production_effect": _string_value(proposal.get("production_effect")),
        "manual_review_only": proposal.get("manual_review_only") is True,
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _validate_weights(
    *,
    production_weights: dict[str, float],
    shadow_weights: dict[str, float],
    proposed_weights: dict[str, float],
) -> dict[str, Any]:
    production_keys = set(production_weights)
    shadow_keys = set(shadow_weights)
    proposed_keys = set(proposed_weights)
    all_keys_match = bool(production_keys) and production_keys == shadow_keys == proposed_keys
    weights_in_range = all(0.0 <= value <= 1.0 for value in proposed_weights.values())
    proposed_sum = sum(proposed_weights.values())
    weights_sum_valid = bool(proposed_weights) and abs(proposed_sum - 1.0) <= WEIGHT_SUM_TOLERANCE
    shadow_matches = all_keys_match and all(
        abs(shadow_weights[key] - proposed_weights[key]) <= WEIGHT_MATCH_TOLERANCE
        for key in proposed_keys
    )
    checks = [
        _check(
            "production_weight_keys_match",
            production_keys == proposed_keys and bool(production_keys),
            _key_reason(production_keys, proposed_keys),
        ),
        _check(
            "shadow_weight_keys_match",
            shadow_keys == proposed_keys and bool(shadow_keys),
            _key_reason(shadow_keys, proposed_keys),
        ),
        _check(
            "proposal_weight_keys_match",
            production_keys == shadow_keys == proposed_keys and bool(proposed_keys),
            _key_reason(production_keys | shadow_keys, proposed_keys),
        ),
        _check("weights_in_range", weights_in_range and bool(proposed_weights), "[0, 1]"),
        _check(
            "weights_sum_valid",
            weights_sum_valid,
            f"sum={proposed_sum:.10f}, tolerance={WEIGHT_SUM_TOLERANCE:.10f}",
        ),
        _check(
            "shadow_matches_proposal",
            shadow_matches,
            f"tolerance={WEIGHT_MATCH_TOLERANCE:.10f}",
        ),
    ]
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "production_weight_keys_match": production_keys == proposed_keys and bool(production_keys),
        "shadow_weight_keys_match": shadow_keys == proposed_keys and bool(shadow_keys),
        "proposal_weight_keys_match": all_keys_match,
        "shadow_matches_proposal": shadow_matches,
        "weights_sum_valid": weights_sum_valid,
        "blocking_reasons": blocking,
        "missing_in_shadow": sorted(production_keys - shadow_keys),
        "missing_in_production": sorted(shadow_keys - production_keys),
        "missing_in_proposal": sorted((production_keys | shadow_keys) - proposed_keys),
        "extra_in_proposal": sorted(proposed_keys - (production_keys | shadow_keys)),
        "proposed_weight_sum": round(proposed_sum, 10),
        "checks": checks,
    }


def _validate_target_profile(
    *,
    approval: dict[str, Any],
    production_profile: dict[str, Any],
    production_profile_path: Path,
    target_profile_name: str,
) -> dict[str, Any]:
    target = _mapping(approval.get("target"))
    approval_target_name = _string_value(target.get("target_profile_name"))
    approval_target_path = _string_value(target.get("target_profile_path"))
    checks = [
        _check(
            "target_profile_name",
            approval_target_name == target_profile_name,
            f"approval={approval_target_name}, expected={target_profile_name}",
        ),
        _check(
            "target_profile_path",
            _path_matches(approval_target_path, production_profile_path),
            f"approval={approval_target_path}, expected={production_profile_path}",
        ),
    ]
    metadata_checks = _production_profile_metadata_checks(
        production_profile=production_profile,
        target_profile_name=target_profile_name,
    )
    checks.extend(metadata_checks)
    blocking = [str(check["check"]) for check in checks if check["status"] != "PASS"]
    return {
        "status": "PASS" if not blocking else "FAIL",
        "target_profile_name": target_profile_name,
        "target_profile_path": str(production_profile_path),
        "blocking_reasons": blocking,
        "checks": checks,
    }


def _production_profile_metadata_checks(
    *,
    production_profile: dict[str, Any],
    target_profile_name: str,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for key in ("profile_name", "target_profile_name", "name"):
        value = _string_value(production_profile.get(key))
        if value:
            checks.append(
                _check(
                    key,
                    value == target_profile_name,
                    f"value={value}, target={target_profile_name}",
                )
            )
    status = _string_value(production_profile.get("status"))
    if status and target_profile_name == "production":
        checks.append(_check("status", status == "production", f"value={status}"))
    environment = _string_value(production_profile.get("environment"))
    if environment:
        checks.append(
            _check(
                "environment",
                environment in {target_profile_name, "production"},
                f"value={environment}, target={target_profile_name}",
            )
        )
    schema_version = _string_value(production_profile.get("schema_version"))
    if schema_version:
        checks.append(_check("schema_version_present", True, f"value={schema_version}"))
    return checks


def _build_safety_checklist(proposal: dict[str, Any]) -> dict[str, Any]:
    contract = _mapping(proposal.get("pipeline_contract"))
    unsafe_contract_fields = []
    for field in (
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
    ):
        if field in contract and contract.get(field) is not False:
            unsafe_contract_fields.append(field)
    checklist = {
        "production_effect_none": True,
        "manual_review_only": True,
        "promotion_executed_false": True,
        "apply_executed_false": True,
        "preflight_only": True,
        "safe_for_production_false": True,
        "writes_production_profile": False,
        "writes_production_weights": False,
        "runs_broker_runner": False,
        "runs_replay_runner": False,
        "runs_scoring_pipeline": False,
        "unsafe_proposal_contract_fields": unsafe_contract_fields,
        "blocking_reasons": [
            f"unsafe_proposal_contract:{field}" for field in unsafe_contract_fields
        ],
    }
    checklist["status"] = "PASS" if not checklist["blocking_reasons"] else "FAIL"
    return checklist


def _build_diff_preview(
    *,
    production_profile_path: Path,
    production_profile_sha256: str,
    production_weights: dict[str, float],
    proposed_weights: dict[str, float],
) -> dict[str, Any]:
    delta = {}
    for key in sorted(set(production_weights) | set(proposed_weights)):
        before = production_weights.get(key)
        after = proposed_weights.get(key)
        if before is None or after is None:
            continue
        delta[key] = round(after - before, 10)
    changed = [key for key, value in delta.items() if abs(value) > WEIGHT_MATCH_TOLERANCE]
    return {
        "target_profile_path": str(production_profile_path),
        "target_profile_sha256_before": production_profile_sha256,
        "changed_weight_keys": changed,
        "production_weights_before": production_weights,
        "production_weights_after_preview": proposed_weights,
        "delta": delta,
    }


def _build_rollback_plan(
    *,
    data_root: Path,
    as_of: date,
    production_profile_path: Path,
    production_profile_sha256: str,
) -> dict[str, Any]:
    snapshot_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback"
        / f"production_profile_before_shadow_promotion_{as_of.isoformat()}.json"
    )
    return {
        "required": True,
        "rollback_snapshot_would_be_created": True,
        "rollback_snapshot_path_preview": str(snapshot_path),
        "target_profile_path": str(production_profile_path),
        "target_profile_sha256_before": production_profile_sha256,
        "rollback_instructions": [
            "Restore the rollback snapshot to the target production profile path.",
            "Run validation tests after rollback.",
        ],
    }


def _preflight_decision(
    *,
    input_artifacts: dict[str, Any],
    approval_validation: dict[str, Any],
    proposal_validation: dict[str, Any],
    weight_validation: dict[str, Any],
    target_validation: dict[str, Any],
    safety_checklist: dict[str, Any],
    warnings: list[str],
) -> str:
    missing = [
        name
        for name, artifact in input_artifacts.items()
        if _mapping(artifact).get("status") != "FOUND"
    ]
    if missing:
        return DECISION_INSUFFICIENT_DATA
    if target_validation.get("status") != "PASS":
        return DECISION_TARGET_PROFILE_MISMATCH
    if proposal_validation.get("status") != "PASS":
        return DECISION_PROPOSAL_INVALID
    if safety_checklist.get("status") != "PASS":
        return DECISION_SAFETY_BLOCKED
    if approval_validation.get("status") != "PASS":
        return DECISION_APPROVAL_INVALID
    if weight_validation.get("status") != "PASS":
        return DECISION_WEIGHT_MISMATCH
    if warnings:
        return DECISION_WARNING
    return DECISION_PASS


def _preflight_reason(
    *,
    decision: str,
    approval_validation: dict[str, Any],
    proposal_validation: dict[str, Any],
    weight_validation: dict[str, Any],
    target_validation: dict[str, Any],
    safety_checklist: dict[str, Any],
    warnings: list[str],
) -> str:
    if decision == DECISION_PASS:
        return (
            "Proposal and approval artifact matched. Diff preview and rollback plan generated. "
            "No production files were modified."
        )
    if decision == DECISION_WARNING:
        return "Preflight passed with non-blocking warnings: " + ", ".join(warnings)
    if decision == DECISION_APPROVAL_INVALID:
        return "Approval validation failed: " + ", ".join(
            _strings(approval_validation.get("blocking_reasons")),
        )
    if decision == DECISION_PROPOSAL_INVALID:
        return "Proposal validation failed: " + ", ".join(
            _strings(proposal_validation.get("blocking_reasons")),
        )
    if decision == DECISION_WEIGHT_MISMATCH:
        return "Weight validation failed: " + ", ".join(
            _strings(weight_validation.get("blocking_reasons")),
        )
    if decision == DECISION_TARGET_PROFILE_MISMATCH:
        return "Target profile validation failed: " + ", ".join(
            _strings(target_validation.get("blocking_reasons")),
        )
    if decision == DECISION_SAFETY_BLOCKED:
        return "Safety checklist failed: " + ", ".join(
            _strings(safety_checklist.get("blocking_reasons")),
        )
    if decision == DECISION_INSUFFICIENT_DATA:
        return "One or more required input artifacts are missing or unreadable."
    return "Unexpected preflight error."


def _warning_reasons(*, approval: dict[str, Any], proposal: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    if approval and not _string_value(approval.get("approval_statement")):
        warnings.append("approval_statement_missing")
    if approval and not _string_value(approval.get("approved_by")):
        warnings.append("approved_by_missing")
    if approval and not _string_value(approval.get("approved_at")):
        warnings.append("approved_at_missing")
    if proposal and "outputs" not in proposal:
        warnings.append("proposal_outputs_missing")
    return warnings


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("outputs"))
    decision = _string_value(payload.get("preflight_decision"))
    run_log = {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "generated_at": generated_at.isoformat(),
        "run_status": "PASS" if decision in {DECISION_PASS, DECISION_WARNING} else "BLOCKED",
        "preflight_decision": decision,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "promotion_executed": False,
        "apply_executed": False,
        "preflight_only": True,
        "safe_for_production": False,
        "preflight_json": outputs.get("json", ""),
        "preflight_markdown": outputs.get("markdown", ""),
        "run_log_json": outputs.get("run_log_json", ""),
        "run_log_markdown": outputs.get("run_log_markdown", ""),
        "pipeline_contract": payload.get("pipeline_contract", {}),
        "audit": payload.get("audit", {}),
    }
    _assert_safety_invariants(run_log)
    return run_log


def _artifact_record(path: Path) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "status": "FOUND" if exists else "MISSING",
        "path": str(path),
        "exists": exists,
        "sha256": _sha256(path) if exists else "",
        "size_bytes": path.stat().st_size if exists else 0,
    }


def _extract_weights(payload: dict[str, Any]) -> dict[str, float]:
    for key in ("base_weights", "weights", "production_weights", "target_weights"):
        weights = _mapping(payload.get(key))
        parsed = _weights_from_mapping(weights)
        if parsed:
            return parsed
    return {}


def _extract_proposed_weights(payload: dict[str, Any]) -> dict[str, float]:
    return _weights_from_mapping(_mapping(payload.get("proposed_production_weights")))


def _weights_from_mapping(payload: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in payload.items():
        parsed = _optional_float(value)
        if parsed is not None:
            weights[str(key)] = parsed
    return weights


def _rounded_weights(weights: dict[str, float]) -> dict[str, float]:
    return {key: round(value, 10) for key, value in sorted(weights.items())}


def _check(name: str, passed: bool, reason: str) -> dict[str, Any]:
    return {"check": name, "status": "PASS" if passed else "FAIL", "reason": reason}


def _check_status(checks: list[dict[str, Any]], name: str) -> bool:
    return any(check.get("check") == name and check.get("status") == "PASS" for check in checks)


def _validation_row(check: dict[str, Any]) -> str:
    return (
        f"| {check.get('check', '')} | `{check.get('status', 'FAIL')}` | "
        f"{check.get('reason', '')} |"
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


def _normalized_path_text(path: Path) -> str:
    return str(path).replace("\\", "/").rstrip("/")


def _assert_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("apply preflight production_effect must be none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("apply preflight manual_review_only must be true")
    if payload.get("promotion_executed") is not False:
        raise ValueError("TRADING-018E1 cannot execute promotion")
    if payload.get("apply_executed") is not False:
        raise ValueError("TRADING-018E1 cannot execute apply")
    if payload.get("preflight_only") is not True:
        raise ValueError("TRADING-018E1 must remain preflight_only")
    if payload.get("safe_for_production") is not False:
        raise ValueError("TRADING-018E1 must not mark output safe_for_production")
    contract = _mapping(payload.get("pipeline_contract"))
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
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if field in contract and contract.get(field) is not False:
            raise ValueError(f"unsafe apply preflight contract field: {field}")


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
