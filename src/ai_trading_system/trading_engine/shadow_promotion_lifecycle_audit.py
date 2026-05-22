from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.shadow_promotion_apply import (
    DEFAULT_DATA_ROOT,
    _artifact_record,
    _format_float,
    _format_signed_float,
    _mapping,
    _path_matches,
    _read_json_object,
    _read_structured_object,
    _string_value,
    _strings,
    _write_json,
    _write_text,
    get_profile_weights,
)

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "shadow_promotion_lifecycle_audit"
RUN_REPORT_TYPE = "shadow_promotion_lifecycle_audit_run"
TASK_ID = "TRADING-018F"
MODE = "promotion_lifecycle_audit_only"
PRODUCTION_EFFECT_NONE = "none"

DECISION_COMPLETE_WITH_ROLLBACK = "COMPLETE_WITH_ROLLBACK"
DECISION_COMPLETE_APPLIED_NO_ROLLBACK = "COMPLETE_APPLIED_NO_ROLLBACK"
DECISION_PROPOSAL_ONLY = "PROPOSAL_ONLY"
DECISION_PREFLIGHT_ONLY = "PREFLIGHT_ONLY"
DECISION_APPLY_FAILED_OR_BLOCKED = "APPLY_FAILED_OR_BLOCKED"
DECISION_ROLLBACK_FAILED_OR_BLOCKED = "ROLLBACK_FAILED_OR_BLOCKED"
DECISION_INCOMPLETE_ARTIFACTS = "INCOMPLETE_ARTIFACTS"
DECISION_SAFETY_ANOMALY = "SAFETY_ANOMALY"
DECISION_ERROR = "ERROR"

LIFECYCLE_DECISIONS = {
    DECISION_COMPLETE_WITH_ROLLBACK,
    DECISION_COMPLETE_APPLIED_NO_ROLLBACK,
    DECISION_PROPOSAL_ONLY,
    DECISION_PREFLIGHT_ONLY,
    DECISION_APPLY_FAILED_OR_BLOCKED,
    DECISION_ROLLBACK_FAILED_OR_BLOCKED,
    DECISION_INCOMPLETE_ARTIFACTS,
    DECISION_SAFETY_ANOMALY,
    DECISION_ERROR,
}

STAGE_PROPOSAL = "proposal"
STAGE_PREFLIGHT = "preflight"
STAGE_APPLY = "apply_result"
STAGE_ROLLBACK = "rollback_result"

SOURCE_PROPOSAL_TASK_ID = "TRADING-018D"
SOURCE_PREFLIGHT_TASK_ID = "TRADING-018E1"
SOURCE_APPLY_TASK_ID = "TRADING-018E2"
SOURCE_ROLLBACK_TASK_ID = "TRADING-018E3"

PROPOSAL_REPORT_TYPE = "shadow_promotion_proposal"
PREFLIGHT_REPORT_TYPE = "shadow_promotion_apply_preflight"
APPLY_REPORT_TYPE = "shadow_promotion_apply_result"
ROLLBACK_REPORT_TYPE = "shadow_promotion_rollback_result"

PROPOSAL_DECISION_REQUIRED = "PROPOSE_FOR_MANUAL_REVIEW"
PREFLIGHT_DECISION_PASS = "PASS"
APPLY_DECISION_APPLIED = "APPLIED"
ROLLBACK_DECISION_ROLLED_BACK = "ROLLED_BACK"


def default_proposal_json_path(data_root: Path, promotion_date: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "proposals"
        / f"shadow_promotion_proposal_{promotion_date.isoformat()}.json"
    )


def default_preflight_json_path(data_root: Path, promotion_date: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "preflight"
        / f"shadow_promotion_apply_preflight_{promotion_date.isoformat()}.json"
    )


def default_apply_result_json_path(data_root: Path, promotion_date: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "apply"
        / f"shadow_promotion_apply_result_{promotion_date.isoformat()}.json"
    )


def default_rollback_result_json_path(data_root: Path, promotion_date: date) -> Path:
    return (
        data_root
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback_results"
        / f"shadow_promotion_rollback_result_{promotion_date.isoformat()}.json"
    )


def default_preflight_approval_path(data_root: Path, promotion_date: date) -> Path:
    return (
        data_root
        / "manual_approvals"
        / f"shadow_promotion_approval_{promotion_date.isoformat()}.json"
    )


def default_apply_approval_path(data_root: Path, promotion_date: date) -> Path:
    return (
        data_root
        / "manual_approvals"
        / f"shadow_promotion_apply_approval_{promotion_date.isoformat()}.json"
    )


def default_rollback_approval_path(data_root: Path, promotion_date: date) -> Path:
    return (
        data_root
        / "manual_approvals"
        / f"shadow_promotion_rollback_approval_{promotion_date.isoformat()}.json"
    )


def default_audit_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "promotion" / "audit"


def default_audit_json_path(data_root: Path, as_of: date) -> Path:
    return default_audit_root(data_root) / (
        f"shadow_promotion_lifecycle_audit_{as_of.isoformat()}.json"
    )


def default_audit_run_log_json_path(data_root: Path, as_of: date) -> Path:
    return (
        default_audit_root(data_root)
        / "logs"
        / (f"shadow_promotion_lifecycle_audit_run_{as_of.isoformat()}.json")
    )


def write_shadow_promotion_lifecycle_audit_report(
    *,
    as_of: date,
    promotion_date: date | None = None,
    data_root: Path = DEFAULT_DATA_ROOT,
    proposal_file: Path | None = None,
    preflight_file: Path | None = None,
    apply_result_file: Path | None = None,
    rollback_result_file: Path | None = None,
    include_approval_artifacts: bool = False,
    fail_on_safety_anomaly: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    promotion_as_of = promotion_date or as_of
    output_json_path = output_json_path or default_audit_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = default_audit_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_json_path.with_suffix(".md")

    try:
        payload = build_shadow_promotion_lifecycle_audit_payload(
            as_of=as_of,
            promotion_date=promotion_as_of,
            data_root=data_root,
            proposal_file=proposal_file,
            preflight_file=preflight_file,
            apply_result_file=apply_result_file,
            rollback_result_file=rollback_result_file,
            include_approval_artifacts=include_approval_artifacts,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
        )
    except Exception as exc:  # pragma: no cover - defensive report path
        payload = _error_payload(
            as_of=as_of,
            promotion_date=promotion_as_of,
            data_root=data_root,
            output_json_path=output_json_path,
            output_md_path=output_md_path,
            run_log_json_path=run_log_json_path,
            run_log_md_path=run_log_md_path,
            generated_at=generated,
            error=str(exc),
        )

    _write_json(output_json_path, payload)
    _write_text(output_md_path, render_shadow_promotion_lifecycle_audit_report(payload))
    run_log = _run_log_payload(payload=payload, generated_at=generated)
    _write_json(run_log_json_path, run_log)
    _write_text(run_log_md_path, render_shadow_promotion_lifecycle_audit_run_log(run_log))

    if fail_on_safety_anomaly and payload.get("lifecycle_decision") == DECISION_SAFETY_ANOMALY:
        raise SystemExit(2)
    return payload


def build_shadow_promotion_lifecycle_audit_payload(
    *,
    as_of: date,
    promotion_date: date,
    data_root: Path = DEFAULT_DATA_ROOT,
    proposal_file: Path | None = None,
    preflight_file: Path | None = None,
    apply_result_file: Path | None = None,
    rollback_result_file: Path | None = None,
    include_approval_artifacts: bool = False,
    output_json_path: Path | None = None,
    output_md_path: Path | None = None,
    run_log_json_path: Path | None = None,
    run_log_md_path: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    output_json_path = output_json_path or default_audit_json_path(data_root, as_of)
    output_md_path = output_md_path or output_json_path.with_suffix(".md")
    run_log_json_path = run_log_json_path or default_audit_run_log_json_path(data_root, as_of)
    run_log_md_path = run_log_md_path or run_log_json_path.with_suffix(".md")

    paths = {
        STAGE_PROPOSAL: proposal_file or default_proposal_json_path(data_root, promotion_date),
        STAGE_PREFLIGHT: preflight_file or default_preflight_json_path(data_root, promotion_date),
        STAGE_APPLY: apply_result_file or default_apply_result_json_path(data_root, promotion_date),
        STAGE_ROLLBACK: (
            rollback_result_file or default_rollback_result_json_path(data_root, promotion_date)
        ),
    }
    artifacts = {
        stage: _input_artifact_record(path, optional=stage == STAGE_ROLLBACK)
        for stage, path in paths.items()
    }
    if include_approval_artifacts:
        artifacts["approval_artifacts"] = {
            "preflight_approval": _input_artifact_record(
                default_preflight_approval_path(data_root, promotion_date),
                optional=True,
            ),
            "apply_approval": _input_artifact_record(
                default_apply_approval_path(data_root, promotion_date),
                optional=True,
            ),
            "rollback_approval": _input_artifact_record(
                default_rollback_approval_path(data_root, promotion_date),
                optional=True,
            ),
        }

    proposal = _read_json_object(paths[STAGE_PROPOSAL])
    preflight = _read_json_object(paths[STAGE_PREFLIGHT])
    apply_result = _read_json_object(paths[STAGE_APPLY])
    rollback_result = _read_json_object(paths[STAGE_ROLLBACK])

    stage_payloads = {
        STAGE_PROPOSAL: proposal,
        STAGE_PREFLIGHT: preflight,
        STAGE_APPLY: apply_result,
        STAGE_ROLLBACK: rollback_result,
    }
    stage_paths = paths
    stage_artifacts = {
        STAGE_PROPOSAL: _mapping(artifacts[STAGE_PROPOSAL]),
        STAGE_PREFLIGHT: _mapping(artifacts[STAGE_PREFLIGHT]),
        STAGE_APPLY: _mapping(artifacts[STAGE_APPLY]),
        STAGE_ROLLBACK: _mapping(artifacts[STAGE_ROLLBACK]),
    }

    audit_findings = {"critical_findings": [], "warnings": [], "notes": []}
    artifact_chain = _build_artifact_chain(
        promotion_date=promotion_date,
        stage_payloads=stage_payloads,
        stage_paths=stage_paths,
        stage_artifacts=stage_artifacts,
        audit_findings=audit_findings,
    )
    safety_boundary = _build_safety_boundary_audit(
        stage_payloads=stage_payloads,
        stage_artifacts=stage_artifacts,
        audit_findings=audit_findings,
    )
    weight_lifecycle = _build_weight_lifecycle(
        proposal=proposal,
        preflight=preflight,
        apply_result=apply_result,
        rollback_result=rollback_result,
        audit_findings=audit_findings,
    )
    proposal_summary = _proposal_summary(proposal)
    preflight_summary = _preflight_summary(preflight)
    apply_summary = _apply_summary(apply_result)
    rollback_summary = _rollback_summary(rollback_result)

    lifecycle_decision = _lifecycle_decision(
        artifact_chain=artifact_chain,
        audit_findings=audit_findings,
        stage_payloads=stage_payloads,
        stage_artifacts=stage_artifacts,
    )
    lifecycle_reason = _lifecycle_reason(
        lifecycle_decision=lifecycle_decision,
        artifact_chain=artifact_chain,
        audit_findings=audit_findings,
        rollback_summary=rollback_summary,
    )
    _add_lifecycle_notes(
        lifecycle_decision=lifecycle_decision,
        rollback_present=_artifact_found(stage_artifacts[STAGE_ROLLBACK]),
        audit_findings=audit_findings,
    )

    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "promotion_date": promotion_date.isoformat(),
        "generated_at": generated.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "audit_only": True,
        "apply_executed_by_audit": False,
        "rollback_executed_by_audit": False,
        "safe_for_scheduler": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "lifecycle_decision": lifecycle_decision,
        "lifecycle_reason": lifecycle_reason,
        "input_artifacts": artifacts,
        "artifact_chain": artifact_chain,
        "proposal_summary": proposal_summary,
        "preflight_summary": preflight_summary,
        "apply_summary": apply_summary,
        "rollback_summary": rollback_summary,
        "weight_lifecycle": weight_lifecycle,
        "safety_boundary_audit": safety_boundary,
        "audit_findings": audit_findings,
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "run_log_json": str(run_log_json_path),
            "run_log_markdown": str(run_log_md_path),
        },
        "pipeline_contract": {
            "reads_existing_artifacts_only": True,
            "runs_shadow_iteration_pipeline": False,
            "runs_comparison_pipeline": False,
            "runs_multi_day_review_pipeline": False,
            "runs_promotion_proposal_pipeline": False,
            "runs_apply_preflight_pipeline": False,
            "runs_promotion_apply": False,
            "runs_promotion_rollback": False,
            "runs_lifecycle_audit_pipeline": False,
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
            "audit_only": True,
        },
        "audit": {
            "created_by": "scripts/run_shadow_promotion_lifecycle_audit.py",
            "created_at": generated.isoformat(),
            "read_only": True,
            "no_files_modified_except_audit_artifacts": True,
            "apply_executed_by_audit": False,
            "rollback_executed_by_audit": False,
        },
    }
    _assert_audit_safety_invariants(payload)
    return payload


def render_shadow_promotion_lifecycle_audit_report(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    chain = _mapping(payload.get("artifact_chain"))
    safety = _mapping(payload.get("safety_boundary_audit"))
    findings = _mapping(payload.get("audit_findings"))
    weight_lifecycle = _mapping(payload.get("weight_lifecycle"))
    before = _mapping(weight_lifecycle.get("production_weights_before_apply"))
    after_apply = _mapping(weight_lifecycle.get("production_weights_after_apply"))
    after_rollback = weight_lifecycle.get("production_weights_after_rollback")
    net_delta = _mapping(weight_lifecycle.get("net_delta_after_lifecycle"))

    lines = [
        f"# Shadow Promotion Lifecycle Audit - {payload.get('date')}",
        "",
        "## 1. Audit Summary",
        "",
        f"- Lifecycle Decision: `{payload.get('lifecycle_decision')}`",
        f"- Promotion Date: `{payload.get('promotion_date')}`",
        "- Production Effect: `none`",
        "- Audit Only: `true`",
        "- Apply Executed By Audit: `false`",
        "- Rollback Executed By Audit: `false`",
        "- Broker Execution: `false`",
        "- Replay Execution: `false`",
        "- Trading Execution: `false`",
        "",
        "## 2. Artifact Chain",
        "",
        "| Stage | Status | Path | SHA256 |",
        "|---|---:|---|---|",
    ]
    for key, label in (
        (STAGE_PROPOSAL, "018D Proposal"),
        (STAGE_PREFLIGHT, "018E1 Preflight"),
        (STAGE_APPLY, "018E2 Apply Result"),
        (STAGE_ROLLBACK, "018E3 Rollback Result"),
    ):
        artifact = _mapping(artifacts.get(key))
        lines.append(
            f"| {label} | `{artifact.get('status', 'MISSING')}` | "
            f"`{artifact.get('path', '')}` | `{artifact.get('sha256', '')}` |"
        )

    lines.extend(
        [
            "",
            "## 3. Stage Summary",
            "",
            "| Stage | Decision | Executed | Production Effect |",
            "|---|---:|---:|---|",
            _stage_summary_row("Proposal", _mapping(payload.get("proposal_summary"))),
            _stage_summary_row("Preflight", _mapping(payload.get("preflight_summary"))),
            _stage_summary_row("Apply", _mapping(payload.get("apply_summary"))),
            _stage_summary_row("Rollback", _mapping(payload.get("rollback_summary"))),
            "",
            "## 4. Weight Lifecycle",
            "",
            "| Weight Key | Before Apply | After Apply | After Rollback | Net Delta |",
            "|---|---:|---:|---:|---:|",
        ],
    )
    for key in _ordered_weight_keys(before, after_apply, _mapping(after_rollback), net_delta):
        lines.append(
            "| "
            f"{key} | {_format_float(before.get(key))} | {_format_float(after_apply.get(key))} | "
            f"{_format_float(_mapping(after_rollback).get(key))} | "
            f"{_format_signed_float(net_delta.get(key))} |"
        )

    lines.extend(
        [
            "",
            "## 5. Safety Boundary Audit",
            "",
            "| Check | Status | Notes |",
            "|---|---:|---|",
            _safety_row("broker_execution", not payload.get("broker_execution"), "false"),
            _safety_row("replay_execution", not payload.get("replay_execution"), "false"),
            _safety_row("trading_execution", not payload.get("trading_execution"), "false"),
            _safety_row(
                "dashboard_read_only",
                safety.get("dashboard_read_only") is True,
                "no pipeline trigger detected",
            ),
            _safety_row(
                "scheduler_did_not_apply_or_rollback",
                safety.get("scheduler_did_not_apply_or_rollback") is True,
                "true",
            ),
            "",
            "## 6. Critical Findings",
            "",
        ],
    )
    critical = _strings(findings.get("critical_findings"))
    lines.extend([f"- {item}" for item in critical] or ["- None."])
    lines.extend(["", "## 7. Warnings", ""])
    warnings = _strings(findings.get("warnings"))
    lines.extend([f"- {item}" for item in warnings] or ["- None."])
    lines.extend(["", "## 8. Notes", ""])
    notes = _strings(findings.get("notes"))
    lines.extend([f"- {item}" for item in notes] or ["- None."])
    lines.extend(
        [
            "",
            "## 9. Traceability",
            "",
            f"- Artifact Chain Status: `{chain.get('status', 'FAIL')}`",
        ],
    )
    for key in (STAGE_PROPOSAL, STAGE_PREFLIGHT, STAGE_APPLY, STAGE_ROLLBACK):
        artifact = _mapping(artifacts.get(key))
        lines.append(
            f"- `{key}`: status=`{artifact.get('status', '')}`, "
            f"path=`{artifact.get('path', '')}`, sha256=`{artifact.get('sha256', '')}`"
        )
    lines.extend(
        [
            "",
            "## 10. Next Step",
            "",
            "Use this report as the lifecycle audit record for the promotion event.",
            "",
        ],
    )
    return "\n".join(lines)


def render_shadow_promotion_lifecycle_audit_run_log(payload: dict[str, Any]) -> str:
    lines = [
        f"# Shadow Promotion Lifecycle Audit Run - {payload.get('date')}",
        "",
        f"- Run Status: `{payload.get('run_status')}`",
        f"- Lifecycle Decision: `{payload.get('lifecycle_decision')}`",
        f"- Promotion Date: `{payload.get('promotion_date')}`",
        "- Production Effect: `none`",
        "- Audit Only: `true`",
        f"- Critical Findings: `{payload.get('critical_findings_count')}`",
        f"- Warnings: `{payload.get('warnings_count')}`",
        f"- JSON: `{payload.get('audit_json')}`",
        f"- Markdown: `{payload.get('audit_markdown')}`",
        "",
    ]
    return "\n".join(lines)


def _build_artifact_chain(
    *,
    promotion_date: date,
    stage_payloads: dict[str, dict[str, Any]],
    stage_paths: dict[str, Path],
    stage_artifacts: dict[str, dict[str, Any]],
    audit_findings: dict[str, list[str]],
) -> dict[str, Any]:
    blocking: list[str] = []
    critical: list[str] = []
    proposal_to_preflight = False
    preflight_to_apply = False
    apply_to_rollback = False
    target_profile_consistent = True

    proposal = stage_payloads[STAGE_PROPOSAL]
    preflight = stage_payloads[STAGE_PREFLIGHT]
    apply_result = stage_payloads[STAGE_APPLY]
    rollback_result = stage_payloads[STAGE_ROLLBACK]

    if not _artifact_found(stage_artifacts[STAGE_PROPOSAL]) and (
        _artifact_found(stage_artifacts[STAGE_PREFLIGHT])
        or _artifact_found(stage_artifacts[STAGE_APPLY])
        or _artifact_found(stage_artifacts[STAGE_ROLLBACK])
    ):
        blocking.append("proposal_missing_but_later_artifacts_found")

    if _artifact_found(stage_artifacts[STAGE_PREFLIGHT]):
        proposal_ref = _mapping(
            _mapping(preflight.get("input_artifacts")).get("promotion_proposal")
        )
        if not proposal_ref:
            blocking.append("preflight_missing_proposal_reference")
        elif not _artifact_found(stage_artifacts[STAGE_PROPOSAL]):
            blocking.append("proposal_missing_for_preflight_reference")
        else:
            proposal_to_preflight = _validate_reference(
                label="proposal_to_preflight",
                ref=proposal_ref,
                actual_path=stage_paths[STAGE_PROPOSAL],
                actual_sha256=_string_value(stage_artifacts[STAGE_PROPOSAL].get("sha256")),
                blocking=blocking,
                critical=critical,
            )
            preflight_proposal_decision = _string_value(
                _mapping(preflight.get("proposal_validation")).get("proposal_decision")
            )
            proposal_decision = _string_value(proposal.get("proposal_decision"))
            if preflight_proposal_decision and preflight_proposal_decision != proposal_decision:
                critical.append("proposal_to_preflight:proposal_decision_mismatch")
                proposal_to_preflight = False
            if _string_value(proposal.get("date")) != promotion_date.isoformat():
                critical.append("proposal_to_preflight:proposal_date_mismatch")
                proposal_to_preflight = False
            if _string_value(preflight.get("date")) != promotion_date.isoformat():
                critical.append("proposal_to_preflight:preflight_date_mismatch")
                proposal_to_preflight = False

    if _artifact_found(stage_artifacts[STAGE_APPLY]):
        preflight_ref = _mapping(_mapping(apply_result.get("input_artifacts")).get("preflight"))
        if not preflight_ref:
            blocking.append("apply_missing_preflight_reference")
        elif not _artifact_found(stage_artifacts[STAGE_PREFLIGHT]):
            blocking.append("preflight_missing_for_apply_reference")
        else:
            preflight_to_apply = _validate_reference(
                label="preflight_to_apply",
                ref=preflight_ref,
                actual_path=stage_paths[STAGE_PREFLIGHT],
                actual_sha256=_string_value(stage_artifacts[STAGE_PREFLIGHT].get("sha256")),
                blocking=blocking,
                critical=critical,
            )
            apply_preflight_decision = _string_value(
                _mapping(apply_result.get("preflight_validation")).get("preflight_decision")
            )
            if apply_preflight_decision != PREFLIGHT_DECISION_PASS:
                critical.append("preflight_to_apply:preflight_decision_not_pass")
                preflight_to_apply = False
            apply_target = _mapping(apply_result.get("target_profile_validation"))
            preflight_diff = _mapping(preflight.get("diff_preview"))
            if _string_value(apply_target.get("sha256_expected_from_preflight")) != _string_value(
                preflight_diff.get("target_profile_sha256_before")
            ):
                critical.append("preflight_to_apply:target_profile_sha256_mismatch")
                preflight_to_apply = False
                target_profile_consistent = False

    if _artifact_found(stage_artifacts[STAGE_ROLLBACK]):
        apply_ref = _mapping(_mapping(rollback_result.get("input_artifacts")).get("apply_result"))
        if not apply_ref:
            blocking.append("rollback_missing_apply_result_reference")
        elif not _artifact_found(stage_artifacts[STAGE_APPLY]):
            blocking.append("apply_missing_for_rollback_reference")
        else:
            apply_to_rollback = _validate_reference(
                label="apply_to_rollback",
                ref=apply_ref,
                actual_path=stage_paths[STAGE_APPLY],
                actual_sha256=_string_value(stage_artifacts[STAGE_APPLY].get("sha256")),
                blocking=blocking,
                critical=critical,
            )
            rollback_apply_decision = _string_value(
                _mapping(rollback_result.get("apply_result_validation")).get("apply_decision")
            )
            if rollback_apply_decision != APPLY_DECISION_APPLIED:
                critical.append("apply_to_rollback:apply_decision_not_applied")
                apply_to_rollback = False
            rollback_snapshot_ref = _mapping(
                _mapping(rollback_result.get("input_artifacts")).get("rollback_snapshot")
            )
            apply_rollback = _mapping(apply_result.get("rollback"))
            expected_snapshot_shas = [
                _string_value(apply_rollback.get("snapshot_sha256")),
                _string_value(apply_rollback.get("snapshot_file_sha256")),
            ]
            actual_snapshot_sha = _string_value(rollback_snapshot_ref.get("sha256"))
            if actual_snapshot_sha not in {item for item in expected_snapshot_shas if item}:
                critical.append("apply_to_rollback:rollback_snapshot_sha256_mismatch")
                apply_to_rollback = False
            rollback_target = _mapping(rollback_result.get("target_profile_validation"))
            apply_target = _mapping(apply_result.get("target_profile_validation"))
            apply_after = _string_value(apply_target.get("sha256_after"))
            rollback_expected_current = _string_value(
                rollback_target.get("sha256_expected_current")
            )
            if (
                apply_after
                and rollback_expected_current
                and rollback_expected_current != apply_after
            ):
                critical.append("apply_to_rollback:target_profile_current_sha256_mismatch")
                apply_to_rollback = False
                target_profile_consistent = False

    if critical:
        audit_findings["critical_findings"].extend(critical)
    return {
        "status": "PASS" if not blocking and not critical else "FAIL",
        "proposal_to_preflight_match": proposal_to_preflight,
        "preflight_to_apply_match": preflight_to_apply,
        "apply_to_rollback_match": apply_to_rollback,
        "target_profile_consistent": target_profile_consistent,
        "blocking_reasons": blocking + critical,
    }


def _build_safety_boundary_audit(
    *,
    stage_payloads: dict[str, dict[str, Any]],
    stage_artifacts: dict[str, dict[str, Any]],
    audit_findings: dict[str, list[str]],
) -> dict[str, Any]:
    blocking: list[str] = []
    for stage, payload in stage_payloads.items():
        if not _artifact_found(stage_artifacts[stage]):
            continue
        blocking.extend(_stage_safety_findings(stage=stage, payload=payload))
    if blocking:
        audit_findings["critical_findings"].extend(blocking)
    return {
        "status": "PASS" if not blocking else "FAIL",
        "proposal_safe": not any(item.startswith("proposal:") for item in blocking),
        "preflight_safe": not any(item.startswith("preflight:") for item in blocking),
        "apply_safe": not any(item.startswith("apply_result:") for item in blocking),
        "rollback_safe": not any(item.startswith("rollback_result:") for item in blocking),
        "dashboard_read_only": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "scheduler_did_not_apply_or_rollback": True,
        "blocking_reasons": blocking,
    }


def _stage_safety_findings(*, stage: str, payload: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is True:
            findings.append(f"{stage}:{field}_true")

    if stage == STAGE_PROPOSAL:
        if payload.get("promotion_executed") is True:
            findings.append("proposal:promotion_executed_true")
        if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
            findings.append("proposal:production_effect_not_none")
        if payload.get("manual_review_only") is not True:
            findings.append("proposal:manual_review_only_not_true")

    if stage == STAGE_PREFLIGHT:
        if payload.get("apply_executed") is True:
            findings.append("preflight:apply_executed_true")
        if payload.get("promotion_executed") is True:
            findings.append("preflight:promotion_executed_true")
        if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
            findings.append("preflight:production_effect_not_none")
        if payload.get("manual_review_only") is not True:
            findings.append("preflight:manual_review_only_not_true")

    if stage == STAGE_APPLY:
        apply_decision = _string_value(payload.get("apply_decision"))
        apply_executed = payload.get("apply_executed") is True
        if apply_executed and apply_decision != APPLY_DECISION_APPLIED:
            findings.append("apply_result:apply_executed_true_but_decision_not_applied")
        if apply_decision == APPLY_DECISION_APPLIED and not _apply_rollback_snapshot_exists(
            payload
        ):
            findings.append("apply_result:applied_but_rollback_snapshot_missing")
        if apply_executed and payload.get("manual_review_only") is not True:
            findings.append("apply_result:manual_review_only_not_true")

    if stage == STAGE_ROLLBACK:
        rollback_decision = _string_value(payload.get("rollback_decision"))
        rollback_executed = payload.get("rollback_executed") is True
        if rollback_executed and rollback_decision != ROLLBACK_DECISION_ROLLED_BACK:
            findings.append("rollback_result:rollback_executed_true_but_decision_not_rolled_back")
        post_rollback_status = _mapping(payload.get("post_rollback_validation")).get("status")
        if rollback_executed and post_rollback_status != "PASS":
            findings.append("rollback_result:post_rollback_validation_not_pass")
        if rollback_executed and payload.get("manual_review_only") is not True:
            findings.append("rollback_result:manual_review_only_not_true")

    contract = _mapping(payload.get("pipeline_contract"))
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is True:
            findings.append(f"{stage}:pipeline_contract_{field}_true")
    return findings


def _build_weight_lifecycle(
    *,
    proposal: dict[str, Any],
    preflight: dict[str, Any],
    apply_result: dict[str, Any],
    rollback_result: dict[str, Any],
    audit_findings: dict[str, list[str]],
) -> dict[str, Any]:
    before = _first_weights(
        _mapping(_mapping(apply_result.get("diff_applied")).get("production_weights_before")),
        _mapping(_mapping(preflight.get("diff_preview")).get("production_weights_before")),
        _mapping(proposal.get("production_weights")),
    )
    after_apply = _first_weights(
        _mapping(_mapping(apply_result.get("diff_applied")).get("production_weights_after")),
        _mapping(_mapping(preflight.get("diff_preview")).get("production_weights_after_preview")),
        _mapping(proposal.get("proposed_production_weights")),
    )
    after_rollback = _first_weights(
        _mapping(
            _mapping(rollback_result.get("rollback_applied")).get(
                "production_weights_after_rollback"
            )
        ),
        (
            _rollback_snapshot_weights(apply_result=apply_result, rollback_result=rollback_result)
            if rollback_result
            else {}
        ),
    )
    if not rollback_result:
        audit_findings["warnings"].append(
            "Unable to derive production_weights_after_rollback because rollback result "
            "was not found."
        )
    elif not after_rollback:
        audit_findings["warnings"].append(
            "Unable to derive production_weights_after_rollback from rollback result or snapshot."
        )

    final_weights = after_rollback or after_apply
    return {
        "production_weights_before_apply": before or None,
        "production_weights_after_apply": after_apply or None,
        "production_weights_after_rollback": after_rollback or None,
        "apply_delta": _weight_delta(before, after_apply),
        "rollback_delta": _weight_delta(after_apply, after_rollback),
        "net_delta_after_lifecycle": _weight_delta(before, final_weights),
    }


def _proposal_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "proposal_decision": _string_value(payload.get("proposal_decision")) or "MISSING",
        "promotion_proposed": payload.get("promotion_proposed") is True,
        "promotion_executed": payload.get("promotion_executed") is True,
        "production_effect": _string_value(payload.get("production_effect"))
        or PRODUCTION_EFFECT_NONE,
        "manual_review_only": payload.get("manual_review_only") is True,
    }


def _preflight_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "preflight_decision": _string_value(payload.get("preflight_decision")) or "MISSING",
        "preflight_only": payload.get("preflight_only") is True,
        "apply_executed": payload.get("apply_executed") is True,
        "promotion_executed": payload.get("promotion_executed") is True,
        "production_effect": _string_value(payload.get("production_effect"))
        or PRODUCTION_EFFECT_NONE,
    }


def _apply_summary(payload: dict[str, Any]) -> dict[str, Any]:
    rollback = _mapping(payload.get("rollback"))
    target = _mapping(payload.get("target_profile_validation"))
    return {
        "apply_decision": _string_value(payload.get("apply_decision")) or "MISSING",
        "apply_executed": payload.get("apply_executed") is True,
        "promotion_executed": payload.get("promotion_executed") is True,
        "production_effect": _string_value(payload.get("production_effect"))
        or PRODUCTION_EFFECT_NONE,
        "target_profile_path": _string_value(target.get("path")),
        "rollback_snapshot_created": rollback.get("snapshot_created") is True,
    }


def _rollback_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "rollback_present": bool(payload),
        "rollback_decision": _string_value(payload.get("rollback_decision")) or "MISSING",
        "rollback_executed": payload.get("rollback_executed") is True,
        "production_effect": _string_value(payload.get("production_effect"))
        or PRODUCTION_EFFECT_NONE,
    }


def _lifecycle_decision(
    *,
    artifact_chain: dict[str, Any],
    audit_findings: dict[str, list[str]],
    stage_payloads: dict[str, dict[str, Any]],
    stage_artifacts: dict[str, dict[str, Any]],
) -> str:
    if audit_findings["critical_findings"]:
        return DECISION_SAFETY_ANOMALY
    proposal_found = _artifact_found(stage_artifacts[STAGE_PROPOSAL])
    preflight_found = _artifact_found(stage_artifacts[STAGE_PREFLIGHT])
    apply_found = _artifact_found(stage_artifacts[STAGE_APPLY])
    rollback_found = _artifact_found(stage_artifacts[STAGE_ROLLBACK])
    if artifact_chain.get("status") != "PASS":
        return DECISION_INCOMPLETE_ARTIFACTS
    if not proposal_found:
        return DECISION_INCOMPLETE_ARTIFACTS

    apply_result = stage_payloads[STAGE_APPLY]
    rollback_result = stage_payloads[STAGE_ROLLBACK]
    if rollback_found and (
        _string_value(rollback_result.get("rollback_decision")) != ROLLBACK_DECISION_ROLLED_BACK
        or rollback_result.get("rollback_executed") is not True
    ):
        return DECISION_ROLLBACK_FAILED_OR_BLOCKED
    if apply_found and (
        _string_value(apply_result.get("apply_decision")) != APPLY_DECISION_APPLIED
        or apply_result.get("apply_executed") is not True
    ):
        return DECISION_APPLY_FAILED_OR_BLOCKED
    if proposal_found and not preflight_found and not apply_found and not rollback_found:
        return DECISION_PROPOSAL_ONLY
    if proposal_found and preflight_found and not apply_found and not rollback_found:
        return DECISION_PREFLIGHT_ONLY
    if proposal_found and preflight_found and apply_found and not rollback_found:
        return DECISION_COMPLETE_APPLIED_NO_ROLLBACK
    if proposal_found and preflight_found and apply_found and rollback_found:
        return DECISION_COMPLETE_WITH_ROLLBACK
    return DECISION_INCOMPLETE_ARTIFACTS


def _lifecycle_reason(
    *,
    lifecycle_decision: str,
    artifact_chain: dict[str, Any],
    audit_findings: dict[str, list[str]],
    rollback_summary: dict[str, Any],
) -> str:
    if lifecycle_decision == DECISION_COMPLETE_WITH_ROLLBACK:
        return (
            "Proposal, preflight, apply, and rollback artifacts were found and safety "
            "boundaries were consistent."
        )
    if lifecycle_decision == DECISION_COMPLETE_APPLIED_NO_ROLLBACK:
        return (
            "Proposal, preflight, and APPLIED apply result were found; rollback result was "
            "not found, and no safety anomaly was detected."
        )
    if lifecycle_decision == DECISION_PROPOSAL_ONLY:
        return "Only the promotion proposal artifact was found; no later lifecycle artifacts exist."
    if lifecycle_decision == DECISION_PREFLIGHT_ONLY:
        return (
            "Proposal and preflight artifacts were found; apply and rollback artifacts "
            "are absent."
        )
    if lifecycle_decision == DECISION_APPLY_FAILED_OR_BLOCKED:
        return "Apply result exists but was not APPLIED or did not execute."
    if lifecycle_decision == DECISION_ROLLBACK_FAILED_OR_BLOCKED:
        return (
            "Rollback result exists but was not ROLLED_BACK or did not execute; "
            f"rollback_decision={rollback_summary.get('rollback_decision')}."
        )
    if lifecycle_decision == DECISION_SAFETY_ANOMALY:
        return "; ".join(audit_findings["critical_findings"]) or "Safety anomaly detected."
    if lifecycle_decision == DECISION_INCOMPLETE_ARTIFACTS:
        reasons = _strings(artifact_chain.get("blocking_reasons"))
        return "; ".join(reasons) or "Required artifacts are missing or cannot be linked."
    return "Unexpected lifecycle audit error."


def _add_lifecycle_notes(
    *,
    lifecycle_decision: str,
    rollback_present: bool,
    audit_findings: dict[str, list[str]],
) -> None:
    if lifecycle_decision == DECISION_COMPLETE_WITH_ROLLBACK:
        audit_findings["notes"].append(
            "Rollback result was present and indicates production weights were restored."
        )
    elif lifecycle_decision == DECISION_COMPLETE_APPLIED_NO_ROLLBACK and not rollback_present:
        audit_findings["notes"].append("Rollback result was not present for this promotion audit.")


def _run_log_payload(*, payload: dict[str, Any], generated_at: datetime) -> dict[str, Any]:
    outputs = _mapping(payload.get("outputs"))
    findings = _mapping(payload.get("audit_findings"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RUN_REPORT_TYPE,
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "promotion_date": payload.get("promotion_date"),
        "generated_at": generated_at.isoformat(),
        "run_status": (
            "PASS"
            if payload.get("lifecycle_decision") != DECISION_SAFETY_ANOMALY
            else "SAFETY_ANOMALY"
        ),
        "lifecycle_decision": payload.get("lifecycle_decision"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "audit_only": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "artifact_chain_status": _mapping(payload.get("artifact_chain")).get("status"),
        "safety_boundary_status": _mapping(payload.get("safety_boundary_audit")).get("status"),
        "critical_findings_count": len(_strings(findings.get("critical_findings"))),
        "warnings_count": len(_strings(findings.get("warnings"))),
        "audit_json": outputs.get("json"),
        "audit_markdown": outputs.get("markdown"),
    }


def _error_payload(
    *,
    as_of: date,
    promotion_date: date,
    data_root: Path,
    output_json_path: Path,
    output_md_path: Path,
    run_log_json_path: Path,
    run_log_md_path: Path,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "promotion_date": promotion_date.isoformat(),
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "audit_only": True,
        "apply_executed_by_audit": False,
        "rollback_executed_by_audit": False,
        "safe_for_scheduler": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "lifecycle_decision": DECISION_ERROR,
        "lifecycle_reason": error,
        "input_artifacts": {
            STAGE_PROPOSAL: _input_artifact_record(
                default_proposal_json_path(data_root, promotion_date)
            ),
            STAGE_PREFLIGHT: _input_artifact_record(
                default_preflight_json_path(data_root, promotion_date)
            ),
            STAGE_APPLY: _input_artifact_record(
                default_apply_result_json_path(data_root, promotion_date)
            ),
            STAGE_ROLLBACK: _input_artifact_record(
                default_rollback_result_json_path(data_root, promotion_date),
                optional=True,
            ),
        },
        "artifact_chain": {"status": "FAIL", "blocking_reasons": [error]},
        "proposal_summary": _proposal_summary({}),
        "preflight_summary": _preflight_summary({}),
        "apply_summary": _apply_summary({}),
        "rollback_summary": _rollback_summary({}),
        "weight_lifecycle": {},
        "safety_boundary_audit": {
            "status": "FAIL",
            "dashboard_read_only": True,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "scheduler_did_not_apply_or_rollback": True,
            "blocking_reasons": [error],
        },
        "audit_findings": {"critical_findings": [], "warnings": [], "notes": [error]},
        "outputs": {
            "json": str(output_json_path),
            "markdown": str(output_md_path),
            "run_log_json": str(run_log_json_path),
            "run_log_markdown": str(run_log_md_path),
        },
        "audit": {
            "created_by": "scripts/run_shadow_promotion_lifecycle_audit.py",
            "created_at": generated_at.isoformat(),
            "read_only": True,
            "no_files_modified_except_audit_artifacts": True,
        },
    }


def _input_artifact_record(path: Path, *, optional: bool = False) -> dict[str, Any]:
    record = _artifact_record(path)
    if optional and record["status"] == "MISSING":
        record["status"] = "NOT_FOUND"
    return record


def _validate_reference(
    *,
    label: str,
    ref: dict[str, Any],
    actual_path: Path,
    actual_sha256: str,
    blocking: list[str],
    critical: list[str],
) -> bool:
    declared_path = _string_value(ref.get("path"))
    declared_sha = _string_value(ref.get("sha256"))
    if not declared_path:
        blocking.append(f"{label}:path_missing")
    if not declared_sha:
        blocking.append(f"{label}:sha256_missing")
    path_ok = bool(declared_path) and _path_matches(declared_path, actual_path)
    sha_ok = bool(actual_sha256) and bool(declared_sha) and declared_sha == actual_sha256
    if declared_path and not path_ok:
        critical.append(f"{label}:path_mismatch")
    if declared_sha and not sha_ok:
        critical.append(f"{label}:sha256_mismatch")
    return path_ok and sha_ok


def _apply_rollback_snapshot_exists(payload: dict[str, Any]) -> bool:
    rollback = _mapping(payload.get("rollback"))
    snapshot_path = Path(_string_value(rollback.get("snapshot_path")))
    snapshot_created = rollback.get("snapshot_created") is True
    snapshot_sha = _string_value(rollback.get("snapshot_sha256")) or _string_value(
        rollback.get("snapshot_file_sha256")
    )
    return snapshot_created and bool(snapshot_sha) and snapshot_path.exists()


def _artifact_found(artifact: dict[str, Any]) -> bool:
    return artifact.get("status") == "FOUND" and artifact.get("exists") is True


def _first_weights(*candidates: dict[str, Any]) -> dict[str, float]:
    for candidate in candidates:
        parsed = _weights_from_mapping(candidate)
        if parsed:
            return parsed
    return {}


def _weights_from_mapping(payload: dict[str, Any]) -> dict[str, float]:
    weights: dict[str, float] = {}
    for key, value in payload.items():
        try:
            if isinstance(value, bool):
                continue
            weights[str(key)] = round(float(value), 10)
        except (TypeError, ValueError):
            continue
    return dict(sorted(weights.items()))


def _rollback_snapshot_weights(
    *,
    apply_result: dict[str, Any],
    rollback_result: dict[str, Any],
) -> dict[str, Any]:
    snapshot_path = ""
    if rollback_result:
        snapshot_path = _string_value(
            _mapping(_mapping(rollback_result.get("input_artifacts")).get("rollback_snapshot")).get(
                "path"
            )
        )
    if not snapshot_path:
        snapshot_path = _string_value(_mapping(apply_result.get("rollback")).get("snapshot_path"))
    if not snapshot_path:
        return {}
    snapshot = _read_structured_object(Path(snapshot_path))
    if not snapshot:
        return {}
    try:
        _, weights = get_profile_weights(snapshot)
    except ValueError:
        return _mapping(snapshot.get("weights"))
    return weights


def _weight_delta(
    before: dict[str, float],
    after: dict[str, float],
) -> dict[str, float] | None:
    if not before or not after:
        return None
    keys = sorted(set(before) | set(after))
    return {key: round(after.get(key, 0.0) - before.get(key, 0.0), 10) for key in keys}


def _ordered_weight_keys(*mappings: dict[str, Any]) -> list[str]:
    keys: set[str] = set()
    for mapping in mappings:
        keys.update(mapping.keys())
    return sorted(keys)


def _stage_summary_row(stage: str, summary: dict[str, Any]) -> str:
    if stage == "Proposal":
        decision = summary.get("proposal_decision", "MISSING")
        executed = summary.get("promotion_executed", False)
    elif stage == "Preflight":
        decision = summary.get("preflight_decision", "MISSING")
        executed = summary.get("apply_executed", False)
    elif stage == "Apply":
        decision = summary.get("apply_decision", "MISSING")
        executed = summary.get("apply_executed", False)
    else:
        decision = summary.get("rollback_decision", "MISSING")
        executed = summary.get("rollback_executed", False)
    return (
        f"| {stage} | `{decision}` | `{str(executed).lower()}` | "
        f"`{summary.get('production_effect', PRODUCTION_EFFECT_NONE)}` |"
    )


def _safety_row(check: str, passed: bool, notes: str) -> str:
    return f"| {check} | `{'PASS' if passed else 'FAIL'}` | {notes} |"


def _assert_audit_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("lifecycle audit production_effect must remain none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("lifecycle audit must remain manual_review_only")
    if payload.get("audit_only") is not True:
        raise ValueError("lifecycle audit must remain audit_only")
    if payload.get("apply_executed_by_audit") is not False:
        raise ValueError("lifecycle audit must not execute apply")
    if payload.get("rollback_executed_by_audit") is not False:
        raise ValueError("lifecycle audit must not execute rollback")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"lifecycle audit must keep {field}=false")
    if payload.get("safe_for_scheduler") is not True:
        raise ValueError("lifecycle audit report generation should be scheduler-safe")
