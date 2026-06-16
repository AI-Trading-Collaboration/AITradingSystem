from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
REPORT_TYPE = "research_safety_boundary_audit"
VALIDATION_REPORT_TYPE = "research_safety_boundary_validation"
PRODUCTION_EFFECT = "none"

PASS_STATUS = "SAFETY_PASS"
WARN_STATUS = "SAFETY_PASS_WITH_WARNINGS"
FAIL_STATUS = "SAFETY_BLOCKED"
VALID_STATUSES = {PASS_STATUS, WARN_STATUS, FAIL_STATUS}

SAFETY_DIMENSIONS: tuple[str, ...] = (
    "production_effect",
    "broker_effect",
    "order_effect",
    "manual_review_only",
    "official_target_weights",
)
DIMENSION_LABELS: dict[str, str] = {
    "production_effect": "production_effect",
    "broker_effect": "broker_effect",
    "order_effect": "order_effect",
    "manual_review_only": "manual_review_only",
    "official_target_weights": "official_target_weights",
}
DIMENSION_ALIASES: dict[str, tuple[str, ...]] = {
    "production_effect": (
        "production_effect",
        "production_mutation",
        "production_state_mutated",
        "does_not_modify_production",
    ),
    "broker_effect": (
        "broker_effect",
        "broker_action",
        "broker_action_allowed",
        "broker_action_taken",
        "broker_action_generated",
        "broker_integration",
    ),
    "order_effect": (
        "order_effect",
        "order_ticket",
        "order_ticket_generated",
        "order_ticket_allowed",
        "order_action",
        "orders_generated",
    ),
    "manual_review_only": (
        "manual_review_only",
        "manual_review_required",
        "owner_approval_required",
        "advisory_only",
        "research_only",
    ),
    "official_target_weights": (
        "official_target_weights",
        "official_target_weights_allowed",
        "official_target_weights_generated",
        "target_weights_official",
        "not_official_target_weights",
    ),
}
FORBIDDEN_CAPABILITIES: dict[str, tuple[str, ...]] = {
    "official_target_weights": (
        "official target weights",
        "official_target_weights",
        "target_weights_official",
    ),
    "broker_integration": (
        "broker integration",
        "broker workflow",
        "broker_action",
        "broker action",
    ),
    "order_tickets": (
        "order ticket",
        "order_ticket",
        "order tickets",
    ),
    "production_mutation": (
        "production mutation",
        "production state",
        "production_state",
        "production workflow",
    ),
    "automatic_live_allocation": (
        "automatic live allocation",
        "auto live allocation",
        "auto_apply=true",
        "automatic allocation",
    ),
}
NEGATION_MARKERS = (
    "no ",
    "not ",
    "never ",
    "without ",
    "cannot ",
    "can't ",
    "read-only",
    "read only",
    "manual-review-only",
    "manual review only",
    "guardrails",
    "unchanged",
    "production_effect=none",
    "not_official",
    "false",
    "none",
    "不得",
    "不新增",
    "不做",
    "不改变",
    "不写",
    "不补造",
    "不重跑",
    "不运行",
    "不刷新",
    "不修改",
    "不生成",
    "不允许",
    "不支持",
    "不自动",
    "不触发",
    "不接",
    "不会",
    "未",
    "禁止",
    "只读",
)
SAFE_TEXT_PATTERNS: dict[str, tuple[str, ...]] = {
    "production_effect": (
        "production_effect=none",
        '"production_effect": "none"',
        "production_state_mutated=false",
        '"production_state_mutated": false',
        "does_not_modify_production=true",
        '"does_not_modify_production": true',
    ),
    "broker_effect": (
        "broker_action_allowed=false",
        "broker_action_taken=false",
        "broker_action=none",
        '"broker_action_allowed": false',
        '"broker_action_taken": false',
    ),
    "order_effect": (
        "order_ticket_generated=false",
        "order_ticket_allowed=false",
        "order_effect=none",
        '"order_ticket_generated": false',
    ),
    "manual_review_only": (
        "manual_review_only=true",
        "manual_review_required=true",
        "advisory_only=true",
        "research_only=true",
        '"manual_review_only": true',
        '"manual_review_required": true',
    ),
    "official_target_weights": (
        "not_official_target_weights=true",
        "official_target_weights=false",
        "official_target_weights_mutated=false",
        "official_target_weights_write_blocked=true",
        '"not_official_target_weights": true',
        '"official_target_weights": false',
    ),
}
UNSAFE_TEXT_PATTERNS: dict[str, tuple[str, ...]] = {
    "production_effect": (
        "production_effect=true",
        "production_effect=production",
        '"production_effect": "production"',
        '"production_state_mutated": true',
    ),
    "broker_effect": (
        "broker_action_allowed=true",
        "broker_action_taken=true",
        '"broker_action_allowed": true',
        '"broker_action_taken": true',
    ),
    "order_effect": (
        "order_ticket_generated=true",
        "order_ticket_allowed=true",
        '"order_ticket_generated": true',
    ),
    "manual_review_only": (
        "manual_review_only=false",
        "manual_review_required=false",
        '"manual_review_only": false',
        '"manual_review_required": false',
    ),
    "official_target_weights": (
        "official_target_weights=true",
        '"official_target_weights": true',
        '"official_target_weights_generated": true',
    ),
}


def default_research_safety_boundary_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_safety_boundary_audit_{as_of.isoformat()}.json"


def default_research_safety_boundary_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_safety_boundary_audit_{as_of.isoformat()}.md"


def default_research_safety_boundary_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_safety_boundary_validation_{as_of.isoformat()}.json"


def default_research_safety_boundary_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_safety_boundary_validation_{as_of.isoformat()}.md"


def latest_research_safety_boundary_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "research_safety_boundary_audit_", ".json")


def build_research_safety_boundary_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    task_register_path: Path | None = None,
    completed_task_register_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    if report_index_payload is None:
        source_path = report_index_path or (
            project_root / "outputs" / "reports" / f"report_index_{as_of.isoformat()}.json"
        )
        report_index_payload = _read_json_mapping(source_path)
        report_index_path = source_path

    task_register_path = task_register_path or project_root / "docs" / "task_register.md"
    completed_task_register_path = (
        completed_task_register_path
        or project_root / "docs" / "task_register_completed.md"
    )
    task_checks = [
        *_scan_task_register(task_register_path, "active_task_register"),
        *_scan_task_register(completed_task_register_path, "completed_task_register"),
    ]
    artifact_checks = [
        _check_artifact_safety(report=report, project_root=project_root)
        for report in _records(report_index_payload.get("reports"))
        if report.get("exists") is True or report.get("latest_artifact_path")
    ]
    blocking_issues = _dedupe_issues(
        [
            issue
            for check in [*task_checks, *artifact_checks]
            for issue in _records(check.get("blocking_issues"))
        ]
    )
    warning_issues = _dedupe_issues(
        [
            issue
            for check in [*task_checks, *artifact_checks]
            for issue in _records(check.get("warning_issues"))
        ]
    )
    metadata_issues = [
        issue
        for check in artifact_checks
        for issue in _records(check.get("metadata_issues"))
    ]
    unsafe_signals = [
        issue
        for issue in blocking_issues
        if _text(issue.get("issue_id")).startswith("unsafe_")
    ]
    missing_metadata = [
        issue
        for issue in warning_issues
        if issue.get("issue_id") == "missing_safety_metadata"
    ]
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    summary = {
        "task_check_count": len(task_checks),
        "artifact_check_count": len(artifact_checks),
        "checked_artifact_count": len(
            [check for check in artifact_checks if check.get("artifact_read_status") == "READ"]
        ),
        "required_metadata_dimension_count": len(SAFETY_DIMENSIONS),
        "metadata_issue_count": len(metadata_issues),
        "missing_metadata_count": len(missing_metadata),
        "unsafe_signal_count": len(unsafe_signals),
        "blocking_issue_count": len(blocking_issues),
        "warning_issue_count": len(warning_issues),
        "shadow_continuation_readiness_input": (
            "BLOCKED_BY_SAFETY_AUDIT" if blocking_issues else "AVAILABLE_WITH_WARNINGS"
            if warning_issues
            else "AVAILABLE"
        ),
        "future_promotion_board_input": (
            "BLOCKED_BY_SAFETY_AUDIT" if blocking_issues else "REQUIRES_WARNING_REVIEW"
            if warning_issues
            else "SAFETY_AUDIT_PASS"
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "safety_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": (
            "Audit research tasks and report artifacts for explicit safety boundaries before "
            "shadow continuation or future promotion interpretation."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            "task_register": str(task_register_path),
            "completed_task_register": str(completed_task_register_path),
        },
        "output_decision": status,
        "safety_dimensions": [
            {"dimension": dimension, "label": DIMENSION_LABELS[dimension]}
            for dimension in SAFETY_DIMENSIONS
        ],
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "The audit reads existing task registers and report artifacts only.",
            "Legacy artifacts missing explicit metadata are warnings, not automatic repairs.",
            "The audit does not approve promotion, write target weights, or call a broker.",
        ],
        "next_action": _next_action(status),
        "summary": summary,
        "task_checks": task_checks,
        "artifact_checks": artifact_checks,
        "metadata_issues": _dedupe_issues(metadata_issues),
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(status, summary, blocking_issues, warning_issues),
        "methodology": {
            "mode": "read_existing_registers_report_index_and_artifacts_only",
            "required_safety_dimensions": list(SAFETY_DIMENSIONS),
            "unsafe_positive_signals_fail_closed": True,
            "legacy_missing_metadata_is_warning": True,
            "does_not_rewrite_historical_artifacts": True,
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_research_safety_boundary_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    source_status = _text(payload.get("safety_status"), _text(payload.get("status")))
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        "BLOCKING",
        f"report_type must be {REPORT_TYPE}.",
        "rerun_research_safety_boundary_audit",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "BLOCKING",
        "Research safety boundary audit must be production_effect=none.",
        "regenerate_audit_without_production_mutation",
    )
    _append_check(
        checks,
        blocking_issues,
        "status_enum",
        source_status in VALID_STATUSES,
        "BLOCKING",
        f"safety_status must be one of {', '.join(sorted(VALID_STATUSES))}.",
        "restore_safety_status_enum",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_dimensions_declared",
        set(_dimension_ids(payload.get("safety_dimensions"))) == set(SAFETY_DIMENSIONS),
        "BLOCKING",
        "Audit must declare required safety metadata dimensions.",
        "restore_required_safety_dimension_manifest",
    )
    _append_check(
        checks,
        blocking_issues,
        "source_not_blocked",
        source_status != FAIL_STATUS,
        "BLOCKING",
        f"source safety_status is {source_status}.",
        "fix_safety_boundary_blockers_before_promotion_or_shadow_continuation",
    )
    _append_check(
        checks,
        blocking_issues,
        "no_blocking_issues",
        _int(summary.get("blocking_issue_count")) == 0,
        "BLOCKING",
        "Audit summary must not contain blocking safety issues.",
        "fix_unsafe_research_boundary_signal",
    )
    _append_check(
        checks,
        warning_issues,
        "legacy_metadata_complete",
        _int(summary.get("missing_metadata_count")) == 0,
        "WARNING",
        "Some existing artifacts are missing explicit safety metadata.",
        "update_new_report_templates_to_emit_safety_metadata",
    )
    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    blocking_failed_checks = [
        check
        for check in checks
        if check["status"] == "FAIL" and check["severity"] == "BLOCKING"
    ]
    warning_failed_checks = [
        check
        for check in checks
        if check["status"] == "FAIL" and check["severity"] == "WARNING"
    ]
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len(blocking_failed_checks),
        "warning_check_count": len(warning_failed_checks),
        "source_task_check_count": _int(summary.get("task_check_count")),
        "source_artifact_check_count": _int(summary.get("artifact_check_count")),
        "source_missing_metadata_count": _int(summary.get("missing_metadata_count")),
        "source_unsafe_signal_count": _int(summary.get("unsafe_signal_count")),
        "task_check_count": _int(summary.get("task_check_count")),
        "artifact_check_count": _int(summary.get("artifact_check_count")),
        "missing_metadata_count": _int(summary.get("missing_metadata_count")),
        "unsafe_signal_count": _int(summary.get("unsafe_signal_count")),
        "blocking_issue_count": len(blocking_issues),
        "warning_issue_count": len(warning_issues),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "source_safety_status": source_status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": "Validate the research safety boundary audit fail-closed contract.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation reads an existing research safety boundary audit only.",
            (
                "Missing legacy safety metadata remains a warning unless an unsafe positive "
                "signal exists."
            ),
        ],
        "next_action": _validation_next_action(status),
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(status, validation_summary, blocking_issues, warning_issues),
        "methodology": {
            "mode": "read_existing_research_safety_boundary_audit_only",
            "production_effect": PRODUCTION_EFFECT,
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
        },
    }


def write_research_safety_boundary_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_research_safety_boundary_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_research_safety_boundary_markdown(payload), encoding="utf-8")
    return output_path


def write_research_safety_boundary_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_research_safety_boundary_json(payload, output_path)


def write_research_safety_boundary_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_research_safety_boundary_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_research_safety_boundary_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    reader = _mapping(payload.get("reader_brief"))
    lines = [
        f"# Research Safety Boundary Audit {payload.get('as_of')}",
        "",
        "## Reader Brief",
        "",
        f"- Summary：{_text(reader.get('summary'))}",
        f"- Key Result：{_text(reader.get('key_result'))}",
        f"- Blocking Issues：{_text(reader.get('blocking_issues'))}",
        f"- Warnings：{_text(reader.get('warnings'))}",
        f"- Safety Boundary：{_text(reader.get('safety_boundary'))}",
        f"- Next Action：{_text(reader.get('next_action'))}",
        "",
        "## Summary",
        "",
        f"- 状态：{_text(payload.get('safety_status'), 'UNKNOWN')}",
        f"- task checks：{summary.get('task_check_count')}",
        f"- artifact checks：{summary.get('artifact_check_count')}",
        f"- unsafe signals：{summary.get('unsafe_signal_count')}",
        f"- missing metadata：{summary.get('missing_metadata_count')}",
        (
            "- shadow continuation readiness input："
            f"{summary.get('shadow_continuation_readiness_input')}"
        ),
        f"- future promotion board input：{summary.get('future_promotion_board_input')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- next_action：{_text(payload.get('next_action'))}",
        "",
        "## Safety Dimensions",
        "",
        "|dimension|label|",
        "|---|---|",
    ]
    for dimension in _records(payload.get("safety_dimensions")):
        lines.append(
            f"|{_markdown_cell(dimension.get('dimension'))}|"
            f"{_markdown_cell(dimension.get('label'))}|"
        )
    lines.extend(
        [
            "",
            "## Blocking Issues",
            "",
            "|issue_id|scope|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("blocking_issues"))[:80]:
        lines.append(_issue_row(issue))
    if not _records(payload.get("blocking_issues")):
        lines.append("|NONE|research_safety_boundary|无阻断项。||")
    lines.extend(
        [
            "",
            "## Warning Issues",
            "",
            "|issue_id|scope|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("warning_issues"))[:120]:
        lines.append(_issue_row(issue))
    if not _records(payload.get("warning_issues")):
        lines.append("|NONE|research_safety_boundary|无 warning。||")
    lines.extend(
        [
            "",
            "## Artifact Metadata Issues",
            "",
            "|report_id|dimension|status|field|artifact_path|",
            "|---|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("metadata_issues"))[:120]:
        lines.append(
            f"|{_markdown_cell(issue.get('report_id'))}|"
            f"{_markdown_cell(issue.get('dimension'))}|"
            f"{_markdown_cell(issue.get('status'))}|"
            f"{_markdown_cell(issue.get('field'))}|"
            f"{_markdown_cell(issue.get('artifact_path'))}|"
        )
    if not _records(payload.get("metadata_issues")):
        lines.append("|NONE||||")
    lines.extend(
        [
            "",
            "## Methodology",
            "",
            (
                "本 audit 只读取 task registers、report index 和既有 report artifacts；"
                "不重写历史 artifact、"
            ),
            "不运行上游、不刷新数据、不写 official target weights、不触发 broker/order。",
            "",
        ]
    )
    return "\n".join(lines)


def render_research_safety_boundary_validation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Research Safety Boundary Validation {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('validation_status'), 'UNKNOWN')}",
        f"- source_safety_status：{_text(payload.get('source_safety_status'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- checks：{summary.get('check_count')}",
        f"- failed：{summary.get('failed_check_count')}",
        f"- warnings：{summary.get('warning_check_count')}",
        f"- next_action：{_text(payload.get('next_action'))}",
        "",
        "## Checks",
        "",
        "|check_id|status|severity|message|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_markdown_cell(check.get('check_id'))}|"
            f"{_markdown_cell(check.get('status'))}|"
            f"{_markdown_cell(check.get('severity'))}|"
            f"{_markdown_cell(check.get('message'))}|"
        )
    lines.extend(
        [
            "",
            "## Blocking Issues",
            "",
            "|issue_id|scope|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("blocking_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("blocking_issues")):
        lines.append("|NONE|research_safety_boundary_validation|无阻断项。||")
    lines.extend(
        [
            "",
            "## Warning Issues",
            "",
            "|issue_id|scope|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("warning_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("warning_issues")):
        lines.append("|NONE|research_safety_boundary_validation|无 warning。||")
    return "\n".join(lines) + "\n"


def _scan_task_register(path: Path, source_id: str) -> list[dict[str, Any]]:
    if not path.exists():
        issue = _issue(
            issue_id="task_register_missing",
            severity="BLOCKING",
            scope=source_id,
            message=f"{source_id} is missing.",
            recommended_action="restore_task_register_before_safety_audit",
            artifact_path=path,
        )
        return [
            {
                "source_id": source_id,
                "artifact_path": str(path),
                "line_number": 0,
                "status": FAIL_STATUS,
                "blocking_issues": [issue],
                "warning_issues": [],
            }
        ]
    checks: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.startswith("|") or line.startswith("|---") or line.startswith("|ID|"):
            continue
        blocking_issues = _task_boundary_issues(
            line,
            source_id=source_id,
            line_number=line_number,
            artifact_path=path,
        )
        checks.append(
            {
                "source_id": source_id,
                "artifact_path": str(path),
                "line_number": line_number,
                "status": FAIL_STATUS if blocking_issues else PASS_STATUS,
                "blocking_issues": blocking_issues,
                "warning_issues": [],
            }
        )
    return checks


def _task_boundary_issues(
    text: str,
    *,
    source_id: str,
    line_number: int,
    artifact_path: Path,
) -> list[dict[str, Any]]:
    lower = text.lower()
    issues: list[dict[str, Any]] = []
    for capability, phrases in FORBIDDEN_CAPABILITIES.items():
        for phrase in phrases:
            phrase_lower = phrase.lower()
            start = lower.find(phrase_lower)
            if start < 0:
                continue
            context = lower[max(0, start - 240) : start + len(phrase_lower) + 240]
            if _has_negation_marker(context):
                continue
            issues.append(
                _issue(
                    issue_id=f"unsafe_task_boundary_{capability}",
                    severity="BLOCKING",
                    scope=source_id,
                    message=(
                        f"Task register line {line_number} mentions {phrase} without a "
                        "nearby safety negation."
                    ),
                    recommended_action="rewrite_task_scope_as_research_only_or_create_blocker",
                    artifact_path=artifact_path,
                    capability=capability,
                    line_number=line_number,
                )
            )
            break
    return issues


def _check_artifact_safety(
    *,
    report: Mapping[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    report_id = _text(report.get("report_id"), "UNKNOWN")
    raw_path = _text(report.get("latest_artifact_path"))
    artifact_path = _resolve_path(raw_path, project_root) if raw_path else None
    check: dict[str, Any] = {
        "report_id": report_id,
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "artifact_read_status": "MISSING",
        "dimension_checks": [],
        "metadata_issues": [],
        "blocking_issues": [],
        "warning_issues": [],
    }
    if artifact_path is None or not artifact_path.exists():
        issue = _issue(
            issue_id="artifact_missing_for_safety_boundary_audit",
            severity="WARNING",
            scope=report_id,
            message="Report index artifact path is missing or unavailable for safety audit.",
            recommended_action="regenerate_report_or_confirm_report_index_pointer",
            artifact_path=artifact_path,
        )
        check["warning_issues"] = [issue]
        return check
    payload, text = _read_artifact(artifact_path)
    check["artifact_read_status"] = "READ"
    for dimension in SAFETY_DIMENSIONS:
        dimension_check = _check_dimension(
            dimension=dimension,
            payload=payload,
            text=text,
            report_id=report_id,
            artifact_path=artifact_path,
        )
        check["dimension_checks"].append(dimension_check)
        check["metadata_issues"].extend(_records(dimension_check.get("metadata_issues")))
        check["blocking_issues"].extend(_records(dimension_check.get("blocking_issues")))
        check["warning_issues"].extend(_records(dimension_check.get("warning_issues")))
    return check


def _check_dimension(
    *,
    dimension: str,
    payload: Mapping[str, Any] | None,
    text: str,
    report_id: str,
    artifact_path: Path,
) -> dict[str, Any]:
    value = _MISSING
    field = ""
    if payload is not None:
        field, value = _find_nested_value(payload, DIMENSION_ALIASES[dimension])
    if value is not _MISSING:
        status, reason = _classify_dimension_value(dimension, field, value)
    else:
        field, status, reason = _classify_dimension_text(dimension, text)
    metadata_issue: dict[str, Any] | None = None
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    if status == FAIL_STATUS:
        metadata_issue = _metadata_issue(report_id, dimension, status, field, reason, artifact_path)
        blocking_issues.append(
            _issue(
                issue_id=f"unsafe_{dimension}",
                severity="BLOCKING",
                scope=report_id,
                message=reason,
                recommended_action="fix_or_block_research_safety_boundary_before_promotion",
                artifact_path=artifact_path,
                dimension=dimension,
                field=field,
            )
        )
    elif status == WARN_STATUS:
        metadata_issue = _metadata_issue(report_id, dimension, status, field, reason, artifact_path)
        warning_issues.append(
            _issue(
                issue_id="missing_safety_metadata",
                severity="WARNING",
                scope=report_id,
                message=reason,
                recommended_action="update_report_template_to_emit_explicit_safety_metadata",
                artifact_path=artifact_path,
                dimension=dimension,
                field=field,
            )
        )
    return {
        "dimension": dimension,
        "status": status,
        "field": field,
        "value": "" if value is _MISSING else _stringify(value),
        "reason": reason,
        "metadata_issues": [] if metadata_issue is None else [metadata_issue],
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
    }


def _classify_dimension_value(dimension: str, field: str, value: Any) -> tuple[str, str]:
    normalized = _normal_value(value)
    field_lower = field.lower()
    if dimension == "production_effect":
        if field_lower == "does_not_modify_production" and value is True:
            return PASS_STATUS, "does_not_modify_production=true."
        if field_lower in {"production_mutation", "production_state_mutated"} and normalized in {
            "false",
            "none",
            "no",
            "not_applicable",
        }:
            return PASS_STATUS, f"{field} confirms no production mutation."
        if normalized == "none":
            return PASS_STATUS, "production_effect=none."
        return FAIL_STATUS, f"{field} is {normalized}, expected production_effect=none."
    if dimension == "manual_review_only":
        if normalized in {"true", "yes", "required", "manual_review_required", "research_only"}:
            return PASS_STATUS, f"{field} confirms manual/research-only use."
        return FAIL_STATUS, f"{field} is {normalized}, expected manual-review-only metadata."
    if dimension == "official_target_weights":
        if field_lower == "not_official_target_weights" and normalized in {"true", "yes"}:
            return PASS_STATUS, "not_official_target_weights=true."
        if normalized in {"false", "none", "no", "not_applicable"}:
            return PASS_STATUS, f"{field} confirms no official target weights."
        return FAIL_STATUS, f"{field} is {normalized}, expected no official target weights."
    if normalized in {"false", "none", "no", "not_applicable"}:
        return PASS_STATUS, f"{field} confirms no {dimension}."
    return FAIL_STATUS, f"{field} is {normalized}, expected no {dimension}."


def _classify_dimension_text(dimension: str, text: str) -> tuple[str, str, str]:
    lower = text.lower()
    unsafe_text = _text_without_safe_negative_aliases(dimension, lower)
    for pattern in UNSAFE_TEXT_PATTERNS[dimension]:
        if pattern in unsafe_text:
            return pattern, FAIL_STATUS, f"Artifact text contains unsafe {dimension}: {pattern}."
    for pattern in SAFE_TEXT_PATTERNS[dimension]:
        if pattern in lower:
            return pattern, PASS_STATUS, f"Artifact text contains safe {dimension}: {pattern}."
    return "", WARN_STATUS, f"Artifact is missing explicit {dimension} safety metadata."


def _text_without_safe_negative_aliases(dimension: str, text: str) -> str:
    if dimension != "official_target_weights":
        return text
    sanitized = text
    for pattern in (
        "not_official_target_weights=true",
        '"not_official_target_weights": true',
    ):
        sanitized = sanitized.replace(pattern, "")
    return sanitized


def _read_artifact(path: Path) -> tuple[Mapping[str, Any] | None, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix.lower() == ".json":
        try:
            payload = json.loads(text)
            if isinstance(payload, Mapping):
                return payload, json.dumps(payload, ensure_ascii=False, sort_keys=True)
        except json.JSONDecodeError:
            return None, text
    return None, text


def _find_nested_value(payload: Mapping[str, Any], aliases: Sequence[str]) -> tuple[str, Any]:
    aliases_lower = {alias.lower(): alias for alias in aliases}
    stack: list[Any] = [payload]
    while stack:
        current = stack.pop()
        if isinstance(current, Mapping):
            for key, value in current.items():
                key_text = str(key).lower()
                if key_text in aliases_lower:
                    return str(key), value
                if isinstance(value, Mapping | list | tuple):
                    stack.append(value)
        elif isinstance(current, list | tuple):
            stack.extend(current)
    return "", _MISSING


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_registers_report_index_and_artifacts_only",
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_rewrite_historical_artifacts": True,
        "does_not_modify_scores_weights_or_gates": True,
        "official_target_weights": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "production_state_mutated": False,
        "automatic_live_allocation": False,
        "production_effect": PRODUCTION_EFFECT,
    }


def _reader_brief(
    status: str,
    summary: Mapping[str, Any],
    blocking_issues: Sequence[Mapping[str, Any]],
    warning_issues: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    return {
        "summary": (
            "Research safety boundary audit checked "
            f"{_int(summary.get('artifact_check_count'))} artifacts and "
            f"{_int(summary.get('task_check_count'))} task rows."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if not blocking_issues
            else f"{len(blocking_issues)} blocking safety issue(s)."
        ),
        "warnings": (
            "none" if not warning_issues else f"{len(warning_issues)} warning safety issue(s)."
        ),
        "safety_boundary": (
            "Read-only safety audit; no historical rewrite, no upstream rerun, no data refresh, "
            "no score/weight/gate mutation, no official target weights, no broker/order, "
            "production_effect=none."
        ),
        "next_action": _next_action(status),
    }


def _next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "fix_safety_boundary_blockers_before_shadow_or_promotion"
    if status == WARN_STATUS:
        return "review_legacy_missing_safety_metadata"
    return "continue_research_safety_boundary_monitoring"


def _validation_next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "fix_research_safety_boundary_validation_blockers"
    if status == WARN_STATUS:
        return "review_research_safety_boundary_warnings"
    return "continue_research_safety_boundary_monitoring"


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
) -> None:
    status = "PASS" if passed else "FAIL"
    checks.append(
        {
            "check_id": check_id,
            "status": status,
            "severity": severity,
            "message": message,
            "recommended_action": "" if passed else recommended_action,
        }
    )
    if not passed:
        issues.append(
            _issue(
                issue_id=check_id,
                severity=severity,
                scope="research_safety_boundary_validation",
                message=message,
                recommended_action=recommended_action,
            )
        )


def _issue(
    *,
    issue_id: str,
    severity: str,
    scope: str,
    message: str,
    recommended_action: str,
    artifact_path: Path | None = None,
    **extra: Any,
) -> dict[str, Any]:
    issue = {
        "issue_id": issue_id,
        "severity": severity,
        "scope": scope,
        "message": message,
        "recommended_action": recommended_action,
    }
    if artifact_path is not None:
        issue["artifact_path"] = str(artifact_path)
    issue.update(extra)
    return issue


def _metadata_issue(
    report_id: str,
    dimension: str,
    status: str,
    field: str,
    reason: str,
    artifact_path: Path,
) -> dict[str, Any]:
    issue_id = "unsafe_safety_metadata" if status == FAIL_STATUS else "missing_safety_metadata"
    return {
        "issue_id": issue_id,
        "report_id": report_id,
        "dimension": dimension,
        "status": status,
        "field": field,
        "message": reason,
        "artifact_path": str(artifact_path),
        "severity": "BLOCKING" if status == FAIL_STATUS else "WARNING",
    }


def _dedupe_issues(items: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in items:
        key = (
            _text(item.get("issue_id")),
            _text(item.get("scope"), _text(item.get("report_id"))),
            _text(item.get("dimension")),
            _text(item.get("artifact_path")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(item))
    return deduped


def _dimension_ids(raw: Any) -> list[str]:
    ids: list[str] = []
    for item in _records(raw):
        ids.append(_text(item.get("dimension")))
    return ids


def _records(raw: Any) -> list[Mapping[str, Any]]:
    if isinstance(raw, Mapping):
        return [raw]
    if isinstance(raw, Sequence) and not isinstance(raw, str | bytes):
        return [item for item in raw if isinstance(item, Mapping)]
    return []


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def _read_json_mapping(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {"status": "MISSING", "reports": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"status": "FAILED", "reports": []}
    return payload if isinstance(payload, Mapping) else {"status": "FAILED", "reports": []}


def _resolve_path(raw_path: str, project_root: Path) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    candidates = sorted(output_dir.glob(f"{prefix}*{suffix}"))
    return candidates[-1] if candidates else None


def _has_negation_marker(text: str) -> bool:
    return any(marker in text for marker in NEGATION_MARKERS)


def _normal_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "none"
    return str(value).strip().lower()


def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _issue_row(issue: Mapping[str, Any]) -> str:
    return (
        f"|{_markdown_cell(issue.get('issue_id'))}|"
        f"{_markdown_cell(issue.get('scope'))}|"
        f"{_markdown_cell(issue.get('message'))}|"
        f"{_markdown_cell(issue.get('recommended_action'))}|"
    )


class _Missing:
    pass


_MISSING = _Missing()
