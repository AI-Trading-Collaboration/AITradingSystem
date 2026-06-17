from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
REPORT_TYPE = "report_quality_gate"
PRODUCTION_EFFECT = "none"
PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

REPORT_SECTIONS: tuple[str, ...] = (
    "purpose",
    "input_artifacts",
    "output_decision",
    "safety_boundary",
    "limitations",
    "next_action",
)
READER_BRIEF_SECTIONS: tuple[str, ...] = (
    "human_readable_summary",
    "key_result",
    "blocking_issues",
    "warnings",
    "safety_boundary",
    "recommended_next_step",
)

JSON_SECTION_KEYS: dict[str, tuple[str, ...]] = {
    "purpose": (
        "purpose",
        "report_purpose",
        "objective",
        "goal",
        "rationale",
        "freshness_rationale",
        "why_this_report",
    ),
    "input_artifacts": (
        "input_artifacts",
        "source_artifacts",
        "source_inputs",
        "upstream_artifacts",
        "sources",
        "inputs",
        "report_index",
    ),
    "output_decision": (
        "output_decision",
        "decision",
        "decision_status",
        "recommended_action",
        "recommended_owner_action",
        "status",
        "report_status",
        "validation_status",
        "quality_status",
    ),
    "safety_boundary": (
        "safety_boundary",
        "safety_status",
        "production_effect",
        "broker_action_allowed",
        "broker_action_taken",
        "not_for_production",
        "not_official_target_weights",
        "research_only",
    ),
    "limitations": (
        "limitations",
        "limitation",
        "known_limitations",
        "warnings",
        "blocking_reasons",
        "missing_limited_artifact_impact",
    ),
    "next_action": (
        "next_action",
        "next_required_action",
        "recommended_next_step",
        "recommended_action",
        "recommended_owner_action",
        "owner_action",
        "action_checklist",
    ),
}

MARKDOWN_SECTION_PATTERNS: dict[str, tuple[str, ...]] = {
    "purpose": ("purpose", "objective", "goal", "目的", "目标", "用途"),
    "input_artifacts": ("input", "source", "artifact", "上游", "输入", "来源"),
    "output_decision": ("decision", "status", "结论", "输出", "状态", "decision_status"),
    "safety_boundary": ("production_effect", "safety", "broker", "安全", "只读", "实盘"),
    "limitations": ("limitation", "limitations", "warning", "风险", "限制", "局限"),
    "next_action": (
        "next_action",
        "next action",
        "next_required_action",
        "owner action",
        "下一步",
        "建议",
    ),
}

CORE_READER_BRIEF_SECTIONS = frozenset(
    {"human_readable_summary", "key_result", "safety_boundary", "recommended_next_step"}
)


def default_report_quality_gate_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_quality_gate_{as_of.isoformat()}.json"


def default_report_quality_gate_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_quality_gate_{as_of.isoformat()}.md"


def build_report_quality_gate_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any],
    report_index_path: Path | None = None,
    reader_brief_payload: Mapping[str, Any] | None = None,
    reader_brief_json_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Build a read-only quality gate report from existing report artifacts."""
    report_checks: list[dict[str, Any]] = []
    missing_sections: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []

    report_index_status = _text(report_index_payload.get("status"), "UNKNOWN")
    if report_index_status in {"FAILED", FAIL_STATUS, "MISSING"}:
        blocking_issues.append(
            _issue(
                issue_id=(
                    "report_index_missing"
                    if report_index_status == "MISSING"
                    else "report_index_failed"
                ),
                severity="BLOCKING",
                scope="report_index",
                message=f"report_index status is {report_index_status}.",
                recommended_action="repair_or_regenerate_report_index_before_quality_gate",
                artifact_path=report_index_path,
            )
        )
    elif report_index_status not in {"PASS", "PASS_WITH_WARNINGS", "PASS_WITH_EXPLICIT_WAIVERS"}:
        warning_issues.append(
            _issue(
                issue_id="report_index_unknown_status",
                severity="WARNING",
                scope="report_index",
                message=f"report_index status is {report_index_status}.",
                recommended_action="review_report_index_status_before_reader_brief_use",
                artifact_path=report_index_path,
            )
        )

    for report in _records(report_index_payload.get("reports")):
        check = _check_report_artifact(
            report=report,
            project_root=project_root,
        )
        report_checks.append(check)
        missing_sections.extend(_records(check.get("missing_sections")))
        blocking_issues.extend(_records(check.get("blocking_quality_issues")))
        warning_issues.extend(_records(check.get("warning_quality_issues")))

    reader_brief_checks = _check_reader_brief(
        reader_brief_payload=reader_brief_payload,
        reader_brief_json_path=reader_brief_json_path,
    )
    for check in reader_brief_checks:
        missing_sections.extend(_records(check.get("missing_sections")))
        blocking_issues.extend(_records(check.get("blocking_quality_issues")))
        warning_issues.extend(_records(check.get("warning_quality_issues")))

    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    missing_sections = _dedupe_issues(missing_sections)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "report_quality_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": (
            "Validate that generated research reports and Reader Briefs disclose the minimum "
            "reader-facing quality sections before downstream interpretation."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            "reader_brief_json": (
                "" if reader_brief_json_path is None else str(reader_brief_json_path)
            ),
        },
        "output_decision": status,
        "safety_boundary": {
            "mode": "read_existing_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_source_reports": True,
            "does_not_modify_production": True,
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "production_effect": PRODUCTION_EFFECT,
        },
        "limitations": [
            (
                "Section checks are structural and conservative; they do not judge report "
                "content quality."
            ),
            (
                "Legacy reports can produce warnings until their templates expose every "
                "required section."
            ),
            "The gate does not repair or regenerate missing sections.",
        ],
        "next_action": _next_action(status),
        "missing_sections": missing_sections,
        "blocking_quality_issues": blocking_issues,
        "warning_quality_issues": warning_issues,
        "summary": {
            "checked_report_count": len(report_checks),
            "checked_reader_brief_count": len(reader_brief_checks),
            "missing_section_count": len(missing_sections),
            "blocking_quality_issue_count": len(blocking_issues),
            "warning_quality_issue_count": len(warning_issues),
            "report_index_status": report_index_status,
        },
        "report_checks": report_checks,
        "reader_brief_checks": reader_brief_checks,
        "methodology": {
            "report_sections": list(REPORT_SECTIONS),
            "reader_brief_sections": list(READER_BRIEF_SECTIONS),
            "missing_report_sections_are_warnings": True,
            "production_effect_risk_is_blocking": True,
            "reader_brief_core_sections_are_blocking": sorted(CORE_READER_BRIEF_SECTIONS),
            "mode": "read_existing_artifacts_only",
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_report_quality_gate_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_report_quality_gate_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_report_quality_gate_markdown(payload), encoding="utf-8")
    return output_path


def render_report_quality_gate_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Report Quality Gate {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('report_quality_status'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- checked reports：{summary.get('checked_report_count')}",
        f"- checked Reader Brief：{summary.get('checked_reader_brief_count')}",
        f"- missing sections：{summary.get('missing_section_count')}",
        f"- blocking issues：{summary.get('blocking_quality_issue_count')}",
        f"- warning issues：{summary.get('warning_quality_issue_count')}",
        f"- next_action：{_text(payload.get('next_action'))}",
        "",
        "## Blocking Quality Issues",
        "",
        "|issue_id|scope|report_id|message|recommended_action|",
        "|---|---|---|---|---|",
    ]
    for issue in _records(payload.get("blocking_quality_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("blocking_quality_issues")):
        lines.append("|NONE|report_quality_gate||无阻断项。||")
    lines.extend(
        [
            "",
            "## Warning Quality Issues",
            "",
            "|issue_id|scope|report_id|message|recommended_action|",
            "|---|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("warning_quality_issues"))[:80]:
        lines.append(_issue_row(issue))
    if not _records(payload.get("warning_quality_issues")):
        lines.append("|NONE|report_quality_gate||无 warning。||")
    lines.extend(
        [
            "",
            "## Missing Sections",
            "",
            "|report_id|section|artifact_path|",
            "|---|---|---|",
        ]
    )
    for item in _records(payload.get("missing_sections"))[:120]:
        lines.append(
            f"|{_markdown_cell(item.get('report_id'))}|"
            f"{_markdown_cell(item.get('section'))}|"
            f"{_markdown_cell(item.get('artifact_path'))}|"
        )
    if not _records(payload.get("missing_sections")):
        lines.append("|NONE|||")
    lines.extend(
        [
            "",
            "## Methodology",
            "",
            "本 gate 只读取 report index、latest report artifacts 和 Reader Brief JSON；"
            "不运行上游命令、不刷新数据、不补造 artifact、不修改 source report "
            "或 production state。",
            "",
        ]
    )
    return "\n".join(lines)


def _check_report_artifact(
    *,
    report: Mapping[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    report_id = _text(report.get("report_id"), "UNKNOWN_REPORT")
    artifact_path = _resolve_path(_text(report.get("latest_artifact_path")), project_root)
    exists = bool(report.get("exists")) and artifact_path is not None and artifact_path.exists()
    if not exists:
        return {
            "report_id": report_id,
            "title": _text(report.get("title")),
            "artifact_path": "" if artifact_path is None else str(artifact_path),
            "artifact_type": "",
            "exists": False,
            "report_quality_status": "SKIPPED_MISSING_ARTIFACT",
            "section_checks": {section: "NOT_CHECKED" for section in REPORT_SECTIONS},
            "missing_sections": [],
            "blocking_quality_issues": [],
            "warning_quality_issues": [],
        }

    warning_issues: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    payload: Mapping[str, Any] | None = None
    text_content = ""
    artifact_type = artifact_path.suffix.lower().lstrip(".") or "unknown"
    try:
        if artifact_path.suffix.lower() == ".json":
            raw = json.loads(artifact_path.read_text(encoding="utf-8"))
            if not isinstance(raw, Mapping):
                raise ValueError("JSON report artifact must be an object")
            payload = raw
        elif artifact_path.suffix.lower() in {".md", ".html", ".htm"}:
            text_content = artifact_path.read_text(encoding="utf-8")
        else:
            warning_issues.append(
                _issue(
                    issue_id=f"{report_id}_unsupported_artifact_type",
                    severity="WARNING",
                    scope="report",
                    report_id=report_id,
                    artifact_path=artifact_path,
                    message=(
                        "Unsupported artifact type for structural section scan: "
                        f"{artifact_type}."
                    ),
                    recommended_action=(
                        "ensure_report_registry_points_to_json_or_markdown_report_artifact"
                    ),
                )
            )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        blocking_issues.append(
            _issue(
                issue_id=f"{report_id}_unreadable_artifact",
                severity="BLOCKING",
                scope="report",
                report_id=report_id,
                artifact_path=artifact_path,
                message=f"Report artifact cannot be read for quality validation: {exc}",
                recommended_action="repair_or_regenerate_report_artifact_before_reader_use",
            )
        )

    production_effect = _text(report.get("artifact_production_effect"))
    if payload is not None:
        production_effect = _text(payload.get("production_effect"), production_effect)
    if production_effect and production_effect not in {"none", "read_only", "advisory"}:
        blocking_issues.append(
            _issue(
                issue_id=f"{report_id}_production_effect_risk",
                severity="BLOCKING",
                scope="report",
                report_id=report_id,
                artifact_path=artifact_path,
                message=f"Report artifact production_effect is {production_effect}.",
                recommended_action="remove_or_reclassify_production_effect_before_quality_gate_pass",
            )
        )

    section_checks = {
        section: _section_present(section, payload=payload, text_content=text_content)
        for section in REPORT_SECTIONS
    }
    missing_sections = [
        _missing_section_issue(
            report_id=report_id,
            artifact_path=artifact_path,
            section=section,
            scope="report",
            blocking=False,
        )
        for section, present in section_checks.items()
        if not present
    ]
    warning_issues.extend(missing_sections)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    return {
        "report_id": report_id,
        "title": _text(report.get("title")),
        "artifact_path": str(artifact_path),
        "artifact_type": artifact_type,
        "exists": True,
        "freshness_status": _text(report.get("freshness_status"), "UNKNOWN"),
        "source_artifact_status": _text(report.get("artifact_status"), "UNKNOWN"),
        "source_artifact_production_effect": production_effect,
        "report_quality_status": status,
        "section_checks": {
            section: "PASS" if present else "MISSING"
            for section, present in section_checks.items()
        },
        "missing_sections": missing_sections,
        "blocking_quality_issues": blocking_issues,
        "warning_quality_issues": warning_issues,
    }


def _check_reader_brief(
    *,
    reader_brief_payload: Mapping[str, Any] | None,
    reader_brief_json_path: Path | None,
) -> list[dict[str, Any]]:
    if not reader_brief_payload:
        issue = _issue(
            issue_id="reader_brief_missing",
            severity="BLOCKING",
            scope="reader_brief",
            report_id="reader_brief",
            artifact_path=reader_brief_json_path,
            message="Reader Brief JSON is missing; cannot validate reader-facing summary.",
            recommended_action="generate_reader_brief_before_report_quality_gate",
        )
        return [
            {
                "report_id": "reader_brief",
                "artifact_path": (
                    "" if reader_brief_json_path is None else str(reader_brief_json_path)
                ),
                "report_quality_status": FAIL_STATUS,
                "section_checks": {section: "MISSING" for section in READER_BRIEF_SECTIONS},
                "missing_sections": [
                    _missing_section_issue(
                        report_id="reader_brief",
                        artifact_path=reader_brief_json_path,
                        section=section,
                        scope="reader_brief",
                        blocking=section in CORE_READER_BRIEF_SECTIONS,
                    )
                    for section in READER_BRIEF_SECTIONS
                ],
                "blocking_quality_issues": [issue],
                "warning_quality_issues": [],
            }
        ]

    checks = {
        "human_readable_summary": _reader_brief_has_summary(reader_brief_payload),
        "key_result": _reader_brief_has_key_result(reader_brief_payload),
        "blocking_issues": _reader_brief_has_blocking_issue_disclosure(reader_brief_payload),
        "warnings": "warnings" in reader_brief_payload,
        "safety_boundary": _reader_brief_has_safety_boundary(reader_brief_payload),
        "recommended_next_step": _reader_brief_has_next_step(reader_brief_payload),
    }
    missing_sections = [
        _missing_section_issue(
            report_id="reader_brief",
            artifact_path=reader_brief_json_path,
            section=section,
            scope="reader_brief",
            blocking=section in CORE_READER_BRIEF_SECTIONS,
        )
        for section, present in checks.items()
        if not present
    ]
    blocking_issues = [
        issue
        for issue in missing_sections
        if _text(issue.get("severity")) == "BLOCKING"
    ]
    warning_issues = [
        issue
        for issue in missing_sections
        if _text(issue.get("severity")) != "BLOCKING"
    ]
    production_effect = _text(reader_brief_payload.get("production_effect"))
    if production_effect != PRODUCTION_EFFECT:
        blocking_issues.append(
            _issue(
                issue_id="reader_brief_production_effect_risk",
                severity="BLOCKING",
                scope="reader_brief",
                report_id="reader_brief",
                artifact_path=reader_brief_json_path,
                message=f"Reader Brief production_effect is {production_effect or 'UNKNOWN'}.",
                recommended_action="regenerate_reader_brief_with_production_effect_none",
            )
        )
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    return [
        {
            "report_id": "reader_brief",
            "artifact_path": "" if reader_brief_json_path is None else str(reader_brief_json_path),
            "source_reader_brief_status": _text(reader_brief_payload.get("status"), "UNKNOWN"),
            "report_quality_status": status,
            "section_checks": {
                section: "PASS" if present else "MISSING" for section, present in checks.items()
            },
            "missing_sections": missing_sections,
            "blocking_quality_issues": blocking_issues,
            "warning_quality_issues": warning_issues,
        }
    ]


def _section_present(
    section: str,
    *,
    payload: Mapping[str, Any] | None,
    text_content: str,
) -> bool:
    if payload is not None:
        return _has_any_key(payload, JSON_SECTION_KEYS[section])
    if not text_content:
        return False
    lower = text_content.lower()
    return any(pattern.lower() in lower for pattern in MARKDOWN_SECTION_PATTERNS[section])


def _has_any_key(value: Any, aliases: Sequence[str]) -> bool:
    normalized_aliases = {_normalize_key(alias) for alias in aliases}
    if isinstance(value, Mapping):
        for key, nested in value.items():
            key_text = _normalize_key(_text(key))
            if key_text in normalized_aliases and _non_empty(nested):
                return True
            if _has_any_key(nested, aliases):
                return True
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_has_any_key(item, aliases) for item in value)
    return False


def _reader_brief_has_summary(payload: Mapping[str, Any]) -> bool:
    summary = _mapping(payload.get("narrative_executive_summary"))
    return any(_text(value) for value in summary.values())


def _reader_brief_has_key_result(payload: Mapping[str, Any]) -> bool:
    status_panel = _mapping(payload.get("status_panel"))
    decision = _mapping(payload.get("executive_decision"))
    return any(
        _text(status_panel.get(key))
        for key in ("build_status", "decision_usability", "research_promotion_status")
    ) or any(_text(decision.get(key)) for key in ("action", "decision", "status"))


def _reader_brief_has_blocking_issue_disclosure(payload: Mapping[str, Any]) -> bool:
    impact = _mapping(payload.get("missing_limited_artifact_impact"))
    queue = _mapping(payload.get("manual_review_queue"))
    return bool(impact) and (
        "blocking_count" in impact
        or _text(impact.get("status")) == "OK"
        or bool(_records(impact.get("items")))
        or bool(_records(queue.get("items")))
    )


def _reader_brief_has_safety_boundary(payload: Mapping[str, Any]) -> bool:
    decision = _mapping(payload.get("executive_decision"))
    return _text(payload.get("production_effect")) == PRODUCTION_EFFECT and (
        decision.get("not_trade_instruction") is True
        or _text(payload.get("production_effect_statement"))
        or _text(decision.get("production_effect"))
    )


def _reader_brief_has_next_step(payload: Mapping[str, Any]) -> bool:
    actions = _records(payload.get("action_checklist"))
    if actions:
        return True
    queue = _mapping(payload.get("manual_review_queue"))
    return any(_text(item.get("recommended_next_action")) for item in _records(queue.get("items")))


def _missing_section_issue(
    *,
    report_id: str,
    artifact_path: Path | None,
    section: str,
    scope: str,
    blocking: bool,
) -> dict[str, Any]:
    severity = "BLOCKING" if blocking else "WARNING"
    return _issue(
        issue_id=f"{report_id}_missing_{section}",
        severity=severity,
        scope=scope,
        report_id=report_id,
        artifact_path=artifact_path,
        section=section,
        message=f"{report_id} is missing required section: {section}.",
        recommended_action=f"add_or_expose_{section}_before_next_report_quality_review",
    )


def _issue(
    *,
    issue_id: str,
    severity: str,
    scope: str,
    message: str,
    recommended_action: str,
    report_id: str = "",
    artifact_path: Path | None = None,
    section: str = "",
) -> dict[str, Any]:
    return {
        "issue_id": issue_id,
        "severity": severity,
        "scope": scope,
        "report_id": report_id,
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "section": section,
        "message": message,
        "recommended_action": recommended_action,
    }


def _dedupe_issues(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for record in records:
        key = (
            _text(record.get("issue_id")),
            _text(record.get("report_id")),
            _text(record.get("artifact_path")),
            _text(record.get("section")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped


def _resolve_path(raw: str, project_root: Path) -> Path | None:
    if not raw:
        return None
    path = Path(raw)
    if path.is_absolute():
        return path
    return project_root / path


def _non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping | Sequence) and not isinstance(value, (bytes, bytearray)):
        return bool(value)
    return True


def _next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "resolve_blocking_quality_issues_before_using_reports_as_complete_reader_input"
    if status == WARN_STATUS:
        return "review_warning_quality_issues_and_migrate_legacy_reports_to_required_sections"
    return "continue_daily_report_generation_and_quality_monitoring"


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _issue_row(issue: Mapping[str, Any]) -> str:
    return (
        f"|{_markdown_cell(issue.get('issue_id'))}|"
        f"{_markdown_cell(issue.get('scope'))}|"
        f"{_markdown_cell(issue.get('report_id'))}|"
        f"{_markdown_cell(issue.get('message'))}|"
        f"{_markdown_cell(issue.get('recommended_action'))}|"
    )
