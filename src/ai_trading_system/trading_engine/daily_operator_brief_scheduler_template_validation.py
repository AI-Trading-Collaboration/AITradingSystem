from __future__ import annotations

import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "daily_operator_brief_scheduler_template_validation"
TASK_ID = "TRADING-029"
INPUT_TASK_ID = "TRADING-028"
MODE = "daily_operator_brief_scheduler_template_validation_only"
PRODUCTION_EFFECT_NONE = "none"

STATUS_PASS = "PASS"
STATUS_PASS_WITH_WARNINGS = "PASS_WITH_WARNINGS"
STATUS_FAIL = "FAIL"
STATUS_SAFETY_BLOCKED = "SAFETY_BLOCKED"
STATUS_INPUT_MISSING = "INPUT_MISSING"
STATUS_INPUT_INVALID = "INPUT_INVALID"
STATUS_ERROR = "ERROR"

TEMPLATE_STATUS_PASS = "PASS"
TEMPLATE_STATUS_WARNING = "WARNING"
TEMPLATE_STATUS_FAIL = "FAIL"
TEMPLATE_STATUS_SAFETY_BLOCKED = "SAFETY_BLOCKED"
TEMPLATE_STATUS_MISSING = "MISSING"
TEMPLATE_STATUS_ERROR = "ERROR"

SUMMARY_NORMAL = "NORMAL"
SUMMARY_WATCH = "WATCH"
SUMMARY_ACTION_REQUIRED = "ACTION_REQUIRED"
SUMMARY_SAFETY_BLOCKED = "SAFETY_BLOCKED"
SUMMARY_ERROR = "ERROR"

REPO_ROOT = Path(__file__).resolve().parents[3]

TEMPLATE_LABELS = {
    "windows_task_xml": "Windows Task XML",
    "powershell_wrapper": "PowerShell Wrapper",
    "batch_wrapper": "Batch Wrapper",
    "cron_line": "cron Line",
    "github_actions_workflow": "GitHub Actions Workflow",
}

VALIDATION_TRUE_FIELDS = (
    "manual_review_only",
    "scheduler_template_validation_only",
    "read_only",
    "safe_for_scheduler",
)
VALIDATION_FALSE_FIELDS = (
    "scheduler_created",
    "scheduler_installed",
    "scheduler_enabled",
    "templates_executed_by_validator",
    "operator_brief_executed_by_validator",
    "pipelines_executed_by_validator",
    "data_downloaded_by_validator",
    "apply_executed_by_validator",
    "rollback_executed_by_validator",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)

METADATA_TRUE_FIELDS = (
    "manual_review_only",
    "scheduler_template_only",
    "read_only",
    "safe_for_scheduler",
)
METADATA_FALSE_FIELDS = (
    "scheduler_created",
    "scheduler_installed",
    "scheduler_enabled",
    "operator_brief_executed_by_template_generator",
    "pipelines_executed_by_template_generator",
    "data_downloaded_by_template_generator",
    "apply_executed_by_template_generator",
    "rollback_executed_by_template_generator",
    "broker_execution",
    "replay_execution",
    "trading_execution",
)

ALLOWED_SCRIPTS = (
    "scripts/run_daily_operator_brief_scheduler_dry_run.py",
    "scripts/run_daily_trading_system_operator_brief.py",
)
ALLOWED_EXTERNAL_COMMANDS = {
    "python",
    "python3",
    "powershell",
    "powershell.exe",
    "pwsh",
    "cd",
    "set-location",
    "get-date",
    "write-output",
    "write-error",
    "echo",
    "exit",
}
SHELL_STRUCTURAL_COMMANDS = {
    "&",
    "@echo",
    "for",
    "if",
    "set",
    "mkdir",
    "new-item",
    "join-path",
    "out-null",
    "rem",
}

DANGEROUS_TEXT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "Template references shadow promotion apply script.",
        re.compile(r"scripts[/\\]run_shadow_promotion_apply\.py", re.IGNORECASE),
    ),
    (
        "Template references shadow promotion rollback script.",
        re.compile(r"scripts[/\\]run_shadow_promotion_rollback\.py", re.IGNORECASE),
    ),
    (
        "Template references run_shadow_promotion_apply.",
        re.compile(r"\brun_shadow_promotion_apply\b", re.IGNORECASE),
    ),
    (
        "Template references run_shadow_promotion_rollback.",
        re.compile(r"\brun_shadow_promotion_rollback\b", re.IGNORECASE),
    ),
    (
        "Template contains schtasks /Create.",
        re.compile(r"\bschtasks(?:\.exe)?\s+/create\b", re.IGNORECASE),
    ),
    (
        "Template contains crontab installation command.",
        re.compile(r"\bcrontab\s+-(?:e\b|[^#\r\n]*)", re.IGNORECASE),
    ),
    (
        "Template contains broker execution command.",
        re.compile(r"\bbroker\s+execution\b", re.IGNORECASE),
    ),
    (
        "Template contains replay runner command.",
        re.compile(r"\breplay\s+runner\b", re.IGNORECASE),
    ),
    (
        "Template contains trading execution command.",
        re.compile(r"\btrading\s+execution\b", re.IGNORECASE),
    ),
)

DANGEROUS_ACTIVE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "Active command attempts to write GitHub Actions workflow.",
        re.compile(r"(?:^|\s|[\"'])\.github[/\\]workflows[/\\]", re.IGNORECASE),
    ),
    (
        "Active command references Task Scheduler install path.",
        re.compile(r"windows[/\\]system32[/\\]tasks", re.IGNORECASE),
    ),
    (
        "Active command references system cron path.",
        re.compile(r"(?:/etc/cron\.d|/var/spool/cron)", re.IGNORECASE),
    ),
    (
        "Active command references broker.",
        re.compile(r"\bbroker\b", re.IGNORECASE),
    ),
    (
        "Active command references replay.",
        re.compile(r"\breplay\b", re.IGNORECASE),
    ),
    (
        "Active command references apply.",
        re.compile(r"\bapply\b", re.IGNORECASE),
    ),
    (
        "Active command references rollback.",
        re.compile(r"\brollback\b", re.IGNORECASE),
    ),
)

DANGEROUS_PATH_MARKERS = (
    ".github/workflows",
    ".github\\workflows",
    "windows/system32/tasks",
    "windows\\system32\\tasks",
    "/etc/cron.d",
    "\\etc\\cron.d",
    "/var/spool/cron",
    "\\var\\spool\\cron",
    "crontab",
)

PLACEHOLDER_PATTERNS = (
    re.compile(r"c:[/\\]path[/\\]to[/\\]aitradingsystem", re.IGNORECASE),
    re.compile(r"[/\\]path[/\\]to[/\\]aitradingsystem", re.IGNORECASE),
    re.compile(r"\bpath[/\\]to\b", re.IGNORECASE),
)

SCRIPT_PATH_RE = re.compile(r"scripts[/\\][A-Za-z0-9_./\\-]+\.py", re.IGNORECASE)
CRON_FIELD_RE = re.compile(r"^[\d*/,\-?]+$")


def default_data_root(repo_root: Path = REPO_ROOT) -> Path:
    return repo_root / "data"


def default_templates_root(repo_root: Path = REPO_ROOT, data_root: Path | None = None) -> Path:
    root = data_root or default_data_root(repo_root)
    return root / "derived" / "operator_briefs" / "scheduler_templates"


def default_validation_root(repo_root: Path = REPO_ROOT, data_root: Path | None = None) -> Path:
    root = data_root or default_data_root(repo_root)
    return root / "derived" / "operator_briefs" / "scheduler_template_validation"


def write_daily_operator_brief_scheduler_template_validation(
    *,
    as_of: date,
    repo_root: Path = REPO_ROOT,
    data_root: Path | str = "data",
    template_metadata_file: Path | str | None = None,
    templates_root: Path | str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    started_at = generated_at or datetime.now(tz=UTC)
    try:
        payload = build_daily_operator_brief_scheduler_template_validation_payload(
            as_of=as_of,
            repo_root=repo_root,
            data_root=data_root,
            template_metadata_file=template_metadata_file,
            templates_root=templates_root,
            generated_at=started_at,
        )
    except Exception as exc:  # pragma: no cover - defensive output path.
        resolved_repo_root = _resolved(repo_root)
        resolved_data_root = _resolve_path(resolved_repo_root, data_root)
        validation_root = default_validation_root(resolved_repo_root, resolved_data_root)
        metadata_path = _resolve_optional_path(
            resolved_repo_root,
            template_metadata_file,
        ) or _default_template_metadata_path(
            default_templates_root(resolved_repo_root, resolved_data_root),
            as_of,
        )
        payload = _base_payload(
            as_of=as_of,
            repo_root=resolved_repo_root,
            validation_root=validation_root,
            metadata_path=metadata_path,
            validation_status=STATUS_ERROR,
            generated_at=started_at,
            input_artifact=_input_artifact_record(
                metadata_path,
                resolved_repo_root,
                status="ERROR",
            ),
            headline="Scheduler template validation failed with an unexpected error.",
        )
        payload["alerts"]["critical"].append(str(exc))
        payload["safety_validation"]["status"] = STATUS_FAIL
        payload["safety_validation"]["blocking_reasons"].append(str(exc))

    validation_path = _validation_json_path(
        default_validation_root(
            _resolved(repo_root),
            _resolve_path(_resolved(repo_root), data_root),
        ),
        as_of,
    )
    markdown_path = validation_path.with_suffix(".md")
    run_log_path = _run_log_json_path(validation_path.parent, as_of)
    run_log_markdown_path = run_log_path.with_suffix(".md")

    payload["output_artifacts"] = {
        "validation_json": {"path": _display_path(validation_path, _resolved(repo_root))},
        "validation_markdown": {"path": _display_path(markdown_path, _resolved(repo_root))},
        "run_log_json": {"path": _display_path(run_log_path, _resolved(repo_root))},
        "run_log_markdown": {"path": _display_path(run_log_markdown_path, _resolved(repo_root))},
    }
    _assert_validation_safety_invariants(payload)

    _write_json(validation_path, payload)
    _write_text(markdown_path, render_scheduler_template_validation_markdown(payload))
    completed_at = datetime.now(tz=UTC) if generated_at is None else generated_at
    run_log = _run_log_payload(
        payload=payload,
        started_at=started_at,
        completed_at=completed_at,
    )
    _write_json(run_log_path, run_log)
    _write_text(
        run_log_markdown_path,
        render_scheduler_template_validation_run_log_markdown(run_log),
    )
    return payload


def build_daily_operator_brief_scheduler_template_validation_payload(
    *,
    as_of: date,
    repo_root: Path = REPO_ROOT,
    data_root: Path | str = "data",
    template_metadata_file: Path | str | None = None,
    templates_root: Path | str | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_repo_root = _resolved(repo_root)
    resolved_data_root = _resolve_path(resolved_repo_root, data_root)
    resolved_templates_root = (
        _resolve_path(resolved_repo_root, templates_root)
        if templates_root is not None
        else default_templates_root(resolved_repo_root, resolved_data_root)
    )
    validation_root = default_validation_root(resolved_repo_root, resolved_data_root)
    metadata_path = (
        _resolve_path(resolved_repo_root, template_metadata_file)
        if template_metadata_file is not None
        else _latest_template_metadata_path(resolved_templates_root, as_of)
    )
    input_artifact = _input_artifact_record(metadata_path, resolved_repo_root)

    if not metadata_path.exists():
        payload = _base_payload(
            as_of=as_of,
            repo_root=resolved_repo_root,
            validation_root=validation_root,
            metadata_path=metadata_path,
            validation_status=STATUS_INPUT_MISSING,
            generated_at=generated,
            input_artifact=input_artifact,
            headline="TRADING-028 scheduler template metadata is missing.",
        )
        payload["alerts"]["critical"].append("TRADING-028 template metadata was not found.")
        _assert_validation_safety_invariants(payload)
        return payload

    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        payload = _base_payload(
            as_of=as_of,
            repo_root=resolved_repo_root,
            validation_root=validation_root,
            metadata_path=metadata_path,
            validation_status=STATUS_INPUT_INVALID,
            generated_at=generated,
            input_artifact=input_artifact,
            headline="TRADING-028 scheduler template metadata is invalid JSON.",
        )
        payload["alerts"]["critical"].append(f"Metadata JSON parse failed: {exc}")
        _assert_validation_safety_invariants(payload)
        return payload

    if not isinstance(metadata, dict):
        payload = _base_payload(
            as_of=as_of,
            repo_root=resolved_repo_root,
            validation_root=validation_root,
            metadata_path=metadata_path,
            validation_status=STATUS_INPUT_INVALID,
            generated_at=generated,
            input_artifact=input_artifact,
            headline="TRADING-028 scheduler template metadata is not a JSON object.",
        )
        payload["alerts"]["critical"].append("Metadata root must be a JSON object.")
        _assert_validation_safety_invariants(payload)
        return payload

    if metadata.get("task_id") != INPUT_TASK_ID:
        payload = _base_payload(
            as_of=as_of,
            repo_root=resolved_repo_root,
            validation_root=validation_root,
            metadata_path=metadata_path,
            validation_status=STATUS_INPUT_INVALID,
            generated_at=generated,
            input_artifact=input_artifact,
            headline="Metadata task_id is not TRADING-028.",
        )
        payload["alerts"]["critical"].append(
            f"Expected metadata task_id={INPUT_TASK_ID}, got {metadata.get('task_id')!r}."
        )
        _assert_validation_safety_invariants(payload)
        return payload

    metadata_blocking_reasons = _metadata_safety_blocking_reasons(metadata)
    template_records = _template_records_from_metadata(metadata)
    template_results = [
        _validate_template_record(
            record=record,
            repo_root=resolved_repo_root,
            templates_root=resolved_templates_root,
        )
        for record in template_records
    ]
    coverage = _coverage(template_results, declared_count=len(template_records))
    warnings = _template_warnings(template_results)
    critical = _template_critical_findings(template_results)
    if metadata_blocking_reasons:
        critical.extend(metadata_blocking_reasons)

    validation_status = _aggregate_validation_status(
        metadata_blocking_reasons=metadata_blocking_reasons,
        template_results=template_results,
    )
    payload = _base_payload(
        as_of=as_of,
        repo_root=resolved_repo_root,
        validation_root=validation_root,
        metadata_path=metadata_path,
        validation_status=validation_status,
        generated_at=generated,
        input_artifact=input_artifact,
        headline=_headline(validation_status),
    )
    payload["template_results"] = template_results
    payload["coverage"] = coverage
    payload["safety_validation"] = {
        "status": STATUS_PASS if validation_status not in {STATUS_SAFETY_BLOCKED} else STATUS_FAIL,
        "metadata_safe": not metadata_blocking_reasons,
        "templates_only": True,
        "no_scheduler_created": True,
        "no_scheduler_installed": True,
        "no_scheduler_enabled": True,
        "no_template_execution": True,
        "no_operator_brief_execution": True,
        "no_pipeline_execution": True,
        "no_data_download": True,
        "no_apply_or_rollback": True,
        "no_broker_replay_trading": True,
        "blocking_reasons": list(dict.fromkeys(metadata_blocking_reasons + critical)),
    }
    payload["alerts"] = {
        "critical": list(dict.fromkeys(critical)),
        "warnings": list(dict.fromkeys(warnings)),
        "notes": [
            "Validation is static and does not install or run any scheduler.",
        ],
    }
    payload["metadata_validation"] = {
        "status": STATUS_PASS if not metadata_blocking_reasons else STATUS_SAFETY_BLOCKED,
        "task_id": metadata.get("task_id"),
        "metadata_safe": not metadata_blocking_reasons,
        "blocking_reasons": metadata_blocking_reasons,
    }
    _assert_validation_safety_invariants(payload)
    return payload


def render_scheduler_template_validation_markdown(payload: dict[str, Any]) -> str:
    status = str(payload.get("validation_status") or "")
    banner = {
        STATUS_SAFETY_BLOCKED: "## Scheduler Template Validation Safety Blocked",
        STATUS_FAIL: "## Scheduler Template Validation Failed",
        STATUS_PASS_WITH_WARNINGS: "## Scheduler Template Validation Passed With Warnings",
        STATUS_INPUT_MISSING: "## Scheduler Template Validation Input Missing",
        STATUS_INPUT_INVALID: "## Scheduler Template Validation Input Invalid",
        STATUS_ERROR: "## Scheduler Template Validation Error",
    }.get(status, "")
    lines = [f"# Scheduler Template Validation Report - {payload.get('date')}", ""]
    if banner:
        lines.extend([banner, ""])
    coverage = _mapping(payload.get("coverage"))
    alerts = _mapping(payload.get("alerts"))
    lines.extend(
        [
            "## 1. Validation Summary",
            "",
            f"- Validation Status: `{payload.get('validation_status')}`",
            f"- Summary Level: `{payload.get('summary_level')}`",
            f"- Scheduler Created: `{_bool_text(payload.get('scheduler_created'))}`",
            f"- Scheduler Installed: `{_bool_text(payload.get('scheduler_installed'))}`",
            f"- Scheduler Enabled: `{_bool_text(payload.get('scheduler_enabled'))}`",
            "- Templates Executed By Validator: "
            f"`{_bool_text(payload.get('templates_executed_by_validator'))}`",
            "- Manual Review Required: "
            f"`{_bool_text(_mapping(payload.get('manual_review_required')).get('required'))}`",
            f"- Templates Declared: `{coverage.get('templates_declared', 0)}`",
            f"- Templates Found: `{coverage.get('templates_found', 0)}`",
            f"- Templates Passed: `{coverage.get('templates_passed', 0)}`",
            "",
            "## 2. Template Results",
            "",
            "| Template | Status | Syntax | Allowlist | Danger Scan | Path |",
            "|---|---:|---:|---:|---:|---|",
        ]
    )
    for result in _records(payload.get("template_results")):
        label = TEMPLATE_LABELS.get(str(result.get("template_id")), str(result.get("template_id")))
        lines.append(
            "| "
            f"{label} | `{result.get('status')}` | `{result.get('syntax_status')}` | "
            f"`{result.get('command_allowlist_status')}` | "
            f"`{result.get('dangerous_command_scan_status')}` | "
            f"`{result.get('path')}` |"
        )
    critical = _strings(alerts.get("critical"))
    warnings = _strings(alerts.get("warnings"))
    lines.extend(
        [
            "",
            "## 3. Critical Findings",
            "",
            *_markdown_bullets(critical),
            "",
            "## 4. Warnings",
            "",
            *_markdown_bullets(warnings),
            "",
            "## 5. Safety Statement",
            "",
            "TRADING-029 performs static validation only.",
            "",
            "It does not:",
            "- install scheduler",
            "- enable scheduler",
            "- run templates",
            "- run operator brief",
            "- run apply / rollback",
            "- run broker / replay / trading",
            "",
            "## 6. Recommended Next Steps",
            "",
            *_markdown_bullets(_strings(payload.get("recommended_next_steps"))),
            "",
        ]
    )
    return "\n".join(lines)


def render_scheduler_template_validation_run_log_markdown(payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Scheduler Template Validation Run Log - {payload.get('date')}",
            "",
            f"- Validation Status: `{payload.get('validation_status')}`",
            f"- Started At: `{payload.get('started_at')}`",
            f"- Completed At: `{payload.get('completed_at')}`",
            f"- production_effect: `{payload.get('production_effect')}`",
            f"- read_only: `{_bool_text(payload.get('read_only'))}`",
            f"- scheduler_created: `{_bool_text(payload.get('scheduler_created'))}`",
            f"- scheduler_installed: `{_bool_text(payload.get('scheduler_installed'))}`",
            f"- scheduler_enabled: `{_bool_text(payload.get('scheduler_enabled'))}`",
            "- templates_executed_by_validator: "
            f"`{_bool_text(payload.get('templates_executed_by_validator'))}`",
            "",
        ]
    )


def should_fail_cli(
    payload: dict[str, Any],
    *,
    fail_on_warning: bool = False,
    fail_on_critical: bool = False,
) -> bool:
    status = payload.get("validation_status")
    if fail_on_critical and status in {
        STATUS_FAIL,
        STATUS_SAFETY_BLOCKED,
        STATUS_INPUT_MISSING,
        STATUS_INPUT_INVALID,
        STATUS_ERROR,
    }:
        return True
    if fail_on_warning and status == STATUS_PASS_WITH_WARNINGS:
        return True
    return False


def _validate_template_record(
    *,
    record: dict[str, Any],
    repo_root: Path,
    templates_root: Path,
) -> dict[str, Any]:
    template_id = str(record.get("template_id") or record.get("key") or "unknown_template")
    raw_path = str(record.get("path") or "")
    template_path = _resolve_path(repo_root, raw_path) if raw_path else repo_root
    template_type = _template_type(template_id, template_path)
    result = {
        "template_id": template_id,
        "template_type": template_type,
        "path": _display_path(template_path, repo_root) if raw_path else "",
        "status": TEMPLATE_STATUS_PASS,
        "exists": template_path.exists() if raw_path else False,
        "suffix_valid": raw_path.endswith(".template"),
        "path_safe": _path_is_safe(template_path, templates_root),
        "syntax_status": STATUS_PASS,
        "command_allowlist_status": STATUS_PASS,
        "dangerous_command_scan_status": STATUS_PASS,
        "manual_review_required": True,
        "blocking_reasons": [],
        "warnings": [],
        "notes": [],
    }
    blocking: list[str] = []
    warnings: list[str] = []
    notes: list[str] = []
    if not raw_path:
        blocking.append("Template path is missing in metadata.")
        result["status"] = TEMPLATE_STATUS_FAIL
    if not result["path_safe"]:
        blocking.append("Template path is outside scheduler_templates root or is dangerous.")
        result["status"] = TEMPLATE_STATUS_SAFETY_BLOCKED
    if not result["suffix_valid"]:
        blocking.append("Template file name must end with .template.")
        result["status"] = TEMPLATE_STATUS_FAIL
    if not result["exists"]:
        blocking.append("Template file was declared but not found.")
        result["status"] = TEMPLATE_STATUS_MISSING
        result["syntax_status"] = "MISSING"
        result["command_allowlist_status"] = "MISSING"
        result["dangerous_command_scan_status"] = "MISSING"
    elif result["path_safe"] and result["suffix_valid"]:
        try:
            text = template_path.read_text(encoding="utf-8")
            text_scan = _scan_template_text(template_id=template_id, text=text)
            syntax = _syntax_check(template_type=template_type, text=text)
            allowlist = _command_allowlist_check(template_type=template_type, text=text)
            blocking.extend(text_scan["blocking_reasons"])
            warnings.extend(text_scan["warnings"])
            notes.extend(text_scan["notes"])
            blocking.extend(syntax["blocking_reasons"])
            warnings.extend(syntax["warnings"])
            blocking.extend(allowlist["blocking_reasons"])
            warnings.extend(allowlist["warnings"])
            result["syntax_status"] = syntax["status"]
            result["command_allowlist_status"] = allowlist["status"]
            result["dangerous_command_scan_status"] = text_scan["status"]
        except Exception as exc:  # pragma: no cover - defensive per-template path.
            blocking.append(f"Template validation error: {exc}")
            result["status"] = TEMPLATE_STATUS_ERROR
            result["syntax_status"] = STATUS_ERROR
            result["command_allowlist_status"] = STATUS_ERROR
            result["dangerous_command_scan_status"] = STATUS_ERROR

    if (
        result["dangerous_command_scan_status"] == STATUS_SAFETY_BLOCKED
        or result["path_safe"] is False
    ):
        result["status"] = TEMPLATE_STATUS_SAFETY_BLOCKED
    elif result["status"] == TEMPLATE_STATUS_PASS:
        if blocking:
            result["status"] = TEMPLATE_STATUS_FAIL
        elif warnings:
            result["status"] = TEMPLATE_STATUS_WARNING
    result["blocking_reasons"] = list(dict.fromkeys(blocking))
    result["warnings"] = list(dict.fromkeys(warnings))
    result["notes"] = list(dict.fromkeys(notes))
    return result


def _scan_template_text(*, template_id: str, text: str) -> dict[str, Any]:
    blocking: list[str] = []
    warnings: list[str] = []
    notes: list[str] = []
    for message, pattern in DANGEROUS_TEXT_PATTERNS:
        if pattern.search(text):
            blocking.append(f"{template_id}: {message}")
    active_lines = _active_template_lines(text)
    for line in active_lines:
        allowed_operator_line = "run_daily_trading_system_operator_brief.py" in line
        for message, pattern in DANGEROUS_ACTIVE_PATTERNS:
            if not pattern.search(line):
                continue
            if "trading" in message.lower() and allowed_operator_line:
                continue
            blocking.append(f"{template_id}: {message} Line: {line}")
    for line in text.splitlines():
        _classify_risk_words(line=line, warnings=warnings, notes=notes)
    for pattern in PLACEHOLDER_PATTERNS:
        if pattern.search(text):
            warnings.append(f"{template_id}: placeholder path detected; manual review required.")
            break
    return {
        "status": STATUS_PASS if not blocking else STATUS_SAFETY_BLOCKED,
        "blocking_reasons": list(dict.fromkeys(blocking)),
        "warnings": list(dict.fromkeys(warnings)),
        "notes": list(dict.fromkeys(notes)),
    }


def _syntax_check(*, template_type: str, text: str) -> dict[str, Any]:
    if template_type == "windows_task_xml":
        return _check_xml_template(text)
    if template_type == "github_actions_workflow":
        return _check_github_actions_template(text)
    if template_type == "cron_line":
        return _check_cron_template(text)
    if template_type in {"powershell_wrapper", "batch_wrapper"}:
        return _check_script_template(text)
    return {"status": STATUS_PASS, "blocking_reasons": [], "warnings": []}


def _command_allowlist_check(*, template_type: str, text: str) -> dict[str, Any]:
    blocking: list[str] = []
    warnings: list[str] = []
    active_lines = _active_template_lines(text)
    if template_type == "github_actions_workflow":
        active_lines = _github_actions_run_commands(text)
    for line in active_lines:
        script_paths = _script_paths(line)
        for script_path in script_paths:
            if script_path not in ALLOWED_SCRIPTS:
                blocking.append(f"Disallowed script path in template command: {script_path}")
        if script_paths and set(script_paths).issubset(set(ALLOWED_SCRIPTS)):
            continue
        if template_type == "github_actions_workflow" and "run:" not in line:
            continue
        command = _first_command_token(line)
        if not command:
            continue
        normalized = command.lower().strip("&\"'")
        if normalized.endswith(".exe") and normalized[:-4] in ALLOWED_EXTERNAL_COMMANDS:
            normalized = command.lower()
        if (
            normalized not in ALLOWED_EXTERNAL_COMMANDS
            and normalized not in SHELL_STRUCTURAL_COMMANDS
            and not normalized.startswith("$")
            and not normalized.startswith("%")
            and not re.fullmatch(r"\d+", normalized)
        ):
            warnings.append(f"Command requires manual allowlist review: {command}")
    return {
        "status": STATUS_PASS if not blocking else STATUS_FAIL,
        "blocking_reasons": list(dict.fromkeys(blocking)),
        "warnings": list(dict.fromkeys(warnings)),
    }


def _check_xml_template(text: str) -> dict[str, Any]:
    blocking: list[str] = []
    parse_text = re.sub(r"^\s*<\?xml[^>]*\?>", "", text, count=1, flags=re.IGNORECASE).strip()
    try:
        root = ET.fromstring(parse_text)
    except ET.ParseError as exc:
        return {
            "status": STATUS_FAIL,
            "blocking_reasons": [f"XML parse failed: {exc}"],
            "warnings": [],
        }
    if _local_name(root.tag) != "Task":
        blocking.append("Windows XML template root must be Task.")
    if _find_first(root, "Actions") is None:
        blocking.append("Windows XML template must contain Actions.")
    if _find_first(root, "Exec") is None:
        blocking.append("Windows XML template must contain Exec.")
    return {
        "status": STATUS_PASS if not blocking else STATUS_FAIL,
        "blocking_reasons": blocking,
        "warnings": [],
    }


def _check_script_template(text: str) -> dict[str, Any]:
    blocking: list[str] = []
    if "TEMPLATE ONLY" not in text:
        blocking.append("Script template is missing TEMPLATE ONLY marker.")
    if "Manual review required" not in text:
        blocking.append("Script template is missing Manual review required marker.")
    return {
        "status": STATUS_PASS if not blocking else STATUS_FAIL,
        "blocking_reasons": blocking,
        "warnings": [],
    }


def _check_cron_template(text: str) -> dict[str, Any]:
    blocking: list[str] = []
    warnings: list[str] = []
    cron_lines = [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    if not cron_lines:
        warnings.append("Cron template contains no active cron line.")
        return {"status": "WARNING", "blocking_reasons": [], "warnings": warnings}
    for line in cron_lines:
        parts = line.split()
        if len(parts) < 6:
            blocking.append(f"Cron line must contain 5 time fields and a command: {line}")
            continue
        time_fields = parts[:5]
        if any(not CRON_FIELD_RE.match(field) for field in time_fields):
            blocking.append(f"Cron line contains invalid time field: {line}")
        command = " ".join(parts[5:])
        scripts = set(_script_paths(command))
        if not scripts.intersection(ALLOWED_SCRIPTS):
            blocking.append(f"Cron command must call an allowed scheduler script: {line}")
    return {
        "status": STATUS_PASS if not blocking else STATUS_FAIL,
        "blocking_reasons": blocking,
        "warnings": warnings,
    }


def _check_github_actions_template(text: str) -> dict[str, Any]:
    blocking: list[str] = []
    lowered = text.lower()
    if "secrets" in lowered:
        blocking.append("GitHub Actions template must not reference secrets.")
    try:
        parsed = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        return {
            "status": STATUS_FAIL,
            "blocking_reasons": [f"GitHub Actions YAML parse failed: {exc}"],
            "warnings": [],
        }
    if not isinstance(parsed, dict):
        blocking.append("GitHub Actions YAML root must be a mapping.")
        parsed = {}
    if "name" not in parsed:
        blocking.append("GitHub Actions template must contain name.")
    if "on" not in parsed and True not in parsed and not re.search(r"(?m)^on\s*:", text):
        blocking.append("GitHub Actions template must contain on.")
    if "jobs" not in parsed:
        blocking.append("GitHub Actions template must contain jobs.")
    for run_command in _github_actions_run_commands(text):
        scripts = set(_script_paths(run_command))
        if not scripts or not scripts.issubset(set(ALLOWED_SCRIPTS)):
            blocking.append(f"GitHub Actions run step must call allowed scripts: {run_command}")
    return {
        "status": STATUS_PASS if not blocking else STATUS_FAIL,
        "blocking_reasons": blocking,
        "warnings": [],
    }


def _base_payload(
    *,
    as_of: date,
    repo_root: Path,
    validation_root: Path,
    metadata_path: Path,
    validation_status: str,
    generated_at: datetime,
    input_artifact: dict[str, Any],
    headline: str,
) -> dict[str, Any]:
    validation_path = _validation_json_path(validation_root, as_of)
    markdown_path = validation_path.with_suffix(".md")
    summary_level = _summary_level(validation_status)
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "scheduler_template_validation_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "scheduler_created": False,
        "scheduler_installed": False,
        "scheduler_enabled": False,
        "templates_executed_by_validator": False,
        "operator_brief_executed_by_validator": False,
        "pipelines_executed_by_validator": False,
        "data_downloaded_by_validator": False,
        "apply_executed_by_validator": False,
        "rollback_executed_by_validator": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "validation_status": validation_status,
        "summary_level": summary_level,
        "headline": headline,
        "input_artifacts": {"template_metadata": input_artifact},
        "template_results": [],
        "coverage": {
            "templates_declared": 0,
            "templates_found": 0,
            "templates_missing": 0,
            "templates_passed": 0,
            "templates_with_warnings": 0,
            "templates_failed": 0,
        },
        "safety_validation": {
            "status": STATUS_PASS,
            "metadata_safe": validation_status not in {STATUS_SAFETY_BLOCKED},
            "templates_only": True,
            "no_scheduler_created": True,
            "no_scheduler_installed": True,
            "no_scheduler_enabled": True,
            "no_template_execution": True,
            "no_operator_brief_execution": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_apply_or_rollback": True,
            "no_broker_replay_trading": True,
            "blocking_reasons": [],
        },
        "alerts": {
            "critical": [],
            "warnings": [],
            "notes": [
                "Validation is static and does not install or run any scheduler.",
            ],
        },
        "recommended_next_steps": [
            "Review templates manually before installation.",
            "Run TRADING-026 scheduler dry run before enabling any scheduler.",
            "Use TRADING-027 runbook for manual installation guidance.",
        ],
        "manual_review_required": {
            "required": True,
            "instructions": [
                "Do not install templates automatically.",
                "Review command paths and environment assumptions.",
                "Confirm no apply, rollback, broker, replay, or trading commands are present.",
            ],
        },
        "output_artifacts": {
            "validation_json": {"path": _display_path(validation_path, repo_root)},
            "validation_markdown": {"path": _display_path(markdown_path, repo_root)},
        },
        "audit": {
            "created_by": "scripts/validate_daily_operator_brief_scheduler_templates.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_validation_artifacts": True,
            "template_metadata_path": _display_path(metadata_path, repo_root),
        },
    }
    return payload


def _run_log_payload(
    *,
    payload: dict[str, Any],
    started_at: datetime,
    completed_at: datetime,
) -> dict[str, Any]:
    run_log = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "daily_operator_brief_scheduler_template_validation_run_log",
        "task_id": TASK_ID,
        "date": payload.get("date"),
        "mode": MODE,
        "started_at": _isoformat_z(started_at),
        "completed_at": _isoformat_z(completed_at),
        "validation_status": payload.get("validation_status"),
        "summary_level": payload.get("summary_level"),
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "scheduler_template_validation_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "scheduler_created": False,
        "scheduler_installed": False,
        "scheduler_enabled": False,
        "templates_executed_by_validator": False,
        "operator_brief_executed_by_validator": False,
        "pipelines_executed_by_validator": False,
        "data_downloaded_by_validator": False,
        "apply_executed_by_validator": False,
        "rollback_executed_by_validator": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "input_artifacts": payload.get("input_artifacts"),
        "output_artifacts": payload.get("output_artifacts"),
        "coverage": payload.get("coverage"),
        "alerts": payload.get("alerts"),
    }
    _assert_validation_safety_invariants(run_log)
    return run_log


def _template_records_from_metadata(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    output_templates = _mapping(metadata.get("output_templates"))
    records: list[dict[str, Any]] = []
    for key, value in output_templates.items():
        record = _mapping(value)
        path = str(record.get("path") or "")
        if not path:
            continue
        if record.get("enabled") is False and record.get("generated") is False:
            continue
        records.append({"template_id": str(key), "path": path})
    return records


def _metadata_safety_blocking_reasons(metadata: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if metadata.get("production_effect") != PRODUCTION_EFFECT_NONE:
        reasons.append("TRADING-028 metadata production_effect must be none.")
    for field in METADATA_TRUE_FIELDS:
        if metadata.get(field) is not True:
            reasons.append(f"TRADING-028 metadata must keep {field}=true.")
    for field in METADATA_FALSE_FIELDS:
        if metadata.get(field) is not False:
            reasons.append(f"TRADING-028 metadata must keep {field}=false.")
    return reasons


def _aggregate_validation_status(
    *,
    metadata_blocking_reasons: list[str],
    template_results: list[dict[str, Any]],
) -> str:
    if metadata_blocking_reasons:
        return STATUS_SAFETY_BLOCKED
    statuses = {str(result.get("status")) for result in template_results}
    if TEMPLATE_STATUS_SAFETY_BLOCKED in statuses:
        return STATUS_SAFETY_BLOCKED
    if statuses.intersection(
        {TEMPLATE_STATUS_FAIL, TEMPLATE_STATUS_MISSING, TEMPLATE_STATUS_ERROR}
    ):
        return STATUS_FAIL
    if TEMPLATE_STATUS_WARNING in statuses:
        return STATUS_PASS_WITH_WARNINGS
    return STATUS_PASS


def _coverage(
    template_results: list[dict[str, Any]],
    *,
    declared_count: int,
) -> dict[str, int]:
    return {
        "templates_declared": declared_count,
        "templates_found": sum(1 for result in template_results if result.get("exists") is True),
        "templates_missing": sum(
            1 for result in template_results if result.get("status") == TEMPLATE_STATUS_MISSING
        ),
        "templates_passed": sum(
            1 for result in template_results if result.get("status") == TEMPLATE_STATUS_PASS
        ),
        "templates_with_warnings": sum(
            1 for result in template_results if result.get("status") == TEMPLATE_STATUS_WARNING
        ),
        "templates_failed": sum(
            1
            for result in template_results
            if result.get("status")
            in {
                TEMPLATE_STATUS_FAIL,
                TEMPLATE_STATUS_MISSING,
                TEMPLATE_STATUS_SAFETY_BLOCKED,
                TEMPLATE_STATUS_ERROR,
            }
        ),
    }


def _template_warnings(template_results: list[dict[str, Any]]) -> list[str]:
    warnings: list[str] = []
    for result in template_results:
        for warning in _strings(result.get("warnings")):
            warnings.append(f"{result.get('template_id')}: {warning}")
    return warnings


def _template_critical_findings(template_results: list[dict[str, Any]]) -> list[str]:
    findings: list[str] = []
    for result in template_results:
        for reason in _strings(result.get("blocking_reasons")):
            findings.append(f"{result.get('template_id')}: {reason}")
    return findings


def _latest_template_metadata_path(templates_root: Path, as_of: date) -> Path:
    default_path = _default_template_metadata_path(templates_root, as_of)
    if default_path.exists():
        return default_path
    if not templates_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in templates_root.glob("daily_operator_brief_scheduler_templates_*.json"):
        raw_date = path.stem.removeprefix("daily_operator_brief_scheduler_templates_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _default_template_metadata_path(templates_root: Path, as_of: date) -> Path:
    return templates_root / f"daily_operator_brief_scheduler_templates_{as_of.isoformat()}.json"


def _validation_json_path(validation_root: Path, as_of: date) -> Path:
    return validation_root / (
        f"daily_operator_brief_scheduler_template_validation_{as_of.isoformat()}.json"
    )


def _run_log_json_path(validation_root: Path, as_of: date) -> Path:
    return (
        validation_root
        / "logs"
        / f"daily_operator_brief_scheduler_template_validation_run_{as_of.isoformat()}.json"
    )


def _input_artifact_record(
    metadata_path: Path,
    repo_root: Path,
    *,
    status: str | None = None,
) -> dict[str, Any]:
    exists = metadata_path.exists()
    return {
        "status": status or ("FOUND" if exists else "MISSING"),
        "path": _display_path(metadata_path, repo_root),
        "sha256": _sha256_file(metadata_path) if exists else "",
    }


def _path_is_safe(path: Path, templates_root: Path) -> bool:
    resolved = _resolved(path)
    resolved_root = _resolved(templates_root)
    if not _is_relative_to(resolved, resolved_root):
        return False
    normalized = str(resolved).replace("\\", "/").lower()
    for marker in DANGEROUS_PATH_MARKERS:
        if marker.replace("\\", "/").lower() in normalized:
            return False
    return True


def _template_type(template_id: str, path: Path) -> str:
    name = path.name.lower()
    lowered_id = template_id.lower()
    if "windows_task_xml" in lowered_id or name.endswith(".xml.template"):
        return "windows_task_xml"
    if "powershell" in lowered_id or name.endswith(".ps1.template"):
        return "powershell_wrapper"
    if "batch" in lowered_id or name.endswith(".bat.template"):
        return "batch_wrapper"
    if "cron" in lowered_id or name.endswith(".txt.template"):
        return "cron_line"
    if "github" in lowered_id or name.endswith((".yml.template", ".yaml.template")):
        return "github_actions_workflow"
    return "unknown_template"


def _active_template_lines(text: str) -> list[str]:
    active: list[str] = []
    in_xml_comment = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if in_xml_comment:
            if "-->" in line:
                in_xml_comment = False
            continue
        if line.startswith("<!--"):
            if "-->" not in line:
                in_xml_comment = True
            continue
        if line.startswith(("#", "REM ", "rem ", "::")):
            continue
        active.append(line)
    return active


def _github_actions_run_commands(text: str) -> list[str]:
    commands: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("run:"):
            commands.append(line.removeprefix("run:").strip())
    return commands


def _script_paths(line: str) -> list[str]:
    return [match.group(0).replace("\\", "/") for match in SCRIPT_PATH_RE.finditer(line)]


def _first_command_token(line: str) -> str:
    stripped = line.strip()
    if not stripped:
        return ""
    if stripped.startswith(("<", "$", "%", "{", "}", ")")):
        return ""
    if stripped.startswith("& "):
        return "&"
    if stripped[0].isdigit():
        return stripped.split()[0]
    token = stripped.split()[0]
    return token.strip('"')


def _classify_risk_words(
    *,
    line: str,
    warnings: list[str],
    notes: list[str],
) -> None:
    lowered = line.lower()
    lowered = re.sub(r"\btrading-\d+\b", "", lowered)
    if not any(
        word in lowered
        for word in (
            "broker",
            "replay",
            "trading",
            "apply",
            "rollback",
            "secrets",
            "api_key",
            "credential",
        )
    ):
        return
    is_comment = line.strip().startswith(("#", "REM ", "rem ", "::", "<!--"))
    safety_context = any(
        phrase in lowered
        for phrase in (
            "must not",
            "do not",
            "不得",
            "禁止",
            "no apply",
            "not run",
        )
    )
    if is_comment and safety_context:
        notes.append("Risk word appears only in a safety instruction comment.")
        return
    if is_comment:
        warnings.append(f"Risk keyword requires manual review: {line.strip()}")


def _summary_level(status: str) -> str:
    if status == STATUS_PASS:
        return SUMMARY_NORMAL
    if status == STATUS_PASS_WITH_WARNINGS:
        return SUMMARY_WATCH
    if status in {STATUS_FAIL, STATUS_INPUT_MISSING, STATUS_INPUT_INVALID}:
        return SUMMARY_ACTION_REQUIRED
    if status == STATUS_SAFETY_BLOCKED:
        return SUMMARY_SAFETY_BLOCKED
    return SUMMARY_ERROR


def _headline(status: str) -> str:
    return {
        STATUS_PASS: (
            "Scheduler templates passed static validation and are ready for manual review."
        ),
        STATUS_PASS_WITH_WARNINGS: (
            "Scheduler templates passed static validation with warnings for manual review."
        ),
        STATUS_FAIL: "Scheduler template validation failed; fix template issues before review.",
        STATUS_SAFETY_BLOCKED: (
            "Scheduler template validation was safety blocked; do not install templates."
        ),
        STATUS_INPUT_MISSING: "TRADING-028 scheduler template metadata is missing.",
        STATUS_INPUT_INVALID: "TRADING-028 scheduler template metadata is invalid.",
        STATUS_ERROR: "Scheduler template validation failed with an unexpected error.",
    }[status]


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _find_first(root: ET.Element, local_name: str) -> ET.Element | None:
    for element in root.iter():
        if _local_name(element.tag) == local_name:
            return element
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_path(repo_root: Path, path: Path | str) -> Path:
    value = Path(str(path))
    return _resolved(value if value.is_absolute() else repo_root / value)


def _resolve_optional_path(repo_root: Path, path: Path | str | None) -> Path | None:
    if path is None:
        return None
    return _resolve_path(repo_root, path)


def _resolved(path: Path) -> Path:
    return path.expanduser().resolve()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _display_path(path: Path, repo_root: Path) -> str:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return str(path)


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if str(item)]
    return []


def _markdown_bullets(items: list[str]) -> list[str]:
    return [f"- {item}" for item in items] or ["- None."]


def _bool_text(value: Any) -> str:
    return str(value is True).lower()


def _assert_validation_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("scheduler template validator production_effect must remain none")
    for field in VALIDATION_TRUE_FIELDS:
        if payload.get(field) is not True:
            raise ValueError(f"scheduler template validator must keep {field}=true")
    for field in VALIDATION_FALSE_FIELDS:
        if payload.get(field) is not False:
            raise ValueError(f"scheduler template validator must keep {field}=false")
