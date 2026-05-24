from __future__ import annotations

import json
import re
import sys
from datetime import UTC, date, datetime, time, timedelta, timezone
from html import escape as html_escape
from pathlib import Path
from typing import Any

try:  # pragma: no cover - depends on local timezone database availability.
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - Python 3.11 in this project provides zoneinfo.
    ZoneInfo = None  # type: ignore[assignment]

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "daily_operator_brief_scheduler_templates"
TASK_ID = "TRADING-028"
MODE = "daily_operator_brief_scheduler_template_generation_only"
PRODUCTION_EFFECT_NONE = "none"

STATUS_GENERATED = "GENERATED"
STATUS_GENERATED_WITH_WARNINGS = "GENERATED_WITH_WARNINGS"
STATUS_SAFETY_BLOCKED = "SAFETY_BLOCKED"
STATUS_ERROR = "ERROR"

SUMMARY_NORMAL = "NORMAL"
SUMMARY_WATCH = "WATCH"
SUMMARY_SAFETY_BLOCKED = "SAFETY_BLOCKED"
SUMMARY_ERROR = "ERROR"

DEFAULT_EXPECTED_RUN_HOUR = 9
DEFAULT_EXPECTED_RUN_MINUTE = 0
DEFAULT_TIMEZONE = "Asia/Tokyo"
DEFAULT_OPERATOR_BRIEF_SCRIPT = "scripts/run_daily_trading_system_operator_brief.py"
DEFAULT_DRY_RUN_SCRIPT = "scripts/run_daily_operator_brief_scheduler_dry_run.py"

REPO_ROOT = Path(__file__).resolve().parents[3]

SAFETY_TRUE_FIELDS = (
    "manual_review_only",
    "scheduler_template_only",
    "read_only",
    "safe_for_scheduler",
)
SAFETY_FALSE_FIELDS = (
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

DANGEROUS_ACTIVE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "Template active command references shadow promotion apply.",
        re.compile(r"scripts[/\\]run_shadow_promotion_apply\.py", re.IGNORECASE),
    ),
    (
        "Template active command references shadow promotion rollback.",
        re.compile(r"scripts[/\\]run_shadow_promotion_rollback\.py", re.IGNORECASE),
    ),
    (
        "Template active command references paper trading runner.",
        re.compile(
            r"scripts[/\\]run_paper_trading(?:_from_candidates|_replay)?\.py",
            re.IGNORECASE,
        ),
    ),
    (
        "Template active command references IBKR or broker runner.",
        re.compile(r"\b(?:ibkr|broker|paperbroker)\b", re.IGNORECASE),
    ),
    (
        "Template active command references replay runner.",
        re.compile(r"\breplay\b", re.IGNORECASE),
    ),
    (
        "Template active command references trading execution.",
        re.compile(r"trading\s+execution", re.IGNORECASE),
    ),
    (
        "Template active command contains schtasks /Create.",
        re.compile(r"schtasks\s+/create", re.IGNORECASE),
    ),
    (
        "Template active command contains crontab installation.",
        re.compile(r"\bcrontab\s+-", re.IGNORECASE),
    ),
    (
        "Template active command writes GitHub Actions workflow path.",
        re.compile(r"(?:^|\s|[\"'])\.github[/\\]workflows[/\\]", re.IGNORECASE),
    ),
    (
        "Template active command uses explicit production-write danger flag.",
        re.compile(
            r"--i-understand-this-(?:writes-production|rolls-back-production)",
            re.IGNORECASE,
        ),
    ),
)

UNSAFE_OUTPUT_PATH_MARKERS = (
    ".github/workflows",
    ".github\\workflows",
    "windows/system32/tasks",
    "windows\\system32\\tasks",
    "/etc/cron",
    "\\etc\\cron",
    "/var/spool/cron",
    "\\var\\spool\\cron",
)


def default_scheduler_template_root(repo_root: Path = REPO_ROOT) -> Path:
    return repo_root / "data" / "derived" / "operator_briefs" / "scheduler_templates"


def default_scheduler_template_json_path(repo_root: Path, as_of: date) -> Path:
    return default_scheduler_template_root(repo_root) / (
        f"daily_operator_brief_scheduler_templates_{as_of.isoformat()}.json"
    )


def write_daily_operator_brief_scheduler_templates(
    *,
    as_of: date,
    repo_root: Path = REPO_ROOT,
    python_path: Path | str | None = None,
    output_root: Path | None = None,
    expected_run_hour: int = DEFAULT_EXPECTED_RUN_HOUR,
    expected_run_minute: int = DEFAULT_EXPECTED_RUN_MINUTE,
    timezone_name: str = DEFAULT_TIMEZONE,
    include_windows_task_xml: bool = True,
    include_powershell_wrapper: bool = True,
    include_batch_wrapper: bool = True,
    include_cron: bool = True,
    include_github_actions: bool = True,
    operator_brief_script: str = DEFAULT_OPERATOR_BRIEF_SCRIPT,
    dry_run_script: str = DEFAULT_DRY_RUN_SCRIPT,
    generated_at: datetime | None = None,
    template_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_repo_root = _resolved(repo_root)
    requested_output_root = _resolve_output_root(
        repo_root=resolved_repo_root,
        output_root=output_root,
    )
    write_root = requested_output_root
    if not _is_safe_output_root(repo_root=resolved_repo_root, output_root=requested_output_root):
        write_root = default_scheduler_template_root(resolved_repo_root)

    try:
        payload, templates = _build_payload_and_templates(
            as_of=as_of,
            repo_root=resolved_repo_root,
            python_path=Path(str(python_path or sys.executable)),
            output_root=requested_output_root,
            metadata_root=write_root,
            expected_run_hour=expected_run_hour,
            expected_run_minute=expected_run_minute,
            timezone_name=timezone_name,
            include_windows_task_xml=include_windows_task_xml,
            include_powershell_wrapper=include_powershell_wrapper,
            include_batch_wrapper=include_batch_wrapper,
            include_cron=include_cron,
            include_github_actions=include_github_actions,
            operator_brief_script=operator_brief_script,
            dry_run_script=dry_run_script,
            generated_at=generated,
            template_overrides=template_overrides,
        )
    except Exception as exc:  # pragma: no cover - defensive artifact path.
        payload = _error_payload(
            as_of=as_of,
            repo_root=resolved_repo_root,
            output_root=requested_output_root,
            metadata_root=write_root,
            expected_run_hour=expected_run_hour,
            expected_run_minute=expected_run_minute,
            timezone_name=timezone_name,
            operator_brief_script=operator_brief_script,
            dry_run_script=dry_run_script,
            generated_at=generated,
            error=str(exc),
        )
        templates = []

    if payload["template_generation_status"] != STATUS_SAFETY_BLOCKED:
        for template in templates:
            if template["enabled"]:
                _write_text(Path(str(template["absolute_path"])), str(template["text"]))

    metadata_path = _metadata_json_path(write_root, as_of)
    markdown_path = metadata_path.with_suffix(".md")
    _write_json(metadata_path, payload)
    _write_text(markdown_path, render_scheduler_template_summary_markdown(payload))
    return payload


def build_daily_operator_brief_scheduler_templates_payload(
    *,
    as_of: date,
    repo_root: Path = REPO_ROOT,
    python_path: Path | str | None = None,
    output_root: Path | None = None,
    expected_run_hour: int = DEFAULT_EXPECTED_RUN_HOUR,
    expected_run_minute: int = DEFAULT_EXPECTED_RUN_MINUTE,
    timezone_name: str = DEFAULT_TIMEZONE,
    include_windows_task_xml: bool = True,
    include_powershell_wrapper: bool = True,
    include_batch_wrapper: bool = True,
    include_cron: bool = True,
    include_github_actions: bool = True,
    operator_brief_script: str = DEFAULT_OPERATOR_BRIEF_SCRIPT,
    dry_run_script: str = DEFAULT_DRY_RUN_SCRIPT,
    generated_at: datetime | None = None,
    template_overrides: dict[str, str] | None = None,
) -> dict[str, Any]:
    resolved_repo_root = _resolved(repo_root)
    requested_output_root = _resolve_output_root(
        repo_root=resolved_repo_root,
        output_root=output_root,
    )
    metadata_root = (
        requested_output_root
        if _is_safe_output_root(repo_root=resolved_repo_root, output_root=requested_output_root)
        else default_scheduler_template_root(resolved_repo_root)
    )
    payload, _templates = _build_payload_and_templates(
        as_of=as_of,
        repo_root=resolved_repo_root,
        python_path=Path(str(python_path or sys.executable)),
        output_root=requested_output_root,
        metadata_root=metadata_root,
        expected_run_hour=expected_run_hour,
        expected_run_minute=expected_run_minute,
        timezone_name=timezone_name,
        include_windows_task_xml=include_windows_task_xml,
        include_powershell_wrapper=include_powershell_wrapper,
        include_batch_wrapper=include_batch_wrapper,
        include_cron=include_cron,
        include_github_actions=include_github_actions,
        operator_brief_script=operator_brief_script,
        dry_run_script=dry_run_script,
        generated_at=generated_at or datetime.now(tz=UTC),
        template_overrides=template_overrides,
    )
    return payload


def render_scheduler_template_summary_markdown(payload: dict[str, Any]) -> str:
    templates = _mapping(payload.get("output_templates"))
    manual_review = _mapping(payload.get("manual_review_required"))
    instructions = _strings(manual_review.get("instructions"))
    lines = [
        f"# Daily Operator Brief Scheduler Templates - {payload.get('date')}",
        "",
        "## 1. Summary",
        "",
        f"- Template Generation Status: `{payload.get('template_generation_status')}`",
        f"- Scheduler Created: `{_bool_text(payload.get('scheduler_created'))}`",
        f"- Scheduler Installed: `{_bool_text(payload.get('scheduler_installed'))}`",
        f"- Scheduler Enabled: `{_bool_text(payload.get('scheduler_enabled'))}`",
        f"- Manual Review Required: `{_bool_text(manual_review.get('required'))}`",
        f"- Generated Template Count: `{payload.get('generated_template_count')}`",
        "",
        "## 2. Generated Templates",
        "",
        "| Template | Path | Notes |",
        "|---|---|---|",
    ]
    labels = {
        "windows_task_xml": "Windows Task XML",
        "powershell_wrapper": "PowerShell Wrapper",
        "batch_wrapper": "Batch Wrapper",
        "cron_line": "cron Line",
        "github_actions_workflow": "GitHub Actions Workflow",
    }
    for key, label in labels.items():
        record = _mapping(templates.get(key))
        notes = "Template only" if record.get("generated") is True else "Not generated"
        lines.append(f"| {label} | `{record.get('path', '')}` | {notes} |")
    lines.extend(
        [
            "",
            "## 3. Safety Statement",
            "",
            "TRADING-028 does not create or enable any scheduler.",
            "",
            "It does not run:",
            "- apply",
            "- rollback",
            "- broker",
            "- replay",
            "- trading execution",
            "- data download",
            "- market/backtest/scoring pipeline",
            "",
            "## 4. Manual Review Checklist",
            "",
            "- [ ] Run TRADING-026 dry run first.",
            "- [ ] Confirm dry_run_decision = READY.",
            "- [ ] Review all generated templates.",
            "- [ ] Confirm no apply / rollback / broker / replay / trading commands exist.",
            "- [ ] Copy template manually only after review.",
            "- [ ] Keep logs under data/derived/operator_briefs/scheduler_logs/.",
            "",
            "## 5. Next Steps",
            "",
            "- Use TRADING-027 runbook to install manually.",
            "- Do not install if dry-run is NOT_READY or SAFETY_BLOCKED.",
            "",
            "## Audit",
            "",
            f"- production_effect: `{payload.get('production_effect')}`",
            f"- manual_review_only: `{_bool_text(payload.get('manual_review_only'))}`",
            f"- scheduler_template_only: `{_bool_text(payload.get('scheduler_template_only'))}`",
            f"- read_only: `{_bool_text(payload.get('read_only'))}`",
            f"- safe_for_scheduler: `{_bool_text(payload.get('safe_for_scheduler'))}`",
            f"- scheduler_created: `{_bool_text(payload.get('scheduler_created'))}`",
            f"- scheduler_installed: `{_bool_text(payload.get('scheduler_installed'))}`",
            f"- scheduler_enabled: `{_bool_text(payload.get('scheduler_enabled'))}`",
            "",
            "## Manual Review Required",
            "",
            *_markdown_bullets(instructions),
            "",
        ]
    )
    return "\n".join(lines)


def scan_scheduler_template_safety(
    templates: dict[str, str],
    *,
    output_paths: list[Path] | None = None,
) -> dict[str, Any]:
    blocking_reasons: list[str] = []
    output_paths = output_paths or []
    for path in output_paths:
        reason = _unsafe_output_path_reason(path)
        if reason:
            blocking_reasons.append(reason)
    for key, text in templates.items():
        blocking_reasons.extend(_template_blocking_reasons(key=key, text=text))
    return {
        "status": "PASS" if not blocking_reasons else "FAIL",
        "templates_only": not blocking_reasons,
        "no_scheduler_created": True,
        "no_scheduler_installed": True,
        "no_scheduler_enabled": True,
        "no_operator_brief_execution": True,
        "no_pipeline_execution": True,
        "no_data_download": True,
        "no_apply_or_rollback": True,
        "no_broker_replay_trading": True,
        "blocking_reasons": list(dict.fromkeys(blocking_reasons)),
    }


def _build_payload_and_templates(
    *,
    as_of: date,
    repo_root: Path,
    python_path: Path,
    output_root: Path,
    metadata_root: Path,
    expected_run_hour: int,
    expected_run_minute: int,
    timezone_name: str,
    include_windows_task_xml: bool,
    include_powershell_wrapper: bool,
    include_batch_wrapper: bool,
    include_cron: bool,
    include_github_actions: bool,
    operator_brief_script: str,
    dry_run_script: str,
    generated_at: datetime,
    template_overrides: dict[str, str] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    _validate_schedule_inputs(
        expected_run_hour=expected_run_hour,
        expected_run_minute=expected_run_minute,
    )
    output_safe = _is_safe_output_root(repo_root=repo_root, output_root=output_root)
    warnings = _warnings(
        repo_root=repo_root,
        python_path=python_path,
        include_values=[
            include_windows_task_xml,
            include_powershell_wrapper,
            include_batch_wrapper,
            include_cron,
            include_github_actions,
        ],
        timezone_name=timezone_name,
    )
    blocking_reasons: list[str] = []
    if not output_safe:
        blocking_reasons.append(
            "Output root must stay under data/derived/operator_briefs/scheduler_templates/."
        )
        unsafe_reason = _unsafe_output_path_reason(output_root)
        if unsafe_reason:
            blocking_reasons.append(unsafe_reason)

    template_records = _template_records(
        as_of=as_of,
        repo_root=repo_root,
        python_path=python_path,
        output_root=output_root,
        expected_run_hour=expected_run_hour,
        expected_run_minute=expected_run_minute,
        timezone_name=timezone_name,
        include_windows_task_xml=include_windows_task_xml,
        include_powershell_wrapper=include_powershell_wrapper,
        include_batch_wrapper=include_batch_wrapper,
        include_cron=include_cron,
        include_github_actions=include_github_actions,
        operator_brief_script=operator_brief_script,
        dry_run_script=dry_run_script,
        template_overrides=template_overrides or {},
    )
    safety_scan = scan_scheduler_template_safety(
        {str(record["key"]): str(record["text"]) for record in template_records},
        output_paths=[Path(str(record["absolute_path"])) for record in template_records],
    )
    blocking_reasons.extend(_strings(safety_scan.get("blocking_reasons")))
    blocking_reasons = list(dict.fromkeys(blocking_reasons))
    safety_pass = not blocking_reasons
    enabled_count = sum(1 for record in template_records if record["enabled"])
    generated_count = enabled_count if safety_pass else 0
    if not safety_pass:
        template_generation_status = STATUS_SAFETY_BLOCKED
    elif warnings:
        template_generation_status = STATUS_GENERATED_WITH_WARNINGS
    else:
        template_generation_status = STATUS_GENERATED

    metadata_path = _metadata_json_path(metadata_root, as_of)
    markdown_path = metadata_path.with_suffix(".md")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "scheduler_template_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "scheduler_created": False,
        "scheduler_installed": False,
        "scheduler_enabled": False,
        "operator_brief_executed_by_template_generator": False,
        "pipelines_executed_by_template_generator": False,
        "data_downloaded_by_template_generator": False,
        "apply_executed_by_template_generator": False,
        "rollback_executed_by_template_generator": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "template_generation_status": template_generation_status,
        "summary_level": _summary_level(template_generation_status),
        "headline": _headline(template_generation_status),
        "generated_template_count": generated_count,
        "template_warnings": warnings,
        "template_inputs": {
            "repo_root": str(repo_root),
            "python_path": str(python_path),
            "output_root": _display_path(output_root, repo_root),
            "metadata_output_root": _display_path(metadata_root, repo_root),
            "expected_run_time_local": _time_label(expected_run_hour, expected_run_minute),
            "timezone": timezone_name,
            "operator_brief_script": operator_brief_script,
            "dry_run_script": dry_run_script,
        },
        "output_templates": _output_template_records(
            template_records=template_records,
            repo_root=repo_root,
            generated=safety_pass,
        ),
        "output_artifacts": {
            "metadata_json": {"path": _display_path(metadata_path, repo_root)},
            "summary_markdown": {"path": _display_path(markdown_path, repo_root)},
        },
        "summary_markdown_path": _display_path(markdown_path, repo_root),
        "safety_validation": {
            "status": "PASS" if safety_pass else "FAIL",
            "templates_only": safety_pass,
            "no_scheduler_created": True,
            "no_scheduler_installed": True,
            "no_scheduler_enabled": True,
            "no_operator_brief_execution": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_apply_or_rollback": True,
            "no_broker_replay_trading": True,
            "blocking_reasons": blocking_reasons,
        },
        "manual_review_required": {
            "required": True,
            "instructions": [
                "Review generated templates before copying them.",
                "Run TRADING-026 scheduler dry run before enabling any scheduler.",
                "Do not schedule apply, rollback, broker, replay, or trading scripts.",
            ],
        },
        "safety_checklist": [
            "Confirm every template remains a .template file.",
            "Confirm templates are copied manually only after review.",
            "Confirm TRADING-026 dry run is READY before enabling any scheduler.",
            "Confirm no apply, rollback, broker, replay, or trading command exists.",
        ],
        "audit": {
            "created_by": "scripts/generate_daily_operator_brief_scheduler_templates.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_scheduler_template_artifacts": True,
        },
    }
    _assert_scheduler_template_safety_invariants(payload)
    return payload, template_records


def _template_records(
    *,
    as_of: date,
    repo_root: Path,
    python_path: Path,
    output_root: Path,
    expected_run_hour: int,
    expected_run_minute: int,
    timezone_name: str,
    include_windows_task_xml: bool,
    include_powershell_wrapper: bool,
    include_batch_wrapper: bool,
    include_cron: bool,
    include_github_actions: bool,
    operator_brief_script: str,
    dry_run_script: str,
    template_overrides: dict[str, str],
) -> list[dict[str, Any]]:
    suffix = as_of.isoformat()
    windows_root = output_root / "windows"
    cron_root = output_root / "cron"
    github_root = output_root / "github_actions"
    records = [
        {
            "key": "windows_task_xml",
            "enabled": include_windows_task_xml,
            "absolute_path": windows_root / f"daily_operator_brief_task_{suffix}.xml.template",
            "text": template_overrides.get(
                "windows_task_xml",
                _render_windows_task_xml_template(
                    as_of=as_of,
                    repo_root=repo_root,
                    output_root=output_root,
                    expected_run_hour=expected_run_hour,
                    expected_run_minute=expected_run_minute,
                ),
            ),
        },
        {
            "key": "powershell_wrapper",
            "enabled": include_powershell_wrapper,
            "absolute_path": windows_root / f"run_daily_operator_brief_{suffix}.ps1.template",
            "text": template_overrides.get(
                "powershell_wrapper",
                _render_powershell_wrapper_template(
                    repo_root=repo_root,
                    python_path=python_path,
                    operator_brief_script=operator_brief_script,
                    dry_run_script=dry_run_script,
                ),
            ),
        },
        {
            "key": "batch_wrapper",
            "enabled": include_batch_wrapper,
            "absolute_path": windows_root / f"run_daily_operator_brief_{suffix}.bat.template",
            "text": template_overrides.get(
                "batch_wrapper",
                _render_batch_wrapper_template(
                    repo_root=repo_root,
                    python_path=python_path,
                    operator_brief_script=operator_brief_script,
                    dry_run_script=dry_run_script,
                ),
            ),
        },
        {
            "key": "cron_line",
            "enabled": include_cron,
            "absolute_path": cron_root / f"daily_operator_brief_cron_{suffix}.txt.template",
            "text": template_overrides.get(
                "cron_line",
                _render_cron_line_template(
                    repo_root=repo_root,
                    python_path=python_path,
                    expected_run_hour=expected_run_hour,
                    expected_run_minute=expected_run_minute,
                    operator_brief_script=operator_brief_script,
                    dry_run_script=dry_run_script,
                ),
            ),
        },
        {
            "key": "github_actions_workflow",
            "enabled": include_github_actions,
            "absolute_path": github_root / f"daily_operator_brief_workflow_{suffix}.yml.template",
            "text": template_overrides.get(
                "github_actions_workflow",
                _render_github_actions_template(
                    as_of=as_of,
                    expected_run_hour=expected_run_hour,
                    expected_run_minute=expected_run_minute,
                    timezone_name=timezone_name,
                    operator_brief_script=operator_brief_script,
                    dry_run_script=dry_run_script,
                ),
            ),
        },
    ]
    return [record for record in records if record["enabled"]]


def _render_windows_task_xml_template(
    *,
    as_of: date,
    repo_root: Path,
    output_root: Path,
    expected_run_hour: int,
    expected_run_minute: int,
) -> str:
    start = datetime.combine(as_of, time(expected_run_hour, expected_run_minute))
    ps_template = (
        output_root / "windows" / f"run_daily_operator_brief_{as_of.isoformat()}.ps1.template"
    )
    return "\n".join(
        [
            '<?xml version="1.0" encoding="UTF-16"?>',
            "<!--",
            "TRADING-028 TEMPLATE ONLY.",
            "Manual review required.",
            "Do not import until TRADING-026 dry run is READY.",
            "This task must not run apply, rollback, broker, replay, or trading commands.",
            "-->",
            (
                '<Task version="1.4" '
                'xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">'
            ),
            "  <RegistrationInfo>",
            "    <Description>TRADING-028 template only. Manual review required.</Description>",
            "  </RegistrationInfo>",
            "  <Triggers>",
            "    <CalendarTrigger>",
            f"      <StartBoundary>{start.isoformat()}</StartBoundary>",
            "      <Enabled>true</Enabled>",
            "      <ScheduleByDay><DaysInterval>1</DaysInterval></ScheduleByDay>",
            "    </CalendarTrigger>",
            "  </Triggers>",
            "  <Principals>",
            '    <Principal id="Author"><LogonType>InteractiveToken</LogonType></Principal>',
            "  </Principals>",
            "  <Settings>",
            "    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>",
            "    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
            "    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>",
            "    <AllowHardTerminate>true</AllowHardTerminate>",
            "    <StartWhenAvailable>false</StartWhenAvailable>",
            "    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>",
            "    <Enabled>true</Enabled>",
            "  </Settings>",
            '  <Actions Context="Author">',
            "    <Exec>",
            "      <Command>powershell.exe</Command>",
            "      <Arguments>"
            "-NoProfile -ExecutionPolicy Bypass -File "
            f"&quot;{html_escape(str(ps_template), quote=True)}&quot;</Arguments>",
            f"      <WorkingDirectory>{html_escape(str(repo_root), quote=True)}</WorkingDirectory>",
            "    </Exec>",
            "  </Actions>",
            "</Task>",
            "",
        ]
    )


def _render_powershell_wrapper_template(
    *,
    repo_root: Path,
    python_path: Path,
    operator_brief_script: str,
    dry_run_script: str,
) -> str:
    return "\n".join(
        [
            "# TRADING-028 TEMPLATE ONLY.",
            "# Manual review required before use.",
            "# This script must not run apply, rollback, broker, replay, or trading commands.",
            "",
            f'$RepoRoot = "{_powershell_string(str(repo_root))}"',
            f'$Python = "{_powershell_string(str(python_path))}"',
            '$Date = Get-Date -Format "yyyy-MM-dd"',
            '$LogRoot = Join-Path $RepoRoot "data/derived/operator_briefs/scheduler_logs"',
            '$LogPath = Join-Path $LogRoot "operator_brief_$Date.log"',
            "",
            "New-Item -ItemType Directory -Path $LogRoot -Force | Out-Null",
            "Set-Location $RepoRoot",
            "",
            f'& $Python "{dry_run_script}" --date $Date *>> $LogPath',
            "if ($LASTEXITCODE -ne 0) {",
            '    Write-Error "Scheduler dry run failed. Operator brief generation skipped."',
            "    exit 1",
            "}",
            "",
            f'& $Python "{operator_brief_script}" --date $Date *>> $LogPath',
            "if ($LASTEXITCODE -ne 0) {",
            '    Write-Error "Operator brief generation failed."',
            "    exit 1",
            "}",
            "",
        ]
    )


def _render_batch_wrapper_template(
    *,
    repo_root: Path,
    python_path: Path,
    operator_brief_script: str,
    dry_run_script: str,
) -> str:
    dry_script = dry_run_script.replace("/", "\\")
    brief_script = operator_brief_script.replace("/", "\\")
    return "\n".join(
        [
            "@echo off",
            "REM TRADING-028 TEMPLATE ONLY.",
            "REM Manual review required before use.",
            "REM This script must not run apply, rollback, broker, replay, or trading commands.",
            "",
            f'set "RepoRoot={repo_root}"',
            f'set "Python={python_path}"',
            "for /f %%I in ('powershell -NoProfile -Command \"Get-Date -Format yyyy-MM-dd\"') "
            'do set "RunDate=%%I"',
            'set "LogRoot=%RepoRoot%\\data\\derived\\operator_briefs\\scheduler_logs"',
            'set "LogPath=%LogRoot%\\operator_brief_%RunDate%.log"',
            "",
            'if not exist "%LogRoot%" mkdir "%LogRoot%"',
            'cd /d "%RepoRoot%"',
            "",
            f'"%Python%" "{dry_script}" --date "%RunDate%" >> "%LogPath%" 2>&1',
            "if errorlevel 1 (",
            "  echo Scheduler dry run failed. Operator brief generation skipped.",
            "  exit /b 1",
            ")",
            "",
            f'"%Python%" "{brief_script}" --date "%RunDate%" >> "%LogPath%" 2>&1',
            "if errorlevel 1 (",
            "  echo Operator brief generation failed.",
            "  exit /b 1",
            ")",
            "",
        ]
    )


def _render_cron_line_template(
    *,
    repo_root: Path,
    python_path: Path,
    expected_run_hour: int,
    expected_run_minute: int,
    operator_brief_script: str,
    dry_run_script: str,
) -> str:
    repo = _posix_path(repo_root)
    python = _posix_path(python_path)
    log = "data/derived/operator_briefs/scheduler_logs/operator_brief_cron.log"
    return "\n".join(
        [
            "# TRADING-028 TEMPLATE ONLY.",
            "# Manual review required before use.",
            "# Do not schedule apply, rollback, broker, replay, or trading commands.",
            (
                f"{expected_run_minute} {expected_run_hour} * * * cd {repo} && "
                f"{{ {python} {dry_run_script} --date $(date +\\%F) && "
                f"{python} {operator_brief_script} --date $(date +\\%F); }} "
                f">> {log} 2>&1"
            ),
            "",
        ]
    )


def _render_github_actions_template(
    *,
    as_of: date,
    expected_run_hour: int,
    expected_run_minute: int,
    timezone_name: str,
    operator_brief_script: str,
    dry_run_script: str,
) -> str:
    utc_minute, utc_hour = _github_actions_utc_schedule(
        as_of=as_of,
        expected_run_hour=expected_run_hour,
        expected_run_minute=expected_run_minute,
        timezone_name=timezone_name,
    )
    return "\n".join(
        [
            "# TRADING-028 TEMPLATE ONLY.",
            "# Manual review required before copying to .github/workflows/.",
            "# Do not run apply, rollback, broker, replay, or trading commands.",
            "",
            "name: Daily Operator Brief",
            "",
            "on:",
            "  workflow_dispatch:",
            "  # Optional schedule after manual review.",
            "  # schedule:",
            f'  #   - cron: "{utc_minute} {utc_hour} * * *"',
            "",
            "jobs:",
            "  operator-brief:",
            "    runs-on: ubuntu-latest",
            "    steps:",
            "      - uses: actions/checkout@v4",
            "      - uses: actions/setup-python@v5",
            "        with:",
            '          python-version: "3.12"',
            "      - name: Run scheduler dry run",
            f"        run: python {dry_run_script} --date \"$(date -u +'%Y-%m-%d')\"",
            "      - name: Generate daily operator brief",
            f"        run: python {operator_brief_script} --date \"$(date -u +'%Y-%m-%d')\"",
            "",
        ]
    )


def _output_template_records(
    *,
    template_records: list[dict[str, Any]],
    repo_root: Path,
    generated: bool,
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for record in template_records:
        output[str(record["key"])] = {
            "enabled": record["enabled"] is True,
            "generated": bool(generated and record["enabled"]),
            "path": _display_path(Path(str(record["absolute_path"])), repo_root),
        }
    return output


def _template_blocking_reasons(*, key: str, text: str) -> list[str]:
    reasons: list[str] = []
    if "TEMPLATE ONLY" not in text:
        reasons.append(f"{key} is missing TEMPLATE ONLY marker.")
    if "Manual review required" not in text:
        reasons.append(f"{key} is missing Manual review required marker.")
    for line in _active_template_lines(text):
        for message, pattern in DANGEROUS_ACTIVE_PATTERNS:
            if pattern.search(line):
                reasons.append(f"{key}: {message}")
    return reasons


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


def _resolve_output_root(*, repo_root: Path, output_root: Path | None) -> Path:
    value = output_root or default_scheduler_template_root(repo_root)
    return _resolved(value if value.is_absolute() else repo_root / value)


def _is_safe_output_root(*, repo_root: Path, output_root: Path) -> bool:
    safe_root = _resolved(default_scheduler_template_root(repo_root))
    resolved_output = _resolved(output_root)
    if not _is_relative_to(resolved_output, safe_root):
        return False
    return _unsafe_output_path_reason(resolved_output) == ""


def _unsafe_output_path_reason(path: Path) -> str:
    normalized = str(path).replace("\\", "/").lower()
    for marker in UNSAFE_OUTPUT_PATH_MARKERS:
        if marker.replace("\\", "/").lower() in normalized:
            return f"Unsafe scheduler output path: {path}"
    return ""


def _metadata_json_path(output_root: Path, as_of: date) -> Path:
    return output_root / f"daily_operator_brief_scheduler_templates_{as_of.isoformat()}.json"


def _warnings(
    *,
    repo_root: Path,
    python_path: Path,
    include_values: list[bool],
    timezone_name: str,
) -> list[str]:
    warnings: list[str] = []
    root_text = str(repo_root)
    python_text = str(python_path)
    if "path\\to" in root_text.lower() or "path/to" in root_text.lower():
        warnings.append("repo_root appears to be a placeholder.")
    if "path\\to" in python_text.lower() or "path/to" in python_text.lower():
        warnings.append("python_path appears to be a placeholder.")
    if not any(include_values):
        warnings.append("No optional templates were selected.")
    if timezone_name and _timezone_for_name(timezone_name) is None:
        warnings.append(f"timezone {timezone_name} is not recognized; schedule remains a template.")
    return warnings


def _timezone_for_name(timezone_name: str) -> timezone | Any | None:
    fixed_offsets = {
        "Asia/Tokyo": timezone(timedelta(hours=9)),
        "UTC": UTC,
        "Etc/UTC": UTC,
    }
    if timezone_name in fixed_offsets:
        return fixed_offsets[timezone_name]
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(timezone_name)
    except Exception:
        return None


def _github_actions_utc_schedule(
    *,
    as_of: date,
    expected_run_hour: int,
    expected_run_minute: int,
    timezone_name: str,
) -> tuple[int, int]:
    tzinfo = _timezone_for_name(timezone_name) or UTC
    local_dt = datetime.combine(
        as_of,
        time(expected_run_hour, expected_run_minute),
        tzinfo=tzinfo,
    )
    utc_dt = local_dt.astimezone(UTC)
    return utc_dt.minute, utc_dt.hour


def _summary_level(status: str) -> str:
    return {
        STATUS_GENERATED: SUMMARY_NORMAL,
        STATUS_GENERATED_WITH_WARNINGS: SUMMARY_WATCH,
        STATUS_SAFETY_BLOCKED: SUMMARY_SAFETY_BLOCKED,
        STATUS_ERROR: SUMMARY_ERROR,
    }[status]


def _headline(status: str) -> str:
    return {
        STATUS_GENERATED: (
            "Scheduler configuration templates were generated for manual review. "
            "No scheduler was installed or enabled."
        ),
        STATUS_GENERATED_WITH_WARNINGS: (
            "Scheduler configuration templates were generated with warnings for manual "
            "review. No scheduler was installed or enabled."
        ),
        STATUS_SAFETY_BLOCKED: (
            "Scheduler configuration template generation was safety blocked. No "
            "scheduler template files were written."
        ),
        STATUS_ERROR: "Scheduler configuration template generation failed with an error.",
    }[status]


def _error_payload(
    *,
    as_of: date,
    repo_root: Path,
    output_root: Path,
    metadata_root: Path,
    expected_run_hour: int,
    expected_run_minute: int,
    timezone_name: str,
    operator_brief_script: str,
    dry_run_script: str,
    generated_at: datetime,
    error: str,
) -> dict[str, Any]:
    metadata_path = _metadata_json_path(metadata_root, as_of)
    markdown_path = metadata_path.with_suffix(".md")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "mode": MODE,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "scheduler_template_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "scheduler_created": False,
        "scheduler_installed": False,
        "scheduler_enabled": False,
        "operator_brief_executed_by_template_generator": False,
        "pipelines_executed_by_template_generator": False,
        "data_downloaded_by_template_generator": False,
        "apply_executed_by_template_generator": False,
        "rollback_executed_by_template_generator": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "template_generation_status": STATUS_ERROR,
        "summary_level": SUMMARY_ERROR,
        "headline": _headline(STATUS_ERROR),
        "generated_template_count": 0,
        "template_warnings": [],
        "template_inputs": {
            "repo_root": str(repo_root),
            "python_path": str(sys.executable),
            "output_root": _display_path(output_root, repo_root),
            "metadata_output_root": _display_path(metadata_root, repo_root),
            "expected_run_time_local": _safe_time_label(expected_run_hour, expected_run_minute),
            "timezone": timezone_name,
            "operator_brief_script": operator_brief_script,
            "dry_run_script": dry_run_script,
        },
        "output_templates": {},
        "output_artifacts": {
            "metadata_json": {"path": _display_path(metadata_path, repo_root)},
            "summary_markdown": {"path": _display_path(markdown_path, repo_root)},
        },
        "summary_markdown_path": _display_path(markdown_path, repo_root),
        "safety_validation": {
            "status": "PASS",
            "templates_only": True,
            "no_scheduler_created": True,
            "no_scheduler_installed": True,
            "no_scheduler_enabled": True,
            "no_operator_brief_execution": True,
            "no_pipeline_execution": True,
            "no_data_download": True,
            "no_apply_or_rollback": True,
            "no_broker_replay_trading": True,
            "blocking_reasons": [],
        },
        "manual_review_required": {
            "required": True,
            "instructions": ["Inspect the template generator error before retrying."],
        },
        "safety_checklist": [],
        "audit": {
            "created_by": "scripts/generate_daily_operator_brief_scheduler_templates.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_scheduler_template_artifacts": True,
            "error": error,
        },
    }
    _assert_scheduler_template_safety_invariants(payload)
    return payload


def _assert_scheduler_template_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("scheduler template generator production_effect must remain none")
    for field in SAFETY_TRUE_FIELDS:
        if payload.get(field) is not True:
            raise ValueError(f"scheduler template generator must keep {field}=true")
    for field in SAFETY_FALSE_FIELDS:
        if payload.get(field) is not False:
            raise ValueError(f"scheduler template generator must keep {field}=false")


def _validate_schedule_inputs(*, expected_run_hour: int, expected_run_minute: int) -> None:
    if not 0 <= expected_run_hour <= 23:
        raise ValueError("expected_run_hour must be between 0 and 23")
    if not 0 <= expected_run_minute <= 59:
        raise ValueError("expected_run_minute must be between 0 and 59")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _display_path(path: Path, repo_root: Path) -> str:
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return str(path)


def _resolved(path: Path) -> Path:
    return path.expanduser().resolve()


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _time_label(hour: int, minute: int) -> str:
    return f"{hour:02d}:{minute:02d}"


def _safe_time_label(hour: int, minute: int) -> str:
    try:
        _validate_schedule_inputs(expected_run_hour=hour, expected_run_minute=minute)
        return _time_label(hour, minute)
    except ValueError:
        return "invalid"


def _posix_path(path: Path) -> str:
    return str(path).replace("\\", "/")


def _powershell_string(value: str) -> str:
    return value.replace("`", "``").replace('"', '`"')


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _bool_text(value: Any) -> str:
    return str(value is True).lower()


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if str(item)]
    return []


def _markdown_bullets(items: list[str]) -> list[str]:
    return [f"- {item}" for item in items] or ["- None."]
