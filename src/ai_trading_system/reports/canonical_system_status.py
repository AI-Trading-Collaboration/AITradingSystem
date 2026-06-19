from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.engineering_closeout import (
    latest_engineering_surface_inventory_json_path,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    default_report_index_json_path,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "canonical_system_status"
VALIDATION_REPORT_TYPE = "canonical_system_doctor"
PRODUCTION_EFFECT = "none"

PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"
READY_STATUS = "ENGINEERING_CONTROL_PLANE_READY"
READY_WITH_LIMITATIONS_STATUS = "ENGINEERING_CONTROL_PLANE_READY_WITH_LIMITATIONS"
BLOCKED_STATUS = "ENGINEERING_CONTROL_PLANE_BLOCKED"

REQUIRED_CANONICAL_WORKFLOW_IDS = (
    "system_doctor",
    "system_status",
    "research_status",
    "research_run",
    "research_validate",
    "reports_latest",
    "artifact_reproduce",
)

CANONICAL_WORKFLOWS: tuple[dict[str, str], ...] = (
    {
        "workflow_id": "system_doctor",
        "audience": "operator",
        "command": "aits system doctor --as-of YYYY-MM-DD",
        "purpose": (
            "Validate the canonical status bundle, report index freshness, "
            "and safety boundary."
        ),
        "legacy_compatibility": "Keeps individual validate-* commands available for scripted use.",
        "next_action_on_failure": "Run the failed command listed in doctor checks.",
    },
    {
        "workflow_id": "system_status",
        "audience": "owner/researcher/operator",
        "command": "aits system status --as-of YYYY-MM-DD",
        "purpose": (
            "Open one first-screen summary of system state, research gate, "
            "data health, blockers, and next action."
        ),
        "legacy_compatibility": (
            "Reader Brief, report index, and task-specific reports remain source artifacts."
        ),
        "next_action_on_failure": "Run aits system doctor for fail-closed diagnostics.",
    },
    {
        "workflow_id": "research_status",
        "audience": "researcher/owner",
        "command": (
            "aits reports latest --report-id candidate_v2_research_cycle_snapshot "
            "--as-of YYYY-MM-DD"
        ),
        "purpose": "Find the latest research-cycle source artifact without knowing task IDs.",
        "legacy_compatibility": (
            "Task-specific research reports remain unchanged and are linked by report id."
        ),
        "next_action_on_failure": (
            "Regenerate the expected research snapshot or use report index to inspect "
            "missing artifacts."
        ),
    },
    {
        "workflow_id": "research_run",
        "audience": "researcher",
        "command": (
            "Use the source artifact next_action from aits system status; "
            "do not start paper-shadow from this entry."
        ),
        "purpose": "Route research execution through the latest gated research source artifact.",
        "legacy_compatibility": (
            "Existing research report commands remain direct compatibility entrypoints."
        ),
        "next_action_on_failure": "Stop at the failing source validation artifact.",
    },
    {
        "workflow_id": "research_validate",
        "audience": "researcher/operator",
        "command": (
            "aits reports latest --report-id candidate_v2_research_cycle_snapshot_validation "
            "--as-of YYYY-MM-DD"
        ),
        "purpose": "Locate the validation artifact for the latest research-cycle source.",
        "legacy_compatibility": "Existing validate-* report commands remain available.",
        "next_action_on_failure": (
            "Use validation blocking_issues before running any downstream research step."
        ),
    },
    {
        "workflow_id": "reports_latest",
        "audience": "owner/researcher/operator",
        "command": "aits reports latest --report-id REPORT_ID --as-of YYYY-MM-DD",
        "purpose": (
            "Resolve the current artifact path, freshness, status, and owner action "
            "for any report registry id."
        ),
        "legacy_compatibility": "Does not rename or remove existing report commands.",
        "next_action_on_failure": (
            "Use aits reports index --as-of YYYY-MM-DD to inspect registry coverage."
        ),
    },
    {
        "workflow_id": "artifact_reproduce",
        "audience": "operator/reviewer",
        "command": (
            "aits reports artifact-lineage --date YYYY-MM-DD; "
            "inspect source_artifacts and run manifests."
        ),
        "purpose": "Follow artifact lineage and checksums before attempting manual reproduction.",
        "legacy_compatibility": (
            "Existing artifact-lineage and run manifest artifacts remain the source of truth."
        ),
        "next_action_on_failure": (
            "Restore missing source artifacts; do not fabricate replacement evidence."
        ),
    },
)


def default_canonical_system_status_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"canonical_system_status_{as_of.isoformat()}.json"


def default_canonical_system_status_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"canonical_system_status_{as_of.isoformat()}.md"


def default_canonical_system_doctor_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"canonical_system_doctor_{as_of.isoformat()}.json"


def default_canonical_system_doctor_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"canonical_system_doctor_{as_of.isoformat()}.md"


def latest_canonical_system_status_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "canonical_system_status_", ".json")


def build_canonical_system_status_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Path | None = DEFAULT_REPORT_INDEX_WAIVER_PATH,
) -> dict[str, Any]:
    report_index = build_report_index_payload(
        as_of=as_of,
        project_root=project_root,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )
    reports = _records(report_index.get("reports"))
    report_lookup = {_text(report.get("report_id")): report for report in reports}
    reports_dir = project_root / "outputs" / "reports"
    surface_inventory = _load_json_from_record(
        report_lookup.get("engineering_surface_inventory"),
        fallback=latest_engineering_surface_inventory_json_path(reports_dir),
        project_root=project_root,
    )
    reader_brief_consistency = _load_json_from_record(
        report_lookup.get("reader_brief_consistency_validation"),
        project_root=project_root,
    )
    research_snapshot = _latest_payload(
        report_lookup,
        (
            "candidate_v2_research_cycle_snapshot",
            "next_candidate_research_cycle_snapshot",
            "research_cycle_reset_pack",
        ),
        project_root=project_root,
    )
    research_gate = _latest_payload(
        report_lookup,
        (
            "candidate_v2_research_gate",
            "next_candidate_research_gate",
            "etf_dynamic_v3_primary_research_candidate_gate",
        ),
        project_root=project_root,
    )
    first_screen = _first_screen(
        as_of=as_of,
        project_root=project_root,
        report_index=report_index,
        surface_inventory=surface_inventory,
        reader_brief_consistency=reader_brief_consistency,
        research_snapshot=research_snapshot,
        research_gate=research_gate,
        report_lookup=report_lookup,
        registry_path=registry_path,
    )
    status = first_screen["current_system_state"]
    warnings = _status_warnings(
        report_index=report_index,
        surface_inventory=surface_inventory,
        reader_brief_consistency=reader_brief_consistency,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "system_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": "not_applicable_system_status_bundle",
        "purpose": (
            "Provide one canonical first-screen system status and workflow navigation surface."
        ),
        "first_screen": first_screen,
        "canonical_workflows": list(CANONICAL_WORKFLOWS),
        "report_index_summary": _mapping(report_index.get("summary")),
        "report_index_visibility_audit": _mapping(report_index.get("visibility_audit")),
        "artifact_lifecycle_summary": _artifact_lifecycle_summary(reports),
        "schema_config_summary": _schema_config_summary(project_root),
        "reproducibility_summary": _reproducibility_summary(project_root),
        "test_suite_summary": _test_suite_summary(),
        "documentation_summary": _documentation_summary(project_root),
        "warnings": warnings,
        "source_artifacts": _source_artifacts(
            project_root=project_root,
            report_index=report_index,
            surface_inventory=surface_inventory,
            reader_brief_consistency=reader_brief_consistency,
            research_snapshot=research_snapshot,
            research_gate=research_gate,
            registry_path=registry_path,
        ),
        "safety_boundary": {
            "mode": "read_existing_status_sources_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_strategy_logic": True,
            "does_not_modify_research_decisions": True,
            "does_not_append_owner_decision": True,
            "does_not_activate_paper_shadow": True,
            "does_not_approve_extended_or_live": True,
            "does_not_generate_official_target_weights": True,
            "does_not_touch_broker_or_orders": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
        "reader_brief": {
            "summary": (
                f"Canonical system status is {status}; "
                f"research gate={first_screen['latest_research_gate']}; "
                f"data health={first_screen['data_health']}."
            ),
            "key_result": status,
            "blocking_issues": "; ".join(first_screen["current_blockers"]) or "none",
            "warnings": "; ".join(first_screen["current_warnings"]) or "none",
            "safety_boundary": first_screen["safety_boundary"],
            "next_action": first_screen["recommended_next_action"],
        },
        "methodology": {
            "collector_mode": "read_existing_report_index_and_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_canonical_system_status_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    first_screen = _mapping(payload.get("first_screen"))
    workflows = _records(payload.get("canonical_workflows"))
    workflow_ids = {_text(item.get("workflow_id")) for item in workflows}
    visibility = _mapping(payload.get("report_index_visibility_audit"))
    summary = _mapping(payload.get("report_index_summary"))
    effective_unwaived = _effective_unwaived_issue_ids(visibility)
    missing_workflows = [
        workflow_id
        for workflow_id in REQUIRED_CANONICAL_WORKFLOW_IDS
        if workflow_id not in workflow_ids
    ]

    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        severity="BLOCKING",
        message=f"report_type must be {REPORT_TYPE}.",
        recommended_action="rerun_aits_system_status",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect_none",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        severity="BLOCKING",
        message="canonical system status must be production_effect=none.",
        recommended_action="regenerate_without_production_mutation",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="first_screen_required_fields",
        passed=all(
            key in first_screen
            for key in (
                "current_system_state",
                "active_research_candidate",
                "latest_research_gate",
                "data_health",
                "validation_health",
                "current_blockers",
                "current_warnings",
                "safety_boundary",
                "recommended_next_action",
                "source_artifacts",
            )
        ),
        severity="BLOCKING",
        message="first_screen must contain the canonical owner/operator fields.",
        recommended_action="fix_status_builder_first_screen_contract",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="canonical_workflows_present",
        passed=not missing_workflows,
        severity="BLOCKING",
        message=(
            "canonical workflow definitions must cover system, research, reports, "
            "and artifact reproduction."
        ),
        recommended_action="add_missing_canonical_workflow_entries",
        details={"missing_workflow_ids": missing_workflows},
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="report_index_unwaived_warnings_zero",
        passed=not effective_unwaived,
        severity="BLOCKING",
        message="report index must have zero unwaived warnings for canonical status use.",
        recommended_action="refresh_missing_or_stale_artifacts_or_add_reviewed_waiver",
        details={
            "effective_unwaived_issue_ids": effective_unwaived,
            "ignored_self_generated_issue_ids": [
                issue_id
                for issue_id in _strings(visibility.get("unwaived_issue_ids"))
                if issue_id not in effective_unwaived
            ],
        },
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="report_index_expired_waivers_zero",
        passed=int(summary.get("expired_waiver_count") or 0) == 0,
        severity="BLOCKING",
        message="report index must not depend on expired waivers.",
        recommended_action="renew_or_remove_expired_waivers_before_status_use",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="status_not_blocked",
        passed=_text(first_screen.get("current_system_state")) != BLOCKED_STATUS,
        severity="BLOCKING",
        message="canonical system status is blocked.",
        recommended_action="resolve_current_blockers_before_platform_freeze_review",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="status_limitations_visible",
        passed=_text(first_screen.get("current_system_state")) == READY_STATUS,
        severity="WARNING",
        message="canonical status has documented limitations.",
        recommended_action="review_current_warnings_before_platform_freeze",
        details={"current_warnings": _strings(first_screen.get("current_warnings"))},
    )
    _append_check(
        checks,
        warning_issues,
        check_id="research_not_misread_as_promoted",
        passed="paper-shadow disabled" in _text(first_screen.get("safety_boundary")),
        severity="WARNING",
        message=(
            "safety boundary should explicitly prevent research PASS from being read "
            "as promotion."
        ),
        recommended_action="keep_safety_boundary_language_in_canonical_status",
    )
    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of")),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "source_system_status": _text(payload.get("system_status"), _text(payload.get("status"))),
        "input_artifacts": {
            "canonical_system_status": _text(
                _mapping(payload.get("input_artifacts")).get("canonical_system_status")
            )
        },
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(
                [check for check in checks if check.get("status") == FAIL_STATUS]
            ),
            "warning_check_count": len(
                [check for check in checks if check.get("status") == WARN_STATUS]
            ),
            "blocking_issue_count": len(blocking_issues),
            "warning_issue_count": len(warning_issues),
            "canonical_workflow_count": len(workflows),
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": f"Canonical system doctor status is {status}.",
            "key_result": status,
            "blocking_issues": [issue["issue_id"] for issue in blocking_issues],
            "warnings": [issue["issue_id"] for issue in warning_issues],
            "safety_boundary": "Read-only doctor; production_effect=none.",
            "next_action": (
                "resolve_canonical_system_status_blockers"
                if status == FAIL_STATUS
                else "use_aits_system_status_as_owner_operator_entrypoint"
            ),
        },
        "methodology": {
            "mode": "read_canonical_system_status_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def resolve_latest_report_record(
    *,
    report_id: str,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Path | None = DEFAULT_REPORT_INDEX_WAIVER_PATH,
) -> dict[str, Any]:
    report_index = build_report_index_payload(
        as_of=as_of,
        project_root=project_root,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )
    for report in _records(report_index.get("reports")):
        if _text(report.get("report_id")) == report_id:
            return {
                "schema_version": SCHEMA_VERSION,
                "report_type": "report_latest_lookup",
                "as_of": as_of.isoformat(),
                "status": "FOUND" if bool(report.get("exists")) else "MISSING",
                "production_effect": PRODUCTION_EFFECT,
                "report": report,
                "report_index_summary": _mapping(report_index.get("summary")),
            }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "report_latest_lookup",
        "as_of": as_of.isoformat(),
        "status": "REPORT_ID_NOT_REGISTERED",
        "production_effect": PRODUCTION_EFFECT,
        "report": {"report_id": report_id},
        "report_index_summary": _mapping(report_index.get("summary")),
    }


def write_canonical_system_status_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_canonical_system_status_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_canonical_system_status_markdown(payload), encoding="utf-8")
    return output_path


def write_canonical_system_doctor_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    return write_canonical_system_status_json(payload, output_path)


def write_canonical_system_doctor_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_canonical_system_doctor_markdown(payload), encoding="utf-8")
    return output_path


def render_canonical_system_status_markdown(payload: Mapping[str, Any]) -> str:
    first = _mapping(payload.get("first_screen"))
    lines = [
        f"# Canonical System Status {payload.get('as_of')}",
        "",
        "## First Screen",
        "",
        f"- Current system state：{_text(first.get('current_system_state'))}",
        f"- Active research candidate：{_text(first.get('active_research_candidate'))}",
        f"- Latest research gate：{_text(first.get('latest_research_gate'))}",
        f"- Data health：{_text(first.get('data_health'))}",
        f"- Validation health：{_text(first.get('validation_health'))}",
        f"- Safety boundary：{_text(first.get('safety_boundary'))}",
        f"- Recommended next action：{_text(first.get('recommended_next_action'))}",
        "",
        "## Current Blockers",
        "",
    ]
    blockers = _strings(first.get("current_blockers"))
    lines.extend(f"- {item}" for item in blockers)
    if not blockers:
        lines.append("- none")
    lines.extend(["", "## Current Warnings", ""])
    warnings = _strings(first.get("current_warnings"))
    lines.extend(f"- {item}" for item in warnings)
    if not warnings:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Canonical Workflows",
            "",
            "|workflow_id|command|purpose|legacy_compatibility|",
            "|---|---|---|---|",
        ]
    )
    for workflow in _records(payload.get("canonical_workflows")):
        lines.append(
            f"|{_markdown_cell(workflow.get('workflow_id'))}|"
            f"{_markdown_cell(workflow.get('command'))}|"
            f"{_markdown_cell(workflow.get('purpose'))}|"
            f"{_markdown_cell(workflow.get('legacy_compatibility'))}|"
        )
    lines.extend(
        [
            "",
            "## Source Artifacts",
            "",
            "|artifact_id|status|path|",
            "|---|---|---|",
        ]
    )
    for artifact in _records(payload.get("source_artifacts")):
        lines.append(
            f"|{_markdown_cell(artifact.get('artifact_id'))}|"
            f"{_markdown_cell(artifact.get('status'))}|"
            f"{_markdown_cell(artifact.get('path'))}|"
        )
    lines.extend([""])
    return "\n".join(lines)


def render_canonical_system_doctor_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Canonical System Doctor {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('validation_status'))}",
        f"- production_effect：{_text(payload.get('production_effect'))}",
        f"- checks：{summary.get('check_count')}",
        f"- failed_checks：{summary.get('failed_check_count')}",
        f"- warnings：{summary.get('warning_check_count')}",
        "",
        "## Checks",
        "",
        "|check_id|status|severity|message|recommended_action|",
        "|---|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_markdown_cell(check.get('check_id'))}|"
            f"{_markdown_cell(check.get('status'))}|"
            f"{_markdown_cell(check.get('severity'))}|"
            f"{_markdown_cell(check.get('message'))}|"
            f"{_markdown_cell(check.get('recommended_action'))}|"
        )
    lines.extend([""])
    return "\n".join(lines)


def _first_screen(
    *,
    as_of: date,
    project_root: Path,
    report_index: Mapping[str, Any],
    surface_inventory: Mapping[str, Any],
    reader_brief_consistency: Mapping[str, Any],
    research_snapshot: Mapping[str, Any],
    research_gate: Mapping[str, Any],
    report_lookup: Mapping[str, Mapping[str, Any]],
    registry_path: Path,
) -> dict[str, Any]:
    summary = _mapping(report_index.get("summary"))
    visibility = _mapping(report_index.get("visibility_audit"))
    effective_unwaived = _effective_unwaived_issue_ids(visibility)
    blockers = _blockers(report_index)
    warnings = _status_warnings(
        report_index=report_index,
        surface_inventory=surface_inventory,
        reader_brief_consistency=reader_brief_consistency,
    )
    if blockers:
        state = BLOCKED_STATUS
    elif warnings:
        state = READY_WITH_LIMITATIONS_STATUS
    else:
        state = READY_STATUS
    research_summary = _mapping(research_snapshot.get("summary"))
    gate_summary = _mapping(research_gate.get("summary"))
    data_health = _health_from_reports(
        report_lookup,
        ("data_quality", "data_refresh_audit", "data_source_fallback_policy", "cache_catalog"),
    )
    validation_health = (
        "PASS"
        if not effective_unwaived
        and int(summary.get("expired_waiver_count") or 0) == 0
        else "NEEDS_REVIEW"
    )
    return {
        "as_of": as_of.isoformat(),
        "current_system_state": state,
        "active_research_candidate": _text(
            research_summary.get("candidate_id"),
            _text(gate_summary.get("candidate_id"), "UNKNOWN"),
        ),
        "latest_research_gate": _text(
            research_summary.get("source_research_gate_decision"),
            _text(research_gate.get("status"), "UNKNOWN"),
        ),
        "data_health": data_health,
        "validation_health": validation_health,
        "current_blockers": blockers,
        "current_warnings": warnings,
        "safety_boundary": (
            "research-only; paper-shadow disabled; extended/live disabled; "
            "official target weights disabled; broker/order disabled; production_effect=none"
        ),
        "recommended_next_action": _recommended_next_action(
            state=state,
            research_snapshot=research_snapshot,
            warnings=warnings,
        ),
        "source_artifacts": [
            artifact["artifact_id"] for artifact in _source_artifacts(
                project_root=project_root,
                report_index=report_index,
                surface_inventory=surface_inventory,
                reader_brief_consistency=reader_brief_consistency,
                research_snapshot=research_snapshot,
                research_gate=research_gate,
                registry_path=registry_path,
            )
        ],
        "report_index_unwaived_issue_ids": effective_unwaived,
    }


def _blockers(report_index: Mapping[str, Any]) -> list[str]:
    summary = _mapping(report_index.get("summary"))
    visibility = _mapping(report_index.get("visibility_audit"))
    effective_unwaived = _effective_unwaived_issue_ids(visibility)
    blockers: list[str] = []
    if effective_unwaived:
        blockers.append(
            "report_index_unwaived_warnings:"
            + ",".join(effective_unwaived)
        )
    if int(summary.get("expired_waiver_count") or 0):
        blockers.append("report_index_expired_waivers")
    if int(summary.get("production_effect_risk_count") or 0):
        blockers.append("report_index_production_effect_risk")
    return blockers


def _effective_unwaived_issue_ids(visibility: Mapping[str, Any]) -> list[str]:
    self_generated_prefixes = (
        "canonical_system_status_",
        "canonical_system_doctor_",
    )
    return [
        issue_id
        for issue_id in _strings(visibility.get("unwaived_issue_ids"))
        if not issue_id.startswith(self_generated_prefixes)
    ]


def _status_warnings(
    *,
    report_index: Mapping[str, Any],
    surface_inventory: Mapping[str, Any],
    reader_brief_consistency: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    report_summary = _mapping(report_index.get("summary"))
    if int(report_summary.get("explicit_waiver_count") or 0):
        warnings.append(f"explicit_waivers={report_summary.get('explicit_waiver_count')}")
    inventory_summary = _mapping(surface_inventory.get("summary"))
    if int(inventory_summary.get("warning_count") or 0):
        warnings.append(f"engineering_surface_inventory_warnings={inventory_summary.get('warning_count')}")
    reader_summary = _mapping(reader_brief_consistency.get("summary"))
    if int(reader_summary.get("warning_check_count") or 0):
        warnings.append(
            f"reader_brief_consistency_warnings={reader_summary.get('warning_check_count')}"
        )
    return warnings


def _recommended_next_action(
    *,
    state: str,
    research_snapshot: Mapping[str, Any],
    warnings: Sequence[str],
) -> str:
    if state == BLOCKED_STATUS:
        return "run_aits_system_doctor_and_resolve_blocking_checks"
    reader_brief = _mapping(research_snapshot.get("reader_brief"))
    next_action = _text(
        reader_brief.get("next_action"),
        _text(research_snapshot.get("next_action")),
    )
    if next_action:
        return next_action
    if warnings:
        return "review_documented_limitations_before_platform_freeze"
    return "continue_engineering_closeout_sequence"


def _health_from_reports(
    report_lookup: Mapping[str, Mapping[str, Any]],
    report_ids: Sequence[str],
) -> str:
    records = [report_lookup.get(report_id) for report_id in report_ids]
    records = [record for record in records if record is not None]
    if not records:
        return "UNKNOWN"
    bad = [
        record
        for record in records
        if _text(record.get("visibility_status")) == "WARNING"
        or _text(record.get("artifact_status")).upper() in {"FAIL", "FAILED", "BLOCKED"}
    ]
    if bad:
        return "NEEDS_REVIEW"
    waived = [record for record in records if _text(record.get("visibility_status")) == "WAIVED"]
    if waived:
        return "PASS_WITH_EXPLICIT_WAIVERS"
    return "PASS"


def _artifact_lifecycle_summary(reports: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts = {"CURRENT": 0, "SUPERSEDED": 0, "ARCHIVED": 0, "INVALID": 0, "LEGACY": 0}
    for report in reports:
        status = _lifecycle_status(report)
        counts[status] = counts.get(status, 0) + 1
    return {
        "lifecycle_status_counts": counts,
        "policy": {
            "CURRENT": "Visible current artifact for the requested as-of date.",
            "SUPERSEDED": "Stale artifact retained for audit.",
            "ARCHIVED": "Missing optional artifact with explicit waiver.",
            "INVALID": "Unsafe production effect or required missing source.",
            "LEGACY": "Explicit waiver applies; not primary evidence.",
        },
    }


def _lifecycle_status(report: Mapping[str, Any]) -> str:
    if bool(report.get("artifact_production_effect_risk")):
        return "INVALID"
    visibility = _text(report.get("visibility_status"))
    freshness = _text(report.get("freshness_status"))
    if visibility == "WAIVED":
        return "LEGACY"
    if freshness == "MISSING" and bool(report.get("required_for_daily_reading")):
        return "INVALID"
    if freshness == "MISSING":
        return "ARCHIVED"
    if freshness == "STALE":
        return "SUPERSEDED"
    return "CURRENT"


def _schema_config_summary(project_root: Path) -> dict[str, Any]:
    config_root = project_root / "config"
    files = sorted(
        path
        for path in config_root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".yaml", ".yml", ".json", ".toml"}
    )
    schema_present = 0
    schema_missing = 0
    for path in files:
        text = path.read_text(encoding="utf-8", errors="ignore")[:1000]
        if "schema_version" in text:
            schema_present += 1
        else:
            schema_missing += 1
    return {
        "configuration_file_count": len(files),
        "schema_version_visible_count": schema_present,
        "schema_version_missing_count": schema_missing,
        "profile_support_status": "PARTIAL_CONFIG_INVENTORY",
    }


def _reproducibility_summary(project_root: Path) -> dict[str, Any]:
    daily_manifests = list((project_root / "outputs").glob("daily/**/manifest.json"))
    validation_manifests = list(
        (project_root / "outputs" / "validation_runtime").glob("*/test_runtime_summary.json")
    )
    return {
        "daily_run_manifest_count": len(daily_manifests),
        "validation_runtime_manifest_count": len(validation_manifests),
        "required_manifest_fields": [
            "git commit",
            "command",
            "resolved config",
            "input artifact checksums",
            "schema versions",
            "as-of date",
            "environment summary",
            "output artifacts",
            "elapsed time",
            "warnings",
        ],
        "doctor_status": "AVAILABLE_AS_CANONICAL_STATUS_SECTION",
    }


def _test_suite_summary() -> dict[str, Any]:
    return {
        "suite_ids": [
            "fast-unit",
            "contract-validation",
            "report-validation",
            "integration",
            "slow-research-regression",
            "full",
        ],
        "command": "python scripts/run_validation_tier.py <suite> --write-runtime-artifact",
        "runtime_budget_status": "DEFINED_BY_TRADING_347",
        "production_effect": PRODUCTION_EFFECT,
    }


def _documentation_summary(project_root: Path) -> dict[str, Any]:
    docs_root = project_root / "docs"
    return {
        "entrypoints": [
            "README.md",
            "docs/system_flow.md",
            "docs/artifact_catalog.md",
            "docs/operations/operations_runbook.md",
            "docs/task_register.md",
        ],
        "requirements_doc_count": len(list((docs_root / "requirements").glob("*.md"))),
        "operations_runbook_exists": (docs_root / "operations" / "operations_runbook.md").exists(),
    }


def _source_artifacts(
    *,
    project_root: Path,
    report_index: Mapping[str, Any],
    surface_inventory: Mapping[str, Any],
    reader_brief_consistency: Mapping[str, Any],
    research_snapshot: Mapping[str, Any],
    research_gate: Mapping[str, Any],
    registry_path: Path,
) -> list[dict[str, Any]]:
    artifacts = [
        _artifact_record("report_registry", registry_path, "configured"),
        _report_index_source_record(report_index, project_root),
        _payload_record("engineering_surface_inventory", surface_inventory),
        _payload_record("reader_brief_consistency_validation", reader_brief_consistency),
        _payload_record("research_cycle_snapshot", research_snapshot),
        _payload_record("research_gate", research_gate),
    ]
    return [artifact for artifact in artifacts if artifact["path"] or artifact["status"]]


def _report_index_source_record(
    report_index: Mapping[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    path = ""
    try:
        path = str(
            default_report_index_json_path(
                project_root / "outputs" / "reports",
                date.fromisoformat(_text(report_index.get("as_of"))),
            )
        )
    except ValueError:
        path = ""
    return {
        "artifact_id": "report_index",
        "path": path,
        "status": _text(report_index.get("status"), "UNKNOWN"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _payload_record(artifact_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    path = ""
    input_artifacts = _mapping(payload.get("input_artifacts"))
    if artifact_id in input_artifacts:
        path = _text(input_artifacts.get(artifact_id))
    if not path:
        report_type = _text(payload.get("report_type"))
        if report_type in input_artifacts:
            path = _text(input_artifacts.get(report_type))
    return {
        "artifact_id": artifact_id,
        "path": path,
        "status": _text(payload.get("status"), _text(payload.get("validation_status"))),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
    }


def _artifact_record(artifact_id: str, path: Path | None, status: str) -> dict[str, Any]:
    return {
        "artifact_id": artifact_id,
        "path": "" if path is None else str(path),
        "status": status if path is not None and path.exists() else "MISSING",
        "production_effect": PRODUCTION_EFFECT,
    }


def _latest_payload(
    report_lookup: Mapping[str, Mapping[str, Any]],
    report_ids: Sequence[str],
    *,
    project_root: Path,
) -> dict[str, Any]:
    for report_id in report_ids:
        payload = _load_json_from_record(report_lookup.get(report_id), project_root=project_root)
        if payload:
            return payload
    return {}


def _load_json_from_record(
    record: Mapping[str, Any] | None,
    *,
    fallback: Path | None = None,
    project_root: Path,
) -> dict[str, Any]:
    path = _report_path(record, project_root) if record else None
    if path is None or not path.exists():
        path = fallback
    if path is not None and path.exists() and path.suffix.lower() != ".json":
        json_sibling = path.with_suffix(".json")
        if json_sibling.exists():
            path = json_sibling
    if path is None or not path.exists() or path.suffix.lower() != ".json":
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, Mapping):
        return {}
    result = dict(payload)
    result.setdefault("input_artifacts", {})
    if isinstance(result["input_artifacts"], dict):
        result["input_artifacts"].setdefault(_text(result.get("report_type"), "source"), str(path))
    return result


def _report_path(record: Mapping[str, Any] | None, project_root: Path) -> Path | None:
    if record is None:
        return None
    raw = _text(record.get("latest_artifact_path"))
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_absolute() else project_root / path


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    *,
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    status = PASS_STATUS if passed else WARN_STATUS if severity == "WARNING" else FAIL_STATUS
    checks.append(
        {
            "check_id": check_id,
            "status": status,
            "severity": severity,
            "message": message,
            "recommended_action": recommended_action,
            "details": {} if details is None else dict(details),
        }
    )
    if passed:
        return
    issues.append(
        {
            "issue_id": check_id,
            "severity": severity,
            "message": message,
            "recommended_action": recommended_action,
            "details": {} if details is None else dict(details),
        }
    )


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    if not output_dir.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in output_dir.glob(f"{prefix}*{suffix}"):
        raw = path.stem.removeprefix(prefix)
        try:
            parsed = date.fromisoformat(raw)
        except ValueError:
            continue
        candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].name))[1]


def _dedupe_issues(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for record in records:
        key = _text(record.get("issue_id"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(record))
    return deduped


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item not in {"", None}]


def _text(value: Any, default: str = "") -> str:
    if value is None or value == "":
        return default
    return str(value)


def _markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")
