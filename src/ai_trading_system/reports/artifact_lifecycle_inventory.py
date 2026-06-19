from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from glob import glob
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "artifact_lifecycle_inventory"
VALIDATION_REPORT_TYPE = "artifact_lifecycle_inventory_validation"
PRODUCTION_EFFECT = "none"

READY_STATUS = "ARTIFACT_LIFECYCLE_READY"
READY_WITH_LIMITATIONS_STATUS = "ARTIFACT_LIFECYCLE_READY_WITH_LIMITATIONS"
BLOCKED_STATUS = "ARTIFACT_LIFECYCLE_BLOCKED"

PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

CURRENT = "CURRENT"
SUPERSEDED = "SUPERSEDED"
ARCHIVED = "ARCHIVED"
INVALID = "INVALID"
LEGACY = "LEGACY"
LIFECYCLE_STATUSES = frozenset({CURRENT, SUPERSEDED, ARCHIVED, INVALID, LEGACY})
SAFE_PRODUCTION_EFFECTS = frozenset({"", "none", "read_only", "advisory"})


def default_artifact_lifecycle_inventory_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"artifact_lifecycle_inventory_{as_of.isoformat()}.json"


def default_artifact_lifecycle_inventory_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"artifact_lifecycle_inventory_{as_of.isoformat()}.md"


def default_artifact_lifecycle_inventory_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"artifact_lifecycle_inventory_validation_{as_of.isoformat()}.json"


def default_artifact_lifecycle_inventory_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"artifact_lifecycle_inventory_validation_{as_of.isoformat()}.md"


def latest_artifact_lifecycle_inventory_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "artifact_lifecycle_inventory_", ".json")


def build_artifact_lifecycle_inventory_payload(
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
    records = [
        _lifecycle_record(report, project_root=project_root, as_of=as_of)
        for report in _records(report_index.get("reports"))
    ]
    status = _inventory_status(report_index=report_index, records=records)
    summary = _summary(report_index=report_index, records=records)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "inventory_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": "not_applicable_artifact_lifecycle_inventory",
        "purpose": (
            "Expose report artifact lifecycle, latest pointer, retention, and "
            "archive readiness without moving or deleting artifacts."
        ),
        "input_artifacts": {
            "report_registry": str(registry_path),
            "report_index_visibility_waivers": "" if waiver_path is None else str(waiver_path),
            "report_index_status": _text(report_index.get("status"), "UNKNOWN"),
        },
        "lifecycle_policy": _lifecycle_policy(),
        "latest_pointer_policy": _latest_pointer_policy(),
        "retention_policy": _retention_policy(),
        "summary": summary,
        "lifecycle_status_counts": dict(Counter(record["lifecycle_status"] for record in records)),
        "latest_pointer_summary": {
            "pointer_source": "report_index",
            "selection_policy": "registry_artifact_globs_with_report_index_selection",
            "report_count": len(records),
            "records_with_latest_artifact": len(
                [record for record in records if record["latest_artifact_path"]]
            ),
            "records_without_latest_artifact": len(
                [record for record in records if not record["latest_artifact_path"]]
            ),
            "superseded_artifact_count": sum(
                int(record["superseded_artifact_count"]) for record in records
            ),
            "archived_artifact_count": sum(
                int(record["archived_artifact_count"]) for record in records
            ),
        },
        "artifact_records": records,
        "blocking_issues": _blocking_issues(report_index=report_index, records=records),
        "warning_issues": _warning_issues(report_index=report_index, records=records),
        "reader_brief": {
            "summary": (
                f"Artifact lifecycle inventory is {status}; "
                f"report_index={_text(report_index.get('status'), 'UNKNOWN')}; "
                f"unwaived={summary['report_index_unwaived_issue_count']}."
            ),
            "key_result": status,
            "blocking_issues": (
                "; ".join(issue["issue_id"] for issue in _blocking_issues(
                    report_index=report_index,
                    records=records,
                ))
                or "none"
            ),
            "next_action": _next_action(status),
        },
        "safety_boundary": _safety_boundary(),
        "methodology": {
            "mode": "read_existing_report_index_and_artifact_globs_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_move_or_delete_artifacts": True,
            "does_not_write_latest_pointers": True,
            "does_not_modify_strategy_logic": True,
            "does_not_modify_research_decisions": True,
            "does_not_activate_paper_shadow": True,
            "does_not_generate_official_target_weights": True,
            "does_not_touch_broker_or_orders": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_artifact_lifecycle_inventory_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    records = _records(payload.get("artifact_records"))
    summary = _mapping(payload.get("summary"))
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    lifecycle_values = [_text(record.get("lifecycle_status")) for record in records]
    unknown_lifecycle = [
        value for value in lifecycle_values if value not in LIFECYCLE_STATUSES
    ]
    unsafe_records = [
        record
        for record in records
        if _text(record.get("artifact_production_effect")) not in SAFE_PRODUCTION_EFFECTS
        or bool(record.get("artifact_production_effect_risk"))
    ]
    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        severity="BLOCKING",
        message="payload must be artifact_lifecycle_inventory.",
        recommended_action="regenerate_artifact_lifecycle_inventory",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect_none",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        severity="BLOCKING",
        message="artifact lifecycle inventory must be read-only.",
        recommended_action="restore_production_effect_none",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="allowed_lifecycle_statuses",
        passed=not unknown_lifecycle,
        severity="BLOCKING",
        message="every artifact record must use the reviewed lifecycle taxonomy.",
        recommended_action="map_unknown_lifecycle_statuses_before_freeze",
        details={"unknown_lifecycle_statuses": unknown_lifecycle},
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="report_index_unwaived_zero",
        passed=int(summary.get("report_index_unwaived_issue_count") or 0) == 0,
        severity="BLOCKING",
        message="latest pointer inventory cannot be canonical with unwaived report-index issues.",
        recommended_action="refresh_artifacts_or_add_reviewed_visibility_waiver",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="report_index_expired_waivers_zero",
        passed=int(summary.get("report_index_expired_waiver_count") or 0) == 0,
        severity="BLOCKING",
        message="latest pointer inventory cannot depend on expired waivers.",
        recommended_action="renew_or_remove_expired_report_index_waivers",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="safe_artifact_production_effects",
        passed=not unsafe_records,
        severity="BLOCKING",
        message="artifact lifecycle inventory must not bless production-mutating artifacts.",
        recommended_action="investigate_unsafe_artifact_production_effects",
        details={
            "report_ids": [_text(record.get("report_id")) for record in unsafe_records],
        },
    )
    _append_check(
        checks,
        warning_issues,
        check_id="limitations_visible",
        passed=_text(payload.get("inventory_status"), _text(payload.get("status")))
        == READY_STATUS,
        severity="WARNING",
        message="artifact lifecycle inventory has documented limitations.",
        recommended_action="review_legacy_missing_and_stale_artifact_families",
        details={
            "legacy_count": int(summary.get("legacy_count") or 0),
            "invalid_count": int(summary.get("invalid_count") or 0),
            "explicit_waiver_count": int(summary.get("report_index_explicit_waiver_count") or 0),
        },
    )
    _append_check(
        checks,
        warning_issues,
        check_id="superseded_artifacts_visible",
        passed=int(summary.get("superseded_artifact_count") or 0) == 0,
        severity="WARNING",
        message="superseded artifact count should be reviewed before a freeze release.",
        recommended_action="review_retention_policy_before_archive_cleanup",
        details={"superseded_artifact_count": summary.get("superseded_artifact_count")},
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
        "source_inventory_status": _text(
            payload.get("inventory_status"),
            _text(payload.get("status")),
        ),
        "input_artifacts": {
            "artifact_lifecycle_inventory": _text(
                _mapping(payload.get("input_artifacts")).get("artifact_lifecycle_inventory")
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
            "artifact_record_count": len(records),
            "blocking_issue_count": len(blocking_issues),
            "warning_issue_count": len(warning_issues),
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": f"Artifact lifecycle inventory validation is {status}.",
            "key_result": status,
            "blocking_issues": (
                "; ".join(issue["issue_id"] for issue in blocking_issues) or "none"
            ),
            "next_action": (
                "resolve_artifact_lifecycle_validation_blockers"
                if blocking_issues
                else "review_artifact_lifecycle_warnings"
                if warning_issues
                else "use_artifact_lifecycle_inventory_for_a4_closeout"
            ),
        },
        "safety_boundary": _safety_boundary(),
    }


def write_artifact_lifecycle_inventory_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_artifact_lifecycle_inventory_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_artifact_lifecycle_inventory_markdown(payload), encoding="utf-8")
    return output_path


def write_artifact_lifecycle_inventory_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_artifact_lifecycle_inventory_json(payload, output_path)


def write_artifact_lifecycle_inventory_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_artifact_lifecycle_inventory_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_artifact_lifecycle_inventory_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Artifact Lifecycle Inventory {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- status: {_text(payload.get('inventory_status'), _text(payload.get('status')))}",
        f"- report_count: {summary.get('report_count')}",
        f"- current_count: {summary.get('current_count')}",
        f"- legacy_count: {summary.get('legacy_count')}",
        f"- invalid_count: {summary.get('invalid_count')}",
        f"- superseded_artifact_count: {summary.get('superseded_artifact_count')}",
        f"- archived_artifact_count: {summary.get('archived_artifact_count')}",
        f"- report_index_unwaived_issue_count: {summary.get('report_index_unwaived_issue_count')}",
        "",
        "## Lifecycle Policy",
        "",
        "|status|meaning|default visibility|",
        "|---|---|---|",
    ]
    for item in _records(payload.get("lifecycle_policy")):
        lines.append(
            f"|{_md_cell(item.get('status'))}|"
            f"{_md_cell(item.get('meaning'))}|"
            f"{_md_cell(item.get('default_visibility'))}|"
        )
    lines.extend(
        [
            "",
            "## Artifact Records",
            "",
            "|report_id|lifecycle|freshness|visibility|latest|superseded|archived|",
            "|---|---|---|---|---|---:|---:|",
        ]
    )
    for record in _records(payload.get("artifact_records")):
        lines.append(
            f"|{_md_cell(record.get('report_id'))}|"
            f"{_md_cell(record.get('lifecycle_status'))}|"
            f"{_md_cell(record.get('freshness_status'))}|"
            f"{_md_cell(record.get('visibility_status'))}|"
            f"{_md_cell(record.get('latest_artifact_name'))}|"
            f"{record.get('superseded_artifact_count', 0)}|"
            f"{record.get('archived_artifact_count', 0)}|"
        )
    lines.extend(["", "## Safety Boundary", ""])
    safety = _mapping(payload.get("safety_boundary"))
    for key in sorted(safety):
        lines.append(f"- {key}: {safety[key]}")
    lines.append("")
    return "\n".join(lines)


def render_artifact_lifecycle_inventory_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    lines = [
        f"# Artifact Lifecycle Inventory Validation {payload.get('as_of')}",
        "",
        f"- status: {_text(payload.get('validation_status'), _text(payload.get('status')))}",
        f"- production_effect: {_text(payload.get('production_effect'))}",
        "",
        "## Checks",
        "",
        "|check_id|status|severity|message|recommended_action|",
        "|---|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_md_cell(check.get('check_id'))}|"
            f"{_md_cell(check.get('status'))}|"
            f"{_md_cell(check.get('severity'))}|"
            f"{_md_cell(check.get('message'))}|"
            f"{_md_cell(check.get('recommended_action'))}|"
        )
    lines.append("")
    return "\n".join(lines)


def _lifecycle_record(
    report: Mapping[str, Any],
    *,
    project_root: Path,
    as_of: date,
) -> dict[str, Any]:
    latest_path = _resolve_path(_text(report.get("latest_artifact_path")), project_root)
    candidates = _candidate_paths(_strings(report.get("artifact_globs")), project_root)
    latest_resolved = latest_path.resolve() if latest_path is not None else None
    archived_candidates = [path for path in candidates if _is_archived_path(path)]
    superseded_candidates = [
        path
        for path in candidates
        if latest_resolved is None or path.resolve() != latest_resolved
    ]
    artifact_dates = [
        parsed for parsed in (_date_from_path(path) for path in candidates) if parsed is not None
    ]
    lifecycle_status = _lifecycle_status(report, latest_path)
    return {
        "report_id": _text(report.get("report_id")),
        "title": _text(report.get("title")),
        "group": _text(report.get("group")),
        "cadence": _text(report.get("cadence")),
        "owner": _text(report.get("owner")),
        "command": _text(report.get("command")),
        "lifecycle_status": lifecycle_status,
        "latest_pointer_source": "report_index",
        "artifact_selection_policy": _text(report.get("artifact_selection_policy")),
        "freshness_status": _text(report.get("freshness_status")),
        "visibility_status": _text(report.get("visibility_status")),
        "visibility_issue_id": _text(_mapping(report.get("visibility_issue")).get("issue_id")),
        "visibility_waiver_id": _text(_mapping(report.get("visibility_waiver")).get("waiver_id")),
        "latest_artifact_path": "" if latest_path is None else str(latest_path),
        "latest_artifact_name": _text(report.get("latest_artifact_name")),
        "latest_artifact_exists": bool(report.get("exists")),
        "latest_artifact_status": _text(report.get("artifact_status")),
        "latest_artifact_date": _text(report.get("artifact_date")),
        "latest_artifact_after_as_of": bool(report.get("artifact_after_as_of")),
        "artifact_production_effect": _text(report.get("artifact_production_effect")),
        "artifact_production_effect_risk": bool(report.get("artifact_production_effect_risk")),
        "candidate_artifact_count": len(candidates),
        "superseded_artifact_count": len(superseded_candidates),
        "archived_artifact_count": len(archived_candidates),
        "oldest_artifact_date": "" if not artifact_dates else min(artifact_dates).isoformat(),
        "newest_artifact_date": "" if not artifact_dates else max(artifact_dates).isoformat(),
        "default_visibility": _default_visibility(lifecycle_status),
        "retention_action": _retention_action(lifecycle_status),
        "production_effect": PRODUCTION_EFFECT,
    }


def _lifecycle_status(report: Mapping[str, Any], latest_path: Path | None) -> str:
    if bool(report.get("artifact_production_effect_risk")) or bool(
        report.get("artifact_after_as_of")
    ):
        return INVALID
    if _text(report.get("visibility_status")) == "WARNING":
        return INVALID
    if latest_path is None or not bool(report.get("exists")):
        return INVALID
    if _is_archived_path(latest_path):
        return ARCHIVED
    freshness = _text(report.get("freshness_status"))
    if freshness == "FRESH":
        return CURRENT
    if freshness == "STALE":
        return LEGACY
    return CURRENT


def _inventory_status(
    *,
    report_index: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> str:
    if _blocking_issues(report_index=report_index, records=records):
        return BLOCKED_STATUS
    if _warning_issues(report_index=report_index, records=records):
        return READY_WITH_LIMITATIONS_STATUS
    return READY_STATUS


def _summary(
    *,
    report_index: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    counts = Counter(_text(record.get("lifecycle_status")) for record in records)
    report_summary = _mapping(report_index.get("summary"))
    visibility = _mapping(report_index.get("visibility_audit"))
    superseded_count = sum(int(record.get("superseded_artifact_count") or 0) for record in records)
    archived_count = sum(int(record.get("archived_artifact_count") or 0) for record in records)
    return {
        "report_count": len(records),
        "current_count": counts.get(CURRENT, 0),
        "superseded_latest_count": counts.get(SUPERSEDED, 0),
        "archived_latest_count": counts.get(ARCHIVED, 0),
        "invalid_count": counts.get(INVALID, 0),
        "legacy_count": counts.get(LEGACY, 0),
        "candidate_artifact_count": sum(
            int(record.get("candidate_artifact_count") or 0) for record in records
        ),
        "superseded_artifact_count": superseded_count,
        "archived_artifact_count": archived_count,
        "report_index_status": _text(report_index.get("status"), "UNKNOWN"),
        "report_index_report_count": int(report_summary.get("report_count") or 0),
        "report_index_missing_count": int(report_summary.get("missing_count") or 0),
        "report_index_stale_count": int(report_summary.get("stale_count") or 0),
        "report_index_explicit_waiver_count": int(
            report_summary.get("explicit_waiver_count") or 0
        ),
        "report_index_expired_waiver_count": int(report_summary.get("expired_waiver_count") or 0),
        "report_index_raw_unwaived_issue_count": len(
            _strings(visibility.get("unwaived_issue_ids"))
        ),
        "report_index_unwaived_issue_count": len(_effective_unwaived_issue_ids(visibility)),
        "production_effect_risk_count": int(
            report_summary.get("production_effect_risk_count") or 0
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _blocking_issues(
    *,
    report_index: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    summary = _summary(report_index=report_index, records=records)
    issues: list[dict[str, Any]] = []
    if summary["report_index_unwaived_issue_count"]:
        issues.append(
            {
                "issue_id": "report_index_unwaived_warnings",
                "severity": "BLOCKING",
                "details": _effective_unwaived_issue_ids(
                    _mapping(report_index.get("visibility_audit"))
                ),
            }
        )
    if summary["report_index_expired_waiver_count"]:
        issues.append({"issue_id": "report_index_expired_waivers", "severity": "BLOCKING"})
    if summary["production_effect_risk_count"]:
        issues.append({"issue_id": "artifact_production_effect_risk", "severity": "BLOCKING"})
    unknown_records = [
        record
        for record in records
        if _text(record.get("lifecycle_status")) not in LIFECYCLE_STATUSES
    ]
    if unknown_records:
        issues.append(
            {
                "issue_id": "unknown_lifecycle_status",
                "severity": "BLOCKING",
                "details": [_text(record.get("report_id")) for record in unknown_records],
            }
        )
    return issues


def _warning_issues(
    *,
    report_index: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    summary = _summary(report_index=report_index, records=records)
    issues: list[dict[str, Any]] = []
    if summary["report_index_explicit_waiver_count"]:
        issues.append(
            {
                "issue_id": "explicit_visibility_waivers_present",
                "severity": "WARNING",
                "count": summary["report_index_explicit_waiver_count"],
            }
        )
    if summary["invalid_count"]:
        issues.append(
            {
                "issue_id": "invalid_or_missing_latest_artifacts_present",
                "severity": "WARNING",
                "count": summary["invalid_count"],
            }
        )
    if summary["legacy_count"]:
        issues.append(
            {
                "issue_id": "legacy_stale_latest_artifacts_present",
                "severity": "WARNING",
                "count": summary["legacy_count"],
            }
        )
    if summary["superseded_artifact_count"]:
        issues.append(
            {
                "issue_id": "superseded_artifacts_require_retention_review",
                "severity": "WARNING",
                "count": summary["superseded_artifact_count"],
            }
        )
    return issues


def _lifecycle_policy() -> list[dict[str, str]]:
    return [
        {
            "status": CURRENT,
            "meaning": "Latest selected artifact is fresh and visible in report index.",
            "default_visibility": "show",
        },
        {
            "status": SUPERSEDED,
            "meaning": "Older artifact exists under the same report family but is not latest.",
            "default_visibility": "hide_by_default_link_from_lineage",
        },
        {
            "status": ARCHIVED,
            "meaning": "Artifact path is explicitly under an archive location.",
            "default_visibility": "hide_by_default_keep_reproducible",
        },
        {
            "status": INVALID,
            "meaning": (
                "Latest artifact is missing, unsafe, after as_of, "
                "or has unwaived visibility risk."
            ),
            "default_visibility": "show_as_blocker_or_warning",
        },
        {
            "status": LEGACY,
            "meaning": (
                "Latest artifact exists but is stale/waived and should not be "
                "first-screen default."
            ),
            "default_visibility": "hide_by_default_show_warning",
        },
    ]


def _latest_pointer_policy() -> dict[str, Any]:
    return {
        "pointer_source": "report_index",
        "selection_inputs": [
            "config/report_registry.yaml artifact_globs",
            "artifact date parsed from path",
            "file modified time tie-breaker",
            "report index visibility waivers",
        ],
        "does_not_guess_manual_filenames": True,
        "query_command": "aits reports latest --report-id REPORT_ID --as-of YYYY-MM-DD",
        "production_effect": PRODUCTION_EFFECT,
    }


def _retention_policy() -> dict[str, Any]:
    return {
        "current": "show as latest",
        "superseded": "retain for audit and reproduction; hide by default",
        "archived": "retain checksums and lineage; do not delete during this report",
        "invalid": "surface as warning/blocker; repair upstream",
        "legacy": "retain and hide by default unless explicitly requested",
        "this_report_moves_or_deletes_files": False,
        "production_effect": PRODUCTION_EFFECT,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_report_index_and_artifact_paths_only",
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_move_or_delete_artifacts": True,
        "does_not_archive_files": True,
        "does_not_modify_strategy_logic": True,
        "does_not_modify_research_decisions": True,
        "does_not_activate_paper_shadow": True,
        "does_not_generate_official_target_weights": True,
        "does_not_touch_broker_or_orders": True,
        "does_not_modify_production": True,
        "production_effect": PRODUCTION_EFFECT,
    }


def _effective_unwaived_issue_ids(visibility: Mapping[str, Any]) -> list[str]:
    self_generated_prefixes = (
        "artifact_lifecycle_inventory_",
        "artifact_lifecycle_inventory_validation_",
    )
    return [
        issue_id
        for issue_id in _strings(visibility.get("unwaived_issue_ids"))
        if not issue_id.startswith(self_generated_prefixes)
    ]


def _candidate_paths(patterns: Sequence[str], project_root: Path) -> list[Path]:
    paths: list[Path] = []
    seen: set[Path] = set()
    for raw_pattern in patterns:
        pattern_path = Path(raw_pattern)
        pattern = str(pattern_path if pattern_path.is_absolute() else project_root / pattern_path)
        for raw_path in glob(pattern):
            path = Path(raw_path)
            if not path.is_file():
                continue
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            paths.append(path)
    return sorted(paths, key=lambda path: str(path))


def _resolve_path(raw: str, project_root: Path) -> Path | None:
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_absolute() else project_root / path


def _is_archived_path(path: Path) -> bool:
    parts = {part.lower() for part in path.parts}
    return "archive" in parts or "archived" in parts


def _date_from_path(path: Path) -> date | None:
    match = re.search(r"(20\d{2}-\d{2}-\d{2})", " ".join([path.name, path.parent.name]))
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def _default_visibility(lifecycle_status: str) -> str:
    for item in _lifecycle_policy():
        if item["status"] == lifecycle_status:
            return item["default_visibility"]
    return "review_required"


def _retention_action(lifecycle_status: str) -> str:
    if lifecycle_status == CURRENT:
        return "keep_as_latest"
    if lifecycle_status == SUPERSEDED:
        return "retain_for_audit_hide_by_default"
    if lifecycle_status == ARCHIVED:
        return "retain_archive_do_not_delete"
    if lifecycle_status == LEGACY:
        return "retain_legacy_hide_by_default"
    return "repair_or_waive_upstream_visibility_issue"


def _next_action(status: str) -> str:
    if status == BLOCKED_STATUS:
        return "resolve_artifact_lifecycle_blockers_before_platform_freeze"
    if status == READY_WITH_LIMITATIONS_STATUS:
        return "review_lifecycle_limitations_before_a4_signoff"
    return "use_artifact_lifecycle_inventory_for_a4_signoff"


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
            "details": dict(details or {}),
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
            "details": dict(details or {}),
        }
    )


def _dedupe_issues(issues: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for issue in issues:
        issue_id = _text(issue.get("issue_id"))
        if issue_id in seen:
            continue
        seen.add(issue_id)
        result.append(dict(issue))
    return result


def _latest_dated_path(directory: Path, prefix: str, suffix: str) -> Path | None:
    paths = [path for path in directory.glob(f"{prefix}*{suffix}") if path.is_file()]
    if not paths:
        return None
    return max(paths, key=lambda path: (path.stat().st_mtime, path.name))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _md_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", "<br/>")
