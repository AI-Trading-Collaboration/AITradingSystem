from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.reports.report_index import (
    ACTIVE_WAIVER_REVIEW_STATUS,
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    PRODUCTION_EFFECT,
    load_report_index_visibility_waivers,
    load_report_registry,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "report_index_waiver_inventory"
VALIDATION_REPORT_TYPE = "report_index_waiver_inventory_validation"

PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

# Operational review window only; it is a warning, not an investment threshold.
EXPIRING_SOON_DAYS = 14


def default_waiver_inventory_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_waiver_inventory_{as_of.isoformat()}.json"


def default_waiver_inventory_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_waiver_inventory_{as_of.isoformat()}.md"


def default_waiver_inventory_validation_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_waiver_inventory_validation_{as_of.isoformat()}.json"


def default_waiver_inventory_validation_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_waiver_inventory_validation_{as_of.isoformat()}.md"


def latest_waiver_inventory_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "report_index_waiver_inventory_", ".json")


def build_waiver_inventory_payload(
    *,
    as_of: date,
    waiver_path: Path = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
) -> dict[str, Any]:
    policy = load_report_index_visibility_waivers(waiver_path)
    registry = load_report_registry(registry_path)
    registry_ids = {
        _text(entry.get("report_id"))
        for entry in _records(registry.get("reports"))
        if _text(entry.get("report_id"))
    }
    waivers = _expanded_waivers(policy)
    inventory = [
        _inventory_record(waiver, as_of=as_of, registry_ids=registry_ids)
        for waiver in waivers
    ]
    expired = [item for item in inventory if item["expired"] is True]
    missing_registry = [item for item in inventory if item["registry_entry_exists"] is False]
    expiring_soon = [item for item in inventory if item["expiring_soon"] is True]
    inactive = [
        item
        for item in inventory
        if item["review_status"] != ACTIVE_WAIVER_REVIEW_STATUS
    ]
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    for item in expired:
        blocking_issues.append(
            _issue(
                "expired_waiver",
                item,
                "Waiver is expired and must not clear report-index warnings.",
                "renew_or_remove_expired_report_index_waiver",
            )
        )
    for item in missing_registry:
        blocking_issues.append(
            _issue(
                "missing_report_registry_entry",
                item,
                "Waiver references a report_id that is not in report_registry.",
                "remove_waiver_or_restore_report_registry_entry",
            )
        )
    for item in expiring_soon:
        warning_issues.append(
            _issue(
                "waiver_expiring_soon",
                item,
                "Waiver expires soon and needs owner review.",
                "review_waiver_before_expiry",
            )
        )
    for item in inactive:
        warning_issues.append(
            _issue(
                "inactive_review_status",
                item,
                "Waiver review_status is not approved_active.",
                "complete_owner_review_before_using_waiver",
            )
        )

    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    summary = {
        "configured_waiver_count": len(_records(policy.get("waivers"))),
        "expanded_waiver_count": len(inventory),
        "active_waiver_count": len(
            [
                item
                for item in inventory
                if item["active"] is True and item["registry_entry_exists"] is True
            ]
        ),
        "expired_waiver_count": len(expired),
        "expiring_soon_waiver_count": len(expiring_soon),
        "inactive_waiver_count": len(inactive),
        "missing_registry_entry_count": len(missing_registry),
        "blocking_issue_count": len(blocking_issues),
        "warning_issue_count": len(warning_issues),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "inventory_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": "Inventory current report-index visibility waivers and expiry state.",
        "input_artifacts": {
            "waiver_policy": str(waiver_path),
            "report_registry": str(registry_path),
        },
        "output_decision": status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Inventory reads waiver policy and report registry only.",
            "It does not run report index, regenerate artifacts, or clear warnings.",
        ],
        "next_action": _next_action(status),
        "summary": summary,
        "waiver_policy": {
            "path": str(waiver_path),
            "policy_id": _text(policy.get("policy_id")),
            "policy_metadata": _mapping(policy.get("policy_metadata")),
        },
        "waivers": inventory,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(status, summary, blocking_issues, warning_issues),
        "methodology": {
            "mode": "read_existing_waiver_policy_only",
            "expiry_is_inclusive_through_expires_at": True,
            "expiring_soon_days": EXPIRING_SOON_DAYS,
            "does_not_run_upstream_commands": True,
            "does_not_modify_reports": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_waiver_inventory_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    source_status = _text(payload.get("inventory_status"), _text(payload.get("status")))
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        "BLOCKING",
        f"report_type must be {REPORT_TYPE}.",
        "rerun_waiver_inventory_with_supported_report_type",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "BLOCKING",
        "Waiver inventory must be production_effect=none.",
        "regenerate_inventory_without_production_mutation",
    )
    _append_check(
        checks,
        blocking_issues,
        "source_not_fail",
        source_status != FAIL_STATUS,
        "BLOCKING",
        f"source inventory_status is {source_status}.",
        "fix_waiver_inventory_blockers",
    )
    _append_check(
        checks,
        blocking_issues,
        "expired_waivers_absent",
        _int(summary.get("expired_waiver_count")) == 0,
        "BLOCKING",
        "Expired waivers must fail validation.",
        "renew_or_remove_expired_report_index_waivers",
    )
    _append_check(
        checks,
        blocking_issues,
        "missing_registry_entries_absent",
        _int(summary.get("missing_registry_entry_count")) == 0,
        "BLOCKING",
        "Waivers must reference existing report registry entries.",
        "fix_waiver_report_registry_references",
    )
    _append_check(
        checks,
        warning_issues,
        "expiring_soon_review_visible",
        _int(summary.get("expiring_soon_waiver_count")) == 0,
        "WARNING",
        "Waivers expiring soon should be owner-reviewed.",
        "review_expiring_waivers_before_next_governance_pack",
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
        "purpose": "Validate waiver inventory expiry and report registry references.",
        "input_artifacts": _mapping(payload.get("input_artifacts")),
        "source_inventory_status": source_status,
        "output_decision": status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation reads the generated waiver inventory only.",
            "Validation does not renew, remove, or apply waivers.",
        ],
        "next_action": _next_action(status),
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(
                [check for check in checks if check["status"] == FAIL_STATUS]
            ),
            "warning_check_count": len(
                [check for check in checks if check["status"] == WARN_STATUS]
            ),
            "blocking_issue_count": len(blocking_issues),
            "warning_issue_count": len(warning_issues),
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(status, {}, blocking_issues, warning_issues),
        "methodology": {
            "mode": "read_existing_waiver_inventory_only",
            "does_not_run_report_index": True,
            "does_not_modify_waiver_policy": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_waiver_inventory_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_waiver_inventory_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_waiver_inventory_markdown(payload), encoding="utf-8")
    return output_path


def write_waiver_inventory_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_waiver_inventory_json(payload, output_path)


def write_waiver_inventory_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_waiver_inventory_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_waiver_inventory_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    reader = _mapping(payload.get("reader_brief"))
    lines = [
        f"# Report Index Waiver Inventory {payload.get('as_of')}",
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
        f"- 状态：{_text(payload.get('inventory_status'), 'UNKNOWN')}",
        f"- configured_waiver_count：{summary.get('configured_waiver_count')}",
        f"- expanded_waiver_count：{summary.get('expanded_waiver_count')}",
        f"- active_waiver_count：{summary.get('active_waiver_count')}",
        f"- expired_waiver_count：{summary.get('expired_waiver_count')}",
        f"- expiring_soon_waiver_count：{summary.get('expiring_soon_waiver_count')}",
        f"- missing_registry_entry_count：{summary.get('missing_registry_entry_count')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        "",
        "## Waivers",
        "",
        "|waiver_id|report_id|issue_status|review_status|expires_at|days_until_expiry|status|",
        "|---|---|---|---|---|---|---|",
    ]
    for waiver in _records(payload.get("waivers")):
        lines.append(
            f"|{_markdown_cell(waiver.get('waiver_id'))}|"
            f"{_markdown_cell(waiver.get('report_id'))}|"
            f"{_markdown_cell(waiver.get('issue_status'))}|"
            f"{_markdown_cell(waiver.get('review_status'))}|"
            f"{_markdown_cell(waiver.get('expires_at'))}|"
            f"{_markdown_cell(waiver.get('days_until_expiry'))}|"
            f"{_markdown_cell(waiver.get('waiver_state'))}|"
        )
    if not _records(payload.get("waivers")):
        lines.append("|NONE||||||PASS|")
    lines.extend(["", "## Blocking Issues", "", "|issue_id|waiver_id|message|", "|---|---|---|"])
    for issue in _records(payload.get("blocking_issues")):
        lines.append(
            f"|{_markdown_cell(issue.get('issue_id'))}|"
            f"{_markdown_cell(issue.get('waiver_id'))}|"
            f"{_markdown_cell(issue.get('message'))}|"
        )
    if not _records(payload.get("blocking_issues")):
        lines.append("|NONE||无阻断项。|")
    lines.extend([""])
    return "\n".join(lines)


def render_waiver_inventory_validation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Report Index Waiver Inventory Validation {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('validation_status'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- checks：{summary.get('check_count')}",
        f"- failed_checks：{summary.get('failed_check_count')}",
        f"- warnings：{summary.get('warning_check_count')}",
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
    lines.append("")
    return "\n".join(lines)


def _inventory_record(
    waiver: Mapping[str, Any],
    *,
    as_of: date,
    registry_ids: set[str],
) -> dict[str, Any]:
    report_id = _text(waiver.get("report_id"))
    expires_at = date.fromisoformat(_text(waiver.get("expires_at")))
    created_at = date.fromisoformat(_text(waiver.get("created_at")))
    days_until_expiry = (expires_at - as_of).days
    expired = expires_at < as_of
    review_status = _text(waiver.get("review_status"))
    active = review_status == ACTIVE_WAIVER_REVIEW_STATUS and not expired
    expiring_soon = active and 0 <= days_until_expiry <= EXPIRING_SOON_DAYS
    registry_entry_exists = report_id in registry_ids
    if expired:
        state = "EXPIRED"
    elif review_status != ACTIVE_WAIVER_REVIEW_STATUS:
        state = "INACTIVE_REVIEW"
    elif not registry_entry_exists:
        state = "MISSING_REGISTRY_ENTRY"
    elif expiring_soon:
        state = "EXPIRING_SOON"
    else:
        state = "ACTIVE"
    return {
        "waiver_id": _text(waiver.get("waiver_id")),
        "report_id": report_id,
        "affected_report_registry_entry": report_id,
        "affected_artifact_family": report_id,
        "issue_status": _text(waiver.get("issue_status")),
        "reason": _text(waiver.get("reason")),
        "owner": _text(waiver.get("owner")),
        "created_at": created_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "review_status": review_status,
        "linked_task_id": _text(waiver.get("linked_task_id")),
        "accepted_impact": _text(waiver.get("accepted_impact")),
        "validation_coverage": _text(waiver.get("validation_coverage")),
        "exit_condition": _text(waiver.get("exit_condition")),
        "days_until_expiry": days_until_expiry,
        "expired": expired,
        "expiring_soon": expiring_soon,
        "active": active,
        "registry_entry_exists": registry_entry_exists,
        "waiver_state": state,
        "production_effect": PRODUCTION_EFFECT,
    }


def _issue(
    issue_id: str,
    waiver: Mapping[str, Any],
    message: str,
    recommended_action: str,
) -> dict[str, Any]:
    return {
        "issue_id": issue_id,
        "waiver_id": _text(waiver.get("waiver_id")),
        "report_id": _text(waiver.get("report_id")),
        "severity": "BLOCKING" if issue_id != "waiver_expiring_soon" else "WARNING",
        "message": message,
        "recommended_action": recommended_action,
    }


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
) -> None:
    status = PASS_STATUS if passed else WARN_STATUS if severity == "WARNING" else FAIL_STATUS
    checks.append(
        {
            "check_id": check_id,
            "status": status,
            "severity": severity,
            "message": message,
            "recommended_action": recommended_action,
        }
    )
    if not passed:
        issues.append(
            {
                "issue_id": check_id,
                "severity": severity,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _reader_brief(
    status: str,
    summary: Mapping[str, Any],
    blocking_issues: Sequence[Mapping[str, Any]],
    warning_issues: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            "Report index waiver inventory status "
            f"{status}; active={summary.get('active_waiver_count', 0)}, "
            f"expired={summary.get('expired_waiver_count', 0)}."
        ),
        "key_result": status,
        "blocking_issues": [_text(issue.get("issue_id")) for issue in blocking_issues],
        "warnings": [_text(issue.get("issue_id")) for issue in warning_issues],
        "safety_boundary": (
            "read_existing_waiver_policy_only; production_effect=none; "
            "does not clear or renew waivers."
        ),
        "next_action": _next_action(status),
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_waiver_policy_only",
        "does_not_run_upstream_commands": True,
        "does_not_modify_waiver_policy": True,
        "does_not_modify_reports": True,
        "does_not_modify_production": True,
        "official_target_weights": False,
        "broker_action_allowed": False,
        "order_ticket_generated": False,
        "production_effect": PRODUCTION_EFFECT,
    }


def _next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "renew_or_remove_expired_report_index_waivers_before_governance_pack"
    if status == WARN_STATUS:
        return "review_expiring_or_inactive_waivers_before_next_governance_cycle"
    return "continue_governance_task_sequence"


def _expanded_waivers(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for waiver in _records(policy.get("waivers")):
        report_ids = _waiver_report_ids(waiver)
        for report_id in report_ids:
            item = dict(waiver)
            item["report_id"] = report_id
            item.pop("report_ids", None)
            expanded.append(item)
    return expanded


def _waiver_report_ids(waiver: Mapping[str, Any]) -> list[str]:
    report_id = _text(waiver.get("report_id"))
    if report_id:
        return [report_id]
    return _strings(waiver.get("report_ids"))


def _dedupe_issues(issues: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for issue in issues:
        key = (_text(issue.get("issue_id")), _text(issue.get("waiver_id")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(issue))
    return deduped


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    if not output_dir.exists():
        return None
    candidates = [path for path in output_dir.glob(f"{prefix}*{suffix}") if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _records(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [_text(item) for item in value if _text(item)]
    return []


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")
