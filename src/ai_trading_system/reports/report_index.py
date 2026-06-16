from __future__ import annotations

import html
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from glob import glob
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = 1
REPORT_TYPE = "report_index"
PRODUCTION_EFFECT = "none"
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"
DEFAULT_REPORT_INDEX_WAIVER_PATH = PROJECT_ROOT / "config" / "report_index_visibility_waivers.yaml"
DEFAULT_ARTIFACT_SELECTION_POLICY = "as_of_or_unknown"
ARTIFACT_SELECTION_POLICIES = frozenset(
    {
        DEFAULT_ARTIFACT_SELECTION_POLICY,
        "latest_available",
    }
)
DEFAULT_FRESHNESS_BASIS = "calendar_days"
FRESHNESS_BASIS_VALUES = frozenset(
    {
        DEFAULT_FRESHNESS_BASIS,
        "us_equity_trading_days",
    }
)
WAIVABLE_VISIBILITY_ISSUES = frozenset({"MISSING", "STALE"})
ACTIVE_WAIVER_REVIEW_STATUS = "approved_active"
WAIVER_REVIEW_STATUSES = frozenset(
    {
        ACTIVE_WAIVER_REVIEW_STATUS,
        "under_review",
        "retired",
    }
)

STATUS_KEYS: tuple[str, ...] = (
    "status",
    "report_status",
    "health_status",
    "freshness_status",
    "evaluation_status",
    "gate_status",
    "promotion_gate_status",
    "impact_status",
    "shadow_status",
    "monitor_status",
)


def default_report_index_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_{as_of.isoformat()}.json"


def default_report_index_html_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_{as_of.isoformat()}.html"


def load_report_registry(path: Path = DEFAULT_REPORT_REGISTRY_PATH) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"report registry not found: {path}")
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError("report registry must be a mapping")
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"report registry schema_version must be {SCHEMA_VERSION}")
    reports = raw.get("reports")
    if not isinstance(reports, list) or not reports:
        raise ValueError("report registry must contain a non-empty reports list")
    seen: set[str] = set()
    for index, entry in enumerate(reports):
        if not isinstance(entry, dict):
            raise ValueError(f"report registry entry {index} must be a mapping")
        report_id = _string(entry.get("report_id"))
        if not report_id:
            raise ValueError(f"report registry entry {index} missing report_id")
        if report_id in seen:
            raise ValueError(f"duplicate report_id in report registry: {report_id}")
        seen.add(report_id)
        for field in (
            "title",
            "group",
            "cadence",
            "audience",
            "owner",
            "command",
            "artifact_globs",
            "owner_action",
            "freshness_rationale",
        ):
            if field not in entry:
                raise ValueError(f"report registry {report_id} missing required field: {field}")
        if not isinstance(entry.get("artifact_globs"), list) or not entry["artifact_globs"]:
            raise ValueError(f"report registry {report_id} artifact_globs must be a non-empty list")
        if "freshness_sla_days" not in entry:
            raise ValueError(
                f"report registry {report_id} missing required field: freshness_sla_days"
            )
        sla = entry.get("freshness_sla_days")
        if not isinstance(sla, int) or isinstance(sla, bool) or sla < 0:
            raise ValueError(f"report registry {report_id} freshness_sla_days must be >= 0")
        artifact_selection_policy = _string(
            entry.get("artifact_selection_policy"),
            DEFAULT_ARTIFACT_SELECTION_POLICY,
        )
        if artifact_selection_policy not in ARTIFACT_SELECTION_POLICIES:
            raise ValueError(
                f"report registry {report_id} artifact_selection_policy must be one of "
                f"{sorted(ARTIFACT_SELECTION_POLICIES)}"
            )
        if "freshness_basis" in entry:
            freshness_basis = _string(entry.get("freshness_basis"))
            if freshness_basis and freshness_basis not in FRESHNESS_BASIS_VALUES:
                raise ValueError(
                    f"report registry {report_id} freshness_basis must be one of "
                    f"{sorted(FRESHNESS_BASIS_VALUES)}"
                )
    defaults = _mapping(raw.get("defaults"))
    default_basis = _string(defaults.get("freshness_basis"), DEFAULT_FRESHNESS_BASIS)
    if default_basis not in FRESHNESS_BASIS_VALUES:
        raise ValueError(
            f"report registry defaults freshness_basis must be one of "
            f"{sorted(FRESHNESS_BASIS_VALUES)}"
        )
    basis_by_cadence = _mapping(defaults.get("freshness_basis_by_cadence"))
    for cadence, basis in basis_by_cadence.items():
        if _string(basis) not in FRESHNESS_BASIS_VALUES:
            raise ValueError(
                f"report registry defaults freshness_basis_by_cadence[{cadence}] must be one "
                f"of {sorted(FRESHNESS_BASIS_VALUES)}"
            )
    return raw


def load_report_index_visibility_waivers(
    path: Path = DEFAULT_REPORT_INDEX_WAIVER_PATH,
) -> dict[str, Any]:
    if not path.exists():
        return {
            "schema_version": SCHEMA_VERSION,
            "policy_id": "report_index_visibility_waivers_not_configured",
            "policy_metadata": {
                "owner": "system",
                "status": "not_configured",
                "rationale": "No report index visibility waiver file exists.",
            },
            "waivers": [],
        }
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ValueError("report index visibility waiver file must be a mapping")
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"report index visibility waiver schema_version must be {SCHEMA_VERSION}")
    waivers = raw.get("waivers")
    if waivers is None:
        raw["waivers"] = []
        return raw
    if not isinstance(waivers, list):
        raise ValueError("report index visibility waiver file waivers must be a list")
    seen: set[tuple[str, str]] = set()
    for index, waiver in enumerate(waivers):
        if not isinstance(waiver, Mapping):
            raise ValueError(f"report index visibility waiver {index} must be a mapping")
        waiver_id = _string(waiver.get("waiver_id"))
        if not waiver_id:
            raise ValueError(f"report index visibility waiver {index} missing waiver_id")
        issue_status = _string(waiver.get("issue_status"))
        if issue_status not in WAIVABLE_VISIBILITY_ISSUES:
            raise ValueError(
                f"report index visibility waiver {waiver_id} issue_status must be one of "
                f"{sorted(WAIVABLE_VISIBILITY_ISSUES)}"
            )
        report_ids = _waiver_report_ids(waiver)
        if not report_ids:
            raise ValueError(f"report index visibility waiver {waiver_id} missing report_id(s)")
        for field in (
            "owner",
            "reason",
            "created_at",
            "expires_at",
            "review_status",
            "linked_task_id",
            "accepted_impact",
            "validation_coverage",
            "exit_condition",
        ):
            if not _string(waiver.get(field)):
                raise ValueError(
                    f"report index visibility waiver {waiver_id} missing required field: {field}"
                )
        created_at = _parse_waiver_date(waiver.get("created_at"), waiver_id, "created_at")
        expires_at = _parse_waiver_date(waiver.get("expires_at"), waiver_id, "expires_at")
        if expires_at <= created_at:
            raise ValueError(
                f"report index visibility waiver {waiver_id} expires_at must be after created_at"
            )
        review_status = _string(waiver.get("review_status"))
        if review_status not in WAIVER_REVIEW_STATUSES:
            raise ValueError(
                f"report index visibility waiver {waiver_id} review_status must be one of "
                f"{sorted(WAIVER_REVIEW_STATUSES)}"
            )
        for report_id in report_ids:
            key = (report_id, issue_status)
            if key in seen:
                raise ValueError(
                    f"duplicate report index visibility waiver for {report_id}/{issue_status}"
                )
            seen.add(key)
    return raw


def build_report_index_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Path | None = DEFAULT_REPORT_INDEX_WAIVER_PATH,
) -> dict[str, Any]:
    registry = load_report_registry(registry_path)
    defaults = _mapping(registry.get("defaults"))
    reports = [
        _report_record(
            entry=entry,
            as_of=as_of,
            project_root=project_root,
            defaults=defaults,
        )
        for entry in _records(registry.get("reports"))
    ]
    waiver_policy = (
        load_report_index_visibility_waivers(waiver_path)
        if waiver_path is not None
        else load_report_index_visibility_waivers(Path("__missing_report_index_waivers__.yaml"))
    )
    reports, explicit_waivers, unwaived_issues = _apply_visibility_waivers(
        reports=reports,
        waiver_policy=waiver_policy,
        as_of=as_of,
    )
    expanded_waivers = _expanded_waivers(waiver_policy)
    expired_waivers = [
        waiver for waiver in expanded_waivers if _waiver_expired(waiver, as_of=as_of)
    ]
    inactive_waivers = [
        waiver
        for waiver in expanded_waivers
        if _string(waiver.get("review_status")) != ACTIVE_WAIVER_REVIEW_STATUS
    ]
    missing = [item for item in reports if item["freshness_status"] == "MISSING"]
    stale = [item for item in reports if item["freshness_status"] == "STALE"]
    required_missing = [item for item in missing if item.get("required_for_daily_reading") is True]
    production_effect_risks = [
        item for item in reports if item["artifact_production_effect_risk"] is True
    ]
    warnings = [_warning_text(issue) for issue in unwaived_issues]
    if warnings:
        status = "PASS_WITH_WARNINGS"
    elif explicit_waivers:
        status = "PASS_WITH_EXPLICIT_WAIVERS"
    else:
        status = "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "registry": {
            "path": str(registry_path),
            "policy_version": _string(registry.get("policy_version")),
            "policy_metadata": _mapping(registry.get("policy_metadata")),
        },
        "summary": {
            "report_count": len(reports),
            "available_count": len([item for item in reports if item["exists"] is True]),
            "missing_count": len(missing),
            "stale_count": len(stale),
            "required_missing_count": len(required_missing),
            "production_effect_risk_count": len(production_effect_risks),
            "explicit_waiver_count": len(explicit_waivers),
            "expired_waiver_count": len(expired_waivers),
            "inactive_waiver_count": len(inactive_waivers),
            "unwaived_warning_count": len(warnings),
            "reader_brief_count": len(
                [item for item in reports if item["include_in_reader_brief"] is True]
            ),
            "daily_task_dashboard_count": len(
                [item for item in reports if item["include_in_daily_task_dashboard"] is True]
            ),
            "groups": _group_counts(reports),
        },
        "reports": reports,
        "warnings": warnings,
        "explicit_waivers": explicit_waivers,
        "waiver_policy": {
            "path": "" if waiver_path is None else str(waiver_path),
            "policy_id": _string(waiver_policy.get("policy_id")),
            "policy_metadata": _mapping(waiver_policy.get("policy_metadata")),
            "configured_waiver_count": len(expanded_waivers),
            "active_waiver_count": len(explicit_waivers),
            "expired_waiver_count": len(expired_waivers),
            "inactive_waiver_count": len(inactive_waivers),
        },
        "visibility_audit": {
            "audit_status": "PASS" if not warnings else "WARN",
            "freshness_basis_values": sorted(
                {_string(item.get("freshness_basis"), DEFAULT_FRESHNESS_BASIS) for item in reports}
            ),
            "missing_report_ids": [item["report_id"] for item in missing],
            "stale_report_ids": [item["report_id"] for item in stale],
            "waived_report_ids": [item["report_id"] for item in explicit_waivers],
            "expired_waiver_ids": [item["waiver_id"] for item in expired_waivers],
            "inactive_waiver_ids": [item["waiver_id"] for item in inactive_waivers],
            "unwaived_issue_ids": [item["issue_id"] for item in unwaived_issues],
            "required_missing_report_ids": [item["report_id"] for item in required_missing],
            "production_effect_risk_report_ids": [
                item["report_id"] for item in production_effect_risks
            ],
        },
        "methodology": {
            "collector_mode": "read_existing_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_report_index_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_report_index_html(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_report_index_html(payload), encoding="utf-8")
    return output_path


def render_report_index_html(payload: Mapping[str, Any]) -> str:
    as_of = _escape(payload.get("as_of"))
    summary = _mapping(payload.get("summary"))
    reports = _records(payload.get("reports"))
    rows = "\n".join(_report_row(report) for report in reports)
    waiver_summary = _mapping(payload.get("waiver_policy"))
    visibility_audit = _mapping(payload.get("visibility_audit"))
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="zh-CN">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>Report Index {as_of}</title>",
            f"<style>{_css()}</style>",
            "</head>",
            "<body>",
            "<main>",
            "<header>",
            "<p>Report Registry & Cadence Calendar</p>",
            f"<h1>Report Index {as_of}</h1>",
            f"<span>Status: {_escape(payload.get('status'))}</span>",
            "</header>",
            '<section class="summary-grid">',
            _summary_item("reports", summary.get("report_count")),
            _summary_item("available", summary.get("available_count")),
            _summary_item("missing", summary.get("missing_count")),
            _summary_item("stale", summary.get("stale_count")),
            _summary_item("required missing", summary.get("required_missing_count")),
            _summary_item("explicit waivers", summary.get("explicit_waiver_count")),
            _summary_item("expired waivers", summary.get("expired_waiver_count")),
            _summary_item("inactive waivers", summary.get("inactive_waiver_count")),
            _summary_item("unwaived warnings", summary.get("unwaived_warning_count")),
            _summary_item("production effect risk", summary.get("production_effect_risk_count")),
            "</section>",
            "<section>",
            "<h2>Visibility Audit</h2>",
            f"<p>Audit status: {_badge(visibility_audit.get('audit_status'))}</p>",
            "<p>Explicit waiver policy: "
            f"<code>{_escape(waiver_summary.get('policy_id'))}</code>；"
            f"active waivers: {_escape(waiver_summary.get('active_waiver_count'))}；"
            f"configured waivers: {_escape(waiver_summary.get('configured_waiver_count'))}</p>",
            "</section>",
            "<section>",
            "<h2>Report Freshness</h2>",
            "<p>本页面只读扫描既有 artifact，不运行上游报告命令，也不补造结论。</p>",
            '<div class="table-wrap"><table>',
            "<thead><tr>"
            "<th>Report</th><th>Group</th><th>Cadence</th><th>Freshness</th>"
            "<th>Artifact</th><th>Status</th><th>Owner action</th>"
            "</tr></thead>",
            f"<tbody>{rows}</tbody>",
            "</table></div>",
            "</section>",
            "</main>",
            "</body>",
            "</html>",
            "",
        ]
    )


def _report_record(
    *,
    entry: Mapping[str, Any],
    as_of: date,
    project_root: Path,
    defaults: Mapping[str, Any],
) -> dict[str, Any]:
    artifact_selection_policy = _artifact_selection_policy(entry, defaults)
    freshness_basis = _freshness_basis(entry, defaults)
    latest = _latest_artifact(
        report_id=_string(entry.get("report_id")),
        artifact_globs=_strings(entry.get("artifact_globs")),
        project_root=project_root,
        as_of=as_of,
        artifact_selection_policy=artifact_selection_policy,
    )
    path = latest["path"]
    exists = path is not None and path.exists()
    artifact_date = latest["artifact_date"]
    age_days = _artifact_age_days(
        as_of=as_of,
        artifact_date=artifact_date,
        artifact_selection_policy=artifact_selection_policy,
        freshness_basis=freshness_basis,
    )
    artifact_temporal_relation = _artifact_temporal_relation(
        as_of=as_of,
        artifact_date=artifact_date,
        exists=exists,
    )
    freshness_status = _freshness_status(
        exists=exists,
        age_days=age_days,
        freshness_sla_days=entry.get("freshness_sla_days"),
        missing_status=_string(defaults.get("missing_status"), "MISSING"),
        stale_status=_string(defaults.get("stale_status"), "STALE"),
    )
    json_payload = _read_json_object(path) if path is not None and path.suffix == ".json" else {}
    artifact_status = _artifact_status(json_payload, exists=exists)
    artifact_production_effect = (
        _string(json_payload.get("production_effect"))
        or _string(entry.get("production_effect"))
        or _string(defaults.get("production_effect"), PRODUCTION_EFFECT)
    )
    return {
        "report_id": _string(entry.get("report_id")),
        "title": _string(entry.get("title")),
        "group": _string(entry.get("group")),
        "cadence": _string(entry.get("cadence")),
        "audience": _string(entry.get("audience")),
        "owner": _string(entry.get("owner")),
        "command": _string(entry.get("command")),
        "artifact_globs": _strings(entry.get("artifact_globs")),
        "latest_artifact_path": "" if path is None else str(path),
        "latest_artifact_name": "" if path is None else path.name,
        "artifact_date": "" if artifact_date is None else artifact_date.isoformat(),
        "artifact_selection_policy": artifact_selection_policy,
        "artifact_temporal_relation": artifact_temporal_relation,
        "artifact_after_as_of": artifact_temporal_relation == "AFTER_AS_OF",
        "exists": exists,
        "freshness_status": freshness_status,
        "freshness_basis": freshness_basis,
        "freshness_sla_days": entry.get("freshness_sla_days"),
        "age_days": age_days,
        "freshness_rationale": _string(entry.get("freshness_rationale")),
        "artifact_status": artifact_status,
        "artifact_production_effect": artifact_production_effect,
        "artifact_production_effect_risk": artifact_production_effect
        not in {"", "none", "advisory", "read_only"},
        "production_effect": PRODUCTION_EFFECT,
        "owner_action": _string(entry.get("owner_action")),
        "include_in_reader_brief": bool(entry.get("include_in_reader_brief")),
        "include_in_daily_task_dashboard": bool(entry.get("include_in_daily_task_dashboard")),
        "required_for_daily_reading": bool(entry.get("required_for_daily_reading")),
        "visibility_status": "OK",
        "visibility_issue": {},
        "visibility_waiver": {},
    }


def _latest_artifact(
    *,
    report_id: str,
    artifact_globs: Sequence[str],
    project_root: Path,
    as_of: date,
    artifact_selection_policy: str,
) -> dict[str, Any]:
    candidates: list[tuple[date, float, Path]] = []
    include_after_as_of = artifact_selection_policy == "latest_available"
    for raw_pattern in artifact_globs:
        pattern_path = Path(raw_pattern)
        pattern = str(pattern_path if pattern_path.is_absolute() else project_root / pattern_path)
        for path in _glob_paths(pattern):
            if not path.is_file():
                continue
            if report_id == "backtest_daily" and not re.fullmatch(
                r"backtest_\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.md",
                path.name,
            ):
                continue
            artifact_date = _date_from_path(path)
            if artifact_date is not None and artifact_date > as_of and not include_after_as_of:
                continue
            sort_date = artifact_date or date.min
            candidates.append((sort_date, path.stat().st_mtime, path))
    if not candidates:
        return {"path": None, "artifact_date": None}
    artifact_date, _, path = max(candidates, key=lambda item: (item[0], item[1], item[2].name))
    return {"path": path, "artifact_date": None if artifact_date == date.min else artifact_date}


def _artifact_selection_policy(
    entry: Mapping[str, Any],
    defaults: Mapping[str, Any],
) -> str:
    raw_policy = (
        _string(entry.get("artifact_selection_policy"))
        or _string(defaults.get("artifact_selection_policy"))
        or DEFAULT_ARTIFACT_SELECTION_POLICY
    )
    if raw_policy not in ARTIFACT_SELECTION_POLICIES:
        return DEFAULT_ARTIFACT_SELECTION_POLICY
    return raw_policy


def _freshness_basis(entry: Mapping[str, Any], defaults: Mapping[str, Any]) -> str:
    raw_basis = _string(entry.get("freshness_basis"))
    if raw_basis:
        return raw_basis if raw_basis in FRESHNESS_BASIS_VALUES else DEFAULT_FRESHNESS_BASIS
    basis_by_cadence = _mapping(defaults.get("freshness_basis_by_cadence"))
    cadence = _string(entry.get("cadence"))
    cadence_basis = _string(basis_by_cadence.get(cadence))
    if cadence_basis:
        return cadence_basis if cadence_basis in FRESHNESS_BASIS_VALUES else DEFAULT_FRESHNESS_BASIS
    default_basis = _string(defaults.get("freshness_basis"), DEFAULT_FRESHNESS_BASIS)
    return default_basis if default_basis in FRESHNESS_BASIS_VALUES else DEFAULT_FRESHNESS_BASIS


def _artifact_age_days(
    *,
    as_of: date,
    artifact_date: date | None,
    artifact_selection_policy: str,
    freshness_basis: str,
) -> int | None:
    if artifact_date is None:
        return None
    if freshness_basis == "us_equity_trading_days":
        return _us_equity_trading_day_age(
            as_of=as_of,
            artifact_date=artifact_date,
            artifact_selection_policy=artifact_selection_policy,
        )
    age_days = (as_of - artifact_date).days
    if artifact_selection_policy == "latest_available" and age_days < 0:
        return 0
    return age_days


def _us_equity_trading_day_age(
    *,
    as_of: date,
    artifact_date: date,
    artifact_selection_policy: str,
) -> int:
    if artifact_selection_policy == "latest_available" and artifact_date > as_of:
        return 0
    if artifact_date >= as_of:
        return 0
    age = 0
    current = artifact_date + timedelta(days=1)
    while current <= as_of:
        if is_us_equity_trading_day(current):
            age += 1
        current += timedelta(days=1)
    return age


def _artifact_temporal_relation(
    *,
    as_of: date,
    artifact_date: date | None,
    exists: bool,
) -> str:
    if not exists:
        return "MISSING"
    if artifact_date is None:
        return "DATE_UNKNOWN"
    if artifact_date > as_of:
        return "AFTER_AS_OF"
    return "ON_OR_BEFORE_AS_OF"


def _glob_paths(pattern: str) -> list[Path]:
    return [Path(item) for item in glob(pattern)]


def _date_from_path(path: Path) -> date | None:
    search_text = " ".join([path.name, path.parent.name])
    matches = re.findall(r"\d{4}-\d{2}-\d{2}", search_text)
    if not matches:
        compact_matches = re.findall(r"(?<!\d)(\d{8})(?:T\d{6}Z?)?(?!\d)", search_text)
        if not compact_matches:
            return None
        value = compact_matches[-1]
        matches = [f"{value[:4]}-{value[4:6]}-{value[6:]}"]
    try:
        return date.fromisoformat(matches[-1])
    except ValueError:
        return None


def _freshness_status(
    *,
    exists: bool,
    age_days: int | None,
    freshness_sla_days: object,
    missing_status: str,
    stale_status: str,
) -> str:
    if not exists:
        return missing_status
    if age_days is None:
        return "AVAILABLE_DATE_UNKNOWN"
    if freshness_sla_days is None:
        return "AVAILABLE"
    sla = int(freshness_sla_days)
    return "FRESH" if age_days <= sla else stale_status


def _artifact_status(payload: Mapping[str, Any], *, exists: bool) -> str:
    if not exists:
        return "MISSING"
    if not payload:
        return "AVAILABLE"
    for key in STATUS_KEYS:
        value = _string(payload.get(key))
        if value:
            return value
    return "AVAILABLE"


def _warnings(
    *,
    missing: Sequence[Mapping[str, Any]],
    stale: Sequence[Mapping[str, Any]],
    required_missing: Sequence[Mapping[str, Any]],
    production_effect_risks: Sequence[Mapping[str, Any]],
) -> list[str]:
    warnings: list[str] = []
    warnings.extend(
        f"{item['report_id']}_missing:{item['latest_artifact_path']}" for item in missing
    )
    warnings.extend(
        f"{item['report_id']}_stale:age_days={item['age_days']};sla={item['freshness_sla_days']}"
        for item in stale
    )
    warnings.extend(f"{item['report_id']}_required_missing" for item in required_missing)
    warnings.extend(
        f"{item['report_id']}_production_effect_not_none:{item['artifact_production_effect']}"
        for item in production_effect_risks
    )
    return warnings


def _apply_visibility_waivers(
    *,
    reports: Sequence[Mapping[str, Any]],
    waiver_policy: Mapping[str, Any],
    as_of: date,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    waivers_by_key = {
        (_string(waiver.get("report_id")), _string(waiver.get("issue_status"))): waiver
        for waiver in _expanded_waivers(waiver_policy)
    }
    updated: list[dict[str, Any]] = []
    explicit_waivers: list[dict[str, Any]] = []
    unwaived_issues: list[dict[str, Any]] = []
    for raw_report in reports:
        report = dict(raw_report)
        issue = _report_visibility_issue(report)
        if issue is None:
            report["visibility_status"] = "OK"
            report["visibility_issue"] = {}
            report["visibility_waiver"] = {}
            updated.append(report)
            continue
        waiver = waivers_by_key.get((issue["report_id"], issue["issue_status"]))
        if waiver is not None and _waiver_can_apply(issue) and _waiver_current(
            waiver,
            as_of=as_of,
        ):
            applied = _applied_waiver(issue=issue, waiver=waiver)
            report["visibility_status"] = "WAIVED"
            report["visibility_issue"] = issue
            report["visibility_waiver"] = applied
            explicit_waivers.append(applied)
        else:
            report["visibility_status"] = "WARNING"
            report["visibility_issue"] = issue
            report["visibility_waiver"] = {}
            unwaived_issues.append(issue)
        updated.append(report)
    return updated, explicit_waivers, unwaived_issues


def _report_visibility_issue(report: Mapping[str, Any]) -> dict[str, Any] | None:
    report_id = _string(report.get("report_id"))
    if report.get("artifact_production_effect_risk") is True:
        return {
            "issue_id": f"{report_id}_production_effect_not_none",
            "report_id": report_id,
            "issue_status": "PRODUCTION_EFFECT_RISK",
            "severity": "error",
            "warning_text": (
                f"{report_id}_production_effect_not_none:"
                f"{_string(report.get('artifact_production_effect'))}"
            ),
            "required_for_daily_reading": bool(report.get("required_for_daily_reading")),
        }
    freshness_status = _string(report.get("freshness_status"))
    if freshness_status == "MISSING":
        required = bool(report.get("required_for_daily_reading"))
        warning_text = (
            f"{report_id}_required_missing"
            if required
            else f"{report_id}_missing:{_string(report.get('latest_artifact_path'))}"
        )
        return {
            "issue_id": f"{report_id}_missing",
            "report_id": report_id,
            "issue_status": "MISSING",
            "severity": "error" if required else "warning",
            "warning_text": warning_text,
            "required_for_daily_reading": required,
            "latest_artifact_path": _string(report.get("latest_artifact_path")),
            "freshness_basis": _string(report.get("freshness_basis")),
        }
    if freshness_status == "STALE":
        return {
            "issue_id": f"{report_id}_stale",
            "report_id": report_id,
            "issue_status": "STALE",
            "severity": "warning",
            "warning_text": (
                f"{report_id}_stale:age_days={report.get('age_days')};"
                f"sla={report.get('freshness_sla_days')}"
            ),
            "required_for_daily_reading": bool(report.get("required_for_daily_reading")),
            "age_days": report.get("age_days"),
            "freshness_sla_days": report.get("freshness_sla_days"),
            "freshness_basis": _string(report.get("freshness_basis")),
        }
    return None


def _waiver_can_apply(issue: Mapping[str, Any]) -> bool:
    issue_status = _string(issue.get("issue_status"))
    if issue_status not in WAIVABLE_VISIBILITY_ISSUES:
        return False
    if issue_status == "MISSING" and bool(issue.get("required_for_daily_reading")):
        return False
    return True


def _waiver_current(waiver: Mapping[str, Any], *, as_of: date) -> bool:
    if _string(waiver.get("review_status")) != ACTIVE_WAIVER_REVIEW_STATUS:
        return False
    return not _waiver_expired(waiver, as_of=as_of)


def _waiver_expired(waiver: Mapping[str, Any], *, as_of: date) -> bool:
    expires_at = _parse_waiver_date(
        waiver.get("expires_at"),
        _string(waiver.get("waiver_id"), "unknown"),
        "expires_at",
    )
    return expires_at < as_of


def _applied_waiver(
    *,
    issue: Mapping[str, Any],
    waiver: Mapping[str, Any],
) -> dict[str, Any]:
    report_id = _string(issue.get("report_id"))
    return {
        "waiver_id": _string(waiver.get("waiver_id")),
        "report_id": report_id,
        "affected_report_registry_entry": report_id,
        "affected_artifact_family": report_id,
        "issue_status": _string(issue.get("issue_status")),
        "reason": _string(waiver.get("reason")),
        "owner": _string(waiver.get("owner")),
        "created_at": _string(waiver.get("created_at")),
        "expires_at": _string(waiver.get("expires_at")),
        "review_status": _string(waiver.get("review_status")),
        "linked_task_id": _string(waiver.get("linked_task_id")),
        "accepted_impact": _string(waiver.get("accepted_impact")),
        "validation_coverage": _string(waiver.get("validation_coverage")),
        "exit_condition": _string(waiver.get("exit_condition")),
        "visibility_issue_id": _string(issue.get("issue_id")),
        "warning_text": _string(issue.get("warning_text")),
    }


def _expanded_waivers(policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for waiver in _records(policy.get("waivers")):
        for report_id in _waiver_report_ids(waiver):
            item = dict(waiver)
            item["report_id"] = report_id
            item.pop("report_ids", None)
            expanded.append(item)
    return expanded


def _waiver_report_ids(waiver: Mapping[str, Any]) -> list[str]:
    report_id = _string(waiver.get("report_id"))
    if report_id:
        return [report_id]
    return _strings(waiver.get("report_ids"))


def _parse_waiver_date(value: Any, waiver_id: str, field_name: str) -> date:
    raw = _string(value)
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            f"report index visibility waiver {waiver_id} {field_name} must be YYYY-MM-DD"
        ) from exc


def _warning_text(issue: Mapping[str, Any]) -> str:
    return _string(issue.get("warning_text")) or _string(issue.get("issue_id"))


def _group_counts(reports: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    groups: dict[str, int] = {}
    for report in reports:
        group = _string(report.get("group"), "unknown")
        groups[group] = groups.get(group, 0) + 1
    return groups


def _report_row(report: Mapping[str, Any]) -> str:
    artifact = _string(report.get("latest_artifact_name")) or "MISSING"
    artifact_metadata = " / ".join(
        item
        for item in (
            _string(report.get("artifact_date")),
            _string(report.get("artifact_temporal_relation")),
            _string(report.get("artifact_selection_policy")),
            _string(report.get("freshness_basis")),
        )
        if item
    )
    visibility = _string(report.get("visibility_status"), "OK")
    return (
        "<tr>"
        f"<td><strong>{_escape(report.get('title'))}</strong><br>"
        f"<code>{_escape(report.get('report_id'))}</code></td>"
        f"<td>{_escape(report.get('group'))}</td>"
        f"<td>{_escape(report.get('cadence'))}</td>"
        f"<td>{_badge(report.get('freshness_status'))}</td>"
        f"<td><code>{_escape(artifact)}</code><br>"
        f"<small>{_escape(artifact_metadata)}</small></td>"
        f"<td>{_escape(report.get('artifact_status'))}<br><small>{_escape(visibility)}</small></td>"
        f"<td>{_escape(report.get('owner_action'))}</td>"
        "</tr>"
    )


def _summary_item(label: str, value: object) -> str:
    return (
        '<div class="summary-item">'
        f"<span>{_escape(label)}</span>"
        f"<strong>{_escape(value)}</strong>"
        "</div>"
    )


def _badge(value: object) -> str:
    text = _string(value, "UNKNOWN")
    normalized = text.lower()
    class_name = "badge"
    if normalized in {"fresh", "pass", "available"}:
        class_name += " ok"
    elif normalized in {"missing", "stale"} or "warning" in normalized or "waiver" in normalized:
        class_name += " warn"
    return f'<span class="{class_name}">{_escape(text)}</span>'


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item not in {"", None}]


def _string(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    return str(value)


def _escape(value: object) -> str:
    return html.escape(_string(value), quote=True)


def _css() -> str:
    return """
:root {
  color-scheme: light;
  --bg: #f7f9fb;
  --surface: #fff;
  --ink: #111827;
  --muted: #5b6472;
  --line: #d7dee8;
  --ok: #0f766e;
  --warn: #b45309;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: Arial, "Microsoft YaHei", sans-serif;
  line-height: 1.5;
}
main { width: min(1180px, calc(100% - 32px)); margin: 0 auto; padding: 28px 0 36px; }
header { border-bottom: 1px solid var(--line); margin-bottom: 22px; padding-bottom: 16px; }
header p { color: var(--muted); margin: 0 0 6px; text-transform: uppercase; font-size: 12px; }
h1 { margin: 0 0 8px; font-size: 28px; letter-spacing: 0; }
h2 { margin: 0 0 8px; font-size: 20px; letter-spacing: 0; }
section {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 18px;
  margin-bottom: 18px;
}
.summary-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
}
.summary-item { border: 1px solid var(--line); border-radius: 6px; padding: 12px; }
.summary-item span { display: block; color: var(--muted); font-size: 12px; }
.summary-item strong { display: block; margin-top: 4px; font-size: 20px; }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; min-width: 860px; }
th, td {
  border-bottom: 1px solid var(--line);
  padding: 10px;
  text-align: left;
  vertical-align: top;
}
th { color: var(--muted); font-size: 12px; text-transform: uppercase; }
code { font-family: Consolas, "Courier New", monospace; font-size: 12px; }
small { color: var(--muted); }
.badge {
  display: inline-block;
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 2px 8px;
  font-size: 12px;
}
.badge.ok { color: var(--ok); border-color: color-mix(in srgb, var(--ok) 35%, var(--line)); }
.badge.warn { color: var(--warn); border-color: color-mix(in srgb, var(--warn) 35%, var(--line)); }
"""
