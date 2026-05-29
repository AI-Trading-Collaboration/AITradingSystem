from __future__ import annotations

import html
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = 1
REPORT_TYPE = "report_index"
PRODUCTION_EFFECT = "none"
DEFAULT_REPORT_REGISTRY_PATH = PROJECT_ROOT / "config" / "report_registry.yaml"

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
        sla = entry.get("freshness_sla_days")
        if sla is not None and (not isinstance(sla, int) or isinstance(sla, bool) or sla < 0):
            raise ValueError(f"report registry {report_id} freshness_sla_days must be >= 0")
    return raw


def build_report_index_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
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
    missing = [item for item in reports if item["freshness_status"] == "MISSING"]
    stale = [item for item in reports if item["freshness_status"] == "STALE"]
    required_missing = [item for item in missing if item.get("required_for_daily_reading") is True]
    production_effect_risks = [
        item for item in reports if item["artifact_production_effect_risk"] is True
    ]
    warnings = _warnings(
        missing=missing,
        stale=stale,
        required_missing=required_missing,
        production_effect_risks=production_effect_risks,
    )
    status = "PASS_WITH_WARNINGS" if warnings else "PASS"
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
            _summary_item("production effect risk", summary.get("production_effect_risk_count")),
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
    latest = _latest_artifact(
        report_id=_string(entry.get("report_id")),
        artifact_globs=_strings(entry.get("artifact_globs")),
        project_root=project_root,
        as_of=as_of,
    )
    path = latest["path"]
    exists = path is not None and path.exists()
    artifact_date = latest["artifact_date"]
    age_days = (as_of - artifact_date).days if artifact_date is not None else None
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
        "exists": exists,
        "freshness_status": freshness_status,
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
    }


def _latest_artifact(
    *,
    report_id: str,
    artifact_globs: Sequence[str],
    project_root: Path,
    as_of: date,
) -> dict[str, Any]:
    candidates: list[tuple[date, float, Path]] = []
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
            if artifact_date is not None and artifact_date > as_of:
                continue
            sort_date = artifact_date or date.min
            candidates.append((sort_date, path.stat().st_mtime, path))
    if not candidates:
        return {"path": None, "artifact_date": None}
    artifact_date, _, path = max(candidates, key=lambda item: (item[0], item[1], item[2].name))
    return {"path": path, "artifact_date": None if artifact_date == date.min else artifact_date}


def _glob_paths(pattern: str) -> list[Path]:
    if re.match(r"^[A-Za-z]:", pattern):
        drive = Path(pattern[:3])
        rest = pattern[3:]
        return list(drive.glob(rest))
    return list(Path().glob(pattern))


def _date_from_path(path: Path) -> date | None:
    matches = re.findall(r"\d{4}-\d{2}-\d{2}", path.name)
    if not matches:
        return None
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


def _group_counts(reports: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    groups: dict[str, int] = {}
    for report in reports:
        group = _string(report.get("group"), "unknown")
        groups[group] = groups.get(group, 0) + 1
    return groups


def _report_row(report: Mapping[str, Any]) -> str:
    artifact = _string(report.get("latest_artifact_name")) or "MISSING"
    return (
        "<tr>"
        f"<td><strong>{_escape(report.get('title'))}</strong><br>"
        f"<code>{_escape(report.get('report_id'))}</code></td>"
        f"<td>{_escape(report.get('group'))}</td>"
        f"<td>{_escape(report.get('cadence'))}</td>"
        f"<td>{_badge(report.get('freshness_status'))}</td>"
        f"<td><code>{_escape(artifact)}</code><br>"
        f"<small>{_escape(report.get('artifact_date'))}</small></td>"
        f"<td>{_escape(report.get('artifact_status'))}</td>"
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
    elif normalized in {"missing", "stale"} or "warning" in normalized:
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
