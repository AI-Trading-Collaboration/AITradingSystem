from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from fnmatch import fnmatch
from functools import cache
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "documentation_contract"
PRODUCTION_EFFECT = "none"
DEFAULT_ARTIFACT_CATALOG_PATH = PROJECT_ROOT / "docs" / "artifact_catalog.md"

CATALOG_COLUMNS: tuple[str, ...] = (
    "artifact",
    "generator",
    "upstream_inputs",
    "key_fields",
    "downstream_use",
    "production_effect",
    "common_misread",
)

SCHEMA_STATUS_TERMS: tuple[str, ...] = (
    "schema_version",
    "report_type",
    "freshness_status",
    "evaluation_status",
    "gate_status",
    "promotion_gate_status",
    "impact_status",
    "health_status",
    "status",
    "状态",
)


def default_documentation_contract_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"documentation_contract_{as_of.isoformat()}.json"


def default_documentation_contract_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"documentation_contract_{as_of.isoformat()}.md"


def build_documentation_contract_payload(
    *,
    as_of: date,
    registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    artifact_catalog_path: Path = DEFAULT_ARTIFACT_CATALOG_PATH,
) -> dict[str, Any]:
    registry = load_report_registry(registry_path)
    defaults = _mapping(registry.get("defaults"))
    catalog_rows = load_artifact_catalog_rows(artifact_catalog_path)
    report_records = [
        _report_contract_record(
            entry=entry,
            defaults=defaults,
            catalog_rows=catalog_rows,
        )
        for entry in _records(registry.get("reports"))
    ]
    issues = [
        issue for report in report_records for issue in _records(report.get("documentation_issues"))
    ]
    error_count = len([issue for issue in issues if issue.get("severity") == "ERROR"])
    warning_count = len([issue for issue in issues if issue.get("severity") == "WARNING"])
    status = "FAIL" if error_count else "PASS_WITH_WARNINGS" if warning_count else "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "inputs": {
            "registry_path": str(registry_path),
            "registry_policy_version": _string(registry.get("policy_version")),
            "artifact_catalog_path": str(artifact_catalog_path),
        },
        "summary": {
            "report_count": len(report_records),
            "catalog_documented_count": len(
                [
                    report
                    for report in report_records
                    if report.get("catalog_status") == "DOCUMENTED"
                ]
            ),
            "missing_catalog_count": len(
                [report for report in report_records if report.get("catalog_status") == "MISSING"]
            ),
            "error_count": error_count,
            "warning_count": warning_count,
        },
        "reports": report_records,
        "issues": issues,
        "methodology": {
            "collector_mode": "read_registry_and_artifact_catalog_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "does_not_generate_artifact_catalog": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def load_artifact_catalog_rows(path: Path = DEFAULT_ARTIFACT_CATALOG_PATH) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"artifact catalog not found: {path}")
    rows: list[dict[str, str]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        cells = _split_markdown_table_row(line)
        if not cells or len(cells) < len(CATALOG_COLUMNS):
            continue
        if _is_header_or_separator(cells):
            continue
        row = {key: cells[index].strip() for index, key in enumerate(CATALOG_COLUMNS)}
        row["line_number"] = str(line_number)
        row["raw"] = line.strip()
        rows.append(row)
    return rows


def render_documentation_contract_report(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    reports = _records(payload.get("reports"))
    issues = _records(payload.get("issues"))
    lines = [
        "# Documentation Contract",
        "",
        f"- 状态：{_string(payload.get('status'))}",
        f"- as_of：{_string(payload.get('as_of'))}",
        f"- production_effect：`{_string(payload.get('production_effect'))}`",
        f"- registry reports：{summary.get('report_count', 0)}",
        f"- missing catalog：{summary.get('missing_catalog_count', 0)}",
        f"- errors / warnings：{summary.get('error_count', 0)} / {summary.get('warning_count', 0)}",
        "",
        "本报告只读检查 `config/report_registry.yaml` 与 `docs/artifact_catalog.md` 的覆盖关系；"
        "不运行上游报告、不生成交易指令、不修改 production。",
        "",
        "## Issues",
        "",
    ]
    if not issues:
        lines.append("未发现 documentation contract 问题。")
    else:
        lines.extend(["| Severity | Report | Code | Message |", "|---|---|---|---|"])
        for issue in issues:
            lines.append(
                "| "
                f"{_md(issue.get('severity'))} | "
                f"`{_md(issue.get('report_id'))}` | "
                f"`{_md(issue.get('code'))}` | "
                f"{_md(issue.get('message'))} |"
            )
    lines.extend(
        [
            "",
            "## Generated Catalog",
            "",
            "| Report | Command | Catalog | Source artifact | Schema/status | "
            "Production effect | Common misread |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for report in reports:
        source_artifacts = _records(report.get("catalog_rows"))
        source = "<br/>".join(_string(row.get("artifact")) for row in source_artifacts) or "MISSING"
        production_effect = (
            "<br/>".join(_string(row.get("production_effect")) for row in source_artifacts)
            or "MISSING"
        )
        common_misread = (
            "<br/>".join(_string(row.get("common_misread")) for row in source_artifacts)
            or "MISSING"
        )
        schema_terms = ", ".join(_strings(report.get("schema_status_terms"))) or "MISSING"
        lines.append(
            "| "
            f"`{_md(report.get('report_id'))}`<br/>{_md(report.get('title'))} | "
            f"`{_md(report.get('command'))}` | "
            f"{_md(report.get('catalog_status'))} | "
            f"{_md(source)} | "
            f"{_md(schema_terms)} | "
            f"{_md(production_effect)} | "
            f"{_md(common_misread)} |"
        )
    return "\n".join(lines) + "\n"


def write_documentation_contract_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_documentation_contract_report(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_documentation_contract_report(payload), encoding="utf-8")
    return output_path


def _report_contract_record(
    *,
    entry: Mapping[str, Any],
    defaults: Mapping[str, Any],
    catalog_rows: Sequence[Mapping[str, str]],
) -> dict[str, Any]:
    report_id = _string(entry.get("report_id"))
    artifact_globs = _strings(entry.get("artifact_globs"))
    matching_rows = [
        _catalog_row_summary(row)
        for row in catalog_rows
        if _catalog_row_matches(artifact_globs, row)
    ]
    production_effects = [
        _string(row.get("production_effect"))
        for row in matching_rows
        if _string(row.get("production_effect"))
    ]
    common_misreads = [
        _string(row.get("common_misread"))
        for row in matching_rows
        if _string(row.get("common_misread"))
    ]
    schema_status_terms = _schema_status_terms(matching_rows)
    command_documented = _command_documented(_string(entry.get("command")), matching_rows)
    catalog_status = "DOCUMENTED" if matching_rows else "MISSING"
    issues = _documentation_issues(
        report_id=report_id,
        catalog_status=catalog_status,
        production_effects=production_effects,
        common_misreads=common_misreads,
        schema_status_terms=schema_status_terms,
        command_documented=command_documented,
    )
    return {
        "report_id": report_id,
        "title": _string(entry.get("title")),
        "command": _string(entry.get("command")),
        "artifact_globs": artifact_globs,
        "registry_production_effect": _string(
            entry.get("production_effect"),
            _string(defaults.get("production_effect"), PRODUCTION_EFFECT),
        ),
        "catalog_status": catalog_status,
        "catalog_rows": matching_rows,
        "command_documented": command_documented,
        "schema_status_terms": schema_status_terms,
        "documentation_issues": issues,
    }


def _documentation_issues(
    *,
    report_id: str,
    catalog_status: str,
    production_effects: Sequence[str],
    common_misreads: Sequence[str],
    schema_status_terms: Sequence[str],
    command_documented: bool,
) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    if catalog_status != "DOCUMENTED":
        issues.append(
            _issue(
                report_id,
                "ERROR",
                "missing_artifact_catalog_row",
                "report registry entry has no matching row in docs/artifact_catalog.md.",
            )
        )
        return issues
    if not production_effects:
        issues.append(
            _issue(
                report_id,
                "ERROR",
                "missing_catalog_production_effect",
                "matched artifact catalog row does not document production_effect.",
            )
        )
    if not common_misreads:
        issues.append(
            _issue(
                report_id,
                "ERROR",
                "missing_catalog_common_misread",
                "matched artifact catalog row does not document common misread.",
            )
        )
    if not command_documented:
        issues.append(
            _issue(
                report_id,
                "WARNING",
                "command_not_documented_in_catalog_row",
                "matched artifact catalog row does not clearly mention the registry command.",
            )
        )
    if not schema_status_terms:
        issues.append(
            _issue(
                report_id,
                "WARNING",
                "missing_schema_status_terms",
                "matched artifact catalog row does not mention obvious schema/status terms.",
            )
        )
    return issues


def _issue(report_id: str, severity: str, code: str, message: str) -> dict[str, str]:
    return {
        "report_id": report_id,
        "severity": severity,
        "code": code,
        "message": message,
    }


def _catalog_row_matches(
    artifact_globs: Sequence[str],
    row: Mapping[str, str],
) -> bool:
    artifact_text = _normalize_path_text(_string(row.get("artifact")))
    for pattern in artifact_globs:
        if _artifact_pattern_matches(pattern, artifact_text):
            return True
    return False


def _artifact_pattern_matches(pattern: str, artifact_text: str) -> bool:
    normalized_pattern = _normalize_path_text(pattern)
    candidates = _code_spans(artifact_text) or (artifact_text,)
    prefix = normalized_pattern.split("*", maxsplit=1)[0]
    if prefix and _literal_prefix_can_short_circuit(prefix):
        return any(prefix in _normalize_path_text(candidate) for candidate in candidates)
    for candidate in candidates:
        normalized_candidate = _normalize_path_text(candidate)
        if fnmatch(normalized_candidate, normalized_pattern):
            return True
        if normalized_candidate.endswith("/md") and fnmatch(
            normalized_candidate[:-3],
            normalized_pattern,
        ):
            return True
        if prefix and prefix in normalized_candidate:
            return True
    return False


def _catalog_row_summary(row: Mapping[str, str]) -> dict[str, str]:
    return {
        "line_number": _string(row.get("line_number")),
        "artifact": _string(row.get("artifact")),
        "generator": _string(row.get("generator")),
        "upstream_inputs": _string(row.get("upstream_inputs")),
        "key_fields": _string(row.get("key_fields")),
        "downstream_use": _string(row.get("downstream_use")),
        "production_effect": _string(row.get("production_effect")),
        "common_misread": _string(row.get("common_misread")),
    }


def _schema_status_terms(rows: Sequence[Mapping[str, str]]) -> list[str]:
    found: set[str] = set()
    text = " ".join(" ".join(_string(value) for value in row.values()).lower() for row in rows)
    for term in SCHEMA_STATUS_TERMS:
        if term.lower() in text:
            found.add(term)
    return sorted(found)


def _command_documented(command: str, rows: Sequence[Mapping[str, str]]) -> bool:
    if not command:
        return False
    text = " ".join(" ".join(_string(value) for value in row.values()).lower() for row in rows)
    normalized_command = " ".join(command.lower().split())
    if normalized_command in text:
        return True
    tokens = normalized_command.split()
    if len(tokens) >= 2 and " ".join(tokens[:2]) in text:
        return True
    script = next((token for token in tokens if token.endswith(".py")), "")
    return bool(script and script in text)


def _split_markdown_table_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        return []
    return [cell.strip() for cell in stripped.strip("|").split("|")]


def _is_header_or_separator(cells: Sequence[str]) -> bool:
    first = cells[0].strip().lower()
    if first in {"artifact", "report", "文档"}:
        return True
    return all(set(cell.strip()) <= {"-", ":"} for cell in cells if cell.strip())


def _literal_prefix_can_short_circuit(prefix: str) -> bool:
    return not any(marker in prefix for marker in ("?", "[", "]"))


@cache
def _code_spans(text: str) -> tuple[str, ...]:
    return tuple(re.findall(r"`([^`]+)`", text))


@cache
def _normalize_path_text(value: str) -> str:
    return value.replace("\\", "/").strip()


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


def _md(value: object) -> str:
    return _string(value).replace("\n", "<br/>").replace("|", "/")
