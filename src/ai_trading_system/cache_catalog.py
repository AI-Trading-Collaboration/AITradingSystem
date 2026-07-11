from __future__ import annotations

import csv
import fnmatch
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT, DataSourceConfig, DataSourcesConfig
from ai_trading_system.platform.artifacts import (
    write_json_atomic_without_trailing_newline,
    write_text_atomic,
)

CACHE_CATALOG_SCHEMA_VERSION = 1
CACHE_CATALOG_REPORT_TYPE = "cache_catalog"
CACHE_CATALOG_VALIDATION_REPORT_TYPE = "cache_catalog_validation"
CACHE_CATALOG_POLICY_VERSION = "cache_catalog_policy_v1"
PRODUCTION_EFFECT = "none"

DEFAULT_CACHE_CATALOG_POLICY_PATH = PROJECT_ROOT / "config" / "cache_catalog.yaml"
DEFAULT_CACHE_CATALOG_DIR = PROJECT_ROOT / "reports" / "data_governance" / "cache_catalog"
DEFAULT_DATA_REFRESH_AUDIT_DIR = (
    PROJECT_ROOT / "reports" / "data_governance" / "data_refresh_audit"
)
DEFAULT_VALIDATE_DATA_AUDIT_DIR = PROJECT_ROOT / "artifacts" / "data_refresh_audit" / "validation"
LATEST_POINTER_NAME = "latest_cache_catalog.json"
LATEST_DATA_REFRESH_AUDIT_POINTER_NAME = "latest_data_refresh_audit.json"
LATEST_VALIDATE_DATA_AUDIT_POINTER_NAME = "latest_validate_data_audit.json"

CACHE_CATALOG_STATUSES = frozenset({"PASS", "PASS_WITH_WARNINGS", "FAIL"})
CACHE_INTEGRITY_STATUSES = frozenset({"OK", "WARNING", "FAIL"})
CACHE_ENTRY_STATUSES = frozenset(
    {
        "OK",
        "MISSING_REQUIRED",
        "MISSING_OPTIONAL",
        "UNREADABLE",
        "CHECKSUM_MISMATCH",
        "CHECKSUM_CHANGED_WITHOUT_REFRESH_AUDIT",
    }
)

CACHE_CATALOG_SAFETY = {
    "read_only": True,
    "data_refresh_allowed": False,
    "cache_mutation_allowed": False,
    "cache_repair_allowed": False,
    "score_or_backtest_allowed": False,
    "broker_action_allowed": False,
    "order_ticket_allowed": False,
    "production_state_mutation_allowed": False,
    "production_effect": PRODUCTION_EFFECT,
    "boundary": (
        "Cache catalog report only; reads configured cache artifacts, latest "
        "refresh audit and validate-data audit metadata, does not refresh data, "
        "repair or mutate cache, run scoring/backtests, or create broker/order actions."
    ),
}


class CacheCatalogIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class CacheCatalogIssue:
    severity: CacheCatalogIssueSeverity
    code: str
    message: str
    entry_id: str | None = None
    field: str | None = None


@dataclass(frozen=True)
class CacheCatalogValidationReport:
    as_of: date
    generated_at: datetime
    catalog_id: str
    catalog_path: Path
    entry_count: int
    issues: tuple[CacheCatalogIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == CacheCatalogIssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == CacheCatalogIssueSeverity.WARNING
        )

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    @property
    def status(self) -> str:
        if self.error_count:
            return "FAIL"
        if self.warning_count:
            return "PASS_WITH_WARNINGS"
        return "PASS"


def load_cache_catalog_policy(path: Path = DEFAULT_CACHE_CATALOG_POLICY_PATH) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Cache catalog policy must be a YAML object: {path}")
    return raw


def build_cache_catalog_payload(
    *,
    config: DataSourcesConfig,
    policy: Mapping[str, Any],
    as_of: date,
    output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
    expected_checksums: Mapping[str, str] | None = None,
    previous_catalog_path: Path | None = None,
    refresh_audit_report_path: Path | None = None,
    refresh_audit_output_dir: Path = DEFAULT_DATA_REFRESH_AUDIT_DIR,
    validation_audit_dir: Path = DEFAULT_VALIDATE_DATA_AUDIT_DIR,
    generated_at: datetime | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    refresh_audit = _latest_refresh_audit(
        report_path=refresh_audit_report_path,
        output_dir=refresh_audit_output_dir,
    )
    validation_audit = _latest_validate_data_audit(validation_audit_dir)
    previous_catalog = _previous_catalog(
        previous_catalog_path=previous_catalog_path,
        output_dir=output_dir,
    )
    previous_records = {
        _text(record.get("entry_id")): record
        for record in _records(previous_catalog.get("records"))
        if _text(record.get("entry_id"))
    }
    source_map = {source.source_id: source for source in config.sources}
    expected = {str(key): str(value) for key, value in (expected_checksums or {}).items()}
    build_issues: list[CacheCatalogIssue] = []
    records = [
        _entry_record(
            entry=entry,
            source_map=source_map,
            all_sources=config.sources,
            as_of=as_of,
            generated_at=generated,
            project_root=project_root,
            expected_checksums=expected,
            previous_records=previous_records,
            refresh_audit=refresh_audit,
            validation_audit=validation_audit,
            build_issues=build_issues,
        )
        for entry in _records(policy.get("entries"))
    ]
    summary = _summary(records=records, build_issues=build_issues)
    catalog_id = _catalog_id(as_of=as_of, generated_at=generated, records=records)
    payload: dict[str, Any] = {
        "schema_version": CACHE_CATALOG_SCHEMA_VERSION,
        "report_type": CACHE_CATALOG_REPORT_TYPE,
        "catalog_id": catalog_id,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": summary["status"],
        "validation_status": summary["status"],
        "cache_integrity_status": summary["cache_integrity_status"],
        "production_effect": PRODUCTION_EFFECT,
        "policy": _policy_metadata(policy),
        "policy_path": str(DEFAULT_CACHE_CATALOG_POLICY_PATH),
        "safety_boundary": _safety_boundary(policy),
        "refresh_audit_summary": refresh_audit,
        "validate_data_audit_summary": validation_audit,
        "summary": summary,
        "records": records,
        "build_issues": [_issue_dict(issue) for issue in build_issues],
        "methodology": {
            "entry_statuses": sorted(CACHE_ENTRY_STATUSES),
            "integrity_statuses": sorted(CACHE_INTEGRITY_STATUSES),
            "required_metadata_fields": list(_texts(policy.get("required_metadata_fields"))),
            "checksum_policy": (
                "Explicit expected checksum mismatches fail closed. A checksum "
                "change from the previous catalog fails closed when the linked "
                "refresh audit id did not change. New checksums with changed or "
                "missing prior lineage are reported through record metadata."
            ),
            "read_only": True,
            "does_not_refresh_data": True,
            "does_not_mutate_cache": True,
        },
    }
    return _with_validation_summary(payload, catalog_path=Path(""))


def build_and_write_cache_catalog(
    *,
    config: DataSourcesConfig,
    policy: Mapping[str, Any],
    as_of: date,
    output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
    expected_checksums: Mapping[str, str] | None = None,
    previous_catalog_path: Path | None = None,
    refresh_audit_report_path: Path | None = None,
    refresh_audit_output_dir: Path = DEFAULT_DATA_REFRESH_AUDIT_DIR,
    validation_audit_dir: Path = DEFAULT_VALIDATE_DATA_AUDIT_DIR,
    project_root: Path = PROJECT_ROOT,
) -> tuple[dict[str, Any], dict[str, Path]]:
    payload = build_cache_catalog_payload(
        config=config,
        policy=policy,
        as_of=as_of,
        output_dir=output_dir,
        expected_checksums=expected_checksums,
        previous_catalog_path=previous_catalog_path,
        refresh_audit_report_path=refresh_audit_report_path,
        refresh_audit_output_dir=refresh_audit_output_dir,
        validation_audit_dir=validation_audit_dir,
        project_root=project_root,
    )
    paths = write_cache_catalog_artifact(payload, output_dir=output_dir)
    return load_cache_catalog_payload(paths["catalog_json"]), paths


def write_cache_catalog_artifact(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
) -> dict[str, Path]:
    catalog_id = _text(payload.get("catalog_id"))
    if not catalog_id:
        raise ValueError("cache catalog payload missing catalog_id")
    artifact_dir = output_dir / catalog_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    catalog_path = artifact_dir / "cache_catalog.json"
    markdown_path = artifact_dir / "cache_catalog.md"
    validation_json_path = artifact_dir / "cache_catalog_validation.json"
    validation_md_path = artifact_dir / "cache_catalog_validation.md"
    reader_brief_path = artifact_dir / "reader_brief_section.md"

    payload_with_paths = dict(payload)
    payload_with_paths["artifact_paths"] = {
        "artifact_dir": str(artifact_dir),
        "catalog_json": str(catalog_path),
        "catalog_markdown": str(markdown_path),
        "validation_json": str(validation_json_path),
        "validation_markdown": str(validation_md_path),
        "reader_brief_section": str(reader_brief_path),
    }
    validation = validate_cache_catalog_payload(payload_with_paths, catalog_path=catalog_path)
    payload_with_paths = _with_validation_summary(
        payload_with_paths,
        catalog_path=catalog_path,
        validation=validation,
    )
    write_json_atomic_without_trailing_newline(catalog_path, payload_with_paths)
    write_text_atomic(markdown_path, render_cache_catalog_markdown(payload_with_paths))
    write_json_atomic_without_trailing_newline(
        validation_json_path, validation_report_to_payload(validation)
    )
    write_text_atomic(
        validation_md_path, render_cache_catalog_validation_markdown(validation)
    )
    write_text_atomic(
        reader_brief_path, render_cache_catalog_reader_brief(payload_with_paths)
    )
    _write_latest_pointer(
        output_dir=output_dir,
        payload=payload_with_paths,
        catalog_path=catalog_path,
    )
    return {
        "artifact_dir": artifact_dir,
        "catalog_json": catalog_path,
        "catalog_markdown": markdown_path,
        "validation_json": validation_json_path,
        "validation_markdown": validation_md_path,
        "reader_brief_section": reader_brief_path,
    }


def validate_cache_catalog_payload(
    payload: Mapping[str, Any],
    *,
    catalog_path: Path,
) -> CacheCatalogValidationReport:
    issues = [_issue_from_mapping(item) for item in _records(payload.get("build_issues"))]
    as_of = _parse_date(_text(payload.get("as_of"))) or date.today()
    catalog_id = _text(payload.get("catalog_id"), "UNKNOWN")
    records = _records(payload.get("records"))
    _check_top_level_contract(payload, issues)
    _check_safety_boundary(payload, issues)
    _check_records(records, payload, issues)
    return CacheCatalogValidationReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        catalog_id=catalog_id,
        catalog_path=catalog_path,
        entry_count=len(records),
        issues=tuple(issues),
    )


def validate_cache_catalog_artifact(
    *,
    catalog_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
) -> tuple[CacheCatalogValidationReport, Path]:
    catalog_path = resolve_cache_catalog_path(
        catalog_id=catalog_id,
        latest=latest,
        output_dir=output_dir,
    )
    payload = load_cache_catalog_payload(catalog_path)
    validation = validate_cache_catalog_payload(payload, catalog_path=catalog_path)
    artifact_dir = catalog_path.parent
    write_json_atomic_without_trailing_newline(
        artifact_dir / "cache_catalog_validation.json",
        validation_report_to_payload(validation),
    )
    write_text_atomic(
        artifact_dir / "cache_catalog_validation.md",
        render_cache_catalog_validation_markdown(validation),
    )
    updated = _with_validation_summary(payload, catalog_path=catalog_path, validation=validation)
    write_json_atomic_without_trailing_newline(catalog_path, updated)
    return validation, catalog_path


def resolve_cache_catalog_path(
    *,
    catalog_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
) -> Path:
    if catalog_id:
        candidate = output_dir / catalog_id / "cache_catalog.json"
        if not candidate.exists():
            raise FileNotFoundError(f"Cache catalog not found: {candidate}")
        return candidate
    if latest:
        latest_path = _latest_catalog_from_pointer(output_dir)
        if latest_path is not None:
            return latest_path
    candidates = sorted(
        output_dir.glob("*/cache_catalog.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No cache catalog artifacts found under {output_dir}")
    return candidates[0]


def load_cache_catalog_payload(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Cache catalog must be a JSON object: {path}")
    return raw


def latest_cache_catalog_summary(
    *,
    report_path: Path | None = None,
    output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
) -> dict[str, Any]:
    try:
        catalog_path = report_path or resolve_cache_catalog_path(latest=True, output_dir=output_dir)
        payload = load_cache_catalog_payload(catalog_path)
    except FileNotFoundError:
        return _missing_cache_catalog_summary("cache_catalog artifact missing.")
    except Exception as exc:
        return _missing_cache_catalog_summary(f"cache_catalog artifact unreadable: {exc}")
    summary = _mapping(payload.get("summary"))
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "validation_status": _text(
            payload.get("validation_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "cache_integrity_status": _text(
            payload.get("cache_integrity_status"),
            _text(summary.get("cache_integrity_status"), "UNKNOWN"),
        ),
        "catalog_id": _text(payload.get("catalog_id"), "UNKNOWN"),
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "entry_count": _int(summary.get("entry_count")),
        "required_entry_count": _int(summary.get("required_entry_count")),
        "missing_required_count": _int(summary.get("missing_required_count")),
        "missing_optional_count": _int(summary.get("missing_optional_count")),
        "checksum_mismatch_count": _int(summary.get("checksum_mismatch_count")),
        "checksum_changed_without_refresh_count": _int(
            summary.get("checksum_changed_without_refresh_count")
        ),
        "blocking_entry_count": _int(summary.get("blocking_entry_count")),
        "blocking_entry_ids": _texts(summary.get("blocking_entry_ids")),
        "warning_entry_ids": _texts(summary.get("warning_entry_ids")),
        "refresh_audit_id": _text(summary.get("refresh_audit_id"), "UNKNOWN"),
        "validated_at": _text(summary.get("validated_at"), "UNKNOWN"),
        "next_action": _text(summary.get("next_action"), "UNKNOWN"),
        "report_path": str(catalog_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "limitation": (
            "Cache catalog is governance-only; it records observed cache metadata "
            "and does not refresh, repair or approve production use."
        ),
    }


def render_cache_catalog_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    refresh = _mapping(payload.get("refresh_audit_summary"))
    validation = _mapping(payload.get("validate_data_audit_summary"))
    lines = [
        f"# Cache Catalog {payload.get('as_of')}",
        "",
        f"- Status: `{_text(payload.get('status'), 'UNKNOWN')}`",
        f"- Cache integrity status: `{_text(payload.get('cache_integrity_status'), 'UNKNOWN')}`",
        f"- Catalog id: `{_text(payload.get('catalog_id'), 'UNKNOWN')}`",
        f"- Entry count: `{_text(summary.get('entry_count'), '0')}`",
        f"- Missing required count: `{_text(summary.get('missing_required_count'), '0')}`",
        f"- Checksum mismatch count: `{_text(summary.get('checksum_mismatch_count'), '0')}`",
        f"- Refresh audit id: `{_text(refresh.get('audit_id'), 'UNKNOWN')}`",
        f"- Validate-data audit id: `{_text(validation.get('audit_record_id'), 'UNKNOWN')}`",
        f"- Next action: `{_text(summary.get('next_action'), 'UNKNOWN')}`",
        f"- production_effect: `{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}`",
        "",
        "This report is read-only cache governance. It records observed cache metadata "
        "and does not refresh data, repair cache, run scoring/backtests, "
        "or create broker/order actions.",
        "",
        "## Cache Entries",
        "",
        "| Entry | Data type | Required | Status | As-of | Rows | Columns | Checksum | "
        "Source | Refresh audit | Path |",
        "|---|---|---:|---|---|---:|---:|---|---|---|---|",
    ]
    for record in _records(payload.get("records")):
        checksum = _text(record.get("checksum"))
        lines.append(
            "| "
            f"`{_text(record.get('entry_id'))}` | "
            f"{_text(record.get('data_type'))} | "
            f"{_text(record.get('required'))} | "
            f"{_text(record.get('status'))} | "
            f"{_text(record.get('as_of'), 'UNKNOWN')} | "
            f"{_text(record.get('row_count'), '0')} | "
            f"{_text(record.get('column_count'), '0')} | "
            f"`{checksum[:12] if checksum else 'MISSING'}` | "
            f"{_escape_markdown_table(_text(record.get('source_name'), 'UNKNOWN'))} | "
            f"`{_text(record.get('refresh_audit_id'), 'UNKNOWN')}` | "
            f"`{_text(record.get('cache_path'), 'MISSING')}` |"
        )
    issues = _records(payload.get("build_issues"))
    if issues:
        lines.extend(
            [
                "",
                "## Issues",
                "",
                "| Severity | Code | Entry | Message |",
                "|---|---|---|---|",
            ]
        )
        for issue in issues:
            lines.append(
                "| "
                f"{_text(issue.get('severity'))} | "
                f"`{_text(issue.get('code'))}` | "
                f"`{_text(issue.get('entry_id'), '')}` | "
                f"{_escape_markdown_table(_text(issue.get('message')))} |"
            )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- read_only: `true`",
            "- data_refresh_allowed: `false`",
            "- cache_mutation_allowed: `false`",
            "- cache_repair_allowed: `false`",
            "- score_or_backtest_allowed: `false`",
            "- broker_action_allowed: `false`",
            "- order_ticket_allowed: `false`",
            "- production_effect: `none`",
        ]
    )
    return "\n".join(lines) + "\n"


def render_cache_catalog_reader_brief(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    cache_integrity_status = _text(payload.get("cache_integrity_status"), "UNKNOWN")
    blocking_entries = ", ".join(_texts(summary.get("blocking_entry_ids"))) or "none"
    return "\n".join(
        [
            "### Cache Catalog",
            "",
            f"- Status: `{_text(payload.get('status'), 'UNKNOWN')}`",
            f"- Cache integrity status: `{cache_integrity_status}`",
            f"- Entry count: `{_text(summary.get('entry_count'), '0')}`",
            f"- Missing required count: `{_text(summary.get('missing_required_count'), '0')}`",
            f"- Checksum mismatch count: `{_text(summary.get('checksum_mismatch_count'), '0')}`",
            f"- Blocking entries: `{blocking_entries}`",
            f"- Next action: `{_text(summary.get('next_action'), 'UNKNOWN')}`",
            "- Boundary: read-only cache governance; no refresh, repair, scoring, "
            "broker or order action.",
            "",
        ]
    )


def render_cache_catalog_validation_markdown(report: CacheCatalogValidationReport) -> str:
    lines = [
        f"# Cache Catalog Validation {report.as_of.isoformat()}",
        "",
        f"- Status: `{report.status}`",
        f"- Catalog id: `{report.catalog_id}`",
        f"- Entry count: `{report.entry_count}`",
        f"- Error count: `{report.error_count}`",
        f"- Warning count: `{report.warning_count}`",
        f"- Catalog path: `{report.catalog_path}`",
        "",
        "## Issues",
        "",
    ]
    if report.issues:
        lines.extend(["| Severity | Code | Entry | Field | Message |", "|---|---|---|---|---|"])
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity.value} | "
                f"`{issue.code}` | "
                f"`{issue.entry_id or ''}` | "
                f"`{issue.field or ''}` | "
                f"{_escape_markdown_table(issue.message)} |"
            )
    else:
        lines.append("- No issues.")
    lines.extend(
        [
            "",
            "Validation is read-only and does not refresh, repair, mutate, "
            "score, backtest, or trade.",
        ]
    )
    return "\n".join(lines) + "\n"


def validation_report_to_payload(report: CacheCatalogValidationReport) -> dict[str, Any]:
    return {
        "schema_version": CACHE_CATALOG_SCHEMA_VERSION,
        "report_type": CACHE_CATALOG_VALIDATION_REPORT_TYPE,
        "catalog_id": report.catalog_id,
        "as_of": report.as_of.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "status": report.status,
        "passed": report.passed,
        "entry_count": report.entry_count,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "issues": [_issue_dict(issue) for issue in report.issues],
        "catalog_path": str(report.catalog_path),
        "production_effect": PRODUCTION_EFFECT,
    }


def _entry_record(
    *,
    entry: Mapping[str, Any],
    source_map: Mapping[str, DataSourceConfig],
    all_sources: Sequence[DataSourceConfig],
    as_of: date,
    generated_at: datetime,
    project_root: Path,
    expected_checksums: Mapping[str, str],
    previous_records: Mapping[str, Mapping[str, Any]],
    refresh_audit: Mapping[str, Any],
    validation_audit: Mapping[str, Any],
    build_issues: list[CacheCatalogIssue],
) -> dict[str, Any]:
    entry_id = _text(entry.get("entry_id"), "UNKNOWN")
    required = bool(entry.get("required", False))
    path = _resolve_entry_path(entry=entry, project_root=project_root)
    relative_path = (
        _relative_path(path, project_root)
        if path is not None
        else _text(entry.get("path_glob"))
    )
    exists = path is not None and path.exists()
    source_ids = _texts(entry.get("source_ids")) or _source_ids_for_path(
        relative_path=relative_path,
        all_sources=all_sources,
    )
    sources = [source_map[source_id] for source_id in source_ids if source_id in source_map]
    source_names = _source_names(entry=entry, sources=sources)
    file_info = _file_info(
        path=path,
        entry=entry,
        project_root=project_root,
        as_of=as_of,
        build_issues=build_issues,
    )
    expected_checksum = _text(expected_checksums.get(entry_id)) or _text(
        entry.get("expected_checksum")
    )
    previous = _mapping(previous_records.get(entry_id))
    previous_checksum = _text(previous.get("checksum"))
    refresh_audit_id = _text(refresh_audit.get("audit_id"), "MISSING")
    previous_refresh_audit_id = _text(previous.get("refresh_audit_id"))
    status = "OK"
    checksum_mismatch = False
    checksum_changed_without_refresh = False
    if not exists:
        status = "MISSING_REQUIRED" if required else "MISSING_OPTIONAL"
        build_issues.append(
            CacheCatalogIssue(
                severity=(
                    CacheCatalogIssueSeverity.ERROR
                    if required
                    else CacheCatalogIssueSeverity.WARNING
                ),
                code="required_cache_entry_missing" if required else "optional_cache_entry_missing",
                message=f"Cache entry path is missing: {relative_path}",
                entry_id=entry_id,
                field="cache_path",
            )
        )
    elif _text(file_info.get("read_status")) == "UNREADABLE":
        status = "UNREADABLE"
        build_issues.append(
            CacheCatalogIssue(
                severity=CacheCatalogIssueSeverity.ERROR,
                code="cache_entry_unreadable",
                message=f"Cache entry exists but could not be read: {relative_path}",
                entry_id=entry_id,
                field="cache_path",
            )
        )
    elif expected_checksum and file_info.get("checksum") != expected_checksum:
        status = "CHECKSUM_MISMATCH"
        checksum_mismatch = True
        build_issues.append(
            CacheCatalogIssue(
                severity=CacheCatalogIssueSeverity.ERROR,
                code="checksum_mismatch",
                message=(
                    f"Observed checksum for {entry_id} does not match the explicit "
                    "expected checksum."
                ),
                entry_id=entry_id,
                field="checksum",
            )
        )
    elif (
        previous_checksum
        and file_info.get("checksum")
        and previous_checksum != file_info.get("checksum")
        and previous_refresh_audit_id
        and previous_refresh_audit_id == refresh_audit_id
    ):
        status = "CHECKSUM_CHANGED_WITHOUT_REFRESH_AUDIT"
        checksum_changed_without_refresh = True
        build_issues.append(
            CacheCatalogIssue(
                severity=CacheCatalogIssueSeverity.ERROR,
                code="checksum_changed_without_refresh_audit_change",
                message=(
                    f"Observed checksum for {entry_id} changed from the previous "
                    "catalog while refresh_audit_id did not change."
                ),
                entry_id=entry_id,
                field="checksum",
            )
        )
    artifact_id = _artifact_id(
        entry_id=entry_id,
        as_of=_text(file_info.get("as_of"), as_of.isoformat()),
        checksum=_text(file_info.get("checksum")),
    )
    return {
        "entry_id": entry_id,
        "title": _text(entry.get("title"), entry_id),
        "data_type": _text(entry.get("data_type"), "UNKNOWN"),
        "required": required,
        "status": status,
        "exists": exists,
        "cache_path": str(path) if path is not None else relative_path,
        "cache_path_relative": relative_path,
        "artifact_id": artifact_id,
        "as_of": _text(file_info.get("as_of"), as_of.isoformat()),
        "checksum": _text(file_info.get("checksum")),
        "expected_checksum": expected_checksum,
        "previous_checksum": previous_checksum,
        "checksum_mismatch": checksum_mismatch,
        "checksum_changed_without_refresh_audit": checksum_changed_without_refresh,
        "row_count": _int(file_info.get("row_count")),
        "column_count": _int(file_info.get("column_count")),
        "created_at": _text(file_info.get("created_at"), "UNKNOWN"),
        "modified_at": _text(file_info.get("modified_at"), "UNKNOWN"),
        "validated_at": _text(
            validation_audit.get("ended_at")
            or validation_audit.get("generated_at")
            or validation_audit.get("as_of"),
            "MISSING",
        ),
        "validated_at_source": _text(validation_audit.get("source"), "latest_validate_data_audit"),
        "source_ids": source_ids,
        "source_name": ", ".join(source_names) if source_names else "UNKNOWN",
        "source_names": source_names,
        "source_providers": [source.provider for source in sources],
        "source_statuses": {source.source_id: source.status for source in sources},
        "refresh_audit_id": refresh_audit_id,
        "refresh_audit_status": _text(refresh_audit.get("status"), "MISSING"),
        "refresh_audit_path": _text(refresh_audit.get("audit_path")),
        "validation_audit_record_id": _text(validation_audit.get("audit_record_id"), "MISSING"),
        "validation_audit_status": _text(validation_audit.get("status"), "MISSING"),
        "format": _text(entry.get("format"), "UNKNOWN"),
        "read_status": _text(file_info.get("read_status"), "UNKNOWN"),
        "metadata_complete": _record_metadata_complete(
            {
                "entry_id": entry_id,
                "data_type": _text(entry.get("data_type")),
                "cache_path": str(path) if path is not None else relative_path,
                "artifact_id": artifact_id,
                "as_of": _text(file_info.get("as_of"), as_of.isoformat()),
                "checksum": _text(file_info.get("checksum")),
                "row_count": _int(file_info.get("row_count")),
                "column_count": _int(file_info.get("column_count")),
                "created_at": _text(file_info.get("created_at"), "UNKNOWN"),
                "validated_at": _text(validation_audit.get("as_of"), "MISSING"),
                "source_name": ", ".join(source_names) if source_names else "",
                "refresh_audit_id": refresh_audit_id,
            },
            _texts(entry.get("required_metadata_fields")),
        ),
    }


def _file_info(
    *,
    path: Path | None,
    entry: Mapping[str, Any],
    project_root: Path,
    as_of: date,
    build_issues: list[CacheCatalogIssue],
) -> dict[str, Any]:
    if path is None or not path.exists():
        return {
            "read_status": "MISSING",
            "as_of": as_of.isoformat(),
            "checksum": "",
            "row_count": 0,
            "column_count": 0,
            "created_at": "MISSING",
            "modified_at": "MISSING",
        }
    stat = path.stat()
    base = {
        "checksum": _sha256_file(path),
        "created_at": _datetime_from_timestamp(getattr(stat, "st_ctime", stat.st_mtime)),
        "modified_at": _datetime_from_timestamp(stat.st_mtime),
    }
    fmt = _text(entry.get("format"), path.suffix.removeprefix(".")).lower()
    try:
        if fmt == "json":
            data = json.loads(path.read_text(encoding="utf-8"))
            dimensions = _json_dimensions(data, entry=entry)
            return {
                **base,
                "read_status": "OK",
                "as_of": _json_as_of(data, entry=entry, fallback=as_of),
                **dimensions,
            }
        dimensions = _csv_dimensions(path=path, entry=entry, as_of=as_of)
        return {
            **base,
            "read_status": "OK",
            **dimensions,
        }
    except Exception as exc:
        build_issues.append(
            CacheCatalogIssue(
                severity=CacheCatalogIssueSeverity.ERROR,
                code="cache_entry_read_error",
                message=f"Failed to inspect cache file {path}: {exc}",
                entry_id=_text(entry.get("entry_id"), "UNKNOWN"),
                field="cache_path",
            )
        )
        return {
            **base,
            "read_status": "UNREADABLE",
            "as_of": as_of.isoformat(),
            "row_count": 0,
            "column_count": 0,
        }


def _csv_dimensions(*, path: Path, entry: Mapping[str, Any], as_of: date) -> dict[str, Any]:
    row_count = 0
    column_count = 0
    latest_date: date | None = None
    date_column = _text(entry.get("date_column"))
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        column_count = len(fieldnames)
        for row in reader:
            row_count += 1
            if date_column and date_column in row:
                parsed = _parse_date(_text(row.get(date_column)))
                if parsed is not None and (latest_date is None or parsed > latest_date):
                    latest_date = parsed
    return {
        "row_count": row_count,
        "column_count": column_count,
        "as_of": (latest_date or as_of).isoformat(),
    }


def _json_dimensions(data: Any, *, entry: Mapping[str, Any]) -> dict[str, int]:
    target = _json_path_value(data, _text(entry.get("row_count_json_path")))
    if isinstance(target, list):
        row_count = len(target)
        keys = set()
        for item in target:
            if isinstance(item, Mapping):
                keys.update(str(key) for key in item)
        return {"row_count": row_count, "column_count": len(keys)}
    if isinstance(data, list):
        keys = set()
        for item in data:
            if isinstance(item, Mapping):
                keys.update(str(key) for key in item)
        return {"row_count": len(data), "column_count": len(keys)}
    if isinstance(data, Mapping):
        return {"row_count": 1, "column_count": len(data)}
    return {"row_count": 1, "column_count": 1}


def _json_as_of(data: Any, *, entry: Mapping[str, Any], fallback: date) -> str:
    value = _json_path_value(data, _text(entry.get("as_of_json_path"), "as_of"))
    parsed = _parse_date(_text(value))
    return (parsed or fallback).isoformat()


def _json_path_value(data: Any, path: str) -> Any:
    if not path:
        return None
    value: Any = data
    for part in path.split("."):
        if isinstance(value, Mapping):
            value = value.get(part)
        else:
            return None
    return value


def _summary(
    *,
    records: Sequence[Mapping[str, Any]],
    build_issues: Sequence[CacheCatalogIssue],
) -> dict[str, Any]:
    required_records = [record for record in records if record.get("required") is True]
    missing_required = [
        record for record in records if record.get("status") == "MISSING_REQUIRED"
    ]
    missing_optional = [
        record for record in records if record.get("status") == "MISSING_OPTIONAL"
    ]
    checksum_mismatch = [
        record for record in records if record.get("status") == "CHECKSUM_MISMATCH"
    ]
    checksum_changed_without_refresh = [
        record
        for record in records
        if record.get("status") == "CHECKSUM_CHANGED_WITHOUT_REFRESH_AUDIT"
    ]
    unreadable = [record for record in records if record.get("status") == "UNREADABLE"]
    blocking = [
        record
        for record in records
        if record.get("status")
        in {
            "MISSING_REQUIRED",
            "UNREADABLE",
            "CHECKSUM_MISMATCH",
            "CHECKSUM_CHANGED_WITHOUT_REFRESH_AUDIT",
        }
    ]
    warning = [record for record in records if record.get("status") == "MISSING_OPTIONAL"]
    error_count = sum(
        1 for issue in build_issues if issue.severity == CacheCatalogIssueSeverity.ERROR
    )
    warning_count = sum(
        1 for issue in build_issues if issue.severity == CacheCatalogIssueSeverity.WARNING
    )
    status = "FAIL" if error_count else "PASS_WITH_WARNINGS" if warning_count else "PASS"
    integrity_status = "FAIL" if blocking else "WARNING" if warning else "OK"
    refresh_audit_ids = _texts(record.get("refresh_audit_id") for record in records)
    validated_at_values = _texts(record.get("validated_at") for record in records)
    return {
        "status": status,
        "validation_status": status,
        "cache_integrity_status": integrity_status,
        "entry_count": len(records),
        "required_entry_count": len(required_records),
        "missing_required_count": len(missing_required),
        "missing_optional_count": len(missing_optional),
        "unreadable_count": len(unreadable),
        "checksum_mismatch_count": len(checksum_mismatch),
        "checksum_changed_without_refresh_count": len(checksum_changed_without_refresh),
        "blocking_entry_count": len(blocking),
        "warning_entry_count": len(warning),
        "blocking_entry_ids": [_text(record.get("entry_id")) for record in blocking],
        "warning_entry_ids": [_text(record.get("entry_id")) for record in warning],
        "refresh_audit_id": refresh_audit_ids[0] if refresh_audit_ids else "MISSING",
        "validated_at": validated_at_values[0] if validated_at_values else "MISSING",
        "error_count": error_count,
        "warning_count": warning_count,
        "next_action": _next_action(
            blocking=blocking,
            warning=warning,
            error_count=error_count,
        ),
    }


def _next_action(
    *,
    blocking: Sequence[Mapping[str, Any]],
    warning: Sequence[Mapping[str, Any]],
    error_count: int,
) -> str:
    if blocking or error_count:
        return "repair_cache_lineage_then_rerun_validate_data_and_cache_catalog"
    if warning:
        return "review_optional_cache_gap_before_interpreting_dependent_reports"
    return "cache_catalog_clear_for_manual_review"


def _check_top_level_contract(
    payload: Mapping[str, Any],
    issues: list[CacheCatalogIssue],
) -> None:
    if payload.get("schema_version") != CACHE_CATALOG_SCHEMA_VERSION:
        issues.append(_error("invalid_schema_version", "Invalid cache catalog schema version."))
    if payload.get("report_type") != CACHE_CATALOG_REPORT_TYPE:
        issues.append(_error("invalid_report_type", "Invalid cache catalog report_type."))
    if _text(payload.get("status")) not in CACHE_CATALOG_STATUSES:
        issues.append(_error("invalid_status", "Invalid cache catalog status.", field="status"))
    if _text(payload.get("cache_integrity_status")) not in CACHE_INTEGRITY_STATUSES:
        issues.append(
            _error(
                "invalid_cache_integrity_status",
                "Invalid cache_integrity_status.",
                field="cache_integrity_status",
            )
        )
    if _text(payload.get("production_effect")) != PRODUCTION_EFFECT:
        issues.append(
            _error(
                "invalid_production_effect",
                "Cache catalog must keep production_effect=none.",
                field="production_effect",
            )
        )


def _check_safety_boundary(
    payload: Mapping[str, Any],
    issues: list[CacheCatalogIssue],
) -> None:
    safety = _mapping(payload.get("safety_boundary"))
    expected_false = (
        "data_refresh_allowed",
        "cache_mutation_allowed",
        "cache_repair_allowed",
        "score_or_backtest_allowed",
        "broker_action_allowed",
        "order_ticket_allowed",
        "production_state_mutation_allowed",
    )
    if safety.get("read_only") is not True:
        issues.append(_error("safety_read_only_missing", "Cache catalog must be read-only."))
    for field_name in expected_false:
        if safety.get(field_name) is not False:
            issues.append(
                _error(
                    "safety_boundary_violation",
                    f"{field_name} must be false.",
                    field=field_name,
                )
            )
    if _text(safety.get("production_effect")) != PRODUCTION_EFFECT:
        issues.append(
            _error(
                "safety_production_effect_violation",
                "Safety boundary must keep production_effect=none.",
                field="production_effect",
            )
        )


def _check_records(
    records: Sequence[Mapping[str, Any]],
    payload: Mapping[str, Any],
    issues: list[CacheCatalogIssue],
) -> None:
    required_fields = _texts(_mapping(payload.get("policy")).get("required_metadata_fields"))
    if not records:
        issues.append(_error("cache_catalog_empty", "Cache catalog must contain records."))
    seen: set[str] = set()
    for record in records:
        entry_id = _text(record.get("entry_id"), "UNKNOWN")
        if entry_id in seen:
            issues.append(
                _error(
                    "duplicate_cache_entry_id",
                    f"Duplicate cache entry id: {entry_id}",
                    entry_id=entry_id,
                    field="entry_id",
                )
            )
        seen.add(entry_id)
        if _text(record.get("status")) not in CACHE_ENTRY_STATUSES:
            issues.append(
                _error(
                    "invalid_cache_entry_status",
                    f"Invalid cache entry status: {record.get('status')}",
                    entry_id=entry_id,
                    field="status",
                )
            )
        for field_name in required_fields:
            value = record.get(field_name)
            if value in (None, "", "MISSING", "UNKNOWN"):
                issues.append(
                    _error(
                        "cache_entry_metadata_missing",
                        f"Cache entry missing required metadata field: {field_name}",
                        entry_id=entry_id,
                        field=field_name,
                    )
                )
        if record.get("required") is True and record.get("exists") is not True:
            issues.append(
                _error(
                    "required_cache_entry_missing",
                    f"Required cache entry is missing: {entry_id}",
                    entry_id=entry_id,
                    field="cache_path",
                )
            )
        if _text(record.get("checksum_mismatch")).lower() == "true":
            issues.append(
                _error(
                    "checksum_mismatch",
                    f"Checksum mismatch for cache entry: {entry_id}",
                    entry_id=entry_id,
                    field="checksum",
                )
            )
        if _text(record.get("checksum_changed_without_refresh_audit")).lower() == "true":
            issues.append(
                _error(
                    "checksum_changed_without_refresh_audit_change",
                    f"Checksum changed without refresh audit id change: {entry_id}",
                    entry_id=entry_id,
                    field="checksum",
                )
            )


def _with_validation_summary(
    payload: Mapping[str, Any],
    *,
    catalog_path: Path,
    validation: CacheCatalogValidationReport | None = None,
) -> dict[str, Any]:
    output = dict(payload)
    report = validation or validate_cache_catalog_payload(output, catalog_path=catalog_path)
    status = report.status
    summary = dict(_mapping(output.get("summary")))
    summary["status"] = status
    summary["validation_status"] = status
    summary["validation_error_count"] = report.error_count
    summary["validation_warning_count"] = report.warning_count
    output["summary"] = summary
    output["status"] = status
    output["validation_status"] = status
    output["error_count"] = report.error_count
    output["warning_count"] = report.warning_count
    return output


def _policy_metadata(policy: Mapping[str, Any]) -> dict[str, Any]:
    metadata = _mapping(policy.get("policy_metadata"))
    return {
        "policy_version": _text(policy.get("policy_version"), CACHE_CATALOG_POLICY_VERSION),
        "owner": _text(metadata.get("owner"), "system"),
        "status": _text(metadata.get("status"), "pilot_baseline"),
        "rationale": _text(metadata.get("rationale")),
        "intended_effect": _text(metadata.get("intended_effect")),
        "validation_evidence": _text(metadata.get("validation_evidence")),
        "review_condition": _text(metadata.get("review_condition")),
        "required_metadata_fields": _texts(policy.get("required_metadata_fields")),
    }


def _safety_boundary(policy: Mapping[str, Any]) -> dict[str, Any]:
    safety = dict(CACHE_CATALOG_SAFETY)
    safety.update(_mapping(policy.get("safety_boundary")))
    safety["production_effect"] = PRODUCTION_EFFECT
    return safety


def _resolve_entry_path(*, entry: Mapping[str, Any], project_root: Path) -> Path | None:
    raw_path = _text(entry.get("path"))
    if raw_path:
        return _project_path(raw_path, project_root)
    raw_glob = _text(entry.get("path_glob"))
    if not raw_glob:
        return None
    pattern = _project_path(raw_glob, project_root)
    candidates = sorted(
        pattern.parent.glob(pattern.name),
        key=lambda candidate: candidate.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _project_path(raw_path: str, project_root: Path) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def _relative_path(path: Path, project_root: Path) -> str:
    try:
        return str(path.relative_to(project_root))
    except ValueError:
        return str(path)


def _source_ids_for_path(
    *,
    relative_path: str,
    all_sources: Sequence[DataSourceConfig],
) -> list[str]:
    normalized = relative_path.replace("\\", "/")
    source_ids: list[str] = []
    for source in all_sources:
        for cache_path in source.cache_paths:
            pattern = cache_path.replace("\\", "/")
            if normalized == pattern or fnmatch.fnmatch(normalized, pattern):
                source_ids.append(source.source_id)
                break
    return source_ids


def _source_names(
    *,
    entry: Mapping[str, Any],
    sources: Sequence[DataSourceConfig],
) -> list[str]:
    explicit = _text(entry.get("source_name"))
    if explicit:
        return [explicit]
    return [source.provider for source in sources]


def _record_metadata_complete(
    record: Mapping[str, Any],
    required_fields: Sequence[str],
) -> bool:
    fields = tuple(required_fields) or (
        "entry_id",
        "data_type",
        "cache_path",
        "artifact_id",
        "as_of",
        "checksum",
        "row_count",
        "column_count",
        "created_at",
        "validated_at",
        "source_name",
        "refresh_audit_id",
    )
    return all(record.get(field) not in (None, "", "MISSING", "UNKNOWN") for field in fields)


def _latest_refresh_audit(
    *,
    report_path: Path | None,
    output_dir: Path,
) -> dict[str, Any]:
    try:
        path = report_path or _latest_pointer_path(
            output_dir=output_dir,
            pointer_name=LATEST_DATA_REFRESH_AUDIT_POINTER_NAME,
            path_field="audit_path",
            glob_pattern="*/data_refresh_audit.json",
        )
        if path is None:
            return {
                "availability": "MISSING",
                "status": "MISSING",
                "audit_id": "MISSING",
                "audit_path": "",
                "source": "latest_data_refresh_audit",
            }
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("data refresh audit payload is not an object")
        return {
            "availability": "AVAILABLE",
            "status": _text(payload.get("status"), "UNKNOWN"),
            "validation_status": _text(payload.get("validation_status"), "UNKNOWN"),
            "audit_id": _text(payload.get("audit_id"), "UNKNOWN"),
            "audit_path": str(path),
            "as_of": _text(payload.get("as_of"), "UNKNOWN"),
            "generated_at": _text(payload.get("generated_at"), "UNKNOWN"),
            "source": "latest_data_refresh_audit",
        }
    except Exception as exc:
        return {
            "availability": "UNREADABLE",
            "status": "MISSING",
            "audit_id": "MISSING",
            "audit_path": str(report_path or output_dir),
            "source": "latest_data_refresh_audit",
            "error": str(exc),
        }


def _latest_validate_data_audit(output_dir: Path) -> dict[str, Any]:
    pointer = output_dir / LATEST_VALIDATE_DATA_AUDIT_POINTER_NAME
    try:
        if not pointer.exists():
            return {
                "availability": "MISSING",
                "status": "MISSING",
                "audit_record_id": "MISSING",
                "source": "latest_validate_data_audit",
            }
        pointer_payload = json.loads(pointer.read_text(encoding="utf-8"))
        record_path = Path(_text(_mapping(pointer_payload).get("record_path")))
        record = (
            json.loads(record_path.read_text(encoding="utf-8"))
            if record_path.exists()
            else pointer_payload
        )
        if not isinstance(record, Mapping):
            raise ValueError("validate-data audit payload is not an object")
        return {
            "availability": "AVAILABLE",
            "status": _text(record.get("status"), _text(pointer_payload.get("status"), "UNKNOWN")),
            "audit_record_id": _text(
                record.get("audit_record_id"),
                _text(pointer_payload.get("audit_record_id"), "UNKNOWN"),
            ),
            "record_path": str(record_path),
            "as_of": _text(record.get("as_of"), _text(pointer_payload.get("as_of"), "UNKNOWN")),
            "generated_at": _text(record.get("end_time"), _text(record.get("generated_at"))),
            "ended_at": _text(record.get("end_time")),
            "source": "latest_validate_data_audit",
        }
    except Exception as exc:
        return {
            "availability": "UNREADABLE",
            "status": "MISSING",
            "audit_record_id": "MISSING",
            "source": "latest_validate_data_audit",
            "error": str(exc),
        }


def _previous_catalog(
    *,
    previous_catalog_path: Path | None,
    output_dir: Path,
) -> dict[str, Any]:
    try:
        path = previous_catalog_path or _latest_catalog_from_pointer(output_dir)
        if path is None:
            return {}
        return load_cache_catalog_payload(path)
    except Exception:
        return {}


def _latest_catalog_from_pointer(output_dir: Path) -> Path | None:
    return _latest_pointer_path(
        output_dir=output_dir,
        pointer_name=LATEST_POINTER_NAME,
        path_field="catalog_path",
        glob_pattern="*/cache_catalog.json",
    )


def _latest_pointer_path(
    *,
    output_dir: Path,
    pointer_name: str,
    path_field: str,
    glob_pattern: str,
) -> Path | None:
    pointer = output_dir / pointer_name
    if pointer.exists():
        try:
            raw = json.loads(pointer.read_text(encoding="utf-8"))
            path = Path(_text(_mapping(raw).get(path_field)))
            if path.exists():
                return path
        except Exception:
            pass
    candidates = sorted(
        output_dir.glob(glob_pattern),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _write_latest_pointer(
    *,
    output_dir: Path,
    payload: Mapping[str, Any],
    catalog_path: Path,
) -> None:
    write_json_atomic_without_trailing_newline(
        output_dir / LATEST_POINTER_NAME,
        {
            "schema_version": CACHE_CATALOG_SCHEMA_VERSION,
            "report_type": CACHE_CATALOG_REPORT_TYPE,
            "catalog_id": _text(payload.get("catalog_id")),
            "catalog_path": str(catalog_path),
            "as_of": _text(payload.get("as_of")),
            "status": _text(payload.get("status")),
            "cache_integrity_status": _text(payload.get("cache_integrity_status")),
            "generated_at": _text(payload.get("generated_at")),
            "production_effect": PRODUCTION_EFFECT,
        },
    )


def _artifact_id(*, entry_id: str, as_of: str, checksum: str) -> str:
    basis = f"{entry_id}|{as_of}|{checksum}"
    return f"{entry_id}_{sha256(basis.encode('utf-8')).hexdigest()[:16]}"


def _catalog_id(
    *,
    as_of: date,
    generated_at: datetime,
    records: Sequence[Mapping[str, Any]],
) -> str:
    basis = json.dumps(
        {
            "as_of": as_of.isoformat(),
            "generated_at": generated_at.isoformat(),
            "records": [
                {
                    "entry_id": record.get("entry_id"),
                    "checksum": record.get("checksum"),
                    "status": record.get("status"),
                }
                for record in records
            ],
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return f"cache-catalog_{as_of.isoformat()}_{sha256(basis.encode('utf-8')).hexdigest()[:16]}"


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _datetime_from_timestamp(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat()


def _missing_cache_catalog_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "cache_integrity_status": "MISSING",
        "catalog_id": "MISSING",
        "as_of": "UNKNOWN",
        "entry_count": 0,
        "required_entry_count": 0,
        "missing_required_count": 0,
        "missing_optional_count": 0,
        "checksum_mismatch_count": 0,
        "checksum_changed_without_refresh_count": 0,
        "blocking_entry_count": 0,
        "blocking_entry_ids": [],
        "warning_entry_ids": [],
        "refresh_audit_id": "MISSING",
        "validated_at": "MISSING",
        "next_action": "generate_cache_catalog",
        "report_path": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "limitation": reason,
    }


def _issue_from_mapping(payload: Mapping[str, Any]) -> CacheCatalogIssue:
    severity = (
        CacheCatalogIssueSeverity.ERROR
        if _text(payload.get("severity")) == CacheCatalogIssueSeverity.ERROR.value
        else CacheCatalogIssueSeverity.WARNING
    )
    return CacheCatalogIssue(
        severity=severity,
        code=_text(payload.get("code"), "unknown_issue"),
        message=_text(payload.get("message")),
        entry_id=_text(payload.get("entry_id")) or None,
        field=_text(payload.get("field")) or None,
    )


def _issue_dict(issue: CacheCatalogIssue) -> dict[str, Any]:
    return {
        "severity": issue.severity.value,
        "code": issue.code,
        "message": issue.message,
        "entry_id": issue.entry_id,
        "field": issue.field,
    }


def _error(
    code: str,
    message: str,
    *,
    entry_id: str | None = None,
    field: str | None = None,
) -> CacheCatalogIssue:
    return CacheCatalogIssue(
        severity=CacheCatalogIssueSeverity.ERROR,
        code=code,
        message=message,
        entry_id=entry_id,
        field=field,
    )


def _records(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, Mapping)]
    if isinstance(value, tuple):
        return [item for item in value if isinstance(item, Mapping)]
    return []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _texts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Mapping):
        return [str(key) for key in value if str(key)]
    try:
        return [str(item) for item in value if str(item)]
    except TypeError:
        return [str(value)] if str(value) else []


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _parse_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
