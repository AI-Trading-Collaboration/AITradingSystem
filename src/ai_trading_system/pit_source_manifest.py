from __future__ import annotations

import json
import re
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from glob import glob
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT, DataSourceConfig, DataSourcesConfig

PIT_SOURCE_MANIFEST_SCHEMA_VERSION = 1
PIT_SOURCE_MANIFEST_REPORT_TYPE = "pit_source_manifest"
PIT_SOURCE_MANIFEST_POLICY_VERSION = "pit_source_manifest_policy_v1"
PIT_SOURCE_MANIFEST_VALIDATION_REPORT_TYPE = "pit_source_manifest_validation"
PRODUCTION_EFFECT = "none"
DEFAULT_PIT_SOURCE_MANIFEST_DIR = (
    PROJECT_ROOT / "reports" / "data_governance" / "pit_source_manifest"
)
LATEST_POINTER_NAME = "latest_pit_source_manifest.json"

PIT_SOURCE_QUALITY_GRADES = frozenset(
    {
        "STRONG_PIT",
        "APPROX_PIT",
        "NON_PIT",
        "UNKNOWN",
    }
)
REVISION_RISK_LEVELS = frozenset({"HIGH", "MEDIUM", "LOW", "UNKNOWN"})
PIT_SOURCE_MANIFEST_STATUSES = frozenset({"PASS", "PASS_WITH_WARNINGS", "FAIL"})

PIT_SOURCE_MANIFEST_REQUIRED_RECORD_FIELDS = (
    "source_id",
    "source_name",
    "retrieval_time",
    "effective_date",
    "revision_risk",
    "pit_quality_grade",
    "cache_path",
    "checksum",
    "refresh_policy",
    "validation_policy",
)

DOWNLOAD_MANIFEST_COLUMNS = (
    "downloaded_at",
    "source_id",
    "provider",
    "endpoint",
    "request_parameters",
    "output_path",
    "row_count",
    "checksum_sha256",
)

PIT_EVIDENCE_MARKERS = frozenset(
    {
        "accepted_datetime",
        "accepted_time",
        "available_for_signal_date",
        "available_time",
        "captured_at",
        "filed_date",
        "filing",
        "forward_only",
        "ingested_at",
        "pit",
        "point-in-time",
        "point_in_time",
        "snapshot_time",
    }
)
STRICT_SOURCE_ID_MARKERS = frozenset(
    {
        "accession",
        "edgar",
        "filing",
        "pit",
        "sec",
        "snapshot",
    }
)
HIGH_REVISION_DOMAINS = frozenset(
    {
        "fundamentals",
        "macro_rates",
        "news_events",
        "risk_events",
        "valuation",
    }
)
MEDIUM_REVISION_DOMAINS = frozenset(
    {
        "market_prices",
        "portfolio_positions",
        "trade_records",
        "trade_thesis",
    }
)
DATE_COLUMNS = (
    "available_time",
    "accepted_datetime",
    "filed_date",
    "captured_at",
    "as_of",
    "as_of_date",
    "date",
    "signal_date",
    "snapshot_time",
    "downloaded_at",
)

# TRADING-355: this baseline classifies source-level PIT readiness only. The
# policy is intentionally conservative and must not be read as score/backtest
# promotion evidence.
PIT_SOURCE_MANIFEST_POLICY = {
    "policy_version": PIT_SOURCE_MANIFEST_POLICY_VERSION,
    "owner": "system",
    "status": "pilot_baseline",
    "rationale": (
        "Expose source-level PIT quality, revision risk, cache/checksum and "
        "validation policy before future research data is interpreted."
    ),
    "intended_effect": (
        "Make STRONG_PIT, APPROX_PIT, NON_PIT and UNKNOWN classifications "
        "auditable without changing scoring, backtests, shadow state or broker "
        "behavior."
    ),
    "validation_evidence": (
        "Focused tests cover manifest generation, grade validation, CLI "
        "report/validate and Reader Brief summary."
    ),
    "review_condition": (
        "Review before any source-level grade is used as a promotion gate or "
        "as strict PIT backtest evidence."
    ),
}

PIT_SOURCE_MANIFEST_SAFETY = {
    "read_only": True,
    "data_refresh_allowed": False,
    "score_or_backtest_allowed": False,
    "broker_action_allowed": False,
    "trading_action_allowed": False,
    "production_effect": PRODUCTION_EFFECT,
    "boundary": (
        "Source-level governance report only; does not refresh data, change "
        "score/backtest inputs, mutate shadow or production state, or create "
        "broker/order actions."
    ),
}


class PitSourceManifestIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class PitSourceManifestIssue:
    severity: PitSourceManifestIssueSeverity
    code: str
    message: str
    source_id: str | None = None
    field: str | None = None


@dataclass(frozen=True)
class PitSourceManifestValidationReport:
    as_of: date
    generated_at: datetime
    manifest_id: str
    manifest_path: Path
    source_count: int
    grade_counts: Mapping[str, int]
    issues: tuple[PitSourceManifestIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == PitSourceManifestIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for issue in self.issues if issue.severity == PitSourceManifestIssueSeverity.WARNING
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


def build_pit_source_manifest_payload(
    *,
    config: DataSourcesConfig,
    as_of: date,
    download_manifest_path: Path,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    generated_at = datetime.now(tz=UTC)
    manifest_rows, download_manifest_issues = _read_download_manifest(download_manifest_path)
    records = [
        _source_record(
            source=source,
            as_of=as_of,
            manifest_rows=manifest_rows,
            project_root=project_root,
        )
        for source in config.sources
    ]
    grade_counts = _grade_counts(records)
    non_strong_source_ids = tuple(
        record["source_id"]
        for record in records
        if record["pit_quality_grade"] != "STRONG_PIT"
    )
    unknown_source_ids = tuple(
        record["source_id"] for record in records if record["pit_quality_grade"] == "UNKNOWN"
    )
    manifest_id = _manifest_id(as_of=as_of, generated_at=generated_at, records=records)
    payload: dict[str, Any] = {
        "schema_version": PIT_SOURCE_MANIFEST_SCHEMA_VERSION,
        "report_type": PIT_SOURCE_MANIFEST_REPORT_TYPE,
        "manifest_id": manifest_id,
        "as_of": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "status": "UNKNOWN",
        "production_effect": PRODUCTION_EFFECT,
        "policy": PIT_SOURCE_MANIFEST_POLICY,
        "safety_boundary": PIT_SOURCE_MANIFEST_SAFETY,
        "download_manifest": {
            "path": str(download_manifest_path),
            "exists": download_manifest_path.exists(),
            "row_count": len(manifest_rows),
            "required_columns": list(DOWNLOAD_MANIFEST_COLUMNS),
        },
        "summary": {
            "source_count": len(records),
            "strong_pit_count": grade_counts["STRONG_PIT"],
            "approx_pit_count": grade_counts["APPROX_PIT"],
            "non_pit_count": grade_counts["NON_PIT"],
            "unknown_count": grade_counts["UNKNOWN"],
            "non_strong_source_count": len(non_strong_source_ids),
            "non_strong_source_ids": list(non_strong_source_ids),
            "unknown_source_ids": list(unknown_source_ids),
        },
        "grade_counts": grade_counts,
        "records": records,
        "build_issues": [_issue_dict(issue) for issue in download_manifest_issues],
        "methodology": {
            "pit_quality_grades": sorted(PIT_SOURCE_QUALITY_GRADES),
            "revision_risk_levels": sorted(REVISION_RISK_LEVELS),
            "required_record_fields": list(PIT_SOURCE_MANIFEST_REQUIRED_RECORD_FIELDS),
            "pit_quality_policy": (
                "STRONG_PIT requires explicit source-level visibility markers "
                "such as available_time, filed_date, accepted_datetime or "
                "forward-only PIT archive evidence. APPROX_PIT indicates "
                "auditable active sources without strict PIT evidence. NON_PIT "
                "marks public convenience/current views or known non-PIT "
                "sources. UNKNOWN is used for planned or unclassifiable sources."
            ),
            "revision_risk_policy": (
                "Fundamental, valuation, macro and event domains are HIGH "
                "revision risk; market, trade and portfolio operational inputs "
                "are MEDIUM unless inactive/planned status makes risk UNKNOWN."
            ),
        },
    }
    return _with_validation_summary(payload, manifest_path=Path(""))


def write_pit_source_manifest_artifact(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_PIT_SOURCE_MANIFEST_DIR,
) -> dict[str, Path]:
    manifest_id = _text(payload.get("manifest_id"), "")
    if not manifest_id:
        raise ValueError("pit source manifest payload missing manifest_id")
    artifact_dir = output_dir / manifest_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = artifact_dir / "pit_source_manifest.json"
    report_path = artifact_dir / "pit_source_manifest.md"
    validation_json_path = artifact_dir / "pit_source_manifest_validation.json"
    validation_md_path = artifact_dir / "pit_source_manifest_validation.md"
    reader_brief_path = artifact_dir / "reader_brief_section.md"

    payload_with_paths = dict(payload)
    payload_with_paths["artifact_paths"] = {
        "artifact_dir": str(artifact_dir),
        "manifest_json": str(manifest_path),
        "manifest_markdown": str(report_path),
        "validation_json": str(validation_json_path),
        "validation_markdown": str(validation_md_path),
        "reader_brief_section": str(reader_brief_path),
    }
    payload_with_paths = _with_validation_summary(
        payload_with_paths,
        manifest_path=manifest_path,
    )
    validation = validate_pit_source_manifest_payload(
        payload_with_paths,
        manifest_path=manifest_path,
    )
    payload_with_paths = _with_validation_summary(
        payload_with_paths,
        manifest_path=manifest_path,
        validation=validation,
    )

    _write_json(manifest_path, payload_with_paths)
    _write_text(report_path, render_pit_source_manifest_markdown(payload_with_paths))
    _write_json(validation_json_path, validation_report_to_payload(validation))
    _write_text(validation_md_path, render_pit_source_manifest_validation_markdown(validation))
    _write_text(reader_brief_path, render_pit_source_manifest_reader_brief(payload_with_paths))
    _write_latest_pointer(
        output_dir=output_dir,
        payload=payload_with_paths,
        manifest_path=manifest_path,
    )
    return {
        "artifact_dir": artifact_dir,
        "manifest_json": manifest_path,
        "manifest_markdown": report_path,
        "validation_json": validation_json_path,
        "validation_markdown": validation_md_path,
        "reader_brief_section": reader_brief_path,
    }


def build_and_write_pit_source_manifest(
    *,
    config: DataSourcesConfig,
    as_of: date,
    download_manifest_path: Path,
    output_dir: Path = DEFAULT_PIT_SOURCE_MANIFEST_DIR,
    project_root: Path = PROJECT_ROOT,
) -> tuple[dict[str, Any], dict[str, Path]]:
    payload = build_pit_source_manifest_payload(
        config=config,
        as_of=as_of,
        download_manifest_path=download_manifest_path,
        project_root=project_root,
    )
    paths = write_pit_source_manifest_artifact(payload, output_dir=output_dir)
    return load_pit_source_manifest_payload(paths["manifest_json"]), paths


def validate_pit_source_manifest_payload(
    payload: Mapping[str, Any],
    *,
    manifest_path: Path,
) -> PitSourceManifestValidationReport:
    issues = [_issue_from_mapping(item) for item in _records(payload.get("build_issues"))]
    as_of = _parse_date(_text(payload.get("as_of"))) or date.today()
    manifest_id = _text(payload.get("manifest_id"), "UNKNOWN")
    records = _records(payload.get("records"))

    _check_top_level_contract(payload, issues)
    _check_safety_boundary(payload, issues)
    _check_records(records, issues)
    grade_counts = _grade_counts(records)
    return PitSourceManifestValidationReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        manifest_id=manifest_id,
        manifest_path=manifest_path,
        source_count=len(records),
        grade_counts=grade_counts,
        issues=tuple(issues),
    )


def validate_pit_source_manifest_artifact(
    *,
    manifest_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PIT_SOURCE_MANIFEST_DIR,
) -> tuple[PitSourceManifestValidationReport, Path]:
    manifest_path = resolve_pit_source_manifest_path(
        manifest_id=manifest_id,
        latest=latest,
        output_dir=output_dir,
    )
    payload = load_pit_source_manifest_payload(manifest_path)
    validation = validate_pit_source_manifest_payload(payload, manifest_path=manifest_path)
    artifact_dir = manifest_path.parent
    _write_json(
        artifact_dir / "pit_source_manifest_validation.json",
        validation_report_to_payload(validation),
    )
    _write_text(
        artifact_dir / "pit_source_manifest_validation.md",
        render_pit_source_manifest_validation_markdown(validation),
    )
    updated = _with_validation_summary(payload, manifest_path=manifest_path, validation=validation)
    _write_json(manifest_path, updated)
    return validation, manifest_path


def resolve_pit_source_manifest_path(
    *,
    manifest_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PIT_SOURCE_MANIFEST_DIR,
) -> Path:
    if manifest_id:
        candidate = output_dir / manifest_id / "pit_source_manifest.json"
        if not candidate.exists():
            raise FileNotFoundError(f"PIT source manifest not found: {candidate}")
        return candidate
    if latest:
        latest_path = _latest_manifest_from_pointer(output_dir)
        if latest_path is not None:
            return latest_path
    candidates = sorted(
        output_dir.glob("*/pit_source_manifest.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No PIT source manifest artifacts found under {output_dir}")
    return candidates[0]


def load_pit_source_manifest_payload(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"PIT source manifest must be a JSON object: {path}")
    return raw


def validation_report_to_payload(report: PitSourceManifestValidationReport) -> dict[str, Any]:
    return {
        "schema_version": PIT_SOURCE_MANIFEST_SCHEMA_VERSION,
        "report_type": PIT_SOURCE_MANIFEST_VALIDATION_REPORT_TYPE,
        "manifest_id": report.manifest_id,
        "manifest_path": str(report.manifest_path),
        "as_of": report.as_of.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "status": report.status,
        "source_count": report.source_count,
        "grade_counts": dict(report.grade_counts),
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "production_effect": PRODUCTION_EFFECT,
        "issues": [_issue_dict(issue) for issue in report.issues],
    }


def render_pit_source_manifest_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    paths = _mapping(payload.get("artifact_paths"))
    records = sorted(
        _records(payload.get("records")),
        key=lambda item: _text(item.get("source_id")),
    )
    lines = [
        "# Point-in-Time Source Manifest",
        "",
        f"- 状态：{_text(payload.get('status'), 'UNKNOWN')}",
        f"- Manifest ID：`{_text(payload.get('manifest_id'), 'UNKNOWN')}`",
        f"- 评估日期：{_text(payload.get('as_of'), 'UNKNOWN')}",
        f"- 生成时间：{_text(payload.get('generated_at'), 'UNKNOWN')}",
        f"- Source count：{_text(summary.get('source_count'), '0')}",
        f"- STRONG_PIT：{_text(summary.get('strong_pit_count'), '0')}",
        f"- APPROX_PIT：{_text(summary.get('approx_pit_count'), '0')}",
        f"- NON_PIT：{_text(summary.get('non_pit_count'), '0')}",
        f"- UNKNOWN：{_text(summary.get('unknown_count'), '0')}",
        f"- Non-strong source count：{_text(summary.get('non_strong_source_count'), '0')}",
        f"- Production effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- Validation report：`{_text(paths.get('validation_markdown'), 'UNKNOWN')}`",
        "",
        "## 治理边界",
        "",
        _text(_mapping(payload.get("safety_boundary")).get("boundary")),
        "",
        "## 方法说明",
        "",
        f"- Policy version：`{_text(_mapping(payload.get('policy')).get('policy_version'))}`",
        "- 本报告只建立 source-level contract，不修复全部 PIT 数据问题。",
        "- `APPROX_PIT`、`NON_PIT` 或 `UNKNOWN` 不得自动支持 production backtest 结论。",
        "- 缺少 retrieval time、cache checksum 或 validation policy 的来源保留为 warning 调查项。",
        "",
        "## 来源分级",
        "",
        "| Source | Provider | Domains | Status | Retrieval time | Effective date | "
        "Revision risk | PIT grade | Cache checksum |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for record in records:
        lines.append(
            "| "
            f"{_escape_table(_text(record.get('source_id')))} | "
            f"{_escape_table(_text(record.get('source_name')))} | "
            f"{_escape_table(', '.join(_texts(record.get('domains'))))} | "
            f"{_escape_table(_text(record.get('status')))} | "
            f"{_escape_table(_text(record.get('retrieval_time')))} | "
            f"{_escape_table(_text(record.get('effective_date')))} | "
            f"{_escape_table(_text(record.get('revision_risk')))} | "
            f"{_escape_table(_text(record.get('pit_quality_grade')))} | "
            f"{_checksum_prefix(_text(record.get('checksum')))} |"
        )

    issues = _records(payload.get("validation_issues"))
    lines.extend(["", "## 校验问题", ""])
    if not issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| Severity | Code | Source | Field | Message |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in issues:
            lines.append(
                "| "
                f"{_text(issue.get('severity'))} | "
                f"{_escape_table(_text(issue.get('code')))} | "
                f"{_escape_table(_text(issue.get('source_id')))} | "
                f"{_escape_table(_text(issue.get('field')))} | "
                f"{_escape_table(_text(issue.get('message')))} |"
            )
    return "\n".join(lines) + "\n"


def render_pit_source_manifest_validation_markdown(
    report: PitSourceManifestValidationReport,
) -> str:
    lines = [
        "# PIT Source Manifest Validation",
        "",
        f"- 状态：{report.status}",
        f"- Manifest ID：`{report.manifest_id}`",
        f"- Manifest path：`{report.manifest_path}`",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- Source count：{report.source_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- Production effect：{PRODUCTION_EFFECT}",
        "",
        "## Grade Counts",
        "",
        "| Grade | Count |",
        "|---|---:|",
    ]
    for grade in sorted(PIT_SOURCE_QUALITY_GRADES):
        lines.append(f"| {grade} | {report.grade_counts.get(grade, 0)} |")
    lines.extend(["", "## Issues", ""])
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| Severity | Code | Source | Field | Message |",
                "|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity.value} | "
                f"{_escape_table(issue.code)} | "
                f"{_escape_table(issue.source_id or '')} | "
                f"{_escape_table(issue.field or '')} | "
                f"{_escape_table(issue.message)} |"
            )
    lines.extend(
        [
            "",
            "## Safety",
            "",
            "- 该校验只读 existing manifest artifact；不刷新数据、不运行评分或回测、不写生产状态。",
            f"- production_effect=`{PRODUCTION_EFFECT}`。",
        ]
    )
    return "\n".join(lines) + "\n"


def render_pit_source_manifest_reader_brief(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    non_strong = _texts(summary.get("non_strong_source_ids"))
    return (
        "## PIT Source Manifest\n\n"
        f"- status: `{_text(payload.get('status'), 'UNKNOWN')}`\n"
        f"- source_count: `{_text(summary.get('source_count'), '0')}`\n"
        f"- STRONG_PIT: `{_text(summary.get('strong_pit_count'), '0')}`\n"
        f"- APPROX_PIT: `{_text(summary.get('approx_pit_count'), '0')}`\n"
        f"- NON_PIT: `{_text(summary.get('non_pit_count'), '0')}`\n"
        f"- UNKNOWN: `{_text(summary.get('unknown_count'), '0')}`\n"
        f"- non_strong_source_ids: `{', '.join(non_strong) or 'none'}`\n"
        f"- production_effect: `{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}`\n"
    )


def _source_record(
    *,
    source: DataSourceConfig,
    as_of: date,
    manifest_rows: Sequence[Mapping[str, Any]],
    project_root: Path,
) -> dict[str, Any]:
    latest_manifest = _latest_manifest_record(source.source_id, manifest_rows)
    cache_details = [
        _cache_path_detail(configured_path=path, project_root=project_root)
        for path in source.cache_paths
    ]
    primary_cache = _primary_cache_detail(cache_details)
    retrieval_time, retrieval_time_source = _retrieval_time(source, latest_manifest, cache_details)
    effective_date, effective_date_source = _effective_date(
        latest_manifest=latest_manifest,
        cache_details=cache_details,
        as_of=as_of,
    )
    revision_risk, revision_reason = _revision_risk(source)
    pit_grade, pit_reason = _pit_quality_grade(source)
    checksum = _record_checksum(latest_manifest=latest_manifest, cache_details=cache_details)
    return {
        "source_id": source.source_id,
        "source_name": source.provider,
        "provider": source.provider,
        "source_type": source.source_type,
        "status": source.status,
        "domains": list(source.domains),
        "endpoint": source.endpoint,
        "adapter": source.adapter,
        "retrieval_time": retrieval_time,
        "retrieval_time_source": retrieval_time_source,
        "effective_date": effective_date,
        "effective_date_source": effective_date_source,
        "revision_risk": revision_risk,
        "revision_risk_reason": revision_reason,
        "pit_quality_grade": pit_grade,
        "pit_quality_reason": pit_reason,
        "cache_path": _text(primary_cache.get("configured_path"), "UNKNOWN"),
        "cache_path_resolved": _text(primary_cache.get("resolved_path"), "UNKNOWN"),
        "checksum": checksum,
        "checksum_algorithm": "sha256",
        "refresh_policy": _refresh_policy(source),
        "validation_policy": _validation_policy(source),
        "cache_paths": cache_details,
        "latest_download_manifest": latest_manifest or {},
        "primary_for": list(source.primary_for),
        "audit_fields": list(source.audit_fields),
        "validation_checks": list(source.validation_checks),
        "limitations": list(source.limitations),
        "owner_notes": source.owner_notes,
        "llm_permission": source.llm_permission.model_dump(mode="json"),
    }


def _read_download_manifest(
    path: Path,
) -> tuple[list[dict[str, Any]], list[PitSourceManifestIssue]]:
    issues: list[PitSourceManifestIssue] = []
    if not path.exists():
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.WARNING,
                code="download_manifest_missing",
                message=f"download_manifest 不存在：{path}",
            )
        )
        return [], issues
    try:
        frame = pd.read_csv(path)
    except Exception as exc:
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.ERROR,
                code="download_manifest_unreadable",
                message=f"download_manifest 无法读取：{exc}",
            )
        )
        return [], issues
    missing = [column for column in DOWNLOAD_MANIFEST_COLUMNS if column not in frame.columns]
    if missing:
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.WARNING,
                code="download_manifest_missing_columns",
                message=f"download_manifest 缺少字段：{', '.join(missing)}",
            )
        )
    return frame.to_dict(orient="records"), issues


def _latest_manifest_record(
    source_id: str,
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any] | None:
    source_rows = [row for row in rows if _text(row.get("source_id")) == source_id]
    if not source_rows:
        return None

    def sort_key(row: Mapping[str, Any]) -> tuple[datetime, str]:
        parsed = _parse_datetime(_text(row.get("downloaded_at")))
        return (parsed or datetime.min.replace(tzinfo=UTC), _text(row.get("output_path")))

    latest = max(source_rows, key=sort_key)
    return {
        "downloaded_at": _text(latest.get("downloaded_at"), "UNKNOWN"),
        "source_id": _text(latest.get("source_id"), source_id),
        "provider": _text(latest.get("provider")),
        "endpoint": _text(latest.get("endpoint")),
        "request_parameters": _text(latest.get("request_parameters")),
        "output_path": _text(latest.get("output_path")),
        "row_count": _optional_int(latest.get("row_count")),
        "checksum_sha256": _text(latest.get("checksum_sha256")),
    }


def _cache_path_detail(*, configured_path: str, project_root: Path) -> dict[str, Any]:
    resolved_pattern = _resolve_path(_template_to_glob(configured_path), project_root)
    matched_paths = _matched_files(resolved_pattern)
    exists = bool(matched_paths)
    checksum = _aggregate_files_sha256(matched_paths) if matched_paths else "UNKNOWN"
    newest_mtime = _latest_mtime(matched_paths)
    effective_date = _latest_date_from_paths_or_csvs(matched_paths)
    row_count = _combined_row_count(matched_paths)
    return {
        "configured_path": configured_path,
        "resolved_path": str(resolved_pattern),
        "exists": exists,
        "matched_file_count": len(matched_paths),
        "checksum_sha256": checksum,
        "row_count": row_count,
        "latest_modified_at": "" if newest_mtime is None else newest_mtime.isoformat(),
        "latest_effective_date": "" if effective_date is None else effective_date.isoformat(),
    }


def _matched_files(path_or_pattern: Path) -> list[Path]:
    raw = str(path_or_pattern)
    if _has_glob(raw):
        paths = [Path(item) for item in glob(raw)]
    else:
        paths = [path_or_pattern]
    files: list[Path] = []
    for path in paths:
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(child for child in path.rglob("*") if child.is_file())
    return sorted(set(files), key=lambda item: str(item))


def _primary_cache_detail(details: Sequence[Mapping[str, Any]]) -> Mapping[str, Any]:
    for detail in details:
        if detail.get("exists") is True:
            return detail
    if details:
        return details[0]
    return {"configured_path": "UNKNOWN", "resolved_path": "UNKNOWN"}


def _retrieval_time(
    source: DataSourceConfig,
    latest_manifest: Mapping[str, Any] | None,
    cache_details: Sequence[Mapping[str, Any]],
) -> tuple[str, str]:
    if latest_manifest:
        downloaded_at = _text(latest_manifest.get("downloaded_at"))
        if downloaded_at:
            return downloaded_at, "download_manifest.downloaded_at"
    if source.source_type == "manual_input":
        latest_modified = _latest_detail_mtime(cache_details)
        if latest_modified:
            return latest_modified, "manual_cache_latest_modified_at"
    return "UNKNOWN", "missing_download_manifest"


def _effective_date(
    *,
    latest_manifest: Mapping[str, Any] | None,
    cache_details: Sequence[Mapping[str, Any]],
    as_of: date,
) -> tuple[str, str]:
    latest_cache_date = _latest_detail_effective_date(cache_details)
    if latest_cache_date:
        return latest_cache_date, "cache_content_or_path_date"
    if latest_manifest:
        output_date = _latest_date_from_text(_text(latest_manifest.get("output_path")))
        if output_date is not None:
            return output_date.isoformat(), "download_manifest.output_path_date"
    return as_of.isoformat(), "as_of_fallback_no_cache_effective_date"


def _revision_risk(source: DataSourceConfig) -> tuple[str, str]:
    if source.status != "active":
        return "UNKNOWN", "inactive_or_planned_source"
    domains = set(source.domains)
    if domains.intersection(HIGH_REVISION_DOMAINS):
        return "HIGH", "fundamental_valuation_macro_or_event_domain"
    if domains.intersection(MEDIUM_REVISION_DOMAINS):
        return "MEDIUM", "market_trade_or_portfolio_operational_domain"
    return "UNKNOWN", "unclassified_domain"


def _pit_quality_grade(source: DataSourceConfig) -> tuple[str, str]:
    if source.status == "planned":
        return "UNKNOWN", "planned_source_not_implemented"
    if source.source_type == "public_convenience":
        return "NON_PIT", "public_convenience_source_not_strict_pit"
    searchable = _searchable_source_text(source)
    has_visibility_marker = any(marker in searchable for marker in PIT_EVIDENCE_MARKERS)
    has_strict_source_marker = any(marker in searchable for marker in STRICT_SOURCE_ID_MARKERS)
    if has_visibility_marker and has_strict_source_marker:
        return "STRONG_PIT", "explicit_visibility_marker_and_pit_source_marker"
    if source.status != "active":
        return "UNKNOWN", "inactive_source_not_currently_qualified"
    if source.source_type in {"primary_source", "paid_vendor", "manual_input"}:
        if source.audit_fields and source.validation_checks:
            return "APPROX_PIT", "auditable_active_source_without_strict_visibility_marker"
    return "UNKNOWN", "insufficient_audit_or_validation_policy"


def _searchable_source_text(source: DataSourceConfig) -> str:
    values: list[str] = [
        source.source_id,
        source.provider,
        source.source_type,
        source.status,
        source.endpoint,
        source.adapter,
        " ".join(source.domains),
        " ".join(source.primary_for),
        " ".join(source.audit_fields),
        " ".join(source.validation_checks),
        " ".join(source.limitations),
        source.owner_notes,
    ]
    return " ".join(values).lower()


def _record_checksum(
    *,
    latest_manifest: Mapping[str, Any] | None,
    cache_details: Sequence[Mapping[str, Any]],
) -> str:
    if latest_manifest:
        checksum = _text(latest_manifest.get("checksum_sha256"))
        if checksum:
            return checksum
    detail_checksums = [
        _text(detail.get("checksum_sha256"))
        for detail in cache_details
        if _text(detail.get("checksum_sha256"))
        and _text(detail.get("checksum_sha256")) != "UNKNOWN"
    ]
    if not detail_checksums:
        return "UNKNOWN"
    digest = sha256()
    for value in sorted(detail_checksums):
        digest.update(value.encode("utf-8"))
    return digest.hexdigest()


def _refresh_policy(source: DataSourceConfig) -> str:
    return (
        f"cadence={source.cadence}; status={source.status}; "
        f"requires_credentials={str(source.requires_credentials).lower()}"
    )


def _validation_policy(source: DataSourceConfig) -> str:
    if not source.validation_checks:
        return "UNKNOWN"
    return "; ".join(source.validation_checks)


def _check_top_level_contract(
    payload: Mapping[str, Any],
    issues: list[PitSourceManifestIssue],
) -> None:
    expected = {
        "schema_version",
        "report_type",
        "manifest_id",
        "as_of",
        "generated_at",
        "production_effect",
        "policy",
        "safety_boundary",
        "records",
    }
    for field_name in sorted(expected):
        if field_name not in payload:
            issues.append(
                PitSourceManifestIssue(
                    severity=PitSourceManifestIssueSeverity.ERROR,
                    code="manifest_missing_top_level_field",
                    field=field_name,
                    message=f"Manifest 缺少顶层字段：{field_name}",
                )
            )
    if payload.get("schema_version") != PIT_SOURCE_MANIFEST_SCHEMA_VERSION:
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.ERROR,
                code="manifest_schema_version_invalid",
                field="schema_version",
                message=(
                    "Manifest schema_version 必须为 "
                    f"{PIT_SOURCE_MANIFEST_SCHEMA_VERSION}。"
                ),
            )
        )
    if _text(payload.get("report_type")) != PIT_SOURCE_MANIFEST_REPORT_TYPE:
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.ERROR,
                code="manifest_report_type_invalid",
                field="report_type",
                message=f"Manifest report_type 必须为 {PIT_SOURCE_MANIFEST_REPORT_TYPE}。",
            )
        )
    if _text(payload.get("production_effect")) != PRODUCTION_EFFECT:
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.ERROR,
                code="manifest_production_effect_invalid",
                field="production_effect",
                message="PIT source manifest 必须固定 production_effect=none。",
            )
        )


def _check_safety_boundary(
    payload: Mapping[str, Any],
    issues: list[PitSourceManifestIssue],
) -> None:
    safety = _mapping(payload.get("safety_boundary"))
    expected_false = (
        "data_refresh_allowed",
        "score_or_backtest_allowed",
        "broker_action_allowed",
        "trading_action_allowed",
    )
    if safety.get("read_only") is not True:
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.ERROR,
                code="safety_boundary_not_read_only",
                field="safety_boundary.read_only",
                message="Manifest safety boundary 必须声明 read_only=true。",
            )
        )
    for field_name in expected_false:
        if safety.get(field_name) is not False:
            issues.append(
                PitSourceManifestIssue(
                    severity=PitSourceManifestIssueSeverity.ERROR,
                    code="safety_boundary_forbidden_action_allowed",
                    field=f"safety_boundary.{field_name}",
                    message=f"Manifest safety boundary 必须声明 {field_name}=false。",
                )
            )
    if _text(safety.get("production_effect")) != PRODUCTION_EFFECT:
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.ERROR,
                code="safety_boundary_production_effect_invalid",
                field="safety_boundary.production_effect",
                message="Safety boundary 必须固定 production_effect=none。",
            )
        )


def _check_records(
    records: Sequence[Mapping[str, Any]],
    issues: list[PitSourceManifestIssue],
) -> None:
    if not records:
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.ERROR,
                code="manifest_records_empty",
                message="PIT source manifest records 不能为空。",
            )
        )
        return
    seen: set[str] = set()
    for index, record in enumerate(records):
        source_id = _text(record.get("source_id"), f"row_{index + 1}")
        if source_id in seen:
            issues.append(
                PitSourceManifestIssue(
                    severity=PitSourceManifestIssueSeverity.ERROR,
                    code="duplicate_source_id",
                    source_id=source_id,
                    field="source_id",
                    message="PIT source manifest source_id 重复。",
                )
            )
        seen.add(source_id)
        for field_name in PIT_SOURCE_MANIFEST_REQUIRED_RECORD_FIELDS:
            if field_name not in record:
                issues.append(
                    PitSourceManifestIssue(
                        severity=PitSourceManifestIssueSeverity.ERROR,
                        code="source_record_missing_required_field",
                        source_id=source_id,
                        field=field_name,
                        message=f"Source record 缺少必填字段：{field_name}",
                    )
                )
                continue
            value = _text(record.get(field_name))
            if not value:
                issues.append(
                    PitSourceManifestIssue(
                        severity=PitSourceManifestIssueSeverity.WARNING,
                        code="source_record_required_field_blank",
                        source_id=source_id,
                        field=field_name,
                        message=f"Source record 必填字段为空：{field_name}",
                    )
                )
        grade = _text(record.get("pit_quality_grade"))
        if grade not in PIT_SOURCE_QUALITY_GRADES:
            issues.append(
                PitSourceManifestIssue(
                    severity=PitSourceManifestIssueSeverity.ERROR,
                    code="source_record_invalid_pit_quality_grade",
                    source_id=source_id,
                    field="pit_quality_grade",
                    message=(
                        "pit_quality_grade 必须为 "
                        f"{', '.join(sorted(PIT_SOURCE_QUALITY_GRADES))}。"
                    ),
                )
            )
        revision_risk = _text(record.get("revision_risk"))
        if revision_risk not in REVISION_RISK_LEVELS:
            issues.append(
                PitSourceManifestIssue(
                    severity=PitSourceManifestIssueSeverity.ERROR,
                    code="source_record_invalid_revision_risk",
                    source_id=source_id,
                    field="revision_risk",
                    message=(
                        "revision_risk 必须为 "
                        f"{', '.join(sorted(REVISION_RISK_LEVELS))}。"
                    ),
                )
            )
        _check_record_warnings(record, source_id, issues)


def _check_record_warnings(
    record: Mapping[str, Any],
    source_id: str,
    issues: list[PitSourceManifestIssue],
) -> None:
    if _text(record.get("retrieval_time")) == "UNKNOWN":
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.WARNING,
                code="source_record_retrieval_time_unknown",
                source_id=source_id,
                field="retrieval_time",
                message="Source 缺少可审计 retrieval time；需要补 download manifest 或采集记录。",
            )
        )
    if _text(record.get("checksum")) == "UNKNOWN":
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.WARNING,
                code="source_record_checksum_unknown",
                source_id=source_id,
                field="checksum",
                message="Source 缺少 cache checksum；后续研究解释需人工复核。",
            )
        )
    if _text(record.get("validation_policy")) == "UNKNOWN":
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.WARNING,
                code="source_record_validation_policy_unknown",
                source_id=source_id,
                field="validation_policy",
                message="Source 未声明 validation policy。",
            )
        )
    if _text(record.get("effective_date_source")) == "as_of_fallback_no_cache_effective_date":
        issues.append(
            PitSourceManifestIssue(
                severity=PitSourceManifestIssueSeverity.WARNING,
                code="source_record_effective_date_inferred_from_as_of",
                source_id=source_id,
                field="effective_date",
                message=(
                    "Source effective_date 没有从 cache 或 manifest 解析，"
                    "只能标记为本次 as_of。"
                ),
            )
        )
    for detail in _records(record.get("cache_paths")):
        if detail.get("exists") is False:
            issues.append(
                PitSourceManifestIssue(
                    severity=PitSourceManifestIssueSeverity.WARNING,
                    code="source_record_cache_path_missing",
                    source_id=source_id,
                    field="cache_path",
                    message=(
                        "配置的 cache path 当前无匹配文件："
                        f"{_text(detail.get('configured_path'))}"
                    ),
                )
            )


def _with_validation_summary(
    payload: Mapping[str, Any],
    *,
    manifest_path: Path,
    validation: PitSourceManifestValidationReport | None = None,
) -> dict[str, Any]:
    output = dict(payload)
    report = validation or validate_pit_source_manifest_payload(output, manifest_path=manifest_path)
    output["status"] = report.status
    output["validation_status"] = report.status
    output["error_count"] = report.error_count
    output["warning_count"] = report.warning_count
    output["validation_issues"] = [_issue_dict(issue) for issue in report.issues]
    output["grade_counts"] = dict(report.grade_counts)
    return output


def _grade_counts(records: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {grade: 0 for grade in sorted(PIT_SOURCE_QUALITY_GRADES)}
    for record in records:
        grade = _text(record.get("pit_quality_grade"), "UNKNOWN")
        if grade in counts:
            counts[grade] += 1
        else:
            counts["UNKNOWN"] += 1
    return counts


def _manifest_id(
    *,
    as_of: date,
    generated_at: datetime,
    records: Sequence[Mapping[str, Any]],
) -> str:
    digest = sha256()
    digest.update(as_of.isoformat().encode("utf-8"))
    digest.update(generated_at.isoformat().encode("utf-8"))
    for record in sorted(records, key=lambda item: _text(item.get("source_id"))):
        digest.update(_text(record.get("source_id")).encode("utf-8"))
        digest.update(_text(record.get("pit_quality_grade")).encode("utf-8"))
        digest.update(_text(record.get("checksum")).encode("utf-8"))
    return f"pit_source_manifest_{as_of.isoformat()}_{digest.hexdigest()[:16]}"


def _write_latest_pointer(
    *,
    output_dir: Path,
    payload: Mapping[str, Any],
    manifest_path: Path,
) -> None:
    pointer = {
        "schema_version": PIT_SOURCE_MANIFEST_SCHEMA_VERSION,
        "manifest_id": _text(payload.get("manifest_id")),
        "manifest_path": str(manifest_path),
        "generated_at": _text(payload.get("generated_at")),
        "status": _text(payload.get("status")),
        "production_effect": PRODUCTION_EFFECT,
    }
    _write_json(output_dir / LATEST_POINTER_NAME, pointer)


def _latest_manifest_from_pointer(output_dir: Path) -> Path | None:
    pointer_path = output_dir / LATEST_POINTER_NAME
    if not pointer_path.exists():
        return None
    try:
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    manifest_path = Path(_text(_mapping(pointer).get("manifest_path")))
    if manifest_path.exists():
        return manifest_path
    return None


def _aggregate_files_sha256(paths: Sequence[Path]) -> str:
    digest = sha256()
    for path in sorted(paths, key=lambda item: str(item)):
        file_digest = sha256()
        try:
            with path.open("rb") as file:
                for chunk in iter(lambda: file.read(1024 * 1024), b""):
                    file_digest.update(chunk)
        except OSError:
            continue
        digest.update(str(path).encode("utf-8"))
        digest.update(file_digest.hexdigest().encode("ascii"))
    return digest.hexdigest()


def _latest_mtime(paths: Sequence[Path]) -> datetime | None:
    if not paths:
        return None
    latest = max(path.stat().st_mtime for path in paths if path.exists())
    return datetime.fromtimestamp(latest, tz=UTC)


def _latest_detail_mtime(details: Sequence[Mapping[str, Any]]) -> str:
    values = [_text(detail.get("latest_modified_at")) for detail in details]
    values = [value for value in values if value]
    return max(values) if values else ""


def _latest_detail_effective_date(details: Sequence[Mapping[str, Any]]) -> str:
    values = [_text(detail.get("latest_effective_date")) for detail in details]
    values = [value for value in values if value]
    return max(values) if values else ""


def _latest_date_from_paths_or_csvs(paths: Sequence[Path]) -> date | None:
    candidates: list[date] = []
    for path in paths:
        from_name = _latest_date_from_text(str(path))
        if from_name is not None:
            candidates.append(from_name)
        if path.suffix.lower() == ".csv":
            from_csv = _latest_date_from_csv(path)
            if from_csv is not None:
                candidates.append(from_csv)
    return max(candidates) if candidates else None


def _latest_date_from_csv(path: Path) -> date | None:
    try:
        columns = pd.read_csv(path, nrows=0).columns
    except Exception:
        return None
    selected = [column for column in DATE_COLUMNS if column in columns]
    if not selected:
        return None
    try:
        frame = pd.read_csv(path, usecols=selected)
    except Exception:
        return None
    dates: list[date] = []
    for column in selected:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            parsed = pd.to_datetime(frame[column], errors="coerce", utc=True)
        if parsed.notna().any():
            dates.append(parsed.max().date())
    return max(dates) if dates else None


def _combined_row_count(paths: Sequence[Path]) -> int | None:
    total = 0
    saw_csv = False
    for path in paths:
        if path.suffix.lower() != ".csv":
            continue
        try:
            count = len(pd.read_csv(path))
        except Exception:
            continue
        total += count
        saw_csv = True
    return total if saw_csv else None


def _latest_date_from_text(value: str) -> date | None:
    matches = re.findall(r"\d{4}-\d{2}-\d{2}", value)
    if not matches:
        compact = re.findall(r"(?<!\d)(\d{8})(?:T\d{6}Z?)?(?!\d)", value)
        matches = [f"{item[:4]}-{item[4:6]}-{item[6:]}" for item in compact]
    dates: list[date] = []
    for item in matches:
        parsed = _parse_date(item)
        if parsed is not None:
            dates.append(parsed)
    return max(dates) if dates else None


def _template_to_glob(path: str) -> str:
    return (
        path.replace("YYYY-MM-DD", "*")
        .replace("YYYY_Qn", "*")
        .replace("YYYY", "*")
        .replace("Qn", "*")
    )


def _resolve_path(path: str, project_root: Path) -> Path:
    candidate = Path(path)
    return candidate if candidate.is_absolute() else project_root / candidate


def _has_glob(value: str) -> bool:
    return any(char in value for char in "*?[]")


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return path


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _issue_dict(issue: PitSourceManifestIssue) -> dict[str, Any]:
    return {
        "severity": issue.severity.value,
        "code": issue.code,
        "message": issue.message,
        "source_id": issue.source_id or "",
        "field": issue.field or "",
    }


def _issue_from_mapping(raw: Mapping[str, Any]) -> PitSourceManifestIssue:
    severity = _text(raw.get("severity"), PitSourceManifestIssueSeverity.WARNING.value)
    if severity not in {item.value for item in PitSourceManifestIssueSeverity}:
        severity = PitSourceManifestIssueSeverity.WARNING.value
    return PitSourceManifestIssue(
        severity=PitSourceManifestIssueSeverity(severity),
        code=_text(raw.get("code"), "unknown_issue"),
        message=_text(raw.get("message")),
        source_id=_text(raw.get("source_id")) or None,
        field=_text(raw.get("field")) or None,
    )


def _parse_datetime(value: str) -> datetime | None:
    if not value or value == "UNKNOWN":
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed_ts = pd.to_datetime(value, utc=True, errors="coerce")
        except Exception:
            return None
        if pd.isna(parsed_ts):
            return None
        parsed = parsed_ts.to_pydatetime()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        parsed = _parse_datetime(value)
        return None if parsed is None else parsed.date()


def _optional_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _records(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _texts(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    text = str(value)
    return text if text else default


def _checksum_prefix(value: str) -> str:
    if not value or value == "UNKNOWN":
        return "UNKNOWN"
    return value[:12]


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
