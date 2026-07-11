from __future__ import annotations

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

DATA_SOURCE_FALLBACK_SCHEMA_VERSION = 1
DATA_SOURCE_FALLBACK_REPORT_TYPE = "data_source_fallback_policy"
DATA_SOURCE_FALLBACK_VALIDATION_REPORT_TYPE = "data_source_fallback_policy_validation"
DATA_SOURCE_FALLBACK_POLICY_VERSION = "data_source_fallback_policy_v1"
PRODUCTION_EFFECT = "none"

DEFAULT_DATA_SOURCE_FALLBACK_POLICY_PATH = (
    PROJECT_ROOT / "config" / "data_source_fallback_policy.yaml"
)
DEFAULT_DATA_SOURCE_FALLBACK_DIR = (
    PROJECT_ROOT / "reports" / "data_governance" / "data_source_fallback_policy"
)
LATEST_POINTER_NAME = "latest_data_source_fallback_policy.json"

FALLBACK_STATE_PRIMARY_OK = "PRIMARY_OK"
FALLBACK_STATE_FALLBACK_USED = "FALLBACK_USED"
FALLBACK_STATE_FALLBACK_UNAVAILABLE = "FALLBACK_UNAVAILABLE"
FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE = "BLOCKED_NO_VALID_SOURCE"
FALLBACK_STATES = frozenset(
    {
        FALLBACK_STATE_PRIMARY_OK,
        FALLBACK_STATE_FALLBACK_USED,
        FALLBACK_STATE_FALLBACK_UNAVAILABLE,
        FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
    }
)
DATA_SOURCE_FALLBACK_STATUSES = frozenset({"PASS", "PASS_WITH_WARNINGS", "FAIL"})

DATA_SOURCE_FALLBACK_SAFETY = {
    "read_only": True,
    "paper_shadow_research_only": True,
    "data_refresh_allowed": False,
    "cache_mutation_allowed": False,
    "score_or_backtest_allowed": False,
    "broker_action_allowed": False,
    "order_ticket_allowed": False,
    "production_state_mutation_allowed": False,
    "production_effect": PRODUCTION_EFFECT,
    "boundary": (
        "Data source fallback policy report only; reads policy/source catalog "
        "metadata and explicit fallback declarations, does not refresh data, "
        "mutate cache, run scoring/backtests, or create broker/order actions."
    ),
}


class DataSourceFallbackIssueSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class DataSourceFallbackIssue:
    severity: DataSourceFallbackIssueSeverity
    code: str
    message: str
    data_type: str | None = None
    source_id: str | None = None
    field: str | None = None


@dataclass(frozen=True)
class DataSourceFallbackValidationReport:
    as_of: date
    generated_at: datetime
    report_id: str
    report_path: Path
    source_group_count: int
    issues: tuple[DataSourceFallbackIssue, ...] = field(default_factory=tuple)

    @property
    def error_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == DataSourceFallbackIssueSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1
            for issue in self.issues
            if issue.severity == DataSourceFallbackIssueSeverity.WARNING
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


def load_data_source_fallback_policy(
    path: Path = DEFAULT_DATA_SOURCE_FALLBACK_POLICY_PATH,
) -> dict[str, Any]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Data source fallback policy must be a YAML object: {path}")
    return raw


def build_data_source_fallback_policy_payload(
    *,
    config: DataSourcesConfig,
    policy: Mapping[str, Any],
    as_of: date,
    unavailable_source_ids: Sequence[str] = (),
    fallback_used_source_ids: Sequence[str] = (),
    fallback_reasons: Mapping[str, str] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    source_map = {source.source_id: source for source in config.sources}
    unavailable = set(_texts(unavailable_source_ids))
    used = set(_texts(fallback_used_source_ids))
    reasons = {str(key): str(value) for key, value in (fallback_reasons or {}).items()}
    build_issues: list[DataSourceFallbackIssue] = []
    records = [
        _source_group_record(
            group=group,
            source_map=source_map,
            unavailable_source_ids=unavailable,
            fallback_used_source_ids=used,
            fallback_reasons=reasons,
            build_issues=build_issues,
        )
        for group in _records(policy.get("source_groups"))
    ]
    summary = _summary(records)
    report_id = _report_id(as_of=as_of, generated_at=generated, records=records)
    payload: dict[str, Any] = {
        "schema_version": DATA_SOURCE_FALLBACK_SCHEMA_VERSION,
        "report_type": DATA_SOURCE_FALLBACK_REPORT_TYPE,
        "report_id": report_id,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": summary["status"],
        "validation_status": summary["status"],
        "fallback_status": summary["fallback_status"],
        "production_effect": PRODUCTION_EFFECT,
        "policy": _policy_metadata(policy),
        "policy_path": str(DEFAULT_DATA_SOURCE_FALLBACK_POLICY_PATH),
        "safety_boundary": _safety_boundary(policy),
        "summary": summary,
        "records": records,
        "build_issues": [_issue_dict(issue) for issue in build_issues],
        "methodology": {
            "fallback_states": sorted(FALLBACK_STATES),
            "state_precedence": [
                FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
                FALLBACK_STATE_FALLBACK_UNAVAILABLE,
                FALLBACK_STATE_FALLBACK_USED,
                FALLBACK_STATE_PRIMARY_OK,
            ],
            "source_eligibility_policy": _mapping(policy.get("eligibility_policy")),
            "metadata_requirements": _mapping(policy.get("metadata_requirements")),
        },
    }
    return _with_validation_summary(payload, report_path=Path(""))


def build_and_write_data_source_fallback_policy(
    *,
    config: DataSourcesConfig,
    policy: Mapping[str, Any],
    as_of: date,
    output_dir: Path = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    unavailable_source_ids: Sequence[str] = (),
    fallback_used_source_ids: Sequence[str] = (),
    fallback_reasons: Mapping[str, str] | None = None,
) -> tuple[dict[str, Any], dict[str, Path]]:
    payload = build_data_source_fallback_policy_payload(
        config=config,
        policy=policy,
        as_of=as_of,
        unavailable_source_ids=unavailable_source_ids,
        fallback_used_source_ids=fallback_used_source_ids,
        fallback_reasons=fallback_reasons,
    )
    paths = write_data_source_fallback_policy_artifact(payload, output_dir=output_dir)
    return load_data_source_fallback_policy_payload(paths["report_json"]), paths


def write_data_source_fallback_policy_artifact(
    payload: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
) -> dict[str, Path]:
    report_id = _text(payload.get("report_id"))
    if not report_id:
        raise ValueError("data source fallback policy payload missing report_id")
    artifact_dir = output_dir / report_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    report_path = artifact_dir / "data_source_fallback_policy.json"
    markdown_path = artifact_dir / "data_source_fallback_policy.md"
    validation_json_path = artifact_dir / "data_source_fallback_policy_validation.json"
    validation_md_path = artifact_dir / "data_source_fallback_policy_validation.md"
    reader_brief_path = artifact_dir / "reader_brief_section.md"
    payload_with_paths = dict(payload)
    payload_with_paths["artifact_paths"] = {
        "artifact_dir": str(artifact_dir),
        "report_json": str(report_path),
        "report_markdown": str(markdown_path),
        "validation_json": str(validation_json_path),
        "validation_markdown": str(validation_md_path),
        "reader_brief_section": str(reader_brief_path),
    }
    validation = validate_data_source_fallback_policy_payload(
        payload_with_paths,
        report_path=report_path,
    )
    payload_with_paths = _with_validation_summary(
        payload_with_paths,
        report_path=report_path,
        validation=validation,
    )
    write_json_atomic_without_trailing_newline(report_path, payload_with_paths)
    write_text_atomic(
        markdown_path, render_data_source_fallback_policy_markdown(payload_with_paths)
    )
    write_json_atomic_without_trailing_newline(
        validation_json_path, validation_report_to_payload(validation)
    )
    write_text_atomic(
        validation_md_path,
        render_data_source_fallback_policy_validation_markdown(validation),
    )
    write_text_atomic(
        reader_brief_path,
        render_data_source_fallback_policy_reader_brief(payload_with_paths),
    )
    _write_latest_pointer(
        output_dir=output_dir,
        payload=payload_with_paths,
        report_path=report_path,
    )
    return {
        "artifact_dir": artifact_dir,
        "report_json": report_path,
        "report_markdown": markdown_path,
        "validation_json": validation_json_path,
        "validation_markdown": validation_md_path,
        "reader_brief_section": reader_brief_path,
    }


def validate_data_source_fallback_policy_payload(
    payload: Mapping[str, Any],
    *,
    report_path: Path,
) -> DataSourceFallbackValidationReport:
    issues = [_issue_from_mapping(item) for item in _records(payload.get("build_issues"))]
    as_of = _parse_date(_text(payload.get("as_of"))) or date.today()
    report_id = _text(payload.get("report_id"), "UNKNOWN")
    records = _records(payload.get("records"))
    _check_top_level_contract(payload, issues)
    _check_safety_boundary(payload, issues)
    _check_records(records, issues)
    return DataSourceFallbackValidationReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        report_id=report_id,
        report_path=report_path,
        source_group_count=len(records),
        issues=tuple(issues),
    )


def validate_data_source_fallback_policy_artifact(
    *,
    report_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
) -> tuple[DataSourceFallbackValidationReport, Path]:
    report_path = resolve_data_source_fallback_policy_path(
        report_id=report_id,
        latest=latest,
        output_dir=output_dir,
    )
    payload = load_data_source_fallback_policy_payload(report_path)
    validation = validate_data_source_fallback_policy_payload(payload, report_path=report_path)
    artifact_dir = report_path.parent
    write_json_atomic_without_trailing_newline(
        artifact_dir / "data_source_fallback_policy_validation.json",
        validation_report_to_payload(validation),
    )
    write_text_atomic(
        artifact_dir / "data_source_fallback_policy_validation.md",
        render_data_source_fallback_policy_validation_markdown(validation),
    )
    updated = _with_validation_summary(payload, report_path=report_path, validation=validation)
    write_json_atomic_without_trailing_newline(report_path, updated)
    return validation, report_path


def resolve_data_source_fallback_policy_path(
    *,
    report_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
) -> Path:
    if report_id:
        candidate = output_dir / report_id / "data_source_fallback_policy.json"
        if not candidate.exists():
            raise FileNotFoundError(f"Data source fallback policy report not found: {candidate}")
        return candidate
    if latest:
        pointer_path = output_dir / LATEST_POINTER_NAME
        if pointer_path.exists():
            pointer = _read_json(pointer_path)
            candidate = Path(_text(pointer.get("report_path")))
            if candidate.exists():
                return candidate
    candidates = sorted(
        output_dir.glob("*/data_source_fallback_policy.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(
            f"No data source fallback policy artifacts found under {output_dir}"
        )
    return candidates[0]


def load_data_source_fallback_policy_payload(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"Data source fallback policy must be a JSON object: {path}")
    return raw


def latest_data_source_fallback_policy_summary(
    *,
    report_path: Path | None = None,
    output_dir: Path = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
) -> dict[str, Any]:
    try:
        resolved = report_path or resolve_data_source_fallback_policy_path(
            latest=True,
            output_dir=output_dir,
        )
        payload = load_data_source_fallback_policy_payload(resolved)
    except (FileNotFoundError, OSError, ValueError, json.JSONDecodeError) as exc:
        return missing_data_source_fallback_policy_summary(str(exc))
    return data_source_fallback_policy_summary_from_payload(payload, report_path=resolved)


def data_source_fallback_policy_summary_from_payload(
    payload: Mapping[str, Any],
    *,
    report_path: Path | None = None,
) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "validation_status": _text(
            payload.get("validation_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "report_id": _text(payload.get("report_id"), "UNKNOWN"),
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "fallback_status": _text(summary.get("fallback_status"), "UNKNOWN"),
        "source_group_count": summary.get("source_group_count", 0),
        "primary_ok_count": summary.get("primary_ok_count", 0),
        "fallback_used_count": summary.get("fallback_used_count", 0),
        "fallback_unavailable_count": summary.get("fallback_unavailable_count", 0),
        "blocked_no_valid_source_count": summary.get("blocked_no_valid_source_count", 0),
        "blocking_source_count": summary.get("blocking_source_count", 0),
        "fallback_used_sources": ", ".join(_texts(summary.get("fallback_used_sources")))
        or "none",
        "blocking_data_types": ", ".join(_texts(summary.get("blocking_data_types")))
        or "none",
        "next_action": _text(summary.get("next_action"), "UNKNOWN"),
        "safety_status": _fallback_safety_status(payload),
        "report_path": str(
            report_path or _text(_mapping(payload.get("artifact_paths")).get("report_json"))
        ),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "limitation": (
            "Fallback policy is paper-shadow governance only; it does not "
            "download data, mutate caches or approve production use."
        ),
    }


def missing_data_source_fallback_policy_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "report_id": "MISSING",
        "as_of": "UNKNOWN",
        "fallback_status": "MISSING",
        "source_group_count": 0,
        "primary_ok_count": 0,
        "fallback_used_count": 0,
        "fallback_unavailable_count": 0,
        "blocked_no_valid_source_count": 0,
        "blocking_source_count": 0,
        "fallback_used_sources": "none",
        "blocking_data_types": "none",
        "next_action": "generate_data_source_fallback_policy_report",
        "safety_status": "MISSING",
        "report_path": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "limitation": reason,
    }


def validation_report_to_payload(report: DataSourceFallbackValidationReport) -> dict[str, Any]:
    return {
        "schema_version": DATA_SOURCE_FALLBACK_SCHEMA_VERSION,
        "report_type": DATA_SOURCE_FALLBACK_VALIDATION_REPORT_TYPE,
        "report_id": report.report_id,
        "report_path": str(report.report_path),
        "as_of": report.as_of.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "status": report.status,
        "source_group_count": report.source_group_count,
        "error_count": report.error_count,
        "warning_count": report.warning_count,
        "production_effect": PRODUCTION_EFFECT,
        "issues": [_issue_dict(issue) for issue in report.issues],
    }


def render_data_source_fallback_policy_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    paths = _mapping(payload.get("artifact_paths"))
    lines = [
        "# Data Source Fallback Policy",
        "",
        f"- 状态：{_text(payload.get('status'), 'UNKNOWN')}",
        f"- Fallback status：{_text(payload.get('fallback_status'), 'UNKNOWN')}",
        f"- Report ID：`{_text(payload.get('report_id'), 'UNKNOWN')}`",
        f"- 评估日期：{_text(payload.get('as_of'), 'UNKNOWN')}",
        f"- Source group count：{_text(summary.get('source_group_count'), '0')}",
        f"- Fallback used count：{_text(summary.get('fallback_used_count'), '0')}",
        f"- Blocking source count：{_text(summary.get('blocking_source_count'), '0')}",
        f"- Next action：{_text(summary.get('next_action'), 'UNKNOWN')}",
        f"- Production effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- Validation report：`{_text(paths.get('validation_markdown'), 'UNKNOWN')}`",
        "",
        "## 治理边界",
        "",
        _text(_mapping(payload.get("safety_boundary")).get("boundary")),
        "",
        "## Source Groups",
        "",
        "| Data type | Domain | State | Primary sources | Fallback source | "
        "Metadata status | Next action |",
        "|---|---|---|---|---|---|---|",
    ]
    for record in _records(payload.get("records")):
        metadata_status = _text(
            _mapping(record.get("fallback_metadata")).get("status"),
            "NOT_REQUIRED",
        )
        lines.append(
            "| "
            f"{_escape_table(_text(record.get('data_type')))} | "
            f"{_escape_table(_text(record.get('domain')))} | "
            f"{_escape_table(_text(record.get('fallback_state')))} | "
            f"{_escape_table(', '.join(_texts(record.get('primary_source_ids'))))} | "
            f"{_escape_table(_text(record.get('fallback_source_id')) or 'none')} | "
            f"{_escape_table(metadata_status)} | "
            f"{_escape_table(_text(record.get('next_action')))} |"
        )
    issues = _records(payload.get("validation_issues"))
    lines.extend(["", "## 校验问题", ""])
    if not issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| Severity | Code | Data type | Source | Field | Message |",
                "|---|---|---|---|---|---|",
            ]
        )
        for issue in issues:
            lines.append(
                "| "
                f"{_escape_table(_text(issue.get('severity')))} | "
                f"{_escape_table(_text(issue.get('code')))} | "
                f"{_escape_table(_text(issue.get('data_type')))} | "
                f"{_escape_table(_text(issue.get('source_id')))} | "
                f"{_escape_table(_text(issue.get('field')))} | "
                f"{_escape_table(_text(issue.get('message')))} |"
            )
    return "\n".join(lines) + "\n"


def render_data_source_fallback_policy_validation_markdown(
    report: DataSourceFallbackValidationReport,
) -> str:
    lines = [
        "# Data Source Fallback Policy Validation",
        "",
        f"- 状态：{report.status}",
        f"- Report ID：`{report.report_id}`",
        f"- Report path：`{report.report_path}`",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- Source group count：{report.source_group_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- Production effect：{PRODUCTION_EFFECT}",
        "",
        "## Issues",
        "",
    ]
    if not report.issues:
        lines.append("未发现问题。")
    else:
        lines.extend(
            [
                "| Severity | Code | Data type | Source | Field | Message |",
                "|---|---|---|---|---|---|",
            ]
        )
        for issue in report.issues:
            lines.append(
                "| "
                f"{issue.severity.value} | "
                f"{_escape_table(issue.code)} | "
                f"{_escape_table(issue.data_type or '')} | "
                f"{_escape_table(issue.source_id or '')} | "
                f"{_escape_table(issue.field or '')} | "
                f"{_escape_table(issue.message)} |"
            )
    return "\n".join(lines) + "\n"


def render_data_source_fallback_policy_reader_brief(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    return "\n".join(
        [
            "## Data Source Fallback Policy",
            "",
            "- summary: paper-shadow research data fallback policy gate.",
            f"- key_result: fallback_status={_text(summary.get('fallback_status'), 'UNKNOWN')}",
            f"- status: {_text(payload.get('status'), 'UNKNOWN')}",
            f"- report_id: `{_text(payload.get('report_id'), 'UNKNOWN')}`",
            f"- as_of: {_text(payload.get('as_of'), 'UNKNOWN')}",
            f"- fallback_used_count: {_text(summary.get('fallback_used_count'), '0')}",
            "- fallback_used_sources: "
            f"{', '.join(_texts(summary.get('fallback_used_sources'))) or 'none'}",
            "- blocking_data_types: "
            f"{', '.join(_texts(summary.get('blocking_data_types'))) or 'none'}",
            f"- blocking_issues: {', '.join(_texts(summary.get('blocking_reasons'))) or 'none'}",
            f"- warnings: {', '.join(_texts(summary.get('warnings'))) or 'none'}",
            "- safety_boundary: read-only fallback policy / no refresh / "
            "no cache mutation / production_effect="
            f"{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
            f"- next_action: {_text(summary.get('next_action'), 'UNKNOWN')}",
            "",
        ]
    )


def _source_group_record(
    *,
    group: Mapping[str, Any],
    source_map: Mapping[str, DataSourceConfig],
    unavailable_source_ids: set[str],
    fallback_used_source_ids: set[str],
    fallback_reasons: Mapping[str, str],
    build_issues: list[DataSourceFallbackIssue],
) -> dict[str, Any]:
    data_type = _text(group.get("data_type"), "UNKNOWN")
    domain = _text(group.get("domain"), "UNKNOWN")
    primary_ids = _texts(group.get("primary_sources"))
    fallback_ids = _texts(group.get("fallback_sources"))
    ineligible_ids = _texts(group.get("ineligible_sources"))
    primary_records = [
        _source_status(source_id=source_id, source_map=source_map, domain=domain)
        for source_id in primary_ids
    ]
    fallback_records = [
        _source_status(source_id=source_id, source_map=source_map, domain=domain)
        for source_id in fallback_ids
    ]
    ineligible_records = [
        _source_status(source_id=source_id, source_map=source_map, domain=domain)
        for source_id in ineligible_ids
    ]
    primary_available = any(
        row["eligibility_status"] == "ELIGIBLE"
        and row["source_id"] not in unavailable_source_ids
        for row in primary_records
    )
    used_candidates = [
        source_id for source_id in fallback_ids if source_id in fallback_used_source_ids
    ]
    used_source_id = used_candidates[0] if used_candidates else ""
    eligible_fallbacks = [
        row for row in fallback_records if row["eligibility_status"] == "ELIGIBLE"
    ]
    fallback_record = next(
        (row for row in fallback_records if row["source_id"] == used_source_id),
        {},
    )
    if used_source_id:
        state = FALLBACK_STATE_FALLBACK_USED
    elif primary_available:
        state = FALLBACK_STATE_PRIMARY_OK
    elif eligible_fallbacks:
        state = FALLBACK_STATE_FALLBACK_UNAVAILABLE
    else:
        state = FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE

    primary_unavailable = [
        row["source_id"]
        for row in primary_records
        if row["source_id"] in unavailable_source_ids or row["eligibility_status"] != "ELIGIBLE"
    ]
    reason = (
        _text(fallback_reasons.get(used_source_id))
        or _text(fallback_reasons.get(data_type))
        or (
            "explicit fallback source declared"
            if used_source_id
            else "primary source unavailable or no valid source declared"
        )
    )
    fallback_metadata = _fallback_metadata(
        state=state,
        data_type=data_type,
        primary_source_ids=primary_ids,
        primary_unavailable_source_ids=primary_unavailable,
        fallback_source_id=used_source_id,
        fallback_record=fallback_record,
        fallback_reason=reason,
    )
    if state == FALLBACK_STATE_FALLBACK_USED and fallback_metadata["status"] != "COMPLETE":
        build_issues.append(
            DataSourceFallbackIssue(
                severity=DataSourceFallbackIssueSeverity.ERROR,
                code="fallback_used_metadata_incomplete",
                message="Fallback used requires explicit complete artifact metadata.",
                data_type=data_type,
                source_id=used_source_id,
                field="fallback_metadata",
            )
        )
    if (
        state == FALLBACK_STATE_FALLBACK_USED
        and _text(fallback_record.get("eligibility_status")) != "ELIGIBLE"
    ):
        build_issues.append(
            DataSourceFallbackIssue(
                severity=DataSourceFallbackIssueSeverity.ERROR,
                code="fallback_used_source_not_eligible",
                message="Fallback source was declared used but is not eligible by policy.",
                data_type=data_type,
                source_id=used_source_id,
                field="fallback_source_id",
            )
        )
    return {
        "schema_version": DATA_SOURCE_FALLBACK_SCHEMA_VERSION,
        "data_type": data_type,
        "domain": domain,
        "fallback_state": state,
        "primary_source_ids": primary_ids,
        "primary_unavailable_source_ids": primary_unavailable,
        "fallback_source_id": used_source_id,
        "fallback_candidate_source_ids": fallback_ids,
        "eligible_fallback_source_ids": [row["source_id"] for row in eligible_fallbacks],
        "ineligible_source_ids": ineligible_ids,
        "primary_sources": primary_records,
        "fallback_sources": fallback_records,
        "ineligible_sources": ineligible_records,
        "fallback_metadata": fallback_metadata,
        "fallback_used_by_artifact_metadata": state == FALLBACK_STATE_FALLBACK_USED,
        "artifact_metadata_required": group.get("artifact_metadata_required") is not False,
        "fail_closed_when_unavailable": group.get("fail_closed_when_unavailable") is not False,
        "blocking": state
        in {FALLBACK_STATE_FALLBACK_UNAVAILABLE, FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE},
        "warning": state == FALLBACK_STATE_FALLBACK_USED,
        "next_action": _text(group.get("owner_action")) or _record_next_action(state),
        "production_effect": PRODUCTION_EFFECT,
        "safety_boundary": DATA_SOURCE_FALLBACK_SAFETY,
    }


def _source_status(
    *,
    source_id: str,
    source_map: Mapping[str, DataSourceConfig],
    domain: str,
) -> dict[str, Any]:
    source = source_map.get(source_id)
    if source is None:
        return {
            "source_id": source_id,
            "exists_in_catalog": False,
            "status": "MISSING",
            "source_type": "UNKNOWN",
            "domains": [],
            "cache_paths": [],
            "cache_path_status": "MISSING",
            "eligibility_status": "INELIGIBLE",
            "eligibility_reason": "source_missing_from_config_data_sources",
        }
    cache_paths = [str(path) for path in source.cache_paths]
    artifact_metadata = _cache_artifact_metadata(cache_paths)
    cache_exists = artifact_metadata["source_artifact_path_status"] == "AVAILABLE"
    allowed_type = source.source_type in {"primary_source", "paid_vendor"}
    active = source.status == "active"
    domain_match = domain in source.domains
    eligible = active and allowed_type and domain_match
    reason = "eligible"
    if not active:
        reason = "source_not_active"
    elif not allowed_type:
        reason = "source_type_not_allowed_for_paper_shadow_fallback"
    elif not domain_match:
        reason = "source_domain_mismatch"
    return {
        "source_id": source.source_id,
        "provider": source.provider,
        "endpoint": source.endpoint,
        "request_parameters": "UNKNOWN",
        "exists_in_catalog": True,
        "status": source.status,
        "source_type": source.source_type,
        "domains": list(source.domains),
        "cache_paths": cache_paths,
        "cache_path_status": "AVAILABLE" if cache_exists else "MISSING",
        **artifact_metadata,
        "eligibility_status": "ELIGIBLE" if eligible else "INELIGIBLE",
        "eligibility_reason": reason,
    }


def _fallback_metadata(
    *,
    state: str,
    data_type: str,
    primary_source_ids: Sequence[str],
    primary_unavailable_source_ids: Sequence[str],
    fallback_source_id: str,
    fallback_record: Mapping[str, Any],
    fallback_reason: str,
) -> dict[str, Any]:
    if state != FALLBACK_STATE_FALLBACK_USED:
        return {
            "status": "NOT_REQUIRED",
            "fallback_state": state,
            "required_when": FALLBACK_STATE_FALLBACK_USED,
        }
    cache_paths = _texts(fallback_record.get("cache_paths"))
    source_artifact_path = next((path for path in cache_paths if Path(path).exists()), "")
    if not source_artifact_path and cache_paths:
        source_artifact_path = cache_paths[0]
    required = {
        "fallback_state": state,
        "primary_source_id": ",".join(primary_source_ids),
        "primary_unavailable_source_ids": list(primary_unavailable_source_ids),
        "fallback_source_id": fallback_source_id,
        "fallback_reason": fallback_reason,
        "provider": _text(fallback_record.get("provider")),
        "endpoint": _text(fallback_record.get("endpoint")),
        "request_parameters": _text(fallback_record.get("request_parameters"), "UNKNOWN"),
        "downloaded_at": _text(fallback_record.get("downloaded_at")),
        "downloaded_at_source": _text(fallback_record.get("downloaded_at_source")),
        "row_count": fallback_record.get("row_count"),
        "checksum": _text(fallback_record.get("checksum")),
        "source_priority_rank": 1,
        "source_eligibility_status": _text(fallback_record.get("eligibility_status")),
        "source_artifact_path": source_artifact_path,
        "source_artifact_path_status": _text(fallback_record.get("cache_path_status"), "UNKNOWN"),
        "data_type": data_type,
        "production_effect": PRODUCTION_EFFECT,
    }
    missing = [
        key
        for key in (
            "fallback_state",
            "primary_source_id",
            "fallback_source_id",
            "fallback_reason",
            "provider",
            "endpoint",
            "downloaded_at",
            "row_count",
            "checksum",
            "source_priority_rank",
            "source_eligibility_status",
            "source_artifact_path",
            "production_effect",
        )
        if required.get(key) in (None, "")
    ]
    return {
        "status": "COMPLETE" if not missing else "INCOMPLETE",
        "missing_fields": missing,
        **required,
    }


def _cache_artifact_metadata(cache_paths: Sequence[str]) -> dict[str, Any]:
    existing = next((Path(path) for path in cache_paths if Path(path).exists()), None)
    if existing is None:
        return {
            "source_artifact_path": "",
            "source_artifact_path_status": "MISSING",
            "downloaded_at": "",
            "downloaded_at_source": "MISSING_CACHE_ARTIFACT",
            "row_count": None,
            "checksum": "",
        }
    stat = existing.stat()
    return {
        "source_artifact_path": str(existing),
        "source_artifact_path_status": "AVAILABLE",
        "downloaded_at": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
        "downloaded_at_source": "cache_file_mtime_utc",
        "row_count": _count_rows(existing),
        "checksum": _sha256(existing),
    }


def _count_rows(path: Path) -> int | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except UnicodeDecodeError:
        return None
    if not lines:
        return 0
    return max(len(lines) - 1, 0)


def _summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    states = [_text(record.get("fallback_state")) for record in records]
    state_counts = {state: states.count(state) for state in sorted(FALLBACK_STATES)}
    fallback_status = _overall_fallback_status(states)
    blocking_records = [
        record
        for record in records
        if record.get("fallback_state")
        in {FALLBACK_STATE_FALLBACK_UNAVAILABLE, FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE}
    ]
    fallback_used_records = [
        record for record in records if record.get("fallback_state") == FALLBACK_STATE_FALLBACK_USED
    ]
    status = (
        "FAIL"
        if blocking_records
        else "PASS_WITH_WARNINGS"
        if fallback_used_records
        else "PASS"
    )
    blocking_data_types = [_text(record.get("data_type")) for record in blocking_records]
    warnings = [
        f"{record.get('data_type')}:fallback_used:{record.get('fallback_source_id')}"
        for record in fallback_used_records
    ]
    blocking_reasons = [
        f"{record.get('data_type')}:{record.get('fallback_state')}"
        for record in blocking_records
    ]
    return {
        "status": status,
        "fallback_status": fallback_status,
        "source_group_count": len(records),
        "primary_ok_count": state_counts[FALLBACK_STATE_PRIMARY_OK],
        "fallback_used_count": state_counts[FALLBACK_STATE_FALLBACK_USED],
        "fallback_unavailable_count": state_counts[FALLBACK_STATE_FALLBACK_UNAVAILABLE],
        "blocked_no_valid_source_count": state_counts[FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE],
        "blocking_source_count": len(blocking_records),
        "state_counts": state_counts,
        "fallback_used_sources": _dedupe_texts(
            [record.get("fallback_source_id") for record in fallback_used_records]
        ),
        "blocking_data_types": _dedupe_texts(blocking_data_types),
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "next_action": _summary_next_action(fallback_status),
    }


def _overall_fallback_status(states: Sequence[str]) -> str:
    if FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE in states:
        return FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE
    if FALLBACK_STATE_FALLBACK_UNAVAILABLE in states:
        return FALLBACK_STATE_FALLBACK_UNAVAILABLE
    if FALLBACK_STATE_FALLBACK_USED in states:
        return FALLBACK_STATE_FALLBACK_USED
    return FALLBACK_STATE_PRIMARY_OK


def _summary_next_action(status: str) -> str:
    return {
        FALLBACK_STATE_PRIMARY_OK: "continue_with_normal_data_quality_and_freshness_gates",
        FALLBACK_STATE_FALLBACK_USED: "manual_review_fallback_metadata_before_interpretation",
        FALLBACK_STATE_FALLBACK_UNAVAILABLE: (
            "restore_primary_or_generate_explicit_eligible_fallback_artifact"
        ),
        FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE: (
            "restore_primary_or_approve_new_valid_source_before_continuing"
        ),
    }.get(status, "manual_review_required")


def _record_next_action(state: str) -> str:
    return {
        FALLBACK_STATE_PRIMARY_OK: "continue_with_primary_source",
        FALLBACK_STATE_FALLBACK_USED: "review_explicit_fallback_metadata",
        FALLBACK_STATE_FALLBACK_UNAVAILABLE: "restore_primary_or_explicit_fallback",
        FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE: "block_until_valid_source_available",
    }.get(state, "manual_review_required")


def _check_top_level_contract(
    payload: Mapping[str, Any],
    issues: list[DataSourceFallbackIssue],
) -> None:
    if payload.get("report_type") != DATA_SOURCE_FALLBACK_REPORT_TYPE:
        issues.append(
            DataSourceFallbackIssue(
                DataSourceFallbackIssueSeverity.ERROR,
                "invalid_report_type",
                "Report type must be data_source_fallback_policy.",
                field="report_type",
            )
        )
    if payload.get("status") not in DATA_SOURCE_FALLBACK_STATUSES:
        issues.append(
            DataSourceFallbackIssue(
                DataSourceFallbackIssueSeverity.ERROR,
                "invalid_status",
                "Status must be PASS, PASS_WITH_WARNINGS or FAIL.",
                field="status",
            )
        )
    if payload.get("production_effect") != PRODUCTION_EFFECT:
        issues.append(
            DataSourceFallbackIssue(
                DataSourceFallbackIssueSeverity.ERROR,
                "invalid_production_effect",
                "production_effect must be none.",
                field="production_effect",
            )
        )


def _check_safety_boundary(
    payload: Mapping[str, Any],
    issues: list[DataSourceFallbackIssue],
) -> None:
    safety = _mapping(payload.get("safety_boundary"))
    required = {
        "read_only": True,
        "data_refresh_allowed": False,
        "cache_mutation_allowed": False,
        "score_or_backtest_allowed": False,
        "broker_action_allowed": False,
        "order_ticket_allowed": False,
        "production_state_mutation_allowed": False,
    }
    for field_name, expected in required.items():
        if safety.get(field_name) is not expected:
            issues.append(
                DataSourceFallbackIssue(
                    DataSourceFallbackIssueSeverity.ERROR,
                    "safety_boundary_invalid",
                    f"safety_boundary.{field_name} must be {expected}.",
                    field=f"safety_boundary.{field_name}",
                )
            )


def _check_records(
    records: Sequence[Mapping[str, Any]],
    issues: list[DataSourceFallbackIssue],
) -> None:
    if not records:
        issues.append(
            DataSourceFallbackIssue(
                DataSourceFallbackIssueSeverity.ERROR,
                "source_groups_missing",
                "At least one source group is required.",
                field="records",
            )
        )
    for record in records:
        data_type = _text(record.get("data_type"))
        state = _text(record.get("fallback_state"))
        if state not in FALLBACK_STATES:
            issues.append(
                DataSourceFallbackIssue(
                    DataSourceFallbackIssueSeverity.ERROR,
                    "invalid_fallback_state",
                    "Fallback state is not part of the policy state set.",
                    data_type=data_type,
                    field="fallback_state",
                )
            )
        if state in {
            FALLBACK_STATE_FALLBACK_UNAVAILABLE,
            FALLBACK_STATE_BLOCKED_NO_VALID_SOURCE,
        }:
            issues.append(
                DataSourceFallbackIssue(
                    DataSourceFallbackIssueSeverity.ERROR,
                    "fallback_policy_blocking_state",
                    "Fallback policy has no valid source for this data type.",
                    data_type=data_type,
                    field="fallback_state",
                )
            )
        if state == FALLBACK_STATE_FALLBACK_USED:
            issues.append(
                DataSourceFallbackIssue(
                    DataSourceFallbackIssueSeverity.WARNING,
                    "fallback_used_manual_review_required",
                    (
                        "FALLBACK_USED requires explicit owner review before "
                        "investment interpretation."
                    ),
                    data_type=data_type,
                    source_id=_text(record.get("fallback_source_id")),
                    field="fallback_state",
                )
            )
        metadata = _mapping(record.get("fallback_metadata"))
        if state == FALLBACK_STATE_FALLBACK_USED:
            if metadata.get("status") != "COMPLETE":
                issues.append(
                    DataSourceFallbackIssue(
                        DataSourceFallbackIssueSeverity.ERROR,
                        "fallback_used_metadata_incomplete",
                        "FALLBACK_USED records require complete explicit metadata.",
                        data_type=data_type,
                        source_id=_text(record.get("fallback_source_id")),
                        field="fallback_metadata",
                    )
                )
            if not record.get("fallback_source_id"):
                issues.append(
                    DataSourceFallbackIssue(
                        DataSourceFallbackIssueSeverity.ERROR,
                        "fallback_used_source_missing",
                        "FALLBACK_USED records require fallback_source_id.",
                        data_type=data_type,
                        field="fallback_source_id",
                    )
                )


def _policy_metadata(policy: Mapping[str, Any]) -> dict[str, Any]:
    metadata = dict(_mapping(policy.get("policy_metadata")))
    metadata.setdefault(
        "policy_version",
        _text(policy.get("policy_version"), DATA_SOURCE_FALLBACK_POLICY_VERSION),
    )
    return metadata


def _safety_boundary(policy: Mapping[str, Any]) -> dict[str, Any]:
    safety = dict(DATA_SOURCE_FALLBACK_SAFETY)
    safety.update(_mapping(policy.get("safety_boundary")))
    safety["production_effect"] = PRODUCTION_EFFECT
    safety["boundary"] = DATA_SOURCE_FALLBACK_SAFETY["boundary"]
    return safety


def _fallback_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety_boundary"))
    return (
        "PASS"
        if _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("read_only") is True
        and safety.get("data_refresh_allowed") is False
        and safety.get("cache_mutation_allowed") is False
        and safety.get("broker_action_allowed") is False
        and safety.get("order_ticket_allowed") is False
        else "REVIEW_REQUIRED"
    )


def _with_validation_summary(
    payload: Mapping[str, Any],
    *,
    report_path: Path,
    validation: DataSourceFallbackValidationReport | None = None,
) -> dict[str, Any]:
    output = dict(payload)
    report = validation or validate_data_source_fallback_policy_payload(
        output,
        report_path=report_path,
    )
    output["status"] = report.status
    output["validation_status"] = report.status
    output["validation_error_count"] = report.error_count
    output["validation_warning_count"] = report.warning_count
    output["validation_issues"] = [_issue_dict(issue) for issue in report.issues]
    return output


def _write_latest_pointer(
    *,
    output_dir: Path,
    payload: Mapping[str, Any],
    report_path: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic_without_trailing_newline(
        output_dir / LATEST_POINTER_NAME,
        {
            "schema_version": DATA_SOURCE_FALLBACK_SCHEMA_VERSION,
            "report_type": DATA_SOURCE_FALLBACK_REPORT_TYPE,
            "report_id": _text(payload.get("report_id")),
            "report_path": str(report_path),
            "as_of": _text(payload.get("as_of")),
            "status": _text(payload.get("status")),
            "fallback_status": _text(payload.get("fallback_status")),
            "production_effect": PRODUCTION_EFFECT,
        },
    )


def _report_id(
    *,
    as_of: date,
    generated_at: datetime,
    records: Sequence[Mapping[str, Any]],
) -> str:
    digest = sha256(
        json.dumps(
            {
                "as_of": as_of.isoformat(),
                "generated_at": generated_at.isoformat(),
                "records": records,
            },
            sort_keys=True,
            default=str,
        ).encode("utf-8")
    ).hexdigest()
    return f"data-source-fallback-policy_{as_of.isoformat()}_{digest[:16]}"


def _issue_dict(issue: DataSourceFallbackIssue) -> dict[str, Any]:
    return {
        "severity": issue.severity.value,
        "code": issue.code,
        "message": issue.message,
        "data_type": issue.data_type or "",
        "source_id": issue.source_id or "",
        "field": issue.field or "",
    }


def _issue_from_mapping(payload: Mapping[str, Any]) -> DataSourceFallbackIssue:
    severity = _text(payload.get("severity"), DataSourceFallbackIssueSeverity.ERROR.value)
    return DataSourceFallbackIssue(
        severity=DataSourceFallbackIssueSeverity(severity)
        if severity in {"ERROR", "WARNING"}
        else DataSourceFallbackIssueSeverity.ERROR,
        code=_text(payload.get("code"), "unknown_issue"),
        message=_text(payload.get("message")),
        data_type=_text(payload.get("data_type")) or None,
        source_id=_text(payload.get("source_id")) or None,
        field=_text(payload.get("field")) or None,
    )


def _read_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"JSON artifact must be an object: {path}")
    return raw


def _sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _parse_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value[:10])
    except (TypeError, ValueError):
        return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _texts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [str(item) for item in value if str(item)]
    return []


def _dedupe_texts(values: Any) -> list[str]:
    output: list[str] = []
    for value in _texts(values):
        if value not in output:
            output.append(value)
    return output


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
