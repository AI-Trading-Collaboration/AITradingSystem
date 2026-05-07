from __future__ import annotations

import csv
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path


class PipelineHealthSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class PipelineArtifactSpec:
    artifact_id: str
    label: str
    path: Path
    required: bool
    investigation_hint: str


@dataclass(frozen=True)
class PipelineArtifactCheck:
    spec: PipelineArtifactSpec
    exists: bool
    size_bytes: int | None
    modified_at: datetime | None
    severity: PipelineHealthSeverity | None
    message: str


@dataclass(frozen=True)
class PipelineHealthReport:
    as_of: date
    generated_at: datetime
    checks: tuple[PipelineArtifactCheck, ...]
    production_effect: str = "none"

    @property
    def error_count(self) -> int:
        return sum(1 for check in self.checks if check.severity == PipelineHealthSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for check in self.checks if check.severity == PipelineHealthSeverity.WARNING)

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


def build_pipeline_health_report(
    *,
    as_of: date,
    artifacts: tuple[PipelineArtifactSpec, ...],
    extra_checks: tuple[PipelineArtifactCheck, ...] = (),
) -> PipelineHealthReport:
    return PipelineHealthReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        checks=tuple(_check_artifact(artifact) for artifact in artifacts) + extra_checks,
    )


def build_pit_snapshot_health_checks(
    *,
    as_of: date,
    manifest_path: Path,
    normalized_path: Path,
    validation_report_path: Path,
    fetch_report_path: Path | None = None,
    project_root: Path,
    min_manifest_records: int = 1,
    min_normalized_rows: int = 1,
    max_snapshot_age_days: int = 3,
) -> tuple[PipelineArtifactCheck, ...]:
    if min_manifest_records < 0:
        raise ValueError("min_manifest_records must be non-negative")
    if min_normalized_rows < 0:
        raise ValueError("min_normalized_rows must be non-negative")
    if max_snapshot_age_days < 0:
        raise ValueError("max_snapshot_age_days must be non-negative")

    checks = [
        _check_artifact(
            PipelineArtifactSpec(
                "pit_manifest",
                "PIT raw snapshot manifest",
                manifest_path,
                True,
                (
                    "运行 `aits pit-snapshots fetch-fmp-forward` "
                    "或 `aits pit-snapshots build-manifest`。"
                ),
            )
        ),
        _check_artifact(
            PipelineArtifactSpec(
                "pit_validation_report",
                "PIT 快照质量报告",
                validation_report_path,
                True,
                "运行 `aits pit-snapshots validate` 并检查错误清单。",
            )
        ),
        _check_artifact(
            PipelineArtifactSpec(
                "fmp_forward_pit_normalized",
                "FMP PIT 标准化 as-of 索引",
                normalized_path,
                True,
                "运行 `aits pit-snapshots fetch-fmp-forward`。",
            )
        ),
    ]
    if fetch_report_path is not None:
        checks.append(
            _check_artifact(
                PipelineArtifactSpec(
                    "fmp_forward_pit_fetch_report",
                    "FMP PIT 抓取报告",
                    fetch_report_path,
                    True,
                    (
                        "运行 `aits pit-snapshots fetch-fmp-forward`，"
                        "并检查抓取/写入/校验阶段错误。"
                    ),
                )
            )
        )
        if _path_size(fetch_report_path):
            checks.append(_fmp_forward_pit_fetch_report_status_check(fetch_report_path))
    manifest_rows = _read_csv_rows(manifest_path)
    normalized_rows = _read_csv_rows(normalized_path)
    checks.extend(
        [
            _row_count_check(
                artifact_id="pit_manifest_row_count",
                label="PIT manifest row count",
                path=manifest_path,
                row_count=len(manifest_rows),
                minimum=min_manifest_records,
                investigation_hint="确认每日 PIT 抓取是否成功，或检查 raw cache 发现目录。",
            ),
            _row_count_check(
                artifact_id="fmp_forward_pit_normalized_row_count",
                label="FMP PIT normalized row count",
                path=normalized_path,
                row_count=len(normalized_rows),
                minimum=min_normalized_rows,
                investigation_hint="确认 FMP forward PIT endpoint 是否返回记录。",
            ),
            _freshness_check(
                artifact_id="pit_manifest_freshness",
                label="PIT manifest available_time 新鲜度",
                path=manifest_path,
                rows=manifest_rows,
                as_of=as_of,
                max_age_days=max_snapshot_age_days,
                investigation_hint="检查最近一次 PIT raw snapshot 的 available_time。",
            ),
            _freshness_check(
                artifact_id="fmp_forward_pit_normalized_freshness",
                label="FMP PIT normalized available_time 新鲜度",
                path=normalized_path,
                rows=normalized_rows,
                as_of=as_of,
                max_age_days=max_snapshot_age_days,
                investigation_hint="检查 FMP PIT 标准化索引是否断更。",
            ),
            _pit_manifest_checksum_check(
                manifest_path=manifest_path,
                rows=manifest_rows,
                project_root=project_root,
            ),
        ]
    )
    return tuple(checks)


def render_pipeline_health_report(report: PipelineHealthReport) -> str:
    lines = [
        "# Pipeline Health 报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 检查项：{len(report.checks)}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- 生产影响：{report.production_effect}",
        "",
        "## 方法边界",
        "",
        "- 本报告检查关键输入/输出文件的存在性、大小、mtime、PIT row count、"
        "available_time 新鲜度、raw payload checksum 和排查入口。",
        "- 运行健康不等于投资结论有效；投资结论仍以数据质量门禁、结论使用等级、"
        "输入覆盖和审计报告为准。",
        "- 第一阶段未接入结构化 run log、后台调度器、异常栈或 API 错误采集。",
        "",
        "## Artifact 检查",
        "",
        "| Artifact | Required | Status | Size | Modified At | Path | 排查入口 |",
        "|---|---|---|---:|---|---|---|",
    ]
    for check in report.checks:
        lines.append(
            "| "
            f"{_escape_markdown_table(check.spec.label)}（`{check.spec.artifact_id}`） | "
            f"{check.spec.required} | "
            f"{_check_status(check)} | "
            f"{'' if check.size_bytes is None else check.size_bytes} | "
            f"{'' if check.modified_at is None else check.modified_at.isoformat()} | "
            f"`{check.spec.path}` | "
            f"{_escape_markdown_table(check.spec.investigation_hint)} |"
        )
    issues = [check for check in report.checks if check.severity is not None]
    lines.extend(["", "## 问题清单", ""])
    if not issues:
        lines.append("未发现运行健康问题。")
    else:
        lines.extend(["| Severity | Artifact | Message |", "|---|---|---|"])
        for check in issues:
            lines.append(
                "| "
                f"{check.severity.value if check.severity else ''} | "
                f"`{check.spec.artifact_id}` | "
                f"{_escape_markdown_table(check.message)} |"
            )
    return "\n".join(lines) + "\n"


def write_pipeline_health_report(
    report: PipelineHealthReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_pipeline_health_report(report), encoding="utf-8")
    return output_path


def default_pipeline_health_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"pipeline_health_{as_of.isoformat()}.md"


def _check_artifact(spec: PipelineArtifactSpec) -> PipelineArtifactCheck:
    if not spec.path.exists():
        severity = (
            PipelineHealthSeverity.ERROR
            if spec.required
            else PipelineHealthSeverity.WARNING
        )
        return PipelineArtifactCheck(
            spec=spec,
            exists=False,
            size_bytes=None,
            modified_at=None,
            severity=severity,
            message=f"文件不存在：{spec.path}",
        )
    try:
        stat = spec.path.stat()
    except OSError as exc:
        return PipelineArtifactCheck(
            spec=spec,
            exists=True,
            size_bytes=None,
            modified_at=None,
            severity=PipelineHealthSeverity.ERROR,
            message=f"无法读取文件状态：{exc}",
        )
    modified_at = datetime.fromtimestamp(stat.st_mtime, tz=UTC)
    if stat.st_size <= 0:
        return PipelineArtifactCheck(
            spec=spec,
            exists=True,
            size_bytes=stat.st_size,
            modified_at=modified_at,
            severity=PipelineHealthSeverity.ERROR,
            message=f"文件为空：{spec.path}",
        )
    return PipelineArtifactCheck(
        spec=spec,
        exists=True,
        size_bytes=stat.st_size,
        modified_at=modified_at,
        severity=None,
        message="OK",
    )


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    size_bytes = _path_size(path)
    if size_bytes is None or size_bytes <= 0:
        return []
    try:
        with path.open(encoding="utf-8", newline="") as file:
            return list(csv.DictReader(file))
    except (OSError, csv.Error, UnicodeDecodeError):
        return []


def _row_count_check(
    *,
    artifact_id: str,
    label: str,
    path: Path,
    row_count: int,
    minimum: int,
    investigation_hint: str,
) -> PipelineArtifactCheck:
    severity = None if row_count >= minimum else PipelineHealthSeverity.ERROR
    message = "OK" if severity is None else f"row_count={row_count} 低于最低要求 {minimum}。"
    return PipelineArtifactCheck(
        spec=PipelineArtifactSpec(artifact_id, label, path, True, investigation_hint),
        exists=path.exists(),
        size_bytes=_path_size(path),
        modified_at=_modified_at(path),
        severity=severity,
        message=message,
    )


def _freshness_check(
    *,
    artifact_id: str,
    label: str,
    path: Path,
    rows: Iterable[dict[str, str]],
    as_of: date,
    max_age_days: int,
    investigation_hint: str,
) -> PipelineArtifactCheck:
    latest = _latest_available_time(rows)
    severity = None
    if latest is None:
        severity = PipelineHealthSeverity.WARNING
        message = "未发现可解析的 available_time。"
    else:
        age_days = (as_of - latest.date()).days
        if age_days < 0:
            severity = PipelineHealthSeverity.ERROR
            message = (
                f"latest_available_time={latest.isoformat()} 晚于 as_of="
                f"{as_of.isoformat()}。"
            )
        elif age_days > max_age_days:
            severity = PipelineHealthSeverity.WARNING
            message = f"latest_available_time={latest.isoformat()}，距 as_of 已 {age_days} 天。"
        else:
            message = "OK"
    return PipelineArtifactCheck(
        spec=PipelineArtifactSpec(artifact_id, label, path, True, investigation_hint),
        exists=path.exists(),
        size_bytes=_path_size(path),
        modified_at=_modified_at(path),
        severity=severity,
        message=message,
    )


def _pit_manifest_checksum_check(
    *,
    manifest_path: Path,
    rows: list[dict[str, str]],
    project_root: Path,
) -> PipelineArtifactCheck:
    mismatches: list[str] = []
    for row in rows:
        raw_payload_path = row.get("raw_payload_path") or ""
        expected_checksum = row.get("raw_payload_sha256") or ""
        if not raw_payload_path or not expected_checksum:
            mismatches.append(row.get("snapshot_id") or "manifest_row_without_checksum")
            continue
        payload_path = _resolve_payload_path(raw_payload_path, manifest_path, project_root)
        actual_checksum = _file_sha256(payload_path) if payload_path.exists() else None
        if actual_checksum != expected_checksum:
            mismatches.append(row.get("snapshot_id") or raw_payload_path)
    severity = PipelineHealthSeverity.ERROR if mismatches else None
    message = (
        "OK"
        if not mismatches
        else f"PIT raw payload checksum 异常 {len(mismatches)} 条：{', '.join(mismatches[:5])}"
    )
    return PipelineArtifactCheck(
        spec=PipelineArtifactSpec(
            "pit_manifest_checksum",
            "PIT manifest raw payload checksum",
            manifest_path,
            True,
            "运行 `aits pit-snapshots validate` 定位 checksum mismatch。",
        ),
        exists=manifest_path.exists(),
        size_bytes=_path_size(manifest_path),
        modified_at=_modified_at(manifest_path),
        severity=severity,
        message=message,
    )


def _fmp_forward_pit_fetch_report_status_check(
    fetch_report_path: Path,
) -> PipelineArtifactCheck:
    status = _read_markdown_status(fetch_report_path)
    severity: PipelineHealthSeverity | None = None
    if status is None:
        severity = PipelineHealthSeverity.WARNING
        message = "无法解析 FMP PIT 抓取报告状态。"
    elif status == "PASS":
        message = "OK"
    elif status in {"PASS_WITH_WARNINGS", "FAIL"}:
        severity = PipelineHealthSeverity.WARNING
        message = (
            f"FMP PIT 抓取报告状态为 {status}；"
            "日常调度可继续，但失败或降级 PIT 不得作为可用输入。"
        )
    else:
        severity = PipelineHealthSeverity.WARNING
        message = f"FMP PIT 抓取报告状态未知：{status}。"
    return PipelineArtifactCheck(
        spec=PipelineArtifactSpec(
            "fmp_forward_pit_fetch_status",
            "FMP PIT 抓取报告状态",
            fetch_report_path,
            True,
            "打开 FMP PIT 抓取报告的问题清单，确认供应商请求、写入和 PIT 校验阶段。",
        ),
        exists=fetch_report_path.exists(),
        size_bytes=_path_size(fetch_report_path),
        modified_at=_modified_at(fetch_report_path),
        severity=severity,
        message=message,
    )


def _read_markdown_status(path: Path) -> str | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return None
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- 状态："):
            return stripped.removeprefix("- 状态：").strip().strip("`")
    return None


def _latest_available_time(rows: Iterable[dict[str, str]]) -> datetime | None:
    latest: datetime | None = None
    for row in rows:
        raw_value = row.get("available_time") or ""
        try:
            value = _parse_datetime(raw_value)
        except ValueError:
            continue
        if latest is None or value > latest:
            latest = value
    return latest


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _resolve_payload_path(value: str, manifest_path: Path, project_root: Path) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    manifest_relative = manifest_path.parent / path
    if manifest_relative.exists():
        return manifest_relative
    return project_root / path


def _file_sha256(path: Path) -> str | None:
    digest = sha256()
    try:
        with path.open("rb") as file:
            for chunk in iter(lambda: file.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def _path_size(path: Path) -> int | None:
    try:
        return path.stat().st_size if path.exists() else None
    except OSError:
        return None


def _modified_at(path: Path) -> datetime | None:
    if not path.exists():
        return None
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return None


def _check_status(check: PipelineArtifactCheck) -> str:
    if check.severity is None:
        return "OK"
    return check.severity.value


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
