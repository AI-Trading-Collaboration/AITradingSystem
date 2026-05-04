from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
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
) -> PipelineHealthReport:
    return PipelineHealthReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        checks=tuple(_check_artifact(artifact) for artifact in artifacts),
    )


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
        "- 本报告只检查关键输入/输出文件的存在性、大小、mtime 和排查入口。",
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
        lines.append("未发现缺失或空文件。")
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


def _check_status(check: PipelineArtifactCheck) -> str:
    if check.severity is None:
        return "OK"
    return check.severity.value


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
