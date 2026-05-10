from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

LAST_UPDATED_RE = re.compile(r"最后更新[:：]\s*(20\d{2}-\d{2}-\d{2})")
STATUS_DATE_RE = re.compile(r"(20\d{2}-\d{2}-\d{2})[：:]")


@dataclass(frozen=True)
class DocsFreshnessIssue:
    path: Path
    code: str
    message: str


@dataclass(frozen=True)
class DocsFreshnessRecord:
    path: Path
    last_updated: date | None
    latest_status_date: date | None
    status: str


@dataclass(frozen=True)
class DocsFreshnessReport:
    generated_at: datetime
    records: tuple[DocsFreshnessRecord, ...]
    issues: tuple[DocsFreshnessIssue, ...]

    @property
    def passed(self) -> bool:
        return not self.issues

    @property
    def status(self) -> str:
        return "PASS" if self.passed else "FAIL"


def default_docs_freshness_paths(project_root: Path) -> tuple[Path, ...]:
    docs_dir = project_root / "docs"
    paths = [
        docs_dir / "task_register.md",
        docs_dir / "task_register_completed.md",
        docs_dir / "implementation_backlog.md",
        docs_dir / "runbook_daily_ops.md",
    ]
    paths.extend(sorted((docs_dir / "requirements").glob("*.md")))
    return tuple(dict.fromkeys(paths))


def validate_docs_freshness(paths: tuple[Path, ...]) -> DocsFreshnessReport:
    records: list[DocsFreshnessRecord] = []
    issues: list[DocsFreshnessIssue] = []
    for path in paths:
        if not path.exists():
            records.append(
                DocsFreshnessRecord(
                    path=path,
                    last_updated=None,
                    latest_status_date=None,
                    status="FAIL",
                )
            )
            issues.append(
                DocsFreshnessIssue(
                    path=path,
                    code="missing_document",
                    message="文档不存在。",
                )
            )
            continue
        text = path.read_text(encoding="utf-8")
        last_updated = _last_updated_date(text)
        latest_status = _latest_status_record_date(text)
        status = "PASS"
        if last_updated is None:
            status = "FAIL"
            issues.append(
                DocsFreshnessIssue(
                    path=path,
                    code="missing_last_updated",
                    message="缺少 `最后更新：YYYY-MM-DD`。",
                )
            )
        elif latest_status is not None and latest_status > last_updated:
            status = "FAIL"
            issues.append(
                DocsFreshnessIssue(
                    path=path,
                    code="stale_last_updated",
                    message=(
                        "`最后更新` 早于文档内部最新状态记录日期："
                        f"{last_updated.isoformat()} < {latest_status.isoformat()}。"
                    ),
                )
            )
        records.append(
            DocsFreshnessRecord(
                path=path,
                last_updated=last_updated,
                latest_status_date=latest_status,
                status=status,
            )
        )
    return DocsFreshnessReport(
        generated_at=datetime.now(tz=UTC),
        records=tuple(records),
        issues=tuple(issues),
    )


def render_docs_freshness_report(report: DocsFreshnessReport) -> str:
    lines = [
        "# 文档新鲜度检查",
        "",
        f"- 状态：{report.status}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 检查文档数：{len(report.records)}",
        f"- 问题数：{len(report.issues)}",
        "",
        "## 检查结果",
        "",
        "| 文档 | 状态 | 最后更新 | 最新状态记录 |",
        "|---|---|---|---|",
    ]
    for record in report.records:
        last_updated = "" if record.last_updated is None else record.last_updated.isoformat()
        latest_status = (
            ""
            if record.latest_status_date is None
            else record.latest_status_date.isoformat()
        )
        lines.append(
            "| "
            f"`{record.path}` | "
            f"{record.status} | "
            f"{last_updated} | "
            f"{latest_status} |"
        )
    lines.extend(["", "## 问题", ""])
    if not report.issues:
        lines.append("未发现文档新鲜度问题。")
    else:
        lines.extend(["| 文档 | Code | Message |", "|---|---|---|"])
        for issue in report.issues:
            lines.append(f"| `{issue.path}` | `{issue.code}` | {issue.message} |")
    return "\n".join(lines) + "\n"


def write_docs_freshness_report(report: DocsFreshnessReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_docs_freshness_report(report), encoding="utf-8")
    return output_path


def _last_updated_date(text: str) -> date | None:
    match = LAST_UPDATED_RE.search(text)
    if match is None:
        return None
    return date.fromisoformat(match.group(1))


def _latest_status_record_date(text: str) -> date | None:
    found: list[date] = []
    for line in text.splitlines():
        if "最后更新" in line:
            continue
        for match in STATUS_DATE_RE.finditer(line):
            found.append(date.fromisoformat(match.group(1)))
    return max(found) if found else None
