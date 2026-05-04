from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path

SECRET_PATTERNS = (
    re.compile(
        r"(?i)\b(api[_-]?key|secret|token|password|authorization)\b\s*[:=]\s*['\"]?([A-Za-z0-9_.\-]{24,})"
    ),
    re.compile(r"(?i)\bBearer\s+([A-Za-z0-9_.\-]{24,})"),
)
SCANNED_SUFFIXES = {
    ".csv",
    ".json",
    ".md",
    ".py",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
IGNORED_PARTS = {".git", ".mypy_cache", ".pytest_cache", ".ruff_cache", "__pycache__"}


class SecretFindingSeverity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"


@dataclass(frozen=True)
class SecretScanFinding:
    severity: SecretFindingSeverity
    code: str
    path: Path
    line_number: int
    matched_label: str
    redacted_value: str
    message: str


@dataclass(frozen=True)
class SecretScanReport:
    as_of: date
    generated_at: datetime
    scanned_paths: tuple[Path, ...]
    scanned_file_count: int
    findings: tuple[SecretScanFinding, ...]
    production_effect: str = "none"

    @property
    def error_count(self) -> int:
        return sum(
            1
            for finding in self.findings
            if finding.severity == SecretFindingSeverity.ERROR
        )

    @property
    def warning_count(self) -> int:
        return sum(
            1 for finding in self.findings if finding.severity == SecretFindingSeverity.WARNING
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


def scan_secrets(
    *,
    paths: tuple[Path, ...],
    as_of: date,
) -> SecretScanReport:
    files = tuple(_iter_scannable_files(paths))
    findings: list[SecretScanFinding] = []
    for path in files:
        _scan_file(path, findings)
    return SecretScanReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        scanned_paths=paths,
        scanned_file_count=len(files),
        findings=tuple(findings),
    )


def render_secret_scan_report(report: SecretScanReport) -> str:
    lines = [
        "# Secret Hygiene 扫描报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.as_of.isoformat()}",
        f"- 生成时间：{report.generated_at.isoformat()}",
        f"- 扫描入口数：{len(report.scanned_paths)}",
        f"- 扫描文件数：{report.scanned_file_count}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- 生产影响：{report.production_effect}",
        "",
        "## 方法边界",
        "",
        "- 本报告扫描疑似 API key、token、secret、password 和 bearer credential。",
        "- 报告只输出脱敏片段，不输出完整疑似密钥。",
        "- 第一阶段不替代企业密钥管理、pre-commit hook、CI secret scan 或供应商权限审批。",
        "",
        "## 扫描入口",
        "",
    ]
    lines.extend(f"- `{path}`" for path in report.scanned_paths)
    lines.extend(["", "## 发现项", ""])
    if not report.findings:
        lines.append("未发现疑似 secret。")
    else:
        lines.extend(
            [
                "| Severity | Code | File | Line | Label | Redacted | Message |",
                "|---|---|---|---:|---|---|---|",
            ]
        )
        for finding in report.findings:
            lines.append(
                "| "
                f"{finding.severity.value} | "
                f"`{finding.code}` | "
                f"`{finding.path}` | "
                f"{finding.line_number} | "
                f"{_escape_markdown_table(finding.matched_label)} | "
                f"`{finding.redacted_value}` | "
                f"{_escape_markdown_table(finding.message)} |"
            )
    return "\n".join(lines) + "\n"


def write_secret_scan_report(report: SecretScanReport, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_secret_scan_report(report), encoding="utf-8")
    return output_path


def default_secret_scan_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"secret_hygiene_{as_of.isoformat()}.md"


def _iter_scannable_files(paths: tuple[Path, ...]):
    for path in paths:
        if not path.exists():
            continue
        if path.is_file():
            if _is_scannable(path):
                yield path
            continue
        for child in path.rglob("*"):
            if child.is_file() and _is_scannable(child):
                yield child


def _is_scannable(path: Path) -> bool:
    if path.suffix.lower() not in SCANNED_SUFFIXES:
        return False
    return not any(part in IGNORED_PARTS for part in path.parts)


def _scan_file(path: Path, findings: list[SecretScanFinding]) -> None:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return
    except OSError:
        return
    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern in SECRET_PATTERNS:
            match = pattern.search(line)
            if match is None:
                continue
            value = match.group(match.lastindex or 1)
            label = match.group(1) if (match.lastindex or 0) > 1 else "Bearer"
            findings.append(
                SecretScanFinding(
                    severity=SecretFindingSeverity.ERROR,
                    code="suspected_secret_literal",
                    path=path,
                    line_number=line_number,
                    matched_label=label,
                    redacted_value=_redact(value),
                    message=(
                        "疑似 secret literal 出现在可扫描文件中；"
                        "应改为环境变量或安全密钥管理。"
                    ),
                )
            )


def _redact(value: str) -> str:
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
