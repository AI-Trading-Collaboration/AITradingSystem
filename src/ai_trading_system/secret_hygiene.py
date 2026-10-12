from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT

SECRET_PATTERNS = (
    re.compile(
        r"(?i)['\"]?\b(api[_-]?key|secret[_-]?key|secret|token|password|authorization)\b['\"]?\s*[:=]\s*['\"]?([A-Za-z0-9_.\-]{24,})"
    ),
    re.compile(r"(?i)\bBearer\s+([A-Za-z0-9_.\-]{24,})"),
)
GENERATED_REPORT_METADATA_FILE_PATTERN = re.compile(
    r"^(executable_binding_safety_audit|next_candidate_research_cycle_snapshot|"
    r"reader_brief_consistency_pack)_\d{4}-\d{2}-\d{2}\.(json|md)$"
)
GENERATED_REPORT_METADATA_LABELS = {
    "api_key",
    "apikey",
    "api-key",
    "secret_key",
    "secret-key",
    "secret",
    "token",
    "password",
    "authorization",
}
SOURCE_LOCATION_VALUE_PATTERN = re.compile(
    r"^[A-Za-z0-9_.\-]+\.(py|yaml|yml|json|md)$"
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
SECRET_SCAN_CACHE_SCHEMA_VERSION = "secret_scan_cache.v1"
DEFAULT_SECRET_SCAN_CACHE_PATH = (
    PROJECT_ROOT / "data" / "cache" / "secret_hygiene" / "scan_cache.json"
)
# Operational bound, not an investment threshold: once the last full scan is this old the cache is
# ignored and every file is read again, so an edit that kept both size and mtime is still caught
# within a week (OPS-083).
SECRET_SCAN_FULL_RESCAN_INTERVAL = timedelta(days=7)


class SecretScanMode(StrEnum):
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"


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
    scan_mode: SecretScanMode = SecretScanMode.FULL
    full_scan_reason: str | None = None
    read_file_count: int | None = None
    reused_file_count: int = 0
    last_full_scan_at: datetime | None = None

    @property
    def error_count(self) -> int:
        return sum(
            1 for finding in self.findings if finding.severity == SecretFindingSeverity.ERROR
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
    cache_path: Path | None = None,
    force_full_scan: bool = False,
    now: datetime | None = None,
) -> SecretScanReport:
    """Scan ``paths`` for suspected secret literals.

    Without ``cache_path`` every file is read (the original behaviour). With it, a file whose size
    and mtime match the previous scan and that had no finding then is not read again; the cache is
    ignored (full scan) when it is missing or unreadable, when the scanner code or the scanned roots
    changed, when ``force_full_scan`` is set, or when the last full scan is older than
    SECRET_SCAN_FULL_RESCAN_INTERVAL. Files with findings are never cached, so a finding is
    reported on every run until the file changes.
    """
    generated_at = now or datetime.now(tz=UTC)
    entries = tuple(_iter_scannable_entries(paths))
    findings: list[SecretScanFinding] = []
    if cache_path is None:
        for path, _ in entries:
            _scan_file(path, findings)
        return SecretScanReport(
            as_of=as_of,
            generated_at=generated_at,
            scanned_paths=paths,
            scanned_file_count=len(entries),
            findings=tuple(findings),
            full_scan_reason="未启用增量缓存",
            read_file_count=len(entries),
        )

    scope = _cache_scope(paths)
    rules_sha256 = _scan_rules_sha256()
    cache, full_scan_reason = _load_reusable_cache(
        cache_path,
        scope=scope,
        rules_sha256=rules_sha256,
        now=generated_at,
        force_full_scan=force_full_scan,
    )
    previous = {} if cache is None else cache.entries
    clean_entries: dict[str, tuple[int, int]] = {}
    read_file_count = 0
    reused_file_count = 0
    for path, stamp in entries:
        key = str(path)
        if stamp is not None and previous.get(key) == stamp:
            clean_entries[key] = stamp
            reused_file_count += 1
            continue
        read_file_count += 1
        finding_count = len(findings)
        readable = _scan_file(path, findings)
        if readable and stamp is not None and len(findings) == finding_count:
            clean_entries[key] = stamp
    last_full_scan_at = generated_at if cache is None else cache.last_full_scan_at
    _write_cache(
        cache_path,
        _SecretScanCache(
            rules_sha256=rules_sha256,
            scope=scope,
            last_full_scan_at=last_full_scan_at,
            entries=clean_entries,
        ),
    )
    return SecretScanReport(
        as_of=as_of,
        generated_at=generated_at,
        scanned_paths=paths,
        scanned_file_count=len(entries),
        findings=tuple(findings),
        scan_mode=SecretScanMode.FULL if cache is None else SecretScanMode.INCREMENTAL,
        full_scan_reason=full_scan_reason,
        read_file_count=read_file_count,
        reused_file_count=reused_file_count,
        last_full_scan_at=last_full_scan_at,
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
        f"- 扫描方式：{_scan_mode_text(report)}",
        f"- 本次读取文件数：{_read_file_count(report)}；沿用上次结果：{report.reused_file_count}",
        f"- 上次全量扫描：{_last_full_scan_text(report)}",
        f"- 错误数：{report.error_count}",
        f"- 警告数：{report.warning_count}",
        f"- 生产影响：{report.production_effect}",
        "",
        "## 方法边界",
        "",
        "- 本报告扫描疑似 API key、token、secret、password 和 bearer credential。",
        "- 报告只输出脱敏片段，不输出完整疑似密钥。",
        "- 第一阶段不替代企业密钥管理、pre-commit hook、CI secret scan 或供应商权限审批。",
        "- 增量扫描只沿用上次无发现、且大小和修改时间都没变的文件的结果；有发现的文件每次重读。"
        "缓存缺失或损坏、扫描规则代码或扫描入口变化、显式要求或距上次全量扫描满 "
        f"{SECRET_SCAN_FULL_RESCAN_INTERVAL.days} 天时自动全量。",
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


def _iter_scannable_entries(
    paths: tuple[Path, ...],
) -> Iterator[tuple[Path, tuple[int, int] | None]]:
    """Yield scannable regular files with their (size, mtime_ns) stamp, in rglob order."""
    for path in paths:
        if not path.exists():
            continue
        if path.is_file():
            if _is_scannable(path):
                yield path, _file_stamp(path)
            continue
        for child in path.rglob("*"):
            if not _is_scannable(child):
                continue
            try:
                child_stat = child.stat()
            except OSError:
                continue
            if stat.S_ISREG(child_stat.st_mode):
                yield child, (child_stat.st_size, child_stat.st_mtime_ns)


def _file_stamp(path: Path) -> tuple[int, int] | None:
    try:
        path_stat = path.stat()
    except OSError:
        return None
    return path_stat.st_size, path_stat.st_mtime_ns


def _is_scannable(path: Path) -> bool:
    if path.suffix.lower() not in SCANNED_SUFFIXES:
        return False
    return not any(part in IGNORED_PARTS for part in path.parts)


def _scan_file(path: Path, findings: list[SecretScanFinding]) -> bool:
    """Append findings for ``path``; return False when the file could not be read this time."""
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return True
    except OSError:
        return False
    for line_number, line in enumerate(text.splitlines(), start=1):
        for pattern in SECRET_PATTERNS:
            for match in pattern.finditer(line):
                value = match.group(match.lastindex or 1)
                label = match.group(1) if (match.lastindex or 0) > 1 else "Bearer"
                if _is_allowed_generated_report_metadata_match(
                    path=path,
                    line=line,
                    label=label,
                    value=value,
                ):
                    continue
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
    return True


@dataclass(frozen=True)
class _SecretScanCache:
    rules_sha256: str
    scope: tuple[str, ...]
    last_full_scan_at: datetime
    entries: dict[str, tuple[int, int]]


def _cache_scope(paths: tuple[Path, ...]) -> tuple[str, ...]:
    return tuple(str(path.resolve()) for path in paths)


def _scan_rules_sha256() -> str:
    # Any change to this module (patterns, suffixes, allowlist logic) invalidates the cache.
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def _load_reusable_cache(
    cache_path: Path,
    *,
    scope: tuple[str, ...],
    rules_sha256: str,
    now: datetime,
    force_full_scan: bool,
) -> tuple[_SecretScanCache | None, str | None]:
    if force_full_scan:
        return None, "显式要求全量扫描"
    if not cache_path.exists():
        return None, "缓存不存在"
    try:
        cache = _parse_cache(json.loads(cache_path.read_text(encoding="utf-8")))
    except (OSError, ValueError, TypeError, KeyError):
        return None, "缓存无法读取或格式不符"
    if cache.rules_sha256 != rules_sha256:
        return None, "扫描规则代码已变化"
    if cache.scope != scope:
        return None, "扫描入口已变化"
    if cache.last_full_scan_at > now:
        return None, "上次全量扫描时间晚于当前时间"
    if now - cache.last_full_scan_at >= SECRET_SCAN_FULL_RESCAN_INTERVAL:
        return None, f"距上次全量扫描已满 {SECRET_SCAN_FULL_RESCAN_INTERVAL.days} 天"
    return cache, None


def _parse_cache(payload: object) -> _SecretScanCache:
    if not isinstance(payload, dict):
        raise ValueError("cache payload must be an object")
    if payload["schema_version"] != SECRET_SCAN_CACHE_SCHEMA_VERSION:
        raise ValueError("unsupported cache schema")
    last_full_scan_at = datetime.fromisoformat(payload["last_full_scan_at"])
    if last_full_scan_at.tzinfo is None:
        raise ValueError("last_full_scan_at must carry a timezone")
    raw_entries = payload["entries"]
    if not isinstance(raw_entries, dict):
        raise ValueError("entries must be an object")
    entries: dict[str, tuple[int, int]] = {}
    for key, value in raw_entries.items():
        if (
            not isinstance(value, list)
            or len(value) != 2
            or not all(isinstance(item, int) and not isinstance(item, bool) for item in value)
        ):
            raise ValueError("cache entry must be [size, mtime_ns]")
        entries[str(key)] = (value[0], value[1])
    scope = payload["scope"]
    if not isinstance(scope, list) or not all(isinstance(item, str) for item in scope):
        raise ValueError("scope must be a list of strings")
    rules_sha256 = payload["rules_sha256"]
    if not isinstance(rules_sha256, str):
        raise ValueError("rules_sha256 must be a string")
    return _SecretScanCache(
        rules_sha256=rules_sha256,
        scope=tuple(scope),
        last_full_scan_at=last_full_scan_at,
        entries=entries,
    )


def _write_cache(cache_path: Path, cache: _SecretScanCache) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SECRET_SCAN_CACHE_SCHEMA_VERSION,
        "rules_sha256": cache.rules_sha256,
        "scope": list(cache.scope),
        "last_full_scan_at": cache.last_full_scan_at.isoformat(),
        "entries": {key: list(stamp) for key, stamp in cache.entries.items()},
    }
    temporary_path = cache_path.with_name(f"{cache_path.name}.tmp")
    temporary_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary_path, cache_path)


def _scan_mode_text(report: SecretScanReport) -> str:
    text = "增量" if report.scan_mode == SecretScanMode.INCREMENTAL else "全量"
    if report.full_scan_reason:
        return f"{text}（{report.full_scan_reason}）"
    return text


def _read_file_count(report: SecretScanReport) -> int:
    if report.read_file_count is None:
        return report.scanned_file_count
    return report.read_file_count


def _last_full_scan_text(report: SecretScanReport) -> str:
    if report.last_full_scan_at is None:
        return "本次"
    return report.last_full_scan_at.isoformat()


def _is_allowed_generated_report_metadata_match(
    *,
    path: Path,
    line: str,
    label: str,
    value: str,
) -> bool:
    if GENERATED_REPORT_METADATA_FILE_PATTERN.fullmatch(path.name) is None:
        return False

    normalized_label = label.lower().replace("-", "_")
    if normalized_label not in GENERATED_REPORT_METADATA_LABELS:
        return False
    if SOURCE_LOCATION_VALUE_PATTERN.fullmatch(value) is None:
        return False

    source_location = re.compile(
        rf"(?<![A-Za-z0-9_.\-]){re.escape(label)}:{re.escape(value)}:\d+"
        r"(?![A-Za-z0-9_.\-])",
        flags=re.IGNORECASE,
    )
    if source_location.search(line) is None:
        return False

    return _has_generated_report_metadata_context(line)


def _has_generated_report_metadata_context(line: str) -> bool:
    stripped = line.strip()
    # Generated governance reports encode finding metadata as
    # <term_family>:<source_file>:<line>. These are audit source locations, not
    # secret values, and this allowlist is limited to those report metadata forms.
    if (
        stripped.startswith('"finding_id":')
        or stripped.startswith('"warnings":')
        or stripped.startswith("- warnings:")
    ):
        return True
    if stripped.startswith('"decision_state":') and "\\n- warnings:" in stripped:
        return True
    if re.fullmatch(
        r'"[A-Za-z0-9_\-]+:[A-Za-z0-9_.\-]+\.(py|yaml|yml|json|md):\d+",?',
        stripped,
    ):
        return True
    if re.match(
        r"^\|[A-Za-z0-9_\-]+:[A-Za-z0-9_.\-]+\.(py|yaml|yml|json|md):\d+\|",
        stripped,
    ):
        return True
    return False


def _redact(value: str) -> str:
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


def _escape_markdown_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
