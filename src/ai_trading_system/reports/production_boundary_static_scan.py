from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, date, datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = 1
REPORT_TYPE = "production_boundary_static_scan"
VALIDATION_REPORT_TYPE = "production_boundary_static_scan_validation"
PRODUCTION_EFFECT = "none"

OK_STATUS = "OK"
WARN_STATUS = "WARNING"
FAIL_STATUS = "BLOCKING"
VALID_STATUSES = {OK_STATUS, WARN_STATUS, FAIL_STATUS}

DEFAULT_ALLOWLIST_PATH = PROJECT_ROOT / "config" / "production_boundary_static_scan_allowlist.yaml"
DEFAULT_SCAN_ROOTS: tuple[str, ...] = ("src", "config", "docs", "scripts", "README.md")
REQUIRED_TERM_FAMILIES: tuple[str, ...] = (
    "broker",
    "order_ticket",
    "live_allocation",
    "official_target_weight",
    "production_mutation",
    "auto_execute",
    "live_order",
    "account_id",
    "api_key",
)
TERM_PATTERNS: dict[str, tuple[str, ...]] = {
    "broker": (
        "broker",
        "broker_action",
        "broker integration",
        "broker workflow",
    ),
    "order_ticket": (
        "order ticket",
        "order_ticket",
        "order tickets",
    ),
    "live_allocation": (
        "live allocation",
        "live_allocation",
        "automatic live allocation",
    ),
    "official_target_weight": (
        "official target weight",
        "official target weights",
        "official_target_weight",
        "official_target_weights",
        "target_weights_official",
    ),
    "production_mutation": (
        "production mutation",
        "production_state",
        "production state",
        "production workflow",
        "production weight",
        "production weights",
    ),
    "auto_execute": (
        "auto execute",
        "auto_execute",
        "auto-execute",
        "automatic execution",
        "auto_apply=true",
    ),
    "live_order": (
        "live order",
        "live_order",
        "submit order",
        "place order",
    ),
    "account_id": (
        "account id",
        "account_id",
        "accountid",
    ),
    "api_key": (
        "api key",
        "api_key",
        "apikey",
        "secret key",
        "secret_key",
    ),
}
SAFE_CONTEXT_MARKERS: tuple[str, ...] = (
    "no ",
    "not ",
    "never ",
    "without ",
    "cannot ",
    "can't ",
    "blocked",
    "read-only",
    "read only",
    "manual-review-only",
    "manual review only",
    "research-only",
    "research only",
    "paper-shadow-only",
    "paper shadow only",
    "observation-only",
    "observe-only",
    "governance-only",
    "documentation-only",
    "safety boundary",
    "guardrail",
    "guardrails",
    "unchanged",
    "production_effect=none",
    "broker_action_allowed=false",
    "broker_action_taken=false",
    "order_ticket_generated=false",
    "order_ticket_allowed=false",
    "not_official",
    "mutated=false",
    "auto_apply=false",
    "false",
    "none",
    "不得",
    "不新增",
    "不做",
    "不改变",
    "不写",
    "不补造",
    "不重跑",
    "不运行",
    "不刷新",
    "不修改",
    "不生成",
    "不允许",
    "不支持",
    "不自动",
    "不触发",
    "不接",
    "不会",
    "未",
    "禁止",
    "只读",
)
TEXT_SUFFIXES: set[str] = {
    ".bat",
    ".cfg",
    ".ini",
    ".json",
    ".md",
    ".ps1",
    ".py",
    ".sh",
    ".toml",
    ".txt",
    ".yaml",
    ".yml",
}
SKIP_DIR_NAMES: set[str] = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "data",
    "htmlcov",
    "node_modules",
    "outputs",
    "reports",
}
SECRET_LITERAL_RE = re.compile(
    r"\b(?P<key>api[_-]?key|secret[_-]?key|account[_-]?id|accountid|apikey)\b"
    r"\s*[:=]\s*[\"'](?P<value>[^\"']{8,})[\"']",
    re.IGNORECASE,
)
PLACEHOLDER_MARKERS = (
    "example",
    "placeholder",
    "redacted",
    "dummy",
    "test",
    "xxxx",
    "<",
    ">",
    "your_",
    "none",
    "changeme",
)


def default_production_boundary_static_scan_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"production_boundary_static_scan_{as_of.isoformat()}.json"


def default_production_boundary_static_scan_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"production_boundary_static_scan_{as_of.isoformat()}.md"


def default_production_boundary_static_scan_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"production_boundary_static_scan_validation_{as_of.isoformat()}.json"


def default_production_boundary_static_scan_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"production_boundary_static_scan_validation_{as_of.isoformat()}.md"


def latest_production_boundary_static_scan_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "production_boundary_static_scan_", ".json")


def build_production_boundary_static_scan_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    scan_roots: Sequence[Path | str] | None = None,
    allowlist_path: Path = DEFAULT_ALLOWLIST_PATH,
) -> dict[str, Any]:
    policy = load_production_boundary_allowlist(allowlist_path)
    resolved_roots = _resolve_scan_roots(project_root, scan_roots)
    files = list(_iter_scan_files(resolved_roots, project_root))
    file_checks: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    allowed_match_count = 0
    scanned_line_count = 0
    covered_families: set[str] = set()
    for path in files:
        check = _scan_file(path, project_root=project_root, allowlist_policy=policy)
        file_checks.append(check)
        findings.extend(_records(check.get("findings")))
        allowed_match_count += _int(check.get("allowed_match_count"))
        scanned_line_count += _int(check.get("scanned_line_count"))
        covered_families.update(str(item) for item in check.get("covered_term_families", []))

    blocking_findings = [finding for finding in findings if finding["severity"] == FAIL_STATUS]
    warning_findings = [finding for finding in findings if finding["severity"] == WARN_STATUS]
    status = FAIL_STATUS if blocking_findings else WARN_STATUS if warning_findings else OK_STATUS
    family_counts = _family_counts(findings)
    summary = {
        "scan_root_count": len(resolved_roots),
        "scanned_file_count": len(files),
        "scanned_line_count": scanned_line_count,
        "term_family_count": len(REQUIRED_TERM_FAMILIES),
        "covered_term_family_count": len(covered_families),
        "finding_count": len(findings),
        "blocking_finding_count": len(blocking_findings),
        "warning_finding_count": len(warning_findings),
        "allowed_match_count": allowed_match_count,
        "family_counts": family_counts,
        "static_scan_input": _static_scan_input(status),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "scan_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": (
            "Detect dangerous production-facing terms or accidental broker/order "
            "integration in source, config, and docs."
        ),
        "input_artifacts": {
            "allowlist_policy": str(allowlist_path),
            "scan_roots": [str(path) for path in resolved_roots],
        },
        "output_decision": status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Static scanner only reads local text files under configured scan roots.",
            (
                "Documentation-only and explicit safety-boundary mentions can be allowed, "
                "but suspicious source/config terms remain visible."
            ),
            "Secret-like values are not printed; only a masked marker is reported.",
        ],
        "next_action": _next_action(status),
        "allowlist_policy": {
            "path": str(allowlist_path),
            "policy_id": _text(policy.get("policy_id")),
            "version": _text(policy.get("version")),
            "status": _text(policy.get("status")),
            "rule_count": len(_records(policy.get("rules"))),
        },
        "term_families": [
            {"term_family": family, "patterns": list(TERM_PATTERNS[family])}
            for family in REQUIRED_TERM_FAMILIES
        ],
        "scan_roots": [str(path) for path in resolved_roots],
        "summary": summary,
        "findings": findings,
        "blocking_findings": blocking_findings,
        "warning_findings": warning_findings,
        "file_checks": file_checks,
        "reader_brief": _reader_brief(status, summary, blocking_findings, warning_findings),
        "methodology": {
            "mode": "read_local_text_files_only",
            "skipped_directories": sorted(SKIP_DIR_NAMES),
            "text_suffixes": sorted(TEXT_SUFFIXES),
            "safe_context_markers": list(SAFE_CONTEXT_MARKERS),
            "does_not_run_upstream_commands": True,
            "does_not_modify_source": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_production_boundary_static_scan_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    status = _text(payload.get("scan_status"), _text(payload.get("status"), "UNKNOWN"))
    summary = _mapping(payload.get("summary"))
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        payload.get("report_type") == REPORT_TYPE,
        "BLOCKING",
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_static_scan_report",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect",
        payload.get("production_effect") == PRODUCTION_EFFECT,
        "BLOCKING",
        "Static scanner must be production_effect=none.",
        "regenerate_without_production_mutation",
    )
    _append_check(
        checks,
        blocking_issues,
        "status_enum",
        status in VALID_STATUSES,
        "BLOCKING",
        f"scan_status must be one of {', '.join(sorted(VALID_STATUSES))}.",
        "restore_scan_status_enum",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_term_families_declared",
        set(_term_family_ids(payload.get("term_families"))) == set(REQUIRED_TERM_FAMILIES),
        "BLOCKING",
        "Static scanner must declare all required production-boundary term families.",
        "restore_required_term_family_manifest",
    )
    _append_check(
        checks,
        blocking_issues,
        "no_blocking_findings",
        _int(summary.get("blocking_finding_count")) == 0 and status != FAIL_STATUS,
        "BLOCKING",
        "Static scanner must not contain blocking production-boundary findings.",
        "fix_or_explicitly_block_production_boundary_violation",
    )
    _append_check(
        checks,
        warning_issues,
        "no_warning_findings",
        _int(summary.get("warning_finding_count")) == 0,
        "WARNING",
        "Static scanner found warning-level production-boundary terms.",
        "review_documentation_or_allowlist_suspicious_warning",
    )
    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    validation_status = (
        FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else OK_STATUS
    )
    failed_checks = [
        check
        for check in checks
        if check["status"] == "FAIL" and check["severity"] == "BLOCKING"
    ]
    warning_checks = [
        check
        for check in checks
        if check["status"] == "FAIL" and check["severity"] == "WARNING"
    ]
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len(failed_checks),
        "warning_check_count": len(warning_checks),
        "source_scanned_file_count": _int(summary.get("scanned_file_count")),
        "source_finding_count": _int(summary.get("finding_count")),
        "source_blocking_finding_count": _int(summary.get("blocking_finding_count")),
        "source_warning_finding_count": _int(summary.get("warning_finding_count")),
        "source_allowed_match_count": _int(summary.get("allowed_match_count")),
        "blocking_issue_count": len(blocking_issues),
        "warning_issue_count": len(warning_issues),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_scan_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "purpose": "Validate the production boundary static scanner fail-closed contract.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": validation_status,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation reads an existing static scan report only.",
            "Warning findings remain visible but do not weaken blocking findings.",
        ],
        "next_action": _validation_next_action(validation_status),
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(
            validation_status,
            validation_summary,
            blocking_issues,
            warning_issues,
        ),
        "methodology": {
            "mode": "read_existing_production_boundary_static_scan_only",
            "production_effect": PRODUCTION_EFFECT,
            "does_not_run_upstream_commands": True,
            "does_not_modify_source": True,
            "does_not_modify_production": True,
        },
    }


def load_production_boundary_allowlist(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {"policy_id": "missing_allowlist_policy", "status": "MISSING", "rules": []}
    raw = safe_load_yaml_path(path) or {}
    if isinstance(raw, Mapping):
        return raw
    return {"policy_id": "invalid_allowlist_policy", "rules": []}


def write_production_boundary_static_scan_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_production_boundary_static_scan_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_production_boundary_static_scan_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def write_production_boundary_static_scan_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_production_boundary_static_scan_json(payload, output_path)


def write_production_boundary_static_scan_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_production_boundary_static_scan_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_production_boundary_static_scan_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    reader = _mapping(payload.get("reader_brief"))
    lines = [
        f"# Production Boundary Static Scan {payload.get('as_of')}",
        "",
        "## Reader Brief",
        "",
        f"- Summary: {_text(reader.get('summary'))}",
        f"- Key Result: {_text(reader.get('key_result'))}",
        f"- Blocking Issues: {_text(reader.get('blocking_issues'))}",
        f"- Warnings: {_text(reader.get('warnings'))}",
        f"- Safety Boundary: {_text(reader.get('safety_boundary'))}",
        f"- Next Action: {_text(reader.get('next_action'))}",
        "",
        "## Summary",
        "",
        f"- status: {_text(payload.get('scan_status'), 'UNKNOWN')}",
        f"- scanned files: {summary.get('scanned_file_count')}",
        f"- findings: {summary.get('finding_count')}",
        f"- blocking findings: {summary.get('blocking_finding_count')}",
        f"- warning findings: {summary.get('warning_finding_count')}",
        f"- allowed matches: {summary.get('allowed_match_count')}",
        f"- static scan input: {summary.get('static_scan_input')}",
        f"- production_effect: {_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- next_action: {_text(payload.get('next_action'))}",
        "",
        "## Findings",
        "",
        "|severity|term family|path|line|matched term|reason|",
        "|---|---|---|---:|---|---|",
    ]
    for finding in _records(payload.get("findings"))[:200]:
        lines.append(
            "|"
            f"{_markdown_cell(finding.get('severity'))}|"
            f"{_markdown_cell(finding.get('term_family'))}|"
            f"{_markdown_cell(finding.get('path'))}|"
            f"{_markdown_cell(finding.get('line_number'))}|"
            f"{_markdown_cell(finding.get('matched_term'))}|"
            f"{_markdown_cell(finding.get('reason'))}|"
        )
    if not _records(payload.get("findings")):
        lines.append("|OK|none|none|0|none|No suspicious production-boundary findings.|")
    lines.extend(["", "## Safety Boundary", ""])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    return "\n".join(lines)


def render_production_boundary_static_scan_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Production Boundary Static Scan Validation {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- status: {_text(payload.get('validation_status'), 'UNKNOWN')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- source findings: {summary.get('source_finding_count')}",
        f"- production_effect: {_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- next_action: {_text(payload.get('next_action'))}",
        "",
        "## Checks",
        "",
        "|check|severity|status|message|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            "|"
            f"{_markdown_cell(check.get('check_id'))}|"
            f"{_markdown_cell(check.get('severity'))}|"
            f"{_markdown_cell(check.get('status'))}|"
            f"{_markdown_cell(check.get('message'))}|"
        )
    lines.append("")
    return "\n".join(lines)


def _resolve_scan_roots(project_root: Path, scan_roots: Sequence[Path | str] | None) -> list[Path]:
    raw_roots: Sequence[Path | str] = scan_roots or DEFAULT_SCAN_ROOTS
    resolved: list[Path] = []
    for raw in raw_roots:
        path = Path(raw)
        resolved.append(path if path.is_absolute() else project_root / path)
    return resolved


def _iter_scan_files(roots: Iterable[Path], project_root: Path) -> Iterable[Path]:
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        if root.is_file():
            if _is_text_file(root) and root not in seen:
                seen.add(root)
                yield root
            continue
        for path in root.rglob("*"):
            if path in seen or not path.is_file():
                continue
            if _should_skip_path(path, project_root) or not _is_text_file(path):
                continue
            seen.add(path)
            yield path


def _scan_file(
    path: Path,
    *,
    project_root: Path,
    allowlist_policy: Mapping[str, Any],
) -> dict[str, Any]:
    rel_path = _relative_path(path, project_root)
    findings: list[dict[str, Any]] = []
    allowed_count = 0
    covered: set[str] = set()
    scanned_lines = 0
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as exc:
        return {
            "path": rel_path,
            "read_status": "FAILED",
            "error": str(exc),
            "scanned_line_count": 0,
            "covered_term_families": [],
            "allowed_match_count": 0,
            "findings": [
                {
                    "severity": WARN_STATUS,
                    "term_family": "file_read",
                    "path": rel_path,
                    "line_number": 0,
                    "matched_term": "unreadable_file",
                    "context": "",
                    "allowlist_rule_id": "",
                    "reason": "Scanner could not read file.",
                    "recommended_action": "inspect_unreadable_file",
                }
            ],
        }
    for line_number, line in enumerate(lines, start=1):
        scanned_lines += 1
        lower = line.lower()
        for family, patterns in TERM_PATTERNS.items():
            matched = _matched_pattern(lower, patterns)
            if not matched:
                continue
            covered.add(family)
            context = _context(lines, line_number)
            severity, reason, rule_id = _classify_match(
                family=family,
                line=line,
                context=context,
                rel_path=rel_path,
                allowlist_policy=allowlist_policy,
            )
            if severity == OK_STATUS:
                allowed_count += 1
                continue
            findings.append(
                {
                    "severity": severity,
                    "term_family": family,
                    "path": rel_path,
                    "line_number": line_number,
                    "matched_term": matched,
                    "context": _safe_context_excerpt(context),
                    "allowlist_rule_id": rule_id,
                    "reason": reason,
                    "recommended_action": _recommended_action(severity, family),
                }
            )
    return {
        "path": rel_path,
        "read_status": "READ",
        "scanned_line_count": scanned_lines,
        "covered_term_families": sorted(covered),
        "allowed_match_count": allowed_count,
        "finding_count": len(findings),
        "blocking_finding_count": len(
            [item for item in findings if item["severity"] == FAIL_STATUS]
        ),
        "warning_finding_count": len(
            [item for item in findings if item["severity"] == WARN_STATUS]
        ),
        "findings": findings,
    }


def _classify_match(
    *,
    family: str,
    line: str,
    context: str,
    rel_path: str,
    allowlist_policy: Mapping[str, Any],
) -> tuple[str, str, str]:
    rule = _matching_allowlist_rule(rel_path, family, allowlist_policy)
    secret = SECRET_LITERAL_RE.search(line)
    if secret and family in {"api_key", "account_id"}:
        if _is_placeholder_secret(secret.group("value")):
            return OK_STATUS, "placeholder_secret_context", "safe_context"
        if family == "account_id" and (
            _has_paper_context(context)
            or (rule and _text(rule.get("max_severity")) == WARN_STATUS)
        ):
            return WARN_STATUS, "paper_account_id_literal_detected_value_masked", _text(
                _mapping(rule).get("rule_id")
            )
        return FAIL_STATUS, "secret_like_literal_detected_value_masked", ""
    if _has_safe_context(context):
        return OK_STATUS, "explicit_safety_boundary_context", "safe_context"
    if _has_high_confidence_blocking_signal(family, line):
        if rule and _text(rule.get("max_severity")) == WARN_STATUS:
            return WARN_STATUS, _text(rule.get("reason"), "allowlist_limited_to_warning"), _text(
                rule.get("rule_id")
            )
        return FAIL_STATUS, "high_confidence_production_boundary_action", ""
    base = _base_severity(family, rel_path)
    if rule and base == FAIL_STATUS and _text(rule.get("max_severity")) == WARN_STATUS:
        return WARN_STATUS, _text(rule.get("reason"), "allowlist_limited_to_warning"), _text(
            rule.get("rule_id")
        )
    if rule and base == WARN_STATUS and _text(rule.get("max_severity")) == WARN_STATUS:
        return WARN_STATUS, _text(rule.get("reason"), "allowlist_warning"), _text(
            rule.get("rule_id")
        )
    if rule and _text(rule.get("max_severity")) == OK_STATUS:
        return OK_STATUS, _text(rule.get("reason"), "allowlisted"), _text(rule.get("rule_id"))
    return base, "production_boundary_term_without_explicit_safe_context", ""


def _base_severity(family: str, rel_path: str) -> str:
    return WARN_STATUS


def _has_high_confidence_blocking_signal(family: str, line: str) -> bool:
    lower = line.lower()
    if family in {"broker", "live_order"}:
        return bool(
            re.search(r"\bbroker\.\w*order\b", lower)
            or re.search(r"\bsubmit_order\s*\(", lower)
            or re.search(r"\bplace_order\s*\(", lower)
            or re.search(r"\blive_order\s*[:=]\s*true\b", lower)
        )
    if family == "order_ticket":
        return bool(
            re.search(r"\border_ticket_(generated|allowed)\s*[:=]\s*true\b", lower)
        )
    if family == "official_target_weight":
        return bool(re.search(r"\bofficial_target_weights\s*[:=]\s*true\b", lower))
    if family == "production_mutation":
        return bool(
            re.search(r"\bproduction_state_mutated\s*[:=]\s*true\b", lower)
            or re.search(r"\bproduction_effect\s*[:=]\s*[\"']?production[\"']?\b", lower)
        )
    if family in {"auto_execute", "live_allocation"}:
        return bool(
            re.search(r"\bauto_apply\s*[:=]\s*true\b", lower)
            or re.search(r"\bauto_execute\s*[:=]\s*true\b", lower)
            or re.search(r"\blive_allocation\s*[:=]\s*true\b", lower)
        )
    return False


def _matching_allowlist_rule(
    rel_path: str,
    family: str,
    policy: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    for rule in _records(policy.get("rules")):
        path_glob = _text(rule.get("path_glob"))
        families = {str(item) for item in _list_values(rule.get("term_families"))}
        if not path_glob or not fnmatch(rel_path, path_glob):
            continue
        if "*" not in families and family not in families:
            continue
        return rule
    return None


def _has_safe_context(text: str) -> bool:
    lower = text.lower()
    return any(marker in lower for marker in SAFE_CONTEXT_MARKERS)


def _has_paper_context(text: str) -> bool:
    lower = text.lower()
    return any(
        marker in lower
        for marker in (
            "paper",
            "paperbroker",
            "paper_broker",
            "paper trading",
            "paper_trading",
            "trading_mode: paper",
        )
    )


def _matched_pattern(lower_line: str, patterns: Sequence[str]) -> str:
    for pattern in patterns:
        if pattern in lower_line:
            return pattern
    return ""


def _context(lines: Sequence[str], line_number: int) -> str:
    start = max(0, line_number - 2)
    end = min(len(lines), line_number + 1)
    return "\n".join(lines[start:end])


def _safe_context_excerpt(context: str) -> str:
    redacted = SECRET_LITERAL_RE.sub(lambda match: f"{match.group('key')}=<redacted>", context)
    return " ".join(redacted.split())[:360]


def _is_placeholder_secret(value: str) -> bool:
    lower = value.lower()
    return any(marker in lower for marker in PLACEHOLDER_MARKERS)


def _is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_SUFFIXES


def _should_skip_path(path: Path, project_root: Path) -> bool:
    try:
        rel_parts = path.relative_to(project_root).parts
    except ValueError:
        rel_parts = path.parts
    return any(part in SKIP_DIR_NAMES for part in rel_parts)


def _relative_path(path: Path, project_root: Path) -> str:
    try:
        return path.relative_to(project_root).as_posix()
    except ValueError:
        return path.as_posix()


def _family_counts(findings: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {family: 0 for family in REQUIRED_TERM_FAMILIES}
    for finding in findings:
        family = _text(finding.get("term_family"))
        if family in counts:
            counts[family] += 1
    return counts


def _term_family_ids(raw: Any) -> list[str]:
    ids: list[str] = []
    for item in _records(raw):
        ids.append(_text(item.get("term_family")))
    return ids


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_local_text_files_only",
        "does_not_modify_source": True,
        "does_not_refresh_data": True,
        "does_not_run_upstream_commands": True,
        "does_not_call_broker": True,
        "does_not_generate_order_ticket": True,
        "does_not_modify_production": True,
        "production_effect": PRODUCTION_EFFECT,
    }


def _reader_brief(
    status: str,
    summary: Mapping[str, Any],
    blocking: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    return {
        "summary": (
            f"Production boundary static scan status={status}; "
            f"blocking={_int(summary.get('blocking_finding_count'))}; "
            f"warnings={_int(summary.get('warning_finding_count'))}."
        ),
        "key_result": status,
        "blocking_issues": _issue_summary(blocking),
        "warnings": _issue_summary(warnings),
        "safety_boundary": (
            "只读扫描 source/config/docs；不修改命中位置、不运行上游、不接 broker/order，"
            "production_effect=none。"
        ),
        "next_action": _next_action(status),
    }


def _issue_summary(issues: Sequence[Mapping[str, Any]]) -> str:
    if not issues:
        return "none"
    first = issues[0]
    more = len(issues) - 1
    suffix = "" if more <= 0 else f"; +{more} more"
    return (
        f"{_text(first.get('term_family'), 'unknown')} at "
        f"{_text(first.get('path'), 'unknown')}:{_text(first.get('line_number'), '0')}"
        f"{suffix}"
    )


def _next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "fix_or_block_production_boundary_findings_before_promotion"
    if status == WARN_STATUS:
        return "review_warning_findings_and_allowlist_policy"
    return "continue_governance_chain"


def _validation_next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "fix_static_scan_blockers_before_shadow_or_promotion"
    if status == WARN_STATUS:
        return "review_static_scan_warnings"
    return "continue_governance_chain"


def _static_scan_input(status: str) -> str:
    if status == FAIL_STATUS:
        return "BLOCKS_PROMOTION_AND_SHADOW_CONTINUATION_REVIEW"
    if status == WARN_STATUS:
        return "AVAILABLE_WITH_WARNINGS"
    return "AVAILABLE"


def _recommended_action(severity: str, family: str) -> str:
    if severity == FAIL_STATUS:
        return f"remove_or_gate_{family}_production_boundary_signal"
    return f"review_{family}_documentation_or_allowlist"


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": "PASS" if passed else "FAIL",
            "severity": severity,
            "message": message,
            "recommended_action": "" if passed else recommended_action,
        }
    )
    if not passed:
        issues.append(
            {
                "issue_id": check_id,
                "severity": severity,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _dedupe_issues(issues: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in issues:
        key = (
            _text(item.get("issue_id")),
            _text(item.get("severity")),
            _text(item.get("message")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(item))
    return deduped


def _records(raw: Any) -> list[Mapping[str, Any]]:
    if isinstance(raw, Mapping):
        return [raw]
    if isinstance(raw, Sequence) and not isinstance(raw, str | bytes):
        return [item for item in raw if isinstance(item, Mapping)]
    return []


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def _list_values(raw: Any) -> list[Any]:
    if isinstance(raw, list | tuple):
        return list(raw)
    if raw is None:
        return []
    return [raw]


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    dated_name = re.compile(rf"^{re.escape(prefix)}\d{{4}}-\d{{2}}-\d{{2}}{re.escape(suffix)}$")
    candidates = sorted(
        path for path in output_dir.glob(f"{prefix}*{suffix}") if dated_name.match(path.name)
    )
    return candidates[-1] if candidates else None


def _markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
