from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_HEURISTIC_GOVERNANCE_CONFIG_PATH = PROJECT_ROOT / "config" / "heuristic_governance.yaml"
REPORT_TYPE = "heuristic_governance_audit"
TASK_ID = "GOV-004"
PRODUCTION_EFFECT = "none"

_ALLOWED_DEFAULT_NUMERIC_LITERALS = frozenset({0.0, 1.0, 100.0})
_COMPARE_OPS = (
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
)


@dataclass(frozen=True)
class NumericLiteralFinding:
    path: str
    line: int
    expression: str
    numeric_literals: tuple[str, ...]

    @property
    def key(self) -> str:
        return _finding_key(self.path, self.expression)


def default_heuristic_governance_report_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"{REPORT_TYPE}_{as_of.isoformat()}.md"


def default_heuristic_governance_json_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"{REPORT_TYPE}_{as_of.isoformat()}.json"


def build_heuristic_governance_payload(
    *,
    as_of: date,
    config_path: Path = DEFAULT_HEURISTIC_GOVERNANCE_CONFIG_PATH,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    config = _load_mapping(config_path)
    policy_metadata = _mapping(config.get("policy_metadata"), "policy_metadata")
    source_paths = _source_paths(config, project_root)
    baseline_entries = _baseline_entries(config)
    baseline_by_key = {entry["key"]: entry for entry in baseline_entries}
    allowed_numeric_literals = _allowed_numeric_literals(config)

    findings: list[NumericLiteralFinding] = []
    scan_errors: list[dict[str, Any]] = []
    for source_file in _source_files(source_paths):
        try:
            findings.extend(
                _scan_numeric_compare_findings(
                    source_file,
                    project_root,
                    allowed_numeric_literals,
                )
            )
        except SyntaxError as exc:
            scan_errors.append(
                {
                    "path": _relative_path(source_file, project_root),
                    "code": "python_parse_error",
                    "message": str(exc),
                }
            )

    found_keys = {finding.key for finding in findings}
    numeric_payloads = [
        _numeric_finding_payload(finding, baseline_by_key.get(finding.key)) for finding in findings
    ]
    unregistered_numeric = [
        item for item in numeric_payloads if item["registration_status"] == "UNREGISTERED"
    ]
    stale_baseline = [entry for entry in baseline_entries if str(entry["key"]) not in found_keys]
    policy_checks = _policy_metadata_checks(config, project_root)
    failed_policy_checks = [check for check in policy_checks if check["status"] == "FAIL"]

    error_count = len(unregistered_numeric) + len(failed_policy_checks) + len(scan_errors)
    warning_count = len(stale_baseline)
    status = "FAIL" if error_count else ("PASS_WITH_WARNINGS" if warning_count else "PASS")

    return {
        "schema_version": 1,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "as_of": as_of.isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "policy_version": str(policy_metadata.get("version", "")),
        "policy_metadata": dict(policy_metadata),
        "config_path": _relative_path(Path(config_path), project_root),
        "audit_scope": {
            "source_paths": [_relative_path(path, project_root) for path in source_paths],
        },
        "summary": {
            "source_path_count": len(source_paths),
            "numeric_literal_finding_count": len(numeric_payloads),
            "registered_numeric_literal_count": len(numeric_payloads) - len(unregistered_numeric),
            "unregistered_numeric_literal_count": len(unregistered_numeric),
            "baseline_entry_count": len(baseline_entries),
            "stale_baseline_entry_count": len(stale_baseline),
            "policy_metadata_check_count": len(policy_checks),
            "failed_policy_metadata_check_count": len(failed_policy_checks),
            "scan_error_count": len(scan_errors),
            "error_count": error_count,
            "warning_count": warning_count,
        },
        "numeric_literal_findings": numeric_payloads,
        "unregistered_numeric_literal_findings": unregistered_numeric,
        "stale_baseline_entries": stale_baseline,
        "policy_metadata_checks": policy_checks,
        "scan_errors": scan_errors,
    }


def write_heuristic_governance_report(payload: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_heuristic_governance_markdown(payload), encoding="utf-8")
    return output_path


def write_heuristic_governance_json(payload: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return output_path


def render_heuristic_governance_markdown(payload: dict[str, Any]) -> str:
    summary = payload["summary"]
    lines = [
        f"# Heuristic Governance Audit - {payload['as_of']}",
        "",
        f"- 状态：{payload['status']}",
        f"- production_effect={payload['production_effect']}",
        f"- task_id={payload['task_id']}",
        f"- policy_version={payload['policy_version']}",
        f"- config_path=`{payload['config_path']}`",
        "",
        "## 摘要",
        "",
        "|指标|数量|",
        "|---|---:|",
        f"|扫描路径|{summary['source_path_count']}|",
        f"|numeric literal 命中|{summary['numeric_literal_finding_count']}|",
        f"|已登记命中|{summary['registered_numeric_literal_count']}|",
        f"|未登记命中|{summary['unregistered_numeric_literal_count']}|",
        f"|baseline 条目|{summary['baseline_entry_count']}|",
        f"|过期 baseline 条目|{summary['stale_baseline_entry_count']}|",
        f"|policy metadata 检查|{summary['policy_metadata_check_count']}|",
        f"|policy metadata 失败|{summary['failed_policy_metadata_check_count']}|",
        f"|扫描错误|{summary['scan_error_count']}|",
        f"|错误|{summary['error_count']}|",
        f"|警告|{summary['warning_count']}|",
        "",
        "## 未登记 Numeric Literal",
        "",
    ]
    unregistered = payload["unregistered_numeric_literal_findings"]
    if not unregistered:
        lines.append("未发现未登记的投资解释 numeric literal。")
    else:
        lines.extend(["|文件|行|表达式|数字|", "|---|---:|---|---|"])
        for item in unregistered:
            lines.append(
                f"|`{item['path']}`|{item['line']}|`{item['expression']}`|"
                f"`{', '.join(item['numeric_literals'])}`|"
            )

    lines.extend(
        [
            "",
            "## Policy Metadata 检查",
            "",
            "|配置|section|状态|缺失字段|",
            "|---|---|---|---|",
        ]
    )
    for check in payload["policy_metadata_checks"]:
        missing = ", ".join(check["missing_fields"]) if check["missing_fields"] else "-"
        lines.append(
            f"|`{check['path']}`|`{check['section'] or '<top-level>'}`|"
            f"{check['status']}|{missing}|"
        )

    stale = payload["stale_baseline_entries"]
    lines.extend(["", "## 过期 Baseline", ""])
    if not stale:
        lines.append("未发现过期 baseline。")
    else:
        lines.extend(["|文件|表达式|类别|", "|---|---|---|"])
        for item in stale:
            lines.append(f"|`{item['path']}`|`{item['expression']}`|{item['category']}|")

    lines.extend(["", "## 已登记 Numeric Literal", ""])
    registered = [
        item
        for item in payload["numeric_literal_findings"]
        if item["registration_status"] == "REGISTERED"
    ]
    if not registered:
        lines.append("未发现已登记 numeric literal。")
    else:
        lines.extend(["|文件|行|表达式|类别|rationale|", "|---|---:|---|---|---|"])
        for item in registered:
            lines.append(
                f"|`{item['path']}`|{item['line']}|`{item['expression']}`|"
                f"{item['category']}|{item['rationale']}|"
            )

    return "\n".join(lines).rstrip() + "\n"


def _scan_numeric_compare_findings(
    path: Path,
    project_root: Path,
    allowed_numeric_literals: frozenset[float],
) -> list[NumericLiteralFinding]:
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text, filename=str(path))
    visitor = _NumericCompareVisitor(
        text,
        _relative_path(path, project_root),
        allowed_numeric_literals,
    )
    visitor.visit(tree)
    return visitor.findings


class _NumericCompareVisitor(ast.NodeVisitor):
    def __init__(
        self,
        text: str,
        relative_path: str,
        allowed_numeric_literals: frozenset[float],
    ) -> None:
        self._text = text
        self._relative_path = relative_path
        self._allowed_numeric_literals = allowed_numeric_literals
        self.findings: list[NumericLiteralFinding] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._scan_function_defaults(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._scan_function_defaults(node)
        self.generic_visit(node)

    def visit_Compare(self, node: ast.Compare) -> None:
        if any(isinstance(operator, _COMPARE_OPS) for operator in node.ops):
            numeric_literals = _non_allowed_numeric_literals(
                node,
                self._text,
                self._allowed_numeric_literals,
            )
            if numeric_literals:
                expression = _normalize_expression(
                    ast.get_source_segment(self._text, node) or ast.unparse(node)
                )
                self.findings.append(
                    NumericLiteralFinding(
                        path=self._relative_path,
                        line=node.lineno,
                        expression=expression,
                        numeric_literals=tuple(numeric_literals),
                    )
                )
        self.generic_visit(node)

    def _scan_function_defaults(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        args = [*node.args.posonlyargs, *node.args.args]
        defaults = [None] * (len(args) - len(node.args.defaults)) + list(node.args.defaults)
        default_pairs = [
            (arg.arg, default)
            for arg, default in zip(args, defaults, strict=True)
            if default is not None
        ]
        default_pairs.extend(
            (arg.arg, default)
            for arg, default in zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True)
            if default is not None
        )
        for arg_name, default in default_pairs:
            numeric_literals = _non_allowed_numeric_literals(
                default,
                self._text,
                self._allowed_numeric_literals,
            )
            if not numeric_literals:
                continue
            literal = ast.get_source_segment(self._text, default) or ast.unparse(default)
            expression = _normalize_expression(f"{node.name} default {arg_name}={literal}")
            self.findings.append(
                NumericLiteralFinding(
                    path=self._relative_path,
                    line=default.lineno,
                    expression=expression,
                    numeric_literals=tuple(numeric_literals),
                )
            )


def _non_allowed_numeric_literals(
    node: ast.AST,
    text: str,
    allowed_numeric_literals: frozenset[float],
) -> list[str]:
    literals: list[str] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Constant):
            continue
        if isinstance(child.value, bool) or not isinstance(child.value, (int, float)):
            continue
        numeric_value = float(child.value)
        if numeric_value in allowed_numeric_literals:
            continue
        literal = ast.get_source_segment(text, child) or repr(child.value)
        normalized_literal = _normalize_expression(literal)
        if normalized_literal not in literals:
            literals.append(normalized_literal)
    return literals


def _allowed_numeric_literals(config: dict[str, Any]) -> frozenset[float]:
    allowed = set(_ALLOWED_DEFAULT_NUMERIC_LITERALS)
    raw_allowed = config.get("allowed_numeric_literals", {})
    if raw_allowed is None:
        return frozenset(allowed)
    allowed_mapping = _mapping(raw_allowed, "allowed_numeric_literals")
    for raw_value in allowed_mapping.get("scale_bounds", []):
        if isinstance(raw_value, bool) or not isinstance(raw_value, (int, float)):
            raise ValueError("allowed_numeric_literals.scale_bounds must contain numbers")
        allowed.add(float(raw_value))
    return frozenset(allowed)


def _numeric_finding_payload(
    finding: NumericLiteralFinding,
    baseline_entry: dict[str, Any] | None,
) -> dict[str, Any]:
    registered = baseline_entry is not None
    return {
        "key": finding.key,
        "path": finding.path,
        "line": finding.line,
        "expression": finding.expression,
        "numeric_literals": list(finding.numeric_literals),
        "registration_status": "REGISTERED" if registered else "UNREGISTERED",
        "category": baseline_entry.get("category", "") if baseline_entry else "",
        "rationale": baseline_entry.get("rationale", "") if baseline_entry else "",
        "validation": baseline_entry.get("validation", "") if baseline_entry else "",
    }


def _source_paths(config: dict[str, Any], project_root: Path) -> list[Path]:
    scope = _mapping(config.get("audit_scope"), "audit_scope")
    raw_paths = scope.get("source_paths")
    if not isinstance(raw_paths, list) or not raw_paths:
        raise ValueError("heuristic governance audit_scope.source_paths must be a non-empty list")
    paths: list[Path] = []
    for raw_path in raw_paths:
        if not isinstance(raw_path, str) or not raw_path.strip():
            raise ValueError("heuristic governance source path must be a non-empty string")
        path = Path(raw_path)
        paths.append(path if path.is_absolute() else project_root / path)
    return paths


def _source_files(source_paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for source_path in source_paths:
        if not source_path.exists():
            raise FileNotFoundError(f"heuristic governance source path not found: {source_path}")
        if source_path.is_file():
            if source_path.suffix == ".py":
                files.append(source_path)
            continue
        files.extend(
            path for path in sorted(source_path.rglob("*.py")) if "__pycache__" not in path.parts
        )
    return sorted(set(files))


def _baseline_entries(config: dict[str, Any]) -> list[dict[str, Any]]:
    raw_entries = config.get("numeric_literal_baseline", [])
    if not isinstance(raw_entries, list):
        raise ValueError("heuristic governance numeric_literal_baseline must be a list")
    entries: list[dict[str, Any]] = []
    for raw_entry in raw_entries:
        entry = _mapping(raw_entry, "numeric_literal_baseline entry")
        path = _required_string(entry, "path")
        expression = _normalize_expression(_required_string(entry, "expression"))
        category = _required_string(entry, "category")
        rationale = _required_string(entry, "rationale")
        validation = _required_string(entry, "validation")
        entries.append(
            {
                "key": _finding_key(path, expression),
                "path": path,
                "line_hint": entry.get("line_hint"),
                "expression": expression,
                "category": category,
                "rationale": rationale,
                "validation": validation,
            }
        )
    return entries


def _policy_metadata_checks(config: dict[str, Any], project_root: Path) -> list[dict[str, Any]]:
    raw_checks = config.get("required_policy_metadata", [])
    if not isinstance(raw_checks, list):
        raise ValueError("heuristic governance required_policy_metadata must be a list")
    checks: list[dict[str, Any]] = []
    for raw_check in raw_checks:
        check = _mapping(raw_check, "required_policy_metadata entry")
        relative_config_path = _required_string(check, "path")
        section = str(check.get("section", "") or "")
        required_fields = check.get("required_fields")
        if not isinstance(required_fields, list) or not required_fields:
            raise ValueError("required_policy_metadata.required_fields must be a non-empty list")
        path = project_root / relative_config_path
        missing_fields: list[str] = []
        if not path.exists():
            missing_fields = [str(field) for field in required_fields]
        else:
            payload = _load_mapping(path)
            section_payload = _section_mapping(payload, section)
            for field in required_fields:
                field_name = str(field)
                if not _has_reviewable_value(section_payload.get(field_name)):
                    missing_fields.append(field_name)
        checks.append(
            {
                "path": relative_config_path,
                "section": section,
                "required_fields": [str(field) for field in required_fields],
                "missing_fields": missing_fields,
                "status": "FAIL" if missing_fields else "PASS",
            }
        )
    return checks


def _section_mapping(payload: dict[str, Any], section: str) -> dict[str, Any]:
    if not section:
        return payload
    current: Any = payload
    for part in section.split("."):
        if not isinstance(current, dict):
            return {}
        current = current.get(part)
    return current if isinstance(current, dict) else {}


def _has_reviewable_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def _load_mapping(path: Path) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    return _mapping(payload, str(path))


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping")
    return value


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"missing required non-empty string: {key}")
    return value.strip()


def _finding_key(path: str, expression: str) -> str:
    return f"{path}:{_normalize_expression(expression)}"


def _normalize_expression(expression: str) -> str:
    return " ".join(expression.strip().split())


def _relative_path(path: Path, project_root: Path) -> str:
    resolved_root = project_root.resolve()
    resolved_path = path.resolve()
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError:
        return resolved_path.as_posix()
