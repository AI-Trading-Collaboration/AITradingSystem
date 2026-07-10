from __future__ import annotations

import ast
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system.platform.artifacts import write_yaml_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path


@dataclass(frozen=True)
class ArchitectureViolation:
    rule_id: str
    path: str
    line: int
    owner: str
    message: str
    remediation: str

    def to_dict(self) -> dict[str, object]:
        return {
            "rule_id": self.rule_id,
            "path": self.path,
            "line": self.line,
            "owner": self.owner,
            "message": self.message,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class ArchitectureGateReport:
    policy_id: str
    status: str
    scanned_python_files: int
    current_direct_writer_calls: int
    baseline_direct_writer_calls: int
    violations: tuple[ArchitectureViolation, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "status": self.status,
            "scanned_python_files": self.scanned_python_files,
            "current_direct_writer_calls": self.current_direct_writer_calls,
            "baseline_direct_writer_calls": self.baseline_direct_writer_calls,
            "violation_count": len(self.violations),
            "violations": [item.to_dict() for item in self.violations],
        }


@dataclass(frozen=True)
class _DirectWriterFinding:
    path: str
    scope: str
    kind: str
    line: int

    @property
    def key(self) -> str:
        return f"{self.path}|{self.scope}|{self.kind}"


def validate_architecture_dependencies(
    *,
    policy_path: Path,
    baseline_path: Path,
    source_root: Path,
) -> ArchitectureGateReport:
    policy = _load_mapping(policy_path, "architecture policy")
    baseline = _load_mapping(baseline_path, "direct writer baseline")
    project_root = source_root.parent.parent
    python_paths = tuple(sorted(source_root.rglob("*.py")))
    violations: list[ArchitectureViolation] = []
    findings: list[_DirectWriterFinding] = []
    parse_trees: dict[Path, ast.AST] = {}
    for path in python_paths:
        try:
            parse_trees[path] = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError) as exc:
            violations.append(
                ArchitectureViolation(
                    rule_id="PYTHON_SOURCE_PARSE_REQUIRED",
                    path=_portable_path(path, project_root),
                    line=getattr(exc, "lineno", 0) or 0,
                    owner="architecture_governance",
                    message=str(exc),
                    remediation="修复源码解析错误后重新运行 architecture gate。",
                )
            )

    layer_rules = policy.get("layer_rules")
    if not isinstance(layer_rules, list):
        raise ValueError("architecture policy layer_rules must be a list")
    for path, tree in parse_trees.items():
        portable = _portable_path(path, project_root)
        violations.extend(_layer_violations(path=portable, tree=tree, rules=layer_rules))
        if portable != str(policy.get("canonical_writer_path") or ""):
            findings.extend(_direct_writer_findings(portable, tree))

    baseline_counts = _baseline_counts(baseline)
    current_counts = Counter(item.key for item in findings)
    finding_by_key = {item.key: item for item in findings}
    direct_policy = _mapping(policy.get("direct_writer_ratchet"), "direct_writer_ratchet")
    for key, count in sorted(current_counts.items()):
        allowed = baseline_counts.get(key, 0)
        if count <= allowed:
            continue
        finding = finding_by_key[key]
        violations.append(
            ArchitectureViolation(
                rule_id="NEW_DIRECT_ARTIFACT_WRITER_FORBIDDEN",
                path=finding.path,
                line=finding.line,
                owner=str(direct_policy.get("owner") or "platform_io"),
                message=f"{finding.kind} count={count} exceeds frozen baseline={allowed}",
                remediation=str(
                    direct_policy.get("remediation")
                    or "改用 ai_trading_system.platform.artifacts 公共原子 writer。"
                ),
            )
        )
    ordered = tuple(sorted(violations, key=lambda item: (item.path, item.line, item.rule_id)))
    return ArchitectureGateReport(
        policy_id=str(policy.get("policy_id") or "unknown"),
        status="PASS" if not ordered else "FAIL",
        scanned_python_files=len(python_paths),
        current_direct_writer_calls=sum(current_counts.values()),
        baseline_direct_writer_calls=sum(baseline_counts.values()),
        violations=ordered,
    )


def capture_direct_writer_baseline(
    *,
    source_root: Path,
    output_path: Path,
    canonical_writer_path: str,
    source_commit: str,
) -> Path:
    project_root = source_root.parent.parent
    findings: list[_DirectWriterFinding] = []
    scanned = 0
    for source_path in sorted(source_root.rglob("*.py")):
        portable = _portable_path(source_path, project_root)
        if portable == canonical_writer_path:
            continue
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        scanned += 1
        findings.extend(_direct_writer_findings(portable, tree))
    counts = Counter(item.key for item in findings)
    rows = []
    for key, count in sorted(counts.items()):
        recorded_path, scope, kind = key.split("|", 2)
        rows.append({"path": recorded_path, "scope": scope, "kind": kind, "count": count})
    payload = {
        "schema_version": "arch_004c_direct_writer_baseline.v1",
        "status": "FROZEN_ARCH_004C_C2",
        "source_commit": source_commit,
        "scanned_python_files": scanned,
        "direct_writer_call_count": sum(counts.values()),
        "entries": rows,
    }
    write_yaml_atomic(output_path, payload, sort_keys=False)
    return output_path


def _layer_violations(
    *, path: str, tree: ast.AST, rules: list[object]
) -> list[ArchitectureViolation]:
    violations: list[ArchitectureViolation] = []
    for raw_rule in rules:
        rule = _mapping(raw_rule, "layer rule")
        path_prefix = str(rule.get("path_prefix") or "")
        if not path.startswith(path_prefix):
            continue
        forbidden = tuple(str(item) for item in _list(rule.get("forbidden_import_prefixes")))
        for node in ast.walk(tree):
            module = _imported_module(node)
            if module is None or not any(
                module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden
            ):
                continue
            violations.append(
                ArchitectureViolation(
                    rule_id=str(rule.get("rule_id") or "LAYER_IMPORT_FORBIDDEN"),
                    path=path,
                    line=getattr(node, "lineno", 0),
                    owner=str(rule.get("owner") or "architecture_governance"),
                    message=f"forbidden import {module}",
                    remediation=str(
                        rule.get("remediation")
                        or "通过 contracts/platform 正向依赖或 time-bounded legacy adapter 接入。"
                    ),
                )
            )
    return violations


def _imported_module(node: ast.AST) -> str | None:
    if isinstance(node, ast.ImportFrom):
        return node.module or ""
    if isinstance(node, ast.Import) and node.names:
        return node.names[0].name
    return None


def _direct_writer_findings(path: str, tree: ast.AST) -> list[_DirectWriterFinding]:
    findings: list[_DirectWriterFinding] = []

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.scope = ["<module>"]

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.scope.append(node.name)
            self.generic_visit(node)
            self.scope.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self.scope.append(node.name)
            self.generic_visit(node)
            self.scope.pop()

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            self.scope.append(node.name)
            self.generic_visit(node)
            self.scope.pop()

        def visit_Call(self, node: ast.Call) -> None:
            kind = _direct_writer_kind(node.func)
            if kind is not None:
                findings.append(
                    _DirectWriterFinding(
                        path=path,
                        scope=".".join(self.scope),
                        kind=kind,
                        line=node.lineno,
                    )
                )
            self.generic_visit(node)

    Visitor().visit(tree)
    return findings


def _direct_writer_kind(function: ast.expr) -> str | None:
    if not isinstance(function, ast.Attribute):
        return None
    if function.attr == "write_text":
        return "PATH_WRITE_TEXT"
    if function.attr == "write_bytes":
        return "PATH_WRITE_BYTES"
    if (
        function.attr == "dump"
        and isinstance(function.value, ast.Name)
        and function.value.id == "json"
    ):
        return "JSON_DUMP"
    return None


def _baseline_counts(payload: Mapping[str, object]) -> dict[str, int]:
    rows = payload.get("entries")
    if not isinstance(rows, list):
        raise ValueError("direct writer baseline entries must be a list")
    counts: dict[str, int] = {}
    for raw in rows:
        row = _mapping(raw, "direct writer baseline entry")
        key = "|".join(
            (str(row.get("path") or ""), str(row.get("scope") or ""), str(row.get("kind") or ""))
        )
        count = row.get("count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError(f"invalid direct writer baseline count: {row!r}")
        counts[key] = count
    return counts


def _portable_path(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def _load_mapping(path: Path, label: str) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{label} must be a mapping: {path}")
    return dict(raw)


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be a mapping")
    return value


def _list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("architecture policy list field must be a list")
    return value
