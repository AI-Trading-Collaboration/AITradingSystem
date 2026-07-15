from __future__ import annotations

import ast
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system.yaml_loader import safe_load_yaml_path

CALLBACK_MIGRATION_SCHEMA_VERSION = "arch_004g2_callback_migration_matrix.v1"


class CallbackMigrationError(RuntimeError):
    """Raised when the G2.4 callback migration matrix cannot be reconciled."""


@dataclass(frozen=True)
class CallbackDeclaration:
    app_name: str
    command_name: str
    function_name: str
    source_path: str
    line_number: int

    @property
    def identity(self) -> tuple[str, str]:
        return self.app_name, self.command_name


def scan_callback_source(source: str, *, source_path: str) -> tuple[CallbackDeclaration, ...]:
    tree = ast.parse(source, filename=source_path)
    rows: list[CallbackDeclaration] = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            declaration = _callback_declaration(
                decorator,
                function_name=node.name,
                source_path=source_path,
                line_number=node.lineno,
            )
            if declaration is not None:
                rows.append(declaration)
    _assert_unique(rows, scope=source_path)
    return tuple(rows)


def scan_current_callbacks(*, project_root: Path) -> tuple[CallbackDeclaration, ...]:
    root_path = project_root / "src/ai_trading_system/cli_commands/etf_portfolio.py"
    interface_root = project_root / "src/ai_trading_system/interfaces/cli/etf_portfolio"
    paths = (root_path, *sorted(interface_root.glob("*.py")))
    rows: list[CallbackDeclaration] = []
    for path in paths:
        relative = path.relative_to(project_root).as_posix()
        rows.extend(scan_callback_source(path.read_text(encoding="utf-8"), source_path=relative))
    _assert_unique(rows, scope="current ETF CLI callback owners")
    return tuple(rows)


def build_callback_migration_matrix(
    *,
    baseline_callbacks: tuple[CallbackDeclaration, ...],
    baseline_source_commit: str,
    baseline_source_path: str,
    baseline_source_sha256: str,
    project_root: Path,
) -> dict[str, object]:
    _assert_unique(baseline_callbacks, scope="G2.4 callback baseline")
    current_callbacks = scan_current_callbacks(project_root=project_root)
    baseline_by_identity = {row.identity: row for row in baseline_callbacks}
    current_by_identity = {row.identity: row for row in current_callbacks}
    missing = sorted(set(baseline_by_identity) - set(current_by_identity))
    if missing:
        raise CallbackMigrationError(
            "CALLBACK_MIGRATION_COMMAND_LOSS: "
            + ", ".join(f"{app}:{command}" for app, command in missing)
        )

    callbacks: list[dict[str, object]] = []
    status_counts: Counter[str] = Counter()
    pending_apps: Counter[str] = Counter()
    owner_counts: Counter[str] = Counter()
    legacy_root = "src/ai_trading_system/cli_commands/etf_portfolio.py"
    canonical_prefix = "src/ai_trading_system/interfaces/cli/etf_portfolio/"
    for identity in sorted(baseline_by_identity):
        baseline = baseline_by_identity[identity]
        current = current_by_identity[identity]
        if current.source_path == legacy_root:
            migration_status = "PENDING_LEGACY_ROOT"
            pending_apps[current.app_name] += 1
        elif current.source_path.startswith(canonical_prefix):
            migration_status = "MIGRATED_CANONICAL"
        else:
            raise CallbackMigrationError(
                "CALLBACK_MIGRATION_OWNER_INVALID: "
                f"{current.app_name}:{current.command_name} -> {current.source_path}"
            )
        status_counts[migration_status] += 1
        owner_counts[current.source_path] += 1
        callbacks.append(
            {
                "callback_id": _callback_id(*identity),
                "app_name": baseline.app_name,
                "command_name": baseline.command_name,
                "baseline_function": baseline.function_name,
                "baseline_line": baseline.line_number,
                "current_function": current.function_name,
                "current_owner": current.source_path,
                "migration_status": migration_status,
            }
        )

    baseline_identities = set(baseline_by_identity)
    pre_g2_4_callbacks = [
        row for row in current_callbacks if row.identity not in baseline_identities
    ]
    pending_count = status_counts["PENDING_LEGACY_ROOT"]
    migrated_count = status_counts["MIGRATED_CANONICAL"]
    summary = {
        "baseline_callback_count": len(baseline_callbacks),
        "migrated_callback_count": migrated_count,
        "pending_callback_count": pending_count,
        "unresolved_callback_count": 0,
        "duplicate_callback_count": 0,
        "pre_g2_4_canonical_callback_count": len(pre_g2_4_callbacks),
        "current_total_callback_count": len(current_callbacks),
        "phase_exit_ready": pending_count == 0,
        "phase_completion_status": (
            "COMPLETE" if pending_count == 0 else "IN_PROGRESS"
        ),
    }
    payload: dict[str, object] = {
        "schema_version": CALLBACK_MIGRATION_SCHEMA_VERSION,
        "status": "PASS",
        "source_phase": "ARCH-004G2.4",
        "baseline": {
            "commit": baseline_source_commit,
            "path": baseline_source_path,
            "sha256": baseline_source_sha256,
        },
        "summary": summary,
        "current_callback_set_sha256": _callback_set_sha256(current_callbacks),
        "pending_by_app": dict(sorted(pending_apps.items())),
        "current_owner_counts": dict(sorted(owner_counts.items())),
        "callbacks": callbacks,
        "production_effect": "none",
        "broker_action": "none",
    }
    payload["matrix_id"] = "arch_004g2_callback_matrix_" + _sha256_json(payload)[:20]
    return payload


def baseline_callbacks_from_matrix(path: Path) -> tuple[CallbackDeclaration, ...]:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise CallbackMigrationError(f"CALLBACK_MIGRATION_MATRIX_INVALID: {path}")
    rows = payload.get("callbacks")
    if not isinstance(rows, list):
        raise CallbackMigrationError("CALLBACK_MIGRATION_BASELINE_ROWS_MISSING")
    callbacks: list[CallbackDeclaration] = []
    for row in rows:
        if not isinstance(row, dict):
            raise CallbackMigrationError("CALLBACK_MIGRATION_BASELINE_ROW_INVALID")
        callbacks.append(
            CallbackDeclaration(
                app_name=_required_text(row, "app_name"),
                command_name=_required_text(row, "command_name"),
                function_name=_required_text(row, "baseline_function"),
                source_path="src/ai_trading_system/cli_commands/etf_portfolio.py",
                line_number=_required_positive_int(row, "baseline_line"),
            )
        )
    _assert_unique(callbacks, scope="tracked G2.4 callback baseline")
    return tuple(callbacks)


def assert_frozen_callback_migration_matrix(
    actual: dict[str, object], *, baseline_path: Path
) -> None:
    frozen = safe_load_yaml_path(baseline_path)
    if not isinstance(frozen, dict):
        raise CallbackMigrationError(f"CALLBACK_MIGRATION_MATRIX_INVALID: {baseline_path}")
    if actual != frozen:
        raise CallbackMigrationError(f"CALLBACK_MIGRATION_MATRIX_DRIFT: {baseline_path}")


def _callback_declaration(
    decorator: ast.expr,
    *,
    function_name: str,
    source_path: str,
    line_number: int,
) -> CallbackDeclaration | None:
    if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
        return None
    if decorator.func.attr != "command":
        return None
    app_name = _expression_name(decorator.func.value)
    if not app_name:
        raise CallbackMigrationError(
            f"CALLBACK_MIGRATION_APP_UNRESOLVED: {source_path}:{line_number}"
        )
    command_name: str | None = None
    if decorator.args:
        command_name = _constant_text(decorator.args[0])
    if command_name is None:
        for keyword in decorator.keywords:
            if keyword.arg == "name":
                command_name = _constant_text(keyword.value)
                break
    if command_name is None:
        command_name = function_name.replace("_", "-")
    if not command_name:
        raise CallbackMigrationError(
            f"CALLBACK_MIGRATION_COMMAND_NAME_EMPTY: {source_path}:{line_number}"
        )
    return CallbackDeclaration(
        app_name=app_name,
        command_name=command_name,
        function_name=function_name,
        source_path=source_path,
        line_number=line_number,
    )


def _expression_name(node: ast.expr) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _expression_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _constant_text(node: ast.expr) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _assert_unique(
    rows: list[CallbackDeclaration] | tuple[CallbackDeclaration, ...], *, scope: str
) -> None:
    counts = Counter(row.identity for row in rows)
    duplicates = sorted(identity for identity, count in counts.items() if count > 1)
    if duplicates:
        raise CallbackMigrationError(
            f"CALLBACK_MIGRATION_DUPLICATE: {scope}: "
            + ", ".join(f"{app}:{command}" for app, command in duplicates)
        )


def _callback_id(app_name: str, command_name: str) -> str:
    digest = hashlib.sha256(f"{app_name}\0{command_name}".encode()).hexdigest()
    return f"callback_{digest[:20]}"


def _callback_set_sha256(rows: tuple[CallbackDeclaration, ...]) -> str:
    normalized = [
        {
            "app_name": row.app_name,
            "command_name": row.command_name,
            "function_name": row.function_name,
            "source_path": row.source_path,
        }
        for row in sorted(rows, key=lambda item: (*item.identity, item.source_path))
    ]
    return _sha256_json(normalized)


def _sha256_json(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _required_text(payload: dict[str, Any], field: str) -> str:
    value = str(payload.get(field) or "").strip()
    if not value:
        raise CallbackMigrationError(f"CALLBACK_MIGRATION_FIELD_REQUIRED: {field}")
    return value


def _required_positive_int(payload: dict[str, Any], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise CallbackMigrationError(f"CALLBACK_MIGRATION_FIELD_INVALID: {field}")
    return value


__all__ = [
    "CALLBACK_MIGRATION_SCHEMA_VERSION",
    "CallbackDeclaration",
    "CallbackMigrationError",
    "assert_frozen_callback_migration_matrix",
    "baseline_callbacks_from_matrix",
    "build_callback_migration_matrix",
    "scan_callback_source",
    "scan_current_callbacks",
]
