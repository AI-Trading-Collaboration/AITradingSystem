from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any

import click
import typer
from typer.main import get_command, get_command_name

from ai_trading_system.yaml_loader import safe_load_yaml_path

CLI_CONTRACT_SCHEMA_VERSION = "arch_004g2_cli_contract.v1"


class CliContractError(RuntimeError):
    """Raised when a CLI contract cannot be frozen or no longer matches its baseline."""


def build_cli_contract(
    app: typer.Typer,
    *,
    source_path: Path,
    project_root: Path,
) -> dict[str, object]:
    """Build a deterministic, callback-location-independent Click/Typer contract."""
    root = get_command(app)
    duplicate_paths, registered_leaf_count = _registered_duplicate_paths(app)
    contracts = _walk_click_tree(root)
    leaf_count = sum(row["kind"] == "command" for row in contracts)
    group_count = sum(row["kind"] == "group" for row in contracts)
    paths = [str(row["path"]) for row in contracts]
    if duplicate_paths or len(paths) != len(set(paths)):
        raise CliContractError(
            "CLI_CONTRACT_DUPLICATE_PATH: "
            + ", ".join(sorted(set(duplicate_paths + _duplicates(paths))))
        )
    if registered_leaf_count != leaf_count:
        raise CliContractError(
            "CLI_CONTRACT_REGISTRATION_LOSS: "
            f"registered={registered_leaf_count}, resolved={leaf_count}"
        )
    node_summaries = [
        {
            "path": row["path"],
            "kind": row["kind"],
            "parameter_count": len(row["parameters"]),
            "contract_sha256": _sha256_json(row),
        }
        for row in contracts
    ]
    resolved_source = source_path.resolve()
    try:
        display_source = resolved_source.relative_to(project_root.resolve()).as_posix()
    except ValueError:
        display_source = resolved_source.as_posix()
    return {
        "schema_version": CLI_CONTRACT_SCHEMA_VERSION,
        "source": {
            "path": display_source,
            "sha256": hashlib.sha256(resolved_source.read_bytes()).hexdigest(),
        },
        "counts": {
            "root_command_count": len(root.commands) if isinstance(root, click.Group) else 0,
            "group_count": group_count,
            "leaf_command_count": leaf_count,
            "registered_leaf_count": registered_leaf_count,
            "unique_path_count": len(paths),
            "duplicate_path_count": 0,
        },
        "tree_sha256": _sha256_json(contracts),
        "nodes": node_summaries,
        "production_effect": "none",
    }


def assert_frozen_cli_contract(
    actual: dict[str, object],
    *,
    baseline_path: Path,
) -> None:
    frozen = safe_load_yaml_path(baseline_path)
    if not isinstance(frozen, dict):
        raise CliContractError(f"CLI_CONTRACT_BASELINE_INVALID: {baseline_path}")
    if actual != frozen:
        raise CliContractError(f"CLI_CONTRACT_BASELINE_DRIFT: {baseline_path}")


def _walk_click_tree(root: click.Command) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []

    def visit(command: click.Command, path: tuple[str, ...]) -> None:
        command_path = " ".join(path) if path else "<root>"
        is_group = isinstance(command, click.Group)
        row: dict[str, object] = {
            "path": command_path,
            "kind": "group" if is_group else "command",
            "name": command.name,
            "help": command.help,
            "short_help": command.short_help,
            "epilog": command.epilog,
            "hidden": bool(command.hidden),
            "deprecated": bool(command.deprecated),
            "parameters": [_parameter_contract(param) for param in command.params],
        }
        if is_group:
            group = command
            row.update(
                invoke_without_command=bool(group.invoke_without_command),
                no_args_is_help=bool(group.no_args_is_help),
                chain=bool(group.chain),
                child_names=sorted(group.commands),
            )
        rows.append(row)
        if is_group:
            for name, child in sorted(command.commands.items()):
                visit(child, (*path, name))

    visit(root, ())
    return rows


def _parameter_contract(param: click.Parameter) -> dict[str, object]:
    row: dict[str, object] = {
        "kind": "option" if isinstance(param, click.Option) else "argument",
        "name": param.name,
        "required": bool(param.required),
        "nargs": param.nargs,
        "multiple": bool(param.multiple),
        "default": _normalize_value(param.default),
        "type": _type_contract(param.type),
        "metavar": param.metavar,
        "expose_value": bool(param.expose_value),
        "is_eager": bool(param.is_eager),
        "envvar": _normalize_value(param.envvar),
        "callback_present": param.callback is not None,
    }
    if isinstance(param, click.Option):
        row.update(
            opts=list(param.opts),
            secondary_opts=list(param.secondary_opts),
            prompt=_normalize_value(param.prompt),
            confirmation_prompt=bool(param.confirmation_prompt),
            hide_input=bool(param.hide_input),
            is_flag=bool(param.is_flag),
            flag_value=_normalize_value(param.flag_value),
            count=bool(param.count),
            allow_from_autoenv=bool(param.allow_from_autoenv),
            help=param.help,
            hidden=bool(param.hidden),
            show_default=_normalize_value(param.show_default),
            show_choices=bool(param.show_choices),
            show_envvar=bool(param.show_envvar),
        )
    return row


def _type_contract(param_type: click.ParamType) -> dict[str, object]:
    row: dict[str, object] = {
        "class": f"{type(param_type).__module__}.{type(param_type).__qualname__}",
        "name": param_type.name,
    }
    for attribute in (
        "choices",
        "case_sensitive",
        "min",
        "max",
        "clamp",
        "formats",
        "exists",
        "file_okay",
        "dir_okay",
        "writable",
        "readable",
        "resolve_path",
        "allow_dash",
    ):
        if hasattr(param_type, attribute):
            row[attribute] = _normalize_value(getattr(param_type, attribute))
    path_type = getattr(param_type, "path_type", None)
    if path_type is not None:
        row["path_type"] = f"{path_type.__module__}.{path_type.__qualname__}"
    return row


def _registered_duplicate_paths(app: typer.Typer) -> tuple[list[str], int]:
    duplicates: list[str] = []
    leaf_count = 0

    def visit(current: typer.Typer, path: tuple[str, ...]) -> None:
        nonlocal leaf_count
        names: list[str] = []
        for command in current.registered_commands:
            callback = command.callback
            fallback = get_command_name(callback.__name__) if callback is not None else ""
            name = command.name or fallback
            names.append(name)
            leaf_count += 1
        for group in current.registered_groups:
            name = group.name or ""
            names.append(name)
        for name, count in Counter(names).items():
            if count > 1:
                duplicates.append(" ".join((*path, name)))
        for group in current.registered_groups:
            visit(group.typer_instance, (*path, group.name or ""))

    visit(app, ())
    return duplicates, leaf_count


def _normalize_value(value: Any) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Path):
        return value.as_posix()
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_normalize_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_value(value[key]) for key in sorted(value, key=str)}
    raise CliContractError(
        "CLI_CONTRACT_NON_DETERMINISTIC_VALUE: "
        f"{type(value).__module__}.{type(value).__qualname__}"
    )


def _sha256_json(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _duplicates(values: list[str]) -> list[str]:
    return [value for value, count in Counter(values).items() if count > 1]


__all__ = [
    "CLI_CONTRACT_SCHEMA_VERSION",
    "CliContractError",
    "assert_frozen_cli_contract",
    "build_cli_contract",
]
