from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from ai_trading_system.platform.artifacts import write_text_atomic, write_yaml_atomic

_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


class ScaffoldKind(StrEnum):
    MODULE = "module"
    EXPERIMENT = "experiment"
    REPORT = "report"


@dataclass(frozen=True)
class ScaffoldResult:
    kind: ScaffoldKind
    identifier: str
    paths: tuple[Path, ...]


def create_scaffold(
    *, project_root: Path, kind: ScaffoldKind, identifier: str, owner: str
) -> ScaffoldResult:
    if not _IDENTIFIER.fullmatch(identifier):
        raise ValueError("scaffold identifier must be snake_case and start with a letter")
    if not owner.strip():
        raise ValueError("scaffold owner is required")
    planned = _planned_paths(project_root, kind, identifier)
    existing = [path for path in planned if path.exists()]
    if existing:
        raise FileExistsError(f"scaffold target already exists: {existing[0]}")
    if kind is ScaffoldKind.MODULE:
        module_path, fragment_path = planned
        write_text_atomic(
            module_path,
            f'"""{identifier} capability package; generated scaffold, '
            'behavior not implemented."""\n',
        )
        write_yaml_atomic(
            fragment_path,
            {
                "schema_version": "arch_004e_module_fragment.v1",
                "fragment_id": f"module.{identifier}",
                "fragment_kind": "module",
                "owner": owner,
                "module_prefix": f"src/ai_trading_system/{identifier}/",
                "public_contracts": [],
                "compatibility_facades": [],
                "deprecation_status": "draft_scaffold",
            },
            sort_keys=False,
        )
    elif kind is ScaffoldKind.EXPERIMENT:
        (spec_path,) = planned
        write_yaml_atomic(
            spec_path,
            {
                "schema_version": "experiment_spec.v1",
                "experiment_id": identifier,
                "spec_version": f"{identifier}.experiment.draft.v1",
                "status": "draft_scaffold_not_executable",
                "owner": owner,
                "calculator_plugin": {"plugin_id": "OWNER_MUST_DEFINE", "version": "v1"},
                "report_plugin": {"plugin_id": "OWNER_MUST_DEFINE", "version": "v1"},
                "production_effect": "none",
                "broker_action": "none",
            },
            sort_keys=False,
        )
    else:
        (fragment_path,) = planned
        write_yaml_atomic(
            fragment_path,
            {
                "schema_version": "arch_004e_report_fragment.v1",
                "fragment_id": f"report.{identifier}",
                "fragment_kind": "report",
                "owner": owner,
                "target_id": "report_registry",
                "report_id": identifier,
                "compatibility_registry_entry_required": False,
                "generated_source_of_truth_active": False,
            },
            sort_keys=False,
        )
    return ScaffoldResult(kind=kind, identifier=identifier, paths=planned)


def _planned_paths(project_root: Path, kind: ScaffoldKind, identifier: str) -> tuple[Path, ...]:
    if kind is ScaffoldKind.MODULE:
        return (
            project_root / "src/ai_trading_system" / identifier / "__init__.py",
            project_root / "config/architecture/fragments/modules" / f"{identifier}.yaml",
        )
    if kind is ScaffoldKind.EXPERIMENT:
        return (project_root / "config/research/experiments" / f"{identifier}.yaml",)
    return (project_root / "config/architecture/fragments/reports" / f"{identifier}.yaml",)
