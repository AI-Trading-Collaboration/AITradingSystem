from __future__ import annotations

import ast
import hashlib
import json
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.deprecation import SurfaceLifecycle
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_DEPRECATION_POLICY_PATH = (
    PROJECT_ROOT / "config" / "architecture" / "arch_004g_deprecation_policy.yaml"
)
DEFAULT_DEPRECATION_INVENTORY_PATH = (
    PROJECT_ROOT / "inputs" / "architecture" / "arch_004g_deprecation_inventory.yaml"
)
DEFAULT_ARCHITECTURE_FITNESS_PATH = (
    PROJECT_ROOT / "inputs" / "architecture" / "arch_004e_architecture_fitness.yaml"
)


class DeprecationArchitectureError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class DeprecationTargetPolicy:
    surface_id: str
    lane: str
    scan_mode: str
    path: str
    owner: str
    replacement: str
    replacement_status: str
    lifecycle: SurfaceLifecycle
    migration_wave: str
    compatibility_window: str
    sunset_condition: str
    usage_evidence_ref: str
    interpretation_sensitive: bool

    def __post_init__(self) -> None:
        for value, field in (
            (self.surface_id, "surface_id"),
            (self.lane, "lane"),
            (self.scan_mode, "scan_mode"),
            (self.path, "path"),
            (self.owner, "owner"),
            (self.replacement, "replacement"),
            (self.replacement_status, "replacement_status"),
            (self.migration_wave, "migration_wave"),
            (self.compatibility_window, "compatibility_window"),
            (self.sunset_condition, "sunset_condition"),
            (self.usage_evidence_ref, "usage_evidence_ref"),
        ):
            if not value.strip():
                raise DeprecationArchitectureError("DEPRECATION_TARGET_FIELD_REQUIRED", field)
        if self.scan_mode not in {"python_file", "python_glob", "repository_metric"}:
            raise DeprecationArchitectureError(
                "DEPRECATION_SCAN_MODE_INVALID", self.scan_mode
            )


@dataclass(frozen=True)
class DeprecationPolicy:
    policy_id: str
    owner: str
    version: str
    states: tuple[SurfaceLifecycle, ...]
    required_gate_ids: tuple[str, ...]
    permanent_dual_track_allowed: bool
    g0_runtime_removal_allowed: bool
    artifact_retention_separate_from_code_removal: bool
    unknown_reachability_is_removal_ready: bool
    targets: tuple[DeprecationTargetPolicy, ...]
    production_effect: str
    broker_action: str

    def __post_init__(self) -> None:
        if self.states != tuple(SurfaceLifecycle):
            raise DeprecationArchitectureError(
                "DEPRECATION_LIFECYCLE_STATES_INVALID", self.policy_id
            )
        if not self.required_gate_ids or len(set(self.required_gate_ids)) != len(
            self.required_gate_ids
        ):
            raise DeprecationArchitectureError(
                "DEPRECATION_REQUIRED_GATES_INVALID", self.policy_id
            )
        if self.permanent_dual_track_allowed or self.g0_runtime_removal_allowed:
            raise DeprecationArchitectureError(
                "DEPRECATION_G0_SAFETY_INVALID", self.policy_id
            )
        if not self.artifact_retention_separate_from_code_removal:
            raise DeprecationArchitectureError(
                "DEPRECATION_ARTIFACT_RETENTION_REQUIRED", self.policy_id
            )
        if self.unknown_reachability_is_removal_ready:
            raise DeprecationArchitectureError(
                "DEPRECATION_UNKNOWN_REACHABILITY_UNSAFE", self.policy_id
            )
        if self.production_effect != "none" or self.broker_action != "none":
            raise DeprecationArchitectureError(
                "DEPRECATION_PRODUCTION_BOUNDARY_INVALID", self.policy_id
            )
        surface_ids = tuple(item.surface_id for item in self.targets)
        if not surface_ids or len(set(surface_ids)) != len(surface_ids):
            raise DeprecationArchitectureError(
                "DEPRECATION_TARGETS_INVALID", self.policy_id
            )
        if any(item.lifecycle is SurfaceLifecycle.REMOVED for item in self.targets):
            raise DeprecationArchitectureError(
                "DEPRECATION_G0_REMOVED_TARGET_FORBIDDEN", self.policy_id
            )


@dataclass(frozen=True)
class DeprecationSurfaceInventory:
    surface_id: str
    lane: str
    lifecycle: str
    replacement_status: str
    migration_wave: str
    path: str
    file_count: int
    line_count: int
    byte_count: int
    source_sha256: str
    top_level_function_count: int
    top_level_class_count: int
    cli_command_decorator_count: int
    static_import_reachability_file_count: int
    test_reference_file_count: int
    docs_config_reference_file_count: int
    required_removal_gate_count: int
    removal_ready: bool
    open_gate_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "surface_id": self.surface_id,
            "lane": self.lane,
            "lifecycle": self.lifecycle,
            "replacement_status": self.replacement_status,
            "migration_wave": self.migration_wave,
            "path": self.path,
            "file_count": self.file_count,
            "line_count": self.line_count,
            "byte_count": self.byte_count,
            "source_sha256": self.source_sha256,
            "top_level_function_count": self.top_level_function_count,
            "top_level_class_count": self.top_level_class_count,
            "cli_command_decorator_count": self.cli_command_decorator_count,
            "static_import_reachability_file_count": (
                self.static_import_reachability_file_count
            ),
            "test_reference_file_count": self.test_reference_file_count,
            "docs_config_reference_file_count": self.docs_config_reference_file_count,
            "required_removal_gate_count": self.required_removal_gate_count,
            "removal_ready": self.removal_ready,
            "open_gate_ids": list(self.open_gate_ids),
        }


@dataclass(frozen=True)
class DeprecationInventory:
    policy_id: str
    python_module_count: int
    python_test_file_count: int
    direct_writer_baseline_count: int
    direct_writer_current_count: int
    direct_writer_violation_count: int
    legacy_adapter_file_count: int
    dynamic_strategy_wrapper_file_count: int
    research_quality_matching_wrapper_count: int
    lifecycle_counts: tuple[tuple[str, int], ...]
    removal_ready_count: int
    surfaces: tuple[DeprecationSurfaceInventory, ...]

    @property
    def inventory_id(self) -> str:
        encoded = json.dumps(
            self.to_dict(include_id=False),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"arch_004g_deprecation_inventory_{hashlib.sha256(encoded).hexdigest()[:20]}"

    def to_dict(self, *, include_id: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": "arch_004g_deprecation_inventory.v1",
            "policy_id": self.policy_id,
            "repository": {
                "python_module_count": self.python_module_count,
                "python_test_file_count": self.python_test_file_count,
                "direct_writer_baseline_count": self.direct_writer_baseline_count,
                "direct_writer_current_count": self.direct_writer_current_count,
                "direct_writer_violation_count": self.direct_writer_violation_count,
                "legacy_adapter_file_count": self.legacy_adapter_file_count,
                "dynamic_strategy_wrapper_file_count": (
                    self.dynamic_strategy_wrapper_file_count
                ),
                "research_quality_matching_wrapper_count": (
                    self.research_quality_matching_wrapper_count
                ),
            },
            "lifecycle_counts": dict(self.lifecycle_counts),
            "removal_ready_count": self.removal_ready_count,
            "surfaces": [item.to_dict() for item in self.surfaces],
        }
        return {"inventory_id": self.inventory_id, **payload} if include_id else payload


def load_deprecation_policy(
    path: Path = DEFAULT_DEPRECATION_POLICY_PATH,
) -> DeprecationPolicy:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping) or payload.get("schema_version") != (
        "arch_004g_deprecation_policy.v1"
    ):
        raise DeprecationArchitectureError("DEPRECATION_POLICY_SCHEMA_INVALID", str(path))
    lifecycle = _mapping(payload, "lifecycle")
    removal = _mapping(payload, "removal_policy")
    safety = _mapping(payload, "safety_boundary")
    targets = tuple(
        _target(_as_mapping(item, "target")) for item in _list(payload, "targets")
    )
    policy = DeprecationPolicy(
        policy_id=str(payload.get("policy_id", "")),
        owner=str(payload.get("owner", "")),
        version=str(payload.get("version", "")),
        states=tuple(SurfaceLifecycle(str(item)) for item in _list(lifecycle, "states")),
        required_gate_ids=tuple(str(item) for item in _list(removal, "required_gate_ids")),
        permanent_dual_track_allowed=_bool(removal, "permanent_dual_track_allowed"),
        g0_runtime_removal_allowed=_bool(removal, "g0_runtime_removal_allowed"),
        artifact_retention_separate_from_code_removal=_bool(
            removal, "artifact_retention_separate_from_code_removal"
        ),
        unknown_reachability_is_removal_ready=_bool(
            removal, "unknown_reachability_is_removal_ready"
        ),
        targets=targets,
        production_effect=str(safety.get("production_effect", "")),
        broker_action=str(safety.get("broker_action", "")),
    )
    _validate_transitions(lifecycle)
    return policy


def scan_deprecation_inventory(
    policy: DeprecationPolicy | None = None,
    *,
    project_root: Path = PROJECT_ROOT,
    architecture_fitness_path: Path = DEFAULT_ARCHITECTURE_FITNESS_PATH,
) -> DeprecationInventory:
    resolved = policy or load_deprecation_policy()
    source_root = project_root / "src" / "ai_trading_system"
    test_root = project_root / "tests"
    python_files = tuple(sorted(source_root.rglob("*.py")))
    test_files = tuple(sorted(test_root.rglob("*.py")))
    source_text = {path: path.read_text(encoding="utf-8") for path in python_files}
    test_text = {path: path.read_text(encoding="utf-8") for path in test_files}
    reference_files = tuple(
        sorted((project_root / "docs").rglob("*.md"))
        + sorted((project_root / "config").rglob("*.yaml"))
    )
    reference_text = {
        path: path.read_text(encoding="utf-8") for path in reference_files
    }
    surfaces = tuple(
        _scan_target(
            target,
            required_gate_ids=resolved.required_gate_ids,
            project_root=project_root,
            source_text=source_text,
            test_text=test_text,
            reference_text=reference_text,
        )
        for target in resolved.targets
    )
    fitness = safe_load_yaml_path(architecture_fitness_path)
    if not isinstance(fitness, Mapping):
        raise DeprecationArchitectureError(
            "DEPRECATION_FITNESS_INVALID", str(architecture_fitness_path)
        )
    dependency = _mapping(fitness, "dependency_gate")
    root_wrappers = tuple(sorted(source_root.glob("dynamic_strategy_*.py")))
    quality_names = {
        path.name
        for path in (source_root / "research_quality").glob("*.py")
    }
    lifecycle_counts = Counter(item.lifecycle.value for item in resolved.targets)
    return DeprecationInventory(
        policy_id=resolved.policy_id,
        python_module_count=_integer(fitness, "module_count"),
        python_test_file_count=_integer(fitness, "test_file_count"),
        direct_writer_baseline_count=_integer(
            dependency, "baseline_direct_writer_calls"
        ),
        direct_writer_current_count=_integer(dependency, "current_direct_writer_calls"),
        direct_writer_violation_count=_integer(dependency, "violation_count"),
        legacy_adapter_file_count=len(
            tuple((source_root / "legacy").glob("*.py"))
        ),
        dynamic_strategy_wrapper_file_count=len(root_wrappers),
        research_quality_matching_wrapper_count=sum(
            path.name.removeprefix("dynamic_strategy_") in quality_names
            for path in root_wrappers
        ),
        lifecycle_counts=tuple(sorted(lifecycle_counts.items())),
        removal_ready_count=sum(item.removal_ready for item in surfaces),
        surfaces=surfaces,
    )


def assert_frozen_deprecation_inventory(
    actual: DeprecationInventory,
    path: Path = DEFAULT_DEPRECATION_INVENTORY_PATH,
) -> None:
    frozen = safe_load_yaml_path(path)
    if not isinstance(frozen, Mapping):
        raise DeprecationArchitectureError("DEPRECATION_INVENTORY_INVALID", str(path))
    observed = actual.to_dict(include_id=False)
    expected = {
        key: frozen.get(key)
        for key in (
            "schema_version",
            "policy_id",
            "repository",
            "lifecycle_counts",
            "removal_ready_count",
            "surfaces",
        )
    }
    if observed != expected:
        raise DeprecationArchitectureError("DEPRECATION_INVENTORY_DRIFT", str(path))


def _target(payload: Mapping[str, object]) -> DeprecationTargetPolicy:
    return DeprecationTargetPolicy(
        surface_id=str(payload.get("surface_id", "")),
        lane=str(payload.get("lane", "")),
        scan_mode=str(payload.get("scan_mode", "")),
        path=str(payload.get("path", "")),
        owner=str(payload.get("owner", "")),
        replacement=str(payload.get("replacement", "")),
        replacement_status=str(payload.get("replacement_status", "")),
        lifecycle=SurfaceLifecycle(str(payload.get("lifecycle", ""))),
        migration_wave=str(payload.get("migration_wave", "")),
        compatibility_window=str(payload.get("compatibility_window", "")),
        sunset_condition=str(payload.get("sunset_condition", "")),
        usage_evidence_ref=str(payload.get("usage_evidence_ref", "")),
        interpretation_sensitive=_bool(payload, "interpretation_sensitive"),
    )


def _scan_target(
    target: DeprecationTargetPolicy,
    *,
    required_gate_ids: tuple[str, ...],
    project_root: Path,
    source_text: Mapping[Path, str],
    test_text: Mapping[Path, str],
    reference_text: Mapping[Path, str],
) -> DeprecationSurfaceInventory:
    paths = _target_paths(target, project_root)
    if not paths:
        raise DeprecationArchitectureError(
            "DEPRECATION_TARGET_PATH_EMPTY", target.surface_id
        )
    stats = [_python_stats(path) for path in paths]
    modules = tuple(
        _module_name(path, project_root)
        for path in paths
        if path.suffix == ".py" and "src" in path.parts
    )
    target_paths = {path.resolve() for path in paths}
    import_references = sum(
        path.resolve() not in target_paths
        and any(_imports_module(text, module) for module in modules)
        for path, text in source_text.items()
    )
    tokens = {
        target.surface_id,
        *(path.name for path in paths),
        *(path.stem for path in paths),
        *modules,
    }
    test_references = sum(
        any(token in text for token in tokens) for text in test_text.values()
    )
    docs_references = sum(
        any(token in text for token in tokens) for text in reference_text.values()
    )
    digest_material = "\n".join(
        (
            f"{_project_path(path, project_root)}:"
            f"{hashlib.sha256(_canonical_repository_text_bytes(path)).hexdigest()}"
        )
        for path in paths
    ).encode("utf-8")
    return DeprecationSurfaceInventory(
        surface_id=target.surface_id,
        lane=target.lane,
        lifecycle=target.lifecycle.value,
        replacement_status=target.replacement_status,
        migration_wave=target.migration_wave,
        path=target.path,
        file_count=len(paths),
        line_count=sum(item[0] for item in stats),
        byte_count=sum(item[1] for item in stats),
        source_sha256=hashlib.sha256(digest_material).hexdigest(),
        top_level_function_count=sum(item[2] for item in stats),
        top_level_class_count=sum(item[3] for item in stats),
        cli_command_decorator_count=sum(item[4] for item in stats),
        static_import_reachability_file_count=import_references,
        test_reference_file_count=test_references,
        docs_config_reference_file_count=docs_references,
        required_removal_gate_count=len(required_gate_ids),
        removal_ready=False,
        open_gate_ids=required_gate_ids,
    )


def _target_paths(target: DeprecationTargetPolicy, project_root: Path) -> tuple[Path, ...]:
    if target.scan_mode == "python_glob":
        return tuple(sorted(project_root.glob(target.path)))
    path = project_root / target.path
    return (path,) if path.is_file() else ()


def _python_stats(path: Path) -> tuple[int, int, int, int, int]:
    canonical_bytes = _canonical_repository_text_bytes(path)
    text = canonical_bytes.decode("utf-8")
    if path.suffix != ".py":
        return len(text.splitlines()), len(canonical_bytes), 0, 0, 0
    tree = ast.parse(text)
    functions = sum(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in tree.body
    )
    classes = sum(isinstance(node, ast.ClassDef) for node in tree.body)
    command_decorators = sum(
        _is_command_decorator(decorator)
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        for decorator in node.decorator_list
    )
    return len(text.splitlines()), len(canonical_bytes), functions, classes, command_decorators


def _canonical_repository_text_bytes(path: Path) -> bytes:
    """Return UTF-8 bytes with universal newlines normalized to LF."""
    return path.read_text(encoding="utf-8").encode("utf-8")


def _is_command_decorator(node: ast.expr) -> bool:
    target = node.func if isinstance(node, ast.Call) else node
    if isinstance(target, ast.Attribute):
        return target.attr == "command"
    return isinstance(target, ast.Name) and target.id == "command"


def _imports_module(text: str, module: str) -> bool:
    return f"from {module} import" in text or f"import {module}" in text


def _module_name(path: Path, project_root: Path) -> str:
    relative = path.resolve().relative_to((project_root / "src").resolve())
    return ".".join(relative.with_suffix("").parts)


def _project_path(path: Path, project_root: Path) -> str:
    return path.resolve().relative_to(project_root.resolve()).as_posix()


def _validate_transitions(payload: Mapping[str, object]) -> None:
    transitions = _mapping(payload, "allowed_transitions")
    expected = {
        "EXPERIMENTAL": ["ACTIVE"],
        "ACTIVE": ["DEPRECATED"],
        "DEPRECATED": ["FROZEN"],
        "FROZEN": ["REMOVED"],
        "REMOVED": [],
    }
    observed = {key: [str(item) for item in _list(transitions, key)] for key in expected}
    if observed != expected:
        raise DeprecationArchitectureError(
            "DEPRECATION_TRANSITIONS_INVALID", json.dumps(observed, sort_keys=True)
        )


def _mapping(payload: Mapping[str, object], field: str) -> Mapping[str, object]:
    value = payload.get(field)
    if not isinstance(value, Mapping):
        raise DeprecationArchitectureError("DEPRECATION_MAPPING_REQUIRED", field)
    return value


def _as_mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise DeprecationArchitectureError("DEPRECATION_MAPPING_REQUIRED", field)
    return value


def _list(payload: Mapping[str, object], field: str) -> list[object]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise DeprecationArchitectureError("DEPRECATION_LIST_REQUIRED", field)
    return value


def _bool(payload: Mapping[str, object], field: str) -> bool:
    value = payload.get(field)
    if not isinstance(value, bool):
        raise DeprecationArchitectureError("DEPRECATION_BOOL_REQUIRED", field)
    return value


def _integer(payload: Mapping[str, object], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise DeprecationArchitectureError("DEPRECATION_INT_REQUIRED", field)
    return value


__all__ = [
    "DEFAULT_DEPRECATION_INVENTORY_PATH",
    "DEFAULT_DEPRECATION_POLICY_PATH",
    "DeprecationArchitectureError",
    "DeprecationInventory",
    "DeprecationPolicy",
    "DeprecationSurfaceInventory",
    "DeprecationTargetPolicy",
    "assert_frozen_deprecation_inventory",
    "load_deprecation_policy",
    "scan_deprecation_inventory",
]
