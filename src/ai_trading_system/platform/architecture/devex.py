from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system.platform.architecture.dependency_gate import (
    validate_architecture_dependencies,
)
from ai_trading_system.platform.artifacts import write_yaml_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

_GIT_EOL_LF_SUFFIXES = frozenset({".md", ".py", ".toml", ".yaml", ".yml"})


class DevExArchitectureError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class ImpactSelection:
    changed_paths: tuple[str, ...]
    owner_profiles: tuple[str, ...]
    focused_tests: tuple[str, ...]
    validation_tiers: tuple[str, ...]
    integration_coordinator_required: bool
    full_validation_required: bool
    unresolved_paths: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "changed_paths": list(self.changed_paths),
            "owner_profiles": list(self.owner_profiles),
            "focused_tests": list(self.focused_tests),
            "validation_tiers": list(self.validation_tiers),
            "integration_coordinator_required": self.integration_coordinator_required,
            "full_validation_required": self.full_validation_required,
            "unresolved_paths": list(self.unresolved_paths),
        }


def build_module_manifest(*, project_root: Path, policy_path: Path) -> dict[str, Any]:
    policy = _load_mapping(policy_path, "ownership policy")
    profiles = _mapping(policy.get("owner_profiles"), "owner_profiles")
    rules = _list(policy.get("module_rules"), "module_rules")
    shared = set(_strings(policy.get("shared_integration_paths")))
    source_root = project_root / "src/ai_trading_system"
    rows = []
    for path in sorted(source_root.rglob("*.py")):
        portable = _portable(path, project_root)
        rule = _select_rule(portable, rules, "module")
        profile_id = _required_text(rule, "owner_profile")
        owners = _owner_profile(profiles, profile_id)
        rows.append(
            {
                "module_id": portable.removesuffix(".py").replace("/", "."),
                "path": portable,
                "sha256": _sha256(path),
                "rule_id": _required_text(rule, "rule_id"),
                "owner_profile": profile_id,
                "owners": owners,
                "layer": _required_text(rule, "layer"),
                "category": _required_text(rule, "category"),
                "public_contract": bool(rule.get("public_contract", False)),
                "deprecation_status": str(rule.get("deprecation_status") or "active"),
                "test_globs": _strings(rule.get("test_globs")),
                "validation_tiers": _strings(rule.get("validation_tiers")),
                "shared_integration": portable in shared,
            }
        )
    return {
        "schema_version": "arch_004e_module_manifest.v1",
        "status": "PASS",
        "policy_id": str(policy.get("policy_id") or ""),
        "policy_sha256": _sha256(policy_path),
        "source_root": _portable(source_root, project_root),
        "module_count": len(rows),
        "orphan_count": 0,
        "specific_overlap_count": 0,
        "modules": rows,
    }


def build_test_manifest(*, project_root: Path, policy_path: Path) -> dict[str, Any]:
    policy = _load_mapping(policy_path, "ownership policy")
    profiles = _mapping(policy.get("owner_profiles"), "owner_profiles")
    rules = _list(policy.get("test_rules"), "test_rules")
    test_root = project_root / "tests"
    rows = []
    for path in sorted(test_root.rglob("*.py")):
        portable = _portable(path, project_root)
        rule = _select_rule(portable, rules, "test")
        profile_id = _required_text(rule, "owner_profile")
        rows.append(
            {
                "test_id": portable.removesuffix(".py").replace("/", "."),
                "path": portable,
                "sha256": _sha256(path),
                "file_role": "test" if path.name.startswith("test_") else "test_support",
                "rule_id": _required_text(rule, "rule_id"),
                "owner_profile": profile_id,
                "owners": _owner_profile(profiles, profile_id),
                "category": _required_text(rule, "category"),
                "suite": _required_text(rule, "suite"),
                "covered_module_prefixes": _strings(rule.get("covered_module_prefixes")),
            }
        )
    return {
        "schema_version": "arch_004e_test_manifest.v1",
        "status": "PASS",
        "policy_id": str(policy.get("policy_id") or ""),
        "policy_sha256": _sha256(policy_path),
        "test_root": _portable(test_root, project_root),
        "test_file_count": len(rows),
        "test_count": sum(1 for row in rows if row["file_role"] == "test"),
        "support_file_count": sum(1 for row in rows if row["file_role"] == "test_support"),
        "orphan_count": 0,
        "specific_overlap_count": 0,
        "tests": rows,
    }


def build_aggregate_shadow_index(*, project_root: Path, policy_path: Path) -> dict[str, Any]:
    policy = _load_mapping(policy_path, "ownership policy")
    targets = _list(policy.get("aggregate_targets"), "aggregate_targets")
    target_rows = []
    known_target_ids: set[str] = set()
    for raw_target in targets:
        target = _mapping(raw_target, "aggregate target")
        target_id = _required_text(target, "target_id")
        if target_id in known_target_ids:
            raise DevExArchitectureError("DUPLICATE_AGGREGATE_TARGET", target_id)
        known_target_ids.add(target_id)
        current_path = project_root / _required_text(target, "current_path")
        fragment_root = project_root / _required_text(target, "fragment_root")
        fragments = tuple(sorted(fragment_root.rglob("*.yaml"))) if fragment_root.exists() else ()
        target_rows.append(
            {
                "target_id": target_id,
                "current_path": _portable(current_path, project_root),
                "current_exists": current_path.is_file(),
                "current_sha256": _sha256(current_path) if current_path.is_file() else None,
                "fragment_root": _portable(fragment_root, project_root),
                "fragment_count": len(fragments),
                "fragment_paths": [_portable(path, project_root) for path in fragments],
            }
        )
    all_fragment_root = project_root / "config/architecture/fragments"
    all_fragments = tuple(sorted(all_fragment_root.rglob("*.yaml")))
    fragment_ids: set[str] = set()
    fragment_rows = []
    for path in all_fragments:
        payload = _load_mapping(path, "aggregate fragment")
        fragment_id = _required_text(payload, "fragment_id")
        if fragment_id in fragment_ids:
            raise DevExArchitectureError("DUPLICATE_AGGREGATE_FRAGMENT", fragment_id)
        fragment_ids.add(fragment_id)
        fragment_target = payload.get("target_id")
        if fragment_target is not None and str(fragment_target) not in known_target_ids:
            raise DevExArchitectureError(
                "UNKNOWN_AGGREGATE_FRAGMENT_TARGET", f"{fragment_id}:{fragment_target}"
            )
        fragment_rows.append(
            {
                "fragment_id": fragment_id,
                "fragment_kind": _required_text(payload, "fragment_kind"),
                "owner": _required_text(payload, "owner"),
                "target_id": None if fragment_target is None else str(fragment_target),
                "path": _portable(path, project_root),
                "sha256": _sha256(path),
                "generated_source_of_truth_active": bool(
                    payload.get("generated_source_of_truth_active", False)
                ),
            }
        )
    return {
        "schema_version": "arch_004e_aggregate_shadow_index.v1",
        "status": "SHADOW_COMPATIBILITY_PASS",
        "policy_id": str(policy.get("policy_id") or ""),
        "policy_sha256": _sha256(policy_path),
        "existing_aggregate_source_of_truth_changed": False,
        "target_count": len(target_rows),
        "fragment_count": len(fragment_rows),
        "targets": target_rows,
        "fragments": fragment_rows,
    }


def select_impacted_tests(
    *,
    project_root: Path,
    policy_path: Path,
    module_manifest: Mapping[str, Any],
    test_manifest: Mapping[str, Any],
    changed_paths: Sequence[str],
) -> ImpactSelection:
    policy = _load_mapping(policy_path, "ownership policy")
    module_by_path = {str(row["path"]): row for row in _records(module_manifest.get("modules"))}
    test_by_path = {str(row["path"]): row for row in _records(test_manifest.get("tests"))}
    change_rules = _list(policy.get("change_rules"), "change_rules")
    shared_paths = set(_strings(policy.get("shared_integration_paths")))
    profiles: set[str] = set()
    test_globs: set[str] = set()
    tests: set[str] = set()
    tiers: set[str] = set()
    unresolved: set[str] = set()
    coordinator = False
    normalized_paths = tuple(sorted({_normalize_path(path) for path in changed_paths}))
    for path in normalized_paths:
        if path in shared_paths:
            coordinator = True
            tiers.update(("architecture-fitness", "contract-validation", "full"))
        if path in module_by_path:
            row = module_by_path[path]
            profiles.add(str(row["owner_profile"]))
            test_globs.update(_strings(row.get("test_globs")))
            tiers.update(_strings(row.get("validation_tiers")))
            continue
        if path in test_by_path:
            row = test_by_path[path]
            profiles.add(str(row["owner_profile"]))
            tests.add(path)
            tiers.add(str(row["suite"]))
            continue
        try:
            rule = _select_rule(path, change_rules, "change")
        except DevExArchitectureError:
            unresolved.add(path)
            continue
        profiles.add(_required_text(rule, "owner_profile"))
        test_globs.update(_strings(rule.get("test_globs")))
        tiers.update(_strings(rule.get("validation_tiers")))
        if bool(rule.get("fallback", False)):
            unresolved.add(path)
    for pattern in sorted(test_globs):
        tests.update(
            _portable(path, project_root)
            for path in project_root.glob(pattern)
            if path.is_file() and path.suffix == ".py"
        )
    if unresolved:
        coordinator = True
        tiers.update(("architecture-fitness", "full"))
    return ImpactSelection(
        changed_paths=normalized_paths,
        owner_profiles=tuple(sorted(profiles)),
        focused_tests=tuple(sorted(tests)),
        validation_tiers=tuple(sorted(tiers)),
        integration_coordinator_required=coordinator,
        full_validation_required=True,
        unresolved_paths=tuple(sorted(unresolved)),
    )


def build_architecture_fitness(
    *,
    project_root: Path,
    policy_path: Path,
    module_manifest_path: Path,
    test_manifest_path: Path,
    aggregate_index_path: Path,
    dependency_policy_path: Path,
    direct_writer_baseline_path: Path,
) -> dict[str, Any]:
    violations: list[dict[str, object]] = []
    expected_module = build_module_manifest(project_root=project_root, policy_path=policy_path)
    expected_test = build_test_manifest(project_root=project_root, policy_path=policy_path)
    expected_aggregate = build_aggregate_shadow_index(
        project_root=project_root, policy_path=policy_path
    )
    actual_module = _load_mapping(module_manifest_path, "module manifest")
    actual_test = _load_mapping(test_manifest_path, "test manifest")
    actual_aggregate = _load_mapping(aggregate_index_path, "aggregate shadow index")
    for check_id, actual, expected, remediation in (
        (
            "module_manifest_fresh",
            actual_module,
            expected_module,
            "regenerate module manifest",
        ),
        ("test_manifest_fresh", actual_test, expected_test, "regenerate test manifest"),
        (
            "aggregate_shadow_index_reproducible",
            actual_aggregate,
            expected_aggregate,
            "regenerate aggregate shadow index",
        ),
    ):
        if actual != expected:
            violations.append(
                {
                    "rule_id": check_id,
                    "owner": "architecture_governance",
                    "message": "generated artifact differs from deterministic rebuild",
                    "remediation": remediation,
                }
            )
    dependency = validate_architecture_dependencies(
        policy_path=dependency_policy_path,
        baseline_path=direct_writer_baseline_path,
        source_root=project_root / "src/ai_trading_system",
    )
    violations.extend(item.to_dict() for item in dependency.violations)
    return {
        "schema_version": "arch_004e_architecture_fitness.v1",
        "status": "PASS" if not violations else "FAIL",
        "policy_id": _load_mapping(policy_path, "ownership policy").get("policy_id"),
        "module_count": expected_module["module_count"],
        "test_file_count": expected_test["test_file_count"],
        "module_orphan_count": expected_module["orphan_count"],
        "module_specific_overlap_count": expected_module["specific_overlap_count"],
        "test_orphan_count": expected_test["orphan_count"],
        "test_specific_overlap_count": expected_test["specific_overlap_count"],
        "aggregate_target_count": expected_aggregate["target_count"],
        "aggregate_fragment_count": expected_aggregate["fragment_count"],
        "dependency_gate": dependency.to_dict(),
        "existing_aggregate_source_of_truth_changed": False,
        "impact_selection_replaces_full_gate": False,
        "violation_count": len(violations),
        "violations": violations,
    }


def write_generated_architecture_artifact(path: Path, payload: Mapping[str, Any]) -> Path:
    write_yaml_atomic(path, dict(payload), sort_keys=False)
    return path


def _select_rule(path: str, raw_rules: list[object], kind: str) -> Mapping[str, Any]:
    rules = [_mapping(item, f"{kind} rule") for item in raw_rules]
    specific = [rule for rule in rules if not rule.get("fallback") and _rule_matches(path, rule)]
    if len(specific) > 1:
        ids = [_required_text(rule, "rule_id") for rule in specific]
        raise DevExArchitectureError("SPECIFIC_OWNERSHIP_OVERLAP", f"{path}: {','.join(ids)}")
    if specific:
        return specific[0]
    fallbacks = [rule for rule in rules if rule.get("fallback") and _rule_matches(path, rule)]
    if len(fallbacks) != 1:
        raise DevExArchitectureError("OWNERSHIP_ORPHAN", f"{path}: fallback_count={len(fallbacks)}")
    return fallbacks[0]


def _rule_matches(path: str, rule: Mapping[str, Any]) -> bool:
    exact = _strings(rule.get("exact_paths"))
    exact_match = bool(exact) and path in exact
    prefix_match = "path_prefix" in rule and path.startswith(str(rule.get("path_prefix") or ""))
    return exact_match or prefix_match


def _owner_profile(profiles: Mapping[str, Any], profile_id: str) -> dict[str, str]:
    profile = _mapping(profiles.get(profile_id), f"owner profile {profile_id}")
    fields = ("code_owner", "policy_owner", "data_owner", "artifact_owner", "runtime_owner")
    return {field: _required_text(profile, field) for field in fields}


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = str(payload.get(field) or "").strip()
    if not value:
        raise DevExArchitectureError("REQUIRED_DEVEX_FIELD_EMPTY", field)
    return value


def _load_mapping(path: Path, label: str) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise DevExArchitectureError("DEVEX_MAPPING_REQUIRED", f"{label}: {path}")
    return dict(raw)


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DevExArchitectureError("DEVEX_MAPPING_REQUIRED", label)
    return value


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise DevExArchitectureError("DEVEX_LIST_REQUIRED", label)
    return value


def _records(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _strings(value: object) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item) for item in value]


def _sha256(path: Path) -> str:
    payload = path.read_bytes()
    if path.suffix.lower() in _GIT_EOL_LF_SUFFIXES:
        payload = payload.replace(b"\r\n", b"\n")
    return hashlib.sha256(payload).hexdigest()


def _portable(path: Path, project_root: Path) -> str:
    return path.resolve().relative_to(project_root.resolve()).as_posix()


def _normalize_path(path: str) -> str:
    return path.strip().replace("\\", "/").removeprefix("./")
