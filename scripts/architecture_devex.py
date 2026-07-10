from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.platform.architecture import (
    ScaffoldKind,
    build_aggregate_shadow_index,
    build_architecture_fitness,
    build_module_manifest,
    build_test_manifest,
    create_scaffold,
    select_impacted_tests,
    write_generated_architecture_artifact,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/architecture/devex_ownership_policy.yaml"
MODULE_MANIFEST_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_module_manifest.yaml"
TEST_MANIFEST_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_test_manifest.yaml"
AGGREGATE_INDEX_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_aggregate_shadow_index.yaml"
FITNESS_PATH = PROJECT_ROOT / "inputs/architecture/arch_004e_architecture_fitness.yaml"
DEPENDENCY_POLICY_PATH = PROJECT_ROOT / "config/architecture/arch_004c_dependency_policy.yaml"
DIRECT_WRITER_BASELINE_PATH = (
    PROJECT_ROOT / "inputs/architecture/arch_004c_direct_writer_baseline.yaml"
)


def main() -> int:
    parser = argparse.ArgumentParser(description="ARCH-004E DevEx architecture control plane")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("generate")
    subparsers.add_parser("validate")
    impact = subparsers.add_parser("impact")
    impact.add_argument("paths", nargs="+")
    scaffold = subparsers.add_parser("scaffold")
    scaffold.add_argument("kind", choices=[item.value for item in ScaffoldKind])
    scaffold.add_argument("identifier")
    scaffold.add_argument("--owner", required=True)
    scaffold.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    args = parser.parse_args()
    if args.command == "generate":
        return _generate()
    if args.command == "validate":
        return _validate()
    if args.command == "impact":
        return _impact(args.paths)
    result = create_scaffold(
        project_root=args.project_root,
        kind=ScaffoldKind(args.kind),
        identifier=args.identifier,
        owner=args.owner,
    )
    print(
        json.dumps(
            {
                "kind": result.kind.value,
                "identifier": result.identifier,
                "paths": [str(path) for path in result.paths],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _generate() -> int:
    module_manifest = build_module_manifest(project_root=PROJECT_ROOT, policy_path=POLICY_PATH)
    test_manifest = build_test_manifest(project_root=PROJECT_ROOT, policy_path=POLICY_PATH)
    aggregate = build_aggregate_shadow_index(project_root=PROJECT_ROOT, policy_path=POLICY_PATH)
    write_generated_architecture_artifact(MODULE_MANIFEST_PATH, module_manifest)
    write_generated_architecture_artifact(TEST_MANIFEST_PATH, test_manifest)
    write_generated_architecture_artifact(AGGREGATE_INDEX_PATH, aggregate)
    fitness = _fitness()
    write_generated_architecture_artifact(FITNESS_PATH, fitness)
    print(json.dumps(fitness, ensure_ascii=False, indent=2))
    return 0 if fitness["status"] == "PASS" else 1


def _validate() -> int:
    fitness = _fitness()
    print(json.dumps(fitness, ensure_ascii=False, indent=2))
    return 0 if fitness["status"] == "PASS" else 1


def _impact(paths: list[str]) -> int:
    selection = select_impacted_tests(
        project_root=PROJECT_ROOT,
        policy_path=POLICY_PATH,
        module_manifest=_mapping(safe_load_yaml_path(MODULE_MANIFEST_PATH)),
        test_manifest=_mapping(safe_load_yaml_path(TEST_MANIFEST_PATH)),
        changed_paths=paths,
    )
    print(json.dumps(selection.to_dict(), ensure_ascii=False, indent=2))
    return 0


def _fitness() -> dict[str, object]:
    return build_architecture_fitness(
        project_root=PROJECT_ROOT,
        policy_path=POLICY_PATH,
        module_manifest_path=MODULE_MANIFEST_PATH,
        test_manifest_path=TEST_MANIFEST_PATH,
        aggregate_index_path=AGGREGATE_INDEX_PATH,
        dependency_policy_path=DEPENDENCY_POLICY_PATH,
        direct_writer_baseline_path=DIRECT_WRITER_BASELINE_PATH,
    )


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("generated manifest must be a mapping")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
