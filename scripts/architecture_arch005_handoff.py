from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.platform.architecture import (
    bootstrap_handoff_checksum,
    build_bootstrap_handoff,
    validate_bootstrap_handoff,
    write_generated_architecture_artifact,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "inputs/architecture/arch_005_bootstrap_handoff.yaml"


def main() -> int:
    parser = argparse.ArgumentParser(description="ARCH-005 bootstrap handoff control")
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate = subparsers.add_parser("generate")
    generate.add_argument("--base-commit", required=True)
    generate.add_argument("--focused-artifact", type=Path, required=True)
    generate.add_argument("--architecture-artifact", type=Path, required=True)
    generate.add_argument("--contract-artifact", type=Path, required=True)
    generate.add_argument("--full-artifact", type=Path, required=True)
    generate.add_argument("--known-unrelated-file", action="append", default=[])
    generate.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    validate = subparsers.add_parser("validate")
    validate.add_argument("--input", type=Path, default=DEFAULT_OUTPUT)
    repair = subparsers.add_parser("repair-hash-basis")
    repair.add_argument("--input", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    if args.command == "generate":
        return _generate(args)
    if args.command == "repair-hash-basis":
        return _repair_hash_basis(args.input)
    return _validate(args.input)


def _generate(args: argparse.Namespace) -> int:
    head = _git("rev-parse", "HEAD")
    branch = _git("branch", "--show-current")
    upstream = _git("rev-parse", "@{u}")
    if head != upstream:
        raise SystemExit(f"source HEAD is not pushed: head={head} upstream={upstream}")
    status = _git("status", "--porcelain")
    if status:
        raise SystemExit("source worktree must be clean before generating handoff")
    frozen = _frozen_tracked_files_for_paths(head, _tracked_paths())
    payload = build_bootstrap_handoff(
        project_root=PROJECT_ROOT,
        head_commit=head,
        base_commit=args.base_commit,
        branch=branch,
        validation_artifacts={
            "focused": args.focused_artifact,
            "architecture_fitness": args.architecture_artifact,
            "contract_validation": args.contract_artifact,
            "full_validation": args.full_artifact,
        },
        known_unrelated_worktree_files=args.known_unrelated_file,
        generated_at=datetime.now(UTC),
        frozen_tracked_files=frozen,
    )
    write_generated_architecture_artifact(args.output, payload)
    validate_bootstrap_handoff(
        payload,
        project_root=PROJECT_ROOT,
        expected_head_commit=head,
        expected_branch=branch,
        frozen_tracked_files=frozen,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _validate(path: Path) -> int:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise SystemExit("handoff artifact must be a mapping")
    validate_bootstrap_handoff(
        payload,
        project_root=PROJECT_ROOT,
        frozen_tracked_files=_frozen_tracked_files(payload),
    )
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "status": "PASS",
                "path": str(path),
                "head_commit": payload["head_commit"],
                "next_slice_unblocked": payload["next_slice_unblocked"],
                "production_effect": payload["production_effect"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _repair_hash_basis(path: Path) -> int:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise SystemExit("handoff artifact must be a mapping")
    frozen = _frozen_tracked_files(payload)
    payload["tracked_file_hash_basis"] = "source_commit_git_blob_sha256"
    matrix = payload["migration_matrix"]
    architecture_state = payload["architecture_state"]
    attribution = payload["worktree_attribution"]
    if not isinstance(matrix, dict) or not isinstance(architecture_state, dict):
        raise SystemExit("handoff tracked references must be mappings")
    if not isinstance(attribution, dict):
        raise SystemExit("handoff attribution must be a mapping")
    matrix_path = str(matrix["path"])
    matrix["sha256"] = hashlib.sha256(frozen[matrix_path]).hexdigest()
    for record in architecture_state.values():
        if not isinstance(record, dict):
            raise SystemExit("handoff architecture state record must be a mapping")
        relative = str(record["path"])
        record["sha256"] = hashlib.sha256(frozen[relative]).hexdigest()
    attribution_path = str(attribution["attribution_path"])
    attribution["attribution_sha256"] = hashlib.sha256(
        frozen[attribution_path]
    ).hexdigest()
    payload["handoff_checksum"] = bootstrap_handoff_checksum(payload)
    write_generated_architecture_artifact(path, payload)
    validate_bootstrap_handoff(
        payload,
        project_root=PROJECT_ROOT,
        frozen_tracked_files=frozen,
    )
    print(
        json.dumps(
            {
                "schema_version": payload["schema_version"],
                "status": "PASS_REPAIRED_HASH_BASIS",
                "path": str(path),
                "head_commit": payload["head_commit"],
                "tracked_file_hash_basis": payload["tracked_file_hash_basis"],
                "handoff_checksum": payload["handoff_checksum"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _frozen_tracked_files(payload: dict[str, object]) -> dict[str, bytes]:
    head = str(payload["head_commit"])
    matrix = payload["migration_matrix"]
    architecture_state = payload["architecture_state"]
    attribution = payload["worktree_attribution"]
    if not isinstance(matrix, dict) or not isinstance(architecture_state, dict):
        raise SystemExit("handoff tracked references must be mappings")
    if not isinstance(attribution, dict):
        raise SystemExit("handoff attribution must be a mapping")
    paths = {str(matrix["path"]), str(attribution["attribution_path"])}
    for record in architecture_state.values():
        if not isinstance(record, dict):
            raise SystemExit("handoff architecture state record must be a mapping")
        paths.add(str(record["path"]))
    return _frozen_tracked_files_for_paths(head, paths)


def _tracked_paths() -> set[str]:
    return {
        "inputs/architecture/arch_004g2_callback_migration_matrix.yaml",
        "inputs/architecture/arch_004e_module_manifest.yaml",
        "inputs/architecture/arch_004e_test_manifest.yaml",
        "inputs/architecture/arch_004_compatibility_baseline.yaml",
        "inputs/architecture/arch_004g_deprecation_inventory.yaml",
        "inputs/architecture/arch_004_worktree_attribution.yaml",
    }


def _frozen_tracked_files_for_paths(
    head: str,
    paths: set[str],
) -> dict[str, bytes]:
    result: dict[str, bytes] = {}
    for relative in sorted(paths):
        process = subprocess.run(
            ["git", "show", f"{head}:{relative}"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
        )
        result[relative] = process.stdout
    return result


if __name__ == "__main__":
    raise SystemExit(main())
