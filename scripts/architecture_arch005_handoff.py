from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.platform.architecture import (
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
    args = parser.parse_args()
    if args.command == "generate":
        return _generate(args)
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
    )
    write_generated_architecture_artifact(args.output, payload)
    validate_bootstrap_handoff(
        payload,
        project_root=PROJECT_ROOT,
        expected_head_commit=head,
        expected_branch=branch,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def _validate(path: Path) -> int:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise SystemExit("handoff artifact must be a mapping")
    validate_bootstrap_handoff(payload, project_root=PROJECT_ROOT)
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


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
