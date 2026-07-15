from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

from ai_trading_system.platform.architecture import (
    assert_frozen_callback_migration_matrix,
    baseline_callbacks_from_matrix,
    build_callback_migration_matrix,
    scan_callback_source,
    write_generated_architecture_artifact,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BASELINE_COMMIT = "7c4cce3e"
BASELINE_SOURCE_PATH = "src/ai_trading_system/cli_commands/etf_portfolio.py"
MATRIX_PATH = PROJECT_ROOT / "inputs/architecture/arch_004g2_callback_migration_matrix.yaml"


def main() -> int:
    parser = argparse.ArgumentParser(description="ARCH-004G2.4 callback migration matrix")
    parser.add_argument("command", choices=("generate", "validate"))
    args = parser.parse_args()

    if MATRIX_PATH.exists():
        tracked = safe_load_yaml_path(MATRIX_PATH)
        if not isinstance(tracked, dict) or not isinstance(tracked.get("baseline"), dict):
            raise RuntimeError(f"invalid tracked callback matrix: {MATRIX_PATH}")
        baseline = tracked["baseline"]
        baseline_callbacks = baseline_callbacks_from_matrix(MATRIX_PATH)
        baseline_commit = str(baseline.get("commit") or "")
        baseline_path = str(baseline.get("path") or "")
        baseline_sha256 = str(baseline.get("sha256") or "")
    else:
        source_bytes = _git_blob(BASELINE_COMMIT, BASELINE_SOURCE_PATH)
        baseline_callbacks = scan_callback_source(
            source_bytes.decode("utf-8"),
            source_path=BASELINE_SOURCE_PATH,
        )
        baseline_commit = BASELINE_COMMIT
        baseline_path = BASELINE_SOURCE_PATH
        baseline_sha256 = hashlib.sha256(source_bytes).hexdigest()

    matrix = build_callback_migration_matrix(
        baseline_callbacks=baseline_callbacks,
        baseline_source_commit=baseline_commit,
        baseline_source_path=baseline_path,
        baseline_source_sha256=baseline_sha256,
        project_root=PROJECT_ROOT,
    )
    if args.command == "generate":
        write_generated_architecture_artifact(MATRIX_PATH, matrix)
    else:
        assert_frozen_callback_migration_matrix(matrix, baseline_path=MATRIX_PATH)
    print(
        json.dumps(
            {
                "status": "PASS",
                "matrix_path": str(MATRIX_PATH),
                "matrix_id": matrix["matrix_id"],
                "summary": matrix["summary"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def _git_blob(commit: str, path: str) -> bytes:
    completed = subprocess.run(
        ["git", "show", f"{commit}:{path}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    )
    return completed.stdout


if __name__ == "__main__":
    raise SystemExit(main())
