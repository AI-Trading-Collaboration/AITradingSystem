from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from ai_trading_system.platform.validation_parent_run_import import (  # noqa: E402
    ValidationParentRunImportError,
    build_parent_run_import,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a same-byte portable import proof for one copied formal Full parent run."
        )
    )
    parser.add_argument(
        "--parent-run",
        required=True,
        type=Path,
        help=(
            "Current repository path to "
            "outputs/validation_runtime/<run_id>/test_runtime_summary.json."
        ),
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=PROJECT_ROOT,
        help="Repository root that owns outputs/validation_runtime (default: script project root).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        result = build_parent_run_import(
            args.parent_run,
            repo_root=args.repo_root,
        )
    except ValidationParentRunImportError as exc:
        parser.error(str(exc))
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
