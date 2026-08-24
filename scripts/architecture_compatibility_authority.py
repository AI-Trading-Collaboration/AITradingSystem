from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPOSITORY_ROOT / "src"
if str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

from ai_trading_system.platform.architecture.compatibility_authority import (  # noqa: E402
    CompatibilityAuthorityError,
    build_repository_authority,
    validate_repository_authority,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build or validate compatibility authority")
    parser.add_argument("command", choices=("build", "validate"))
    parser.add_argument("--repository-root", type=Path, default=Path("."))
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        if arguments.command == "build":
            result = build_repository_authority(arguments.repository_root, write=True)
        else:
            result = validate_repository_authority(arguments.repository_root)
    except CompatibilityAuthorityError as exc:
        print(json.dumps({"status": "FAIL", "code": exc.code, "detail": exc.detail}))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
