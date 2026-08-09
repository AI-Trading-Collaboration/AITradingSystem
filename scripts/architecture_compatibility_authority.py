from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.platform.architecture.compatibility_authority import (
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
