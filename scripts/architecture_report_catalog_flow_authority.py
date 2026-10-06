from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.platform.architecture.report_catalog_flow_authority import (
    ReportCatalogFlowAuthorityError,
    build_repository_authority,
    reseal_policy_seals,
    validate_repository_authority,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build or validate report/catalog/flow lossless fragment shadow"
    )
    parser.add_argument("command", choices=("build", "validate", "reseal"))
    parser.add_argument("--repository-root", type=Path, default=Path("."))
    parser.add_argument(
        "--target", action="append", default=[],
        help="reseal only: a target id (repeatable); default every target",
    )
    parser.add_argument(
        "--write", action="store_true",
        help="reseal only: rewrite the seal lines; without it the command only reports",
    )
    return parser


def main() -> int:
    arguments = _parser().parse_args()
    try:
        if arguments.command == "build":
            result = build_repository_authority(arguments.repository_root, write=True)
        elif arguments.command == "reseal":
            result = reseal_policy_seals(
                arguments.repository_root, targets=tuple(arguments.target), write=arguments.write,
            )
        else:
            result = validate_repository_authority(arguments.repository_root)
    except ReportCatalogFlowAuthorityError as exc:
        print(json.dumps({"status": "FAIL", "code": exc.code, "detail": exc.detail}))
        return 1
    summary = {
        key: value
        for key, value in result.items()
        if key not in {"fragment_paths", "fragment_sha256"}
    }
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
