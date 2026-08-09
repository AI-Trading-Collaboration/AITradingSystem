from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.platform.architecture.report_catalog_flow_authority import (
    ReportCatalogFlowAuthorityError,
    build_repository_authority,
    validate_repository_authority,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build or validate report/catalog/flow lossless fragment shadow"
    )
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
