"""Print Full dependency readiness; never dispatch validation or repair files."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = str(ROOT / "src")
if SOURCE in sys.path:
    sys.path.remove(SOURCE)
sys.path.insert(0, SOURCE)

from ai_trading_system.platform.architecture.validation_readiness import (  # noqa: E402
    check_full_readiness,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="只读核查Full的现有证据及最终候选绑定。")
    parser.add_argument("--repository-root", type=Path, required=True)
    parser.add_argument("--candidate-sha", required=True)
    args = parser.parse_args(argv)
    result = check_full_readiness(args.repository_root, args.candidate_sha)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
