from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.platform.architecture import validate_architecture_dependencies

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate ARCH-004 dependency and IO ratchets")
    parser.add_argument(
        "--policy",
        type=Path,
        default=PROJECT_ROOT / "config/architecture/arch_004c_dependency_policy.yaml",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=PROJECT_ROOT / "inputs/architecture/arch_004c_direct_writer_baseline.yaml",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=PROJECT_ROOT / "src/ai_trading_system",
    )
    args = parser.parse_args()
    report = validate_architecture_dependencies(
        policy_path=args.policy,
        baseline_path=args.baseline,
        source_root=args.source_root,
    )
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if report.status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
