from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.platform.architecture import (
    assert_frozen_cli_contract,
    build_cli_contract,
    write_generated_architecture_artifact,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_PATH = PROJECT_ROOT / "src/ai_trading_system/cli_commands/etf_portfolio.py"
BASELINE_PATH = PROJECT_ROOT / "inputs/architecture/arch_004g2_etf_cli_contract.yaml"


def main() -> int:
    parser = argparse.ArgumentParser(description="ARCH-004G2 ETF CLI contract control")
    parser.add_argument("command", choices=("generate", "validate"))
    args = parser.parse_args()
    contract = build_cli_contract(
        etf_app,
        source_path=SOURCE_PATH,
        project_root=PROJECT_ROOT,
    )
    if args.command == "generate":
        write_generated_architecture_artifact(BASELINE_PATH, contract)
    else:
        assert_frozen_cli_contract(contract, baseline_path=BASELINE_PATH)
    print(
        json.dumps(
            {
                "status": "PASS",
                "baseline_path": str(BASELINE_PATH),
                "counts": contract["counts"],
                "tree_sha256": contract["tree_sha256"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
