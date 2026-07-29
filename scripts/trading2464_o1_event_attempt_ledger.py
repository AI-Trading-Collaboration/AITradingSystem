from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from ai_trading_system.data.o1_relative_opportunity_event_attempt_ledger import (
    freeze_o1_event_and_attempt_ledgers,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="冻结 TRADING-2464 O1 official event lineage 与 pre-result attempt ledger。",
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--generated-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    args = parser.parse_args()

    result = freeze_o1_event_and_attempt_ledgers(
        output_root=args.output_root,
        project_root=args.project_root,
        generated_at=args.generated_at,
        cli_argv=tuple(sys.argv),
    )
    print(
        json.dumps(
            {
                "status": result.status,
                "gate_id": result.gate["gate_id"],
                "gate_path": result.gate_path.as_posix(),
                "source_manifest_path": result.source_manifest_path.as_posix(),
                "attempt_ledger_path": result.attempt_ledger_path.as_posix(),
                "event_ledger_path": (
                    None
                    if result.event_ledger_path is None
                    else result.event_ledger_path.as_posix()
                ),
                "mechanical_classification": result.gate["mechanical_classification"],
                "next_authorization": result.gate["next_authorization"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
