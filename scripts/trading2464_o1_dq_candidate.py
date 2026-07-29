from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from ai_trading_system.data.o1_relative_opportunity_dq_candidate import (
    materialize_and_validate_o1_candidate,
    resume_existing_o1_candidate,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="物化 TRADING-2464 隔离候选区并在严格 DQ 后停止。",
    )
    parser.add_argument("--source-project-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--generated-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument(
        "--resume-existing",
        action="store_true",
        help="只复验已物化候选及其唯一 DQ receipt，并补写缺失的 O1 gate。",
    )
    args = parser.parse_args()

    runner = (
        resume_existing_o1_candidate
        if args.resume_existing
        else materialize_and_validate_o1_candidate
    )
    result = runner(
        source_project_root=args.source_project_root,
        output_root=args.output_root,
        project_root=args.project_root,
        generated_at=args.generated_at,
    )
    print(
        json.dumps(
            {
                "status": result.gate["status"],
                "gate_id": result.gate["gate_id"],
                "gate_path": result.gate_path.as_posix(),
                "candidate_project_root": result.candidate_project_root.as_posix(),
                "receipt_id": result.gate["fresh_data_quality"]["receipt_id"],
                "claim_boundary": result.gate["claim_boundary"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
