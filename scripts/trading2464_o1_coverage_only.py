from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from ai_trading_system.research_framework.plugins.o1_relative_opportunity_coverage import (
    run_o1_coverage_only,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="执行 TRADING-2464 O1 coverage-only gate；不训练模型。",
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--generated-at", type=datetime.fromisoformat, required=True)
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    parser.add_argument("--source-commit-sha")
    args = parser.parse_args()

    result = run_o1_coverage_only(
        output_root=args.output_root,
        project_root=args.project_root,
        generated_at=args.generated_at,
        source_commit_sha=args.source_commit_sha,
        cli_argv=tuple(sys.argv),
    )
    print(
        json.dumps(
            {
                "status": result.gate["status"],
                "gate_id": result.gate["gate_id"],
                "gate_path": result.gate_path.as_posix(),
                "report_id": result.report["report_id"],
                "report_path": result.report_path.as_posix(),
                "mechanical_classification": result.gate[
                    "mechanical_classification"
                ],
                "next_authorization": result.gate["next_authorization"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
