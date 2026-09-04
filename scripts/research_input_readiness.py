"""Print read-only research input diagnostics without dispatch or output files."""

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

from ai_trading_system.data.research_input_readiness import (  # noqa: E402
    inspect_research_input_readiness,
    load_research_input_readiness_request,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True, help="显式只读 JSON 核查请求")
    args = parser.parse_args()
    try:
        request = load_research_input_readiness_request(args.request.read_bytes())
        result = inspect_research_input_readiness(request)
    except (OSError, ValueError) as exc:
        result = {
            "schema_version": "research_input_readiness.v1",
            "status": "NOT_READY",
            "blockers": [{"code": "READINESS_REQUEST_INVALID", "detail": str(exc)}],
            "dispatch_allowed": False,
            "consumer_cutover_allowed": False,
            "dq_validation_executed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return 0 if result["status"] == "READY_FOR_REVIEW" else 1


if __name__ == "__main__":
    raise SystemExit(main())
