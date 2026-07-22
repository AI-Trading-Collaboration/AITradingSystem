from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.platform.architecture.task_portfolio_normalization import (
    build_normalization_decision_manifest,
    load_normalization_policy,
    validate_normalization_decision_manifest,
)
from ai_trading_system.platform.artifacts import write_json_atomic

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = PROJECT_ROOT / "config/governance/gov_006_wave1_normalization.yaml"
MANIFEST_PATH = PROJECT_ROOT / "inputs/governance/gov_006_wave1_decision_manifest.json"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="GOV-006 active task portfolio normalization dry-run control"
    )
    parser.add_argument("command", choices=("generate", "validate"))
    args = parser.parse_args()
    policy = load_normalization_policy(POLICY_PATH)
    if args.command == "generate":
        payload = build_normalization_decision_manifest(
            project_root=PROJECT_ROOT,
            policy=policy,
            policy_path=POLICY_PATH,
        )
        write_json_atomic(MANIFEST_PATH, payload)
    else:
        payload = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        validate_normalization_decision_manifest(
            payload,
            project_root=PROJECT_ROOT,
            policy=policy,
            policy_path=POLICY_PATH,
        )
    print(
        json.dumps(
            {
                "status": "PASS",
                "mode": "DRY_RUN_ONLY",
                "manifest_path": str(MANIFEST_PATH),
                "manifest_id": payload["manifest_id"],
                "manifest_sha256": payload["manifest_sha256"],
                "base_commit": payload["source"]["base_commit"],
                "decision_count": payload["decision_count"],
                "automatic_apply_allowed": False,
                "production_effect": "none",
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
