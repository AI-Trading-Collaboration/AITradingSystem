from __future__ import annotations

import argparse
import json
from pathlib import Path

from ai_trading_system.data.price_issue_attribution_review_pack import (
    DEFAULT_JSON_PATH,
    DEFAULT_MARKDOWN_PATH,
    DEFAULT_VALIDATION_PATH,
    load_and_validate_price_issue_attribution_review_pack,
    write_price_issue_attribution_review_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build or validate the DATA-GOV-002C2P price non-market-session "
            "source-owner review pack."
        )
    )
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    parser.add_argument("--validation-output", type=Path)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    repo_root = args.repo_root.resolve()
    json_path = args.json_output if args.json_output is not None else repo_root / DEFAULT_JSON_PATH
    if args.check:
        validation = load_and_validate_price_issue_attribution_review_pack(
            repo_root=repo_root,
            pack_path=json_path,
        )
        print(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if validation["status"] == "PASS" else 1

    markdown_path = (
        args.markdown_output
        if args.markdown_output is not None
        else repo_root / DEFAULT_MARKDOWN_PATH
    )
    validation_path = (
        args.validation_output
        if args.validation_output is not None
        else repo_root / DEFAULT_VALIDATION_PATH
    )
    result = write_price_issue_attribution_review_artifacts(
        repo_root=repo_root,
        json_path=json_path,
        markdown_path=markdown_path,
        validation_path=validation_path,
    )
    print(
        json.dumps(
            {
                "status": result["validation"]["status"],
                "review_pack_id": result["pack"]["review_pack_id"],
                "summary": result["pack"]["summary"],
                "paths": result["paths"],
                "production_effect": "none",
                "broker_action": "none",
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if result["validation"]["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
