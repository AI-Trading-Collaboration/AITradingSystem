from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from ai_trading_system.atlas.cited_query_renderer import (
    build_cited_query_showcase,
    write_cited_query_artifacts,
)
from ai_trading_system.atlas.live_snapshot import build_live_snapshot_bundle
from ai_trading_system.atlas.page_effectiveness import (
    build_page_task_coverage,
    load_page_effectiveness_policy,
    repository_head,
)
from ai_trading_system.platform.architecture.task_registry_canonical import (
    validate_canonical_registry,
)

DEFAULT_OUTPUT = "outputs/atlas/strategy_research_cited_query/trading_2470_v1"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="从 canonical task registry 重建 Atlas 策略研究 live 页面。"
    )
    parser.add_argument("--repository-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--exact-commit")
    parser.add_argument("--output-directory", default=DEFAULT_OUTPUT)
    return parser


def _require_committed_sources(*, root: Path, exact_commit: str) -> None:
    policy = load_page_effectiveness_policy(repository_root=root)
    registry = validate_canonical_registry(project_root=root)
    coverage = build_page_task_coverage(root=root, policy=policy, registry=registry)
    paths = tuple(
        dict.fromkeys(
            (
                *policy.relevant_source_paths,
                *(item.requirement_path for item in coverage),
                *(item.task_fragment_path for item in coverage),
            )
        )
    )
    for path in paths:
        tracked = subprocess.run(
            ["git", "cat-file", "-e", f"{exact_commit}:{path}"],
            cwd=root,
            capture_output=True,
        )
        if tracked.returncode != 0:
            raise SystemExit(f"ATLAS_PAGE_SOURCE_NOT_IN_EXACT_COMMIT:{path}")
        clean = subprocess.run(
            ["git", "diff", "--quiet", exact_commit, "--", path],
            cwd=root,
            capture_output=True,
        )
        if clean.returncode != 0:
            raise SystemExit(f"ATLAS_PAGE_SOURCE_WORKTREE_DRIFT:{path}")


def main() -> int:
    args = _parser().parse_args()
    root = args.repository_root.resolve()
    head = repository_head(root)
    exact_commit = args.exact_commit or head
    if exact_commit != head:
        raise SystemExit(
            f"ATLAS_PAGE_EXACT_COMMIT_NOT_HEAD:exact={exact_commit}:head={head}"
        )
    _require_committed_sources(root=root, exact_commit=exact_commit)
    bundle = build_live_snapshot_bundle(repository_root=root, exact_commit=exact_commit)
    showcase = build_cited_query_showcase(
        target_ids=bundle.target_ids,
        snapshot_payload=bundle.current_snapshot.to_dict(),
        before_payload=bundle.comparison_snapshot.to_dict(),
        after_payload=bundle.current_snapshot.to_dict(),
        diff_payload=bundle.current_diff.to_dict(),
        repository_root=root,
    )
    output = (root / str(args.output_directory)).resolve()
    try:
        output.relative_to(root)
    except ValueError as exc:
        raise SystemExit("ATLAS_PAGE_OUTPUT_OUTSIDE_REPOSITORY") from exc
    artifacts = write_cited_query_artifacts(showcase, output)
    print(
        json.dumps(
            {
                "status": "PASS",
                "source_commit": exact_commit,
                "research_state_as_of": bundle.research_state_as_of,
                "evidence_evaluated_at": bundle.evidence_evaluated_at,
                "page_source_commit_at": bundle.page_source_commit_at,
                "comparison_snapshot_id": bundle.comparison_snapshot.snapshot_id,
                "current_snapshot_id": bundle.current_snapshot.snapshot_id,
                "current_diff_id": bundle.current_diff.diff_id,
                "artifact_count": len(artifacts),
                "output_directory": output.as_posix(),
                "investment_conclusion_generated": False,
                "production_effect": "none",
                "broker_action": "none",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
