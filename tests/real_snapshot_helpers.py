from __future__ import annotations

from pathlib import Path
from typing import Any

from manual_portfolio_guardrail_helpers import (
    consensus_candidate_weights,
    write_manual_snapshot,
    write_shadow_shortlist,
)

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    apply_real_snapshot_paper_action,
    create_real_execution_owner_review,
    intake_real_snapshot,
    record_real_execution_owner_decision,
    run_real_snapshot_dry_run,
)


def real_snapshot_dry_run_fixture(tmp_path: Path) -> dict[str, Any]:
    snapshot_path = write_manual_snapshot(tmp_path)
    intake = intake_real_snapshot(
        snapshot_path=snapshot_path,
        output_dir=tmp_path / "real_snapshot_intake",
        manual_snapshot_output_dir=tmp_path / "manual_portfolio_snapshot",
    )
    shadow = write_shadow_shortlist(tmp_path, consensus_candidate_weights())
    dry_run = run_real_snapshot_dry_run(
        snapshot_intake_id=intake["snapshot_intake_id"],
        shadow_shortlist_id=shadow["shadow_shortlist_id"],
        intake_dir=tmp_path / "real_snapshot_intake",
        manual_snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        exposure_dir=tmp_path / "portfolio_exposure",
        drift_dir=tmp_path / "position_drift",
        guardrail_dir=tmp_path / "execution_guardrails",
        manual_review_dir=tmp_path / "manual_execution_review",
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "real_snapshot_dry_run",
    )
    return {"intake": intake, "shadow": shadow, "dry_run": dry_run}


def real_snapshot_owner_review_fixture(
    tmp_path: Path,
    *,
    decision: str = "monitor",
) -> dict[str, Any]:
    fixture = real_snapshot_dry_run_fixture(tmp_path)
    owner_review = create_real_execution_owner_review(
        dry_run_id=fixture["dry_run"]["dry_run_id"],
        dry_run_dir=tmp_path / "real_snapshot_dry_run",
        output_dir=tmp_path / "real_execution_owner_review",
    )
    recorded = record_real_execution_owner_decision(
        review_id=owner_review["review_id"],
        decision=decision,
        output_dir=tmp_path / "real_execution_owner_review",
    )
    fixture["owner_review"] = owner_review
    fixture["recorded_owner_review"] = recorded
    return fixture


def real_snapshot_paper_action_fixture(
    tmp_path: Path,
    *,
    decision: str = "monitor",
) -> dict[str, Any]:
    fixture = real_snapshot_owner_review_fixture(tmp_path, decision=decision)
    paper_action = apply_real_snapshot_paper_action(
        owner_review_id=fixture["owner_review"]["review_id"],
        owner_review_dir=tmp_path / "real_execution_owner_review",
        dry_run_dir=tmp_path / "real_snapshot_dry_run",
        manual_snapshot_dir=tmp_path / "manual_portfolio_snapshot",
        drift_dir=tmp_path / "position_drift",
        guardrail_dir=tmp_path / "execution_guardrails",
        output_dir=tmp_path / "real_snapshot_paper_action",
    )
    fixture["paper_action"] = paper_action
    return fixture


def report_index_for_real_snapshot_review(fixture: dict[str, Any]) -> dict[str, Any]:
    paths = {
        "etf_dynamic_v3_real_snapshot_dry_run": (
            fixture["dry_run"]["dry_run_dir"] / "real_snapshot_dry_run_manifest.json"
        ),
        "etf_dynamic_v3_real_execution_owner_review": (
            fixture["owner_review"]["review_dir"] / "real_execution_owner_review_manifest.json"
        ),
        "etf_dynamic_v3_real_snapshot_paper_action": (
            fixture["paper_action"]["paper_action_dir"]
            / "real_snapshot_paper_action_manifest.json"
        ),
        "etf_dynamic_v3_weekly_real_snapshot_review": (
            fixture["weekly_review"]["weekly_real_review_dir"]
            / "weekly_real_snapshot_review_manifest.json"
        ),
    }
    return {
        "reports": [
            {
                "report_id": report_id,
                "latest_artifact_path": str(path),
                "status": "FRESH",
                "freshness_status": "FRESH",
            }
            for report_id, path in paths.items()
        ]
    }
