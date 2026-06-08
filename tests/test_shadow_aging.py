from __future__ import annotations

from pathlib import Path

from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_shadow_shortlist_and_monitoring,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    run_shadow_aging,
    validate_shadow_aging_artifact,
)


def test_shadow_aging_promotion_clock_and_downgrade_recommendation(
    tmp_path: Path,
) -> None:
    fixture = write_shadow_shortlist_and_monitoring(tmp_path, degraded=True)

    aging = run_shadow_aging(
        shadow_shortlist_id=fixture["shadow_shortlist_id"],
        config_path=paper_config_path(tmp_path),
        output_dir=tmp_path / "shadow_aging",
        shadow_shortlist_dir=fixture["shadow_shortlist_dir"],
        shadow_monitor_run_dir=fixture["shadow_monitor_run_dir"],
        consensus_drift_dir=fixture["consensus_drift_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
    )

    by_candidate = {row["candidate_id"]: row for row in aging["candidate_aging_status"]}
    assert by_candidate["candidate-a"]["promotion_clock_status"] == "eligible_for_review"
    assert by_candidate["candidate-b"]["promotion_clock_status"] == "downgrade_recommended"
    assert aging["promotion_clock_v2_summary"]["eligible_for_review_count"] == 1
    assert aging["promotion_clock_v2_summary"]["downgrade_recommended_count"] == 1
    assert aging["manifest"]["production_candidate_generated"] is False
    assert (
        validate_shadow_aging_artifact(
            aging_id=aging["aging_id"],
            output_dir=tmp_path / "shadow_aging",
        )["status"]
        == "PASS"
    )
