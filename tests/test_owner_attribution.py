from __future__ import annotations

from pathlib import Path

from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_daily_advisory,
    write_owner_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    init_paper_portfolio,
    run_owner_attribution,
    track_advisory_outcome,
    validate_owner_attribution_artifact,
)


def test_owner_attribution_links_reviews_and_keeps_insufficient_outcome_data(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_daily_advisory(tmp_path)
    write_owner_review(
        tmp_path,
        daily_advisory_id=advisory["daily_advisory_id"],
        owner_decision="monitor",
    )
    track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
    )

    attribution = run_owner_attribution(
        output_dir=tmp_path / "owner_attribution",
        owner_review_dir=tmp_path / "owner_review_journal",
        outcome_dir=tmp_path / "advisory_outcome",
    )

    assert attribution["owner_decision_summary"]["total_reviews"] == 1
    assert attribution["owner_decision_summary"]["monitor"] == 1
    assert attribution["manifest"]["linked_outcome_count"] == 1
    assert (
        attribution["advisory_acceptance_matrix"]["by_recommended_action"]["manual_review"][
            "accepted_monitor"
        ]
        == 1
    )
    assert attribution["decision_outcome_comparison"]["status"] == "INSUFFICIENT_DATA"
    assert attribution["manifest"]["broker_action_taken"] is False
    assert (
        validate_owner_attribution_artifact(
            attribution_id=attribution["attribution_id"],
            output_dir=tmp_path / "owner_attribution",
        )["status"]
        == "PASS"
    )
