from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_daily_advisory,
    write_owner_review,
    write_shadow_shortlist_and_monitoring,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    init_paper_portfolio,
    run_owner_attribution,
    run_shadow_aging,
    run_weekly_advisory_review,
    track_advisory_outcome,
    validate_weekly_advisory_review_artifact,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
)
from ai_trading_system.reports import reader_brief


def test_weekly_advisory_review_aggregates_and_flows_to_reader_brief(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    paper = init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_daily_advisory(tmp_path, as_of="2026-06-08")
    write_owner_review(
        tmp_path,
        daily_advisory_id=advisory["daily_advisory_id"],
        owner_decision="monitor",
        as_of="2026-06-08",
    )
    outcome = track_advisory_outcome(
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
    shadow_fixture = write_shadow_shortlist_and_monitoring(tmp_path, degraded=False)
    aging = run_shadow_aging(
        shadow_shortlist_id=shadow_fixture["shadow_shortlist_id"],
        config_path=config_path,
        output_dir=tmp_path / "shadow_aging",
        shadow_shortlist_dir=shadow_fixture["shadow_shortlist_dir"],
        shadow_monitor_run_dir=shadow_fixture["shadow_monitor_run_dir"],
        consensus_drift_dir=shadow_fixture["consensus_drift_dir"],
        advisory_outcome_dir=tmp_path / "advisory_outcome",
    )

    weekly = run_weekly_advisory_review(
        week_ending=date(2026, 6, 14),
        output_dir=tmp_path / "weekly_advisory_review",
        shadow_monitor_run_dir=shadow_fixture["shadow_monitor_run_dir"],
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        owner_review_dir=tmp_path / "owner_review_journal",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        shadow_aging_dir=tmp_path / "shadow_aging",
    )

    assert weekly["manifest"]["status"] == "PASS"
    assert weekly["manifest"]["paper_portfolio_status"] == "ACTIVE"
    assert weekly["weekly_owner_decision_summary"]["monitor"] == 1
    assert "manual_review_required" in weekly["manifest"]["next_actions"]
    assert (
        validate_weekly_advisory_review_artifact(
            weekly_review_id=weekly["weekly_review_id"],
            output_dir=tmp_path / "weekly_advisory_review",
        )["status"]
        == "PASS"
    )

    leaderboard_path = _write_minimal_leaderboard(tmp_path)
    summary = reader_brief._etf_dynamic_v3_parameter_research_summary(
        {
            "reports": [
                _report_record("etf_dynamic_v3_parameter_sweep_leaderboard", leaderboard_path),
                _report_record(
                    "etf_dynamic_v3_paper_portfolio",
                    tmp_path
                    / "paper_portfolio"
                    / paper["paper_portfolio_id"]
                    / "paper_portfolio_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_advisory_outcome",
                    tmp_path
                    / "advisory_outcome"
                    / outcome["outcome_id"]
                    / "advisory_outcome_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_owner_attribution",
                    tmp_path
                    / "owner_attribution"
                    / attribution["attribution_id"]
                    / "owner_attribution_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_shadow_aging",
                    tmp_path / "shadow_aging" / aging["aging_id"] / "shadow_aging_manifest.json",
                ),
                _report_record(
                    "etf_dynamic_v3_weekly_advisory_review",
                    tmp_path
                    / "weekly_advisory_review"
                    / weekly["weekly_review_id"]
                    / "weekly_review_manifest.json",
                ),
            ]
        }
    )

    assert summary["paper_portfolio_status"] == "ACTIVE"
    assert summary["advisory_outcome_status"] == "PENDING"
    assert summary["owner_attribution_total_reviews"] == 1
    assert summary["shadow_aging_eligible_for_review_count"] == 2
    assert summary["weekly_advisory_recommendation"] == weekly["manifest"]["weekly_recommendation"]
    assert summary["paper_portfolio_broker_action_taken"] is False


def _write_minimal_leaderboard(tmp_path: Path) -> Path:
    path = tmp_path / "leaderboard.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "status": "completed",
                "candidate_count": 2,
                "evaluator_mode": "real_dynamic_v3_rescue",
                "evaluator_version": "test",
                "metrics_source": "real_evaluation_artifact",
                "not_for_investment_decision": False,
                "production_candidate_generated": False,
                "automatic_candidate_promotion": False,
                "shadow_enrollment_allowed": False,
                "top_eligible_candidates": [
                    {"candidate_id": "candidate-a", "gate": "OBSERVE_ONLY", "score": 1.0}
                ],
                "most_common_reject_reasons": [],
                "recommended_next_actions": ["owner_review_required"],
                "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-08",
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }
