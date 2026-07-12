from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_outcome_loop_helpers import run_safe_update_fixture
from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_market_cache,
    write_validated_owner_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DynamicV3PaperTrackingError,
    init_paper_portfolio,
    run_owner_attribution,
    run_weekly_advisory_review,
    track_advisory_outcome,
    update_advisory_outcome,
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
    paper = init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 9, tzinfo=UTC),
    )
    review = write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
    )
    outcome = track_advisory_outcome(
        daily_advisory_id=review["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=review["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    attribution = run_owner_attribution(
        output_dir=tmp_path / "owner_attribution",
        owner_review_dir=tmp_path / "owner_review_journal",
        outcome_dir=tmp_path / "advisory_outcome",
    )
    weekly = run_weekly_advisory_review(
        week_ending=date(2026, 6, 14),
        output_dir=tmp_path / "weekly_advisory_review",
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        owner_review_dir=tmp_path / "owner_review_journal",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        shadow_aging_dir=tmp_path / "shadow_aging",
        config_path=config_path,
        generated_at=datetime(2026, 6, 14, 18, tzinfo=UTC),
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
    assert summary["shadow_aging_eligible_for_review_count"] == 0
    assert summary["weekly_advisory_recommendation"] == weekly["manifest"]["weekly_recommendation"]
    assert summary["paper_portfolio_broker_action_taken"] is False


def test_weekly_review_missing_outcome_stays_null_and_requires_review(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 9, tzinfo=UTC),
    )
    write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
    )

    weekly = _run_weekly(tmp_path, config_path=config_path)

    outcome = weekly["weekly_advisory_summary"]["outcome_summary"]
    assert outcome["linked_outcome_count"] == 0
    assert outcome["available_window_count"] == 0
    assert outcome["avg_relative_to_no_trade"] is None
    assert weekly["manifest"]["evidence_status"] == "INSUFFICIENT_EVIDENCE"
    assert weekly["manifest"]["weekly_recommendation"] == "manual_review_required"


def test_weekly_review_rejects_ambiguous_paper_portfolios_before_output(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    for hour in (8, 9):
        init_paper_portfolio(
            config_path=config_path,
            output_dir=tmp_path / "paper_portfolio",
            generated_at=datetime(2026, 6, 8, hour, tzinfo=UTC),
        )
    write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
    )

    with pytest.raises(DynamicV3PaperTrackingError, match="ambiguous paper portfolio"):
        _run_weekly(tmp_path, config_path=config_path)
    assert not (tmp_path / "weekly_advisory_review").exists()


def test_weekly_review_rejects_duplicate_daily_as_of_before_output(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 8, tzinfo=UTC),
    )
    write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
        generated_at=datetime(2026, 6, 8, 9, tzinfo=UTC),
    )
    second = write_validated_owner_review(
        tmp_path / "second",
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
        generated_at=datetime(2026, 6, 8, 14, tzinfo=UTC),
    )
    second_daily_source = second["daily_advisory_dir"] / second["daily_advisory_id"]
    second_manifest = json.loads(
        (second_daily_source / "daily_advisory_manifest.json").read_text(encoding="utf-8")
    )
    shutil.copytree(
        second_daily_source,
        tmp_path / "position_advisory_daily" / second["daily_advisory_id"],
    )
    second_monitor_id = second_manifest["source_shadow_monitor_run_id"]
    shutil.copytree(
        tmp_path / "second" / "shadow_monitor_runs" / second_monitor_id,
        tmp_path / "shadow_monitor_runs" / second_monitor_id,
    )

    with pytest.raises(DynamicV3PaperTrackingError, match="as-of dates must be unique"):
        _run_weekly(tmp_path, config_path=config_path)
    assert not (tmp_path / "weekly_advisory_review").exists()


def test_weekly_review_excludes_post_week_generated_daily_source(tmp_path: Path) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 8, tzinfo=UTC),
    )
    write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
        generated_at=datetime(2026, 6, 8, 9, tzinfo=UTC),
    )
    future = write_validated_owner_review(
        tmp_path / "future",
        owner_decision="monitor",
        as_of=date(2026, 6, 9),
        generated_at=datetime(2026, 6, 20, 9, tzinfo=UTC),
    )
    future_daily_source = future["daily_advisory_dir"] / future["daily_advisory_id"]
    future_manifest = json.loads(
        (future_daily_source / "daily_advisory_manifest.json").read_text(encoding="utf-8")
    )
    shutil.copytree(
        future_daily_source,
        tmp_path / "position_advisory_daily" / future["daily_advisory_id"],
    )
    future_monitor_id = future_manifest["source_shadow_monitor_run_id"]
    shutil.copytree(
        tmp_path / "future" / "shadow_monitor_runs" / future_monitor_id,
        tmp_path / "shadow_monitor_runs" / future_monitor_id,
    )

    weekly = _run_weekly(tmp_path, config_path=config_path)

    assert weekly["manifest"]["daily_advisory_count"] == 1
    assert weekly["manifest"]["shadow_monitor_run_count"] == 1


def test_weekly_review_invalid_selected_daily_fails_before_output(tmp_path: Path) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 8, tzinfo=UTC),
    )
    review = write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
    )
    actions_path = (
        review["daily_advisory_dir"]
        / review["daily_advisory_id"]
        / "daily_advisory_actions.json"
    )
    actions = json.loads(actions_path.read_text(encoding="utf-8"))
    actions["recommended_action"] = "no_trade"
    actions_path.write_text(json.dumps(actions, sort_keys=True), encoding="utf-8")

    with pytest.raises(DynamicV3PaperTrackingError, match="validation must PASS"):
        _run_weekly(tmp_path, config_path=config_path)
    assert not (tmp_path / "weekly_advisory_review").exists()


def test_weekly_review_uses_cutoff_outcome_prefix_not_post_week_update(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = run_safe_update_fixture(tmp_path, monkeypatch)
    later_prices, later_rates = write_market_cache(
        tmp_path / "later_market_cache",
        start="2026-06-08",
        end="2026-06-20",
    )
    update_advisory_outcome(
        as_of=date(2026, 6, 20),
        outcome_id=fixture["outcome"]["outcome_id"],
        output_dir=tmp_path / "advisory_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=later_prices,
        rates_path=later_rates,
        generated_at=datetime(2026, 6, 20, 18, tzinfo=UTC),
    )

    weekly = _run_weekly(tmp_path, config_path=fixture["config_path"])

    selected = weekly["source_snapshot"]["selected_outcomes"][0]
    assert len(selected["update_event_prefix"]) == 1
    assert weekly["manifest"]["available_outcome_window_count"] == 1
    assert weekly["manifest"]["evidence_status"] == "COMPLETE_EVIDENCE"
    assert weekly["manifest"]["weekly_recommendation"] == "continue_monitoring"
    validation = validate_weekly_advisory_review_artifact(
        weekly_review_id=weekly["weekly_review_id"],
        output_dir=tmp_path / "weekly_advisory_review",
    )
    assert validation["status"] == "PASS"


def test_weekly_review_rehashed_snapshot_tamper_fails_validation(tmp_path: Path) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 9, tzinfo=UTC),
    )
    write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
    )
    weekly = _run_weekly(tmp_path, config_path=config_path)
    artifact_dir = Path(weekly["weekly_review_dir"])
    snapshot_path = artifact_dir / "weekly_review_source_snapshot.json"
    manifest_path = artifact_dir / "weekly_review_manifest.json"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["selected_daily_advisories"][0]["manifest"]["status"] = "FAIL"
    snapshot_path.write_text(json.dumps(snapshot, sort_keys=True), encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["source_snapshot_checksum"] = hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")

    validation = validate_weekly_advisory_review_artifact(
        weekly_review_id=weekly["weekly_review_id"],
        output_dir=tmp_path / "weekly_advisory_review",
    )
    assert validation["status"] == "FAIL"


def test_weekly_review_report_and_reader_brief_tamper_fail_validation(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 9, tzinfo=UTC),
    )
    write_validated_owner_review(
        tmp_path,
        owner_decision="monitor",
        as_of=date(2026, 6, 8),
    )
    weekly = _run_weekly(tmp_path, config_path=config_path)
    artifact_dir = Path(weekly["weekly_review_dir"])
    for artifact_name in ("weekly_review_report.md", "reader_brief_section.md"):
        artifact_path = artifact_dir / artifact_name
        original = artifact_path.read_text(encoding="utf-8")
        artifact_path.write_text(f"{original}\ntampered\n", encoding="utf-8")
        validation = validate_weekly_advisory_review_artifact(
            weekly_review_id=weekly["weekly_review_id"],
            output_dir=tmp_path / "weekly_advisory_review",
        )
        assert validation["status"] == "FAIL"
        artifact_path.write_text(original, encoding="utf-8")


def test_weekly_review_legacy_unsnapshotted_is_warning_only(tmp_path: Path) -> None:
    weekly_id = "legacy-weekly"
    artifact_dir = tmp_path / "weekly_advisory_review" / weekly_id
    artifact_dir.mkdir(parents=True)
    manifest = {
        "weekly_review_id": weekly_id,
        "weekly_recommendation": "manual_review_required",
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }
    (artifact_dir / "weekly_review_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    for name in (
        "weekly_advisory_summary.json",
        "weekly_owner_decision_summary.json",
        "weekly_paper_portfolio_summary.json",
        "weekly_shadow_candidate_summary.json",
    ):
        (artifact_dir / name).write_text("{}", encoding="utf-8")
    (artifact_dir / "weekly_review_report.md").write_text("legacy", encoding="utf-8")
    (artifact_dir / "reader_brief_section.md").write_text("legacy", encoding="utf-8")

    validation = validate_weekly_advisory_review_artifact(
        weekly_review_id=weekly_id,
        output_dir=tmp_path / "weekly_advisory_review",
    )
    assert validation["status"] == "PASS_WITH_WARNINGS"
    assert validation["source_snapshot_status"] == "LEGACY_UNSNAPSHOTTED"


def _run_weekly(tmp_path: Path, *, config_path: Path) -> dict[str, Any]:
    return run_weekly_advisory_review(
        week_ending=date(2026, 6, 14),
        output_dir=tmp_path / "weekly_advisory_review",
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        owner_review_dir=tmp_path / "owner_review_journal",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        advisory_outcome_dir=tmp_path / "advisory_outcome",
        shadow_aging_dir=tmp_path / "shadow_aging",
        config_path=config_path,
        generated_at=datetime(2026, 6, 14, 18, tzinfo=UTC),
    )


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
