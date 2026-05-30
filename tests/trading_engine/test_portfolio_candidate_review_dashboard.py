from __future__ import annotations

from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    decide_portfolio_candidate,
    run_portfolio_candidate_review,
)
from trading_engine.test_portfolio_candidate_review import _review_fixture
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot


def test_dashboard_reads_portfolio_candidate_review_decision(tmp_path: Path) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decision_run = decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reviewer="manual",
        reason="Signal quality remains LIMITED; continue observing.",
        config_path=review_config,
    )
    metadata_path = _write_dashboard_metadata(tmp_path, fixture["as_of"])

    report = build_daily_task_dashboard_report(
        as_of=fixture["as_of"],
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["portfolio_candidate_review_summary"]
    assert card["exists"] is True
    assert card["status"] == "watch"
    assert card["reviewer"] == "manual"
    assert card["candidate_profile"] == decision_run.decision_payload["candidate"][
        "profile_name"
    ]
    assert card["production_effect"] == "none"
    assert card["production_config_modified"] is False
    assert "Portfolio Candidate Review" in html
    assert "Review Decision Markdown" in html


def test_reader_brief_displays_portfolio_candidate_review_status(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="approved_for_shadow_candidate",
        as_of=fixture["as_of"],
        reviewer="manual",
        reason="Approved for shadow tracking only.",
        config_path=review_config,
    )
    shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
    )
    snapshot_path = _write_decision_snapshot(tmp_path, fixture["as_of"])

    payload = build_reader_brief_payload(
        as_of=fixture["as_of"],
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["portfolio_candidate_review_status"] == "approved_for_shadow_candidate"
    assert review["portfolio_candidate_review_profile"]
    assert review["portfolio_candidate_review_reviewer"] == "manual"
    assert review["portfolio_candidate_review_next_step"] == "continue_shadow_tracking"
    assert "shadow tracking only" in review["portfolio_candidate_review_summary"]
