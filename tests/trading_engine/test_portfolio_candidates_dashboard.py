from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.portfolio_candidates import run_portfolio_candidates
from ai_trading_system.trading_engine.portfolio_sensitivity import run_portfolio_sensitivity
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_portfolio_candidates import _write_portfolio_candidate_config
from trading_engine.test_portfolio_sensitivity import _write_portfolio_sensitivity_config
from trading_engine.test_shadow_parameter_backtest import (
    _write_dashboard_metadata,
    _write_shadow_backtest_fixture,
)
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot


def test_dashboard_reads_portfolio_candidates_summary(tmp_path: Path) -> None:
    fixture = _candidate_fixture(tmp_path)
    config_path = _write_portfolio_candidate_config(tmp_path, fixture["config_path"])
    candidates_run = run_portfolio_candidates(
        as_of=fixture["as_of"],
        profile_names=("baseline_current", "balanced_responsive"),
        config_path=config_path,
    )
    metadata_path = _write_dashboard_metadata(tmp_path, fixture["as_of"])

    report = build_daily_task_dashboard_report(
        as_of=fixture["as_of"],
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["portfolio_candidates_summary"]
    assert card["exists"] is True
    assert card["status"] == "LIMITED"
    assert card["profiles_tested"] == len(candidates_run.payload["profiles"])
    assert card["best_profile"] == candidates_run.payload["ranking"]["best_profile"]
    assert card["candidate_promotion_eligibility"] is False
    assert card["manual_review_required"] is True
    assert card["production_effect"] == "none"
    assert card["data_gate"] == "OK"
    assert "Portfolio Candidate Profiles" in html
    assert "signal transmission improvement" in html


def test_reader_brief_displays_portfolio_candidates_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _candidate_fixture(tmp_path)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    config_path = _write_portfolio_candidate_config(tmp_path, fixture["config_path"])
    candidates_run = run_portfolio_candidates(
        as_of=fixture["as_of"],
        profile_names=("baseline_current", "balanced_responsive"),
        config_path=config_path,
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
    assert review["portfolio_candidates_status"] == "LIMITED"
    assert review["portfolio_candidates_best_profile"] == candidates_run.payload["ranking"][
        "best_profile"
    ]
    assert review["portfolio_candidates_profiles_tested"] == len(
        candidates_run.payload["profiles"]
    )
    assert review["portfolio_candidates_guardrail_status"] in {"PASS", "FAIL", "UNKNOWN"}
    assert review["portfolio_candidates_promotion_eligibility"] is False
    assert "Portfolio candidate evaluation" in review["portfolio_candidates_summary"]


def _candidate_fixture(tmp_path: Path) -> dict[str, object]:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=80, min_history_days=20)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    sensitivity_config_path = _write_portfolio_sensitivity_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_sensitivity(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1", "lower_rebalance_threshold"),
        config_path=sensitivity_config_path,
    )
    return fixture
