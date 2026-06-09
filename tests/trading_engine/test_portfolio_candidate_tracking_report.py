from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.trading_engine.portfolio_candidate_review import (
    decide_portfolio_candidate,
    run_portfolio_candidate_review,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    load_portfolio_candidate_tracking_payload,
    portfolio_candidate_tracking_payload_date,
    run_portfolio_candidate_tracking,
    validate_portfolio_candidate_tracking_payload,
    write_portfolio_candidate_tracking_report_alias,
)
from trading_engine.test_portfolio_candidate_review import _review_fixture
from trading_engine.test_portfolio_candidate_tracking import (
    _write_portfolio_candidate_tracking_config,
)


def test_portfolio_candidate_tracking_report_alias_reads_summary(
    tmp_path: Path,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    tracking_config = _write_portfolio_candidate_tracking_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_candidate_review(as_of=fixture["as_of"], config_path=review_config)
    decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Continue shadow tracking.",
        config_path=review_config,
    )
    tracking_run = run_portfolio_candidate_tracking(
        as_of=fixture["as_of"],
        config_path=tracking_config,
    )

    payload = load_portfolio_candidate_tracking_payload(tracking_run.json_path)
    assert validate_portfolio_candidate_tracking_payload(payload) == []
    report_date = portfolio_candidate_tracking_payload_date(
        payload,
        tracking_run.json_path,
    )
    alias_json, alias_markdown = write_portfolio_candidate_tracking_report_alias(
        payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert alias_json.exists()
    assert alias_markdown.exists()
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == "portfolio_candidate_tracking_report"
    assert alias_payload["candidate"]["tracking_status"] == "active_tracking"
    assert alias_payload["candidate"]["profile_name"]
    assert "Portfolio Candidate Tracking Summary" in alias_markdown.read_text(encoding="utf-8")
