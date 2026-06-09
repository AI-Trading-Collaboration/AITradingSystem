from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.trading_engine.portfolio_candidate_review import (
    decide_portfolio_candidate,
    load_portfolio_candidate_review_payload,
    portfolio_candidate_review_payload_date,
    run_portfolio_candidate_review,
    validate_portfolio_candidate_review_decision_payload,
    write_portfolio_candidate_review_report_alias,
)
from trading_engine.test_portfolio_candidate_review import _review_fixture


def test_portfolio_candidate_review_report_alias_reads_decision(
    tmp_path: Path,
) -> None:
    fixture, _, review_config = _review_fixture(tmp_path)
    review_run = run_portfolio_candidate_review(
        as_of=fixture["as_of"],
        config_path=review_config,
    )
    decision_run = decide_portfolio_candidate(
        decision="watch",
        as_of=fixture["as_of"],
        reason="Signal quality remains LIMITED; continue observing.",
        config_path=review_config,
    )

    decision_payload = load_portfolio_candidate_review_payload(decision_run.decision_json_path)
    assert validate_portfolio_candidate_review_decision_payload(decision_payload) == []
    report_date = portfolio_candidate_review_payload_date(
        decision_payload,
        decision_run.decision_json_path,
    )
    alias_json, alias_markdown = write_portfolio_candidate_review_report_alias(
        review_run.package_payload,
        decision_payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert alias_json.exists()
    assert alias_markdown.exists()
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == "portfolio_candidate_review_report"
    assert alias_payload["decision"]["decision"]["status"] == "watch"
    assert alias_payload["package"]["candidate"]["profile_name"]
    assert "Portfolio Candidate Review Report" in alias_markdown.read_text(encoding="utf-8")
