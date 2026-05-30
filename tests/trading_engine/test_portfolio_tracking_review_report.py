from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.trading_engine.portfolio_tracking_review import (
    load_portfolio_tracking_review_payload,
    portfolio_tracking_review_payload_date,
    run_portfolio_tracking_review,
    validate_portfolio_tracking_review_payload,
    write_portfolio_tracking_review_report_alias,
)
from trading_engine.test_portfolio_tracking_review import _tracking_review_fixture


def test_portfolio_tracking_review_report_alias_reads_summary(tmp_path: Path) -> None:
    fixture, _, tracking_review_config = _tracking_review_fixture(tmp_path)
    review_run = run_portfolio_tracking_review(
        as_of=fixture["as_of"],
        config_path=tracking_review_config,
    )

    payload = load_portfolio_tracking_review_payload(review_run.json_path)
    assert validate_portfolio_tracking_review_payload(payload) == []
    report_date = portfolio_tracking_review_payload_date(payload, review_run.json_path)
    alias_json, alias_markdown = write_portfolio_tracking_review_report_alias(
        payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert alias_json.exists()
    assert alias_markdown.exists()
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == "portfolio_tracking_review_report"
    assert alias_payload["recommendation"]["status"] == "needs_more_data"
    assert "Portfolio Tracking Review Summary" in alias_markdown.read_text(
        encoding="utf-8"
    )
