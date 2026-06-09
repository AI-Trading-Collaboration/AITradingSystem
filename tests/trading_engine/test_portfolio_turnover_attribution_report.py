from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.trading_engine.portfolio_turnover_attribution import (
    PORTFOLIO_TURNOVER_ATTRIBUTION_ALIAS_REPORT_TYPE,
    PORTFOLIO_TURNOVER_ATTRIBUTION_REPORT_TYPE,
    load_portfolio_turnover_attribution_payload,
    portfolio_turnover_attribution_payload_date,
    validate_portfolio_turnover_attribution_payload,
    write_portfolio_turnover_attribution_report_alias,
)
from trading_engine.test_portfolio_turnover_attribution import (
    write_portfolio_turnover_attribution_artifact,
)


def test_portfolio_turnover_attribution_report_alias_is_auditable(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 28)
    source_path = write_portfolio_turnover_attribution_artifact(tmp_path, as_of=as_of)

    payload = load_portfolio_turnover_attribution_payload(source_path)
    report_date = portfolio_turnover_attribution_payload_date(payload, source_path)
    alias_json, alias_markdown = write_portfolio_turnover_attribution_report_alias(
        payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert validate_portfolio_turnover_attribution_payload(payload) == []
    assert report_date == as_of
    assert "Portfolio Turnover Attribution Summary" in source_path.with_suffix(".md").read_text(
        encoding="utf-8"
    )
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == PORTFOLIO_TURNOVER_ATTRIBUTION_ALIAS_REPORT_TYPE
    assert alias_payload["source_report_type"] == PORTFOLIO_TURNOVER_ATTRIBUTION_REPORT_TYPE
    assert "Root Cause Assessment" in alias_markdown.read_text(encoding="utf-8")
