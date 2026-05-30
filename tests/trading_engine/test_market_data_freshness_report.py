from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from ai_trading_system.trading_engine.market_data_freshness import (
    load_market_data_freshness_payload,
    market_data_freshness_payload_date,
    run_market_data_freshness,
    validate_market_data_freshness_payload,
    write_market_data_freshness_report_alias,
)
from trading_engine.test_market_data_freshness import _freshness_fixture


def test_market_data_freshness_report_json_markdown_and_alias(tmp_path: Path) -> None:
    as_of = date(2026, 1, 6)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [as_of], "NVDA": [as_of]},
        manifest_date=as_of,
    )

    run = run_market_data_freshness(
        as_of=as_of,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 1, 0, tzinfo=UTC),
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    payload = load_market_data_freshness_payload(run.json_path)
    assert validate_market_data_freshness_payload(payload) == []
    assert payload["metadata"]["production_effect"] == "none"
    assert payload["metadata"]["manual_review_required"] is True
    assert payload["metadata"]["auto_promotion"] is False
    assert "Market Data Freshness Summary" in run.markdown_path.read_text(encoding="utf-8")

    report_date = market_data_freshness_payload_date(payload, run.json_path)
    alias_json, alias_markdown = write_market_data_freshness_report_alias(
        payload,
        tmp_path / "outputs" / "reports",
        report_date,
    )

    assert alias_json.exists()
    assert alias_markdown.exists()
    alias_payload = json.loads(alias_json.read_text(encoding="utf-8"))
    assert alias_payload["report_type"] == "market_data_freshness_report"
    assert alias_payload["freshness"]["status"] == "OK"
