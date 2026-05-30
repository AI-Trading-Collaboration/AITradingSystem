from __future__ import annotations

from ai_trading_system.trading_engine.market_data_refresh import (
    render_market_data_refresh_markdown,
    validate_market_data_refresh_payload,
)


def test_market_data_refresh_json_schema_and_safety_fields() -> None:
    payload = _refresh_payload()

    assert validate_market_data_refresh_payload(payload) == []
    assert payload["metadata"]["production_effect"] == "none"
    assert payload["metadata"]["manual_review_required"] is True
    assert payload["metadata"]["auto_promotion"] is False
    assert payload["safety"]["fake_price_rows_generated"] is False
    assert payload["safety"]["synthetic_latest_bar_generated"] is False


def test_market_data_refresh_markdown_report_contains_required_sections() -> None:
    markdown = render_market_data_refresh_markdown(_refresh_payload())

    assert "# Market Data Refresh Summary" in markdown
    assert "## 2. Before State" in markdown
    assert "## 4. Assets Refreshed" in markdown
    assert "## 8. Freshness Recovery" in markdown
    assert "## 11. Manual Review Checklist" in markdown
    assert "refresh_status: `OK`" in markdown


def _refresh_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "market_data_refresh",
        "metadata": {
            "run_id": "market-data-refresh-2026-01-06",
            "generated_at": "2026-01-07T00:00:00+00:00",
            "status": "OK",
            "reason": "Market data freshness recovered for required assets.",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "before": {
            "freshness_status": "STALE",
            "tracking_date": "2026-01-06",
            "effective_data_date": "2026-01-05",
            "tracking_readiness": "cannot_track",
        },
        "refresh_plan": {
            "refresh_actions": [
                {
                    "action": "fetch_latest_daily_bar",
                    "target_date": "2026-01-06",
                    "required": True,
                }
            ],
            "symbol_mapping": {"BRK.B": {"source_symbol": "BRK-B"}},
        },
        "asset_results": [
            {
                "symbol": "BRK.B",
                "source_symbol": "BRK-B",
                "status": "FETCHED",
                "source": "audited_fmp_raw_cache",
                "rows_written": 1,
            }
        ],
        "actions": {
            "fetched_assets": ["BRK.B"],
            "target_date": "2026-01-06",
            "source": "audited_fmp_raw_cache",
            "updated_price_cache_registry": True,
            "refreshed_backtest_manifest": True,
        },
        "after": {
            "freshness_status": "OK",
            "effective_data_date": "2026-01-06",
            "tracking_readiness": "can_track",
            "candidate_tracking_status": "active_tracking",
        },
        "remaining_limitations": ["candidate promotion remains disabled."],
        "supporting_artifacts": {},
        "safety": {
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "production_write_allowed": False,
            "fake_price_rows_generated": False,
            "synthetic_latest_bar_generated": False,
            "data_quality_gate_lowered": False,
        },
    }
