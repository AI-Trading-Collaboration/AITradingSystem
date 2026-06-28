from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system import equal_weight_proxy_data_fix
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.data.market_data import PriceRequest


def test_equal_weight_proxy_data_fix_repairs_price_cache_and_keeps_true_breadth_blocked(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    manifest_path = tmp_path / "download_manifest.csv"
    _write_base_prices(prices_path)

    monkeypatch.setattr(
        equal_weight_proxy_data_fix,
        "validate_cached_market_data",
        lambda **kwargs: {
            "status": "PASS",
            "passed": True,
            "checked_at": "2026-06-28T00:00:00+00:00",
            "as_of": kwargs["as_of_date"].isoformat(),
            "price_path": str(kwargs["prices_path"]),
            "rates_path": str(kwargs["rates_path"]),
            "secondary_prices_path": "",
            "expected_price_tickers": list(kwargs["expected_price_tickers"]),
            "expected_rate_series": [],
            "price_row_count": 100,
            "rate_row_count": 0,
            "price_checksum": "fixture",
            "rate_checksum": "fixture",
            "warning_count": 0,
            "error_count": 0,
        },
    )
    monkeypatch.setattr(
        equal_weight_proxy_data_fix,
        "run_first_layer_proxy_coverage_audit_pack",
        _fake_proxy_coverage_audit,
    )

    payload = equal_weight_proxy_data_fix.run_equal_weight_proxy_data_fix_pack(
        prices_path=prices_path,
        rates_path=tmp_path / "rates_daily.csv",
        marketstack_prices_path=None,
        download_manifest_path=manifest_path,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        as_of_date=date(2026, 1, 10),
        repair_start=date(2026, 1, 2),
        price_provider=_FakeProvider(),
    )

    repaired = pd.read_csv(prices_path)
    target = repaired.loc[repaired["ticker"].isin(["RSP", "QQQE"])]
    by_symbol = {row["symbol"]: row for row in payload["resolution_rows"]}

    assert set(target["ticker"]) == {"RSP", "QQQE"}
    assert (
        by_symbol["RSP"]["root_cause"]
        == "LOCAL_PRICE_CACHE_COVERAGE_GAP_NOT_PROVIDER_OR_MAPPING"
    )
    assert by_symbol["RSP"]["command_entry_cache_status"] == "CACHE_MISSING_TICKER_NOT_DOWNLOADED"
    assert by_symbol["QQQE"]["repair_status"] == "RESOLVED_PRICE_CACHE_AVAILABLE"
    assert payload["replacement_for_true_breadth"] is False
    assert payload["promotion_allowed"] is False
    assert payload["blocked_proxy_resolution_status"]["true_breadth_replaced"] is False
    assert Path(payload["artifact_paths"]["equal_weight_proxy_data_fix_report"]).exists()
    assert Path(payload["artifact_paths"]["updated_proxy_coverage_matrix"]).exists()
    assert Path(payload["artifact_paths"]["blocked_proxy_resolution_status"]).exists()
    assert manifest_path.exists()


def test_equal_weight_proxy_data_fix_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "equal-weight-proxy-data-fix" in result.output


class _FakeProvider:
    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        symbol = request.tickers[0]
        dates = pd.bdate_range(request.start, request.end)
        rows = []
        for idx, day in enumerate(dates):
            close = 100.0 + idx
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": symbol,
                    "open": close - 0.1,
                    "high": close + 0.2,
                    "low": close - 0.2,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000 + idx,
                }
            )
        return pd.DataFrame(rows)


def _write_base_prices(path: Path) -> None:
    dates = pd.bdate_range("2026-01-02", "2026-01-10")
    rows = []
    for ticker in ["QQQ", "SPY", "SMH", "SOXX"]:
        for idx, day in enumerate(dates):
            close = 100.0 + idx
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": ticker,
                    "symbol": ticker,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000,
                    "source": "fixture",
                    "updated_at": "",
                    "source_symbol": ticker,
                    "canonical_symbol": ticker,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _fake_proxy_coverage_audit(**kwargs: Any) -> dict[str, Any]:
    frame = pd.read_csv(kwargs["prices_path"])
    tickers = set(frame["ticker"].astype(str))
    rows = [
        _proxy_row("rsp_to_spy", {"RSP", "SPY"}.issubset(tickers)),
        _proxy_row("qqqe_to_qqq", {"QQQE", "QQQ"}.issubset(tickers)),
        _proxy_row("sector_etf_relative_strength", False),
    ]
    return {
        "status": "FREE_LOW_COST_PROXY_COVERAGE_AUDIT_READY_PROMOTION_BLOCKED",
        "summary": {
            "as_of": kwargs["as_of_date"].isoformat(),
            "proxy_count": len(rows),
            "data_available_count": sum(1 for row in rows if row["data_available"]),
            "primary_window_covered_count": 2,
            "replacement_for_true_breadth_count": 0,
            "true_breadth_replaced": False,
        },
        "rows": rows,
    }


def _proxy_row(proxy_id: str, available: bool) -> dict[str, Any]:
    return {
        "proxy_id": proxy_id,
        "proxy_group": "etf_ratio_price_proxy",
        "data_available": available,
        "PIT_safe_or_not": "PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH"
        if available
        else "PIT_BLOCKED_BY_PRICE_COVERAGE",
        "replacement_for_true_breadth": False,
    }
