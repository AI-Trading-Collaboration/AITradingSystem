from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import configured_price_tickers, configured_rate_series, load_universe
from ai_trading_system.free_pit_data_sources import (
    DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH,
    load_free_data_source_registry,
    run_free_data_source_ingestion,
    validate_free_data_source_registry,
)


def test_fred_series_have_source_contract() -> None:
    registry = load_free_data_source_registry(DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH)
    validation = validate_free_data_source_registry(registry)

    fred = next(
        source for source in registry["sources"] if source["source_id"] == "fred_market_series"
    )
    assert validation["status"] == "PASS"
    assert fred["provider"] == "Federal Reserve Economic Data"
    assert fred["free_or_paid"] == "free"
    assert fred["PIT_status"] == "PIT_APPROVED"
    assert "revision_sensitive_macro_model_ready_without_vintage" in fred["blocked_usage"]


def test_alfred_vintage_required_for_revision_sensitive_macro() -> None:
    registry = load_free_data_source_registry(DEFAULT_FREE_DATA_SOURCE_REGISTRY_PATH)
    broken = dict(registry)
    broken["sources"] = [dict(source) for source in registry["sources"]]
    broken["sources"][0] = dict(broken["sources"][0])
    broken["sources"][0]["request_parameters"] = {"series": ["CPIAUCSL"]}
    broken["sources"][0]["vintage_support"] = False

    validation = validate_free_data_source_registry(broken)

    assert validation["status"] == "FAIL"
    assert any(
        issue["code"] == "revision_sensitive_macro_requires_vintage"
        for issue in validation["issues"]
    )


def test_free_source_ingestion_writes_vix_crosscheck_and_features(tmp_path: Path) -> None:
    prices_path, rates_path, as_of = _write_cache_inputs(tmp_path)
    payload = run_free_data_source_ingestion(
        rates_path=rates_path,
        prices_path=prices_path,
        marketstack_prices_path=None,
        manifest_path=tmp_path / "missing_manifest.csv",
        output_root=tmp_path / "processed" / "free_sources",
        feature_output_root=tmp_path / "features",
        docs_root=tmp_path / "docs" / "research",
        inputs_root=tmp_path / "inputs" / "research_reviews",
        as_of_date=as_of,
    )

    assert payload["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    vix_path = Path(payload["artifact_paths"]["vix_history"])
    rates_features_path = Path(payload["artifact_paths"]["rates_liquidity_free_v1"])
    readiness_path = Path(payload["artifact_paths"]["free_feature_family_reopen_readiness_yaml"])
    assert vix_path.exists()
    assert rates_features_path.exists()
    assert readiness_path.exists()
    assert not pd.read_parquet(vix_path).empty
    rates_features = pd.read_parquet(rates_features_path)
    assert "rate_stress_score" in rates_features.columns
    assert payload["summary"]["available_fred_series"] == ["DGS10", "DGS2", "DTWEXBGS"]


def test_free_sources_cli_validate_is_registered() -> None:
    result = CliRunner().invoke(
        app,
        ["data", "free-sources", "validate"],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "Free PIT data sources" in result.output
    assert "promotion_allowed=false" in result.output


def _write_cache_inputs(tmp_path: Path) -> tuple[Path, Path, date]:
    universe = load_universe()
    tickers = configured_price_tickers(universe)
    rate_series = configured_rate_series(universe)
    dates = pd.bdate_range("2026-02-02", periods=90)
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows: list[dict[str, Any]] = []
    for ticker_index, ticker in enumerate(tickers):
        for row_index, day in enumerate(dates):
            close = 100.0 + ticker_index + row_index * 0.05
            price_rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": ticker,
                    "open": close,
                    "high": close + 0.1,
                    "low": close - 0.1,
                    "close": close,
                    "adj_close": close,
                    "volume": 0 if ticker == "^VIX" else 1000,
                }
            )
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    rate_rows: list[dict[str, Any]] = []
    for series_index, series in enumerate(rate_series):
        for row_index, day in enumerate(dates):
            value = 2.0 + series_index * 0.5 + row_index * 0.001
            if series == "DTWEXBGS":
                value = 110.0 + row_index * 0.02
            rate_rows.append(
                {
                    "date": day.date().isoformat(),
                    "series": series,
                    "value": value,
                }
            )
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)
    return prices_path, rates_path, dates[-1].date()
