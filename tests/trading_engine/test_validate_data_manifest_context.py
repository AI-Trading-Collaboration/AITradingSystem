from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.config import load_data_quality
from ai_trading_system.data.quality import validate_data_cache


def test_validate_data_uses_manifest_context_for_cache_mismatch(tmp_path: Path) -> None:
    prices_path, rates_path = _write_cache(tmp_path, tickers=("QQQ",))
    manifest_path = _write_backtest_manifest(
        tmp_path,
        prices_path=prices_path,
        assets=("QQQ", "GOOGL", "BRK.B", "SGOV"),
        symbol_mapping={"BRK.B": {"canonical_symbol": "BRK.B", "source_symbol": "BRK-B"}},
    )

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["QQQ", "GOOGL", "BRK.B", "SGOV"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 1, 2),
        backtest_manifest_path=manifest_path,
    )

    codes = {issue.code for issue in report.issues}
    assert report.passed is False
    assert "MANIFEST_PRICE_CACHE_MISMATCH" in codes
    assert "prices_missing_expected_values" not in codes


def test_validate_data_reports_missing_symbol_mapping(tmp_path: Path) -> None:
    prices_path, rates_path = _write_cache(tmp_path, tickers=("QQQ",))
    manifest_path = _write_backtest_manifest(
        tmp_path,
        prices_path=prices_path,
        assets=("QQQ", "BRK.B"),
        symbol_mapping={},
    )

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["QQQ", "BRK.B"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 1, 2),
        backtest_manifest_path=manifest_path,
    )

    assert "SYMBOL_MAPPING_MISSING" in {issue.code for issue in report.issues}


def _write_cache(tmp_path: Path, *, tickers: tuple[str, ...]) -> tuple[Path, Path]:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows: list[dict[str, object]] = []
    for current in (date(2026, 1, 1), date(2026, 1, 2)):
        for ticker in tickers:
            price_rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": ticker,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.0,
                    "adj_close": 100.0,
                    "volume": 1000,
                }
            )
    rate_rows = [
        {"date": "2026-01-01", "series": "DGS2", "value": 4.0},
        {"date": "2026-01-02", "series": "DGS2", "value": 4.1},
        {"date": "2026-01-01", "series": "DGS10", "value": 4.4},
        {"date": "2026-01-02", "series": "DGS10", "value": 4.5},
    ]
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)
    return prices_path, rates_path


def _write_backtest_manifest(
    tmp_path: Path,
    *,
    prices_path: Path,
    assets: tuple[str, ...],
    symbol_mapping: dict[str, object],
) -> Path:
    path = tmp_path / "backtest_input_manifest.json"
    path.write_text(
        json.dumps(
            {
                "report_type": "backtest_input_manifest",
                "status": "OK",
                "assets": list(assets),
                "symbol_mapping": symbol_mapping,
                "price_data_files": [str(prices_path)],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path
