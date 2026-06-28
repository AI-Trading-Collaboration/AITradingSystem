from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.free_feature_family_reablation import (
    run_free_feature_family_reablation_pack,
)
from ai_trading_system.participation_proxy_validation import (
    run_participation_proxy_validation_pack,
)


def test_free_feature_reablation_builds_dataset_and_blocks_promotion(tmp_path: Path) -> None:
    prices_path, rates_path, as_of = _write_price_and_rate_cache(tmp_path)
    feature_root = tmp_path / "features"
    feature_root.mkdir()
    pd.DataFrame(
        [
            {"date": "2026-02-02", "rate_stress_score": 0.1},
            {"date": "2026-02-03", "rate_stress_score": 0.2},
        ]
    ).to_parquet(feature_root / "rates_liquidity_free_v1.parquet", index=False)
    pd.DataFrame(
        [
            {"date": "2026-02-02", "vix_percentile": 0.3},
            {"date": "2026-02-03", "vix_percentile": 0.4},
        ]
    ).to_parquet(feature_root / "volatility_compression_free_v1.parquet", index=False)

    payload = run_free_feature_family_reablation_pack(
        feature_root=feature_root,
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=None,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        free_feature_pit_audit_path=tmp_path / "missing_pit.yaml",
        coverage_matrix_path=tmp_path / "missing_coverage.yaml",
        channel_closeout_path=tmp_path / "missing_closeout.yaml",
        as_of_date=as_of,
    )

    dataset = pd.read_parquet(payload["artifact_paths"]["ablation_dataset"])
    assert set(dataset["feature_family"]) == {
        "rates_liquidity_free_v1",
        "volatility_compression_free_v1",
    }
    assert "future_20d_return" in dataset.columns
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"
    assert payload["summary"]["candidate_count"] == 0


def test_participation_proxy_validation_keeps_proxy_out_of_model_ready_breadth(
    tmp_path: Path,
) -> None:
    prices_path, rates_path, as_of = _write_price_and_rate_cache(tmp_path)
    payload = run_participation_proxy_validation_pack(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=None,
        feature_root=tmp_path / "features",
        processed_root=tmp_path / "processed",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        allow_network_trials=False,
        as_of_date=as_of,
    )

    proxy_v2 = pd.read_parquet(payload["artifact_paths"]["participation_proxy_free_v2"])
    pit_contract = payload["artifact_paths"]["pit_contract_yaml"]

    assert not proxy_v2.empty
    assert proxy_v2["PIT_status"].str.contains("NOT_TRUE_BREADTH").all()
    assert Path(pit_contract).exists()
    assert payload["summary"]["participation_proxy_true_pit_breadth"] is False
    assert payload["summary"]["model_ready_breadth_allowed"] is False
    assert payload["promotion_allowed"] is False
    assert payload["broker_action"] == "none"


def _write_price_and_rate_cache(tmp_path: Path) -> tuple[Path, Path, date]:
    dates = pd.bdate_range("2026-02-02", periods=80)
    tickers = ["QQQ", "QQQE", "RSP", "SPY", "SMH", "SOXX", "XLK"]
    price_rows: list[dict[str, Any]] = []
    for ticker_index, ticker in enumerate(tickers):
        for row_index, day in enumerate(dates):
            close = 100.0 + ticker_index + row_index * (0.05 + ticker_index * 0.001)
            price_rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": ticker,
                    "open": close,
                    "high": close + 0.1,
                    "low": close - 0.1,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000,
                }
            )
    rates_rows = [
        {"date": day.date().isoformat(), "series": "DGS10", "value": 4.0}
        for day in dates
    ]
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    pd.DataFrame(rates_rows).to_csv(rates_path, index=False)
    return prices_path, rates_path, dates[-1].date()
