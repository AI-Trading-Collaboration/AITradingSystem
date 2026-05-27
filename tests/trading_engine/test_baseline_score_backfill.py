from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_features, load_portfolio, load_scoring_rules
from ai_trading_system.scoring.baseline_score_backfill import (
    BASELINE_SCORE_BACKFILL_COLUMNS,
    run_baseline_score_backfill,
)


def test_baseline_score_backfill_writes_research_schema(tmp_path: Path) -> None:
    prices_path, rates_path = _write_market_inputs(tmp_path)
    output_path = tmp_path / "data" / "processed" / "scores_daily.csv"

    result = run_baseline_score_backfill(
        start=date(2023, 1, 3),
        end=date(2023, 1, 10),
        tickers=["NVDA", "MSFT"],
        prices_path=prices_path,
        rates_path=rates_path,
        output_path=output_path,
        feature_config=load_features(),
        scoring_rules=load_scoring_rules(),
        portfolio=load_portfolio(),
        data_quality_status="PASS",
        data_quality_report_path=tmp_path / "quality.md",
    )

    frame = pd.read_csv(output_path)
    assert result.row_count == len(frame)
    assert tuple(frame.columns) == BASELINE_SCORE_BACKFILL_COLUMNS
    assert set(frame["ticker"]) == {"MSFT", "NVDA"}
    assert frame["research_backfill"].astype(str).str.lower().eq("true").all()
    assert set(frame["production_effect"]) == {"none"}
    assert frame["baseline_score"].notna().all()
    assert frame["baseline_rank"].notna().all()
    assert frame["baseline_action"].astype(str).str.len().gt(0).all()
    assert frame["score_completeness_ratio"].between(0, 1).all()
    assert "market_wide_score_replicated_by_ticker" in frame.iloc[0]["baseline_limitations"]


def test_baseline_score_backfill_does_not_overwrite_without_flag(tmp_path: Path) -> None:
    prices_path, rates_path = _write_market_inputs(tmp_path)
    output_path = tmp_path / "scores_daily.csv"
    output_path.write_text("sentinel\n", encoding="utf-8")

    with pytest.raises(FileExistsError):
        run_baseline_score_backfill(
            start=date(2023, 1, 3),
            end=date(2023, 1, 10),
            tickers=["NVDA"],
            prices_path=prices_path,
            rates_path=rates_path,
            output_path=output_path,
            feature_config=load_features(),
            scoring_rules=load_scoring_rules(),
            portfolio=load_portfolio(),
            data_quality_status="PASS",
        )

    assert output_path.read_text(encoding="utf-8") == "sentinel\n"


def test_baseline_score_backfill_is_deterministic(tmp_path: Path) -> None:
    prices_path, rates_path = _write_market_inputs(tmp_path)
    output_path = tmp_path / "scores_daily.csv"
    kwargs = {
        "start": date(2023, 1, 3),
        "end": date(2023, 1, 10),
        "tickers": ["NVDA", "MSFT"],
        "prices_path": prices_path,
        "rates_path": rates_path,
        "output_path": output_path,
        "feature_config": load_features(),
        "scoring_rules": load_scoring_rules(),
        "portfolio": load_portfolio(),
        "data_quality_status": "PASS",
        "overwrite": True,
    }

    run_baseline_score_backfill(**kwargs)
    first = output_path.read_text(encoding="utf-8")
    run_baseline_score_backfill(**kwargs)

    assert output_path.read_text(encoding="utf-8") == first


def test_backfill_cli_blocks_production_scores_path_without_confirmation() -> None:
    result = CliRunner().invoke(
        app,
        [
            "score-daily",
            "backfill-baseline",
            "--start",
            "2023-01-01",
            "--end",
            "2023-01-02",
            "--tickers",
            "NVDA",
            "--output-path",
            "data/processed/scores_daily.csv",
        ],
    )

    assert result.exit_code != 0
    assert "--overwrite-production-path" in result.output


def _write_market_inputs(tmp_path: Path) -> tuple[Path, Path]:
    dates = pd.bdate_range("2022-12-01", periods=40)
    tickers = ["NVDA", "MSFT", "SPY", "QQQ", "SMH", "SOXX", "^VIX"]
    price_rows = []
    for ticker_index, ticker in enumerate(tickers):
        for offset, current in enumerate(dates):
            close = 100 + ticker_index * 5 + offset * (0.5 if ticker != "^VIX" else -0.1)
            price_rows.append(
                {
                    "date": current.date().isoformat(),
                    "ticker": ticker,
                    "open": close - 0.1,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "adj_close": close,
                    "volume": 1_000_000,
                }
            )
    rate_rows = []
    for offset, current in enumerate(dates):
        for series in ("DGS2", "DGS10", "DTWEXBGS"):
            rate_rows.append(
                {
                    "date": current.date().isoformat(),
                    "series": series,
                    "value": 4.0 + offset * 0.01,
                }
            )
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)
    return prices_path, rates_path
