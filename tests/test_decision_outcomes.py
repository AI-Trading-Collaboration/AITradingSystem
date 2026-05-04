from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import configured_rate_series, load_universe
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.decision_outcomes import (
    build_decision_outcomes,
    render_decision_calibration_report,
)


def test_build_decision_outcomes_calculates_returns_and_buckets(tmp_path: Path) -> None:
    snapshots = (_decision_snapshot("2026-04-01"),)
    prices = _sample_prices(("SMH", "SPY", "QQQ", "SOXX"), periods=7)

    result = build_decision_outcomes(
        snapshots=snapshots,
        prices=prices,
        as_of=date(2026, 4, 9),
        horizons=(1, 5),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY", "QQQ", "SMH", "SOXX"),
        data_quality_report=_quality_report(),
    )

    assert len(result.available_rows) == 2
    one_day = next(row for row in result.available_rows if row["horizon_days"] == 1)
    assert one_day["score_bucket"] == "65_80"
    assert one_day["confidence_level"] == "high"
    assert one_day["gate_state"] == "extra_gate_triggered"
    assert one_day["risk_level"] == "L2"
    assert one_day["valuation_state"] == "PASS_WITH_WARNINGS"
    assert one_day["ai_proxy_return"] == pytest.approx(0.01)
    assert one_day["SPY_return"] == pytest.approx(0.005)
    assert one_day["excess_SPY_return"] == pytest.approx(0.005)
    assert one_day["hit"] is True

    markdown = render_decision_calibration_report(
        result,
        outcomes_path=tmp_path / "decision_outcomes.csv",
        data_quality_report_path=tmp_path / "quality.md",
    )

    assert "# 决策结果校准报告" in markdown
    assert "PASS_WITH_LIMITATIONS" in markdown
    assert "## 分桶校准" in markdown
    assert "总分分桶" in markdown
    assert "GOV-001" in markdown


def test_feedback_calibrate_cli_writes_outcomes_and_report(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "decision_snapshots"
    snapshot_dir.mkdir()
    (snapshot_dir / "decision_snapshot_2026-04-01.json").write_text(
        json.dumps(_decision_snapshot("2026-04-01"), ensure_ascii=False),
        encoding="utf-8",
    )
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    outcomes_path = tmp_path / "decision_outcomes.csv"
    report_path = tmp_path / "decision_calibration.md"
    quality_path = tmp_path / "quality.md"
    _sample_prices(("SMH", "SPY", "QQQ", "SOXX"), periods=10).to_csv(
        prices_path,
        index=False,
    )
    _sample_rates(configured_rate_series(load_universe()), periods=10).to_csv(
        rates_path,
        index=False,
    )

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "calibrate",
            "--decision-snapshot-path",
            str(snapshot_dir),
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2026-04-14",
            "--horizons",
            "1,5",
            "--outcomes-path",
            str(outcomes_path),
            "--report-path",
            str(report_path),
            "--quality-report-path",
            str(quality_path),
        ],
    )

    assert result.exit_code == 0
    outcomes = pd.read_csv(outcomes_path)
    report_text = report_path.read_text(encoding="utf-8")
    assert set(outcomes["horizon_days"]) == {1, 5}
    assert set(outcomes["outcome_status"]) == {"AVAILABLE"}
    assert "决策结果校准报告" in report_text
    assert "样本限制" in report_text
    assert "决策校准完成" in result.output


def _decision_snapshot(signal_date: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "snapshot_id": f"decision_snapshot:{signal_date}",
        "signal_date": signal_date,
        "market_regime": {"regime_id": "ai_after_chatgpt"},
        "scores": {
            "overall_score": 72.0,
            "confidence_score": 80.0,
            "confidence_level": "high",
        },
        "positions": {
            "final_risk_asset_ai_band": {
                "min_position": 0.40,
                "max_position": 0.55,
                "label": "中性/仓位受限",
            },
            "position_gates": [
                {
                    "gate_id": "score_model",
                    "triggered": True,
                    "max_position": 0.80,
                },
                {
                    "gate_id": "valuation",
                    "triggered": True,
                    "max_position": 0.55,
                },
            ],
        },
        "quality": {
            "market_data_status": "PASS",
            "feature_status": "PASS",
        },
        "manual_review": [
            {
                "name": "交易 thesis",
                "status": "PASS_WITH_WARNINGS",
            }
        ],
        "risk_event_state": {
            "score_eligible_active_count": 1,
            "items": [
                {
                    "status": "active",
                    "score_eligible": True,
                    "level": "L2",
                }
            ],
        },
        "valuation_state": {
            "status": "PASS_WITH_WARNINGS",
            "items": [
                {
                    "health": "EXPENSIVE_OR_CROWDED",
                }
            ],
        },
        "belief_state_ref": {
            "path": "belief_state.json",
            "production_effect": "none",
        },
    }


def _sample_prices(tickers: tuple[str, ...], periods: int) -> pd.DataFrame:
    dates = pd.bdate_range(start="2026-04-01", periods=periods)
    rows: list[dict[str, object]] = []
    steps = {"SMH": 1.0, "SPY": 0.5, "QQQ": 0.75, "SOXX": 0.9}
    for ticker in tickers:
        for index, row_date in enumerate(dates):
            close = 100 + index * steps.get(ticker, 1.0)
            rows.append(
                {
                    "date": row_date.date().isoformat(),
                    "ticker": ticker,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adj_close": close,
                    "volume": 1_000_000,
                }
            )
    return pd.DataFrame(rows)


def _sample_rates(series_ids: list[str], periods: int) -> pd.DataFrame:
    dates = pd.bdate_range(start="2026-04-01", periods=periods)
    rows: list[dict[str, object]] = []
    for series in series_ids:
        for index, row_date in enumerate(dates):
            rows.append(
                {
                    "date": row_date.date().isoformat(),
                    "series": series,
                    "value": 4.0 + index * 0.01,
                }
            )
    return pd.DataFrame(rows)


def _quality_report() -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-04-10T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 4, 9),
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=1),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1),
        expected_price_tickers=("SMH", "SPY", "QQQ", "SOXX"),
        expected_rate_series=("DGS10",),
        issues=(),
    )
