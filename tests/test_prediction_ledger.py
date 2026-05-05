from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.prediction_ledger import (
    append_prediction_records,
    build_prediction_outcomes,
    build_prediction_record_from_decision_snapshot,
    load_prediction_ledger,
    render_prediction_outcome_report,
    write_prediction_outcome_report,
    write_prediction_outcomes_csv,
)


def test_prediction_ledger_records_shadow_and_builds_outcomes(tmp_path: Path) -> None:
    record = build_prediction_record_from_decision_snapshot(
        snapshot=_decision_snapshot(),
        trace_bundle={"run_manifest": {"run_id": "run:test:2026-04-27"}},
        trace_bundle_path=tmp_path / "trace.json",
        features_path=tmp_path / "features.csv",
        data_quality_report_path=tmp_path / "quality.md",
        candidate_id="challenger_v2",
        production_effect="none",
    )
    ledger_path = append_prediction_records((record,), tmp_path / "prediction_ledger.csv")

    rows = load_prediction_ledger(ledger_path)
    assert len(rows) == 1
    assert rows[0]["candidate_id"] == "challenger_v2"
    assert rows[0]["production_effect"] == "none"
    assert rows[0]["outcome_status"] == "PENDING"

    result = build_prediction_outcomes(
        prediction_rows=rows,
        prices=_prices(),
        as_of=date(2026, 4, 30),
        horizons=(1, 3),
        strategy_ticker="SMH",
        benchmark_tickers=("SPY",),
        market_regime=None,
        data_quality_report=_quality_report(),
    )

    assert len(result.available_rows) == 2
    assert not result.pending_rows
    assert all(row["production_effect"] == "none" for row in result.available_rows)
    assert "excess_SPY_return" in result.available_rows[0]
    outcomes_path = write_prediction_outcomes_csv(
        result,
        tmp_path / "prediction_outcomes.csv",
    )
    report_path = write_prediction_outcome_report(
        result,
        outcomes_path=outcomes_path,
        data_quality_report_path=tmp_path / "quality.md",
        output_path=tmp_path / "prediction_outcomes.md",
    )
    markdown = render_prediction_outcome_report(
        result,
        outcomes_path=outcomes_path,
        data_quality_report_path=tmp_path / "quality.md",
    )
    assert outcomes_path.exists()
    assert report_path.exists()
    assert "# Prediction / Shadow Outcome 报告" in markdown
    assert "challenger_v2" in markdown


def _decision_snapshot() -> dict[str, object]:
    return {
        "signal_date": "2026-04-27",
        "generated_at": "2026-04-27T21:00:00+00:00",
        "market_regime": {"regime_id": "ai_after_chatgpt"},
        "scores": {
            "overall_score": 63.5,
            "confidence_score": 74.0,
            "confidence_level": "medium",
        },
        "positions": {
            "model_risk_asset_ai_band": {
                "min_position": 0.3,
                "max_position": 0.5,
                "label": "偏积极",
            },
            "final_risk_asset_ai_band": {
                "min_position": 0.2,
                "max_position": 0.4,
                "label": "中性偏多",
            },
        },
        "rule_versions": {"rules": [{"rule_id": "scoring.weighted_score.v1"}]},
    }


def _prices() -> pd.DataFrame:
    dates = pd.date_range("2026-04-27", periods=4, freq="D")
    rows: list[dict[str, object]] = []
    for ticker, base, step in (("SMH", 100.0, 2.0), ("SPY", 100.0, 1.0)):
        for index, value_date in enumerate(dates):
            rows.append(
                {
                    "date": value_date.date().isoformat(),
                    "ticker": ticker,
                    "adj_close": base + step * index,
                }
            )
    return pd.DataFrame(rows)


def _quality_report() -> DataQualityReport:
    return DataQualityReport(
        checked_at=pd.Timestamp("2026-04-30T00:00:00Z").to_pydatetime(),
        as_of=date(2026, 4, 30),
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=8),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1),
        expected_price_tickers=("SMH", "SPY"),
        expected_rate_series=("DGS10",),
        issues=(),
    )
