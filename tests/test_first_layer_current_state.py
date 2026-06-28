from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system import first_layer_current_state
from ai_trading_system.cli_commands.research_trends import trends_app


def test_first_layer_current_state_generates_required_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    predictions_path = tmp_path / "predictions.csv"
    prices_path = tmp_path / "prices.csv"
    _write_predictions(predictions_path)
    _write_prices(prices_path)

    monkeypatch.setattr(
        first_layer_current_state,
        "validate_cached_market_data",
        lambda **_: {
            "status": "PASS",
            "passed": True,
            "checked_at": "2026-06-28T00:00:00+00:00",
            "as_of": "2023-04-10",
            "price_path": str(prices_path),
            "rates_path": str(tmp_path / "rates.csv"),
            "secondary_prices_path": "",
            "expected_price_tickers": ["QQQ", "SPY", "SMH"],
            "expected_rate_series": [],
            "price_row_count": 210,
            "rate_row_count": 0,
            "price_checksum": "fixture",
            "rate_checksum": "fixture",
            "warning_count": 0,
            "error_count": 0,
        },
    )

    payload = first_layer_current_state.run_first_layer_current_state_pack(
        predictions_path=predictions_path,
        prices_path=prices_path,
        rates_path=tmp_path / "rates.csv",
        marketstack_prices_path=None,
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        as_of_date=date(2023, 4, 10),
    )

    assert payload["status"] == "FIRST_LAYER_CURRENT_STATE_READY_PROMOTION_BLOCKED"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"

    artifact_paths = payload["artifact_paths"]
    taxonomy = json.loads(Path(artifact_paths["first_layer_failure_taxonomy"]).read_text())
    benchmark = json.loads(Path(artifact_paths["benchmark_consistency_report"]).read_text())

    assert Path(artifact_paths["first_layer_current_state_report"]).exists()
    assert Path(artifact_paths["regime_slice_summary"]).exists()
    assert taxonomy["data_quality_status"] == "PASS"
    assert {row["failure_type"] for row in taxonomy["failure_taxonomy"]} == {
        "false_risk_on",
        "false_risk_off",
        "late_risk_off",
        "late_risk_on",
    }
    assert sum(row["event_count"] for row in taxonomy["failure_taxonomy"]) > 0
    assert benchmark["summary"]["optional_benchmarks_missing"] == ["IWM", "RSP"]
    assert benchmark["promotion_allowed"] is False


def test_first_layer_current_state_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-current-state" in result.output


def _write_predictions(path: Path) -> None:
    dates = pd.bdate_range("2023-01-03", periods=70)
    rows = []
    for idx, day in enumerate(dates):
        if idx < 15:
            state = "risk_on"
        elif idx < 35:
            state = "neutral"
        elif idx < 45:
            state = "risk_off"
        else:
            state = "neutral"
        rows.append({"date": day.date().isoformat(), "trend_state": state})
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_prices(path: Path) -> None:
    dates = pd.bdate_range("2023-01-03", periods=70)
    rows = []
    for ticker, offset in {"QQQ": 0.0, "SPY": 0.8, "SMH": 1.5}.items():
        for idx, day in enumerate(dates):
            if idx < 20:
                price = 100.0 + idx * 0.15 + offset
            elif idx < 40:
                price = 103.0 - (idx - 20) * 0.85 + offset
            else:
                price = 86.0 + (idx - 40) * 1.25 + offset
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": ticker,
                    "adj_close": round(price, 4),
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
