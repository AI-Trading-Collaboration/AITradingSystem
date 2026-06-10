from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    run_pressure_regime_tagging,
    validate_pressure_regime_tag_artifact,
)


def test_pressure_regime_tagging_uses_config_thresholds_and_tags_outcomes(
    tmp_path: Path,
) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    _write_pressure_prices(prices_path)
    advisory_dir = _write_advisory_outcome(tmp_path)

    result = run_pressure_regime_tagging(
        start=date(2026, 6, 1),
        end=date(2026, 6, 10),
        output_dir=tmp_path / "pressure_regime_tag",
        advisory_outcome_dir=advisory_dir,
        prices_path=prices_path,
        rates_path=tmp_path / "rates_daily.csv",
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 10, tzinfo=UTC),
    )

    summary = result["pressure_regime_summary"]
    samples = summary["pressure_samples"]
    outcome_tags = result["outcome_regime_tags"]
    assert samples["tech_drawdown"] > 0
    assert samples["semiconductor_pullback"] > 0
    assert any(row["defensive_validation_relevant"] is True for row in outcome_tags)
    assert summary["defensive_validation_relevant_outcomes"] == 1
    assert (
        validate_pressure_regime_tag_artifact(
            tag_id=result["tag_id"],
            output_dir=tmp_path / "pressure_regime_tag",
        )["status"]
        == "PASS"
    )


def _write_pressure_prices(path: Path) -> None:
    dates = pd.bdate_range("2026-06-01", "2026-06-10")
    qqq = [100, 98, 96, 94, 93, 92, 95, 97]
    smh = [100, 99, 96, 94, 92, 91, 93, 95]
    rows = []
    for symbol, values in {"QQQ": qqq, "SMH": smh}.items():
        for day_value, close in zip(dates, values, strict=True):
            rows.append(
                {
                    "date": day_value.date().isoformat(),
                    "ticker": symbol,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_advisory_outcome(tmp_path: Path) -> Path:
    root = tmp_path / "advisory_outcome"
    outcome_dir = root / "outcome-1"
    outcome_dir.mkdir(parents=True)
    (outcome_dir / "advisory_outcome_manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "outcome_id": "outcome-1",
                "daily_advisory_id": "daily-1",
                "as_of": "2026-06-01",
                "broker_action_allowed": False,
                "production_effect": "none",
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    (outcome_dir / "outcome_windows.jsonl").write_text(
        json.dumps(
            {
                "daily_advisory_id": "daily-1",
                "window_days": 5,
                "start_date": "2026-06-01",
                "end_date": "2026-06-05",
                "outcome_status": "AVAILABLE",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return root
