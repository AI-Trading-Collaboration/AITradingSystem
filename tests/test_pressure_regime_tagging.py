from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from dynamic_v3_outcome_loop_helpers import build_ready_outcome_update_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_confirmation_operations
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    DynamicV3ConfirmationOperationsError,
    run_pressure_regime_tagging,
    validate_pressure_regime_tag_artifact,
)


def test_pressure_regime_tagging_uses_config_thresholds_and_validated_outcomes_only(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    _write_pressure_prices(prices_path)
    outcome_root = tmp_path / "outcome"
    outcome_root.mkdir()
    outcome_fixture = build_ready_outcome_update_fixture(outcome_root, monkeypatch)
    advisory_dir = outcome_fixture["outcome"]["outcome_dir"].parent

    result = run_pressure_regime_tagging(
        start=date(2026, 6, 1),
        end=date(2026, 6, 30),
        output_dir=tmp_path / "pressure_regime_tag",
        advisory_outcome_dir=advisory_dir,
        prices_path=prices_path,
        rates_path=tmp_path / "rates_daily.csv",
        enforce_data_quality_gate=False,
        generated_at=datetime(2026, 6, 30, tzinfo=UTC),
    )

    summary = result["pressure_regime_summary"]
    samples = summary["pressure_samples"]
    outcome_tags = result["outcome_regime_tags"]
    assert samples["tech_drawdown"] > 0
    assert samples["semiconductor_pullback"] > 0
    assert outcome_tags == []
    assert summary["defensive_validation_relevant_outcomes"] == 0
    assert (
        validate_pressure_regime_tag_artifact(
            tag_id=result["tag_id"],
            output_dir=tmp_path / "pressure_regime_tag",
        )["status"]
        == "PASS"
    )

    summary_path = Path(result["tag_dir"]) / "pressure_regime_summary.json"
    summary_payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_payload["defensive_validation_relevant_outcomes"] = 999
    summary_path.write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    assert (
        validate_pressure_regime_tag_artifact(
            tag_id=result["tag_id"], output_dir=tmp_path / "pressure_regime_tag"
        )["status"]
        == "FAIL"
    )


def test_pressure_regime_rejects_invalid_relevant_outcome_before_output(
    tmp_path: Path,
) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    _write_pressure_prices(prices_path)
    advisory_dir = _write_advisory_outcome(tmp_path)
    output_dir = tmp_path / "pressure_regime_tag"

    with pytest.raises(DynamicV3ConfirmationOperationsError, match="validation failed"):
        run_pressure_regime_tagging(
            start=date(2026, 6, 1),
            end=date(2026, 6, 10),
            output_dir=output_dir,
            advisory_outcome_dir=advisory_dir,
            prices_path=prices_path,
            rates_path=tmp_path / "rates_daily.csv",
            enforce_data_quality_gate=False,
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )

    assert not output_dir.exists()


def test_pressure_regime_dq_failure_leaves_no_formal_artifact(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    _write_pressure_prices(prices_path)
    output_dir = tmp_path / "pressure_regime_tag"

    def fail_quality_gate(**kwargs: Any) -> tuple[str, str]:
        report_path = Path(kwargs["report_path"])
        report_path.write_text("DQ FAIL\n", encoding="utf-8")
        return "FAIL", str(report_path)

    monkeypatch.setattr(
        dynamic_v3_confirmation_operations,
        "_pressure_quality_gate",
        fail_quality_gate,
    )
    with pytest.raises(DynamicV3ConfirmationOperationsError, match="data quality gate failed"):
        run_pressure_regime_tagging(
            start=date(2026, 6, 1),
            end=date(2026, 6, 10),
            output_dir=output_dir,
            advisory_outcome_dir=tmp_path / "empty_outcomes",
            prices_path=prices_path,
            rates_path=tmp_path / "rates_daily.csv",
            enforce_data_quality_gate=True,
            generated_at=datetime(2026, 6, 10, tzinfo=UTC),
        )

    assert not output_dir.exists()


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
                "generated_at": "2026-06-01T00:00:00+00:00",
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
