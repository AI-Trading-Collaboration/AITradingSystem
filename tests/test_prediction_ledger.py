from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.prediction_ledger import (
    append_prediction_records,
    build_prediction_outcomes,
    build_prediction_record_from_decision_snapshot,
    build_shadow_maturity_report,
    build_shadow_prediction_records,
    build_shadow_prediction_run_report,
    load_prediction_ledger,
    render_prediction_outcome_report,
    render_shadow_maturity_report,
    render_shadow_prediction_run_report,
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


def test_shadow_runner_derives_candidate_records_without_production_effect(
    tmp_path: Path,
) -> None:
    records = build_shadow_prediction_records(
        snapshot=_decision_snapshot(),
        trace_bundle={"run_manifest": {"run_id": "run:test:2026-04-27"}},
        trace_bundle_path=tmp_path / "trace.json",
        features_path=tmp_path / "features.csv",
        data_quality_report_path=tmp_path / "quality.md",
        rule_experiment_ledger={
            "candidates": [
                {
                    "candidate_id": "rule_experiment:test_gate",
                    "production_effect": "none",
                    "approved_for_production": False,
                    "forward_shadow_plan": {
                        "status": "PENDING",
                        "start_date": "2026-04-27",
                        "end_date": "2026-05-27",
                        "min_observation_days": 5,
                        "production_effect": "none",
                    },
                }
            ]
        },
        as_of=date(2026, 4, 30),
    )

    assert len(records) == 1
    assert records[0]["candidate_id"] == "rule_experiment:test_gate"
    assert records[0]["production_effect"] == "none"
    assert records[0]["label_horizon_days"] == 5
    report_text = render_shadow_prediction_run_report(
        build_shadow_prediction_run_report(
            as_of=date(2026, 4, 30),
            decision_snapshot_path=tmp_path / "snapshot.json",
            trace_bundle_path=tmp_path / "trace.json",
            rule_experiment_path=tmp_path / "rule_experiments.json",
            prediction_ledger_path=tmp_path / "prediction_ledger.csv",
            records=records,
            candidate_count=1,
        )
    )
    assert "production_effect=none" in report_text
    assert "不改变 `scores_daily.csv`" in report_text


def test_shadow_maturity_report_keeps_small_samples_in_shadow(
    tmp_path: Path,
) -> None:
    report = build_shadow_maturity_report(
        outcome_rows=(
            {
                "candidate_id": "production",
                "production_effect": "production",
                "horizon_days": "5",
                "market_regime_id": "ai_after_chatgpt",
                "outcome_status": "AVAILABLE",
                "ai_proxy_return": "0.01",
                "ai_proxy_max_drawdown": "-0.02",
                "hit": "True",
                "excess_SPY_return": "0.005",
            },
            {
                "candidate_id": "rule_experiment:test_gate",
                "production_effect": "none",
                "horizon_days": "5",
                "market_regime_id": "ai_after_chatgpt",
                "outcome_status": "AVAILABLE",
                "ai_proxy_return": "0.03",
                "ai_proxy_max_drawdown": "-0.01",
                "hit": "True",
                "excess_SPY_return": "0.02",
            },
            {
                "candidate_id": "rule_experiment:test_gate",
                "production_effect": "none",
                "horizon_days": "5",
                "market_regime_id": "ai_after_chatgpt",
                "outcome_status": "PENDING",
            },
        ),
        outcomes_path=tmp_path / "prediction_outcomes.csv",
        as_of=date(2026, 5, 5),
        min_available_samples=2,
    )
    markdown = render_shadow_maturity_report(report)
    challenger = next(
        group for group in report.groups if group["candidate_id"] == "rule_experiment:test_gate"
    )

    assert challenger["maturity_status"] == "READY_FOR_SHADOW"
    assert "Forward Shadow 样本成熟度报告" in markdown
    assert "READY_FOR_SHADOW" in markdown


def test_feedback_run_shadow_cli_appends_challenger_predictions(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "decision_snapshot_2026-04-30.json"
    trace_path = tmp_path / "trace.json"
    features_path = tmp_path / "features.csv"
    quality_path = tmp_path / "quality.md"
    rule_experiment_path = tmp_path / "rule_experiments.json"
    ledger_path = tmp_path / "prediction_ledger.csv"
    report_path = tmp_path / "shadow_predictions.md"
    snapshot = _decision_snapshot()
    snapshot["trace"] = {"trace_bundle_path": str(trace_path)}
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    trace_path.write_text(
        json.dumps(
            {
                "run_manifest": {"run_id": "run:test:2026-04-30"},
                "dataset_refs": [
                    {
                        "dataset_type": "processed_feature_cache",
                        "path": str(features_path),
                    }
                ],
                "quality_refs": [{"report_path": str(quality_path)}],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    rule_experiment_path.write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "rule_experiment:test_gate",
                        "production_effect": "none",
                        "approved_for_production": False,
                        "forward_shadow_plan": {
                            "status": "PENDING",
                            "start_date": "2026-04-01",
                            "end_date": "2026-05-31",
                            "production_effect": "none",
                        },
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "run-shadow",
            "--rule-experiment-path",
            str(rule_experiment_path),
            "--decision-snapshot-path",
            str(snapshot_path),
            "--prediction-ledger-path",
            str(ledger_path),
            "--as-of",
            "2026-04-30",
            "--report-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    rows = load_prediction_ledger(ledger_path)
    assert rows[0]["candidate_id"] == "rule_experiment:test_gate"
    assert rows[0]["production_effect"] == "none"
    assert report_path.exists()
    assert "Shadow runner 状态：PASS" in result.output


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
