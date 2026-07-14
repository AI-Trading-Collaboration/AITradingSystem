from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dynamic_v3_filtered_candidate_readiness_helpers import assert_research_safe
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_metrics_materialization as materialization,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st


def test_benchmark_baseline_metrics_materialization_available_reruns_control(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)

    result = materialization.run_benchmark_baseline_metrics_materialization(
        as_of=date(2026, 6, 17),
        sim_outcome_id=fixture["sim_outcome_id"],
        sim_outcome_dir=fixture["sim_outcome_dir"],
        candidate_metrics_path=fixture["candidate_metrics_path"],
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        cost_sensitivity_review_id=fixture["cost_id"],
        cost_sensitivity_dir=fixture["cost_dir"],
        benchmark_baseline_output_dir=fixture["baseline_control_dir"],
        price_cache_path=fixture["price_path"],
        rates_cache_path=fixture["rates_path"],
        output_dir=fixture["materialization_dir"],
        generated_at=datetime(2026, 6, 17, 3, tzinfo=UTC),
    )
    report = result["benchmark_baseline_metrics_materialization_report"]
    baselines = report["baseline_metrics"]["baselines"]

    assert report["benchmark_baseline_metrics_status"] == "BASELINE_METRICS_AVAILABLE"
    assert report["benchmark_baseline_status"] == "MIXED_BASELINE_RESULT"
    assert {row["baseline_id"] for row in baselines} == {
        "static_allocation",
        "no_trade",
        "qqq_only",
        "spy_only",
        "equal_weight_etf",
    }
    assert report["required_metric_statuses"]["missing_baseline_count"] == 0
    assert report["source_artifacts"]["data_quality_gate"]["status"] == "PASS"
    assert result["benchmark_baseline_metrics_materialization_validation"]["status"] == "PASS"
    assert "benchmark_baseline_metrics_status" in result["reader_brief_section"]
    assert_research_safe(report)
    assert report["benchmark_comparison_live_signal"] is False
    assert report["broker_action_allowed"] is False


def test_benchmark_baseline_metrics_materialization_insufficient_without_candidate_metric(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = _fixture(tmp_path, monkeypatch, include_candidate_metric=False)

    result = materialization.run_benchmark_baseline_metrics_materialization(
        as_of=date(2026, 6, 17),
        source_variant="missing_variant",
        sim_outcome_id=fixture["sim_outcome_id"],
        sim_outcome_dir=fixture["sim_outcome_dir"],
        candidate_metrics_path=fixture["candidate_metrics_path"],
        weekly_review_id=fixture["weekly_id"],
        weekly_review_dir=fixture["weekly_dir"],
        cost_sensitivity_review_id=fixture["cost_id"],
        cost_sensitivity_dir=fixture["cost_dir"],
        benchmark_baseline_output_dir=fixture["baseline_control_dir"],
        price_cache_path=fixture["price_path"],
        rates_cache_path=fixture["rates_path"],
        output_dir=fixture["materialization_dir"],
        generated_at=datetime(2026, 6, 17, 4, tzinfo=UTC),
    )
    report = result["benchmark_baseline_metrics_materialization_report"]

    assert report["benchmark_baseline_metrics_status"] == "INSUFFICIENT_BASELINE_METRICS"
    assert "candidate_metrics:missing_net_performance_proxy" in report["blocking_reasons"]
    assert report["benchmark_baseline_status"] == "INSUFFICIENT_BASELINE_METRICS"
    assert result["benchmark_baseline_metrics_materialization_validation"]["status"] == "PASS"


def test_benchmark_baseline_metrics_materialization_cli_run_report_and_validate(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = _fixture(tmp_path, monkeypatch)

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "benchmark-baseline-metrics-materialization",
            "run",
            "--as-of",
            "2026-06-17",
            "--sim-outcome-id",
            fixture["sim_outcome_id"],
            "--sim-outcome-dir",
            str(fixture["sim_outcome_dir"]),
            "--candidate-metrics-path",
            str(fixture["candidate_metrics_path"]),
            "--weekly-review-id",
            fixture["weekly_id"],
            "--weekly-review-dir",
            str(fixture["weekly_dir"]),
            "--cost-sensitivity-review-id",
            fixture["cost_id"],
            "--cost-sensitivity-dir",
            str(fixture["cost_dir"]),
            "--benchmark-baseline-output-dir",
            str(fixture["baseline_control_dir"]),
            "--price-cache-path",
            str(fixture["price_path"]),
            "--rates-cache-path",
            str(fixture["rates_path"]),
            "--output-dir",
            str(fixture["materialization_dir"]),
        ],
    )
    assert run.exit_code == 0
    assert "benchmark_baseline_metrics_status=BASELINE_METRICS_AVAILABLE" in run.output
    assert "benchmark_baseline_status=MIXED_BASELINE_RESULT" in run.output
    materialization_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("materialization_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "benchmark-baseline-metrics-materialization",
            "report",
            "--materialization-id",
            materialization_id,
            "--output-dir",
            str(fixture["materialization_dir"]),
        ],
    )
    assert report.exit_code == 0
    assert "benchmark_baseline_metrics_status=BASELINE_METRICS_AVAILABLE" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-benchmark-baseline-metrics-materialization",
            "--materialization-id",
            materialization_id,
            "--output-dir",
            str(fixture["materialization_dir"]),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output


@dataclass
class _Quality:
    as_of: date
    status: str = "PASS"
    error_count: int = 0
    warning_count: int = 0

    @property
    def passed(self) -> bool:
        return self.error_count == 0


def _fixture(
    tmp_path: Path,
    monkeypatch: Any,
    *,
    include_candidate_metric: bool = True,
) -> dict[str, Any]:
    fixture = {
        "sim_outcome_id": "sim-outcome-baseline-materialization-test",
        "weekly_id": "paper-shadow-weekly-baseline-materialization-test",
        "cost_id": "cost-sensitivity-review-baseline-materialization-test",
        "sim_outcome_dir": tmp_path / "backtest_sim_outcome",
        "weekly_dir": tmp_path / "paper_shadow_weekly_review",
        "cost_dir": tmp_path / "cost_sensitivity",
        "baseline_control_dir": tmp_path / "benchmark_baseline_control",
        "materialization_dir": tmp_path / "benchmark_baseline_metrics_materialization",
        "price_path": tmp_path / "prices.csv",
        "rates_path": tmp_path / "rates.csv",
        "candidate_metrics_path": tmp_path / "candidate_cost_metrics.json",
    }
    _write_sim_outcome(fixture["sim_outcome_dir"], fixture["sim_outcome_id"])
    _write_weekly_artifact(fixture["weekly_dir"], fixture["weekly_id"])
    _write_cost_artifact(fixture["cost_dir"], fixture["cost_id"], include_candidate_metric)
    _write_candidate_metrics(fixture["candidate_metrics_path"], include_candidate_metric)
    _write_prices(fixture["price_path"])
    fixture["rates_path"].write_text("date,series,value\n2026-01-02,DGS10,4.0\n", encoding="utf-8")

    def fake_gate(**kwargs: Any) -> _Quality:
        Path(kwargs["report_path"]).write_text("# data quality\n", encoding="utf-8")
        return _Quality(as_of=kwargs["as_of"])

    def fake_load_prices(path: Path, *, extra_symbols: set[str] | None = None) -> pd.DataFrame:
        frame = pd.read_csv(path)
        frame["symbol"] = frame["ticker"]
        frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
        frame["_adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
        return frame

    monkeypatch.setattr(materialization, "_run_data_quality_gate", fake_gate)
    monkeypatch.setattr(materialization.sim, "_load_prices", fake_load_prices)
    return fixture


def _write_sim_outcome(root: Path, sim_outcome_id: str) -> None:
    artifact_dir = root / sim_outcome_id
    artifact_dir.mkdir(parents=True)
    windows = [
        {
            "schema_version": st.SCHEMA_VERSION,
            "variant": "no_trade",
            "window_days": 5,
            "outcome_status": "AVAILABLE",
            "start_date": "2026-01-02",
            "end_date": "2026-01-09",
            "return": 0.02,
            "max_drawdown": -0.01,
            "outcome_mode": "BACKTEST_SIMULATION",
            "pit_safety_status": "SIMULATION_NOT_PIT",
            "broker_action_taken": False,
            "production_effect": "none",
        },
        {
            "schema_version": st.SCHEMA_VERSION,
            "variant": "no_trade",
            "window_days": 5,
            "outcome_status": "AVAILABLE",
            "start_date": "2026-01-09",
            "end_date": "2026-01-16",
            "return": 0.01,
            "max_drawdown": -0.02,
            "outcome_mode": "BACKTEST_SIMULATION",
            "pit_safety_status": "SIMULATION_NOT_PIT",
            "broker_action_taken": False,
            "production_effect": "none",
        },
    ]
    summary = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_variant_summary",
        "outcome_mode": "BACKTEST_SIMULATION",
        "summary": [
            {
                "variant": "limited_adjustment",
                "avg_5d_return": 0.025,
                "avg_turnover": 0.005,
                "avg_max_drawdown_20d": -0.03,
                "available_count": 2,
                "event_count": 2,
            },
            {
                "variant": "no_trade",
                "avg_5d_return": 0.015,
                "avg_turnover": 0.0,
                "avg_max_drawdown_20d": -0.02,
                "available_count": 2,
                "event_count": 2,
            },
        ],
        "production_effect": "none",
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_outcome_manifest",
        "sim_outcome_id": sim_outcome_id,
        "event_set_id": "",
        "variant_set_id": "variant-set-test",
        "as_of": "2026-06-17",
        "status": "PASS",
        "outcome_mode": "BACKTEST_SIMULATION",
        "pit_safety_status": "SIMULATION_NOT_PIT",
        "sim_outcome_manifest_path": str(artifact_dir / "sim_outcome_manifest.json"),
        "simulated_outcome_windows_path": str(artifact_dir / "simulated_outcome_windows.jsonl"),
        "simulated_variant_summary_path": str(artifact_dir / "simulated_variant_summary.json"),
        "outcome_input_snapshot_path": str(artifact_dir / "outcome_input_snapshot.json"),
        "backtest_sim_outcome_report_path": str(artifact_dir / "backtest_sim_outcome_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "not_for_production": True,
    }
    _write_json(artifact_dir / "sim_outcome_manifest.json", manifest)
    _write_json(artifact_dir / "simulated_variant_summary.json", summary)
    _write_json(
        artifact_dir / "outcome_input_snapshot.json",
        {
            "schema_version": (
                materialization.sim.BACKTEST_SIM_OUTCOME_SNAPSHOT_SCHEMA_VERSION
            ),
            "generated_at": "2026-06-17T02:00:00+00:00",
            "event_set_id": "synthetic-event-set-test",
            "variant_set_id": "variant-set-test",
            "fixture_scope": "benchmark_baseline_metrics_materialization_read_contract",
            "production_effect": "none",
        },
    )
    (artifact_dir / "simulated_outcome_windows.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in windows),
        encoding="utf-8",
    )
    (artifact_dir / "backtest_sim_outcome_report.md").write_text("# outcome\n", encoding="utf-8")


def _write_weekly_artifact(root: Path, weekly_id: str) -> None:
    artifact_dir = root / weekly_id
    artifact_dir.mkdir(parents=True)
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_manifest",
        "weekly_review_id": weekly_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "week_start": "2026-06-08",
        "week_end": "2026-06-12",
        "status": "PASS",
        "weekly_decision": "CONTINUE",
        "paper_shadow_weekly_manifest_path": str(
            artifact_dir / "paper_shadow_weekly_manifest.json"
        ),
        "paper_shadow_weekly_review_path": str(
            artifact_dir / "paper_shadow_weekly_review.json"
        ),
        "paper_shadow_weekly_report_path": str(
            artifact_dir / "paper_shadow_weekly_report.md"
        ),
        "reader_brief_section_path": str(artifact_dir / "reader_brief_section.md"),
        "validation_path": str(artifact_dir / "paper_shadow_weekly_validation.json"),
        **materialization.BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_weekly_review",
        "weekly_review_id": weekly_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "week_start": "2026-06-08",
        "week_end": "2026-06-12",
        "weekly_decision": "CONTINUE",
        "coverage_status": "PASS",
        **materialization.BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": weekly_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        **materialization.BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }
    _write_json(artifact_dir / "paper_shadow_weekly_manifest.json", manifest)
    _write_json(artifact_dir / "paper_shadow_weekly_review.json", review)
    _write_json(artifact_dir / "paper_shadow_weekly_validation.json", validation)
    (artifact_dir / "paper_shadow_weekly_report.md").write_text("# weekly\n", encoding="utf-8")
    (artifact_dir / "reader_brief_section.md").write_text("# weekly reader\n", encoding="utf-8")


def _write_cost_artifact(root: Path, review_id: str, include_candidate_metric: bool) -> None:
    artifact_dir = root / review_id
    artifact_dir.mkdir(parents=True)
    scenario_results = (
        [
            {
                "scenario_id": "high",
                "label": "High Cost",
                "net_performance_proxy": 0.018,
                "net_improvement_proxy": 0.003,
                "production_effect": "none",
                "broker_action_allowed": False,
            }
        ]
        if include_candidate_metric
        else []
    )
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_sensitivity_manifest",
        "review_id": review_id,
        "cost_sensitivity_report_path": str(artifact_dir / "cost_sensitivity_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    review = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_cost_sensitivity_review",
        "review_id": review_id,
        "candidate": "median_plus_regime_mismatch_filter",
        "cost_sensitivity_status": "NOT_MEANINGFUL_UNDER_COSTS",
        "scenario_results": scenario_results,
        "high_cost_improvement_meaningful": False,
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    validation = {
        "schema_version": st.SCHEMA_VERSION,
        "artifact_id": review_id,
        "status": "PASS",
        "failed_check_count": 0,
        "checks": [],
        "production_effect": "none",
        "broker_action_allowed": False,
    }
    _write_json(artifact_dir / "cost_sensitivity_manifest.json", manifest)
    _write_json(artifact_dir / "cost_sensitivity_review.json", review)
    _write_json(artifact_dir / "cost_sensitivity_validation.json", validation)
    (artifact_dir / "reader_brief_section.md").write_text("# cost reader\n", encoding="utf-8")
    (artifact_dir / "cost_sensitivity_report.md").write_text("# cost\n", encoding="utf-8")


def _write_candidate_metrics(path: Path, include_candidate_metric: bool) -> None:
    payload = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_candidate_cost_metrics",
        "metrics_id": "candidate-cost-metrics-test",
        "candidate": "median_plus_regime_mismatch_filter",
        "as_of": "2026-06-17",
        "source_variant": "limited_adjustment",
        "turnover": 0.005,
        "drawdown_proxy": -0.03,
        "trade_rotation_count": 2,
        "outcome_mode": "BACKTEST_SIMULATION",
        **materialization.BENCHMARK_BASELINE_METRICS_MATERIALIZATION_SAFETY,
    }
    if include_candidate_metric:
        payload["gross_performance_proxy"] = 0.025
    _write_json(path, payload)


def _write_prices(path: Path) -> None:
    rows = []
    prices = {
        "QQQ": [100.0, 104.0, 108.0],
        "SPY": [100.0, 102.0, 104.0],
        "SMH": [100.0, 110.0, 121.0],
        "SOXX": [100.0, 110.0, 121.0],
        "TLT": [100.0, 100.0, 100.0],
    }
    for symbol, values in prices.items():
        for date_text, value in zip(
            ("2026-01-02", "2026-01-09", "2026-01-16"),
            values,
            strict=True,
        ):
            rows.append(
                {
                    "date": date_text,
                    "ticker": symbol,
                    "open": value,
                    "high": value,
                    "low": value,
                    "close": value,
                    "adj_close": value,
                    "volume": 1000,
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
