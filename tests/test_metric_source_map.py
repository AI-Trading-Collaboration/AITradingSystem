from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from dynamic_v3_filtered_candidate_readiness_helpers import assert_research_safe
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_metric_source_map as source_map
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st


def test_metric_source_map_keeps_simulation_contract_insufficient_for_observed_metrics(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    _patch_sim_outcome(monkeypatch)
    price_path = _write_prices(tmp_path)

    result = source_map.run_metric_source_map(
        as_of=date(2026, 6, 17),
        source_variant="limited_adjustment",
        sim_outcome_id="sim-outcome-source-map-test",
        price_cache_path=price_path,
        output_dir=tmp_path / "metric_source_map",
        generated_at=datetime(2026, 6, 17, 5, tzinfo=UTC),
    )
    report = result["metric_source_map_report"]

    assert report["metric_source_map_status"] == "INSUFFICIENT_DATA"
    assert report["source_summary"]["candidate_metric_count"] == 6
    assert report["source_summary"]["baseline_metric_count"] == 5
    assert report["source_summary"]["derivable_now_count"] == 0
    assert report["observed_evidence_status"] == "INSUFFICIENT_DATA"
    assert report["candidate_lineage_status"] == "UNBOUND_SIMULATION_VARIANT"
    assert all(
        row["observed_value"] is None and row["derivable_now"] is False
        for row in [
            *report["candidate_metric_sources"],
            *report["baseline_metric_sources"],
        ]
    )
    assert report["cost_metrics_materialized"] is False
    assert report["benchmark_metrics_materialized"] is False
    assert result["metric_source_map_validation"]["status"] == "PASS"
    assert "metric_source_map_status" in result["reader_brief_section"]
    assert_research_safe(report)


def test_metric_source_map_marks_missing_variant_without_fabrication(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    _patch_sim_outcome(monkeypatch, include_candidate=False)
    price_path = _write_prices(tmp_path)

    result = source_map.run_metric_source_map(
        as_of=date(2026, 6, 17),
        source_variant="limited_adjustment",
        sim_outcome_id="sim-outcome-source-map-test",
        price_cache_path=price_path,
        output_dir=tmp_path / "metric_source_map",
        generated_at=datetime(2026, 6, 17, 6, tzinfo=UTC),
    )
    report = result["metric_source_map_report"]

    assert report["metric_source_map_status"] == "INSUFFICIENT_DATA"
    missing = {
        row["metric_name"]: row["missing_fields"]
        for row in report["candidate_metric_sources"]
        if row["derivable_now"] is False
    }
    assert "summary[source_variant]" in missing["turnover"]
    assert "validated_same_candidate_lineage_dated_metric_source" in missing["turnover"]
    assert "turnover" in report["source_summary"]["missing_metric_names"]
    assert result["metric_source_map_validation"]["status"] == "PASS"
    assert report["cost_metrics_materialized"] is False


def test_metric_source_map_cli_run_report_and_validate(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    _patch_sim_outcome(monkeypatch)
    price_path = _write_prices(tmp_path)
    output_dir = tmp_path / "metric_source_map"

    run = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "metric-source-map",
            "run",
            "--as-of",
            "2026-06-17",
            "--price-cache-path",
            str(price_path),
            "--sim-outcome-id",
            "sim-outcome-source-map-test",
            "--output-dir",
            str(output_dir),
        ],
    )
    assert run.exit_code == 0, run.output
    assert "metric_source_map_status=INSUFFICIENT_DATA" in run.output
    assert "cost_metrics_materialized=false" in run.output
    source_map_id = next(
        line.split("=", 1)[1]
        for line in run.output.splitlines()
        if line.startswith("source_map_id=")
    )

    report = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "metric-source-map",
            "report",
            "--source-map-id",
            source_map_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert report.exit_code == 0, report.output
    assert "derivable_now_count=0" in report.output

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-metric-source-map",
            "--source-map-id",
            source_map_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0, validation.output
    assert "status=PASS" in validation.output


def test_metric_source_map_missing_explicit_sources_is_reproducible(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    def fail_if_called(**kwargs: Any) -> dict[str, Any]:
        raise AssertionError("implicit latest source resolution is forbidden")

    monkeypatch.setattr(
        source_map.sim,
        "backtest_sim_outcome_report_payload",
        fail_if_called,
    )
    missing_price = tmp_path / "missing-prices.csv"
    result = source_map.run_metric_source_map(
        as_of=date(2026, 6, 17),
        source_variant="limited_adjustment",
        price_cache_path=missing_price,
        output_dir=tmp_path / "metric_source_map",
        generated_at=datetime(2026, 6, 17, 5, tzinfo=UTC),
    )

    report = result["metric_source_map_report"]
    assert report["metric_source_map_status"] == "INSUFFICIENT_DATA"
    assert result["input_snapshot"]["price_source"]["exists"] is False
    assert result["input_snapshot"]["sim_source_reference"]["exists"] is False
    assert result["metric_source_map_validation"]["status"] == "PASS"


def test_metric_source_map_tampered_view_fails_closed(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    _patch_sim_outcome(monkeypatch)
    price_path = _write_prices(tmp_path)
    output_dir = tmp_path / "metric_source_map"
    result = source_map.run_metric_source_map(
        as_of=date(2026, 6, 17),
        source_variant="limited_adjustment",
        sim_outcome_id="sim-outcome-source-map-test",
        price_cache_path=price_path,
        output_dir=output_dir,
        generated_at=datetime(2026, 6, 17, 5, tzinfo=UTC),
    )
    report_path = result["source_map_dir"] / "metric_source_map_report.md"
    report_path.write_text("tampered\n", encoding="utf-8")

    validation = source_map.validate_metric_source_map_artifact(
        source_map_id=result["source_map_id"],
        output_dir=output_dir,
        write_output=False,
    )

    assert validation["status"] == "FAIL"


def _patch_sim_outcome(monkeypatch: Any, *, include_candidate: bool = True) -> None:
    def fake_payload(**kwargs: Any) -> dict[str, Any]:
        rows: list[dict[str, Any]] = [
            {
                "variant": "no_trade",
                "avg_5d_return": 0.005236,
                "avg_relative_to_no_trade_5d": 0.0,
                "avg_turnover": 0.0,
                "avg_max_drawdown_20d": -0.036261,
                "event_count": 185,
                "available_count": 730,
            }
        ]
        if include_candidate:
            rows.insert(
                0,
                {
                    "variant": "limited_adjustment",
                    "avg_5d_return": 0.00638,
                    "avg_relative_to_no_trade_5d": 0.001144,
                    "avg_turnover": 0.005945,
                    "avg_max_drawdown_20d": -0.042935,
                    "event_count": 185,
                    "available_count": 730,
                },
            )
        return {
            "schema_version": st.SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_backtest_sim_outcome_manifest",
            "sim_outcome_id": "sim-outcome-source-map-test",
            "sim_outcome_manifest_path": "/tmp/sim_outcome_manifest.json",
            "simulated_variant_summary": {
                "schema_version": st.SCHEMA_VERSION,
                "report_type": "etf_dynamic_v3_backtest_sim_variant_summary",
                "outcome_mode": "BACKTEST_SIMULATION",
                "summary": rows,
                **st.SYSTEM_TARGET_SAFETY,
            },
            "simulated_outcome_windows": [
                {
                    "variant": "no_trade",
                    "window_days": 5,
                    "outcome_status": "AVAILABLE",
                    "start_date": "2026-01-02",
                    "end_date": "2026-01-09",
                    "return": 0.02,
                    "max_drawdown": -0.01,
                },
                {
                    "variant": "no_trade",
                    "window_days": 5,
                    "outcome_status": "AVAILABLE",
                    "start_date": "2026-01-09",
                    "end_date": "2026-01-16",
                    "return": 0.01,
                    "max_drawdown": -0.02,
                },
            ],
            "outcome_mode": "BACKTEST_SIMULATION",
            "production_effect": "none",
            **st.SYSTEM_TARGET_SAFETY,
        }

    monkeypatch.setattr(
        source_map.sim,
        "backtest_sim_outcome_report_payload",
        fake_payload,
    )


def _write_prices(tmp_path: Path) -> Path:
    path = tmp_path / "prices.csv"
    rows = [
        "date,ticker,adj_close",
        "2026-01-02,SPY,100",
        "2026-01-09,SPY,101",
        "2026-01-02,QQQ,100",
        "2026-01-09,QQQ,102",
        "2026-01-02,SMH,100",
        "2026-01-09,SMH,103",
        "2026-01-02,SOXX,100",
        "2026-01-09,SOXX,104",
        "2026-01-02,TLT,100",
        "2026-01-09,TLT,99",
    ]
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path
