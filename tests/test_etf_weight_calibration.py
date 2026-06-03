from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.data import standardize_price_frame, validate_price_data
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.etf_portfolio.weight_calibration import (
    DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH,
    DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH,
    DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH,
    ETFWeightCalibrationPresetRegistry,
    ETFWeightSearchRegistry,
    WeightCalibrationError,
    build_backtest_forward_evidence_aggregation,
    build_candidate_weight_proposals,
    build_dual_track_weight_calibration_report,
    build_dual_track_weight_calibration_validation_report,
    build_weight_candidate_comparison_table,
    build_weight_overfit_diagnostics,
    build_weight_top_candidate_export,
    enroll_candidate_weights_forward,
    generate_weight_candidates,
    load_candidate_weight_registry,
    load_weight_calibration_preset,
    load_weight_calibration_preset_registry,
    load_weight_forward_enrollments,
    load_weight_search_definition,
    load_weight_search_registry,
    register_candidate_weight_sets,
    resolve_weight_calibration_preset,
    run_historical_weight_search,
    summarize_weight_robustness,
    update_candidate_weight_status,
    validate_backtest_forward_evidence_record,
    validate_candidate_weight_proposal,
    validate_candidate_weight_record,
    validate_dual_track_weight_calibration_report,
    validate_dual_track_weight_calibration_validation_report,
    validate_weight_candidate_comparison_table,
    validate_weight_forward_enrollment_record,
    validate_weight_search_registry,
    validate_weight_top_candidate_export,
    weight_overfit_risk_band,
    write_backtest_forward_evidence_aggregation,
    write_dual_track_weight_calibration_report,
    write_dual_track_weight_calibration_validation_report,
    write_weight_candidate_comparison_table,
    write_weight_search_run,
    write_weight_top_candidate_export,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_weight_search_config_loads_default() -> None:
    registry = load_weight_search_registry()
    search = load_weight_search_definition("etf_initial_weight_search_v1")

    assert registry.schema_version == "etf_weight_search_v1"
    assert registry.config_hash
    assert search.search_id == "etf_initial_weight_search_v1"
    assert search.universe == ["SPY", "QQQ", "SMH", "SOXX", "CASH"]
    assert search.grid_step == 0.05
    assert search.max_candidate_count == 1000
    assert search.safety.production_effect == "none"
    assert search.safety.broker_action == "none"


def test_weight_calibration_presets_load_default() -> None:
    presets = load_weight_calibration_preset_registry()

    assert presets.schema_version == "etf_weight_calibration_presets_v1"
    assert set(presets.presets) == {
        "last_2y",
        "last_3y",
        "last_5y",
        "post_2022_bear",
        "ai_cycle_recent",
        "full_available",
    }
    assert presets.presets["ai_cycle_recent"].start_date == "2022-12-01"
    assert presets.presets["ai_cycle_recent"].minimum_coverage_ratio == 0.95
    assert presets.presets["ai_cycle_recent"].safety.production_effect == "none"


def test_weight_calibration_preset_rolling_policy_resolves() -> None:
    preset = load_weight_calibration_preset("last_3y")

    resolved = resolve_weight_calibration_preset(
        preset,
        as_of=date(2026, 6, 3),
        available_end=date(2026, 6, 2),
    )

    assert resolved["preset_id"] == "last_3y"
    assert resolved["start_date"] == date(2023, 6, 2)
    assert resolved["end_date"] == date(2026, 6, 2)
    assert resolved["production_effect"] == "none"


def test_weight_calibration_preset_full_available_requires_available_start() -> None:
    preset = load_weight_calibration_preset("full_available")

    with pytest.raises(WeightCalibrationError, match="available_start"):
        resolve_weight_calibration_preset(
            preset,
            available_end=date(2026, 6, 2),
        )

    resolved = resolve_weight_calibration_preset(
        preset,
        available_start=date(2020, 1, 2),
        available_end=date(2026, 6, 2),
    )

    assert resolved["start_date"] == date(2020, 1, 2)
    assert resolved["end_date"] == date(2026, 6, 2)


def test_weight_calibration_preset_unknown_required_asset_fails(tmp_path: Path) -> None:
    raw = _raw_presets()
    raw["presets"]["ai_cycle_recent"]["minimum_required_assets"].append("MISSING")

    with pytest.raises(WeightCalibrationError, match="unknown symbols"):
        load_weight_calibration_preset_registry(
            _write_presets_config(raw, tmp_path / "presets.yaml"),
        )


def test_weight_calibration_preset_coverage_threshold_valid() -> None:
    raw = _raw_presets()
    raw["presets"]["ai_cycle_recent"]["minimum_coverage_ratio"] = 1.20

    with pytest.raises(ValueError, match="minimum_coverage_ratio"):
        ETFWeightCalibrationPresetRegistry.model_validate(raw)


def test_weight_calibration_preset_invalid_date_range_fails() -> None:
    raw = _raw_presets()
    raw["presets"]["post_2022_bear"]["start_date"] = "2024-01-01"
    raw["presets"]["post_2022_bear"]["end_date_policy"] = "fixed:2023-10-31"

    with pytest.raises(ValueError, match="invalid range"):
        ETFWeightCalibrationPresetRegistry.model_validate(raw)


def test_weight_calibration_preset_unsafe_safety_fields_fail() -> None:
    raw = _raw_presets()
    raw["presets"]["ai_cycle_recent"]["safety"]["broker_action"] = "place_order"

    with pytest.raises(ValueError, match="broker_action"):
        ETFWeightCalibrationPresetRegistry.model_validate(raw)


def test_weight_search_constraints_are_valid() -> None:
    search = load_weight_search_definition("etf_initial_weight_search_v1")

    assert set(search.weight_constraints) == set(search.universe)
    for symbol, constraint in search.weight_constraints.items():
        assert 0.0 <= constraint.min <= constraint.max <= 1.0, symbol
    assert search.sleeve_constraints.semiconductor_total_max == 0.35
    assert search.sleeve_constraints.cash_min_when_risk_off == 0.30


def test_weight_search_universe_symbols_exist() -> None:
    raw = _raw_registry()
    search = raw["weight_searches"]["etf_initial_weight_search_v1"]
    search["universe"].append("MISSING")
    search["weight_constraints"]["MISSING"] = {"min": 0.0, "max": 0.1}
    registry = ETFWeightSearchRegistry.model_validate(raw)

    with pytest.raises(WeightCalibrationError, match="unknown symbols"):
        validate_weight_search_registry(registry, etf_config=load_etf_config_bundle())


def test_weight_search_benchmark_set_exists() -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["benchmark_set"] = "missing"

    with pytest.raises(ValueError, match="unknown benchmark_set"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_benchmark_ids_exist_in_etf_backtest_config() -> None:
    raw = _raw_registry()
    raw["benchmark_sets"]["standard_etf"]["benchmark_ids"].append("B999")
    registry = ETFWeightSearchRegistry.model_validate(raw)

    with pytest.raises(WeightCalibrationError, match="unknown ETF benchmark ids"):
        validate_weight_search_registry(registry, etf_config=load_etf_config_bundle())


def test_weight_search_safety_fields_are_required() -> None:
    raw = _raw_registry()
    del raw["weight_searches"]["etf_initial_weight_search_v1"]["safety"][
        "manual_review_required"
    ]

    with pytest.raises(ValueError, match="manual_review_required"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_unsafe_production_effect_fails() -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["safety"][
        "production_effect"
    ] = "apply_weights"

    with pytest.raises(ValueError, match="production_effect"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_invalid_grid_step_fails() -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["grid_step"] = 0.03

    with pytest.raises(ValueError, match="grid_step"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_invalid_constraint_order_fails() -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["weight_constraints"]["SPY"] = {
        "min": 0.70,
        "max": 0.60,
    }

    with pytest.raises(ValueError, match="min must be <= max"):
        ETFWeightSearchRegistry.model_validate(raw)


def test_weight_search_config_cli_validate_smoke() -> None:
    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "validate-config",
            "--search",
            "etf_initial_weight_search_v1",
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "search_id=etf_initial_weight_search_v1" in result.output
    assert "objective_policy=robust_risk_adjusted_v1" in result.output
    assert "observe_only=true" in result.output
    assert "candidate_only=true" in result.output
    assert "production_effect=none" in result.output
    assert "broker_action=none" in result.output
    assert "manual_review_required=true" in result.output


def test_weight_search_bounded_grid_generates_valid_candidates() -> None:
    config = load_etf_config_bundle()
    search = load_weight_search_definition("etf_initial_weight_search_v1")

    candidates, generation = generate_weight_candidates(
        search,
        etf_config=config,
        max_candidates=12,
    )

    assert len(candidates) == 12
    assert generation["total_valid_candidate_count"] > len(candidates)
    assert generation["truncated_by_candidate_limit"] is True
    for weights in candidates:
        assert round(sum(weights.values()), 10) == 1.0
        assert weights["SMH"] + weights["SOXX"] <= 0.35
        for symbol, weight in weights.items():
            constraint = search.weight_constraints[symbol]
            assert constraint.min <= weight <= constraint.max


def test_weight_search_rejects_candidate_limit_above_config() -> None:
    config = load_etf_config_bundle()
    search = load_weight_search_definition("etf_initial_weight_search_v1")

    with pytest.raises(WeightCalibrationError, match="max_candidate_count"):
        generate_weight_candidates(
            search,
            etf_config=config,
            max_candidates=search.max_candidate_count + 1,
        )


def test_historical_weight_search_runs_candidate_backtests() -> None:
    config, prices, quality_report = _search_inputs()
    registry = load_weight_search_registry(etf_config=config)

    run = run_historical_weight_search(
        prices,
        etf_config=config,
        quality_report=quality_report,
        registry=registry,
        search_id="etf_initial_weight_search_v1",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        max_candidates=6,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    payload = run.payload
    assert payload["schema_version"] == "etf_weight_search_run_v1"
    assert payload["search_run_id"] == "etf-weight-search-20260602T000000Z"
    assert payload["candidate_generation"]["evaluated_candidate_count"] == 6
    assert len(payload["candidate_weight_sets"]) == 6
    assert len(payload["metrics"]) == 6
    assert len(payload["ranking"]) == 6
    assert payload["baseline_weight_set"]["weights"]["QQQ"] == 0.40
    assert payload["benchmark_set"]["benchmark_metrics"]["B001"]["status"] == "AVAILABLE"
    assert payload["production_weights_mutated"] is False
    assert payload["applied_weight_set"] is None
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"


def test_weight_search_includes_walk_forward_and_regime_robustness() -> None:
    config, prices, quality_report = _search_inputs()
    registry = load_weight_search_registry(etf_config=config)

    payload = run_historical_weight_search(
        prices,
        etf_config=config,
        quality_report=quality_report,
        registry=registry,
        search_id="etf_initial_weight_search_v1",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        max_candidates=4,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    ).payload

    robustness = payload["robustness_evaluation"]
    assert set(robustness["evaluation_modes"]) >= {
        "full_period",
        "walk_forward_windows",
        "risk_on_periods",
        "neutral_periods",
        "risk_off_periods",
        "high_volatility_periods",
        "semiconductor_leadership_periods",
        "growth_underperformance_periods",
    }
    candidate = robustness["candidate_evaluations"][0]
    slice_by_id = {row["slice_id"]: row for row in candidate["slice_metrics"]}
    assert slice_by_id["full_period"]["status"] == "AVAILABLE"
    assert slice_by_id["full_period"]["return"] is not None
    assert slice_by_id["full_period"]["volatility"] is not None
    assert slice_by_id["full_period"]["cash_exposure"] is not None
    assert slice_by_id["full_period"]["semiconductor_exposure"] is not None
    assert slice_by_id["full_period"]["constraint_hit_rate"] == 0.0
    assert "ai_after_chatgpt_initial" in slice_by_id
    assert slice_by_id["ai_after_chatgpt_recent"]["status"] == "INSUFFICIENT_DATA"
    assert "risk_on_periods" in slice_by_id
    assert "growth_underperformance_periods" in slice_by_id
    metric_row = payload["metrics"][0]
    assert metric_row["robustness_summary"]["stability_score"] == (
        metric_row["component_scores"]["regime_robustness_score"]
    )
    assert payload["robustness_evaluation"]["summary"]["candidate_count"] == 4


def test_weight_robustness_penalizes_unstable_slices() -> None:
    objective = load_weight_search_registry().objective_policies["robust_risk_adjusted_v1"]
    stable = summarize_weight_robustness(
        [
            _slice_metric("risk_on_periods", 0.03, 0.01),
            _slice_metric("neutral_periods", 0.02, 0.00),
            _slice_metric("high_volatility_periods", 0.01, -0.01),
        ],
        objective=objective,
    )
    unstable = summarize_weight_robustness(
        [
            _slice_metric("risk_on_periods", 0.05, 0.02),
            _slice_metric("neutral_periods", -0.04, -0.05),
            _slice_metric("risk_off_periods", -0.03, -0.04),
        ],
        objective=objective,
    )

    assert stable["stability_score"] > unstable["stability_score"]
    assert stable["weak_slice_count"] == 0
    assert unstable["weak_slice_count"] == 2
    assert "WEAK_ROBUSTNESS_SLICES_PRESENT" in unstable["reason_codes"]


def test_historical_weight_search_ranking_is_deterministic() -> None:
    config, prices, quality_report = _search_inputs()
    registry = load_weight_search_registry(etf_config=config)
    kwargs = {
        "prices": prices,
        "etf_config": config,
        "quality_report": quality_report,
        "registry": registry,
        "search_id": "etf_initial_weight_search_v1",
        "start": date(2022, 12, 1),
        "end": date(2022, 12, 20),
        "max_candidates": 6,
        "generated_at": datetime(2026, 6, 2, tzinfo=UTC),
    }

    first = run_historical_weight_search(**kwargs).payload
    second = run_historical_weight_search(**kwargs).payload

    assert first["ranking"] == second["ranking"]
    assert first["metrics"] == second["metrics"]


def test_weight_search_runtime_output_schema_is_stable(tmp_path: Path) -> None:
    config, prices, quality_report = _search_inputs()
    registry = load_weight_search_registry(etf_config=config)
    run = run_historical_weight_search(
        prices,
        etf_config=config,
        quality_report=quality_report,
        registry=registry,
        search_id="etf_initial_weight_search_v1",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        max_candidates=4,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    paths = write_weight_search_run(
        run,
        report_root=tmp_path / "reports",
        data_root=tmp_path / "data",
    )

    assert paths["summary_json"].exists()
    assert paths["summary_md"].exists()
    assert paths["metrics_csv"].exists()
    assert paths["ranking_json"].exists()
    assert paths["robustness_json"].exists()
    assert paths["candidates_json"].exists()
    assert paths["candidates_csv"].exists()
    written = json.loads(paths["summary_json"].read_text(encoding="utf-8"))
    assert tuple(written)[:6] == (
        "applied_weight_set",
        "baseline_weight_set",
        "benchmark_set",
        "blocked_candidates",
        "broker_action",
        "candidate_generation",
    )
    assert "## Top Historical Candidates" in paths["summary_md"].read_text(encoding="utf-8")


def test_weight_search_cli_search_writes_outputs(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    _make_prices(days=360).to_csv(prices_path, index=False)
    output_dir = tmp_path / "reports"
    data_dir = tmp_path / "data"

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "search",
            "--search",
            "etf_initial_weight_search_v1",
            "--prices-path",
            str(prices_path),
            "--start",
            "2022-12-01",
            "--end",
            "2022-12-20",
            "--max-candidates",
            "4",
            "--output-dir",
            str(output_dir),
            "--data-output-dir",
            str(data_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF weight calibration search 完成" in result.output
    assert "evaluated_candidate_count=4" in result.output
    assert "production_effect=none" in result.output
    assert list(output_dir.glob("*/summary.json"))
    assert list(data_dir.glob("*/candidate_weight_sets.json"))


def test_weight_search_cli_search_accepts_historical_preset(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    _make_prices(days=900).to_csv(prices_path, index=False)
    output_dir = tmp_path / "reports"
    data_dir = tmp_path / "data"

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "search",
            "--search",
            "etf_initial_weight_search_v1",
            "--preset",
            "ai_cycle_recent",
            "--prices-path",
            str(prices_path),
            "--max-candidates",
            "4",
            "--output-dir",
            str(output_dir),
            "--data-output-dir",
            str(data_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "preset_id=ai_cycle_recent" in result.output
    written = json.loads(next(output_dir.glob("*/summary.json")).read_text(encoding="utf-8"))
    assert written["historical_range_preset"]["preset_id"] == "ai_cycle_recent"
    assert written["requested_date_range"]["start"] == "2022-12-01"
    assert written["historical_range_preset"]["production_effect"] == "none"


def test_weight_top_candidate_export_contains_required_fields() -> None:
    run = _small_search_run()

    payload = build_weight_top_candidate_export(
        run.payload,
        top=3,
        generated_at=datetime(2026, 6, 3, tzinfo=UTC),
    )

    assert payload["schema_version"] == "etf_weight_top_candidate_export_v1"
    assert payload["exported_candidate_count"] == 3
    assert payload["production_effect"] == "none"
    first = payload["candidates"][0]
    assert first["rank"] == 1
    assert first["weight_set_id"].startswith(run.run_id)
    assert round(sum(first["weights"].values()), 10) == 1.0
    assert "historical_score" in first
    assert "benchmark_excess_return" in first
    assert "drawdown_reduction_vs_QQQ" in first
    assert first["overfit_risk"] in {"low", "medium", "high", "critical"}
    assert first["forward_readiness_status"] in {
        "shadow_ready",
        "needs_manual_review",
        "needs_more_historical_validation",
        "blocked_by_risk",
        "blocked_by_data_quality",
        "blocked_by_overfit_risk",
    }
    assert first["production_effect"] == "none"
    validate_weight_top_candidate_export(payload)


def test_weight_top_candidate_export_ranking_is_stable() -> None:
    run = _small_search_run()

    first = build_weight_top_candidate_export(run.payload, top=3)
    second = build_weight_top_candidate_export(run.payload, top=3)

    assert [row["weight_set_id"] for row in first["candidates"]] == [
        row["weight_set_id"] for row in second["candidates"]
    ]


def test_weight_top_candidate_export_writes_json_csv_markdown(tmp_path: Path) -> None:
    payload = build_weight_top_candidate_export(_small_search_run().payload, top=2)

    paths = write_weight_top_candidate_export(payload, output_dir=tmp_path / "top")

    assert paths["json"].exists()
    assert paths["csv"].exists()
    assert paths["markdown"].exists()
    assert "ETF Weight Top-N Candidate Export" in paths["markdown"].read_text(
        encoding="utf-8"
    )
    csv_text = paths["csv"].read_text(encoding="utf-8")
    assert "forward_readiness_status" in csv_text
    assert "production_effect" in csv_text


def test_weight_top_candidate_export_cli_latest_and_run_id(tmp_path: Path) -> None:
    run = _small_search_run()
    report_root = tmp_path / "reports"
    data_root = tmp_path / "data"
    write_weight_search_run(run, report_root=report_root, data_root=data_root)

    latest_result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "export-top",
            "--latest",
            "--top",
            "2",
            "--output-dir",
            str(report_root),
            "--export-dir",
            str(tmp_path / "exports_latest"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert latest_result.exit_code == 0, latest_result.output
    assert "exported_candidate_count=2" in latest_result.output
    assert "production_effect=none" in latest_result.output
    assert list((tmp_path / "exports_latest").glob("*.json"))
    assert list((tmp_path / "exports_latest").glob("*.csv"))
    assert list((tmp_path / "exports_latest").glob("*.md"))

    run_id_result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "export-top",
            "--run-id",
            run.run_id,
            "--top",
            "1",
            "--output-dir",
            str(report_root),
            "--export-dir",
            str(tmp_path / "exports_run_id"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert run_id_result.exit_code == 0, run_id_result.output
    assert "exported_candidate_count=1" in run_id_result.output


def test_weight_candidate_comparison_includes_benchmarks_and_top_candidates() -> None:
    run = _small_search_run()
    top_export = build_weight_top_candidate_export(run.payload, top=2)

    payload = build_weight_candidate_comparison_table(
        run.payload,
        top_export_payload=top_export,
        top=2,
        generated_at=datetime(2026, 6, 3, tzinfo=UTC),
    )

    assert payload["schema_version"] == "etf_weight_candidate_comparison_v1"
    row_ids = [row["candidate_id"] for row in payload["comparison_rows"]]
    assert row_ids[:4] == [
        "current_baseline",
        "buy_hold_QQQ",
        "buy_hold_SPY",
        "buy_hold_SMH",
    ]
    assert any(
        row["row_type"] == "static_reference_candidate"
        for row in payload["comparison_rows"]
    )
    top_rows = [
        row for row in payload["comparison_rows"] if row["row_type"] == "top_N_weight_candidate"
    ]
    assert len(top_rows) == 2
    assert top_rows[0]["weights"]
    assert "CASH" in top_rows[0]["weights"]
    assert "excess_return_vs_QQQ" in top_rows[0]
    assert top_rows[0]["overfit_risk"] in {"low", "medium", "high", "critical"}
    assert payload["production_effect"] == "none"
    validate_weight_candidate_comparison_table(payload)


def test_weight_candidate_comparison_handles_missing_metric_with_reason() -> None:
    run = _small_search_run()
    payload = deepcopy(run.payload)
    payload["metrics"][0]["Sharpe"] = None

    comparison = build_weight_candidate_comparison_table(payload, top=1)

    top_row = next(
        row
        for row in comparison["comparison_rows"]
        if row["row_type"] == "top_N_weight_candidate"
    )
    assert top_row["Sharpe"] is None
    assert top_row["metric_null_reasons"]["Sharpe"] == "metric_not_available"


def test_weight_candidate_comparison_writes_json_csv_markdown(tmp_path: Path) -> None:
    payload = build_weight_candidate_comparison_table(_small_search_run().payload, top=2)

    paths = write_weight_candidate_comparison_table(payload, output_dir=tmp_path / "comparison")

    assert paths["json"].exists()
    assert paths["csv"].exists()
    assert paths["markdown"].exists()
    assert "ETF Weight Candidate Comparison Table" in paths["markdown"].read_text(
        encoding="utf-8"
    )
    csv_text = paths["csv"].read_text(encoding="utf-8")
    assert "current_baseline" in csv_text
    assert "buy_hold_QQQ" in csv_text


def test_weight_candidate_comparison_cli_writes_outputs(tmp_path: Path) -> None:
    run = _small_search_run()
    report_root = tmp_path / "reports"
    data_root = tmp_path / "data"
    write_weight_search_run(run, report_root=report_root, data_root=data_root)

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "comparison",
            "--latest",
            "--top",
            "2",
            "--output-dir",
            str(report_root),
            "--comparison-dir",
            str(tmp_path / "comparison"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "row_count=" in result.output
    assert "production_effect=none" in result.output
    assert list((tmp_path / "comparison").glob("*.json"))
    assert list((tmp_path / "comparison").glob("*.csv"))
    assert list((tmp_path / "comparison").glob("*.md"))


def test_candidate_weight_registry_writes_candidate_records(tmp_path: Path) -> None:
    run = _small_search_run()
    registry_path = tmp_path / "candidate_weight_registry.json"

    registry = register_candidate_weight_sets(
        run.payload,
        registry_path=registry_path,
        top=2,
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert registry_path.exists()
    assert registry["candidate_count"] == 2
    record = registry["weight_sets"][0]
    assert record["weight_set_id"].startswith(run.run_id)
    assert record["source_search_run_id"] == run.run_id
    assert record["rank"] in {1, 2}
    assert record["status"] in {"candidate", "needs_more_data", "blocked", "rejected"}
    assert round(sum(record["weights"].values()), 10) == 1.0
    assert record["metrics_summary"]["candidate_score"] is not None
    assert record["robustness_summary"]["stability_score"] is not None
    assert record["production_effect"] == "none"
    assert record["broker_action"] == "none"


def test_candidate_weight_registry_duplicate_weight_set_is_idempotent(tmp_path: Path) -> None:
    run = _small_search_run()
    registry_path = tmp_path / "candidate_weight_registry.json"

    first = register_candidate_weight_sets(run.payload, registry_path=registry_path, top=1)
    second = register_candidate_weight_sets(run.payload, registry_path=registry_path, top=1)

    assert first["candidate_count"] == 1
    assert second["candidate_count"] == 1
    assert load_candidate_weight_registry(registry_path)["candidate_count"] == 1


def test_candidate_weight_registry_rejects_unsafe_status(tmp_path: Path) -> None:
    record = _candidate_weight_record(tmp_path)
    record["status"] = "production"

    with pytest.raises(WeightCalibrationError, match="status"):
        validate_candidate_weight_record(record)


def test_candidate_weight_registry_requires_production_effect_none(tmp_path: Path) -> None:
    record = _candidate_weight_record(tmp_path)
    record["production_effect"] = "apply_weights"

    with pytest.raises(WeightCalibrationError, match="production_effect"):
        validate_candidate_weight_record(record)


def test_candidate_weight_registry_requires_weights_sum_to_one(tmp_path: Path) -> None:
    record = _candidate_weight_record(tmp_path)
    record["weights"]["CASH"] = 0.42

    with pytest.raises(WeightCalibrationError, match="weights_sum"):
        validate_candidate_weight_record(record)


def test_blocked_candidate_weight_cannot_be_shadow_ready(tmp_path: Path) -> None:
    record = _candidate_weight_record(tmp_path)
    record["blockers"] = ["TURNOVER_TOO_HIGH"]
    record["status"] = "shadow_ready"

    with pytest.raises(WeightCalibrationError, match="blocked_candidate_cannot"):
        validate_candidate_weight_record(record)


def test_candidate_weight_status_update_blocks_shadow_ready_for_blocked_record(
    tmp_path: Path,
) -> None:
    run = _small_search_run()
    registry_path = tmp_path / "candidate_weight_registry.json"
    registry = register_candidate_weight_sets(run.payload, registry_path=registry_path, top=1)
    record = registry["weight_sets"][0]
    record["blockers"] = ["TURNOVER_TOO_HIGH"]

    with pytest.raises(WeightCalibrationError, match="blocked_candidate_cannot"):
        update_candidate_weight_status(
            registry,
            weight_set_id=record["weight_set_id"],
            status="shadow_ready",
        )


def test_weight_calibration_register_candidates_cli(tmp_path: Path) -> None:
    run = _small_search_run()
    report_root = tmp_path / "reports"
    data_root = tmp_path / "data"
    registry_path = tmp_path / "candidate_weight_registry.json"
    write_weight_search_run(run, report_root=report_root, data_root=data_root)

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "register-candidates",
            "--run-id",
            run.run_id,
            "--output-dir",
            str(report_root),
            "--registry-path",
            str(registry_path),
            "--top",
            "2",
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF candidate weight registry" in result.output
    assert "candidate_count=2" in result.output
    assert "production_effect=none" in result.output
    assert load_candidate_weight_registry(registry_path)["candidate_count"] == 2


def test_forward_enrollment_enrolls_candidate_weight_set(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=2)
    enrollment_path = tmp_path / "forward_enrollments.json"

    enrollment = enroll_candidate_weights_forward(
        registry,
        enrollment_path=enrollment_path,
        top=1,
        enrolled_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert enrollment_path.exists()
    assert enrollment["schema_version"] == "etf_weight_forward_enrollment_v1"
    assert enrollment["enrollment_count"] == 1
    record = enrollment["enrollments"][0]
    assert record["weight_set_id"] == registry["weight_sets"][0]["weight_set_id"]
    assert record["status"] == "active"
    assert record["tracking_state"]["tracking_status"] == "active"
    assert record["tracking_state"]["evidence_status"] == "needs_more_forward_data"
    assert record["production_effect"] == "none"
    assert record["broker_action"] == "none"
    assert record["production_weights_mutated"] is False


def test_forward_enrollment_blocks_blocked_candidate(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["status"] = "blocked"
    registry["weight_sets"][0]["blockers"] = ["TURNOVER_TOO_HIGH"]

    with pytest.raises(WeightCalibrationError, match="cannot enroll forward"):
        enroll_candidate_weights_forward(
            registry,
            enrollment_path=tmp_path / "forward_enrollments.json",
            top=1,
        )


def test_forward_enrollment_shadow_record_includes_weight_set_id(
    tmp_path: Path,
) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)

    enrollment = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / DEFAULT_WEIGHT_FORWARD_ENROLLMENT_PATH.name,
        top=1,
        enrolled_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    record = enrollment["enrollments"][0]
    shadow = record["shadow_record"]
    assert shadow["record_type"] == "etf_weight_calibration_shadow_candidate"
    assert shadow["weight_set_id"] == record["weight_set_id"]
    assert shadow["shadow_id"] == record["shadow_id"]
    assert record["forward_tracking_link"].endswith(f"#{record['shadow_id']}")
    validate_weight_forward_enrollment_record(record)


def test_forward_enrollment_preserves_safety_fields(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)

    enrollment = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
    )

    record = enrollment["enrollments"][0]
    for payload in (enrollment, record, record["shadow_record"]):
        assert payload["observe_only"] is True
        assert payload["candidate_only"] is True
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["manual_review_required"] is True


def test_forward_enrollment_is_idempotent_by_weight_set_id(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    enrollment_path = tmp_path / "forward_enrollments.json"

    first = enroll_candidate_weights_forward(registry, enrollment_path=enrollment_path, top=1)
    second = enroll_candidate_weights_forward(registry, enrollment_path=enrollment_path, top=1)

    assert first["enrollment_count"] == 1
    assert second["enrollment_count"] == 1
    assert load_weight_forward_enrollments(enrollment_path)["enrollment_count"] == 1


def test_weight_calibration_enroll_forward_cli(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=2)
    registry_path = tmp_path / "candidate_weight_registry.json"
    enrollment_path = tmp_path / "forward_enrollments.json"
    weight_set_id = registry["weight_sets"][0]["weight_set_id"]

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "enroll-forward",
            "--weight-set",
            weight_set_id,
            "--registry-path",
            str(registry_path),
            "--enrollment-path",
            str(enrollment_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF weight calibration forward enrollment" in result.output
    assert "enrollment_count=1" in result.output
    assert "shared_shadow_registry_mutated=false" in result.output
    assert "production_weights_mutated=false" in result.output
    assert "production_effect=none" in result.output
    assert load_weight_forward_enrollments(enrollment_path)["enrollment_count"] == 1


def test_backtest_forward_evidence_links_records(tmp_path: Path) -> None:
    run = _small_search_run()
    registry = _candidate_weight_registry(tmp_path, top=1)
    enrollments = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
        enrolled_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    dashboard = _forward_dashboard_payload(registry, enrollments, return_delta=0.0)

    payload = build_backtest_forward_evidence_aggregation(
        as_of=date(2026, 6, 2),
        candidate_registry=registry,
        forward_enrollments=enrollments,
        search_payload=run.payload,
        forward_dashboard=dashboard,
        source_paths={
            "historical_search": "summary.json",
            "candidate_registry": "candidate_weight_registry.json",
            "forward_enrollment": "forward_enrollments.json",
            "forward_dashboard": "forward_dashboard.json",
        },
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    record = payload["evidence_records"][0]
    assert payload["schema_version"] == "etf_weight_backtest_forward_evidence_v1"
    assert record["weight_set_id"] == registry["weight_sets"][0]["weight_set_id"]
    assert record["source_search_run_id"] == run.run_id
    assert record["shadow_id"] == enrollments["enrollments"][0]["shadow_id"]
    assert record["source_links"]["forward_row_found"] is True
    assert record["production_weights_mutated"] is False


def test_backtest_forward_evidence_computes_expectation_gap(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    enrollments = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
    )
    dashboard = _forward_dashboard_payload(registry, enrollments, return_delta=0.05)

    payload = build_backtest_forward_evidence_aggregation(
        as_of=date(2026, 6, 2),
        candidate_registry=registry,
        forward_enrollments=enrollments,
        forward_dashboard=dashboard,
    )

    record = payload["evidence_records"][0]
    assert record["expectation_gap"] == pytest.approx(0.05)
    assert record["evidence_status"] == "forward_better_than_backtest"
    validate_backtest_forward_evidence_record(record)


def test_backtest_forward_evidence_insufficient_forward_data(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    enrollments = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
    )

    payload = build_backtest_forward_evidence_aggregation(
        as_of=date(2026, 6, 2),
        candidate_registry=registry,
        forward_enrollments=enrollments,
        forward_dashboard={},
    )

    assert payload["status"] == "needs_more_forward_data"
    record = payload["evidence_records"][0]
    assert record["evidence_status"] == "needs_more_forward_data"
    assert record["expectation_gap"] is None
    assert "forward_evidence_not_found" in record["status_reasons"]


def test_backtest_forward_evidence_detects_forward_worse(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    enrollments = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
    )
    dashboard = _forward_dashboard_payload(
        registry,
        enrollments,
        return_delta=-0.08,
        drawdown_delta=-0.05,
    )

    payload = build_backtest_forward_evidence_aggregation(
        as_of=date(2026, 6, 2),
        candidate_registry=registry,
        forward_enrollments=enrollments,
        forward_dashboard=dashboard,
    )

    record = payload["evidence_records"][0]
    assert record["evidence_status"] == "forward_worse_than_backtest"
    assert record["expectation_gap"] == pytest.approx(-0.08)
    assert record["drawdown_gap"] > 0.02


def test_backtest_forward_evidence_schema_output_is_stable(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    enrollments = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
    )
    payload = build_backtest_forward_evidence_aggregation(
        as_of=date(2026, 6, 2),
        candidate_registry=registry,
        forward_enrollments=enrollments,
        forward_dashboard=_forward_dashboard_payload(registry, enrollments),
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    paths = write_backtest_forward_evidence_aggregation(
        payload,
        output_dir=tmp_path / "evidence",
    )

    assert paths["json"].exists()
    assert paths["markdown"].exists()
    written = json.loads(paths["json"].read_text(encoding="utf-8"))
    assert tuple(written)[:6] == (
        "applied_weight_set",
        "as_of",
        "broker_action",
        "candidate_count",
        "candidate_only",
        "enrollment_count",
    )
    assert "ETF Weight Backtest vs Forward Evidence" in paths["markdown"].read_text(
        encoding="utf-8"
    )


def test_weight_calibration_aggregate_evidence_cli(tmp_path: Path) -> None:
    run = _small_search_run()
    report_root = tmp_path / "reports"
    data_root = tmp_path / "data"
    paths = write_weight_search_run(run, report_root=report_root, data_root=data_root)
    registry = register_candidate_weight_sets(
        run.payload,
        registry_path=tmp_path / "candidate_weight_registry.json",
        top=1,
    )
    enrollments = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
    )
    dashboard_path = tmp_path / "forward_dashboard.json"
    dashboard_path.write_text(
        json.dumps(_forward_dashboard_payload(registry, enrollments), indent=2) + "\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "aggregate-evidence",
            "--as-of",
            "2026-06-02",
            "--search-run-id",
            run.run_id,
            "--search-output-dir",
            str(paths["report_dir"].parent),
            "--candidate-registry-path",
            str(tmp_path / "candidate_weight_registry.json"),
            "--enrollment-path",
            str(tmp_path / "forward_enrollments.json"),
            "--forward-dashboard-path",
            str(dashboard_path),
            "--output-dir",
            str(tmp_path / "evidence"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF weight backtest-forward evidence" in result.output
    assert "evidence_record_count=1" in result.output
    assert "production_effect=none" in result.output
    assert list((tmp_path / "evidence").glob("backtest_forward_evidence_*.json"))


def test_overfit_diagnostics_flags_concentrated_performance(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    run = _small_search_run()
    concentrated = _search_payload_with_slice_values(
        run.payload,
        "excess_return_vs_baseline",
        [0.20, 0.01, 0.00],
    )
    balanced = _search_payload_with_slice_values(
        run.payload,
        "excess_return_vs_baseline",
        [0.05, 0.05, 0.05],
    )

    concentrated_payload = build_weight_overfit_diagnostics(
        candidate_registry=registry,
        search_payload=concentrated,
    )
    balanced_payload = build_weight_overfit_diagnostics(
        candidate_registry=registry,
        search_payload=balanced,
    )

    concentrated_score = _component_score(concentrated_payload, "performance_concentration")
    balanced_score = _component_score(balanced_payload, "performance_concentration")
    assert concentrated_score > balanced_score
    assert "PERFORMANCE_CONCENTRATED_IN_FEW_SLICES" in (
        concentrated_payload["candidate_diagnostics"][0]["reason_codes"]
    )


def test_overfit_diagnostics_flags_bad_walk_forward_consistency(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["robustness_summary"]["stability_score"] = 0.20
    registry["weight_sets"][0]["robustness_summary"]["weak_slice_count"] = 3

    payload = build_weight_overfit_diagnostics(candidate_registry=registry)

    diagnostic = payload["candidate_diagnostics"][0]
    assert diagnostic["component_diagnostics"]["regime_fragility"]["risk_score"] >= 0.75
    assert "REGIME_FRAGILITY" in diagnostic["reason_codes"]


def test_overfit_diagnostics_flags_high_turnover(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["metrics_summary"]["turnover_vs_baseline"] = 1.5

    payload = build_weight_overfit_diagnostics(candidate_registry=registry)

    diagnostic = payload["candidate_diagnostics"][0]
    assert diagnostic["component_diagnostics"]["turnover_instability"]["risk_score"] == 1.0
    assert "HIGH_TURNOVER_INSTABILITY" in diagnostic["reason_codes"]


def test_overfit_diagnostics_flags_extreme_weights(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["weights"] = {
        "SPY": 0.0,
        "QQQ": 1.0,
        "SMH": 0.0,
        "SOXX": 0.0,
        "CASH": 0.0,
    }

    payload = build_weight_overfit_diagnostics(candidate_registry=registry)

    diagnostic = payload["candidate_diagnostics"][0]
    assert diagnostic["component_diagnostics"]["weight_extremeness"]["risk_score"] == 1.0
    assert "EXTREME_WEIGHT_CONCENTRATION" in diagnostic["reason_codes"]


def test_overfit_diagnostics_flags_forward_divergence(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    evidence = _backtest_forward_evidence_payload(tmp_path, registry, return_delta=-0.08)

    payload = build_weight_overfit_diagnostics(
        candidate_registry=registry,
        evidence_payload=evidence,
    )

    diagnostic = payload["candidate_diagnostics"][0]
    assert diagnostic["component_diagnostics"]["forward_backtest_divergence"][
        "risk_score"
    ] >= 0.8
    assert "FORWARD_BACKTEST_DIVERGENCE" in diagnostic["reason_codes"]


def test_overfit_risk_band_mapping() -> None:
    assert weight_overfit_risk_band(0.10) == "low"
    assert weight_overfit_risk_band(0.25) == "medium"
    assert weight_overfit_risk_band(0.50) == "high"
    assert weight_overfit_risk_band(0.75) == "critical"


def test_weight_calibration_overfit_diagnostics_cli(tmp_path: Path) -> None:
    run = _small_search_run()
    report_root = tmp_path / "reports"
    data_root = tmp_path / "data"
    paths = write_weight_search_run(run, report_root=report_root, data_root=data_root)
    registry = register_candidate_weight_sets(
        run.payload,
        registry_path=tmp_path / "candidate_weight_registry.json",
        top=1,
    )
    evidence = _backtest_forward_evidence_payload(tmp_path, registry)
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "overfit-diagnostics",
            "--search-run-id",
            run.run_id,
            "--search-output-dir",
            str(paths["report_dir"].parent),
            "--candidate-registry-path",
            str(tmp_path / "candidate_weight_registry.json"),
            "--evidence-path",
            str(evidence_path),
            "--output-dir",
            str(tmp_path / "overfit"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF weight overfit diagnostics" in result.output
    assert "candidate_count=1" in result.output
    assert "production_effect=none" in result.output
    assert list((tmp_path / "overfit").glob("overfit_diagnostics_*.json"))


def test_candidate_weight_proposal_manual_review_when_evidence_is_strong(
    tmp_path: Path,
) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["metrics_summary"]["candidate_score"] = 0.90
    evidence = _backtest_forward_evidence_payload(tmp_path, registry, return_delta=0.05)
    overfit = _overfit_payload_with_band(registry, band="low", score=0.10)

    payload = build_candidate_weight_proposals(
        candidate_registry=registry,
        evidence_payload=evidence,
        overfit_payload=overfit,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    proposal = payload["proposals"][0]
    assert proposal["proposal_type"] == "propose_manual_baseline_review"
    assert proposal["manual_review_required"] is True
    assert proposal["application_allowed"] is False
    assert proposal["production_effect"] == "none"


def test_candidate_weight_proposal_defers_when_forward_data_insufficient(
    tmp_path: Path,
) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["metrics_summary"]["candidate_score"] = 0.90
    overfit = _overfit_payload_with_band(registry, band="low", score=0.10)

    payload = build_candidate_weight_proposals(
        candidate_registry=registry,
        evidence_payload={},
        overfit_payload=overfit,
    )

    proposal = payload["proposals"][0]
    assert proposal["proposal_type"] == "defer_until_more_forward_data"
    assert proposal["forward_evidence_status"] == "needs_more_forward_data"


def test_candidate_weight_proposal_rejects_high_overfit_risk(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["metrics_summary"]["candidate_score"] = 0.90
    evidence = _backtest_forward_evidence_payload(tmp_path, registry, return_delta=0.05)
    overfit = _overfit_payload_with_band(registry, band="high", score=0.70)

    payload = build_candidate_weight_proposals(
        candidate_registry=registry,
        evidence_payload=evidence,
        overfit_payload=overfit,
    )

    proposal = payload["proposals"][0]
    assert proposal["proposal_type"] == "reject_weight_set"
    assert any(item["reason_code"] == "HIGH_OVERFIT_RISK" for item in proposal["blocking_evidence"])


def test_candidate_weight_proposal_rejects_bad_forward_evidence(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["metrics_summary"]["candidate_score"] = 0.90
    evidence = _backtest_forward_evidence_payload(tmp_path, registry, return_delta=-0.08)
    overfit = _overfit_payload_with_band(registry, band="low", score=0.10)

    payload = build_candidate_weight_proposals(
        candidate_registry=registry,
        evidence_payload=evidence,
        overfit_payload=overfit,
    )

    proposal = payload["proposals"][0]
    assert proposal["proposal_type"] == "reject_weight_set"
    assert proposal["forward_evidence_status"] == "forward_worse_than_backtest"


def test_candidate_weight_proposal_rejects_unsafe_type(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    proposal = build_candidate_weight_proposals(candidate_registry=registry)["proposals"][0]
    proposal["proposal_type"] = "apply_weight_set"

    with pytest.raises(WeightCalibrationError, match="unsafe_proposal_type"):
        validate_candidate_weight_proposal(proposal)


def test_weight_calibration_generate_proposals_cli(tmp_path: Path) -> None:
    registry = _candidate_weight_registry(tmp_path, top=1)
    registry["weight_sets"][0]["metrics_summary"]["candidate_score"] = 0.90
    registry_path = tmp_path / "candidate_weight_registry.json"
    registry_path.write_text(json.dumps(registry, indent=2) + "\n", encoding="utf-8")
    evidence = _backtest_forward_evidence_payload(tmp_path, registry, return_delta=0.05)
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    overfit = _overfit_payload_with_band(registry, band="low", score=0.10)
    overfit_path = tmp_path / "overfit.json"
    overfit_path.write_text(json.dumps(overfit, indent=2) + "\n", encoding="utf-8")

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "generate-proposals",
            "--candidate-registry-path",
            str(registry_path),
            "--evidence-path",
            str(evidence_path),
            "--overfit-path",
            str(overfit_path),
            "--output-dir",
            str(tmp_path / "proposals"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF weight candidate proposals" in result.output
    assert "proposal_count=1" in result.output
    assert "production_effect=none" in result.output
    assert list((tmp_path / "proposals").glob("candidate_weight_proposals_*.json"))


def test_dual_track_calibration_report_includes_required_sections(tmp_path: Path) -> None:
    inputs = _dual_track_report_inputs(tmp_path, return_delta=0.05, overfit_band="low")

    payload = build_dual_track_weight_calibration_report(
        as_of=date(2026, 6, 2),
        candidate_registry=inputs["registry"],
        forward_enrollments=inputs["enrollments"],
        search_payload=inputs["search_payload"],
        evidence_payload=inputs["evidence"],
        overfit_payload=inputs["overfit"],
        proposals_payload=inputs["proposals"],
        source_paths={"historical_search": "reports/search/summary.json"},
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["report_type"] == "etf_weight_dual_track_calibration_report"
    assert payload["status"] == "manual_review_ready"
    assert payload["search_configuration"]["market_regime"] == "ai_after_chatgpt"
    assert payload["top_historical_candidates"]
    assert payload["walk_forward_regime_robustness"]["status"] == "available"
    assert payload["overfit_diagnostics"]["risk_counts"]["low"] == 1
    assert payload["forward_evidence_comparison"]["status"] == "forward_better_than_backtest"
    assert payload["candidate_registry_status"]["candidate_count"] == 1
    proposal_counts = payload["proposal_scorecard"]["proposal_type_counts"]
    assert proposal_counts["propose_manual_baseline_review"] == 1
    assert payload["manual_review_package"]["candidate_shortlist"][0]["weight_set_id"]
    assert payload["production_effect"] == "none"

    paths = write_dual_track_weight_calibration_report(payload, output_dir=tmp_path / "reports")
    markdown = paths["markdown"].read_text(encoding="utf-8")
    assert "## Safety Banner" in markdown
    assert "## Walk-Forward / Regime Robustness" in markdown
    assert "## Proposal Scorecard" in markdown
    assert "production_effect = none" in markdown


def test_dual_track_calibration_report_missing_forward_data_needs_more_data(
    tmp_path: Path,
) -> None:
    run = _small_search_run()
    registry = register_candidate_weight_sets(
        run.payload,
        registry_path=tmp_path / "candidate_weight_registry.json",
        top=1,
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    payload = build_dual_track_weight_calibration_report(
        as_of=date(2026, 6, 2),
        candidate_registry=registry,
        search_payload=run.payload,
    )

    assert payload["status"] == "needs_more_forward_data"
    assert payload["forward_evidence_comparison"]["status"] == "missing"
    assert payload["proposal_scorecard"]["status"] == "missing"
    assert payload["manual_review_package"]["application_allowed"] is False


def test_dual_track_calibration_report_rejects_unsafe_proposal_payload(
    tmp_path: Path,
) -> None:
    inputs = _dual_track_report_inputs(tmp_path, return_delta=0.05, overfit_band="low")
    payload = build_dual_track_weight_calibration_report(
        as_of=date(2026, 6, 2),
        candidate_registry=inputs["registry"],
        forward_enrollments=inputs["enrollments"],
        search_payload=inputs["search_payload"],
        evidence_payload=inputs["evidence"],
        overfit_payload=inputs["overfit"],
        proposals_payload=inputs["proposals"],
    )
    payload["proposal_scorecard"]["proposals"][0]["proposal_type"] = "apply_weight_set"

    with pytest.raises(WeightCalibrationError, match="unsafe_proposal_type"):
        validate_dual_track_weight_calibration_report(payload)


def test_weight_calibration_report_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    inputs = _dual_track_report_inputs(tmp_path, return_delta=0.05, overfit_band="low")
    run = inputs["run"]
    search_paths = write_weight_search_run(
        run,
        report_root=tmp_path / "search_reports",
        data_root=tmp_path / "search_data",
    )
    registry_path = tmp_path / "candidate_weight_registry.json"
    registry_path.write_text(json.dumps(inputs["registry"], indent=2) + "\n", encoding="utf-8")
    enrollment_path = tmp_path / "forward_enrollments.json"
    enrollment_path.write_text(
        json.dumps(inputs["enrollments"], indent=2) + "\n",
        encoding="utf-8",
    )
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(inputs["evidence"], indent=2) + "\n", encoding="utf-8")
    overfit_path = tmp_path / "overfit.json"
    overfit_path.write_text(json.dumps(inputs["overfit"], indent=2) + "\n", encoding="utf-8")
    proposals_path = tmp_path / "proposals.json"
    proposals_path.write_text(
        json.dumps(inputs["proposals"], indent=2) + "\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "report",
            "--as-of",
            "2026-06-02",
            "--search-run-id",
            run.run_id,
            "--search-output-dir",
            str(search_paths["report_dir"].parent),
            "--candidate-registry-path",
            str(registry_path),
            "--enrollment-path",
            str(enrollment_path),
            "--evidence-path",
            str(evidence_path),
            "--overfit-path",
            str(overfit_path),
            "--proposals-path",
            str(proposals_path),
            "--output-dir",
            str(tmp_path / "reports"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "ETF weight dual-track calibration report" in result.output
    assert "status=manual_review_ready" in result.output
    assert "production_effect=none" in result.output
    assert (tmp_path / "reports" / "dual_track_calibration_2026-06-02.json").exists()
    assert (tmp_path / "reports" / "dual_track_calibration_2026-06-02.md").exists()


def test_weight_calibration_validation_gate_passes_complete_workflow(tmp_path: Path) -> None:
    inputs = _dual_track_report_inputs(tmp_path, return_delta=0.05, overfit_band="low")
    report = build_dual_track_weight_calibration_report(
        as_of=date(2026, 6, 2),
        candidate_registry=inputs["registry"],
        forward_enrollments=inputs["enrollments"],
        search_payload=inputs["search_payload"],
        evidence_payload=inputs["evidence"],
        overfit_payload=inputs["overfit"],
        proposals_payload=inputs["proposals"],
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    payload = build_dual_track_weight_calibration_validation_report(
        proposals_payload=inputs["proposals"],
        report_payload=report,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "PASS"
    assert payload["failed_check_count"] == 0
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    check_ids = {check["check_id"] for check in payload["checks"]}
    assert {
        "weight_search_config_valid",
        "weight_search_bounded",
        "historical_search_engine_available",
        "walk_forward_regime_robustness_available",
        "candidate_weight_registry_available",
        "forward_enrollment_available",
        "backtest_forward_aggregator_available",
        "overfit_diagnostics_available",
        "proposal_generator_available",
        "report_generator_available",
        "reader_brief_integration_available",
        "unsafe_proposal_types_blocked",
        "proposals_evidence_linked",
        "proposal_only_behavior",
    }.issubset(check_ids)
    validate_dual_track_weight_calibration_validation_report(payload)

    paths = write_dual_track_weight_calibration_validation_report(
        payload,
        output_dir=tmp_path / "validation",
    )
    assert paths["json"].exists()
    markdown = paths["markdown"].read_text(encoding="utf-8")
    assert "ETF Weight Dual-Track Calibration Validation Gate" in markdown
    assert "production_effect=none" in markdown


def test_weight_calibration_validation_fails_unsafe_proposal_payload(
    tmp_path: Path,
) -> None:
    inputs = _dual_track_report_inputs(tmp_path, return_delta=0.05, overfit_band="low")
    report = build_dual_track_weight_calibration_report(
        as_of=date(2026, 6, 2),
        candidate_registry=inputs["registry"],
        forward_enrollments=inputs["enrollments"],
        search_payload=inputs["search_payload"],
        evidence_payload=inputs["evidence"],
        overfit_payload=inputs["overfit"],
        proposals_payload=inputs["proposals"],
    )
    proposals = deepcopy(inputs["proposals"])
    proposals["proposals"][0]["proposal_type"] = "apply_weight_set"

    payload = build_dual_track_weight_calibration_validation_report(
        proposals_payload=proposals,
        report_payload=report,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "FAIL"
    failed = {check["check_id"] for check in payload["checks"] if check["status"] == "FAIL"}
    assert "proposal_payload_schema_valid" in failed
    assert "unsafe_proposal_types_absent" in failed


def test_weight_calibration_validation_fails_unsafe_report_payload(
    tmp_path: Path,
) -> None:
    inputs = _dual_track_report_inputs(tmp_path, return_delta=0.05, overfit_band="low")
    report = build_dual_track_weight_calibration_report(
        as_of=date(2026, 6, 2),
        candidate_registry=inputs["registry"],
        forward_enrollments=inputs["enrollments"],
        search_payload=inputs["search_payload"],
        evidence_payload=inputs["evidence"],
        overfit_payload=inputs["overfit"],
        proposals_payload=inputs["proposals"],
    )
    report["production_effect"] = "apply_weights"

    payload = build_dual_track_weight_calibration_validation_report(
        proposals_payload=inputs["proposals"],
        report_payload=report,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "FAIL"
    failed = {check["check_id"] for check in payload["checks"] if check["status"] == "FAIL"}
    assert "report_payload_schema_valid" in failed
    assert "report_payload_production_effect_none" in failed


def test_weight_calibration_validation_fails_when_safety_missing(
    tmp_path: Path,
) -> None:
    inputs = _dual_track_report_inputs(tmp_path, return_delta=0.05, overfit_band="low")
    report = build_dual_track_weight_calibration_report(
        as_of=date(2026, 6, 2),
        candidate_registry=inputs["registry"],
        forward_enrollments=inputs["enrollments"],
        search_payload=inputs["search_payload"],
        evidence_payload=inputs["evidence"],
        overfit_payload=inputs["overfit"],
        proposals_payload=inputs["proposals"],
    )
    del report["manual_review_required"]

    payload = build_dual_track_weight_calibration_validation_report(
        proposals_payload=inputs["proposals"],
        report_payload=report,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "FAIL"
    failed = {check["check_id"] for check in payload["checks"] if check["status"] == "FAIL"}
    assert "report_payload_schema_valid" in failed
    assert "report_payload_manual_review_required_true" in failed


def test_weight_calibration_validation_fails_when_search_unbounded(
    tmp_path: Path,
) -> None:
    raw = _raw_registry()
    raw["weight_searches"]["etf_initial_weight_search_v1"]["grid_step"] = 0.005
    config_path = tmp_path / "unbounded_weight_search.yaml"
    config_path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")

    payload = build_dual_track_weight_calibration_validation_report(
        search_config_path=config_path,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )

    assert payload["status"] == "FAIL"
    bounded_check = next(
        check for check in payload["checks"] if check["check_id"] == "weight_search_bounded"
    )
    assert bounded_check["status"] == "FAIL"
    assert "grid_step_too_fine" in bounded_check["message"]


def test_weight_calibration_validate_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    result = CliRunner().invoke(
        etf_app,
        [
            "weight-calibration",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert "ETF weight dual-track calibration validation gate" in result.output
    assert "status=PASS" in result.output
    assert "failed_check_count=0" in result.output
    assert "production_effect=none" in result.output
    assert list((tmp_path / "validation").glob("weight_calibration_validation_*.json"))
    assert list((tmp_path / "validation").glob("weight_calibration_validation_*.md"))


def _raw_registry() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)


def _raw_presets() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_WEIGHT_CALIBRATION_PRESET_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)


def _write_presets_config(raw: dict[str, object], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    return path


def _search_inputs():
    config = load_etf_config_bundle()
    prices, metadata_issues = standardize_price_frame(
        _make_prices(days=360),
        assets=config.assets,
        source_name="fixture",
    )
    quality_report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=date.fromisoformat(str(prices["date"].max())),
        extra_issues=metadata_issues,
    )
    assert quality_report.passed
    return config, prices, quality_report


def _small_search_run():
    config, prices, quality_report = _search_inputs()
    registry = load_weight_search_registry(etf_config=config)
    return run_historical_weight_search(
        prices,
        etf_config=config,
        quality_report=quality_report,
        registry=registry,
        search_id="etf_initial_weight_search_v1",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        max_candidates=4,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )


def _candidate_weight_record(tmp_path: Path) -> dict[str, object]:
    registry = register_candidate_weight_sets(
        _small_search_run().payload,
        registry_path=tmp_path / "candidate_weight_registry.json",
        top=1,
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    return deepcopy(registry["weight_sets"][0])


def _candidate_weight_registry(tmp_path: Path, *, top: int) -> dict[str, object]:
    return register_candidate_weight_sets(
        _small_search_run().payload,
        registry_path=tmp_path / "candidate_weight_registry.json",
        top=top,
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
    )


def _forward_dashboard_payload(
    registry: dict[str, object],
    enrollments: dict[str, object],
    *,
    return_delta: float = 0.0,
    drawdown_delta: float = 0.0,
) -> dict[str, object]:
    candidate = registry["weight_sets"][0]
    enrollment = enrollments["enrollments"][0]
    metrics = candidate["metrics_summary"]
    robustness = candidate["robustness_summary"]
    expected_return = float(metrics["total_return"])
    expected_drawdown = float(metrics["max_drawdown"] or 0.0)
    expected_turnover = float(metrics["turnover_vs_baseline"] or 0.0)
    expected_stability = float(robustness["stability_score"])
    return {
        "schema_version": "etf_forward_dashboard_v1",
        "report_type": "etf_forward_dashboard",
        "status": "AVAILABLE",
        "as_of": "2026-06-02",
        "candidate_summary_table": [
            {
                "weight_set_id": candidate["weight_set_id"],
                "shadow_id": enrollment["shadow_id"],
                "candidate_id": candidate["source_candidate_id"],
                "days_since_enrollment": 25,
                "return_since_enrollment": expected_return + return_delta,
                "max_drawdown_since_enrollment": expected_drawdown + drawdown_delta,
                "turnover_since_enrollment": expected_turnover,
                "weight_stability_score": expected_stability,
                "metric_null_reasons": {},
            }
        ],
        "observe_only": True,
        "candidate_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _search_payload_with_slice_values(
    search_payload: dict[str, object],
    field: str,
    values: list[float],
) -> dict[str, object]:
    payload = deepcopy(search_payload)
    candidate_id = payload["candidate_weight_sets"][0]["candidate_id"]
    for candidate in payload["robustness_evaluation"]["candidate_evaluations"]:
        if candidate["candidate_id"] != candidate_id:
            continue
        candidate["slice_metrics"] = [
            {
                "slice_id": f"slice_{index}",
                "slice_type": "walk_forward_window",
                "status": "AVAILABLE",
                "return": value if field == "return" else value / 2.0,
                "excess_return_vs_baseline": (
                    value if field == "excess_return_vs_baseline" else value / 2.0
                ),
                "constraint_hit_rate": value if field == "constraint_hit_rate" else 0.0,
            }
            for index, value in enumerate(values, start=1)
        ]
        break
    return payload


def _component_score(payload: dict[str, object], component_id: str) -> float:
    diagnostics = payload["candidate_diagnostics"][0]["component_diagnostics"]
    return float(diagnostics[component_id]["risk_score"])


def _backtest_forward_evidence_payload(
    tmp_path: Path,
    registry: dict[str, object],
    *,
    return_delta: float = 0.0,
) -> dict[str, object]:
    enrollments = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
    )
    dashboard = _forward_dashboard_payload(
        registry,
        enrollments,
        return_delta=return_delta,
        drawdown_delta=-0.05 if return_delta < 0 else 0.0,
    )
    return build_backtest_forward_evidence_aggregation(
        as_of=date(2026, 6, 2),
        candidate_registry=registry,
        forward_enrollments=enrollments,
        forward_dashboard=dashboard,
    )


def _overfit_payload_with_band(
    registry: dict[str, object],
    *,
    band: str,
    score: float,
) -> dict[str, object]:
    payload = build_weight_overfit_diagnostics(candidate_registry=registry)
    diagnostic = payload["candidate_diagnostics"][0]
    diagnostic["overfit_risk_band"] = band
    diagnostic["overfit_risk_score"] = score
    diagnostic["reason_codes"] = [f"TEST_{band.upper()}_RISK"]
    payload["risk_counts"] = {"low": 0, "medium": 0, "high": 0, "critical": 0}
    payload["risk_counts"][band] = 1
    payload["highest_risk_candidate"] = {
        "weight_set_id": diagnostic["weight_set_id"],
        "overfit_risk_score": score,
        "overfit_risk_band": band,
    }
    return payload


def _dual_track_report_inputs(
    tmp_path: Path,
    *,
    return_delta: float,
    overfit_band: str,
) -> dict[str, object]:
    run = _small_search_run()
    registry = register_candidate_weight_sets(
        run.payload,
        registry_path=tmp_path / "candidate_weight_registry.json",
        top=1,
        created_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    registry["weight_sets"][0]["metrics_summary"]["candidate_score"] = 0.90
    enrollments = enroll_candidate_weights_forward(
        registry,
        enrollment_path=tmp_path / "forward_enrollments.json",
        top=1,
    )
    dashboard = _forward_dashboard_payload(
        registry,
        enrollments,
        return_delta=return_delta,
    )
    evidence = build_backtest_forward_evidence_aggregation(
        as_of=date(2026, 6, 2),
        candidate_registry=registry,
        forward_enrollments=enrollments,
        search_payload=run.payload,
        forward_dashboard=dashboard,
    )
    overfit = _overfit_payload_with_band(
        registry,
        band=overfit_band,
        score=0.10 if overfit_band == "low" else 0.70,
    )
    proposals = build_candidate_weight_proposals(
        candidate_registry=registry,
        evidence_payload=evidence,
        overfit_payload=overfit,
        generated_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    return {
        "run": run,
        "search_payload": run.payload,
        "registry": registry,
        "enrollments": enrollments,
        "evidence": evidence,
        "overfit": overfit,
        "proposals": proposals,
    }


def _make_prices(days: int) -> pd.DataFrame:
    dates = pd.bdate_range("2022-01-03", periods=days)
    rows = []
    for symbol_index, symbol in enumerate(["SPY", "QQQ", "SMH", "SOXX"]):
        for index, current_date in enumerate(dates):
            price = 100.0 + index * (80.0 / max(days - 1, 1)) + symbol_index
            rows.append(
                {
                    "date": current_date.date().isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "fixture",
                    "created_at": "2026-06-01T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)


def _slice_metric(
    slice_id: str,
    excess_return: float,
    drawdown_reduction: float,
) -> dict[str, object]:
    return {
        "slice_id": slice_id,
        "slice_type": "regime_slice",
        "status": "AVAILABLE",
        "excess_return_vs_baseline": excess_return,
        "drawdown_reduction_vs_baseline": drawdown_reduction,
    }
