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
    ETFWeightSearchRegistry,
    WeightCalibrationError,
    generate_weight_candidates,
    load_candidate_weight_registry,
    load_weight_search_definition,
    load_weight_search_registry,
    register_candidate_weight_sets,
    run_historical_weight_search,
    summarize_weight_robustness,
    update_candidate_weight_status,
    validate_candidate_weight_record,
    validate_weight_search_registry,
    write_weight_search_run,
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


def _raw_registry() -> dict[str, object]:
    raw = safe_load_yaml_path(DEFAULT_ETF_WEIGHT_SEARCH_CONFIG_PATH)
    assert isinstance(raw, dict)
    return deepcopy(raw)


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
