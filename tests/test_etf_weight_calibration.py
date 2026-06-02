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
    load_weight_search_definition,
    load_weight_search_registry,
    run_historical_weight_search,
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
