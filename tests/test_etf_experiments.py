from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio.data import standardize_price_frame, validate_price_data
from ai_trading_system.etf_portfolio.experiments import (
    EXPERIMENT_COMPARISON_SCHEMA_KEYS,
    ETFExperimentPackRegistry,
    ETFExperimentRegistry,
    build_experiment_comparison_report,
    build_experiment_config_bundle,
    load_experiment_pack_registry,
    load_experiment_registry,
    run_experiment_batch,
    validate_experiment_pack_registry,
    write_experiment_comparison_report,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle


def test_etf_experiment_registry_loads_default_config() -> None:
    registry = load_experiment_registry()

    assert registry.policy_metadata.version == "etf_experiment_registry_v0_1"
    assert len(registry.experiments) == 16
    assert "base_balanced_growth" in registry.experiments
    assert registry.config_hash


def test_etf_experiment_ids_are_unique_and_match_keys() -> None:
    registry = load_experiment_registry()
    ids = [experiment.experiment_id for experiment in registry.experiments.values()]

    assert len(ids) == len(set(ids))
    for key, experiment in registry.experiments.items():
        assert experiment.experiment_id == key


def test_etf_experiment_base_weights_sum_to_one() -> None:
    registry = load_experiment_registry()

    for experiment in registry.experiments.values():
        weights = experiment.overrides.get("base_weights")
        if weights is None:
            continue
        assert round(sum(float(value) for value in weights.values()), 10) == 1.0


def test_etf_experiment_safety_fields_are_required() -> None:
    registry = load_experiment_registry()

    for experiment in registry.experiments.values():
        assert experiment.observe_only is True
        assert experiment.production_effect == "none"
        assert experiment.broker_action == "none"
        assert experiment.manual_review_required is True


def test_etf_experiment_registry_rejects_unknown_base_config_ref() -> None:
    raw = _registry_raw()
    raw["experiments"]["base_balanced_growth"]["base_config_ref"] = "missing"

    with pytest.raises(ValueError, match="unknown base_config_ref"):
        ETFExperimentRegistry.model_validate(raw)


def test_etf_experiment_registry_rejects_invalid_override_key() -> None:
    raw = _registry_raw()
    raw["experiments"]["base_balanced_growth"]["overrides"]["unknown_weight_knob"] = 0.1

    with pytest.raises(ValueError, match="unsupported keys"):
        ETFExperimentRegistry.model_validate(raw)


def test_etf_experiment_registry_rejects_unsafe_production_effect() -> None:
    raw = _registry_raw()
    raw["experiments"]["base_balanced_growth"]["production_effect"] = "target_weights"

    with pytest.raises(ValueError, match="production_effect=none"):
        ETFExperimentRegistry.model_validate(raw)


def test_etf_experiment_registry_rejects_bad_base_weight_sum() -> None:
    raw = _registry_raw()
    raw["experiments"]["base_balanced_growth"]["overrides"]["base_weights"]["CASH"] = 0.20

    with pytest.raises(ValueError, match="base_weights must sum"):
        ETFExperimentRegistry.model_validate(raw)


def test_etf_experiment_pack_loads_default_pack() -> None:
    pack_registry = load_experiment_pack_registry()
    pack = pack_registry.experiment_packs["etf_calibration_v1"]

    assert pack.pack_id == "etf_calibration_v1"
    assert pack.created_for_task == "TRADING-064"
    assert len(pack.experiment_ids) == 16
    assert pack.ranking_policy == "risk_adjusted_v1"
    assert pack.promotion_policy == "shadow_only_manual_review"
    assert pack_registry.config_hash


def test_etf_experiment_pack_references_existing_experiments() -> None:
    registry = load_experiment_registry()
    pack_registry = load_experiment_pack_registry(experiment_registry=registry)
    pack = pack_registry.experiment_packs["etf_calibration_v1"]

    assert set(pack.experiment_ids).issubset(set(registry.experiments))


def test_etf_experiment_pack_rejects_duplicate_experiments() -> None:
    raw = _pack_raw()
    ids = raw["experiment_packs"]["etf_calibration_v1"]["experiment_ids"]
    ids.append(ids[0])

    with pytest.raises(ValueError, match="duplicate experiments"):
        ETFExperimentPackRegistry.model_validate(raw)


def test_etf_experiment_pack_rejects_unknown_experiment_reference() -> None:
    raw = _pack_raw()
    raw["experiment_packs"]["etf_calibration_v1"]["experiment_ids"].append("missing")
    pack_registry = ETFExperimentPackRegistry.model_validate(raw)

    with pytest.raises(ValueError, match="unknown experiment_id"):
        validate_experiment_pack_registry(
            pack_registry,
            experiment_registry=load_experiment_registry(),
        )


def test_etf_experiment_pack_rejects_unsafe_experiment() -> None:
    registry = load_experiment_registry()
    registry.experiments["regime_mild"].production_effect = "target_weights"
    pack_registry = ETFExperimentPackRegistry.model_validate(_pack_raw())

    with pytest.raises(ValueError, match="unsafe experiment"):
        validate_experiment_pack_registry(pack_registry, experiment_registry=registry)


def test_etf_experiment_pack_rejects_missing_ranking_policy() -> None:
    raw = _pack_raw()
    raw["experiment_packs"]["etf_calibration_v1"]["ranking_policy"] = "missing"

    with pytest.raises(ValueError, match="unknown ranking_policy"):
        ETFExperimentPackRegistry.model_validate(raw)


def test_etf_experiment_pack_rejects_missing_promotion_policy() -> None:
    raw = _pack_raw()
    raw["experiment_packs"]["etf_calibration_v1"]["promotion_policy"] = "missing"

    with pytest.raises(ValueError, match="unknown promotion_policy"):
        ETFExperimentPackRegistry.model_validate(raw)


def test_etf_experiment_overrides_build_config_without_mutating_base() -> None:
    base_config = load_etf_config_bundle()
    registry = load_experiment_registry()
    experiment = registry.experiments["base_ai_growth"]

    experiment_config = build_experiment_config_bundle(base_config, experiment)

    assert experiment_config.config_hash != base_config.config_hash
    assert experiment_config.assets.assets["SPY"].default_weight == 0.15
    assert base_config.assets.assets["SPY"].default_weight != 0.15


def test_etf_experiment_relative_strength_override_rebalances_score_weights() -> None:
    base_config = load_etf_config_bundle()
    experiment = load_experiment_registry().experiments["rs_weight_30"]

    experiment_config = build_experiment_config_bundle(base_config, experiment)
    weights = {key: value.weight for key, value in experiment_config.strategy.scores.items()}

    assert weights["relative_strength"] == 0.30
    assert round(sum(weights.values()), 10) == 1.0


def test_etf_single_experiment_batch_writes_required_outputs(tmp_path: Path) -> None:
    base_config, prices, quality_report = _batch_inputs()
    registry = load_experiment_registry()

    batch = run_experiment_batch(
        prices,
        base_config=base_config,
        quality_report=quality_report,
        experiment_registry=registry,
        experiment_id="rebalance_05",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        output_root=tmp_path,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert batch.manifest["experiment_ids"] == ["rebalance_05"]
    assert batch.manifest["metric_schema_version"] == "etf_experiment_metrics_v1"
    assert batch.manifest["production_effect"] == "none"
    assert batch.manifest["broker_action"] == "none"
    assert batch.diagnostics_summary["status"] == "PASS"
    for name in (
        "run_manifest.json",
        "experiment_results.json",
        "benchmark_results.json",
        "metrics_summary.json",
        "diagnostics_summary.json",
    ):
        assert (batch.run_dir / name).exists()


def test_etf_pack_experiment_batch_runs_all_pack_members(tmp_path: Path) -> None:
    base_config, prices, quality_report = _batch_inputs()
    registry = load_experiment_registry()
    pack_registry = load_experiment_pack_registry(experiment_registry=registry)

    batch = run_experiment_batch(
        prices,
        base_config=base_config,
        quality_report=quality_report,
        experiment_registry=registry,
        pack_registry=pack_registry,
        pack_id="etf_calibration_v1",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        output_root=tmp_path,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    results = batch.experiment_results["results"]
    assert len(results) == 16
    assert {row["status"] for row in results} == {"PASS"}
    assert set(batch.benchmark_results["benchmarks"]) == set(batch.manifest["experiment_ids"])


def test_etf_batch_runner_isolates_failed_experiment(tmp_path: Path) -> None:
    base_config, prices, quality_report = _batch_inputs()
    registry = load_experiment_registry()
    registry.experiments["regime_mild"].overrides = {"base_weights": {"MISSING": 1.0}}
    pack_registry = _mini_pack_registry(["regime_mild", "rebalance_05"])

    batch = run_experiment_batch(
        prices,
        base_config=base_config,
        quality_report=quality_report,
        experiment_registry=registry,
        pack_registry=pack_registry,
        pack_id="mini_pack",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        output_root=tmp_path,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    statuses = {row["experiment_id"]: row["status"] for row in batch.experiment_results["results"]}
    assert batch.diagnostics_summary["status"] == "PARTIAL_FAIL"
    assert statuses["regime_mild"] == "FAILED"
    assert statuses["rebalance_05"] == "PASS"


def test_etf_batch_runner_blocks_unsafe_experiment(tmp_path: Path) -> None:
    base_config, prices, quality_report = _batch_inputs()
    registry = load_experiment_registry()
    registry.experiments["regime_mild"].production_effect = "target_weights"
    pack_registry = _mini_pack_registry(["regime_mild"])

    with pytest.raises(ValueError, match="unsafe experiment"):
        run_experiment_batch(
            prices,
            base_config=base_config,
            quality_report=quality_report,
            experiment_registry=registry,
            pack_registry=pack_registry,
            pack_id="mini_pack",
            start=date(2022, 12, 1),
            end=date(2022, 12, 20),
            output_root=tmp_path,
            generated_at=datetime(2026, 6, 1, tzinfo=UTC),
        )


def test_etf_experiment_batch_cli_smoke(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "experiment_runs"
    _make_prices(days=360).to_csv(prices_path, index=False)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "etf",
            "experiments",
            "run",
            "--experiment",
            "rebalance_05",
            "--start",
            "2022-12-01",
            "--end",
            "2022-12-20",
            "--prices-path",
            str(prices_path),
            "--output-dir",
            str(output_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF experiment batch 完成" in result.output
    assert "production_effect=none" in result.output
    assert list(output_dir.glob("*/run_manifest.json"))


def test_etf_experiment_comparison_loads_run_output_and_includes_context(tmp_path: Path) -> None:
    batch = _single_batch(tmp_path)

    report = build_experiment_comparison_report(batch.run_dir)

    assert tuple(report) == EXPERIMENT_COMPARISON_SCHEMA_KEYS
    assert report["run_metadata"]["run_id"] == batch.run_id
    assert report["experiment_list"][0]["experiment_id"] == "rebalance_05"
    assert report["baseline_comparison"]["status"] == "MISSING_BASELINE_METRICS"
    assert "B002" in report["benchmark_comparison"]["benchmark_ids"]
    assert report["metrics_table"][0]["candidate_status"] == "needs_ranking_policy"
    assert report["top_candidates_by_ranking_policy"] == []
    assert report["ranking_policy_status"] == "PENDING_TRADING_064E_RISK_ADJUSTED_V1"
    assert report["production_effect"] == "none"


def test_etf_experiment_comparison_writes_json_and_markdown(tmp_path: Path) -> None:
    batch = _single_batch(tmp_path)
    report = build_experiment_comparison_report(batch.run_dir)
    json_path = batch.run_dir / "comparison_report.json"
    md_path = batch.run_dir / "comparison_report.md"

    write_experiment_comparison_report(report, json_path=json_path, markdown_path=md_path)

    assert json_path.exists()
    assert md_path.exists()
    assert "ETF Experiment Comparison Report" in md_path.read_text(encoding="utf-8")
    assert "does not rank candidates by return only" in md_path.read_text(encoding="utf-8")


def test_etf_experiment_comparison_keeps_missing_metrics_null_with_reason(
    tmp_path: Path,
) -> None:
    base_config, prices, quality_report = _batch_inputs()
    registry = load_experiment_registry()
    registry.experiments["regime_mild"].overrides = {"base_weights": {"MISSING": 1.0}}
    pack_registry = _mini_pack_registry(["regime_mild", "rebalance_05"])
    batch = run_experiment_batch(
        prices,
        base_config=base_config,
        quality_report=quality_report,
        experiment_registry=registry,
        pack_registry=pack_registry,
        pack_id="mini_pack",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        output_root=tmp_path,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    report = build_experiment_comparison_report(batch.run_dir)
    failed_row = next(
        row for row in report["metrics_table"] if row["experiment_id"] == "regime_mild"
    )

    assert failed_row["total_return"] is None
    assert "total_return" in failed_row["metric_null_reasons"]
    assert "FAILED_EXPERIMENT:regime_mild" in report["warning_summary"]


def test_etf_experiment_compare_cli_latest_smoke(tmp_path: Path) -> None:
    prices_path = tmp_path / "prices.csv"
    output_dir = tmp_path / "experiment_runs"
    _make_prices(days=360).to_csv(prices_path, index=False)
    runner = CliRunner()
    run_result = runner.invoke(
        app,
        [
            "etf",
            "experiments",
            "run",
            "--experiment",
            "rebalance_05",
            "--start",
            "2022-12-01",
            "--end",
            "2022-12-20",
            "--prices-path",
            str(prices_path),
            "--output-dir",
            str(output_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    compare_result = runner.invoke(
        app,
        [
            "etf",
            "experiments",
            "compare",
            "--latest",
            "--output-dir",
            str(output_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert run_result.exit_code == 0, run_result.output
    assert compare_result.exit_code == 0, compare_result.output
    assert "ETF experiment comparison report" in compare_result.output
    assert list(output_dir.glob("*/comparison_report.json"))


def _registry_raw() -> dict[str, object]:
    return deepcopy(load_experiment_registry().model_dump(mode="json"))


def _pack_raw() -> dict[str, object]:
    return deepcopy(load_experiment_pack_registry().model_dump(mode="json"))


def _mini_pack_registry(experiment_ids: list[str]) -> ETFExperimentPackRegistry:
    raw = _pack_raw()
    pack = raw["experiment_packs"]["etf_calibration_v1"]
    pack["pack_id"] = "mini_pack"
    pack["experiment_ids"] = experiment_ids
    raw["experiment_packs"] = {"mini_pack": pack}
    return ETFExperimentPackRegistry.model_validate(raw)


def _batch_inputs():
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


def _single_batch(tmp_path: Path):
    base_config, prices, quality_report = _batch_inputs()
    return run_experiment_batch(
        prices,
        base_config=base_config,
        quality_report=quality_report,
        experiment_registry=load_experiment_registry(),
        experiment_id="rebalance_05",
        start=date(2022, 12, 1),
        end=date(2022, 12, 20),
        output_root=tmp_path,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )


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
