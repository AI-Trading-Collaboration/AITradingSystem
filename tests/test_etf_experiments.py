from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio.data import standardize_price_frame, validate_price_data
from ai_trading_system.etf_portfolio.experiments import (
    DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    EXPERIMENT_CANDIDATE_SELECTION_SCHEMA_KEYS,
    EXPERIMENT_COMPARISON_SCHEMA_KEYS,
    EXPERIMENT_VALIDATION_SCHEMA_VERSION,
    ETFExperimentPackRegistry,
    ETFExperimentRegistry,
    apply_ranking_policy_to_comparison_report,
    build_candidate_selection_report,
    build_experiment_comparison_report,
    build_experiment_config_bundle,
    build_experiment_validation_report,
    build_weekly_experiment_review,
    enroll_shadow_candidates,
    load_experiment_pack_registry,
    load_experiment_registry,
    load_shadow_candidate_registry,
    rank_experiment_candidates,
    run_experiment_batch,
    validate_experiment_pack_registry,
    write_candidate_selection_report,
    write_experiment_comparison_report,
    write_experiment_validation_report,
    write_weekly_experiment_review_report,
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
    promotion_policy = pack_registry.promotion_policies["shadow_only_manual_review"]
    review_policy = pack_registry.review_policies["weekly_shadow_review_v1"]

    assert pack.pack_id == "etf_calibration_v1"
    assert pack.created_for_task == "TRADING-064"
    assert len(pack.experiment_ids) == 16
    assert pack.ranking_policy == "risk_adjusted_v1"
    assert pack.promotion_policy == "shadow_only_manual_review"
    assert promotion_policy.thresholds["min_candidate_score"] == 0.50
    assert promotion_policy.production_promotion_allowed is False
    assert promotion_policy.shadow_observation_allowed is True
    assert review_policy.thresholds["min_review_days"] == 5
    assert review_policy.production_promotion_allowed is False
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


def test_etf_experiment_ranking_computes_component_scores() -> None:
    ranked = rank_experiment_candidates(
        _ranking_report([_ranking_row("candidate_a")]),
        ranking_policy=_ranking_policy(),
    )

    row = ranked[0]
    assert row["candidate_score"] > 0
    assert "benchmark_excess_return_score" in row
    assert "drawdown_reduction_score" in row
    assert "risk_adjusted_return_score" in row
    assert "turnover_penalty_score" in row
    assert "stability_score" in row
    assert row["hard_rejection_flags"] == []


def test_etf_experiment_ranking_rewards_drawdown_reduction() -> None:
    stronger = _ranking_row("stronger", drawdown_reduction_vs_qqq=0.08)
    weaker = _ranking_row("weaker", drawdown_reduction_vs_qqq=0.01)

    ranked = rank_experiment_candidates(
        _ranking_report([weaker, stronger]),
        ranking_policy=_ranking_policy(),
    )

    assert ranked[0]["experiment_id"] == "stronger"


def test_etf_experiment_ranking_rejects_excessive_turnover_despite_return() -> None:
    row = _ranking_row("high_return_high_turnover", total_return=0.80, turnover=0.60)

    ranked = rank_experiment_candidates(
        _ranking_report([row]),
        ranking_policy=_ranking_policy(),
    )

    assert ranked[0]["candidate_score"] == 0
    assert ranked[0]["candidate_status"] == "rejected"
    assert "TURNOVER_TOO_HIGH" in ranked[0]["hard_rejection_flags"]


def test_etf_experiment_ranking_rejects_missing_benchmark_comparison() -> None:
    row = _ranking_row("missing_benchmark", excess_return_vs_qqq=None)

    ranked = rank_experiment_candidates(
        _ranking_report([row]),
        ranking_policy=_ranking_policy(),
    )

    assert "NO_BENCHMARK_COMPARISON" in ranked[0]["hard_rejection_flags"]


def test_etf_experiment_ranking_rejects_unsafe_production_effect() -> None:
    row = _ranking_row("unsafe")
    row["production_effect"] = "target_weights"

    ranked = rank_experiment_candidates(
        _ranking_report([row]),
        ranking_policy=_ranking_policy(),
    )

    assert "UNSAFE_PRODUCTION_EFFECT" in ranked[0]["hard_rejection_flags"]


def test_etf_experiment_ranking_is_deterministic() -> None:
    report = _ranking_report([_ranking_row("b"), _ranking_row("a")])
    policy = _ranking_policy()

    assert rank_experiment_candidates(report, ranking_policy=policy) == (
        rank_experiment_candidates(report, ranking_policy=policy)
    )


def test_etf_experiment_comparison_can_apply_ranking_policy() -> None:
    report = _ranking_report([_ranking_row("candidate_a")])

    ranked_report = apply_ranking_policy_to_comparison_report(
        report,
        ranking_policy=_ranking_policy(),
        ranking_policy_id="risk_adjusted_v1",
    )

    assert ranked_report["ranking_policy_status"] == "APPLIED:risk_adjusted_v1"
    assert ranked_report["top_candidates_by_ranking_policy"][0]["experiment_id"] == (
        "candidate_a"
    )


def test_etf_experiment_candidate_selection_marks_eligible_shadow_candidate() -> None:
    ranked_report = apply_ranking_policy_to_comparison_report(
        _ranking_report([_ranking_row("candidate_a")]),
        ranking_policy=_ranking_policy(),
        ranking_policy_id="risk_adjusted_v1",
    )

    selection = build_candidate_selection_report(
        ranked_report,
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )

    candidate = selection["candidates"][0]
    assert tuple(selection) == EXPERIMENT_CANDIDATE_SELECTION_SCHEMA_KEYS
    assert selection["selection_summary"]["status"] == "PASS"
    assert candidate["selection_status"] == "eligible_for_shadow"
    assert candidate["shadow_observation_allowed"] is True
    assert candidate["production_promotion_allowed"] is False
    assert selection["production_promotion_allowed"] is False


def test_etf_experiment_candidate_selection_needs_more_data_below_policy_score() -> None:
    selection = build_candidate_selection_report(
        _selection_report([_ranked_candidate("low_score", score=0.20)]),
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )

    candidate = selection["candidates"][0]
    assert selection["selection_summary"]["status"] == "NO_ELIGIBLE_CANDIDATE"
    assert candidate["selection_status"] == "needs_more_data"
    assert candidate["shadow_observation_allowed"] is False


def test_etf_experiment_candidate_selection_blocks_missing_benchmark() -> None:
    ranked_report = apply_ranking_policy_to_comparison_report(
        _ranking_report([_ranking_row("missing_benchmark", excess_return_vs_qqq=None)]),
        ranking_policy=_ranking_policy(),
        ranking_policy_id="risk_adjusted_v1",
    )

    selection = build_candidate_selection_report(
        ranked_report,
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )

    candidate = selection["candidates"][0]
    assert candidate["selection_status"] == "blocked"
    assert "NO_BENCHMARK_COMPARISON" in candidate["blockers"]
    assert candidate["shadow_observation_allowed"] is False


def test_etf_experiment_candidate_selection_rejects_high_turnover() -> None:
    ranked_report = apply_ranking_policy_to_comparison_report(
        _ranking_report([_ranking_row("high_turnover", turnover=0.60)]),
        ranking_policy=_ranking_policy(),
        ranking_policy_id="risk_adjusted_v1",
    )

    selection = build_candidate_selection_report(
        ranked_report,
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )

    candidate = selection["candidates"][0]
    assert candidate["selection_status"] == "rejected"
    assert "TURNOVER_TOO_HIGH" not in candidate["blockers"]
    assert candidate["shadow_observation_allowed"] is False


def test_etf_experiment_candidate_selection_blocks_without_ranking_policy() -> None:
    selection = build_candidate_selection_report(
        _ranking_report([_ranking_row("candidate_a")]),
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )

    assert selection["selection_summary"]["status"] == "BLOCKED_NO_RANKED_CANDIDATES"
    assert selection["selection_summary"]["blockers"] == ["RANKING_POLICY_NOT_APPLIED"]
    assert selection["candidates"] == []


def test_etf_experiment_candidate_selection_writes_json_and_markdown(tmp_path: Path) -> None:
    selection = build_candidate_selection_report(
        _selection_report([_ranked_candidate("candidate_a", score=0.70)]),
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )
    json_path = tmp_path / "candidate_selection_report.json"
    md_path = tmp_path / "candidate_selection_report.md"

    write_candidate_selection_report(selection, json_path=json_path, markdown_path=md_path)

    assert json_path.exists()
    assert md_path.exists()
    text = md_path.read_text(encoding="utf-8")
    assert "ETF Experiment Candidate Selection Gate" in text
    assert "eligible_for_shadow" in text
    assert "Production Promotion Allowed: false" in text


def test_etf_experiment_select_candidates_cli_smoke(tmp_path: Path) -> None:
    _single_batch(tmp_path)
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "etf",
            "experiments",
            "select-candidates",
            "--latest",
            "--output-dir",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF experiment candidate selection gate" in result.output
    assert "production_promotion_allowed=false" in result.output
    assert list(tmp_path.glob("*/candidate_selection_report.json"))


def test_etf_shadow_enrollment_creates_observe_only_record(tmp_path: Path) -> None:
    selection = build_candidate_selection_report(
        _selection_report([_ranked_candidate("candidate_a", score=0.70)]),
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )
    registry_path = tmp_path / "etf_shadow_candidates.json"

    registry = enroll_shadow_candidates(
        selection,
        registry_path=registry_path,
        enrolled_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    record = registry["candidates"][0]
    assert registry_path.exists()
    assert registry["candidate_count"] == 1
    assert record["candidate_id"] == "unit:candidate_a"
    assert record["experiment_id"] == "candidate_a"
    assert record["source_run_id"] == "unit"
    assert record["status"] == "active_shadow_observation"
    assert record["observe_only"] is True
    assert record["production_effect"] == "none"
    assert record["broker_action"] == "none"
    assert record["manual_review_required"] is True
    assert record["production_promotion_allowed"] is False
    assert record["evaluation_schedule"]["weekly_review_task"] == "TRADING-064H"


def test_etf_shadow_enrollment_duplicate_is_deterministic(tmp_path: Path) -> None:
    selection = build_candidate_selection_report(
        _selection_report([_ranked_candidate("candidate_a", score=0.70)]),
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )
    registry_path = tmp_path / "etf_shadow_candidates.json"

    enroll_shadow_candidates(
        selection,
        registry_path=registry_path,
        enrolled_at=datetime(2026, 6, 1, tzinfo=UTC),
    )
    first = registry_path.read_text(encoding="utf-8")
    enroll_shadow_candidates(
        selection,
        registry_path=registry_path,
        enrolled_at=datetime(2026, 6, 2, tzinfo=UTC),
    )
    second = registry_path.read_text(encoding="utf-8")

    assert first == second
    assert load_shadow_candidate_registry(registry_path)["candidate_count"] == 1


def test_etf_shadow_enrollment_rejects_unsafe_candidate(tmp_path: Path) -> None:
    selection = build_candidate_selection_report(
        _selection_report([_ranked_candidate("candidate_a", score=0.70)]),
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )
    selection["candidates"][0]["production_effect"] = "target_weights"

    with pytest.raises(ValueError, match="production_effect=none"):
        enroll_shadow_candidates(
            selection,
            registry_path=tmp_path / "etf_shadow_candidates.json",
            enrolled_at=datetime(2026, 6, 1, tzinfo=UTC),
        )


def test_etf_shadow_enrollment_blocks_noneligible_candidate(tmp_path: Path) -> None:
    selection = build_candidate_selection_report(
        _selection_report([_ranked_candidate("low_score", score=0.20)]),
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )

    with pytest.raises(ValueError, match="no eligible"):
        enroll_shadow_candidates(
            selection,
            registry_path=tmp_path / "etf_shadow_candidates.json",
            enrolled_at=datetime(2026, 6, 1, tzinfo=UTC),
        )


def test_etf_shadow_enrollment_runtime_path_is_ignored() -> None:
    ignored_path = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH
    ignore_text = (Path.cwd() / ".gitignore").read_text(encoding="utf-8")

    assert ignored_path.as_posix().endswith("data/simulation/etf_shadow_candidates.json")
    assert "data/simulation/" in ignore_text


def test_etf_shadow_enrollment_cli_smoke(tmp_path: Path) -> None:
    run_dir = _write_eligible_experiment_run(tmp_path)
    registry_path = tmp_path / "state" / "etf_shadow_candidates.json"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "etf",
            "experiments",
            "enroll-shadow",
            "--run-id",
            run_dir.name,
            "--output-dir",
            str(tmp_path),
            "--top",
            "1",
            "--registry-path",
            str(registry_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF shadow candidates registry" in result.output
    assert "production_effect=none" in result.output
    assert json.loads(registry_path.read_text(encoding="utf-8"))["candidate_count"] == 1


def test_etf_weekly_review_loads_shadow_candidates_and_metrics(tmp_path: Path) -> None:
    registry_path = _enrolled_shadow_registry(tmp_path)

    review = build_weekly_experiment_review(
        as_of=date(2022, 12, 20),
        shadow_registry_path=registry_path,
        run_root=tmp_path,
        review_policy=_review_policy(),
        review_policy_id="weekly_shadow_review_v1",
    )

    row = review["candidate_reviews"][0]
    assert review["review_period"]["active_candidate_count"] == 1
    assert row["candidate_forward_return"] == 0.20
    assert row["baseline_forward_return"] == 0.15
    assert row["benchmark_forward_returns"]["QQQ"] == 0.10
    assert row["drawdown_during_review_period"] == -0.08
    assert row["turnover_during_review_period"] == 0.10
    assert row["constraint_hits"]["constraint_hit_rate"] == 0.05
    assert row["recommended_action"] == "promote_to_longer_observation"
    assert review["production_promotion_allowed"] is False


def test_etf_weekly_review_marks_short_window_needs_more_data(tmp_path: Path) -> None:
    registry_path = _enrolled_shadow_registry(tmp_path)

    review = build_weekly_experiment_review(
        as_of=date(2022, 12, 2),
        shadow_registry_path=registry_path,
        run_root=tmp_path,
        review_policy=_review_policy(),
        review_policy_id="weekly_shadow_review_v1",
    )

    row = review["candidate_reviews"][0]
    assert row["review_days"] == 2
    assert row["recommended_action"] == "needs_more_data"
    assert "promote_to_production_effect" not in json.dumps(review)


def test_etf_weekly_review_writes_json_and_markdown(tmp_path: Path) -> None:
    registry_path = _enrolled_shadow_registry(tmp_path)
    review = build_weekly_experiment_review(
        as_of=date(2022, 12, 20),
        shadow_registry_path=registry_path,
        run_root=tmp_path,
        review_policy=_review_policy(),
        review_policy_id="weekly_shadow_review_v1",
    )
    json_path = tmp_path / "weekly_review.json"
    md_path = tmp_path / "weekly_review.md"

    write_weekly_experiment_review_report(review, json_path=json_path, markdown_path=md_path)

    assert json_path.exists()
    assert md_path.exists()
    text = md_path.read_text(encoding="utf-8")
    assert "ETF Experiment Weekly Review" in text
    assert "promote_to_longer_observation" in text
    assert "Production Promotion Allowed: false" in text


def test_etf_weekly_review_cli_smoke(tmp_path: Path) -> None:
    registry_path = _enrolled_shadow_registry(tmp_path)
    output_dir = tmp_path / "weekly_reviews"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "etf",
            "experiments",
            "weekly-review",
            "--as-of",
            "2022-12-20",
            "--registry-path",
            str(registry_path),
            "--run-root",
            str(tmp_path),
            "--output-dir",
            str(output_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF experiment weekly review" in result.output
    assert "production_promotion_allowed=false" in result.output
    assert (output_dir / "weekly_review_2022-12-20.json").exists()


def test_etf_experiment_validation_passes_with_valid_pack() -> None:
    report = build_experiment_validation_report(
        pack_id="etf_calibration_v1",
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert report["schema_version"] == EXPERIMENT_VALIDATION_SCHEMA_VERSION
    assert report["status"] == "PASS"
    assert report["safe_for_shadow_observation"] is True
    assert report["production_effect"] == "none"
    assert report["broker_action"] == "none"
    assert report["manual_review_required"] is True
    assert {check["status"] for check in report["checks"]} == {"PASS"}


def test_etf_experiment_validation_fails_with_unsafe_experiment() -> None:
    registry = load_experiment_registry()
    registry.experiments["regime_mild"].production_effect = "target_weights"
    report = build_experiment_validation_report(
        pack_id="etf_calibration_v1",
        experiment_registry=registry,
        pack_registry=load_experiment_pack_registry(),
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert report["status"] == "FAIL"
    assert any(
        "production_effect=none" in blocker or "unsafe experiment" in blocker
        for blocker in report["summary"]["blockers"]
    )


def test_etf_experiment_validation_fails_when_ranking_policy_missing() -> None:
    pack_registry = load_experiment_pack_registry()
    del pack_registry.ranking_policies["risk_adjusted_v1"]

    report = build_experiment_validation_report(
        pack_id="etf_calibration_v1",
        pack_registry=pack_registry,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert report["status"] == "FAIL"
    assert "RANKING_POLICY_MISSING:risk_adjusted_v1" in report["summary"]["blockers"]


def test_etf_experiment_validation_fails_when_candidate_gate_missing() -> None:
    pack_registry = load_experiment_pack_registry()
    del pack_registry.promotion_policies["shadow_only_manual_review"]

    report = build_experiment_validation_report(
        pack_id="etf_calibration_v1",
        pack_registry=pack_registry,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert report["status"] == "FAIL"
    assert "CANDIDATE_GATE_MISSING:shadow_only_manual_review" in (
        report["summary"]["blockers"]
    )


def test_etf_experiment_validation_fails_when_pack_production_effect_is_unsafe() -> None:
    pack_registry = load_experiment_pack_registry()
    pack_registry.experiment_packs["etf_calibration_v1"].production_effect = "target_weights"

    report = build_experiment_validation_report(
        pack_id="etf_calibration_v1",
        pack_registry=pack_registry,
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )

    assert report["status"] == "FAIL"
    assert any(
        blocker == "PACK_PRODUCTION_EFFECT_UNSAFE" or "production_effect=none" in blocker
        for blocker in report["summary"]["blockers"]
    )


def test_etf_experiment_validation_writes_json_and_markdown(tmp_path: Path) -> None:
    report = build_experiment_validation_report(
        pack_id="etf_calibration_v1",
        generated_at=datetime(2026, 6, 1, tzinfo=UTC),
    )
    json_path = tmp_path / "experiment_validation.json"
    md_path = tmp_path / "experiment_validation.md"

    write_experiment_validation_report(report, json_path=json_path, markdown_path=md_path)

    assert json_path.exists()
    assert md_path.exists()
    text = md_path.read_text(encoding="utf-8")
    assert "ETF Experiment Validation Gate" in text
    assert "Status: PASS" in text


def test_etf_experiment_validation_cli_smoke(tmp_path: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "etf",
            "experiments",
            "validate",
            "--pack",
            "etf_calibration_v1",
            "--output-dir",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "ETF experiment validation gate" in result.output
    assert "status=PASS" in result.output
    assert "production_effect=none" in result.output
    assert list(tmp_path.glob("*_experiment_validation.json"))


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


def _write_eligible_experiment_run(tmp_path: Path) -> Path:
    run_dir = tmp_path / "eligible_run"
    run_dir.mkdir(parents=True)
    documents = {
        "run_manifest.json": {
            "run_id": "eligible_run",
            "pack_id": "etf_calibration_v1",
            "start_date": "2022-12-01",
            "end_date": "2022-12-20",
            "data_quality_status": "PASS",
            "metric_schema_version": "etf_experiment_metrics_v1",
            "observe_only": True,
            "production_effect": "none",
            "broker_action": "none",
            "manual_review_required": True,
        },
        "experiment_results.json": {
            "results": [
                {
                    "experiment_id": "base_ai_growth",
                    "status": "PASS",
                    "experiment_version": "v0_1",
                    "family": "base_allocation",
                    "config_hash": "config_hash_base_ai_growth",
                    "model_version": "etf_model_base_ai_growth",
                    "first_signal_date": "2022-12-01",
                    "last_signal_date": "2022-12-20",
                    "observe_only": True,
                    "production_effect": "none",
                    "broker_action": "none",
                    "manual_review_required": True,
                }
            ]
        },
        "benchmark_results.json": {
            "benchmarks": {
                "base_ai_growth": {
                    "B002": {"total_return": 0.10, "max_drawdown": -0.12}
                }
            }
        },
        "metrics_summary.json": {
            "baseline_metrics": {"total_return": 0.15, "max_drawdown": -0.10},
            "metrics": [
                {
                    "experiment_id": "base_ai_growth",
                    "standardized_metrics": {
                        "total_return": 0.20,
                        "CAGR": 0.12,
                        "max_drawdown": -0.08,
                        "Sharpe": 1.2,
                        "Sortino": 1.4,
                        "Calmar": 1.5,
                        "turnover": 0.10,
                        "average_equity_exposure": 0.85,
                        "average_cash_weight": 0.15,
                    },
                    "allocation_stability_diagnostics": {
                        "constraint_hit_rate": 0.05,
                        "regime_transition_count": 3,
                    },
                }
            ],
        },
        "diagnostics_summary.json": {"status": "PASS", "failed_experiment_ids": []},
    }
    for filename, payload in documents.items():
        (run_dir / filename).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return run_dir


def _enrolled_shadow_registry(tmp_path: Path) -> Path:
    run_dir = _write_eligible_experiment_run(tmp_path)
    comparison = build_experiment_comparison_report(run_dir)
    ranked = apply_ranking_policy_to_comparison_report(
        comparison,
        ranking_policy=_ranking_policy(),
        ranking_policy_id="risk_adjusted_v1",
    )
    selection = build_candidate_selection_report(
        ranked,
        promotion_policy=_promotion_policy(),
        promotion_policy_id="shadow_only_manual_review",
    )
    registry_path = tmp_path / "etf_shadow_candidates.json"
    enroll_shadow_candidates(
        selection,
        registry_path=registry_path,
        enrolled_at=datetime(2026, 6, 1, tzinfo=UTC),
    )
    return registry_path


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


def _ranking_policy():
    return load_experiment_pack_registry().ranking_policies["risk_adjusted_v1"]


def _promotion_policy():
    return load_experiment_pack_registry().promotion_policies["shadow_only_manual_review"]


def _review_policy():
    return load_experiment_pack_registry().review_policies["weekly_shadow_review_v1"]


def _ranking_report(rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "etf_experiment_comparison",
        "run_metadata": {
            "run_id": "unit",
            "pack_id": "etf_calibration_v1",
            "start_date": "2022-12-01",
            "end_date": "2022-12-20",
        },
        "experiment_list": [{"experiment_id": row["experiment_id"]} for row in rows],
        "baseline_comparison": {"status": "AVAILABLE", "metrics": {}},
        "benchmark_comparison": {"status": "AVAILABLE", "benchmark_ids": ["B002"]},
        "metrics_table": rows,
        "risk_metrics_table": [],
        "turnover_stability_table": [],
        "constraint_hit_summary": {},
        "warning_summary": [],
        "top_candidates_by_ranking_policy": [],
        "ranking_policy_status": "PENDING_TRADING_064E_RISK_ADJUSTED_V1",
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _selection_report(candidates: list[dict[str, object]]) -> dict[str, object]:
    rows = [_ranking_row(str(candidate["experiment_id"])) for candidate in candidates]
    report = _ranking_report(rows)
    report["top_candidates_by_ranking_policy"] = candidates
    report["ranking_policy_status"] = "APPLIED:risk_adjusted_v1"
    return report


def _ranked_candidate(
    experiment_id: str,
    *,
    score: float,
    hard_rejection_flags: list[str] | None = None,
) -> dict[str, object]:
    hard_flags = hard_rejection_flags or []
    return {
        "candidate_id": f"unit:{experiment_id}",
        "experiment_id": experiment_id,
        "source_run_id": "unit",
        "model_version": "etf_model_unit",
        "config_hash": f"config_hash_{experiment_id}",
        "start_date": "2022-12-01",
        "candidate_score": score,
        "benchmark_excess_return_score": score,
        "drawdown_reduction_score": score,
        "risk_adjusted_return_score": score,
        "turnover_penalty_score": score,
        "stability_score": score,
        "hard_rejection_flags": hard_flags,
        "candidate_status": "rejected" if hard_flags else "ranked",
        "ranking_reason": [],
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }


def _ranking_row(
    experiment_id: str,
    *,
    total_return: float = 0.20,
    excess_return_vs_qqq: float | None = 0.05,
    drawdown_reduction_vs_qqq: float = 0.04,
    turnover: float = 0.10,
) -> dict[str, object]:
    return {
        "experiment_id": experiment_id,
        "total_return": total_return,
        "CAGR": 0.12,
        "max_drawdown": -0.08,
        "Sharpe": 1.2,
        "Sortino": 1.4,
        "Calmar": 1.5,
        "turnover": turnover,
        "average_equity_exposure": 0.85,
        "average_cash_weight": 0.15,
        "excess_return_vs_baseline": 0.03,
        "drawdown_reduction_vs_baseline": 0.02,
        "excess_return_vs_QQQ": excess_return_vs_qqq,
        "drawdown_reduction_vs_QQQ": drawdown_reduction_vs_qqq,
        "constraint_hit_rate": 0.05,
        "regime_transition_count": 3,
        "candidate_status": "needs_ranking_policy",
        "metric_null_reasons": {},
        "model_version": f"etf_model_{experiment_id}",
        "config_hash": f"config_hash_{experiment_id}",
        "first_signal_date": "2022-12-01",
        "last_signal_date": "2022-12-20",
        "observe_only": True,
        "production_effect": "none",
        "broker_action": "none",
        "manual_review_required": True,
    }
