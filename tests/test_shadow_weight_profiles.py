from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.prediction_ledger import load_prediction_ledger
from ai_trading_system.shadow_weight_profiles import (
    build_shadow_parameter_promotion_report,
    build_shadow_parameter_search_report,
    build_shadow_weight_performance_report,
    build_shadow_weight_profile_run_report,
    load_shadow_parameter_promotion_contract,
    load_shadow_position_gate_profile_manifest,
    load_shadow_weight_profile_manifest,
    render_shadow_parameter_search_report,
    write_shadow_parameter_promotion_report,
    write_shadow_parameter_search_bundle,
    write_shadow_weight_performance_csv,
)


def test_default_shadow_weight_profile_manifest_is_isolated() -> None:
    manifest, source_profile, source_path = load_shadow_weight_profile_manifest()

    assert manifest.production_effect == "none"
    assert source_path.name == "weight_profile_current.yaml"
    assert len(manifest.profiles) >= 3
    for profile in manifest.profiles:
        assert profile.production_effect == "none"
        assert profile.status == "shadow"
        assert set(profile.target_weights) == set(source_profile.base_weights)
        assert sum(profile.target_weights.values()) == pytest.approx(1.0)


def test_default_shadow_position_gate_profile_manifest_is_isolated() -> None:
    manifest = load_shadow_position_gate_profile_manifest()

    assert manifest.production_effect == "none"
    assert len(manifest.profiles) >= 3
    for profile in manifest.profiles:
        assert profile.production_effect == "none"
        assert profile.status == "shadow"
        assert profile.gate_cap_overrides
        assert "score_model" not in profile.gate_cap_overrides
        assert all(0.0 <= value <= 1.0 for value in profile.gate_cap_overrides.values())


def test_shadow_weight_profile_report_compares_against_production(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_snapshot(tmp_path)
    manifest_path = _write_manifest(tmp_path)

    report = build_shadow_weight_profile_run_report(
        as_of=date(2026, 5, 14),
        decision_snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        generated_at=datetime.fromisoformat("2026-05-14T12:00:00+00:00"),
    )

    assert report.status == "PASS"
    assert report.production_effect == "none"
    assert len(report.observations) == 1
    observation = report.observations[0]
    assert observation.profile_id == "shadow_test_alpha"
    assert observation.production_score == 70.0
    assert observation.shadow_score != observation.production_score
    assert observation.shadow_final_band["max_position"] == 0.4


def test_shadow_weight_profile_report_applies_shadow_gate_profile(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_snapshot(tmp_path)
    manifest_path = _write_manifest(tmp_path)
    gate_manifest_path = _write_gate_manifest(tmp_path)

    report = build_shadow_weight_profile_run_report(
        as_of=date(2026, 5, 14),
        decision_snapshot_path=snapshot_path,
        manifest_path=manifest_path,
        gate_manifest_path=gate_manifest_path,
        generated_at=datetime.fromisoformat("2026-05-14T12:00:00+00:00"),
    )

    observation = report.observations[0]

    assert report.status == "PASS"
    assert report.gate_manifest_path == gate_manifest_path
    assert observation.profile_id == "shadow_test_alpha__shadow_gate_test_relaxed"
    assert observation.gate_profile_id == "shadow_gate_test_relaxed"
    assert observation.gate_cap_max_position == pytest.approx(0.6)
    assert observation.shadow_final_band["max_position"] == 0.6
    assert observation.gate_cap_sources == ("valuation:40%->60%",)


def test_shadow_weight_profiles_cli_writes_observations_and_optional_predictions(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_snapshot(tmp_path)
    trace_path = tmp_path / "trace.json"
    features_path = tmp_path / "features.csv"
    quality_path = tmp_path / "quality.md"
    manifest_path = _write_manifest(tmp_path)
    gate_manifest_path = _write_gate_manifest(tmp_path)
    observation_path = tmp_path / "shadow_observations.csv"
    prediction_ledger_path = tmp_path / "shadow_prediction_ledger.csv"
    report_path = tmp_path / "shadow_weight_profiles.md"
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    snapshot["trace"] = {"trace_bundle_path": str(trace_path)}
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    trace_path.write_text(
        json.dumps(
            {
                "run_manifest": {"run_id": "run:test:2026-05-14"},
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

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "run-shadow-weight-profiles",
            "--manifest-path",
            str(manifest_path),
            "--gate-manifest-path",
            str(gate_manifest_path),
            "--decision-snapshot-path",
            str(snapshot_path),
            "--observation-ledger-path",
            str(observation_path),
            "--prediction-ledger-path",
            str(prediction_ledger_path),
            "--as-of",
            "2026-05-14",
            "--report-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "Shadow weight profile 状态：PASS" in result.output
    observations = pd.read_csv(observation_path)
    assert observations.loc[0, "profile_id"] == (
        "shadow_test_alpha__shadow_gate_test_relaxed"
    )
    assert observations.loc[0, "gate_profile_id"] == "shadow_gate_test_relaxed"
    rows = load_prediction_ledger(prediction_ledger_path)
    assert len(rows) == 1
    assert rows[0]["candidate_id"].startswith(
        "shadow_weight_profile:shadow_test_alpha__shadow_gate_test_relaxed"
    )
    assert rows[0]["production_effect"] == "none"
    assert (
        rows[0]["execution_assumption"]
        == "shadow_weight_profile_no_order_no_position_change"
    )
    assert report_path.exists()


def test_shadow_weight_performance_report_compares_position_weighted_returns(
    tmp_path: Path,
) -> None:
    observation_path = tmp_path / "shadow_observations.csv"
    prices_path = tmp_path / "prices.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-05-01",
                "profile_id": "shadow_test_alpha",
                "profile_version": "v1",
                "production_gated_target_position": "0.4",
                "shadow_gated_target_position": "0.6",
            },
            {
                "as_of": "2026-05-04",
                "profile_id": "shadow_test_alpha",
                "profile_version": "v1",
                "production_gated_target_position": "0.4",
                "shadow_gated_target_position": "0.6",
            },
        ]
    ).to_csv(observation_path, index=False)
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
            {"date": "2026-05-05", "ticker": "SMH", "adj_close": 121.0},
        ]
    ).to_csv(prices_path, index=False)

    report = build_shadow_weight_performance_report(
        as_of=date(2026, 5, 5),
        observation_ledger_path=observation_path,
        prices_path=prices_path,
        horizon_days=1,
        cost_bps=5.0,
    )
    csv_path = write_shadow_weight_performance_csv(
        report,
        tmp_path / "shadow_performance.csv",
    )
    summary = report.summaries[0]

    assert csv_path.exists()
    assert report.status == "PASS"
    assert report.best_profile is not None
    assert report.best_profile.profile_id == "shadow_test_alpha"
    assert summary.available_count == 2
    assert summary.shadow_total_return > summary.production_total_return
    assert summary.excess_total_return > 0
    assert summary.shadow_turnover == pytest.approx(0.6)
    assert summary.production_turnover == pytest.approx(0.4)


def test_shadow_weight_performance_cli_writes_report(tmp_path: Path) -> None:
    observation_path = tmp_path / "shadow_observations.csv"
    prices_path = tmp_path / "prices.csv"
    report_path = tmp_path / "shadow_performance.md"
    csv_path = tmp_path / "shadow_performance.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-05-01",
                "profile_id": "shadow_test_alpha",
                "profile_version": "v1",
                "production_gated_target_position": "0.4",
                "shadow_gated_target_position": "0.6",
            }
        ]
    ).to_csv(observation_path, index=False)
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
        ]
    ).to_csv(prices_path, index=False)

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "evaluate-shadow-weight-performance",
            "--observation-ledger-path",
            str(observation_path),
            "--prices-path",
            str(prices_path),
            "--as-of",
            "2026-05-04",
            "--horizon-days",
            "1",
            "--output-path",
            str(report_path),
            "--csv-output-path",
            str(csv_path),
        ],
    )

    assert result.exit_code == 0
    assert "Shadow weight performance 状态：PASS" in result.output
    assert "Return-leading profile：shadow_test_alpha" in result.output
    assert report_path.exists()
    assert csv_path.exists()


def test_shadow_parameter_search_writes_trial_registry_and_best_profile(
    tmp_path: Path,
) -> None:
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-01.json",
        signal_date="2026-05-01",
    )
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-04.json",
        signal_date="2026-05-04",
    )
    prices_path = tmp_path / "prices.csv"
    search_space_path = _write_search_space(tmp_path)
    objective_path = _write_objective(tmp_path)
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
            {"date": "2026-05-05", "ticker": "SMH", "adj_close": 121.0},
        ]
    ).to_csv(prices_path, index=False)

    report = build_shadow_parameter_search_report(
        run_id="test_search",
        start=date(2026, 5, 1),
        end=date(2026, 5, 5),
        decision_snapshot_path=snapshot_dir,
        prices_path=prices_path,
        search_space_path=search_space_path,
        objective_path=objective_path,
        output_dir=tmp_path / "search_output",
        horizon_days=1,
        cost_bps=5.0,
    )
    paths = write_shadow_parameter_search_bundle(report)

    assert report.status == "PASS"
    assert report.best_trial is not None
    assert report.best_trial.available_count == 2
    assert report.best_trial.excess_total_return is not None
    assert report.best_trial.excess_total_return > 0
    assert report.factorial_attribution is not None
    assert report.factorial_attribution.primary_driver == "gate"
    assert report.cap_attribution
    assert report.cap_attribution[0].gate_id == "valuation"
    assert report.position_change_rows
    assert report.position_change_rows[0].candidate_position > (
        report.position_change_rows[0].production_position
    )
    assert report.source_weight_profile_checksum
    assert report.prices_checksum
    assert report.decision_snapshot_checksum
    markdown = render_shadow_parameter_search_report(report)
    assert "## Factorial Attribution" in markdown
    assert "## Cap-Level Attribution" in markdown
    assert "## Position Change Attribution" in markdown
    assert "weight_only" in markdown
    assert "gate_only" in markdown
    assert paths["trials_csv"].exists()
    assert paths["pareto_front_csv"].exists()
    assert paths["best_profiles_yaml"].exists()
    assert paths["manifest_json"].exists()
    assert paths["search_report"].exists()


def test_shadow_parameter_search_gate_grid_fits_numeric_caps(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-01.json",
        signal_date="2026-05-01",
    )
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-04.json",
        signal_date="2026-05-04",
    )
    prices_path = tmp_path / "prices.csv"
    search_space_path = _write_search_space(
        tmp_path,
        include_shadow_gate_profiles=False,
        gate_grid_values={"valuation": [0.40, 0.55, 0.60]},
    )
    objective_path = _write_objective(tmp_path)
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
            {"date": "2026-05-05", "ticker": "SMH", "adj_close": 121.0},
        ]
    ).to_csv(prices_path, index=False)

    report = build_shadow_parameter_search_report(
        run_id="test_gate_grid",
        start=date(2026, 5, 1),
        end=date(2026, 5, 5),
        decision_snapshot_path=snapshot_dir,
        prices_path=prices_path,
        search_space_path=search_space_path,
        objective_path=objective_path,
        output_dir=tmp_path / "search_output",
        horizon_days=1,
        cost_bps=0.0,
    )

    assert report.gate_candidate_count == 4
    assert report.best_trial is not None
    assert report.best_trial.gate_candidate_id.startswith("grid_gate_")
    assert report.best_trial.gate_cap_overrides["valuation"] == 0.60


def test_shadow_parameter_objective_can_block_large_weight_distance(
    tmp_path: Path,
) -> None:
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-01.json",
        signal_date="2026-05-01",
    )
    prices_path = tmp_path / "prices.csv"
    search_space_path = _write_search_space(tmp_path)
    objective_path = _write_objective(
        tmp_path,
        max_l1_distance_from_production=0.10,
        max_single_factor_step=0.025,
    )
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
        ]
    ).to_csv(prices_path, index=False)

    report = build_shadow_parameter_search_report(
        run_id="test_regularized_objective",
        start=date(2026, 5, 1),
        end=date(2026, 5, 4),
        decision_snapshot_path=snapshot_dir,
        prices_path=prices_path,
        search_space_path=search_space_path,
        objective_path=objective_path,
        output_dir=tmp_path / "search_output",
        horizon_days=1,
        cost_bps=0.0,
    )

    far_weight_trials = [
        trial for trial in report.trials if trial.weight_candidate_id == "grid_weight_0001"
    ]
    assert far_weight_trials
    assert all(not trial.eligible for trial in far_weight_trials)
    assert {
        trial.ineligibility_reason for trial in far_weight_trials
    } == {"weight_l1_distance_above_objective_limit"}


def test_shadow_parameter_search_strict_objective_keeps_diagnostic_only(
    tmp_path: Path,
) -> None:
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-01.json",
        signal_date="2026-05-01",
    )
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-04.json",
        signal_date="2026-05-04",
    )
    prices_path = tmp_path / "prices.csv"
    search_space_path = _write_search_space(tmp_path)
    objective_path = _write_objective(
        tmp_path,
        min_available_samples=3,
        require_positive_excess=True,
    )
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
            {"date": "2026-05-05", "ticker": "SMH", "adj_close": 121.0},
        ]
    ).to_csv(prices_path, index=False)

    report = build_shadow_parameter_search_report(
        run_id="test_strict_objective",
        start=date(2026, 5, 1),
        end=date(2026, 5, 5),
        decision_snapshot_path=snapshot_dir,
        prices_path=prices_path,
        search_space_path=search_space_path,
        objective_path=objective_path,
        output_dir=tmp_path / "search_output",
        horizon_days=1,
        cost_bps=0.0,
    )
    paths = write_shadow_parameter_search_bundle(report)

    assert report.status == "PASS_WITH_LIMITATIONS"
    assert report.best_trial is None
    assert report.best_diagnostic_trial is not None
    assert report.factorial_attribution is not None
    assert not report.factorial_attribution.selected_trial_eligible
    markdown = render_shadow_parameter_search_report(report)
    assert "诊断领先 trial" in markdown
    assert "diagnostic_only_not_eligible" in markdown
    best_payload = yaml.safe_load(paths["best_profiles_yaml"].read_text(encoding="utf-8"))
    assert best_payload["selected_profile"] is None
    assert best_payload["diagnostic_leading_trial"]["production_effect"] == "none"


def test_shadow_parameter_search_cli_writes_outputs(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-01.json",
        signal_date="2026-05-01",
    )
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-04.json",
        signal_date="2026-05-04",
    )
    prices_path = tmp_path / "prices.csv"
    search_space_path = _write_search_space(tmp_path)
    objective_path = _write_objective(tmp_path)
    output_root = tmp_path / "parameter_search"
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
            {"date": "2026-05-05", "ticker": "SMH", "adj_close": 121.0},
        ]
    ).to_csv(prices_path, index=False)

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "search-shadow-parameters",
            "--from",
            "2026-05-01",
            "--to",
            "2026-05-05",
            "--decision-snapshot-path",
            str(snapshot_dir),
            "--prices-path",
            str(prices_path),
            "--search-space-path",
            str(search_space_path),
            "--objective-path",
            str(objective_path),
            "--output-root",
            str(output_root),
            "--run-id",
            "test_cli_search",
        ],
    )

    assert result.exit_code == 0
    assert "Shadow parameter search 状态：PASS" in result.output
    assert (output_root / "test_cli_search" / "trials.csv").exists()
    assert (output_root / "test_cli_search" / "best_profiles.yaml").exists()


def test_shadow_parameter_promotion_contract_keeps_search_separate(
    tmp_path: Path,
) -> None:
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-01.json",
        signal_date="2026-05-01",
    )
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-04.json",
        signal_date="2026-05-04",
    )
    prices_path = tmp_path / "prices.csv"
    search_space_path = _write_search_space(tmp_path)
    objective_path = _write_objective(tmp_path)
    contract_path = _write_promotion_contract(tmp_path)
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
            {"date": "2026-05-05", "ticker": "SMH", "adj_close": 121.0},
        ]
    ).to_csv(prices_path, index=False)
    search_report = build_shadow_parameter_search_report(
        run_id="test_promotion",
        start=date(2026, 5, 1),
        end=date(2026, 5, 5),
        decision_snapshot_path=snapshot_dir,
        prices_path=prices_path,
        search_space_path=search_space_path,
        objective_path=objective_path,
        output_dir=tmp_path / "search_output",
        horizon_days=1,
        cost_bps=0.0,
    )
    paths = write_shadow_parameter_search_bundle(search_report)

    contract = load_shadow_parameter_promotion_contract(contract_path)
    promotion = build_shadow_parameter_promotion_report(
        search_output_dir=paths["output_dir"],
        contract_path=contract_path,
    )
    output_path = write_shadow_parameter_promotion_report(
        promotion,
        tmp_path / "promotion.md",
    )

    assert contract.production_effect == "none"
    assert promotion.status == "READY_FOR_FORWARD_SHADOW"
    assert any(
        check.check_id == "gate_driver_cap_review" and check.status == "MISSING"
        for check in promotion.checks
    )
    assert any(
        check.check_id == "forward_shadow" and check.status == "MISSING"
        for check in promotion.checks
    )
    assert output_path.exists()
    assert output_path.with_suffix(".json").exists()


def test_shadow_parameter_promotion_cli_writes_report(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    _write_snapshot(
        tmp_path,
        output_path=snapshot_dir / "decision_snapshot_2026-05-01.json",
        signal_date="2026-05-01",
    )
    prices_path = tmp_path / "prices.csv"
    search_space_path = _write_search_space(tmp_path)
    objective_path = _write_objective(tmp_path)
    contract_path = _write_promotion_contract(tmp_path)
    output_root = tmp_path / "parameter_search"
    pd.DataFrame(
        [
            {"date": "2026-05-01", "ticker": "SMH", "adj_close": 100.0},
            {"date": "2026-05-04", "ticker": "SMH", "adj_close": 110.0},
        ]
    ).to_csv(prices_path, index=False)
    search_result = CliRunner().invoke(
        app,
        [
            "feedback",
            "search-shadow-parameters",
            "--from",
            "2026-05-01",
            "--to",
            "2026-05-04",
            "--decision-snapshot-path",
            str(snapshot_dir),
            "--prices-path",
            str(prices_path),
            "--search-space-path",
            str(search_space_path),
            "--objective-path",
            str(objective_path),
            "--output-root",
            str(output_root),
            "--run-id",
            "test_cli_promotion_search",
        ],
    )
    assert search_result.exit_code == 0
    promotion_path = tmp_path / "promotion.md"

    result = CliRunner().invoke(
        app,
        [
            "feedback",
            "evaluate-shadow-parameter-promotion",
            "--search-output-dir",
            str(output_root / "test_cli_promotion_search"),
            "--contract-path",
            str(contract_path),
            "--output-path",
            str(promotion_path),
        ],
    )

    assert result.exit_code == 0
    assert "Shadow parameter promotion 状态：READY_FOR_FORWARD_SHADOW" in result.output
    assert promotion_path.exists()
    assert promotion_path.with_suffix(".json").exists()


def _write_manifest(tmp_path: Path) -> Path:
    manifest_path = tmp_path / "shadow_weight_profiles.yaml"
    manifest_path.write_text(
        """
version: shadow_weight_profiles_test
status: pilot
owner: system
production_effect: none
source_weight_profile_path: config/weights/weight_profile_current.yaml
label_horizon_days: 20
rationale: test manifest
review_after_reports: 3
profiles:
  - profile_id: shadow_test_alpha
    version: v1
    status: shadow
    owner: system
    production_effect: none
    rationale: test profile
    review_after_reports: 3
    target_weights:
      trend: 0.30
      fundamentals: 0.30
      macro_liquidity: 0.125
      risk_sentiment: 0.125
      valuation: 0.075
      policy_geopolitics: 0.075
""".lstrip(),
        encoding="utf-8",
    )
    return manifest_path


def _write_gate_manifest(tmp_path: Path) -> Path:
    manifest_path = tmp_path / "shadow_position_gate_profiles.yaml"
    manifest_path.write_text(
        """
version: shadow_gate_profiles_test
status: pilot
owner: system
production_effect: none
source_policy_paths:
  - config/scoring_rules.yaml
  - config/portfolio.yaml
rationale: test gate manifest
review_after_reports: 3
profiles:
  - profile_id: shadow_gate_test_relaxed
    version: v1
    status: shadow
    owner: system
    production_effect: none
    rationale: test relaxed gate profile
    review_after_reports: 3
    gate_cap_overrides:
      valuation: 0.60
""".lstrip(),
        encoding="utf-8",
    )
    return manifest_path


def _write_search_space(
    tmp_path: Path,
    *,
    include_shadow_gate_profiles: bool = True,
    gate_grid_values: dict[str, list[float]] | None = None,
) -> Path:
    gate_manifest_path = _write_gate_manifest(tmp_path)
    gate_grid_yaml = ""
    if gate_grid_values is not None:
        value_lines = "\n".join(
            f"    {gate_id}: {values}"
            for gate_id, values in gate_grid_values.items()
        )
        gate_grid_yaml = f"""
gate_grid:
  enabled: true
  max_candidates: 20
  cap_values:
{value_lines}
"""
    search_space_path = tmp_path / "shadow_parameter_search_space.yaml"
    search_space_path.write_text(
        f"""
version: search_space_test
status: pilot
owner: system
production_effect: none
source_weight_profile_path: config/weights/weight_profile_current.yaml
shadow_gate_profile_manifest_path: {gate_manifest_path.as_posix()}
include_source_weight_profile: true
include_shadow_weight_profiles: false
include_production_observed_gate_profile: true
include_shadow_gate_profiles: {str(include_shadow_gate_profiles).lower()}
rationale: test search space
review_after_reports: 3
weight_grid:
  enabled: true
  max_candidates: 10
  signal_values:
    trend: [0.30]
    fundamentals: [0.30]
    macro_liquidity: [0.125]
    risk_sentiment: [0.125]
    valuation: [0.075]
    policy_geopolitics: [0.075]
{gate_grid_yaml}
""".lstrip(),
        encoding="utf-8",
    )
    return search_space_path


def _write_objective(
    tmp_path: Path,
    *,
    min_available_samples: int = 1,
    require_positive_excess: bool = False,
    max_l1_distance_from_production: float | None = None,
    max_single_factor_step: float | None = None,
) -> Path:
    objective_path = tmp_path / "shadow_parameter_objective.yaml"
    optional_limits = ""
    if max_l1_distance_from_production is not None:
        optional_limits += (
            "max_l1_distance_from_production: "
            f"{max_l1_distance_from_production}\n"
        )
    if max_single_factor_step is not None:
        optional_limits += f"max_single_factor_step: {max_single_factor_step}\n"
    objective_path.write_text(
        f"""
version: objective_test
status: pilot
owner: system
production_effect: none
primary_metric: objective_score
rationale: test objective
excess_return_weight: 1.0
shadow_return_weight: 0.0
excess_drawdown_penalty: 0.0
excess_turnover_penalty: 0.0
missing_sample_penalty: 0.0
min_available_samples: {min_available_samples}
require_positive_excess: {str(require_positive_excess).lower()}
top_n: 5
{optional_limits}
""".lstrip(),
        encoding="utf-8",
    )
    return objective_path


def _write_promotion_contract(tmp_path: Path) -> Path:
    contract_path = tmp_path / "shadow_parameter_promotion_contract.yaml"
    contract_path.write_text(
        """
version: promotion_contract_test
status: validation
owner: system
production_effect: none
rationale: test promotion contract
min_available_samples: 1
require_search_eligible_best: true
require_positive_excess: true
max_drawdown_degradation: 1.0
max_shadow_turnover: 10.0
gate_primary_driver_requires_cap_review: true
required_forward_shadow_available_samples: 2
owner_approval_required: true
rollback_condition_required: true
approved_hard_allowed: false
""".lstrip(),
        encoding="utf-8",
    )
    return contract_path


def _write_snapshot(
    tmp_path: Path,
    *,
    output_path: Path | None = None,
    signal_date: str = "2026-05-14",
) -> Path:
    snapshot_path = output_path or tmp_path / f"decision_snapshot_{signal_date}.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "signal_date": signal_date,
                "generated_at": "2026-05-14T21:00:00+00:00",
                "market_regime": {"regime_id": "ai_after_chatgpt"},
                "scores": {
                    "overall_score": 70.0,
                    "components": [
                        {"component": "trend", "score": 90.0},
                        {"component": "fundamentals", "score": 80.0},
                        {"component": "macro_liquidity", "score": 40.0},
                        {"component": "risk_sentiment", "score": 50.0},
                        {"component": "valuation", "score": 35.0},
                        {"component": "policy_geopolitics", "score": 60.0},
                    ],
                },
                "positions": {
                    "model_risk_asset_ai_band": {
                        "label": "偏重仓",
                        "min_position": 0.6,
                        "max_position": 0.8,
                    },
                    "final_risk_asset_ai_band": {
                        "label": "偏重仓/仓位受限",
                        "min_position": 0.4,
                        "max_position": 0.4,
                    },
                    "position_gates": [
                        {
                            "gate_id": "score_model",
                            "label": "评分模型仓位",
                            "max_position": 0.8,
                        },
                        {
                            "gate_id": "valuation",
                            "label": "估值拥挤",
                            "max_position": 0.4,
                        },
                    ],
                },
                "rule_versions": {"rules": [{"rule_id": "scoring.weighted_score.v1"}]},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return snapshot_path
