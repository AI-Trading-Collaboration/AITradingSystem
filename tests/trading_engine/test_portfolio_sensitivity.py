from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    constraint_binding_diagnostics,
    load_portfolio_sensitivity_config,
    run_portfolio_sensitivity,
    score_dispersion_diagnostics,
    score_to_target_weight_diagnostics,
    target_to_actual_weight_diagnostics,
    turnover_cost_impact,
    validate_portfolio_sensitivity_payload,
)
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_default_portfolio_sensitivity_config_loads() -> None:
    config = load_portfolio_sensitivity_config()

    assert config["production_effect"] == "none"
    assert config["manual_review_required"] is True
    assert config["auto_promotion"] is False
    assert set(config["profiles"]) >= {
        "baseline_v0_1",
        "more_sensitive_scores",
        "lower_rebalance_threshold",
        "relaxed_position_caps",
        "nonlinear_score_mapping",
    }
    assert config["ranking"]["weights"]["score_to_weight_effectiveness"] > 0


def test_portfolio_sensitivity_runs_profiles_and_writes_recommended(
    tmp_path: Path,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=80, min_history_days=20)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    config_path = _write_portfolio_sensitivity_config(tmp_path, fixture["config_path"])
    baseline_before = fixture["baseline_path"].read_text(encoding="utf-8")

    run = run_portfolio_sensitivity(
        as_of=fixture["as_of"],
        profile_names=(
            "baseline_v0_1",
            "more_sensitive_scores",
            "lower_rebalance_threshold",
        ),
        config_path=config_path,
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    assert run.recommended_profile_path.exists()
    assert validate_portfolio_sensitivity_payload(run.payload) == []
    assert run.payload["metadata"]["status"] == "LIMITED"
    assert run.payload["metadata"]["backtest_mode"] == "full_signal_backtest_limited"
    assert run.payload["metadata"]["production_effect"] == "none"
    assert run.payload["metadata"]["manual_review_required"] is True
    assert run.payload["metadata"]["auto_promotion"] is False
    assert run.payload["promotion_impact"]["can_support_candidate_promotion"] is False
    assert run.payload["safety"]["production_parameters_modified"] is False
    assert run.payload["safety"]["candidate_promotion_triggered"] is False
    assert len(run.payload["profiles"]) == 3
    assert run.payload["ranking"]["best_profile"] in {
        "baseline_v0_1",
        "more_sensitive_scores",
        "lower_rebalance_threshold",
    }
    assert run.payload["diagnosis"]["primary_bottleneck"]
    assert run.payload["fallback_signal_dilution"]["fallback_or_proxy_signal_weight"] >= 0.0

    for profile in run.payload["profiles"]:
        assert profile["status"] == "LIMITED"
        assert profile["score_dispersion"]["p50"] >= 0.0
        assert profile["score_to_target_weight"]["score_weight_correlation"] >= -1.0
        assert profile["target_to_actual_weight"]["rebalance_opportunity_days"] >= 0
        assert profile["constraint_binding"]["single_asset_cap_binding_days"] >= 0
        assert profile["turnover_cost_impact"]["turnover"] >= 0.0
        assert profile["performance"]["turnover"] >= 0.0
        assert "actual_rebalance_effectiveness" in profile["ranking_metrics"]

    recommended = yaml.safe_load(run.recommended_profile_path.read_text(encoding="utf-8"))
    assert recommended["production_effect"] == "none"
    assert recommended["manual_review_required"] is True
    assert recommended["auto_promotion"] is False
    assert recommended["profile_name"] == run.payload["ranking"]["best_profile"]
    assert fixture["baseline_path"].read_text(encoding="utf-8") == baseline_before


def test_portfolio_sensitivity_diagnostic_helpers_detect_bottlenecks(
    tmp_path: Path,
) -> None:
    config = load_portfolio_sensitivity_config(
        _write_portfolio_sensitivity_config(tmp_path, tmp_path)
    )
    diagnostics = config["diagnostics"]
    index = pd.date_range("2026-01-01", periods=5)
    compressed_score = pd.DataFrame(
        {"QQQ": [0.50, 0.51, 0.50, 0.49, 0.50], "NVDA": [0.50] * 5},
        index=index,
    )
    target = pd.DataFrame(
        {"QQQ": [0.20, 0.20, 0.21, 0.21, 0.22], "NVDA": [0.15, 0.15, 0.15, 0.16, 0.16]},
        index=index,
    )
    actual = pd.DataFrame(
        {"QQQ": [0.20, 0.20, 0.20, 0.20, 0.22], "NVDA": [0.15, 0.15, 0.15, 0.15, 0.16]},
        index=index,
    )

    dispersion = score_dispersion_diagnostics(compressed_score, diagnostics)
    score_to_target = score_to_target_weight_diagnostics(compressed_score, target, diagnostics)
    target_to_actual = target_to_actual_weight_diagnostics(
        actual,
        rebalance_days=1,
        rebalance_opportunity_days=5,
        diagnostics_config=diagnostics,
    )
    binding = constraint_binding_diagnostics(
        {
            "single_asset_cap_binding_days": 4,
            "sector_cap_binding_days": 0,
            "cash_floor_binding_days": 0,
            "most_constrained_assets": ["NVDA"],
        },
        total_days=5,
        diagnostics_config=diagnostics,
    )
    turnover = turnover_cost_impact(
        SimpleNamespace(metrics={"turnover": 0.50}, transaction_cost_drag=0.003),
        SimpleNamespace(metrics={"turnover": 0.10}, transaction_cost_drag=0.001),
        diagnostics,
    )

    assert dispersion["neutral_score_ratio_0_45_to_0_55"] == 1.0
    assert "compressed" in dispersion["warning"]
    assert score_to_target["mean_abs_target_weight_delta"] >= 0.0
    assert target_to_actual["rebalance_suppression_ratio"] == 0.8
    assert "Rebalance threshold" in target_to_actual["warning"]
    assert binding["most_constrained_assets"] == ["NVDA"]
    assert "constraints frequently bind" in binding["warning"]
    assert turnover["turnover_delta_vs_baseline"] == 0.4
    assert "increases turnover" in turnover["warning"]


def test_portfolio_sensitivity_shadow_backtest_reference_does_not_promote(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=80, min_history_days=20)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    run_backtest_input_diagnostics(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )
    config_path = _write_portfolio_sensitivity_config(tmp_path, fixture["config_path"])
    sensitivity_run = run_portfolio_sensitivity(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1",),
        config_path=config_path,
    )
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)

    shadow_run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = shadow_run.payload["promotion_decision"]
    assert decision["status"] in {"rejected", "watch"}
    assert "portfolio sensitivity summary" in decision["reason"].lower()
    assert decision["supporting_artifacts"]["portfolio_sensitivity"] == str(
        sensitivity_run.json_path
    )
    assert shadow_run.payload["promotion_constraints"]["allow_candidate"] is False
    assert shadow_run.payload["candidate_parameters"]["promotion_eligible"] is False
    assert shadow_run.payload["safety"]["auto_promotion"] is False


def _write_portfolio_sensitivity_config(
    tmp_path: Path,
    shadow_config_path: object,
) -> Path:
    config_path = tmp_path / "config" / "portfolio" / "portfolio_sensitivity_profiles.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "portfolio-sensitivity-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test portfolio sensitivity diagnostics",
                "intended_effect": "test score-to-weight transmission diagnostics",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "shadow_backtest_config_path": str(shadow_config_path),
                "output": {
                    "portfolio_sensitivity_dir": str(
                        tmp_path / "artifacts" / "portfolio_sensitivity"
                    ),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                },
                "diagnostics": {
                    "neutral_band": {"lower": 0.45, "upper": 0.55},
                    "neutral_compression_warning_ratio": 0.50,
                    "min_score_std": 0.03,
                    "min_score_to_target_delta": 0.005,
                    "weight_change_epsilon": 0.000001,
                    "rebalance_suppression_warning_ratio": 0.70,
                    "constraint_binding_warning_ratio": 0.25,
                    "fallback_weight_warning_ratio": 0.30,
                    "turnover_warning_delta_vs_baseline": 0.20,
                    "max_turnover_delta_for_recommendation": 0.35,
                },
                "ranking": {
                    "policy_version": "test-ranking",
                    "rationale": "test ranking",
                    "weights": {
                        "score_to_weight_effectiveness": 20.0,
                        "actual_rebalance_effectiveness": 20.0,
                        "improvement_in_sharpe": 10.0,
                        "improvement_in_max_drawdown": 10.0,
                        "turnover_penalty": -8.0,
                        "constraint_binding_penalty": -8.0,
                        "stability_across_walk_forward_windows": 12.0,
                    },
                },
                "profiles": {
                    "baseline_v0_1": {
                        "description": "Current portfolio construction settings.",
                        "score_sensitivity_multiplier": 1.0,
                        "rebalance_threshold": 0.05,
                        "max_single_asset_weight": 0.30,
                        "max_sector_weight": 0.60,
                        "min_cash_weight": 0.05,
                        "score_to_weight_method": "linear",
                    },
                    "more_sensitive_scores": {
                        "description": "Increase score sensitivity.",
                        "score_sensitivity_multiplier": 1.5,
                        "rebalance_threshold": 0.05,
                        "max_single_asset_weight": 0.30,
                        "max_sector_weight": 0.60,
                        "min_cash_weight": 0.05,
                        "score_to_weight_method": "linear",
                    },
                    "lower_rebalance_threshold": {
                        "description": "Allow smaller score changes to affect actual weights.",
                        "score_sensitivity_multiplier": 1.0,
                        "rebalance_threshold": 0.025,
                        "max_single_asset_weight": 0.30,
                        "max_sector_weight": 0.60,
                        "min_cash_weight": 0.05,
                        "score_to_weight_method": "linear",
                    },
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path
