from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from ai_trading_system.trading_engine import signal_calibration
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.signal_calibration import (
    load_signal_calibration_config,
    run_signal_calibration,
    signal_correlation_diagnostics,
    signal_distribution_diagnostics,
    validate_signal_calibration_payload,
)
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture
from trading_engine.test_signal_ablation import _write_signal_ablation_config


def test_signal_calibration_runs_profiles_and_writes_recommended(
    tmp_path: Path,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    config_path = _write_signal_calibration_config(tmp_path, fixture["config_path"])
    baseline_before = fixture["baseline_path"].read_text(encoding="utf-8")

    run = run_signal_calibration(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1", "trend_long_bias"),
        config_path=config_path,
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    assert run.recommended_profile_path.exists()
    assert validate_signal_calibration_payload(run.payload) == []
    assert run.payload["metadata"]["production_effect"] == "none"
    assert run.payload["metadata"]["manual_review_required"] is True
    assert run.payload["metadata"]["auto_promotion"] is False
    assert run.payload["metadata"]["backtest_mode"] == "full_signal_backtest_limited"
    assert len(run.payload["profiles"]) == 2
    assert run.payload["ranking"]["best_profile"] in {"baseline_v0_1", "trend_long_bias"}
    assert run.payload["promotion_impact"]["can_support_candidate_promotion"] is False
    assert run.payload["safety"]["production_parameters_modified"] is False

    for profile in run.payload["profiles"]:
        assert profile["metrics"]["turnover"] >= 0.0
        assert profile["signal_distribution"]["trend_momentum"]
        assert profile["signal_distribution"]["sector_strength"]
        assert "trend_momentum_vs_sector_strength" in profile["signal_correlation"]
        assert profile["ranking_metrics"]["walk_forward_stability_ratio"] >= 0.0
        assert Path(profile["artifacts"]["calibrated_signal_snapshot"]).exists()

    recommended = yaml.safe_load(run.recommended_profile_path.read_text(encoding="utf-8"))
    assert recommended["production_effect"] == "none"
    assert recommended["manual_review_required"] is True
    assert recommended["auto_promotion"] is False
    assert recommended["profile_name"] == run.payload["ranking"]["best_profile"]
    assert fixture["baseline_path"].read_text(encoding="utf-8") == baseline_before


def test_signal_calibration_distribution_and_correlation_warnings(tmp_path: Path) -> None:
    config = load_signal_calibration_config(_write_signal_calibration_config(tmp_path, tmp_path))
    diagnostics_config = config["diagnostics"]
    index = pd.date_range("2026-01-01", periods=5)
    neutral_frame = pd.DataFrame({"QQQ": [0.5] * 5, "NVDA": [0.51] * 5}, index=index)
    correlated_frame = pd.DataFrame({"QQQ": [0.1, 0.2, 0.3, 0.4, 0.5]}, index=index)

    distribution = signal_distribution_diagnostics(
        {
            "trend_momentum": neutral_frame,
            "sector_strength": correlated_frame,
        },
        diagnostics_config,
    )
    correlation = signal_correlation_diagnostics(
        {
            "trend_momentum": correlated_frame,
            "sector_strength": correlated_frame,
        },
        diagnostics_config,
    )

    assert distribution["trend_momentum"]["neutral_ratio_0_45_to_0_55"] == 1.0
    assert "over-compressed" in distribution["trend_momentum"]["warning"]
    assert correlation["trend_momentum_vs_sector_strength"] == 1.0
    assert "correlation" in correlation["warning"].lower()


def test_signal_calibration_shadow_backtest_reference_does_not_promote(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_calibration_config(tmp_path, fixture["config_path"])
    calibration_run = run_signal_calibration(
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
    assert "signal calibration summary" in decision["reason"].lower()
    assert decision["supporting_artifacts"]["signal_calibration"] == str(calibration_run.json_path)
    assert shadow_run.payload["candidate_parameters"]["promotion_eligible"] is False
    assert shadow_run.payload["safety"]["auto_promotion"] is False


def _write_signal_calibration_config(tmp_path: Path, shadow_config_path: object) -> Path:
    signal_ablation_config = _write_signal_ablation_config(tmp_path, Path(shadow_config_path))
    config_path = tmp_path / "config" / "signals" / "signal_calibration_profiles.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "signal-calibration-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test signal calibration",
                "intended_effect": "test profile diagnostics",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "shadow_backtest_config_path": str(shadow_config_path),
                "signal_ablation_config_path": str(signal_ablation_config),
                "output": {
                    "signal_calibration_dir": str(tmp_path / "artifacts" / "signal_calibration"),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                },
                "diagnostics": {
                    "neutral_band": {"lower": 0.45, "upper": 0.55},
                    "extreme_band": {"lower": 0.20, "upper": 0.80},
                    "neutral_compression_warning_ratio": 0.50,
                    "correlation_thresholds": {
                        "high_correlation": 0.75,
                        "very_high_correlation": 0.90,
                    },
                },
                "normalization_policy": {
                    "rolling_window": 60,
                    "min_periods": 5,
                    "zscore_clip": 3.0,
                    "zscore_scale": 1.0,
                    "minmax_epsilon": 0.000000001,
                },
                "ranking": {
                    "policy_version": "test-ranking",
                    "rationale": "test ranking",
                    "weights": {
                        "promotion_credit_signal_count": 100.0,
                        "ablation_positive_signal_count": 20.0,
                        "walk_forward_stability_ratio": 10.0,
                        "mean_abs_score_delta": 5.0,
                        "mean_abs_portfolio_weight_delta": 5.0,
                        "annualized_return": 1.0,
                        "max_drawdown_abs": -1.0,
                        "turnover": -1.0,
                        "signal_neutral_ratio": -5.0,
                        "signal_correlation_penalty": -5.0,
                    },
                },
                "profiles": {
                    "baseline_v0_1": {
                        "description": "Current test profile.",
                        "trend_momentum": {
                            "windows": [5, 10],
                            "ma_pairs": [[5, 10]],
                            "normalization": "minmax_rolling",
                            "clipping": {"lower": 0.05, "upper": 0.95},
                        },
                        "sector_strength": {
                            "benchmark": "QQQ",
                            "windows": [5, 10],
                            "normalization": "zscore_to_unit",
                        },
                    },
                    "trend_long_bias": {
                        "description": "Longer trend test profile.",
                        "trend_momentum": {
                            "windows": [10, 15],
                            "ma_pairs": [[5, 15]],
                            "normalization": "sigmoid_zscore",
                            "clipping": {"lower": 0.03, "upper": 0.97},
                        },
                        "sector_strength": {
                            "benchmark": "QQQ",
                            "windows": [10, 15],
                            "normalization": "sigmoid_zscore",
                        },
                    },
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path


def test_default_signal_calibration_config_loads() -> None:
    config = load_signal_calibration_config(
        signal_calibration.DEFAULT_SIGNAL_CALIBRATION_PROFILES_PATH
    )

    assert config["production_effect"] == "none"
    assert config["manual_review_required"] is True
    assert config["auto_promotion"] is False
    assert "baseline_v0_1" in config["profiles"]
