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
from ai_trading_system.trading_engine.portfolio_candidates import (
    _risk_guardrails_payload,
    load_portfolio_candidate_config,
    run_portfolio_candidates,
    validate_portfolio_candidates_payload,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import run_portfolio_sensitivity
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_portfolio_sensitivity import _write_portfolio_sensitivity_config
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture
from trading_engine.test_signal_ablation import _write_signal_ablation_config


def test_default_portfolio_candidate_config_loads() -> None:
    config = load_portfolio_candidate_config()

    assert config["production_effect"] == "none"
    assert config["manual_review_required"] is True
    assert config["auto_promotion"] is False
    assert config["baseline_profile"] == "baseline_current"
    assert set(config["profiles"]) >= {
        "baseline_current",
        "lower_rebalance_threshold_3pct",
        "lower_rebalance_threshold_2pct",
        "higher_score_sensitivity",
        "balanced_responsive",
        "softmax_mapping",
        "relaxed_caps_responsive",
    }
    assert config["guardrails"]["turnover_relative_increase_limit"] == 0.30


def test_portfolio_candidates_run_profiles_and_write_recommended(tmp_path: Path) -> None:
    fixture = _candidate_fixture(tmp_path)
    config_path = _write_portfolio_candidate_config(tmp_path, fixture["config_path"])
    baseline_before = fixture["baseline_path"].read_text(encoding="utf-8")

    run = run_portfolio_candidates(
        as_of=fixture["as_of"],
        profile_names=(
            "baseline_current",
            "lower_rebalance_threshold_3pct",
            "higher_score_sensitivity",
            "softmax_mapping",
        ),
        config_path=config_path,
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    assert run.recommended_candidate_path.exists()
    assert validate_portfolio_candidates_payload(run.payload) == []
    assert run.payload["metadata"]["status"] == "LIMITED"
    assert run.payload["metadata"]["production_effect"] == "none"
    assert run.payload["metadata"]["manual_review_required"] is True
    assert run.payload["metadata"]["auto_promotion"] is False
    assert run.payload["data_gate"]["status"] == "OK"
    assert run.payload["promotion_impact"]["can_support_candidate_promotion"] is False
    assert run.payload["safety"]["production_parameters_modified"] is False
    assert run.payload["safety"]["candidate_promotion_triggered"] is False
    assert run.payload["baseline"]["profile_name"] == "baseline_current"
    assert len(run.payload["candidates"]) == 3
    assert run.payload["ranking"]["best_profile"] in {
        "baseline_current",
        "lower_rebalance_threshold_3pct",
        "higher_score_sensitivity",
        "softmax_mapping",
    }

    profiles = {item["profile_name"]: item for item in run.payload["profiles"]}
    assert profiles["lower_rebalance_threshold_3pct"]["profile_config"][
        "rebalance_threshold"
    ] == 0.03
    assert profiles["higher_score_sensitivity"]["profile_config"][
        "score_sensitivity_multiplier"
    ] == 1.5
    assert profiles["softmax_mapping"]["profile_config"]["score_to_weight_method"] == "softmax"
    for candidate in run.payload["candidates"]:
        assert set(candidate["signal_transmission"]) >= {
            "score_to_target_weight_effectiveness",
            "target_to_actual_weight_effectiveness",
            "rebalance_suppression_ratio",
            "mean_abs_actual_weight_delta",
            "rebalance_days",
            "rebalance_opportunity_days",
        }
        assert set(candidate["delta_vs_baseline"]) >= {
            "rebalance_suppression_ratio_delta",
            "mean_abs_actual_weight_delta_delta",
            "rebalance_days_delta",
        }
        assert set(candidate["performance"]) >= {
            "cumulative_return",
            "annualized_return",
            "max_drawdown",
            "volatility",
            "sharpe_ratio",
            "sortino_ratio",
            "calmar_ratio",
            "turnover",
            "estimated_cost_drag",
        }
        assert candidate["risk_guardrails"]["guardrail_status"] in {"PASS", "FAIL"}
        assert candidate["signal_contribution"]["promotion_credit_signals"] == 0
        assert candidate["signal_contribution"]["reason"] == (
            "Signal snapshot quality remains LIMITED."
        )
        assert isinstance(candidate["ranking_score"], float)

    recommended = yaml.safe_load(run.recommended_candidate_path.read_text(encoding="utf-8"))
    assert recommended["production_effect"] == "none"
    assert recommended["manual_review_required"] is True
    assert recommended["auto_promotion"] is False
    assert recommended["profile_name"] == run.payload["ranking"]["best_profile"]
    assert fixture["baseline_path"].read_text(encoding="utf-8") == baseline_before


def test_portfolio_candidate_guardrail_helpers_detect_breaches() -> None:
    baseline = SimpleNamespace(metrics={"max_drawdown": -0.10, "turnover": 1.0})
    drawdown_breach = SimpleNamespace(metrics={"max_drawdown": -0.15, "turnover": 1.0})
    turnover_breach = SimpleNamespace(metrics={"max_drawdown": -0.10, "turnover": 1.5})
    guardrails = {
        "max_drawdown_worse_limit": 0.03,
        "turnover_relative_increase_limit": 0.30,
        "max_single_asset_binding_ratio": 0.25,
        "max_sector_cap_binding_ratio": 0.25,
        "max_cash_floor_binding_ratio": 0.25,
    }

    drawdown = _risk_guardrails_payload(
        simulation=drawdown_breach,
        baseline_simulation=baseline,
        constraint_binding={
            "single_asset_cap_binding_ratio": 0.0,
            "sector_cap_binding_ratio": 0.0,
            "cash_floor_binding_ratio": 0.0,
        },
        guardrail_config=guardrails,
        data_gate={"status": "OK"},
    )
    turnover = _risk_guardrails_payload(
        simulation=turnover_breach,
        baseline_simulation=baseline,
        constraint_binding={
            "single_asset_cap_binding_ratio": 0.0,
            "sector_cap_binding_ratio": 0.0,
            "cash_floor_binding_ratio": 0.0,
        },
        guardrail_config=guardrails,
        data_gate={"status": "OK"},
    )

    assert drawdown["max_drawdown_worse_than_baseline"] is True
    assert drawdown["guardrail_status"] == "FAIL"
    assert turnover["turnover_too_high"] is True
    assert turnover["guardrail_status"] == "FAIL"


def test_portfolio_candidates_shadow_backtest_reference_does_not_promote(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _candidate_fixture(tmp_path)
    config_path = _write_portfolio_candidate_config(tmp_path, fixture["config_path"])
    candidates_run = run_portfolio_candidates(
        as_of=fixture["as_of"],
        profile_names=("baseline_current", "balanced_responsive"),
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
    assert "portfolio candidate" in decision["reason"].lower()
    assert decision["supporting_artifacts"]["portfolio_candidates"] == str(
        candidates_run.json_path
    )
    assert shadow_run.payload["promotion_constraints"]["allow_candidate"] is False
    assert shadow_run.payload["candidate_parameters"]["promotion_eligible"] is False
    assert shadow_run.payload["safety"]["auto_promotion"] is False


def test_portfolio_candidates_data_gate_failure_blocks_evaluation(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=40, min_history_days=12)
    prices_path = tmp_path / "data" / "prices_daily.csv"
    prices = pd.read_csv(prices_path)
    prices = prices.loc[prices["ticker"] != "NVDA"]
    prices.to_csv(prices_path, index=False)
    config_path = _write_portfolio_candidate_config(tmp_path, fixture["config_path"])

    run = run_portfolio_candidates(
        as_of=fixture["as_of"],
        config_path=config_path,
    )

    assert run.payload["metadata"]["status"] == "FAILED"
    assert run.payload["candidates"] == []
    assert run.payload["safety"]["production_parameters_modified"] is False
    assert run.payload["data_gate"]["status"] == "FAILED"


def _candidate_fixture(tmp_path: Path) -> dict[str, object]:
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
    sensitivity_config_path = _write_portfolio_sensitivity_config(
        tmp_path,
        fixture["config_path"],
    )
    run_portfolio_sensitivity(
        as_of=fixture["as_of"],
        profile_names=("baseline_v0_1", "lower_rebalance_threshold"),
        config_path=sensitivity_config_path,
    )
    return fixture


def _write_portfolio_candidate_config(
    tmp_path: Path,
    shadow_config_path: object,
) -> Path:
    ablation_config_path = _write_signal_ablation_config(tmp_path, Path(shadow_config_path))
    config_path = tmp_path / "config" / "portfolio" / "portfolio_candidate_profiles.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "portfolio-candidates-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test portfolio construction candidates",
                "intended_effect": "test portfolio candidates",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "shadow_backtest_config_path": str(shadow_config_path),
                "signal_ablation_config_path": str(ablation_config_path),
                "input": {
                    "portfolio_sensitivity_dir": str(
                        tmp_path / "artifacts" / "portfolio_sensitivity"
                    ),
                    "shadow_backtest_dir": str(tmp_path / "artifacts" / "shadow_backtest"),
                },
                "output": {
                    "portfolio_candidates_dir": str(
                        tmp_path / "artifacts" / "portfolio_candidates"
                    ),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                },
                "baseline_profile": "baseline_current",
                "guardrails": {
                    "max_drawdown_worse_limit": 0.03,
                    "turnover_relative_increase_limit": 0.30,
                    "max_single_asset_binding_ratio": 0.25,
                    "max_sector_cap_binding_ratio": 0.25,
                    "max_cash_floor_binding_ratio": 0.25,
                },
                "ranking": {
                    "policy_version": "test-ranking",
                    "rationale": "test ranking",
                    "weights": {
                        "signal_transmission_improvement": 0.30,
                        "sharpe_improvement": 0.20,
                        "drawdown_control": 0.20,
                        "turnover_penalty": 0.15,
                        "guardrail_pass": 0.15,
                    },
                    "hard_rejection": [
                        "max_drawdown_worse_than_baseline_by_more_than_3pct",
                        "turnover_increase_more_than_30pct",
                        "data_gate_not_ok",
                    ],
                },
                "profiles": {
                    "baseline_current": {
                        "description": "Current production-like portfolio construction.",
                        "score_sensitivity_multiplier": 1.0,
                        "rebalance_threshold": 0.05,
                        "max_single_asset_weight": 0.30,
                        "max_sector_weight": 0.60,
                        "min_cash_weight": 0.05,
                        "score_to_weight_method": "linear",
                    },
                    "lower_rebalance_threshold_3pct": {
                        "description": "Lower rebalance threshold.",
                        "score_sensitivity_multiplier": 1.0,
                        "rebalance_threshold": 0.03,
                        "max_single_asset_weight": 0.30,
                        "max_sector_weight": 0.60,
                        "min_cash_weight": 0.05,
                        "score_to_weight_method": "linear",
                    },
                    "higher_score_sensitivity": {
                        "description": "Increase score sensitivity.",
                        "score_sensitivity_multiplier": 1.5,
                        "rebalance_threshold": 0.05,
                        "max_single_asset_weight": 0.30,
                        "max_sector_weight": 0.60,
                        "min_cash_weight": 0.05,
                        "score_to_weight_method": "linear",
                    },
                    "balanced_responsive": {
                        "description": "Balanced responsiveness.",
                        "score_sensitivity_multiplier": 1.25,
                        "rebalance_threshold": 0.03,
                        "max_single_asset_weight": 0.30,
                        "max_sector_weight": 0.60,
                        "min_cash_weight": 0.05,
                        "score_to_weight_method": "linear",
                    },
                    "softmax_mapping": {
                        "description": "Softmax score mapping.",
                        "score_sensitivity_multiplier": 1.0,
                        "rebalance_threshold": 0.04,
                        "max_single_asset_weight": 0.30,
                        "max_sector_weight": 0.60,
                        "min_cash_weight": 0.05,
                        "score_to_weight_method": "softmax",
                        "softmax_temperature": 0.75,
                    },
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path
