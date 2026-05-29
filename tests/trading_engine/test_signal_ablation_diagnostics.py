from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import cli
from ai_trading_system.trading_engine import signal_ablation
from ai_trading_system.trading_engine.parameters.parameter_schema import SignalAblationThresholds
from ai_trading_system.trading_engine.signal_ablation import run_signal_ablation
from ai_trading_system.trading_engine.signal_snapshots import run_signal_snapshot_build
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture
from trading_engine.test_signal_ablation import _contribution, _write_signal_ablation_config


def test_signal_ablation_diagnostics_show_real_signal_usage_and_impacts(
    tmp_path: Path,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])

    run = run_signal_ablation(as_of=fixture["as_of"], config_path=config_path)
    payload = run.payload

    diagnostics = payload["diagnostics"]
    assert diagnostics["all_real_signals_used_in_score"] is True
    assert diagnostics["classification_reasons_present"] is True
    assert "No promotion-credit signals" in diagnostics["no_promotion_credit_reason"]

    trend = _contribution(payload, "trend_momentum")
    usage = trend["signal_usage_diagnostics"]
    assert usage["present_in_snapshot"] is True
    assert usage["present_in_weights"] is True
    assert usage["effective_weight"] > 0
    assert usage["non_neutral_value_ratio"] >= 0
    assert usage["used_in_score_calculation"] is True

    assert "affected_asset_days" in trend["score_impact"]
    assert "mean_abs_weight_delta" in trend["portfolio_impact"]
    assert trend["diagnostic_status"] in {
        "VALID",
        "BELOW_THRESHOLD",
        "NO_SCORE_IMPACT",
        "NO_PORTFOLIO_IMPACT",
    }
    assert trend["classification_reason"]
    assert trend["threshold_diagnostics"]["sharpe_delta"]["threshold"] == 0.05


def test_signal_ablation_diagnostic_status_rules() -> None:
    thresholds = SignalAblationThresholds(
        annualized_return_noise_band=0.01,
        sharpe_noise_band=0.05,
        max_drawdown_noise_band=0.02,
        turnover_noise_band=0.10,
    )
    usage = {
        "present_in_snapshot": True,
        "present_in_weights": True,
        "used_in_score_calculation": False,
    }
    score_impact = {"max_abs_score_delta": 0.0}
    portfolio_impact = {"max_abs_weight_delta": 0.0}

    status = signal_ablation._diagnostic_status(
        signal="trend_momentum",
        quality="price_derived",
        contribution_class="neutral",
        usage_diagnostics=usage,
        score_impact=score_impact,
        portfolio_impact=portfolio_impact,
        score_delta_epsilon=0.000001,
        portfolio_weight_delta_epsilon=0.000001,
    )
    assert status == "NOT_USED_IN_SCORE"

    warnings = signal_ablation._signal_warnings(
        signal="trend_momentum",
        quality="price_derived",
        contribution_class="neutral",
        usage_diagnostics=usage,
        score_impact=score_impact,
        portfolio_impact=portfolio_impact,
        diagnostic_status=status,
        score_delta_epsilon=0.000001,
        portfolio_weight_delta_epsilon=0.000001,
    )
    assert "not used in score calculation" in " ".join(warnings)

    no_score_status = signal_ablation._diagnostic_status(
        signal="trend_momentum",
        quality="price_derived",
        contribution_class="neutral",
        usage_diagnostics={**usage, "used_in_score_calculation": True},
        score_impact=score_impact,
        portfolio_impact=portfolio_impact,
        score_delta_epsilon=0.000001,
        portfolio_weight_delta_epsilon=0.000001,
    )
    assert no_score_status == "NO_SCORE_IMPACT"

    no_portfolio_status = signal_ablation._diagnostic_status(
        signal="sector_strength",
        quality="price_derived",
        contribution_class="neutral",
        usage_diagnostics={**usage, "used_in_score_calculation": True},
        score_impact={"max_abs_score_delta": 0.01},
        portfolio_impact=portfolio_impact,
        score_delta_epsilon=0.000001,
        portfolio_weight_delta_epsilon=0.000001,
    )
    assert no_portfolio_status == "NO_PORTFOLIO_IMPACT"

    below_reason = signal_ablation._classification_reason(
        signal="trend_momentum",
        quality="price_derived",
        contribution_class="neutral",
        diagnostic_status="BELOW_THRESHOLD",
        delta={
            "annualized_return_delta": -0.003,
            "sharpe_ratio_delta": -0.02,
            "max_drawdown_delta": -0.001,
        },
        thresholds=thresholds,
        score_impact={"max_abs_score_delta": 0.01},
        portfolio_impact={"max_abs_weight_delta": 0.01},
    )
    assert "below the configured noise bands" in below_reason


def test_fallback_signal_receives_fallback_diagnostic_status(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])

    run = run_signal_ablation(as_of=fixture["as_of"], config_path=config_path)
    earnings = _contribution(run.payload, "earnings_quality")

    assert earnings["diagnostic_status"] == "FALLBACK_SIGNAL"
    assert earnings["promotion_credit_allowed"] is False
    assert "neutral fallback" in earnings["classification_reason"].lower()


def test_signal_ablation_explain_cli_outputs_diagnostics(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])
    run = run_signal_ablation(as_of=fixture["as_of"], config_path=config_path)

    result = CliRunner().invoke(
        cli.app,
        ["signals", "explain-ablation", "--input-path", str(run.json_path)],
    )

    assert result.exit_code == 0
    assert "Signal Ablation Diagnostics" in result.output
    assert "trend_momentum" in result.output
    assert "used_in_score" in result.output
