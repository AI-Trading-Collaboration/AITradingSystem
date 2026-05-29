from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from ai_trading_system.trading_engine import signal_ablation
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.parameters.parameter_schema import SignalAblationThresholds
from ai_trading_system.trading_engine.signal_ablation import (
    classify_ablation_delta,
    run_signal_ablation,
    validate_signal_ablation_payload,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    REQUIRED_SIGNALS,
    run_signal_snapshot_build,
)
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_signal_ablation_remove_one_signal_writes_summary(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])

    run = run_signal_ablation(
        as_of=fixture["as_of"],
        config_path=config_path,
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    payload = json.loads(run.json_path.read_text(encoding="utf-8"))
    assert validate_signal_ablation_payload(payload) == []
    assert payload["metadata"]["status"] == "LIMITED"
    assert payload["metadata"]["production_effect"] == "none"
    assert payload["metadata"]["manual_review_required"] is True
    assert payload["metadata"]["auto_promotion"] is False
    assert payload["metadata"]["backtest_mode"] == "full_signal_backtest_limited"
    assert {item["signal"] for item in payload["signal_contributions"]} == set(
        REQUIRED_SIGNALS
    )

    trend = _contribution(payload, "trend_momentum")
    sector = _contribution(payload, "sector_strength")
    assert trend["mode"] == "remove_one_signal"
    assert trend["quality"] == "price_derived"
    assert "sharpe_ratio_delta" in trend["remove_one_signal_delta"]
    assert trend["window_stability"]["window_count"] >= 2
    assert trend["used_in_score_calculation"] is True
    assert trend["classification_reason"]
    assert trend["diagnostic_status"] in {
        "VALID",
        "BELOW_THRESHOLD",
        "NO_SCORE_IMPACT",
        "NO_PORTFOLIO_IMPACT",
    }
    assert sector["quality"] == "price_derived"
    assert sector["window_stability"]["window_count"] >= 2

    earnings = _contribution(payload, "earnings_quality")
    assert earnings["quality"] == "neutral_fallback"
    assert earnings["promotion_credit_allowed"] is False
    assert "Neutral fallback signal cannot be used" in " ".join(earnings["warnings"])

    macro = _contribution(payload, "macro_liquidity")
    assert macro["promotion_credit_allowed"] is False
    assert "Proxy signal cannot be used" in " ".join(macro["warnings"])

    summary = payload["summary"]
    assert summary["can_support_candidate_promotion"] is False
    assert "No promotion-credit signals" in summary["no_promotion_credit_reason"]
    assert set(summary["fallback_signals"]) >= {"earnings_quality", "valuation_risk", "event_risk"}


def test_signal_ablation_supports_selected_signals_and_dry_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    monkeypatch.setattr(signal_ablation, "PROJECT_ROOT", tmp_path)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])

    run = run_signal_ablation(
        as_of=fixture["as_of"],
        signals=("trend_momentum", "sector_strength"),
        config_path=config_path,
        dry_run=True,
    )

    assert "outputs" in str(run.json_path)
    assert "dry_runs" in str(run.json_path)
    assert {item["signal"] for item in run.payload["signal_contributions"]} == {
        "trend_momentum",
        "sector_strength",
    }


def test_signal_ablation_classification_rules() -> None:
    thresholds = SignalAblationThresholds(
        annualized_return_noise_band=0.01,
        sharpe_noise_band=0.05,
        max_drawdown_noise_band=0.02,
        turnover_noise_band=0.10,
    )

    assert (
        classify_ablation_delta(
            {
                "annualized_return_delta": -0.03,
                "sharpe_ratio_delta": -0.12,
                "max_drawdown_delta": -0.03,
                "turnover_delta": 0.02,
            },
            thresholds,
        )
        == "positive"
    )
    assert (
        classify_ablation_delta(
            {
                "annualized_return_delta": 0.00,
                "sharpe_ratio_delta": 0.12,
                "max_drawdown_delta": 0.03,
                "turnover_delta": -0.02,
            },
            thresholds,
        )
        == "negative"
    )
    assert (
        classify_ablation_delta(
            {
                "annualized_return_delta": 0.001,
                "sharpe_ratio_delta": 0.01,
                "max_drawdown_delta": 0.0,
                "turnover_delta": 0.01,
            },
            thresholds,
        )
        == "neutral"
    )
    assert (
        signal_ablation._overall_class(
            delta={
                "annualized_return_delta": -0.03,
                "sharpe_ratio_delta": -0.12,
                "max_drawdown_delta": -0.03,
                "turnover_delta": 0.0,
            },
            window_rows=[{"class": "positive"}, {"class": "negative"}],
            thresholds=thresholds,
            min_walk_forward_windows=2,
        )
        == "unstable"
    )
    assert (
        signal_ablation._overall_class(
            delta={
                "annualized_return_delta": -0.03,
                "sharpe_ratio_delta": -0.12,
                "max_drawdown_delta": -0.03,
                "turnover_delta": 0.0,
            },
            window_rows=[{"class": "positive"}],
            thresholds=thresholds,
            min_walk_forward_windows=2,
        )
        == "insufficient_data"
    )


def test_shadow_promotion_decision_links_signal_ablation_without_promoting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=16, min_history_days=8)
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)
    run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )
    config_path = _write_signal_ablation_config(tmp_path, fixture["config_path"])
    ablation_run = run_signal_ablation(as_of=fixture["as_of"], config_path=config_path)

    shadow_run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
    )

    decision = shadow_run.payload["promotion_decision"]
    assert decision["status"] in {"rejected", "watch"}
    assert "candidate promotion remains disabled" in decision["reason"].lower()
    assert decision["supporting_artifacts"]["signal_ablation"] == str(ablation_run.json_path)
    assert shadow_run.payload["candidate_parameters"]["promotion_eligible"] is False


def _contribution(payload: dict[str, object], signal: str) -> dict[str, object]:
    for item in payload["signal_contributions"]:  # type: ignore[index]
        if isinstance(item, dict) and item.get("signal") == signal:
            return item
    raise AssertionError(f"missing contribution for {signal}")


def _write_signal_ablation_config(tmp_path: Path, shadow_config_path: Path) -> Path:
    config_path = tmp_path / "config" / "parameters" / "signal_ablation.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "signal-ablation-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test signal contribution validation",
                "intended_effect": "test remove-one-signal ablation",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "shadow_backtest_config_path": str(shadow_config_path),
                "ablation_modes": ["remove_one_signal"],
                "default_mode": "remove_one_signal",
                "thresholds": {
                    "annualized_return_noise_band": 0.01,
                    "sharpe_noise_band": 0.05,
                    "max_drawdown_noise_band": 0.02,
                    "turnover_noise_band": 0.10,
                },
                "stability": {"min_walk_forward_windows": 2},
                "diagnostics": {
                    "score_delta_epsilon": 0.000001,
                    "portfolio_weight_delta_epsilon": 0.000001,
                    "non_neutral_value_epsilon": 0.000001,
                },
                "output": {
                    "signal_ablation_dir": str(tmp_path / "artifacts" / "signal_ablation"),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path
