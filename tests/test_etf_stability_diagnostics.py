from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio.stability import (
    build_allocation_stability_diagnostics,
    render_allocation_stability_markdown,
    write_allocation_stability_diagnostics,
)


def test_allocation_stability_diagnostics_calculates_turnover_and_exposure() -> None:
    payload = build_allocation_stability_diagnostics(
        _daily_frame(),
        _weights_frame(),
        max_daily_turnover=0.30,
        max_rebalance_trade_weight=0.50,
    )

    assert payload["schema_version"] == 1
    assert payload["diagnostic_type"] == "allocation_stability"
    assert payload["status"] == "STABLE"
    assert payload["daily_turnover"] == [
        {"signal_date": "2026-01-02", "turnover": 0.8},
        {"signal_date": "2026-01-05", "turnover": 0.2},
        {"signal_date": "2026-01-06", "turnover": 0.1},
    ]
    assert payload["daily_turnover_average"] == pytest.approx(0.3666666667)
    assert payload["max_rebalance_turnover"] == pytest.approx(0.2)
    assert payload["average_absolute_weight_delta"] == pytest.approx(0.2555555556)
    assert payload["median_absolute_weight_delta"] == pytest.approx(0.2)
    assert payload["max_single_day_weight_delta"] == pytest.approx(0.7)
    assert payload["max_single_day_weight_delta_after_initial"] == pytest.approx(0.4)
    assert payload["rebalance_count"] == 3
    assert payload["rebalance_frequency"] == pytest.approx(1.0)
    assert payload["regime_transition_count"] == 1
    assert payload["constraint_hit_count"] == 2
    assert payload["constraint_hit_rate"] == pytest.approx(2 / 3)
    assert payload["constraint_hit_by_id"] == {
        "MAX_DAILY_TURNOVER": 1,
        "REGIME_CASH_MIN": 1,
        "REGIME_EQUITY_CAP": 1,
    }
    assert payload["cash_weight_min"] == pytest.approx(0.3)
    assert payload["cash_weight_max"] == pytest.approx(0.7)
    assert payload["cash_weight_average"] == pytest.approx(0.4333333333)
    assert payload["equity_exposure_average"] == pytest.approx(0.5666666667)
    assert payload["semiconductor_exposure_average"] == pytest.approx(0.1166666667)
    assert payload["asset_exposure_time"] == {
        "CASH": 1.0,
        "SMH": pytest.approx(2 / 3),
        "SPY": 1.0,
    }
    assert payload["average_holding_period"]["portfolio_average_days"] == pytest.approx(
        2.6666666667
    )
    assert payload["policy"]["initial_deployment_excluded_from_policy_check"] is True


def test_allocation_stability_status_flags_post_initial_policy_breach() -> None:
    payload = build_allocation_stability_diagnostics(
        _daily_frame(),
        _weights_frame(),
        max_daily_turnover=0.30,
        max_rebalance_trade_weight=0.30,
    )

    assert payload["status"] == "TOO_JUMPY"
    assert payload["reason_codes"] == ["MAX_SINGLE_DAY_WEIGHT_DELTA_ABOVE_POLICY"]


def test_allocation_stability_markdown_and_json_are_stable(tmp_path: Path) -> None:
    payload = build_allocation_stability_diagnostics(
        _daily_frame(),
        _weights_frame(),
        max_daily_turnover=0.30,
        max_rebalance_trade_weight=0.50,
    )
    json_path, markdown_path = write_allocation_stability_diagnostics(
        payload,
        tmp_path / "stability_diagnostics.json",
        tmp_path / "stability_diagnostics.md",
    )
    markdown = render_allocation_stability_markdown(payload)

    assert json.loads(json_path.read_text(encoding="utf-8"))["status"] == "STABLE"
    assert "ETF Allocation Stability Diagnostics" in markdown_path.read_text(encoding="utf-8")
    assert "Constraint Hit Rate" in markdown
    assert "| SPY | 100.00% | 3.00 |" in markdown


def test_backtest_diagnostics_cli_generates_latest_run_artifacts(tmp_path: Path) -> None:
    run_dir = tmp_path / "etf-backtest-fixture"
    run_dir.mkdir()
    _daily_frame().to_csv(run_dir / "daily.csv", index=False)
    _weights_frame().to_csv(run_dir / "weights.csv", index=False)
    (run_dir / "summary.json").write_text("{}", encoding="utf-8")
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "etf",
            "backtest",
            "diagnostics",
            "--output-dir",
            str(tmp_path),
            "--latest",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "ETF allocation stability status" in result.output
    payload = json.loads((run_dir / "stability_diagnostics.json").read_text(encoding="utf-8"))
    assert payload["status"] == "TOO_JUMPY"
    assert (run_dir / "stability_diagnostics.md").exists()


def _daily_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"signal_date": "2026-01-02", "turnover": 0.80, "regime": "Risk-On"},
            {"signal_date": "2026-01-05", "turnover": 0.20, "regime": "Risk-On"},
            {"signal_date": "2026-01-06", "turnover": 0.10, "regime": "Risk-Off"},
        ]
    )


def _weights_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _weight_row("2026-01-02", "SPY", 0.50, 0.50, []),
            _weight_row("2026-01-02", "SMH", 0.20, 0.20, []),
            _weight_row("2026-01-02", "CASH", 0.30, -0.70, []),
            _weight_row("2026-01-05", "SPY", 0.55, 0.05, ["MAX_DAILY_TURNOVER"]),
            _weight_row("2026-01-05", "SMH", 0.15, -0.05, ["MAX_DAILY_TURNOVER"]),
            _weight_row("2026-01-05", "CASH", 0.30, 0.00, ["MAX_DAILY_TURNOVER"]),
            _weight_row(
                "2026-01-06",
                "SPY",
                0.30,
                -0.25,
                ["REGIME_EQUITY_CAP", "REGIME_CASH_MIN"],
            ),
            _weight_row(
                "2026-01-06",
                "SMH",
                0.00,
                -0.15,
                ["REGIME_EQUITY_CAP", "REGIME_CASH_MIN"],
            ),
            _weight_row(
                "2026-01-06",
                "CASH",
                0.70,
                0.40,
                ["REGIME_EQUITY_CAP", "REGIME_CASH_MIN"],
            ),
        ]
    )


def _weight_row(
    signal_date: str,
    symbol: str,
    target_weight: float,
    trade_delta: float,
    constraints: list[str],
) -> dict[str, object]:
    return {
        "signal_date": signal_date,
        "symbol": symbol,
        "target_weight": target_weight,
        "trade_delta": trade_delta,
        "constraints_applied": json.dumps(constraints),
    }
