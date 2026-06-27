from __future__ import annotations

from pathlib import Path

from test_execution_semantics import _write_execution_caches
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    run_execution_semantics_rebacktest,
    run_execution_semantics_rebacktest_gate,
)


def test_legacy_dynamic_backtest_is_not_promotion_eligible(tmp_path: Path) -> None:
    payload = run_execution_semantics_rebacktest_gate(output_root=tmp_path)

    assert payload["status"] == "EXECUTION_SEMANTICS_REBACKTEST_REQUIRED"
    assert payload["gate_decision"]["promotion_eligible"] is False
    assert payload["gate_decision"]["rebacktest_required"] is True
    assert "PRE_EXECUTION_SEMANTICS" in payload["legacy_result_tags"]
    assert "TARGET_PATH_NOT_PROMOTION_ELIGIBLE" in payload["gate_decision"][
        "blocking_reasons"
    ]


def test_static_baseline_not_blocked_by_dynamic_rebacktest_gate(tmp_path: Path) -> None:
    payload = run_execution_semantics_rebacktest_gate(
        strategy_id="100_qqq",
        backtest_generation="STATIC_BASELINE",
        position_path_used_for_metrics="ACTUAL",
        output_root=tmp_path,
    )

    assert payload["status"] == "STATIC_BASELINE_NOT_BLOCKED_BY_EXECUTION_SEMANTICS"
    assert payload["gate_decision"]["promotion_eligible"] is True
    assert payload["gate_decision"]["rebacktest_required"] is False


def test_execution_semantics_rebacktest_writes_required_strategy_artifacts(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "execution_semantics_rebacktests"

    payload = run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        strategy_ids=["limited_adjustment"],
        as_of_date=as_of,
    )

    assert payload["status"] == "EXECUTION_SEMANTICS_AWARE_REBACKTEST_COMPLETE"
    row = payload["strategy_rows"][0]
    assert row["position_path_used_for_metrics"] == "ACTUAL"
    assert row["promotion_eligible"] is False
    strategy_dir = output_root / "limited_adjustment"
    for file_name in (
        "summary.json",
        "metrics_actual_path.json",
        "metrics_target_path.json",
        "target_vs_actual_position_path.csv",
        "lag_cost_report.md",
        "signal_staleness_report.md",
        "execution_policy_snapshot.yaml",
        "promotion_readiness.json",
    ):
        assert (strategy_dir / file_name).exists()


def test_execution_semantics_rebacktest_cli_accepts_strategy_list(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "cli_rebacktest"
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "execution-semantics-rebacktest",
            "--strategy",
            "limited_adjustment",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output",
            str(output_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert (output_root / "execution_semantics_rebacktest.json").exists()
    assert (output_root / "limited_adjustment" / "promotion_readiness.json").exists()
