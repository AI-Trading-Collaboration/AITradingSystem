from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml
from test_execution_semantics import _write_execution_caches
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    ACTUAL_PATH_OWNER_REVIEW_BASELINES,
    ACTUAL_PATH_OWNER_REVIEW_CANDIDATES,
    run_dynamic_actual_path_owner_review_decision,
    run_dynamic_actual_path_policy_sensitivity_review,
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


def test_dynamic_actual_path_owner_review_decision_uses_actual_metrics_only(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "execution_semantics"
    run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        strategy_ids=[
            *ACTUAL_PATH_OWNER_REVIEW_BASELINES,
            *ACTUAL_PATH_OWNER_REVIEW_CANDIDATES,
        ],
        as_of_date=as_of,
    )
    docs_path = tmp_path / "docs" / "research" / "dynamic_actual_path_owner_review_decision.md"
    yaml_path = (
        tmp_path
        / "inputs"
        / "research_reviews"
        / "dynamic_actual_path_owner_review_decision.yaml"
    )

    payload = run_dynamic_actual_path_owner_review_decision(
        output_root=output_root,
        docs_path=docs_path,
        yaml_path=yaml_path,
    )

    assert payload["status"] == "DYNAMIC_ACTUAL_PATH_OWNER_REVIEW_DECISION_READY"
    assert docs_path.exists()
    assert yaml_path.exists()
    recorded = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
    assert recorded["target_path_metrics_used_for_decision"] is False
    assert recorded["dynamic_promotion_blocked"] is True
    assert len(recorded["owner_review_decisions"]) == 2
    for decision in recorded["owner_review_decisions"]:
        assert decision["review_scope"] == "actual_path_only"
        assert decision["owner_decision"]["status"] == "pending"
        assert decision["owner_manual_review_required"] is True
        assert decision["promotion_readiness"]["target_metrics_used_for_decision"] is False
        assert "annual_return" in decision["actual_path_metrics"]
        assert "target_path_annual_return" not in decision["actual_path_metrics"]
    assert "Target-path metrics" in docs_path.read_text(encoding="utf-8")


def test_dynamic_actual_path_policy_sensitivity_outputs_actual_path_leaderboard(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "policy_sensitivity"
    docs_path = tmp_path / "docs" / "research" / "dynamic_actual_path_policy_sensitivity_review.md"
    yaml_path = (
        tmp_path
        / "inputs"
        / "research_reviews"
        / "dynamic_actual_path_policy_sensitivity_matrix.yaml"
    )

    payload = run_dynamic_actual_path_policy_sensitivity_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        docs_path=docs_path,
        yaml_path=yaml_path,
        as_of_date=as_of,
    )

    assert payload["status"] == "POLICY_SENSITIVITY_REVIEW_READY"
    for file_name in (
        "index.json",
        "leaderboard_actual_path.csv",
        "target_vs_actual_gap_summary.csv",
        "promotion_readiness_summary.json",
        "policy_sensitivity_matrix.csv",
        "policy_sensitivity_summary.json",
    ):
        assert (output_root / file_name).exists()
    assert docs_path.exists()
    assert yaml_path.exists()

    matrix = pd.read_csv(output_root / "policy_sensitivity_matrix.csv")
    assert set(matrix["strategy_id"]) == {
        *ACTUAL_PATH_OWNER_REVIEW_BASELINES,
        *ACTUAL_PATH_OWNER_REVIEW_CANDIDATES,
    }
    assert set(matrix["execution_lag_days"]) == {0, 1, 2}
    assert set(matrix["rebalance_frequency"]) == {
        "next_trading_day",
        "weekly",
        "monthly",
    }
    assert set(matrix["signal_validity_window_days"]) == {1, 3, 5, 10, 20}
    assert set(matrix["turnover_constraint"]) == {
        "existing_default",
        "relaxed",
        "strict",
    }

    leaderboard = pd.read_csv(output_root / "leaderboard_actual_path.csv")
    assert "actual_path_sharpe_daily_zero_rf" in leaderboard.columns
    assert "target_path_sharpe_daily_zero_rf" not in leaderboard.columns
    assert "target_path_annual_return" not in leaderboard.columns

    readiness = json.loads(
        (output_root / "promotion_readiness_summary.json").read_text(encoding="utf-8")
    )
    assert readiness["dynamic_promotion_blocked"] is True
    assert readiness["target_path_metrics_used_for_ranking"] is False
    assert {item["strategy_id"] for item in payload["strategy_classifications"]} == set(
        ACTUAL_PATH_OWNER_REVIEW_CANDIDATES
    )
    assert {
        item["sensitivity_classification"] for item in payload["strategy_classifications"]
    } <= {
        "POLICY_STABLE",
        "POLICY_SENSITIVE_BUT_WATCHABLE",
        "POLICY_FRAGILE",
        "INSUFFICIENT_EVIDENCE",
    }
