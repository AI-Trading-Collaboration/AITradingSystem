from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml
from test_execution_semantics import _write_execution_caches
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    ACTUAL_PATH_EDGE_ATTRIBUTION_STRATEGIES,
    DEFAULT_DYNAMIC_PROMOTION_GATE_V2_PATH,
    DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH,
    run_actual_path_edge_attribution_review,
    run_dynamic_strategy_objective_gate_review,
    run_execution_semantics_rebacktest,
)


def test_actual_path_edge_attribution_writes_required_artifacts(tmp_path: Path) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    edge_root = tmp_path / "edge_attribution"
    review_path = tmp_path / "actual_path_edge_attribution_review.md"
    matrix_path = tmp_path / "actual_path_edge_attribution_matrix.yaml"

    payload = run_actual_path_edge_attribution_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=edge_root,
        run_id="unit",
        docs_path=review_path,
        yaml_path=matrix_path,
        as_of_date=as_of,
    )

    assert payload["status"] == "EDGE_ATTRIBUTION_REVIEW_READY"
    assert payload["summary"]["promotion_decision_source"] == "actual_path_only"
    assert payload["summary"]["target_path_metrics_role"] == "diagnostic_only"
    assert payload["summary"]["dynamic_promotion_blocked"] is True
    assert review_path.exists()
    assert matrix_path.exists()

    matrix = yaml.safe_load(matrix_path.read_text(encoding="utf-8"))
    assert matrix["status"] == "EDGE_ATTRIBUTION_REVIEW_READY"
    assert matrix["dynamic_promotion"]["final_status"] == "BLOCKED"
    assert matrix["target_path_metrics_role"] == "diagnostic_only"
    assert matrix["run_id"] == "unit"
    assert matrix["attribution_policy"]["policy_id"]
    assert set(matrix["artifact_sha256"]) >= {
        "edge_attribution_by_strategy",
        "risk_off_event_attribution",
        "risk_on_recovery_attribution",
        "qqq_exposure_drag",
        "sgov_allocation_benefit",
    }
    rows = matrix["strategy_attributions"]
    assert {row["strategy_id"] for row in rows} == set(
        ACTUAL_PATH_EDGE_ATTRIBUTION_STRATEGIES
    )
    assert all(row["promotion_decision_source"] == "actual_path_only" for row in rows)
    assert all(row["target_path_metrics_role"] == "diagnostic_only" for row in rows)
    assert all(row["verdict"] for row in rows)


def test_dynamic_objective_gate_blocks_promotion_and_excludes_target_path(
    tmp_path: Path,
) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    matrix_path = tmp_path / "actual_path_edge_attribution_matrix.yaml"
    run_actual_path_edge_attribution_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "edge_attribution",
        run_id="unit",
        docs_path=tmp_path / "actual_path_edge_attribution_review.md",
        yaml_path=matrix_path,
        as_of_date=as_of,
    )
    gate_review_path = tmp_path / "dynamic_strategy_objective_gate_review.md"
    gate_matrix_path = tmp_path / "dynamic_strategy_objective_gate_matrix.yaml"

    payload = run_dynamic_strategy_objective_gate_review(
        edge_matrix_path=matrix_path,
        objectives_path=DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH,
        promotion_gate_path=DEFAULT_DYNAMIC_PROMOTION_GATE_V2_PATH,
        docs_path=gate_review_path,
        yaml_path=gate_matrix_path,
    )

    assert payload["status"] == "OBJECTIVE_GATE_REVIEW_READY"
    assert payload["summary"]["dynamic_promotion_blocked"] is True
    assert payload["summary"]["promotion_decision_source"] == "actual_path_only"
    rows = payload["strategy_gate_rows"]
    assert rows
    assert all(row["gate_v2_status"] == "BLOCKED" for row in rows)
    assert all(row["promotion_eligible"] is False for row in rows)
    assert all(row["target_path_metrics_role"] == "diagnostic_only" for row in rows)
    assert gate_review_path.exists()
    assert gate_matrix_path.exists()


def test_edge_attribution_and_gate_cli(tmp_path: Path) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    edge_matrix_path = tmp_path / "edge_matrix.yaml"
    runner = CliRunner()

    edge = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "actual-path-edge-attribution",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--source-root",
            str(source_root),
            "--output-root",
            str(tmp_path / "edge_attribution"),
            "--run-id",
            "cli",
            "--review-path",
            str(tmp_path / "edge_review.md"),
            "--matrix-path",
            str(edge_matrix_path),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert edge.exit_code == 0, edge.output
    assert edge_matrix_path.exists()

    gate = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-objective-gate-review",
            "--edge-matrix",
            str(edge_matrix_path),
            "--objective-config",
            str(DEFAULT_DYNAMIC_STRATEGY_OBJECTIVES_PATH),
            "--gate-config",
            str(DEFAULT_DYNAMIC_PROMOTION_GATE_V2_PATH),
            "--review-path",
            str(tmp_path / "gate_review.md"),
            "--matrix-path",
            str(tmp_path / "gate_matrix.yaml"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert gate.exit_code == 0, gate.output
    assert (tmp_path / "gate_matrix.yaml").exists()


def _write_rebacktest_source(tmp_path: Path) -> tuple[Path, Path, Path, Path, date]:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    source_root = tmp_path / "execution_semantics"
    run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=source_root,
        strategy_ids=[
            "no_trade",
            "100_qqq",
            "qqq_60_sgov_40",
            "qqq_50_sgov_50",
            "limited_adjustment",
            "dynamic_v0_5_ai_trend_confirmed_only",
        ],
        as_of_date=as_of,
        enable_event_override=True,
        emit_pending_plan_ledger=True,
        emit_supersede_log=True,
        emit_event_override_trace=True,
        event_override_survival_matrix_path=tmp_path / "event_override_matrix.yaml",
        event_override_review_path=tmp_path / "event_override_review.md",
    )
    return source_root, prices_path, marketstack_path, rates_path, as_of
