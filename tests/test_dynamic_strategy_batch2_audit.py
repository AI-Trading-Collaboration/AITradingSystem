from __future__ import annotations

from datetime import date
from pathlib import Path

import yaml
from test_execution_semantics import _write_execution_caches
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    DEFAULT_DYNAMIC_WALK_FORWARD_POLICY_PATH,
    run_actual_path_edge_attribution_review,
    run_dynamic_strategy_walk_forward_validation,
    run_execution_semantics_rebacktest,
    run_pit_data_availability_audit,
)


def test_pit_data_availability_audit_writes_inventory(tmp_path: Path) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    inventory_path = tmp_path / "pit_data_availability_inventory.yaml"
    review_path = tmp_path / "pit_data_availability_audit.md"

    payload = run_pit_data_availability_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "pit_audit",
        run_id="unit",
        docs_path=review_path,
        inventory_path=inventory_path,
        as_of_date=as_of,
    )

    assert payload["status"] in {
        "PIT_DATA_AVAILABILITY_REVIEW_READY_WITH_CAVEATS",
        "PIT_DATA_AVAILABILITY_REVIEW_BLOCKED",
    }
    assert payload["summary"]["dynamic_promotion_blocked"] is True
    assert payload["summary"]["target_path_metrics_role"] == "diagnostic_only"
    assert review_path.exists()
    assert inventory_path.exists()

    inventory = yaml.safe_load(inventory_path.read_text(encoding="utf-8"))
    assert inventory["schema_version"] == "pit_data_availability_inventory.v1"
    assert inventory["promotion_gate"]["dynamic_promotion_final_status"] == "BLOCKED"
    rows = inventory["signal_inventory"]
    assert rows
    assert all("available_to_system_at" in row for row in rows)
    assert all("decision_at" in row for row in rows)
    assert all("effective_at" in row for row in rows)
    assert all("pit_risk_level" in row for row in rows)
    assert any(row["pit_risk_level"] == "PIT_APPROXIMATED" for row in rows)


def test_dynamic_strategy_walk_forward_validation_writes_matrix(tmp_path: Path) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    edge_matrix_path = tmp_path / "actual_path_edge_attribution_matrix.yaml"
    run_actual_path_edge_attribution_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "edge_attribution",
        run_id="unit",
        docs_path=tmp_path / "actual_path_edge_attribution_review.md",
        yaml_path=edge_matrix_path,
        as_of_date=as_of,
    )
    review_path = tmp_path / "walk_forward_review.md"
    matrix_path = tmp_path / "walk_forward_matrix.yaml"

    payload = run_dynamic_strategy_walk_forward_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        edge_matrix_path=edge_matrix_path,
        source_root=source_root,
        output_root=tmp_path / "walk_forward",
        run_id="unit",
        docs_path=review_path,
        yaml_path=matrix_path,
        as_of_date=as_of,
    )

    assert payload["status"] == "WALK_FORWARD_VALIDATION_READY_WITH_BLOCKERS"
    assert payload["summary"]["dynamic_promotion_blocked"] is True
    assert payload["summary"]["target_path_metrics_role"] == "diagnostic_only"
    assert review_path.exists()
    assert matrix_path.exists()

    matrix = yaml.safe_load(matrix_path.read_text(encoding="utf-8"))
    assert matrix["schema_version"] == "dynamic_strategy_walk_forward_matrix.v1"
    assert matrix["dynamic_promotion"]["final_status"] == "BLOCKED"
    assert matrix["target_path_metrics_role"] == "diagnostic_only"
    assert set(matrix["artifact_sha256"]) >= {
        "walk_forward_leaderboard",
        "rolling_oos_metrics",
        "parameter_stability_heatmap",
        "regime_holdout_results",
    }
    rows = matrix["strategy_validation_rows"]
    assert rows
    assert all(row["promotion_gate_status"] == "BLOCKED" for row in rows)
    assert all(row["target_path_metrics_role"] == "diagnostic_only" for row in rows)


def test_batch2_audit_cli_commands(tmp_path: Path) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    edge_matrix_path = tmp_path / "edge_matrix.yaml"
    run_actual_path_edge_attribution_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "edge_attribution",
        run_id="unit",
        docs_path=tmp_path / "edge_review.md",
        yaml_path=edge_matrix_path,
        as_of_date=as_of,
    )
    runner = CliRunner()

    pit = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "pit-data-availability-audit",
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
            str(tmp_path / "pit_audit"),
            "--inventory-path",
            str(tmp_path / "pit_inventory.yaml"),
            "--review-path",
            str(tmp_path / "pit_review.md"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert pit.exit_code == 0, pit.output
    assert (tmp_path / "pit_inventory.yaml").exists()

    walk = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-walk-forward-validation",
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
            "--edge-matrix-path",
            str(edge_matrix_path),
            "--walk-forward-policy",
            str(DEFAULT_DYNAMIC_WALK_FORWARD_POLICY_PATH),
            "--output-root",
            str(tmp_path / "walk_forward"),
            "--matrix-path",
            str(tmp_path / "walk_matrix.yaml"),
            "--review-path",
            str(tmp_path / "walk_review.md"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert walk.exit_code == 0, walk.output
    assert (tmp_path / "walk_matrix.yaml").exists()


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
