from __future__ import annotations

from pathlib import Path

import yaml
from test_dynamic_strategy_batch2_audit import _write_rebacktest_source
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    run_regime_segmentation_baseline_expansion_review,
    run_research_artifact_governance_review,
    run_stress_risk_metrics_review,
    run_transaction_cost_cash_yield_audit,
)


def test_batch4_cost_stress_regime_builders_write_governed_matrices(
    tmp_path: Path,
) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )

    cost_matrix_path = tmp_path / "transaction_cost_cash_yield_matrix.yaml"
    cost_payload = run_transaction_cost_cash_yield_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "cost_cash_yield",
        run_id="unit",
        docs_path=tmp_path / "transaction_cost_cash_yield_audit.md",
        yaml_path=cost_matrix_path,
        as_of_date=as_of,
    )
    assert cost_payload["status"] == "TRANSACTION_COST_CASH_YIELD_REVIEW_READY_WITH_BLOCKERS"
    cost_matrix = yaml.safe_load(cost_matrix_path.read_text(encoding="utf-8"))
    assert cost_matrix["schema_version"] == "transaction_cost_cash_yield_matrix.v1"
    assert cost_matrix["transaction_cost_model_hash"]
    assert cost_matrix["cash_yield_model_hash"]
    assert cost_matrix["dynamic_promotion"]["final_status"] == "BLOCKED"
    assert cost_matrix["target_path_metrics_role"] == "diagnostic_only"
    assert cost_matrix["strategy_cost_cash_yield_rows"]
    assert all(
        row["promotion_gate_status"] == "BLOCKED"
        for row in cost_matrix["strategy_cost_cash_yield_rows"]
    )

    stress_matrix_path = tmp_path / "stress_risk_metrics_matrix.yaml"
    stress_payload = run_stress_risk_metrics_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "stress_risk",
        run_id="unit",
        docs_path=tmp_path / "stress_risk_metrics_review.md",
        yaml_path=stress_matrix_path,
        as_of_date=as_of,
    )
    assert stress_payload["status"] == "STRESS_RISK_METRICS_REVIEW_READY_WITH_BLOCKERS"
    stress_matrix = yaml.safe_load(stress_matrix_path.read_text(encoding="utf-8"))
    assert stress_matrix["schema_version"] == "stress_risk_metrics_matrix.v1"
    assert stress_matrix["stress_policy_hash"]
    assert stress_matrix["target_path_metrics_role"] == "diagnostic_only"
    assert stress_matrix["strategy_stress_rows"]
    assert all(
        row["promotion_gate_status"] == "BLOCKED"
        for row in stress_matrix["strategy_stress_rows"]
    )

    regime_matrix_path = tmp_path / "regime_baseline_expansion_matrix.yaml"
    regime_payload = run_regime_segmentation_baseline_expansion_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "regime_review",
        run_id="unit",
        docs_path=tmp_path / "regime_segmentation_baseline_expansion_review.md",
        yaml_path=regime_matrix_path,
        as_of_date=as_of,
    )
    assert regime_payload["status"] == "REGIME_BASELINE_EXPANSION_REVIEW_READY_WITH_BLOCKERS"
    regime_matrix = yaml.safe_load(regime_matrix_path.read_text(encoding="utf-8"))
    assert regime_matrix["schema_version"] == "regime_baseline_expansion_matrix.v1"
    assert regime_matrix["regime_policy_hash"]
    assert regime_matrix["target_path_metrics_role"] == "diagnostic_only"
    assert regime_matrix["regime_metric_rows"]
    assert regime_matrix["expanded_baseline_leaderboard"]
    assert all(
        row["promotion_gate_status"] == "BLOCKED"
        for row in regime_matrix["regime_metric_rows"]
    )


def test_every_research_snapshot_has_source_commit_and_config_hash(
    tmp_path: Path,
) -> None:
    rows = _governance_rows(tmp_path)
    snapshot_rows = [
        row
        for row in rows
        if row["artifact_id"] in set(_tracked_snapshot_paths(tmp_path))
    ]

    assert snapshot_rows
    assert all(row["source_commit"] for row in snapshot_rows)
    assert all(row["config_hash"] for row in snapshot_rows)
    assert all(row["policy_hash_present"] is True for row in snapshot_rows)
    assert all(row["data_snapshot_hash"] for row in snapshot_rows)
    assert all(row["governance_status"] == "PASS" for row in snapshot_rows)


def test_actual_path_metric_namespace_required_for_leaderboard(
    tmp_path: Path,
) -> None:
    rows = _governance_rows(tmp_path)
    actual_metric_rows = [
        row for row in rows if str(row["artifact_id"]).endswith("metrics_actual_path.json")
    ]

    assert actual_metric_rows
    assert all("actual_path" in str(row["metric_namespace"]) for row in actual_metric_rows)
    assert all(row["promotion_status"] == "INPUT_ONLY" for row in actual_metric_rows)
    assert all(row["governance_status"] == "PASS" for row in actual_metric_rows)


def test_legacy_dynamic_result_cannot_unlock_promotion(tmp_path: Path) -> None:
    rows = _governance_rows(tmp_path)
    legacy_row = next(
        row
        for row in rows
        if row["artifact_id"] == "legacy_dynamic_result_cannot_unlock_promotion"
    )

    assert legacy_row["status"] == "DYNAMIC_PROMOTION_BLOCKED"
    assert legacy_row["promotion_status"] == "BLOCKED"
    assert legacy_row["governance_status"] == "PASS"


def test_target_path_metric_cannot_enter_promotion_gate(tmp_path: Path) -> None:
    rows = _governance_rows(tmp_path)
    target_metric_rows = [
        row for row in rows if str(row["artifact_id"]).endswith("metrics_target_path.json")
    ]

    assert target_metric_rows
    assert all("target_path" in str(row["metric_namespace"]) for row in target_metric_rows)
    assert all(row["promotion_status"] == "BLOCKED" for row in target_metric_rows)
    assert all(row["governance_status"] == "PASS" for row in target_metric_rows)


def test_batch4_cli_commands_are_registered(tmp_path: Path) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    runner = CliRunner()

    cost = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "transaction-cost-cash-yield-audit",
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
            str(tmp_path / "cost_cash_yield"),
            "--matrix-path",
            str(tmp_path / "cost_cash_yield.yaml"),
            "--review-path",
            str(tmp_path / "cost_cash_yield.md"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert cost.exit_code == 0, cost.output
    assert (tmp_path / "cost_cash_yield.yaml").exists()

    for command_name in (
        "stress-risk-metrics-review",
        "regime-segmentation-baseline-expansion-review",
        "research-artifact-governance-review",
    ):
        result = runner.invoke(
            app,
            ["research", "strategies", command_name, "--help"],
            env={"COLUMNS": "180"},
            terminal_width=180,
        )
        assert result.exit_code == 0, result.output


def _governance_rows(tmp_path: Path) -> list[dict[str, object]]:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    tracked_paths = _tracked_snapshot_paths(tmp_path)
    for artifact_id, path in tracked_paths.items():
        _write_snapshot(path, artifact_id)

    review_path = tmp_path / "research_artifact_governance_review.md"
    snapshot_path = tmp_path / "research_artifact_governance_snapshot.yaml"
    payload = run_research_artifact_governance_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "artifact_governance",
        run_id="unit",
        docs_path=review_path,
        yaml_path=snapshot_path,
        as_of_date=as_of,
        tracked_snapshot_paths=tracked_paths,
    )

    assert payload["summary"]["dynamic_promotion_blocked"] is True
    assert payload["summary"]["target_path_metrics_role"] == "diagnostic_only"
    assert review_path.exists()
    assert snapshot_path.exists()
    snapshot = yaml.safe_load(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["schema_version"] == "research_artifact_governance_snapshot.v1"
    assert snapshot["dynamic_promotion"]["final_status"] == "BLOCKED"
    assert snapshot["target_path_metrics_role"] == "diagnostic_only"
    return snapshot["artifact_governance_rows"]


def _tracked_snapshot_paths(tmp_path: Path) -> dict[str, Path]:
    snapshot_root = tmp_path / "snapshots"
    return {
        artifact_id: snapshot_root / f"{artifact_id}.yaml"
        for artifact_id in (
            "actual_path_edge_attribution_matrix",
            "dynamic_strategy_objective_gate_matrix",
            "pit_data_availability_inventory",
            "dynamic_strategy_walk_forward_matrix",
            "event_override_ex_ante_taxonomy",
            "risk_timing_quality_matrix",
            "transaction_cost_cash_yield_matrix",
            "stress_risk_metrics_matrix",
            "regime_baseline_expansion_matrix",
        )
    }


def _write_snapshot(path: Path, artifact_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": f"{artifact_id}.v1",
                "report_type": artifact_id,
                "status": "UNIT_READY",
                "source_commit": "unit-source-commit",
                "config_hash": "unit-config-hash",
                "policy_hash": "unit-policy-hash",
                "data_snapshot_hash": "unit-data-snapshot-hash",
                "date_range": {
                    "start": "2022-12-01",
                    "end": "2025-10-29",
                    "market_regime": "ai_after_chatgpt",
                },
                "dynamic_promotion": {"final_status": "BLOCKED"},
                "promotion_decision_source": "actual_path_only",
                "target_path_metrics_role": "diagnostic_only",
                "artifact_sha256": {"unit_artifact": "unit-artifact-hash"},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
