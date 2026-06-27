from __future__ import annotations

from pathlib import Path

import yaml
from test_dynamic_strategy_batch2_audit import _write_rebacktest_source
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    run_event_override_ex_ante_taxonomy_review,
    run_risk_timing_quality_review,
)


def test_event_override_ex_ante_taxonomy_review_writes_snapshot(
    tmp_path: Path,
) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    review_path = tmp_path / "event_taxonomy_review.md"
    snapshot_path = tmp_path / "event_taxonomy_snapshot.yaml"

    payload = run_event_override_ex_ante_taxonomy_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "event_taxonomy",
        run_id="unit",
        docs_path=review_path,
        yaml_path=snapshot_path,
        as_of_date=as_of,
    )

    assert payload["status"] in {
        "EVENT_OVERRIDE_EX_ANTE_TAXONOMY_READY",
        "EVENT_OVERRIDE_EX_ANTE_TAXONOMY_READY_WITH_RUNTIME_GAPS",
    }
    assert payload["summary"]["dynamic_promotion_blocked"] is True
    assert payload["summary"]["target_path_metrics_role"] == "diagnostic_only"
    assert review_path.exists()
    assert snapshot_path.exists()

    snapshot = yaml.safe_load(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot["schema_version"] == "event_override_ex_ante_taxonomy.v1"
    assert snapshot["dynamic_promotion"]["final_status"] == "BLOCKED"
    assert snapshot["event_override_role"] == "watch_only"
    assert snapshot["target_path_metrics_role"] == "diagnostic_only"
    assert snapshot["event_taxonomy_rows"]
    assert snapshot["runtime_guard_rows"]
    assert "event_override_taxonomy_audit" in snapshot["artifact_sha256"]


def test_risk_timing_quality_review_writes_matrix(tmp_path: Path) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    review_path = tmp_path / "risk_timing_review.md"
    matrix_path = tmp_path / "risk_timing_matrix.yaml"

    payload = run_risk_timing_quality_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        source_root=source_root,
        output_root=tmp_path / "timing_quality",
        run_id="unit",
        docs_path=review_path,
        yaml_path=matrix_path,
        as_of_date=as_of,
    )

    assert payload["status"] == "RISK_TIMING_QUALITY_REVIEW_READY_WITH_BLOCKERS"
    assert payload["summary"]["dynamic_promotion_blocked"] is True
    assert payload["summary"]["target_path_metrics_role"] == "diagnostic_only"
    assert review_path.exists()
    assert matrix_path.exists()

    matrix = yaml.safe_load(matrix_path.read_text(encoding="utf-8"))
    assert matrix["schema_version"] == "risk_timing_quality_matrix.v1"
    assert matrix["dynamic_promotion"]["final_status"] == "BLOCKED"
    assert matrix["target_path_metrics_role"] == "diagnostic_only"
    assert set(matrix["artifact_sha256"]) >= {
        "risk_off_entry_quality",
        "risk_on_exit_quality",
        "re_risk_delay_cost",
    }
    rows = matrix["strategy_timing_rows"]
    assert rows
    assert all(row["promotion_gate_status"] == "BLOCKED" for row in rows)
    assert all(row["target_path_metrics_role"] == "diagnostic_only" for row in rows)


def test_batch3_timing_cli_commands(tmp_path: Path) -> None:
    source_root, prices_path, marketstack_path, rates_path, as_of = _write_rebacktest_source(
        tmp_path
    )
    runner = CliRunner()

    taxonomy = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "event-override-ex-ante-taxonomy-review",
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
            str(tmp_path / "event_taxonomy"),
            "--snapshot-path",
            str(tmp_path / "event_taxonomy.yaml"),
            "--review-path",
            str(tmp_path / "event_taxonomy.md"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert taxonomy.exit_code == 0, taxonomy.output
    assert (tmp_path / "event_taxonomy.yaml").exists()

    timing = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "risk-timing-quality-review",
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
            str(tmp_path / "timing_quality"),
            "--matrix-path",
            str(tmp_path / "risk_timing.yaml"),
            "--review-path",
            str(tmp_path / "risk_timing.md"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert timing.exit_code == 0, timing.output
    assert (tmp_path / "risk_timing.yaml").exists()
