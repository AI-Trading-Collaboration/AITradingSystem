from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.breadth_current_constituents_proxy_diagnostics import (
    CurrentConstituentsProxyDiagnosticsError,
    build_current_constituents_breadth_proxy_diagnostics_artifacts,
    run_current_constituents_breadth_proxy_diagnostics,
)
from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_current_constituents_proxy_diagnostics_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "current-constituents-breadth-proxy-diagnostics" in result.output


def test_current_constituents_proxy_diagnostics_writes_source_blocked_outputs(
    tmp_path: Path,
) -> None:
    feasibility_dir = _write_trading_2302_source(tmp_path / "source")
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "current-constituents-breadth-proxy-diagnostics",
            "--feasibility-dir",
            str(feasibility_dir),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required_outputs = [
        "breadth_proxy_diagnostics_summary.json",
        "breadth_proxy_source_coverage_matrix.json",
        "breadth_proxy_signal_distribution_matrix.json",
        "breadth_proxy_asset_horizon_drilldown.json",
        "breadth_proxy_bias_warning_report.json",
        "breadth_proxy_next_step_recommendation.json",
        "breadth_proxy_safety_boundary.json",
    ]
    for filename in required_outputs:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "current_constituents_breadth_proxy_diagnostics_report.md").exists()

    summary = json.loads(
        (output_dir / "breadth_proxy_diagnostics_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["status"] == "CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED"
    assert summary["summary"]["source_snapshot_status"] == (
        "ALL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_MISSING"
    )
    assert summary["pit_status"] == "current_constituents_proxy_only"
    assert summary["strict_pit_ready"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["candidate_artifact_generated"] is False
    assert summary["candidate_signal_series_generated"] is False
    assert summary["actual_path_validation_executed"] is False


def test_current_constituents_proxy_diagnostics_distribution_is_not_computed(
    tmp_path: Path,
) -> None:
    artifacts = build_current_constituents_breadth_proxy_diagnostics_artifacts(
        feasibility_dir=_write_trading_2302_source(tmp_path / "source"),
        current_constituents_dir=None,
        target_etfs=["QQQ", "SPY", "SMH"],
        target_assets=["QQQ", "SPY", "SMH"],
        horizons=["5d", "10d", "20d"],
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )

    rows = artifacts["signal_distribution_matrix"]["rows"]
    assert {row["signal_name"] for row in rows} == {
        "breadth_participation_score",
        "trend_fragility_score",
    }
    assert all(
        row["distribution_status"]
        == "NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING"
        for row in rows
    )
    assert all(row["sample_count"] == 0 for row in rows)


def test_current_constituents_proxy_diagnostics_fails_closed_on_unsafe_source(
    tmp_path: Path,
) -> None:
    feasibility_dir = _write_trading_2302_source(
        tmp_path / "source",
        summary_overrides={"promotion_allowed": True},
    )

    with pytest.raises(CurrentConstituentsProxyDiagnosticsError):
        run_current_constituents_breadth_proxy_diagnostics(
            feasibility_dir=feasibility_dir,
            output_dir=tmp_path / "out",
        )


def test_current_constituents_proxy_diagnostics_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "current-constituents-breadth-proxy-diagnostics",
            "--feasibility-dir",
            str(_write_trading_2302_source(tmp_path / "source")),
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def _write_trading_2302_source(
    root: Path,
    *,
    summary_overrides: dict[str, object] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    summary = {
        "status": "BREADTH_FEASIBILITY_AUDIT_READY_PROXY_ONLY",
        "recommended_next_action": "TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "actual_path_validation_executed": False,
        "candidate_artifact_generated": False,
    }
    if summary_overrides:
        summary.update(summary_overrides)
    _write_json(root / "breadth_participation_feasibility_summary.json", summary)
    _write_json(
        root / "breadth_candidate_signal_concept_matrix.json",
        {
            "rows": [
                {"signal_name": "breadth_participation_score"},
                {"signal_name": "trend_fragility_score"},
            ]
        },
    )
    _write_json(root / "current_constituents_proxy_risk_matrix.json", {"rows": []})
    _write_json(
        root / "breadth_2303_task_route.json",
        {
            "next_task": "TRADING-2303_Current_Constituents_Proxy_Diagnostics_Only",
            "promotion_allowed": False,
            "broker_action": "none",
        },
    )
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
