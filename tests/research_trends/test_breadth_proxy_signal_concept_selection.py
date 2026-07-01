from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.breadth_proxy_signal_concept_selection import (
    BreadthProxySignalConceptSelectionError,
    build_breadth_proxy_signal_concept_selection_artifacts,
    run_breadth_proxy_signal_concept_selection,
)
from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app


def test_breadth_proxy_signal_concept_selection_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "breadth-proxy-signal-concept-selection" in result.output


def test_breadth_proxy_signal_concept_selection_writes_no_selection_outputs(
    tmp_path: Path,
) -> None:
    diagnostics_dir = _write_trading_2303_source(tmp_path / "source")
    output_dir = tmp_path / "out"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "breadth-proxy-signal-concept-selection",
            "--diagnostics-dir",
            str(diagnostics_dir),
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required_outputs = [
        "breadth_proxy_signal_selection_summary.json",
        "breadth_signal_concept_scorecard.json",
        "breadth_signal_concept_scorecard.csv",
        "selected_breadth_signal_concepts.json",
        "rejected_breadth_signal_concepts.json",
        "breadth_proxy_signal_selection_safety_boundary.json",
    ]
    for filename in required_outputs:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "breadth_proxy_signal_selection_report.md").exists()

    summary = json.loads(
        (output_dir / "breadth_proxy_signal_selection_summary.json").read_text(
            encoding="utf-8"
        )
    )
    selected = json.loads(
        (output_dir / "selected_breadth_signal_concepts.json").read_text(encoding="utf-8")
    )
    rejected = json.loads(
        (output_dir / "rejected_breadth_signal_concepts.json").read_text(encoding="utf-8")
    )
    assert summary["status"] == "BREADTH_PROXY_SIGNAL_SELECTION_SOURCE_BLOCKED_NO_SELECTION"
    assert summary["selected_concept_count"] == 0
    assert summary["advance_to_generator_allowed"] is False
    assert selected["rows"] == []
    assert selected["selected_concept_count"] == 0
    assert rejected["rejected_concept_count"] == 2
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["candidate_artifact_generated"] is False
    assert summary["candidate_signal_series_generated"] is False
    assert summary["actual_path_validation_executed"] is False


def test_breadth_proxy_signal_concept_selection_scorecard_marks_criteria_unavailable(
    tmp_path: Path,
) -> None:
    artifacts = build_breadth_proxy_signal_concept_selection_artifacts(
        diagnostics_dir=_write_trading_2303_source(tmp_path / "source"),
        generated_at=datetime(2026, 7, 1, tzinfo=UTC),
    )

    rows = artifacts["scorecard"]["rows"]
    assert {row["signal_name"] for row in rows} == {
        "breadth_participation_score",
        "trend_fragility_score",
    }
    assert all(row["selection_ready"] is False for row in rows)
    assert all(row["selected_for_poc"] is False for row in rows)
    assert all(row["eligible_for_trading_2305"] is False for row in rows)
    assert all(
        row["distribution_discrimination_status"] == "NOT_EVALUATED_SOURCE_BLOCKED"
        for row in rows
    )
    assert all(row["bias_acceptability_status"] == "NOT_EVALUATED_SOURCE_BLOCKED" for row in rows)


def test_breadth_proxy_signal_concept_selection_fails_closed_on_unsafe_source(
    tmp_path: Path,
) -> None:
    diagnostics_dir = _write_trading_2303_source(
        tmp_path / "source",
        summary_overrides={"promotion_allowed": True},
    )

    with pytest.raises(BreadthProxySignalConceptSelectionError):
        run_breadth_proxy_signal_concept_selection(
            diagnostics_dir=diagnostics_dir,
            output_dir=tmp_path / "out",
        )


def test_breadth_proxy_signal_concept_selection_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "breadth-proxy-signal-concept-selection",
            "--diagnostics-dir",
            str(_write_trading_2303_source(tmp_path / "source")),
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def _write_trading_2303_source(
    root: Path,
    *,
    summary_overrides: dict[str, object] | None = None,
) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    safety = {
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "actual_path_validation_executed": False,
        "candidate_artifact_generated": False,
        "candidate_signal_series_generated": False,
    }
    summary = {
        **safety,
        "status": "CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED",
        "task_id": "TRADING-2303_CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_ONLY",
        "signal_distribution_computed": False,
        "candidate_generation_allowed_now": False,
        "recommended_next_action": "REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT",
        "summary": {
            "source_snapshot_status": "ALL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_MISSING"
        },
    }
    if summary_overrides:
        summary.update(summary_overrides)
    _write_json(root / "breadth_proxy_diagnostics_summary.json", summary)
    _write_json(
        root / "breadth_proxy_signal_distribution_matrix.json",
        {
            **safety,
            "rows": [
                {
                    "signal_name": "breadth_participation_score",
                    "distribution_status": "NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING",
                    "diagnostics_grade": "source_blocked",
                },
                {
                    "signal_name": "trend_fragility_score",
                    "distribution_status": "NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING",
                    "diagnostics_grade": "source_blocked",
                },
            ],
        },
    )
    _write_json(root / "breadth_proxy_asset_horizon_drilldown.json", {**safety, "rows": []})
    _write_json(
        root / "breadth_proxy_bias_warning_report.json",
        {
            **safety,
            "warning_status": "CURRENT_CONSTITUENTS_PROXY_HIGH_BIAS_SOURCE_BLOCKED",
        },
    )
    _write_json(
        root / "breadth_proxy_next_step_recommendation.json",
        {
            **safety,
            "recommended_next_task": "TRADING-2304_BREADTH_PROXY_SIGNAL_CONCEPT_SELECTION",
            "recommendation_status": "REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT",
            "do_not_advance_to_generator": True,
        },
    )
    _write_json(
        root / "breadth_proxy_safety_boundary.json",
        {**safety, "boundary_status": "PROMOTION_PAPER_PRODUCTION_BROKER_BLOCKED"},
    )
    return root


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
