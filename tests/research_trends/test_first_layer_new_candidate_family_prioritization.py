from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.first_layer_new_candidate_family_prioritization import (
    build_candidate_family_score_matrix,
    build_candidate_family_task_backlog,
    build_safety_boundary,
    build_standard_validation_path,
)


def test_new_candidate_family_prioritization_score_order_and_safety() -> None:
    rows = build_candidate_family_score_matrix()

    assert rows[0]["family_id"] == "volatility_risk_cap_forward_observe"
    assert rows[1]["family_id"] == "breadth_participation"
    assert rows[2]["family_id"] == "ai_semiconductor_leadership"
    assert rows[3]["family_id"] == "liquidity_rates_pressure"
    assert rows[1]["score"] > rows[2]["score"] > rows[3]["score"]
    assert all(row["promotion_allowed"] is False for row in rows)
    assert all(row["paper_shadow_allowed"] is False for row in rows)
    assert all(row["production_allowed"] is False for row in rows)
    assert all(row["broker_action"] == "none" for row in rows)


def test_new_candidate_family_task_backlog_maps_legacy_aliases_to_legal_ids() -> None:
    rows = build_candidate_family_task_backlog()
    by_family = {row["family_id"]: row for row in rows}

    breadth = by_family["breadth_participation"]
    assert breadth["task_id"].startswith("TRADING-2302_")
    assert breadth["legacy_alias"] == "TRADING-2294B"
    assert "2294B" not in breadth["task_id"]

    execution = by_family["execution_cooldown_decay_cap_mechanics"]
    assert execution["task_id"].startswith("TRADING-2321_")
    assert execution["broker_action"] == "none"

    leadership = by_family["ai_semiconductor_leadership"]
    assert leadership["task_id"].startswith("TRADING-2307_")

    rates = by_family["liquidity_rates_pressure"]
    assert rates["task_id"].startswith("TRADING-2311_")


def test_new_candidate_family_validation_path_and_boundary_are_non_runtime() -> None:
    path = build_standard_validation_path()
    boundary = build_safety_boundary()

    assert path["standard_path"][0] == "candidate_family_spec"
    assert "actual_path_validation" in path["standard_path"]
    assert boundary["does_not_generate_candidate_bound_artifacts"] is True
    assert boundary["does_not_run_actual_path_validation"] is True
    assert boundary["forward_observe_runtime_started"] is False
    assert boundary["candidate_generation_allowed"] is False


def test_new_candidate_family_prioritization_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "first-layer-new-candidate-family-prioritization" in result.output


def test_new_candidate_family_prioritization_cli_writes_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "candidate_family_prioritization"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "first-layer-new-candidate-family-prioritization",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "first_layer_new_candidate_family_prioritization_summary.json",
        "candidate_family_score_matrix.json",
        "candidate_family_score_matrix.csv",
        "candidate_family_data_feasibility_matrix.json",
        "candidate_family_task_backlog.json",
        "candidate_family_standard_validation_path.json",
        "deferred_current_form_matrix.json",
        "candidate_family_safety_boundary.json",
        "candidate_family_owner_review_note.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename

    assert (docs_root / "first_layer_new_candidate_family_prioritization.md").exists()
    assert (docs_root / "first_layer_new_candidate_family_task_backlog.md").exists()
    summary = json.loads(
        (output_dir / "first_layer_new_candidate_family_prioritization_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["next_new_family_task"].startswith("TRADING-2302_")
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert summary["actual_path_validation_executed"] is False


def test_new_candidate_family_prioritization_cli_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "first-layer-new-candidate-family-prioritization",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0
