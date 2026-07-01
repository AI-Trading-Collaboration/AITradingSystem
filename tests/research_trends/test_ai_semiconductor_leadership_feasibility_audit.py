from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.ai_semiconductor_leadership_feasibility_audit import (
    build_ai_leadership_candidate_design_sketch,
    build_ai_leadership_input_inventory,
    build_ai_leadership_validation_route,
)
from ai_trading_system.cli import app
from ai_trading_system.cli_commands.research_trends import trends_app
from ai_trading_system.yaml_loader import safe_load_yaml_path


def test_ai_semiconductor_leadership_feasibility_cli_is_registered() -> None:
    result = CliRunner().invoke(trends_app, ["--help"])

    assert result.exit_code == 0
    assert "ai-semiconductor-leadership-feasibility-audit" in result.output


def test_ai_leadership_input_inventory_covers_owner_inputs_and_safety() -> None:
    rows = build_ai_leadership_input_inventory()
    by_id = {row["input_id"]: row for row in rows}

    assert "smh_vs_qqq_relative_strength" in by_id
    assert "nvda_vs_smh_leadership" in by_id
    assert "semiconductor_peer_relative_strength" in by_id
    assert "ai_core_basket_vs_qqq" in by_id
    assert "semiconductor_basket_breadth" in by_id
    assert "mega_cap_ai_leadership_concentration" in by_id
    assert all(row["promotion_allowed"] is False for row in rows)
    assert all(row["paper_shadow_allowed"] is False for row in rows)
    assert all(row["production_allowed"] is False for row in rows)
    assert all(row["broker_action"] == "none" for row in rows)
    assert by_id["smh_vs_qqq_relative_strength"]["source_status"] == (
        "CACHE_VALIDATION_REQUIRED_BEFORE_USE"
    )
    assert by_id["ai_core_basket_vs_qqq"]["source_status"] == (
        "BASKET_POLICY_REQUIRED_BEFORE_USE"
    )


def test_ai_leadership_design_sketch_has_expected_candidate_ids() -> None:
    sketch = build_ai_leadership_candidate_design_sketch(
        target_assets=["QQQ", "SMH"],
        horizons=["5d", "10d", "20d"],
    )

    assert sketch["recommended_next_task"] == (
        "TRADING-2308_AI_SEMICONDUCTOR_LEADERSHIP_GENERATOR_POC"
    )
    assert set(sketch["candidate_ids"]) == {
        "ai_semiconductor_leadership_quality_v1",
        "smh_relative_strength_leadership_v1",
        "ai_core_basket_leadership_v1",
    }
    assert "generic_risk_appetite_reopen" in sketch["not_recommended_as"]
    assert sketch["candidate_artifact_generated"] is False


def test_ai_leadership_validation_route_blocks_validation_and_promotion() -> None:
    rows = build_ai_leadership_validation_route()

    assert {row["candidate_id"] for row in rows} >= {
        "smh_relative_strength_leadership_v1",
        "ai_semiconductor_leadership_quality_v1",
        "ai_core_basket_leadership_v1",
    }
    assert all(row["actual_path_validation_ready"] is False for row in rows)
    assert all(row["promotion_eligible"] is False for row in rows)
    assert all(row["candidate_signal_series_generated"] is False for row in rows)
    assert any(
        row["readiness_status"] == "SOURCE_AUDIT_REQUIRED"
        for row in rows
        if row["candidate_id"] == "mega_cap_ai_concentration_warning_v1"
    )


def test_ai_semiconductor_leadership_feasibility_cli_writes_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "ai_leadership"
    docs_root = tmp_path / "docs"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-semiconductor-leadership-feasibility-audit",
            "--output-dir",
            str(output_dir),
            "--docs-root",
            str(docs_root),
        ],
    )

    assert result.exit_code == 0, result.output
    required = [
        "ai_semiconductor_leadership_feasibility_summary.json",
        "ai_leadership_input_inventory.json",
        "ai_leadership_input_inventory.csv",
        "ai_leadership_candidate_design_sketch.json",
        "ai_leadership_validation_route.json",
        "ai_leadership_validation_route.csv",
        "ai_leadership_safety_boundary.json",
    ]
    for filename in required:
        assert (output_dir / filename).exists(), filename
    assert (docs_root / "ai_semiconductor_leadership_feasibility_audit.md").exists()

    summary = json.loads(
        (output_dir / "ai_semiconductor_leadership_feasibility_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["status"] == (
        "AI_SEMICONDUCTOR_LEADERSHIP_FEASIBILITY_AUDIT_READY_PRICE_PROXY_ONLY"
    )
    assert summary["data_quality_status"] == "NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT"
    assert summary["generator_poc_ready_now"] is False
    assert summary["candidate_artifact_generated"] is False
    assert summary["actual_path_validation_executed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"


def test_ai_semiconductor_leadership_feasibility_rejects_wrong_mode(
    tmp_path: Path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "research",
            "trends",
            "ai-semiconductor-leadership-feasibility-audit",
            "--output-dir",
            str(tmp_path / "out"),
            "--mode",
            "promotion",
        ],
    )

    assert result.exit_code != 0


def test_ai_semiconductor_leadership_registry_and_catalog_are_non_promotion() -> None:
    registry = safe_load_yaml_path(Path("config/report_registry.yaml"))
    entry = next(
        report
        for report in registry["reports"]
        if report["report_id"] == "ai_semiconductor_leadership_feasibility_audit"
    )

    assert entry["command"] == (
        "aits research trends ai-semiconductor-leadership-feasibility-audit"
    )
    assert entry["artifact_role"] == "ai_semiconductor_leadership_feasibility_audit"
    assert entry["data_quality_status"] == "NOT_APPLICABLE_STATIC_FEASIBILITY_AUDIT"
    assert entry["promotion_eligible"] is False
    assert entry["paper_shadow_allowed"] is False
    assert entry["production_allowed"] is False
    assert entry["generator_implemented"] is False
    assert entry["actual_path_validation_executed"] is False
    assert entry["candidate_artifact_generated"] is False
    assert entry["candidate_signal_series_generated"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    assert "ai_semiconductor_leadership_feasibility_audit" in catalog
    assert "TRADING-2308 generator POC" in catalog
