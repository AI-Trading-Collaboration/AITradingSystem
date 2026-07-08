from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_defensive_limited_adjustment_component_validation as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_defensive_limited_adjustment_component_validation as validation,
)


def test_defensive_limited_adjustment_component_validation_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == validation.READY_STATUS
    assert payload["source_2433_ready"] is True
    assert payload["source_candidate_found"] is True
    assert payload["component_validation_ready"] is True
    assert payload["component_value_assessment_ready"] is True
    assert payload["primary_value_matrix_ready"] is True
    assert payload["validation_boundary_ready"] is True
    assert payload["component_value_found"] is True
    assert payload["candidate_status"] == "component_value"
    assert payload["primary_value"] == list(validation.PRIMARY_VALUE_IDS)
    assert payload["promotion_candidate_found"] is False
    assert payload["promotion_candidate_count"] == 0
    assert payload["computed_new_metrics"] is False
    assert payload["market_data_component_validation_run"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == validation.NEXT_ROUTE


def test_defensive_limited_adjustment_blocks_when_2433_not_ready() -> None:
    sources = _source_documents()
    sources["source_2433_batch_screen"]["batch_screen_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == validation.BLOCKED_STATUS
    assert payload["source_2433_ready"] is False
    assert payload["component_value_found"] is False
    assert "source_2433_batch_screen_ready" in payload["evidence_gap_ids"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == validation.BLOCKED_ROUTE


def test_defensive_limited_adjustment_does_not_resolve_non_component_value() -> None:
    sources = _source_documents()
    sources["source_2433_batch_screen"]["candidate_screen_matrix"]["candidates"][0][
        "batch_decision"
    ] = "pit_candidate"

    payload = _build_payload(sources)

    assert payload["status"] == validation.BLOCKED_STATUS
    assert payload["component_value_found"] is False
    assert payload["candidate_status"] == "needs_pit"
    assert payload["promotion_candidate_found"] is False
    assert "component_value_prior_evidence" in payload["evidence_gap_ids"]
    assert payload["recommended_next_research_task"] == validation.BLOCKED_ROUTE


def test_defensive_limited_adjustment_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "component_validation"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_defensive_limited_adjustment_component_validation(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == validation.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["component_value_found"] is True
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "component_value_assessment_json",
        "primary_value_matrix_json",
        "validation_boundary_json",
        "markdown_path",
        "component_value_assessment_markdown",
        "primary_value_matrix_markdown",
        "validation_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_defensive_limited_adjustment_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "component_validation_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-defensive-limited-adjustment-component-validation",
            *_source_args(paths),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "260"},
        terminal_width=260,
    )

    assert result.exit_code == 0, result.output
    assert validation.READY_STATUS in result.output
    assert "component_value_found=true" in result.output
    assert "candidate_status=component_value" in result.output
    assert "promotion_candidate_count=0" in result.output
    assert "market_data_component_validation_run=false" in result.output
    assert "computed_new_metrics=false" in result.output
    assert "fresh_market_data_read=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "primary_value=drawdown_control,false_risk_off_reduction" in result.output
    assert f"next_route={validation.NEXT_ROUTE}" in result.output
    assert (output_root / "component_validation_result.json").exists()


def test_defensive_limited_adjustment_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2433"].unlink()

    payload = impl.run_growth_tilt_defensive_limited_adjustment_component_validation(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == validation.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["component_validation_ready"] is False
    assert payload["evidence_gap_ids"] == ["source_artifact_availability"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == validation.BLOCKED_ROUTE


def test_defensive_limited_adjustment_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[validation.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-defensive-limited-adjustment-component-validation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("component_validation_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2435_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert validation.REPORT_TYPE in catalog
    assert "growth-tilt-defensive-limited-adjustment-component-validation" in system_flow
    assert validation.READY_STATUS in system_flow
    assert validation.NEXT_ROUTE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return validation.build_growth_tilt_defensive_limited_adjustment_component_validation(
        sources["source_2433_batch_screen"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2433_batch_screen": _source_2433(),
        "report_registry": {
            "reports": [
                {"report_id": report_id} for report_id in validation.REQUIRED_REPORT_IDS
            ]
        },
        "artifact_catalog_text": "\n".join(validation.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(validation.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "batch_screen": "defensive_limited_adjustment component value",
            "candidate_screen": "defensive_limited_adjustment candidate screen",
        },
    }


def _source_2433() -> dict[str, Any]:
    return {
        "status": validation.EXPECTED_2433_STATUS,
        "batch_screen_ready": True,
        "promotion_candidate_found": False,
        "recommended_next_research_task": validation.EXPECTED_2433_NEXT_ROUTE,
        "candidate_screen_matrix": {
            "candidates": [
                {
                    "candidate_id": validation.CANDIDATE_ID,
                    "batch_decision": "component_value",
                    "research_questions": [
                        "reduce_over_defensive_behavior",
                        "reduce_whipsaw",
                        "reduce_missed_recovery",
                        "preserve_drawdown_protection",
                    ],
                }
            ]
        },
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2433": root / "batch_screen_result.json",
        "batch_screen_doc": root / "batch_screen.md",
        "candidate_screen_matrix_doc": root / "candidate_screen_matrix.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    _write_json(paths["source_2433"], sources["source_2433_batch_screen"])
    paths["batch_screen_doc"].write_text(
        "defensive_limited_adjustment component value",
        encoding="utf-8",
    )
    paths["candidate_screen_matrix_doc"].write_text(
        "defensive_limited_adjustment candidate screen",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}"
            for report_id in validation.REQUIRED_REPORT_IDS
        )
        + "\n",
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        sources["artifact_catalog_text"],
        encoding="utf-8",
    )
    paths["system_flow"].write_text(sources["system_flow_text"], encoding="utf-8")
    return paths


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2433_batch_screen_path": paths["source_2433"],
        "batch_screen_doc_path": paths["batch_screen_doc"],
        "candidate_screen_matrix_doc_path": paths["candidate_screen_matrix_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2433-batch-screen",
        str(paths["source_2433"]),
        "--batch-screen-doc",
        str(paths["batch_screen_doc"]),
        "--candidate-screen-matrix-doc",
        str(paths["candidate_screen_matrix_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
