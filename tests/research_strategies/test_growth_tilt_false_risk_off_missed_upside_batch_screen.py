from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_false_risk_off_missed_upside_batch_screen as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_false_risk_off_missed_upside_batch_screen as screen,
)


def test_false_risk_off_batch_screen_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == screen.READY_STATUS
    assert payload["source_2432_ready"] is True
    assert payload["candidate_set_ready"] is True
    assert payload["candidate_set_id"] == screen.CANDIDATE_SET_ID
    assert payload["batch_screen_ready"] is True
    assert payload["candidate_count"] == 6
    assert payload["candidates_screened"] == 6
    assert payload["rejected_count"] == 0
    assert payload["component_value_count"] == 3
    assert payload["pit_candidate_count"] == 3
    assert payload["promotion_candidate_count"] == 0
    assert payload["promotion_candidate_found"] is False
    assert payload["research_question_covered_count"] == 4
    assert payload["candidate_batch_screen_run"] is True
    assert payload["market_data_candidate_screen_run"] is False
    assert payload["computed_new_metrics"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == screen.NEXT_ROUTE


def test_false_risk_off_batch_screen_blocks_when_2432_not_ready() -> None:
    sources = _source_documents()
    sources["source_2432_candidate_gauntlet_harness"]["harness_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == screen.BLOCKED_STATUS
    assert payload["source_2432_ready"] is False
    assert "source_2432_candidate_gauntlet_harness_ready" in payload[
        "screen_contract_gap_ids"
    ]
    assert payload["recommended_next_research_task"] == screen.BLOCKED_ROUTE


def test_false_risk_off_batch_screen_blocks_promotion_candidate_default() -> None:
    sources = _source_documents()
    sources["candidate_set"]["candidates"][0]["default_batch_decision"] = (
        "promotion_candidate"
    )

    payload = _build_payload(sources)

    assert payload["status"] == screen.BLOCKED_STATUS
    assert payload["promotion_candidate_count"] == 1
    assert "promotion_candidate_not_allowed_by_default" in payload[
        "screen_contract_gap_ids"
    ]


def test_false_risk_off_batch_screen_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "false_risk_off"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_false_risk_off_missed_upside_batch_screen(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == screen.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["candidate_batch_screen_run"] is True
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "candidate_screen_matrix_json",
        "batch_decision_summary_json",
        "research_question_coverage_json",
        "no_effect_boundary_json",
        "markdown_path",
        "candidate_screen_matrix_markdown",
        "batch_decision_summary_markdown",
        "research_question_coverage_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_false_risk_off_batch_screen_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "false_risk_off_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-false-risk-off-missed-upside-batch-screen",
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
    assert screen.READY_STATUS in result.output
    assert "candidate_set_id=false_risk_off_missed_upside_2433" in result.output
    assert "candidate_count=6" in result.output
    assert "component_value_count=3" in result.output
    assert "pit_candidate_count=3" in result.output
    assert "promotion_candidate_count=0" in result.output
    assert "research_question_covered_count=4" in result.output
    assert "candidate_batch_screen_run=true" in result.output
    assert "market_data_candidate_screen_run=false" in result.output
    assert "computed_new_metrics=false" in result.output
    assert "fresh_market_data_read=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={screen.NEXT_ROUTE}" in result.output
    assert (output_root / "batch_screen_result.json").exists()


def test_false_risk_off_batch_screen_missing_candidate_set_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["candidate_set"].unlink()

    payload = impl.run_growth_tilt_false_risk_off_missed_upside_batch_screen(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == screen.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["batch_screen_ready"] is False
    assert payload["screen_contract_gap_count"] == 1
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == screen.BLOCKED_ROUTE


def test_false_risk_off_batch_screen_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[screen.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-false-risk-off-missed-upside-batch-screen"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "false_risk_off_missed_upside_2433.yaml" in item
        for item in entry["artifact_globs"]
    )
    assert any("batch_screen_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2434_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    candidate_set_config = Path(
        "research/configs/growth_tilt/false_risk_off_missed_upside_2433.yaml"
    ).read_text(encoding="utf-8")
    assert screen.REPORT_TYPE in catalog
    assert "growth-tilt-false-risk-off-missed-upside-batch-screen" in system_flow
    assert screen.CANDIDATE_SET_ID in system_flow
    assert screen.READY_STATUS in system_flow
    assert screen.NEXT_ROUTE in system_flow
    assert "threshold_value: null" in candidate_set_config
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return screen.build_growth_tilt_false_risk_off_missed_upside_batch_screen(
        sources["source_2432_candidate_gauntlet_harness"],
        sources["candidate_set"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2432_candidate_gauntlet_harness": _source_2432(),
        "candidate_set": _candidate_set(),
        "report_registry": {
            "reports": [{"report_id": report_id} for report_id in screen.REQUIRED_REPORT_IDS]
        },
        "artifact_catalog_text": "\n".join(screen.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(screen.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {"harness": "growth_tilt_candidate_gauntlet_harness"},
    }


def _source_2432() -> dict[str, Any]:
    return {
        "status": screen.EXPECTED_2432_STATUS,
        "harness_ready": True,
        "candidate_set_id": screen.EXPECTED_2432_CANDIDATE_SET_ID,
        "candidates_tested": 0,
        "candidate_gauntlet_run": False,
        "recommended_next_research_task": screen.EXPECTED_2432_NEXT_ROUTE,
    }


def _candidate_set() -> dict[str, Any]:
    decisions = [
        "component_value",
        "pit_candidate",
        "pit_candidate",
        "pit_candidate",
        "component_value",
        "component_value",
    ]
    question_sets = [
        ["over_defensive_entry", "false_defensive_day_reduction"],
        ["slow_growth_recovery_reentry", "missed_upside_without_drawdown_damage"],
        ["over_defensive_entry", "false_defensive_day_reduction"],
        ["slow_growth_recovery_reentry", "missed_upside_without_drawdown_damage"],
        ["false_defensive_day_reduction", "missed_upside_without_drawdown_damage"],
        ["over_defensive_entry", "false_defensive_day_reduction"],
    ]
    return {
        "candidate_set_id": screen.CANDIDATE_SET_ID,
        "status": "ready",
        "safety_boundary": {
            "research_only": True,
            "market_data_screen_allowed_in_2433": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "trading_advice_allowed": False,
        },
        "candidates": [
            {
                "candidate_id": f"candidate_{index}",
                "candidate_family": "false_risk_off",
                "research_questions": question_sets[index],
                "default_batch_decision": decisions[index],
                "decision_rationale": "configured prior-evidence triage",
                "next_validation_route": screen.NEXT_ROUTE,
                "threshold_value": None,
            }
            for index in range(6)
        ],
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2432": root / "candidate_gauntlet_result.json",
        "candidate_set": root / "false_risk_off_missed_upside_2433.yaml",
        "candidate_gauntlet_harness_doc": root / "candidate_gauntlet_harness.md",
        "candidate_set_2432_doc": root / "candidate_set_2432.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    _write_json(paths["source_2432"], sources["source_2432_candidate_gauntlet_harness"])
    paths["candidate_set"].write_text(
        json.dumps(sources["candidate_set"], ensure_ascii=False),
        encoding="utf-8",
    )
    paths["candidate_gauntlet_harness_doc"].write_text(
        "growth_tilt_candidate_gauntlet_harness",
        encoding="utf-8",
    )
    paths["candidate_set_2432_doc"].write_text(
        "growth_tilt_candidate_gauntlet_harness",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in screen.REQUIRED_REPORT_IDS
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
        "source_2432_candidate_gauntlet_harness_path": paths["source_2432"],
        "candidate_set_path": paths["candidate_set"],
        "candidate_gauntlet_harness_doc_path": paths["candidate_gauntlet_harness_doc"],
        "candidate_set_2432_doc_path": paths["candidate_set_2432_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2432-candidate-gauntlet-harness",
        str(paths["source_2432"]),
        "--candidate-set",
        str(paths["candidate_set"]),
        "--candidate-gauntlet-harness-doc",
        str(paths["candidate_gauntlet_harness_doc"]),
        "--candidate-set-2432-doc",
        str(paths["candidate_set_2432_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
