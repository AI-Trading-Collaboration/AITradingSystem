from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_pit_coverage_matrix_reusable_implementation as impl
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_pit_coverage_matrix_reusable_implementation_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "pit_reusable"
    rq_output_root = tmp_path / "outputs" / "research_quality" / "pit_coverage_matrix"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_dynamic_strategy_pit_coverage_matrix_reusable_implementation(
        **_source_kwargs(source_paths),
        output_root=output_root,
        research_quality_output_root=rq_output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["pit_input_registry_created"] is True
    assert payload["pit_input_registry_ready"] is True
    assert payload["pit_coverage_matrix_generator_ready"] is True
    assert payload["pit_gate_checker_ready"] is True
    assert payload["pit_blocker_summary_ready"] is True
    assert payload["pit_remediation_matrix_ready"] is True
    assert payload["pit_coverage_matrix_rows"] == 17
    assert payload["blocking_gaps"] == list(impl.BLOCKING_GAP_INPUTS)
    assert payload["candidate_search_allowed"] is False
    assert payload["research_only_observation_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["candidate_search_resumed"] is False
    assert payload["research_only_observation_approved"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["daily_report_generated"] is False
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert "policy-derived safety gate" in payload["gate_policy_note"]

    gate = payload["pit_gate_result"]
    assert gate["blockers"] == [
        "BLOCKING_GAP_GROWTH_TILT_ENGINE",
        "BLOCKING_GAP_VALID_UNTIL_WINDOW",
    ]
    assert gate["candidate_search_allowed"] is False
    assert gate["research_only_observation_allowed"] is False
    assert gate["paper_shadow_allowed"] is False
    assert gate["production_allowed"] is False

    routes = payload["pit_remediation_routes"]["routes"]
    assert routes["growth_tilt_engine"]["next_task"] == impl.NEXT_ROUTE
    assert routes["growth_tilt_engine"]["candidate_search_blocker"] is True
    assert routes["valid_until_window"]["next_task"] == (
        "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_"
        "Remediation_Plan"
    )
    assert routes["regime_expectation_scoring"]["next_task"] == (
        "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan"
    )
    assert routes["threshold_meta_dataset"]["next_task"] == (
        "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan"
    )

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "pit_input_registry_snapshot_json",
        "pit_coverage_matrix_json",
        "pit_gate_result_json",
        "pit_blocker_summary_json",
        "pit_remediation_routes_json",
        "research_quality_pit_coverage_matrix_json",
        "research_quality_pit_gate_result_json",
        "research_quality_pit_blocker_summary_json",
        "research_quality_pit_remediation_matrix_json",
        "markdown_path",
        "pit_input_registry_markdown",
        "pit_gate_result_markdown",
        "pit_remediation_routes_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_pit_coverage_matrix_reusable_implementation_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "pit_reusable_cli"
    rq_output_root = tmp_path / "outputs" / "research_quality" / "pit_coverage_matrix"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-pit-coverage-matrix-generate",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-07",
            "--output-root",
            str(output_root),
            "--research-quality-output-root",
            str(rq_output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert impl.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "implementation_result.json").exists()
    assert (output_root / "pit_input_registry_snapshot.json").exists()
    assert (output_root / "pit_coverage_matrix.json").exists()
    assert (output_root / "pit_gate_result.json").exists()
    assert (output_root / "pit_blocker_summary.json").exists()
    assert (output_root / "pit_remediation_routes.json").exists()
    assert (rq_output_root / "dynamic_strategy_pit_coverage_matrix.json").exists()


def test_dynamic_strategy_pit_coverage_matrix_reusable_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_pit_coverage_matrix_reusable_implementation"]

    assert entry["command"] == (
        "aits research strategies dynamic-strategy-pit-coverage-matrix-generate"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("implementation_result.json" in item for item in entry["artifact_globs"])
    assert any(
        "dynamic_strategy_pit_coverage_matrix.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2406_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_pit_coverage_matrix_reusable_implementation" in catalog
    assert "dynamic-strategy-pit-coverage-matrix-generate" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "implementation_2404": root / "implementation_2404.json",
        "registry_schema_2404": root / "registry_schema_2404.json",
        "gate_policy_2404": root / "gate_policy_2404.json",
        "blocker_summary_2404": root / "blocker_summary_2404.json",
        "pit_matrix_2403": root / "pit_matrix_2403.json",
        "remediation_matrix_2403": root / "remediation_matrix_2403.json",
    }
    _write_json(
        paths["implementation_2404"],
        {
            **_safe_doc(impl.m2404.READY_STATUS),
            "recommended_next_research_task": impl.m2404.NEXT_ROUTE,
            "blocking_gaps": list(impl.BLOCKING_GAP_INPUTS),
            "pit_input_registry_schema_ready": True,
            "pit_gate_policy_ready": True,
            "remediation_routes_ready": True,
            "current_blocker_summary_ready": True,
        },
    )
    for key, payload_key in (
        ("registry_schema_2404", "pit_input_registry_schema"),
        ("gate_policy_2404", "pit_gate_policy"),
        ("blocker_summary_2404", "current_blocker_summary"),
    ):
        _write_json(
            paths[key],
            {**_safe_doc(impl.m2404.READY_STATUS), payload_key: {"record_ready": True}},
        )
    _write_json(
        paths["pit_matrix_2403"],
        {**_safe_doc(impl.m2403.READY_STATUS), "pit_coverage_matrix": _pit_rows()},
    )
    _write_json(
        paths["remediation_matrix_2403"],
        {
            **_safe_doc(impl.m2403.READY_STATUS),
            "prioritized_remediation_matrix": _remediation_rows(),
        },
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2404_implementation_path": paths["implementation_2404"],
        "source_2404_registry_schema_path": paths["registry_schema_2404"],
        "source_2404_gate_policy_path": paths["gate_policy_2404"],
        "source_2404_blocker_summary_path": paths["blocker_summary_2404"],
        "source_2403_pit_matrix_path": paths["pit_matrix_2403"],
        "source_2403_remediation_matrix_path": paths["remediation_matrix_2403"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "implementation_2404": "--source-2404-implementation",
        "registry_schema_2404": "--source-2404-registry-schema",
        "gate_policy_2404": "--source-2404-gate-policy",
        "blocker_summary_2404": "--source-2404-blocker-summary",
        "pit_matrix_2403": "--source-2403-pit-matrix",
        "remediation_matrix_2403": "--source-2403-remediation-matrix",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _pit_rows() -> list[dict[str, object]]:
    return [
        {
            "input_id": "growth_tilt_engine",
            "severity": "BLOCKING",
            "point_in_time_status": "UNKNOWN",
        },
        {
            "input_id": "valid_until_window",
            "severity": "BLOCKING",
            "point_in_time_status": "UNKNOWN",
        },
    ]


def _remediation_rows() -> list[dict[str, object]]:
    return [
        {
            "remediation_id": "2403-SIGNAL-01",
            "input_id": "growth_tilt_engine",
            "severity": "BLOCKING",
        },
        {
            "remediation_id": "2403-VALIDUNTIL-01",
            "input_id": "valid_until_window",
            "severity": "BLOCKING",
        },
    ]


def _safe_doc(status: str) -> dict[str, object]:
    return {
        "status": status,
        **{field: False for field in impl.SAFETY_FALSE_FIELDS},
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
