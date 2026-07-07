from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_pit_coverage_matrix_implementation_plan as plan
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_pit_coverage_matrix_implementation_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "pit_impl"
    docs_root = tmp_path / "docs" / "research"

    payload = plan.run_dynamic_strategy_pit_coverage_matrix_implementation_plan(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == plan.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == plan.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["pit_matrix_implementation_plan_ready"] is True
    assert payload["pit_input_registry_schema_ready"] is True
    assert payload["pit_gate_policy_ready"] is True
    assert payload["remediation_routes_ready"] is True
    assert payload["current_blocker_summary_ready"] is True
    assert payload["blocking_gaps"] == list(plan.BLOCKING_GAP_INPUTS)
    assert payload["candidate_search_allowed"] is False
    assert payload["research_only_observation_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["candidate_search_resumed"] is False
    assert payload["recommended_next_research_task"] == plan.NEXT_ROUTE
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    blocker_summary = payload["current_blocker_summary"]
    assert blocker_summary["candidate_search_allowed"] is False
    assert blocker_summary["research_only_observation_allowed"] is False
    assert blocker_summary["paper_shadow_allowed"] is False
    assert blocker_summary["production_allowed"] is False
    assert blocker_summary["reason"] == [
        "BLOCKING_GAP_GROWTH_TILT_ENGINE",
        "BLOCKING_GAP_VALID_UNTIL_WINDOW",
    ]
    for input_id in plan.BLOCKING_GAP_INPUTS:
        detail = blocker_summary["blocking_gap_details"][input_id]
        assert detail["candidate_search_blocked"] is True
        assert detail["observation_blocked"] is True

    gate_result = payload["pit_gate_policy"]["current_gate_result"]
    assert gate_result["candidate_search_allowed"] is False
    assert gate_result["research_only_observation_allowed"] is False
    assert gate_result["paper_shadow_allowed"] is False
    assert gate_result["production_allowed"] is False
    assert gate_result["reason"] == blocker_summary["reason"]

    registry = payload["pit_input_registry_schema"]
    assert registry["recommended_path"] == (
        "config/research/dynamic_strategy_pit_input_registry.yaml"
    )
    registry_ids = {entry["input_id"] for entry in registry["planned_initial_entries"]}
    assert _pit_input_ids().issubset(registry_ids)
    assert "threshold_meta_dataset" in registry_ids

    routes = payload["remediation_routes"]["routes"]
    assert plan.NEXT_ROUTE in routes
    assert routes[plan.NEXT_ROUTE]["default_next_route"] is True
    assert "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan" in routes
    assert "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan" in routes
    assert "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan" in routes
    assert "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan" in routes

    for field in plan.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "pit_input_registry_schema_json",
        "pit_gate_policy_json",
        "remediation_routes_json",
        "current_blocker_summary_json",
        "markdown_path",
        "pit_input_registry_schema_markdown",
        "pit_gate_policy_markdown",
        "remediation_routes_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_pit_coverage_matrix_implementation_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "pit_impl_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-pit-coverage-matrix-implementation-plan",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-07",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert plan.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "implementation_plan_result.json").exists()
    assert (output_root / "pit_input_registry_schema.json").exists()
    assert (output_root / "pit_gate_policy.json").exists()
    assert (output_root / "remediation_routes.json").exists()
    assert (output_root / "current_blocker_summary.json").exists()


def test_dynamic_strategy_pit_coverage_matrix_implementation_plan_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_pit_coverage_matrix_implementation_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-pit-coverage-matrix-implementation-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("implementation_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("pit_input_registry_schema.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2405_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_pit_coverage_matrix_implementation_plan" in catalog
    assert "dynamic-strategy-pit-coverage-matrix-implementation-plan" in system_flow
    assert plan.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "review_2403": root / "review_2403.json",
        "pit_matrix_2403": root / "pit_matrix_2403.json",
        "signal_review_2403": root / "signal_review_2403.json",
        "regime_review_2403": root / "regime_review_2403.json",
        "remediation_matrix_2403": root / "remediation_matrix_2403.json",
        "threshold_gap_2403": root / "threshold_gap_2403.json",
        "gap_review_2402": root / "gap_review_2402.json",
        "data_quality_gap_matrix_2402": root / "data_quality_gap_matrix_2402.json",
        "pit_gap_review_2402": root / "pit_gap_review_2402.json",
        "signal_gap_review_2402": root / "signal_gap_review_2402.json",
        "regime_gap_review_2402": root / "regime_gap_review_2402.json",
        "threshold_gap_review_2402": root / "threshold_gap_review_2402.json",
        "plateau_decision_2401": root / "plateau_decision_2401.json",
        "next_direction_2401": root / "next_direction_2401.json",
    }
    _write_json(
        paths["review_2403"],
        {
            **_safe_doc(plan.m2403.READY_STATUS),
            "recommended_next_research_task": plan.m2403.NEXT_ROUTE,
            "candidate_search_resumed": False,
            "pit_coverage_matrix": _pit_rows(),
            "prioritized_remediation_matrix": _remediation_rows(),
        },
    )
    _write_json(
        paths["pit_matrix_2403"],
        {**_safe_doc(plan.m2403.READY_STATUS), "pit_coverage_matrix": _pit_rows()},
    )
    _write_json(
        paths["remediation_matrix_2403"],
        {
            **_safe_doc(plan.m2403.READY_STATUS),
            "prioritized_remediation_matrix": _remediation_rows(),
        },
    )
    for key, payload_key in (
        ("signal_review_2403", "signal_construction_review"),
        ("regime_review_2403", "regime_labeling_review"),
        ("threshold_gap_2403", "threshold_meta_dataset_gap"),
    ):
        _write_json(
            paths[key],
            {**_safe_doc(plan.m2403.READY_STATUS), payload_key: {"record_ready": True}},
        )
    _write_json(
        paths["gap_review_2402"],
        {
            **_safe_doc(plan.m2402.READY_STATUS),
            "recommended_next_research_task": plan.m2402.NEXT_ROUTE,
        },
    )
    for key, payload_key in (
        ("data_quality_gap_matrix_2402", "data_quality_gap_matrix"),
        ("pit_gap_review_2402", "pit_coverage_gap_review"),
        ("signal_gap_review_2402", "signal_quality_gap_review"),
        ("regime_gap_review_2402", "regime_labeling_gap_review"),
        ("threshold_gap_review_2402", "threshold_meta_dataset_gap_review"),
    ):
        _write_json(
            paths[key],
            {**_safe_doc(plan.m2402.READY_STATUS), payload_key: {"record_ready": True}},
        )
    _write_json(
        paths["plateau_decision_2401"],
        {
            **_safe_doc(plan.m2401.READY_STATUS),
            "owner_decision": plan.m2401.OWNER_DECISION,
        },
    )
    _write_json(
        paths["next_direction_2401"],
        {
            **_safe_doc(plan.m2401.READY_STATUS),
            "recommended_next_research_task": plan.m2402.TASK_ID,
        },
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_review_2403_path": paths["review_2403"],
        "source_pit_matrix_2403_path": paths["pit_matrix_2403"],
        "source_signal_review_2403_path": paths["signal_review_2403"],
        "source_regime_review_2403_path": paths["regime_review_2403"],
        "source_remediation_matrix_2403_path": paths["remediation_matrix_2403"],
        "source_threshold_gap_2403_path": paths["threshold_gap_2403"],
        "source_gap_review_2402_path": paths["gap_review_2402"],
        "source_data_quality_gap_matrix_2402_path": paths[
            "data_quality_gap_matrix_2402"
        ],
        "source_pit_gap_review_2402_path": paths["pit_gap_review_2402"],
        "source_signal_gap_review_2402_path": paths["signal_gap_review_2402"],
        "source_regime_gap_review_2402_path": paths["regime_gap_review_2402"],
        "source_threshold_gap_review_2402_path": paths["threshold_gap_review_2402"],
        "source_plateau_decision_2401_path": paths["plateau_decision_2401"],
        "source_next_direction_2401_path": paths["next_direction_2401"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "review_2403": "--source-review-2403",
        "pit_matrix_2403": "--source-pit-matrix-2403",
        "signal_review_2403": "--source-signal-review-2403",
        "regime_review_2403": "--source-regime-review-2403",
        "remediation_matrix_2403": "--source-remediation-matrix-2403",
        "threshold_gap_2403": "--source-threshold-gap-2403",
        "gap_review_2402": "--source-gap-review-2402",
        "data_quality_gap_matrix_2402": "--source-data-quality-gap-matrix-2402",
        "pit_gap_review_2402": "--source-pit-gap-review-2402",
        "signal_gap_review_2402": "--source-signal-gap-review-2402",
        "regime_gap_review_2402": "--source-regime-gap-review-2402",
        "threshold_gap_review_2402": "--source-threshold-gap-review-2402",
        "plateau_decision_2401": "--source-plateau-decision-2401",
        "next_direction_2401": "--source-next-direction-2401",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _pit_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for input_id, input_type, severity, pit_status in (
        ("market_prices", "market_data", "INFO", "TRUE_PIT"),
        ("adjusted_prices", "market_data", "MATERIAL", "APPROXIMATE_PIT"),
        ("volume", "market_data", "MINOR", "APPROXIMATE_PIT"),
        ("returns", "technical_features", "MATERIAL", "APPROXIMATE_PIT"),
        ("volatility_inputs", "technical_features", "MATERIAL", "APPROXIMATE_PIT"),
        ("trend_features", "technical_features", "MATERIAL", "APPROXIMATE_PIT"),
        ("drawdown_features", "technical_features", "MATERIAL", "APPROXIMATE_PIT"),
        ("growth_tilt_engine", "strategy_signals", "BLOCKING", "UNKNOWN"),
        ("lower_turnover_guardrail", "strategy_signals", "MATERIAL", "APPROXIMATE_PIT"),
        ("valid_until_window", "execution_semantics", "BLOCKING", "UNKNOWN"),
        ("signal_to_execution_lag", "execution_semantics", "MATERIAL", "APPROXIMATE_PIT"),
        ("stale_signal_detection", "execution_semantics", "MATERIAL", "APPROXIMATE_PIT"),
        ("regime_labels", "regime_labels", "MATERIAL", "APPROXIMATE_PIT"),
        ("gate_inputs", "gate_inputs", "MATERIAL", "NOT_APPLICABLE"),
    ):
        rows.append(
            {
                "input_id": input_id,
                "input_type": input_type,
                "severity": severity,
                "point_in_time_status": pit_status,
                "pit_confidence": "LOW" if severity == "BLOCKING" else "MEDIUM",
                "source_artifact_or_config": f"{input_id}.json",
                "used_by_tasks": ["dynamic_strategy_research"],
                "recommended_action": f"review {input_id}",
            }
        )
    return rows


def _pit_input_ids() -> set[str]:
    return {str(row["input_id"]) for row in _pit_rows()}


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
        **{field: False for field in plan.SAFETY_FALSE_FIELDS},
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
