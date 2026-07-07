from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_blocking_gap_remediation_implementation_plan as m2408
import ai_trading_system.dynamic_strategy_pit_coverage_matrix_reusable_implementation as m2405
import ai_trading_system.dynamic_strategy_signal_as_of_validity_contract_schema as impl
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_signal_as_of_validity_contract_schema_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "contract_schema"
    docs_root = tmp_path / "docs" / "research"
    quality_root = tmp_path / "outputs" / "research_quality" / "signal_contracts"

    payload = impl.run_dynamic_strategy_signal_as_of_validity_contract_schema(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        research_quality_output_root=quality_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_tasks"] == [
        "TRADING-2405",
        "TRADING-2406",
        "TRADING-2407",
        "TRADING-2408",
    ]
    assert payload["blocking_gaps"] == ["growth_tilt_engine", "valid_until_window"]
    assert payload["signal_as_of_contract_schema_ready"] is True
    assert payload["source_feature_traceability_contract_schema_ready"] is True
    assert payload["signal_validity_contract_schema_ready"] is True
    assert payload["schema_validation_helpers_ready"] is True
    assert payload["contract_schema_snapshot_ready"] is True
    assert payload["pit_gate_integration_plan_ready"] is True
    assert payload["growth_tilt_engine_blocking_gap_resolved"] is False
    assert payload["valid_until_window_blocking_gap_resolved"] is False
    assert payload["any_blocker_severity_downgraded"] is False
    assert payload["candidate_search_allowed"] is False
    assert payload["candidate_search_resumed"] is False
    assert payload["research_only_observation_allowed"] is False
    assert payload["research_only_observation_approved"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_allowed"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["daily_report_generated"] is False
    assert payload["route_to_next_task"] == impl.NEXT_ROUTE
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    as_of_schema = payload["signal_as_of_contract_schema"]
    assert "signal_id" in as_of_schema["required_fields"]
    assert "generated_at >= source_data_cutoff" in as_of_schema["invariants"]
    feature_schema = payload["source_feature_traceability_contract_schema"]
    assert "pit_status" in feature_schema["required_fields"]
    assert "TRUE_PIT" in feature_schema["fields"]["pit_status"]["values"]
    validity_schema = payload["signal_validity_contract_schema"]
    assert "valid_until" in validity_schema["required_fields"]
    assert "stale_after <= valid_until" in validity_schema["invariants"]
    snapshot = payload["contract_schema_snapshot"]
    assert snapshot["schema_validation_helpers_ready"] is True
    assert snapshot["signal_as_of_contract"]["schema_ready"] is True
    pit_plan = payload["pit_gate_integration_plan"]
    assert pit_plan["current_gate_change_in_2409"] == "none"
    assert pit_plan["current_gate_result"]["candidate_search_allowed"] is False

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "signal_as_of_contract_schema_json",
        "source_feature_traceability_contract_schema_json",
        "signal_validity_contract_schema_json",
        "contract_schema_snapshot_json",
        "pit_gate_integration_plan_json",
        "quality_signal_as_of_contract_schema_json",
        "quality_source_feature_traceability_contract_schema_json",
        "quality_signal_validity_contract_schema_json",
        "quality_contract_schema_snapshot_json",
        "markdown_path",
        "signal_as_of_contract_markdown",
        "source_feature_traceability_contract_markdown",
        "signal_validity_contract_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_signal_as_of_validity_contract_schema_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "contract_schema_cli"
    docs_root = tmp_path / "docs" / "research"
    quality_root = tmp_path / "outputs" / "research_quality" / "signal_contracts"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-signal-as-of-validity-contract-schema",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-07",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
            "--research-quality-output-root",
            str(quality_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert impl.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "contract_schema_result.json").exists()
    assert (output_root / "signal_as_of_contract_schema.json").exists()
    assert (output_root / "source_feature_traceability_contract_schema.json").exists()
    assert (output_root / "signal_validity_contract_schema.json").exists()
    assert (output_root / "contract_schema_snapshot.json").exists()
    assert (output_root / "pit_gate_integration_plan.json").exists()
    assert (quality_root / "contract_schema_snapshot.json").exists()


def test_dynamic_strategy_signal_as_of_validity_contract_schema_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_signal_as_of_validity_contract_schema"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-signal-as-of-validity-contract-schema"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("contract_schema_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2410_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_signal_as_of_validity_contract_schema" in catalog
    assert "dynamic-strategy-signal-as-of-validity-contract-schema" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "implementation_plan_2408": root / "implementation_plan_2408.json",
        "contract_schema_plan_2408": root / "contract_schema_plan_2408.json",
        "candidate_search_gate_policy_2408": (
            root / "candidate_search_gate_policy_2408.json"
        ),
        "registry_snapshot_2405": root / "registry_snapshot_2405.json",
        "pit_gate_result_2405": root / "pit_gate_result_2405.json",
        "blocker_summary_2405": root / "blocker_summary_2405.json",
        "pit_input_registry_config": root / "pit_input_registry.yaml",
    }
    _write_json(
        paths["implementation_plan_2408"],
        {
            **_safe_2408_doc(),
            "blocking_gaps": ["growth_tilt_engine", "valid_until_window"],
            "route_to_next_task": m2408.NEXT_ROUTE,
            "recommended_next_research_task": m2408.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["contract_schema_plan_2408"],
        {
            "task_id": m2408.TASK_ID,
            "status": m2408.READY_STATUS,
            "contract_schema_plan": {
                "contracts": {
                    "signal_as_of_contract": {},
                    "source_feature_traceability_contract": {},
                    "signal_validity_contract": {},
                }
            },
        },
    )
    _write_json(
        paths["candidate_search_gate_policy_2408"],
        {
            "task_id": m2408.TASK_ID,
            "status": m2408.READY_STATUS,
            "candidate_search_gate_policy": {
                "candidate_search_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
            },
        },
    )
    _write_json(
        paths["registry_snapshot_2405"],
        {
            "task_id": impl.SOURCE_TASKS[0],
            "status": m2405.READY_STATUS,
            "pit_input_registry_snapshot": {
                "entries": [_growth_registry_entry(), _valid_until_registry_entry()]
            },
        },
    )
    _write_json(
        paths["pit_gate_result_2405"],
        {
            "task_id": impl.SOURCE_TASKS[0],
            "status": m2405.READY_STATUS,
            "pit_gate_result": {
                "candidate_search_allowed": False,
                "research_only_observation_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "blockers": [
                    "BLOCKING_GAP_GROWTH_TILT_ENGINE",
                    "BLOCKING_GAP_VALID_UNTIL_WINDOW",
                ],
            },
        },
    )
    _write_json(
        paths["blocker_summary_2405"],
        {
            "task_id": impl.SOURCE_TASKS[0],
            "status": m2405.READY_STATUS,
            "pit_blocker_summary": {
                "blocking_gaps": ["growth_tilt_engine", "valid_until_window"],
                "blocking_gap_details": {
                    "growth_tilt_engine": _growth_registry_entry(),
                    "valid_until_window": _valid_until_registry_entry(),
                },
            },
        },
    )
    paths["pit_input_registry_config"].write_text(
        "\n".join(
            [
                "schema_version: test",
                "entries:",
                "  - input_id: growth_tilt_engine",
                "    input_type: SIGNAL",
                "    severity: BLOCKING",
                "    candidate_search_blocker: true",
                "    pit_status: UNKNOWN",
                "    pit_confidence: LOW",
                "  - input_id: valid_until_window",
                "    input_type: EXECUTION_SEMANTIC",
                "    severity: BLOCKING",
                "    candidate_search_blocker: true",
                "    pit_status: UNKNOWN",
                "    pit_confidence: LOW",
            ]
        ),
        encoding="utf-8",
    )
    return paths


def _safe_2408_doc() -> dict[str, object]:
    payload: dict[str, object] = {
        "task_id": m2408.TASK_ID,
        "status": m2408.READY_STATUS,
        "production_effect": "none",
        "broker_action": "none",
    }
    for field in m2408.SAFETY_FALSE_FIELDS:
        payload[field] = False
    return payload


def _growth_registry_entry() -> dict[str, object]:
    return {
        "input_id": "growth_tilt_engine",
        "input_type": "SIGNAL",
        "severity": "BLOCKING",
        "candidate_search_blocker": True,
        "pit_status": "UNKNOWN",
        "pit_confidence": "LOW",
    }


def _valid_until_registry_entry() -> dict[str, object]:
    return {
        "input_id": "valid_until_window",
        "input_type": "EXECUTION_SEMANTIC",
        "severity": "BLOCKING",
        "candidate_search_blocker": True,
        "pit_status": "UNKNOWN",
        "pit_confidence": "LOW",
    }


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2408_implementation_plan_path": paths["implementation_plan_2408"],
        "source_2408_contract_schema_plan_path": paths["contract_schema_plan_2408"],
        "source_2408_candidate_search_gate_policy_path": (
            paths["candidate_search_gate_policy_2408"]
        ),
        "source_2405_registry_snapshot_path": paths["registry_snapshot_2405"],
        "source_2405_pit_gate_result_path": paths["pit_gate_result_2405"],
        "source_2405_blocker_summary_path": paths["blocker_summary_2405"],
        "pit_input_registry_path": paths["pit_input_registry_config"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    mapping = {
        "implementation_plan_2408": "--source-2408-implementation-plan",
        "contract_schema_plan_2408": "--source-2408-contract-schema-plan",
        "candidate_search_gate_policy_2408": (
            "--source-2408-candidate-search-gate-policy"
        ),
        "registry_snapshot_2405": "--source-2405-registry-snapshot",
        "pit_gate_result_2405": "--source-2405-pit-gate-result",
        "blocker_summary_2405": "--source-2405-blocker-summary",
        "pit_input_registry_config": "--pit-input-registry",
    }
    args: list[str] = []
    for key, option in mapping.items():
        args.extend([option, str(paths[key])])
    return args


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
