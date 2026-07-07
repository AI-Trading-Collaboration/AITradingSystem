from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_blocking_gap_remediation_implementation_plan as impl
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_dynamic_strategy_blocking_gap_remediation_implementation_plan_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "implementation_plan"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_dynamic_strategy_blocking_gap_remediation_implementation_plan(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_tasks"] == ["TRADING-2405", "TRADING-2406", "TRADING-2407"]
    assert payload["blocking_gaps"] == ["growth_tilt_engine", "valid_until_window"]
    assert payload["unified_remediation_architecture_ready"] is True
    assert payload["contract_schema_plan_ready"] is True
    assert payload["implementation_sequence_ready"] is True
    assert payload["blocker_downgrade_workflow_ready"] is True
    assert payload["candidate_search_gate_policy_ready"] is True
    assert payload["growth_tilt_engine_blocking_gap_resolved"] is False
    assert payload["valid_until_window_blocking_gap_resolved"] is False
    assert payload["any_blocker_severity_downgraded"] is False
    assert payload["automatic_downgrade_allowed"] is False
    assert payload["owner_review_required_for_any_downgrade"] is True
    assert payload["candidate_search_allowed"] is False
    assert payload["candidate_search_resumed"] is False
    assert payload["research_only_observation_allowed"] is False
    assert payload["research_only_observation_approved"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["paper_shadow_enabled"] is False
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

    gaps = payload["current_blocking_gaps"]
    assert gaps["growth_tilt_engine"]["severity"] == "BLOCKING"
    assert gaps["growth_tilt_engine"]["resolved"] is False
    assert gaps["valid_until_window"]["severity"] == "BLOCKING"
    assert gaps["valid_until_window"]["downgraded"] is False

    architecture = payload["unified_remediation_architecture"]
    assert [row["layer_id"] for row in architecture["layers"]] == [
        "layer_1_signal_as_of_contract",
        "layer_2_source_feature_traceability",
        "layer_3_signal_validity_contract",
        "layer_4_stale_signal_and_execution_lag_contract",
        "layer_5_as_of_replay_validation",
        "layer_6_pit_gate_downgrade_workflow",
    ]
    contract_schema = payload["contract_schema_plan"]
    assert "signal_as_of_contract" in contract_schema["contracts"]
    assert "signal_validity_contract" in contract_schema["contracts"]
    sequence = payload["implementation_sequence"]
    assert sequence["recommended_immediate_next_task"] == impl.NEXT_ROUTE
    assert sequence["phases"][0]["task_id"] == impl.NEXT_ROUTE
    workflow = payload["blocker_downgrade_workflow"]
    assert workflow["automatic_downgrade_allowed"] is False
    assert workflow["owner_review_required_for_any_downgrade"] is True
    gate_policy = payload["candidate_search_gate_policy"]
    assert gate_policy["candidate_search_allowed"] is False
    assert "both blockers downgraded from BLOCKING" in (
        gate_policy["candidate_search_can_be_reconsidered_only_after"]
    )

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "unified_remediation_architecture_json",
        "contract_schema_plan_json",
        "implementation_sequence_json",
        "blocker_downgrade_workflow_json",
        "candidate_search_gate_policy_json",
        "markdown_path",
        "contract_schema_plan_markdown",
        "blocker_downgrade_workflow_markdown",
        "implementation_sequence_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_blocking_gap_remediation_implementation_plan_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "implementation_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-blocking-gap-remediation-implementation-plan",
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
    assert impl.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "implementation_plan_result.json").exists()
    assert (output_root / "unified_remediation_architecture.json").exists()
    assert (output_root / "contract_schema_plan.json").exists()
    assert (output_root / "implementation_sequence.json").exists()
    assert (output_root / "blocker_downgrade_workflow.json").exists()
    assert (output_root / "candidate_search_gate_policy.json").exists()


def test_dynamic_strategy_blocking_gap_remediation_implementation_plan_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_blocking_gap_remediation_implementation_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-blocking-gap-remediation-implementation-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("implementation_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2409_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_blocking_gap_remediation_implementation_plan" in catalog
    assert "dynamic-strategy-blocking-gap-remediation-implementation-plan" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "implementation_2405": root / "implementation_2405.json",
        "registry_snapshot_2405": root / "registry_snapshot_2405.json",
        "pit_matrix_2405": root / "pit_matrix_2405.json",
        "pit_gate_result_2405": root / "pit_gate_result_2405.json",
        "blocker_summary_2405": root / "blocker_summary_2405.json",
        "remediation_routes_2405": root / "remediation_routes_2405.json",
        "remediation_plan_2406": root / "remediation_plan_2406.json",
        "source_feature_inventory_2406": root / "source_feature_inventory_2406.json",
        "pit_risk_audit_2406": root / "pit_risk_audit_2406.json",
        "signal_construction_gap_analysis_2406": (
            root / "signal_construction_gap_analysis_2406.json"
        ),
        "severity_downgrade_conditions_2406": (
            root / "severity_downgrade_conditions_2406.json"
        ),
        "validation_plan_2406": root / "validation_plan_2406.json",
        "remediation_plan_2407": root / "remediation_plan_2407.json",
        "valid_until_semantics_review_2407": (
            root / "valid_until_semantics_review_2407.json"
        ),
        "stale_signal_risk_audit_2407": root / "stale_signal_risk_audit_2407.json",
        "signal_validity_contract_plan_2407": (
            root / "signal_validity_contract_plan_2407.json"
        ),
        "severity_downgrade_conditions_2407": (
            root / "severity_downgrade_conditions_2407.json"
        ),
        "validation_plan_2407": root / "validation_plan_2407.json",
        "pit_input_registry_config": root / "pit_input_registry.yaml",
    }
    _write_json(
        paths["implementation_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "blocking_gaps": ["growth_tilt_engine", "valid_until_window"],
            "recommended_next_research_task": impl.m2405.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["registry_snapshot_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_input_registry_snapshot": {
                "entries": [_growth_registry_entry(), _valid_until_registry_entry()]
            },
        },
    )
    _write_json(
        paths["pit_matrix_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_coverage_matrix": {"pit_coverage_matrix": _pit_rows()},
        },
    )
    _write_json(
        paths["pit_gate_result_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
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
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_blocker_summary": {
                "blocking_gaps": ["growth_tilt_engine", "valid_until_window"],
                "blocking_gap_details": {
                    "growth_tilt_engine": _growth_registry_entry(),
                    "valid_until_window": _valid_until_registry_entry(),
                },
            },
        },
    )
    _write_json(
        paths["remediation_routes_2405"],
        {
            **_safe_doc(impl.m2405.READY_STATUS),
            "pit_remediation_routes": {
                "routes": {
                    "growth_tilt_engine": {"next_task": impl.m2405.NEXT_ROUTE},
                    "valid_until_window": {"next_task": impl.m2407.TASK_ID},
                }
            },
        },
    )
    _write_json(
        paths["remediation_plan_2406"],
        {
            **_safe_doc(impl.m2406.READY_STATUS),
            "recommended_next_research_task": impl.m2406.NEXT_ROUTE,
            "growth_tilt_engine_blocking_gap_resolved": False,
            "growth_tilt_engine_severity_downgraded": False,
        },
    )
    _write_json(
        paths["source_feature_inventory_2406"],
        {
            **_safe_doc(impl.m2406.READY_STATUS),
            "source_feature_inventory": [{"feature_id": "growth_tilt_engine"}],
        },
    )
    for key in (
        "pit_risk_audit_2406",
        "signal_construction_gap_analysis_2406",
        "severity_downgrade_conditions_2406",
        "validation_plan_2406",
    ):
        _write_json(paths[key], _safe_doc(impl.m2406.READY_STATUS))
    _write_json(
        paths["remediation_plan_2407"],
        {
            **_safe_doc(impl.m2407.READY_STATUS),
            "recommended_next_research_task": impl.m2407.NEXT_ROUTE,
            "valid_until_window_blocking_gap_resolved": False,
            "valid_until_window_severity_downgraded": False,
        },
    )
    for key in (
        "valid_until_semantics_review_2407",
        "stale_signal_risk_audit_2407",
        "severity_downgrade_conditions_2407",
        "validation_plan_2407",
    ):
        _write_json(paths[key], _safe_doc(impl.m2407.READY_STATUS))
    _write_json(
        paths["signal_validity_contract_plan_2407"],
        {
            **_safe_doc(impl.m2407.READY_STATUS),
            "signal_validity_contract_plan": {
                "required_fields": ["signal_id", "valid_from", "valid_until"]
            },
        },
    )
    paths["pit_input_registry_config"].write_text(
        _pit_input_registry_yaml(),
        encoding="utf-8",
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2405_implementation_path": paths["implementation_2405"],
        "source_2405_registry_snapshot_path": paths["registry_snapshot_2405"],
        "source_2405_pit_coverage_matrix_path": paths["pit_matrix_2405"],
        "source_2405_pit_gate_result_path": paths["pit_gate_result_2405"],
        "source_2405_blocker_summary_path": paths["blocker_summary_2405"],
        "source_2405_remediation_routes_path": paths["remediation_routes_2405"],
        "source_2406_remediation_plan_path": paths["remediation_plan_2406"],
        "source_2406_source_feature_inventory_path": paths[
            "source_feature_inventory_2406"
        ],
        "source_2406_pit_risk_audit_path": paths["pit_risk_audit_2406"],
        "source_2406_signal_construction_gap_analysis_path": paths[
            "signal_construction_gap_analysis_2406"
        ],
        "source_2406_severity_downgrade_conditions_path": paths[
            "severity_downgrade_conditions_2406"
        ],
        "source_2406_validation_plan_path": paths["validation_plan_2406"],
        "source_2407_remediation_plan_path": paths["remediation_plan_2407"],
        "source_2407_valid_until_semantics_review_path": paths[
            "valid_until_semantics_review_2407"
        ],
        "source_2407_stale_signal_risk_audit_path": paths[
            "stale_signal_risk_audit_2407"
        ],
        "source_2407_signal_validity_contract_plan_path": paths[
            "signal_validity_contract_plan_2407"
        ],
        "source_2407_severity_downgrade_conditions_path": paths[
            "severity_downgrade_conditions_2407"
        ],
        "source_2407_validation_plan_path": paths["validation_plan_2407"],
        "pit_input_registry_path": paths["pit_input_registry_config"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "implementation_2405": "--source-2405-implementation",
        "registry_snapshot_2405": "--source-2405-registry-snapshot",
        "pit_matrix_2405": "--source-2405-pit-matrix",
        "pit_gate_result_2405": "--source-2405-pit-gate-result",
        "blocker_summary_2405": "--source-2405-blocker-summary",
        "remediation_routes_2405": "--source-2405-remediation-routes",
        "remediation_plan_2406": "--source-2406-remediation-plan",
        "source_feature_inventory_2406": "--source-2406-source-feature-inventory",
        "pit_risk_audit_2406": "--source-2406-pit-risk-audit",
        "signal_construction_gap_analysis_2406": (
            "--source-2406-signal-construction-gap-analysis"
        ),
        "severity_downgrade_conditions_2406": (
            "--source-2406-severity-downgrade-conditions"
        ),
        "validation_plan_2406": "--source-2406-validation-plan",
        "remediation_plan_2407": "--source-2407-remediation-plan",
        "valid_until_semantics_review_2407": (
            "--source-2407-valid-until-semantics-review"
        ),
        "stale_signal_risk_audit_2407": "--source-2407-stale-signal-risk-audit",
        "signal_validity_contract_plan_2407": (
            "--source-2407-signal-validity-contract-plan"
        ),
        "severity_downgrade_conditions_2407": (
            "--source-2407-severity-downgrade-conditions"
        ),
        "validation_plan_2407": "--source-2407-validation-plan",
        "pit_input_registry_config": "--pit-input-registry",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _pit_rows() -> list[dict[str, object]]:
    return [
        {"input_id": "growth_tilt_engine", "severity": "BLOCKING"},
        {"input_id": "valid_until_window", "severity": "BLOCKING"},
    ]


def _growth_registry_entry() -> dict[str, object]:
    return {
        "input_id": "growth_tilt_engine",
        "input_type": "SIGNAL",
        "severity": "BLOCKING",
        "pit_status": "UNKNOWN",
        "pit_confidence": "LOW",
        "candidate_search_blocker": True,
        "observation_blocker": True,
        "paper_shadow_blocker": True,
        "production_blocker": True,
    }


def _valid_until_registry_entry() -> dict[str, object]:
    return {
        "input_id": "valid_until_window",
        "input_type": "EXECUTION_SEMANTIC",
        "severity": "BLOCKING",
        "pit_status": "UNKNOWN",
        "pit_confidence": "LOW",
        "candidate_search_blocker": True,
        "observation_blocker": True,
        "paper_shadow_blocker": True,
        "production_blocker": True,
    }


def _pit_input_registry_yaml() -> str:
    return """
schema_version: dynamic_strategy_pit_input_registry.v1
entries:
  - input_id: growth_tilt_engine
    input_type: SIGNAL
    pit_status: UNKNOWN
    pit_confidence: LOW
    severity: BLOCKING
    candidate_search_blocker: true
    observation_blocker: true
    paper_shadow_blocker: true
    production_blocker: true
  - input_id: valid_until_window
    input_type: EXECUTION_SEMANTIC
    pit_status: UNKNOWN
    pit_confidence: LOW
    severity: BLOCKING
    candidate_search_blocker: true
    observation_blocker: true
    paper_shadow_blocker: true
    production_blocker: true
"""


def _safe_doc(status: str) -> dict[str, object]:
    return {
        "status": status,
        **{field: False for field in impl.m2405.SAFETY_FALSE_FIELDS},
        **{field: False for field in impl.m2406.SAFETY_FALSE_FIELDS},
        **{field: False for field in impl.m2407.SAFETY_FALSE_FIELDS},
        "candidate_search_allowed": False,
        "research_only_observation_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "candidate_search_resumed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
