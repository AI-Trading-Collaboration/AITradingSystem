from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as closure_plan,
)


def test_remaining_blocker_closure_plan_builder_preserves_2415_blockers() -> None:
    payload = closure_plan.build_growth_tilt_pit_gate_remaining_blocker_closure_plan(
        _snapshot_2415(),
        _matrix_2415(),
        _remaining_summary_2415(),
        pit_input_registry=_pit_input_registry(),
    )

    assert payload["status"] == closure_plan.READY_STATUS
    assert payload["source_feature_count"] == 10
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["pit_gate_blocked_count"] == 10
    assert payload["blocked_by_source_traceability_count"] == 5
    assert payload["blocked_by_valid_until_window_count"] == 1
    assert payload["remaining_blocker_matrix_ready"] is True
    assert payload["source_traceability_closure_plan_ready"] is True
    assert payload["as_of_evidence_closure_plan_ready"] is True
    assert payload["valid_until_dependency_closure_plan_ready"] is True
    assert payload["pit_gate_evidence_requirements_ready"] is True
    assert payload["growth_tilt_engine_blocking_gap_resolved"] is False
    assert payload["growth_tilt_engine_severity_downgraded"] is False
    assert payload["valid_until_window_blocking_gap_resolved"] is False
    assert payload["valid_until_window_severity_downgraded"] is False
    assert payload["candidate_search_allowed"] is False
    assert payload["candidate_search_resumed"] is False
    assert payload["research_only_observation_allowed"] is False
    assert payload["research_only_observation_approved"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["event_append_enabled"] is False
    assert payload["outcome_binding_enabled"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["daily_report_generated"] is False
    assert payload["recommended_next_research_task"] == closure_plan.NEXT_ROUTE

    rows = {
        row["feature_id"]: row
        for row in payload["remaining_blocker_matrix"]["matrix_rows"]
    }
    assert rows["trend_features"]["blocked_by_source_traceability"] is True
    assert rows["growth_tilt_engine_signal_artifact"]["blocked_by_upstream_artifact"] is True
    assert rows["execution_signal_validity_policy"]["blocked_by_valid_until_window"] is True
    assert (
        rows["execution_signal_validity_policy"]["recommended_closure_task"]
        == closure_plan.VALID_UNTIL_ROUTE
    )
    assert payload["source_traceability_closure_plan"]["source_traceability_gap_count"] == 5
    assert payload["valid_until_dependency_closure_plan"]["dependent_feature_ids"] == [
        "execution_signal_validity_policy"
    ]


def test_growth_tilt_engine_remaining_blocker_closure_plan_strategy(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "closure"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["remaining_blocker_matrix_ready"] is True
    assert payload["source_traceability_closure_plan_ready"] is True
    assert payload["as_of_evidence_closure_plan_ready"] is True
    assert payload["valid_until_dependency_closure_plan_ready"] is True
    assert payload["pit_gate_evidence_requirements_ready"] is True
    assert payload["source_feature_count"] == 10
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["pit_gate_blocked_count"] == 10
    assert payload["blocked_by_source_traceability_count"] == 5
    assert payload["blocked_by_valid_until_window_count"] == 1
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "remaining_blocker_matrix_json",
        "source_traceability_closure_plan_json",
        "as_of_evidence_closure_plan_json",
        "valid_until_dependency_closure_plan_json",
        "pit_gate_evidence_requirements_json",
        "markdown_path",
        "remaining_blocker_matrix_markdown",
        "source_traceability_closure_plan_markdown",
        "valid_until_dependency_closure_plan_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_remaining_blocker_closure_plan_cli(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "closure_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan",
            *_source_args(paths),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "220"},
        terminal_width=220,
    )

    assert result.exit_code == 0, result.output
    assert impl.READY_STATUS in result.output
    assert "remaining_blocker_matrix_ready=true" in result.output
    assert "source_traceability_closure_plan_ready=true" in result.output
    assert "as_of_evidence_closure_plan_ready=true" in result.output
    assert "valid_until_dependency_closure_plan_ready=true" in result.output
    assert "pit_gate_evidence_requirements_ready=true" in result.output
    assert "growth_tilt_engine_blocking_gap_resolved=false" in result.output
    assert "growth_tilt_engine_severity_downgraded=false" in result.output
    assert "candidate_search_resumed=false" in result.output
    assert "research_only_observation_approved=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "event_append_enabled=false" in result.output
    assert "outcome_binding_enabled=false" in result.output
    assert "scheduler_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_action_enabled=false" in result.output
    assert "daily_report_generated=false" in result.output
    assert "source_feature_count=10" in result.output
    assert "pit_gate_ready_count=0" in result.output
    assert "contract_ready_count=0" in result.output
    assert "pit_gate_blocked_count=10" in result.output
    assert "blocked_by_source_traceability_count=5" in result.output
    assert "blocked_by_valid_until_window_count=1" in result.output
    assert f"next_route={impl.NEXT_ROUTE}" in result.output
    assert (output_root / "closure_plan_result.json").exists()
    assert (output_root / "remaining_blocker_matrix.json").exists()


def test_growth_tilt_engine_remaining_blocker_closure_plan_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_pit_gate_remaining_blocker_closure_plan"]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("closure_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2417_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_registers = (
        Path("docs/task_register.md").read_text(encoding="utf-8")
        + Path("docs/task_register_completed.md").read_text(encoding="utf-8")
    )
    assert "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan" in catalog
    assert "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan" in system_flow
    assert impl.TASK_REGISTER_ID in task_registers


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "snapshot": root / "pit_gate_readiness_snapshot_result.json",
        "matrix": root / "pit_gate_readiness_matrix.json",
        "validation": root / "pit_gate_readiness_validation.json",
        "summary": root / "remaining_blocker_summary.json",
        "pit_input_registry": root / "pit_input_registry.yaml",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    _write_json(paths["snapshot"], _snapshot_2415())
    _write_json(paths["matrix"], _matrix_2415())
    _write_json(paths["validation"], _validation_2415())
    _write_json(paths["summary"], _remaining_summary_2415())
    paths["pit_input_registry"].write_text(_pit_input_registry_yaml(), encoding="utf-8")
    paths["report_registry"].write_text(_report_registry_yaml(), encoding="utf-8")
    paths["artifact_catalog"].write_text(_artifact_catalog_text(), encoding="utf-8")
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2415_readiness_snapshot_result_path": paths["snapshot"],
        "source_2415_readiness_matrix_path": paths["matrix"],
        "source_2415_readiness_validation_path": paths["validation"],
        "source_2415_remaining_blocker_summary_path": paths["summary"],
        "pit_input_registry_path": paths["pit_input_registry"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2415-readiness-snapshot-result",
        str(paths["snapshot"]),
        "--source-2415-readiness-matrix",
        str(paths["matrix"]),
        "--source-2415-readiness-validation",
        str(paths["validation"]),
        "--source-2415-remaining-blocker-summary",
        str(paths["summary"]),
        "--pit-input-registry",
        str(paths["pit_input_registry"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
    ]


def _snapshot_2415() -> dict[str, object]:
    return {
        "task_id": "TRADING-2415",
        "status": m2415.READY_STATUS,
        "recommended_next_research_task": m2415.NEXT_ROUTE,
        "source_feature_count": 10,
        "pit_gate_ready_count": 0,
        "contract_ready_count": 0,
        "pit_gate_blocked_count": 10,
        "blocked_by_source_traceability_count": 5,
        "blocked_by_valid_until_window_count": 1,
        "pit_gate_readiness_matrix": _matrix_2415()["pit_gate_readiness_matrix"],
        "remaining_blocker_summary": _remaining_summary_2415()["remaining_blocker_summary"],
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in m2415.SAFETY_FALSE_FIELDS},
    }


def _matrix_2415() -> dict[str, object]:
    rows = [
        _row(
            "adjusted_prices",
            "MARKET_DATA",
            "cached_data_artifact",
            "mapped_with_caveats",
            "not_applicable",
            "pit_gate_blocked_by_missing_as_of_semantics",
            "data/raw/prices_daily.csv adjusted close fields",
        ),
        _row(
            "returns",
            "TECHNICAL_FEATURES",
            "derived_research_artifact",
            "mapped_with_caveats",
            "not_applicable",
            "pit_gate_blocked_by_missing_as_of_semantics",
            "derived from adjusted prices",
        ),
        _row(
            "volatility_inputs",
            "TECHNICAL_FEATURES",
            "derived_research_artifact",
            "not_ready",
            "blocked",
            "pit_gate_blocked_by_missing_source_traceability",
            "rolling price-derived volatility features",
            as_of_status="ready",
            remediation_status="validity_dependency_blocked_by_missing_source_traceability",
        ),
        _row(
            "trend_features",
            "TECHNICAL_FEATURES",
            "derived_research_artifact",
            "not_ready",
            "blocked",
            "pit_gate_blocked_by_missing_source_traceability",
            "historical price trend / momentum windows",
            remediation_status="validity_dependency_blocked_by_missing_source_traceability",
        ),
        _row(
            "drawdown_features",
            "TECHNICAL_FEATURES",
            "derived_research_artifact",
            "not_ready",
            "blocked",
            "pit_gate_blocked_by_missing_source_traceability",
            "historical drawdown windows",
            as_of_status="ready",
            remediation_status="validity_dependency_blocked_by_missing_source_traceability",
        ),
        _row(
            "equal_risk_baseline_weights",
            "PORTFOLIO_STATE",
            "governed_config",
            "ready",
            "ready",
            "pit_gate_blocked_by_missing_as_of_semantics",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:policy",
            remediation_status="validity_dependency_remediated",
        ),
        _row(
            "target_vol_policy",
            "SIGNAL_CONSTRUCTION_POLICY",
            "governed_config",
            "not_ready",
            "blocked",
            "pit_gate_blocked_by_missing_source_traceability",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:vol_target",
            remediation_status="validity_dependency_blocked_by_missing_source_traceability",
        ),
        _row(
            "risk_on_trend_filter_context",
            "BEHAVIOR_GUARDRAIL_CONTEXT",
            "governed_config",
            "ready",
            "ready",
            "pit_gate_blocked_by_missing_as_of_semantics",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:trend_filter",
            remediation_status="validity_dependency_remediated",
        ),
        _row(
            "execution_signal_validity_policy",
            "EXECUTION_SEMANTIC_DEPENDENCY",
            "governed_config",
            "mapped_with_caveats",
            "blocked",
            "pit_gate_blocked_by_valid_until_window",
            "config/research/strategy_execution_policy_registry.yaml:signal_policy",
            valid_until_required=True,
            remediation_status="validity_dependency_blocked_by_valid_until_window",
        ),
        _row(
            "growth_tilt_engine_signal_artifact",
            "SIGNAL_ARTIFACT_CONTRACT",
            "missing_artifact",
            "not_ready",
            "blocked",
            "pit_gate_blocked_by_missing_upstream_artifact",
            "missing standalone growth_tilt_engine signal artifact",
            remediation_status="validity_dependency_blocked_by_missing_source_traceability",
        ),
    ]
    return {
        "task_id": "TRADING-2415",
        "status": m2415.READY_STATUS,
        "report_type": "growth_tilt_engine_pit_gate_readiness_matrix",
        "schema_version": "growth_tilt_engine_pit_gate_readiness_matrix.v1",
        "pit_gate_readiness_matrix": {
            "schema_version": "growth_tilt_engine_pit_gate_readiness_matrix.v1",
            "row_count": 10,
            "matrix_rows": rows,
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _row(
    feature_id: str,
    feature_type: str,
    source_system: str,
    source_traceability_status: str,
    validity_dependency_status: str,
    pit_gate_status: str,
    reference: str,
    *,
    as_of_status: str = "not_ready",
    valid_until_required: bool = False,
    remediation_status: str | None = None,
) -> dict[str, object]:
    return {
        "source_feature_id": feature_id,
        "source_feature_name": feature_id,
        "source_feature_type": feature_type,
        "source_system": source_system,
        "source_traceability_status": source_traceability_status,
        "as_of_semantics_status": as_of_status,
        "validity_dependency_status": validity_dependency_status,
        "source_validity_dependency_remediation_status": remediation_status,
        "valid_until_required": valid_until_required,
        "valid_until_available": False,
        "pit_gate_status": pit_gate_status,
        "pit_gate_blocking_reason": pit_gate_status.replace("pit_gate_blocked_by_", ""),
        "contract_ready": False,
        "upstream_artifact_or_registry_reference": reference,
        "production_effect": "none",
        "broker_action": "none",
    }


def _validation_2415() -> dict[str, object]:
    return {
        "task_id": "TRADING-2415",
        "status": m2415.READY_STATUS,
        "pit_gate_readiness_validation": {
            "valid": True,
            "contract_ready_count": 0,
            "expected_feature_count": 10,
            "observed_feature_count": 10,
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_summary_2415() -> dict[str, object]:
    return {
        "task_id": "TRADING-2415",
        "status": m2415.READY_STATUS,
        "remaining_blocker_summary": {
            "source_feature_count": 10,
            "pit_gate_ready_count": 0,
            "contract_ready_count": 0,
            "pit_gate_blocked_count": 10,
            "blocked_by_source_traceability_count": 5,
            "blocked_by_valid_until_window_count": 1,
            "growth_tilt_engine_blocker_resolved": False,
            "growth_tilt_engine_blocker_downgraded": False,
            "valid_until_window_blocker_resolved": False,
            "valid_until_window_blocker_downgraded": False,
            "candidate_search_enabled": False,
            "observation_enabled": False,
            "paper_shadow_enabled": False,
            "production_enabled": False,
            "broker_enabled": False,
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _pit_input_registry() -> dict[str, object]:
    return {
        "entries": [
            {
                "input_id": "growth_tilt_engine",
                "severity": "BLOCKING",
                "candidate_search_blocker": True,
            },
            {
                "input_id": "valid_until_window",
                "severity": "BLOCKING",
                "candidate_search_blocker": True,
            },
        ]
    }


def _pit_input_registry_yaml() -> str:
    return "\n".join(
        [
            "entries:",
            "  - input_id: growth_tilt_engine",
            "    severity: BLOCKING",
            "    candidate_search_blocker: true",
            "  - input_id: valid_until_window",
            "    severity: BLOCKING",
            "    candidate_search_blocker: true",
        ]
    )


def _report_registry_yaml() -> str:
    return "\n".join(
        [
            "reports:",
            "  - report_id: growth_tilt_engine_pit_gate_readiness_snapshot",
            "    production_effect: none",
            "    broker_action: none",
            "  - report_id: growth_tilt_engine_pit_gate_remaining_blocker_closure_plan",
            "    production_effect: none",
            "    broker_action: none",
        ]
    )


def _artifact_catalog_text() -> str:
    return "\n".join(
        [
            "growth-tilt-engine-pit-gate-readiness-snapshot",
            "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan",
        ]
    )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
