from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation as m2412,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as m2415,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as m2416,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_remediation as m2413,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_upstream_artifact_closure as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_pit_gate_remaining_blocker_closure_plan as closure_plan_2416,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_source_traceability_upstream_artifact_closure as closure,
)


def test_source_traceability_upstream_artifact_closure_builder_preserves_gate_state() -> None:
    sources = _source_documents()
    payload = closure.build_growth_tilt_source_traceability_upstream_artifact_closure(
        sources["closure_result_2416"],
        sources["remaining_blocker_matrix_2416"],
        sources["source_traceability_closure_plan_2416"],
        sources["readiness_snapshot_result_2415"],
        sources["readiness_matrix_2415"],
        sources["source_traceability_remediation_result_2413"],
        sources["updated_source_feature_mapping_2413"],
        sources["updated_source_feature_mapping_2412"],
        sources["source_feature_contract_mapping_2410"],
        pit_input_registry=_pit_input_registry(),
    )

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_feature_count"] == 10
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["pit_gate_blocked_count"] == 10
    assert payload["blocked_by_source_traceability_count"] == 5
    assert payload["blocked_by_valid_until_window_count"] == 1
    assert payload["source_traceability_evidence_row_count"] == 5
    assert payload["source_traceability_pre_recheck_evidence_ready_count"] == 4
    assert payload["source_traceability_still_blocked_count"] == 1
    assert payload["upstream_artifact_pre_recheck_evidence_ready_count"] == 4
    assert payload["pit_gate_recheck_required"] is True
    assert payload["auto_mark_pit_gate_ready"] is False
    assert payload["auto_mark_contract_ready"] is False
    assert payload["candidate_search_allowed"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_action_enabled"] is False
    assert payload["daily_report_generated"] is False
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE

    evidence_rows = {
        row["feature_id"]: row
        for row in payload["source_traceability_closure_evidence"]["evidence_rows"]
    }
    assert evidence_rows["volatility_inputs"]["source_traceability_evidence_ready"] is True
    assert evidence_rows["target_vol_policy"]["source_traceability_evidence_ready"] is True
    assert (
        evidence_rows["growth_tilt_engine_signal_artifact"][
            "traceability_closure_status"
        ]
        == "STILL_BLOCKED_MISSING_UPSTREAM_SIGNAL_ARTIFACT"
    )
    assert (
        evidence_rows["growth_tilt_engine_signal_artifact"]["required_next_action"]
        == "create_or_register_upstream_signal_artifact_metadata"
    )

    mapping_rows = {
        row["feature_id"]: row
        for row in payload["updated_source_feature_mapping"]["mapping_rows"]
    }
    assert mapping_rows["target_vol_policy"]["contract_ready_after_2417"] is False
    assert mapping_rows["target_vol_policy"]["pit_gate_ready_after_2417"] is False
    assert (
        mapping_rows["target_vol_policy"]["source_traceability_status_after_2417"]
        == "evidence_available_pending_pit_recheck"
    )


def test_growth_tilt_engine_source_traceability_upstream_artifact_closure_strategy(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "closure"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_source_traceability_upstream_artifact_closure(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_traceability_closure_evidence_ready"] is True
    assert payload["upstream_artifact_closure_evidence_ready"] is True
    assert payload["updated_source_feature_mapping_ready"] is True
    assert payload["remaining_blocker_summary_ready"] is True
    assert payload["source_traceability_pre_recheck_evidence_ready_count"] == 4
    assert payload["source_traceability_still_blocked_count"] == 1
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "source_traceability_closure_evidence_json",
        "upstream_artifact_closure_evidence_json",
        "updated_source_feature_mapping_json",
        "remaining_blocker_summary_json",
        "markdown_path",
        "source_traceability_closure_evidence_markdown",
        "upstream_artifact_closure_evidence_markdown",
        "updated_source_feature_mapping_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_source_traceability_upstream_artifact_closure_cli(
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
            "growth-tilt-engine-source-traceability-upstream-artifact-closure",
            *_source_args(paths),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "240"},
        terminal_width=240,
    )

    assert result.exit_code == 0, result.output
    assert impl.READY_STATUS in result.output
    assert "source_traceability_closure_evidence_ready=true" in result.output
    assert "upstream_artifact_closure_evidence_ready=true" in result.output
    assert "updated_source_feature_mapping_ready=true" in result.output
    assert "remaining_blocker_summary_ready=true" in result.output
    assert "pit_gate_recheck_required=true" in result.output
    assert "auto_mark_pit_gate_ready=false" in result.output
    assert "auto_mark_contract_ready=false" in result.output
    assert "candidate_search_resumed=false" in result.output
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
    assert "source_traceability_pre_recheck_evidence_ready_count=4" in result.output
    assert "source_traceability_still_blocked_count=1" in result.output
    assert f"next_route={impl.NEXT_ROUTE}" in result.output
    assert (output_root / "closure_result.json").exists()
    assert (output_root / "source_traceability_closure_evidence.json").exists()


def test_growth_tilt_engine_source_traceability_upstream_artifact_closure_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_source_traceability_upstream_artifact_closure"]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-source-traceability-upstream-artifact-closure"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("closure_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2418_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_registers = (
        Path("docs/task_register.md").read_text(encoding="utf-8")
        + Path("docs/task_register_completed.md").read_text(encoding="utf-8")
    )
    assert "growth_tilt_engine_source_traceability_upstream_artifact_closure" in catalog
    assert (
        "growth-tilt-engine-source-traceability-upstream-artifact-closure"
        in system_flow
    )
    assert impl.TASK_REGISTER_ID in task_registers


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "closure_result_2416": root / "closure_plan_result.json",
        "remaining_blocker_matrix_2416": root / "remaining_blocker_matrix.json",
        "source_traceability_closure_plan_2416": (
            root / "source_traceability_closure_plan.json"
        ),
        "as_of_evidence_closure_plan_2416": root / "as_of_evidence_closure_plan.json",
        "valid_until_dependency_closure_plan_2416": (
            root / "valid_until_dependency_closure_plan.json"
        ),
        "pit_gate_evidence_requirements_2416": (
            root / "pit_gate_evidence_requirements.json"
        ),
        "readiness_snapshot_result_2415": root / "pit_gate_readiness_snapshot_result.json",
        "readiness_matrix_2415": root / "pit_gate_readiness_matrix.json",
        "source_traceability_remediation_result_2413": (
            root / "source_traceability_remediation_result.json"
        ),
        "updated_source_feature_mapping_2413": (
            root / "updated_source_feature_mapping_2413.json"
        ),
        "remaining_blocker_summary_2413": root / "remaining_blocker_summary_2413.json",
        "updated_source_feature_mapping_2412": (
            root / "updated_source_feature_mapping_2412.json"
        ),
        "mapping_result_2410": root / "mapping_result_2410.json",
        "source_feature_contract_mapping_2410": (
            root / "source_feature_contract_mapping_2410.json"
        ),
        "pit_input_registry": root / "pit_input_registry.yaml",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    for key, path in paths.items():
        if key in sources:
            _write_json(path, sources[key])
    paths["pit_input_registry"].write_text(_pit_input_registry_yaml(), encoding="utf-8")
    paths["report_registry"].write_text(_report_registry_yaml(), encoding="utf-8")
    paths["artifact_catalog"].write_text(_artifact_catalog_text(), encoding="utf-8")
    return paths


def _source_documents() -> dict[str, object]:
    snapshot_2415 = _snapshot_2415()
    matrix_2415 = _matrix_2415()
    closure_result_2416 = (
        closure_plan_2416.build_growth_tilt_pit_gate_remaining_blocker_closure_plan(
            snapshot_2415,
            matrix_2415,
            _remaining_summary_2415(),
            pit_input_registry=_pit_input_registry(),
        )
    )
    mapping_rows = _mapping_rows()
    return {
        "closure_result_2416": closure_result_2416,
        "remaining_blocker_matrix_2416": {
            "task_id": "TRADING-2416",
            "status": m2416.READY_STATUS,
            "remaining_blocker_matrix": closure_result_2416[
                "remaining_blocker_matrix"
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
        "source_traceability_closure_plan_2416": {
            "task_id": "TRADING-2416",
            "status": m2416.READY_STATUS,
            "source_traceability_closure_plan": closure_result_2416[
                "source_traceability_closure_plan"
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
        "as_of_evidence_closure_plan_2416": {
            "task_id": "TRADING-2416",
            "status": m2416.READY_STATUS,
            "as_of_evidence_closure_plan": closure_result_2416[
                "as_of_evidence_closure_plan"
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
        "valid_until_dependency_closure_plan_2416": {
            "task_id": "TRADING-2416",
            "status": m2416.READY_STATUS,
            "valid_until_dependency_closure_plan": closure_result_2416[
                "valid_until_dependency_closure_plan"
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
        "pit_gate_evidence_requirements_2416": {
            "task_id": "TRADING-2416",
            "status": m2416.READY_STATUS,
            "pit_gate_evidence_requirements": closure_result_2416[
                "pit_gate_evidence_requirements"
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
        "readiness_snapshot_result_2415": snapshot_2415,
        "readiness_matrix_2415": matrix_2415,
        "source_traceability_remediation_result_2413": {
            "task_id": "TRADING-2413",
            "status": m2413.READY_STATUS,
            "generated_at": "2026-07-07T16:52:28Z",
            "remaining_source_traceability_gap_count": 5,
            "source_traceability_remediated_count": 2,
            "production_effect": "none",
            "broker_action": "none",
        },
        "updated_source_feature_mapping_2413": _mapping_doc(
            m2413.READY_STATUS,
            mapping_rows,
        ),
        "remaining_blocker_summary_2413": {
            "task_id": "TRADING-2413",
            "status": m2413.READY_STATUS,
            "remaining_blocker_summary": {
                "remaining_source_traceability_gap_count": 5,
                "source_traceability_gap_count": 7,
                "source_traceability_remediated_count": 2,
                "production_effect": "none",
                "broker_action": "none",
            },
            "production_effect": "none",
            "broker_action": "none",
        },
        "updated_source_feature_mapping_2412": _mapping_doc(
            m2412.READY_STATUS,
            mapping_rows,
        ),
        "mapping_result_2410": {
            "task_id": "TRADING-2410",
            "status": m2410.READY_STATUS,
            "generated_at": "2026-07-07T15:44:46Z",
            "known_source_feature_count": 10,
            "contract_ready_count": 0,
            "recommended_next_research_task": m2410.NEXT_ROUTE,
            "production_effect": "none",
            "broker_action": "none",
        },
        "source_feature_contract_mapping_2410": {
            "task_id": "TRADING-2410",
            "status": m2410.READY_STATUS,
            "source_feature_contract_mapping": {
                "known_source_feature_count": 10,
                "contract_ready_count": 0,
                "mapping_rows": mapping_rows,
                "production_effect": "none",
                "broker_action": "none",
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    }


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2416_closure_result_path": paths["closure_result_2416"],
        "source_2416_remaining_blocker_matrix_path": paths[
            "remaining_blocker_matrix_2416"
        ],
        "source_2416_source_traceability_closure_plan_path": paths[
            "source_traceability_closure_plan_2416"
        ],
        "source_2416_as_of_evidence_closure_plan_path": paths[
            "as_of_evidence_closure_plan_2416"
        ],
        "source_2416_valid_until_dependency_closure_plan_path": paths[
            "valid_until_dependency_closure_plan_2416"
        ],
        "source_2416_pit_gate_evidence_requirements_path": paths[
            "pit_gate_evidence_requirements_2416"
        ],
        "source_2415_readiness_snapshot_result_path": paths[
            "readiness_snapshot_result_2415"
        ],
        "source_2415_readiness_matrix_path": paths["readiness_matrix_2415"],
        "source_2413_source_traceability_remediation_result_path": paths[
            "source_traceability_remediation_result_2413"
        ],
        "source_2413_updated_source_feature_mapping_path": paths[
            "updated_source_feature_mapping_2413"
        ],
        "source_2413_remaining_blocker_summary_path": paths[
            "remaining_blocker_summary_2413"
        ],
        "source_2412_updated_source_feature_mapping_path": paths[
            "updated_source_feature_mapping_2412"
        ],
        "source_2410_mapping_result_path": paths["mapping_result_2410"],
        "source_2410_source_feature_contract_mapping_path": paths[
            "source_feature_contract_mapping_2410"
        ],
        "pit_input_registry_path": paths["pit_input_registry"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    option_by_key = {
        "closure_result_2416": "--source-2416-closure-result",
        "remaining_blocker_matrix_2416": "--source-2416-remaining-blocker-matrix",
        "source_traceability_closure_plan_2416": (
            "--source-2416-source-traceability-closure-plan"
        ),
        "as_of_evidence_closure_plan_2416": (
            "--source-2416-as-of-evidence-closure-plan"
        ),
        "valid_until_dependency_closure_plan_2416": (
            "--source-2416-valid-until-dependency-closure-plan"
        ),
        "pit_gate_evidence_requirements_2416": (
            "--source-2416-pit-gate-evidence-requirements"
        ),
        "readiness_snapshot_result_2415": "--source-2415-readiness-snapshot-result",
        "readiness_matrix_2415": "--source-2415-readiness-matrix",
        "source_traceability_remediation_result_2413": (
            "--source-2413-source-traceability-remediation-result"
        ),
        "updated_source_feature_mapping_2413": (
            "--source-2413-updated-source-feature-mapping"
        ),
        "remaining_blocker_summary_2413": "--source-2413-remaining-blocker-summary",
        "updated_source_feature_mapping_2412": (
            "--source-2412-updated-source-feature-mapping"
        ),
        "mapping_result_2410": "--source-2410-mapping-result",
        "source_feature_contract_mapping_2410": (
            "--source-2410-source-feature-contract-mapping"
        ),
        "pit_input_registry": "--pit-input-registry",
        "report_registry": "--report-registry",
        "artifact_catalog": "--artifact-catalog",
    }
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


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
        ),
        _row(
            "trend_features",
            "TECHNICAL_FEATURES",
            "derived_research_artifact",
            "not_ready",
            "blocked",
            "pit_gate_blocked_by_missing_source_traceability",
            "historical price trend / momentum windows",
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
        ),
        _row(
            "equal_risk_baseline_weights",
            "PORTFOLIO_STATE",
            "governed_config",
            "ready",
            "ready",
            "pit_gate_blocked_by_missing_as_of_semantics",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
        ),
        _row(
            "target_vol_policy",
            "SIGNAL_CONSTRUCTION_POLICY",
            "governed_config",
            "not_ready",
            "blocked",
            "pit_gate_blocked_by_missing_source_traceability",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
        ),
        _row(
            "risk_on_trend_filter_context",
            "BEHAVIOR_GUARDRAIL_CONTEXT",
            "governed_config",
            "ready",
            "ready",
            "pit_gate_blocked_by_missing_as_of_semantics",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule",
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
        ),
        _row(
            "growth_tilt_engine_signal_artifact",
            "SIGNAL_ARTIFACT_CONTRACT",
            "missing_artifact",
            "not_ready",
            "blocked",
            "pit_gate_blocked_by_missing_upstream_artifact",
            "missing standalone growth_tilt_engine signal artifact",
        ),
    ]
    return {
        "task_id": "TRADING-2415",
        "status": m2415.READY_STATUS,
        "pit_gate_readiness_matrix": {
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
) -> dict[str, object]:
    return {
        "source_feature_id": feature_id,
        "source_feature_name": feature_id,
        "source_feature_type": feature_type,
        "source_system": source_system,
        "source_traceability_status": source_traceability_status,
        "as_of_semantics_status": as_of_status,
        "validity_dependency_status": validity_dependency_status,
        "source_validity_dependency_remediation_status": (
            "validity_dependency_blocked_by_valid_until_window"
            if valid_until_required
            else "validity_dependency_blocked_by_missing_source_traceability"
        ),
        "valid_until_required": valid_until_required,
        "valid_until_available": False,
        "pit_gate_status": pit_gate_status,
        "pit_gate_blocking_reason": pit_gate_status.replace("pit_gate_blocked_by_", ""),
        "contract_ready": False,
        "upstream_artifact_or_registry_reference": reference,
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_summary_2415() -> dict[str, object]:
    return {
        "remaining_blocker_summary": {
            "source_feature_count": 10,
            "pit_gate_ready_count": 0,
            "contract_ready_count": 0,
            "pit_gate_blocked_count": 10,
            "blocked_by_source_traceability_count": 5,
            "blocked_by_valid_until_window_count": 1,
            "growth_tilt_engine_blocker_resolved": False,
            "valid_until_window_blocker_resolved": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    }


def _mapping_doc(status: str, mapping_rows: list[dict[str, object]]) -> dict[str, object]:
    return {
        "task_id": "SOURCE",
        "status": status,
        "updated_source_feature_mapping": {
            "known_source_feature_count": 10,
            "contract_ready_count": 0,
            "mapping_rows": mapping_rows,
            "production_effect": "none",
            "broker_action": "none",
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _mapping_rows() -> list[dict[str, object]]:
    return [
        _mapping_row(
            "adjusted_prices",
            "cached_data_artifact",
            "data/raw/prices_daily.csv adjusted close fields",
        ),
        _mapping_row("returns", "derived_research_artifact", "derived from adjusted prices"),
        _mapping_row(
            "volatility_inputs",
            "derived_research_artifact",
            "rolling price-derived volatility features",
            upstream_report="growth_tilt_engine_as_of_semantics_remediation",
            as_of_ready=True,
        ),
        _mapping_row(
            "trend_features",
            "derived_research_artifact",
            "historical price trend / momentum windows",
            upstream_report="growth_tilt_engine_source_feature_contract_mapping",
        ),
        _mapping_row(
            "drawdown_features",
            "derived_research_artifact",
            "historical drawdown windows",
            upstream_report="growth_tilt_engine_as_of_semantics_remediation",
            as_of_ready=True,
        ),
        _mapping_row(
            "equal_risk_baseline_weights",
            "governed_config",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.equal_risk",
            source_snapshot_hash="sha256:test",
            traceability_ready=True,
        ),
        _mapping_row(
            "target_vol_policy",
            "governed_config",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:search_grids.vol_target_growth_tilt",
            upstream_report="growth_tilt_engine_source_feature_contract_mapping",
            config_key="search_grids.vol_target_growth_tilt",
        ),
        _mapping_row(
            "risk_on_trend_filter_context",
            "governed_config",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:research_policy.trend_filter_rule",
            source_snapshot_hash="sha256:test",
            traceability_ready=True,
        ),
        _mapping_row(
            "execution_signal_validity_policy",
            "governed_config",
            "config/research/strategy_execution_policy_registry.yaml:signal_policy",
        ),
        _mapping_row(
            "growth_tilt_engine_signal_artifact",
            "missing_artifact",
            "missing standalone growth_tilt_engine signal artifact",
        ),
    ]


def _mapping_row(
    feature_id: str,
    source_system: str,
    reference: str,
    *,
    upstream_report: str | None = None,
    as_of_ready: bool = False,
    source_snapshot_hash: str | None = None,
    traceability_ready: bool = False,
    config_key: str | None = None,
) -> dict[str, object]:
    metadata = {
        "source_traceability_contract_id": (
            f"growth_tilt_engine:{feature_id}:source_traceability:v1"
        ),
        "upstream_report_registry_id": upstream_report,
        "upstream_artifact_id": reference,
        "upstream_artifact_path": (
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml"
            if source_system == "governed_config"
            else None
        ),
        "upstream_config_path": (
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml"
            if config_key
            else None
        ),
        "upstream_config_key": config_key,
        "source_snapshot_hash": source_snapshot_hash,
        "source_snapshot_reference": (
            f"{reference}@{source_snapshot_hash}" if source_snapshot_hash else None
        ),
        "traceability_status": "ready" if traceability_ready else "not_ready",
        "source_traceability_remediation_status": (
            "source_traceability_remediated"
            if traceability_ready
            else "source_traceability_blocked_by_missing_upstream_artifact"
        ),
        "production_effect": "none",
        "broker_action": "none",
    }
    row: dict[str, object] = {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "source_system": source_system,
        "source_traceability_status": "ready" if traceability_ready else "not_ready",
        "traceability_status": "ready" if traceability_ready else "blocked",
        "contract_ready": False,
        "pit_gate_status": "blocked_pending_pit_evidence",
        "contract_payload": {
            "feature_id": feature_id,
            "source_config": reference,
            "source_data": reference,
            "production_effect": "none",
            "broker_action": "none",
        },
        "source_traceability_contract_metadata": metadata,
        "upstream_artifact_or_registry_reference": reference,
        "production_effect": "none",
        "broker_action": "none",
    }
    if as_of_ready:
        row["as_of_contract_metadata"] = {
            "feature_id": feature_id,
            "as_of_date": "2026-07-08",
            "source_observed_at": "source_rows_with_source_date_lte_as_of_date",
            "production_effect": "none",
            "broker_action": "none",
        }
    return row


def _pit_input_registry() -> dict[str, object]:
    return {
        "entries": [
            {"input_id": "growth_tilt_engine", "severity": "BLOCKING"},
            {"input_id": "valid_until_window", "severity": "BLOCKING"},
        ]
    }


def _pit_input_registry_yaml() -> str:
    return "\n".join(
        [
            "entries:",
            "  - input_id: growth_tilt_engine",
            "    severity: BLOCKING",
            "  - input_id: valid_until_window",
            "    severity: BLOCKING",
        ]
    )


def _report_registry_yaml() -> str:
    return "\n".join(
        [
            "reports:",
            "  - report_id: growth_tilt_engine_source_feature_contract_mapping",
            "    production_effect: none",
            "    broker_action: none",
            "  - report_id: growth_tilt_engine_as_of_semantics_remediation",
            "    production_effect: none",
            "    broker_action: none",
            "  - report_id: growth_tilt_engine_source_traceability_remediation",
            "    production_effect: none",
            "    broker_action: none",
            "  - report_id: growth_tilt_engine_pit_gate_readiness_snapshot",
            "    production_effect: none",
            "    broker_action: none",
            "  - report_id: growth_tilt_engine_pit_gate_remaining_blocker_closure_plan",
            "    production_effect: none",
            "    broker_action: none",
            "  - report_id: growth_tilt_engine_source_traceability_upstream_artifact_closure",
            "    production_effect: none",
            "    broker_action: none",
        ]
    )


def _artifact_catalog_text() -> str:
    return "\n".join(
        [
            "growth-tilt-engine-source-feature-contract-mapping",
            "growth-tilt-engine-source-traceability-remediation",
            "growth-tilt-engine-pit-gate-remaining-blocker-closure-plan",
            "growth-tilt-engine-source-traceability-upstream-artifact-closure",
        ]
    )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
