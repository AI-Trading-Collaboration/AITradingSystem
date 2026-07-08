from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation as m2412,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as m2411,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_snapshot as impl,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_validity_dependency_remediation as m2414,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_remediation as m2413,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_pit_gate_readiness_snapshot as pit_readiness,
)


def test_pit_gate_readiness_snapshot_builder_classifies_all_features() -> None:
    payload = pit_readiness.build_growth_tilt_pit_gate_readiness_snapshot(
        _mapping_result_2410(),
        _source_feature_contract_mapping_2410(),
        _remediation_plan_result_2411(),
        _as_of_result_2412(),
        _source_traceability_result_2413(),
        _signal_validity_result_2414(),
    )

    assert payload["pit_gate_readiness_snapshot_completed"] is True
    assert payload["source_feature_count"] == 10
    assert payload["as_of_ready_count"] == 2
    assert payload["source_traceability_ready_count"] == 2
    assert payload["validity_dependency_ready_count"] == 2
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["pit_gate_blocked_count"] == 10
    assert payload["blocked_by_source_traceability_count"] == 5
    assert payload["blocked_by_valid_until_window_count"] == 1
    assert payload["recommended_next_research_task"] == pit_readiness.NEXT_ROUTE

    rows = {
        row["source_feature_id"]: row
        for row in payload["pit_gate_readiness_matrix"]["matrix_rows"]
    }
    assert list(rows) == [
        "adjusted_prices",
        "returns",
        "volatility_inputs",
        "trend_features",
        "drawdown_features",
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "risk_on_trend_filter_context",
        "execution_signal_validity_policy",
        "growth_tilt_engine_signal_artifact",
    ]
    assert rows["execution_signal_validity_policy"]["valid_until_required"] is True
    assert rows["execution_signal_validity_policy"]["valid_until_available"] is False
    assert rows["execution_signal_validity_policy"]["pit_gate_status"] == (
        "pit_gate_blocked_by_valid_until_window"
    )
    assert rows["trend_features"]["pit_gate_status"] == (
        "pit_gate_blocked_by_missing_source_traceability"
    )
    assert rows["growth_tilt_engine_signal_artifact"]["pit_gate_status"] == (
        "pit_gate_blocked_by_missing_upstream_artifact"
    )
    assert all(row["contract_ready"] is False for row in rows.values())
    assert all(row["eligible_for_candidate_search"] is False for row in rows.values())
    assert all(row["eligible_for_observation"] is False for row in rows.values())
    assert all(row["eligible_for_paper_shadow"] is False for row in rows.values())
    assert all(row["eligible_for_production"] is False for row in rows.values())
    assert payload["contract_ready_not_increased"] is True


def test_pit_gate_readiness_valid_until_missing_cannot_be_ready() -> None:
    payload = pit_readiness.build_growth_tilt_pit_gate_readiness_snapshot(
        _mapping_result_2410(),
        _source_feature_contract_mapping_2410(),
        _remediation_plan_result_2411(),
        _as_of_result_2412(),
        _source_traceability_result_2413(),
        _signal_validity_result_2414(),
    )
    rows = {
        row["source_feature_id"]: row
        for row in payload["pit_gate_readiness_matrix"]["matrix_rows"]
    }

    execution = rows["execution_signal_validity_policy"]
    assert execution["valid_until_required"] is True
    assert execution["valid_until_available"] is False
    assert execution["pit_gate_status"] == "pit_gate_blocked_by_valid_until_window"
    assert execution["pit_gate_blocking_reason"] == "valid_until_window_unresolved"
    assert execution["contract_ready"] is False
    assert payload["valid_until_window_blocker_resolved"] is False
    assert payload["valid_until_window_blocker_downgraded"] is False


def test_growth_tilt_engine_pit_gate_readiness_snapshot_strategy(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "pit_gate"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_pit_gate_readiness_snapshot(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["pit_gate_readiness_snapshot_completed"] is True
    assert payload["source_feature_count"] == 10
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["blocked_by_source_traceability_count"] == 5
    assert payload["blocked_by_valid_until_window_count"] == 1
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "pit_gate_readiness_matrix_json",
        "pit_gate_readiness_validation_json",
        "remaining_blocker_summary_json",
        "markdown_path",
        "pit_gate_readiness_matrix_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_pit_gate_readiness_snapshot_cli(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "pit_gate_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-pit-gate-readiness-snapshot",
            *_source_args(paths),
            "--as-of",
            "2026-07-08",
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
    assert "pit_gate_readiness_snapshot_completed=true" in result.output
    assert "growth_tilt_engine_blocker_resolved=false" in result.output
    assert "valid_until_window_blocker_resolved=false" in result.output
    assert "candidate_search_enabled=false" in result.output
    assert "observation_enabled=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "source_feature_count=10" in result.output
    assert "pit_gate_ready_count=0" in result.output
    assert "contract_ready_count=0" in result.output
    assert "blocked_by_source_traceability_count=5" in result.output
    assert "blocked_by_valid_until_window_count=1" in result.output
    assert f"next_route={impl.NEXT_ROUTE}" in result.output
    assert (output_root / "pit_gate_readiness_snapshot_result.json").exists()
    assert (output_root / "pit_gate_readiness_matrix.json").exists()


def test_growth_tilt_engine_pit_gate_readiness_snapshot_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_pit_gate_readiness_snapshot"]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-pit-gate-readiness-snapshot"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "pit_gate_readiness_snapshot_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2416_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "growth_tilt_engine_pit_gate_readiness_snapshot" in catalog
    assert "growth-tilt-engine-pit-gate-readiness-snapshot" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "mapping_result_2410": root / "mapping_result.json",
        "source_feature_contract_mapping_2410": root / "mapping.json",
        "remediation_plan_result_2411": root / "remediation_plan_result.json",
        "ordered_remediation_items_2411": root / "ordered_items.json",
        "unresolved_blocker_summary_2411": root / "unresolved_summary.json",
        "as_of_remediation_result_2412": root / "as_of_result.json",
        "updated_source_feature_mapping_2412": root / "as_of_mapping.json",
        "remaining_blocker_summary_2412": root / "as_of_remaining.json",
        "source_traceability_remediation_result_2413": root / "trace_result.json",
        "updated_source_feature_mapping_2413": root / "trace_mapping.json",
        "remaining_blocker_summary_2413": root / "trace_remaining.json",
        "signal_validity_dependency_remediation_result_2414": root / "validity_result.json",
        "updated_source_feature_mapping_2414": root / "validity_mapping.json",
        "remaining_blocker_summary_2414": root / "validity_remaining.json",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    _write_json(paths["mapping_result_2410"], _mapping_result_2410())
    _write_json(
        paths["source_feature_contract_mapping_2410"],
        _source_feature_contract_mapping_2410(),
    )
    _write_json(paths["remediation_plan_result_2411"], _remediation_plan_result_2411())
    _write_json(
        paths["ordered_remediation_items_2411"],
        {"ordered_remediation_items": _ordered_items()},
    )
    _write_json(
        paths["unresolved_blocker_summary_2411"],
        {"unresolved_blocker_summary": _summary("2411")},
    )
    _write_json(paths["as_of_remediation_result_2412"], _as_of_result_2412())
    _write_json(
        paths["updated_source_feature_mapping_2412"],
        {"updated_source_feature_mapping": _updated_mapping_2412()},
    )
    _write_json(
        paths["remaining_blocker_summary_2412"],
        {"remaining_blocker_summary": _summary("2412")},
    )
    _write_json(
        paths["source_traceability_remediation_result_2413"],
        _source_traceability_result_2413(),
    )
    _write_json(
        paths["updated_source_feature_mapping_2413"],
        {"updated_source_feature_mapping": _updated_mapping_2413()},
    )
    _write_json(
        paths["remaining_blocker_summary_2413"],
        {"remaining_blocker_summary": _summary("2413")},
    )
    _write_json(
        paths["signal_validity_dependency_remediation_result_2414"],
        _signal_validity_result_2414(),
    )
    _write_json(
        paths["updated_source_feature_mapping_2414"],
        {"updated_source_feature_mapping": _updated_mapping_2414()},
    )
    _write_json(
        paths["remaining_blocker_summary_2414"],
        {"remaining_blocker_summary": _summary("2414")},
    )
    paths["report_registry"].write_text(_report_registry_yaml(), encoding="utf-8")
    paths["artifact_catalog"].write_text(_artifact_catalog_text(), encoding="utf-8")
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2410_mapping_result_path": paths["mapping_result_2410"],
        "source_2410_source_feature_contract_mapping_path": paths[
            "source_feature_contract_mapping_2410"
        ],
        "source_2411_remediation_plan_result_path": paths[
            "remediation_plan_result_2411"
        ],
        "source_2411_ordered_remediation_items_path": paths[
            "ordered_remediation_items_2411"
        ],
        "source_2411_unresolved_blocker_summary_path": paths[
            "unresolved_blocker_summary_2411"
        ],
        "source_2412_as_of_remediation_result_path": paths[
            "as_of_remediation_result_2412"
        ],
        "source_2412_updated_source_feature_mapping_path": paths[
            "updated_source_feature_mapping_2412"
        ],
        "source_2412_remaining_blocker_summary_path": paths[
            "remaining_blocker_summary_2412"
        ],
        "source_2413_source_traceability_remediation_result_path": paths[
            "source_traceability_remediation_result_2413"
        ],
        "source_2413_updated_source_feature_mapping_path": paths[
            "updated_source_feature_mapping_2413"
        ],
        "source_2413_remaining_blocker_summary_path": paths[
            "remaining_blocker_summary_2413"
        ],
        "source_2414_signal_validity_dependency_remediation_result_path": paths[
            "signal_validity_dependency_remediation_result_2414"
        ],
        "source_2414_updated_source_feature_mapping_path": paths[
            "updated_source_feature_mapping_2414"
        ],
        "source_2414_remaining_blocker_summary_path": paths[
            "remaining_blocker_summary_2414"
        ],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2410-mapping-result",
        str(paths["mapping_result_2410"]),
        "--source-2410-source-feature-contract-mapping",
        str(paths["source_feature_contract_mapping_2410"]),
        "--source-2411-remediation-plan-result",
        str(paths["remediation_plan_result_2411"]),
        "--source-2411-ordered-remediation-items",
        str(paths["ordered_remediation_items_2411"]),
        "--source-2411-unresolved-blocker-summary",
        str(paths["unresolved_blocker_summary_2411"]),
        "--source-2412-as-of-remediation-result",
        str(paths["as_of_remediation_result_2412"]),
        "--source-2412-updated-source-feature-mapping",
        str(paths["updated_source_feature_mapping_2412"]),
        "--source-2412-remaining-blocker-summary",
        str(paths["remaining_blocker_summary_2412"]),
        "--source-2413-source-traceability-remediation-result",
        str(paths["source_traceability_remediation_result_2413"]),
        "--source-2413-updated-source-feature-mapping",
        str(paths["updated_source_feature_mapping_2413"]),
        "--source-2413-remaining-blocker-summary",
        str(paths["remaining_blocker_summary_2413"]),
        "--source-2414-signal-validity-dependency-remediation-result",
        str(paths["signal_validity_dependency_remediation_result_2414"]),
        "--source-2414-updated-source-feature-mapping",
        str(paths["updated_source_feature_mapping_2414"]),
        "--source-2414-remaining-blocker-summary",
        str(paths["remaining_blocker_summary_2414"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
    ]


def _mapping_result_2410() -> dict[str, object]:
    return {
        "status": m2410.READY_STATUS,
        "known_source_feature_count": 10,
        "contract_ready_count": 0,
        "recommended_next_research_task": m2410.NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in m2410.SAFETY_FALSE_FIELDS},
    }


def _source_feature_contract_mapping_2410() -> dict[str, object]:
    rows = [_base_row(feature_id) for feature_id in _feature_ids()]
    return {
        "source_feature_contract_mapping": {
            "known_source_feature_count": 10,
            "contract_ready_count": 0,
            "mapping_rows": rows,
        },
    }


def _remediation_plan_result_2411() -> dict[str, object]:
    return {
        "status": m2411.READY_STATUS,
        "gap_count": 7,
        "ordered_remediation_items": _ordered_items(),
        "recommended_next_research_task": m2411.NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in m2411.SAFETY_FALSE_FIELDS},
    }


def _as_of_result_2412() -> dict[str, object]:
    return {
        "status": m2412.READY_STATUS,
        "input_gap_count": 7,
        "as_of_remediated_count": 2,
        "contract_ready_count": 0,
        "recommended_next_research_task": m2412.NEXT_ROUTE,
        "updated_source_feature_mapping": _updated_mapping_2412(),
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in m2412.SAFETY_FALSE_FIELDS},
    }


def _source_traceability_result_2413() -> dict[str, object]:
    return {
        "status": m2413.READY_STATUS,
        "input_gap_count": 7,
        "source_traceability_gap_count": 7,
        "source_traceability_remediated_count": 2,
        "remaining_source_traceability_gap_count": 5,
        "contract_ready_count": 0,
        "recommended_next_research_task": m2413.NEXT_ROUTE,
        "updated_source_feature_mapping": _updated_mapping_2413(),
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in m2413.SAFETY_FALSE_FIELDS},
    }


def _signal_validity_result_2414() -> dict[str, object]:
    return {
        "status": m2414.READY_STATUS,
        "input_gap_count": 7,
        "validity_dependency_gap_count": 8,
        "validity_dependency_remediated_count": 2,
        "validity_dependency_blocked_by_valid_until_window_count": 1,
        "validity_dependency_blocked_by_source_traceability_count": 5,
        "contract_ready_count": 0,
        "recommended_next_research_task": m2414.NEXT_ROUTE,
        "updated_source_feature_mapping": _updated_mapping_2414(),
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in m2414.SAFETY_FALSE_FIELDS},
    }


def _updated_mapping_2412() -> dict[str, object]:
    rows = [_base_row(feature_id) for feature_id in _feature_ids()]
    for row in rows:
        if row["feature_id"] in {"volatility_inputs", "drawdown_features"}:
            row["as_of_semantics_status"] = "ready"
    return {
        "known_source_feature_count": 10,
        "contract_ready_count": 0,
        "mapping_rows": rows,
    }


def _updated_mapping_2413() -> dict[str, object]:
    rows = _updated_mapping_2412()["mapping_rows"]
    for row in rows:
        if row["feature_id"] in {
            "equal_risk_baseline_weights",
            "risk_on_trend_filter_context",
        }:
            row["source_traceability_status"] = "ready"
            row["source_traceability_remediation_status"] = (
                "source_traceability_remediated"
            )
            row["traceability_status"] = "ready"
        elif row["feature_id"] in {
            "volatility_inputs",
            "trend_features",
            "drawdown_features",
            "target_vol_policy",
            "growth_tilt_engine_signal_artifact",
        }:
            row["source_traceability_status"] = "not_ready"
            row["traceability_status"] = "blocked"
        row["pit_gate_status"] = "blocked_pending_pit_evidence"
        row["contract_ready"] = False
    return {
        "known_source_feature_count": 10,
        "contract_ready_count": 0,
        "mapping_rows": rows,
    }


def _updated_mapping_2414() -> dict[str, object]:
    rows = _updated_mapping_2413()["mapping_rows"]
    for row in rows:
        feature_id = row["feature_id"]
        if feature_id in {
            "equal_risk_baseline_weights",
            "risk_on_trend_filter_context",
        }:
            row["validity_dependency_status"] = "ready"
            row["validity_dependency_remediation_status"] = (
                "validity_dependency_remediated"
            )
            row["signal_validity_dependency_contract_metadata"] = _metadata(
                feature_id,
                source_status="ready",
                validity_status="ready",
            )
        elif feature_id == "execution_signal_validity_policy":
            row["source_traceability_status"] = None
            row["traceability_status"] = "mapped_with_caveats"
            row["validity_dependency_status"] = "blocked"
            row["validity_dependency_remediation_status"] = (
                "validity_dependency_blocked_by_valid_until_window"
            )
            row["signal_validity_dependency_contract_metadata"] = _metadata(
                feature_id,
                source_status="mapped_with_caveats",
                validity_status="blocked",
                valid_until_required=True,
                blocking_reason="valid_until_window_unresolved",
            )
        elif feature_id in {
            "volatility_inputs",
            "trend_features",
            "drawdown_features",
            "target_vol_policy",
            "growth_tilt_engine_signal_artifact",
        }:
            row["validity_dependency_status"] = "blocked"
            row["validity_dependency_remediation_status"] = (
                "validity_dependency_blocked_by_missing_source_traceability"
            )
            row["signal_validity_dependency_contract_metadata"] = _metadata(
                feature_id,
                source_status="not_ready",
                validity_status="blocked",
                blocking_reason="source_traceability_unresolved",
            )
    return {
        "known_source_feature_count": 10,
        "contract_ready_count": 0,
        "mapping_rows": rows,
    }


def _base_row(feature_id: str) -> dict[str, object]:
    row = {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "feature_type": "TECHNICAL_FEATURES",
        "mapping_status": "blocked_unresolved",
        "source_system": "derived_research_artifact",
        "traceability_status": "missing",
        "validity_dependency": "none_identified_in_2410",
        "contract_ready": False,
    }
    if feature_id in {"adjusted_prices", "returns"}:
        row["mapping_status"] = "mapped_with_caveats"
        row["traceability_status"] = "mapped_with_caveats"
    if feature_id == "adjusted_prices":
        row["feature_type"] = "MARKET_DATA"
    if feature_id in {
        "equal_risk_baseline_weights",
        "risk_on_trend_filter_context",
        "execution_signal_validity_policy",
    }:
        row["source_system"] = "governed_config"
        row["traceability_status"] = "mapped_with_caveats"
    if feature_id == "growth_tilt_engine_signal_artifact":
        row["source_system"] = "missing_artifact"
    if feature_id == "execution_signal_validity_policy":
        row["validity_dependency"] = "depends_on_valid_until_window_contract"
    return row


def _metadata(
    feature_id: str,
    *,
    source_status: str,
    validity_status: str,
    valid_until_required: bool = False,
    blocking_reason: str | None = None,
) -> dict[str, object]:
    return {
        "source_feature_id": feature_id,
        "source_feature_name": feature_id,
        "source_traceability_status": source_status,
        "validity_dependency_status": validity_status,
        "valid_until_required": valid_until_required,
        "valid_until_available": False,
        "validity_blocking_reason": blocking_reason,
        "pit_gate_status": "blocked_pending_pit_evidence",
        "contract_ready": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _ordered_items() -> list[dict[str, object]]:
    return [
        {
            "remediation_order": 1,
            "feature_id": "execution_signal_validity_policy",
            "source_feature_name": "execution_signal_validity_policy",
            "remediation_category": "validity_dependency_required",
            "missing_validity_dependency": True,
        }
    ]


def _summary(source_task: str) -> dict[str, object]:
    return {
        "source_task": source_task,
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
    }


def _feature_ids() -> list[str]:
    return [
        "adjusted_prices",
        "returns",
        "volatility_inputs",
        "trend_features",
        "drawdown_features",
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "risk_on_trend_filter_context",
        "execution_signal_validity_policy",
        "growth_tilt_engine_signal_artifact",
    ]


def _report_registry_yaml() -> str:
    report_ids = [
        "growth_tilt_engine_source_feature_contract_mapping",
        "growth_tilt_engine_contract_gap_remediation_plan",
        "growth_tilt_engine_as_of_semantics_remediation",
        "growth_tilt_engine_source_traceability_remediation",
        "growth_tilt_engine_signal_validity_dependency_remediation",
    ]
    entries = "\n".join(
        (
            f"  - report_id: {report_id}\n"
            "    production_effect: none\n"
            "    broker_action: none"
        )
        for report_id in report_ids
    )
    return f"reports:\n{entries}\n"


def _artifact_catalog_text() -> str:
    return "\n".join(
        [
            "growth-tilt-engine-source-feature-contract-mapping",
            "growth-tilt-engine-contract-gap-remediation-plan",
            "growth-tilt-engine-as-of-semantics-remediation",
            "growth-tilt-engine-source-traceability-remediation",
            "growth-tilt-engine-signal-validity-dependency-remediation",
        ]
    )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
