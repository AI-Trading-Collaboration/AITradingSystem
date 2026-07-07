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
    dynamic_strategy_growth_tilt_engine_signal_validity_dependency_remediation as impl,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_traceability_remediation as m2413,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_growth_tilt_engine_signal_validity_dependency_remediation_builder(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "validity"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_signal_validity_dependency_remediation(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_tasks"] == ["TRADING-2413", "TRADING-2412", "TRADING-2411"]
    assert payload["signal_validity_dependency_remediation_completed"] is True
    assert payload["input_gap_count"] == 7
    assert payload["validity_dependency_gap_count"] == 8
    assert payload["validity_dependency_remediated_count"] == 2
    assert payload["validity_dependency_blocked_by_valid_until_window_count"] == 1
    assert payload["validity_dependency_blocked_by_source_traceability_count"] == 5
    assert payload["remaining_blocked_or_gap_count"] == 7
    assert payload["contract_ready_count"] == 0
    assert payload["as_of_status_rollback_count"] == 0
    assert payload["source_traceability_status_rollback_count"] == 0
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    records = payload["signal_validity_dependency_remediation"][
        "signal_validity_dependency_remediation_records"
    ]
    assert [record["feature_id"] for record in records] == [
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "trend_features",
        "volatility_inputs",
        "drawdown_features",
        "execution_signal_validity_policy",
        "risk_on_trend_filter_context",
        "growth_tilt_engine_signal_artifact",
    ]
    assert all(record["contract_ready"] is False for record in records)

    for key in (
        "json_path",
        "signal_validity_dependency_contract_metadata_json",
        "before_after_signal_validity_dependency_remediation_json",
        "updated_source_feature_mapping_json",
        "remaining_blocker_summary_json",
        "markdown_path",
        "signal_validity_dependency_contract_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_signal_validity_dependency_remediation_cli(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "validity_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-signal-validity-dependency-remediation",
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
    assert "signal_validity_dependency_remediation_completed=True" in result.output
    assert "growth_tilt_engine_blocker_resolved=False" in result.output
    assert "valid_until_window_blocker_resolved=False" in result.output
    assert "candidate_search_enabled=False" in result.output
    assert "observation_enabled=False" in result.output
    assert "paper_shadow_enabled=False" in result.output
    assert "production_enabled=False" in result.output
    assert "broker_enabled=False" in result.output
    assert "input_gap_count=7" in result.output
    assert "validity_dependency_gap_count=8" in result.output
    assert "validity_dependency_remediated_count=2" in result.output
    assert "validity_dependency_blocked_by_valid_until_window_count=1" in result.output
    assert "validity_dependency_blocked_by_source_traceability_count=5" in result.output
    assert "remaining_blocked_or_gap_count=7" in result.output
    assert "contract_ready_count=0" in result.output
    assert f"next_route={impl.NEXT_ROUTE}" in result.output
    assert (output_root / "signal_validity_dependency_remediation_result.json").exists()
    assert (
        output_root / "signal_validity_dependency_contract_metadata.json"
    ).exists()


def test_growth_tilt_engine_signal_validity_missing_source_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "blocked"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_signal_validity_dependency_remediation(
        **{
            **_source_kwargs(paths),
            "source_2413_source_traceability_remediation_result_path": (
                tmp_path / "missing.json"
            ),
        },
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.BLOCKED_SOURCE_STATUS
    assert payload["signal_validity_dependency_remediation_completed"] is False
    assert payload["validity_dependency_gap_count"] == 0
    assert any(
        "missing source artifact: source_traceability_remediation_result_2413" in error
        for error in payload["source_validation_errors"]
    )
    assert payload["growth_tilt_engine_blocker_resolved"] is False
    assert payload["candidate_search_enabled"] is False
    assert (output_root / "signal_validity_dependency_remediation_result.json").exists()


def test_growth_tilt_engine_signal_validity_dependency_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_signal_validity_dependency_remediation"]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-signal-validity-dependency-remediation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "signal_validity_dependency_remediation_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2415_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "growth_tilt_engine_signal_validity_dependency_remediation" in catalog
    assert "growth-tilt-engine-signal-validity-dependency-remediation" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    rows = _mapping_rows()
    source_records = _source_records(rows)
    remediation_items = _remediation_items()
    paths = {
        "source_traceability_remediation_result_2413": root / "source_trace_result.json",
        "source_traceability_contract_metadata_2413": root / "source_trace_metadata.json",
        "before_after_source_traceability_remediation_2413": (
            root / "source_trace_before_after.json"
        ),
        "updated_source_feature_mapping_2413": root / "source_trace_updated_mapping.json",
        "remaining_blocker_summary_2413": root / "source_trace_remaining.json",
        "research_doc_2413": root / "source_trace_doc.md",
        "as_of_remediation_result_2412": root / "as_of_result.json",
        "updated_source_feature_mapping_2412": root / "as_of_updated_mapping.json",
        "remaining_blocker_summary_2412": root / "as_of_remaining.json",
        "research_doc_2412": root / "as_of_doc.md",
        "remediation_plan_result_2411": root / "remediation_result.json",
        "ordered_remediation_items_2411": root / "ordered_items.json",
        "unresolved_blocker_summary_2411": root / "unresolved_summary.json",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    _write_json(
        paths["source_traceability_remediation_result_2413"],
        {
            "task_id": m2413.TASK_ID,
            "status": m2413.READY_STATUS,
            "input_gap_count": 7,
            "source_traceability_gap_count": 7,
            "source_traceability_remediated_count": 2,
            "remaining_source_traceability_gap_count": 5,
            "remaining_blocked_or_gap_count": 7,
            "contract_ready_count": 0,
            "recommended_next_research_task": m2413.NEXT_ROUTE,
            "source_traceability_remediation": {
                "source_traceability_remediation_records": source_records
            },
            "updated_source_feature_mapping": {"mapping_rows": rows},
            "production_effect": "none",
            "broker_action": "none",
            **{field: False for field in m2413.SAFETY_FALSE_FIELDS},
        },
    )
    _write_json(
        paths["source_traceability_contract_metadata_2413"],
        {
            "status": m2413.READY_STATUS,
            "source_traceability_contract_metadata": {
                "metadata_rows": [
                    row["source_traceability_contract_metadata"] for row in rows[:7]
                ]
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["before_after_source_traceability_remediation_2413"],
        {
            "status": m2413.READY_STATUS,
            "before_after_source_traceability_remediation": {"record_count": 7},
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["updated_source_feature_mapping_2413"],
        {
            "status": m2413.READY_STATUS,
            "updated_source_feature_mapping": {
                "contract_ready_count": 0,
                "mapping_rows": rows,
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["remaining_blocker_summary_2413"],
        {
            "status": m2413.READY_STATUS,
            "remaining_blocker_summary": {
                "growth_tilt_engine_blocker_resolved": False,
                "valid_until_window_blocker_resolved": False,
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["as_of_remediation_result_2412"],
        {
            "task_id": m2412.TASK_ID,
            "status": m2412.READY_STATUS,
            "input_gap_count": 7,
            "contract_ready_count": 0,
            "recommended_next_research_task": m2412.NEXT_ROUTE,
            "production_effect": "none",
            "broker_action": "none",
            **{field: False for field in m2412.SAFETY_FALSE_FIELDS},
        },
    )
    _write_json(
        paths["updated_source_feature_mapping_2412"],
        {
            "status": m2412.READY_STATUS,
            "updated_source_feature_mapping": {
                "contract_ready_count": 0,
                "mapping_rows": rows,
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["remaining_blocker_summary_2412"],
        {
            "status": m2412.READY_STATUS,
            "remaining_blocker_summary": {
                "growth_tilt_engine_blocker_resolved": False,
                "valid_until_window_blocker_resolved": False,
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["remediation_plan_result_2411"],
        {
            "task_id": m2411.TASK_ID,
            "status": m2411.READY_STATUS,
            "gap_count": 7,
            "ordered_remediation_items": remediation_items,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["ordered_remediation_items_2411"],
        {
            "status": m2411.READY_STATUS,
            "ordered_remediation_items": remediation_items,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["unresolved_blocker_summary_2411"],
        {
            "status": m2411.READY_STATUS,
            "unresolved_blocker_summary": {
                "growth_tilt_engine_blocker_resolved": False,
                "valid_until_window_blocker_resolved": False,
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    paths["research_doc_2413"].write_text(m2413.READY_STATUS, encoding="utf-8")
    paths["research_doc_2412"].write_text(m2412.READY_STATUS, encoding="utf-8")
    paths["report_registry"].write_text(
        "\n".join(
            [
                "reports:",
                "  - report_id: growth_tilt_engine_source_traceability_remediation",
                "    production_effect: none",
                "    broker_action: none",
                "  - report_id: growth_tilt_engine_as_of_semantics_remediation",
                "    production_effect: none",
                "    broker_action: none",
                "  - report_id: growth_tilt_engine_contract_gap_remediation_plan",
                "    production_effect: none",
                "    broker_action: none",
                "",
            ]
        ),
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        "growth-tilt-engine-source-traceability-remediation\n"
        "growth-tilt-engine-as-of-semantics-remediation\n"
        "growth-tilt-engine-contract-gap-remediation-plan\n",
        encoding="utf-8",
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2413_source_traceability_remediation_result_path": paths[
            "source_traceability_remediation_result_2413"
        ],
        "source_2413_source_traceability_contract_metadata_path": paths[
            "source_traceability_contract_metadata_2413"
        ],
        "source_2413_before_after_remediation_path": paths[
            "before_after_source_traceability_remediation_2413"
        ],
        "source_2413_updated_source_feature_mapping_path": paths[
            "updated_source_feature_mapping_2413"
        ],
        "source_2413_remaining_blocker_summary_path": paths[
            "remaining_blocker_summary_2413"
        ],
        "source_2413_research_doc_path": paths["research_doc_2413"],
        "source_2412_as_of_remediation_result_path": paths[
            "as_of_remediation_result_2412"
        ],
        "source_2412_updated_source_feature_mapping_path": paths[
            "updated_source_feature_mapping_2412"
        ],
        "source_2412_remaining_blocker_summary_path": paths[
            "remaining_blocker_summary_2412"
        ],
        "source_2412_research_doc_path": paths["research_doc_2412"],
        "source_2411_remediation_plan_result_path": paths["remediation_plan_result_2411"],
        "source_2411_ordered_remediation_items_path": paths["ordered_remediation_items_2411"],
        "source_2411_unresolved_blocker_summary_path": paths[
            "unresolved_blocker_summary_2411"
        ],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2413-source-traceability-remediation-result",
        str(paths["source_traceability_remediation_result_2413"]),
        "--source-2413-source-traceability-contract-metadata",
        str(paths["source_traceability_contract_metadata_2413"]),
        "--source-2413-before-after-remediation",
        str(paths["before_after_source_traceability_remediation_2413"]),
        "--source-2413-updated-source-feature-mapping",
        str(paths["updated_source_feature_mapping_2413"]),
        "--source-2413-remaining-blocker-summary",
        str(paths["remaining_blocker_summary_2413"]),
        "--source-2413-research-doc",
        str(paths["research_doc_2413"]),
        "--source-2412-as-of-remediation-result",
        str(paths["as_of_remediation_result_2412"]),
        "--source-2412-updated-source-feature-mapping",
        str(paths["updated_source_feature_mapping_2412"]),
        "--source-2412-remaining-blocker-summary",
        str(paths["remaining_blocker_summary_2412"]),
        "--source-2412-research-doc",
        str(paths["research_doc_2412"]),
        "--source-2411-remediation-plan-result",
        str(paths["remediation_plan_result_2411"]),
        "--source-2411-ordered-remediation-items",
        str(paths["ordered_remediation_items_2411"]),
        "--source-2411-unresolved-blocker-summary",
        str(paths["unresolved_blocker_summary_2411"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
    ]


def _remediation_items() -> list[dict[str, object]]:
    return [
        {
            "remediation_order": 6,
            "feature_id": "execution_signal_validity_policy",
            "source_feature_name": "execution_signal_validity_policy",
            "current_mapping_status": "missing_validity_dependency",
            "remediation_category": "validity_dependency_required",
            "missing_validity_dependency": True,
            "required_upstream_artifact": (
                "signal_validity_contract artifact and valid_until_window remediation result"
            ),
            "validation_requirement": "valid_until_window must be available",
        }
    ]


def _source_records(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    feature_order = [
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "trend_features",
        "volatility_inputs",
        "drawdown_features",
        "risk_on_trend_filter_context",
        "growth_tilt_engine_signal_artifact",
    ]
    row_by_feature = {row["feature_id"]: row for row in rows}
    return [
        {
            "remediation_order": index,
            "feature_id": feature_id,
            "after": row_by_feature[feature_id],
            "contract_ready": False,
            "production_effect": "none",
            "broker_action": "none",
        }
        for index, feature_id in enumerate(feature_order, start=1)
    ]


def _mapping_rows() -> list[dict[str, object]]:
    return [
        _row("adjusted_prices", "mapped_with_caveats", "mapped_with_caveats"),
        _row("returns", "mapped_with_caveats", "mapped_with_caveats"),
        _row(
            "volatility_inputs",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            as_of_status="ready",
            validity_status="not_assessed_in_2412",
        ),
        _row(
            "trend_features",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            validity_status="not_assessed_in_2413",
        ),
        _row(
            "drawdown_features",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            as_of_status="ready",
            validity_status="not_assessed_in_2412",
        ),
        _row(
            "equal_risk_baseline_weights",
            "mapped_with_caveats",
            "ready",
            source_traceability_status="ready",
            source_traceability_remediation_status="source_traceability_remediated",
            validity_status="not_assessed_in_2413",
            source_snapshot_reference="config:equal_risk@sha256:abc",
        ),
        _row(
            "target_vol_policy",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            validity_status="not_assessed_in_2413",
        ),
        _row(
            "risk_on_trend_filter_context",
            "mapped_with_caveats",
            "ready",
            source_traceability_status="ready",
            source_traceability_remediation_status="source_traceability_remediated",
            validity_status="not_assessed_in_2413",
            source_snapshot_reference="config:trend_filter@sha256:abc",
        ),
        _row(
            "execution_signal_validity_policy",
            "blocked_unresolved",
            "mapped_with_caveats",
            validity_status=None,
            validity_dependency="depends_on_valid_until_window_contract",
        ),
        _row(
            "growth_tilt_engine_signal_artifact",
            "blocked_unresolved",
            "blocked",
            source_traceability_status="not_ready",
            validity_status="not_assessed_in_2413",
            source_system="missing_artifact",
        ),
    ]


def _row(
    feature_id: str,
    mapping_status: str,
    traceability_status: str,
    *,
    source_traceability_status: str | None = None,
    source_traceability_remediation_status: str | None = None,
    source_system: str = "derived_research_artifact",
    as_of_status: str | None = None,
    validity_status: str | None = None,
    validity_dependency: str | None = None,
    source_snapshot_reference: str | None = None,
) -> dict[str, object]:
    metadata = {
        "source_snapshot_reference": source_snapshot_reference,
        "source_snapshot_hash": "sha256:abc" if source_snapshot_reference else None,
    }
    return {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "mapping_status": mapping_status,
        "traceability_status": traceability_status,
        "source_traceability_status": source_traceability_status,
        "source_traceability_remediation_status": source_traceability_remediation_status,
        "source_system": source_system,
        "as_of_semantics_status": as_of_status,
        "validity_dependency_status": validity_status,
        "validity_dependency": validity_dependency,
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "contract_ready": False,
        "source_traceability_contract_metadata": metadata,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
