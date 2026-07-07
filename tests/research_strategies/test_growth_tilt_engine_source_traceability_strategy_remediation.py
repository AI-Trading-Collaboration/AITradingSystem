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
    dynamic_strategy_growth_tilt_engine_source_traceability_remediation as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_growth_tilt_engine_source_traceability_remediation_builder(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "traceability"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_source_traceability_remediation(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_tasks"] == ["TRADING-2412", "TRADING-2411", "TRADING-2410"]
    assert payload["source_traceability_remediation_completed"] is True
    assert payload["input_gap_count"] == 7
    assert payload["source_traceability_gap_count"] == 7
    assert payload["source_traceability_remediated_count"] == 2
    assert payload["remaining_source_traceability_gap_count"] == 5
    assert payload["remaining_blocked_or_gap_count"] == 7
    assert payload["contract_ready_count"] == 0
    assert payload["as_of_status_rollback_count"] == 0
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    records = payload["source_traceability_remediation"][
        "source_traceability_remediation_records"
    ]
    assert [record["feature_id"] for record in records] == [
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "trend_features",
        "volatility_inputs",
        "drawdown_features",
        "risk_on_trend_filter_context",
        "growth_tilt_engine_signal_artifact",
    ]
    assert all(record["contract_ready"] is False for record in records)

    for key in (
        "json_path",
        "source_traceability_contract_metadata_json",
        "before_after_source_traceability_remediation_json",
        "updated_source_feature_mapping_json",
        "remaining_blocker_summary_json",
        "markdown_path",
        "source_traceability_contract_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_source_traceability_remediation_cli(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "traceability_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-source-traceability-remediation",
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
    assert "source_traceability_remediation_completed=True" in result.output
    assert "growth_tilt_engine_blocker_resolved=False" in result.output
    assert "valid_until_window_blocker_resolved=False" in result.output
    assert "candidate_search_enabled=False" in result.output
    assert "observation_enabled=False" in result.output
    assert "paper_shadow_enabled=False" in result.output
    assert "production_enabled=False" in result.output
    assert "broker_enabled=False" in result.output
    assert "input_gap_count=7" in result.output
    assert "source_traceability_gap_count=7" in result.output
    assert "source_traceability_remediated_count=2" in result.output
    assert "remaining_blocked_or_gap_count=7" in result.output
    assert "contract_ready_count=0" in result.output
    assert f"next_route={impl.NEXT_ROUTE}" in result.output
    assert (output_root / "source_traceability_remediation_result.json").exists()
    assert (output_root / "source_traceability_contract_metadata.json").exists()


def test_growth_tilt_engine_source_traceability_missing_source_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "blocked"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_source_traceability_remediation(
        **{
            **_source_kwargs(paths),
            "source_2412_as_of_remediation_result_path": tmp_path / "missing.json",
        },
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.BLOCKED_SOURCE_STATUS
    assert payload["source_traceability_remediation_completed"] is False
    assert payload["source_traceability_gap_count"] == 0
    assert any(
        "missing source artifact: as_of_remediation_result_2412" in error
        for error in payload["source_validation_errors"]
    )
    assert payload["growth_tilt_engine_blocker_resolved"] is False
    assert payload["candidate_search_enabled"] is False
    assert (output_root / "source_traceability_remediation_result.json").exists()


def test_growth_tilt_engine_source_traceability_remediation_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_source_traceability_remediation"]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-source-traceability-remediation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "source_traceability_remediation_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2414_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "growth_tilt_engine_source_traceability_remediation" in catalog
    assert "growth-tilt-engine-source-traceability-remediation" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    rows = _mapping_rows()
    remediation_items = _remediation_items()
    paths = {
        "as_of_remediation_result_2412": root / "as_of_result.json",
        "before_after_remediation_2412": root / "before_after.json",
        "updated_source_feature_mapping_2412": root / "updated_mapping.json",
        "remaining_blocker_summary_2412": root / "remaining_summary.json",
        "research_doc_2412": root / "as_of_doc.md",
        "remediation_plan_result_2411": root / "remediation_result.json",
        "ordered_remediation_items_2411": root / "ordered_items.json",
        "unresolved_blocker_summary_2411": root / "unresolved_summary.json",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    _write_json(
        paths["as_of_remediation_result_2412"],
        {
            "task_id": m2412.TASK_ID,
            "status": m2412.READY_STATUS,
            "input_gap_count": 7,
            "as_of_gap_count": 2,
            "as_of_remediated_count": 2,
            "remaining_blocked_or_gap_count": 7,
            "contract_ready_count": 0,
            "recommended_next_research_task": m2412.NEXT_ROUTE,
            "updated_source_feature_mapping": {"mapping_rows": rows},
            "production_effect": "none",
            "broker_action": "none",
            **{field: False for field in m2412.SAFETY_FALSE_FIELDS},
        },
    )
    _write_json(
        paths["before_after_remediation_2412"],
        {"status": m2412.READY_STATUS, "before_after_remediation": {"record_count": 2}},
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
    paths["research_doc_2412"].write_text(m2412.READY_STATUS, encoding="utf-8")
    paths["report_registry"].write_text(
        "\n".join(
            [
                "reports:",
                "  - report_id: growth_tilt_engine_as_of_semantics_remediation",
                "    production_effect: none",
                "    broker_action: none",
                "  - report_id: growth_tilt_engine_contract_gap_remediation_plan",
                "    production_effect: none",
                "    broker_action: none",
                "  - report_id: growth_tilt_engine_source_feature_contract_mapping",
                "    production_effect: none",
                "    broker_action: none",
                "",
            ]
        ),
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        "growth-tilt-engine-as-of-semantics-remediation\n"
        "growth-tilt-engine-contract-gap-remediation-plan\n",
        encoding="utf-8",
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2412_as_of_remediation_result_path": paths[
            "as_of_remediation_result_2412"
        ],
        "source_2412_before_after_remediation_path": paths[
            "before_after_remediation_2412"
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
        "--source-2412-as-of-remediation-result",
        str(paths["as_of_remediation_result_2412"]),
        "--source-2412-before-after-remediation",
        str(paths["before_after_remediation_2412"]),
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
        _item(
            1,
            "equal_risk_baseline_weights",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
            "research_policy.equal_risk",
        ),
        _item(
            2,
            "target_vol_policy",
            "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
            "search_grids.vol_target_growth_tilt",
        ),
        _item(3, "trend_features", "historical price trend / momentum windows"),
    ]


def _item(order: int, feature_id: str, upstream: str) -> dict[str, object]:
    return {
        "remediation_order": order,
        "feature_id": feature_id,
        "source_feature_name": feature_id,
        "current_mapping_status": "missing_source_traceability",
        "remediation_category": "source_traceability_required",
        "missing_source_traceability": True,
        "required_upstream_artifact": upstream,
        "production_effect": "none",
        "broker_action": "none",
    }


def _mapping_rows() -> list[dict[str, object]]:
    return [
        _row("adjusted_prices", "mapped_with_caveats", "mapped_with_caveats"),
        _row("returns", "mapped_with_caveats", "mapped_with_caveats"),
        _row(
            "volatility_inputs",
            "mapped_with_caveats",
            "missing",
            source_traceability_status="not_ready_missing_source_snapshot",
            source_ref="rolling price-derived volatility features",
            as_of_status="ready",
        ),
        _row(
            "trend_features",
            "missing_source_traceability",
            "partial",
            source_ref="historical price trend / momentum windows",
        ),
        _row(
            "drawdown_features",
            "mapped_with_caveats",
            "missing",
            source_traceability_status="not_ready_missing_source_snapshot",
            source_ref="historical drawdown windows",
            as_of_status="ready",
        ),
        _row(
            "equal_risk_baseline_weights",
            "missing_source_traceability",
            "mapped_with_caveats",
            source_system="governed_config",
            source_ref=(
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "research_policy.equal_risk"
            ),
        ),
        _row(
            "target_vol_policy",
            "missing_source_traceability",
            "mapped_with_caveats",
            source_system="governed_config",
            source_ref=(
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "search_grids.vol_target_growth_tilt"
            ),
        ),
        _row(
            "risk_on_trend_filter_context",
            "mapped_with_caveats",
            "missing",
            source_system="governed_config",
            source_ref=(
                "config/research/equal_risk_growth_tilt_candidate_registry.yaml:"
                "research_policy.trend_filter_rule"
            ),
        ),
        _row("execution_signal_validity_policy", "blocked_unresolved", "mapped_with_caveats"),
        _row(
            "growth_tilt_engine_signal_artifact",
            "blocked_unresolved",
            "missing",
            source_system="missing_artifact",
            source_ref="missing standalone growth_tilt_engine signal artifact",
        ),
    ]


def _row(
    feature_id: str,
    mapping_status: str,
    traceability_status: str,
    *,
    source_traceability_status: str | None = None,
    source_system: str = "derived_research_artifact",
    source_ref: str | None = None,
    as_of_status: str | None = None,
) -> dict[str, object]:
    return {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "mapping_status": mapping_status,
        "traceability_status": traceability_status,
        "source_traceability_status": source_traceability_status,
        "source_system": source_system,
        "upstream_artifact_or_registry_reference": source_ref or f"{feature_id} source",
        "as_of_semantics_status": as_of_status,
        "validity_dependency_status": None,
        "pit_gate_status": None,
        "contract_ready": False,
    }


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
