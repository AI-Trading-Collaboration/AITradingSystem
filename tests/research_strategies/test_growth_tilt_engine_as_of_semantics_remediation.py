from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_as_of_semantics_remediation as impl,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as m2411,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_growth_tilt_engine_as_of_semantics_remediation_builder(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "as_of"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_as_of_semantics_remediation(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_tasks"] == ["TRADING-2411", "TRADING-2410", "TRADING-2409"]
    assert payload["as_of_remediation_completed"] is True
    assert payload["input_gap_count"] == 7
    assert payload["as_of_gap_count"] == 2
    assert payload["as_of_remediated_count"] == 2
    assert payload["remaining_blocked_or_gap_count"] == 7
    assert payload["contract_ready_count"] == 0
    assert payload["lookahead_violation_count"] == 0
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    records = payload["as_of_remediation"]["as_of_remediation_records"]
    assert [record["feature_id"] for record in records] == [
        "drawdown_features",
        "volatility_inputs",
    ]
    assert all(record["lookahead_allowed"] is False for record in records)
    assert all(record["contract_ready"] is False for record in records)
    assert all(
        record["as_of_remediation_status"] == "as_of_semantics_remediated"
        for record in records
    )

    for key in (
        "json_path",
        "as_of_contract_metadata_json",
        "before_after_remediation_json",
        "updated_source_feature_mapping_json",
        "remaining_blocker_summary_json",
        "markdown_path",
        "as_of_contract_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_as_of_semantics_remediation_cli(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "as_of_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-as-of-semantics-remediation",
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
    assert "as_of_remediation_completed=True" in result.output
    assert "growth_tilt_engine_blocker_resolved=False" in result.output
    assert "valid_until_window_blocker_resolved=False" in result.output
    assert "candidate_search_enabled=False" in result.output
    assert "observation_enabled=False" in result.output
    assert "paper_shadow_enabled=False" in result.output
    assert "production_enabled=False" in result.output
    assert "broker_enabled=False" in result.output
    assert "input_gap_count=7" in result.output
    assert "as_of_gap_count=2" in result.output
    assert "as_of_remediated_count=2" in result.output
    assert "remaining_blocked_or_gap_count=7" in result.output
    assert "contract_ready_count=0" in result.output
    assert f"next_route={impl.NEXT_ROUTE}" in result.output
    assert (output_root / "as_of_remediation_result.json").exists()
    assert (output_root / "as_of_contract_metadata.json").exists()


def test_growth_tilt_engine_as_of_semantics_remediation_missing_source_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "blocked"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_as_of_semantics_remediation(
        **{
            **_source_kwargs(paths),
            "source_2411_remediation_plan_result_path": tmp_path / "missing.json",
        },
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.BLOCKED_SOURCE_STATUS
    assert payload["as_of_remediation_completed"] is False
    assert payload["as_of_gap_count"] == 0
    assert any(
        "missing source artifact: remediation_plan_result_2411" in error
        for error in payload["source_validation_errors"]
    )
    assert payload["growth_tilt_engine_blocker_resolved"] is False
    assert payload["candidate_search_enabled"] is False
    assert (output_root / "as_of_remediation_result.json").exists()


def test_growth_tilt_engine_as_of_semantics_remediation_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_as_of_semantics_remediation"]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-as-of-semantics-remediation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("as_of_remediation_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2413_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "growth_tilt_engine_as_of_semantics_remediation" in catalog
    assert "growth-tilt-engine-as-of-semantics-remediation" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    items = _remediation_items()
    mapping_rows = _mapping_rows()
    remediation_plan = {
        "schema_version": "growth_tilt_engine_contract_gap_remediation_plan.v1",
        "engine_id": "growth_tilt_engine",
        "gap_count": 7,
        "ordered_remediation_items": items,
        "remediation_plan_validation": {
            "valid": True,
            "unclassified_remediation_item_count": 0,
            "silent_gap_resolution_count": 0,
            "silent_blocker_downgrade_count": 0,
        },
        "production_effect": "none",
        "broker_action": "none",
    }
    result = {
        "task_id": m2411.TASK_ID,
        "status": m2411.READY_STATUS,
        "gap_count": 7,
        "remediation_item_count": 7,
        "contract_gap_remediation_plan": remediation_plan,
        "ordered_remediation_items": items,
        "unclassified_remediation_item_count": 0,
        "silent_gap_resolution_count": 0,
        "silent_blocker_downgrade_count": 0,
        "recommended_next_research_task": m2411.NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in m2411.SAFETY_FALSE_FIELDS},
    }
    mapping = {
        "task_id": m2410.TASK_ID,
        "status": m2410.READY_STATUS,
        "known_source_feature_count": 10,
        "source_feature_contract_mapping": {"mapping_rows": mapping_rows},
        "production_effect": "none",
        "broker_action": "none",
    }
    paths = {
        "remediation_plan_result_2411": root / "remediation_plan_result.json",
        "contract_gap_remediation_plan_2411": root / "contract_gap_plan.json",
        "ordered_remediation_items_2411": root / "ordered_items.json",
        "unresolved_blocker_summary_2411": root / "unresolved_summary.json",
        "research_doc_2411": root / "remediation_doc.md",
        "mapping_result_2410": root / "mapping_result.json",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    _write_json(paths["remediation_plan_result_2411"], result)
    _write_json(
        paths["contract_gap_remediation_plan_2411"],
        {
            "status": m2411.READY_STATUS,
            "contract_gap_remediation_plan": remediation_plan,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["ordered_remediation_items_2411"],
        {
            "status": m2411.READY_STATUS,
            "ordered_remediation_items": items,
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
                "candidate_search_enabled": False,
                "production_enabled": False,
                "broker_enabled": False,
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(paths["mapping_result_2410"], mapping)
    paths["research_doc_2411"].write_text(
        f"# 2411\n\nstatus：`{m2411.READY_STATUS}`\n",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        """
schema_version: report_registry.v1
reports:
  - report_id: growth_tilt_engine_contract_gap_remediation_plan
    command: aits research strategies growth-tilt-engine-contract-gap-remediation-plan
    artifact_selection_policy: latest_available
    required_for_daily_reading: false
    artifact_globs:
      - outputs/research_strategies/growth_tilt_engine_contract_gap_remediation_plan/*.json
    production_effect: none
    broker_action: none
""",
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        "growth-tilt-engine-contract-gap-remediation-plan\n",
        encoding="utf-8",
    )
    return paths


def _remediation_items() -> list[dict[str, object]]:
    return [
        _item("equal_risk_baseline_weights", "source_traceability_required", 1),
        _item("target_vol_policy", "source_traceability_required", 2),
        _item("trend_features", "source_traceability_required", 3),
        _item("drawdown_features", "as_of_semantics_required", 4),
        _item("volatility_inputs", "as_of_semantics_required", 5),
        _item("execution_signal_validity_policy", "validity_dependency_required", 6),
        _item("growth_tilt_engine_signal_artifact", "blocked_pending_prior_remediation", 7),
    ]


def _item(feature_id: str, category: str, order: int) -> dict[str, object]:
    return {
        "remediation_order": order,
        "feature_id": feature_id,
        "source_feature_name": feature_id,
        "current_mapping_status": (
            "missing_as_of_semantics"
            if category == "as_of_semantics_required"
            else "missing_source_traceability"
        ),
        "remediation_category": category,
        "missing_as_of_semantics": category == "as_of_semantics_required",
        "missing_source_traceability": category == "source_traceability_required",
        "missing_validity_dependency": category == "validity_dependency_required",
        "required_upstream_artifact": f"source:{feature_id}",
        "blocks_contract_ready": True,
        "blocks_pit_gate": True,
        "gap_resolved_in_2411": False,
        "blocker_downgraded_in_2411": False,
    }


def _mapping_rows() -> list[dict[str, object]]:
    return [
        _row("equal_risk_baseline_weights", "missing_source_traceability"),
        _row("target_vol_policy", "missing_source_traceability"),
        _row("trend_features", "missing_source_traceability"),
        _row("drawdown_features", "missing_as_of_semantics"),
        _row("volatility_inputs", "missing_as_of_semantics"),
        _row("execution_signal_validity_policy", "blocked_unresolved"),
        _row("growth_tilt_engine_signal_artifact", "blocked_unresolved"),
    ]


def _row(feature_id: str, status: str) -> dict[str, object]:
    return {
        "feature_id": feature_id,
        "feature_name": feature_id,
        "feature_type": "TECHNICAL_FEATURES",
        "mapping_status": status,
        "mapping_status_reasons": [status],
        "as_of_semantics": "missing",
        "traceability_status": "missing",
        "validity_dependency": "none_identified_in_2410",
        "pit_eligibility": "APPROXIMATE_PIT",
        "upstream_artifact_or_registry_reference": f"source:{feature_id}",
        "contract_payload": {
            "lookback_window": f"{feature_id}_lookback",
            "forward_window_used": False,
        },
    }


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2411_remediation_plan_result_path": (
            paths["remediation_plan_result_2411"]
        ),
        "source_2411_contract_gap_remediation_plan_path": (
            paths["contract_gap_remediation_plan_2411"]
        ),
        "source_2411_ordered_remediation_items_path": (
            paths["ordered_remediation_items_2411"]
        ),
        "source_2411_unresolved_blocker_summary_path": (
            paths["unresolved_blocker_summary_2411"]
        ),
        "source_2411_research_doc_path": paths["research_doc_2411"],
        "source_2410_mapping_result_path": paths["mapping_result_2410"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "remediation_plan_result_2411": "--source-2411-remediation-plan-result",
        "contract_gap_remediation_plan_2411": (
            "--source-2411-contract-gap-remediation-plan"
        ),
        "ordered_remediation_items_2411": "--source-2411-ordered-remediation-items",
        "unresolved_blocker_summary_2411": "--source-2411-unresolved-blocker-summary",
        "research_doc_2411": "--source-2411-research-doc",
        "mapping_result_2410": "--source-2410-mapping-result",
        "report_registry": "--report-registry",
        "artifact_catalog": "--artifact-catalog",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
