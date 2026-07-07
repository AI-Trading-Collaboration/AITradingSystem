from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_gap_remediation_plan as impl,
)
from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as m2410,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_growth_tilt_engine_contract_gap_remediation_plan_builder(
    tmp_path: Path,
) -> None:
    paths = _write_2410_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "remediation"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_contract_gap_remediation_plan(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_tasks"] == ["TRADING-2410", "TRADING-2409", "TRADING-2406"]
    assert payload["source_mapping_ready"] is True
    assert payload["contract_gap_remediation_plan_ready"] is True
    assert payload["ordered_remediation_items_ready"] is True
    assert payload["validation_design_ready"] is True
    assert payload["source_blocked_or_gap_count"] == 7
    assert payload["gap_count"] == 7
    assert payload["remediation_item_count"] == 7
    assert payload["unclassified_remediation_item_count"] == 0
    assert payload["silent_gap_resolution_count"] == 0
    assert payload["silent_blocker_downgrade_count"] == 0
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    assert [
        (item["remediation_order"], item["feature_id"], item["remediation_category"])
        for item in payload["ordered_remediation_items"]
    ] == [
        (1, "equal_risk_baseline_weights", "source_traceability_required"),
        (2, "target_vol_policy", "source_traceability_required"),
        (3, "trend_features", "source_traceability_required"),
        (4, "drawdown_features", "as_of_semantics_required"),
        (5, "volatility_inputs", "as_of_semantics_required"),
        (6, "execution_signal_validity_policy", "validity_dependency_required"),
        (7, "growth_tilt_engine_signal_artifact", "blocked_pending_prior_remediation"),
    ]
    assert payload["unresolved_blocker_summary"]["growth_tilt_engine_blocker_resolved"] is False
    assert payload["unresolved_blocker_summary"]["valid_until_window_blocker_resolved"] is False

    for key in (
        "json_path",
        "contract_gap_remediation_plan_json",
        "ordered_remediation_items_json",
        "validation_design_json",
        "unresolved_blocker_summary_json",
        "markdown_path",
        "validation_design_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_contract_gap_remediation_plan_cli(
    tmp_path: Path,
) -> None:
    paths = _write_2410_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "remediation_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-contract-gap-remediation-plan",
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
    assert "growth_tilt_engine_blocker_resolved=False" in result.output
    assert "growth_tilt_engine_blocker_downgraded=False" in result.output
    assert "valid_until_window_blocker_resolved=False" in result.output
    assert "valid_until_window_blocker_downgraded=False" in result.output
    assert "candidate_search_enabled=False" in result.output
    assert "observation_enabled=False" in result.output
    assert "paper_shadow_enabled=False" in result.output
    assert "production_enabled=False" in result.output
    assert "broker_enabled=False" in result.output
    assert f"next_route={impl.NEXT_ROUTE}" in result.output
    assert (output_root / "remediation_plan_result.json").exists()
    assert (output_root / "contract_gap_remediation_plan.json").exists()
    assert (output_root / "ordered_remediation_items.json").exists()


def test_growth_tilt_engine_contract_gap_remediation_plan_missing_mapping_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_2410_sources(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "blocked"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_contract_gap_remediation_plan(
        **{
            **_source_kwargs(paths),
            "source_2410_mapping_result_path": tmp_path / "missing_mapping.json",
        },
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.BLOCKED_SOURCE_STATUS
    assert payload["source_mapping_ready"] is False
    assert payload["contract_gap_remediation_plan_ready"] is False
    assert payload["gap_count"] == 0
    assert any(
        "missing source artifact: mapping_result_2410" in error
        for error in payload["source_validation_errors"]
    )
    assert payload["growth_tilt_engine_blocker_resolved"] is False
    assert payload["candidate_search_enabled"] is False
    assert (output_root / "remediation_plan_result.json").exists()


def test_growth_tilt_engine_contract_gap_remediation_plan_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_contract_gap_remediation_plan"]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-contract-gap-remediation-plan"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("remediation_plan_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2412_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "growth_tilt_engine_contract_gap_remediation_plan" in catalog
    assert "growth-tilt-engine-contract-gap-remediation-plan" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_2410_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    rows = _mapping_rows()
    validation = {
        "schema_version": "growth_tilt_engine_source_feature_contract_mapping_validation.v1",
        "valid": True,
        "feature_count": 10,
        "contract_ready_count": 0,
        "blocked_or_gap_count": 7,
        "unclassified_feature_count": 0,
        "error_count": 0,
        "errors": [],
        "warning_count": 0,
        "warnings": [],
        "production_effect": "none",
        "broker_action": "none",
    }
    mapping = {
        "schema_version": "growth_tilt_engine_source_feature_contract_mapping.v1",
        "engine_id": "growth_tilt_engine",
        "known_source_feature_count": 10,
        "mapping_rows": rows,
        "contract_mapping_validation": validation,
        "blockers_resolved": False,
        "blockers_downgraded": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    mapping_result = {
        "task_id": m2410.TASK_ID,
        "status": m2410.READY_STATUS,
        "known_source_feature_count": 10,
        "source_feature_contract_mapping_ready": True,
        "contract_mapping_validation_ready": True,
        "unresolved_gap_summary_ready": True,
        "source_feature_contract_mapping": mapping,
        "contract_mapping_validation": validation,
        "blocked_or_gap_count": 7,
        "unclassified_feature_count": 0,
        "route_to_next_task": m2410.NEXT_ROUTE,
        "recommended_next_research_task": m2410.NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
        **{field: False for field in m2410.SAFETY_FALSE_FIELDS},
    }
    paths = {
        "mapping_result_2410": root / "mapping_result.json",
        "source_feature_contract_mapping_2410": root / "source_mapping.json",
        "contract_mapping_validation_2410": root / "mapping_validation.json",
        "unresolved_gap_summary_2410": root / "unresolved_gap_summary.json",
        "research_doc_2410": root / "growth_tilt_mapping.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    _write_json(paths["mapping_result_2410"], mapping_result)
    _write_json(
        paths["source_feature_contract_mapping_2410"],
        {
            "status": m2410.READY_STATUS,
            "source_feature_contract_mapping": mapping,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["contract_mapping_validation_2410"],
        {
            "status": m2410.READY_STATUS,
            "contract_mapping_validation": validation,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        paths["unresolved_gap_summary_2410"],
        {
            "status": m2410.READY_STATUS,
            "unresolved_gap_summary": {
                "growth_tilt_engine_blocking_gap_resolved": False,
                "growth_tilt_engine_severity_downgraded": False,
                "candidate_search_enabled": False,
                "production_enabled": False,
                "broker_enabled": False,
            },
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    paths["research_doc_2410"].write_text(
        f"# 2410\n\nstatus：`{m2410.READY_STATUS}`\n",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        """
schema_version: report_registry.v1
reports:
  - report_id: growth_tilt_engine_source_feature_contract_mapping
    command: aits research strategies growth-tilt-engine-source-feature-contract-mapping
    artifact_selection_policy: latest_available
    required_for_daily_reading: false
    artifact_globs:
      - outputs/research_strategies/growth_tilt_engine_source_feature_contract_mapping/*.json
    production_effect: none
    broker_action: none
""",
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        "growth-tilt-engine-source-feature-contract-mapping\n",
        encoding="utf-8",
    )
    return paths


def _mapping_rows() -> list[dict[str, object]]:
    return [
        _row("adjusted_prices", "mapped_with_caveats", pit_eligibility="APPROXIMATE_PIT"),
        _row("returns", "mapped_with_caveats", pit_eligibility="APPROXIMATE_PIT"),
        _row(
            "volatility_inputs",
            "missing_as_of_semantics",
            pit_eligibility="UNKNOWN_OR_APPROXIMATE_PIT",
        ),
        _row("trend_features", "missing_source_traceability"),
        _row(
            "drawdown_features",
            "missing_as_of_semantics",
            pit_eligibility="UNKNOWN_OR_APPROXIMATE_PIT",
        ),
        _row("equal_risk_baseline_weights", "missing_source_traceability"),
        _row("target_vol_policy", "missing_source_traceability"),
        _row(
            "risk_on_trend_filter_context",
            "mapped_with_caveats",
            pit_eligibility="APPROXIMATE_PIT",
        ),
        _row(
            "execution_signal_validity_policy",
            "blocked_unresolved",
            feature_type="VALIDITY_POLICY",
            validity_dependency="valid_until_window",
        ),
        _row(
            "growth_tilt_engine_signal_artifact",
            "blocked_unresolved",
            feature_type="SIGNAL_ARTIFACT_CONTRACT",
        ),
    ]


def _row(
    feature_id: str,
    mapping_status: str,
    *,
    feature_type: str = "TECHNICAL_FEATURES",
    validity_dependency: str = "none",
    pit_eligibility: str = "UNKNOWN_OR_APPROXIMATE_PIT",
) -> dict[str, object]:
    return {
        "feature_id": feature_id,
        "feature_name": feature_id.replace("_", " "),
        "feature_type": feature_type,
        "source_system": "prior_research_artifact",
        "upstream_artifact_or_registry_reference": (
            "outputs/research_strategies/growth_tilt_engine_pit_signal_remediation_plan/"
            f"{feature_id}.json"
        ),
        "as_of_semantics": "missing source cutoff",
        "pit_eligibility": pit_eligibility,
        "source_snapshot_requirement": "manifest with generated_at required",
        "traceability_status": mapping_status,
        "validity_dependency": validity_dependency,
        "mapping_status": mapping_status,
        "mapping_status_reasons": [mapping_status],
        "blocking_reason_if_unresolved": [f"{feature_id} remains unresolved"],
    }


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2410_mapping_result_path": paths["mapping_result_2410"],
        "source_2410_source_feature_contract_mapping_path": (
            paths["source_feature_contract_mapping_2410"]
        ),
        "source_2410_contract_mapping_validation_path": (
            paths["contract_mapping_validation_2410"]
        ),
        "source_2410_unresolved_gap_summary_path": (
            paths["unresolved_gap_summary_2410"]
        ),
        "source_2410_research_doc_path": paths["research_doc_2410"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "mapping_result_2410": "--source-2410-mapping-result",
        "source_feature_contract_mapping_2410": (
            "--source-2410-source-feature-contract-mapping"
        ),
        "contract_mapping_validation_2410": "--source-2410-contract-mapping-validation",
        "unresolved_gap_summary_2410": "--source-2410-unresolved-gap-summary",
        "research_doc_2410": "--source-2410-research-doc",
        "report_registry": "--report-registry",
        "artifact_catalog": "--artifact-catalog",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
