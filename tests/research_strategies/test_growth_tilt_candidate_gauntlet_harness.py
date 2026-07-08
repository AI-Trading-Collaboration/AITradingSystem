from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import dynamic_strategy_growth_tilt_candidate_gauntlet_harness as impl
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import growth_tilt_candidate_gauntlet_harness as harness


def test_candidate_gauntlet_harness_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == harness.READY_STATUS
    assert payload["source_2431_ready"] is True
    assert payload["candidate_set_ready"] is True
    assert payload["candidate_set_id"] == harness.CANDIDATE_SET_ID
    assert payload["harness_ready"] is True
    assert payload["baseline_ready"] is True
    assert payload["metrics_ready"] is True
    assert payload["kill_criteria_ready"] is True
    assert payload["promotion_criteria_ready"] is True
    assert payload["regime_slices_ready"] is True
    assert payload["parameter_plateau_check_ready"] is True
    assert payload["ablation_output_ready"] is True
    assert payload["candidates_tested"] == 0
    assert payload["candidate_gauntlet_run"] is False
    assert payload["candidate_batch_screen_run"] is False
    assert payload["new_investment_threshold_values_set"] is False
    assert payload["criteria_threshold_values_all_null"] is True
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == harness.NEXT_ROUTE


def test_candidate_gauntlet_harness_blocks_when_2431_not_ready() -> None:
    sources = _source_documents()
    sources["source_2431_existing_candidate_evidence_matrix"]["candidate_gauntlet_run"] = True

    payload = _build_payload(sources)

    assert payload["status"] == harness.BLOCKED_STATUS
    assert payload["source_2431_ready"] is False
    assert (
        "source_2431_existing_candidate_evidence_matrix_ready"
        in payload["contract_gap_ids"]
    )
    assert payload["recommended_next_research_task"] == harness.BLOCKED_ROUTE


def test_candidate_gauntlet_harness_requires_metric_contract() -> None:
    sources = _source_documents()
    sources["candidate_set"]["unified_metrics"]["metrics"] = []

    payload = _build_payload(sources)

    assert payload["status"] == harness.BLOCKED_STATUS
    assert payload["metrics_ready"] is False
    assert "candidate_set_sections_ready" in payload["contract_gap_ids"]
    assert "candidate_set_metric_coverage" in payload["contract_gap_ids"]


def test_candidate_gauntlet_harness_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "candidate_gauntlet"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_candidate_gauntlet_harness(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == harness.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["candidate_set_id"] == harness.CANDIDATE_SET_ID
    assert payload["candidates_tested"] == 0
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "candidate_set_snapshot_json",
        "baseline_contract_json",
        "metric_contract_json",
        "criteria_contract_json",
        "regime_plateau_ablation_contract_json",
        "no_effect_boundary_json",
        "markdown_path",
        "candidate_set_snapshot_markdown",
        "baseline_contract_markdown",
        "metric_contract_markdown",
        "criteria_contract_markdown",
        "regime_plateau_ablation_contract_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_candidate_gauntlet_harness_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "candidate_gauntlet_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-candidate-gauntlet",
            *_source_args(paths),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "260"},
        terminal_width=260,
    )

    assert result.exit_code == 0, result.output
    assert harness.READY_STATUS in result.output
    assert "candidate_set_id=growth_tilt_batch_2432" in result.output
    assert "harness_ready=true" in result.output
    assert "baseline_ready=true" in result.output
    assert "metrics_ready=true" in result.output
    assert "kill_criteria_ready=true" in result.output
    assert "promotion_criteria_ready=true" in result.output
    assert "regime_slices_ready=true" in result.output
    assert "parameter_plateau_check_ready=true" in result.output
    assert "ablation_output_ready=true" in result.output
    assert "candidates_tested=0" in result.output
    assert "candidate_gauntlet_run=false" in result.output
    assert "candidate_batch_screen_run=false" in result.output
    assert "new_investment_threshold_values_set=false" in result.output
    assert "threshold_policy_required_for_execution=true" in result.output
    assert "criteria_threshold_values_all_null=true" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={harness.NEXT_ROUTE}" in result.output
    assert (output_root / "candidate_gauntlet_result.json").exists()


def test_candidate_gauntlet_harness_missing_candidate_set_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["candidate_set"].unlink()

    payload = impl.run_growth_tilt_candidate_gauntlet_harness(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == harness.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["harness_ready"] is False
    assert payload["contract_gap_count"] == 1
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == harness.BLOCKED_ROUTE


def test_candidate_gauntlet_harness_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[harness.REPORT_TYPE]

    assert entry["command"] == "aits research strategies growth-tilt-candidate-gauntlet"
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("candidate_set_2432.yaml" in item for item in entry["artifact_globs"])
    assert any(
        "candidate_gauntlet_result.json" in item for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2433_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    candidate_set_config = Path(
        "research/configs/growth_tilt/candidate_set_2432.yaml"
    ).read_text(encoding="utf-8")
    assert harness.REPORT_TYPE in catalog
    assert "growth-tilt-candidate-gauntlet" in system_flow
    assert harness.CANDIDATE_SET_ID in system_flow
    assert harness.READY_STATUS in system_flow
    assert harness.NEXT_ROUTE in system_flow
    assert "threshold_value: null" in candidate_set_config
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return harness.build_growth_tilt_candidate_gauntlet_harness(
        sources["source_2431_existing_candidate_evidence_matrix"],
        sources["candidate_set"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2431_existing_candidate_evidence_matrix": _source_2431(),
        "candidate_set": _candidate_set(),
        "report_registry": {
            "reports": [{"report_id": report_id} for report_id in harness.REQUIRED_REPORT_IDS]
        },
        "artifact_catalog_text": "\n".join(harness.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(harness.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "existing": "growth_tilt_existing_candidate_evidence_matrix"
        },
    }


def _source_2431() -> dict[str, Any]:
    return {
        "status": harness.EXPECTED_2431_STATUS,
        "existing_candidate_evidence_matrix_ready": True,
        "candidate_count": 6,
        "promotion_candidate_found": False,
        "candidate_gauntlet_run": False,
        "recommended_next_research_task": harness.EXPECTED_2431_NEXT_ROUTE,
    }


def _candidate_set() -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_candidate_set.v1",
        "candidate_set_id": harness.CANDIDATE_SET_ID,
        "status": "ready",
        "safety_boundary": {
            "research_only": True,
            "candidate_execution_allowed": False,
            "candidate_gauntlet_run_allowed_in_2432": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "trading_advice_allowed": False,
        },
        "batch_runner": {"runner_id": "growth_tilt_batch_gauntlet_runner_v1"},
        "unified_baseline": {
            "baseline_id": "growth_tilt_prior_best_and_static_baseline_contract",
            "threshold_source": "future_screen_policy_required",
            "threshold_value": None,
        },
        "unified_metrics": {
            "metrics": [
                {"metric_id": metric_id, "computed_in_2432": False}
                for metric_id in harness.REQUIRED_METRICS
            ]
        },
        "kill_criteria": [
            {
                "criterion_id": "missing_pit_traceability_kill",
                "threshold_source": "contract_boolean",
                "threshold_value": None,
            }
        ],
        "promotion_criteria": [
            {
                "criterion_id": "positive_net_of_cost_edge_required",
                "threshold_source": "future_screen_policy_required",
                "threshold_value": None,
            }
        ],
        "parameter_plateau_check": {
            "ready": True,
            "threshold_source": "future_screen_policy_required",
            "threshold_value": None,
            "dimensions": ["trend_window", "turnover_cooldown"],
        },
        "regime_slice_check": {
            "ready": True,
            "slices": ["ai_after_chatgpt_full_window", "growth_recovery_windows"],
        },
        "ablation_output": {
            "ready": True,
            "outputs": ["full_candidate", "without_turnover_cooldown"],
        },
        "candidate_groups": [
            {
                "candidate_group_id": "growth_tilt_engine_signal_variants",
                "candidate_family": "growth_tilt_engine_signal",
                "source_candidate_ids": ["growth_tilt_engine_signal"],
                "default_2431_status": "needs_pit",
                "included_in_2432_harness": True,
            }
        ],
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2431": root / "existing_candidate_evidence_matrix.json",
        "candidate_set": root / "candidate_set_2432.yaml",
        "existing_candidate_evidence_matrix_doc": root / "existing_matrix.md",
        "existing_candidate_evidence_matrix_table_doc": root / "existing_matrix_table.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    _write_json(paths["source_2431"], sources["source_2431_existing_candidate_evidence_matrix"])
    paths["candidate_set"].write_text(
        json.dumps(sources["candidate_set"], ensure_ascii=False),
        encoding="utf-8",
    )
    paths["existing_candidate_evidence_matrix_doc"].write_text(
        "growth_tilt_existing_candidate_evidence_matrix",
        encoding="utf-8",
    )
    paths["existing_candidate_evidence_matrix_table_doc"].write_text(
        "growth_tilt_existing_candidate_evidence_matrix",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in harness.REQUIRED_REPORT_IDS
        )
        + "\n",
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        sources["artifact_catalog_text"],
        encoding="utf-8",
    )
    paths["system_flow"].write_text(sources["system_flow_text"], encoding="utf-8")
    return paths


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2431_existing_candidate_evidence_matrix_path": paths["source_2431"],
        "candidate_set_path": paths["candidate_set"],
        "existing_candidate_evidence_matrix_doc_path": paths[
            "existing_candidate_evidence_matrix_doc"
        ],
        "existing_candidate_evidence_matrix_table_doc_path": paths[
            "existing_candidate_evidence_matrix_table_doc"
        ],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2431-existing-candidate-evidence-matrix",
        str(paths["source_2431"]),
        "--candidate-set",
        str(paths["candidate_set"]),
        "--existing-candidate-evidence-matrix-doc",
        str(paths["existing_candidate_evidence_matrix_doc"]),
        "--existing-candidate-evidence-matrix-table-doc",
        str(paths["existing_candidate_evidence_matrix_table_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
