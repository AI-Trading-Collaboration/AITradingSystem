from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_turnover_cooldown_parameter_plateau_study as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_turnover_cooldown_parameter_plateau_study as plateau,
)


def test_turnover_cooldown_parameter_plateau_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == plateau.READY_STATUS
    assert payload["source_2435_ready"] is True
    assert payload["source_2432_gauntlet_ready"] is True
    assert payload["candidate_set_parameter_plateau_contract_ready"] is True
    assert payload["candidate_set_turnover_cooldown_group_ready"] is True
    assert payload["candidate_set_required_metrics_ready"] is True
    assert payload["parameter_plateau_study_ready"] is True
    assert payload["parameter_plateau_found"] is False
    assert payload["isolated_winner"] is False
    assert payload["robust_region_count"] == 0
    assert payload["component_value_found"] is False
    assert payload["candidate_status"] == "needs_pit"
    assert payload["nearby_parameter_pass_count"] == 0
    assert payload["turnover_delta"] == 0.0
    assert payload["computed_new_metrics"] is False
    assert payload["parameter_sweep_run"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == plateau.NEXT_ROUTE


def test_turnover_cooldown_parameter_plateau_blocks_when_2435_not_ready() -> None:
    sources = _source_documents()
    sources["source_2435_hit_rate_study"]["hit_rate_study_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == plateau.BLOCKED_STATUS
    assert payload["source_2435_ready"] is False
    assert "source_2435_hit_rate_study_ready" in payload["evidence_gap_ids"]
    assert payload["parameter_plateau_found"] is False
    assert payload["recommended_next_research_task"] == plateau.BLOCKED_ROUTE


def test_turnover_cooldown_parameter_plateau_blocks_missing_required_metric() -> None:
    sources = _source_documents()
    sources["candidate_set_2432"]["unified_metrics"]["metrics"] = []

    payload = _build_payload(sources)

    assert payload["status"] == plateau.BLOCKED_STATUS
    assert payload["candidate_set_required_metrics_ready"] is False
    assert "candidate_set_required_metrics_ready" in payload["evidence_gap_ids"]
    assert payload["parameter_sweep_run"] is False


def test_turnover_cooldown_parameter_plateau_wrapper_writes_outputs(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "plateau"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_turnover_cooldown_parameter_plateau_study(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == plateau.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "parameter_plateau_matrix_json",
        "turnover_cooldown_check_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "parameter_plateau_matrix_markdown",
        "turnover_cooldown_check_summary_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_turnover_cooldown_parameter_plateau_cli_deterministic(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "plateau_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-turnover-cooldown-parameter-plateau-study",
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
    assert plateau.READY_STATUS in result.output
    assert "parameter_plateau_found=false" in result.output
    assert "isolated_winner=false" in result.output
    assert "robust_region_count=0" in result.output
    assert "component_value_found=false" in result.output
    assert "candidate_status=needs_pit" in result.output
    assert "parameter_sweep_run=false" in result.output
    assert "computed_new_metrics=false" in result.output
    assert "fresh_market_data_read=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={plateau.NEXT_ROUTE}" in result.output
    assert (output_root / "parameter_plateau_study_result.json").exists()


def test_turnover_cooldown_parameter_plateau_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2435"].unlink()

    payload = impl.run_growth_tilt_turnover_cooldown_parameter_plateau_study(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == plateau.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["parameter_plateau_study_ready"] is False
    assert payload["evidence_gap_ids"] == ["source_artifact_availability"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == plateau.BLOCKED_ROUTE


def test_turnover_cooldown_parameter_plateau_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[plateau.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-turnover-cooldown-parameter-plateau-study"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "parameter_plateau_study_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2437_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert plateau.REPORT_TYPE in catalog
    assert "growth-tilt-turnover-cooldown-parameter-plateau-study" in system_flow
    assert plateau.READY_STATUS in system_flow
    assert plateau.NEXT_ROUTE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return plateau.build_growth_tilt_turnover_cooldown_parameter_plateau_study(
        sources["source_2435_hit_rate_study"],
        sources["source_2432_candidate_gauntlet"],
        sources["candidate_set_2432"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2435_hit_rate_study": _source_2435(),
        "source_2432_candidate_gauntlet": _source_2432(),
        "candidate_set_2432": _candidate_set_2432(),
        "report_registry": {
            "reports": [
                {"report_id": report_id} for report_id in plateau.REQUIRED_REPORT_IDS
            ]
        },
        "artifact_catalog_text": "\n".join(plateau.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(plateau.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "hit_rate": "turnover cooldown study",
            "gauntlet": "turnover cooldown parameter plateau",
            "candidate_set": "turnover cooldown parameter plateau",
        },
    }


def _source_2435() -> dict[str, Any]:
    return {
        "status": plateau.EXPECTED_2435_STATUS,
        "hit_rate_study_ready": True,
        "recommended_next_research_task": plateau.EXPECTED_2435_NEXT_ROUTE,
    }


def _source_2432() -> dict[str, Any]:
    return {
        "status": plateau.EXPECTED_2432_STATUS,
        "candidate_set_ready": True,
        "candidate_set_id": plateau.EXPECTED_CANDIDATE_SET_ID,
        "candidates_tested": 0,
        "candidate_gauntlet_run": False,
        "candidate_set_section_status": {"parameter_plateau_check": True},
    }


def _candidate_set_2432() -> dict[str, Any]:
    return {
        "candidate_set_id": plateau.EXPECTED_CANDIDATE_SET_ID,
        "parameter_plateau_check": {
            "ready": True,
            "dimensions": list(plateau.REQUIRED_PARAMETER_DIMENSIONS),
        },
        "unified_metrics": {
            "metrics": [{"metric_id": metric_id} for metric_id in plateau.REQUIRED_METRIC_IDS]
        },
        "candidate_groups": [
            {
                "candidate_group_id": plateau.TURNOVER_COOLDOWN_CANDIDATE_GROUP_ID,
                "default_2431_status": "component_value",
                "included_in_2432_harness": True,
            }
        ],
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2435": root / "hit_rate_study_result.json",
        "source_2432": root / "candidate_gauntlet_result.json",
        "candidate_set_2432": root / "candidate_set_2432.yaml",
        "hit_rate_study_doc": root / "hit_rate_study.md",
        "candidate_gauntlet_doc": root / "candidate_gauntlet.md",
        "candidate_set_2432_doc": root / "candidate_set_2432.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    _write_json(paths["source_2435"], sources["source_2435_hit_rate_study"])
    _write_json(paths["source_2432"], sources["source_2432_candidate_gauntlet"])
    paths["candidate_set_2432"].write_text(
        json.dumps(sources["candidate_set_2432"], ensure_ascii=False),
        encoding="utf-8",
    )
    paths["hit_rate_study_doc"].write_text("turnover cooldown study", encoding="utf-8")
    paths["candidate_gauntlet_doc"].write_text(
        "turnover cooldown parameter plateau",
        encoding="utf-8",
    )
    paths["candidate_set_2432_doc"].write_text(
        "turnover cooldown parameter plateau",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in plateau.REQUIRED_REPORT_IDS
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
        "source_2435_hit_rate_study_path": paths["source_2435"],
        "source_2432_candidate_gauntlet_path": paths["source_2432"],
        "candidate_set_2432_path": paths["candidate_set_2432"],
        "hit_rate_study_doc_path": paths["hit_rate_study_doc"],
        "candidate_gauntlet_doc_path": paths["candidate_gauntlet_doc"],
        "candidate_set_2432_doc_path": paths["candidate_set_2432_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2435-hit-rate-study",
        str(paths["source_2435"]),
        "--source-2432-candidate-gauntlet",
        str(paths["source_2432"]),
        "--candidate-set-2432",
        str(paths["candidate_set_2432"]),
        "--hit-rate-study-doc",
        str(paths["hit_rate_study_doc"]),
        "--candidate-gauntlet-doc",
        str(paths["candidate_gauntlet_doc"]),
        "--candidate-set-2432-doc",
        str(paths["candidate_set_2432_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
