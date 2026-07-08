from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_regime_slice_attribution_review as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_regime_slice_attribution_review as regime,
)


def test_regime_slice_attribution_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == regime.READY_STATUS
    assert payload["source_2436_ready"] is True
    assert payload["source_2432_gauntlet_ready"] is True
    assert payload["candidate_set_regime_slice_contract_ready"] is True
    assert payload["candidate_set_required_metrics_ready"] is True
    assert payload["regime_slice_attribution_review_ready"] is True
    assert payload["regime_slice_attribution_matrix_ready"] is True
    assert payload["candidate_status_by_regime_ready"] is True
    assert payload["regime_robustness_score"] == 0.0
    assert payload["single_regime_dependency_detected"] is False
    assert payload["single_regime_dependency_assessed"] is False
    assert payload["regime_pass_count"] == 0
    assert payload["regime_fail_count"] == 0
    assert payload["regime_inconclusive_count"] == len(regime.RECOMMENDED_REGIME_SLICES)
    assert set(payload["candidate_status_by_regime"].values()) == {"inconclusive"}
    assert payload["component_value_found"] is False
    assert payload["candidate_status"] == "needs_pit"
    assert payload["computed_new_metrics"] is False
    assert payload["regime_attribution_run"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == regime.NEXT_ROUTE


def test_regime_slice_attribution_blocks_when_2436_not_ready() -> None:
    sources = _source_documents()
    sources["source_2436_parameter_plateau_study"]["parameter_plateau_study_ready"] = (
        False
    )

    payload = _build_payload(sources)

    assert payload["status"] == regime.BLOCKED_STATUS
    assert payload["source_2436_ready"] is False
    assert "source_2436_parameter_plateau_study_ready" in payload["evidence_gap_ids"]
    assert payload["candidate_status_by_regime"]["growth_bull"] == "inconclusive"
    assert payload["recommended_next_research_task"] == regime.BLOCKED_ROUTE


def test_regime_slice_attribution_blocks_missing_candidate_set_slice() -> None:
    sources = _source_documents()
    sources["candidate_set_2432"]["regime_slice_check"]["slices"] = []

    payload = _build_payload(sources)

    assert payload["status"] == regime.BLOCKED_STATUS
    assert payload["candidate_set_regime_slice_contract_ready"] is False
    assert "candidate_set_regime_slice_contract_ready" in payload["evidence_gap_ids"]
    assert payload["regime_attribution_run"] is False


def test_regime_slice_attribution_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "regime"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_regime_slice_attribution_review(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == regime.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "regime_slice_attribution_matrix_json",
        "candidate_status_by_regime_json",
        "no_effect_boundary_json",
        "markdown_path",
        "regime_slice_attribution_matrix_markdown",
        "candidate_status_by_regime_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_regime_slice_attribution_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "regime_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-regime-slice-attribution-review",
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
    assert regime.READY_STATUS in result.output
    assert "regime_robustness_score=0.0" in result.output
    assert "single_regime_dependency_detected=false" in result.output
    assert "single_regime_dependency_assessed=false" in result.output
    assert "regime_pass_count=0" in result.output
    assert "regime_fail_count=0" in result.output
    assert "regime_inconclusive_count=9" in result.output
    assert "all_recommended_regime_status_inconclusive=true" in result.output
    assert "candidate_status=needs_pit" in result.output
    assert "regime_attribution_run=false" in result.output
    assert "computed_new_metrics=false" in result.output
    assert "fresh_market_data_read=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={regime.NEXT_ROUTE}" in result.output
    assert (output_root / "regime_slice_attribution_review_result.json").exists()


def test_regime_slice_attribution_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2436"].unlink()

    payload = impl.run_growth_tilt_regime_slice_attribution_review(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == regime.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["regime_slice_attribution_review_ready"] is False
    assert payload["evidence_gap_ids"] == ["source_artifact_availability"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == regime.BLOCKED_ROUTE


def test_regime_slice_attribution_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[regime.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-regime-slice-attribution-review"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "regime_slice_attribution_review_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2438_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert regime.REPORT_TYPE in catalog
    assert "growth-tilt-regime-slice-attribution-review" in system_flow
    assert regime.READY_STATUS in system_flow
    assert regime.NEXT_ROUTE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return regime.build_growth_tilt_regime_slice_attribution_review(
        sources["source_2436_parameter_plateau_study"],
        sources["source_2432_candidate_gauntlet"],
        sources["candidate_set_2432"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2436_parameter_plateau_study": _source_2436(),
        "source_2432_candidate_gauntlet": _source_2432(),
        "candidate_set_2432": _candidate_set_2432(),
        "report_registry": {
            "reports": [
                {"report_id": report_id} for report_id in regime.REQUIRED_REPORT_IDS
            ]
        },
        "artifact_catalog_text": "\n".join(regime.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(regime.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "plateau": "regime slice attribution",
            "route": "regime slice attribution",
            "gauntlet": "regime slice check",
            "candidate_set": "regime slice check",
        },
    }


def _source_2436() -> dict[str, Any]:
    return {
        "status": regime.EXPECTED_2436_STATUS,
        "parameter_plateau_study_ready": True,
        "recommended_next_research_task": regime.EXPECTED_2436_NEXT_ROUTE,
        "parameter_sweep_run": False,
    }


def _source_2432() -> dict[str, Any]:
    return {
        "status": regime.EXPECTED_2432_STATUS,
        "candidate_set_ready": True,
        "candidate_set_id": regime.EXPECTED_CANDIDATE_SET_ID,
        "candidates_tested": 0,
        "candidate_gauntlet_run": False,
        "candidate_set_section_status": {"regime_slice_check": True},
    }


def _candidate_set_2432() -> dict[str, Any]:
    return {
        "candidate_set_id": regime.EXPECTED_CANDIDATE_SET_ID,
        "regime_slice_check": {
            "ready": True,
            "slices": list(regime.REQUIRED_CANDIDATE_SET_REGIME_SLICES),
        },
        "unified_metrics": {
            "metrics": [{"metric_id": metric_id} for metric_id in regime.REQUIRED_METRIC_IDS]
        },
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2436": root / "parameter_plateau_study_result.json",
        "source_2432": root / "candidate_gauntlet_result.json",
        "candidate_set_2432": root / "candidate_set_2432.yaml",
        "parameter_plateau_study_doc": root / "parameter_plateau_study.md",
        "route_2437_doc": root / "dynamic_strategy_2437_route.md",
        "candidate_gauntlet_doc": root / "candidate_gauntlet.md",
        "candidate_set_2432_doc": root / "candidate_set_2432.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    _write_json(paths["source_2436"], sources["source_2436_parameter_plateau_study"])
    _write_json(paths["source_2432"], sources["source_2432_candidate_gauntlet"])
    paths["candidate_set_2432"].write_text(
        json.dumps(sources["candidate_set_2432"], ensure_ascii=False),
        encoding="utf-8",
    )
    paths["parameter_plateau_study_doc"].write_text(
        "regime slice attribution",
        encoding="utf-8",
    )
    paths["route_2437_doc"].write_text("regime slice attribution", encoding="utf-8")
    paths["candidate_gauntlet_doc"].write_text("regime slice check", encoding="utf-8")
    paths["candidate_set_2432_doc"].write_text("regime slice check", encoding="utf-8")
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in regime.REQUIRED_REPORT_IDS
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
        "source_2436_parameter_plateau_study_path": paths["source_2436"],
        "source_2432_candidate_gauntlet_path": paths["source_2432"],
        "candidate_set_2432_path": paths["candidate_set_2432"],
        "parameter_plateau_study_doc_path": paths["parameter_plateau_study_doc"],
        "route_2437_doc_path": paths["route_2437_doc"],
        "candidate_gauntlet_doc_path": paths["candidate_gauntlet_doc"],
        "candidate_set_2432_doc_path": paths["candidate_set_2432_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2436-parameter-plateau-study",
        str(paths["source_2436"]),
        "--source-2432-candidate-gauntlet",
        str(paths["source_2432"]),
        "--candidate-set-2432",
        str(paths["candidate_set_2432"]),
        "--parameter-plateau-study-doc",
        str(paths["parameter_plateau_study_doc"]),
        "--route-2437-doc",
        str(paths["route_2437_doc"]),
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
