from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_paper_shadow_candidate_promotion_review as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_paper_shadow_candidate_promotion_review as review,
)


def test_paper_shadow_candidate_promotion_review_blocks_on_forward_aging_gate() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == review.BLOCKED_FORWARD_AGING_STATUS
    assert payload["source_2431_ready"] is True
    assert payload["source_2432_ready"] is True
    assert payload["source_2434_ready"] is True
    assert payload["source_2437_ready"] is True
    assert payload["source_2438_ready"] is False
    assert payload["source_2439_forward_aging_ready"] is False
    assert payload["forward_aging_source_status"] == (
        "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE"
    )
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_gate_passed"] is True
    assert payload["promotion_review_ready"] is False
    assert payload["evidence_summary_ready"] is True
    assert payload["candidate_decision_matrix_ready"] is True
    assert payload["blocked_promotion_route_ready"] is True
    assert payload["no_effect_boundary_ready"] is True
    assert payload["forward_aging_candidate_count"] == 0
    assert payload["review_candidate_count"] == 0
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["paper_shadow_candidate_count"] == 0
    assert payload["selected_candidates"] == []
    assert payload["forward_aging_observation_started"] is False
    assert payload["forward_aging_observation_written"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == review.BLOCKED_ROUTE
    assert "source_2439_forward_aging_candidate_pack_ready" in payload[
        "evidence_gap_ids"
    ]


def test_paper_shadow_candidate_promotion_review_no_candidate_when_forward_ready() -> None:
    sources = _source_documents()
    sources["source_2438_pit_replay"] = _source_2438_ready()
    sources["source_2439_forward_pack"] = _source_2439_ready(
        paper_shadow_candidate=False
    )

    payload = _build_payload(sources)

    assert payload["status"] == review.NO_CANDIDATE_STATUS
    assert payload["promotion_review_ready"] is True
    assert payload["review_candidate_count"] == 1
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["paper_shadow_candidate_count"] == 0
    assert payload["candidate_decision_rows"][0]["status"] == "needs_more_forward_evidence"
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE_IF_NO_CANDIDATE


def test_paper_shadow_candidate_promotion_review_candidate_found() -> None:
    sources = _source_documents()
    sources["source_2438_pit_replay"] = _source_2438_ready()
    sources["source_2439_forward_pack"] = _source_2439_ready(
        paper_shadow_candidate=True
    )

    payload = _build_payload(sources)

    assert payload["status"] == review.CANDIDATE_FOUND_STATUS
    assert payload["paper_shadow_candidate_found"] is True
    assert payload["paper_shadow_candidate_count"] == 1
    assert payload["selected_candidates"][0]["candidate_id"] == (
        "recovery_reentry_speedup_guard"
    )
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == (
        review.NEXT_ROUTE_IF_CANDIDATE_FOUND
    )


def test_paper_shadow_candidate_promotion_review_blocks_data_quality_failure() -> None:
    sources = _source_documents()
    sources["data_quality_summary"] = {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": False,
        "data_quality_status": "FAIL",
        "data_quality_report_path": "outputs/reports/data_quality_fail.md",
    }

    payload = _build_payload(sources)

    assert payload["status"] == review.BLOCKED_DATA_QUALITY_STATUS
    assert payload["data_quality_gate_passed"] is False
    assert "data_quality_gate_passed" in payload["evidence_gap_ids"]
    assert payload["paper_shadow_candidate_count"] == 0
    assert payload["recommended_next_research_task"] == review.BLOCKED_ROUTE


def test_paper_shadow_candidate_promotion_review_wrapper_writes_outputs(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "promotion_review"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_paper_shadow_candidate_promotion_review(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == review.BLOCKED_FORWARD_AGING_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["paper_shadow_candidate_count"] == 0
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "evidence_summary_json",
        "candidate_decision_matrix_json",
        "blocked_promotion_route_json",
        "no_effect_boundary_json",
        "markdown_path",
        "evidence_summary_markdown",
        "candidate_decision_matrix_markdown",
        "blocked_promotion_route_markdown",
        "no_effect_boundary_markdown",
        "blocked_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_paper_shadow_candidate_promotion_review_cli_deterministic(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "promotion_review_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-paper-shadow-candidate-promotion-review",
            *_source_args(paths),
            "--data-quality-summary",
            str(paths["data_quality_summary"]),
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
    assert review.BLOCKED_FORWARD_AGING_STATUS in result.output
    assert "source_2439_forward_aging_ready=false" in result.output
    assert "data_quality_gate_executed=true" in result.output
    assert "data_quality_gate_passed=true" in result.output
    assert "promotion_review_ready=false" in result.output
    assert "paper_shadow_candidate_found=false" in result.output
    assert "paper_shadow_candidate_count=0" in result.output
    assert "forward_aging_observation_started=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={review.BLOCKED_ROUTE}" in result.output
    assert (output_root / "promotion_review_result.json").exists()


def test_paper_shadow_candidate_promotion_review_missing_source_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2439"].unlink()

    payload = impl.run_growth_tilt_paper_shadow_candidate_promotion_review(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == review.BLOCKED_EVIDENCE_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["promotion_review_ready"] is False
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["evidence_gap_ids"] == ["source_artifact_availability"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == review.BLOCKED_ROUTE


def test_paper_shadow_candidate_promotion_review_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[review.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-paper-shadow-candidate-promotion-review"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("promotion_review_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2440_blocked_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert review.REPORT_TYPE in catalog
    assert "growth-tilt-paper-shadow-candidate-promotion-review" in system_flow
    assert review.BLOCKED_FORWARD_AGING_STATUS in system_flow
    assert review.BLOCKED_ROUTE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return review.build_growth_tilt_paper_shadow_candidate_promotion_review(
        sources["source_2431_existing_candidate_evidence"],
        sources["source_2432_candidate_gauntlet"],
        sources["source_2434_component_validation"],
        sources["source_2437_regime_review"],
        sources["source_2438_pit_replay"],
        sources["source_2439_forward_pack"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2431_existing_candidate_evidence": _source_2431(),
        "source_2432_candidate_gauntlet": _source_2432(),
        "source_2434_component_validation": _source_2434(),
        "source_2437_regime_review": _source_2437(),
        "source_2438_pit_replay": _source_2438_blocked(),
        "source_2439_forward_pack": _source_2439_blocked(),
        "data_quality_summary": _data_quality_summary(),
        "report_registry": {
            "reports": [{"report_id": report_id} for report_id in review.REQUIRED_REPORT_IDS]
        },
        "artifact_catalog_text": "\n".join(review.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(review.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "candidate": "paper-shadow promotion candidate evidence",
            "forward": "paper-shadow promotion forward aging",
            "pit": "paper-shadow promotion PIT replay",
        },
    }


def _source_2431() -> dict[str, Any]:
    return {
        "status": review.EXPECTED_2431_STATUS,
        "existing_candidate_evidence_matrix_ready": True,
        "candidate_status_summary_ready": True,
    }


def _source_2432() -> dict[str, Any]:
    return {
        "status": review.EXPECTED_2432_STATUS,
        "harness_ready": True,
        "candidate_gauntlet_run": False,
    }


def _source_2434() -> dict[str, Any]:
    return {
        "status": review.EXPECTED_2434_STATUS,
        "component_validation_ready": True,
        "component_value_found": True,
    }


def _source_2437() -> dict[str, Any]:
    return {
        "status": review.EXPECTED_2437_STATUS,
        "regime_slice_attribution_review_ready": True,
        "regime_attribution_run": False,
    }


def _source_2438_blocked() -> dict[str, Any]:
    return {
        "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
        "pit_replay_pass_count": 0,
        "pit_replay_executed": False,
    }


def _source_2438_ready() -> dict[str, Any]:
    return {
        "status": review.EXPECTED_2438_STATUS,
        "pit_replay_pass_count": 1,
        "pit_replay_executed": True,
    }


def _source_2439_blocked() -> dict[str, Any]:
    return {
        "status": "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE",
        "forward_aging_candidate_pack_ready": False,
        "forward_aging_candidate_count": 0,
        "recommended_next_research_task": review.BLOCKED_ROUTE,
    }


def _source_2439_ready(*, paper_shadow_candidate: bool) -> dict[str, Any]:
    return {
        "status": review.EXPECTED_2439_STATUS,
        "forward_aging_candidate_pack_ready": True,
        "forward_aging_candidate_count": 1,
        "recommended_next_research_task": review.EXPECTED_2439_NEXT_ROUTE,
        "forward_aging_candidate_pack": {
            "candidates": [
                {
                    "candidate_id": "recovery_reentry_speedup_guard",
                    "primary_value": "missed_upside_reduction",
                    "key_risk": "forward_evidence_not_matured",
                    "paper_shadow_candidate": paper_shadow_candidate,
                }
            ]
        },
    }


def _data_quality_summary() -> dict[str, Any]:
    return {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": True,
        "data_quality_status": "PASS_WITH_WARNINGS",
        "data_quality_report_path": "outputs/reports/data_quality_2026-07-08.md",
        "data_quality_as_of": "2026-07-08",
        "data_quality_error_count": 0,
        "data_quality_warning_count": 2,
        "data_quality_info_count": 12,
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2431": root / "existing_candidate_evidence_matrix_result.json",
        "source_2432": root / "candidate_gauntlet_result.json",
        "source_2434": root / "component_validation_result.json",
        "source_2437": root / "regime_slice_attribution_review_result.json",
        "source_2438": root / "top3_candidate_pit_replay_result.json",
        "source_2439": root / "forward_aging_candidate_pack_result.json",
        "source_2431_doc": root / "source_2431.md",
        "source_2432_doc": root / "source_2432.md",
        "source_2434_doc": root / "source_2434.md",
        "source_2437_doc": root / "source_2437.md",
        "source_2438_doc": root / "source_2438.md",
        "source_2439_doc": root / "source_2439.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2431"], sources["source_2431_existing_candidate_evidence"])
    _write_json(paths["source_2432"], sources["source_2432_candidate_gauntlet"])
    _write_json(paths["source_2434"], sources["source_2434_component_validation"])
    _write_json(paths["source_2437"], sources["source_2437_regime_review"])
    _write_json(paths["source_2438"], sources["source_2438_pit_replay"])
    _write_json(paths["source_2439"], sources["source_2439_forward_pack"])
    for key in (
        "source_2431_doc",
        "source_2432_doc",
        "source_2434_doc",
        "source_2437_doc",
        "source_2438_doc",
        "source_2439_doc",
    ):
        paths[key].write_text("paper-shadow promotion candidate evidence", encoding="utf-8")
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in review.REQUIRED_REPORT_IDS
        )
        + "\n",
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        sources["artifact_catalog_text"],
        encoding="utf-8",
    )
    paths["system_flow"].write_text(sources["system_flow_text"], encoding="utf-8")
    _write_json(paths["data_quality_summary"], sources["data_quality_summary"])
    return paths


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2431_existing_candidate_evidence_path": paths["source_2431"],
        "source_2432_candidate_gauntlet_path": paths["source_2432"],
        "source_2434_component_validation_path": paths["source_2434"],
        "source_2437_regime_review_path": paths["source_2437"],
        "source_2438_pit_replay_path": paths["source_2438"],
        "source_2439_forward_pack_path": paths["source_2439"],
        "source_2431_doc_path": paths["source_2431_doc"],
        "source_2432_doc_path": paths["source_2432_doc"],
        "source_2434_doc_path": paths["source_2434_doc"],
        "source_2437_doc_path": paths["source_2437_doc"],
        "source_2438_doc_path": paths["source_2438_doc"],
        "source_2439_doc_path": paths["source_2439_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2431-existing-candidate-evidence",
        str(paths["source_2431"]),
        "--source-2432-candidate-gauntlet",
        str(paths["source_2432"]),
        "--source-2434-component-validation",
        str(paths["source_2434"]),
        "--source-2437-regime-review",
        str(paths["source_2437"]),
        "--source-2438-pit-replay",
        str(paths["source_2438"]),
        "--source-2439-forward-pack",
        str(paths["source_2439"]),
        "--source-2431-doc",
        str(paths["source_2431_doc"]),
        "--source-2432-doc",
        str(paths["source_2432_doc"]),
        "--source-2434-doc",
        str(paths["source_2434_doc"]),
        "--source-2437-doc",
        str(paths["source_2437_doc"]),
        "--source-2438-doc",
        str(paths["source_2438_doc"]),
        "--source-2439-doc",
        str(paths["source_2439_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
