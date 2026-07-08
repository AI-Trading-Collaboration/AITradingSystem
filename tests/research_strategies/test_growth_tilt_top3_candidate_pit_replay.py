from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import dynamic_strategy_growth_tilt_top3_candidate_pit_replay as impl
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_top3_candidate_pit_replay as replay,
)


def test_top3_candidate_pit_replay_builder_blocks_on_replay_engine_gap() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == replay.BLOCKED_REPLAY_ENGINE_STATUS
    assert payload["source_2437_ready"] is True
    assert payload["source_2433_batch_screen_ready"] is True
    assert payload["source_2431_existing_candidate_evidence_ready"] is True
    assert payload["candidate_set_2433_ready"] is True
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_gate_passed"] is True
    assert payload["data_quality_status"] == "PASS_WITH_WARNINGS"
    assert payload["top3_candidate_selection_ready"] is True
    assert payload["pit_candidates_selected"] == 3
    assert payload["pit_candidates_tested"] == 0
    assert payload["pit_replay_pass_count"] == 0
    assert payload["pit_replay_fail_count"] == 0
    assert payload["pit_replay_blocked_count"] == 3
    assert payload["promotion_review_candidate_count"] == 0
    assert payload["candidate_pit_replay_engine_available"] is False
    assert payload["candidate_replay_input_specs_ready"] is False
    assert payload["candidate_source_traceability_manifests_ready"] is False
    assert payload["candidate_as_of_boundary_specs_ready"] is False
    assert payload["candidate_valid_until_boundary_specs_ready"] is False
    assert payload["candidate_outcome_linkage_specs_ready"] is False
    assert payload["source_traceability_verified_count"] == 0
    assert payload["as_of_boundary_verified_count"] == 0
    assert payload["valid_until_boundary_verified_count"] == 0
    assert payload["outcome_linkage_ready_count"] == 0
    assert payload["pit_replay_run"] is False
    assert payload["pit_replay_executed"] is False
    assert payload["computed_new_metrics"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == replay.BLOCKED_ROUTE
    assert {
        candidate["candidate_id"] for candidate in payload["selected_candidates"]
    } == {
        "recovery_reentry_speedup_guard",
        "false_risk_off_confirmation_relaxation",
        "missed_upside_reentry_accelerator",
    }
    assert "candidate_pit_replay_engine_available" in payload["evidence_gap_ids"]


def test_top3_candidate_pit_replay_blocks_on_data_quality_failure() -> None:
    sources = _source_documents()
    sources["data_quality_summary"] = {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": False,
        "data_quality_status": "FAIL",
        "data_quality_report_path": "outputs/reports/data_quality_fail.md",
    }

    payload = _build_payload(sources)

    assert payload["status"] == replay.BLOCKED_DATA_QUALITY_STATUS
    assert payload["data_quality_gate_passed"] is False
    assert "data_quality_gate_passed" in payload["evidence_gap_ids"]
    assert payload["pit_candidates_tested"] == 0
    assert payload["promotion_review_candidate_count"] == 0
    assert payload["recommended_next_research_task"] == replay.BLOCKED_ROUTE


def test_top3_candidate_pit_replay_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "top3_pit_replay"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_top3_candidate_pit_replay(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == replay.BLOCKED_REPLAY_ENGINE_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_gate_passed"] is True
    assert payload["pit_candidates_selected"] == 3
    assert payload["pit_candidates_tested"] == 0
    assert payload["pit_replay_blocked_count"] == 3
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "top3_candidate_selection_json",
        "pit_replay_evidence_json",
        "pit_replay_blocker_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "top3_candidate_selection_markdown",
        "pit_replay_evidence_markdown",
        "pit_replay_blocker_summary_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_top3_candidate_pit_replay_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "top3_pit_replay_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-top3-candidate-pit-replay",
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
    assert replay.BLOCKED_REPLAY_ENGINE_STATUS in result.output
    assert "data_quality_gate_executed=true" in result.output
    assert "data_quality_gate_passed=true" in result.output
    assert "data_quality_status=PASS_WITH_WARNINGS" in result.output
    assert "pit_candidates_selected=3" in result.output
    assert "pit_candidates_tested=0" in result.output
    assert "pit_replay_pass_count=0" in result.output
    assert "pit_replay_fail_count=0" in result.output
    assert "pit_replay_blocked_count=3" in result.output
    assert "promotion_review_candidate_count=0" in result.output
    assert "candidate_pit_replay_engine_available=false" in result.output
    assert "candidate_replay_input_specs_ready=false" in result.output
    assert "candidate_source_traceability_manifests_ready=false" in result.output
    assert "candidate_as_of_boundary_specs_ready=false" in result.output
    assert "candidate_valid_until_boundary_specs_ready=false" in result.output
    assert "candidate_outcome_linkage_specs_ready=false" in result.output
    assert "pit_replay_run=false" in result.output
    assert "pit_replay_executed=false" in result.output
    assert "computed_new_metrics=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={replay.BLOCKED_ROUTE}" in result.output
    assert (output_root / "top3_candidate_pit_replay_result.json").exists()


def test_top3_candidate_pit_replay_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2437"].unlink()

    payload = impl.run_growth_tilt_top3_candidate_pit_replay(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == replay.BLOCKED_EVIDENCE_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["source_2437_ready"] is False
    assert payload["top3_candidate_selection_ready"] is False
    assert payload["pit_replay_evidence_artifact_ready"] is False
    assert payload["pit_replay_blocker_summary_ready"] is True
    assert payload["no_effect_boundary_ready"] is True
    assert payload["pit_candidates_tested"] == 0
    assert payload["evidence_gap_ids"] == ["source_artifact_availability"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == replay.BLOCKED_ROUTE
    assert Path(payload["artifact_paths"]["pit_replay_evidence_json"]).exists()


def test_top3_candidate_pit_replay_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[replay.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-top3-candidate-pit-replay"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "top3_candidate_pit_replay_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2438A_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert replay.REPORT_TYPE in catalog
    assert "growth-tilt-top3-candidate-pit-replay" in system_flow
    assert replay.BLOCKED_REPLAY_ENGINE_STATUS in system_flow
    assert replay.BLOCKED_ROUTE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return replay.build_growth_tilt_top3_candidate_pit_replay(
        sources["source_2437_regime_review"],
        sources["source_2433_batch_screen"],
        sources["source_2431_existing_candidate_evidence"],
        sources["candidate_set_2433"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2437_regime_review": _source_2437(),
        "source_2433_batch_screen": _source_2433(),
        "source_2431_existing_candidate_evidence": _source_2431(),
        "candidate_set_2433": _candidate_set_2433(),
        "data_quality_summary": _data_quality_summary(),
        "report_registry": {
            "reports": [{"report_id": report_id} for report_id in replay.REQUIRED_REPORT_IDS]
        },
        "artifact_catalog_text": "\n".join(replay.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(replay.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "regime": "PIT replay pit_candidate regime slice attribution",
            "batch": "PIT replay pit_candidate batch screen",
            "candidate": "PIT replay pit_candidate evidence matrix",
        },
    }


def _source_2437() -> dict[str, Any]:
    return {
        "status": replay.EXPECTED_2437_STATUS,
        "regime_slice_attribution_review_ready": True,
        "recommended_next_research_task": replay.EXPECTED_2437_NEXT_ROUTE,
        "regime_attribution_run": False,
    }


def _source_2433() -> dict[str, Any]:
    candidates = [
        _candidate(
            "recovery_reentry_speedup_guard",
            "recovery_reentry",
            ["slow_growth_recovery_reentry", "missed_upside_without_drawdown_damage"],
        ),
        _candidate(
            "false_risk_off_confirmation_relaxation",
            "risk_off_filter",
            ["over_defensive_entry", "false_defensive_day_reduction"],
        ),
        _candidate(
            "missed_upside_reentry_accelerator",
            "missed_upside",
            ["slow_growth_recovery_reentry", "missed_upside_without_drawdown_damage"],
        ),
    ]
    return {
        "status": replay.EXPECTED_2433_STATUS,
        "batch_screen_ready": True,
        "pit_candidate_count": 3,
        "candidate_batch_screen_run": True,
        "candidate_screen_matrix": {"candidates": candidates},
    }


def _source_2431() -> dict[str, Any]:
    return {
        "status": replay.EXPECTED_2431_STATUS,
        "existing_candidate_evidence_matrix_ready": True,
        "candidate_status_summary_ready": True,
    }


def _candidate_set_2433() -> dict[str, Any]:
    candidates = [
        _candidate(
            "recovery_reentry_speedup_guard",
            "recovery_reentry",
            ["slow_growth_recovery_reentry", "missed_upside_without_drawdown_damage"],
            default_decision="pit_candidate",
        ),
        _candidate(
            "false_risk_off_confirmation_relaxation",
            "risk_off_filter",
            ["over_defensive_entry", "false_defensive_day_reduction"],
            default_decision="pit_candidate",
        ),
        _candidate(
            "missed_upside_reentry_accelerator",
            "missed_upside",
            ["slow_growth_recovery_reentry", "missed_upside_without_drawdown_damage"],
            default_decision="pit_candidate",
        ),
    ]
    return {
        "candidate_set_id": replay.EXPECTED_2433_CANDIDATE_SET_ID,
        "status": "ready",
        "candidates": candidates,
    }


def _candidate(
    candidate_id: str,
    candidate_family: str,
    research_questions: list[str],
    *,
    default_decision: str | None = None,
) -> dict[str, Any]:
    candidate = {
        "candidate_id": candidate_id,
        "candidate_family": candidate_family,
        "batch_decision": "pit_candidate",
        "research_questions": research_questions,
    }
    if default_decision is not None:
        candidate["default_batch_decision"] = default_decision
    return candidate


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
        "source_2437": root / "regime_slice_attribution_review_result.json",
        "source_2433": root / "batch_screen_result.json",
        "source_2431": root / "existing_candidate_evidence_matrix_result.json",
        "candidate_set_2433": root / "false_risk_off_missed_upside_2433.yaml",
        "regime_review_doc": root / "growth_tilt_regime_slice_attribution_review.md",
        "batch_screen_doc": root / "growth_tilt_false_risk_off_batch_screen.md",
        "existing_candidate_doc": root / "growth_tilt_existing_candidate.md",
        "candidate_set_2433_doc": root / "candidate_set_2433.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2437"], sources["source_2437_regime_review"])
    _write_json(paths["source_2433"], sources["source_2433_batch_screen"])
    _write_json(paths["source_2431"], sources["source_2431_existing_candidate_evidence"])
    paths["candidate_set_2433"].write_text(
        json.dumps(sources["candidate_set_2433"], ensure_ascii=False),
        encoding="utf-8",
    )
    paths["regime_review_doc"].write_text(
        "PIT replay pit_candidate regime slice attribution",
        encoding="utf-8",
    )
    paths["batch_screen_doc"].write_text(
        "PIT replay pit_candidate batch screen",
        encoding="utf-8",
    )
    paths["existing_candidate_doc"].write_text(
        "PIT replay pit_candidate evidence matrix",
        encoding="utf-8",
    )
    paths["candidate_set_2433_doc"].write_text(
        "PIT replay pit_candidate candidate-set",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in replay.REQUIRED_REPORT_IDS
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
        "source_2437_regime_review_path": paths["source_2437"],
        "source_2433_batch_screen_path": paths["source_2433"],
        "source_2431_existing_candidate_evidence_path": paths["source_2431"],
        "candidate_set_2433_path": paths["candidate_set_2433"],
        "regime_review_doc_path": paths["regime_review_doc"],
        "batch_screen_doc_path": paths["batch_screen_doc"],
        "existing_candidate_doc_path": paths["existing_candidate_doc"],
        "candidate_set_2433_doc_path": paths["candidate_set_2433_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2437-regime-review",
        str(paths["source_2437"]),
        "--source-2433-batch-screen",
        str(paths["source_2433"]),
        "--source-2431-existing-candidate-evidence",
        str(paths["source_2431"]),
        "--candidate-set-2433",
        str(paths["candidate_set_2433"]),
        "--regime-review-doc",
        str(paths["regime_review_doc"]),
        "--batch-screen-doc",
        str(paths["batch_screen_doc"]),
        "--existing-candidate-doc",
        str(paths["existing_candidate_doc"]),
        "--candidate-set-2433-doc",
        str(paths["candidate_set_2433_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
