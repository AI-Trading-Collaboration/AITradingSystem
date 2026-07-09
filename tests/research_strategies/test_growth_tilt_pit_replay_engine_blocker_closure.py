from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_pit_replay_engine_blocker_closure as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_pit_replay_engine_blocker_closure as closure,
)


def test_blocker_closure_reads_2438a_blocked_artifact_not_no_candidate() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_2438a_remediation_blocked"] is True
    assert payload["not_no_candidate_status"] is True
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["blocker_count_before"] == 8
    assert payload["blocker_count_after"] == 0
    assert set(payload["closed_blockers"]) == set(closure.CORE_BLOCKERS)


def test_blocker_closure_identifies_8_prior_blockers_without_no_candidate() -> None:
    payload = _build_payload(_source_documents())

    assert payload["prior_status"] == closure.EXPECTED_2438A_STATUS
    assert payload["prior_pit_replay_status"] == closure.EXPECTED_2438_STATUS
    assert payload["source_2438_pit_replay_blocked"] is True
    assert payload["blocker_closure_ready"] is True
    assert payload["remaining_blockers"] == []


def test_blocker_closure_blocks_when_pit_replay_engine_missing() -> None:
    sources = _source_documents()
    sources["closure_artifacts"]["pit_replay_engine_contract"][
        "engine_entrypoint_exists"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "pit_replay_engine" in _remaining_blocker_ids(payload)


def test_blocker_closure_blocks_when_input_specs_missing() -> None:
    sources = _source_documents()
    sources["closure_artifacts"]["input_specs"]["baseline_id"] = ""

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "input_specs" in _remaining_blocker_ids(payload)


def test_blocker_closure_blocks_when_evidence_contract_missing() -> None:
    sources = _source_documents()
    sources["closure_artifacts"]["evidence_completeness_contract"][
        "replay_result_evidence"
    ] = "missing"

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "evidence_completeness" in _remaining_blocker_ids(payload)


def test_blocker_closure_blocks_when_source_traceability_missing() -> None:
    sources = _source_documents()
    sources["closure_artifacts"]["source_traceability_manifest"][
        "generation_command_recorded"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "source_traceability" in _remaining_blocker_ids(payload)


def test_blocker_closure_blocks_when_as_of_boundary_missing() -> None:
    sources = _source_documents()
    sources["closure_artifacts"]["as_of_boundary_manifest"][
        "future_data_allowed"
    ] = True

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "as_of_boundary" in _remaining_blocker_ids(payload)


def test_blocker_closure_blocks_when_valid_until_boundary_missing() -> None:
    sources = _source_documents()
    sources["closure_artifacts"]["valid_until_boundary_manifest"][
        "stale_signal_allowed"
    ] = True

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "valid_until_boundary" in _remaining_blocker_ids(payload)


def test_blocker_closure_blocks_when_outcome_linkage_missing() -> None:
    sources = _source_documents()
    sources["closure_artifacts"]["outcome_linkage_map"][
        "candidate_to_outcome_key_ready"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "outcome_linkage" in _remaining_blocker_ids(payload)


def test_blocker_closure_blocks_when_forward_aging_handoff_missing() -> None:
    sources = _source_documents()
    sources["closure_artifacts"]["forward_aging_handoff_contract"][
        "unresolved_handoff_gap_count"
    ] = 1

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert "forward_aging_handoff" in _remaining_blocker_ids(payload)


def test_blocker_closure_ready_when_all_8_contracts_are_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == closure.READY_STATUS
    assert payload["blocker_closure_ready"] is True
    assert payload["pit_replay_engine_ready"] is True
    assert payload["input_specs_ready"] is True
    assert payload["evidence_completeness_ready"] is True
    assert payload["source_traceability_ready"] is True
    assert payload["as_of_boundary_ready"] is True
    assert payload["valid_until_boundary_ready"] is True
    assert payload["outcome_linkage_ready"] is True
    assert payload["forward_aging_handoff_ready"] is True
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE_READY


def test_ready_path_still_has_no_paper_shadow_or_trading_advice() -> None:
    payload = _build_payload(_source_documents())

    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_schedule_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["broker_order_generated"] is False
    assert payload["portfolio_weight_mutated"] is False
    assert payload["automatic_execution_allowed"] is False


def test_blocker_closure_next_route_deterministic() -> None:
    ready = _build_payload(_source_documents())
    blocked_sources = _source_documents()
    blocked_sources["closure_artifacts"]["input_specs"]["baseline_id"] = ""
    blocked = _build_payload(blocked_sources)

    assert ready["recommended_next_research_task"] == closure.NEXT_ROUTE_READY
    assert blocked["recommended_next_research_task"] == closure.NEXT_ROUTE_BLOCKED


def test_blocker_closure_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "closure"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_pit_replay_engine_blocker_closure(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_passed"] is True
    assert payload["blocker_count_after"] == 0
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "pit_replay_engine_contract_json",
        "input_specs_json",
        "evidence_completeness_contract_json",
        "source_traceability_manifest_json",
        "as_of_boundary_manifest_json",
        "valid_until_boundary_manifest_json",
        "outcome_linkage_map_json",
        "forward_aging_handoff_contract_json",
        "blocker_before_after_matrix_json",
        "unresolved_blocker_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "blocker_before_after_markdown",
        "unresolved_blocker_summary_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_blocker_closure_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "closure_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-pit-replay-engine-blocker-closure",
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
    assert closure.READY_STATUS in result.output
    assert "source_2438a_remediation_blocked=true" in result.output
    assert "not_no_candidate_status=true" in result.output
    assert "data_quality_gate_executed=true" in result.output
    assert "data_quality_gate_passed=true" in result.output
    assert "blocker_closure_ready=true" in result.output
    assert "blocker_count_before=8" in result.output
    assert "blocker_count_after=0" in result.output
    assert "pit_replay_engine_ready=true" in result.output
    assert "input_specs_ready=true" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={closure.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "blocker_closure_result.json").exists()


def test_blocker_closure_missing_source_artifact_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438a"].unlink()

    payload = impl.run_growth_tilt_pit_replay_engine_blocker_closure(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "missing_source",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert "source_2438a_remediation_blocked" in payload["evidence_gap_ids"]
    assert Path(payload["artifact_paths"]["unresolved_blocker_summary_json"]).exists()


def test_blocker_closure_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[closure.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-pit-replay-engine-blocker-closure"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("blocker_closure_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2438C_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert closure.REPORT_TYPE in catalog
    assert "growth-tilt-pit-replay-engine-blocker-closure" in system_flow
    assert closure.READY_STATUS in system_flow
    assert closure.BLOCKED_STATUS in system_flow
    assert closure.NEXT_ROUTE_READY in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return closure.build_growth_tilt_pit_replay_engine_blocker_closure(
        sources["source_2438a_remediation"],
        sources["source_2438_pit_replay"],
        sources["pit_replay_evidence"],
        sources["pit_replay_blocker_summary"],
        sources["data_quality_summary"],
        closure_artifacts=sources["closure_artifacts"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
        as_of="2026-07-08",
    )


def _source_documents() -> dict[str, Any]:
    source_2438 = _source_2438()
    selected = source_2438["selected_candidates"]
    generation_command = (
        "aits research strategies growth-tilt-pit-replay-engine-blocker-closure "
        "--as-of 2026-07-08"
    )
    return {
        "source_2438a_remediation": {
            "status": closure.EXPECTED_2438A_STATUS,
            "recommended_next_research_task": closure.EXPECTED_2438A_ROUTE,
            "not_no_candidate_status": True,
            "paper_shadow_candidate_found": False,
            "unresolved_engine_blocker_count": 8,
            "evidence_gap_ids": list(closure.CORE_BLOCKERS),
        },
        "source_2438_pit_replay": source_2438,
        "pit_replay_evidence": {
            "status": source_2438["status"],
            "pit_replay_evidence": source_2438["pit_replay_evidence"],
        },
        "pit_replay_blocker_summary": {
            "status": source_2438["status"],
            "pit_replay_blocker_summary": source_2438["pit_replay_blocker_summary"],
        },
        "closure_artifacts": closure.default_closure_artifacts(
            selected_candidates=selected,
            as_of="2026-07-08",
            generation_command=generation_command,
        ),
        "data_quality_summary": _data_quality_summary(),
        "report_registry": {
            "reports": [
                {"report_id": report_id}
                for report_id in closure.REQUIRED_REPORT_IDS
            ]
        },
        "artifact_catalog_text": "\n".join(closure.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(closure.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "source_2438a_doc": (
                "PIT replay blocker closure forward-aging handoff no-effect"
            ),
            "source_2438_doc": (
                "PIT replay blocker closure forward-aging handoff no-effect"
            ),
        },
    }


def _source_2438() -> dict[str, Any]:
    selected = _selected_candidates()
    rows = [
        {
            "candidate_id": candidate["candidate_id"],
            "pit_replay_status": "blocked_replay_engine_gap",
            "source_traceability_verified": False,
            "as_of_boundary_verified": False,
            "valid_until_boundary_verified": False,
            "outcome_linkage_ready": False,
            "pit_replay_passed": False,
            "blocking_gap_ids": list(closure.CORE_BLOCKERS),
            "production_effect": "none",
            "broker_action": "none",
        }
        for candidate in selected
    ]
    return {
        "schema_version": "growth_tilt_top3_candidate_pit_replay.v1",
        "status": closure.EXPECTED_2438_STATUS,
        "recommended_next_research_task": (
            "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation"
        ),
        "top3_candidate_selection_ready": True,
        "selected_candidates": selected,
        "pit_candidates_tested": 0,
        "pit_replay_pass_count": 0,
        "pit_replay_blocked_count": 3,
        "pit_replay_executed": False,
        "pit_replay_evidence": {
            "schema_version": "growth_tilt_top3_candidate_pit_replay_evidence.v1",
            "status": closure.EXPECTED_2438_STATUS,
            "pit_replay_evidence_ready": True,
            "pit_replay_executed": False,
            "pit_candidates_tested": 0,
            "pit_replay_pass_count": 0,
            "pit_replay_fail_count": 0,
            "pit_replay_blocked_count": 3,
            "rows": rows,
            "production_effect": "none",
            "broker_action": "none",
        },
        "pit_replay_blocker_summary": {
            "schema_version": "growth_tilt_top3_candidate_pit_replay_blocker_summary.v1",
            "status": closure.EXPECTED_2438_STATUS,
            "blocked": True,
            "blocking_gap_ids": list(closure.CORE_BLOCKERS),
            "blocking_gap_count": 8,
            "next_route": (
                "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    }


def _selected_candidates() -> list[dict[str, Any]]:
    return [
        {"selection_rank": 1, "candidate_id": "recovery_reentry_speedup_guard"},
        {
            "selection_rank": 2,
            "candidate_id": "false_risk_off_confirmation_relaxation",
        },
        {"selection_rank": 3, "candidate_id": "missed_upside_reentry_accelerator"},
    ]


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


def _remaining_blocker_ids(payload: dict[str, Any]) -> set[str]:
    return {str(gap.get("blocker_id")) for gap in payload["remaining_blockers"]}


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2438a": root / "remediation_result.json",
        "source_2438": root / "top3_candidate_pit_replay_result.json",
        "pit_replay_evidence": root / "pit_replay_evidence.json",
        "pit_replay_blocker_summary": root / "pit_replay_blocker_summary.json",
        "source_2438a_doc": root / "remediation.md",
        "source_2438_doc": root / "pit_replay.md",
        "pit_replay_evidence_doc": root / "pit_replay_evidence.md",
        "pit_replay_blocker_doc": root / "pit_replay_blocker.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2438a"], sources["source_2438a_remediation"])
    _write_json(paths["source_2438"], sources["source_2438_pit_replay"])
    _write_json(paths["pit_replay_evidence"], sources["pit_replay_evidence"])
    _write_json(
        paths["pit_replay_blocker_summary"],
        sources["pit_replay_blocker_summary"],
    )
    for key in (
        "source_2438a_doc",
        "source_2438_doc",
        "pit_replay_evidence_doc",
        "pit_replay_blocker_doc",
    ):
        paths[key].write_text(
            "PIT replay blocker closure forward-aging handoff no-effect",
            encoding="utf-8",
        )
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}"
            for report_id in closure.REQUIRED_REPORT_IDS
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
        "source_2438a_remediation_path": paths["source_2438a"],
        "source_2438_pit_replay_path": paths["source_2438"],
        "pit_replay_evidence_path": paths["pit_replay_evidence"],
        "pit_replay_blocker_summary_path": paths["pit_replay_blocker_summary"],
        "source_2438a_doc_path": paths["source_2438a_doc"],
        "source_2438_doc_path": paths["source_2438_doc"],
        "pit_replay_evidence_doc_path": paths["pit_replay_evidence_doc"],
        "pit_replay_blocker_doc_path": paths["pit_replay_blocker_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2438a-remediation",
        str(paths["source_2438a"]),
        "--source-2438-pit-replay",
        str(paths["source_2438"]),
        "--pit-replay-evidence",
        str(paths["pit_replay_evidence"]),
        "--pit-replay-blocker-summary",
        str(paths["pit_replay_blocker_summary"]),
        "--source-2438a-doc",
        str(paths["source_2438a_doc"]),
        "--source-2438-doc",
        str(paths["source_2438_doc"]),
        "--pit-replay-evidence-doc",
        str(paths["pit_replay_evidence_doc"]),
        "--pit-replay-blocker-doc",
        str(paths["pit_replay_blocker_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
