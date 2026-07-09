from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_level_pit_replay_blocker_closure as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_top3_candidate_level_pit_replay_blocker_closure as closure,
)


def test_candidate_level_blocker_closure_reads_2438e_artifact() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_2438e_candidate_level_blocked"] is True
    assert payload["candidate_replay_outputs_complete"] is True
    assert payload["candidate_replay_output_record_count"] == 3
    assert payload["candidate_level_blocker_count_before"] == 3
    assert payload["candidate_level_blocker_count_after"] == 0
    assert payload["candidate_replayable_after_closure_count"] == 3
    assert payload["replayability_handoff_ready"] is True
    assert payload["forward_aging_handoff_ready"] is False
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE_READY


def test_candidate_level_blocker_closure_blocks_when_2438e_is_not_candidate_blocked() -> None:
    sources = _source_documents()
    sources["source_2438e_recheck"]["status"] = (
        "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_READY"
    )

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["source_2438e_candidate_level_blocked"] is False
    assert "source_2438e_candidate_level_blocked" in payload["evidence_gap_ids"]
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE_BLOCKED


def test_candidate_level_blocker_closure_blocks_when_output_records_incomplete() -> None:
    sources = _source_documents()
    sources["source_2438e_recheck"]["candidate_replay_outputs_complete"] = False

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["candidate_replay_outputs_complete"] is False
    assert "candidate_output_records_complete" in payload["evidence_gap_ids"]


def test_candidate_level_blocker_closure_blocks_when_record_count_is_not_three() -> None:
    payload = _build_payload(_source_documents(), records=_records(2))

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["candidate_replay_output_record_count"] == 2
    assert "candidate_record_count" in payload["evidence_gap_ids"]


def test_candidate_level_blocker_closure_blocks_when_blocker_reason_missing() -> None:
    sources = _source_documents()
    blockers = sources["candidate_level_blocker_summary"]["candidate_level_blocker_summary"][
        "candidate_level_blockers"
    ]
    blockers[0]["blocker_reason"] = ""

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["each_candidate_has_prior_blocker_reason"] is False
    assert "each_candidate_has_prior_blocker_reason" in payload["evidence_gap_ids"]


def test_candidate_level_blocker_closure_blocks_when_closure_action_missing() -> None:
    attempts = [_closure_attempt(_selected_candidates()[0]["candidate_id"])]
    attempts[0]["closure_action_taken"] = ""

    payload = _build_payload(_source_documents(), closure_attempts=attempts)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["each_candidate_has_closure_action"] is False
    assert "each_candidate_has_closure_action" in payload["evidence_gap_ids"]


def test_candidate_level_blocker_closure_blocks_when_closure_evidence_ref_missing() -> None:
    attempts = [_closure_attempt(_selected_candidates()[0]["candidate_id"])]
    attempts[0]["closure_evidence_ref"] = ""

    payload = _build_payload(_source_documents(), closure_attempts=attempts)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["each_candidate_has_closure_evidence_ref"] is False
    assert "each_candidate_has_closure_evidence_ref" in payload["evidence_gap_ids"]


def test_candidate_level_blocker_closure_blocks_when_after_state_missing() -> None:
    attempts = [_closure_attempt(_selected_candidates()[0]["candidate_id"])]
    attempts[0]["after_state"] = {}

    payload = _build_payload(_source_documents(), closure_attempts=attempts)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["each_candidate_has_after_state"] is False
    assert "each_candidate_has_after_state" in payload["evidence_gap_ids"]


def test_candidate_level_blocker_closure_ready_when_all_blockers_closed() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == closure.READY_STATUS
    assert payload["candidate_level_blocker_closure_ready"] is True
    assert payload["all_candidate_blockers_closed"] is True
    assert payload["candidate_level_blocker_count_after"] == 0


def test_candidate_level_blocker_closure_blocked_when_candidate_remains_open() -> None:
    attempts = [_closure_attempt(_selected_candidates()[1]["candidate_id"])]
    attempts[0]["blocker_closed"] = False
    attempts[0]["candidate_replayable_after_closure"] = False
    attempts[0]["remaining_blocker_reason"] = "Candidate source evidence unresolved."

    payload = _build_payload(_source_documents(), closure_attempts=attempts)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["candidate_level_blocker_count_after"] == 1
    assert payload["replayability_handoff_ready"] is False
    assert payload["remaining_candidate_blockers"][0]["candidate_id"] == (
        _selected_candidates()[1]["candidate_id"]
    )
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE_BLOCKED


def test_ready_status_does_not_mark_candidate_pass_or_fail() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == closure.READY_STATUS
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    for record in payload["candidate_level_blocker_closure_records"]["records"]:
        assert record["replay_status_after_closure"] == "BLOCKED"
        assert record["candidate_replay_passed_after_closure"] is False
        assert record["candidate_replay_failed_after_closure"] is False


def test_ready_status_keeps_paper_shadow_and_trading_disabled() -> None:
    payload = _build_payload(_source_documents())

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["broker_order_generated"] is False
    assert payload["portfolio_weight_mutated"] is False
    assert payload["broker_action"] == "none"


def test_candidate_level_blocker_closure_next_routes_are_deterministic() -> None:
    ready = _build_payload(_source_documents())
    attempts = [_closure_attempt(_selected_candidates()[2]["candidate_id"])]
    attempts[0]["blocker_closed"] = False
    attempts[0]["candidate_replayable_after_closure"] = False
    blocked = _build_payload(_source_documents(), closure_attempts=attempts)

    assert ready["recommended_next_research_task"] == closure.NEXT_ROUTE_READY
    assert blocked["recommended_next_research_task"] == closure.NEXT_ROUTE_BLOCKED


def test_candidate_level_blocker_closure_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "candidate_level_closure"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_top3_candidate_level_pit_replay_blocker_closure(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_passed"] is True
    assert payload["candidate_level_blocker_count_after"] == 0
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "candidate_level_blocker_closure_records_json",
        "candidate_level_before_after_matrix_json",
        "unresolved_candidate_blocker_summary_json",
        "replayability_handoff_manifest_json",
        "no_effect_boundary_json",
        "markdown_path",
        "candidate_level_blocker_closure_records_markdown",
        "candidate_level_before_after_matrix_markdown",
        "unresolved_candidate_blocker_summary_markdown",
        "replayability_handoff_manifest_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_candidate_level_blocker_closure_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "candidate_level_closure_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-top3-candidate-level-pit-replay-blocker-closure",
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
        env={"COLUMNS": "300"},
        terminal_width=300,
    )

    assert result.exit_code == 0, result.output
    assert closure.READY_STATUS in result.output
    assert "source_2438e_candidate_level_blocked=true" in result.output
    assert "candidate_replay_outputs_complete=true" in result.output
    assert "candidate_replay_output_record_count=3" in result.output
    assert "candidate_level_blocker_closure_ready=true" in result.output
    assert "candidate_level_blocker_count_before=3" in result.output
    assert "candidate_level_blocker_count_after=0" in result.output
    assert "candidate_replayable_after_closure_count=3" in result.output
    assert "replayability_handoff_ready=true" in result.output
    assert "forward_aging_handoff_ready=false" in result.output
    assert "candidate_replay_pass_count=0" in result.output
    assert "candidate_replay_fail_count=0" in result.output
    assert "candidate_replay_blocked_count=3" in result.output
    assert "paper_shadow_candidate_found=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert f"next_route={closure.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "blocker_closure_result.json").exists()


def test_candidate_level_blocker_closure_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438e"].unlink()

    payload = impl.run_growth_tilt_top3_candidate_level_pit_replay_blocker_closure(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "missing_source",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert "source_2438e_candidate_level_blocked" in payload["evidence_gap_ids"]
    assert Path(payload["artifact_paths"]["no_effect_boundary_json"]).exists()


def test_candidate_level_blocker_closure_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[closure.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-top3-candidate-level-pit-replay-blocker-closure"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    for reference in closure.REQUIRED_CATALOG_REFERENCES:
        assert reference in "\n".join(entry["artifact_globs"]) or reference.startswith(
            "aits "
        )

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert closure.REPORT_TYPE in catalog
    for reference in closure.REQUIRED_CATALOG_REFERENCES:
        assert reference in catalog
    for reference in closure.REQUIRED_SYSTEM_FLOW_REFERENCES:
        assert reference in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(
    sources: dict[str, Any],
    *,
    records: list[dict[str, Any]] | None = None,
    closure_attempts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    selected_records = records or _records(3)
    source_2438e = _source_2438e_blocked(selected_records)
    source_2438e.update(sources["source_2438e_recheck"])
    candidate_records = {
        "candidate_replay_output_records": {"records": selected_records},
    }
    return closure.build_growth_tilt_top3_candidate_level_pit_replay_blocker_closure(
        source_2438e,
        candidate_records,
        sources["candidate_level_blocker_summary"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
        closure_attempts=closure_attempts,
        as_of="2026-07-08",
    )


def _source_documents() -> dict[str, Any]:
    records = _records(3)
    return {
        "source_2438e_recheck": _source_2438e_blocked(records),
        "candidate_replay_output_records": {
            "candidate_replay_output_records": {"records": records},
        },
        "candidate_level_blocker_summary": _candidate_level_blocker_summary(records),
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
            "source_2438e_doc": "PIT replay candidate blocker closure handoff",
            "candidate_output_records_doc": (
                "PIT replay candidate blocker closure handoff"
            ),
            "candidate_level_blocker_doc": (
                "PIT replay candidate blocker closure handoff"
            ),
        },
    }


def _source_2438e_blocked(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": closure.EXPECTED_2438E_STATUS,
        "recommended_next_research_task": closure.EXPECTED_2438E_ROUTE,
        "candidate_replay_outputs_complete": True,
        "candidate_replay_output_record_count": len(records),
        "candidate_replay_pass_count": 0,
        "candidate_replay_fail_count": 0,
        "candidate_replay_blocked_count": len(records),
        "blocked_candidates": records,
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _candidate_level_blocker_summary(
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    blockers = [_candidate_blocker(record) for record in records]
    return {
        "candidate_level_blocker_summary": {
            "status": closure.EXPECTED_2438E_STATUS,
            "candidate_level_blocker_summary_ready": True,
            "candidate_level_blocker_count": len(blockers),
            "candidate_level_blockers": blockers,
            "next_route": closure.EXPECTED_2438E_ROUTE,
            "paper_shadow_enabled": False,
            "production_enabled": False,
            "broker_enabled": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    }


def _candidate_blocker(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": record["candidate_id"],
        "replay_status": "BLOCKED",
        "blocker_category": [
            "missing_metric_summary",
            "unresolved_input_dependency",
            "insufficient_pit_window",
            "unresolved_source_traceability",
            "invalid_valid_until_policy",
            "missing_outcome_linkage",
            "replay_engine_execution_gap",
        ],
        "blocker_reason": record["status_reason"]["blocker_reason"],
        "required_next_action": (
            "Close TRADING-2438F candidate-level PIT replay blockers before "
            "forward-aging handoff."
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _records(count: int) -> list[dict[str, Any]]:
    return [_record(candidate["candidate_id"]) for candidate in _selected_candidates()[:count]]


def _record(candidate_id: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "candidate_family": candidate_id,
        "replay_status": "BLOCKED",
        "source_replay_status": "blocked_replay_engine_gap",
        "as_of": "2026-07-08",
        "replay_window": "ai_after_chatgpt_pit_replay_window",
        "baseline_id": "growth_tilt_current_policy_baseline",
        "input_spec_ref": f"outputs/input_specs.json#{candidate_id}",
        "source_traceability_ref": f"outputs/source_traceability.json#{candidate_id}",
        "evidence_ref": f"outputs/pit_replay_evidence.json#{candidate_id}",
        "valid_until_policy_ref": f"outputs/valid_until.json#{candidate_id}",
        "outcome_linkage_key": (
            f"growth_tilt_pit_replay:{candidate_id}:1d,5d,10d,20d"
        ),
        "forward_aging_handoff_key": (
            f"TRADING-2439A:forward_aging_candidate_pack:{candidate_id}"
        ),
        "blocking_gap_ids": [
            "candidate_pit_replay_engine_available",
            "candidate_replay_input_specs_ready",
            "candidate_source_traceability_manifests_ready",
            "candidate_as_of_boundary_specs_ready",
            "candidate_valid_until_boundary_specs_ready",
            "candidate_outcome_linkage_specs_ready",
        ],
        "metric_summary": {
            "return_delta_vs_baseline": None,
            "max_drawdown_delta_vs_baseline": None,
            "turnover_delta_vs_baseline": None,
            "false_risk_off_delta": None,
            "missed_upside_delta": None,
            "whipsaw_delta": None,
        },
        "status_reason": {
            "pass_reason": None,
            "fail_reason": None,
            "blocker_reason": "Candidate remains BLOCKED by candidate-level PIT replay gaps.",
        },
        "paper_shadow_candidate_found": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _closure_attempt(candidate_id: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "closure_action_taken": "Close candidate-level blocker for 2438G recheck.",
        "closure_evidence_ref": f"TRADING-2438F:fixture:{candidate_id}",
        "after_state": {
            "blocker_after_state": "CLOSED_FOR_2438G_RECHECK",
            "candidate_replayable_after_closure": True,
        },
        "blocker_closed": True,
        "candidate_replayable_after_closure": True,
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


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2438e": root / "recheck_after_output_closure_result.json",
        "candidate_records": root / "candidate_replay_output_records.json",
        "candidate_blockers": root / "candidate_level_blocker_summary.json",
        "source_2438e_doc": root / "recheck_after_output_closure.md",
        "candidate_records_doc": root / "candidate_replay_output_records.md",
        "candidate_blockers_doc": root / "candidate_level_blocker_summary.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2438e"], sources["source_2438e_recheck"])
    _write_json(paths["candidate_records"], sources["candidate_replay_output_records"])
    _write_json(paths["candidate_blockers"], sources["candidate_level_blocker_summary"])
    for key, text in sources["research_doc_texts"].items():
        path_key = {
            "source_2438e_doc": "source_2438e_doc",
            "candidate_output_records_doc": "candidate_records_doc",
            "candidate_level_blocker_doc": "candidate_blockers_doc",
        }[key]
        paths[path_key].write_text(text, encoding="utf-8")
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
        "source_2438e_recheck_path": paths["source_2438e"],
        "candidate_replay_output_records_path": paths["candidate_records"],
        "candidate_level_blocker_summary_path": paths["candidate_blockers"],
        "source_2438e_doc_path": paths["source_2438e_doc"],
        "candidate_output_records_doc_path": paths["candidate_records_doc"],
        "candidate_level_blocker_doc_path": paths["candidate_blockers_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2438e-recheck",
        str(paths["source_2438e"]),
        "--candidate-replay-output-records",
        str(paths["candidate_records"]),
        "--candidate-level-blocker-summary",
        str(paths["candidate_blockers"]),
        "--source-2438e-doc",
        str(paths["source_2438e_doc"]),
        "--candidate-output-records-doc",
        str(paths["candidate_records_doc"]),
        "--candidate-level-blocker-doc",
        str(paths["candidate_blockers_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
