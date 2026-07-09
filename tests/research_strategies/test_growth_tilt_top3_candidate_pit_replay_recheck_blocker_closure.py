from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure as closure,
)


def test_blocker_closure_reads_2438c_output_gap_and_2438b_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_2438c_recheck_blocked"] is True
    assert payload["source_2438b_blocker_closure_ready"] is True
    assert payload["prior_candidate_replay_outputs_complete"] is False
    assert payload["candidate_replay_outputs_complete"] is True
    assert payload["candidate_replay_output_record_count"] == 3
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE_READY


def test_ready_path_allows_zero_pass_candidates_without_paper_shadow() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == closure.READY_STATUS
    assert payload["candidate_replay_pass_count"] == 0
    assert {row["replay_status"] for row in payload["blocked_candidates"]} == {"BLOCKED"}
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_schedule_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["automatic_execution_allowed"] is False


def test_blocker_closure_blocks_when_2438c_is_not_output_gap_blocked() -> None:
    sources = _source_documents()
    sources["source_2438c_recheck"]["candidate_replay_outputs_complete"] = True

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["source_2438c_recheck_blocked"] is False
    assert "source_2438c_recheck_blocked_by_output_gap" in payload["evidence_gap_ids"]


def test_blocker_closure_blocks_when_2438b_is_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438b_blocker_closure"]["blocker_count_after"] = 1

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["source_2438b_blocker_closure_ready"] is False
    assert "source_2438b_blocker_closure_ready" in payload["evidence_gap_ids"]


def test_blocker_closure_blocks_when_top3_ids_are_missing() -> None:
    sources = _source_documents()
    sources["source_2438_pit_replay"]["selected_candidates"] = _selected_candidates()[:2]

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["top3_candidate_ids_present"] is False
    assert "top3_candidate_ids_present" in payload["evidence_gap_ids"]


def test_blocker_closure_blocks_when_output_record_count_is_short() -> None:
    sources = _source_documents()
    records = _complete_records()[:2]

    payload = _build_payload(sources, records=records)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["candidate_replay_output_record_count"] == 2
    assert "candidate_output_record_count" in payload["evidence_gap_ids"]


def test_blocker_closure_blocks_when_replay_status_is_missing() -> None:
    records = _complete_records()
    records[0]["replay_status"] = ""

    payload = _build_payload(_source_documents(), records=records)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["each_candidate_has_replay_status"] is False
    assert "each_candidate_has_replay_status" in payload["evidence_gap_ids"]


def test_blocker_closure_blocks_when_status_reason_is_missing() -> None:
    records = _complete_records()
    records[0]["status_reason"] = {
        "pass_reason": None,
        "fail_reason": None,
        "blocker_reason": None,
    }

    payload = _build_payload(_source_documents(), records=records)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["each_candidate_has_status_reason"] is False
    assert "each_candidate_has_status_reason" in payload["evidence_gap_ids"]


@pytest.mark.parametrize(
    ("field", "flag", "requirement_id"),
    [
        (
            "input_spec_ref",
            "each_candidate_has_input_spec_ref",
            "each_candidate_has_input_spec_ref",
        ),
        (
            "source_traceability_ref",
            "each_candidate_has_source_traceability_ref",
            "each_candidate_has_source_traceability_ref",
        ),
        ("evidence_ref", "each_candidate_has_evidence_ref", "each_candidate_has_evidence_ref"),
        ("as_of", "each_candidate_has_as_of_boundary", "each_candidate_has_as_of_boundary"),
        (
            "valid_until_policy_ref",
            "each_candidate_has_valid_until_policy_ref",
            "each_candidate_has_valid_until_policy_ref",
        ),
        (
            "outcome_linkage_key",
            "each_candidate_has_outcome_linkage_key",
            "each_candidate_has_outcome_linkage_key",
        ),
        (
            "forward_aging_handoff_key",
            "each_candidate_has_forward_aging_handoff_key",
            "each_candidate_has_forward_aging_handoff_key",
        ),
    ],
)
def test_blocker_closure_blocks_when_required_record_ref_is_missing(
    field: str,
    flag: str,
    requirement_id: str,
) -> None:
    records = _complete_records()
    records[0][field] = ""

    payload = _build_payload(_source_documents(), records=records)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload[flag] is False
    assert requirement_id in payload["evidence_gap_ids"]


def test_blocker_closure_next_route_deterministic() -> None:
    ready = _build_payload(_source_documents())
    blocked_records = _complete_records()
    blocked_records[0]["input_spec_ref"] = ""
    blocked = _build_payload(_source_documents(), records=blocked_records)

    assert ready["recommended_next_research_task"] == closure.NEXT_ROUTE_READY
    assert blocked["recommended_next_research_task"] == closure.NEXT_ROUTE_BLOCKED


def test_blocker_closure_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "closure"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_passed"] is True
    assert payload["candidate_replay_output_record_count"] == 3
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "candidate_replay_output_records_json",
        "output_completeness_closure_json",
        "before_after_matrix_json",
        "remaining_output_blocker_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "candidate_replay_output_records_markdown",
        "output_completeness_closure_markdown",
        "before_after_matrix_markdown",
        "remaining_output_blocker_summary_markdown",
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
            "growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure",
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
    assert "source_2438c_recheck_blocked=true" in result.output
    assert "source_2438b_blocker_closure_ready=true" in result.output
    assert "candidate_replay_outputs_complete=true" in result.output
    assert "candidate_replay_output_record_count=3" in result.output
    assert "candidate_replay_pass_count=0" in result.output
    assert "candidate_replay_fail_count=0" in result.output
    assert "candidate_replay_blocked_count=3" in result.output
    assert "paper_shadow_candidate_found=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={closure.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "blocker_closure_result.json").exists()


def test_blocker_closure_missing_source_artifact_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438c"].unlink()

    payload = impl.run_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "missing_source",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert "source_2438c_recheck_blocked_by_output_gap" in payload["evidence_gap_ids"]
    assert Path(payload["artifact_paths"]["remaining_output_blocker_summary_json"]).exists()


def test_blocker_closure_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[closure.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure"
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
    assert "growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure" in system_flow
    assert closure.READY_STATUS in system_flow
    assert closure.BLOCKED_STATUS in system_flow
    assert closure.NEXT_ROUTE_READY in system_flow
    assert closure.NEXT_ROUTE_BLOCKED in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(
    sources: dict[str, Any],
    *,
    records: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return closure.build_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure(
        sources["source_2438c_recheck"],
        sources["source_2438b_blocker_closure"],
        sources["source_2438_pit_replay"],
        sources["pit_replay_evidence"],
        sources["pit_replay_blocker_summary"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
        candidate_replay_output_records=records,
        as_of="2026-07-08",
    )


def _source_documents() -> dict[str, Any]:
    source_2438 = _source_2438()
    return {
        "source_2438c_recheck": _source_2438c_blocked(),
        "source_2438b_blocker_closure": _source_2438b_ready(),
        "source_2438_pit_replay": source_2438,
        "pit_replay_evidence": {
            "status": source_2438["status"],
            "pit_replay_evidence": source_2438["pit_replay_evidence"],
        },
        "pit_replay_blocker_summary": {
            "status": source_2438["status"],
            "pit_replay_blocker_summary": source_2438["pit_replay_blocker_summary"],
        },
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
            "source_2438c_doc": "PIT replay output handoff",
            "source_2438b_doc": "PIT replay output handoff",
            "source_2438_doc": "PIT replay output handoff",
            "pit_replay_evidence_doc": "PIT replay output handoff",
            "pit_replay_blocker_doc": "PIT replay output handoff",
        },
    }


def _source_2438c_blocked() -> dict[str, Any]:
    return {
        "status": closure.EXPECTED_2438C_STATUS,
        "recommended_next_research_task": closure.EXPECTED_2438C_ROUTE,
        "candidate_replay_outputs_complete": False,
        "candidate_replay_pass_count": 0,
        "candidate_replay_fail_count": 0,
        "candidate_replay_blocked_count": 3,
        "selected_candidate_ids": [row["candidate_id"] for row in _selected_candidates()],
        "passing_candidates": [],
        "failed_candidates": [],
        "blocked_candidates": [
            {
                "candidate_id": row["candidate_id"],
                "replay_status": "BLOCKED",
                "blocking_gap_ids": ["candidate_replay_outputs_complete"],
            }
            for row in _selected_candidates()
        ],
        "remaining_recheck_blockers": [
            {
                "blocker_id": "candidate_replay_outputs",
                "requirement_id": "candidate_replay_outputs_complete",
            }
        ],
        "evidence_gap_ids": ["candidate_replay_outputs_complete"],
        "paper_shadow_candidate_found": False,
    }


def _source_2438b_ready() -> dict[str, Any]:
    return {
        "status": closure.EXPECTED_2438B_STATUS,
        "recommended_next_research_task": closure.EXPECTED_2438B_ROUTE,
        "blocker_closure_ready": True,
        "blocker_count_after": 0,
        "input_specs": {
            "baseline_id": "growth_tilt_current_policy_baseline",
            "replay_window": {"window_id": "ai_after_chatgpt_pit_replay_window"},
            "outcome_horizons": ["1d", "5d", "10d", "20d"],
        },
        "artifact_paths": {
            "input_specs_json": "outputs/input_specs.json",
            "source_traceability_manifest_json": "outputs/source_traceability.json",
            "valid_until_boundary_manifest_json": "outputs/valid_until.json",
        },
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _source_2438() -> dict[str, Any]:
    selected = _selected_candidates()
    rows = [
        {
            "candidate_id": candidate["candidate_id"],
            "pit_replay_status": "blocked_replay_engine_gap",
            "pit_replay_passed": False,
            "blocking_gap_ids": ["candidate_replay_outputs_complete"],
            "return_delta_vs_baseline": None,
            "max_drawdown_delta_vs_baseline": None,
            "turnover_delta_vs_baseline": None,
            "false_risk_off_delta": None,
            "missed_upside_delta": None,
            "whipsaw_delta": None,
            "production_effect": "none",
            "broker_action": "none",
        }
        for candidate in selected
    ]
    return {
        "schema_version": "growth_tilt_top3_candidate_pit_replay.v1",
        "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
        "selected_candidates": selected,
        "artifact_paths": {
            "pit_replay_evidence_json": "outputs/pit_replay_evidence.json",
        },
        "pit_replay_pass_count": 0,
        "pit_replay_fail_count": 0,
        "pit_replay_blocked_count": 3,
        "pit_replay_evidence": {
            "schema_version": "growth_tilt_top3_candidate_pit_replay_evidence.v1",
            "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
            "rows": rows,
            "production_effect": "none",
            "broker_action": "none",
        },
        "pit_replay_blocker_summary": {
            "schema_version": "growth_tilt_top3_candidate_pit_replay_blocker_summary.v1",
            "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP",
            "blocked": True,
            "blocking_gap_ids": ["candidate_replay_outputs_complete"],
            "production_effect": "none",
            "broker_action": "none",
        },
    }


def _complete_records() -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": candidate["candidate_id"],
            "candidate_family": candidate["candidate_id"],
            "replay_status": "BLOCKED",
            "as_of": "2026-07-08",
            "replay_window": "ai_after_chatgpt_pit_replay_window",
            "baseline_id": "growth_tilt_current_policy_baseline",
            "input_spec_ref": f"outputs/input_specs.json#{candidate['candidate_id']}",
            "source_traceability_ref": (
                f"outputs/source_traceability.json#{candidate['candidate_id']}"
            ),
            "evidence_ref": f"outputs/pit_replay_evidence.json#{candidate['candidate_id']}",
            "valid_until_policy_ref": f"outputs/valid_until.json#{candidate['candidate_id']}",
            "outcome_linkage_key": (
                f"growth_tilt_pit_replay:{candidate['candidate_id']}:1d,5d,10d,20d"
            ),
            "forward_aging_handoff_key": (
                f"TRADING-2439:forward_aging_candidate_pack:{candidate['candidate_id']}"
            ),
            "metric_summary": {key: None for key in closure.METRIC_KEYS},
            "status_reason": {
                "pass_reason": None,
                "fail_reason": None,
                "blocker_reason": "Candidate remains BLOCKED by replay evidence.",
            },
            "production_effect": "none",
            "broker_action": "none",
        }
        for candidate in _selected_candidates()
    ]


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
        "source_2438c": root / "pit_replay_recheck_result.json",
        "source_2438b": root / "blocker_closure_result.json",
        "source_2438": root / "top3_candidate_pit_replay_result.json",
        "pit_replay_evidence": root / "pit_replay_evidence.json",
        "pit_replay_blocker_summary": root / "pit_replay_blocker_summary.json",
        "source_2438c_doc": root / "recheck.md",
        "source_2438b_doc": root / "blocker_closure.md",
        "source_2438_doc": root / "pit_replay.md",
        "pit_replay_evidence_doc": root / "pit_replay_evidence.md",
        "pit_replay_blocker_doc": root / "pit_replay_blocker.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2438c"], sources["source_2438c_recheck"])
    _write_json(paths["source_2438b"], sources["source_2438b_blocker_closure"])
    _write_json(paths["source_2438"], sources["source_2438_pit_replay"])
    _write_json(paths["pit_replay_evidence"], sources["pit_replay_evidence"])
    _write_json(
        paths["pit_replay_blocker_summary"],
        sources["pit_replay_blocker_summary"],
    )
    for key, text in sources["research_doc_texts"].items():
        paths[key].write_text(text, encoding="utf-8")
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
        "source_2438c_recheck_path": paths["source_2438c"],
        "source_2438b_blocker_closure_path": paths["source_2438b"],
        "source_2438_pit_replay_path": paths["source_2438"],
        "pit_replay_evidence_path": paths["pit_replay_evidence"],
        "pit_replay_blocker_summary_path": paths["pit_replay_blocker_summary"],
        "source_2438c_doc_path": paths["source_2438c_doc"],
        "source_2438b_doc_path": paths["source_2438b_doc"],
        "source_2438_doc_path": paths["source_2438_doc"],
        "pit_replay_evidence_doc_path": paths["pit_replay_evidence_doc"],
        "pit_replay_blocker_doc_path": paths["pit_replay_blocker_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2438c-recheck",
        str(paths["source_2438c"]),
        "--source-2438b-blocker-closure",
        str(paths["source_2438b"]),
        "--source-2438-pit-replay",
        str(paths["source_2438"]),
        "--pit-replay-evidence",
        str(paths["pit_replay_evidence"]),
        "--pit-replay-blocker-summary",
        str(paths["pit_replay_blocker_summary"]),
        "--source-2438c-doc",
        str(paths["source_2438c_doc"]),
        "--source-2438b-doc",
        str(paths["source_2438b_doc"]),
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
