from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_persistent_candidate_pit_replay_blocker_escalation as impl,  # noqa: E501
)
from ai_trading_system.cli import app
from ai_trading_system.research_quality import (
    growth_tilt_persistent_candidate_pit_replay_blocker_escalation as escalation,
)

RUN_IMPL = impl.run_growth_tilt_persistent_candidate_pit_replay_blocker_escalation
BUILD_ESCALATION = (
    escalation.build_growth_tilt_persistent_candidate_pit_replay_blocker_escalation
)


def test_persistent_candidate_escalation_reads_2438i_and_routes_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == escalation.READY_STATUS
    assert payload["prior_status"] == escalation.EXPECTED_2438I_STATUS
    assert payload["source_2438i_blocked_recheck_ready"] is True
    assert payload["persistent_blocker_escalation_required"] is True
    assert payload["closure_history_confirmed"] is True
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    assert payload["persistent_blocked_candidate_count"] == 3
    assert payload["recommended_next_research_task"] == (
        escalation.NEXT_ROUTE_ROOT_CAUSE_REMEDIATION
    )


def test_persistent_candidate_escalation_builds_root_cause_matrix() -> None:
    payload = _build_payload(_source_documents())
    rows = payload["candidate_persistent_blocker_root_cause_matrix"]["rows"]

    assert len(rows) == 3
    assert {row["root_cause_category"] for row in rows} == {
        "replay_engine_contract_ready_but_runtime_not_executable"
    }
    assert {row["recommended_next_action"] for row in rows} == {
        "replay_runtime_materialization_remediation"
    }
    assert all(row["replay_outcome_after_escalation"] == "NOT_RECHECKED" for row in rows)
    assert all(row["eligible_for_forward_aging"] is False for row in rows)


def test_persistent_candidate_escalation_no_forward_aging_safety_boundary() -> None:
    payload = _build_payload(_source_documents())

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["no_forward_aging_safety_decision"][
        "forward_aging_handoff_ready"
    ] is False
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["broker_action"] == "none"
    assert payload["production_effect"] == "none"


def test_persistent_candidate_escalation_blocks_when_2438i_not_expected_status() -> None:
    sources = _source_documents()
    sources["source_2438i_blocked_recheck"]["status"] = "WRONG_STATUS"

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert payload["source_2438i_blocked_recheck_ready"] is False
    assert "source_2438i_blocked_recheck_ready" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_blocks_when_not_zero_zero_three() -> None:
    sources = _source_documents()
    source_2438i = sources["source_2438i_blocked_recheck"]
    source_2438i["candidate_replay_pass_count"] = 1
    source_2438i["candidate_replay_blocked_count"] = 2

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert "persistent_blocker_condition_is_zero_zero_three" in payload[
        "evidence_gap_ids"
    ]


def test_persistent_candidate_escalation_blocks_when_2438b_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438b_engine_blocker_closure"]["status"] = "BLOCKED"

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert payload["pit_replay_engine_blocker_closure_ready"] is False
    assert "source_2438b_engine_closure_ready" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_blocks_when_2438d_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438d_output_closure"]["candidate_replay_outputs_complete"] = False

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert payload["output_completeness_closure_ready"] is False
    assert "source_2438d_output_closure_ready" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_blocks_when_2438f_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438f_candidate_level_blocker_closure"][
        "candidate_level_blocker_closure_ready"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert payload["candidate_level_blocker_closure_ready"] is False
    assert "source_2438f_candidate_level_closure_ready" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_blocks_when_2438h_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438h_remaining_blocker_closure"][
        "remaining_candidate_blocker_closure_ready"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert payload["remaining_blocker_closure_ready"] is False
    assert "source_2438h_remaining_closure_ready" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_blocks_when_record_count_not_three() -> None:
    sources = _source_documents(records=_records("BLOCKED", "BLOCKED"))
    source_2438i = sources["source_2438i_blocked_recheck"]
    source_2438i["candidate_replay_blocked_count"] = 2
    source_2438i["persistent_candidate_replay_blocker_count"] = 2

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert payload["candidate_replay_output_record_count"] == 2
    assert "candidate_replay_output_records_complete" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_blocks_when_persistent_blocker_missing() -> None:
    sources = _source_documents()
    summary = sources["persistent_candidate_replay_blocker_summary"][
        "persistent_candidate_replay_blocker_summary"
    ]
    summary["persistent_candidate_replay_blockers"] = summary[
        "persistent_candidate_replay_blockers"
    ][:2]

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert "persistent_blocker_records_complete" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_blocks_when_reason_missing() -> None:
    records = _records("BLOCKED", "BLOCKED", "BLOCKED")
    for record in records:
        record["status_reason"].pop("blocker_reason", None)
    sources = _source_documents(records=records)
    for blocker in sources["persistent_candidate_replay_blocker_summary"][
        "persistent_candidate_replay_blocker_summary"
    ]["persistent_candidate_replay_blockers"]:
        blocker["blocker_reason"] = ""

    payload = _build_payload(sources)

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert "all_escalation_records_have_blocker_reason" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_routes_definition_rebuild() -> None:
    records = _records_with_materialized_metrics()
    sources = _source_documents(
        records=records,
        blocker_categories=("candidate_definition_not_replayable",),
    )

    payload = _build_payload(sources)

    assert payload["status"] == escalation.READY_STATUS
    assert payload["persistent_blocker_root_causes"][0]["root_cause_category"] == (
        "candidate_definition_not_replayable"
    )
    assert payload["recommended_next_research_task"] == (
        escalation.NEXT_ROUTE_DEFINITION_REBUILD
    )


def test_persistent_candidate_escalation_routes_manual_review_for_other_root() -> None:
    records = _records_with_materialized_metrics()
    sources = _source_documents(records=records, blocker_categories=("unknown_gap",))

    payload = _build_payload(sources)

    assert payload["status"] == escalation.READY_STATUS
    assert {row["root_cause_category"] for row in payload["persistent_blocker_root_causes"]} == {
        "other"
    }
    assert payload["recommended_next_research_task"] == (
        escalation.NEXT_ROUTE_MANUAL_REVIEW
    )


def test_persistent_candidate_escalation_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "persistent_escalation"
    docs_root = tmp_path / "docs" / "research"

    payload = RUN_IMPL(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == escalation.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["registry_catalog_docs_alignment"] is True
    for key in (
        "json_path",
        "candidate_persistent_blocker_root_cause_matrix_json",
        "repeated_closure_failure_summary_json",
        "recommended_remediation_route_json",
        "no_forward_aging_safety_decision_json",
        "markdown_path",
        "candidate_persistent_blocker_root_cause_matrix_markdown",
        "repeated_closure_failure_summary_markdown",
        "recommended_remediation_route_markdown",
        "no_forward_aging_safety_decision_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_persistent_candidate_escalation_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "persistent_escalation_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-persistent-candidate-pit-replay-blocker-escalation",
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
    assert escalation.READY_STATUS in result.output
    assert "source_2438i_blocked_recheck_ready=true" in result.output
    assert "persistent_blocker_escalation_required=true" in result.output
    assert "closure_history_confirmed=true" in result.output
    assert "candidate_replay_pass_count=0" in result.output
    assert "candidate_replay_fail_count=0" in result.output
    assert "candidate_replay_blocked_count=3" in result.output
    assert "persistent_blocked_candidate_count=3" in result.output
    assert "forward_aging_handoff_ready=false" in result.output
    assert "paper_shadow_candidate_found=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert f"next_route={escalation.NEXT_ROUTE_ROOT_CAUSE_REMEDIATION}" in result.output


def test_persistent_candidate_escalation_wrapper_missing_source_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438b_engine_blocker_closure"].unlink()

    payload = RUN_IMPL(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert payload["source_validation_error_count"] == 1
    assert "source_2438b_engine_closure_ready" in payload["evidence_gap_ids"]


def test_persistent_candidate_escalation_blocks_without_registry_catalog_docs() -> None:
    sources = _source_documents()

    payload = BUILD_ESCALATION(
        sources["source_2438i_blocked_recheck"],
        sources["persistent_candidate_replay_blocker_summary"],
        sources["source_2438h_remaining_blocker_closure"],
        sources["source_2438f_candidate_level_blocker_closure"],
        sources["source_2438d_output_closure"],
        sources["candidate_replay_output_records"],
        sources["source_2438b_engine_blocker_closure"],
        _data_quality_summary(),
        report_registry={"reports": []},
        artifact_catalog_text="",
        system_flow_text="",
        research_doc_texts={},
        as_of="2026-07-08",
    )

    assert payload["status"] == escalation.BLOCKED_STATUS
    assert payload["registry_catalog_docs_alignment"] is False
    assert "registry_catalog_docs_alignment" in payload["evidence_gap_ids"]


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return BUILD_ESCALATION(
        sources["source_2438i_blocked_recheck"],
        sources["persistent_candidate_replay_blocker_summary"],
        sources["source_2438h_remaining_blocker_closure"],
        sources["source_2438f_candidate_level_blocker_closure"],
        sources["source_2438d_output_closure"],
        sources["candidate_replay_output_records"],
        sources["source_2438b_engine_blocker_closure"],
        _data_quality_summary(),
        report_registry=_report_registry(),
        artifact_catalog_text=_artifact_catalog_text(),
        system_flow_text=_system_flow_text(),
        research_doc_texts=_research_doc_texts(),
        as_of="2026-07-08",
    )


def _source_documents(
    *,
    records: list[dict[str, Any]] | None = None,
    blocker_categories: tuple[str, ...] = (
        "missing_metric_summary",
        "unresolved_input_dependency",
        "insufficient_pit_window",
        "unresolved_source_traceability",
        "missing_outcome_linkage",
        "replay_engine_execution_gap",
    ),
) -> dict[str, Any]:
    resolved_records = records or _records("BLOCKED", "BLOCKED", "BLOCKED")
    blockers = [
        {
            "candidate_id": record["candidate_id"],
            "blocker_category": list(blocker_categories),
            "blocker_reason": f"{record['candidate_id']} remains BLOCKED after closures",
            "evidence_ref": f"evidence/{record['candidate_id']}",
            "closure_evidence_ref": f"closure/{record['candidate_id']}",
            "remaining_blocker_closure_result": "CLOSED",
            "replay_outcome_after_remaining_blocker_closure": "NOT_RECHECKED",
        }
        for record in resolved_records
    ]
    return {
        "source_2438i_blocked_recheck": {
            "status": escalation.EXPECTED_2438I_STATUS,
            "recommended_next_research_task": escalation.EXPECTED_2438I_ROUTE,
            "as_of": "2026-07-08",
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": len(resolved_records),
            "persistent_candidate_replay_blocker_count": len(blockers),
            "candidate_replay_outputs_complete": True,
            "candidate_replay_output_record_count": len(resolved_records),
            "candidate_replay_output_records": {"records": resolved_records},
            "persistent_candidate_replay_blocker_summary": {
                "persistent_candidate_replay_blocker_count": len(blockers),
                "persistent_candidate_replay_blockers": blockers,
            },
        },
        "persistent_candidate_replay_blocker_summary": {
            "persistent_candidate_replay_blocker_summary": {
                "persistent_candidate_replay_blocker_count": len(blockers),
                "persistent_candidate_replay_blockers": blockers,
            },
        },
        "source_2438h_remaining_blocker_closure": {
            "status": escalation.EXPECTED_2438H_STATUS,
            "remaining_candidate_blocker_closure_ready": True,
            "remaining_candidate_blocker_count_after": 0,
        },
        "source_2438f_candidate_level_blocker_closure": {
            "status": escalation.EXPECTED_2438F_STATUS,
            "candidate_level_blocker_closure_ready": True,
            "candidate_level_blocker_count_after": 0,
        },
        "source_2438d_output_closure": {
            "status": escalation.EXPECTED_2438D_STATUS,
            "blocker_closure_ready": True,
            "candidate_replay_outputs_complete": True,
            "candidate_replay_output_record_count": len(resolved_records),
            "candidate_replay_output_records": {"records": resolved_records},
        },
        "candidate_replay_output_records": {
            "candidate_replay_output_records": {
                "candidate_replay_output_records_ready": True,
                "records": resolved_records,
            },
        },
        "source_2438b_engine_blocker_closure": {
            "status": escalation.EXPECTED_2438B_STATUS,
            "blocker_closure_ready": True,
            "pit_replay_engine_ready": True,
            "blocker_count_after": 0,
        },
    }


def _records(*statuses: str) -> list[dict[str, Any]]:
    return [
        _record(candidate_id=f"growth_tilt_candidate_{index}", replay_status=status)
        for index, status in enumerate(statuses, start=1)
    ]


def _records_with_materialized_metrics() -> list[dict[str, Any]]:
    records = _records("BLOCKED", "BLOCKED", "BLOCKED")
    for record in records:
        record["blocking_gap_ids"] = []
        record["metric_summary"] = {
            key: 0.0
            for key in (
                "return_delta_vs_baseline",
                "max_drawdown_delta_vs_baseline",
                "turnover_delta_vs_baseline",
                "false_risk_off_delta",
                "missed_upside_delta",
                "whipsaw_delta",
            )
        }
    return records


def _record(candidate_id: str, replay_status: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "as_of": "2026-07-08",
        "replay_status": replay_status,
        "source_replay_status": "BLOCKED",
        "baseline_id": "growth_tilt_baseline_v1",
        "blocking_gap_ids": [
            "replay_engine_execution_gap",
            "missing_metric_summary",
            "unresolved_input_dependency",
            "missing_outcome_linkage",
        ],
        "status_reason": {
            "blocker_reason": f"{candidate_id} lacks executable replay metrics",
        },
        "metric_summary": {
            "return_delta_vs_baseline": None,
            "max_drawdown_delta_vs_baseline": None,
            "turnover_delta_vs_baseline": None,
            "false_risk_off_delta": None,
            "missed_upside_delta": None,
            "whipsaw_delta": None,
        },
        "evidence_ref": f"candidate_output/{candidate_id}",
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    paths = {
        "source_2438i_blocked_recheck": tmp_path / "source_2438i.json",
        "persistent_candidate_replay_blocker_summary": tmp_path / "persistent.json",
        "source_2438h_remaining_blocker_closure": tmp_path / "source_2438h.json",
        "source_2438f_candidate_level_blocker_closure": tmp_path / "source_2438f.json",
        "source_2438d_output_closure": tmp_path / "source_2438d.json",
        "candidate_replay_output_records": tmp_path / "candidate_records.json",
        "source_2438b_engine_blocker_closure": tmp_path / "source_2438b.json",
        "source_2438i_doc": tmp_path / "source_2438i.md",
        "persistent_blocker_summary_doc": tmp_path / "persistent_summary.md",
        "source_2438h_doc": tmp_path / "source_2438h.md",
        "source_2438f_doc": tmp_path / "source_2438f.md",
        "source_2438d_doc": tmp_path / "source_2438d.md",
        "candidate_output_records_doc": tmp_path / "candidate_output_records.md",
        "source_2438b_doc": tmp_path / "source_2438b.md",
        "requirement_doc": tmp_path / "requirement.md",
        "report_registry": tmp_path / "report_registry.yaml",
        "artifact_catalog": tmp_path / "artifact_catalog.md",
        "system_flow": tmp_path / "system_flow.md",
        "data_quality_summary": tmp_path / "data_quality_summary.json",
    }
    for key in (
        "source_2438i_blocked_recheck",
        "persistent_candidate_replay_blocker_summary",
        "source_2438h_remaining_blocker_closure",
        "source_2438f_candidate_level_blocker_closure",
        "source_2438d_output_closure",
        "candidate_replay_output_records",
        "source_2438b_engine_blocker_closure",
    ):
        paths[key].write_text(json.dumps(sources[key]), encoding="utf-8")
    for key in (
        "source_2438i_doc",
        "persistent_blocker_summary_doc",
        "source_2438h_doc",
        "source_2438f_doc",
        "source_2438d_doc",
        "candidate_output_records_doc",
        "source_2438b_doc",
        "requirement_doc",
    ):
        paths[key].write_text(
            "TRADING-2438J persistent root blocked forward-aging evidence",
            encoding="utf-8",
        )
    paths["report_registry"].write_text(_report_registry_yaml(), encoding="utf-8")
    paths["artifact_catalog"].write_text(_artifact_catalog_text(), encoding="utf-8")
    paths["system_flow"].write_text(_system_flow_text(), encoding="utf-8")
    paths["data_quality_summary"].write_text(
        json.dumps(_data_quality_summary()),
        encoding="utf-8",
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2438i_blocked_recheck_path": paths["source_2438i_blocked_recheck"],
        "persistent_candidate_replay_blocker_summary_path": paths[
            "persistent_candidate_replay_blocker_summary"
        ],
        "source_2438h_remaining_blocker_closure_path": paths[
            "source_2438h_remaining_blocker_closure"
        ],
        "source_2438f_candidate_level_blocker_closure_path": paths[
            "source_2438f_candidate_level_blocker_closure"
        ],
        "source_2438d_output_closure_path": paths["source_2438d_output_closure"],
        "candidate_replay_output_records_path": paths[
            "candidate_replay_output_records"
        ],
        "source_2438b_engine_blocker_closure_path": paths[
            "source_2438b_engine_blocker_closure"
        ],
        "source_2438i_doc_path": paths["source_2438i_doc"],
        "persistent_blocker_summary_doc_path": paths["persistent_blocker_summary_doc"],
        "source_2438h_doc_path": paths["source_2438h_doc"],
        "source_2438f_doc_path": paths["source_2438f_doc"],
        "source_2438d_doc_path": paths["source_2438d_doc"],
        "candidate_output_records_doc_path": paths["candidate_output_records_doc"],
        "source_2438b_doc_path": paths["source_2438b_doc"],
        "requirement_doc_path": paths["requirement_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    for option, key in (
        ("--source-2438i-blocked-recheck", "source_2438i_blocked_recheck"),
        (
            "--persistent-candidate-replay-blocker-summary",
            "persistent_candidate_replay_blocker_summary",
        ),
        ("--source-2438h-remaining-blocker-closure", "source_2438h_remaining_blocker_closure"),
        (
            "--source-2438f-candidate-level-blocker-closure",
            "source_2438f_candidate_level_blocker_closure",
        ),
        ("--source-2438d-output-closure", "source_2438d_output_closure"),
        ("--candidate-replay-output-records", "candidate_replay_output_records"),
        ("--source-2438b-engine-blocker-closure", "source_2438b_engine_blocker_closure"),
        ("--source-2438i-doc", "source_2438i_doc"),
        ("--persistent-blocker-summary-doc", "persistent_blocker_summary_doc"),
        ("--source-2438h-doc", "source_2438h_doc"),
        ("--source-2438f-doc", "source_2438f_doc"),
        ("--source-2438d-doc", "source_2438d_doc"),
        ("--candidate-output-records-doc", "candidate_output_records_doc"),
        ("--source-2438b-doc", "source_2438b_doc"),
        ("--requirement-doc", "requirement_doc"),
        ("--report-registry", "report_registry"),
        ("--artifact-catalog", "artifact_catalog"),
        ("--system-flow", "system_flow"),
    ):
        args.extend((option, str(paths[key])))
    return args


def _data_quality_summary() -> dict[str, Any]:
    return {
        "data_quality_gate_executed": True,
        "data_quality_gate_passed": True,
        "data_quality_status": "PASS_WITH_WARNINGS",
        "data_quality_report_path": "outputs/reports/data_quality_report.json",
    }


def _report_registry() -> dict[str, Any]:
    return {"reports": [{"report_id": report_id} for report_id in escalation.REQUIRED_REPORT_IDS]}


def _report_registry_yaml() -> str:
    lines = ["reports:"]
    for report_id in escalation.REQUIRED_REPORT_IDS:
        lines.append(f"  - report_id: {report_id}")
    return "\n".join(lines)


def _artifact_catalog_text() -> str:
    return "\n".join(escalation.REQUIRED_CATALOG_REFERENCES)


def _system_flow_text() -> str:
    return "\n".join(escalation.REQUIRED_SYSTEM_FLOW_REFERENCES)


def _research_doc_texts() -> dict[str, str]:
    return {
        "requirement_doc": "TRADING-2438J persistent root blocked forward-aging evidence",
    }
