from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation as impl,  # noqa: E501
)
from ai_trading_system.cli import app
from ai_trading_system.research_quality import (
    growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation as remediation,  # noqa: E501
)

RUN_IMPL = (
    impl.run_growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation
)
BUILD_REMEDIATION = (
    remediation.build_growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation
)


def test_root_cause_remediation_reads_2438j_and_routes_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == remediation.READY_STATUS
    assert payload["prior_status"] == remediation.EXPECTED_2438J_STATUS
    assert payload["source_2438j_escalation_ready"] is True
    assert payload["prior_root_cause_matched"] is True
    assert payload["root_cause_remediation_ready"] is True
    assert payload["replay_runtime_materialization_ready"] is True
    assert payload["candidate_replay_runtime_executable"] is True
    assert payload["candidate_replay_runtime_executable_count"] == 3
    assert payload["runtime_blocker_count_before"] == 3
    assert payload["runtime_blocker_count_after"] == 0
    assert payload["recommended_next_research_task"] == (
        remediation.NEXT_ROUTE_RECHECK_AFTER_RUNTIME_REMEDIATION
    )


def test_root_cause_remediation_blocks_when_2438j_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438j_escalation"]["status"] = "BLOCKED"

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["source_2438j_escalation_ready"] is False
    assert "source_2438j_escalation_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_when_root_cause_not_runtime_gap() -> None:
    sources = _source_documents(root_cause="candidate_definition_not_replayable")

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["prior_root_cause_matched"] is False
    assert "prior_root_cause_matched" in payload["evidence_gap_ids"]
    assert payload["recommended_next_research_task"] == (
        remediation.NEXT_ROUTE_RUNTIME_BLOCKER_CONTINUATION
    )


def test_root_cause_remediation_materializes_runtime_records() -> None:
    payload = _build_payload(_source_documents())
    records = payload["candidate_runtime_remediation_records"]

    assert len(records) == 3
    assert all(record["prior_replay_status"] == "BLOCKED" for record in records)
    assert all(record["prior_root_cause"] == remediation.EXPECTED_ROOT_CAUSE for record in records)
    assert all(record["runtime_input_materialized"] is True for record in records)
    assert all(record["replay_window_materialized"] is True for record in records)
    assert all(record["baseline_comparison_materialized"] is True for record in records)
    assert all(record["metric_materialization_ready"] is True for record in records)
    assert all(record["pass_fail_threshold_evaluator_ready"] is True for record in records)
    assert all(
        record["runtime_execution_smoke_check_status"] == "PASS" for record in records
    )
    assert all(record["replay_outcome_after_remediation"] == "NOT_RECHECKED" for record in records)


def test_root_cause_remediation_blocks_when_input_spec_missing() -> None:
    sources = _source_documents()
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "records"
    ][0].pop("input_spec_ref")

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["candidate_spec_to_runtime_input_adapter_ready"] is False
    assert "candidate_spec_to_runtime_input_adapter_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_when_runtime_entrypoint_disabled() -> None:
    sources = _source_documents()
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "records"
    ][0]["runtime_entrypoint_available"] = False

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["replay_runtime_entrypoint_ready"] is False
    assert "replay_runtime_entrypoint_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_when_replay_window_missing() -> None:
    sources = _source_documents()
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "records"
    ][0].pop("replay_window")

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["replay_window_materialization_ready"] is False
    assert "replay_window_materialization_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_when_baseline_missing() -> None:
    sources = _source_documents()
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "records"
    ][0].pop("baseline_id")

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["baseline_comparison_runtime_ready"] is False
    assert "baseline_comparison_runtime_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_when_metric_key_missing() -> None:
    sources = _source_documents()
    metric_summary = sources["candidate_replay_output_records"][
        "candidate_replay_output_records"
    ]["records"][0]["metric_summary"]
    metric_summary.pop("whipsaw_delta")

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["metric_materialization_runtime_ready"] is False
    assert "metric_materialization_runtime_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_when_threshold_evaluator_missing() -> None:
    sources = _source_documents()
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "records"
    ][0]["pass_fail_threshold_evaluator_available"] = False

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["pass_fail_threshold_evaluator_ready"] is False
    assert "pass_fail_threshold_evaluator_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_when_traceability_missing() -> None:
    sources = _source_documents()
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "records"
    ][0].pop("source_traceability_ref")

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["source_traceability_runtime_bindings_ready"] is False
    assert "source_traceability_runtime_bindings_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_when_boundary_bindings_missing() -> None:
    sources = _source_documents()
    record = sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "records"
    ][0]
    record.pop("valid_until_policy_ref")
    record.pop("outcome_linkage_key")
    record.pop("forward_aging_handoff_key")

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["valid_until_policy_bound_at_runtime"] is False
    assert payload["outcome_linkage_key_runtime_bound"] is False
    assert payload["forward_aging_handoff_key_runtime_bound"] is False


def test_root_cause_remediation_blocks_when_smoke_check_fails() -> None:
    sources = _source_documents()
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "records"
    ][0]["runtime_smoke_check_forced_status"] = "FAIL"

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "all_runtime_smoke_checks_passed" in payload["evidence_gap_ids"]
    assert payload["candidate_runtime_remediation_records"][0][
        "runtime_execution_smoke_check_status"
    ] == "FAIL"


def test_root_cause_remediation_keeps_replay_outcome_not_rechecked() -> None:
    payload = _build_payload(_source_documents())

    assert payload["candidate_replay_outcome_rechecked"] is False
    assert payload["replay_outcome_after_remediation"] == "NOT_RECHECKED"
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    assert payload["pit_replay_run"] is False
    assert payload["pit_replay_executed"] is False


def test_root_cause_remediation_keeps_no_effect_boundary() -> None:
    payload = _build_payload(_source_documents())

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["forward_aging_handoff_ready"] is False
    assert payload["forward_aging_candidate_count"] == 0
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["broker_action"] == "none"
    assert payload["production_effect"] == "none"


def test_root_cause_remediation_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "runtime_remediation"
    docs_root = tmp_path / "docs" / "research"

    payload = RUN_IMPL(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == remediation.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["registry_catalog_docs_alignment"] is True
    for key in (
        "json_path",
        "runtime_materialization_remediation_json",
        "runtime_before_after_matrix_json",
        "executable_replay_readiness_handoff_json",
        "remaining_runtime_blocker_summary_json",
        "runtime_execution_audit_trail_json",
        "markdown_path",
        "runtime_materialization_remediation_markdown",
        "runtime_before_after_matrix_markdown",
        "executable_replay_readiness_handoff_markdown",
        "remaining_runtime_blocker_summary_markdown",
        "runtime_execution_audit_trail_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_root_cause_remediation_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "runtime_remediation_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-persistent-candidate-pit-replay-blocker-root-cause-remediation",
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
    assert remediation.READY_STATUS in result.output
    assert "source_2438j_escalation_ready=true" in result.output
    assert "prior_root_cause_matched=true" in result.output
    assert "replay_runtime_materialization_ready=true" in result.output
    assert "candidate_replay_runtime_executable_count=3" in result.output
    assert "candidate_replay_outcome_rechecked=false" in result.output
    assert "candidate_replay_pass_count=0" in result.output
    assert "candidate_replay_fail_count=0" in result.output
    assert "candidate_replay_blocked_count=3" in result.output
    assert "forward_aging_handoff_ready=false" in result.output
    assert f"next_route={remediation.NEXT_ROUTE_RECHECK_AFTER_RUNTIME_REMEDIATION}" in result.output


def test_root_cause_remediation_wrapper_missing_source_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438j_escalation"].unlink()

    payload = RUN_IMPL(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["source_validation_error_count"] == 1
    assert "source_2438j_escalation_ready" in payload["evidence_gap_ids"]


def test_root_cause_remediation_blocks_without_registry_catalog_docs() -> None:
    sources = _source_documents()

    payload = BUILD_REMEDIATION(
        sources["source_2438j_escalation"],
        sources["root_cause_matrix"],
        sources["source_2438i_blocked_recheck"],
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

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["registry_catalog_docs_alignment"] is False
    assert "registry_catalog_docs_alignment" in payload["evidence_gap_ids"]


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return BUILD_REMEDIATION(
        sources["source_2438j_escalation"],
        sources["root_cause_matrix"],
        sources["source_2438i_blocked_recheck"],
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
    root_cause: str = remediation.EXPECTED_ROOT_CAUSE,
) -> dict[str, Any]:
    resolved_records = records or _records("BLOCKED", "BLOCKED", "BLOCKED")
    root_rows = [
        {
            "candidate_id": record["candidate_id"],
            "root_cause_category": root_cause,
            "root_cause_categories": [root_cause],
            "root_cause_layer": ["engine_runtime"],
            "recommended_next_action": "replay_runtime_materialization_remediation",
            "replay_outcome_after_escalation": "NOT_RECHECKED",
            "eligible_for_forward_aging": False,
        }
        for record in resolved_records
    ]
    return {
        "source_2438j_escalation": {
            "status": remediation.EXPECTED_2438J_STATUS,
            "recommended_next_research_task": remediation.EXPECTED_2438J_ROUTE,
            "as_of": "2026-07-08",
            "persistent_blocker_escalation_ready": True,
            "persistent_blocked_candidate_count": 3,
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": 3,
            "candidate_persistent_blocker_escalation_records": [
                {
                    "candidate_id": row["candidate_id"],
                    "persistent_blocker_category": root_cause,
                    "persistent_blocker_root_cause_categories": [root_cause],
                    "recommended_next_action": (
                        "replay_runtime_materialization_remediation"
                    ),
                    "replay_outcome_after_escalation": "NOT_RECHECKED",
                    "eligible_for_forward_aging": False,
                }
                for row in root_rows
            ],
            "candidate_persistent_blocker_root_cause_matrix": {"rows": root_rows},
        },
        "root_cause_matrix": {
            "candidate_persistent_blocker_root_cause_matrix": {
                "rows": root_rows,
                "root_cause_matrix_ready": True,
            },
        },
        "source_2438i_blocked_recheck": {
            "status": remediation.EXPECTED_2438I_STATUS,
            "as_of": "2026-07-08",
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": 3,
            "candidate_replay_outputs_complete": True,
            "candidate_replay_output_record_count": 3,
            "candidate_replay_output_records": {"records": resolved_records},
        },
        "source_2438h_remaining_blocker_closure": {
            "status": remediation.EXPECTED_2438H_STATUS,
            "remaining_candidate_blocker_closure_ready": True,
            "remaining_candidate_blocker_count_after": 0,
        },
        "source_2438f_candidate_level_blocker_closure": {
            "status": remediation.EXPECTED_2438F_STATUS,
            "candidate_level_blocker_closure_ready": True,
            "candidate_level_blocker_count_after": 0,
        },
        "source_2438d_output_closure": {
            "status": remediation.EXPECTED_2438D_STATUS,
            "blocker_closure_ready": True,
            "candidate_replay_outputs_complete": True,
            "candidate_replay_output_record_count": 3,
            "candidate_replay_output_records": {"records": resolved_records},
        },
        "candidate_replay_output_records": {
            "candidate_replay_output_records": {
                "candidate_replay_output_records_ready": True,
                "records": resolved_records,
            },
        },
        "source_2438b_engine_blocker_closure": {
            "status": remediation.EXPECTED_2438B_STATUS,
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


def _record(candidate_id: str, replay_status: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "as_of": "2026-07-08",
        "replay_status": replay_status,
        "source_replay_status": "blocked_replay_engine_gap",
        "baseline_id": "growth_tilt_current_policy_baseline",
        "blocking_gap_ids": [
            "candidate_pit_replay_engine_available",
            "candidate_replay_input_specs_ready",
        ],
        "status_reason": {
            "blocker_reason": f"{candidate_id} lacks executable replay runtime",
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
        "input_spec_ref": f"input_specs/{candidate_id}",
        "source_traceability_ref": f"source_traceability/{candidate_id}",
        "valid_until_policy_ref": f"valid_until/{candidate_id}",
        "outcome_linkage_key": f"growth_tilt_pit_replay:{candidate_id}:1d,5d,10d,20d",
        "forward_aging_handoff_key": f"TRADING-2439:forward_aging:{candidate_id}",
        "replay_window": "ai_after_chatgpt_pit_replay_window",
        "paper_shadow_candidate_found": False,
        "trading_advice_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    paths = {
        "source_2438j_escalation": tmp_path / "source_2438j.json",
        "root_cause_matrix": tmp_path / "root_cause_matrix.json",
        "source_2438i_blocked_recheck": tmp_path / "source_2438i.json",
        "source_2438h_remaining_blocker_closure": tmp_path / "source_2438h.json",
        "source_2438f_candidate_level_blocker_closure": tmp_path / "source_2438f.json",
        "source_2438d_output_closure": tmp_path / "source_2438d.json",
        "candidate_replay_output_records": tmp_path / "candidate_records.json",
        "source_2438b_engine_blocker_closure": tmp_path / "source_2438b.json",
        "source_2438j_doc": tmp_path / "source_2438j.md",
        "root_cause_matrix_doc": tmp_path / "root_cause_matrix.md",
        "source_2438i_doc": tmp_path / "source_2438i.md",
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
        "source_2438j_escalation",
        "root_cause_matrix",
        "source_2438i_blocked_recheck",
        "source_2438h_remaining_blocker_closure",
        "source_2438f_candidate_level_blocker_closure",
        "source_2438d_output_closure",
        "candidate_replay_output_records",
        "source_2438b_engine_blocker_closure",
    ):
        paths[key].write_text(json.dumps(sources[key]), encoding="utf-8")
    for key in (
        "source_2438j_doc",
        "root_cause_matrix_doc",
        "source_2438i_doc",
        "source_2438h_doc",
        "source_2438f_doc",
        "source_2438d_doc",
        "candidate_output_records_doc",
        "source_2438b_doc",
        "requirement_doc",
    ):
        paths[key].write_text(
            "TRADING-2438K runtime NOT_RECHECKED forward-aging evidence",
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
        "source_2438j_escalation_path": paths["source_2438j_escalation"],
        "root_cause_matrix_path": paths["root_cause_matrix"],
        "source_2438i_blocked_recheck_path": paths["source_2438i_blocked_recheck"],
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
        "source_2438j_doc_path": paths["source_2438j_doc"],
        "root_cause_matrix_doc_path": paths["root_cause_matrix_doc"],
        "source_2438i_doc_path": paths["source_2438i_doc"],
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
        ("--source-2438j-escalation", "source_2438j_escalation"),
        ("--root-cause-matrix", "root_cause_matrix"),
        ("--source-2438i-blocked-recheck", "source_2438i_blocked_recheck"),
        ("--source-2438h-remaining-blocker-closure", "source_2438h_remaining_blocker_closure"),
        (
            "--source-2438f-candidate-level-blocker-closure",
            "source_2438f_candidate_level_blocker_closure",
        ),
        ("--source-2438d-output-closure", "source_2438d_output_closure"),
        ("--candidate-replay-output-records", "candidate_replay_output_records"),
        ("--source-2438b-engine-blocker-closure", "source_2438b_engine_blocker_closure"),
        ("--source-2438j-doc", "source_2438j_doc"),
        ("--root-cause-matrix-doc", "root_cause_matrix_doc"),
        ("--source-2438i-doc", "source_2438i_doc"),
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
    return {
        "reports": [
            {"report_id": report_id} for report_id in remediation.REQUIRED_REPORT_IDS
        ]
    }


def _report_registry_yaml() -> str:
    lines = ["reports:"]
    for report_id in remediation.REQUIRED_REPORT_IDS:
        lines.append(f"  - report_id: {report_id}")
    return "\n".join(lines)


def _artifact_catalog_text() -> str:
    return "\n".join(remediation.REQUIRED_CATALOG_REFERENCES)


def _system_flow_text() -> str:
    return "\n".join(remediation.REQUIRED_SYSTEM_FLOW_REFERENCES)


def _research_doc_texts() -> dict[str, str]:
    return {
        "requirement_doc": "TRADING-2438K runtime NOT_RECHECKED forward-aging evidence",
    }
