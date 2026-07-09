from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation as impl,  # noqa: E501
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation as recheck,  # noqa: E501
)

RUN_IMPL = (
    impl.run_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation
)
BUILD_RECHECK = (
    recheck.build_growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation
)


def test_after_runtime_remediation_reads_2438k_and_blocks_null_metrics() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["prior_status"] == recheck.EXPECTED_2438K_STATUS
    assert payload["source_2438k_runtime_remediation_ready"] is True
    assert payload["runtime_remediation_ready"] is True
    assert payload["runtime_blocker_count_after"] == 0
    assert payload["candidate_replay_runtime_executable_count"] == 3
    assert payload["executable_replay_readiness_handoff_ready"] is True
    assert payload["candidate_replay_outcome_rechecked"] is True
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    assert payload["post_runtime_candidate_replay_blocker_count"] == 3
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_BLOCKED


def test_after_runtime_remediation_blocks_when_2438k_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438k_runtime_remediation"]["status"] = "BLOCKED"

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["source_2438k_runtime_remediation_ready"] is False
    assert payload["candidate_replay_outcome_rechecked"] is False
    assert "source_2438k_runtime_remediation_ready" in payload["evidence_gap_ids"]


def test_after_runtime_remediation_blocks_when_runtime_count_less_than_three() -> None:
    sources = _source_documents(records=_records("BLOCKED", "BLOCKED"))
    sources["source_2438k_runtime_remediation"][
        "candidate_replay_runtime_executable_count"
    ] = 2

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["runtime_remediation_record_count"] == 2
    assert "candidate_replay_runtime_executable_count_three" in payload[
        "evidence_gap_ids"
    ]


def test_after_runtime_remediation_blocks_when_executable_handoff_missing() -> None:
    sources = _source_documents()
    handoff = sources["executable_replay_readiness_handoff"][
        "executable_replay_readiness_handoff"
    ]
    handoff["executable_replay_readiness_handoff_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["executable_replay_readiness_handoff_ready"] is False
    assert "executable_replay_readiness_handoff_ready" in payload["evidence_gap_ids"]


def test_after_runtime_remediation_blocks_when_metric_values_missing() -> None:
    payload = _build_payload(_source_documents(records=_records("BLOCKED")))
    decision = payload["candidate_pass_fail_blocked_decision_matrix"]["rows"][0]

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["runtime_metric_materialization_output_ready"] is False
    assert decision["metric_values_materialized"] is False
    assert decision["remaining_blocker_category"] == "runtime_metric_values_not_materialized"


def test_after_runtime_remediation_blocks_when_baseline_output_missing() -> None:
    sources = _source_documents(records=_records("PASS", "FAIL", "FAIL"))
    sources["runtime_materialization_remediation"]["runtime_materialization_remediation"][
        "candidate_runtime_remediation_records"
    ][0]["baseline_comparison_materialized"] = False

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["baseline_comparison_runtime_output_ready"] is False
    assert payload["candidate_pass_fail_blocked_decision_matrix"]["rows"][0][
        "remaining_blocker_category"
    ] == "baseline_comparison_runtime_output_missing"


def test_after_runtime_remediation_blocks_when_threshold_output_missing() -> None:
    sources = _source_documents(records=_records("PASS", "FAIL", "FAIL"))
    runtime_record = sources["runtime_materialization_remediation"][
        "runtime_materialization_remediation"
    ]["candidate_runtime_remediation_records"][0]
    runtime_record.pop("threshold_evaluation")
    runtime_record.pop("threshold_evaluation_ref")

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["threshold_evaluator_runtime_output_ready"] is False
    assert payload["candidate_pass_fail_blocked_decision_matrix"]["rows"][0][
        "remaining_blocker_category"
    ] == "threshold_evaluation_runtime_output_missing"


def test_after_runtime_remediation_blocks_when_candidate_output_incomplete() -> None:
    sources = _source_documents()
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "candidate_replay_output_records_ready"
    ] = False
    sources["source_2438k_runtime_remediation"]["candidate_replay_outputs_complete"] = (
        False
    )

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["candidate_replay_outputs_complete"] is False
    assert "candidate_replay_output_records_complete" in payload["evidence_gap_ids"]


def test_after_runtime_remediation_zero_zero_three_stays_blocked() -> None:
    payload = _build_payload(
        _source_documents(records=_records("BLOCKED", "BLOCKED", "BLOCKED")),
    )

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["status"] != recheck.NO_PASSING_CANDIDATE_STATUS
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3


def test_after_runtime_remediation_three_fail_routes_no_passing_candidate() -> None:
    payload = _build_payload(_source_documents(records=_records("FAIL", "FAIL", "FAIL")))

    assert payload["status"] == recheck.NO_PASSING_CANDIDATE_STATUS
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 3
    assert payload["candidate_replay_blocked_count"] == 0
    assert payload["forward_aging_handoff_ready"] is False
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_NO_PASS


def test_after_runtime_remediation_pass_without_blocked_routes_ready() -> None:
    payload = _build_payload(_source_documents(records=_records("PASS", "FAIL", "FAIL")))

    assert payload["status"] == recheck.READY_STATUS
    assert payload["candidate_replay_pass_count"] == 1
    assert payload["candidate_replay_fail_count"] == 2
    assert payload["candidate_replay_blocked_count"] == 0
    assert payload["forward_aging_handoff_ready"] is True
    assert payload["forward_aging_candidate_count"] == 1
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_READY


def test_after_runtime_remediation_blocked_candidate_prevents_no_passing() -> None:
    payload = _build_payload(_source_documents(records=_records("FAIL", "FAIL", "BLOCKED")))

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["status"] != recheck.NO_PASSING_CANDIDATE_STATUS
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_BLOCKED


def test_after_runtime_remediation_pass_does_not_enable_paper_shadow() -> None:
    payload = _build_payload(_source_documents(records=_records("PASS", "FAIL", "FAIL")))

    assert payload["status"] == recheck.READY_STATUS
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_schedule_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False


def test_after_runtime_remediation_forward_aging_handoff_contains_only_pass() -> None:
    payload = _build_payload(_source_documents(records=_records("PASS", "FAIL", "FAIL")))

    assert payload["forward_aging_handoff_ready"] is True
    assert {record["replay_status"] for record in payload["forward_aging_candidates"]} == {
        "PASS"
    }
    assert all(
        row["eligible_for_forward_aging"] == (row["replay_status"] == "PASS")
        for row in payload["candidate_pass_fail_blocked_decision_matrix"]["rows"]
    )


def test_after_runtime_remediation_no_pass_has_no_forward_aging_handoff() -> None:
    payload = _build_payload(_source_documents(records=_records("FAIL", "FAIL", "FAIL")))

    assert payload["status"] == recheck.NO_PASSING_CANDIDATE_STATUS
    assert payload["forward_aging_handoff_ready"] is False
    assert payload["forward_aging_candidate_count"] == 0
    assert payload["forward_aging_candidates"] == []


def test_after_runtime_remediation_ready_keeps_safety_boundary_false() -> None:
    payload = _build_payload(_source_documents(records=_records("PASS", "FAIL", "FAIL")))

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["generated_trading_advice"] is False
    assert payload["broker_action"] == "none"
    assert payload["production_effect"] == "none"


def test_after_runtime_remediation_never_generates_trading_advice() -> None:
    for statuses in (
        ("PASS", "FAIL", "FAIL"),
        ("FAIL", "FAIL", "FAIL"),
        ("BLOCKED", "BLOCKED", "BLOCKED"),
    ):
        payload = _build_payload(_source_documents(records=_records(*statuses)))
        assert payload["generated_trading_advice"] is False
        assert payload["trading_advice_generated"] is False
        assert payload["actionable_allocation_generated"] is False
        assert payload["portfolio_weight_mutated"] is False


def test_after_runtime_remediation_next_routes_are_deterministic() -> None:
    ready = _build_payload(_source_documents(records=_records("PASS", "FAIL", "FAIL")))
    no_pass = _build_payload(_source_documents(records=_records("FAIL", "FAIL", "FAIL")))
    blocked = _build_payload(
        _source_documents(records=_records("BLOCKED", "BLOCKED", "BLOCKED")),
    )

    assert ready["recommended_next_research_task"] == recheck.NEXT_ROUTE_READY
    assert no_pass["recommended_next_research_task"] == recheck.NEXT_ROUTE_NO_PASS
    assert blocked["recommended_next_research_task"] == recheck.NEXT_ROUTE_BLOCKED


def test_after_runtime_remediation_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "after_runtime"
    docs_root = tmp_path / "docs" / "research"

    payload = RUN_IMPL(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_passed"] is True
    assert payload["candidate_replay_outcome_rechecked"] is True
    for key in (
        "json_path",
        "runtime_remediation_after_recheck_json",
        "candidate_pass_fail_blocked_decision_matrix_json",
        "forward_aging_handoff_readiness_summary_json",
        "post_runtime_candidate_replay_blocker_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "runtime_remediation_after_recheck_markdown",
        "candidate_pass_fail_blocked_decision_matrix_markdown",
        "forward_aging_handoff_readiness_summary_markdown",
        "post_runtime_candidate_replay_blocker_summary_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()
    assert (output_root / "recheck_after_runtime_remediation_result.json").exists()


def test_after_runtime_remediation_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "after_runtime_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-top3-candidate-pit-replay-recheck-after-runtime-remediation",
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
    assert recheck.BLOCKED_STATUS in result.output
    assert "source_2438k_runtime_remediation_ready=true" in result.output
    assert "runtime_blocker_count_after=0" in result.output
    assert "candidate_replay_runtime_executable_count=3" in result.output
    assert "executable_replay_readiness_handoff_ready=true" in result.output
    assert "candidate_replay_outcome_rechecked=true" in result.output
    assert "candidate_replay_pass_count=0" in result.output
    assert "candidate_replay_fail_count=0" in result.output
    assert "candidate_replay_blocked_count=3" in result.output
    assert "post_runtime_candidate_replay_blocker_count=3" in result.output
    assert "forward_aging_handoff_ready=false" in result.output
    assert "paper_shadow_candidate_found=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert f"next_route={recheck.NEXT_ROUTE_BLOCKED}" in result.output


def test_after_runtime_remediation_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[recheck.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-top3-candidate-pit-replay-recheck-after-runtime-remediation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    requirement_doc = Path(
        "docs/requirements/"
        "TRADING-2438L_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_"
        "Runtime_Remediation.md"
    ).read_text(encoding="utf-8")
    assert recheck.REPORT_TYPE in catalog
    for reference in recheck.REQUIRED_CATALOG_REFERENCES:
        assert reference in catalog
    for reference in recheck.REQUIRED_SYSTEM_FLOW_REFERENCES:
        assert reference in system_flow
    assert impl.TASK_REGISTER_ID in task_register
    assert "TRADING-2438M_Growth_Tilt_Post_Runtime_Candidate_PIT_Replay_Blocker_Resolution" in (
        requirement_doc
    )


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return BUILD_RECHECK(
        sources["source_2438k_runtime_remediation"],
        sources["executable_replay_readiness_handoff"],
        sources["runtime_materialization_remediation"],
        sources["runtime_execution_audit_trail"],
        sources["candidate_replay_output_records"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
        as_of="2026-07-08",
    )


def _source_documents(records: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    selected_records = records or _records("BLOCKED", "BLOCKED", "BLOCKED")
    runtime_records = [_runtime_record_from_candidate(record) for record in selected_records]
    return {
        "source_2438k_runtime_remediation": _source_2438k_ready(
            selected_records,
            runtime_records,
        ),
        "executable_replay_readiness_handoff": {
            "executable_replay_readiness_handoff": _handoff(selected_records),
        },
        "runtime_materialization_remediation": {
            "runtime_materialization_remediation": _runtime_materialization(
                runtime_records,
            ),
        },
        "runtime_execution_audit_trail": {
            "runtime_execution_audit_trail": _audit_trail(selected_records),
        },
        "candidate_replay_output_records": {
            "candidate_replay_output_records": {
                "candidate_replay_output_records_ready": True,
                "records": selected_records,
            }
        },
        "data_quality_summary": _data_quality_summary(),
        "report_registry": {
            "reports": [
                {"report_id": report_id} for report_id in recheck.REQUIRED_REPORT_IDS
            ]
        },
        "artifact_catalog_text": "\n".join(recheck.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(recheck.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "source_2438k_doc": "2438L runtime PASS FAIL BLOCKED forward-aging",
            "executable_handoff_doc": "2438L runtime PASS FAIL BLOCKED forward-aging",
            "runtime_materialization_doc": (
                "2438L runtime PASS FAIL BLOCKED forward-aging"
            ),
            "runtime_audit_doc": "2438L runtime PASS FAIL BLOCKED forward-aging",
            "candidate_output_records_doc": (
                "2438L runtime PASS FAIL BLOCKED forward-aging"
            ),
            "requirement_doc": "2438L runtime PASS FAIL BLOCKED forward-aging",
        },
    }


def _source_2438k_ready(
    candidate_records: list[dict[str, Any]],
    runtime_records: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": recheck.EXPECTED_2438K_STATUS,
        "recommended_next_research_task": recheck.EXPECTED_2438K_ROUTE,
        "root_cause_remediation_ready": True,
        "runtime_remediation_ready": True,
        "runtime_blocker_count_before": len(candidate_records),
        "runtime_blocker_count_after": 0,
        "replay_runtime_materialization_ready": True,
        "candidate_replay_runtime_executable": True,
        "candidate_replay_runtime_executable_count": len(runtime_records),
        "candidate_replay_outputs_complete": True,
        "candidate_replay_output_record_count": len(candidate_records),
        "candidate_replay_outcome_rechecked": False,
        "candidate_replay_pass_count": 0,
        "candidate_replay_fail_count": 0,
        "candidate_replay_blocked_count": len(candidate_records),
        "forward_aging_handoff_ready": False,
        "candidate_runtime_remediation_records": runtime_records,
        "executable_replay_readiness_handoff": _handoff(candidate_records),
        "runtime_materialization_remediation": _runtime_materialization(runtime_records),
        "runtime_execution_audit_trail": _audit_trail(candidate_records),
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _runtime_materialization(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_candidate_replay_runtime_materialization.v1",
        "status": recheck.EXPECTED_2438K_STATUS,
        "runtime_materialization_remediation_ready": True,
        "runtime_remediation_record_count": len(records),
        "candidate_runtime_remediation_records": records,
        "production_effect": "none",
        "broker_action": "none",
    }


def _handoff(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_executable_replay_readiness_handoff.v1",
        "status": recheck.EXPECTED_2438K_STATUS,
        "executable_replay_readiness_handoff_ready": True,
        "ready_for_2438l_recheck": True,
        "handoff_candidate_count": len(records),
        "handoff_policy": "RECHECK_ONLY_2438L_DECIDES_PASS_FAIL_BLOCKED",
        "next_route": recheck.EXPECTED_2438K_ROUTE,
        "handoff_candidates": [
            {
                "candidate_id": record["candidate_id"],
                "runtime_execution_smoke_check_ref": (
                    f"TRADING-2438K:runtime_smoke:{record['candidate_id']}"
                ),
                "forward_aging_handoff_key": (
                    f"TRADING-2439A:forward_aging_candidate_pack:"
                    f"{record['candidate_id']}"
                ),
                "replay_outcome_after_remediation": "NOT_RECHECKED",
            }
            for record in records
        ],
        "forward_aging_handoff_ready": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _audit_trail(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_runtime_execution_audit_trail.v1",
        "status": recheck.EXPECTED_2438K_STATUS,
        "audit_record_count": len(records),
        "audit_records": [
            {
                "candidate_id": record["candidate_id"],
                "runtime_execution_smoke_check_status": "PASS",
                "runtime_execution_smoke_check_ref": (
                    f"TRADING-2438K:runtime_smoke:{record['candidate_id']}"
                ),
                "production_effect": "none",
                "broker_action": "none",
            }
            for record in records
        ],
    }


def _records(*statuses: str) -> list[dict[str, Any]]:
    statuses = statuses or ("BLOCKED", "BLOCKED", "BLOCKED")
    candidates = _selected_candidates()[: len(statuses)]
    return [
        _candidate_record(candidate["candidate_id"], status)
        for candidate, status in zip(candidates, statuses, strict=True)
    ]


def _candidate_record(candidate_id: str, status: str) -> dict[str, Any]:
    normalized_status = status.upper()
    blocked = normalized_status == "BLOCKED"
    fail = normalized_status == "FAIL"
    return {
        "candidate_id": candidate_id,
        "candidate_family": candidate_id,
        "replay_status": normalized_status,
        "as_of": "2026-07-08",
        "replay_window": "ai_after_chatgpt_pit_replay_window",
        "baseline_id": "growth_tilt_current_policy_baseline",
        "input_spec_ref": f"outputs/input_specs.json#{candidate_id}",
        "source_traceability_ref": f"outputs/source_traceability.json#{candidate_id}",
        "evidence_ref": f"outputs/pit_replay_evidence.json#{candidate_id}",
        "valid_until_policy_ref": f"outputs/valid_until.json#{candidate_id}",
        "outcome_linkage_key": f"growth_tilt_pit_replay:{candidate_id}:1d,5d,10d,20d",
        "forward_aging_handoff_key": (
            f"TRADING-2439A:forward_aging_candidate_pack:{candidate_id}"
        ),
        "metric_summary": _metric_summary(blocked=blocked, fail=fail),
        "threshold_evaluation": _threshold_evaluation(candidate_id, normalized_status),
        "paper_shadow_candidate_found": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _runtime_record_from_candidate(candidate_record: dict[str, Any]) -> dict[str, Any]:
    candidate_id = candidate_record["candidate_id"]
    runtime_record = {
        "candidate_id": candidate_id,
        "runtime_remediation_ready": True,
        "runtime_execution_smoke_check_status": "PASS",
        "runtime_execution_smoke_check_ref": (
            f"TRADING-2438K:runtime_smoke:{candidate_id}"
        ),
        "metric_materialization_ready": True,
        "metric_materialization_ref": (
            f"outputs/runtime_metric_materialization.json#{candidate_id}"
        ),
        "metric_summary": candidate_record["metric_summary"],
        "baseline_comparison_materialized": True,
        "baseline_comparison_ref": f"outputs/baseline_comparison.json#{candidate_id}",
        "threshold_evaluator_ready": True,
        "threshold_evaluation_ref": f"outputs/threshold_evaluation.json#{candidate_id}",
        "threshold_evaluation": candidate_record["threshold_evaluation"],
        "source_traceability_runtime_ref": (
            f"outputs/runtime_source_traceability.json#{candidate_id}"
        ),
        "valid_until_policy_ref": candidate_record["valid_until_policy_ref"],
        "outcome_linkage_key": candidate_record["outcome_linkage_key"],
        "forward_aging_handoff_key": candidate_record["forward_aging_handoff_key"],
        "evidence_ref": candidate_record["evidence_ref"],
        "as_of": "2026-07-08",
        "production_effect": "none",
        "broker_action": "none",
    }
    if candidate_record["replay_status"] == "BLOCKED":
        runtime_record.pop("threshold_evaluation")
        runtime_record.pop("threshold_evaluation_ref")
    return runtime_record


def _metric_summary(*, blocked: bool, fail: bool) -> dict[str, float | None]:
    if blocked:
        return {key: None for key in recheck.METRIC_KEYS}
    value = -0.01 if fail else 0.01
    return {key: value for key in recheck.METRIC_KEYS}


def _threshold_evaluation(candidate_id: str, status: str) -> dict[str, Any]:
    if status == "PASS":
        return {
            "status": "PASS",
            "pass_reason": f"{candidate_id} passed explicit threshold evaluation.",
            "failed_criteria": [],
        }
    if status == "FAIL":
        return {
            "status": "FAIL",
            "fail_reason": f"{candidate_id} failed explicit threshold evaluation.",
            "failed_criteria": ["return_delta_vs_baseline"],
        }
    return {
        "status": "BLOCKED",
        "blocker_reason": "Runtime metric values are not materialized.",
        "failed_criteria": [],
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
        "source_2438k": root / "root_cause_remediation_result.json",
        "handoff": root / "executable_replay_readiness_handoff.json",
        "runtime_materialization": root / "runtime_materialization_remediation.json",
        "runtime_audit": root / "runtime_execution_audit_trail.json",
        "candidate_records": root / "candidate_replay_output_records.json",
        "source_2438k_doc": root / "source_2438k.md",
        "handoff_doc": root / "handoff.md",
        "runtime_materialization_doc": root / "runtime_materialization.md",
        "runtime_audit_doc": root / "runtime_audit.md",
        "candidate_records_doc": root / "candidate_records.md",
        "requirement_doc": root / "requirement.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2438k"], sources["source_2438k_runtime_remediation"])
    _write_json(paths["handoff"], sources["executable_replay_readiness_handoff"])
    _write_json(
        paths["runtime_materialization"],
        sources["runtime_materialization_remediation"],
    )
    _write_json(paths["runtime_audit"], sources["runtime_execution_audit_trail"])
    _write_json(paths["candidate_records"], sources["candidate_replay_output_records"])
    for key, text in sources["research_doc_texts"].items():
        path_key = {
            "source_2438k_doc": "source_2438k_doc",
            "executable_handoff_doc": "handoff_doc",
            "runtime_materialization_doc": "runtime_materialization_doc",
            "runtime_audit_doc": "runtime_audit_doc",
            "candidate_output_records_doc": "candidate_records_doc",
            "requirement_doc": "requirement_doc",
        }[key]
        paths[path_key].write_text(text, encoding="utf-8")
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in recheck.REQUIRED_REPORT_IDS
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
        "source_2438k_runtime_remediation_path": paths["source_2438k"],
        "executable_replay_readiness_handoff_path": paths["handoff"],
        "runtime_materialization_remediation_path": paths["runtime_materialization"],
        "runtime_execution_audit_trail_path": paths["runtime_audit"],
        "candidate_replay_output_records_path": paths["candidate_records"],
        "source_2438k_doc_path": paths["source_2438k_doc"],
        "executable_handoff_doc_path": paths["handoff_doc"],
        "runtime_materialization_doc_path": paths["runtime_materialization_doc"],
        "runtime_audit_doc_path": paths["runtime_audit_doc"],
        "candidate_output_records_doc_path": paths["candidate_records_doc"],
        "requirement_doc_path": paths["requirement_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2438k-runtime-remediation",
        str(paths["source_2438k"]),
        "--executable-replay-readiness-handoff",
        str(paths["handoff"]),
        "--runtime-materialization-remediation",
        str(paths["runtime_materialization"]),
        "--runtime-execution-audit-trail",
        str(paths["runtime_audit"]),
        "--candidate-replay-output-records",
        str(paths["candidate_records"]),
        "--source-2438k-doc",
        str(paths["source_2438k_doc"]),
        "--executable-handoff-doc",
        str(paths["handoff_doc"]),
        "--runtime-materialization-doc",
        str(paths["runtime_materialization_doc"]),
        "--runtime-audit-doc",
        str(paths["runtime_audit_doc"]),
        "--candidate-output-records-doc",
        str(paths["candidate_records_doc"]),
        "--requirement-doc",
        str(paths["requirement_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
