from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure as impl,  # noqa: E501
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure as recheck,  # noqa: E501
)

RUN_IMPL = (
    impl.run_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure
)
BUILD_RECHECK = (
    recheck.build_growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure
)


def test_after_remaining_blocker_closure_reads_2438h_and_blocks_current_zero_zero_three() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["source_2438h_remaining_blocker_closure_ready"] is True
    assert payload["remaining_candidate_blocker_closure_ready"] is True
    assert payload["remaining_candidate_blocker_count_after"] == 0
    assert payload["replay_recheck_handoff_ready"] is True
    assert payload["candidate_recheckable_after_closure_count"] == 3
    assert payload["candidate_replay_outputs_complete"] is True
    assert payload["candidate_replay_output_record_count"] == 3
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    assert payload["persistent_candidate_replay_blocker_count"] == 3
    assert payload["forward_aging_handoff_ready"] is False
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_BLOCKED


def test_after_remaining_blocker_closure_blocks_when_2438h_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438h_remaining_blocker_closure"]["status"] = (
        "GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_BLOCKED"
    )

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["source_2438h_remaining_blocker_closure_ready"] is False
    assert "source_2438h_remaining_blocker_closure_ready" in payload["evidence_gap_ids"]
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_BLOCKED


def test_after_remaining_blocker_closure_blocks_when_handoff_not_ready() -> None:
    sources = _source_documents()
    handoff = sources["replay_recheck_readiness_handoff"][
        "replay_recheck_readiness_handoff"
    ]
    handoff["replay_recheck_handoff_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["replay_recheck_handoff_ready"] is False
    assert "replay_recheck_handoff_ready" in payload["evidence_gap_ids"]


def test_after_remaining_blocker_closure_blocks_when_remaining_after_count_is_not_zero() -> None:
    sources = _source_documents()
    source_2438h = sources["source_2438h_remaining_blocker_closure"]
    source_2438h["remaining_candidate_blocker_closure_ready"] = False
    source_2438h["remaining_candidate_blocker_count_after"] = 1
    before_after = sources["remaining_blocker_before_after_matrix"][
        "remaining_candidate_blocker_before_after_matrix"
    ]
    before_after["after"]["remaining_candidate_blocker_count_after"] = 1
    before_after["rows"][0]["remaining_blocker_after_closure"] = "still open"

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["remaining_candidate_blocker_closure_ready"] is False
    assert "remaining_candidate_blocker_closure_ready" in payload["evidence_gap_ids"]


def test_after_remaining_blocker_closure_blocks_when_record_count_is_not_three() -> None:
    payload = _build_payload(_source_documents(records=_records("BLOCKED", "BLOCKED")))

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["candidate_replay_output_record_count"] == 2
    assert "candidate_replay_output_record_count" in payload["evidence_gap_ids"]


def test_after_remaining_blocker_closure_zero_zero_three_stays_blocked() -> None:
    payload = _build_payload(
        _source_documents(records=_records("BLOCKED", "BLOCKED", "BLOCKED")),
    )

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["status"] != recheck.NO_PASSING_CANDIDATE_STATUS
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_BLOCKED


def test_after_remaining_blocker_closure_three_fail_routes_to_no_passing_candidate() -> None:
    payload = _build_payload(_source_documents(records=_records("FAIL", "FAIL", "FAIL")))

    assert payload["status"] == recheck.NO_PASSING_CANDIDATE_STATUS
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 3
    assert payload["candidate_replay_blocked_count"] == 0
    assert payload["forward_aging_handoff_ready"] is False
    assert payload["forward_aging_candidate_count"] == 0
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_NO_PASS


def test_after_remaining_blocker_closure_pass_without_blocked_routes_ready() -> None:
    payload = _build_payload(_source_documents(records=_records("PASS", "FAIL", "FAIL")))

    assert payload["status"] == recheck.READY_STATUS
    assert payload["candidate_replay_pass_count"] == 1
    assert payload["candidate_replay_fail_count"] == 2
    assert payload["candidate_replay_blocked_count"] == 0
    assert payload["forward_aging_handoff_ready"] is True
    assert payload["forward_aging_candidate_count"] == 1
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_READY
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["paper_shadow_enabled"] is False


def test_after_remaining_blocker_closure_blocked_candidate_never_becomes_no_passing() -> None:
    payload = _build_payload(_source_documents(records=_records("FAIL", "FAIL", "BLOCKED")))

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["status"] != recheck.NO_PASSING_CANDIDATE_STATUS
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_BLOCKED


def test_after_remaining_blocker_closure_forward_aging_handoff_contains_only_pass_records() -> None:
    payload = _build_payload(_source_documents(records=_records("PASS", "FAIL", "FAIL")))

    assert payload["forward_aging_handoff_ready"] is True
    assert {record["replay_status"] for record in payload["forward_aging_candidates"]} == {
        "PASS"
    }
    assert all(
        row["forward_aging_eligible"] == (row["replay_status"] == "PASS")
        for row in payload["candidate_pass_fail_blocked_decision_matrix"]["rows"]
    )


def test_after_remaining_blocker_closure_no_pass_has_no_forward_aging_handoff() -> None:
    payload = _build_payload(_source_documents(records=_records("FAIL", "FAIL", "FAIL")))

    assert payload["forward_aging_handoff_ready"] is False
    assert payload["forward_aging_candidate_count"] == 0
    assert payload["forward_aging_candidates"] == []


def test_after_remaining_blocker_closure_keeps_safety_boundary_false() -> None:
    ready = _build_payload(_source_documents(records=_records("PASS", "FAIL", "FAIL")))
    blocked = _build_payload(_source_documents())

    for payload in (ready, blocked):
        for field in impl.SAFETY_FALSE_FIELDS:
            assert payload[field] is False
        assert payload["generated_trading_advice"] is False
        assert payload["broker_action"] == "none"
        assert payload["production_effect"] == "none"


def test_after_remaining_blocker_closure_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "after_remaining_blocker"
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
    assert payload["candidate_replay_blocked_count"] == 3
    for key in (
        "json_path",
        "candidate_pass_fail_blocked_decision_matrix_json",
        "forward_aging_handoff_readiness_summary_json",
        "persistent_candidate_replay_blocker_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "candidate_pass_fail_blocked_decision_matrix_markdown",
        "forward_aging_handoff_readiness_summary_markdown",
        "persistent_candidate_replay_blocker_summary_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_after_remaining_blocker_closure_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "after_remaining_blocker_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            (
                "growth-tilt-top3-candidate-pit-replay-recheck-after-remaining-"
                "blocker-closure"
            ),
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
    assert "source_2438h_remaining_blocker_closure_ready=true" in result.output
    assert "remaining_candidate_blocker_closure_ready=true" in result.output
    assert "remaining_candidate_blocker_count_after=0" in result.output
    assert "replay_recheck_handoff_ready=true" in result.output
    assert "candidate_recheckable_after_closure_count=3" in result.output
    assert "candidate_replay_output_record_count=3" in result.output
    assert "candidate_replay_pass_count=0" in result.output
    assert "candidate_replay_fail_count=0" in result.output
    assert "candidate_replay_blocked_count=3" in result.output
    assert "persistent_candidate_replay_blocker_count=3" in result.output
    assert "forward_aging_handoff_ready=false" in result.output
    assert "paper_shadow_candidate_found=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert f"next_route={recheck.NEXT_ROUTE_BLOCKED}" in result.output
    assert (output_root / "recheck_after_remaining_blocker_closure_result.json").exists()


def test_after_remaining_blocker_closure_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438h"].unlink()

    payload = RUN_IMPL(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "missing_source",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert "source_2438h_remaining_blocker_closure_ready" in payload["evidence_gap_ids"]
    assert Path(payload["artifact_paths"]["no_effect_boundary_json"]).exists()


def test_after_remaining_blocker_closure_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[recheck.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-top3-candidate-pit-replay-recheck-after-remaining-blocker-closure"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert recheck.REPORT_TYPE in catalog
    for reference in recheck.REQUIRED_CATALOG_REFERENCES:
        assert reference in catalog
    for reference in recheck.REQUIRED_SYSTEM_FLOW_REFERENCES:
        assert reference in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return BUILD_RECHECK(
        sources["source_2438h_remaining_blocker_closure"],
        sources["replay_recheck_readiness_handoff"],
        sources["candidate_replay_output_records"],
        sources["remaining_blocker_before_after_matrix"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
        as_of="2026-07-08",
    )


def _source_documents(records: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    selected_records = records or _records("BLOCKED", "BLOCKED", "BLOCKED")
    return {
        "source_2438h_remaining_blocker_closure": _source_2438h_ready(selected_records),
        "replay_recheck_readiness_handoff": {
            "replay_recheck_readiness_handoff": _handoff(selected_records),
        },
        "candidate_replay_output_records": {
            "candidate_replay_output_records": {
                "candidate_replay_output_records_ready": True,
                "records": selected_records,
            }
        },
        "remaining_blocker_before_after_matrix": {
            "remaining_candidate_blocker_before_after_matrix": _before_after(
                selected_records
            ),
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
            "source_2438h_doc": "2438I PASS FAIL BLOCKED remaining forward-aging",
            "replay_recheck_handoff_doc": (
                "2438I PASS FAIL BLOCKED remaining forward-aging"
            ),
            "candidate_output_records_doc": (
                "2438I PASS FAIL BLOCKED remaining forward-aging"
            ),
            "remaining_blocker_before_after_doc": (
                "2438I PASS FAIL BLOCKED remaining forward-aging"
            ),
        },
    }


def _source_2438h_ready(records: list[dict[str, Any]]) -> dict[str, Any]:
    pass_count = sum(record["replay_status"] == "PASS" for record in records)
    fail_count = sum(record["replay_status"] == "FAIL" for record in records)
    blocked_count = sum(record["replay_status"] == "BLOCKED" for record in records)
    return {
        "status": recheck.EXPECTED_2438H_STATUS,
        "recommended_next_research_task": recheck.EXPECTED_2438H_ROUTE,
        "remaining_candidate_blocker_closure_ready": True,
        "remaining_candidate_blocker_count_before": len(records),
        "remaining_candidate_blocker_count_after": 0,
        "replay_recheck_handoff_ready": True,
        "candidate_recheckable_after_closure_count": len(records),
        "candidate_output_records_complete": True,
        "candidate_replay_output_record_count": len(records),
        "candidate_replay_pass_count": pass_count,
        "candidate_replay_fail_count": fail_count,
        "candidate_replay_blocked_count": blocked_count,
        "replay_recheck_readiness_handoff": _handoff(records),
        "remaining_candidate_blocker_before_after_matrix": _before_after(records),
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _handoff(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_replay_recheck_readiness_handoff.v1",
        "status": recheck.EXPECTED_2438H_STATUS,
        "replay_recheck_handoff_ready": True,
        "candidate_recheckable_after_closure_count": len(records),
        "handoff_policy": "RECHECK_ONLY_2438I_DECIDES_PASS_FAIL_BLOCKED",
        "next_route": recheck.EXPECTED_2438H_ROUTE,
        "recheckable_candidates": [
            {
                "candidate_id": record["candidate_id"],
                "closure_evidence_ref": (
                    f"TRADING-2438H:remaining_closure:{record['candidate_id']}"
                ),
                "replay_outcome_after_closure": "NOT_RECHECKED",
            }
            for record in records
        ],
        "forward_aging_handoff_ready": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _before_after(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_remaining_candidate_replay_blocker_before_after.v1",
        "status": recheck.EXPECTED_2438H_STATUS,
        "before": {
            "remaining_candidate_blocker_count_before": len(records),
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": len(records),
        },
        "after": {
            "remaining_candidate_blocker_count_after": 0,
            "candidate_recheckable_after_closure_count": len(records),
            "candidate_replay_pass_count": 0,
            "candidate_replay_fail_count": 0,
            "candidate_replay_blocked_count": len(records),
            "replay_outcome_after_closure": "NOT_RECHECKED",
        },
        "rows": [
            {
                "candidate_id": record["candidate_id"],
                "prior_replay_status": "BLOCKED",
                "closure_result": "CLOSED",
                "remaining_blocker_after_closure": None,
                "candidate_recheckable_after_closure": True,
                "replay_outcome_after_closure": "NOT_RECHECKED",
                "paper_shadow_candidate_found": False,
                "production_effect": "none",
                "broker_action": "none",
            }
            for record in records
        ],
        "next_route": recheck.EXPECTED_2438H_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _records(*statuses: str) -> list[dict[str, Any]]:
    candidates = _selected_candidates()[: len(statuses)]
    return [
        _record(candidate["candidate_id"], status)
        for candidate, status in zip(candidates, statuses, strict=True)
    ]


def _record(candidate_id: str, status: str) -> dict[str, Any]:
    normalized_status = status.upper()
    blocked = normalized_status == "BLOCKED"
    fail = normalized_status == "FAIL"
    return {
        "candidate_id": candidate_id,
        "candidate_family": candidate_id,
        "replay_status": normalized_status,
        "source_replay_status": (
            "blocked_replay_engine_gap" if blocked else normalized_status.lower()
        ),
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
        "blocking_gap_ids": _blocking_gap_ids() if blocked else [],
        "metric_summary": (
            {key: None for key in recheck.METRIC_KEYS}
            if blocked
            else {key: -0.01 if fail else 0.01 for key in recheck.METRIC_KEYS}
        ),
        "failed_criteria": (
            ["return_delta_vs_baseline"] if normalized_status == "FAIL" else []
        ),
        "status_reason": _status_reason(normalized_status),
        "paper_shadow_candidate_found": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _status_reason(status: str) -> dict[str, str | None]:
    if status == "PASS":
        return {
            "pass_reason": "Candidate replay passed after remaining blocker closure.",
            "fail_reason": None,
            "blocker_reason": None,
        }
    if status == "FAIL":
        return {
            "pass_reason": None,
            "fail_reason": "Candidate replay failed required criteria.",
            "blocker_reason": None,
        }
    return {
        "pass_reason": None,
        "fail_reason": None,
        "blocker_reason": "Candidate remains BLOCKED after remaining blocker closure.",
    }


def _blocking_gap_ids() -> list[str]:
    return [
        "candidate_pit_replay_engine_available",
        "candidate_replay_input_specs_ready",
        "candidate_source_traceability_manifests_ready",
        "candidate_as_of_boundary_specs_ready",
        "candidate_valid_until_boundary_specs_ready",
        "candidate_outcome_linkage_specs_ready",
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
        "source_2438h": root / "blocker_closure_result.json",
        "handoff": root / "replay_recheck_readiness_handoff.json",
        "candidate_records": root / "candidate_replay_output_records.json",
        "before_after": root / "remaining_blocker_before_after_matrix.json",
        "source_2438h_doc": root / "source_2438h.md",
        "handoff_doc": root / "handoff.md",
        "candidate_records_doc": root / "candidate_replay_output_records.md",
        "before_after_doc": root / "before_after.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2438h"], sources["source_2438h_remaining_blocker_closure"])
    _write_json(paths["handoff"], sources["replay_recheck_readiness_handoff"])
    _write_json(paths["candidate_records"], sources["candidate_replay_output_records"])
    _write_json(paths["before_after"], sources["remaining_blocker_before_after_matrix"])
    for key, text in sources["research_doc_texts"].items():
        path_key = {
            "source_2438h_doc": "source_2438h_doc",
            "replay_recheck_handoff_doc": "handoff_doc",
            "candidate_output_records_doc": "candidate_records_doc",
            "remaining_blocker_before_after_doc": "before_after_doc",
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
        "source_2438h_remaining_blocker_closure_path": paths["source_2438h"],
        "replay_recheck_readiness_handoff_path": paths["handoff"],
        "candidate_replay_output_records_path": paths["candidate_records"],
        "remaining_blocker_before_after_matrix_path": paths["before_after"],
        "source_2438h_doc_path": paths["source_2438h_doc"],
        "replay_recheck_handoff_doc_path": paths["handoff_doc"],
        "candidate_output_records_doc_path": paths["candidate_records_doc"],
        "remaining_blocker_before_after_doc_path": paths["before_after_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2438h-remaining-blocker-closure",
        str(paths["source_2438h"]),
        "--replay-recheck-readiness-handoff",
        str(paths["handoff"]),
        "--candidate-replay-output-records",
        str(paths["candidate_records"]),
        "--remaining-blocker-before-after-matrix",
        str(paths["before_after"]),
        "--source-2438h-doc",
        str(paths["source_2438h_doc"]),
        "--replay-recheck-handoff-doc",
        str(paths["handoff_doc"]),
        "--candidate-output-records-doc",
        str(paths["candidate_records_doc"]),
        "--remaining-blocker-before-after-doc",
        str(paths["before_after_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
