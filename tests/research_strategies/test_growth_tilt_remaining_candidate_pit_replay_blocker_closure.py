from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_remaining_candidate_pit_replay_blocker_closure as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_remaining_candidate_pit_replay_blocker_closure as closure,
)

RUN_IMPL = impl.run_growth_tilt_remaining_candidate_pit_replay_blocker_closure
BUILD_CLOSURE = closure.build_growth_tilt_remaining_candidate_pit_replay_blocker_closure


def test_remaining_candidate_blocker_closure_ready_from_2438g_blocked_artifact() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == closure.READY_STATUS
    assert payload["prior_status"] == closure.EXPECTED_2438G_STATUS
    assert payload["prior_candidate_replay_pass_count"] == 0
    assert payload["prior_candidate_replay_fail_count"] == 0
    assert payload["prior_candidate_replay_blocked_count"] == 3
    assert payload["remaining_candidate_blocker_count_before"] == 3
    assert payload["remaining_candidate_blocker_count_after"] == 0
    assert payload["candidate_recheckable_after_closure_count"] == 3
    assert payload["replay_recheck_handoff_ready"] is True
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    assert payload["forward_aging_handoff_ready"] is False
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE_READY
    assert all(
        record["replay_outcome_after_closure"] == "NOT_RECHECKED"
        for record in payload["remaining_candidate_blocker_closure_records"]["records"]
    )


def test_remaining_candidate_blocker_closure_blocks_when_2438g_not_blocked() -> None:
    sources = _source_documents()
    sources["source_2438g"]["status"] = (
        "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_"
        "BLOCKER_CLOSURE_READY"
    )

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["source_2438g_blocked_recheck_ready"] is False
    assert "prior_2438g_status_is_blocked" in payload["evidence_gap_ids"]
    assert payload["recommended_next_research_task"] == closure.NEXT_ROUTE_BLOCKED


def test_remaining_candidate_blocker_closure_blocks_when_blocked_count_is_not_three() -> None:
    sources = _source_documents(records=_records(2))
    sources["source_2438g"]["candidate_replay_blocked_count"] = 2

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["candidate_replay_output_record_count"] == 2
    assert "prior_candidate_replay_blocked_count" in payload["evidence_gap_ids"]
    assert "candidate_output_records_complete" in payload["evidence_gap_ids"]


def test_remaining_candidate_blocker_closure_blocks_when_outputs_incomplete() -> None:
    sources = _source_documents()
    sources["source_2438g"]["candidate_replay_outputs_complete"] = False
    sources["candidate_replay_output_records"]["candidate_replay_output_records"][
        "candidate_replay_output_records_ready"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["candidate_output_records_complete"] is False
    assert "candidate_output_records_complete" in payload["evidence_gap_ids"]


def test_remaining_candidate_blocker_closure_blocks_when_handoff_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438g"]["replayability_handoff_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["replayability_handoff_ready"] is False
    assert "replayability_handoff_ready" in payload["evidence_gap_ids"]


def test_remaining_candidate_blocker_closure_blocks_when_blocker_reason_missing() -> None:
    sources = _source_documents()
    blocker = sources["remaining_blocker_summary"]["remaining_candidate_replay_blocker_summary"][
        "remaining_candidate_replay_blockers"
    ][0]
    blocker["blocker_reason"] = ""

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["remaining_candidate_blocker_count_after"] == 1
    assert "each_blocked_candidate_has_remaining_blocker_reason" in payload[
        "evidence_gap_ids"
    ]


def test_remaining_candidate_blocker_closure_blocks_when_closure_action_missing() -> None:
    sources = _source_documents()
    blocker = sources["remaining_blocker_summary"]["remaining_candidate_replay_blocker_summary"][
        "remaining_candidate_replay_blockers"
    ][1]
    blocker["required_next_action"] = ""

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["remaining_candidate_blocker_count_after"] == 1
    assert "each_blocked_candidate_has_closure_action" in payload["evidence_gap_ids"]


def test_remaining_candidate_blocker_closure_blocks_when_evidence_ref_missing() -> None:
    sources = _source_documents()
    candidate_id = "missed_upside_reentry_accelerator"
    for record in _records_by_id(sources, candidate_id):
        for field in (
            "evidence_ref",
            "closure_evidence_ref",
            "input_spec_ref",
            "source_traceability_ref",
            "valid_until_policy_ref",
            "outcome_linkage_key",
            "forward_aging_handoff_key",
            "closure_evidence_refs",
        ):
            record.pop(field, None)

    payload = _build_payload(sources)

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["remaining_candidate_blocker_count_after"] == 1
    assert "each_closure_action_has_evidence_ref" in payload["evidence_gap_ids"]
    assert "each_candidate_has_after_state" in payload["evidence_gap_ids"]


def test_remaining_candidate_blocker_closure_keeps_safety_boundary_false() -> None:
    payload = _build_payload(_source_documents())

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["broker_action"] == "none"
    assert payload["production_effect"] == "none"


def test_remaining_candidate_blocker_closure_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "remaining_blocker"
    docs_root = tmp_path / "docs" / "research"

    payload = RUN_IMPL(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == closure.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_passed"] is True
    for key in (
        "json_path",
        "remaining_candidate_blocker_closure_records_json",
        "remaining_candidate_blocker_before_after_matrix_json",
        "replay_recheck_readiness_handoff_json",
        "unresolved_remaining_candidate_blocker_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "remaining_candidate_blocker_closure_records_markdown",
        "remaining_candidate_blocker_before_after_matrix_markdown",
        "replay_recheck_readiness_handoff_markdown",
        "unresolved_remaining_candidate_blocker_summary_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_remaining_candidate_blocker_closure_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "remaining_blocker_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-remaining-candidate-pit-replay-blocker-closure",
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
    assert "source_2438g_blocked_recheck_ready=true" in result.output
    assert "remaining_candidate_blocker_closure_ready=true" in result.output
    assert "remaining_candidate_blocker_count_before=3" in result.output
    assert "remaining_candidate_blocker_count_after=0" in result.output
    assert "candidate_recheckable_after_closure_count=3" in result.output
    assert "replay_recheck_handoff_ready=true" in result.output
    assert "candidate_replay_pass_count=0" in result.output
    assert "candidate_replay_fail_count=0" in result.output
    assert "candidate_replay_blocked_count=3" in result.output
    assert "forward_aging_handoff_ready=false" in result.output
    assert "paper_shadow_candidate_found=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert f"next_route={closure.NEXT_ROUTE_READY}" in result.output


def test_remaining_candidate_blocker_closure_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438g"].unlink()

    payload = RUN_IMPL(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "missing_source",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == closure.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert "prior_2438g_status_is_blocked" in payload["evidence_gap_ids"]
    assert Path(payload["artifact_paths"]["no_effect_boundary_json"]).exists()


def test_remaining_candidate_blocker_closure_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[closure.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-remaining-candidate-pit-replay-blocker-closure"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert closure.REPORT_TYPE in catalog
    for reference in closure.REQUIRED_CATALOG_REFERENCES:
        assert reference in catalog
    for reference in closure.REQUIRED_SYSTEM_FLOW_REFERENCES:
        assert reference in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return BUILD_CLOSURE(
        sources["source_2438g"],
        sources["source_2438f"],
        sources["candidate_replay_output_records"],
        sources["remaining_blocker_summary"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
        as_of="2026-07-08",
    )


def _source_documents(records: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    selected_records = records or _records(3)
    blockers = _remaining_blockers(selected_records)
    return {
        "source_2438g": _source_2438g(selected_records, blockers),
        "source_2438f": _source_2438f(selected_records),
        "candidate_replay_output_records": {
            "candidate_replay_output_records": {
                "candidate_replay_output_records_ready": True,
                "records": selected_records,
            }
        },
        "remaining_blocker_summary": {
            "remaining_candidate_replay_blocker_summary": {
                "remaining_candidate_replay_blocker_summary_ready": True,
                "remaining_candidate_replay_blocker_count": len(blockers),
                "remaining_candidate_replay_blockers": blockers,
            }
        },
        "data_quality_summary": _data_quality_summary(),
        "report_registry": {
            "reports": [
                {"report_id": report_id} for report_id in closure.REQUIRED_REPORT_IDS
            ]
        },
        "artifact_catalog_text": "\n".join(closure.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(closure.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "source_2438g_doc": "2438H remaining blocker NOT_RECHECKED",
            "source_2438f_doc": "2438H remaining blocker NOT_RECHECKED",
            "candidate_output_records_doc": "2438H remaining blocker NOT_RECHECKED",
            "remaining_blocker_summary_doc": "2438H remaining blocker NOT_RECHECKED",
        },
    }


def _source_2438g(
    records: list[dict[str, Any]],
    blockers: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "status": closure.EXPECTED_2438G_STATUS,
        "recommended_next_research_task": closure.EXPECTED_2438G_ROUTE,
        "candidate_replay_outputs_complete": True,
        "replayability_handoff_ready": True,
        "candidate_replay_pass_count": 0,
        "candidate_replay_fail_count": 0,
        "candidate_replay_blocked_count": len(records),
        "blocked_candidates": records,
        "remaining_candidate_replay_blockers": blockers,
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _source_2438f(records: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": closure.EXPECTED_2438F_STATUS,
        "candidate_level_blocker_closure_ready": True,
        "candidate_level_blocker_count_after": 0,
        "replayability_handoff_ready": True,
        "candidate_level_blocker_closure_records": {
            "records": [
                {
                    "candidate_id": record["candidate_id"],
                    "closure_evidence_ref": (
                        f"TRADING-2438F:closure_record:{record['candidate_id']}"
                    ),
                    "closure_evidence_refs": [
                        f"TRADING-2438F:evidence:{record['candidate_id']}"
                    ],
                }
                for record in records
            ]
        },
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
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
        "outcome_linkage_key": f"growth_tilt_pit_replay:{candidate_id}:1d,5d,10d,20d",
        "forward_aging_handoff_key": f"TRADING-2439:forward_aging:{candidate_id}",
        "blocking_gap_ids": [
            "candidate_pit_replay_engine_available",
            "candidate_replay_input_specs_ready",
            "candidate_source_traceability_manifests_ready",
            "candidate_as_of_boundary_specs_ready",
            "candidate_valid_until_boundary_specs_ready",
            "candidate_outcome_linkage_specs_ready",
        ],
        "metric_summary": {"return_delta_vs_baseline": None},
        "status_reason": {
            "pass_reason": None,
            "fail_reason": None,
            "blocker_reason": "Candidate remains BLOCKED by replay gaps.",
        },
        "paper_shadow_candidate_found": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _remaining_blockers(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": record["candidate_id"],
            "blocker_category": [
                "missing_metric_summary",
                "unresolved_input_dependency",
                "insufficient_pit_window",
                "unresolved_source_traceability",
                "invalid_valid_until_policy",
                "missing_outcome_linkage",
                "replay_engine_execution_gap",
            ],
            "blocker_reason": "Candidate remains BLOCKED by replay gaps.",
            "required_next_action": (
                "Close TRADING-2438H remaining candidate PIT replay blocker "
                "before 2438I recheck."
            ),
            "evidence_ref": record["evidence_ref"],
            "closure_evidence_ref": f"TRADING-2438G:blocker:{record['candidate_id']}",
            "production_effect": "none",
            "broker_action": "none",
        }
        for record in records
    ]


def _selected_candidates() -> list[dict[str, Any]]:
    return [
        {"candidate_id": "recovery_reentry_speedup_guard"},
        {"candidate_id": "false_risk_off_confirmation_relaxation"},
        {"candidate_id": "missed_upside_reentry_accelerator"},
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


def _records_by_id(sources: dict[str, Any], candidate_id: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    records.extend(sources["source_2438g"]["blocked_candidates"])
    records.extend(
        sources["candidate_replay_output_records"]["candidate_replay_output_records"][
            "records"
        ]
    )
    records.extend(
        sources["remaining_blocker_summary"]["remaining_candidate_replay_blocker_summary"][
            "remaining_candidate_replay_blockers"
        ]
    )
    records.extend(
        sources["source_2438f"]["candidate_level_blocker_closure_records"]["records"]
    )
    return [record for record in records if record.get("candidate_id") == candidate_id]


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2438g": root / "source_2438g.json",
        "source_2438f": root / "source_2438f.json",
        "candidate_records": root / "candidate_replay_output_records.json",
        "remaining_blocker_summary": root / "remaining_blocker_summary.json",
        "source_2438g_doc": root / "source_2438g.md",
        "source_2438f_doc": root / "source_2438f.md",
        "candidate_records_doc": root / "candidate_replay_output_records.md",
        "remaining_blocker_summary_doc": root / "remaining_blocker_summary.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2438g"], sources["source_2438g"])
    _write_json(paths["source_2438f"], sources["source_2438f"])
    _write_json(paths["candidate_records"], sources["candidate_replay_output_records"])
    _write_json(paths["remaining_blocker_summary"], sources["remaining_blocker_summary"])
    for key, text in sources["research_doc_texts"].items():
        path_key = {
            "source_2438g_doc": "source_2438g_doc",
            "source_2438f_doc": "source_2438f_doc",
            "candidate_output_records_doc": "candidate_records_doc",
            "remaining_blocker_summary_doc": "remaining_blocker_summary_doc",
        }[key]
        paths[path_key].write_text(text, encoding="utf-8")
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in closure.REQUIRED_REPORT_IDS
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
        "source_2438g_blocked_recheck_path": paths["source_2438g"],
        "source_2438f_candidate_level_blocker_closure_path": paths["source_2438f"],
        "candidate_replay_output_records_path": paths["candidate_records"],
        "remaining_candidate_replay_blocker_summary_path": paths[
            "remaining_blocker_summary"
        ],
        "source_2438g_doc_path": paths["source_2438g_doc"],
        "source_2438f_doc_path": paths["source_2438f_doc"],
        "candidate_output_records_doc_path": paths["candidate_records_doc"],
        "remaining_blocker_summary_doc_path": paths["remaining_blocker_summary_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2438g-blocked-recheck",
        str(paths["source_2438g"]),
        "--source-2438f-candidate-level-blocker-closure",
        str(paths["source_2438f"]),
        "--candidate-replay-output-records",
        str(paths["candidate_records"]),
        "--remaining-candidate-replay-blocker-summary",
        str(paths["remaining_blocker_summary"]),
        "--source-2438g-doc",
        str(paths["source_2438g_doc"]),
        "--source-2438f-doc",
        str(paths["source_2438f_doc"]),
        "--candidate-output-records-doc",
        str(paths["candidate_records_doc"]),
        "--remaining-blocker-summary-doc",
        str(paths["remaining_blocker_summary_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
