from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_top3_candidate_pit_replay_recheck as recheck,
)


def test_recheck_reads_2438b_closure_but_blocks_on_current_replay_rows() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["source_2438b_blocker_closure_ready"] is True
    assert payload["source_2438a_remediation_blocked"] is True
    assert payload["source_2438_pit_replay_blocked"] is True
    assert payload["top3_candidate_selection_resolves"] is True
    assert payload["pit_replay_evidence_exists"] is True
    assert payload["candidate_replay_outputs_complete"] is False
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 0
    assert payload["candidate_replay_blocked_count"] == 3
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_BLOCKED
    assert "candidate_replay_outputs" in _remaining_blocker_ids(payload)


def test_recheck_blocks_when_2438b_closure_is_not_ready() -> None:
    sources = _source_documents()
    sources["source_2438b_blocker_closure"]["blocker_count_after"] = 1

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["source_2438b_blocker_closure_ready"] is False
    assert "source_2438b_blocker_closure_ready" in payload["evidence_gap_ids"]
    assert "blocker_closure" in _remaining_blocker_ids(payload)


@pytest.mark.parametrize(
    ("field", "requirement_id"),
    [
        ("pit_replay_engine_ready", "pit_replay_engine_ready"),
        ("input_specs_ready", "input_specs_ready"),
        ("evidence_completeness_ready", "evidence_completeness_ready"),
        ("source_traceability_ready", "source_traceability_ready"),
        ("as_of_boundary_ready", "as_of_boundary_ready"),
        ("valid_until_boundary_ready", "valid_until_boundary_ready"),
        ("outcome_linkage_ready", "outcome_linkage_ready"),
        ("forward_aging_handoff_ready", "forward_aging_handoff_ready"),
    ],
)
def test_recheck_blocks_when_any_2438b_contract_flag_is_missing(
    field: str,
    requirement_id: str,
) -> None:
    sources = _source_documents()
    sources["source_2438b_blocker_closure"][field] = False

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload[field] is False
    assert requirement_id in payload["evidence_gap_ids"]


def test_recheck_blocks_when_top3_selection_is_missing() -> None:
    sources = _source_documents()
    sources["source_2438_pit_replay"]["selected_candidates"] = _selected_candidates()[:2]

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["top3_candidate_selection_resolves"] is False
    assert "top3_candidate_selection" in _remaining_blocker_ids(payload)


def test_recheck_blocks_when_pit_replay_evidence_is_missing() -> None:
    sources = _source_documents()
    sources["pit_replay_evidence"] = {"status": "", "pit_replay_evidence": {"rows": []}}

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["pit_replay_evidence_exists"] is False
    assert "pit_replay_evidence" in _remaining_blocker_ids(payload)


def test_recheck_counts_pass_fail_and_blocked_candidates() -> None:
    sources = _source_documents()
    _set_replay_rows(
        sources,
        [
            _replay_row("recovery_reentry_speedup_guard", "pass"),
            _replay_row("false_risk_off_confirmation_relaxation", "fail"),
            _replay_row(
                "missed_upside_reentry_accelerator",
                "blocked_replay_engine_gap",
                blocking_gap_ids=["candidate_replay_outputs_complete"],
            ),
        ],
    )

    payload = _build_payload(sources)

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["candidate_replay_pass_count"] == 1
    assert payload["candidate_replay_fail_count"] == 1
    assert payload["candidate_replay_blocked_count"] == 1
    assert payload["candidate_replay_outputs_complete"] is False


def test_recheck_routes_to_no_passing_candidate_when_complete_without_pass() -> None:
    sources = _source_documents()
    _set_replay_rows(
        sources,
        [
            _replay_row("recovery_reentry_speedup_guard", "fail"),
            _replay_row("false_risk_off_confirmation_relaxation", "fail"),
            _replay_row("missed_upside_reentry_accelerator", "fail"),
        ],
    )

    payload = _build_payload(sources)

    assert payload["status"] == recheck.NO_PASSING_CANDIDATE_STATUS
    assert payload["pit_replay_recheck_ready"] is True
    assert payload["candidate_replay_outputs_complete"] is True
    assert payload["candidate_replay_pass_count"] == 0
    assert payload["candidate_replay_fail_count"] == 3
    assert payload["candidate_replay_blocked_count"] == 0
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_NO_PASS
    assert payload["paper_shadow_candidate_found"] is False


def test_recheck_routes_to_ready_when_complete_with_passing_candidate() -> None:
    sources = _source_documents()
    _set_replay_rows(
        sources,
        [
            _replay_row("recovery_reentry_speedup_guard", "pass"),
            _replay_row("false_risk_off_confirmation_relaxation", "fail"),
            _replay_row("missed_upside_reentry_accelerator", "fail"),
        ],
    )

    payload = _build_payload(sources)

    assert payload["status"] == recheck.READY_STATUS
    assert payload["pit_replay_recheck_ready"] is True
    assert payload["candidate_replay_outputs_complete"] is True
    assert payload["candidate_replay_pass_count"] == 1
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE_READY
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_trading_advice"] is False


def test_recheck_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "recheck"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_top3_candidate_pit_replay_recheck(
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
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "candidate_replay_evidence_json",
        "candidate_replay_summary_json",
        "remaining_recheck_blocker_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "candidate_replay_evidence_markdown",
        "candidate_replay_summary_markdown",
        "remaining_recheck_blocker_summary_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_recheck_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "recheck_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-top3-candidate-pit-replay-recheck",
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
    assert recheck.BLOCKED_STATUS in result.output
    assert "source_2438b_blocker_closure_ready=true" in result.output
    assert "pit_replay_recheck_ready=false" in result.output
    assert "candidate_replay_outputs_complete=false" in result.output
    assert "candidate_replay_pass_count=0" in result.output
    assert "candidate_replay_fail_count=0" in result.output
    assert "candidate_replay_blocked_count=3" in result.output
    assert "paper_shadow_candidate_found=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={recheck.NEXT_ROUTE_BLOCKED}" in result.output
    assert (output_root / "pit_replay_recheck_result.json").exists()


def test_recheck_missing_source_artifact_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2438b"].unlink()

    payload = impl.run_growth_tilt_top3_candidate_pit_replay_recheck(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "missing",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == recheck.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert "source_2438b_blocker_closure_ready" in payload["evidence_gap_ids"]
    assert Path(payload["artifact_paths"]["remaining_recheck_blocker_summary_json"]).exists()


def test_recheck_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[recheck.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-top3-candidate-pit-replay-recheck"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    for reference in recheck.REQUIRED_CATALOG_REFERENCES:
        assert reference in "\n".join(entry["artifact_globs"]) or reference.startswith(
            "aits "
        )

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert recheck.REPORT_TYPE in catalog
    assert "growth-tilt-top3-candidate-pit-replay-recheck" in system_flow
    assert recheck.READY_STATUS in system_flow
    assert recheck.BLOCKED_STATUS in system_flow
    assert recheck.NO_PASSING_CANDIDATE_STATUS in system_flow
    assert recheck.NEXT_ROUTE_READY in system_flow
    assert recheck.NEXT_ROUTE_NO_PASS in system_flow
    assert recheck.NEXT_ROUTE_BLOCKED in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return recheck.build_growth_tilt_top3_candidate_pit_replay_recheck(
        sources["source_2438b_blocker_closure"],
        sources["source_2438a_remediation"],
        sources["source_2438_pit_replay"],
        sources["pit_replay_evidence"],
        sources["pit_replay_blocker_summary"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
        as_of="2026-07-08",
    )


def _source_documents() -> dict[str, Any]:
    source_2438 = _source_2438_blocked()
    return {
        "source_2438b_blocker_closure": _source_2438b_ready(),
        "source_2438a_remediation": {
            "status": recheck.EXPECTED_2438A_STATUS,
            "recommended_next_research_task": (
                "TRADING-2438B_Growth_Tilt_PIT_Replay_Engine_Blocker_Closure"
            ),
            "not_no_candidate_status": True,
            "paper_shadow_candidate_found": False,
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
        "data_quality_summary": _data_quality_summary(),
        "report_registry": {
            "reports": [
                {"report_id": report_id}
                for report_id in recheck.REQUIRED_REPORT_IDS
            ]
        },
        "artifact_catalog_text": "\n".join(recheck.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(recheck.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "source_2438b_doc": "PIT replay recheck handoff",
            "source_2438a_doc": "PIT replay remediation handoff",
            "source_2438_doc": "PIT replay evidence handoff",
            "pit_replay_evidence_doc": "PIT replay evidence handoff",
            "pit_replay_blocker_doc": "PIT replay blocker handoff",
        },
    }


def _source_2438b_ready() -> dict[str, Any]:
    return {
        "status": recheck.EXPECTED_2438B_STATUS,
        "recommended_next_research_task": recheck.EXPECTED_2438B_ROUTE,
        "blocker_closure_ready": True,
        "blocker_count_before": 8,
        "blocker_count_after": 0,
        "pit_replay_engine_ready": True,
        "input_specs_ready": True,
        "evidence_completeness_ready": True,
        "source_traceability_ready": True,
        "as_of_boundary_ready": True,
        "valid_until_boundary_ready": True,
        "outcome_linkage_ready": True,
        "forward_aging_handoff_ready": True,
        "paper_shadow_candidate_found": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _source_2438_blocked() -> dict[str, Any]:
    selected = _selected_candidates()
    rows = [
        _replay_row(
            candidate["candidate_id"],
            "blocked_replay_engine_gap",
            blocking_gap_ids=["candidate_replay_outputs_complete"],
        )
        for candidate in selected
    ]
    return {
        "schema_version": "growth_tilt_top3_candidate_pit_replay.v1",
        "status": recheck.EXPECTED_2438_STATUS,
        "recommended_next_research_task": (
            "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation"
        ),
        "selected_candidates": selected,
        "pit_candidates_tested": 0,
        "pit_replay_pass_count": 0,
        "pit_replay_fail_count": 0,
        "pit_replay_blocked_count": 3,
        "pit_replay_executed": False,
        "pit_replay_evidence": {
            "schema_version": "growth_tilt_top3_candidate_pit_replay_evidence.v1",
            "status": recheck.EXPECTED_2438_STATUS,
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
            "status": recheck.EXPECTED_2438_STATUS,
            "blocked": True,
            "blocking_gap_ids": ["candidate_replay_outputs_complete"],
            "blocking_gap_count": 1,
            "next_route": (
                "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation"
            ),
            "production_effect": "none",
            "broker_action": "none",
        },
    }


def _set_replay_rows(sources: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    pass_count = sum(row["pit_replay_status"] == "pass" for row in rows)
    fail_count = sum(row["pit_replay_status"] == "fail" for row in rows)
    blocked_count = len(rows) - pass_count - fail_count
    source_2438 = sources["source_2438_pit_replay"]
    source_2438.update(
        {
            "status": "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_READY",
            "recommended_next_research_task": (
                "TRADING-2439_Growth_Tilt_Forward_Aging_Candidate_Pack"
            ),
            "pit_candidates_tested": len(rows),
            "pit_replay_pass_count": pass_count,
            "pit_replay_fail_count": fail_count,
            "pit_replay_blocked_count": blocked_count,
            "pit_replay_executed": True,
        }
    )
    evidence = {
        "schema_version": "growth_tilt_top3_candidate_pit_replay_evidence.v1",
        "status": source_2438["status"],
        "pit_replay_evidence_ready": True,
        "pit_replay_executed": True,
        "pit_candidates_tested": len(rows),
        "pit_replay_pass_count": pass_count,
        "pit_replay_fail_count": fail_count,
        "pit_replay_blocked_count": blocked_count,
        "rows": rows,
        "production_effect": "none",
        "broker_action": "none",
    }
    blocker = {
        "schema_version": "growth_tilt_top3_candidate_pit_replay_blocker_summary.v1",
        "status": source_2438["status"],
        "blocked": blocked_count > 0,
        "blocking_gap_ids": (
            ["candidate_replay_outputs_complete"] if blocked_count else []
        ),
        "blocking_gap_count": blocked_count,
        "next_route": "TRADING-2439_Growth_Tilt_Forward_Aging_Candidate_Pack",
        "production_effect": "none",
        "broker_action": "none",
    }
    source_2438["pit_replay_evidence"] = evidence
    source_2438["pit_replay_blocker_summary"] = blocker
    sources["pit_replay_evidence"] = {
        "status": source_2438["status"],
        "pit_replay_evidence": evidence,
    }
    sources["pit_replay_blocker_summary"] = {
        "status": source_2438["status"],
        "pit_replay_blocker_summary": blocker,
    }


def _replay_row(
    candidate_id: str,
    status: str,
    *,
    blocking_gap_ids: list[str] | None = None,
) -> dict[str, Any]:
    passed = status == "pass"
    return {
        "candidate_id": candidate_id,
        "pit_replay_status": status,
        "source_traceability_verified": not blocking_gap_ids,
        "as_of_boundary_verified": not blocking_gap_ids,
        "valid_until_boundary_verified": not blocking_gap_ids,
        "outcome_linkage_ready": not blocking_gap_ids,
        "pit_replay_passed": passed,
        "blocking_gap_ids": blocking_gap_ids or [],
        "production_effect": "none",
        "broker_action": "none",
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
    return {
        str(gap.get("blocker_id"))
        for gap in payload["remaining_recheck_blockers"]
        if gap.get("blocker_id")
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2438b": root / "blocker_closure_result.json",
        "source_2438a": root / "remediation_result.json",
        "source_2438": root / "top3_candidate_pit_replay_result.json",
        "pit_replay_evidence": root / "pit_replay_evidence.json",
        "pit_replay_blocker_summary": root / "pit_replay_blocker_summary.json",
        "source_2438b_doc": root / "blocker_closure.md",
        "source_2438a_doc": root / "remediation.md",
        "source_2438_doc": root / "pit_replay.md",
        "pit_replay_evidence_doc": root / "pit_replay_evidence.md",
        "pit_replay_blocker_doc": root / "pit_replay_blocker.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2438b"], sources["source_2438b_blocker_closure"])
    _write_json(paths["source_2438a"], sources["source_2438a_remediation"])
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
            for report_id in recheck.REQUIRED_REPORT_IDS
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
        "source_2438b_blocker_closure_path": paths["source_2438b"],
        "source_2438a_remediation_path": paths["source_2438a"],
        "source_2438_pit_replay_path": paths["source_2438"],
        "pit_replay_evidence_path": paths["pit_replay_evidence"],
        "pit_replay_blocker_summary_path": paths["pit_replay_blocker_summary"],
        "source_2438b_doc_path": paths["source_2438b_doc"],
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
        "--source-2438b-blocker-closure",
        str(paths["source_2438b"]),
        "--source-2438a-remediation",
        str(paths["source_2438a"]),
        "--source-2438-pit-replay",
        str(paths["source_2438"]),
        "--pit-replay-evidence",
        str(paths["pit_replay_evidence"]),
        "--pit-replay-blocker-summary",
        str(paths["pit_replay_blocker_summary"]),
        "--source-2438b-doc",
        str(paths["source_2438b_doc"]),
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
