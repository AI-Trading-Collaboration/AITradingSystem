from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_top3_candidate_pit_replay_engine_remediation as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_top3_candidate_pit_replay_engine_remediation as remediation,
)


def test_remediation_builder_reads_2440_blocked_gate_not_no_candidate() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["blocked_by_forward_aging_gate"] is True
    assert payload["not_no_candidate_status"] is True
    assert payload["paper_shadow_candidate_found"] is False
    assert payload["remediation_ready"] is False
    assert payload["candidate_selection_resolves"] is True
    assert payload["top3_candidate_ids_present"] is True
    assert payload["pit_replay_artifacts_present"] is True
    assert payload["unresolved_engine_blocker_count"] >= 6
    assert payload["recommended_next_research_task"] == remediation.NEXT_ROUTE_BLOCKED


def test_remediation_builder_locates_2438_2439_engine_blockers() -> None:
    payload = _build_payload(_source_documents())

    assert payload["source_2439_blocked_by_pit_replay_gate"] is True
    assert payload["source_2438_replay_engine_blocked"] is True
    assert "candidate_pit_replay_engine_available" in payload["evidence_gap_ids"]
    assert "candidate_replay_input_specs_ready" in payload["evidence_gap_ids"]
    assert "source_traceability_complete" in payload["evidence_gap_ids"]
    assert "as_of_boundary_explicit" in payload["evidence_gap_ids"]
    assert "valid_until_boundary_explicit" in payload["evidence_gap_ids"]
    assert "forward_aging_handoff_ready" in payload["evidence_gap_ids"]


def test_remediation_blocks_when_candidate_selection_missing() -> None:
    sources = _source_documents(ready=True)
    sources["source_2438_pit_replay"]["top3_candidate_selection_ready"] = False
    sources["source_2438_pit_replay"]["selected_candidates"] = []

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "candidate_selection_resolves" in payload["evidence_gap_ids"]
    assert "top3_candidate_ids_present" in payload["evidence_gap_ids"]


def test_remediation_blocks_when_candidate_ids_missing() -> None:
    sources = _source_documents(ready=True)
    sources["source_2438_pit_replay"]["selected_candidates"][0]["candidate_id"] = None

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "top3_candidate_ids_present" in payload["evidence_gap_ids"]


def test_remediation_blocks_when_pit_replay_evidence_missing() -> None:
    sources = _source_documents(ready=True)
    sources["pit_replay_evidence"] = {}
    sources["source_2438_pit_replay"].pop("pit_replay_evidence")

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "pit_replay_artifacts_present" in payload["evidence_gap_ids"]
    assert "pit_replay_evidence_complete" in payload["evidence_gap_ids"]


def test_remediation_blocks_when_source_traceability_incomplete() -> None:
    sources = _source_documents(ready=True)
    _evidence_rows(sources)[0]["source_traceability_verified"] = False

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "source_traceability_complete" in payload["evidence_gap_ids"]


def test_remediation_blocks_when_as_of_boundary_missing() -> None:
    sources = _source_documents(ready=True)
    _evidence_rows(sources)[0]["as_of_boundary_verified"] = False

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "as_of_boundary_explicit" in payload["evidence_gap_ids"]


def test_remediation_blocks_when_valid_until_boundary_missing() -> None:
    sources = _source_documents(ready=True)
    _evidence_rows(sources)[0]["valid_until_boundary_verified"] = False

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "valid_until_boundary_explicit" in payload["evidence_gap_ids"]


def test_remediation_blocks_when_outcome_linkage_missing() -> None:
    sources = _source_documents(ready=True)
    _evidence_rows(sources)[0]["outcome_linkage_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "outcome_linkage_complete" in payload["evidence_gap_ids"]


def test_remediation_blocks_when_forward_aging_handoff_missing() -> None:
    sources = _source_documents(ready=True)
    sources["source_2438_pit_replay"][
        "recommended_next_research_task"
    ] = remediation.EXPECTED_2440_ROUTE

    payload = _build_payload(sources)

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert "forward_aging_handoff_ready" in payload["evidence_gap_ids"]


def test_remediation_ready_when_replay_evidence_complete() -> None:
    payload = _build_payload(_source_documents(ready=True))

    assert payload["status"] == remediation.READY_STATUS
    assert payload["remediation_ready"] is True
    assert payload["remediation_gap_count"] == 0
    assert payload["unresolved_engine_blocker_count"] == 0
    assert payload["pit_replay_engine_ready"] is True
    assert payload["pit_replay_evidence_complete"] is True
    assert payload["source_traceability_complete"] is True
    assert payload["as_of_boundary_explicit"] is True
    assert payload["valid_until_boundary_explicit"] is True
    assert payload["forward_aging_handoff_ready"] is True
    assert payload["recommended_next_research_task"] == remediation.NEXT_ROUTE_READY


def test_ready_path_still_has_no_paper_shadow_or_trading_advice() -> None:
    payload = _build_payload(_source_documents(ready=True))

    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_schedule_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["broker_order_generated"] is False
    assert payload["portfolio_weight_mutated"] is False


def test_remediation_next_route_deterministic() -> None:
    blocked = _build_payload(_source_documents())
    ready = _build_payload(_source_documents(ready=True))

    assert blocked["recommended_next_research_task"] == remediation.NEXT_ROUTE_BLOCKED
    assert ready["recommended_next_research_task"] == remediation.NEXT_ROUTE_READY


def test_remediation_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "remediation"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_top3_candidate_pit_replay_engine_remediation(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is True
    assert payload["data_quality_gate_passed"] is True
    assert payload["blocked_by_forward_aging_gate"] is True
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "remediation_evidence_json",
        "before_after_comparison_json",
        "remaining_blocker_summary_json",
        "no_effect_boundary_json",
        "markdown_path",
        "remediation_evidence_markdown",
        "before_after_comparison_markdown",
        "remaining_blocker_summary_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_remediation_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "remediation_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-top3-candidate-pit-replay-engine-remediation",
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
    assert remediation.BLOCKED_STATUS in result.output
    assert "blocked_by_forward_aging_gate=true" in result.output
    assert "not_no_candidate_status=true" in result.output
    assert "data_quality_gate_executed=true" in result.output
    assert "data_quality_gate_passed=true" in result.output
    assert "candidate_selection_resolves=true" in result.output
    assert "pit_replay_engine_ready=false" in result.output
    assert "remediation_ready=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={remediation.NEXT_ROUTE_BLOCKED}" in result.output
    assert (output_root / "remediation_result.json").exists()


def test_remediation_missing_source_artifact_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2440"].unlink()

    payload = impl.run_growth_tilt_top3_candidate_pit_replay_engine_remediation(
        **_source_kwargs(paths),
        data_quality_summary_path=paths["data_quality_summary"],
        output_root=tmp_path / "outputs" / "missing_source",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == remediation.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["blocked_by_forward_aging_gate"] is False
    assert "prior_2440_blocked_by_forward_aging_gate" in payload["evidence_gap_ids"]
    assert Path(payload["artifact_paths"]["remaining_blocker_summary_json"]).exists()


def test_remediation_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[remediation.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-top3-candidate-pit-replay-engine-remediation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("remediation_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2438B_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert remediation.REPORT_TYPE in catalog
    assert "growth-tilt-top3-candidate-pit-replay-engine-remediation" in system_flow
    assert remediation.READY_STATUS in system_flow
    assert remediation.BLOCKED_STATUS in system_flow
    assert remediation.NEXT_ROUTE_BLOCKED in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return remediation.build_growth_tilt_top3_candidate_pit_replay_engine_remediation(
        sources["source_2440_promotion_review"],
        sources["source_2439_forward_pack"],
        sources["source_2438_pit_replay"],
        sources["pit_replay_evidence"],
        sources["pit_replay_blocker_summary"],
        sources["data_quality_summary"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents(*, ready: bool = False) -> dict[str, Any]:
    source_2438 = _source_2438(ready=ready)
    return {
        "source_2440_promotion_review": {
            "status": remediation.EXPECTED_2440_STATUS,
            "recommended_next_research_task": remediation.EXPECTED_2440_ROUTE,
            "paper_shadow_candidate_found": False,
            "paper_shadow_candidate_count": 0,
            "forward_aging_source_status": remediation.EXPECTED_2439_BLOCKED_STATUS,
            "pit_replay_source_status": remediation.EXPECTED_2438_BLOCKED_STATUS,
        },
        "source_2439_forward_pack": {
            "status": remediation.EXPECTED_2439_BLOCKED_STATUS,
            "recommended_next_research_task": remediation.EXPECTED_2440_ROUTE,
            "forward_aging_candidate_pack_ready": False,
            "forward_aging_candidate_count": 0,
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
                for report_id in remediation.REQUIRED_REPORT_IDS
            ]
        },
        "artifact_catalog_text": "\n".join(remediation.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(remediation.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "source_2440_doc": "PIT replay forward aging paper-shadow",
            "source_2439_doc": "PIT replay forward aging handoff",
            "source_2438_doc": "PIT replay forward aging engine remediation",
        },
    }


def _source_2438(*, ready: bool) -> dict[str, Any]:
    selected = _selected_candidates()
    rows = _evidence_rows_for(selected, ready=ready)
    status = (
        remediation.EXPECTED_2438_READY_STATUS
        if ready
        else remediation.EXPECTED_2438_BLOCKED_STATUS
    )
    gap_ids = [] if ready else list(remediation.REMEDIATION_REQUIREMENT_IDS[3:9])
    evidence = {
        "schema_version": "growth_tilt_top3_candidate_pit_replay_evidence.v1",
        "status": status,
        "pit_replay_evidence_ready": True,
        "pit_replay_executed": ready,
        "pit_candidates_tested": 3 if ready else 0,
        "pit_replay_pass_count": 3 if ready else 0,
        "pit_replay_fail_count": 0,
        "pit_replay_blocked_count": 0 if ready else 3,
        "rows": rows,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {
        "schema_version": "growth_tilt_top3_candidate_pit_replay.v1",
        "status": status,
        "recommended_next_research_task": remediation.EXPECTED_2438_READY_ROUTE
        if ready
        else remediation.EXPECTED_2440_ROUTE,
        "top3_candidate_selection_ready": True,
        "selected_candidates": selected,
        "candidate_pit_replay_engine_available": ready,
        "candidate_replay_input_specs_ready": ready,
        "candidate_source_traceability_manifests_ready": ready,
        "candidate_as_of_boundary_specs_ready": ready,
        "candidate_valid_until_boundary_specs_ready": ready,
        "candidate_outcome_linkage_specs_ready": ready,
        "pit_candidates_tested": 3 if ready else 0,
        "pit_replay_pass_count": 3 if ready else 0,
        "pit_replay_blocked_count": 0 if ready else 3,
        "pit_replay_executed": ready,
        "pit_replay_evidence": evidence,
        "pit_replay_blocker_summary": {
            "schema_version": "growth_tilt_top3_candidate_pit_replay_blocker_summary.v1",
            "status": status,
            "blocked": not ready,
            "blocking_gap_ids": gap_ids,
            "blocking_gap_count": len(gap_ids),
            "next_route": remediation.EXPECTED_2438_READY_ROUTE
            if ready
            else remediation.EXPECTED_2440_ROUTE,
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


def _evidence_rows_for(
    selected: list[dict[str, Any]],
    *,
    ready: bool,
) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": candidate["candidate_id"],
            "pit_replay_status": "pass" if ready else "blocked_replay_engine_gap",
            "source_traceability_verified": ready,
            "as_of_boundary_verified": ready,
            "valid_until_boundary_verified": ready,
            "outcome_linkage_ready": ready,
            "pit_replay_passed": ready,
            "blocking_gap_ids": []
            if ready
            else list(remediation.REMEDIATION_REQUIREMENT_IDS[3:9]),
            "production_effect": "none",
            "broker_action": "none",
        }
        for candidate in selected
    ]


def _evidence_rows(sources: dict[str, Any]) -> list[dict[str, Any]]:
    return sources["pit_replay_evidence"]["pit_replay_evidence"]["rows"]


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
        "source_2440": root / "promotion_review_result.json",
        "source_2439": root / "forward_aging_candidate_pack_result.json",
        "source_2438": root / "top3_candidate_pit_replay_result.json",
        "pit_replay_evidence": root / "pit_replay_evidence.json",
        "pit_replay_blocker_summary": root / "pit_replay_blocker_summary.json",
        "source_2440_doc": root / "promotion_review.md",
        "source_2439_doc": root / "forward_pack.md",
        "source_2438_doc": root / "pit_replay.md",
        "pit_replay_evidence_doc": root / "pit_replay_evidence.md",
        "pit_replay_blocker_doc": root / "pit_replay_blocker.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
        "data_quality_summary": root / "data_quality_summary.json",
    }
    _write_json(paths["source_2440"], sources["source_2440_promotion_review"])
    _write_json(paths["source_2439"], sources["source_2439_forward_pack"])
    _write_json(paths["source_2438"], sources["source_2438_pit_replay"])
    _write_json(paths["pit_replay_evidence"], sources["pit_replay_evidence"])
    _write_json(
        paths["pit_replay_blocker_summary"],
        sources["pit_replay_blocker_summary"],
    )
    for key in (
        "source_2440_doc",
        "source_2439_doc",
        "source_2438_doc",
        "pit_replay_evidence_doc",
        "pit_replay_blocker_doc",
    ):
        paths[key].write_text("PIT replay forward aging remediation", encoding="utf-8")
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}"
            for report_id in remediation.REQUIRED_REPORT_IDS
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
        "source_2440_promotion_review_path": paths["source_2440"],
        "source_2439_forward_pack_path": paths["source_2439"],
        "source_2438_pit_replay_path": paths["source_2438"],
        "pit_replay_evidence_path": paths["pit_replay_evidence"],
        "pit_replay_blocker_summary_path": paths["pit_replay_blocker_summary"],
        "source_2440_doc_path": paths["source_2440_doc"],
        "source_2439_doc_path": paths["source_2439_doc"],
        "source_2438_doc_path": paths["source_2438_doc"],
        "pit_replay_evidence_doc_path": paths["pit_replay_evidence_doc"],
        "pit_replay_blocker_doc_path": paths["pit_replay_blocker_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2440-promotion-review",
        str(paths["source_2440"]),
        "--source-2439-forward-pack",
        str(paths["source_2439"]),
        "--source-2438-pit-replay",
        str(paths["source_2438"]),
        "--pit-replay-evidence",
        str(paths["pit_replay_evidence"]),
        "--pit-replay-blocker-summary",
        str(paths["pit_replay_blocker_summary"]),
        "--source-2440-doc",
        str(paths["source_2440_doc"]),
        "--source-2439-doc",
        str(paths["source_2439_doc"]),
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
