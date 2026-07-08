from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_candidate_promotion_evidence_review as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_candidate_promotion_evidence_review as review,
)


def test_candidate_promotion_evidence_review_builder_no_promotion() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == review.NO_PROMOTION_STATUS
    assert payload["promotion_evidence_review_ready"] is True
    assert payload["promotion_candidate_found"] is False
    assert payload["promotion_candidate_count"] == 0
    assert payload["candidate_count"] == 1
    assert payload["engineering_readiness_is_alpha_evidence"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE_NO_CANDIDATE


def test_candidate_promotion_evidence_review_can_route_candidate_found() -> None:
    sources = _source_documents()
    sources["candidate_registry"]["safety_boundary"]["paper_shadow_allowed"] = True
    sources["candidate_registry"]["candidate_families"][0][
        "paper_shadow_allowed"
    ] = True
    sources["prior_candidate_evidence"]["paper_shadow_approved"] = True
    sources["prior_candidate_evidence"]["paper_shadow_allowed"] = True
    sources["prior_candidate_evidence"]["research_only_observation_approved"] = True
    sources["prior_candidate_evidence"]["current_best_candidate_previous_decision"] = (
        "ACCEPT_FOR_SHADOW_RESEARCH"
    )

    payload = _build_payload(sources)

    assert payload["status"] == review.PROMOTION_CANDIDATE_FOUND_STATUS
    assert payload["promotion_candidate_found"] is True
    assert payload["promotion_candidate_count"] == 1
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE_CANDIDATE_FOUND


def test_candidate_promotion_evidence_review_does_not_promote_without_candidate_allowance() -> None:
    sources = _source_documents()
    sources["candidate_registry"]["safety_boundary"]["paper_shadow_allowed"] = True
    sources["prior_candidate_evidence"]["paper_shadow_approved"] = True
    sources["prior_candidate_evidence"]["paper_shadow_allowed"] = True
    sources["prior_candidate_evidence"]["research_only_observation_approved"] = True
    sources["prior_candidate_evidence"]["current_best_candidate_previous_decision"] = (
        "ACCEPT_FOR_SHADOW_RESEARCH"
    )

    payload = _build_payload(sources)

    assert payload["status"] == review.NO_PROMOTION_STATUS
    assert payload["promotion_candidate_found"] is False
    assert payload["promotion_candidate_count"] == 0
    assert (
        "candidate_registry_paper_shadow_allowed_false"
        in payload["candidate_evidence_matrix"]["candidates"][0]["promotion_blockers"]
    )


def test_candidate_promotion_review_requires_shadow_research_accept() -> None:
    sources = _source_documents()
    sources["candidate_registry"]["safety_boundary"]["paper_shadow_allowed"] = True
    sources["candidate_registry"]["candidate_families"][0][
        "paper_shadow_allowed"
    ] = True
    sources["prior_candidate_evidence"]["paper_shadow_approved"] = True
    sources["prior_candidate_evidence"]["paper_shadow_allowed"] = True
    sources["prior_candidate_evidence"]["research_only_observation_approved"] = True

    payload = _build_payload(sources)

    assert payload["status"] == review.NO_PROMOTION_STATUS
    assert payload["promotion_candidate_found"] is False
    assert payload["promotion_candidate_count"] == 0
    assert (
        "prior_decision_not_shadow_research_accept"
        in payload["candidate_evidence_matrix"]["candidates"][0]["promotion_blockers"]
    )


def test_candidate_promotion_evidence_review_blocks_prior_not_ready() -> None:
    sources = _source_documents()
    sources["forward_outcome_boundary_result_2429"][
        "forward_outcome_binding_boundary_ready"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == review.BLOCKED_STATUS
    assert payload["promotion_evidence_review_ready"] is False
    assert "prior_2429_forward_outcome_boundary_ready" in (
        payload["promotion_evidence_review_gap_ids"]
    )
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE_BLOCKED


def test_candidate_promotion_evidence_review_blocks_registry_gap() -> None:
    sources = _source_documents()
    sources["candidate_registry"]["candidate_families"] = []

    payload = _build_payload(sources)

    assert payload["status"] == review.BLOCKED_STATUS
    assert "candidate_registry_ready" in payload["promotion_evidence_review_gap_ids"]
    assert payload["candidate_registry_ready"] is False


def test_candidate_promotion_evidence_review_wrapper_writes_outputs(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "promotion_review"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_candidate_promotion_evidence_review(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == review.NO_PROMOTION_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["promotion_evidence_review_ready"] is True
    assert payload["promotion_candidate_found"] is False
    assert payload["promotion_candidate_count"] == 0

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "candidate_evidence_matrix_json",
        "candidate_decision_summary_json",
        "no_promotion_rationale_json",
        "no_effect_boundary_json",
        "markdown_path",
        "candidate_evidence_matrix_markdown",
        "candidate_decision_summary_markdown",
        "no_promotion_rationale_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_candidate_promotion_evidence_review_cli_deterministic(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "promotion_review_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-candidate-promotion-evidence-review",
            *_source_args(paths),
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
    assert review.NO_PROMOTION_STATUS in result.output
    assert "promotion_evidence_review_ready=true" in result.output
    assert "promotion_candidate_found=false" in result.output
    assert "promotion_candidate_count=0" in result.output
    assert "engineering_readiness_is_alpha_evidence=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={review.NEXT_ROUTE_NO_CANDIDATE}" in result.output
    assert (output_root / "promotion_evidence_review_result.json").exists()


def test_candidate_promotion_evidence_review_missing_prior_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["forward_outcome_boundary_result_2429"].unlink()

    payload = impl.run_growth_tilt_engine_candidate_promotion_evidence_review(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == review.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["promotion_evidence_review_started"] is False
    assert payload["promotion_evidence_review_ready"] is False
    assert payload["promotion_evidence_review_gap_count"] == 1
    assert payload["missing_promotion_review_evidence_count"] == 1
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == review.NEXT_ROUTE_BLOCKED


def test_candidate_promotion_evidence_review_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[review.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-candidate-promotion-evidence-review"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "promotion_evidence_review_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2431_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert review.REPORT_TYPE in catalog
    assert "growth-tilt-engine-candidate-promotion-evidence-review" in system_flow
    assert review.NO_PROMOTION_STATUS in system_flow
    assert review.NEXT_ROUTE_NO_CANDIDATE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return review.build_growth_tilt_engine_candidate_promotion_evidence_review(
        sources["schedule_dry_run_result_2426"],
        sources["manual_review_packet_dry_run_result_2427"],
        sources["observe_only_boundary_result_2428"],
        sources["forward_outcome_boundary_result_2429"],
        sources["candidate_registry"],
        sources["prior_candidate_evidence"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "schedule_dry_run_result_2426": {
            "status": review.EXPECTED_2426_STATUS,
            "paper_shadow_schedule_dry_run_ready": True,
        },
        "manual_review_packet_dry_run_result_2427": {
            "status": review.EXPECTED_2427_STATUS,
            "manual_review_packet_dry_run_ready": True,
        },
        "observe_only_boundary_result_2428": {
            "status": review.EXPECTED_2428_STATUS,
            "observe_only_signal_artifact_boundary_ready": True,
        },
        "forward_outcome_boundary_result_2429": {
            "status": review.EXPECTED_2429_STATUS,
            "forward_outcome_binding_boundary_ready": True,
            "recommended_next_research_task": review.EXPECTED_2429_NEXT_ROUTE,
        },
        "candidate_registry": _candidate_registry(),
        "prior_candidate_evidence": _prior_candidate_evidence(),
        "report_registry": {
            "reports": [{"report_id": report_id} for report_id in review.REQUIRED_REPORT_IDS]
        },
        "artifact_catalog_text": "\n".join(review.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(review.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "source_2429_route_doc": (
                f"{review.EXPECTED_2429_STATUS}\n{review.EXPECTED_2429_NEXT_ROUTE}\n"
            )
        },
    }


def _candidate_registry() -> dict[str, Any]:
    return {
        "policy_id": "equal_risk_growth_tilt_candidate_registry_v1",
        "safety_boundary": {
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "research_only": True,
            "observe_only": True,
        },
        "candidate_families": [
            {
                "strategy_id": review.TARGET_STRATEGY_ID,
                "candidate_family": "vol_target_growth_tilt",
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            }
        ],
    }


def _prior_candidate_evidence() -> dict[str, Any]:
    return {
        "status": (
            "DYNAMIC_STRATEGY_CALIBRATED_GATE_CANDIDATE_OWNER_REVIEW_AND_"
            "OBSERVATION_DECISION_READY"
        ),
        "current_best_candidate": review.TARGET_STRATEGY_ID,
        "current_best_candidate_preview_decision": "OWNER_REVIEW_REQUIRED",
        "current_best_candidate_previous_decision": "CONTINUE_OPTIMIZATION",
        "owner_decision": (
            "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_"
            "COMPONENT_ATTRIBUTION"
        ),
        "research_only_observation_approved": False,
        "observation_approved": False,
        "paper_shadow_approved": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action": "none",
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "schedule_dry_run_result_2426": root / "schedule_dry_run_result.json",
        "manual_review_packet_dry_run_result_2427": (
            root / "manual_review_packet_dry_run_result.json"
        ),
        "observe_only_boundary_result_2428": (
            root / "observe_only_signal_artifact_boundary_result.json"
        ),
        "forward_outcome_boundary_result_2429": (
            root / "forward_outcome_binding_boundary_result.json"
        ),
        "candidate_registry": root / "candidate_registry.yaml",
        "prior_candidate_evidence": root / "prior_candidate_evidence.json",
        "source_2426_research_doc": root / "source_2426_research_doc.md",
        "source_2427_research_doc": root / "source_2427_research_doc.md",
        "source_2428_research_doc": root / "source_2428_research_doc.md",
        "source_2429_research_doc": root / "source_2429_research_doc.md",
        "source_2429_route_doc": root / "dynamic_strategy_2430_route.md",
        "prior_candidate_evidence_doc": root / "prior_candidate_evidence_doc.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    for key in (
        "schedule_dry_run_result_2426",
        "manual_review_packet_dry_run_result_2427",
        "observe_only_boundary_result_2428",
        "forward_outcome_boundary_result_2429",
        "prior_candidate_evidence",
    ):
        _write_json(paths[key], sources[key])
    paths["candidate_registry"].write_text(
        "policy_id: equal_risk_growth_tilt_candidate_registry_v1\n"
        "safety_boundary:\n"
        "  paper_shadow_allowed: false\n"
        "  production_allowed: false\n"
        "  broker_action: none\n"
        "  research_only: true\n"
        "  observe_only: true\n"
        "candidate_families:\n"
        f"  - strategy_id: {review.TARGET_STRATEGY_ID}\n"
        "    candidate_family: vol_target_growth_tilt\n"
        "    paper_shadow_allowed: false\n"
        "    production_allowed: false\n"
        "    broker_action: none\n",
        encoding="utf-8",
    )
    doc_text = f"{review.EXPECTED_2429_STATUS}\n{review.EXPECTED_2429_NEXT_ROUTE}\n"
    for key in (
        "source_2426_research_doc",
        "source_2427_research_doc",
        "source_2428_research_doc",
        "source_2429_research_doc",
        "source_2429_route_doc",
        "prior_candidate_evidence_doc",
    ):
        paths[key].write_text(doc_text, encoding="utf-8")
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in review.REQUIRED_REPORT_IDS
        )
        + "\n",
        encoding="utf-8",
    )
    paths["artifact_catalog"].write_text(
        sources["artifact_catalog_text"],
        encoding="utf-8",
    )
    paths["system_flow"].write_text(sources["system_flow_text"], encoding="utf-8")
    return paths


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2426_schedule_dry_run_result_path": (
            paths["schedule_dry_run_result_2426"]
        ),
        "source_2427_manual_review_packet_dry_run_result_path": (
            paths["manual_review_packet_dry_run_result_2427"]
        ),
        "source_2428_observe_only_boundary_result_path": (
            paths["observe_only_boundary_result_2428"]
        ),
        "source_2429_forward_outcome_boundary_result_path": (
            paths["forward_outcome_boundary_result_2429"]
        ),
        "candidate_registry_path": paths["candidate_registry"],
        "prior_candidate_evidence_path": paths["prior_candidate_evidence"],
        "source_2426_research_doc_path": paths["source_2426_research_doc"],
        "source_2427_research_doc_path": paths["source_2427_research_doc"],
        "source_2428_research_doc_path": paths["source_2428_research_doc"],
        "source_2429_research_doc_path": paths["source_2429_research_doc"],
        "source_2429_route_doc_path": paths["source_2429_route_doc"],
        "prior_candidate_evidence_doc_path": paths["prior_candidate_evidence_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2426-schedule-dry-run-result",
        str(paths["schedule_dry_run_result_2426"]),
        "--source-2427-manual-review-packet-dry-run-result",
        str(paths["manual_review_packet_dry_run_result_2427"]),
        "--source-2428-observe-only-boundary-result",
        str(paths["observe_only_boundary_result_2428"]),
        "--source-2429-forward-outcome-boundary-result",
        str(paths["forward_outcome_boundary_result_2429"]),
        "--candidate-registry",
        str(paths["candidate_registry"]),
        "--prior-candidate-evidence",
        str(paths["prior_candidate_evidence"]),
        "--source-2426-research-doc",
        str(paths["source_2426_research_doc"]),
        "--source-2427-research-doc",
        str(paths["source_2427_research_doc"]),
        "--source-2428-research-doc",
        str(paths["source_2428_research_doc"]),
        "--source-2429-research-doc",
        str(paths["source_2429_research_doc"]),
        "--source-2429-route-doc",
        str(paths["source_2429_route_doc"]),
        "--prior-candidate-evidence-doc",
        str(paths["prior_candidate_evidence_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
