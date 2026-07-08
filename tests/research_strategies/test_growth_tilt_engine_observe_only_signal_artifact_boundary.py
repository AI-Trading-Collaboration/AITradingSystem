from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_observe_only_signal_artifact_boundary as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_observe_only_signal_artifact_boundary as boundary,
)


def test_observe_only_signal_artifact_boundary_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == boundary.READY_STATUS
    assert payload["manual_review_packet_dry_run_ready"] is True
    assert payload["observe_only_signal_artifact_boundary_ready"] is True
    assert payload["observe_only_signal_artifact_boundary_gap_count"] == 0
    assert payload["signal_artifact_schema_ready"] is True
    assert payload["valid_until_required"] is True
    assert payload["source_traceability_required"] is True
    assert payload["manual_review_required"] is True
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == boundary.NEXT_ROUTE_READY


def test_observe_only_signal_artifact_boundary_blocks_prior_not_ready() -> None:
    sources = _source_documents()
    sources["manual_review_packet_dry_run_result_2427"][
        "manual_review_packet_dry_run_ready"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert payload["observe_only_signal_artifact_boundary_ready"] is False
    assert "prior_manual_review_packet_dry_run_ready" in (
        payload["observe_only_signal_artifact_boundary_gap_ids"]
    )
    assert payload["recommended_next_research_task"] == boundary.NEXT_ROUTE_BLOCKED


def test_observe_only_signal_artifact_boundary_blocks_schema_gap() -> None:
    schema = _signal_schema()
    schema["required_fields"] = [
        field
        for field in boundary.REQUIRED_SIGNAL_SCHEMA_FIELDS
        if field != "valid_until"
    ]

    payload = _build_payload(_source_documents(), signal_artifact_schema=schema)

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert "signal_artifact_schema_ready" in (
        payload["observe_only_signal_artifact_boundary_gap_ids"]
    )
    assert payload["signal_artifact_schema"]["signal_artifact_schema_ready"] is False


def test_observe_only_signal_artifact_boundary_blocks_valid_until_gap() -> None:
    valid_until = _valid_until_requirements()
    valid_until["valid_until_required"] = False

    payload = _build_payload(
        _source_documents(),
        valid_until_requirements=valid_until,
    )

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert "valid_until_requirements_ready" in (
        payload["observe_only_signal_artifact_boundary_gap_ids"]
    )
    assert payload["valid_until_requirements_ready"] is False


def test_observe_only_signal_artifact_boundary_blocks_traceability_gap() -> None:
    traceability = _source_traceability_requirements()
    traceability["required_source_fields"] = ["source_feature_id"]

    payload = _build_payload(
        _source_documents(),
        source_traceability_requirements=traceability,
    )

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert "source_traceability_requirements_ready" in (
        payload["observe_only_signal_artifact_boundary_gap_ids"]
    )
    assert payload["source_traceability_requirements_ready"] is False


def test_observe_only_signal_artifact_boundary_blocks_generated_signal() -> None:
    no_advice = _no_trading_advice_boundary()
    no_advice["generated_signal"] = True

    payload = _build_payload(
        _source_documents(),
        no_trading_advice_boundary=no_advice,
    )

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert "no_trading_advice_boundary_ready" in (
        payload["observe_only_signal_artifact_boundary_gap_ids"]
    )
    assert "no_signal_or_advice_generated" in (
        payload["observe_only_signal_artifact_boundary_gap_ids"]
    )
    assert payload["no_trading_advice_boundary_ready"] is False


def test_observe_only_signal_artifact_boundary_wrapper_writes_outputs(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "observe_only_boundary"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_observe_only_signal_artifact_boundary(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == boundary.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["observe_only_signal_artifact_boundary_ready"] is True
    assert payload["observe_only_signal_artifact_boundary_gap_count"] == 0
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "signal_artifact_schema_json",
        "valid_until_requirements_json",
        "source_traceability_requirements_json",
        "pit_contract_manual_review_requirements_json",
        "no_trading_advice_boundary_json",
        "markdown_path",
        "signal_artifact_schema_markdown",
        "valid_until_requirements_markdown",
        "source_traceability_requirements_markdown",
        "pit_contract_manual_review_requirements_markdown",
        "no_trading_advice_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_observe_only_signal_artifact_boundary_cli_deterministic(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "observe_only_boundary_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-observe-only-signal-artifact-boundary",
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
    assert boundary.READY_STATUS in result.output
    assert "manual_review_packet_dry_run_ready=true" in result.output
    assert "observe_only_signal_artifact_boundary_ready=true" in result.output
    assert "signal_artifact_schema_ready=true" in result.output
    assert "valid_until_required=true" in result.output
    assert "source_traceability_required=true" in result.output
    assert "manual_review_required=true" in result.output
    assert "generated_signal=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={boundary.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "observe_only_signal_artifact_boundary_result.json").exists()


def test_observe_only_signal_artifact_boundary_missing_prior_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["manual_review_packet_dry_run_result_2427"].unlink()

    payload = impl.run_growth_tilt_engine_observe_only_signal_artifact_boundary(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["observe_only_signal_artifact_boundary_started"] is False
    assert payload["observe_only_signal_artifact_boundary_ready"] is False
    assert payload["observe_only_signal_artifact_boundary_gap_count"] == 1
    assert payload["missing_observe_only_boundary_evidence_count"] == 1
    assert payload["generated_signal"] is False
    assert payload["recommended_next_research_task"] == boundary.NEXT_ROUTE_BLOCKED


def test_observe_only_signal_artifact_boundary_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[boundary.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-observe-only-signal-artifact-boundary"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "observe_only_signal_artifact_boundary_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2429_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert boundary.REPORT_TYPE in catalog
    assert "growth-tilt-engine-observe-only-signal-artifact-boundary" in system_flow
    assert boundary.READY_STATUS in system_flow
    assert boundary.NEXT_ROUTE_READY in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(
    sources: dict[str, dict],
    **kwargs: object,
) -> dict[str, object]:
    return boundary.build_growth_tilt_engine_observe_only_signal_artifact_boundary(
        sources["manual_review_packet_dry_run_result_2427"],
        sources["manual_review_packet_2427"],
        sources["manual_review_checklist_2427"],
        sources["no_advice_boundary_summary_2427"],
        sources["reviewer_handoff_manifest_2427"],
        report_registry=_report_registry(),
        artifact_catalog_text=_artifact_catalog_text(),
        system_flow_text=_system_flow_text(),
        research_doc_texts=_research_doc_texts(),
        **kwargs,
    )


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "manual_review_packet_dry_run_result_2427": root / "result_2427.json",
        "manual_review_packet_2427": root / "packet_2427.json",
        "manual_review_checklist_2427": root / "checklist_2427.json",
        "no_advice_boundary_summary_2427": root / "no_advice_2427.json",
        "reviewer_handoff_manifest_2427": root / "handoff_2427.json",
        "research_doc_2427": root / "research_doc_2427.md",
        "packet_doc_2427": root / "packet_doc_2427.md",
        "checklist_doc_2427": root / "checklist_doc_2427.md",
        "no_advice_doc_2427": root / "no_advice_doc_2427.md",
        "handoff_doc_2427": root / "handoff_doc_2427.md",
        "route_doc_2427": root / "route_doc_2427.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    docs = _research_doc_texts()
    for key, path in paths.items():
        if key == "report_registry":
            path.write_text(_report_registry_yaml(), encoding="utf-8")
        elif key == "artifact_catalog":
            path.write_text(_artifact_catalog_text(), encoding="utf-8")
        elif key == "system_flow":
            path.write_text(_system_flow_text(), encoding="utf-8")
        elif key in docs:
            path.write_text(docs[key], encoding="utf-8")
        else:
            path.write_text(json.dumps(sources[key], indent=2), encoding="utf-8")
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2427_manual_review_packet_dry_run_result_path": paths[
            "manual_review_packet_dry_run_result_2427"
        ],
        "source_2427_manual_review_packet_path": paths[
            "manual_review_packet_2427"
        ],
        "source_2427_manual_review_checklist_path": paths[
            "manual_review_checklist_2427"
        ],
        "source_2427_no_advice_boundary_summary_path": paths[
            "no_advice_boundary_summary_2427"
        ],
        "source_2427_reviewer_handoff_manifest_path": paths[
            "reviewer_handoff_manifest_2427"
        ],
        "source_2427_research_doc_path": paths["research_doc_2427"],
        "source_2427_packet_doc_path": paths["packet_doc_2427"],
        "source_2427_checklist_doc_path": paths["checklist_doc_2427"],
        "source_2427_no_advice_doc_path": paths["no_advice_doc_2427"],
        "source_2427_handoff_doc_path": paths["handoff_doc_2427"],
        "source_2427_route_doc_path": paths["route_doc_2427"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    for option, key in (
        (
            "--source-2427-manual-review-packet-dry-run-result",
            "manual_review_packet_dry_run_result_2427",
        ),
        ("--source-2427-manual-review-packet", "manual_review_packet_2427"),
        ("--source-2427-manual-review-checklist", "manual_review_checklist_2427"),
        (
            "--source-2427-no-advice-boundary-summary",
            "no_advice_boundary_summary_2427",
        ),
        (
            "--source-2427-reviewer-handoff-manifest",
            "reviewer_handoff_manifest_2427",
        ),
        ("--source-2427-research-doc", "research_doc_2427"),
        ("--source-2427-packet-doc", "packet_doc_2427"),
        ("--source-2427-checklist-doc", "checklist_doc_2427"),
        ("--source-2427-no-advice-doc", "no_advice_doc_2427"),
        ("--source-2427-handoff-doc", "handoff_doc_2427"),
        ("--source-2427-route-doc", "route_doc_2427"),
        ("--report-registry", "report_registry"),
        ("--artifact-catalog", "artifact_catalog"),
        ("--system-flow", "system_flow"),
    ):
        args.extend([option, str(paths[key])])
    return args


def _source_documents() -> dict[str, dict]:
    return {
        "manual_review_packet_dry_run_result_2427": _result_2427(),
        "manual_review_packet_2427": {"manual_review_packet": _manual_packet_2427()},
        "manual_review_checklist_2427": {
            "manual_review_checklist": _manual_checklist_2427()
        },
        "no_advice_boundary_summary_2427": {
            "no_advice_boundary_summary": _no_advice_2427()
        },
        "reviewer_handoff_manifest_2427": {
            "reviewer_handoff_manifest": _reviewer_handoff_2427()
        },
    }


def _result_2427() -> dict[str, object]:
    return {
        "status": boundary.MANUAL_REVIEW_PACKET_DRY_RUN_READY_STATUS,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "contract_ready": True,
        "contract_ready_count": 1,
        "contract_gap_count": 0,
        "manual_review_packet_dry_run_ready": True,
        "manual_review_packet_gap_count": 0,
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "schedule_hook_invoked": False,
        "schedule_state_mutated": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "broker_action": "none",
        "order_generated": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "actual_portfolio_weights_modified": False,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "new_feature_generated": False,
        "backtest_run": False,
        "new_strategy_backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "automatic_execution_allowed": False,
        "recommended_next_research_task": boundary.EXPECTED_PRIOR_NEXT_ROUTE,
    }


def _manual_packet_2427() -> dict[str, object]:
    return {
        "status": boundary.MANUAL_REVIEW_PACKET_DRY_RUN_READY_STATUS,
        "manual_review_packet_ready": True,
        "packet_mode": "dry_run_no_advice",
        "manual_review_required": True,
        "manual_review_only": True,
        "contains_trading_advice": False,
        "contains_actionable_allocation": False,
        "contains_broker_order": False,
        "contains_portfolio_mutation": False,
        "next_route": boundary.EXPECTED_PRIOR_NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _manual_checklist_2427() -> dict[str, object]:
    return {
        "status": boundary.MANUAL_REVIEW_PACKET_DRY_RUN_READY_STATUS,
        "manual_review_checklist_ready": True,
        "failed_check_count": 0,
        "checks": [{"check_id": "packet_is_dry_run_no_advice", "passed": True}],
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_advice_2427() -> dict[str, object]:
    return _no_trading_advice_boundary() | {"no_advice_boundary_ready": True}


def _reviewer_handoff_2427() -> dict[str, object]:
    return {
        "status": boundary.MANUAL_REVIEW_PACKET_DRY_RUN_READY_STATUS,
        "reviewer_handoff_manifest_ready": True,
        "manual_review_required": True,
        "manual_review_only": True,
        "handoff_mode": "dry_run_no_advice",
        "automatic_execution_allowed": False,
        "next_route": boundary.EXPECTED_PRIOR_NEXT_ROUTE,
        "production_effect": "none",
        "broker_action": "none",
    }


def _signal_schema() -> dict[str, object]:
    return {
        "signal_artifact_schema_ready": True,
        "artifact_mode": "observe_only_boundary",
        "observe_only": True,
        "generated_signal": False,
        "signal_artifact_instance_generated": False,
        "required_fields": list(boundary.REQUIRED_SIGNAL_SCHEMA_FIELDS),
        "next_route": boundary.NEXT_ROUTE_READY,
        "production_effect": "none",
        "broker_action": "none",
    }


def _valid_until_requirements() -> dict[str, object]:
    return {
        "valid_until_requirements_ready": True,
        "valid_until_required": True,
        "known_at_required": True,
        "decision_at_required": True,
        "stale_signal_policy_required": True,
        "valid_until_must_be_after_as_of": True,
        "valid_until_must_not_be_inferred_from_future_outcome": True,
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_traceability_requirements() -> dict[str, object]:
    return {
        "source_traceability_requirements_ready": True,
        "source_traceability_required": True,
        "missing_source_evidence_policy": "fail_closed",
        "upstream_artifact_closure_required": True,
        "required_source_fields": [
            "source_feature_id",
            "source_report_id",
            "source_artifact_path",
            "source_artifact_checksum",
            "source_as_of",
            "source_known_at",
            "source_valid_until",
        ],
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_trading_advice_boundary() -> dict[str, object]:
    return {
        "no_trading_advice_boundary_ready": True,
        "observe_only": True,
        "generated_signal": False,
        "new_signal_generated": False,
        "signal_artifact_instance_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "allocation_change_generated": False,
        "recommendation_generated": False,
        "broker_order_generated": False,
        "order_generated": False,
        "portfolio_weight_mutated": False,
        "actual_portfolio_weights_modified": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "fresh_market_data_read": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _research_doc_texts() -> dict[str, str]:
    return {
        "research_doc_2427": boundary.MANUAL_REVIEW_PACKET_DRY_RUN_READY_STATUS,
        "packet_doc_2427": "manual review packet ready",
        "checklist_doc_2427": "manual review checklist ready",
        "no_advice_doc_2427": "no advice boundary ready",
        "handoff_doc_2427": "reviewer handoff ready",
        "route_doc_2427": boundary.EXPECTED_PRIOR_NEXT_ROUTE,
    }


def _report_registry() -> dict[str, list[dict[str, object]]]:
    return {
        "reports": [
            {
                "report_id": report_id,
                "command": _command_for_report(report_id),
                "artifact_globs": [],
                "artifact_selection_policy": "latest_available",
                "required_for_daily_reading": False,
                "production_effect": "none",
                "broker_action": "none",
            }
            for report_id in boundary.REQUIRED_REPORT_IDS
        ]
    }


def _report_registry_yaml() -> str:
    rows = "\n".join(
        f"  - report_id: {report_id}\n"
        f"    command: {_command_for_report(report_id)}\n"
        "    artifact_globs: []\n"
        "    artifact_selection_policy: latest_available\n"
        "    required_for_daily_reading: false\n"
        "    production_effect: none\n"
        "    broker_action: none"
        for report_id in boundary.REQUIRED_REPORT_IDS
    )
    return f"reports:\n{rows}\n"


def _artifact_catalog_text() -> str:
    return "\n".join(boundary.REQUIRED_CATALOG_REFERENCES)


def _system_flow_text() -> str:
    return "\n".join(boundary.REQUIRED_SYSTEM_FLOW_REFERENCES)


def _command_for_report(report_id: str) -> str:
    return {
        boundary.REPORT_TYPE: (
            "aits research strategies "
            "growth-tilt-engine-observe-only-signal-artifact-boundary"
        ),
        "growth_tilt_engine_manual_review_packet_dry_run": (
            "aits research strategies growth-tilt-engine-manual-review-packet-dry-run"
        ),
    }[report_id]
