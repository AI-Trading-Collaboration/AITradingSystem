from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_dry_run_wiring as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_paper_shadow_dry_run_wiring as wiring,
)


def test_paper_shadow_dry_run_wiring_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == wiring.READY_STATUS
    assert payload["pit_gate_ready"] is True
    assert payload["contract_ready"] is True
    assert payload["paper_shadow_preflight_ready"] is True
    assert payload["enablement_plan_ready"] is True
    assert payload["dry_run_wiring_ready"] is True
    assert payload["dry_run_wiring_gap_count"] == 0
    assert payload["input_contract_map_ready"] is True
    assert payload["output_artifact_contract_map_ready"] is True
    assert payload["manual_review_handoff_wired"] is True
    assert payload["schedule_hook_verified_disabled"] is True
    assert payload["no_effect_audit_ready"] is True
    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_schedule_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["recommended_next_research_task"] == wiring.NEXT_ROUTE_READY


def test_paper_shadow_dry_run_wiring_blocks_enablement_not_ready() -> None:
    sources = _source_documents()
    sources["enablement_plan_result_2424"]["enablement_plan_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert payload["dry_run_wiring_ready"] is False
    assert "enablement_plan_ready" in payload["dry_run_wiring_gap_ids"]
    assert payload["recommended_next_research_task"] == wiring.NEXT_ROUTE_BLOCKED


def test_paper_shadow_dry_run_wiring_blocks_preflight_not_ready() -> None:
    sources = _source_documents()
    sources["preflight_result_2423"]["paper_shadow_preflight_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "paper_shadow_preflight_ready" in payload["dry_run_wiring_gap_ids"]


def test_paper_shadow_dry_run_wiring_blocks_contract_not_ready() -> None:
    sources = _source_documents()
    sources["contract_readiness_snapshot_2422"]["contract_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "contract_ready" in payload["dry_run_wiring_gap_ids"]


def test_paper_shadow_dry_run_wiring_blocks_pit_gate_not_ready() -> None:
    sources = _source_documents()
    sources["readiness_recheck_2421"]["pit_gate_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "pit_gate_ready" in payload["dry_run_wiring_gap_ids"]


def test_paper_shadow_dry_run_wiring_blocks_missing_input_contract_map() -> None:
    sources = _source_documents()

    payload = _build_payload(sources, input_contract_map=[])

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "input_contract_map_resolves" in payload["dry_run_wiring_gap_ids"]
    assert payload["input_contract_map_ready"] is False


def test_paper_shadow_dry_run_wiring_blocks_missing_output_contract_map() -> None:
    sources = _source_documents()

    payload = _build_payload(sources, output_artifact_contract_map=[])

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "output_artifact_contract_map_resolves" in payload["dry_run_wiring_gap_ids"]
    assert payload["output_artifact_contract_map_ready"] is False


def test_paper_shadow_dry_run_wiring_blocks_missing_manual_handoff() -> None:
    payload = _build_payload(
        _source_documents(),
        manual_review_handoff_route={"manual_review_handoff_wired": False},
    )

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "manual_review_handoff_route_resolves" in payload["dry_run_wiring_gap_ids"]


def test_paper_shadow_dry_run_wiring_blocks_schedule_hook_not_disabled() -> None:
    payload = _build_payload(
        _source_documents(),
        schedule_hook_disabled_verification={
            "schedule_hook_verified_disabled": False,
            "paper_shadow_schedule_enabled": False,
            "scheduler_enabled": False,
            "scheduled_task_created": False,
            "paper_shadow_daily_job_run": False,
            "cron_or_windows_task_created": False,
        },
    )

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "schedule_hook_verified_disabled" in payload["dry_run_wiring_gap_ids"]


def test_paper_shadow_dry_run_wiring_blocks_runtime_or_schedule_enabled() -> None:
    sources = _source_documents()
    sources["enablement_plan_result_2424"]["paper_shadow_enabled"] = True
    sources["enablement_plan_result_2424"]["paper_shadow_schedule_enabled"] = True

    payload = _build_payload(sources)

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "paper_shadow_runtime_disabled" in payload["dry_run_wiring_gap_ids"]
    assert "paper_shadow_schedule_disabled" in payload["dry_run_wiring_gap_ids"]


def test_paper_shadow_dry_run_wiring_blocks_production_or_broker_enabled() -> None:
    sources = _source_documents()
    sources["enablement_plan_result_2424"]["production_enabled"] = True
    sources["enablement_plan_result_2424"]["broker_enabled"] = True

    payload = _build_payload(sources)

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "production_disabled" in payload["dry_run_wiring_gap_ids"]
    assert "broker_disabled" in payload["dry_run_wiring_gap_ids"]


def test_paper_shadow_dry_run_wiring_ready_has_no_trading_outputs() -> None:
    payload = _build_payload(_source_documents())

    assert payload["dry_run_wiring_ready"] is True
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["backtest_run"] is False
    assert payload["scoring_run"] is False
    assert payload["daily_report_run"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["portfolio_weight_mutated"] is False
    assert payload["broker_order_generated"] is False


def test_paper_shadow_dry_run_wiring_blocks_missing_registration() -> None:
    sources = _source_documents()

    payload = wiring.build_growth_tilt_engine_paper_shadow_dry_run_wiring(
        sources["enablement_plan_result_2424"],
        sources["preflight_result_2423"],
        sources["contract_readiness_snapshot_2422"],
        sources["readiness_recheck_2421"],
        sources["remediation_result_2420"],
        sources["source_traceability_manifest_2420"],
        sources["source_lineage_map_2420"],
        sources["missing_source_evidence_summary_2420"],
        report_registry={"reports": []},
        artifact_catalog_text="",
        system_flow_text="",
        research_doc_texts={"source_2424_research_doc": ""},
    )

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert "report_registry_registered" in payload["dry_run_wiring_gap_ids"]
    assert "artifact_catalog_registered" in payload["dry_run_wiring_gap_ids"]
    assert "system_flow_registered" in payload["dry_run_wiring_gap_ids"]
    assert "research_docs_registered" in payload["dry_run_wiring_gap_ids"]


def test_paper_shadow_dry_run_wiring_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "dry_run_wiring"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_paper_shadow_dry_run_wiring(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == wiring.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["dry_run_wiring_ready"] is True
    assert payload["dry_run_wiring_gap_count"] == 0
    assert payload["paper_shadow_enabled"] is False
    assert payload["paper_shadow_schedule_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "input_output_contract_map_json",
        "runtime_boundary_manifest_json",
        "schedule_hook_disabled_verification_json",
        "manual_review_handoff_wiring_plan_json",
        "dry_run_no_effect_audit_summary_json",
        "markdown_path",
        "input_output_contract_map_markdown",
        "runtime_boundary_manifest_markdown",
        "schedule_hook_disabled_verification_markdown",
        "manual_review_handoff_wiring_plan_markdown",
        "dry_run_no_effect_audit_summary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_paper_shadow_dry_run_wiring_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "dry_run_wiring_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-paper-shadow-dry-run-wiring",
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
    assert wiring.READY_STATUS in result.output
    assert "pit_gate_ready=true" in result.output
    assert "contract_ready=true" in result.output
    assert "paper_shadow_preflight_ready=true" in result.output
    assert "enablement_plan_ready=true" in result.output
    assert "dry_run_wiring_ready=true" in result.output
    assert "dry_run_wiring_gap_count=0" in result.output
    assert "input_contract_map_ready=true" in result.output
    assert "output_artifact_contract_map_ready=true" in result.output
    assert "manual_review_handoff_wired=true" in result.output
    assert "schedule_hook_verified_disabled=true" in result.output
    assert "no_effect_audit_ready=true" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "paper_shadow_schedule_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "generated_signal=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert f"next_route={wiring.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "dry_run_wiring_result.json").exists()


def test_paper_shadow_dry_run_wiring_missing_prior_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["enablement_plan_result_2424"].unlink()

    payload = impl.run_growth_tilt_engine_paper_shadow_dry_run_wiring(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == wiring.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["paper_shadow_dry_run_wiring_started"] is False
    assert payload["dry_run_wiring_ready"] is False
    assert payload["dry_run_wiring_gap_count"] == 1
    assert payload["missing_dry_run_evidence_count"] == 1
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == wiring.NEXT_ROUTE_BLOCKED


def test_paper_shadow_dry_run_wiring_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[wiring.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-paper-shadow-dry-run-wiring"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("dry_run_wiring_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2426_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert wiring.REPORT_TYPE in catalog
    assert "growth-tilt-engine-paper-shadow-dry-run-wiring" in system_flow
    assert wiring.READY_STATUS in system_flow
    assert wiring.NEXT_ROUTE_READY in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(
    sources: dict[str, dict],
    **kwargs: object,
) -> dict[str, object]:
    return wiring.build_growth_tilt_engine_paper_shadow_dry_run_wiring(
        sources["enablement_plan_result_2424"],
        sources["preflight_result_2423"],
        sources["contract_readiness_snapshot_2422"],
        sources["readiness_recheck_2421"],
        sources["remediation_result_2420"],
        sources["source_traceability_manifest_2420"],
        sources["source_lineage_map_2420"],
        sources["missing_source_evidence_summary_2420"],
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
        "enablement_plan_result_2424": root / "enablement_result_2424.json",
        "enablement_plan_2424": root / "enablement_plan_2424.json",
        "runtime_boundary_checklist_2424": root / "runtime_boundary_2424.json",
        "schedule_boundary_plan_2424": root / "schedule_boundary_2424.json",
        "manual_review_checklist_2424": root / "manual_review_2424.json",
        "rollback_stop_condition_summary_2424": root / "rollback_2424.json",
        "research_doc_2424": root / "research_doc_2424.md",
        "runtime_boundary_doc_2424": root / "runtime_boundary_doc_2424.md",
        "schedule_boundary_doc_2424": root / "schedule_boundary_doc_2424.md",
        "manual_review_doc_2424": root / "manual_review_doc_2424.md",
        "rollback_doc_2424": root / "rollback_doc_2424.md",
        "route_doc_2424": root / "route_doc_2424.md",
        "preflight_result_2423": root / "preflight_result_2423.json",
        "research_doc_2423": root / "research_doc_2423.md",
        "route_doc_2423": root / "route_doc_2423.md",
        "contract_readiness_snapshot_2422": root / "contract_snapshot_2422.json",
        "research_doc_2422": root / "research_doc_2422.md",
        "route_doc_2422": root / "route_doc_2422.md",
        "readiness_recheck_2421": root / "readiness_recheck_2421.json",
        "research_doc_2421": root / "research_doc_2421.md",
        "route_doc_2421": root / "route_doc_2421.md",
        "remediation_result_2420": root / "remediation_result_2420.json",
        "source_traceability_manifest_2420": root / "manifest_2420.json",
        "source_lineage_map_2420": root / "lineage_2420.json",
        "missing_source_evidence_summary_2420": root / "missing_2420.json",
        "research_doc_2420": root / "research_doc_2420.md",
        "manifest_doc_2420": root / "manifest_doc_2420.md",
        "lineage_doc_2420": root / "lineage_doc_2420.md",
        "route_doc_2420": root / "route_doc_2420.md",
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
        "source_2424_enablement_plan_result_path": (
            paths["enablement_plan_result_2424"]
        ),
        "source_2424_enablement_plan_path": paths["enablement_plan_2424"],
        "source_2424_runtime_boundary_checklist_path": (
            paths["runtime_boundary_checklist_2424"]
        ),
        "source_2424_schedule_boundary_plan_path": (
            paths["schedule_boundary_plan_2424"]
        ),
        "source_2424_manual_review_checklist_path": (
            paths["manual_review_checklist_2424"]
        ),
        "source_2424_rollback_stop_condition_summary_path": (
            paths["rollback_stop_condition_summary_2424"]
        ),
        "source_2424_research_doc_path": paths["research_doc_2424"],
        "source_2424_runtime_boundary_doc_path": paths["runtime_boundary_doc_2424"],
        "source_2424_schedule_boundary_doc_path": paths["schedule_boundary_doc_2424"],
        "source_2424_manual_review_doc_path": paths["manual_review_doc_2424"],
        "source_2424_rollback_doc_path": paths["rollback_doc_2424"],
        "source_2424_route_doc_path": paths["route_doc_2424"],
        "source_2423_preflight_result_path": paths["preflight_result_2423"],
        "source_2423_research_doc_path": paths["research_doc_2423"],
        "source_2423_route_doc_path": paths["route_doc_2423"],
        "source_2422_contract_readiness_snapshot_path": (
            paths["contract_readiness_snapshot_2422"]
        ),
        "source_2422_research_doc_path": paths["research_doc_2422"],
        "source_2422_route_doc_path": paths["route_doc_2422"],
        "source_2421_readiness_recheck_result_path": paths["readiness_recheck_2421"],
        "source_2421_research_doc_path": paths["research_doc_2421"],
        "source_2421_route_doc_path": paths["route_doc_2421"],
        "source_2420_remediation_result_path": paths["remediation_result_2420"],
        "source_2420_source_traceability_manifest_path": (
            paths["source_traceability_manifest_2420"]
        ),
        "source_2420_source_lineage_map_path": paths["source_lineage_map_2420"],
        "source_2420_missing_source_evidence_summary_path": (
            paths["missing_source_evidence_summary_2420"]
        ),
        "source_2420_research_doc_path": paths["research_doc_2420"],
        "source_2420_manifest_doc_path": paths["manifest_doc_2420"],
        "source_2420_lineage_doc_path": paths["lineage_doc_2420"],
        "source_2420_route_doc_path": paths["route_doc_2420"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    for option, key in (
        ("--source-2424-enablement-plan-result", "enablement_plan_result_2424"),
        ("--source-2424-enablement-plan", "enablement_plan_2424"),
        (
            "--source-2424-runtime-boundary-checklist",
            "runtime_boundary_checklist_2424",
        ),
        ("--source-2424-schedule-boundary-plan", "schedule_boundary_plan_2424"),
        ("--source-2424-manual-review-checklist", "manual_review_checklist_2424"),
        (
            "--source-2424-rollback-stop-condition-summary",
            "rollback_stop_condition_summary_2424",
        ),
        ("--source-2424-research-doc", "research_doc_2424"),
        ("--source-2424-runtime-boundary-doc", "runtime_boundary_doc_2424"),
        ("--source-2424-schedule-boundary-doc", "schedule_boundary_doc_2424"),
        ("--source-2424-manual-review-doc", "manual_review_doc_2424"),
        ("--source-2424-rollback-doc", "rollback_doc_2424"),
        ("--source-2424-route-doc", "route_doc_2424"),
        ("--source-2423-preflight-result", "preflight_result_2423"),
        ("--source-2423-research-doc", "research_doc_2423"),
        ("--source-2423-route-doc", "route_doc_2423"),
        (
            "--source-2422-contract-readiness-snapshot",
            "contract_readiness_snapshot_2422",
        ),
        ("--source-2422-research-doc", "research_doc_2422"),
        ("--source-2422-route-doc", "route_doc_2422"),
        ("--source-2421-readiness-recheck-result", "readiness_recheck_2421"),
        ("--source-2421-research-doc", "research_doc_2421"),
        ("--source-2421-route-doc", "route_doc_2421"),
        ("--source-2420-remediation-result", "remediation_result_2420"),
        (
            "--source-2420-source-traceability-manifest",
            "source_traceability_manifest_2420",
        ),
        ("--source-2420-source-lineage-map", "source_lineage_map_2420"),
        (
            "--source-2420-missing-source-evidence-summary",
            "missing_source_evidence_summary_2420",
        ),
        ("--source-2420-research-doc", "research_doc_2420"),
        ("--source-2420-manifest-doc", "manifest_doc_2420"),
        ("--source-2420-lineage-doc", "lineage_doc_2420"),
        ("--source-2420-route-doc", "route_doc_2420"),
        ("--report-registry", "report_registry"),
        ("--artifact-catalog", "artifact_catalog"),
        ("--system-flow", "system_flow"),
    ):
        args.extend([option, str(paths[key])])
    return args


def _source_documents() -> dict[str, dict]:
    return {
        "enablement_plan_result_2424": _enablement_plan_result_2424(),
        "enablement_plan_2424": {"paper_shadow_enablement_plan": {}},
        "runtime_boundary_checklist_2424": {"runtime_boundary_checklist": {}},
        "schedule_boundary_plan_2424": {"schedule_boundary_plan": {}},
        "manual_review_checklist_2424": {"manual_review_checklist": {}},
        "rollback_stop_condition_summary_2424": {
            "rollback_stop_condition_summary": {}
        },
        "preflight_result_2423": _preflight_result_2423(),
        "contract_readiness_snapshot_2422": _contract_readiness_snapshot_2422(),
        "readiness_recheck_2421": _readiness_recheck_2421(),
        "remediation_result_2420": _remediation_result_2420(),
        "source_traceability_manifest_2420": {
            "source_traceability_manifest": _manifest()
        },
        "source_lineage_map_2420": {"source_lineage_map": _lineage_map()},
        "missing_source_evidence_summary_2420": {
            "missing_source_evidence_summary": {
                "artifact_id": wiring.ARTIFACT_ID,
                "missing_field_count": 0,
                "incomplete_field_count": 0,
                "unresolved_blocker_count": 0,
                "prior_missing_evidence_closed_by_2420": True,
            }
        },
    }


def _enablement_plan_result_2424() -> dict[str, object]:
    return {
        "status": wiring.ENABLEMENT_PLAN_READY_STATUS,
        "readiness_status": wiring.ENABLEMENT_PLAN_READY_STATUS,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "remaining_pit_blockers": [],
        "remaining_pit_blocker_count": 0,
        "contract_ready": True,
        "contract_ready_count": 1,
        "contract_gap_count": 0,
        "source_traceability_recheck_status": "ACCEPTED",
        "source_traceability_accepted": True,
        "paper_shadow_preflight_ready": True,
        "enablement_plan_ready": True,
        "enablement_gap_count": 0,
        "manual_review_required": True,
        "manual_review_only": True,
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_schedule_enabled": False,
        "paper_shadow_daily_job_enabled": False,
        "paper_shadow_daily_job_run": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "broker_action": "none",
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "new_signal_generated": False,
        "new_feature_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "automatic_execution_allowed": False,
        "recommended_next_research_task": wiring.EXPECTED_PRIOR_NEXT_ROUTE,
    }


def _preflight_result_2423() -> dict[str, object]:
    return {
        "status": wiring.PREFLIGHT_READY_STATUS,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "remaining_pit_blockers": [],
        "remaining_pit_blocker_count": 0,
        "contract_ready": True,
        "contract_ready_count": 1,
        "source_traceability_recheck_status": "ACCEPTED",
        "source_traceability_accepted": True,
        "paper_shadow_preflight_ready": True,
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_schedule_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "broker_action": "none",
        "generated_signal": False,
        "generated_trading_advice": False,
        "daily_report_generated": False,
        "daily_report_run": False,
        "new_signal_generated": False,
        "new_feature_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "fresh_market_data_read": False,
        "manual_review_required": True,
    }


def _contract_readiness_snapshot_2422() -> dict[str, object]:
    return {
        "status": wiring.CONTRACT_READINESS_SNAPSHOT_READY_STATUS,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "remaining_blockers": [],
        "remaining_blocker_count": 0,
        "contract_ready": True,
        "contract_ready_count": 1,
        "contract_gap_count": 0,
        "source_traceability_recheck_status": "ACCEPTED",
        "paper_shadow_enabled": False,
        "paper_shadow_allowed": False,
        "paper_shadow_schedule_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "broker_action": "none",
        "generated_signal": False,
        "generated_trading_advice": False,
        "daily_report_generated": False,
        "new_signal_generated": False,
        "new_feature_generated": False,
        "backtest_run": False,
        "scoring_run": False,
        "manual_review_required": True,
    }


def _readiness_recheck_2421() -> dict[str, object]:
    return {
        "status": wiring.PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS,
        "source_traceability_recheck_status": "ACCEPTED",
        "remaining_blockers": [],
        "remaining_blocker_count": 0,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "paper_trade_created": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "broker_action": "none",
        "daily_report_generated": False,
        "new_signal_generated": False,
        "manual_review_required": True,
    }


def _remediation_result_2420() -> dict[str, object]:
    return {
        "status": wiring.SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS,
        "remediation_status": "READY",
        "artifact_id": wiring.ARTIFACT_ID,
        "source_traceability_evidence_complete": True,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "broker_action": "none",
        "daily_report_generated": False,
        "new_signal_generated": False,
        "manual_review_required": True,
    }


def _manifest() -> dict[str, object]:
    return {
        "artifact_id": wiring.ARTIFACT_ID,
        "traceability_status": "READY",
        "source_artifacts": [{"artifact_id": "source_artifact"}],
        "source_documents": [{"document_id": "source_document"}],
    }


def _lineage_map() -> dict[str, object]:
    return {
        "artifact_id": wiring.ARTIFACT_ID,
        "upstream_dependencies": [{"artifact_id": "source_artifact"}],
        "source_documents": [{"document_id": "source_document"}],
    }


def _research_doc_texts() -> dict[str, str]:
    return {
        "research_doc_2424": wiring.ENABLEMENT_PLAN_READY_STATUS,
        "runtime_boundary_doc_2424": "runtime boundary checklist ready",
        "schedule_boundary_doc_2424": "schedule hook disabled",
        "manual_review_doc_2424": "manual review checklist ready",
        "rollback_doc_2424": "rollback summary ready",
        "route_doc_2424": wiring.EXPECTED_PRIOR_NEXT_ROUTE,
        "research_doc_2423": wiring.PREFLIGHT_READY_STATUS,
        "route_doc_2423": "TRADING-2424_Growth_Tilt_Engine_Paper_Shadow_Enablement_Plan",
        "research_doc_2422": wiring.CONTRACT_READINESS_SNAPSHOT_READY_STATUS,
        "route_doc_2422": "TRADING-2423_Growth_Tilt_Engine_Paper_Shadow_Preflight",
        "research_doc_2421": wiring.PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS,
        "route_doc_2421": "TRADING-2422_Growth_Tilt_Engine_Contract_Readiness_Snapshot",
        "research_doc_2420": wiring.SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS,
        "manifest_doc_2420": "source traceability manifest READY",
        "lineage_doc_2420": "source lineage map READY",
        "route_doc_2420": (
            "TRADING-2421_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck_"
            "After_Source_Traceability_Remediation"
        ),
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
            for report_id in wiring.REQUIRED_REPORT_IDS
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
        for report_id in wiring.REQUIRED_REPORT_IDS
    )
    return f"reports:\n{rows}\n"


def _artifact_catalog_text() -> str:
    return "\n".join(wiring.REQUIRED_CATALOG_REFERENCES)


def _system_flow_text() -> str:
    return "\n".join(wiring.REQUIRED_SYSTEM_FLOW_REFERENCES)


def _command_for_report(report_id: str) -> str:
    return {
        wiring.REPORT_TYPE: (
            "aits research strategies growth-tilt-engine-paper-shadow-dry-run-wiring"
        ),
        "growth_tilt_engine_paper_shadow_enablement_plan": (
            "aits research strategies growth-tilt-engine-paper-shadow-enablement-plan"
        ),
        "growth_tilt_engine_paper_shadow_preflight": (
            "aits research strategies growth-tilt-engine-paper-shadow-preflight"
        ),
        "growth_tilt_engine_contract_readiness_snapshot": (
            "aits research strategies growth-tilt-engine-contract-readiness-snapshot"
        ),
        "growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
        "remediation": (
            "aits research strategies "
            "growth-tilt-engine-pit-gate-readiness-recheck-after-source-"
            "traceability-remediation"
        ),
        "growth_tilt_engine_signal_artifact_source_traceability_remediation": (
            "aits research strategies "
            "growth-tilt-engine-signal-artifact-source-traceability-remediation"
        ),
    }[report_id]
