from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_paper_shadow_preflight as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_paper_shadow_preflight as preflight,
)


def test_paper_shadow_preflight_builder_ready() -> None:
    sources = _source_documents()

    payload = preflight.build_growth_tilt_engine_paper_shadow_preflight(
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
    )

    assert payload["status"] == preflight.READY_STATUS
    assert payload["pit_gate_ready"] is True
    assert payload["contract_ready"] is True
    assert payload["contract_gap_count"] == 0
    assert payload["remaining_pit_blockers"] == []
    assert payload["source_traceability_accepted"] is True
    assert payload["paper_shadow_preflight_started"] is True
    assert payload["paper_shadow_preflight_ready"] is True
    assert payload["preflight_gap_count"] == 0
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False
    assert payload["recommended_next_research_task"] == preflight.NEXT_ROUTE_READY


def test_paper_shadow_preflight_blocks_pit_gate_not_ready() -> None:
    sources = _source_documents()
    sources["contract_readiness_snapshot_2422"]["pit_gate_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == preflight.BLOCKED_STATUS
    assert payload["paper_shadow_preflight_ready"] is False
    assert "pit_gate_ready" in payload["preflight_gap_ids"]
    assert payload["recommended_next_research_task"] == preflight.NEXT_ROUTE_BLOCKED


def test_paper_shadow_preflight_blocks_contract_not_ready() -> None:
    sources = _source_documents()
    sources["contract_readiness_snapshot_2422"]["contract_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == preflight.BLOCKED_STATUS
    assert payload["paper_shadow_preflight_ready"] is False
    assert "contract_readiness_snapshot_ready" in payload["preflight_gap_ids"]
    assert payload["paper_shadow_enabled"] is False


def test_paper_shadow_preflight_blocks_existing_paper_shadow_enabled() -> None:
    sources = _source_documents()
    sources["contract_readiness_snapshot_2422"]["paper_shadow_enabled"] = True

    payload = _build_payload(sources)

    assert payload["status"] == preflight.BLOCKED_STATUS
    assert payload["paper_shadow_preflight_ready"] is False
    assert "paper_shadow_not_enabled" in payload["preflight_gap_ids"]
    assert payload["safety_boundary_gap_count"] > 0


def test_paper_shadow_preflight_blocks_production_or_broker_enabled() -> None:
    sources = _source_documents()
    sources["contract_readiness_snapshot_2422"]["production_enabled"] = True
    sources["contract_readiness_snapshot_2422"]["broker_enabled"] = True

    payload = _build_payload(sources)

    assert payload["status"] == preflight.BLOCKED_STATUS
    assert "production_disabled" in payload["preflight_gap_ids"]
    assert "broker_disabled" in payload["preflight_gap_ids"]
    assert payload["recommended_next_research_task"] == preflight.NEXT_ROUTE_BLOCKED


def test_paper_shadow_preflight_blocks_missing_registration() -> None:
    sources = _source_documents()

    payload = preflight.build_growth_tilt_engine_paper_shadow_preflight(
        sources["contract_readiness_snapshot_2422"],
        sources["readiness_recheck_2421"],
        sources["remediation_result_2420"],
        sources["source_traceability_manifest_2420"],
        sources["source_lineage_map_2420"],
        sources["missing_source_evidence_summary_2420"],
        report_registry={"reports": []},
        artifact_catalog_text="",
        system_flow_text="",
        research_doc_texts={"source_2422_research_doc": ""},
    )

    assert payload["status"] == preflight.BLOCKED_STATUS
    assert payload["missing_preflight_evidence_count"] >= 4
    assert "report_registry_registered" in payload["preflight_gap_ids"]
    assert "artifact_catalog_registered" in payload["preflight_gap_ids"]
    assert "system_flow_registered" in payload["preflight_gap_ids"]
    assert "research_docs_registered" in payload["preflight_gap_ids"]


def test_paper_shadow_preflight_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "preflight"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_paper_shadow_preflight(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == preflight.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["paper_shadow_preflight_started"] is True
    assert payload["paper_shadow_preflight_ready"] is True
    assert payload["preflight_gap_count"] == 0
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["generated_signal"] is False
    assert payload["generated_trading_advice"] is False

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "preflight_checklist_json",
        "preflight_gap_summary_json",
        "markdown_path",
        "preflight_checklist_markdown",
        "preflight_gap_summary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_paper_shadow_preflight_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "preflight_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-paper-shadow-preflight",
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
    assert preflight.READY_STATUS in result.output
    assert "pit_gate_ready=true" in result.output
    assert "contract_ready=true" in result.output
    assert "contract_gap_count=0" in result.output
    assert "source_traceability_accepted=true" in result.output
    assert "paper_shadow_preflight_started=true" in result.output
    assert "paper_shadow_preflight_ready=true" in result.output
    assert "preflight_gap_count=0" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "paper_shadow_schedule_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "generated_signal=false" in result.output
    assert "generated_trading_advice=false" in result.output
    assert "fresh_market_data_read=false" in result.output
    assert f"next_route={preflight.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "paper_shadow_preflight_result.json").exists()


def test_paper_shadow_preflight_missing_prior_artifact_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["contract_readiness_snapshot_2422"].unlink()

    payload = impl.run_growth_tilt_engine_paper_shadow_preflight(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == preflight.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["paper_shadow_preflight_started"] is False
    assert payload["paper_shadow_preflight_ready"] is False
    assert payload["preflight_gap_count"] == 1
    assert payload["missing_preflight_evidence_count"] == 1
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == preflight.NEXT_ROUTE_BLOCKED


def test_paper_shadow_preflight_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[preflight.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-paper-shadow-preflight"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "paper_shadow_preflight_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2424_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert preflight.REPORT_TYPE in catalog
    assert "growth-tilt-engine-paper-shadow-preflight" in system_flow
    assert preflight.READY_STATUS in system_flow
    assert preflight.NEXT_ROUTE_READY in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, dict]) -> dict[str, object]:
    return preflight.build_growth_tilt_engine_paper_shadow_preflight(
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
    )


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "contract_readiness_snapshot_2422": root / "contract_snapshot_2422.json",
        "contract_evidence_map_2422": root / "contract_evidence_map_2422.json",
        "contract_gap_summary_2422": root / "contract_gap_summary_2422.json",
        "contract_requirements_2422": root / "contract_requirements_2422.json",
        "readiness_recheck_2421": root / "readiness_recheck_2421.json",
        "pit_gate_recheck_matrix_2421": root / "matrix_2421.json",
        "blocker_resolution_summary_2421": root / "blocker_2421.json",
        "source_2421_contract_readiness_snapshot_gate": root / "contract_gate_2421.json",
        "remediation_result_2420": root / "remediation_result_2420.json",
        "source_traceability_manifest_2420": root / "manifest_2420.json",
        "source_lineage_map_2420": root / "lineage_2420.json",
        "missing_source_evidence_summary_2420": root / "missing_2420.json",
        "research_doc_2422": root / "research_doc_2422.md",
        "evidence_map_doc_2422": root / "evidence_map_doc_2422.md",
        "gap_summary_doc_2422": root / "gap_summary_doc_2422.md",
        "route_doc_2422": root / "route_doc_2422.md",
        "research_doc_2421": root / "research_doc_2421.md",
        "matrix_doc_2421": root / "matrix_doc_2421.md",
        "blocker_doc_2421": root / "blocker_doc_2421.md",
        "route_doc_2421": root / "route_doc_2421.md",
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
        "source_2422_contract_readiness_snapshot_path": (
            paths["contract_readiness_snapshot_2422"]
        ),
        "source_2422_contract_evidence_map_path": paths["contract_evidence_map_2422"],
        "source_2422_contract_gap_summary_path": paths["contract_gap_summary_2422"],
        "source_2422_contract_requirements_path": paths["contract_requirements_2422"],
        "source_2422_research_doc_path": paths["research_doc_2422"],
        "source_2422_evidence_map_doc_path": paths["evidence_map_doc_2422"],
        "source_2422_gap_summary_doc_path": paths["gap_summary_doc_2422"],
        "source_2422_route_doc_path": paths["route_doc_2422"],
        "source_2421_readiness_recheck_result_path": paths["readiness_recheck_2421"],
        "source_2421_pit_gate_recheck_matrix_path": (
            paths["pit_gate_recheck_matrix_2421"]
        ),
        "source_2421_blocker_resolution_summary_path": (
            paths["blocker_resolution_summary_2421"]
        ),
        "source_2421_contract_readiness_snapshot_gate_path": (
            paths["source_2421_contract_readiness_snapshot_gate"]
        ),
        "source_2421_research_doc_path": paths["research_doc_2421"],
        "source_2421_matrix_doc_path": paths["matrix_doc_2421"],
        "source_2421_blocker_doc_path": paths["blocker_doc_2421"],
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
        ("--source-2422-contract-readiness-snapshot", "contract_readiness_snapshot_2422"),
        ("--source-2422-contract-evidence-map", "contract_evidence_map_2422"),
        ("--source-2422-contract-gap-summary", "contract_gap_summary_2422"),
        ("--source-2422-contract-requirements", "contract_requirements_2422"),
        ("--source-2422-research-doc", "research_doc_2422"),
        ("--source-2422-evidence-map-doc", "evidence_map_doc_2422"),
        ("--source-2422-gap-summary-doc", "gap_summary_doc_2422"),
        ("--source-2422-route-doc", "route_doc_2422"),
        ("--source-2421-readiness-recheck-result", "readiness_recheck_2421"),
        ("--source-2421-pit-gate-recheck-matrix", "pit_gate_recheck_matrix_2421"),
        ("--source-2421-blocker-resolution-summary", "blocker_resolution_summary_2421"),
        (
            "--source-2421-contract-readiness-snapshot-gate",
            "source_2421_contract_readiness_snapshot_gate",
        ),
        ("--source-2421-research-doc", "research_doc_2421"),
        ("--source-2421-matrix-doc", "matrix_doc_2421"),
        ("--source-2421-blocker-doc", "blocker_doc_2421"),
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
        "contract_readiness_snapshot_2422": _contract_readiness_snapshot_2422(),
        "contract_evidence_map_2422": {"contract_evidence_map": {}},
        "contract_gap_summary_2422": {"contract_gap_summary": {"gaps": []}},
        "contract_requirements_2422": {"contract_requirements": {"requirements": []}},
        "readiness_recheck_2421": _readiness_recheck_2421(),
        "pit_gate_recheck_matrix_2421": {"rows": []},
        "blocker_resolution_summary_2421": {"remaining_blockers": []},
        "source_2421_contract_readiness_snapshot_gate": {
            "contract_readiness_snapshot_gate": {
                "pit_gate_ready": True,
                "contract_readiness_snapshot_required": True,
            }
        },
        "remediation_result_2420": _remediation_result_2420(),
        "source_traceability_manifest_2420": {
            "source_traceability_manifest": _manifest()
        },
        "source_lineage_map_2420": {"source_lineage_map": _lineage_map()},
        "missing_source_evidence_summary_2420": {
            "missing_source_evidence_summary": {
                "artifact_id": "growth_tilt_engine_signal_artifact",
                "missing_field_count": 0,
                "incomplete_field_count": 0,
                "unresolved_blocker_count": 0,
                "prior_missing_evidence_closed_by_2420": True,
            }
        },
    }


def _contract_readiness_snapshot_2422() -> dict[str, object]:
    return {
        "status": preflight.CONTRACT_READINESS_SNAPSHOT_READY_STATUS,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "pit_gate_blocked_count": 0,
        "remaining_blockers": [],
        "remaining_blocker_count": 0,
        "contract_ready": True,
        "contract_ready_count": 1,
        "contract_gap_count": 0,
        "missing_contract_evidence_count": 0,
        "incomplete_contract_field_count": 0,
        "source_traceability_remediation_status": "READY",
        "source_traceability_recheck_status": "ACCEPTED",
        "source_traceability_evidence_complete_after_2420": True,
        "paper_shadow_preflight_started": False,
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
        "status": preflight.PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS,
        "source_traceability_remediation_status": "READY",
        "source_traceability_recheck_status": "ACCEPTED",
        "source_traceability_evidence_complete_after_2420": True,
        "source_traceability_blocker_resolved": True,
        "remaining_blockers": [],
        "remaining_blocker_count": 0,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "paper_shadow_enabled": False,
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
        "status": preflight.SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS,
        "remediation_status": "READY",
        "artifact_id": "growth_tilt_engine_signal_artifact",
        "source_traceability_evidence_complete": True,
        "source_traceability_blocker_resolved": True,
        "blocker_resolved": True,
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
        "artifact_id": "growth_tilt_engine_signal_artifact",
        "traceability_status": "READY",
        "source_artifacts": [{"artifact_id": "source_artifact"}],
        "source_documents": [{"document_id": "source_document"}],
    }


def _lineage_map() -> dict[str, object]:
    return {
        "artifact_id": "growth_tilt_engine_signal_artifact",
        "upstream_dependencies": [{"artifact_id": "source_artifact"}],
        "source_documents": [{"document_id": "source_document"}],
    }


def _research_doc_texts() -> dict[str, str]:
    return {
        "research_doc_2422": preflight.CONTRACT_READINESS_SNAPSHOT_READY_STATUS,
        "evidence_map_doc_2422": "contract evidence map ready",
        "gap_summary_doc_2422": "contract gaps []",
        "route_doc_2422": "TRADING-2423_Growth_Tilt_Engine_Paper_Shadow_Preflight",
        "research_doc_2421": preflight.PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS,
        "matrix_doc_2421": "pit gate recheck matrix ready",
        "blocker_doc_2421": "remaining blockers []",
        "route_doc_2421": "TRADING-2422_Growth_Tilt_Engine_Contract_Readiness_Snapshot",
        "research_doc_2420": preflight.SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS,
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
            for report_id in preflight.REQUIRED_REPORT_IDS
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
        for report_id in preflight.REQUIRED_REPORT_IDS
    )
    return f"reports:\n{rows}\n"


def _artifact_catalog_text() -> str:
    return "\n".join(preflight.REQUIRED_CATALOG_REFERENCES)


def _system_flow_text() -> str:
    return "\n".join(preflight.REQUIRED_SYSTEM_FLOW_REFERENCES)


def _command_for_report(report_id: str) -> str:
    return {
        preflight.REPORT_TYPE: (
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
