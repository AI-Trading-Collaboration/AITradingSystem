from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_contract_readiness_snapshot as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_contract_readiness_snapshot as snapshot,
)


def test_contract_readiness_builder_marks_ready_after_2421() -> None:
    sources = _source_documents()

    payload = snapshot.build_growth_tilt_engine_contract_readiness_snapshot(
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

    assert payload["status"] == snapshot.READY_STATUS
    assert payload["pit_gate_ready"] is True
    assert payload["pit_gate_ready_count"] == 1
    assert payload["remaining_blockers"] == []
    assert payload["contract_ready"] is True
    assert payload["contract_ready_count"] == 1
    assert payload["contract_gap_count"] == 0
    assert payload["missing_contract_evidence_count"] == 0
    assert payload["incomplete_contract_field_count"] == 0
    assert payload["blockers_resolved_by_snapshot"] is False
    assert payload["paper_shadow_preflight_required"] is True
    assert payload["paper_shadow_preflight_started"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == snapshot.NEXT_ROUTE_READY

    gap_summary = payload["contract_gap_summary"]
    assert gap_summary["contract_gap_count"] == 0
    assert gap_summary["next_route"] == snapshot.NEXT_ROUTE_READY


def test_contract_readiness_builder_blocks_remaining_pit_blocker() -> None:
    sources = _source_documents()
    sources["readiness_recheck_2421"]["remaining_blockers"] = [
        "growth_tilt_engine_signal_artifact"
    ]
    sources["readiness_recheck_2421"]["remaining_blocker_count"] = 1

    payload = snapshot.build_growth_tilt_engine_contract_readiness_snapshot(
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

    assert payload["status"] == snapshot.BLOCKED_STATUS
    assert payload["contract_ready"] is False
    assert payload["contract_ready_count"] == 0
    assert payload["contract_gap_count"] > 0
    assert "remaining_pit_blockers_closed" in payload["contract_gap_ids"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == snapshot.NEXT_ROUTE_BLOCKED


def test_contract_readiness_builder_blocks_source_traceability_regression() -> None:
    sources = _source_documents()
    sources["remediation_result_2420"]["status"] = "BROKEN"

    payload = snapshot.build_growth_tilt_engine_contract_readiness_snapshot(
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

    assert payload["status"] == snapshot.BLOCKED_STATUS
    assert payload["contract_ready"] is False
    assert "source_traceability_remediation_ready" in payload["contract_gap_ids"]
    assert payload["source_traceability_evidence_complete_after_2420"] is True
    assert payload["recommended_next_research_task"] == snapshot.NEXT_ROUTE_BLOCKED


def test_contract_readiness_builder_blocks_missing_registration() -> None:
    sources = _source_documents()

    payload = snapshot.build_growth_tilt_engine_contract_readiness_snapshot(
        sources["readiness_recheck_2421"],
        sources["remediation_result_2420"],
        sources["source_traceability_manifest_2420"],
        sources["source_lineage_map_2420"],
        sources["missing_source_evidence_summary_2420"],
        report_registry={"reports": []},
        artifact_catalog_text="",
        system_flow_text="",
        research_doc_texts={"source_2421_research_doc": ""},
    )

    assert payload["status"] == snapshot.BLOCKED_STATUS
    assert payload["missing_contract_evidence_count"] >= 4
    assert "report_registry_registered" in payload["contract_gap_ids"]
    assert "artifact_catalog_registered" in payload["contract_gap_ids"]
    assert "system_flow_registered" in payload["contract_gap_ids"]
    assert "research_docs_registered" in payload["contract_gap_ids"]


def test_contract_readiness_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "contract_snapshot"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_contract_readiness_snapshot(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == snapshot.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["contract_ready"] is True
    assert payload["contract_ready_count"] == 1
    assert payload["contract_gap_count"] == 0
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == snapshot.NEXT_ROUTE_READY

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "contract_evidence_map_json",
        "contract_gap_summary_json",
        "contract_requirements_json",
        "markdown_path",
        "contract_evidence_map_markdown",
        "contract_gap_summary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_contract_readiness_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "contract_snapshot_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-contract-readiness-snapshot",
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
    assert snapshot.READY_STATUS in result.output
    assert "pit_gate_ready=true" in result.output
    assert "pit_gate_ready_count=1" in result.output
    assert "remaining_blockers=[]" in result.output
    assert "contract_ready=true" in result.output
    assert "contract_ready_count=1" in result.output
    assert "contract_gap_count=0" in result.output
    assert "missing_contract_evidence_count=0" in result.output
    assert "incomplete_contract_field_count=0" in result.output
    assert "paper_shadow_preflight_required=true" in result.output
    assert "paper_shadow_preflight_started=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "fresh_market_data_read=false" in result.output
    assert "source_validation_error_count=0" in result.output
    assert f"next_route={snapshot.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "contract_readiness_snapshot_result.json").exists()


def test_contract_readiness_missing_prior_artifact_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2421_contract_readiness_snapshot_gate"].unlink()

    payload = impl.run_growth_tilt_engine_contract_readiness_snapshot(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == snapshot.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["contract_ready"] is False
    assert payload["contract_ready_count"] == 0
    assert payload["contract_gap_count"] == 1
    assert payload["missing_contract_evidence_count"] == 1
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == snapshot.NEXT_ROUTE_BLOCKED


def test_contract_readiness_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[snapshot.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-contract-readiness-snapshot"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "contract_readiness_snapshot_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2423_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert snapshot.REPORT_TYPE in catalog
    assert "growth-tilt-engine-contract-readiness-snapshot" in system_flow
    assert snapshot.READY_STATUS in system_flow
    assert snapshot.NEXT_ROUTE_READY in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "readiness_recheck_2421": root / "readiness_recheck_2421.json",
        "pit_gate_recheck_matrix_2421": root / "matrix_2421.json",
        "blocker_resolution_summary_2421": root / "blocker_2421.json",
        "source_2421_contract_readiness_snapshot_gate": root / "contract_gate_2421.json",
        "remediation_result_2420": root / "remediation_result_2420.json",
        "source_traceability_manifest_2420": root / "manifest_2420.json",
        "source_lineage_map_2420": root / "lineage_2420.json",
        "missing_source_evidence_summary_2420": root / "missing_2420.json",
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
        "readiness_recheck_2421": _readiness_recheck_2421(),
        "pit_gate_recheck_matrix_2421": {"rows": []},
        "blocker_resolution_summary_2421": {"remaining_blockers": []},
        "source_2421_contract_readiness_snapshot_gate": {
            "contract_readiness_snapshot_gate": {
                "pit_gate_ready": True,
                "contract_readiness_snapshot_required": True,
                "next_route": (
                    "TRADING-2422_Growth_Tilt_Engine_Contract_Readiness_Snapshot"
                ),
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


def _readiness_recheck_2421() -> dict[str, object]:
    return {
        "status": snapshot.PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS,
        "source_traceability_remediation_status": "READY",
        "source_traceability_recheck_status": "ACCEPTED",
        "source_traceability_evidence_complete_after_2420": True,
        "source_traceability_blocker_resolved": True,
        "resolved_blockers": ["growth_tilt_engine_signal_artifact"],
        "remaining_blockers": [],
        "remaining_blocker_count": 0,
        "blocker_classification": {
            "growth_tilt_engine_signal_artifact": "source_traceability"
        },
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "pit_gate_blocked_count": 0,
        "contract_ready": False,
        "contract_ready_count": 0,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _remediation_result_2420() -> dict[str, object]:
    return {
        "status": snapshot.SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS,
        "remediation_status": "READY",
        "artifact_id": "growth_tilt_engine_signal_artifact",
        "source_traceability_evidence_complete": True,
        "source_traceability_blocker_resolved": True,
        "blocker_resolved": True,
        "contract_ready": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "production_allowed": False,
        "broker_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _manifest() -> dict[str, object]:
    return {
        "artifact_id": "growth_tilt_engine_signal_artifact",
        "traceability_status": "READY",
        "source_artifacts": [
            {
                "artifact_id": "growth_tilt_engine_signal_artifact_remediation",
                "path": (
                    "outputs/research_strategies/"
                    "growth_tilt_engine_signal_artifact_source_traceability_"
                    "remediation/remediation_result.json"
                ),
                "report_id": (
                    "growth_tilt_engine_signal_artifact_source_traceability_"
                    "remediation"
                ),
            }
        ],
        "source_documents": [
            {
                "document_id": "growth_tilt_engine_source_traceability_manifest",
                "path": (
                    "docs/research/"
                    "growth_tilt_engine_signal_artifact_source_traceability_"
                    "manifest.md"
                ),
                "report_id": (
                    "growth_tilt_engine_signal_artifact_source_traceability_"
                    "remediation"
                ),
            }
        ],
    }


def _lineage_map() -> dict[str, object]:
    return {
        "artifact_id": "growth_tilt_engine_signal_artifact",
        "upstream_dependencies": [
            {
                "artifact_id": "growth_tilt_engine_signal_artifact_remediation",
                "source_task": "TRADING-2420",
            }
        ],
        "source_documents": [
            {
                "document_id": "growth_tilt_engine_signal_artifact_manifest_doc",
                "source_task": "TRADING-2420",
            }
        ],
    }


def _research_doc_texts() -> dict[str, str]:
    return {
        "research_doc_2421": snapshot.PIT_GATE_AFTER_SOURCE_TRACEABILITY_READY_STATUS,
        "matrix_doc_2421": "pit gate recheck matrix ready",
        "blocker_doc_2421": "remaining blockers []",
        "route_doc_2421": "TRADING-2422_Growth_Tilt_Engine_Contract_Readiness_Snapshot",
        "research_doc_2420": snapshot.SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS,
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
            for report_id in snapshot.REQUIRED_REPORT_IDS
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
        for report_id in snapshot.REQUIRED_REPORT_IDS
    )
    return f"reports:\n{rows}\n"


def _artifact_catalog_text() -> str:
    return "\n".join(snapshot.REQUIRED_CATALOG_REFERENCES)


def _system_flow_text() -> str:
    return "\n".join(snapshot.REQUIRED_SYSTEM_FLOW_REFERENCES)


def _command_for_report(report_id: str) -> str:
    return {
        snapshot.REPORT_TYPE: (
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
