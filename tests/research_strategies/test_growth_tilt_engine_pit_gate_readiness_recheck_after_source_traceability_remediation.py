from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation as impl,  # noqa: E501
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_remediation as recheck_after,  # noqa: E501
)

RUN_AFTER_RECHECK_NAME = (
    "run_growth_tilt_engine_pit_gate_readiness_recheck_after_source_traceability_"
    "remediation"
)
RUN_AFTER_RECHECK = getattr(impl, RUN_AFTER_RECHECK_NAME)


def test_after_remediation_builder_accepts_2420_and_marks_pit_gate_ready() -> None:
    sources = _source_documents()

    payload = (
        recheck_after.build_growth_tilt_pit_gate_readiness_recheck_after_source_traceability_remediation(
            sources["remediation_result_2420"],
            sources["source_traceability_manifest_2420"],
            sources["source_lineage_map_2420"],
            sources["missing_source_evidence_summary_2420"],
            sources["readiness_recheck_2419"],
            report_registry=_report_registry(),
            artifact_catalog_text=_artifact_catalog_text(),
            research_doc_texts=_research_doc_texts(),
        )
    )

    assert payload["status"] == recheck_after.READY_STATUS
    assert payload["source_traceability_recheck_status"] == "ACCEPTED"
    assert payload["source_traceability_blocker_resolved"] is True
    assert payload["blockers_resolved"] is True
    assert payload["resolved_blockers"] == ["growth_tilt_engine_signal_artifact"]
    assert payload["remaining_blockers"] == []
    assert payload["pit_gate_ready"] is True
    assert payload["pit_gate_ready_count"] == 1
    assert payload["pit_gate_blocked_count"] == 0
    assert payload["contract_ready"] is False
    assert payload["contract_ready_count"] == 0
    assert payload["contract_readiness_snapshot_required"] is True
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == recheck_after.NEXT_ROUTE_READY

    gate = payload["contract_readiness_snapshot_gate"]
    assert gate["contract_readiness_snapshot_required"] is True
    assert gate["next_route"] == recheck_after.NEXT_ROUTE_READY


def test_after_remediation_builder_blocks_nonzero_missing_evidence() -> None:
    sources = _source_documents()
    sources["missing_source_evidence_summary_2420"]["missing_source_evidence_summary"][
        "unresolved_blocker_count"
    ] = 1

    payload = (
        recheck_after.build_growth_tilt_pit_gate_readiness_recheck_after_source_traceability_remediation(
            sources["remediation_result_2420"],
            sources["source_traceability_manifest_2420"],
            sources["source_lineage_map_2420"],
            sources["missing_source_evidence_summary_2420"],
            sources["readiness_recheck_2419"],
            report_registry=_report_registry(),
            artifact_catalog_text=_artifact_catalog_text(),
            research_doc_texts=_research_doc_texts(),
        )
    )

    assert payload["status"] == recheck_after.BLOCKED_STATUS
    assert payload["source_traceability_recheck_status"] == "REJECTED"
    assert payload["source_traceability_blocker_resolved"] is False
    assert payload["pit_gate_ready"] is False
    assert payload["pit_gate_ready_count"] == 0
    assert payload["remaining_blockers"] == ["growth_tilt_engine_signal_artifact"]
    assert payload["recommended_next_research_task"] == recheck_after.NEXT_ROUTE_BLOCKED
    assert payload["blocker_resolution_error_count"] > 0


def test_after_remediation_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "after_recheck"
    docs_root = tmp_path / "docs" / "research"

    payload = RUN_AFTER_RECHECK(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == recheck_after.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["pit_gate_ready"] is True
    assert payload["pit_gate_ready_count"] == 1
    assert payload["contract_ready"] is False
    assert payload["contract_ready_count"] == 0
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == recheck_after.NEXT_ROUTE_READY

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "pit_gate_recheck_after_remediation_matrix_json",
        "blocker_resolution_summary_json",
        "contract_readiness_snapshot_gate_json",
        "markdown_path",
        "pit_gate_recheck_after_remediation_matrix_markdown",
        "blocker_resolution_summary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_after_remediation_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "after_recheck_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            (
                "growth-tilt-engine-pit-gate-readiness-recheck-after-source-"
                "traceability-remediation"
            ),
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
    assert recheck_after.READY_STATUS in result.output
    assert "source_traceability_recheck_status=ACCEPTED" in result.output
    assert "source_traceability_blocker_resolved=true" in result.output
    assert "blockers_resolved=true" in result.output
    assert "remaining_blockers=[]" in result.output
    assert "pit_gate_ready=true" in result.output
    assert "pit_gate_ready_count=1" in result.output
    assert "contract_ready=false" in result.output
    assert "contract_ready_count=0" in result.output
    assert "contract_readiness_snapshot_required=true" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "fresh_market_data_read=false" in result.output
    assert "blocker_resolution_error_count=0" in result.output
    assert f"next_route={recheck_after.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "readiness_recheck_after_remediation_result.json").exists()


def test_after_remediation_missing_prior_artifact_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["source_traceability_manifest_2420"].unlink()

    payload = RUN_AFTER_RECHECK(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == recheck_after.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["source_traceability_recheck_status"] == "REJECTED"
    assert payload["pit_gate_ready"] is False
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == recheck_after.NEXT_ROUTE_BLOCKED


def test_after_remediation_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[recheck_after.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-pit-gate-readiness-recheck-after-source-traceability-"
        "remediation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "readiness_recheck_after_remediation_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2422_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert recheck_after.REPORT_TYPE in catalog
    assert (
        "growth-tilt-engine-pit-gate-readiness-recheck-after-source-traceability-"
        "remediation"
        in system_flow
    )
    assert impl.TASK_REGISTER_ID in task_register
    assert recheck_after.NEXT_ROUTE_READY in system_flow


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "remediation_result_2420": root / "remediation_result_2420.json",
        "source_traceability_manifest_2420": root / "manifest_2420.json",
        "source_lineage_map_2420": root / "lineage_2420.json",
        "missing_source_evidence_summary_2420": root / "missing_2420.json",
        "readiness_recheck_2419": root / "readiness_recheck_2419.json",
        "research_doc_2420": root / "research_doc_2420.md",
        "manifest_doc_2420": root / "manifest_doc_2420.md",
        "lineage_doc_2420": root / "lineage_doc_2420.md",
        "route_doc_2420": root / "route_doc_2420.md",
        "research_doc_2419": root / "research_doc_2419.md",
        "blocker_doc_2419": root / "blocker_doc_2419.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    for key, path in paths.items():
        if key == "report_registry":
            path.write_text(_report_registry_yaml(), encoding="utf-8")
        elif key == "artifact_catalog":
            path.write_text(_artifact_catalog_text(), encoding="utf-8")
        elif key.endswith("_doc_2420") or key.endswith("_doc_2419"):
            path.write_text(_research_doc_texts().get(key, "evidence doc"), encoding="utf-8")
        else:
            path.write_text(json.dumps(sources[key], indent=2), encoding="utf-8")
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
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
        "source_2419_recheck_result_path": paths["readiness_recheck_2419"],
        "source_2419_research_doc_path": paths["research_doc_2419"],
        "source_2419_blocker_doc_path": paths["blocker_doc_2419"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    for option, key in (
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
        ("--source-2419-recheck-result", "readiness_recheck_2419"),
        ("--source-2419-research-doc", "research_doc_2419"),
        ("--source-2419-blocker-doc", "blocker_doc_2419"),
        ("--report-registry", "report_registry"),
        ("--artifact-catalog", "artifact_catalog"),
    ):
        args.extend([option, str(paths[key])])
    return args


def _source_documents() -> dict[str, dict]:
    return {
        "remediation_result_2420": {
            "status": recheck_after.SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS,
            "remediation_status": "READY",
            "artifact_id": "growth_tilt_engine_signal_artifact",
            "source_traceability_evidence_complete": True,
            "source_traceability_blocker_resolved": True,
            "blocker_resolved": True,
            "pit_gate_ready": False,
            "contract_ready": False,
            "recommended_next_research_task": (
                "TRADING-2421_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck_"
                "After_Source_Traceability_Remediation"
            ),
        },
        "source_traceability_manifest_2420": {
            "source_traceability_manifest": _manifest(),
        },
        "source_lineage_map_2420": {
            "source_lineage_map": _lineage_map(),
        },
        "missing_source_evidence_summary_2420": {
            "missing_source_evidence_summary": {
                "artifact_id": "growth_tilt_engine_signal_artifact",
                "missing_field_count": 0,
                "incomplete_field_count": 0,
                "unresolved_blocker_count": 0,
                "prior_missing_evidence_closed_by_2420": True,
            }
        },
        "readiness_recheck_2419": {
            "status": recheck_after.SOURCE_TRACEABILITY_RECHECK_2419_BLOCKED_STATUS,
            "remaining_blockers": ["growth_tilt_engine_signal_artifact"],
            "blocker_classification": {
                "blocker_classification": {
                    "growth_tilt_engine_signal_artifact": "source_traceability"
                }
            },
        },
    }


def _manifest() -> dict[str, object]:
    return {
        "artifact_id": "growth_tilt_engine_signal_artifact",
        "traceability_status": "READY",
        "pit_gate_ready_after_2420": False,
        "contract_ready_after_2420": False,
        "source_artifacts": [
            {
                "artifact_id": f"artifact_{index}",
                "path": path,
                "report_id": report_id,
                "report_registry_present": True,
                "catalog_reference_present": True,
                "source_file_present": True,
                "source_file_checksum": "sha256:test",
            }
            for index, (path, report_id) in enumerate(
                [
                    (
                        "outputs/research_strategies/"
                        "growth_tilt_engine_pit_gate_readiness_recheck/"
                        "readiness_recheck_result.json",
                        "growth_tilt_engine_pit_gate_readiness_recheck",
                    ),
                    (
                        "outputs/research_strategies/"
                        "growth_tilt_engine_signal_artifact_source_traceability_"
                        "remediation/remediation_result.json",
                        "growth_tilt_engine_signal_artifact_source_traceability_remediation",
                    ),
                ],
                start=1,
            )
        ],
        "source_documents": [
            {
                "document_id": f"doc_{index}",
                "path": path,
                "report_id": report_id,
                "report_registry_present": True,
                "catalog_reference_present": True,
                "document_present": True,
            }
            for index, (path, report_id) in enumerate(
                [
                    (
                        "docs/research/"
                        "growth_tilt_engine_signal_artifact_source_traceability_"
                        "remediation.md",
                        "growth_tilt_engine_signal_artifact_source_traceability_remediation",
                    ),
                    (
                        "docs/research/growth_tilt_engine_pit_gate_readiness_recheck.md",
                        "growth_tilt_engine_pit_gate_readiness_recheck",
                    ),
                ],
                start=1,
            )
        ],
    }


def _lineage_map() -> dict[str, object]:
    return {
        "artifact_id": "growth_tilt_engine_signal_artifact",
        "dependency_closure_reference": {
            "ready_for_pit_gate_recheck": True,
            "source_task": "TRADING-2418",
        },
        "upstream_dependencies": [
            {
                "artifact_id": "growth_tilt_engine_signal_artifact_source_traceability_manifest",
                "source_task": "TRADING-2420",
            }
        ],
        "source_documents": [
            {
                "document_id": "growth_tilt_engine_signal_artifact_source_traceability_manifest",
                "source_task": "TRADING-2420",
            }
        ],
    }


def _research_doc_texts() -> dict[str, str]:
    return {
        "research_doc_2420": recheck_after.SOURCE_TRACEABILITY_REMEDIATION_READY_STATUS,
        "manifest_doc_2420": "growth_tilt_engine_signal_artifact traceability READY",
        "lineage_doc_2420": "growth_tilt_engine_signal_artifact lineage READY",
        "route_doc_2420": (
            "TRADING-2421_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck_After_"
            "Source_Traceability_Remediation"
        ),
        "research_doc_2419": recheck_after.SOURCE_TRACEABILITY_RECHECK_2419_BLOCKED_STATUS,
        "blocker_doc_2419": "growth_tilt_engine_signal_artifact source_traceability",
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
            for report_id in recheck_after.REQUIRED_REPORT_IDS
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
        for report_id in recheck_after.REQUIRED_REPORT_IDS
    )
    return f"reports:\n{rows}\n"


def _artifact_catalog_text() -> str:
    return "\n".join(recheck_after.REQUIRED_CATALOG_REFERENCES)


def _command_for_report(report_id: str) -> str:
    return {
        recheck_after.REPORT_TYPE: (
            "aits research strategies "
            "growth-tilt-engine-pit-gate-readiness-recheck-after-source-"
            "traceability-remediation"
        ),
        "growth_tilt_engine_signal_artifact_source_traceability_remediation": (
            "aits research strategies "
            "growth-tilt-engine-signal-artifact-source-traceability-remediation"
        ),
        "growth_tilt_engine_pit_gate_readiness_recheck": (
            "aits research strategies growth-tilt-engine-pit-gate-readiness-recheck"
        ),
    }[report_id]
