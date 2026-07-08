from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_signal_artifact_source_traceability_remediation as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_signal_artifact_source_traceability_remediation as remediation,
)


def test_signal_artifact_source_traceability_builder_ready() -> None:
    sources = _source_documents()
    payload = remediation.build_growth_tilt_signal_artifact_source_traceability_remediation(
        sources["readiness_recheck_2419"],
        sources["valid_until_dependency_evidence_2418"],
        sources["signal_validity_contract_evidence_2418"],
        sources["stale_signal_policy_evidence_2418"],
        sources["growth_tilt_valid_until_alignment_evidence_2418"],
        sources["source_traceability_closure_evidence_2417"],
        sources["upstream_artifact_closure_evidence_2417"],
        report_registry=_report_registry(),
        artifact_catalog_text=_artifact_catalog_text(),
        source_file_manifest=_source_file_manifest(exists=True),
        source_document_manifest=_source_document_manifest(exists=True),
    )

    assert payload["status"] == remediation.READY_STATUS
    assert payload["remediation_status"] == "READY"
    assert payload["artifact_id"] == "growth_tilt_engine_signal_artifact"
    assert payload["source_traceability_evidence_complete"] is True
    assert payload["source_traceability_blocker_resolved"] is True
    assert payload["blocker_resolved"] is True
    assert payload["blocker_downgraded"] is False
    assert payload["pit_gate_ready"] is False
    assert payload["contract_ready"] is False
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["new_signal_generated"] is False
    assert payload["backtest_run"] is False
    assert payload["recommended_next_research_task"] == remediation.NEXT_ROUTE_READY

    manifest = payload["source_traceability_manifest"]
    assert manifest["traceability_status"] == "READY"
    assert manifest["valid_until_boundary"]["boundary_explicit"] is True
    assert manifest["dependency_closure_reference"]["ready_for_pit_gate_recheck"] is True
    assert manifest["prior_missing_evidence_reference"][
        "source_traceability_evidence_ready_before_2420"
    ] is False
    assert payload["missing_source_evidence_summary"]["missing_field_count"] == 0
    assert payload["missing_source_evidence_summary"]["incomplete_field_count"] == 0
    assert payload["missing_source_evidence_summary"]["unresolved_blocker_count"] == 0


def test_signal_artifact_source_traceability_builder_blocks_missing_evidence() -> None:
    sources = _source_documents()
    payload = remediation.build_growth_tilt_signal_artifact_source_traceability_remediation(
        sources["readiness_recheck_2419"],
        sources["valid_until_dependency_evidence_2418"],
        sources["signal_validity_contract_evidence_2418"],
        sources["stale_signal_policy_evidence_2418"],
        sources["growth_tilt_valid_until_alignment_evidence_2418"],
        sources["source_traceability_closure_evidence_2417"],
        sources["upstream_artifact_closure_evidence_2417"],
        report_registry=_report_registry(),
        artifact_catalog_text=_artifact_catalog_text().replace(
            "source_traceability_closure_evidence.json",
            "missing_source_traceability_closure_evidence.json",
        ),
        source_file_manifest=_source_file_manifest(exists=True),
        source_document_manifest=_source_document_manifest(exists=True),
    )

    assert payload["status"] == remediation.BLOCKED_MISSING_EVIDENCE_STATUS
    assert payload["remediation_status"] == "BLOCKED"
    assert payload["source_traceability_evidence_complete"] is False
    assert payload["blocker_resolved"] is False
    assert payload["pit_gate_ready"] is False
    assert payload["contract_ready"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == remediation.NEXT_ROUTE_BLOCKED
    assert payload["missing_source_evidence_summary"]["missing_field_count"] > 0


def test_signal_artifact_source_traceability_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "remediation"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_signal_artifact_source_traceability_remediation(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == remediation.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["source_traceability_blocker_resolved"] is True
    assert payload["pit_gate_ready"] is False
    assert payload["contract_ready"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["new_signal_generated"] is False
    assert payload["recommended_next_research_task"] == remediation.NEXT_ROUTE_READY

    for key in (
        "json_path",
        "source_traceability_manifest_json",
        "source_lineage_map_json",
        "missing_source_evidence_summary_json",
        "markdown_path",
        "source_traceability_manifest_markdown",
        "source_lineage_map_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_signal_artifact_source_traceability_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "remediation_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-signal-artifact-source-traceability-remediation",
            *_source_args(paths),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "240"},
        terminal_width=240,
    )

    assert result.exit_code == 0, result.output
    assert remediation.READY_STATUS in result.output
    assert "remediation_status=READY" in result.output
    assert "artifact_id=growth_tilt_engine_signal_artifact" in result.output
    assert "source_traceability_evidence_complete=true" in result.output
    assert "source_traceability_blocker_resolved=true" in result.output
    assert "blocker_resolved=true" in result.output
    assert "blocker_downgraded=false" in result.output
    assert "pit_gate_ready=false" in result.output
    assert "contract_ready=false" in result.output
    assert "pit_gate_ready_count=0" in result.output
    assert "contract_ready_count=0" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "new_signal_generated=false" in result.output
    assert "missing_field_count=0" in result.output
    assert "incomplete_field_count=0" in result.output
    assert "unresolved_blocker_count=0" in result.output
    assert f"next_route={remediation.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "remediation_result.json").exists()


def test_signal_artifact_source_traceability_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[
        "growth_tilt_engine_signal_artifact_source_traceability_remediation"
    ]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-signal-artifact-source-traceability-remediation"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("remediation_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2421_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "growth_tilt_engine_signal_artifact_source_traceability_remediation" in catalog
    assert (
        "growth-tilt-engine-signal-artifact-source-traceability-remediation"
        in system_flow
    )
    assert impl.TASK_REGISTER_ID in task_register
    assert remediation.NEXT_ROUTE_READY in system_flow


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "readiness_recheck_2419": root / "readiness_recheck_2419.json",
        "blocker_classification_2419": root / "blocker_classification_2419.json",
        "valid_until_dependency_evidence_2418": (
            root / "valid_until_dependency_evidence_2418.json"
        ),
        "signal_validity_contract_evidence_2418": (
            root / "signal_validity_contract_evidence_2418.json"
        ),
        "stale_signal_policy_evidence_2418": (
            root / "stale_signal_policy_evidence_2418.json"
        ),
        "growth_tilt_valid_until_alignment_evidence_2418": (
            root / "growth_tilt_valid_until_alignment_evidence_2418.json"
        ),
        "source_traceability_closure_evidence_2417": (
            root / "source_traceability_closure_evidence_2417.json"
        ),
        "upstream_artifact_closure_evidence_2417": (
            root / "upstream_artifact_closure_evidence_2417.json"
        ),
        "research_doc_2419": root / "research_doc_2419.md",
        "blocker_doc_2419": root / "blocker_doc_2419.md",
        "research_doc_2418": root / "research_doc_2418.md",
        "source_traceability_doc_2417": root / "source_traceability_doc_2417.md",
        "upstream_artifact_doc_2417": root / "upstream_artifact_doc_2417.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
    }
    for key, path in paths.items():
        if key == "blocker_classification_2419":
            path.write_text(
                json.dumps(sources["readiness_recheck_2419"]["blocker_classification"]),
                encoding="utf-8",
            )
        elif key == "report_registry":
            path.write_text(_report_registry_yaml(), encoding="utf-8")
        elif key == "artifact_catalog":
            path.write_text(_artifact_catalog_text(), encoding="utf-8")
        elif key.endswith("_doc_2419") or key.endswith("_doc_2418") or key.endswith("_doc_2417"):
            path.write_text("growth_tilt_engine_signal_artifact evidence", encoding="utf-8")
        else:
            path.write_text(json.dumps(sources[key], indent=2), encoding="utf-8")
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2419_recheck_result_path": paths["readiness_recheck_2419"],
        "source_2419_blocker_classification_path": paths["blocker_classification_2419"],
        "source_2419_research_doc_path": paths["research_doc_2419"],
        "source_2419_blocker_doc_path": paths["blocker_doc_2419"],
        "source_2418_valid_until_dependency_evidence_path": (
            paths["valid_until_dependency_evidence_2418"]
        ),
        "source_2418_signal_validity_contract_evidence_path": (
            paths["signal_validity_contract_evidence_2418"]
        ),
        "source_2418_stale_signal_policy_evidence_path": (
            paths["stale_signal_policy_evidence_2418"]
        ),
        "source_2418_growth_tilt_valid_until_alignment_evidence_path": (
            paths["growth_tilt_valid_until_alignment_evidence_2418"]
        ),
        "source_2418_research_doc_path": paths["research_doc_2418"],
        "source_2417_source_traceability_closure_evidence_path": (
            paths["source_traceability_closure_evidence_2417"]
        ),
        "source_2417_upstream_artifact_closure_evidence_path": (
            paths["upstream_artifact_closure_evidence_2417"]
        ),
        "source_2417_source_traceability_doc_path": (
            paths["source_traceability_doc_2417"]
        ),
        "source_2417_upstream_artifact_doc_path": paths["upstream_artifact_doc_2417"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    for option, key in (
        ("--source-2419-recheck-result", "readiness_recheck_2419"),
        ("--source-2419-blocker-classification", "blocker_classification_2419"),
        ("--source-2419-research-doc", "research_doc_2419"),
        ("--source-2419-blocker-doc", "blocker_doc_2419"),
        (
            "--source-2418-valid-until-dependency-evidence",
            "valid_until_dependency_evidence_2418",
        ),
        (
            "--source-2418-signal-validity-contract-evidence",
            "signal_validity_contract_evidence_2418",
        ),
        ("--source-2418-stale-signal-policy-evidence", "stale_signal_policy_evidence_2418"),
        (
            "--source-2418-growth-tilt-valid-until-alignment-evidence",
            "growth_tilt_valid_until_alignment_evidence_2418",
        ),
        ("--source-2418-research-doc", "research_doc_2418"),
        (
            "--source-2417-source-traceability-closure-evidence",
            "source_traceability_closure_evidence_2417",
        ),
        (
            "--source-2417-upstream-artifact-closure-evidence",
            "upstream_artifact_closure_evidence_2417",
        ),
        ("--source-2417-source-traceability-doc", "source_traceability_doc_2417"),
        ("--source-2417-upstream-artifact-doc", "upstream_artifact_doc_2417"),
        ("--report-registry", "report_registry"),
        ("--artifact-catalog", "artifact_catalog"),
    ):
        args.extend([option, str(paths[key])])
    return args


def _source_documents() -> dict[str, dict]:
    return {
        "readiness_recheck_2419": {
            "status": (
                "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_RECHECK_"
                "BLOCKED_BY_SIGNAL_ARTIFACT_SOURCE_TRACEABILITY"
            ),
            "as_of": "2026-07-08",
            "generated_at": "2026-07-08T14:12:43Z",
            "remaining_blockers": ["growth_tilt_engine_signal_artifact"],
            "blocker_classification": {
                "rows": [
                    {
                        "blocker_id": "growth_tilt_engine_signal_artifact",
                        "blocker_classification": "source_traceability",
                        "still_blocked_after_recheck": True,
                        "recommended_next_task": (
                            "TRADING-2420_Growth_Tilt_Engine_Signal_Artifact_"
                            "Source_Traceability_Remediation"
                        ),
                    }
                ]
            },
        },
        "valid_until_dependency_evidence_2418": {
            "valid_until_dependency_evidence": {
                "dependency_feature_id": "execution_signal_validity_policy",
                "evidence_rows": [
                    {
                        "dependency_id": (
                            "growth_tilt_engine:execution_signal_validity_policy:"
                            "signal_validity_dependency:v1"
                        ),
                        "dependent_feature_or_signal": "execution_signal_validity_policy",
                        "evidence_status": "CLOSED_WITH_EVIDENCE",
                        "ready_for_pit_gate_recheck": True,
                        "source_reference": (
                            "config/research/strategy_execution_policy_registry.yaml:"
                            "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1."
                            "signal_policy"
                        ),
                        "valid_from_source": "not emitted per signal; policy says next_trading_day",
                        "valid_until_source": "policy window=10 bdays; per-signal field missing",
                        "stale_after_source": "valid_until_or_earlier_decay_boundary",
                        "policy_window_bdays": 10,
                        "execution_lag_bdays": 1,
                    }
                ],
            }
        },
        "signal_validity_contract_evidence_2418": {
            "signal_validity_contract_evidence": {
                "ready_for_recheck": True,
                "missing_field_count": 0,
                "required_fields": ["signal_id", "valid_until"],
            }
        },
        "stale_signal_policy_evidence_2418": {
            "stale_signal_policy_evidence": {"ready_for_recheck": True}
        },
        "growth_tilt_valid_until_alignment_evidence_2418": {
            "growth_tilt_valid_until_alignment_evidence": {
                "ready_for_recheck": True,
                "alignment_rows": [
                    {
                        "remaining_gap": (
                            "growth_tilt_engine_signal_artifact remains source-"
                            "traceability blocked"
                        )
                    }
                ],
            }
        },
        "source_traceability_closure_evidence_2417": {
            "source_traceability_closure_evidence": {
                "evidence_rows": [
                    {
                        "feature_id": "growth_tilt_engine_signal_artifact",
                        "source_traceability_evidence_ready": False,
                        "traceability_closure_status": (
                            "STILL_BLOCKED_MISSING_UPSTREAM_SIGNAL_ARTIFACT"
                        ),
                        "still_blocked_reason": (
                            "missing standalone growth_tilt_engine signal artifact"
                        ),
                    }
                ]
            }
        },
        "upstream_artifact_closure_evidence_2417": {
            "upstream_artifact_closure_evidence": {
                "evidence_rows": [
                    {
                        "feature_id": "growth_tilt_engine_signal_artifact",
                        "upstream_artifact_available_after_2417": False,
                        "upstream_artifact_closure_status": (
                            "STILL_BLOCKED_MISSING_UPSTREAM_SIGNAL_ARTIFACT"
                        ),
                    }
                ]
            }
        },
    }


def _source_file_manifest(*, exists: bool) -> dict[str, dict[str, object]]:
    return {
        spec["path"]: {"exists": exists, "sha256": "sha256:test"}
        for spec in remediation.SOURCE_ARTIFACT_SPECS
    }


def _source_document_manifest(*, exists: bool) -> dict[str, dict[str, object]]:
    return {spec["path"]: {"exists": exists} for spec in remediation.SOURCE_DOCUMENT_SPECS}


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
            for report_id in remediation.REQUIRED_REPORT_IDS
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
        for report_id in remediation.REQUIRED_REPORT_IDS
    )
    return f"reports:\n{rows}\n"


def _artifact_catalog_text() -> str:
    parts: list[str] = []
    parts.extend(spec["path"] for spec in remediation.SOURCE_ARTIFACT_SPECS)
    parts.extend(spec["path"] for spec in remediation.SOURCE_DOCUMENT_SPECS)
    parts.extend(_command_for_report(report_id) for report_id in remediation.REQUIRED_REPORT_IDS)
    return "\n".join(parts)


def _command_for_report(report_id: str) -> str:
    return {
        "growth_tilt_engine_signal_artifact_source_traceability_remediation": (
            "aits research strategies "
            "growth-tilt-engine-signal-artifact-source-traceability-remediation"
        ),
        "growth_tilt_engine_pit_gate_readiness_recheck": (
            "aits research strategies growth-tilt-engine-pit-gate-readiness-recheck"
        ),
        "growth_tilt_engine_valid_until_dependency_evidence_closure": (
            "aits research strategies "
            "growth-tilt-engine-valid-until-dependency-evidence-closure"
        ),
        "growth_tilt_engine_source_traceability_upstream_artifact_closure": (
            "aits research strategies "
            "growth-tilt-engine-source-traceability-upstream-artifact-closure"
        ),
    }[report_id]
