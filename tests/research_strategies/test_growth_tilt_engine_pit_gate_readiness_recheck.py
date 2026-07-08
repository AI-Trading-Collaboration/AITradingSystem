from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_pit_gate_readiness_recheck as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_pit_gate_readiness_recheck as recheck,
)


def test_pit_gate_readiness_recheck_builder_preserves_blocker_state() -> None:
    sources = _source_documents()
    payload = recheck.build_growth_tilt_pit_gate_readiness_recheck(
        sources["closure_result_2418"],
        sources["valid_until_dependency_evidence_2418"],
        sources["remaining_blocker_summary_2418"],
        sources["closure_result_2417"],
        sources["remaining_blocker_summary_2417"],
        sources["closure_result_2416"],
        sources["remaining_blocker_matrix_2416"],
        sources["pit_gate_evidence_requirements_2416"],
        sources["readiness_snapshot_result_2415"],
        sources["readiness_matrix_2415"],
        sources["readiness_validation_2415"],
        sources["remaining_blocker_summary_2415"],
        pit_input_registry=_pit_input_registry(),
    )

    assert payload["status"] == recheck.BLOCKED_BY_SIGNAL_ARTIFACT_STATUS
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["pit_gate_blocked_count"] == 10
    assert payload["valid_until_dependency_evidence_ready_from_2418"] is True
    assert payload["valid_until_dependency_still_blocked_count_after_recheck"] == 0
    assert payload["remaining_blockers"] == ["growth_tilt_engine_signal_artifact"]
    assert payload["blocker_classification"]["blocker_classification"] == {
        "growth_tilt_engine_signal_artifact": "source_traceability"
    }
    assert payload["auto_mark_pit_gate_ready"] is False
    assert payload["auto_mark_contract_ready"] is False
    assert payload["blockers_resolved"] is False
    assert payload["blockers_downgraded"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE

    matrix_rows = payload["pit_gate_recheck_matrix"]["matrix_rows"]
    signal_row = next(
        row
        for row in matrix_rows
        if row["source_feature_id"] == "growth_tilt_engine_signal_artifact"
    )
    assert signal_row["recheck_status"] == (
        "blocked_by_signal_artifact_source_traceability"
    )
    assert signal_row["remaining_blocker_after_recheck"] is True


def test_pit_gate_readiness_recheck_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "recheck"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_pit_gate_readiness_recheck(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["remaining_blockers"] == ["growth_tilt_engine_signal_artifact"]
    assert payload["recommended_next_research_task"] == recheck.NEXT_ROUTE
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "pit_gate_recheck_matrix_json",
        "blocker_classification_json",
        "remaining_blocker_summary_json",
        "markdown_path",
        "pit_gate_recheck_matrix_markdown",
        "signal_artifact_blocker_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_pit_gate_readiness_recheck_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "recheck_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-pit-gate-readiness-recheck",
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
    assert impl.READY_STATUS in result.output
    assert "pit_gate_ready_count=0" in result.output
    assert "contract_ready_count=0" in result.output
    assert "pit_gate_blocked_count=10" in result.output
    assert "remaining_blockers=['growth_tilt_engine_signal_artifact']" in result.output
    assert "valid_until_dependency_evidence_ready_from_2418=true" in result.output
    assert "valid_until_dependency_still_blocked_count_after_recheck=0" in result.output
    assert "auto_mark_pit_gate_ready=false" in result.output
    assert "auto_mark_contract_ready=false" in result.output
    assert "blockers_resolved=false" in result.output
    assert "blockers_downgraded=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert "daily_report_generated=false" in result.output
    assert f"next_route={recheck.NEXT_ROUTE}" in result.output
    assert (output_root / "readiness_recheck_result.json").exists()


def test_pit_gate_readiness_recheck_missing_source_blocks(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    paths["valid_until_dependency_evidence_2418"].unlink()

    payload = impl.run_growth_tilt_engine_pit_gate_readiness_recheck(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.BLOCKED_SOURCE_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["recheck_validation"]["valid"] is False
    assert payload["pit_gate_ready_count"] == 0
    assert payload["contract_ready_count"] == 0
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False


def test_pit_gate_readiness_recheck_docs_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_pit_gate_readiness_recheck"]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-pit-gate-readiness-recheck"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("readiness_recheck_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2420_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "growth_tilt_engine_pit_gate_readiness_recheck" in catalog
    assert "growth-tilt-engine-pit-gate-readiness-recheck" in system_flow
    assert impl.TASK_REGISTER_ID in task_register
    assert recheck.NEXT_ROUTE in system_flow


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    sources = _source_documents()
    paths = {
        "closure_result_2418": root / "closure_result_2418.json",
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
        "remaining_blocker_summary_2418": (
            root / "remaining_blocker_summary_2418.json"
        ),
        "closure_result_2417": root / "closure_result_2417.json",
        "remaining_blocker_summary_2417": (
            root / "remaining_blocker_summary_2417.json"
        ),
        "closure_result_2416": root / "closure_result_2416.json",
        "remaining_blocker_matrix_2416": root / "remaining_blocker_matrix_2416.json",
        "pit_gate_evidence_requirements_2416": (
            root / "pit_gate_evidence_requirements_2416.json"
        ),
        "readiness_snapshot_result_2415": (
            root / "readiness_snapshot_result_2415.json"
        ),
        "readiness_matrix_2415": root / "readiness_matrix_2415.json",
        "readiness_validation_2415": root / "readiness_validation_2415.json",
        "remaining_blocker_summary_2415": (
            root / "remaining_blocker_summary_2415.json"
        ),
        "pit_input_registry": root / "pit_input_registry.yaml",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "research_doc_2418": root / "research_doc_2418.md",
        "route_doc_2418": root / "route_doc_2418.md",
    }
    for key, path in paths.items():
        if key == "pit_input_registry":
            path.write_text(_pit_input_registry_yaml(), encoding="utf-8")
        elif key == "report_registry":
            path.write_text(_report_registry_yaml(), encoding="utf-8")
        elif key == "artifact_catalog":
            path.write_text(_artifact_catalog_text(), encoding="utf-8")
        elif key == "research_doc_2418":
            path.write_text(
                "GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY",
                encoding="utf-8",
            )
        elif key == "route_doc_2418":
            path.write_text(
                "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck",
                encoding="utf-8",
            )
        else:
            path.write_text(json.dumps(sources[key], indent=2), encoding="utf-8")
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2418_closure_result_path": paths["closure_result_2418"],
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
        "source_2418_remaining_blocker_summary_path": (
            paths["remaining_blocker_summary_2418"]
        ),
        "source_2418_research_doc_path": paths["research_doc_2418"],
        "source_2418_route_doc_path": paths["route_doc_2418"],
        "source_2417_closure_result_path": paths["closure_result_2417"],
        "source_2417_remaining_blocker_summary_path": (
            paths["remaining_blocker_summary_2417"]
        ),
        "source_2416_closure_result_path": paths["closure_result_2416"],
        "source_2416_remaining_blocker_matrix_path": (
            paths["remaining_blocker_matrix_2416"]
        ),
        "source_2416_pit_gate_evidence_requirements_path": (
            paths["pit_gate_evidence_requirements_2416"]
        ),
        "source_2415_readiness_snapshot_result_path": (
            paths["readiness_snapshot_result_2415"]
        ),
        "source_2415_readiness_matrix_path": paths["readiness_matrix_2415"],
        "source_2415_readiness_validation_path": paths["readiness_validation_2415"],
        "source_2415_remaining_blocker_summary_path": (
            paths["remaining_blocker_summary_2415"]
        ),
        "pit_input_registry_path": paths["pit_input_registry"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    args: list[str] = []
    for option, key in (
        ("--source-2418-closure-result", "closure_result_2418"),
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
        ("--source-2418-remaining-blocker-summary", "remaining_blocker_summary_2418"),
        ("--source-2418-research-doc", "research_doc_2418"),
        ("--source-2418-route-doc", "route_doc_2418"),
        ("--source-2417-closure-result", "closure_result_2417"),
        ("--source-2417-remaining-blocker-summary", "remaining_blocker_summary_2417"),
        ("--source-2416-closure-result", "closure_result_2416"),
        ("--source-2416-remaining-blocker-matrix", "remaining_blocker_matrix_2416"),
        (
            "--source-2416-pit-gate-evidence-requirements",
            "pit_gate_evidence_requirements_2416",
        ),
        ("--source-2415-readiness-snapshot-result", "readiness_snapshot_result_2415"),
        ("--source-2415-readiness-matrix", "readiness_matrix_2415"),
        ("--source-2415-readiness-validation", "readiness_validation_2415"),
        ("--source-2415-remaining-blocker-summary", "remaining_blocker_summary_2415"),
        ("--pit-input-registry", "pit_input_registry"),
        ("--report-registry", "report_registry"),
        ("--artifact-catalog", "artifact_catalog"),
    ):
        args.extend([option, str(paths[key])])
    return args


def _source_documents() -> dict[str, dict]:
    rows = _readiness_rows()
    return {
        "closure_result_2418": {
            "status": "GROWTH_TILT_ENGINE_VALID_UNTIL_DEPENDENCY_EVIDENCE_CLOSURE_READY",
            "recommended_next_research_task": (
                "TRADING-2419_Growth_Tilt_Engine_PIT_Gate_Readiness_Recheck"
            ),
            "source_feature_count": 10,
            "pit_gate_ready_count": 0,
            "contract_ready_count": 0,
            "pit_gate_blocked_count": 10,
            "blocked_by_source_traceability_count": 5,
            "blocked_by_valid_until_window_count": 1,
            "valid_until_dependency_evidence_ready": True,
            "valid_until_dependency_still_blocked_count": 0,
            "source_traceability_still_blocked": [
                "growth_tilt_engine_signal_artifact"
            ],
        },
        "valid_until_dependency_evidence_2418": {
            "valid_until_dependency_evidence": {
                "evidence_rows": [
                    {
                        "dependent_feature_or_signal": (
                            "execution_signal_validity_policy"
                        ),
                        "ready_for_pit_gate_recheck": True,
                    }
                ]
            }
        },
        "signal_validity_contract_evidence_2418": {
            "signal_validity_contract_evidence": {"ready_for_recheck": True}
        },
        "stale_signal_policy_evidence_2418": {
            "stale_signal_policy_evidence": {"ready_for_recheck": True}
        },
        "growth_tilt_valid_until_alignment_evidence_2418": {
            "growth_tilt_valid_until_alignment_evidence": {"ready_for_recheck": True}
        },
        "remaining_blocker_summary_2418": {
            "remaining_blocker_summary": {
                "source_traceability_still_blocked_feature_ids": [
                    "growth_tilt_engine_signal_artifact"
                ]
            }
        },
        "closure_result_2417": {
            "status": "GROWTH_TILT_ENGINE_SOURCE_TRACEABILITY_AND_UPSTREAM_ARTIFACT_CLOSURE_READY"
        },
        "remaining_blocker_summary_2417": {
            "remaining_blocker_summary": {
                "source_traceability_still_blocked_feature_ids": [
                    "growth_tilt_engine_signal_artifact"
                ]
            }
        },
        "closure_result_2416": {
            "status": "GROWTH_TILT_ENGINE_PIT_GATE_REMAINING_BLOCKER_CLOSURE_PLAN_READY"
        },
        "remaining_blocker_matrix_2416": {
            "remaining_blocker_matrix": {"matrix_rows": rows}
        },
        "pit_gate_evidence_requirements_2416": {
            "pit_gate_evidence_requirements": {"source_feature_count": 10}
        },
        "readiness_snapshot_result_2415": {
            "status": (
                "GROWTH_TILT_ENGINE_PIT_GATE_READINESS_SNAPSHOT_"
                "READY_WITH_BLOCKERS_UNRESOLVED"
            ),
            "source_feature_count": 10,
            "pit_gate_ready_count": 0,
            "contract_ready_count": 0,
            "pit_gate_blocked_count": 10,
            "blocked_by_source_traceability_count": 5,
            "blocked_by_valid_until_window_count": 1,
        },
        "readiness_matrix_2415": {
            "pit_gate_readiness_matrix": {"matrix_rows": rows}
        },
        "readiness_validation_2415": {
            "pit_gate_readiness_validation": {"valid": True}
        },
        "remaining_blocker_summary_2415": {
            "remaining_blocker_summary": {
                "source_traceability_still_blocked_feature_ids": [
                    "growth_tilt_engine_signal_artifact"
                ]
            }
        },
    }


def _readiness_rows() -> list[dict[str, object]]:
    feature_ids = [
        "adjusted_prices",
        "returns",
        "volatility_inputs",
        "trend_features",
        "drawdown_features",
        "equal_risk_baseline_weights",
        "target_vol_policy",
        "risk_on_trend_filter_context",
        "execution_signal_validity_policy",
        "growth_tilt_engine_signal_artifact",
    ]
    rows: list[dict[str, object]] = []
    for feature_id in feature_ids:
        if feature_id == "growth_tilt_engine_signal_artifact":
            status = "pit_gate_blocked_by_missing_upstream_artifact"
            reason = "upstream_artifact_or_source_snapshot_unresolved"
        elif feature_id == "execution_signal_validity_policy":
            status = "pit_gate_blocked_by_valid_until_window"
            reason = "valid_until_window_unresolved"
        else:
            status = "pit_gate_blocked_by_missing_source_traceability"
            reason = "source_traceability_unresolved"
        rows.append(
            {
                "source_feature_id": feature_id,
                "source_feature_type": "TECHNICAL_FEATURES",
                "pit_gate_status": status,
                "pit_gate_blocking_reason": reason,
                "contract_ready": False,
                "recommended_next_task": "TRADING-2419",
            }
        )
    return rows


def _pit_input_registry() -> dict[str, list[dict[str, str]]]:
    return {
        "pit_inputs": [
            {"input_id": "growth_tilt_engine", "severity": "BLOCKING"},
            {"input_id": "valid_until_window", "severity": "BLOCKING"},
        ]
    }


def _pit_input_registry_yaml() -> str:
    return """
pit_inputs:
  - input_id: growth_tilt_engine
    severity: BLOCKING
  - input_id: valid_until_window
    severity: BLOCKING
"""


def _report_registry_yaml() -> str:
    report_ids = [
        "growth_tilt_engine_pit_gate_readiness_recheck",
        "growth_tilt_engine_valid_until_dependency_evidence_closure",
        "growth_tilt_engine_source_traceability_upstream_artifact_closure",
        "growth_tilt_engine_pit_gate_remaining_blocker_closure_plan",
        "growth_tilt_engine_pit_gate_readiness_snapshot",
    ]
    rows = "\n".join(
        f"  - report_id: {report_id}\n"
        "    command: aits research strategies growth-tilt-engine-pit-gate-readiness-recheck\n"
        "    artifact_globs: []\n"
        "    artifact_selection_policy: latest_available\n"
        "    required_for_daily_reading: false\n"
        "    production_effect: none\n"
        "    broker_action: none"
        for report_id in report_ids
    )
    return f"reports:\n{rows}\n"


def _artifact_catalog_text() -> str:
    return "\n".join(
        [
            "aits research strategies growth-tilt-engine-pit-gate-readiness-recheck",
            (
                "aits research strategies "
                "growth-tilt-engine-valid-until-dependency-evidence-closure"
            ),
        ]
    )
