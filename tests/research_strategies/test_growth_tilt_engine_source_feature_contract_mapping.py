from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_growth_tilt_engine_pit_signal_remediation_plan as m2406
import ai_trading_system.dynamic_strategy_growth_tilt_engine_source_feature_contract_mapping as impl
import ai_trading_system.dynamic_strategy_pit_coverage_matrix_reusable_implementation as m2405
import ai_trading_system.dynamic_strategy_signal_as_of_validity_contract_schema as m2409
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality.signal_as_of_contract import (
    build_signal_as_of_contract_schema,
)
from ai_trading_system.research_quality.signal_contract_schema_snapshot import (
    build_signal_contract_schema_snapshot,
)
from ai_trading_system.research_quality.signal_validity_contract import (
    build_signal_validity_contract_schema,
)
from ai_trading_system.research_quality.source_feature_traceability_contract import (
    build_source_feature_traceability_contract_schema,
)


def test_growth_tilt_engine_source_feature_contract_mapping_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "mapping"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_source_feature_contract_mapping(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == impl.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_tasks"] == ["TRADING-2405", "TRADING-2406", "TRADING-2409"]
    assert payload["blocker_under_review"] == "growth_tilt_engine"
    assert payload["known_source_feature_count"] == 4
    assert payload["source_feature_contract_mapping_ready"] is True
    assert payload["contract_mapping_validation_ready"] is True
    assert payload["unresolved_gap_summary_ready"] is True
    assert payload["unclassified_feature_count"] == 0
    assert payload["contract_ready_count"] == 1
    assert payload["blocked_or_gap_count"] == 3
    assert payload["route_to_next_task"] == impl.NEXT_ROUTE
    assert payload["recommended_next_research_task"] == impl.NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    statuses = {
        row["feature_id"]: row["mapping_status"]
        for row in payload["source_feature_contract_mapping"]["mapping_rows"]
    }
    assert statuses == {
        "ready_price_feature": "mapped_contract_ready",
        "missing_as_of_feature": "missing_as_of_semantics",
        "ambiguous_signal_artifact": "ambiguous_source_feature",
        "blocked_validity_policy": "blocked_unresolved",
    }
    gap_summary = payload["unresolved_gap_summary"]
    assert gap_summary["growth_tilt_engine_blocking_gap_resolved"] is False
    assert gap_summary["growth_tilt_engine_severity_downgraded"] is False
    assert gap_summary["candidate_search_enabled"] is False
    assert gap_summary["production_enabled"] is False

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False

    for key in (
        "json_path",
        "source_feature_contract_mapping_json",
        "contract_mapping_validation_json",
        "unresolved_gap_summary_json",
        "markdown_path",
        "validation_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_growth_tilt_engine_source_feature_contract_mapping_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "mapping_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-source-feature-contract-mapping",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-08",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert impl.READY_STATUS in result.output
    assert "blockers_resolved=False" in result.output
    assert "blockers_downgraded=False" in result.output
    assert "candidate_search_enabled=False" in result.output
    assert "paper_shadow_enabled=False" in result.output
    assert "production_enabled=False" in result.output
    assert "broker_enabled=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "mapping_result.json").exists()
    assert (output_root / "source_feature_contract_mapping.json").exists()
    assert (output_root / "contract_mapping_validation.json").exists()
    assert (output_root / "unresolved_gap_summary.json").exists()


def test_growth_tilt_engine_source_feature_contract_mapping_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["growth_tilt_engine_source_feature_contract_mapping"]

    assert entry["command"] == (
        "aits research strategies growth-tilt-engine-source-feature-contract-mapping"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("mapping_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2411_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "growth_tilt_engine_source_feature_contract_mapping" in catalog
    assert "growth-tilt-engine-source-feature-contract-mapping" in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "contract_schema_result_2409": root / "contract_schema_result_2409.json",
        "source_feature_contract_schema_2409": root / "source_feature_schema_2409.json",
        "signal_as_of_contract_schema_2409": root / "signal_as_of_schema_2409.json",
        "signal_validity_contract_schema_2409": root / "signal_validity_schema_2409.json",
        "contract_schema_snapshot_2409": root / "contract_schema_snapshot_2409.json",
        "source_feature_inventory_2406": root / "source_feature_inventory_2406.json",
        "pit_risk_audit_2406": root / "pit_risk_audit_2406.json",
        "signal_construction_gap_analysis_2406": root / "signal_gap_2406.json",
        "remediation_plan_2406": root / "remediation_plan_2406.json",
        "pit_gate_result_2405": root / "pit_gate_result_2405.json",
        "blocker_summary_2405": root / "blocker_summary_2405.json",
        "pit_input_registry_config": root / "pit_input_registry.yaml",
        "growth_tilt_candidate_registry_config": root / "growth_tilt_registry.yaml",
    }
    as_of_schema = build_signal_as_of_contract_schema()
    feature_schema = build_source_feature_traceability_contract_schema()
    validity_schema = build_signal_validity_contract_schema()
    snapshot = build_signal_contract_schema_snapshot()
    _write_json(
        paths["contract_schema_result_2409"],
        {
            **_safe_2409_doc(),
            "route_to_next_task": m2409.NEXT_ROUTE,
            "recommended_next_research_task": m2409.NEXT_ROUTE,
            "source_feature_traceability_contract_schema": feature_schema,
        },
    )
    _write_json(
        paths["source_feature_contract_schema_2409"],
        {
            "status": m2409.READY_STATUS,
            "source_feature_traceability_contract_schema": feature_schema,
        },
    )
    _write_json(
        paths["signal_as_of_contract_schema_2409"],
        {"status": m2409.READY_STATUS, "signal_as_of_contract_schema": as_of_schema},
    )
    _write_json(
        paths["signal_validity_contract_schema_2409"],
        {"status": m2409.READY_STATUS, "signal_validity_contract_schema": validity_schema},
    )
    _write_json(
        paths["contract_schema_snapshot_2409"],
        {"status": m2409.READY_STATUS, "contract_schema_snapshot": snapshot},
    )
    _write_json(
        paths["source_feature_inventory_2406"],
        {
            "status": m2406.READY_STATUS,
            "source_feature_inventory": _source_feature_inventory(),
        },
    )
    _write_json(
        paths["pit_risk_audit_2406"],
        {"status": m2406.READY_STATUS, "pit_risk_audit": {"blocking_risk_count": 1}},
    )
    _write_json(
        paths["signal_construction_gap_analysis_2406"],
        {
            "status": m2406.READY_STATUS,
            "signal_construction_gap_analysis": {"signal_id": "growth_tilt_engine"},
        },
    )
    _write_json(
        paths["remediation_plan_2406"],
        {
            **_safe_2406_doc(),
            "recommended_next_research_task": m2406.NEXT_ROUTE,
            "source_feature_inventory": _source_feature_inventory(),
        },
    )
    _write_json(
        paths["pit_gate_result_2405"],
        {
            "status": m2405.READY_STATUS,
            "pit_gate_result": {
                "candidate_search_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "blockers": [
                    "BLOCKING_GAP_GROWTH_TILT_ENGINE",
                    "BLOCKING_GAP_VALID_UNTIL_WINDOW",
                ],
            },
        },
    )
    _write_json(
        paths["blocker_summary_2405"],
        {
            "status": m2405.READY_STATUS,
            "pit_blocker_summary": {
                "blocking_gaps": ["growth_tilt_engine", "valid_until_window"],
            },
        },
    )
    paths["pit_input_registry_config"].write_text(
        """
schema_version: dynamic_strategy_pit_input_registry.v1
entries:
  - input_id: growth_tilt_engine
    input_type: SIGNAL
    severity: BLOCKING
    candidate_search_blocker: true
    pit_status: UNKNOWN
    pit_confidence: LOW
""",
        encoding="utf-8",
    )
    paths["growth_tilt_candidate_registry_config"].write_text(
        """
schema_version: test
research_policy:
  required_price_tickers:
    - QQQ
    - TQQQ
    - SGOV
""",
        encoding="utf-8",
    )
    return paths


def _source_feature_inventory() -> list[dict[str, object]]:
    return [
        _source_feature("ready_price_feature", pit_status="TRUE_PIT", pit_confidence="HIGH"),
        _source_feature(
            "missing_as_of_feature",
            as_of_handling="missing source cutoff and signal-time observation",
            recommended_action="missing as-of semantics must be remediated",
        ),
        _source_feature(
            "ambiguous_signal_artifact",
            feature_type="SIGNAL_ARTIFACT_CONTRACT",
            source_config_or_artifact="TBD signal artifact registry",
        ),
        _source_feature(
            "blocked_validity_policy",
            feature_type="VALIDITY_POLICY",
            pit_status="UNKNOWN_OR_APPROXIMATE_PIT",
            pit_confidence="LOW",
            severity="BLOCKING",
            recommended_action="valid_until dependency remains unresolved",
        ),
    ]


def _source_feature(
    feature_id: str,
    *,
    feature_type: str = "TECHNICAL_FEATURES",
    source_config_or_artifact: str = "config/research/test_feature.yaml",
    as_of_handling: str = "explicit as-of timestamp at signal construction",
    generated_at_handling: str = "explicit generated_at from pipeline run",
    pit_status: str = "APPROXIMATE_PIT",
    pit_confidence: str = "MEDIUM",
    severity: str = "MATERIAL",
    recommended_action: str = "record source snapshot before promotion review",
) -> dict[str, object]:
    return {
        "feature_id": feature_id,
        "feature_type": feature_type,
        "source_config_or_artifact": source_config_or_artifact,
        "as_of_handling": as_of_handling,
        "generated_at_handling": generated_at_handling,
        "lookback_window": 63,
        "forward_window_used": "none",
        "pit_status": pit_status,
        "pit_confidence": pit_confidence,
        "revision_or_backfill_risk": "LOW",
        "severity": severity,
        "recommended_action": recommended_action,
        "used_by_growth_tilt_engine": True,
    }


def _safe_2409_doc() -> dict[str, object]:
    return {
        "task_id": m2409.TASK_ID,
        "status": m2409.READY_STATUS,
        **{field: False for field in m2409.SAFETY_FALSE_FIELDS},
        "production_effect": "none",
        "broker_action": "none",
    }


def _safe_2406_doc() -> dict[str, object]:
    return {
        "task_id": m2406.TASK_ID,
        "status": m2406.READY_STATUS,
        **{field: False for field in m2406.SAFETY_FALSE_FIELDS},
        "production_effect": "none",
        "broker_action": "none",
    }


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_2409_contract_schema_result_path": paths["contract_schema_result_2409"],
        "source_2409_source_feature_contract_schema_path": (
            paths["source_feature_contract_schema_2409"]
        ),
        "source_2409_signal_as_of_contract_schema_path": (
            paths["signal_as_of_contract_schema_2409"]
        ),
        "source_2409_signal_validity_contract_schema_path": (
            paths["signal_validity_contract_schema_2409"]
        ),
        "source_2409_contract_schema_snapshot_path": paths["contract_schema_snapshot_2409"],
        "source_2406_source_feature_inventory_path": paths["source_feature_inventory_2406"],
        "source_2406_pit_risk_audit_path": paths["pit_risk_audit_2406"],
        "source_2406_signal_construction_gap_analysis_path": (
            paths["signal_construction_gap_analysis_2406"]
        ),
        "source_2406_remediation_plan_path": paths["remediation_plan_2406"],
        "source_2405_pit_gate_result_path": paths["pit_gate_result_2405"],
        "source_2405_blocker_summary_path": paths["blocker_summary_2405"],
        "pit_input_registry_path": paths["pit_input_registry_config"],
        "growth_tilt_candidate_registry_path": (
            paths["growth_tilt_candidate_registry_config"]
        ),
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "contract_schema_result_2409": "--source-2409-contract-schema-result",
        "source_feature_contract_schema_2409": (
            "--source-2409-source-feature-contract-schema"
        ),
        "signal_as_of_contract_schema_2409": "--source-2409-signal-as-of-contract-schema",
        "signal_validity_contract_schema_2409": (
            "--source-2409-signal-validity-contract-schema"
        ),
        "contract_schema_snapshot_2409": "--source-2409-contract-schema-snapshot",
        "source_feature_inventory_2406": "--source-2406-source-feature-inventory",
        "pit_risk_audit_2406": "--source-2406-pit-risk-audit",
        "signal_construction_gap_analysis_2406": (
            "--source-2406-signal-construction-gap-analysis"
        ),
        "remediation_plan_2406": "--source-2406-remediation-plan",
        "pit_gate_result_2405": "--source-2405-pit-gate-result",
        "blocker_summary_2405": "--source-2405-blocker-summary",
        "pit_input_registry_config": "--pit-input-registry",
        "growth_tilt_candidate_registry_config": "--growth-tilt-candidate-registry",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
