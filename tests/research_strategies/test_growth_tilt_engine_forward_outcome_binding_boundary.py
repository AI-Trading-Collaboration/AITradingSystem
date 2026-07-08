from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_engine_forward_outcome_binding_boundary as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_engine_forward_outcome_binding_boundary as boundary,
)


def test_forward_outcome_binding_boundary_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == boundary.READY_STATUS
    assert payload["observe_only_signal_artifact_boundary_ready"] is True
    assert payload["forward_outcome_binding_boundary_ready"] is True
    assert payload["forward_outcome_binding_boundary_gap_count"] == 0
    assert payload["outcome_horizons"] == list(boundary.OUTCOME_HORIZONS)
    assert payload["outcome_schema_ready"] is True
    assert payload["valid_until_binding_ready"] is True
    assert payload["baseline_comparison_ready"] is True
    assert payload["generated_signal"] is False
    assert payload["outcome_backfilled"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == boundary.NEXT_ROUTE_READY


def test_forward_outcome_binding_boundary_blocks_prior_not_ready() -> None:
    sources = _source_documents()
    sources["observe_only_boundary_result_2428"][
        "observe_only_signal_artifact_boundary_ready"
    ] = False

    payload = _build_payload(sources)

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert payload["forward_outcome_binding_boundary_ready"] is False
    assert "prior_observe_only_boundary_ready" in (
        payload["forward_outcome_binding_boundary_gap_ids"]
    )
    assert payload["recommended_next_research_task"] == boundary.NEXT_ROUTE_BLOCKED


def test_forward_outcome_binding_boundary_blocks_horizon_gap() -> None:
    horizon_rules = _outcome_horizon_rules()
    horizon_rules["outcome_horizons"] = ["1d", "5d"]

    payload = _build_payload(_source_documents(), outcome_horizon_rules=horizon_rules)

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert "outcome_horizon_rules_ready" in (
        payload["forward_outcome_binding_boundary_gap_ids"]
    )
    assert payload["outcome_horizon_rules_ready"] is False


def test_forward_outcome_binding_boundary_blocks_valid_until_binding_gap() -> None:
    rules = _valid_until_binding_rules()
    rules["require_no_future_data_at_decision_time"] = False

    payload = _build_payload(_source_documents(), valid_until_binding_rules=rules)

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert "valid_until_binding_rules_ready" in (
        payload["forward_outcome_binding_boundary_gap_ids"]
    )
    assert payload["valid_until_binding_ready"] is False


def test_forward_outcome_binding_boundary_blocks_outcome_schema_gap() -> None:
    schema = _outcome_artifact_schema()
    schema["required_fields"] = [
        field
        for field in boundary.REQUIRED_OUTCOME_SCHEMA_FIELDS
        if field != "source_signal_artifact_checksum"
    ]

    payload = _build_payload(_source_documents(), outcome_artifact_schema=schema)

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert "outcome_artifact_schema_ready" in (
        payload["forward_outcome_binding_boundary_gap_ids"]
    )
    assert payload["outcome_schema_ready"] is False


def test_forward_outcome_binding_boundary_blocks_no_effect_gap() -> None:
    no_effect = _no_effect_boundary()
    no_effect["outcome_backfilled"] = True

    payload = _build_payload(_source_documents(), no_effect_boundary=no_effect)

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert "no_effect_boundary_ready" in (
        payload["forward_outcome_binding_boundary_gap_ids"]
    )
    assert payload["no_effect_boundary_ready"] is False


def test_forward_outcome_binding_boundary_wrapper_writes_outputs(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "forward_outcome_boundary"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_engine_forward_outcome_binding_boundary(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == boundary.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["forward_outcome_binding_boundary_ready"] is True
    assert payload["forward_outcome_binding_boundary_gap_count"] == 0
    assert payload["outcome_backfilled"] is False
    assert payload["generated_signal"] is False

    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "outcome_horizon_rules_json",
        "valid_until_binding_rules_json",
        "outcome_decision_rules_json",
        "baseline_comparison_rules_json",
        "outcome_artifact_schema_json",
        "signal_to_outcome_linkage_json",
        "no_effect_boundary_json",
        "markdown_path",
        "outcome_horizon_rules_markdown",
        "valid_until_binding_rules_markdown",
        "outcome_decision_rules_markdown",
        "baseline_comparison_rules_markdown",
        "outcome_artifact_schema_markdown",
        "signal_to_outcome_linkage_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_forward_outcome_binding_boundary_cli_deterministic(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "forward_outcome_boundary_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-engine-forward-outcome-binding-boundary",
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
    assert "observe_only_signal_artifact_boundary_ready=true" in result.output
    assert "forward_outcome_binding_boundary_ready=true" in result.output
    assert "outcome_schema_ready=true" in result.output
    assert "valid_until_binding_ready=true" in result.output
    assert "baseline_comparison_ready=true" in result.output
    assert "generated_signal=false" in result.output
    assert "outcome_backfilled=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={boundary.NEXT_ROUTE_READY}" in result.output
    assert (output_root / "forward_outcome_binding_boundary_result.json").exists()


def test_forward_outcome_binding_boundary_missing_prior_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["observe_only_boundary_result_2428"].unlink()

    payload = impl.run_growth_tilt_engine_forward_outcome_binding_boundary(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == boundary.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["forward_outcome_binding_boundary_started"] is False
    assert payload["forward_outcome_binding_boundary_ready"] is False
    assert payload["forward_outcome_binding_boundary_gap_count"] == 1
    assert payload["missing_binding_boundary_evidence_count"] == 1
    assert payload["outcome_backfilled"] is False
    assert payload["recommended_next_research_task"] == boundary.NEXT_ROUTE_BLOCKED


def test_forward_outcome_binding_boundary_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[boundary.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies "
        "growth-tilt-engine-forward-outcome-binding-boundary"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "forward_outcome_binding_boundary_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2430_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert boundary.REPORT_TYPE in catalog
    assert "growth-tilt-engine-forward-outcome-binding-boundary" in system_flow
    assert boundary.READY_STATUS in system_flow
    assert boundary.NEXT_ROUTE_READY in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(
    sources: dict[str, Any],
    **overrides: Mapping[str, Any],
) -> dict[str, Any]:
    return boundary.build_growth_tilt_engine_forward_outcome_binding_boundary(
        sources["observe_only_boundary_result_2428"],
        sources["signal_artifact_schema_2428"],
        sources["valid_until_requirements_2428"],
        sources["source_traceability_requirements_2428"],
        sources["pit_contract_manual_review_requirements_2428"],
        sources["no_trading_advice_boundary_2428"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
        **overrides,
    )


def _source_documents() -> dict[str, Any]:
    return {
        "observe_only_boundary_result_2428": _observe_only_boundary_result(),
        "signal_artifact_schema_2428": {"signal_artifact_schema": _signal_schema()},
        "valid_until_requirements_2428": {
            "valid_until_requirements": _valid_until_requirements()
        },
        "source_traceability_requirements_2428": {
            "source_traceability_requirements": _source_traceability_requirements()
        },
        "pit_contract_manual_review_requirements_2428": {
            "pit_contract_manual_review_requirements": _pit_contract_requirements()
        },
        "no_trading_advice_boundary_2428": {
            "no_trading_advice_boundary": _no_trading_advice_boundary()
        },
        "report_registry": {
            "reports": [
                {"report_id": boundary.REPORT_TYPE},
                {"report_id": "growth_tilt_engine_observe_only_signal_artifact_boundary"},
            ]
        },
        "artifact_catalog_text": "\n".join(boundary.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(boundary.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "source_2428_route_doc": (
                f"{boundary.EXPECTED_PRIOR_NEXT_ROUTE}\n"
                f"{boundary.OBSERVE_ONLY_BOUNDARY_READY_STATUS}\n"
            )
        },
    }


def _observe_only_boundary_result() -> dict[str, Any]:
    return {
        "status": boundary.OBSERVE_ONLY_BOUNDARY_READY_STATUS,
        "readiness_status": boundary.OBSERVE_ONLY_BOUNDARY_READY_STATUS,
        "pit_gate_ready": True,
        "pit_gate_ready_count": 1,
        "contract_ready": True,
        "contract_ready_count": 1,
        "observe_only_signal_artifact_boundary_ready": True,
        "recommended_next_research_task": boundary.EXPECTED_PRIOR_NEXT_ROUTE,
        "generated_signal": False,
        "generated_trading_advice": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _signal_schema() -> dict[str, Any]:
    return {
        "schema_version": "growth_tilt_engine_observe_only_signal_artifact_schema.v1",
        "signal_artifact_schema_ready": True,
        "required_fields": list(boundary.REQUIRED_PRIOR_SIGNAL_FIELDS),
        "generated_signal": False,
        "signal_artifact_instance_generated": False,
    }


def _valid_until_requirements() -> dict[str, Any]:
    return {
        "schema_version": (
            "growth_tilt_engine_observe_only_signal_valid_until_requirements.v1"
        ),
        "valid_until_required": True,
        "valid_until_requirements_ready": True,
    }


def _source_traceability_requirements() -> dict[str, Any]:
    return {
        "schema_version": (
            "growth_tilt_engine_observe_only_signal_source_traceability_requirements.v1"
        ),
        "source_traceability_required": True,
        "source_traceability_requirements_ready": True,
    }


def _pit_contract_requirements() -> dict[str, Any]:
    return {
        "schema_version": (
            "growth_tilt_engine_observe_only_signal_"
            "pit_contract_manual_review_requirements.v1"
        ),
        "pit_contract_manual_review_requirements_ready": True,
        "manual_review_required": True,
        "pit_fields_required": True,
        "contract_fields_required": True,
    }


def _no_trading_advice_boundary() -> dict[str, Any]:
    return {
        "schema_version": (
            "growth_tilt_engine_observe_only_signal_no_trading_advice_boundary.v1"
        ),
        "no_trading_advice_boundary_ready": True,
        "generated_signal": False,
        "generated_trading_advice": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
    }


def _outcome_horizon_rules() -> dict[str, Any]:
    return {
        "schema_version": boundary.OUTCOME_HORIZON_RULES_SCHEMA_VERSION,
        "outcome_horizon_rules_ready": True,
        "outcome_horizons": list(boundary.OUTCOME_HORIZONS),
        "horizon_unit": "us_trading_day",
    }


def _valid_until_binding_rules() -> dict[str, Any]:
    return {
        "schema_version": boundary.VALID_UNTIL_BINDING_RULES_SCHEMA_VERSION,
        "valid_until_binding_ready": True,
        "bind_only_after_valid_until": True,
        "require_outcome_window_closed": True,
        "require_no_future_data_at_decision_time": True,
        "require_source_traceability_preserved": True,
        "require_data_quality_gate_for_future_binding": True,
        "backfill_real_outcome_now": False,
    }


def _outcome_artifact_schema() -> dict[str, Any]:
    return {
        "schema_version": boundary.OUTCOME_ARTIFACT_SCHEMA_VERSION,
        "outcome_schema_ready": True,
        "required_fields": list(boundary.REQUIRED_OUTCOME_SCHEMA_FIELDS),
        "allowed_outcome_horizons": list(boundary.OUTCOME_HORIZONS),
        "allowed_classifications": list(boundary.OUTCOME_CLASSIFICATIONS),
        "generated_signal": False,
        "generated_trading_advice": False,
    }


def _no_effect_boundary() -> dict[str, Any]:
    return {
        "schema_version": boundary.NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "no_effect_boundary_ready": True,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "fresh_market_data_read": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "automatic_execution_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "observe_only_boundary_result_2428": (
            root / "observe_only_signal_artifact_boundary_result.json"
        ),
        "signal_artifact_schema_2428": root / "signal_artifact_schema.json",
        "valid_until_requirements_2428": root / "valid_until_requirements.json",
        "source_traceability_requirements_2428": (
            root / "source_traceability_requirements.json"
        ),
        "pit_contract_manual_review_requirements_2428": (
            root / "pit_contract_manual_review_requirements.json"
        ),
        "no_trading_advice_boundary_2428": root / "no_trading_advice_boundary.json",
        "source_2428_research_doc": root / "source_2428_research_doc.md",
        "source_2428_schema_doc": root / "source_2428_schema_doc.md",
        "source_2428_valid_until_doc": root / "source_2428_valid_until_doc.md",
        "source_2428_traceability_doc": root / "source_2428_traceability_doc.md",
        "source_2428_no_advice_doc": root / "source_2428_no_advice_doc.md",
        "source_2428_route_doc": root / "dynamic_strategy_2429_route.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    _write_json(paths["observe_only_boundary_result_2428"], _observe_only_boundary_result())
    _write_json(paths["signal_artifact_schema_2428"], sources["signal_artifact_schema_2428"])
    _write_json(
        paths["valid_until_requirements_2428"],
        sources["valid_until_requirements_2428"],
    )
    _write_json(
        paths["source_traceability_requirements_2428"],
        sources["source_traceability_requirements_2428"],
    )
    _write_json(
        paths["pit_contract_manual_review_requirements_2428"],
        sources["pit_contract_manual_review_requirements_2428"],
    )
    _write_json(
        paths["no_trading_advice_boundary_2428"],
        sources["no_trading_advice_boundary_2428"],
    )
    doc_text = (
        f"{boundary.EXPECTED_PRIOR_NEXT_ROUTE}\n"
        f"{boundary.OBSERVE_ONLY_BOUNDARY_READY_STATUS}\n"
    )
    for key in (
        "source_2428_research_doc",
        "source_2428_schema_doc",
        "source_2428_valid_until_doc",
        "source_2428_traceability_doc",
        "source_2428_no_advice_doc",
        "source_2428_route_doc",
    ):
        paths[key].write_text(doc_text, encoding="utf-8")
    paths["report_registry"].write_text(
        "reports:\n"
        f"  - report_id: {boundary.REPORT_TYPE}\n"
        "  - report_id: growth_tilt_engine_observe_only_signal_artifact_boundary\n",
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
        "source_2428_observe_only_boundary_result_path": (
            paths["observe_only_boundary_result_2428"]
        ),
        "source_2428_signal_artifact_schema_path": (
            paths["signal_artifact_schema_2428"]
        ),
        "source_2428_valid_until_requirements_path": (
            paths["valid_until_requirements_2428"]
        ),
        "source_2428_source_traceability_requirements_path": (
            paths["source_traceability_requirements_2428"]
        ),
        "source_2428_pit_contract_manual_review_requirements_path": (
            paths["pit_contract_manual_review_requirements_2428"]
        ),
        "source_2428_no_trading_advice_boundary_path": (
            paths["no_trading_advice_boundary_2428"]
        ),
        "source_2428_research_doc_path": paths["source_2428_research_doc"],
        "source_2428_schema_doc_path": paths["source_2428_schema_doc"],
        "source_2428_valid_until_doc_path": paths["source_2428_valid_until_doc"],
        "source_2428_traceability_doc_path": paths["source_2428_traceability_doc"],
        "source_2428_no_advice_doc_path": paths["source_2428_no_advice_doc"],
        "source_2428_route_doc_path": paths["source_2428_route_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2428-observe-only-boundary-result",
        str(paths["observe_only_boundary_result_2428"]),
        "--source-2428-signal-artifact-schema",
        str(paths["signal_artifact_schema_2428"]),
        "--source-2428-valid-until-requirements",
        str(paths["valid_until_requirements_2428"]),
        "--source-2428-source-traceability-requirements",
        str(paths["source_traceability_requirements_2428"]),
        "--source-2428-pit-contract-manual-review-requirements",
        str(paths["pit_contract_manual_review_requirements_2428"]),
        "--source-2428-no-trading-advice-boundary",
        str(paths["no_trading_advice_boundary_2428"]),
        "--source-2428-research-doc",
        str(paths["source_2428_research_doc"]),
        "--source-2428-schema-doc",
        str(paths["source_2428_schema_doc"]),
        "--source-2428-valid-until-doc",
        str(paths["source_2428_valid_until_doc"]),
        "--source-2428-traceability-doc",
        str(paths["source_2428_traceability_doc"]),
        "--source-2428-no-advice-doc",
        str(paths["source_2428_no_advice_doc"]),
        "--source-2428-route-doc",
        str(paths["source_2428_route_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
