from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_valid_until_outcome_hit_rate_study as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_valid_until_outcome_hit_rate_study as study,
)


def test_valid_until_outcome_hit_rate_study_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == study.READY_STATUS
    assert payload["source_2434_ready"] is True
    assert payload["source_2418_valid_until_evidence_ready"] is True
    assert payload["source_2429_forward_outcome_boundary_ready"] is True
    assert payload["candidate_set_valid_until_metric_ready"] is True
    assert payload["hit_rate_study_ready"] is True
    assert payload["valid_until_component_value_found"] is True
    assert payload["valid_until_hit_rate_delta"] == 0.0
    assert payload["stale_signal_reduction"] == 0.0
    assert payload["expiry_failure_count"] == 0
    assert payload["outcome_sample_count"] == 0
    assert payload["observed_outcome_hit_rate_available"] is False
    assert payload["candidate_status"] == "component_value"
    assert payload["computed_new_metrics"] is False
    assert payload["fresh_market_data_read"] is False
    assert payload["fresh_outcome_data_read"] is False
    assert payload["outcome_binding_executed"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == study.NEXT_ROUTE


def test_valid_until_outcome_hit_rate_blocks_when_2434_not_ready() -> None:
    sources = _source_documents()
    sources["source_2434_component_validation"]["component_validation_ready"] = False

    payload = _build_payload(sources)

    assert payload["status"] == study.BLOCKED_STATUS
    assert payload["source_2434_ready"] is False
    assert payload["valid_until_component_value_found"] is False
    assert payload["candidate_status"] == "needs_pit"
    assert "source_2434_component_validation_ready" in payload["evidence_gap_ids"]
    assert payload["recommended_next_research_task"] == study.BLOCKED_ROUTE


def test_valid_until_outcome_hit_rate_blocks_missing_valid_until_metric() -> None:
    sources = _source_documents()
    sources["candidate_set_2432"]["unified_metrics"]["metrics"] = []

    payload = _build_payload(sources)

    assert payload["status"] == study.BLOCKED_STATUS
    assert payload["candidate_set_valid_until_metric_ready"] is False
    assert "candidate_set_valid_until_metric_ready" in payload["evidence_gap_ids"]
    assert payload["computed_new_metrics"] is False


def test_valid_until_outcome_hit_rate_wrapper_writes_outputs(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "valid_until_hit_rate"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_valid_until_outcome_hit_rate_study(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == study.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["valid_until_component_value_found"] is True
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "valid_until_hit_rate_matrix_json",
        "stale_signal_reduction_summary_json",
        "expiry_failure_audit_json",
        "no_effect_boundary_json",
        "markdown_path",
        "valid_until_hit_rate_matrix_markdown",
        "stale_signal_reduction_summary_markdown",
        "expiry_failure_audit_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_valid_until_outcome_hit_rate_cli_deterministic(tmp_path: Path) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "valid_until_hit_rate_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-valid-until-outcome-hit-rate-study",
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
    assert study.READY_STATUS in result.output
    assert "valid_until_component_value_found=true" in result.output
    assert "valid_until_hit_rate_delta=0.0" in result.output
    assert "stale_signal_reduction=0.0" in result.output
    assert "expiry_failure_count=0" in result.output
    assert "outcome_sample_count=0" in result.output
    assert "observed_outcome_hit_rate_available=false" in result.output
    assert "candidate_status=component_value" in result.output
    assert "computed_new_metrics=false" in result.output
    assert "fresh_market_data_read=false" in result.output
    assert "fresh_outcome_data_read=false" in result.output
    assert "outcome_binding_executed=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={study.NEXT_ROUTE}" in result.output
    assert (output_root / "hit_rate_study_result.json").exists()


def test_valid_until_outcome_hit_rate_missing_source_artifact_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["source_2434"].unlink()

    payload = impl.run_growth_tilt_valid_until_outcome_hit_rate_study(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == study.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["hit_rate_study_ready"] is False
    assert payload["evidence_gap_ids"] == ["source_artifact_availability"]
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == study.BLOCKED_ROUTE


def test_valid_until_outcome_hit_rate_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[study.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-valid-until-outcome-hit-rate-study"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("hit_rate_study_result.json" in item for item in entry["artifact_globs"])
    assert any("dynamic_strategy_2436_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert study.REPORT_TYPE in catalog
    assert "growth-tilt-valid-until-outcome-hit-rate-study" in system_flow
    assert study.READY_STATUS in system_flow
    assert study.NEXT_ROUTE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return study.build_growth_tilt_valid_until_outcome_hit_rate_study(
        sources["source_2434_component_validation"],
        sources["source_2418_valid_until_alignment"],
        sources["source_2418_stale_signal_policy"],
        sources["source_2429_forward_outcome_boundary"],
        sources["candidate_set_2432"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2434_component_validation": _source_2434(),
        "source_2418_valid_until_alignment": _source_2418_alignment(),
        "source_2418_stale_signal_policy": _source_2418_stale_policy(),
        "source_2429_forward_outcome_boundary": _source_2429_boundary(),
        "candidate_set_2432": _candidate_set_2432(),
        "report_registry": {
            "reports": [{"report_id": report_id} for report_id in study.REQUIRED_REPORT_IDS]
        },
        "artifact_catalog_text": "\n".join(study.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(study.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {
            "component_validation": "valid_until outcome component evidence",
            "alignment": "valid_until outcome alignment",
            "forward_outcome": "valid_until outcome boundary",
        },
    }


def _source_2434() -> dict[str, Any]:
    return {
        "status": study.EXPECTED_2434_STATUS,
        "component_validation_ready": True,
        "component_value_found": True,
        "recommended_next_research_task": study.EXPECTED_2434_NEXT_ROUTE,
    }


def _source_2418_alignment() -> dict[str, Any]:
    return {
        "status": study.EXPECTED_2418_STATUS,
        "growth_tilt_valid_until_alignment_evidence": {
            "ready_for_recheck": True,
            "proposed_horizon_to_valid_until_mapping": [
                {"signal_horizon_class": "medium_growth_tilt"}
            ],
        },
    }


def _source_2418_stale_policy() -> dict[str, Any]:
    return {
        "status": study.EXPECTED_2418_STATUS,
        "stale_signal_policy_evidence": {
            "ready_for_recheck": True,
            "stale_carry_forward_policy_ready": True,
            "signal_to_execution_lag_policy_ready": True,
            "replay_validation_required": True,
        },
    }


def _source_2429_boundary() -> dict[str, Any]:
    return {
        "status": study.EXPECTED_2429_STATUS,
        "forward_outcome_binding_boundary_ready": True,
        "valid_until_binding_ready": True,
        "outcome_schema_ready": True,
        "signal_to_outcome_linkage_ready": True,
    }


def _candidate_set_2432() -> dict[str, Any]:
    return {
        "candidate_set_id": study.EXPECTED_CANDIDATE_SET_ID,
        "unified_metrics": {
            "metrics": [
                {
                    "metric_id": study.VALID_UNTIL_METRIC_ID,
                    "direction": "higher_is_better",
                    "computed_in_2432": False,
                }
            ]
        },
        "candidate_groups": [
            {
                "candidate_group_id": study.VALID_UNTIL_CANDIDATE_GROUP_ID,
                "default_2431_status": "component_value",
                "included_in_2432_harness": True,
            }
        ],
    }


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2434": root / "component_validation_result.json",
        "source_2418_alignment": root / "valid_until_alignment.json",
        "source_2418_stale_policy": root / "stale_signal_policy.json",
        "source_2429_boundary": root / "forward_outcome_boundary.json",
        "candidate_set_2432": root / "candidate_set_2432.yaml",
        "component_validation_doc": root / "component_validation.md",
        "valid_until_alignment_doc": root / "valid_until_alignment.md",
        "forward_outcome_boundary_doc": root / "forward_outcome_boundary.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    _write_json(paths["source_2434"], sources["source_2434_component_validation"])
    _write_json(paths["source_2418_alignment"], sources["source_2418_valid_until_alignment"])
    _write_json(paths["source_2418_stale_policy"], sources["source_2418_stale_signal_policy"])
    _write_json(paths["source_2429_boundary"], sources["source_2429_forward_outcome_boundary"])
    paths["candidate_set_2432"].write_text(
        json.dumps(sources["candidate_set_2432"], ensure_ascii=False),
        encoding="utf-8",
    )
    paths["component_validation_doc"].write_text(
        "valid_until outcome component evidence",
        encoding="utf-8",
    )
    paths["valid_until_alignment_doc"].write_text(
        "valid_until outcome alignment",
        encoding="utf-8",
    )
    paths["forward_outcome_boundary_doc"].write_text(
        "valid_until outcome boundary",
        encoding="utf-8",
    )
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in study.REQUIRED_REPORT_IDS
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
        "source_2434_component_validation_path": paths["source_2434"],
        "source_2418_valid_until_alignment_path": paths["source_2418_alignment"],
        "source_2418_stale_signal_policy_path": paths["source_2418_stale_policy"],
        "source_2429_forward_outcome_boundary_path": paths["source_2429_boundary"],
        "candidate_set_2432_path": paths["candidate_set_2432"],
        "component_validation_doc_path": paths["component_validation_doc"],
        "valid_until_alignment_doc_path": paths["valid_until_alignment_doc"],
        "forward_outcome_boundary_doc_path": paths["forward_outcome_boundary_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2434-component-validation",
        str(paths["source_2434"]),
        "--source-2418-valid-until-alignment",
        str(paths["source_2418_alignment"]),
        "--source-2418-stale-signal-policy",
        str(paths["source_2418_stale_policy"]),
        "--source-2429-forward-outcome-boundary",
        str(paths["source_2429_boundary"]),
        "--candidate-set-2432",
        str(paths["candidate_set_2432"]),
        "--component-validation-doc",
        str(paths["component_validation_doc"]),
        "--valid-until-alignment-doc",
        str(paths["valid_until_alignment_doc"]),
        "--forward-outcome-boundary-doc",
        str(paths["forward_outcome_boundary_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
