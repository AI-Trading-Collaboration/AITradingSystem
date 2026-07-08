from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system import (
    dynamic_strategy_growth_tilt_existing_candidate_evidence_matrix as impl,
)
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.research_quality import (
    growth_tilt_existing_candidate_evidence_matrix as matrix,
)


def test_existing_candidate_evidence_matrix_builder_ready() -> None:
    payload = _build_payload(_source_documents())

    assert payload["status"] == matrix.READY_STATUS
    assert payload["existing_candidate_evidence_matrix_ready"] is True
    assert payload["candidate_count"] == 6
    assert payload["component_value_count"] == 4
    assert payload["needs_pit_count"] == 2
    assert payload["promotion_candidate_count"] == 0
    assert payload["promotion_candidate_found"] is False
    assert payload["metric_coverage_partial_count"] == 6
    assert payload["market_data_experiment_run"] is False
    assert payload["historical_screen_run"] is False
    assert payload["pit_replay_run"] is False
    assert payload["candidate_gauntlet_run"] is False
    assert payload["paper_shadow_enabled"] is False
    assert payload["production_enabled"] is False
    assert payload["broker_enabled"] is False
    assert payload["recommended_next_research_task"] == matrix.NEXT_ROUTE


def test_existing_candidate_evidence_matrix_blocks_when_2430_not_ready() -> None:
    sources = _source_documents()
    sources["source_2430_promotion_review"]["promotion_candidate_found"] = True

    payload = _build_payload(sources)

    assert payload["status"] == matrix.BLOCKED_STATUS
    assert payload["source_2430_ready"] is False
    assert "source_2430_no_promotion_review_ready" in payload["evidence_gap_ids"]
    assert payload["recommended_next_research_task"] == matrix.BLOCKED_ROUTE


def test_existing_candidate_evidence_matrix_requires_component_value_source() -> None:
    sources = _source_documents()
    sources["prior_component_value_matrix"]["component_value_candidates"] = []

    payload = _build_payload(sources)

    assert payload["status"] == matrix.BLOCKED_STATUS
    assert payload["component_value_evidence_ready"] is False
    assert "component_value_evidence_ready" in payload["evidence_gap_ids"]


def test_existing_candidate_evidence_matrix_wrapper_writes_outputs(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "existing_matrix"
    docs_root = tmp_path / "docs" / "research"

    payload = impl.run_growth_tilt_existing_candidate_evidence_matrix(
        **_source_kwargs(paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == matrix.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == impl.DATA_QUALITY_GATE_REASON
    assert payload["candidate_count"] == 6
    assert payload["promotion_candidate_count"] == 0
    for field in impl.SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    for key in (
        "json_path",
        "candidate_evidence_matrix_json",
        "candidate_status_summary_json",
        "candidate_metric_coverage_json",
        "no_effect_boundary_json",
        "markdown_path",
        "candidate_evidence_matrix_markdown",
        "candidate_status_summary_markdown",
        "candidate_metric_coverage_markdown",
        "no_effect_boundary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_existing_candidate_evidence_matrix_cli_deterministic(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    output_root = tmp_path / "outputs" / "existing_matrix_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "growth-tilt-existing-candidate-evidence-matrix",
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
    assert matrix.READY_STATUS in result.output
    assert "existing_candidate_evidence_matrix_ready=true" in result.output
    assert "candidate_count=6" in result.output
    assert "component_value_count=4" in result.output
    assert "needs_pit_count=2" in result.output
    assert "promotion_candidate_count=0" in result.output
    assert "promotion_candidate_found=false" in result.output
    assert "market_data_experiment_run=false" in result.output
    assert "candidate_gauntlet_run=false" in result.output
    assert "paper_shadow_enabled=false" in result.output
    assert "production_enabled=false" in result.output
    assert "broker_enabled=false" in result.output
    assert f"next_route={matrix.NEXT_ROUTE}" in result.output
    assert (output_root / "existing_candidate_evidence_matrix_result.json").exists()


def test_existing_candidate_evidence_matrix_missing_prior_blocks(
    tmp_path: Path,
) -> None:
    paths = _write_sources(tmp_path)
    paths["prior_component_value_matrix"].unlink()

    payload = impl.run_growth_tilt_existing_candidate_evidence_matrix(
        **_source_kwargs(paths),
        output_root=tmp_path / "outputs" / "blocked",
        docs_root=tmp_path / "docs" / "research",
        as_of_date=date(2026, 7, 8),
    )

    assert payload["status"] == matrix.BLOCKED_STATUS
    assert payload["source_validation_error_count"] > 0
    assert payload["existing_candidate_evidence_matrix_ready"] is False
    assert payload["evidence_gap_count"] == 1
    assert payload["paper_shadow_enabled"] is False
    assert payload["recommended_next_research_task"] == matrix.BLOCKED_ROUTE


def test_existing_candidate_evidence_matrix_registry_catalog_system_flow() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries[matrix.REPORT_TYPE]

    assert entry["command"] == (
        "aits research strategies growth-tilt-existing-candidate-evidence-matrix"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "existing_candidate_evidence_matrix_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any("dynamic_strategy_2432_route.md" in item for item in entry["artifact_globs"])

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert matrix.REPORT_TYPE in catalog
    assert "growth-tilt-existing-candidate-evidence-matrix" in system_flow
    assert matrix.READY_STATUS in system_flow
    assert matrix.NEXT_ROUTE in system_flow
    assert impl.TASK_REGISTER_ID in task_register


def _build_payload(sources: dict[str, Any]) -> dict[str, Any]:
    return matrix.build_growth_tilt_existing_candidate_evidence_matrix(
        sources["source_2430_promotion_review"],
        sources["candidate_registry"],
        sources["prior_candidate_evidence"],
        sources["prior_component_value_matrix"],
        report_registry=sources["report_registry"],
        artifact_catalog_text=sources["artifact_catalog_text"],
        system_flow_text=sources["system_flow_text"],
        research_doc_texts=sources["research_doc_texts"],
    )


def _source_documents() -> dict[str, Any]:
    return {
        "source_2430_promotion_review": _source_2430_promotion_review(),
        "candidate_registry": _candidate_registry(),
        "prior_candidate_evidence": _prior_candidate_evidence(),
        "prior_component_value_matrix": _component_value_matrix(),
        "report_registry": {
            "reports": [{"report_id": report_id} for report_id in matrix.REQUIRED_REPORT_IDS]
        },
        "artifact_catalog_text": "\n".join(matrix.REQUIRED_CATALOG_REFERENCES),
        "system_flow_text": "\n".join(matrix.REQUIRED_SYSTEM_FLOW_REFERENCES),
        "research_doc_texts": {"combined": _research_doc_text()},
    }


def _source_2430_promotion_review() -> dict[str, Any]:
    return {
        "status": matrix.EXPECTED_2430_STATUS,
        "promotion_evidence_review_ready": True,
        "promotion_candidate_found": False,
        "promotion_candidate_count": 0,
        "recommended_next_research_task": matrix.EXPECTED_2430_NEXT_ROUTE,
        "candidate_evidence_matrix": {
            "candidates": [
                {
                    "candidate_id": "equal_risk_growth_tilt_vol_target_v1",
                    "paper_shadow_promotion_candidate": False,
                }
            ]
        },
    }


def _candidate_registry() -> dict[str, Any]:
    return {
        "policy_id": "equal_risk_growth_tilt_candidate_registry_v1",
        "safety_boundary": {
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "research_only": True,
            "observe_only": True,
        },
        "candidate_families": [
            {
                "strategy_id": "equal_risk_growth_tilt_vol_target_v1",
                "candidate_family": "vol_target_growth_tilt",
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            }
        ],
    }


def _prior_candidate_evidence() -> dict[str, Any]:
    return {
        "current_best_candidate": "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
        "owner_decision": (
            "DO_NOT_APPROVE_OBSERVATION_KEEP_OWNER_REVIEW_REQUIRED_AND_CONTINUE_"
            "COMPONENT_ATTRIBUTION"
        ),
        "component_value_candidates": [
            "dynamic_turnover_budgeted_growth_tilt_v1",
            "dynamic_valid_until_expiry_strict_v1",
        ],
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action": "none",
        "candidate_owner_review_record": {
            "supporting_metrics": {
                "dynamic_vs_static_gap": 0.021302,
                "turnover": 1.964574,
                "valid_until_window_preserved": True,
                "regime_slice_pass_rate": 0.0,
            },
            "failure_metrics": {
                "drawdown_gap_vs_static": 0.043574,
                "drawdown_not_materially_worse": False,
            },
        },
    }


def _component_value_matrix() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_component_value_matrix.v1",
        "component_value_candidates": [
            "dynamic_turnover_budgeted_growth_tilt_v1",
            "dynamic_valid_until_expiry_strict_v1",
        ],
        "components": [
            {
                "candidate_id": "dynamic_turnover_budgeted_growth_tilt_v1",
                "component_value": True,
            },
            {
                "candidate_id": "dynamic_valid_until_expiry_strict_v1",
                "component_value": True,
            },
        ],
    }


def _research_doc_text() -> str:
    return "\n".join(
        [
            "defensive_limited_adjustment",
            "dynamic_regime_overlay_v0_4_lower_turnover",
            "dynamic_regime_growth_tilt_lower_turnover_fusion_v1",
            "equal_risk_growth_tilt_lower_turnover_guarded_v1",
            "growth_tilt_lower_turnover_guarded_transfer_v1",
            "dynamic_valid_until_expiry_strict_v1",
            "dynamic_turnover_budgeted_growth_tilt_v1",
            "equal_risk_growth_tilt_vol_target_v1",
            "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1",
            "growth_tilt_engine_signal",
            "growth_tilt_engine_signal_artifact",
        ]
    )


def _write_sources(tmp_path: Path) -> dict[str, Path]:
    sources = _source_documents()
    root = tmp_path / "sources"
    root.mkdir(parents=True)
    paths = {
        "source_2430_promotion_review": root / "promotion_review.json",
        "candidate_registry": root / "candidate_registry.yaml",
        "prior_candidate_evidence": root / "prior_candidate_evidence.json",
        "prior_component_value_matrix": root / "component_value_matrix.json",
        "component_value_doc": root / "component_value_doc.md",
        "prior_candidate_evidence_doc": root / "prior_candidate_evidence_doc.md",
        "candidate_reclassification_doc": root / "candidate_reclassification_doc.md",
        "execution_semantics_review_doc": root / "execution_semantics_review_doc.md",
        "growth_tilt_signal_doc": root / "growth_tilt_signal_doc.md",
        "report_registry": root / "report_registry.yaml",
        "artifact_catalog": root / "artifact_catalog.md",
        "system_flow": root / "system_flow.md",
    }
    _write_json(paths["source_2430_promotion_review"], sources["source_2430_promotion_review"])
    _write_json(paths["prior_candidate_evidence"], sources["prior_candidate_evidence"])
    _write_json(paths["prior_component_value_matrix"], sources["prior_component_value_matrix"])
    paths["candidate_registry"].write_text(
        "policy_id: equal_risk_growth_tilt_candidate_registry_v1\n"
        "safety_boundary:\n"
        "  paper_shadow_allowed: false\n"
        "  production_allowed: false\n"
        "  broker_action: none\n"
        "  research_only: true\n"
        "  observe_only: true\n"
        "candidate_families:\n"
        "  - strategy_id: equal_risk_growth_tilt_vol_target_v1\n"
        "    candidate_family: vol_target_growth_tilt\n"
        "    paper_shadow_allowed: false\n"
        "    production_allowed: false\n"
        "    broker_action: none\n",
        encoding="utf-8",
    )
    for key in (
        "component_value_doc",
        "prior_candidate_evidence_doc",
        "candidate_reclassification_doc",
        "execution_semantics_review_doc",
        "growth_tilt_signal_doc",
    ):
        paths[key].write_text(_research_doc_text(), encoding="utf-8")
    paths["report_registry"].write_text(
        "reports:\n"
        + "\n".join(
            f"  - report_id: {report_id}" for report_id in matrix.REQUIRED_REPORT_IDS
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
        "source_2430_promotion_review_result_path": (
            paths["source_2430_promotion_review"]
        ),
        "candidate_registry_path": paths["candidate_registry"],
        "prior_candidate_evidence_path": paths["prior_candidate_evidence"],
        "prior_component_value_matrix_path": paths["prior_component_value_matrix"],
        "component_value_doc_path": paths["component_value_doc"],
        "prior_candidate_evidence_doc_path": paths["prior_candidate_evidence_doc"],
        "candidate_reclassification_doc_path": paths["candidate_reclassification_doc"],
        "execution_semantics_review_doc_path": paths["execution_semantics_review_doc"],
        "growth_tilt_signal_doc_path": paths["growth_tilt_signal_doc"],
        "report_registry_path": paths["report_registry"],
        "artifact_catalog_path": paths["artifact_catalog"],
        "system_flow_path": paths["system_flow"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    return [
        "--source-2430-promotion-review-result",
        str(paths["source_2430_promotion_review"]),
        "--candidate-registry",
        str(paths["candidate_registry"]),
        "--prior-candidate-evidence",
        str(paths["prior_candidate_evidence"]),
        "--prior-component-value-matrix",
        str(paths["prior_component_value_matrix"]),
        "--component-value-doc",
        str(paths["component_value_doc"]),
        "--prior-candidate-evidence-doc",
        str(paths["prior_candidate_evidence_doc"]),
        "--candidate-reclassification-doc",
        str(paths["candidate_reclassification_doc"]),
        "--execution-semantics-review-doc",
        str(paths["execution_semantics_review_doc"]),
        "--growth-tilt-signal-doc",
        str(paths["growth_tilt_signal_doc"]),
        "--report-registry",
        str(paths["report_registry"]),
        "--artifact-catalog",
        str(paths["artifact_catalog"]),
        "--system-flow",
        str(paths["system_flow"]),
    ]
