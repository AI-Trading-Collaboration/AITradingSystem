from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_targeted_gate_evidence_owner_review_decision as decision
from ai_trading_system.cli import app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SAFETY_FALSE_FIELDS: tuple[str, ...] = (
    "candidate_auto_accept_approved",
    "research_only_observation_approved",
    "paper_shadow_enabled",
    "paper_shadow_approved",
    "paper_trade_created",
    "shadow_position_created",
    "scheduler_enabled",
    "event_append_enabled",
    "event_append_approved",
    "outcome_binding_enabled",
    "outcome_binding_approved",
    "production_enabled",
    "broker_action_enabled",
    "daily_report_generated",
)


def test_dynamic_strategy_targeted_gate_evidence_owner_review_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_decision"
    docs_root = tmp_path / "docs" / "research"

    payload = decision.run_dynamic_strategy_targeted_gate_evidence_owner_review_decision(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == decision.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_owner_review_decision"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == decision.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["owner_review_decision_recorded"] is True
    assert payload["owner_decision"] == decision.OWNER_DECISION
    assert payload["base_candidate"] == decision.BASE_CANDIDATE
    assert payload["best_targeted_variant"] == decision.BEST_TARGETED_VARIANT
    assert (
        payload["best_targeted_variant_decision_from_2399"]
        == decision.EXPECTED_DECISION_FROM_2399
    )
    assert payload["observation_preview_candidates_count"] == 0
    assert payload["research_only_observation_approved"] is False
    assert payload["targeted_improvement_value_retained"] is True
    assert payload["plateau_review_required"] is True
    assert payload["data_signal_quality_review_recommended"] is True
    assert payload["threshold_meta_dataset_recommended"] is True
    assert payload["recommended_next_research_task"] == decision.NEXT_ROUTE

    value_summary = payload["targeted_improvement_value_summary"]
    assert value_summary["record_ready"] is True
    assert value_summary["best_variant"] == decision.BEST_TARGETED_VARIANT
    assert value_summary["research_value"] is True
    assert value_summary["observation_ready"] is False
    assert payload["observation_non_approval_record"]["record_ready"] is True
    assert payload["observation_non_approval_reason"] == list(
        decision.OBSERVATION_NON_APPROVAL_REASONS
    )

    for field in SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "observation_non_approval_record_json",
        "targeted_improvement_value_summary_json",
        "next_route_json",
        "markdown_path",
        "observation_non_approval_markdown",
        "targeted_improvement_value_summary_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_targeted_gate_evidence_owner_review_decision_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-targeted-gate-evidence-owner-review-decision",
            *_source_args(source_paths),
            "--as-of",
            "2026-07-07",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert decision.READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "owner_review_decision.json").exists()
    assert (output_root / "observation_non_approval_record.json").exists()
    assert (output_root / "targeted_improvement_value_summary.json").exists()
    assert (output_root / "next_route.json").exists()


def test_dynamic_strategy_targeted_gate_evidence_owner_review_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_targeted_gate_evidence_owner_review_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-targeted-gate-evidence-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("owner_review_decision.json" in item for item in entry["artifact_globs"])
    assert any(
        "targeted_improvement_value_summary.json" in item
        for item in entry["artifact_globs"]
    )

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_targeted_gate_evidence_owner_review_decision" in catalog
    assert (
        "dynamic-strategy-targeted-gate-evidence-owner-review-decision"
        in system_flow
    )
    assert decision.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "targeted_retest_result_2399": root / "targeted_gate_evidence_retest_result.json",
        "targeted_variant_ranking_2399": root / "targeted_variant_ranking.json",
        "gate_evidence_matrix_2399": root / "gate_evidence_matrix.json",
        "decision_update_2399": root / "decision_update.json",
        "gate_evidence_plan_result_2398": root / "gate_evidence_plan_result.json",
        "targeted_improvement_plan_2398": root / "targeted_improvement_plan.json",
        "next_route_2398": root / "next_route_2398.json",
        "owner_review_decision_2397": root / "owner_review_decision_2397.json",
        "recombination_retest_result_2396": root / "recombination_retest_result.json",
        "decision_update_2396": root / "decision_update_2396.json",
    }
    best_row = {
        "rank": 1,
        "candidate_id": decision.BEST_TARGETED_VARIANT,
        "decision": decision.EXPECTED_DECISION_FROM_2399,
        "annualized_return": 0.205,
        "max_drawdown": -0.138,
        "time_slice_pass_rate": 0.42,
        "regime_expectation_score": 0.36,
        "return_retention_vs_raw_growth_tilt": 0.96,
        "turnover_reduction_vs_raw_growth_tilt": -0.32,
        "stale_signal_execution_count": 0,
        "no_stale_signal_carry_forward": True,
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }
    matrix_row = {
        "candidate_id": decision.BEST_TARGETED_VARIANT,
        "decision_evidence": {
            "candidate_decision": decision.EXPECTED_DECISION_FROM_2399,
            "observation_preview_candidate": False,
            "owner_review_required": False,
            "research_only_observation_approved": False,
            "recommended_next_research_task": decision.m2399.NEXT_ROUTE,
        },
        "research_only_observation_approved": False,
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
    }
    _write_json(
        paths["targeted_retest_result_2399"],
        {
            **_safe_doc(decision.m2399.READY_STATUS),
            "candidate_under_review": decision.BASE_CANDIDATE,
            "best_targeted_variant": decision.BEST_TARGETED_VARIANT,
            "best_targeted_variant_decision": decision.EXPECTED_DECISION_FROM_2399,
            "observation_preview_candidates_count": 0,
            "targeted_retest_ready": True,
            "variant_ranking_ready": True,
            "gate_evidence_matrix_ready": True,
            "decision_update_ready": True,
            "data_quality_gate_executed": True,
            "recommended_next_research_task": decision.m2399.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["targeted_variant_ranking_2399"],
        {
            **_safe_doc(decision.m2399.READY_STATUS),
            "best_targeted_variant": decision.BEST_TARGETED_VARIANT,
            "best_targeted_variant_decision": decision.EXPECTED_DECISION_FROM_2399,
            "observation_preview_candidates_count": 0,
            "recommended_next_research_task": decision.m2399.NEXT_ROUTE,
            "targeted_variant_ranking": [best_row],
        },
    )
    _write_json(
        paths["gate_evidence_matrix_2399"],
        {
            **_safe_doc(decision.m2399.READY_STATUS),
            "gate_evidence_matrix": [matrix_row],
        },
    )
    _write_json(
        paths["decision_update_2399"],
        {
            **_safe_doc(decision.m2399.READY_STATUS),
            "recommended_next_research_task": decision.m2399.NEXT_ROUTE,
            "decision_update": {
                "decision_update_ready": True,
                "best_targeted_variant": decision.BEST_TARGETED_VARIANT,
                "best_targeted_variant_decision": (
                    decision.EXPECTED_DECISION_FROM_2399
                ),
                "observation_preview_candidates": [],
                "observation_preview_candidates_count": 0,
                "owner_review_required_candidates": [],
                "owner_review_required_candidates_count": 0,
                "research_only_observation_preview_exists": False,
                "research_only_observation_approved": False,
                "recommended_next_research_task": decision.m2399.NEXT_ROUTE,
            },
        },
    )
    _write_json(
        paths["gate_evidence_plan_result_2398"],
        {
            **_safe_doc(decision.m2398.READY_STATUS),
            "candidate_under_review": decision.BASE_CANDIDATE,
            "planned_targeted_variants": list(decision.m2399.TARGETED_VARIANT_IDS),
            "recommended_next_research_task": decision.m2398.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["targeted_improvement_plan_2398"],
        {
            **_safe_doc(decision.m2398.READY_STATUS),
            "targeted_improvement_plan": {
                "record_ready": True,
                "targeted_variants": [
                    {"candidate_id": candidate_id}
                    for candidate_id in decision.m2399.TARGETED_VARIANT_IDS
                ],
            },
        },
    )
    _write_json(
        paths["next_route_2398"],
        {
            **_safe_doc(decision.m2398.READY_STATUS),
            "recommended_next_research_task": decision.m2398.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["owner_review_decision_2397"],
        {
            **_safe_doc(decision.m2397.READY_STATUS),
            "owner_decision": decision.m2397.OWNER_DECISION,
            "best_recombination_candidate": decision.BASE_CANDIDATE,
            "research_only_observation_approved": False,
            "recommended_next_research_task": decision.m2397.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["recombination_retest_result_2396"],
        {
            **_safe_doc(decision.m2396.READY_STATUS),
            "best_recombination_candidate": decision.BASE_CANDIDATE,
            "best_recombination_decision": decision.m2396.DECISION_OWNER_REVIEW,
            "research_only_observation_approved": False,
            "recommended_next_research_task": decision.m2396.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["decision_update_2396"],
        {
            **_safe_doc(decision.m2396.READY_STATUS),
            "decision_update": {
                "best_recombination_candidate": decision.BASE_CANDIDATE,
                "best_recombination_decision": decision.m2396.DECISION_OWNER_REVIEW,
            },
        },
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_targeted_retest_result_2399_path": paths[
            "targeted_retest_result_2399"
        ],
        "source_targeted_variant_ranking_2399_path": paths[
            "targeted_variant_ranking_2399"
        ],
        "source_gate_evidence_matrix_2399_path": paths["gate_evidence_matrix_2399"],
        "source_decision_update_2399_path": paths["decision_update_2399"],
        "source_gate_evidence_plan_result_2398_path": paths[
            "gate_evidence_plan_result_2398"
        ],
        "source_targeted_improvement_plan_2398_path": paths[
            "targeted_improvement_plan_2398"
        ],
        "source_next_route_2398_path": paths["next_route_2398"],
        "source_owner_review_decision_2397_path": paths["owner_review_decision_2397"],
        "source_recombination_retest_result_2396_path": paths[
            "recombination_retest_result_2396"
        ],
        "source_decision_update_2396_path": paths["decision_update_2396"],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "targeted_retest_result_2399": "--source-targeted-retest-result-2399",
        "targeted_variant_ranking_2399": "--source-targeted-variant-ranking-2399",
        "gate_evidence_matrix_2399": "--source-gate-evidence-matrix-2399",
        "decision_update_2399": "--source-decision-update-2399",
        "gate_evidence_plan_result_2398": "--source-gate-evidence-plan-result-2398",
        "targeted_improvement_plan_2398": "--source-targeted-improvement-plan-2398",
        "next_route_2398": "--source-next-route-2398",
        "owner_review_decision_2397": "--source-owner-review-decision-2397",
        "recombination_retest_result_2396": "--source-recombination-retest-result-2396",
        "decision_update_2396": "--source-decision-update-2396",
    }
    args: list[str] = []
    for key, option in option_by_key.items():
        args.extend([option, str(paths[key])])
    return args


def _safe_doc(status: str) -> dict[str, object]:
    return {
        "status": status,
        **{field: False for field in decision.SAFETY_FALSE_FIELDS},
        "production_effect": "none",
        "broker_action": "none",
    }


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
