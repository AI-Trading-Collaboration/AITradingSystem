from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.dynamic_strategy_recombination_line_plateau_decision as decision
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


def test_dynamic_strategy_recombination_line_plateau_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "plateau"
    docs_root = tmp_path / "docs" / "research"

    payload = decision.run_dynamic_strategy_recombination_line_plateau_decision(
        **_source_kwargs(source_paths),
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 7),
    )

    assert payload["status"] == decision.READY_STATUS
    assert payload["source_validation_errors"] == []
    assert payload["source_ready_for_plateau_decision"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == decision.DATA_QUALITY_GATE_REASON
    assert payload["fresh_market_data_read"] is False
    assert payload["backtest_run"] is False
    assert payload["new_signal_generated"] is False
    assert payload["scoring_run"] is False
    assert payload["owner_decision_recorded"] is True
    assert payload["owner_decision"] == decision.OWNER_DECISION
    assert payload["base_candidate"] == decision.BASE_CANDIDATE
    assert payload["best_targeted_variant"] == decision.BEST_TARGETED_VARIANT
    assert (
        payload["best_targeted_variant_decision_from_2399"]
        == decision.EXPECTED_DECISION_FROM_2399
    )
    assert payload["observation_preview_candidates_count"] == 0
    assert payload["recombination_line_plateau_review_ready"] is True
    assert payload["recombination_line_plateau_detected"] is True
    assert payload["continue_local_targeted_improvement_recommended"] is False
    assert payload["data_signal_quality_review_recommended"] is True
    assert payload["pit_coverage_review_recommended"] is True
    assert payload["regime_labeling_review_recommended"] is True
    assert payload["threshold_meta_dataset_recommended"] is True
    assert payload["recommended_next_research_task"] == decision.NEXT_ROUTE

    plateau_review = payload["recombination_line_plateau_review"]
    assert plateau_review["record_ready"] is True
    assert plateau_review["recombination_line_plateau_detected"] is True
    assert "TRADING-2400" in payload["recombination_line_history"]
    next_decision = payload["next_research_direction_decision"]
    assert next_decision["recommended_default_option"] == (
        decision.DEFAULT_NEXT_DIRECTION_OPTION
    )
    assert next_decision["secondary_recommended_option"] == (
        decision.SECONDARY_RECOMMENDED_OPTION
    )

    for field in SAFETY_FALSE_FIELDS:
        assert payload[field] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    for key in (
        "json_path",
        "recombination_plateau_review_json",
        "next_research_direction_decision_json",
        "data_signal_quality_review_route_json",
        "markdown_path",
        "recombination_plateau_review_markdown",
        "next_research_direction_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_recombination_line_plateau_decision_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "plateau_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-recombination-line-plateau-decision",
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
    assert (output_root / "plateau_decision_result.json").exists()
    assert (output_root / "recombination_plateau_review.json").exists()
    assert (output_root / "next_research_direction_decision.json").exists()
    assert (output_root / "data_signal_quality_review_route.json").exists()


def test_dynamic_strategy_recombination_line_plateau_registry_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_recombination_line_plateau_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-recombination-line-plateau-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("plateau_decision_result.json" in item for item in entry["artifact_globs"])
    assert any(
        "data_signal_quality_review_route.json" in item
        for item in entry["artifact_globs"]
    )

    catalog = Path("docs/artifact_catalog.md").read_text(encoding="utf-8")
    system_flow = Path("docs/system_flow.md").read_text(encoding="utf-8")
    task_register = Path("docs/task_register.md").read_text(encoding="utf-8")
    assert "dynamic_strategy_recombination_line_plateau_decision" in catalog
    assert "dynamic-strategy-recombination-line-plateau-decision" in system_flow
    assert decision.TASK_REGISTER_ID in task_register


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "sources"
    root.mkdir(parents=True, exist_ok=True)
    paths = {
        "owner_review_decision_2400": root / "owner_review_decision_2400.json",
        "observation_non_approval_record_2400": root / "non_approval_2400.json",
        "targeted_improvement_value_summary_2400": root / "value_summary_2400.json",
        "next_route_2400": root / "next_route_2400.json",
        "targeted_retest_result_2399": root / "targeted_retest_result_2399.json",
        "targeted_variant_ranking_2399": root / "targeted_variant_ranking_2399.json",
        "decision_update_2399": root / "decision_update_2399.json",
        "gate_evidence_plan_result_2398": root / "gate_evidence_plan_2398.json",
        "owner_review_decision_2397": root / "owner_review_decision_2397.json",
        "recombination_retest_result_2396": root / "recombination_retest_2396.json",
    }
    _write_json(
        paths["owner_review_decision_2400"],
        {
            **_safe_doc(decision.m2400.READY_STATUS),
            "owner_decision": decision.m2400.OWNER_DECISION,
            "base_candidate": decision.BASE_CANDIDATE,
            "best_targeted_variant": decision.BEST_TARGETED_VARIANT,
            "best_targeted_variant_decision_from_2399": (
                decision.EXPECTED_DECISION_FROM_2399
            ),
            "observation_preview_candidates_count": 0,
            "targeted_improvement_value_retained": True,
            "plateau_review_required": True,
            "data_signal_quality_review_recommended": True,
            "threshold_meta_dataset_recommended": True,
            "research_only_observation_approved": False,
            "recommended_next_research_task": decision.m2400.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["observation_non_approval_record_2400"],
        {
            **_safe_doc(decision.m2400.READY_STATUS),
            "observation_non_approval_record": {
                "record_ready": True,
                "research_only_observation_approved": False,
            },
        },
    )
    _write_json(
        paths["targeted_improvement_value_summary_2400"],
        {
            **_safe_doc(decision.m2400.READY_STATUS),
            "targeted_improvement_value_summary": {
                "record_ready": True,
                "best_variant": decision.BEST_TARGETED_VARIANT,
                "research_value": True,
                "observation_ready": False,
            },
        },
    )
    _write_json(
        paths["next_route_2400"],
        {
            **_safe_doc(decision.m2400.READY_STATUS),
            "next_route": {
                "recommended_next_research_task": decision.m2400.NEXT_ROUTE,
            },
            "recommended_next_research_task": decision.m2400.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["targeted_retest_result_2399"],
        {
            **_safe_doc(decision.m2399.READY_STATUS),
            "best_targeted_variant": decision.BEST_TARGETED_VARIANT,
            "best_targeted_variant_decision": decision.EXPECTED_DECISION_FROM_2399,
            "observation_preview_candidates_count": 0,
            "recommended_next_research_task": decision.m2399.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["targeted_variant_ranking_2399"],
        {
            **_safe_doc(decision.m2399.READY_STATUS),
            "best_targeted_variant": decision.BEST_TARGETED_VARIANT,
        },
    )
    _write_json(
        paths["decision_update_2399"],
        {
            **_safe_doc(decision.m2399.READY_STATUS),
            "decision_update": {
                "research_only_observation_preview_exists": False,
                "observation_preview_candidates_count": 0,
            },
        },
    )
    _write_json(
        paths["gate_evidence_plan_result_2398"],
        {
            **_safe_doc(decision.m2398.READY_STATUS),
            "planned_targeted_variants": list(decision.m2399.TARGETED_VARIANT_IDS),
            "recommended_next_research_task": decision.m2398.NEXT_ROUTE,
        },
    )
    _write_json(
        paths["owner_review_decision_2397"],
        {
            **_safe_doc(decision.m2397.READY_STATUS),
            "owner_decision": decision.m2397.OWNER_DECISION,
        },
    )
    _write_json(
        paths["recombination_retest_result_2396"],
        {
            **_safe_doc(decision.m2396.READY_STATUS),
            "best_recombination_candidate": decision.BASE_CANDIDATE,
            "best_recombination_decision": decision.m2396.DECISION_OWNER_REVIEW,
        },
    )
    return paths


def _source_kwargs(paths: dict[str, Path]) -> dict[str, Path]:
    return {
        "source_owner_review_decision_2400_path": paths["owner_review_decision_2400"],
        "source_observation_non_approval_record_2400_path": paths[
            "observation_non_approval_record_2400"
        ],
        "source_targeted_improvement_value_summary_2400_path": paths[
            "targeted_improvement_value_summary_2400"
        ],
        "source_next_route_2400_path": paths["next_route_2400"],
        "source_targeted_retest_result_2399_path": paths[
            "targeted_retest_result_2399"
        ],
        "source_targeted_variant_ranking_2399_path": paths[
            "targeted_variant_ranking_2399"
        ],
        "source_decision_update_2399_path": paths["decision_update_2399"],
        "source_gate_evidence_plan_result_2398_path": paths[
            "gate_evidence_plan_result_2398"
        ],
        "source_owner_review_decision_2397_path": paths["owner_review_decision_2397"],
        "source_recombination_retest_result_2396_path": paths[
            "recombination_retest_result_2396"
        ],
    }


def _source_args(paths: dict[str, Path]) -> list[str]:
    option_by_key = {
        "owner_review_decision_2400": "--source-owner-review-decision-2400",
        "observation_non_approval_record_2400": (
            "--source-observation-non-approval-record-2400"
        ),
        "targeted_improvement_value_summary_2400": (
            "--source-targeted-improvement-value-summary-2400"
        ),
        "next_route_2400": "--source-next-route-2400",
        "targeted_retest_result_2399": "--source-targeted-retest-result-2399",
        "targeted_variant_ranking_2399": "--source-targeted-variant-ranking-2399",
        "decision_update_2399": "--source-decision-update-2399",
        "gate_evidence_plan_result_2398": "--source-gate-evidence-plan-result-2398",
        "owner_review_decision_2397": "--source-owner-review-decision-2397",
        "recombination_retest_result_2396": "--source-recombination-retest-result-2396",
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
