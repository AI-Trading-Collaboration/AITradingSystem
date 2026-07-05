from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DATA_QUALITY_GATE_REASON,
    NEXT_ROUTE,
    OWNER_DECISION,
    PRIMARY_CANDIDATE_ID,
    READY_STATUS,
    run_dynamic_strategy_targeted_retest_owner_review_decision,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2375_READY = (
    "DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_AND_"
    "RANKING_ROBUSTNESS_DIVERGENCE_REVIEW_READY"
)
SOURCE_2376_READY = "DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_READY"
SOURCE_2375_ROUTE = "TRADING-2376_Dynamic_Strategy_Optimized_Candidate_Targeted_Retest"
SOURCE_2376_ROUTE = (
    "TRADING-2377_Dynamic_Strategy_Targeted_Retest_Owner_Review_And_"
    "Observation_Decision"
)


def test_dynamic_strategy_targeted_retest_owner_review_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_review"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_targeted_retest_owner_review_decision(
        source_event_retest_path=source_paths["event_retest"],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_decision_update_path=source_paths[
            "sensitivity_decision_update"
        ],
        source_optimization_review_path=source_paths["optimization_review"],
        source_optimization_decision_update_path=source_paths[
            "optimization_decision_update"
        ],
        source_targeted_retest_path=source_paths["targeted_retest"],
        source_targeted_decision_update_path=source_paths[
            "targeted_decision_update"
        ],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_owner_review_decision"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == DATA_QUALITY_GATE_REASON
    assert payload["primary_candidate"] == PRIMARY_CANDIDATE_ID
    assert payload["decision_from_2375"] == "OWNER_REVIEW_REQUIRED"
    assert payload["decision_from_2376"] == "CONTINUE_OPTIMIZATION"
    assert payload["owner_review_decision_recorded"] is True
    assert payload["owner_decision"] == OWNER_DECISION
    assert payload["research_only_observation_approved"] is False
    assert payload["continue_optimization_approved"] is True
    assert payload["primary_execution_cadence"] == "valid_until_window"
    assert payload["recommended_next_research_task"] == NEXT_ROUTE

    targeted_summary = payload["targeted_retest_summary"]
    assert targeted_summary["survives_realistic_cost"] is True
    assert targeted_summary["survives_conservative_cost"] is True
    assert targeted_summary["survives_harsh_cost"] is True
    assert targeted_summary["time_slice_retest_insufficient"] is True
    assert targeted_summary["regime_slice_retest_insufficient"] is True
    assert targeted_summary["return_gap_vs_ranking_top_remains"] is True

    gate = payload["continue_optimization_gate"]
    assert gate["continue_optimization_gate_ready"] is True
    assert gate["candidate_remains_worth_optimizing"] is True
    assert gate["research_only_observation_approved"] is False
    assert gate["continue_optimization_approved"] is True
    assert {
        "time_slice_robustness_improvement",
        "regime_slice_robustness_improvement",
        "return_gap_repair_vs_ranking_top",
        "upside_capture_without_turnover_increase",
        "valid_until_window_parameter_tuning",
    } <= set(gate["optimization_focus"])

    for key in (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "scheduled_task_created",
        "event_append_enabled",
        "outcome_binding_enabled",
        "outcome_store_mutated",
        "production_enabled",
        "broker_action_enabled",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    artifact_paths = payload["artifact_paths"]
    for key in (
        "json_path",
        "owner_review_decision_json",
        "continue_optimization_gate_json",
        "markdown_path",
        "continue_optimization_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_targeted_retest_owner_review_decision_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_review_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-targeted-retest-owner-review-decision",
            "--source-event-retest",
            str(source_paths["event_retest"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-decision-update",
            str(source_paths["sensitivity_decision_update"]),
            "--source-optimization-review",
            str(source_paths["optimization_review"]),
            "--source-optimization-decision-update",
            str(source_paths["optimization_decision_update"]),
            "--source-targeted-retest",
            str(source_paths["targeted_retest"]),
            "--source-targeted-decision-update",
            str(source_paths["targeted_decision_update"]),
            "--as-of",
            "2026-07-05",
            "--output-root",
            str(output_root),
            "--docs-root",
            str(docs_root),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert READY_STATUS in result.output
    assert "paper_shadow_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "owner_review_decision.json").exists()
    assert (output_root / "continue_optimization_gate.json").exists()
    assert (
        docs_root / "dynamic_strategy_targeted_retest_owner_review_decision.md"
    ).exists()
    assert (docs_root / "dynamic_strategy_2378_route.md").exists()


def test_dynamic_strategy_targeted_retest_owner_review_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_targeted_retest_owner_review_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-targeted-retest-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("owner_review_decision.json" in item for item in entry["artifact_globs"])
    assert any("continue_optimization_gate.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_targeted_retest_owner_review_decision" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-targeted-retest-owner-review-decision" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    task_text = Path("docs/task_register.md").read_text(encoding="utf-8") + Path(
        "docs/task_register_completed.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2377_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_AND_"
        "OPTIMIZATION_DECISION"
    ) in task_text
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2377_Dynamic_Strategy_Targeted_Retest_Owner_Review_And_"
        "Optimization_Decision.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "event_retest": _event_retest(),
        "candidate_ranking": _candidate_ranking_document(),
        "sensitivity_result": _sensitivity_result(),
        "sensitivity_decision_update": _sensitivity_decision_update_document(),
        "optimization_review": _optimization_review(),
        "optimization_decision_update": _optimization_decision_update_document(),
        "targeted_retest": _targeted_retest(),
        "targeted_decision_update": _targeted_decision_update_document(),
    }
    paths: dict[str, Path] = {}
    for name, payload in payloads.items():
        path = source_root / f"{name}.json"
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        paths[name] = path
    return paths


def _event_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": _candidate_rows(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _candidate_ranking_document() -> dict[str, object]:
    return {
        "status": SOURCE_2365_READY,
        "primary_execution_cadence": "valid_until_window",
        "candidate_ranking": _candidate_rows(),
    }


def _sensitivity_result() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "primary_execution_cadence": "valid_until_window",
        "robustness_ranking": _robustness_rows(),
        "decision_update": _sensitivity_decision_update_payload(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _sensitivity_decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2366_READY,
        "decision_update": _sensitivity_decision_update_payload(),
    }


def _optimization_review() -> dict[str, object]:
    return {
        "status": SOURCE_2375_READY,
        "primary_execution_cadence": "valid_until_window",
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE_ID,
        "best_candidate_after_optimization": PRIMARY_CANDIDATE_ID,
        "recommended_decision_after_optimization": "OWNER_REVIEW_REQUIRED",
        "recommended_next_research_task": SOURCE_2375_ROUTE,
        "candidate_decision_update": _optimization_decision_payload(),
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _optimization_decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2375_READY,
        "candidate_decision_update": _optimization_decision_payload(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _targeted_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2376_READY,
        "as_of": "2026-07-05",
        "primary_candidate": PRIMARY_CANDIDATE_ID,
        "decision_from_2375": "OWNER_REVIEW_REQUIRED",
        "candidate_decision_after_targeted_retest": "CONTINUE_OPTIMIZATION",
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE_ID,
        "primary_execution_cadence": "valid_until_window",
        "recommended_next_research_task": SOURCE_2376_ROUTE,
        "monthly_rebalance": {
            "allowed_for_reference": True,
            "allowed_for_primary_decision": False,
        },
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        "data_quality": {"status": "PASS_WITH_WARNINGS", "passed": True},
        "decision_update": _targeted_decision_update_payload(),
        "summary_findings": _targeted_summary_findings(),
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "daily_report_generated": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _targeted_decision_update_document() -> dict[str, object]:
    return {
        "status": SOURCE_2376_READY,
        "decision_update": _targeted_decision_update_payload(),
        "summary_findings": _targeted_summary_findings(),
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_rows() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": RANKING_TOP,
            "rank": 1,
            "primary_execution_cadence": "valid_until_window",
        },
        {
            "candidate_id": PRIMARY_CANDIDATE_ID,
            "rank": 2,
            "primary_execution_cadence": "valid_until_window",
        },
    ]


def _robustness_rows() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": PRIMARY_CANDIDATE_ID,
            "robust_rank": 1,
            "survives_realistic_cost": True,
            "survives_conservative_cost": True,
            "survives_harsh_cost": True,
        },
        {
            "candidate_id": RANKING_TOP,
            "robust_rank": 2,
            "survives_realistic_cost": True,
            "survives_conservative_cost": True,
            "survives_harsh_cost": True,
        },
    ]


def _sensitivity_decision_update_payload() -> dict[str, object]:
    return {
        "decision_update_ready": True,
        "top_candidate_after_sensitivity": PRIMARY_CANDIDATE_ID,
        "top_candidate_decision_after_sensitivity": "OWNER_REVIEW_REQUIRED",
        "robustness_ranking": _robustness_rows(),
    }


def _optimization_decision_payload() -> dict[str, object]:
    return {
        "decision_update_ready": True,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE_ID,
        "best_candidate_after_optimization": PRIMARY_CANDIDATE_ID,
        "recommended_decision_after_optimization": "OWNER_REVIEW_REQUIRED",
        "recommended_next_research_task": SOURCE_2375_ROUTE,
    }


def _targeted_decision_update_payload() -> dict[str, object]:
    return {
        "schema_version": "dynamic_strategy_targeted_retest_decision_update.v1",
        "decision_update_ready": True,
        "primary_candidate": PRIMARY_CANDIDATE_ID,
        "decision_from_2375": "OWNER_REVIEW_REQUIRED",
        "candidate_decision_after_targeted_retest": "CONTINUE_OPTIMIZATION",
        "candidate_ready_for_research_only_observation": False,
        "ranking_top_from_2365": RANKING_TOP,
        "realistic_dynamic_vs_static_gap": 0.002205,
        "conservative_dynamic_vs_static_gap": 0.001524,
        "harsh_dynamic_vs_static_gap": 0.000843,
        "time_slice_pass_rate": 0.428571,
        "regime_slice_pass_rate": 0.0,
        "ablation_support_rate": 1.0,
        "dynamic_vs_ranking_top_gap": -0.019097,
        "decision_reasons": [
            "time_slice_pass_rate=0.428571",
            "regime_slice_pass_rate=0.0",
            "dynamic_vs_ranking_top_gap=-0.019097",
        ],
        "monthly_rebalance_allowed_for_primary_decision": False,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "recommended_next_research_task": SOURCE_2376_ROUTE,
    }


def _targeted_summary_findings() -> dict[str, object]:
    return {
        "candidate_survives_realistic_cost": "YES",
        "candidate_survives_conservative_cost": "YES",
        "candidate_survives_time_slices": "NO",
        "candidate_survives_regime_slices": "NO",
        "candidate_ablation_supports_guardrails": "YES",
        "candidate_ready_for_research_only_observation": "NO",
        "valid_until_window_remains_necessary": "YES",
        "lower_turnover_guardrail_has_actual_contribution": "YES",
        "ranking_top_conflict_resolved": "NO",
        "candidate_decision_after_targeted_retest": "CONTINUE_OPTIMIZATION",
        "recommended_next_research_task": SOURCE_2376_ROUTE,
        "paper_shadow_remains_disabled": True,
        "production_remains_disabled": True,
        "broker_remains_disabled": True,
    }
