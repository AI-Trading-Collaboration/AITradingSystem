from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_guarded_variant_owner_review_decision import (
    BEST_GUARDED_VARIANT,
    DATA_QUALITY_GATE_REASON,
    NEXT_DIRECTION,
    NEXT_ROUTE,
    OWNER_DECISION,
    READY_STATUS,
    run_dynamic_strategy_guarded_variant_owner_review_decision,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SOURCE_2379_READY = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_OPTIMIZED_VARIANT_RETEST_READY"
)
SOURCE_2380_READY = (
    "DYNAMIC_STRATEGY_OPTIMIZED_VARIANT_OWNER_REVIEW_AND_OBSERVATION_DECISION_READY"
)
SOURCE_2381_READY = (
    "DYNAMIC_STRATEGY_OPTIMIZATION_PLATEAU_AND_NEXT_CANDIDATE_DECISION_READY"
)
SOURCE_2382_READY = (
    "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_TURNOVER_RETEST_PLAN_READY"
)
SOURCE_2383_READY = "DYNAMIC_STRATEGY_RANKING_TOP_GUARDED_VARIANT_RETEST_READY"
SOURCE_2379_ROUTE = (
    "TRADING-2380_Dynamic_Strategy_Optimized_Variant_Owner_Review_And_"
    "Observation_Decision"
)
SOURCE_2380_ROUTE = (
    "TRADING-2381_Dynamic_Strategy_Optimization_Plateau_And_Next_Candidate_"
    "Decision"
)
SOURCE_2381_ROUTE = (
    "TRADING-2382_Dynamic_Strategy_Ranking_Top_Guarded_Turnover_Retest_Plan"
)
SOURCE_2382_ROUTE = "TRADING-2383_Dynamic_Strategy_Ranking_Top_Guarded_Variant_Retest"
SOURCE_2383_ROUTE = (
    "TRADING-2384_Dynamic_Strategy_Guarded_Variant_Owner_Review_And_"
    "Observation_Decision"
)
SOURCE_2380_OWNER_DECISION = (
    "DO_NOT_APPROVE_OBSERVATION_CONTINUE_OPTIMIZATION_REVIEW_REQUIRED"
)
SOURCE_2381_NEXT_DIRECTION = "OPTION_B_SWITCH_TO_RANKING_TOP_GUARDED_VARIANT"
BASE_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
LOWER_BEST_VARIANT = "dynamic_regime_overlay_v0_4_cooldown_balanced_v1"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
PRIMARY_CADENCE = "valid_until_window"


def test_dynamic_strategy_guarded_variant_owner_review_decision_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "owner_review"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_guarded_variant_owner_review_decision(
        source_guarded_variant_retest_path=source_paths["guarded_variant_retest"],
        source_guarded_decision_update_path=source_paths["guarded_decision_update"],
        source_guarded_variant_ranking_path=source_paths["guarded_variant_ranking"],
        source_retest_plan_path=source_paths["retest_plan"],
        source_guarded_variant_plan_path=source_paths["guarded_variant_plan"],
        source_plateau_decision_path=source_paths["plateau_decision"],
        source_next_direction_path=source_paths["next_direction"],
        source_variant_retest_path=source_paths["variant_retest"],
        source_optimized_variant_ranking_path=source_paths[
            "optimized_variant_ranking"
        ],
        source_owner_review_path=source_paths["owner_review"],
        source_observation_rejection_path=source_paths["observation_rejection"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 6),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_owner_review_decision"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == DATA_QUALITY_GATE_REASON
    assert payload["owner_review_decision_recorded"] is True
    assert payload["owner_decision"] == OWNER_DECISION
    assert payload["research_only_observation_approved"] is False
    assert payload["continue_local_optimization_allowed"] is False
    assert payload["candidate_pool_expansion_recommended"] is True
    assert payload["signal_family_diversification_recommended"] is True
    assert payload["primary_execution_cadence"] == PRIMARY_CADENCE
    assert payload["recommended_next_research_task"] == NEXT_ROUTE

    lower_line = payload["lower_turnover_line"]
    assert lower_line["best_variant"] == LOWER_BEST_VARIANT
    assert lower_line["decision"] == "CONTINUE_OPTIMIZATION"
    assert lower_line["observation_approved"] is False

    ranking_line = payload["ranking_top_guarded_line"]
    assert ranking_line["base_candidate"] == RANKING_TOP
    assert ranking_line["best_variant"] == BEST_GUARDED_VARIANT
    assert ranking_line["decision"] == "CONTINUE_OPTIMIZATION"
    assert ranking_line["candidate_ready_for_research_only_observation"] is False
    assert ranking_line["observation_approved"] is False

    next_direction = payload["next_research_direction_decision"]
    assert next_direction["next_direction"] == NEXT_DIRECTION
    assert next_direction["recommended_next_research_task"] == NEXT_ROUTE
    assert next_direction["continue_local_optimization_allowed"] is False
    assert next_direction["candidate_pool_expansion_recommended"] is True
    assert next_direction["signal_family_diversification_recommended"] is True

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
        "two_line_candidate_review_json",
        "next_research_direction_decision_json",
        "markdown_path",
        "two_line_candidate_review_markdown",
        "observation_rejection_after_guarded_retest_markdown",
        "next_route_markdown",
    ):
        assert Path(artifact_paths[key]).exists()


def test_dynamic_strategy_guarded_variant_owner_review_decision_cli(
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
            "dynamic-strategy-guarded-variant-owner-review-decision",
            "--source-guarded-variant-retest",
            str(source_paths["guarded_variant_retest"]),
            "--source-guarded-decision-update",
            str(source_paths["guarded_decision_update"]),
            "--source-guarded-variant-ranking",
            str(source_paths["guarded_variant_ranking"]),
            "--source-retest-plan",
            str(source_paths["retest_plan"]),
            "--source-guarded-variant-plan",
            str(source_paths["guarded_variant_plan"]),
            "--source-plateau-decision",
            str(source_paths["plateau_decision"]),
            "--source-next-direction",
            str(source_paths["next_direction"]),
            "--source-variant-retest",
            str(source_paths["variant_retest"]),
            "--source-optimized-variant-ranking",
            str(source_paths["optimized_variant_ranking"]),
            "--source-owner-review",
            str(source_paths["owner_review"]),
            "--source-observation-rejection",
            str(source_paths["observation_rejection"]),
            "--as-of",
            "2026-07-06",
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
    assert "production_allowed=False" in result.output
    assert "broker_action=none" in result.output
    assert (output_root / "owner_review_decision.json").exists()
    assert (output_root / "two_line_candidate_review.json").exists()
    assert (output_root / "next_research_direction_decision.json").exists()
    assert (
        docs_root / "dynamic_strategy_guarded_variant_owner_review_decision.md"
    ).exists()
    assert (docs_root / "dynamic_strategy_two_line_candidate_review.md").exists()
    assert (
        docs_root / "dynamic_strategy_observation_rejection_after_guarded_retest.md"
    ).exists()
    assert (docs_root / "dynamic_strategy_2385_route.md").exists()


def test_dynamic_strategy_guarded_variant_owner_review_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_guarded_variant_owner_review_decision"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-guarded-variant-owner-review-decision"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("owner_review_decision.json" in item for item in entry["artifact_globs"])
    assert any(
        "two_line_candidate_review.json" in item for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_guarded_variant_owner_review_decision" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-guarded-variant-owner-review-decision" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    task_text = Path("docs/task_register.md").read_text(encoding="utf-8") + Path(
        "docs/task_register_completed.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2384_DYNAMIC_STRATEGY_GUARDED_VARIANT_OWNER_REVIEW_AND_"
        "OBSERVATION_DECISION"
    ) in task_text
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2384_Dynamic_Strategy_Guarded_Variant_Owner_Review_And_"
        "Observation_Decision.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "guarded_variant_retest": _guarded_variant_retest(),
        "guarded_decision_update": _guarded_decision_update(),
        "guarded_variant_ranking": _guarded_variant_ranking(),
        "retest_plan": _retest_plan(),
        "guarded_variant_plan": _guarded_variant_plan(),
        "plateau_decision": _plateau_decision(),
        "next_direction": _next_direction(),
        "variant_retest": _variant_retest(),
        "optimized_variant_ranking": _optimized_variant_ranking(),
        "owner_review": _owner_review(),
        "observation_rejection": _observation_rejection(),
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


def _guarded_variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "as_of": "2026-07-05",
        "ranking_top_candidate": RANKING_TOP,
        "best_guarded_variant": BEST_GUARDED_VARIANT,
        "best_guarded_variant_decision": "CONTINUE_OPTIMIZATION",
        "candidate_ready_for_research_only_observation": False,
        "recommended_next_research_task": SOURCE_2383_ROUTE,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "data_quality_gate_executed": True,
        "data_quality_passed": True,
        "decision_update": {
            "best_guarded_variant": BEST_GUARDED_VARIANT,
            "best_guarded_variant_decision": "CONTINUE_OPTIMIZATION",
            "candidate_ready_for_research_only_observation": False,
        },
        **_safety_fields(),
    }


def _guarded_decision_update() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "decision_update": {
            "best_guarded_variant": BEST_GUARDED_VARIANT,
            "best_guarded_variant_decision": "CONTINUE_OPTIMIZATION",
            "candidate_ready_for_research_only_observation": False,
            "recommended_next_research_task": SOURCE_2383_ROUTE,
        },
        **_safety_fields(),
    }


def _guarded_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2383_READY,
        "guarded_variant_ranking": [
            {
                "rank": 1,
                "variant_id": BEST_GUARDED_VARIANT,
                "decision": "CONTINUE_OPTIMIZATION",
            }
        ],
        **_safety_fields(),
    }


def _retest_plan() -> dict[str, object]:
    return {
        "status": SOURCE_2382_READY,
        "ranking_top_candidate": RANKING_TOP,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "recommended_next_research_task": SOURCE_2382_ROUTE,
        **_safety_fields(),
    }


def _guarded_variant_plan() -> dict[str, object]:
    return {
        "status": SOURCE_2382_READY,
        "ranking_top_candidate": RANKING_TOP,
        **_safety_fields(),
    }


def _plateau_decision() -> dict[str, object]:
    return {
        "status": SOURCE_2381_READY,
        "next_direction_decision": SOURCE_2381_NEXT_DIRECTION,
        "recommended_next_research_task": SOURCE_2381_ROUTE,
        **_safety_fields(),
    }


def _next_direction() -> dict[str, object]:
    return {
        "status": SOURCE_2381_READY,
        "next_candidate_direction": {
            "next_direction_decision": SOURCE_2381_NEXT_DIRECTION,
            "recommended_next_research_task": SOURCE_2381_ROUTE,
        },
        **_safety_fields(),
    }


def _variant_retest() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "as_of": "2026-07-05",
        "base_candidate": BASE_CANDIDATE,
        "ranking_top_reference": RANKING_TOP,
        "best_variant_after_retest": LOWER_BEST_VARIANT,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        "candidate_ready_for_research_only_observation": False,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "recommended_next_research_task": SOURCE_2379_ROUTE,
        "decision_update": {
            "best_variant_after_retest": LOWER_BEST_VARIANT,
            "best_variant_decision": "CONTINUE_OPTIMIZATION",
        },
        **_safety_fields(),
    }


def _optimized_variant_ranking() -> dict[str, object]:
    return {
        "status": SOURCE_2379_READY,
        "best_variant_after_retest": LOWER_BEST_VARIANT,
        "best_variant_decision": "CONTINUE_OPTIMIZATION",
        **_safety_fields(),
    }


def _owner_review() -> dict[str, object]:
    return {
        "status": SOURCE_2380_READY,
        "base_candidate": BASE_CANDIDATE,
        "best_variant_from_2379": LOWER_BEST_VARIANT,
        "best_variant_decision_from_2379": "CONTINUE_OPTIMIZATION",
        "owner_decision": SOURCE_2380_OWNER_DECISION,
        "research_only_observation_approved": False,
        "primary_execution_cadence": PRIMARY_CADENCE,
        "recommended_next_research_task": SOURCE_2380_ROUTE,
        **_safety_fields(),
    }


def _observation_rejection() -> dict[str, object]:
    return {
        "status": SOURCE_2380_READY,
        "observation_rejection_rationale": {
            "research_only_observation_approved": False,
            "paper_shadow_approved": False,
        },
        **_safety_fields(),
    }


def _safety_fields() -> dict[str, object]:
    return {
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "production_effect": "none",
        "broker_action": "none",
    }
