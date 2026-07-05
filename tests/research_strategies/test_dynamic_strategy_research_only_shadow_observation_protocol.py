from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_protocol import (
    NEXT_ROUTE,
    READY_STATUS,
    run_dynamic_strategy_research_only_shadow_observation_protocol,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

PRIMARY_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_2365_READY = "DYNAMIC_STRATEGY_EVENT_DRIVEN_RETEST_AND_CANDIDATE_RANKING_READY"
SOURCE_2366_READY = "DYNAMIC_STRATEGY_COST_TURNOVER_AND_COOLDOWN_SENSITIVITY_READY"
SOURCE_2367_READY = (
    "DYNAMIC_STRATEGY_TOP_CANDIDATE_OWNER_REVIEW_AND_SHADOW_RESEARCH_GATE_READY"
)
SOURCE_2367_ROUTE = (
    "TRADING-2368_Dynamic_Strategy_Research_Only_Shadow_Observation_Protocol"
)
OWNER_REVIEW_REQUIRED = "OWNER_REVIEW_REQUIRED"


def test_dynamic_strategy_research_only_shadow_observation_protocol_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "protocol"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_research_only_shadow_observation_protocol(
        source_owner_review_gate_path=source_paths["owner_review_gate"],
        source_candidate_owner_review_comparison_path=source_paths[
            "candidate_owner_review_comparison"
        ],
        source_shadow_research_gate_decision_path=source_paths[
            "shadow_research_gate_decision"
        ],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_sensitivity_matrix_path=source_paths["sensitivity_matrix"],
        source_decision_update_path=source_paths["decision_update"],
        source_event_retest_path=source_paths["event_retest"],
        source_candidate_ranking_path=source_paths["candidate_ranking"],
        source_cadence_matrix_path=source_paths["cadence_matrix"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_protocol"] is True
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["robustness_top_from_2366"] == PRIMARY_CANDIDATE
    assert payload["gate_decision_from_2367"] == OWNER_REVIEW_REQUIRED
    assert payload["research_only_shadow_observation_protocol_ready"] is True
    assert payload["observation_field_schema_ready"] is True
    assert payload["review_thresholds_ready"] is True
    assert payload["research_only_shadow_observation_allowed"] is True
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == (
        "NOT_APPLICABLE_PRIOR_ARTIFACT_PROTOCOL_ONLY_NO_FRESH_MARKET_DATA"
    )
    assert payload["recommended_next_research_task"] == NEXT_ROUTE

    for key in (
        "promotion_allowed",
        "paper_shadow_allowed",
        "paper_shadow_enabled",
        "paper_shadow_attempted",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "scheduler_attempted",
        "scheduled_task_created",
        "event_append_enabled",
        "event_append_attempted",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "outcome_store_mutated",
        "production_allowed",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["broker_action"] == "none"
    assert payload["production_effect"] == "none"

    protocol = payload["observation_protocol"]
    assert protocol["mode"] == "RESEARCH_ONLY"
    assert protocol["manual_run_only"] is True
    assert protocol["scheduler_enabled"] is False
    assert protocol["primary_candidate"] == PRIMARY_CANDIDATE
    assert protocol["comparison_candidate"] == RANKING_TOP
    assert {"static_baseline", RANKING_TOP, PRIMARY_CANDIDATE} <= set(
        protocol["comparison_candidates"]
    )
    assert protocol["guardrail_plan"]["hard_fail_if_any_execution_flag_true"] is True
    assert protocol["guardrail_plan"]["event_append_enabled"] is False

    field_schema = payload["observation_field_schema"]
    assert field_schema["mode"] == "RESEARCH_ONLY"
    assert field_schema["append_event_allowed"] is False
    assert field_schema["bind_outcome_allowed"] is False
    assert field_schema["paper_trade_allowed"] is False
    assert field_schema["broker_action_allowed"] is False
    assert {
        "identity",
        "signal_state",
        "portfolio_preview",
        "cost_and_turnover",
        "comparison",
        "guardrails",
        "review",
    } <= {item["section"] for item in field_schema["field_sections"]}

    thresholds = payload["review_thresholds"]
    trigger_ids = {
        item["trigger_id"] for item in thresholds["owner_review_triggers"]
    }
    assert "guardrail_trigger" in trigger_ids
    assert "paper_shadow_enabled_true" in thresholds["hard_fail_conditions"]
    assert "broker_action_enabled_true" in thresholds["hard_fail_conditions"]

    for key in (
        "json_path",
        "observation_field_schema_json",
        "review_thresholds_json",
        "markdown_path",
        "observation_field_schema_markdown",
        "review_thresholds_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_research_only_shadow_observation_protocol_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "protocol_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-research-only-shadow-observation-protocol",
            "--source-owner-review-gate",
            str(source_paths["owner_review_gate"]),
            "--source-candidate-owner-review-comparison",
            str(source_paths["candidate_owner_review_comparison"]),
            "--source-shadow-research-gate-decision",
            str(source_paths["shadow_research_gate_decision"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-sensitivity-matrix",
            str(source_paths["sensitivity_matrix"]),
            "--source-decision-update",
            str(source_paths["decision_update"]),
            "--source-event-retest",
            str(source_paths["event_retest"]),
            "--source-candidate-ranking",
            str(source_paths["candidate_ranking"]),
            "--source-cadence-matrix",
            str(source_paths["cadence_matrix"]),
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
    assert (output_root / "observation_protocol.json").exists()
    assert (output_root / "observation_field_schema.json").exists()
    assert (output_root / "review_thresholds.json").exists()

    payload = json.loads(
        (output_root / "observation_protocol.json").read_text(encoding="utf-8")
    )
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["next_route"] == NEXT_ROUTE


def test_dynamic_strategy_research_only_shadow_observation_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_research_only_shadow_observation_protocol"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-research-only-shadow-observation-protocol"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any("observation_protocol.json" in item for item in entry["artifact_globs"])
    assert any("review_thresholds.json" in item for item in entry["artifact_globs"])

    assert "dynamic_strategy_research_only_shadow_observation_protocol" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert (
        "dynamic-strategy-research-only-shadow-observation-protocol"
        in Path("docs/system_flow.md").read_text(encoding="utf-8")
    )
    assert (
        "TRADING-2368_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2368_Dynamic_Strategy_Research_Only_Shadow_Observation_Protocol.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    comparison = _candidate_review_comparison()
    shadow_decision = {
        "recommended_gate_candidate": PRIMARY_CANDIDATE,
        "recommended_gate_decision": OWNER_REVIEW_REQUIRED,
        "research_only_shadow_observation_allowed": True,
        "paper_shadow_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "next_route": SOURCE_2367_ROUTE,
    }
    decision_update = {
        "top_candidate_after_sensitivity": PRIMARY_CANDIDATE,
        "top_candidate_decision_after_sensitivity": OWNER_REVIEW_REQUIRED,
        "recommended_next_research_task": (
            "TRADING-2367_Dynamic_Strategy_Top_Candidate_Owner_Review_And_"
            "Shadow_Research_Gate"
        ),
    }
    documents = {
        "owner_review_gate": {
            "status": SOURCE_2367_READY,
            "as_of": "2026-07-05",
            "next_route": SOURCE_2367_ROUTE,
            "ranking_top_from_2365": RANKING_TOP,
            "robustness_top_from_2366": PRIMARY_CANDIDATE,
            "ranking_robustness_divergence_detected": True,
            "recommended_gate_candidate": PRIMARY_CANDIDATE,
            "recommended_gate_decision": OWNER_REVIEW_REQUIRED,
            "research_only_shadow_observation_allowed": True,
            "primary_execution_cadence": "valid_until_window",
            "candidate_review_comparison": comparison,
            "shadow_research_gate_decision": shadow_decision,
            "data_quality": {
                "status": "PASS_WITH_WARNINGS",
                "error_count": 0,
                "warning_count": 1,
            },
            "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
            "paper_shadow_enabled": False,
            "production_enabled": False,
            "broker_action_enabled": False,
        },
        "candidate_owner_review_comparison": {
            "status": SOURCE_2367_READY,
            "candidate_review_comparison": comparison,
            "paper_shadow_enabled": False,
            "production_enabled": False,
            "broker_action_enabled": False,
        },
        "shadow_research_gate_decision": {
            "status": SOURCE_2367_READY,
            "shadow_research_gate_decision": shadow_decision,
            "paper_shadow_enabled": False,
            "production_enabled": False,
            "broker_action_enabled": False,
        },
        "sensitivity_result": {
            "status": SOURCE_2366_READY,
            "as_of": "2026-07-05",
            "primary_execution_cadence": "valid_until_window",
            "summary": {
                "top_candidate_from_2365": RANKING_TOP,
                "top_candidate_after_sensitivity": PRIMARY_CANDIDATE,
            },
            "decision_update": decision_update,
            "data_quality": {
                "status": "PASS_WITH_WARNINGS",
                "error_count": 0,
                "warning_count": 1,
            },
            "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        },
        "sensitivity_matrix": {
            "status": SOURCE_2366_READY,
            "sensitivity_matrix": [
                {"candidate_id": PRIMARY_CANDIDATE, "scenario_id": "combined_realistic"}
            ],
        },
        "decision_update": {
            "status": SOURCE_2366_READY,
            "decision_update": decision_update,
        },
        "event_retest": {
            "status": SOURCE_2365_READY,
            "as_of": "2026-07-05",
            "primary_execution_cadence": "valid_until_window",
            "summary": {"top_candidate": RANKING_TOP},
            "candidate_ranking": [
                {"candidate_id": RANKING_TOP, "rank": 1},
                {"candidate_id": PRIMARY_CANDIDATE, "rank": 2},
            ],
            "data_quality": {
                "status": "PASS_WITH_WARNINGS",
                "error_count": 0,
                "warning_count": 1,
            },
            "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        },
        "candidate_ranking": {
            "status": SOURCE_2365_READY,
            "candidate_ranking": [
                {"candidate_id": RANKING_TOP, "rank": 1},
                {"candidate_id": PRIMARY_CANDIDATE, "rank": 2},
            ],
        },
        "cadence_matrix": {
            "status": SOURCE_2365_READY,
            "cadence_comparison_matrix": [
                {"candidate_id": RANKING_TOP, "scenario_id": "valid_until_window"},
                {
                    "candidate_id": PRIMARY_CANDIDATE,
                    "scenario_id": "valid_until_window",
                },
            ],
        },
    }
    paths = {
        "owner_review_gate": source_root / "owner_review_gate_result.json",
        "candidate_owner_review_comparison": (
            source_root / "candidate_owner_review_comparison.json"
        ),
        "shadow_research_gate_decision": source_root / "shadow_research_gate_decision.json",
        "sensitivity_result": source_root / "sensitivity_result.json",
        "sensitivity_matrix": source_root / "sensitivity_matrix.json",
        "decision_update": source_root / "decision_update.json",
        "event_retest": source_root / "event_driven_retest_result.json",
        "candidate_ranking": source_root / "candidate_ranking.json",
        "cadence_matrix": source_root / "cadence_comparison_matrix.json",
    }
    for key, path in paths.items():
        path.write_text(
            json.dumps(documents[key], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return paths


def _candidate_review_comparison() -> list[dict[str, object]]:
    return [
        {
            "candidate_id": RANKING_TOP,
            "ranking_rank_from_2365": 1.0,
            "robustness_rank": 2.0,
            "decision_after_2366": OWNER_REVIEW_REQUIRED,
            "turnover_acceptable_after_2366": False,
        },
        {
            "candidate_id": PRIMARY_CANDIDATE,
            "ranking_rank_from_2365": 2.0,
            "robustness_rank": 1.0,
            "decision_after_2366": OWNER_REVIEW_REQUIRED,
            "turnover_acceptable_after_2366": False,
        },
        {
            "candidate_id": "static_baseline",
            "ranking_rank_from_2365": None,
            "robustness_rank": None,
            "decision_after_2366": "STATIC_BASELINE_REFERENCE",
            "turnover_acceptable_after_2366": True,
        },
    ]
