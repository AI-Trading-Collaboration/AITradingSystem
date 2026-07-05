from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_research_only_shadow_observation_dry_run import (
    NEXT_ROUTE,
    OBSERVATION_MODE,
    READY_STATUS,
    run_dynamic_strategy_research_only_shadow_observation_dry_run,
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
SOURCE_2368_READY = "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_PROTOCOL_READY"
SOURCE_2368_ROUTE = "TRADING-2369_Dynamic_Strategy_Research_Only_Shadow_Observation_Dry_Run"
OWNER_REVIEW_REQUIRED = "OWNER_REVIEW_REQUIRED"


def test_dynamic_strategy_research_only_shadow_observation_dry_run_builder(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "dry_run"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_research_only_shadow_observation_dry_run(
        source_observation_protocol_path=source_paths["observation_protocol"],
        source_observation_field_schema_path=source_paths["observation_field_schema"],
        source_review_thresholds_path=source_paths["review_thresholds"],
        source_owner_review_gate_path=source_paths["owner_review_gate"],
        source_candidate_owner_review_comparison_path=source_paths[
            "candidate_owner_review_comparison"
        ],
        source_sensitivity_result_path=source_paths["sensitivity_result"],
        source_event_retest_path=source_paths["event_retest"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_dry_run"] is True
    assert payload["observation_mode"] == OBSERVATION_MODE
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["robustness_top_from_2366"] == PRIMARY_CANDIDATE
    assert payload["gate_decision_from_2367"] == OWNER_REVIEW_REQUIRED
    assert payload["observation_protocol_loaded"] is True
    assert payload["observation_field_schema_loaded"] is True
    assert payload["review_thresholds_loaded"] is True
    assert payload["owner_review_gate_loaded"] is True
    assert payload["sensitivity_result_loaded"] is True
    assert payload["event_retest_loaded"] is True
    assert payload["observation_dry_run_record_ready"] is True
    assert payload["no_side_effect_evidence_ready"] is True
    assert payload["research_only_shadow_observation_allowed"] is True
    assert payload["observation_decision"] == OWNER_REVIEW_REQUIRED
    assert payload["owner_review_required"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == (
        "NOT_APPLICABLE_PRIOR_ARTIFACT_DRY_RUN_ONLY_NO_FRESH_MARKET_DATA"
    )

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
        "historical_event_log_mutated",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "outcome_store_mutated",
        "production_allowed",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["broker_action"] == "none"
    assert payload["production_effect"] == "none"

    record = payload["observation_dry_run_record"]
    assert record["observation_mode"] == OBSERVATION_MODE
    assert record["identity"]["candidate_id"] == PRIMARY_CANDIDATE
    assert record["signal_state"]["signal_state"] == (
        "SOURCE_ARTIFACT_PREVIEW_ONLY_NOT_RECOMPUTED"
    )
    assert record["portfolio_preview"]["no_trade_reason"] == (
        "RESEARCH_ONLY_DRY_RUN_NO_EXECUTION"
    )
    assert record["cost_and_turnover"]["turnover_cap_state"] == (
        "OWNER_REVIEW_REQUIRED_TURNOVER_NOT_ACCEPTABLE_AFTER_2366"
    )
    assert record["comparison"]["static_baseline_comparison"]["candidate_id"] == (
        "static_baseline"
    )
    assert record["comparison"]["ranking_top_candidate_comparison"]["candidate_id"] == (
        RANKING_TOP
    )
    assert record["comparison"]["robustness_top_candidate_comparison"]["candidate_id"] == (
        PRIMARY_CANDIDATE
    )
    assert record["review"]["observation_decision"] == OWNER_REVIEW_REQUIRED
    assert record["guardrails"]["event_append_attempted"] is False
    assert record["guardrails"]["broker_action_enabled"] is False

    evidence = payload["no_side_effect_evidence"]
    assert evidence["status"] == "PASS"
    assert evidence["event_append_attempted"] is False
    assert evidence["event_append_performed"] is False
    assert evidence["outcome_binding_attempted"] is False
    assert evidence["outcome_store_mutated"] is False
    assert evidence["paper_trade_created"] is False
    assert evidence["shadow_position_created"] is False
    assert evidence["daily_report_generated"] is False
    assert evidence["broker_action_attempted"] is False

    for key in (
        "json_path",
        "observation_dry_run_record_json",
        "no_side_effect_evidence_json",
        "markdown_path",
        "observation_dry_run_record_markdown",
        "no_side_effect_evidence_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_research_only_shadow_observation_dry_run_cli(
    tmp_path: Path,
) -> None:
    source_paths = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "dry_run_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-research-only-shadow-observation-dry-run",
            "--source-observation-protocol",
            str(source_paths["observation_protocol"]),
            "--source-observation-field-schema",
            str(source_paths["observation_field_schema"]),
            "--source-review-thresholds",
            str(source_paths["review_thresholds"]),
            "--source-owner-review-gate",
            str(source_paths["owner_review_gate"]),
            "--source-candidate-owner-review-comparison",
            str(source_paths["candidate_owner_review_comparison"]),
            "--source-sensitivity-result",
            str(source_paths["sensitivity_result"]),
            "--source-event-retest",
            str(source_paths["event_retest"]),
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
    assert (output_root / "observation_dry_run_result.json").exists()
    assert (output_root / "observation_dry_run_record.json").exists()
    assert (output_root / "no_side_effect_evidence.json").exists()

    payload = json.loads(
        (output_root / "observation_dry_run_result.json").read_text(encoding="utf-8")
    )
    assert payload["observation_mode"] == OBSERVATION_MODE
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["next_route"] == NEXT_ROUTE


def test_dynamic_strategy_research_only_shadow_observation_dry_run_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_research_only_shadow_observation_dry_run"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-research-only-shadow-observation-dry-run"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "observation_dry_run_result.json" in item for item in entry["artifact_globs"]
    )
    assert any(
        "no_side_effect_evidence.json" in item for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_research_only_shadow_observation_dry_run" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert "dynamic-strategy-research-only-shadow-observation-dry-run" in Path(
        "docs/system_flow.md"
    ).read_text(encoding="utf-8")
    assert (
        "TRADING-2369_DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )
    assert READY_STATUS in Path(
        "docs/requirements/"
        "TRADING-2369_Dynamic_Strategy_Research_Only_Shadow_Observation_Dry_Run.md"
    ).read_text(encoding="utf-8")


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True, exist_ok=True)
    comparison = _candidate_review_comparison()
    protocol = {
        "status": SOURCE_2368_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2368_ROUTE,
        "primary_observation_candidate": PRIMARY_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE,
        "gate_decision_from_2367": OWNER_REVIEW_REQUIRED,
        "primary_execution_cadence": "valid_until_window",
        "research_only_shadow_observation_allowed": True,
        "observation_field_schema_ready": True,
        "review_thresholds_ready": True,
        "observation_protocol": {
            "mode": "RESEARCH_ONLY",
            "primary_candidate": PRIMARY_CANDIDATE,
            "comparison_candidate": RANKING_TOP,
            "comparison_candidates": ["static_baseline", RANKING_TOP, PRIMARY_CANDIDATE],
            "gate_decision": OWNER_REVIEW_REQUIRED,
        },
        "data_quality": {
            "status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            "warning_count": 1,
        },
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        "paper_shadow_enabled": False,
        "event_append_enabled": False,
        "outcome_binding_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
    }
    documents = {
        "observation_protocol": protocol,
        "observation_field_schema": {
            "status": SOURCE_2368_READY,
            "observation_field_schema": {
                "mode": "RESEARCH_ONLY",
                "field_sections": [
                    {"section": "identity", "fields": ["observation_id"]},
                    {"section": "review", "fields": ["observation_decision"]},
                ],
                "append_event_allowed": False,
                "bind_outcome_allowed": False,
                "paper_trade_allowed": False,
                "broker_action_allowed": False,
            },
        },
        "review_thresholds": {
            "status": SOURCE_2368_READY,
            "review_thresholds": {
                "mode": "RESEARCH_ONLY",
                "owner_review_triggers": [
                    {"trigger_id": "guardrail_trigger", "action": "HARD_FAIL"}
                ],
                "hard_fail_conditions": ["paper_shadow_enabled_true"],
            },
        },
        "owner_review_gate": {
            "status": SOURCE_2367_READY,
            "as_of": "2026-07-05",
            "next_route": (
                "TRADING-2368_Dynamic_Strategy_Research_Only_Shadow_"
                "Observation_Protocol"
            ),
            "ranking_top_from_2365": RANKING_TOP,
            "robustness_top_from_2366": PRIMARY_CANDIDATE,
            "recommended_gate_candidate": PRIMARY_CANDIDATE,
            "recommended_gate_decision": OWNER_REVIEW_REQUIRED,
            "research_only_shadow_observation_allowed": True,
            "primary_execution_cadence": "valid_until_window",
            "candidate_review_comparison": comparison,
            "data_quality": {
                "status": "PASS_WITH_WARNINGS",
                "error_count": 0,
                "warning_count": 1,
            },
            "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
            "paper_shadow_enabled": False,
            "event_append_enabled": False,
            "outcome_binding_enabled": False,
            "production_enabled": False,
            "broker_action_enabled": False,
        },
        "candidate_owner_review_comparison": {
            "status": SOURCE_2367_READY,
            "candidate_review_comparison": comparison,
        },
        "sensitivity_result": {
            "status": SOURCE_2366_READY,
            "as_of": "2026-07-05",
            "primary_execution_cadence": "valid_until_window",
            "summary": {"top_candidate_after_sensitivity": PRIMARY_CANDIDATE},
            "data_quality": {
                "status": "PASS_WITH_WARNINGS",
                "error_count": 0,
                "warning_count": 1,
            },
            "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
            "paper_shadow_allowed": False,
        },
        "event_retest": {
            "status": SOURCE_2365_READY,
            "as_of": "2026-07-05",
            "primary_execution_cadence": "valid_until_window",
            "summary": {"top_candidate": RANKING_TOP},
            "data_quality": {
                "status": "PASS_WITH_WARNINGS",
                "error_count": 0,
                "warning_count": 1,
            },
            "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
            "paper_shadow_allowed": False,
        },
    }
    paths = {
        "observation_protocol": source_root / "observation_protocol.json",
        "observation_field_schema": source_root / "observation_field_schema.json",
        "review_thresholds": source_root / "review_thresholds.json",
        "owner_review_gate": source_root / "owner_review_gate_result.json",
        "candidate_owner_review_comparison": (
            source_root / "candidate_owner_review_comparison.json"
        ),
        "sensitivity_result": source_root / "sensitivity_result.json",
        "event_retest": source_root / "event_driven_retest_result.json",
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
            "candidate_id": PRIMARY_CANDIDATE,
            "roles": ["robustness_top_from_2366", "current_dynamic_default"],
            "review_priority_rank": 1,
            "cost_adjusted_return": 0.194762,
            "dynamic_vs_static_gap": 0.002205,
            "max_drawdown": -0.122866,
            "turnover": 2.04,
            "transaction_cost_drag": 0.00102,
            "slippage_drag": 0.00102,
            "constraint_hit_count": 0,
            "stale_signal_count": 0,
            "cooldown_fragility": "NOT_SEVERE",
            "turnover_acceptable_after_2366": False,
            "decision_after_2366": OWNER_REVIEW_REQUIRED,
        },
        {
            "candidate_id": RANKING_TOP,
            "roles": ["ranking_top_from_2365"],
            "review_priority_rank": 2,
            "cost_adjusted_return": 0.213859,
            "dynamic_vs_static_gap": 0.021302,
            "max_drawdown": -0.183642,
            "turnover": 1.964574,
            "transaction_cost_drag": 0.000983,
            "slippage_drag": 0.000983,
            "constraint_hit_count": 0,
            "stale_signal_count": 0,
            "cooldown_fragility": "NOT_SEVERE",
            "turnover_acceptable_after_2366": False,
            "decision_after_2366": OWNER_REVIEW_REQUIRED,
        },
        {
            "candidate_id": "static_baseline",
            "roles": ["static_baseline"],
            "review_priority_rank": 3,
            "cost_adjusted_return": 0.192557,
            "dynamic_vs_static_gap": 0,
            "max_drawdown": -0.140068,
            "turnover": 0,
            "constraint_hit_count": 0,
            "stale_signal_count": 0,
            "cooldown_fragility": "NOT_APPLICABLE_STATIC_BASELINE",
            "turnover_acceptable_after_2366": None,
            "decision_after_2366": "STATIC_BASELINE_REFERENCE",
        },
    ]
