from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.dynamic_strategy_research_only_observation_owner_decision import (
    OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_research_only_observation_report_dry_run import (
    NEXT_ROUTE,
    READY_STATUS,
    REPORT_MODE,
    run_dynamic_strategy_research_only_observation_report_dry_run,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

PRIMARY_CANDIDATE = "dynamic_regime_overlay_v0_4_lower_turnover"
RANKING_TOP = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
SOURCE_2369_READY = "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_READY"
SOURCE_2370_READY = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_"
    "NO_SIDE_EFFECT_VALIDATION_READY"
)
SOURCE_2371_READY = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_"
    "OWNER_REVIEW_DECISION_READY"
)
SOURCE_2372_READY = (
    "DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_"
    "LOG_SCHEMA_AND_REPORT_PLAN_READY"
)
SOURCE_2369_ROUTE = (
    "TRADING-2370_Dynamic_Strategy_Research_Only_Shadow_Observation_"
    "Replay_No_Side_Effect_Validation"
)
SOURCE_2370_ROUTE = (
    "TRADING-2371_Dynamic_Strategy_Research_Only_Shadow_Observation_"
    "Owner_Review_Decision"
)
SOURCE_2371_ROUTE = (
    "TRADING-2372_Dynamic_Strategy_Research_Only_Observation_"
    "Log_Schema_And_Report_Plan"
)
SOURCE_2372_ROUTE = (
    "TRADING-2373_Dynamic_Strategy_Research_Only_Observation_Report_Dry_Run"
)


def test_dynamic_strategy_research_only_observation_report_dry_run_builder(
    tmp_path: Path,
) -> None:
    sources = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "report_dry_run"
    docs_root = tmp_path / "docs" / "research"

    payload = run_dynamic_strategy_research_only_observation_report_dry_run(
        source_owner_review_decision_path=sources["owner_decision"],
        source_log_schema_plan_path=sources["log_schema_plan"],
        source_observation_dry_run_result_path=sources["dry_run_result"],
        source_observation_dry_run_record_path=sources["dry_run_record"],
        source_replay_validation_path=sources["replay_validation"],
        output_root=output_root,
        docs_root=docs_root,
        as_of_date=date(2026, 7, 5),
    )

    assert payload["status"] == READY_STATUS
    assert payload["source_ready_for_report_dry_run"] is True
    assert payload["primary_observation_candidate"] == PRIMARY_CANDIDATE
    assert payload["ranking_top_from_2365"] == RANKING_TOP
    assert payload["robustness_top_from_2366"] == PRIMARY_CANDIDATE
    assert payload["report_mode"] == REPORT_MODE
    assert payload["observation_record_example_ready"] is True
    assert payload["observation_report_dry_run_ready"] is True
    assert payload["no_side_effect_evidence_ready"] is True
    assert payload["owner_decision_from_2371"] == OWNER_DECISION
    assert payload["schema_ready_from_2372"] is True
    assert payload["report_plan_ready_from_2372"] is True
    assert payload["stable_semantic_replay_passed_from_2370"] is True
    assert payload["recommended_next_research_task"] == NEXT_ROUTE
    assert payload["next_route"] == NEXT_ROUTE
    assert payload["data_quality_gate_executed"] is False
    assert payload["data_quality_gate_reason"] == (
        "NOT_APPLICABLE_PRIOR_ARTIFACT_REPORT_DRY_RUN_ONLY_NO_FRESH_MARKET_DATA"
    )

    for key in (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "scheduler_enabled",
        "event_append_enabled",
        "event_append_attempted",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
        "periodic_daily_report_generated",
    ):
        assert payload[key] is False
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"

    record = payload["observation_record_example"]
    assert set(record) == {
        "identity",
        "candidate_context",
        "signal_context",
        "portfolio_preview",
        "cost_turnover",
        "comparison",
        "review",
        "guardrails",
    }
    assert record["identity"]["generated_by_task"] == "TRADING-2373"
    assert record["candidate_context"]["owner_decision_from_2371"] == OWNER_DECISION
    assert record["signal_context"]["valid_until_window_state"] == (
        "NOT_COMPUTED_NO_FRESH_MARKET_DATA"
    )
    assert record["portfolio_preview"]["no_trade_reason"] == (
        "RESEARCH_ONLY_DRY_RUN_NO_EXECUTION"
    )
    assert record["cost_turnover"]["turnover_cap_state"] == (
        "OWNER_REVIEW_REQUIRED_TURNOVER_NOT_ACCEPTABLE_AFTER_2366"
    )
    assert record["comparison"]["dynamic_vs_static_preview_gap"] == 0.002205
    assert record["review"]["observation_decision"] == "OWNER_REVIEW_REQUIRED"
    assert record["guardrails"]["daily_report_generated"] is False

    report = payload["observation_report_dry_run"]
    assert report["report_mode"] == REPORT_MODE
    assert report["sections"] == _report_sections()
    assert report["section_payloads"]["Explicit non-goals"]["not_daily_report"] is True
    assert report["section_payloads"]["Guardrail summary"]["event_append_enabled"] is False

    evidence = payload["no_side_effect_evidence"]
    assert evidence["status"] == "PASS"
    assert evidence["manual_report_dry_run_only"] is True
    assert evidence["observation_record_example_only"] is True
    assert evidence["event_append_attempted"] is False
    assert evidence["outcome_binding_attempted"] is False
    assert evidence["daily_report_generated"] is False

    for key in (
        "json_path",
        "observation_record_example_json",
        "no_side_effect_evidence_json",
        "markdown_path",
        "observation_record_example_markdown",
        "next_route_markdown",
    ):
        assert Path(payload["artifact_paths"][key]).exists()


def test_dynamic_strategy_research_only_observation_report_dry_run_cli(
    tmp_path: Path,
) -> None:
    sources = _write_source_artifacts(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "report_dry_run_cli"
    docs_root = tmp_path / "docs" / "research"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "dynamic-strategy-research-only-observation-report-dry-run",
            "--source-owner-review-decision",
            str(sources["owner_decision"]),
            "--source-log-schema-plan",
            str(sources["log_schema_plan"]),
            "--source-observation-dry-run-result",
            str(sources["dry_run_result"]),
            "--source-observation-dry-run-record",
            str(sources["dry_run_record"]),
            "--source-replay-validation",
            str(sources["replay_validation"]),
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
    assert (output_root / "observation_report_dry_run_result.json").exists()
    assert (output_root / "observation_record_example.json").exists()
    assert (output_root / "no_side_effect_evidence.json").exists()

    payload = json.loads(
        (output_root / "observation_report_dry_run_result.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["observation_report_dry_run_ready"] is True
    assert payload["report_mode"] == REPORT_MODE
    assert payload["next_route"] == NEXT_ROUTE


def test_dynamic_strategy_research_only_observation_report_dry_run_registry_and_docs() -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    entry = entries["dynamic_strategy_research_only_observation_report_dry_run"]

    assert entry["command"] == (
        "aits research strategies "
        "dynamic-strategy-research-only-observation-report-dry-run"
    )
    assert entry["artifact_selection_policy"] == "latest_available"
    assert entry["required_for_daily_reading"] is False
    assert entry["production_effect"] == "none"
    assert entry["broker_action"] == "none"
    assert any(
        "observation_report_dry_run_result.json" in item
        for item in entry["artifact_globs"]
    )
    assert any(
        "observation_record_example.json" in item for item in entry["artifact_globs"]
    )

    assert "dynamic_strategy_research_only_observation_report_dry_run" in Path(
        "docs/artifact_catalog.md"
    ).read_text(encoding="utf-8")
    assert (
        "dynamic-strategy-research-only-observation-report-dry-run"
        in Path("docs/system_flow.md").read_text(encoding="utf-8")
    )
    assert (
        "TRADING-2373_DYNAMIC_STRATEGY_RESEARCH_ONLY_OBSERVATION_REPORT_DRY_RUN"
        in Path("docs/task_register.md").read_text(encoding="utf-8")
    )


def _write_source_artifacts(tmp_path: Path) -> dict[str, Path]:
    root = tmp_path / "source"
    paths = {
        "owner_decision": root / "owner_review_decision_result.json",
        "log_schema_plan": root / "log_schema_plan_result.json",
        "dry_run_result": root / "observation_dry_run_result.json",
        "dry_run_record": root / "observation_dry_run_record.json",
        "replay_validation": root / "replay_validation_result.json",
    }
    payloads = {
        "owner_decision": _owner_decision(),
        "log_schema_plan": _log_schema_plan(),
        "dry_run_result": _dry_run_result(),
        "dry_run_record": _dry_run_record_doc(),
        "replay_validation": _replay_validation(),
    }
    root.mkdir(parents=True, exist_ok=True)
    for key, path in paths.items():
        path.write_text(
            json.dumps(payloads[key], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return paths


def _owner_decision() -> dict[str, object]:
    return {
        "task_id": "TRADING-2371",
        "status": SOURCE_2371_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2371_ROUTE,
        "owner_review_decision_recorded": True,
        "owner_decision": OWNER_DECISION,
        "research_only_observation_continue_allowed": True,
        "primary_observation_candidate": PRIMARY_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE,
        **_common_context(),
        **_disabled_flags(),
    }


def _log_schema_plan() -> dict[str, object]:
    return {
        "task_id": "TRADING-2372",
        "status": SOURCE_2372_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2372_ROUTE,
        "primary_observation_candidate": PRIMARY_CANDIDATE,
        "observation_log_schema_ready": True,
        "observation_report_plan_ready": True,
        "schema_only": True,
        "report_plan_only": True,
        "periodic_daily_report_generated": False,
        "event_log_written": False,
        "observation_report_plan": {
            "sections": _report_sections(),
            "report_mode": "RESEARCH_ONLY_MANUAL_REPORT_PLAN",
        },
        **_common_context(),
        **_disabled_flags(),
    }


def _dry_run_result() -> dict[str, object]:
    return {
        "task_id": "TRADING-2369",
        "status": SOURCE_2369_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2369_ROUTE,
        "observation_mode": "RESEARCH_ONLY_DRY_RUN",
        "primary_observation_candidate": PRIMARY_CANDIDATE,
        "ranking_top_from_2365": RANKING_TOP,
        "robustness_top_from_2366": PRIMARY_CANDIDATE,
        "execution_cadence": "valid_until_window",
        "gate_decision_from_2367": "OWNER_REVIEW_REQUIRED",
        "observation_decision": "OWNER_REVIEW_REQUIRED",
        "owner_review_required": True,
        "observation_dry_run_record_ready": True,
        "no_side_effect_evidence_ready": True,
        **_common_context(),
        **_disabled_flags(),
    }


def _dry_run_record_doc() -> dict[str, object]:
    return {
        "report_type": "dynamic_strategy_shadow_observation_dry_run_record",
        "status": SOURCE_2369_READY,
        "observation_dry_run_record": {
            "identity": {
                "observation_id": f"TRADING-2369_2026-07-05_{PRIMARY_CANDIDATE}",
                "as_of": "2026-07-05",
                "source_task": "TRADING-2369",
                "candidate_id": PRIMARY_CANDIDATE,
                "candidate_version": "source_artifact_candidate_from_trading_2367",
                "execution_cadence": "valid_until_window",
            },
            "signal_state": {
                "signal_state": "SOURCE_ARTIFACT_PREVIEW_ONLY_NOT_RECOMPUTED",
                "advisory_valid_from": "2026-07-05",
                "advisory_valid_until": "NOT_COMPUTED_NO_FRESH_MARKET_DATA",
                "signal_horizon": "valid_until_window",
            },
            "portfolio_preview": {
                "reference_weight": None,
                "proposed_research_weight": None,
                "proposed_weight_delta": None,
                "risk_cap_state": "NOT_RECOMPUTED_PRIOR_ARTIFACT_DRY_RUN",
                "constraint_state": (
                    "NO_SOURCE_CONSTRAINT_OR_STALE_SIGNAL_HITS_IN_PRIOR_ARTIFACT"
                ),
                "cooldown_state": "NOT_SEVERE",
                "no_trade_reason": "RESEARCH_ONLY_DRY_RUN_NO_EXECUTION",
            },
            "cost_and_turnover": {
                "expected_turnover": 2.04,
                "transaction_cost_bps": 10.2,
                "slippage_bps": 10.2,
                "estimated_cost_drag": 0.00204,
                "turnover_cap_state": (
                    "OWNER_REVIEW_REQUIRED_TURNOVER_NOT_ACCEPTABLE_AFTER_2366"
                ),
            },
            "comparison": {
                "static_baseline_comparison": {
                    "candidate_id": "static_baseline",
                    "decision": "STATIC_BASELINE_REFERENCE",
                    "dynamic_vs_static_gap": 0.0,
                },
                "ranking_top_candidate_comparison": {
                    "candidate_id": RANKING_TOP,
                    "decision": "OWNER_REVIEW_REQUIRED",
                    "dynamic_vs_static_gap": 0.021302,
                },
                "robustness_top_candidate_comparison": {
                    "candidate_id": PRIMARY_CANDIDATE,
                    "decision": "OWNER_REVIEW_REQUIRED",
                    "dynamic_vs_static_gap": 0.002205,
                },
                "dynamic_vs_static_preview_gap": 0.002205,
            },
            "review": {
                "observation_decision": "OWNER_REVIEW_REQUIRED",
                "owner_review_required": True,
                "review_reason": "ranking and robustness divergence remains",
                "escalation_flag": "OWNER_REVIEW_REQUIRED",
            },
        },
    }


def _replay_validation() -> dict[str, object]:
    return {
        "task_id": "TRADING-2370",
        "status": SOURCE_2370_READY,
        "as_of": "2026-07-05",
        "next_route": SOURCE_2370_ROUTE,
        "stable_semantic_replay_passed": True,
        "no_side_effect_evidence_ready": True,
        "observation_decision": "OWNER_REVIEW_REQUIRED",
        "owner_review_required": True,
        **_common_context(),
        **_disabled_flags(),
    }


def _common_context() -> dict[str, object]:
    return {
        "requested_date_range": {"start": "2022-12-01", "end": "2026-07-05"},
        "data_quality": {
            "status": "PASS_WITH_WARNINGS",
            "error_count": 0,
            "warning_count": 1,
        },
        "production_effect": "none",
        "broker_action": "none",
    }


def _disabled_flags() -> dict[str, object]:
    return {
        "paper_shadow_enabled": False,
        "paper_shadow_approved": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "event_append_enabled": False,
        "event_append_approved": False,
        "event_append_attempted": False,
        "outcome_binding_enabled": False,
        "outcome_binding_approved": False,
        "outcome_binding_attempted": False,
        "scheduler_enabled": False,
        "production_enabled": False,
        "broker_action_enabled": False,
        "broker_action_attempted": False,
        "order_generated": False,
        "daily_report_generated": False,
    }


def _report_sections() -> list[str]:
    return [
        "Executive summary",
        "Candidate under observation",
        "Signal / valid-until status",
        "Portfolio preview",
        "Static baseline comparison",
        "Ranking top vs robustness top comparison",
        "Cost / turnover / cooldown status",
        "Review flags",
        "Guardrail summary",
        "Explicit non-goals",
    ]
