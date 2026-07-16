from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path

from ai_trading_system.ops_daily import build_daily_ops_plan, run_daily_ops_plan
from ai_trading_system.scheduled_tasks import (
    DAILY_CADENCE_ID,
    NON_DAILY_CADENCE_IDS,
    load_scheduled_tasks_config,
    scheduled_daily_step_ids,
    scheduled_non_daily_task_ids,
    scheduled_safety_issues,
)


def test_scheduled_tasks_config_registers_required_cadences_and_safety() -> None:
    config = load_scheduled_tasks_config()

    assert config.policy_version == "scheduled_tasks_v2"
    cadence_ids = {cadence.cadence_id for cadence in config.cadences}
    assert DAILY_CADENCE_ID in cadence_ids
    assert set(NON_DAILY_CADENCE_IDS).issubset(cadence_ids)
    assert scheduled_safety_issues(config) == ()
    tasks_by_id = config.tasks_by_id()
    assert tasks_by_id["daily_download_data"].max_attempts == 2
    assert tasks_by_id["daily_validate_data"].max_attempts == 1
    assert {
        "weekly_backtest",
        "weekly_backtest_robustness",
        "weekly_parameter_replay",
        "weekly_parameter_candidates",
        "weekly_parameter_governance",
        "weekly_weight_candidate_evaluation",
        "weekly_weight_promotion_gate",
        "weekly_research_governance_summary_review",
        "weekly_dynamic_v3_rescue_artifact_validate",
        "weekly_dynamic_v3_rescue_artifact_stale",
        "weekly_dynamic_v3_rescue_governance_validate",
        "weekly_dynamic_v3_rescue_research_index",
        "weekly_dynamic_v3_rescue_shadow_monitor",
        "biweekly_investment_review",
        "biweekly_feedback_loop_review",
        "biweekly_shadow_lane_review",
        "biweekly_sec_pit_observe_only_review",
        "biweekly_manual_thesis_review",
        "biweekly_manual_risk_review",
        "monthly_documentation_contract_audit",
        "monthly_artifact_catalog_review",
        "monthly_report_registry_audit",
        "monthly_data_source_coverage_review",
        "monthly_pit_coverage_review",
        "monthly_long_window_backtest_review",
        "ad_hoc_sec_pit_historical_backfill",
        "ad_hoc_sec_pit_cognitive_evaluation",
        "ad_hoc_sec_pit_baseline_comparison",
        "ad_hoc_sec_pit_diagnostics",
        "ad_hoc_sec_pit_candidate_review",
        "ad_hoc_large_parameter_search",
        "ad_hoc_cache_only_replay_window",
        "ad_hoc_dynamic_v3_rescue_data_audit",
        "ad_hoc_dynamic_v3_rescue_profile_validate",
        "ad_hoc_dynamic_v3_rescue_small_real_sweep",
        "ad_hoc_dynamic_v3_rescue_injection_audit",
        "ad_hoc_dynamic_v3_rescue_candidate_attribution",
        "ad_hoc_dynamic_v3_rescue_walk_forward_selection",
        "ad_hoc_dynamic_v3_rescue_overfit",
        "ad_hoc_dynamic_v3_rescue_promotion_pack",
    }.issubset(set(scheduled_non_daily_task_ids(config)))


def test_daily_plan_matches_required_scheduled_order() -> None:
    config = load_scheduled_tasks_config()
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        skip_risk_event_openai_precheck=True,
    )

    assert tuple(step.step_id for step in plan.steps) == scheduled_daily_step_ids(
        config, is_trading_day=True
    )
    command_text = "\n".join(" ".join(step.command) for step in plan.steps if step.command)
    for expected in (
        "download-data",
        "validate-data",
        "pit-snapshots fetch-fmp-forward",
        "pit-snapshots build-manifest",
        "pit-snapshots validate",
        "fundamentals download-sec-companyfacts",
        "fundamentals extract-sec-metrics",
        "fundamentals merge-tsm-ir-sec-metrics",
        "fundamentals validate-sec-metrics",
        "valuation fetch-fmp",
        "score-daily",
        "reports dashboard",
        "sec-pit shadow-observe --latest",
        "sec-pit shadow-monitor --latest",
        "reports score-change-attribution --latest",
        "reports market-panel --latest",
        "data freshness --latest",
        "data recover-freshness --latest",
        "portfolio track-candidate --latest",
        "portfolio review-tracking --latest --show-window-progress",
        "reports portfolio-tracking-review --latest",
        "reports artifact-lineage --latest",
        "reports validate-artifact-lineage --latest",
        "reports index --latest",
        "docs report-contract --latest",
        "reports research-governance-summary --latest",
        "reports reader-brief --latest",
        "reports quality-gate --latest",
        "reports validate-reader-brief --latest",
        "etf dynamic-v3-rescue schedule observe",
        "ops health",
        "security scan-secrets",
    ):
        assert expected in command_text


def test_non_daily_tasks_are_registered_but_not_in_daily_plan() -> None:
    config = load_scheduled_tasks_config()
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        skip_risk_event_openai_precheck=True,
    )

    daily_step_ids = {step.step_id for step in plan.steps}
    assert daily_step_ids.isdisjoint(set(scheduled_non_daily_task_ids(config)))
    daily_commands = {" ".join(step.command) for step in plan.steps if step.command}
    assert "aits feedback build-parameter-replay --as-of {as_of}" not in daily_commands
    assert "python scripts/run_weight_candidate_evaluation.py --date {as_of}" not in daily_commands
    assert "aits ops replay-window --mode cache-only" not in daily_commands
    assert not any("dynamic-v3-rescue sweep run-profile" in command for command in daily_commands)
    assert not any("dynamic-v3-rescue promotion pack" in command for command in daily_commands)
    assert any("dynamic-v3-rescue schedule observe" in command for command in daily_commands)


def test_dynamic_v3_rescue_scheduled_tasks_are_condition_gated() -> None:
    config = load_scheduled_tasks_config()
    tasks_by_id = config.tasks_by_id()
    daily_gate = tasks_by_id["daily_dynamic_v3_rescue_schedule_observe"]

    assert daily_gate.cadence == DAILY_CADENCE_ID
    assert daily_gate.daily_plan_step_id == "dynamic_v3_rescue_schedule_observe"
    assert daily_gate.manual_review_required is True
    assert daily_gate.date_gate
    assert daily_gate.trigger_condition
    assert daily_gate.data_quality_gate
    assert "schedule observe" in daily_gate.command

    dynamic_non_daily = [
        task for task in config.non_daily_tasks() if "dynamic-v3-rescue" in task.command
    ]
    assert dynamic_non_daily
    for task in dynamic_non_daily:
        assert task.production_effect == "none"
        assert task.production_weight_write is False
        assert task.active_shadow_weight_write is False
        assert task.broker_action is False
        assert task.trading_action is False
        assert task.manual_review_required is True
        assert task.date_gate
        assert task.trigger_condition
        assert task.data_quality_gate


def test_closed_market_skips_score_and_reader_artifacts_but_keeps_data_refresh(
    tmp_path: Path,
) -> None:
    _write_price_cache(tmp_path / "data" / "raw" / "prices_daily.csv", "2026-05-08")
    _write_price_cache(
        tmp_path / "data" / "raw" / "prices_marketstack_daily.csv",
        "2026-05-08",
    )

    plan = build_daily_ops_plan(as_of=date(2026, 5, 10), project_root=tmp_path)
    step_by_id = {step.step_id: step for step in plan.steps}

    assert step_by_id["score_daily"].enabled is False
    assert "official_policy_sources" in step_by_id
    assert "official_policy_sources" in scheduled_daily_step_ids(
        load_scheduled_tasks_config(), is_trading_day=False
    )
    assert "official_policy_sources" not in scheduled_daily_step_ids(
        load_scheduled_tasks_config(), is_trading_day=True
    )
    for step_id in (
        "reports_dashboard",
        "score_change_attribution",
        "market_panel",
        "market_data_freshness",
        "market_data_recover_freshness",
        "portfolio_candidate_tracking",
        "portfolio_tracking_review",
        "portfolio_tracking_review_report",
        "artifact_lineage",
        "validate_artifact_lineage",
        "report_index",
        "documentation_contract",
        "research_governance_summary",
        "reader_brief",
        "report_quality_gate",
        "validate_reader_brief",
    ):
        assert step_by_id[step_id].enabled is False
    for step_id in (
        "validate_data",
        "pit_snapshots_fetch_fmp_forward",
        "pit_snapshots_build_manifest",
        "pit_snapshots_validate",
        "sec_companyfacts",
        "sec_metrics",
        "tsm_ir_sec_metrics_merge",
        "sec_metrics_validation",
        "valuation_snapshots",
        "dynamic_v3_rescue_schedule_observe",
        "pipeline_health",
    ):
        assert step_by_id[step_id].enabled is True
    assert step_by_id["pipeline_health"].command[-1] == "--non-trading-day"


def test_daily_run_executes_forward_evidence_then_reader_brief_chain_after_score_daily(
    tmp_path: Path,
) -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        project_root=tmp_path,
        include_download_data=False,
        include_pit_snapshots=False,
        include_sec_fundamentals=False,
        include_valuation_snapshots=False,
        include_secret_scan=False,
        skip_risk_event_openai_precheck=True,
    )
    _write_status(
        next(step for step in plan.steps if step.step_id == "validate_data").produced_paths[0]
    )
    score_step = next(step for step in plan.steps if step.step_id == "score_daily")
    _write_status(score_step.produced_paths[2])
    _write_status(score_step.produced_paths[4])
    _write_status(
        next(step for step in plan.steps if step.step_id == "pipeline_health").produced_paths[0]
    )
    calls: list[tuple[str, ...]] = []

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command[3:])
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )

    assert report.status == "PASS_WITH_SKIPS"
    score_index = calls.index(
        (
            "score-daily",
            "--as-of",
            "2026-05-06",
            "--skip-risk-event-openai-precheck",
        )
    )
    assert calls[score_index + 1 : score_index + 24] == [
        ("forward-evidence", "capture-dry-run-daily", "--as-of", "2026-05-06"),
        ("reports", "dashboard", "--as-of", "2026-05-06"),
        ("sec-pit", "shadow-observe", "--latest", "--end", "2026-05-06"),
        ("sec-pit", "shadow-monitor", "--latest", "--as-of", "2026-05-06"),
        ("reports", "score-change-attribution", "--latest"),
        ("reports", "market-panel", "--latest"),
        ("data", "freshness", "--latest"),
        ("data", "recover-freshness", "--latest"),
        ("portfolio", "track-candidate", "--latest"),
        ("portfolio", "review-tracking", "--latest", "--show-window-progress"),
        ("reports", "portfolio-tracking-review", "--latest"),
        ("etf", "forward", "update", "--latest"),
        ("etf", "forward", "dashboard", "--latest"),
        ("etf", "forward", "watchlist", "--latest"),
        ("reports", "artifact-lineage", "--latest"),
        ("reports", "validate-artifact-lineage", "--latest"),
        ("reports", "index", "--latest"),
        ("docs", "report-contract", "--latest"),
        ("reports", "research-governance-summary", "--latest"),
        ("reports", "reader-brief", "--latest"),
        ("reports", "quality-gate", "--latest"),
        ("reports", "validate-reader-brief", "--latest"),
        ("etf", "dynamic-v3-rescue", "schedule", "observe", "--as-of", "2026-05-06"),
    ]


def _write_price_cache(path: Path, latest_date: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"date,ticker,open,high,low,close,adj_close,volume\n{latest_date},NVDA,1,1,1,1,1,100\n",
        encoding="utf-8",
    )


def _write_status(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("- 状态：PASS\n", encoding="utf-8")
