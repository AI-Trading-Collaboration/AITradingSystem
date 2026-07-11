from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.cli_commands.ops as ops_cli
from ai_trading_system.cli import app
from ai_trading_system.core import ProductionEffect
from ai_trading_system.ops_daily import (
    DailyOpsPlan,
    DailyOpsRunMetadata,
    DailyOpsRunReport,
    DailyOpsStep,
    DailyOpsStepResult,
    _execution_command,
    _purge_source_pycache_dirs,
    build_daily_ops_plan,
    daily_ops_run_metadata_path_for_report,
    daily_ops_step_result_to_workflow_step_result,
    daily_ops_step_to_workflow_step,
    render_daily_ops_plan,
    render_daily_ops_run_report,
    resolve_daily_ops_default_as_of,
    run_daily_ops_plan,
    write_daily_ops_run_report,
)
from ai_trading_system.trading_calendar import us_equity_market_session


def test_daily_ops_plan_reports_missing_required_env() -> None:
    plan = build_daily_ops_plan(as_of=date(2026, 5, 6))
    missing = plan.missing_env_vars(
        {
            "FMP_API_KEY": "",
            "MARKETSTACK_API_KEY": "",
            "OPENAI_API_KEY": "",
        }
    )

    assert plan.status({"FMP_API_KEY": "", "MARKETSTACK_API_KEY": ""}) == "BLOCKED_ENV"
    assert missing == (
        "FMP_API_KEY",
        "MARKETSTACK_API_KEY",
        "OPENAI_API_KEY",
        "SEC_USER_AGENT",
    )
    markdown = render_daily_ops_plan(plan, env={})
    pit_command = "`aits pit-snapshots fetch-fmp-forward --as-of 2026-05-06 --continue-on-failure`"
    assert "状态：BLOCKED_ENV" in markdown
    assert "`aits download-data --start 2018-01-01 --end 2026-05-06`" in markdown
    assert "download_data_diagnostics_2026-05-06.md" in markdown
    assert "失败时写入脱敏 download_data_diagnostics 报告" in markdown
    assert "`aits validate-data --as-of 2026-05-06`" in markdown
    assert pit_command in markdown
    assert "`aits pit-snapshots build-manifest --as-of 2026-05-06`" in markdown
    assert "`aits pit-snapshots validate --as-of 2026-05-06`" in markdown
    assert "`aits fundamentals download-sec-companyfacts`" in markdown
    assert "`aits fundamentals extract-sec-metrics --as-of 2026-05-06`" in markdown
    assert "`aits fundamentals merge-tsm-ir-sec-metrics --as-of 2026-05-06`" in markdown
    assert "`aits fundamentals validate-sec-metrics --as-of 2026-05-06`" in markdown
    assert "`aits valuation fetch-fmp --as-of 2026-05-06`" in markdown
    assert "`aits score-daily --as-of 2026-05-06" in markdown
    assert "--llm-request-profile risk_event_daily_official_precheck" in markdown
    assert "`aits reports dashboard --as-of 2026-05-06`" in markdown
    assert "`aits sec-pit shadow-observe --latest --end 2026-05-06`" in markdown
    assert "`aits sec-pit shadow-monitor --latest --as-of 2026-05-06`" in markdown
    assert "`aits reports score-change-attribution --latest`" in markdown
    assert "`aits reports market-panel --latest`" in markdown
    assert "`aits reports index --latest`" in markdown
    assert "`aits docs report-contract --latest`" in markdown
    assert "`aits reports research-governance-summary --latest`" in markdown
    assert "`aits reports reader-brief --latest`" in markdown
    assert "`aits reports validate-reader-brief --latest`" in markdown
    assert "`aits etf dynamic-v3-rescue schedule observe --as-of 2026-05-06`" in markdown
    assert "`live_provider`" in markdown
    assert "`readonly`" in markdown
    assert "缺少关键环境变量时，后续真实执行器必须 fail closed" in markdown


def test_daily_ops_default_as_of_uses_latest_completed_us_market_day() -> None:
    observed_at = datetime(2026, 5, 12, 1, 44, tzinfo=UTC)

    assert resolve_daily_ops_default_as_of(observed_at) == date(2026, 5, 11)


def test_daily_ops_default_as_of_waits_for_provider_ready_buffer() -> None:
    observed_at = datetime(2026, 6, 29, 22, 31, tzinfo=UTC)

    assert resolve_daily_ops_default_as_of(observed_at) == date(2026, 6, 26)


def test_daily_ops_plan_cli_default_as_of_uses_market_resolver(monkeypatch) -> None:
    monkeypatch.setattr(
        ops_cli,
        "resolve_daily_ops_default_as_of",
        lambda observed_at=None: date(2026, 5, 11),
    )

    plan_date, plan = ops_cli._build_daily_ops_plan_from_cli_options(
        as_of=None,
        download_start="2018-01-01",
        include_download_data=False,
        include_pit_snapshots=False,
        include_sec_fundamentals=False,
        include_valuation_snapshots=False,
        include_secret_scan=False,
        risk_event_openai_precheck=False,
        risk_event_openai_precheck_max_candidates=None,
        llm_request_profile="risk_event_daily_official_precheck",
        full_universe=False,
    )

    assert plan_date == date(2026, 5, 11)
    assert plan.as_of == date(2026, 5, 11)


def test_execution_command_prefers_project_venv_python(tmp_path: Path) -> None:
    local_python = tmp_path / ".venv" / "Scripts" / "python.exe"
    local_python.parent.mkdir(parents=True)
    local_python.write_text("", encoding="utf-8")

    command = _execution_command(("aits", "score-daily", "--as-of", "2026-05-11"), tmp_path)

    assert command[:3] == (
        str(local_python),
        "-m",
        "ai_trading_system.cli_direct",
    )
    assert command[3:] == ("score-daily", "--as-of", "2026-05-11")


def test_daily_ops_plan_allows_explicit_openai_skip() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        skip_risk_event_openai_precheck=True,
    )
    env = {
        "FMP_API_KEY": "present",
        "MARKETSTACK_API_KEY": "present",
        "SEC_USER_AGENT": "AITradingSystem test@example.com",
    }
    markdown = render_daily_ops_plan(plan, env=env)

    assert plan.status(env) == "READY"
    assert plan.missing_env_vars(env) == ()
    assert "--skip-risk-event-openai-precheck" in markdown
    assert "OPENAI_API_KEY" not in plan.missing_env_vars(env)


def test_daily_ops_plan_threads_llm_request_profile_without_candidate_override() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        llm_request_profile="risk_event_triaged_official_candidates",
    )
    score_step = next(step for step in plan.steps if step.step_id == "score_daily")

    assert "--llm-request-profile" in score_step.command
    assert "risk_event_triaged_official_candidates" in score_step.command
    assert "--risk-event-openai-precheck-max-candidates" not in score_step.command


def test_daily_ops_plan_threads_run_id_into_score_daily() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        skip_risk_event_openai_precheck=True,
        run_id="daily_ops_run:2026-05-06:test",
    )
    score_step = next(step for step in plan.steps if step.step_id == "score_daily")

    assert score_step.command[-2:] == ("--run-id", "daily_ops_run:2026-05-06:test")
    assert "--run-id daily_ops_run:2026-05-06:test" in render_daily_ops_plan(
        plan,
        env={
            "FMP_API_KEY": "present",
            "MARKETSTACK_API_KEY": "present",
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
        },
    )


def test_daily_ops_run_injects_risk_event_openai_visibility_cutoff(
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
        skip_risk_event_openai_precheck=False,
    )
    for step in plan.steps:
        for path in step.produced_paths:
            if path.suffix.lower() != ".md":
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("- 状态：PASS\n", encoding="utf-8")

    calls: list[tuple[str, ...]] = []

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={
            "OPENAI_API_KEY": "present",
            "FMP_API_KEY": "present",
            "MARKETSTACK_API_KEY": "present",
        },
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
        visibility_latest_completed_trading_day=date(2026, 5, 6),
    )
    score_call = next(command for command in calls if "score-daily" in command)
    cutoff_index = score_call.index("--risk-event-openai-precheck-visibility-cutoff") + 1
    cutoff = datetime.fromisoformat(score_call[cutoff_index])
    score_result = next(result for result in report.step_results if result.step_id == "score_daily")
    metadata_score_result = next(
        result for result in report.metadata.step_results if result["step_id"] == "score_daily"
    )

    assert report.status == "PASS_WITH_SKIPS"
    assert cutoff.tzinfo is not None
    assert "--risk-event-openai-precheck-visibility-cutoff" in score_result.command
    assert "--risk-event-openai-precheck-visibility-cutoff" in metadata_score_result["command"]


def test_daily_ops_workflow_step_adapter_preserves_command_and_outputs(tmp_path: Path) -> None:
    output_path = tmp_path / "outputs" / "reports" / "daily_ops_run_2026-05-06.md"
    output_path.parent.mkdir(parents=True)
    output_path.write_text("# run\n", encoding="utf-8")
    step = DailyOpsStep(
        step_id="daily_ops_run",
        title="每日运行",
        command=("aits", "ops", "daily-run", "--as-of", "2026-05-06"),
        required_env_vars=("FMP_API_KEY",),
        produced_paths=(output_path,),
        quality_gate="daily-run report",
        blocks_downstream=True,
    )

    workflow_step = daily_ops_step_to_workflow_step(step)

    assert workflow_step.step_id == "daily_ops_run"
    assert workflow_step.name == "每日运行"
    assert workflow_step.command_name == "aits ops daily-run"
    assert workflow_step.command == step.command
    assert workflow_step.production_effect is ProductionEffect.NONE
    assert workflow_step.expected_outputs[0].path == output_path
    assert workflow_step.expected_outputs[0].exists is True
    assert workflow_step.blocking is True


def test_daily_ops_step_result_adapter_maps_status_and_artifacts(tmp_path: Path) -> None:
    output_path = tmp_path / "outputs" / "reports" / "daily_ops_run_2026-05-06.md"
    output_path.parent.mkdir(parents=True)
    output_path.write_text("# run\n", encoding="utf-8")
    started_at = datetime(2026, 5, 6, 21, 0, tzinfo=UTC)
    ended_at = datetime(2026, 5, 6, 21, 1, tzinfo=UTC)
    result = DailyOpsStepResult(
        step_id="daily_ops_run",
        title="每日运行",
        command=("aits", "ops", "daily-run"),
        status="PASS",
        return_code=0,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=60.0,
        produced_paths=(output_path,),
        blocks_downstream=True,
    )

    workflow_result = daily_ops_step_result_to_workflow_step_result(result)

    assert workflow_result.step_id == result.step_id
    assert workflow_result.status == "PASS"
    assert workflow_result.started_at == started_at
    assert workflow_result.finished_at == ended_at
    assert workflow_result.artifacts[0].path == output_path
    assert workflow_result.production_effect is ProductionEffect.NONE


def test_daily_ops_metadata_omits_failure_only_diagnostic_on_success(tmp_path: Path) -> None:
    output_path = tmp_path / "data" / "raw" / "prices_daily.csv"
    failure_report = tmp_path / "outputs" / "reports" / "download_data_diagnostics_2026-05-06.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("date,ticker,close\n", encoding="utf-8")
    step = DailyOpsStep(
        step_id="download_data",
        title="更新市场和宏观缓存",
        command=("aits", "download-data", "--start", "2018-01-01", "--end", "2026-05-06"),
        required_env_vars=(),
        produced_paths=(output_path,),
        failure_diagnostic_paths=(failure_report,),
        quality_gate="失败时写入脱敏 download_data_diagnostics 报告并停止下游。",
        blocks_downstream=True,
        input_visibility="live_provider",
    )
    plan = DailyOpsPlan(
        as_of=date(2026, 5, 6),
        generated_at=datetime(2026, 5, 6, 20, 0, tzinfo=UTC),
        steps=(step,),
        market_session=us_equity_market_session(date(2026, 5, 6)),
    )

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
        visibility_latest_completed_trading_day=date(2026, 5, 6),
    )
    produced_paths = {artifact.path for artifact in report.metadata.produced_artifacts}

    assert "download_data_diagnostics_2026-05-06.md" in render_daily_ops_plan(plan)
    assert report.status == "PASS"
    assert output_path in produced_paths
    assert failure_report not in produced_paths


def test_daily_ops_metadata_records_failure_only_diagnostic_when_generated(
    tmp_path: Path,
) -> None:
    output_path = tmp_path / "data" / "raw" / "prices_daily.csv"
    failure_report = tmp_path / "outputs" / "reports" / "download_data_diagnostics_2026-05-06.md"
    step = DailyOpsStep(
        step_id="download_data",
        title="更新市场和宏观缓存",
        command=("aits", "download-data", "--start", "2018-01-01", "--end", "2026-05-06"),
        required_env_vars=(),
        produced_paths=(output_path,),
        failure_diagnostic_paths=(failure_report,),
        quality_gate="失败时写入脱敏 download_data_diagnostics 报告并停止下游。",
        blocks_downstream=True,
        input_visibility="live_provider",
    )
    plan = DailyOpsPlan(
        as_of=date(2026, 5, 6),
        generated_at=datetime(2026, 5, 6, 20, 0, tzinfo=UTC),
        steps=(step,),
        market_session=us_equity_market_session(date(2026, 5, 6)),
    )

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        failure_report.parent.mkdir(parents=True, exist_ok=True)
        failure_report.write_text("# 下载诊断\n", encoding="utf-8")
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="failed")

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
        visibility_latest_completed_trading_day=date(2026, 5, 6),
    )
    produced_paths = {artifact.path for artifact in report.metadata.produced_artifacts}

    assert report.status == "FAIL"
    assert failure_report in produced_paths
    assert report.failed_step is not None
    assert report.failed_step.diagnostic_path in produced_paths


def test_daily_ops_step_result_adapter_maps_summary_statuses_to_workflow_statuses() -> None:
    base = {
        "step_id": "step",
        "title": "Step",
        "command": ("aits", "step"),
        "return_code": None,
        "started_at": None,
        "ended_at": None,
        "duration_seconds": None,
        "produced_paths": (),
        "blocks_downstream": False,
    }

    assert (
        daily_ops_step_result_to_workflow_step_result(
            DailyOpsStepResult(status="PASS_WITH_SKIPS", **base),
        ).status
        == "WARN"
    )
    assert (
        daily_ops_step_result_to_workflow_step_result(
            DailyOpsStepResult(status="BLOCKED_VISIBILITY", **base),
        ).status
        == "BLOCKED"
    )


def test_daily_ops_plan_includes_forward_evidence_after_score_daily() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        skip_risk_event_openai_precheck=True,
    )
    step_ids = [step.step_id for step in plan.steps]
    dashboard_step = next(step for step in plan.steps if step.step_id == "reports_dashboard")
    expected_after_score = [
        "forward_evidence_dry_run_daily",
        "reports_dashboard",
        "sec_pit_shadow_observe",
        "sec_pit_shadow_monitor",
        "score_change_attribution",
        "market_panel",
        "market_data_freshness",
        "market_data_recover_freshness",
        "portfolio_candidate_tracking",
        "portfolio_tracking_review",
        "portfolio_tracking_review_report",
        "etf_forward_update",
        "etf_forward_dashboard",
        "etf_forward_watchlist",
        "artifact_lineage",
        "validate_artifact_lineage",
        "report_index",
        "documentation_contract",
        "research_governance_summary",
        "reader_brief",
        "report_quality_gate",
        "validate_reader_brief",
        "dynamic_v3_rescue_schedule_observe",
        "pipeline_health",
    ]

    assert step_ids[step_ids.index("score_daily") + 1 : step_ids.index("secret_hygiene")] == (
        expected_after_score
    )
    forward_step = next(
        step for step in plan.steps if step.step_id == "forward_evidence_dry_run_daily"
    )
    assert forward_step.command == (
        "aits",
        "forward-evidence",
        "capture-dry-run-daily",
        "--as-of",
        "2026-05-06",
    )
    assert forward_step.required_env_vars == ()
    assert forward_step.blocks_downstream is False
    assert dashboard_step.command == ("aits", "reports", "dashboard", "--as-of", "2026-05-06")
    assert dashboard_step.required_env_vars == ()
    assert dashboard_step.blocks_downstream is False
    assert dashboard_step.produced_paths[0].name == "evidence_dashboard_2026-05-06.html"
    assert dashboard_step.produced_paths[1].name == "evidence_dashboard_2026-05-06.json"
    assert next(
        step for step in plan.steps if step.step_id == "sec_pit_shadow_observe"
    ).command == (
        "aits",
        "sec-pit",
        "shadow-observe",
        "--latest",
        "--end",
        "2026-05-06",
    )
    assert next(
        step for step in plan.steps if step.step_id == "sec_pit_shadow_monitor"
    ).command == (
        "aits",
        "sec-pit",
        "shadow-monitor",
        "--latest",
        "--as-of",
        "2026-05-06",
    )
    assert next(step for step in plan.steps if step.step_id == "market_data_freshness").command == (
        "aits",
        "data",
        "freshness",
        "--latest",
    )
    assert next(
        step for step in plan.steps if step.step_id == "portfolio_tracking_review"
    ).command == (
        "aits",
        "portfolio",
        "review-tracking",
        "--latest",
        "--show-window-progress",
    )
    assert next(step for step in plan.steps if step.step_id == "etf_forward_update").command == (
        "aits",
        "etf",
        "forward",
        "update",
        "--latest",
    )
    assert next(step for step in plan.steps if step.step_id == "artifact_lineage").command == (
        "aits",
        "reports",
        "artifact-lineage",
        "--latest",
    )
    assert next(
        step for step in plan.steps if step.step_id == "validate_artifact_lineage"
    ).command == (
        "aits",
        "reports",
        "validate-artifact-lineage",
        "--latest",
    )
    assert next(step for step in plan.steps if step.step_id == "report_index").command == (
        "aits",
        "reports",
        "index",
        "--latest",
    )
    assert next(
        step for step in plan.steps if step.step_id == "documentation_contract"
    ).command == (
        "aits",
        "docs",
        "report-contract",
        "--latest",
    )
    assert next(step for step in plan.steps if step.step_id == "reader_brief").command == (
        "aits",
        "reports",
        "reader-brief",
        "--latest",
    )
    assert next(
        step for step in plan.steps if step.step_id == "dynamic_v3_rescue_schedule_observe"
    ).command == (
        "aits",
        "etf",
        "dynamic-v3-rescue",
        "schedule",
        "observe",
        "--as-of",
        "2026-05-06",
    )


def test_daily_ops_plan_cli_writes_report(tmp_path: Path) -> None:
    output_path = tmp_path / "daily_ops_plan.md"

    result = CliRunner().invoke(
        app,
        [
            "ops",
            "daily-plan",
            "--as-of",
            "2026-05-06",
            "--skip-risk-event-openai-precheck",
            "--output-path",
            str(output_path),
        ],
        env={
            "FMP_API_KEY": "present",
            "MARKETSTACK_API_KEY": "present",
            "OPENAI_API_KEY": "",
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
        },
    )

    assert result.exit_code == 0
    assert "每日运行计划：READY" in result.output
    assert output_path.exists()
    markdown = output_path.read_text(encoding="utf-8")
    assert "# 每日运行计划" in markdown
    assert "validate-data --as-of 2026-05-06" in markdown
    assert "pit-snapshots fetch-fmp-forward" in markdown
    assert "--continue-on-failure" in markdown
    assert "pit-snapshots build-manifest --as-of 2026-05-06" in markdown
    assert "pit-snapshots validate --as-of 2026-05-06" in markdown
    assert "fundamentals download-sec-companyfacts" in markdown
    assert "fundamentals extract-sec-metrics --as-of 2026-05-06" in markdown
    assert "fundamentals merge-tsm-ir-sec-metrics --as-of 2026-05-06" in markdown
    assert "valuation fetch-fmp --as-of 2026-05-06" in markdown
    assert "reports dashboard --as-of 2026-05-06" in markdown
    assert "sec-pit shadow-observe --latest --end 2026-05-06" in markdown
    assert "sec-pit shadow-monitor --latest --as-of 2026-05-06" in markdown
    assert "reports score-change-attribution --latest" in markdown
    assert "reports market-panel --latest" in markdown
    assert "data freshness --latest" in markdown
    assert "data recover-freshness --latest" in markdown
    assert "portfolio track-candidate --latest" in markdown
    assert "portfolio review-tracking --latest --show-window-progress" in markdown
    assert "reports portfolio-tracking-review --latest" in markdown
    assert "reports index --latest" in markdown
    assert "docs report-contract --latest" in markdown
    assert "reports research-governance-summary --latest" in markdown
    assert "reports reader-brief --latest" in markdown
    assert "reports validate-reader-brief --latest" in markdown
    assert "etf dynamic-v3-rescue schedule observe --as-of 2026-05-06" in markdown
    assert "ops health --as-of 2026-05-06" in markdown
    assert "security scan-secrets --as-of 2026-05-06" in markdown


def test_daily_ops_run_cli_writes_daily_task_dashboard(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_output_root = tmp_path / "runs"
    snapshot_dir = tmp_path / "data" / "processed" / "decision_snapshots"
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "decision_snapshot_2026-05-06.json").write_text(
        json.dumps(
            {
                "snapshot_id": "decision_snapshot:2026-05-06",
                "signal_date": "2026-05-06",
                "market_regime": {
                    "regime_id": "ai_after_chatgpt",
                    "start_date": "2022-12-01",
                },
                "scores": {
                    "overall_score": 72.0,
                    "confidence_score": 65.0,
                    "confidence_level": "medium",
                    "components": [
                        {
                            "component": "trend",
                            "score": 72.0,
                            "weight": 25.0,
                            "coverage": 1.0,
                            "confidence": 0.9,
                        }
                    ],
                },
                "positions": {
                    "final_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.4,
                        "label": "受限中配",
                    },
                    "final_total_risk_asset_band": {
                        "min_position": 0.4,
                        "max_position": 0.6,
                    },
                    "position_gates": [
                        {
                            "gate_id": "valuation",
                            "label": "估值拥挤",
                            "max_position": 0.4,
                            "triggered": True,
                        }
                    ],
                },
                "quality": {"market_data_status": "PASS"},
                "manual_review": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    def fake_run_daily_ops_plan(
        plan,
        *,
        project_root,
        env,
        run_id,
        diagnostics_dir,
    ) -> DailyOpsRunReport:
        reports_dir = project_root / "outputs" / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        (reports_dir / "data_quality_2026-05-06.md").write_text(
            "# 数据质量\n\n- 状态：PASS\n- 错误数：0\n- 警告数：0\n",
            encoding="utf-8",
        )
        (reports_dir / "download_data_diagnostics_2026-05-06.md").write_text(
            "# 下载诊断\n\n- 状态：PASS\n",
            encoding="utf-8",
        )
        started_at = datetime(2020, 1, 1, 0, 0, tzinfo=UTC)
        finished_at = datetime(2020, 1, 1, 0, 1, tzinfo=UTC)
        step_results = tuple(
            DailyOpsStepResult(
                step_id=step.step_id,
                title=step.title,
                command=step.command,
                status="SKIPPED" if not step.enabled else "PASS",
                return_code=None if not step.enabled else 0,
                started_at=None if not step.enabled else started_at,
                ended_at=None if not step.enabled else finished_at,
                duration_seconds=None if not step.enabled else 1.0,
                produced_paths=step.produced_paths,
                blocks_downstream=step.blocks_downstream,
                skip_reason=step.skip_reason,
            )
            for step in plan.steps
        )
        metadata = DailyOpsRunMetadata(
            schema_version=1,
            run_id=run_id,
            as_of=plan.as_of,
            generated_at=started_at,
            project_root=project_root,
            status="PASS",
            started_at=started_at,
            finished_at=finished_at,
            visibility_cutoff=finished_at,
            visibility_cutoff_source="test",
            input_visibility_status="PASS",
            input_visibility_issues=(),
            git={"commit": "test", "dirty": False},
            config_artifacts=(),
            rule_card_sha256=None,
            env_presence={},
            commands=tuple(
                {
                    "step_id": step.step_id,
                    "enabled": step.enabled,
                    "command": " ".join(step.command),
                    "required_env_vars": list(step.required_env_vars),
                    "blocks_downstream": step.blocks_downstream,
                    "skip_reason": step.skip_reason,
                    "input_visibility": step.input_visibility,
                }
                for step in plan.steps
            ),
            step_results=tuple(
                {
                    "step_id": result.step_id,
                    "status": result.status,
                    "return_code": result.return_code,
                    "started_at": (
                        None if result.started_at is None else result.started_at.isoformat()
                    ),
                    "ended_at": None if result.ended_at is None else result.ended_at.isoformat(),
                    "duration_seconds": result.duration_seconds,
                    "stdout_line_count": result.stdout_line_count,
                    "stderr_line_count": result.stderr_line_count,
                    "error": result.error,
                }
                for result in step_results
            ),
            pre_run_input_artifacts=(),
            produced_artifacts=(),
        )
        return DailyOpsRunReport(
            plan=plan,
            started_at=started_at,
            finished_at=finished_at,
            status="PASS",
            step_results=step_results,
            metadata=metadata,
        )

    monkeypatch.setattr(ops_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(ops_cli, "DEFAULT_DECISION_SNAPSHOT_DIR", snapshot_dir)
    monkeypatch.setattr(ops_cli, "run_daily_ops_plan", fake_run_daily_ops_plan)

    result = CliRunner().invoke(
        app,
        [
            "ops",
            "daily-run",
            "--as-of",
            "2026-05-06",
            "--skip-risk-event-openai-precheck",
            "--run-output-root",
            str(run_output_root),
            "--run-id",
            "daily_ops_run:2026-05-06:test",
        ],
        env={
            "FMP_API_KEY": "present",
            "MARKETSTACK_API_KEY": "present",
            "OPENAI_API_KEY": "",
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
        },
    )

    assert result.exit_code == 0, result.output
    assert "每日任务" in result.output
    assert "Dashboard" in result.output
    task_dashboard = next(run_output_root.rglob("reports/daily_task_dashboard_2026-05-06.html"))
    task_dashboard_json = next(
        run_output_root.rglob("reports/daily_task_dashboard_2026-05-06.json")
    )
    decision_summary_json = next(
        run_output_root.rglob("reports/daily_decision_summary_2026-05-06.json")
    )
    order_intent_candidates_json = next(
        run_output_root.rglob("reports/order_intent_candidates_2026-05-06.json")
    )
    reader_brief_json = next(run_output_root.rglob("reports/reader_brief_2026-05-06.json"))
    owner_daily_brief_json = next(
        run_output_root.rglob("reports/owner_daily_brief_2026-05-06.json")
    )
    reader_brief_quality_json = next(
        run_output_root.rglob("reports/reader_brief_quality_2026-05-06.json")
    )
    periodic_plan_json = next(
        run_output_root.rglob("metadata/periodic_operations_plan_2026-05-06.json")
    )
    assert task_dashboard.exists()
    assert task_dashboard_json.exists()
    assert decision_summary_json.exists()
    assert order_intent_candidates_json.exists()
    assert reader_brief_json.exists()
    assert owner_daily_brief_json.exists()
    assert reader_brief_quality_json.exists()
    assert periodic_plan_json.exists()
    assert "关键结论总览" in task_dashboard.read_text(encoding="utf-8")
    assert (tmp_path / "outputs" / "reports" / "daily_task_dashboard_2026-05-06.html").exists()
    assert (tmp_path / "outputs" / "reports" / "daily_task_dashboard_2026-05-06.json").exists()
    assert (tmp_path / "outputs" / "reports" / "daily_decision_summary_2026-05-06.json").exists()
    assert (tmp_path / "outputs" / "reports" / "order_intent_candidates_2026-05-06.json").exists()
    assert (tmp_path / "outputs" / "reports" / "reader_brief_2026-05-06.json").exists()
    assert (tmp_path / "outputs" / "reports" / "owner_daily_brief_2026-05-06.json").exists()
    assert (tmp_path / "outputs" / "reports" / "reader_brief_quality_2026-05-06.json").exists()
    decision_summary = json.loads(decision_summary_json.read_text(encoding="utf-8"))
    assert decision_summary["production_effect"] == "none"
    assert decision_summary["investment_conclusion"]["availability"] == "missing"
    assert decision_summary["decision_bus_role"]["order_intent_builder_connected"] is False
    reader_brief = json.loads(reader_brief_json.read_text(encoding="utf-8"))
    assert reader_brief["run_context"]["run_id"] == "daily_ops_run:2026-05-06:test"
    assert reader_brief["production_effect"] == "none"
    owner_daily_brief = json.loads(owner_daily_brief_json.read_text(encoding="utf-8"))
    assert len(owner_daily_brief["sections"]) == 10
    assert owner_daily_brief["production_effect"] == "none"
    order_candidates = json.loads(order_intent_candidates_json.read_text(encoding="utf-8"))
    assert order_candidates["production_effect"] == "none"
    assert order_candidates["execution_boundary"] == {
        "creates_order_intent": False,
        "creates_execution_action": False,
        "broker_api_allowed": False,
        "paper_broker_allowed": False,
        "account_state_required": False,
        "trading_engine_connected": False,
    }
    assert order_candidates["candidate_count"] == 1
    order_candidate = order_candidates["candidates"][0]
    assert order_candidate["blocked"] is True
    assert {"trading_engine_not_enabled", "manual_approval_required"}.issubset(
        set(order_candidate["blocked_by"])
    )
    assert order_candidate["execution_action"] == "none"
    assert order_candidate["would_create_order_intent"] is False
    assert order_candidate["would_submit_order"] is False
    periodic_plan = json.loads(periodic_plan_json.read_text(encoding="utf-8"))
    assert periodic_plan["schema_version"] == "periodic_operations_plan.v1"
    assert len(periodic_plan["entries"]) == 41
    assert periodic_plan["automatic_command_dispatch_enabled"] is False
    assert all(entry["command_executed"] is False for entry in periodic_plan["entries"])


def test_daily_ops_plan_cli_can_fail_on_missing_env(tmp_path: Path) -> None:
    output_path = tmp_path / "daily_ops_plan.md"

    result = CliRunner().invoke(
        app,
        [
            "ops",
            "daily-plan",
            "--as-of",
            "2026-05-06",
            "--output-path",
            str(output_path),
            "--fail-on-missing-env",
        ],
        env={
            "FMP_API_KEY": "",
            "MARKETSTACK_API_KEY": "",
            "OPENAI_API_KEY": "",
            "SEC_USER_AGENT": "",
        },
    )

    assert result.exit_code == 1
    assert "每日运行计划：BLOCKED_ENV" in result.output
    assert output_path.exists()


def test_periodic_dispatch_cli_requires_evidence_and_uses_controlled_runtime(
    tmp_path: Path, monkeypatch
) -> None:
    calls: list[tuple[str, ...]] = []

    def fake_runner(command, *, cwd):
        calls.append(command)
        assert cwd == tmp_path
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(ops_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(ops_cli, "_run_periodic_command", fake_runner)
    arguments = [
        "ops",
        "periodic-dispatch",
        "--as-of",
        "2026-07-10",
        "--task-id",
        "weekly_backtest",
        "--daily-status",
        "PASS",
        "--data-quality-status",
        "PASS",
        "--data-quality-evidence-id",
        "dq:2026-07-10",
        "--source-artifact-id",
        "daily:2026-07-10",
        "--owner-decision-id",
        "owner:weekly",
        "--confirm-manual-dispatch",
    ]

    first = CliRunner().invoke(app, arguments)
    duplicate = CliRunner().invoke(app, arguments)

    assert first.exit_code == 0, first.output
    assert duplicate.exit_code == 0, duplicate.output
    assert "weekly_backtest：PASS" in first.output
    assert len(calls) == 1
    plan_path = (
        tmp_path
        / "outputs"
        / "run_control"
        / "periodic"
        / "plans"
        / "periodic_operations_plan_2026-07-10.json"
    )
    assert plan_path.exists()
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    assert payload["automatic_command_dispatch_enabled"] is False
    assert len(payload["entries"]) == 41


def test_periodic_dispatch_cli_fails_without_manual_confirmation() -> None:
    result = CliRunner().invoke(
        app,
        [
            "ops",
            "periodic-dispatch",
            "--as-of",
            "2026-07-10",
            "--task-id",
            "weekly_backtest",
        ],
    )

    assert result.exit_code != 0
    assert "confirm-manual-dispatch" in result.output


def test_daily_ops_plan_pit_failure_is_not_a_downstream_blocker() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        include_download_data=False,
        skip_risk_event_openai_precheck=True,
    )
    pit_step = next(
        step for step in plan.steps if step.step_id == "pit_snapshots_fetch_fmp_forward"
    )
    pit_manifest_step = next(
        step for step in plan.steps if step.step_id == "pit_snapshots_build_manifest"
    )
    pit_validate_step = next(
        step for step in plan.steps if step.step_id == "pit_snapshots_validate"
    )

    assert pit_step.required_env_vars == ()
    assert pit_step.blocks_downstream is False
    assert pit_manifest_step.blocks_downstream is True
    assert pit_validate_step.blocks_downstream is True
    assert "--continue-on-failure" in pit_step.command
    assert (
        plan.status(
            {
                "FMP_API_KEY": "present",
                "SEC_USER_AGENT": "AITradingSystem test@example.com",
            }
        )
        == "READY_WITH_SKIPS"
    )


def test_daily_ops_plan_sec_and_valuation_steps_block_score_daily() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        skip_risk_event_openai_precheck=True,
    )
    step_ids = [step.step_id for step in plan.steps]

    assert step_ids.index("sec_companyfacts") < step_ids.index("score_daily")
    assert step_ids.index("sec_metrics") < step_ids.index("score_daily")
    assert step_ids.index("tsm_ir_sec_metrics_merge") < step_ids.index("score_daily")
    assert step_ids.index("sec_metrics_validation") < step_ids.index("score_daily")
    assert step_ids.index("sec_metrics") < step_ids.index("tsm_ir_sec_metrics_merge")
    assert step_ids.index("tsm_ir_sec_metrics_merge") < step_ids.index("sec_metrics_validation")
    assert step_ids.index("valuation_snapshots") < step_ids.index("score_daily")

    sec_companyfacts = next(step for step in plan.steps if step.step_id == "sec_companyfacts")
    sec_metrics = next(step for step in plan.steps if step.step_id == "sec_metrics")
    tsm_merge = next(step for step in plan.steps if step.step_id == "tsm_ir_sec_metrics_merge")
    valuation = next(step for step in plan.steps if step.step_id == "valuation_snapshots")

    assert sec_companyfacts.required_env_vars == ("SEC_USER_AGENT",)
    assert sec_companyfacts.blocks_downstream is True
    assert "download-sec-companyfacts" in sec_companyfacts.command
    assert sec_metrics.blocks_downstream is True
    assert "extract-sec-metrics" in sec_metrics.command
    assert tsm_merge.blocks_downstream is True
    assert "merge-tsm-ir-sec-metrics" in tsm_merge.command
    assert valuation.required_env_vars == ("FMP_API_KEY",)
    assert valuation.blocks_downstream is True
    assert "fetch-fmp" in valuation.command


def test_daily_ops_plan_closed_market_skips_score_and_current_download(
    tmp_path: Path,
) -> None:
    _write_price_cache(tmp_path / "data" / "raw" / "prices_daily.csv", "2026-05-08")
    _write_price_cache(
        tmp_path / "data" / "raw" / "prices_marketstack_daily.csv",
        "2026-05-08",
    )

    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 10),
        project_root=tmp_path,
    )
    markdown = render_daily_ops_plan(
        plan,
        env={
            "FMP_API_KEY": "present",
            "MARKETSTACK_API_KEY": "present",
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
        },
    )
    step_by_id = {step.step_id: step for step in plan.steps}

    assert plan.market_session.session_status == "CLOSED_MARKET"
    assert plan.market_session.previous_trading_day == date(2026, 5, 8)
    assert step_by_id["download_data"].enabled is False
    assert "休市日模式" in (step_by_id["download_data"].skip_reason or "")
    assert step_by_id["validate_data"].enabled is True
    assert step_by_id["validate_data"].command == (
        "aits",
        "validate-data",
        "--as-of",
        "2026-05-08",
    )
    assert step_by_id["score_daily"].enabled is False
    assert step_by_id["score_daily"].required_env_vars == ()
    assert step_by_id["reports_dashboard"].enabled is False
    assert step_by_id["reports_dashboard"].required_env_vars == ()
    assert step_by_id["sec_pit_shadow_monitor"].enabled is False
    assert step_by_id["score_change_attribution"].enabled is False
    assert step_by_id["market_panel"].enabled is False
    assert step_by_id["market_data_freshness"].enabled is False
    assert step_by_id["market_data_recover_freshness"].enabled is False
    assert step_by_id["portfolio_candidate_tracking"].enabled is False
    assert step_by_id["portfolio_tracking_review"].enabled is False
    assert step_by_id["portfolio_tracking_review_report"].enabled is False
    assert step_by_id["artifact_lineage"].enabled is False
    assert step_by_id["validate_artifact_lineage"].enabled is False
    assert step_by_id["report_index"].enabled is False
    assert step_by_id["documentation_contract"].enabled is False
    assert step_by_id["research_governance_summary"].enabled is False
    assert step_by_id["reader_brief"].enabled is False
    assert step_by_id["report_quality_gate"].enabled is False
    assert step_by_id["validate_reader_brief"].enabled is False
    assert step_by_id["dynamic_v3_rescue_schedule_observe"].enabled is True
    assert step_by_id["dynamic_v3_rescue_schedule_observe"].command == (
        "aits",
        "etf",
        "dynamic-v3-rescue",
        "schedule",
        "observe",
        "--as-of",
        "2026-05-10",
    )
    assert step_by_id["pit_snapshots_fetch_fmp_forward"].produced_paths[0] == (
        tmp_path / "data" / "raw" / "fmp_forward_pit"
    )
    assert step_by_id["pit_snapshots_fetch_fmp_forward"].produced_paths[1] == (
        tmp_path / "data" / "processed" / "pit_snapshots" / "fmp_forward_pit_2026-05-10.csv"
    )
    assert step_by_id["pit_snapshots_build_manifest"].produced_paths[0] == (
        tmp_path / "data" / "raw" / "pit_snapshots" / "manifest.csv"
    )
    assert "official_policy_sources" in step_by_id
    assert "--non-trading-day" in step_by_id["pipeline_health"].command
    assert "`aits score-daily --as-of 2026-05-10" not in markdown
    assert "`aits reports dashboard --as-of 2026-05-10`" not in markdown
    assert "`aits reports reader-brief --latest`" not in markdown
    assert "市场日状态：CLOSED_MARKET" in markdown
    assert "不生成新的 daily_score" in markdown


def test_daily_ops_plan_closed_market_downloads_previous_trading_day_when_cache_missing(
    tmp_path: Path,
) -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 10),
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    download_step = next(step for step in plan.steps if step.step_id == "download_data")

    assert download_step.enabled is True
    assert download_step.command == (
        "aits",
        "download-data",
        "--start",
        "2018-01-01",
        "--end",
        "2026-05-08",
    )


def test_run_daily_ops_plan_stops_on_first_failed_command(tmp_path: Path) -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        project_root=tmp_path,
        include_download_data=False,
        include_pit_snapshots=False,
        include_valuation_snapshots=False,
        include_secret_scan=False,
        skip_risk_event_openai_precheck=True,
    )
    validate_step = next(step for step in plan.steps if step.step_id == "validate_data")
    validate_step.produced_paths[0].parent.mkdir(parents=True, exist_ok=True)
    validate_step.produced_paths[0].write_text("- 状态：PASS\n", encoding="utf-8")
    calls: list[tuple[str, ...]] = []

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return_code = 1 if "extract-sec-metrics" in command else 0
        return subprocess.CompletedProcess(
            command,
            return_code,
            stdout="stdout summary\n",
            stderr="stderr summary\n" if return_code else "",
        )

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={"SEC_USER_AGENT": "AITradingSystem test@example.com"},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )

    assert report.status == "FAIL"
    assert report.failed_step is not None
    assert report.failed_step.step_id == "sec_metrics"
    assert [call[3:] for call in calls] == [
        ("validate-data", "--as-of", "2026-05-06"),
        ("fundamentals", "download-sec-companyfacts"),
        ("fundamentals", "extract-sec-metrics", "--as-of", "2026-05-06"),
    ]


def test_run_daily_ops_plan_writes_redacted_failure_diagnostic(tmp_path: Path) -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        project_root=tmp_path,
        include_download_data=False,
        include_pit_snapshots=False,
        include_valuation_snapshots=False,
        include_secret_scan=False,
        skip_risk_event_openai_precheck=True,
    )
    secret = "TEST_ONLY_NON_PROVIDER_SECRET"
    validate_step = next(step for step in plan.steps if step.step_id == "validate_data")
    validate_step.produced_paths[0].parent.mkdir(parents=True, exist_ok=True)
    validate_step.produced_paths[0].write_text("- 状态：PASS\n", encoding="utf-8")

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        return_code = 1 if "extract-sec-metrics" in command else 0
        stderr = (
            "Traceback line\n"
            f"https://example.test?apikey={secret}\n"
            f"Authorization: Bearer {secret}\n"
        )
        return subprocess.CompletedProcess(
            command,
            return_code,
            stdout=f"stdout contains {secret}\n",
            stderr=stderr if return_code else "",
        )

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
            "FMP_API_KEY": secret,
        },
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )
    assert report.failed_step is not None
    diagnostic_path = report.failed_step.diagnostic_path
    assert diagnostic_path is not None
    diagnostic_text = diagnostic_path.read_text(encoding="utf-8")
    markdown = render_daily_ops_run_report(report)

    assert report.status == "FAIL"
    assert str(diagnostic_path) in markdown
    assert "Traceback line" in diagnostic_text
    assert "apikey=***" in diagnostic_text
    assert "Authorization: Bearer ***" in diagnostic_text
    assert secret not in diagnostic_text
    assert secret not in markdown
    assert report.metadata is not None
    failed_step = next(
        item for item in report.metadata.step_results if item["step_id"] == "sec_metrics"
    )
    assert failed_step["diagnostic_path"] == str(diagnostic_path)
    produced_paths = {artifact.path for artifact in report.metadata.produced_artifacts}
    assert diagnostic_path in produced_paths


def test_run_daily_ops_plan_sets_stable_child_python_env() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        include_download_data=False,
        include_pit_snapshots=False,
        include_valuation_snapshots=False,
        include_secret_scan=False,
        skip_risk_event_openai_precheck=True,
    )
    observed_env: dict[str, str] = {}

    def fake_runner(command: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        observed_env.update(kwargs["env"])  # type: ignore[arg-type]
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="")

    run_daily_ops_plan(
        plan,
        env={
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
            "PYTHONPYCACHEPREFIX": "bad-inherited-cache",
        },
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )

    assert observed_env["PYTHONMALLOC"] == "malloc"
    assert observed_env["PYTHONFAULTHANDLER"] == "1"
    assert observed_env["PYTHONDONTWRITEBYTECODE"] == "1"
    pycache_parts = Path(observed_env["PYTHONPYCACHEPREFIX"]).parts
    assert pycache_parts[-5:-1] == (
        "outputs",
        "tmp",
        "pycache",
        "daily_run",
    )
    assert pycache_parts[-1].startswith("run_")


def test_run_daily_ops_plan_purges_source_pycache_before_child(tmp_path: Path) -> None:
    source_pycache = tmp_path / "src" / "ai_trading_system" / "__pycache__"
    source_pycache.mkdir(parents=True)
    (source_pycache / "config.cpython-311.pyc").write_bytes(b"bad bytecode")
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

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        assert not source_pycache.exists()
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="")

    run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )


def test_purge_source_pycache_leaves_non_source_cache(tmp_path: Path) -> None:
    source_pycache = tmp_path / "src" / "pkg" / "__pycache__"
    other_pycache = tmp_path / "outputs" / "__pycache__"
    source_pycache.mkdir(parents=True)
    other_pycache.mkdir(parents=True)

    _purge_source_pycache_dirs(tmp_path)

    assert not source_pycache.exists()
    assert other_pycache.exists()


def test_run_daily_ops_plan_blocks_historical_as_of_before_commands() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 8),
        skip_risk_event_openai_precheck=False,
    )
    calls: list[tuple[str, ...]] = []

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan(
        plan,
        env={
            "FMP_API_KEY": "present",
            "MARKETSTACK_API_KEY": "present",
            "OPENAI_API_KEY": "present",
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
        },
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 10),
    )
    markdown = render_daily_ops_run_report(report)

    assert report.status == "BLOCKED_VISIBILITY"
    assert calls == []
    assert report.failed_step is not None
    assert report.failed_step.step_id == "input_visibility"
    assert report.metadata is not None
    assert report.metadata.input_visibility_status == "BLOCKED"
    assert "daily_run_historical_as_of_requires_replay" in markdown
    assert "aits ops replay-day --mode cache-only --as-of 2026-05-08" in markdown


def test_run_daily_ops_plan_blocks_trading_day_before_market_close_window() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 11),
        skip_risk_event_openai_precheck=True,
    )
    calls: list[tuple[str, ...]] = []

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan(
        plan,
        env={
            "FMP_API_KEY": "present",
            "MARKETSTACK_API_KEY": "present",
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
        },
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 11),
        visibility_latest_completed_trading_day=date(2026, 5, 8),
    )

    assert report.status == "BLOCKED_VISIBILITY"
    assert calls == []
    assert report.failed_step is not None
    assert report.failed_step.step_id == "input_visibility"


def test_run_daily_ops_plan_allows_explicit_current_closed_market_day(tmp_path: Path) -> None:
    _write_price_cache(tmp_path / "data" / "raw" / "prices_daily.csv", "2026-05-08")
    _write_price_cache(
        tmp_path / "data" / "raw" / "prices_marketstack_daily.csv",
        "2026-05-08",
    )
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 10),
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    validate_step = next(step for step in plan.steps if step.step_id == "validate_data")
    validate_step.produced_paths[0].parent.mkdir(parents=True, exist_ok=True)
    validate_step.produced_paths[0].write_text("- 状态：PASS\n", encoding="utf-8")
    calls: list[tuple[str, ...]] = []

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={
            "FMP_API_KEY": "present",
            "MARKETSTACK_API_KEY": "present",
            "SEC_USER_AGENT": "AITradingSystem test@example.com",
        },
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 10),
        visibility_latest_completed_trading_day=date(2026, 5, 8),
    )

    assert calls
    assert report.failed_step is None or report.failed_step.step_id != "input_visibility"
    assert calls[0][3:] == ("validate-data", "--as-of", "2026-05-08")
    assert calls[1][3:6] == ("risk-events", "fetch-official-sources", "--as-of")
    assert calls[1][6] == "2026-05-10"


def test_daily_ops_run_report_omits_command_output_text(tmp_path: Path) -> None:
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
    for step in plan.steps:
        if step.step_id == "validate_data":
            status_paths = (step.produced_paths[0],)
        elif step.step_id == "score_daily":
            status_paths = (step.produced_paths[2], step.produced_paths[4])
        elif step.step_id == "pipeline_health":
            status_paths = (step.produced_paths[0],)
        else:
            status_paths = ()
        for path in status_paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("- 状态：PASS\n", encoding="utf-8")

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="SECRET_SHOULD_NOT_APPEAR\n",
            stderr="PAID_CONTENT_SHOULD_NOT_APPEAR\n",
        )

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )
    markdown = render_daily_ops_run_report(report)

    assert report.status == "PASS_WITH_SKIPS"
    assert "SECRET_SHOULD_NOT_APPEAR" not in markdown
    assert "PAID_CONTENT_SHOULD_NOT_APPEAR" not in markdown
    assert "Stdout Lines" in markdown
    assert "Stderr Lines" in markdown


def test_daily_ops_run_report_writes_sanitized_metadata_sidecar(tmp_path: Path) -> None:
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
    for step in plan.steps:
        if step.step_id == "validate_data":
            status_paths = (step.produced_paths[0],)
        elif step.step_id == "score_daily":
            status_paths = (step.produced_paths[2], step.produced_paths[4])
        elif step.step_id == "pipeline_health":
            status_paths = (step.produced_paths[0],)
        else:
            status_paths = ()
        for path in status_paths:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("- 状态：PASS\n", encoding="utf-8")

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            command,
            0,
            stdout="SECRET_SHOULD_NOT_APPEAR\n",
            stderr="PAID_CONTENT_SHOULD_NOT_APPEAR\n",
        )

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={"UNUSED_SECRET": "SECRET_SHOULD_NOT_APPEAR"},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )
    output_path = tmp_path / "reports" / "daily_ops_run_2026-05-06.md"
    write_daily_ops_run_report(report, output_path)

    metadata_path = daily_ops_run_metadata_path_for_report(output_path)
    raw_metadata = metadata_path.read_text(encoding="utf-8")
    metadata = json.loads(raw_metadata)

    assert metadata_path.exists()
    assert metadata["run_id"].startswith("daily_ops_run:2026-05-06:")
    assert metadata["status"] == report.status
    assert metadata["visibility_cutoff_source"] == "daily_run_finished_at_utc"
    assert metadata["input_visibility_status"] == "PASS"
    assert "SECRET_SHOULD_NOT_APPEAR" not in raw_metadata
    assert "PAID_CONTENT_SHOULD_NOT_APPEAR" not in raw_metadata
    assert "env_presence" in metadata
    assert metadata["commands"]
    assert metadata["pre_run_input_artifacts"]
    assert metadata["produced_artifacts"]
    pre_run_paths = {artifact["path"] for artifact in metadata["pre_run_input_artifacts"]}
    assert str(tmp_path / "data" / "raw" / "prices_daily.csv") in pre_run_paths

    explicit_report_path = tmp_path / "bundle" / "reports" / "daily_ops_run_2026-05-06.md"
    explicit_metadata_path = (
        tmp_path / "bundle" / "metadata" / "daily_ops_run_metadata_2026-05-06.json"
    )
    write_daily_ops_run_report(
        report,
        explicit_report_path,
        metadata_path=explicit_metadata_path,
    )

    assert explicit_metadata_path.exists()
    assert str(explicit_metadata_path) in explicit_report_path.read_text(encoding="utf-8")


def test_run_daily_ops_plan_fails_when_artifact_status_fails(tmp_path: Path) -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        project_root=tmp_path,
        include_download_data=False,
        include_sec_fundamentals=False,
        include_valuation_snapshots=False,
        include_secret_scan=False,
        skip_risk_event_openai_precheck=True,
    )
    validate_step = next(step for step in plan.steps if step.step_id == "validate_data")
    validate_step.produced_paths[0].parent.mkdir(parents=True, exist_ok=True)
    validate_step.produced_paths[0].write_text("- 状态：PASS\n", encoding="utf-8")
    pit_step = next(
        step for step in plan.steps if step.step_id == "pit_snapshots_fetch_fmp_forward"
    )
    pit_step.produced_paths[2].parent.mkdir(parents=True, exist_ok=True)
    pit_step.produced_paths[2].write_text("- 状态：FAIL\n", encoding="utf-8")
    calls: list[tuple[str, ...]] = []

    def fake_runner(command: tuple[str, ...], **_: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    report = run_daily_ops_plan(
        plan,
        project_root=tmp_path,
        env={},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )

    assert report.status == "FAIL"
    assert report.failed_step is not None
    assert report.failed_step.step_id == "pit_snapshots_fetch_fmp_forward"
    assert "artifact_status_failed" in (report.failed_step.error or "")
    assert [call[3:] for call in calls] == [
        validate_step.command[1:],
        pit_step.command[1:],
    ]


def _write_price_cache(path: Path, latest_date: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"date,ticker,open,high,low,close,adj_close,volume\n{latest_date},NVDA,1,1,1,1,1,100\n",
        encoding="utf-8",
    )
