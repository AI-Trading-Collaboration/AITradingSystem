from __future__ import annotations

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

from typer.testing import CliRunner

import ai_trading_system.cli as cli_module
from ai_trading_system.cli import app
from ai_trading_system.ops_daily import (
    _execution_command,
    _purge_source_pycache_dirs,
    build_daily_ops_plan,
    daily_ops_run_metadata_path_for_report,
    render_daily_ops_plan,
    render_daily_ops_run_report,
    resolve_daily_ops_default_as_of,
    run_daily_ops_plan,
    write_daily_ops_run_report,
)


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
    assert pit_command in markdown
    assert "`aits fundamentals download-sec-companyfacts`" in markdown
    assert "`aits fundamentals extract-sec-metrics --as-of 2026-05-06`" in markdown
    assert "`aits fundamentals merge-tsm-ir-sec-metrics --as-of 2026-05-06`" in markdown
    assert "`aits fundamentals validate-sec-metrics --as-of 2026-05-06`" in markdown
    assert "`aits valuation fetch-fmp --as-of 2026-05-06`" in markdown
    assert "`aits score-daily --as-of 2026-05-06" in markdown
    assert "--llm-request-profile risk_event_daily_official_precheck" in markdown
    assert "`aits reports dashboard --as-of 2026-05-06`" in markdown
    assert "`live_provider`" in markdown
    assert "`readonly`" in markdown
    assert "缺少关键环境变量时，后续真实执行器必须 fail closed" in markdown


def test_daily_ops_default_as_of_uses_latest_completed_us_market_day() -> None:
    observed_at = datetime(2026, 5, 12, 1, 44, tzinfo=UTC)

    assert resolve_daily_ops_default_as_of(observed_at) == date(2026, 5, 11)


def test_daily_ops_plan_cli_default_as_of_uses_market_resolver(monkeypatch) -> None:
    monkeypatch.setattr(
        cli_module,
        "resolve_daily_ops_default_as_of",
        lambda observed_at=None: date(2026, 5, 11),
    )

    plan_date, plan = cli_module._build_daily_ops_plan_from_cli_options(
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


def test_daily_ops_plan_generates_feedback_reports_before_dashboard() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        skip_risk_event_openai_precheck=True,
    )
    step_ids = [step.step_id for step in plan.steps]
    market_feedback_step = next(
        step for step in plan.steps if step.step_id == "market_feedback_optimization"
    )
    loop_review_step = next(
        step for step in plan.steps if step.step_id == "feedback_loop_review"
    )
    investment_review_step = next(
        step for step in plan.steps if step.step_id == "investment_weekly_review"
    )
    dashboard_step = next(step for step in plan.steps if step.step_id == "reports_dashboard")

    assert step_ids.index("score_daily") < step_ids.index("market_feedback_optimization")
    assert step_ids.index("market_feedback_optimization") < step_ids.index(
        "feedback_loop_review"
    )
    assert step_ids.index("feedback_loop_review") < step_ids.index(
        "investment_weekly_review"
    )
    assert step_ids.index("investment_weekly_review") < step_ids.index("reports_dashboard")
    assert step_ids.index("reports_dashboard") < step_ids.index("pipeline_health")
    assert market_feedback_step.command == (
        "aits",
        "feedback",
        "optimize-market-feedback",
        "--as-of",
        "2026-05-06",
    )
    assert market_feedback_step.produced_paths[0].name == (
        "market_feedback_optimization_2026-05-06.md"
    )
    assert loop_review_step.command == (
        "aits",
        "feedback",
        "loop-review",
        "--as-of",
        "2026-05-06",
    )
    assert loop_review_step.produced_paths[0].name == "feedback_loop_review_2026-05-06.md"
    assert investment_review_step.command == (
        "aits",
        "reports",
        "investment-review",
        "--period",
        "weekly",
        "--as-of",
        "2026-05-06",
    )
    assert investment_review_step.produced_paths[0].name == (
        "investment_weekly_review_2026-05-06.md"
    )
    assert dashboard_step.command == ("aits", "reports", "dashboard", "--as-of", "2026-05-06")
    assert dashboard_step.required_env_vars == ()
    assert dashboard_step.blocks_downstream is False
    assert dashboard_step.produced_paths[0].name == "evidence_dashboard_2026-05-06.html"
    assert dashboard_step.produced_paths[1].name == "evidence_dashboard_2026-05-06.json"


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
    assert "pit-snapshots fetch-fmp-forward" in markdown
    assert "--continue-on-failure" in markdown
    assert "失败会写入脱敏报告或 pipeline health 告警" in markdown
    assert "fundamentals download-sec-companyfacts" in markdown
    assert "fundamentals extract-sec-metrics --as-of 2026-05-06" in markdown
    assert "fundamentals merge-tsm-ir-sec-metrics --as-of 2026-05-06" in markdown
    assert "valuation fetch-fmp --as-of 2026-05-06" in markdown
    assert "reports dashboard --as-of 2026-05-06" in markdown
    assert "ops health --as-of 2026-05-06" in markdown
    assert "security scan-secrets --as-of 2026-05-06" in markdown


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


def test_daily_ops_plan_pit_failure_is_not_a_downstream_blocker() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        include_download_data=False,
        skip_risk_event_openai_precheck=True,
    )
    pit_step = next(step for step in plan.steps if step.step_id == "pit_snapshots")

    assert pit_step.required_env_vars == ()
    assert pit_step.blocks_downstream is False
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
    assert step_ids.index("tsm_ir_sec_metrics_merge") < step_ids.index(
        "sec_metrics_validation"
    )
    assert step_ids.index("valuation_snapshots") < step_ids.index("score_daily")

    sec_companyfacts = next(step for step in plan.steps if step.step_id == "sec_companyfacts")
    sec_metrics = next(step for step in plan.steps if step.step_id == "sec_metrics")
    tsm_merge = next(
        step for step in plan.steps if step.step_id == "tsm_ir_sec_metrics_merge"
    )
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
    assert step_by_id["score_daily"].enabled is False
    assert step_by_id["score_daily"].required_env_vars == ()
    assert step_by_id["market_feedback_optimization"].enabled is False
    assert step_by_id["feedback_loop_review"].enabled is False
    assert step_by_id["investment_weekly_review"].enabled is False
    assert step_by_id["reports_dashboard"].enabled is False
    assert step_by_id["reports_dashboard"].required_env_vars == ()
    assert step_by_id["pit_snapshots"].produced_paths[0] == (
        tmp_path / "data" / "raw" / "fmp_forward_pit"
    )
    assert step_by_id["pit_snapshots"].produced_paths[1] == (
        tmp_path / "data" / "processed" / "pit_snapshots" / "fmp_forward_pit_2026-05-10.csv"
    )
    assert step_by_id["pit_snapshots"].produced_paths[2] == (
        tmp_path / "data" / "raw" / "pit_snapshots" / "manifest.csv"
    )
    assert "official_policy_sources" in step_by_id
    assert "--non-trading-day" in step_by_id["pipeline_health"].command
    assert "`aits score-daily --as-of 2026-05-10" not in markdown
    assert "`aits reports dashboard --as-of 2026-05-10`" not in markdown
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


def test_run_daily_ops_plan_stops_on_first_failed_command() -> None:
    plan = build_daily_ops_plan(
        as_of=date(2026, 5, 6),
        include_download_data=False,
        include_pit_snapshots=False,
        include_valuation_snapshots=False,
        include_secret_scan=False,
        skip_risk_event_openai_precheck=True,
    )
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
        env={"SEC_USER_AGENT": "AITradingSystem test@example.com"},
        runner=fake_runner,
        visibility_check_date=date(2026, 5, 6),
    )

    assert report.status == "FAIL"
    assert report.failed_step is not None
    assert report.failed_step.step_id == "sec_metrics"
    assert [call[3:] for call in calls] == [
        ("fundamentals", "download-sec-companyfacts"),
        ("fundamentals", "extract-sec-metrics", "--as-of", "2026-05-06"),
    ]


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
    assert calls[0][3:6] == ("risk-events", "fetch-official-sources", "--as-of")
    assert calls[0][6] == "2026-05-10"


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
        if step.step_id == "score_daily":
            status_paths = (step.produced_paths[2], step.produced_paths[4])
        elif step.step_id in {
            "market_feedback_optimization",
            "feedback_loop_review",
            "investment_weekly_review",
        }:
            status_paths = (step.produced_paths[0],)
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
        if step.step_id == "score_daily":
            status_paths = (step.produced_paths[2], step.produced_paths[4])
        elif step.step_id in {
            "market_feedback_optimization",
            "feedback_loop_review",
            "investment_weekly_review",
        }:
            status_paths = (step.produced_paths[0],)
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
    assert str(explicit_metadata_path) in explicit_report_path.read_text(
        encoding="utf-8"
    )


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
    pit_step = next(step for step in plan.steps if step.step_id == "pit_snapshots")
    pit_step.produced_paths[3].parent.mkdir(parents=True, exist_ok=True)
    pit_step.produced_paths[3].write_text("- 状态：FAIL\n", encoding="utf-8")
    pit_step.produced_paths[4].parent.mkdir(parents=True, exist_ok=True)
    pit_step.produced_paths[4].write_text("- 状态：PASS\n", encoding="utf-8")
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
    assert report.failed_step.step_id == "pit_snapshots"
    assert "artifact_status_failed" in (report.failed_step.error or "")
    assert [call[3:] for call in calls] == [pit_step.command[1:]]


def _write_price_cache(path: Path, latest_date: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"date,ticker,open,high,low,close,adj_close,volume\n{latest_date},NVDA,1,1,1,1,1,100\n",
        encoding="utf-8",
    )
