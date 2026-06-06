from __future__ import annotations

import os
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.alerts import (
    build_pipeline_health_alert_report,
    default_pipeline_health_alert_report_path,
    write_alert_report,
)
from ai_trading_system.cli_commands.risk_event_artifacts import (
    DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_report,
    default_daily_decision_summary_path,
    default_daily_task_dashboard_json_path,
    default_daily_task_dashboard_path,
    write_daily_decision_summary_json,
    write_daily_task_dashboard,
    write_daily_task_dashboard_json,
)
from ai_trading_system.data.quality import default_quality_report_path
from ai_trading_system.decision_snapshots import (
    DEFAULT_DECISION_SNAPSHOT_DIR,
    default_decision_snapshot_path,
)
from ai_trading_system.documentation_contract import default_documentation_contract_json_path
from ai_trading_system.evidence_dashboard import default_evidence_dashboard_json_path
from ai_trading_system.fmp_forward_pit import (
    DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
    default_fmp_forward_pit_fetch_report_path,
    default_fmp_forward_pit_normalized_path,
)
from ai_trading_system.historical_replay import (
    default_historical_replay_output_root,
    run_historical_day_replay,
    run_historical_replay_window,
)
from ai_trading_system.ops_daily import (
    build_daily_ops_plan,
    default_daily_ops_plan_path,
    default_daily_ops_run_metadata_path,
    default_daily_ops_run_report_path,
    resolve_daily_ops_default_as_of,
    run_daily_ops_plan,
    write_daily_ops_plan,
    write_daily_ops_run_report,
)
from ai_trading_system.order_intent_candidates import (
    default_order_intent_candidates_path,
    write_order_intent_candidates_json,
)
from ai_trading_system.pipeline_health import (
    PipelineArtifactSpec,
    build_pipeline_health_report,
    build_pit_snapshot_health_checks,
    default_pipeline_health_report_path,
    write_pipeline_health_report,
)
from ai_trading_system.pit_snapshots import (
    DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    default_pit_snapshot_validation_report_path,
)
from ai_trading_system.report_traceability import default_report_trace_bundle_path
from ai_trading_system.reports.calculation_explainers import default_calculation_explainers_path
from ai_trading_system.reports.market_panel import default_market_panel_json_path
from ai_trading_system.reports.reader_brief import (
    build_reader_brief_payload,
    build_reader_brief_quality_payload,
    default_reader_brief_html_path,
    default_reader_brief_json_path,
    default_reader_brief_quality_json_path,
    default_reader_brief_quality_markdown_path,
    write_reader_brief_html,
    write_reader_brief_json,
    write_reader_brief_quality_json,
    write_reader_brief_quality_markdown,
)
from ai_trading_system.reports.report_index import default_report_index_json_path
from ai_trading_system.reports.research_governance_summary import (
    default_research_governance_summary_json_path,
)
from ai_trading_system.reports.score_change_attribution import (
    default_score_change_attribution_json_path,
)
from ai_trading_system.run_artifacts import (
    build_run_artifact_paths,
    collect_run_files,
    default_daily_run_id,
    mirror_canonical_daily_ops_outputs_to_legacy,
    mirror_legacy_reports_to_run,
    prepare_run_directories,
    validate_legacy_output_mode,
    write_run_manifest,
)
from ai_trading_system.scoring.daily import default_daily_score_report_path

ops_app = typer.Typer(help="运行监控和 pipeline health。", no_args_is_help=True)
console = Console()


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _parse_datetime(value: str) -> datetime:
    normalized = value.strip()
    if not normalized:
        raise typer.BadParameter("时间不能为空。")
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        if "T" not in normalized and " " not in normalized:
            parsed_date = date.fromisoformat(normalized)
            return datetime.combine(parsed_date, time.min, tzinfo=UTC)
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise typer.BadParameter("时间必须使用 ISO datetime 或 YYYY-MM-DD 格式。") from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


@ops_app.command("health")
def pipeline_health_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="检查日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化日线利率 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    features_path: Annotated[
        Path,
        typer.Option(help="每日特征 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "features_daily.csv",
    scores_path: Annotated[
        Path,
        typer.Option(help="每日评分 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "processed"
    / "scores_daily.csv",
    data_quality_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 数据质量报告路径。"),
    ] = None,
    daily_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日评分报告路径。"),
    ] = None,
    pit_manifest_path: Annotated[
        Path,
        typer.Option(help="PIT raw snapshot manifest 路径。"),
    ] = DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    pit_normalized_path: Annotated[
        Path | None,
        typer.Option(help="FMP PIT 标准化 as-of CSV 路径，默认按 as-of 日期生成。"),
    ] = None,
    pit_validation_report_path: Annotated[
        Path | None,
        typer.Option(help="PIT 快照质量报告路径，默认按 as-of 日期生成。"),
    ] = None,
    pit_fetch_report_path: Annotated[
        Path | None,
        typer.Option(help="FMP PIT 抓取报告路径，默认按 as-of 日期生成。"),
    ] = None,
    min_pit_manifest_records: Annotated[
        int,
        typer.Option(help="PIT manifest 最低记录数。"),
    ] = 1,
    min_pit_normalized_rows: Annotated[
        int,
        typer.Option(help="FMP PIT 标准化 as-of CSV 最低行数。"),
    ] = 1,
    max_pit_snapshot_age_days: Annotated[
        int,
        typer.Option(help="PIT latest available_time 最大允许日龄，超出后告警。"),
    ] = 3,
    non_trading_day: Annotated[
        bool,
        typer.Option(
            "--non-trading-day/--trading-day",
            help="休市日健康检查模式：不要求当日评分产物存在。",
        ),
    ] = False,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown pipeline health 报告输出路径。"),
    ] = None,
    alert_output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown pipeline health 告警报告输出路径。"),
    ] = None,
) -> None:
    """检查关键 pipeline 输入/输出 artifact 和 PIT 快照归档健康。"""
    health_observed_at = datetime.now(tz=UTC)
    health_date = _parse_date(as_of) if as_of else date.today()
    production_health_cutoff = (
        health_observed_at
        if health_date == resolve_daily_ops_default_as_of(health_observed_at)
        else None
    )
    quality_report = data_quality_report_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    daily_report = daily_report_path or default_daily_score_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    report_path = output_path or default_pipeline_health_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    pit_normalized = pit_normalized_path or default_fmp_forward_pit_normalized_path(
        DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
        health_date,
    )
    pit_validation_report = (
        pit_validation_report_path
        or default_pit_snapshot_validation_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            health_date,
        )
    )
    pit_fetch_report = pit_fetch_report_path or default_fmp_forward_pit_fetch_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    pipeline_alert_report_path = alert_output_path or default_pipeline_health_alert_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        health_date,
    )
    pit_checks = build_pit_snapshot_health_checks(
        as_of=health_date,
        manifest_path=pit_manifest_path,
        normalized_path=pit_normalized,
        validation_report_path=pit_validation_report,
        fetch_report_path=pit_fetch_report,
        project_root=PROJECT_ROOT,
        min_manifest_records=min_pit_manifest_records,
        min_normalized_rows=min_pit_normalized_rows,
        max_snapshot_age_days=max_pit_snapshot_age_days,
        visibility_cutoff=production_health_cutoff,
    )
    core_artifacts = [
        PipelineArtifactSpec(
            "prices_daily",
            "价格缓存",
            prices_path,
            True,
            "运行 `aits download-data` 并检查 download manifest。",
        ),
        PipelineArtifactSpec(
            "rates_daily",
            "利率缓存",
            rates_path,
            True,
            "运行 `aits download-data` 并检查 FRED 下载状态。",
        ),
    ]
    if not non_trading_day:
        core_artifacts.extend(
            [
                PipelineArtifactSpec(
                    "data_quality_report",
                    "数据质量报告",
                    quality_report,
                    True,
                    "运行 `aits validate-data` 或 `aits score-daily`。",
                ),
                PipelineArtifactSpec(
                    "features_daily",
                    "每日特征缓存",
                    features_path,
                    True,
                    "运行 `aits build-features` 或 `aits score-daily`。",
                ),
                PipelineArtifactSpec(
                    "scores_daily",
                    "每日评分缓存",
                    scores_path,
                    True,
                    "运行 `aits score-daily`。",
                ),
                PipelineArtifactSpec(
                    "daily_score_report",
                    "每日评分报告",
                    daily_report,
                    True,
                    "运行 `aits score-daily` 并检查数据质量、SEC、风险事件和估值报告。",
                ),
            ]
        )
    report = build_pipeline_health_report(
        as_of=health_date,
        artifacts=tuple(core_artifacts),
        extra_checks=pit_checks,
        market_session="CLOSED_MARKET" if non_trading_day else "TRADING_DAY",
        market_session_note=(
            "休市日模式：不要求当日 data_quality、features、scores 或 daily_score 报告。"
            if non_trading_day
            else "交易日模式：要求当日数据质量、特征、评分和日报产物。"
        ),
    )
    write_pipeline_health_report(report, report_path)
    alert_report = build_pipeline_health_alert_report(
        report,
        pipeline_health_report_path=report_path,
    )
    write_alert_report(alert_report, pipeline_alert_report_path)

    style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{style}]Pipeline health：{report.status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(f"告警报告：{pipeline_alert_report_path}")
    console.print(f"检查项：{len(report.checks)}")
    console.print(f"错误数：{report.error_count}；警告数：{report.warning_count}")
    console.print(f"活跃告警数：{len(alert_report.alerts)}")
    if not report.passed:
        raise typer.Exit(code=1)


def _build_daily_ops_plan_from_cli_options(
    *,
    as_of: str | None,
    download_start: str,
    include_download_data: bool,
    include_pit_snapshots: bool,
    include_sec_fundamentals: bool,
    include_valuation_snapshots: bool,
    include_secret_scan: bool,
    risk_event_openai_precheck: bool,
    risk_event_openai_precheck_max_candidates: int | None,
    llm_request_profile: str,
    full_universe: bool,
    run_id: str | None = None,
    default_observed_at: datetime | None = None,
):
    plan_date = (
        _parse_date(as_of) if as_of else resolve_daily_ops_default_as_of(default_observed_at)
    )
    start_date = _parse_date(download_start)
    if (
        risk_event_openai_precheck_max_candidates is not None
        and risk_event_openai_precheck_max_candidates < 0
    ):
        raise typer.BadParameter("OpenAI 风险事件预审候选上限不能为负数。")
    try:
        plan = build_daily_ops_plan(
            as_of=plan_date,
            download_start=start_date,
            include_download_data=include_download_data,
            include_pit_snapshots=include_pit_snapshots,
            include_sec_fundamentals=include_sec_fundamentals,
            include_valuation_snapshots=include_valuation_snapshots,
            include_secret_scan=include_secret_scan,
            skip_risk_event_openai_precheck=not risk_event_openai_precheck,
            full_universe=full_universe,
            llm_request_profile=llm_request_profile,
            risk_event_openai_precheck_max_candidates=(risk_event_openai_precheck_max_candidates),
            run_id=run_id,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    return plan_date, plan


@ops_app.command("daily-plan")
def daily_ops_plan_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="计划日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    download_start: Annotated[
        str,
        typer.Option(help="市场数据下载起始日期，格式为 YYYY-MM-DD。"),
    ] = "2018-01-01",
    include_download_data: Annotated[
        bool,
        typer.Option(
            "--include-download-data/--skip-download-data",
            help="是否在计划中包含 `aits download-data`。",
        ),
    ] = True,
    include_pit_snapshots: Annotated[
        bool,
        typer.Option(
            "--include-pit-snapshots/--skip-pit-snapshots",
            help="是否在计划中包含 FMP forward-only PIT 抓取和校验。",
        ),
    ] = True,
    include_sec_fundamentals: Annotated[
        bool,
        typer.Option(
            "--include-sec-fundamentals/--skip-sec-fundamentals",
            help="是否在计划中包含 SEC companyfacts 刷新和 SEC metrics 抽取。",
        ),
    ] = True,
    include_valuation_snapshots: Annotated[
        bool,
        typer.Option(
            "--include-valuation-snapshots/--skip-valuation-snapshots",
            help="是否在计划中包含 FMP 估值和预期快照刷新。",
        ),
    ] = True,
    include_secret_scan: Annotated[
        bool,
        typer.Option(
            "--include-secret-scan/--skip-secret-scan",
            help="是否在计划中包含 secret hygiene 扫描。",
        ),
    ] = True,
    risk_event_openai_precheck: Annotated[
        bool,
        typer.Option(
            "--risk-event-openai-precheck/--skip-risk-event-openai-precheck",
            help="是否让计划中的 score-daily 默认执行 OpenAI 风险事件预审。",
        ),
    ] = True,
    risk_event_openai_precheck_max_candidates: Annotated[
        int | None,
        typer.Option(help="覆盖 LLM request profile 中的 OpenAI 风险事件预审候选上限。"),
    ] = None,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="计划中的 score-daily 使用的 LLM request profile。"),
    ] = DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="计划中对 download-data 使用完整 AI 产业链标的。",
        ),
    ] = False,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日运行计划输出路径。"),
    ] = None,
    fail_on_missing_env: Annotated[
        bool,
        typer.Option(
            "--fail-on-missing-env",
            help="如果计划发现缺少关键环境变量，写出报告后返回非零退出码。",
        ),
    ] = False,
) -> None:
    """生成适合本地或云 VM 调度的每日运行计划。"""
    plan_date, plan = _build_daily_ops_plan_from_cli_options(
        as_of=as_of,
        download_start=download_start,
        include_download_data=include_download_data,
        include_pit_snapshots=include_pit_snapshots,
        include_sec_fundamentals=include_sec_fundamentals,
        include_valuation_snapshots=include_valuation_snapshots,
        include_secret_scan=include_secret_scan,
        risk_event_openai_precheck=risk_event_openai_precheck,
        risk_event_openai_precheck_max_candidates=(risk_event_openai_precheck_max_candidates),
        llm_request_profile=llm_request_profile,
        full_universe=full_universe,
    )

    report_path = output_path or default_daily_ops_plan_path(
        PROJECT_ROOT / "outputs" / "reports",
        plan_date,
    )
    write_daily_ops_plan(plan, report_path, env=os.environ)

    status = plan.status(os.environ)
    style = "green" if status == "READY" else "yellow" if status == "READY_WITH_SKIPS" else "red"
    missing_env = plan.missing_env_vars(os.environ)
    console.print(f"[{style}]每日运行计划：{status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(f"步骤数：{len(plan.steps)}")
    if missing_env:
        console.print(f"缺失环境变量：{', '.join(missing_env)}")
    if fail_on_missing_env and missing_env:
        raise typer.Exit(code=1)


def _refresh_reader_brief_from_daily_run_summary(
    *,
    as_of: date,
    reports_dir: Path,
    daily_decision_summary_path: Path,
    daily_task_dashboard_json_path: Path,
) -> tuple[Path, ...]:
    daily_report_path = default_daily_score_report_path(reports_dir, as_of)
    reader_brief_html_path = default_reader_brief_html_path(reports_dir, as_of)
    reader_brief_json_path = default_reader_brief_json_path(reports_dir, as_of)
    reader_brief_quality_json_path = default_reader_brief_quality_json_path(reports_dir, as_of)
    reader_brief_quality_md_path = default_reader_brief_quality_markdown_path(reports_dir, as_of)
    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        decision_snapshot_path=default_decision_snapshot_path(
            DEFAULT_DECISION_SNAPSHOT_DIR,
            as_of,
        ),
        calculation_explainers_path=default_calculation_explainers_path(reports_dir, as_of),
        daily_decision_summary_path=daily_decision_summary_path,
        evidence_dashboard_json_path=default_evidence_dashboard_json_path(reports_dir, as_of),
        daily_task_dashboard_json_path=daily_task_dashboard_json_path,
        daily_report_path=daily_report_path,
        trace_bundle_path=default_report_trace_bundle_path(daily_report_path),
        score_change_attribution_path=default_score_change_attribution_json_path(
            reports_dir,
            as_of,
        ),
        market_panel_path=default_market_panel_json_path(reports_dir, as_of),
        research_governance_summary_path=default_research_governance_summary_json_path(
            reports_dir,
            as_of,
        ),
        report_index_path=default_report_index_json_path(reports_dir, as_of),
        documentation_contract_path=default_documentation_contract_json_path(reports_dir, as_of),
    )
    html_path = write_reader_brief_html(payload, reader_brief_html_path)
    json_path = write_reader_brief_json(payload, reader_brief_json_path)
    quality_payload = build_reader_brief_quality_payload(
        reader_brief_payload=payload,
        reader_brief_json_path=json_path,
        reader_brief_html_path=html_path,
    )
    quality_json_path = write_reader_brief_quality_json(
        quality_payload,
        reader_brief_quality_json_path,
    )
    quality_md_path = write_reader_brief_quality_markdown(
        quality_payload,
        reader_brief_quality_md_path,
    )
    return html_path, json_path, quality_json_path, quality_md_path


@ops_app.command("daily-run")
def daily_ops_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="运行日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    download_start: Annotated[
        str,
        typer.Option(help="市场数据下载起始日期，格式为 YYYY-MM-DD。"),
    ] = "2018-01-01",
    include_download_data: Annotated[
        bool,
        typer.Option(
            "--include-download-data/--skip-download-data",
            help="是否执行 `aits download-data`。",
        ),
    ] = True,
    include_pit_snapshots: Annotated[
        bool,
        typer.Option(
            "--include-pit-snapshots/--skip-pit-snapshots",
            help="是否执行 FMP forward-only PIT 抓取和校验。",
        ),
    ] = True,
    include_sec_fundamentals: Annotated[
        bool,
        typer.Option(
            "--include-sec-fundamentals/--skip-sec-fundamentals",
            help="是否执行 SEC companyfacts 刷新和 SEC metrics 抽取。",
        ),
    ] = True,
    include_valuation_snapshots: Annotated[
        bool,
        typer.Option(
            "--include-valuation-snapshots/--skip-valuation-snapshots",
            help="是否执行 FMP 估值和预期快照刷新。",
        ),
    ] = True,
    include_secret_scan: Annotated[
        bool,
        typer.Option(
            "--include-secret-scan/--skip-secret-scan",
            help="是否执行 secret hygiene 扫描。",
        ),
    ] = True,
    risk_event_openai_precheck: Annotated[
        bool,
        typer.Option(
            "--risk-event-openai-precheck/--skip-risk-event-openai-precheck",
            help="是否让 score-daily 执行 OpenAI 风险事件预审。",
        ),
    ] = True,
    risk_event_openai_precheck_max_candidates: Annotated[
        int | None,
        typer.Option(help="覆盖 LLM request profile 中的 OpenAI 风险事件预审候选上限。"),
    ] = None,
    llm_request_profile: Annotated[
        str,
        typer.Option(help="score-daily 使用的 LLM request profile。"),
    ] = DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="对 download-data 使用完整 AI 产业链标的。",
        ),
    ] = False,
    plan_output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日运行计划输出路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 每日执行报告输出路径。"),
    ] = None,
    run_output_root: Annotated[
        Path,
        typer.Option(help="Canonical run bundle 根目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "runs",
    run_id: Annotated[
        str | None,
        typer.Option(help="可选固定 run id；默认由 as_of 和 UTC 时间生成。"),
    ] = None,
    legacy_output_mode: Annotated[
        str,
        typer.Option(help="Legacy outputs/reports 兼容模式：mirror 或 off。"),
    ] = "mirror",
) -> None:
    """按每日运行计划真实执行本地 CLI，并生成脱敏执行报告。"""
    try:
        legacy_mode = validate_legacy_output_mode(legacy_output_mode)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    run_generated_at = datetime.now(tz=UTC)
    plan_date = _parse_date(as_of) if as_of else resolve_daily_ops_default_as_of(run_generated_at)
    resolved_run_id = run_id or default_daily_run_id(
        plan_date,
        generated_at=run_generated_at,
    )
    run_paths = prepare_run_directories(
        build_run_artifact_paths(
            as_of=plan_date,
            run_id=resolved_run_id,
            output_root=run_output_root,
            generated_at=run_generated_at,
        )
    )
    plan_date, plan = _build_daily_ops_plan_from_cli_options(
        as_of=as_of,
        download_start=download_start,
        include_download_data=include_download_data,
        include_pit_snapshots=include_pit_snapshots,
        include_sec_fundamentals=include_sec_fundamentals,
        include_valuation_snapshots=include_valuation_snapshots,
        include_secret_scan=include_secret_scan,
        risk_event_openai_precheck=risk_event_openai_precheck,
        risk_event_openai_precheck_max_candidates=(risk_event_openai_precheck_max_candidates),
        llm_request_profile=llm_request_profile,
        full_universe=full_universe,
        run_id=resolved_run_id,
        default_observed_at=run_generated_at,
    )

    reports_dir = PROJECT_ROOT / "outputs" / "reports"
    plan_report_path = plan_output_path or default_daily_ops_plan_path(
        run_paths.reports_dir,
        plan_date,
    )
    run_report_path = output_path or default_daily_ops_run_report_path(
        run_paths.reports_dir,
        plan_date,
    )
    write_daily_ops_plan(plan, plan_report_path, env=os.environ)
    if legacy_mode == "mirror":
        write_daily_ops_plan(
            plan,
            default_daily_ops_plan_path(reports_dir, plan_date),
            env=os.environ,
        )
    run_report = run_daily_ops_plan(
        plan,
        project_root=PROJECT_ROOT,
        env=os.environ,
        run_id=resolved_run_id,
        diagnostics_dir=run_paths.reports_dir / "diagnostics",
    )
    metadata_path = default_daily_ops_run_metadata_path(
        run_paths.metadata_dir,
        plan_date,
    )
    write_daily_ops_run_report(run_report, run_report_path, metadata_path=metadata_path)
    canonical_outputs = mirror_legacy_reports_to_run(
        as_of=plan_date,
        legacy_reports_dir=reports_dir,
        paths=run_paths,
        min_modified_at=run_report.started_at,
    )
    daily_task_dashboard_report = build_daily_task_dashboard_report(
        as_of=plan_date,
        metadata_path=metadata_path,
        run_report_path=run_report_path,
        reports_dir=run_paths.reports_dir,
    )
    daily_task_dashboard_path = write_daily_task_dashboard(
        daily_task_dashboard_report,
        default_daily_task_dashboard_path(run_paths.reports_dir, plan_date),
    )
    daily_task_dashboard_json_path = write_daily_task_dashboard_json(
        daily_task_dashboard_report,
        default_daily_task_dashboard_json_path(run_paths.reports_dir, plan_date),
    )
    daily_decision_summary_path = write_daily_decision_summary_json(
        daily_task_dashboard_report,
        default_daily_decision_summary_path(run_paths.reports_dir, plan_date),
    )
    order_intent_candidates_path = write_order_intent_candidates_json(
        as_of=plan_date,
        daily_decision_summary_path=daily_decision_summary_path,
        output_path=default_order_intent_candidates_path(run_paths.reports_dir, plan_date),
        project_root=PROJECT_ROOT,
    )
    reader_brief_step = next(
        (result for result in run_report.step_results if result.step_id == "reader_brief"),
        None,
    )
    reader_brief_final_paths = (
        _refresh_reader_brief_from_daily_run_summary(
            as_of=plan_date,
            reports_dir=run_paths.reports_dir,
            daily_decision_summary_path=daily_decision_summary_path,
            daily_task_dashboard_json_path=daily_task_dashboard_json_path,
        )
        if reader_brief_step is not None and reader_brief_step.status == "PASS"
        else ()
    )
    if legacy_mode == "mirror":
        legacy_outputs = mirror_canonical_daily_ops_outputs_to_legacy(
            paths=run_paths,
            legacy_reports_dir=reports_dir,
        )
    else:
        legacy_outputs = ()
    if run_report.metadata is not None:
        write_run_manifest(
            paths=run_paths,
            project_root=PROJECT_ROOT,
            status=run_report.status,
            visibility_cutoff=run_report.metadata.visibility_cutoff,
            visibility_cutoff_source=run_report.metadata.visibility_cutoff_source,
            legacy_output_mode=legacy_mode,
            input_artifacts=(
                artifact.path for artifact in run_report.metadata.pre_run_input_artifacts
            ),
            canonical_output_artifacts=(
                *collect_run_files(run_paths),
                *canonical_outputs,
            ),
            legacy_output_artifacts=(
                *(artifact.path for artifact in run_report.metadata.produced_artifacts),
                *legacy_outputs,
            ),
        )

    status = run_report.status
    style = "green" if status == "PASS" else "yellow" if status == "PASS_WITH_SKIPS" else "red"
    console.print(f"[{style}]每日运行执行：{status}[/{style}]")
    console.print(f"Run ID：{resolved_run_id}")
    console.print(f"Run bundle：{run_paths.run_root}")
    console.print(f"计划报告：{plan_report_path}")
    console.print(f"执行报告：{run_report_path}")
    console.print(f"每日任务 Dashboard：{daily_task_dashboard_path}")
    console.print(f"每日任务 Dashboard JSON：{daily_task_dashboard_json_path}")
    console.print(f"每日决策总线 JSON：{daily_decision_summary_path}")
    console.print(f"Order intent candidates JSON：{order_intent_candidates_path}")
    if reader_brief_final_paths:
        console.print(f"Reader Brief final：{reader_brief_final_paths[0]}")
    if run_report.metadata is not None:
        console.print(f"Metadata JSON：{metadata_path}")
        console.print(f"Run manifest：{run_paths.manifest_path}")
    console.print(f"执行步骤数：{len(run_report.step_results)} / {len(plan.steps)}")
    if run_report.missing_env_vars:
        console.print(f"缺失环境变量：{', '.join(run_report.missing_env_vars)}")
    if run_report.failed_step is not None:
        failed = run_report.failed_step
        console.print(f"失败步骤：{failed.step_id}；return_code={failed.return_code}")
    if status not in {"PASS", "PASS_WITH_SKIPS"}:
        raise typer.Exit(code=1)


@ops_app.command("replay-day")
def historical_replay_day_command(
    as_of: Annotated[
        str,
        typer.Option(help="回放的历史交易日，格式为 YYYY-MM-DD。"),
    ],
    mode: Annotated[
        str,
        typer.Option(help="回放模式；MVP 仅支持 cache-only。"),
    ] = "cache-only",
    visible_at: Annotated[
        str | None,
        typer.Option(help="显式可见时间上限，ISO datetime；不传则使用 as-of 当日 UTC 末尾。"),
    ] = None,
    output_root: Annotated[
        Path | None,
        typer.Option(help="Replay bundle 根目录；默认写入项目 outputs/replays。"),
    ] = None,
    label: Annotated[
        str | None,
        typer.Option(help="可选 replay 标签，用于 run id。"),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(help="可选固定 run id，便于测试或重复验证。"),
    ] = None,
    inventory_only: Annotated[
        bool,
        typer.Option(
            "--inventory-only/--run-score",
            help="只生成 input freeze manifest，不运行 score/health/secret replay。",
        ),
    ] = False,
    allow_incomplete: Annotated[
        bool,
        typer.Option(
            "--allow-incomplete/--strict",
            help="允许缺关键输入时只生成 INCOMPLETE_REPLAY 诊断报告。",
        ),
    ] = False,
    compare_to_production: Annotated[
        bool,
        typer.Option(
            "--compare-to-production/--no-compare-to-production",
            help="生成 replay 输出与 production artifact 的结构化 diff。",
        ),
    ] = False,
    openai_replay_policy: Annotated[
        str,
        typer.Option(
            help=(
                "OpenAI replay 策略：disabled 或 cache-only；cache-only 只复制历史"
                "预审队列/报告，不调用 live OpenAI。"
            ),
        ),
    ] = "disabled",
    full_universe: Annotated[
        bool,
        typer.Option("--full-universe", help="对 replay score-daily 使用完整 AI 产业链标的。"),
    ] = False,
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录；默认使用当前安装包配置的 PROJECT_ROOT。"),
    ] = PROJECT_ROOT,
) -> None:
    """基于本地归档输入回放某个历史交易日的分析产出。"""
    replay_date = _parse_date(as_of)
    replay_visible_at = _parse_datetime(visible_at) if visible_at else None
    try:
        replay_run = run_historical_day_replay(
            as_of=replay_date,
            project_root=project_root,
            output_root=output_root or default_historical_replay_output_root(project_root),
            mode=mode,
            visible_at=replay_visible_at,
            label=label,
            run_id=run_id,
            inventory_only=inventory_only,
            allow_incomplete=allow_incomplete,
            compare_to_production=compare_to_production,
            openai_replay_policy=openai_replay_policy,
            full_universe=full_universe,
            env=os.environ,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    status = replay_run.status
    style = (
        "green"
        if status in {"PASS", "PASS_INVENTORY"}
        else "yellow" if status == "INCOMPLETE_REPLAY" else "red"
    )
    console.print(f"[{style}]历史交易日回放：{status}[/{style}]")
    console.print(f"Replay bundle：{replay_run.paths.root}")
    console.print(f"回放报告：{replay_run.paths.run_report_path}")
    console.print(f"输入冻结清单：{replay_run.paths.input_manifest_csv_path}")
    if replay_run.production_diff is not None:
        console.print(f"Production diff：{replay_run.production_diff.report_path}")
    if replay_run.errors:
        console.print(f"输入阻断：{len(replay_run.errors)} 项")
    if replay_run.failed_step is not None:
        failed = replay_run.failed_step
        console.print(f"失败步骤：{failed.step_id}；return_code={failed.return_code}")
    if status not in {"PASS", "PASS_INVENTORY"}:
        raise typer.Exit(code=1)


@ops_app.command("replay-window")
def historical_replay_window_command(
    start: Annotated[
        str,
        typer.Option(help="批量回放起始日期，格式为 YYYY-MM-DD。"),
    ],
    end: Annotated[
        str,
        typer.Option(help="批量回放结束日期，格式为 YYYY-MM-DD。"),
    ],
    mode: Annotated[
        str,
        typer.Option(help="回放模式；目前仅支持 cache-only。"),
    ] = "cache-only",
    output_root: Annotated[
        Path | None,
        typer.Option(help="Replay bundle 根目录；默认写入项目 outputs/replays。"),
    ] = None,
    label: Annotated[
        str | None,
        typer.Option(help="可选 replay 标签，用于 window run id 和单日 run id。"),
    ] = None,
    run_id: Annotated[
        str | None,
        typer.Option(help="可选固定 window run id，便于测试或重复验证。"),
    ] = None,
    inventory_only: Annotated[
        bool,
        typer.Option(
            "--inventory-only/--run-score",
            help="只生成每个交易日的 input freeze manifest，不运行 score/health/secret replay。",
        ),
    ] = False,
    allow_incomplete: Annotated[
        bool,
        typer.Option(
            "--allow-incomplete/--strict",
            help="允许缺关键输入时只生成 INCOMPLETE_REPLAY 诊断报告。",
        ),
    ] = False,
    compare_to_production: Annotated[
        bool,
        typer.Option(
            "--compare-to-production/--no-compare-to-production",
            help="为每个交易日生成 replay 输出与 production artifact 的结构化 diff。",
        ),
    ] = False,
    openai_replay_policy: Annotated[
        str,
        typer.Option(
            help=(
                "OpenAI replay 策略：disabled 或 cache-only；cache-only 只复制历史"
                "预审队列/报告，不调用 live OpenAI。"
            ),
        ),
    ] = "disabled",
    full_universe: Annotated[
        bool,
        typer.Option("--full-universe", help="对 replay score-daily 使用完整 AI 产业链标的。"),
    ] = False,
    continue_on_failure: Annotated[
        bool,
        typer.Option(
            "--continue-on-failure/--stop-on-failure",
            help="某个交易日 replay 失败后是否继续后续交易日。",
        ),
    ] = False,
    project_root: Annotated[
        Path,
        typer.Option(help="项目根目录；默认使用当前安装包配置的 PROJECT_ROOT。"),
    ] = PROJECT_ROOT,
) -> None:
    """按交易日窗口批量运行历史交易日归档回放。"""
    start_date = _parse_date(start)
    end_date = _parse_date(end)
    try:
        window_run = run_historical_replay_window(
            start=start_date,
            end=end_date,
            project_root=project_root,
            output_root=output_root or default_historical_replay_output_root(project_root),
            mode=mode,
            label=label,
            run_id=run_id,
            inventory_only=inventory_only,
            allow_incomplete=allow_incomplete,
            compare_to_production=compare_to_production,
            openai_replay_policy=openai_replay_policy,
            full_universe=full_universe,
            continue_on_failure=continue_on_failure,
            env=os.environ,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    status = window_run.status
    style = "green" if status in {"PASS", "PASS_WITH_SKIPS"} else "red"
    console.print(f"[{style}]历史交易日批量回放：{status}[/{style}]")
    console.print(f"Window report：{window_run.report_path}")
    console.print(f"Window JSON：{window_run.json_path}")
    console.print(f"交易日回放数：{len(window_run.day_runs)}")
    console.print(f"跳过非交易日数：{len(window_run.skipped_dates)}")
    if window_run.failed_run is not None:
        failed = window_run.failed_run
        console.print(f"失败日期：{failed.as_of.isoformat()}；status={failed.status}")
    if status not in {"PASS", "PASS_WITH_SKIPS"}:
        raise typer.Exit(code=1)
