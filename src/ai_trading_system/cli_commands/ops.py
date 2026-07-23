from __future__ import annotations

import inspect
import os
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Annotated, Any, Protocol

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
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityExecutionContractError,
    VerifiedDataQualityPreflight,
)
from ai_trading_system.contracts.status import CanonicalStatus
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
from ai_trading_system.data.quality_execution import (
    DataQualityExecutionError,
    verify_data_quality_execution_receipt,
)
from ai_trading_system.data.quality_execution_discovery import (
    DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DiscoveredDataQualityExecution,
    default_data_quality_execution_discovery_path,
    load_default_data_quality_execution_discovery,
)
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
from ai_trading_system.legacy.periodic_operations_adapter import (
    build_periodic_operations_plan,
)
from ai_trading_system.ops_daily import (
    DailyOpsPlan,
    DailyOpsRunReport,
    _execution_command,
    build_daily_ops_plan,
    daily_ops_shadow_path_for_plan,
    default_daily_ops_plan_path,
    default_daily_ops_run_metadata_path,
    default_daily_ops_run_report_path,
    resolve_daily_ops_default_as_of,
    write_daily_ops_plan,
    write_daily_ops_run_report,
    write_daily_ops_shadow_plan,
)
from ai_trading_system.ops_daily import (
    run_daily_ops_plan_controlled as run_daily_ops_plan,
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
from ai_trading_system.platform.artifacts import (
    StrictJsonContractError,
    load_strict_json_path,
    load_strict_json_text,
    sha256_bytes,
    sha256_path,
    write_json_atomic,
)
from ai_trading_system.platform.operations import (
    OperationsRunControl,
    build_periodic_due_contexts_from_daily,
    default_periodic_operations_plan_path,
    dispatch_periodic_operations_plan,
    load_operations_runtime_control_policy,
    load_periodic_operations_control_policy,
    write_periodic_operations_plan,
)
from ai_trading_system.platform.operations.periodic_consumer_migration import (
    DataQualityReceiptVerifier,
    NativeConsumerExpectedContext,
    build_native_periodic_consumer_parity_plan,
    default_native_periodic_consumer_parity_plan_path,
    write_native_periodic_consumer_parity_plan,
)
from ai_trading_system.platform.reporting import write_owner_daily_brief_sidecars
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
from ai_trading_system.reports.report_quality_gate import (
    build_report_quality_gate_payload,
    default_report_quality_gate_json_path,
    default_report_quality_gate_markdown_path,
    write_report_quality_gate_json,
    write_report_quality_gate_markdown,
)
from ai_trading_system.reports.research_governance_summary import (
    default_research_governance_summary_json_path,
)
from ai_trading_system.reports.score_change_attribution import (
    default_score_change_attribution_json_path,
)
from ai_trading_system.run_artifacts import (
    RunArtifactPaths,
    build_run_artifact_paths,
    build_run_manifest,
    collect_run_files,
    default_daily_run_id,
    mirror_canonical_daily_ops_outputs_to_legacy,
    mirror_legacy_reports_to_run,
    prepare_run_directories,
    validate_legacy_output_mode,
)
from ai_trading_system.scheduled_tasks import (
    DEFAULT_SCHEDULED_TASKS_CONFIG_PATH,
    load_scheduled_tasks_config,
)
from ai_trading_system.scoring.daily import default_daily_score_report_path

ops_app = typer.Typer(help="运行监控和 pipeline health。", no_args_is_help=True)
console = Console()

_FINAL_REPORT_QUALITY_ALLOWED_STATUSES = frozenset({"PASS", "PASS_WITH_WARNINGS"})
_FINAL_READER_QUALITY_ALLOWED_STATUSES = frozenset(
    {"OK", "PASS_WITH_WARNINGS", "LIMITED_READER_CONTEXT"}
)
_FINAL_READER_QUALITY_LIMITED_STATUSES = frozenset({"PASS_WITH_WARNINGS", "LIMITED_READER_CONTEXT"})
_FINAL_READER_ARTIFACT_PREFIXES = (
    "reader_brief_",
    "owner_daily_brief_",
    "reader_brief_quality_",
    "report_quality_gate_",
)
_CANONICAL_DAILY_OPS_RUNNER = run_daily_ops_plan


class DailyOpsCanonicalFinalizationError(RuntimeError):
    """Typed failure raised before canonical daily run-control may declare PASS."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        evidence_path: Path | None = None,
    ) -> None:
        self.code = code
        self.message = message
        self.evidence_path = evidence_path
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class DailyOpsCanonicalFinalizationResult:
    status: str
    periodic_plan_path: Path
    daily_task_dashboard_path: Path
    daily_task_dashboard_json_path: Path
    daily_decision_summary_path: Path
    order_intent_candidates_path: Path
    reader_brief_final_paths: tuple[Path, ...]
    report_quality_paths: tuple[Path, ...]
    reader_quality_paths: tuple[Path, ...]
    finalization_evidence_path: Path
    canonical_outputs: tuple[Path, ...]
    legacy_outputs: tuple[Path, ...]
    report_quality_status: str | None
    reader_quality_status: str | None


@dataclass(frozen=True)
class _FinalReaderBriefQualityResult:
    report_quality_paths: tuple[Path, Path]
    reader_quality_paths: tuple[Path, Path]
    report_quality_status: str
    reader_quality_status: str
    reader_brief_bytes: Mapping[str, object]
    blocker_code: str | None = None
    blocker_message: str | None = None


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
) -> tuple[date, DailyOpsPlan]:
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
    shadow_path = daily_ops_shadow_path_for_plan(report_path)
    write_daily_ops_shadow_plan(plan, shadow_path)

    status = plan.status(os.environ)
    style = "green" if status == "READY" else "yellow" if status == "READY_WITH_SKIPS" else "red"
    missing_env = plan.missing_env_vars(os.environ)
    console.print(f"[{style}]每日运行计划：{status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(f"Canonical shadow：{shadow_path}")
    console.print(f"步骤数：{len(plan.steps)}")
    if missing_env:
        console.print(f"缺失环境变量：{', '.join(missing_env)}")
    if fail_on_missing_env and missing_env:
        raise typer.Exit(code=1)


def _refresh_reader_brief_from_daily_run_summary(
    *,
    as_of: date,
    reports_dir: Path,
    trace_bundle_path: Path,
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
        trace_bundle_path=trace_bundle_path,
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
    owner_json_path, owner_html_path = write_owner_daily_brief_sidecars(
        payload,
        output_dir=reports_dir,
    )
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
    return (
        html_path,
        json_path,
        quality_json_path,
        quality_md_path,
        owner_json_path,
        owner_html_path,
    )


def _default_daily_ops_finalization_evidence_path(
    reports_dir: Path,
    as_of: date,
) -> Path:
    return reports_dir / f"daily_ops_finalization_{as_of.isoformat()}.json"


def _artifact_byte_identity(path: Path) -> dict[str, object]:
    content = path.read_bytes()
    return {
        "path": str(path),
        "sha256": sha256_bytes(content),
        "size_bytes": len(content),
    }


def _strict_json_mapping(path: Path, *, error_code: str) -> dict[str, object]:
    try:
        payload = load_strict_json_path(path)
    except StrictJsonContractError as exc:
        raise DailyOpsCanonicalFinalizationError(
            error_code,
            f"strict JSON load failed: {path}: {exc}",
        ) from exc
    if not isinstance(payload, dict):
        raise DailyOpsCanonicalFinalizationError(
            error_code,
            f"JSON root must be an object: {path}",
        )
    return payload


def _artifact_record_matches_path(record: object, path: Path) -> bool:
    if not isinstance(record, Mapping):
        return False
    identity = _artifact_byte_identity(path)
    return (
        record.get("path") == identity["path"]
        and record.get("exists", True) is True
        and record.get("sha256") == identity["sha256"]
        and record.get("size_bytes") == identity["size_bytes"]
    )


def _load_report_index_for_finalization(
    *,
    report_index_path: Path,
    as_of: date,
) -> tuple[dict[str, object], str | None]:
    if not report_index_path.exists():
        return (
            {
                "schema_version": 1,
                "report_type": "report_index",
                "as_of": as_of.isoformat(),
                "status": "MISSING",
                "production_effect": "none",
                "reports": [],
                "summary": {"report_count": 0},
            },
            f"report_index JSON not found: {report_index_path}",
        )
    try:
        raw = load_strict_json_path(report_index_path)
    except StrictJsonContractError as exc:
        return (
            {
                "schema_version": 1,
                "report_type": "report_index",
                "as_of": as_of.isoformat(),
                "status": "FAILED",
                "production_effect": "none",
                "reports": [],
                "summary": {"report_count": 0},
            },
            f"report_index JSON cannot be loaded: {report_index_path}: {exc}",
        )
    if not isinstance(raw, dict):
        return (
            {
                "schema_version": 1,
                "report_type": "report_index",
                "as_of": as_of.isoformat(),
                "status": "FAILED",
                "production_effect": "none",
                "reports": [],
                "summary": {"report_count": 0},
            },
            f"report_index JSON must be an object: {report_index_path}",
        )
    schema_version = raw.get("schema_version")
    reports = raw.get("reports")
    summary = raw.get("summary")
    report_count = summary.get("report_count") if isinstance(summary, Mapping) else None
    if (
        type(schema_version) is not int
        or schema_version != 1
        or raw.get("report_type") != "report_index"
        or raw.get("production_effect") != "none"
        or raw.get("status") not in {"PASS", "PASS_WITH_WARNINGS", "PASS_WITH_EXPLICIT_WAIVERS"}
        or not isinstance(reports, list)
        or any(not isinstance(item, Mapping) for item in reports)
        or not isinstance(summary, Mapping)
        or type(report_count) is not int
        or report_count != len(reports)
    ):
        return (
            {
                **raw,
                "status": "FAILED",
            },
            (
                "report_index contract invalid: requires integer schema_version=1, "
                "report_type=report_index, production_effect=none and an allowed "
                "success status plus a consistent reports list/summary count: "
                f"{report_index_path}"
            ),
        )
    observed_as_of = str(raw.get("as_of") or "")
    if observed_as_of != as_of.isoformat():
        return (
            {
                **raw,
                "status": "FAILED",
            },
            (
                "report_index as_of mismatch: "
                f"expected={as_of.isoformat()} actual={observed_as_of or 'MISSING'}"
            ),
        )
    return raw, None


def _final_quality_status_blocker(
    *,
    report_quality_status: str,
    reader_quality_status: str,
    report_quality_production_effect: object,
    reader_quality_production_effect: object,
    report_quality_as_of: object,
    reader_quality_as_of: object,
    expected_as_of: date,
) -> tuple[str | None, str | None]:
    expected_as_of_text = expected_as_of.isoformat()
    if report_quality_as_of != expected_as_of_text:
        return (
            "DAILY_FINALIZATION_REPORT_QUALITY_AS_OF_MISMATCH",
            (
                "final report quality as_of mismatch: "
                f"expected={expected_as_of_text} actual={report_quality_as_of!r}"
            ),
        )
    if reader_quality_as_of != expected_as_of_text:
        return (
            "DAILY_FINALIZATION_READER_QUALITY_AS_OF_MISMATCH",
            (
                "final Reader Brief quality as_of mismatch: "
                f"expected={expected_as_of_text} actual={reader_quality_as_of!r}"
            ),
        )
    if report_quality_production_effect != "none":
        return (
            "DAILY_FINALIZATION_REPORT_QUALITY_PRODUCTION_EFFECT_INVALID",
            (
                "final report quality must declare production_effect=none; "
                f"actual={report_quality_production_effect!r}"
            ),
        )
    if reader_quality_production_effect != "none":
        return (
            "DAILY_FINALIZATION_READER_QUALITY_PRODUCTION_EFFECT_INVALID",
            (
                "final Reader Brief quality must declare production_effect=none; "
                f"actual={reader_quality_production_effect!r}"
            ),
        )
    if report_quality_status == "FAIL":
        return (
            "DAILY_FINALIZATION_REPORT_QUALITY_FAILED",
            "final Reader Brief bytes failed the report quality gate",
        )
    if report_quality_status not in _FINAL_REPORT_QUALITY_ALLOWED_STATUSES:
        return (
            "DAILY_FINALIZATION_REPORT_QUALITY_STATUS_INVALID",
            f"unknown final report quality status={report_quality_status!r}",
        )
    if reader_quality_status == "FAILED":
        return (
            "DAILY_FINALIZATION_READER_QUALITY_FAILED",
            "final Reader Brief bytes failed Reader Brief quality validation",
        )
    if reader_quality_status not in _FINAL_READER_QUALITY_ALLOWED_STATUSES:
        return (
            "DAILY_FINALIZATION_READER_QUALITY_STATUS_INVALID",
            f"unknown final Reader Brief quality status={reader_quality_status!r}",
        )
    return None, None


def _raise_for_final_quality_failure(
    quality_result: _FinalReaderBriefQualityResult,
    *,
    evidence_path: Path,
) -> None:
    if quality_result.blocker_code is None:
        return
    raise DailyOpsCanonicalFinalizationError(
        quality_result.blocker_code,
        quality_result.blocker_message or quality_result.blocker_code,
        evidence_path=evidence_path,
    )


def _regenerate_final_reader_brief_quality(
    *,
    as_of: date,
    reports_dir: Path,
    project_root: Path,
    reader_brief_json_path: Path,
    reader_brief_html_path: Path,
) -> _FinalReaderBriefQualityResult:
    try:
        reader_brief_json_bytes = reader_brief_json_path.read_bytes()
        reader_brief_html_bytes = reader_brief_html_path.read_bytes()
    except OSError as exc:
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_MISSING",
            f"cannot read final Reader Brief artifacts: {exc}",
        ) from exc
    if not reader_brief_json_bytes or not reader_brief_html_bytes:
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_EMPTY",
            "final Reader Brief JSON and HTML must both be non-empty",
        )
    try:
        raw_reader_brief = load_strict_json_text(
            reader_brief_json_bytes.decode("utf-8"),
            label=str(reader_brief_json_path),
        )
    except (UnicodeError, StrictJsonContractError) as exc:
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_INVALID",
            f"final Reader Brief JSON cannot be parsed: {reader_brief_json_path}",
        ) from exc
    if not isinstance(raw_reader_brief, dict):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_INVALID",
            f"final Reader Brief JSON must be an object: {reader_brief_json_path}",
        )
    schema_version = raw_reader_brief.get("schema_version")
    if (
        type(schema_version) is not int
        or schema_version != 1
        or raw_reader_brief.get("report_type") != "reader_brief"
    ):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_CONTRACT_INVALID",
            (
                "final Reader Brief requires integer schema_version=1 and "
                f"report_type=reader_brief: {reader_brief_json_path}"
            ),
        )
    observed_as_of = str(raw_reader_brief.get("as_of") or "")
    if observed_as_of != as_of.isoformat():
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_AS_OF_MISMATCH",
            (
                "final Reader Brief as_of mismatch: "
                f"expected={as_of.isoformat()} actual={observed_as_of or 'MISSING'}"
            ),
        )
    if raw_reader_brief.get("production_effect") != "none":
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_PRODUCTION_EFFECT_INVALID",
            (
                "final Reader Brief must declare production_effect=none; "
                f"actual={raw_reader_brief.get('production_effect')!r}"
            ),
        )
    reader_brief_status = raw_reader_brief.get("status")
    if reader_brief_status == "FAILED":
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_SOURCE_FAILED",
            "final Reader Brief source status is FAILED",
        )
    if (
        not isinstance(reader_brief_status, str)
        or reader_brief_status not in _FINAL_READER_QUALITY_ALLOWED_STATUSES
    ):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BRIEF_STATUS_INVALID",
            f"unknown final Reader Brief source status={reader_brief_status!r}",
        )

    reader_brief_json_sha256 = sha256_bytes(reader_brief_json_bytes)
    reader_brief_html_sha256 = sha256_bytes(reader_brief_html_bytes)
    reader_brief_bytes: dict[str, object] = {
        "binding_type": "final_reader_brief_bytes.v1",
        "as_of": as_of.isoformat(),
        "reader_brief_json": {
            "path": str(reader_brief_json_path),
            "sha256": reader_brief_json_sha256,
            "size_bytes": len(reader_brief_json_bytes),
        },
        "reader_brief_html": {
            "path": str(reader_brief_html_path),
            "sha256": reader_brief_html_sha256,
            "size_bytes": len(reader_brief_html_bytes),
        },
    }
    report_index_path = default_report_index_json_path(reports_dir, as_of)
    report_index_payload, report_index_load_error = _load_report_index_for_finalization(
        report_index_path=report_index_path,
        as_of=as_of,
    )
    report_quality_payload = build_report_quality_gate_payload(
        as_of=as_of,
        report_index_payload=report_index_payload,
        report_index_path=report_index_path,
        reader_brief_payload=raw_reader_brief,
        reader_brief_json_path=reader_brief_json_path,
        project_root=project_root,
    )
    reader_quality_payload = build_reader_brief_quality_payload(
        reader_brief_payload=raw_reader_brief,
        reader_brief_json_path=reader_brief_json_path,
        reader_brief_html_path=reader_brief_html_path,
    )
    report_quality_payload["final_reader_brief_bytes"] = reader_brief_bytes
    reader_quality_payload["final_reader_brief_bytes"] = reader_brief_bytes
    if report_index_load_error is not None:
        report_quality_payload["report_index_load_error"] = report_index_load_error

    report_quality_json_path = default_report_quality_gate_json_path(reports_dir, as_of)
    report_quality_markdown_path = default_report_quality_gate_markdown_path(reports_dir, as_of)
    reader_quality_json_path = default_reader_brief_quality_json_path(reports_dir, as_of)
    reader_quality_markdown_path = default_reader_brief_quality_markdown_path(reports_dir, as_of)
    write_report_quality_gate_json(report_quality_payload, report_quality_json_path)
    write_report_quality_gate_markdown(report_quality_payload, report_quality_markdown_path)
    write_reader_brief_quality_json(reader_quality_payload, reader_quality_json_path)
    write_reader_brief_quality_markdown(reader_quality_payload, reader_quality_markdown_path)

    blocker_code: str | None
    blocker_message: str | None
    if (
        sha256_path(reader_brief_json_path) != reader_brief_json_sha256
        or sha256_path(reader_brief_html_path) != reader_brief_html_sha256
    ):
        blocker_code = "DAILY_FINALIZATION_READER_BRIEF_BYTES_CHANGED"
        blocker_message = "final Reader Brief bytes changed while quality evidence was generated"
    else:
        report_quality_status = str(report_quality_payload.get("report_quality_status") or "")
        reader_quality_status = str(reader_quality_payload.get("status") or "")
        blocker_code, blocker_message = _final_quality_status_blocker(
            report_quality_status=report_quality_status,
            reader_quality_status=reader_quality_status,
            report_quality_production_effect=report_quality_payload.get("production_effect"),
            reader_quality_production_effect=reader_quality_payload.get("production_effect"),
            report_quality_as_of=report_quality_payload.get("as_of"),
            reader_quality_as_of=reader_quality_payload.get("as_of"),
            expected_as_of=as_of,
        )
    return _FinalReaderBriefQualityResult(
        report_quality_paths=(report_quality_json_path, report_quality_markdown_path),
        reader_quality_paths=(reader_quality_json_path, reader_quality_markdown_path),
        report_quality_status=str(report_quality_payload.get("report_quality_status") or ""),
        reader_quality_status=str(reader_quality_payload.get("status") or ""),
        reader_brief_bytes=reader_brief_bytes,
        blocker_code=blocker_code,
        blocker_message=blocker_message,
    )


def _write_daily_ops_finalization_evidence(
    *,
    output_path: Path,
    run_report: DailyOpsRunReport,
    status: str,
    canonical_outputs: tuple[Path, ...],
    legacy_outputs: tuple[Path, ...],
    quality_result: _FinalReaderBriefQualityResult | None,
    blocker_code: str | None = None,
    blocker_message: str | None = None,
) -> Path:
    quality_artifacts: dict[str, object] = {}
    if quality_result is not None:
        quality_artifacts = {
            "report_quality_json": _artifact_byte_identity(quality_result.report_quality_paths[0]),
            "report_quality_markdown": _artifact_byte_identity(
                quality_result.report_quality_paths[1]
            ),
            "reader_quality_json": _artifact_byte_identity(quality_result.reader_quality_paths[0]),
            "reader_quality_markdown": _artifact_byte_identity(
                quality_result.reader_quality_paths[1]
            ),
        }
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": "daily_ops_canonical_finalization",
        "as_of": run_report.plan.as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "run_id": run_report.metadata.run_id if run_report.metadata is not None else "UNKNOWN",
        "status": status,
        "source_daily_run_status": run_report.status,
        "report_quality_status": (
            None if quality_result is None else quality_result.report_quality_status
        ),
        "reader_quality_status": (
            None if quality_result is None else quality_result.reader_quality_status
        ),
        "reader_quality_limited": (
            quality_result is not None
            and quality_result.reader_quality_status in _FINAL_READER_QUALITY_LIMITED_STATUSES
        ),
        "final_reader_brief_bytes": (
            {} if quality_result is None else quality_result.reader_brief_bytes
        ),
        "quality_artifacts": quality_artifacts,
        "canonical_outputs": [str(path) for path in canonical_outputs],
        "legacy_outputs": [str(path) for path in legacy_outputs],
        "blocker": (
            None
            if blocker_code is None
            else {
                "code": blocker_code,
                "message": blocker_message or blocker_code,
            }
        ),
        "safety_boundary": {
            "production_effect": "none",
            "writes_production_weights": False,
            "writes_active_shadow_weights": False,
            "broker_action_allowed": False,
            "broker_action_taken": False,
        },
        "production_effect": "none",
    }
    write_json_atomic(
        output_path,
        payload,
        sort_keys=True,
        trailing_newline=False,
    )
    return output_path


def _downgrade_daily_ops_finalization_evidence(
    *,
    evidence_path: Path,
    run_report: DailyOpsRunReport,
    blocker_code: str,
    blocker_message: str,
) -> Path:
    prior_load_error: str | None = None
    try:
        raw = load_strict_json_path(evidence_path)
        payload = dict(raw) if isinstance(raw, Mapping) else {}
        if not isinstance(raw, Mapping):
            prior_load_error = f"root_type={type(raw).__name__}"
    except StrictJsonContractError as exc:
        payload = {}
        prior_load_error = str(exc)
    payload.update(
        {
            "schema_version": 1,
            "report_type": "daily_ops_canonical_finalization",
            "as_of": run_report.plan.as_of.isoformat(),
            "run_id": (
                run_report.metadata.run_id if run_report.metadata is not None else "UNKNOWN"
            ),
            "status": "FAILED",
            "source_daily_run_status": run_report.status,
            "blocker": {
                "code": blocker_code,
                "message": blocker_message,
            },
            "safety_boundary": {
                "production_effect": "none",
                "writes_production_weights": False,
                "writes_active_shadow_weights": False,
                "broker_action_allowed": False,
                "broker_action_taken": False,
            },
            "production_effect": "none",
            "failure_recorded_at": datetime.now(tz=UTC).isoformat(),
        }
    )
    if prior_load_error is not None:
        payload["prior_evidence_load_error"] = prior_load_error
    write_json_atomic(
        evidence_path,
        payload,
        sort_keys=True,
        trailing_newline=False,
    )
    return evidence_path


def _validate_daily_ops_finalization_closure(
    *,
    run_report: DailyOpsRunReport,
    finalization: DailyOpsCanonicalFinalizationResult,
    run_paths: RunArtifactPaths,
    metadata_path: Path,
    legacy_reports_dir: Path,
    legacy_mode: str,
    expected_manifest_status: str,
    manifest_payload: Mapping[str, object] | None = None,
) -> None:
    evidence = _strict_json_mapping(
        finalization.finalization_evidence_path,
        error_code="DAILY_FINALIZATION_EVIDENCE_INVALID",
    )
    evidence_schema_version = evidence.get("schema_version")
    if (
        type(evidence_schema_version) is not int
        or evidence_schema_version != 1
        or evidence.get("report_type") != "daily_ops_canonical_finalization"
    ):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_EVIDENCE_CONTRACT_INVALID",
            "finalization evidence requires integer schema_version=1 and canonical report_type",
            evidence_path=finalization.finalization_evidence_path,
        )
    if evidence.get("status") != finalization.status:
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_EVIDENCE_STATUS_MISMATCH",
            (f"expected={finalization.status!r} " f"actual={evidence.get('status')!r}"),
            evidence_path=finalization.finalization_evidence_path,
        )
    if evidence.get("as_of") != run_report.plan.as_of.isoformat():
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_EVIDENCE_AS_OF_MISMATCH",
            f"actual={evidence.get('as_of')!r}",
            evidence_path=finalization.finalization_evidence_path,
        )
    if evidence.get("production_effect") != "none":
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_EVIDENCE_PRODUCTION_EFFECT_INVALID",
            f"actual={evidence.get('production_effect')!r}",
            evidence_path=finalization.finalization_evidence_path,
        )
    if (
        evidence.get("run_id") != run_paths.run_id
        or evidence.get("source_daily_run_status") != run_report.status
        or evidence.get("blocker") is not None
    ):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_EVIDENCE_RUN_CONTEXT_INVALID",
            (
                f"run_id={evidence.get('run_id')!r} "
                f"source_status={evidence.get('source_daily_run_status')!r} "
                f"blocker={evidence.get('blocker')!r}"
            ),
            evidence_path=finalization.finalization_evidence_path,
        )
    safety_boundary = evidence.get("safety_boundary")
    expected_safety_boundary = {
        "production_effect": "none",
        "writes_production_weights": False,
        "writes_active_shadow_weights": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }
    if safety_boundary != expected_safety_boundary:
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_EVIDENCE_SAFETY_BOUNDARY_INVALID",
            f"actual={safety_boundary!r}",
            evidence_path=finalization.finalization_evidence_path,
        )
    if evidence.get("canonical_outputs") != [
        str(path) for path in finalization.canonical_outputs
    ] or evidence.get("legacy_outputs") != [str(path) for path in finalization.legacy_outputs]:
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_EVIDENCE_OUTPUT_SET_INVALID",
            "finalization evidence output sets differ from the finalized result",
            evidence_path=finalization.finalization_evidence_path,
        )

    binding = evidence.get("final_reader_brief_bytes")
    if not isinstance(binding, Mapping):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BINDING_MISSING",
            "finalization evidence lacks final_reader_brief_bytes",
            evidence_path=finalization.finalization_evidence_path,
        )
    if binding and (
        binding.get("binding_type") != "final_reader_brief_bytes.v1"
        or binding.get("as_of") != run_report.plan.as_of.isoformat()
    ):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_BINDING_CONTRACT_INVALID",
            (f"binding_type={binding.get('binding_type')!r} " f"as_of={binding.get('as_of')!r}"),
            evidence_path=finalization.finalization_evidence_path,
        )
    reader_outputs_present = len(finalization.reader_brief_final_paths) >= 2
    if not reader_outputs_present and (
        finalization.reader_brief_final_paths
        or finalization.report_quality_paths
        or finalization.reader_quality_paths
        or binding
        or finalization.report_quality_status is not None
        or finalization.reader_quality_status is not None
        or evidence.get("report_quality_status") is not None
        or evidence.get("reader_quality_status") is not None
        or evidence.get("quality_artifacts") not in ({}, None)
    ):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_READER_OUTPUT_SET_INCONSISTENT",
            "Reader Brief/quality outputs must be either complete or entirely not due",
            evidence_path=finalization.finalization_evidence_path,
        )
    if not reader_outputs_present:
        as_of_text = run_report.plan.as_of.isoformat()
        stale_reader_outputs = tuple(
            path
            for path in (
                *finalization.canonical_outputs,
                *finalization.legacy_outputs,
                *run_paths.reports_dir.rglob(f"*{as_of_text}*"),
                *legacy_reports_dir.rglob(f"*{as_of_text}*"),
            )
            if path.is_file() and path.name.startswith(_FINAL_READER_ARTIFACT_PREFIXES)
        )
        if stale_reader_outputs:
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_NOT_DUE_READER_ARTIFACT_PRESENT",
                "Reader Brief is not due but dated Reader/quality artifacts are present: "
                + ", ".join(str(path) for path in stale_reader_outputs),
                evidence_path=finalization.finalization_evidence_path,
            )
    if reader_outputs_present:
        if (
            len(finalization.report_quality_paths) != 2
            or len(finalization.reader_quality_paths) != 2
        ):
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_QUALITY_OUTPUTS_MISSING",
                "final Reader Brief requires complete report/reader quality outputs",
                evidence_path=finalization.finalization_evidence_path,
            )
        reader_html_path = finalization.reader_brief_final_paths[0]
        reader_json_path = finalization.reader_brief_final_paths[1]
        for key, path in (
            ("reader_brief_json", reader_json_path),
            ("reader_brief_html", reader_html_path),
        ):
            if not _artifact_record_matches_path(binding.get(key), path):
                raise DailyOpsCanonicalFinalizationError(
                    "DAILY_FINALIZATION_READER_BYTES_DRIFT",
                    f"final Reader Brief binding drift: {key} path={path}",
                    evidence_path=finalization.finalization_evidence_path,
                )

        report_quality = _strict_json_mapping(
            finalization.report_quality_paths[0],
            error_code="DAILY_FINALIZATION_REPORT_QUALITY_INVALID",
        )
        reader_quality = _strict_json_mapping(
            finalization.reader_quality_paths[0],
            error_code="DAILY_FINALIZATION_READER_QUALITY_INVALID",
        )
        report_schema_version = report_quality.get("schema_version")
        reader_schema_version = reader_quality.get("schema_version")
        if (
            type(report_schema_version) is not int
            or report_schema_version != 1
            or report_quality.get("report_type") != "report_quality_gate"
            or type(reader_schema_version) is not int
            or reader_schema_version != 1
            or reader_quality.get("report_type") != "reader_brief_quality"
        ):
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_QUALITY_CONTRACT_INVALID",
                "final report/Reader quality schema or report_type is invalid",
                evidence_path=finalization.finalization_evidence_path,
            )
        report_quality_status = str(report_quality.get("report_quality_status") or "")
        reader_quality_status = str(reader_quality.get("status") or "")
        blocker_code, blocker_message = _final_quality_status_blocker(
            report_quality_status=report_quality_status,
            reader_quality_status=reader_quality_status,
            report_quality_production_effect=report_quality.get("production_effect"),
            reader_quality_production_effect=reader_quality.get("production_effect"),
            report_quality_as_of=report_quality.get("as_of"),
            reader_quality_as_of=reader_quality.get("as_of"),
            expected_as_of=run_report.plan.as_of,
        )
        if blocker_code is not None:
            raise DailyOpsCanonicalFinalizationError(
                blocker_code,
                blocker_message or blocker_code,
                evidence_path=finalization.finalization_evidence_path,
            )
        if (
            report_quality.get("status") != report_quality_status
            or report_quality_status != finalization.report_quality_status
            or reader_quality_status != finalization.reader_quality_status
            or evidence.get("report_quality_status") != report_quality_status
            or evidence.get("reader_quality_status") != reader_quality_status
        ):
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_QUALITY_STATUS_DRIFT",
                "final quality statuses differ across artifacts/evidence/result",
                evidence_path=finalization.finalization_evidence_path,
            )
        if report_quality.get("final_reader_brief_bytes") != binding:
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_REPORT_QUALITY_BINDING_DRIFT",
                "report quality does not bind the final Reader Brief bytes",
                evidence_path=finalization.finalization_evidence_path,
            )
        if reader_quality.get("final_reader_brief_bytes") != binding:
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_READER_QUALITY_BINDING_DRIFT",
                "Reader Brief quality does not bind the final Reader Brief bytes",
                evidence_path=finalization.finalization_evidence_path,
            )

        quality_artifacts = evidence.get("quality_artifacts")
        if not isinstance(quality_artifacts, Mapping):
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_QUALITY_ARTIFACT_BINDING_MISSING",
                "finalization evidence lacks quality_artifacts",
                evidence_path=finalization.finalization_evidence_path,
            )
        for key, path in (
            ("report_quality_json", finalization.report_quality_paths[0]),
            ("report_quality_markdown", finalization.report_quality_paths[1]),
            ("reader_quality_json", finalization.reader_quality_paths[0]),
            ("reader_quality_markdown", finalization.reader_quality_paths[1]),
        ):
            if not _artifact_record_matches_path(quality_artifacts.get(key), path):
                raise DailyOpsCanonicalFinalizationError(
                    "DAILY_FINALIZATION_QUALITY_ARTIFACT_BYTES_DRIFT",
                    f"quality artifact binding drift: {key} path={path}",
                    evidence_path=finalization.finalization_evidence_path,
                )

    if legacy_mode == "mirror":
        final_canonical_paths = (
            *finalization.reader_brief_final_paths,
            *finalization.report_quality_paths,
            *finalization.reader_quality_paths,
        )
        for canonical_path in final_canonical_paths:
            legacy_path = legacy_reports_dir / canonical_path.name
            if not legacy_path.exists() or legacy_path.read_bytes() != canonical_path.read_bytes():
                raise DailyOpsCanonicalFinalizationError(
                    "DAILY_FINALIZATION_LEGACY_MIRROR_DRIFT",
                    f"canonical/legacy bytes differ: {canonical_path} -> {legacy_path}",
                    evidence_path=finalization.finalization_evidence_path,
                )

    decision_summary = _strict_json_mapping(
        finalization.daily_decision_summary_path,
        error_code="DAILY_FINALIZATION_DECISION_SUMMARY_INVALID",
    )
    checksums = decision_summary.get("checksums")
    expected_metadata_sha = sha256_path(metadata_path)
    if (
        not isinstance(checksums, Mapping)
        or checksums.get("daily_ops_metadata") != expected_metadata_sha
    ):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_METADATA_BINDING_DRIFT",
            (
                "daily decision summary metadata checksum mismatch: "
                f"expected={expected_metadata_sha}"
            ),
            evidence_path=finalization.finalization_evidence_path,
        )

    manifest = (
        _strict_json_mapping(
            run_paths.manifest_path,
            error_code="DAILY_FINALIZATION_MANIFEST_INVALID",
        )
        if manifest_payload is None
        else dict(manifest_payload)
    )
    manifest_schema_version = manifest.get("schema_version")
    if (
        type(manifest_schema_version) is not int
        or manifest_schema_version != 1
        or manifest.get("report_type") != "daily_run_manifest"
        or manifest.get("run_id") != run_paths.run_id
        or manifest.get("status") != expected_manifest_status
        or manifest.get("as_of") != run_report.plan.as_of.isoformat()
        or manifest.get("production_effect") != "none"
    ):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_MANIFEST_STATUS_INVALID",
            (
                f"expected_status={expected_manifest_status!r} "
                f"status={manifest.get('status')!r} "
                f"as_of={manifest.get('as_of')!r} "
                f"production_effect={manifest.get('production_effect')!r}"
            ),
            evidence_path=finalization.finalization_evidence_path,
        )
    raw_output_records = manifest.get("output_artifacts")
    if not isinstance(raw_output_records, list):
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_MANIFEST_OUTPUTS_INVALID",
            "manifest output_artifacts must be a list",
            evidence_path=finalization.finalization_evidence_path,
        )
    output_records = {
        str(record.get("path")): record
        for record in raw_output_records
        if isinstance(record, Mapping)
    }
    required_output_paths = tuple(
        dict.fromkeys(
            (
                finalization.finalization_evidence_path,
                metadata_path,
                finalization.periodic_plan_path,
                finalization.daily_task_dashboard_path,
                finalization.daily_task_dashboard_json_path,
                finalization.daily_decision_summary_path,
                finalization.order_intent_candidates_path,
                *finalization.canonical_outputs,
            )
        )
    )
    for path in required_output_paths:
        if not _artifact_record_matches_path(output_records.get(str(path)), path):
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_MANIFEST_OUTPUT_DRIFT",
                f"manifest output identity drift: {path}",
                evidence_path=finalization.finalization_evidence_path,
            )
    if legacy_mode == "mirror":
        raw_legacy_records = manifest.get("legacy_output_artifacts")
        if not isinstance(raw_legacy_records, list):
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_MANIFEST_LEGACY_OUTPUTS_INVALID",
                "manifest legacy_output_artifacts must be a list",
                evidence_path=finalization.finalization_evidence_path,
            )
        legacy_records = {
            str(record.get("path")): record
            for record in raw_legacy_records
            if isinstance(record, Mapping)
        }
        for path in finalization.legacy_outputs:
            if not _artifact_record_matches_path(legacy_records.get(str(path)), path):
                raise DailyOpsCanonicalFinalizationError(
                    "DAILY_FINALIZATION_MANIFEST_LEGACY_OUTPUT_DRIFT",
                    f"manifest legacy output identity drift: {path}",
                    evidence_path=finalization.finalization_evidence_path,
                )


def _is_resumed_daily_step(result: object) -> bool:
    return getattr(result, "status", None) == "SKIPPED" and str(
        getattr(result, "skip_reason", "") or ""
    ).startswith("Canonical resume")


def _freeze_executor_metadata_before_finalization(
    run_report: DailyOpsRunReport,
    *,
    legacy_reports_dir: Path,
    as_of: date,
) -> DailyOpsRunReport:
    metadata = run_report.metadata
    if metadata is None:
        return run_report
    suffix = as_of.isoformat()
    finalization_owned_paths = {
        legacy_reports_dir / f"reader_brief_{suffix}.json",
        legacy_reports_dir / f"reader_brief_{suffix}.html",
        legacy_reports_dir / f"owner_daily_brief_{suffix}.json",
        legacy_reports_dir / f"owner_daily_brief_{suffix}.html",
        legacy_reports_dir / f"reader_brief_quality_{suffix}.json",
        legacy_reports_dir / f"reader_brief_quality_{suffix}.md",
        legacy_reports_dir / f"report_quality_gate_{suffix}.json",
        legacy_reports_dir / f"report_quality_gate_{suffix}.md",
    }
    stable_produced_artifacts = tuple(
        artifact
        for artifact in metadata.produced_artifacts
        if artifact.path not in finalization_owned_paths
    )
    return replace(
        run_report,
        metadata=replace(
            metadata,
            produced_artifacts=stable_produced_artifacts,
        ),
    )


def _finalize_daily_ops_canonical_outputs(
    *,
    run_report: DailyOpsRunReport,
    run_paths: RunArtifactPaths,
    plan_date: date,
    run_report_path: Path,
    metadata_path: Path,
    legacy_reports_dir: Path,
    legacy_mode: str,
    project_root: Path,
) -> DailyOpsCanonicalFinalizationResult:
    finalization_evidence_path = _default_daily_ops_finalization_evidence_path(
        run_paths.reports_dir,
        plan_date,
    )
    canonical_outputs: tuple[Path, ...] = ()
    legacy_outputs: tuple[Path, ...] = ()
    quality_result: _FinalReaderBriefQualityResult | None = None
    try:
        run_report = _freeze_executor_metadata_before_finalization(
            run_report,
            legacy_reports_dir=legacy_reports_dir,
            as_of=plan_date,
        )
        write_daily_ops_run_report(
            run_report,
            run_report_path,
            metadata_path=metadata_path,
        )
        periodic_plan_path = _write_periodic_plan_from_daily_run(
            run_report=run_report,
            output_root=run_paths.metadata_dir,
            project_root=project_root,
        )
        reader_brief_step = next(
            (result for result in run_report.step_results if result.step_id == "reader_brief"),
            None,
        )
        planned_reader_brief_step = next(
            (step for step in run_report.plan.steps if step.step_id == "reader_brief"),
            None,
        )
        refresh_reader_brief = reader_brief_step is not None and (
            reader_brief_step.status == "PASS" or _is_resumed_daily_step(reader_brief_step)
        )
        reader_brief_expected = (
            planned_reader_brief_step is not None and planned_reader_brief_step.enabled
        ) or (reader_brief_step is not None and _is_resumed_daily_step(reader_brief_step))
        has_resumed_steps = any(_is_resumed_daily_step(item) for item in run_report.step_results)
        canonical_outputs = mirror_legacy_reports_to_run(
            as_of=plan_date,
            legacy_reports_dir=legacy_reports_dir,
            paths=run_paths,
            min_modified_at=None if has_resumed_steps else run_report.started_at,
            excluded_name_prefixes=(
                () if reader_brief_expected else _FINAL_READER_ARTIFACT_PREFIXES
            ),
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
            project_root=project_root,
        )
        if (
            run_report.status in {"PASS", "PASS_WITH_SKIPS"}
            and reader_brief_expected
            and not refresh_reader_brief
        ):
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_READER_BRIEF_STEP_INVALID",
                (
                    "successful daily run requires reader_brief PASS or canonical-resume "
                    "evidence before final refresh"
                ),
            )
        if refresh_reader_brief:
            reader_brief_final_paths = _refresh_reader_brief_from_daily_run_summary(
                as_of=plan_date,
                reports_dir=run_paths.reports_dir,
                trace_bundle_path=(
                    run_paths.traces_dir
                    / default_report_trace_bundle_path(
                        default_daily_score_report_path(run_paths.reports_dir, plan_date)
                    ).name
                ),
                daily_decision_summary_path=daily_decision_summary_path,
                daily_task_dashboard_json_path=daily_task_dashboard_json_path,
            )
            quality_result = _regenerate_final_reader_brief_quality(
                as_of=plan_date,
                reports_dir=run_paths.reports_dir,
                project_root=project_root,
                reader_brief_json_path=reader_brief_final_paths[1],
                reader_brief_html_path=reader_brief_final_paths[0],
            )
        else:
            reader_brief_final_paths = ()

        if legacy_mode == "mirror":
            legacy_outputs = mirror_canonical_daily_ops_outputs_to_legacy(
                paths=run_paths,
                legacy_reports_dir=legacy_reports_dir,
            )

        canonical_outputs = tuple(
            dict.fromkeys(
                (
                    *canonical_outputs,
                    run_report_path,
                    metadata_path,
                    periodic_plan_path,
                    daily_task_dashboard_path,
                    daily_task_dashboard_json_path,
                    daily_decision_summary_path,
                    order_intent_candidates_path,
                    *reader_brief_final_paths,
                    *(
                        ()
                        if quality_result is None
                        else (
                            *quality_result.report_quality_paths,
                            *quality_result.reader_quality_paths,
                        )
                    ),
                )
            )
        )
        blocker_code = None if quality_result is None else quality_result.blocker_code
        blocker_message = None if quality_result is None else quality_result.blocker_message
        if run_report.status not in {"PASS", "PASS_WITH_SKIPS"}:
            finalization_status = "FAILED"
        elif blocker_code is not None:
            finalization_status = "FAILED"
        elif (
            quality_result is None
            or quality_result.report_quality_status == "PASS_WITH_WARNINGS"
            or quality_result.reader_quality_status in _FINAL_READER_QUALITY_LIMITED_STATUSES
        ):
            finalization_status = "PASS_WITH_WARNINGS"
        else:
            finalization_status = "PASS"
        _write_daily_ops_finalization_evidence(
            output_path=finalization_evidence_path,
            run_report=run_report,
            status=finalization_status,
            canonical_outputs=canonical_outputs,
            legacy_outputs=legacy_outputs,
            quality_result=quality_result,
            blocker_code=blocker_code,
            blocker_message=blocker_message,
        )
        if quality_result is not None:
            _raise_for_final_quality_failure(
                quality_result,
                evidence_path=finalization_evidence_path,
            )
        return DailyOpsCanonicalFinalizationResult(
            status=finalization_status,
            periodic_plan_path=periodic_plan_path,
            daily_task_dashboard_path=daily_task_dashboard_path,
            daily_task_dashboard_json_path=daily_task_dashboard_json_path,
            daily_decision_summary_path=daily_decision_summary_path,
            order_intent_candidates_path=order_intent_candidates_path,
            reader_brief_final_paths=reader_brief_final_paths,
            report_quality_paths=(
                () if quality_result is None else quality_result.report_quality_paths
            ),
            reader_quality_paths=(
                () if quality_result is None else quality_result.reader_quality_paths
            ),
            finalization_evidence_path=finalization_evidence_path,
            canonical_outputs=canonical_outputs,
            legacy_outputs=legacy_outputs,
            report_quality_status=(
                None if quality_result is None else quality_result.report_quality_status
            ),
            reader_quality_status=(
                None if quality_result is None else quality_result.reader_quality_status
            ),
        )
    except DailyOpsCanonicalFinalizationError as exc:
        if exc.evidence_path is None:
            try:
                _write_daily_ops_finalization_evidence(
                    output_path=finalization_evidence_path,
                    run_report=run_report,
                    status="FAILED",
                    canonical_outputs=canonical_outputs,
                    legacy_outputs=legacy_outputs,
                    quality_result=quality_result,
                    blocker_code=exc.code,
                    blocker_message=exc.message,
                )
            except Exception:
                pass
            else:
                exc.evidence_path = finalization_evidence_path
        raise
    except Exception as exc:
        evidence_path: Path | None = None
        try:
            evidence_path = _write_daily_ops_finalization_evidence(
                output_path=finalization_evidence_path,
                run_report=run_report,
                status="FAILED",
                canonical_outputs=canonical_outputs,
                legacy_outputs=legacy_outputs,
                quality_result=quality_result,
                blocker_code="DAILY_FINALIZATION_UNHANDLED_EXCEPTION",
                blocker_message=str(exc),
            )
        except Exception:
            evidence_path = None
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_UNHANDLED_EXCEPTION",
            str(exc),
            evidence_path=evidence_path,
        ) from exc


def _run_daily_ops_with_completion_callback(
    plan: DailyOpsPlan,
    *,
    completion_callback: Callable[[DailyOpsRunReport], None],
    terminal_failure_callback: Callable[[DailyOpsRunReport, Exception], None],
    project_root: Path,
    env: Mapping[str, str],
    run_id: str,
    diagnostics_dir: Path,
) -> DailyOpsRunReport:
    runner_parameters = inspect.signature(run_daily_ops_plan).parameters
    runner_kwargs: dict[str, Any] = {
        "project_root": project_root,
        "env": env,
        "run_id": run_id,
        "diagnostics_dir": diagnostics_dir,
    }
    if "completion_callback" in runner_parameters:
        return run_daily_ops_plan(
            plan,
            **runner_kwargs,
            completion_callback=completion_callback,
            terminal_failure_callback=terminal_failure_callback,
        )
    if run_daily_ops_plan is _CANONICAL_DAILY_OPS_RUNNER:
        raise DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_CALLBACK_UNAVAILABLE",
            (
                "canonical controlled runner lacks completion_callback; refusing to "
                "finalize after run-control PASS"
            ),
        )
    report = run_daily_ops_plan(plan, **runner_kwargs)
    if not report.status.startswith("RUN_CONTROL_"):
        completion_callback(report)
    return report


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
    write_daily_ops_shadow_plan(
        plan,
        daily_ops_shadow_path_for_plan(plan_report_path),
    )
    if legacy_mode == "mirror":
        legacy_plan_path = write_daily_ops_plan(
            plan,
            default_daily_ops_plan_path(reports_dir, plan_date),
            env=os.environ,
        )
        write_daily_ops_shadow_plan(
            plan,
            daily_ops_shadow_path_for_plan(legacy_plan_path),
        )
    metadata_path = default_daily_ops_run_metadata_path(
        run_paths.metadata_dir,
        plan_date,
    )
    manifest_command = _daily_run_manifest_command(
        plan_date=plan_date,
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
        plan_output_path=plan_output_path,
        output_path=output_path,
        run_output_root=run_output_root,
        resolved_run_id=resolved_run_id,
        legacy_mode=legacy_mode,
    )
    finalization_result: DailyOpsCanonicalFinalizationResult | None = None

    def _build_completed_bundle_manifest(
        completed_report: DailyOpsRunReport,
        *,
        manifest_status: str,
        completed_finalization: DailyOpsCanonicalFinalizationResult | None,
        extra_warning: str | None = None,
    ) -> Mapping[str, object]:
        if completed_report.metadata is None:
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_RUN_METADATA_MISSING",
                "canonical bundle manifest requires daily run metadata",
                evidence_path=_default_daily_ops_finalization_evidence_path(
                    run_paths.reports_dir,
                    plan_date,
                ),
            )
        warnings = list(_daily_run_manifest_warnings(completed_report))
        if completed_finalization is not None:
            warnings.append(f"canonical_finalization:{completed_finalization.status}")
        if extra_warning is not None:
            warnings.append(extra_warning)
        return build_run_manifest(
            paths=run_paths,
            project_root=PROJECT_ROOT,
            status=manifest_status,
            visibility_cutoff=completed_report.metadata.visibility_cutoff,
            visibility_cutoff_source=(completed_report.metadata.visibility_cutoff_source),
            legacy_output_mode=legacy_mode,
            input_artifacts=(
                artifact.path for artifact in completed_report.metadata.pre_run_input_artifacts
            ),
            canonical_output_artifacts=(
                *collect_run_files(run_paths),
                *(
                    ()
                    if completed_finalization is None
                    else completed_finalization.canonical_outputs
                ),
            ),
            legacy_output_artifacts=(
                *(artifact.path for artifact in completed_report.metadata.produced_artifacts),
                *(() if completed_finalization is None else completed_finalization.legacy_outputs),
            ),
            command=manifest_command,
            resolved_config={
                "scheduled_tasks": DEFAULT_SCHEDULED_TASKS_CONFIG_PATH,
                "periodic_operations_control": (
                    PROJECT_ROOT / "config" / "operations" / "periodic_control.yaml"
                ),
            },
            schema_versions={
                "scheduled_tasks": "1",
                "periodic_operations_plan": "periodic_operations_plan.v1",
                "daily_ops_canonical_finalization": "1",
            },
            elapsed_seconds=(
                completed_report.finished_at - completed_report.started_at
            ).total_seconds(),
            warnings=warnings,
        )

    def _write_completed_bundle_manifest(
        completed_report: DailyOpsRunReport,
        *,
        manifest_status: str,
        completed_finalization: DailyOpsCanonicalFinalizationResult | None,
        extra_warning: str | None = None,
    ) -> None:
        payload = _build_completed_bundle_manifest(
            completed_report,
            manifest_status=manifest_status,
            completed_finalization=completed_finalization,
            extra_warning=extra_warning,
        )
        write_json_atomic(
            run_paths.manifest_path,
            payload,
            sort_keys=False,
            trailing_newline=False,
        )

    def _completion_callback(completed_report: DailyOpsRunReport) -> None:
        nonlocal finalization_result
        try:
            completed_finalization = _finalize_daily_ops_canonical_outputs(
                run_report=completed_report,
                run_paths=run_paths,
                plan_date=plan_date,
                run_report_path=run_report_path,
                metadata_path=metadata_path,
                legacy_reports_dir=reports_dir,
                legacy_mode=legacy_mode,
                project_root=PROJECT_ROOT,
            )
            _write_completed_bundle_manifest(
                completed_report,
                manifest_status="FINALIZING",
                completed_finalization=completed_finalization,
            )
            _validate_daily_ops_finalization_closure(
                run_report=completed_report,
                finalization=completed_finalization,
                run_paths=run_paths,
                metadata_path=metadata_path,
                legacy_reports_dir=reports_dir,
                legacy_mode=legacy_mode,
                expected_manifest_status="FINALIZING",
            )
            final_manifest_payload = _build_completed_bundle_manifest(
                completed_report,
                manifest_status=completed_report.status,
                completed_finalization=completed_finalization,
            )
            _validate_daily_ops_finalization_closure(
                run_report=completed_report,
                finalization=completed_finalization,
                run_paths=run_paths,
                metadata_path=metadata_path,
                legacy_reports_dir=reports_dir,
                legacy_mode=legacy_mode,
                expected_manifest_status=completed_report.status,
                manifest_payload=final_manifest_payload,
            )
            write_json_atomic(
                run_paths.manifest_path,
                final_manifest_payload,
                sort_keys=False,
                trailing_newline=False,
            )
            finalization_result = completed_finalization
        except Exception as raw_exc:
            typed_error = (
                raw_exc
                if isinstance(raw_exc, DailyOpsCanonicalFinalizationError)
                else DailyOpsCanonicalFinalizationError(
                    "DAILY_FINALIZATION_CLOSURE_FAILED",
                    f"{type(raw_exc).__name__}: {raw_exc}",
                    evidence_path=_default_daily_ops_finalization_evidence_path(
                        run_paths.reports_dir,
                        plan_date,
                    ),
                )
            )
            evidence_path = (
                typed_error.evidence_path
                or _default_daily_ops_finalization_evidence_path(
                    run_paths.reports_dir,
                    plan_date,
                )
            )
            evidence_write_error: Exception | None = None
            try:
                typed_error.evidence_path = _downgrade_daily_ops_finalization_evidence(
                    evidence_path=evidence_path,
                    run_report=completed_report,
                    blocker_code=typed_error.code,
                    blocker_message=typed_error.message,
                )
            except Exception as exc:
                evidence_write_error = exc
            manifest_write_error: Exception | None = None
            try:
                _write_completed_bundle_manifest(
                    completed_report,
                    manifest_status="FAILED",
                    completed_finalization=None,
                    extra_warning=f"canonical_finalization_failed:{typed_error.code}",
                )
            except Exception as exc:
                manifest_write_error = exc
            if evidence_write_error is not None or manifest_write_error is not None:
                raise DailyOpsCanonicalFinalizationError(
                    "DAILY_FINALIZATION_FAILURE_RECORDING_FAILED",
                    (
                        f"primary_blocker={typed_error.code}; "
                        f"evidence_error={evidence_write_error!r}; "
                        f"manifest_error={manifest_write_error!r}"
                    ),
                    evidence_path=typed_error.evidence_path,
                ) from (manifest_write_error or evidence_write_error)
            if typed_error is raw_exc:
                raise
            raise typed_error from raw_exc

    def _terminal_failure_callback(
        completed_report: DailyOpsRunReport,
        terminal_error: Exception,
    ) -> None:
        evidence_path = _default_daily_ops_finalization_evidence_path(
            run_paths.reports_dir,
            plan_date,
        )
        typed_error = DailyOpsCanonicalFinalizationError(
            "DAILY_FINALIZATION_TERMINAL_STATE_WRITE_FAILED",
            f"{type(terminal_error).__name__}: {terminal_error}",
            evidence_path=evidence_path,
        )
        recording_errors: list[str] = []
        try:
            _downgrade_daily_ops_finalization_evidence(
                evidence_path=evidence_path,
                run_report=completed_report,
                blocker_code=typed_error.code,
                blocker_message=typed_error.message,
            )
        except Exception as exc:
            recording_errors.append(f"evidence={exc!r}")
        try:
            _write_completed_bundle_manifest(
                completed_report,
                manifest_status="FAILED",
                completed_finalization=None,
                extra_warning=f"canonical_terminal_failed:{typed_error.code}",
            )
        except Exception as exc:
            recording_errors.append(f"manifest={exc!r}")
        if recording_errors:
            raise DailyOpsCanonicalFinalizationError(
                "DAILY_FINALIZATION_TERMINAL_FAILURE_RECORDING_FAILED",
                (f"primary_blocker={typed_error.code}; " + "; ".join(recording_errors)),
                evidence_path=typed_error.evidence_path,
            ) from terminal_error
        raise typed_error from terminal_error

    try:
        run_report = _run_daily_ops_with_completion_callback(
            plan,
            completion_callback=_completion_callback,
            terminal_failure_callback=_terminal_failure_callback,
            project_root=PROJECT_ROOT,
            env=os.environ,
            run_id=resolved_run_id,
            diagnostics_dir=run_paths.reports_dir / "diagnostics",
        )
    except DailyOpsCanonicalFinalizationError as exc:
        console.print(f"[red]Canonical finalization：FAILED ({exc.code})[/red]")
        console.print(f"Run ID：{resolved_run_id}")
        console.print(f"Run bundle：{run_paths.run_root}")
        if exc.evidence_path is not None:
            console.print(f"Finalization evidence：{exc.evidence_path}")
        raise typer.Exit(code=1) from exc

    if run_report.status == "FAIL_FINALIZATION":
        write_daily_ops_run_report(run_report, run_report_path, metadata_path=metadata_path)
        periodic_plan_path = _write_periodic_plan_from_daily_run(
            run_report=run_report,
            output_root=run_paths.metadata_dir,
            project_root=PROJECT_ROOT,
        )
        try:
            _write_completed_bundle_manifest(
                run_report,
                manifest_status="FAILED",
                completed_finalization=None,
                extra_warning="canonical_finalization_failed:FAIL_FINALIZATION",
            )
        except Exception as manifest_exc:
            console.print(
                "[red]FAILED bundle manifest write failed："
                f"{type(manifest_exc).__name__}: {manifest_exc}[/red]"
            )
        console.print("[red]Canonical finalization：FAIL_FINALIZATION[/red]")
        console.print(f"Run ID：{resolved_run_id}")
        console.print(f"Run bundle：{run_paths.run_root}")
        console.print(f"执行报告：{run_report_path}")
        console.print(f"周期任务评估：{periodic_plan_path}")
        finalization_evidence_path = _default_daily_ops_finalization_evidence_path(
            run_paths.reports_dir,
            plan_date,
        )
        if finalization_evidence_path.exists():
            console.print(f"Finalization evidence：{finalization_evidence_path}")
        raise typer.Exit(code=1)

    if not run_report.status.startswith("RUN_CONTROL_") and finalization_result is None:
        try:
            _completion_callback(run_report)
        except DailyOpsCanonicalFinalizationError as exc:
            console.print(f"[red]Canonical finalization：FAILED ({exc.code})[/red]")
            console.print(f"Run ID：{resolved_run_id}")
            console.print(f"Run bundle：{run_paths.run_root}")
            if exc.evidence_path is not None:
                console.print(f"Finalization evidence：{exc.evidence_path}")
            raise typer.Exit(code=1) from exc

    if run_report.status.startswith("RUN_CONTROL_"):
        write_daily_ops_run_report(run_report, run_report_path, metadata_path=metadata_path)
        periodic_plan_path = _write_periodic_plan_from_daily_run(
            run_report=run_report,
            output_root=run_paths.metadata_dir,
            project_root=PROJECT_ROOT,
        )
        console.print(f"Canonical run control：{run_report.status}")
        console.print(f"Run ID：{resolved_run_id}")
        console.print(f"执行报告：{run_report_path}")
        console.print(f"周期任务评估：{periodic_plan_path}")
        if run_report.status == "RUN_CONTROL_ALREADY_COMPLETE":
            return
        raise typer.Exit(code=1)
    if finalization_result is None:
        raise typer.Exit(code=1)

    status = run_report.status
    style = "green" if status == "PASS" else "yellow" if status == "PASS_WITH_SKIPS" else "red"
    console.print(f"[{style}]每日运行执行：{status}[/{style}]")
    console.print(f"Run ID：{resolved_run_id}")
    console.print(f"Run bundle：{run_paths.run_root}")
    console.print(f"计划报告：{plan_report_path}")
    console.print(f"执行报告：{run_report_path}")
    console.print(f"周期任务评估：{finalization_result.periodic_plan_path}")
    console.print(f"每日任务 Dashboard：{finalization_result.daily_task_dashboard_path}")
    console.print(f"每日任务 Dashboard JSON：{finalization_result.daily_task_dashboard_json_path}")
    console.print(f"每日决策总线 JSON：{finalization_result.daily_decision_summary_path}")
    console.print(
        f"Order intent candidates JSON：{finalization_result.order_intent_candidates_path}"
    )
    if finalization_result.reader_brief_final_paths:
        console.print(f"Reader Brief final：{finalization_result.reader_brief_final_paths[0]}")
        console.print(
            "Final quality："
            f"report={finalization_result.report_quality_status}；"
            f"reader={finalization_result.reader_quality_status}"
        )
    console.print(
        f"Canonical finalization：{finalization_result.status}；"
        f"evidence={finalization_result.finalization_evidence_path}"
    )
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


@ops_app.command("periodic-dispatch")
def periodic_dispatch_command(
    as_of: Annotated[
        str,
        typer.Option(help="调度判断日期，格式为 YYYY-MM-DD。"),
    ],
    task_id: Annotated[
        list[str] | None,
        typer.Option("--task-id", help="显式选择任务；可重复传入。"),
    ] = None,
    daily_status: Annotated[
        str | None,
        typer.Option(help="latest daily canonical status；必须显式提供。"),
    ] = None,
    data_quality_status: Annotated[
        str | None,
        typer.Option(help="cached data canonical quality status；必须显式提供。"),
    ] = None,
    data_quality_evidence_id: Annotated[
        str | None,
        typer.Option(help="数据质量证据 ID。"),
    ] = None,
    source_artifact_id: Annotated[
        list[str] | None,
        typer.Option("--source-artifact-id", help="required source artifact ID；可重复传入。"),
    ] = None,
    owner_decision_id: Annotated[
        str | None,
        typer.Option(help="人工 owner 决策证据 ID。"),
    ] = None,
    explicit_trigger: Annotated[
        bool,
        typer.Option("--explicit-trigger", help="仅用于显式 ad-hoc event。"),
    ] = False,
    confirm_manual_dispatch: Annotated[
        bool,
        typer.Option(
            "--confirm-manual-dispatch",
            help="确认这是人工受控 dispatch，不是新增 scheduler entry。",
        ),
    ] = False,
    output_path: Annotated[
        Path | None,
        typer.Option(help="periodic plan JSON 输出路径。"),
    ] = None,
) -> None:
    """用显式 evidence/owner gate 人工执行已登记的 non-daily task。"""
    if not confirm_manual_dispatch:
        raise typer.BadParameter("必须显式传入 --confirm-manual-dispatch。")
    selected = tuple(item.strip() for item in (task_id or []) if item.strip())
    if not selected:
        raise typer.BadParameter("至少需要一个 --task-id。")
    try:
        resolved_daily_status = CanonicalStatus(str(daily_status or ""))
        resolved_quality_status = CanonicalStatus(str(data_quality_status or ""))
    except ValueError as exc:
        raise typer.BadParameter(
            "--daily-status 与 --data-quality-status 必须是 canonical status。"
        ) from exc
    if not data_quality_evidence_id or not owner_decision_id:
        raise typer.BadParameter("必须提供 data-quality evidence 与 owner decision ID。")
    source_ids = tuple(item.strip() for item in (source_artifact_id or []) if item.strip())
    if not source_ids:
        raise typer.BadParameter("至少需要一个 --source-artifact-id。")

    dispatch_date = _parse_date(as_of)
    generated_at = datetime.now(tz=UTC)
    contexts = build_periodic_due_contexts_from_daily(
        as_of=dispatch_date,
        daily_status=resolved_daily_status,
        data_quality_status=resolved_quality_status,
        data_quality_evidence_id=data_quality_evidence_id,
        required_artifacts_ready=True,
        source_artifact_ids=source_ids,
        owner_gate_approved=True,
        owner_decision_id=owner_decision_id,
        explicit_trigger=explicit_trigger,
    )
    periodic_policy = load_periodic_operations_control_policy()
    plan = build_periodic_operations_plan(
        as_of=dispatch_date,
        generated_at=generated_at,
        contexts=contexts,
        policy=periodic_policy,
    )
    plan_path = output_path or default_periodic_operations_plan_path(
        PROJECT_ROOT / "outputs" / "run_control" / "periodic" / "plans",
        dispatch_date,
    )
    write_periodic_operations_plan(plan, plan_path)
    runtime_policy = load_operations_runtime_control_policy()
    control = OperationsRunControl(
        root=PROJECT_ROOT / "outputs" / "run_control" / "periodic",
        policy=runtime_policy,
    )
    results = dispatch_periodic_operations_plan(
        plan,
        selected_task_ids=selected,
        control=control,
        policy=periodic_policy,
        runner=_run_periodic_command,
        project_root=PROJECT_ROOT,
        manual_invocation=True,
    )
    console.print(f"周期任务计划：{plan_path}")
    for result in results:
        console.print(
            f"{result.task_id}：{result.status.value}"
            + (f"；blockers={','.join(result.blocker_codes)}" if result.blocker_codes else "")
        )
    if any(result.status is not CanonicalStatus.PASS for result in results):
        raise typer.Exit(code=1)


def _run_periodic_command(command: tuple[str, ...], *, cwd: Path) -> object:
    execution_command = _execution_command(command, cwd)
    return subprocess.run(
        execution_command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )


_DAILY_DEFAULT_DATA_QUALITY_INPUT_ROLES = (
    "prices",
    "rates",
    "secondary_prices",
)
_DAILY_VALIDATE_DATA_COMMAND_PREFIX = ("aits", "validate-data", "--as-of")


class _DataQualityExecutionDiscoveryLoader(Protocol):
    def __call__(
        self,
        as_of: date,
        *,
        project_root: Path = PROJECT_ROOT,
    ) -> DiscoveredDataQualityExecution: ...


class _StaticClock:
    def __init__(self, value: datetime) -> None:
        self._value = value

    def now(self) -> datetime:
        return self._value


class _RejectedDataQualityReceiptVerifier:
    def __init__(self, error: DataQualityExecutionError) -> None:
        self._error = error

    def __call__(
        self,
        receipt_path: Path,
        *,
        expected_as_of: date,
        expected_policy_path: Path,
        expected_input_roles: tuple[str, ...],
        project_root: Path = PROJECT_ROOT,
    ) -> VerifiedDataQualityPreflight:
        del (
            receipt_path,
            expected_as_of,
            expected_policy_path,
            expected_input_roles,
            project_root,
        )
        raise self._error


class _PointerBoundDataQualityReceiptVerifier:
    def __init__(
        self,
        *,
        discovered: DiscoveredDataQualityExecution,
        validate_started_at: datetime,
        validate_ended_at: datetime,
        delegate: DataQualityReceiptVerifier,
    ) -> None:
        self._discovered = discovered
        self._validate_started_at = validate_started_at
        self._validate_ended_at = validate_ended_at
        self._delegate = delegate

    def __call__(
        self,
        receipt_path: Path,
        *,
        expected_as_of: date,
        expected_policy_path: Path,
        expected_input_roles: tuple[str, ...],
        project_root: Path = PROJECT_ROOT,
    ) -> VerifiedDataQualityPreflight:
        preflight = self._delegate(
            receipt_path,
            expected_as_of=expected_as_of,
            expected_policy_path=expected_policy_path,
            expected_input_roles=expected_input_roles,
            project_root=project_root,
        ).assert_strict_passed()
        _validate_discovered_data_quality_preflight(
            discovered=self._discovered,
            preflight=preflight,
            receipt_path=receipt_path,
            expected_as_of=expected_as_of,
            validate_started_at=self._validate_started_at,
            validate_ended_at=self._validate_ended_at,
            project_root=project_root,
        )
        return preflight


def _write_periodic_plan_from_daily_run(
    *,
    run_report: DailyOpsRunReport,
    output_root: Path,
    project_root: Path = PROJECT_ROOT,
    discovery_loader: _DataQualityExecutionDiscoveryLoader = (
        load_default_data_quality_execution_discovery
    ),
    receipt_verifier: DataQualityReceiptVerifier = verify_data_quality_execution_receipt,
    generated_at: datetime | None = None,
) -> Path:
    resolved_generated_at = generated_at or datetime.now(tz=UTC)
    if resolved_generated_at.tzinfo is None or resolved_generated_at.utcoffset() is None:
        raise ValueError("generated_at must be timezone-aware")

    if run_report.status == "RUN_CONTROL_ALREADY_COMPLETE":
        daily_status = CanonicalStatus.PASS
    elif run_report.status in {"PASS", "PASS_WITH_SKIPS"}:
        daily_status = CanonicalStatus.PASS
    elif run_report.status.startswith("BLOCKED") or run_report.status.startswith(
        "RUN_CONTROL_BLOCKED"
    ):
        daily_status = CanonicalStatus.BLOCKED
    else:
        daily_status = CanonicalStatus.FAILED

    data_quality_as_of, planned_command, command_error = _planned_daily_data_quality_as_of(
        run_report
    )
    receipt_path = default_data_quality_execution_discovery_path(
        data_quality_as_of,
        project_root=project_root,
    )
    native_verifier: DataQualityReceiptVerifier
    if command_error is not None:
        native_verifier = _RejectedDataQualityReceiptVerifier(command_error)
    else:
        try:
            validate_started_at, validate_ended_at = _current_validate_data_interval(
                run_report,
                expected_command=planned_command,
            )
            discovered = discovery_loader(data_quality_as_of, project_root=project_root)
            receipt_path = discovered.receipt_path
            native_verifier = _PointerBoundDataQualityReceiptVerifier(
                discovered=discovered,
                validate_started_at=validate_started_at,
                validate_ended_at=validate_ended_at,
                delegate=receipt_verifier,
            )
        except (DataQualityExecutionContractError, OSError) as exc:
            native_verifier = _RejectedDataQualityReceiptVerifier(
                _as_data_quality_execution_error(exc)
            )

    native_context = NativeConsumerExpectedContext(
        as_of=run_report.plan.as_of,
        data_quality_as_of=data_quality_as_of,
        expected_policy_path=project_root / "config" / "data_quality.yaml",
        expected_input_roles=_DAILY_DEFAULT_DATA_QUALITY_INPUT_ROLES,
        daily_status=daily_status,
        required_artifacts_ready=daily_status is CanonicalStatus.PASS,
        source_artifact_ids=(),
        owner_gate_approved=None,
        owner_decision_id=None,
    )
    native_plan = build_native_periodic_consumer_parity_plan(
        receipt_path,
        expected_context=native_context,
        scheduled=load_scheduled_tasks_config(),
        verifier=native_verifier,
        clock=_StaticClock(resolved_generated_at),
        project_root=project_root,
    )
    write_native_periodic_consumer_parity_plan(
        native_plan,
        default_native_periodic_consumer_parity_plan_path(
            run_report.plan.as_of,
            output_root,
        ),
    )

    data_quality_status = CanonicalStatus.FAILED
    data_quality_evidence_id: str | None = None
    source_artifact_ids: tuple[str, ...] = ()
    if native_plan.status is CanonicalStatus.PASS:
        native_resolution = native_plan.entry("daily_score_daily").due_resolution
        if (
            native_resolution.data_quality_evidence_id is not None
            and native_resolution.source_artifact_ids
        ):
            data_quality_status = CanonicalStatus.PASS
            data_quality_evidence_id = native_resolution.data_quality_evidence_id
            source_artifact_ids = native_resolution.source_artifact_ids

    contexts = build_periodic_due_contexts_from_daily(
        as_of=run_report.plan.as_of,
        daily_status=daily_status,
        data_quality_status=data_quality_status,
        data_quality_evidence_id=data_quality_evidence_id,
        required_artifacts_ready=(
            daily_status is CanonicalStatus.PASS and data_quality_status is CanonicalStatus.PASS
        ),
        source_artifact_ids=source_artifact_ids,
    )
    plan = build_periodic_operations_plan(
        as_of=run_report.plan.as_of,
        generated_at=resolved_generated_at,
        contexts=contexts,
    )
    return write_periodic_operations_plan(
        plan,
        default_periodic_operations_plan_path(output_root, run_report.plan.as_of),
    )


def _planned_daily_data_quality_as_of(
    run_report: DailyOpsRunReport,
) -> tuple[date, tuple[str, ...], DataQualityExecutionError | None]:
    fallback_as_of = (
        run_report.plan.as_of
        if run_report.plan.market_session.is_trading_day
        else run_report.plan.market_session.previous_trading_day
    )
    step = next(
        (item for item in run_report.plan.steps if item.step_id == "validate_data"),
        None,
    )
    if step is None:
        return (
            fallback_as_of,
            (),
            DataQualityExecutionError(
                "DQ_RECEIPT_FIELDS_INVALID",
                "daily plan is missing the validate_data step",
            ),
        )
    try:
        parsed_as_of = _parse_daily_validate_data_as_of(step.command)
    except DataQualityExecutionError as exc:
        return fallback_as_of, step.command, exc
    if parsed_as_of != fallback_as_of:
        return (
            fallback_as_of,
            step.command,
            DataQualityExecutionError(
                "DQ_AS_OF_MISMATCH",
                (f"expected={fallback_as_of.isoformat()} " f"actual={parsed_as_of.isoformat()}"),
            ),
        )
    return parsed_as_of, step.command, None


def _parse_daily_validate_data_as_of(command: tuple[str, ...]) -> date:
    if (
        len(command) != 6
        or command[:3] != _DAILY_VALIDATE_DATA_COMMAND_PREFIX
        or command[4:]
        != (
            "--execution-profile",
            DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        )
    ):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_FIELDS_INVALID",
            (
                "validate_data command must be exactly `aits validate-data --as-of "
                "YYYY-MM-DD --execution-profile daily_default.v1`"
            ),
        )
    try:
        parsed = date.fromisoformat(command[3])
    except ValueError as exc:
        raise DataQualityExecutionError(
            "DQ_AS_OF_MISMATCH",
            f"invalid validate_data --as-of={command[3]!r}",
        ) from exc
    if parsed.isoformat() != command[3]:
        raise DataQualityExecutionError(
            "DQ_AS_OF_MISMATCH",
            f"validate_data --as-of must be canonical ISO date: {command[3]!r}",
        )
    return parsed


def _current_validate_data_interval(
    run_report: DailyOpsRunReport,
    *,
    expected_command: tuple[str, ...],
) -> tuple[datetime, datetime]:
    if run_report.status == "RUN_CONTROL_ALREADY_COMPLETE":
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID",
            "already-complete run control has no current validate_data execution interval",
        )
    result = next(
        (item for item in run_report.step_results if item.step_id == "validate_data"),
        None,
    )
    if result is None:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_MISSING",
            "current daily run has no validate_data result",
        )
    observed_as_of = _parse_daily_validate_data_as_of(result.command)
    expected_as_of = _parse_daily_validate_data_as_of(expected_command)
    if observed_as_of != expected_as_of or result.command != expected_command:
        raise DataQualityExecutionError(
            "DQ_AS_OF_MISMATCH",
            "validate_data result command differs from the current daily plan",
        )
    if result.status != "PASS":
        raise DataQualityExecutionError(
            "DQ_EXECUTION_FAILED",
            f"validate_data status={result.status}",
        )
    if result.started_at is None or result.ended_at is None:
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID",
            "validate_data PASS is missing its current execution interval",
        )
    _require_aware_datetime(run_report.started_at, "daily_run.started_at")
    _require_aware_datetime(run_report.finished_at, "daily_run.finished_at")
    _require_aware_datetime(result.started_at, "validate_data.started_at")
    _require_aware_datetime(result.ended_at, "validate_data.ended_at")
    if not (
        run_report.started_at <= result.started_at <= result.ended_at <= run_report.finished_at
    ):
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID",
            "validate_data interval is outside the current daily run interval",
        )
    return result.started_at, result.ended_at


def _require_aware_datetime(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID",
            f"{field} must be timezone-aware",
        )


def _validate_discovered_data_quality_preflight(
    *,
    discovered: DiscoveredDataQualityExecution,
    preflight: VerifiedDataQualityPreflight,
    receipt_path: Path,
    expected_as_of: date,
    validate_started_at: datetime,
    validate_ended_at: datetime,
    project_root: Path,
) -> None:
    pointer = discovered.pointer
    root = project_root.resolve()
    expected_pointer_path = default_data_quality_execution_discovery_path(
        expected_as_of,
        project_root=root,
    )
    if discovered.pointer_path.resolve() != expected_pointer_path.resolve():
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH",
            "discovery pointer path differs from the daily_default path",
        )
    if (
        pointer.profile_id != DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID
        or pointer.as_of != expected_as_of
        or discovered.receipt.as_of != expected_as_of
        or preflight.as_of != expected_as_of
    ):
        raise DataQualityExecutionError(
            "DQ_AS_OF_MISMATCH",
            "discovery, receipt, and preflight as_of values must match validate_data",
        )
    expected_receipt_path = (root / Path(pointer.receipt_path)).resolve()
    try:
        expected_receipt_path.relative_to(root)
    except ValueError as exc:
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH",
            "discovery receipt path escapes project_root",
        ) from exc
    if (
        receipt_path.resolve() != expected_receipt_path
        or discovered.receipt_path.resolve() != expected_receipt_path
        or preflight.receipt_path != pointer.receipt_path
    ):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH",
            "discovery and verified receipt paths differ",
        )
    if (
        preflight.receipt_id != pointer.receipt_id
        or discovered.receipt.receipt_id != pointer.receipt_id
        or preflight.receipt_sha256 != pointer.receipt_sha256
        or preflight.receipt_size_bytes != pointer.receipt_size_bytes
    ):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH",
            "discovery pointer identity differs from the verified preflight",
        )
    if (
        discovered.receipt.started_at != preflight.receipt.started_at
        or discovered.receipt.ended_at != preflight.receipt.ended_at
    ):
        raise DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH",
            "discovered receipt lifecycle differs from the verified preflight",
        )
    _require_aware_datetime(pointer.published_at, "discovery.published_at")
    _require_aware_datetime(preflight.receipt.started_at, "receipt.started_at")
    _require_aware_datetime(preflight.receipt.ended_at, "receipt.ended_at")
    if not (
        validate_started_at
        <= preflight.receipt.started_at
        <= preflight.receipt.ended_at
        <= pointer.published_at
        <= validate_ended_at
    ):
        raise DataQualityExecutionError(
            "DQ_EXECUTION_CHRONOLOGY_INVALID",
            "receipt lifecycle and discovery publication must be inside validate_data",
        )


def _as_data_quality_execution_error(
    exc: DataQualityExecutionContractError | OSError,
) -> DataQualityExecutionError:
    if isinstance(exc, DataQualityExecutionContractError):
        return DataQualityExecutionError(exc.code, exc.message)
    return DataQualityExecutionError("DQ_RECEIPT_MISSING", str(exc))


def _daily_run_manifest_command(
    *,
    plan_date: date,
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
    plan_output_path: Path | None,
    output_path: Path | None,
    run_output_root: Path,
    resolved_run_id: str,
    legacy_mode: str,
) -> tuple[str, ...]:
    command = [
        "aits",
        "ops",
        "daily-run",
        "--as-of",
        plan_date.isoformat(),
        "--download-start",
        download_start,
        "--run-output-root",
        str(run_output_root),
        "--run-id",
        resolved_run_id,
        "--legacy-output-mode",
        legacy_mode,
        "--llm-request-profile",
        llm_request_profile,
        "--include-download-data" if include_download_data else "--skip-download-data",
        "--include-pit-snapshots" if include_pit_snapshots else "--skip-pit-snapshots",
        ("--include-sec-fundamentals" if include_sec_fundamentals else "--skip-sec-fundamentals"),
        (
            "--include-valuation-snapshots"
            if include_valuation_snapshots
            else "--skip-valuation-snapshots"
        ),
        "--include-secret-scan" if include_secret_scan else "--skip-secret-scan",
        (
            "--risk-event-openai-precheck"
            if risk_event_openai_precheck
            else "--skip-risk-event-openai-precheck"
        ),
    ]
    if risk_event_openai_precheck_max_candidates is not None:
        command.extend(
            [
                "--risk-event-openai-precheck-max-candidates",
                str(risk_event_openai_precheck_max_candidates),
            ]
        )
    if full_universe:
        command.append("--full-universe")
    if plan_output_path is not None:
        command.extend(["--plan-output-path", str(plan_output_path)])
    if output_path is not None:
        command.extend(["--output-path", str(output_path)])
    return tuple(command)


def _daily_run_manifest_warnings(report: DailyOpsRunReport) -> tuple[str, ...]:
    warnings: list[str] = []
    if report.missing_env_vars:
        warnings.append("missing_env_vars:" + ",".join(report.missing_env_vars))
    warnings.extend("input_visibility:" + issue.code for issue in report.visibility_issues)
    warnings.extend(
        f"step:{result.step_id}:{result.status}"
        for result in report.step_results
        if result.status not in {"PASS", "SKIP"}
    )
    return tuple(warnings)


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
