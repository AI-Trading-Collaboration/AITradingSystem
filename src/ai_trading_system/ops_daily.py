from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.alerts import (
    default_alert_report_path,
    default_pipeline_health_alert_report_path,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core import (
    ArtifactRef,
    ProductionEffect,
    StepStatus,
    WorkflowStep,
    WorkflowStepResult,
)
from ai_trading_system.data.download import default_download_failure_report_path
from ai_trading_system.data.quality import default_quality_report_path
from ai_trading_system.documentation_contract import (
    default_documentation_contract_json_path,
    default_documentation_contract_report_path,
)
from ai_trading_system.evidence_dashboard import (
    default_evidence_dashboard_json_path,
    default_evidence_dashboard_path,
)
from ai_trading_system.external_request_cache import sanitize_diagnostic_text
from ai_trading_system.features.market import default_feature_report_path
from ai_trading_system.fmp_forward_pit import (
    default_fmp_forward_pit_fetch_report_path,
    default_fmp_forward_pit_normalized_path,
)
from ai_trading_system.fundamentals.sec_metrics import (
    default_sec_fundamental_metrics_csv_path,
    default_sec_fundamental_metrics_report_path,
    default_sec_fundamental_metrics_validation_report_path,
)
from ai_trading_system.fundamentals.sec_validation import (
    default_sec_companyfacts_validation_report_path,
)
from ai_trading_system.official_policy_sources import (
    default_official_policy_candidates_path,
    default_official_policy_fetch_report_path,
)
from ai_trading_system.pipeline_health import default_pipeline_health_report_path
from ai_trading_system.pit_snapshots import default_pit_snapshot_validation_report_path
from ai_trading_system.reports.market_panel import (
    default_market_panel_json_path,
    default_market_panel_report_path,
)
from ai_trading_system.reports.reader_brief import (
    default_reader_brief_html_path,
    default_reader_brief_json_path,
    default_reader_brief_quality_json_path,
    default_reader_brief_quality_markdown_path,
)
from ai_trading_system.reports.report_index import (
    default_report_index_html_path,
    default_report_index_json_path,
)
from ai_trading_system.reports.research_governance_summary import (
    default_research_governance_summary_json_path,
    default_research_governance_summary_report_path,
)
from ai_trading_system.reports.score_change_attribution import (
    default_score_change_attribution_json_path,
    default_score_change_attribution_report_path,
)
from ai_trading_system.scheduled_tasks import (
    DAILY_CADENCE_ID,
    load_scheduled_tasks_config,
)
from ai_trading_system.scoring.daily import default_daily_score_report_path
from ai_trading_system.secret_hygiene import default_secret_scan_report_path
from ai_trading_system.trading_calendar import (
    MarketSession,
    current_us_equity_market_date,
    latest_completed_us_equity_trading_day,
    us_equity_market_session,
)
from ai_trading_system.trading_engine.market_data_freshness import (
    default_market_data_freshness_json_path,
    default_market_data_freshness_markdown_path,
)
from ai_trading_system.trading_engine.market_data_refresh import (
    default_market_data_refresh_json_path,
    default_market_data_refresh_markdown_path,
    default_market_data_refresh_plan_path,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    default_active_shadow_candidates_path,
    default_portfolio_candidate_tracking_json_path,
    default_portfolio_candidate_tracking_markdown_path,
)
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    default_portfolio_tracking_review_json_path,
    default_portfolio_tracking_review_markdown_path,
    portfolio_tracking_review_report_alias_paths,
)
from ai_trading_system.valuation import default_valuation_validation_report_path
from ai_trading_system.valuation_sources import (
    default_fmp_analyst_estimate_history_dir,
    default_fmp_valuation_fetch_report_path,
)


@dataclass(frozen=True)
class DailyOpsStep:
    step_id: str
    title: str
    command: tuple[str, ...]
    required_env_vars: tuple[str, ...]
    produced_paths: tuple[Path, ...]
    quality_gate: str
    blocks_downstream: bool
    enabled: bool = True
    skip_reason: str | None = None
    input_visibility: str = "local_or_readonly"

    def missing_env_vars(self, env: Mapping[str, str]) -> tuple[str, ...]:
        if not self.enabled:
            return ()
        return tuple(
            env_var for env_var in self.required_env_vars if not env.get(env_var, "").strip()
        )


@dataclass(frozen=True)
class DailyOpsPlan:
    as_of: date
    generated_at: datetime
    steps: tuple[DailyOpsStep, ...]
    market_session: MarketSession
    production_effect: str = ProductionEffect.NONE.value

    def missing_env_by_step(
        self,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, tuple[str, ...]]:
        checked_env = os.environ if env is None else env
        return {
            step.step_id: missing
            for step in self.steps
            if (missing := step.missing_env_vars(checked_env))
        }

    def missing_env_vars(self, env: Mapping[str, str] | None = None) -> tuple[str, ...]:
        missing = {
            env_var for values in self.missing_env_by_step(env).values() for env_var in values
        }
        return tuple(sorted(missing))

    def status(self, env: Mapping[str, str] | None = None) -> str:
        if self.missing_env_vars(env):
            return "BLOCKED_ENV"
        if any(not step.enabled for step in self.steps):
            return "READY_WITH_SKIPS"
        return "READY"


@dataclass(frozen=True)
class DailyOpsStepResult:
    step_id: str
    title: str
    command: tuple[str, ...]
    status: str
    return_code: int | None
    started_at: datetime | None
    ended_at: datetime | None
    duration_seconds: float | None
    produced_paths: tuple[Path, ...]
    blocks_downstream: bool
    skip_reason: str | None = None
    stdout_line_count: int = 0
    stderr_line_count: int = 0
    error: str | None = None
    diagnostic_path: Path | None = None


@dataclass(frozen=True)
class DailyOpsInputVisibilityIssue:
    code: str
    severity: str
    message: str
    step_id: str | None = None


@dataclass(frozen=True)
class DailyOpsArtifactDigest:
    path: Path
    exists: bool
    artifact_type: str
    sha256: str | None
    size_bytes: int | None
    file_count: int | None


@dataclass(frozen=True)
class DailyOpsRunMetadata:
    schema_version: int
    run_id: str
    as_of: date
    generated_at: datetime
    project_root: Path
    status: str
    started_at: datetime
    finished_at: datetime
    visibility_cutoff: datetime
    visibility_cutoff_source: str
    input_visibility_status: str
    input_visibility_issues: tuple[Mapping[str, object], ...]
    git: Mapping[str, object]
    config_artifacts: tuple[DailyOpsArtifactDigest, ...]
    rule_card_sha256: str | None
    env_presence: Mapping[str, bool]
    commands: tuple[Mapping[str, object], ...]
    step_results: tuple[Mapping[str, object], ...]
    pre_run_input_artifacts: tuple[DailyOpsArtifactDigest, ...]
    produced_artifacts: tuple[DailyOpsArtifactDigest, ...]


@dataclass(frozen=True)
class DailyOpsRunReport:
    plan: DailyOpsPlan
    started_at: datetime
    finished_at: datetime
    status: str
    step_results: tuple[DailyOpsStepResult, ...]
    missing_env_vars: tuple[str, ...] = ()
    visibility_issues: tuple[DailyOpsInputVisibilityIssue, ...] = ()
    production_effect: str = "writes local data/cache/reports and calls provider APIs"
    metadata: DailyOpsRunMetadata | None = None

    @property
    def failed_step(self) -> DailyOpsStepResult | None:
        return next(
            (result for result in self.step_results if result.status == "FAIL"),
            None,
        )


DailyOpsCommandRunner = subprocess.run
_DIAGNOSTIC_TEXT_MAX_CHARS = 60_000
_DIAGNOSTIC_ENV_SECRET_TOKENS = (
    "KEY",
    "TOKEN",
    "SECRET",
    "PASSWORD",
    "CREDENTIAL",
    "AUTH",
)


def daily_ops_step_to_workflow_step(step: DailyOpsStep) -> WorkflowStep:
    return WorkflowStep(
        step_id=step.step_id,
        name=step.title,
        command_name=_workflow_command_name(step.command),
        command=step.command,
        production_effect=ProductionEffect.NONE,
        expected_outputs=tuple(ArtifactRef.from_path(path) for path in step.produced_paths),
        blocking=step.blocks_downstream,
    )


def daily_ops_step_result_to_workflow_step_result(
    result: DailyOpsStepResult,
) -> WorkflowStepResult:
    return WorkflowStepResult(
        step_id=result.step_id,
        status=_workflow_status(result.status),
        started_at=result.started_at,
        finished_at=result.ended_at,
        artifacts=tuple(ArtifactRef.from_path(path) for path in result.produced_paths),
        risks=(result.error,) if result.error else (),
        production_effect=ProductionEffect.NONE,
    )


def resolve_daily_ops_default_as_of(observed_at: datetime | None = None) -> date:
    return latest_completed_us_equity_trading_day(observed_at)


def resolve_daily_ops_market_date(observed_at: datetime | None = None) -> date:
    return current_us_equity_market_date(observed_at)


def _enforce_scheduled_daily_plan(plan: DailyOpsPlan) -> None:
    scheduled = load_scheduled_tasks_config()
    daily_tasks = scheduled.cadence(DAILY_CADENCE_ID).tasks
    expected_step_ids = tuple(
        task.daily_plan_step_id for task in daily_tasks if task.daily_plan_step_id
    )
    step_by_id = {step.step_id: step for step in plan.steps}
    missing_step_ids = [step_id for step_id in expected_step_ids if step_id not in step_by_id]
    if missing_step_ids:
        raise ValueError(
            "daily ops plan is missing scheduled steps: " + ", ".join(missing_step_ids)
        )
    observed_order = tuple(step.step_id for step in plan.steps if step.step_id in expected_step_ids)
    if observed_order != expected_step_ids:
        raise ValueError(
            "daily ops plan order does not match config/scheduled_tasks.yaml: "
            + " -> ".join(observed_order)
        )
    non_daily_ids = {task.task_id for task in scheduled.non_daily_tasks()}
    leaked = sorted(non_daily_ids & {step.step_id for step in plan.steps})
    if leaked:
        raise ValueError(
            "non-daily scheduled tasks leaked into daily ops plan: " + ", ".join(leaked)
        )

    for task in daily_tasks:
        step_id = task.daily_plan_step_id
        if step_id is None:
            continue
        step = step_by_id[step_id]
        if (
            not plan.market_session.is_trading_day
            and task.closed_market_behavior == "skip_score_artifacts"
            and step.enabled
        ):
            raise ValueError(f"closed-market daily plan must skip score artifact step: {step_id}")
        if not step.enabled:
            continue
        command_text = " ".join(step.command)
        missing_tokens = [
            token for token in task.command_contains if token and token not in command_text
        ]
        if missing_tokens:
            raise ValueError(
                f"daily ops step {step_id} does not match scheduled command tokens: "
                + ", ".join(missing_tokens)
            )


def build_daily_ops_plan(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    download_start: date = date(2018, 1, 1),
    include_download_data: bool = True,
    include_pit_snapshots: bool = True,
    include_sec_fundamentals: bool = True,
    include_valuation_snapshots: bool = True,
    include_secret_scan: bool = True,
    skip_risk_event_openai_precheck: bool = False,
    full_universe: bool = False,
    llm_request_profile: str = "risk_event_daily_official_precheck",
    risk_event_openai_precheck_max_candidates: int | None = None,
    run_id: str | None = None,
) -> DailyOpsPlan:
    if (
        risk_event_openai_precheck_max_candidates is not None
        and risk_event_openai_precheck_max_candidates < 0
    ):
        raise ValueError("risk_event_openai_precheck_max_candidates must be non-negative")

    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    reports_dir = project_root / "outputs" / "reports"
    as_of_text = as_of.isoformat()
    market_session = us_equity_market_session(as_of)
    download_end = as_of if market_session.is_trading_day else market_session.previous_trading_day
    if download_start > download_end:
        raise ValueError("download_start must not be later than the download end date")

    market_cache_coverage = _market_cache_coverage(
        raw_dir=raw_dir,
        required_date=download_end,
    )
    download_enabled = include_download_data
    download_skip_reason = None if include_download_data else "显式跳过数据下载，只复用已有缓存。"
    if include_download_data and not market_session.is_trading_day:
        if market_cache_coverage.covers_required_date:
            download_enabled = False
            download_skip_reason = (
                "休市日模式：美股无新收盘价；上一交易日 "
                f"{download_end.isoformat()} 已由价格缓存覆盖"
                f"（{market_cache_coverage.summary}），跳过 download-data。"
            )
        else:
            download_skip_reason = None

    download_command = [
        "aits",
        "download-data",
        "--start",
        download_start.isoformat(),
        "--end",
        download_end.isoformat(),
    ]
    if full_universe:
        download_command.append("--full-universe")

    pit_raw_dir = raw_dir / "fmp_forward_pit"
    pit_manifest = raw_dir / "pit_snapshots" / "manifest.csv"
    pit_normalized_dir = processed_dir / "pit_snapshots"
    pit_fetch_report = default_fmp_forward_pit_fetch_report_path(reports_dir, as_of)
    pit_normalized = default_fmp_forward_pit_normalized_path(
        pit_normalized_dir,
        as_of,
    )
    pit_validation_report = default_pit_snapshot_validation_report_path(
        reports_dir,
        as_of,
    )
    sec_companyfacts_dir = raw_dir / "sec_companyfacts"
    sec_companyfacts_validation_report = default_sec_companyfacts_validation_report_path(
        reports_dir,
        as_of,
    )
    sec_metrics_csv = default_sec_fundamental_metrics_csv_path(processed_dir, as_of)
    tsm_ir_metrics_csv = processed_dir / "tsm_ir_quarterly_metrics.csv"
    sec_metrics_report = default_sec_fundamental_metrics_report_path(
        reports_dir,
        as_of,
    )
    sec_metrics_validation_report = default_sec_fundamental_metrics_validation_report_path(
        reports_dir,
        as_of,
    )
    valuation_snapshots_dir = project_root / "data" / "external" / "valuation_snapshots"
    fmp_analyst_history_dir = default_fmp_analyst_estimate_history_dir(raw_dir)
    fmp_valuation_fetch_report = default_fmp_valuation_fetch_report_path(
        reports_dir,
        as_of,
    )
    valuation_validation_report = default_valuation_validation_report_path(
        reports_dir,
        as_of,
    )
    official_policy_raw_dir = raw_dir / "official_policy_sources"
    official_policy_report = default_official_policy_fetch_report_path(reports_dir, as_of)
    official_policy_candidates = default_official_policy_candidates_path(
        processed_dir,
        as_of,
    )

    score_command = [
        "aits",
        "score-daily",
        "--as-of",
        as_of_text,
    ]
    score_required_env: tuple[str, ...] = ()
    if skip_risk_event_openai_precheck:
        score_command.append("--skip-risk-event-openai-precheck")
    else:
        score_command.extend(["--llm-request-profile", llm_request_profile])
        if risk_event_openai_precheck_max_candidates is not None:
            score_command.extend(
                [
                    "--risk-event-openai-precheck-max-candidates",
                    str(risk_event_openai_precheck_max_candidates),
                ]
            )
    if not skip_risk_event_openai_precheck:
        score_required_env = ("OPENAI_API_KEY",)
    if run_id:
        score_command.extend(["--run-id", run_id])
    score_enabled = market_session.is_trading_day
    dashboard_enabled = score_enabled
    scoring_artifact_skip_reason = (
        None
        if dashboard_enabled
        else (
            "休市日模式：未生成新的 daily_score、decision snapshot、evidence bundle "
            "或执行动作，因此跳过 score-derived Reader Brief 链路。"
        )
    )
    data_quality_report = default_quality_report_path(reports_dir, download_end)
    score_change_report = default_score_change_attribution_report_path(reports_dir, as_of)
    score_change_json = default_score_change_attribution_json_path(reports_dir, as_of)
    market_panel_report = default_market_panel_report_path(reports_dir, as_of)
    market_panel_json = default_market_panel_json_path(reports_dir, as_of)
    market_data_freshness_root = project_root / "artifacts" / "data_freshness"
    market_data_refresh_root = project_root / "artifacts" / "data_refresh"
    portfolio_candidate_tracking_root = project_root / "artifacts" / "portfolio_candidate_tracking"
    portfolio_tracking_review_root = project_root / "artifacts" / "portfolio_tracking_reviews"
    market_data_freshness_json = default_market_data_freshness_json_path(
        market_data_freshness_root,
        as_of,
    )
    market_data_freshness_md = default_market_data_freshness_markdown_path(
        market_data_freshness_root,
        as_of,
    )
    market_data_refresh_plan = default_market_data_refresh_plan_path(
        market_data_refresh_root,
        as_of,
    )
    market_data_refresh_json = default_market_data_refresh_json_path(
        market_data_refresh_root,
        as_of,
    )
    market_data_refresh_md = default_market_data_refresh_markdown_path(
        market_data_refresh_root,
        as_of,
    )
    portfolio_candidate_tracking_json = default_portfolio_candidate_tracking_json_path(
        portfolio_candidate_tracking_root,
        as_of,
    )
    portfolio_candidate_tracking_md = default_portfolio_candidate_tracking_markdown_path(
        portfolio_candidate_tracking_root,
        as_of,
    )
    active_shadow_candidates = default_active_shadow_candidates_path(
        portfolio_candidate_tracking_root,
    )
    portfolio_tracking_review_json = default_portfolio_tracking_review_json_path(
        portfolio_tracking_review_root,
        as_of,
    )
    portfolio_tracking_review_md = default_portfolio_tracking_review_markdown_path(
        portfolio_tracking_review_root,
        as_of,
    )
    portfolio_tracking_review_alias_json, portfolio_tracking_review_alias_md = (
        portfolio_tracking_review_report_alias_paths(reports_dir, as_of)
    )
    etf_forward_root = project_root / "reports" / "etf_portfolio" / "forward"
    etf_forward_update_json = etf_forward_root / "updates" / f"forward_update_{as_of_text}.json"
    etf_forward_update_md = etf_forward_root / "updates" / f"forward_update_{as_of_text}.md"
    etf_forward_dashboard_json = (
        etf_forward_root / "dashboard" / f"forward_dashboard_{as_of_text}.json"
    )
    etf_forward_dashboard_md = etf_forward_root / "dashboard" / f"forward_dashboard_{as_of_text}.md"
    etf_forward_watchlist_json = (
        etf_forward_root / "watchlist" / f"forward_watchlist_{as_of_text}.json"
    )
    etf_forward_watchlist_md = etf_forward_root / "watchlist" / f"forward_watchlist_{as_of_text}.md"
    report_index_html = default_report_index_html_path(reports_dir, as_of)
    report_index_json = default_report_index_json_path(reports_dir, as_of)
    documentation_contract_report = default_documentation_contract_report_path(reports_dir, as_of)
    documentation_contract_json = default_documentation_contract_json_path(reports_dir, as_of)
    research_governance_report = default_research_governance_summary_report_path(
        reports_dir,
        as_of,
    )
    research_governance_json = default_research_governance_summary_json_path(
        reports_dir,
        as_of,
    )
    reader_brief_html = default_reader_brief_html_path(reports_dir, as_of)
    reader_brief_json = default_reader_brief_json_path(reports_dir, as_of)
    reader_brief_quality_json = default_reader_brief_quality_json_path(reports_dir, as_of)
    reader_brief_quality_report = default_reader_brief_quality_markdown_path(
        reports_dir,
        as_of,
    )

    steps = [
        DailyOpsStep(
            step_id="download_data",
            title="更新市场和宏观缓存",
            command=tuple(download_command) if download_enabled else (),
            required_env_vars=(("FMP_API_KEY", "MARKETSTACK_API_KEY") if download_enabled else ()),
            produced_paths=(
                raw_dir / "prices_daily.csv",
                raw_dir / "prices_marketstack_daily.csv",
                raw_dir / "rates_daily.csv",
                raw_dir / "download_manifest.csv",
                default_download_failure_report_path(reports_dir, download_end),
            ),
            quality_gate=(
                "下载审计 manifest 记录 provider、endpoint、请求参数、row count 和 checksum；"
                "失败时写入脱敏 download_data_diagnostics 报告并停止下游。"
            ),
            blocks_downstream=True,
            enabled=download_enabled,
            skip_reason=download_skip_reason,
            input_visibility="live_provider",
        ),
        DailyOpsStep(
            step_id="validate_data",
            title="校验市场和宏观缓存",
            command=("aits", "validate-data", "--as-of", download_end.isoformat()),
            required_env_vars=(),
            produced_paths=(data_quality_report,),
            quality_gate=(
                "`aits validate-data` 是缓存市场/宏观数据进入技术特征、评分、"
                "回测和报告链路前的强制质量门禁；失败时停止下游。"
            ),
            blocks_downstream=True,
            input_visibility="derived_local",
        ),
    ]
    if not market_session.is_trading_day:
        steps.append(
            DailyOpsStep(
                step_id="official_policy_sources",
                title="休市日抓取官方政策和地缘来源",
                command=(
                    "aits",
                    "risk-events",
                    "fetch-official-sources",
                    "--as-of",
                    as_of_text,
                    "--raw-dir",
                    str(official_policy_raw_dir),
                    "--processed-dir",
                    str(processed_dir),
                    "--output-path",
                    str(official_policy_report),
                ),
                required_env_vars=(),
                produced_paths=(
                    official_policy_raw_dir / as_of_text,
                    official_policy_candidates,
                    official_policy_report,
                ),
                quality_gate=(
                    "休市日不生成交易评分，但仍抓取官方政策/地缘来源并输出 "
                    "pending_review 候选；失败时停止，避免漏报非交易日风险事件。"
                ),
                blocks_downstream=True,
                input_visibility="live_provider",
            )
        )
    steps.extend(
        [
            DailyOpsStep(
                step_id="pit_snapshots_fetch_fmp_forward",
                title="抓取 FMP forward-only PIT 快照",
                command=(
                    (
                        "aits",
                        "pit-snapshots",
                        "fetch-fmp-forward",
                        "--as-of",
                        as_of_text,
                        "--continue-on-failure",
                    )
                    if include_pit_snapshots
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    pit_raw_dir,
                    pit_normalized,
                    pit_fetch_report,
                ),
                quality_gate=(
                    "命令读取 FMP_API_KEY 并刷新 forward-only PIT raw/normalized 缓存；"
                    "`--continue-on-failure` 只保证故障被结构化记录，后续 manifest "
                    "和 validate 步骤仍会显式披露 PIT 可用性。"
                ),
                blocks_downstream=False,
                enabled=include_pit_snapshots,
                skip_reason=(
                    None
                    if include_pit_snapshots
                    else "显式跳过 PIT 抓取；缺跑日期不能事后补成 strict PIT。"
                ),
                input_visibility="live_provider",
            ),
            DailyOpsStep(
                step_id="pit_snapshots_build_manifest",
                title="重建 PIT 快照 manifest",
                command=(
                    ("aits", "pit-snapshots", "build-manifest", "--as-of", as_of_text)
                    if include_pit_snapshots
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(pit_manifest, pit_validation_report),
                quality_gate=(
                    "从现有 PIT raw cache 重建 manifest，记录 provider、endpoint、"
                    "请求参数、row count 和 checksum；失败时停止下游。"
                ),
                blocks_downstream=True,
                enabled=include_pit_snapshots,
                skip_reason=(
                    None
                    if include_pit_snapshots
                    else "显式跳过 PIT manifest 重建；缺跑日期不能事后补成 strict PIT。"
                ),
                input_visibility="derived_local",
            ),
            DailyOpsStep(
                step_id="pit_snapshots_validate",
                title="校验 PIT 快照 manifest",
                command=(
                    ("aits", "pit-snapshots", "validate", "--as-of", as_of_text)
                    if include_pit_snapshots
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(pit_validation_report,),
                quality_gate=(
                    "校验 PIT manifest schema、checksum、available_time 和 source catalog；"
                    "失败时停止下游，避免把不可审计 PIT 输入送入报告链。"
                ),
                blocks_downstream=True,
                enabled=include_pit_snapshots,
                skip_reason=(
                    None
                    if include_pit_snapshots
                    else "显式跳过 PIT manifest 校验；score-daily 仍会执行自身质量门禁。"
                ),
                input_visibility="derived_local",
            ),
            DailyOpsStep(
                step_id="sec_companyfacts",
                title="刷新 SEC companyfacts 原始缓存",
                command=(
                    (
                        "aits",
                        "fundamentals",
                        "download-sec-companyfacts",
                    )
                    if include_sec_fundamentals
                    else ()
                ),
                required_env_vars=("SEC_USER_AGENT",) if include_sec_fundamentals else (),
                produced_paths=(
                    sec_companyfacts_dir,
                    sec_companyfacts_dir / "sec_companyfacts_manifest.csv",
                ),
                quality_gate=(
                    "命令使用 SEC_USER_AGENT 调用 SEC EDGAR companyfacts，并写入 "
                    "endpoint、请求参数、row count 和 checksum manifest；失败时停止日报前置链路。"
                ),
                blocks_downstream=True,
                enabled=include_sec_fundamentals,
                skip_reason=(
                    None
                    if include_sec_fundamentals
                    else "显式跳过 SEC companyfacts 刷新，只复用已有 SEC 原始缓存。"
                ),
                input_visibility="live_provider",
            ),
            DailyOpsStep(
                step_id="sec_metrics",
                title="抽取当日 SEC 基本面指标",
                command=(
                    (
                        "aits",
                        "fundamentals",
                        "extract-sec-metrics",
                        "--as-of",
                        as_of_text,
                    )
                    if include_sec_fundamentals
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    sec_companyfacts_validation_report,
                    sec_metrics_csv,
                    sec_metrics_report,
                ),
                quality_gate=(
                    "先复用 SEC companyfacts 质量门禁，通过后写入当日 "
                    "sec_fundamentals CSV；后续 score-daily 会再次校验该 CSV 并构建 SEC 特征。"
                ),
                blocks_downstream=True,
                enabled=include_sec_fundamentals,
                skip_reason=(
                    None
                    if include_sec_fundamentals
                    else "显式跳过 SEC metrics 抽取，score-daily 只能校验既有当日 CSV。"
                ),
                input_visibility="derived_local",
            ),
            DailyOpsStep(
                step_id="tsm_ir_sec_metrics_merge",
                title="合并 TSMC IR 季度指标",
                command=(
                    (
                        "aits",
                        "fundamentals",
                        "merge-tsm-ir-sec-metrics",
                        "--as-of",
                        as_of_text,
                    )
                    if include_sec_fundamentals
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    tsm_ir_metrics_csv,
                    sec_metrics_csv,
                    sec_metrics_validation_report,
                ),
                quality_gate=(
                    "按 as-of 选择已披露的 TSMC IR 官方季度 Management Report 指标，"
                    "合并到当日 SEC-style metrics 后再进入统一校验；缺失或转换失败时阻断下游。"
                ),
                blocks_downstream=True,
                enabled=include_sec_fundamentals,
                skip_reason=(
                    None
                    if include_sec_fundamentals
                    else "显式跳过 SEC fundamentals，TSMC IR 合并也不执行。"
                ),
                input_visibility="derived_local",
            ),
            DailyOpsStep(
                step_id="sec_metrics_validation",
                title="校验当日 SEC 基本面指标 CSV",
                command=(
                    (
                        "aits",
                        "fundamentals",
                        "validate-sec-metrics",
                        "--as-of",
                        as_of_text,
                    )
                    if include_sec_fundamentals
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(sec_metrics_validation_report,),
                quality_gate=(
                    "校验当日 SEC metrics schema、重复键、未来披露日期、数值合法性和覆盖率；"
                    "失败时不进入 score-daily。"
                ),
                blocks_downstream=True,
                enabled=include_sec_fundamentals,
                skip_reason=(
                    None
                    if include_sec_fundamentals
                    else "显式跳过 SEC metrics 预校验，score-daily 仍会执行同一门禁。"
                ),
                input_visibility="derived_local",
            ),
            DailyOpsStep(
                step_id="valuation_snapshots",
                title="刷新 FMP 估值和预期快照",
                command=(
                    (
                        "aits",
                        "valuation",
                        "fetch-fmp",
                        "--as-of",
                        as_of_text,
                    )
                    if include_valuation_snapshots
                    else ()
                ),
                required_env_vars=("FMP_API_KEY",) if include_valuation_snapshots else (),
                produced_paths=(
                    valuation_snapshots_dir,
                    fmp_analyst_history_dir,
                    fmp_valuation_fetch_report,
                    valuation_validation_report,
                ),
                quality_gate=(
                    "命令读取 FMP_API_KEY 写入估值快照 YAML、analyst history 和校验报告；"
                    "失败时停止，避免日报读取过期估值输入。"
                ),
                blocks_downstream=True,
                enabled=include_valuation_snapshots,
                skip_reason=(
                    None
                    if include_valuation_snapshots
                    else "显式跳过估值快照刷新，只复用已有 valuation_snapshots。"
                ),
                input_visibility="live_provider",
            ),
            DailyOpsStep(
                step_id="score_daily",
                title="生成每日评分、日报、trace 和告警",
                command=tuple(score_command) if score_enabled else (),
                required_env_vars=score_required_env if score_enabled else (),
                produced_paths=(
                    processed_dir / "features_daily.csv",
                    processed_dir / "scores_daily.csv",
                    default_quality_report_path(reports_dir, as_of),
                    default_feature_report_path(reports_dir, as_of),
                    default_daily_score_report_path(reports_dir, as_of),
                    default_alert_report_path(reports_dir, as_of),
                    processed_dir / "risk_event_prereview_queue.json",
                    reports_dir / f"risk_event_llm_formal_assessment_{as_of_text}.md",
                ),
                quality_gate=(
                    "`score-daily` 内部先运行市场数据质量门禁，并校验 SEC metrics、"
                    "估值快照、风险事件发生记录和 rule card；OpenAI 预审成功后会写入 "
                    "LLM formal 风险评估，失败时停止。"
                ),
                blocks_downstream=True,
                enabled=score_enabled,
                skip_reason=(
                    None
                    if score_enabled
                    else (
                        "休市日模式：无新美股收盘价，不生成新的 daily_score、"
                        "decision snapshot、evidence bundle 或执行动作。"
                    )
                ),
                input_visibility=(
                    "live_provider" if not skip_risk_event_openai_precheck else "derived_local"
                ),
            ),
            DailyOpsStep(
                step_id="reports_dashboard",
                title="生成只读决策 dashboard",
                command=(
                    (
                        "aits",
                        "reports",
                        "dashboard",
                        "--as-of",
                        as_of_text,
                    )
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    default_evidence_dashboard_path(reports_dir, as_of),
                    default_evidence_dashboard_json_path(reports_dir, as_of),
                ),
                quality_gate=(
                    "只读读取日报、trace、decision snapshot、alerts、scores_daily "
                    "并生成 HTML/JSON 展示层；production_effect=none，不改变评分、仓位或执行建议。"
                ),
                blocks_downstream=False,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="sec_pit_shadow_observe",
                title="生成 SEC PIT observe-only shadow lane",
                command=(
                    (
                        "aits",
                        "sec-pit",
                        "shadow-observe",
                        "--latest",
                        "--end",
                        as_of_text,
                    )
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    project_root
                    / "outputs"
                    / "sec_pit_shadow_observe"
                    / f"sec_pit_shadow_observe_summary_{as_of_text}.json",
                    project_root
                    / "outputs"
                    / "sec_pit_shadow_observe"
                    / f"sec_pit_shadow_observe_summary_{as_of_text}.md",
                ),
                quality_gate=(
                    "只读刷新 SEC PIT observe-only lane；production_effect=none，"
                    "不写 production weights、不写 active shadow weights、不触发交易。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="sec_pit_shadow_monitor",
                title="生成 SEC PIT shadow monitor",
                command=(
                    (
                        "aits",
                        "sec-pit",
                        "shadow-monitor",
                        "--latest",
                        "--as-of",
                        as_of_text,
                    )
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    project_root
                    / "outputs"
                    / "sec_pit_shadow_monitor"
                    / f"sec_pit_shadow_monitor_summary_{as_of_text}.json",
                    project_root
                    / "outputs"
                    / "sec_pit_shadow_monitor"
                    / f"sec_pit_shadow_monitor_summary_{as_of_text}.md",
                ),
                quality_gate=(
                    "只读滚动监控 SEC PIT observe-only lane；production_effect=none，"
                    "不写 production weights、不写 active shadow weights、不触发交易。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="score_change_attribution",
                title="生成 score change attribution",
                command=(
                    ("aits", "reports", "score-change-attribution", "--latest")
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(score_change_report, score_change_json),
                quality_gate=(
                    "只读比较 latest decision snapshot 与上一条 snapshot；不重算 score，"
                    "production_effect=none。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="market_panel",
                title="生成 market panel",
                command=(
                    ("aits", "reports", "market-panel", "--latest") if dashboard_enabled else ()
                ),
                required_env_vars=(),
                produced_paths=(market_panel_report, market_panel_json),
                quality_gate=(
                    "只读生成 SPY/QQQ/SMH/SOXX/VIX/DGS10 市场上下文，"
                    "先执行同一 data quality 门禁；production_effect=none。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="market_data_freshness",
                title="生成 market data freshness tracking readiness",
                command=(("aits", "data", "freshness", "--latest") if dashboard_enabled else ()),
                required_env_vars=(),
                produced_paths=(market_data_freshness_json, market_data_freshness_md),
                quality_gate=(
                    "只读检查 latest market data freshness 和 candidate tracking readiness；"
                    "不降低 data gate，不把 stale 数据静默传给 tracking review。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="derived_local",
            ),
            DailyOpsStep(
                step_id="market_data_recover_freshness",
                title="执行 market data freshness recovery",
                command=(
                    ("aits", "data", "recover-freshness", "--latest") if dashboard_enabled else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    market_data_refresh_plan,
                    market_data_refresh_json,
                    market_data_refresh_md,
                ),
                quality_gate=(
                    "在 freshness 不足时尝试受控 refresh / manifest recovery；"
                    "失败必须显式阻断，不伪造价格或 tracking days。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="live_provider_or_cached",
            ),
            DailyOpsStep(
                step_id="portfolio_candidate_tracking",
                title="滚动 active portfolio candidate tracking",
                command=(
                    ("aits", "portfolio", "track-candidate", "--latest")
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    portfolio_candidate_tracking_json,
                    portfolio_candidate_tracking_md,
                    active_shadow_candidates,
                ),
                quality_gate=(
                    "读取 latest candidate review、freshness 和 data gate 后写 shadow "
                    "tracking artifact；production_effect=none，不启用 candidate。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="portfolio_tracking_review",
                title="生成 portfolio tracking review window progress",
                command=(
                    (
                        "aits",
                        "portfolio",
                        "review-tracking",
                        "--latest",
                        "--show-window-progress",
                    )
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(portfolio_tracking_review_json, portfolio_tracking_review_md),
                quality_gate=(
                    "读取真实 daily tracking summaries 生成 window progress；"
                    "tracking_days<5 时保持 needs_more_data，不作为失败。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="portfolio_tracking_review_report",
                title="生成 portfolio tracking review report alias",
                command=(
                    ("aits", "reports", "portfolio-tracking-review", "--latest")
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(
                    portfolio_tracking_review_alias_json,
                    portfolio_tracking_review_alias_md,
                ),
                quality_gate=(
                    "只读读取 tracking review artifact 并写 reports alias；不运行上游 "
                    "candidate tracking 或修改 production。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="etf_forward_update",
                title="更新 ETF forward shadow simulation",
                command=(
                    ("aits", "etf", "forward", "update", "--latest") if dashboard_enabled else ()
                ),
                required_env_vars=(),
                produced_paths=(etf_forward_update_json, etf_forward_update_md),
                quality_gate=(
                    "在全局 validate-data 通过后读取 ETF price cache 和 shadow registry；"
                    "只写 evaluation-only forward update，不修改 production weights。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="local_or_readonly",
            ),
            DailyOpsStep(
                step_id="etf_forward_dashboard",
                title="生成 ETF forward simulation dashboard",
                command=(
                    ("aits", "etf", "forward", "dashboard", "--latest") if dashboard_enabled else ()
                ),
                required_env_vars=(),
                produced_paths=(etf_forward_dashboard_json, etf_forward_dashboard_md),
                quality_gate=(
                    "只读 latest forward update 和 shadow registry；输出 candidate、baseline "
                    "和 benchmark 对比，不触发 broker action。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="etf_forward_watchlist",
                title="生成 ETF forward simulation watchlist",
                command=(
                    ("aits", "etf", "forward", "watchlist", "--latest") if dashboard_enabled else ()
                ),
                required_env_vars=(),
                produced_paths=(etf_forward_watchlist_json, etf_forward_watchlist_md),
                quality_gate=(
                    "只读 dashboard 生成本地 attention summary；allowed actions "
                    "限定为 manual review / observation，不发送外部 alert。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="report_index",
                title="生成 report registry index",
                command=("aits", "reports", "index", "--latest") if dashboard_enabled else (),
                required_env_vars=(),
                produced_paths=(report_index_html, report_index_json),
                quality_gate=(
                    "只读扫描 report registry 和已存在 artifacts；production_effect=none。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="documentation_contract",
                title="生成 documentation contract",
                command=(
                    ("aits", "docs", "report-contract", "--latest") if dashboard_enabled else ()
                ),
                required_env_vars=(),
                produced_paths=(documentation_contract_report, documentation_contract_json),
                quality_gate=(
                    "只读校验 report registry 与 artifact catalog 契约；"
                    "production_effect=none，不运行上游报告。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="research_governance_summary",
                title="生成 research governance summary",
                command=(
                    ("aits", "reports", "research-governance-summary", "--latest")
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(research_governance_report, research_governance_json),
                quality_gate=(
                    "只读汇总 backtest、weight、shadow observe、SEC PIT、documentation "
                    "和 registry 状态；production_effect=none，不改变生产权重或交易行为。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="reader_brief",
                title="生成 Reader Brief",
                command=(
                    ("aits", "reports", "reader-brief", "--latest") if dashboard_enabled else ()
                ),
                required_env_vars=(),
                produced_paths=(reader_brief_html, reader_brief_json),
                quality_gate=(
                    "只读消费 daily decision summary、dashboard、score change、market panel、"
                    "research governance、report index 和 documentation contract；"
                    "production_effect=none。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="validate_reader_brief",
                title="校验 Reader Brief",
                command=(
                    ("aits", "reports", "validate-reader-brief", "--latest")
                    if dashboard_enabled
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(reader_brief_quality_json, reader_brief_quality_report),
                quality_gate=(
                    "只读校验既有 Reader Brief JSON/HTML；不运行上游报告，"
                    "production_effect=none。"
                ),
                blocks_downstream=True,
                enabled=dashboard_enabled,
                skip_reason=scoring_artifact_skip_reason,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="pipeline_health",
                title="检查关键 artifact 和 PIT 健康",
                command=(
                    ("aits", "ops", "health", "--as-of", as_of_text)
                    if market_session.is_trading_day
                    else (
                        "aits",
                        "ops",
                        "health",
                        "--as-of",
                        as_of_text,
                        "--non-trading-day",
                    )
                ),
                required_env_vars=(),
                produced_paths=(
                    default_pipeline_health_report_path(reports_dir, as_of),
                    default_pipeline_health_alert_report_path(reports_dir, as_of),
                ),
                quality_gate="只读运行健康检查；不把 pipeline health 解释为投资结论有效。",
                blocks_downstream=False,
                input_visibility="readonly",
            ),
            DailyOpsStep(
                step_id="secret_hygiene",
                title="扫描报告和配置中的疑似 secret",
                command=(
                    ("aits", "security", "scan-secrets", "--as-of", as_of_text)
                    if include_secret_scan
                    else ()
                ),
                required_env_vars=(),
                produced_paths=(default_secret_scan_report_path(reports_dir, as_of),),
                quality_gate="只输出脱敏片段；不输出完整疑似密钥。",
                blocks_downstream=False,
                enabled=include_secret_scan,
                skip_reason=None if include_secret_scan else "显式跳过 secret hygiene 扫描。",
                input_visibility="readonly",
            ),
        ]
    )
    plan = DailyOpsPlan(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        steps=tuple(steps),
        market_session=market_session,
    )
    _enforce_scheduled_daily_plan(plan)
    return plan


def run_daily_ops_plan(
    plan: DailyOpsPlan,
    *,
    project_root: Path = PROJECT_ROOT,
    env: Mapping[str, str] | None = None,
    runner: Any = DailyOpsCommandRunner,
    stop_on_failure: bool = True,
    run_id: str | None = None,
    diagnostics_dir: Path | None = None,
    visibility_check_date: date | None = None,
    visibility_latest_completed_trading_day: date | None = None,
) -> DailyOpsRunReport:
    started_at = datetime.now(tz=UTC)
    resolved_diagnostics_dir = (
        diagnostics_dir
        if diagnostics_dir is not None
        else project_root / "outputs" / "reports" / "diagnostics" / "daily_ops"
    )
    checked_env = dict(os.environ if env is None else env)
    checked_env["PYTHONFAULTHANDLER"] = "1"
    checked_env["PYTHONMALLOC"] = "malloc"
    checked_env["PYTHONDONTWRITEBYTECODE"] = "1"
    run_pycache_token = f"run_{started_at.strftime('%Y%m%dT%H%M%S%fZ')}_{os.getpid()}"
    pycache_prefix = project_root / "outputs" / "tmp" / "pycache" / "daily_run" / run_pycache_token
    pycache_prefix.mkdir(parents=True, exist_ok=True)
    checked_env["PYTHONPYCACHEPREFIX"] = str(pycache_prefix)
    _purge_source_pycache_dirs(project_root)
    pre_run_input_artifacts = _build_pre_run_input_artifacts(plan, project_root)
    visibility_run_date = visibility_check_date or resolve_daily_ops_market_date(started_at)
    visibility_completed_trading_day = (
        visibility_latest_completed_trading_day
        if visibility_latest_completed_trading_day is not None
        else visibility_check_date or resolve_daily_ops_default_as_of(started_at)
    )
    visibility_issues = _validate_daily_ops_input_visibility(
        plan,
        run_date=visibility_run_date,
        latest_completed_trading_day=visibility_completed_trading_day,
    )
    if visibility_issues:
        finished_at = datetime.now(tz=UTC)
        status = "BLOCKED_VISIBILITY"
        result = DailyOpsStepResult(
            step_id="input_visibility",
            title="输入可见性预检查",
            command=(),
            status="FAIL",
            return_code=None,
            started_at=started_at,
            ended_at=finished_at,
            duration_seconds=(finished_at - started_at).total_seconds(),
            produced_paths=(),
            blocks_downstream=True,
            error=_visibility_issue_summary(visibility_issues),
        )
        return DailyOpsRunReport(
            plan=plan,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            step_results=(result,),
            visibility_issues=visibility_issues,
            metadata=_build_daily_ops_run_metadata(
                plan=plan,
                project_root=project_root,
                env=checked_env,
                results=(result,),
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                pre_run_input_artifacts=pre_run_input_artifacts,
                run_id=run_id,
                visibility_issues=visibility_issues,
            ),
        )
    missing_env = plan.missing_env_vars(checked_env)
    if missing_env:
        finished_at = datetime.now(tz=UTC)
        status = "BLOCKED_ENV"
        return DailyOpsRunReport(
            plan=plan,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            step_results=(),
            missing_env_vars=missing_env,
            visibility_issues=(),
            metadata=_build_daily_ops_run_metadata(
                plan=plan,
                project_root=project_root,
                env=checked_env,
                results=(),
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                pre_run_input_artifacts=pre_run_input_artifacts,
                run_id=run_id,
            ),
        )

    results: list[DailyOpsStepResult] = []
    for step in plan.steps:
        if not step.enabled:
            results.append(
                DailyOpsStepResult(
                    step_id=step.step_id,
                    title=step.title,
                    command=step.command,
                    status="SKIPPED",
                    return_code=None,
                    started_at=None,
                    ended_at=None,
                    duration_seconds=None,
                    produced_paths=step.produced_paths,
                    blocks_downstream=step.blocks_downstream,
                    skip_reason=step.skip_reason,
                )
            )
            continue

        execution_step = replace(
            step,
            command=_daily_ops_step_command_with_visibility_cutoff(
                step,
                plan=plan,
                visibility_cutoff=started_at,
                latest_completed_trading_day=visibility_completed_trading_day,
            ),
        )
        step_started = datetime.now(tz=UTC)
        try:
            completed = runner(
                _execution_command(execution_step.command, project_root=project_root),
                cwd=project_root,
                env=checked_env,
                text=True,
                capture_output=True,
                check=False,
            )
            step_ended = datetime.now(tz=UTC)
            return_code = completed.returncode
            stdout_text = completed.stdout or ""
            stderr_text = completed.stderr or ""
            artifact_error = _post_step_artifact_status_error(step) if return_code == 0 else None
            status = "PASS" if return_code == 0 and artifact_error is None else "FAIL"
            diagnostic_path = None
            if status == "FAIL":
                diagnostic_path = _write_step_failure_diagnostic(
                    step=execution_step,
                    as_of=plan.as_of,
                    started_at=step_started,
                    ended_at=step_ended,
                    return_code=return_code,
                    stdout_text=stdout_text,
                    stderr_text=stderr_text,
                    error=artifact_error,
                    env=checked_env,
                    diagnostics_dir=resolved_diagnostics_dir,
                )
            result = DailyOpsStepResult(
                step_id=step.step_id,
                title=step.title,
                command=execution_step.command,
                status=status,
                return_code=return_code,
                started_at=step_started,
                ended_at=step_ended,
                duration_seconds=(step_ended - step_started).total_seconds(),
                produced_paths=step.produced_paths,
                blocks_downstream=step.blocks_downstream,
                stdout_line_count=len(stdout_text.splitlines()),
                stderr_line_count=len(stderr_text.splitlines()),
                error=artifact_error,
                diagnostic_path=diagnostic_path,
            )
        except OSError as exc:
            step_ended = datetime.now(tz=UTC)
            error = f"{type(exc).__name__}: {exc}"
            diagnostic_path = _write_step_failure_diagnostic(
                step=execution_step,
                as_of=plan.as_of,
                started_at=step_started,
                ended_at=step_ended,
                return_code=None,
                stdout_text="",
                stderr_text="",
                error=error,
                env=checked_env,
                diagnostics_dir=resolved_diagnostics_dir,
            )
            result = DailyOpsStepResult(
                step_id=step.step_id,
                title=step.title,
                command=execution_step.command,
                status="FAIL",
                return_code=None,
                started_at=step_started,
                ended_at=step_ended,
                duration_seconds=(step_ended - step_started).total_seconds(),
                produced_paths=step.produced_paths,
                blocks_downstream=step.blocks_downstream,
                error=error,
                diagnostic_path=diagnostic_path,
            )
        results.append(result)
        if result.status == "FAIL" and (stop_on_failure or step.blocks_downstream):
            break

    finished_at = datetime.now(tz=UTC)
    status = _daily_run_status(results)
    return DailyOpsRunReport(
        plan=plan,
        started_at=started_at,
        finished_at=finished_at,
        status=status,
        step_results=tuple(results),
        visibility_issues=(),
        metadata=_build_daily_ops_run_metadata(
            plan=plan,
            project_root=project_root,
            env=checked_env,
            results=tuple(results),
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            pre_run_input_artifacts=pre_run_input_artifacts,
            run_id=run_id,
        ),
    )


def _validate_daily_ops_input_visibility(
    plan: DailyOpsPlan,
    *,
    run_date: date,
    latest_completed_trading_day: date | None = None,
) -> tuple[DailyOpsInputVisibilityIssue, ...]:
    current_production_as_of = (
        latest_completed_trading_day if plan.market_session.is_trading_day else run_date
    )
    if current_production_as_of is None:
        current_production_as_of = run_date
    if plan.as_of > current_production_as_of:
        return (
            DailyOpsInputVisibilityIssue(
                code="daily_run_as_of_in_future",
                severity="error",
                message=(
                    "daily-run 不能对未来 as_of 生成生产运行；交易日请等待 "
                    "U.S. equity market 收盘后可见窗口结束，休市日请等待 "
                    "America/New_York 日期到达，或只生成 daily-plan 做调度预检查。"
                ),
            ),
        )
    if plan.as_of == current_production_as_of:
        return ()

    enabled_live_steps = tuple(
        step.step_id
        for step in plan.steps
        if step.enabled and step.input_visibility == "live_provider"
    )
    live_step_text = ", ".join(f"`{step_id}`" for step_id in enabled_live_steps)
    if not live_step_text:
        live_step_text = "无 live_provider 步骤，但 daily-run 仍会写生产路径"
    return (
        DailyOpsInputVisibilityIssue(
            code="daily_run_historical_as_of_requires_replay",
            severity="error",
            message=(
                "daily-run 是生产调度入口，不用于历史时点复现。当前运行日期 "
                f"{run_date.isoformat()} 晚于 as_of {plan.as_of.isoformat()}；"
                f"启用的 live/生产写入边界：{live_step_text}。"
                "请改用 `aits ops replay-day --mode cache-only --as-of "
                f"{plan.as_of.isoformat()}`，OpenAI 仅使用 `disabled` 或 `cache-only`。"
            ),
        ),
    )


def _visibility_issue_summary(
    issues: tuple[DailyOpsInputVisibilityIssue, ...],
) -> str:
    return "; ".join(f"{issue.code}: {issue.message}" for issue in issues)


def _visibility_issue_to_json(
    issue: DailyOpsInputVisibilityIssue,
) -> Mapping[str, object]:
    return {
        "code": issue.code,
        "severity": issue.severity,
        "message": issue.message,
        "step_id": issue.step_id,
    }


def render_daily_ops_plan(
    plan: DailyOpsPlan,
    *,
    env: Mapping[str, str] | None = None,
) -> str:
    checked_env = os.environ if env is None else env
    missing_by_step = plan.missing_env_by_step(checked_env)
    missing_env = plan.missing_env_vars(checked_env)
    lines = [
        "# 每日运行计划",
        "",
        f"- 状态：{plan.status(checked_env)}",
        f"- 评估日期：{plan.as_of.isoformat()}",
        f"- 市场日状态：{plan.market_session.session_status}",
        f"- 上一交易日：{plan.market_session.previous_trading_day.isoformat()}",
        f"- 市场日判断原因：{plan.market_session.reason}",
        f"- 交易日历来源：{plan.market_session.calendar_source}",
        f"- 生成时间：{plan.generated_at.isoformat()}",
        f"- 生产影响：{plan.production_effect}",
        "",
        "## 方法边界",
        "",
        "- 本计划只生成调度顺序和环境检查，不执行下载、API 调用、评分或报告生成。",
        "- 缺少关键环境变量时，后续真实执行器必须 fail closed，不能静默跳过。",
        "- 运行健康不等于投资结论有效；投资结论仍以数据质量门禁、"
        "结论使用等级、输入覆盖和审计报告为准。",
        "",
        "## 环境变量检查",
        "",
    ]
    if missing_env:
        lines.append(f"- 缺失环境变量：{', '.join(f'`{item}`' for item in missing_env)}")
    else:
        lines.append("- 必需环境变量：全部可见（仅检查是否非空，不输出值）。")
    lines.extend(
        [
            "",
            "## 步骤",
            "",
            "| 顺序 | Step | Enabled | Input Visibility | Command | Required Env | Missing Env | "
            "Gate / 边界 | Outputs | Blocks Downstream |",
            "|---:|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for index, step in enumerate(plan.steps, start=1):
        required_env = ", ".join(f"`{item}`" for item in step.required_env_vars) or ""
        step_missing = ", ".join(f"`{item}`" for item in missing_by_step.get(step.step_id, ()))
        command = _command_cell(step)
        outputs = "<br/>".join(f"`{path}`" for path in step.produced_paths)
        lines.append(
            "| "
            f"{index} | "
            f"`{step.step_id}`<br/>{_escape_table(step.title)} | "
            f"{step.enabled} | "
            f"`{step.input_visibility}` | "
            f"{command} | "
            f"{required_env} | "
            f"{step_missing} | "
            f"{_escape_table(step.quality_gate)} | "
            f"{_escape_table(outputs)} | "
            f"{step.blocks_downstream} |"
        )
    lines.extend(
        [
            "",
            "## 调度建议",
            "",
            "- 云 VM 上先用本计划确认凭据、缓存路径和输出目录，再接入真实执行器。",
            "- 建议将 stdout/stderr 写入独立日志文件，并保留本计划、质量报告、"
            "日报、pipeline health 和 secret hygiene 报告。",
            "- 如显式跳过 OpenAI 预审或 PIT 抓取，日报和运行记录必须声明该限制；"
            "不能把缺跑日期补写成当时可见数据。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_daily_ops_plan(
    plan: DailyOpsPlan,
    output_path: Path,
    *,
    env: Mapping[str, str] | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_daily_ops_plan(plan, env=env), encoding="utf-8")
    return output_path


def render_daily_ops_run_report(
    report: DailyOpsRunReport,
    *,
    metadata_path: Path | None = None,
) -> str:
    duration = (report.finished_at - report.started_at).total_seconds()
    lines = [
        "# 每日运行执行报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.plan.as_of.isoformat()}",
        f"- 市场日状态：{report.plan.market_session.session_status}",
        f"- 上一交易日：{report.plan.market_session.previous_trading_day.isoformat()}",
        f"- 市场日判断原因：{report.plan.market_session.reason}",
        f"- 交易日历来源：{report.plan.market_session.calendar_source}",
        f"- 开始时间：{report.started_at.isoformat()}",
        f"- 结束时间：{report.finished_at.isoformat()}",
        f"- 总耗时秒：{duration:.1f}",
        f"- 生产影响：{report.production_effect}",
    ]
    if report.metadata is not None:
        lines.extend(
            [
                f"- Run ID：`{report.metadata.run_id}`",
                f"- Metadata JSON：`{metadata_path}`" if metadata_path else "- Metadata JSON：",
            ]
        )
    lines.extend(
        [
            "",
            "## 方法边界",
            "",
            "- 本报告由真实每日执行器生成，会按每日运行计划顺序调用本地 CLI。",
            "- 执行器内部优先用项目 `.venv` Python 调用 daily-run direct dispatcher，",
            "找不到本地虚拟环境时才回退当前 Python，避免 Windows 上从 `aits.exe` "
            "父进程递归启动 `aits.exe`，同时绕开 Typer 对整棵 CLI 的全局解析。",
            "- 子命令环境显式设置 `PYTHONMALLOC=malloc` 和 `PYTHONFAULTHANDLER=1`，"
            "并禁用 `.pyc` 写入、隔离 `PYTHONPYCACHEPREFIX`，降低 Windows 本机"
            "长流程子进程原生崩溃风险，"
            "同时保留崩溃堆栈行数。",
            "- 启动子命令前会清理项目源码目录下已有 `__pycache__`，避免损坏的本地 "
            "bytecode cache 被后续子进程读取。",
            "- 执行报告只记录命令状态、退出码、耗时和预期 artifact 路径；",
            "不在主报告写入 stdout/stderr 原文、API key、token 或付费内容原文。",
            "- 失败步骤会写入单独的脱敏 stdout/stderr 诊断 artifact，并在阻断步骤和 "
            "metadata 中记录路径。",
            "- Metadata sidecar 记录 run id、git/config/rule hash、命令清单、"
            "env presence 和 artifact checksum；不记录 secret 值。",
            "- 投资结论仍以 `score-daily`、数据质量报告、SEC/估值校验、",
            "风险事件校验、rule card 和告警报告为准。",
            "",
        ]
    )
    if report.metadata is not None:
        metadata = report.metadata
        lines.extend(
            [
                "## 元数据归档",
                "",
                f"- Git commit：`{metadata.git.get('commit') or ''}`",
                f"- Git dirty：{metadata.git.get('dirty')}",
                f"- Visibility cutoff：{metadata.visibility_cutoff.isoformat()}",
                f"- Visibility cutoff source：{metadata.visibility_cutoff_source}",
                f"- Input visibility status：{metadata.input_visibility_status}",
                f"- Config artifacts：{len(metadata.config_artifacts)}",
                f"- Pre-run input artifacts：{len(metadata.pre_run_input_artifacts)}",
                f"- Produced artifacts：{len(metadata.produced_artifacts)}",
                f"- Env presence keys：{', '.join(f'`{key}`' for key in metadata.env_presence)}",
                "",
            ]
        )
    if report.missing_env_vars:
        lines.extend(
            [
                "## 环境变量阻断",
                "",
                "- 缺失环境变量：" + ", ".join(f"`{item}`" for item in report.missing_env_vars),
                "",
            ]
        )
    if report.visibility_issues:
        lines.extend(
            [
                "## 输入可见性阻断",
                "",
            ]
        )
        for issue in report.visibility_issues:
            step_text = "" if issue.step_id is None else f"；Step：`{issue.step_id}`"
            lines.append(
                f"- `{issue.code}`（{issue.severity}{step_text}）："
                f"{_escape_table(issue.message)}"
            )
        lines.append("")
    failed = report.failed_step
    if failed is not None:
        failed_command = (
            _escape_table(_join_command(failed.command)) if failed.command else "PRECHECK"
        )
        lines.extend(
            [
                "## 阻断步骤",
                "",
                f"- Step：`{failed.step_id}` {failed.title}",
                f"- Return code：{_display_return_code(failed.return_code)}",
                f"- Command：`{failed_command}`",
            ]
        )
        if failed.error:
            lines.append(f"- Error：`{_escape_table(failed.error)}`")
        if failed.diagnostic_path is not None:
            lines.append(f"- 失败诊断：`{failed.diagnostic_path}`")
        lines.append("")
    lines.extend(
        [
            "## 步骤结果",
            "",
            "| 顺序 | Step | Status | Return Code | Duration Seconds | Command | "
            "Stdout Lines | Stderr Lines | Outputs |",
            "|---:|---|---|---:|---:|---|---:|---:|---|",
        ]
    )
    for index, result in enumerate(report.step_results, start=1):
        duration_text = (
            f"{result.duration_seconds:.1f}" if result.duration_seconds is not None else ""
        )
        outputs = "<br/>".join(f"`{path}`" for path in result.produced_paths)
        command = (
            f"`{_escape_table(_join_command(result.command))}`" if result.command else "PRECHECK"
        )
        if result.status == "SKIPPED" and result.skip_reason:
            command = f"SKIPPED: {_escape_table(result.skip_reason)}"
        lines.append(
            "| "
            f"{index} | "
            f"`{result.step_id}`<br/>{_escape_table(result.title)} | "
            f"{result.status} | "
            f"{_display_return_code(result.return_code)} | "
            f"{duration_text} | "
            f"{command} | "
            f"{result.stdout_line_count} | "
            f"{result.stderr_line_count} | "
            f"{_escape_table(outputs)} |"
        )
    lines.extend(
        [
            "",
            "## 输出说明",
            "",
            "- `PASS` 表示命令退出码为 0；`FAIL` 表示命令退出码非 0 或命令无法启动。",
            "- `SKIPPED` 只会出现在显式传入 `--skip-*` 选项的步骤。",
            "- 报告中的 `Stdout Lines` / `Stderr Lines` 只记录行数，不在主报告保存原文。",
            "- 失败步骤如生成诊断 artifact，仅保存脱敏 stdout/stderr，"
            "并在阻断步骤和 metadata 中记录路径。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_daily_ops_run_report(
    report: DailyOpsRunReport,
    output_path: Path,
    metadata_path: Path | None = None,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_metadata_path = metadata_path or daily_ops_run_metadata_path_for_report(output_path)
    output_path.write_text(
        render_daily_ops_run_report(report, metadata_path=resolved_metadata_path),
        encoding="utf-8",
    )
    if report.metadata is not None:
        write_daily_ops_run_metadata(report.metadata, resolved_metadata_path)
    return output_path


def write_daily_ops_run_metadata(
    metadata: DailyOpsRunMetadata,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(_daily_ops_run_metadata_to_json(metadata), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def default_daily_ops_plan_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_ops_plan_{as_of.isoformat()}.md"


def default_daily_ops_run_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_ops_run_{as_of.isoformat()}.md"


def default_daily_ops_run_metadata_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_ops_run_metadata_{as_of.isoformat()}.json"


def daily_ops_run_metadata_path_for_report(output_path: Path) -> Path:
    stem = output_path.stem
    if stem.startswith("daily_ops_run_"):
        suffix = stem.removeprefix("daily_ops_run_")
        return output_path.with_name(f"daily_ops_run_metadata_{suffix}.json")
    return output_path.with_name(f"{stem}_metadata.json")


def _daily_ops_run_metadata_to_json(metadata: DailyOpsRunMetadata) -> Mapping[str, object]:
    return {
        "schema_version": metadata.schema_version,
        "run_id": metadata.run_id,
        "as_of": metadata.as_of.isoformat(),
        "generated_at": metadata.generated_at.isoformat(),
        "project_root": str(metadata.project_root),
        "status": metadata.status,
        "started_at": metadata.started_at.isoformat(),
        "finished_at": metadata.finished_at.isoformat(),
        "visibility_cutoff": metadata.visibility_cutoff.isoformat(),
        "visibility_cutoff_source": metadata.visibility_cutoff_source,
        "input_visibility_status": metadata.input_visibility_status,
        "input_visibility_issues": [dict(issue) for issue in metadata.input_visibility_issues],
        "git": dict(metadata.git),
        "config_artifacts": [
            _artifact_digest_to_json(artifact) for artifact in metadata.config_artifacts
        ],
        "rule_card_sha256": metadata.rule_card_sha256,
        "env_presence": dict(metadata.env_presence),
        "commands": [dict(command) for command in metadata.commands],
        "step_results": [dict(result) for result in metadata.step_results],
        "pre_run_input_artifacts": [
            _artifact_digest_to_json(artifact) for artifact in metadata.pre_run_input_artifacts
        ],
        "produced_artifacts": [
            _artifact_digest_to_json(artifact) for artifact in metadata.produced_artifacts
        ],
    }


def _artifact_digest_to_json(artifact: DailyOpsArtifactDigest) -> Mapping[str, object]:
    return {
        "path": str(artifact.path),
        "exists": artifact.exists,
        "artifact_type": artifact.artifact_type,
        "sha256": artifact.sha256,
        "size_bytes": artifact.size_bytes,
        "file_count": artifact.file_count,
    }


def _command_cell(step: DailyOpsStep) -> str:
    if not step.enabled:
        reason = step.skip_reason or "显式跳过。"
        return f"SKIPPED: {_escape_table(reason)}"
    return f"`{_escape_table(_join_command(step.command))}`"


def _join_command(command: tuple[str, ...]) -> str:
    return " ".join(_quote_command_arg(arg) for arg in command)


def _daily_ops_step_command_with_visibility_cutoff(
    step: DailyOpsStep,
    *,
    plan: DailyOpsPlan,
    visibility_cutoff: datetime,
    latest_completed_trading_day: date | None,
) -> tuple[str, ...]:
    command = step.command
    if (
        step.step_id != "score_daily"
        or not command
        or "--skip-risk-event-openai-precheck" in command
        or "--risk-event-openai-precheck-visibility-cutoff" in command
        or latest_completed_trading_day is None
        or plan.as_of != latest_completed_trading_day
    ):
        return command
    return (
        *command,
        "--risk-event-openai-precheck-visibility-cutoff",
        visibility_cutoff.astimezone(UTC).isoformat(),
    )


def _execution_command(
    command: tuple[str, ...],
    project_root: Path = PROJECT_ROOT,
) -> tuple[str, ...]:
    if command and command[0] == "aits":
        return (
            str(_project_python_executable(project_root)),
            "-m",
            "ai_trading_system.cli_direct",
            *command[1:],
        )
    return command


def _project_python_executable(project_root: Path) -> Path:
    candidates = (
        project_root / ".venv" / "Scripts" / "python.exe",
        project_root / ".venv" / "bin" / "python",
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return Path(sys.executable)


def _purge_source_pycache_dirs(project_root: Path) -> None:
    source_root = (project_root / "src").resolve()
    if not source_root.exists():
        return
    for pycache_dir in source_root.rglob("__pycache__"):
        resolved = pycache_dir.resolve()
        try:
            resolved.relative_to(source_root)
        except ValueError:
            continue
        shutil.rmtree(resolved, ignore_errors=True)


def _write_step_failure_diagnostic(
    *,
    step: DailyOpsStep,
    as_of: date,
    started_at: datetime,
    ended_at: datetime,
    return_code: int | None,
    stdout_text: str,
    stderr_text: str,
    error: str | None,
    env: Mapping[str, str],
    diagnostics_dir: Path,
) -> Path:
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    timestamp = started_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_path = diagnostics_dir / (
        "daily_ops_step_failure_"
        f"{as_of.isoformat()}_{_safe_file_token(step.step_id)}_{timestamp}.md"
    )
    stdout_redacted = _redact_diagnostic_output(stdout_text, env)
    stderr_redacted = _redact_diagnostic_output(stderr_text, env)
    error_redacted = "" if error is None else _redact_diagnostic_output(error, env)
    duration_seconds = (ended_at - started_at).total_seconds()
    lines = [
        "# Daily ops step failure diagnostic",
        "",
        f"- as_of: {as_of.isoformat()}",
        f"- step_id: {step.step_id}",
        f"- title: {step.title}",
        f"- command: `{_join_command(step.command) if step.command else 'PRECHECK'}`",
        f"- return_code: {_display_return_code(return_code)}",
        f"- started_at: {started_at.isoformat()}",
        f"- ended_at: {ended_at.isoformat()}",
        f"- duration_seconds: {duration_seconds:.1f}",
        f"- stdout_line_count: {len(stdout_text.splitlines())}",
        f"- stderr_line_count: {len(stderr_text.splitlines())}",
        "- redaction: env secret values and sensitive URL/header tokens are replaced with `***`",
    ]
    if error_redacted:
        lines.extend(["", "## Error", "", "```text", _fence_safe(error_redacted), "```"])
    lines.extend(
        [
            "",
            "## Stdout redacted",
            "",
            "```text",
            _fence_safe(stdout_redacted),
            "```",
            "",
            "## Stderr redacted",
            "",
            "```text",
            _fence_safe(stderr_redacted),
            "```",
        ]
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def _redact_diagnostic_output(text: str, env: Mapping[str, str]) -> str:
    return sanitize_diagnostic_text(
        text,
        extra_secrets=_diagnostic_secret_values(env),
        max_length=_DIAGNOSTIC_TEXT_MAX_CHARS,
    )


def _diagnostic_secret_values(env: Mapping[str, str]) -> tuple[str, ...]:
    values: list[str] = []
    for name, value in env.items():
        if not value:
            continue
        normalized = name.upper()
        if any(token in normalized for token in _DIAGNOSTIC_ENV_SECRET_TOKENS):
            values.append(value)
    return tuple(dict.fromkeys(values))


def _safe_file_token(value: str) -> str:
    token = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in value)
    return "_".join(part for part in token.split("_") if part) or "step"


def _fence_safe(text: str) -> str:
    return text.replace("```", "` ` `")


def _quote_command_arg(value: str) -> str:
    if not value:
        return "''"
    if any(char.isspace() for char in value):
        return "'" + value.replace("'", "'\"'\"'") + "'"
    return value


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def _daily_run_status(results: list[DailyOpsStepResult]) -> str:
    if any(result.status == "FAIL" for result in results):
        return "FAIL"
    if any(result.status == "SKIPPED" for result in results):
        return "PASS_WITH_SKIPS"
    return "PASS"


def _workflow_status(status: str) -> StepStatus:
    normalized = status.strip().upper()
    if normalized == "PASS":
        return "PASS"
    if normalized == "SKIPPED":
        return "SKIPPED"
    if normalized == "FAIL":
        return "FAIL"
    if normalized.startswith("PASS_WITH") or normalized == "WARN":
        return "WARN"
    if normalized.startswith("BLOCKED"):
        return "BLOCKED"
    return "FAIL"


def _workflow_command_name(command: tuple[str, ...]) -> str:
    if not command:
        return ""
    if len(command) >= 3 and command[0] == "aits":
        return " ".join(command[:3])
    if len(command) >= 2 and command[0] == "aits":
        return " ".join(command[:2])
    return command[0]


def _build_pre_run_input_artifacts(
    plan: DailyOpsPlan,
    project_root: Path,
) -> tuple[DailyOpsArtifactDigest, ...]:
    return tuple(_path_digest(path) for path in _pre_run_input_paths(plan, project_root))


def _pre_run_input_paths(plan: DailyOpsPlan, project_root: Path) -> tuple[Path, ...]:
    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    reports_dir = project_root / "outputs" / "reports"
    external_dir = project_root / "data" / "external"
    as_of = plan.as_of
    paths = [
        raw_dir / "prices_daily.csv",
        raw_dir / "prices_marketstack_daily.csv",
        raw_dir / "rates_daily.csv",
        raw_dir / "download_manifest.csv",
        raw_dir / "pit_snapshots" / "manifest.csv",
        processed_dir / "pit_snapshots" / f"fmp_forward_pit_{as_of.isoformat()}.csv",
        processed_dir / f"sec_fundamentals_{as_of.isoformat()}.csv",
        processed_dir / "features_daily.csv",
        processed_dir / "scores_daily.csv",
        processed_dir / "prediction_ledger.csv",
        processed_dir / "risk_event_prereview_queue.json",
        processed_dir / f"official_policy_source_candidates_{as_of.isoformat()}.csv",
        external_dir / "valuation_snapshots",
        external_dir / "risk_event_occurrences",
        external_dir / "trade_theses",
        external_dir / "trades",
        reports_dir / f"data_quality_{as_of.isoformat()}.md",
        reports_dir / f"fmp_forward_pit_fetch_{as_of.isoformat()}.md",
        reports_dir / f"pit_snapshots_validation_{as_of.isoformat()}.md",
        reports_dir / f"sec_fundamentals_validation_{as_of.isoformat()}.md",
        reports_dir / f"valuation_validation_{as_of.isoformat()}.md",
        reports_dir / f"risk_event_prereview_openai_{as_of.isoformat()}.md",
    ]
    paths.extend(sorted((project_root / "config").glob("*.yaml")))
    return tuple(dict.fromkeys(paths))


def _build_daily_ops_run_metadata(
    *,
    plan: DailyOpsPlan,
    project_root: Path,
    env: Mapping[str, str],
    results: tuple[DailyOpsStepResult, ...],
    started_at: datetime,
    finished_at: datetime,
    status: str,
    pre_run_input_artifacts: tuple[DailyOpsArtifactDigest, ...],
    run_id: str | None = None,
    visibility_issues: tuple[DailyOpsInputVisibilityIssue, ...] = (),
) -> DailyOpsRunMetadata:
    required_env = sorted({env_var for step in plan.steps for env_var in step.required_env_vars})
    produced_paths = tuple(
        dict.fromkeys(
            (
                *(path for step in plan.steps for path in step.produced_paths),
                *(
                    result.diagnostic_path
                    for result in results
                    if result.diagnostic_path is not None
                ),
            )
        )
    )
    config_paths = tuple(sorted((project_root / "config").glob("*.yaml")))
    resolved_run_id = run_id or (
        "daily_ops_run:"
        f"{plan.as_of.isoformat()}:"
        f"{started_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    )
    return DailyOpsRunMetadata(
        schema_version=1,
        run_id=resolved_run_id,
        as_of=plan.as_of,
        generated_at=datetime.now(tz=UTC),
        project_root=project_root,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        visibility_cutoff=finished_at,
        visibility_cutoff_source="daily_run_finished_at_utc",
        input_visibility_status="BLOCKED" if visibility_issues else "PASS",
        input_visibility_issues=tuple(
            _visibility_issue_to_json(issue) for issue in visibility_issues
        ),
        git=_git_metadata(project_root),
        config_artifacts=tuple(_path_digest(path) for path in config_paths),
        rule_card_sha256=_sha256_file(project_root / "config" / "rule_cards.yaml"),
        env_presence={name: bool(env.get(name, "").strip()) for name in required_env},
        commands=tuple(_metadata_command_record(step) for step in plan.steps),
        step_results=tuple(_metadata_step_result(result) for result in results),
        pre_run_input_artifacts=pre_run_input_artifacts,
        produced_artifacts=tuple(_path_digest(path) for path in produced_paths),
    )


def _metadata_command_record(step: DailyOpsStep) -> Mapping[str, object]:
    return {
        "step_id": step.step_id,
        "enabled": step.enabled,
        "command": _join_command(step.command) if step.command else "",
        "required_env_vars": list(step.required_env_vars),
        "blocks_downstream": step.blocks_downstream,
        "skip_reason": step.skip_reason,
        "input_visibility": step.input_visibility,
    }


def _metadata_step_result(result: DailyOpsStepResult) -> Mapping[str, object]:
    return {
        "step_id": result.step_id,
        "status": result.status,
        "return_code": result.return_code,
        "command": _join_command(result.command) if result.command else "",
        "started_at": None if result.started_at is None else result.started_at.isoformat(),
        "ended_at": None if result.ended_at is None else result.ended_at.isoformat(),
        "duration_seconds": result.duration_seconds,
        "stdout_line_count": result.stdout_line_count,
        "stderr_line_count": result.stderr_line_count,
        "error": result.error,
        "diagnostic_path": None if result.diagnostic_path is None else str(result.diagnostic_path),
    }


@dataclass(frozen=True)
class _MarketCacheCoverage:
    required_date: date
    primary_latest: date | None
    secondary_latest: date | None

    @property
    def covers_required_date(self) -> bool:
        return (
            self.primary_latest is not None
            and self.primary_latest >= self.required_date
            and self.secondary_latest is not None
            and self.secondary_latest >= self.required_date
        )

    @property
    def summary(self) -> str:
        primary = self.primary_latest.isoformat() if self.primary_latest else "missing"
        secondary = self.secondary_latest.isoformat() if self.secondary_latest else "missing"
        return f"prices_daily max={primary}, marketstack max={secondary}"


def _market_cache_coverage(*, raw_dir: Path, required_date: date) -> _MarketCacheCoverage:
    return _MarketCacheCoverage(
        required_date=required_date,
        primary_latest=_latest_csv_date(raw_dir / "prices_daily.csv"),
        secondary_latest=_latest_csv_date(raw_dir / "prices_marketstack_daily.csv"),
    )


def _latest_csv_date(path: Path) -> date | None:
    if not path.exists() or path.stat().st_size <= 0:
        return None
    latest: date | None = None
    try:
        with path.open(encoding="utf-8") as handle:
            header = handle.readline().strip().split(",")
            try:
                date_index = header.index("date")
            except ValueError:
                return None
            for line in handle:
                columns = line.strip().split(",")
                if date_index >= len(columns):
                    continue
                raw_date = columns[date_index]
                try:
                    parsed = date.fromisoformat(raw_date)
                except ValueError:
                    continue
                if latest is None or parsed > latest:
                    latest = parsed
    except (OSError, TypeError, UnicodeDecodeError):
        return None
    return latest


def _display_return_code(return_code: int | None) -> str:
    return "" if return_code is None else str(return_code)


def _path_digest(path: Path) -> DailyOpsArtifactDigest:
    if not path.exists():
        return DailyOpsArtifactDigest(
            path=path,
            exists=False,
            artifact_type="missing",
            sha256=None,
            size_bytes=None,
            file_count=None,
        )
    if path.is_file():
        return DailyOpsArtifactDigest(
            path=path,
            exists=True,
            artifact_type="file",
            sha256=_sha256_file(path),
            size_bytes=_safe_file_size(path),
            file_count=1,
        )
    if path.is_dir():
        size_bytes, file_count = _directory_size_and_count(path)
        return DailyOpsArtifactDigest(
            path=path,
            exists=True,
            artifact_type="directory",
            sha256=_directory_digest(path),
            size_bytes=size_bytes,
            file_count=file_count,
        )
    return DailyOpsArtifactDigest(
        path=path,
        exists=True,
        artifact_type="other",
        sha256=None,
        size_bytes=None,
        file_count=None,
    )


def _sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    return digest.hexdigest()


def _directory_digest(path: Path) -> str | None:
    if not path.exists() or not path.is_dir():
        return None
    digest = hashlib.sha256()
    try:
        files = sorted(item for item in path.rglob("*") if item.is_file())
    except OSError:
        return None
    for file_path in files:
        digest.update(str(file_path.relative_to(path)).encode("utf-8"))
        file_digest = _sha256_file(file_path)
        if file_digest is not None:
            digest.update(file_digest.encode("ascii"))
    return digest.hexdigest()


def _directory_size_and_count(path: Path) -> tuple[int, int]:
    total_size = 0
    file_count = 0
    try:
        files = (item for item in path.rglob("*") if item.is_file())
        for file_path in files:
            try:
                total_size += file_path.stat().st_size
            except OSError:
                continue
            file_count += 1
    except OSError:
        return 0, 0
    return total_size, file_count


def _safe_file_size(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        return None


def _git_metadata(project_root: Path) -> Mapping[str, object]:
    commit = _git_output(project_root, ("rev-parse", "HEAD"))
    status_text = _git_output(project_root, ("status", "--short"))
    unstaged_diff = _git_output(
        project_root,
        ("diff", "--no-ext-diff", "--binary"),
    )
    staged_diff = _git_output(
        project_root,
        ("diff", "--cached", "--no-ext-diff", "--binary"),
    )
    diff_payload = (unstaged_diff or "") + "\n" + (staged_diff or "")
    return {
        "available": commit is not None,
        "commit": commit,
        "dirty": bool(status_text),
        "status_sha256": _text_sha256(status_text or ""),
        "dirty_diff_sha256": _text_sha256(diff_payload),
    }


def _git_output(project_root: Path, args: tuple[str, ...]) -> str | None:
    try:
        completed = subprocess.run(
            ("git", *args),
            cwd=project_root,
            text=True,
            capture_output=True,
            check=False,
            timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip()


def _text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _post_step_artifact_status_error(step: DailyOpsStep) -> str | None:
    status_report_indexes = {
        "official_policy_sources": (2,),
        "validate_data": (0,),
        "pit_snapshots_fetch_fmp_forward": (2,),
        "pit_snapshots_build_manifest": (1,),
        "pit_snapshots_validate": (0,),
        "sec_metrics": (0, 2),
        "sec_metrics_validation": (0,),
        "valuation_snapshots": (2, 3),
        "score_daily": (2, 4),
        "pipeline_health": (0,),
        "secret_hygiene": (0,),
    }.get(step.step_id, ())
    for index in status_report_indexes:
        if index >= len(step.produced_paths):
            return f"artifact_status_path_missing: {step.step_id}[{index}]"
        path = step.produced_paths[index]
        status = _read_markdown_status(path)
        if status is None:
            return f"artifact_status_missing: {path}"
        if not status.startswith("PASS"):
            return f"artifact_status_failed: {path} status={status}"
    return None


def _read_markdown_status(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if stripped.startswith("- 状态："):
                    return stripped.removeprefix("- 状态：").strip()
                if stripped.startswith("- 状态:"):
                    return stripped.removeprefix("- 状态:").strip()
    except OSError:
        return None
    return None
