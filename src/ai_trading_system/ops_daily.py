from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from ai_trading_system.alerts import (
    default_alert_report_path,
    default_pipeline_health_alert_report_path,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import default_quality_report_path
from ai_trading_system.features.market import default_feature_report_path
from ai_trading_system.fmp_forward_pit import (
    DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
    DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
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
from ai_trading_system.pipeline_health import default_pipeline_health_report_path
from ai_trading_system.pit_snapshots import (
    DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
    default_pit_snapshot_validation_report_path,
)
from ai_trading_system.scoring.daily import default_daily_score_report_path
from ai_trading_system.secret_hygiene import default_secret_scan_report_path
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

    def missing_env_vars(self, env: Mapping[str, str]) -> tuple[str, ...]:
        if not self.enabled:
            return ()
        return tuple(
            env_var
            for env_var in self.required_env_vars
            if not env.get(env_var, "").strip()
        )


@dataclass(frozen=True)
class DailyOpsPlan:
    as_of: date
    generated_at: datetime
    steps: tuple[DailyOpsStep, ...]
    production_effect: str = "none"

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
            env_var
            for values in self.missing_env_by_step(env).values()
            for env_var in values
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


@dataclass(frozen=True)
class DailyOpsRunReport:
    plan: DailyOpsPlan
    started_at: datetime
    finished_at: datetime
    status: str
    step_results: tuple[DailyOpsStepResult, ...]
    missing_env_vars: tuple[str, ...] = ()
    production_effect: str = "writes local data/cache/reports and calls provider APIs"

    @property
    def failed_step(self) -> DailyOpsStepResult | None:
        return next(
            (result for result in self.step_results if result.status == "FAIL"),
            None,
        )


DailyOpsCommandRunner = subprocess.run


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
    risk_event_openai_precheck_max_candidates: int = 20,
) -> DailyOpsPlan:
    if download_start > as_of:
        raise ValueError("download_start must not be later than as_of")
    if risk_event_openai_precheck_max_candidates < 0:
        raise ValueError("risk_event_openai_precheck_max_candidates must be non-negative")

    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    reports_dir = project_root / "outputs" / "reports"
    as_of_text = as_of.isoformat()

    download_command = [
        "aits",
        "download-data",
        "--start",
        download_start.isoformat(),
        "--end",
        as_of_text,
    ]
    if full_universe:
        download_command.append("--full-universe")

    pit_fetch_report = default_fmp_forward_pit_fetch_report_path(reports_dir, as_of)
    pit_normalized = default_fmp_forward_pit_normalized_path(
        DEFAULT_FMP_FORWARD_PIT_NORMALIZED_DIR,
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
        score_command.extend(
            [
                "--risk-event-openai-precheck-max-candidates",
                str(risk_event_openai_precheck_max_candidates),
            ]
        )
        score_required_env = ("OPENAI_API_KEY",)

    steps = [
        DailyOpsStep(
            step_id="download_data",
            title="更新市场和宏观缓存",
            command=tuple(download_command) if include_download_data else (),
            required_env_vars=(
                ("FMP_API_KEY", "MARKETSTACK_API_KEY") if include_download_data else ()
            ),
            produced_paths=(
                raw_dir / "prices_daily.csv",
                raw_dir / "prices_marketstack_daily.csv",
                raw_dir / "rates_daily.csv",
                raw_dir / "download_manifest.csv",
            ),
            quality_gate=(
                "下载审计 manifest 记录 provider、endpoint、请求参数、"
                "row count 和 checksum。"
            ),
            blocks_downstream=True,
            enabled=include_download_data,
            skip_reason=None if include_download_data else "显式跳过数据下载，只复用已有缓存。",
        ),
        DailyOpsStep(
            step_id="pit_snapshots",
            title="抓取并校验 forward-only PIT 快照",
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
                DEFAULT_FMP_FORWARD_PIT_RAW_DIR,
                pit_normalized,
                DEFAULT_PIT_SNAPSHOT_MANIFEST_PATH,
                pit_fetch_report,
                pit_validation_report,
            ),
            quality_gate=(
                "命令读取 FMP_API_KEY 并刷新 PIT manifest；失败会写入脱敏报告或 "
                "pipeline health 告警，后续 score-daily 仍执行自身质量门禁。"
            ),
            blocks_downstream=False,
            enabled=include_pit_snapshots,
            skip_reason=(
                None
                if include_pit_snapshots
                else "显式跳过 PIT 抓取；缺跑日期不能事后补成 strict PIT。"
            ),
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
        ),
        DailyOpsStep(
            step_id="score_daily",
            title="生成每日评分、日报、trace 和告警",
            command=tuple(score_command),
            required_env_vars=score_required_env,
            produced_paths=(
                processed_dir / "features_daily.csv",
                processed_dir / "scores_daily.csv",
                default_quality_report_path(reports_dir, as_of),
                default_feature_report_path(reports_dir, as_of),
                default_daily_score_report_path(reports_dir, as_of),
                default_alert_report_path(reports_dir, as_of),
            ),
            quality_gate=(
                "`score-daily` 内部先运行市场数据质量门禁，并校验 SEC metrics、"
                "估值快照、风险事件发生记录和 rule card；失败时停止。"
            ),
            blocks_downstream=True,
        ),
        DailyOpsStep(
            step_id="pipeline_health",
            title="检查关键 artifact 和 PIT 健康",
            command=("aits", "ops", "health", "--as-of", as_of_text),
            required_env_vars=(),
            produced_paths=(
                default_pipeline_health_report_path(reports_dir, as_of),
                default_pipeline_health_alert_report_path(reports_dir, as_of),
            ),
            quality_gate="只读运行健康检查；不把 pipeline health 解释为投资结论有效。",
            blocks_downstream=False,
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
        ),
    ]
    return DailyOpsPlan(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        steps=tuple(steps),
    )


def run_daily_ops_plan(
    plan: DailyOpsPlan,
    *,
    project_root: Path = PROJECT_ROOT,
    env: Mapping[str, str] | None = None,
    runner=DailyOpsCommandRunner,
    stop_on_failure: bool = True,
) -> DailyOpsRunReport:
    checked_env = dict(os.environ if env is None else env)
    started_at = datetime.now(tz=UTC)
    missing_env = plan.missing_env_vars(checked_env)
    if missing_env:
        finished_at = datetime.now(tz=UTC)
        return DailyOpsRunReport(
            plan=plan,
            started_at=started_at,
            finished_at=finished_at,
            status="BLOCKED_ENV",
            step_results=(),
            missing_env_vars=missing_env,
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

        step_started = datetime.now(tz=UTC)
        try:
            completed = runner(
                _execution_command(step.command),
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
            artifact_error = (
                _post_step_artifact_status_error(step) if return_code == 0 else None
            )
            result = DailyOpsStepResult(
                step_id=step.step_id,
                title=step.title,
                command=step.command,
                status="PASS" if return_code == 0 and artifact_error is None else "FAIL",
                return_code=return_code,
                started_at=step_started,
                ended_at=step_ended,
                duration_seconds=(step_ended - step_started).total_seconds(),
                produced_paths=step.produced_paths,
                blocks_downstream=step.blocks_downstream,
                stdout_line_count=len(stdout_text.splitlines()),
                stderr_line_count=len(stderr_text.splitlines()),
                error=artifact_error,
            )
        except OSError as exc:
            step_ended = datetime.now(tz=UTC)
            result = DailyOpsStepResult(
                step_id=step.step_id,
                title=step.title,
                command=step.command,
                status="FAIL",
                return_code=None,
                started_at=step_started,
                ended_at=step_ended,
                duration_seconds=(step_ended - step_started).total_seconds(),
                produced_paths=step.produced_paths,
                blocks_downstream=step.blocks_downstream,
                error=f"{type(exc).__name__}: {exc}",
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
    )


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
            "| 顺序 | Step | Enabled | Command | Required Env | Missing Env | "
            "Gate / 边界 | Outputs | Blocks Downstream |",
            "|---:|---|---|---|---|---|---|---|---|",
        ]
    )
    for index, step in enumerate(plan.steps, start=1):
        required_env = ", ".join(f"`{item}`" for item in step.required_env_vars) or ""
        step_missing = ", ".join(
            f"`{item}`" for item in missing_by_step.get(step.step_id, ())
        )
        command = _command_cell(step)
        outputs = "<br/>".join(f"`{path}`" for path in step.produced_paths)
        lines.append(
            "| "
            f"{index} | "
            f"`{step.step_id}`<br/>{_escape_table(step.title)} | "
            f"{step.enabled} | "
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


def render_daily_ops_run_report(report: DailyOpsRunReport) -> str:
    duration = (report.finished_at - report.started_at).total_seconds()
    lines = [
        "# 每日运行执行报告",
        "",
        f"- 状态：{report.status}",
        f"- 评估日期：{report.plan.as_of.isoformat()}",
        f"- 开始时间：{report.started_at.isoformat()}",
        f"- 结束时间：{report.finished_at.isoformat()}",
        f"- 总耗时秒：{duration:.1f}",
        f"- 生产影响：{report.production_effect}",
        "",
        "## 方法边界",
        "",
        "- 本报告由真实每日执行器生成，会按每日运行计划顺序调用本地 CLI。",
        "- 执行器内部用当前 Python 解释器调用 `ai_trading_system.cli` 模块，",
        "避免在 Windows 上从 `aits.exe` 父进程递归启动 `aits.exe`。",
        "- 执行报告只记录命令状态、退出码、耗时和预期 artifact 路径；",
        "不写入 stdout/stderr 原文、API key、token 或付费内容原文。",
        "- 投资结论仍以 `score-daily`、数据质量报告、SEC/估值校验、",
        "风险事件校验、rule card 和告警报告为准。",
        "",
    ]
    if report.missing_env_vars:
        lines.extend(
            [
                "## 环境变量阻断",
                "",
                "- 缺失环境变量："
                + ", ".join(f"`{item}`" for item in report.missing_env_vars),
                "",
            ]
        )
    failed = report.failed_step
    if failed is not None:
        lines.extend(
            [
                "## 阻断步骤",
                "",
                f"- Step：`{failed.step_id}` {failed.title}",
                f"- Return code：{_display_return_code(failed.return_code)}",
                f"- Command：`{_escape_table(_join_command(failed.command))}`",
            ]
        )
        if failed.error:
            lines.append(f"- Error：`{_escape_table(failed.error)}`")
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
            f"{result.duration_seconds:.1f}"
            if result.duration_seconds is not None
            else ""
        )
        outputs = "<br/>".join(f"`{path}`" for path in result.produced_paths)
        command = (
            f"`{_escape_table(_join_command(result.command))}`"
            if result.command
            else "SKIPPED"
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
            "- 报告中的 `Stdout Lines` / `Stderr Lines` 只记录行数，不保存原文。",
        ]
    )
    return "\n".join(lines) + "\n"


def write_daily_ops_run_report(
    report: DailyOpsRunReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_daily_ops_run_report(report), encoding="utf-8")
    return output_path


def default_daily_ops_plan_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_ops_plan_{as_of.isoformat()}.md"


def default_daily_ops_run_report_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_ops_run_{as_of.isoformat()}.md"


def _command_cell(step: DailyOpsStep) -> str:
    if not step.enabled:
        reason = step.skip_reason or "显式跳过。"
        return f"SKIPPED: {_escape_table(reason)}"
    return f"`{_escape_table(_join_command(step.command))}`"


def _join_command(command: tuple[str, ...]) -> str:
    return " ".join(_quote_command_arg(arg) for arg in command)


def _execution_command(command: tuple[str, ...]) -> tuple[str, ...]:
    if command and command[0] == "aits":
        return (sys.executable, "-m", "ai_trading_system.cli", *command[1:])
    return command


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


def _display_return_code(return_code: int | None) -> str:
    return "" if return_code is None else str(return_code)


def _post_step_artifact_status_error(step: DailyOpsStep) -> str | None:
    status_report_indexes = {
        "pit_snapshots": (3, 4),
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
