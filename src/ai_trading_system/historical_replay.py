from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.alerts import (
    default_alert_report_path,
    default_pipeline_health_alert_report_path,
)
from ai_trading_system.belief_state import (
    DEFAULT_BELIEF_STATE_HISTORY_PATH,
    default_belief_state_path,
)
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.quality import default_quality_report_path
from ai_trading_system.decision_snapshots import (
    DEFAULT_DECISION_SNAPSHOT_DIR,
    default_decision_snapshot_path,
)
from ai_trading_system.evidence_dashboard import (
    default_evidence_dashboard_json_path,
    default_evidence_dashboard_path,
)
from ai_trading_system.execution_policy import default_execution_policy_report_path
from ai_trading_system.feature_availability import (
    default_feature_availability_report_path,
)
from ai_trading_system.features.market import default_feature_report_path
from ai_trading_system.fmp_forward_pit import (
    FmpForwardPitRawPayload,
    default_fmp_forward_pit_fetch_report_path,
    default_fmp_forward_pit_normalized_path,
    normalize_fmp_forward_pit_payloads,
    write_fmp_forward_pit_normalized_csv,
)
from ai_trading_system.fundamentals.sec_features import (
    default_sec_fundamental_features_csv_path,
    default_sec_fundamental_features_report_path,
)
from ai_trading_system.fundamentals.sec_metrics import (
    default_sec_fundamental_metrics_csv_path,
    default_sec_fundamental_metrics_validation_report_path,
)
from ai_trading_system.historical_inputs import risk_event_occurrence_store_as_of
from ai_trading_system.ops_daily import _join_command, default_daily_ops_run_metadata_path
from ai_trading_system.pipeline_health import default_pipeline_health_report_path
from ai_trading_system.pit_snapshots import default_pit_snapshot_validation_report_path
from ai_trading_system.portfolio_exposure import default_portfolio_exposure_report_path
from ai_trading_system.prediction_ledger import DEFAULT_PREDICTION_LEDGER_PATH
from ai_trading_system.report_traceability import default_report_trace_bundle_path
from ai_trading_system.risk_event_prereview import (
    default_risk_event_openai_prereview_report_path,
)
from ai_trading_system.risk_events import (
    RiskEventOccurrenceStore,
    default_risk_event_occurrence_report_path,
    load_risk_event_occurrence_store,
)
from ai_trading_system.scoring.daily import default_daily_score_report_path
from ai_trading_system.secret_hygiene import default_secret_scan_report_path
from ai_trading_system.trading_calendar import us_equity_market_session
from ai_trading_system.valuation import default_valuation_validation_report_path
from ai_trading_system.valuation_sources import default_fmp_valuation_fetch_report_path

ReplayCommandRunner = subprocess.run
RISK_EVENT_PREREVIEW_QUEUE_NAME = "risk_event_prereview_queue.json"
OPENAI_REPLAY_POLICIES = {"disabled", "cache-only"}


@dataclass(frozen=True)
class ReplayPaths:
    run_id: str
    root: Path
    input_root: Path
    output_root: Path
    data_raw_dir: Path
    data_processed_dir: Path
    reports_dir: Path
    logs_dir: Path
    input_manifest_csv_path: Path
    input_manifest_json_path: Path
    run_report_path: Path
    run_json_path: Path


@dataclass(frozen=True)
class ReplayInputRecord:
    artifact_id: str
    artifact_class: str
    source_path: str
    replay_path: str
    status: str
    row_count: int | None = None
    included_count: int | None = None
    excluded_count: int | None = None
    sha256: str | None = None
    min_timestamp: str | None = None
    max_timestamp: str | None = None
    reason: str = ""


@dataclass(frozen=True)
class ReplayCommandResult:
    step_id: str
    title: str
    command: tuple[str, ...]
    status: str
    return_code: int | None
    started_at: datetime | None
    ended_at: datetime | None
    duration_seconds: float | None
    stdout_line_count: int = 0
    stderr_line_count: int = 0
    error: str | None = None


@dataclass(frozen=True)
class ReplayDiffArtifact:
    artifact_id: str
    artifact_class: str
    production_path: str
    replay_path: str
    status: str
    production_sha256: str | None = None
    replay_sha256: str | None = None
    production_row_count: int | None = None
    replay_row_count: int | None = None
    details: str = ""


@dataclass(frozen=True)
class ReplayProductionDiff:
    as_of: date
    generated_at: datetime
    status: str
    report_path: Path
    json_path: Path
    artifacts: tuple[ReplayDiffArtifact, ...]


@dataclass(frozen=True)
class HistoricalReplayRun:
    as_of: date
    mode: str
    run_id: str
    generated_at: datetime
    visible_at: datetime
    cutoff_policy: str
    paths: ReplayPaths
    input_records: tuple[ReplayInputRecord, ...]
    command_results: tuple[ReplayCommandResult, ...]
    errors: tuple[str, ...] = ()
    inventory_only: bool = False
    allow_incomplete: bool = False
    label: str | None = None
    openai_replay_policy: str = "disabled"
    production_diff: ReplayProductionDiff | None = None

    @property
    def failed_step(self) -> ReplayCommandResult | None:
        return next(
            (result for result in self.command_results if result.status == "FAIL"),
            None,
        )

    @property
    def status(self) -> str:
        if self.errors:
            return "INCOMPLETE_REPLAY" if self.allow_incomplete else "FAIL"
        if self.failed_step is not None:
            return "FAIL"
        if self.inventory_only:
            return "PASS_INVENTORY"
        return "PASS"


@dataclass(frozen=True)
class ReplayWindowSkippedDate:
    as_of: date
    reason: str


@dataclass(frozen=True)
class HistoricalReplayWindowRun:
    start: date
    end: date
    mode: str
    run_id: str
    generated_at: datetime
    report_path: Path
    json_path: Path
    day_runs: tuple[HistoricalReplayRun, ...]
    skipped_dates: tuple[ReplayWindowSkippedDate, ...]
    continue_on_failure: bool = False
    label: str | None = None

    @property
    def failed_run(self) -> HistoricalReplayRun | None:
        return next(
            (
                replay_run
                for replay_run in self.day_runs
                if replay_run.status not in {"PASS", "PASS_INVENTORY"}
            ),
            None,
        )

    @property
    def status(self) -> str:
        if self.failed_run is not None:
            return "FAIL"
        if self.skipped_dates:
            return "PASS_WITH_SKIPS"
        return "PASS"


def default_historical_replay_output_root(project_root: Path = PROJECT_ROOT) -> Path:
    return project_root / "outputs" / "replays"


def run_historical_day_replay(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    output_root: Path | None = None,
    mode: str = "cache-only",
    visible_at: datetime | None = None,
    label: str | None = None,
    run_id: str | None = None,
    inventory_only: bool = False,
    allow_incomplete: bool = False,
    compare_to_production: bool = False,
    openai_replay_policy: str = "disabled",
    full_universe: bool = False,
    env: Mapping[str, str] | None = None,
    runner: Any = ReplayCommandRunner,
) -> HistoricalReplayRun:
    if mode != "cache-only":
        raise ValueError("historical replay MVP only supports mode=cache-only")
    if openai_replay_policy not in OPENAI_REPLAY_POLICIES:
        raise ValueError(
            "openai replay policy must be one of: "
            + ", ".join(sorted(OPENAI_REPLAY_POLICIES))
        )

    generated_at = datetime.now(tz=UTC)
    replay_visible_at, cutoff_policy = _resolve_replay_visible_at(
        as_of=as_of,
        project_root=project_root,
        visible_at=visible_at,
    )
    replay_paths = _build_replay_paths(
        as_of=as_of,
        output_root=output_root or default_historical_replay_output_root(project_root),
        generated_at=generated_at,
        label=label,
        run_id=run_id,
    )
    _create_replay_dirs(replay_paths)

    input_records, preparation_errors = _prepare_replay_inputs(
        as_of=as_of,
        visible_at=replay_visible_at,
        project_root=project_root,
        paths=replay_paths,
        openai_replay_policy=openai_replay_policy,
    )
    _write_input_manifest(input_records, replay_paths)

    command_results: tuple[ReplayCommandResult, ...] = ()
    if not inventory_only and not (preparation_errors and not allow_incomplete):
        command_results = _run_replay_commands(
            as_of=as_of,
            project_root=project_root,
            paths=replay_paths,
            full_universe=full_universe,
            env=_cache_only_env(env),
            runner=runner,
        )

    production_diff = (
        build_replay_production_diff(
            as_of=as_of,
            project_root=project_root,
            paths=replay_paths,
        )
        if compare_to_production
        else None
    )
    if production_diff is not None:
        write_replay_production_diff(production_diff)

    replay_run = HistoricalReplayRun(
        as_of=as_of,
        mode=mode,
        run_id=replay_paths.run_id,
        generated_at=generated_at,
        visible_at=replay_visible_at,
        cutoff_policy=cutoff_policy,
        paths=replay_paths,
        input_records=tuple(input_records),
        command_results=command_results,
        errors=tuple(preparation_errors),
        inventory_only=inventory_only,
        allow_incomplete=allow_incomplete,
        label=label,
        openai_replay_policy=openai_replay_policy,
        production_diff=production_diff,
    )
    write_historical_replay_run(replay_run)
    return replay_run


def run_historical_replay_window(
    *,
    start: date,
    end: date,
    project_root: Path = PROJECT_ROOT,
    output_root: Path | None = None,
    mode: str = "cache-only",
    label: str | None = None,
    run_id: str | None = None,
    inventory_only: bool = False,
    allow_incomplete: bool = False,
    compare_to_production: bool = False,
    openai_replay_policy: str = "disabled",
    full_universe: bool = False,
    continue_on_failure: bool = False,
    env: Mapping[str, str] | None = None,
    runner: Any = ReplayCommandRunner,
) -> HistoricalReplayWindowRun:
    if mode != "cache-only":
        raise ValueError("historical replay window only supports mode=cache-only")
    if start > end:
        raise ValueError("replay window start must be on or before end")
    if openai_replay_policy not in OPENAI_REPLAY_POLICIES:
        raise ValueError(
            "openai replay policy must be one of: "
            + ", ".join(sorted(OPENAI_REPLAY_POLICIES))
        )

    generated_at = datetime.now(tz=UTC)
    resolved_output_root = output_root or default_historical_replay_output_root(project_root)
    resolved_run_id = run_id or _default_window_run_id(start, end, generated_at, label)
    window_root = resolved_output_root / "windows" / resolved_run_id
    report_path = window_root / "replay_window.md"
    json_path = window_root / "replay_window.json"
    window_root.mkdir(parents=True, exist_ok=True)

    day_runs: list[HistoricalReplayRun] = []
    skipped_dates: list[ReplayWindowSkippedDate] = []
    current = start
    while current <= end:
        session = us_equity_market_session(current)
        if not session.is_trading_day:
            skipped_dates.append(
                ReplayWindowSkippedDate(as_of=current, reason=session.reason)
            )
            current += timedelta(days=1)
            continue
        day_run = run_historical_day_replay(
            as_of=current,
            project_root=project_root,
            output_root=resolved_output_root,
            mode=mode,
            label=label,
            run_id=f"{resolved_run_id}_{current.strftime('%Y%m%d')}",
            inventory_only=inventory_only,
            allow_incomplete=allow_incomplete,
            compare_to_production=compare_to_production,
            openai_replay_policy=openai_replay_policy,
            full_universe=full_universe,
            env=env,
            runner=runner,
        )
        day_runs.append(day_run)
        if day_run.status not in {"PASS", "PASS_INVENTORY"} and not continue_on_failure:
            break
        current += timedelta(days=1)

    window_run = HistoricalReplayWindowRun(
        start=start,
        end=end,
        mode=mode,
        run_id=resolved_run_id,
        generated_at=generated_at,
        report_path=report_path,
        json_path=json_path,
        day_runs=tuple(day_runs),
        skipped_dates=tuple(skipped_dates),
        continue_on_failure=continue_on_failure,
        label=label,
    )
    write_historical_replay_window_run(window_run)
    return window_run


def write_historical_replay_run(replay_run: HistoricalReplayRun) -> Path:
    replay_run.paths.run_report_path.write_text(
        render_historical_replay_run(replay_run),
        encoding="utf-8",
    )
    replay_run.paths.run_json_path.write_text(
        json.dumps(_replay_run_to_json(replay_run), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return replay_run.paths.run_report_path


def write_historical_replay_window_run(window_run: HistoricalReplayWindowRun) -> Path:
    window_run.report_path.write_text(
        render_historical_replay_window_run(window_run),
        encoding="utf-8",
    )
    window_run.json_path.write_text(
        json.dumps(_replay_window_to_json(window_run), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return window_run.report_path


def write_replay_production_diff(diff: ReplayProductionDiff) -> Path:
    diff.report_path.write_text(
        render_replay_production_diff(diff),
        encoding="utf-8",
    )
    diff.json_path.write_text(
        json.dumps(_production_diff_to_json(diff), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return diff.report_path


def render_historical_replay_run(replay_run: HistoricalReplayRun) -> str:
    paths = replay_run.paths
    lines = [
        "# 历史交易日归档回放报告",
        "",
        f"- 状态：{replay_run.status}",
        f"- 评估日期：{replay_run.as_of.isoformat()}",
        f"- Run ID：`{replay_run.run_id}`",
        f"- 模式：{replay_run.mode}",
        f"- 标签：{replay_run.label or ''}",
        f"- 可见时间上限：{replay_run.visible_at.isoformat()}",
        f"- cutoff policy：{replay_run.cutoff_policy}",
        f"- OpenAI replay policy：{replay_run.openai_replay_policy}",
        f"- 生成时间：{replay_run.generated_at.isoformat()}",
        f"- Replay bundle：`{paths.root}`",
        "",
        "## 方法边界",
        "",
        "- 本回放默认 cache-only，不调用 live vendor、OpenAI 或其他外部服务。",
        "- OpenAI replay policy 只允许 `disabled` 或 `cache-only`；`cache-only` 只复用"
        "可证明不晚于 effective replay cutoff 的历史预审记录，不发起 live OpenAI 请求。",
        "- 本回放产物写入隔离 replay bundle，不应改写生产 daily_score、decision snapshot、"
        "prediction ledger、PIT manifest 或 valuation snapshots。",
        "- 回放结果用于模型调优、规则回归和事故复盘；除非另有 production replay 元数据，"
        "不得解释为原始生产运行的逐字复现。",
        "",
    ]
    if replay_run.errors:
        lines.extend(["## 输入阻断", ""])
        lines.extend(f"- {error}" for error in replay_run.errors)
        lines.append("")
    failed = replay_run.failed_step
    if failed is not None:
        lines.extend(
            [
                "## 阻断步骤",
                "",
                f"- Step：`{failed.step_id}` {failed.title}",
                f"- Return code：{_display_return_code(failed.return_code)}",
                f"- Command：`{_join_command(failed.command)}`",
            ]
        )
        if failed.error:
            lines.append(f"- Error：`{failed.error}`")
        lines.append("")
    lines.extend(
        [
            "## 输入冻结清单摘要",
            "",
            "| Artifact | Class | Status | Rows | Included | Excluded | Source | "
            "Replay Path | Reason |",
            "|---|---|---|---:|---:|---:|---|---|---|",
        ]
    )
    for record in replay_run.input_records:
        lines.append(
            "| "
            f"`{_escape_table(record.artifact_id)}` | "
            f"{_escape_table(record.artifact_class)} | "
            f"{record.status} | "
            f"{_display_int(record.row_count)} | "
            f"{_display_int(record.included_count)} | "
            f"{_display_int(record.excluded_count)} | "
            f"`{_escape_table(record.source_path)}` | "
            f"`{_escape_table(record.replay_path)}` | "
            f"{_escape_table(record.reason)} |"
        )
    lines.extend(
        [
            "",
            "## 子命令结果",
            "",
            "| Step | Status | Return Code | Duration Seconds | Stdout Lines | "
            "Stderr Lines | Command |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    if not replay_run.command_results:
        lines.append("|  |  |  |  |  |  |  |")
    for result in replay_run.command_results:
        duration_text = (
            f"{result.duration_seconds:.1f}" if result.duration_seconds is not None else ""
        )
        lines.append(
            "| "
            f"`{result.step_id}` | "
            f"{result.status} | "
            f"{_display_return_code(result.return_code)} | "
            f"{duration_text} | "
            f"{result.stdout_line_count} | "
            f"{result.stderr_line_count} | "
            f"`{_escape_table(_join_command(result.command))}` |"
        )
    if replay_run.production_diff is not None:
        diff = replay_run.production_diff
        lines.extend(
            [
                "",
                "## Production Diff",
                "",
                f"- 状态：{diff.status}",
                f"- Diff report：`{diff.report_path}`",
                f"- Diff JSON：`{diff.json_path}`",
            ]
        )
    lines.extend(
        [
            "",
            "## 关键输出",
            "",
            f"- Input freeze manifest CSV：`{paths.input_manifest_csv_path}`",
            f"- Input freeze manifest JSON：`{paths.input_manifest_json_path}`",
            f"- Daily score：`{_daily_score_report_path(paths, replay_run.as_of)}`",
            f"- Alerts：`{_alert_report_path(paths, replay_run.as_of)}`",
            f"- Dashboard：`{_dashboard_html_path(paths, replay_run.as_of)}`",
            f"- Dashboard JSON：`{_dashboard_json_path(paths, replay_run.as_of)}`",
            f"- Evidence bundle：`{_trace_bundle_path(paths, replay_run.as_of)}`",
            f"- Decision snapshot：`{_decision_snapshot_path(paths, replay_run.as_of)}`",
            f"- Pipeline health：`{_pipeline_health_report_path(paths, replay_run.as_of)}`",
            f"- Secret hygiene：`{_secret_report_path(paths, replay_run.as_of)}`",
            f"- Replay JSON：`{paths.run_json_path}`",
        ]
    )
    if replay_run.production_diff is not None:
        lines.extend(
            [
                f"- Diff vs production：`{replay_run.production_diff.report_path}`",
                f"- Diff vs production JSON：`{replay_run.production_diff.json_path}`",
            ]
        )
    return "\n".join(lines) + "\n"


def render_historical_replay_window_run(window_run: HistoricalReplayWindowRun) -> str:
    lines = [
        "# 历史交易日批量回放报告",
        "",
        f"- 状态：{window_run.status}",
        f"- 起始日期：{window_run.start.isoformat()}",
        f"- 结束日期：{window_run.end.isoformat()}",
        f"- Run ID：`{window_run.run_id}`",
        f"- 模式：{window_run.mode}",
        f"- 标签：{window_run.label or ''}",
        f"- 生成时间：{window_run.generated_at.isoformat()}",
        f"- 交易日回放数：{len(window_run.day_runs)}",
        f"- 跳过非交易日数：{len(window_run.skipped_dates)}",
        f"- 失败后继续：{window_run.continue_on_failure}",
        "",
        "## 方法边界",
        "",
        "- 本窗口报告只编排 `cache-only` 单日 replay，不调用 live vendor 或 OpenAI。",
        "- 周末和 NYSE 常规整日休市日默认跳过，并在本报告中记录原因。",
        "- 每个交易日仍输出独立 replay bundle；窗口报告只做索引和结构化汇总。",
        "",
        "## 单日回放结果",
        "",
        "| Date | Status | Run ID | Diff | Bundle | Report |",
        "|---|---|---|---|---|---|",
    ]
    if not window_run.day_runs:
        lines.append("|  |  |  |  |  |  |")
    for replay_run in window_run.day_runs:
        diff_status = (
            "" if replay_run.production_diff is None else replay_run.production_diff.status
        )
        lines.append(
            "| "
            f"{replay_run.as_of.isoformat()} | "
            f"{replay_run.status} | "
            f"`{_escape_table(replay_run.run_id)}` | "
            f"{diff_status} | "
            f"`{_escape_table(str(replay_run.paths.root))}` | "
            f"`{_escape_table(str(replay_run.paths.run_report_path))}` |"
        )
    lines.extend(
        [
            "",
            "## 跳过日期",
            "",
            "| Date | Reason |",
            "|---|---|",
        ]
    )
    if not window_run.skipped_dates:
        lines.append("|  |  |")
    for skipped in window_run.skipped_dates:
        lines.append(
            f"| {skipped.as_of.isoformat()} | {_escape_table(skipped.reason)} |"
        )
    return "\n".join(lines) + "\n"


def render_replay_production_diff(diff: ReplayProductionDiff) -> str:
    lines = [
        "# Replay 与 Production 产物差异报告",
        "",
        f"- 状态：{diff.status}",
        f"- 评估日期：{diff.as_of.isoformat()}",
        f"- 生成时间：{diff.generated_at.isoformat()}",
        "",
        "## 方法边界",
        "",
        "- 本报告只比较本地 production artifact 与 replay artifact 的路径、checksum 和当日行摘要。",
        "- 差异不自动代表 replay 错误；candidate replay 使用当前代码和当前规则，"
        "可能与历史 production 运行不同。",
        "",
        "## Artifact Diff",
        "",
        "| Artifact | Class | Status | Production Rows | Replay Rows | Production SHA256 | "
        "Replay SHA256 | Production Path | Replay Path | Details |",
        "|---|---|---|---:|---:|---|---|---|---|---|",
    ]
    if not diff.artifacts:
        lines.append("|  |  |  |  |  |  |  |  |  |  |")
    for artifact in diff.artifacts:
        lines.append(
            "| "
            f"`{_escape_table(artifact.artifact_id)}` | "
            f"{_escape_table(artifact.artifact_class)} | "
            f"{artifact.status} | "
            f"{_display_int(artifact.production_row_count)} | "
            f"{_display_int(artifact.replay_row_count)} | "
            f"`{_escape_table(_short_digest(artifact.production_sha256))}` | "
            f"`{_escape_table(_short_digest(artifact.replay_sha256))}` | "
            f"`{_escape_table(artifact.production_path)}` | "
            f"`{_escape_table(artifact.replay_path)}` | "
            f"{_escape_table(artifact.details)} |"
        )
    return "\n".join(lines) + "\n"


def build_replay_production_diff(
    *,
    as_of: date,
    project_root: Path,
    paths: ReplayPaths,
) -> ReplayProductionDiff:
    reports_dir = project_root / "outputs" / "reports"
    processed_dir = project_root / "data" / "processed"
    artifacts = [
        _compare_file_artifact(
            artifact_id="daily_score_report",
            artifact_class="report",
            production_path=default_daily_score_report_path(reports_dir, as_of),
            replay_path=_daily_score_report_path(paths, as_of),
        ),
        _compare_file_artifact(
            artifact_id="alerts_report",
            artifact_class="report",
            production_path=default_alert_report_path(reports_dir, as_of),
            replay_path=_alert_report_path(paths, as_of),
        ),
        _compare_file_artifact(
            artifact_id="dashboard_html",
            artifact_class="dashboard",
            production_path=default_evidence_dashboard_path(reports_dir, as_of),
            replay_path=_dashboard_html_path(paths, as_of),
        ),
        _compare_file_artifact(
            artifact_id="dashboard_json",
            artifact_class="dashboard",
            production_path=default_evidence_dashboard_json_path(reports_dir, as_of),
            replay_path=_dashboard_json_path(paths, as_of),
        ),
        _compare_file_artifact(
            artifact_id="data_quality_report",
            artifact_class="report",
            production_path=default_quality_report_path(reports_dir, as_of),
            replay_path=default_quality_report_path(paths.reports_dir, as_of),
        ),
        _compare_file_artifact(
            artifact_id="feature_summary_report",
            artifact_class="report",
            production_path=default_feature_report_path(reports_dir, as_of),
            replay_path=default_feature_report_path(paths.reports_dir, as_of),
        ),
        _compare_file_artifact(
            artifact_id="sec_metrics_validation_report",
            artifact_class="report",
            production_path=default_sec_fundamental_metrics_validation_report_path(
                reports_dir,
                as_of,
            ),
            replay_path=default_sec_fundamental_metrics_validation_report_path(
                paths.reports_dir,
                as_of,
            ),
        ),
        _compare_file_artifact(
            artifact_id="sec_fundamental_features",
            artifact_class="derived_csv",
            production_path=default_sec_fundamental_features_csv_path(
                processed_dir,
                as_of,
            ),
            replay_path=default_sec_fundamental_features_csv_path(
                paths.data_processed_dir,
                as_of,
            ),
        ),
        _compare_file_artifact(
            artifact_id="decision_snapshot",
            artifact_class="json",
            production_path=default_decision_snapshot_path(
                processed_dir / "decision_snapshots",
                as_of,
            ),
            replay_path=_decision_snapshot_path(paths, as_of),
        ),
        _compare_json_projection_artifact(
            artifact_id="decision_snapshot_core_fields",
            artifact_class="json_projection",
            production_path=default_decision_snapshot_path(
                processed_dir / "decision_snapshots",
                as_of,
            ),
            replay_path=_decision_snapshot_path(paths, as_of),
            projection=_decision_snapshot_core_fields,
            details=(
                "selected score/position/execution fields; excludes generated_at and paths"
            ),
        ),
        _compare_file_artifact(
            artifact_id="evidence_bundle",
            artifact_class="json",
            production_path=default_report_trace_bundle_path(
                default_daily_score_report_path(reports_dir, as_of)
            ),
            replay_path=_trace_bundle_path(paths, as_of),
        ),
        _compare_csv_rows_artifact(
            artifact_id="features_daily_rows",
            artifact_class="history_csv_rows",
            production_path=processed_dir / "features_daily.csv",
            replay_path=paths.data_processed_dir / "features_daily.csv",
            as_of=as_of,
        ),
        _compare_csv_rows_artifact(
            artifact_id="scores_daily_rows",
            artifact_class="history_csv_rows",
            production_path=processed_dir / "scores_daily.csv",
            replay_path=paths.data_processed_dir / "scores_daily.csv",
            as_of=as_of,
        ),
    ]
    return ReplayProductionDiff(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        status=_production_diff_status(artifacts),
        report_path=paths.root / "diff_vs_production.md",
        json_path=paths.root / "diff_vs_production.json",
        artifacts=tuple(artifacts),
    )


def _prepare_replay_inputs(
    *,
    as_of: date,
    visible_at: datetime,
    project_root: Path,
    paths: ReplayPaths,
    openai_replay_policy: str,
) -> tuple[list[ReplayInputRecord], list[str]]:
    records: list[ReplayInputRecord] = []
    errors: list[str] = []
    raw_dir = project_root / "data" / "raw"
    processed_dir = project_root / "data" / "processed"
    reports_dir = project_root / "outputs" / "reports"
    pit_input_cutoff = min(visible_at, _default_visible_at(as_of))
    openai_input_cutoff = min(visible_at, _default_visible_at(as_of))

    records.append(
        _filter_pit_manifest(
            source_path=raw_dir / "pit_snapshots" / "manifest.csv",
            replay_path=paths.data_raw_dir / "pit_snapshots" / "manifest.csv",
            visible_at=pit_input_cutoff,
            errors=errors,
        )
    )
    records.append(
        _filter_fmp_forward_pit_normalized(
            artifact_id="fmp_forward_pit_normalized",
            artifact_class="pit_normalized",
            source_path=default_fmp_forward_pit_normalized_path(
                processed_dir / "pit_snapshots",
                as_of,
            ),
            replay_path=(
                paths.data_processed_dir
                / "pit_snapshots"
                / f"fmp_forward_pit_{as_of.isoformat()}.csv"
            ),
            manifest_path=paths.data_raw_dir / "pit_snapshots" / "manifest.csv",
            project_root=project_root,
            as_of=as_of,
            visible_at=pit_input_cutoff,
            errors=errors,
        )
    )
    records.append(
        _copy_required_file(
            artifact_id="pit_validation_report",
            artifact_class="pit_report",
            source_path=default_pit_snapshot_validation_report_path(reports_dir, as_of),
            replay_path=paths.input_root
            / "outputs"
            / "reports"
            / f"pit_snapshots_validation_{as_of.isoformat()}.md",
            errors=errors,
        )
    )
    records.append(
        _copy_required_file(
            artifact_id="fmp_forward_pit_fetch_report",
            artifact_class="pit_report",
            source_path=default_fmp_forward_pit_fetch_report_path(reports_dir, as_of),
            replay_path=paths.input_root
            / "outputs"
            / "reports"
            / f"fmp_forward_pit_fetch_{as_of.isoformat()}.md",
            errors=errors,
        )
    )
    records.append(
        _filter_valuation_snapshots(
            source_dir=project_root / "data" / "external" / "valuation_snapshots",
            replay_dir=paths.input_root / "data" / "external" / "valuation_snapshots",
            as_of=as_of,
            errors=errors,
        )
    )
    records.append(
        _copy_required_file(
            artifact_id="sec_fundamentals",
            artifact_class="sec_metrics",
            source_path=default_sec_fundamental_metrics_csv_path(processed_dir, as_of),
            replay_path=paths.data_processed_dir / f"sec_fundamentals_{as_of.isoformat()}.csv",
            errors=errors,
            count_csv_rows=True,
        )
    )
    records.append(
        _copy_optional_file(
            artifact_id="valuation_fetch_report",
            artifact_class="valuation_report",
            source_path=default_fmp_valuation_fetch_report_path(reports_dir, as_of),
            replay_path=paths.input_root
            / "outputs"
            / "reports"
            / f"fmp_valuation_fetch_{as_of.isoformat()}.md",
        )
    )
    records.append(
        _copy_optional_file(
            artifact_id="valuation_validation_report",
            artifact_class="valuation_report",
            source_path=default_valuation_validation_report_path(reports_dir, as_of),
            replay_path=paths.input_root
            / "outputs"
            / "reports"
            / f"valuation_validation_{as_of.isoformat()}.md",
        )
    )
    records.append(
        _filter_risk_event_occurrences(
            source_dir=project_root / "data" / "external" / "risk_event_occurrences",
            replay_dir=paths.input_root / "data" / "external" / "risk_event_occurrences",
            as_of=as_of,
            errors=errors,
        )
    )
    records.extend(
        _prepare_openai_replay_cache(
            project_root=project_root,
            source_processed_dir=processed_dir,
            source_reports_dir=reports_dir,
            replay_paths=paths,
            as_of=as_of,
            visible_at=openai_input_cutoff,
            policy=openai_replay_policy,
            errors=errors,
        )
    )
    records.append(
        _initialize_historical_csv(
            artifact_id="features_daily_history",
            artifact_class="replay_seed",
            source_path=processed_dir / "features_daily.csv",
            replay_path=paths.data_processed_dir / "features_daily.csv",
            as_of=as_of,
        )
    )
    records.append(
        _initialize_historical_csv(
            artifact_id="scores_daily_history",
            artifact_class="replay_seed",
            source_path=processed_dir / "scores_daily.csv",
            replay_path=paths.data_processed_dir / "scores_daily.csv",
            as_of=as_of,
        )
    )
    return records, errors


def _run_replay_commands(
    *,
    as_of: date,
    project_root: Path,
    paths: ReplayPaths,
    full_universe: bool,
    env: Mapping[str, str],
    runner: Any,
) -> tuple[ReplayCommandResult, ...]:
    commands = [
        (
            "score_daily",
            "生成 replay 每日评分、trace 和告警",
            _score_daily_command(project_root, paths, as_of, full_universe=full_universe),
        ),
        (
            "reports_dashboard",
            "生成 replay 只读决策 dashboard",
            _dashboard_command(paths, as_of),
        ),
        (
            "pipeline_health",
            "检查 replay artifact 和 PIT 健康",
            _pipeline_health_command(project_root, paths, as_of),
        ),
        (
            "secret_hygiene",
            "扫描 replay bundle 中的疑似 secret",
            _secret_scan_command(paths, as_of),
        ),
    ]
    results: list[ReplayCommandResult] = []
    for step_id, title, command in commands:
        result = _run_command_step(
            step_id=step_id,
            title=title,
            command=command,
            project_root=project_root,
            env=env,
            runner=runner,
        )
        results.append(result)
        if result.status == "FAIL":
            break
    return tuple(results)


def _score_daily_command(
    project_root: Path,
    paths: ReplayPaths,
    as_of: date,
    *,
    full_universe: bool,
) -> tuple[str, ...]:
    reports = paths.reports_dir
    command = [
        "aits",
        "score-daily",
        "--as-of",
        as_of.isoformat(),
        "--prices-path",
        str(project_root / "data" / "raw" / "prices_daily.csv"),
        "--rates-path",
        str(project_root / "data" / "raw" / "rates_daily.csv"),
        "--features-path",
        str(paths.data_processed_dir / "features_daily.csv"),
        "--scores-path",
        str(paths.data_processed_dir / "scores_daily.csv"),
        "--report-path",
        str(_daily_score_report_path(paths, as_of)),
        "--alert-report-path",
        str(_alert_report_path(paths, as_of)),
        "--portfolio-exposure-report-path",
        str(default_portfolio_exposure_report_path(reports, as_of)),
        "--trace-bundle-path",
        str(_trace_bundle_path(paths, as_of)),
        "--decision-snapshot-path",
        str(_decision_snapshot_path(paths, as_of)),
        "--belief-state-path",
        str(
            default_belief_state_path(
                paths.output_root / "data" / "processed" / "belief_state",
                as_of,
            )
        ),
        "--belief-state-history-path",
        str(paths.data_processed_dir / DEFAULT_BELIEF_STATE_HISTORY_PATH.name),
        "--feature-report-path",
        str(default_feature_report_path(reports, as_of)),
        "--quality-report-path",
        str(default_quality_report_path(reports, as_of)),
        "--feature-availability-report-path",
        str(default_feature_availability_report_path(reports, as_of)),
        "--prediction-ledger-path",
        str(paths.data_processed_dir / DEFAULT_PREDICTION_LEDGER_PATH.name),
        "--prediction-candidate-id",
        "replay",
        "--prediction-production-effect",
        "none",
        "--sec-fundamentals-path",
        str(paths.data_processed_dir / f"sec_fundamentals_{as_of.isoformat()}.csv"),
        "--sec-fundamental-features-path",
        str(default_sec_fundamental_features_csv_path(paths.data_processed_dir, as_of)),
        "--sec-fundamental-feature-report-path",
        str(default_sec_fundamental_features_report_path(reports, as_of)),
        "--sec-metrics-validation-report-path",
        str(default_sec_fundamental_metrics_validation_report_path(reports, as_of)),
        "--risk-event-occurrence-report-path",
        str(default_risk_event_occurrence_report_path(reports, as_of)),
        "--risk-event-occurrences-path",
        str(paths.input_root / "data" / "external" / "risk_event_occurrences"),
        "--risk-event-prereview-queue-path",
        str(paths.data_processed_dir / RISK_EVENT_PREREVIEW_QUEUE_NAME),
        "--risk-event-openai-precheck-report-path",
        str(default_risk_event_openai_prereview_report_path(reports, as_of)),
        "--execution-policy-report-path",
        str(default_execution_policy_report_path(reports, as_of)),
        "--valuation-path",
        str(paths.input_root / "data" / "external" / "valuation_snapshots"),
        "--skip-risk-event-openai-precheck",
    ]
    if full_universe:
        command.append("--full-universe")
    return tuple(command)


def _dashboard_command(paths: ReplayPaths, as_of: date) -> tuple[str, ...]:
    return (
        "aits",
        "reports",
        "dashboard",
        "--as-of",
        as_of.isoformat(),
        "--daily-report-path",
        str(_daily_score_report_path(paths, as_of)),
        "--trace-bundle-path",
        str(_trace_bundle_path(paths, as_of)),
        "--decision-snapshot-path",
        str(_decision_snapshot_path(paths, as_of)),
        "--belief-state-path",
        str(
            default_belief_state_path(
                paths.output_root / "data" / "processed" / "belief_state",
                as_of,
            )
        ),
        "--alerts-report-path",
        str(_alert_report_path(paths, as_of)),
        "--scores-daily-path",
        str(paths.data_processed_dir / "scores_daily.csv"),
        "--output-path",
        str(_dashboard_html_path(paths, as_of)),
        "--json-output-path",
        str(_dashboard_json_path(paths, as_of)),
    )


def _pipeline_health_command(
    project_root: Path,
    paths: ReplayPaths,
    as_of: date,
) -> tuple[str, ...]:
    reports = paths.reports_dir
    return (
        "aits",
        "ops",
        "health",
        "--as-of",
        as_of.isoformat(),
        "--prices-path",
        str(project_root / "data" / "raw" / "prices_daily.csv"),
        "--rates-path",
        str(project_root / "data" / "raw" / "rates_daily.csv"),
        "--features-path",
        str(paths.data_processed_dir / "features_daily.csv"),
        "--scores-path",
        str(paths.data_processed_dir / "scores_daily.csv"),
        "--data-quality-report-path",
        str(default_quality_report_path(reports, as_of)),
        "--daily-report-path",
        str(_daily_score_report_path(paths, as_of)),
        "--pit-manifest-path",
        str(paths.data_raw_dir / "pit_snapshots" / "manifest.csv"),
        "--pit-normalized-path",
        str(
            paths.data_processed_dir
            / "pit_snapshots"
            / f"fmp_forward_pit_{as_of.isoformat()}.csv"
        ),
        "--pit-validation-report-path",
        str(
            paths.input_root
            / "outputs"
            / "reports"
            / f"pit_snapshots_validation_{as_of.isoformat()}.md"
        ),
        "--pit-fetch-report-path",
        str(
            paths.input_root
            / "outputs"
            / "reports"
            / f"fmp_forward_pit_fetch_{as_of.isoformat()}.md"
        ),
        "--output-path",
        str(_pipeline_health_report_path(paths, as_of)),
        "--alert-output-path",
        str(default_pipeline_health_alert_report_path(reports, as_of)),
    )


def _secret_scan_command(paths: ReplayPaths, as_of: date) -> tuple[str, ...]:
    return (
        "aits",
        "security",
        "scan-secrets",
        "--as-of",
        as_of.isoformat(),
        "--scan-paths",
        str(paths.root),
        "--output-path",
        str(_secret_report_path(paths, as_of)),
    )


def _run_command_step(
    *,
    step_id: str,
    title: str,
    command: tuple[str, ...],
    project_root: Path,
    env: Mapping[str, str],
    runner: Any,
) -> ReplayCommandResult:
    started_at = datetime.now(tz=UTC)
    try:
        completed = runner(
            _execution_command(command),
            cwd=project_root,
            env=dict(env),
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        ended_at = datetime.now(tz=UTC)
        return ReplayCommandResult(
            step_id=step_id,
            title=title,
            command=command,
            status="FAIL",
            return_code=None,
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=(ended_at - started_at).total_seconds(),
            error=f"{type(exc).__name__}: {exc}",
        )
    ended_at = datetime.now(tz=UTC)
    stdout_text = completed.stdout or ""
    stderr_text = completed.stderr or ""
    return ReplayCommandResult(
        step_id=step_id,
        title=title,
        command=command,
        status="PASS" if completed.returncode == 0 else "FAIL",
        return_code=completed.returncode,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=(ended_at - started_at).total_seconds(),
        stdout_line_count=len(stdout_text.splitlines()),
        stderr_line_count=len(stderr_text.splitlines()),
    )


def _filter_pit_manifest(
    *,
    source_path: Path,
    replay_path: Path,
    visible_at: datetime,
    errors: list[str],
) -> ReplayInputRecord:
    if not source_path.exists():
        errors.append(f"PIT manifest 不存在：{source_path}")
        return ReplayInputRecord(
            artifact_id="pit_manifest",
            artifact_class="pit_manifest",
            source_path=str(source_path),
            replay_path=str(replay_path),
            status="MISSING",
            reason="required PIT manifest missing",
        )
    rows = _read_csv_dicts(source_path)
    included, excluded, timestamps = _split_rows_by_timestamp(rows, visible_at)
    if not included:
        errors.append("PIT manifest 可见窗口内没有记录。")
    _write_csv_dicts(replay_path, included, fieldnames=rows[0].keys() if rows else ())
    return ReplayInputRecord(
        artifact_id="pit_manifest",
        artifact_class="pit_manifest",
        source_path=str(source_path),
        replay_path=str(replay_path),
        status="PASS" if included else "FAIL",
        row_count=len(rows),
        included_count=len(included),
        excluded_count=len(excluded),
        sha256=_sha256_file(replay_path),
        min_timestamp=_iso_or_none(min(timestamps) if timestamps else None),
        max_timestamp=_iso_or_none(max(timestamps) if timestamps else None),
        reason="filtered by available_time/snapshot_time <= effective PIT input cutoff",
    )


def _filter_timestamped_csv(
    *,
    artifact_id: str,
    artifact_class: str,
    source_path: Path,
    replay_path: Path,
    visible_at: datetime,
    errors: list[str],
) -> ReplayInputRecord:
    if not source_path.exists():
        errors.append(f"{artifact_id} 不存在：{source_path}")
        return ReplayInputRecord(
            artifact_id=artifact_id,
            artifact_class=artifact_class,
            source_path=str(source_path),
            replay_path=str(replay_path),
            status="MISSING",
            reason="required timestamped CSV missing",
        )
    rows = _read_csv_dicts(source_path)
    included, excluded, timestamps = _split_rows_by_timestamp(rows, visible_at)
    if not included:
        errors.append(f"{artifact_id} 可见窗口内没有记录。")
    _write_csv_dicts(replay_path, included, fieldnames=rows[0].keys() if rows else ())
    return ReplayInputRecord(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        source_path=str(source_path),
        replay_path=str(replay_path),
        status="PASS" if included else "FAIL",
        row_count=len(rows),
        included_count=len(included),
        excluded_count=len(excluded),
        sha256=_sha256_file(replay_path),
        min_timestamp=_iso_or_none(min(timestamps) if timestamps else None),
        max_timestamp=_iso_or_none(max(timestamps) if timestamps else None),
        reason="filtered by available_time/snapshot_time <= effective PIT input cutoff",
    )


def _filter_fmp_forward_pit_normalized(
    *,
    artifact_id: str,
    artifact_class: str,
    source_path: Path,
    replay_path: Path,
    manifest_path: Path,
    project_root: Path,
    as_of: date,
    visible_at: datetime,
    errors: list[str],
) -> ReplayInputRecord:
    if not source_path.exists():
        rebuilt = _rebuild_fmp_forward_pit_normalized_from_manifest(
            manifest_path=manifest_path,
            replay_path=replay_path,
            project_root=project_root,
            as_of=as_of,
        )
        if rebuilt is not None:
            return rebuilt
        errors.append(f"{artifact_id} 不存在：{source_path}")
        return ReplayInputRecord(
            artifact_id=artifact_id,
            artifact_class=artifact_class,
            source_path=str(source_path),
            replay_path=str(replay_path),
            status="MISSING",
            reason="required timestamped CSV missing and raw manifest rebuild unavailable",
        )

    rows = _read_csv_dicts(source_path)
    included, excluded, timestamps = _split_rows_by_timestamp(rows, visible_at)
    if included:
        _write_csv_dicts(replay_path, included, fieldnames=rows[0].keys() if rows else ())
        return ReplayInputRecord(
            artifact_id=artifact_id,
            artifact_class=artifact_class,
            source_path=str(source_path),
            replay_path=str(replay_path),
            status="PASS",
            row_count=len(rows),
            included_count=len(included),
            excluded_count=len(excluded),
            sha256=_sha256_file(replay_path),
            min_timestamp=_iso_or_none(min(timestamps) if timestamps else None),
            max_timestamp=_iso_or_none(max(timestamps) if timestamps else None),
            reason="filtered by available_time/snapshot_time <= effective PIT input cutoff",
        )

    rebuilt = _rebuild_fmp_forward_pit_normalized_from_manifest(
        manifest_path=manifest_path,
        replay_path=replay_path,
        project_root=project_root,
        as_of=as_of,
    )
    if rebuilt is not None:
        return ReplayInputRecord(
            artifact_id=artifact_id,
            artifact_class=artifact_class,
            source_path=str(source_path),
            replay_path=rebuilt.replay_path,
            status=rebuilt.status,
            row_count=rebuilt.included_count,
            included_count=rebuilt.included_count,
            excluded_count=0,
            sha256=rebuilt.sha256,
            min_timestamp=rebuilt.min_timestamp,
            max_timestamp=rebuilt.max_timestamp,
            reason=(
                f"source normalized CSV had {len(rows)} rows but none within "
                "effective PIT input cutoff; "
                "rebuilt replay normalized CSV from filtered raw PIT manifest"
            ),
        )

    errors.append(f"{artifact_id} 可见窗口内没有记录。")
    _write_csv_dicts(replay_path, [], fieldnames=rows[0].keys() if rows else ())
    return ReplayInputRecord(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        source_path=str(source_path),
        replay_path=str(replay_path),
        status="FAIL",
        row_count=len(rows),
        included_count=0,
        excluded_count=len(excluded),
        sha256=_sha256_file(replay_path),
        min_timestamp=_iso_or_none(min(timestamps) if timestamps else None),
        max_timestamp=_iso_or_none(max(timestamps) if timestamps else None),
        reason="filtered by available_time/snapshot_time <= effective PIT input cutoff",
    )


def _rebuild_fmp_forward_pit_normalized_from_manifest(
    *,
    manifest_path: Path,
    replay_path: Path,
    project_root: Path,
    as_of: date,
) -> ReplayInputRecord | None:
    if not manifest_path.exists():
        return None
    payloads_by_ticker: dict[str, FmpForwardPitRawPayload] = {}
    for row in _read_csv_dicts(manifest_path):
        raw_payload_path = row.get("raw_payload_path", "")
        if "fmp_forward_pit" not in raw_payload_path.replace("\\", "/"):
            continue
        payload = _load_fmp_forward_pit_raw_payload(
            raw_payload_path=raw_payload_path,
            raw_payload_sha256=row.get("raw_payload_sha256", ""),
            project_root=project_root,
        )
        if payload is None or payload.as_of != as_of:
            continue
        current = payloads_by_ticker.get(payload.ticker)
        if current is None or payload.downloaded_at > current.downloaded_at:
            payloads_by_ticker[payload.ticker] = payload
    payloads = tuple(
        payloads_by_ticker[ticker] for ticker in sorted(payloads_by_ticker)
    )
    if not payloads:
        return None

    normalized_rows = normalize_fmp_forward_pit_payloads(payloads)
    if not normalized_rows:
        return None
    write_fmp_forward_pit_normalized_csv(normalized_rows, replay_path)
    timestamps = [
        timestamp
        for row in normalized_rows
        if (timestamp := _parse_datetime_value(row.available_time)) is not None
    ]
    return ReplayInputRecord(
        artifact_id="fmp_forward_pit_normalized",
        artifact_class="pit_normalized",
        source_path=str(manifest_path),
        replay_path=str(replay_path),
        status="PASS",
        row_count=len(normalized_rows),
        included_count=len(normalized_rows),
        excluded_count=0,
        sha256=_sha256_file(replay_path),
        min_timestamp=_iso_or_none(min(timestamps) if timestamps else None),
        max_timestamp=_iso_or_none(max(timestamps) if timestamps else None),
        reason="rebuilt from filtered FMP forward PIT raw payload manifest",
    )


def _load_fmp_forward_pit_raw_payload(
    *,
    raw_payload_path: str,
    raw_payload_sha256: str,
    project_root: Path,
) -> FmpForwardPitRawPayload | None:
    path = Path(raw_payload_path)
    resolved_path = path if path.is_absolute() else project_root / path
    if not resolved_path.exists():
        return None
    raw = _read_yaml_mapping(resolved_path)
    ticker = str(raw.get("ticker") or "").strip().upper()
    as_of = _parse_date_value(raw.get("as_of"))
    captured_at = _parse_date_value(raw.get("captured_at"))
    downloaded_at = _parse_datetime_value(raw.get("downloaded_at"))
    records_by_endpoint = raw.get("records_by_endpoint")
    request_parameters = raw.get("request_parameters_by_endpoint")
    if (
        not ticker
        or as_of is None
        or captured_at is None
        or downloaded_at is None
        or not isinstance(records_by_endpoint, dict)
        or not isinstance(request_parameters, dict)
    ):
        return None
    endpoint_records = {
        str(endpoint): tuple(
            item for item in records if isinstance(item, dict)
        )
        for endpoint, records in records_by_endpoint.items()
        if isinstance(records, list)
    }
    request_parameters_by_endpoint = {
        str(endpoint): params
        for endpoint, params in request_parameters.items()
        if isinstance(params, dict)
    }
    return FmpForwardPitRawPayload(
        ticker=ticker,
        as_of=as_of,
        captured_at=captured_at,
        downloaded_at=downloaded_at,
        provider_symbol=str(raw.get("provider_symbol") or ticker),
        endpoint_records=endpoint_records,
        request_parameters_by_endpoint=request_parameters_by_endpoint,
        checksum_sha256=raw_payload_sha256 or _sha256_file(resolved_path) or "",
        source_path=path,
    )


def _filter_valuation_snapshots(
    *,
    source_dir: Path,
    replay_dir: Path,
    as_of: date,
    errors: list[str],
) -> ReplayInputRecord:
    if not source_dir.exists():
        errors.append(f"valuation snapshot 目录不存在：{source_dir}")
        return ReplayInputRecord(
            artifact_id="valuation_snapshots",
            artifact_class="valuation_snapshots",
            source_path=str(source_dir),
            replay_path=str(replay_dir),
            status="MISSING",
            reason="required valuation snapshot directory missing",
        )
    replay_dir.mkdir(parents=True, exist_ok=True)
    yaml_paths = sorted(source_dir.glob("*.yaml"))
    included = 0
    excluded = 0
    timestamps: list[datetime] = []
    for source_path in yaml_paths:
        raw = _read_yaml_mapping(source_path)
        snapshot_as_of = _parse_date_value(raw.get("as_of"))
        captured_at = _parse_date_value(raw.get("captured_at"))
        if snapshot_as_of is not None:
            timestamps.append(datetime.combine(snapshot_as_of, time.max, tzinfo=UTC))
        if captured_at is not None:
            timestamps.append(datetime.combine(captured_at, time.max, tzinfo=UTC))
        if (
            snapshot_as_of is not None
            and captured_at is not None
            and snapshot_as_of <= as_of
            and captured_at <= as_of
        ):
            shutil.copy2(source_path, replay_dir / source_path.name)
            included += 1
        else:
            excluded += 1
    if included == 0:
        errors.append("valuation snapshots 可见窗口内没有 YAML。")
    return ReplayInputRecord(
        artifact_id="valuation_snapshots",
        artifact_class="valuation_snapshots",
        source_path=str(source_dir),
        replay_path=str(replay_dir),
        status="PASS" if included else "FAIL",
        row_count=len(yaml_paths),
        included_count=included,
        excluded_count=excluded,
        sha256=_directory_digest(replay_dir),
        min_timestamp=_iso_or_none(min(timestamps) if timestamps else None),
        max_timestamp=_iso_or_none(max(timestamps) if timestamps else None),
        reason="included when valuation as_of and captured_at are not later than replay as_of",
    )


def _filter_risk_event_occurrences(
    *,
    source_dir: Path,
    replay_dir: Path,
    as_of: date,
    errors: list[str],
) -> ReplayInputRecord:
    if not source_dir.exists():
        return ReplayInputRecord(
            artifact_id="risk_event_occurrences",
            artifact_class="risk_event_occurrences",
            source_path=str(source_dir),
            replay_path=str(replay_dir),
            status="MISSING_OPTIONAL",
            reason=(
                "optional risk event occurrence directory missing; score-daily will "
                "report missing path in replay output"
            ),
        )

    replay_dir.mkdir(parents=True, exist_ok=True)
    source_store = load_risk_event_occurrence_store(source_dir)
    filtered_store = risk_event_occurrence_store_as_of(
        store=source_store,
        as_of=as_of,
    )
    for load_error in source_store.load_errors:
        errors.append(f"risk_event_occurrences YAML 读取失败：{load_error.path}")

    for loaded in filtered_store.loaded:
        _write_risk_event_occurrence_yaml(
            replay_dir / f"{loaded.occurrence.occurrence_id}.yaml",
            loaded.occurrence.model_dump(mode="json", exclude_none=False),
        )
    for loaded_attestation in filtered_store.review_attestations:
        _write_risk_event_occurrence_yaml(
            replay_dir / f"{loaded_attestation.attestation.attestation_id}.yaml",
            {
                "review_attestation": loaded_attestation.attestation.model_dump(
                    mode="json",
                    exclude_none=False,
                )
            },
        )

    source_count = len(source_store.loaded) + len(source_store.review_attestations)
    included_count = len(filtered_store.loaded) + len(filtered_store.review_attestations)
    timestamps = _risk_event_occurrence_timestamps(source_store)
    return ReplayInputRecord(
        artifact_id="risk_event_occurrences",
        artifact_class="risk_event_occurrences",
        source_path=str(source_dir),
        replay_path=str(replay_dir),
        status="FAIL" if source_store.load_errors else "PASS",
        row_count=source_count,
        included_count=included_count,
        excluded_count=source_count - included_count,
        sha256=_directory_digest(replay_dir),
        min_timestamp=_iso_or_none(min(timestamps) if timestamps else None),
        max_timestamp=_iso_or_none(max(timestamps) if timestamps else None),
        reason=(
            "filtered by occurrence, evidence, review attestation and checked source "
            "dates not later than replay as_of"
        ),
    )


def _write_risk_event_occurrence_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def _risk_event_occurrence_timestamps(store: RiskEventOccurrenceStore) -> list[datetime]:
    timestamps: list[datetime] = []
    for loaded in store.loaded:
        occurrence = loaded.occurrence
        for value in (
            occurrence.triggered_at,
            occurrence.last_confirmed_at,
            occurrence.resolved_at,
            occurrence.reviewed_at,
            occurrence.expiry_time,
            occurrence.next_review_due,
        ):
            _append_date_timestamp(timestamps, value)
        for source in occurrence.evidence_sources:
            _append_date_timestamp(timestamps, source.published_at)
            _append_date_timestamp(timestamps, source.captured_at)
    for loaded_attestation in store.review_attestations:
        attestation = loaded_attestation.attestation
        for value in (
            attestation.review_date,
            attestation.coverage_start,
            attestation.coverage_end,
            attestation.reviewed_at,
            attestation.next_review_due,
        ):
            _append_date_timestamp(timestamps, value)
        for checked_source in attestation.checked_sources:
            _append_date_timestamp(timestamps, checked_source.captured_at)
    return timestamps


def _append_date_timestamp(timestamps: list[datetime], value: date | None) -> None:
    if value is not None:
        timestamps.append(datetime.combine(value, time.max, tzinfo=UTC))


def _copy_required_file(
    *,
    artifact_id: str,
    artifact_class: str,
    source_path: Path,
    replay_path: Path,
    errors: list[str],
    count_csv_rows: bool = False,
) -> ReplayInputRecord:
    if not source_path.exists():
        errors.append(f"{artifact_id} 不存在：{source_path}")
        return ReplayInputRecord(
            artifact_id=artifact_id,
            artifact_class=artifact_class,
            source_path=str(source_path),
            replay_path=str(replay_path),
            status="MISSING",
            reason="required replay input missing",
        )
    replay_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, replay_path)
    row_count = _csv_row_count(replay_path) if count_csv_rows else None
    return ReplayInputRecord(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        source_path=str(source_path),
        replay_path=str(replay_path),
        status="PASS",
        row_count=row_count,
        included_count=row_count,
        excluded_count=0 if row_count is not None else None,
        sha256=_sha256_file(replay_path),
        reason="copied into replay input view",
    )


def _copy_optional_file(
    *,
    artifact_id: str,
    artifact_class: str,
    source_path: Path,
    replay_path: Path,
) -> ReplayInputRecord:
    if not source_path.exists():
        return ReplayInputRecord(
            artifact_id=artifact_id,
            artifact_class=artifact_class,
            source_path=str(source_path),
            replay_path=str(replay_path),
            status="MISSING_OPTIONAL",
            reason="optional prior report missing",
        )
    replay_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, replay_path)
    return ReplayInputRecord(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        source_path=str(source_path),
        replay_path=str(replay_path),
        status="PASS",
        sha256=_sha256_file(replay_path),
        reason="copied for audit context",
    )


def _prepare_openai_replay_cache(
    *,
    project_root: Path,
    source_processed_dir: Path,
    source_reports_dir: Path,
    replay_paths: ReplayPaths,
    as_of: date,
    visible_at: datetime,
    policy: str,
    errors: list[str],
) -> list[ReplayInputRecord]:
    policy_record = ReplayInputRecord(
        artifact_id="openai_replay_policy",
        artifact_class="openai_replay_policy",
        source_path="",
        replay_path=str(replay_paths.data_processed_dir / RISK_EVENT_PREREVIEW_QUEUE_NAME),
        status="DISABLED" if policy == "disabled" else "CACHE_ONLY",
        reason=(
            "OpenAI live replay disabled"
            if policy == "disabled"
            else "using only archived OpenAI prereview cache; live API remains disabled"
        ),
    )
    if policy == "disabled":
        return [policy_record]

    queue_source = source_processed_dir / RISK_EVENT_PREREVIEW_QUEUE_NAME
    queue_replay = replay_paths.data_processed_dir / RISK_EVENT_PREREVIEW_QUEUE_NAME
    report_source = default_risk_event_openai_prereview_report_path(
        source_reports_dir,
        as_of,
    )
    report_replay = default_risk_event_openai_prereview_report_path(
        replay_paths.reports_dir,
        as_of,
    )
    if not queue_source.exists():
        errors.append(f"risk_event_openai_prereview_queue 不存在：{queue_source}")
        return [
            policy_record,
            ReplayInputRecord(
                artifact_id="risk_event_openai_prereview_queue",
                artifact_class="openai_replay_cache",
                source_path=str(queue_source),
                replay_path=str(queue_replay),
                status="MISSING",
                reason="required OpenAI replay cache queue missing",
            ),
            _copy_required_file(
                artifact_id="risk_event_openai_prereview_report",
                artifact_class="openai_replay_cache",
                source_path=report_source,
                replay_path=report_replay,
                errors=errors,
            ),
        ]
    if not report_source.exists():
        errors.append(f"risk_event_openai_prereview_report 不存在：{report_source}")
        return [
            policy_record,
            _copy_required_json_file(
                artifact_id="risk_event_openai_prereview_queue",
                artifact_class="openai_replay_cache",
                source_path=queue_source,
                replay_path=queue_replay,
                errors=errors,
            ),
            ReplayInputRecord(
                artifact_id="risk_event_openai_prereview_report",
                artifact_class="openai_replay_cache",
                source_path=str(report_source),
                replay_path=str(report_replay),
                status="MISSING",
                reason="required OpenAI replay cache report missing",
            ),
        ]

    queue_record, report_record = _filter_openai_prereview_queue_for_replay(
        queue_source=queue_source,
        queue_replay=queue_replay,
        report_source=report_source,
        report_replay=report_replay,
        project_root=project_root,
        source_processed_dir=source_processed_dir,
        as_of=as_of,
        visible_at=visible_at,
        errors=errors,
    )
    return [
        policy_record,
        queue_record,
        report_record,
    ]


def _filter_openai_prereview_queue_for_replay(
    *,
    queue_source: Path,
    queue_replay: Path,
    report_source: Path,
    report_replay: Path,
    project_root: Path,
    source_processed_dir: Path,
    as_of: date,
    visible_at: datetime,
    errors: list[str],
) -> tuple[ReplayInputRecord, ReplayInputRecord]:
    try:
        raw_payload = json.loads(queue_source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        errors.append(f"risk_event_openai_prereview_queue 读取失败：{queue_source} ({exc})")
        return (
            ReplayInputRecord(
                artifact_id="risk_event_openai_prereview_queue",
                artifact_class="openai_replay_cache",
                source_path=str(queue_source),
                replay_path=str(queue_replay),
                status="FAIL",
                reason="OpenAI replay cache queue is not valid JSON",
            ),
            _copy_required_file(
                artifact_id="risk_event_openai_prereview_report",
                artifact_class="openai_replay_cache",
                source_path=report_source,
                replay_path=report_replay,
                errors=errors,
            ),
        )
    if not isinstance(raw_payload, dict):
        errors.append(f"risk_event_openai_prereview_queue 顶层结构不是 object：{queue_source}")
        return (
            ReplayInputRecord(
                artifact_id="risk_event_openai_prereview_queue",
                artifact_class="openai_replay_cache",
                source_path=str(queue_source),
                replay_path=str(queue_replay),
                status="FAIL",
                reason="OpenAI replay cache queue top-level JSON is not an object",
            ),
            _copy_required_file(
                artifact_id="risk_event_openai_prereview_report",
                artifact_class="openai_replay_cache",
                source_path=report_source,
                replay_path=report_replay,
                errors=errors,
            ),
        )

    raw_records = raw_payload.get("records")
    if not isinstance(raw_records, list):
        errors.append(f"risk_event_openai_prereview_queue 缺少 records list：{queue_source}")
        return (
            ReplayInputRecord(
                artifact_id="risk_event_openai_prereview_queue",
                artifact_class="openai_replay_cache",
                source_path=str(queue_source),
                replay_path=str(queue_replay),
                status="FAIL",
                reason="OpenAI replay cache queue missing records list",
            ),
            _copy_required_file(
                artifact_id="risk_event_openai_prereview_report",
                artifact_class="openai_replay_cache",
                source_path=report_source,
                replay_path=report_replay,
                errors=errors,
            ),
        )

    included: list[dict[str, Any]] = []
    excluded: list[dict[str, str]] = []
    timestamps: list[datetime] = []
    for index, raw_record in enumerate(raw_records, start=1):
        if not isinstance(raw_record, dict):
            excluded.append(
                {
                    "precheck_id": f"record_{index}",
                    "reason": "record_not_object",
                    "request_timestamp": "",
                    "cache_created_at": "",
                    "available_time": "",
                }
            )
            continue
        available_at, record_timestamps = _openai_prereview_record_available_time(
            raw_record,
            project_root=project_root,
            source_processed_dir=source_processed_dir,
        )
        timestamps.extend(record_timestamps)
        if available_at is None:
            excluded.append(
                _openai_prereview_exclusion(
                    raw_record,
                    reason="missing_provable_available_time",
                    available_at=None,
                )
            )
            continue
        if available_at > visible_at:
            excluded.append(
                _openai_prereview_exclusion(
                    raw_record,
                    reason="available_after_replay_cutoff",
                    available_at=available_at,
                )
            )
            continue
        included.append(dict(raw_record))

    filtered_payload = dict(raw_payload)
    filtered_payload["generated_at"] = datetime.now(tz=UTC).isoformat()
    filtered_payload["records"] = included
    filtered_payload["row_count"] = len(included)
    filtered_payload["record_count"] = len(included)
    filtered_payload["source_queue_path"] = str(queue_source)
    filtered_payload["source_queue_checksum_sha256"] = _sha256_file(queue_source)
    filtered_payload["replay_filter"] = {
        "filter_kind": "openai_prereview_cache_visibility",
        "as_of": as_of.isoformat(),
        "visibility_cutoff": visible_at.isoformat(),
        "source_report_path": str(report_source),
        "source_report_checksum_sha256": _sha256_file(report_source),
        "source_record_count": len(raw_records),
        "included_count": len(included),
        "excluded_count": len(excluded),
        "excluded_records": excluded,
    }
    _write_json(queue_replay, filtered_payload)
    _write_openai_replay_filter_report(
        report_path=report_replay,
        queue_source=queue_source,
        queue_replay=queue_replay,
        report_source=report_source,
        as_of=as_of,
        visible_at=visible_at,
        source_record_count=len(raw_records),
        included_count=len(included),
        excluded=excluded,
    )
    status = "PASS_WITH_EXCLUSIONS" if excluded else "PASS"
    return (
        ReplayInputRecord(
            artifact_id="risk_event_openai_prereview_queue",
            artifact_class="openai_replay_cache",
            source_path=str(queue_source),
            replay_path=str(queue_replay),
            status=status,
            row_count=len(raw_records),
            included_count=len(included),
            excluded_count=len(excluded),
            sha256=_sha256_file(queue_replay),
            min_timestamp=_iso_or_none(min(timestamps) if timestamps else None),
            max_timestamp=_iso_or_none(max(timestamps) if timestamps else None),
            reason=(
                "filtered by max(request_timestamp, cache_created_at, cache file "
                "created/request time) <= effective OpenAI replay cutoff"
            ),
        ),
        ReplayInputRecord(
            artifact_id="risk_event_openai_prereview_report",
            artifact_class="openai_replay_cache",
            source_path=str(report_source),
            replay_path=str(report_replay),
            status=status,
            row_count=len(raw_records),
            included_count=len(included),
            excluded_count=len(excluded),
            sha256=_sha256_file(report_replay),
            reason="generated replay cache-only visibility report from source prereview report",
        ),
    )


def _copy_required_json_file(
    *,
    artifact_id: str,
    artifact_class: str,
    source_path: Path,
    replay_path: Path,
    errors: list[str],
) -> ReplayInputRecord:
    record = _copy_required_file(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        source_path=source_path,
        replay_path=replay_path,
        errors=errors,
    )
    if record.status != "PASS":
        return record
    return ReplayInputRecord(
        artifact_id=record.artifact_id,
        artifact_class=record.artifact_class,
        source_path=record.source_path,
        replay_path=record.replay_path,
        status=record.status,
        row_count=_json_record_count(replay_path),
        included_count=_json_record_count(replay_path),
        excluded_count=0,
        sha256=record.sha256,
        min_timestamp=record.min_timestamp,
        max_timestamp=record.max_timestamp,
        reason=record.reason,
    )


def _openai_prereview_record_available_time(
    record: Mapping[str, Any],
    *,
    project_root: Path,
    source_processed_dir: Path,
) -> tuple[datetime | None, list[datetime]]:
    timestamps: list[datetime] = []
    for key in ("request_timestamp", "cache_created_at"):
        timestamp = _parse_datetime_value(record.get(key))
        if timestamp is not None:
            timestamps.append(timestamp)

    cache_payload = _read_openai_cache_payload(
        record,
        project_root=project_root,
        source_processed_dir=source_processed_dir,
    )
    if cache_payload is not None:
        for key in ("request_timestamp", "created_at"):
            timestamp = _parse_datetime_value(cache_payload.get(key))
            if timestamp is not None:
                timestamps.append(timestamp)

    if not timestamps:
        return None, timestamps
    return max(timestamps), timestamps


def _read_openai_cache_payload(
    record: Mapping[str, Any],
    *,
    project_root: Path,
    source_processed_dir: Path,
) -> dict[str, Any] | None:
    for candidate_path in _candidate_openai_cache_paths(
        record,
        project_root=project_root,
        source_processed_dir=source_processed_dir,
    ):
        if not candidate_path.exists() or not candidate_path.is_file():
            continue
        try:
            loaded = json.loads(candidate_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(loaded, dict):
            continue
        if _openai_cache_payload_matches_record(loaded, record):
            return loaded
    return None


def _candidate_openai_cache_paths(
    record: Mapping[str, Any],
    *,
    project_root: Path,
    source_processed_dir: Path,
) -> tuple[Path, ...]:
    candidates: list[Path] = []

    def add(path: Path) -> None:
        if path not in candidates:
            candidates.append(path)

    cache_path_text = str(record.get("cache_path") or "").strip()
    if cache_path_text:
        raw_cache_path = Path(cache_path_text)
        if raw_cache_path.is_absolute():
            add(raw_cache_path)
        else:
            add(project_root / raw_cache_path)
            add(source_processed_dir / raw_cache_path)

    cache_key = str(record.get("cache_key") or "").strip()
    if cache_key:
        add(source_processed_dir / "agent_request_cache" / f"{cache_key}.json")
    return tuple(candidates)


def _openai_cache_payload_matches_record(
    cache_payload: Mapping[str, Any],
    record: Mapping[str, Any],
) -> bool:
    cache_key = str(record.get("cache_key") or "").strip()
    if cache_key and cache_payload.get("cache_key") != cache_key:
        return False
    input_checksum = str(record.get("input_checksum_sha256") or "").strip()
    if input_checksum and cache_payload.get("input_checksum_sha256") != input_checksum:
        return False
    output_checksum = str(record.get("output_checksum_sha256") or "").strip()
    cache_output_checksum = str(cache_payload.get("output_checksum_sha256") or "").strip()
    if output_checksum and cache_output_checksum and cache_output_checksum != output_checksum:
        return False
    return True


def _openai_prereview_exclusion(
    record: Mapping[str, Any],
    *,
    reason: str,
    available_at: datetime | None,
) -> dict[str, str]:
    return {
        "precheck_id": str(record.get("precheck_id") or record.get("risk_id") or ""),
        "reason": reason,
        "request_timestamp": str(record.get("request_timestamp") or ""),
        "cache_created_at": str(record.get("cache_created_at") or ""),
        "available_time": "" if available_at is None else available_at.isoformat(),
    }


def _write_openai_replay_filter_report(
    *,
    report_path: Path,
    queue_source: Path,
    queue_replay: Path,
    report_source: Path,
    as_of: date,
    visible_at: datetime,
    source_record_count: int,
    included_count: int,
    excluded: list[dict[str, str]],
) -> None:
    status = "PASS_WITH_EXCLUSIONS" if excluded else "PASS"
    lines = [
        "# OpenAI prereview replay 缓存过滤报告",
        "",
        f"- 状态：{status}",
        f"- 评估日期：{as_of.isoformat()}",
        f"- 可见性截止：{visible_at.isoformat()}",
        "- replay 策略：cache-only；不调用 live OpenAI API。",
        f"- 源 queue：`{queue_source}`",
        f"- 源报告：`{report_source}`",
        f"- replay queue：`{queue_replay}`",
        f"- 源记录数：{source_record_count}",
        f"- 复用记录数：{included_count}",
        f"- 排除记录数：{len(excluded)}",
        "",
        "## 过滤规则",
        "",
        (
            "- 只复用 `request_timestamp`、`cache_created_at` 或匹配 cache 文件中的 "
            "`created_at/request_timestamp` 可证明不晚于 replay 可见性截止的记录。"
        ),
        "- 缺少可证明时间戳或晚于截止时间的记录不会进入 replay queue。",
    ]
    if excluded:
        lines.extend(
            [
                "",
                "## 排除记录",
                "",
                "|precheck_id|reason|request_timestamp|cache_created_at|available_time|",
                "|---|---|---|---|---|",
            ]
        )
        for item in excluded:
            lines.append(
                f"|{_escape_table(item.get('precheck_id', ''))}|"
                f"{_escape_table(item.get('reason', ''))}|"
                f"{_escape_table(item.get('request_timestamp', ''))}|"
                f"{_escape_table(item.get('cache_created_at', ''))}|"
                f"{_escape_table(item.get('available_time', ''))}|"
            )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _initialize_historical_csv(
    *,
    artifact_id: str,
    artifact_class: str,
    source_path: Path,
    replay_path: Path,
    as_of: date,
) -> ReplayInputRecord:
    if not source_path.exists():
        return ReplayInputRecord(
            artifact_id=artifact_id,
            artifact_class=artifact_class,
            source_path=str(source_path),
            replay_path=str(replay_path),
            status="MISSING_OPTIONAL",
            reason="history seed missing; replay command will create output if needed",
        )
    rows = _read_csv_dicts(source_path)
    fieldnames = tuple(rows[0].keys()) if rows else _read_csv_header(source_path)
    included: list[dict[str, str]] = []
    excluded = 0
    for row in rows:
        row_date = _parse_date_value(row.get("as_of"))
        if row_date is not None and row_date < as_of:
            included.append(row)
        else:
            excluded += 1
    _write_csv_dicts(replay_path, included, fieldnames=fieldnames)
    return ReplayInputRecord(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        source_path=str(source_path),
        replay_path=str(replay_path),
        status="PASS",
        row_count=len(rows),
        included_count=len(included),
        excluded_count=excluded,
        sha256=_sha256_file(replay_path),
        reason="seeded with rows whose as_of is earlier than replay date",
    )


def _write_input_manifest(
    records: list[ReplayInputRecord],
    paths: ReplayPaths,
) -> None:
    fieldnames = [
        "artifact_id",
        "artifact_class",
        "source_path",
        "replay_path",
        "status",
        "row_count",
        "included_count",
        "excluded_count",
        "sha256",
        "min_timestamp",
        "max_timestamp",
        "reason",
    ]
    paths.input_manifest_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with paths.input_manifest_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))
    paths.input_manifest_json_path.write_text(
        json.dumps([asdict(record) for record in records], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _build_replay_paths(
    *,
    as_of: date,
    output_root: Path,
    generated_at: datetime,
    label: str | None,
    run_id: str | None,
) -> ReplayPaths:
    resolved_run_id = run_id or _default_run_id(as_of, generated_at, label)
    root = output_root / as_of.isoformat() / resolved_run_id
    input_root = root / "input"
    output = root / "output"
    reports = output / "outputs" / "reports"
    return ReplayPaths(
        run_id=resolved_run_id,
        root=root,
        input_root=input_root,
        output_root=output,
        data_raw_dir=input_root / "data" / "raw",
        data_processed_dir=output / "data" / "processed",
        reports_dir=reports,
        logs_dir=root / "logs",
        input_manifest_csv_path=root / "input_freeze_manifest.csv",
        input_manifest_json_path=root / "input_freeze_manifest.json",
        run_report_path=root / "replay_run.md",
        run_json_path=root / "replay_run.json",
    )


def _create_replay_dirs(paths: ReplayPaths) -> None:
    for path in (
        paths.input_root,
        paths.output_root,
        paths.data_raw_dir,
        paths.data_processed_dir,
        paths.reports_dir,
        paths.logs_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)


def _default_run_id(as_of: date, generated_at: datetime, label: str | None) -> str:
    token = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    label_token = _slug(label or "")
    suffix = f"_{label_token}" if label_token else ""
    return f"replay_{as_of.isoformat()}_{token}{suffix}"


def _default_window_run_id(
    start: date,
    end: date,
    generated_at: datetime,
    label: str | None,
) -> str:
    token = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    label_token = _slug(label or "")
    suffix = f"_{label_token}" if label_token else ""
    return f"replay_window_{start.isoformat()}_{end.isoformat()}_{token}{suffix}"


def _default_visible_at(as_of: date) -> datetime:
    return datetime.combine(as_of, time.max, tzinfo=UTC)


def _resolve_replay_visible_at(
    *,
    as_of: date,
    project_root: Path,
    visible_at: datetime | None,
) -> tuple[datetime, str]:
    if visible_at is not None:
        return visible_at, "explicit"
    production_cutoff = _production_visibility_cutoff(as_of, project_root)
    if production_cutoff is not None:
        return production_cutoff, "production_daily_run_metadata"
    return _default_visible_at(as_of), "end_of_asof_utc"


def _production_visibility_cutoff(as_of: date, project_root: Path) -> datetime | None:
    metadata_path = default_daily_ops_run_metadata_path(
        project_root / "outputs" / "reports",
        as_of,
    )
    if not metadata_path.exists():
        return None
    try:
        raw = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return _parse_datetime_value(raw.get("visibility_cutoff"))


def _cache_only_env(env: Mapping[str, str] | None) -> dict[str, str]:
    checked_env = dict(os.environ if env is None else env)
    for key in (
        "OPENAI_API_KEY",
        "FMP_API_KEY",
        "MARKETSTACK_API_KEY",
        "CONGRESS_API_KEY",
        "GOVINFO_API_KEY",
    ):
        checked_env[key] = ""
    return checked_env


def _execution_command(command: tuple[str, ...]) -> tuple[str, ...]:
    if command and command[0] == "aits":
        return (sys.executable, "-m", "ai_trading_system.cli", *command[1:])
    return command


def _read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_csv_header(path: Path) -> tuple[str, ...]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            return tuple(next(reader))
        except StopIteration:
            return ()


def _write_csv_dicts(
    path: Path,
    rows: list[dict[str, str]],
    *,
    fieldnames: Iterable[str],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized_fieldnames = tuple(fieldnames)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=normalized_fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in normalized_fieldnames})


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _split_rows_by_timestamp(
    rows: list[dict[str, str]],
    visible_at: datetime,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[datetime]]:
    included: list[dict[str, str]] = []
    excluded: list[dict[str, str]] = []
    timestamps: list[datetime] = []
    for row in rows:
        timestamp = _row_timestamp(row)
        if timestamp is not None:
            timestamps.append(timestamp)
        if timestamp is not None and timestamp <= visible_at:
            included.append(row)
        else:
            excluded.append(row)
    return included, excluded, timestamps


def _row_timestamp(row: Mapping[str, str]) -> datetime | None:
    for key in ("available_time", "snapshot_time", "downloaded_at", "captured_at"):
        value = row.get(key) or ""
        parsed = _parse_datetime_value(value)
        if parsed is not None:
            return parsed
    return None


def _parse_datetime_value(value: object) -> datetime | None:
    text = str(value or "").strip().strip("'\"")
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        if "T" not in text and " " not in text:
            parsed_date = date.fromisoformat(text)
            return datetime.combine(parsed_date, time.max, tzinfo=UTC)
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_date_value(value: object) -> date | None:
    text = str(value or "").strip().strip("'\"")
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    return loaded if isinstance(loaded, dict) else {}


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _json_record_count(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(raw, list):
        return len(raw)
    if isinstance(raw, dict):
        for key in ("records", "items", "queue", "prereview_records"):
            value = raw.get(key)
            if isinstance(value, list):
                return len(value)
        return len(raw)
    return None


def _sha256_file(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _directory_digest(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    for file_path in sorted(item for item in path.rglob("*") if item.is_file()):
        digest.update(str(file_path.relative_to(path)).encode("utf-8"))
        file_digest = _sha256_file(file_path)
        if file_digest:
            digest.update(file_digest.encode("ascii"))
    return digest.hexdigest()


def _compare_file_artifact(
    *,
    artifact_id: str,
    artifact_class: str,
    production_path: Path,
    replay_path: Path,
) -> ReplayDiffArtifact:
    production_digest = _sha256_file(production_path)
    replay_digest = _sha256_file(replay_path)
    return ReplayDiffArtifact(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        production_path=str(production_path),
        replay_path=str(replay_path),
        status=_artifact_compare_status(production_digest, replay_digest),
        production_sha256=production_digest,
        replay_sha256=replay_digest,
        details="file checksum comparison",
    )


def _compare_csv_rows_artifact(
    *,
    artifact_id: str,
    artifact_class: str,
    production_path: Path,
    replay_path: Path,
    as_of: date,
) -> ReplayDiffArtifact:
    production_rows, production_digest = _csv_rows_digest(production_path, as_of)
    replay_rows, replay_digest = _csv_rows_digest(replay_path, as_of)
    return ReplayDiffArtifact(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        production_path=str(production_path),
        replay_path=str(replay_path),
        status=_artifact_compare_status(production_digest, replay_digest),
        production_sha256=production_digest,
        replay_sha256=replay_digest,
        production_row_count=production_rows,
        replay_row_count=replay_rows,
        details=f"filtered rows where as_of == {as_of.isoformat()}",
    )


def _compare_json_projection_artifact(
    *,
    artifact_id: str,
    artifact_class: str,
    production_path: Path,
    replay_path: Path,
    projection: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    details: str,
) -> ReplayDiffArtifact:
    production_digest = _json_projection_digest(production_path, projection)
    replay_digest = _json_projection_digest(replay_path, projection)
    return ReplayDiffArtifact(
        artifact_id=artifact_id,
        artifact_class=artifact_class,
        production_path=str(production_path),
        replay_path=str(replay_path),
        status=_artifact_compare_status(production_digest, replay_digest),
        production_sha256=production_digest,
        replay_sha256=replay_digest,
        details=details,
    )


def _artifact_compare_status(
    production_digest: str | None,
    replay_digest: str | None,
) -> str:
    if production_digest is None and replay_digest is None:
        return "MISSING_BOTH"
    if production_digest is None:
        return "MISSING_PRODUCTION"
    if replay_digest is None:
        return "MISSING_REPLAY"
    if production_digest == replay_digest:
        return "MATCH"
    return "DIFFER"


def _production_diff_status(artifacts: list[ReplayDiffArtifact]) -> str:
    statuses = {artifact.status for artifact in artifacts}
    if not statuses:
        return "NO_COMPARABLE_ARTIFACTS"
    if any(status.startswith("MISSING") for status in statuses):
        return "INCOMPLETE_DIFF"
    if "DIFFER" in statuses:
        return "DIFFER"
    return "MATCH"


def _csv_rows_digest(path: Path, as_of: date) -> tuple[int | None, str | None]:
    if not path.exists():
        return None, None
    rows = _read_csv_dicts(path)
    if not rows:
        payload = json.dumps([], ensure_ascii=False, sort_keys=True).encode("utf-8")
        return 0, hashlib.sha256(payload).hexdigest()
    date_column = next(
        (column for column in ("as_of", "signal_date", "decision_date") if column in rows[0]),
        None,
    )
    if date_column is None:
        return len(rows), _sha256_file(path)
    filtered_rows = [
        row for row in rows if _parse_date_value(row.get(date_column)) == as_of
    ]
    sorted_rows = sorted(
        filtered_rows,
        key=lambda row: json.dumps(row, ensure_ascii=False, sort_keys=True),
    )
    payload = json.dumps(sorted_rows, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return len(filtered_rows), hashlib.sha256(payload).hexdigest()


def _json_projection_digest(
    path: Path,
    projection: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> str | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _sha256_file(path)
    source = raw if isinstance(raw, dict) else {}
    payload = json.dumps(
        projection(source),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _decision_snapshot_core_fields(raw: Mapping[str, Any]) -> Mapping[str, Any]:
    scores = _mapping_value(raw.get("scores"))
    positions = _mapping_value(raw.get("positions"))
    market_regime = _mapping_value(raw.get("market_regime"))
    return {
        "signal_date": raw.get("signal_date"),
        "market_regime_id": market_regime.get("regime_id"),
        "scores": {
            "overall_score": scores.get("overall_score"),
            "confidence_score": scores.get("confidence_score"),
            "confidence_level": scores.get("confidence_level"),
        },
        "positions": {
            "model_risk_asset_ai_band": positions.get("model_risk_asset_ai_band"),
            "final_risk_asset_ai_band": positions.get("final_risk_asset_ai_band"),
            "confidence_adjusted_risk_asset_ai_band": positions.get(
                "confidence_adjusted_risk_asset_ai_band"
            ),
            "total_asset_ai_band": positions.get("total_asset_ai_band"),
            "final_total_risk_asset_band": positions.get("final_total_risk_asset_band"),
            "position_gates": _position_gate_summary(
                positions.get("position_gates")
            ),
        },
        "execution": raw.get("execution"),
        "recommendation": raw.get("recommendation"),
    }


def _position_gate_summary(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    gates: list[Mapping[str, Any]] = []
    for item in value:
        gate = _mapping_value(item)
        gates.append(
            {
                "gate_id": gate.get("gate_id"),
                "gate_class": gate.get("gate_class"),
                "execution_effect": gate.get("execution_effect"),
                "min_position": gate.get("min_position"),
                "max_position": gate.get("max_position"),
            }
        )
    return gates


def _mapping_value(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _replay_run_to_json(replay_run: HistoricalReplayRun) -> dict[str, Any]:
    return {
        "status": replay_run.status,
        "as_of": replay_run.as_of.isoformat(),
        "mode": replay_run.mode,
        "run_id": replay_run.run_id,
        "generated_at": replay_run.generated_at.isoformat(),
        "visible_at": replay_run.visible_at.isoformat(),
        "cutoff_policy": replay_run.cutoff_policy,
        "inventory_only": replay_run.inventory_only,
        "allow_incomplete": replay_run.allow_incomplete,
        "label": replay_run.label,
        "openai_replay_policy": replay_run.openai_replay_policy,
        "paths": {key: str(value) for key, value in asdict(replay_run.paths).items()},
        "errors": list(replay_run.errors),
        "input_records": [asdict(record) for record in replay_run.input_records],
        "command_results": [
            {
                **asdict(result),
                "started_at": (
                    None if result.started_at is None else result.started_at.isoformat()
                ),
                "ended_at": None if result.ended_at is None else result.ended_at.isoformat(),
            }
            for result in replay_run.command_results
        ],
        "production_diff": (
            None
            if replay_run.production_diff is None
            else _production_diff_to_json(replay_run.production_diff)
        ),
    }


def _production_diff_to_json(diff: ReplayProductionDiff) -> dict[str, Any]:
    return {
        "status": diff.status,
        "as_of": diff.as_of.isoformat(),
        "generated_at": diff.generated_at.isoformat(),
        "report_path": str(diff.report_path),
        "json_path": str(diff.json_path),
        "artifacts": [asdict(artifact) for artifact in diff.artifacts],
    }


def _replay_window_to_json(window_run: HistoricalReplayWindowRun) -> dict[str, Any]:
    return {
        "status": window_run.status,
        "start": window_run.start.isoformat(),
        "end": window_run.end.isoformat(),
        "mode": window_run.mode,
        "run_id": window_run.run_id,
        "generated_at": window_run.generated_at.isoformat(),
        "report_path": str(window_run.report_path),
        "json_path": str(window_run.json_path),
        "continue_on_failure": window_run.continue_on_failure,
        "label": window_run.label,
        "day_runs": [
            {
                "as_of": replay_run.as_of.isoformat(),
                "status": replay_run.status,
                "run_id": replay_run.run_id,
                "bundle_path": str(replay_run.paths.root),
                "report_path": str(replay_run.paths.run_report_path),
                "diff_status": (
                    None
                    if replay_run.production_diff is None
                    else replay_run.production_diff.status
                ),
                "diff_report_path": (
                    None
                    if replay_run.production_diff is None
                    else str(replay_run.production_diff.report_path)
                ),
            }
            for replay_run in window_run.day_runs
        ],
        "skipped_dates": [
            {
                "as_of": skipped.as_of.isoformat(),
                "reason": skipped.reason,
            }
            for skipped in window_run.skipped_dates
        ],
    }


def _daily_score_report_path(paths: ReplayPaths, as_of: date) -> Path:
    return default_daily_score_report_path(paths.reports_dir, as_of)


def _alert_report_path(paths: ReplayPaths, as_of: date) -> Path:
    return default_alert_report_path(paths.reports_dir, as_of)


def _trace_bundle_path(paths: ReplayPaths, as_of: date) -> Path:
    return default_report_trace_bundle_path(_daily_score_report_path(paths, as_of))


def _decision_snapshot_path(paths: ReplayPaths, as_of: date) -> Path:
    return default_decision_snapshot_path(
        paths.output_root / DEFAULT_DECISION_SNAPSHOT_DIR.relative_to(PROJECT_ROOT),
        as_of,
    )


def _dashboard_html_path(paths: ReplayPaths, as_of: date) -> Path:
    return default_evidence_dashboard_path(paths.reports_dir, as_of)


def _dashboard_json_path(paths: ReplayPaths, as_of: date) -> Path:
    return default_evidence_dashboard_json_path(paths.reports_dir, as_of)


def _pipeline_health_report_path(paths: ReplayPaths, as_of: date) -> Path:
    return default_pipeline_health_report_path(paths.reports_dir, as_of)


def _secret_report_path(paths: ReplayPaths, as_of: date) -> Path:
    return default_secret_scan_report_path(paths.reports_dir, as_of)


def _slug(value: str) -> str:
    chars = [char.lower() if char.isalnum() else "_" for char in value.strip()]
    slug = "".join(chars).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug[:48]


def _iso_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _display_int(value: int | None) -> str:
    return "" if value is None else str(value)


def _display_return_code(value: int | None) -> str:
    return "" if value is None else str(value)


def _short_digest(value: str | None) -> str:
    return "" if value is None else value[:12]


def _escape_table(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")
