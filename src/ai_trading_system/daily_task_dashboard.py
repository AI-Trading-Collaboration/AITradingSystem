from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from html import escape
from pathlib import Path
from urllib.parse import quote

from ai_trading_system.reports.daily_task_dashboard_view_model import (
    DailyTaskDashboardReport,
    DailyTaskDetail,
    DailyTaskKeyConclusion,
    TraceRecord,
)


@dataclass(frozen=True)
class _ReportSpec:
    label: str
    filename: str
    required: bool = True


_STEP_TITLES: dict[str, str] = {
    "input_visibility": "输入可见性预检查",
    "download_data": "更新市场和宏观缓存",
    "pit_snapshots": "抓取并校验 forward-only PIT 快照",
    "official_policy_sources": "抓取官方政策/地缘来源",
    "sec_companyfacts": "刷新 SEC companyfacts 原始缓存",
    "sec_metrics": "抽取当日 SEC 基本面指标",
    "tsm_ir_sec_metrics_merge": "合并 TSMC IR 季度指标",
    "sec_metrics_validation": "校验当日 SEC 基本面指标 CSV",
    "valuation_snapshots": "刷新 FMP 估值和预期快照",
    "score_daily": "生成每日评分、日报、trace 和告警",
    "parameter_governance": "生成参数配置治理报告",
    "market_feedback_optimization": "生成市场反馈优化复盘报告",
    "feedback_loop_review": "生成反馈闭环周期复核报告",
    "investment_weekly_review": "生成投资周度复盘报告",
    "reports_dashboard": "生成只读决策 dashboard",
    "pipeline_health": "检查关键 artifact 和 PIT 健康",
    "secret_hygiene": "扫描报告和配置中的疑似 secret",
}


def default_daily_task_dashboard_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_task_dashboard_{as_of.isoformat()}.html"


def default_daily_task_dashboard_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_task_dashboard_{as_of.isoformat()}.json"


def build_daily_task_dashboard_report(
    *,
    as_of: date,
    metadata_path: Path,
    reports_dir: Path,
    run_report_path: Path | None = None,
) -> DailyTaskDashboardReport:
    metadata = _read_metadata(metadata_path)
    commands = _records_by_step(metadata.get("commands", []))
    results = list(_records(metadata.get("step_results", [])))
    tasks = tuple(
        _build_task_detail(
            result=result,
            command=commands.get(str(result.get("step_id")), {}),
            as_of=as_of,
            reports_dir=reports_dir,
        )
        for result in results
    )
    raw_git = metadata.get("git")
    git: dict[str, object] = raw_git if isinstance(raw_git, dict) else {}
    raw_git_dirty = git.get("dirty")
    return DailyTaskDashboardReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        run_id=str(metadata.get("run_id") or ""),
        status=str(metadata.get("status") or "UNKNOWN"),
        metadata_path=metadata_path,
        run_report_path=run_report_path,
        reports_dir=reports_dir,
        project_root=Path(str(metadata.get("project_root") or Path.cwd())),
        started_at=str(metadata.get("started_at") or ""),
        finished_at=str(metadata.get("finished_at") or ""),
        visibility_cutoff=str(metadata.get("visibility_cutoff") or ""),
        input_visibility_status=str(metadata.get("input_visibility_status") or ""),
        git_commit=str(git.get("commit") or ""),
        git_dirty=raw_git_dirty if isinstance(raw_git_dirty, bool) else None,
        tasks=tasks,
    )


def write_daily_task_dashboard(
    report: DailyTaskDashboardReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_daily_task_dashboard(report), encoding="utf-8")
    return output_path


def write_daily_task_dashboard_json(
    report: DailyTaskDashboardReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_daily_task_dashboard_payload(report)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def build_daily_task_dashboard_payload(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    return {
        "as_of": report.as_of.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "status": report.status,
        "production_effect": report.production_effect,
        "run_id": report.run_id,
        "metadata_path": str(report.metadata_path),
        "run_report_path": None
        if report.run_report_path is None
        else str(report.run_report_path),
        "reports_dir": str(report.reports_dir),
        "project_root": str(report.project_root),
        "started_at": report.started_at,
        "finished_at": report.finished_at,
        "visibility_cutoff": report.visibility_cutoff,
        "input_visibility_status": report.input_visibility_status,
        "git": {
            "commit": report.git_commit,
            "dirty": report.git_dirty,
        },
        "summary": {
            "task_count": len(report.tasks),
            "failed_count": report.failed_count,
            "skipped_count": report.skipped_count,
            "risk_count": report.risk_count,
        },
        "key_conclusions": [
            {
                "area": conclusion.area,
                "title": conclusion.title,
                "status": conclusion.status,
                "primary": conclusion.primary,
                "supporting": list(conclusion.supporting),
                "important_risk": conclusion.important_risk,
                "risk_level": conclusion.risk_level,
                "source_steps": list(conclusion.source_steps),
                "result_comparison": list(conclusion.result_comparison),
                "result_methodology": conclusion.result_methodology,
                "parameter_comparison": list(conclusion.parameter_comparison),
            }
            for conclusion in _build_key_conclusions(report)
        ],
        "tasks": [
            {
                "step_id": task.step_id,
                "title": task.title,
                "status": task.status,
                "conclusion": task.conclusion,
                "important_risk": task.important_risk,
                "risk_level": task.risk_level,
                "duration_seconds": task.duration_seconds,
                "return_code": task.return_code,
                "stdout_line_count": task.stdout_line_count,
                "stderr_line_count": task.stderr_line_count,
                "command": task.command,
                "input_visibility": task.input_visibility,
                "blocks_downstream": task.blocks_downstream,
                "detail_reports": list(task.detail_reports),
            }
            for task in report.tasks
        ],
    }


def render_daily_task_dashboard(report: DailyTaskDashboardReport) -> str:
    title = f"每日任务展示 Dashboard {report.as_of.isoformat()}"
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="zh-Hans">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>{_text(title)}</title>",
            f"<style>{_CSS}</style>",
            "</head>",
            "<body>",
            _render_header(report, title),
            "<main>",
            _render_key_conclusions(report),
            _render_risks(report),
            _render_summary(report),
            _render_task_table(report),
            _render_task_details(report),
            "</main>",
            _render_footer(report),
            "</body>",
            "</html>",
            "",
        ]
    )


def _build_task_detail(
    *,
    result: TraceRecord,
    command: TraceRecord,
    as_of: date,
    reports_dir: Path,
) -> DailyTaskDetail:
    step_id = str(result.get("step_id") or "")
    status = str(result.get("status") or "UNKNOWN")
    detail_reports = _collect_detail_reports(step_id, as_of, reports_dir)
    conclusion = _task_conclusion(
        step_id=step_id,
        status=status,
        result=result,
        detail_reports=detail_reports,
        reports_dir=reports_dir,
        as_of=as_of,
    )
    risk, risk_level = _task_risk(
        status=status,
        result=result,
        command=command,
        detail_reports=detail_reports,
    )
    return DailyTaskDetail(
        step_id=step_id,
        title=_STEP_TITLES.get(step_id, step_id),
        status=status,
        conclusion=conclusion,
        important_risk=risk,
        risk_level=risk_level,
        duration_seconds=_optional_float(result.get("duration_seconds")),
        return_code=_optional_int(result.get("return_code")),
        stdout_line_count=_optional_int(result.get("stdout_line_count")) or 0,
        stderr_line_count=_optional_int(result.get("stderr_line_count")) or 0,
        command=str(command.get("command") or ""),
        input_visibility=str(command.get("input_visibility") or ""),
        blocks_downstream=bool(command.get("blocks_downstream")),
        detail_reports=detail_reports,
    )


def _task_conclusion(
    *,
    step_id: str,
    status: str,
    result: TraceRecord,
    detail_reports: tuple[TraceRecord, ...],
    reports_dir: Path,
    as_of: date,
) -> str:
    if status == "SKIPPED":
        return "本轮显式跳过或休市日模式不适用。"
    if status == "FAIL":
        error = str(result.get("error") or "").strip()
        return f"步骤失败；{error}" if error else "步骤失败；下游应停止或降级。"

    report_values = _merge_report_values(detail_reports)
    if step_id == "pit_snapshots":
        rows = report_values.get("标准化行数") or report_values.get("原始记录数")
        status_text = _joined_report_status(detail_reports)
        return _join_nonempty(["PIT 抓取和校验完成", status_text, _label("行数", rows)])
    if step_id in {"sec_companyfacts", "sec_metrics", "tsm_ir_sec_metrics_merge"}:
        status_text = _joined_report_status(detail_reports)
        return _join_nonempty(["SEC/TSM 基本面输入准备完成", status_text])
    if step_id == "sec_metrics_validation":
        status_text = _joined_report_status(detail_reports)
        return _join_nonempty(["SEC-style 指标校验完成", status_text])
    if step_id == "valuation_snapshots":
        snapshots = report_values.get("估值快照") or report_values.get("快照数")
        status_text = _joined_report_status(detail_reports)
        return _join_nonempty(["估值和预期快照刷新完成", status_text, snapshots])
    if step_id == "score_daily":
        decision = _read_decision_summary(reports_dir, as_of)
        if decision:
            return decision
        return _join_nonempty(["日报评分完成", _joined_report_status(detail_reports)])
    if step_id == "parameter_governance":
        action = report_values.get("Action 分布")
        owner_input = report_values.get("Owner quantitative input")
        return _join_nonempty(
            [
                "参数治理报告完成",
                _joined_report_status(detail_reports),
                _label("Owner quantitative input", owner_input),
                _label("Action 分布", action),
            ]
        )
    if step_id == "market_feedback_optimization":
        readiness = report_values.get("Readiness")
        decision_samples = report_values.get("Decision outcome 可用样本")
        return _join_nonempty(["市场反馈优化复盘完成", readiness, decision_samples])
    if step_id == "feedback_loop_review":
        available = report_values.get("可用 outcome")
        prediction = report_values.get("可用 prediction outcome")
        return _join_nonempty(
            [
                "反馈闭环复核完成",
                _label("Decision outcome", available),
                _label("Prediction outcome", prediction),
            ]
        )
    if step_id == "investment_weekly_review":
        score_change = _first_report_bullet(detail_reports, "## 本期结论是否变化")
        gates = _metadata_value_from_reports(detail_reports, "最新触发 gate")
        return _join_nonempty(["投资周度复盘完成", score_change, _label("最新 gate", gates)])
    if step_id == "reports_dashboard":
        return _join_nonempty(
            [
                "决策 evidence dashboard 已生成",
                _joined_report_status(detail_reports),
            ]
        )
    if step_id == "pipeline_health":
        checks = report_values.get("检查项")
        errors = report_values.get("错误数")
        warnings = report_values.get("警告数")
        return _join_nonempty(
            [
                "运行健康检查完成",
                _joined_report_status(detail_reports),
                _label("检查项", checks),
                _label(
                    "错误/警告",
                    _join_nonempty([errors, warnings], separator="/"),
                ),
            ]
        )
    if step_id == "secret_hygiene":
        files = report_values.get("扫描文件数")
        errors = report_values.get("错误数")
        warnings = report_values.get("警告数")
        return _join_nonempty(
            [
                "secret hygiene 扫描完成",
                _label("扫描文件", files),
                _label(
                    "错误/警告",
                    _join_nonempty([errors, warnings], separator="/"),
                ),
            ]
        )
    if step_id == "download_data":
        status_text = _joined_report_status(detail_reports)
        return _join_nonempty(["市场和宏观缓存更新完成", status_text])
    if step_id == "official_policy_sources":
        return _join_nonempty(["官方政策/地缘来源抓取完成", _joined_report_status(detail_reports)])
    return "步骤完成。"


def _task_risk(
    *,
    status: str,
    result: TraceRecord,
    command: TraceRecord,
    detail_reports: tuple[TraceRecord, ...],
) -> tuple[str, str]:
    risks: list[str] = []
    high = False
    if status == "FAIL":
        high = True
        error = str(result.get("error") or "").strip()
        risks.append(error or "步骤失败，后续结论不能视为完整。")
    elif status == "SKIPPED":
        reason = str(command.get("skip_reason") or "").strip()
        risks.append(reason or "步骤未执行，需要确认是否为预期跳过。")

    stderr_lines = _optional_int(result.get("stderr_line_count")) or 0
    if stderr_lines:
        risks.append(f"stderr 行数为 {stderr_lines}，需要查看子报告定位。")

    for report in detail_reports:
        if not report["exists"]:
            if status != "SKIPPED" and report.get("required", True):
                risks.append(f"详情报告缺失：{report['label']}。")
            continue
        report_status = str(report.get("status") or "")
        if report_status in {"FAIL", "BLOCKED", "BLOCKED_ENV", "BLOCKED_VISIBILITY"}:
            high = True
            risks.append(f"{report['label']} 状态为 {report_status}。")
        elif report_status.startswith("PASS_WITH_") or report_status in {
            "ACTIVE_WARNINGS",
            "READY_FOR_WEIGHT_DIAGNOSTIC_REVIEW",
        }:
            risks.append(f"{report['label']} 状态为 {report_status}，需阅读限制说明。")

    if not risks:
        return "未发现该步骤的阻断风险。", "none"
    return "；".join(dict.fromkeys(risks)), "high" if high else "medium"


def _collect_detail_reports(
    step_id: str,
    as_of: date,
    reports_dir: Path,
) -> tuple[TraceRecord, ...]:
    specs = _report_specs(as_of).get(step_id, ())
    records: list[TraceRecord] = []
    for spec in specs:
        path = reports_dir / spec.filename
        text = path.read_text(encoding="utf-8") if path.exists() else None
        values = _report_metadata_values(text or "")
        records.append(
            {
                "label": spec.label,
                "path": str(path),
                "href": _report_href(path, reports_dir),
                "exists": path.exists(),
                "required": spec.required,
                "status": values.get("状态") or ("PRESENT" if path.exists() else "MISSING"),
                "values": values,
            }
        )
    return tuple(records)


def _report_specs(as_of: date) -> dict[str, tuple[_ReportSpec, ...]]:
    suffix = as_of.isoformat()
    return {
        "download_data": (
            _ReportSpec(
                "下载诊断",
                f"download_data_diagnostics_{suffix}.md",
                required=False,
            ),
            _ReportSpec("数据质量门禁", f"data_quality_{suffix}.md"),
        ),
        "pit_snapshots": (
            _ReportSpec("FMP PIT 抓取报告", f"fmp_forward_pit_fetch_{suffix}.md"),
            _ReportSpec("PIT 快照质量报告", f"pit_snapshots_validation_{suffix}.md"),
        ),
        "official_policy_sources": (
            _ReportSpec("官方政策来源抓取", f"official_policy_sources_{suffix}.md"),
        ),
        "sec_companyfacts": (
            _ReportSpec("SEC companyfacts 校验", f"sec_companyfacts_validation_{suffix}.md"),
        ),
        "sec_metrics": (
            _ReportSpec("SEC fundamentals", f"sec_fundamentals_{suffix}.md"),
        ),
        "tsm_ir_sec_metrics_merge": (
            _ReportSpec("SEC fundamentals 校验", f"sec_fundamentals_validation_{suffix}.md"),
        ),
        "sec_metrics_validation": (
            _ReportSpec("SEC fundamentals 校验", f"sec_fundamentals_validation_{suffix}.md"),
        ),
        "valuation_snapshots": (
            _ReportSpec("FMP valuation 抓取", f"fmp_valuation_fetch_{suffix}.md"),
            _ReportSpec("估值校验", f"valuation_validation_{suffix}.md"),
        ),
        "score_daily": (
            _ReportSpec("数据质量门禁", f"data_quality_{suffix}.md"),
            _ReportSpec("每日评分报告", f"daily_score_{suffix}.md"),
            _ReportSpec("告警报告", f"alerts_{suffix}.md"),
        ),
        "parameter_governance": (
            _ReportSpec("参数治理", f"parameter_governance_{suffix}.md"),
        ),
        "market_feedback_optimization": (
            _ReportSpec("市场反馈优化", f"market_feedback_optimization_{suffix}.md"),
        ),
        "feedback_loop_review": (
            _ReportSpec("反馈闭环复核", f"feedback_loop_review_{suffix}.md"),
        ),
        "investment_weekly_review": (
            _ReportSpec("投资周度复盘", f"investment_weekly_review_{suffix}.md"),
        ),
        "reports_dashboard": (
            _ReportSpec("Evidence dashboard", f"evidence_dashboard_{suffix}.html"),
            _ReportSpec("Evidence dashboard JSON", f"evidence_dashboard_{suffix}.json"),
        ),
        "pipeline_health": (
            _ReportSpec("Pipeline health", f"pipeline_health_{suffix}.md"),
            _ReportSpec("Pipeline health alerts", f"pipeline_health_alerts_{suffix}.md"),
        ),
        "secret_hygiene": (
            _ReportSpec("Secret hygiene", f"secret_hygiene_{suffix}.md"),
        ),
    }


def _read_decision_summary(reports_dir: Path, as_of: date) -> str:
    path = reports_dir / f"evidence_dashboard_{as_of.isoformat()}.json"
    if not path.exists():
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return ""
    decision = payload.get("decision") if isinstance(payload, dict) else None
    if not isinstance(decision, dict):
        return ""
    return _join_nonempty(
        [
            _label("执行动作", decision.get("action")),
            _label("最终 AI 仓位", decision.get("final_risk_asset_ai_position")),
            _label("置信度", decision.get("confidence")),
            _label("Data Gate", decision.get("data_gate")),
        ]
    )


def _build_key_conclusions(
    report: DailyTaskDashboardReport,
) -> tuple[DailyTaskKeyConclusion, ...]:
    tasks = {task.step_id: task for task in report.tasks}
    dashboard_payload = _read_evidence_dashboard_payload(report)
    conclusions = [
        _investment_key_conclusion(tasks, dashboard_payload),
        _data_key_conclusion(tasks),
        _parameter_key_conclusion(tasks),
        _feedback_key_conclusion(report, tasks, dashboard_payload),
        _operations_key_conclusion(tasks),
    ]
    return tuple(conclusion for conclusion in conclusions if conclusion is not None)


def _investment_key_conclusion(
    tasks: dict[str, DailyTaskDetail],
    dashboard_payload: TraceRecord,
) -> DailyTaskKeyConclusion | None:
    score_task = tasks.get("score_daily")
    dashboard_task = tasks.get("reports_dashboard")
    if score_task is None and dashboard_task is None:
        return None
    decision = _mapping_value(dashboard_payload, "decision")
    supporting = _string_list(dashboard_payload.get("top_supporting_evidence"), limit=3)
    invalidators = _string_list(dashboard_payload.get("top_invalidators"), limit=2)
    largest_constraint = _string_value(decision.get("largest_constraint"))
    primary = _join_nonempty(
        [
            _label("执行动作", decision.get("action")),
            _label("最终 AI 仓位", decision.get("final_risk_asset_ai_position")),
            _label("置信度", decision.get("confidence")),
            _label("Data Gate", decision.get("data_gate")),
        ]
    )
    if not primary and score_task is not None:
        primary = score_task.conclusion
    risk = _join_nonempty([largest_constraint, *invalidators])
    if not risk and score_task is not None:
        risk = score_task.important_risk
    source_tasks = tuple(
        step for step in ("score_daily", "reports_dashboard") if step in tasks
    )
    return DailyTaskKeyConclusion(
        area="投资结论",
        title="当日动作、仓位与主要约束",
        status=_combined_status(_existing_tasks(tasks, source_tasks)),
        primary=primary or "未生成当日投资结论。",
        supporting=supporting,
        important_risk=risk or "未发现投资结论层面的新增阻断风险。",
        risk_level=_combined_risk_level(_existing_tasks(tasks, source_tasks)),
        source_steps=source_tasks,
    )


def _data_key_conclusion(
    tasks: dict[str, DailyTaskDetail],
) -> DailyTaskKeyConclusion | None:
    source_steps = (
        "download_data",
        "pit_snapshots",
        "sec_companyfacts",
        "sec_metrics",
        "sec_metrics_validation",
        "valuation_snapshots",
    )
    data_tasks = _existing_tasks(tasks, source_steps)
    if not data_tasks:
        return None
    data_quality = _first_detail_status(data_tasks, "数据质量门禁")
    pit_status = _joined_task_report_status(tasks.get("pit_snapshots"))
    sec_status = _join_nonempty(
        [
            _joined_task_report_status(tasks.get("sec_companyfacts")),
            _joined_task_report_status(tasks.get("sec_metrics")),
            _joined_task_report_status(tasks.get("sec_metrics_validation")),
        ]
    )
    valuation_status = _joined_task_report_status(tasks.get("valuation_snapshots"))
    supporting = tuple(
        task.conclusion
        for task in data_tasks
        if task.step_id in {"pit_snapshots", "sec_metrics_validation", "valuation_snapshots"}
    )
    return DailyTaskKeyConclusion(
        area="数据可信度",
        title="市场、PIT、SEC 与估值输入",
        status=_combined_status(data_tasks),
        primary=_join_nonempty(
            [
                _label("Data Gate", data_quality),
                _label("PIT", pit_status),
                _label("SEC", sec_status),
                _label("估值", valuation_status),
            ]
        )
        or "关键数据输入结论缺失，需要查看子报告。",
        supporting=supporting[:3],
        important_risk=_combined_task_risk(data_tasks),
        risk_level=_combined_risk_level(data_tasks),
        source_steps=tuple(task.step_id for task in data_tasks),
    )


def _parameter_key_conclusion(
    tasks: dict[str, DailyTaskDetail],
) -> DailyTaskKeyConclusion | None:
    task = tasks.get("parameter_governance")
    if task is None:
        return None
    values = _merge_report_values(task.detail_reports)
    supporting = tuple(
        value
        for value in (
            _label("Owner quantitative input", values.get("Owner quantitative input")),
            _label("Action 分布", values.get("Action 分布")),
        )
        if value
    )
    return DailyTaskKeyConclusion(
        area="参数治理",
        title="参数候选和上线边界",
        status=_combined_status((task,)),
        primary=task.conclusion,
        supporting=supporting,
        important_risk=task.important_risk,
        risk_level=task.risk_level,
        source_steps=(task.step_id,),
    )


def _feedback_key_conclusion(
    report: DailyTaskDashboardReport,
    tasks: dict[str, DailyTaskDetail],
    dashboard_payload: TraceRecord,
) -> DailyTaskKeyConclusion | None:
    source_steps = (
        "market_feedback_optimization",
        "feedback_loop_review",
        "investment_weekly_review",
    )
    feedback_tasks = _existing_tasks(tasks, source_steps)
    feedback = _mapping_value(dashboard_payload, "feedback_review")
    market = _mapping_value(feedback, "market_feedback")
    loop = _mapping_value(feedback, "feedback_loop")
    investment = _mapping_value(feedback, "investment_review")
    shadow_parameter = _latest_shadow_parameter_summary(report)
    if not feedback_tasks and not shadow_parameter:
        return None
    primary = _join_nonempty(
        [
            _label("Readiness", market.get("readiness")),
            _string_value(market.get("current_conclusion")),
            _label("周度仓位变化", investment.get("risk_asset_position_change")),
            _string_value(shadow_parameter.get("primary")),
        ]
    )
    supporting = tuple(
        value
        for value in (
            _label("Decision outcome", market.get("decision_available_sample_summary")),
            _label("Prediction outcome", market.get("prediction_available_sample_summary")),
            _label("Loop outcome", loop.get("outcome_summary")),
            _label("最新 gates", investment.get("latest_gates")),
            _string_value(shadow_parameter.get("driver_summary")),
            _string_value(shadow_parameter.get("parameter_summary")),
        )
        if value
    )
    if not primary:
        primary = "；".join(task.conclusion for task in feedback_tasks if task.conclusion)
    base_risk = _combined_task_risk(feedback_tasks) if feedback_tasks else ""
    return DailyTaskKeyConclusion(
        area="反馈复盘",
        title="样本、学习闭环、周度判断和 shadow 参数",
        status=_combined_status(feedback_tasks)
        if feedback_tasks
        else "PASS_WITH_LIMITATIONS",
        primary=primary or "反馈复盘结论缺失，需要查看子报告。",
        supporting=supporting[:6],
        important_risk=_join_nonempty(
            [
                base_risk,
                _string_value(shadow_parameter.get("risk")),
            ]
        ),
        risk_level=_max_risk_level(
            _combined_risk_level(feedback_tasks),
            _string_value(shadow_parameter.get("risk_level")),
        ),
        source_steps=(
            *tuple(task.step_id for task in feedback_tasks),
            *(
                ("shadow_parameter_search",)
                if shadow_parameter.get("connected") is True
                else ()
            ),
        ),
        result_comparison=tuple(
            row
            for row in shadow_parameter.get("result_comparison", ())
            if isinstance(row, dict)
        ),
        result_methodology=_string_value(shadow_parameter.get("result_methodology")),
        parameter_comparison=tuple(
            row
            for row in shadow_parameter.get("parameter_comparison", ())
            if isinstance(row, dict)
        ),
    )


def _operations_key_conclusion(
    tasks: dict[str, DailyTaskDetail],
) -> DailyTaskKeyConclusion | None:
    source_steps = ("pipeline_health", "secret_hygiene")
    operation_tasks = _existing_tasks(tasks, source_steps)
    if not operation_tasks:
        return None
    primary = "；".join(task.conclusion for task in operation_tasks if task.conclusion)
    return DailyTaskKeyConclusion(
        area="运行健康",
        title="Pipeline health 与 secret hygiene",
        status=_combined_status(operation_tasks),
        primary=primary or "运行健康结论缺失，需要查看子报告。",
        supporting=(),
        important_risk=_combined_task_risk(operation_tasks),
        risk_level=_combined_risk_level(operation_tasks),
        source_steps=tuple(task.step_id for task in operation_tasks),
    )


def _read_evidence_dashboard_payload(report: DailyTaskDashboardReport) -> TraceRecord:
    path = report.reports_dir / f"evidence_dashboard_{report.as_of.isoformat()}.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_shadow_parameter_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    search_root = report.project_root / "outputs" / "parameter_search"
    if not search_root.exists():
        return {}
    candidates: list[tuple[datetime, Path, TraceRecord]] = []
    for manifest_path in search_root.glob("*/manifest.json"):
        manifest = _read_json_object(manifest_path)
        if manifest.get("report_type") != "shadow_parameter_search":
            continue
        window = _mapping_value(manifest, "search_window")
        window_end = _parse_iso_date(_string_value(window.get("end")))
        if window_end is not None and window_end > report.as_of:
            continue
        generated_at = _parse_iso_datetime(_string_value(manifest.get("generated_at")))
        if generated_at is None:
            generated_at = datetime.fromtimestamp(manifest_path.stat().st_mtime, tz=UTC)
        candidates.append((generated_at, manifest_path, manifest))
    if not candidates:
        return {}

    _generated_at, manifest_path, manifest = max(candidates, key=lambda item: item[0])
    selected_trial_id = _string_value(manifest.get("best_trial_id")) or _string_value(
        manifest.get("best_diagnostic_trial_id")
    )
    if not selected_trial_id:
        return {}
    output_dir = manifest_path.parent
    trial = _read_trial_row(output_dir / "trials.csv", selected_trial_id)
    if not trial:
        return {}
    baseline_trial_id = _string_value(
        _mapping_value(manifest, "factorial_attribution").get("baseline_trial_id")
    )
    baseline = _read_trial_row(
        output_dir / "trials.csv",
        baseline_trial_id or "source_current__production_observed_gates",
    )
    promotion = _read_latest_shadow_parameter_promotion(output_dir)
    promotion_status = _string_value(promotion.get("status")) or "NOT_EVALUATED"
    eligible = _string_value(trial.get("eligible")).lower() == "true"
    selected_kind = "eligible best" if eligible else "diagnostic-leading"
    production_return = _optional_float(trial.get("production_total_return"))
    shadow_return = _optional_float(trial.get("shadow_total_return"))
    excess_return = _optional_float(trial.get("excess_total_return"))
    available_count = _optional_int(trial.get("available_count"))
    total_count = _optional_int(trial.get("total_count"))
    primary_driver = _string_value(
        _mapping_value(manifest, "factorial_attribution").get("primary_driver")
    )
    cap_summary = _primary_cap_summary(manifest)
    gate_caps = _json_mapping_from_cell(trial.get("gate_cap_overrides_json"))
    target_weights = _json_mapping_from_cell(trial.get("target_weights_json"))
    baseline_gate_caps = _json_mapping_from_cell(baseline.get("gate_cap_overrides_json"))
    baseline_weights = _json_mapping_from_cell(baseline.get("target_weights_json"))
    production_gate_caps = _production_gate_cap_observations(manifest)
    parameter_comparison = _shadow_parameter_comparison_rows(
        baseline_weights=baseline_weights,
        candidate_weights=target_weights,
        baseline_gate_caps=baseline_gate_caps,
        candidate_gate_caps=gate_caps,
        production_gate_caps=production_gate_caps,
        manifest=manifest,
    )
    result_comparison = _shadow_result_comparison_rows(trial=trial, manifest=manifest)
    result_methodology = _shadow_result_methodology(manifest)
    blockers = _promotion_blockers(promotion)
    sample_summary = _join_nonempty(
        [
            _label("available", available_count),
            _label("total", total_count),
            _label("pending", trial.get("pending_count")),
            _label("missing", trial.get("missing_count")),
        ],
        separator="，",
    )
    primary = (
        "Shadow parameter search："
        f"{selected_kind} `{selected_trial_id}`，"
        f"shadow return {_format_percent(shadow_return)} vs production "
        f"{_format_percent(production_return)}，excess {_format_signed_percent(excess_return)}，"
        f"promotion={promotion_status}。"
    )
    driver_summary = _join_nonempty(
        [
            _label("主因", primary_driver),
            cap_summary,
            _label("样本", sample_summary),
        ]
    )
    parameter_summary = _join_nonempty(
        [
            _label("Gate caps", _format_percent_mapping(gate_caps)),
            _label("Weights", _format_percent_mapping(target_weights)),
        ]
    )
    risk = _join_nonempty(
        [
            (
                "当前没有 eligible trial，诊断领先结果不得进入 production。"
                if not eligible
                else ""
            ),
            _label("Promotion", promotion_status),
            "；".join(blockers[:3]),
        ]
    )
    return {
        "connected": True,
        "run_id": _string_value(manifest.get("run_id")),
        "path": str(output_dir),
        "search_report_path": str(output_dir / "search_report.md"),
        "promotion_status": promotion_status,
        "selected_trial_id": selected_trial_id,
        "selected_kind": selected_kind,
        "primary": primary,
        "driver_summary": driver_summary,
        "parameter_summary": parameter_summary,
        "result_comparison": result_comparison,
        "result_methodology": result_methodology,
        "parameter_comparison": parameter_comparison,
        "risk": risk,
        "risk_level": "medium" if risk else "none",
    }


def _read_json_object(path: Path) -> TraceRecord:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_trial_row(path: Path, trial_id: str) -> TraceRecord:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                if row.get("trial_id") == trial_id:
                    return {key: value for key, value in row.items() if key is not None}
    except OSError:
        return {}
    return {}


def _read_latest_shadow_parameter_promotion(output_dir: Path) -> TraceRecord:
    promotions: list[tuple[datetime, TraceRecord]] = []
    for path in output_dir.glob("shadow_parameter_promotion_*.json"):
        payload = _read_json_object(path)
        generated_at = _parse_iso_datetime(_string_value(payload.get("generated_at")))
        if generated_at is None:
            generated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        promotions.append((generated_at, payload))
    if not promotions:
        return {}
    return max(promotions, key=lambda item: item[0])[1]


def _primary_cap_summary(manifest: TraceRecord) -> str:
    cap_rows = manifest.get("cap_attribution")
    if not isinstance(cap_rows, list):
        return ""
    records = tuple(row for row in cap_rows if isinstance(row, dict))
    if not records:
        return ""
    selected = max(
        records,
        key=lambda row: abs(_optional_float(row.get("excess_delta_vs_baseline")) or 0.0),
    )
    gate_id = _string_value(selected.get("gate_id"))
    if not gate_id:
        return ""
    selected_cap = _format_percent(_optional_float(selected.get("selected_cap_value")))
    excess_delta = _format_signed_percent(
        _optional_float(selected.get("excess_delta_vs_baseline"))
    )
    return (
        f"主要 cap：{gate_id}={selected_cap}，"
        f"cap-only excess {excess_delta}"
    )


def _shadow_result_comparison_rows(
    *,
    trial: TraceRecord,
    manifest: TraceRecord,
) -> tuple[TraceRecord, ...]:
    production_return = _optional_float(trial.get("production_total_return"))
    shadow_return = _optional_float(trial.get("shadow_total_return"))
    excess_return = _optional_float(trial.get("excess_total_return"))
    production_drawdown = _optional_float(trial.get("production_max_drawdown"))
    shadow_drawdown = _optional_float(trial.get("shadow_max_drawdown"))
    production_turnover = _optional_float(trial.get("production_turnover"))
    shadow_turnover = _optional_float(trial.get("shadow_turnover"))
    beat_rate = _optional_float(trial.get("shadow_beats_production_rate"))
    available_count = _optional_int(trial.get("available_count"))
    total_count = _optional_int(trial.get("total_count"))
    pending_count = _optional_int(trial.get("pending_count"))
    missing_count = _optional_int(trial.get("missing_count"))
    cost_bps = _optional_float(manifest.get("cost_bps"))
    slippage_bps = _optional_float(manifest.get("slippage_bps"))
    cost_note = _join_nonempty(
        [
            _label("cost_bps", _format_decimal(cost_bps, digits=1)),
            _label("slippage_bps", _format_decimal(slippage_bps, digits=1)),
        ],
        separator="，",
    )
    sample_note = _join_nonempty(
        [
            _label("AVAILABLE", available_count),
            _label("total", total_count),
        ],
        separator=" / ",
    )
    rows: list[TraceRecord] = [
        {
            "metric": "Total return",
            "production": _format_percent(production_return),
            "shadow": _format_percent(shadow_return),
            "delta": _format_signed_percent(excess_return),
            "note": _join_nonempty(
                ["AVAILABLE outcome 复利累计", sample_note, cost_note]
            ),
        },
        {
            "metric": "Max drawdown",
            "production": _format_percent(production_drawdown),
            "shadow": _format_percent(shadow_drawdown),
            "delta": _format_signed_percent_delta(
                production_drawdown,
                shadow_drawdown,
            ),
            "note": "负值更大代表 shadow 回撤更深。",
        },
        {
            "metric": "Turnover",
            "production": _format_decimal(production_turnover),
            "shadow": _format_decimal(shadow_turnover),
            "delta": _format_signed_decimal_delta(
                production_turnover,
                shadow_turnover,
            ),
            "note": "AVAILABLE outcome 间仓位中点变化绝对值累计。",
        },
    ]
    if beat_rate is not None:
        rows.append(
            {
                "metric": "Beat rate",
                "production": "比较基准 50.00%",
                "shadow": _format_percent(beat_rate),
                "delta": _format_signed_percent_delta(0.5, beat_rate),
                "note": "AVAILABLE outcome 中 shadow 单日 return 高于 production 的占比。",
            }
        )
    rows.append(
        {
            "metric": "Sample coverage",
            "production": _format_count_pair(available_count, total_count),
            "shadow": _join_nonempty(
                [
                    _label("pending", pending_count),
                    _label("missing", missing_count),
                ],
                separator="，",
            )
            or "NA",
            "delta": "eligible"
            if _string_value(trial.get("eligible")).lower() == "true"
            else "未达准入",
            "note": _string_value(trial.get("ineligibility_reason")) or "NA",
        }
    )
    return tuple(rows)


def _shadow_result_methodology(manifest: TraceRecord) -> str:
    cost_bps = _optional_float(manifest.get("cost_bps"))
    slippage_bps = _optional_float(manifest.get("slippage_bps"))
    cost_text = _join_nonempty(
        [
            _label("本次 cost_bps", _format_decimal(cost_bps, digits=1)),
            _label("slippage_bps", _format_decimal(slippage_bps, digits=1)),
        ],
        separator="，",
    )
    return (
        "Return 口径：只统计 AVAILABLE outcome；单日 return = 仓位中点 × "
        "标的 outcome return - 仓位中点变化绝对值 × "
        "(cost_bps + slippage_bps) / 10000；total return 为单日 return "
        "复利累计；excess = shadow total return - production total return"
        f"{('；' + cost_text) if cost_text else ''}。"
    )


def _shadow_parameter_comparison_rows(
    *,
    baseline_weights: TraceRecord,
    candidate_weights: TraceRecord,
    baseline_gate_caps: TraceRecord,
    candidate_gate_caps: TraceRecord,
    production_gate_caps: dict[str, TraceRecord],
    manifest: TraceRecord,
) -> tuple[TraceRecord, ...]:
    rows: list[TraceRecord] = []
    for key in sorted(set(baseline_weights) | set(candidate_weights)):
        baseline_value = _optional_float(baseline_weights.get(key))
        candidate_value = _optional_float(candidate_weights.get(key))
        if baseline_value is None and candidate_value is None:
            continue
        rows.append(
            {
                "group": "weight",
                "parameter": key,
                "production": _format_percent(baseline_value),
                "candidate": _format_percent(candidate_value),
                "delta": _format_signed_percent_delta(baseline_value, candidate_value),
                "note": "未变化"
                if _floats_equal(baseline_value, candidate_value)
                else "已变化",
            }
        )

    cap_notes = _cap_attribution_notes(manifest)
    for key in sorted(set(baseline_gate_caps) | set(candidate_gate_caps)):
        baseline_value = _optional_float(baseline_gate_caps.get(key))
        candidate_value = _optional_float(candidate_gate_caps.get(key))
        if baseline_value is None and candidate_value is None:
            continue
        observed_cap = production_gate_caps.get(key, {})
        production = _production_gate_cap_label(
            override_value=baseline_value,
            observed_cap=observed_cap,
        )
        production_min = _optional_float(observed_cap.get("min"))
        production_max = _optional_float(observed_cap.get("max"))
        rows.append(
            {
                "group": "gate_cap",
                "parameter": key,
                "production": production,
                "production_observed_min": production_min,
                "production_observed_max": production_max,
                "production_observed_count": _optional_int(observed_cap.get("count")),
                "production_observed_snapshot_count": _optional_int(
                    observed_cap.get("snapshot_count")
                ),
                "candidate": _format_percent(candidate_value),
                "delta": "新增 override"
                if baseline_value is None
                else _format_signed_percent_delta(baseline_value, candidate_value),
                "note": cap_notes.get(key, "候选 gate cap override"),
            }
        )
    return tuple(rows)


def _production_gate_cap_observations(manifest: TraceRecord) -> dict[str, TraceRecord]:
    snapshot_path = _string_value(manifest.get("decision_snapshot_path"))
    if not snapshot_path:
        return {}
    snapshot_dir = Path(snapshot_path)
    if not snapshot_dir.exists():
        return {}
    window = _mapping_value(manifest, "search_window")
    start = _parse_iso_date(_string_value(window.get("start")))
    end = _parse_iso_date(_string_value(window.get("end")))
    observed: dict[str, list[float]] = {}
    snapshot_count = 0
    for path in sorted(snapshot_dir.glob("decision_snapshot_*.json")):
        snapshot_date = _decision_snapshot_date(path)
        if start is not None and snapshot_date is not None and snapshot_date < start:
            continue
        if end is not None and snapshot_date is not None and snapshot_date > end:
            continue
        payload = _read_json_object(path)
        gate_caps = _position_gate_caps(payload)
        if not gate_caps:
            continue
        snapshot_count += 1
        for gate_id, cap in gate_caps.items():
            observed.setdefault(gate_id, []).append(cap)
    if not snapshot_count:
        return {}
    return {
        gate_id: {
            "min": min(values),
            "max": max(values),
            "count": len(values),
            "snapshot_count": snapshot_count,
        }
        for gate_id, values in observed.items()
        if values
    }


def _decision_snapshot_date(path: Path) -> date | None:
    match = re.search(r"decision_snapshot_(\d{4}-\d{2}-\d{2})", path.name)
    if not match:
        return None
    return _parse_iso_date(match.group(1))


def _position_gate_caps(payload: TraceRecord) -> dict[str, float]:
    positions = _mapping_value(payload, "positions")
    raw_gates = positions.get("position_gates")
    if not isinstance(raw_gates, list):
        return {}
    caps: dict[str, float] = {}
    for gate in raw_gates:
        if not isinstance(gate, dict):
            continue
        gate_id = _string_value(gate.get("gate_id"))
        if not gate_id or gate_id == "score_model":
            continue
        cap = _optional_float(gate.get("max_position"))
        if cap is not None:
            caps[gate_id] = cap
    return caps


def _production_gate_cap_label(
    *,
    override_value: float | None,
    observed_cap: TraceRecord,
) -> str:
    if override_value is not None:
        return _format_percent(override_value)
    observed_min = _optional_float(observed_cap.get("min"))
    observed_max = _optional_float(observed_cap.get("max"))
    count = _optional_int(observed_cap.get("count"))
    snapshot_count = _optional_int(observed_cap.get("snapshot_count"))
    if observed_min is None or observed_max is None:
        return "主线实际 gate：未记录；无静态 override"
    if _floats_equal(observed_min, observed_max):
        value_text = _format_percent(observed_min)
    else:
        value_text = f"{_format_percent(observed_min)}-{_format_percent(observed_max)}"
    coverage = (
        f"；{count}/{snapshot_count} 快照"
        if count is not None and snapshot_count is not None and count != snapshot_count
        else ""
    )
    return f"主线实际 gate：{value_text}（无静态 override{coverage}）"


def _cap_attribution_notes(manifest: TraceRecord) -> dict[str, str]:
    cap_rows = manifest.get("cap_attribution")
    if not isinstance(cap_rows, list):
        return {}
    notes: dict[str, str] = {}
    records = tuple(row for row in cap_rows if isinstance(row, dict))
    if not records:
        return notes
    primary_gate = ""
    primary_abs_delta = -1.0
    for row in records:
        gate_id = _string_value(row.get("gate_id"))
        if not gate_id:
            continue
        delta = _optional_float(row.get("excess_delta_vs_baseline")) or 0.0
        if abs(delta) > primary_abs_delta:
            primary_gate = gate_id
            primary_abs_delta = abs(delta)
        notes[gate_id] = (
            "cap-only excess "
            f"{_format_signed_percent(delta)}；"
            f"MDD {_format_percent(_optional_float(row.get('cap_only_shadow_max_drawdown')))}"
        )
    if primary_gate and primary_gate in notes:
        notes[primary_gate] = f"primary cap；{notes[primary_gate]}"
    return notes


def _promotion_blockers(promotion: TraceRecord) -> tuple[str, ...]:
    checks = promotion.get("checks")
    if not isinstance(checks, list):
        return ()
    blockers: list[str] = []
    for check in checks:
        if not isinstance(check, dict):
            continue
        status = _string_value(check.get("status"))
        if status not in {"FAIL", "MISSING", "BLOCKED"}:
            continue
        reason = _string_value(check.get("reason"))
        check_id = _string_value(check.get("check_id"))
        blockers.append(_join_nonempty([check_id, reason], separator="："))
    return tuple(blockers)


def _json_mapping_from_cell(value: object) -> TraceRecord:
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_iso_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _existing_tasks(
    tasks: dict[str, DailyTaskDetail],
    source_steps: tuple[str, ...],
) -> tuple[DailyTaskDetail, ...]:
    return tuple(task for step in source_steps if (task := tasks.get(step)) is not None)


def _combined_status(tasks: tuple[DailyTaskDetail, ...]) -> str:
    if not tasks:
        return "NOT_CONNECTED"
    if any(task.status == "FAIL" for task in tasks):
        return "FAIL"
    if all(task.status == "SKIPPED" for task in tasks):
        return "SKIPPED"
    if any(task.risk_level != "none" or task.status == "SKIPPED" for task in tasks):
        return "PASS_WITH_LIMITATIONS"
    return "PASS"


def _combined_risk_level(tasks: tuple[DailyTaskDetail, ...]) -> str:
    if any(task.risk_level == "high" for task in tasks):
        return "high"
    if any(task.risk_level == "medium" for task in tasks):
        return "medium"
    return "none"


def _max_risk_level(first: str, second: str) -> str:
    order = {"none": 0, "medium": 1, "high": 2}
    return first if order.get(first, 0) >= order.get(second, 0) else second


def _combined_task_risk(tasks: tuple[DailyTaskDetail, ...]) -> str:
    risks = [
        f"{task.title}：{task.important_risk}"
        for task in tasks
        if task.risk_level != "none"
    ]
    if not risks:
        return "未发现该结论域的新增阻断风险。"
    return "；".join(dict.fromkeys(risks))


def _first_detail_status(tasks: tuple[DailyTaskDetail, ...], label: str) -> str:
    for task in tasks:
        for report in task.detail_reports:
            if report.get("label") == label and report.get("exists"):
                return str(report.get("status") or "")
    return ""


def _joined_task_report_status(task: DailyTaskDetail | None) -> str:
    if task is None:
        return ""
    return _joined_report_status(task.detail_reports)


def _mapping_value(record: TraceRecord, key: str) -> TraceRecord:
    value = record.get(key)
    return value if isinstance(value, dict) else {}


def _string_value(value: object) -> str:
    return value if isinstance(value, str) else ""


def _string_list(value: object, *, limit: int) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    strings = tuple(item for item in value if isinstance(item, str))
    return strings[:limit]


def _render_header(report: DailyTaskDashboardReport, title: str) -> str:
    return "\n".join(
        [
            "<header>",
            '<p class="eyebrow">daily-run conclusion surface</p>',
            f"<h1>{_text(title)}</h1>",
            (
                '<p class="subtle">先汇总重要任务给出的业务结论和优先风险；'
                "执行状态和详情路径放在后面作为审计入口。</p>"
            ),
            '<div class="header-meta">',
            _meta("运行状态", report.status),
            _meta("评估日期", report.as_of.isoformat()),
            _meta("Run ID", report.run_id),
            _meta("production_effect", report.production_effect),
            "</div>",
            "</header>",
        ]
    )


def _render_key_conclusions(report: DailyTaskDashboardReport) -> str:
    conclusions = _build_key_conclusions(report)
    cards = [_render_key_conclusion_card(conclusion) for conclusion in conclusions]
    if not cards:
        cards = ['<p class="subtle">没有可汇总的关键结论；请查看执行明细。</p>']
    return "\n".join(
        [
            '<section class="key-section" aria-labelledby="key-title">',
            '<div class="section-head">',
            '<h2 id="key-title">关键结论总览</h2>',
            "<p>先看这些会影响判断、复核优先级和下一步动作的结论。</p>",
            "</div>",
            '<div class="conclusion-grid">',
            *cards,
            "</div>",
            "</section>",
        ]
    )


def _render_key_conclusion_card(conclusion: DailyTaskKeyConclusion) -> str:
    supporting = (
        "\n".join(f"<li>{_text(item)}</li>" for item in conclusion.supporting)
        if conclusion.supporting
        else "<li>暂无补充要点；以主结论和来源报告为准。</li>"
    )
    sources = "、".join(conclusion.source_steps)
    comparison_class = (
        " has-comparison"
        if conclusion.parameter_comparison or conclusion.result_comparison
        else ""
    )
    return "\n".join(
        [
            (
                '<article class="conclusion-card '
                f"risk-{_text(conclusion.risk_level)}{comparison_class}"
                '">'
            ),
            '<div class="card-head">',
            f'<span class="area">{_text(conclusion.area)}</span>',
            _status_badge(conclusion.status),
            "</div>",
            f"<h3>{_text(conclusion.title)}</h3>",
            f'<p class="primary">{_text(conclusion.primary)}</p>',
            f"<ul>{supporting}</ul>",
            _render_shadow_comparison(
                parameter_rows=conclusion.parameter_comparison,
                result_rows=conclusion.result_comparison,
                result_methodology=conclusion.result_methodology,
            ),
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(conclusion.important_risk)}</p>"
            ),
            f'<p class="sources">来源：<code>{_text(sources)}</code></p>',
            "</article>",
        ]
    )


def _render_shadow_comparison(
    *,
    parameter_rows: tuple[TraceRecord, ...],
    result_rows: tuple[TraceRecord, ...],
    result_methodology: str,
) -> str:
    if not parameter_rows and not result_rows:
        return ""
    gate_rows = tuple(row for row in parameter_rows if row.get("group") == "gate_cap")
    weight_rows = tuple(row for row in parameter_rows if row.get("group") == "weight")
    other_rows = tuple(
        row
        for row in parameter_rows
        if row.get("group") not in {"gate_cap", "weight"}
    )
    sections = []
    if result_rows:
        sections.append(_render_result_comparison_table(result_rows, result_methodology))
    if gate_rows:
        sections.append(_render_gate_cap_parameter_table(gate_rows))
    if weight_rows:
        sections.append(_render_weight_parameter_table(weight_rows))
    if other_rows:
        sections.append(_render_generic_parameter_table(other_rows))
    return "\n".join(
        [
            '<div class="parameter-comparison">',
            "<h4>Shadow 结果与参数对比</h4>",
            (
                '<p class="comparison-intro">'
                "先看结果差异，再看 gate cap override：这是本轮 shadow candidate "
                "相比主线实际 gate 真正新增的静态限制；权重参数单独列出用于确认"
                "是否发生变化。"
                "</p>"
            ),
            '<div class="shadow-sections">',
            *sections,
            "</div>",
            "</div>",
        ]
    )


def _render_result_comparison_table(
    rows: tuple[TraceRecord, ...],
    methodology: str,
) -> str:
    rendered_rows = []
    for row in rows:
        rendered_rows.append(
            "".join(
                [
                    "<tr>",
                    (
                        '<td class="metric-cell" data-label="指标">'
                        f'{_text(row.get("metric", ""))}</td>'
                    ),
                    (
                        '<td class="value-cell" data-label="Production/current">'
                        f"{_text(row.get('production', ''))}</td>"
                    ),
                    (
                        '<td class="value-cell" data-label="Shadow candidate">'
                        f"{_text(row.get('shadow', ''))}</td>"
                    ),
                    (
                        '<td class="result-delta" data-label="差异">'
                        f"{_text(row.get('delta', ''))}</td>"
                    ),
                    (
                        '<td class="note-cell" data-label="说明">'
                        f"{_text(row.get('note', ''))}</td>"
                    ),
                    "</tr>",
                ]
            )
        )
    methodology_html = (
        f'<p class="result-methodology">{_text(methodology)}</p>'
        if methodology
        else ""
    )
    return "\n".join(
        [
            '<section class="shadow-section result-section">',
            "<h5>结果对比</h5>",
            methodology_html,
            '<div class="table-wrap shadow-table-wrap"><table class="shadow-table shadow-results">',
            "<thead><tr><th>指标</th><th>Production/current</th>"
            "<th>Shadow candidate</th><th>差异</th><th>说明</th></tr></thead>",
            "<tbody>",
            *rendered_rows,
            "</tbody></table></div>",
            "</section>",
        ]
    )


def _render_gate_cap_parameter_table(rows: tuple[TraceRecord, ...]) -> str:
    rendered_rows = []
    for row in rows:
        change = _render_parameter_change(row)
        rendered_rows.append(
            "".join(
                [
                    "<tr>",
                    (
                        '<td data-label="参数"><code>'
                        f'{_text(row.get("parameter", ""))}</code></td>'
                    ),
                    (
                        '<td class="cap-value-cell" data-label="Production/current">'
                        f"{_render_gate_production_value(row.get('production', ''))}</td>"
                    ),
                    (
                        '<td class="value-cell candidate-cell" '
                        'data-label="Shadow override">'
                        f'<strong>{_text(row.get("candidate", ""))}</strong>'
                        f"{change}</td>"
                    ),
                    (
                        '<td class="note-cell" data-label="说明">'
                        f"{_text(_display_parameter_note(row.get('note', '')))}</td>"
                    ),
                    "</tr>",
                ]
            )
        )
    return "\n".join(
        [
            '<section class="shadow-section">',
            "<h5>Gate cap override</h5>",
            '<div class="table-wrap shadow-table-wrap"><table class="shadow-table shadow-gates">',
            "<thead><tr><th>参数</th><th>Production/current</th>"
            "<th>Shadow override</th><th>说明</th></tr></thead>",
            "<tbody>",
            *rendered_rows,
            "</tbody></table></div>",
            "</section>",
        ]
    )


def _render_weight_parameter_table(rows: tuple[TraceRecord, ...]) -> str:
    unchanged_count = sum(1 for row in rows if row.get("note") == "未变化")
    rendered_rows = []
    for row in rows:
        change = _render_parameter_change(row)
        rendered_rows.append(
            "".join(
                [
                    "<tr>",
                    (
                        '<td data-label="参数"><code>'
                        f'{_text(row.get("parameter", ""))}</code></td>'
                    ),
                    (
                        '<td class="value-cell" data-label="Production/current">'
                        f"{_text(row.get('production', ''))}</td>"
                    ),
                    (
                        '<td class="value-cell" data-label="Shadow candidate">'
                        f"{_text(row.get('candidate', ''))}</td>"
                    ),
                    f'<td data-label="变化">{change}</td>',
                    "</tr>",
                ]
            )
        )
    return "\n".join(
        [
            '<section class="shadow-section">',
            (
                "<h5>权重参数 "
                f'<span>{unchanged_count}/{len(rows)} 未变化</span></h5>'
            ),
            '<div class="table-wrap shadow-table-wrap"><table class="shadow-table shadow-weights">',
            "<thead><tr><th>参数</th><th>Production/current</th>"
            "<th>Shadow candidate</th><th>变化</th></tr></thead>",
            "<tbody>",
            *rendered_rows,
            "</tbody></table></div>",
            "</section>",
        ]
    )


def _render_generic_parameter_table(rows: tuple[TraceRecord, ...]) -> str:
    rendered_rows = []
    for row in rows:
        rendered_rows.append(
            "<tr>"
            f"<td>{_text(_parameter_group_label(row.get('group')))}</td>"
            f"<td><code>{_text(row.get('parameter', ''))}</code></td>"
            f"<td>{_text(row.get('production', ''))}</td>"
            f"<td>{_text(row.get('candidate', ''))}</td>"
            f"<td>{_text(row.get('delta', ''))}</td>"
            f"<td>{_text(row.get('note', ''))}</td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<section class="shadow-section">',
            "<h5>其他参数</h5>",
            '<div class="table-wrap"><table class="shadow-table">',
            (
                "<thead><tr><th>类型</th><th>参数</th><th>Production/current</th>"
                "<th>Shadow candidate</th><th>变化</th><th>说明</th></tr></thead>"
            ),
            "<tbody>",
            *rendered_rows,
            "</tbody></table></div>",
            "</section>",
        ]
    )


def _render_parameter_change(row: TraceRecord) -> str:
    note = _string_value(row.get("note"))
    delta = _string_value(row.get("delta"))
    label = note if note in {"未变化", "已变化"} else delta
    if not label:
        label = "NA"
    class_name = "unchanged" if label in {"未变化", "+0.00%"} else "changed"
    detail = ""
    if note in {"未变化", "已变化"} and delta:
        detail = f'<small>{_text(delta)}</small>'
    return f'<span class="change-chip {class_name}">{_text(label)}</span>{detail}'


def _compact_parameter_value(value: object) -> str:
    text = _string_value(value)
    if text == "主线实际 gate（无静态 override）":
        return "无静态 override"
    if text.startswith("主线实际 gate："):
        return f"实际 {text.removeprefix('主线实际 gate：')}"
    return text


def _render_gate_production_value(value: object) -> str:
    text = _string_value(value)
    prefix = "主线实际 gate："
    if not text.startswith(prefix):
        return _text(_compact_parameter_value(text))
    compact = text.removeprefix(prefix)
    match = re.fullmatch(r"(.+?)（(.+)）", compact)
    if not match:
        return _text(f"实际 {compact}")
    value_text, note = match.groups()
    return (
        '<span class="cap-value">'
        f'<strong>实际 {_text(value_text)}</strong>'
        f"<small>{_text(note)}</small>"
        "</span>"
    )


def _display_parameter_note(value: object) -> str:
    text = _string_value(value)
    return text.replace("primary cap", "主要 cap")


def _parameter_group_label(value: object) -> str:
    labels = {
        "weight": "权重",
        "gate_cap": "Gate cap",
    }
    return labels.get(str(value), str(value))


def _render_summary(report: DailyTaskDashboardReport) -> str:
    return "\n".join(
        [
            '<section aria-labelledby="summary-title">',
            '<div class="section-head">',
            '<h2 id="summary-title">运行状态摘要</h2>',
            "<p>这里用于审计本轮 daily-run 是否完整，不作为首要阅读区。</p>",
            "</div>",
            '<div class="summary-grid">',
            _summary_item("任务数", str(len(report.tasks))),
            _summary_item("失败", str(report.failed_count)),
            _summary_item("跳过", str(report.skipped_count)),
            _summary_item("有风险/限制", str(report.risk_count)),
            _summary_item("输入可见性", report.input_visibility_status or "未记录"),
            _summary_item("Visibility cutoff", report.visibility_cutoff or "未记录"),
            _summary_item("Git commit", report.git_commit or "未记录"),
            _summary_item("Git dirty", str(report.git_dirty)),
            "</div>",
            "</section>",
        ]
    )


def _render_risks(report: DailyTaskDashboardReport) -> str:
    risky_tasks = [task for task in report.tasks if task.risk_level != "none"]
    rows = [
        "<tr><td>无</td><td>未发现阻断或限制风险。</td><td></td></tr>"
    ] if not risky_tasks else [
        (
            f"<tr><td>{_text(task.title)}</td>"
            f"<td>{_status_badge(task.risk_level)}</td>"
            f"<td>{_text(task.important_risk)}</td></tr>"
        )
        for task in risky_tasks
    ]
    return "\n".join(
        [
            '<section aria-labelledby="risk-title">',
            '<div class="section-head">',
            '<h2 id="risk-title">重要风险</h2>',
            "<p>只列需要读者优先注意的阻断、警告、限制或缺失报告。</p>",
            "</div>",
            '<div class="table-wrap"><table>',
            "<thead><tr><th>子任务</th><th>风险等级</th><th>说明</th></tr></thead>",
            "<tbody>",
            *rows,
            "</tbody></table></div>",
            "</section>",
        ]
    )


def _render_task_table(report: DailyTaskDashboardReport) -> str:
    rows = []
    for task in report.tasks:
        rows.append(
            "<tr>"
            f"<td>{_text(task.title)}<br><code>{_text(task.step_id)}</code></td>"
            f"<td>{_status_badge(task.status)}</td>"
            f"<td>{_text(task.conclusion)}</td>"
            f"<td>{_text(task.important_risk)}</td>"
            f"<td>{_text(_format_seconds(task.duration_seconds))}</td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<section aria-labelledby="task-table-title">',
            '<div class="section-head">',
            '<h2 id="task-table-title">任务执行明细</h2>',
            "<p>用于排查具体子任务；首屏结论已在上方汇总。</p>",
            "</div>",
            '<div class="table-wrap"><table>',
            "<thead><tr><th>子任务</th><th>状态</th><th>重要结论</th><th>重要风险</th><th>耗时</th></tr></thead>",
            "<tbody>",
            *rows,
            "</tbody></table></div>",
            "</section>",
        ]
    )


def _render_task_details(report: DailyTaskDashboardReport) -> str:
    sections: list[str] = [
        '<section aria-labelledby="detail-title">',
        '<div class="section-head">',
        '<h2 id="detail-title">子任务详情入口</h2>',
        "<p>从这里下钻到各子任务已有报告；后续专属网页沿用同一入口。</p>",
        "</div>",
        '<div class="subtask-link-grid">',
    ]
    for task in report.tasks:
        sections.append(_render_subtask_link_card(task, report.reports_dir))
    sections.extend(["</div>", "</section>"])
    return "\n".join(sections)


def _render_subtask_link_card(task: DailyTaskDetail, reports_dir: Path) -> str:
    return "\n".join(
        [
            f'<article class="subtask-card risk-{_text(task.risk_level)}">',
            '<div class="subtask-card-head">',
            "<div>",
            f"<h3>{_text(task.title)}</h3>",
            f"<code>{_text(task.step_id)}</code>",
            "</div>",
            _status_badge(task.status),
            "</div>",
            f'<p class="subtask-conclusion">{_text(task.conclusion)}</p>',
            (
                '<p class="subtask-risk"><strong>风险：</strong>'
                f"{_text(task.important_risk)}</p>"
            ),
            _render_report_links(task, reports_dir),
            "</article>",
        ]
    )


def _render_report_links(task: DailyTaskDetail, reports_dir: Path) -> str:
    if not task.detail_reports:
        return '<p class="subtle">该步骤暂无专属详情报告路径。</p>'
    links = []
    for report in task.detail_reports:
        status = str(report.get("status") or "")
        label = str(report["label"])
        href = str(report.get("href") or _report_href(report.get("path"), reports_dir))
        if report.get("exists"):
            links.append(
                '<a class="report-link" '
                f'href="{_text(href)}">'
                f"<span>{_text(label)}</span>"
                f"<small>{_text(status)}</small>"
                "</a>"
            )
            continue
        required = "required" if report.get("required", True) else "optional"
        links.append(
            '<span class="report-link missing">'
            f"<span>{_text(label)}</span>"
            f"<small>{_text(required)} · MISSING</small>"
            "</span>"
        )
    return "\n".join(
        [
            '<div class="report-link-list">',
            *links,
            "</div>",
        ]
    )


def _report_href(path_value: object, reports_dir: Path) -> str:
    path = Path(str(path_value or ""))
    link_path: Path
    try:
        link_path = path.relative_to(reports_dir)
    except ValueError:
        try:
            link_path = path.resolve().relative_to(reports_dir.resolve())
        except (OSError, RuntimeError, ValueError):
            if path.is_absolute():
                try:
                    return path.as_uri()
                except ValueError:
                    pass
            link_path = path
    return quote(link_path.as_posix(), safe="/._-:%#?=&")


def _render_footer(report: DailyTaskDashboardReport) -> str:
    run_report = "未提供" if report.run_report_path is None else str(report.run_report_path)
    return (
        "<footer>"
        "每日任务展示页为只读关键结论入口；正式运行审计仍以 "
        f"<code>{_text(run_report)}</code> 和 "
        f"<code>{_text(str(report.metadata_path))}</code> 为准。"
        "</footer>"
    )


def _read_metadata(path: Path) -> TraceRecord:
    if not path.exists():
        raise FileNotFoundError(f"daily ops metadata not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"daily ops metadata must be a JSON object: {path}")
    return payload


def _records(value: object) -> tuple[TraceRecord, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _records_by_step(value: object) -> dict[str, TraceRecord]:
    return {str(record.get("step_id")): record for record in _records(value)}


def _report_metadata_values(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        match = re.match(r"^-+\s*([^：:]+)[：:]\s*(.+)$", line)
        if match:
            values[match.group(1).strip()] = match.group(2).strip()
    return values


def _merge_report_values(detail_reports: tuple[TraceRecord, ...]) -> dict[str, str]:
    merged: dict[str, str] = {}
    for report in detail_reports:
        values = report.get("values")
        if isinstance(values, dict):
            for key, value in values.items():
                if isinstance(value, str) and key not in merged:
                    merged[key] = value
    return merged


def _joined_report_status(detail_reports: tuple[TraceRecord, ...]) -> str:
    statuses = [
        f"{report['label']}={report.get('status')}"
        for report in detail_reports
        if report.get("exists")
    ]
    return "；".join(statuses)


def _metadata_value_from_reports(
    detail_reports: tuple[TraceRecord, ...],
    label: str,
) -> str:
    for report in detail_reports:
        values = report.get("values")
        if isinstance(values, dict):
            value = values.get(label)
            if isinstance(value, str):
                return value
    return ""


def _first_report_bullet(
    detail_reports: tuple[TraceRecord, ...],
    section_title: str,
) -> str:
    for report in detail_reports:
        path = Path(str(report["path"]))
        if not path.exists():
            continue
        bullet = _first_bullet(_markdown_section(path.read_text(encoding="utf-8"), section_title))
        if bullet:
            return bullet
    return ""


def _markdown_section(text: str, heading: str) -> str:
    marker = heading.strip()
    start = text.find(marker)
    if start == -1:
        return ""
    rest = text[start + len(marker) :]
    match = re.search(r"\n#{1,6}\s+", rest)
    return rest[: match.start()] if match else rest


def _first_bullet(text: str) -> str:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("- "):
            return line[2:].strip()
    return ""


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        if isinstance(value, (str, bytes, bytearray, int, float)):
            return int(value)
        return None
    except (TypeError, ValueError):
        return None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, (str, bytes, bytearray, int, float)):
            return float(value)
        return None
    except (TypeError, ValueError):
        return None


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "未执行"
    return f"{value:.1f}s"


def _join_nonempty(
    values: list[object | None] | tuple[object | None, ...],
    *,
    separator: str = "；",
) -> str:
    return separator.join(
        str(value) for value in values if value is not None and str(value) != ""
    )


def _label(label: str, value: object | None) -> str:
    if value is None or str(value) == "":
        return ""
    return f"{label}：{value}"


def _format_percent(value: float | None) -> str:
    if value is None:
        return "NA"
    return f"{value * 100:.2f}%"


def _format_signed_percent(value: float | None) -> str:
    if value is None:
        return "NA"
    sign = "+" if value >= 0 else ""
    return f"{sign}{value * 100:.2f}%"


def _format_signed_percent_delta(
    baseline: float | None,
    candidate: float | None,
) -> str:
    if baseline is None or candidate is None:
        return "NA"
    return _format_signed_percent(candidate - baseline)


def _format_decimal(value: float | None, *, digits: int = 2) -> str:
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def _format_signed_decimal_delta(
    baseline: float | None,
    candidate: float | None,
    *,
    digits: int = 2,
) -> str:
    if baseline is None or candidate is None:
        return "NA"
    value = candidate - baseline
    sign = "+" if value >= 0 else ""
    return f"{sign}{value:.{digits}f}"


def _format_count_pair(available_count: int | None, total_count: int | None) -> str:
    if available_count is None or total_count is None:
        return "NA"
    return f"{available_count}/{total_count} AVAILABLE"


def _format_percent_mapping(values: TraceRecord) -> str:
    parts: list[str] = []
    for key in sorted(values):
        number = _optional_float(values.get(key))
        if number is None:
            continue
        parts.append(f"{key}={_format_percent(number)}")
    return "，".join(parts)


def _floats_equal(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is right
    return abs(left - right) < 1e-12


def _text(value: object) -> str:
    return escape(str(value), quote=True)


def _meta(label: str, value: object) -> str:
    return f"<span><strong>{_text(label)}:</strong> {_text(value)}</span>"


def _summary_item(label: str, value: object) -> str:
    return (
        '<div class="summary-item">'
        f"<span>{_text(label)}</span>"
        f"<strong>{_text(value)}</strong>"
        "</div>"
    )


def _status_badge(status: str) -> str:
    normalized = status.lower()
    class_name = "status"
    if normalized in {"fail", "high", "blocked", "blocked_env", "blocked_visibility"}:
        class_name += " danger"
    elif (
        normalized in {"skipped", "medium"}
        or "warning" in normalized
        or "limitation" in normalized
    ):
        class_name += " warn"
    elif normalized in {"pass", "present", "none"}:
        class_name += " ok"
    return f'<span class="{class_name}">{_text(status)}</span>'


_CSS = """
:root {
  color-scheme: light;
  --bg: #f7f9fb;
  --surface: #ffffff;
  --ink: #111827;
  --muted: #5b6472;
  --line: #d7dee8;
  --ok: #0f766e;
  --warn: #b45309;
  --danger: #b91c1c;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: Arial, "Microsoft YaHei", sans-serif;
  line-height: 1.55;
}
header, main, footer {
  width: min(1180px, calc(100% - 32px));
  margin: 0 auto;
}
header {
  padding: 28px 0 18px;
  border-bottom: 1px solid var(--line);
}
main {
  display: flex;
  flex-direction: column;
  gap: 20px;
  padding: 22px 0 34px;
}
h1, h2, h3, p { margin-top: 0; }
h1 { margin-bottom: 8px; font-size: 28px; letter-spacing: 0; }
h2 { margin-bottom: 6px; font-size: 20px; letter-spacing: 0; }
h3 { margin: 18px 0 10px; font-size: 16px; letter-spacing: 0; }
h4 { margin: 12px 0 8px; font-size: 14px; letter-spacing: 0; }
section {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 18px;
}
.section-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 14px;
  border-bottom: 1px solid var(--line);
  margin-bottom: 14px;
  padding-bottom: 10px;
}
.section-head p, .subtle, .eyebrow, footer { color: var(--muted); }
.eyebrow {
  margin-bottom: 4px;
  font-size: 12px;
  text-transform: uppercase;
}
.key-section {
  border-color: #b9c8d8;
}
.conclusion-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 14px;
}
.conclusion-card {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fbfcfd;
  padding: 14px;
}
.conclusion-card.has-comparison {
  grid-column: 1 / -1;
}
.conclusion-card.risk-medium {
  border-color: #f1c680;
  background: #fffaf0;
}
.conclusion-card.risk-high {
  border-color: #f0a7a7;
  background: #fff5f5;
}
.card-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 8px;
}
.area, .sources {
  color: var(--muted);
  font-size: 12px;
}
.primary {
  margin-bottom: 10px;
  font-weight: 700;
}
.conclusion-card ul {
  margin: 0 0 10px 18px;
  padding: 0;
}
.risk-line {
  margin-bottom: 8px;
}
.parameter-comparison {
  margin: 14px 0;
  border-top: 1px solid var(--line);
  padding-top: 12px;
}
.parameter-comparison h4 {
  margin-bottom: 4px;
}
.comparison-intro {
  margin: 0 0 12px;
  color: var(--muted);
  font-size: 13px;
  line-height: 1.55;
}
.shadow-sections {
  display: grid;
  gap: 12px;
}
.shadow-section {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.54);
  padding: 10px 12px 12px;
}
.shadow-section h5 {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
  margin: 0 0 8px;
  font-size: 13px;
}
.shadow-section h5 span {
  color: var(--muted);
  font-size: 12px;
  font-weight: 500;
}
.table-wrap.shadow-table-wrap {
  overflow-x: visible;
}
.parameter-comparison table {
  font-size: 13px;
  table-layout: fixed;
}
.parameter-comparison th,
.parameter-comparison td {
  padding: 8px 9px;
}
.shadow-gates th:nth-child(1),
.shadow-gates td:nth-child(1) { width: 18%; }
.shadow-gates th:nth-child(2),
.shadow-gates td:nth-child(2) { width: 30%; }
.shadow-gates th:nth-child(3),
.shadow-gates td:nth-child(3) { width: 16%; }
.shadow-gates th:nth-child(4),
.shadow-gates td:nth-child(4) { width: 36%; }
.shadow-weights th:nth-child(1),
.shadow-weights td:nth-child(1) { width: 34%; }
.shadow-weights th:nth-child(2),
.shadow-weights td:nth-child(2),
.shadow-weights th:nth-child(3),
.shadow-weights td:nth-child(3),
.shadow-weights th:nth-child(4),
.shadow-weights td:nth-child(4) { width: 22%; }
.result-methodology {
  margin: 0 0 8px;
  color: var(--muted);
  font-size: 12px;
  line-height: 1.5;
}
.shadow-results th:nth-child(1),
.shadow-results td:nth-child(1) { width: 17%; }
.shadow-results th:nth-child(2),
.shadow-results td:nth-child(2),
.shadow-results th:nth-child(3),
.shadow-results td:nth-child(3) { width: 18%; }
.shadow-results th:nth-child(4),
.shadow-results td:nth-child(4) { width: 13%; }
.shadow-results th:nth-child(5),
.shadow-results td:nth-child(5) { width: 34%; }
.metric-cell {
  font-weight: 700;
}
.shadow-results .value-cell,
.result-delta {
  white-space: normal;
}
.result-delta {
  font-weight: 700;
}
.parameter-comparison code {
  white-space: nowrap;
}
.value-cell {
  white-space: nowrap;
}
.cap-value-cell {
  white-space: normal;
}
.cap-value {
  display: flex;
  flex-direction: column;
  gap: 2px;
  line-height: 1.35;
}
.cap-value strong {
  font-weight: 700;
}
.cap-value small {
  color: var(--muted);
  font-size: 12px;
}
.candidate-cell strong {
  display: block;
  margin-bottom: 4px;
}
.note-cell {
  color: var(--muted);
  line-height: 1.5;
}
.change-chip {
  display: inline-flex;
  align-items: center;
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 2px 7px;
  font-size: 12px;
  font-weight: 700;
  white-space: nowrap;
}
.change-chip.changed {
  border-color: #e2a64b;
  background: #fff4df;
  color: #7a4b00;
}
.change-chip.unchanged {
  border-color: #bdd7c4;
  background: #eef8f0;
  color: #2d6a3f;
}
.change-chip + small {
  display: block;
  margin-top: 3px;
  color: var(--muted);
}
.header-meta, .summary-grid, .task-meta {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: 10px;
}
.header-meta span, .summary-item {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fbfcfd;
  padding: 10px 12px;
}
.summary-item span {
  display: block;
  color: var(--muted);
  font-size: 12px;
}
.summary-item strong {
  display: block;
  overflow-wrap: anywhere;
  font-size: 15px;
}
.table-wrap { overflow-x: auto; }
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 14px;
}
th, td {
  border-bottom: 1px solid var(--line);
  padding: 9px 10px;
  text-align: left;
  vertical-align: top;
}
th { color: var(--muted); font-size: 12px; }
code {
  font-family: Consolas, "Liberation Mono", monospace;
  font-size: 12px;
}
.status {
  display: inline-block;
  border: 1px solid var(--line);
  border-radius: 999px;
  padding: 2px 8px;
  font-size: 12px;
  font-weight: 700;
}
.status.ok { color: var(--ok); border-color: #9bd3ca; background: #edf8f6; }
.status.warn { color: var(--warn); border-color: #f1c680; background: #fff8eb; }
.status.danger { color: var(--danger); border-color: #f0a7a7; background: #fff1f1; }
.subtask-link-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 12px;
}
.subtask-card {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fbfcfd;
  padding: 14px;
}
.subtask-card.risk-medium {
  border-color: #f1c680;
  background: #fffaf0;
}
.subtask-card.risk-high {
  border-color: #f0a7a7;
  background: #fff5f5;
}
.subtask-card-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 10px;
  margin-bottom: 10px;
}
.subtask-card h3 {
  margin: 0 0 2px;
}
.subtask-conclusion {
  margin-bottom: 8px;
  font-weight: 700;
}
.subtask-risk {
  color: var(--muted);
  font-size: 13px;
}
.report-link-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 10px;
}
.report-link {
  display: inline-flex;
  flex-direction: column;
  min-width: 132px;
  max-width: 100%;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #ffffff;
  padding: 7px 9px;
  color: var(--ink);
  text-decoration: none;
}
.report-link:hover {
  border-color: #9fb2c8;
  background: #f5f8fb;
}
.report-link span {
  font-size: 13px;
  font-weight: 700;
}
.report-link small {
  color: var(--muted);
  font-size: 11px;
}
.report-link.missing {
  color: var(--muted);
  background: #f6f7f9;
}
.task-detail {
  border-top: 1px solid var(--line);
  padding-top: 12px;
}
footer {
  border-top: 1px solid var(--line);
  padding: 16px 0 28px;
  font-size: 13px;
}
@media (max-width: 760px) {
  header, main, footer { width: min(100% - 20px, 1180px); }
  .section-head { display: block; }
  .conclusion-grid { grid-template-columns: 1fr; }
  .shadow-table thead { display: none; }
  .shadow-table,
  .shadow-table tbody,
  .shadow-table tr,
  .shadow-table td {
    display: block;
    width: 100%;
  }
  .shadow-table tr {
    border-bottom: 1px solid var(--line);
    padding: 8px 0;
  }
  .shadow-table tr:last-child { border-bottom: 0; }
  .shadow-table td {
    border-bottom: 0;
    padding: 3px 0;
    white-space: normal;
  }
  .shadow-table td::before {
    content: attr(data-label);
    display: block;
    margin-bottom: 1px;
    color: var(--muted);
    font-size: 11px;
    font-weight: 700;
  }
  .candidate-cell strong { display: inline-block; margin-right: 6px; }
  .change-chip + small { display: inline-block; margin-left: 6px; }
}
"""
