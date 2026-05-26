from __future__ import annotations

import csv
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from html import escape
from pathlib import Path
from urllib.parse import quote

from ai_trading_system.core import ArtifactRef, ProductionEffect
from ai_trading_system.reports.daily_task_dashboard_view_model import (
    DailyTaskDashboardReport,
    DailyTaskDetail,
    DailyTaskKeyConclusion,
    TraceRecord,
)

DAILY_DECISION_SUMMARY_SCHEMA_VERSION = 1
PAPER_TRADING_TREND_WINDOWS: tuple[int, ...] = (7, 14, 30)
PAPER_TRADING_TREND_REPLAY_MODE = "daily_independent"


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
    "daily_shadow_weight_iteration": "生成每日 shadow 权重迭代报告",
}


def default_daily_task_dashboard_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_task_dashboard_{as_of.isoformat()}.html"


def default_daily_task_dashboard_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_task_dashboard_{as_of.isoformat()}.json"


def default_daily_decision_summary_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"daily_decision_summary_{as_of.isoformat()}.json"


def build_daily_task_dashboard_report(
    *,
    as_of: date,
    metadata_path: Path,
    reports_dir: Path,
    run_report_path: Path | None = None,
    paper_trading_trend_days: int = 7,
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
        paper_trading_trend_days=_normalize_paper_trading_trend_days(paper_trading_trend_days),
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


def write_daily_decision_summary_json(
    report: DailyTaskDashboardReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = build_daily_decision_summary_payload(report)
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
        "run_report_path": None if report.run_report_path is None else str(report.run_report_path),
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
        "paper_trading_summary": _paper_trading_summary(report),
        "paper_trading_trend": _paper_trading_trend(report),
        "paper_signal_quality": _paper_signal_quality_summary(report),
        "shadow_parameter_impact": _shadow_parameter_impact_summary(report),
        "weight_adjustment_candidates": _weight_adjustment_candidates_summary(report),
        "weight_candidate_evaluation": _weight_candidate_evaluation_summary(report),
        "weight_promotion_gate": _weight_promotion_gate_summary(report),
        "daily_weight_adjustment_summary": _daily_weight_adjustment_summary(report),
        "shadow_weight_iteration": _shadow_weight_iteration_summary(report),
        "shadow_vs_production_comparison": _shadow_vs_production_comparison_summary(report),
        "shadow_vs_production_multi_day_review": (
            _shadow_vs_production_multi_day_review_summary(report)
        ),
        "shadow_promotion_proposal": _shadow_promotion_proposal_summary(report),
        "shadow_promotion_apply_preflight": _shadow_promotion_apply_preflight_summary(report),
        "shadow_promotion_apply": _shadow_promotion_apply_summary(report),
        "shadow_promotion_rollback": _shadow_promotion_rollback_summary(report),
        "shadow_promotion_lifecycle_audit": _shadow_promotion_lifecycle_audit_summary(report),
        "parameter_governance_summary": _parameter_governance_summary_summary(report),
        "parameter_governance_web_view": _parameter_governance_web_view_summary(report),
        "parameter_governance_daily_digest": _parameter_governance_daily_digest_summary(report),
        "pipeline_health_summary": _pipeline_health_summary_summary(report),
        "data_freshness_summary": _data_freshness_summary_summary(report),
        "daily_trading_system_operator_brief": (
            _daily_trading_system_operator_brief_summary(report)
        ),
        "daily_operator_brief_scheduler_dry_run": (
            _daily_operator_brief_scheduler_dry_run_summary(report)
        ),
        "daily_operator_brief_scheduler_templates": (
            _daily_operator_brief_scheduler_templates_summary(report)
        ),
        "daily_operator_brief_scheduler_template_validation": (
            _daily_operator_brief_scheduler_template_validation_summary(report)
        ),
        "operator_brief_notification_draft": _operator_brief_notification_draft_summary(report),
        "operator_brief_notification_delivery_preflight": (
            _operator_brief_notification_delivery_preflight_summary(report)
        ),
        "operator_brief_notification_dispatch_preview": (
            _operator_brief_notification_dispatch_preview_summary(report)
        ),
        "operator_brief_notification_approval_gate": (
            _operator_brief_notification_approval_gate_summary(report)
        ),
        "operator_brief_notification_draft_dispatch": (
            _operator_brief_notification_draft_dispatch_summary(report)
        ),
        "notification_delivery_audit_summary": _notification_delivery_audit_summary(report),
        "notification_delivery_failure_classification": (
            _notification_delivery_failure_classification(report)
        ),
        "retry_candidate_queue": _retry_candidate_queue(report),
        "retry_execution_dry_run": _retry_execution_dry_run(report),
        "sec_pit_evaluation_summary": _sec_pit_evaluation_summary(report),
        "sec_pit_baseline_comparison": _sec_pit_baseline_comparison(report),
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


def build_daily_decision_summary_payload(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    dashboard_payload = _read_evidence_dashboard_payload(report)
    conclusions = _dashboard_key_conclusions_by_area(report)
    artifacts = _daily_decision_source_artifacts(report)
    hrefs = {
        artifact["id"]: artifact["href"]
        for artifact in artifacts
        if artifact.get("exists") and artifact.get("href")
    }
    checksums = {
        artifact["id"]: artifact["checksum_sha256"]
        for artifact in artifacts
        if artifact.get("checksum_sha256")
    }
    return {
        "schema_version": DAILY_DECISION_SUMMARY_SCHEMA_VERSION,
        "report_type": "daily_decision_summary",
        "as_of": report.as_of.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "run_id": report.run_id,
        "production_effect": ProductionEffect.NONE.value,
        "status": _decision_summary_status(conclusions),
        "decision_bus_role": {
            "upstream_for": "order_intent_candidate",
            "current_behavior": "read_only_no_trade",
            "order_intent_builder_connected": False,
        },
        "data_gate": _decision_summary_data_gate(report, dashboard_payload, conclusions),
        "investment_conclusion": _decision_summary_investment(
            report,
            dashboard_payload,
            conclusions,
        ),
        "parameter_governance": _decision_summary_parameter_governance(
            report,
            conclusions,
        ),
        "feedback_review": _decision_summary_feedback_review(
            report,
            dashboard_payload,
            conclusions,
        ),
        "system_health": _decision_summary_system_health(report, conclusions),
        "source_artifacts": artifacts,
        "hrefs": hrefs,
        "checksums": checksums,
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
            _render_paper_trading_summary(report),
            _render_paper_trading_trend(report),
            _render_paper_signal_quality(report),
            _render_shadow_parameter_impact(report),
            _render_weight_adjustment_candidates(report),
            _render_weight_candidate_evaluation(report),
            _render_weight_promotion_gate(report),
            _render_daily_weight_adjustment_summary(report),
            _render_shadow_weight_iteration(report),
            _render_shadow_vs_production_comparison(report),
            _render_shadow_vs_production_multi_day_review(report),
            _render_shadow_promotion_proposal(report),
            _render_shadow_promotion_apply_preflight(report),
            _render_shadow_promotion_apply(report),
            _render_shadow_promotion_rollback(report),
            _render_shadow_promotion_lifecycle_audit(report),
            _render_parameter_governance_summary(report),
            _render_parameter_governance_web_view(report),
            _render_parameter_governance_daily_digest(report),
            _render_pipeline_health_summary(report),
            _render_data_freshness_summary(report),
            _render_daily_trading_system_operator_brief(report),
            _render_daily_operator_brief_scheduler_dry_run(report),
            _render_daily_operator_brief_scheduler_templates(report),
            _render_daily_operator_brief_scheduler_template_validation(report),
            _render_operator_brief_notification_draft(report),
            _render_operator_brief_notification_delivery_preflight(report),
            _render_operator_brief_notification_dispatch_preview(report),
            _render_operator_brief_notification_approval_gate(report),
            _render_operator_brief_notification_draft_dispatch(report),
            _render_notification_delivery_audit_summary(report),
            _render_notification_delivery_failure_classification(report),
            _render_retry_candidate_queue(report),
            _render_retry_execution_dry_run(report),
            _render_sec_pit_evaluation_summary(report),
            _render_sec_pit_baseline_comparison(report),
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
        "sec_metrics": (_ReportSpec("SEC fundamentals", f"sec_fundamentals_{suffix}.md"),),
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
        "parameter_governance": (_ReportSpec("参数治理", f"parameter_governance_{suffix}.md"),),
        "market_feedback_optimization": (
            _ReportSpec("市场反馈优化", f"market_feedback_optimization_{suffix}.md"),
        ),
        "feedback_loop_review": (_ReportSpec("反馈闭环复核", f"feedback_loop_review_{suffix}.md"),),
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
        "secret_hygiene": (_ReportSpec("Secret hygiene", f"secret_hygiene_{suffix}.md"),),
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
        _shadow_iteration_key_conclusion(report),
        _operations_key_conclusion(tasks),
    ]
    return tuple(conclusion for conclusion in conclusions if conclusion is not None)


def _dashboard_key_conclusions_by_area(
    report: DailyTaskDashboardReport,
) -> dict[str, DailyTaskKeyConclusion]:
    return {conclusion.area: conclusion for conclusion in _build_key_conclusions(report)}


def _decision_summary_status(
    conclusions: dict[str, DailyTaskKeyConclusion],
) -> str:
    if not conclusions:
        return "missing"
    if any(conclusion.status == "FAIL" for conclusion in conclusions.values()):
        return "limited"
    if any(
        conclusion.risk_level != "none" or conclusion.status != "PASS"
        for conclusion in conclusions.values()
    ):
        return "limited"
    return "available"


def _decision_summary_data_gate(
    report: DailyTaskDashboardReport,
    dashboard_payload: TraceRecord,
    conclusions: dict[str, DailyTaskKeyConclusion],
) -> TraceRecord:
    decision = _mapping_value(dashboard_payload, "decision")
    data_gate = _string_value(decision.get("data_gate"))
    data_conclusion = conclusions.get("数据可信度")
    data_tasks = _existing_tasks(
        {task.step_id: task for task in report.tasks},
        (
            "download_data",
            "pit_snapshots",
            "sec_companyfacts",
            "sec_metrics",
            "sec_metrics_validation",
            "valuation_snapshots",
            "score_daily",
        ),
    )
    if not data_gate:
        data_gate = _first_detail_status(data_tasks, "数据质量门禁")
    blocking = _blocking_reasons(data_conclusion)
    if not data_gate:
        blocking = (*blocking, "data_gate 来源报告缺失或未暴露结构化状态。")
    return {
        "availability": "available" if data_gate else "missing",
        "status": data_gate or "MISSING",
        "blocking_reasons": list(blocking),
        "source_dashboard_key_conclusion": _key_conclusion_reference(data_conclusion),
    }


def _decision_summary_investment(
    report: DailyTaskDashboardReport,
    dashboard_payload: TraceRecord,
    conclusions: dict[str, DailyTaskKeyConclusion],
) -> TraceRecord:
    investment = conclusions.get("投资结论")
    decision = _mapping_value(dashboard_payload, "decision")
    action = _string_value(decision.get("action"))
    confidence = _string_value(decision.get("confidence"))
    position_band = _string_value(decision.get("final_risk_asset_ai_position"))
    risks = _string_list(dashboard_payload.get("top_invalidators"), limit=4)
    largest_constraint = _string_value(decision.get("largest_constraint"))
    if largest_constraint:
        risks = (largest_constraint, *risks)
    availability = "available" if action or confidence or position_band else "missing"
    major_risks = tuple(dict.fromkeys(item for item in risks if item))
    if availability == "missing":
        major_risks = (
            *major_risks,
            "evidence_dashboard JSON 缺失或缺少 decision；未合成投资动作、置信度或仓位。",
        )
    return {
        "availability": availability,
        "action_bias": action or "missing",
        "confidence": confidence or "missing",
        "position_band": position_band or "missing",
        "major_risks": list(major_risks),
        "source_dashboard_key_conclusion": _key_conclusion_reference(investment),
        "source_steps": list(investment.source_steps) if investment is not None else [],
        "production_effect": report.production_effect,
    }


def _decision_summary_parameter_governance(
    report: DailyTaskDashboardReport,
    conclusions: dict[str, DailyTaskKeyConclusion],
) -> TraceRecord:
    parameter = conclusions.get("参数治理")
    payload = _read_parameter_governance_summary(report)
    shadow_parameter = _latest_shadow_parameter_summary(report)
    shadow_iteration = _latest_shadow_iteration_summary(report)
    warnings = _string_list(payload.get("warnings"), limit=20)
    production_profile = _parameter_production_profile(payload)
    shadow_candidate = _parameter_shadow_candidate(shadow_parameter, shadow_iteration)
    promotion_status = (
        _string_value(shadow_parameter.get("promotion_status"))
        or _string_value(shadow_iteration.get("promotion_status"))
        or "NOT_EVALUATED"
    )
    blocking = tuple(
        dict.fromkeys(
            (
                *_blocking_reasons(parameter),
                *warnings,
                _string_value(shadow_parameter.get("risk")),
            )
        )
    )
    if not payload:
        blocking = (
            *blocking,
            "parameter_governance JSON 缺失；production profile 只能标记 missing。",
        )
    return {
        "availability": "available" if payload or shadow_parameter else "limited",
        "status": _string_value(payload.get("status"))
        or (parameter.status if parameter is not None else "MISSING"),
        "production_profile": production_profile,
        "shadow_candidate": shadow_candidate,
        "promotion_status": promotion_status,
        "blocking_reasons": [reason for reason in blocking if reason],
        "source_dashboard_key_conclusion": _key_conclusion_reference(parameter),
    }


def _decision_summary_feedback_review(
    report: DailyTaskDashboardReport,
    dashboard_payload: TraceRecord,
    conclusions: dict[str, DailyTaskKeyConclusion],
) -> TraceRecord:
    feedback_conclusion = conclusions.get("反馈复盘")
    feedback = _mapping_value(dashboard_payload, "feedback_review")
    market = _mapping_value(feedback, "market_feedback")
    loop = _mapping_value(feedback, "feedback_loop")
    investment = _mapping_value(feedback, "investment_review")
    shadow_parameter = _latest_shadow_parameter_summary(report)
    connected = any(_string_value(section.get("status")) for section in (market, loop, investment))
    summary = _join_nonempty(
        [
            _label("market readiness", market.get("readiness")),
            _string_value(market.get("current_conclusion")),
            _label("loop outcome", loop.get("outcome_summary")),
            _label("weekly position", investment.get("risk_asset_position_change")),
            _string_value(shadow_parameter.get("primary")),
        ]
    )
    if not summary and feedback_conclusion is not None:
        summary = feedback_conclusion.primary
    blocking = _blocking_reasons(feedback_conclusion)
    if not connected and not shadow_parameter:
        blocking = (
            *blocking,
            "feedback 子报告缺失或未接入；summary 标记为 missing。",
        )
    return {
        "availability": "available" if connected or shadow_parameter else "missing",
        "status": feedback_conclusion.status if feedback_conclusion is not None else "MISSING",
        "summary": summary or "missing",
        "market_feedback_status": _string_value(market.get("status")) or "MISSING",
        "feedback_loop_status": _string_value(loop.get("status")) or "MISSING",
        "investment_review_status": _string_value(investment.get("status")) or "MISSING",
        "blocking_reasons": list(blocking),
        "source_dashboard_key_conclusion": _key_conclusion_reference(feedback_conclusion),
    }


def _decision_summary_system_health(
    report: DailyTaskDashboardReport,
    conclusions: dict[str, DailyTaskKeyConclusion],
) -> TraceRecord:
    operations = conclusions.get("运行健康")
    health_tasks = [
        task for task in report.tasks if task.step_id in {"pipeline_health", "secret_hygiene"}
    ]
    warnings = [
        f"{task.step_id}: {task.important_risk}"
        for task in health_tasks
        if task.risk_level != "none"
    ]
    if operations is None:
        warnings.append("pipeline health / secret hygiene 步骤缺失。")
    return {
        "availability": "available" if operations is not None else "missing",
        "status": operations.status if operations is not None else "MISSING",
        "warnings": list(dict.fromkeys(warnings)),
        "run_status": report.status,
        "failed_count": report.failed_count,
        "skipped_count": report.skipped_count,
        "source_dashboard_key_conclusion": _key_conclusion_reference(operations),
    }


def _read_parameter_governance_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = report.reports_dir / f"parameter_governance_{report.as_of.isoformat()}.json"
    payload = _read_json_object(path)
    if not payload:
        fallback = (
            report.project_root
            / "outputs"
            / "reports"
            / f"parameter_governance_{report.as_of.isoformat()}.json"
        )
        if fallback != path:
            payload = _read_json_object(fallback)
    if payload.get("report_type") != "parameter_governance":
        return {}
    return payload


def _parameter_production_profile(payload: TraceRecord) -> TraceRecord:
    if not payload:
        return {
            "availability": "missing",
            "manifest_version": "missing",
            "manifest_status": "missing",
            "owner_quantitative_input_status": "missing",
            "action_counts": {},
        }
    action_counts = payload.get("action_counts")
    return {
        "availability": "available",
        "manifest_version": _string_value(payload.get("manifest_version")) or "missing",
        "manifest_status": _string_value(payload.get("manifest_status")) or "missing",
        "owner_quantitative_input_status": (
            _string_value(payload.get("owner_quantitative_input_status")) or "missing"
        ),
        "candidate_ledger_status": (
            _string_value(payload.get("candidate_ledger_status")) or "missing"
        ),
        "candidate_evaluation_mode": (
            _string_value(payload.get("candidate_evaluation_mode")) or "missing"
        ),
        "action_counts": action_counts if isinstance(action_counts, dict) else {},
    }


def _parameter_shadow_candidate(
    shadow_parameter: TraceRecord,
    shadow_iteration: TraceRecord,
) -> TraceRecord:
    if shadow_parameter:
        return {
            "availability": "available",
            "source": "shadow_parameter_search",
            "run_id": _string_value(shadow_parameter.get("run_id")) or "missing",
            "selected_trial_id": (
                _string_value(shadow_parameter.get("selected_trial_id")) or "missing"
            ),
            "selected_kind": _string_value(shadow_parameter.get("selected_kind")) or "missing",
            "summary": _string_value(shadow_parameter.get("primary")) or "missing",
        }
    if shadow_iteration:
        best_gate = _mapping_value(shadow_iteration, "best_gate_only")
        return {
            "availability": "limited",
            "source": "shadow_iteration",
            "run_id": _string_value(shadow_iteration.get("source_search_run_id")) or "missing",
            "selected_trial_id": _string_value(best_gate.get("trial_id")) or "missing",
            "selected_kind": "shadow_iteration_gate_only",
            "summary": _shadow_iteration_candidate_label(best_gate),
        }
    return {
        "availability": "missing",
        "source": "missing",
        "run_id": "missing",
        "selected_trial_id": "missing",
        "selected_kind": "missing",
        "summary": "missing",
    }


def _daily_decision_source_artifacts(report: DailyTaskDashboardReport) -> list[TraceRecord]:
    records: list[TraceRecord] = []
    records.append(
        _source_artifact_record(
            artifact_id="daily_ops_metadata",
            label="daily ops metadata",
            path=report.metadata_path,
            reports_dir=report.reports_dir,
        )
    )
    if report.run_report_path is not None:
        records.append(
            _source_artifact_record(
                artifact_id="daily_ops_run_report",
                label="daily ops run report",
                path=report.run_report_path,
                reports_dir=report.reports_dir,
            )
        )
    for task in report.tasks:
        for index, detail in enumerate(task.detail_reports, start=1):
            records.append(
                _source_artifact_record(
                    artifact_id=(
                        f"{task.step_id}:{index}:" f"{_slug(str(detail.get('label') or 'report'))}"
                    ),
                    label=str(detail.get("label") or "report"),
                    path=Path(str(detail.get("path") or "")),
                    reports_dir=report.reports_dir,
                )
            )
    suffix = report.as_of.isoformat()
    extras = (
        (
            "parameter_governance_json",
            "parameter governance JSON",
            report.reports_dir / f"parameter_governance_{suffix}.json",
        ),
        (
            "shadow_iteration_json",
            "shadow iteration JSON",
            report.reports_dir / f"shadow_iteration_{suffix}.json",
        ),
        (
            "daily_task_dashboard_json",
            "daily task dashboard JSON",
            report.reports_dir / f"daily_task_dashboard_{suffix}.json",
        ),
        (
            "paper_trading_summary_json",
            "paper trading summary JSON",
            report.reports_dir / f"paper_trading_summary_{suffix}.json",
        ),
        (
            "paper_signal_quality_json",
            "paper signal quality JSON",
            report.reports_dir / f"paper_signal_quality_{suffix}.json",
        ),
        (
            "shadow_parameter_impact_json",
            "shadow parameter impact JSON",
            report.reports_dir / f"shadow_parameter_impact_{suffix}.json",
        ),
        (
            "weight_adjustment_candidates_json",
            "weight adjustment candidates JSON",
            report.reports_dir / f"weight_adjustment_candidates_{suffix}.json",
        ),
        (
            "weight_candidate_evaluation_json",
            "weight candidate evaluation JSON",
            report.reports_dir / f"weight_candidate_evaluation_{suffix}.json",
        ),
        (
            "weight_promotion_gate_json",
            "weight promotion gate JSON",
            report.reports_dir / f"weight_promotion_gate_{suffix}.json",
        ),
        (
            "daily_weight_adjustment_summary_json",
            "daily weight adjustment summary JSON",
            report.reports_dir / f"daily_weight_adjustment_summary_{suffix}.json",
        ),
        (
            "shadow_vs_production_comparison_json",
            "shadow vs production comparison JSON",
            report.project_root
            / "data"
            / "derived"
            / "weight_iterations"
            / "comparison"
            / f"daily_shadow_vs_production_{suffix}.json",
        ),
        (
            "shadow_vs_production_multi_day_review_json",
            "shadow vs production multi-day review JSON",
            _latest_shadow_vs_production_review_path(report),
        ),
        (
            "shadow_promotion_proposal_json",
            "shadow promotion proposal JSON",
            _latest_shadow_promotion_proposal_path(report),
        ),
        (
            "shadow_promotion_apply_preflight_json",
            "shadow promotion apply preflight JSON",
            _latest_shadow_promotion_apply_preflight_path(report),
        ),
        (
            "shadow_promotion_apply_json",
            "shadow promotion apply result JSON",
            _latest_shadow_promotion_apply_path(report),
        ),
        (
            "shadow_promotion_rollback_json",
            "shadow promotion rollback result JSON",
            _latest_shadow_promotion_rollback_path(report),
        ),
        (
            "shadow_promotion_lifecycle_audit_json",
            "shadow promotion lifecycle audit JSON",
            _latest_shadow_promotion_lifecycle_audit_path(report),
        ),
        (
            "parameter_governance_summary_json",
            "parameter governance summary JSON",
            _latest_parameter_governance_summary_path(report),
        ),
        (
            "parameter_governance_daily_digest_json",
            "parameter governance daily digest JSON",
            _latest_parameter_governance_daily_digest_path(report),
        ),
        (
            "pipeline_health_summary_json",
            "pipeline health summary JSON",
            _latest_pipeline_health_summary_path(report),
        ),
        (
            "data_freshness_summary_json",
            "data freshness summary JSON",
            _latest_data_freshness_summary_path(report),
        ),
        (
            "daily_trading_system_operator_brief_json",
            "daily trading system operator brief JSON",
            _latest_daily_trading_system_operator_brief_path(report),
        ),
    )
    for artifact_id, label, path in extras:
        records.append(
            _source_artifact_record(
                artifact_id=artifact_id,
                label=label,
                path=path,
                reports_dir=report.reports_dir,
            )
        )
    unique: dict[str, TraceRecord] = {}
    for record in records:
        unique[str(record["id"])] = record
    return list(unique.values())


def _source_artifact_record(
    *,
    artifact_id: str,
    label: str,
    path: Path,
    reports_dir: Path,
) -> TraceRecord:
    ref = ArtifactRef.from_path(path)
    return {
        "id": artifact_id,
        "label": label,
        "path": str(path),
        "href": _report_href(path, reports_dir),
        "exists": ref.exists,
        "artifact_type": ref.artifact_type,
        "checksum_sha256": ref.sha256,
        "size_bytes": ref.size_bytes,
    }


def _key_conclusion_reference(
    conclusion: DailyTaskKeyConclusion | None,
) -> TraceRecord:
    if conclusion is None:
        return {
            "availability": "missing",
            "area": "missing",
            "status": "MISSING",
            "primary": "missing",
            "source_steps": [],
        }
    return {
        "availability": "available",
        "area": conclusion.area,
        "title": conclusion.title,
        "status": conclusion.status,
        "primary": conclusion.primary,
        "important_risk": conclusion.important_risk,
        "source_steps": list(conclusion.source_steps),
    }


def _blocking_reasons(conclusion: DailyTaskKeyConclusion | None) -> tuple[str, ...]:
    if conclusion is None:
        return ()
    if conclusion.risk_level == "none":
        return ()
    return tuple(
        reason.strip() for reason in conclusion.important_risk.split("；") if reason.strip()
    )


def _slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._=-]+", "_", value.strip())
    return slug.strip("._-") or "artifact"


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
        primary = "当日投资结论受限：evidence_dashboard JSON 未生成或缺少 decision。"
    risk = _join_nonempty([largest_constraint, *invalidators])
    if not risk and score_task is not None:
        risk = _join_nonempty(
            [
                "evidence_dashboard JSON 缺失或缺少 decision；不从任务状态补造投资结论。",
                score_task.important_risk,
            ]
        )
    source_tasks = tuple(step for step in ("score_daily", "reports_dashboard") if step in tasks)
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
        status=_combined_status(feedback_tasks) if feedback_tasks else "PASS_WITH_LIMITATIONS",
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
            *(("shadow_parameter_search",) if shadow_parameter.get("connected") is True else ()),
        ),
        result_comparison=tuple(
            row for row in shadow_parameter.get("result_comparison", ()) if isinstance(row, dict)
        ),
        result_methodology=_string_value(shadow_parameter.get("result_methodology")),
        parameter_comparison=tuple(
            row for row in shadow_parameter.get("parameter_comparison", ()) if isinstance(row, dict)
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


def _shadow_iteration_key_conclusion(
    report: DailyTaskDashboardReport,
) -> DailyTaskKeyConclusion | None:
    summary = _latest_shadow_iteration_summary(report)
    if not summary:
        return None
    active_count = _optional_int(summary.get("active_candidate_count"))
    primary_driver = _string_value(summary.get("primary_driver")) or "unknown"
    best_weight = _mapping_value(summary, "best_weight_only")
    best_gate = _mapping_value(summary, "best_gate_only")
    best_bundle = _mapping_value(summary, "best_weight_gate_bundle")
    blocked_reasons = _string_list(summary.get("blocked_reasons"), limit=3)
    primary = _join_nonempty(
        [
            _label("active candidates", active_count),
            _label("primary driver", primary_driver),
            _label("best weight-only", _shadow_iteration_candidate_label(best_weight)),
            _label("best gate-only", _shadow_iteration_candidate_label(best_gate)),
            _label("best bundle", _shadow_iteration_candidate_label(best_bundle)),
        ]
    )
    supporting = tuple(
        item
        for item in (
            _label("next action", summary.get("next_action")),
            _label("source search", summary.get("source_search_run_id")),
            "production 参数未改变；dashboard 只读取 shadow_iteration JSON，不重算结果。",
        )
        if item
    )
    risk = "；".join(blocked_reasons) or "当前 shadow iteration 未报告 blocked reasons。"
    return DailyTaskKeyConclusion(
        area="Shadow Iteration",
        title="Shadow 参数持续迭代状态",
        status=_string_value(summary.get("status")) or "PASS_WITH_LIMITATIONS",
        primary=primary or "Shadow iteration JSON 缺少候选摘要。",
        supporting=supporting,
        important_risk=risk,
        risk_level="medium" if blocked_reasons else "none",
        source_steps=("shadow_iteration",),
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


def _paper_trading_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    path = report.reports_dir / f"paper_trading_summary_{suffix}.json"
    payload = _read_json_object(path)
    if payload.get("report_type") != "paper_trading_summary":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "production_effect": ProductionEffect.NONE.value,
            "candidate_count": "missing",
            "blocked_candidates": "missing",
            "generated_intents": "missing",
            "approved": "missing",
            "rejected": "missing",
            "submitted": "missing",
            "filled": "missing",
            "open": "missing",
            "cancelled": "missing",
            "realized_pnl": "missing",
            "unrealized_pnl": "missing",
            "reconciliation_status": "MISSING",
            "audit_log_path": "missing",
            "report_path": "missing",
            "report_href": "",
            "risk": "paper trading summary JSON 缺失；dashboard 不补造执行复盘。",
        }
    report_path = _string_value(payload.get("report_path"))
    audit_log_path = _string_value(payload.get("audit_log_path"))
    reconciliation_status = _string_value(payload.get("reconciliation_status")) or "MISSING"
    summary_status = _string_value(payload.get("status")) or reconciliation_status
    return {
        "status": summary_status,
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "production_effect": _string_value(payload.get("production_effect")) or "none",
        "candidate_count": payload.get("candidate_count", "missing"),
        "blocked_candidates": payload.get("blocked_candidates", "missing"),
        "generated_intents": payload.get("generated_intents", "missing"),
        "approved": payload.get("approved", "missing"),
        "rejected": payload.get("rejected", "missing"),
        "submitted": payload.get("submitted", "missing"),
        "filled": payload.get("filled", "missing"),
        "open": payload.get("open", "missing"),
        "cancelled": payload.get("cancelled", "missing"),
        "realized_pnl": payload.get("realized_pnl", "missing"),
        "unrealized_pnl": payload.get("unrealized_pnl", "missing"),
        "reconciliation_status": reconciliation_status,
        "audit_log_path": audit_log_path or "missing",
        "report_path": report_path or "missing",
        "report_href": _report_href(Path(report_path), report.reports_dir) if report_path else "",
        "risk": _paper_trading_summary_risk(payload),
    }


def _paper_trading_summary_risk(payload: TraceRecord) -> str:
    status = _string_value(payload.get("status"))
    production_effect = _string_value(payload.get("production_effect"))
    reconciliation_status = _string_value(payload.get("reconciliation_status"))
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("paper trading summary production_effect 不是 none。")
    if status == "ERROR":
        risks.append("paper trading runner 标记 ERROR；执行复盘不能视为完整。")
    elif status == "LIMITED":
        risks.append("paper trading runner 标记 LIMITED；候选或执行覆盖受限。")
    if reconciliation_status in {"BLOCK", "MISSING", ""}:
        risks.append("portfolio reconciliation 未通过或缺失。")
    return "；".join(risks) or "未发现 paper trading 执行复盘阻断风险。"


def _paper_trading_trend(report: DailyTaskDashboardReport) -> TraceRecord:
    selected_days = _normalize_paper_trading_trend_days(report.paper_trading_trend_days)
    windows = {
        str(days): _paper_trading_trend_window(report, days) for days in PAPER_TRADING_TREND_WINDOWS
    }
    selected = dict(windows[str(selected_days)])
    latest_replay = _latest_paper_trading_replay_summary(report)
    selected["latest_replay"] = latest_replay
    if latest_replay.get("exists"):
        selected["replay_mode"] = latest_replay["replay_mode"]
        selected["portfolio_carry_forward"] = latest_replay["portfolio_carry_forward"]
        selected["latest_replay_mode"] = latest_replay["replay_mode"]
        if latest_replay.get("replay_mode") == "continuous_portfolio":
            selected["continuous_portfolio_summary"] = _mapping_value(
                latest_replay,
                "continuous_portfolio_summary",
            )
            selected["risk"] = _paper_trading_trend_risk_with_continuous_replay(
                _string_value(selected.get("risk")),
            )
    else:
        selected["latest_replay_mode"] = selected["replay_mode"]
    selected["windows"] = windows
    selected["available_windows"] = list(windows)
    return selected


def _paper_trading_trend_window(
    report: DailyTaskDashboardReport,
    days: int,
) -> TraceRecord:
    start = report.as_of - timedelta(days=days - 1)
    records: list[TraceRecord] = []
    totals: TraceRecord = {
        "candidate_count": 0,
        "blocked_candidates": 0,
        "generated_intents": 0,
        "approved": 0,
        "rejected": 0,
        "submitted": 0,
        "filled": 0,
        "open": 0,
        "cancelled": 0,
        "realized_pnl": 0.0,
        "unrealized_pnl": 0.0,
    }
    reconciliation_distribution: dict[str, int] = {}
    market_snapshot_source_distribution: Counter[str] = Counter()
    blocked_by_distribution: Counter[str] = Counter()
    reason_code_distribution: Counter[str] = Counter()
    missing_dates: list[str] = []
    missing_candidate_dates: list[str] = []
    non_none_production_effect_dates: list[str] = []
    synthetic_snapshot_dates: list[str] = []

    for offset in range(days):
        current = start + timedelta(days=offset)
        path = report.reports_dir / f"paper_trading_summary_{current.isoformat()}.json"
        payload = _read_json_object(path)
        if payload.get("report_type") != "paper_trading_summary":
            missing_dates.append(current.isoformat())
            records.append(
                {
                    "as_of": current.isoformat(),
                    "exists": False,
                    "status": "MISSING",
                    "path": str(path),
                    "href": _report_href(path, report.reports_dir),
                    "production_effect": ProductionEffect.NONE.value,
                    "candidate_count": "missing",
                    "generated_intents": "missing",
                    "submitted": "missing",
                    "filled": "missing",
                    "open": "missing",
                    "cancelled": "missing",
                    "realized_pnl": "missing",
                    "unrealized_pnl": "missing",
                    "reconciliation_status": "MISSING",
                    "market_snapshot_source": "missing",
                }
            )
            continue

        production_effect = _string_value(payload.get("production_effect")) or "none"
        if production_effect != ProductionEffect.NONE.value:
            non_none_production_effect_dates.append(current.isoformat())
        reconciliation_status = _string_value(payload.get("reconciliation_status")) or "MISSING"
        reconciliation_distribution[reconciliation_status] = (
            reconciliation_distribution.get(reconciliation_status, 0) + 1
        )
        source_counts = _paper_trading_snapshot_source_counts(payload)
        market_snapshot_source_distribution.update(source_counts)
        if source_counts.get("synthetic_limit_price", 0) > 0:
            synthetic_snapshot_dates.append(current.isoformat())
        candidate_path = report.reports_dir / f"order_intent_candidates_{current.isoformat()}.json"
        if not _add_paper_candidate_explanations(
            candidate_path,
            blocked_by_distribution,
            reason_code_distribution,
        ):
            missing_candidate_dates.append(current.isoformat())
        record = {
            "as_of": current.isoformat(),
            "exists": True,
            "status": _string_value(payload.get("status")) or reconciliation_status,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "production_effect": production_effect,
            "candidate_count": payload.get("candidate_count", 0),
            "blocked_candidates": payload.get("blocked_candidates", 0),
            "generated_intents": payload.get("generated_intents", 0),
            "approved": payload.get("approved", 0),
            "rejected": payload.get("rejected", 0),
            "submitted": payload.get("submitted", 0),
            "filled": payload.get("filled", 0),
            "open": payload.get("open", 0),
            "cancelled": payload.get("cancelled", 0),
            "realized_pnl": payload.get("realized_pnl", 0.0),
            "unrealized_pnl": payload.get("unrealized_pnl", 0.0),
            "reconciliation_status": reconciliation_status,
            "market_snapshot_source": _string_value(payload.get("market_snapshot_source"))
            or "none",
            "candidate_explanation_path": str(candidate_path),
        }
        for key in (
            "candidate_count",
            "blocked_candidates",
            "generated_intents",
            "approved",
            "rejected",
            "submitted",
            "filled",
            "open",
            "cancelled",
        ):
            totals[key] = (_optional_int(totals.get(key)) or 0) + (
                _optional_int(record.get(key)) or 0
            )
        totals["realized_pnl"] = (_optional_float(totals.get("realized_pnl")) or 0.0) + (
            _optional_float(record.get("realized_pnl")) or 0.0
        )
        totals["unrealized_pnl"] = (_optional_float(totals.get("unrealized_pnl")) or 0.0) + (
            _optional_float(record.get("unrealized_pnl")) or 0.0
        )
        records.append(record)

    available_count = days - len(missing_dates)
    snapshot_total = sum(market_snapshot_source_distribution.values())
    synthetic_snapshot_count = market_snapshot_source_distribution.get(
        "synthetic_limit_price",
        0,
    )
    synthetic_snapshot_ratio = synthetic_snapshot_count / snapshot_total if snapshot_total else 0.0
    status = "PASS"
    risks: list[str] = []
    existing_statuses = {str(record.get("status")) for record in records if record.get("exists")}
    if non_none_production_effect_dates:
        status = "ERROR"
        risks.append(
            "存在 production_effect 非 none 的 paper summary："
            + ", ".join(non_none_production_effect_dates)
        )
    if "ERROR" in existing_statuses:
        status = "ERROR"
        risks.append("至少一个 paper trading summary 标记 ERROR。")
    if missing_dates:
        if status != "ERROR":
            status = "LIMITED"
        risks.append("历史 paper_trading_summary 缺失；dashboard 不补造趋势结论。")
    if missing_candidate_dates:
        if status != "ERROR":
            status = "LIMITED"
        risks.append("部分 order_intent_candidates 缺失；top blocked_by/reason_code 受限。")
    if any(item not in {"PASS", "MISSING"} for item in existing_statuses):
        if status != "ERROR":
            status = "LIMITED"
        risks.append("至少一个 paper trading summary 不是 PASS，趋势只能作为受限观察。")
    if synthetic_snapshot_count:
        risks.append("存在 synthetic limit price snapshot；fill 质量解释受限。")

    return {
        "status": status,
        "production_effect": ProductionEffect.NONE.value,
        "production_position_effect": ProductionEffect.NONE.value,
        "parameter_promotion_effect": ProductionEffect.NONE.value,
        "replay_mode": PAPER_TRADING_TREND_REPLAY_MODE,
        "portfolio_carry_forward": False,
        "execution_boundary": {
            "read_only": True,
            "runs_paper_runner": False,
            "runs_replay": False,
            "broker_api_allowed": False,
            "changes_production_position_recommendation": False,
            "changes_parameter_promotion": False,
        },
        "window_days": days,
        "start": start.isoformat(),
        "end": report.as_of.isoformat(),
        "available_count": available_count,
        "missing_count": len(missing_dates),
        "missing_dates": missing_dates,
        "missing_candidate_count": len(missing_candidate_dates),
        "missing_candidate_dates": missing_candidate_dates,
        "totals": totals,
        "reconciliation_status_distribution": dict(sorted(reconciliation_distribution.items())),
        "market_snapshot_source_distribution": dict(
            sorted(market_snapshot_source_distribution.items())
        ),
        "synthetic_snapshot_count": synthetic_snapshot_count,
        "synthetic_snapshot_ratio": synthetic_snapshot_ratio,
        "synthetic_snapshot_dates": synthetic_snapshot_dates,
        "top_blocked_by": _top_counter_records(blocked_by_distribution),
        "top_reason_code": _top_counter_records(reason_code_distribution),
        "quality_flags": {
            "missing_summary_days": len(missing_dates),
            "missing_candidate_days": len(missing_candidate_dates),
            "synthetic_snapshot_days": len(synthetic_snapshot_dates),
            "synthetic_snapshot_count": synthetic_snapshot_count,
        },
        "daily_results": records,
        "risk": "；".join(dict.fromkeys(risks))
        or ("最近 paper trading summary 输入完整；趋势仍仅为 paper-only " "逐日独立复盘。"),
    }


def _latest_paper_trading_replay_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    candidates: list[tuple[date, datetime, str, Path, TraceRecord]] = []
    for path in report.reports_dir.glob("paper_trading_replay_*.json"):
        payload = _read_json_object(path)
        if payload.get("report_type") != "paper_trading_replay":
            continue
        end_date = _parse_iso_date(_string_value(payload.get("end")))
        if end_date is None or end_date > report.as_of:
            continue
        generated_at = _parse_iso_datetime(_string_value(payload.get("generated_at")))
        candidates.append((end_date, generated_at, path.name, path, payload))
    if not candidates:
        return {
            "exists": False,
            "path": "",
            "href": "",
            "replay_mode": PAPER_TRADING_TREND_REPLAY_MODE,
            "portfolio_carry_forward": False,
            "production_effect": ProductionEffect.NONE.value,
            "risk": "未找到已有 paper_trading_replay JSON；趋势保持 daily-independent warning。",
        }

    _, _, _, path, payload = max(candidates)
    replay_mode = _string_value(payload.get("replay_mode")) or PAPER_TRADING_TREND_REPLAY_MODE
    carry_forward = _bool_value(payload.get("portfolio_carry_forward"))
    final_positions = _records(payload.get("final_positions"))
    final_positions_count = _optional_int(payload.get("carried_positions_count"))
    if final_positions_count is None:
        final_positions_count = len(final_positions)
    max_drawdown = _mapping_value(payload, "max_drawdown")
    max_drawdown_pct = _optional_float(payload.get("max_drawdown_pct"))
    if max_drawdown_pct is None:
        max_drawdown_pct = _optional_float(max_drawdown.get("percent"))
    continuous_summary = {
        "final_equity": payload.get("final_equity"),
        "final_cash": payload.get("final_cash"),
        "max_drawdown_pct": max_drawdown_pct,
        "exposure_peak": payload.get("exposure_peak"),
        "final_positions_count": final_positions_count,
        "portfolio_carry_forward": carry_forward,
        "expired_day_orders": payload.get("expired_day_orders", 0),
    }
    return {
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "start": _string_value(payload.get("start")),
        "end": _string_value(payload.get("end")),
        "replay_mode": replay_mode,
        "portfolio_carry_forward": carry_forward,
        "production_effect": _string_value(payload.get("production_effect")) or "none",
        "continuous_metrics_available": bool(payload.get("continuous_metrics_available")),
        "order_expiration_policy": _string_value(payload.get("order_expiration_policy")),
        "unsupported_order_policy": _string_value(payload.get("unsupported_order_policy")),
        "continuous_portfolio_summary": continuous_summary,
        "risk": _latest_replay_risk(payload),
    }


def _paper_trading_trend_risk_with_continuous_replay(base_risk: str) -> str:
    continuous_note = (
        "最近 existing replay 为 continuous-portfolio；dashboard 只读展示 final "
        "portfolio summary，仍不触发 replay 或 broker。"
    )
    daily_only = "最近 paper trading summary 输入完整；趋势仍仅为 paper-only 逐日独立复盘。"
    if base_risk == daily_only:
        return continuous_note
    return "；".join(item for item in (base_risk, continuous_note) if item)


def _latest_replay_risk(payload: TraceRecord) -> str:
    replay_mode = _string_value(payload.get("replay_mode")) or PAPER_TRADING_TREND_REPLAY_MODE
    if replay_mode == "continuous_portfolio":
        return (
            "latest replay 是 paper-only continuous-portfolio 模拟；不是实盘收益、"
            "真实 broker 成交或上线依据。"
        )
    return "latest replay 是 daily-independent；每天重新初始化组合，不能解释连续组合收益。"


def _paper_signal_quality_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    path = report.reports_dir / f"paper_signal_quality_{suffix}.json"
    payload = _read_json_object(path)
    if payload.get("report_type") != "paper_signal_quality":
        return {
            "status": "MISSING",
            "evaluation_status": "INSUFFICIENT_DATA",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "production_effect": ProductionEffect.NONE.value,
            "observe_only": True,
            "primary_blocked_by": "missing",
            "synthetic_snapshot_ratio": "missing",
            "sample_count": "missing",
            "risk": "paper signal quality JSON 缺失；dashboard 不补造质量评价。",
        }
    summary = _mapping_value(payload, "summary")
    outputs = _mapping_value(payload, "outputs")
    report_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(report_path, report.reports_dir) if report_path.exists() else ""
    production_effect = _string_value(payload.get("production_effect")) or "none"
    evaluation_status = _string_value(payload.get("evaluation_status")) or "INSUFFICIENT_DATA"
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("paper signal quality production_effect 不是 none。")
    if evaluation_status != "OBSERVE_ONLY":
        gate = _mapping_value(payload, "evaluation_gate")
        reasons = _strings(gate.get("blocked_by")) or _strings(gate.get("blocking_reasons"))
        reason_text = ", ".join(reasons) if reasons else evaluation_status
        risks.append(f"evaluation_gate 限制：{reason_text}。")
    return {
        "status": evaluation_status,
        "evaluation_status": evaluation_status,
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "production_effect": production_effect,
        "observe_only": True,
        "primary_blocked_by": summary.get("primary_blocked_by", "none"),
        "synthetic_snapshot_ratio": summary.get("synthetic_snapshot_ratio", 0.0),
        "sample_count": summary.get("sample_count", 0),
        "risk": "；".join(risks) or "Paper signal quality 当前未触发评价限制。",
    }


def _shadow_parameter_impact_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    path = report.reports_dir / f"shadow_parameter_impact_{suffix}.json"
    payload = _read_json_object(path)
    if payload.get("report_type") != "shadow_parameter_impact":
        return {
            "status": "MISSING",
            "impact_status": "INSUFFICIENT_DATA",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "production_effect": ProductionEffect.NONE.value,
            "observe_only": True,
            "main_blocked_by": "missing",
            "warnings": ["missing"],
            "window_sample_counts": {
                "7": {"production": 0, "shadow": 0, "unknown": 0},
                "14": {"production": 0, "shadow": 0, "unknown": 0},
                "30": {"production": 0, "shadow": 0, "unknown": 0},
            },
            "production_vs_shadow_filled_count": {"production": "missing", "shadow": "missing"},
            "production_vs_shadow_paper_pnl": {"production": "missing", "shadow": "missing"},
            "continuous_replay_available": False,
            "continuous_replay_mode": "missing",
            "risk": "shadow parameter impact JSON 缺失；dashboard 不补造 impact 结论。",
        }
    summary = _mapping_value(payload, "summary")
    gate = _mapping_value(payload, "impact_gate")
    outputs = _mapping_value(payload, "outputs")
    report_path = Path(
        _string_value(outputs.get("markdown")) or str(path.with_suffix(".md")),
    )
    report_href = _report_href(report_path, report.reports_dir) if report_path.exists() else ""
    production_effect = _string_value(payload.get("production_effect")) or "none"
    impact_status = _string_value(payload.get("impact_status")) or "INSUFFICIENT_DATA"
    window_sample_counts = _shadow_impact_window_sample_counts(payload)
    comparison = _mapping_value(payload, "profile_comparison")
    production = _mapping_value(comparison, "production")
    shadow = _mapping_value(comparison, "shadow")
    continuous = _mapping_value(payload, "continuous_replay")
    blockers = _strings(gate.get("blocked_by")) or _strings(gate.get("blocking_reasons"))
    warnings = _strings(gate.get("warnings")) or _strings(payload.get("warning_codes"))
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("shadow impact production_effect 不是 none。")
    if blockers:
        risks.append(f"impact_gate 限制：{', '.join(blockers)}。")
    if warnings:
        risks.append(f"warnings：{', '.join(warnings)}。")
    return {
        "status": impact_status,
        "impact_status": impact_status,
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "production_effect": production_effect,
        "observe_only": True,
        "main_blocked_by": summary.get("main_blocked_by") or (blockers[0] if blockers else "none"),
        "warnings": warnings,
        "window_sample_counts": window_sample_counts,
        "production_vs_shadow_filled_count": {
            "production": production.get("filled_count", 0),
            "shadow": shadow.get("filled_count", 0),
        },
        "production_vs_shadow_paper_pnl": {
            "production": production.get("paper_pnl_total", 0.0),
            "shadow": shadow.get("paper_pnl_total", 0.0),
        },
        "continuous_replay_available": bool(continuous.get("available")),
        "continuous_replay_mode": continuous.get("replay_mode", "daily_independent"),
        "risk": "；".join(risks) or "Shadow impact 当前未触发阻断性限制。",
    }


def _shadow_impact_window_sample_counts(payload: TraceRecord) -> TraceRecord:
    windows = _mapping_value(payload, "windows")
    sample_counts: TraceRecord = {}
    for window_days in PAPER_TRADING_TREND_WINDOWS:
        window = _mapping_value(windows, str(window_days))
        summary = _mapping_value(window, "summary")
        counts = _mapping_value(summary, "sample_counts")
        sample_counts[str(window_days)] = {
            "production": counts.get("production", 0),
            "shadow": counts.get("shadow", 0),
            "unknown": counts.get("unknown", 0),
        }
    return sample_counts


def _weight_adjustment_candidates_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    path = report.reports_dir / f"weight_adjustment_candidates_{suffix}.json"
    payload = _read_json_object(path)
    if payload.get("report_type") != "weight_adjustment_candidates":
        return {
            "status": "MISSING",
            "gate_status": "LIMITED",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "candidate_count": 0,
            "top_candidate_id": "",
            "main_blocked_by": "missing",
            "blocked_by": ["missing"],
            "production_effect": ProductionEffect.NONE.value,
            "mode": "observe_only",
            "risk": "weight adjustment candidate JSON 缺失；dashboard 不生成候选或调参。",
        }
    summary = _mapping_value(payload, "summary")
    gate = _mapping_value(payload, "candidate_gate")
    outputs = _mapping_value(payload, "outputs")
    report_path = Path(
        _string_value(outputs.get("markdown")) or str(path.with_suffix(".md")),
    )
    report_href = _report_href(report_path, report.reports_dir) if report_path.exists() else ""
    production_effect = _string_value(payload.get("production_effect")) or "none"
    gate_status = _string_value(payload.get("gate_status")) or _string_value(
        summary.get("gate_status")
    )
    blocked_by = _strings(gate.get("blocked_by"))
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("weight adjustment candidates production_effect 不是 none。")
    if _string_value(payload.get("mode")) != "observe_only":
        risks.append("weight adjustment candidates mode 不是 observe_only。")
    if blocked_by:
        risks.append(f"candidate_gate 限制：{', '.join(blocked_by)}。")
    return {
        "status": gate_status or "LIMITED",
        "gate_status": gate_status or "LIMITED",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "candidate_count": summary.get("candidate_count", payload.get("candidate_count", 0)),
        "top_candidate_id": summary.get("top_candidate_id", payload.get("top_candidate_id", "")),
        "main_blocked_by": summary.get("main_blocked_by")
        or (blocked_by[0] if blocked_by else "none"),
        "blocked_by": blocked_by,
        "production_effect": production_effect,
        "mode": _string_value(payload.get("mode")) or "observe_only",
        "risk": "；".join(risks) or "Weight adjustment candidates 当前仅作只读展示。",
    }


def _weight_candidate_evaluation_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    path = report.reports_dir / f"weight_candidate_evaluation_{suffix}.json"
    payload = _read_json_object(path)
    if payload.get("report_type") != "weight_candidate_evaluation":
        return {
            "status": "MISSING",
            "evaluation_status": "INSUFFICIENT_DATA",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "candidate_count": 0,
            "evaluable_candidate_count": 0,
            "top_candidate_id": "",
            "main_blocked_by": "missing",
            "blocked_by": ["missing"],
            "production_effect": ProductionEffect.NONE.value,
            "evaluation_mode": "observe_only",
            "risk": "weight candidate evaluation JSON 缺失；dashboard 不生成评估或调参。",
        }
    summary = _mapping_value(payload, "summary")
    outputs = _mapping_value(payload, "outputs")
    selected_window = _mapping_value(
        _mapping_value(payload, "windows"),
        str(payload.get("selected_window_days", 30)),
    )
    report_path = Path(
        _string_value(outputs.get("markdown")) or str(path.with_suffix(".md")),
    )
    report_href = _report_href(report_path, report.reports_dir) if report_path.exists() else ""
    production_effect = _string_value(payload.get("production_effect")) or "none"
    evaluation_mode = _string_value(payload.get("evaluation_mode")) or "observe_only"
    evaluation_status = (
        _string_value(payload.get("evaluation_status"))
        or _string_value(summary.get("evaluation_status"))
        or "INSUFFICIENT_DATA"
    )
    blocked_by = _strings(selected_window.get("blocked_by"))
    main_blocked_by = (
        _string_value(summary.get("main_blocked_by"))
        or _string_value(selected_window.get("main_blocked_by"))
        or (blocked_by[0] if blocked_by else "none")
    )
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("weight candidate evaluation production_effect 不是 none。")
    if evaluation_mode != "observe_only":
        risks.append("weight candidate evaluation evaluation_mode 不是 observe_only。")
    if blocked_by:
        risks.append(f"evaluation gate 限制：{', '.join(blocked_by)}。")
    return {
        "status": evaluation_status,
        "evaluation_status": evaluation_status,
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "candidate_count": summary.get("candidate_count", payload.get("candidate_count", 0)),
        "evaluable_candidate_count": summary.get("evaluable_candidate_count", 0),
        "top_candidate_id": summary.get("top_candidate_id", ""),
        "main_blocked_by": main_blocked_by,
        "blocked_by": blocked_by,
        "production_effect": production_effect,
        "evaluation_mode": evaluation_mode,
        "risk": "；".join(risks) or "Weight candidate evaluation 当前仅作只读展示。",
    }


def _weight_promotion_gate_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    path = report.reports_dir / f"weight_promotion_gate_{suffix}.json"
    payload = _read_json_object(path)
    if payload.get("report_type") != "weight_promotion_gate":
        return {
            "status": "MISSING",
            "gate_status": "INSUFFICIENT_DATA",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "candidate_count": 0,
            "ready_for_manual_review_count": 0,
            "blocked_count": 0,
            "top_candidate_id": "",
            "main_blocked_by": "missing",
            "blocked_by": ["missing"],
            "production_effect": ProductionEffect.NONE.value,
            "gate_mode": "manual_review_only",
            "risk": "weight promotion gate JSON 缺失；dashboard 不运行 gate 或调参。",
        }
    summary = _mapping_value(payload, "summary")
    outputs = _mapping_value(payload, "outputs")
    report_path = Path(
        _string_value(outputs.get("markdown")) or str(path.with_suffix(".md")),
    )
    report_href = _report_href(report_path, report.reports_dir) if report_path.exists() else ""
    production_effect = _string_value(payload.get("production_effect")) or "none"
    gate_mode = _string_value(payload.get("gate_mode")) or "manual_review_only"
    gate_status = (
        _string_value(summary.get("gate_status"))
        or _string_value(payload.get("promotion_gate_status"))
        or "INSUFFICIENT_DATA"
    )
    blocked_by = _weight_promotion_gate_blockers(payload)
    main_blocked_by = _string_value(summary.get("main_blocked_by")) or (
        blocked_by[0] if blocked_by else "none"
    )
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("weight promotion gate production_effect 不是 none。")
    if gate_mode != "manual_review_only":
        risks.append("weight promotion gate gate_mode 不是 manual_review_only。")
    if blocked_by:
        risks.append(f"promotion gate 限制：{', '.join(blocked_by)}。")
    return {
        "status": gate_status,
        "gate_status": gate_status,
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "candidate_count": summary.get("candidate_count", payload.get("candidate_count", 0)),
        "ready_for_manual_review_count": summary.get("ready_for_manual_review_count", 0),
        "blocked_count": summary.get("blocked_count", 0),
        "top_candidate_id": summary.get("top_candidate_id", ""),
        "main_blocked_by": main_blocked_by,
        "blocked_by": blocked_by,
        "production_effect": production_effect,
        "gate_mode": gate_mode,
        "risk": "；".join(risks) or "Weight promotion gate 当前仅作只读展示。",
    }


def _weight_promotion_gate_blockers(payload: TraceRecord) -> list[str]:
    summary = _mapping_value(payload, "summary")
    summary_blockers = _strings(summary.get("blocked_by"))
    if summary_blockers:
        return summary_blockers
    counter: Counter[str] = Counter()
    for candidate in _records(payload.get("candidates")):
        counter.update(_strings(candidate.get("blocked_by")))
    return [value for value, _count in counter.most_common()]


def _daily_weight_adjustment_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    path = report.reports_dir / f"daily_weight_adjustment_summary_{suffix}.json"
    payload = _read_json_object(path)
    if payload.get("report_type") != "daily_weight_adjustment_summary":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "candidate_count": 0,
            "evaluation_status": "INSUFFICIENT_DATA",
            "promotion_gate_status": "INSUFFICIENT_DATA",
            "ready_for_manual_review_count": 0,
            "main_blocked_by": "missing",
            "warnings": ["missing"],
            "production_effect": ProductionEffect.NONE.value,
            "mode": "observe_only",
            "manual_review_only": True,
            "risk": (
                "daily weight adjustment summary JSON 缺失；dashboard 不运行 "
                "weight adjustment pipeline。"
            ),
        }
    outputs = _mapping_value(payload, "outputs")
    report_path = Path(
        _string_value(outputs.get("markdown")) or str(path.with_suffix(".md")),
    )
    report_href = _report_href(report_path, report.reports_dir) if report_path.exists() else ""
    production_effect = _string_value(payload.get("production_effect")) or "none"
    mode = _string_value(payload.get("mode")) or "observe_only"
    warnings = _strings(payload.get("warnings"))
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("daily weight adjustment summary production_effect 不是 none。")
    if mode != "observe_only":
        risks.append("daily weight adjustment summary mode 不是 observe_only。")
    if payload.get("manual_review_only") is not True:
        risks.append("daily weight adjustment summary 不是 manual_review_only。")
    if warnings:
        risks.append(f"summary warnings：{', '.join(warnings)}。")
    return {
        "status": _string_value(payload.get("status")) or "LIMITED",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "candidate_count": payload.get("candidate_count", 0),
        "evaluation_status": payload.get("evaluation_status", "INSUFFICIENT_DATA"),
        "promotion_gate_status": payload.get("promotion_gate_status", "INSUFFICIENT_DATA"),
        "ready_for_manual_review_count": payload.get("ready_for_manual_review_count", 0),
        "main_blocked_by": payload.get("main_blocked_by", "missing"),
        "warnings": warnings,
        "production_effect": production_effect,
        "mode": mode,
        "manual_review_only": payload.get("manual_review_only") is True,
        "risk": "；".join(risks) or "Daily weight adjustment summary 当前仅作只读展示。",
    }


def _shadow_weight_iteration_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    shadow_root = report.project_root / "data" / "derived" / "weight_iterations" / "shadow"
    current_path = shadow_root / "current_shadow_weights.json"
    candidate_path = shadow_root / "candidates" / f"shadow_weight_candidate_{suffix}.json"
    current = _read_json_object(current_path)
    candidate = _read_json_object(candidate_path)
    current_exists = current.get("report_type") == "current_shadow_weights"
    candidate_exists = candidate.get("report_type") == "daily_shadow_weight_iteration"
    if not current_exists and not candidate_exists:
        return {
            "status": "MISSING",
            "exists": False,
            "current_path": str(current_path),
            "candidate_path": str(candidate_path),
            "candidate_report_href": "",
            "decision": "MISSING",
            "last_updated_date": "missing",
            "current_weights": {},
            "latest_delta": {},
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "risk": "shadow weight iteration state 缺失；dashboard 不运行 shadow learn pipeline。",
        }
    outputs = _mapping_value(candidate, "outputs")
    candidate_md = Path(
        _string_value(outputs.get("candidate_markdown")) or str(candidate_path.with_suffix(".md"))
    )
    candidate_report_href = (
        _report_href(candidate_md, report.reports_dir)
        if candidate_md.exists()
        else _report_href(candidate_path, report.reports_dir)
    )
    current_weights = _mapping_value(current, "weights")
    latest_delta = _mapping_value(candidate, "proposed_delta")
    audit = _mapping_value(current, "audit")
    production_effect = (
        _string_value(candidate.get("production_effect"))
        or _string_value(current.get("production_effect"))
        or ProductionEffect.NONE.value
    )
    manual_review_only = (
        candidate.get("manual_review_only") is True
        if candidate_exists
        else current.get("manual_review_only") is True
    )
    decision = _string_value(candidate.get("decision")) or _string_value(audit.get("last_decision"))
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("shadow weight iteration production_effect 不是 none。")
    if not manual_review_only:
        risks.append("shadow weight iteration 不是 manual_review_only。")
    if candidate_exists and decision in {"INSUFFICIENT_DATA", "SAFETY_BLOCKED", "ERROR"}:
        risks.append(f"最近 candidate decision 为 {decision}。")
    return {
        "status": decision or "MISSING",
        "exists": current_exists or candidate_exists,
        "current_path": str(current_path),
        "candidate_path": str(candidate_path),
        "candidate_report_href": candidate_report_href if candidate_exists else "",
        "decision": decision or "MISSING",
        "last_updated_date": _string_value(current.get("last_updated_date")) or "missing",
        "current_weights": current_weights,
        "latest_delta": latest_delta,
        "production_effect": production_effect,
        "manual_review_only": manual_review_only,
        "risk": "；".join(risks) or "Shadow Weight Iteration 当前仅作只读展示。",
    }


def _shadow_vs_production_comparison_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    suffix = report.as_of.isoformat()
    path = (
        report.project_root
        / "data"
        / "derived"
        / "weight_iterations"
        / "comparison"
        / f"daily_shadow_vs_production_{suffix}.json"
    )
    payload = _read_json_object(path)
    if payload.get("report_type") != "daily_shadow_vs_production_comparison":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "production_decision": "MISSING",
            "shadow_decision": "MISSING",
            "score_delta": "NA",
            "decision_changed": "MISSING",
            "main_reason": "missing",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "risk": "shadow vs production comparison JSON 缺失；dashboard 不运行比较。",
        }
    outputs = _mapping_value(payload, "outputs")
    markdown_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    production = _mapping_value(payload, "production")
    shadow = _mapping_value(payload, "shadow")
    difference = _mapping_value(payload, "difference")
    validation = _mapping_value(payload, "input_validation")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    manual_review_only = payload.get("manual_review_only") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("shadow vs production comparison production_effect 不是 none。")
    if not manual_review_only:
        risks.append("shadow vs production comparison 不是 manual_review_only。")
    blockers = _strings(validation.get("blocking_reasons"))
    if blockers:
        risks.append(f"comparison 输入限制：{', '.join(blockers)}。")
    contract = _mapping_value(payload, "pipeline_contract")
    for field in (
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_approved_profile",
        "promotes_shadow_to_production",
    ):
        if contract.get(field) is not False:
            risks.append(f"comparison safety contract 异常：{field}。")
    score_delta = _optional_float(difference.get("score_delta"))
    return {
        "status": _string_value(payload.get("comparison_status")) or "INSUFFICIENT_DATA",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "production_decision": _string_value(production.get("decision")) or "MISSING",
        "shadow_decision": _string_value(shadow.get("decision")) or "MISSING",
        "production_score": production.get("score"),
        "shadow_score": shadow.get("score"),
        "score_delta": _format_signed_decimal_delta(0.0, score_delta, digits=2),
        "normalized_score_delta": difference.get("normalized_score_delta"),
        "decision_changed": difference.get("decision_changed"),
        "score_band_changed": difference.get("score_band_changed"),
        "main_reason": _string_value(difference.get("main_reason")) or "missing",
        "production_effect": production_effect,
        "manual_review_only": manual_review_only,
        "risk": "；".join(risks) or "Shadow vs Production Comparison 当前仅作只读展示。",
    }


def _shadow_vs_production_multi_day_review_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_shadow_vs_production_review_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "shadow_vs_production_multi_day_review":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_review_markdown_path": "",
            "review_decision": "MISSING",
            "lookback_days": 0,
            "available_comparison_days": 0,
            "average_score_delta": "NA",
            "decision_difference_count": 0,
            "promotion_ready": False,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "risk": (
                "multi-day shadow vs production review JSON 缺失；"
                "dashboard 不运行 review pipeline。"
            ),
        }

    outputs = _mapping_value(payload, "outputs")
    markdown_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    manual_review_only = payload.get("manual_review_only") is True
    promotion = _mapping_value(payload, "promotion_readiness")
    promotion_ready = promotion.get("ready") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("multi-day review production_effect 不是 none。")
    if not manual_review_only:
        risks.append("multi-day review 不是 manual_review_only。")
    if promotion_ready:
        risks.append("TRADING-018C2 不允许 promotion_readiness.ready=true。")
    contract = _mapping_value(payload, "pipeline_contract")
    for field in (
        "runs_scoring_pipeline",
        "runs_comparison_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_approved_profile",
        "promotes_shadow_to_production",
    ):
        if contract.get(field) is not False:
            risks.append(f"multi-day review safety contract 异常：{field}。")
    score_delta = _optional_float(payload.get("average_score_delta"))
    return {
        "status": _string_value(payload.get("review_decision")) or "INSUFFICIENT_HISTORY",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_review_markdown_path": str(markdown_path),
        "review_decision": _string_value(payload.get("review_decision")) or "INSUFFICIENT_HISTORY",
        "lookback_days": payload.get("lookback_days", 0),
        "available_comparison_days": payload.get("available_comparison_days", 0),
        "average_score_delta": _format_signed_decimal_delta(0.0, score_delta, digits=2),
        "decision_difference_count": payload.get("decision_difference_count", 0),
        "promotion_ready": promotion_ready,
        "production_effect": production_effect,
        "manual_review_only": manual_review_only,
        "risk": "；".join(risks) or "Shadow vs Production Multi-day Review 当前仅作只读展示。",
    }


def _shadow_promotion_proposal_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_shadow_promotion_proposal_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "shadow_promotion_proposal":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_proposal_markdown_path": "",
            "proposal_decision": "MISSING",
            "promotion_proposed": False,
            "promotion_executed": False,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "safe_for_production": False,
            "average_score_delta": "NA",
            "risk_flag_delta_total": 0,
            "available_comparison_days": 0,
            "risk": ("shadow promotion proposal JSON 缺失；dashboard 不运行 proposal pipeline。"),
        }

    outputs = _mapping_value(payload, "outputs")
    markdown_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    impact = _mapping_value(payload, "impact_summary")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    manual_review_only = payload.get("manual_review_only") is True
    promotion_executed = payload.get("promotion_executed") is True
    safe_for_production = payload.get("safe_for_production") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("shadow promotion proposal production_effect 不是 none。")
    if not manual_review_only:
        risks.append("shadow promotion proposal 不是 manual_review_only。")
    if promotion_executed:
        risks.append("TRADING-018D 不允许 promotion_executed=true。")
    if safe_for_production:
        risks.append("TRADING-018D 不允许 safe_for_production=true。")
    contract = _mapping_value(payload, "pipeline_contract")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_apply",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
    ):
        if contract.get(field) is not False:
            risks.append(f"shadow promotion proposal safety contract 异常：{field}。")
    score_delta = _optional_float(impact.get("expected_score_delta"))
    return {
        "status": _string_value(payload.get("proposal_decision")) or "INSUFFICIENT_DATA",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_proposal_markdown_path": str(markdown_path),
        "proposal_decision": _string_value(payload.get("proposal_decision")) or "INSUFFICIENT_DATA",
        "promotion_proposed": payload.get("promotion_proposed") is True,
        "promotion_executed": promotion_executed,
        "production_effect": production_effect,
        "manual_review_only": manual_review_only,
        "safe_for_production": safe_for_production,
        "average_score_delta": _format_signed_decimal_delta(0.0, score_delta, digits=2),
        "risk_flag_delta_total": impact.get("risk_flag_delta_total", 0),
        "available_comparison_days": impact.get("available_comparison_days", 0),
        "risk": "；".join(risks) or "Shadow Promotion Proposal 当前仅作只读展示。",
    }


def _shadow_promotion_apply_preflight_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_shadow_promotion_apply_preflight_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "shadow_promotion_apply_preflight":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_preflight_markdown_path": "",
            "preflight_decision": "MISSING",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "promotion_executed": False,
            "apply_executed": False,
            "preflight_only": True,
            "safe_for_production": False,
            "changed_weight_keys": [],
            "proposal_path": "",
            "approval_path": "",
            "risk": (
                "shadow promotion apply preflight JSON 缺失；"
                "dashboard 不运行 preflight pipeline。"
            ),
        }

    outputs = _mapping_value(payload, "outputs")
    markdown_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    diff = _mapping_value(payload, "diff_preview")
    artifacts = _mapping_value(payload, "input_artifacts")
    proposal = _mapping_value(artifacts, "promotion_proposal")
    approval = _mapping_value(artifacts, "approval_artifact")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    manual_review_only = payload.get("manual_review_only") is True
    promotion_executed = payload.get("promotion_executed") is True
    apply_executed = payload.get("apply_executed") is True
    preflight_only = payload.get("preflight_only") is True
    safe_for_production = payload.get("safe_for_production") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("apply preflight production_effect 不是 none。")
    if not manual_review_only:
        risks.append("apply preflight 不是 manual_review_only。")
    if promotion_executed:
        risks.append("TRADING-018E1 不允许 promotion_executed=true。")
    if apply_executed:
        risks.append("TRADING-018E1 不允许 apply_executed=true。")
    if not preflight_only:
        risks.append("TRADING-018E1 必须 preflight_only=true。")
    if safe_for_production:
        risks.append("TRADING-018E1 不允许 safe_for_production=true。")
    contract = _mapping_value(payload, "pipeline_contract")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_promotion_apply",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
    ):
        if contract.get(field) is not False:
            risks.append(f"apply preflight safety contract 异常：{field}。")
    changed_weight_keys = list(_strings(diff.get("changed_weight_keys")))
    return {
        "status": _string_value(payload.get("preflight_decision")) or "INSUFFICIENT_DATA",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_preflight_markdown_path": str(markdown_path),
        "preflight_decision": _string_value(payload.get("preflight_decision"))
        or "INSUFFICIENT_DATA",
        "production_effect": production_effect,
        "manual_review_only": manual_review_only,
        "promotion_executed": promotion_executed,
        "apply_executed": apply_executed,
        "preflight_only": preflight_only,
        "safe_for_production": safe_for_production,
        "changed_weight_keys": changed_weight_keys,
        "proposal_path": _string_value(proposal.get("path")),
        "approval_path": _string_value(approval.get("path")),
        "risk": "；".join(risks) or "Shadow Promotion Apply Preflight 当前仅作只读展示。",
    }


def _shadow_promotion_apply_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_shadow_promotion_apply_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "shadow_promotion_apply_result":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_apply_markdown_path": "",
            "apply_decision": "MISSING",
            "apply_executed": False,
            "promotion_executed": False,
            "production_effect": ProductionEffect.NONE.value,
            "target_profile_path": "",
            "changed_weight_keys": [],
            "rollback_snapshot_path": "",
            "post_apply_validation_status": "MISSING",
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": "shadow promotion apply result JSON 缺失；dashboard 不运行 apply pipeline。",
        }

    outputs = _mapping_value(payload, "outputs")
    markdown_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    target = _mapping_value(payload, "target_profile_validation")
    diff = _mapping_value(payload, "diff_applied")
    rollback = _mapping_value(payload, "rollback")
    post_apply = _mapping_value(payload, "post_apply_validation")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    apply_executed = payload.get("apply_executed") is True
    promotion_executed = payload.get("promotion_executed") is True
    safe_for_scheduler = payload.get("safe_for_scheduler") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if safe_for_scheduler:
        risks.append("TRADING-018E2 apply result 不允许 safe_for_scheduler=true。")
    if broker_execution:
        risks.append("TRADING-018E2 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-018E2 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-018E2 不允许 trading_execution=true。")
    if apply_executed and production_effect != "profile_updated_only_if_apply_executed":
        risks.append("已执行 apply 但 production_effect 未记录 profile update。")
    if not apply_executed and production_effect != ProductionEffect.NONE.value:
        risks.append("未执行 apply 时 production_effect 必须为 none。")
    contract = _mapping_value(payload, "pipeline_contract")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_approved_profile",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is not False:
            risks.append(f"apply result safety contract 异常：{field}。")
    changed_weight_keys = list(_strings(diff.get("changed_weight_keys")))
    return {
        "status": _string_value(payload.get("apply_decision")) or "INSUFFICIENT_DATA",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_apply_markdown_path": str(markdown_path),
        "apply_decision": _string_value(payload.get("apply_decision")) or "INSUFFICIENT_DATA",
        "apply_executed": apply_executed,
        "promotion_executed": promotion_executed,
        "production_effect": production_effect,
        "target_profile_path": _string_value(target.get("path")),
        "changed_weight_keys": changed_weight_keys,
        "rollback_snapshot_path": _string_value(rollback.get("snapshot_path")),
        "post_apply_validation_status": _string_value(post_apply.get("status")) or "MISSING",
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Shadow Promotion Apply Result 当前仅作只读展示。",
    }


def _shadow_promotion_rollback_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_shadow_promotion_rollback_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "shadow_promotion_rollback_result":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_rollback_markdown_path": "",
            "rollback_decision": "MISSING",
            "rollback_executed": False,
            "production_effect": ProductionEffect.NONE.value,
            "target_profile_path": "",
            "changed_weight_keys": [],
            "current_snapshot_path": "",
            "rollback_snapshot_path": "",
            "post_rollback_validation_status": "MISSING",
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "shadow promotion rollback result JSON 缺失；dashboard 不运行 "
                "rollback pipeline。"
            ),
        }

    outputs = _mapping_value(payload, "outputs")
    markdown_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    target = _mapping_value(payload, "target_profile_validation")
    rollback_applied = _mapping_value(payload, "rollback_applied")
    current_snapshot = _mapping_value(payload, "current_snapshot")
    input_artifacts = _mapping_value(payload, "input_artifacts")
    raw_rollback_snapshot = input_artifacts.get("rollback_snapshot")
    rollback_snapshot = raw_rollback_snapshot if isinstance(raw_rollback_snapshot, dict) else {}
    post_rollback = _mapping_value(payload, "post_rollback_validation")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    rollback_executed = payload.get("rollback_executed") is True
    safe_for_scheduler = payload.get("safe_for_scheduler") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if safe_for_scheduler:
        risks.append("TRADING-018E3 rollback result 不允许 safe_for_scheduler=true。")
    if broker_execution:
        risks.append("TRADING-018E3 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-018E3 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-018E3 不允许 trading_execution=true。")
    if rollback_executed and production_effect != "profile_rolled_back_only_if_rollback_executed":
        risks.append("已执行 rollback 但 production_effect 未记录 profile rollback。")
    if not rollback_executed and production_effect != ProductionEffect.NONE.value:
        risks.append("未执行 rollback 时 production_effect 必须为 none。")
    contract = _mapping_value(payload, "pipeline_contract")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_promotion_apply",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is not False:
            risks.append(f"rollback result safety contract 异常：{field}。")
    changed_weight_keys = list(_strings(rollback_applied.get("changed_weight_keys")))
    return {
        "status": _string_value(payload.get("rollback_decision")) or "INSUFFICIENT_DATA",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_rollback_markdown_path": str(markdown_path),
        "rollback_decision": _string_value(payload.get("rollback_decision")) or "INSUFFICIENT_DATA",
        "rollback_executed": rollback_executed,
        "production_effect": production_effect,
        "target_profile_path": _string_value(target.get("path")),
        "changed_weight_keys": changed_weight_keys,
        "current_snapshot_path": _string_value(current_snapshot.get("snapshot_path")),
        "rollback_snapshot_path": _string_value(rollback_snapshot.get("path")),
        "post_rollback_validation_status": _string_value(post_rollback.get("status")) or "MISSING",
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Shadow Promotion Rollback Result 当前仅作只读展示。",
    }


def _shadow_promotion_lifecycle_audit_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_shadow_promotion_lifecycle_audit_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "shadow_promotion_lifecycle_audit":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_audit_markdown_path": "",
            "lifecycle_decision": "MISSING",
            "promotion_date": "",
            "proposal_status": "MISSING",
            "preflight_status": "MISSING",
            "apply_status": "MISSING",
            "rollback_status": "NOT_FOUND",
            "safety_boundary_status": "MISSING",
            "critical_findings_count": 0,
            "warnings_count": 0,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "production_effect": ProductionEffect.NONE.value,
            "audit_only": True,
            "risk": (
                "shadow promotion lifecycle audit JSON 缺失；dashboard 不运行 "
                "018F audit pipeline 或任何上游 promotion pipeline。"
            ),
        }

    outputs = _mapping_value(payload, "outputs")
    markdown_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    artifacts = _mapping_value(payload, "input_artifacts")
    safety = _mapping_value(payload, "safety_boundary_audit")
    findings = _mapping_value(payload, "audit_findings")
    contract = _mapping_value(payload, "pipeline_contract")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("lifecycle audit production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("lifecycle audit 必须 manual_review_only=true。")
    if payload.get("audit_only") is not True:
        risks.append("lifecycle audit 必须 audit_only=true。")
    if payload.get("apply_executed_by_audit") is not False:
        risks.append("018F audit 不允许执行 apply。")
    if payload.get("rollback_executed_by_audit") is not False:
        risks.append("018F audit 不允许执行 rollback。")
    if broker_execution:
        risks.append("018F audit 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("018F audit 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("018F audit 不允许 trading_execution=true。")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_promotion_apply",
        "runs_promotion_rollback",
        "runs_lifecycle_audit_pipeline",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is True:
            risks.append(f"lifecycle audit safety contract 异常：{field}。")
    critical = list(_strings(findings.get("critical_findings")))
    warnings = list(_strings(findings.get("warnings")))
    return {
        "status": _string_value(payload.get("lifecycle_decision")) or "INCOMPLETE_ARTIFACTS",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_audit_markdown_path": str(markdown_path),
        "lifecycle_decision": _string_value(payload.get("lifecycle_decision"))
        or "INCOMPLETE_ARTIFACTS",
        "promotion_date": _string_value(payload.get("promotion_date")),
        "proposal_status": _string_value(_mapping_value(artifacts, "proposal").get("status")),
        "preflight_status": _string_value(_mapping_value(artifacts, "preflight").get("status")),
        "apply_status": _string_value(_mapping_value(artifacts, "apply_result").get("status")),
        "rollback_status": _string_value(
            _mapping_value(artifacts, "rollback_result").get("status")
        ),
        "safety_boundary_status": _string_value(safety.get("status")) or "MISSING",
        "critical_findings_count": len(critical),
        "warnings_count": len(warnings),
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "production_effect": production_effect,
        "audit_only": payload.get("audit_only") is True,
        "risk": "；".join(risks) or "Shadow Promotion Lifecycle Audit 当前仅作只读展示。",
    }


def _parameter_governance_summary_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_parameter_governance_summary_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "parameter_governance_summary":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "governance_state": "MISSING",
            "action_required": False,
            "action_level": "NONE",
            "recommended_action": "",
            "production_weights_summary": "MISSING",
            "shadow_weights_summary": "MISSING",
            "review_decision": "MISSING",
            "proposal_decision": "MISSING",
            "preflight_decision": "MISSING",
            "apply_decision": "MISSING",
            "rollback_decision": "MISSING",
            "lifecycle_decision": "MISSING",
            "safety_boundary_status": "MISSING",
            "critical_findings_count": 0,
            "warnings_count": 0,
            "latest_summary_markdown_path": "",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "governance_only": True,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "parameter governance summary JSON 缺失；dashboard 不运行 TRADING-019 "
                "或任何 018B-018F / scoring / broker / replay / trading pipeline。"
            ),
        }

    outputs = _mapping_value(payload, "outputs")
    markdown_path = Path(_string_value(outputs.get("markdown")) or str(path.with_suffix(".md")))
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    production = _mapping_value(payload, "production_state")
    shadow = _mapping_value(payload, "shadow_state")
    review = _mapping_value(payload, "shadow_vs_production_review")
    promotion = _mapping_value(payload, "promotion_status")
    safety = _mapping_value(payload, "safety_boundary_audit")
    findings = _mapping_value(payload, "audit_findings")
    contract = _mapping_value(payload, "pipeline_contract")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    critical = list(_strings(findings.get("critical_findings")))
    warnings = list(_strings(findings.get("warnings")))
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-019 summary production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-019 summary 必须 manual_review_only=true。")
    if payload.get("governance_only") is not True:
        risks.append("TRADING-019 summary 必须 governance_only=true。")
    if payload.get("apply_executed_by_governance") is not False:
        risks.append("TRADING-019 summary 不允许执行 apply。")
    if payload.get("rollback_executed_by_governance") is not False:
        risks.append("TRADING-019 summary 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-019 summary 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-019 summary 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-019 summary 不允许 trading_execution=true。")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_promotion_apply",
        "runs_promotion_rollback",
        "runs_lifecycle_audit_pipeline",
        "runs_governance_summary_pipeline",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_shadow_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is True:
            risks.append(f"parameter governance summary safety contract 异常：{field}。")

    return {
        "status": _string_value(payload.get("governance_state")) or "INCOMPLETE_DATA",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "governance_state": _string_value(payload.get("governance_state")) or "INCOMPLETE_DATA",
        "action_required": payload.get("action_required") is True,
        "action_level": _string_value(payload.get("action_level")) or "REVIEW_REQUIRED",
        "recommended_action": _string_value(payload.get("recommended_action")),
        "production_weights_summary": _weight_summary(_mapping_value(production, "weights")),
        "shadow_weights_summary": _weight_summary(_mapping_value(shadow, "weights")),
        "review_decision": _string_value(review.get("review_decision")) or "MISSING",
        "proposal_decision": _string_value(promotion.get("proposal_decision")) or "MISSING",
        "preflight_decision": _string_value(promotion.get("preflight_decision")) or "MISSING",
        "apply_decision": _string_value(promotion.get("apply_decision")) or "MISSING",
        "rollback_decision": _string_value(promotion.get("rollback_decision")) or "MISSING",
        "lifecycle_decision": _string_value(promotion.get("lifecycle_decision")) or "MISSING",
        "safety_boundary_status": _string_value(safety.get("status")) or "MISSING",
        "critical_findings_count": len(critical),
        "warnings_count": len(warnings),
        "latest_summary_markdown_path": str(markdown_path),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "governance_only": payload.get("governance_only") is True,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Parameter Governance Summary 当前仅作只读展示。",
    }


def _parameter_governance_web_view_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_parameter_governance_web_view_metadata_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "parameter_governance_web_view":
        default_html = (
            report.project_root
            / "data"
            / "derived"
            / "weight_iterations"
            / "governance"
            / "web"
            / f"parameter_governance_web_view_{report.as_of.isoformat()}.html"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_web_view_html_path": str(default_html),
            "latest_render_metadata_path": str(path),
            "render_decision": "MISSING",
            "governance_state": "MISSING",
            "action_required": False,
            "action_level": "NONE",
            "safety_boundary_status": "MISSING",
            "critical_findings_count": 0,
            "warnings_count": 0,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "governance_only": True,
            "web_view_only": True,
            "apply_executed_by_web_view": False,
            "rollback_executed_by_web_view": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "parameter governance web view metadata 缺失；dashboard 不运行 "
                "TRADING-020 render script、TRADING-019 或任何 018B-018F / scoring / "
                "broker / replay / trading pipeline。"
            ),
        }

    output_artifacts = _mapping_value(payload, "output_artifacts")
    html_artifact = _mapping_value(output_artifacts, "html")
    html_path = Path(_string_value(html_artifact.get("path")) or str(path.with_suffix(".html")))
    report_href = _report_href(html_path, report.reports_dir) if html_path.exists() else ""
    render_summary = _mapping_value(payload, "render_summary")
    contract = _mapping_value(payload, "pipeline_contract")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-020 metadata production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-020 metadata 必须 manual_review_only=true。")
    if payload.get("governance_only") is not True:
        risks.append("TRADING-020 metadata 必须 governance_only=true。")
    if payload.get("web_view_only") is not True:
        risks.append("TRADING-020 metadata 必须 web_view_only=true。")
    if payload.get("apply_executed_by_web_view") is not False:
        risks.append("TRADING-020 web view 不允许执行 apply。")
    if payload.get("rollback_executed_by_web_view") is not False:
        risks.append("TRADING-020 web view 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-020 web view 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-020 web view 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-020 web view 不允许 trading_execution=true。")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_promotion_apply",
        "runs_promotion_rollback",
        "runs_lifecycle_audit_pipeline",
        "runs_governance_summary_pipeline",
        "runs_web_view_render_script",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_shadow_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is True:
            risks.append(f"parameter governance web view safety contract 异常：{field}。")

    return {
        "status": _string_value(payload.get("render_decision")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_web_view_html_path": str(html_path),
        "latest_render_metadata_path": str(path),
        "render_decision": _string_value(payload.get("render_decision")) or "MISSING",
        "governance_state": _string_value(render_summary.get("governance_state")) or "MISSING",
        "action_required": render_summary.get("action_required") is True,
        "action_level": _string_value(render_summary.get("action_level")) or "NONE",
        "safety_boundary_status": (
            _string_value(render_summary.get("safety_boundary_status")) or "MISSING"
        ),
        "critical_findings_count": _optional_int(render_summary.get("critical_findings_count"))
        or 0,
        "warnings_count": _optional_int(render_summary.get("warnings_count")) or 0,
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "governance_only": payload.get("governance_only") is True,
        "web_view_only": payload.get("web_view_only") is True,
        "apply_executed_by_web_view": payload.get("apply_executed_by_web_view") is True,
        "rollback_executed_by_web_view": payload.get("rollback_executed_by_web_view") is True,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Parameter Governance Web View 当前仅作只读展示。",
    }


def _parameter_governance_daily_digest_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_parameter_governance_daily_digest_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "parameter_governance_daily_digest":
        default_markdown = (
            report.project_root
            / "data"
            / "derived"
            / "weight_iterations"
            / "governance"
            / "digests"
            / f"parameter_governance_daily_digest_{report.as_of.isoformat()}.md"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_digest_markdown_path": str(default_markdown),
            "digest_status": "MISSING",
            "summary_level": "UNKNOWN",
            "headline": "",
            "governance_state": "MISSING",
            "action_required": False,
            "action_level": "NONE",
            "safety_boundary_status": "MISSING",
            "pending_apply": False,
            "pending_rollback": False,
            "critical_alert_count": 0,
            "warning_count": 0,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "digest_only": True,
            "governance_only": True,
            "apply_executed_by_digest": False,
            "rollback_executed_by_digest": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "parameter governance daily digest JSON 缺失；dashboard 不运行 "
                "TRADING-021 script、TRADING-020、TRADING-019 或任何 018B-018F / scoring / "
                "broker / replay / trading pipeline。"
            ),
        }

    output_artifacts = _mapping_value(payload, "output_artifacts")
    markdown_path = Path(
        _string_value(_mapping_value(output_artifacts, "markdown").get("path"))
        or str(path.with_suffix(".md"))
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    snapshot = _mapping_value(payload, "governance_snapshot")
    pending = _mapping_value(payload, "pending_items")
    alerts = _mapping_value(payload, "alerts")
    contract = _mapping_value(payload, "pipeline_contract")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    critical = _strings(alerts.get("critical"))
    warnings = _strings(alerts.get("warnings"))
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-021 digest production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-021 digest 必须 manual_review_only=true。")
    if payload.get("digest_only") is not True:
        risks.append("TRADING-021 digest 必须 digest_only=true。")
    if payload.get("governance_only") is not True:
        risks.append("TRADING-021 digest 必须 governance_only=true。")
    if payload.get("apply_executed_by_digest") is not False:
        risks.append("TRADING-021 digest 不允许执行 apply。")
    if payload.get("rollback_executed_by_digest") is not False:
        risks.append("TRADING-021 digest 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-021 digest 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-021 digest 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-021 digest 不允许 trading_execution=true。")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_promotion_apply",
        "runs_promotion_rollback",
        "runs_lifecycle_audit_pipeline",
        "runs_governance_summary_pipeline",
        "runs_web_view_render_script",
        "runs_daily_digest_script",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_shadow_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "changes_daily_dashboard_main_conclusion",
        "triggers_trade",
    ):
        if contract.get(field) is True:
            risks.append(f"parameter governance daily digest safety contract 异常：{field}。")

    return {
        "status": _string_value(payload.get("digest_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_digest_markdown_path": str(markdown_path),
        "digest_status": _string_value(payload.get("digest_status")) or "MISSING",
        "summary_level": _string_value(payload.get("summary_level")) or "UNKNOWN",
        "headline": _string_value(payload.get("headline")),
        "governance_state": _string_value(snapshot.get("governance_state")) or "MISSING",
        "action_required": snapshot.get("action_required") is True,
        "action_level": _string_value(snapshot.get("action_level")) or "NONE",
        "safety_boundary_status": (
            _string_value(snapshot.get("safety_boundary_status")) or "MISSING"
        ),
        "pending_apply": pending.get("pending_apply") is True,
        "pending_rollback": pending.get("pending_rollback") is True,
        "critical_alert_count": len(critical),
        "warning_count": len(warnings),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "digest_only": payload.get("digest_only") is True,
        "governance_only": payload.get("governance_only") is True,
        "apply_executed_by_digest": payload.get("apply_executed_by_digest") is True,
        "rollback_executed_by_digest": payload.get("rollback_executed_by_digest") is True,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Parameter Governance Daily Digest 当前仅作只读展示。",
    }


def _pipeline_health_summary_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_pipeline_health_summary_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "pipeline_health_summary":
        default_markdown = (
            report.project_root
            / "data"
            / "derived"
            / "pipeline_health"
            / f"pipeline_health_summary_{report.as_of.isoformat()}.md"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_summary_markdown_path": str(default_markdown),
            "health_status": "MISSING",
            "summary_level": "UNKNOWN",
            "headline": "",
            "required_pipelines": 0,
            "missing_required_pipelines": 0,
            "stale_required_pipelines": 0,
            "critical_pipelines": 0,
            "warning_pipelines": 0,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "pipeline_health_only": True,
            "read_only": True,
            "pipelines_executed_by_health_check": False,
            "apply_executed_by_health_check": False,
            "rollback_executed_by_health_check": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "pipeline health summary JSON 缺失；dashboard 不运行 TRADING-023 script、"
                "TRADING-018B-022 或任何 market / backtest / scoring / broker / replay / "
                "trading pipeline。"
            ),
        }

    output_artifacts = _mapping_value(payload, "output_artifacts")
    markdown_path = Path(
        _string_value(_mapping_value(output_artifacts, "markdown").get("path"))
        or str(path.with_suffix(".md"))
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    coverage = _mapping_value(payload, "coverage")
    contract = _mapping_value(payload, "pipeline_contract")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    pipelines_executed = payload.get("pipelines_executed_by_health_check") is True
    apply_executed = payload.get("apply_executed_by_health_check") is True
    rollback_executed = payload.get("rollback_executed_by_health_check") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-023 pipeline health summary production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-023 pipeline health summary 必须 manual_review_only=true。")
    if payload.get("pipeline_health_only") is not True:
        risks.append("TRADING-023 pipeline health summary 必须 pipeline_health_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-023 pipeline health summary 必须 read_only=true。")
    if pipelines_executed:
        risks.append("TRADING-023 pipeline health summary 不允许运行上游 pipeline。")
    if apply_executed:
        risks.append("TRADING-023 pipeline health summary 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-023 pipeline health summary 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-023 pipeline health summary 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-023 pipeline health summary 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-023 pipeline health summary 不允许 trading_execution=true。")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_promotion_apply",
        "runs_promotion_rollback",
        "runs_lifecycle_audit_pipeline",
        "runs_governance_summary_pipeline",
        "runs_web_view_render_script",
        "runs_daily_digest_script",
        "runs_operator_brief_script",
        "runs_pipeline_health_summary_script",
        "runs_market_pipeline",
        "runs_backtest_pipeline",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_shadow_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "triggers_trade",
    ):
        if contract.get(field) is True:
            risks.append(f"pipeline health summary safety contract 异常：{field}。")

    return {
        "status": _string_value(payload.get("health_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_summary_markdown_path": str(markdown_path),
        "health_status": _string_value(payload.get("health_status")) or "MISSING",
        "summary_level": _string_value(payload.get("summary_level")) or "UNKNOWN",
        "headline": _string_value(payload.get("headline")),
        "required_pipelines": _optional_int(coverage.get("required_pipelines")) or 0,
        "missing_required_pipelines": (
            _optional_int(coverage.get("missing_required_pipelines")) or 0
        ),
        "stale_required_pipelines": (_optional_int(coverage.get("stale_required_pipelines")) or 0),
        "critical_pipelines": _optional_int(coverage.get("critical_pipelines")) or 0,
        "warning_pipelines": _optional_int(coverage.get("warning_pipelines")) or 0,
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "pipeline_health_only": payload.get("pipeline_health_only") is True,
        "read_only": payload.get("read_only") is True,
        "pipelines_executed_by_health_check": pipelines_executed,
        "apply_executed_by_health_check": apply_executed,
        "rollback_executed_by_health_check": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Pipeline Health Summary 当前仅作只读展示。",
    }


def _data_freshness_summary_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_data_freshness_summary_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "data_freshness_summary":
        default_markdown = (
            report.project_root
            / "data"
            / "derived"
            / "data_freshness"
            / f"data_freshness_summary_{report.as_of.isoformat()}.md"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_summary_markdown_path": str(default_markdown),
            "freshness_status": "MISSING",
            "summary_level": "UNKNOWN",
            "headline": "",
            "required_sources": 0,
            "missing_required_sources": 0,
            "stale_required_sources": 0,
            "critical_sources": 0,
            "warning_sources": 0,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "data_freshness_only": True,
            "read_only": True,
            "data_downloaded_by_freshness_check": False,
            "pipelines_executed_by_freshness_check": False,
            "apply_executed_by_freshness_check": False,
            "rollback_executed_by_freshness_check": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "data freshness summary JSON 缺失；dashboard 不运行 TRADING-024 script、"
                "TRADING-018B-023 或任何 data download / market / backtest / scoring / "
                "broker / replay / trading pipeline。"
            ),
        }

    output_artifacts = _mapping_value(payload, "output_artifacts")
    markdown_path = Path(
        _string_value(_mapping_value(output_artifacts, "markdown").get("path"))
        or str(path.with_suffix(".md"))
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    coverage = _mapping_value(payload, "coverage")
    contract = _mapping_value(payload, "freshness_contract")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    data_downloaded = payload.get("data_downloaded_by_freshness_check") is True
    pipelines_executed = payload.get("pipelines_executed_by_freshness_check") is True
    apply_executed = payload.get("apply_executed_by_freshness_check") is True
    rollback_executed = payload.get("rollback_executed_by_freshness_check") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-024 data freshness summary production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-024 data freshness summary 必须 manual_review_only=true。")
    if payload.get("data_freshness_only") is not True:
        risks.append("TRADING-024 data freshness summary 必须 data_freshness_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-024 data freshness summary 必须 read_only=true。")
    if data_downloaded:
        risks.append("TRADING-024 data freshness summary 不允许下载或刷新数据。")
    if pipelines_executed:
        risks.append("TRADING-024 data freshness summary 不允许运行上游 pipeline。")
    if apply_executed:
        risks.append("TRADING-024 data freshness summary 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-024 data freshness summary 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-024 data freshness summary 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-024 data freshness summary 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-024 data freshness summary 不允许 trading_execution=true。")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_promotion_apply",
        "runs_promotion_rollback",
        "runs_lifecycle_audit_pipeline",
        "runs_governance_summary_pipeline",
        "runs_web_view_render_script",
        "runs_daily_digest_script",
        "runs_operator_brief_script",
        "runs_pipeline_health_summary_script",
        "runs_data_freshness_summary_script",
        "runs_market_pipeline",
        "runs_backtest_pipeline",
        "runs_scoring_pipeline",
        "runs_data_download",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_shadow_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "triggers_trade",
    ):
        if contract.get(field) is True:
            risks.append(f"data freshness summary safety contract 异常：{field}。")

    return {
        "status": _string_value(payload.get("freshness_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_summary_markdown_path": str(markdown_path),
        "freshness_status": _string_value(payload.get("freshness_status")) or "MISSING",
        "summary_level": _string_value(payload.get("summary_level")) or "UNKNOWN",
        "headline": _string_value(payload.get("headline")),
        "required_sources": _optional_int(coverage.get("required_sources")) or 0,
        "missing_required_sources": _optional_int(coverage.get("missing_required_sources")) or 0,
        "stale_required_sources": _optional_int(coverage.get("stale_required_sources")) or 0,
        "critical_sources": _optional_int(coverage.get("critical_sources")) or 0,
        "warning_sources": _optional_int(coverage.get("warning_sources")) or 0,
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "data_freshness_only": payload.get("data_freshness_only") is True,
        "read_only": payload.get("read_only") is True,
        "data_downloaded_by_freshness_check": data_downloaded,
        "pipelines_executed_by_freshness_check": pipelines_executed,
        "apply_executed_by_freshness_check": apply_executed,
        "rollback_executed_by_freshness_check": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Data Freshness Summary 当前仅作只读展示。",
    }


def _daily_trading_system_operator_brief_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_daily_trading_system_operator_brief_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "daily_trading_system_operator_brief":
        default_markdown = (
            report.project_root
            / "data"
            / "derived"
            / "operator_briefs"
            / f"daily_trading_system_operator_brief_{report.as_of.isoformat()}.md"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_brief_markdown_path": str(default_markdown),
            "brief_status": "MISSING",
            "summary_level": "UNKNOWN",
            "headline": "",
            "can_trust_outputs_today": False,
            "manual_action_required": False,
            "parameter_governance_digest_status": "MISSING",
            "pipeline_health_status": "UNKNOWN",
            "pipeline_health_health_status": "UNKNOWN",
            "data_freshness_status": "UNKNOWN",
            "data_freshness_freshness_status": "UNKNOWN",
            "missing_required_pipelines": 0,
            "stale_required_pipelines": 0,
            "missing_required_sources": 0,
            "stale_required_sources": 0,
            "critical_alert_count": 0,
            "warning_count": 0,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "operator_brief_only": True,
            "read_only": True,
            "apply_executed_by_operator_brief": False,
            "rollback_executed_by_operator_brief": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "daily trading system operator brief JSON 缺失；dashboard 不运行 "
                "TRADING-022 script、TRADING-021、TRADING-020、TRADING-019、018B-018F、"
                "TRADING-023、TRADING-024、TRADING-025 "
                "或任何 market / backtest / scoring / broker / replay / trading pipeline。"
            ),
        }

    output_artifacts = _mapping_value(payload, "output_artifacts")
    markdown_path = Path(
        _string_value(_mapping_value(output_artifacts, "markdown").get("path"))
        or str(path.with_suffix(".md"))
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    snapshot = _mapping_value(payload, "system_snapshot")
    governance = _mapping_value(payload, "parameter_governance")
    pipeline = _mapping_value(payload, "pipeline_health")
    freshness = _mapping_value(payload, "data_freshness")
    alerts = _mapping_value(payload, "alerts")
    contract = _mapping_value(payload, "pipeline_contract")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    critical = _strings(alerts.get("critical"))
    warnings = _strings(alerts.get("warnings"))
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-022 operator brief production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-022 operator brief 必须 manual_review_only=true。")
    if payload.get("operator_brief_only") is not True:
        risks.append("TRADING-022 operator brief 必须 operator_brief_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-022 operator brief 必须 read_only=true。")
    if payload.get("apply_executed_by_operator_brief") is not False:
        risks.append("TRADING-022 operator brief 不允许执行 apply。")
    if payload.get("rollback_executed_by_operator_brief") is not False:
        risks.append("TRADING-022 operator brief 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-022 operator brief 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-022 operator brief 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-022 operator brief 不允许 trading_execution=true。")
    for field in (
        "runs_shadow_iteration_pipeline",
        "runs_comparison_pipeline",
        "runs_multi_day_review_pipeline",
        "runs_promotion_proposal_pipeline",
        "runs_apply_preflight_pipeline",
        "runs_promotion_apply",
        "runs_promotion_rollback",
        "runs_lifecycle_audit_pipeline",
        "runs_governance_summary_pipeline",
        "runs_web_view_render_script",
        "runs_daily_digest_script",
        "runs_operator_brief_script",
        "runs_market_pipeline",
        "runs_backtest_pipeline",
        "runs_scoring_pipeline",
        "runs_broker_runner",
        "runs_paper_runner",
        "runs_replay_runner",
        "writes_production_profile",
        "writes_production_weights",
        "writes_shadow_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
        "triggers_trade",
    ):
        if contract.get(field) is True:
            risks.append(f"daily trading system operator brief safety contract 异常：{field}。")

    return {
        "status": _string_value(payload.get("brief_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_brief_markdown_path": str(markdown_path),
        "brief_status": _string_value(payload.get("brief_status")) or "MISSING",
        "summary_level": _string_value(payload.get("summary_level")) or "UNKNOWN",
        "headline": _string_value(payload.get("headline")),
        "can_trust_outputs_today": snapshot.get("can_trust_outputs_today") is True,
        "manual_action_required": snapshot.get("manual_action_required") is True,
        "parameter_governance_digest_status": (
            _string_value(governance.get("digest_status")) or "MISSING"
        ),
        "pipeline_health_status": _string_value(pipeline.get("status")) or "UNKNOWN",
        "pipeline_health_health_status": (
            _string_value(pipeline.get("health_status")) or "UNKNOWN"
        ),
        "data_freshness_status": _string_value(freshness.get("status")) or "UNKNOWN",
        "data_freshness_freshness_status": (
            _string_value(freshness.get("freshness_status")) or "UNKNOWN"
        ),
        "missing_required_pipelines": _int_value(pipeline.get("missing_required_pipelines")),
        "stale_required_pipelines": _int_value(pipeline.get("stale_required_pipelines")),
        "missing_required_sources": _int_value(freshness.get("missing_required_sources")),
        "stale_required_sources": _int_value(freshness.get("stale_required_sources")),
        "critical_alert_count": len(critical),
        "warning_count": len(warnings),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "operator_brief_only": payload.get("operator_brief_only") is True,
        "read_only": payload.get("read_only") is True,
        "apply_executed_by_operator_brief": (
            payload.get("apply_executed_by_operator_brief") is True
        ),
        "rollback_executed_by_operator_brief": (
            payload.get("rollback_executed_by_operator_brief") is True
        ),
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Daily Trading System Operator Brief 当前仅作只读展示。",
    }


def _daily_operator_brief_scheduler_dry_run_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_daily_operator_brief_scheduler_dry_run_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "daily_operator_brief_scheduler_dry_run":
        default_markdown = (
            report.project_root
            / "data"
            / "derived"
            / "operator_briefs"
            / "scheduler_dry_run"
            / f"daily_operator_brief_scheduler_dry_run_{report.as_of.isoformat()}.md"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "latest_dry_run_markdown_path": str(default_markdown),
            "dry_run_decision": "MISSING",
            "dry_run_status": "MISSING",
            "summary_level": "UNKNOWN",
            "expected_run_time_local": "",
            "dependency_check_status": "MISSING",
            "safety_check_status": "MISSING",
            "missing_required_inputs_count": 0,
            "missing_optional_inputs_count": 0,
            "stale_inputs_count": 0,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "scheduler_dry_run_only": True,
            "read_only": True,
            "scheduler_created": False,
            "operator_brief_executed_by_scheduler_dry_run": False,
            "pipelines_executed_by_scheduler_dry_run": False,
            "data_downloaded_by_scheduler_dry_run": False,
            "apply_executed_by_scheduler_dry_run": False,
            "rollback_executed_by_scheduler_dry_run": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "daily operator brief scheduler dry run JSON 缺失；dashboard 不运行 "
                "018B-025、TRADING-026 script、operator brief、scheduler creation、"
                "market / backtest / scoring / data download / broker / replay / trading。"
            ),
        }

    output_artifacts = _mapping_value(payload, "output_artifacts")
    markdown_path = Path(
        _string_value(_mapping_value(output_artifacts, "markdown").get("path"))
        or str(path.with_suffix(".md"))
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    schedule = _mapping_value(payload, "schedule_plan")
    dependency = _mapping_value(payload, "dependency_check")
    safety = _mapping_value(payload, "safety_check")
    contract = _mapping_value(payload, "scheduler_contract")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    scheduler_created = payload.get("scheduler_created") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_scheduler_dry_run") is True
    pipelines_executed = payload.get("pipelines_executed_by_scheduler_dry_run") is True
    data_downloaded = payload.get("data_downloaded_by_scheduler_dry_run") is True
    apply_executed = payload.get("apply_executed_by_scheduler_dry_run") is True
    rollback_executed = payload.get("rollback_executed_by_scheduler_dry_run") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-026 scheduler dry run production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-026 scheduler dry run 必须 manual_review_only=true。")
    if payload.get("scheduler_dry_run_only") is not True:
        risks.append("TRADING-026 scheduler dry run 必须 scheduler_dry_run_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-026 scheduler dry run 必须 read_only=true。")
    if payload.get("safe_for_scheduler") is not True:
        risks.append("TRADING-026 scheduler dry run 本身必须 safe_for_scheduler=true。")
    if scheduler_created:
        risks.append("TRADING-026 scheduler dry run 不允许创建真实 scheduler。")
    if operator_brief_executed:
        risks.append("TRADING-026 scheduler dry run 不允许运行 TRADING-022。")
    if pipelines_executed:
        risks.append("TRADING-026 scheduler dry run 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-026 scheduler dry run 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-026 scheduler dry run 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-026 scheduler dry run 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-026 scheduler dry run 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-026 scheduler dry run 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-026 scheduler dry run 不允许 trading_execution=true。")
    for field in (
        "runs_daily_digest_script",
        "runs_pipeline_health_summary_script",
        "runs_data_freshness_summary_script",
        "runs_operator_brief_script",
        "creates_windows_task_scheduler_task",
        "creates_cron_job",
        "creates_github_actions_workflow",
        "runs_market_pipeline",
        "runs_backtest_pipeline",
        "runs_scoring_pipeline",
        "runs_data_download",
        "runs_broker_runner",
        "runs_replay_runner",
        "triggers_trade",
        "writes_production_profile",
        "writes_production_weights",
        "writes_shadow_weights",
        "writes_approved_profile",
        "promotes_shadow_to_production",
    ):
        if contract.get(field) is True:
            risks.append(f"daily operator brief scheduler dry run safety contract 异常：{field}。")

    return {
        "status": _string_value(payload.get("dry_run_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "latest_dry_run_markdown_path": str(markdown_path),
        "dry_run_decision": _string_value(payload.get("dry_run_decision")) or "MISSING",
        "dry_run_status": _string_value(payload.get("dry_run_status")) or "MISSING",
        "summary_level": _string_value(payload.get("summary_level")) or "UNKNOWN",
        "expected_run_time_local": _string_value(schedule.get("expected_run_time_local")),
        "dependency_check_status": _string_value(dependency.get("status")) or "MISSING",
        "safety_check_status": _string_value(safety.get("status")) or "MISSING",
        "missing_required_inputs_count": len(_strings(dependency.get("missing_required_inputs"))),
        "missing_optional_inputs_count": len(_strings(dependency.get("missing_optional_inputs"))),
        "stale_inputs_count": len(_strings(dependency.get("stale_inputs"))),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "scheduler_dry_run_only": payload.get("scheduler_dry_run_only") is True,
        "read_only": payload.get("read_only") is True,
        "scheduler_created": scheduler_created,
        "operator_brief_executed_by_scheduler_dry_run": operator_brief_executed,
        "pipelines_executed_by_scheduler_dry_run": pipelines_executed,
        "data_downloaded_by_scheduler_dry_run": data_downloaded,
        "apply_executed_by_scheduler_dry_run": apply_executed,
        "rollback_executed_by_scheduler_dry_run": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Daily Operator Brief Scheduler Dry Run 当前仅作只读展示。",
    }


def _daily_operator_brief_scheduler_templates_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_daily_operator_brief_scheduler_templates_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "daily_operator_brief_scheduler_templates":
        default_markdown = (
            report.project_root
            / "data"
            / "derived"
            / "operator_briefs"
            / "scheduler_templates"
            / f"daily_operator_brief_scheduler_templates_{report.as_of.isoformat()}.md"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "summary_markdown_path": str(default_markdown),
            "template_generation_status": "MISSING",
            "scheduler_created": False,
            "scheduler_installed": False,
            "scheduler_enabled": False,
            "manual_review_required": True,
            "generated_template_count": 0,
            "windows_template_path": "",
            "cron_template_path": "",
            "github_actions_template_path": "",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "scheduler_template_only": True,
            "read_only": True,
            "operator_brief_executed_by_template_generator": False,
            "pipelines_executed_by_template_generator": False,
            "data_downloaded_by_template_generator": False,
            "apply_executed_by_template_generator": False,
            "rollback_executed_by_template_generator": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "TRADING-028 scheduler template metadata 缺失；dashboard 不运行 018B-027、"
                "TRADING-028 script、operator brief、scheduler creation、market / backtest / "
                "scoring / data download / broker / replay / trading。"
            ),
        }

    output_artifacts = _mapping_value(payload, "output_artifacts")
    summary_markdown_value = _string_value(
        _mapping_value(output_artifacts, "summary_markdown").get("path")
    ) or _string_value(payload.get("summary_markdown_path"))
    summary_markdown_path = _project_path(report.project_root, summary_markdown_value) or (
        path.with_suffix(".md")
    )
    report_href = (
        _report_href(summary_markdown_path, report.reports_dir)
        if summary_markdown_path.exists()
        else ""
    )
    output_templates = _mapping_value(payload, "output_templates")
    windows_template = _mapping_value(output_templates, "windows_task_xml")
    cron_template = _mapping_value(output_templates, "cron_line")
    github_template = _mapping_value(output_templates, "github_actions_workflow")
    manual_review = _mapping_value(payload, "manual_review_required")
    safety = _mapping_value(payload, "safety_validation")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    scheduler_created = payload.get("scheduler_created") is True
    scheduler_installed = payload.get("scheduler_installed") is True
    scheduler_enabled = payload.get("scheduler_enabled") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_template_generator") is True
    pipelines_executed = payload.get("pipelines_executed_by_template_generator") is True
    data_downloaded = payload.get("data_downloaded_by_template_generator") is True
    apply_executed = payload.get("apply_executed_by_template_generator") is True
    rollback_executed = payload.get("rollback_executed_by_template_generator") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-028 scheduler templates production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-028 scheduler templates 必须 manual_review_only=true。")
    if payload.get("scheduler_template_only") is not True:
        risks.append("TRADING-028 scheduler templates 必须 scheduler_template_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-028 scheduler templates 必须 read_only=true。")
    if scheduler_created:
        risks.append("TRADING-028 不允许创建真实 scheduler。")
    if scheduler_installed:
        risks.append("TRADING-028 不允许安装 scheduler。")
    if scheduler_enabled:
        risks.append("TRADING-028 不允许启用 scheduler。")
    if operator_brief_executed:
        risks.append("TRADING-028 template generator 不允许运行 TRADING-022。")
    if pipelines_executed:
        risks.append("TRADING-028 template generator 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-028 template generator 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-028 template generator 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-028 template generator 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-028 template generator 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-028 template generator 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-028 template generator 不允许 trading_execution=true。")
    if safety.get("status") == "FAIL":
        risks.extend(_strings(safety.get("blocking_reasons")))

    return {
        "status": _string_value(payload.get("template_generation_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "summary_markdown_path": str(summary_markdown_path),
        "template_generation_status": (
            _string_value(payload.get("template_generation_status")) or "MISSING"
        ),
        "scheduler_created": scheduler_created,
        "scheduler_installed": scheduler_installed,
        "scheduler_enabled": scheduler_enabled,
        "manual_review_required": manual_review.get("required") is True,
        "generated_template_count": _int_value(payload.get("generated_template_count")),
        "windows_template_path": _string_value(windows_template.get("path")),
        "cron_template_path": _string_value(cron_template.get("path")),
        "github_actions_template_path": _string_value(github_template.get("path")),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "scheduler_template_only": payload.get("scheduler_template_only") is True,
        "read_only": payload.get("read_only") is True,
        "operator_brief_executed_by_template_generator": operator_brief_executed,
        "pipelines_executed_by_template_generator": pipelines_executed,
        "data_downloaded_by_template_generator": data_downloaded,
        "apply_executed_by_template_generator": apply_executed,
        "rollback_executed_by_template_generator": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Scheduler Configuration Templates 当前仅作只读展示。",
    }


def _daily_operator_brief_scheduler_template_validation_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_daily_operator_brief_scheduler_template_validation_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "daily_operator_brief_scheduler_template_validation":
        default_markdown = (
            report.project_root
            / "data"
            / "derived"
            / "operator_briefs"
            / "scheduler_template_validation"
            / f"daily_operator_brief_scheduler_template_validation_{report.as_of.isoformat()}.md"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "validation_markdown_path": str(default_markdown),
            "validation_status": "MISSING",
            "summary_level": "UNKNOWN",
            "templates_declared": 0,
            "templates_found": 0,
            "templates_passed": 0,
            "templates_with_warnings": 0,
            "templates_failed": 0,
            "critical_findings_count": 0,
            "warnings_count": 0,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "scheduler_template_validation_only": True,
            "read_only": True,
            "scheduler_created": False,
            "scheduler_installed": False,
            "scheduler_enabled": False,
            "templates_executed_by_validator": False,
            "operator_brief_executed_by_validator": False,
            "pipelines_executed_by_validator": False,
            "data_downloaded_by_validator": False,
            "apply_executed_by_validator": False,
            "rollback_executed_by_validator": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "TRADING-029 scheduler template validation JSON 缺失；dashboard 不运行 "
                "018B-028、TRADING-029 script、template generator、operator brief、"
                "scheduler creation、market / backtest / scoring / data download / "
                "broker / replay / trading。"
            ),
        }

    output_artifacts = _mapping_value(payload, "output_artifacts")
    validation_markdown_value = _string_value(
        _mapping_value(output_artifacts, "validation_markdown").get("path")
    )
    validation_markdown_path = _project_path(report.project_root, validation_markdown_value) or (
        path.with_suffix(".md")
    )
    report_href = (
        _report_href(validation_markdown_path, report.reports_dir)
        if validation_markdown_path.exists()
        else ""
    )
    coverage = _mapping_value(payload, "coverage")
    alerts = _mapping_value(payload, "alerts")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    scheduler_created = payload.get("scheduler_created") is True
    scheduler_installed = payload.get("scheduler_installed") is True
    scheduler_enabled = payload.get("scheduler_enabled") is True
    templates_executed = payload.get("templates_executed_by_validator") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_validator") is True
    pipelines_executed = payload.get("pipelines_executed_by_validator") is True
    data_downloaded = payload.get("data_downloaded_by_validator") is True
    apply_executed = payload.get("apply_executed_by_validator") is True
    rollback_executed = payload.get("rollback_executed_by_validator") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-029 scheduler template validation production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-029 scheduler template validation 必须 manual_review_only=true。")
    if payload.get("scheduler_template_validation_only") is not True:
        risks.append(
            "TRADING-029 scheduler template validation 必须 "
            "scheduler_template_validation_only=true。"
        )
    if payload.get("read_only") is not True:
        risks.append("TRADING-029 scheduler template validation 必须 read_only=true。")
    if scheduler_created:
        risks.append("TRADING-029 不允许创建真实 scheduler。")
    if scheduler_installed:
        risks.append("TRADING-029 不允许安装 scheduler。")
    if scheduler_enabled:
        risks.append("TRADING-029 不允许启用 scheduler。")
    if templates_executed:
        risks.append("TRADING-029 validator 不允许运行模板。")
    if operator_brief_executed:
        risks.append("TRADING-029 validator 不允许运行 operator brief。")
    if pipelines_executed:
        risks.append("TRADING-029 validator 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-029 validator 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-029 validator 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-029 validator 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-029 validator 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-029 validator 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-029 validator 不允许 trading_execution=true。")

    return {
        "status": _string_value(payload.get("validation_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "validation_markdown_path": str(validation_markdown_path),
        "validation_status": _string_value(payload.get("validation_status")) or "MISSING",
        "summary_level": _string_value(payload.get("summary_level")) or "UNKNOWN",
        "templates_declared": _int_value(coverage.get("templates_declared")),
        "templates_found": _int_value(coverage.get("templates_found")),
        "templates_passed": _int_value(coverage.get("templates_passed")),
        "templates_with_warnings": _int_value(coverage.get("templates_with_warnings")),
        "templates_failed": _int_value(coverage.get("templates_failed")),
        "critical_findings_count": len(_strings(alerts.get("critical"))),
        "warnings_count": len(_strings(alerts.get("warnings"))),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "scheduler_template_validation_only": (
            payload.get("scheduler_template_validation_only") is True
        ),
        "read_only": payload.get("read_only") is True,
        "scheduler_created": scheduler_created,
        "scheduler_installed": scheduler_installed,
        "scheduler_enabled": scheduler_enabled,
        "templates_executed_by_validator": templates_executed,
        "operator_brief_executed_by_validator": operator_brief_executed,
        "pipelines_executed_by_validator": pipelines_executed,
        "data_downloaded_by_validator": data_downloaded,
        "apply_executed_by_validator": apply_executed,
        "rollback_executed_by_validator": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": ("；".join(risks) or "Scheduler Template Validation Report 当前仅作只读展示。"),
    }


def _operator_brief_notification_draft_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_operator_brief_notification_draft_path(report)
    payload = _read_json_object(path)
    if payload.get("report_type") != "operator_brief_notification_draft":
        default_markdown = (
            report.project_root
            / "data"
            / "derived"
            / "operator_briefs"
            / "notifications"
            / f"operator_brief_notification_draft_{report.as_of.isoformat()}.md"
        )
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "summary_markdown_path": str(default_markdown),
            "draft_status": "MISSING",
            "notification_severity": "UNKNOWN",
            "headline": "",
            "email_draft_path": "",
            "chat_draft_path": "",
            "mobile_summary_path": "",
            "manual_review_required": True,
            "email_sent": False,
            "gmail_draft_created": False,
            "slack_sent": False,
            "discord_sent": False,
            "mobile_push_sent": False,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "notification_draft_only": True,
            "read_only": True,
            "operator_brief_executed_by_notification_draft": False,
            "pipelines_executed_by_notification_draft": False,
            "data_downloaded_by_notification_draft": False,
            "apply_executed_by_notification_draft": False,
            "rollback_executed_by_notification_draft": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "TRADING-030 notification draft metadata 缺失；dashboard 不运行 018B-029、"
                "TRADING-030 script、operator brief、email/Gmail/Slack/Discord/mobile push、"
                "market / backtest / scoring / data download / broker / replay / trading。"
            ),
        }

    outputs = _mapping_value(payload, "draft_outputs")
    email_output = _mapping_value(outputs, "email_draft")
    chat_output = _mapping_value(outputs, "chat_draft")
    mobile_output = _mapping_value(outputs, "mobile_summary")
    summary_output = _mapping_value(outputs, "summary_markdown")
    summary_markdown_path = _project_path(
        report.project_root,
        _string_value(summary_output.get("path")),
    ) or path.with_suffix(".md")
    report_href = (
        _report_href(summary_markdown_path, report.reports_dir)
        if summary_markdown_path.exists()
        else ""
    )
    manual_review = _mapping_value(payload, "manual_review_required")
    safety = _mapping_value(payload, "safety_validation")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    email_sent = payload.get("email_sent") is True
    gmail_draft_created = payload.get("gmail_draft_created") is True
    slack_sent = payload.get("slack_sent") is True
    discord_sent = payload.get("discord_sent") is True
    mobile_push_sent = payload.get("mobile_push_sent") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_notification_draft") is True
    pipelines_executed = payload.get("pipelines_executed_by_notification_draft") is True
    data_downloaded = payload.get("data_downloaded_by_notification_draft") is True
    apply_executed = payload.get("apply_executed_by_notification_draft") is True
    rollback_executed = payload.get("rollback_executed_by_notification_draft") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-030 notification draft production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-030 notification draft 必须 manual_review_only=true。")
    if payload.get("notification_draft_only") is not True:
        risks.append("TRADING-030 notification draft 必须 notification_draft_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-030 notification draft 必须 read_only=true。")
    if email_sent:
        risks.append("TRADING-030 不允许发送 email。")
    if gmail_draft_created:
        risks.append("TRADING-030 不允许创建 Gmail draft。")
    if slack_sent:
        risks.append("TRADING-030 不允许发送 Slack 通知。")
    if discord_sent:
        risks.append("TRADING-030 不允许发送 Discord 通知。")
    if mobile_push_sent:
        risks.append("TRADING-030 不允许发送 mobile push。")
    if operator_brief_executed:
        risks.append("TRADING-030 不允许运行 operator brief。")
    if pipelines_executed:
        risks.append("TRADING-030 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-030 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-030 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-030 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-030 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-030 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-030 不允许 trading_execution=true。")
    if safety.get("status") == "FAIL":
        risks.extend(_strings(safety.get("blocking_reasons")))

    return {
        "status": _string_value(payload.get("draft_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "summary_markdown_path": str(summary_markdown_path),
        "draft_status": _string_value(payload.get("draft_status")) or "MISSING",
        "notification_severity": (_string_value(payload.get("notification_severity")) or "UNKNOWN"),
        "headline": _string_value(payload.get("headline")),
        "email_draft_path": _string_value(email_output.get("path")),
        "chat_draft_path": _string_value(chat_output.get("path")),
        "mobile_summary_path": _string_value(mobile_output.get("path")),
        "manual_review_required": manual_review.get("required") is True,
        "email_sent": email_sent,
        "gmail_draft_created": gmail_draft_created,
        "slack_sent": slack_sent,
        "discord_sent": discord_sent,
        "mobile_push_sent": mobile_push_sent,
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "notification_draft_only": payload.get("notification_draft_only") is True,
        "read_only": payload.get("read_only") is True,
        "operator_brief_executed_by_notification_draft": operator_brief_executed,
        "pipelines_executed_by_notification_draft": pipelines_executed,
        "data_downloaded_by_notification_draft": data_downloaded,
        "apply_executed_by_notification_draft": apply_executed,
        "rollback_executed_by_notification_draft": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or "Operator Brief Notification Draft 当前仅作只读展示。",
    }


def _operator_brief_notification_delivery_preflight_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_operator_brief_notification_delivery_preflight_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "delivery_preflight"
        / f"operator_brief_notification_delivery_preflight_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "operator_brief_notification_delivery_preflight":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "preflight_markdown_path": str(default_markdown),
            "preflight_status": "MISSING",
            "delivery_readiness": "UNKNOWN",
            "notification_severity": "UNKNOWN",
            "email_channel_status": "MISSING",
            "chat_channel_status": "MISSING",
            "mobile_channel_status": "MISSING",
            "approval_required": True,
            "critical_alert_count": 0,
            "warning_count": 0,
            "email_sent": False,
            "gmail_draft_created": False,
            "gmail_draft_modified": False,
            "slack_sent": False,
            "discord_sent": False,
            "webhook_called": False,
            "mobile_push_sent": False,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "notification_delivery_preflight_only": True,
            "read_only": True,
            "operator_brief_executed_by_delivery_preflight": False,
            "notification_draft_executed_by_delivery_preflight": False,
            "pipelines_executed_by_delivery_preflight": False,
            "data_downloaded_by_delivery_preflight": False,
            "apply_executed_by_delivery_preflight": False,
            "rollback_executed_by_delivery_preflight": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "TRADING-031 delivery preflight artifact 缺失；dashboard 不运行 "
                "018B-030、TRADING-031 script、operator brief、notification draft "
                "generator、email/Gmail/Slack/Discord/webhook/mobile、broker/replay/交易。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    preflight_output = _mapping_value(outputs, "preflight_markdown")
    markdown_path = (
        _project_path(report.project_root, _string_value(preflight_output.get("path")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    approval = _mapping_value(payload, "approval_validation")
    channels = _mapping_value(payload, "channel_readiness")
    email_channel = _mapping_value(channels, "email")
    chat_channel = _mapping_value(channels, "chat")
    mobile_channel = _mapping_value(channels, "mobile")
    alerts = _mapping_value(payload, "alerts")
    safety = _mapping_value(payload, "safety_validation")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    email_sent = payload.get("email_sent") is True
    gmail_draft_created = payload.get("gmail_draft_created") is True
    gmail_draft_modified = payload.get("gmail_draft_modified") is True
    slack_sent = payload.get("slack_sent") is True
    discord_sent = payload.get("discord_sent") is True
    webhook_called = payload.get("webhook_called") is True
    mobile_push_sent = payload.get("mobile_push_sent") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_delivery_preflight") is True
    notification_draft_executed = (
        payload.get("notification_draft_executed_by_delivery_preflight") is True
    )
    pipelines_executed = payload.get("pipelines_executed_by_delivery_preflight") is True
    data_downloaded = payload.get("data_downloaded_by_delivery_preflight") is True
    apply_executed = payload.get("apply_executed_by_delivery_preflight") is True
    rollback_executed = payload.get("rollback_executed_by_delivery_preflight") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-031 delivery preflight production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-031 delivery preflight 必须 manual_review_only=true。")
    if payload.get("notification_delivery_preflight_only") is not True:
        risks.append("TRADING-031 必须 notification_delivery_preflight_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-031 delivery preflight 必须 read_only=true。")
    if email_sent:
        risks.append("TRADING-031 不允许发送 email。")
    if gmail_draft_created:
        risks.append("TRADING-031 不允许创建 Gmail draft。")
    if gmail_draft_modified:
        risks.append("TRADING-031 不允许修改 Gmail draft。")
    if slack_sent:
        risks.append("TRADING-031 不允许发送 Slack 通知。")
    if discord_sent:
        risks.append("TRADING-031 不允许发送 Discord 通知。")
    if webhook_called:
        risks.append("TRADING-031 不允许调用 webhook。")
    if mobile_push_sent:
        risks.append("TRADING-031 不允许发送 mobile push。")
    if operator_brief_executed:
        risks.append("TRADING-031 不允许运行 operator brief。")
    if notification_draft_executed:
        risks.append("TRADING-031 不允许运行 notification draft generator。")
    if pipelines_executed:
        risks.append("TRADING-031 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-031 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-031 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-031 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-031 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-031 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-031 不允许 trading_execution=true。")
    if safety.get("status") == "FAIL":
        risks.extend(_strings(safety.get("blocking_reasons")))
    return {
        "status": _string_value(payload.get("preflight_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "preflight_markdown_path": str(markdown_path),
        "preflight_status": _string_value(payload.get("preflight_status")) or "MISSING",
        "delivery_readiness": _string_value(payload.get("delivery_readiness")) or "UNKNOWN",
        "notification_severity": (_string_value(payload.get("notification_severity")) or "UNKNOWN"),
        "email_channel_status": _string_value(email_channel.get("status")) or "MISSING",
        "chat_channel_status": _string_value(chat_channel.get("status")) or "MISSING",
        "mobile_channel_status": _string_value(mobile_channel.get("status")) or "MISSING",
        "approval_required": approval.get("approval_required") is True,
        "critical_alert_count": len(_strings(alerts.get("critical"))),
        "warning_count": len(_strings(alerts.get("warnings"))),
        "email_sent": email_sent,
        "gmail_draft_created": gmail_draft_created,
        "gmail_draft_modified": gmail_draft_modified,
        "slack_sent": slack_sent,
        "discord_sent": discord_sent,
        "webhook_called": webhook_called,
        "mobile_push_sent": mobile_push_sent,
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "notification_delivery_preflight_only": (
            payload.get("notification_delivery_preflight_only") is True
        ),
        "read_only": payload.get("read_only") is True,
        "operator_brief_executed_by_delivery_preflight": operator_brief_executed,
        "notification_draft_executed_by_delivery_preflight": notification_draft_executed,
        "pipelines_executed_by_delivery_preflight": pipelines_executed,
        "data_downloaded_by_delivery_preflight": data_downloaded,
        "apply_executed_by_delivery_preflight": apply_executed,
        "rollback_executed_by_delivery_preflight": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks)
        or ("Operator Brief Notification Delivery Preflight 当前仅作只读展示。"),
    }


def _operator_brief_notification_dispatch_preview_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_operator_brief_notification_dispatch_preview_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "dispatch_preview"
        / f"operator_brief_notification_dispatch_preview_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "operator_brief_notification_dispatch_preview":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "dispatch_preview_markdown_path": str(default_markdown),
            "latest_artifact_path": str(path),
            "final_status": "MISSING",
            "preflight_status": "MISSING",
            "dispatch_status": "MISSING",
            "channel_count": 0,
            "would_send_channel_count": 0,
            "human_action_required": True,
            "next_recommended_action": "",
            "generated_at": "",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "dispatch_preview_only": True,
            "read_only": True,
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "email_sent": False,
            "gmail_draft_created": False,
            "gmail_draft_modified": False,
            "slack_sent": False,
            "telegram_sent": False,
            "discord_sent": False,
            "webhook_called": False,
            "mobile_push_sent": False,
            "operator_brief_executed_by_dispatch_preview": False,
            "notification_draft_executed_by_dispatch_preview": False,
            "delivery_preflight_executed_by_dispatch_preview": False,
            "pipelines_executed_by_dispatch_preview": False,
            "data_downloaded_by_dispatch_preview": False,
            "apply_executed_by_dispatch_preview": False,
            "rollback_executed_by_dispatch_preview": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "TRADING-032 dispatch preview artifact 缺失；dashboard 不运行 "
                "018B-031、TRADING-032 script、delivery preflight、operator brief、"
                "notification draft generator、email/Gmail/Slack/Telegram/Discord/"
                "webhook/mobile、broker/replay/交易。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    preview_output = _mapping_value(outputs, "dispatch_preview_markdown")
    markdown_path = (
        _project_path(report.project_root, _string_value(preview_output.get("path")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    metadata = _mapping_value(payload, "metadata")
    preflight = _mapping_value(payload, "preflight_summary")
    preview = _mapping_value(payload, "dispatch_preview")
    decision = _mapping_value(payload, "decision")
    safety = _mapping_value(payload, "safety")
    channels = _records(preview.get("channels"))
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    external_side_effects = payload.get("external_side_effects") is True
    network_access_required = payload.get("network_access_required") is True
    secrets_required = payload.get("secrets_required") is True
    email_sent = payload.get("email_sent") is True
    gmail_draft_created = payload.get("gmail_draft_created") is True
    gmail_draft_modified = payload.get("gmail_draft_modified") is True
    slack_sent = payload.get("slack_sent") is True
    telegram_sent = payload.get("telegram_sent") is True
    discord_sent = payload.get("discord_sent") is True
    webhook_called = payload.get("webhook_called") is True
    mobile_push_sent = payload.get("mobile_push_sent") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_dispatch_preview") is True
    notification_draft_executed = (
        payload.get("notification_draft_executed_by_dispatch_preview") is True
    )
    delivery_preflight_executed = (
        payload.get("delivery_preflight_executed_by_dispatch_preview") is True
    )
    pipelines_executed = payload.get("pipelines_executed_by_dispatch_preview") is True
    data_downloaded = payload.get("data_downloaded_by_dispatch_preview") is True
    apply_executed = payload.get("apply_executed_by_dispatch_preview") is True
    rollback_executed = payload.get("rollback_executed_by_dispatch_preview") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-032 dispatch preview production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-032 dispatch preview 必须 manual_review_only=true。")
    if payload.get("dispatch_preview_only") is not True:
        risks.append("TRADING-032 必须 dispatch_preview_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-032 dispatch preview 必须 read_only=true。")
    if external_side_effects:
        risks.append("TRADING-032 不允许 external_side_effects=true。")
    if network_access_required:
        risks.append("TRADING-032 不允许 network_access_required=true。")
    if secrets_required:
        risks.append("TRADING-032 不允许 secrets_required=true。")
    if email_sent:
        risks.append("TRADING-032 不允许发送 email。")
    if gmail_draft_created:
        risks.append("TRADING-032 不允许创建 Gmail draft。")
    if gmail_draft_modified:
        risks.append("TRADING-032 不允许修改 Gmail draft。")
    if slack_sent:
        risks.append("TRADING-032 不允许发送 Slack 通知。")
    if telegram_sent:
        risks.append("TRADING-032 不允许发送 Telegram 通知。")
    if discord_sent:
        risks.append("TRADING-032 不允许发送 Discord 通知。")
    if webhook_called:
        risks.append("TRADING-032 不允许调用 webhook。")
    if mobile_push_sent:
        risks.append("TRADING-032 不允许发送 mobile push。")
    if operator_brief_executed:
        risks.append("TRADING-032 不允许运行 operator brief。")
    if notification_draft_executed:
        risks.append("TRADING-032 不允许运行 notification draft generator。")
    if delivery_preflight_executed:
        risks.append("TRADING-032 不允许运行 delivery preflight。")
    if pipelines_executed:
        risks.append("TRADING-032 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-032 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-032 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-032 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-032 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-032 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-032 不允许 trading_execution=true。")
    risks.extend(_strings(safety.get("sensitive_content_flags")))
    return {
        "status": _string_value(decision.get("final_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "dispatch_preview_markdown_path": str(markdown_path),
        "latest_artifact_path": str(path),
        "final_status": _string_value(decision.get("final_status")) or "MISSING",
        "preflight_status": _string_value(preflight.get("status")) or "MISSING",
        "dispatch_status": _string_value(preview.get("dispatch_status")) or "MISSING",
        "channel_count": len(channels),
        "would_send_channel_count": sum(
            1 for channel in channels if channel.get("would_send") is True
        ),
        "human_action_required": decision.get("human_action_required") is True,
        "next_recommended_action": _string_value(decision.get("next_recommended_action")),
        "generated_at": _string_value(metadata.get("generated_at")),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "dispatch_preview_only": payload.get("dispatch_preview_only") is True,
        "read_only": payload.get("read_only") is True,
        "external_side_effects": external_side_effects,
        "network_access_required": network_access_required,
        "secrets_required": secrets_required,
        "email_sent": email_sent,
        "gmail_draft_created": gmail_draft_created,
        "gmail_draft_modified": gmail_draft_modified,
        "slack_sent": slack_sent,
        "telegram_sent": telegram_sent,
        "discord_sent": discord_sent,
        "webhook_called": webhook_called,
        "mobile_push_sent": mobile_push_sent,
        "operator_brief_executed_by_dispatch_preview": operator_brief_executed,
        "notification_draft_executed_by_dispatch_preview": notification_draft_executed,
        "delivery_preflight_executed_by_dispatch_preview": delivery_preflight_executed,
        "pipelines_executed_by_dispatch_preview": pipelines_executed,
        "data_downloaded_by_dispatch_preview": data_downloaded,
        "apply_executed_by_dispatch_preview": apply_executed,
        "rollback_executed_by_dispatch_preview": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks)
        or ("Operator Brief Notification Dispatch Preview 当前仅作只读展示。"),
    }


def _operator_brief_notification_approval_gate_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_operator_brief_notification_approval_gate_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "approval_gate"
        / f"operator_brief_notification_approval_gate_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "operator_brief_notification_approval_gate":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "approval_gate_markdown_path": str(default_markdown),
            "latest_artifact_path": str(path),
            "approval_gate_status": "MISSING",
            "allowed_to_enter_dispatch": False,
            "human_action_required": True,
            "dispatch_preview_status": "MISSING",
            "approval_marker_exists": False,
            "hash_matches": False,
            "expired": False,
            "generated_at": "",
            "next_recommended_action": "",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "approval_gate_only": True,
            "read_only": True,
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "email_sent": False,
            "gmail_draft_created": False,
            "gmail_draft_modified": False,
            "slack_sent": False,
            "telegram_sent": False,
            "discord_sent": False,
            "webhook_called": False,
            "mobile_push_sent": False,
            "operator_brief_executed_by_approval_gate": False,
            "notification_draft_executed_by_approval_gate": False,
            "delivery_preflight_executed_by_approval_gate": False,
            "dispatch_preview_executed_by_approval_gate": False,
            "pipelines_executed_by_approval_gate": False,
            "data_downloaded_by_approval_gate": False,
            "apply_executed_by_approval_gate": False,
            "rollback_executed_by_approval_gate": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "TRADING-033 approval gate artifact 缺失；dashboard 不运行 "
                "018B-032、TRADING-033 script、dispatch preview、delivery preflight、"
                "operator brief、notification draft generator、email/Gmail/SMTP/Slack/"
                "Telegram/Discord/webhook/mobile、broker/replay/交易。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    gate_output = _mapping_value(outputs, "approval_gate_markdown")
    markdown_path = (
        _project_path(report.project_root, _string_value(gate_output.get("path")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    metadata = _mapping_value(payload, "metadata")
    preview = _mapping_value(payload, "dispatch_preview_summary")
    marker = _mapping_value(payload, "approval_marker_summary")
    decision = _mapping_value(payload, "decision")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    external_side_effects = payload.get("external_side_effects") is True
    network_access_required = payload.get("network_access_required") is True
    secrets_required = payload.get("secrets_required") is True
    email_sent = payload.get("email_sent") is True
    gmail_draft_created = payload.get("gmail_draft_created") is True
    gmail_draft_modified = payload.get("gmail_draft_modified") is True
    slack_sent = payload.get("slack_sent") is True
    telegram_sent = payload.get("telegram_sent") is True
    discord_sent = payload.get("discord_sent") is True
    webhook_called = payload.get("webhook_called") is True
    mobile_push_sent = payload.get("mobile_push_sent") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_approval_gate") is True
    notification_draft_executed = (
        payload.get("notification_draft_executed_by_approval_gate") is True
    )
    delivery_preflight_executed = (
        payload.get("delivery_preflight_executed_by_approval_gate") is True
    )
    dispatch_preview_executed = payload.get("dispatch_preview_executed_by_approval_gate") is True
    pipelines_executed = payload.get("pipelines_executed_by_approval_gate") is True
    data_downloaded = payload.get("data_downloaded_by_approval_gate") is True
    apply_executed = payload.get("apply_executed_by_approval_gate") is True
    rollback_executed = payload.get("rollback_executed_by_approval_gate") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    approval_gate_status = _string_value(decision.get("approval_gate_status")) or "MISSING"
    allowed_to_enter_dispatch = decision.get("allowed_to_enter_dispatch") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-033 approval gate production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-033 approval gate 必须 manual_review_only=true。")
    if payload.get("approval_gate_only") is not True:
        risks.append("TRADING-033 必须 approval_gate_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-033 approval gate 必须 read_only=true。")
    if external_side_effects:
        risks.append("TRADING-033 不允许 external_side_effects=true。")
    if network_access_required:
        risks.append("TRADING-033 不允许 network_access_required=true。")
    if secrets_required:
        risks.append("TRADING-033 不允许 secrets_required=true。")
    if email_sent:
        risks.append("TRADING-033 不允许发送 email。")
    if gmail_draft_created:
        risks.append("TRADING-033 不允许创建 Gmail draft。")
    if gmail_draft_modified:
        risks.append("TRADING-033 不允许修改 Gmail draft。")
    if slack_sent:
        risks.append("TRADING-033 不允许发送 Slack 通知。")
    if telegram_sent:
        risks.append("TRADING-033 不允许发送 Telegram 通知。")
    if discord_sent:
        risks.append("TRADING-033 不允许发送 Discord 通知。")
    if webhook_called:
        risks.append("TRADING-033 不允许调用 webhook。")
    if mobile_push_sent:
        risks.append("TRADING-033 不允许发送 mobile push。")
    if operator_brief_executed:
        risks.append("TRADING-033 不允许运行 operator brief。")
    if notification_draft_executed:
        risks.append("TRADING-033 不允许运行 notification draft generator。")
    if delivery_preflight_executed:
        risks.append("TRADING-033 不允许运行 delivery preflight。")
    if dispatch_preview_executed:
        risks.append("TRADING-033 不允许运行 dispatch preview。")
    if pipelines_executed:
        risks.append("TRADING-033 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-033 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-033 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-033 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-033 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-033 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-033 不允许 trading_execution=true。")
    if allowed_to_enter_dispatch and approval_gate_status != "APPROVED":
        risks.append("TRADING-033 只有 APPROVED 才允许 allowed_to_enter_dispatch=true。")
    return {
        "status": approval_gate_status,
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "approval_gate_markdown_path": str(markdown_path),
        "latest_artifact_path": str(path),
        "approval_gate_status": approval_gate_status,
        "allowed_to_enter_dispatch": allowed_to_enter_dispatch,
        "human_action_required": decision.get("human_action_required") is True,
        "dispatch_preview_status": _string_value(preview.get("final_status")) or "MISSING",
        "approval_marker_exists": marker.get("exists") is True,
        "hash_matches": marker.get("hash_matches") is True,
        "expired": marker.get("expired") is True,
        "generated_at": _string_value(metadata.get("generated_at")),
        "next_recommended_action": _string_value(decision.get("next_recommended_action")),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "approval_gate_only": payload.get("approval_gate_only") is True,
        "read_only": payload.get("read_only") is True,
        "external_side_effects": external_side_effects,
        "network_access_required": network_access_required,
        "secrets_required": secrets_required,
        "email_sent": email_sent,
        "gmail_draft_created": gmail_draft_created,
        "gmail_draft_modified": gmail_draft_modified,
        "slack_sent": slack_sent,
        "telegram_sent": telegram_sent,
        "discord_sent": discord_sent,
        "webhook_called": webhook_called,
        "mobile_push_sent": mobile_push_sent,
        "operator_brief_executed_by_approval_gate": operator_brief_executed,
        "notification_draft_executed_by_approval_gate": notification_draft_executed,
        "delivery_preflight_executed_by_approval_gate": delivery_preflight_executed,
        "dispatch_preview_executed_by_approval_gate": dispatch_preview_executed,
        "pipelines_executed_by_approval_gate": pipelines_executed,
        "data_downloaded_by_approval_gate": data_downloaded,
        "apply_executed_by_approval_gate": apply_executed,
        "rollback_executed_by_approval_gate": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks)
        or ("Operator Brief Notification Approval Gate 当前仅作只读展示。"),
    }


def _operator_brief_notification_draft_dispatch_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_operator_brief_notification_draft_dispatch_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "draft_dispatch"
        / f"operator_brief_notification_draft_dispatch_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "operator_brief_notification_draft_dispatch":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "draft_dispatch_markdown_path": str(default_markdown),
            "latest_artifact_path": str(path),
            "final_status": "MISSING",
            "ready_for_actual_dispatch": False,
            "approval_gate_status": "MISSING",
            "channel_count": 0,
            "draft_ready_channel_count": 0,
            "draft_hash": "",
            "generated_at": "",
            "next_recommended_action": "",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "draft_dispatch_only": True,
            "read_only": True,
            "external_side_effects": False,
            "network_access_required": False,
            "secrets_required": False,
            "email_sent": False,
            "gmail_draft_created": False,
            "gmail_draft_modified": False,
            "smtp_called": False,
            "slack_sent": False,
            "telegram_sent": False,
            "discord_sent": False,
            "webhook_called": False,
            "mobile_push_sent": False,
            "operator_brief_executed_by_draft_dispatch": False,
            "notification_draft_executed_by_draft_dispatch": False,
            "delivery_preflight_executed_by_draft_dispatch": False,
            "dispatch_preview_executed_by_draft_dispatch": False,
            "approval_gate_executed_by_draft_dispatch": False,
            "pipelines_executed_by_draft_dispatch": False,
            "data_downloaded_by_draft_dispatch": False,
            "apply_executed_by_draft_dispatch": False,
            "rollback_executed_by_draft_dispatch": False,
            "operator_brief_executed_by_dispatch": False,
            "pipelines_executed_by_dispatch": False,
            "data_downloaded_by_dispatch": False,
            "apply_executed_by_dispatch": False,
            "rollback_executed_by_dispatch": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "TRADING-034 draft dispatch artifact 缺失；dashboard 只读取 latest.json，"
                "不运行 TRADING-034 script、approval gate、dispatch preview、delivery "
                "preflight、operator brief、notification draft generator、email/Gmail/"
                "SMTP/Slack/Telegram/Discord/webhook/mobile、broker/replay/交易。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    draft_output = _mapping_value(outputs, "draft_dispatch_markdown")
    markdown_path = (
        _project_path(report.project_root, _string_value(draft_output.get("path")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    metadata = _mapping_value(payload, "metadata")
    gate = _mapping_value(payload, "approval_gate_summary")
    draft = _mapping_value(payload, "draft")
    decision = _mapping_value(payload, "decision")
    hashes = _mapping_value(payload, "hashes")
    safety = _mapping_value(payload, "safety")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    external_side_effects = payload.get("external_side_effects") is True
    network_access_required = payload.get("network_access_required") is True
    secrets_required = payload.get("secrets_required") is True
    email_sent = payload.get("email_sent") is True
    gmail_draft_created = payload.get("gmail_draft_created") is True
    gmail_draft_modified = payload.get("gmail_draft_modified") is True
    smtp_called = payload.get("smtp_called") is True
    slack_sent = payload.get("slack_sent") is True
    telegram_sent = payload.get("telegram_sent") is True
    discord_sent = payload.get("discord_sent") is True
    webhook_called = payload.get("webhook_called") is True
    mobile_push_sent = payload.get("mobile_push_sent") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_draft_dispatch") is True
    notification_draft_executed = (
        payload.get("notification_draft_executed_by_draft_dispatch") is True
    )
    delivery_preflight_executed = (
        payload.get("delivery_preflight_executed_by_draft_dispatch") is True
    )
    dispatch_preview_executed = payload.get("dispatch_preview_executed_by_draft_dispatch") is True
    approval_gate_executed = payload.get("approval_gate_executed_by_draft_dispatch") is True
    pipelines_executed = payload.get("pipelines_executed_by_draft_dispatch") is True
    data_downloaded = payload.get("data_downloaded_by_draft_dispatch") is True
    apply_executed = payload.get("apply_executed_by_draft_dispatch") is True
    rollback_executed = payload.get("rollback_executed_by_draft_dispatch") is True
    operator_brief_executed_by_dispatch = payload.get("operator_brief_executed_by_dispatch") is True
    pipelines_executed_by_dispatch = payload.get("pipelines_executed_by_dispatch") is True
    data_downloaded_by_dispatch = payload.get("data_downloaded_by_dispatch") is True
    apply_executed_by_dispatch = payload.get("apply_executed_by_dispatch") is True
    rollback_executed_by_dispatch = payload.get("rollback_executed_by_dispatch") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    final_status = _string_value(decision.get("final_status")) or "MISSING"
    ready_for_actual_dispatch = decision.get("ready_for_actual_dispatch") is True
    approval_gate_status = _string_value(gate.get("approval_gate_status")) or "MISSING"
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-034 draft dispatch production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-034 draft dispatch 必须 manual_review_only=true。")
    if payload.get("draft_dispatch_only") is not True:
        risks.append("TRADING-034 必须 draft_dispatch_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-034 draft dispatch 必须 read_only=true。")
    if external_side_effects:
        risks.append("TRADING-034 不允许 external_side_effects=true。")
    if network_access_required:
        risks.append("TRADING-034 不允许 network_access_required=true。")
    if secrets_required:
        risks.append("TRADING-034 不允许 secrets_required=true。")
    if email_sent:
        risks.append("TRADING-034 不允许发送 email。")
    if gmail_draft_created:
        risks.append("TRADING-034 不允许创建 Gmail draft。")
    if gmail_draft_modified:
        risks.append("TRADING-034 不允许修改 Gmail draft。")
    if smtp_called:
        risks.append("TRADING-034 不允许调用 SMTP。")
    if slack_sent:
        risks.append("TRADING-034 不允许发送 Slack 通知。")
    if telegram_sent:
        risks.append("TRADING-034 不允许发送 Telegram 通知。")
    if discord_sent:
        risks.append("TRADING-034 不允许发送 Discord 通知。")
    if webhook_called:
        risks.append("TRADING-034 不允许调用 webhook。")
    if mobile_push_sent:
        risks.append("TRADING-034 不允许发送 mobile push。")
    if operator_brief_executed:
        risks.append("TRADING-034 不允许运行 operator brief。")
    if notification_draft_executed:
        risks.append("TRADING-034 不允许运行 notification draft generator。")
    if delivery_preflight_executed:
        risks.append("TRADING-034 不允许运行 delivery preflight。")
    if dispatch_preview_executed:
        risks.append("TRADING-034 不允许运行 dispatch preview。")
    if approval_gate_executed:
        risks.append("TRADING-034 不允许运行 approval gate。")
    if pipelines_executed:
        risks.append("TRADING-034 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-034 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-034 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-034 不允许执行 rollback。")
    if operator_brief_executed_by_dispatch:
        risks.append("TRADING-034 不允许通用 dispatch 路径运行 operator brief。")
    if pipelines_executed_by_dispatch:
        risks.append("TRADING-034 不允许通用 dispatch 路径运行上游 pipeline。")
    if data_downloaded_by_dispatch:
        risks.append("TRADING-034 不允许通用 dispatch 路径下载或刷新数据。")
    if apply_executed_by_dispatch:
        risks.append("TRADING-034 不允许通用 dispatch 路径执行 apply。")
    if rollback_executed_by_dispatch:
        risks.append("TRADING-034 不允许通用 dispatch 路径执行 rollback。")
    if broker_execution:
        risks.append("TRADING-034 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-034 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-034 不允许 trading_execution=true。")
    if ready_for_actual_dispatch and final_status != "DRAFT_READY":
        risks.append("TRADING-034 只有 DRAFT_READY 才允许 ready_for_actual_dispatch=true。")
    if final_status == "DRAFT_READY" and approval_gate_status != "APPROVED":
        risks.append("TRADING-034 DRAFT_READY 必须来自 APPROVED approval gate。")
    risks.extend(_strings(safety.get("sensitive_content_flags")))
    return {
        "status": final_status,
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "draft_dispatch_markdown_path": str(markdown_path),
        "latest_artifact_path": str(path),
        "final_status": final_status,
        "ready_for_actual_dispatch": ready_for_actual_dispatch,
        "approval_gate_status": approval_gate_status,
        "channel_count": draft.get("channel_count", 0),
        "draft_ready_channel_count": draft.get("draft_ready_channel_count", 0),
        "draft_hash": _string_value(hashes.get("draft_hash")),
        "generated_at": _string_value(metadata.get("generated_at")),
        "next_recommended_action": _string_value(decision.get("next_recommended_action")),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "draft_dispatch_only": payload.get("draft_dispatch_only") is True,
        "read_only": payload.get("read_only") is True,
        "external_side_effects": external_side_effects,
        "network_access_required": network_access_required,
        "secrets_required": secrets_required,
        "email_sent": email_sent,
        "gmail_draft_created": gmail_draft_created,
        "gmail_draft_modified": gmail_draft_modified,
        "smtp_called": smtp_called,
        "slack_sent": slack_sent,
        "telegram_sent": telegram_sent,
        "discord_sent": discord_sent,
        "webhook_called": webhook_called,
        "mobile_push_sent": mobile_push_sent,
        "operator_brief_executed_by_draft_dispatch": operator_brief_executed,
        "notification_draft_executed_by_draft_dispatch": notification_draft_executed,
        "delivery_preflight_executed_by_draft_dispatch": delivery_preflight_executed,
        "dispatch_preview_executed_by_draft_dispatch": dispatch_preview_executed,
        "approval_gate_executed_by_draft_dispatch": approval_gate_executed,
        "pipelines_executed_by_draft_dispatch": pipelines_executed,
        "data_downloaded_by_draft_dispatch": data_downloaded,
        "apply_executed_by_draft_dispatch": apply_executed,
        "rollback_executed_by_draft_dispatch": rollback_executed,
        "operator_brief_executed_by_dispatch": operator_brief_executed_by_dispatch,
        "pipelines_executed_by_dispatch": pipelines_executed_by_dispatch,
        "data_downloaded_by_dispatch": data_downloaded_by_dispatch,
        "apply_executed_by_dispatch": apply_executed_by_dispatch,
        "rollback_executed_by_dispatch": rollback_executed_by_dispatch,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks)
        or ("Operator Brief Notification Draft Dispatch 当前仅作只读展示。"),
    }


def _notification_delivery_audit_summary(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_notification_delivery_audit_summary_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "delivery_audit"
        / f"notification_delivery_audit_summary_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "notification_delivery_audit_summary":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "markdown_path": str(default_markdown),
            "audit_status": "MISSING",
            "notification_lifecycle_status": "UNKNOWN",
            "summary_level": "UNKNOWN",
            "draft_status": "MISSING",
            "preflight_status": "MISSING",
            "dispatch_status": "MISSING",
            "draft_hash_match": False,
            "latest_json_match": False,
            "external_side_effect_audit_status": "MISSING",
            "critical_alert_count": 0,
            "warning_count": 0,
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "notification_delivery_audit_only": True,
            "read_only": True,
            "email_sent": False,
            "gmail_draft_created": False,
            "gmail_draft_modified": False,
            "slack_sent": False,
            "discord_sent": False,
            "webhook_called": False,
            "mobile_push_sent": False,
            "notification_draft_executed_by_audit": False,
            "delivery_preflight_executed_by_audit": False,
            "draft_dispatch_executed_by_audit": False,
            "operator_brief_executed_by_audit": False,
            "pipelines_executed_by_audit": False,
            "data_downloaded_by_audit": False,
            "apply_executed_by_audit": False,
            "rollback_executed_by_audit": False,
            "broker_execution": False,
            "replay_execution": False,
            "trading_execution": False,
            "risk": (
                "TRADING-035 notification delivery audit artifact 缺失；dashboard 只读取 "
                "TRADING-035 audit artifact，不运行 018B-034、TRADING-035 script、"
                "notification draft generator、delivery preflight、draft dispatch、"
                "operator brief、email/Gmail/webhook/mobile 或任何交易路径。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    markdown_output = _mapping_value(outputs, "audit_markdown")
    markdown_path = (
        _project_path(report.project_root, _string_value(markdown_output.get("path")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    chain = _mapping_value(payload, "artifact_chain")
    draft = _mapping_value(payload, "draft_summary")
    preflight = _mapping_value(payload, "preflight_summary")
    dispatch = _mapping_value(payload, "dispatch_summary")
    side_effects = _mapping_value(payload, "external_side_effect_audit")
    alerts = _mapping_value(payload, "alerts")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    email_sent = payload.get("email_sent") is True
    gmail_draft_created = payload.get("gmail_draft_created") is True
    gmail_draft_modified = payload.get("gmail_draft_modified") is True
    slack_sent = payload.get("slack_sent") is True
    discord_sent = payload.get("discord_sent") is True
    webhook_called = payload.get("webhook_called") is True
    mobile_push_sent = payload.get("mobile_push_sent") is True
    notification_draft_executed = payload.get("notification_draft_executed_by_audit") is True
    delivery_preflight_executed = payload.get("delivery_preflight_executed_by_audit") is True
    draft_dispatch_executed = payload.get("draft_dispatch_executed_by_audit") is True
    operator_brief_executed = payload.get("operator_brief_executed_by_audit") is True
    pipelines_executed = payload.get("pipelines_executed_by_audit") is True
    data_downloaded = payload.get("data_downloaded_by_audit") is True
    apply_executed = payload.get("apply_executed_by_audit") is True
    rollback_executed = payload.get("rollback_executed_by_audit") is True
    broker_execution = payload.get("broker_execution") is True
    replay_execution = payload.get("replay_execution") is True
    trading_execution = payload.get("trading_execution") is True
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-035 audit production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-035 audit 必须 manual_review_only=true。")
    if payload.get("notification_delivery_audit_only") is not True:
        risks.append("TRADING-035 必须 notification_delivery_audit_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-035 audit 必须 read_only=true。")
    if email_sent:
        risks.append("TRADING-035 不允许发送 email。")
    if gmail_draft_created:
        risks.append("TRADING-035 不允许创建 Gmail draft。")
    if gmail_draft_modified:
        risks.append("TRADING-035 不允许修改 Gmail draft。")
    if slack_sent:
        risks.append("TRADING-035 不允许发送 Slack 通知。")
    if discord_sent:
        risks.append("TRADING-035 不允许发送 Discord 通知。")
    if webhook_called:
        risks.append("TRADING-035 不允许调用 webhook。")
    if mobile_push_sent:
        risks.append("TRADING-035 不允许发送 mobile push。")
    if notification_draft_executed:
        risks.append("TRADING-035 不允许运行 notification draft generator。")
    if delivery_preflight_executed:
        risks.append("TRADING-035 不允许运行 delivery preflight。")
    if draft_dispatch_executed:
        risks.append("TRADING-035 不允许运行 draft dispatch。")
    if operator_brief_executed:
        risks.append("TRADING-035 不允许运行 operator brief。")
    if pipelines_executed:
        risks.append("TRADING-035 不允许运行上游 pipeline。")
    if data_downloaded:
        risks.append("TRADING-035 不允许下载或刷新数据。")
    if apply_executed:
        risks.append("TRADING-035 不允许执行 apply。")
    if rollback_executed:
        risks.append("TRADING-035 不允许执行 rollback。")
    if broker_execution:
        risks.append("TRADING-035 不允许 broker_execution=true。")
    if replay_execution:
        risks.append("TRADING-035 不允许 replay_execution=true。")
    if trading_execution:
        risks.append("TRADING-035 不允许 trading_execution=true。")
    if side_effects.get("status") == "FAIL":
        risks.extend(_strings(side_effects.get("blocking_reasons")))
    return {
        "status": _string_value(payload.get("audit_status")) or "MISSING",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "markdown_path": str(markdown_path),
        "audit_status": _string_value(payload.get("audit_status")) or "MISSING",
        "notification_lifecycle_status": (
            _string_value(payload.get("notification_lifecycle_status")) or "UNKNOWN"
        ),
        "summary_level": _string_value(payload.get("summary_level")) or "UNKNOWN",
        "draft_status": _string_value(draft.get("draft_status")) or "MISSING",
        "preflight_status": _string_value(preflight.get("preflight_status")) or "MISSING",
        "dispatch_status": _string_value(dispatch.get("dispatch_status")) or "MISSING",
        "draft_hash_match": chain.get("draft_hash_match") is True,
        "latest_json_match": chain.get("dispatch_latest_match") is True,
        "external_side_effect_audit_status": (
            _string_value(side_effects.get("status")) or "MISSING"
        ),
        "critical_alert_count": len(_strings(alerts.get("critical"))),
        "warning_count": len(_strings(alerts.get("warnings"))),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "notification_delivery_audit_only": (
            payload.get("notification_delivery_audit_only") is True
        ),
        "read_only": payload.get("read_only") is True,
        "email_sent": email_sent,
        "gmail_draft_created": gmail_draft_created,
        "gmail_draft_modified": gmail_draft_modified,
        "slack_sent": slack_sent,
        "discord_sent": discord_sent,
        "webhook_called": webhook_called,
        "mobile_push_sent": mobile_push_sent,
        "notification_draft_executed_by_audit": notification_draft_executed,
        "delivery_preflight_executed_by_audit": delivery_preflight_executed,
        "draft_dispatch_executed_by_audit": draft_dispatch_executed,
        "operator_brief_executed_by_audit": operator_brief_executed,
        "pipelines_executed_by_audit": pipelines_executed,
        "data_downloaded_by_audit": data_downloaded,
        "apply_executed_by_audit": apply_executed,
        "rollback_executed_by_audit": rollback_executed,
        "broker_execution": broker_execution,
        "replay_execution": replay_execution,
        "trading_execution": trading_execution,
        "risk": "；".join(risks) or ("Notification Delivery Audit Summary 当前仅作只读展示。"),
    }


def _sec_pit_evaluation_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_sec_pit_evaluation_summary_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "outputs"
        / "sec_pit_evaluation"
        / f"sec_pit_evaluation_summary_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "sec_pit_cognitive_evaluation":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "markdown_path": str(default_markdown),
            "latest_evaluation_date": "",
            "universe_size": 0,
            "feature_count": 0,
            "promote_to_shadow_count": 0,
            "research_only_count": 0,
            "excluded_count": 0,
            "top_features": [],
            "pit_safety_status": "MISSING",
            "production_effect": ProductionEffect.NONE.value,
            "read_only": True,
            "risk": (
                "No SEC PIT evaluation summary available. Dashboard 只读取 "
                "TRADING-040 artifact，不运行 evaluation、不读取市场数据、不修改权重。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    markdown_path = (
        _project_path(report.project_root, _string_value(outputs.get("summary_markdown")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    recommendations = _mapping_value(payload, "recommendations")
    coverage = _mapping_value(payload, "data_coverage")
    pit_violations = _int_value(coverage.get("pit_violation_count"))
    missing_available = _int_value(coverage.get("missing_available_time"))
    excluded = _int_value(coverage.get("excluded_rows"))
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-040 evaluation production_effect 必须为 none。")
    if pit_violations:
        risks.append("存在 available_time 晚于 decision_time 的排除行。")
    if missing_available:
        risks.append("存在缺少 available_time 的排除行。")
    top_features = _sec_pit_top_features(payload.get("top_features"))
    return {
        "status": _string_value(payload.get("status")) or "UNKNOWN",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "markdown_path": str(markdown_path),
        "latest_evaluation_date": _string_value(payload.get("end_date")),
        "universe_size": _int_value(payload.get("universe_size")),
        "feature_count": _int_value(payload.get("feature_count")),
        "promote_to_shadow_count": _int_value(recommendations.get("promote_to_shadow")),
        "research_only_count": _int_value(recommendations.get("keep_research_only")),
        "excluded_count": excluded + _int_value(recommendations.get("exclude_insufficient_data")),
        "top_features": top_features,
        "pit_safety_status": "PASS" if pit_violations == 0 and missing_available == 0 else "WATCH",
        "production_effect": production_effect,
        "read_only": True,
        "risk": "；".join(risks) or "SEC PIT evaluation dashboard card 当前仅作只读展示。",
    }


def _sec_pit_baseline_comparison(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_sec_pit_baseline_comparison_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "outputs"
        / "sec_pit_baseline_comparison"
        / f"sec_pit_baseline_comparison_summary_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "sec_pit_baseline_comparison":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "markdown_path": str(default_markdown),
            "latest_comparison_date": "",
            "decision_count": 0,
            "action_changed_count": 0,
            "material_rank_shift_count": 0,
            "incremental_alpha_20d": None,
            "drawdown_improvement_20d": None,
            "top_promoted_tickers": [],
            "top_downgraded_tickers": [],
            "production_effect": ProductionEffect.NONE.value,
            "read_only": True,
            "risk": (
                "No SEC PIT baseline comparison summary available. Dashboard 只读取 "
                "TRADING-041 artifact，不运行 comparison、不读取 market data、"
                "不修改 production 权重。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    markdown_path = (
        _project_path(report.project_root, _string_value(outputs.get("summary_markdown")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    safety = _mapping_value(payload, "safety")
    production_effect = (
        _string_value(safety.get("production_effect"))
        or _string_value(payload.get("production_effect"))
        or ProductionEffect.NONE.value
    )
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-041 comparison production_effect 必须为 none。")
    if safety.get("manual_review_required") is not True:
        risks.append("TRADING-041 comparison 必须要求人工复核。")
    if safety.get("production_weights_modified") is True:
        risks.append("TRADING-041 comparison 不得修改 production weights。")
    if safety.get("production_actions_modified") is True:
        risks.append("TRADING-041 comparison 不得修改 production actions。")
    status = _string_value(payload.get("comparison_status")) or "UNKNOWN"
    if status.startswith("LIMITED") or status == "INSUFFICIENT_OVERLAP":
        risks.append(f"Comparison status is {status}; conclusions are limited.")
    return {
        "status": status,
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "markdown_path": str(markdown_path),
        "latest_comparison_date": _string_value(payload.get("end_date")),
        "decision_count": _int_value(payload.get("decision_count")),
        "action_changed_count": _int_value(payload.get("action_changed_count")),
        "material_rank_shift_count": _int_value(payload.get("material_rank_shift_count")),
        "incremental_alpha_20d": _optional_float(payload.get("incremental_alpha_20d")),
        "drawdown_improvement_20d": _optional_float(payload.get("drawdown_improvement_20d")),
        "top_promoted_tickers": _dashboard_ticker_delta_list(
            payload.get("top_promoted_tickers"),
        ),
        "top_downgraded_tickers": _dashboard_ticker_delta_list(
            payload.get("top_downgraded_tickers"),
        ),
        "production_effect": production_effect,
        "read_only": True,
        "risk": "；".join(risks) or "SEC PIT baseline comparison dashboard card 当前仅作只读展示。",
    }


def _dashboard_ticker_delta_list(value: object) -> list[TraceRecord]:
    if not isinstance(value, list):
        return []
    records: list[TraceRecord] = []
    for item in value[:5]:
        if not isinstance(item, dict):
            continue
        records.append(
            {
                "ticker": _string_value(item.get("ticker")),
                "rank_delta": _int_value(item.get("rank_delta")),
                "score_delta": _optional_float(item.get("score_delta")),
            }
        )
    return records


def _sec_pit_top_features(value: object) -> list[TraceRecord]:
    if not isinstance(value, list):
        return []
    records: list[TraceRecord] = []
    for item in value[:5]:
        if isinstance(item, dict):
            records.append(
                {
                    "feature_id": _string_value(item.get("feature_id")),
                    "metric_id": _string_value(item.get("metric_id")),
                    "rank_ic_20d": item.get("rank_ic_20d"),
                    "recommendation": _string_value(item.get("recommendation")),
                }
            )
    return records


def _notification_delivery_failure_classification(
    report: DailyTaskDashboardReport,
) -> TraceRecord:
    path = _latest_notification_delivery_failure_classification_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "outputs"
        / "notification_delivery_failure_classification"
        / f"notification_delivery_failure_classification_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "notification_delivery_failure_classification":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "markdown_path": str(default_markdown),
            "overall_status": "MISSING",
            "highest_severity": "UNKNOWN",
            "total_failures": 0,
            "requires_manual_review": False,
            "safe_to_retry": False,
            "blocks_notification_chain": False,
            "source_audit_status": "MISSING",
            "generated_at": "",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "read_only": True,
            "notification_delivery_failure_classification_only": True,
            "risk": (
                "No notification delivery failure classification report available. "
                "Dashboard 只读取 TRADING-036 JSON，不运行 classifier、不读取外部通知状态、"
                "不发送 notification、不触发 retry。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    markdown_output = _mapping_value(outputs, "classification_markdown")
    markdown_path = (
        _project_path(report.project_root, _string_value(markdown_output.get("path")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    metadata = _mapping_value(payload, "metadata")
    source = _mapping_value(payload, "source_audit")
    summary = _mapping_value(payload, "classification_summary")
    safety = _mapping_value(payload, "safety_invariants")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-036 classification production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-036 classification 必须 manual_review_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-036 classification 必须 read_only=true。")
    if payload.get("notification_delivery_failure_classification_only") is not True:
        risks.append("TRADING-036 必须 classification-only。")
    if safety.get("no_external_delivery") is not True:
        risks.append("TRADING-036 safety_invariants.no_external_delivery 必须为 true。")
    if safety.get("no_state_mutation") is not True:
        risks.append("TRADING-036 safety_invariants.no_state_mutation 必须为 true。")
    if safety.get("no_production_parameter_change") is not True:
        risks.append("TRADING-036 不允许修改 production 参数。")
    if payload.get("email_sent") is True:
        risks.append("TRADING-036 不允许发送 email。")
    if payload.get("gmail_draft_created") is True:
        risks.append("TRADING-036 不允许创建 Gmail draft。")
    if payload.get("gmail_draft_modified") is True:
        risks.append("TRADING-036 不允许修改 Gmail draft。")
    if payload.get("slack_sent") is True:
        risks.append("TRADING-036 不允许发送 Slack 通知。")
    if payload.get("discord_sent") is True:
        risks.append("TRADING-036 不允许发送 Discord 通知。")
    if payload.get("webhook_called") is True:
        risks.append("TRADING-036 不允许调用 webhook。")
    if payload.get("mobile_push_sent") is True:
        risks.append("TRADING-036 不允许发送 mobile push。")
    if payload.get("retry_executed") is True:
        risks.append("TRADING-036 不允许自动 retry。")
    if payload.get("delivery_state_mutated") is True:
        risks.append("TRADING-036 不允许修改 delivery state。")
    if payload.get("notification_delivery_audit_executed_by_classifier") is True:
        risks.append("TRADING-036 不允许运行 TRADING-035 audit。")
    if payload.get("notification_draft_executed_by_classifier") is True:
        risks.append("TRADING-036 不允许运行 notification draft generator。")
    if payload.get("delivery_preflight_executed_by_classifier") is True:
        risks.append("TRADING-036 不允许运行 delivery preflight。")
    if payload.get("draft_dispatch_executed_by_classifier") is True:
        risks.append("TRADING-036 不允许运行 draft dispatch。")
    if payload.get("operator_brief_executed_by_classifier") is True:
        risks.append("TRADING-036 不允许运行 operator brief。")
    if payload.get("pipelines_executed_by_classifier") is True:
        risks.append("TRADING-036 不允许运行上游 pipeline。")
    if payload.get("data_downloaded_by_classifier") is True:
        risks.append("TRADING-036 不允许下载或刷新数据。")
    if payload.get("broker_execution") is True:
        risks.append("TRADING-036 不允许 broker_execution=true。")
    if payload.get("replay_execution") is True:
        risks.append("TRADING-036 不允许 replay_execution=true。")
    if payload.get("trading_execution") is True:
        risks.append("TRADING-036 不允许 trading_execution=true。")
    return {
        "status": _string_value(summary.get("overall_status")) or "UNKNOWN",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "markdown_path": str(markdown_path),
        "overall_status": _string_value(summary.get("overall_status")) or "UNKNOWN",
        "highest_severity": _string_value(summary.get("highest_severity")) or "UNKNOWN",
        "total_failures": _int_value(summary.get("total_failures")),
        "requires_manual_review": summary.get("requires_manual_review") is True,
        "safe_to_retry": summary.get("safe_to_retry") is True,
        "blocks_notification_chain": summary.get("blocks_notification_chain") is True,
        "source_audit_status": _string_value(source.get("audit_status")) or "UNKNOWN",
        "source_parse_status": _string_value(source.get("source_parse_status")) or "UNKNOWN",
        "generated_at": _string_value(metadata.get("generated_at")),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "read_only": payload.get("read_only") is True,
        "notification_delivery_failure_classification_only": (
            payload.get("notification_delivery_failure_classification_only") is True
        ),
        "risk": "；".join(risks)
        or ("Notification Delivery Failure Classification 当前仅作只读展示。"),
    }


def _retry_candidate_queue(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_retry_candidate_queue_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "outputs"
        / "retry_candidate_queue"
        / f"retry_candidate_queue_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "retry_candidate_queue":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "markdown_path": str(default_markdown),
            "queue_status": "MISSING",
            "total_candidates": 0,
            "blocked_candidates": 0,
            "manual_review_required": False,
            "has_retryable_candidates": False,
            "safe_to_execute_retry": False,
            "approval_required": True,
            "approval_status": "MISSING",
            "retry_execution_allowed": False,
            "source_classification_status": "MISSING",
            "source_parse_status": "MISSING",
            "generated_at": "",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "retry_candidate_queue_only": True,
            "read_only": True,
            "risk": (
                "No retry candidate queue report available. Dashboard 只读取 "
                "TRADING-037 JSON，不运行 queue generator、不执行 retry、不发送 "
                "notification、不修改 approval state。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    markdown_output = _mapping_value(outputs, "retry_candidate_queue_markdown")
    markdown_path = (
        _project_path(report.project_root, _string_value(markdown_output.get("path")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    metadata = _mapping_value(payload, "metadata")
    source = _mapping_value(payload, "source_classification")
    summary = _mapping_value(payload, "queue_summary")
    approval = _mapping_value(payload, "approval_gate")
    safety = _mapping_value(payload, "safety_invariants")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-037 queue production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-037 queue 必须 manual_review_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-037 queue 必须 read_only=true。")
    if payload.get("retry_candidate_queue_only") is not True:
        risks.append("TRADING-037 必须 retry_candidate_queue_only=true。")
    if safety.get("no_external_delivery") is not True:
        risks.append("TRADING-037 safety_invariants.no_external_delivery 必须为 true。")
    if safety.get("no_retry_execution") is not True:
        risks.append("TRADING-037 safety_invariants.no_retry_execution 必须为 true。")
    if safety.get("no_state_mutation") is not True:
        risks.append("TRADING-037 safety_invariants.no_state_mutation 必须为 true。")
    if safety.get("no_production_parameter_change") is not True:
        risks.append("TRADING-037 不允许修改 production 参数。")
    if approval.get("retry_execution_allowed") is True:
        risks.append("TRADING-037 approval gate 不允许 retry_execution_allowed=true。")
    if payload.get("email_sent") is True:
        risks.append("TRADING-037 不允许发送 email。")
    if payload.get("gmail_draft_created") is True:
        risks.append("TRADING-037 不允许创建 Gmail draft。")
    if payload.get("gmail_draft_modified") is True:
        risks.append("TRADING-037 不允许修改 Gmail draft。")
    if payload.get("slack_sent") is True:
        risks.append("TRADING-037 不允许发送 Slack 通知。")
    if payload.get("discord_sent") is True:
        risks.append("TRADING-037 不允许发送 Discord 通知。")
    if payload.get("webhook_called") is True:
        risks.append("TRADING-037 不允许调用 webhook。")
    if payload.get("mobile_push_sent") is True:
        risks.append("TRADING-037 不允许发送 mobile push。")
    if payload.get("retry_executed") is True:
        risks.append("TRADING-037 不允许执行 retry。")
    if payload.get("delivery_state_mutated") is True:
        risks.append("TRADING-037 不允许修改 delivery state。")
    if payload.get("approval_state_modified") is True:
        risks.append("TRADING-037 dashboard/queue 不允许修改 approval state。")
    if payload.get("notification_delivery_failure_classification_executed_by_queue") is True:
        risks.append("TRADING-037 不允许运行 TRADING-036 classifier。")
    if payload.get("notification_delivery_audit_executed_by_queue") is True:
        risks.append("TRADING-037 不允许运行 TRADING-035 audit。")
    if payload.get("notification_draft_executed_by_queue") is True:
        risks.append("TRADING-037 不允许运行 notification draft generator。")
    if payload.get("delivery_preflight_executed_by_queue") is True:
        risks.append("TRADING-037 不允许运行 delivery preflight。")
    if payload.get("draft_dispatch_executed_by_queue") is True:
        risks.append("TRADING-037 不允许运行 draft dispatch。")
    if payload.get("operator_brief_executed_by_queue") is True:
        risks.append("TRADING-037 不允许运行 operator brief。")
    if payload.get("pipelines_executed_by_queue") is True:
        risks.append("TRADING-037 不允许运行上游 pipeline。")
    if payload.get("data_downloaded_by_queue") is True:
        risks.append("TRADING-037 不允许下载或刷新数据。")
    if payload.get("broker_execution") is True:
        risks.append("TRADING-037 不允许 broker_execution=true。")
    if payload.get("replay_execution") is True:
        risks.append("TRADING-037 不允许 replay_execution=true。")
    if payload.get("trading_execution") is True:
        risks.append("TRADING-037 不允许 trading_execution=true。")
    return {
        "status": _string_value(summary.get("queue_status")) or "UNKNOWN",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "markdown_path": str(markdown_path),
        "queue_status": _string_value(summary.get("queue_status")) or "UNKNOWN",
        "total_candidates": _int_value(summary.get("total_candidates")),
        "blocked_candidates": _int_value(summary.get("blocked_candidates")),
        "manual_review_required": summary.get("manual_review_required") is True,
        "has_retryable_candidates": summary.get("has_retryable_candidates") is True,
        "safe_to_execute_retry": summary.get("safe_to_execute_retry") is True,
        "approval_required": approval.get("approval_required") is True,
        "approval_status": _string_value(approval.get("approval_status")) or "UNKNOWN",
        "retry_execution_allowed": approval.get("retry_execution_allowed") is True,
        "source_classification_status": (_string_value(source.get("overall_status")) or "UNKNOWN"),
        "source_parse_status": _string_value(source.get("source_parse_status")) or "UNKNOWN",
        "generated_at": _string_value(metadata.get("generated_at")),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "retry_candidate_queue_only": payload.get("retry_candidate_queue_only") is True,
        "read_only": payload.get("read_only") is True,
        "risk": "；".join(risks) or ("Retry Candidate Queue 当前仅作只读展示。"),
    }


def _retry_execution_dry_run(report: DailyTaskDashboardReport) -> TraceRecord:
    path = _latest_retry_execution_dry_run_path(report)
    payload = _read_json_object(path)
    default_markdown = (
        report.project_root
        / "outputs"
        / "retry_execution_dry_run"
        / f"retry_execution_dry_run_{report.as_of.isoformat()}.md"
    )
    if payload.get("report_type") != "retry_execution_dry_run":
        return {
            "status": "MISSING",
            "exists": False,
            "path": str(path),
            "href": _report_href(path, report.reports_dir),
            "report_href": "",
            "markdown_path": str(default_markdown),
            "dry_run_status": "MISSING",
            "total_candidates": 0,
            "approved_for_dry_run": 0,
            "blocked_from_dry_run": 0,
            "simulated_retry_actions": 0,
            "real_retry_allowed": False,
            "external_delivery_allowed": False,
            "production_state_mutation_allowed": False,
            "source_queue_status": "MISSING",
            "source_parse_status": "MISSING",
            "approval_record_available": False,
            "approval_parse_status": "MISSING",
            "approved_candidate_count": 0,
            "rejected_candidate_count": 0,
            "unapproved_candidate_count": 0,
            "generated_at": "",
            "production_effect": ProductionEffect.NONE.value,
            "manual_review_only": True,
            "retry_execution_dry_run_only": True,
            "dry_run_only": True,
            "read_only": True,
            "approval_record_modified": False,
            "retry_executed": False,
            "actual_retry_executed": False,
            "external_delivery_executed": False,
            "delivery_state_mutated": False,
            "state_mutation_executed": False,
            "risk": (
                "No retry execution dry-run report available. Dashboard 只读取 "
                "TRADING-038 JSON，不运行 dry-run generator、不执行 retry、不发送 "
                "notification、不修改 approval record 或 delivery state。"
            ),
        }

    outputs = _mapping_value(payload, "output_artifacts")
    markdown_output = _mapping_value(outputs, "retry_execution_dry_run_markdown")
    markdown_path = (
        _project_path(report.project_root, _string_value(markdown_output.get("path")))
        or default_markdown
    )
    report_href = _report_href(markdown_path, report.reports_dir) if markdown_path.exists() else ""
    metadata = _mapping_value(payload, "metadata")
    source = _mapping_value(payload, "source_queue")
    approval = _mapping_value(payload, "approval_record")
    summary = _mapping_value(payload, "dry_run_summary")
    safety = _mapping_value(payload, "safety_invariants")
    production_effect = (
        _string_value(payload.get("production_effect")) or ProductionEffect.NONE.value
    )
    risks: list[str] = []
    if production_effect != ProductionEffect.NONE.value:
        risks.append("TRADING-038 dry-run production_effect 必须为 none。")
    if payload.get("manual_review_only") is not True:
        risks.append("TRADING-038 dry-run 必须 manual_review_only=true。")
    if payload.get("dry_run_only") is not True:
        risks.append("TRADING-038 必须 dry_run_only=true。")
    if payload.get("read_only") is not True:
        risks.append("TRADING-038 必须 read_only=true。")
    if payload.get("retry_execution_dry_run_only") is not True:
        risks.append("TRADING-038 必须 retry_execution_dry_run_only=true。")
    if safety.get("dry_run_only") is not True:
        risks.append("TRADING-038 safety_invariants.dry_run_only 必须为 true。")
    if safety.get("no_external_delivery") is not True:
        risks.append("TRADING-038 safety_invariants.no_external_delivery 必须为 true。")
    if safety.get("no_retry_execution") is not True:
        risks.append("TRADING-038 safety_invariants.no_retry_execution 必须为 true。")
    if safety.get("no_state_mutation") is not True:
        risks.append("TRADING-038 safety_invariants.no_state_mutation 必须为 true。")
    if safety.get("no_production_parameter_change") is not True:
        risks.append("TRADING-038 不允许修改 production 参数。")
    if safety.get("approval_record_is_input_only") is not True:
        risks.append("TRADING-038 approval record 必须保持 input-only。")
    if summary.get("real_retry_allowed") is True:
        risks.append("TRADING-038 dry-run 不允许 real_retry_allowed=true。")
    if summary.get("external_delivery_allowed") is True:
        risks.append("TRADING-038 dry-run 不允许 external_delivery_allowed=true。")
    if summary.get("production_state_mutation_allowed") is True:
        risks.append("TRADING-038 dry-run 不允许 production state mutation。")
    if payload.get("approval_record_modified") is True:
        risks.append("TRADING-038 不允许修改 approval record。")
    if payload.get("approval_state_modified") is True:
        risks.append("TRADING-038 不允许修改 approval state。")
    if payload.get("email_sent") is True:
        risks.append("TRADING-038 不允许发送 email。")
    if payload.get("gmail_draft_created") is True:
        risks.append("TRADING-038 不允许创建 Gmail draft。")
    if payload.get("gmail_draft_modified") is True:
        risks.append("TRADING-038 不允许修改 Gmail draft。")
    if payload.get("slack_sent") is True:
        risks.append("TRADING-038 不允许发送 Slack 通知。")
    if payload.get("discord_sent") is True:
        risks.append("TRADING-038 不允许发送 Discord 通知。")
    if payload.get("webhook_called") is True:
        risks.append("TRADING-038 不允许调用 webhook。")
    if payload.get("mobile_push_sent") is True:
        risks.append("TRADING-038 不允许发送 mobile push。")
    if payload.get("retry_executed") is True or payload.get("actual_retry_executed") is True:
        risks.append("TRADING-038 不允许执行 retry。")
    if payload.get("external_delivery_executed") is True:
        risks.append("TRADING-038 不允许执行外部 delivery。")
    if (
        payload.get("delivery_state_mutated") is True
        or payload.get("state_mutation_executed") is True
    ):
        risks.append("TRADING-038 不允许修改 delivery state。")
    if payload.get("retry_candidate_queue_executed_by_dry_run") is True:
        risks.append("TRADING-038 不允许运行 TRADING-037 queue generator。")
    if payload.get("notification_delivery_failure_classification_executed_by_dry_run") is True:
        risks.append("TRADING-038 不允许运行 TRADING-036 classifier。")
    if payload.get("notification_delivery_audit_executed_by_dry_run") is True:
        risks.append("TRADING-038 不允许运行 TRADING-035 audit。")
    if payload.get("notification_draft_executed_by_dry_run") is True:
        risks.append("TRADING-038 不允许运行 notification draft generator。")
    if payload.get("delivery_preflight_executed_by_dry_run") is True:
        risks.append("TRADING-038 不允许运行 delivery preflight。")
    if payload.get("draft_dispatch_executed_by_dry_run") is True:
        risks.append("TRADING-038 不允许运行 draft dispatch。")
    if payload.get("operator_brief_executed_by_dry_run") is True:
        risks.append("TRADING-038 不允许运行 operator brief。")
    if payload.get("pipelines_executed_by_dry_run") is True:
        risks.append("TRADING-038 不允许运行上游 pipeline。")
    if payload.get("data_downloaded_by_dry_run") is True:
        risks.append("TRADING-038 不允许下载或刷新数据。")
    if payload.get("broker_execution") is True:
        risks.append("TRADING-038 不允许 broker_execution=true。")
    if payload.get("replay_execution") is True:
        risks.append("TRADING-038 不允许 replay_execution=true。")
    if payload.get("trading_execution") is True:
        risks.append("TRADING-038 不允许 trading_execution=true。")
    for action in _records(payload.get("simulated_retry_actions")):
        if action.get("actual_retry_executed") is not False:
            risks.append("TRADING-038 simulated action 必须 actual_retry_executed=false。")
        if action.get("external_delivery_executed") is not False:
            risks.append("TRADING-038 simulated action 必须 external_delivery_executed=false。")
        if action.get("state_mutation_executed") is not False:
            risks.append("TRADING-038 simulated action 必须 state_mutation_executed=false。")
    return {
        "status": _string_value(summary.get("dry_run_status")) or "UNKNOWN",
        "exists": True,
        "path": str(path),
        "href": _report_href(path, report.reports_dir),
        "report_href": report_href or _report_href(path, report.reports_dir),
        "markdown_path": str(markdown_path),
        "dry_run_status": _string_value(summary.get("dry_run_status")) or "UNKNOWN",
        "total_candidates": _int_value(summary.get("total_candidates")),
        "approved_for_dry_run": _int_value(summary.get("approved_for_dry_run")),
        "blocked_from_dry_run": _int_value(summary.get("blocked_from_dry_run")),
        "simulated_retry_actions": _int_value(summary.get("simulated_retry_actions")),
        "real_retry_allowed": summary.get("real_retry_allowed") is True,
        "external_delivery_allowed": summary.get("external_delivery_allowed") is True,
        "production_state_mutation_allowed": (
            summary.get("production_state_mutation_allowed") is True
        ),
        "source_queue_status": _string_value(source.get("queue_status")) or "UNKNOWN",
        "source_parse_status": _string_value(source.get("source_parse_status")) or "UNKNOWN",
        "approval_record_available": approval.get("approval_record_available") is True,
        "approval_parse_status": _string_value(approval.get("approval_parse_status")) or "UNKNOWN",
        "approved_candidate_count": _int_value(approval.get("approved_candidate_count")),
        "rejected_candidate_count": _int_value(approval.get("rejected_candidate_count")),
        "unapproved_candidate_count": _int_value(approval.get("unapproved_candidate_count")),
        "generated_at": _string_value(metadata.get("generated_at")),
        "production_effect": production_effect,
        "manual_review_only": payload.get("manual_review_only") is True,
        "retry_execution_dry_run_only": payload.get("retry_execution_dry_run_only") is True,
        "dry_run_only": payload.get("dry_run_only") is True,
        "read_only": payload.get("read_only") is True,
        "approval_record_modified": payload.get("approval_record_modified") is True,
        "retry_executed": payload.get("retry_executed") is True,
        "actual_retry_executed": payload.get("actual_retry_executed") is True,
        "external_delivery_executed": payload.get("external_delivery_executed") is True,
        "delivery_state_mutated": payload.get("delivery_state_mutated") is True,
        "state_mutation_executed": payload.get("state_mutation_executed") is True,
        "risk": "；".join(risks) or ("Retry Execution Dry Run 当前仅作只读展示。"),
    }


def _latest_shadow_vs_production_review_path(report: DailyTaskDashboardReport) -> Path:
    review_root = (
        report.project_root / "data" / "derived" / "weight_iterations" / "comparison" / "reviews"
    )
    default_path = review_root / f"shadow_vs_production_review_{report.as_of.isoformat()}.json"
    if not review_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in review_root.glob("shadow_vs_production_review_*.json"):
        raw_date = path.stem.removeprefix("shadow_vs_production_review_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_shadow_promotion_proposal_path(report: DailyTaskDashboardReport) -> Path:
    proposal_root = (
        report.project_root / "data" / "derived" / "weight_iterations" / "promotion" / "proposals"
    )
    default_path = proposal_root / f"shadow_promotion_proposal_{report.as_of.isoformat()}.json"
    if not proposal_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in proposal_root.glob("shadow_promotion_proposal_*.json"):
        raw_date = path.stem.removeprefix("shadow_promotion_proposal_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_shadow_promotion_apply_preflight_path(report: DailyTaskDashboardReport) -> Path:
    preflight_root = (
        report.project_root / "data" / "derived" / "weight_iterations" / "promotion" / "preflight"
    )
    default_path = preflight_root / (
        f"shadow_promotion_apply_preflight_{report.as_of.isoformat()}.json"
    )
    if not preflight_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in preflight_root.glob("shadow_promotion_apply_preflight_*.json"):
        raw_date = path.stem.removeprefix("shadow_promotion_apply_preflight_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_shadow_promotion_apply_path(report: DailyTaskDashboardReport) -> Path:
    apply_root = (
        report.project_root / "data" / "derived" / "weight_iterations" / "promotion" / "apply"
    )
    default_path = apply_root / f"shadow_promotion_apply_result_{report.as_of.isoformat()}.json"
    if not apply_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in apply_root.glob("shadow_promotion_apply_result_*.json"):
        raw_date = path.stem.removeprefix("shadow_promotion_apply_result_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_shadow_promotion_rollback_path(report: DailyTaskDashboardReport) -> Path:
    rollback_root = (
        report.project_root
        / "data"
        / "derived"
        / "weight_iterations"
        / "promotion"
        / "rollback_results"
    )
    default_path = rollback_root / (
        f"shadow_promotion_rollback_result_{report.as_of.isoformat()}.json"
    )
    if not rollback_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in rollback_root.glob("shadow_promotion_rollback_result_*.json"):
        raw_date = path.stem.removeprefix("shadow_promotion_rollback_result_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_shadow_promotion_lifecycle_audit_path(report: DailyTaskDashboardReport) -> Path:
    audit_root = (
        report.project_root / "data" / "derived" / "weight_iterations" / "promotion" / "audit"
    )
    default_path = audit_root / (
        f"shadow_promotion_lifecycle_audit_{report.as_of.isoformat()}.json"
    )
    if not audit_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in audit_root.glob("shadow_promotion_lifecycle_audit_*.json"):
        raw_date = path.stem.removeprefix("shadow_promotion_lifecycle_audit_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_parameter_governance_summary_path(report: DailyTaskDashboardReport) -> Path:
    governance_root = report.project_root / "data" / "derived" / "weight_iterations" / "governance"
    default_path = governance_root / (
        f"parameter_governance_summary_{report.as_of.isoformat()}.json"
    )
    if not governance_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in governance_root.glob("parameter_governance_summary_*.json"):
        raw_date = path.stem.removeprefix("parameter_governance_summary_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_parameter_governance_web_view_metadata_path(report: DailyTaskDashboardReport) -> Path:
    web_root = report.project_root / "data" / "derived" / "weight_iterations" / "governance" / "web"
    default_path = web_root / f"parameter_governance_web_view_{report.as_of.isoformat()}.json"
    if not web_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in web_root.glob("parameter_governance_web_view_*.json"):
        raw_date = path.stem.removeprefix("parameter_governance_web_view_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_parameter_governance_daily_digest_path(report: DailyTaskDashboardReport) -> Path:
    digest_root = (
        report.project_root / "data" / "derived" / "weight_iterations" / "governance" / "digests"
    )
    default_path = digest_root / (
        f"parameter_governance_daily_digest_{report.as_of.isoformat()}.json"
    )
    if not digest_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in digest_root.glob("parameter_governance_daily_digest_*.json"):
        raw_date = path.stem.removeprefix("parameter_governance_daily_digest_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_pipeline_health_summary_path(report: DailyTaskDashboardReport) -> Path:
    health_root = report.project_root / "data" / "derived" / "pipeline_health"
    default_path = health_root / f"pipeline_health_summary_{report.as_of.isoformat()}.json"
    if not health_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in health_root.glob("pipeline_health_summary_*.json"):
        raw_date = path.stem.removeprefix("pipeline_health_summary_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_data_freshness_summary_path(report: DailyTaskDashboardReport) -> Path:
    freshness_root = report.project_root / "data" / "derived" / "data_freshness"
    default_path = freshness_root / f"data_freshness_summary_{report.as_of.isoformat()}.json"
    if not freshness_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in freshness_root.glob("data_freshness_summary_*.json"):
        raw_date = path.stem.removeprefix("data_freshness_summary_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_daily_trading_system_operator_brief_path(report: DailyTaskDashboardReport) -> Path:
    brief_root = report.project_root / "data" / "derived" / "operator_briefs"
    default_path = brief_root / (
        f"daily_trading_system_operator_brief_{report.as_of.isoformat()}.json"
    )
    if not brief_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in brief_root.glob("daily_trading_system_operator_brief_*.json"):
        raw_date = path.stem.removeprefix("daily_trading_system_operator_brief_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_daily_operator_brief_scheduler_dry_run_path(
    report: DailyTaskDashboardReport,
) -> Path:
    dry_run_root = (
        report.project_root / "data" / "derived" / "operator_briefs" / "scheduler_dry_run"
    )
    default_path = dry_run_root / (
        f"daily_operator_brief_scheduler_dry_run_{report.as_of.isoformat()}.json"
    )
    if not dry_run_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in dry_run_root.glob("daily_operator_brief_scheduler_dry_run_*.json"):
        raw_date = path.stem.removeprefix("daily_operator_brief_scheduler_dry_run_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_daily_operator_brief_scheduler_templates_path(
    report: DailyTaskDashboardReport,
) -> Path:
    template_root = (
        report.project_root / "data" / "derived" / "operator_briefs" / "scheduler_templates"
    )
    default_path = template_root / (
        f"daily_operator_brief_scheduler_templates_{report.as_of.isoformat()}.json"
    )
    if not template_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in template_root.glob("daily_operator_brief_scheduler_templates_*.json"):
        raw_date = path.stem.removeprefix("daily_operator_brief_scheduler_templates_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_daily_operator_brief_scheduler_template_validation_path(
    report: DailyTaskDashboardReport,
) -> Path:
    validation_root = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "scheduler_template_validation"
    )
    default_path = validation_root / (
        f"daily_operator_brief_scheduler_template_validation_{report.as_of.isoformat()}.json"
    )
    if not validation_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in validation_root.glob("daily_operator_brief_scheduler_template_validation_*.json"):
        raw_date = path.stem.removeprefix("daily_operator_brief_scheduler_template_validation_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_operator_brief_notification_draft_path(report: DailyTaskDashboardReport) -> Path:
    notification_root = (
        report.project_root / "data" / "derived" / "operator_briefs" / "notifications"
    )
    default_path = notification_root / (
        f"operator_brief_notification_draft_{report.as_of.isoformat()}.json"
    )
    if not notification_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in notification_root.glob("operator_brief_notification_draft_*.json"):
        raw_date = path.stem.removeprefix("operator_brief_notification_draft_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_operator_brief_notification_delivery_preflight_path(
    report: DailyTaskDashboardReport,
) -> Path:
    preflight_root = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "delivery_preflight"
    )
    default_path = preflight_root / (
        f"operator_brief_notification_delivery_preflight_{report.as_of.isoformat()}.json"
    )
    if not preflight_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in preflight_root.glob("operator_brief_notification_delivery_preflight_*.json"):
        raw_date = path.stem.removeprefix("operator_brief_notification_delivery_preflight_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_operator_brief_notification_dispatch_preview_path(
    report: DailyTaskDashboardReport,
) -> Path:
    preview_root = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "dispatch_preview"
    )
    default_path = preview_root / (
        f"operator_brief_notification_dispatch_preview_{report.as_of.isoformat()}.json"
    )
    if not preview_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in preview_root.glob("operator_brief_notification_dispatch_preview_*.json"):
        raw_date = path.stem.removeprefix("operator_brief_notification_dispatch_preview_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if candidates:
        return max(candidates, key=lambda item: item[0])[1]
    latest = preview_root / "latest.json"
    return latest if latest.exists() else default_path


def _latest_operator_brief_notification_approval_gate_path(
    report: DailyTaskDashboardReport,
) -> Path:
    approval_root = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "approval_gate"
    )
    default_path = approval_root / (
        f"operator_brief_notification_approval_gate_{report.as_of.isoformat()}.json"
    )
    latest = approval_root / "latest.json"
    if latest.exists():
        return latest
    return default_path


def _latest_operator_brief_notification_draft_dispatch_path(
    report: DailyTaskDashboardReport,
) -> Path:
    draft_dispatch_root = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "draft_dispatch"
    )
    return draft_dispatch_root / "latest.json"


def _latest_notification_delivery_audit_summary_path(
    report: DailyTaskDashboardReport,
) -> Path:
    audit_root = (
        report.project_root
        / "data"
        / "derived"
        / "operator_briefs"
        / "notifications"
        / "delivery_audit"
    )
    default_path = audit_root / (
        f"notification_delivery_audit_summary_{report.as_of.isoformat()}.json"
    )
    if not audit_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in audit_root.glob("notification_delivery_audit_summary_*.json"):
        raw_date = path.stem.removeprefix("notification_delivery_audit_summary_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_notification_delivery_failure_classification_path(
    report: DailyTaskDashboardReport,
) -> Path:
    classification_root = (
        report.project_root / "outputs" / "notification_delivery_failure_classification"
    )
    default_path = classification_root / (
        f"notification_delivery_failure_classification_{report.as_of.isoformat()}.json"
    )
    if not classification_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in classification_root.glob("notification_delivery_failure_classification_*.json"):
        raw_date = path.stem.removeprefix("notification_delivery_failure_classification_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_sec_pit_evaluation_summary_path(report: DailyTaskDashboardReport) -> Path:
    evaluation_root = report.project_root / "outputs" / "sec_pit_evaluation"
    default_path = evaluation_root / f"sec_pit_evaluation_summary_{report.as_of.isoformat()}.json"
    if not evaluation_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in evaluation_root.glob("sec_pit_evaluation_summary_*.json"):
        raw_date = path.stem.removeprefix("sec_pit_evaluation_summary_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_sec_pit_baseline_comparison_path(report: DailyTaskDashboardReport) -> Path:
    comparison_root = report.project_root / "outputs" / "sec_pit_baseline_comparison"
    default_path = (
        comparison_root / f"sec_pit_baseline_comparison_summary_{report.as_of.isoformat()}.json"
    )
    if not comparison_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in comparison_root.glob("sec_pit_baseline_comparison_summary_*.json"):
        raw_date = path.stem.removeprefix("sec_pit_baseline_comparison_summary_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_retry_candidate_queue_path(report: DailyTaskDashboardReport) -> Path:
    queue_root = report.project_root / "outputs" / "retry_candidate_queue"
    default_path = queue_root / f"retry_candidate_queue_{report.as_of.isoformat()}.json"
    if not queue_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in queue_root.glob("retry_candidate_queue_*.json"):
        raw_date = path.stem.removeprefix("retry_candidate_queue_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _latest_retry_execution_dry_run_path(report: DailyTaskDashboardReport) -> Path:
    dry_run_root = report.project_root / "outputs" / "retry_execution_dry_run"
    default_path = dry_run_root / f"retry_execution_dry_run_{report.as_of.isoformat()}.json"
    if not dry_run_root.exists():
        return default_path
    candidates: list[tuple[date, Path]] = []
    for path in dry_run_root.glob("retry_execution_dry_run_*.json"):
        raw_date = path.stem.removeprefix("retry_execution_dry_run_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and parsed <= report.as_of:
            candidates.append((parsed, path))
    if not candidates:
        return default_path
    return max(candidates, key=lambda item: item[0])[1]


def _paper_trading_snapshot_source_counts(payload: TraceRecord) -> Counter[str]:
    counts: Counter[str] = Counter()
    source_counts = _mapping_value(payload, "market_snapshot_source_counts")
    if source_counts:
        for source, count in source_counts.items():
            source_name = _string_value(source) or str(source)
            count_value = _optional_int(count) or 0
            if source_name and count_value > 0:
                counts[source_name] += count_value
        return counts

    source = _string_value(payload.get("market_snapshot_source"))
    if source and source != "none":
        counts[source] += _optional_int(payload.get("generated_intents")) or 1
    return counts


def _add_paper_candidate_explanations(
    path: Path,
    blocked_by_distribution: Counter[str],
    reason_code_distribution: Counter[str],
) -> bool:
    payload = _read_json_object(path)
    if payload.get("report_type") != "order_intent_candidates":
        return False
    for candidate in _records(payload.get("candidates")):
        for blocked_by in _strings(candidate.get("blocked_by")):
            blocked_by_distribution[blocked_by] += 1
        for reason_code in _strings(candidate.get("reason_codes")):
            reason_code_distribution[reason_code] += 1
    return True


def _top_counter_records(counter: Counter[str], *, limit: int = 5) -> list[TraceRecord]:
    return [
        {"value": value, "count": count}
        for value, count in counter.most_common(limit)
        if value and count > 0
    ]


def _normalize_paper_trading_trend_days(value: int) -> int:
    if value not in set(PAPER_TRADING_TREND_WINDOWS):
        raise ValueError("paper_trading_trend_days must be one of 7, 14, or 30")
    return value


def _latest_shadow_iteration_summary(report: DailyTaskDashboardReport) -> TraceRecord:
    path = report.reports_dir / f"shadow_iteration_{report.as_of.isoformat()}.json"
    payload = _read_json_object(path)
    if not payload:
        fallback = (
            report.project_root
            / "outputs"
            / "reports"
            / f"shadow_iteration_{report.as_of.isoformat()}.json"
        )
        if fallback != path:
            payload = _read_json_object(fallback)
    if payload.get("report_type") != "shadow_iteration":
        return {}
    summary = _mapping_value(payload, "summary")
    best = _mapping_value(payload, "best_candidates")
    blocked = _mapping_value(payload, "blocked_reasons")
    promotion = _mapping_value(payload, "promotion_contract_check")
    blocked_reasons = []
    for trial_id, reasons in blocked.items():
        if not isinstance(reasons, list):
            continue
        for reason in reasons:
            if isinstance(reason, str):
                blocked_reasons.append(f"{trial_id}: {reason}")
    return {
        "status": _string_value(payload.get("status")) or "PASS_WITH_LIMITATIONS",
        "active_candidate_count": summary.get("active_candidate_count"),
        "primary_driver": summary.get("primary_driver"),
        "next_action": summary.get("next_action"),
        "source_search_run_id": payload.get("source_search_run_id"),
        "promotion_status": promotion.get("status"),
        "best_weight_only": _mapping_value(best, "weight_only"),
        "best_gate_only": _mapping_value(best, "gate_only"),
        "best_weight_gate_bundle": _mapping_value(best, "weight_gate_bundle"),
        "blocked_reasons": blocked_reasons,
    }


def _shadow_iteration_candidate_label(candidate: TraceRecord) -> str:
    trial_id = _string_value(candidate.get("trial_id"))
    if not trial_id:
        return "unavailable"
    return _join_nonempty(
        [
            f"`{trial_id}`",
            _label("status", candidate.get("status")),
            _label("excess", _format_percent(_optional_float(candidate.get("excess_return")))),
            _label("next", candidate.get("next_action")),
        ],
        separator="，",
    )


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
            ("当前没有 eligible trial，诊断领先结果不得进入 production。" if not eligible else ""),
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
    excess_delta = _format_signed_percent(_optional_float(selected.get("excess_delta_vs_baseline")))
    return f"主要 cap：{gate_id}={selected_cap}，" f"cap-only excess {excess_delta}"


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
            "note": _join_nonempty(["AVAILABLE outcome 复利累计", sample_note, cost_note]),
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
            "delta": (
                "eligible" if _string_value(trial.get("eligible")).lower() == "true" else "未达准入"
            ),
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
                "note": "未变化" if _floats_equal(baseline_value, candidate_value) else "已变化",
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
                "delta": (
                    "新增 override"
                    if baseline_value is None
                    else _format_signed_percent_delta(baseline_value, candidate_value)
                ),
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
    risks = [f"{task.title}：{task.important_risk}" for task in tasks if task.risk_level != "none"]
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


def _int_value(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _string_list(value: object, *, limit: int) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    strings = tuple(item for item in value if isinstance(item, str))
    return strings[:limit]


def _strings(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


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
        " has-comparison" if conclusion.parameter_comparison or conclusion.result_comparison else ""
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
        row for row in parameter_rows if row.get("group") not in {"gate_cap", "weight"}
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
        f'<p class="result-methodology">{_text(methodology)}</p>' if methodology else ""
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
            ("<h5>权重参数 " f"<span>{unchanged_count}/{len(rows)} 未变化</span></h5>"),
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
        detail = f"<small>{_text(delta)}</small>"
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
        f"<strong>实际 {_text(value_text)}</strong>"
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


def _render_paper_trading_summary(report: DailyTaskDashboardReport) -> str:
    summary = _paper_trading_summary(report)
    report_href = _string_value(summary.get("report_href"))
    summary_href = _string_value(summary.get("href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Trading daily report</span>'
        f"<small>{_text(summary.get('reconciliation_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing"><span>Trading daily report</span>'
        "<small>MISSING</small></span>"
    )
    summary_link = (
        '<a class="report-link" '
        f'href="{_text(summary_href)}"><span>Paper summary JSON</span>'
        f"<small>{_text(summary.get('status', 'MISSING'))}</small></a>"
        if summary.get("exists")
        else '<span class="report-link missing"><span>Paper summary JSON</span>'
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="paper-trading-summary-title">',
            '<div class="section-head">',
            '<h2 id="paper-trading-summary-title">Paper Trading Summary</h2>',
            ("<p>只读 paper 执行复盘；production_effect 必须保持 " "<code>none</code>。</p>"),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("candidate_count", summary.get("candidate_count", "missing")),
            _summary_item(
                "blocked_candidates",
                summary.get("blocked_candidates", "missing"),
            ),
            _summary_item("generated_intents", summary.get("generated_intents", "missing")),
            _summary_item("approved / rejected", _count_pair(summary, "approved", "rejected")),
            _summary_item("submitted", summary.get("submitted", "missing")),
            _summary_item(
                "filled / open / cancelled",
                _count_triplet(summary, "filled", "open", "cancelled"),
            ),
            _summary_item("realized_pnl", _format_money_value(summary.get("realized_pnl"))),
            _summary_item(
                "unrealized_pnl",
                _format_money_value(summary.get("unrealized_pnl")),
            ),
            _summary_item(
                "reconciliation_status",
                summary.get("reconciliation_status", "MISSING"),
            ),
            _summary_item("production_effect", summary.get("production_effect", "none")),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            (
                '<p class="subtle">audit_log_path: '
                f"<code>{_text(summary.get('audit_log_path', 'missing'))}</code></p>"
            ),
            '<div class="report-link-list">',
            summary_link,
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_paper_trading_trend(report: DailyTaskDashboardReport) -> str:
    trend = _paper_trading_trend(report)
    totals = _mapping_value(trend, "totals")
    windows = _mapping_value(trend, "windows")
    latest_replay = _mapping_value(trend, "latest_replay")
    continuous_summary = _mapping_value(trend, "continuous_portfolio_summary")
    latest_replay_is_continuous = (
        _string_value(latest_replay.get("replay_mode")) == "continuous_portfolio"
    )
    trend_intro = (
        "只读读取最近历史 summary；缺失显示 "
        "<code>LIMITED</code>，不运行 replay 或补造结论；最近已有 replay 为"
        " <code>continuous-portfolio</code> 时，额外展示 final portfolio summary。"
        if latest_replay_is_continuous
        else (
            "只读读取最近历史 summary；缺失显示 "
            "<code>LIMITED</code>，不运行 replay 或补造结论；当前汇总语义为"
            "逐日独立 paper-only 复盘，不是连续组合收益。"
        )
    )
    continuous_items = []
    if latest_replay_is_continuous:
        continuous_items = [
            _summary_item(
                "final_equity",
                _format_money_value(continuous_summary.get("final_equity")),
            ),
            _summary_item(
                "max_drawdown",
                _format_percent(_optional_float(continuous_summary.get("max_drawdown_pct"))),
            ),
            _summary_item(
                "exposure_peak",
                _format_money_value(continuous_summary.get("exposure_peak")),
            ),
            _summary_item(
                "final_positions",
                continuous_summary.get("final_positions_count", 0),
            ),
            _summary_item(
                "expired DAY orders",
                continuous_summary.get("expired_day_orders", 0),
            ),
        ]
    window_rows = []
    for window_days in PAPER_TRADING_TREND_WINDOWS:
        window = _mapping_value(windows, str(window_days))
        window_totals = _mapping_value(window, "totals")
        reconciliation_text = _format_distribution(window.get("reconciliation_status_distribution"))
        synthetic_ratio_text = _format_percent(
            _optional_float(window.get("synthetic_snapshot_ratio"))
        )
        window_rows.append(
            "<tr>"
            f"<td>{window_days} 日</td>"
            f"<td>{_text(window.get('status', 'LIMITED'))}</td>"
            f"<td>{_text(window.get('replay_mode', 'daily_independent'))}</td>"
            f"<td>{_text(window.get('available_count', 0))} / "
            f"{_text(window.get('missing_count', 0))}</td>"
            f"<td>{_text(window_totals.get('candidate_count', 0))}</td>"
            f"<td>{_text(window_totals.get('generated_intents', 0))}</td>"
            f"<td>{_text(_count_triplet(window_totals, 'filled', 'open', 'cancelled'))}</td>"
            f"<td>{_text(_format_money_value(window_totals.get('realized_pnl')))} / "
            f"{_text(_format_money_value(window_totals.get('unrealized_pnl')))}</td>"
            f"<td>{_text(reconciliation_text)}</td>"
            f"<td>{_text(synthetic_ratio_text)}</td>"
            f"<td>{_text(_format_top_records(window.get('top_blocked_by')))}</td>"
            f"<td>{_text(_format_top_records(window.get('top_reason_code')))}</td>"
            "</tr>"
        )
    rows = []
    for record in _records(trend.get("daily_results")):
        if not record.get("exists"):
            rows.append(
                "<tr>"
                f"<td>{_text(record.get('as_of'))}</td>"
                "<td>MISSING</td>"
                '<td colspan="6">LIMITED：历史 summary 缺失；未补造结论。</td>'
                "</tr>"
            )
            continue
        rows.append(
            "<tr>"
            f"<td>{_text(record.get('as_of'))}</td>"
            f"<td>{_text(record.get('status'))}</td>"
            f"<td>{_text(record.get('candidate_count'))}</td>"
            f"<td>{_text(record.get('generated_intents'))}</td>"
            f"<td>{_text(record.get('submitted'))}</td>"
            f"<td>{_text(_count_triplet(record, 'filled', 'open', 'cancelled'))}</td>"
            f"<td>{_text(record.get('reconciliation_status'))}</td>"
            f"<td>{_text(record.get('market_snapshot_source'))}</td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<section aria-labelledby="paper-trading-trend-title">',
            '<div class="section-head">',
            '<h2 id="paper-trading-trend-title">Paper Trading Trend (7/14/30 日)</h2>',
            f"<p>{trend_intro}</p>",
            "</div>",
            '<div class="summary-grid">',
            _summary_item("Trend status", trend.get("status", "LIMITED")),
            _summary_item("replay_mode", trend.get("replay_mode", "daily_independent")),
            _summary_item(
                "latest replay",
                latest_replay.get("replay_mode", trend.get("replay_mode", "daily_independent")),
            ),
            _summary_item(
                "portfolio_carry_forward",
                str(trend.get("portfolio_carry_forward", False)),
            ),
            *continuous_items,
            _summary_item(
                "available / missing",
                f"{trend.get('available_count', 0)} / {trend.get('missing_count', 0)}",
            ),
            _summary_item("candidate_count", totals.get("candidate_count", 0)),
            _summary_item("generated_intents", totals.get("generated_intents", 0)),
            _summary_item("submitted", totals.get("submitted", 0)),
            _summary_item(
                "filled / open / cancelled",
                _count_triplet(totals, "filled", "open", "cancelled"),
            ),
            _summary_item("realized_pnl", _format_money_value(totals.get("realized_pnl"))),
            _summary_item(
                "unrealized_pnl",
                _format_money_value(totals.get("unrealized_pnl")),
            ),
            _summary_item(
                "synthetic snapshot",
                (
                    f"{trend.get('synthetic_snapshot_count', 0)} / "
                    f"{_format_percent(_optional_float(trend.get('synthetic_snapshot_ratio')))}"
                ),
            ),
            _summary_item(
                "top blocked_by",
                _format_top_records(trend.get("top_blocked_by")),
            ),
            _summary_item(
                "top reason_code",
                _format_top_records(trend.get("top_reason_code")),
            ),
            _summary_item(
                "production_effect",
                trend.get("production_effect", ProductionEffect.NONE.value),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(trend.get('risk', ''))}</p>"
            ),
            '<div class="table-wrap"><table>',
            "<thead><tr><th>Window</th><th>Status</th><th>Replay mode</th>"
            "<th>Available/Missing</th><th>Candidates</th><th>Intents</th>"
            "<th>Filled/Open/Cancelled</th><th>Realized/Unrealized PnL</th>"
            "<th>Reconciliation</th><th>Synthetic snapshot</th>"
            "<th>Top blocked_by</th><th>Top reason_code</th></tr></thead>",
            "<tbody>",
            *window_rows,
            "</tbody></table></div>",
            '<div class="table-wrap"><table>',
            "<thead><tr><th>Date</th><th>Status</th><th>Candidates</th>"
            "<th>Intents</th><th>Submitted</th><th>Filled/Open/Cancelled</th>"
            "<th>Reconciliation</th><th>Snapshot source</th></tr></thead>",
            "<tbody>",
            *rows,
            "</tbody></table></div>",
            "</section>",
        ]
    )


def _render_paper_signal_quality(report: DailyTaskDashboardReport) -> str:
    quality = _paper_signal_quality_summary(report)
    report_href = _string_value(quality.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Paper Signal Quality</span>'
        f"<small>{_text(quality.get('evaluation_status', 'INSUFFICIENT_DATA'))}</small></a>"
        if quality.get("exists")
        else '<span class="report-link missing"><span>Paper Signal Quality</span>'
        "<small>MISSING</small></span>"
    )
    synthetic_ratio = _optional_float(quality.get("synthetic_snapshot_ratio"))
    synthetic_display = "missing" if synthetic_ratio is None else _format_percent(synthetic_ratio)
    return "\n".join(
        [
            '<section aria-labelledby="paper-signal-quality-title">',
            '<div class="section-head">',
            '<h2 id="paper-signal-quality-title">Paper Signal Quality</h2>',
            (
                "<p>observe-only 质量评价；只读已有 paper artifacts，"
                "production_effect=<code>none</code>。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "evaluation_status",
                quality.get("evaluation_status", "INSUFFICIENT_DATA"),
            ),
            _summary_item("主要 blocked_by", quality.get("primary_blocked_by", "missing")),
            _summary_item("synthetic_snapshot_ratio", synthetic_display),
            _summary_item("sample_count", quality.get("sample_count", "missing")),
            _summary_item("observe-only", str(quality.get("observe_only", True))),
            _summary_item(
                "production_effect",
                quality.get("production_effect", ProductionEffect.NONE.value),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(quality.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_parameter_impact(report: DailyTaskDashboardReport) -> str:
    impact = _shadow_parameter_impact_summary(report)
    report_href = _string_value(impact.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow Impact</span>'
        f"<small>{_text(impact.get('impact_status', 'INSUFFICIENT_DATA'))}</small></a>"
        if impact.get("exists")
        else '<span class="report-link missing"><span>Shadow Impact</span>'
        "<small>MISSING</small></span>"
    )
    filled = _mapping_value(impact, "production_vs_shadow_filled_count")
    pnl = _mapping_value(impact, "production_vs_shadow_paper_pnl")
    warnings = _strings(impact.get("warnings"))
    return "\n".join(
        [
            '<section aria-labelledby="shadow-parameter-impact-title">',
            '<div class="section-head">',
            '<h2 id="shadow-parameter-impact-title">Shadow Impact</h2>',
            (
                "<p>observe-only impact 观察；只读已有 paper artifacts，"
                "不改变 production conclusion。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "impact_status",
                impact.get("impact_status", "INSUFFICIENT_DATA"),
            ),
            _summary_item("main blocked_by", impact.get("main_blocked_by", "missing")),
            _summary_item("warnings", "；".join(warnings) if warnings else "none"),
            _summary_item("7/14/30d samples", _shadow_impact_sample_text(impact)),
            _summary_item(
                "filled production/shadow",
                _count_pair(filled, "production", "shadow"),
            ),
            _summary_item(
                "paper PnL production/shadow",
                (
                    f"{_format_money_value(pnl.get('production'))} / "
                    f"{_format_money_value(pnl.get('shadow'))}"
                ),
            ),
            _summary_item(
                "continuous replay",
                (
                    f"{impact.get('continuous_replay_available', False)} / "
                    f"{impact.get('continuous_replay_mode', 'missing')}"
                ),
            ),
            _summary_item(
                "production_effect",
                impact.get("production_effect", ProductionEffect.NONE.value),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(impact.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_weight_adjustment_candidates(report: DailyTaskDashboardReport) -> str:
    candidates = _weight_adjustment_candidates_summary(report)
    report_href = _string_value(candidates.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Weight Adjustment Candidate</span>'
        f"<small>{_text(candidates.get('gate_status', 'LIMITED'))}</small></a>"
        if candidates.get("exists")
        else '<span class="report-link missing"><span>Weight Adjustment Candidate</span>'
        "<small>MISSING</small></span>"
    )
    blocked_by = _strings(candidates.get("blocked_by"))
    return "\n".join(
        [
            '<section aria-labelledby="weight-adjustment-candidates-title">',
            '<div class="section-head">',
            '<h2 id="weight-adjustment-candidates-title">Weight Adjustment Candidate</h2>',
            (
                "<p>observe-only 权重候选入口；dashboard 只读已有 JSON，"
                "不改变主结论、不应用参数。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("candidate_count", candidates.get("candidate_count", 0)),
            _summary_item("top_candidate_id", candidates.get("top_candidate_id", "")),
            _summary_item("gate_status", candidates.get("gate_status", "LIMITED")),
            _summary_item("main blocked_by", candidates.get("main_blocked_by", "missing")),
            _summary_item("blocked_by", "；".join(blocked_by) if blocked_by else "none"),
            _summary_item(
                "production_effect",
                candidates.get("production_effect", ProductionEffect.NONE.value),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(candidates.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_weight_candidate_evaluation(report: DailyTaskDashboardReport) -> str:
    evaluation = _weight_candidate_evaluation_summary(report)
    report_href = _string_value(evaluation.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Weight Candidate Evaluation</span>'
        f"<small>{_text(evaluation.get('evaluation_status', 'INSUFFICIENT_DATA'))}</small></a>"
        if evaluation.get("exists")
        else '<span class="report-link missing"><span>Weight Candidate Evaluation</span>'
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="weight-candidate-evaluation-title">',
            '<div class="section-head">',
            '<h2 id="weight-candidate-evaluation-title">Weight Candidate Evaluation</h2>',
            (
                "<p>observe-only 权重候选评估入口；dashboard 只读已有 JSON，"
                "不重跑评估、不应用参数、不影响主结论。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "evaluation_status",
                evaluation.get("evaluation_status", "INSUFFICIENT_DATA"),
            ),
            _summary_item("candidate_count", evaluation.get("candidate_count", 0)),
            _summary_item(
                "evaluable_candidate_count",
                evaluation.get("evaluable_candidate_count", 0),
            ),
            _summary_item("top_candidate_id", evaluation.get("top_candidate_id", "")),
            _summary_item("main blocked_by", evaluation.get("main_blocked_by", "missing")),
            _summary_item(
                "production_effect",
                evaluation.get("production_effect", ProductionEffect.NONE.value),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(evaluation.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_weight_promotion_gate(report: DailyTaskDashboardReport) -> str:
    gate = _weight_promotion_gate_summary(report)
    report_href = _string_value(gate.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Weight Promotion Gate</span>'
        f"<small>{_text(gate.get('gate_status', 'INSUFFICIENT_DATA'))}</small></a>"
        if gate.get("exists")
        else '<span class="report-link missing"><span>Weight Promotion Gate</span>'
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="weight-promotion-gate-title">',
            '<div class="section-head">',
            '<h2 id="weight-promotion-gate-title">Weight Promotion Gate</h2>',
            (
                "<p>manual-review-only 权重 promotion gate；dashboard 只读已有 JSON，"
                "不重跑 gate、不应用参数、不触发交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("gate_status", gate.get("gate_status", "INSUFFICIENT_DATA")),
            _summary_item("candidate_count", gate.get("candidate_count", 0)),
            _summary_item(
                "ready_for_manual_review_count",
                gate.get("ready_for_manual_review_count", 0),
            ),
            _summary_item("blocked_count", gate.get("blocked_count", 0)),
            _summary_item("top_candidate_id", gate.get("top_candidate_id", "")),
            _summary_item("main blocked_by", gate.get("main_blocked_by", "missing")),
            _summary_item(
                "production_effect",
                gate.get("production_effect", ProductionEffect.NONE.value),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(gate.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_daily_weight_adjustment_summary(report: DailyTaskDashboardReport) -> str:
    summary = _daily_weight_adjustment_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Daily Weight Adjustment Summary</span>'
        f"<small>{_text(summary.get('status', 'LIMITED'))}</small></a>"
        if summary.get("exists")
        else '<span class="report-link missing"><span>Daily Weight Adjustment Summary</span>'
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="daily-weight-adjustment-summary-title">',
            '<div class="section-head">',
            (
                '<h2 id="daily-weight-adjustment-summary-title">'
                "Daily Weight Adjustment Summary</h2>"
            ),
            (
                "<p>observe-only 每日权重调节汇总；dashboard 只读 summary JSON，"
                "不重跑候选、评估或 gate，不应用参数。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("candidate_count", summary.get("candidate_count", 0)),
            _summary_item(
                "evaluation_status",
                summary.get("evaluation_status", "INSUFFICIENT_DATA"),
            ),
            _summary_item(
                "promotion_gate_status",
                summary.get("promotion_gate_status", "INSUFFICIENT_DATA"),
            ),
            _summary_item(
                "ready_for_manual_review_count",
                summary.get("ready_for_manual_review_count", 0),
            ),
            _summary_item("main blocked_by", summary.get("main_blocked_by", "missing")),
            _summary_item(
                "production_effect",
                summary.get("production_effect", ProductionEffect.NONE.value),
            ),
            _summary_item("manual_review_only", summary.get("manual_review_only", True)),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_weight_iteration(report: DailyTaskDashboardReport) -> str:
    summary = _shadow_weight_iteration_summary(report)
    report_href = _string_value(summary.get("candidate_report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow Weight Iteration</span>'
        f"<small>{_text(summary.get('decision', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing"><span>Shadow Weight Iteration</span>'
        "<small>MISSING</small></span>"
    )
    weights = _mapping_value(summary, "current_weights")
    deltas = _mapping_value(summary, "latest_delta")
    rows = []
    for key in sorted(weights):
        delta_value = _format_signed_decimal_delta(0.0, _optional_float(deltas.get(key)), digits=4)
        rows.append(
            "<tr>"
            f'<td data-label="Weight"><code>{_text(key)}</code></td>'
            f'<td data-label="Current" class="value-cell">'
            f"{_text(_format_decimal(_optional_float(weights.get(key)), digits=4))}</td>"
            f'<td data-label="Latest delta" class="value-cell">'
            f"{_text(delta_value)}"
            "</td>"
            "</tr>"
        )
    weights_table = (
        '<div class="table-wrap"><table class="shadow-table">'
        "<thead><tr><th>Weight</th><th>Current</th><th>Latest delta</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></div>"
        if rows
        else '<p class="subtle">当前 shadow weights 尚未生成。</p>'
    )
    return "\n".join(
        [
            '<section aria-labelledby="shadow-weight-iteration-title">',
            '<div class="section-head">',
            '<h2 id="shadow-weight-iteration-title">Shadow Weight Iteration</h2>',
            (
                "<p>shadow-only 权重状态；dashboard 只读 current/candidate JSON，"
                "不重跑 pipeline、不应用 production 参数。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("latest decision", summary.get("decision", "MISSING")),
            _summary_item("last updated", summary.get("last_updated_date", "missing")),
            _summary_item(
                "production_effect",
                summary.get("production_effect", ProductionEffect.NONE.value),
            ),
            _summary_item("manual_review_only", summary.get("manual_review_only", True)),
            "</div>",
            weights_table,
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_vs_production_comparison(report: DailyTaskDashboardReport) -> str:
    summary = _shadow_vs_production_comparison_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow vs Production Comparison</span>'
        f"<small>{_text(summary.get('status', 'INSUFFICIENT_DATA'))}</small></a>"
        if report_href
        else '<span class="report-link missing"><span>Shadow vs Production Comparison</span>'
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="shadow-vs-production-title">',
            '<div class="section-head">',
            '<h2 id="shadow-vs-production-title">Shadow vs Production Comparison</h2>',
            (
                "<p>offline comparison 只读卡片；dashboard 只读 comparison JSON，"
                "不重跑评分、不接 broker/replay、不应用 shadow。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("status", summary.get("status", "INSUFFICIENT_DATA")),
            _summary_item("production decision", summary.get("production_decision", "MISSING")),
            _summary_item("shadow decision", summary.get("shadow_decision", "MISSING")),
            _summary_item("score_delta", summary.get("score_delta", "NA")),
            _summary_item("decision_changed", summary.get("decision_changed", "MISSING")),
            _summary_item(
                "production_effect",
                summary.get("production_effect", ProductionEffect.NONE.value),
            ),
            _summary_item("manual_review_only", summary.get("manual_review_only", True)),
            "</div>",
            (
                '<p class="risk-line"><strong>Main reason：</strong>'
                f"{_text(summary.get('main_reason', ''))}</p>"
            ),
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_vs_production_multi_day_review(report: DailyTaskDashboardReport) -> str:
    summary = _shadow_vs_production_multi_day_review_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow vs Production Multi-day Review</span>'
        f"<small>{_text(summary.get('review_decision', 'INSUFFICIENT_HISTORY'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Shadow vs Production Multi-day Review</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="shadow-vs-production-review-title">',
            '<div class="section-head">',
            (
                '<h2 id="shadow-vs-production-review-title">'
                "Shadow vs Production Multi-day Review</h2>"
            ),
            (
                "<p>只读多日观察卡片；dashboard 只读取 latest review artifact，"
                "不重跑 scoring/comparison/review pipeline。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("latest review_decision", summary.get("review_decision", "MISSING")),
            _summary_item("lookback_days", summary.get("lookback_days", 0)),
            _summary_item(
                "available_comparison_days",
                summary.get("available_comparison_days", 0),
            ),
            _summary_item("average_score_delta", summary.get("average_score_delta", "NA")),
            _summary_item(
                "decision_difference_count",
                summary.get("decision_difference_count", 0),
            ),
            _summary_item("promotion_readiness.ready", summary.get("promotion_ready", False)),
            _summary_item(
                "production_effect",
                summary.get("production_effect", ProductionEffect.NONE.value),
            ),
            _summary_item("manual_review_only", summary.get("manual_review_only", True)),
            _summary_item(
                "latest review markdown path",
                summary.get("latest_review_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_promotion_proposal(report: DailyTaskDashboardReport) -> str:
    summary = _shadow_promotion_proposal_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow Promotion Proposal</span>'
        f"<small>{_text(summary.get('proposal_decision', 'INSUFFICIENT_DATA'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Shadow Promotion Proposal</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="shadow-promotion-proposal-title">',
            '<div class="section-head">',
            '<h2 id="shadow-promotion-proposal-title">Shadow Promotion Proposal</h2>',
            (
                "<p>manual proposal 只读卡片；dashboard 只读取 proposal artifact，"
                "不重跑 018B/018C/018C2/proposal/scoring/broker/replay。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("latest proposal_decision", summary.get("proposal_decision", "MISSING")),
            _summary_item("promotion_proposed", summary.get("promotion_proposed", False)),
            _summary_item("promotion_executed", summary.get("promotion_executed", False)),
            _summary_item(
                "production_effect",
                summary.get("production_effect", ProductionEffect.NONE.value),
            ),
            _summary_item("manual_review_only", summary.get("manual_review_only", True)),
            _summary_item("average_score_delta", summary.get("average_score_delta", "NA")),
            _summary_item("risk_flag_delta_total", summary.get("risk_flag_delta_total", 0)),
            _summary_item(
                "available_comparison_days",
                summary.get("available_comparison_days", 0),
            ),
            _summary_item(
                "proposal markdown path",
                summary.get("latest_proposal_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_promotion_apply_preflight(report: DailyTaskDashboardReport) -> str:
    summary = _shadow_promotion_apply_preflight_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow Promotion Apply Preflight</span>'
        f"<small>{_text(summary.get('preflight_decision', 'INSUFFICIENT_DATA'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Shadow Promotion Apply Preflight</span><small>MISSING</small></span>"
    )
    changed_keys = ", ".join(_strings(summary.get("changed_weight_keys"))) or "none"
    return "\n".join(
        [
            '<section aria-labelledby="shadow-promotion-apply-preflight-title">',
            '<div class="section-head">',
            (
                '<h2 id="shadow-promotion-apply-preflight-title">'
                "Shadow Promotion Apply Preflight</h2>"
            ),
            (
                "<p>approved apply preflight 只读卡片；dashboard 只读取 preflight "
                "artifact，不重跑 018B/018C/018C2/018D/018E1/scoring/broker/replay。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "latest preflight_decision",
                summary.get("preflight_decision", "MISSING"),
            ),
            _summary_item(
                "production_effect",
                summary.get("production_effect", ProductionEffect.NONE.value),
            ),
            _summary_item("manual_review_only", summary.get("manual_review_only", True)),
            _summary_item("promotion_executed", summary.get("promotion_executed", False)),
            _summary_item("apply_executed", summary.get("apply_executed", False)),
            _summary_item("preflight_only", summary.get("preflight_only", True)),
            _summary_item("changed_weight_keys", changed_keys),
            _summary_item("proposal path", summary.get("proposal_path", "")),
            _summary_item("approval path", summary.get("approval_path", "")),
            _summary_item(
                "preflight markdown path",
                summary.get("latest_preflight_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_promotion_apply(report: DailyTaskDashboardReport) -> str:
    summary = _shadow_promotion_apply_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow Promotion Apply Result</span>'
        f"<small>{_text(summary.get('apply_decision', 'INSUFFICIENT_DATA'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Shadow Promotion Apply Result</span><small>MISSING</small></span>"
    )
    changed_keys = ", ".join(_strings(summary.get("changed_weight_keys"))) or "none"
    return "\n".join(
        [
            '<section aria-labelledby="shadow-promotion-apply-title">',
            '<div class="section-head">',
            '<h2 id="shadow-promotion-apply-title">Shadow Promotion Apply Result</h2>',
            (
                "<p>explicit apply result 只读卡片；dashboard 只读取 apply result "
                "artifact，不触发 018B/018C/018C2/018D/018E1/018E2/scoring/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("latest apply_decision", summary.get("apply_decision", "MISSING")),
            _summary_item("apply_executed", summary.get("apply_executed", False)),
            _summary_item("promotion_executed", summary.get("promotion_executed", False)),
            _summary_item(
                "production_effect",
                summary.get("production_effect", ProductionEffect.NONE.value),
            ),
            _summary_item("target_profile_path", summary.get("target_profile_path", "")),
            _summary_item("changed_weight_keys", changed_keys),
            _summary_item("rollback_snapshot_path", summary.get("rollback_snapshot_path", "")),
            _summary_item(
                "post_apply_validation.status",
                summary.get("post_apply_validation_status", "MISSING"),
            ),
            _summary_item("broker_execution", summary.get("broker_execution", False)),
            _summary_item("replay_execution", summary.get("replay_execution", False)),
            _summary_item("trading_execution", summary.get("trading_execution", False)),
            _summary_item(
                "apply result markdown path", summary.get("latest_apply_markdown_path", "")
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_promotion_rollback(report: DailyTaskDashboardReport) -> str:
    summary = _shadow_promotion_rollback_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow Promotion Rollback Result</span>'
        f"<small>{_text(summary.get('rollback_decision', 'INSUFFICIENT_DATA'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Shadow Promotion Rollback Result</span><small>MISSING</small></span>"
    )
    changed_keys = ", ".join(_strings(summary.get("changed_weight_keys"))) or "none"
    return "\n".join(
        [
            '<section aria-labelledby="shadow-promotion-rollback-title">',
            '<div class="section-head">',
            '<h2 id="shadow-promotion-rollback-title">Shadow Promotion Rollback Result</h2>',
            (
                "<p>explicit rollback result 只读卡片；dashboard 只读取 rollback result "
                "artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/"
                "scoring/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "latest rollback_decision",
                summary.get("rollback_decision", "MISSING"),
            ),
            _summary_item("rollback_executed", summary.get("rollback_executed", False)),
            _summary_item(
                "production_effect",
                summary.get("production_effect", ProductionEffect.NONE.value),
            ),
            _summary_item("target_profile_path", summary.get("target_profile_path", "")),
            _summary_item("changed_weight_keys", changed_keys),
            _summary_item("current_snapshot_path", summary.get("current_snapshot_path", "")),
            _summary_item("rollback_snapshot_path", summary.get("rollback_snapshot_path", "")),
            _summary_item(
                "post_rollback_validation.status",
                summary.get("post_rollback_validation_status", "MISSING"),
            ),
            _summary_item("broker_execution", summary.get("broker_execution", False)),
            _summary_item("replay_execution", summary.get("replay_execution", False)),
            _summary_item("trading_execution", summary.get("trading_execution", False)),
            _summary_item(
                "rollback result markdown path",
                summary.get("latest_rollback_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_promotion_lifecycle_audit(report: DailyTaskDashboardReport) -> str:
    summary = _shadow_promotion_lifecycle_audit_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Shadow Promotion Lifecycle Audit</span>'
        f"<small>{_text(summary.get('lifecycle_decision', 'INCOMPLETE_ARTIFACTS'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Shadow Promotion Lifecycle Audit</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="shadow-promotion-lifecycle-audit-title">',
            '<div class="section-head">',
            (
                '<h2 id="shadow-promotion-lifecycle-audit-title">'
                "Shadow Promotion Lifecycle Audit</h2>"
            ),
            (
                "<p>promotion lifecycle audit 只读卡片；dashboard 只读取 018F audit "
                "artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/018F/"
                "scoring/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "latest lifecycle_decision",
                summary.get("lifecycle_decision", "MISSING"),
            ),
            _summary_item("promotion_date", summary.get("promotion_date", "")),
            _summary_item("proposal status", summary.get("proposal_status", "MISSING")),
            _summary_item("preflight status", summary.get("preflight_status", "MISSING")),
            _summary_item("apply status", summary.get("apply_status", "MISSING")),
            _summary_item("rollback status", summary.get("rollback_status", "NOT_FOUND")),
            _summary_item(
                "safety_boundary_audit.status",
                summary.get("safety_boundary_status", "MISSING"),
            ),
            _summary_item("critical_findings", summary.get("critical_findings_count", 0)),
            _summary_item("warnings", summary.get("warnings_count", 0)),
            _summary_item("broker_execution", summary.get("broker_execution", False)),
            _summary_item("replay_execution", summary.get("replay_execution", False)),
            _summary_item("trading_execution", summary.get("trading_execution", False)),
            _summary_item(
                "audit markdown path",
                summary.get("latest_audit_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_parameter_governance_summary(report: DailyTaskDashboardReport) -> str:
    summary = _parameter_governance_summary_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Parameter Governance Summary</span>'
        f"<small>{_text(summary.get('governance_state', 'INCOMPLETE_DATA'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Parameter Governance Summary</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="parameter-governance-summary-title">',
            '<div class="section-head">',
            ('<h2 id="parameter-governance-summary-title">' "Parameter Governance Summary</h2>"),
            (
                "<p>参数治理总览只读卡片；dashboard 只读取 TRADING-019 summary "
                "artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/018F/019/"
                "scoring/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("governance_state", summary.get("governance_state", "MISSING")),
            _summary_item("action_required", summary.get("action_required", False)),
            _summary_item("action_level", summary.get("action_level", "NONE")),
            _summary_item("recommended_action", summary.get("recommended_action", "")),
            _summary_item(
                "production weights", summary.get("production_weights_summary", "MISSING")
            ),
            _summary_item("shadow weights", summary.get("shadow_weights_summary", "MISSING")),
            _summary_item("latest 018C2 review", summary.get("review_decision", "MISSING")),
            _summary_item("latest proposal", summary.get("proposal_decision", "MISSING")),
            _summary_item("latest preflight", summary.get("preflight_decision", "MISSING")),
            _summary_item("latest apply", summary.get("apply_decision", "MISSING")),
            _summary_item("latest rollback", summary.get("rollback_decision", "MISSING")),
            _summary_item("latest lifecycle", summary.get("lifecycle_decision", "MISSING")),
            _summary_item(
                "safety_boundary_audit.status",
                summary.get("safety_boundary_status", "MISSING"),
            ),
            _summary_item("critical_findings", summary.get("critical_findings_count", 0)),
            _summary_item("warnings", summary.get("warnings_count", 0)),
            _summary_item(
                "summary markdown path",
                summary.get("latest_summary_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_parameter_governance_web_view(report: DailyTaskDashboardReport) -> str:
    summary = _parameter_governance_web_view_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Parameter Governance Web View</span>'
        f"<small>{_text(summary.get('render_decision', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Parameter Governance Web View</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="parameter-governance-web-view-title">',
            '<div class="section-head">',
            ('<h2 id="parameter-governance-web-view-title">' "Parameter Governance Web View</h2>"),
            (
                "<p>参数治理 Web View 只读卡片；dashboard 只读取 TRADING-020 "
                "render metadata artifact，不触发 018B/018C/018C2/018D/018E1/"
                "018E2/018E3/018F/019/020/scoring/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "latest web view html path",
                summary.get("latest_web_view_html_path", ""),
            ),
            _summary_item(
                "latest render metadata path",
                summary.get("latest_render_metadata_path", ""),
            ),
            _summary_item("render_decision", summary.get("render_decision", "MISSING")),
            _summary_item("governance_state", summary.get("governance_state", "MISSING")),
            _summary_item("action_required", summary.get("action_required", False)),
            _summary_item("action_level", summary.get("action_level", "NONE")),
            _summary_item(
                "safety_boundary_status",
                summary.get("safety_boundary_status", "MISSING"),
            ),
            _summary_item("critical_findings", summary.get("critical_findings_count", 0)),
            _summary_item("warnings", summary.get("warnings_count", 0)),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_parameter_governance_daily_digest(report: DailyTaskDashboardReport) -> str:
    summary = _parameter_governance_daily_digest_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Parameter Governance Daily Digest</span>'
        f"<small>{_text(summary.get('digest_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Parameter Governance Daily Digest</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="parameter-governance-daily-digest-title">',
            '<div class="section-head">',
            (
                '<h2 id="parameter-governance-daily-digest-title">'
                "Parameter Governance Daily Digest</h2>"
            ),
            (
                "<p>参数治理 Daily Digest 只读卡片；dashboard 只读取 TRADING-021 "
                "digest artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/"
                "018F/019/020/021/scoring/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("digest_status", summary.get("digest_status", "MISSING")),
            _summary_item("summary_level", summary.get("summary_level", "UNKNOWN")),
            _summary_item("headline", summary.get("headline", "")),
            _summary_item("governance_state", summary.get("governance_state", "MISSING")),
            _summary_item("action_required", summary.get("action_required", False)),
            _summary_item("action_level", summary.get("action_level", "NONE")),
            _summary_item(
                "safety_boundary_status",
                summary.get("safety_boundary_status", "MISSING"),
            ),
            _summary_item("pending_apply", summary.get("pending_apply", False)),
            _summary_item("pending_rollback", summary.get("pending_rollback", False)),
            _summary_item("critical_alert_count", summary.get("critical_alert_count", 0)),
            _summary_item("warning_count", summary.get("warning_count", 0)),
            _summary_item(
                "digest markdown path",
                summary.get("latest_digest_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_pipeline_health_summary(report: DailyTaskDashboardReport) -> str:
    summary = _pipeline_health_summary_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Pipeline Health Summary</span>'
        f"<small>{_text(summary.get('health_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Pipeline Health Summary</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="pipeline-health-summary-title">',
            '<div class="section-head">',
            '<h2 id="pipeline-health-summary-title">Pipeline Health Summary</h2>',
            (
                "<p>Pipeline Health Summary 只读卡片；dashboard 只读取 TRADING-023 "
                "artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/"
                "018F/019/020/021/022/023/market/backtest/scoring/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("health_status", summary.get("health_status", "MISSING")),
            _summary_item("summary_level", summary.get("summary_level", "UNKNOWN")),
            _summary_item("headline", summary.get("headline", "")),
            _summary_item("required_pipelines", summary.get("required_pipelines", 0)),
            _summary_item(
                "missing_required_pipelines",
                summary.get("missing_required_pipelines", 0),
            ),
            _summary_item(
                "stale_required_pipelines",
                summary.get("stale_required_pipelines", 0),
            ),
            _summary_item("critical_pipelines", summary.get("critical_pipelines", 0)),
            _summary_item("warning_pipelines", summary.get("warning_pipelines", 0)),
            _summary_item(
                "pipeline health markdown path",
                summary.get("latest_summary_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_data_freshness_summary(report: DailyTaskDashboardReport) -> str:
    summary = _data_freshness_summary_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Data Freshness Summary</span>'
        f"<small>{_text(summary.get('freshness_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Data Freshness Summary</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="data-freshness-summary-title">',
            '<div class="section-head">',
            '<h2 id="data-freshness-summary-title">Data Freshness Summary</h2>',
            (
                "<p>Data Freshness Summary 只读卡片；dashboard 只读取 TRADING-024 "
                "artifact，不触发 018B/018C/018C2/018D/018E1/018E2/018E3/"
                "018F/019/020/021/022/023/024/data download/market/backtest/scoring/"
                "broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("freshness_status", summary.get("freshness_status", "MISSING")),
            _summary_item("summary_level", summary.get("summary_level", "UNKNOWN")),
            _summary_item("headline", summary.get("headline", "")),
            _summary_item("required_sources", summary.get("required_sources", 0)),
            _summary_item(
                "missing_required_sources",
                summary.get("missing_required_sources", 0),
            ),
            _summary_item(
                "stale_required_sources",
                summary.get("stale_required_sources", 0),
            ),
            _summary_item("critical_sources", summary.get("critical_sources", 0)),
            _summary_item("warning_sources", summary.get("warning_sources", 0)),
            _summary_item(
                "data freshness markdown path",
                summary.get("latest_summary_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_daily_trading_system_operator_brief(report: DailyTaskDashboardReport) -> str:
    summary = _daily_trading_system_operator_brief_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Daily Trading System Operator Brief</span>'
        f"<small>{_text(summary.get('brief_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Daily Trading System Operator Brief</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="daily-trading-system-operator-brief-title">',
            '<div class="section-head">',
            (
                '<h2 id="daily-trading-system-operator-brief-title">'
                "Daily Trading System Operator Brief</h2>"
            ),
            (
                "<p>系统级 operator brief 只读卡片；dashboard 只读取 TRADING-022 "
                "operator brief artifact，不触发 018B/018C/018C2/018D/018E1/018E2/"
                "018E3/018F/019/020/021/022/023/024/025/market/backtest/scoring/"
                "data download/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("brief_status", summary.get("brief_status", "MISSING")),
            _summary_item("summary_level", summary.get("summary_level", "UNKNOWN")),
            _summary_item("headline", summary.get("headline", "")),
            _summary_item(
                "can_trust_outputs_today",
                summary.get("can_trust_outputs_today", False),
            ),
            _summary_item(
                "manual_action_required",
                summary.get("manual_action_required", False),
            ),
            _summary_item(
                "parameter_governance.digest_status",
                summary.get("parameter_governance_digest_status", "MISSING"),
            ),
            _summary_item(
                "pipeline_health.status",
                summary.get("pipeline_health_status", "UNKNOWN"),
            ),
            _summary_item(
                "pipeline_health.health_status",
                summary.get("pipeline_health_health_status", "UNKNOWN"),
            ),
            _summary_item(
                "data_freshness.status",
                summary.get("data_freshness_status", "UNKNOWN"),
            ),
            _summary_item(
                "data_freshness.freshness_status",
                summary.get("data_freshness_freshness_status", "UNKNOWN"),
            ),
            _summary_item(
                "missing_required_pipelines",
                summary.get("missing_required_pipelines", 0),
            ),
            _summary_item(
                "stale_required_pipelines",
                summary.get("stale_required_pipelines", 0),
            ),
            _summary_item(
                "missing_required_sources",
                summary.get("missing_required_sources", 0),
            ),
            _summary_item(
                "stale_required_sources",
                summary.get("stale_required_sources", 0),
            ),
            _summary_item("critical_alert_count", summary.get("critical_alert_count", 0)),
            _summary_item("warning_count", summary.get("warning_count", 0)),
            _summary_item(
                "operator brief markdown path",
                summary.get("latest_brief_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_daily_operator_brief_scheduler_dry_run(report: DailyTaskDashboardReport) -> str:
    summary = _daily_operator_brief_scheduler_dry_run_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Daily Operator Brief Scheduler Dry Run</span>'
        f"<small>{_text(summary.get('dry_run_decision', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Daily Operator Brief Scheduler Dry Run</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="daily-operator-brief-scheduler-dry-run-title">',
            '<div class="section-head">',
            (
                '<h2 id="daily-operator-brief-scheduler-dry-run-title">'
                "Daily Operator Brief Scheduler Dry Run</h2>"
            ),
            (
                "<p>Operator brief scheduler dry run 只读卡片；dashboard 只读取 "
                "TRADING-026 dry-run artifact，不触发 018B-025、TRADING-026 script、"
                "operator brief、scheduler creation、market/backtest/scoring/data download/"
                "broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("dry_run_decision", summary.get("dry_run_decision", "MISSING")),
            _summary_item("dry_run_status", summary.get("dry_run_status", "MISSING")),
            _summary_item("summary_level", summary.get("summary_level", "UNKNOWN")),
            _summary_item(
                "expected_run_time_local",
                summary.get("expected_run_time_local", ""),
            ),
            _summary_item(
                "dependency_check.status",
                summary.get("dependency_check_status", "MISSING"),
            ),
            _summary_item("safety_check.status", summary.get("safety_check_status", "MISSING")),
            _summary_item(
                "missing_required_inputs_count",
                summary.get("missing_required_inputs_count", 0),
            ),
            _summary_item(
                "missing_optional_inputs_count",
                summary.get("missing_optional_inputs_count", 0),
            ),
            _summary_item("stale_inputs_count", summary.get("stale_inputs_count", 0)),
            _summary_item(
                "scheduler dry-run markdown path",
                summary.get("latest_dry_run_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_daily_operator_brief_scheduler_templates(report: DailyTaskDashboardReport) -> str:
    summary = _daily_operator_brief_scheduler_templates_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Scheduler Template Summary</span>'
        f"<small>{_text(summary.get('template_generation_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Scheduler Template Summary</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="daily-operator-brief-scheduler-templates-title">',
            '<div class="section-head">',
            (
                '<h2 id="daily-operator-brief-scheduler-templates-title">'
                "Scheduler Configuration Templates</h2>"
            ),
            (
                "<p>Scheduler template 只读卡片；dashboard 只读取 TRADING-028 metadata "
                "artifact，不触发 018B-027、TRADING-028 script、operator brief、scheduler "
                "creation、market/backtest/scoring/data download/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "template_generation_status",
                summary.get("template_generation_status", "MISSING"),
            ),
            _summary_item("scheduler_created", summary.get("scheduler_created", False)),
            _summary_item("scheduler_installed", summary.get("scheduler_installed", False)),
            _summary_item("scheduler_enabled", summary.get("scheduler_enabled", False)),
            _summary_item(
                "manual_review_required",
                summary.get("manual_review_required", True),
            ),
            _summary_item(
                "generated_template_count",
                summary.get("generated_template_count", 0),
            ),
            _summary_item(
                "windows template path",
                summary.get("windows_template_path", ""),
            ),
            _summary_item("cron template path", summary.get("cron_template_path", "")),
            _summary_item(
                "GitHub Actions template path",
                summary.get("github_actions_template_path", ""),
            ),
            _summary_item(
                "summary markdown path",
                summary.get("summary_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_daily_operator_brief_scheduler_template_validation(
    report: DailyTaskDashboardReport,
) -> str:
    summary = _daily_operator_brief_scheduler_template_validation_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Scheduler Template Validation</span>'
        f"<small>{_text(summary.get('validation_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Scheduler Template Validation</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="daily-operator-brief-scheduler-template-validation-title">',
            '<div class="section-head">',
            (
                '<h2 id="daily-operator-brief-scheduler-template-validation-title">'
                "Scheduler Template Validation Report</h2>"
            ),
            (
                "<p>Scheduler template validation 只读卡片；dashboard 只读取 TRADING-029 "
                "validation artifact，不触发 018B-028、TRADING-029 script、template "
                "generator、operator brief、scheduler creation、market/backtest/scoring/"
                "data download/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("validation_status", summary.get("validation_status", "MISSING")),
            _summary_item("summary_level", summary.get("summary_level", "UNKNOWN")),
            _summary_item("templates_declared", summary.get("templates_declared", 0)),
            _summary_item("templates_found", summary.get("templates_found", 0)),
            _summary_item("templates_passed", summary.get("templates_passed", 0)),
            _summary_item(
                "templates_with_warnings",
                summary.get("templates_with_warnings", 0),
            ),
            _summary_item("templates_failed", summary.get("templates_failed", 0)),
            _summary_item(
                "critical findings count",
                summary.get("critical_findings_count", 0),
            ),
            _summary_item("warnings count", summary.get("warnings_count", 0)),
            _summary_item(
                "validation markdown path",
                summary.get("validation_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_operator_brief_notification_draft(report: DailyTaskDashboardReport) -> str:
    summary = _operator_brief_notification_draft_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}"><span>Operator Brief Notification Draft</span>'
        f"<small>{_text(summary.get('draft_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Operator Brief Notification Draft</span><small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="operator-brief-notification-draft-title">',
            '<div class="section-head">',
            (
                '<h2 id="operator-brief-notification-draft-title">'
                "Operator Brief Notification Draft</h2>"
            ),
            (
                "<p>Operator brief notification draft 只读卡片；dashboard 只读取 "
                "TRADING-030 metadata artifact，不触发 018B-029、TRADING-030 script、"
                "operator brief、email/Gmail/Slack/Discord/mobile push、market/backtest/"
                "scoring/data download/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("draft_status", summary.get("draft_status", "MISSING")),
            _summary_item(
                "notification_severity",
                summary.get("notification_severity", "UNKNOWN"),
            ),
            _summary_item("headline", summary.get("headline", "")),
            _summary_item("email draft path", summary.get("email_draft_path", "")),
            _summary_item("chat draft path", summary.get("chat_draft_path", "")),
            _summary_item("mobile summary path", summary.get("mobile_summary_path", "")),
            _summary_item(
                "manual_review_required",
                summary.get("manual_review_required", True),
            ),
            _summary_item("email_sent", summary.get("email_sent", False)),
            _summary_item("slack_sent", summary.get("slack_sent", False)),
            _summary_item("discord_sent", summary.get("discord_sent", False)),
            _summary_item("mobile_push_sent", summary.get("mobile_push_sent", False)),
            _summary_item(
                "summary markdown path",
                summary.get("summary_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_operator_brief_notification_delivery_preflight(
    report: DailyTaskDashboardReport,
) -> str:
    summary = _operator_brief_notification_delivery_preflight_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>Operator Brief Notification Delivery Preflight</span>"
        f"<small>{_text(summary.get('preflight_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Operator Brief Notification Delivery Preflight</span>"
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="operator-brief-notification-delivery-preflight-title">',
            '<div class="section-head">',
            (
                '<h2 id="operator-brief-notification-delivery-preflight-title">'
                "Operator Brief Notification Delivery Preflight</h2>"
            ),
            (
                "<p>Operator brief notification delivery preflight 只读卡片；dashboard "
                "只读取 TRADING-031 metadata artifact，不触发 018B-030、TRADING-031 "
                "script、operator brief、notification draft generator、email/Gmail/"
                "Slack/Discord/webhook/mobile、market/backtest/scoring/data download/"
                "broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("preflight_status", summary.get("preflight_status", "MISSING")),
            _summary_item("delivery_readiness", summary.get("delivery_readiness", "UNKNOWN")),
            _summary_item(
                "notification_severity",
                summary.get("notification_severity", "UNKNOWN"),
            ),
            _summary_item("email channel", summary.get("email_channel_status", "MISSING")),
            _summary_item("chat channel", summary.get("chat_channel_status", "MISSING")),
            _summary_item("mobile channel", summary.get("mobile_channel_status", "MISSING")),
            _summary_item("approval_required", summary.get("approval_required", True)),
            _summary_item(
                "critical_alert_count",
                summary.get("critical_alert_count", 0),
            ),
            _summary_item("warning_count", summary.get("warning_count", 0)),
            _summary_item("email_sent", summary.get("email_sent", False)),
            _summary_item("gmail_draft_created", summary.get("gmail_draft_created", False)),
            _summary_item("gmail_draft_modified", summary.get("gmail_draft_modified", False)),
            _summary_item("slack_sent", summary.get("slack_sent", False)),
            _summary_item("discord_sent", summary.get("discord_sent", False)),
            _summary_item("webhook_called", summary.get("webhook_called", False)),
            _summary_item("mobile_push_sent", summary.get("mobile_push_sent", False)),
            _summary_item(
                "preflight markdown path",
                summary.get("preflight_markdown_path", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_operator_brief_notification_dispatch_preview(
    report: DailyTaskDashboardReport,
) -> str:
    summary = _operator_brief_notification_dispatch_preview_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>Operator Brief Notification Dispatch Preview</span>"
        f"<small>{_text(summary.get('final_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Operator Brief Notification Dispatch Preview</span>"
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="operator-brief-notification-dispatch-preview-title">',
            '<div class="section-head">',
            (
                '<h2 id="operator-brief-notification-dispatch-preview-title">'
                "Operator Brief Notification Dispatch Preview</h2>"
            ),
            (
                "<p>Operator brief notification dispatch preview 只读卡片；dashboard "
                "只读取 TRADING-032 artifact，不触发 018B-031、TRADING-032 script、"
                "delivery preflight、operator brief、notification draft generator、"
                "email/Gmail/Slack/Telegram/Discord/webhook/mobile、market/backtest/"
                "scoring/data download/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("final_status", summary.get("final_status", "MISSING")),
            _summary_item("preflight_status", summary.get("preflight_status", "MISSING")),
            _summary_item("dispatch_status", summary.get("dispatch_status", "MISSING")),
            _summary_item("channel_count", summary.get("channel_count", 0)),
            _summary_item(
                "would_send_channel_count",
                summary.get("would_send_channel_count", 0),
            ),
            _summary_item(
                "human_action_required",
                summary.get("human_action_required", True),
            ),
            _summary_item(
                "next_recommended_action",
                summary.get("next_recommended_action", ""),
            ),
            _summary_item("latest artifact path", summary.get("latest_artifact_path", "")),
            _summary_item("generated_at", summary.get("generated_at", "")),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_operator_brief_notification_approval_gate(
    report: DailyTaskDashboardReport,
) -> str:
    summary = _operator_brief_notification_approval_gate_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>Operator Brief Notification Approval Gate</span>"
        f"<small>{_text(summary.get('approval_gate_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Operator Brief Notification Approval Gate</span>"
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="operator-brief-notification-approval-gate-title">',
            '<div class="section-head">',
            (
                '<h2 id="operator-brief-notification-approval-gate-title">'
                "Operator Brief Notification Approval Gate</h2>"
            ),
            (
                "<p>Operator brief notification approval gate 只读卡片；dashboard "
                "只读取 TRADING-033 artifact，不触发 018B-032、TRADING-033 script、"
                "dispatch preview、delivery preflight、operator brief、notification draft "
                "generator、email/Gmail/SMTP/Slack/Telegram/Discord/webhook/mobile、"
                "market/backtest/scoring/data download/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item(
                "approval_gate_status",
                summary.get("approval_gate_status", "MISSING"),
            ),
            _summary_item(
                "allowed_to_enter_dispatch",
                summary.get("allowed_to_enter_dispatch", False),
            ),
            _summary_item(
                "human_action_required",
                summary.get("human_action_required", True),
            ),
            _summary_item(
                "dispatch_preview_status",
                summary.get("dispatch_preview_status", "MISSING"),
            ),
            _summary_item(
                "approval_marker_exists",
                summary.get("approval_marker_exists", False),
            ),
            _summary_item("hash_matches", summary.get("hash_matches", False)),
            _summary_item("expired", summary.get("expired", False)),
            _summary_item("generated_at", summary.get("generated_at", "")),
            _summary_item("latest artifact path", summary.get("latest_artifact_path", "")),
            _summary_item(
                "next_recommended_action",
                summary.get("next_recommended_action", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_operator_brief_notification_draft_dispatch(
    report: DailyTaskDashboardReport,
) -> str:
    summary = _operator_brief_notification_draft_dispatch_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>Operator Brief Notification Draft Dispatch</span>"
        f"<small>{_text(summary.get('final_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Operator Brief Notification Draft Dispatch</span>"
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="operator-brief-notification-draft-dispatch-title">',
            '<div class="section-head">',
            (
                '<h2 id="operator-brief-notification-draft-dispatch-title">'
                "Operator Brief Notification Draft Dispatch</h2>"
            ),
            (
                "<p>Operator brief notification draft dispatch 只读卡片；dashboard "
                "只读取 TRADING-034 latest.json artifact，不触发 TRADING-034 script、"
                "approval gate、dispatch preview、delivery preflight、operator brief、"
                "notification draft generator、email/Gmail/SMTP/Slack/Telegram/"
                "Discord/webhook/mobile、market/backtest/scoring/data download/broker/"
                "replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("final_status", summary.get("final_status", "MISSING")),
            _summary_item(
                "ready_for_actual_dispatch",
                summary.get("ready_for_actual_dispatch", False),
            ),
            _summary_item(
                "approval_gate_status",
                summary.get("approval_gate_status", "MISSING"),
            ),
            _summary_item("channel_count", summary.get("channel_count", 0)),
            _summary_item(
                "draft_ready_channel_count",
                summary.get("draft_ready_channel_count", 0),
            ),
            _summary_item("draft_hash", summary.get("draft_hash", "")),
            _summary_item("generated_at", summary.get("generated_at", "")),
            _summary_item("latest artifact path", summary.get("latest_artifact_path", "")),
            _summary_item(
                "next_recommended_action",
                summary.get("next_recommended_action", ""),
            ),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_notification_delivery_audit_summary(report: DailyTaskDashboardReport) -> str:
    summary = _notification_delivery_audit_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>Notification Delivery Audit Summary</span>"
        f"<small>{_text(summary.get('audit_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Notification Delivery Audit Summary</span>"
        "<small>MISSING</small></span>"
    )
    return "\n".join(
        [
            '<section aria-labelledby="notification-delivery-audit-summary-title">',
            '<div class="section-head">',
            (
                '<h2 id="notification-delivery-audit-summary-title">'
                "Notification Delivery Audit Summary</h2>"
            ),
            (
                "<p>Notification delivery audit summary 只读卡片；dashboard 只读取 "
                "TRADING-035 audit artifact，不触发 018B-034、TRADING-035 script、"
                "notification draft generator、delivery preflight、draft dispatch、"
                "operator brief、email/Gmail/Slack/Discord/webhook/mobile、market/"
                "backtest/scoring/data download/broker/replay/交易。</p>"
            ),
            "</div>",
            '<div class="summary-grid">',
            _summary_item("audit_status", summary.get("audit_status", "MISSING")),
            _summary_item(
                "notification_lifecycle_status",
                summary.get("notification_lifecycle_status", "UNKNOWN"),
            ),
            _summary_item("summary_level", summary.get("summary_level", "UNKNOWN")),
            _summary_item("draft_status", summary.get("draft_status", "MISSING")),
            _summary_item("preflight_status", summary.get("preflight_status", "MISSING")),
            _summary_item("dispatch_status", summary.get("dispatch_status", "MISSING")),
            _summary_item("draft_hash_match", summary.get("draft_hash_match", False)),
            _summary_item("latest_json_match", summary.get("latest_json_match", False)),
            _summary_item(
                "external_side_effect_audit.status",
                summary.get("external_side_effect_audit_status", "MISSING"),
            ),
            _summary_item(
                "critical_alert_count",
                summary.get("critical_alert_count", 0),
            ),
            _summary_item("warning_count", summary.get("warning_count", 0)),
            _summary_item("markdown path", summary.get("markdown_path", "")),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_notification_delivery_failure_classification(
    report: DailyTaskDashboardReport,
) -> str:
    summary = _notification_delivery_failure_classification(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>Notification Delivery Failure Classification</span>"
        f"<small>{_text(summary.get('overall_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Notification Delivery Failure Classification</span>"
        "<small>MISSING</small></span>"
    )
    missing_note = (
        '<p class="subtle">'
        "No notification delivery failure classification report available."
        "</p>"
        if summary.get("exists") is not True
        else ""
    )
    return "\n".join(
        [
            '<section aria-labelledby="notification-delivery-failure-classification-title">',
            '<div class="section-head">',
            (
                '<h2 id="notification-delivery-failure-classification-title">'
                "Notification Delivery Failure Classification</h2>"
            ),
            (
                "<p>Notification delivery failure classification 只读卡片；dashboard 只读取 "
                "TRADING-036 JSON，不运行 classifier、不读取或修改外部通知状态、不发送 "
                "notification、不触发 retry、不修改 production 参数。</p>"
            ),
            "</div>",
            missing_note,
            '<div class="summary-grid">',
            _summary_item("overall_status", summary.get("overall_status", "MISSING")),
            _summary_item("highest_severity", summary.get("highest_severity", "UNKNOWN")),
            _summary_item("total_failures", summary.get("total_failures", 0)),
            _summary_item(
                "requires_manual_review",
                summary.get("requires_manual_review", False),
            ),
            _summary_item("safe_to_retry", summary.get("safe_to_retry", False)),
            _summary_item(
                "blocks_notification_chain",
                summary.get("blocks_notification_chain", False),
            ),
            _summary_item(
                "source audit status",
                summary.get("source_audit_status", "MISSING"),
            ),
            _summary_item("generated_at", summary.get("generated_at", "")),
            _summary_item("markdown path", summary.get("markdown_path", "")),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_retry_candidate_queue(report: DailyTaskDashboardReport) -> str:
    summary = _retry_candidate_queue(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>Retry Candidate Queue</span>"
        f"<small>{_text(summary.get('queue_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Retry Candidate Queue</span>"
        "<small>MISSING</small></span>"
    )
    missing_note = (
        '<p class="subtle">No retry candidate queue report available.</p>'
        if summary.get("exists") is not True
        else ""
    )
    return "\n".join(
        [
            '<section aria-labelledby="retry-candidate-queue-title">',
            '<div class="section-head">',
            '<h2 id="retry-candidate-queue-title">Retry Candidate Queue</h2>',
            (
                "<p>Retry candidate queue 只读卡片；dashboard 只读取 TRADING-037 "
                "JSON，不运行 queue generator、不执行 retry、不发送 notification、"
                "不修改 approval state 或 production 参数。</p>"
            ),
            "</div>",
            missing_note,
            '<div class="summary-grid">',
            _summary_item("queue_status", summary.get("queue_status", "MISSING")),
            _summary_item("total_candidates", summary.get("total_candidates", 0)),
            _summary_item("blocked_candidates", summary.get("blocked_candidates", 0)),
            _summary_item(
                "manual_review_required",
                summary.get("manual_review_required", False),
            ),
            _summary_item(
                "has_retryable_candidates",
                summary.get("has_retryable_candidates", False),
            ),
            _summary_item(
                "safe_to_execute_retry",
                summary.get("safe_to_execute_retry", False),
            ),
            _summary_item("approval_status", summary.get("approval_status", "MISSING")),
            _summary_item(
                "retry_execution_allowed",
                summary.get("retry_execution_allowed", False),
            ),
            _summary_item(
                "source classification status",
                summary.get("source_classification_status", "MISSING"),
            ),
            _summary_item(
                "source parse status",
                summary.get("source_parse_status", "MISSING"),
            ),
            _summary_item("generated_at", summary.get("generated_at", "")),
            _summary_item("markdown path", summary.get("markdown_path", "")),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_retry_execution_dry_run(report: DailyTaskDashboardReport) -> str:
    summary = _retry_execution_dry_run(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>Retry Execution Dry Run</span>"
        f"<small>{_text(summary.get('dry_run_status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>Retry Execution Dry Run</span>"
        "<small>MISSING</small></span>"
    )
    missing_note = (
        '<p class="subtle">No retry execution dry-run report available.</p>'
        if summary.get("exists") is not True
        else ""
    )
    return "\n".join(
        [
            '<section aria-labelledby="retry-execution-dry-run-title">',
            '<div class="section-head">',
            '<h2 id="retry-execution-dry-run-title">Retry Execution Dry Run</h2>',
            (
                "<p>Retry execution dry-run 只读卡片；dashboard 只读取 TRADING-038 "
                "JSON，不运行 dry-run generator、不执行 retry、不发送 notification、"
                "不修改 approval record、delivery state 或 production 参数。</p>"
            ),
            "</div>",
            missing_note,
            '<div class="summary-grid">',
            _summary_item("dry_run_status", summary.get("dry_run_status", "MISSING")),
            _summary_item("total_candidates", summary.get("total_candidates", 0)),
            _summary_item(
                "approved_for_dry_run",
                summary.get("approved_for_dry_run", 0),
            ),
            _summary_item(
                "blocked_from_dry_run",
                summary.get("blocked_from_dry_run", 0),
            ),
            _summary_item(
                "simulated_retry_actions",
                summary.get("simulated_retry_actions", 0),
            ),
            _summary_item("real_retry_allowed", summary.get("real_retry_allowed", False)),
            _summary_item(
                "external_delivery_allowed",
                summary.get("external_delivery_allowed", False),
            ),
            _summary_item(
                "production_state_mutation_allowed",
                summary.get("production_state_mutation_allowed", False),
            ),
            _summary_item(
                "source queue status",
                summary.get("source_queue_status", "MISSING"),
            ),
            _summary_item(
                "approval parse status",
                summary.get("approval_parse_status", "MISSING"),
            ),
            _summary_item("generated_at", summary.get("generated_at", "")),
            _summary_item("markdown path", summary.get("markdown_path", "")),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_sec_pit_evaluation_summary(report: DailyTaskDashboardReport) -> str:
    summary = _sec_pit_evaluation_summary(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>SEC PIT Evaluation Summary</span>"
        f"<small>{_text(summary.get('status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>SEC PIT Evaluation Summary</span>"
        "<small>MISSING</small></span>"
    )
    missing_note = (
        '<p class="subtle">No SEC PIT evaluation summary available.</p>'
        if summary.get("exists") is not True
        else ""
    )
    top_features = summary.get("top_features")
    feature_text = "none"
    if isinstance(top_features, list) and top_features:
        parts = []
        for item in top_features[:5]:
            if isinstance(item, dict):
                parts.append(
                    f"{item.get('feature_id', '')}:"
                    f"{_text(item.get('rank_ic_20d', ''))}:"
                    f"{item.get('recommendation', '')}"
                )
        feature_text = "；".join(parts) or "none"
    return "\n".join(
        [
            '<section aria-labelledby="sec-pit-evaluation-summary-title">',
            '<div class="section-head">',
            '<h2 id="sec-pit-evaluation-summary-title">SEC PIT Evaluation Summary</h2>',
            (
                "<p>SEC PIT evaluation 只读卡片；dashboard 只读取 TRADING-040 "
                "artifact，不运行 evaluation、不重新读取 market data、不修改 production 权重。</p>"
            ),
            "</div>",
            missing_note,
            '<div class="summary-grid">',
            _summary_item("latest evaluation date", summary.get("latest_evaluation_date", "")),
            _summary_item("universe size", summary.get("universe_size", 0)),
            _summary_item("feature count", summary.get("feature_count", 0)),
            _summary_item(
                "promote_to_shadow",
                summary.get("promote_to_shadow_count", 0),
            ),
            _summary_item("research_only", summary.get("research_only_count", 0)),
            _summary_item("excluded", summary.get("excluded_count", 0)),
            _summary_item("top 5 features by rank_ic_20d", feature_text),
            _summary_item("PIT safety status", summary.get("pit_safety_status", "MISSING")),
            _summary_item("production_effect", summary.get("production_effect", "none")),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _render_sec_pit_baseline_comparison(report: DailyTaskDashboardReport) -> str:
    summary = _sec_pit_baseline_comparison(report)
    report_href = _string_value(summary.get("report_href"))
    report_link = (
        '<a class="report-link" '
        f'href="{_text(report_href)}">'
        "<span>SEC PIT Baseline Comparison</span>"
        f"<small>{_text(summary.get('status', 'MISSING'))}</small></a>"
        if report_href
        else '<span class="report-link missing">'
        "<span>SEC PIT Baseline Comparison</span>"
        "<small>MISSING</small></span>"
    )
    missing_note = (
        '<p class="subtle">No SEC PIT baseline comparison summary available.</p>'
        if summary.get("exists") is not True
        else ""
    )
    return "\n".join(
        [
            '<section aria-labelledby="sec-pit-baseline-comparison-title">',
            '<div class="section-head">',
            '<h2 id="sec-pit-baseline-comparison-title">SEC PIT Baseline Comparison</h2>',
            (
                "<p>TRADING-041 只读卡片；dashboard 只读取 comparison artifact，"
                "不运行 comparison、不重新读取 market data、不修改 production 权重或 action。</p>"
            ),
            "</div>",
            missing_note,
            '<div class="summary-grid">',
            _summary_item("latest comparison date", summary.get("latest_comparison_date", "")),
            _summary_item("comparison status", summary.get("status", "MISSING")),
            _summary_item("decision count", summary.get("decision_count", 0)),
            _summary_item("action changed count", summary.get("action_changed_count", 0)),
            _summary_item(
                "material rank shift count",
                summary.get("material_rank_shift_count", 0),
            ),
            _summary_item(
                "incremental alpha 20d",
                _format_signed_percent(summary.get("incremental_alpha_20d")),
            ),
            _summary_item(
                "drawdown improvement 20d",
                _format_signed_percent(summary.get("drawdown_improvement_20d")),
            ),
            _summary_item(
                "top promoted tickers",
                _dashboard_ticker_delta_text(summary.get("top_promoted_tickers")),
            ),
            _summary_item(
                "top downgraded tickers",
                _dashboard_ticker_delta_text(summary.get("top_downgraded_tickers")),
            ),
            _summary_item("production_effect", summary.get("production_effect", "none")),
            "</div>",
            (
                '<p class="risk-line"><strong>重点风险：</strong>'
                f"{_text(summary.get('risk', ''))}</p>"
            ),
            '<div class="report-link-list">',
            report_link,
            "</div>",
            "</section>",
        ]
    )


def _dashboard_ticker_delta_text(value: object) -> str:
    if not isinstance(value, list) or not value:
        return "none"
    parts: list[str] = []
    for item in value[:5]:
        if not isinstance(item, dict):
            continue
        ticker = item.get("ticker", "")
        rank_delta = item.get("rank_delta", 0)
        score_delta = item.get("score_delta")
        score_text = "NA" if score_delta is None else f"{float(score_delta):+.2f}"
        parts.append(f"{ticker} rank {rank_delta:+} / score {score_text}")
    return "；".join(parts) or "none"


def _shadow_impact_sample_text(impact: TraceRecord) -> str:
    counts = _mapping_value(impact, "window_sample_counts")
    parts = []
    for window_days in PAPER_TRADING_TREND_WINDOWS:
        window = _mapping_value(counts, str(window_days))
        parts.append(
            f"{window_days}d P/S/U "
            f"{window.get('production', 0)}/"
            f"{window.get('shadow', 0)}/"
            f"{window.get('unknown', 0)}"
        )
    return "；".join(parts)


def _count_pair(summary: TraceRecord, left_key: str, right_key: str) -> str:
    return f"{summary.get(left_key, 'missing')} / {summary.get(right_key, 'missing')}"


def _count_triplet(
    summary: TraceRecord,
    first_key: str,
    second_key: str,
    third_key: str,
) -> str:
    return (
        f"{summary.get(first_key, 'missing')} / "
        f"{summary.get(second_key, 'missing')} / "
        f"{summary.get(third_key, 'missing')}"
    )


def _format_money_value(value: object) -> str:
    number = _optional_float(value)
    if number is None:
        return "missing"
    return f"{number:.2f}"


def _format_distribution(value: object) -> str:
    if not isinstance(value, dict) or not value:
        return "none"
    parts = [
        f"{key}:{count}" for key, count in sorted(value.items()) if _string_value(key) or str(key)
    ]
    return "；".join(parts) or "none"


def _format_top_records(value: object) -> str:
    parts = []
    for record in _records(value):
        label = _string_value(record.get("value"))
        count = _optional_int(record.get("count"))
        if label and count is not None:
            parts.append(f"{label}:{count}")
    return "；".join(parts) or "none"


def _render_risks(report: DailyTaskDashboardReport) -> str:
    risky_tasks = [task for task in report.tasks if task.risk_level != "none"]
    rows = (
        ["<tr><td>无</td><td>未发现阻断或限制风险。</td><td></td></tr>"]
        if not risky_tasks
        else [
            (
                f"<tr><td>{_text(task.title)}</td>"
                f"<td>{_status_badge(task.risk_level)}</td>"
                f"<td>{_text(task.important_risk)}</td></tr>"
            )
            for task in risky_tasks
        ]
    )
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
            ('<p class="subtask-risk"><strong>风险：</strong>' f"{_text(task.important_risk)}</p>"),
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


def _project_path(project_root: Path, path_value: object) -> Path | None:
    text = str(path_value or "").strip()
    if not text:
        return None
    path = Path(text)
    return path if path.is_absolute() else project_root / path


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


def _bool_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return False


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, (str, bytes, bytearray, int, float)):
            return float(value)
        return None
    except (TypeError, ValueError):
        return None


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _parse_iso_datetime(value: str) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _format_seconds(value: float | None) -> str:
    if value is None:
        return "未执行"
    return f"{value:.1f}s"


def _join_nonempty(
    values: list[object | None] | tuple[object | None, ...],
    *,
    separator: str = "；",
) -> str:
    return separator.join(str(value) for value in values if value is not None and str(value) != "")


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


def _weight_summary(values: TraceRecord) -> str:
    parts: list[str] = []
    for key in sorted(values):
        number = _optional_float(values.get(key))
        if number is None:
            continue
        parts.append(f"{key}={number:.4f}")
    return "，".join(parts) or "MISSING"


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
        normalized in {"skipped", "medium"} or "warning" in normalized or "limitation" in normalized
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
