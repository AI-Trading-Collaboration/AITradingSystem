from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.alerts import (
    default_alert_report_path,
)
from ai_trading_system.cli_commands.data_artifacts import (
    _resolve_market_data_freshness_path,
    _resolve_market_data_refresh_path,
)
from ai_trading_system.cli_commands.parameter_artifacts import (
    _resolve_shadow_backtest_summary_path,
    _resolve_weight_stability_path,
    _resolve_weight_stability_readiness_path,
    _resolve_weight_tuning_failure_path,
    _resolve_weight_tuning_path,
)
from ai_trading_system.cli_commands.portfolio_artifacts import (
    _resolve_portfolio_candidate_review_decision_path,
    _resolve_portfolio_candidate_tracking_path,
    _resolve_portfolio_candidates_path,
    _resolve_portfolio_sensitivity_path,
    _resolve_portfolio_tracking_review_path,
    _resolve_portfolio_turnover_attribution_path,
)
from ai_trading_system.config import (
    PROJECT_ROOT,
    load_data_quality,
)
from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_report,
    default_daily_decision_summary_path,
    default_daily_task_dashboard_json_path,
    default_daily_task_dashboard_path,
    write_daily_decision_summary_json,
    write_daily_task_dashboard,
    write_daily_task_dashboard_json,
)
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.decision_learning_queue import (
    DEFAULT_DECISION_LEARNING_QUEUE_PATH,
)
from ai_trading_system.decision_outcomes import (
    DEFAULT_DECISION_OUTCOMES_PATH,
)
from ai_trading_system.decision_snapshots import (
    DEFAULT_DECISION_SNAPSHOT_DIR,
    default_decision_snapshot_path,
)
from ai_trading_system.documentation_contract import default_documentation_contract_json_path
from ai_trading_system.evidence_dashboard import (
    build_evidence_dashboard_report,
    default_evidence_dashboard_json_path,
    default_evidence_dashboard_path,
    write_evidence_dashboard,
    write_evidence_dashboard_json,
)
from ai_trading_system.feedback_loop_review import (
    default_feedback_loop_review_report_path,
)
from ai_trading_system.market_feedback_optimization import (
    default_market_feedback_optimization_report_path,
)
from ai_trading_system.ops_daily import (
    default_daily_ops_run_metadata_path,
    default_daily_ops_run_report_path,
)
from ai_trading_system.order_intent_candidates import (
    default_order_intent_candidates_path,
    write_order_intent_candidates_json,
)
from ai_trading_system.periodic_investment_review import (
    DEFAULT_PERIODIC_INVESTMENT_REVIEW_REPORT_DIR,
    DEFAULT_SCORES_DAILY_PATH,
    build_periodic_investment_review_report,
    default_periodic_investment_review_report_path,
    write_periodic_investment_review_report,
)
from ai_trading_system.prediction_ledger import (
    DEFAULT_PREDICTION_OUTCOMES_PATH,
)
from ai_trading_system.report_traceability import (
    default_report_trace_bundle_path,
)
from ai_trading_system.reports.calculation_explainers import (
    DEFAULT_METRIC_EXPLAINERS_CONFIG_PATH,
    build_calculation_explainers_payload,
    default_calculation_explainers_path,
    write_calculation_explainers_json,
)
from ai_trading_system.reports.market_panel import (
    build_market_panel_payload,
    default_market_panel_json_path,
    default_market_panel_report_path,
    write_market_panel_json,
    write_market_panel_report,
)
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
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
    default_report_index_html_path,
    default_report_index_json_path,
    write_report_index_html,
    write_report_index_json,
)
from ai_trading_system.reports.report_quality_gate import (
    build_report_quality_gate_payload,
    default_report_quality_gate_json_path,
    default_report_quality_gate_markdown_path,
    write_report_quality_gate_json,
    write_report_quality_gate_markdown,
)
from ai_trading_system.reports.research_governance_summary import (
    build_research_governance_summary_payload,
    default_research_governance_summary_json_path,
    default_research_governance_summary_report_path,
    write_research_governance_summary_json,
    write_research_governance_summary_report,
)
from ai_trading_system.reports.score_change_attribution import (
    build_score_change_attribution_payload,
    default_score_change_attribution_json_path,
    default_score_change_attribution_report_path,
    write_score_change_attribution_json,
    write_score_change_attribution_report,
)
from ai_trading_system.rule_experiments import (
    DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
)
from ai_trading_system.scoring.daily import (
    default_daily_score_report_path,
)
from ai_trading_system.trading_engine.market_data_freshness import (
    load_market_data_freshness_payload,
    market_data_freshness_payload_date,
    validate_market_data_freshness_payload,
    write_market_data_freshness_report_alias,
)
from ai_trading_system.trading_engine.market_data_refresh import (
    load_market_data_refresh_payload,
    market_data_refresh_payload_date,
    validate_market_data_refresh_payload,
    write_market_data_refresh_report_alias,
)
from ai_trading_system.trading_engine.parameters.weight_stability import (
    load_weight_stability_payload,
    validate_weight_stability_payload,
    weight_stability_payload_date,
    write_weight_stability_report_alias,
)
from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    load_weight_stability_readiness_payload,
    validate_weight_stability_readiness_payload,
    weight_stability_readiness_payload_date,
    write_weight_stability_readiness_report_alias,
)
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    load_weight_tuning_payload,
    validate_weight_tuning_payload,
    weight_tuning_payload_date,
    write_weight_tuning_report_alias,
)
from ai_trading_system.trading_engine.parameters.weight_tuning_failure import (
    load_weight_tuning_failure_payload,
    validate_weight_tuning_failure_payload,
    weight_tuning_failure_payload_date,
    write_weight_tuning_failure_report_alias,
)
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    load_portfolio_candidate_review_payload,
    portfolio_candidate_review_payload_date,
    validate_portfolio_candidate_review_decision_payload,
    write_portfolio_candidate_review_report_alias,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    load_portfolio_candidate_tracking_payload,
    portfolio_candidate_tracking_payload_date,
    validate_portfolio_candidate_tracking_payload,
    write_portfolio_candidate_tracking_report_alias,
)
from ai_trading_system.trading_engine.portfolio_candidates import (
    load_portfolio_candidates_payload,
    portfolio_candidates_payload_date,
    validate_portfolio_candidates_payload,
    write_portfolio_candidates_report_alias,
)
from ai_trading_system.trading_engine.portfolio_sensitivity import (
    load_portfolio_sensitivity_payload,
    portfolio_sensitivity_payload_date,
    validate_portfolio_sensitivity_payload,
    write_portfolio_sensitivity_report_alias,
)
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    load_portfolio_tracking_review_payload,
    portfolio_tracking_review_payload_date,
    validate_portfolio_tracking_review_payload,
    write_portfolio_tracking_review_report_alias,
)
from ai_trading_system.trading_engine.portfolio_turnover_attribution import (
    load_portfolio_turnover_attribution_payload,
    portfolio_turnover_attribution_payload_date,
    validate_portfolio_turnover_attribution_payload,
    write_portfolio_turnover_attribution_report_alias,
)
from ai_trading_system.trading_engine.reports.parameter_promotion_report import (
    default_parameter_promotion_json_path,
    load_parameter_promotion_payload,
    write_parameter_promotion_report_alias,
)
from ai_trading_system.trading_engine.reports.shadow_backtest_report import (
    load_shadow_backtest_payload,
    validate_shadow_backtest_payload,
    write_shadow_backtest_report_alias,
)
from ai_trading_system.trading_engine.signal_ablation import (
    default_signal_ablation_json_path,
    default_signal_ablation_root,
    latest_signal_ablation_path,
    load_signal_ablation_payload,
    signal_ablation_payload_date,
    validate_signal_ablation_payload,
    write_signal_ablation_report_alias,
)
from ai_trading_system.trading_engine.signal_calibration import (
    default_signal_calibration_json_path,
    default_signal_calibration_root,
    latest_signal_calibration_path,
    load_signal_calibration_payload,
    signal_calibration_payload_date,
    validate_signal_calibration_payload,
    write_signal_calibration_report_alias,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    default_signal_snapshot_json_path,
    default_signal_snapshot_root,
    latest_signal_snapshot_path,
    load_signal_snapshot_payload,
    signal_snapshot_summary,
    validate_signal_snapshot_payload,
    write_signal_snapshot_report_alias,
)

reports_app = typer.Typer(help="投资报告和周期复盘。", no_args_is_help=True)
console = Console()


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _resolve_signal_snapshot_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_signal_snapshot_root()
    if latest or as_of is None:
        latest_path = latest_signal_snapshot_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal snapshot artifact：{root}")
        return latest_path
    return default_signal_snapshot_json_path(root, _parse_date(as_of))


def _resolve_signal_ablation_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_signal_ablation_root()
    if latest or as_of is None:
        latest_path = latest_signal_ablation_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal ablation artifact：{root}")
        return latest_path
    return default_signal_ablation_json_path(root, _parse_date(as_of))


def _resolve_signal_calibration_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_signal_calibration_root()
    if latest or as_of is None:
        latest_path = latest_signal_calibration_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 signal calibration artifact：{root}")
        return latest_path
    return default_signal_calibration_json_path(root, _parse_date(as_of))


def _resolve_parameter_promotion_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "parameter_promotion"
    if latest or as_of is None:
        candidates = sorted(root.glob("*/parameter_promotion_decision.json"))
        if not candidates:
            raise typer.BadParameter(f"未找到 parameter promotion artifact：{root}")
        return max(candidates, key=lambda path: path.stat().st_mtime)
    return default_parameter_promotion_json_path(root, _parse_date(as_of))


def _shadow_backtest_payload_date(payload: dict[str, object], source_path: Path) -> date:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        run_id = str(metadata.get("run_id") or "")
        raw_date = run_id.removeprefix("shadow-backtest-")
        try:
            return date.fromisoformat(raw_date)
        except ValueError:
            pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"无法从 shadow backtest artifact 推断日期：{source_path}"
        raise typer.BadParameter(message) from exc


def _signal_snapshot_payload_date(payload: dict[str, object], source_path: Path) -> date:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        raw_date = str(metadata.get("as_of") or "")
        try:
            return date.fromisoformat(raw_date)
        except ValueError:
            snapshot_id = str(metadata.get("snapshot_id") or "")
            raw_date = snapshot_id.removeprefix("signal-snapshot-")
            try:
                return date.fromisoformat(raw_date)
            except ValueError:
                pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"无法从 signal snapshot artifact 推断日期：{source_path}"
        raise typer.BadParameter(message) from exc


def _parameter_promotion_payload_date(payload: dict[str, object], source_path: Path) -> date:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        run_id = str(metadata.get("run_id") or "")
        raw_date = run_id.removeprefix("shadow-backtest-")
        try:
            return date.fromisoformat(raw_date)
        except ValueError:
            pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"无法从 parameter promotion artifact 推断日期：{source_path}"
        raise typer.BadParameter(message) from exc


def _latest_decision_snapshot_path(snapshot_dir: Path) -> Path:
    candidates: list[tuple[date, Path]] = []
    for path in snapshot_dir.glob("decision_snapshot_*.json"):
        if not path.is_file():
            continue
        try:
            candidates.append((_decision_snapshot_date(path), path))
        except typer.BadParameter:
            continue
    if not candidates:
        raise typer.BadParameter(f"未找到可用 decision_snapshot：{snapshot_dir}")
    return max(candidates, key=lambda item: (item[0], item[1].name))[1]


def _decision_snapshot_date(path: Path) -> date:
    raw_date = path.stem.removeprefix("decision_snapshot_")
    try:
        return date.fromisoformat(raw_date)
    except ValueError:
        pass
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise typer.BadParameter(f"无法读取 decision_snapshot 日期：{path}") from exc
    if isinstance(payload, dict):
        signal_date = payload.get("signal_date") or payload.get("as_of")
        if signal_date:
            return _parse_date(str(signal_date))
    raise typer.BadParameter(f"decision_snapshot 文件名或内容缺少 YYYY-MM-DD 日期：{path}")


def _download_manifest_path(prices_path: Path) -> Path:
    return prices_path.parent / "download_manifest.csv"


def _marketstack_prices_path(prices_path: Path) -> Path:
    return prices_path.parent / "prices_marketstack_daily.csv"


def _requires_marketstack_prices(prices_path: Path) -> bool:
    default_prices_path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
    try:
        return prices_path.resolve() == default_prices_path.resolve()
    except OSError:
        return prices_path == default_prices_path


@reports_app.command("investment-review")
def investment_periodic_review_command(
    period: Annotated[
        str,
        typer.Option(help="复盘周期：weekly 或 monthly。"),
    ] = "weekly",
    as_of: Annotated[
        str | None,
        typer.Option(help="复盘截止日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    since: Annotated[
        str | None,
        typer.Option(help="复盘起始日期，格式为 YYYY-MM-DD；不传时按周期默认。"),
    ] = None,
    scores_path: Annotated[
        Path,
        typer.Option(help="scores_daily.csv 路径。"),
    ] = DEFAULT_SCORES_DAILY_PATH,
    decision_snapshot_path: Annotated[
        Path,
        typer.Option(help="decision snapshot 文件或目录。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    outcomes_path: Annotated[
        Path,
        typer.Option(help="decision_outcomes.csv 路径。"),
    ] = DEFAULT_DECISION_OUTCOMES_PATH,
    prediction_outcomes_path: Annotated[
        Path,
        typer.Option(help="prediction_outcomes.csv 路径，用于 production vs challenger 复盘。"),
    ] = DEFAULT_PREDICTION_OUTCOMES_PATH,
    learning_queue_path: Annotated[
        Path,
        typer.Option(help="decision learning queue JSON 路径。"),
    ] = DEFAULT_DECISION_LEARNING_QUEUE_PATH,
    rule_experiment_path: Annotated[
        Path,
        typer.Option(help="rule experiment ledger JSON 路径。"),
    ] = DEFAULT_RULE_EXPERIMENT_LEDGER_PATH,
    market_regime_id: Annotated[
        str,
        typer.Option(help="报告声明使用的市场阶段 id。"),
    ] = "ai_after_chatgpt",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 周报/月报复盘报告输出路径。"),
    ] = None,
) -> None:
    """生成周报/月报投资复盘报告。"""
    if period not in {"weekly", "monthly"}:
        raise typer.BadParameter("period 必须是 weekly 或 monthly")
    review_date = _parse_date(as_of) if as_of else date.today()
    since_date = _parse_date(since) if since else None
    report = build_periodic_investment_review_report(
        period=period,  # type: ignore[arg-type]
        as_of=review_date,
        since=since_date,
        market_regime_id=market_regime_id,
        scores_path=scores_path,
        decision_snapshot_path=decision_snapshot_path,
        outcomes_path=outcomes_path,
        prediction_outcomes_path=prediction_outcomes_path,
        learning_queue_path=learning_queue_path,
        rule_experiment_path=rule_experiment_path,
    )
    report_path = output_path or default_periodic_investment_review_report_path(
        DEFAULT_PERIODIC_INVESTMENT_REVIEW_REPORT_DIR,
        period,  # type: ignore[arg-type]
        review_date,
    )
    write_periodic_investment_review_report(report, report_path)
    style = "green" if report.status == "PASS" else "yellow"
    console.print(f"[{style}]投资复盘状态：{report.status}[/{style}]")
    console.print(f"报告：{report_path}")
    console.print(
        f"区间：{report.since.isoformat()} 至 {report.as_of.isoformat()}；"
        f"样本：{len(report.score_rows)}"
    )


@reports_app.command("calculation-explainers")
def calculation_explainers_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="计算解释日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    registry_path: Annotated[
        Path,
        typer.Option(help="metric explainer registry YAML 路径。"),
    ] = DEFAULT_METRIC_EXPLAINERS_CONFIG_PATH,
    scores_daily_path: Annotated[
        Path | None,
        typer.Option(help="scores_daily.csv 路径；不传时使用默认处理后评分缓存。"),
    ] = DEFAULT_SCORES_DAILY_PATH,
    output_path: Annotated[
        Path | None,
        typer.Option(help="calculation explainers JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读计算解释 JSON。"""
    report_date = _parse_date(as_of) if as_of else date.today()
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        report_date,
    )
    resolved_output_path = output_path or default_calculation_explainers_path(
        PROJECT_ROOT / "outputs" / "reports",
        report_date,
    )
    try:
        payload = build_calculation_explainers_payload(
            as_of=report_date,
            decision_snapshot_path=snapshot_path,
            registry_path=registry_path,
            scores_daily_path=scores_daily_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path = write_calculation_explainers_json(payload, resolved_output_path)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]计算解释：{payload['status']}[/{style}]")
    console.print(f"Calculation explainers JSON：{json_path}")
    console.print(
        f"metrics：{len(payload['metrics'])}；"
        f"warnings：{len(payload['warnings'])}；"
        f"production_effect={payload['production_effect']}"
    )


@reports_app.command("shadow-parameter-backtest")
def shadow_parameter_backtest_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 shadow backtest artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 shadow_backtest_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 shadow parameter backtest 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_shadow_backtest_summary_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_shadow_backtest_payload(json_source)
    issues = validate_shadow_backtest_payload(payload)
    if issues:
        raise typer.BadParameter("shadow backtest JSON 校验失败：" + "; ".join(issues))
    report_date = _shadow_backtest_payload_date(payload, json_source)
    json_path, markdown_path = write_shadow_backtest_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    console.print("[green]Shadow parameter backtest report：OK[/green]")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；不修改 production 参数")


@reports_app.command("parameter-promotion")
def parameter_promotion_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 parameter promotion artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 parameter_promotion_decision.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 parameter promotion decision 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_parameter_promotion_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_parameter_promotion_payload(json_source)
    if not payload:
        raise typer.BadParameter(f"无法读取 parameter promotion JSON：{json_source}")
    report_date = _parameter_promotion_payload_date(payload, json_source)
    json_path, markdown_path = write_parameter_promotion_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    decision = payload.get("promotion_decision", {})
    status = decision.get("status", "UNKNOWN") if isinstance(decision, dict) else "UNKNOWN"
    console.print("[green]Parameter promotion report：OK[/green]")
    console.print(f"promotion_status：{status}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("signal-snapshot")
def signal_snapshot_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 signal snapshot artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_snapshot.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 signal snapshot 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_signal_snapshot_path(latest=latest, as_of=as_of)
    payload = load_signal_snapshot_payload(json_source)
    issues = validate_signal_snapshot_payload(payload)
    if issues:
        raise typer.BadParameter("signal snapshot JSON 校验失败：" + "; ".join(issues))
    report_date = _signal_snapshot_payload_date(payload, json_source)
    json_path, markdown_path = write_signal_snapshot_report_alias(payload, reports_dir, report_date)
    summary = signal_snapshot_summary(payload)
    console.print("[green]Signal snapshot report：OK[/green]")
    console.print(f"status：{summary.get('status', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("signal-ablation")
def signal_ablation_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 signal ablation artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_ablation_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 signal ablation 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_signal_ablation_path(latest=latest, as_of=as_of)
    payload = load_signal_ablation_payload(json_source)
    issues = validate_signal_ablation_payload(payload)
    if issues:
        raise typer.BadParameter("signal ablation JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = signal_ablation_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_signal_ablation_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    summary = payload.get("summary", {}) if isinstance(payload, dict) else {}
    console.print("[green]Signal ablation report：OK[/green]")
    if isinstance(summary, dict):
        console.print(
            f"positive_signals={len(summary.get('positive_signals', []))}；"
            f"negative_signals={len(summary.get('negative_signals', []))}；"
            f"promotion_credit_signals={len(summary.get('promotion_credit_signals', []))}"
        )
        reason = summary.get("no_promotion_credit_reason")
        if reason:
            console.print(f"no_promotion_credit_reason={reason}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("signal-calibration")
def signal_calibration_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 signal calibration artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 signal_calibration_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 signal calibration 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_signal_calibration_path(latest=latest, as_of=as_of)
    payload = load_signal_calibration_payload(json_source)
    issues = validate_signal_calibration_payload(payload)
    if issues:
        raise typer.BadParameter("signal calibration JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = signal_calibration_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_signal_calibration_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    ranking = payload.get("ranking", {}) if isinstance(payload, dict) else {}
    console.print("[green]Signal calibration report：OK[/green]")
    if isinstance(ranking, dict):
        console.print(f"best_profile={ranking.get('best_profile', 'UNKNOWN')}")
        reason = ranking.get("reason")
        if reason:
            console.print(f"reason={reason}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-sensitivity")
def portfolio_sensitivity_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio sensitivity artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_sensitivity_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio sensitivity 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_sensitivity_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_sensitivity_payload(json_source)
    issues = validate_portfolio_sensitivity_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio sensitivity JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = portfolio_sensitivity_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_sensitivity_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    ranking = payload.get("ranking", {}) if isinstance(payload, dict) else {}
    diagnosis = payload.get("diagnosis", {}) if isinstance(payload, dict) else {}
    console.print("[green]Portfolio sensitivity report：OK[/green]")
    if isinstance(ranking, dict):
        console.print(f"best_profile={ranking.get('best_profile', 'UNKNOWN')}")
    if isinstance(diagnosis, dict):
        console.print(f"primary_bottleneck={diagnosis.get('primary_bottleneck', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-candidates")
def portfolio_candidates_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio candidates artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidates_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio candidates 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_candidates_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_candidates_payload(json_source)
    issues = validate_portfolio_candidates_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio candidates JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = portfolio_candidates_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_candidates_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    ranking = payload.get("ranking", {}) if isinstance(payload, dict) else {}
    console.print("[green]Portfolio candidates report：OK[/green]")
    if isinstance(ranking, dict):
        console.print(f"best_profile={ranking.get('best_profile', 'UNKNOWN')}")
        reason = ranking.get("reason")
        if reason:
            console.print(f"reason={reason}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-turnover-attribution")
def portfolio_turnover_attribution_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio turnover attribution artifact。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_turnover_attribution_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio turnover attribution 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_turnover_attribution_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_turnover_attribution_payload(json_source)
    issues = validate_portfolio_turnover_attribution_payload(payload)
    if issues:
        raise typer.BadParameter(
            "portfolio turnover attribution JSON 校验失败：" + "; ".join(issues)
        )
    try:
        report_date = portfolio_turnover_attribution_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_turnover_attribution_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    root_cause = payload.get("root_cause", {}) if isinstance(payload, dict) else {}
    candidate_summary = (
        payload.get("candidate_turnover_summary", {}) if isinstance(payload, dict) else {}
    )
    console.print("[green]Portfolio turnover attribution report：OK[/green]")
    if isinstance(root_cause, dict):
        console.print(f"root_cause_category={root_cause.get('category', 'mixed')}")
    if isinstance(candidate_summary, dict):
        console.print(f"failed_by_turnover={candidate_summary.get('total_failed_by_turnover', 0)}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-candidate-review")
def portfolio_candidate_review_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio candidate review decision。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidate_review_decision.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio candidate review 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    decision_source = source_path or _resolve_portfolio_candidate_review_decision_path(
        latest=latest,
        as_of=as_of,
    )
    decision_payload = load_portfolio_candidate_review_payload(decision_source)
    issues = validate_portfolio_candidate_review_decision_payload(decision_payload)
    if issues:
        raise typer.BadParameter("portfolio candidate review JSON 校验失败：" + "; ".join(issues))
    package_source = decision_source.parent / "portfolio_candidate_review_package.json"
    package_payload = load_portfolio_candidate_review_payload(package_source)
    if not package_payload:
        raise typer.BadParameter(f"未找到 review package artifact：{package_source}")
    try:
        report_date = portfolio_candidate_review_payload_date(
            decision_payload,
            decision_source,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_candidate_review_report_alias(
        package_payload,
        decision_payload,
        reports_dir,
        report_date,
    )
    decision = decision_payload.get("decision", {}) if isinstance(decision_payload, dict) else {}
    candidate = decision_payload.get("candidate", {}) if isinstance(decision_payload, dict) else {}
    console.print("[green]Portfolio candidate review report：OK[/green]")
    if isinstance(decision, dict):
        console.print(f"status={decision.get('status', 'UNKNOWN')}")
        console.print(f"allowed_next_step={decision.get('allowed_next_step', 'UNKNOWN')}")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-candidate-tracking")
def portfolio_candidate_tracking_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio candidate tracking summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_candidate_tracking_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio candidate tracking 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_candidate_tracking_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_candidate_tracking_payload(json_source)
    issues = validate_portfolio_candidate_tracking_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio candidate tracking JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = portfolio_candidate_tracking_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_candidate_tracking_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    candidate = payload.get("candidate", {}) if isinstance(payload, dict) else {}
    console.print("[green]Portfolio candidate tracking report：OK[/green]")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
        console.print(f"tracking_status={candidate.get('tracking_status', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("portfolio-tracking-review")
def portfolio_tracking_review_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 portfolio tracking review summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 portfolio_tracking_review_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 portfolio tracking review 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_portfolio_tracking_review_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_portfolio_tracking_review_payload(json_source)
    issues = validate_portfolio_tracking_review_payload(payload)
    if issues:
        raise typer.BadParameter("portfolio tracking review JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = portfolio_tracking_review_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_portfolio_tracking_review_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    recommendation = payload.get("recommendation", {}) if isinstance(payload, dict) else {}
    candidate = payload.get("candidate", {}) if isinstance(payload, dict) else {}
    console.print("[green]Portfolio tracking review report：OK[/green]")
    if isinstance(candidate, dict):
        console.print(f"candidate_profile={candidate.get('profile_name', 'UNKNOWN')}")
        console.print(f"tracking_days={candidate.get('tracking_days', 0)}")
    if isinstance(recommendation, dict):
        console.print(f"recommendation={recommendation.get('status', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("weight-tuning")
def weight_tuning_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 weight tuning summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_tuning_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 weight tuning 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_weight_tuning_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_tuning_payload(json_source)
    issues = validate_weight_tuning_payload(payload)
    if issues:
        raise typer.BadParameter("weight tuning JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = weight_tuning_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_weight_tuning_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    recommended = payload.get("recommended_candidate", {}) if isinstance(payload, dict) else {}
    search = payload.get("search", {}) if isinstance(payload, dict) else {}
    console.print("[green]Weight tuning report：OK[/green]")
    if isinstance(recommended, dict):
        console.print(f"weight_candidate_status={recommended.get('status', 'UNKNOWN')}")
    if isinstance(search, dict):
        console.print(f"candidates_evaluated={search.get('candidates_evaluated', 0)}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("weight-stability")
def weight_stability_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 weight stability summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_stability_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 weight stability 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_weight_stability_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_stability_payload(json_source)
    issues = validate_weight_stability_payload(payload)
    if issues:
        raise typer.BadParameter("weight stability JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = weight_stability_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_weight_stability_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    recommended = payload.get("recommended_candidate", {}) if isinstance(payload, dict) else {}
    search = payload.get("search_summary", {}) if isinstance(payload, dict) else {}
    console.print("[green]Weight stability report：OK[/green]")
    if isinstance(recommended, dict):
        console.print(f"stable_candidate_status={recommended.get('status', 'UNKNOWN')}")
    if isinstance(search, dict):
        console.print(f"candidates_backtested={search.get('candidates_backtested', 0)}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("weight-stability-readiness")
def weight_stability_readiness_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 weight stability readiness summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_stability_readiness_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 weight stability readiness 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_weight_stability_readiness_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_stability_readiness_payload(json_source)
    issues = validate_weight_stability_readiness_payload(payload)
    if issues:
        raise typer.BadParameter("weight stability readiness JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = weight_stability_readiness_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_weight_stability_readiness_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    eligibility = payload.get("stable_tuning_eligibility", {})
    console.print("[green]Weight stability readiness report：OK[/green]")
    if isinstance(eligibility, dict):
        console.print(
            f"readiness_status={eligibility.get('status', 'UNKNOWN')}；"
            f"can_run={str(eligibility.get('can_run', False)).lower()}"
        )
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("weight-tuning-failure")
def weight_tuning_failure_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 weight tuning failure attribution。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 weight_tuning_failure_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 weight tuning failure attribution 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_weight_tuning_failure_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_weight_tuning_failure_payload(json_source)
    issues = validate_weight_tuning_failure_payload(payload)
    if issues:
        raise typer.BadParameter("weight tuning failure JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = weight_tuning_failure_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_weight_tuning_failure_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    root_cause = payload.get("root_cause", {}) if isinstance(payload, dict) else {}
    rejection = payload.get("candidate_rejection_summary", {}) if isinstance(payload, dict) else {}
    console.print("[green]Weight tuning failure report：OK[/green]")
    if isinstance(root_cause, dict):
        console.print(f"root_cause_category={root_cause.get('category', 'mixed')}")
    if isinstance(rejection, dict):
        console.print(f"total_candidates={rejection.get('total_candidates', 0)}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("data-freshness")
def market_data_freshness_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 market data freshness summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 market_data_freshness_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 market data freshness 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_market_data_freshness_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_market_data_freshness_payload(json_source)
    issues = validate_market_data_freshness_payload(payload)
    if issues:
        raise typer.BadParameter("market data freshness JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = market_data_freshness_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_market_data_freshness_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    freshness = payload.get("freshness", {}) if isinstance(payload, dict) else {}
    readiness = payload.get("tracking_readiness", {}) if isinstance(payload, dict) else {}
    console.print("[green]Market data freshness report：OK[/green]")
    if isinstance(freshness, dict):
        console.print(f"freshness_status={freshness.get('status', 'UNKNOWN')}")
    if isinstance(readiness, dict):
        console.print(f"tracking_readiness={readiness.get('readiness', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("data-refresh")
def market_data_refresh_report_command(
    latest: Annotated[
        bool,
        typer.Option(help="读取最新正式 market data refresh summary。"),
    ] = False,
    as_of: Annotated[
        str | None,
        typer.Option("--date", "--as-of", help="报告日期，格式为 YYYY-MM-DD。"),
    ] = None,
    source_path: Annotated[
        Path | None,
        typer.Option(help="显式 market_data_refresh_summary.json 路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 alias 输出目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
) -> None:
    """从正式 artifact 生成 market data refresh 报告 alias。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --date/--as-of 同时使用")
    json_source = source_path or _resolve_market_data_refresh_path(
        latest=latest,
        as_of=as_of,
    )
    payload = load_market_data_refresh_payload(json_source)
    issues = validate_market_data_refresh_payload(payload)
    if issues:
        raise typer.BadParameter("market data refresh JSON 校验失败：" + "; ".join(issues))
    try:
        report_date = market_data_refresh_payload_date(payload, json_source)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    json_path, markdown_path = write_market_data_refresh_report_alias(
        payload,
        reports_dir,
        report_date,
    )
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    after = payload.get("after", {}) if isinstance(payload, dict) else {}
    console.print("[green]Market data refresh report：OK[/green]")
    if isinstance(metadata, dict):
        console.print(f"refresh_status={metadata.get('status', 'UNKNOWN')}")
    if isinstance(after, dict):
        console.print(f"freshness_status={after.get('freshness_status', 'UNKNOWN')}")
        console.print(f"tracking_status={after.get('candidate_tracking_status', 'UNKNOWN')}")
    console.print(f"JSON：{json_path}")
    console.print(f"Markdown：{markdown_path}")
    console.print("production_effect=none；manual_review_required=true；auto_promotion=false")


@reports_app.command("reader-brief")
def reader_brief_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Reader Brief 日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date snapshot。"),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="同日报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    calculation_explainers_path: Annotated[
        Path | None,
        typer.Option(help="calculation explainers JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    daily_decision_summary_path: Annotated[
        Path | None,
        typer.Option(help="daily_decision_summary JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    evidence_dashboard_json_path: Annotated[
        Path | None,
        typer.Option(help="evidence dashboard JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    daily_task_dashboard_json_path: Annotated[
        Path | None,
        typer.Option(help="daily task dashboard JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    daily_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 日报路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="日报 trace bundle JSON 路径；不传时按日报路径推导。"),
    ] = None,
    score_change_attribution_path: Annotated[
        Path | None,
        typer.Option(help="score_change_attribution JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    market_panel_path: Annotated[
        Path | None,
        typer.Option(help="market_panel JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    research_governance_summary_path: Annotated[
        Path | None,
        typer.Option(
            help="research_governance_summary JSON 路径；不传时按 as_of 使用默认报告路径。"
        ),
    ] = None,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="report_index JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    documentation_contract_path: Annotated[
        Path | None,
        typer.Option(help="documentation_contract JSON 路径；不传时按 as_of 使用默认报告路径。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief HTML 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读统一读者入口 HTML/JSON。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = decision_snapshot_path or _latest_decision_snapshot_path(
            DEFAULT_DECISION_SNAPSHOT_DIR
        )
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
            DEFAULT_DECISION_SNAPSHOT_DIR,
            report_date,
        )
    calc_path = calculation_explainers_path or default_calculation_explainers_path(
        reports_dir,
        report_date,
    )
    decision_summary_path = daily_decision_summary_path or default_daily_decision_summary_path(
        reports_dir,
        report_date,
    )
    dashboard_json_path = evidence_dashboard_json_path or default_evidence_dashboard_json_path(
        reports_dir,
        report_date,
    )
    task_dashboard_json_path = (
        daily_task_dashboard_json_path
        or default_daily_task_dashboard_json_path(
            reports_dir,
            report_date,
        )
    )
    daily_path = daily_report_path or default_daily_score_report_path(reports_dir, report_date)
    trace_path = trace_bundle_path or default_report_trace_bundle_path(daily_path)
    score_change_path = score_change_attribution_path or default_score_change_attribution_json_path(
        reports_dir, report_date
    )
    market_panel_json_path = market_panel_path or default_market_panel_json_path(
        reports_dir,
        report_date,
    )
    research_governance_path = (
        research_governance_summary_path
        or default_research_governance_summary_json_path(reports_dir, report_date)
    )
    index_path = report_index_path or default_report_index_json_path(reports_dir, report_date)
    docs_contract_path = documentation_contract_path or default_documentation_contract_json_path(
        reports_dir,
        report_date,
    )
    html_output = output_path or default_reader_brief_html_path(reports_dir, report_date)
    json_output = json_output_path or default_reader_brief_json_path(reports_dir, report_date)
    try:
        payload = build_reader_brief_payload(
            as_of=report_date,
            reports_dir=reports_dir,
            decision_snapshot_path=snapshot_path,
            calculation_explainers_path=calc_path,
            daily_decision_summary_path=decision_summary_path,
            evidence_dashboard_json_path=dashboard_json_path,
            daily_task_dashboard_json_path=task_dashboard_json_path,
            daily_report_path=daily_path,
            trace_bundle_path=trace_path,
            score_change_attribution_path=score_change_path,
            market_panel_path=market_panel_json_path,
            research_governance_summary_path=research_governance_path,
            report_index_path=index_path,
            documentation_contract_path=docs_contract_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    html_path = write_reader_brief_html(payload, html_output)
    json_path = write_reader_brief_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Reader Brief：{payload['status']}[/{style}]")
    console.print(f"Reader Brief HTML：{html_path}")
    console.print(f"Reader Brief JSON：{json_path}")
    console.print(
        f"warnings：{len(payload['warnings'])}；"
        f"production_effect={payload['production_effect']}；"
        "不生成交易指令"
    )


@reports_app.command("validate-reader-brief")
def validate_reader_brief_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Reader Brief 质量校验日期，格式为 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(
            help="使用默认 decision snapshot 目录中的最新 signal-date，并校验对应 Reader Brief。"
        ),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="Reader Brief artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    reader_brief_json_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    reader_brief_html_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief HTML 路径；不传时按日期使用默认路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief quality JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief quality Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验既有 Reader Brief，并生成只读 quality report。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    source_json = reader_brief_json_path or default_reader_brief_json_path(
        reports_dir,
        report_date,
    )
    source_html = reader_brief_html_path or default_reader_brief_html_path(
        reports_dir,
        report_date,
    )
    if not source_json.exists():
        raise typer.BadParameter(f"Reader Brief JSON not found: {source_json}")
    raw = json.loads(source_json.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise typer.BadParameter(f"Reader Brief JSON must be an object: {source_json}")
    payload = build_reader_brief_quality_payload(
        reader_brief_payload=raw,
        reader_brief_json_path=source_json,
        reader_brief_html_path=source_html,
    )
    quality_json = json_output_path or default_reader_brief_quality_json_path(
        reports_dir,
        report_date,
    )
    quality_md = markdown_output_path or default_reader_brief_quality_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_reader_brief_quality_json(payload, quality_json)
    md_path = write_reader_brief_quality_markdown(payload, quality_md)
    style = "green" if payload["status"] == "OK" else "yellow"
    console.print(f"[{style}]Reader Brief quality：{payload['status']}[/{style}]")
    console.print(f"Reader Brief quality JSON：{json_path}")
    console.print(f"Reader Brief quality Markdown：{md_path}")
    console.print(
        f"checks：{payload['summary']['check_count']}；"
        f"failed：{payload['summary']['failed_check_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )


@reports_app.command("quality-gate")
def report_quality_gate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report quality gate 日期，格式为 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(
            help="使用默认 decision snapshot 目录中的最新 signal-date，并校验对应报告集合。"
        ),
    ] = False,
    reports_dir: Annotated[
        Path,
        typer.Option(help="报告 artifact 所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    project_root: Annotated[
        Path,
        typer.Option(help="用于解析 report index 中相对 artifact 路径的项目根目录。"),
    ] = PROJECT_ROOT,
    report_index_path: Annotated[
        Path | None,
        typer.Option(help="report_index JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    reader_brief_json_path: Annotated[
        Path | None,
        typer.Option(help="Reader Brief JSON 路径；不传时按日期使用默认路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report quality gate JSON 输出路径。"),
    ] = None,
    markdown_output_path: Annotated[
        Path | None,
        typer.Option(help="Report quality gate Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 report / Reader Brief 的基础可读性 section，并生成只读 quality gate report。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    index_path = report_index_path or default_report_index_json_path(reports_dir, report_date)
    brief_json_path = reader_brief_json_path or default_reader_brief_json_path(
        reports_dir,
        report_date,
    )
    report_index_payload: dict[str, object]
    if index_path.exists():
        try:
            raw_index = json.loads(index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"report_index JSON cannot be parsed: {index_path}") from exc
        if not isinstance(raw_index, dict):
            raise typer.BadParameter(f"report_index JSON must be an object: {index_path}")
        report_index_payload = raw_index
    else:
        report_index_payload = {
            "schema_version": 1,
            "report_type": "report_index",
            "as_of": report_date.isoformat(),
            "status": "MISSING",
            "production_effect": "none",
            "reports": [],
            "summary": {"report_count": 0},
        }
    reader_brief_payload: dict[str, object] | None = None
    if brief_json_path.exists():
        try:
            raw_brief = json.loads(brief_json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(
                f"Reader Brief JSON cannot be parsed: {brief_json_path}"
            ) from exc
        if not isinstance(raw_brief, dict):
            raise typer.BadParameter(f"Reader Brief JSON must be an object: {brief_json_path}")
        reader_brief_payload = raw_brief
    payload = build_report_quality_gate_payload(
        as_of=report_date,
        report_index_payload=report_index_payload,
        report_index_path=index_path,
        reader_brief_payload=reader_brief_payload,
        reader_brief_json_path=brief_json_path,
        project_root=project_root,
    )
    quality_json = json_output_path or default_report_quality_gate_json_path(
        reports_dir,
        report_date,
    )
    quality_md = markdown_output_path or default_report_quality_gate_markdown_path(
        reports_dir,
        report_date,
    )
    json_path = write_report_quality_gate_json(payload, quality_json)
    md_path = write_report_quality_gate_markdown(payload, quality_md)
    style = "green" if payload["report_quality_status"] == "PASS" else "yellow"
    if payload["report_quality_status"] == "FAIL":
        style = "red"
    summary = payload["summary"]
    console.print(f"[{style}]Report quality gate：{payload['report_quality_status']}[/{style}]")
    console.print(f"Report quality gate JSON：{json_path}")
    console.print(f"Report quality gate Markdown：{md_path}")
    console.print(
        f"checked_reports：{summary['checked_report_count']}；"
        f"missing_sections：{summary['missing_section_count']}；"
        f"blocking：{summary['blocking_quality_issue_count']}；"
        f"warnings：{summary['warning_quality_issue_count']}；"
        f"production_effect={payload['production_effect']}；只读校验"
    )


@reports_app.command("score-change-attribution")
def score_change_attribution_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="变化归因日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date snapshot。"),
    ] = False,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="当前 decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    previous_decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="上一条 decision snapshot JSON 路径；不传时从 snapshot 目录自动发现。"),
    ] = None,
    snapshot_dir: Annotated[
        Path,
        typer.Option(help="用于自动发现上一条 decision snapshot 的目录。"),
    ] = DEFAULT_DECISION_SNAPSHOT_DIR,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Score change attribution Markdown 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Score change attribution JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读 score change attribution 报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        snapshot_path = decision_snapshot_path or _latest_decision_snapshot_path(snapshot_dir)
        report_date = _decision_snapshot_date(snapshot_path)
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
        snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
            snapshot_dir,
            report_date,
        )
    reports_dir = PROJECT_ROOT / "outputs" / "reports"
    markdown_output = output_path or default_score_change_attribution_report_path(
        reports_dir,
        report_date,
    )
    json_output = json_output_path or default_score_change_attribution_json_path(
        reports_dir,
        report_date,
    )
    try:
        payload = build_score_change_attribution_payload(
            as_of=report_date,
            decision_snapshot_path=snapshot_path,
            previous_decision_snapshot_path=previous_decision_snapshot_path,
            snapshot_dir=snapshot_dir,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    report_path = write_score_change_attribution_report(payload, markdown_output)
    json_path = write_score_change_attribution_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Score change attribution：{payload['status']}[/{style}]")
    console.print(f"Score change attribution report：{report_path}")
    console.print(f"Score change attribution JSON：{json_path}")
    console.print(
        f"warnings：{len(payload['warnings'])}；"
        f"production_effect={payload['production_effect']}；"
        "不重算 score"
    )


@reports_app.command("market-panel")
def market_panel_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Market panel 日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    prices_path: Annotated[
        Path,
        typer.Option(help="标准化日线价格 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="标准化 FRED 宏观序列 CSV 路径。"),
    ] = PROJECT_ROOT
    / "data"
    / "raw"
    / "rates_daily.csv",
    output_path: Annotated[
        Path | None,
        typer.Option(help="Market panel Markdown 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Market panel JSON 输出路径。"),
    ] = None,
    data_quality_output_path: Annotated[
        Path | None,
        typer.Option(help="数据质量 Markdown 输出路径；不传时按日期使用默认报告路径。"),
    ] = None,
) -> None:
    """生成只读 market price panel 报告。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    reports_dir = PROJECT_ROOT / "outputs" / "reports"
    markdown_output = output_path or default_market_panel_report_path(reports_dir, report_date)
    json_output = json_output_path or default_market_panel_json_path(reports_dir, report_date)
    quality_report_path = data_quality_output_path or default_quality_report_path(
        reports_dir,
        report_date,
    )
    quality_config = load_data_quality()
    quality_report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["SPY", "QQQ", "SMH", "SOXX", "^VIX"],
        expected_rate_series=["DGS10"],
        quality_config=quality_config,
        as_of=report_date,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(quality_report, quality_report_path)
    if not quality_report.passed:
        payload = build_market_panel_payload(
            as_of=report_date,
            prices_path=prices_path,
            rates_path=rates_path,
            data_quality_status=quality_report.status,
            data_quality_report_path=quality_report_path,
            read_cached_data=False,
        )
        report_path = write_market_panel_report(payload, markdown_output)
        json_path = write_market_panel_json(payload, json_output)
        console.print("[red]Market panel 数据质量状态：FAIL[/red]")
        console.print(f"数据质量报告：{quality_report_path}")
        console.print(f"Market panel report：{report_path}")
        console.print(f"Market panel JSON：{json_path}")
        raise typer.Exit(code=1)
    payload = build_market_panel_payload(
        as_of=report_date,
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_status=quality_report.status,
        data_quality_report_path=quality_report_path,
    )
    report_path = write_market_panel_report(payload, markdown_output)
    json_path = write_market_panel_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Market panel：{payload['status']}[/{style}]")
    console.print(f"Market panel report：{report_path}")
    console.print(f"Market panel JSON：{json_path}")
    console.print(
        f"available：{payload['summary']['available_proxy_count']}/"
        f"{payload['summary']['proxy_count']}；"
        f"production_effect={payload['production_effect']}；只读市场面板"
    )


@reports_app.command("research-governance-summary")
def research_governance_summary_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="Research governance summary 日期，格式为 YYYY-MM-DD，默认今天。",
        ),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance summary Markdown 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Research governance summary JSON 输出路径。"),
    ] = None,
    project_root: Annotated[
        Path,
        typer.Option(help="用于发现 research / shadow / governance artifacts 的项目根目录。"),
    ] = PROJECT_ROOT,
) -> None:
    """生成只读 research governance summary。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    reports_dir = project_root / "outputs" / "reports"
    markdown_output = output_path or default_research_governance_summary_report_path(
        reports_dir,
        report_date,
    )
    json_output = json_output_path or default_research_governance_summary_json_path(
        reports_dir,
        report_date,
    )
    payload = build_research_governance_summary_payload(
        as_of=report_date,
        project_root=project_root,
    )
    report_path = write_research_governance_summary_report(payload, markdown_output)
    json_path = write_research_governance_summary_json(payload, json_output)
    style = "green" if payload["governance_status"] == "OK" else "yellow"
    console.print(f"[{style}]Research governance summary：{payload['governance_status']}[/{style}]")
    console.print(f"Research governance summary report：{report_path}")
    console.print(f"Research governance summary JSON：{json_path}")
    console.print(
        f"cards：{payload['summary']['card_count']}；"
        f"manual_review：{len(payload['manual_review_queue'])}；"
        f"promotion_status={payload['promotion_status']}；"
        f"production_effect={payload['production_effect']}；"
        "只读汇总"
    )


@reports_app.command("index")
def report_index_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="Report index 日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(help="使用默认 decision snapshot 目录中的最新 signal-date。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="report_registry.yaml 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Annotated[
        Path,
        typer.Option(help="report index visibility waiver YAML 路径。"),
    ] = DEFAULT_REPORT_INDEX_WAIVER_PATH,
    project_root: Annotated[
        Path,
        typer.Option(help="用于扫描 report artifacts 的项目根目录。"),
    ] = PROJECT_ROOT,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Report index HTML 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Report index JSON 输出路径。"),
    ] = None,
) -> None:
    """生成只读报告 registry / cadence index。"""
    if latest and as_of:
        raise typer.BadParameter("--latest 不能和 --as-of/--date 同时使用")
    if latest:
        report_date = _decision_snapshot_date(
            _latest_decision_snapshot_path(DEFAULT_DECISION_SNAPSHOT_DIR)
        )
    else:
        report_date = _parse_date(as_of) if as_of else date.today()
    reports_dir = project_root / "outputs" / "reports"
    html_output = output_path or default_report_index_html_path(reports_dir, report_date)
    json_output = json_output_path or default_report_index_json_path(reports_dir, report_date)
    try:
        payload = build_report_index_payload(
            as_of=report_date,
            project_root=project_root,
            registry_path=registry_path,
            waiver_path=waiver_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    html_path = write_report_index_html(payload, html_output)
    json_path = write_report_index_json(payload, json_output)
    style = "green" if payload["status"] == "PASS" else "yellow"
    console.print(f"[{style}]Report index：{payload['status']}[/{style}]")
    console.print(f"Report index HTML：{html_path}")
    console.print(f"Report index JSON：{json_path}")
    console.print(
        f"reports：{payload['summary']['report_count']}；"
        f"missing：{payload['summary']['missing_count']}；"
        f"stale：{payload['summary']['stale_count']}；"
        f"waived：{payload['summary']['explicit_waiver_count']}；"
        f"unwaived：{payload['summary']['unwaived_warning_count']}；"
        f"production_effect={payload['production_effect']}；"
        "只读扫描"
    )


@reports_app.command("dashboard")
def evidence_dashboard_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="Dashboard 评估日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    daily_report_path: Annotated[
        Path | None,
        typer.Option(help="Markdown 日报路径；不传时按 as_of 使用默认日报路径。"),
    ] = None,
    trace_bundle_path: Annotated[
        Path | None,
        typer.Option(help="日报 evidence bundle JSON 路径；不传时按日报路径推导。"),
    ] = None,
    decision_snapshot_path: Annotated[
        Path | None,
        typer.Option(help="decision snapshot JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    belief_state_path: Annotated[
        Path | None,
        typer.Option(help="belief_state JSON 路径；不传时从 decision snapshot 读取。"),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="HTML dashboard 输出路径。"),
    ] = None,
    alerts_report_path: Annotated[
        Path | None,
        typer.Option(help="alerts Markdown 路径；不传时按 as_of 使用默认告警报告路径。"),
    ] = None,
    scores_daily_path: Annotated[
        Path | None,
        typer.Option(help="scores_daily.csv 路径；不传时使用默认处理后评分缓存。"),
    ] = None,
    market_feedback_report_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "market_feedback_optimization Markdown 路径；不传且使用默认日报路径时，"
                "若同日默认报告存在则接入。"
            ),
        ),
    ] = None,
    feedback_loop_review_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "feedback_loop_review Markdown 路径；不传且使用默认日报路径时，"
                "若同日默认报告存在则接入。"
            ),
        ),
    ] = None,
    investment_review_path: Annotated[
        Path | None,
        typer.Option(
            help=(
                "investment weekly/monthly review Markdown 路径；不传且使用默认日报路径时，"
                "若同日 weekly 默认报告存在则接入。"
            ),
        ),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="Dashboard JSON payload 输出路径；不传时写入默认同名 JSON。"),
    ] = None,
) -> None:
    """生成只读 evidence-first HTML dashboard。"""
    dashboard_date = _parse_date(as_of) if as_of else date.today()
    daily_path = daily_report_path or default_daily_score_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        dashboard_date,
    )
    trace_path = trace_bundle_path or default_report_trace_bundle_path(daily_path)
    snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        DEFAULT_DECISION_SNAPSHOT_DIR,
        dashboard_date,
    )
    dashboard_output = output_path or default_evidence_dashboard_path(
        PROJECT_ROOT / "outputs" / "reports",
        dashboard_date,
    )
    alert_path = alerts_report_path or default_alert_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        dashboard_date,
    )
    scores_path = scores_daily_path or DEFAULT_SCORES_DAILY_PATH
    market_feedback_path = market_feedback_report_path
    loop_review_path = feedback_loop_review_path
    periodic_review_path = investment_review_path
    if daily_report_path is None:
        default_market_feedback_path = default_market_feedback_optimization_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            dashboard_date,
        )
        default_loop_review_path = default_feedback_loop_review_report_path(
            PROJECT_ROOT / "outputs" / "reports",
            dashboard_date,
        )
        default_periodic_review_path = default_periodic_investment_review_report_path(
            DEFAULT_PERIODIC_INVESTMENT_REVIEW_REPORT_DIR,
            "weekly",
            dashboard_date,
        )
        market_feedback_path = market_feedback_path or (
            default_market_feedback_path if default_market_feedback_path.exists() else None
        )
        loop_review_path = loop_review_path or (
            default_loop_review_path if default_loop_review_path.exists() else None
        )
        periodic_review_path = periodic_review_path or (
            default_periodic_review_path if default_periodic_review_path.exists() else None
        )
    dashboard_json_output = json_output_path or (
        default_evidence_dashboard_json_path(PROJECT_ROOT / "outputs" / "reports", dashboard_date)
        if output_path is None
        else dashboard_output.with_suffix(".json")
    )
    try:
        report = build_evidence_dashboard_report(
            as_of=dashboard_date,
            daily_report_path=daily_path,
            trace_bundle_path=trace_path,
            decision_snapshot_path=snapshot_path,
            belief_state_path=belief_state_path,
            alerts_report_path=alert_path,
            scores_daily_path=scores_path,
            market_feedback_report_path=market_feedback_path,
            feedback_loop_review_path=loop_review_path,
            investment_review_path=periodic_review_path,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    dashboard_path = write_evidence_dashboard(report, dashboard_output)
    dashboard_json_path = write_evidence_dashboard_json(report, dashboard_json_output)
    style = "green" if report.status == "PASS" else "yellow"
    claim_count = len(report.trace_bundle.get("claims", []))
    dataset_count = len(report.trace_bundle.get("dataset_refs", []))
    console.print(f"[{style}]Evidence dashboard：{report.status}[/{style}]")
    console.print(f"Dashboard：{dashboard_path}")
    console.print(f"Dashboard JSON：{dashboard_json_path}")
    console.print(
        f"核心 claim：{claim_count}；"
        f"输入 dataset：{dataset_count}；"
        f"警告：{len(report.warnings)}"
    )


@reports_app.command("daily-tasks")
def daily_task_dashboard_command(
    as_of: Annotated[
        str | None,
        typer.Option(help="每日任务展示日期，格式为 YYYY-MM-DD，默认今天。"),
    ] = None,
    metadata_path: Annotated[
        Path | None,
        typer.Option(help="daily_ops_run_metadata JSON 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    run_report_path: Annotated[
        Path | None,
        typer.Option(help="daily_ops_run Markdown 路径；不传时按 as_of 使用默认路径。"),
    ] = None,
    reports_dir: Annotated[
        Path,
        typer.Option(help="同日子任务报告所在目录。"),
    ] = PROJECT_ROOT
    / "outputs"
    / "reports",
    output_path: Annotated[
        Path | None,
        typer.Option(help="每日任务 HTML dashboard 输出路径。"),
    ] = None,
    json_output_path: Annotated[
        Path | None,
        typer.Option(help="每日任务 JSON payload 输出路径。"),
    ] = None,
    decision_summary_output_path: Annotated[
        Path | None,
        typer.Option(help="每日决策总线 JSON 输出路径。"),
    ] = None,
    order_intent_candidates_output_path: Annotated[
        Path | None,
        typer.Option(help="Order intent candidate JSON 输出路径。"),
    ] = None,
    paper_trading_trend_days: Annotated[
        int,
        typer.Option(help="Paper Trading Trend 读取窗口；可选 7、14 或 30。"),
    ] = 7,
) -> None:
    """生成 daily-run 子任务总控展示页。"""
    dashboard_date = _parse_date(as_of) if as_of else date.today()
    resolved_metadata_path = metadata_path or default_daily_ops_run_metadata_path(
        reports_dir,
        dashboard_date,
    )
    resolved_run_report_path = run_report_path or default_daily_ops_run_report_path(
        reports_dir,
        dashboard_date,
    )
    dashboard_output = output_path or default_daily_task_dashboard_path(
        reports_dir,
        dashboard_date,
    )
    dashboard_json_output = json_output_path or (
        default_daily_task_dashboard_json_path(reports_dir, dashboard_date)
        if output_path is None
        else dashboard_output.with_suffix(".json")
    )
    decision_summary_output = decision_summary_output_path or default_daily_decision_summary_path(
        reports_dir,
        dashboard_date,
    )
    order_intent_candidates_output = (
        order_intent_candidates_output_path
        or default_order_intent_candidates_path(reports_dir, dashboard_date)
    )
    try:
        report = build_daily_task_dashboard_report(
            as_of=dashboard_date,
            metadata_path=resolved_metadata_path,
            run_report_path=resolved_run_report_path if resolved_run_report_path.exists() else None,
            reports_dir=reports_dir,
            paper_trading_trend_days=paper_trading_trend_days,
        )
    except FileNotFoundError as exc:
        raise typer.BadParameter(str(exc)) from exc
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    html_path = write_daily_task_dashboard(report, dashboard_output)
    json_path = write_daily_task_dashboard_json(report, dashboard_json_output)
    decision_summary_path = write_daily_decision_summary_json(
        report,
        decision_summary_output,
    )
    order_intent_candidates_path = write_order_intent_candidates_json(
        as_of=dashboard_date,
        daily_decision_summary_path=decision_summary_path,
        output_path=order_intent_candidates_output,
        project_root=report.project_root,
    )
    style = (
        "green"
        if report.status == "PASS"
        else "yellow" if report.status == "PASS_WITH_SKIPS" else "red"
    )
    console.print(f"[{style}]每日任务展示：{report.status}[/{style}]")
    console.print(f"Dashboard：{html_path}")
    console.print(f"Dashboard JSON：{json_path}")
    console.print(f"Daily decision summary JSON：{decision_summary_path}")
    console.print(f"Order intent candidates JSON：{order_intent_candidates_path}")
    console.print(
        f"步骤：{len(report.tasks)}；失败：{report.failed_count}；"
        f"跳过：{report.skipped_count}；风险/限制：{report.risk_count}"
    )
