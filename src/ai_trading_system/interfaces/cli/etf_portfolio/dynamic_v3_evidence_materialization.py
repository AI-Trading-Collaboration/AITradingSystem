from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_control as benchmark_baseline_control,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_benchmark_baseline_metrics_materialization as baseline_metrics_materialization,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_candidate_regression_replay as candidate_regression_replay,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_cost_metrics_materialization as cost_metrics_materialization,
)
from ai_trading_system.etf_portfolio import dynamic_v3_cost_sensitivity as cost_sensitivity
from ai_trading_system.etf_portfolio import dynamic_v3_drawdown_casebook as drawdown_casebook
from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness as filtered_readiness,
)
from ai_trading_system.etf_portfolio import dynamic_v3_flip_rotation_casebook as flip_casebook
from ai_trading_system.etf_portfolio import dynamic_v3_metric_source_map as metric_source_map
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as paper_shadow_health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as paper_shadow_weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj as _mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_benchmark_baseline_control_app,
    dynamic_v3_benchmark_baseline_metrics_materialization_app,
    dynamic_v3_candidate_regression_replay_app,
    dynamic_v3_cost_sensitivity_metrics_materialization_app,
    dynamic_v3_cost_sensitivity_review_app,
    dynamic_v3_drawdown_event_casebook_app,
    dynamic_v3_flip_rotation_event_casebook_app,
    dynamic_v3_metric_source_map_app,
    dynamic_v3_rescue_app,
)


def _echo_cost_sensitivity_summary(
    *,
    manifest: Mapping[str, Any],
    review: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    typer.echo(f"review_id={review.get('review_id') or manifest.get('review_id')}")
    typer.echo(f"candidate={review.get('candidate') or manifest.get('candidate')}")
    typer.echo(f"cost_sensitivity_status={review.get('cost_sensitivity_status')}")
    typer.echo(f"policy_id={review.get('policy_id')}")
    typer.echo(f"policy_version={review.get('policy_version')}")
    typer.echo(f"turnover={review.get('turnover')}")
    typer.echo(f"gross_performance_proxy={review.get('gross_performance_proxy')}")
    typer.echo(f"gross_improvement_proxy={review.get('gross_improvement_proxy')}")
    typer.echo(f"worst_net_improvement_proxy={review.get('worst_net_improvement_proxy')}")
    typer.echo(f"high_cost_improvement_meaningful={review.get('high_cost_improvement_meaningful')}")
    typer.echo(f"blocking_reasons={','.join(_texts(review.get('blocking_reasons')))}")
    typer.echo(f"warnings={','.join(_texts(review.get('warnings')))}")
    typer.echo(f"next_required_action={review.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('cost_sensitivity_report_path')}")
    typer.echo("research_only=true")
    typer.echo("cost_sensitivity_review_only=true")
    typer.echo("execution_model_ready=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")
    typer.echo("paper_account_state_mutated=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("production_effect=none")


@dynamic_v3_cost_sensitivity_review_app.command("run")
def dynamic_v3_cost_sensitivity_review_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="cost-sensitivity review as-of date YYYY-MM-DD；省略时使用 source as_of。",
        ),
    ] = None,
    candidate_metrics_path: Annotated[
        Path | None,
        typer.Option(
            "--candidate-metrics-path",
            help="显式 candidate metrics JSON，需含 turnover / gross proxy。",
        ),
    ] = None,
    weekly_review_id: Annotated[
        str | None,
        typer.Option("--weekly-review-id", help="paper-shadow weekly review id；缺省 latest。"),
    ] = None,
    weekly_review_dir: Annotated[
        Path,
        typer.Option("--weekly-review-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_id: Annotated[
        str | None,
        typer.Option("--paper-shadow-health-id", "--health-id", help="paper-shadow health id。"),
    ] = None,
    paper_shadow_health_dir: Annotated[
        Path,
        typer.Option("--paper-shadow-health-dir", help="paper-shadow health artifact root。"),
    ] = paper_shadow_health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="cost sensitivity policy YAML。"),
    ] = cost_sensitivity.DEFAULT_COST_SENSITIVITY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cost-sensitivity review artifact root。"),
    ] = cost_sensitivity.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
) -> None:
    result = cost_sensitivity.run_cost_sensitivity_review(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate_metrics_path=candidate_metrics_path,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        paper_shadow_health_id=paper_shadow_health_id,
        paper_shadow_health_dir=paper_shadow_health_dir,
        config_path=config_path,
        output_dir=output_dir,
    )
    _echo_cost_sensitivity_summary(
        manifest=_mapping_obj(result.get("manifest")),
        review=_mapping_obj(result.get("cost_sensitivity_review")),
        validation=_mapping_obj(result.get("cost_sensitivity_validation")),
    )


@dynamic_v3_cost_sensitivity_review_app.command("report")
def dynamic_v3_cost_sensitivity_review_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    review_id: Annotated[
        str | None,
        typer.Option("--review-id", help="cost-sensitivity review artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cost-sensitivity review artifact root。"),
    ] = cost_sensitivity.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
) -> None:
    if not latest and not review_id:
        raise typer.BadParameter("--review-id or --latest is required")
    payload = cost_sensitivity.cost_sensitivity_report_payload(
        review_id=review_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_cost_sensitivity_summary(
        manifest=_mapping_obj(payload),
        review=_mapping_obj(payload.get("cost_sensitivity_review")),
        validation=_mapping_obj(payload.get("cost_sensitivity_validation")),
    )


@dynamic_v3_rescue_app.command("validate-cost-sensitivity-review")
def dynamic_v3_validate_cost_sensitivity_review_command(
    review_id: Annotated[
        str | None,
        typer.Option("--review-id", help="cost-sensitivity review artifact id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cost-sensitivity review artifact root。"),
    ] = cost_sensitivity.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
) -> None:
    resolved_id = review_id
    if latest:
        payload = cost_sensitivity.cost_sensitivity_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("review_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--review-id or --latest is required")
    _echo_validation_payload(
        cost_sensitivity.validate_cost_sensitivity_artifact(
            review_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_cost_metrics_materialization_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    metrics = _mapping_obj(report.get("materialized_metrics"))
    typer.echo(
        "materialization_id="
        f"{report.get('materialization_id') or manifest.get('materialization_id')}"
    )
    typer.echo(
        f"cost_metrics_materialization_status={report.get('cost_metrics_materialization_status')}"
    )
    typer.echo(f"candidate={report.get('candidate') or manifest.get('candidate')}")
    typer.echo(f"source_variant={report.get('source_variant') or manifest.get('source_variant')}")
    typer.echo(f"turnover={metrics.get('turnover')}")
    typer.echo(f"gross_performance_proxy={metrics.get('gross_performance_proxy')}")
    typer.echo(f"gross_improvement_proxy={metrics.get('gross_improvement_proxy')}")
    typer.echo(f"drawdown_proxy={metrics.get('drawdown_proxy')}")
    typer.echo(f"trade_rotation_count={metrics.get('trade_rotation_count')}")
    typer.echo(f"candidate_metrics_path={report.get('candidate_metrics_path')}")
    typer.echo(f"cost_sensitivity_review_id={report.get('cost_sensitivity_review_id')}")
    typer.echo(f"cost_sensitivity_status={report.get('cost_sensitivity_status')}")
    typer.echo(f"blocking_reasons={','.join(_texts(report.get('blocking_reasons')))}")
    typer.echo(f"warnings={','.join(_texts(report.get('warnings')))}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('cost_metrics_materialization_markdown_path')}")
    typer.echo("research_only=true")
    typer.echo("cost_metrics_materialization_only=true")
    typer.echo("execution_model_ready=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("production_effect=none")


@dynamic_v3_cost_sensitivity_metrics_materialization_app.command("run")
def dynamic_v3_cost_sensitivity_metrics_materialization_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="cost metrics materialization as-of date YYYY-MM-DD；省略时使用当前 UTC 日期。",
        ),
    ] = None,
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="governance candidate id。"),
    ] = filtered_readiness.TOP_FILTERED_CANDIDATE,
    source_variant: Annotated[
        str,
        typer.Option("--source-variant", help="source simulation variant with numeric metrics。"),
    ] = "limited_adjustment",
    sim_outcome_id: Annotated[
        str | None,
        typer.Option("--sim-outcome-id", help="backtest sim outcome id；缺省 latest。"),
    ] = None,
    sim_outcome_dir: Annotated[
        Path,
        typer.Option("--sim-outcome-dir", help="backtest sim outcome artifact root。"),
    ] = cost_metrics_materialization.sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    weekly_review_id: Annotated[
        str | None,
        typer.Option("--weekly-review-id", help="paper-shadow weekly review id；缺省 latest。"),
    ] = None,
    weekly_review_dir: Annotated[
        Path,
        typer.Option("--weekly-review-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_id: Annotated[
        str | None,
        typer.Option("--paper-shadow-health-id", "--health-id", help="paper-shadow health id。"),
    ] = None,
    paper_shadow_health_dir: Annotated[
        Path,
        typer.Option("--paper-shadow-health-dir", help="paper-shadow health artifact root。"),
    ] = paper_shadow_health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    cost_sensitivity_output_dir: Annotated[
        Path,
        typer.Option("--cost-sensitivity-output-dir", help="rerun cost review artifact root。"),
    ] = cost_sensitivity.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cost metrics materialization artifact root。"),
    ] = cost_metrics_materialization.DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
) -> None:
    result = cost_metrics_materialization.run_cost_metrics_materialization(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate=candidate,
        source_variant=source_variant,
        sim_outcome_id=sim_outcome_id,
        sim_outcome_dir=sim_outcome_dir,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        paper_shadow_health_id=paper_shadow_health_id,
        paper_shadow_health_dir=paper_shadow_health_dir,
        cost_sensitivity_output_dir=cost_sensitivity_output_dir,
        output_dir=output_dir,
    )
    _echo_cost_metrics_materialization_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("cost_metrics_materialization_report")),
        validation=_mapping_obj(result.get("cost_metrics_materialization_validation")),
    )


@dynamic_v3_cost_sensitivity_metrics_materialization_app.command("report")
def dynamic_v3_cost_sensitivity_metrics_materialization_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    materialization_id: Annotated[
        str | None,
        typer.Option("--materialization-id", help="cost metrics materialization id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cost metrics materialization artifact root。"),
    ] = cost_metrics_materialization.DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
) -> None:
    if not latest and not materialization_id:
        raise typer.BadParameter("--materialization-id or --latest is required")
    payload = cost_metrics_materialization.cost_metrics_materialization_report_payload(
        materialization_id=materialization_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_cost_metrics_materialization_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("cost_metrics_materialization_report")),
        validation=_mapping_obj(payload.get("cost_metrics_materialization_validation")),
    )


@dynamic_v3_rescue_app.command("validate-cost-sensitivity-metrics-materialization")
def dynamic_v3_validate_cost_sensitivity_metrics_materialization_command(
    materialization_id: Annotated[
        str | None,
        typer.Option("--materialization-id", help="cost metrics materialization id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="cost metrics materialization artifact root。"),
    ] = cost_metrics_materialization.DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
) -> None:
    resolved_id = materialization_id
    if latest:
        payload = cost_metrics_materialization.cost_metrics_materialization_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("materialization_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--materialization-id or --latest is required")
    _echo_validation_payload(
        cost_metrics_materialization.validate_cost_metrics_materialization_artifact(
            materialization_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_metric_source_map_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    summary = _mapping_obj(report.get("source_summary"))
    typer.echo(f"source_map_id={report.get('source_map_id') or manifest.get('source_map_id')}")
    typer.echo(f"metric_source_map_status={report.get('metric_source_map_status')}")
    typer.echo(f"candidate={report.get('candidate') or manifest.get('candidate')}")
    typer.echo(f"source_variant={report.get('source_variant') or manifest.get('source_variant')}")
    typer.echo(f"candidate_metric_count={summary.get('candidate_metric_count')}")
    typer.echo(f"baseline_metric_count={summary.get('baseline_metric_count')}")
    typer.echo(f"derivable_now_count={summary.get('derivable_now_count')}")
    typer.echo(f"missing_metric_count={summary.get('missing_metric_count')}")
    typer.echo("missing_metric_names=" + ",".join(_texts(summary.get("missing_metric_names"))))
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('metric_source_map_markdown_path')}")
    typer.echo("research_only=true")
    typer.echo("metric_source_map_only=true")
    typer.echo("cost_metrics_materialized=false")
    typer.echo("benchmark_metrics_materialized=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")
    typer.echo("paper_account_state_mutated=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("production_effect=none")


@dynamic_v3_metric_source_map_app.command("run")
def dynamic_v3_metric_source_map_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="metric source map as-of date YYYY-MM-DD。",
        ),
    ] = None,
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="governance candidate id。"),
    ] = filtered_readiness.TOP_FILTERED_CANDIDATE,
    source_variant: Annotated[
        str,
        typer.Option("--source-variant", help="source simulation variant for candidate metrics。"),
    ] = "limited_adjustment",
    sim_outcome_id: Annotated[
        str | None,
        typer.Option("--sim-outcome-id", help="backtest sim outcome id；缺省 latest。"),
    ] = None,
    sim_outcome_dir: Annotated[
        Path,
        typer.Option("--sim-outcome-dir", help="backtest sim outcome artifact root。"),
    ] = metric_source_map.sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache-path", help="cached ETF price path；source map 只读检查。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="metric source map artifact root。"),
    ] = metric_source_map.DEFAULT_METRIC_SOURCE_MAP_DIR,
) -> None:
    result = metric_source_map.run_metric_source_map(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate=candidate,
        source_variant=source_variant,
        sim_outcome_id=sim_outcome_id,
        sim_outcome_dir=sim_outcome_dir,
        price_cache_path=price_cache_path,
        output_dir=output_dir,
    )
    _echo_metric_source_map_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("metric_source_map_report")),
        validation=_mapping_obj(result.get("metric_source_map_validation")),
    )


@dynamic_v3_metric_source_map_app.command("report")
def dynamic_v3_metric_source_map_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    source_map_id: Annotated[
        str | None,
        typer.Option("--source-map-id", help="metric source map artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="metric source map artifact root。"),
    ] = metric_source_map.DEFAULT_METRIC_SOURCE_MAP_DIR,
) -> None:
    if not latest and not source_map_id:
        raise typer.BadParameter("--source-map-id or --latest is required")
    payload = metric_source_map.metric_source_map_report_payload(
        source_map_id=source_map_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_metric_source_map_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("metric_source_map_report")),
        validation=_mapping_obj(payload.get("metric_source_map_validation")),
    )


@dynamic_v3_rescue_app.command("validate-metric-source-map")
def dynamic_v3_validate_metric_source_map_command(
    source_map_id: Annotated[
        str | None,
        typer.Option("--source-map-id", help="metric source map artifact id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="metric source map artifact root。"),
    ] = metric_source_map.DEFAULT_METRIC_SOURCE_MAP_DIR,
) -> None:
    resolved_id = source_map_id
    if latest:
        payload = metric_source_map.metric_source_map_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("source_map_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--source-map-id or --latest is required")
    _echo_validation_payload(
        metric_source_map.validate_metric_source_map_artifact(
            source_map_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_benchmark_baseline_control_summary(
    *,
    manifest: Mapping[str, Any],
    pack: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    summary = _mapping_obj(pack.get("comparison_summary"))
    typer.echo(f"control_id={pack.get('control_id') or manifest.get('control_id')}")
    typer.echo(f"candidate={pack.get('candidate') or manifest.get('candidate')}")
    typer.echo(f"benchmark_baseline_status={pack.get('benchmark_baseline_status')}")
    typer.echo(f"policy_id={pack.get('policy_id')}")
    typer.echo(f"policy_version={pack.get('policy_version')}")
    typer.echo(f"baseline_count={pack.get('baseline_count')}")
    typer.echo(f"outperformed_baseline_count={summary.get('outperformed_baseline_count')}")
    typer.echo(f"underperformed_baseline_count={summary.get('underperformed_baseline_count')}")
    typer.echo(
        f"insufficient_metric_baseline_count={summary.get('insufficient_metric_baseline_count')}"
    )
    typer.echo(f"worst_baseline_delta={summary.get('worst_baseline_delta')}")
    typer.echo(f"best_baseline_delta={summary.get('best_baseline_delta')}")
    typer.echo(f"blocking_reasons={','.join(_texts(pack.get('blocking_reasons')))}")
    typer.echo(f"warnings={','.join(_texts(pack.get('warnings')))}")
    typer.echo(f"next_required_action={pack.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('benchmark_baseline_report_path')}")
    typer.echo("research_only=true")
    typer.echo("benchmark_control_pack_only=true")
    typer.echo("execution_model_ready=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")
    typer.echo("paper_account_state_mutated=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("production_effect=none")


@dynamic_v3_benchmark_baseline_control_app.command("run")
def dynamic_v3_benchmark_baseline_control_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="benchmark baseline control as-of date YYYY-MM-DD；省略时使用 source as_of。",
        ),
    ] = None,
    candidate_metrics_path: Annotated[
        Path | None,
        typer.Option(
            "--candidate-metrics-path",
            help="显式 candidate metrics JSON，需含 net_performance_proxy。",
        ),
    ] = None,
    baseline_metrics_path: Annotated[
        Path | None,
        typer.Option(
            "--baseline-metrics-path",
            help="显式 baseline metrics JSON，需含 baselines[].net_performance_proxy。",
        ),
    ] = None,
    weekly_review_id: Annotated[
        str | None,
        typer.Option("--weekly-review-id", help="paper-shadow weekly review id；缺省 latest。"),
    ] = None,
    weekly_review_dir: Annotated[
        Path,
        typer.Option("--weekly-review-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    cost_sensitivity_review_id: Annotated[
        str | None,
        typer.Option(
            "--cost-sensitivity-review-id",
            "--review-id",
            help="cost-sensitivity review artifact id；缺省 latest。",
        ),
    ] = None,
    cost_sensitivity_dir: Annotated[
        Path,
        typer.Option("--cost-sensitivity-dir", help="cost-sensitivity review artifact root。"),
    ] = cost_sensitivity.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="benchmark baseline control YAML。"),
    ] = benchmark_baseline_control.DEFAULT_BENCHMARK_BASELINE_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="benchmark baseline control artifact root。"),
    ] = benchmark_baseline_control.DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR,
) -> None:
    result = benchmark_baseline_control.run_benchmark_baseline_control_pack(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate_metrics_path=candidate_metrics_path,
        baseline_metrics_path=baseline_metrics_path,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        cost_sensitivity_review_id=cost_sensitivity_review_id,
        cost_sensitivity_dir=cost_sensitivity_dir,
        config_path=config_path,
        output_dir=output_dir,
    )
    _echo_benchmark_baseline_control_summary(
        manifest=_mapping_obj(result.get("manifest")),
        pack=_mapping_obj(result.get("benchmark_baseline_control_pack")),
        validation=_mapping_obj(result.get("benchmark_baseline_validation")),
    )


@dynamic_v3_benchmark_baseline_control_app.command("report")
def dynamic_v3_benchmark_baseline_control_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    control_id: Annotated[
        str | None,
        typer.Option("--control-id", help="benchmark baseline control artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="benchmark baseline control artifact root。"),
    ] = benchmark_baseline_control.DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR,
) -> None:
    if not latest and not control_id:
        raise typer.BadParameter("--control-id or --latest is required")
    payload = benchmark_baseline_control.benchmark_baseline_report_payload(
        control_id=control_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_benchmark_baseline_control_summary(
        manifest=_mapping_obj(payload),
        pack=_mapping_obj(payload.get("benchmark_baseline_control_pack")),
        validation=_mapping_obj(payload.get("benchmark_baseline_validation")),
    )


@dynamic_v3_rescue_app.command("validate-benchmark-baseline-control")
def dynamic_v3_validate_benchmark_baseline_control_command(
    control_id: Annotated[
        str | None,
        typer.Option("--control-id", help="benchmark baseline control artifact id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="benchmark baseline control artifact root。"),
    ] = benchmark_baseline_control.DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR,
) -> None:
    resolved_id = control_id
    if latest:
        payload = benchmark_baseline_control.benchmark_baseline_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("control_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--control-id or --latest is required")
    _echo_validation_payload(
        benchmark_baseline_control.validate_benchmark_baseline_artifact(
            control_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_benchmark_baseline_metrics_materialization_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    metric_statuses = _mapping_obj(report.get("required_metric_statuses"))
    summary = _mapping_obj(report.get("comparison_summary"))
    typer.echo(
        "materialization_id="
        f"{report.get('materialization_id') or manifest.get('materialization_id')}"
    )
    typer.echo(
        f"benchmark_baseline_metrics_status={report.get('benchmark_baseline_metrics_status')}"
    )
    typer.echo(f"candidate={report.get('candidate') or manifest.get('candidate')}")
    typer.echo(f"source_variant={report.get('source_variant') or manifest.get('source_variant')}")
    typer.echo(f"candidate_metric_status={metric_statuses.get('candidate')}")
    typer.echo(f"available_baseline_count={metric_statuses.get('available_baseline_count')}")
    typer.echo(f"missing_baseline_count={metric_statuses.get('missing_baseline_count')}")
    typer.echo(f"candidate_metrics_path={report.get('candidate_metrics_path')}")
    typer.echo(f"baseline_metrics_path={report.get('baseline_metrics_path')}")
    typer.echo(f"benchmark_baseline_control_id={report.get('benchmark_baseline_control_id')}")
    typer.echo(f"benchmark_baseline_status={report.get('benchmark_baseline_status')}")
    typer.echo(f"outperformed_baseline_count={summary.get('outperformed_baseline_count')}")
    typer.echo(f"underperformed_baseline_count={summary.get('underperformed_baseline_count')}")
    typer.echo(
        f"insufficient_metric_baseline_count={summary.get('insufficient_metric_baseline_count')}"
    )
    typer.echo(f"blocking_reasons={','.join(_texts(report.get('blocking_reasons')))}")
    typer.echo(f"warnings={','.join(_texts(report.get('warnings')))}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(
        f"report_path={manifest.get('benchmark_baseline_metrics_materialization_markdown_path')}"
    )
    typer.echo("research_only=true")
    typer.echo("benchmark_baseline_metrics_materialization_only=true")
    typer.echo("benchmark_comparison_live_signal=false")
    typer.echo("execution_model_ready=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")
    typer.echo("paper_account_state_mutated=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("production_effect=none")


@dynamic_v3_benchmark_baseline_metrics_materialization_app.command("run")
def dynamic_v3_benchmark_baseline_metrics_materialization_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help="benchmark baseline metrics materialization as-of date YYYY-MM-DD。",
        ),
    ] = None,
    candidate: Annotated[
        str,
        typer.Option("--candidate", help="governance candidate id。"),
    ] = filtered_readiness.TOP_FILTERED_CANDIDATE,
    source_variant: Annotated[
        str,
        typer.Option("--source-variant", help="source simulation variant for candidate metrics。"),
    ] = "limited_adjustment",
    sim_outcome_id: Annotated[
        str | None,
        typer.Option("--sim-outcome-id", help="backtest sim outcome id；缺省 latest。"),
    ] = None,
    sim_outcome_dir: Annotated[
        Path,
        typer.Option("--sim-outcome-dir", help="backtest sim outcome artifact root。"),
    ] = baseline_metrics_materialization.sim.DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    candidate_metrics_path: Annotated[
        Path | None,
        typer.Option("--candidate-metrics-path", help="显式 candidate cost metrics JSON。"),
    ] = None,
    candidate_cost_materialization_id: Annotated[
        str | None,
        typer.Option(
            "--candidate-cost-materialization-id",
            "--cost-materialization-id",
            help="TRADING-389 cost metrics materialization id；缺省 latest。",
        ),
    ] = None,
    candidate_cost_materialization_dir: Annotated[
        Path,
        typer.Option(
            "--candidate-cost-materialization-dir",
            "--cost-materialization-dir",
            help="cost metrics materialization artifact root。",
        ),
    ] = cost_metrics_materialization.DEFAULT_COST_METRICS_MATERIALIZATION_DIR,
    weekly_review_id: Annotated[
        str | None,
        typer.Option("--weekly-review-id", help="paper-shadow weekly review id；缺省 latest。"),
    ] = None,
    weekly_review_dir: Annotated[
        Path,
        typer.Option("--weekly-review-dir", help="paper-shadow weekly review artifact root。"),
    ] = paper_shadow_weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    cost_sensitivity_review_id: Annotated[
        str | None,
        typer.Option(
            "--cost-sensitivity-review-id",
            "--review-id",
            help="cost-sensitivity review id；缺省 latest。",
        ),
    ] = None,
    cost_sensitivity_dir: Annotated[
        Path,
        typer.Option("--cost-sensitivity-dir", help="cost-sensitivity review artifact root。"),
    ] = cost_sensitivity.DEFAULT_COST_SENSITIVITY_REVIEW_DIR,
    benchmark_baseline_output_dir: Annotated[
        Path,
        typer.Option("--benchmark-baseline-output-dir", help="rerun baseline control root。"),
    ] = benchmark_baseline_control.DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR,
    price_cache_path: Annotated[
        Path,
        typer.Option("--price-cache-path", help="cached ETF price path；会先跑 data gate。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_cache_path: Annotated[
        Path,
        typer.Option("--rates-cache-path", help="cached rates path；会先跑 data gate。"),
    ] = system_target.DEFAULT_RATES_CACHE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="baseline metrics materialization artifact root。"),
    ] = (baseline_metrics_materialization.DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR),
) -> None:
    result = baseline_metrics_materialization.run_benchmark_baseline_metrics_materialization(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        candidate=candidate,
        source_variant=source_variant,
        sim_outcome_id=sim_outcome_id,
        sim_outcome_dir=sim_outcome_dir,
        candidate_metrics_path=candidate_metrics_path,
        candidate_cost_materialization_id=candidate_cost_materialization_id,
        candidate_cost_materialization_dir=candidate_cost_materialization_dir,
        weekly_review_id=weekly_review_id,
        weekly_review_dir=weekly_review_dir,
        cost_sensitivity_review_id=cost_sensitivity_review_id,
        cost_sensitivity_dir=cost_sensitivity_dir,
        benchmark_baseline_output_dir=benchmark_baseline_output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        output_dir=output_dir,
    )
    _echo_benchmark_baseline_metrics_materialization_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("benchmark_baseline_metrics_materialization_report")),
        validation=_mapping_obj(
            result.get("benchmark_baseline_metrics_materialization_validation")
        ),
    )


@dynamic_v3_benchmark_baseline_metrics_materialization_app.command("report")
def dynamic_v3_benchmark_baseline_metrics_materialization_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    materialization_id: Annotated[
        str | None,
        typer.Option(
            "--materialization-id",
            help="benchmark baseline metrics materialization id。",
        ),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="baseline metrics materialization artifact root。"),
    ] = (baseline_metrics_materialization.DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR),
) -> None:
    if not latest and not materialization_id:
        raise typer.BadParameter("--materialization-id or --latest is required")
    payload = (
        baseline_metrics_materialization.benchmark_baseline_metrics_materialization_report_payload(
            materialization_id=materialization_id,
            latest=latest,
            output_dir=output_dir,
        )
    )
    _echo_benchmark_baseline_metrics_materialization_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("benchmark_baseline_metrics_materialization_report")),
        validation=_mapping_obj(
            payload.get("benchmark_baseline_metrics_materialization_validation")
        ),
    )


@dynamic_v3_rescue_app.command("validate-benchmark-baseline-metrics-materialization")
def dynamic_v3_validate_benchmark_baseline_metrics_materialization_command(
    materialization_id: Annotated[
        str | None,
        typer.Option(
            "--materialization-id",
            help="benchmark baseline metrics materialization id。",
        ),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="baseline metrics materialization artifact root。"),
    ] = (baseline_metrics_materialization.DEFAULT_BENCHMARK_BASELINE_METRICS_MATERIALIZATION_DIR),
) -> None:
    resolved_id = materialization_id
    if latest:
        report_payload = baseline_metrics_materialization.benchmark_baseline_metrics_materialization_report_payload  # noqa: E501
        payload = report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("materialization_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--materialization-id or --latest is required")
    _echo_validation_payload(
        baseline_metrics_materialization.validate_benchmark_baseline_metrics_materialization_artifact(
            materialization_id=resolved_id,
            output_dir=output_dir,
        )
    )


def _echo_candidate_regression_replay_summary(
    *,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
    validation: Mapping[str, Any],
) -> None:
    summary = _mapping_obj(report.get("comparison_summary"))
    typer.echo(f"replay_id={report.get('replay_id') or manifest.get('replay_id')}")
    typer.echo(f"candidate={report.get('candidate') or manifest.get('candidate')}")
    typer.echo(
        f"candidate_regression_replay_status={report.get('candidate_regression_replay_status')}"
    )
    typer.echo(f"policy_id={report.get('policy_id')}")
    typer.echo(f"policy_version={report.get('policy_version')}")
    typer.echo(f"expected_behavior_id={report.get('expected_behavior_id')}")
    typer.echo(f"comparison_count={summary.get('comparison_count')}")
    typer.echo(f"breaking_change_count={summary.get('breaking_change_count')}")
    typer.echo(f"acceptable_change_count={summary.get('acceptable_change_count')}")
    typer.echo(f"unchanged_count={summary.get('unchanged_count')}")
    typer.echo(f"blocking_reasons={','.join(_texts(report.get('blocking_reasons')))}")
    typer.echo(f"warnings={','.join(_texts(report.get('warnings')))}")
    typer.echo(f"next_required_action={report.get('next_required_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('candidate_regression_replay_markdown_path')}")
    typer.echo("research_only=true")
    typer.echo("regression_guard_only=true")
    typer.echo("strategy_behavior_changed=false")
    typer.echo("data_downloaded_by_replay=false")
    typer.echo("pipelines_executed_by_replay=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("order_ticket_generated=false")
    typer.echo("paper_account_state_mutated=false")
    typer.echo("not_official_target_weights=true")
    typer.echo("production_effect=none")


@dynamic_v3_candidate_regression_replay_app.command("run")
def dynamic_v3_candidate_regression_replay_run_command(
    as_of: Annotated[
        str | None,
        typer.Option(
            "--as-of",
            "--date",
            help=(
                "candidate regression replay as-of date YYYY-MM-DD；省略时使用 policy window end。"
            ),
        ),
    ] = None,
    current_behavior_path: Annotated[
        Path | None,
        typer.Option(
            "--current-behavior-path",
            help=(
                "显式 current candidate behavior JSON；"
                "省略时读取 latest benchmark baseline control。"
            ),
        ),
    ] = None,
    benchmark_baseline_control_id: Annotated[
        str | None,
        typer.Option(
            "--benchmark-baseline-control-id",
            "--control-id",
            help="benchmark baseline control artifact id；缺省 latest。",
        ),
    ] = None,
    benchmark_baseline_control_dir: Annotated[
        Path,
        typer.Option(
            "--benchmark-baseline-control-dir",
            help="benchmark baseline control artifact root。",
        ),
    ] = benchmark_baseline_control.DEFAULT_BENCHMARK_BASELINE_CONTROL_DIR,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="candidate regression replay YAML。"),
    ] = candidate_regression_replay.DEFAULT_CANDIDATE_REGRESSION_REPLAY_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate regression replay artifact root。"),
    ] = candidate_regression_replay.DEFAULT_CANDIDATE_REGRESSION_REPLAY_DIR,
) -> None:
    result = candidate_regression_replay.run_candidate_regression_replay(
        as_of=None if as_of is None else _parse_dynamic_v3_outcome_date(as_of, "--as-of"),
        current_behavior_path=current_behavior_path,
        benchmark_baseline_control_id=benchmark_baseline_control_id,
        benchmark_baseline_control_dir=benchmark_baseline_control_dir,
        config_path=config_path,
        output_dir=output_dir,
    )
    _echo_candidate_regression_replay_summary(
        manifest=_mapping_obj(result.get("manifest")),
        report=_mapping_obj(result.get("candidate_regression_replay_report")),
        validation=_mapping_obj(result.get("candidate_regression_replay_validation")),
    )


@dynamic_v3_candidate_regression_replay_app.command("report")
def dynamic_v3_candidate_regression_replay_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    replay_id: Annotated[
        str | None,
        typer.Option("--replay-id", help="candidate regression replay artifact id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate regression replay artifact root。"),
    ] = candidate_regression_replay.DEFAULT_CANDIDATE_REGRESSION_REPLAY_DIR,
) -> None:
    if not latest and not replay_id:
        raise typer.BadParameter("--replay-id or --latest is required")
    payload = candidate_regression_replay.candidate_regression_replay_report_payload(
        replay_id=replay_id,
        latest=latest,
        output_dir=output_dir,
    )
    _echo_candidate_regression_replay_summary(
        manifest=_mapping_obj(payload),
        report=_mapping_obj(payload.get("candidate_regression_replay_report")),
        validation=_mapping_obj(payload.get("candidate_regression_replay_validation")),
    )


@dynamic_v3_rescue_app.command("validate-candidate-regression-replay")
def dynamic_v3_validate_candidate_regression_replay_command(
    replay_id: Annotated[
        str | None,
        typer.Option("--replay-id", help="candidate regression replay artifact id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="candidate regression replay artifact root。"),
    ] = candidate_regression_replay.DEFAULT_CANDIDATE_REGRESSION_REPLAY_DIR,
) -> None:
    resolved_id = replay_id
    if latest:
        payload = candidate_regression_replay.candidate_regression_replay_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("replay_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--replay-id or --latest is required")
    _echo_validation_payload(
        candidate_regression_replay.validate_candidate_regression_replay_artifact(
            replay_id=resolved_id,
            output_dir=output_dir,
        )
    )

@dynamic_v3_drawdown_event_casebook_app.command("report")
def dynamic_v3_drawdown_event_casebook_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    casebook_run_id: Annotated[
        str | None,
        typer.Option("--casebook-run-id", help="drawdown event casebook run id。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="drawdown event casebook YAML。"),
    ] = drawdown_casebook.DEFAULT_DRAWDOWN_EVENT_CASEBOOK_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="drawdown event casebook artifact root。"),
    ] = drawdown_casebook.DEFAULT_DRAWDOWN_EVENT_CASEBOOK_DIR,
) -> None:
    if latest or casebook_run_id:
        payload = drawdown_casebook.drawdown_event_casebook_report_payload(
            casebook_run_id=casebook_run_id,
            latest=latest,
            output_dir=output_dir,
        )
        manifest = _mapping_obj(payload)
        casebook = _mapping_obj(payload.get("drawdown_event_casebook"))
        validation = _mapping_obj(payload.get("drawdown_event_casebook_validation"))
    else:
        result = drawdown_casebook.build_drawdown_event_casebook(
            config_path=config_path,
            output_dir=output_dir,
        )
        manifest = _mapping_obj(result.get("manifest"))
        casebook = _mapping_obj(result.get("drawdown_event_casebook"))
        validation = _mapping_obj(result.get("drawdown_event_casebook_validation"))
    typer.echo(f"casebook_run_id={manifest.get('casebook_run_id')}")
    typer.echo(f"drawdown_casebook_id={casebook.get('drawdown_casebook_id')}")
    typer.echo(f"event_count={casebook.get('event_count')}")
    typer.echo(f"worst_event={casebook.get('worst_event')}")
    typer.echo(f"regime_coverage={','.join(_texts(casebook.get('regime_coverage')))}")
    typer.echo(f"next_review_action={casebook.get('next_review_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('drawdown_event_casebook_report_path')}")
    typer.echo("drawdown_event_casebook_only=true")
    typer.echo("research_diagnostic_only=true")
    typer.echo("not_trading_signal=true")
    typer.echo("data_downloaded_by_casebook=false")
    typer.echo("pipelines_executed_by_casebook=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-drawdown-event-casebook")
def dynamic_v3_validate_drawdown_event_casebook_command(
    casebook_run_id: Annotated[
        str | None,
        typer.Option("--casebook-run-id", help="drawdown event casebook run id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="drawdown event casebook artifact root。"),
    ] = drawdown_casebook.DEFAULT_DRAWDOWN_EVENT_CASEBOOK_DIR,
) -> None:
    resolved_id = casebook_run_id
    if latest:
        payload = drawdown_casebook.drawdown_event_casebook_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("casebook_run_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--casebook-run-id or --latest is required")
    _echo_validation_payload(
        drawdown_casebook.validate_drawdown_event_casebook_artifact(
            casebook_run_id=resolved_id,
            output_dir=output_dir,
        )
    )


@dynamic_v3_flip_rotation_event_casebook_app.command("report")
def dynamic_v3_flip_rotation_event_casebook_report_command(
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    casebook_run_id: Annotated[
        str | None,
        typer.Option("--casebook-run-id", help="flip/rotation event casebook run id。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="flip/rotation event casebook YAML。"),
    ] = flip_casebook.DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="flip/rotation event casebook artifact root。"),
    ] = flip_casebook.DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_DIR,
) -> None:
    if latest or casebook_run_id:
        payload = flip_casebook.flip_rotation_event_casebook_report_payload(
            casebook_run_id=casebook_run_id,
            latest=latest,
            output_dir=output_dir,
        )
        manifest = _mapping_obj(payload)
        casebook = _mapping_obj(payload.get("flip_rotation_event_casebook"))
        validation = _mapping_obj(payload.get("flip_rotation_event_casebook_validation"))
    else:
        result = flip_casebook.build_flip_rotation_event_casebook(
            config_path=config_path,
            output_dir=output_dir,
        )
        manifest = _mapping_obj(result.get("manifest"))
        casebook = _mapping_obj(result.get("flip_rotation_event_casebook"))
        validation = _mapping_obj(result.get("flip_rotation_event_casebook_validation"))
    typer.echo(f"casebook_run_id={manifest.get('casebook_run_id')}")
    typer.echo(f"flip_rotation_casebook_id={casebook.get('flip_rotation_casebook_id')}")
    typer.echo(f"event_count={casebook.get('event_count')}")
    typer.echo(f"useful_flip_count={casebook.get('useful_flip_count')}")
    typer.echo(f"false_positive_count={casebook.get('false_positive_count')}")
    typer.echo(f"dominant_trigger_signal={casebook.get('dominant_trigger_signal')}")
    typer.echo(f"next_review_action={casebook.get('next_review_action')}")
    typer.echo(f"validation_status={validation.get('status', 'NOT_RUN')}")
    typer.echo(f"report_path={manifest.get('flip_rotation_event_casebook_report_path')}")
    typer.echo("flip_rotation_event_casebook_only=true")
    typer.echo("research_diagnostic_only=true")
    typer.echo("not_trading_signal=true")
    typer.echo("data_downloaded_by_casebook=false")
    typer.echo("pipelines_executed_by_casebook=false")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_rescue_app.command("validate-flip-rotation-event-casebook")
def dynamic_v3_validate_flip_rotation_event_casebook_command(
    casebook_run_id: Annotated[
        str | None,
        typer.Option("--casebook-run-id", help="flip/rotation event casebook run id。"),
    ] = None,
    latest: Annotated[bool, typer.Option("--latest/--no-latest", help="读取 latest。")] = False,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="flip/rotation event casebook artifact root。"),
    ] = flip_casebook.DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_DIR,
) -> None:
    resolved_id = casebook_run_id
    if latest:
        payload = flip_casebook.flip_rotation_event_casebook_report_payload(
            latest=True,
            output_dir=output_dir,
        )
        resolved_id = str(payload.get("casebook_run_id") or "")
    if not resolved_id:
        raise typer.BadParameter("--casebook-run-id or --latest is required")
    _echo_validation_payload(
        flip_casebook.validate_flip_rotation_event_casebook_artifact(
            casebook_run_id=resolved_id,
            output_dir=output_dir,
        )
    )

def _parse_dynamic_v3_outcome_date(value: str, option_name: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"{option_name} must use YYYY-MM-DD") from exc


def _echo_validation_payload(payload: Mapping[str, Any]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)

def _texts(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [str(item) for item in value if item not in (None, "")]
    return [str(value)] if value != "" else []
