from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any

import typer

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_experiment_factory as experiment_factory,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    mapping_obj as _mapping_obj,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_batch_experiment_app,
    dynamic_v3_experiment_matrix_app,
    dynamic_v3_experiment_triage_app,
    dynamic_v3_hypothesis_backlog_app,
    dynamic_v3_method_promotion_plan_app,
    dynamic_v3_rescue_app,
    dynamic_v3_top_variant_interpretation_app,
    dynamic_v3_variant_transform_app,
)


def _records_obj(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _texts(value: object) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    return [str(item) for item in value]


def _echo_validation_payload(payload: Mapping[str, Any]) -> None:
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    if payload["status"] != "PASS":
        for check in payload.get("checks", []):
            if isinstance(check, Mapping) and check.get("status") != "PASS":
                typer.echo(f"failed_check={check.get('name')}: {check.get('detail')}")
        raise typer.Exit(code=1)


@dynamic_v3_hypothesis_backlog_app.command("build")
def dynamic_v3_hypothesis_backlog_build_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="weight optimization hypothesis config。"),
    ] = experiment_factory.DEFAULT_WEIGHT_OPTIMIZATION_HYPOTHESIS_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="hypothesis backlog artifact root。"),
    ] = experiment_factory.DEFAULT_HYPOTHESIS_BACKLOG_DIR,
) -> None:
    """生成 TRADING-239 failure mode taxonomy 和 hypothesis backlog。"""
    result = experiment_factory.build_hypothesis_backlog(
        config_path=config_path,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    priority = result["hypothesis_priority_summary"]
    typer.echo(f"backlog_id={result['backlog_id']}")
    typer.echo(f"backlog_dir={result['backlog_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"failure_modes_count={manifest['failure_modes_count']}")
    typer.echo(f"hypotheses_count={manifest['hypotheses_count']}")
    typer.echo(
        "high_priority_hypotheses=" + ",".join(_texts(priority.get("high_priority_hypotheses")))
    )
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_hypothesis_backlog_app.command("report")
def dynamic_v3_hypothesis_backlog_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest hypothesis backlog。"),
    ] = False,
    backlog_id: Annotated[
        str | None,
        typer.Option("--backlog-id", help="hypothesis backlog id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="hypothesis backlog artifact root。"),
    ] = experiment_factory.DEFAULT_HYPOTHESIS_BACKLOG_DIR,
) -> None:
    """展示 TRADING-239 hypothesis backlog 摘要。"""
    payload = experiment_factory.hypothesis_backlog_report_payload(
        backlog_id=backlog_id,
        latest=latest,
        output_dir=output_dir,
    )
    priority = _mapping_obj(payload.get("hypothesis_priority_summary"))
    typer.echo(f"backlog_id={payload['backlog_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failure_modes_count={payload['failure_modes_count']}")
    typer.echo(f"hypotheses_count={payload['hypotheses_count']}")
    typer.echo(
        "recommended_for_experiment_matrix="
        + ",".join(_texts(priority.get("recommended_for_experiment_matrix")))
    )
    typer.echo(f"report_path={payload['hypothesis_backlog_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-hypothesis-backlog")
def dynamic_v3_validate_hypothesis_backlog_command(
    backlog_id: Annotated[str, typer.Option("--backlog-id", help="hypothesis backlog id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="hypothesis backlog artifact root。"),
    ] = experiment_factory.DEFAULT_HYPOTHESIS_BACKLOG_DIR,
) -> None:
    """校验 TRADING-239 hypothesis backlog artifact。"""
    payload = experiment_factory.validate_hypothesis_backlog_artifact(
        backlog_id=backlog_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_variant_transform_app.command("validate-spec")
def dynamic_v3_variant_transform_validate_spec_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="weight variant transform config。"),
    ] = experiment_factory.DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
) -> None:
    """校验 TRADING-240 lightweight variant transform config。"""
    payload = experiment_factory.validate_weight_variant_transform_spec_config(config_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_variant_transform_app.command("report-spec")
def dynamic_v3_variant_transform_report_spec_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="weight variant transform config。"),
    ] = experiment_factory.DEFAULT_WEIGHT_VARIANT_TRANSFORM_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="variant transform spec artifact root。"),
    ] = experiment_factory.DEFAULT_VARIANT_TRANSFORM_SPEC_DIR,
) -> None:
    """生成 TRADING-240 transform catalog/report。"""
    result = experiment_factory.build_variant_transform_spec_report(
        config_path=config_path,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"spec_id={result['spec_id']}")
    typer.echo(f"spec_dir={result['spec_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"transform_type_count={manifest['transform_type_count']}")
    typer.echo(f"report_path={manifest['variant_transform_spec_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-variant-transform-spec")
def dynamic_v3_validate_variant_transform_spec_command(
    spec_id: Annotated[str, typer.Option("--spec-id", help="variant transform spec id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="variant transform spec artifact root。"),
    ] = experiment_factory.DEFAULT_VARIANT_TRANSFORM_SPEC_DIR,
) -> None:
    """校验 TRADING-240 variant transform spec artifact。"""
    payload = experiment_factory.validate_variant_transform_spec_artifact(
        spec_id=spec_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_experiment_matrix_app.command("build")
def dynamic_v3_experiment_matrix_build_command(
    config_path: Annotated[
        Path,
        typer.Option("--config", "--config-path", help="weight experiment matrix config。"),
    ] = experiment_factory.DEFAULT_WEIGHT_EXPERIMENT_MATRIX_CONFIG_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="experiment matrix artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_MATRIX_DIR,
) -> None:
    """生成 TRADING-241 lightweight experiment matrix。"""
    result = experiment_factory.build_experiment_matrix(
        config_path=config_path,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"matrix_id={result['matrix_id']}")
    typer.echo(f"matrix_dir={result['matrix_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"variant_count={manifest['variant_count']}")
    typer.echo("families_covered=" + ",".join(_texts(manifest.get("families_covered"))))
    typer.echo("failure_modes_covered=" + ",".join(_texts(manifest.get("failure_modes_covered"))))
    typer.echo("not_formal_research_method=true")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_experiment_matrix_app.command("report")
def dynamic_v3_experiment_matrix_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest experiment matrix。"),
    ] = False,
    matrix_id: Annotated[
        str | None,
        typer.Option("--matrix-id", help="experiment matrix id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="experiment matrix artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_MATRIX_DIR,
) -> None:
    """展示 TRADING-241 experiment matrix 摘要。"""
    payload = experiment_factory.experiment_matrix_report_payload(
        matrix_id=matrix_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"matrix_id={payload['matrix_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variant_count={payload['variant_count']}")
    typer.echo("families_covered=" + ",".join(_texts(payload.get("families_covered"))))
    typer.echo("failure_modes_covered=" + ",".join(_texts(payload.get("failure_modes_covered"))))
    typer.echo(f"report_path={payload['experiment_matrix_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-experiment-matrix")
def dynamic_v3_validate_experiment_matrix_command(
    matrix_id: Annotated[str, typer.Option("--matrix-id", help="experiment matrix id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="experiment matrix artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_MATRIX_DIR,
) -> None:
    """校验 TRADING-241 experiment matrix artifact。"""
    payload = experiment_factory.validate_experiment_matrix_artifact(
        matrix_id=matrix_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_batch_experiment_app.command("run")
def dynamic_v3_batch_experiment_run_command(
    matrix_id: Annotated[str, typer.Option("--matrix-id", help="experiment matrix id。")],
    matrix_dir: Annotated[
        Path,
        typer.Option("--matrix-dir", help="experiment matrix artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_MATRIX_DIR,
    baseline_backfill_dir: Annotated[
        Path,
        typer.Option("--baseline-backfill-dir", help="paper-shadow backfill artifact root。"),
    ] = experiment_factory.DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="batch experiment artifact root。"),
    ] = experiment_factory.DEFAULT_BATCH_EXPERIMENT_DIR,
    price_cache_path: Annotated[
        Path | None,
        typer.Option("--price-cache-path", help="可选 cached price path。"),
    ] = None,
    rates_cache_path: Annotated[
        Path,
        typer.Option("--rates-cache-path", help="cached rates path。"),
    ] = experiment_factory.DEFAULT_RATES_CACHE_PATH,
) -> None:
    """运行 TRADING-242 batch lightweight backfill experiment。"""
    result = experiment_factory.run_batch_experiment(
        matrix_id=matrix_id,
        matrix_dir=matrix_dir,
        baseline_backfill_dir=baseline_backfill_dir,
        output_dir=output_dir,
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
    )
    manifest = result["manifest"]
    typer.echo(f"batch_id={result['batch_id']}")
    typer.echo(f"batch_dir={result['batch_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"variants_completed={manifest['variants_completed']}")
    typer.echo(f"data_quality_status={manifest['data_quality_status']}")
    typer.echo(f"date_range={manifest['date_start']}..{manifest['date_end']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_batch_experiment_app.command("report")
def dynamic_v3_batch_experiment_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest batch experiment。"),
    ] = False,
    batch_id: Annotated[
        str | None,
        typer.Option("--batch-id", help="batch experiment id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="batch experiment artifact root。"),
    ] = experiment_factory.DEFAULT_BATCH_EXPERIMENT_DIR,
) -> None:
    """展示 TRADING-242 batch experiment 摘要。"""
    payload = experiment_factory.batch_experiment_report_payload(
        batch_id=batch_id,
        latest=latest,
        output_dir=output_dir,
    )
    performance = _records_obj(payload.get("variant_performance_metrics"))
    stability = _records_obj(payload.get("variant_stability_metrics"))
    best_drawdown = (
        max(
            performance,
            key=lambda row: float(row.get("drawdown_delta_vs_limited", 0.0)),
        )
        if performance
        else {}
    )
    best_rolling = next(
        (row for row in stability if row.get("rolling_consistency_delta") == "IMPROVED"),
        stability[0] if stability else {},
    )
    typer.echo(f"batch_id={payload['batch_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"variants_completed={payload['variants_completed']}")
    typer.echo(f"best_drawdown_variant={best_drawdown.get('variant_id', 'MISSING')}")
    typer.echo(f"best_rolling_consistency_variant={best_rolling.get('variant_id', 'MISSING')}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['batch_experiment_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-batch-experiment")
def dynamic_v3_validate_batch_experiment_command(
    batch_id: Annotated[str, typer.Option("--batch-id", help="batch experiment id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="batch experiment artifact root。"),
    ] = experiment_factory.DEFAULT_BATCH_EXPERIMENT_DIR,
) -> None:
    """校验 TRADING-242 batch experiment artifact。"""
    payload = experiment_factory.validate_batch_experiment_artifact(
        batch_id=batch_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_experiment_triage_app.command("run")
def dynamic_v3_experiment_triage_run_command(
    batch_id: Annotated[str, typer.Option("--batch-id", help="batch experiment id。")],
    batch_dir: Annotated[
        Path,
        typer.Option("--batch-dir", help="batch experiment artifact root。"),
    ] = experiment_factory.DEFAULT_BATCH_EXPERIMENT_DIR,
    matrix_dir: Annotated[
        Path,
        typer.Option("--matrix-dir", help="experiment matrix artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_MATRIX_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="experiment triage artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_TRIAGE_DIR,
) -> None:
    """运行 TRADING-243 experiment triage gate。"""
    result = experiment_factory.run_experiment_triage(
        batch_id=batch_id,
        batch_dir=batch_dir,
        matrix_dir=matrix_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    summary = result["triage_summary"]
    typer.echo(f"triage_id={result['triage_id']}")
    typer.echo(f"triage_dir={result['triage_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"promote_count={summary['promote_count']}")
    typer.echo(f"keep_testing_count={summary['keep_testing_count']}")
    typer.echo(f"reject_count={summary['reject_count']}")
    typer.echo(f"top_variant={summary['top_variant']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_experiment_triage_app.command("report")
def dynamic_v3_experiment_triage_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest experiment triage。"),
    ] = False,
    triage_id: Annotated[
        str | None,
        typer.Option("--triage-id", help="experiment triage id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="experiment triage artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_TRIAGE_DIR,
) -> None:
    """展示 TRADING-243 experiment triage 摘要。"""
    payload = experiment_factory.experiment_triage_report_payload(
        triage_id=triage_id,
        latest=latest,
        output_dir=output_dir,
    )
    summary = _mapping_obj(payload.get("triage_summary"))
    promoted = [
        str(row.get("variant_id")) for row in _records_obj(payload.get("promotion_candidates"))
    ]
    typer.echo(f"triage_id={payload['triage_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"promote_count={summary.get('promote_count')}")
    typer.echo(f"keep_testing_count={summary.get('keep_testing_count')}")
    typer.echo(f"reject_count={summary.get('reject_count')}")
    typer.echo("top_promoted_variants=" + ",".join(promoted))
    typer.echo(f"report_path={payload['triage_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-experiment-triage")
def dynamic_v3_validate_experiment_triage_command(
    triage_id: Annotated[str, typer.Option("--triage-id", help="experiment triage id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="experiment triage artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_TRIAGE_DIR,
) -> None:
    """校验 TRADING-243 experiment triage artifact。"""
    payload = experiment_factory.validate_experiment_triage_artifact(
        triage_id=triage_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_top_variant_interpretation_app.command("run")
def dynamic_v3_top_variant_interpretation_run_command(
    triage_id: Annotated[str, typer.Option("--triage-id", help="experiment triage id。")],
    triage_dir: Annotated[
        Path,
        typer.Option("--triage-dir", help="experiment triage artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_TRIAGE_DIR,
    matrix_dir: Annotated[
        Path,
        typer.Option("--matrix-dir", help="experiment matrix artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_MATRIX_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="top variant interpretation artifact root。"),
    ] = experiment_factory.DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
) -> None:
    """生成 TRADING-244 top variant interpretation pack。"""
    result = experiment_factory.run_top_variant_interpretation(
        triage_id=triage_id,
        triage_dir=triage_dir,
        matrix_dir=matrix_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"interpretation_id={result['interpretation_id']}")
    typer.echo(f"interpretation_dir={result['interpretation_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"recommended_variant={manifest['recommended_variant']}")
    typer.echo(f"top_variant_count={manifest['top_variant_count']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_top_variant_interpretation_app.command("report")
def dynamic_v3_top_variant_interpretation_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest top variant interpretation。"),
    ] = False,
    interpretation_id: Annotated[
        str | None,
        typer.Option("--interpretation-id", help="top variant interpretation id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="top variant interpretation artifact root。"),
    ] = experiment_factory.DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
) -> None:
    """展示 TRADING-244 top variant interpretation 摘要。"""
    payload = experiment_factory.top_variant_interpretation_report_payload(
        interpretation_id=interpretation_id,
        latest=latest,
        output_dir=output_dir,
    )
    explanations = _records_obj(payload.get("top_variant_explanations"))
    best = _mapping_obj(explanations[0] if explanations else {})
    typer.echo(f"interpretation_id={payload['interpretation_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"recommended_variant={payload['recommended_variant']}")
    typer.echo("solved_failure_modes=" + ",".join(_texts(best.get("why_it_helped"))))
    typer.echo("expected_costs=" + ",".join(_texts(best.get("what_it_costs"))))
    typer.echo(f"report_path={payload['top_variant_interpretation_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-top-variant-interpretation")
def dynamic_v3_validate_top_variant_interpretation_command(
    interpretation_id: Annotated[
        str,
        typer.Option("--interpretation-id", help="top variant interpretation id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="top variant interpretation artifact root。"),
    ] = experiment_factory.DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
) -> None:
    """校验 TRADING-244 top variant interpretation artifact。"""
    payload = experiment_factory.validate_top_variant_interpretation_artifact(
        interpretation_id=interpretation_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_method_promotion_plan_app.command("run")
def dynamic_v3_method_promotion_plan_run_command(
    triage_id: Annotated[str, typer.Option("--triage-id", help="experiment triage id。")],
    interpretation_id: Annotated[
        str,
        typer.Option("--interpretation-id", help="top variant interpretation id。"),
    ],
    triage_dir: Annotated[
        Path,
        typer.Option("--triage-dir", help="experiment triage artifact root。"),
    ] = experiment_factory.DEFAULT_EXPERIMENT_TRIAGE_DIR,
    interpretation_dir: Annotated[
        Path,
        typer.Option("--interpretation-dir", help="top variant interpretation artifact root。"),
    ] = experiment_factory.DEFAULT_TOP_VARIANT_INTERPRETATION_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="method promotion plan artifact root。"),
    ] = experiment_factory.DEFAULT_METHOD_PROMOTION_PLAN_DIR,
) -> None:
    """生成 TRADING-245 formal research method promotion plan。"""
    result = experiment_factory.run_method_promotion_plan(
        triage_id=triage_id,
        interpretation_id=interpretation_id,
        triage_dir=triage_dir,
        interpretation_dir=interpretation_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"promotion_plan_id={result['promotion_plan_id']}")
    typer.echo(f"promotion_plan_dir={result['promotion_plan_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo("proposed_method_names=" + ",".join(_texts(manifest.get("proposed_method_names"))))
    typer.echo(f"implementation_scope={manifest['implementation_scope']}")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")


@dynamic_v3_method_promotion_plan_app.command("report")
def dynamic_v3_method_promotion_plan_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest method promotion plan。"),
    ] = False,
    promotion_plan_id: Annotated[
        str | None,
        typer.Option("--promotion-plan-id", help="method promotion plan id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="method promotion plan artifact root。"),
    ] = experiment_factory.DEFAULT_METHOD_PROMOTION_PLAN_DIR,
) -> None:
    """展示 TRADING-245 method promotion plan 摘要。"""
    payload = experiment_factory.method_promotion_plan_report_payload(
        promotion_plan_id=promotion_plan_id,
        latest=latest,
        output_dir=output_dir,
    )
    specs = _mapping_obj(payload.get("promoted_method_specs"))
    methods = _records_obj(specs.get("methods"))
    typer.echo(f"promotion_plan_id={payload['promotion_plan_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(
        "proposed_method_names=" + ",".join(str(row.get("proposed_method_name")) for row in methods)
    )
    typer.echo(f"implementation_scope={payload['implementation_scope']}")
    typer.echo(f"next_action={specs.get('next_action')}")
    typer.echo(f"report_path={payload['method_promotion_plan_report_path']}")
    typer.echo("broker_action_allowed=false")


@dynamic_v3_rescue_app.command("validate-method-promotion-plan")
def dynamic_v3_validate_method_promotion_plan_command(
    promotion_plan_id: Annotated[
        str,
        typer.Option("--promotion-plan-id", help="method promotion plan id。"),
    ],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="method promotion plan artifact root。"),
    ] = experiment_factory.DEFAULT_METHOD_PROMOTION_PLAN_DIR,
) -> None:
    """校验 TRADING-245 method promotion plan artifact。"""
    payload = experiment_factory.validate_method_promotion_plan_artifact(
        promotion_plan_id=promotion_plan_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("experiment_only=true")
    typer.echo("broker_action_allowed=false")
    typer.echo("production_effect=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
