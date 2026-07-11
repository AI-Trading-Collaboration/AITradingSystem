from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_EVIDENCE_SUMMARY_DIR,
    DEFAULT_INTERPRETATION_PACK_DIR,
    DEFAULT_MEDIUM_REAL_DIR,
    DEFAULT_OBSERVE_POOL_DIR,
    DEFAULT_OVERNIGHT_READINESS_DIR,
    DEFAULT_REGIME_COVERAGE_DIR,
    DEFAULT_SWEEP_OUTPUT_DIR,
    DynamicV3ParameterResearchError,
    build_medium_real_report,
    build_observe_pool,
    evidence_summary_report_payload,
    interpretation_report_payload,
    medium_real_report_payload,
    observe_pool_report_payload,
    overnight_readiness_report_payload,
    regime_coverage_report_payload,
    run_evidence_summary,
    run_interpretation_pack,
    run_overnight_readiness,
    run_regime_coverage,
    validate_evidence_summary_artifact,
    validate_interpretation_pack_artifact,
    validate_medium_real_sweep,
    validate_observe_pool_artifact,
    validate_overnight_readiness_artifact,
    validate_regime_coverage_artifact,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_candidate_app,
    dynamic_v3_evidence_summary_app,
    dynamic_v3_medium_real_app,
    dynamic_v3_observe_pool_app,
    dynamic_v3_overnight_readiness_app,
    dynamic_v3_regime_coverage_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_evidence_summary_app.command("run")
def dynamic_v3_evidence_summary_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
) -> None:
    """生成 TRADING-114 evidence summary。"""
    try:
        result = run_evidence_summary(
            sweep_id=sweep_id,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = result["manifest"]
    typer.echo(f"summary_id={result['summary_id']}")
    typer.echo(f"summary_dir={result['summary_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"candidate_count={manifest['candidate_count']}")
    typer.echo(f"usable_for_research_count={manifest['usable_for_research_count']}")
    typer.echo(f"can_enter_medium_real={manifest['can_enter_medium_real']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_evidence_summary_app.command("report")
def dynamic_v3_evidence_summary_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest evidence summary pointer。"),
    ] = False,
    summary_id: Annotated[str | None, typer.Option("--summary-id", help="summary id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
) -> None:
    """展示 TRADING-114 evidence summary 摘要。"""
    payload = evidence_summary_report_payload(
        summary_id=summary_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"summary_id={payload['summary_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"usable_for_research_count={payload['usable_for_research_count']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-evidence-summary")
def dynamic_v3_validate_evidence_summary_command(
    summary_id: Annotated[str, typer.Option("--summary-id", help="summary id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="evidence summary artifact root。"),
    ] = DEFAULT_EVIDENCE_SUMMARY_DIR,
) -> None:
    """校验 TRADING-114 evidence summary artifact。"""
    payload = validate_evidence_summary_artifact(summary_id=summary_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_medium_real_app.command("report")
def dynamic_v3_medium_real_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest medium_real report。"),
    ] = False,
    sweep_id: Annotated[str | None, typer.Option("--sweep-id", help="source sweep id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="medium_real report artifact root。"),
    ] = DEFAULT_MEDIUM_REAL_DIR,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """生成或展示 TRADING-115 medium_real report。"""
    payload = (
        medium_real_report_payload(
            latest=True,
            output_dir=output_dir,
            sweep_output_dir=sweep_output_dir,
        )
        if latest and sweep_id is None
        else build_medium_real_report(
            sweep_id=sweep_id,
            latest=latest,
            sweep_output_dir=sweep_output_dir,
            output_dir=output_dir,
        )
    )
    typer.echo(f"medium_real_report_id={payload['medium_real_report_id']}")
    typer.echo(f"source_sweep_id={payload['source_sweep_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"candidate_count={payload['candidate_count']}")
    typer.echo(f"completed_count={payload['completed_count']}")
    typer.echo(f"failed_count={payload['failed_count']}")
    typer.echo(f"observe_only_count={payload['observe_only_count']}")
    typer.echo(f"report_path={payload['medium_real_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-medium-real")
def dynamic_v3_validate_medium_real_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="medium_real sweep id。")],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
) -> None:
    """校验 TRADING-115 medium_real sweep artifact。"""
    payload = validate_medium_real_sweep(sweep_id=sweep_id, sweep_output_dir=sweep_output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"completed_count={payload['completed_count']}")
    typer.echo(f"failed_count={payload['failed_count']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_regime_coverage_app.command("run")
def dynamic_v3_regime_coverage_run_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    focus: Annotated[str, typer.Option("--focus", help="coverage focus。")] = "tech_semiconductor",
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    prices_path: Annotated[
        Path,
        typer.Option("--prices-path", help="standardized ETF price cache。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
) -> None:
    """生成 TRADING-116 tech / semiconductor regime coverage audit。"""
    try:
        result = run_regime_coverage(
            sweep_id=sweep_id,
            focus=focus,
            sweep_output_dir=sweep_output_dir,
            prices_path=prices_path,
            output_dir=output_dir,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    manifest = result["manifest"]
    typer.echo(f"coverage_id={result['coverage_id']}")
    typer.echo(f"coverage_dir={result['coverage_dir']}")
    typer.echo(f"coverage_status={manifest['coverage_status']}")
    typer.echo(f"tech_semiconductor_relevance={manifest['tech_semiconductor_relevance']}")
    typer.echo(f"ai_bull_market_overfit_risk={manifest['ai_bull_market_overfit_risk']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_regime_coverage_app.command("report")
def dynamic_v3_regime_coverage_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest regime coverage pointer。"),
    ] = False,
    coverage_id: Annotated[str | None, typer.Option("--coverage-id", help="coverage id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
) -> None:
    """展示 TRADING-116 regime coverage 摘要。"""
    payload = regime_coverage_report_payload(
        coverage_id=coverage_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"coverage_id={payload['coverage_id']}")
    typer.echo(f"coverage_status={payload['coverage_status']}")
    typer.echo(f"tech_semiconductor_relevance={payload['tech_semiconductor_relevance']}")
    typer.echo(f"ai_bull_market_overfit_risk={payload['ai_bull_market_overfit_risk']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-regime-coverage")
def dynamic_v3_validate_regime_coverage_command(
    coverage_id: Annotated[str, typer.Option("--coverage-id", help="coverage id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="regime coverage artifact root。"),
    ] = DEFAULT_REGIME_COVERAGE_DIR,
) -> None:
    """校验 TRADING-116 regime coverage artifact。"""
    payload = validate_regime_coverage_artifact(coverage_id=coverage_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_candidate_app.command("interpretation-pack")
def dynamic_v3_candidate_interpretation_pack_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    top_n: Annotated[int, typer.Option("--top-n", help="top candidate count。")] = 10,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="interpretation pack artifact root。"),
    ] = DEFAULT_INTERPRETATION_PACK_DIR,
) -> None:
    """生成 TRADING-117 top candidate interpretation pack。"""
    result = run_interpretation_pack(
        sweep_id=sweep_id,
        top_n=top_n,
        sweep_output_dir=sweep_output_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"pack_id={result['pack_id']}")
    typer.echo(f"pack_dir={result['pack_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"candidate_count={manifest['candidate_count']}")
    typer.echo(f"incomplete_weight_path_count={manifest['incomplete_weight_path_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_candidate_app.command("interpretation-report")
def dynamic_v3_candidate_interpretation_report_command(
    candidate_id: Annotated[str, typer.Option("--candidate-id", help="candidate id。")],
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest interpretation pack。"),
    ] = True,
    pack_id: Annotated[
        str | None, typer.Option("--pack-id", help="interpretation pack id。")
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="interpretation pack artifact root。"),
    ] = DEFAULT_INTERPRETATION_PACK_DIR,
) -> None:
    """展示 TRADING-117 candidate interpretation report 路径。"""
    payload = interpretation_report_payload(
        candidate_id=candidate_id,
        pack_id=pack_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"pack_id={payload['pack_id']}")
    typer.echo(f"candidate_id={payload['candidate_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-interpretation-pack")
def dynamic_v3_validate_interpretation_pack_command(
    pack_id: Annotated[str, typer.Option("--pack-id", help="interpretation pack id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="interpretation pack artifact root。"),
    ] = DEFAULT_INTERPRETATION_PACK_DIR,
) -> None:
    """校验 TRADING-117 interpretation pack。"""
    payload = validate_interpretation_pack_artifact(pack_id=pack_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_observe_pool_app.command("build")
def dynamic_v3_observe_pool_build_command(
    sweep_id: Annotated[str, typer.Option("--sweep-id", help="source sweep id。")],
    top_n: Annotated[int, typer.Option("--top-n", help="top candidate count。")] = 20,
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
) -> None:
    """生成 TRADING-118 observe-only candidate pool。"""
    result = build_observe_pool(
        sweep_id=sweep_id,
        top_n=top_n,
        sweep_output_dir=sweep_output_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"pool_id={result['pool_id']}")
    typer.echo(f"pool_dir={result['pool_dir']}")
    typer.echo(f"status={manifest['status']}")
    typer.echo(f"observe_candidate_count={manifest['observe_candidate_count']}")
    typer.echo(f"manual_review_required_count={manifest['manual_review_required_count']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_observe_pool_app.command("report")
def dynamic_v3_observe_pool_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest observe pool pointer。"),
    ] = False,
    pool_id: Annotated[str | None, typer.Option("--pool-id", help="pool id。")] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
) -> None:
    """展示 TRADING-118 observe pool 摘要。"""
    payload = observe_pool_report_payload(pool_id=pool_id, latest=latest, output_dir=output_dir)
    typer.echo(f"pool_id={payload['pool_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"observe_candidate_count={payload['observe_candidate_count']}")
    typer.echo(f"report_path={payload['observe_pool_report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-observe-pool")
def dynamic_v3_validate_observe_pool_command(
    pool_id: Annotated[str, typer.Option("--pool-id", help="pool id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="observe pool artifact root。"),
    ] = DEFAULT_OBSERVE_POOL_DIR,
) -> None:
    """校验 TRADING-118 observe pool artifact。"""
    payload = validate_observe_pool_artifact(pool_id=pool_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


@dynamic_v3_overnight_readiness_app.command("run")
def dynamic_v3_overnight_readiness_run_command(
    source_sweep_id: Annotated[
        str,
        typer.Option("--source-sweep-id", help="source medium_real sweep id。"),
    ],
    sweep_output_dir: Annotated[
        Path,
        typer.Option("--sweep-output-dir", help="sweep artifact root。"),
    ] = DEFAULT_SWEEP_OUTPUT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overnight readiness artifact root。"),
    ] = DEFAULT_OVERNIGHT_READINESS_DIR,
) -> None:
    """生成 TRADING-119 overnight_real readiness check。"""
    result = run_overnight_readiness(
        source_sweep_id=source_sweep_id,
        sweep_output_dir=sweep_output_dir,
        output_dir=output_dir,
    )
    manifest = result["manifest"]
    typer.echo(f"readiness_id={result['readiness_id']}")
    typer.echo(f"readiness_dir={result['readiness_dir']}")
    typer.echo(f"overnight_readiness={manifest['overnight_readiness']}")
    typer.echo(f"projected_overnight_runtime_hours={manifest['projected_overnight_runtime_hours']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_overnight_readiness_app.command("report")
def dynamic_v3_overnight_readiness_report_command(
    latest: Annotated[
        bool,
        typer.Option("--latest/--no-latest", help="读取 latest overnight readiness pointer。"),
    ] = False,
    readiness_id: Annotated[
        str | None,
        typer.Option("--readiness-id", help="readiness id。"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overnight readiness artifact root。"),
    ] = DEFAULT_OVERNIGHT_READINESS_DIR,
) -> None:
    """展示 TRADING-119 overnight readiness 摘要。"""
    payload = overnight_readiness_report_payload(
        readiness_id=readiness_id,
        latest=latest,
        output_dir=output_dir,
    )
    blockers = ",".join(str(item) for item in payload.get("blocking_reasons") or [])
    typer.echo(f"readiness_id={payload['readiness_id']}")
    typer.echo(f"overnight_readiness={payload['overnight_readiness']}")
    typer.echo(f"blocking_reasons={blockers}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-overnight-readiness")
def dynamic_v3_validate_overnight_readiness_command(
    readiness_id: Annotated[str, typer.Option("--readiness-id", help="readiness id。")],
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="overnight readiness artifact root。"),
    ] = DEFAULT_OVERNIGHT_READINESS_DIR,
) -> None:
    """校验 TRADING-119 overnight readiness artifact。"""
    payload = validate_overnight_readiness_artifact(
        readiness_id=readiness_id,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
