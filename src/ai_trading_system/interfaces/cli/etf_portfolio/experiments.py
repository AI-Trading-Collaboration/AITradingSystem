from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.experiments import (
    DEFAULT_ETF_EXPERIMENT_RUN_DIR,
    DEFAULT_ETF_EXPERIMENT_WEEKLY_REVIEW_DIR,
    DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    apply_ranking_policy_to_comparison_report,
    build_candidate_selection_report,
    build_experiment_comparison_report,
    build_experiment_validation_report,
    build_weekly_experiment_review,
    enroll_shadow_candidates,
    find_latest_experiment_run_dir,
    load_experiment_pack_registry,
    load_experiment_registry,
    run_experiment_batch,
    write_candidate_selection_report,
    write_experiment_comparison_report,
    write_experiment_validation_report,
    write_weekly_experiment_review_report,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_STRATEGY_CONFIG_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.p1 import (
    append_experiment_registry,
    append_experiment_run,
    build_experiment_comparison,
    write_frame_and_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import experiments_app


@experiments_app.command("register")
def experiments_register_command(
    status: Annotated[
        str,
        typer.Option(help="实验状态：candidate/shadow/retired 等。"),
    ] = "candidate",
    notes: Annotated[str, typer.Option(help="实验备注。")] = "manual registration",
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
) -> None:
    """登记 ETF experiment registry 记录；不触发 promotion。"""
    config = load_etf_config_bundle()
    append_experiment_registry(
        registry_path=registry_path,
        model_version=config.strategy.model.version,
        parent_model_version=config.strategy.model.version,
        config_hash=config.config_hash,
        parameter_diff={},
        metrics={},
        status=status,
        notes=notes,
    )
    typer.echo(f"ETF experiment registry 已更新：{registry_path}")


@experiments_app.command("run")
def experiments_run_command(
    config_path: Annotated[
        Path | None,
        typer.Option("--config", help="候选 ETF strategy/config YAML；旧 P1 registry 路径。"),
    ] = None,
    pack: Annotated[
        str | None,
        typer.Option("--pack", help="TRADING-064 experiment pack id。"),
    ] = None,
    experiment: Annotated[
        str | None,
        typer.Option("--experiment", help="TRADING-064 single experiment id。"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", help="TRADING-064 batch backtest start YYYY-MM-DD。"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="TRADING-064 batch backtest end YYYY-MM-DD。"),
    ] = None,
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    output_dir: Annotated[
        Path,
        typer.Option(help="TRADING-064 batch run 输出目录。"),
    ] = DEFAULT_ETF_EXPERIMENT_RUN_DIR,
    metrics_path: Annotated[
        Path | None,
        typer.Option("--backtest-summary-path", help="可选：候选回测 summary.json。"),
    ] = None,
    baseline_config_path: Annotated[
        Path,
        typer.Option(help="production baseline strategy config，用于 diff。"),
    ] = DEFAULT_ETF_STRATEGY_CONFIG_PATH,
    status: Annotated[
        str,
        typer.Option(help="实验状态：candidate/shadow/retired 等。"),
    ] = "candidate",
    notes: Annotated[str, typer.Option(help="实验备注。")] = "candidate experiment run",
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
) -> None:
    """记录 P1 candidate run，或执行 TRADING-064 controlled experiment batch。"""
    config = load_etf_config_bundle()
    if pack is not None or experiment is not None:
        if config_path is not None:
            raise typer.BadParameter("--config cannot be combined with --pack/--experiment")
        if start is None or end is None:
            raise typer.BadParameter("--start and --end are required for batch experiments")
        prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
        if not quality_report.passed:
            typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 experiment batch。")
            raise typer.Exit(code=1)
        experiment_registry = load_experiment_registry()
        pack_registry = load_experiment_pack_registry(experiment_registry=experiment_registry)
        batch = run_experiment_batch(
            prices,
            base_config=config,
            quality_report=quality_report,
            experiment_registry=experiment_registry,
            pack_registry=pack_registry,
            pack_id=pack,
            experiment_id=experiment,
            start=_parse_date(start),
            end=_parse_date(end),
            output_root=output_dir,
        )
        typer.echo(f"ETF experiment batch 完成：{batch.run_id}")
        typer.echo(f"Run dir：{batch.run_dir}")
        typer.echo(f"status={batch.diagnostics_summary['status']}")
        typer.echo("production_effect=none")
        typer.echo("broker_action=none")
        if batch.diagnostics_summary["status"] != "PASS":
            raise typer.Exit(code=1)
        return
    if config_path is None:
        raise typer.BadParameter("--config or --pack/--experiment is required")
    append_experiment_run(
        registry_path=registry_path,
        candidate_config_path=config_path,
        baseline_config_path=baseline_config_path,
        config=config,
        metrics_path=metrics_path,
        status=status,
        notes=notes,
    )
    typer.echo(f"ETF experiment run 已登记：{registry_path}")
    typer.echo("production_effect=none")


@experiments_app.command("compare")
def experiments_compare_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    baseline: Annotated[str, typer.Option(help="比较基准，默认 production。")] = "production",
    baseline_metrics_path: Annotated[
        Path | None,
        typer.Option("--baseline-summary-path", help="可选：production 回测 summary.json。"),
    ] = None,
    registry_path: Annotated[Path, typer.Option(help="实验 registry JSONL。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments" / "registry.jsonl"
    ),
    output_dir: Annotated[Path, typer.Option(help="比较报告输出目录。")] = (
        DEFAULT_ETF_REPORT_DIR / "experiments"
    ),
) -> None:
    """只读比较 ETF experiment registry 或 TRADING-064 batch run；不自动 promotion。"""
    if run_id is not None or latest:
        if run_id is not None and latest:
            raise typer.BadParameter("--run-id and --latest cannot be combined")
        run_dir = find_latest_experiment_run_dir(output_dir) if latest else output_dir / str(run_id)
        payload = build_experiment_comparison_report(run_dir)
        pack_id = payload["run_metadata"].get("pack_id")
        if pack_id:
            pack_registry = load_experiment_pack_registry()
            pack_config = pack_registry.experiment_packs.get(str(pack_id))
            if pack_config is not None:
                payload = apply_ranking_policy_to_comparison_report(
                    payload,
                    ranking_policy=pack_registry.ranking_policies[pack_config.ranking_policy],
                    ranking_policy_id=pack_config.ranking_policy,
                )
        json_path = run_dir / "comparison_report.json"
        md_path = run_dir / "comparison_report.md"
        write_experiment_comparison_report(payload, json_path=json_path, markdown_path=md_path)
        typer.echo(f"ETF experiment comparison report：{md_path}")
        typer.echo(f"run_id={payload['run_metadata']['run_id']}")
        typer.echo(f"production_effect={payload['production_effect']}")
        return
    frame = build_experiment_comparison(
        registry_path=registry_path,
        baseline=baseline,
        baseline_metrics_path=baseline_metrics_path,
    )
    run_date = date.today()
    csv_path = output_dir / f"{run_date.isoformat()}_experiment_compare.csv"
    md_path = output_dir / f"{run_date.isoformat()}_experiment_compare.md"
    write_frame_and_report(
        frame,
        csv_path,
        md_path,
        "ETF Experiment Comparison",
        metadata={
            "baseline": baseline,
            "registry_path": registry_path,
            "production_effect": "none",
            "auto_promotion": False,
        },
    )
    typer.echo(f"ETF experiment comparison：{md_path}")
    typer.echo("production_effect=none")


@experiments_app.command("select-candidates")
def experiments_select_candidates_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    promotion_policy: Annotated[
        str | None,
        typer.Option("--promotion-policy", help="覆盖默认 pack promotion policy。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="experiment run 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
) -> None:
    """生成 TRADING-064 candidate selection gate；只允许 shadow-only/manual-review。"""
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    run_dir = find_latest_experiment_run_dir(output_dir) if latest else output_dir / str(run_id)
    selection = _build_experiment_candidate_selection(
        run_dir,
        promotion_policy_override=promotion_policy,
    )
    json_path = run_dir / "candidate_selection_report.json"
    md_path = run_dir / "candidate_selection_report.md"
    write_candidate_selection_report(selection, json_path=json_path, markdown_path=md_path)
    summary = selection["selection_summary"]
    typer.echo(f"ETF experiment candidate selection gate：{md_path}")
    typer.echo(f"run_id={selection['run_metadata'].get('run_id')}")
    typer.echo(f"status={summary['status']}")
    typer.echo(f"eligible_for_shadow={summary['eligible_for_shadow_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo(f"production_effect={selection['production_effect']}")


@experiments_app.command("enroll-shadow")
def experiments_enroll_shadow_command(
    run_id: Annotated[
        str | None,
        typer.Option("--run-id", help="TRADING-064 experiment batch run id。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="读取最新 TRADING-064 experiment batch run。"),
    ] = False,
    candidate: Annotated[
        list[str] | None,
        typer.Option("--candidate", help="candidate_id 或 experiment_id，可重复。"),
    ] = None,
    top: Annotated[
        int | None,
        typer.Option("--top", help="登记前 N 个 eligible_for_shadow candidates。"),
    ] = None,
    promotion_policy: Annotated[
        str | None,
        typer.Option("--promotion-policy", help="覆盖默认 pack promotion policy。"),
    ] = None,
    output_dir: Annotated[Path, typer.Option(help="experiment run 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 输出路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
) -> None:
    """把 eligible ETF experiment candidate 登记到 observe-only shadow registry。"""
    if run_id is None and not latest:
        raise typer.BadParameter("--run-id or --latest is required")
    if run_id is not None and latest:
        raise typer.BadParameter("--run-id and --latest cannot be combined")
    run_dir = find_latest_experiment_run_dir(output_dir) if latest else output_dir / str(run_id)
    selection = _build_experiment_candidate_selection(
        run_dir,
        promotion_policy_override=promotion_policy,
    )
    write_candidate_selection_report(
        selection,
        json_path=run_dir / "candidate_selection_report.json",
        markdown_path=run_dir / "candidate_selection_report.md",
    )
    try:
        registry = enroll_shadow_candidates(
            selection,
            registry_path=registry_path,
            candidate_ids=candidate,
            top=top,
        )
    except ValueError as exc:
        typer.echo(f"ETF shadow enrollment blocked: {exc}")
        raise typer.Exit(code=1) from exc
    typer.echo(f"ETF shadow candidates registry：{registry_path}")
    typer.echo(f"candidate_count={registry['candidate_count']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")


@experiments_app.command("weekly-review")
def experiments_weekly_review_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="周度复核日期 YYYY-MM-DD。"),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option("--latest", help="使用当前日期生成 latest weekly review。"),
    ] = False,
    registry_path: Annotated[
        Path,
        typer.Option(help="shadow candidate registry 路径。"),
    ] = DEFAULT_ETF_SHADOW_CANDIDATE_REGISTRY_PATH,
    run_root: Annotated[Path, typer.Option(help="experiment run 根目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR
    ),
    output_dir: Annotated[Path, typer.Option(help="weekly review 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_WEEKLY_REVIEW_DIR
    ),
    review_policy: Annotated[
        str,
        typer.Option("--review-policy", help="weekly review policy id。"),
    ] = "weekly_shadow_review_v1",
) -> None:
    """生成 observe-only ETF experiment weekly review；不允许 production promotion。"""
    if latest and as_of is not None:
        raise typer.BadParameter("--latest and --as-of cannot be combined")
    review_date = date.today() if latest else _parse_date(as_of)
    pack_registry = load_experiment_pack_registry()
    policy = pack_registry.review_policies.get(review_policy)
    if policy is None:
        raise typer.BadParameter(f"unknown review policy: {review_policy}")
    payload = build_weekly_experiment_review(
        as_of=review_date,
        shadow_registry_path=registry_path,
        run_root=run_root,
        review_policy=policy,
        review_policy_id=review_policy,
    )
    json_path = output_dir / f"weekly_review_{review_date.isoformat()}.json"
    md_path = output_dir / f"weekly_review_{review_date.isoformat()}.md"
    write_weekly_experiment_review_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF experiment weekly review：{md_path}")
    typer.echo(f"status={payload['summary']['status']}")
    typer.echo(f"candidate_count={payload['summary']['candidate_count']}")
    typer.echo("production_promotion_allowed=false")
    typer.echo(f"production_effect={payload['production_effect']}")


@experiments_app.command("validate")
def experiments_validate_command(
    pack: Annotated[
        str,
        typer.Option("--pack", help="TRADING-064 experiment pack id。"),
    ] = "etf_calibration_v1",
    output_dir: Annotated[Path, typer.Option(help="validation report 输出目录。")] = (
        DEFAULT_ETF_EXPERIMENT_RUN_DIR / "validation"
    ),
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry config path。"),
    ] = PROJECT_ROOT / "config" / "report_registry.yaml",
) -> None:
    """生成 TRADING-064 final experiment validation gate；失败时 fail closed。"""
    generated = datetime.now(UTC)
    payload = build_experiment_validation_report(
        pack_id=pack,
        report_registry_path=report_registry_path,
        generated_at=generated,
    )
    safe_pack = pack.replace("/", "_").replace("\\", "_")
    stem = f"{generated.date().isoformat()}_{safe_pack}_experiment_validation"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    write_experiment_validation_report(payload, json_path=json_path, markdown_path=md_path)
    typer.echo(f"ETF experiment validation gate：{md_path}")
    typer.echo(f"status={payload['status']}")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


def _build_experiment_candidate_selection(
    run_dir: Path,
    *,
    promotion_policy_override: str | None,
) -> dict[str, object]:
    payload = build_experiment_comparison_report(run_dir)
    pack_registry = load_experiment_pack_registry()
    pack_id = payload["run_metadata"].get("pack_id")
    policy_id = promotion_policy_override or "shadow_only_manual_review"
    if pack_id:
        pack_config = pack_registry.experiment_packs.get(str(pack_id))
        if pack_config is not None:
            payload = apply_ranking_policy_to_comparison_report(
                payload,
                ranking_policy=pack_registry.ranking_policies[pack_config.ranking_policy],
                ranking_policy_id=pack_config.ranking_policy,
            )
            policy_id = promotion_policy_override or pack_config.promotion_policy
    policy = pack_registry.promotion_policies.get(policy_id)
    if policy is None:
        raise typer.BadParameter(f"unknown promotion policy: {policy_id}")
    return build_candidate_selection_report(
        payload,
        promotion_policy=policy,
        promotion_policy_id=policy_id,
    )
