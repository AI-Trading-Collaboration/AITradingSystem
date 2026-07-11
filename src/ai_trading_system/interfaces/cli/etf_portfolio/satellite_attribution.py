from __future__ import annotations

from pathlib import Path
from typing import Annotated, Any, Literal

import typer

from ai_trading_system.etf_portfolio.ai_confirmation import (
    DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
)
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.satellite import (
    DEFAULT_SATELLITE_STANDALONE_REPORT_DIR,
    DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH,
    load_satellite_universe_config,
    satellite_price_symbols,
)
from ai_trading_system.etf_portfolio.satellite_attribution import (
    DEFAULT_SATELLITE_ATTRIBUTION_DATASET_DIR,
    DEFAULT_SATELLITE_ATTRIBUTION_REVIEW_DIR,
    DEFAULT_SATELLITE_ATTRIBUTION_VALIDATION_DIR,
    build_satellite_attribution_dataset,
    build_satellite_attribution_report,
    build_satellite_attribution_validation_report,
    load_ai_confirmation_report_payloads_for_satellite,
    load_satellite_replacement_report_payloads,
    write_satellite_attribution_dataset,
    write_satellite_attribution_report,
    write_satellite_attribution_validation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload,
    parse_date,
    quality_metadata,
    resolve_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    satellite_attribution_app,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def prepare_satellite_attribution_dataset(
    *,
    prices_path: Path,
    as_of: str | None,
    start: str,
    satellite_report_dir: Path,
    ai_confirmation_report_dir: Path,
    universe_path: Path,
    workflow: Literal["build", "report"],
) -> tuple[dict[str, object], Any]:
    config = load_etf_config_bundle()
    satellite_config = load_satellite_universe_config(universe_path)
    extra_symbols = set(satellite_price_symbols(satellite_config)) - set(config.assets.assets)
    prices, quality_report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols,
    )
    if not quality_report.passed:
        typer.echo(
            f"ETF 数据质量状态：{quality_report.status}，已停止 satellite attribution {workflow}。"
        )
        raise typer.Exit(code=1)
    run_date = resolve_date(as_of, prices=prices)
    start_date = parse_date(start)
    report_metadata = quality_metadata(quality_report)
    satellite_reports = load_satellite_replacement_report_payloads(
        satellite_report_dir,
        as_of=run_date,
        start=start_date,
    )
    ai_reports = load_ai_confirmation_report_payloads_for_satellite(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    dataset = build_satellite_attribution_dataset(
        satellite_reports=satellite_reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        universe_config=satellite_config,
        ai_confirmation_reports=ai_reports,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    return dataset, quality_report


@satellite_attribution_app.command("build")
def satellite_attribution_build_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = "2022-12-01",
    satellite_report_dir: Annotated[
        Path,
        typer.Option(help="既有 satellite replacement report JSON 目录。"),
    ] = DEFAULT_SATELLITE_STANDALONE_REPORT_DIR,
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="可选 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    output_dir: Annotated[
        Path,
        typer.Option(help="satellite attribution dataset 输出目录。"),
    ] = DEFAULT_SATELLITE_ATTRIBUTION_DATASET_DIR,
) -> None:
    """生成 TRADING-073A satellite replacement forward attribution dataset。"""
    payload, quality_report = prepare_satellite_attribution_dataset(
        prices_path=prices_path,
        as_of=as_of,
        start=start,
        satellite_report_dir=satellite_report_dir,
        ai_confirmation_report_dir=ai_confirmation_report_dir,
        universe_path=universe_path,
        workflow="build",
    )
    paths = write_satellite_attribution_dataset(payload, output_dir=output_dir)
    typer.echo(f"Satellite attribution dataset JSON：{paths['json']}")
    typer.echo(f"Satellite attribution dataset CSV：{paths['csv']}")
    typer.echo(f"record_count={payload['record_count']}")
    typer.echo(f"available_sample_count={payload['available_sample_count']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@satellite_attribution_app.command("report")
def satellite_attribution_report_command(
    prices_path: Annotated[
        Path,
        typer.Option(help="ETF / satellite 标准价格 CSV/Parquet 路径。"),
    ] = DEFAULT_ETF_PRICE_PATH,
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = "2022-12-01",
    satellite_report_dir: Annotated[
        Path,
        typer.Option(help="既有 satellite replacement report JSON 目录。"),
    ] = DEFAULT_SATELLITE_STANDALONE_REPORT_DIR,
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="可选 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    universe_path: Annotated[Path, typer.Option(help="satellite universe config。")] = (
        DEFAULT_SATELLITE_UNIVERSE_CONFIG_PATH
    ),
    dataset_output_dir: Annotated[
        Path,
        typer.Option(help="同步写出的 satellite attribution dataset 目录。"),
    ] = DEFAULT_SATELLITE_ATTRIBUTION_DATASET_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(help="satellite attribution report 输出目录。"),
    ] = DEFAULT_SATELLITE_ATTRIBUTION_REVIEW_DIR,
) -> None:
    """生成 TRADING-073J satellite replacement forward attribution report。"""
    dataset, _ = prepare_satellite_attribution_dataset(
        prices_path=prices_path,
        as_of=as_of,
        start=start,
        satellite_report_dir=satellite_report_dir,
        ai_confirmation_report_dir=ai_confirmation_report_dir,
        universe_path=universe_path,
        workflow="report",
    )
    dataset_paths = write_satellite_attribution_dataset(
        dataset,
        output_dir=dataset_output_dir,
    )
    payload = build_satellite_attribution_report(dataset)
    paths = write_satellite_attribution_report(payload, output_dir=output_dir)
    typer.echo(f"Satellite attribution report JSON：{paths['json']}")
    typer.echo(f"Satellite attribution report Markdown：{paths['markdown']}")
    typer.echo(f"Satellite attribution dataset JSON：{dataset_paths['json']}")
    typer.echo(f"overall_status={payload['evidence_scorecard']['overall_status']}")
    typer.echo(f"available_sample_count={dataset['available_sample_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@satellite_attribution_app.command("validate")
def satellite_attribution_validate_command(
    output_dir: Annotated[
        Path,
        typer.Option(help="satellite attribution validation 输出目录。"),
    ] = DEFAULT_SATELLITE_ATTRIBUTION_VALIDATION_DIR,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-073J attribution report JSON path。"),
    ] = None,
) -> None:
    """执行 TRADING-073L satellite attribution validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    payload = build_satellite_attribution_validation_report(
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        report_payload=(
            load_optional_json_payload(report_path) if report_path is not None else None
        ),
    )
    paths = write_satellite_attribution_validation_report(payload, output_dir=output_dir)
    typer.echo(f"Satellite attribution validation gate：{paths['markdown']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "prepare_satellite_attribution_dataset",
    "satellite_attribution_build_command",
    "satellite_attribution_report_command",
    "satellite_attribution_validate_command",
]
