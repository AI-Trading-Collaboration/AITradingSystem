from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START
from ai_trading_system.etf_portfolio.ai_attribution import (
    DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
    DEFAULT_AI_ATTRIBUTION_REVIEW_DIR,
    DEFAULT_AI_ATTRIBUTION_VALIDATION_DIR,
    build_ai_attribution_dataset,
    build_ai_attribution_report,
    build_ai_attribution_validation_report,
    load_ai_confirmation_report_payloads,
    write_ai_attribution_dataset,
    write_ai_attribution_report,
    write_ai_attribution_validation_report,
)
from ai_trading_system.etf_portfolio.ai_confirmation import (
    DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
)
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, load_etf_config_bundle
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    load_optional_json_payload as _load_optional_json_payload,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    quality_metadata as _quality_metadata,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import ai_attribution_app
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


@ai_attribution_app.command("build")
def ai_attribution_build_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = PRIMARY_RESEARCH_START,
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="既有 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution dataset 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
) -> None:
    """生成 TRADING-072A AI confirmation forward attribution dataset。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI attribution build。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    start_date = _parse_date(start)
    report_metadata = _quality_metadata(quality_report)
    reports = load_ai_confirmation_report_payloads(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    payload = build_ai_attribution_dataset(
        ai_confirmation_reports=reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    paths = write_ai_attribution_dataset(payload, output_dir=output_dir)
    typer.echo(f"AI attribution dataset JSON：{paths['json']}")
    typer.echo(f"AI attribution dataset CSV：{paths['csv']}")
    typer.echo(f"record_count={payload['record_count']}")
    typer.echo(f"available_sample_count={payload['available_sample_count']}")
    typer.echo(f"data_quality_status={quality_report.status}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ai_attribution_app.command("report")
def ai_attribution_report_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
    start: Annotated[
        str,
        typer.Option(help="attribution 起始日期，默认 AI regime start。"),
    ] = PRIMARY_RESEARCH_START,
    ai_confirmation_report_dir: Annotated[
        Path,
        typer.Option(help="既有 AI confirmation report JSON 目录。"),
    ] = DEFAULT_AI_CONFIRMATION_STANDALONE_REPORT_DIR,
    dataset_output_dir: Annotated[
        Path,
        typer.Option(help="同步写出的 AI attribution dataset 目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_DATASET_DIR,
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution report 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_REVIEW_DIR,
) -> None:
    """生成 TRADING-072H AI confirmation forward attribution report。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 AI attribution report。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    start_date = _parse_date(start)
    report_metadata = _quality_metadata(quality_report)
    reports = load_ai_confirmation_report_payloads(
        ai_confirmation_report_dir,
        as_of=run_date,
        start=start_date,
    )
    dataset = build_ai_attribution_dataset(
        ai_confirmation_reports=reports,
        prices=prices,
        evaluation_as_of_date=run_date,
        start=start_date,
        data_quality_status=quality_report.status,
        data_quality_report=str(report_metadata["data_quality_report"]).strip("`"),
        market_regime=config.backtest.backtest.regime,
        requested_date_range={"start": start_date.isoformat(), "end": run_date.isoformat()},
    )
    dataset_paths = write_ai_attribution_dataset(dataset, output_dir=dataset_output_dir)
    payload = build_ai_attribution_report(dataset)
    paths = write_ai_attribution_report(payload, output_dir=output_dir)
    typer.echo(f"AI attribution report JSON：{paths['json']}")
    typer.echo(f"AI attribution report Markdown：{paths['markdown']}")
    typer.echo(f"AI attribution dataset JSON：{dataset_paths['json']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"available_sample_count={dataset['available_sample_count']}")
    typer.echo("evaluation_only=true")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")


@ai_attribution_app.command("validate")
def ai_attribution_validate_command(
    output_dir: Annotated[
        Path,
        typer.Option(help="AI attribution validation 输出目录。"),
    ] = DEFAULT_AI_ATTRIBUTION_VALIDATION_DIR,
    report_registry_path: Annotated[
        Path,
        typer.Option(help="report registry YAML path。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    report_path: Annotated[
        Path | None,
        typer.Option(help="optional TRADING-072H attribution report JSON path。"),
    ] = None,
) -> None:
    """执行 TRADING-072J AI attribution validation gate。"""
    from ai_trading_system.reports.reader_brief import build_reader_brief_payload

    payload = build_ai_attribution_validation_report(
        report_registry=load_report_registry(report_registry_path),
        reader_brief_available=callable(build_reader_brief_payload),
        report_payload=(
            _load_optional_json_payload(report_path) if report_path is not None else None
        ),
    )
    paths = write_ai_attribution_validation_report(payload, output_dir=output_dir)
    typer.echo(f"AI attribution validation gate：{paths['markdown']}")
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
