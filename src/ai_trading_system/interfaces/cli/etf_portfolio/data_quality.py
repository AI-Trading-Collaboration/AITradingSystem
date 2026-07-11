from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.data import read_price_frame, standardize_price_frame
from ai_trading_system.etf_portfolio.data_quality import (
    DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    DEFAULT_ETF_DATA_QUALITY_REPORT_DIR,
    DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR,
    build_data_quality_report,
    build_data_quality_validation_report,
    check_price_freshness,
    load_data_quality_policy_config,
    write_data_quality_report,
    write_data_quality_validation_report,
)
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_PRICE_PATH,
    load_etf_config_bundle,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date, resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import data_quality_app
from ai_trading_system.reports.report_index import DEFAULT_REPORT_REGISTRY_PATH


@data_quality_app.command("price-freshness")
def data_quality_price_freshness_command(
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用 latest。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="data quality policy config path。"),
    ] = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
) -> None:
    """检查 ETF required / optional price freshness；不写 production state。"""
    config = load_etf_config_bundle()
    policy = load_data_quality_policy_config(config_path)
    run_date = resolve_date(as_of, prices_path=prices_path)
    raw = read_price_frame(prices_path)
    prices, _ = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name=str(prices_path),
        extra_symbols=set(policy.data_quality.price_freshness.optional_assets),
    )
    payload = check_price_freshness(prices, policy=policy, as_of=run_date)
    summary = payload["summary"]
    typer.echo(f"ETF data quality price freshness as_of={run_date.isoformat()}")
    typer.echo(f"record_count={summary['record_count']}")
    typer.echo(f"blocking_count={summary['blocking_count']}")
    typer.echo(f"warning_count={summary['warning_count']}")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if summary["blocking_count"]:
        raise typer.Exit(code=1)


@data_quality_app.command("report")
def data_quality_report_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用 latest。"),
    ] = None,
    prices_path: Annotated[Path, typer.Option(help="ETF 标准价格 CSV/Parquet 路径。")] = (
        DEFAULT_ETF_PRICE_PATH
    ),
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="data quality policy config path。"),
    ] = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    root_path: Annotated[
        Path,
        typer.Option("--root-path", help="扫描既有 artifacts 的根目录。"),
    ] = PROJECT_ROOT,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data quality governance report 输出目录。"),
    ] = DEFAULT_ETF_DATA_QUALITY_REPORT_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """生成 TRADING-075 data quality governance report；只读扫描现有缓存和 artifacts。"""
    run_date = resolve_date(as_of, prices_path=prices_path)
    payload = build_data_quality_report(
        as_of=run_date,
        prices_path=prices_path,
        policy_config_path=config_path,
        report_registry_path=report_registry_path,
        root_path=root_path,
    )
    json_output = json_path or output_dir / f"data_quality_report_{run_date.isoformat()}.json"
    markdown_output = markdown_path or output_dir / f"data_quality_report_{run_date.isoformat()}.md"
    paths = write_data_quality_report(payload, json_path=json_output, markdown_path=markdown_output)
    typer.echo(f"ETF data quality governance JSON：{paths['json']}")
    typer.echo(f"ETF data quality governance Markdown：{paths['markdown']}")
    typer.echo(f"report_id={payload['report_id']}")
    typer.echo(f"as_of_date={payload['as_of_date']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"blocking_failure_count={len(payload['blocking_failures'])}")
    typer.echo(f"warning_count={len(payload['warnings'])}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] == "BLOCKED":
        raise typer.Exit(code=1)


@data_quality_app.command("validate")
def data_quality_validate_command(
    as_of: Annotated[
        str | None,
        typer.Option("--as-of", "--date", help="评估日期 YYYY-MM-DD；省略时使用今天。"),
    ] = None,
    config_path: Annotated[
        Path,
        typer.Option("--config-path", "--config", help="data quality policy config path。"),
    ] = DEFAULT_ETF_DATA_QUALITY_POLICY_CONFIG_PATH,
    report_registry_path: Annotated[
        Path,
        typer.Option("--report-registry-path", help="report registry YAML 路径。"),
    ] = DEFAULT_REPORT_REGISTRY_PATH,
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", help="data quality validation 输出目录。"),
    ] = DEFAULT_ETF_DATA_QUALITY_VALIDATION_DIR,
    json_path: Annotated[
        Path | None,
        typer.Option("--json-path", help="显式 JSON 输出路径。"),
    ] = None,
    markdown_path: Annotated[
        Path | None,
        typer.Option("--markdown-path", help="显式 Markdown 输出路径。"),
    ] = None,
) -> None:
    """校验 TRADING-075 workflow 完整性和安全边界；失败时 exit 1。"""
    requested_as_of = as_of or date.today().isoformat()
    payload = build_data_quality_validation_report(
        as_of=requested_as_of,
        policy_config_path=config_path,
        report_registry_path=report_registry_path,
    )
    run_date = parse_date(requested_as_of)
    json_output = json_path or output_dir / f"data_quality_validation_{run_date.isoformat()}.json"
    markdown_output = (
        markdown_path or output_dir / f"data_quality_validation_{run_date.isoformat()}.md"
    )
    paths = write_data_quality_validation_report(
        payload,
        json_path=json_output,
        markdown_path=markdown_output,
    )
    typer.echo(f"ETF data quality validation JSON：{paths['json']}")
    typer.echo(f"ETF data quality validation Markdown：{paths['markdown']}")
    typer.echo(f"report_id={payload['report_id']}")
    typer.echo(f"as_of_date={payload['as_of_date']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"warning_check_count={payload['warning_check_count']}")
    typer.echo("commands_executed=false")
    typer.echo("production_state_mutated=false")
    typer.echo("observe_only=true")
    typer.echo("candidate_only=true")
    typer.echo("production_effect=none")
    typer.echo("broker_action=none")
    typer.echo("manual_review_required=true")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)


__all__ = [
    "data_quality_price_freshness_command",
    "data_quality_report_command",
    "data_quality_validate_command",
]
