from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DATA_AUDIT_DIR,
    data_audit_report_payload,
    run_data_audit,
    validate_data_audit_artifact,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_data_audit_app,
    dynamic_v3_rescue_app,
)


@dynamic_v3_data_audit_app.command("run")
def dynamic_v3_data_audit_run_command(
    as_of: Annotated[str, typer.Option("--as-of", help="data audit as-of date。")],
    end: Annotated[str, typer.Option("--end", help="data audit end date。")],
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="标准化 ETF daily price cache。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="标准化 FRED rates cache。")
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="data audit artifact root。")
    ] = DEFAULT_DATA_AUDIT_DIR,
) -> None:
    """运行 TRADING-103 research data manifest / PIT coverage audit。"""
    result = run_data_audit(
        as_of=parse_date(as_of),
        end=parse_date(end),
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
    )
    report = result["report"]
    typer.echo(f"data_audit_id={result['data_audit_id']}")
    typer.echo(f"data_audit_dir={result['data_audit_dir']}")
    typer.echo(f"status={report['status']}")
    typer.echo(f"data_quality_status={report['data_quality_status']}")
    typer.echo(
        "prices_download_manifest_checksum_missing="
        f"{str(report['prices_download_manifest_checksum_missing']).lower()}"
    )
    typer.echo("production_candidate_generated=false")


@dynamic_v3_data_audit_app.command("report")
def dynamic_v3_data_audit_report_command(
    latest: Annotated[
        bool, typer.Option("--latest/--no-latest", help="读取 latest data audit pointer。")
    ] = False,
    audit_id: Annotated[str | None, typer.Option("--audit-id", help="data audit id。")] = None,
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="data audit artifact root。")
    ] = DEFAULT_DATA_AUDIT_DIR,
) -> None:
    """展示 TRADING-103 data audit 摘要。"""
    payload = data_audit_report_payload(
        data_audit_id=audit_id,
        latest=latest,
        output_dir=output_dir,
    )
    typer.echo(f"data_audit_id={payload['data_audit_id']}")
    typer.echo(f"status={payload['status']}")
    typer.echo(f"data_quality_status={payload['data_quality_status']}")
    typer.echo(f"report_path={payload['report_path']}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_rescue_app.command("validate-data-audit")
def dynamic_v3_validate_data_audit_command(
    audit_id: Annotated[str, typer.Option("--audit-id", help="data audit id。")],
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="data audit artifact root。")
    ] = DEFAULT_DATA_AUDIT_DIR,
) -> None:
    """校验 TRADING-103 data audit artifacts。"""
    payload = validate_data_audit_artifact(data_audit_id=audit_id, output_dir=output_dir)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] != "PASS":
        raise typer.Exit(code=1)
