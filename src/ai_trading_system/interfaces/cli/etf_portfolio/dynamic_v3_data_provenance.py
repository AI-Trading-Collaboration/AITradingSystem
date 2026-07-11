from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DATA_PROVENANCE_DIR,
    DynamicV3ParameterResearchError,
    data_provenance_inspect_price_cache,
    data_provenance_repair_price_manifest,
    data_provenance_validate,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH
from ai_trading_system.interfaces.cli.etf_portfolio.common import mapping_obj
from ai_trading_system.interfaces.cli.etf_portfolio.registration import (
    dynamic_v3_data_provenance_app,
)


@dynamic_v3_data_provenance_app.command("inspect-price-cache")
def dynamic_v3_data_provenance_inspect_price_cache_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="标准化 ETF daily price cache。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="标准化 FRED rates cache。")
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    output_dir: Annotated[
        Path, typer.Option("--output-dir", help="data provenance artifact root。")
    ] = DEFAULT_DATA_PROVENANCE_DIR,
) -> None:
    """检查 TRADING-113 price cache checksum provenance。"""
    payload = data_provenance_inspect_price_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        output_dir=output_dir,
    )
    typer.echo(f"status={payload['status']}")
    typer.echo(f"prices_sha256={mapping_obj(payload.get('prices')).get('sha256')}")
    typer.echo(f"download_manifest_status={payload['download_manifest_status']}")
    typer.echo(f"provenance_status={payload['provenance_status']}")
    typer.echo(f"prices_checksum_in_manifest={str(payload['prices_checksum_in_manifest']).lower()}")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_data_provenance_app.command("repair-price-manifest")
def dynamic_v3_data_provenance_repair_price_manifest_command(
    mode: Annotated[
        str,
        typer.Option("--mode", help="repair mode；当前支持 reconstruct-from-cache。"),
    ] = "reconstruct-from-cache",
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="标准化 ETF daily price cache。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="标准化 FRED rates cache。")
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
) -> None:
    """从现有 cache 重建下载 manifest，不伪造原始下载事件。"""
    try:
        payload = data_provenance_repair_price_manifest(
            mode=mode,
            prices_path=prices_path,
            rates_path=rates_path,
        )
    except DynamicV3ParameterResearchError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(f"status={payload['status']}")
    typer.echo(f"reconstructed_manifest_path={payload['reconstructed_manifest_path']}")
    typer.echo(f"provenance_status={payload['provenance_status']}")
    typer.echo("limitations=original_download_event_not_available")
    typer.echo("production_candidate_generated=false")


@dynamic_v3_data_provenance_app.command("validate")
def dynamic_v3_data_provenance_validate_command(
    prices_path: Annotated[
        Path, typer.Option("--prices-path", help="标准化 ETF daily price cache。")
    ] = DEFAULT_ETF_PRICE_PATH,
    rates_path: Annotated[
        Path, typer.Option("--rates-path", help="标准化 FRED rates cache。")
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
) -> None:
    """校验 TRADING-113 price cache provenance。"""
    payload = data_provenance_validate(prices_path=prices_path, rates_path=rates_path)
    typer.echo(f"status={payload['status']}")
    typer.echo(f"failed_check_count={payload['failed_check_count']}")
    typer.echo(f"provenance_status={payload['provenance_status']}")
    typer.echo("production_candidate_generated=false")
    if payload["status"] == "FAIL":
        raise typer.Exit(code=1)
