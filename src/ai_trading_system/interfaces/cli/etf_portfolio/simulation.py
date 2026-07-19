from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
import typer

from ai_trading_system.etf_portfolio.allocation import load_allocation
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.models import (
    DEFAULT_ETF_LEDGER_PATH,
    DEFAULT_ETF_PRICE_PATH,
    DEFAULT_ETF_REPORT_DIR,
    DEFAULT_ETF_TARGET_PATH,
    ETFAllocationRecord,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.simulation import (
    evaluate_simulation_ledger,
    record_simulation_snapshot,
    write_simulation_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio.common import parse_date as _parse_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import resolve_date as _resolve_date
from ai_trading_system.interfaces.cli.etf_portfolio.common import (
    resolve_frame_date as _resolve_frame_date,
)
from ai_trading_system.interfaces.cli.etf_portfolio.registration import simulation_app


@simulation_app.command("record")
def simulation_record_command(
    allocation_path: Annotated[Path, typer.Option(help="目标权重路径。")] = DEFAULT_ETF_TARGET_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    date_option: Annotated[
        str | None,
        typer.Option("--date", help="记录日期或 latest；默认 latest allocation date。"),
    ] = None,
    report_path: Annotated[Path | None, typer.Option(help="关联日报路径。")] = None,
) -> None:
    """记录 ETF 模拟舱快照，按 date/model_version/symbol 幂等 upsert。"""
    allocation = load_allocation(allocation_path)
    run_date = _resolve_frame_date(date_option or "latest", allocation)
    allocation = _select_frame_date(allocation, run_date, label="目标权重")
    records = [_allocation_record_from_row(row) for _, row in allocation.iterrows()]
    record_simulation_snapshot(
        allocation_records=records,
        ledger_path=ledger_path,
        report_path=report_path,
    )
    typer.echo(f"ETF simulation ledger 已更新：{ledger_path}（date={run_date.isoformat()}）")


@simulation_app.command("evaluate")
def simulation_evaluate_command(
    prices_path: Annotated[Path, typer.Option(help="价格缓存路径。")] = DEFAULT_ETF_PRICE_PATH,
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    as_of: Annotated[str | None, typer.Option("--as-of", help="评估日期或 latest。")] = None,
) -> None:
    """补充 ETF 模拟舱 forward return 字段；未来窗口不足保持 null。"""
    config = load_etf_config_bundle()
    prices, quality_report = load_standard_prices(prices_path, config.assets, config.strategy)
    if not quality_report.passed:
        typer.echo(f"ETF 数据质量状态：{quality_report.status}，已停止 simulation evaluate。")
        raise typer.Exit(code=1)
    run_date = _resolve_date(as_of, prices=prices)
    evaluate_simulation_ledger(ledger_path=ledger_path, prices=prices, as_of=run_date)
    typer.echo(f"ETF simulation ledger 已评估：{ledger_path}")


@simulation_app.command("report")
def simulation_report_command(
    ledger_path: Annotated[Path, typer.Option(help="模拟舱 ledger 路径。")] = (
        DEFAULT_ETF_LEDGER_PATH
    ),
    window: Annotated[str, typer.Option(help="报告窗口。")] = "60d",
    output_path: Annotated[Path | None, typer.Option(help="报告输出路径。")] = None,
) -> None:
    """生成 ETF 模拟舱报告。"""
    report_path = output_path or DEFAULT_ETF_REPORT_DIR / f"simulation_report_{window}.md"
    write_simulation_report(ledger_path, report_path, window=window)
    typer.echo(f"ETF simulation report：{report_path}")


def _select_frame_date(
    frame: pd.DataFrame,
    run_date: date,
    *,
    column: str = "date",
    label: str = "数据",
) -> pd.DataFrame:
    if column not in frame.columns:
        raise typer.BadParameter(f"{label}缺少 {column} 字段")
    parsed = pd.to_datetime(frame[column], errors="coerce")
    selected = frame.loc[parsed == pd.Timestamp(run_date)].copy()
    if selected.empty:
        raise typer.BadParameter(f"{label}缺少日期：{run_date.isoformat()}")
    return selected


def _allocation_record_from_row(row: pd.Series) -> ETFAllocationRecord:
    return ETFAllocationRecord(
        date=_parse_date(str(row["date"])),
        symbol=str(row["symbol"]),
        target_weight=float(row["target_weight"]),
        previous_weight=_optional_float(row.get("previous_weight")),
        trade_delta=_optional_float(row.get("trade_delta")),
        composite_score=_optional_float(row.get("composite_score")),
        regime=str(row["regime"]),
        reason_codes=tuple(_json_list(row.get("reason_codes"))),
        constraints_applied=tuple(_json_list(row.get("constraints_applied"))),
        model_version=str(row["model_version"]),
        config_hash=str(row["config_hash"]),
        data_quality_status=str(row["data_quality_status"]),
        created_at=pd.Timestamp(str(row["created_at"])).to_pydatetime(),
        constraint_diagnostics=tuple(_json_records(row.get("constraint_diagnostics"))),
    )


def _optional_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(parsed):
        return None
    return parsed


def _json_list(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except ValueError:
        return [str(value)]
    if not isinstance(parsed, list):
        return [str(parsed)]
    return [str(item) for item in parsed]


def _json_records(value: object) -> list[dict[str, object]]:
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except ValueError:
        return []
    if not isinstance(parsed, list):
        return []
    return [dict(item) for item in parsed if isinstance(item, dict)]
