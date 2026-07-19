from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import typer

from ai_trading_system.etf_portfolio.data import (
    latest_price_date,
    load_standard_prices,
    read_price_frame,
    standardize_price_frame,
    write_quality_report,
)
from ai_trading_system.etf_portfolio.features import latest_feature_date
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_REPORT_DIR, load_etf_config_bundle


def parse_date(value: str | None) -> date:
    if value is None:
        raise typer.BadParameter("日期不能为空")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 或 latest") from exc


def resolve_date(
    value: str | None,
    *,
    prices_path: Path | None = None,
    prices: pd.DataFrame | None = None,
) -> date:
    if value is not None and value != "latest":
        return parse_date(value)
    if prices is None:
        if prices_path is None:
            raise typer.BadParameter("latest 需要 prices 或 prices_path")
        config = load_etf_config_bundle()
        raw = read_price_frame(prices_path)
        prices, _ = standardize_price_frame(raw, assets=config.assets, source_name=str(prices_path))
    return latest_price_date(prices)


def satellite_symbols(config: Any) -> set[str]:
    if config.p1 is None:
        return set()
    return set(config.p1.satellite_stocks)


def load_optional_json_payload(path: Path | None) -> dict[str, object]:
    if path is None:
        return {}
    if not path.exists():
        raise typer.BadParameter(f"JSON artifact 不存在：{path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"JSON artifact 解析失败：{path}") from exc
    if not isinstance(payload, dict):
        raise typer.BadParameter(f"JSON artifact root 必须是 object：{path}")
    return payload


def latest_json_file(directory: Path, pattern: str) -> Path | None:
    """Return the newest matching JSON artifact without interpreting its content."""
    if not directory.exists():
        return None
    candidates = [path for path in directory.glob(pattern) if path.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def quality_metadata(report: Any) -> dict[str, object]:
    report_date = report.max_date.isoformat() if report.max_date else "unknown"
    report_path = DEFAULT_ETF_REPORT_DIR / f"data_quality_{report_date}.md"
    write_quality_report(report, report_path)
    return {
        "data_quality_status": report.status,
        "data_quality_report": f"`{report_path}`",
    }


def artifact_stem(value: object) -> str:
    text = str(value).strip().replace(":", "_").replace("/", "_").replace("\\", "_")
    return "".join(
        character if character.isalnum() or character in "._-" else "_" for character in text
    )


def mapping_obj(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def resolve_feature_date(value: str | None, features: pd.DataFrame) -> date:
    if value is not None and value != "latest":
        return parse_date(value)
    return latest_feature_date(features)


def resolve_frame_date(value: str | None, frame: pd.DataFrame, column: str = "date") -> date:
    if value is not None and value != "latest":
        return parse_date(value)
    if column not in frame.columns:
        raise typer.BadParameter(f"{column} 字段不存在")
    parsed = pd.to_datetime(frame[column], errors="coerce").dropna()
    if parsed.empty:
        raise typer.BadParameter(f"{column} 没有可用日期")
    return parsed.max().date()


def p1_quality_metadata(
    prices_path: Path,
    config,
    *,
    include_satellites: bool,
) -> dict[str, object]:
    _, report = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=satellite_symbols(config) if include_satellites else None,
    )
    if not report.passed:
        typer.echo(f"ETF 数据质量状态：{report.status}，已停止 P1 report。")
        raise typer.Exit(code=1)
    return quality_metadata(report)


def available_price_symbols(prices: pd.DataFrame, run_date: date) -> set[str]:
    frame = prices.copy()
    if "date" not in frame.columns or "symbol" not in frame.columns:
        return set()
    parsed_dates = pd.to_datetime(frame["date"], errors="coerce")
    selected = frame.loc[parsed_dates.notna() & (parsed_dates <= pd.Timestamp(run_date))]
    return {str(symbol).strip().upper() for symbol in selected["symbol"].dropna()}


def price_requested_date_range(prices: pd.DataFrame, run_date: date) -> dict[str, str]:
    if "date" not in prices.columns:
        return {"start": "", "end": run_date.isoformat()}
    parsed_dates = pd.to_datetime(prices["date"], errors="coerce")
    selected = parsed_dates.loc[parsed_dates.notna() & (parsed_dates <= pd.Timestamp(run_date))]
    if selected.empty:
        return {"start": "", "end": run_date.isoformat()}
    return {
        "start": selected.min().date().isoformat(),
        "end": run_date.isoformat(),
    }


__all__ = [
    "resolve_frame_date",
    "resolve_feature_date",
    "price_requested_date_range",
    "p1_quality_metadata",
    "available_price_symbols",
    "artifact_stem",
    "latest_json_file",
    "load_optional_json_payload",
    "mapping_obj",
    "parse_date",
    "quality_metadata",
    "resolve_date",
    "satellite_symbols",
]
