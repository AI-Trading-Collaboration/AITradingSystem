from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import typer

from ai_trading_system.etf_portfolio.data import (
    latest_price_date,
    read_price_frame,
    standardize_price_frame,
    write_quality_report,
)
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


__all__ = [
    "artifact_stem",
    "latest_json_file",
    "load_optional_json_payload",
    "mapping_obj",
    "parse_date",
    "quality_metadata",
    "resolve_date",
    "satellite_symbols",
]
