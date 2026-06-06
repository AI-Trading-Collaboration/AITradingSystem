from __future__ import annotations

from datetime import date
from pathlib import Path

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.parameters.weight_stability import (
    latest_weight_stability_path,
)
from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    latest_weight_stability_readiness_path,
)
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    latest_weight_tuning_path,
)
from ai_trading_system.trading_engine.parameters.weight_tuning_failure import (
    latest_weight_tuning_failure_path,
)
from ai_trading_system.trading_engine.reports.shadow_backtest_report import (
    default_formal_shadow_backtest_root,
    default_shadow_backtest_summary_json_path,
    latest_shadow_backtest_summary_path,
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _resolve_shadow_backtest_summary_path(*, latest: bool, as_of: str | None) -> Path:
    root = default_formal_shadow_backtest_root()
    if latest or as_of is None:
        latest_path = latest_shadow_backtest_summary_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 shadow backtest artifact：{root}")
        return latest_path
    return default_shadow_backtest_summary_json_path(root, _parse_date(as_of))


def _resolve_weight_tuning_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "weight_tuning"
    if latest or as_of is None:
        latest_path = latest_weight_tuning_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 weight tuning artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "weight_tuning_summary.json"


def _resolve_weight_tuning_failure_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "weight_tuning_failure"
    if latest or as_of is None:
        latest_path = latest_weight_tuning_failure_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 weight tuning failure artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "weight_tuning_failure_summary.json"


def _resolve_weight_stability_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "weight_stability"
    if latest or as_of is None:
        latest_path = latest_weight_stability_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 weight stability artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "weight_stability_summary.json"


def _resolve_weight_stability_readiness_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "weight_stability_readiness"
    if latest or as_of is None:
        latest_path = latest_weight_stability_readiness_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 weight stability readiness artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "weight_stability_readiness_summary.json"
