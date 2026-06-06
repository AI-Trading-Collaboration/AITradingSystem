from __future__ import annotations

from datetime import date
from pathlib import Path

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.trading_engine.portfolio_candidate_review import (
    latest_portfolio_candidate_review_decision_path,
)
from ai_trading_system.trading_engine.portfolio_candidate_tracking import (
    latest_portfolio_candidate_tracking_path,
)
from ai_trading_system.trading_engine.portfolio_candidates import latest_portfolio_candidates_path
from ai_trading_system.trading_engine.portfolio_sensitivity import latest_portfolio_sensitivity_path
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    latest_portfolio_tracking_review_path,
)
from ai_trading_system.trading_engine.portfolio_turnover_attribution import (
    latest_portfolio_turnover_attribution_path,
)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _resolve_portfolio_sensitivity_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "portfolio_sensitivity"
    if latest or as_of is None:
        latest_path = latest_portfolio_sensitivity_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 portfolio sensitivity artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "portfolio_sensitivity_summary.json"


def _resolve_portfolio_candidates_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidates"
    if latest or as_of is None:
        latest_path = latest_portfolio_candidates_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 portfolio candidates artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "portfolio_candidates_summary.json"


def _resolve_portfolio_turnover_attribution_path(
    *,
    latest: bool,
    as_of: str | None,
) -> Path:
    root = PROJECT_ROOT / "artifacts" / "portfolio_turnover_attribution"
    if latest or as_of is None:
        latest_path = latest_portfolio_turnover_attribution_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 portfolio turnover attribution artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "portfolio_turnover_attribution_summary.json"


def _resolve_portfolio_candidate_review_decision_path(
    *,
    latest: bool,
    as_of: str | None,
) -> Path:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidate_reviews"
    if latest or as_of is None:
        latest_path = latest_portfolio_candidate_review_decision_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 portfolio candidate review artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "portfolio_candidate_review_decision.json"


def _resolve_portfolio_candidate_tracking_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidate_tracking"
    if latest or as_of is None:
        latest_path = latest_portfolio_candidate_tracking_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 portfolio candidate tracking artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "portfolio_candidate_tracking_summary.json"


def _resolve_portfolio_tracking_review_path(*, latest: bool, as_of: str | None) -> Path:
    root = PROJECT_ROOT / "artifacts" / "portfolio_tracking_reviews"
    if latest or as_of is None:
        latest_path = latest_portfolio_tracking_review_path(root)
        if latest_path is None:
            raise typer.BadParameter(f"未找到 portfolio tracking review artifact：{root}")
        return latest_path
    return root / _parse_date(as_of).isoformat() / "portfolio_tracking_review_summary.json"
