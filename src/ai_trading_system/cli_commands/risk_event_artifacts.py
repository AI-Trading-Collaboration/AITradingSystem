from __future__ import annotations

from datetime import date
from pathlib import Path

import typer

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.llm_precheck import DEFAULT_OPENAI_REQUEST_CACHE_DIR
from ai_trading_system.llm_request_profiles import (
    LlmRequestProfile,
    load_llm_request_profiles,
)

DEFAULT_RISK_EVENT_OCCURRENCES_PATH = PROJECT_ROOT / "data" / "external" / "risk_event_occurrences"
DEFAULT_RISK_EVENT_PREREVIEW_QUEUE_PATH = (
    PROJECT_ROOT / "data" / "processed" / "risk_event_prereview_queue.json"
)
DEFAULT_OPENAI_REQUEST_CACHE_PATH = PROJECT_ROOT / DEFAULT_OPENAI_REQUEST_CACHE_DIR
DEFAULT_RISK_EVENT_SINGLE_PREREVIEW_PROFILE = "risk_event_single_prereview"
DEFAULT_RISK_EVENT_TRIAGED_PREREVIEW_PROFILE = "risk_event_triaged_official_candidates"
DEFAULT_RISK_EVENT_DAILY_PREREVIEW_PROFILE = "risk_event_daily_official_precheck"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("日期必须使用 YYYY-MM-DD 格式。") from exc


def _parse_csv_items(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _load_llm_request_profile(
    profiles_path: Path,
    profile_id: str,
) -> LlmRequestProfile:
    try:
        return load_llm_request_profiles(profiles_path).get_profile(profile_id)
    except (OSError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc


def _coalesce_profile_value(value, profile_value):
    return profile_value if value is None else value
