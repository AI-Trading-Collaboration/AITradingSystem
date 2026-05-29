from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from ai_trading_system.config import DEFAULT_SEC_COMPANIES_CONFIG_PATH, PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_TICKER_ALIASES_CONFIG_PATH = PROJECT_ROOT / "config" / "ticker_aliases.yaml"


@dataclass(frozen=True)
class TickerAliasResolution:
    input_ticker: str
    canonical_ticker: str
    source: str
    resolved: bool
    resolution_reason: str
    used_in_sec_companies: bool
    warning: str


def load_ticker_aliases(path: Path = DEFAULT_TICKER_ALIASES_CONFIG_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    raw = safe_load_yaml_path(path) or {}
    aliases = raw.get("aliases", raw)
    if not isinstance(aliases, dict):
        raise ValueError(f"ticker aliases config must be a mapping: {path}")
    return {
        _normalize_symbol(key): _normalize_symbol(value)
        for key, value in aliases.items()
        if str(key).strip() and str(value).strip()
    }


def resolve_ticker_alias(
    ticker: str,
    *,
    aliases: dict[str, str],
    sec_company_tickers: set[str],
    source: str = "config/ticker_aliases.yaml",
) -> TickerAliasResolution:
    input_ticker = _normalize_symbol(ticker)
    canonical = aliases.get(input_ticker, input_ticker)
    if canonical in sec_company_tickers:
        reason = "alias_remapped" if canonical != input_ticker else "direct_match"
        return TickerAliasResolution(
            input_ticker=input_ticker,
            canonical_ticker=canonical,
            source=source,
            resolved=True,
            resolution_reason=reason,
            used_in_sec_companies=True,
            warning="",
        )
    if input_ticker in sec_company_tickers:
        return TickerAliasResolution(
            input_ticker=input_ticker,
            canonical_ticker=input_ticker,
            source=source,
            resolved=True,
            resolution_reason="direct_match",
            used_in_sec_companies=True,
            warning="",
        )
    warning = (
        f"{input_ticker} did not resolve to a ticker in "
        f"{DEFAULT_SEC_COMPANIES_CONFIG_PATH.name}"
    )
    return TickerAliasResolution(
        input_ticker=input_ticker,
        canonical_ticker=canonical,
        source=source,
        resolved=False,
        resolution_reason="unresolved",
        used_in_sec_companies=False,
        warning=warning,
    )


def canonicalize_tickers(
    tickers: list[str],
    *,
    aliases: dict[str, str],
    sec_company_tickers: set[str],
) -> tuple[list[str], list[TickerAliasResolution]]:
    resolutions = [
        resolve_ticker_alias(
            ticker,
            aliases=aliases,
            sec_company_tickers=sec_company_tickers,
        )
        for ticker in tickers
    ]
    canonical: list[str] = []
    seen: set[str] = set()
    for resolution in resolutions:
        if not resolution.resolved:
            continue
        if resolution.canonical_ticker not in seen:
            canonical.append(resolution.canonical_ticker)
            seen.add(resolution.canonical_ticker)
    return canonical, resolutions


def canonicalize_ticker_series(
    values: pd.Series,
    *,
    aliases: dict[str, str],
) -> pd.Series:
    normalized = values.astype(str).map(_normalize_symbol)
    return normalized.map(lambda value: aliases.get(value, value))


def _normalize_symbol(value: object) -> str:
    return str(value or "").strip().upper()
