from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.market_data import (
    FmpPriceProvider,
    PriceDataProvider,
    PriceRequest,
    YFinancePriceProvider,
)
from ai_trading_system.trading_engine.backtest_input_diagnostics import (
    BacktestInputDiagnosticsRun,
    run_backtest_input_diagnostics,
)
from ai_trading_system.trading_engine.data.symbol_resolver import (
    DEFAULT_PRICE_SYMBOL_ALIASES,
    source_symbol_for,
)
from ai_trading_system.trading_engine.parameters.parameter_loader import (
    DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    load_shadow_backtest_config,
    resolve_project_path,
)

PRICE_HISTORY_REPAIR_SCHEMA_VERSION = 1
PRICE_HISTORY_REPAIR_SOURCE = "price_history_repair"
PRICE_REPAIR_SYMBOL_ALIASES: dict[str, str] = dict(DEFAULT_PRICE_SYMBOL_ALIASES)
PRICE_CACHE_OUTPUT_COLUMNS: tuple[str, ...] = (
    "date",
    "ticker",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "adj_close",
    "volume",
    "source",
    "updated_at",
    "source_symbol",
    "canonical_symbol",
)


@dataclass(frozen=True)
class PriceHistoryRepairAssetResult:
    symbol: str
    source_symbol: str
    status: str
    rows_downloaded: int
    rows_written: int
    missing_date_count: int = 0
    missing_dates_sample: tuple[str, ...] = ()
    error: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "source_symbol": self.source_symbol,
            "status": self.status,
            "rows_downloaded": self.rows_downloaded,
            "rows_written": self.rows_written,
            "missing_date_count": self.missing_date_count,
            "missing_dates_sample": list(self.missing_dates_sample),
            "error": self.error,
        }


@dataclass(frozen=True)
class PriceHistoryRepairRun:
    as_of: date
    status: str
    price_cache_path: Path
    manifest_path: Path
    initial_diagnostics: BacktestInputDiagnosticsRun
    final_diagnostics: BacktestInputDiagnosticsRun
    asset_results: tuple[PriceHistoryRepairAssetResult, ...]
    symbol_mapping: dict[str, dict[str, str]]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": PRICE_HISTORY_REPAIR_SCHEMA_VERSION,
            "report_type": PRICE_HISTORY_REPAIR_SOURCE,
            "as_of": self.as_of.isoformat(),
            "status": self.status,
            "price_cache_path": str(self.price_cache_path),
            "manifest_path": str(self.manifest_path),
            "initial_diagnostic_report": str(self.initial_diagnostics.json_path),
            "final_diagnostic_report": str(self.final_diagnostics.json_path),
            "assets": [result.to_dict() for result in self.asset_results],
            "symbol_mapping": self.symbol_mapping,
            "production_effect": "none",
        }


def build_price_history_repair_provider(
    *,
    provider_name: str,
    fmp_api_key: str = "",
) -> PriceDataProvider:
    normalized = provider_name.strip().lower()
    if normalized == "fmp":
        if not fmp_api_key.strip():
            raise ValueError("FMP API key is required for price history repair")
        return FmpPriceProvider(api_key=fmp_api_key)
    if normalized == "yahoo":
        return YFinancePriceProvider()
    raise ValueError("price history repair provider must be fmp or yahoo")


def repair_backtest_price_history(
    *,
    as_of: date | None = None,
    config_path: Path | str = DEFAULT_SHADOW_BACKTEST_CONFIG_PATH,
    output_root: Path | None = None,
    symbols: tuple[str, ...] = (),
    price_provider: PriceDataProvider,
    provider_name: str | None = None,
    price_only: bool = False,
    generated_at: datetime | None = None,
) -> PriceHistoryRepairRun:
    root = output_root or (PROJECT_ROOT / "artifacts")
    generated = generated_at or datetime.now(tz=UTC)
    initial = run_backtest_input_diagnostics(
        as_of=as_of,
        config_path=config_path,
        output_root=root,
        generated_at=generated,
    )
    config = load_shadow_backtest_config(config_path)
    price_cache_path = resolve_project_path(config.data.prices_path)
    manifest_path = resolve_project_path(config.data.download_manifest_path)
    date_range = _diagnostic_date_range(initial.payload, initial.as_of)
    target_symbols = _repair_target_symbols(initial.payload, symbols=symbols, price_only=price_only)
    provider_label = _effective_provider_label(price_provider, provider_name)
    updated_at = generated.isoformat()
    expected_dates = _existing_price_dates(price_cache_path, start=date_range[0], end=date_range[1])

    asset_results: list[PriceHistoryRepairAssetResult] = []
    repaired_frames: list[pd.DataFrame] = []
    symbol_mapping: dict[str, dict[str, str]] = {}
    for canonical_symbol in target_symbols:
        source_symbol = source_symbol_for_price_repair(canonical_symbol)
        if source_symbol != canonical_symbol:
            symbol_mapping[canonical_symbol] = {
                "source_symbol": source_symbol,
                "canonical_symbol": canonical_symbol,
            }
        try:
            downloaded = price_provider.download_prices(
                PriceRequest(
                    tickers=[source_symbol],
                    start=date_range[0],
                    end=date_range[1],
                    interval="1d",
                )
            )
            normalized = normalize_repaired_price_history(
                downloaded,
                canonical_symbol=canonical_symbol,
                source_symbol=source_symbol,
                source=provider_label,
                updated_at=updated_at,
                start=date_range[0],
                end=date_range[1],
            )
            if normalized.empty:
                asset_results.append(
                    PriceHistoryRepairAssetResult(
                        symbol=canonical_symbol,
                        source_symbol=source_symbol,
                        status="NO_DATA",
                        rows_downloaded=len(downloaded),
                        rows_written=0,
                        error="provider returned no valid rows for requested date range",
                    )
                )
                continue
            missing_dates = _missing_dates(normalized, expected_dates)
            repaired_frames.append(normalized)
            asset_results.append(
                PriceHistoryRepairAssetResult(
                    symbol=canonical_symbol,
                    source_symbol=source_symbol,
                    status="REPAIRED",
                    rows_downloaded=len(downloaded),
                    rows_written=len(normalized),
                    missing_date_count=len(missing_dates),
                    missing_dates_sample=tuple(item.isoformat() for item in missing_dates[:5]),
                )
            )
        except Exception as exc:
            asset_results.append(
                PriceHistoryRepairAssetResult(
                    symbol=canonical_symbol,
                    source_symbol=source_symbol,
                    status="FAILED",
                    rows_downloaded=0,
                    rows_written=0,
                    error=str(exc),
                )
            )

    if repaired_frames:
        repaired = pd.concat(repaired_frames, ignore_index=True)
        merged = upsert_price_history_cache(price_cache_path, repaired)
        _write_price_cache(price_cache_path, merged)
        _append_price_repair_manifest(
            manifest_path=manifest_path,
            provider=price_provider,
            provider_name=provider_label,
            price_cache_path=price_cache_path,
            symbols=tuple(result.symbol for result in asset_results if result.status == "REPAIRED"),
            symbol_mapping=symbol_mapping,
            start=date_range[0],
            end=date_range[1],
            interval="1d",
            row_count=len(repaired),
            price_only=price_only,
        )

    final = run_backtest_input_diagnostics(
        as_of=initial.as_of,
        config_path=config_path,
        output_root=root,
        generated_at=generated,
    )
    status = _repair_status(asset_results)
    return PriceHistoryRepairRun(
        as_of=initial.as_of,
        status=status,
        price_cache_path=price_cache_path,
        manifest_path=manifest_path,
        initial_diagnostics=initial,
        final_diagnostics=final,
        asset_results=tuple(asset_results),
        symbol_mapping=symbol_mapping,
    )


def normalize_repaired_price_history(
    prices: pd.DataFrame,
    *,
    canonical_symbol: str,
    source_symbol: str,
    source: str,
    updated_at: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)
    frame = prices.copy()
    if "ticker" not in frame.columns and "symbol" in frame.columns:
        frame["ticker"] = frame["symbol"]
    if "adj_close" not in frame.columns:
        if "adjClose" in frame.columns:
            frame["adj_close"] = frame["adjClose"]
        elif "close" in frame.columns:
            frame["adj_close"] = frame["close"]
    if "close" not in frame.columns and "adj_close" in frame.columns:
        frame["close"] = frame["adj_close"]
    for column in ("open", "high", "low"):
        if column not in frame.columns:
            frame[column] = frame["close"]
    if "volume" not in frame.columns:
        frame["volume"] = pd.NA

    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame = frame.loc[
        frame["date"].notna() & (frame["date"].dt.date >= start) & (frame["date"].dt.date <= end)
    ].copy()
    for column in ("open", "high", "low", "close", "adj_close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.loc[frame["close"].notna() & frame["adj_close"].notna()].copy()
    if frame.empty:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)

    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    frame["ticker"] = canonical_symbol
    frame["symbol"] = canonical_symbol
    frame["source"] = source
    frame["updated_at"] = updated_at
    frame["source_symbol"] = source_symbol
    frame["canonical_symbol"] = canonical_symbol
    frame = frame.sort_values("date").drop_duplicates(subset=["date", "ticker"], keep="last")
    return frame[list(PRICE_CACHE_OUTPUT_COLUMNS)].reset_index(drop=True)


def upsert_price_history_cache(
    price_cache_path: Path,
    repaired_prices: pd.DataFrame,
) -> pd.DataFrame:
    existing = _read_existing_price_cache(price_cache_path)
    repaired = _normalize_cache_columns(repaired_prices)
    if repaired.empty:
        return existing
    if existing.empty:
        merged = repaired
    else:
        keys = set(zip(repaired["date"].astype(str), repaired["ticker"].astype(str), strict=False))
        existing_keys = list(
            zip(existing["date"].astype(str), existing["ticker"].astype(str), strict=False)
        )
        keep_mask = [key not in keys for key in existing_keys]
        merged = pd.concat([existing.loc[keep_mask], repaired], ignore_index=True)
    merged["_sort_date"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = (
        merged.sort_values(["ticker", "_sort_date", "date"])
        .drop(columns=["_sort_date"])
        .drop_duplicates(subset=["date", "ticker"], keep="last")
        .reset_index(drop=True)
    )
    return merged


def source_symbol_for_price_repair(canonical_symbol: str) -> str:
    return source_symbol_for(canonical_symbol, aliases=PRICE_REPAIR_SYMBOL_ALIASES)


def _repair_target_symbols(
    diagnostic_payload: dict[str, Any],
    *,
    symbols: tuple[str, ...],
    price_only: bool,
) -> tuple[str, ...]:
    explicit_symbols = _normalize_symbols(symbols)
    if explicit_symbols:
        return explicit_symbols
    repair_plan = _mapping(diagnostic_payload.get("repair_plan"))
    targets: list[str] = []
    price_actions = {
        "download_missing_price_history",
        "extend_price_history",
        "repair_price_history_gaps",
    }
    for step in _records(repair_plan.get("steps")):
        if str(step.get("action")) in price_actions:
            targets.extend(_strings(step.get("assets")))
        elif not price_only and str(step.get("action")) == "generate_missing_signal_snapshots":
            continue
    return _normalize_symbols(tuple(targets))


def _diagnostic_date_range(payload: dict[str, Any], fallback_as_of: date) -> tuple[date, date]:
    metadata = _mapping(payload.get("metadata"))
    requested = _mapping(metadata.get("requested_date_range"))
    start = _parse_date_or_none(requested.get("start"))
    end = _parse_date_or_none(requested.get("end"))
    if start is None or end is None:
        return fallback_as_of - timedelta(days=365), fallback_as_of
    return start, end


def _read_existing_price_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)
    try:
        existing = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)
    return _normalize_cache_columns(existing)


def _normalize_cache_columns(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy()
    if normalized.empty:
        return pd.DataFrame(columns=PRICE_CACHE_OUTPUT_COLUMNS)
    if "ticker" not in normalized.columns and "symbol" in normalized.columns:
        normalized["ticker"] = normalized["symbol"]
    if "symbol" not in normalized.columns and "ticker" in normalized.columns:
        normalized["symbol"] = normalized["ticker"]
    if "canonical_symbol" not in normalized.columns and "ticker" in normalized.columns:
        normalized["canonical_symbol"] = normalized["ticker"]
    if "source_symbol" not in normalized.columns and "ticker" in normalized.columns:
        normalized["source_symbol"] = normalized["ticker"]
    if "source" not in normalized.columns:
        normalized["source"] = "existing_cache"
    if "updated_at" not in normalized.columns:
        normalized["updated_at"] = ""
    for column in PRICE_CACHE_OUTPUT_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA
    extra_columns = [
        column for column in normalized.columns if column not in PRICE_CACHE_OUTPUT_COLUMNS
    ]
    return normalized[[*PRICE_CACHE_OUTPUT_COLUMNS, *extra_columns]]


def _write_price_cache(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def _append_price_repair_manifest(
    *,
    manifest_path: Path,
    provider: PriceDataProvider,
    provider_name: str,
    price_cache_path: Path,
    symbols: tuple[str, ...],
    symbol_mapping: dict[str, dict[str, str]],
    start: date,
    end: date,
    interval: str,
    row_count: int,
    price_only: bool,
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    request_parameters = {
        "symbols": list(symbols),
        "symbol_mapping": symbol_mapping,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "interval": interval,
        "repair_mode": "price_only" if price_only else "backtest_input_repair",
    }
    record = {
        "downloaded_at": datetime.now(tz=UTC).isoformat(),
        "source_id": _source_id_for_provider(provider),
        "provider": provider_name,
        "endpoint": _endpoint_for_provider(provider),
        "request_parameters": json.dumps(
            request_parameters,
            ensure_ascii=False,
            sort_keys=True,
        ),
        "output_path": str(price_cache_path),
        "row_count": row_count,
        "checksum_sha256": _sha256_file(price_cache_path),
    }
    new_frame = pd.DataFrame([record])
    if manifest_path.exists():
        existing = pd.read_csv(manifest_path)
        new_frame = pd.concat([existing, new_frame], ignore_index=True)
    new_frame.to_csv(manifest_path, index=False)


def _repair_status(asset_results: list[PriceHistoryRepairAssetResult]) -> str:
    if not asset_results:
        return "NOT_REQUIRED"
    repaired = sum(1 for result in asset_results if result.status == "REPAIRED")
    failed = sum(1 for result in asset_results if result.status in {"FAILED", "NO_DATA"})
    if repaired and failed:
        return "PARTIAL"
    if repaired:
        return "REPAIRED"
    return "FAILED"


def _provider_label(provider: PriceDataProvider) -> str:
    if isinstance(provider, FmpPriceProvider):
        return "Financial Modeling Prep"
    if isinstance(provider, YFinancePriceProvider):
        return "Yahoo Finance via yfinance"
    return provider.__class__.__name__


def _effective_provider_label(provider: PriceDataProvider, provider_name: str | None) -> str:
    normalized_name = str(provider_name or "").strip().lower()
    if normalized_name in {"", "fmp", "yahoo"}:
        return _provider_label(provider)
    return str(provider_name)


def _existing_price_dates(path: Path, *, start: date, end: date) -> tuple[date, ...]:
    frame = _read_existing_price_cache(path)
    if frame.empty or "date" not in frame.columns:
        return ()
    parsed = pd.to_datetime(frame["date"], errors="coerce").dropna()
    observed = {
        pd.Timestamp(item).date() for item in parsed if start <= pd.Timestamp(item).date() <= end
    }
    return tuple(sorted(observed))


def _missing_dates(frame: pd.DataFrame, expected_dates: tuple[date, ...]) -> list[date]:
    if not expected_dates or frame.empty or "date" not in frame.columns:
        return []
    parsed = pd.to_datetime(frame["date"], errors="coerce").dropna()
    actual = {pd.Timestamp(item).date() for item in parsed}
    return [item for item in expected_dates if item not in actual]


def _source_id_for_provider(provider: PriceDataProvider) -> str:
    if isinstance(provider, FmpPriceProvider):
        return "fmp_eod_daily_prices"
    if isinstance(provider, YFinancePriceProvider):
        return "yahoo_finance_daily_prices"
    return _source_id_from_provider(provider.__class__.__name__)


def _endpoint_for_provider(provider: PriceDataProvider) -> str:
    endpoint_summary = getattr(provider, "endpoint_summary", None)
    if callable(endpoint_summary):
        return str(endpoint_summary())
    base_url = getattr(provider, "base_url", None)
    if base_url is not None:
        return str(base_url)
    return provider.__class__.__name__


def _source_id_from_provider(provider_name: str) -> str:
    snake_name = re.sub(r"(?<!^)(?=[A-Z])", "_", provider_name).lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", snake_name).strip("_")
    return normalized or "unknown_provider"


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_date_or_none(value: object) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _normalize_symbols(symbols: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(symbol.strip().upper() for symbol in symbols if symbol.strip()))


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: object) -> list[str]:
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return []
