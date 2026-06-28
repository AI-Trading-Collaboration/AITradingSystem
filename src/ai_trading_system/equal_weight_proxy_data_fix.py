from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data.market_data import (
    PriceDataProvider,
    PriceRequest,
    YFinancePriceProvider,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_COVERAGE_MATRIX_PATH,
    DEFAULT_FEATURE_ROOT,
    DEFAULT_FMP_GATE_PATH,
    DEFAULT_FREE_FEATURE_REGISTRY_PATH,
    DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    DEFAULT_PIT_CONTRACT_PATH,
    run_first_layer_proxy_coverage_audit_pack,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_INPUTS_ROOT as DEFAULT_PROXY_AUDIT_INPUTS_ROOT,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_PROXY_AUDIT_OUTPUT_ROOT,
)
from ai_trading_system.first_layer_proxy_coverage_audit import (
    DEFAULT_POLICY_PATH as DEFAULT_PROXY_AUDIT_POLICY_PATH,
)
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    MARKET_REGIME,
    SAFETY_BOUNDARY,
    clean_for_yaml,
    mapping,
    max_price_date,
    records,
    validate_cached_market_data,
    write_json,
    write_markdown,
)
from ai_trading_system.trading_engine.data.price_history_repair import (
    normalize_repaired_price_history,
    upsert_price_history_cache,
)
from ai_trading_system.trading_engine.data.symbol_resolver import source_symbol_for

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / "equal_weight_proxy_data_fix"
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"
DEFAULT_DOWNLOAD_MANIFEST_PATH = PROJECT_ROOT / "data" / "raw" / "download_manifest.csv"
DEFAULT_REPAIR_START = date(2018, 1, 1)
TARGET_SYMBOLS: tuple[str, ...] = ("RSP", "QQQE")
CORE_QUALITY_TICKERS: tuple[str, ...] = ("QQQ", "SPY", "SMH", "SOXX")
POST_REPAIR_QUALITY_TICKERS: tuple[str, ...] = (*CORE_QUALITY_TICKERS, *TARGET_SYMBOLS)
RESOLVED_PROXY_IDS: tuple[str, ...] = ("rsp_to_spy", "qqqe_to_qqq")

BLOCKED_STATE = {
    "primary_window_validated": False,
    "reopen_gate_allowed": False,
    "validation_ready": False,
    "candidate_count": 0,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "dynamic_promotion_status": "BLOCKED",
}


def run_equal_weight_proxy_data_fix_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    marketstack_prices_path: Path | None = DEFAULT_MARKETSTACK_PRICES_PATH,
    download_manifest_path: Path = DEFAULT_DOWNLOAD_MANIFEST_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    proxy_audit_policy_path: Path = DEFAULT_PROXY_AUDIT_POLICY_PATH,
    free_feature_registry_path: Path = DEFAULT_FREE_FEATURE_REGISTRY_PATH,
    participation_proxy_registry_path: Path = DEFAULT_PARTICIPATION_PROXY_REGISTRY_PATH,
    coverage_matrix_path: Path = DEFAULT_COVERAGE_MATRIX_PATH,
    pit_contract_path: Path = DEFAULT_PIT_CONTRACT_PATH,
    fmp_gate_path: Path = DEFAULT_FMP_GATE_PATH,
    feature_root: Path = DEFAULT_FEATURE_ROOT,
    proxy_audit_output_root: Path = DEFAULT_PROXY_AUDIT_OUTPUT_ROOT,
    proxy_audit_inputs_root: Path = DEFAULT_PROXY_AUDIT_INPUTS_ROOT,
    as_of_date: date | None = None,
    repair_start: date = DEFAULT_REPAIR_START,
    price_provider: PriceDataProvider | None = None,
    write_price_cache: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    resolved_as_of = as_of_date or max_price_date(prices_path)
    provider = price_provider or YFinancePriceProvider()

    data_quality_before = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=resolved_as_of,
        expected_price_tickers=CORE_QUALITY_TICKERS,
        expected_rate_series=(),
    )
    before_cache = _price_cache_status(prices_path, TARGET_SYMBOLS)
    provider_attempts, repaired_frames = _download_target_prices(
        provider=provider,
        target_symbols=TARGET_SYMBOLS,
        start=repair_start,
        end=resolved_as_of,
        generated_at=generated,
    )

    rows_written = 0
    if write_price_cache and repaired_frames:
        repaired = pd.concat(repaired_frames, ignore_index=True)
        rows_written = int(len(repaired))
        merged = upsert_price_history_cache(prices_path, repaired)
        prices_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(prices_path, index=False)
        _append_manifest(
            manifest_path=download_manifest_path,
            provider=provider,
            price_cache_path=prices_path,
            symbols=TARGET_SYMBOLS,
            start=repair_start,
            end=resolved_as_of,
            row_count=rows_written,
            generated_at=generated,
        )

    after_cache = _price_cache_status(prices_path, TARGET_SYMBOLS)
    data_quality_after = validate_cached_market_data(
        prices_path=prices_path,
        rates_path=rates_path,
        marketstack_prices_path=marketstack_prices_path,
        as_of_date=resolved_as_of,
        expected_price_tickers=POST_REPAIR_QUALITY_TICKERS,
        expected_rate_series=(),
    )
    updated_matrix = run_first_layer_proxy_coverage_audit_pack(
        policy_path=proxy_audit_policy_path,
        free_feature_registry_path=free_feature_registry_path,
        participation_proxy_registry_path=participation_proxy_registry_path,
        coverage_matrix_path=coverage_matrix_path,
        pit_contract_path=pit_contract_path,
        fmp_gate_path=fmp_gate_path,
        feature_root=feature_root,
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        output_root=proxy_audit_output_root,
        docs_root=docs_root,
        inputs_root=proxy_audit_inputs_root,
        as_of_date=resolved_as_of,
    )

    resolution_rows = _resolution_rows(before_cache, after_cache, provider_attempts, repair_start)
    blocked_status = _blocked_status_payload(updated_matrix, resolution_rows, resolved_as_of)
    payload = _payload(
        generated=generated,
        as_of_date=resolved_as_of,
        repair_start=repair_start,
        data_quality_before=data_quality_before,
        data_quality_after=data_quality_after,
        before_cache=before_cache,
        after_cache=after_cache,
        provider_attempts=provider_attempts,
        resolution_rows=resolution_rows,
        rows_written=rows_written,
        updated_matrix=updated_matrix,
        blocked_status=blocked_status,
        write_price_cache=write_price_cache,
        prices_path=prices_path,
        download_manifest_path=download_manifest_path,
    )

    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    updated_matrix_path = output_root / "updated_proxy_coverage_matrix.json"
    blocked_status_path = output_root / "blocked_proxy_resolution_status.json"
    report_path = docs_root / "equal_weight_proxy_data_fix_report.md"
    write_json(updated_matrix_path, updated_matrix)
    write_json(blocked_status_path, blocked_status)
    write_markdown(report_path, _render_report(payload))

    payload["artifact_paths"] = {
        "equal_weight_proxy_data_fix_report": str(report_path),
        "updated_proxy_coverage_matrix": str(updated_matrix_path),
        "blocked_proxy_resolution_status": str(blocked_status_path),
        "price_cache": str(prices_path),
        "download_manifest": str(download_manifest_path),
    }
    write_markdown(report_path, _render_report(payload))
    return clean_for_yaml(payload)


def _download_target_prices(
    *,
    provider: PriceDataProvider,
    target_symbols: Sequence[str],
    start: date,
    end: date,
    generated_at: datetime,
) -> tuple[list[dict[str, Any]], list[pd.DataFrame]]:
    attempts: list[dict[str, Any]] = []
    repaired_frames: list[pd.DataFrame] = []
    provider_label = _provider_label(provider)
    for symbol in target_symbols:
        source_symbol = source_symbol_for(symbol)
        try:
            downloaded = provider.download_prices(
                PriceRequest(
                    tickers=[source_symbol],
                    start=start,
                    end=end,
                    interval="1d",
                )
            )
            normalized = normalize_repaired_price_history(
                downloaded,
                canonical_symbol=symbol,
                source_symbol=source_symbol,
                source=provider_label,
                updated_at=generated_at.isoformat(),
                start=start,
                end=end,
            )
            status = "AVAILABLE" if not normalized.empty else "NO_DATA"
            attempts.append(
                {
                    "symbol": symbol,
                    "source_symbol": source_symbol,
                    "provider": provider_label,
                    "endpoint": _provider_endpoint(provider),
                    "request_parameters": {
                        "symbol": source_symbol,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "interval": "1d",
                    },
                    "status": status,
                    "rows_downloaded": int(len(downloaded)),
                    "rows_valid": int(len(normalized)),
                    "history_start_date": _frame_min_date(normalized),
                    "history_end_date": _frame_max_date(normalized),
                    "symbol_mapping_status": "canonical_equals_source"
                    if source_symbol == symbol
                    else "mapped_to_provider_symbol",
                    "error": "",
                }
            )
            if not normalized.empty:
                repaired_frames.append(normalized)
        except Exception as exc:
            attempts.append(
                {
                    "symbol": symbol,
                    "source_symbol": source_symbol,
                    "provider": provider_label,
                    "endpoint": _provider_endpoint(provider),
                    "request_parameters": {
                        "symbol": source_symbol,
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                        "interval": "1d",
                    },
                    "status": "FAILED",
                    "rows_downloaded": 0,
                    "rows_valid": 0,
                    "history_start_date": "",
                    "history_end_date": "",
                    "symbol_mapping_status": "canonical_equals_source"
                    if source_symbol == symbol
                    else "mapped_to_provider_symbol",
                    "error": str(exc),
                }
            )
    return attempts, repaired_frames


def _price_cache_status(path: Path, symbols: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {
            symbol: {
                "available": False,
                "row_count": 0,
                "history_start_date": "",
                "history_end_date": "",
                "source": "",
                "source_symbol": source_symbol_for(symbol),
                "canonical_symbol": symbol,
            }
            for symbol in symbols
        }
    frame = pd.read_csv(path)
    if "ticker" not in frame.columns:
        return {}
    frame["date"] = pd.to_datetime(frame.get("date"), errors="coerce")
    if "adj_close" in frame.columns:
        frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
        frame = frame.loc[frame["adj_close"].notna()].copy()
    summaries: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        group = frame.loc[frame["ticker"].astype(str) == symbol].copy()
        dates = (
            group["date"].dropna()
            if "date" in group.columns
            else pd.Series(dtype="datetime64[ns]")
        )
        first = group.iloc[0].to_dict() if not group.empty else {}
        summaries[symbol] = {
            "available": not dates.empty,
            "row_count": int(len(group)),
            "history_start_date": dates.min().date().isoformat() if not dates.empty else "",
            "history_end_date": dates.max().date().isoformat() if not dates.empty else "",
            "source": str(first.get("source", "")),
            "source_symbol": str(first.get("source_symbol", source_symbol_for(symbol))),
            "canonical_symbol": str(first.get("canonical_symbol", symbol)),
        }
    return summaries


def _resolution_rows(
    before_cache: Mapping[str, Mapping[str, Any]],
    after_cache: Mapping[str, Mapping[str, Any]],
    provider_attempts: Sequence[Mapping[str, Any]],
    repair_start: date,
) -> list[dict[str, Any]]:
    attempts = {str(item.get("symbol")): item for item in provider_attempts}
    rows: list[dict[str, Any]] = []
    for symbol in TARGET_SYMBOLS:
        before = mapping(before_cache.get(symbol))
        after = mapping(after_cache.get(symbol))
        attempt = mapping(attempts.get(symbol))
        before_available = bool(before.get("available"))
        before_start = str(before.get("history_start_date", ""))
        attempt_start = str(attempt.get("history_start_date", ""))
        if not before_available:
            command_entry_cache_status = "CACHE_MISSING_TICKER_NOT_DOWNLOADED"
        elif attempt_start and before_start and before_start > attempt_start:
            command_entry_cache_status = "CACHE_PRESENT_BUT_HISTORY_DEPTH_INCOMPLETE"
        else:
            command_entry_cache_status = "CACHE_ALREADY_AVAILABLE_ON_COMMAND_ENTRY"
        rows.append(
            {
                "symbol": symbol,
                "source_symbol": attempt.get("source_symbol", source_symbol_for(symbol)),
                "root_cause": "LOCAL_PRICE_CACHE_COVERAGE_GAP_NOT_PROVIDER_OR_MAPPING",
                "prior_audit_root_cause": "TRADING_2271_CACHE_MISSING_TICKER_NOT_DOWNLOADED",
                "command_entry_cache_status": command_entry_cache_status,
                "provider_support_status": attempt.get("status", "UNKNOWN"),
                "provider": attempt.get("provider", ""),
                "endpoint": attempt.get("endpoint", ""),
                "rows_downloaded": attempt.get("rows_downloaded", 0),
                "rows_valid": attempt.get("rows_valid", 0),
                "cache_before": dict(before),
                "cache_after": dict(after),
                "symbol_mapping_issue": False,
                "symbol_mapping_status": attempt.get("symbol_mapping_status", ""),
                "repair_status": "RESOLVED_PRICE_CACHE_AVAILABLE"
                if after.get("available")
                else "BLOCKED_PRICE_CACHE_UNAVAILABLE",
                "replacement_for_true_breadth": False,
            }
        )
    return rows


def _blocked_status_payload(
    updated_matrix: Mapping[str, Any],
    resolution_rows: Sequence[Mapping[str, Any]],
    as_of_date: date,
) -> dict[str, Any]:
    matrix_rows = records(updated_matrix.get("rows"))
    by_proxy = {str(row.get("proxy_id")): row for row in matrix_rows}
    resolved_proxy_ids = [
        proxy_id
        for proxy_id in RESOLVED_PROXY_IDS
        if mapping(by_proxy.get(proxy_id)).get("data_available")
    ]
    unresolved_target_symbols = [
        str(row.get("symbol"))
        for row in resolution_rows
        if row.get("repair_status") != "RESOLVED_PRICE_CACHE_AVAILABLE"
    ]
    still_blocked_proxy_ids = [
        str(row.get("proxy_id"))
        for row in matrix_rows
        if not row.get("data_available")
        or row.get("replacement_for_true_breadth")
        or str(row.get("PIT_safe_or_not")) != "PIT_SAFE_PRICE_PROXY_NOT_TRUE_BREADTH"
        and str(row.get("proxy_group")) == "etf_ratio_price_proxy"
    ]
    return {
        "schema_version": "blocked_proxy_resolution_status.v1",
        "report_type": "blocked_proxy_resolution_status",
        "status": "EQUAL_CAP_PRICE_PROXIES_RESOLVED_TRUE_BREADTH_STILL_BLOCKED"
        if not unresolved_target_symbols
        else "EQUAL_CAP_PRICE_PROXY_REPAIR_PARTIAL_BLOCKED",
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": MARKET_REGIME,
        "as_of": as_of_date.isoformat(),
        "resolved_proxy_ids": resolved_proxy_ids,
        "unresolved_target_symbols": unresolved_target_symbols,
        "still_blocked_proxy_ids": sorted(set(still_blocked_proxy_ids)),
        "true_breadth_replaced": False,
        "replacement_for_true_breadth": False,
        "blocked_reason": "price_proxy_not_constituent_membership",
        "behavioral_impact": (
            "RSP/SPY and QQQE/QQQ can be used only as equal/cap price diagnostic "
            "inputs; they do not provide historical constituent membership breadth."
        ),
        "risk": (
            "ETF price ratios can hide constituent churn, survivorship, and "
            "concentration effects."
        ),
        "validation_coverage": "cached data quality gate plus proxy coverage matrix rerun",
        "exit_condition": (
            "Provide owner-approved true PIT breadth source with historical constituents, "
            "daily membership query, delisted securities, and known-at semantics."
        ),
        **SAFETY_BOUNDARY,
        **BLOCKED_STATE,
    }


def _payload(
    *,
    generated: datetime,
    as_of_date: date,
    repair_start: date,
    data_quality_before: Mapping[str, Any],
    data_quality_after: Mapping[str, Any],
    before_cache: Mapping[str, Any],
    after_cache: Mapping[str, Any],
    provider_attempts: Sequence[Mapping[str, Any]],
    resolution_rows: Sequence[Mapping[str, Any]],
    rows_written: int,
    updated_matrix: Mapping[str, Any],
    blocked_status: Mapping[str, Any],
    write_price_cache: bool,
    prices_path: Path,
    download_manifest_path: Path,
) -> dict[str, Any]:
    matrix_summary = mapping(updated_matrix.get("summary"))
    return {
        "schema_version": "equal_weight_proxy_data_fix.v1",
        "report_type": "equal_weight_proxy_data_fix",
        "title": "Equal-Weight Proxy Data Availability Fix",
        "status": "EQUAL_WEIGHT_PROXY_DATA_FIX_READY_PROMOTION_BLOCKED",
        "generated_at": generated.replace(microsecond=0).isoformat(),
        "market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "requested_start": DEFAULT_BACKTEST_START,
        "requested_end": as_of_date.isoformat(),
        "repair_window_start": repair_start.isoformat(),
        "as_of": as_of_date.isoformat(),
        "data_quality_status_before": data_quality_before.get("status"),
        "data_quality_status_after": data_quality_after.get("status"),
        "data_quality_before": clean_for_yaml(dict(data_quality_before)),
        "data_quality_after": clean_for_yaml(dict(data_quality_after)),
        "prior_audit_root_cause": (
            "TRADING-2271 recorded RSP and QQQE as missing price components; "
            "provider probing showed Yahoo Finance supports both symbols, so the "
            "original blocker was local cache coverage rather than symbol mapping."
        ),
        "price_cache_path": str(prices_path),
        "download_manifest_path": str(download_manifest_path),
        "write_price_cache": write_price_cache,
        "rows_written": rows_written,
        "cache_before": clean_for_yaml(dict(before_cache)),
        "cache_after": clean_for_yaml(dict(after_cache)),
        "provider_attempts": clean_for_yaml(list(provider_attempts)),
        "resolution_rows": clean_for_yaml(list(resolution_rows)),
        "updated_proxy_coverage_summary": clean_for_yaml(dict(matrix_summary)),
        "replacement_for_true_breadth": False,
        "true_breadth_replaced": False,
        "low_cost_alternatives": _low_cost_alternatives(updated_matrix),
        "blocked_proxy_resolution_status": clean_for_yaml(dict(blocked_status)),
        "research_audit_metadata": {
            "modified_layer": "validation_only",
            "modified_channel": "equal_weight_proxy_data_fix",
            "model_version": "equal_weight_proxy_data_fix_v1",
            "threshold_policy": "first_layer_proxy_coverage_audit_policy_v1",
            "candidate_count": 0,
            "boundary_contract_version": "equal_weight_proxy_data_fix_research_only_v1",
        },
        **SAFETY_BOUNDARY,
        **BLOCKED_STATE,
    }


def _low_cost_alternatives(updated_matrix: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = {str(row.get("proxy_id")): row for row in records(updated_matrix.get("rows"))}
    return [
        {
            "alternative_id": "qqqe_to_qqq",
            "components": ["QQQE", "QQQ"],
            "status": _alternative_status(rows.get("qqqe_to_qqq")),
            "allowed_usage": "nasdaq_equal_cap_weight_price_diagnostic",
            "replacement_for_true_breadth": False,
        },
        {
            "alternative_id": "rsp_to_spy",
            "components": ["RSP", "SPY"],
            "status": _alternative_status(rows.get("rsp_to_spy")),
            "allowed_usage": "sp500_equal_cap_weight_price_diagnostic",
            "replacement_for_true_breadth": False,
        },
        {
            "alternative_id": "sector_etf_relative_strength",
            "components": ["SMH", "SOXX", "XLK", "QQQ"],
            "status": _alternative_status(rows.get("sector_etf_relative_strength")),
            "allowed_usage": "sector_price_confirmation_diagnostic",
            "replacement_for_true_breadth": False,
        },
        {
            "alternative_id": "top_n_equal_weight_proxy",
            "components": ["cached_top_n_ai_related_tickers"],
            "status": "DIAGNOSTIC_DESIGN_ONLY_NOT_BUILT",
            "allowed_usage": "diagnostic_proxy_only_not_true_breadth",
            "replacement_for_true_breadth": False,
        },
    ]


def _alternative_status(row: Mapping[str, Any] | None) -> str:
    if not row:
        return "MISSING_ROW"
    return "PRICE_PROXY_AVAILABLE_NOT_TRUE_BREADTH" if row.get("data_available") else "BLOCKED"


def _append_manifest(
    *,
    manifest_path: Path,
    provider: PriceDataProvider,
    price_cache_path: Path,
    symbols: Sequence[str],
    start: date,
    end: date,
    row_count: int,
    generated_at: datetime,
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    request_parameters = {
        "symbols": list(symbols),
        "symbol_mapping": {
            symbol: {
                "canonical_symbol": symbol,
                "source_symbol": source_symbol_for(symbol),
            }
            for symbol in symbols
            if source_symbol_for(symbol) != symbol
        },
        "start": start.isoformat(),
        "end": end.isoformat(),
        "interval": "1d",
        "repair_mode": "equal_weight_proxy_data_fix",
        "replacement_for_true_breadth": False,
    }
    record = {
        "downloaded_at": generated_at.isoformat(),
        "source_id": _provider_source_id(provider),
        "provider": _provider_label(provider),
        "endpoint": _provider_endpoint(provider),
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


def _render_report(payload: Mapping[str, Any]) -> str:
    summary = mapping(payload.get("updated_proxy_coverage_summary"))
    lines = [
        "# Equal-weight proxy data fix report",
        "",
        f"- status: `{payload.get('status')}`",
        f"- market_regime: `{payload.get('market_regime')}`",
        (
            f"- requested_date_range: `{payload.get('requested_start')}` "
            f"to `{payload.get('requested_end')}`"
        ),
        f"- repair_window: `{payload.get('repair_window_start')}` to `{payload.get('as_of')}`",
        f"- data_quality_before: `{payload.get('data_quality_status_before')}`",
        f"- data_quality_after: `{payload.get('data_quality_status_after')}`",
        "- safety: `replacement_for_true_breadth=false`, `promotion_allowed=false`, "
        "`paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`",
        "",
        "## 结论",
        "",
        "RSP / QQQE 的原始阻塞不是 provider 不支持，也不是 symbol mapping 问题；"
        "TRADING-2271 的缺失来自本地价格缓存未覆盖这些 ticker。免费 Yahoo Finance "
        "路径可返回两者历史价格，本批已补齐为 price-only diagnostic proxy。即使补齐，"
        "RSP/SPY 与 QQQE/QQQ 仍不是 historical constituent breadth，不能声明 true breadth。",
        "",
        "## Root cause",
        "",
        "|symbol|root_cause|provider_status|rows_valid|cache_after_start|cache_after_end|replacement_for_true_breadth|",
        "|---|---|---|---:|---|---|---:|",
    ]
    for row in records(payload.get("resolution_rows")):
        after = mapping(row.get("cache_after"))
        lines.append(
            f"|`{row.get('symbol')}`|`{row.get('root_cause')}`|"
            f"`{row.get('provider_support_status')}`|{row.get('rows_valid')}|"
            f"{after.get('history_start_date', '')}|{after.get('history_end_date', '')}|"
            f"{row.get('replacement_for_true_breadth')}|"
        )
    lines.extend(
        [
            "",
            "## Updated proxy coverage",
            "",
            f"- proxy_count: `{summary.get('proxy_count')}`",
            f"- data_available_count: `{summary.get('data_available_count')}`",
            f"- primary_window_covered_count: `{summary.get('primary_window_covered_count')}`",
            (
                "- replacement_for_true_breadth_count: "
                f"`{summary.get('replacement_for_true_breadth_count')}`"
            ),
            "",
            "|alternative|status|allowed_usage|replacement_for_true_breadth|",
            "|---|---|---|---:|",
        ]
    )
    for row in records(payload.get("low_cost_alternatives")):
        lines.append(
            f"|`{row.get('alternative_id')}`|`{row.get('status')}`|"
            f"{row.get('allowed_usage')}|{row.get('replacement_for_true_breadth')}|"
        )
    artifact_paths = mapping(payload.get("artifact_paths"))
    if artifact_paths:
        lines.extend(
            [
                "",
                "## Artifacts",
                "",
                (
                    "- updated_proxy_coverage_matrix: "
                    f"`{artifact_paths.get('updated_proxy_coverage_matrix')}`"
                ),
                (
                    "- blocked_proxy_resolution_status: "
                    f"`{artifact_paths.get('blocked_proxy_resolution_status')}`"
                ),
            ]
        )
    lines.extend(
        [
            "",
            "## Blocked status",
            "",
            "True breadth 仍被 `price_proxy_not_constituent_membership` 阻塞。解除条件是提供 "
            "owner-approved PIT breadth source，包含 historical constituents、daily membership "
            "query、delisted securities 和 known-at semantics。",
        ]
    )
    return "\n".join(lines) + "\n"


def _provider_label(provider: PriceDataProvider) -> str:
    if isinstance(provider, YFinancePriceProvider):
        return "Yahoo Finance via yfinance"
    return provider.__class__.__name__


def _provider_endpoint(provider: PriceDataProvider) -> str:
    if isinstance(provider, YFinancePriceProvider):
        return "yfinance.download"
    endpoint_summary = getattr(provider, "endpoint_summary", None)
    if callable(endpoint_summary):
        return str(endpoint_summary())
    base_url = getattr(provider, "base_url", None)
    return str(base_url) if base_url is not None else provider.__class__.__name__


def _provider_source_id(provider: PriceDataProvider) -> str:
    if isinstance(provider, YFinancePriceProvider):
        return "yahoo_finance_daily_prices"
    return provider.__class__.__name__


def _frame_min_date(frame: pd.DataFrame) -> str:
    if frame.empty or "date" not in frame.columns:
        return ""
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    return dates.min().date().isoformat() if not dates.empty else ""


def _frame_max_date(frame: pd.DataFrame) -> str:
    if frame.empty or "date" not in frame.columns:
        return ""
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    return dates.max().date().isoformat() if not dates.empty else ""


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
