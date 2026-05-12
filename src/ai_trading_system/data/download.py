from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.config import (
    UniverseConfig,
    configured_price_tickers,
    configured_rate_series,
)
from ai_trading_system.data.market_data import (
    CBOE_VIX_TICKER,
    CboeVixPriceProvider,
    CsvDataCache,
    FmpPriceProvider,
    FredRateProvider,
    MarketstackPriceProvider,
    PriceDataProvider,
    PriceRequest,
    ProviderDownloadError,
    ProviderRequestDiagnostic,
    RateDataProvider,
    RateRequest,
    YFinancePriceProvider,
)
from ai_trading_system.external_request_cache import sanitize_diagnostic_text


@dataclass(frozen=True)
class DataDownloadSummary:
    prices_path: Path
    rates_path: Path
    manifest_path: Path
    price_rows: int
    rate_rows: int
    price_tickers: tuple[str, ...]
    rate_series: tuple[str, ...]
    secondary_prices_path: Path | None = None
    secondary_price_rows: int = 0


def default_download_failure_report_path(reports_dir: Path, as_of: date) -> Path:
    return reports_dir / f"download_data_diagnostics_{as_of.isoformat()}.md"


def write_download_failure_report(
    *,
    output_path: Path,
    start: date,
    end: date,
    raw_output_dir: Path,
    include_full_ai_chain: bool,
    price_provider_name: str,
    with_marketstack: bool,
    error: BaseException,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(tz=UTC)
    diagnostic = _provider_diagnostic_from_error(error)
    lines = [
        "# download-data 失败诊断报告",
        "",
        "- 状态：FAIL",
        f"- 生成时间：{generated_at.isoformat()}",
        f"- 下载开始日期：{start.isoformat()}",
        f"- 下载结束日期：{end.isoformat()}",
        f"- Raw 输出目录：`{raw_output_dir}`",
        f"- 主价格源：`{price_provider_name}`",
        f"- Full universe：{include_full_ai_chain}",
        f"- Marketstack 第二源：{with_marketstack}",
        "- 安全边界：本报告不保存 API key、token、Cookie、User-Agent、"
        "stdout/stderr 原文或供应商响应正文。",
        "",
        "## 失败摘要",
        "",
        f"- Exception type：`{type(error).__name__}`",
        f"- Sanitized message：{sanitize_diagnostic_text(str(error))}",
    ]
    if diagnostic is None:
        lines.extend(
            [
                "",
                "## Provider 诊断",
                "",
                "未捕获到结构化 provider 诊断。请优先检查子命令 stderr 的脱敏摘要，"
                "或补充对应 provider adapter 的诊断上下文。",
            ]
        )
    else:
        lines.extend(_render_provider_diagnostic(diagnostic))
    lines.extend(
        [
            "",
            "## 下游影响",
            "",
            "- `download-data` 已 fail closed；不得把可能部分刷新的 CSV 当作完整可审计输入。",
            "- 未成功写入本轮下载审计 manifest 时，`daily-run` 必须停止 PIT、SEC、"
            "valuation 和 `score-daily` 下游步骤。",
        ]
    )
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return output_path


def download_daily_data(
    config: UniverseConfig,
    start: date,
    end: date,
    output_dir: Path,
    include_full_ai_chain: bool = False,
    price_provider: PriceDataProvider | None = None,
    vix_price_provider: PriceDataProvider | None = None,
    secondary_price_provider: PriceDataProvider | None = None,
    rate_provider: RateDataProvider | None = None,
) -> DataDownloadSummary:
    if start > end:
        raise ValueError("start date must be earlier than or equal to end date")

    price_tickers = configured_price_tickers(config, include_full_ai_chain=include_full_ai_chain)
    rate_series = configured_rate_series(config)

    if not price_tickers:
        raise ValueError("price ticker list must not be empty")
    if not rate_series:
        raise ValueError("rate series list must not be empty")

    price_provider = price_provider or YFinancePriceProvider()
    rate_provider = rate_provider or FredRateProvider()
    cache = CsvDataCache(output_dir)

    price_request = PriceRequest(tickers=price_tickers, start=start, end=end, interval="1d")
    rate_request = RateRequest(series_ids=rate_series, start=start, end=end)

    primary_prices = price_provider.download_prices(price_request)
    prices = primary_prices
    vix_prices: pd.DataFrame | None = None
    vix_request: PriceRequest | None = None
    if CBOE_VIX_TICKER in price_tickers and CBOE_VIX_TICKER not in set(prices["ticker"]):
        vix_price_provider = vix_price_provider or CboeVixPriceProvider()
        vix_request = PriceRequest(
            tickers=[CBOE_VIX_TICKER],
            start=start,
            end=end,
            interval="1d",
        )
        vix_prices = vix_price_provider.download_prices(vix_request)
        prices = pd.concat([prices, vix_prices], ignore_index=True)
        prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)
    rates = rate_provider.download_rates(rate_request)

    prices_path = cache.write_prices(prices)
    rates_path = cache.write_rates(rates)
    secondary_prices_path: Path | None = None
    secondary_price_rows = 0
    manifest_records = [
        _manifest_record_for_prices(
            price_provider,
            price_request,
            prices_path,
            len(primary_prices),
        ),
        _manifest_record_for_rates(rate_provider, rate_request, rates_path, len(rates)),
    ]
    if vix_price_provider is not None and vix_request is not None and vix_prices is not None:
        manifest_records.append(
            _manifest_record_for_prices(
                vix_price_provider,
                vix_request,
                prices_path,
                len(vix_prices),
            )
        )

    if secondary_price_provider is not None:
        secondary_prices = secondary_price_provider.download_prices(price_request)
        secondary_prices_path = cache.write_prices(
            secondary_prices,
            filename="prices_marketstack_daily.csv",
        )
        secondary_price_rows = len(secondary_prices)
        manifest_records.append(
            _manifest_record_for_prices(
                secondary_price_provider,
                price_request,
                secondary_prices_path,
                secondary_price_rows,
            )
        )

    manifest_path = write_download_manifest(
        output_dir=output_dir,
        records=tuple(manifest_records),
    )

    return DataDownloadSummary(
        prices_path=prices_path,
        rates_path=rates_path,
        manifest_path=manifest_path,
        price_rows=len(prices),
        rate_rows=len(rates),
        price_tickers=tuple(price_tickers),
        rate_series=tuple(rate_series),
        secondary_prices_path=secondary_prices_path,
        secondary_price_rows=secondary_price_rows,
    )


def _provider_diagnostic_from_error(error: BaseException) -> ProviderRequestDiagnostic | None:
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, ProviderDownloadError):
            return current.diagnostic
        current = current.__cause__ or current.__context__
    return None


def _render_provider_diagnostic(diagnostic: ProviderRequestDiagnostic) -> list[str]:
    request_parameters = json.dumps(
        diagnostic.request_parameters,
        ensure_ascii=False,
        sort_keys=True,
    )
    rows = [
        ("Provider", diagnostic.provider),
        ("API family", diagnostic.api_family),
        ("Stage", diagnostic.stage),
        ("Method", diagnostic.method),
        ("Endpoint", diagnostic.endpoint),
        ("Cache status", diagnostic.cache_status),
        ("Cache key", diagnostic.cache_key or ""),
        (
            "Cache metadata path",
            "" if diagnostic.cache_metadata_path is None else str(diagnostic.cache_metadata_path),
        ),
        ("HTTP status", "" if diagnostic.http_status is None else str(diagnostic.http_status)),
        ("Provider error code", diagnostic.error_code or ""),
        (
            "Response body sha256",
            diagnostic.response_body_sha256 or "",
        ),
        (
            "Response body size bytes",
            (
                ""
                if diagnostic.response_body_size_bytes is None
                else str(diagnostic.response_body_size_bytes)
            ),
        ),
        (
            "Rows before failure",
            (
                ""
                if diagnostic.row_count_before_failure is None
                else str(diagnostic.row_count_before_failure)
            ),
        ),
        (
            "Attempt count",
            "" if diagnostic.attempt_count is None else str(diagnostic.attempt_count),
        ),
        (
            "Max attempts",
            "" if diagnostic.max_attempts is None else str(diagnostic.max_attempts),
        ),
        (
            "Timeout seconds",
            "" if diagnostic.timeout_seconds is None else str(diagnostic.timeout_seconds),
        ),
        ("Exception type", diagnostic.exception_type or ""),
        ("Exception message", diagnostic.exception_message or ""),
    ]
    lines = [
        "",
        "## Provider 诊断",
        "",
        "| 字段 | 值 |",
        "|---|---|",
    ]
    lines.extend(f"| {key} | {_escape_markdown_table(value)} |" for key, value in rows)
    lines.extend(
        [
            "",
            "### 脱敏请求参数",
            "",
            "```json",
            request_parameters,
            "```",
        ]
    )
    return lines


def _escape_markdown_table(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def write_download_manifest(
    output_dir: Path,
    records: tuple[dict[str, object], ...],
    filename: str = "download_manifest.csv",
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    new_frame = pd.DataFrame(records)

    if output_path.exists():
        existing = pd.read_csv(output_path)
        new_frame = pd.concat([existing, new_frame], ignore_index=True)

    new_frame.to_csv(output_path, index=False)
    return output_path


def _manifest_record_for_prices(
    provider: PriceDataProvider,
    request: PriceRequest,
    output_path: Path,
    row_count: int,
) -> dict[str, object]:
    source_id, provider_name, endpoint = _price_provider_metadata(provider)
    request_parameters: dict[str, object] = {
        "tickers": request.tickers,
        "start": request.start.isoformat(),
        "end": request.end.isoformat(),
        "interval": request.interval,
    }
    provider_symbol_aliases = _price_provider_symbol_aliases(provider, request.tickers)
    if provider_symbol_aliases:
        request_parameters["provider_symbol_aliases"] = provider_symbol_aliases
    return _manifest_record(
        source_id=source_id,
        provider=provider_name,
        endpoint=endpoint,
        request_parameters=request_parameters,
        output_path=output_path,
        row_count=row_count,
    )


def _manifest_record_for_rates(
    provider: RateDataProvider,
    request: RateRequest,
    output_path: Path,
    row_count: int,
) -> dict[str, object]:
    source_id, provider_name, endpoint = _rate_provider_metadata(provider)
    return _manifest_record(
        source_id=source_id,
        provider=provider_name,
        endpoint=endpoint,
        request_parameters={
            "series_ids": request.series_ids,
            "start": request.start.isoformat(),
            "end": request.end.isoformat(),
        },
        output_path=output_path,
        row_count=row_count,
    )


def _manifest_record(
    source_id: str,
    provider: str,
    endpoint: str,
    request_parameters: dict[str, object],
    output_path: Path,
    row_count: int,
) -> dict[str, object]:
    return {
        "downloaded_at": datetime.now(tz=UTC).isoformat(),
        "source_id": source_id,
        "provider": provider,
        "endpoint": endpoint,
        "request_parameters": json.dumps(
            request_parameters,
            ensure_ascii=False,
            sort_keys=True,
        ),
        "output_path": str(output_path),
        "row_count": row_count,
        "checksum_sha256": _sha256_file(output_path),
    }


def _price_provider_metadata(provider: PriceDataProvider) -> tuple[str, str, str]:
    if isinstance(provider, CboeVixPriceProvider):
        return ("cboe_vix_daily_prices", "Cboe Global Markets", provider.base_url)
    if isinstance(provider, FmpPriceProvider):
        return ("fmp_eod_daily_prices", "Financial Modeling Prep", provider.endpoint_summary())
    if isinstance(provider, YFinancePriceProvider):
        return ("yahoo_finance_daily_prices", "Yahoo Finance via yfinance", "yfinance.download")
    if isinstance(provider, MarketstackPriceProvider):
        return ("marketstack_eod_daily_prices", "Marketstack", provider.base_url)
    provider_name = provider.__class__.__name__
    return (_source_id_from_provider(provider_name), provider_name, provider_name)


def _price_provider_symbol_aliases(
    provider: PriceDataProvider,
    tickers: list[str],
) -> dict[str, str | None]:
    if isinstance(provider, FmpPriceProvider):
        return {
            ticker: provider.provider_symbol_for(ticker)
            for ticker in tickers
            if provider.provider_symbol_for(ticker) != ticker
        }
    return {}


def _rate_provider_metadata(provider: RateDataProvider) -> tuple[str, str, str]:
    if isinstance(provider, FredRateProvider):
        return ("fred_daily_rates", "Federal Reserve Economic Data", provider.base_url)
    provider_name = provider.__class__.__name__
    return (_source_id_from_provider(provider_name), provider_name, provider_name)


def _source_id_from_provider(provider_name: str) -> str:
    snake_name = re.sub(r"(?<!^)(?=[A-Z])", "_", provider_name).lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", snake_name).strip("_")
    return normalized or "unknown_provider"


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
