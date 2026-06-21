from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "data_quality" / "current_subscription_data_coverage"
)
DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "data_quality"
    / "data_source_requirements"
    / "data_source_requirement_matrix.json"
)

REPRESENTATIVE_UNIVERSE = ("SPY", "QQQ", "SMH", "MSFT", "GOOGL", "NVDA", "AMD", "TSM", "cash")
PRICE_PROBE_SYMBOLS = ("SPY", "QQQ", "SMH", "MSFT", "GOOGL", "NVDA", "AMD", "TSM")
PROBE_START = "2024-01-02"
PROBE_END = "2024-01-05"

SAFETY_BOUNDARY = {
    "validation_only": True,
    "observe_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "promotion_gate_allowed": False,
    "paper_shadow_change_allowed": False,
    "production_weight_change_allowed": False,
}


class SubscriptionCoverageAuditError(ValueError):
    pass


@dataclass(frozen=True)
class EndpointProbe:
    provider_id: str
    provider: str
    endpoint_name: str
    key_envs: tuple[str, ...]
    url: str
    params_template: Mapping[str, Any]
    symbol_param: str | None = None
    path_symbol_template: str | None = None
    symbols_param: str | None = None
    requires_key: bool = True
    key_param: str = "apikey"
    probe_symbols: tuple[str, ...] = PRICE_PROBE_SYMBOLS
    raw_price_supported: bool = False
    adjusted_price_supported: bool = False
    splits_supported: bool = False
    dividends_supported: bool = False
    delisted_supported: bool = False
    fundamentals_supported: bool = False
    event_calendar_supported: bool = False
    available_time_supported: bool = False
    source_manifest_possible: bool = True
    current_view_only_risk: bool = False
    allowed_use_if_accessible: str = "diagnostic_only"
    role: str = "diagnostic"
    probe_kind: str = "symbol"


def run_current_subscription_data_coverage_audit(
    *,
    source_requirement_matrix_path: Path = DEFAULT_SOURCE_REQUIREMENT_MATRIX_PATH,
    output_root: Path = DEFAULT_CURRENT_SUBSCRIPTION_COVERAGE_OUTPUT_ROOT,
    env: Mapping[str, str] | None = None,
    http_get: Callable[..., Any] | None = None,
    timeout_seconds: float = 10.0,
) -> dict[str, Any]:
    if timeout_seconds <= 0:
        raise SubscriptionCoverageAuditError("timeout_seconds must be positive")
    checked_env = dict(os.environ if env is None else env)
    endpoint_probes = _endpoint_probe_catalog()
    endpoint_results = [
        _run_endpoint_probe(
            probe,
            env=checked_env,
            http_get=http_get,
            timeout_seconds=timeout_seconds,
        )
        for probe in endpoint_probes
    ]
    provider_key_statuses = _provider_key_statuses(endpoint_results, checked_env)
    requirement_payload = _read_json(source_requirement_matrix_path)
    requirement_matches = _match_requirements_to_subscription(
        _records(requirement_payload.get("source_requirements")),
        endpoint_results,
    )
    accessible_endpoint_count = sum(1 for item in endpoint_results if item["accessible"])
    key_present_provider_count = sum(1 for item in provider_key_statuses if item["key_present"])
    summary = {
        "provider_count": len(provider_key_statuses),
        "endpoint_probe_count": len(endpoint_results),
        "accessible_endpoint_count": accessible_endpoint_count,
        "key_present_provider_count": key_present_provider_count,
        "representative_universe": list(REPRESENTATIVE_UNIVERSE),
        "requirement_match_count": len(requirement_matches),
        "requirements_current_subscription_cover_true_count": sum(
            1 for item in requirement_matches if item["can_current_subscription_cover"] == "true"
        ),
        "requirements_current_subscription_cover_unknown_count": sum(
            1 for item in requirement_matches if item["can_current_subscription_cover"] == "unknown"
        ),
        "requirements_current_subscription_cover_false_count": sum(
            1 for item in requirement_matches if item["can_current_subscription_cover"] == "false"
        ),
        "requires_new_paid_source_count": sum(
            1 for item in requirement_matches if item["requires_new_paid_source"]
        ),
        "status_upgrade_attempted": False,
        "lookahead_violation_count": 0,
        **SAFETY_BOUNDARY,
    }
    payload = {
        "report_type": "current_subscription_data_coverage_matrix",
        "title": "Current subscription data coverage matrix",
        "status": "COVERAGE_AUDIT_RECORDED_NO_SOURCE_UPGRADE",
        "generated_at": _utc_now_iso(),
        "source_requirement_matrix_path": str(source_requirement_matrix_path),
        "summary": summary,
        "provider_key_statuses": provider_key_statuses,
        "endpoint_coverage_matrix": endpoint_results,
        "requirement_subscription_matches": requirement_matches,
        "api_key_material_recorded": False,
        "status_upgrade_attempted": False,
        "lookahead_violation_count": 0,
        **SAFETY_BOUNDARY,
    }
    _assert_payload_has_no_key_material(payload, checked_env)
    _write_artifact_pair(
        payload,
        output_root=output_root,
        artifact_id="current_subscription_data_coverage_matrix",
    )
    return payload


def _endpoint_probe_catalog() -> list[EndpointProbe]:
    fmp_key_envs = ("FINANCIAL_MODELING_PREP_API_KEY", "FMP_API_KEY")
    return [
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="historical_eod_price_full",
            key_envs=fmp_key_envs,
            url="https://financialmodelingprep.com/stable/historical-price-eod/full",
            params_template={"from": PROBE_START, "to": PROBE_END},
            symbol_param="symbol",
            raw_price_supported=True,
            adjusted_price_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="price",
        ),
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="historical_eod_price_light",
            key_envs=fmp_key_envs,
            url="https://financialmodelingprep.com/stable/historical-price-eod/light",
            params_template={"from": PROBE_START, "to": PROBE_END},
            symbol_param="symbol",
            raw_price_supported=True,
            adjusted_price_supported=False,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="price",
        ),
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="historical_eod_price_non_split_adjusted",
            key_envs=fmp_key_envs,
            url=(
                "https://financialmodelingprep.com/stable/"
                "historical-price-eod/non-split-adjusted"
            ),
            params_template={"from": PROBE_START, "to": PROBE_END},
            symbol_param="symbol",
            raw_price_supported=True,
            adjusted_price_supported=False,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="price",
        ),
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="dividends",
            key_envs=fmp_key_envs,
            url="https://financialmodelingprep.com/stable/dividends",
            params_template={},
            symbol_param="symbol",
            dividends_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="corporate_actions",
        ),
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="splits",
            key_envs=fmp_key_envs,
            url="https://financialmodelingprep.com/stable/splits",
            params_template={},
            symbol_param="symbol",
            splits_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="corporate_actions",
        ),
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="delisted_companies",
            key_envs=fmp_key_envs,
            url="https://financialmodelingprep.com/stable/delisted-companies",
            params_template={"page": 0, "limit": 100},
            delisted_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="asset_master",
            probe_kind="global",
        ),
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="earnings_calendar",
            key_envs=fmp_key_envs,
            url="https://financialmodelingprep.com/stable/earnings-calendar",
            params_template={"from": PROBE_START, "to": PROBE_END},
            event_calendar_supported=True,
            allowed_use_if_accessible="research_label_only",
            role="event_calendar",
            probe_kind="global",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="index_constituents_sp500",
            key_envs=fmp_key_envs,
            url="https://financialmodelingprep.com/stable/sp500-constituent",
            params_template={},
            allowed_use_if_accessible="diagnostic_only",
            role="asset_master",
            probe_kind="global",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="fmp",
            provider="Financial Modeling Prep",
            endpoint_name="financial_statements_income_statement",
            key_envs=fmp_key_envs,
            url="https://financialmodelingprep.com/stable/income-statement",
            params_template={"period": "annual", "limit": 1},
            symbol_param="symbol",
            probe_symbols=("MSFT", "GOOGL", "NVDA", "AMD", "TSM"),
            fundamentals_supported=True,
            allowed_use_if_accessible="diagnostic_only",
            role="fundamentals",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="marketstack",
            provider="Marketstack",
            endpoint_name="eod_historical_price",
            key_envs=("MARKETSTACK_API_KEY",),
            url="https://api.marketstack.com/v2/eod",
            params_template={"date_from": PROBE_START, "date_to": PROBE_END, "limit": 100},
            symbols_param="symbols",
            key_param="access_key",
            raw_price_supported=True,
            adjusted_price_supported=False,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="price",
        ),
        EndpointProbe(
            provider_id="marketstack",
            provider="Marketstack",
            endpoint_name="splits",
            key_envs=("MARKETSTACK_API_KEY",),
            url="https://api.marketstack.com/v2/splits",
            params_template={"limit": 100},
            symbols_param="symbols",
            key_param="access_key",
            splits_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="corporate_actions",
        ),
        EndpointProbe(
            provider_id="marketstack",
            provider="Marketstack",
            endpoint_name="dividends",
            key_envs=("MARKETSTACK_API_KEY",),
            url="https://api.marketstack.com/v2/dividends",
            params_template={"limit": 100},
            symbols_param="symbols",
            key_param="access_key",
            dividends_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="corporate_actions",
        ),
        EndpointProbe(
            provider_id="marketstack",
            provider="Marketstack",
            endpoint_name="ticker_exchange_metadata",
            key_envs=("MARKETSTACK_API_KEY",),
            url="https://api.marketstack.com/v2/tickers",
            params_template={"search": "MSFT", "limit": 10},
            key_param="access_key",
            allowed_use_if_accessible="diagnostic_only",
            role="asset_master",
            probe_kind="global",
        ),
        EndpointProbe(
            provider_id="marketstack",
            provider="Marketstack",
            endpoint_name="index_etf_holdings",
            key_envs=("MARKETSTACK_API_KEY",),
            url="https://api.marketstack.com/v2/tickers/QQQ/holdings",
            params_template={},
            key_param="access_key",
            allowed_use_if_accessible="diagnostic_only",
            role="asset_master",
            probe_kind="global",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="eodhd",
            provider="EODHD",
            endpoint_name="eod_raw_adjusted_price",
            key_envs=("EODHD_API_KEY",),
            url="https://eodhd.com/api/eod/{symbol}.US",
            params_template={"fmt": "json", "from": PROBE_START, "to": PROBE_END},
            path_symbol_template="{symbol}",
            key_param="api_token",
            raw_price_supported=True,
            adjusted_price_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="price",
        ),
        EndpointProbe(
            provider_id="eodhd",
            provider="EODHD",
            endpoint_name="dividends",
            key_envs=("EODHD_API_KEY",),
            url="https://eodhd.com/api/div/{symbol}.US",
            params_template={"fmt": "json", "from": PROBE_START, "to": PROBE_END},
            path_symbol_template="{symbol}",
            key_param="api_token",
            dividends_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="corporate_actions",
        ),
        EndpointProbe(
            provider_id="eodhd",
            provider="EODHD",
            endpoint_name="splits",
            key_envs=("EODHD_API_KEY",),
            url="https://eodhd.com/api/splits/{symbol}.US",
            params_template={"fmt": "json", "from": PROBE_START, "to": PROBE_END},
            path_symbol_template="{symbol}",
            key_param="api_token",
            splits_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="corporate_actions",
        ),
        EndpointProbe(
            provider_id="eodhd",
            provider="EODHD",
            endpoint_name="exchange_symbol_list_symbol_change",
            key_envs=("EODHD_API_KEY",),
            url="https://eodhd.com/api/exchange-symbol-list/US",
            params_template={"fmt": "json"},
            key_param="api_token",
            delisted_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="asset_master",
            probe_kind="global",
        ),
        EndpointProbe(
            provider_id="eodhd",
            provider="EODHD",
            endpoint_name="fundamentals",
            key_envs=("EODHD_API_KEY",),
            url="https://eodhd.com/api/fundamentals/{symbol}.US",
            params_template={"fmt": "json"},
            path_symbol_template="{symbol}",
            key_param="api_token",
            probe_symbols=("MSFT", "GOOGL", "NVDA", "AMD", "TSM"),
            fundamentals_supported=True,
            allowed_use_if_accessible="diagnostic_only",
            role="fundamentals",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="eodhd",
            provider="EODHD",
            endpoint_name="options_diagnostic",
            key_envs=("EODHD_API_KEY",),
            url="https://eodhd.com/api/options/{symbol}.US",
            params_template={"fmt": "json"},
            path_symbol_template="{symbol}",
            key_param="api_token",
            probe_symbols=("MSFT",),
            allowed_use_if_accessible="diagnostic_only",
            role="diagnostic",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="alpha_vantage",
            provider="Alpha Vantage",
            endpoint_name="daily_adjusted",
            key_envs=("ALPHA_VANTAGE_API_KEY",),
            url="https://www.alphavantage.co/query",
            params_template={"function": "TIME_SERIES_DAILY_ADJUSTED", "outputsize": "compact"},
            symbol_param="symbol",
            key_param="apikey",
            raw_price_supported=True,
            adjusted_price_supported=True,
            splits_supported=True,
            dividends_supported=True,
            allowed_use_if_accessible="diagnostic_only",
            role="price",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="alpha_vantage",
            provider="Alpha Vantage",
            endpoint_name="dividends",
            key_envs=("ALPHA_VANTAGE_API_KEY",),
            url="https://www.alphavantage.co/query",
            params_template={"function": "DIVIDENDS"},
            symbol_param="symbol",
            key_param="apikey",
            dividends_supported=True,
            allowed_use_if_accessible="diagnostic_only",
            role="corporate_actions",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="alpha_vantage",
            provider="Alpha Vantage",
            endpoint_name="splits",
            key_envs=("ALPHA_VANTAGE_API_KEY",),
            url="https://www.alphavantage.co/query",
            params_template={"function": "SPLITS"},
            symbol_param="symbol",
            key_param="apikey",
            splits_supported=True,
            allowed_use_if_accessible="diagnostic_only",
            role="corporate_actions",
            current_view_only_risk=True,
        ),
        EndpointProbe(
            provider_id="fred",
            provider="FRED",
            endpoint_name="series_observations",
            key_envs=("FRED_API_KEY",),
            url="https://api.stlouisfed.org/fred/series/observations",
            params_template={
                "series_id": "DGS10",
                "observation_start": PROBE_START,
                "observation_end": PROBE_END,
                "file_type": "json",
            },
            key_param="api_key",
            event_calendar_supported=True,
            available_time_supported=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="macro",
            probe_kind="global",
        ),
        EndpointProbe(
            provider_id="cboe",
            provider="Cboe",
            endpoint_name="vix_daily_history",
            key_envs=(),
            url="https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
            params_template={},
            requires_key=False,
            raw_price_supported=True,
            source_manifest_possible=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="macro",
            probe_kind="global",
        ),
        EndpointProbe(
            provider_id="sec",
            provider="SEC EDGAR",
            endpoint_name="company_submissions",
            key_envs=(),
            url="https://data.sec.gov/submissions/CIK0000789019.json",
            params_template={},
            requires_key=False,
            fundamentals_supported=True,
            available_time_supported=True,
            source_manifest_possible=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="fundamentals",
            probe_kind="global",
        ),
        EndpointProbe(
            provider_id="sec",
            provider="SEC EDGAR",
            endpoint_name="companyfacts",
            key_envs=(),
            url="https://data.sec.gov/api/xbrl/companyfacts/CIK0000789019.json",
            params_template={},
            requires_key=False,
            fundamentals_supported=True,
            source_manifest_possible=True,
            allowed_use_if_accessible="promotion_candidate_after_qualification",
            role="fundamentals",
            probe_kind="global",
        ),
        EndpointProbe(
            provider_id="govinfo",
            provider="GovInfo",
            endpoint_name="packages",
            key_envs=("GOVINFO_API_KEY",),
            url="https://api.govinfo.gov/packages",
            params_template={"offset": 0, "pageSize": 1},
            key_param="api_key",
            event_calendar_supported=True,
            available_time_supported=True,
            allowed_use_if_accessible="research_label_only",
            role="event_calendar",
            probe_kind="global",
        ),
        EndpointProbe(
            provider_id="congress",
            provider="Congress.gov",
            endpoint_name="bill_search",
            key_envs=("CONGRESS_API_KEY",),
            url="https://api.congress.gov/v3/bill",
            params_template={"limit": 1, "format": "json"},
            key_param="api_key",
            event_calendar_supported=True,
            available_time_supported=True,
            allowed_use_if_accessible="research_label_only",
            role="event_calendar",
            probe_kind="global",
        ),
    ]


def _run_endpoint_probe(
    probe: EndpointProbe,
    *,
    env: Mapping[str, str],
    http_get: Callable[..., Any] | None,
    timeout_seconds: float,
) -> dict[str, Any]:
    key = _provider_key(probe.key_envs, env)
    key_present = bool(key)
    if probe.requires_key and not key_present:
        return _endpoint_result(
            probe,
            accessible=False,
            key_present=False,
            sanitized_error_class="KEY_MISSING",
            plan_or_limit_info_if_available="key_missing",
            covered_symbols=[],
            probed_symbols=_probe_symbols_for(probe),
            historical_depth_observed="not_observed_key_missing",
        )

    request_outcomes: list[dict[str, Any]] = []
    for symbol in _probe_symbols_for(probe):
        url, params = _probe_request(probe, symbol=symbol, key=key)
        try:
            response = _http_get(
                url,
                params=params,
                timeout_seconds=timeout_seconds,
                http_get=http_get,
                provider=probe.provider,
            )
            payload = _response_payload(response)
            accessible, error_class, limit_info = _classify_response(response, payload)
            request_outcomes.append(
                {
                    "symbol": symbol,
                    "accessible": accessible,
                    "sanitized_error_class": error_class,
                    "plan_or_limit_info_if_available": limit_info,
                    "date_range": _observed_date_range(payload),
                }
            )
        except Exception as exc:  # noqa: BLE001 - converted to sanitized audit class.
            request_outcomes.append(
                {
                    "symbol": symbol,
                    "accessible": False,
                    "sanitized_error_class": _exception_error_class(exc),
                    "plan_or_limit_info_if_available": "request_exception",
                    "date_range": None,
                }
            )
    accessible = any(bool(item["accessible"]) for item in request_outcomes)
    covered_symbols = [
        str(item["symbol"])
        for item in request_outcomes
        if bool(item["accessible"]) and str(item["symbol"]) != "_global"
    ]
    error_class = _dominant_error_class(request_outcomes, accessible=accessible)
    plan_info = _dominant_plan_info(request_outcomes, accessible=accessible)
    return _endpoint_result(
        probe,
        accessible=accessible,
        key_present=key_present,
        sanitized_error_class=error_class,
        plan_or_limit_info_if_available=plan_info,
        covered_symbols=covered_symbols,
        probed_symbols=[str(item["symbol"]) for item in request_outcomes],
        historical_depth_observed=_historical_depth_observed(request_outcomes),
    )


def _http_get(
    url: str,
    *,
    params: Mapping[str, Any],
    timeout_seconds: float,
    http_get: Callable[..., Any] | None,
    provider: str,
) -> Any:
    headers = {}
    if provider == "SEC EDGAR":
        headers["User-Agent"] = "AITradingSystem validation-only entitlement audit"
    if http_get is not None:
        return http_get(url, params=dict(params), timeout=timeout_seconds, headers=headers)
    requests = import_module("requests")
    return requests.get(url, params=dict(params), timeout=timeout_seconds, headers=headers)


def _probe_request(
    probe: EndpointProbe,
    *,
    symbol: str,
    key: str | None,
) -> tuple[str, dict[str, Any]]:
    params = dict(probe.params_template)
    url = probe.url
    if probe.path_symbol_template:
        provider_symbol = _provider_symbol(probe.provider_id, symbol)
        url = url.format(symbol=provider_symbol)
    elif probe.symbol_param:
        params[probe.symbol_param] = _provider_symbol(probe.provider_id, symbol)
    elif probe.symbols_param:
        params[probe.symbols_param] = ",".join(
            _provider_symbol(probe.provider_id, ticker) for ticker in probe.probe_symbols
        )
    if key is not None:
        params[probe.key_param] = key
    return url, params


def _endpoint_result(
    probe: EndpointProbe,
    *,
    accessible: bool,
    key_present: bool,
    sanitized_error_class: str,
    plan_or_limit_info_if_available: str,
    covered_symbols: Sequence[str],
    probed_symbols: Sequence[str],
    historical_depth_observed: str,
) -> dict[str, Any]:
    gaps = _pit_qualification_gaps(
        probe,
        accessible=accessible,
        key_present=key_present,
        sanitized_error_class=sanitized_error_class,
    )
    return {
        "provider": probe.provider,
        "endpoint_name": probe.endpoint_name,
        "key_present": key_present,
        "endpoint_accessible": accessible,
        "accessible": accessible,
        "coverage_for_representative_universe": {
            "requested": list(REPRESENTATIVE_UNIVERSE),
            "probed": [symbol for symbol in probed_symbols if symbol != "_global"],
            "covered": list(covered_symbols),
            "missing": [
                symbol
                for symbol in PRICE_PROBE_SYMBOLS
                if symbol in [item for item in probed_symbols if item != "_global"]
                and symbol not in covered_symbols
            ],
            "cash": "not_applicable_requires_cash_yield_financing_policy",
            "coverage_ratio_observed": _coverage_ratio(covered_symbols, probed_symbols),
        },
        "historical_depth_observed": historical_depth_observed,
        "raw_price_supported": probe.raw_price_supported,
        "adjusted_price_supported": probe.adjusted_price_supported,
        "splits_supported": probe.splits_supported,
        "dividends_supported": probe.dividends_supported,
        "delisted_supported": probe.delisted_supported,
        "fundamentals_supported": probe.fundamentals_supported,
        "event_calendar_supported": probe.event_calendar_supported,
        "available_time_supported": probe.available_time_supported,
        "source_manifest_possible": probe.source_manifest_possible,
        "current_view_only_risk": probe.current_view_only_risk,
        "PIT_qualification_gap": gaps,
        "likely_allowed_use": _likely_allowed_use(probe, accessible=accessible, gaps=gaps),
        "plan_or_limit_info_if_available": plan_or_limit_info_if_available,
        "sanitized_error_class": sanitized_error_class,
        "allowed_uses_candidate": _allowed_uses_candidate(probe),
        "status_upgrade_attempted": False,
        "lookahead_violation_count": 0,
        **SAFETY_BOUNDARY,
    }


def _provider_key_statuses(
    endpoint_results: Sequence[Mapping[str, Any]],
    env: Mapping[str, str],
) -> list[dict[str, Any]]:
    providers = [
        ("Financial Modeling Prep", ("FINANCIAL_MODELING_PREP_API_KEY", "FMP_API_KEY")),
        ("Marketstack", ("MARKETSTACK_API_KEY",)),
        ("EODHD", ("EODHD_API_KEY",)),
        ("Alpha Vantage", ("ALPHA_VANTAGE_API_KEY",)),
        ("FRED", ("FRED_API_KEY",)),
        ("Cboe", ()),
        ("SEC EDGAR", ()),
        ("GovInfo", ("GOVINFO_API_KEY",)),
        ("Congress.gov", ("CONGRESS_API_KEY",)),
    ]
    rows: list[dict[str, Any]] = []
    for provider, key_envs in providers:
        provider_endpoints = [item for item in endpoint_results if item["provider"] == provider]
        endpoint_accessible = any(bool(item["accessible"]) for item in provider_endpoints)
        key_present = bool(_provider_key(key_envs, env)) if key_envs else False
        error_class = "NONE" if endpoint_accessible else _first_error_class(provider_endpoints)
        rows.append(
            {
                "provider": provider,
                "key_present": key_present,
                "endpoint_accessible": endpoint_accessible,
                "plan_or_limit_info_if_available": _first_plan_info(provider_endpoints),
                "sanitized_error_class": error_class,
                "allowed_uses_candidate": _provider_allowed_uses(provider_endpoints),
            }
        )
    return rows


def _match_requirements_to_subscription(
    requirements: Sequence[Mapping[str, Any]],
    endpoint_results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    matches = []
    for requirement in requirements:
        candidates = _candidate_endpoints_for_requirement(requirement, endpoint_results)
        accessible_candidates = [item for item in candidates if item.get("accessible") is True]
        if accessible_candidates:
            cover = _coverage_decision(requirement, accessible_candidates)
            selected = accessible_candidates[:3]
        else:
            cover = "false"
            selected = candidates[:3]
        remaining_gap = _remaining_gap_for_requirement(requirement, selected, cover)
        matches.append(
            {
                "requirement_id": str(requirement.get("requirement_id") or ""),
                "component": str(requirement.get("component") or ""),
                "can_current_subscription_cover": cover,
                "provider_candidate": _unique_strings(
                    str(item.get("provider") or "") for item in selected
                ),
                "endpoint_candidate": _unique_strings(
                    str(item.get("endpoint_name") or "") for item in selected
                ),
                "remaining_gap": remaining_gap,
                "requires_new_paid_source": _requires_new_paid_source(
                    requirement,
                    selected,
                    cover,
                    remaining_gap,
                ),
                "status_upgrade_attempted": False,
                "promotion_gate_allowed": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return matches


def _candidate_endpoints_for_requirement(
    requirement: Mapping[str, Any],
    endpoint_results: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    component = str(requirement.get("component") or "")
    blocked_reason = str(requirement.get("blocked_reason") or "")
    text = f"{component} {blocked_reason} {' '.join(_strings(requirement.get('missing_proof')))}"
    if "price" in text or "trend" in text or component == "pit_feature_store":
        return [
            item
            for item in endpoint_results
            if item.get("raw_price_supported")
            or item.get("adjusted_price_supported")
            or item.get("splits_supported")
            or item.get("dividends_supported")
        ]
    if component in {"asset_master", "tradable_universe"} or "ticker" in text:
        return [
            item
            for item in endpoint_results
            if item.get("delisted_supported")
            or item.get("endpoint_name")
            in {
                "ticker_exchange_metadata",
                "exchange_symbol_list_symbol_change",
                "index_constituents_sp500",
            }
        ]
    if "fundamental" in text or "SEC" in text:
        return [item for item in endpoint_results if item.get("fundamentals_supported")]
    if "valuation" in text:
        return [
            item
            for item in endpoint_results
            if item.get("fundamentals_supported")
            or item.get("endpoint_name")
            in {
                "financial_statements_income_statement",
                "fundamentals",
            }
        ]
    if component == "cost_liquidity_model":
        return [item for item in endpoint_results if item.get("raw_price_supported")]
    if component == "regime_event_cluster_labels":
        return [item for item in endpoint_results if item.get("event_calendar_supported")]
    return []


def _coverage_decision(
    requirement: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> str:
    current_status = str(requirement.get("current_status") or "")
    if current_status in {"CURRENT_VIEW_ONLY", "RESEARCH_LABEL_ONLY"}:
        return "unknown"
    if all(item.get("current_view_only_risk") for item in candidates):
        return "unknown"
    if any(item.get("source_manifest_possible") for item in candidates):
        return "true"
    return "unknown"


def _remaining_gap_for_requirement(
    requirement: Mapping[str, Any],
    selected: Sequence[Mapping[str, Any]],
    cover: str,
) -> list[str]:
    gaps = list(_strings(requirement.get("missing_proof")))
    for candidate in selected:
        gaps.extend(_strings(candidate.get("PIT_qualification_gap")))
    if cover == "false" and not selected:
        gaps.append("no_accessible_current_subscription_endpoint_candidate")
    gaps.append("no_source_status_upgrade_attempted")
    return _unique_strings(gaps)


def _requires_new_paid_source(
    requirement: Mapping[str, Any],
    selected: Sequence[Mapping[str, Any]],
    cover: str,
    remaining_gap: Sequence[str],
) -> bool:
    if cover == "false":
        return True
    if bool(requirement.get("requires_new_data_source")) and any(
        "KEY_MISSING" == str(item.get("sanitized_error_class")) for item in selected
    ):
        return True
    if "TRUE_PIT_LIMITATION" in _strings(requirement.get("requirement_categories")):
        return False
    return any("new_paid_source" in gap for gap in remaining_gap)


def _pit_qualification_gaps(
    probe: EndpointProbe,
    *,
    accessible: bool,
    key_present: bool,
    sanitized_error_class: str,
) -> list[str]:
    gaps: list[str] = []
    if probe.requires_key and not key_present:
        gaps.append("key_missing")
    if not accessible:
        gaps.append(f"endpoint_not_accessible:{sanitized_error_class}")
    if not probe.available_time_supported:
        gaps.append("available_time_contract_missing")
    if probe.current_view_only_risk:
        gaps.append("current_view_only_or_revision_policy_risk")
    if probe.source_manifest_possible:
        gaps.append("source_manifest_not_yet_captured")
    else:
        gaps.append("source_manifest_feasibility_unknown")
    gaps.append("as_of_snapshot_not_yet_captured")
    return _unique_strings(gaps)


def _likely_allowed_use(
    probe: EndpointProbe,
    *,
    accessible: bool,
    gaps: Sequence[str],
) -> str:
    if not accessible:
        return "blocked_until_qualified"
    if probe.allowed_use_if_accessible == "research_label_only":
        return "research_label_only"
    if probe.current_view_only_risk:
        return "diagnostic_only"
    if "available_time_contract_missing" in gaps and probe.role not in {
        "price",
        "corporate_actions",
        "macro",
        "fundamentals",
    }:
        return "diagnostic_only"
    return probe.allowed_use_if_accessible


def _allowed_uses_candidate(probe: EndpointProbe) -> list[str]:
    if probe.allowed_use_if_accessible == "research_label_only":
        return ["analysis", "casebook", "stratified_reporting"]
    if probe.allowed_use_if_accessible == "promotion_candidate_after_qualification":
        return ["diagnostic", "promotion_candidate_after_qualification"]
    return ["diagnostic"]


def _classify_response(response: Any, payload: Any) -> tuple[bool, str, str]:
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 401:
        return False, "UNAUTHORIZED_OR_INVALID_KEY", "invalid_key_or_unauthorized"
    if status_code in {402, 403}:
        return False, "PLAN_OR_PERMISSION_LIMIT", "premium_or_plan_limit"
    if status_code == 429:
        return False, "RATE_LIMIT", "rate_limit"
    if status_code >= 400:
        return False, f"HTTP_{status_code}", "http_error"
    provider_error = _provider_error_class(payload)
    if provider_error:
        return False, provider_error, _plan_info_from_error_class(provider_error)
    if _payload_empty(payload):
        return True, "ACCESSIBLE_EMPTY_RESPONSE", "accessible_empty_response"
    return True, "NONE", "none"


def _provider_error_class(payload: Any) -> str:
    text = json.dumps(_jsonable(payload), ensure_ascii=False).lower()[:4000]
    if not text:
        return ""
    if "invalid api" in text or "invalid_apikey" in text or "api key" in text and "invalid" in text:
        return "UNAUTHORIZED_OR_INVALID_KEY"
    if "premium" in text or "not available under your current subscription" in text:
        return "PLAN_OR_PERMISSION_LIMIT"
    if (
        "rate limit" in text
        or "call frequency" in text
        or "thank you for using alpha vantage" in text
    ):
        return "RATE_LIMIT"
    if '"error"' in text or '"error message"' in text or '"note"' in text:
        return "PROVIDER_ERROR"
    return ""


def _plan_info_from_error_class(error_class: str) -> str:
    return {
        "PLAN_OR_PERMISSION_LIMIT": "premium_or_plan_limit",
        "RATE_LIMIT": "rate_limit",
        "UNAUTHORIZED_OR_INVALID_KEY": "invalid_key_or_unauthorized",
        "PROVIDER_ERROR": "provider_error",
    }.get(error_class, "none")


def _response_payload(response: Any) -> Any:
    try:
        return response.json()
    except Exception:  # noqa: BLE001 - CSV/text endpoints are expected.
        text = str(getattr(response, "text", "") or "")
        return {"_text_sample_present": bool(text), "_text_sample_length": len(text)}


def _payload_empty(payload: Any) -> bool:
    if payload is None:
        return True
    if isinstance(payload, list):
        return len(payload) == 0
    if isinstance(payload, dict):
        if payload.get("_text_sample_present"):
            return False
        data = payload.get("data")
        if isinstance(data, list):
            return len(data) == 0
        return len(payload) == 0
    return False


def _observed_date_range(payload: Any) -> tuple[str, str] | None:
    dates: list[str] = []
    for record in _payload_records(payload):
        for key in ("date", "calendarYear", "fillingDate", "acceptedDate", "lastUpdated"):
            value = record.get(key)
            if isinstance(value, str) and value[:4].isdigit():
                dates.append(value[:10])
                break
    if not dates:
        return None
    return min(dates), max(dates)


def _payload_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "historical", "observations", "results", "trends"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _historical_depth_observed(outcomes: Sequence[Mapping[str, Any]]) -> str:
    ranges = [item.get("date_range") for item in outcomes if item.get("date_range")]
    if not ranges:
        return "not_observed"
    starts = [str(item[0]) for item in ranges if isinstance(item, tuple)]
    ends = [str(item[1]) for item in ranges if isinstance(item, tuple)]
    if not starts or not ends:
        return "not_observed"
    return f"{min(starts)}..{max(ends)}"


def _dominant_error_class(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    accessible: bool,
) -> str:
    if accessible:
        return "NONE"
    for outcome in outcomes:
        value = str(outcome.get("sanitized_error_class") or "")
        if value and value != "NONE":
            return value
    return "UNKNOWN"


def _dominant_plan_info(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    accessible: bool,
) -> str:
    if accessible:
        non_empty = [
            str(item.get("plan_or_limit_info_if_available") or "")
            for item in outcomes
            if str(item.get("plan_or_limit_info_if_available") or "") not in {"", "none"}
        ]
        return non_empty[0] if non_empty else "none"
    for outcome in outcomes:
        value = str(outcome.get("plan_or_limit_info_if_available") or "")
        if value and value != "none":
            return value
    return "none"


def _exception_error_class(exc: Exception) -> str:
    name = type(exc).__name__.upper()
    if "TIMEOUT" in name:
        return "TIMEOUT"
    if "SSL" in name:
        return "TLS_OR_SSL_ERROR"
    if "CONNECTION" in name:
        return "CONNECTION_ERROR"
    return "REQUEST_EXCEPTION"


def _provider_key(key_envs: Sequence[str], env: Mapping[str, str]) -> str | None:
    for key_env in key_envs:
        value = str(env.get(key_env) or "").strip()
        if value:
            return value
    return None


def _probe_symbols_for(probe: EndpointProbe) -> list[str]:
    if probe.probe_kind == "global":
        return ["_global"]
    if probe.symbols_param:
        return [probe.probe_symbols[0]]
    return list(probe.probe_symbols)


def _provider_symbol(provider_id: str, symbol: str) -> str:
    if symbol.lower() == "cash":
        return symbol
    if provider_id == "eodhd" and symbol == "TSM":
        return "TSM"
    return symbol


def _coverage_ratio(covered_symbols: Sequence[str], probed_symbols: Sequence[str]) -> float | None:
    probed = [item for item in probed_symbols if item != "_global"]
    if not probed:
        return None
    return round(len(set(covered_symbols)) / len(set(probed)), 4)


def _first_error_class(provider_endpoints: Sequence[Mapping[str, Any]]) -> str:
    for endpoint in provider_endpoints:
        value = str(endpoint.get("sanitized_error_class") or "")
        if value and value != "NONE":
            return value
    return "UNKNOWN"


def _first_plan_info(provider_endpoints: Sequence[Mapping[str, Any]]) -> str:
    for endpoint in provider_endpoints:
        value = str(endpoint.get("plan_or_limit_info_if_available") or "")
        if value and value != "none":
            return value
    return "none"


def _provider_allowed_uses(provider_endpoints: Sequence[Mapping[str, Any]]) -> list[str]:
    uses: list[str] = []
    for endpoint in provider_endpoints:
        uses.extend(_strings(endpoint.get("allowed_uses_candidate")))
    return _unique_strings(uses) or ["diagnostic"]


def _write_artifact_pair(
    payload: Mapping[str, Any],
    *,
    output_root: Path,
    artifact_id: str,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / f"{artifact_id}.json").write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_root / f"{artifact_id}.md").write_text(
        _render_subscription_markdown(payload),
        encoding="utf-8",
    )


def _render_subscription_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Current subscription data coverage matrix",
        "",
        f"- 状态：`{payload.get('status')}`",
        f"- production_effect：`{payload.get('production_effect')}`",
        f"- broker_action：`{payload.get('broker_action')}`",
        f"- api_key_material_recorded：`{payload.get('api_key_material_recorded')}`",
        "",
        "## Summary",
        "",
        "|字段|值|",
        "|---|---|",
    ]
    for key, value in _mapping(payload.get("summary")).items():
        lines.append(f"|`{key}`|{_compact(value)}|")
    lines.extend(
        [
            "",
            "## Provider Key Status",
            "",
            "|provider|key_present|endpoint_accessible|sanitized_error_class|allowed_uses_candidate|",
            "|---|---:|---:|---|---|",
        ]
    )
    for row in _records(payload.get("provider_key_statuses")):
        lines.append(
            "|{provider}|{key_present}|{endpoint_accessible}|{error}|{uses}|".format(
                provider=row.get("provider"),
                key_present=row.get("key_present"),
                endpoint_accessible=row.get("endpoint_accessible"),
                error=row.get("sanitized_error_class"),
                uses=_compact(row.get("allowed_uses_candidate")),
            )
        )
    lines.extend(
        [
            "",
            "## Requirement Matches",
            "",
            "|requirement_id|can_current_subscription_cover|provider_candidate|remaining_gap|",
            "|---|---|---|---|",
        ]
    )
    for row in _records(payload.get("requirement_subscription_matches")):
        lines.append(
            "|{rid}|{cover}|{provider}|{gap}|".format(
                rid=row.get("requirement_id"),
                cover=row.get("can_current_subscription_cover"),
                provider=_compact(row.get("provider_candidate")),
                gap=_compact(row.get("remaining_gap")),
            )
        )
    return "\n".join(lines) + "\n"


def _assert_payload_has_no_key_material(payload: Mapping[str, Any], env: Mapping[str, str]) -> None:
    serialized = json.dumps(_jsonable(payload), ensure_ascii=False, sort_keys=True)
    secret_values = [
        str(env.get(name) or "")
        for name in (
            "FINANCIAL_MODELING_PREP_API_KEY",
            "FMP_API_KEY",
            "MARKETSTACK_API_KEY",
            "EODHD_API_KEY",
            "ALPHA_VANTAGE_API_KEY",
            "FRED_API_KEY",
            "CONGRESS_API_KEY",
            "GOVINFO_API_KEY",
        )
        if str(env.get(name) or "").strip()
    ]
    for value in secret_values:
        if len(value) >= 4 and value in serialized:
            raise SubscriptionCoverageAuditError("API key material would be written to payload")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SubscriptionCoverageAuditError(f"required TRADING-738 input not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _records(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _strings(value: Any) -> list[str]:
    return [str(item) for item in value if str(item)] if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _unique_strings(values: Sequence[str] | Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value)
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def _compact(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return ", ".join(_compact(item) for item in value) if value else "none"
    if isinstance(value, Mapping):
        return json.dumps(_jsonable(value), ensure_ascii=False, sort_keys=True)
    if value is None:
        return "none"
    return str(value)
